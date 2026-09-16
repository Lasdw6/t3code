/**
 * StoreRefreshPoller - Loads events another process appended to the store.
 *
 * An Abra thread transfer writes a thread's events and projection rows straight
 * into `state.sqlite` while the server runs. The engine only learns about
 * events through its own dispatch, so this layer compares the store head with
 * the engine head on an interval and asks the engine to `refresh` when the
 * store is ahead. It is wired only in the server runtime, never in engine
 * tests, because a scheduled poll misbehaves under a test clock.
 *
 * `T3CODE_STORE_REFRESH_MS` sets the interval (default 2000). `0` disables it.
 *
 * @module StoreRefreshPoller
 */
import * as Config from "effect/Config";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schedule from "effect/Schedule";

import { OrchestrationEventStore } from "../../persistence/Services/OrchestrationEventStore.ts";
import { OrchestrationEngineService } from "../Services/OrchestrationEngine.ts";

export const DEFAULT_STORE_REFRESH_MS = 2000;

export interface StoreRefreshPollerOptions {
  /** Overrides the configured interval; `0` disables the poll. */
  readonly intervalMs?: number;
}

const makeStoreRefreshPoller = (options?: StoreRefreshPollerOptions) =>
  Effect.gen(function* () {
    const engine = yield* OrchestrationEngineService;
    const eventStore = yield* OrchestrationEventStore;
    const intervalMs =
      options?.intervalMs ??
      (yield* Config.int("T3CODE_STORE_REFRESH_MS").pipe(
        Config.withDefault(DEFAULT_STORE_REFRESH_MS),
      ));
    if (intervalMs <= 0) {
      yield* Effect.logDebug("orchestration store refresh poll disabled");
      return;
    }

    const tick = Effect.gen(function* () {
      const storeHead = yield* eventStore.latestSequence;
      const engineHead = yield* engine.latestSequence;
      if (storeHead > engineHead) {
        yield* engine.refresh;
      }
    }).pipe(
      Effect.catchCause((cause) =>
        Effect.logWarning("orchestration store refresh failed", { cause }),
      ),
    );

    yield* Effect.forkScoped(
      tick.pipe(Effect.repeat(Schedule.spaced(Duration.millis(intervalMs)))),
    );
    yield* Effect.logDebug("orchestration store refresh poll started").pipe(
      Effect.annotateLogs({ intervalMs }),
    );
  });

export const StoreRefreshPollerLive = (options?: StoreRefreshPollerOptions) =>
  Layer.effectDiscard(makeStoreRefreshPoller(options));
