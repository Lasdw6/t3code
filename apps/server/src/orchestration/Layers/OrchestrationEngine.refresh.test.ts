import {
  CommandId,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  type OrchestrationEvent,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as ManagedRuntime from "effect/ManagedRuntime";
import * as Stream from "effect/Stream";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { afterEach, describe, expect, it } from "vite-plus/test";

import { OrchestrationCommandReceiptRepositoryLive } from "../../persistence/Layers/OrchestrationCommandReceipts.ts";
import { OrchestrationEventStoreLive } from "../../persistence/Layers/OrchestrationEventStore.ts";
import { makeSqlitePersistenceLive } from "../../persistence/Layers/Sqlite.ts";
import * as RepositoryIdentityResolver from "../../project/RepositoryIdentityResolver.ts";
import { OrchestrationEngineLive } from "./OrchestrationEngine.ts";
import { OrchestrationProjectionPipelineLive } from "./ProjectionPipeline.ts";
import { OrchestrationProjectionSnapshotQueryLive } from "./ProjectionSnapshotQuery.ts";
import { StoreRefreshPollerLive } from "./StoreRefreshPoller.ts";
import * as ThreadBackgroundLiveness from "../ThreadBackgroundLiveness.ts";
import * as ThreadPlanProgress from "../ThreadPlanProgress.ts";
import { OrchestrationEngineService } from "../Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../Services/ProjectionSnapshotQuery.ts";
import { ServerConfig } from "../../config.ts";

/**
 * Two engines on one SQLite file stand in for two processes: the running T3
 * Code server and an Abra transfer that appends a thread behind its back.
 */
async function createOrchestrationSystem(
  dbPath: string,
  options: { readonly pollIntervalMs?: number } = {},
) {
  const ServerConfigLayer = ServerConfig.layerTest(process.cwd(), {
    prefix: "t3-orchestration-engine-refresh-test-",
  });
  const engineLayer = OrchestrationEngineLive.pipe(
    Layer.provide(OrchestrationProjectionSnapshotQueryLive),
    Layer.provide(OrchestrationProjectionPipelineLive),
  );
  const orchestrationLayer = Layer.mergeAll(
    engineLayer,
    OrchestrationProjectionSnapshotQueryLive,
    options.pollIntervalMs === undefined
      ? Layer.empty
      : StoreRefreshPollerLive({ intervalMs: options.pollIntervalMs }).pipe(
          Layer.provide(engineLayer),
        ),
  ).pipe(
    Layer.provide(ThreadBackgroundLiveness.layer),
    Layer.provide(ThreadPlanProgress.layer),
    Layer.provide(OrchestrationEventStoreLive),
    Layer.provide(OrchestrationCommandReceiptRepositoryLive),
    Layer.provide(RepositoryIdentityResolver.layer),
    Layer.provide(makeSqlitePersistenceLive(dbPath)),
    Layer.provideMerge(ServerConfigLayer),
    Layer.provideMerge(NodeServices.layer),
  );
  const runtime = ManagedRuntime.make(orchestrationLayer);
  const engine = await runtime.runPromise(Effect.service(OrchestrationEngineService));
  const snapshotQuery = await runtime.runPromise(Effect.service(ProjectionSnapshotQuery));
  return {
    engine,
    snapshot: snapshotQuery.getSnapshot(),
    readModel: () => runtime.runPromise(snapshotQuery.getSnapshot()),
    run: <A, E>(effect: Effect.Effect<A, E>) => runtime.runPromise(effect),
    dispose: () => runtime.dispose(),
  };
}

const createdAt = "2026-09-16T12:00:00.000Z";

const createProjectAndThread = (
  system: Awaited<ReturnType<typeof createOrchestrationSystem>>,
  suffix: string,
) =>
  system.run(
    Effect.gen(function* () {
      yield* system.engine.dispatch({
        type: "project.create",
        commandId: CommandId.make(`cmd-project-${suffix}-create`),
        projectId: ProjectId.make(`project-${suffix}`),
        title: `Project ${suffix}`,
        workspaceRoot: `/tmp/project-${suffix}`,
        defaultModelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        createdAt,
      });
      yield* system.engine.dispatch({
        type: "thread.create",
        commandId: CommandId.make(`cmd-thread-${suffix}-create`),
        threadId: ThreadId.make(`thread-${suffix}`),
        projectId: ProjectId.make(`project-${suffix}`),
        title: `Imported ${suffix}`,
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        branch: null,
        worktreePath: null,
        createdAt,
      });
    }),
  );

describe("OrchestrationEngine refresh", () => {
  const disposers: Array<() => Promise<void>> = [];
  let tempDir: string | undefined;

  afterEach(async () => {
    for (const dispose of disposers.splice(0)) {
      await dispose();
    }
    if (tempDir !== undefined) {
      NodeFS.rmSync(tempDir, { recursive: true, force: true });
      tempDir = undefined;
    }
  });

  it("folds events another writer appended into the read model, projections, and live stream", async () => {
    tempDir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-refresh-"));
    const dbPath = NodePath.join(tempDir, "state.sqlite");

    const server = await createOrchestrationSystem(dbPath);
    disposers.push(server.dispose);
    const writer = await createOrchestrationSystem(dbPath);
    disposers.push(writer.dispose);

    // Subscribe before the writer appends so the announcement cannot be missed.
    const announced = server.run(
      Effect.scoped(
        Effect.gen(function* () {
          const events = yield* server.engine.subscribeDomainEvents;
          return yield* Stream.runCollect(Stream.take(events, 2));
        }),
      ),
    );
    await createProjectAndThread(writer, "a");

    // The projections are shared, so the server can already read the thread,
    // but its command model still ends at sequence 0.
    expect((await server.readModel()).threads.map((thread) => thread.id)).toEqual(["thread-a"]);
    expect(await server.run(server.engine.latestSequence)).toBe(0);

    const first = await server.run(server.engine.refresh);
    expect(first).toEqual({ loaded: 2 });
    expect(await server.run(server.engine.latestSequence)).toBe(2);

    const snapshot = await server.readModel();
    expect(snapshot.threads.map((thread) => thread.title)).toEqual(["Imported a"]);
    expect(snapshot.projects.map((project) => project.workspaceRoot)).toEqual(["/tmp/project-a"]);

    const events = Array.from(await announced) as OrchestrationEvent[];
    expect(events.map((event) => event.type)).toEqual(["project.created", "thread.created"]);

    // The server can now act on the imported thread itself.
    await server.run(
      server.engine.dispatch({
        type: "thread.meta.update",
        commandId: CommandId.make("cmd-thread-a-rename"),
        threadId: ThreadId.make("thread-a"),
        title: "Renamed after import",
      }),
    );
    expect((await server.readModel()).threads[0]?.title).toBe("Renamed after import");

    expect(await server.run(server.engine.refresh)).toEqual({ loaded: 0 });
  });

  it("polls the store head and loads external events on its own", async () => {
    tempDir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-refresh-poll-"));
    const dbPath = NodePath.join(tempDir, "state.sqlite");

    const server = await createOrchestrationSystem(dbPath, { pollIntervalMs: 50 });
    disposers.push(server.dispose);
    const writer = await createOrchestrationSystem(dbPath);
    disposers.push(writer.dispose);

    await createProjectAndThread(writer, "b");

    const head = await server.run(
      Effect.gen(function* () {
        for (let attempt = 0; attempt < 100; attempt += 1) {
          const sequence = yield* server.engine.latestSequence;
          if (sequence >= 2) {
            return sequence;
          }
          yield* Effect.sleep(Duration.millis(50));
        }
        return yield* server.engine.latestSequence;
      }),
    );
    expect(head).toBe(2);
    expect(
      await server.run(
        server.engine.dispatch({
          type: "thread.meta.update",
          commandId: CommandId.make("cmd-thread-b-rename"),
          threadId: ThreadId.make("thread-b"),
          title: "Seen by the poll",
        }),
      ),
    ).toEqual({ sequence: 3 });
  });
});
