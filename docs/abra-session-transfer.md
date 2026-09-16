# Abra session transfer (personal branch)

Branch `abra-session-transfer` lets an [Abra](https://github.com/Lasdw6/Abra)
adapter move a thread into a running T3 Code server. It is not meant for
upstream.

## What changed

T3 Code keeps every thread as an event stream in `orchestration_events` and
rebuilds its in-memory command model only at startup. The Abra adapter
(`adapters/t3code-session` in the Abra repo) appends a thread's events and
projection rows straight into `state.sqlite`, so without this branch the server
would not notice the thread until restarted.

- `OrchestrationEventStore.latestSequence` reports the highest persisted
  sequence.
- `OrchestrationEngine.refresh` reads every event after the engine's own head,
  folds it into the command read model, runs the projection pipeline over it,
  and publishes a safe subset to live subscribers. It runs on the command
  worker, so it never interleaves with a dispatch.
- The engine polls the store head every `T3CODE_STORE_REFRESH_MS`
  milliseconds (default 2000; `0` disables) and calls `refresh` when another
  process appended events.

Only `project.created`, `thread.created`, `thread.meta-updated` (without a
title regeneration request), and the archive, settle, and pin events are
announced. The `-requested` events that make the provider and checkpoint
reactors act stay silent, because imported history is already settled. The
shell refetches the thread when it sees `thread.created`; opening the thread
loads its projected history.

## Run a controlled dev build

Use a data directory that is not your installed T3 Code's `~/.t3`:

```sh
T3CODE_HOME=/tmp/t3-abra-a pnpm dev
```

State lives at `<T3CODE_HOME>/userdata/state.sqlite`. Point the Abra adapter
at it with `T3CODE_HOME` in the connector's environment, or with the
`t3_home` option. A second instance with another `T3CODE_HOME` and
`T3CODE_PORT_OFFSET` stands in for a second device.

## Tests

```sh
pnpm --filter t3 exec vp test run src/orchestration/Layers/OrchestrationEngine.refresh.test.ts
```

The test boots two engines on one SQLite file, dispatches through one, and
checks that the other loads the thread on `refresh` and through the poll.
