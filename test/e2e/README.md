# Worker exhaustion and recovery

Run `make test-e2e-scheduling` against a **dedicated test AX server** and a
Substrate installation with a working gVisor WorkerPool. The test creates its
own one-worker pool and fixture actors; it does not resize existing pools.
Each fixture actor requires the worker's full CPU capacity, so only one fits.

Prerequisites:

- `make build`, `kubectl` with the `ate` plugin, `jq`, and GNU `timeout`.
- A digest-pinned image containing `/usr/local/bin/ax-task-runner`, built from
  this checkout, and snapshot storage accessible to Substrate.
- AX built from this checkout with Redis and working Substrate credentials.
  Configure its fallback with `--template=ax-scheduling-e2e/fixture`.
- A working source pool (default: `ate-demo-counter/counter`). Its worker image
  and node selector are copied into a new pool. Override `SOURCE_NAMESPACE`
  and `SOURCE_POOL` when needed.

```bash
export AX_SERVER=http://<test-server>:8080
export AX_TEST_IMAGE=<registry>/ax-task-runner@sha256:<digest>
export AX_TEST_SNAPSHOTS_BUCKET=gs://<writable-test-bucket>
# Optional: also verify that a queued resume survives an AX server restart.
export AX_TEST_SERVER_NAMESPACE=<dedicated-ax-test-namespace>
make test-e2e-scheduling
```

Use an endpoint that survives a server rollout when enabling the restart test;
a port-forward attached to the old server Pod will disconnect. The test restarts
`deployment/ax-server` only when `AX_TEST_SERVER_NAMESPACE` is set.

The test fails on missing prerequisites and on any failed assertion, including
in CI. It verifies:

1. One task runs while three additional resume requests stay `Pending` with
   `Ready=False, reason=WaitingForWorker` across multiple retries.
2. Suspending and deleting queued tasks cancel their pending resumes.
3. An optional server restart preserves the remaining queued resume in Redis.
4. Deleting the running task releases capacity; the queued task runs without
   another resume request, and `ax ssh` successfully writes and reads a file.
5. Increasing the dedicated pool to two workers starts another queued task.

The fixture pre-creates actors with a worker selector to isolate placement.
AX then manages these existing actors through its normal ensure/resume/suspend/
delete paths. It does not test AX's generated-template policy. The configured
fallback also lets this test run when the installed Substrate rejects fields
in AX's generated templates; such compatibility errors remain separate issues.

`AX_TEST_ATESPACE` defaults to `ax-scheduling-e2e`; the matching Kubernetes
namespace must not exist. If overriding it, update the server's fallback
accordingly. `TEST_TIMEOUT` defaults to 120 seconds per state transition.
Cleanup removes test tasks, actors, the fixture template, atespace, and namespace
on exit, including failures. Snapshot blobs follow the test bucket's retention
policy.

For an unmodified AX server, the same test fails when the second task resumes:
its phase becomes `Failed` with `ActorResumeFailed` and `ResourceExhausted`.

## Record a review demo

```bash
asciinema rec --overwrite --command 'make test-e2e-scheduling' scheduling.cast
agg --idle-time-limit 2 scheduling.cast scheduling.gif
```

The recording executes the assertions above. Keep the `.cast` for searchable,
copyable output and attach the GIF to the PR. Uploading to asciinema.org is
optional; the cast and GIF can remain local or use your usual artifact host.

A recorded baseline/fix comparison is in [the review GIF](https://raw.githubusercontent.com/erain/ax/65c0258/test/e2e/recordings/scheduling.gif).
The [raw cast](recordings/scheduling.cast) preserves the output and original timing.
It shows upstream main failing on exhaustion and the scheduling fix passing all
assertions, including a server restart and command execution inside the sandbox.

The GIF is hosted in the contributor fork because upstream prohibits binary
files in pull requests. The cast remains a text artifact in this change.
