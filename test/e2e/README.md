# Test worker exhaustion and recovery

This test uses a real AX server, Redis, and Substrate. It creates a separate
worker pool with one CPU and four tasks that each require one CPU, so only one
task can run at a time.

Build the CLI with `make build`. You also need `kubectl` with the `ate` plugin,
`jq`, GNU `timeout`, a working gVisor pool, and writable snapshot storage.
Use a dedicated AX test server built from this checkout and configure it with
`--template=ax-scheduling-e2e/fixture`.

```bash
export AX_SERVER=http://<test-server>:8080
export AX_TEST_IMAGE=<registry>/ax-task-runner@sha256:<digest>
export AX_TEST_SNAPSHOTS_BUCKET=gs://<writable-test-bucket>
# Also check recovery after restarting deployment/ax-server:
export AX_TEST_SERVER_NAMESPACE=<dedicated-ax-test-namespace>
make test-e2e-scheduling
```

The image must contain `/usr/local/bin/ax-task-runner` built from this checkout.
The server endpoint must survive a rollout when testing restarts; a port-forward
to the old server Pod will disconnect. Omit `AX_TEST_SERVER_NAMESPACE` to skip
only the restart step.

Expected results: one task runs and three wait in `Pending`. Suspending or
deleting waiting tasks cancels their resumes. The remaining task survives a
server restart and runs automatically when the first task is deleted. It must
successfully write and read a file inside its sandbox. Scaling the pool to two
workers must also start another waiting task.

The test pre-creates Substrate actors with a worker selector to keep placement
inside its own pool; AX manages their lifecycle. The fixture fallback also
supports installed Substrate versions that reject AX's generated templates.
Generated-template compatibility is outside this test's coverage.

The source pool defaults to `ate-demo-counter/counter`; override
`SOURCE_NAMESPACE` and `SOURCE_POOL` if needed. `AX_TEST_ATESPACE` defaults to
`ax-scheduling-e2e` and must be unused; changing it also requires changing the
server's template setting. `TEST_TIMEOUT` defaults to 120 seconds per transition.
The test fails on missing prerequisites or failed assertions and cleans up the
resources it creates. Snapshot blobs follow the bucket's retention policy.
