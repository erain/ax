# Development

## Prerequisites

- Go 1.27+
- [`ko`](https://ko.build/) for building and deploying control plane images
- Docker or Podman for the task runner image
- A Kubernetes cluster and kubeconfig

## Build

```bash
make build                 # bin/ax, bin/ax-server
make install               # install the ax CLI into $(go env GOPATH)/bin
make build-task-runner     # cross-compile the runner for linux/amd64 and build its image
make push-task-runner      # ...and push it (set TASK_RUNNER_REPO)
```

## Test

Runs everything, including the mock Substrate gRPC server, in-memory store validation, and API server tests:

```bash
make test        # or: go test -v ./...
```

## Contributing

See [CONTRIBUTING.md](../CONTRIBUTING.md) for contribution guidelines, CLA requirements, and PR workflows.

## Scheduling retries

When Substrate has no free worker, a resumed task stays `Pending` and AX retries
automatically. Delays start at 1 second, double to 2 and 4 seconds, then stay at
5 seconds. This keeps new capacity useful without retrying continuously.

Configure the starting delay and cap with ax-server flags:

```bash
./bin/ax-server --schedule-retry-interval=1s --schedule-retry-max-delay=5s
```

The interval also controls how often AX checks for waiting tasks. Both flags
accept durations such as `500ms` or `2s`. The interval must be positive and the
maximum delay must be at least the interval; invalid settings fail at startup.
These flags control retry pacing, not a deadline for waiting for capacity.
