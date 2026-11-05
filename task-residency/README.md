# Demo: task residency, outcome, and actor UID

`residency-demo.sh` drives the `ax` CLI against a live AX + Agent Substrate
cluster and exits non-zero on any unexpected state. It creates a task, resumes
it, reads its actor UID, deletes the worker pod hosting the actor so Substrate
marks it crashed, resumes it again, and suspends it.

```bash
CTX=<kube context> AX=<ax built from the PR branch> ./residency-demo.sh
```

Prerequisites: a cluster running ax-server and the task runner from the PR
branch, a gVisor WorkerPool with at least two workers in `ax-system`, and
`kubectl` access to delete pods there.

`residency-demo.gif` is one recorded run on GKE 1.37 with Agent Substrate
v1.0.0-rc.1 (idle time after the run trimmed).
