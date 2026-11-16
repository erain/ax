# AX live task log demo

Recorded from the asserted `TestE2E_TaskLogs` test at AX commit
`b23d93eed6c2ce0417b9179eb52be5a682b1563c`.

Setup: disposable Kind v1.37 cluster, Substrate v1.0.0 release images,
four gVisor workers, PostgreSQL, and AX built from that commit. The E2E
configuration enables task logs and applies the optional namespaced Role.

After `make e2e-setup`, repeat the demo with:

```sh
./hack/run-e2e.sh -test.run '^TestE2E_TaskLogs$'
```

The test requests logs for a suspended actor, asserts FailedPrecondition,
and checks that the actor remains suspended. It then explicitly resumes the
task and receives three synthetic AX-LIVE-LOG records, checking every record's
UID against the real Substrate actor. The command and recorder both completed
successfully. Only synthetic output was used; no credentials were printed.

The GIF removes initial idle frames and caps idle time at two seconds for
readability. The test's waits and assertions were unchanged.
