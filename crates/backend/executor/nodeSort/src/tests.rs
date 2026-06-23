//! Logic tests for the sort node are deferred to the integration-level executor
//! tests once the slot payload + tuplesort owner land; until then `ExecSort`'s
//! seams (tuplesort, execProcnode, execTuples) panic, so a unit harness here
//! would only exercise mocks. The crate's logic is audited against the C in
//! `audits/backend-executor-nodeSort.md`.
