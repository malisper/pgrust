# pgrust Antithesis scaffolding

This directory is for Antithesis-specific assets, not general-purpose tests.

Current layout:

```text
antithesis/
├── Dockerfile
├── README.md
├── setup-complete.sh
├── config/
│   └── docker-compose.yaml
└── test/
    └── main/
        └── README.md
```

Use this area for:

- container and workload layout
- SDK integration notes
- platform-specific orchestration files
- invariant checks that should run both locally and on-platform

## What we can do before Antithesis replies

We do not need platform access to make useful progress. Local prep falls into
four buckets:

1. Container shape
   Put the server and workload in stable files so we know how the system will
   boot, connect, and shut down.
2. Workload shape
   Define one small common-application scenario with multiple sessions:
   create table, insert rows, update, delete, reconnect, and verify final
   state.
3. Assertion shape
   Decide which invariants matter enough to check everywhere:
   acknowledged commit survives restart, row counts stay sane, and protocol
   failures do not poison later work.
4. Local SDK shape
   When we wire in `antithesis-sdk-rust`, use `ANTITHESIS_SDK_LOCAL_OUTPUT`
   first so the same assertions can be exercised locally.

## Validation rule

- local dry runs must work before any platform submission
- if we cannot run a reduced local version, the setup is too vague
- placeholder files are fine now, but each should eventually correspond to a
  real local command
