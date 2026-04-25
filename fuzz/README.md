# pgrust fuzzing

This directory is for `cargo-fuzz` targets and supporting corpora.

Planned first targets:

- `parse` for raw SQL/parser input
- `startup_packet` for PostgreSQL startup packet decoding

Expected layout:

```text
fuzz/
├── Cargo.toml
├── fuzz_targets/
│   ├── parse.rs
│   └── startup_packet.rs
└── corpus/
    ├── parse/
    └── startup_packet/
```

Validation rule:

- every target must have a 30-second smoke command
- any discovered crash must become a deterministic regression case in the main
  repo tests

Smoke commands:

- `cargo +nightly fuzz run parse -- -max_total_time=30`
- `cargo +nightly fuzz run startup_packet -- -max_total_time=30`

Smoke status as of Apr 24, 2026:

- `startup_packet` ran for 10 seconds, 187,083 executions, no crash.
- `parse` ran for 10 seconds, 46,249 executions, no crash.
