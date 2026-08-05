# miscfam_diff — status (lane p1-mb-miscfam, 2026-07-31)

Six-crate differential family vs verbatim vendored PostgreSQL 18.3
(Stamp-18.3, upstream 62d6c7d3df): cmdtag, pg_class, earthdistance,
pg_rusage, xlogstats, stringinfo. One target, selector = data[0] % 8
(cmdtag owns arms 0-2). See fuzz/core/src/miscfam_diff.rs header for the
arm map, planes, and domain carves; fuzz/core/csrc/pg_miscfam_io.c for
oracle provenance (assembled byte-exact by scratchpad/assemble_miscfam.sh,
never hand-typed; cmdtaglist.h vendored whole under csrc/miscfam/tcop/).

- [x] Oracle TU assembled (verbatim sed-extracts; pg_strcasecmp vendored
      verbatim from port/pgstrcasecmp.c — the csrc copies are prefixed).
- [x] Driver: all planes (value bytes/bits + verdict + errcode class),
      fc plane for earthdistance (dfmgr wrapper; contrib fn, no fixed oid).
- [x] pg_rusage crate factoring: pg_rusage_show_delta pure core (behavior
      identical); clock-read leg executed no-panic, fixture-injected in C.
- [x] Exhaustive-diff (a0-strength, in-tests + fuzz arms): pg_class full
      u8 relkind domain (256 asserted); cmdtag all 193 tags x flags x
      enum roundtrip (case-flipped) x build_qc modes.
- [x] Local campaign: 12.9M execs, 0 divergences, 0 panics. Corpus
      committed (fuzz/corpus/miscfam_diff) + dict.
- [x] Coverage banked: proofs/coverage/microbatch-miscfam/ (summary.tsv
      equation per crate, 0 unaccounted; lcov.gz). 18 exception rows in
      proofs/coverage/phase1-exceptions.tsv, all DA-absent instrument
      classes (const-eval-only / platform-other / instrument-unmappable).
- [x] Routes rows appended (docs/verification/phase1-routes.tsv).
- [x] Fixed pre-existing branch-wide fuzz link break (missing verbatim
      pg_wchar_strlen in pg_wcharfam.c).
- [ ] Fleet 10M CONFIRM — coordinator submits (one job, this target).
- [ ] Trailing mutants audit (cargo mutants scoped to the six crates) —
      submit at gate-close per skill; does not block.

Hazard notes honored: stringinfo.c vendored verbatim incl. the
MaxAllocSize ceiling (the recorded fabrication hazard); the clamp arm
(lib.rs:104) reached via a gated op-6 zone (magic 0xACE1 + committed seed
seed-si-clamp, lazy ~1GiB reserve — pages untouched). xlogstats/pg_rusage
wrap bands fenced by fixture-validity carves documented in the driver.
