# Audit: backend-access-rmgrdesc-xactdesc

- **Verdict: PASS**
- Date: 2026-06-13
- Model: Opus 4.8 (1M context) (claude-opus-4-8[1m])
- Branch: port/backend-access-rmgrdesc-xactdesc
- C source: `src/backend/access/rmgrdesc/xactdesc.c`
- c2rust: `pgrust/c2rust-runs/backend-rmgrdesc-extra/src/xactdesc.rs` (also backend-rmgrdesc-path)
- Port: `crates/backend-access-rmgrdesc-xactdesc/src/lib.rs`

## 1. Function inventory & verdicts

xactdesc.c defines 12 functions (3 extern parsers, 7 static describers, 2 extern
rmgr slots). Every one enumerated and compared against the C, the c2rust
rendering, and the port.

| # | C function (xactdesc.c) | Kind | Port location | Verdict | Notes |
|---|---|---|---|---|---|
| 1 | `ParseCommitRecord` | extern | `parse_commit_record` (lib.rs) + `parse_commit_record_seam` | MATCH | Byte-view parser; `xact_time` from off 0; optional `xl_xact_xinfo` gated by `XLOG_XACT_HAS_INFO`; sub-record walk in `parse_commit_abort_body`. Offsets/`MinSizeOf*` verified vs `access/xact.h`. |
| 2 | `ParseAbortRecord` | extern | `parse_abort_record` + `parse_abort_record_seam` | MATCH | Identical walk; `MinSizeOfXactAbort = sizeof(xl_xact_abort) = 8` (struct is `{TimestampTz}`), confirmed in c2rust. |
| 3 | `ParsePrepareRecord` | extern | `parse_prepare_record` | MATCH | Header field offsets (xid@8, database@12, prepared_at@16, nsubxacts@28 … gidlen@54, origin_lsn@56, origin_timestamp@64; sizeof=72) verified vs c2rust `xl_xact_prepare` layout. `MAXALIGN(8)` stepping over GID + each sub-array matches the C `bufptr` advance exactly. GID copied/trimmed at first NUL like `%s`. |
| 4 | `xact_desc_relations` | static | `xact_desc_relations` | MATCH | `if (nrels>0)` guard; `"; %s:"` then `" %s"` per `relpathperm(loc, MAIN_FORKNUM).str`. relpathperm reached via owner seam (unported leaf). |
| 5 | `xact_desc_subxacts` | static | `xact_desc_subxacts` | MATCH | `"; subxacts:"` + `" %u"` per xid. |
| 6 | `xact_desc_stats` | static | `xact_desc_stats` | MATCH | `"; %sdropped stats:"` + `" %d/%u/%llu"`; objid reassembled `(hi<<32)|lo`. Format widths/signedness match (`%d` i32, `%u` u32, `%llu` u64). |
| 7 | `xact_desc_commit` | static | `xact_desc_commit` | MATCH | twophase prefix, `timestamptz_to_str(xlrec->xact_time)` (== parsed.xact_time for commit), rels/subxacts/stats, `standby_desc_invalidations(.., RelcacheInitFileInval(xinfo))`, apply_feedback/sync, origin block. `%X/%X` uppercase preserved. |
| 8 | `xact_desc_abort` | static | `xact_desc_abort` | MATCH | Same ordering as C: rels/subxacts, then origin block, then stats last (note: stats printed after origin, unlike commit). No inval messages (correct — abort has none). |
| 9 | `xact_desc_prepare` | static | `xact_desc_prepare` | MATCH | `"gid %s: "`, rels(commit)/rels(abort)/commit-stats/abort-stats/subxacts, `standby_desc_invalidations(.., xlrec->initfileinval)` with tsId=InvalidOid (C `parsed.tsId` is memset-zero for prepare), origin gated by `origin_id != InvalidRepOriginId`. |
| 10 | `xact_desc_assignment` | static | `xact_desc_assignment` | MATCH | `"subxacts:"` + `" %u"` over `xsub[0..nsubxacts]` (nsubxacts@4, xsub@8). |
| 11 | `xact_desc` | extern | `xact_desc` (rm_desc slot) | MATCH | `info & XLOG_XACT_OPMASK` dispatch over COMMIT/COMMIT_PREPARED, ABORT/ABORT_PREPARED, PREPARE, ASSIGNMENT (`"xtop %u: "`), INVALIDATIONS (nmsgs@0, msgs@4, dbId/tsId InvalidOid, false). Reads data/info/origin via decoded record. |
| 12 | `xact_identify` | extern | `xact_identify` (rm_identify slot) | MATCH | 7-arm switch → symbolic names, `None`/NULL default. Opcode values verified vs `access/xact.h`. |

### Constants verification (against `access/xact.h`)

- Opcodes 0x00/0x10/0x20/0x30/0x40/0x50/0x60, OPMASK 0x70, HAS_INFO 0x80 — match (`types-wal/src/xact.rs`).
- XACT_XINFO flags `1<<0 … 1<<8` and XACT_COMPLETION `1<<29/30/31` with their accessor macros — match.
- Struct/element sizes: TransactionId 4, RelFileLocator 12, xl_xact_stats_item 16, SharedInvalidationMessage 16, xl_xact_xinfo 4, xl_xact_dbinfo 8, xl_xact_twophase 4 — match header field layouts. GIDSIZE 200 — match.
- `MinSizeOf*` macros (Commit/Abort 8, Subxacts/Relfilelocators/StatsItems/Invals 4) — match c2rust constants.

### Edge cases

The port adds bounds-checked byte readers (`bytes_at`, `validate_count`,
`element_offset`, `nul_terminated_len`) absent from the C, which dereferences
the record image directly. This is a hardening superset: on a well-formed
record the reads are identical; on a truncated record the port returns a
`PgError` instead of reading OOB. No behavioral divergence on valid input.
`validate_count` `<= 0 ⇒ 0` mirrors the C `if (n > 0)` describer guards and the
implicit zero-iteration loops in the parsers.

## 2. Seam audit (§3)

Owned seam crate (by C-source coverage of xactdesc.c): `backend-access-rmgrdesc-xactdesc-seams`.
It declares 4 seams: `xact_desc`, `xact_identify`, `parse_commit_record`,
`parse_abort_record`.

- **Initial finding (FAIL):** `init_seams()` installed only `xact_desc` and
  `xact_identify`. `parse_commit_record` / `parse_abort_record` were declared
  and **consumed** by the already-merged `backend-postmaster-walsummarizer`
  (`SummarizeXactRecord`, lib.rs:1278/1293) yet never `set()` — every call would
  panic "seam not set". Per SKILL §3 (uninstalled owned seam) this is an
  automatic FAIL.
- **Fix applied on branch:** added `parse_commit_record_seam` /
  `parse_abort_record_seam` (thin: parse the body, materialize
  `parsed.xlocators[0..nrels]` into an `mcx` `PgVec`, matching the C
  `ParseCommitRecord`/`ParseAbortRecord` consumers that read `parsed.xlocators`;
  WAL summarizer needs them owned beyond the parse). `init_seams()` now installs
  all four declarations. Re-verified: all 4 seams installed; installer is
  nothing but `set()` calls; `seams-init::init_all()` calls it.

Outward seam calls are all justified thin marshal+delegate:
- `standby_desc_invalidations` — direct call to the ported sibling describer
  (`backend-rmgrdesc-next`); no cycle, no seam needed.
- `relpathperm` → `common-relpath-seams::relpathbackend` (owner `common/relpath.c`
  unported — panics until it lands; legitimate unported leaf).
- `timestamptz_to_str` → `backend-utils-adt-timestamp-seams` (owner unported —
  panics until it lands).
No branching/node-construction/computation in any outward seam path.

## 3b. Design conformance

- No invented opacity: the parsers operate on `&[u8]` record images and return
  plain value structs; `XLogReaderState` is the real ported wal type.
- Allocating seam (`parse_*_record`) takes `Mcx` + returns `PgResult<PgVec<..>>`
  per the allocation rule. `append`/`timestamptz_to_str` surface palloc OOM as
  `PgResult`, matching the C `ereport(ERROR)` surface.
- No shared statics, no ambient-global seams, no locks across `?`, no registry
  side tables, no unledgered divergence markers.

## Re-audit after fix

`cargo check -p backend-access-rmgrdesc-xactdesc -p
backend-access-rmgrdesc-xactdesc-seams -p seams-init` clean;
`cargo check -p backend-postmaster-walsummarizer` clean (consumer);
`cargo test -p backend-access-rmgrdesc-xactdesc` → 20 passed.

All 12 functions MATCH; all 4 owned seams installed; zero outstanding seam
findings. **PASS.**
