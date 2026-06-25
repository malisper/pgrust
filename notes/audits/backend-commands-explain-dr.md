# Audit: backend-commands-explain-dr

Unit: `backend-commands-explain-dr`
C source: `src/backend/commands/explain_dr.c` (PostgreSQL 18.3)
Port: `crates/backend-commands-explain-dr/src/lib.rs`
c2rust: `../pgrust/c2rust-runs/backend-commands-explain-dr/src/explain_dr.rs`
Branch: `port/backend-commands-explain-dr`

Independent re-derivation from the C source and c2rust translation. The port's
comments / self-review were not trusted.

## Scope

`explain_dr.c` is the `DestReceiver` for `EXPLAIN (SERIALIZE)`: it serializes
result rows into DataRow messages (matching `printtup()`) while measuring
serialization cost, and never sends the data. It is a small TU with 7 function
definitions (2 statics + 1 static-receiver + 4 callbacks; precisely: 5 statics,
2 exported). One struct type (`SerializeDestReceiver`).

## Architecture note (design conformance)

The genuinely-external subsystems the C drives (`getTypeOutputInfo` /
`getTypeBinaryOutputInfo` + `fmgr_info`, `slot_getallattrs` / `tts_values` /
`tts_isnull`, `OutputFunctionCall` / `SendFunctionCall`, the per-row
`tmpcontext` mcxt discipline, `INSTR_TIME_SET_CURRENT`, `pgBufferUsage`) are
routed through a stateful per-receiver `SerializeRuntime` trait. This mirrors the
already-merged sibling `printtup.c` port's `PrinttupRuntime`
(`crates/backend-access-common-printtup/src/lib.rs:170`) exactly — it is the
established repo pattern for a `DestReceiver`'s external surface, not an
own-logic stub. All the TU's own logic (format selection, the text/binary
branches, NULL handling, varlena-header stripping arithmetic, the
timing/buffer-usage accumulation `INSTR_TIME_ACCUM_DIFF` / `BufferUsageAccumDiff`,
the message framing) lives in this crate. The `TupleDesc` is NOT on the trait —
it is passed by reference and read directly (`natts`, `TupleDescAttr`,
`atttypid`). C's `attrinfo != typeinfo` pointer-identity test is reproduced via a
descriptor address token (recorded, never dereferenced).

This crate owns no `-seams` crate (no `backend-commands-explain-dr-seams`
exists), so an empty `init_seams()` is correct — there are no inward seam
declarations to install. Mirrors the `functioncmds` precedent. The
recurrence_guard / seam-wiring guard has no owned-seam obligation to check here.

## Per-function audit

| C function (explain_dr.c) | Port location (lib.rs) | Verdict | Notes |
|---|---|---|---|
| `serialize_prepare_info` (static) | `serialize_prepare_info` L320 | MATCH | Clears old finfos; sets attrinfo identity + nattrs; `nattrs <= 0` early return; palloc0(nattrs) -> `try_reserve` (OOM error); per-col format-0 text (`getTypeOutputInfo`+`fmgr_info` via `prepare_text`), format-1 binary (`getTypeBinaryOutputInfo`+`fmgr_info` via `prepare_binary`), else `ereport(ERROR, ERRCODE_INVALID_PARAMETER_VALUE, "unsupported format code: %d")` -> `with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)`, same format string. |
| `serializeAnalyzeReceive` (static) | `serializeAnalyzeReceive` L384 | MATCH | `natts = typeinfo->natts`; reads `es->timing`/`es->buffers`; conditional `INSTR_TIME_SET_CURRENT(start)` / `instr_start = pgBufferUsage`; re-derive when `attrinfo != typeinfo || nattrs != natts`; `slot_getallattrs`; enter tmpcontext; `pq_beginmessage_reuse(buf, PqMsg_DataRow)`; `pq_sendint16(natts)`; per-col loop reads value then null-check (C order preserved), NULL -> `pq_sendint32(-1)` + continue; text -> `OutputFunctionCall` + `pq_sendcountedtext(strlen)`; binary -> `SendFunctionCall` + `pq_sendint32(VARSIZE-VARHDRSZ)` + `pq_sendbytes(VARDATA, VARSIZE-VARHDRSZ)` (runtime returns the exact payload bytes); NO `pq_endmessage_reuse`; `metrics.bytesSent += buf->len`; exit tmpcontext + reset; conditional `INSTR_TIME_ACCUM_DIFF` and `BufferUsageAccumDiff`; returns `true`. |
| `serializeAnalyzeStartup` (static) | `serializeAnalyzeStartup` L493 | MATCH | `Assert(es != NULL)` (owned borrow); `switch (es->serialize)`: NONE -> `Assert(false)` (`debug_assert!(false)`), TEXT -> format=0, BINARY -> format=1; create tmpcontext (`AllocSetContextCreate`); `initStringInfo(&buf)` -> `StringInfo::new_in(mcx)` returned to caller; `memset(&metrics,0)` + `INSTR_TIME_SET_ZERO` -> `SerializeMetrics::default()`. `operation`/`typeinfo` unused in C (matched). |
| `serializeAnalyzeShutdown` (static) | `serializeAnalyzeShutdown` L540 | MATCH | `pfree(finfos); finfos=NULL` -> clear + attrinfo=None + nattrs=0; `pfree(buf.data)` is caller/runtime-owned (buffer lives in caller); `MemoryContextDelete(tmpcontext)` -> `delete_tmpcontext`. |
| `serializeAnalyzeDestroy` (static) | `serializeAnalyzeDestroy` L559 | MATCH | `pfree(self)` -> consuming `drop`. |
| `CreateExplainSerializeDestReceiver` (exported) | `CreateExplainSerializeDestReceiver` L254 / `::create` L236 | MATCH | `palloc0`; installs 4 callbacks (free fns here); `mydest = DestExplainSerialize` -> `CommandDest::ExplainSerialize`; `self->es = es`; zero remainder. |
| `GetSerializationMetrics` (exported) | `GetSerializationMetrics` L574 | MATCH | `dest->mydest == DestExplainSerialize` -> return metrics; else `memset(&empty,0)` + `INSTR_TIME_SET_ZERO` -> `SerializeMetrics::default()`. A non-serialize/None receiver yields all-zeroes (the C IntoRel-receiver else branch). |

### Constants / types verified

- `PqMsg_DataRow = b'D'` (libpq/protocol.h) — MATCH.
- `format` int8: 0=text, 1=binary — MATCH (c2rust `(*receiver).format = 0/1 as int8`).
- `ERRCODE_INVALID_PARAMETER_VALUE` + format string `"unsupported format code: %d"` — MATCH.
- `EXPLAIN_SERIALIZE_NONE/TEXT/BINARY` -> `ExplainSerializeOption` (types-explain) — MATCH.
- `SerializeMetrics { bytesSent:u64, timeSpent:instr_time, bufferUsage:BufferUsage }`, zeroed by `Default` (matches memset + INSTR_TIME_SET_ZERO) — MATCH.
- `BufferUsageAccumDiff`: field-by-field `dst += add - sub` over all 10 block counters + 6 instr_time members — MATCH (verified against the BufferUsage layout used in instrument.c).

### Helper arithmetic (ported inline, not seamed — correct)

- `instr_time_accum_diff` = `INSTR_TIME_ACCUM_DIFF(x, y, z)` (`x += y - z`) — MATCH.
- `buffer_usage_accum_diff` = `BufferUsageAccumDiff` — MATCH.
- `descriptor_identity` reproduces C's `TupleDesc` pointer-identity compare without dereferencing — behaviour-preserving.

## Seam / wiring audit

- No owned `-seams` crate -> `init_seams()` is empty -> correct (no inward seam
  declarations exist for this TU; callers depend on the crate directly when they
  land). `seams-init::init_all()` does not list this crate, which is correct for
  a crate with an empty installer and no owned seam declarations (the
  seam-wiring guard only flags crates that *install* seams but are unlisted).
- The `SerializeRuntime` trait is a stateful per-receiver external interface
  (sibling-`PrinttupRuntime` pattern), not an ambient-global seam registry; each
  method is thin marshal+delegate with a fail-safe default. No branching/node
  construction/computation hidden behind it. No finding.

## No-stub / no-deferral check

No `todo!()`, `unimplemented!()`, own-logic stub, or deferral-error escape. The
trait method defaults are external-callee fallbacks (mirroring printtup), not
absent own-logic. Every C control-flow path has a counterpart.

## Gates

- `cargo check --workspace` — PASS
- `cargo test -p backend-commands-explain-dr` — PASS (7 tests: create/mydest,
  startup format+zeroed metrics+shutdown, binary lengths+byte-count,
  null-column -1+accumulation, timing+buffers accumulation, unsupported-format
  error, GetSerializationMetrics other-receiver)
- `cargo test -p seams-init` — PASS (recurrence_guard / seam-wiring guards green)

## Verdict: PASS

All 7 functions MATCH. Zero seam findings, zero design-conformance findings, no
stubs or deferrals. CATALOG row set to `audited`.
