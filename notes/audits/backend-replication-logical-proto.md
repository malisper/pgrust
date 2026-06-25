# Audit: backend-replication-logical-proto

- C source: `src/backend/replication/logical/proto.c` (PostgreSQL 18.3, 1301 lines)
- Owned header vocabulary: `src/include/replication/logicalproto.h`
- c2rust: `c2rust-runs/backend-replication-logical-proto/src/proto.rs`
- Port: `crates/backend-replication-logical-proto/src/lib.rs`
- Branch: `port/backend-replication-logical-proto`

## Inventory

The C file defines 46 functions: 38 externally-visible (the exact set c2rust
emitted — c2rust inlined the statics into their callers but they all survive in
the port) plus 8 file-local helpers (`logicalrep_write_attrs`,
`logicalrep_write_tuple`, `logicalrep_read_attrs`, `logicalrep_read_tuple`,
`logicalrep_write_namespace`, `logicalrep_read_namespace`,
`logicalrep_write_prepare_common`, `logicalrep_read_prepare_common`). All
present in the port.

## Constants verified against headers

| Constant | Header | Value | Port | OK |
|---|---|---|---|---|
| LOGICAL_REP_MSG_* (19 tags) | logicalproto.h | 'B' 'C' 'O' 'I' 'U' 'D' 'T' 'R' 'Y' 'M' 'b' 'P' 'K' 'r' 'S' 'E' 'c' 'A' 'p' | `LogicalRepMsgType` enum discriminants | yes |
| LOGICALREP_COLUMN_NULL/UNCHANGED/TEXT/BINARY | logicalproto.h | 'n' 'u' 't' 'b' | identical | yes |
| LOGICALREP_IS_REPLICA_IDENTITY | proto.c | 1 | 1 | yes |
| MESSAGE_TRANSACTIONAL / TRUNCATE_CASCADE | proto.c | 1<<0 | 1<<0 | yes |
| TRUNCATE_RESTART_SEQS | proto.c | 1<<1 | 1<<1 | yes |
| PG_CATALOG_NAMESPACE | pg_namespace.dat | 11 | 11 | yes |
| Anum_pg_type_typname / typnamespace / typoutput / typsend | pg_type.h column order | 2 / 3 / 17 / 19 | 2 / 3 / 17 / 19 | yes (counted from CATALOG incl. oid) |
| VARTAG_ONDISK | varatt.h | 18 | 18 | yes |
| FirstLowInvalidHeapAttributeNumber | sysattr.h | -7 | (from types) | yes |
| ATTRIBUTE_GENERATED_STORED | pg_attribute.h | 's' | (from types_tuple) | yes |
| REPLICA_IDENTITY_DEFAULT/FULL/INDEX | pg_class.h | 'd' 'f' 'i' | (from types_tuple) | yes |
| RBTXN_IS_PREPARED | reorderbuffer.h | 0x0040 | 0x0040 | yes |

## Per-function table

| C function (line) | Port location | Verdict | Notes |
|---|---|---|---|
| logicalrep_write_begin (48) | lib.rs:381 | MATCH | byte, 2×int64, int32; xact_time union → single TimestampTz field |
| logicalrep_read_begin (62) | lib.rs:395 | MATCH | final_lsn==Invalid elog preserved |
| logicalrep_write_commit (77) | lib.rs:410 | MATCH | flags=0, byte+byte+3×int64 |
| logicalrep_read_commit (97) | lib.rs:430 | MATCH | flags!=0 elog |
| logicalrep_write_begin_prepare (115) | lib.rs:452 | MATCH | gid sent unconditionally via txn_gid |
| logicalrep_read_begin_prepare (133) | lib.rs:471 | MATCH | two Invalid-LSN elogs; strlcpy_gid |
| logicalrep_write_prepare_common (154, static) | lib.rs:498 | MATCH | 3 Asserts → debug_assert (gid/is_prepared/xid valid) |
| logicalrep_write_prepare (186) | lib.rs:531 | MATCH | delegates to common w/ PREPARE |
| logicalrep_read_prepare_common (198, static) | lib.rs:541 | MATCH | flags, 2 Invalid-LSN, Invalid-xid elogs; msgtype interpolated |
| logicalrep_read_prepare (227) | lib.rs:582 | MATCH | "prepare" |
| logicalrep_write_commit_prepared (236) | lib.rs:592 | MATCH | Assert gid; 3×int64+int32+gid |
| logicalrep_read_commit_prepared (266) | lib.rs:623 | MATCH | flags + 2 Invalid-LSN elogs |
| logicalrep_write_rollback_prepared (292) | lib.rs:661 | MATCH | 4×int64 (prepare_end,end,prepare_time,commit_time)+int32+gid |
| logicalrep_read_rollback_prepared (324) | lib.rs:694 | MATCH | flags + 2 Invalid-LSN elogs; 2 timestamps |
| logicalrep_write_stream_prepare (352) | lib.rs:733 | MATCH | common w/ STREAM_PREPARE |
| logicalrep_read_stream_prepare (364) | lib.rs:742 | MATCH | "stream prepare" |
| logicalrep_write_origin (373) | lib.rs:751 | MATCH | byte+int64+string |
| logicalrep_read_origin (389) | lib.rs:768 | MATCH | pstrdup(getmsgstring) → pstrdup_msgstring |
| logicalrep_write_insert (402) | lib.rs:781 | MATCH | xid only if valid; relid; 'N'; write_tuple |
| logicalrep_read_insert (427) | lib.rs:805 | MATCH | action!='N' elog "expected new tuple but got %d" (numeric, matches) |
| logicalrep_write_update (449) | lib.rs:824 | MATCH | relreplident assert; 'O'/'K' branch on FULL; oldslot optional |
| logicalrep_read_update (486) | lib.rs:864 | MATCH | action validation + has_oldtuple; %c formatting |
| logicalrep_write_delete (527) | lib.rs:907 | MATCH | assert; 'O'/'K' on FULL |
| logicalrep_read_delete (560) | lib.rs:942 | MATCH | action must be 'O'/'K' |
| logicalrep_write_truncate (582) | lib.rs:965 | MATCH | flags from cascade/restart; loop over relids |
| logicalrep_read_truncate (614) | lib.rs:1001 | MATCH | negative nrelids → empty loop; vec capacity clamped via max(0); List→PgVec |
| logicalrep_write_message (639) | lib.rs:1023 | MATCH | flags, lsn, prefix, sz, bytes[..sz] |
| logicalrep_write_rel (667) | lib.rs:1055 | MATCH | namespace+relname+replident+attrs |
| logicalrep_read_rel (697) | lib.rs:1086 | MATCH | palloc struct → owned LogicalRepRelation in mcx |
| logicalrep_write_typ (722) | lib.rs:1119 | MATCH | getBaseType seam; cache lookup fail elog; GETSTRUCT fields via SysCacheGetAttrNotNull (NOT NULL cols, never fires) |
| logicalrep_read_typ (753) | lib.rs:1155 | MATCH | remoteid+nspname+typname |
| logicalrep_write_tuple (766, static) | lib.rs:1170 | MATCH | nliveatts count; slot_getallattrs seam; NULL/UNCHANGED(toast)/binary/text; send len = VARSIZE-VARHDRSZ == seam payload len; cache-fail elog |
| logicalrep_read_tuple (860, static) | lib.rs:1257 | MATCH | natts u16; per-col kind switch; len read; unknown-kind elog. palloc(len+1)+NUL omitted (sentinel invisible past len) — byte content identical |
| logicalrep_write_attrs (920, static) | lib.rs:1314 | MATCH | nliveatts; idattrs via relcache seam unless FULL; LOGICALREP_IS_REPLICA_IDENTITY flag; attnum-FirstLowInvalid offset; bms_free→drop |
| logicalrep_read_attrs (984, static) | lib.rs:1381 | MATCH | per-att flags/replica-id bms_add_member; name/typ; mode read+ignored |
| logicalrep_write_namespace (1026, static) | lib.rs:1418 | MATCH | pg_catalog → '\0'; else get_namespace_name seam, NULL→elog |
| logicalrep_read_namespace (1046, static) | lib.rs:1438 | MATCH | empty → "pg_catalog" |
| logicalrep_write_stream_start (1060) | lib.rs:1453 | MATCH | Assert xid valid; int32+first_segment byte |
| logicalrep_read_stream_start (1078) | lib.rs:1471 | MATCH | first_segment = byte==1 |
| logicalrep_write_stream_stop (1094) | lib.rs:1484 | MATCH | single byte |
| logicalrep_write_stream_commit (1103) | lib.rs:1490 | MATCH | xid+flags+3×int64 |
| logicalrep_read_stream_commit (1128) | lib.rs:1516 | MATCH | xid; flags!=0 elog; fields |
| logicalrep_write_stream_abort (1157) | lib.rs:1543 | MATCH | xid+subxid; optional abort_lsn/time |
| logicalrep_read_stream_abort (1183) | lib.rs:1569 | MATCH | else-branch zeroes abort_lsn=Invalid, abort_time=0 |
| logicalrep_message_type (1208) | lib.rs:1595 | MATCH | 19 cases → strings; unknown → "??? (%d)" (no throw) |
| logicalrep_should_publish_column (1278) | lib.rs:1637 | MATCH | dropped→false; column list→bms_is_member(attnum); non-gen→true; STORED gen→include==Stored; else false |

## Edge-case re-derivations (spot-check of MATCH verdicts)

- **Binary length (write_tuple):** C `len = VARSIZE(outputbytes) - VARHDRSZ`,
  sends `len` then `VARDATA(outputbytes)`. The `oid_send_function_call` seam
  contract returns the payload with the varlena header already stripped
  (`VARSIZE - VARHDRSZ` bytes), so `outputbytes.len()` equals C's `len` and the
  same bytes follow. Identical wire output.
- **Counted text (write_tuple):** C `pq_sendcountedtext(out, outputstr,
  strlen(outputstr))`; the seam returns NUL-excluded bytes and
  `pq_sendcountedtext(out, &outputstr)` computes the length internally — equal.
- **Negative/large length on read:** C `pq_copymsgbytes` rejects negative or
  over-long len with a protocol error; the port's `pq_getmsgbytes(len as usize)`
  rejects the same inputs (`as usize` of a negative i32 is huge → fails the
  remaining-bytes check). Both error.
- **GETSTRUCT vs SysCacheGetAttrNotNull (write_typ / write_tuple):** typname,
  typnamespace, typoutput, typsend are NOT NULL fixed-width catalog columns, so
  the NotNull accessor never raises where C's raw GETSTRUCT would silently read;
  behavior identical on all valid tuples.
- **xact_time union:** C reads `.commit_time`/`.prepare_time`/`.abort_time` of a
  union; all members share storage. The port's single `xact_time: TimestampTz`
  field carries the meaningful value — identical bytes sent.

## Seam audit

`init_seams()` is empty and is registered by `seams-init` (lib.rs:30). The crate
declares no seams of its own (no cyclic callers depend on proto yet) — correct.
All outward seam calls are thin marshal+delegate to genuinely-foreign owners,
each a real dependency a low-level protocol crate cannot take directly:

- `bms_is_member`, `bms_add_member` → backend-nodes-core (bitmapset)
- `get_base_type`, `get_namespace_name` → lsyscache
- `relation_get_identity_key_bitmap` → relcache
- `oid_send_function_call`, `oid_output_function_call` → fmgr
- `slot_getallattrs` → execTuples

No branching, node construction, or computation lives in any seam path; all
logic (flag encoding, column selection, count loops, error predicates) is in
this crate. No function body was replaced by a seam-to-elsewhere.

## Design conformance

- Allocating functions/readers take `Mcx` and return `PgResult`; owned
  `PgVec`/`PgBox`/`StringInfo` replace pallocs. OK.
- No invented opacity, no shared statics, no ambient-global seams, no locks, no
  registry side tables, no unledgered divergence markers.
- `gid` modeled as `Option<PgVec>` (C `char *` NULL-or-set); the unconditional
  two-phase dereferences use `txn_gid` which panics on an unset gid, matching C's
  unchecked pointer deref (caller bug, not an error path). OK.

## Build / tests

`cargo build -p backend-replication-logical-proto` clean; `cargo test`
17/17 pass.

## Verdict: PASS

All 46 functions MATCH; constants verified against headers; zero seam findings;
no design-conformance violations.
