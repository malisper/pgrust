# Audit: backend-utils-adt-ts-index-rank

- Date: 2026-06-12
- Model: Opus 4.8 (claude-opus-4-8[1m])
- Verdict: **PASS**
- C sources (PostgreSQL 18.3): `tsginidx.c`, `tsgistidx.c`, `tsrank.c`
  (`src/backend/utils/adt/`)
- Port crates: `backend-utils-adt-tsginidx`, `backend-utils-adt-tsgistidx`,
  `backend-utils-adt-tsrank`
- This is an independent re-audit (the unit passed an earlier workflow audit
  whose report was never committed; everything below was re-derived from the C,
  the c2rust rendering, and the headers).

## Method

Full function inventory built from the three C files and cross-checked against
`../pgrust/c2rust-runs/backend-utils-adt-ts-index-rank/src/{tsginidx,tsgistidx,tsrank}.rs`
(c2rust function lists match the C inventories; no function was dropped by the
build config). Every function compared on control flow, error paths, constants
(verified against `tsearch/ts_type.h`, `tsearch/ts_utils.h`, `access/gin.h`,
`access/gist.h`, `storage/bufpage.h`, `access/heaptoast.h`,
`access/htup_details.h`), and edge cases. `cargo check` on the three crates is
clean.

## Constants verified against headers

- `WEP_GETWEIGHT = x>>14`, `WEP_GETPOS = x&0x3fff`, `MAXENTRYPOS = 1<<14`,
  `MAXSTRPOS = (1<<20)-1`, `MAXSTRLEN = (1<<11)-1` — match `ts_type.h`.
- `QI_VAL=1`, `QI_OPR=2`, `QI_VALSTOP=3`; `OP_NOT=1/AND=2/OR=3/PHRASE=4` — match.
- `GIN_FALSE/TRUE/MAYBE = 0/1/2`; `GIN_SEARCH_MODE_DEFAULT=0`, `_ALL=2` — match.
- `TS_EXEC_EMPTY=0`, `TS_EXEC_PHRASE_NO_POS=0x02` — match.
- GiST: `SIGLEN_DEFAULT=31*4`; `GISTPageOpaqueData=16`,
  `SizeOfPageHeaderData=24`, `ItemIdData=4`, `IndexTupleData=8`, `BLCKSZ=8192`,
  `GISTMaxIndexTupleSize=MAXALIGN_DOWN((BLCKSZ-24-16)/4-4)`,
  `SIGLEN_MAX=GISTMaxIndexTupleSize-MAXALIGN(8)`,
  `TOAST_INDEX_TARGET=MaxHeapTupleSize/16` — all match.
- `NUM_WEIGHTS=4`, `default_weights={0.1,0.2,0.4,1.0}`, `RANK_NORM_*` bits
  `0x01..0x20`, `DEF_NORM_METHOD=RANK_NO_NORM` — match.

## Per-function table

### tsginidx.c (`backend-utils-adt-tsginidx`)

| C function (line) | Port location | Verdict | Notes |
|---|---|---|---|
| `gin_cmp_tslexeme` (24) | lib.rs:193 | MATCH | delegates to `ts_compare_string` seam (false) |
| `gin_cmp_prefix` (40) | lib.rs:200 | MATCH | prefix compare; `cmp<0 -> 1` "prevent continue scan" |
| `gin_extract_tsvector` (64) | lib.rs:211 | MATCH | one text key per lexeme; `*nentries==size` |
| `gin_extract_tsquery` (94) | lib.rs:246 | MATCH | VAL count, partialmatch, item->operand map, searchMode |
| `checkcondition_gin` (183) | lib.rs:324 | MATCH | TRUE->MAYBE when weight!=0 or data present |
| `gin_tsquery_consistent` (214) | lib.rs:357 | MATCH | bool[]->GinTernaryValue reinterpret; TS_execute_ternary seam |
| `gin_tsquery_triconsistent` (263) | lib.rs:405 | MATCH | ternary check; GIN/TS value equivalence |
| `gin_extract_tsvector_2args` (304) | lib.rs:449 | MATCH | post-dispatch tail-call to primary |
| `gin_extract_tsquery_5args` (316) | lib.rs:457 | MATCH | " |
| `gin_tsquery_consistent_6args` (328) | lib.rs:465 | MATCH | " |
| `gin_extract_tsquery_oldsig` (340) | lib.rs:475 | MATCH | " |
| `gin_tsquery_consistent_oldsig` (350) | lib.rs:483 | MATCH | " |

`tsCompareString`, `tsquery_requires_match`, `TS_execute_ternary` are owned by
`tsvector_op.c` and SEAMED through `backend-utils-adt-tsvector-core-seams`.

### tsgistidx.c (`backend-utils-adt-tsgistidx`)

| C function (line) | Port location | Verdict | Notes |
|---|---|---|---|
| `gtsvectorin` (89) | lib.rs:213 | MATCH | always errors, `ERRCODE_FEATURE_NOT_SUPPORTED` |
| `gtsvectorout` (100) | lib.rs:220 | MATCH | "N unique words" / "all true bits" / "N true, M false" |
| `compareint` (126) | lib.rs:191 | MATCH | `pg_cmp_s32` |
| `makesign` (135) | lib.rs:196 | MATCH | MemSet 0 then HASH each arr element |
| `gtsvector_alloc` (147) | lib.rs:144 | MATCH | ARRKEY/SIGNKEY/ALLISTRUE; sign copy only for plain SIGNKEY |
| `gtsvector_compress` leaf (169) | lib.rs:244 | MATCH | crc per lexeme, sort, qunique, signature-if-too-long |
| `gtsvector_compress` inner (220) | lib.rs:294 | MATCH | all-0xff SIGNKEY -> ALLISTRUE |
| `gtsvector_decompress` (243) | (n/a) | MATCH | pure detoast+GISTENTRY repack; no computational body (documented) |
| `checkcondition_arr` (276) | lib.rs:319 | MATCH | prefix->MAYBE; binary search on int32 hashes |
| `checkcondition_bit` (308) | lib.rs:346 | MATCH | prefix->MAYBE; GETBIT(HASHVAL) |
| `gtsvector_consistent` (325) | lib.rs:369 | MATCH | recheck always true; signkey/alltrue/leaf branches; TS_execute seam |
| `unionkey` (365) | lib.rs:410 | MATCH | OR sign / HASH arr; returns 1 on ALLISTRUE add |
| `gtsvector_union` (393) | lib.rs:432 | MATCH | OR all entries; ALLISTRUE shortcut + break |
| `gtsvector_same` (420) | lib.rs:457 | MATCH | signkey alltrue/byte-eq; arrkey len+elem-eq |
| `sizebitvec` (481) | lib.rs:504 | MATCH | popcount over siglen bytes |
| `hemdistsign` (487) | lib.rs:513 | MATCH | XOR popcount |
| `hemdist` (503) | lib.rs:525 | MATCH | ALLISTRUE shortcuts then hemdistsign |
| `gtsvector_penalty` (524) | lib.rs:551 | MATCH | arrkey: makesign + alltrue/hemdistsign; else hemdist |
| `fillcache` (567) | lib.rs:597 | MATCH | arrkey makesign / alltrue flag / memcpy sign |
| `comparecost` (586) | lib.rs:623 | MATCH | `pg_cmp_s32` on cost |
| `hemdistcache` (596) | lib.rs:628 | MATCH | alltrue shortcuts then hemdistsign |
| `gtsvector_picksplit` (612) | lib.rs:660 | MATCH | seed search, costvector sort, left/right assignment, WISH_F balancing |
| `gtsvector_consistent_oldsig` (794) | lib.rs:848 | MATCH | forwards to consistent |
| `gtsvector_options` (800) | lib.rs:871 | MATCH | init_local_reloptions + add_local_int_reloption (siglen, 1, SIGLEN_MAX) |

Helper `qunique_i32` (lib.rs:891) faithfully implements `lib/qunique.h` over a
sorted int32 slice. `legacy_crc32_lexeme` (pg_crc.h),
`init_local_reloptions`/`add_local_int_reloption` (reloptions.c), and the
`TS_execute` engine are SEAMED to their owners.

### tsrank.c (`backend-utils-adt-tsrank`)

| C function (line) | Port location | Verdict | Notes |
|---|---|---|---|
| `word_distance` (45) | lib.rs:222 | MATCH | `w>100 -> 1e-30`; double-promoted exp body, float4 cast of w |
| `cnt_length` (54) | lib.rs:230 | MATCH | per-entry POSDATALEN (0 counts as 1) |
| `find_wordentry` (87) | lib.rs:247 | MATCH | binary search + prefix scan; nitem out-param |
| `compareQueryOperand` (136) | lib.rs:296 | MATCH | tsCompareString on operand strings |
| `SortAndUniqItems` (155) | lib.rs:311 | MATCH | collect VAL, sort, dedup; carries (item idx, operand) |
| `calc_rank_and` (201) | lib.rs:380 | MATCH | POSNULL weight=MAXENTRYPOS-1; pairwise dist/word_distance; res combine |
| `calc_rank_or` (284) | lib.rs:467 | MATCH | POSNULL pos=0; per-entry resj/wjm; /1.64493406685; /size |
| `calc_rank` (358) | lib.rs:524 | MATCH | AND/PHRASE->and else or; res<0->1e-20; all RANK_NORM_* branches |
| `getWeights` (405) | lib.rs:579 | MATCH | too-short (ARRAY_SUBSCRIPT_ERROR), default for neg, >1 out-of-range; ndim/nulls in seam |
| `ts_rank_wttf` (439) | lib.rs:604 | MATCH | getWeights + calc_rank(method) |
| `ts_rank_wtt` (458) | lib.rs:611 | MATCH | getWeights + calc_rank(DEF_NORM_METHOD) |
| `ts_rank_ttf` (476) | lib.rs:618 | MATCH | default_weights + calc_rank(method) |
| `ts_rank_tt` (491) | lib.rs:623 | MATCH | default_weights + calc_rank(DEF_NORM_METHOD) |
| `compareDocR` (524) | lib.rs:705 | MATCH | (pos, weight, entry) ordering |
| `checkcondition_QueryOperand` (568) | lib.rs:731 | MATCH | operandexists; npos/pos with reverseinsert offset |
| `resetQueryRepresentation` (598) | lib.rs:758 | MATCH | clears exists/npos, sets reverseinsert |
| `fillQueryRepresentationData` (611) | lib.rs:767 | MATCH | per-item insert with reverse/forward lastPos logic |
| `Cover` (651) | lib.rs:814 | MATCH | check_stack_depth seam; up/down bound scan; recursion |
| `get_docrep` (732) | lib.rs:895 | MATCH | build pos stream, sort, join per (pos,entry); NULL when empty |
| `calc_rank_cd` (855) | lib.rs:1000 | MATCH | invws, Cover loop, Cpos/nNoise/SumDist, all RANK_NORM_* branches |
| `ts_rankcd_wttf` (958) | lib.rs:1099 | MATCH | getWeights + calc_rank_cd(method) |
| `ts_rankcd_wtt` (977) | lib.rs:1106 | MATCH | getWeights + calc_rank_cd(DEF_NORM_METHOD) |
| `ts_rankcd_ttf` (995) | lib.rs:1113 | MATCH | default_weights + calc_rank_cd(method) |
| `ts_rankcd_tt` (1010) | lib.rs:1118 | MATCH | default_weights + calc_rank_cd(DEF_NORM_METHOD) |

`tsCompareString` and the `TS_execute` engine (tsvector_op.c),
`deconstruct_float4_array` (arrayfuncs.c; carries the `ARR_NDIM != 1` and
`array_contains_nulls` checks with the exact SQLSTATEs/messages, which are
array-subsystem logic), and `check_stack_depth`/`CHECK_FOR_INTERRUPTS`
(tcop/postgres.c) are SEAMED to their owners.

## Spot-checks (re-derived in detail)

- `gin_extract_tsquery`: `map_item_operand` sized `query->size` indexed by item
  index `i`; `entries`/`partialmatch` indexed by operand counter `j` — Rust
  reproduces both index spaces exactly.
- `calc_rank_or` accumulation `res + (wjm + resj - wjm/((jm+1)^2))/1.644934...`
  with C's float-then-double promotion — Rust matches (`as f64` only on the
  divide, inner expr in f32).
- `word_distance` double-promoted body with `(float4) w` cast — Rust
  `w as f32 as f64` reproduces it.
- `gtsvector_compress` "too long" predicate: C `VARSIZE(res) > TOAST_INDEX_TARGET`
  equals `CALCGTSIZE(ARRKEY, len)` after the repalloc (and equals it when no
  collision), so the Rust `CALCGTSIZE(ARRKEY, len) > TOAST_INDEX_TARGET` is
  behaviorally identical in both branches.

## Seam and design audit

- Owned inward seam crates (per C-source coverage: tsginidx/tsgistidx/tsrank):
  **none** exist. These are leaf adt functions with no cyclic callers, so there
  is no `init_seams()` to populate and no `seams-init` change is required. Per
  the skill, an empty installer is only a FAIL when owned seam crates are
  outstanding — none are.
- Outward seams (`tsvector-core`, `array-more`, `hash-small`, reloptions, tcop)
  each correspond to a real other unit's owned C function (tsvector_op.c,
  arrayfuncs.c, pg_crc.h, reloptions.c, postgres.c) and are thin
  marshal+delegate declarations with no branching/computation in the seam path.
  No function body was replaced by a seam to "somewhere else" — all in-unit
  logic lives in the crates.
- Allocating functions take `Mcx<'mcx>` and return `PgResult`. The only owned
  `Vec` allocations are the GiST `SignTsVector` key payloads, recorded as
  DESIGN_DEBT pending the GiST-AM consumer (infallible per the C model), and the
  `get_docrep` result Vec which uses `try_reserve` + `mcx.oom`. No shared statics
  for per-backend globals, no ambient-global seams, no registry side tables.

## Conclusion

Every function MATCHes (or is correctly SEAMED). All audited constants match the
PostgreSQL 18.3 headers. No MISSING / PARTIAL / DIVERGED functions. Seam wiring
and design conformance are clean. **PASS.**
