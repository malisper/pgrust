# Logic audit: backend-access-nbt-compare

- C: `/Users/malisper/workspace/work/pgrust/postgres-18.3/src/backend/access/nbtree/nbtcompare.c`
- c2rust: `/Users/malisper/workspace/work/pgrust/c2rust-runs/backend-access-nbt-compare/src/nbtcompare.rs`
- Port: `/Users/malisper/workspace/work/pgrust-fabled/.claude/worktrees/agent-aeebd1af93738e95b/crates/backend-access-nbt-compare/src/lib.rs`

Method: re-derived 3-way comparison logic, branch order, overflow flags, and
constants from C. Verified `A_LESS_THAN_B = -1`, `A_GREATER_THAN_B = 1`
(production, not `STRESS_SORT_INT_MIN`). Confirmed all install seams exist in
`backend-access-nbt-compare-seams` (sortsupport int2/int4/int8/oid; skipsupport
bool/int2/int4/int8/oid/char). `oidvector.dim1` confirmed `c_int` (int32) in
c2rust struct, so the length-difference subtraction is int32.

| # | Function | Verdict | Notes |
|---|----------|---------|-------|
| 1 | `btboolcmp` | MATCH | `(a as i32) - (b as i32)`, mirrors `(int32)a - (int32)b`. |
| 2 | `bool_decrement` | MATCH | underflow when `!bexisting` (C: `==false`); returns `(null,true)`; else `BoolGetDatum(false)`. |
| 3 | `bool_increment` | MATCH | overflow when `bexisting` (C: `==true`); else `BoolGetDatum(true)`. |
| 4 | `btboolskipsupport` | MATCH/SEAMED | sets low=false/high=true; fn-ptr install via `install_skipsupport_bool` (substrate-owned). |
| 5 | `btint2cmp` | MATCH | `(a as i32)-(b as i32)`, widened subtraction. |
| 6 | `btint2fastcmp` | MATCH | DatumGetInt16 + `(int)a-(int)b`. |
| 7 | `btint2sortsupport` | MATCH/SEAMED | installs `btint2fastcmp` via seam. |
| 8 | `int2_decrement` | MATCH | underflow at `i16::MIN` (PG_INT16_MIN); else `-1`. |
| 9 | `int2_increment` | MATCH | overflow at `i16::MAX`; else `+1`. |
| 10 | `btint2skipsupport` | MATCH/SEAMED | low=MIN/high=MAX; seam install. |
| 11 | `btint4cmp` | MATCH | `>`/`==`/else 3-way, returns GT/0/LT. No `a-b` overflow risk. |
| 12 | `ssup_datum_int32_cmp` | MATCH | mirrors shared int32 fast cmp 3-way. |
| 13 | `btint4sortsupport` | MATCH/SEAMED | installs `ssup_datum_int32_cmp` via seam. |
| 14 | `int4_decrement` | MATCH | underflow at `i32::MIN`. |
| 15 | `int4_increment` | MATCH | overflow at `i32::MAX`. |
| 16 | `btint4skipsupport` | MATCH/SEAMED | low=MIN/high=MAX; seam install. |
| 17 | `btint8cmp` | MATCH | 64-bit 3-way GT/0/LT. |
| 18 | `ssup_datum_signed_cmp` | MATCH | 64-bit signed 3-way; SIZEOF_DATUM>=8 path. `btint8fastcmp` (SIZEOF_DATUM<8) correctly omitted as unreachable on target. |
| 19 | `btint8sortsupport` | MATCH/SEAMED | installs signed cmp via seam (>=8-byte Datum). |
| 20 | `int8_decrement` | MATCH | underflow at `i64::MIN`. |
| 21 | `int8_increment` | MATCH | overflow at `i64::MAX`. |
| 22 | `btint8skipsupport` | MATCH/SEAMED | low=MIN/high=MAX; seam install. |
| 23 | `btint48cmp` | MATCH | a(i32)->i64 then 3-way vs b(i64); mirrors C int promotion `(int32)a > (int64)b`. |
| 24 | `btint84cmp` | MATCH | b(i32)->i64 then 3-way. |
| 25 | `btint24cmp` | MATCH | a(i16)->i32 then 3-way. |
| 26 | `btint42cmp` | MATCH | b(i16)->i32 then 3-way. |
| 27 | `btint28cmp` | MATCH | a(i16)->i64 then 3-way. |
| 28 | `btint82cmp` | MATCH | b(i16)->i64 then 3-way. |
| 29 | `btoidcmp` | MATCH | unsigned Oid 3-way GT/0/LT. |
| 30 | `btoidfastcmp` | MATCH | DatumGetObjectId + unsigned 3-way. |
| 31 | `btoidsortsupport` | MATCH/SEAMED | installs `btoidfastcmp` via seam. |
| 32 | `oid_decrement` | MATCH | underflow at `InvalidOid` (0); else `-1`. |
| 33 | `oid_increment` | MATCH | overflow at `OID_MAX` (=u32::MAX=UINT_MAX); else `+1`. |
| 34 | `btoidskipsupport` | MATCH/SEAMED | low=InvalidOid/high=OID_MAX; seam install. |
| 35 | `btoidvectorcmp` | MATCH/SEAMED | sort first by length: `a.len() as i32 - b.len() as i32` (dim1 is int32); then unsigned per-element 3-way; returns 0 on full equality. `check_valid_oidvector` (header validation) delegated to fmgr seam caller, matching the doc-noted contract. |
| 36 | `btcharcmp` | MATCH | unsigned compare: `(a as u8 as i32) - (b as u8 as i32)`, mirrors `(int32)(uint8)a - (int32)(uint8)b`. |
| 37 | `char_decrement` | MATCH | underflow at 0; else `(uint8)c - 1`. |
| 38 | `char_increment` | MATCH | overflow at `UCHAR_MAX` (255); else `+1`. |
| 39 | `btcharskipsupport` | MATCH/SEAMED | low=0/high=UCHAR_MAX (unsigned); seam install. |

Constants verified against headers/catalog:
- `A_LESS_THAN_B=-1`, `A_GREATER_THAN_B=1` (production branch). MATCH.
- `OID_MAX = UINT_MAX` (postgres_ext.h:40) -> `Oid::MAX` = u32::MAX. MATCH.
- `UCHAR_MAX = u8::MAX = 255` (<limits.h>). MATCH.
- PG_INT{16,32,64}_{MIN,MAX} -> i{16,32,64}::{MIN,MAX}. MATCH.
- `InvalidOid = 0`. MATCH.

Spot-checked in full: `btoidvectorcmp` (length-first int32 diff + unsigned
element compare + full-equality 0), `btint48cmp` (i32->i64 promotion preserving
sign before 3-way), `char_increment`/`btcharcmp` (unsigned-byte semantics).

VERDICT: PASS — 39/39 functions MATCH or MATCH/SEAMED (field-install
fn-ptr writes correctly delegated to the substrate-owned install seams). No
FAIL/MISSING/PARTIAL/DIVERGES.
