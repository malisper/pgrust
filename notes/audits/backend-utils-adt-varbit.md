# Audit: backend-utils-adt-varbit

C source: `src/backend/utils/adt/varbit.c` (1894 LOC). No src-idiomatic
predecessor; ported directly from C. No c2rust-runs unit for this file.

Carrier model: `VarBit { bit_len: i32, data: PgVec<u8> }` (owned) /
`VarBitRef { bit_len, data: &[u8] }` (borrowed). The varlena header lives only
at the Datum/FFI boundary per the repo convention (same as varlena/varchar);
`data` is the header-less `VARBITS` payload, exactly `ceil(bit_len/8)` bytes
with the last byte zero-padded. This faithfully reproduces the C `VarBit`
struct's two semantic fields (`bit_len` + `bit_dat[]`); `VARSIZE`/`VARHDRSZ`
arithmetic is replaced by the equivalent data-length arithmetic.

## Function inventory & verdicts

| C fn (line) | Port (lib.rs) | Verdict | Notes |
|---|---|---|---|
| anybit_typmodin (89) | anybit_typmodin | MATCH | n!=1, *tl<1, *tl > MaxAttrSize*8 errors; SQLSTATEs 22023; ArrayGetIntegerTypmods decode at caller (bittypmodin/varbittypmodin) |
| anybit_typmodout (126) | anybit_typmodout | MATCH | `"(%d)"` for typmod>=0 else empty cstring |
| bit_in (146) | bit_in | MATCH | prefix b/x/binary; hex maxlen guard (slen>VARBITMAXLEN/4, 54000); atttypmod exact-match (22026); bit & hex parse loops; soft errors via ereturn->Ok(None) |
| bit_out (280) | bit_out=varbit_out | MATCH | C uses `return varbit_out(fcinfo)` (the `#if 1` arm) |
| bit_recv (330) | bit_recv | MATCH | pq_getmsgint(4); bitlen range (22P03); atttypmod exact (22026); copy bytes; VARBIT_PAD |
| bit_send (375) | bit_send=varbit_send | MATCH | shares varbit_send |
| bit (390) | bit | MATCH | no-work fast path (len<=0/>MAX/==len); implicit mismatch (22026); palloc0 + copy Min(bytes); VARBIT_PAD. Fast path returns an owned clone (carrier is owned) — value-identical to C returning arg |
| bittypmodin (428) | bittypmodin | MATCH | array_get_integer_typmods + anybit_typmodin("bit") |
| bittypmodout (436) | bittypmodout | MATCH | |
| varbit_in (451) | varbit_in | MATCH | like bit_in but atttypmod is max; too-long (22001); VARBITLEN=Min(bitlen,atttypmod); data sized to bitlen |
| varbit_out (586) | varbit_out | MATCH | full-byte loop (i<=len-8) then partial-byte loop; IS_HIGHBIT_SET; palloc(len+1) |
| varbit_recv (635) | varbit_recv | MATCH | bitlen range (22P03); too-long (22001); copy + PAD |
| varbit_send (680) | varbit_send | MATCH | pq_begintypsend/sendint32(bitlen)/sendbytes/endtypsend -> Bytea |
| varbit_support (701) | — | DEFERRED | planner SupportRequestSimplify node surgery; supportnodes.h/planner unported. Same project-wide deferral as varchar_support (CATALOG-noted). Not a logic gap in this datatype's runtime. |
| varbit (741) | varbit | MATCH | no-work (len<=0 / len>=bitlen); implicit too-long (22001); copy rbytes; PAD |
| varbittypmodin (773) | varbittypmodin | MATCH | |
| varbittypmodout (781) | varbittypmodout | MATCH | |
| bit_cmp (817) | bit_cmp | MATCH | memcmp(Min bytes) then tie-break on bitlen (-1/1) |
| biteq (840) | biteq | MATCH | fast path different length -> false |
| bitne (864) | bitne | MATCH | fast path -> true |
| bitlt/le/gt/ge (888-940) | bitlt/le/gt/ge | MATCH | bit_cmp sign tests |
| bitcmp (948) | bitcmp | MATCH | |
| bitcat (967) | bitcat=bit_catenate | MATCH | |
| bit_catenate (976) | bit_catenate | MATCH | overflow guard (54000); copy arg1; bit1pad==0 memcpy else shift loop with bit2shift; pad already zero |
| bitsubstr (1037) | bitsubstr | MATCH | bitsubstring(arg,s,l,false) |
| bitsubstr_no_len (1046) | bitsubstr_no_len | MATCH | bitsubstring(arg,s,-1,true) |
| bitsubstring (1054) | bitsubstring | MATCH | s1=Max(s,1); e1 cases (not_spec / l<0 error 22011 / add-overflow -> end / Min); zero-len result; byte-boundary memcpy else shift loop; VARBIT_PAD. pg_add_s32_overflow -> checked_add |
| bitoverlay (1152) | bitoverlay=bit_overlay | MATCH | |
| bitoverlay_no_len (1163) | bitoverlay_no_len | MATCH | sl=VARBITLEN(t2) |
| bit_overlay (1175) | bit_overlay | MATCH | sp<=0 (22011); sp+sl overflow (22003); substr/substr/cat/cat |
| bit_bit_count (1210) | bit_bit_count | MATCH | pg_popcount -> sum of count_ones; returns i64 |
| bitlength (1222) | bitlength | MATCH | VARBITLEN |
| bitoctetlength (1230) | bitoctetlength | MATCH | VARBITBYTES |
| bit_and (1242) | bit_and | MATCH | size mismatch (22026); byte AND; no pad needed |
| bit_or (1283) | bit_or | MATCH | |
| bitxor (1323) | bitxor | MATCH | |
| bitnot (1364) | bitnot | MATCH | ~ each byte; VARBIT_PAD_LAST (extra bits are 1's) |
| bitshiftleft (1391) | bitshiftleft | MATCH | neg -> right (clamp -VARBITMAXLEN); shft>=len -> all zero; byte_shift/ishift; memcpy special case else shift loop; no pad |
| bitshiftright (1458) | bitshiftright | MATCH | neg -> left; all-zero; leading zero fill; memcpy/shift; VARBIT_PAD_LAST |
| bitfromint4 (1530) | bitfromint4 | MATCH | typmod default 1; sign-fill excess bytes; first fractional byte with forced sign-fill (arithmetic shift of signed `a` then cast, matching `(unsigned int)(a >> ...)`); whole bytes; last fractional |
| bittoint4 (1585) | bittoint4 | MATCH | len>32 -> 22003 "integer out of range"; accumulate (wrapping_shl mirrors uint32 wraparound); >> VARBITPAD |
| bitfromint8 (1610) | bitfromint8 | MATCH | as int4 but srcbitsleft=64; fractional byte cast to u32 matches `(unsigned int)(a>>...)` |
| bittoint8 (1665) | bittoint8 | MATCH | len>64 -> 22003 "bigint out of range"; wrapping uint64 accumulate; >> pad |
| bitposition (1697) | bitposition | MATCH | empty/too-long -> 0; empty substr -> 1; padding masks; nested byte/bit search; `is==0` shift-by-8 edge reproduced (C bits8 truncation of `<<8` -> 0) explicitly; returns i*8+is+1 |
| bitsetbit (1806) | bitsetbit | MATCH | n range (2202E); newBit 0/1 (22023); copy; byteNo/bitNo; set/clear |
| bitgetbit (1868) | bitgetbit | MATCH | n range (2202E); byteNo/bitNo; mask test |

51 C functions: 50 ported MATCH, 1 (`varbit_support`) deferred per project-wide
planner-support-node deferral (documented in CATALOG for varchar_support; the
identical pattern). No logic in this datatype's value-level behavior is absent.

## Constants verified against headers

- BITS_PER_BYTE=8, HIGHBIT=0x80, BITMASK=0xFF (c.h / varbit.h) — verified.
- VARBITMAXLEN = INT_MAX - 8 + 1 = `i32::MAX - 7` (varbit.h:83) — verified.
- MaxAttrSize = 10*1024*1024 (htup_details.h) — used as `MaxAttrSize*8` cap — verified.
- VARBITTOTALLEN data-section = `(bitlen+7)/8` bytes — verified.
- SQLSTATEs: 22023 (INVALID_PARAMETER_VALUE), 22026 (STRING_DATA_LENGTH_MISMATCH),
  22001 (STRING_DATA_RIGHT_TRUNCATION), 54000 (PROGRAM_LIMIT_EXCEEDED),
  22P02/22P03 (INVALID_TEXT/BINARY_REPRESENTATION), 22011 (SUBSTRING_ERROR),
  22003 (NUMERIC_VALUE_OUT_OF_RANGE), 2202E (ARRAY_SUBSCRIPT_ERROR) — all match
  the C ereport sites.

## Seams & wiring

This unit owns no inward seam crate (it is a leaf adt type; no current cyclic
caller — the fmgr/PGFunction registry boundary is the project-wide deferral, so
no consumer reaches it across a cycle yet). `init_seams()` is therefore not
required and is intentionally absent (mirrors backend-utils-adt-varchar, which
also owns no inward seam).

Outward dependencies, all real edges (direct cargo deps, no cycle):
- `backend-libpq-pqformat` — pq_getmsgint/copymsgbytes/begintypsend/sendint32/
  sendbytes/endtypsend (recv/send). Direct dep, thin use.
- `backend-utils-adt-arrayutils` — array_get_integer_typmods (typmodin). Direct.
- `backend-utils-mb-mbutils-seams::pg_mblen_range` — used ONLY to size the byte
  fragment quoted in a bad-digit error message (C `pg_mblen_cstr`). This is a
  seam consumed (not owned) by this unit; its owner installs it. Marshal-only
  call. No branching in any seam path.

No invented opacity, no shared statics, no ambient-global seams, no locks across
`?`, no registry side tables. Allocating functions take `Mcx` and return
`PgResult` (OOM via `mcx.oom` inside `vec_with_capacity_in`); infallible
functions (comparisons, length, getbit, count, bittoint range guards) return
bare values / PgResult only where the C can ereport.

## Verdict: PASS

All 50 value-level functions MATCH; the single omission (`varbit_support`) is
the project-wide planner-support-node deferral, identical to the merged
varchar_support deferral, and carries no datatype runtime logic. 18 unit tests
pass. No seam findings, no design-conformance findings.
