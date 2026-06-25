# Audit: backend-utils-adt-mac8

C source: `src/backend/utils/adt/mac8.c` (postgres-18.3). Port:
`crates/backend-utils-adt-mac8/src/lib.rs`. Independent re-derivation against the
C and the c2rust rendering.

## Function inventory + verdicts

| C function (line) | Port location | Verdict | Notes |
|---|---|---|---|
| `hexlookup[128]` table (41) | `HEXLOOKUP` const | MATCH | Byte-for-byte identical table; only first 128 entries, `-1` for non-hex. |
| `hibits` macro (33) | `hibits` (54) | MATCH | `(a<<24)|(b<<16)|(c<<8)|d`. Computed in `u32` not `unsigned long`; ordering identical (sign-extension is monotonic over the u32 total order; verified by the 0x7f vs 0x80 test). |
| `lobits` macro (36) | `lobits` (62) | MATCH | `(e<<24)|(f<<16)|(g<<8)|h`. Same reasoning. |
| `hex2_to_uchar` (58) | `hex2_to_uchar` (118) | MATCH | First/second char `>127` then `hexlookup<0` checks; `ret=lookup<<4` then `+=lookup`. End-of-slice (`.first()/.get(1)` None) maps to C's `'\0'`→hexlookup[-1]→badhex. |
| `macaddr8_in` (96) | `macaddr8_in` (165) | MATCH | skip leading spaces; `while *ptr && *(ptr+1)`; count 1-8 switch, default→fail; badhex→fail; pos+=2; spacer (`:`/`-`/`.`) consistency; count==6\|\|8 trailing-whitespace then non-space→fail; 6-byte widen to FF/FE; count!=8→fail. SQLSTATE `22P02`, msg `invalid input syntax for type macaddr8: "%s"`. Soft-error via `ereturn`→`Ok(None)`. |
| `macaddr8_out` (233) | `macaddr8_out` (308) | MATCH | `%02x` ×8, colon-separated, lowercase. |
| `macaddr8_recv` (253) | `macaddr8_recv` (368) | MATCH | a,b,c then `buf->len==6` ⇒ d=FF,e=FE else read d,e; then f,g,h. `pq_getmsgbyte` past end ⇒ `22P03` INVALID_BINARY_REPRESENTATION (inline `MsgCursor`, raw `&[u8]`, mirrors mac sibling). |
| `macaddr8_send` (286) | `macaddr8_send` (387) | MATCH | Eight bytes in order as the bytea body (`Vec<u8>`). |
| `macaddr8_cmp_internal` (309) | `macaddr8_cmp_internal` (392) | MATCH | hibits then lobits 3-way (-1/1/0). |
| `macaddr8_cmp` (324) | `macaddr8_cmp` (407) | MATCH | |
| `macaddr8_lt` (337) | `macaddr8_lt` (412) | MATCH | `<0`. |
| `macaddr8_le` (346) | `macaddr8_le` (417) | MATCH | `<=0`. |
| `macaddr8_eq` (355) | `macaddr8_eq` (422) | MATCH | `==0`. |
| `macaddr8_ge` (364) | `macaddr8_ge` (427) | MATCH | `>=0`. |
| `macaddr8_gt` (373) | `macaddr8_gt` (432) | MATCH | `>0`. |
| `macaddr8_ne` (382) | `macaddr8_ne` (437) | MATCH | `!=0`. |
| `hashmacaddr8` (394) | `hashmacaddr8` (444) | MATCH | `hash_any` over the 8 raw bytes (in-repo `common_hashfn::hash_bytes`). |
| `hashmacaddr8extended` (402) | `hashmacaddr8extended` (449) | MATCH | `hash_any_extended(.., seed)`. |
| `macaddr8_not` (414) | `macaddr8_not` (457) | MATCH | per-byte `~` ×8. |
| `macaddr8_and` (433) | `macaddr8_and` (470) | MATCH | per-byte `&` ×8. |
| `macaddr8_or` (453) | `macaddr8_or` (483) | MATCH | per-byte `\|` ×8. |
| `macaddr8_trunc` (477) | `macaddr8_trunc` (500) | MATCH | a,b,c kept; d..h zeroed. |
| `macaddr8_set7bit` (500) | `macaddr8_set7bit` (515) | MATCH | a\|0x02; b..h copied. |
| `macaddrtomacaddr8` (523) | `macaddrtomacaddr8` (530) | MATCH | a,b,c; d=FF,e=FE; f=addr6.d,g=addr6.e,h=addr6.f. |
| `macaddr8tomacaddr` (544) | `macaddr8tomacaddr` (547) | MATCH | guard `d!=FF \|\| e!=FE` ⇒ ERROR `22003` NUMERIC_VALUE_OUT_OF_RANGE, msg `macaddr8 data out of range to convert to macaddr` + hint; else a,b,c; d=f,e=g,f=h. |

All 24 C functions/macros/tables: **MATCH**. None MISSING/PARTIAL/DIVERGES.

## Seam audit

`c_sources = */mac8.c`. Owned seam crates = every `crates/X-seams` where `X`
maps to a C file in `c_sources`. mac8.c maps to `backend-utils-adt-mac8`; no
`crates/backend-utils-adt-mac8-seams` exists and none is required:

- mac8.c exposes no function another (unported) crate must call back into — no
  inward seam.
- It reaches no unported neighbour: hashing uses the in-repo `common_hashfn`
  directly; the `pq_getmsgbyte`/`pq_sendbyte` framing is inlined as the deferred
  fmgr boundary (raw `&[u8]` in, `Vec<u8>` out), exactly as the just-landed
  `backend-utils-adt-mac` sibling does. **mac8.c has NO sortsupport routine**
  (mac.c does), so there is no HLL/tuplesort `register` outward seam.

`init_seams()` is therefore empty, which is correct (no owned seam crate to
install). Not a finding.

## Design conformance

- No invented opacity: real `types_network::macaddr` / `macaddr8` carried as
  values.
- No allocating seam (no `Mcx`); the fallible surface is the soft-error path of
  `macaddr8_in` and the hard ERROR of `macaddr8tomacaddr`/recv, all
  `PgResult`-typed with correct SQLSTATEs.
- No shared statics, no ambient-global seams, no locks, no registries, no
  divergence markers.

## Verdict: PASS

Every function MATCH; zero seam findings; design-conformant.
