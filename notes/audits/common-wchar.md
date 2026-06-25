# Audit: common-wchar (`src/common/wchar.c`)

- **Verdict: PASS**
- Date: 2026-06-13
- Model: claude-opus-4-8[1m] (Opus 4.8, 1M context)
- Unit: `probe-next-srv-wchar` (crate `common-wchar`), c_sources `*/wchar.c`
- Sources compared: `postgres-18.3/src/common/wchar.c`,
  `c2rust-runs/probe-next-srv-wchar/src/wchar.rs`,
  `crates/common-wchar/src/{lib.rs,tables.rs}` + `crates/types-wchar/src/{encoding.rs,wchar.rs}`

## Method

Enumerated every function definition in `wchar.c` (including `static` and
`inline` helpers and the two header-inlined helpers `utf8_to_unicode` /
`unicode_to_utf8` that c2rust rendered into this TU). Cross-checked the list
against the c2rust rendering (82 fn defs = 79 wchar.c fns + `pg_utf_mblen` +
2 header inlines; the macro aliases `pg_euccn_verifychar`/`pg_euccn_verifystr`
expand to the EUC-KR routines and have no separate definition). Verified the
three transcribed data tables programmatically against the C headers, and
verified all magic constants/ranges against `mb/pg_wchar.h`, not from memory.

### Data-table verification (programmatic, exact)

| Table | C source | C len | Rust len | Result |
|---|---|---|---|---|
| `NONSPACING` | `unicode_nonspacing_table.h` | 334 | 334 | every `(first,last)` pair identical |
| `EAST_ASIAN_FW` | `unicode_east_asian_fw_table.h` | 122 | 122 | every `(first,last)` pair identical |
| `UTF8_TRANSITION` | `Utf8Transition[256]` in wchar.c | 256 | 256 | every symbolic entry identical token-for-token |

DFA `#define`s (ERR=0, BGN=11, CS1=16, CS2=1, CS3=5, P3A=6, P3B=20, P4A=25,
P4B=30, END=BGN) and the derived `ASC/L2A/L3A/L3B/L3C/L4A/L4B/L4C/CR1/CR2/CR3/ILL`
expressions match the C macros exactly.

### Constant / macro verification against `mb/pg_wchar.h`

- `SS2=0x8e`, `SS3=0x8f` ✓
- `IS_LC1` `0x81..=0x8d` ✓, `IS_LC2` `0x90..=0x99` ✓
- `LCPRV1_A=0x9a`, `LCPRV1_B=0x9b`, `LCPRV2_A=0x9c`, `LCPRV2_B=0x9d` ✓
- `IS_LCPRV1_A_RANGE` `0xa0..=0xdf` ✓, `IS_LCPRV1_B_RANGE` `0xe0..=0xef` ✓
- `IS_LCPRV2_A_RANGE` `0xf0..=0xf4` ✓, `IS_LCPRV2_B_RANGE` `0xf5..=0xfe` ✓
- `ISSJISHEAD` `(0x81..=0x9f)|(0xe0..=0xfc)` ✓, `ISSJISTAIL` `(0x40..=0x7e)|(0x80..=0xfc)` ✓
- `NONUTF8_INVALID_BYTE0=0x8d`, `NONUTF8_INVALID_BYTE1=0x20 (' ')` ✓
- `enum pg_enc` 0..=41 ordering byte-for-byte; `_PG_LAST_ENCODING_=42`,
  `PG_ENCODING_BE_LAST=PG_KOI8U` ✓. `pg_wchar_table` table ordering follows the
  enum exactly (verified entry by entry, including the 30 single-byte LATIN/WIN/
  KOI8/ISO-8859 slots via `SINGLE_BYTE_TBL`).

## Per-function table

All raw `const unsigned char *` / `pg_wchar *` are rendered as `&[u8]` /
`&[pg_wchar]`/`&mut [pg_wchar]` (faithful: the C operates on raw byte strings).

| C function | C loc | Port loc (lib.rs) | Verdict | Notes |
|---|---|---|---|---|
| utf8_to_unicode (hdr inline) | pg_wchar.h:565 | 133 | MATCH | bogus lead -> 0xffffffff (C returns same via fallthrough) |
| unicode_to_utf8 (hdr inline) | pg_wchar.h:591 | 157 | MATCH | 1-4 byte encode, identical masks |
| pg_ascii2wchar_with_len | 73 | 195 | MATCH | |
| pg_ascii_mblen | 88 | 210 | MATCH | |
| pg_ascii_dsplen | 94 | 214 | MATCH | NUL->0, <0x20/0x7f->-1, else 1 |
| pg_euc2wchar_with_len | 108 | 229 | MATCH | SS2/SS3/highbit/ascii, MB2CHAR_NEED_AT_LEAST=break |
| pg_euc_mblen | 149 | 275 | MATCH | |
| pg_euc_dsplen | 165 | 289 | MATCH | |
| pg_eucjp2wchar_with_len | 184 | 304 | MATCH | delegates to euc |
| pg_eucjp_mblen | 190 | 308 | MATCH | |
| pg_eucjp_dsplen | 196 | 312 | MATCH | SS2->1, SS3->2 |
| pg_euckr2wchar_with_len | 215 | 327 | MATCH | |
| pg_euckr_mblen | 221 | 331 | MATCH | |
| pg_euckr_dsplen | 227 | 335 | MATCH | |
| pg_euccn2wchar_with_len | 237 | 341 | MATCH | SS2/SS3 both 3-byte |
| pg_euccn_mblen | 285 | 388 | MATCH | SS2/SS3->3 (differs from verifychar by design, preserved) |
| pg_euccn_dsplen | 301 | 401 | MATCH | |
| pg_euctw2wchar_with_len | 317 | 411 | MATCH | SS2 4-byte: `(uint32)SS2<<24` cast preserved |
| pg_euctw_mblen | 360 | 460 | MATCH | |
| pg_euctw_dsplen | 376 | 473 | MATCH | |
| pg_wchar2euc_with_len | 398 | 487 | MATCH | 4/3/2/1-byte cascade |
| pg_johab_mblen | 444 | 538 | MATCH | |
| pg_johab_dsplen | 450 | 542 | MATCH | |
| pg_utf2wchar_with_len | 462 | 550 | MATCH | 1-4 byte decode, bogus->len1 |
| pg_wchar2utf_with_len | 525 | 608 | MATCH | uses pg_utf_mblen on written byte |
| pg_utf_mblen | 556 | 176 (pg_utf_mblen_byte) / 1555 (pg_utf_mblen_private) | MATCH | leading-byte length; NOT_USED 5/6-byte branches excluded by build, correctly absent |
| mbbisearch | 599 | 628 | MATCH | max=len-1; empty-guard added (tables never empty), behavior identical |
| ucs_wcwidth | 646 | 648 | MATCH | nonspacing-first order, 0x10ffff bound |
| pg_utf_dsplen | 680 | 664 | MATCH | |
| pg_mule2wchar_with_len | 692 | 672 | MATCH | LC1/LCPRV1/LC2/LCPRV2 ranges verified |
| pg_wchar2mule_with_len | 749 | 732 | MATCH | all 6 range branches + ASCII verified |
| pg_mule_mblen | 815 | 802 (byte) / 1561 (pub) | MATCH | |
| pg_mule_dsplen | 833 | 817 | MATCH | |
| pg_latin12wchar_with_len | 861 | 836 | MATCH | |
| pg_wchar2single_with_len | 883 | 851 | MATCH | |
| pg_latin1_mblen | 898 | 866 | MATCH | |
| pg_latin1_dsplen | 904 | 870 | MATCH | |
| pg_sjis_mblen | 913 | 878 | MATCH | 0xa1..0xdf->1 kana |
| pg_sjis_dsplen | 927 | 889 | MATCH | |
| pg_big5_mblen | 944 | 900 | MATCH | |
| pg_big5_dsplen | 956 | 908 | MATCH | |
| pg_gbk_mblen | 971 | 916 | MATCH | |
| pg_gbk_dsplen | 983 | 924 | MATCH | |
| pg_uhc_mblen | 998 | 932 | MATCH | |
| pg_uhc_dsplen | 1010 | 940 | MATCH | |
| pg_gb18030_mblen | 1037 | 948 | MATCH | reads s[1]; `.get(1).unwrap_or(0)` defensive read, same result for valid callers (absent byte not in 0x30..0x39) |
| pg_gb18030_dsplen | 1051 | 964 | MATCH | |
| pg_ascii_verifychar | 1085 | 986 | MATCH | return 1 |
| pg_ascii_verifystr | 1091 | 990 | MATCH | memchr -> nul_pos |
| pg_eucjp_verifychar | 1104 | 994 | MATCH | SS2/SS3/highbit ranges + len checks |
| pg_eucjp_verifystr | 1159 | 1046 | MATCH | shared verify_str |
| pg_euckr_verifychar | 1188 | 1050 | MATCH | |
| pg_euckr_verifystr | 1217 | 1073 | MATCH | |
| pg_euccn_verifychar (=euckr macro) | 1246 | table->pg_euckr_verifychar | MATCH | alias preserved via table slot |
| pg_euccn_verifystr (=euckr macro) | 1247 | table->pg_euckr_verifystr | MATCH | alias preserved via table slot |
| pg_euctw_verifychar | 1250 | 1077 | MATCH | SS2 0xa1..0xa7 first cont, SS3->-1 |
| pg_euctw_verifystr | 1300 | 1122 | MATCH | |
| pg_johab_verifychar | 1329 | 1126 | MATCH | --l>0 trailing EUC-range loop |
| pg_johab_verifystr | 1353 | 1150 | MATCH | |
| pg_mule_verifychar | 1382 | 1154 | MATCH | trailing-byte highbit loop |
| pg_mule_verifystr | 1403 | 1175 | MATCH | |
| pg_latin1_verifychar | 1432 | 1179 | MATCH | |
| pg_latin1_verifystr | 1438 | 1183 | MATCH | |
| pg_sjis_verifychar | 1449 | 1187 | MATCH | ISSJISHEAD/ISSJISTAIL inlined exactly |
| pg_sjis_verifystr | 1472 | 1206 | MATCH | |
| pg_big5_verifychar | 1501 | 1210 | MATCH | NONUTF8_INVALID pair reject + NUL-in-trailer |
| pg_big5_verifystr | 1526 | 1233 | MATCH | |
| pg_gbk_verifychar | 1555 | 1237 | MATCH | |
| pg_gbk_verifystr | 1580 | 1260 | MATCH | |
| pg_uhc_verifychar | 1609 | 1264 | MATCH | |
| pg_uhc_verifystr | 1634 | 1287 | MATCH | |
| pg_gb18030_verifychar | 1663 | 1291 | MATCH | 4/2-byte range validation, len guards |
| pg_gb18030_verifystr | 1694 | 1314 | MATCH | |
| pg_utf8_verifychar | 1723 | 1318 | MATCH | NUL->-1, l>len->-1, islegal check |
| utf8_advance | 1894 | 1376 | MATCH | shift-DFA, mask 31 |
| pg_utf8_verifystr | 1912 | 1387 | MATCH | STRIDE fast path, ERR restart, backtrack-to-lead loop |
| pg_utf8_islegal | 2010 | 1437 (bytes) / 1568 (pub) | MATCH | RFC3629 second-byte tables; lengths 5/6->false |
| pg_encoding_set_invalid | 2072 | 1539 | MATCH | UTF8->0xc0 else 0x8d; `Option<()>` replaces the C Assert(max_length>1) precondition |
| pg_wchar_table[] | 2086 | 1665 | MATCH | 42 entries, enum-ordered; client-only NULL mb2wchar/wchar2mb slots -> loud unreachable!() (see seam note) |
| pg_encoding_mblen | 2156 | 1585 | MATCH | PG_VALID_ENCODING fallback to PG_SQL_ASCII via table_index |
| pg_encoding_mblen_or_incomplete | 2168 | 1593 | MATCH | remaining<1 or GB18030+highbit+<2 -> INT_MAX |
| pg_encoding_mblen_bounded | 2188 | 1602 | MATCH | strnlen over mblen |
| pg_encoding_dsplen | 2198 | 1614 | MATCH | |
| pg_encoding_verifymbchar | 2210 | 1621 | MATCH | explicit C `len` -> slice len (idiomatic slice port) |
| pg_encoding_verifymbstr | 2224 | 1627 | MATCH | likewise |
| pg_encoding_max_length | 2235 | 1633 | MATCH | C Assert(PG_VALID) + valid-check both honored by table_index clamp |

Refactor note (not a divergence): the 11 per-encoding `*_verifystr` functions
are byte-identical except for the char-verifier they call, so the port factors
the loop into one `verify_str(s, len, verifychar)` helper. Verified the helper
reproduces the C loop exactly (ASCII fast path, NUL break, `-1` break, advance
by `l`). The UTF-8 verifystr is kept separate because of its DFA fast path.

## Seam audit

No `crates/X-seams` crate maps to `wchar.c`; this unit owns no seam crates.
`common-wchar` depends only on `types-wchar` and makes no outward seam calls
(wchar.c is a self-contained leaf with no `ereport`/`elog`, no allocation, and
no calls outside its own TU + two header inlines). No `init_seams()` is required
or present. **Zero seam findings.**

## Design conformance (§3b)

- No allocation occurs (all routines write into caller-provided slices), so the
  `Mcx`+`PgResult` rule does not apply; no allocating function lacks them.
- `pg_wchar_table` is an immutable `'static` table — this is genuinely `const`
  in C (`const pg_wchar_tbl pg_wchar_table[]`), not per-backend mutable state, so
  it is not a shared-static / registry-shaped violation.
- No invented opacity: raw byte pointers become `&[u8]`/`&[pg_wchar]`; the only
  new types in `types-wchar` (`mbinterval`, `pg_enc` ids) are real C structs/
  enums, not stand-in handles.
- The client-only NULL converter slots are rendered as loud `unreachable!()`
  (the C contract is "never dispatched as server encoding"); this preserves
  behavior (a panic instead of a NULL deref) and is ledgered in the code's doc
  comments. No silent stub.
- No ambient-global seams, no locks, no divergence markers.

**No design findings.**

## Result

Every function MATCH; all three data tables and all constants verified exact
against the C headers; no seams to install; no design-conformance violations.
`cargo test -p common-wchar` passes (6 tests). **PASS.**
