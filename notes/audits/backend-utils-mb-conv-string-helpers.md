# Audit: backend-utils-mb-conv-string-helpers

Independent function-by-function audit (re-derived from C + c2rust, not trusting
the port's comments or self-review).

- **Unit c_sources:** `*/mbutils/conv.c`, `*/mbutils/stringinfo_mb.c`,
  `*/wstrcmp.c`, `*/wstrncmp.c`
- **C:** `src/backend/utils/mb/{conv,stringinfo_mb,wstrcmp,wstrncmp}.c`
- **Port:** `crates/backend-utils-mb-conv-string-helpers/src/lib.rs`
  (conv.c + stringinfo_mb.c); `wstrcmp.c`/`wstrncmp.c` are separate already-merged
  leaf crates re-exported here.
- **Branch:** `port/backend-utils-mb-conv-string-helpers`
- **Verdict: PASS**

## conv.c

| C function | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|
| `local2local` | conv.c:32 | lib.rs:64 | MATCH | NUL/highbit/table branches; noError early return uses consumed=pos (l not advanced); else consumes full len. Untranslatable -> report seam. |
| `latin2mic` | conv.c:88 | lib.rs:101 | MATCH | NUL check + highbit-prefix-lc-then-byte; no untranslatable path; consumed=pos / full len. |
| `mic2latin` | conv.c:126 | lib.rs:127 | MATCH | ASCII fast path advances 1; mule path: pg_mule_mblen seam, len<l invalid, (l!=2\|\|c1!=lc\|\|!highbit(mic[1])) untranslatable; emits mic[1], advances 2. noError break leaves pos un-advanced like C. |
| `latin2mic_with_table` | conv.c:193 | lib.rs:174 | MATCH | table hit emits lc+c2; miss -> untranslatable(enc,PG_MULE_INTERNAL). |
| `mic2latin_with_table` | conv.c:256 | lib.rs:211 | MATCH | combined predicate `l==2 && c1==lc && highbit && tab[..]!=0` collapsed into one converted==0 untranslatable check; behavior identical to C's compound `if`. |
| `compare3` (bsearch cmp) | conv.c:319 | lib.rs:337 | MATCH | (utf1,utf2) tuple ordering via binary_search_by element.cmp(&key); correct direction. |
| `compare4` (bsearch cmp) | conv.c:338 | lib.rs:432 | MATCH | code ordering via binary_search_by. |
| `store_coded_char` | conv.c:352 | lib.rs:548 | MATCH | byte-at-a-time on the four 0xff masks, MSB first. |
| `pg_mb_radix_conv` | conv.c:372 | lib.rs:465 + radix_lookup1..4 | MATCH | per-length bound checks + root/lower index walk; chars32-preferred, chars16 fallback. u32 intermediate idx == C's uint16 intermediate because chars16 values are <=0xFFFF (truncation is a no-op). Padded b1..b4 mapping verified for l=1..4. |
| `UtfToLocal` | conv.c:506 | lib.rs:266 | MATCH | encoding validate; ASCII 1-byte fast path; combined-map (`cmap && len>l`) with the exact C "need more data" tail (utf=first-char-start, len=len_save-l_save) reproduced via tail_report slice; second-char illegal report; combined miss falls back to ordinary map with first char's bytes/len; ordinary map; conv_func; untranslatable; post-loop invalid-encoding. elog "unsupported character length" preserved in collect_coded_char. |
| `LocalToUtf` | conv.c:716 | lib.rs:389 | MATCH | encoding validate; ASCII fast path; pg_encoding_verifymbchar (l<0 break); map then cmap-bsearch then conv_func; untranslatable; post-loop invalid-encoding. |

Helper notes: `oom_safe_buf` models the C drivers' worst-case `palloc` of the
destination as a `try_reserve` (recoverable OOM) — behavior-preserving since C
sizes for max growth and never reallocs; the produced bytes are identical.
`collect_coded_char` folds bytes MSB-first == C's `b1<<24|b2<<16|b3<<8|b4`.

## stringinfo_mb.c

| C function | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|
| `appendStringInfoStringQuoted` | stringinfo_mb.c:33 | lib.rs:484 | MATCH | maxlen<0/>=slen no-clip vs clip-to-pg_mbcliplen+ellipsis; leading quote; chunk loop doubles each embedded `'` by starting next chunk on the quote; final chunk + (`...'` or `'`). `%s` on a NUL-terminated C string == emitting the byte run (s carries no embedded NUL per C contract). |

## wstrcmp.c / wstrncmp.c (re-exported leaf crates)

| C function | Port | Verdict | Notes |
|---|---|---|---|
| `pg_char_and_wchar_strcmp` | backend-utils-mb-wstrcmp | MATCH | signed-char sign-extend on compare, unsigned-byte on return diff. |
| `pg_wchar_strncmp` | backend-utils-mb-wstrncmp | MATCH | n==0 short-circuit; mismatch diff; terminator/limit stop. |
| `pg_char_and_wchar_strncmp` | backend-utils-mb-wstrncmp | MATCH | unsigned-char zero-extend on both compare and return. |
| `pg_wchar_strlen` | backend-utils-mb-wstrncmp | MATCH | count before terminating zero. |

## Seam audit

- This unit's c_sources have **no** matching `X-seams` crate (no conv-seams /
  stringinfo_mb-seams). The crate owns **no inward seams**, so an empty
  `init_seams()` is correct (consistent with the recurrence_guard's
  every-declared-seam-is-installed-by-owner check, which passes).
- `init_seams()` is wired into `seams-init::init_all()` (lib.rs:202), alongside
  the two leaf crates (203-204).
- Outward seam calls, all into genuinely-unported owners (thin marshal+delegate,
  no logic in the seam path):
  - `report_invalid_encoding` / `report_untranslatable_char` ->
    `backend-utils-mb-mbutils-seams` (owner mbutils.c, unported; panic until it
    lands). These are the C `pg_noreturn` reporters; C falls through after them
    only as dead code, so the Rust `?`-on-Err is faithful.
  - `pg_mbcliplen` -> mbutils-seams (owner mbutils.c).
  - `enlarge_string_info` -> backend-libpq-pqformat (StringInfo growth + 1GB cap,
    homed there until common/stringinfo.c lands).
  - `pg_mule_mblen` / `pg_utf_mblen_private` / `pg_utf8_islegal` /
    `pg_encoding_verifymbchar` -> real ported `common-wchar` functions (direct
    deps, not seams).
- No own-logic stubs, no `todo!`/`unimplemented!`, no deferred/SEAMED-equivalent
  escape of in-crate logic.

## Gates

- `cargo test -p backend-utils-mb-conv-string-helpers`: 14 passed.
- `cargo check --workspace`: clean (warnings only).
- `cargo test -p seams-init`: 2 passed (both recurrence_guard checks).

**PASS** — every conv.c / stringinfo_mb.c function MATCH; wstr* leaves MATCH;
zero seam findings.
