# Audit: port-snprintf (src/port/snprintf.c)

Crate: `crates/port-snprintf`. Pure-Rust leaf, zero deps, no seam crates, no
consumers. Independent re-derivation from `src/port/snprintf.c` (PG 18.3) +
c2rust run.

## Function inventory

| C function | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|
| pg_vsnprintf | 174 | `pg_vsnprintf`/`pg_snprintf_into` | MATCH | C fixed-buffer+nchars overrun count == port owned-Vec "infinite buffer"; returned would-be length identical. count==0 onebyte case == empty-buf truncate. |
| pg_snprintf | 202 | (varargs wrapper) | MATCH | thin va wrapper; collapses into `pg_snprintf_into`. |
| pg_vsprintf | 214 | `pg_sprintf` | MATCH | bufend==NULL (no overrun) == owned Vec. |
| pg_sprintf | 230 | `pg_sprintf` | MATCH | |
| pg_vfprintf | 242 | `pg_fprintf` | MATCH | stream==NULL EINVAL handled by caller passing a Write; 1024 staging buf == direct write_all; nchars == bytes written; failed preserves first io::Error. |
| pg_fprintf | 264 | `pg_fprintf` | MATCH | |
| pg_vprintf | 276 | `pg_printf` | MATCH | stdout. |
| pg_printf | 282 | `pg_printf` | MATCH | |
| flushbuffer | 298 | (subsumed by Target::Stream write_all) | MATCH | C staging-buffer flush == per-call write_all; failed-skip preserved. |
| dopr | 373 | `dopr` | MATCH | full nextch2 switch verified case-by-case (flags, *, .$, l/z/h/', d/i, o/u/x/X, c, s, p, e/E/f/g/G, m, %, default->bad_format). %s fast path present. WIN32 `case 'I'` correctly omitted (outside non-Windows build). %m via strerror == `io::Error::from_raw_os_error`. |
| find_arguments | 764 | `find_arguments` | MATCH | nextch1 scan, argtype consistency table (PG_NL_ARGMAX=31), afterstar handling, 1-based collection; typed-arg validation replaces va_arg type fetch. |
| fmtstr | 1000 | `fmtstr` | MATCH | strnlen(maxwidth) under pointflag; compute_padlen; lead spaces / trail pad. |
| fmtptr | 1029 | `fmtptr` | MATCH | C delegates to libc `%p`; port reproduces glibc/macOS spelling `(nil)` / `0x`+lowercase hex. Faithful platform reproduction. |
| fmtint | 1043 | `fmtint`+`fmtuint`+`fmt_integer` | MATCH | base/dosign per type; adjust_sign inlined into signvalue selection; SUS zero+prec0 => no digits; zeropad=Max(0,prec-vallen); leading/trailing pad. unsigned path ignores forcesign (dosign=0) == fmtuint. |
| fmtchar | 1154 | `fmtchar` | MATCH | |
| fmtfloat | 1172 | `fmtfloat`+`float_convert`+format_{f,e,g} | MATCH | prec clamp <0->0, Min(,350); NaN/Infinity platform-independent; -0.0 via sign bit (== C memcmp-vs-dzero); zeropadlen=precision-prec; e/E exponent zero-injection. libc %e/%f/%g reimplemented in pure Rust (no FFI), validated against C-expected outputs in tests. WIN32 3-digit-exp hack omitted. |
| pg_strfromd | 1318 | `pg_strfromd` | MATCH | Assert(count>0)==debug_assert; precision clamp 1..=32; inlined fmtfloat %g sans padding; NaN/Infinity/sign; truncate+NUL. |
| dostr | 1410 | `Target::dostr` | MATCH | overrun "lose data, count nchars" == grow Vec then truncate; would-be length identical. |
| dopr_outch | 1447 | `Target::outch` | MATCH | |
| dopr_outchmulti | 1463 | `Target::outchmulti` | MATCH | len<=0 guard; chunked stream writes. |
| adjust_sign | 1500 | (inlined) | MATCH | inlined into fmtint/fmtfloat/pg_strfromd sign blocks; same predicate. |
| compute_padlen | 1514 | `compute_padlen` | MATCH | exact. |
| leading_pad | 1528 | `leading_pad` | MATCH | exact: zpad sign-first then zeros; else spaces then sign; maxpad/padlen adjust. |
| trailing_pad | 1564 | `trailing_pad` | MATCH | exact. |
| strchrnul (replacement) | 359 | `memchr` / inline scan in dopr | MATCH | scan-to-'%'-or-end. |

## Seams / wiring

No C file in this unit maps to any `crates/X-seams`; the crate owns **zero**
seam crates. No reverse dependencies exist (no crate calls `port_snprintf`), so
there are no inward seams to declare and no `init_seams()` to wire — correct for
a pure leaf with no consumers (leaf-shape rule). Zero deps => no outward seams.
The empty-installer FAIL rule does not apply (no owned seam crates outstanding).

## Design conformance

- No Mcx/PgResult: the C never pallocs nor ereports; failure surface is
  `-1`/EINVAL, faithfully modeled by `PrintfError` (InvalidFormat==EINVAL).
- Owned-Vec growth on data-derived sizes (`dostr`/`outchmulti`) uses fallible
  `try_reserve` -> `PrintfError::OutOfMemory`; the bounded float/int staging
  Vecs mirror C's fixed `convert[64]`/`convert[1024]` stack buffers (precision
  capped at 350), not pallocs.
- No `todo!`/`unimplemented!`, no shared statics, no ambient-global seams, no
  type-alias stand-ins, no invented opacity, no unledgered divergence markers.

## Verdict: PASS

All 25 C functions MATCH. 32 unit tests pass. `cargo check --workspace` clean.
