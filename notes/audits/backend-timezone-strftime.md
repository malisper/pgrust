# Audit: backend-timezone-strftime

- Catalog unit: `backend-timezone-strftime` — `src/timezone/strftime.c`
- C source: postgres-18.3 `src/timezone/strftime.c` (constants verified against
  `src/timezone/private.h`; `struct pg_tm` against `src/include/pgtime.h`)
- c2rust reference: `c2rust-runs/backend-timezone-strftime/src/strftime.rs`
- Port: `crates/backend-timezone-strftime/src/lib.rs`
- Audit date: 2026-06-12

## Function inventory

`strftime.c` defines exactly five functions — `pg_strftime`, `_fmt`, `_conv`,
`_add`, `_yconv` — plus the static `C_time_locale` table, the `lc_time_T`
struct, and `enum warn`. The c2rust rendering contains exactly these five
functions plus the same data (and the Darwin `__error` errno shim, a header
artifact, not unit logic). The only build-config branch in the C is
`#ifdef KITCHEN_SINK` (`%K` → "kitchen sink"), which is never defined in any
PostgreSQL build config and is absent from the c2rust output; correctly absent
from the port. The port additionally inlines `isleap_sum` from `private.h`
(a macro in C, expanded inline by c2rust) — audited as a row below.

| C definition (location) | Port location | Verdict | Notes |
|---|---|---|---|
| `C_time_locale` (strftime.c:64) | `MON`/`MONTH`/`WDAY`/`WEEKDAY`/`X_FMT`/`X_FMT_LOWER`/`C_FMT`/`AM`/`PM`/`DATE_FMT` consts | MATCH | Every string compared byte-for-byte against the C initializer, including `c_fmt = "%a %b %e %T %Y"` and `date_fmt = "%a %b %e %H:%M:%S %Z %Y"`. |
| `enum warn` (strftime.c:109) | `Warn` enum (`None < Some < This < All`) | MATCH | Ordering used by `>` comparisons preserved via `Ord` derive; `raise` is exactly `if (warn2 > *warnp) *warnp = warn2`. |
| `pg_strftime` (strftime.c:127) | `pg_strftime` (lib.rs:126) | MATCH | Detail below. |
| `_fmt` (strftime.c:150) | `fmt` (lib.rs:142) | MATCH | Detail below — every specifier arm compared individually. |
| `_conv` (strftime.c:515) | `conv` (lib.rs:370) | MATCH | C `sprintf(buf, fmt, n)` with exactly five format strings (`%02d`, `%03d`, `%04d`, `%2d`, `%d`, enumerated from all call sites) becomes the closed `IntFmt` enum; Rust `{n:02}`/`{n:03}`/`{n:04}`/`{n:2}`/`{n}` produce identical output to C `printf` for all i32 values incl. negatives (sign-aware zero padding matches: `%03d` of -1 → `-01` in both). Scratch buffer 16 ≥ C's `INT_STRLEN_MAXIMUM(int)+1 = 12`; max rendering is 11 bytes. |
| `_add` (strftime.c:524) | `add` + `OutBuf::push` (lib.rs:347,106) | MATCH | C copies until NUL or `ptlim`, silently truncating, returns advanced `pt`. Port iterates NUL-free byte slices into the position-tracking `OutBuf` whose `push` drops writes at the limit — identical (all inputs come from `CStr::to_bytes` or NUL-free literals). |
| `_yconv` (strftime.c:540) | `yconv` (lib.rs:393) | MATCH | DIVISOR=100; trail/lead split, both borrow-correction branches (`trail<0 && lead>0`, `lead<0 && trail>0`), the `lead==0 && trail<0` → `"-0"` special case, and `abs(trail)` via `%02d` — all line-for-line. Verified `%C%y == %Y` for negative years by test. |
| `isleap_sum` (private.h:147, macro) | `isleap_sum` (lib.rs:446) | MATCH | `isleap(a % 400 + b % 400)` with `isleap(y) = y%4==0 && (y%100!=0 || y%400==0)` — verified against private.h:133/147; C and Rust `%` are both truncating for negatives. |

## Detail: pg_strftime

C: save errno; `p = _fmt(format, t, s, s+maxsize, &warn)`; `!p` → EOVERFLOW,
return 0 (dead code — `_fmt` has no NULL return path, confirmed in both the C
and the c2rust rendering); `p == s+maxsize` → ERANGE, return 0 with the
truncated bytes left in `s` and no NUL written; else write `*p = '\0'`,
restore errno, return `p - s`.

Port: `fmt` into `OutBuf{buf:s, pos:0}`; `out.full()` (`pos == buf.len()`,
exactly `p == s+maxsize`) → `None`, buffer holds truncated bytes, no NUL;
else push NUL and return `Some(pos)`. The errno protocol is replaced by the
`Option` return — an API-shape change on a crate-owned entry point, with the
success/overflow predicate and all buffer side effects identical (verified by
tests including exact-fit, fit-minus-NUL, zero-size-buffer, and empty-format
cases). The dead EOVERFLOW branch is correctly omitted and documented.

## Detail: _fmt

Compared arm-by-arm against the C switch and the c2rust `current_block_87`
state machine:

- Outer loop / literal path: C `if (pt == ptlim) break; *pt++ = *format;`
  becomes `if out.full() { return; } out.push(...)` — same termination, and
  conversion arms continue to run as no-ops once the buffer is full (the C
  `continue` cases skip the ptlim check), preserved by the port.
- `'\0'` after `%` (trailing `%`, or trailing `%E`/`%O`): C does `--format;
  break;` so the literal write emits the byte *before* the NUL, then the loop
  terminates. Port: `i >= format.len()` emits `format[i-1]` and returns —
  identical for `"%"` (emits `%`) and `"%E"` (emits `E`); both verified by
  tests.
- `%A %a %B %b %h`: bounds checks `< 0 || >= DAYSPERWEEK/MONSPERYEAR` → `"?"`,
  else table lookup — `weekday_name`/`month_name` use `(0..N).contains`,
  identical predicate.
- `%C` `%Y` `%y` `%G` `%g`: `_yconv` calls with the same (convert_top,
  convert_yy) flags; `%y` and `%g` set `*warnp = IN_ALL` before the call, as
  in C.
- `%c` `%x`: recursive `_fmt` with a fresh `warn2 = IN_SOME`, the
  `IN_ALL → IN_THIS` demotion, and the max-merge into `*warnp` — exact.
- `%D %F %R %r %T %v %+ %X`: recursive `_fmt` on the same literal/locale
  format strings, propagating `warnp` — exact.
- `%d %e %H %I %j %k %l %M %m %S %U %u %W %w`: `_conv` with the same
  expressions and format strings; `%I`/`%l` share the C
  `(h % 12) ? h % 12 : 12` (hour12, truncating `%` matches C for negatives);
  `%k`/`%l` keep the swapped SunOS semantics; `%U`/`%W` week arithmetic
  verified term-by-term.
- `%E %O`: consume the modifier and re-dispatch on the next char (the C
  `goto label` / c2rust empty-arm loop), port's `continue 'label` — exact,
  including the trailing-modifier interaction with the `'\0'` case.
- `%V %G %g` ISO block: `len` via `isleap_sum(year, base)`, `bot = (yday + 11
  - wday) % 7 - 3`, `top = bot - len % 7`, `top < -3` correction, `top += len`,
  the three loop exits (`yday >= top` → base+1/w=1, `yday >= bot` → w
  formula, else base-1 and yday += prior-year length using the *decremented*
  base) — line-for-line identical; dispatch on the spec char afterwards
  matches (`V` → `%02d` of w; `g` → warn + 2-digit yconv; `G` → 4-digit
  yconv).
- `%n %t %p`: literal `\n`, `\t`, AM/PM on `tm_hour >= HOURSPERDAY/2` — exact.
- `%Z`: emit `tm_zone` only when present (None ↔ C NULL) — exact.
- `%z`: skip when `tm_isdst < 0`; `diff = tm_gmtoff` (i64, like C long);
  zero-offset sign taken from `tm_zone[0] == '-'`; negate-and-`"-"` vs `"+"`;
  `diff /= 60; diff = diff/60*100 + diff%60`; `_conv((int)diff, "%04d")` with
  the same i64→i32 narrowing point as C/c2rust — exact.
- `%%` and unknown specifiers: fall through to the literal write of the
  specifier char itself (C `case '%': default: break;`) — the port's `other`
  arm, verified by tests (`"%%"` → `%`, `"%Q"` → `Q`).

Constants `SECSPERMIN=60`, `MINSPERHOUR=60`, `HOURSPERDAY=24`,
`DAYSPERWEEK=7`, `DAYSPERNYEAR=365`, `DAYSPERLYEAR=366`, `MONSPERYEAR=12`,
`TM_YEAR_BASE=1900`, `DIVISOR=100` verified against `private.h` and
strftime.c, not from memory.

`struct pg_tm` matches `pgtime.h` field-for-field (`tm_gmtoff` as i64 for C
`long int`; `tm_zone: Option<CString>` for `const char *`, with None ↔ NULL
at both use sites, `%Z` and `%z`).

## Seam audit

- The crate is a leaf: no dependencies, no outward seam calls, no
  `*-seams` crate. `init_seams()` is empty (nothing to install) and is
  called by `seams-init::init_all()` (crates/seams-init/src/lib.rs:17).
  No `set()` calls exist anywhere for this unit. No findings.
- No function body is delegated anywhere; all logic lives in this crate.

## Spot-check

Re-derived in full detail (beyond table-level comparison): the `'\0'`/
trailing-modifier path, the `%V/%G/%g` ISO-week loop, `_yconv`'s borrow
corrections and `"-0"` path, and `_conv`'s printf-vs-Rust formatting for
negative inputs. `cargo test -p backend-timezone-strftime`: 16/16 pass;
workspace `seams-init` builds.

## Verdict

**PASS** — every function MATCH; zero seam findings.
