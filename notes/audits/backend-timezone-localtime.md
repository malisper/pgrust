# Audit: backend-timezone-localtime

Unit: `backend-timezone-localtime` (`src/timezone/localtime.c`, 2020 lines,
PostgreSQL 18.3).
Crates audited: `crates/backend-timezone-localtime`,
`crates/backend-timezone-pgtz-seams`.
Cross-checked against
`../pgrust/c2rust-runs/backend-timezone-localtime/src/localtime.rs` (2199
lines; full function list matches the C inventory below).
Auditor: independent re-derivation from the C sources and headers
(`tzfile.h`, `private.h`, `pgtz.h`, `pgtime.h`, `datatype/timestamp.h`).

## Function inventory (every definition in localtime.c)

| # | C function (localtime.c) | Port location | Verdict | Notes |
|---|---|---|---|---|
| 1 | `init_ttinfo` (:107) | `ttinfo` struct literals in `tzparse` / `build_posix_transitions` | MATCH (inlined) | Sets utoff/isdst/desigidx and clears ttisstd/ttisut; every C call site is mirrored by a full literal with `tt_ttisstd: false, tt_ttisut: false`. |
| 2 | `detzcode` (:117) | `read_be_i32` | MATCH | C's byte ladder with the sign-bit branch is exactly a big-endian two's-complement i32 read (the non-two's-complement compensation is a no-op on two's-complement targets, which i64/i32 Rust is by definition). |
| 3 | `detzcode64` (:143) | `read_be_i64` | MATCH | Same argument for 64-bit. |
| 4 | `differ_by_repeat` (:169) | `differ_by_repeat` | MATCH | `TYPE_BIT - TYPE_SIGNED = 63 >= SECSPERREPEAT_BITS (34)` so the early `return 0` never fires; transcribed anyway. `t1 - t0 == SECSPERREPEAT` via `checked_sub` — operands come from the sorted `ats` table with `t1 >= t0`, so no wrap is reachable; `checked_sub` returning `None` (≠) is conservative-identical. SECSPERREPEAT_BITS=34, SECSPERREPEAT=400*31556952 verified against private.h:154-157. |
| 5 | `tzloadbody` (:210) | `tzload` + `parse_tzif` + `parse_tzif_block` + `parse_footer_posix` + `extend_with_posix` + `set_default_type` | MATCH (after fix rounds, see findings) | Decomposed; every C step re-derived: `:`-prefix strip; `pg_open_tzfile` (SEAMED, see seam audit) with `-1` → `NotFound`; single bounded read of `2*sizeof(tzhead) + 2*sizeof(struct state) + 4*TZ_MAX_TIMES = 2*44 + 2*23440 + 4*2000 = 54968` bytes (struct sizes re-derived from pgtz.h field layout and confirmed by the c2rust `[c_char; 54968]` buffer); 32-bit block parse, then 64-bit re-parse over it when version ≠ 0 (the 64-bit block fully overwrites `sp`, as C's second loop pass does); header count validation (`0 <= cnt < TZ_MAX_*`, ttisstd/ttisut ∈ {0, typecnt}) — negative i32 counts become huge usize and fail the strict upper bounds, mirroring C's `0 <= cnt` tests; C's single up-front `nread < tzheadsize + …` length check accepts/rejects exactly the byte counts the port's incremental `take` consumes (both EINVAL/`Invalid`); transition read with the `at <= TIME_T_MAX` / `TIME_T_MIN` clamps (vacuous for i64, transcribed), duplicate-time drop (`keep[i-1] = false`, `timecnt--`) and strictly-decreasing rejection; type-index bound check against header typecnt; ttinfo parse (`isdst < 2`, `desigidx < charcnt`); chars copy + forced NUL; leap-second parse with `tr < 0` rejection and the `28*SECSPERDAY - 1` spacing / `corr == prevcorr ± 1` invariants; ttisstd/ttisut flag bytes restricted to {0,1}; footer region per C's break/memmove bookkeeping (see finding F3); abbreviation reuse + graft (finding F1); `typecnt == 0` → EINVAL **after** the footer graft; goback/goahead detection via `typesequiv` + `differ_by_repeat` with C's exact loop bounds; defaulttype heuristics (`set_default_type`) — all three stages re-derived line-by-line against :528-575. |
| 6 | `tzload` (:585) | `tzload` | MATCH | malloc-failure branch has no owned-model counterpart. Errno results collapsed to `TzLoadError::{NotFound, Invalid}` — preserves the only distinction PG callers test (ENOENT vs anything else). Note: C returns `errno` if `close()` fails; the port drops the `File` (close error unobservable). A close failure on a read-only fd has no reachable trigger; recorded as an accepted representational note, not absent logic. |
| 7 | `typesequiv` (:601) | `typesequiv` | MATCH | Same bounds checks (the C `sp == NULL` arm is unrepresentable with `&state`); utoff/isdst/ttisstd/ttisut equality plus abbreviation `strcmp` via `read_cstr` (NUL-bounded, identical comparison). |
| 8 | `getzname` (:641) | `parse_zone_name` (unquoted arm) | MATCH | Stops at NUL/digit/`,`/`-`/`+`; the port's `&str` end-of-string is C's NUL; delimiters are all ASCII so byte/char scanning coincide. |
| 9 | `getqzname` (:662) | `parse_zone_name` (`<` arm) | MATCH | Scan to `>`; missing `>` (C: `*name != '>'` after hitting NUL) → `None` → caller returns false. |
| 10 | `getnum` (:679) | `getnum` | MATCH | First byte must be a digit; running value rejected the moment it exceeds `max`; `num < min` rejected at the end; no digit-count limit. `checked_mul/checked_add` cannot fire for C-reachable values (max ≤ 366 ⇒ num ≤ 3669 before each check) so behavior is identical on every input. |
| 11 | `getsecs` (:709) | `getsecs` | MATCH | hh ∈ [0,167] (`HOURSPERDAY*DAYSPERWEEK - 1`), mm ∈ [0,59], ss ∈ [0,60]; optional `:` levels nested exactly as C. |
| 12 | `getoffset` (:750) | `parse_offset` | MATCH | Leading `-`/`+`; negation after parse. |
| 13 | `getrule` (:777) | `parse_rule` | MATCH | `J` (1..365, leap day never counted), `M` m.n.d (1..12 / 1..5 / 0..6 with mandatory dots), bare digit (0..365); `/time` via getoffset else default `2*SECSPERHOUR`. Constants verified against private.h. |
| 14 | `transtime` (:838) | `transtime` | MATCH | All three rule arms re-derived, including Zeller's congruence (`m1`, `yy0/yy1/yy2`, `dow` negative fixup), the week-advance loop capped by `mon_lengths`, and the month-prefix accumulation. C does int32 arithmetic; values are bounded (≤ ~33M) for all caller-reachable inputs (year ∈ [~1570, ~2371] from the tzparse loop), so the port's i64 arithmetic is value-identical. `mon_lengths`/`year_lengths`/`isleap` tables verified (truncating `%` matches C for negative years). |
| 15 | `tzparse` (:935) | `tzparse` + `build_posix_transitions` | MATCH | lastditch: stdname = whole string, stdoffset 0, no DST (C advances `name` to its end). Non-lastditch: quoted/unquoted std name, empty-rest rejection (`*name == '\0'` after the abbrev), offset parse. C's two `sizeof sp->chars < charcnt` checks are merged into one final check — accept/reject identical since charcnt only grows (CHARS_SIZE = 2*(TZ_STRLEN_MAX+1) = 512, verified pgtz.h:52). DST: empty-name rejection, optional offset else `stdoffset - SECSPERHOUR`, `TZDEFRULESTRING` (",M3.2.0,M11.1.0", leading comma pre-consumed) when rest empty (PG's `load_ok` is constant false so this is unconditional), `,`/`;` rules branch, and leftover-text rejection. The C "use the loaded TZDEFRULES transitions" else-branch (:1127-1221) is dead code in PG — reachable only with `*name == '\0'`, which the `!load_ok` rewrite to TZDEFRULESTRING makes impossible; the surviving observable behavior (`*name != '\0'` → false) is preserved. Rules loop: `yearbeg` walk-back do-while (continue while `EPOCH_YEAR - YEARSPERREPEAT/2 < yearbeg`) with `janoffset = -yearsecs` on time overflow; per-year `transtime` pair, reversed swap, the `reversed || (starttime < endtime && endtime - starttime < yearsecs + (stdoffset - dstoffset))` emission gate, `TZ_MAX_TIMES - 2 < timecnt` break, `types = !reversed`/`reversed`, `yearlim = year + YEARSPERREPEAT + 1` extension only on the endtime store, `janfirst` advance with overflow break and `janoffset = 0` reset — all with C's exact break/no-increment structure. Perpetual DST collapse (`ttis[0] = ttis[1]`, typecnt 1) and `YEARSPERREPEAT < year - yearbeg` → goback=goahead=true. chars layout (std at 0, dst at stdlen+1, each NUL-terminated) and charcnt identical; the port's up-front `state::default()` matches C's explicit field writes on all live paths. |
| 16 | `gmtload` (:1244) | `gmtload` | MATCH | `tzload("GMT", NULL, sp, true)` failure → `tzparse("GMT", sp, true)`. |
| 17 | `localsub` (:1258) | `localsub` | MATCH (fixed, round 1) | The `sp == NULL` → gmtsub arm is unrepresentable (`&state`); `pg_localtime` always has a state, matching C's only call site. goback/goahead extrapolation now mirrors C exactly: map `t` by whole 400-year cycles (`repeat_mapping`, wrapping arithmetic as with `-fwrapv`, "cannot happen" range check → `None`), recurse on `newt`, then shift `tm_year` by ±years with the `INT_MIN <= newy <= INT_MAX` check — so `timesub`'s leap-second scan sees the mapped time, as in C (see finding F2). Normal path: defaulttype when `timecnt == 0 || t < ats[0]`, else binary search (`partition_point(at <= t)` ≡ C's `lo=1..hi=timecnt` loop given the `t >= ats[0]` precondition), `timesub` with `tt_utoff`, then isdst/tm_zone (tm_gmtoff set inside timesub, as C). |
| 18 | `pg_localtime` (:1343) | `pg_localtime` | MATCH | Thin wrapper; returns an owned `pg_tm` instead of the C static `tm` (representational). |
| 19 | `gmtsub` (:1356) | `gmtsub` | MATCH | Lazily-initialized GMT state (`thread_local OnceCell` for C's static malloc'd `gmtptr`; the malloc-failure NULL return has no counterpart); `timesub`; tm_zone = WILDABBR ("   ") when offset ≠ 0 else `gmtptr->chars` first abbrev. C stores tm_zone into the out-struct even when timesub failed — unobservable through the NULL-return contract; the port's `None` propagation is equivalent. |
| 20 | `pg_gmtime` (:1388) | `pg_gmtime` | MATCH | `gmtsub(timep, 0)`. |
| 21 | `leaps_thru_end_of_nonneg` (:1399) | `leaps_thru_end_of_nonneg` | MATCH | `y/4 - y/100 + y/400`. |
| 22 | `leaps_thru_end_of` (:1405) | `leaps_thru_end_of` | MATCH | `-1 - f(-1 - y)` for negative y (no overflow: `-1 - y` ≤ INT_MAX for all i32 y). |
| 23 | `timesub` (:1413) | `timesub` + `leap_correction` | MATCH (fixed, round 1) | Leap scan: latest `ls_trans <= t` gives corr; `hit` iff exact hit and corr strictly increased over the previous entry (`i == 0 ? 0 : lp[-1].ls_corr`). Year loop: truncating div/mod, `tdelta` int-range check → `None` (C `EOVERFLOW`/NULL — errno is not part of the port surface), `idelta == 0` fixup, `increment_overflow` on `newy`, leapdays via `leaps_thru_end_of` (now `wrapping_sub(1)`, finding F4). `rem += offset - corr`; rem/idays normalization loops with overflow guards; `tm_year = y - TM_YEAR_BASE` overflow → None; wday formula with the "extra" mods (now `y.wrapping_sub(EPOCH_YEAR)`, finding F4; remaining terms bounded, re-derived: |leaps(y−1)| ≤ ~5.4e8 with y ≥ INT_MIN+1900 after the tm_year guard); hour/min; `tm_sec = rem % 60 + hit`; month walk over `mon_lengths`; `tm_mday = idays + 1`; `tm_isdst = 0`; `tm_gmtoff = offset`. EPOCH_WDAY=4 (TM_THURSDAY), TM_YEAR_BASE=1900, AVGSECSPERYEAR=31556952 verified against private.h. |
| 24 | `increment_overflow` (:1538) | `increment_overflow` | MATCH | `checked_add` ≡ C's pre-checked add. |
| 25 | `increment_overflow_time` (:1556) | `increment_overflow_time` | MATCH | For i64 pg_time_t the C predicate reduces to a full-range checked add; all call sites pass j values within i32-derived bounds. |
| 26 | `leapcorr` (:1573) | `leapcorr` | MATCH | Reverse scan, first `t >= ls_trans` wins, else 0. |
| 27 | `pg_next_dst_boundary` (:1609) | `next_dst_boundary_impl` (+ `pg_next_dst_boundary`, `pg_next_dst_boundary_tristate`) | MATCH | Out-params + tri-state int return modeled as `NextDstBoundary` enum (Overflow / NoTransition{before} / Boundary), preserving the −1/0/1 distinction. timecnt==0 → defaulttype NoTransition; extrapolation via `repeat_mapping` (C's `seconds/YEARSPERREPEAT/AVGSECSPERYEAR` cycle count equals `seconds/SECSPERREPEAT` — truncated division composes; `tcycles != icycles` can never fire with integer pg_time_t) with recursion and `boundary ∓= seconds` adjustment (C adjusts the out-param even for 0/−1 results, where it is undefined garbage by contract — unobservable); `t >= ats[timecnt-1]` → last segment NoTransition; `t < ats[0]` → defaulttype/first-segment Boundary at `ats[0]`; else binary search `lo=1, hi=timecnt-1` transcribed verbatim, before/after from `types[lo-1]`/`types[lo]` at `ats[lo]`. |
| 28 | `pg_interpret_timezone_abbrev` (:1742) | `pg_interpret_timezone_abbrev` + `find_abbrev` | MATCH | Abbrev located by walking NUL-delimited slots (C's `while/strcmp/skip-to-NUL` loop, exactly — unlike tzloadbody's byte scan); cutoff = first transition > t via partition_point (≡ C's lo=0..hi=timecnt loop); backward scan < cutoff, then defaulttype, then forward scan ≥ cutoff; miss → None (C false). |
| 29 | `pg_timezone_abbrev_is_known` (:1860) | `pg_timezone_abbrev_is_known` | MATCH | Same `find_abbrev`; scan ttis[0..typecnt); first use records isfixed=true + gmtoff/isdst; later conflicting use flips isfixed=false and breaks, leaving the first use's gmtoff/isdst (C leaves the out-params at the first-stored values likewise); later matching use continues. |
| 30 | `pg_get_next_timezone_abbrev` (:1935) | `pg_get_next_timezone_abbrev` | MATCH | `indx < 0 || >= charcnt` → None (negative handled by `usize::try_from`); result = current abbrev; index advanced past trailing NUL. Owned `String` for the C `const char *` (representational). |
| 31 | `pg_get_timezone_offset` (:1964) | `pg_get_timezone_offset` | MATCH | All ttis[1..typecnt) must equal ttis[0].tt_utoff; success returns it. Port iterates from 0 (index 0 trivially equal). |
| 32 | `pg_get_timezone_name` (:1988) | `pg_get_timezone_name` | MATCH | `&pg_tz` is non-null by construction; returns `TZname`. |
| 33 | `pg_tz_acceptable` (:2003) | `pg_tz_acceptable` | MATCH | `time2000 = (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY` (2451545/2440588/86400 verified against datatype/timestamp.h); NULL or `tm_sec != 0` → false. |

File-scope data: `wildabbr` ("   "), `gmt` ("GMT"), `TZDEFRULESTRING`,
`mon_lengths`, `year_lengths`, and the static result `tm` (replaced by owned
return values) all verified. The `struct rule`/`enum r_type` become
`TransitionRule`/`Rule` with identical fields/ranges. Constants verified
against headers: TZ_MAX_TIMES 2000, TZ_MAX_TYPES 256, TZ_MAX_CHARS 50,
TZ_MAX_LEAPS 50 (tzfile.h:100-108), TZ_STRLEN_MAX 255 (pgtime.h:54),
chars buffer 512 (pgtz.h:52), tzhead field order/offsets (tzfile.h:41-49 ⇒
counts at byte offsets 20/24/28/32/36/40), SECSPER*/DAYSPER*/MONSPERYEAR/
EPOCH_YEAR/EPOCH_WDAY/TM_YEAR_BASE/YEARSPERREPEAT/AVGSECSPERYEAR/
SECSPERREPEAT(_BITS) (private.h:97-157), input buffer 54968 bytes.

Representational notes (accepted, port-wide convention): timezone names,
POSIX TZ strings, and abbreviations are `&str`/`String` rather than raw byte
strings — non-UTF-8 abbreviation bytes in a TZif file (legal for C, absent
from real tzdata) cannot round-trip; `errno` values collapse to
`TzLoadError`; `pg_tm.tm_zone` is an owned `Option<String>`; the C
`name == NULL → TZDEFAULT` arm of tzloadbody is unrepresentable (no PG caller
passes NULL). The GMT state is per-thread rather than process-global.

## Findings (fix round 1 — all fixed in this round, re-audited from scratch)

- **F1 `tzloadbody` abbreviation reuse (DIVERGES → fixed).** C scans
  `for (j = 0; j < charcnt; j++) strcmp(sp->chars + j, tsabbr)` — byte-by-byte,
  so a footer abbreviation that is a *suffix* of an existing one (e.g. "ST"
  inside "AKST\0") is reused at the suffix offset. The port stepped slot-by-slot
  (strlen+1), which appends instead of reusing — different desigidx/charcnt and
  a different accept/reject outcome near the TZ_MAX_CHARS capacity check.
  Fixed to the byte-by-byte scan; regression test
  `footer_abbrev_reuse_matches_c_suffix_scan` added.
- **F2 `localsub` extrapolation (DIVERGES → fixed).** C maps an out-of-table
  time into the table by whole 400-year cycles, runs `timesub` on the *mapped*
  time, and shifts `tm_year` back (with an explicit int-range check). The port
  used the mapped time only to pick the ttinfo and ran `timesub` on the raw
  time — divergent for goback/goahead zones with leap-second tables (corr/hit
  evaluated at the wrong instant) and at extreme-year overflow edges. Fixed by
  porting `localsub` structurally (recursion + year shift + range check), with
  `repeat_mapping` now returning the C `seconds`/`years` pair (also used by
  `pg_next_dst_boundary`, whose cycle arithmetic is provably the same value).
- **F3 TZif footer gating (DIVERGES → fixed).** C has no magic check, and the
  footer examination runs on whatever the buffer holds when the version-0
  break exits the loop: whole file (v1 break), bytes from the second header
  (second-header version 0), or bytes after the 64-bit block. The port gated
  the footer on `version != 0` only, never examining it in the version-0 break
  cases, and read the TZ string without C's stop-at-first-NUL semantics. Fixed:
  footer region selected per C's break/memmove bookkeeping and the TZ string
  truncated at the first NUL.
- **F4 `timesub` `-fwrapv` arithmetic (DIVERGES in debug builds → fixed).**
  `y - EPOCH_YEAR` (reachable when y ∈ [INT_MIN+1900, INT_MIN+1969] after the
  tm_year guard) and `newy - 1`/`y - 1` in the year-normalization loop can
  overflow i32; C (built with `-fwrapv`) wraps and returns a defined result,
  while the port would panic in debug builds. Converted to `wrapping_sub`,
  matching C on every input. (All other arithmetic re-checked for overflow
  reachability; bounded.)

Also fixed in this round: pre-existing `clippy::absurd_extreme_comparisons`
deny-level errors on the deliberately transcribed vacuous `TIME_T_MIN/MAX`
range checks (scoped `#[allow]` with justification).

## Seam audit

- `crates/backend-timezone-pgtz-seams` declares exactly one seam,
  `pg_open_tzfile(name, want_canonical) -> Option<(File, Option<String>)>`,
  owned by the (unported) `backend-timezone-pgtz` unit. The dependency cycle is
  real: pgtz.c calls `tzload`/`tzparse`/`pg_localtime` (this crate) while
  localtime.c calls `pg_open_tzfile` (pgtz.c) — a direct dependency cannot
  exist in either direction. The call site in `tzload` is thin marshal +
  delegate (strip `:` prefix, one call, `None` → `NotFound`); no branching or
  computation in the seam path. `want_canonical` mirrors C's nullable
  `canonname` out-buffer; the canonical-name production stays in the owner.
  Until the owner lands, calls panic loudly (unported callee — acceptable).
- `backend-timezone-localtime::init_seams()` is empty (the crate owns no
  inward seam crate) and is invoked by `seams-init::init_all()`. No `set()`
  call exists outside the owner; the only `set()` in this crate is a
  `#[cfg(test)]` stub simulating "no tzdata present".
- No function body in this crate was replaced by a seam call; all localtime.c
  logic lives here.

## Verdict

**PASS** (after fix round 1). All 33 functions MATCH (with the single
justified SEAMED call to `pg_open_tzfile` inside `tzload`); no seam findings.
`cargo test -p backend-timezone-localtime` (11 tests) and
`cargo clippy -p backend-timezone-localtime` are clean; full workspace builds.
