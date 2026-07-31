# DIV-1 (array_userfuncs_diff): array_position/array_positions panic in
# debug builds on a valid array whose lower bound is i32::MIN

Status: OPEN — pgrust-bug (debug-build-only panic; release behaviour is correct).
Found: 2026-07-31, lane p1-laneai, 300k-exec local smoke of array_userfuncs_diff.
Reproducer: DIV-1-lbound-int32min-position-debug-overflow.input
(`cargo fuzz run array_userfuncs_diff <file>`; arm 5 = array_positions).

## What happens

`crates/backend/utils/adt/array_userfuncs/src/lib.rs:319` (array_positions_internal)
and `:281` (array_position_internal) both compute

    let mut position = lbs[0] - 1;

With `lbs[0] == i32::MIN` this subtraction overflows. Rust panics under
`overflow-checks` (debug/test/fuzz builds); the release build wraps to
`i32::MAX` and the subsequent `position += 1` wraps back, which is exactly
what the C does.

C counterpart: `src/backend/utils/adt/array_userfuncs.c` array_position_common
`position = (ARR_LBOUND(array))[0] - 1;` and array_positions likewise —
PostgreSQL compiles with `-fwrapv`, so the wrap is DEFINED and intentional.

## Ground truth (oracle pin PostgreSQL 18.3, Docker postgres:18.3,
## Debian/glibc aarch64 — "PostgreSQL 18.3 (Debian 18.3-1.pgdg13+1)")

    select array_positions('[-2147483648:-2147483648]={7}'::int[], 7);
     {-2147483648}
    select array_position('[-2147483648:-2147483647]={7,8}'::int[], 8);
     -2147483647

So the input is SQL-reachable (an array with lower bound INT32_MIN is a valid
stored array: `lb + dim - 1` does not overflow, so ArrayCheckBounds accepts
it) and real PostgreSQL returns the wrapped position.

## Triage

pgrust-bug, severity low-but-real: shipped RELEASE behaviour matches C
bit-for-bit, but every debug/test/fuzz build panics on a valid SQL input.
Per this repo's arithmetic-audit convention (proofs/AUDIT-UNCHECKED-ARITH.md)
an arithmetic site that relies on release wrapping to match C must say so in
the code — `wrapping_sub(1)` / `wrapping_add(1)` with a comment naming the
`-fwrapv` C counterpart — rather than differ by build profile. Two sites:
lib.rs:281 and lib.rs:319 (plus their `position += 1` partners).

Not fixed here: this lane's charter forbids changing shipped crate code from
the fuzz harness. Handing to the lane owner.

## Harness state while this is open

`clamp_lb` in fuzz/core/src/array_userfuncs_diff.rs excludes exactly
`i32::MIN` from generated lower bounds, tagged `DIV-1`, so the campaign can
run. Removing that exclusion is the regression test once the fix lands.
