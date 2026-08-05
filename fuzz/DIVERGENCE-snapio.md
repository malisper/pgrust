# snapio_diff divergence record

## D1 (2026-07-30): strtou64 overflow+negation — pgrust-bug, FIXED

**Found by:** local 200k smoke of `snapio_diff` immediately after the LSan
leak fix (exec ~25,600), macOS host. NOT the leak — a value-plane divergence.

**Repro bytes:** selector `0x00` + ASCII `-518446744073709551616:-1:`
(minimized class: any xmin/xmax/xip literal with a minus sign and magnitude
>= 2^64). libFuzzer artifact reproduced by
`xid8funcs::tests::strtou64_libc_semantics` and the snapio_diff smoke test.

**Symptom:** `pg_snapshot_in("-18446744073709551616:...")` — C parses the
fxid as `18446744073709551615` (u64::MAX); pgrust parsed it as `1`.
Observed image diff: xmin `ff..ff` (C) vs `01 00 .. 00` (Rust).

**Root cause:** `crates/backend/utils/adt/xid8funcs/src/lib.rs strtou64`
saturated to `u64::MAX` on overflow and THEN applied the minus-sign
`wrapping_neg()`, yielding 1. libc strtoull (glibc strtol_l.c AND
macOS/BSD, host-probed both give `18446744073709551615, errno=ERANGE`)
returns the ERANGE clamp WITHOUT negation — the overflow branch returns
before the negate branch. Real PostgreSQL defers to libc here, so real PG
accepts `'-18446744073709551616:18446744073709551615:'::pg_snapshot` with
xmin = 2^64-1 on BOTH platforms; pgrust produced xmin=1 (and could
therefore also flip the xmin<=xmax validity verdict for some pairs).

**Why the existing proof missed it:** the ledger row carries
`strtou64 core proved(len<=8, ...)` — the repro requires >= 20 digits,
outside the proof fence. Textbook case of the campaign metric working:
the fuzz plane covers the region the bounded proof cannot.

**Triage (Csmith-style):** pgrust-bug. Both libcs agree, so this is NOT
oracle-platform-variance. Fix applied in the same worktree:
`strtou64` now `if overflow { MAX } else if neg { wrapping_neg }`;
unit tests extended (`-18446744073709551616`, `-518446744073709551616`);
minimal repro banked as corpus seed `in-err-lsan-… / in-div-strtou64-*`.

**Ground-truth status:** host C probe done (macOS clamp confirmed,
matches glibc source). Docker `postgres:18.3` replay still owed per the
ground-truth law before the ledger row is recut:
```sql
SELECT '-18446744073709551616:18446744073709551615:'::pg_snapshot;
-- expect: 18446744073709551615:18446744073709551615:
```

**Follow-up flags:**
- `adt/scalar strtoul_c` (tidin) has the same shape (sign + saturation) —
  the scalarxid lane must check its overflow/negation ordering.
- The `strtou64 core proved(len<=8)` ledger note should be annotated: the
  fence provably cannot see >=20-digit overflow inputs; the fuzz campaign
  is the covering evidence for that region.

## D1 ground-truth replay (coordinator, 2026-07-31)

Real PostgreSQL 18.3 (`postgres:18.3` Docker, Debian/glibc):
`SELECT '-18446744073709551616:18446744073709551615:'::pg_snapshot;` returns
`18446744073709551615:18446744073709551615:` — the un-negated ERANGE clamp,
exactly the vendored-oracle behavior. pgrust's pre-fix `xmin=1` was a REAL
shipped-code divergence; fix confirmed against ground truth (oracle law
satisfied — not a vendored-model-only record). `'12:13:0'::pg_snapshot`
errors on real PG (invalid input syntax), consistent with the leak-repro
path being an error arm.

## Sibling audit: adt/scalar strtoul_c (tidin) — CLEAR

Same sign+saturation shape audited per the D1 lesson: strtoul_c returns
None on overflow (line ~134), so the clamp is never negated — this matches
C tidin, which CHECKS errno==ERANGE and errors out. The strtou64 bug was
only reachable because C parse_snapshot IGNORES errno, letting the clamp
flow through as a value. Different caller contract, no fix needed.
Corroborated by round-1 scalarxid_diff fleet campaign (10.0M execs, tid
arms incl. overflow seeds, 0 divergences).
