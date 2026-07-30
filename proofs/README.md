# proofs/ — machine-checked C≡Rust equivalence proofs

pgrust claims behavioral faithfulness to PostgreSQL 18.3. This tree is
the strongest evidence behind that claim: a campaign of **direct
equivalence proofs** between shipped pgrust functions and verbatim
PostgreSQL C, checked by [Kani](https://model-checking.github.io/kani/)
(CBMC underneath).

Each proof is a *dual-execution harness* (`-Z c-ffi --c-lib`): the real
pgrust function and the verbatim vendored C function are compiled into
one goto-program, the harness feeds them identical symbolic inputs, and
CBMC proves the outputs equal for **every** input within the stated
bounds (input length / loop unwind / documented fences). This is
exhaustive over the stated domain, not sampled — a green harness is a
theorem, with its bounds recorded in the ledger.

## Headline results

Counted from `USER_FACING_FUNCTIONS.tsv` at this commit (3,189 rows =
every SQL-callable internal builtin in the PostgreSQL 18.3 `pg_proc`
catalog, completeness-audited in `LEDGER-AUDIT-2026-07-28.md`):

- **1,086 functions proved** equivalent to the verbatim C (status
  `proved(...)`, bounds in each row), plus 26 more covered by exhaustive
  or mass-sampled *native differential* testing where the solver walls
  (`tested(differential)` — weaker than proof, recorded as such).
- **The campaign found 12 real divergence findings: 8 pgrust bugs and
  4 upstream PostgreSQL bugs.** (One site — `money` division at
  `MIN/-1` — contributed one finding to each column: the pgrust side
  panicked and the C side is itself defective, so all three observed
  behaviors differed.)
- The **eight pgrust porting bugs**, all fixed in this tree:
  1. `pg_utf8_islegal` out-of-contract behavior;
  2. `numeric` digit comparison;
  3. the `numeric_smaller`/`numeric_larger` tie keeping the wrong
     argument's display scale;
  4. `PG_char_to_encoding` rejecting non-UTF8 names wholesale where C
     cleans byte-wise;
  5. `hstore_recv` accepting invalid-encoding/NUL bytes C rejects;
  6. a `hashoidvector` validation gap (the oidvector validity check C
     ereports was only a debug assert);
  7. NaN-unaware GiST box comparisons — one root cause surfacing in two
     functions, the box-union `adjust_box` and `gist_box_same`, both
     regressions of upstream's back-patched bug #14238 fix and
     adjudicated together in `gist-geo/ADJUDICATION-NAN-PLANES.md`;
  8. the `money` division `MIN/-1` case, which panicked the backend
     (now a clean SQLSTATE 22003 like `int8div`).
- **Four genuine bugs in upstream PostgreSQL C** were isolated by the
  same testing program, each ground-truthed against real running
  PostgreSQL 18.4 (the first two directly off proof counterexamples;
  the other two surfaced as cross-platform splits when the proof suite
  was replicated on Linux-aarch64, then confirmed differentially on
  live servers on both platforms):
  1. `macaddr_in` accepts >8-hex-digit fields via C99 `sscanf %x`
     mod-2^32 wraparound;
  2. `cash_div_int8/4/2` misses the `INT64_MIN / -1` overflow guard
     that the 2024 money-overflow sweep added everywhere else (x86-64
     survives via the SIGFPE handler as an odd error; aarch64 silently
     returns a wrong value) — the C side of the `MIN/-1` site above;
  3. `hashchar`/`hashcharextended` widen a bare `char` without the
     `(uint8)` cast the "char" comparison operators use deliberately,
     so C PostgreSQL itself returns different hash values on
     signed-char vs unsigned-char ABIs (x86-64 vs Linux-aarch64),
     splitting hash partition routing across architectures — see
     `char/ADJUDICATION-CHAR-SIGNEDNESS.md`;
  4. `tidin` parses TID fields with `strtoul` and no
     `endptr == nptr` no-conversion check, so C PostgreSQL accepts
     `'(,5)'` as `(0,5)` on glibc but rejects it on BSD libc —
     platform-dependent acceptance.
- Several more divergence *candidates* were adjudicated as ratified
  non-surfaces rather than bugs: platform splits harmless at the datum
  level (the `char` I/O-and-cast widening seam), integer shift-count
  UB, and version-line differences (`pg_lsn_out` PG18 vs PG19 format).
  Each is recorded in the ledger row that found it.
- The campaign also surfaced **verification-tool defects** (CBMC's
  non-canonical NAN constant; a per-call-nondeterministic `f64::sqrt`
  model; a Kani 0.67 `Box` provenance defect) — each has a standing
  witness harness in-tree so a fixed toolchain is detected. See
  `TRIAGE.md`.

A caveat we record deliberately: equivalence proofs find *porting*
errors, never *inherited* ones — where pgrust faithfully ports a C bug,
the proof is green. The four upstream bugs above were caught only where
the divergence surfaced on the pgrust side of history or as a
platform split the cross-platform suite replication exposed.

## Layout

- `USER_FACING_FUNCTIONS.tsv` — the target ledger. One row per catalog
  function: `oid`, `name`, `source_file` (the implementing pgrust
  crate), `status`, solver `class`, `notes`. Statuses:
  `untriaged` → `candidate` / `excluded(<reason>)` → `in-progress` →
  `proved(<bounds>)` / `tested(differential ...)` /
  `divergence(<adjudication>)` / `wall(<mechanism>)` /
  `blocked(<refactor needed>)`. The parenthetical is load-bearing: a
  `proved` row states exactly what domain and modulo which documented
  seams/models the theorem holds; a `wall` row records the RSS cap it
  was measured under.
- `SUITE.tsv` — the harness manifest (1,850 rows: family, harness name,
  kani flags, expected result, tier, documented solve time). Tiers:
  `per-commit` (<10s documented), `release-gate` (10–30s),
  `calibration` (>30s greens), `defect-witness` (must-FAIL negative
  controls — they guard against a vacuous rig and ride along with every
  tier), `unmeasured`.
- `TRIAGE.md` — the measured solver cost model and the
  proof-engineering laws the campaign learned (what walls, why, and the
  standard remedies). Read this before writing a new harness.
- `LEDGER-AUDIT-2026-07-28.md` — mechanical completeness audit of the
  ledger against the `pg_proc` catalog.
- `PROVENANCE-AUDIT.md` — fetch-and-diff audit of every vendored C file
  against `REL_18_STABLE`.
- `<family>/` — one standalone harness crate per function family
  (e.g. `utf8/`, `geo-cmp/`, `mbconv/`). Each crate: `Cargo.toml`
  (path-dependency on the **real shipped pgrust crate**, plus a
  `[workspace]` escape so it is not a member of the main workspace),
  `c/` or `csrc/` (verbatim vendored PostgreSQL C, `pg_`-prefixed, with
  provenance and every shimmed line documented in the header),
  `src/lib.rs` (the harnesses), and often a `run-all.sh`. Some families
  keep uncompiled upstream snapshots (`*/orig/`, `*_upstream.c`,
  `*_master.c`, `csrc-shared/`) purely for byte-auditing the shims
  against upstream.

## Rules the campaign runs under

1. **Prove shipped code.** Harness crates path-depend on the real pgrust
   crates. Never copy the Rust function under proof.
2. **Vendor C verbatim.** Fetch from postgres/postgres, rename with a
   `pg_` prefix only, document provenance (file, ref, date) and every
   shimmed line (palloc/StringInfo/ereport replacements). Shims replace
   plumbing, never logic.
3. **Divergences are deliverables.** A counterexample is decoded,
   reproduced against a real running C PostgreSQL on the target
   platform (the ground-truth law — see `TRIAGE.md`), recorded in the
   ledger, and adjudicated: either a bug (fixed in pgrust, or reported
   upstream) or a ratified non-surface (the harness's
   canonicalizer/fence is updated and the ruling documented).
4. **10-second budget.** Standing per-commit harnesses must solve in
   ≤10s so the suite stays runnable as a local gate; slower proofs live
   in the release-gate/calibration tiers or go through the escalation
   ladder in `TRIAGE.md`.

## Running proofs

Prerequisites: [cargo-kani](https://model-checking.github.io/kani/install-guide.html)
(the campaign ran on Kani 0.67) and a C toolchain.

One harness, by hand:

```sh
cd proofs/utf8
cargo kani -Z c-ffi --c-lib c/pg_wchar.c --c-lib c/pg_wchar_kernels.c \
  --solver kissat --harness islegal_len1 --exact
```

The exact flags for every harness are the `flags` column of `SUITE.tsv`;
many families also have a `run-all.sh` with the family recipe baked in.

The whole suite:

```sh
cd proofs
./run-suite.sh per-commit     # documented <10s greens + must-fail controls
./run-suite.sh release-gate   # + the 10-30s harnesses
./run-suite.sh all            # everything incl. calibration and unmeasured
```

Harnesses run **strictly serially** (one kani/cbmc solve at a time —
mandatory memory protocol), each under `timeout` plus a 6 GiB RSS
watchdog polling the solver process tree every 15s. Scoreboard goes to
stdout, machine-readable rows to `suite-results.tsv`. Exit is nonzero if
any green-expected harness fails/times out, or if any must-fail control
verifies SUCCESSFUL (a vacuous gate — the worst outcome).

## Licensing

The C sources under `proofs/*/c*` are verbatim (or thinly shimmed and
documented) copies of PostgreSQL source code, copyright the PostgreSQL
Global Development Group and the Regents of the University of
California, distributed under the PostgreSQL License — reproduced in
this repository's top-level `NOTICE`. The vendored files retain their
upstream headers.
