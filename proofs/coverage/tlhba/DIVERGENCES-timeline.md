# timeline_diff divergence log (lane p1-tlhba)

## D1 — scan_u32 wrapped past the 64-bit boundary where libc clamps (FIXED)

- **Found**: 2026-08-02, first `arms_smoke` run of the fresh target (directed
  seed from the overflow hazard class, CAMPAIGN-INTELLIGENCE §A).
- **Repro**: history file line `18446744073709551616\t0/0\tr\n` (any %u/%X
  digit string with magnitude >= 2^64), e.g. target tli 9:
  C parses tli = `0xFFFFFFFF` and FATALs the final
  `targetTLI <= lasttli` check ("invalid data in history file");
  pgrust parsed tli = `0` (wrap mod 2^32) and returned a 2-entry history.
  Verdict + entries planes both diverged.
- **Mechanism**: libc sscanf `%u`/`%X` converts through strtoul/strtoumax:
  64-bit magnitude accumulation, CLAMP to ULONG_MAX on overflow (ERANGE,
  sign ignored), then 32-bit truncation on assignment. The port's
  `scan_u32` accumulated wrapping in u32, which agrees with libc only for
  magnitudes < 2^64.
- **Classification**: pgrust bug (timeline/src/lib.rs `scan_u32`); fixed in
  this lane (64-bit sticky-clamp accumulator + negate-in-u64, truncate).
  Unit tests added (timeline/src/tests.rs sscanf_edge_cases).
- **Ground truth**: macOS libc verified directly (scanftest: `2^64 ->
  FFFFFFFF`, `-2^64-1 -> FFFFFFFF`, `-2^32-2 -> FFFFFFFE`); strtoul clamp is
  POSIX-required. glibc (oracle platform of record) confirmation rides the
  fleet campaign — the local docker daemon is corrupted (blob io-errors), so
  no local postgres:18.3 replay was possible; the vendored timeline.c is
  byte-verbatim and the conversion is pure libc, so the fleet run compares
  the exact production semantics.

## Watchlist (platform-variance candidates, not divergences yet)

- `%X` on `0x` with no following hex digit (e.g. `5\t0x/0\tr`,
  `5\t0xZ/0\tr`): BSD and glibc scanf disagree historically on whether the
  prefix consumes and what converts. macOS: `0x` -> n=1, v=0. The port
  currently models parse-`0`-leave-`x`. Directed seeds committed; the fleet
  (glibc) campaign adjudicates the platform of record. If glibc differs, the
  port follows glibc and the macOS delta becomes a documented
  oracle-platform-variance carve in the target header.

## D2 — bare "0x" %X prefix: glibc consumes, BSD ungets; port modeled BSD (FIXED)

- **Found**: 2026-08-02, fleet smoke job pgrust-fuzz-campaign-1785663216-0c8e-40161
  @ 4fdef9b55c50690473e5fb4bbfb6dfc6d802554f, exec 52 (artifact
  crash-470424f6fc5ad3663418a5dd9fad637f7b8b5419 = the committed
  parse-hex-prefix-bare watchlist seed, `5\t0x/0\tr\n`).
- **Mechanism**: glibc scanf %X consumes a leading "0x"/"0X" even when no
  hex digit follows (conversion succeeds with value 0, stream positioned
  AFTER the 'x'), so the following '/' literal still matches and the line
  parses with nfields=3. BSD/macOS scanf ungets the 'x' (one-char
  pushback), the '/' match fails at 'x', nfields=2, FATAL. The port's
  scan_u32 skipped the prefix only when a hex digit followed — the BSD
  model. On the fleet (glibc): Rust FATALed, C accepted the line.
- **Classification**: pgrust bug against the platform of record
  (PostgreSQL production = Linux/glibc); fixed in scan_u32 (unconditional
  prefix consume, prefix alone is a valid conversion). Unit tests added.
- **Ground truth**: the fleet artifact IS the glibc adjudication (verbatim
  timeline.c + glibc sscanf on aarch64 Linux, the oracle of record).
- **Residue**: macOS local oracle now diverges from the port on the narrow
  shape `0[xX]/` — documented oracle-platform-variance carve in the
  timeline_diff module header; parse arm skips those inputs when the
  oracle is macOS libc. Fleet compares them fully.

## H1 — HARNESS DEFECT (not a product bug): CAPTURED grew without bound (FIXED)

- **Found**: 2026-08-02, 10M floor attempt job
  pgrust-fuzz-campaign-1785664439-4291-80679 @ b6855407bc — OOM
  (sanitizer_artifacts=1) at 310,271 of 10,000,000 execs, outcome
  crashed-early. Artifact oom-b0f0b4eadb0a0989cbc18c114a6e0188b54b922a
  (208 bytes) REPLAYS CLEAN in 8 ms — the tell that it is cumulative RSS,
  not a per-input blowup.
- **Mechanism**: the driver's emit_log_hook captures every PgError so the
  verdict/errcode/message planes can read the FATAL, but `CAPTURED` was
  cleared only inside `rust_read` (the parse arm). The list and name arms
  raise ERROR reports (tliOfPointInHistory non-contiguous /
  tliSwitchPoint not-in-history) on most inputs, so their reports piled up
  for the whole campaign.
- **Classification**: HARNESS DEFECT. Zero product implication: no plane
  read stale entries (each plane reads the report it just triggered), so
  no verdict was wrong — the harness merely leaked. Fixed by clearing once
  per exec in the `timeline_diff` dispatcher.
- **Bank**: the OOM input is committed as corpus/timeline_diff/oom-repro-b0f0b4ea
  (regression rail).

## H2 — HARNESS DEFECT (root cause of both OOMs): oracle leaked the fixture
##      stream on every FATAL escape (FIXED)

- **Found**: 2026-08-02, after H1 failed to stop the OOM. Floor attempt r2
  (pgrust-fuzz-campaign-1785665060-02e7-1962 @ df1c104065) OOMed again at
  305,140/10,000,000 execs — the near-identical exec count to r1 (310,271)
  being the tell that a fixed per-exec cost, not H1's variable one, was
  the driver.
- **Isolation** (local per-arm RSS probe, since RSS is invisible to every
  comparison plane): name 1.6 B/exec, list 4.9, parse-ok 11.5,
  **parse-FATAL 507**, parse-ENOENT 0. Splitting the FATAL arm three ways:
  Rust-only 0.8 B/exec, **C-oracle-only 466 B/exec**, Rust-with-hook-off
  0.0 — the leak was entirely on the C side.
- **Mechanism**: real `AllocateFile` registers the stream in fd.c's
  allocated-descriptor table, and `AtEOXact_Files` closes it when the
  ereport(FATAL) unwinds the transaction — which is exactly why
  timeline.c's parse loop can longjmp out with no fclose and leak nothing
  in production. The oracle has no transaction, so each FATAL exec
  stranded one fmemopen stream.
- **Classification**: HARNESS DEFECT (missing environment mock), NOT a
  product bug and NOT an upstream defect — PG's cleanup path is what makes
  the verbatim code correct, and the oracle simply lacked a counterpart.
  Fixed by tracking the live stream and closing it after the sigsetjmp
  escape (tl_close_leaked_stream, documented as the AtEOXact_Files
  stand-in). Verified: C-only FATAL 466 -> 0.0 B/exec; all arms flat;
  800k-exec plateau check ends at 24.7 MB with growth decayed to ~48
  KB/100k (allocator high-water, not a leak).
- **Lesson (durable)**: an in-process C oracle inherits PG's cleanup
  contracts, not just its computation. Any verbatim body that escapes via
  ereport while holding a resource NEEDS the resource-cleanup mock, or the
  campaign dies at a few hundred thousand execs — and no comparison plane
  can see it, because RSS is not a compared surface. The local
  smoke's own `peak_rss_mb: 431` at 46k execs was the signal, read too
  late: TREAT peak_rss AS A GATE ON EVERY SMOKE, not decoration.

## H3 — LANE-PROCESS DEFECT: the injection script reverted uncommitted
##      product edits (caught, corrected)

- **What happened**: `inject.sh` reverts each planted mutant with
  `git checkout -- <file>`. Runs of the post-cmin mutant checks therefore
  also reverted the UNCOMMITTED `never_reached!()` promotions in
  timeline/src/lib.rs. Commit 5dfca00c1a's message claims the promotion
  landed; the code change was not in it (the harness-side
  `arm_exception_audit()` call was, being a different file). Caught while
  recomputing the coverage equation — the residual still read bare
  `unreachable!()` at the expected lines.
- **Rule it violated**: "Commit product fixes BEFORE injecting" (lane
  brief). The rule exists for exactly this failure mode.
- **Correction**: promotions re-applied and committed BEFORE any further
  injection run; the 10M floor re-run at the corrected sha so the floor and
  the shipped source agree. 5dfca00c1a's message is superseded by that
  commit — the floor of record is the LAST one listed in the claim row.
- **Durable**: any revert-based mutation script must refuse to run with a
  dirty worktree. Recorded here rather than silently fixed because a stale
  claim ("promoted to executable exceptions") over code that does not do it
  is exactly the kind of overstatement the campaign's audits hunt for.

## MUTANTS AUDIT (trailing) — pgrust-mutants-audit-1785669167-16fa-85714 @ 7045a77259

- **Fleet verdict UNUSABLE as evidence**: `missed=31`, but the job's own
  differential-rail baseline FAILED TO BUILD/PASS, so no in-crate survivor
  was adjudicated by the rail. `fetch-mutants-results.sh` correctly refused
  to conclude ("RAIL-DID-NOT-RUN ... caught_by_rail=0 is INFRA, not
  corpus"). Cause is NOT this crate: the rail runs `cargo test --lib` over
  the whole shared `decoder_fuzz` lib and 5 SIBLING lanes' tests fail at
  this sha (timestamp_diff::replay_committed_corpus reports a live
  timestamp_in divergence; nodesfam x2; jsonpathexec recursion probe;
  diff::dasind_fp_contraction_witness). Reported to the coordinator as a
  shared-rail blocker; it makes the mutants rail vacuous for EVERY lane
  until fixed.
- **Local adjudication instead** (apply-mutant -> rebuild -> replay the
  committed corpus, the mutkill.sh mechanic). 31 missed split by carve:
  - **26 OUT-of-carve** (restoreTimeLineHistoryFiles, xlog_temp_path,
    create_temp_history_file, write_all_or_unlink, writeTimeLineHistory,
    archive_recovery_requested, xlog_archiving_active, init_seams,
    unlink_path) + the 5 WAIT_EVENT_* constant arithmetic mutants: ARID by
    construction — the differential target never calls them and their lines
    are carried by platform-other exception rows. The wait-event constants
    are no-op observability seams on both sides, never a compared surface.
  - **5 in-carve**, each replayed against the corpus:
    - `sscanf_history_line` 617 `||`->`&&`  KILLED
    - `scan_u32` 648 `<`->`<=`             KILLED
    - `scan_u32` 648 `+`->`*`              KILLED
    - `readTimeLineHistory` 191 `|`->`^`   **ARID, with proof** — see below
    - `tliOfPointInHistory` 506 `<`->`<=`  **SURVIVED = REAL GAP**, closed
- **191 is an EQUIVALENT mutant, not a gap**: `((hi as u64) << 32) | (lo as
  u64)` puts hi in bits 32..63 and the zero-extended lo in bits 0..31 —
  DISJOINT ranges, so `|`, `^` and `+` are the same function on every
  input. Verified by running the `+` variant too (also SURVIVED, as it must).
  No input can kill it; marked arid on the algebra, not on a shrug.
- **506 was a real hole, and a STRUCTURAL one**: `ptr < tle.end` ->
  `ptr <= tle.end` survived all 446 seeds because in a CONTIGUOUS history —
  everything readTimeLineHistory can build — entry k's end equals entry
  k-1's begin, so whenever ptr == entry k's end the NEWER entry k-1 already
  matched on `begin <= ptr` and entry k is never tested. The boundary is
  unobservable through the parse arm AT ANY EXEC COUNT; only the synthetic
  LIST arm can witness it (non-contiguous list, newer entries failing
  `begin <= ptr`: `<` falls through to the not-contiguous ERROR while `<=`
  returns Ok — a verdict-plane divergence). 5 witness seeds added; retest
  KILLED.
- **Durable lesson**: exec volume cannot substitute for a REACHABILITY
  argument. A predicate that only distinguishes states the primary
  constructor cannot build needs a second arm that builds those states
  directly — which is exactly why the list arm exists, and why it had to be
  seeded deliberately rather than left to the fuzzer.
