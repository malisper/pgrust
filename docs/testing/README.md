# pgrust testing strategy

This doc is the single source of truth for what bug-finding investments we're
making in pgrust, what's already shipped, and what's queued. pgrust is a
from-scratch Postgres reimplementation in Rust. The surface area is enormous;
"more unit tests" won't find the bugs that matter. This plan is about stacking
techniques known to find database bugs at scale.

## Status dashboard

| # | Item | Tier | Status | Artifact |
|---|------|------|--------|----------|
| 1 | PostgreSQL isolation tester harness | 1 | **Done, gated on `pg_locks`** | [PR #58](https://github.com/your-github-org/pgrust/pull/58), `scripts/run_isolation.sh`, [docs/isolation-tests.md](../isolation-tests.md) |
| 2 | Differential testing vs real Postgres (SQLancer) | 1 | **Harness live; first findings captured** | [SQLancer adapter plan](./sqlancer-adapter-plan.md), [findings](./sqlancer-findings.md), `scripts/run_sqlancer_smoke.sh`, `scripts/run_sqlancer_triage.sh`, `/Users/jasonseibel/dev/2026/your-projects-parent/sqlancer` |
| 3 | cargo-fuzz on parser + wire-protocol decoder | 1 | **Parser + startup-packet targets live** | `fuzz/`, `cargo +nightly fuzz run parse -- -max_total_time=30`, `cargo +nightly fuzz run startup_packet -- -max_total_time=30` |
| 4 | Hegel property-based testing | 1 | Not started | — |
| 5 | Antithesis (paid Founder Package or free OSS Giveaway) | 2 | **Prep started** | [system summary](./antithesis-system-summary.md), `antithesis/` |
| 6 | cargo-mutants on executor + storage | 2 | Not started | — |
| 7 | proptest on narrow corners (JSONB, numeric, planner rewrites) | 3 | Not started | — |
| 8 | Miri nightly | 3 | Not started | — |
| 9 | loom/shuttle on buffer pool + lock manager | 3 | Not started | — |
| 10 | Detail.dev (LLM-agent code review) | 4 | Not started | free first scan |
| 11 | Jepsen/Porcupine linearizability | 4 | Deferred | premature (single-node) |
| 12 | In-process Rust DST (madsim / hand-rolled) | 4 | Deferred | high arch cost vs. Antithesis |

## Repo layout for new testing work

The plan doc lives in-repo under `docs/testing/`. New testing investments
should also live in the repo, but separated by mechanism so they don't get
mixed into normal unit and integration tests.

- `docs/testing/`
  strategy, status, outreach notes, execution plan, and
  [Antithesis system summary](./antithesis-system-summary.md) plus the
  [SQLancer adapter plan](./sqlancer-adapter-plan.md)
- `fuzz/`
  `cargo-fuzz` targets, corpora, and crash repro notes
- `tests/hegel/`
  Hegel-backed property tests and fixtures that should stay distinct from
  normal integration tests
- `antithesis/`
  container, workload, and platform-specific scaffolding for Antithesis
- `scripts/`
  harness entrypoints and glue code for orchestration that does not belong in
  Rust test crates

Principle: keep ordinary Rust tests close to the code they validate, but give
heavier external systems their own clearly named homes.

## Active todo board

This is the short list to drive implementation over the next few weeks.

| Priority | Todo | Output | Validation |
|---|---|---|---|
| Now | Land this strategy and scaffolding | `docs/testing/`, `fuzz/`, `tests/hegel/`, `antithesis/` | `git diff --stat` + doc review |
| Now | Wait for Antithesis reply | inbound thread / follow-up reminder | follow up after 5 business days if no reply |
| 1 | Widen SQLancer seed coverage | run more deterministic `WHERE` oracle seeds and save any repros | `PGRUST_SQLANCER_TRIAGE_SEED_COUNT=10 PGRUST_SQLANCER_QUERIES=50 scripts/run_sqlancer_triage.sh` |
| 2 | Turn SQLancer findings into parallelizable implementation tickets | preserve repro SQL and classify common-unsupported vs wrong-result vs harness-noise | update [findings](./sqlancer-findings.md) |
| 3 | Keep iterating on the live fuzz targets | parser/startup-packet corpora, first saved repro if found | `cargo +nightly fuzz run parse -- -max_total_time=30` and `cargo +nightly fuzz run startup_packet -- -max_total_time=30` |
| 4 | Choose the first property-test target without changing production code | Hegel/proptest design note and minimal external harness | doc review + targeted command |
| 5 | Turn Jepsen-style skepticism into local invariants first | common-use-case invariant list for restart, simple histories, and result parity | doc review + next harness picks use that list |
| 6 | Draft Antithesis system summary | [one-page architecture/failure-mode doc](./antithesis-system-summary.md) | doc review with Malis |
| 7 | Prepare local Antithesis harness shape | `antithesis/` layout with placeholders | file review + local dry-run plan |

## Validation loop

Every new testing investment needs a cheap local feedback loop before it is
considered real. The rule is simple: no strategy item is "started" until it
has a command we can rerun.

| Workstream | First command that must exist | Goal |
|---|---|---|
| Repo baseline | `cargo check` | project still builds |
| Core Rust tests | `cargo test --lib --quiet` | baseline regressions stay green |
| Isolation harness | `scripts/run_isolation.sh <spec>` | one spec reruns deterministically |
| Fuzzing | `cargo +nightly fuzz run <target> -- -max_total_time=30` | quick panic-finding smoke run |
| Property tests | targeted command once added | one shrinkable property reruns locally |
| Hegel | targeted Hegel command once added | one property can rerun and shrink |
| SQLancer smoke | `scripts/run_sqlancer_smoke.sh` | one seeded `WHERE` oracle run against pgrust |
| SQLancer triage | `scripts/run_sqlancer_triage.sh 1 2 3` | per-seed logs and summary table for blockers |
| Antithesis prep | local container or SDK-output dry run | infrastructure works before platform handoff |

When we add a new harness, update this table with the exact command.

Recent local validation on Apr 24, 2026:

- `cargo check` passes with the fuzz-only startup-packet hook excluded from the
  normal build.
- `cargo +nightly fuzz run startup_packet -- -max_total_time=10` completed
  187,083 executions with no crash.
- `cargo +nightly fuzz run parse -- -max_total_time=10` completed 46,249
  executions with no crash.
- `PGRUST_SQLANCER_TRIAGE_DIR=/tmp/pgrust-sqlancer-triage-smoke PGRUST_SQLANCER_QUERIES=10 PGRUST_SQLANCER_TIMEOUT_SECONDS=20 scripts/run_sqlancer_triage.sh 1 2`
  produced per-seed logs and extracted the current repeated blocker:
  `ERROR: column "table_schema" does not exist`.

## Common-use-case invariants first

For the next phase, the goal is not to test the strangest corners of Postgres.
It is to make pgrust trustworthy for the most common real-world usage patterns
as early as possible. That means prioritizing invariants around ordinary OLTP
and application-query behavior:

- acknowledged commit survives reconnect and restart
- single-row and small multi-row reads return the same results as Postgres for
  common `SELECT` / `INSERT` / `UPDATE` / `DELETE` shapes
- basic `WHERE` logic, `NULL` behavior, and boolean filtering do not silently
  drop or invent rows
- common `ORDER BY`, `LIMIT`, aggregate, and join queries preserve result
  correctness, even if performance is not yet mature
- malformed protocol input and aborted sessions fail with an error instead of
  panicking or poisoning later statements

That focus should drive both SQLancer and property-test work. We can expand to
rarer features later; the first objective is "useful Postgres subset for normal
apps", not "maximal feature exoticism."

## Tier 1 — do first

These are ranked by expected bugs-per-engineer-week for a young Postgres
reimplementation.

### 1.1 Isolation tester (done)

PR #58 wired up the upstream Postgres isolation tester against pgrust. It runs
`.spec` files from `postgres/src/test/isolation` against a fresh pgrust cluster
and diffs against upstream expected output. Builds `isolationtester` locally
via meson from the `postgres/` checkout.

**Gate:** `ISOLATION_REQUIRES_PG_LOCKS=1` blocks run until Malis ships:
- `pg_locks` (with wait-rows)
- `pg_catalog.pg_isolation_test_session_is_blocked(int, int[]) RETURNS bool`

Flip to `0` in `scripts/run_isolation.sh` once both land. Validated end-to-end
against real PG 17 (~50 specs pass) and live pgrust via
`PGRUST_ISOLATION_OVERRIDE=1` (the `two-ids` spec runs to completion and pgrust
produces real SSI serialization errors matching expected behavior).

### 1.2 Differential testing vs real Postgres (SQLancer)

SQLancer is the state of the art for finding *logic bugs* — wrong results, not
just crashes — in SQL databases. It has found 400+ bugs in SQLite, MySQL,
PostgreSQL, CockroachDB, TiDB, DuckDB, MariaDB. Its three main oracles:

- **PQS** (Pivoted Query Synthesis): pick a row, construct a query guaranteed
  to return it; if it doesn't, bug.
- **NoREC**: run an optimized vs. deoptimized form of the same query; results
  must match.
- **TLP** (Ternary Logic Partitioning): run `q`, `q WHERE p`, `q WHERE NOT p`,
  `q WHERE p IS NULL`; union must equal `q`.

**Why pgrust is ideal**: pgrust's entire goal is Postgres compatibility.
Every diff against real Postgres is either a bug or a documented scope cut.
CockroachDB tried this and struggled with known-divergence noise; we won't.

**Shortcut**: Antithesis maintains a fork at
[antithesishq/sqlancer](https://github.com/antithesishq/sqlancer). Start there.

**Plan**:
- Clone antithesishq/sqlancer into a sibling dir (not this repo).
- Start from the PostgreSQL adapter, but narrow it to a pgrust-specific common
  subset first. See [SQLancer adapter plan](./sqlancer-adapter-plan.md).
- Run in differential mode against a real Postgres container.
- Seed a known-divergence allowlist.
- Wire a nightly CI job (not merge-queue-blocking; results triaged manually).

**Budget**: 3-5 days to first-bug; ongoing ~1 engineer-day/week triage for the
first couple of months.

### 1.3 cargo-fuzz on parser + wire-protocol decoder

Parsers always have latent panics. Hours-to-first-crash territory.

**Plan**:
- `fuzz/fuzz_targets/parse.rs` → `parser::parse(&data)` with libFuzzer-supplied
  bytes. Panics = bugs.
- `fuzz/fuzz_targets/pq_decode.rs` → `pqcomm` message decoder.
- Stash crashes as deterministic regression cases in `src/backend/parser/tests/`
  and `src/backend/libpq/tests/`.
- Nightly CI job behind `cargo-fuzz`.

**Budget**: 1-2 days per target. Consider OSS-Fuzz submission if continuous
fuzz compute becomes valuable.

### 1.4 Hegel property-based testing

[Hegel for Rust](https://github.com/antithesishq/hegel) shipped March 2026.
Written by David MacIver and Liam DeVoe, the Hypothesis maintainers, now at
Antithesis. It's a thin Rust wrapper around the Python Hypothesis engine
(Python is a runtime dep — annoying, but the bug-finding is best-in-class).

Key shape:
```rust
#[hegel::test(test_cases = 1000)]
fn test_fraction_parse_robustness(tc: hegel::TestCase) {
    let s: String = tc.draw(generators::text());
    let _ = Fraction::from_str(&s);  // should never panic
}
```

**Bug categories it finds**:
1. "You forgot about zero."
2. "This data type is cursed and you fell afoul of the curse." (Unicode, NaN,
   edge dates, etc.)
3. "You made an error in a complicated structural invariant." (The
   model-based testing category.)

**Candidate pgrust properties**:
- **Parser roundtrip**: `parse(unparse(plan)) == plan` for a generated plan.
- **Value codec roundtrip**: `decode(encode(value)) == value` across all
  supported types. JSONB, numeric, date/time, arrays are high-payoff.
- **Planner rewrite preservation**: `eval(rewrite(plan), rows) ==
  eval(plan, rows)`.
- **SQL scalar parity**: run generated scalar expressions through both pgrust
  and a model (real Postgres or a Rust reference impl); results must match.
  This is SQLancer-adjacent but at the expression level.
- **Catalog invariants**: after any sequence of DDL, catalog state satisfies
  expected relationships (FK integrity within catalog, etc.).

**Why Hegel over proptest**: internal shrinking gives readable minimal
failures; a test database auto-replays failures on rerun; it's designed to be
the on-ramp to Antithesis later.

**Budget**: 2-3 days to first test + first bug. Incremental from there.

## Tier 2 — substantial investment, substantial payoff

### 2.1 Antithesis

Antithesis is the platform for finding **recovery and durability bugs** —
the category that's nearly impossible to catch with any of the above, because
they require fault injection at the OS/hardware boundary. Deterministic
hypervisor + guided fuzzer + time-travel debugger.

We have two application paths:

#### Path A: Founder Package (paid; realistic for pagerfree)

Announced April 2025 (see [Antithesis for founders](https://antithesis.com/blog/antithesis_for_founders/)):

- For technical founders of early-stage startups.
- "A few thousand dollars a month in most cases."
- Pre-seed / cash-strapped: they'll evaluate equity in lieu of cash via their
  venture partners at Amplify and Spark.
- Limited slots — they said "five companies" initially.
- They describe the experience as "rough" / "crawling over broken glass" but
  "liberating" (Carl Sverre at Graft, first Founder Package customer).
- Self-service docs + Discord channel; limited hand-holding.

**How to apply (verified Apr 23, 2026)**:
- The Founder Package post says "apply here", but the rendered page doesn't
  expose the target URL in plain text.
- The current official Antithesis contact surfaces I could verify are:
  - [Contact us](https://antithesis.com/contact/)
  - `support@antithesis.com` (listed in the docs)
  - Antithesis Discord
- Best move: send a short founder-package inquiry to `support@antithesis.com`
  and include: what pgrust is, why it matches their "highly concurrent,
  stateful" profile, rough company stage, and whether we're asking about cash,
  equity, or both.

**Fit for pagerfree**: we are a technical-founder-led early-stage startup
building a highly concurrent stateful distributed system. This is literally
their described target profile.

**Practical prep we can do before any reply**:
- keep the architecture and invariant brief current
- make the local `antithesis/` layout real enough that we know where the
  server container, workload, and SDK hooks will live
- define one small multi-session workload around common application behavior:
  create table, insert rows, update, delete, reconnect, verify final state
- identify the first assertions we would want both locally and on-platform:
  acknowledged commit survives restart, row counts stay sane, and protocol
  errors fail cleanly

#### Path B: OSS Giveaway Program (free; lottery)

Announced Feb 2024 (see [osgp2024 post](https://antithesis.com/blog/osgp2024/)):

- One application form (Google Form linked from the post).
- Antithesis picks top 4; community votes on Twitter/X.
- One winner per cycle.
- I found the 2024 launch post and a Sept. 2024 post saying applications were
  open, but I did **not** find a separate public 2026 cycle announcement as of
  Apr. 23, 2026. 2025 OSS Pledge posts are a separate grants program, not
  Antithesis-platform access.
- Criteria: "highly concurrent, stateful, client-server or distributed
  programs"; excludes parsers/renderers/OSSFuzz-shaped; requires
  volunteer/charitable effort (not institutionally funded).
- "Free work" on the winner; compute amount, duration, and eng support not
  specified.

**Fit for pgrust**: the criteria match. Downside: it's a Twitter poll, pgrust
is pre-launch and has ~no public following yet, so the poll mechanic favors
projects with existing mindshare.

#### Our recommendation

- **Primary**: apply to the Founder Package via email. This is paid but
  realistic and avoids lottery risk.
- **Secondary**: ping them asking whether there's a 2026 OSS Giveaway cycle
  and whether pgrust would be eligible as a parallel track. If so, apply.
- Either way, start scaffolding what we'd need on the platform (see "Antithesis
  prep work" below) — most of it is valuable even if we never get on-platform.

**OSS Pledge is a separate thing** — cash grants to maintainers ($110K in
2025 to Nix/FreeBSD/Kurtosis), not what we want.

#### Antithesis prep work (do anyway, regardless of program)

Most of this is valuable standalone:

- Add [antithesis-sdk-rust](https://github.com/antithesishq/antithesis-sdk-rust)
  as an optional dep. Sprinkle invariants using:
  - `assert_always!` — page CRC valid; MVCC visibility monotonic; WAL LSN
    ordered; lock-table invariants.
  - `assert_sometimes!` — we exercised concurrent commits, aborts, retries,
    conflicts (fails if the workload never exercises the state).
  - `lifecycle::setup_complete` — after server bind and first heartbeat.
  - `lifecycle::send_event` — structured txn begin/commit/abort events.
  - `random::get_random()` — route any runtime entropy through this so runs
    are deterministic under the hypervisor.
  - `ANTITHESIS_SDK_LOCAL_OUTPUT` env var → JSONL file when not on platform.
    Lets us exercise asserts in plain `cargo test`.
- Build the standard project layout (crib from
  [aardvark-arena](https://github.com/antithesishq/aardvark-arena)):
  ```
  antithesis/
  ├── Dockerfile
  ├── setup-complete.sh
  ├── config/docker-compose.yaml   # SUT + workload + fault-control
  └── test/main/                   # workloads
  ```
- Design the workload using the [valthree](https://github.com/antithesishq/valthree)
  "one generator, three drivers" pattern: same random SQL generator runs in
  (a) `cargo test` quickly, (b) nightly in our CI, (c) on Antithesis
  full-bore. No generator rewrite across environments.
- Install the [antithesis-skills](https://github.com/antithesishq/antithesis-skills)
  Claude Code / Codex skills before scaffolding. The
  `antithesis-research` skill can draft a property catalog directly from our
  source.

### 2.2 cargo-mutants on executor + storage

Deliberately mutates the code (flips comparisons, replaces returns); if tests
still pass, tells us our tests are lying about coverage. Target the modules
where correctness matters most:

- `src/backend/executor/`
- `src/backend/access/`
- `src/backend/storage/`

**Budget**: 2-3 days to run, interpret results, patch or suppress mutants.
One-time spike, maintain in nightly CI.

## Tier 3 — worthwhile but more specialized

### 3.1 proptest on narrow algorithmic corners

For focused modules where Hegel's Python dependency is overkill, plain
proptest is enough. JSONB, numeric, date/time, planner rewrite rules. Cheap
roundtrip properties.

### 3.2 Miri on the test suite (nightly)

UB detector via MIR interpretation. Slow (10-100x) so gate behind nightly.
Any `unsafe` in the storage/buffer layer should be Miri-clean.

### 3.3 loom / shuttle on buffer pool + lock manager

- **loom**: exhaustive interleaving. Small critical sections only (2-3
  threads).
- **shuttle**: random interleaving. AWS S3 Express scale. Use for anything
  larger than loom handles.

## Tier 4 — consider but not yet

### 4.1 Detail.dev

LLM-agent code review, runs for hours per scan. Good for shallow-but-broad
source-level defect finding. Free first scan. Rust support not confirmed —
contact them.

Case studies: Tailscale (5/7 fixed), kubernetes-client/javascript (3/3).
Backed by Rauch, Pomel, Cacioppo, Ferdowsi, Ittycheria, Podjarny, etc. Not a
substitute for any Tier 1/2 item.

### 4.2 Jepsen / Porcupine

Linearizability checker. Premature for single-node pgrust as a full harness.
Revisit if/when we add replication.

That said, the useful Jepsen lesson starts earlier than Jepsen itself: design
tests around explicit invariants, then force the system through awkward
interleavings and failures until one breaks. The goal is not "the server stayed
up"; it is "the system never silently claimed safety it did not actually have."

The Aphyr Kafka post is the right mindset reference here:

- black-box checks matter more than implementation confidence
- acknowledged success is the dangerous line; once we claim success, we need a
  way to prove the result survived
- when safety and availability trade off, tests should catch the exact point
  where the system starts silently violating the safety claim
- if the system cannot preserve an invariant, it should fail loudly instead of
  pretending all is well

For single-node pgrust, that means "Jepsen now" is the wrong tool, but
"Jepsen-style skepticism" is the right habit. We should apply it to:

- crash/recovery invariants: after acknowledged commit and restart, the row is
  still there
- multi-client history checks: simple read/write histories match a serial or
  snapshot-consistent explanation for the guarantees we claim
- durability boundaries: if we report success before an fsync-equivalent
  boundary, tests should make that risk obvious and documented
- protocol/session invariants: malformed or interrupted client behavior should
  fail with an error, not panic or corrupt session state

Practical rule: steal Jepsen's adversarial mindset now, then bring in Jepsen or
Porcupine proper only once pgrust has multi-node replication or explicit
linearizability claims.

### 4.3 In-process Rust DST (madsim / hand-rolled)

Would require plumbing storage, time, and rand through traits everywhere.
High architectural cost. If Antithesis accepts us, they do this at the
hypervisor level for free. Skip unless an Antithesis path falls through and
we still want fault-injection determinism.

## Recommended execution order

1. **Right now**: create this doc, land it as-is so Malis sees the direction.
2. **Week 1**: cargo-fuzz targets (1.3). Low risk, fast first-bug.
3. **Week 1-2 parallel**: apply to Antithesis Founder Package. Email them,
   ask about 2026 OSS Giveaway cycle in the same thread.
4. **Week 2-3**: SQLancer differential harness (1.2). Use antithesishq's
   fork as starting point.
5. **Week 2-3 parallel**: first Hegel tests (1.4) on parser roundtrip + JSONB
   codec roundtrip.
6. **Week 3-4**: cargo-mutants spike (2.2).
7. **If Antithesis accepts**: switch focus to SDK integration + workload
   container (2.1). If not, continue iterating on 1.2 / 1.4.

## Immediate outreach

1. Email `support@antithesis.com` with subject: `Founder Package inquiry for pgrust / Pagerfree`.
2. Ask two concrete questions in that same email:
   - Is the Founder Package still available as of April 2026?
   - Is there also a current OSS Giveaway or similar startup / OSS track we should apply for?
3. Include one paragraph on technical fit:
   - Postgres-compatible database prototype in Rust
   - Highly concurrent, stateful system
   - Strong interest in durability / recovery testing, not just unit tests
4. If they reply positively, next internal milestone is not "buy Antithesis";
   it's "prove local containerized workload + property catalog + SDK hooks are feasible."

## Founder Package fit checklist

Use this as the quick internal sanity check before sending outreach.

- We are a technical-founder-led early-stage startup.
- pgrust is a stateful system, not a stateless library.
- The interesting bugs are in concurrency, recovery, crash behavior,
  durability, protocol handling, and long sequences of operations.
- We can realistically containerize the server and a workload driver.
- We have at least one engineer who can tolerate rough edges and do some
  integration work without heavy vendor hand-holding.
- Paying a few thousand dollars per month is potentially plausible, or we at
  least want to ask about an equity-based path.

If the answer to most of these is "yes", outreach is justified now.

## Draft outreach email

Subject: Founder Package inquiry for pgrust / Pagerfree

Hello Antithesis team,

I'm a technical founder at Pagerfree. We're building `pgrust`, a PostgreSQL-
compatible database prototype in Rust.

This looks like a strong fit for the kind of testing Antithesis is good at:
`pgrust` is a highly concurrent, stateful system where the hardest bugs are
not unit-test bugs, but recovery, crash, durability, protocol, and long-
sequence correctness bugs.

Your Founder Package announcement seems aimed directly at teams like us. We're
an early-stage startup, and we'd like to understand whether that program is
still available as of April 2026. If it is, we'd also like to know whether the
best fit would be:

- the standard Founder Package,
- an equity-based arrangement for a cash-constrained early-stage company, or
- some current OSS / startup program if that is a better path.

Separately, is there an active OSS Giveaway or similar program running now
that pgrust should apply for in parallel?

We've already started mapping the prep work on our side: property catalog,
containerized workload, and candidate SDK invariants. If useful, I can send a
short summary of the system architecture and the kinds of failures we most
want to test.

Thanks,
[name]
Pagerfree
[email]
[repo / website]

## If they respond positively

The next step should be a scoped technical conversation, not a vague "sounds
interesting" loop. The information we want back quickly is:

- Whether Founder Package is still open.
- Expected pricing band and whether equity is realistic.
- What "minimum viable onboarding" looks like for a Rust database project.
- Whether they want Docker Compose first or are fine with a lighter-weight
  pre-POC architecture discussion.
- Whether they recommend starting with SDK assertions, workload generation, or
  a simple end-to-end container harness.

Our own first deliverable back to them should be:

- One-page system summary.
- Initial property list: crash safety, WAL/order invariants, visibility,
  protocol robustness, and result parity.
- A statement of what is already testable locally versus what requires system-
  level fault injection.

## References

- [Hegel: Hypothesis, Antithesis, synthesis](https://antithesis.com/blog/hegel/) — MacIver's launch post (March 2026)
- [Hegel for Rust](https://github.com/antithesishq/hegel)
- [Antithesis for founders](https://antithesis.com/blog/antithesis_for_founders/) — Founder Package announcement
- [OSS Giveaway Program](https://antithesis.com/blog/osgp2024/)
- [antithesis-sdk-rust](https://github.com/antithesishq/antithesis-sdk-rust)
- [antithesis-skills](https://github.com/antithesishq/antithesis-skills)
- [aardvark-arena](https://github.com/antithesishq/aardvark-arena)
- [valthree](https://github.com/antithesishq/valthree)
- [bombadil](https://github.com/antithesishq/bombadil)
- [snouty](https://github.com/antithesishq/snouty)
- [SQLancer (Antithesis fork)](https://github.com/antithesishq/sqlancer)
- [SQLancer (upstream)](https://github.com/sqlancer/sqlancer) + [bug list](https://sqlancer.github.io/bugs/)
- [SQLsmith](https://github.com/anse1/sqlsmith)
- [Cockroach Labs: Antithesis of a One-in-a-Million Bug](https://www.cockroachlabs.com/blog/demonic-nondeterminism/)
- [Carl Sverre at Graft: Antithesis experience](https://antithesis.com/blog/sdk_graft/) (first Founder Package customer)
- [FoundationDB: Simulation and Testing](https://apple.github.io/foundationdb/testing.html)
- [TigerBeetle VOPR](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/internals/vopr.md)
- [Detail.dev](https://detail.dev)
- [PR #58 — isolation tester harness](https://github.com/your-github-org/pgrust/pull/58)
