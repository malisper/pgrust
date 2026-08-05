# FINDING: jsonpath parse-cost blowup — NOT a pgrust defect (upstream regex-compiler property)

Lane: `proofs/p1-laneaa-perf` (worktree `.wt-p1-laneaa-perf`), 2026-07-31.
Trigger: libFuzzer `slow-unit` reports from fleet campaign
`pgrust-fuzz-campaign-1785518461-61c1-18958` (`jsonpath_diff`, 10.0M execs,
**zero** value/verdict/sqlstate divergences) — two ~316-byte arm-0 inputs at
**52,499 ms** and **54,814 ms** per single exec.

**VERDICT: NOT A PGRUST DEFECT.** The primary hypothesis (super-linear /
exponential backtracking in the hand-written recursive-descent parser
`gram.rs`) is **REFUTED**. The cost is `pg_regcomp` — specifically PostgreSQL's
own `fixconstraintloops` / `clonesuccessorstates` constraint-loop cloning in
`regc_nfa.c` — invoked *from inside the parse* by `makeItemLikeRegex` /
`make_item_like_regex`, exactly as C does. **Real PostgreSQL 18.3 pays the same
super-polynomial cost on the same bytes, to within 2%** (27.7 s vs 28.1 s at
N=256; peak measured 51.7 s on a 10 KB literal). pgrust tracks C at a
**~2.0–2.2× constant factor** on this shape (and is ~200× *faster* than C on a
different bounded-repeat shape), i.e. a constant-factor regex-compiler
difference, not a complexity-class difference. **No crate change was made.**

Also refuted: the charter's inference that "PG returning a *syntax error* in
307 ms suggests C never reached regex compilation". It did. PG compiles the
pattern during the `like_regex` reduction and *then* hits the trailing garbage;
truncating the input to just the (valid) `like_regex` clause still costs PG
283.5 ms, while halving the *pattern* drops it to 0.3 ms.

---

## 1. Why the fleet saw 52 s and the server sees 0.5 s

The 52 s (fleet) / 16.8 s (local, re-verified) figures are **sanitizer-coverage
instrumented** measurements. `fuzz/core/build.rs` compiles the vendored C
oracle with `-fsanitize-coverage=inline-8bit-counters,pc-table` (lines 16, 219)
and cargo-fuzz instruments the Rust side the same way, so *both* engines run
~24× slow in the fuzz binary. Uninstrumented, on the **exact same 315 bytes**:

| engine | jsonpath_in on the fleet slow-unit text | verdict |
|---|---|---|
| in-harness C oracle (vendored 18.3), release, no sancov | **205.6 ms** | `42601` syntax error |
| shipped Rust `adt_jsonpath::path::jsonpath_in`, release | **499.8 ms** | `42601` syntax error |
| real PostgreSQL 18.3 (docker `laneaa-pg183`) | **266–285 ms** | `ERROR: syntax error at end of jsonpath input` |
| the libFuzzer target, sancov+libfuzzer instrumented (re-verified) | **16,764 ms** (`Executed … in 16764 ms`) | — |

So the reported ~60× "pgrust vs PG" gap does not exist: the real ratio on this
input is **1.9× vs real PG**, **2.4× vs the in-harness C oracle**. The 60×
compared an instrumented Rust+C build against an uninstrumented server.
All three engines agree on the verdict (`42601`), as the campaign's clean
verdict plane already implied.

## 2. Attribution

**Ablation.** Halving the *pattern* (keeping everything else) collapses the
cost on both sides: `full` 203.5 ms C / 465.5 ms Rust → `first-half` **32 µs /
36 µs**, `second-half` 13 µs / 19 µs. Removing the backslash runs from the
repeated unit (`^^^^|Y||pawt@r` instead of `^^^^|\\\\\?\^^^\\Y||pawt@r`) makes
the family linear and microsecond-scale at every N. The cost lives entirely in
the pattern, i.e. in `pg_regcomp`, not in the token stream or the grammar.

**Profile** (macOS `sample`, 10 ms interval, 4 s over the Rust side compiling
the N=128 pattern; 359 samples, 279 in the parse): a single stack, 100 % of the
in-parse samples:

```
rust_in -> adt_jsonpath::path::json_path_from_cstring
        -> adt_jsonpath::gram::parsejsonpath -> ... -> parse_comparison
        -> adt_jsonpath::gram::make_item_like_regex
        -> regex_core::regex_compile::pg_regcomp
        -> nfatree -> nfanode -> regex_nfa::optimize
        -> regex_nfa::fixconstraintloops      (277/279 samples)
        -> regex_nfa::findconstraintloop  (deep self-recursion)
        -> regex_nfa::clonesuccessorstates (deep self-recursion, ~everything)
```

`fixconstraintloops` / `findconstraintloop` / `clonesuccessorstates` are the
verbatim port of PostgreSQL `src/backend/regex/regc_nfa.c`. The blowup is the
known upstream state-cloning explosion for **constraints (`^`) inside loops** —
the pattern is `(… ^^^^ | … \\ … ^^^ …)+`, i.e. anchors under a `+`.
Zero samples anywhere in the parser's own token/production code.

**Grammar ablation (the load-bearing refutation).** `timing_grammar_only`
measures seven grammar-stress shapes with *no* `like_regex` at all, N=32…2048:
nested parens, filter chains, unary `-+` chains, ambiguous method keywords,
index lists, `||` chains, and a shape that only fails on the **last** token
(worst case for any retry-on-failure). Every shape is **linear** in N and
microsecond-scale (e.g. `nested-paren` N=2048 / 4099 bytes: C 39 µs, Rust
114 µs; `late-failure` N=1024: C 17 µs, Rust 72 µs). Reading `gram.rs`
confirms why: every `Ok(None)` is a **terminal** failure that propagates
straight out to `parsejsonpath` — no production is ever re-attempted at the
same input position, and all lookahead is one token (`peek_tok`/`at_char`).
The parser is strict LL(1), linear time. There is nothing to memoize.
(The `Ok(None)` returns are *not* backtracking; the charter's "backtracking
with `Ok(None)`" reading of the code is incorrect.)

## 3. Scaling law — three engines, identical family

Family (minimized out of the fleet slow-unit, one repeated unit):

```
$ ? (@ like_regex "(UNIT{N})+")     UNIT = ^^^^|\\\\\?\^^^\\Y||pawt@r
```

`fuzz/core/src/jsonpath_diff.rs::timing_scaling_family` (`JP_NS` overridable)
measures the C oracle and shipped Rust; `fuzz/jsonpath_parse_scaling.sh`
measures real PostgreSQL 18.3 in docker on identical bytes.

| N | literal bytes | real PG 18.3 | C oracle (18.3, uninstr.) | shipped Rust | Rust/C | verdict (all three) |
|---:|---:|---:|---:|---:|---:|---|
| 1 | 50 | 0.41 ms | 0.09 ms | 0.14 ms | 1.5 | ok |
| 2 | 76 | 1.73 ms | 1.15 ms | 1.73 ms | 1.5 | ok |
| 4 | 128 | 122 ms | 110 ms | 150 ms | 1.4 | 2201B too complex |
| 8 | 232 | 80 ms | 66 ms | 121 ms | 1.8 | 2201B |
| 16 | 440 | 117 ms | 106 ms | 229 ms | 2.2 | 2201B |
| 32 | 856 | 207 ms | 176 ms | 393 ms | 2.2 | 2201B |
| 64 | 1,688 | 456–531 ms | 0.410 s | 0.832 s | 2.03 | 2201B |
| 128 | 3,352 | 2,212 ms | 1.831 s | 4.135 s | 2.26 | 2201B |
| 192 | 5,016 | 9,052 ms | 8.412 s | 18.043 s | 2.14 | 2201B |
| 256 | 6,680 | **27,687 ms** | **28.110 s** | **54.818 s** | 1.95 | 2201B |
| 384 | 10,008 | **51,724 ms** | **47.878 s** | **105.194 s** | 2.20 | 2201B |
| 512 | 13,336 | 225 ms | 0.190 s | 0.151 s | 0.80 | 2201B |
| 1024 | 26,648 | 481 ms | 0.387 s | 0.337 s | 0.87 | 2201B |

Growth 64→128→192→256→384 is **super-polynomial** (doubling N multiplies cost
by ~4.7 then ~14; 1.5×N at the top multiplies by ~1.8–1.9), consistent with the
exponential state cloning in `clonesuccessorstates`. It is **non-monotonic**:
at N≥512 a cheaper pre-cloning size/complexity check in `pg_regcomp` fires
first and the input is rejected in ~0.2–0.5 s. So the cost is **not** unbounded
in literal length — it peaks in a band (here ~5–10 KB, ~9–52 s) and falls off.
The worst case is a *shape* property, not a length property; the practical fence
is "tens of seconds of CPU per statement", not "1 GB literal ⇒ hours".

Control families in the same test (`unit-no-backslash`, `a|b|ab`, `^`) are
**linear** through N=1024 at 1.1–1.6× C — confirming the blowup needs the
specific constraint-in-loop shape, not merely alternation or anchors.

## 4. Is there an availability finding at all?

Yes, but it is **upstream PostgreSQL 18.3, inherited, not introduced**: a
single ~10 KB `::jsonpath` literal any unprivileged client can send costs
**51.7 s of real-PG CPU** (105 s in pgrust) inside input-function parsing, with
no statement-timeout-independent bound other than PG's own eventual
`regular expression is too complex` rejection, which fires only *after* the
cloning work. This is the well-known `regc_nfa.c` constraint-loop cloning cost,
and it is equally reachable via `~`, `regexp_like()`, `SIMILAR TO`, etc. — the
jsonpath parser is only a delivery vehicle.

Recommended handling: **report as an inherited upstream property, not a pgrust
release blocker.** pgrust's own exposure is the 2.0–2.2× constant factor on
this one code path. Note also that pgrust is *faster* than C on other
compile-heavy shapes — measured `((a|b|ab){0,100}){0,100}`: C **1.661 s**,
Rust **0.008 s** (0.005×) — so the regex-compiler delta is shape-dependent and
not a uniform loss. Closing the 2× on constraint-loop cloning is an
`optimize`-lane question about `regex_core`, out of this lane's DoS charter.

## 5. Correctness / divergence status

* **No new divergence found.** Every input measured here (fleet slow-unit,
  bisections, all four scaling families × 7 N values, all 7 grammar shapes ×
  7 N values) produced **identical verdicts and sqlstates** on the C oracle and
  shipped Rust, and identical error classes on real PG 18.3
  (`42601` syntax error on the fleet unit; `2201B invalid regular expression:
  regular expression is too complex` across the family).
* One expected in-harness-only verdict: the grammar shapes at N=2048 report
  Rust `54001` (statement too complex). That is the documented stack-depth
  carve — the harness arms the Rust guard at 1536 kB on a 2 MiB libtest thread
  while the C shim's `check_stack_depth` is a no-op. Out of the fuzz domain
  (`MAX_TEXT = 512`) and already ratified.
* Crate/corpus gates re-run green after the (test-only) changes:
  `cargo test --manifest-path fuzz/Cargo.toml -p decoder_fuzz jsonpath` →
  6 passed / 4 ignored, including `seed_corpus_replays_clean` over the full
  11,214-unit committed corpus.

## 6. What a reviewer should re-run

```bash
# 1. attribution on the exact fleet bytes (needs no docker)
cargo test --release --manifest-path fuzz/Cargo.toml -p decoder_fuzz \
    timing_slow_unit_attribution -- --ignored --nocapture

# 2. scaling law, C oracle + shipped Rust (~5 min at the default N list)
JP_NS="64 128 192 256 384 512 1024" cargo test --release \
    --manifest-path fuzz/Cargo.toml -p decoder_fuzz \
    timing_scaling_family -- --ignored --nocapture

# 3. the grammar-is-linear refutation (seconds)
cargo test --release --manifest-path fuzz/Cargo.toml -p decoder_fuzz \
    timing_grammar_only -- --ignored --nocapture

# 4. same family against real PostgreSQL 18.3 (~2.5 min; longer with N=384)
docker run -d --rm --name laneaa-pg183 -e POSTGRES_HOST_AUTH_METHOD=trust postgres:18.3
NS="64 128 192 256 384 512 1024" fuzz/jsonpath_parse_scaling.sh

# 5. arbitrary inputs, both sides
JP_TIME_FILES=/path/a:/path/b cargo test --release \
    --manifest-path fuzz/Cargo.toml -p decoder_fuzz timing_files -- --ignored --nocapture

# 6. corpus + crate gates
cargo test --manifest-path fuzz/Cargo.toml -p decoder_fuzz jsonpath
```

Artifacts committed in this worktree:

* `fuzz/testdata/jsonpath-slow/slow-unit-899856ad-text.bin` — the exact 315
  source-text bytes of fleet slow-unit
  `899856ad3a5f72f09a52598b9bc434076004cd93` (the artifact's first two bytes,
  selector `0x03` and mode `0x01`, stripped).
* `fuzz/corpus/jsonpath_diff/jsonpath-regex-constraintloop-{hard,soft}` — the
  minimized constraint-in-loop shape at N=2 (76-byte text, ~2 ms/exec), so the
  `fixconstraintloops` / `clonesuccessorstates` path stays covered by the
  corpus replay without re-introducing a slow unit. The 315-byte fleet text is
  deliberately **not** added to the corpus (0.5 s/exec release buys no
  coverage over the N=2 seed).

## 7. Fleet re-run needed?

**No.** Nothing in the crate changed; the only edits are `#[ignore]`d timing
tests, a docker script, one testdata file and two cheap corpus seeds. The
campaign's own verdict is unchanged and already green (10.0M execs, zero
divergences). Optional, cheap, coordinator's call: re-run `jsonpath_diff` once
so the two new seeds enter the campaign corpus.

## 8. Open questions / what I could not measure

* **Constant factor.** I did not decompose the 2.0–2.2× Rust-over-C gap inside
  `clonesuccessorstates` (allocation strategy vs codegen). It needs an
  `optimize`/`asm-diff` pass on `regex_core::regex_nfa`, not a parser lane.
* **Peak location.** The cost band (~5–10 KB for this unit) and the identity of
  the cheaper check that rejects N≥512 were not pinned to a specific
  `pg_regcomp` limit; I only bracketed the band empirically on all three
  engines.
* **Second artifact.** I attributed the 315-byte text of slow-unit
  `899856ad…`; `f6de6b93…` (315 bytes, same campaign, 54.8 s) was not measured
  separately — the coordinator reports it as the same `like_regex`-heavy shape,
  and the minimization above already isolates that shape's mechanism.
* **Timings are laptop wall-clock** (darwin arm64, `--release`, no fleet), each
  point a single measurement, so ±10 % point-to-point; the growth spans
  ~5 orders of magnitude, far above that noise. No fleet submits were made
  (coordinator owns those).
