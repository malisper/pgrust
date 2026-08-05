# FINDING: flaky SIGSEGV in tsdiff_impl_DecodeInterval during whole-lib parallel `cargo test --release` (pre-existing at main)

Found by fix/mutants-rail 2026-08-02 while validating the mutants-rail
fixes. Distinct from the five red rail-baseline tests (all triaged in
FINDING-mutants-rail-vacuous.md); this one is a process-crash class.

## Signature

`decoder_fuzz` test process dies SIGSEGV (KERN_INVALID_ADDRESS) inside the
vendored C oracle:

```
_platform_strncmp
tsdiff_impl_DecodeInterval
tsdiff_impl_interval_in
pg_tsdiff_interval_in
decoder_fuzz::timestamp_diff::timestamp_diff
decoder_fuzz::timestamp_diff::tests::replay_committed_corpus
```

Faulting addresses look like corrupted/truncated static-region pointers
(`0x10000da2c` / `0x100009adc` — below the slid __TEXT base, "not in any
region"), i.e. a datetkn-cache-style pointer read through corrupted BSS,
not a stack guard hit.

## Observations (macOS aarch64, release, binary 64d2a79e75eab256)

| context | sha | result |
|---|---|---|
| whole lib, default threads, run 1-2,4 | main 70ead1ef2ebc | completes (386 passed / 3 failed — the known red set) |
| whole lib, default threads, run 3 | main 70ead1ef2ebc | **SIGSEGV** (crash report 2026-08-02-055254) |
| whole lib, default threads, run 1 | fix/mutants-rail d9eaeaea21d^ | **SIGSEGV** (crash report 2026-08-02-060214) |
| whole lib, default threads, run 2 | fix/mutants-rail | completes 389/0 |
| timestamp_diff suite alone, default threads | both | clean |
| whole lib --test-threads=1 (debug + release) | both | clean (multiple runs) |
| fleet libFuzzer corpus replay, 25,171 units, linux | main | clean, rc=0 (pgrust-fuzz-campaign-1785674036-1af7-40058) |

Crash reports: `~/Library/Logs/DiagnosticReports/decoder_fuzz-64d2a79e75eab256-2026-08-02-{055254,060214}.ips`
(copies worth banking if this file outlives the laptop's log rotation).

## Why this is NOT the oracle-entry-mutex race

At the fix/mutants-rail crash, the .ips thread list shows the entry guards
doing their job: EVERY other suite thread is parked in
`pthread_mutex_wait` on the oracle mutex; the replay thread is the only
one executing C. The poison is therefore PLANTED EARLIER in the process
lifetime — a wild write out of some earlier-run suite (vendored C oracle
or unsafe Rust harness code) corrupting the timestamp TU's static caches
(`datecache`/`deltacache`/`tzabbrevcache` are plain process-global statics
in csrc/pg_timestamp_io.c) — and only dereferenced later, by whichever
pointer-dense hot path lands on the corrupted page. Different binary
layout = different victim, which is why it presents as "timestamp".

## Repro

```
cd fuzz && cargo test --release -p decoder_fuzz --lib
# whole lib, default parallel threads; ~40% hit rate over 6 observed runs
# (2 crashes / 6). Single-suite or --test-threads=1 never reproduces.
```

## Routing

This is exactly the class the ASan side-channel exists for (asan-treewide,
task #84): run the whole-lib test suite under ASan on Linux and let the
first wild write name its owner. Until then:

- It cannot void other lanes' mutants audits anymore: scoped rails
  (MUTANTS_RAIL_FILTER) dodge it, and an unfiltered rail whose baseline
  it kills now VOIDS loudly (exit 65, no number) instead of minting one.
- It does NOT invalidate the timestamp_in triage: the committed corpus
  replays clean on both platforms whenever the process is not corrupted.

## UPDATE 2026-08-02: the verdict-flip twin is CONFIRMED same-class (attribution probe)

Fleet job pgrust-mutants-audit-1785682208-7afc-64115 @ 9d70674ea21: the
timestamp replay's diverging unit (0ac33966e31f..., ordinal 1034, C
22008-vs-Rust-22007) fails on a SAME-THREAD retry AND on a FRESH-THREAD
retry inside the poisoned process — the sticky state is process-global
C-oracle memory, not Rust thread-locals. Deterministic given the pod's
readdir order (3 shas, same unit, same ordinal); clean under sorted order
and on macOS order. So the whole-lib process carries one corruption class
with two presentations: usually a silent/benign scribble, sometimes a
verdict flip (this), sometimes a wild-pointer SIGSEGV (above), depending
on where the write lands in the victim TU's static layout.
