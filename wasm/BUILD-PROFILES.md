# wasm-release: what the build profile is worth (2026-09-07)

`[profile.wasm-release]` ships the browser module: `inherits = "release"`, `opt-level = "s"`,
`lto = false`, `codegen-units = 16` — chosen for SIZE, never measured for SPEED. The bench
(`pglite-v-pgrust`) has pgrust's wasm 1.3–9× slower than PGlite on bulk writes, so this asks the
narrow question: how much of that is the profile?

- Toolchain: `nightly-2026-07-17`, `-Zbuild-std=std,panic_unwind`, `-C panic=unwind
  -C target-feature=+exception-handling`, target `wasm32-wasip1-threads`
- Build: `LC_ALL=C PGRUST_WASM_TARGET=wasm32-wasip1-threads PGRUST_WASM_PROFILE=wasm-release
  wasm/wasm-build.sh` (846 crates + the `postgres` link), one at a time on an idle machine
- Machine: Linux 7.0.0-30-generic x86_64, i7-1165G7, 8 threads
- Measurement: `pglite-v-pgrust` `bun run bench --suite speedtest|rtt --no-build --configurations
  pglite-memory,pgrust-threads-memory-broker --baseline pglite-memory`, each variant's
  `postgres.wasm` copied over the bench's asset and the original restored afterwards. The
  `pgrust-threads-memory` column could not be used: the bench's vendored host predates the guest's
  `path_symlink` import and refuses to instantiate any module built after `634375ebe6`; its BROKER
  column gets that import from the repacked store adapter, so that is the column here.

## Build and size

| Variant | opt-level | lto | codegen-units | wall build | .wasm raw | gzip -9 | vs baseline gz |
| --- | --- | --- | --- | --- | --- | --- | --- |
| baseline (shipped) | "s" | false | 16 | 276 s | 46 431 092 | 13 306 733 | — |
| A | 3 | false | 16 | 438 s | 50 323 152 | 14 738 844 | +10.8% |
| B | 3 | "thin" | 1 | 316 s | 46 333 130 | 14 405 242 | +8.3% |
| C | 3 | "fat" | 1 | 812 s | 41 659 871 | 13 411 137 | +0.8% |

No variant failed to build. The baseline row's wall time is a `rm -rf
target/wasm32-wasip1-threads/wasm-release` rebuild, the same work the profile switches forced on
A/B/C; the variants' times are as cargo ran them in sequence (each profile edit invalidates every
unit in that directory). The shipped profile is also the QUICKEST to build, by a wide margin over
fat LTO.

Fat LTO makes the SMALLEST module of the four — 10% under the size-tuned baseline, raw and 0.8%
over it gzipped — which is the one result here that was not expected.

## Speedtest, absolute pgrust milliseconds (lower is better)

The pgrust column is what the profile moves. PGlite's column is the control and is quoted for the
one run where it drifted (C's first run landed on a noisy patch — Test 1 105 ms against 52–62 ms
elsewhere — so C was re-run; both are shown).

| Benchmark | baseline | A | B | C (run 1 / run 2) |
| --- | --- | --- | --- | --- |
| 1: 1000 INSERTs | 407.1 | 316.4 | 296.9 | 304.2 / 307.1 |
| 2: 25000 INSERTs in a transaction | 5299.3 | 4600.9 | 4273.3 | 4528.6 / 4236.3 |
| 2.1: 25000 INSERTs in single statement | 254.6 | 225.1 | 197.7 | 227.2 / 212.1 |
| 3: 25000 INSERTs into an indexed table | 5331.6 | 4656.1 | 4467.7 | 4560.3 / 4359.6 |
| 3.1: 25000 INSERTs indexed, single statement | 276.3 | 236.1 | 219.8 | 249.2 |
| 4: 100 SELECTs without an index | 392.5 | 364.9 | 395.6 | 416.6 |
| 5: 100 SELECTs on a string comparison | 709.9 | 704.1 | 712.8 | 820.2 |
| 6: Creating an index | 43.7 | 36.1 | 39.6 | 32.7 |
| 7: 5000 SELECTs with an index | 1144.0 | 938.4 | 898.3 | 904.7 |
| 8: 1000 UPDATEs without an index | 260.2 | 203.9 | 193.4 | 193.2 |
| 9: 25000 UPDATEs with an index | 5253.0 | 4376.6 | 4198.7 | 4491.5 / 4511.6 |
| 10: 25000 text UPDATEs with an index | 7456.0 | 6030.6 | 5767.7 | 5998.8 / 6746.8 |
| 11: INSERTs from a SELECT | 525.2 | 422.9 | 403.5 | 445.1 |
| 12: DELETE without an index | 42.9 | 31.3 | 27.9 | 28.0 |
| 13: DELETE with an index | 87.9 | 58.9 | 50.8 | 50.0 |
| 14: A big INSERT after a big DELETE | 463.6 | 322.0 | 266.2 | 270.3 |
| 15: A big DELETE then many small INSERTs | 1467.5 | 1391.2 | 1119.9 | 1085.0 / 1427.3 |
| 16: DROP TABLE | 15.8 | 14.5 | 12.8 | 18.3 |

Speedup over the baseline (baseline ÷ variant) on the rows that dominate the bulk-write gap:

| Benchmark | A | B | C |
| --- | --- | --- | --- |
| 2: 25000 INSERTs in a transaction | 1.15× | 1.24× | 1.17–1.25× |
| 3: 25000 INSERTs into an indexed table | 1.15× | 1.19× | 1.17–1.22× |
| 9: 25000 UPDATEs with an index | 1.20× | 1.25× | 1.17× |
| 10: 25000 text UPDATEs with an index | 1.24× | 1.29× | 1.10–1.24× |
| 14: A big INSERT after a big DELETE | 1.44× | 1.74× | 1.71× |
| 15: A big DELETE then many small INSERTs | 1.05× | 1.31× | 1.03–1.35× |

## RTT (`--iterations 3`), pgrust median over the 12 tests

| Variant | pgrust median (ms) | PGlite median in the same run (ms) |
| --- | --- | --- |
| baseline | 1.164 | 0.505 |
| A | 0.915 | 0.490 |
| B | 0.957 | 0.417 |
| C | 0.913 | 0.670 |

Three iterations is what the brief asked for and it is not enough to separate these: single rows
move by 2× between runs of the SAME binary (Test 9 insert 10kb: 2.658 ms in A against 1.395 ms in
the baseline and 1.085 ms in C). Read the medians as "all three variants are somewhat quicker than
the baseline", nothing finer.

## Conclusion — nothing committed

`opt-level = 3` is worth a real but modest 15–25% on the big write rows however it is combined, and
`lto = "thin", codegen-units = 1` (B) is the best of the three on almost every row while building
in 316 s. The bar this experiment set for changing the shipped profile was **≥1.5× on the
write-heavy rows and no more than +25% gzipped**, and B does not clear it: the four 25 000-row
rows that carry the bulk-write gap are 1.19–1.29×, and only the two smaller composite rows (14, 15)
reach 1.5–1.75×. So the profile is left exactly as it was.

The result that deserves a follow-up is C's SIZE. `lto = "fat"` produced the smallest module of the
four — smaller than the size-tuned baseline by 4.8 MB raw — while also being 10–25% faster on
writes. It costs 812 s to build (2.9× the baseline). A profile that is both smaller and faster is
worth revisiting when the build time is affordable, e.g. for the released artifact rather than the
iteration one.

What this experiment did NOT find: any evidence that the profile explains the 1.3–9× gap to PGlite.
Even the best variant leaves Test 2 at 4273 ms against PGlite's 579 ms. The gap is in the guest or
the transport, not in codegen flags.
