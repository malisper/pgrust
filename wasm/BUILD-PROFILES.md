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

## The same profile on 0.3 (2026-09-16)

Rebuilt unchanged — same profile, same `nightly-2026-07-17`, same target — on the spike line rebased
onto upstream v0.3 (`79ad992ede`, PostgreSQL 18.6):

| Module | .wasm raw | gzip -9 | vs the row above |
| --- | --- | --- | --- |
| baseline on v0.2 (`46cb91c6cd`) | 46 431 092 | 13 306 733 | — |
| baseline on v0.3 (`5eb87502c3`) | **53 414 445** | 15 097 262 | +15.0% raw, +13.5% gz |

sha256 `b54a54a0c33281d147bacc9f756c8f32128c1540aae4f8e0945656748f67683f`. Seven megabytes of engine,
not of profile: nothing in `[profile.wasm-release]` moved, and the duplication result in the bench's
`2026-09-08-wasm-size-map.md` applies to this module exactly as it did to the last one.

One thing the rebase changed underneath the module: `child_thread_stack_size()` now takes the SCALED
guard budget (`max_stack_depth` x `STACK_DEPTH_SCALE` + 8 MiB = 16 MiB at `max_stack_depth=2048`),
which dominates the 4 MiB wasm `UNLIMITED_STACK_RESERVE` floor, so the floor no longer decides what a
child thread reserves — and the browser peak fell anyway, 656.9 MiB to 263.5 MiB on the bench's
one-machine A/B of the two modules.

## A Binaryen pass after the link (2026-09-16)

`lto = false` with `codegen-units = 16` leaves duplicate function bodies in the module, which is
what the bench's `2026-09-08-wasm-size-map.md` §5/§9 measured. `wasm/wasm-build.sh` now runs
`wasm-opt -Oz` over `postgres.wasm` in place, but ONLY under `PGRUST_WASM_PROFILE=wasm-release`
(dev builds are untouched) and only when `PGRUST_WASM_OPT` is not `0`. If Binaryen is missing under
the release profile the build fails loudly rather than shipping an unoptimised module quietly.

The feature list is spelled out in the script and must stay that way. The release profile strips the
module's `target_features` section, so wasm-opt cannot detect what the module uses; and
`--all-features` makes a *smaller* module that V8 then refuses to compile (`unknown import kind
0x7e`). Only `--enable-threads` differs between the two targets.

Both modules at `d87d883e` (Binaryen 132, i7-1165G7, 8 threads):

| Module | raw before | raw after | gzip -9 before | gzip -9 after | pass wall |
| --- | --- | --- | --- | --- | --- |
| `wasm32-wasip1-threads` | 53 414 445 | **39 040 753** (−26.9%) | 15 097 262 | **13 434 230** (−11.0%) | 306 s |
| `wasm32-wasip1` | 53 265 180 | **38 410 184** (−27.9%) | 15 089 981 | **13 519 294** (−10.4%) | 299 s |

sha256 after the pass: threads
`737762791ff9b34a6339ceb0ae2ad323856ebb03c4ea6f73c10fffa8ec87d0a0`, single-session
`6ef8c4459bec38d12db1ff45e9108f9f52d93168c727f71ba9548ba5ad5d7be6`.

Fourteen megabytes off each module for five minutes of wall time on top of a build that already
takes five. The pass is the duplicate-elimination `lto = "fat"` would have done in the compiler, at
a third of fat LTO's build cost, and it does not touch the Cargo profile. What it costs in speed is
the bench's measurement, not this file's: `pglite-v-pgrust` `docs/results/2026-09-16-wasm-opt-pass.md`.

## What the module links is a feature list (2026-09-16)

`[profile.wasm-release]` decides how the code is compiled; this section is about **which code there
is**. Up to here every wasm module carried the whole engine — 47 contribs, logical replication,
parallel query, base backup, four non-btree access methods — because `seams_init` is a flat list of
353 path dependencies and 311 `init_seams()` calls with no way to say "not that one".

The mechanism is Cargo's, and it is the whole of it: a `seams_init` dependency becomes
`optional = true`, a feature turns it on, and the `init_seams()` call gets a `cfg`. Feature off ⇒
the crate is **not in the link graph at all** ⇒ its seams are never installed. 90 dependencies are
gated this way behind **47 `contrib-*` features and 12 group features** (`replication`, `parallel`,
`backup`, `index-gin`, `index-gist`, `index-spgist`, `index-brin`, `tsearch`, `geo`, `jsonpath`,
`pgrcolumnar`, `plpgsql`), over 92 `cfg`-ed lines in `seams_init/src/lib.rs`. `main_main` takes
`seams_init` with `default-features = false` and forwards its own `full` / `browser` features; it is
the only crate in the graph that depends on `seams_init`, so nothing re-enables the defaults through
feature unification. `default = ["full"]` is exactly the dependency set and the init order that
existed before, so the native build and the default wasm build are unchanged.

`wasm/wasm-build.sh` takes **`PGRUST_WASM_FEATURES`** (default `full`) and always links with
`--no-default-features --features "$PGRUST_WASM_FEATURES"`, so that variable is the only thing that
decides the set.

### `browser`: the first profile that leaves something out

Every group feature is still ON — the Tier A groups (`replication`, `parallel`, `backup`) are
declared but untested off, because the postmaster touches them at BOOT and a seam called with
nothing installed panics `seam not installed`; the Tier B groups are reached only through fmgr/pg_am
dispatch and are safe by construction, but neither is switched off in this profile. What `browser`
drops is **contribs**: it keeps `pgvector`, `pgvector_hnsw`, `pg_trgm` and `pgcrypto`, and nothing
else.

### The exclusion proof

A feature that is merely *declared off* proves nothing — the crate can still be in the module
because some other crate in `main_main`'s graph depends on it. So before the link the script asks
cargo what it actually resolved, and prints:

```
wasm-build: profile browser excludes 40 crates: amcheck auto_explain btree_gin btree_gist citext
  contrib_cube contrib_earthdistance contrib_lo contrib_seg dblink file_fdw fuzzystrmatch hstore
  injection_points intarray isn ltree pageinspect passwordcheck pg_buffercache pg_freespacemap
  pg_logicalinspect pg_overexplain pg_prewarm pg_stat_statements pg_surgery pg_visibility
  pg_walinspect pgoutput pgrowlocks pgstattuple postgres_fdw sslinfo tablefunc tcn
  test_custom_types test_decoding test_oat_hooks unaccent uuid_ossp
wasm-build: profile browser leaves 3 gated crates linked (another crate in the graph depends on
  them; their seams are still not installed):
    bloom <- amapi bloom_build indexam
    tsm_system_rows <- tablesample
    tsm_system_time <- tablesample
wasm-build: exclusion proof OK (no excluded crate is reachable in the link graph)
```

43 of the 47 contribs are gated off; 40 genuinely leave, and the 3 that do not are reported with the
crate that keeps them (core's index and tablesample machinery names them directly). Counting the
transitive deps that leave with them, the package graph goes 922 → 878. Nothing here was
restructured to make the number bigger: where a gate cannot remove a crate, the feature is still
declared — it still stops `seams_init` installing the seams — and the crate stays.

### Sizes, after `wasm-opt -Oz`

| Module | features | .wasm raw | gzip -9 | vs `full` raw |
| --- | --- | --- | --- | --- |
| `wasm32-wasip1-threads` | full | 39 040 753 | 13 434 230 | — |
| `wasm32-wasip1-threads` | **browser** | **37 209 731** | **12 821 035** | −1 831 022 (−4.7%) |
| `wasm32-wasip1` | full | 38 410 184 | 13 519 294 | — |
| `wasm32-wasip1` | **browser** | **36 584 849** | **12 908 762** | −1 825 335 (−4.8%) |

sha256: threads `439df68ba2892023f6a3216d34956e0c428935a49dbe5c46135785dcf94c0a2d`, single-session
`3fb1ad313c5f49ddbded04d9174cb2e281eefa1da7af4c9da9b03efc466d8b28`.

The `full` threads row is a REBUILD with the gates in place, not the old number carried forward: it
came out at 39 040 753 raw and 13 434 230 gzipped, byte-for-byte the same sizes as the module before
this change. **The gates cost nothing when they are all on.** (The sha differs — adding a
`[features]` table changes the crates' metadata hashes and so their symbol names — but not one byte
of size.)

Forty contribs are worth 1.8 MB of a 37 MB module, which is the honest answer to "how much of the
module is the extensions": about five percent. The interesting number is not this one; it is what a
`browser` profile will be worth once the Tier A groups can actually be switched off.

### The runtime smoke

Two files, run through the threaded wire lane's `--sql` runner, split because the runner records any
`ErrorResponse` as a lane failure:

- `wasm/browser-profile-proof.sql` — `LOAD 'pg_trgm'`, `LOAD 'pgcrypto'`, `LOAD 'vector'` all answer,
  and a GIN index over a jsonb column is built and queried (`index-gin` is on): **PASS**.
- `wasm/browser-profile-refusal.sql` — one statement, `LOAD 'dblink'`:
  `ERROR: could not access file "dblink": No such file or directory` (SQLSTATE 58P01), which is
  PostgreSQL's own missing-module error. The same statement on the `full` module answers `LOAD`.

`LOAD` rather than `CREATE EXTENSION`: the seeded image (`wasm/assets/vfs.img`) has no
`share/extension` directory at all, so `CREATE EXTENSION pg_trgm` fails with `extension "pg_trgm" is
not available` (0A000) on **every** profile, the full one included, and proves nothing. `LOAD 'name'`
goes straight to dfmgr's named-builtin-library lookup — the registration a contrib crate performs
from its `init_seams()`, and precisely the thing a feature gate removes.
