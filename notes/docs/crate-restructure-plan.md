# Crate restructure plan — mirror the PostgreSQL source tree

Status: **validated migration tooling on branch `crate-restructure`. NOT applied
repo-wide.** Run the full apply only when the fleet is QUIET (see "Apply
recommendation").

## Goal / target layout

Today every crate is flat under `crates/` and named after the postgres path it
ports, hyphen-joined:

```
crates/backend-access-heap-heapam/      package = backend-access-heap-heapam
```

The target puts the *path* in the directory and names the crate after the `.c`
file it ports — `"common"` (and every other dir) lives in the DIRECTORY, never
in the crate name:

```
crates/backend/access/heap/heapam/      package = heapam
crates/backend/access/common/printtup/  package = printtup
```

The current hyphenated name already encodes the postgres path 1:1, so the
transform is mechanical: split the name into a directory path + a leaf crate
name (the `.c` filename).

## Survey findings (1448 crates @ `d56a68ae0`)

- **`package.name == directory name` for all 1448 crates** — the package name is
  the single source of truth for a crate's identity and postgres path.
- **The import ident is the `[lib]` name, which is usually `pkg.replace('-','_')`
  but is OVERRIDDEN in 15 crates** — the `*-fgram` crates expose a lib name
  WITHOUT the `-fgram` part (e.g. `backend-nodes-list-fgram` → lib
  `backend_nodes_list`), and `backend-executor-tstoreReceiver` lib-cases to
  `backend_executor_tstorereceiver`. Import rewriting therefore keys off the
  REAL lib name read from each `Cargo.toml`, not the package name.
- **All path dependencies are flat siblings**: `foo = { path = "../foo" }`.
  After nesting they must be re-relativized (a heap crate's deps become
  `{ path = "../../../../_support/..." }`).
- **The workspace `members` is a single `["crates/*"]` glob.** Cargo's member
  globs error on any matched directory without a `Cargo.toml`; with varying
  nesting depth no single glob works, so the apply writes an EXPLICIT member
  list of every dir that contains a `Cargo.toml`.
- Prefix taxonomy: `backend` 1250, `types` 111, `common` 44, `port` 18,
  `contrib` 7, plus `pgrust/pg/interfaces/state/seam(s)/mcx/probe/portability/no/
  amcheck`.

### Directory derivation (curated postgres vocabulary)

`backend-*`, `common-*`, `port-*`, `interfaces-*`, `contrib-*` map to
`src/backend/`, `src/common/`, `src/port/`, `src/interfaces/`, `contrib/`. A
curated table of postgres subdirectories (longest token-prefix wins) resolves
the dir/file boundary — e.g. `backend/access/heap`, `backend/utils/adt`,
`backend/storage/buffer`, `backend/optimizer/path`. Special cases handled:
`spg*→spgist`, `nbt*→nbtree`, `backend-conv-*→backend/utils/mb/conversion_procs`,
`backend-pl-plpgsql-*→pl/plpgsql/src`, `backend-timezone-*→src/timezone`.

### Support crates (no direct postgres home)

`types-*`, `pgrust-*`, `pg-*`, `state-*`, `seam(s)-*`, `mcx`, `probe-*`,
`portability-*`, `no-*` are pgrust-invented. They go under `crates/_support/<sub>`
(e.g. `crates/_support/types/`, `crates/_support/seam/`). `amcheck-*` is rehomed
to `contrib/amcheck`. The `_support` prefix sorts these clearly apart from the
postgres-mirroring tree.

## Collision-resolution rules

Dropping the path prefix makes many leaf names collide (606 crates would become
`seams`, 20 would become `core`, etc.) and some hit Rust-reserved/std names
(`core`, `main`, `type`, `box`, ...). Crate names must be **globally unique
across the workspace** and **never a reserved ident**, regardless of directory.
Rules, applied deterministically in order:

1. **Leaf = path tail after the matched directory.** `minmax-multi` →
   `minmax_multi` (the real `minmax_multi.c`). pgrust suffixes (`-seams`,
   `-core`, `-fgram`, `-pc`, `-next`, ...) are NOT postgres files and are kept
   in the leaf so they already disambiguate — `backend-access-heap-heapam-seams`
   → `heapam_seams` (not `seams`). This alone cuts the 606-way `seams` collision
   to 12.
2. **Reserved-name guard.** If a leaf equals a Rust keyword / std crate / reserved
   word (full list in the tool's `RESERVED` set), prefix the nearest parent
   directory token: `backend-access-gist-core` → `gist_core`,
   `backend-main-main` → `main_main`.
3. **Uniqueness disambiguation.** For any leaf still shared by >1 crate, prepend
   the nearest distinguishing directory token(s) until unique within the group:
   `backend-storage-aio-seams` → `aio_seams`, `backend-access-brin-xlog` →
   `brin_xlog`, `backend-access-transam-xlog` → `transam_xlog`. (Occasionally
   redundant vs. the dir, but required for global uniqueness — an accepted
   tradeoff.)
4. **Final paranoia pass.** Any residual duplicate gets a deterministic numeric
   suffix `_2`, `_3`, … ordered by sorted old package name.

Result on the full set: **240 collisions auto-resolved, 0 residual duplicates,
0 reserved-name hits across all 1448 crates.** Full resolution table:
`docs/crate-restructure-collisions.tsv`. Full rename map (old_pkg, old_lib,
new_dir, new_pkg, new_lib): `docs/crate-restructure-map.tsv`.

## The migration script — `tools/crate_restructure.py`

Pure-stdlib Python 3.11+ (uses `tomllib`). Given the computed map it:

- `git mv`s each crate dir to `crates/<target_dir>/<leaf>/`.
- Rewrites `package.name` and (where overridden) `[lib].name` in each moved
  `Cargo.toml`.
- Rewrites every `[dependencies]` key that names a moved crate AND re-relativizes
  every `path = "..."` whose endpoint moved (including a moved crate's deps onto
  unmoved crates, which now sit deeper).
- Rewrites the workspace `members` to an explicit list of all manifest dirs.
- Rewrites every `use <old_lib>` / `extern crate <old_lib>` / fully-qualified
  `<old_lib>::...` across all `.rs`, keyed off the real lib name.

Properties: **idempotent** (skips already-moved dirs / already-renamed deps),
**dry-runnable**, asserts **zero residual collisions + zero reserved hits**
before touching anything.

### Usage

```
# Full plan, no writes; also emit the rename map:
tools/crate_restructure.py --dry-run --emit-map docs/crate-restructure-map.tsv

# Prototype: migrate ONE self-contained subtree and validate:
tools/crate_restructure.py --subtree backend-access-heap --apply

# Full repo-wide apply (run only when the fleet is QUIET):
tools/crate_restructure.py --apply
```

## Validation result (prototype, captured)

Applied to the `backend-access-heap` subtree (20 crates) on this branch:

- **20 dirs moved, 164 files rewritten** (Cargo.tomls + `.rs` idents).
- `crates/backend/access/heap/{heapam, heapam_seams, heapam_visibility,
  pruneheap, hio, vacuumlazy, visibilitymap, ...}` created; deps to unmoved
  crates re-relativized to `../../../../<flat>`, intra-subtree deps to
  `../<sibling>`.
- **`cargo metadata --no-deps` resolves all 1448 packages, exit 0** — the whole
  workspace dependency graph is consistent (every path dep resolves, every
  package name unique).
- **`cargo build -p heapam_visibility` and `cargo build -p heapam` both
  succeed** from the new nested locations.
- **`cargo build -p backend-commands-copyfrom` (an UNMOVED crate that depends on
  the moved `heapam`) succeeds** — proving its dep key + path were rewritten and
  its `backend_access_heap_heapam::` imports were rewritten to `heapam::`
  (grep for the old ident in its `src/` returns nothing).

The prototype was then reverted; the branch carries the tooling + docs only.

## Apply recommendation

**Do NOT run `--apply` (full) while any lane is active.** The full apply rewrites
EVERY crate directory and EVERY cross-crate reference (~1448 dirs, thousands of
`.rs`/`Cargo.toml` edits) and must land as a single atomic commit. It will
conflict catastrophically with any concurrent lane (every lane references crates
by today's names), and a mid-apply state will not build.

Run it only when the fleet is fully idle:

1. Confirm `origin/main` is quiet (no in-flight lane pushes) and the suite is
   green (230/230).
2. Branch from the latest `main`; run `tools/crate_restructure.py --dry-run`
   first and confirm `0 residual duplicate names / 0 reserved-name hits`.
3. `tools/crate_restructure.py --apply`.
4. `cargo metadata --no-deps` (graph check), then a full `cargo build` /
   regression run.
5. Commit as ONE atomic commit; merge to `main` immediately so no lane rebases
   across it.

Re-run `--apply` is safe (idempotent) if interrupted.
