# Audit: backend-geqo-all

Independent function-by-function audit of `backend-geqo-all` (branch
`port/backend-geqo-all`) against the PostgreSQL 18.3 C sources in
`src/backend/optimizer/geqo/` and the c2rust rendering in
`../pgrust/c2rust-runs/backend-geqo-all/`.

The whole `geqo/` subdirectory (15 `.c` files) is ported as one crate, one
module per source file. Active recombination operator is **ERX** (`#define ERX`
in `geqo.h`); the other operators (PMX/CX/PX/OX1/OX2 + mutation) are ported 1:1
as always-available functions (Rust has no per-file `#if`-on-`#define`
compilation) — they are exercised by unit tests but not reached by the default
`geqo()` driver, exactly as in C where their `.c` files are `#if`-guarded out.

## Per-function table

| C function (file) | Port location | Verdict | Notes |
|---|---|---|---|
| `geqo_copy` (geqo_copy.c) | copy.rs `geqo_copy` | MATCH | element copy of `string_length` genes + worth |
| `geqo_set_seed` (geqo_random.c) | random.rs `geqo_set_seed` | MATCH | `pg_prng_fseed` → `seed_from_f64` |
| `geqo_rand` (geqo_random.c) | random.rs `geqo_rand` | MATCH | `pg_prng_double` → `next_f64` (`[0,1)`) |
| `geqo_randint` (geqo_random.c) | random.rs `geqo_randint` | MATCH | `pg_prng_uint64_range(state,lower,upper)` → `u64_range(lower,upper)`, inclusive both ends, arg order preserved |
| `init_tour` (geqo_recombination.c) | recombination.rs `init_tour` | MATCH | inside-out Fisher-Yates; `tour[0]=1` guard for `num_gene>0`; `i!=j` guard preserved |
| `alloc_city_table` (geqo_recombination.c) | recombination.rs `alloc_city_table` | MATCH | `num_gene+1` slots (index 0 unused) |
| `free_city_table` (geqo_recombination.c) | (RAII) | MATCH | `pfree` → Vec drop |
| `geqo_selection` (geqo_selection.c) | selection.rs `geqo_selection` | MATCH | two `linear_rand`, distinct-unless-size-1, copy into momma/daddy |
| `linear_rand` (static, geqo_selection.c) | selection.rs `linear_rand` | MATCH | `sqrtval = bias² - 4(bias-1)·rand`; `sqrt` only if `>0`; retry while `idx<0 \|\| idx>=max` |
| `avg_pool` (static, geqo_misc.c `#ifdef GEQO_DEBUG`) | misc.rs `avg_pool` | MATCH | divide-by-size-before-sum; `elog(ERROR,"pool_size is zero")` → panic |
| `print_pool` (geqo_misc.c) | misc.rs `print_pool` | MATCH | start/stop clamping incl. `start+stop>size` reset; `FILE*` → `core::fmt::Write` |
| `print_gen` (geqo_misc.c) | misc.rs `print_gen` | MATCH | lowest=`size>1?size-2:0`; best/lowest/mid/avg |
| `print_edge_table` (geqo_misc.c) | misc.rs `print_edge_table` | MATCH | 1..=num_gene rows, `unused_edges` entries |
| `geqo_mutation` (geqo_mutation.c `#if CX`) | mutation.rs `geqo_mutation` | MATCH | `num_gene/3` swaps; distinct swap1/swap2 |
| `cx` (geqo_cx.c `#if CX`) | cx.rs `cx` | MATCH | init table, cycle STEP1/2/3, returns `num_diffs` |
| `px` (geqo_px.c `#if PX`) | px.rs `px` | MATCH | `num_positions = randint(2n/3, n/3)`; main fill loop |
| `ox1` (geqo_ox1.c `#if OX1`) | ox1.rs `ox1` | MATCH | left/right swap, copy portion, wrap `%num_gene` fill |
| `ox2` (geqo_ox2.c `#if OX2`) | ox2.rs `ox2` | MATCH | select-list consolidation; `select_list==-1 && j<num_gene` short-circuit order preserved (table has n+1 slots so index n in bounds, matches C) |
| `pmx` (geqo_pmx.c `#if PMX`) | pmx.rs `pmx` | MATCH | DAD=1/MOM=0; STEP1/2/3; `mx_fail--` dead post-decrement kept 1:1 |
| `alloc_edge_table` (geqo_erx.c) | erx.rs `alloc_edge_table` | MATCH | `num_gene+1` slots |
| `free_edge_table` (geqo_erx.c) | (RAII) | MATCH | Vec drop |
| `gimme_edge_table` (geqo_erx.c) | erx.rs `gimme_edge_table` | MATCH | clear, fill bidirectional circular edges, return `(edge_total*2)/num_gene` as f32 |
| `gimme_edge` (static, geqo_erx.c) | erx.rs `gimme_edge` | MATCH | dedup via `abs()`, mark shared negative, return 0/1 |
| `gimme_tour` (geqo_erx.c) | erx.rs `gimme_tour` | MATCH | seed `randint(num_gene,1)`; remove_gene/gimme_gene/edge_failure; `Edge` passed by value (Copy) mirrors C `Edge edge` by-value |
| `remove_gene` (static, geqo_erx.c) | erx.rs `remove_gene` | MATCH | swap-with-last delete by `abs()` |
| `gimme_gene` (static, geqo_erx.c) | erx.rs `gimme_gene` | MATCH | shared-negative priority; min-unused tie tracking; `minimum_count not set`/`neither shared...` → panic (was `elog(ERROR)`) |
| `edge_failure` (static, geqo_erx.c) | erx.rs `edge_failure` | MATCH | four_count / remaining_edges / last-unused fallthroughs; final `no edge found` → panic. The three intermediate `elog(LOG,...)` diagnostics on fallthrough are debug logging only; control flow they precede is preserved exactly |
| `alloc_pool` (geqo_pool.c) | pool.rs `alloc_pool` | MATCH | `string_length+1` genes per chromo |
| `free_pool` (geqo_pool.c) | (RAII) | MATCH | Vec drops |
| `random_init_pool` (geqo_pool.c) | pool.rs `random_init_pool` | MATCH | discard DBL_MAX individuals; `i==0 && bad>=10000` → panic("geqo failed to make a valid plan") |
| `sort_pool` (geqo_pool.c) | pool.rs `sort_pool` | MATCH | `qsort` → `sort_by(compare)` |
| `compare` (static, geqo_pool.c) | pool.rs `compare` | MATCH | ascending by worth (eq/gt/lt → Equal/Greater/Less) |
| `alloc_chromo` (geqo_pool.c) | pool.rs `alloc_chromo` | MATCH | `string_length+1` genes |
| `free_chromo` (geqo_pool.c) | (RAII) | MATCH | Vec drop |
| `spread_chromo` (geqo_pool.c) | pool.rs `spread_chromo` | MATCH | reject-if-worse-than-worst; binary search index; copy into worst slot then shift `index..size` down. Rust `mem::take` swap dance yields identical final array contents as the C `Gene*`-pointer swap (verified by trace) |
| `geqo_eval` (geqo_eval.c) | eval.rs `geqo_eval` | MATCH | save list len + null join_rel_hash, gimme_tree, fitness=`cheapest_total_path.total_cost` else DBL_MAX, truncate+restore. Private temp MemoryContext crosses via `geqo_eval_context_create/delete` seams (owner: planner memory, unported) |
| `gimme_tree` (geqo_eval.c) | eval.rs `gimme_tree` | MATCH | clump list; per-tour single-rel clump; force-join pass if >1; success iff exactly 1 clump |
| `merge_clump` (static, geqo_eval.c) | eval.rs `merge_clump` | MATCH | force/desirable test; build+cost via seam; absorb+recurse; else size-ordered insert (size-1 fast path) |
| `desirable_join` (static, geqo_eval.c) | eval.rs `desirable_join` | MATCH | `have_relevant_joinclause \|\| have_join_order_restriction` (seamed) |
| `geqo` (geqo_main.c) | geqo_main.rs `geqo` | MATCH | set seed, pool/gen sizing, alloc pool, random_init, sort once, alloc momma/daddy, ERX edge table; per-gen selection→gimme_edge_table→gimme_tour(into momma in place, `kid=momma`)→geqo_eval→spread_chromo; final gimme_tree on best, panic if NULL; clear join_search_private. RAII frees. `(void)edge_failures` → `let _`. GUCs modeled as `GeqoConfig` (geqo_main.c is their canonical `extern` home) |
| `gimme_pool_size` (static, geqo_main.c) | geqo_main.rs `gimme_pool_size` | MATCH | `pool_size>=2` short-circuit; `2^(nr_rel+1)`; max=50·effort, min=10·effort; `ceil` |
| `gimme_number_generations` (static, geqo_main.c) | geqo_main.rs `gimme_number_generations` | MATCH | `generations>0` else pool_size |

Constants verified against headers: `DAD=1`/`MOM=0` (geqo_recombination.h),
`DEFAULT_GEQO_EFFORT=5`/`MIN=1`/`MAX=10`, `DEFAULT_GEQO_SELECTION_BIAS=2.0`/
`MIN=1.5`/`MAX=2.0` (geqo.h), `DBL_MAX=f64::MAX`, `Edge.edge_list[4]`,
`Gene=i32`. Active operator `#define ERX` → `operator = Operator::Erx`.

## Seam audit

Owned seam crates (by C-source coverage): GEQO's C files (geqo_*.c) declare no
inward seam — no other unit calls *into* geqo across a cycle (the join-search
hook that reaches `geqo()` is outside this unit). Therefore geqo owns no inward
seam and `init_seams()` is correctly a no-op (same pattern as
joinpath/functioncmds/dest).

`crates/backend-geqo-all-seams` is named after the unit but holds only
**outward** declarations that geqo *consumes*, owned by other (unported) units:

- `build_and_cost_join_rel`, `have_join_order_restriction` — owner `joinrels.c`
- `have_relevant_joinclause` — owner `joininfo.c`
- `geqo_eval_context_create` / `geqo_eval_context_delete` — owner planner
  private-memory machinery

These are thin marshal+delegate (no branching/computation in the seam path) and
each is justified by a real cycle (`geqo_eval → make_join_rel → … → join-search
hook → geqo`). They are declared-but-installed-by-nobody because their real
owners are not yet ported — sanctioned mirror-and-panic. No seam body contains
logic that belongs in this crate. The clump-merging algorithm itself
(`gimme_tree`/`merge_clump`/`desirable_join`) is ported in-crate.

`init_seams()` is wired into `seams-init::init_all()` (lib.rs:105). Both
recurrence-guard tests pass with the unit marked `audited`
(`every_declared_seam_is_installed_by_its_owner`,
`every_seam_installing_crate_is_wired_into_init_all`).

## Design conformance

- `#![forbid(unsafe_code)]`, `#![no_std]` + `extern crate alloc`. No invented
  opacity: `Edge`/`City`/`Chromosome`/`Pool` are real repr'd structs mirroring
  the C headers; `RelId` is the existing planner arena handle (not a new
  stand-in). Owned `Vec`s replace `palloc`/`pfree` (RAII).
- No `todo!()`/`unimplemented!()`; no own-logic stubs. The four panics
  (`gimme_gene` ×2, `random_init_pool`, `edge_failure`, `geqo` final, `avg_pool`)
  all map to C `elog(ERROR)` on the same predicates.
- No shared statics for per-backend globals (PRNG carried in owned
  `GeqoPrivateData`; GUCs in `GeqoConfig`).

## Verdict: PASS

Every function MATCH (or correctly SEAMED for unported cross-crate owners);
zero seam findings; no MISSING/PARTIAL/DIVERGES.

Gates (isolated `CARGO_TARGET_DIR`):
- `cargo check --workspace` — clean (warnings only)
- `cargo test -p backend-geqo-all` — 11 passed
- `cargo test -p seams-init` — 2 passed (recurrence guards)
