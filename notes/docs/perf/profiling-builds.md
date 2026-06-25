# Fast symbolized profiling builds

A symbolized profiling build used to cost ~10 min **every** time, because each
reprofile (a) ran in a throwaway `git worktree` with an **empty `target/`** → a
cold full-workspace build of ~1,449 crates, and (b) edited `[profile.release]`
(`debug=1, strip=false`) → which changes the profile fingerprint and invalidates
the entire release cache anyway.

Two changes fix that:

## 1. The `profiling` cargo profile (in the root `Cargo.toml`)

```toml
[profile.profiling]
inherits = "release"   # identical opt-level / codegen-units=1 / lto → numbers match production
debug = 1              # DWARF debug info: Rust frames resolve by name, not `???`
strip = false          # keep symbols
```

It inherits `release` so the **codegen is identical to the production binary**
(critical — changing `opt-level`/`codegen-units` would change inlining and skew the
profile). It only adds debug info + keeps symbols. Crucially it builds into its own
**`target/profiling/`** subdir, so building it does **not** clobber the `release`
cache, and vice-versa.

## 2. A persistent, shared build cache (warm `CARGO_TARGET_DIR`)

Point every profiling build at one shared, persistent target dir so the
`profiling` artifacts survive between reprofiles → only the crates that changed
since last time recompile (incremental), instead of a cold rebuild.

```sh
# One shared, persistent profiling target dir (do NOT delete between reprofiles):
export CARGO_TARGET_DIR=/private/tmp/pgrust-profiling-target

# Build the symbolized binary (first time: full; afterwards: incremental):
cargo build --profile profiling --bin postgres

# Binary lands at:
$CARGO_TARGET_DIR/profiling/postgres
```

This works even from a fresh `git worktree` on the latest `main` — cargo keys the
cache by crate + fingerprint, so a shared `CARGO_TARGET_DIR` is reused across
worktrees and only rebuilds what actually changed. The first build after this lands
is still cold (it primes the cache once); every reprofile after that is fast.

## Recipe for a reprofile (the fast path)

```sh
# 1. worktree on current main (gets the latest code), build into the SHARED cache:
git worktree add -b reprofile-$(date +%s) /private/tmp/reprofile main   # (stamp passed in, not via $(date) inside a workflow)
cd /private/tmp/reprofile
CARGO_TARGET_DIR=/private/tmp/pgrust-profiling-target cargo build --profile profiling --bin postgres
BIN=/private/tmp/pgrust-profiling-target/profiling/postgres

# 2. boot + sustained boolean.sql loop + sample (see docs/perf/boolean-profile*.md for the boot flags)
#    /usr/bin/sample <busiest-backend-pid> 30 -file prof.txt
```

Do **not** edit `[profile.release]` to get symbols (that forces a cold rebuild and
risks committing the edit). Use `--profile profiling`.

## Why the build is inherently heavy (the part we keep)

Even warm, the changed-crate recompiles can be non-trivial because `main`'s
`release`/`profiling` profile uses `codegen-units = 1` + `opt-level = 3`, and a lot
of the c2rust-ported code has **very large functions** (the generated grammar
`gram.rs`, big catalog `match` statements, node-tree machinery) that LLVM optimizes
slowly. We keep that config because the profile must match the production binary —
the win here is avoiding the *cold full-workspace* rebuild, not changing the codegen.
