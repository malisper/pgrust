---
name: next-crates
description: Maintain CATALOG.tsv against the c2rust crate catalog and report which units are ready to port next, ordered bottom-up by dependencies. Use when deciding what to work on.
---

# next-crates

Answer: "which crates can we work on next?" The catalog ground truth is the
c2rust translation in `../pgrust/c2rust-runs` (it built a real, regress-passing
Postgres, so its unit list is complete). Dependency order comes from the
hand-port workspaces, because c2rust units are standalone and carry no cargo
dependency edges.

## 1. Refresh the catalog

- Enumerate units: `ls ../pgrust/c2rust-runs/*/Cargo.toml` (≈1,067 units).
- `CATALOG.tsv` columns: `unit  c_sources  status  crate  notes`.
  - `c_sources`: from `../pgrust/manifests/<unit>.json` (`source_files`, strip the
    `postgres-18.3/` prefix) when the manifest exists; otherwise inferred
    `*/<basename>.c` from `c2rust-runs/<unit>/src/*.rs`. Resolve inferred paths to
    real ones (search `../pgrust/postgres-18.3/src`) when you touch a row.
  - `status`: `todo | in-progress | ported | audited | merged | skipped:<reason> |
    duplicate-of:<unit> | combined-into:<unit>`.
  - `crate`: the crate name(s) in this repo covering the unit. Not 1:1 —
    src-idiomatic combined some units, c2rust duplicated some; record the mapping
    explicitly rather than assuming name equality.
- Add new rows for unknown units; never delete rows (mark `duplicate-of`/`skipped`
  instead, with the reason).

## 2. Reconcile status against the live tree

Statuses are set ONLY from evidence in this repo: the crate exists under
`crates/`, its `init_seams()` is registered in `seams-init`, an audit report
exists in `audits/`, the merge is in `git log`. Never mark a row from memory, a
plan document, or a comment — in the old repo those claims were routinely wrong
in both directions. Before proposing any unit as "next", spot-check it isn't
already done here under a combined or renamed crate (grep for its function
names, not just its crate name).

## 3. Compute the frontier

1. Dependency edges: run `cargo metadata --no-deps` in `../pgrust/src-idiomatic`
   and read each crate's path dependencies (secondary cross-check:
   `../pgrust/src`). Map those crate names back to catalog units via the `crate`
   column conventions; record newly discovered mappings in `notes`.
2. A unit is **ready** when every unit it depends on is already `merged` (or
   `audited`). Bottom-up ordering minimizes the window where seams panic.
3. Rank ready units by **fan-in**: how many todo units depend on them (higher
   unblocks more). Leaves with zero unported deps come first.
   **Tiebreak upward: units that own declared-but-uninstalled seams.** For each
   ready unit, check whether its `crates/<unit>-seams` crate already exists
   with declarations nothing installs yet — those declarations are consumers'
   guesses at the owner's surface, and every additional consumer compounds the
   cost of a wrong guess. Porting the owner validates the signatures against
   the real implementation while the call-site count is still small. Report
   the pending-seam-declaration count per ready unit alongside fan-in.
4. Cycles in the C are expected — that's what seams are for. A unit stuck only
   behind a cycle partner is still ready; note the partner so the port knows
   which calls will go through `<partner>-seams`.

## 4. Report

Output a ranked list: unit, its C sources, dep-satisfaction (`n/m` deps merged,
naming the missing ones), fan-in count, the src-idiomatic crate(s) to copy from,
and a suggested batch (keep batches small and high-confidence). Note any units
with no src-idiomatic counterpart — those are ports from C/c2rust directly and
cost more.

## Completeness check (run occasionally)

Every `.c` under `../pgrust/postgres-18.3/src/backend`, `src/common`, and
`src/port` that is compiled into the server should map to some catalog unit.
Diff the file list against the union of `c_sources`; unaccounted files mean the
catalog (or a manifest inference) has a hole — add rows rather than discovering
the gap at link time.
