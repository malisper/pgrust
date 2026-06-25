# Audit: backend-lib-knapsack

Independent function-by-function audit of `crates/backend-lib-knapsack`
against the original Postgres C (`../pgrust/postgres-18.3/src/backend/lib/knapsack.c`
+ `src/include/lib/knapsack.h`). Re-derived from the C source; no c2rust run
exists for this unit (`../pgrust/c2rust-runs/backend-lib-knapsack/` absent) and
no src-idiomatic counterpart was used as the authority — the port targets THIS
repo's `Mcx`/`PgBox` bitmapset model, which differs from src-idiomatic's
`OwnedBitmapset`.

## 1. Function inventory

`knapsack.c` defines exactly one function (no statics, no inline helpers):

| C function | C location | Port location | Verdict |
| --- | --- | --- | --- |
| `DiscreteKnapsack` | knapsack.c:50 | src/lib.rs `DiscreteKnapsack` | MATCH |

The header `knapsack.h` declares only `DiscreteKnapsack` (the public ABI). No
other definitions exist.

## 2. Per-function comparison — DiscreteKnapsack

Signature: C
`Bitmapset *DiscreteKnapsack(int max_weight, int num_items, int *item_weights, double *item_values)`.
Port `DiscreteKnapsack<'mcx>(mcx: Mcx<'mcx>, max_weight: i32, num_items: i32,
item_weights: &[i32], item_values: Option<&[f64]>) -> PgResult<Option<PgBox<'mcx, Bitmapset<'mcx>>>>`.

- The optional `double *item_values` (C NULL = "all values 1") becomes
  `Option<&[f64]>` — exact, including the `iv = item_values ? item_values[i] : 1`
  branch (`match item_values { Some(v) => v[i], None => 1.0 }`).
- The C NULL/empty `Bitmapset *` result is `None`; a non-empty result is
  `Some(PgBox<Bitmapset>)`. Matches the crate-wide bitmapset NULL convention.
- The extra leading `mcx: Mcx<'mcx>` parameter is the analogue of C's implicit
  `CurrentMemoryContext` at entry — the context the result must be returned in.
  Required by this repo's explicit-context allocation rule (no ambient global).

Control-flow / line-by-line:

- `local_ctx = AllocSetContextCreate(CurrentMemoryContext, "Knapsack",
  ALLOCSET_SMALL_SIZES)` → `mcx.context().new_child("Knapsack")`. Child of the
  caller's context, same name. ALLOCSET_SMALL_SIZES is AllocSet block sizing;
  the pilot context is pure byte-accounting, so block sizing is moot — name
  carried, behavior preserving. MATCH.
- `MemoryContextSwitchTo(local_ctx)` → scratch allocs routed through
  `local_ctx.mcx()` (`lcx`). Equivalent to switching the current context for the
  scratch palloc's. MATCH.
- `Assert(max_weight >= 0)` → `debug_assert!(max_weight >= 0)`. MATCH.
- `Assert(num_items > 0 && item_weights)` → `debug_assert!(num_items > 0 &&
  item_weights.len() >= num_items as usize)`. The C `item_weights` non-NULL
  assert maps to a slice-length precondition (a Rust slice is never NULL); the
  `is_none_or` length check on `item_values` adds the same trusted-length
  contract C relies on when the pointer is supplied — debug-only, no runtime
  divergence. MATCH.
- `values = palloc((1+max_weight)*sizeof(double))` → `vec_with_capacity_in(lcx,
  1 + max_weight as usize)` of `f64`. `sets = palloc(... sizeof(Bitmapset*))` →
  same-length `PgVec<Option<PgBox<Bitmapset>>>`. palloc OOM in C is a
  non-local elog(ERROR) exit; here it is `?`-propagated `PgResult` — same
  failure surface. MATCH.
- Init loop `for (i = 0; i <= max_weight; ++i) { values[i]=0; sets[i] =
  bms_make_singleton(num_items); }` → `for _i in 0..=max_weight { values.push(0.0);
  sets.push(Some(bms_make_singleton(lcx, num_items)?)); }`. Inclusive `0..=max_weight`
  matches `<= max_weight`. `bms_make_singleton(num_items)` installs the
  unused high bit (member index `num_items`, one past the last valid item index
  `num_items-1`) so storage is pre-sized once — preserved exactly. MATCH.
- Outer loop `for (i = 0; i < num_items; ++i)` → `for i in 0..num_items`.
  `iw = item_weights[i]`, `iv = item_values ? item_values[i] : 1`. MATCH.
- Inner loop `for (j = max_weight; j >= iw; --j)` → `let mut j = max_weight;
  while j >= iw { ...; j -= 1; }`. Descending bound `j >= iw` and decrement
  match exactly (the larger-to-smaller pass that lets the array be reused). MATCH.
- `ow = j - iw`. `if (values[j] <= values[ow] + iv)` → identical `<=` predicate
  (the `<=`, not `<`, is what lets weight-0 items always join). MATCH.
- `if (j != ow) sets[j] = bms_replace_members(sets[j], sets[ow]);` → the
  `j != ow` guard preserved; both endpoints are `.take()`n out of the `PgVec`
  (so `sets[ow]` can be read while `sets[j]` is consumed), `bms_replace_members`
  overwrites `sets[j]`'s contents from `sets[ow]` (resizing only if needed,
  matching C's "without realloc" comment), and `sets[ow]` is restored unchanged.
  No clone of `sets[ow]` is introduced — `as_deref()` borrows it. MATCH.
- `sets[j] = bms_add_member(sets[j], i);` → `sets[j] = Some(bms_add_member(lcx,
  sets[j].take(), i)?)`. MATCH.
- `values[j] = values[ow] + iv;` → identical. MATCH.
- `MemoryContextSwitchTo(oldctx)` then `result = bms_del_member(bms_copy(
  sets[max_weight]), num_items)` → `bms_copy(mcx, sets[max_weight].as_deref())?`
  copies the winning scratch set into the caller's context `mcx` (the analogue
  of switching back to `oldctx` before copying), then `bms_del_member(copied,
  num_items)` strips the unused high bit. `bms_del_member` here takes no `Mcx`
  (it only clears/trims in place) — matches C, which never grows in del. The
  ordering (copy out of scratch, then delete the scratch context) is preserved.
  MATCH.
- `MemoryContextDelete(local_ctx)` → `local_ctx` and the two scratch `PgVec`s
  (and every working bitmapset they hold) drop at function exit in reverse
  declaration order (`sets`, `values`, then `local_ctx`), reclaiming all scratch
  in one shot. `result` already lives in `mcx`, so it survives. MATCH.
- `return result;` → `Ok(result)`. MATCH.

## 3. Seams and wiring

Pure leaf data structure. The unit's single C file `knapsack.c` has no
corresponding `crates/knapsack-seams` / `*-seams` crate — it owns **no inward
seams**. Therefore:

- No `init_seams()` (correctly absent); nothing for `seams-init::init_all()` to
  call.
- No **outward** seam calls. The dependencies (`bms_make_singleton`,
  `bms_copy`, `bms_add_member`, `bms_replace_members`, `bms_del_member`) are
  satisfied by a direct dependency on the already-ported `backend-nodes-core`
  (the bitmapset KEYSTONE), with no dependency cycle, so a direct call is correct
  — no seam is warranted or introduced. `seams-init` recurrence-guard tests pass.

## 4. Design conformance

- Opacity: none introduced. `Bitmapset` is the real `types_nodes::bitmapset::Bitmapset`,
  not an invented handle. (opacity-inherited-never-introduced — OK.)
- Allocation: the allocating entry point carries `Mcx` and returns `PgResult`;
  scratch allocation OOM propagates as `PgError` (the palloc non-local-exit
  surface). (OK.)
- No shared statics, no ambient-global seams, no locks, no registry side tables,
  no divergence markers.
- Constants: no OIDs/NodeTags/flag bits/magic numbers in this function;
  `BITS_PER_BITMAPWORD` etc. live in the audited `backend-nodes-core`, not here.

## 5. Residual todo!()/unimplemented!()

None. `grep` for `todo!`/`unimplemented!`/`unreachable!`/`panic!` in
`crates/backend-lib-knapsack/src` returns nothing.

## Verdict

**PASS.** The sole function `DiscreteKnapsack` is a logic-exact, idiomatic port
of `knapsack.c`. No seams owned or needed; no residual stubs. Gate:
`cargo check --workspace` clean, `cargo test -p backend-lib-knapsack` 5/5 pass,
`cargo test -p seams-init` pass.
