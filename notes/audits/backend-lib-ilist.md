# Audit: backend-lib-ilist

- **Unit:** backend-lib-ilist
- **C source:** `src/backend/lib/ilist.c` (+ struct types from `src/include/lib/ilist.h`)
- **c2rust:** `c2rust-runs/backend-lib-all/src/ilist.rs`
- **Port:** `crates/backend-lib-ilist/src/lib.rs`
- **Verdict: PASS**

## Function inventory (from ilist.c)

`ilist.c` defines exactly 4 functions, no statics, no in-TU inline helpers. The
header (`ilist.h`) inline functions (init/push/insert/delete-current/iteration)
are emitted into consuming crates, not into this TU, so they are out of scope
for this unit. c2rust (built without `ILIST_DEBUG`) kept only `slist_delete`,
confirming the other three are `#ifdef ILIST_DEBUG`-gated.

| # | C function | C loc | Gate | Port loc | Verdict | Notes |
|---|-----------|-------|------|----------|---------|-------|
| 1 | `slist_delete` | ilist.c:30-52 | always | lib.rs `slist_delete` | MATCH | |
| 2 | `dlist_member_check` | ilist.c:59-71 | `ILIST_DEBUG` | lib.rs `dlist_member_check` | MATCH | |
| 3 | `dlist_check` | ilist.c:76-108 | `ILIST_DEBUG` | lib.rs `dlist_check` | MATCH | |
| 4 | `slist_check` | ilist.c:113-127 | `ILIST_DEBUG` | lib.rs `slist_check` | MATCH | |

## Per-function detail

### slist_delete  — MATCH
C: `last = &head->head`; `while ((cur = last->next) != NULL)`; if `cur == node`
then `last->next = cur->next`, set `found`, break; else `last = cur`.
Trailing `Assert(found)` (USE_ASSERT_CHECKING) and `slist_check(head)`.
Port reproduces the walk identically (`&raw mut (*head).head`, NULL break,
pointer-equality splice, `last = cur`). The `found` flag is kept under
`#[cfg(debug_assertions)]` and checked via `debug_assert!(found)`, mirroring
`PG_USED_FOR_ASSERTS_ONLY` + `Assert(found)`. The trailing `slist_check(head)`
is invoked through `abort_on_corruption` (no-op unless `ilist_debug`), matching
the C source. c2rust (no ILIST_DEBUG, no asserts) elides both the assert and
the check; the port is a faithful superset that matches c2rust's compiled
release behavior and the C source under each build config. Pointer-walk splice
logic is byte-for-byte identical to c2rust.

### dlist_member_check — MATCH
C: open-coded forward walk `cur = head->head.next; cur != &head->head;
cur = cur->next`; return on `cur == node`; else `elog(ERROR, "double linked
list member check failure")`. Port matches the walk and the sentinel terminator
(`&raw const (*head).head`); the `elog(ERROR, ...)` maps to
`Err(PgError::error("double linked list member check failure"))` with identical
message text (failure-surface contract). Behind `ilist_debug`; no-op `Ok(())`
otherwise, matching the C macro `((void)(head))`.

### dlist_check — MATCH
C: NULL head -> `elog(ERROR, "doubly linked list head address is NULL")`;
both-NULL sentinel -> return (zero-init OK); forward then backward walks, each
firing `elog(ERROR, "doubly linked list is corrupted")` on the 5-clause
predicate (`cur==NULL || cur->next==NULL || cur->prev==NULL ||
cur->prev->next!=cur || cur->next->prev!=cur`). Port reproduces all three error
sites with exact message text, the zero-init early return, and both walk
directions with the identical 5-clause predicate and sentinel comparison.
Behind `ilist_debug`; no-op otherwise.

### slist_check — MATCH
C: NULL head -> `elog(ERROR, "singly linked list head address is NULL")`; walk
to NULL terminator (cycle/termination check only). Port matches: NULL -> Err
with identical text; walk `cur = (*head).head.next` to null. Behind
`ilist_debug`; no-op otherwise.

## Struct types (ilist.h, repr(C) mirrors) — MATCH
- `dlist_node { prev, next: *mut dlist_node }` == ilist.h:137-141.
- `dlist_head { head: dlist_node }` == ilist.h:151-161.
- `dclist_head { dlist: dlist_head, count: u32 }` == ilist.h:212-216.
- `slist_node { next: *mut slist_node }` == ilist.h:224-227.
- `slist_head { head: slist_node }` == ilist.h:236-239.
Layout verified by `struct_layouts_match_c` test.

## Seams and wiring
No `*-seams` crate maps to `lib/ilist.c` (none exists in the workspace). `ilist.c`
performs no allocation and calls no out-of-unit logic, so it is a pure leaf that
owns **no inward seams** and makes **no outward seam calls**. No `init_seams()`
is required and none is added; `seams-init` is untouched. Compliant.

## Design conformance
- No invented opacity: the port mirrors the C structs field-for-field as raw
  pointers (C-faithful raw-pointer port, same approach as backend-lib-dshash).
- No allocation -> no `Mcx` needed; no shared statics; no ambient-global seams;
  no locks. `elog(ERROR)` integrity failures surface as `PgResult`/`PgError`
  per the failure-surface rule; `slist_delete`'s only failure is a debug Assert,
  so it returns `()`.
- Zero `todo!`/`unimplemented!`/`unreachable!`; no divergence markers.

## Gates
- `cargo check --workspace`: pass (only pre-existing warnings in
  backend-access-common-printtup, unrelated).
- `cargo test -p backend-lib-ilist`: 6 pass (default), 10 pass (`--features ilist_debug`).
- `cargo test -p seams-init`: 2 pass (recurrence guards green).
