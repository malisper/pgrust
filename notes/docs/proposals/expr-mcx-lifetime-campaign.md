# Proposal: add `'mcx` to the flat `Expr` enum (borrow-check the node-tree UAF class)

> Status: SCOPED, plan ready, not yet executed. Branch `expr-mcx-lifetime`. This is the
> follow-on to the already-landed node-opaque flip (`struct Node<'mcx>(PgNodeBox<'mcx>)`):
> the GENERAL `Node` got its `'mcx`, but the SEPARATE flat `pub enum Expr` (primnodes.rs)
> is still lifetime-free with global-allocator children — the remaining UAF surface.

## 0. The bug class this closes

The flat expression tree is `pub enum Expr` (`crates/types-nodes/src/primnodes.rs:1805`) with NO
lifetime parameter. Its payloads are by-value structs whose children are **global-allocator**
`Vec<Expr>` / `Box<Expr>` (41 `Box<Expr>`, 24 `Vec<Expr>` fields in primnodes alone; 65 `Box<Expr>` /
402 `Vec<Expr>` workspace-wide). The enum carries arena-pointing data with FORGED `'static`
lifetimes the borrow-checker cannot police:

- **`Const.constvalue: Datum<'static>`** (primnodes.rs) — `Datum` here is
  `types_tuple::backend_access_common_heaptuple::Datum<'mcx>`, whose `ByRef(PgVec<'mcx, u8>)`
  variant points INTO an arena. Typed `'static`, so a by-ref Const's varlena bytes are unchecked.
- **`SubPlanExpr(Box<SubPlan<'static>>)`**, **`AlternativeSubPlanExpr(Box<AlternativeSubPlan<'static>>)`**
  — `'static` lies on planner subtrees.
- 46 total `'static` occurrences in primnodes.rs are this same forgery.

`Expr::clone_in(&self, mcx) -> PgResult<Expr>` and its helpers (`clone_vec_expr`, `clone_opexpr`, …)
THREAD an `mcx` parameter but allocate the spine with plain `Vec::with_capacity` on the GLOBAL heap and
return a lifetime-free `Expr`; the `mcx` is used (correctly) for any nested arena copies but the RESULT
type discharges the source-arena lifetime silently. This is exactly the gather-grouping UAF
(`b2a48345e`): `pull_var_clause` cloned Aggref/PHV children into a function-local scratch `Mcx` and
returned them through a lifetime-free `Vec<Expr>`, so the scratch's lifetime was silently discharged →
dangling `Box<_, Mcx>`. Adding `'mcx` to `Expr` makes the compiler reject returning an
`Expr<'scratch>` past the scratch arena's drop.

## 1. Blast radius (measured on `origin/main` @ 315d37069)

- **184 crates / 374 files** reference the bare `Expr` type.
- `Vec<Expr>`: **402** sites; `Box<Expr>`: **65**; `Option<Box<Expr>>`: **50** (all become
  `Vec<Expr<'mcx>>` etc).
- `PgBox<…Expr>` / `PgVec<…Expr>` FIELD carriers (already arena-boxed at the field level): **341** —
  these stay; only their element type gains `<'mcx>`.
- `Expr::<Variant>(…)` constructor sites: **1954** (1897 are also structural `match` arms).
- Seam signatures crossing `Expr`: **533** fn/Signature lines. `seam!` already supports HRTB
  (`for<'mcx> fn(&Expr<'mcx>, …)`), so the seam boundary is NOT the blocker — the blocker is that `Expr`
  has no lifetime to thread.
- Infection fan-out per payload struct (crates naming it): `Const` 120, `Var` 106, `TargetEntry` 76,
  `OpExpr` 65, `Aggref` 57, `FuncExpr` 50, `BoolExpr` 39, `CaseExpr` 17. Once `Expr<'mcx>` lands,
  EVERY struct/field that names any of these gains `<'mcx>` (or a `<'static>` alias).
- Within `types-nodes` itself: 26 structs embed `Expr` (93 inline field sites across 74 files),
  including Plan nodes (`nodenestloop`, `nodemergejoin`, …) so the lifetime also spreads into the
  Plan tree, not just primnodes.

## 2. Strategy decision: **A — `'mcx` on the flat enum, payloads stay by-value `Vec<Expr<'mcx>>`**

Two candidates were weighed:

- **A. Add `'mcx` to `Expr` (and every payload struct), children stay global-allocator
  `Vec<Expr<'mcx>>`/`Box<Expr<'mcx>>` that CARRY `'mcx`.** The lifetime threads into
  `Const.constvalue: Datum<'mcx>`, `SubPlanExpr(Box<SubPlan<'mcx>>)`, etc., closing every `'static`
  lie. The `Vec`/`Box` spine staying on the global allocator is FINE — the dangling pointer is the
  by-ref `Datum`/handle INSIDE a payload, not the Vec spine; tying the whole tree's validity to `'mcx`
  via the element type is sufficient to make the borrow-checker reject the UAF. Minimal-disruption:
  payloads keep their shape, only gain a lifetime param.

- **B. Converge the flat `Expr` onto the opaque `PgNodeBox<'mcx>`** (the trait-object machinery the
  node-opaque flip already built for the general `Node`). This is the full opaque treatment for Expr.

**Decision: A.** Rationale:
1. **A directly and minimally closes the UAF class.** The forged-`'static` carriers
   (`Datum`/`SubPlan`/`AlternativeSubPlan`) become `'mcx`-honest; that is the entire fix. B adds a
   vtable/downcast layer that is orthogonal to the lifetime problem.
2. **B's cost/benefit is already MEASURED net-negative.** The node-opaque flip (memory
   `node-flip-gate-ready-2026-06-19`) showed the opaque rep is a multi-day, ~1074-mk_*-site campaign
   with NET-NEGATIVE dev-profile size (+5.75 MB binary, types-nodes rlib +85%) and ZERO pass-rate
   benefit; release was −31 MB but only because the 261-variant ENUM forced 21 MB of `drop_in_place`
   glue — `Expr` is 49 variants and already lives behind `PgBox`/`PgVec` field carriers, so the
   drop-glue win does not transfer. Re-running B for Expr would repeat the +metadata cost without the
   drop-glue payoff.
3. **A reuses the existing `clone_in` machinery.** `Expr::clone_in(mcx)` and all per-payload
   `clone_in` already exist and are correct; A just makes their RETURN type honest
   (`PgResult<Expr<'mcx>>`), so the compiler enforces what the code already intends.
4. **A keeps every structural `match Expr::V(..)` working** (1897 sites) — no accessor-method rewrite,
   unlike B which forces `as_v()`/`into_v()` everywhere.

The `<'static>` alias bridge (below) keeps the 184 downstream crates compiling between steps under A;
under B the same crates would ALSO need accessor rewrites, compounding the churn.

## 3. The alias-bridge that keeps the tree compiling between steps

The lifetime is infectious but the conversion is per-crate stageable IF we keep a `'static`-pinned
alias at each crate boundary that hasn't been converted yet:

```rust
// types-nodes, during the campaign:
pub enum Expr<'mcx> { Var(Var<'mcx>), Const(Const<'mcx>), … }
pub type ExprStatic = Expr<'static>;        // bridge name for not-yet-converted consumers
// (and Const, Var, OpExpr, … each get an analogous `<…>Static = …<'static>` alias)
```

A consumer crate that has NOT been converted imports `ExprStatic`/`ConstStatic`/… (a sed rename
`Expr`→`ExprStatic` at its boundary) and compiles UNCHANGED — its node trees are all `'static`, exactly
today's behavior (no safety REGRESSION, no safety GAIN yet). A consumer that HAS been converted names
`Expr<'mcx>` and gets the borrow-check. The campaign is "walk crates from leaves up, flip
`ExprStatic`→`Expr<'mcx>` one crate at a time, each commit green."

## 4. Constructor-fallibility is NOT a new cost here

Unlike the node-opaque flip (whose `PgNodeBox::new` allocs FALLIBLY, forcing `PgResult` through ~527
`mk_*` chains), **Strategy A does not change allocation fallibility**: `Expr::V(payload)` stays an
infallible enum construction; the 1954 ctor sites keep their exact form, only the payload type gains a
lifetime. The ONLY fallibility already present is in `clone_in` (already `PgResult`). So the
527-mk_*-PgResult long pole from the Node flip DOES NOT APPLY to Expr. This is a major reason A is
tractable where B was a multi-week campaign.

## 5. Staged plan (per-crate, each commit compiles)

**Phase 0 — types-nodes internal (one atomic commit within the crate, alias-bridged outward).**
0a-pre. **`build.rs` string-parses the enum header** — `parse_rust_enum_body` (build.rs:606) does
    `src.find("pub enum Expr {")` to source the variant table for the generated `etag` module +
    accessors, and PANICS if the header changes. So Phase 0 MUST first teach `parse_rust_enum_body`
    (and any per-variant line parser) to accept `pub enum Expr<'mcx> {` and to strip a `<'mcx>` /
    lifetime suffix off each `Ident(Payload<'mcx>),` line. VERIFIED empirically: adding `<'mcx>` to the
    header without touching build.rs fails the build script at build.rs:606 before rustc even runs.
0a. Rename `pub enum Expr` → `pub enum Expr<'mcx>`; add `pub type ExprStatic = Expr<'static>`.
0b. Add `<'mcx>` to all 26 Expr-embedding structs in types-nodes (Const, Var, OpExpr, FuncExpr, Aggref,
    BoolExpr, … + the Plan nodes that inline Expr); each gets a `…Static = …<'static>` alias.
0c. Children: `Vec<Expr>`→`Vec<Expr<'mcx>>`, `Box<Expr>`→`Box<Expr<'mcx>>`,
    `Option<Box<Expr>>`→`Option<Box<Expr<'mcx>>>`.
0d. Forged-`'static` carriers become honest: `Const.constvalue: Datum<'mcx>`,
    `SubPlanExpr(Box<SubPlan<'mcx>>)`, `AlternativeSubPlanExpr(Box<AlternativeSubPlan<'mcx>>)`, and the
    other 46 `'static` sites.
0e. `clone_in` return types: `-> PgResult<Expr<'mcx>>` keyed to the `mcx` param's lifetime; fix
    `clone_vec_expr`/`clone_opexpr`/… to return `Vec<Expr<'mcx>>`. (The bodies are already correct.)
0f. Re-export BOTH `Expr` (now `<'mcx>`) AND `ExprStatic` from `lib.rs`. **Every downstream crate
    imports the `…Static` aliases via a mechanical sed**, so types-nodes flips green while all 184
    consumers stay on `'static` and compile unchanged. THIS is the heavy commit; it is the only one
    that touches types-nodes structurally.

**Phase 1..N — convert consumer crates leaf-up.** Process in reverse-topological order
(leaves first: `backend-optimizer-util-vars`, `backend-optimizer-util-clauses`,
`backend-optimizer-util-joininfo`, then `…-path-*`, then `…-plan-*`, then parser/rewrite/executor).
For each crate: replace its `…Static` imports with `…<'mcx>` (threading `'mcx` through its own fns,
guided by rustc), keeping the seam signatures HRTB (`for<'mcx> fn(&Expr<'mcx>, …)`). Each crate is an
independent green commit. The UAF protection materializes incrementally as crates convert; the gather-
grouping site (`pull_var_clause` in `backend-optimizer-util-vars`) is an EARLY target (it is a leaf
util) — converting it first re-proves the fix on the original bug.

**Phase FINAL — drop the aliases.** Once all 184 crates name `Expr<'mcx>`, delete the `…Static = …<'static>`
aliases from types-nodes. The forged `'static` is now physically absent from the codebase; the seam
signatures are already HRTB so no seam flip is needed (the seam boundary was never the blocker).

### Risk / order notes
- The two-lifetime decoupling pattern from the Node flip applies: a fn holding `&'a Expr<'a>` should
  become `&'a Expr<'mcx>` to avoid E0621 invariance friction.
- Seam fn-ptrs that take `&Expr` become `for<'mcx> fn(&Expr<'mcx>, …)` (HRTB already supported).
- `RinfoRef(u32)` is an INDEX handle (registry-keyed), not a pointer — it does NOT need `'mcx` and is
  not part of the lie; leave it lifetime-free.
- Phase 0 is the single non-decomposable commit (all of types-nodes' Expr structs share the lifetime);
  Phases 1..N are freely parallelizable across lanes once Phase 0 lands.

## 6. Why not just fix `clone_vec_expr` to allocate in `mcx`?

Making `clone_vec_expr` return `PgVec<'mcx, Expr>` instead of `Vec<Expr>` would arena-allocate the spine
but does NOT fix the bug: the dangling data is the by-ref `Datum`/`SubPlan` INSIDE each `Expr`, which is
still typed `'static` and still escapes. The lifetime MUST reach the leaf carriers — i.e. `Expr` itself
must carry `'mcx`. The spine allocator is a red herring.
