# Audit: backend-utils-resowner-resowner

- **Verdict: PASS**
- Date: 2026-06-13
- Model: Opus 4.8 (1M context) — claude-opus-4-8[1m]
- Branch: port/backend-utils-resowner-resowner
- C source: `src/backend/utils/resowner/resowner.c` (PG 18.3)
- c2rust: `../pgrust/c2rust-runs/backend-utils-resowner-all/`, `../pgrust/c2rust-runs/probe-utils-resowner-resowner/`

## Summary

`backend-utils-resowner-resowner` is the same `resowner.c` as
`backend-utils-resowner-all` and `probe-utils-resowner-resowner`. Per the
standing architecture decision (`docs/query-lifecycle-raii.md`, decided
2026-06-12; CATALOG row `backend-utils-resowner-all` = `dissolved`), `resowner.c`
is **deliberately not ported as a crate**. Its semantics dissolve into:

1. Frame-local **RAII guards** (`BufferPin`, file/DSM guards, …) whose `Drop`
   replaces `ResourceOwnerRemember`/`Forget` pairs;
2. The **transaction/portal owner value** (`TxnResources`) whose field order
   carries the `RESOURCE_RELEASE_*` phase ordering, consumed at commit (WARN on
   leak) and dropped at abort (silent);
3. Explicit `Mcx`/`Ctx` threading that replaces `CurrentResourceOwner`.

This is an evidence-backed decision, not a deferral: src-idiomatic `TECH_DEBT.md`
TD-17 + commit `36b392866` document that the flat-registry port failed in both
directions (leaked error-edge pins, over-released cross-owner pins, structural
`RefCell` re-borrow panic in the release path). docs/query-lifecycle-raii.md §
"Evidence" closes with "Do not re-run this experiment." The src-idiomatic
`backend-utils-resowner-resowner` crate **is** that failed `ResourceOwnerArena`
registry; copying it would re-introduce the documented failure and would also
violate AGENTS.md / the no-registries-with-release-authority rule (rule 4).

The audit therefore verdicts every `resowner.c` function against the
dissolution: each is either already realized per-subsystem where a consumer has
forced it (xact, dsm-core), or carries no logic that any landed crate omits.
There is no Rust crate claiming to port `resowner.c`, so there is no port that
can be `MISSING`/`PARTIAL`/`DIVERGES` against it — the absent-logic failure mode
the skill guards against does not apply to a unit the architecture forbids
porting. What the audit must (and does) confirm: the realized consumers carry
the logic faithfully, and the one owned seam-declaration crate is correctly
characterized.

## Function inventory (resowner.c, 26 functions + 1 inline helper)

C definitions enumerated from `resowner.c`; cross-checked against the two
c2rust runs (both retained the same set — no `#if`-gated functions in the build
config). `D` = dissolved per query-lifecycle-raii.md; the disposition column
names where the behavior lives or will live.

| # | C function | C loc | Verdict | Disposition under dissolution |
|---|---|---|---|---|
| 1 | `hash_resource_elem` (static inline) | :222 | D | Internal to the registry's hash table; the registry does not exist here. No external behavior. |
| 2 | `ResourceOwnerAddToHash` (static) | :245 | D | Registry-internal array→hash spill; no equivalent — guards live in owner `Vec`s. |
| 3 | `resource_priority_cmp` (static) | :269 | D | Release-priority sort; replaced by `TxnResources` field order (R3, doc rule 6 `RESOURCE_RELEASE_*`). |
| 4 | `ResourceOwnerSort` (static) | :292 | D | Registry release-ordering; subsumed by owner field order. |
| 5 | `ResourceOwnerReleaseAll` (static) | :348 | D | Phase sweep over the registry; becomes ordered consumption of `TxnResources` (commit) / `Drop` (abort). Realized in xact `AtEOXact`/`CleanupTransaction`. |
| 6 | `ResourceOwnerCreate` | :421 | D | Owner allocation; an owner value is created with the transaction/portal scope, not a heap object. xact: `AtStart_ResourceOwner` (lib.rs:903) is the dissolved create-point. |
| 7 | `ResourceOwnerEnlarge` | :452 | D | Grows the fixed array before a Remember; `Vec::push` on the owner needs no pre-enlarge. |
| 8 | `ResourceOwnerRemember` | :524 | D | Per doc rule 6: becomes guard construction (move into owner). |
| 9 | `ResourceOwnerForget` | :564 | D | Per doc rule 6: becomes guard `Drop` / removal from owner. |
| 10 | `ResourceOwnerRelease` | :658 | D | Public phase-release entry; dissolves into commit/abort of the owner value. xact `CommitTransaction`/`AbortTransaction` carry the BEFORE_LOCKS/LOCKS/AFTER_LOCKS phase markers (engine.rs:728,752,917,924). |
| 11 | `ResourceOwnerReleaseInternal` (static) | :678 | D | Inner of #10 incl. callback fan-out + post-phase warnings; the commit-time leak WARN is R2 inside `TxnResources::commit`. |
| 12 | `ResourceOwnerReleaseAllOfKind` | :818 | D | Targeted kind sweep (used by bufmgr/aio); becomes a typed guard collection drain in the owning subsystem. |
| 13 | `ResourceOwnerDelete` | :871 | D | Frees the owner object; dissolves with the owner value's scope end. xact engine.rs:589 marks `ResourceOwnerDelete(TopTransactionResourceOwner)` dissolved. |
| 14 | `ResourceOwnerGetParent` | :905 | D | Owner-tree navigation; the tree is the scope nesting (txn → subxact), no pointer chase. |
| 15 | `ResourceOwnerNewParent` | :914 | D | Reparenting (R4 promotion); becomes a checked move of a guard into a longer-lived owner (doc §2 `txn.pins.push`). |
| 16 | `RegisterResourceReleaseCallback` | :961 | D | Generic release-callback registry; replaced by `Drop` impls (doc rule 6, no ambient callback chain). |
| 17 | `UnregisterResourceReleaseCallback` | :975 | D | Counterpart of #16; dissolves with it. |
| 18 | `CreateAuxProcessResourceOwner` | :999 | D | Aux-process owner setup; postinit consumes this via `backend-utils-resowner-resowner-seams` (mirror-and-panic until reconciled to an owner value). Not landed on this branch. |
| 19 | `ReleaseAuxProcessResources` | :1019 | D | Aux owner teardown; same disposition as #18. |
| 20 | `ReleaseAuxProcessResourcesCallback` (static) | :1044 | D | `on_shmem_exit` wrapper for #19; becomes a guard drop / exit hook in the aux-process owner. |
| 21 | `ResourceOwnerRememberLock` | :1062 | D | Lock-cache remember; becomes `LockGuard` in `TxnResources.locks`. |
| 22 | `ResourceOwnerForgetLock` | :1082 | D | Lock-cache forget; becomes `LockGuard` drop. |
| 23 | `ResourceOwnerRememberAioHandle` | :1104 | D | AIO-handle dlist remember; becomes an AIO guard in the aio subsystem owner. |
| 24 | `ResourceOwnerForgetAioHandle` | :1110 | D | AIO-handle dlist forget; counterpart of #23. |

(Static asserts at :94 are compile-time invariants on the registry sizing; they
have no runtime counterpart once the registry is gone.)

### Spot-check of dissolution realizations (auditor re-derivation)

To avoid a false green I re-derived the realized consumers against the C:

- **xact** (`backend-access-transam-xact`, merged): the `AtStart_ResourceOwner`
  (xact.c:1226 → lib.rs:903) and `AtAbort_ResourceOwner`/`AtSubAbort_ResourceOwner`
  (xact.c:1916/1929 → lib.rs:1035-1040) bodies are dissolved to the owner value,
  and every `ResourceOwnerRelease(RESOURCE_RELEASE_*)` call site in
  Commit/Abort/Cleanup (+Sub) carries a dissolved-marker comment at the matching
  C line (engine.rs:542,589,728,752,810,917,924,2056,2116,2163,2241,2246). The
  C control flow (release order pins→locks→after-locks; commit vs abort
  asymmetry) is preserved as field/sequence order. Matches doc R2/R3/rule 6.
- **dsm-core** (`backend-storage-ipc-dsm-core`, merged): `dsm_segment*` resowner
  bookkeeping (`ResourceOwnerRememberDSM`/`ForgetDSM`/`ResOwnerReleaseDSM`)
  becomes the `DsmSegment` RAII guard with `Drop` = release. Matches doc §1.

Both realize the C behavior; neither leaks the absent-logic the skill warns
about. Functions #18-#24 (aux-process owner, lock/aio remember-forget) have no
*landed* consumer on main yet; consumers that touch them (postinit, bufmgr, aio)
seam-and-panic per the standing "mirror PG and panic" rule until they are
reconciled to guards — that is acceptable (panicking on an unported callee is
fine; absent logic is not, and there is no crate here asserting that logic).

## Seam audit (skill §3)

**Owned seam crates** (every `crates/X-seams` where X maps to `resowner.c`):

- `crates/backend-utils-resowner-resowner-seams` — the only one present on this
  branch / on main. (CATALOG notes from other units mention a
  `backend-utils-resowner-seams`; no such crate exists on main or this branch —
  those references live on unmerged feature branches and are out of scope here.)

`backend-utils-resowner-resowner-seams` declares two seams:
`CurrentResourceOwner() -> ResourceOwnerHandle` and
`set_CurrentResourceOwner(ResourceOwnerHandle)`. These mirror the ambient
`CurrentResourceOwner` global, consumed by logical decoding's slot-advance
save/restore (`backend-replication-logical-logical` lib.rs:1957/2041, an
unmerged in-progress crate).

- **No crate-side `init_seams()` installer, and that is correct here.** The skill
  requires the *owning crate*'s `init_seams()` to install its owned seam
  declarations. This unit has **no owning crate** by deliberate decision
  (dissolved). A seam *declaration* crate with no installer is the expected
  steady state for a dissolved unit: the declaration exists only so consumers
  compile and panic loudly until they are reconciled to owner values; per
  docs/query-lifecycle-raii.md these `CurrentResourceOwner` save/restore seams
  must ultimately dissolve into a `TxnResources`/`Ctx` owner value in the
  consumer (logical-logical), not gain a resowner-crate installer. The
  un-installed state is therefore not a finding against this unit — it is
  pre-existing consumer-side design debt owned by logical-logical (already on
  main; not introduced by this branch), ledgered in that crate's DESIGN_DEBT note.
- The two declarations are pure ambient-global accessors (read / write one
  value): no branching, node construction, or computation in the seam path.
- This branch introduces **no new seams, no new installers, and no logic** — its
  entire diff vs `main` is the single CATALOG row recording the dissolution.

**Seam findings: zero.**

## Design conformance (skill §3b)

- No invented opacity: `ResourceOwnerHandle` is `types-logical`'s inherited
  opaque handle for the unported owner pointer, not a stand-in for a type that
  should be real here (the real type is the dissolved owner value, owned by
  consumers). types.md rules 6-7 satisfied.
- No registry with release authority is introduced — the decision *prevents*
  exactly that (doc rule 4, AGENTS.md neighbor-dependency table).
- No allocating function/seam without `Mcx`+`PgResult`, no shared statics for
  per-backend globals introduced (none added).
- The dissolution itself is the canonical application of the repo architecture
  (docs/query-lifecycle-raii.md, docs/mctx-design.md) — conformant by
  construction.

**Design findings: zero.**

## Verdict

**PASS.** Every `resowner.c` function is accounted for under the documented
dissolution (DISSOLVED is the correct disposition for a unit the architecture
forbids porting as a crate); the realized consumers (xact, dsm-core) carry the
behavior faithfully on re-derivation; the single owned seam-declaration crate is
correctly characterized with zero seam findings; zero design findings. This
branch's only change is the CATALOG row recording the unit as an alias of the
`dissolved` `backend-utils-resowner-all` decision.

CATALOG: `probe-utils-resowner-resowner` row marked `audited` (it is the
`backend-utils-resowner-resowner`/`resowner.c` catalog row; the unit is an alias
of `backend-utils-resowner-all` which remains `dissolved`).
