# Audit: backend-utils-cache-relcache (assembly #114)

- **Date:** 2026-06-13 (assembly of decomp/relcache-fill-{0..4})
- **Model:** Claude Fable 5 / Opus 4.8 (1M context)
- **C sources:** `src/backend/utils/cache/relcache.c` (+ headers `relcache.h`,
  `rel.h`, `pg_publication.h`)
- **c2rust:** `../pgrust/c2rust-runs/backend-utils-cache-relcache/src/relcache.rs`
- **Port:** `crates/backend-utils-cache-relcache/`
- **Verdict:** **PASS** (the 2 prior `initfile.rs` `xunit` findings are fixed
  and re-audited from scratch, independently)

Scope: `decomp/relcache-assembled` = `main` (8451c36f) + family-fill branches
`decomp/relcache-fill-{0,1,2,3,4}`. The crate was previously merged
(CATALOG=merged), but its `initfile` xunit boundary still carried `todo!()`
placeholders on `main` — verified: `main:src/initfile.rs` is byte-identical to
the fill octopus base (1484 lines, `todo!()` at the xunit shims). The fill
branches complete those boundaries; this audit re-derives the *filled* bodies
from the C under the strict no-deferred rule (a body is real own-logic OR a real
seam `::call` to a genuinely-unported owner; a `todo!()`/collapsed-own-function
deferral = FAIL).

## Assembly mechanics (clean)

- Branched off live `refs/heads/main` (8451c36f). Merged fill-0..4.
- Conflicts (additive) resolved:
  - relmapper `init_seams()` union (fill-1): kept both `set()` blocks.
  - relcache `Cargo.toml` dep blocks: additive union; de-duped
    `backend-storage-smgr-seams` (fill-0/fill-2) and
    `backend-access-index-genam-seams` (fill-3/fill-4, E0428 risk avoided).
  - `Cargo.lock` regenerated (cargo metadata clean).
- `grep todo!()/unimplemented!()` in the crate: **0** (3 hits are doc prose).
- No leftover conflict markers.
- `cargo check --workspace`: clean (warnings only).
- `cargo test -p backend-utils-cache-relcache`: ok.
- `cargo test -p seams-init`: both recurrence guards pass.
- `relcache::init_seams()` (src/seams.rs:21) wired into `seams-init::init_all`
  (line 159); contains only `set()` calls.

## Filled-boundary verdicts

### fill-0 — invalidate.rs xunit (6 shims): MATCH/SEAMED
All six former `todo!()` are real seam `::call`s.
`relation_map_invalidate_all` composes the relmapper per-map seam
`call(true)`+`call(false)` — faithful to C `RelationMapInvalidate(true/false)`.
smgr shims → smgr-seams (smgr unported → panic on unported callee, sanctioned).
No own-logic elided.

### fill-1 — initfile.rs xunit: MATCH/SEAMED (the 2 prior findings are FIXED)
Most shims become real seam `::call`s (relmapper init phases/update, miscinit
bootstrap/proc-pid/database-path, catalog is_shared/is_catalog_namespace,
syscache supports/phase2/pg_class-form, xact sub-xid, parallel-rt + init-small
proc-number, lmgr lock/unlock with kept guard, lwlock RelCacheInitLock dance,
inval AcceptInvalidationMessages, fd file API). `SearchSysCacheRelOid` marshals
the owner's pg_class form into entry-owned `FormPgClass` with the FATAL
"cache lookup failed" path. MATCH/SEAMED.

Sanctioned "Mirror PG and panic" (callee in an unported *other* unit; arg/result
crosses as relcache's own opaque entry type, so no cross-crate seam possible):
- `catalog_schema_attrs` → genbki `Schema_pg_*` bootstrap data → entry-owned
  `Vec<OwnedAttr>`. ACCEPTABLE.
- `RelationBuildTriggers` → trigger.c (unported). ACCEPTABLE.
- `RelationBuildRowSecurity` → policy.c (unported). ACCEPTABLE.

**FINDING 1 (FIXED) — `RelationGetIndexAttOptions` (relcache.c:5988) — MATCH.**
The collapsed `xunit` panic is removed; `load_critical_index` (initfile.rs)
now calls the real in-crate `crate::derived::RelationGetIndexAttOptions(ird,
false)` (derived.rs:861). Independently re-derived: the cached short-circuit
(`rd_opcoptions.is_some()` — the owned-entry analog of
`if (opts) return copy ? CopyIndexAttOptions : opts`; the `copy` flag /
`CopyIndexAttOptions` palloc-`datumCopy` duplication is subsumed by Rust
ownership, behavior-preserving), the `palloc0`→`vec![None; natts]` loop over
`natts = relnatts`, the
`criticalRelcachesBuilt && relid != AttributeRelidNumIndexId` guard
(`ATTRIBUTE_RELID_NUM_INDEX_ID = 2659`, verified vs pg_attribute.h), the
`get_attoptions(relid, i+1)` + `index_opclass_options(relation, i+1,
attoptions, false)` leaf calls (already-installed `nodexform_seam` seams whose
lsyscache/indexam owners install them), and the inline `rd_opcoptions` cache
store (C's `rd_indexcxt` copy) all match. The per-element `pfree`/dual-return
cleanup collapses cleanly under ownership. **MATCH.**

**FINDING 2 (FIXED) — `RelationSetNewRelfilenumber` (relcache.c:3771) —
MATCH.** The `set_new_relfilenumber_storage` collapsed panic is removed; the
body is now real relcache-OWN control flow with only genuinely-unported leaves
seamed. Independently re-derived against C 3771–3984:
- relfilenumber selection: `!IsBinaryUpgrade` → `get_new_relfilenumber` seam;
  `RELKIND_INDEX`/`RELKIND_RELATION` → `consume_next_relfilenumber(true/false)`
  with the `InvalidOid`→ereport guard; else → ereport. All 4 ereports carry
  `ERRCODE_INVALID_PARAMETER_VALUE` with the exact C messages. REAL flow.
- old-storage drop: `IsBinaryUpgrade` → `smgr_unlink_relation_now`
  (smgropen/smgrdounlinkall/smgrclose) else `relation_drop_storage`, both
  passing `(rlocator, backend)` explicitly (relcache owns the entry).
- `newrlocator = rd_locator` with `relNumber = newrelfilenumber`.
- RELKIND dispatch: `RELKIND_HAS_TABLE_AM` → `table_relation_set_new_filelocator`
  (returns `(freezeXid, minmulti)`); else `RELKIND_HAS_STORAGE` →
  `relation_create_storage_main_fork`; else `elog ERROR "does not have
  storage"`. Membership sets verified vs pg_class.h (HAS_TABLE_AM =
  RELATION|TOASTVALUE|MATVIEW; HAS_STORAGE = RELATION|INDEX|SEQUENCE|TOASTVALUE
  |MATVIEW, SEQUENCE='S'). REAL flow.
- `RelationIsMapped` branch (`relkind_has_storage && relfilenode ==
  InvalidRelFileNumber`): mapped → `GetCurrentTransactionId` (force XID) +
  `RelationMapUpdateMap(relid, newnum, relisshared, false)` +
  `cache_invalidate_relcache(relid)`; non-mapped → `update_pg_class_relfilenumber`
  seam carrying `(relid, newnum, persistence, relkind, freezeXid, minmulti)`
  (the pg_class tuple open/lock/copy/mutate/CatalogTupleUpdate/unlock/close is
  the catalog owner's job; the relkind lets it apply the SEQUENCE relpages-etc.
  exception). REAL branch decision.
- tail: `CommandCounterIncrement` seam, then in-crate
  `RelationAssumeNewRelfilelocator` (subid tracking + `EOXactListAdd`). MATCH.
- Behavior-preserving drops: the mapped-branch debug `Assert()`s (and the
  pg_class read that fed only those asserts) are no-ops in release. **MATCH.**

New leaf seams introduced (all marshal+delegate, correctly homed):
`get_new_relfilenumber` (catalog-catalog-seams — INSTALLED in
`backend-catalog-catalog::init_seams` via a marshal-only adapter over the real
`GetNewRelFileNumber`); `consume_next_relfilenumber` (binary-upgrade-seams);
`relation_drop_storage` / `smgr_unlink_relation_now` /
`relation_create_storage_main_fork` / `update_pg_class_relfilenumber`
(catalog-storage-seams, storage.c unported → seam-and-panic);
`cache_invalidate_relcache` (inval-seams, inval.c unported → seam-and-panic);
`table_relation_set_new_filelocator` (tableam-seams — the dispatch crate is
complete but the `TableAmRoutine` slot + heapam_handler AM body are unported,
so it is tracked as `CONTRACT_RECONCILE_PENDING` in seams-init + a DESIGN_DEBT
entry, not force-wired).

### fill-2 — core_entry_store.rs + seams.rs: SEAMED
Index-family scalar reads + amapi/fmgr/ruleutils/smgr seam `::call`s. No
whole-function panics; no own-logic elided in the delta.

### fill-3 — derived.rs + core_entry_store/entry.rs: SEAMED
genam catalog-scan seams + nodexform node-tree-transform seams (new outward
crate `backend-utils-cache-relcache-nodexform-seams`, owner unported → panic on
unported callee, sanctioned). No own-logic elided.

### fill-4 — build.rs: SEAMED
pg_attrdef/pg_constraint genam scan seams. No own-logic elided.

## Seam / wiring audit (clean)
- `init_seams()` only `set()`s; wired into `init_all`.
- New `*-nodexform-seams` (8 decls) is a consumer-side outward seam; owner not
  yet ported, stays uninstalled (seam-and-panic). Recurrence guard
  `every_declared_seam_is_installed_by_its_owner` correctly does not flag it.
- New leaf seams for `RelationSetNewRelfilenumber` (see Finding 2 above):
  `get_new_relfilenumber` is INSTALLED by its complete owner (catalog.c);
  `table_relation_set_new_filelocator` is tracked as `CONTRACT_RECONCILE_PENDING`
  + DESIGN_DEBT (its complete dispatch owner cannot install it until the
  vtable slot + heapam_handler AM body land); the rest seam-and-panic into
  genuinely-unported owners (storage.c, inval.c, binary_upgrade.h).
- No `set()` outside owner; no computation in any new seam path beyond
  marshal+delegate.

## Cleanliness
- `grep todo!()/unimplemented!()` in `crates/.../src`: **0** in code (2 hits are
  doc prose).
- `panic!()` in the crate: exactly **3**, all sanctioned mirror-and-panic into
  genuinely-unported owners — `catalog_schema_attrs` (genbki catalog data),
  `RelationBuildTriggers` (trigger.c), `RelationBuildRowSecurity` (policy.c).

## Verdict: PASS

The two prior FAIL findings (`RelationGetIndexAttOptions`,
`RelationSetNewRelfilenumber`) are fixed: both are now implemented as real
relcache-OWN control flow with only their genuinely-unported cross-unit *leaves*
seamed, and both were independently re-derived from the C from scratch and
verdict **MATCH** (see Finding 1/2 above). All new seam declarations are
correctly homed and marshal-only; one (`get_new_relfilenumber`) is installed by
its complete owner, one (`table_relation_set_new_filelocator`) is ledgered as
tracked debt, and the rest are sanctioned seam-and-panic. The recurrence guards
(`every_declared_seam_is_installed_by_its_owner`,
`every_seam_installing_crate_is_wired_into_init_all`) pass, as do
`cargo check --workspace`, `cargo test -p backend-utils-cache-relcache`, and
`cargo test -p seams-init`. The crate is merge-ready.
