# Seam Registry Summary

Snapshot of the seam registry in `SEAM_OWNERS.tsv` (14 columns: the 8 owner-resolution columns plus `impl_crate`, `impl_exists`, `mishomed`, `mishome_kind`, `contract_divergence`, `block_class`). **7058** seam declarations total (4806 installed, 2252 uninstalled).

Regenerate with `tools/recount_seams.py` (rescans the tree for installs / consumers and recomputes `installed`/`n_consumers`/`block_class`; carries the static columns).

## block_class

One mutually-exclusive reason per seam, assigned in priority order: `installed` > `external_provider` > `contract_divergence` > `unresolved_owner` > `mishomed_to_ported` > `mishomed_to_unported` > `unported_owner` > `wiring_debt` > `no_consumer`.

| block_class | count |
|---|---|
| installed | 4806 |
| external_provider | 117 |
| contract_divergence | 95 |
| unresolved_owner | 593 |
| mishomed_to_ported | 1 |
| unported_owner | 223 |
| wiring_debt | 1203 |
| no_consumer | 20 |
| **total** | 7058 |

## mishome_kind

`true_cross` = declaring unit and owner/installer unit do NOT share >=3 leading dash-segments (real cross-subsystem move). `sibling_split` = share >=3 (sanctioned file-split). `none` = correctly homed.

| mishome_kind | count |
|---|---|
| true_cross | 672 |
| sibling_split | 411 |
| none | 5975 |
| **total** | 7058 |

## contract_divergence

Whether the seam signature can be satisfied by the owner's real body without re-signing (handle/carrier/model mismatch).

**Detection rule (documented; judgement involved):**
- `yes` (strong): the seam fn is keyword-flagged in seams-init `CONTRACT_RECONCILE_PENDING` (comment mentions handle/carrier/divergence/opaque/token/keystone); OR the seam crate is a `-pc-seams` plancache-keystone family; OR the signature carries a known divergent-keystone type (RawStmt/CachedPlan*/PlannedStmtHandle/QueryListHandle/ReorderBuffer*Handle/ChangeHandle/CopyParseState/SnapBuildHandle); OR copyfrom/CopyParseState family.
- `maybe` (weak): the signature carries some other `*Handle`/`*Token` type or `&[u8]` for a structured C type — these are often FAITHFUL opaque-pointer carriers (BackgroundWorkerHandle, DsmSegmentHandle, MemoryContextHandle, LatchHandle, ...), so the divergence is unconfirmed.
- `no`: no handle/token/bytes signal.

| contract_divergence | count |
|---|---|
| yes | 199 |
| maybe | 618 |
| no | 6241 |
| **total** | 7058 |

**Caveat:** `maybe` is deliberately broad and over-includes faithful opaque-pointer carriers; treat it as an upper bound on weak signals, not a divergence count. Only `yes` is a confident divergence claim, and even it leans on the curated `CONTRACT_RECONCILE_PENDING` list plus the documented keystone type set — seams that diverge but use a bespoke carrier name not in that set are missed. The strong/weak split, not the absolute counts, is the reliable signal.

## Top 25 impl_crates by seam count

The crate that actually implements (installs, or owns the ported body of) the seam. 6081 seams have an implementer; 977 have none yet.

| impl_crate | seams |
|---|---|
| backend-utils-cache-syscache | 227 |
| backend-commands-vacuum | 170 |
| backend-storage-lmgr-proc | 119 |
| backend-utils-cache-lsyscache | 112 |
| backend-utils-cache-relcache | 92 |
| backend-catalog-indexing | 85 |
| backend-optimizer-util-pathnode | 84 |
| backend-replication-slot | 84 |
| backend-storage-buffer-bufmgr | 83 |
| backend-access-heap-vacuumlazy | 78 |
| backend-utils-init-miscinit | 77 |
| backend-optimizer-path-costsize | 75 |
| backend-commands-tablecmds | 72 |
| backend-storage-file-fd | 71 |
| backend-access-transam-xlog | 69 |
| backend-executor-nodeModifyTable | 67 |
| backend-nodes-core | 63 |
| backend-access-transam-xlogreader | 58 |
| backend-access-transam-xact | 56 |
| backend-foreign-foreign | 55 |
| backend-executor-execExpr | 55 |
| backend-executor-execTuples | 54 |
| backend-access-nbtree-core | 53 |
| backend-commands-functioncmds | 50 |
| backend-replication-libpqwalreceiver | 48 |

## Top 30 mis-home flows (true_cross)

declaring unit -> owner/installer unit, for seams whose two units do not share >=3 leading dash-segments.

| declaring_unit | owner/impl_unit | count |
|---|---|---|
| interfaces-libpq-fe | backend-replication-libpqwalreceiver | 48 |
| backend-optimizer-util-pathnode | backend-optimizer-path-costsize | 33 |
| backend-postmaster-postmaster | backend-postmaster-bgworker | 19 |
| backend-nodes-nodeFuncs | backend-nodes-core | 15 |
| backend-access-heap-vacuumlazy | backend-commands-vacuum | 13 |
| backend-optimizer-path-joinpath | backend-optimizer-util-pathnode | 13 |
| backend-parser-parse-agg | backend-parser-agg | 12 |
| backend-storage-lmgr-proc | backend-access-transam-xlog | 9 |
| backend-optimizer-util-plancat-ext | backend-utils-cache-relcache | 9 |
| backend-parser-parse-func | backend-parser-func | 8 |
| backend-optimizer-util-relnode | backend-optimizer-path-joinpath | 8 |
| backend-utils-sort-storage | backend-executor-nodeMaterial | 7 |
| backend-access-heap-vacuumlazy | backend-commands-vacuumparallel | 6 |
| backend-utils-mmgr-dsa | backend-storage-ipc-dsm-registry | 5 |
| backend-access-table-tableam | backend-access-heap-heapam-handler-core | 5 |
| backend-port-path | backend-utils-init-miscinit | 5 |
| common-cryptohash | backend-libpq-auth-scram | 5 |
| backend-port-pg-sema | backend-port-sysv-sema | 5 |
| backend-storage-smgr | backend-storage-buffer-support | 5 |
| backend-optimizer-path-equivclass-ext | backend-optimizer-plan-init-subselect | 5 |
| port-dynloader | backend-utils-fmgr-dfmgr | 5 |
| backend-utils-resowner | backend-utils-mmgr-portalmem | 4 |
| backend-statistics-core | backend-statistics-mcv | 4 |
| backend-access-spg-xlog | backend-access-spgist-core | 4 |
| backend-access-rmgrdesc-heapdesc | backend-rmgrdesc-next | 4 |
| backend-access-index-amvalidate | backend-access-brin-validate | 4 |
| backend-utils-init-small | backend-storage-aio-methods | 4 |
| backend-utils-init-small | backend-storage-ipc-dsm-core | 4 |
| backend-utils-adt-pg-locale-catalog | backend-utils-cache-syscache | 4 |
| backend-postmaster-postmaster | backend-libpq-hba | 4 |

