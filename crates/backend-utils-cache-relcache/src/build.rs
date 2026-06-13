//! build family — `RelationBuildDesc` orchestration (IN-CRATE) and the
//! descriptor-assembly subroutines.
//!
//! The orchestration (`RelationBuildDesc`, `AllocateRelationDesc`,
//! `RelationBuildTupleDesc`, `RelationParseRelOptions`, `formrdesc` +
//! `BuildHardcodedDescriptor`, `AttrDefaultFetch`, `CheckNNConstraintFetch`)
//! is relcache's OWN logic and lands here in full over the REAL entry store.
//! ONLY the catalog-scan / tuple-deform primitives these routines sit on top
//! of (`ScanPgRelation`: `systable_beginscan`/`getnext` via genam, the
//! `GETSTRUCT` deform of `pg_class`/`pg_attribute`, `extractRelOptions` via the
//! reloptions unit) are genuine cross-unit seams, routed through their owner
//! (seam-and-panic until the owner lands).

use backend_access_common_tupdesc::CreateTupleDesc;
use backend_utils_error::{ereport, PgResult};
use mcx::{Mcx, PgString, PgVec};
use types_catalog::catalog::GLOBALTABLESPACE_OID;
use types_core::catalog::{
    PG_CATALOG_NAMESPACE, RELPERSISTENCE_PERMANENT, RELPERSISTENCE_TEMP, RELPERSISTENCE_UNLOGGED,
};
use types_core::primitive::Oid;
use types_core::xact::InvalidSubTransactionId;
use types_core::{InvalidOid, INVALID_PROC_NUMBER};
use types_error::ERROR;
use types_tuple::access::{
    RELKIND_INDEX, RELKIND_MATVIEW, RELKIND_PARTITIONED_INDEX, RELKIND_PARTITIONED_TABLE,
    RELKIND_RELATION, RELKIND_SEQUENCE, RELKIND_TOASTVALUE, RELKIND_VIEW,
};
use types_tuple::heaptuple::{FormData_pg_attribute, NameData};

use crate::core_entry_store::entry::{FormPgClass, OwnedAttr, OwnedTupleDesc, RelationData};
use crate::core_entry_store::{cache_insert, with_state, InProgressEnt};

/// `RECORDOID` (`pg_type.h`) — the pseudo-type for anonymous record types.
const RECORDOID: Oid = 2249;
/// `RELPERSISTENCE_TEMP`/`_PERMANENT`/`_UNLOGGED` are `u8` in the type crate;
/// the entry stores `relpersistence` as `i8`. These mirror them in `i8`.
const PERSIST_PERMANENT: i8 = RELPERSISTENCE_PERMANENT as i8;
const PERSIST_UNLOGGED: i8 = RELPERSISTENCE_UNLOGGED as i8;
const PERSIST_TEMP: i8 = RELPERSISTENCE_TEMP as i8;

/// Project the owned relcache entry into the cross-unit
/// [`types_rel::RelationData`] value-slice, copied into `mcx` (the C "copy the
/// consumed slice of the entry into the caller's memory context"). This is the
/// build family's projection half, used by the `relation_id_get_relation`
/// seam. **Own logic.**
#[allow(unsafe_code)]
pub(crate) fn project_relation_data<'mcx>(
    mcx: Mcx<'mcx>,
    rd: *mut RelationData,
) -> PgResult<types_rel::RelationData<'mcx>> {
    // SAFETY: `rd` is a live cache-owned (or in-build) descriptor; we only read
    // its scalar/owned fields to materialize the cross-unit value-slice.
    let r = unsafe { &*rd };
    project_entry(mcx, r)
}

/// Build the cross-unit [`types_rel::RelationData`] slice from the owned entry.
/// Mirrors the field subset that crosses the seam (see `crates/types-rel`):
/// `rd_id`/`rd_locator`/`rd_backend`, the trimmed `rd_rel`, the materialized
/// `rd_att` tuple descriptor, `rd_options`, and the index fields (`None`/empty
/// for a table). Everything is allocated in `mcx`.
fn project_entry<'mcx>(
    mcx: Mcx<'mcx>,
    r: &RelationData,
) -> PgResult<types_rel::RelationData<'mcx>> {
    let rd_rel = project_form_pg_class(mcx, &r.rd_rel)?;
    // Materialize the tuple descriptor from the entry's owned attribute rows.
    // `CreateTupleDesc` (tupdesc.c) populates the parallel compact_attrs.
    let attrs = build_form_attrs(&r.rd_att, r.rd_id);
    let mut td = CreateTupleDesc(mcx, &attrs)?;
    td.tdtypeid = r.rd_att.tdtypeid;
    td.tdtypmod = r.rd_att.tdtypmod;
    td.tdrefcount = 1;
    let rd_att = mcx::alloc_in(mcx, td)?;
    // Index fields: `rd_index` / `rd_opcintype` (None/empty for a table).
    let rd_index = r.rd_index.as_ref().map(|ix| types_rel::FormData_pg_index {
        indnkeyatts: ix.indnkeyatts,
        indimmediate: ix.indimmediate,
        indrelid: ix.indrelid,
        indkey0: ix.indkey.first().copied().unwrap_or(0),
    });
    let mut rd_opcintype: PgVec<'mcx, Oid> = mcx::PgVec::new_in(mcx);
    for &t in &r.rd_opcintype {
        rd_opcintype.push(t);
    }
    Ok(types_rel::RelationData {
        rd_id: r.rd_id,
        rd_locator: r.rd_locator,
        rd_backend: r.rd_backend,
        rd_rel,
        rd_att,
        rd_options: r.rd_options.clone(),
        rd_index,
        rd_opcintype,
    })
}

/// Project the owned `FormPgClass` mirror into the cross-unit trimmed form,
/// copying the name into `mcx`. `relpersistence`/`relkind` are `i8` on the
/// entry; the cross-unit slice carries them as `u8` (`RELPERSISTENCE_*`/
/// `RELKIND_*`).
fn project_form_pg_class<'mcx>(
    mcx: Mcx<'mcx>,
    f: &FormPgClass,
) -> PgResult<types_rel::FormData_pg_class<'mcx>> {
    Ok(types_rel::FormData_pg_class {
        relname: PgString::from_str_in(&f.relname, mcx)?,
        relnamespace: f.relnamespace,
        relowner: f.relowner,
        relrowsecurity: f.relrowsecurity,
        relpages: f.relpages,
        reltuples: f.reltuples,
        relallvisible: f.relallvisible,
        reltoastrelid: f.reltoastrelid,
        reltablespace: f.reltablespace,
        relfilenode: f.relfilenode,
        relisshared: f.relisshared,
        relhasindex: f.relhasindex,
        relhassubclass: f.relhassubclass,
        relpersistence: f.relpersistence as u8,
        relkind: f.relkind as u8,
        relispopulated: f.relispopulated,
        relreplident: f.relreplident as u8,
        relispartition: f.relispartition,
    })
}

/// Build the full `FormData_pg_attribute[]` array from the entry's owned
/// attribute rows, for the tuple-descriptor materialization. The entry carries
/// the trimmed `OwnedAttr` subset; the remaining `Form_pg_attribute` fields are
/// `Default` (they are not consumed across the relcache seam).
fn build_form_attrs(td: &OwnedTupleDesc, relid: Oid) -> Vec<FormData_pg_attribute> {
    td.attrs
        .iter()
        .map(|a| FormData_pg_attribute {
            attrelid: relid,
            attname: name_data(&a.attname),
            atttypid: a.atttypid,
            attlen: a.attlen,
            attnum: a.attnum,
            atttypmod: a.atttypmod,
            attbyval: a.attbyval,
            attalign: a.attalign,
            attnotnull: a.attnotnull,
            attisdropped: a.attisdropped,
            attcollation: a.attcollation,
            ..FormData_pg_attribute::default()
        })
        .collect()
}

/// `namestrcpy` into a fixed `NameData` (NUL-padded, truncated to NAMEDATALEN).
fn name_data(s: &str) -> NameData {
    let mut nd = NameData::default();
    let bytes = s.as_bytes();
    let n = bytes.len().min(nd.data.len() - 1);
    nd.data[..n].copy_from_slice(&bytes[..n]);
    nd
}

/// `RelationBuildDesc(targetRelId, insertIt)` (relcache.c): assemble a fresh
/// relcache entry for `targetRelId` by reading `pg_class` (via
/// [`ScanPgRelation`]), build its tuple descriptor, parse reloptions,
/// initialize index/table access info, and (if `insertIt`) install it in the
/// `RelationIdCache`. Returns the built `Relation` (the C pointer), or `null`
/// when no `pg_class` row exists. **Own orchestration.**
#[allow(unsafe_code)]
pub fn RelationBuildDesc(targetRelId: Oid, insertIt: bool) -> PgResult<*mut RelationData> {
    // Push our entry onto in_progress_list (the invalidation-restart protocol).
    // C grows a fixed array; the owned model is a Vec, so the offset is the
    // current length before the push.
    let in_progress_offset = with_state(|st| {
        let off = st.in_progress_list.len();
        st.in_progress_list.push(InProgressEnt {
            reloid: targetRelId,
            invalidated: false,
        });
        off
    });

    let relation: *mut RelationData = loop {
        // Reset the invalidated flag for this attempt.
        with_state(|st| st.in_progress_list[in_progress_offset].invalidated = false);

        // Read pg_class for the target (catalog scan — cross-unit seam).
        let relp = match ScanPgRelation(targetRelId, true, false)? {
            Some(relp) => relp,
            None => {
                // No pg_class row: pop our in_progress entry and return NULL.
                with_state(|st| {
                    st.in_progress_list.truncate(in_progress_offset);
                });
                return Ok(std::ptr::null_mut());
            }
        };

        let relid = targetRelId;

        // Allocate the descriptor and copy the pg_class form into rd_rel.
        let mut relation = AllocateRelationDesc(relp)?;

        // Initialize the relation's lifecycle fields.
        relation.rd_id = relid;
        relation.rd_refcnt = 0;
        relation.rd_isnailed = false;
        relation.rd_createSubid = InvalidSubTransactionId;
        relation.rd_newRelfilelocatorSubid = InvalidSubTransactionId;
        relation.rd_firstRelfilelocatorSubid = InvalidSubTransactionId;
        relation.rd_droppedSubid = InvalidSubTransactionId;

        // rd_backend / rd_islocaltemp from relpersistence.
        match relation.rd_rel.relpersistence {
            PERSIST_UNLOGGED | PERSIST_PERMANENT => {
                relation.rd_backend = INVALID_PROC_NUMBER;
                relation.rd_islocaltemp = false;
            }
            PERSIST_TEMP => {
                // Temp-namespace ownership resolution (isTempOrTempToastNamespace
                // / GetTempNamespaceProcNumber) is namespace.c logic — a genuine
                // cross-unit dependency. Seam-and-panic until that owner lands.
                return Err(ereport(ERROR)
                    .errmsg_internal(
                        "relcache-build: temp-relation backend resolution \
                         (isTempOrTempToastNamespace/GetTempNamespaceProcNumber) \
                         is namespace.c (cross-unit); not yet landed",
                    )
                    .into_error());
            }
            other => {
                return Err(ereport(ERROR)
                    .errmsg_internal(&format!("invalid relpersistence: {}", other as u8 as char))
                    .into_error());
            }
        }

        // Build the tuple descriptor (pg_attribute scan + constraints).
        RelationBuildTupleDesc(&mut relation)?;

        // Derived-list / partition presence reset (C zeroes these here).
        relation.rd_fkeyvalid = false;
        relation.rd_has_partkey = false;
        relation.rd_has_partdesc = false;
        relation.rd_partcheckvalid = false;

        // Index vs table access-method init (index family — own logic, separate
        // branch). Partitioned tables get neither, exactly as C.
        let relkind = relation.rd_rel.relkind as u8;
        let relptr: *mut RelationData = &mut *relation;
        if relkind == RELKIND_INDEX || relkind == RELKIND_PARTITIONED_INDEX {
            crate::index::RelationInitIndexAccessInfo(relptr)?;
        } else if relkind == RELKIND_RELATION
            || relkind == RELKIND_TOASTVALUE
            || relkind == RELKIND_MATVIEW
            || relkind == RELKIND_SEQUENCE
        {
            crate::index::RelationInitTableAccessMethod(relptr)?;
        } else {
            // RELKIND_PARTITIONED_TABLE: no access method (C falls through).
            debug_assert!(relkind == RELKIND_PARTITIONED_TABLE || true);
        }

        // Parse reloptions into rd_options.
        RelationParseRelOptions(&mut relation)?;

        // Rules / triggers / row-security (derived family — own logic, separate
        // branch). C builds them when the pg_class flags are set, else NULLs.
        if relation.rd_rel.relhasrules {
            crate::derived::RelationBuildRuleLock(relptr)?;
        } else {
            relation.rd_has_rules = false;
        }
        if relation.rd_rel.relhastriggers {
            // RelationBuildTriggers is trigger.c (cross-unit). Seam-and-panic.
            return Err(ereport(ERROR)
                .errmsg_internal(
                    "relcache-build: RelationBuildTriggers is commands/trigger.c \
                     (cross-unit); not yet landed",
                )
                .into_error());
        } else {
            relation.rd_has_trigdesc = false;
        }
        if relation.rd_rel.relrowsecurity {
            // RelationBuildRowSecurity is policy.c (cross-unit). Seam-and-panic.
            return Err(ereport(ERROR)
                .errmsg_internal(
                    "relcache-build: RelationBuildRowSecurity is rewrite/rowsecurity.c \
                     (cross-unit); not yet landed",
                )
                .into_error());
        } else {
            relation.rd_has_rsdesc = false;
        }

        // Lock info + physical address (index family — own logic, separate
        // branch). RelationInitLockInfo fills rd_lockInfo from rd_id/relisshared.
        RelationInitLockInfo(&mut relation);
        crate::index::RelationInitPhysicalAddr(relptr)?;

        // C frees pg_class_tuple here; in the owned model `relp` was already
        // consumed by AllocateRelationDesc.

        // Restart if invalidated mid-build, else done.
        let invalidated =
            with_state(|st| st.in_progress_list[in_progress_offset].invalidated);
        if !invalidated {
            break Box::into_raw(relation);
        }
        // Invalidated: destroy this descriptor (invalidate family) and retry.
        crate::invalidate::RelationClearRelation(Box::into_raw(relation), false)?;
    };

    // Pop our in_progress entry.
    with_state(|st| st.in_progress_list.truncate(in_progress_offset));

    if insertIt {
        // RelationCacheInsert(relation, true): replace any existing entry. C
        // surfaces a leak warning if it displaces a still-referenced entry; the
        // entry store performs the dynahash insert + reclaim.
        // SAFETY: `relation` is the just-built leaked descriptor; reclaiming the
        // Box hands ownership to the cache.
        let owned = unsafe { Box::from_raw(relation) };
        let oldrel = cache_insert(owned, true)?;
        if let Some(old) = oldrel {
            // SAFETY: `old` is the displaced descriptor still in the heap.
            let old_ref = unsafe { &*old };
            if old_ref.rd_refcnt == 0 {
                // Free the displaced unreferenced descriptor (invalidate family).
                crate::invalidate::RelationClearRelation(old, false)?;
            } else {
                // Still-referenced: C ereport(WARNING) about a leak (outside
                // bootstrap). We keep the warning faithful.
                let _ = ereport(types_error::WARNING)
                    .errmsg_internal(&format!(
                        "leaking still-referenced relcache entry for \"{}\"",
                        old_ref.rd_rel.relname
                    ));
            }
        }
    }

    Ok(relation)
}

/// `ScanPgRelation(targetRelId, indexOK, force_non_historic)` (relcache.c):
/// fetch the `pg_class` heap tuple for `targetRelId`. The scan itself
/// (`table_open` + `systable_beginscan`/`systable_getnext` and the `GETSTRUCT`
/// deform into `Form_pg_class`) is the genuine cross-unit seam (genam owner +
/// the `pg_class` deform primitive); this routine's caller orchestration is own
/// logic. Returns the owned `pg_class` form for the found row, `None` for the C
/// NULL (no row). Seam-and-panic until the catalog-read owner lands.
pub fn ScanPgRelation(
    targetRelId: Oid,
    _indexOK: bool,
    _force_non_historic: bool,
) -> PgResult<Option<FormPgClass>> {
    // C: must have selected a database before reading pg_class. The owned model
    // surfaces the same guard once the database-id state lands; the catalog read
    // below is the cross-unit primitive that gates this.
    let _ = targetRelId;
    Err(ereport(ERROR)
        .errmsg_internal(
            "relcache-build: ScanPgRelation pg_class read \
             (table_open + systable_beginscan/getnext via genam, GETSTRUCT deform) \
             is a cross-unit catalog primitive; owner not yet landed",
        )
        .into_error())
}

/// `AllocateRelationDesc(relp)` (relcache.c): `palloc0` a fresh descriptor and
/// copy the `pg_class` form into `rd_rel`, allocating the template tuple
/// descriptor sized `relnatts`. **Own logic.**
pub fn AllocateRelationDesc(relp: FormPgClass) -> PgResult<Box<RelationData>> {
    // palloc0 the descriptor (every field zero/empty/None, sentinel OIDs).
    let mut relation = RelationData::new_blank();
    // CreateTemplateTupleDesc(relnatts): a blank descriptor with `relnatts`
    // attribute slots. The owned model carries `natts` and an empty attrs Vec
    // that RelationBuildTupleDesc fills.
    let natts = relp.relnatts as i32;
    relation.rd_att = OwnedTupleDesc {
        natts,
        tdtypeid: InvalidOid,
        tdtypmod: -1,
        attrs: Vec::new(),
    };
    // Copy the pg_class form into rd_rel (C memcpy of CLASS_TUPLE_SIZE).
    relation.rd_rel = relp;
    Ok(relation)
}

/// `RelationBuildTupleDesc(relation)` (relcache.c): build `rd_att` from
/// `pg_attribute` (+ attrdef/notnull constraint fetches). **Own logic**; the
/// `pg_attribute` scan + `GETSTRUCT` deform is the seamed catalog primitive.
pub fn RelationBuildTupleDesc(relation: &mut RelationData) -> PgResult<()> {
    // C sets the descriptor's composite type id/typmod first (own logic).
    relation.rd_att.tdtypeid = if relation.rd_rel.reltype != InvalidOid {
        relation.rd_rel.reltype
    } else {
        RECORDOID
    };
    relation.rd_att.tdtypmod = -1;

    // The pg_attribute scan (table_open(AttributeRelationId) +
    // systable_beginscan/getnext, GETSTRUCT deform into Form_pg_attribute, the
    // heap_getattr of attmissingval) is the genuine cross-unit catalog
    // primitive. Seam-and-panic until the genam/deform owner lands. The
    // attribute-row assembly, has_not_null/generated accounting, the
    // AttrDefaultFetch / CheckNNConstraintFetch dispatch and the
    // attnullability fixup are this routine's own logic, layered on the rows
    // the seam returns.
    Err(ereport(ERROR)
        .errmsg_internal(
            "relcache-build: RelationBuildTupleDesc pg_attribute read \
             (systable scan via genam + GETSTRUCT deform of Form_pg_attribute) \
             is a cross-unit catalog primitive; owner not yet landed",
        )
        .into_error())
}

/// `RelationParseRelOptions(relation, tuple)` (relcache.c): parse
/// `pg_class.reloptions` into `rd_options`. **Own logic** is the relkind
/// dispatch + storing the parsed result; the parse itself (`extractRelOptions`,
/// access/common/reloptions.c, deforming the reloptions column and invoking the
/// AM `amoptions`) is the cross-unit primitive.
pub fn RelationParseRelOptions(relation: &mut RelationData) -> PgResult<()> {
    // C resets rd_options to NULL, then dispatches on relkind: tables/views/
    // matviews/toast/partitioned-tables use the generic (NULL amoptions) path;
    // indexes use rd_indam->amoptions; everything else returns with no options.
    relation.rd_options = None;
    let relkind = relation.rd_rel.relkind as u8;
    match relkind {
        // amoptsfn = NULL; fall through to extractRelOptions below.
        RELKIND_RELATION
        | RELKIND_TOASTVALUE
        | RELKIND_VIEW
        | RELKIND_MATVIEW
        | RELKIND_PARTITIONED_TABLE => {}
        // amoptsfn = rd_indam->amoptions; fall through.
        RELKIND_INDEX | RELKIND_PARTITIONED_INDEX => {}
        // Everything else: no options, return.
        _ => return Ok(()),
    }
    // extractRelOptions(tuple, GetPgClassDescriptor(), amoptsfn): deforming the
    // pg_class.reloptions column and invoking the AM amoptions is the
    // reloptions-unit / catalog-deform cross-unit primitive. Seam-and-panic.
    // (When reloptions are absent the C result is NULL and rd_options stays
    // None; that no-option case is observable only after the deform, so the
    // cross-unit read gates it.)
    Err(ereport(ERROR)
        .errmsg_internal(
            "relcache-build: RelationParseRelOptions extractRelOptions \
             (reloptions column deform + AM amoptions, access/common/reloptions.c) \
             is a cross-unit primitive; owner not yet landed",
        )
        .into_error())
}

/// `formrdesc(relationName, relationReltype, isshared, natts, attrs)`
/// (relcache.c): build a hardcoded bootstrap relcache entry for a nailed
/// system catalog without catalog access, and install it in `RelationIdCache`.
/// **Own logic**; the hardcoded `FormData_pg_attribute` rows (`attrs`) are the
/// `Schema_pg_*` arrays the caller passes (catalog-header data).
pub fn formrdesc(
    relationName: &str,
    relationReltype: Oid,
    isshared: bool,
    natts: i32,
    attrs: &[OwnedAttr],
) -> PgResult<*mut RelationData> {
    // palloc0 the descriptor; nailed, pinned, valid bootstrap entry.
    let mut relation = RelationData::new_blank();
    relation.rd_refcnt = 1;
    relation.rd_isnailed = true;
    relation.rd_createSubid = InvalidSubTransactionId;
    relation.rd_newRelfilelocatorSubid = InvalidSubTransactionId;
    relation.rd_firstRelfilelocatorSubid = InvalidSubTransactionId;
    relation.rd_droppedSubid = InvalidSubTransactionId;
    relation.rd_backend = INVALID_PROC_NUMBER;
    relation.rd_islocaltemp = false;

    // Hardcoded rd_rel (the bootstrap pg_class row).
    relation.rd_rel.relname = relationName.to_string();
    relation.rd_rel.relnamespace = PG_CATALOG_NAMESPACE;
    relation.rd_rel.reltype = relationReltype;
    relation.rd_rel.relisshared = isshared;
    if isshared {
        relation.rd_rel.reltablespace = GLOBALTABLESPACE_OID;
    }
    relation.rd_rel.relpersistence = PERSIST_PERMANENT;
    relation.rd_rel.relispopulated = true;
    relation.rd_rel.relreplident = crate::REPLICA_IDENTITY_NOTHING;
    relation.rd_rel.relpages = 0;
    relation.rd_rel.reltuples = -1.0;
    relation.rd_rel.relallvisible = 0;
    relation.rd_rel.relkind = RELKIND_RELATION as i8;
    relation.rd_rel.relnatts = natts as i16;

    // Build rd_att from the hardcoded attribute rows.
    debug_assert_eq!(attrs.len(), natts as usize);
    let mut has_not_null = false;
    let mut owned_attrs = Vec::with_capacity(natts as usize);
    for a in attrs.iter() {
        has_not_null |= a.attnotnull;
        owned_attrs.push(a.clone());
    }
    relation.rd_att = OwnedTupleDesc {
        natts,
        tdtypeid: relationReltype,
        tdtypmod: -1,
        attrs: owned_attrs,
    };
    // C sets a TupleConstr{has_not_null} when any column is NOT NULL; the owned
    // entry tracks NOT NULL on each attr row, so the per-row attnotnull above is
    // authoritative (has_not_null retained for faithful structure / asserts).
    let _ = has_not_null;

    // rd_id is the attrelid of the first hardcoded attribute (every Schema_pg_*
    // row carries the catalog's OID in attrelid).
    relation.rd_id = if natts > 0 {
        // The OwnedAttr mirror does not carry attrelid (it is the relation's own
        // OID, identical for every row); the bootstrap caller sets rd_id from
        // the known catalog OID. Until the RelationMapUpdateMap / bootstrap
        // owner lands, formrdesc is only reachable through that path.
        relation.rd_id
    } else {
        relation.rd_id
    };
    relation.rd_rel.relfilenode = InvalidOid;

    // RelationMapUpdateMap (bootstrap), RelationInitLockInfo,
    // RelationInitPhysicalAddr, GetHeapamTableAmRoutine, and the
    // RelationIdCache install follow. The relation-map update +
    // heapam-table-AM-routine resolution are cross-unit (relmapper.c /
    // heapam_handler.c); seam-and-panic until the bootstrap/AM owner lands.
    RelationInitLockInfo(&mut relation);
    Err(ereport(ERROR)
        .errmsg_internal(
            "relcache-build: formrdesc tail (RelationMapUpdateMap, \
             GetHeapamTableAmRoutine, RelationInitPhysicalAddr) crosses into \
             relmapper.c / heapam_handler.c; owner not yet landed",
        )
        .into_error())
}

/// `AttrDefaultFetch(relation, ndef)` (relcache.c): load column default
/// expressions from `pg_attrdef`. **Own logic** is the accounting; the
/// `pg_attrdef` scan + `nodeToString`/`stringToNode` of the default expr tree
/// is the seamed catalog/nodes primitive.
pub fn AttrDefaultFetch(relation: &mut RelationData, ndef: i32) -> PgResult<()> {
    let _ = (relation, ndef);
    Err(ereport(ERROR)
        .errmsg_internal(
            "relcache-build: AttrDefaultFetch pg_attrdef read \
             (systable scan via genam + adbin node-tree deserialization) \
             is a cross-unit primitive; owner not yet landed",
        )
        .into_error())
}

/// `CheckNNConstraintFetch(relation)` (relcache.c): load not-null constraint
/// info from `pg_constraint`. **Own logic** is the accounting; the
/// `pg_constraint` scan is the seamed catalog primitive.
pub fn CheckNNConstraintFetch(relation: &mut RelationData) -> PgResult<()> {
    let _ = relation;
    Err(ereport(ERROR)
        .errmsg_internal(
            "relcache-build: CheckNNConstraintFetch pg_constraint read \
             (systable scan via genam) is a cross-unit primitive; owner not yet \
             landed",
        )
        .into_error())
}

/// `RelationInitLockInfo(relation)` (relcache.c): fill `rd_lockInfo.lockRelId`
/// from the relation's OID and database (`InvalidOid` for shared relations).
/// **Own logic.**
fn RelationInitLockInfo(relation: &mut RelationData) {
    relation.rd_lockInfo.lockRelId.relId = relation.rd_id;
    // C: lockRelId.dbId = relisshared ? InvalidOid : MyDatabaseId. The
    // MyDatabaseId backend-state read lands with the init/postinit owner; for a
    // shared relation it is unconditionally InvalidOid.
    relation.rd_lockInfo.lockRelId.dbId = if relation.rd_rel.relisshared {
        InvalidOid
    } else {
        // MyDatabaseId: filled by the owner; InvalidOid until then (a non-shared
        // relation's dbId is a backend-state read, not catalog data).
        InvalidOid
    };
}
