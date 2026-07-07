// index.c concurrent slice: index_concurrently_create_copy/_build/_swap/
// _set_dead, index_set_state_flags, validate_index.
use datum::Datum;
use mcx::Mcx;
use types_core::{InvalidOid, Oid, INDEX_RELATION_ID, RELATION_RELATION_ID};
use types_error::{PgResult, ERRCODE_FEATURE_NOT_SUPPORTED};
use types_rel::{NoLock, Relation, RowExclusiveLock, ShareUpdateExclusiveLock};
use types_tuple::{HeapTupleData, TupleDescData};

use crate::{
    err, index_create, oid_scankey, IndexCreateExtra, IndexRelidIndexId,
    INDEX_CREATE_CONCURRENT, INDEX_CREATE_SKIP_BUILD,
};

const Anum_pg_class_relname: usize = 2;
const Anum_pg_class_relispartition: usize = 28;
const Anum_pg_class_reloptions: usize = 33;

const Anum_pg_index_indisprimary: i32 = 7;
const Anum_pg_index_indisexclusion: i32 = 8;
const Anum_pg_index_indimmediate: i32 = 9;
const Anum_pg_index_indisclustered: i32 = 10;
const Anum_pg_index_indisvalid: i32 = 11;
const Anum_pg_index_indisready: i32 = 13;
const Anum_pg_index_indislive: i32 = 14;
const Anum_pg_index_indisreplident: i32 = 15;
const Anum_pg_index_indclass: i32 = 18;

const Anum_pg_attribute_attnum: usize = 5;
const Anum_pg_attribute_attstattarget: i32 = 21;
const AttributeRelidNumIndexId: Oid = 2659;
const ATTRIBUTE_RELATION_ID: Oid = 1249;

const ConstraintRelationId: Oid = 2606;
const ConstraintOidIndexId: Oid = 2667;
const TriggerRelationId: Oid = 2620;
const TriggerConstraintIndexId: Oid = 2699;
const Anum_pg_trigger_tgconstrindid: i32 = 10;
const Anum_pg_trigger_tgconstraint: i32 = 11;

const DescriptionRelationId: Oid = 2609;
const DescriptionObjIndexId: Oid = 2675;
const Anum_pg_description_objoid: usize = 1;
const Anum_pg_description_classoid: usize = 2;
const Anum_pg_description_objsubid: usize = 3;

pub enum IndexStateFlagsAction {
    CreateSetReady,
    CreateSetValid,
    DropClearValid,
    DropSetDead,
}

fn getattr_null(
    tup: &HeapTupleData<'_>,
    attnum: i32,
    desc: &TupleDescData<'_>,
) -> (Datum, bool) {
    let mut isnull = false;
    // SAFETY: catalog column under its relation's descriptor.
    let d = unsafe { types_tuple::heap_getattr(tup, attnum, desc, &mut isnull) };
    (d, isnull)
}

fn int4_scankey(attno: usize, v: i32) -> types_scan::scankey::ScanKeyData {
    let mut key = types_scan::scankey::ScanKeyData::empty();
    key.sk_attno = attno as types_core::AttrNumber;
    key.sk_strategy = types_scan::scankey::BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_INT4EQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_INT4EQ) failed: {e:?}"));
    key.sk_argument = Datum::from_i32(v);
    key
}

fn int2_scankey(attno: usize, v: i16) -> types_scan::scankey::ScanKeyData {
    let mut key = types_scan::scankey::ScanKeyData::empty();
    key.sk_attno = attno as types_core::AttrNumber;
    key.sk_strategy = types_scan::scankey::BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_INT2EQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_INT2EQ) failed: {e:?}"));
    key.sk_argument = Datum::from_i16(v);
    key
}

fn name_datum<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<mcx::PgVec<'mcx, u8>> {
    let mut buf: mcx::PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, types_core::NAMEDATALEN as usize)?;
    buf.extend_from_slice(s.as_bytes());
    buf.resize(types_core::NAMEDATALEN as usize, 0);
    Ok(buf)
}

// index_set_state_flags (index.c:3503).
pub fn index_set_state_flags<'mcx>(
    mcx: Mcx<'mcx>,
    indexId: Oid,
    action: IndexStateFlagsAction,
) -> PgResult<()> {
    let pg_index = table::table_open(mcx, INDEX_RELATION_ID, RowExclusiveLock)?;
    let key = [oid_scankey(1, indexId)];
    let mut scan = genam::systable_beginscan(mcx, &pg_index, IndexRelidIndexId, true, None, &key)?;
    let tup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for index {indexId}"));
    let desc = pg_index.descr();
    let natts = desc.natts as usize;
    let get = |attnum: i32| getattr_null(tup, attnum, desc).0.as_bool();

    let mut values: mcx::PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut nulls: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut replace: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    values.resize(natts, Datum::null());
    nulls.resize(natts, false);
    replace.resize(natts, false);
    let set = |anum: i32, v: bool, values: &mut mcx::PgVec<'_, Datum>, replace: &mut mcx::PgVec<'_, bool>| {
        values[anum as usize - 1] = Datum::from_bool(v);
        replace[anum as usize - 1] = true;
    };
    match action {
        IndexStateFlagsAction::CreateSetReady => {
            debug_assert!(get(Anum_pg_index_indislive));
            debug_assert!(!get(Anum_pg_index_indisready));
            debug_assert!(!get(Anum_pg_index_indisvalid));
            set(Anum_pg_index_indisready, true, &mut values, &mut replace);
        }
        IndexStateFlagsAction::CreateSetValid => {
            debug_assert!(get(Anum_pg_index_indislive));
            debug_assert!(get(Anum_pg_index_indisready));
            debug_assert!(!get(Anum_pg_index_indisvalid));
            set(Anum_pg_index_indisvalid, true, &mut values, &mut replace);
        }
        IndexStateFlagsAction::DropClearValid => {
            set(Anum_pg_index_indisvalid, false, &mut values, &mut replace);
            set(Anum_pg_index_indisclustered, false, &mut values, &mut replace);
            set(Anum_pg_index_indisreplident, false, &mut values, &mut replace);
        }
        IndexStateFlagsAction::DropSetDead => {
            debug_assert!(!get(Anum_pg_index_indisvalid));
            set(Anum_pg_index_indisready, false, &mut values, &mut replace);
            set(Anum_pg_index_indislive, false, &mut values, &mut replace);
        }
    }
    let otid = tup.t_self;
    let mut newtup = heaptuple::heap_modify_tuple(mcx, tup, desc, &values, &nulls, &replace)?;
    genam::systable_endscan(mcx, scan)?;
    catalog_indexing::CatalogTupleUpdate(mcx, &pg_index, &otid, &mut newtup)?;
    pg_index.close(RowExclusiveLock)
}

// index_concurrently_create_copy (index.c:1300).
pub fn index_concurrently_create_copy<'mcx>(
    mcx: Mcx<'mcx>,
    heapRelation: &Relation<'mcx>,
    oldIndexId: Oid,
    tablespaceOid: Oid,
    newName: &str,
) -> PgResult<Oid> {
    let indexRelation = indexam::index_open(mcx, oldIndexId, RowExclusiveLock)?;

    let oldInfo = execindexing::BuildIndexInfo(mcx, &indexRelation)?;
    if oldInfo.ii_HasExclusion {
        return Err(err(
            "concurrent index creation for exclusion constraints is not supported".into(),
            ERRCODE_FEATURE_NOT_SUPPORTED,
        ));
    }

    let form = indexRelation.rd_index.as_ref().expect("index relation");
    let nattrs = oldInfo.ii_NumIndexAttrs as usize;

    // indclass off the pg_index row (the Form does not carry it).
    let mut opclass_ids: mcx::PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, nattrs)?;
    {
        let pg_index = table::table_open(mcx, INDEX_RELATION_ID, types_rel::AccessShareLock)?;
        let key = [oid_scankey(1, oldIndexId)];
        let mut scan =
            genam::systable_beginscan(mcx, &pg_index, IndexRelidIndexId, true, None, &key)?;
        let tup = genam::systable_getnext(mcx, &mut scan)?
            .unwrap_or_else(|| panic!("cache lookup failed for index {oldIndexId}"));
        let (d, isnull) = getattr_null(tup, Anum_pg_index_indclass, pg_index.descr());
        debug_assert!(!isnull);
        // SAFETY: not-null plain-storage oidvector column of a live scan tuple;
        // in-place values follow the header (relcache_build precedent).
        unsafe {
            let p = d.as_usize() as *const types_array::oidvector;
            let vals = core::slice::from_raw_parts(p.add(1) as *const Oid, (*p).dim1 as usize);
            opclass_ids.extend_from_slice(vals);
        }
        genam::systable_endscan(mcx, scan)?;
        pg_index.close(types_rel::AccessShareLock)?;
    }

    // reloptions off the pg_class row.
    let mut reloptions_img: Option<mcx::PgVec<'mcx, u8>> = None;
    {
        let pg_class = table::table_open(mcx, RELATION_RELATION_ID, types_rel::AccessShareLock)?;
        let key = [oid_scankey(1, oldIndexId)];
        let mut scan =
            genam::systable_beginscan(mcx, &pg_class, catalog::ClassOidIndexId, true, None, &key)?;
        let tup = genam::systable_getnext(mcx, &mut scan)?
            .unwrap_or_else(|| panic!("cache lookup failed for relation {oldIndexId}"));
        let (d, isnull) = getattr_null(tup, Anum_pg_class_reloptions as i32, pg_class.descr());
        if !isnull {
            let p = d.as_usize() as *const u8;
            // SAFETY: detoasted in-line varlena image of the scan tuple.
            let bytes = unsafe {
                core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p))
            };
            let mut img: mcx::PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, bytes.len())?;
            img.extend_from_slice(bytes);
            reloptions_img = Some(img);
        }
        genam::systable_endscan(mcx, scan)?;
        pg_class.close(types_rel::AccessShareLock)?;
    }

    // Raw (non-flattened) expression/predicate trees, straight off the catalog
    // sources, as C insists for the new index's definition.
    let mut newInfo = execindexing::BuildIndexInfo(mcx, &indexRelation)?;
    if let Some(src) = form.indexprs_src.as_ref() {
        let node = readfuncs::stringToNode(mcx, src.as_str())?;
        newInfo.ii_Expressions = node.as_list().expect("indexprs is a List").clone_in(mcx)?;
    }
    if let Some(src) = form.indpred_src.as_ref() {
        let node = readfuncs::stringToNode(mcx, src.as_str())?;
        newInfo.ii_Predicate = clauses::make_ands_implicit(mcx, Some(node))?;
    }
    newInfo.ii_ReadyForInserts = false;
    newInfo.ii_Concurrent = true;
    newInfo.ii_BrokenHotChain = false;

    let mut colnames: mcx::PgVec<'mcx, &str> = mcx::vec_with_capacity_in(mcx, nattrs)?;
    let index_desc = indexRelation.descr();
    for i in 0..nattrs {
        let att = &index_desc.attrs[i];
        let name = core::str::from_utf8(att.attname.name_str()).expect("non-UTF-8 attname");
        colnames.push(core::str::from_utf8(mcx::slice_borrow_in(mcx, name.as_bytes())?).expect("utf8"));
    }

    let mut opclassOptions: mcx::PgVec<'mcx, Datum> = mcx::vec_with_capacity_in(mcx, nattrs)?;
    for i in 0..nattrs {
        opclassOptions.push(lsyscache::get_attoptions(mcx, oldIndexId, (i + 1) as i16)?);
    }

    let mut stattargets: mcx::PgVec<'mcx, datum::NullableDatum> =
        mcx::vec_with_capacity_in(mcx, nattrs)?;
    {
        let pg_att = table::table_open(mcx, ATTRIBUTE_RELATION_ID, types_rel::AccessShareLock)?;
        for i in 0..nattrs {
            let keys = [
                oid_scankey(1, oldIndexId),
                int2_scankey(Anum_pg_attribute_attnum, (i + 1) as i16),
            ];
            let mut scan = genam::systable_beginscan(
                mcx,
                &pg_att,
                AttributeRelidNumIndexId,
                true,
                None,
                &keys,
            )?;
            let tup = genam::systable_getnext(mcx, &mut scan)?.unwrap_or_else(|| {
                panic!(
                    "cache lookup failed for attribute {} of relation {oldIndexId}",
                    i + 1
                )
            });
            let (value, isnull) =
                getattr_null(tup, Anum_pg_attribute_attstattarget, pg_att.descr());
            stattargets.push(datum::NullableDatum { value, isnull });
            genam::systable_endscan(mcx, scan)?;
        }
        pg_att.close(types_rel::AccessShareLock)?;
    }

    let (newIndexId, _) = index_create(
        mcx,
        heapRelation,
        newName,
        InvalidOid,
        &mut newInfo,
        &colnames,
        indexRelation.rd_rel.relam,
        tablespaceOid,
        &indexRelation.rd_indcollation,
        &opclass_ids,
        &indexRelation.rd_indoption,
        &IndexCreateExtra {
            flags: INDEX_CREATE_SKIP_BUILD | INDEX_CREATE_CONCURRENT,
            constr_flags: 0,
            allow_system_table_mods: true,
            is_internal: false,
            parent_index_relid: InvalidOid,
            parent_constraint_id: InvalidOid,
            reloptions: reloptions_img.as_deref(),
            opclass_options: Some(&opclassOptions[..]),
            stattargets: Some(&stattargets[..]),
            old_number: 0,
        },
    )?;

    indexam::index_close(indexRelation, NoLock)?;
    Ok(newIndexId)
}

// index_concurrently_build (index.c:1485).
pub fn index_concurrently_build<'mcx>(
    mcx: Mcx<'mcx>,
    heapRelationId: Oid,
    indexRelationId: Oid,
) -> PgResult<()> {
    debug_assert!(snapmgr::ActiveSnapshotSet());

    let heapRel = table::table_open(mcx, heapRelationId, ShareUpdateExclusiveLock)?;

    let guard = miscinit::SecContextGuard::security_restricted(heapRel.rd_rel.relowner);
    let save_nestlevel = guc::NewGUCNestLevel();
    guc::RestrictSearchPath()?;

    let indexRelation = indexam::index_open(mcx, indexRelationId, RowExclusiveLock)?;

    let mut indexInfo = execindexing::BuildIndexInfo(mcx, &indexRelation)?;
    debug_assert!(!indexInfo.ii_ReadyForInserts);
    indexInfo.ii_Concurrent = true;
    indexInfo.ii_BrokenHotChain = false;

    crate::index_build(mcx, &heapRel, &indexRelation, &mut indexInfo, false)?;

    guc::AtEOXact_GUC(false, save_nestlevel);
    guard.restore();

    heapRel.close(NoLock)?;
    indexam::index_close(indexRelation, NoLock)?;

    index_set_state_flags(mcx, indexRelationId, IndexStateFlagsAction::CreateSetReady)
}

// validate_index (index.c:3350); progress reporting unported.
pub fn validate_index<'mcx>(
    mcx: Mcx<'mcx>,
    heapId: Oid,
    indexId: Oid,
    snapshot: &snapmgr::Snapshot,
) -> PgResult<()> {
    let heapRelation = table::table_open(mcx, heapId, ShareUpdateExclusiveLock)?;

    let guard = miscinit::SecContextGuard::security_restricted(heapRelation.rd_rel.relowner);
    let save_nestlevel = guc::NewGUCNestLevel();
    guc::RestrictSearchPath()?;

    let indexRelation = indexam::index_open(mcx, indexId, RowExclusiveLock)?;

    let mut indexInfo = execindexing::BuildIndexInfo(mcx, &indexRelation)?;
    indexInfo.ii_Concurrent = true;

    let ivinfo = nbtree::IndexVacuumInfo {
        index: &indexRelation,
        heaprel: &heapRelation,
        analyze_only: false,
        estimated_count: true,
        num_heap_tuples: heapRelation.rd_rel.reltuples as f64,
        strategy: None,
    };

    let mut state = execindexing::ValidateIndexState::new()?;
    {
        let mut cb = |tid: &types_tuple::itemptr::ItemPointerData| state.collect(tid);
        indexam::index_bulk_delete_collect(mcx, &ivinfo, &mut cb)?;
    }
    state.tuplesort.performsort()?;

    execindexing::table_index_validate_scan(
        mcx,
        &heapRelation,
        &indexRelation,
        &mut indexInfo,
        snapshot,
        &mut state,
    )?;

    indexam::index_insert_cleanup(&indexRelation, &mut indexInfo.ii_AmCache)?;

    guc::AtEOXact_GUC(false, save_nestlevel);
    guard.restore();

    indexam::index_close(indexRelation, NoLock)?;
    heapRelation.close(NoLock)
}

// index_concurrently_swap (index.c:1552).
pub fn index_concurrently_swap<'mcx>(
    mcx: Mcx<'mcx>,
    newIndexId: Oid,
    oldIndexId: Oid,
    oldName: &str,
) -> PgResult<()> {
    let oldClassRel = indexam::index_open(mcx, oldIndexId, ShareUpdateExclusiveLock)?;
    let newClassRel = indexam::index_open(mcx, newIndexId, ShareUpdateExclusiveLock)?;

    // pg_class: swap relname and relispartition; old updated first as C.
    {
        let pg_class = table::table_open(mcx, RELATION_RELATION_ID, RowExclusiveLock)?;
        let desc = pg_class.descr();
        let natts = desc.natts as usize;

        let fetch = |mcx: Mcx<'mcx>, relid: Oid| -> PgResult<(mcx::PgVec<'mcx, u8>, bool)> {
            let key = [oid_scankey(1, relid)];
            let mut scan = genam::systable_beginscan(
                mcx,
                &pg_class,
                catalog::ClassOidIndexId,
                true,
                None,
                &key,
            )?;
            let tup = genam::systable_getnext(mcx, &mut scan)?
                .unwrap_or_else(|| panic!("could not find tuple for relation {relid}"));
            let (nd, _) = getattr_null(tup, Anum_pg_class_relname as i32, desc);
            let p = nd.as_usize() as *const u8;
            // SAFETY: name column: fixed 64-byte in-place image.
            let bytes =
                unsafe { core::slice::from_raw_parts(p, types_core::NAMEDATALEN as usize) };
            let mut name: mcx::PgVec<'mcx, u8> =
                mcx::vec_with_capacity_in(mcx, types_core::NAMEDATALEN as usize)?;
            name.extend_from_slice(bytes);
            let (pd, _) = getattr_null(tup, Anum_pg_class_relispartition as i32, desc);
            genam::systable_endscan(mcx, scan)?;
            Ok((name, pd.as_bool()))
        };
        let (old_name_img, old_ispart) = fetch(mcx, oldIndexId)?;
        let (_, new_ispart) = fetch(mcx, newIndexId)?;

        let update = |mcx: Mcx<'mcx>, relid: Oid, name_img: &[u8], ispart: bool| -> PgResult<()> {
            let key = [oid_scankey(1, relid)];
            let mut scan = genam::systable_beginscan(
                mcx,
                &pg_class,
                catalog::ClassOidIndexId,
                true,
                None,
                &key,
            )?;
            let tup = genam::systable_getnext(mcx, &mut scan)?
                .unwrap_or_else(|| panic!("could not find tuple for relation {relid}"));
            let mut values: mcx::PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
            let mut nulls: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
            let mut replace: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
            values.resize(natts, Datum::null());
            nulls.resize(natts, false);
            replace.resize(natts, false);
            values[Anum_pg_class_relname - 1] = Datum::from_usize(name_img.as_ptr() as usize);
            replace[Anum_pg_class_relname - 1] = true;
            values[Anum_pg_class_relispartition - 1] = Datum::from_bool(ispart);
            replace[Anum_pg_class_relispartition - 1] = true;
            let otid = tup.t_self;
            let mut newtup = heaptuple::heap_modify_tuple(mcx, tup, desc, &values, &nulls, &replace)?;
            genam::systable_endscan(mcx, scan)?;
            catalog_indexing::CatalogTupleUpdate(mcx, &pg_class, &otid, &mut newtup)
        };
        let ccold_img = name_datum(mcx, oldName)?;
        update(mcx, oldIndexId, &ccold_img, new_ispart)?;
        update(mcx, newIndexId, &old_name_img, old_ispart)?;
        pg_class.close(RowExclusiveLock)?;
    }

    // pg_index: move constraint flags to the new index, valid/invalid swap.
    {
        let pg_index = table::table_open(mcx, INDEX_RELATION_ID, RowExclusiveLock)?;
        let desc = pg_index.descr();
        let natts = desc.natts as usize;

        let (old_isprimary, old_isexclusion, old_immediate, old_clustered, old_replident) = {
            let key = [oid_scankey(1, oldIndexId)];
            let mut scan =
                genam::systable_beginscan(mcx, &pg_index, IndexRelidIndexId, true, None, &key)?;
            let tup = genam::systable_getnext(mcx, &mut scan)?
                .unwrap_or_else(|| panic!("could not find tuple for relation {oldIndexId}"));
            let g = |a: i32| getattr_null(tup, a, desc).0.as_bool();
            let r = (
                g(Anum_pg_index_indisprimary),
                g(Anum_pg_index_indisexclusion),
                g(Anum_pg_index_indimmediate),
                g(Anum_pg_index_indisclustered),
                g(Anum_pg_index_indisreplident),
            );
            genam::systable_endscan(mcx, scan)?;
            r
        };

        let update = |mcx: Mcx<'mcx>, relid: Oid, sets: &[(i32, bool)]| -> PgResult<()> {
            let key = [oid_scankey(1, relid)];
            let mut scan =
                genam::systable_beginscan(mcx, &pg_index, IndexRelidIndexId, true, None, &key)?;
            let tup = genam::systable_getnext(mcx, &mut scan)?
                .unwrap_or_else(|| panic!("could not find tuple for relation {relid}"));
            let mut values: mcx::PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
            let mut nulls: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
            let mut replace: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
            values.resize(natts, Datum::null());
            nulls.resize(natts, false);
            replace.resize(natts, false);
            for &(a, v) in sets {
                values[a as usize - 1] = Datum::from_bool(v);
                replace[a as usize - 1] = true;
            }
            let otid = tup.t_self;
            let mut newtup = heaptuple::heap_modify_tuple(mcx, tup, desc, &values, &nulls, &replace)?;
            genam::systable_endscan(mcx, scan)?;
            catalog_indexing::CatalogTupleUpdate(mcx, &pg_index, &otid, &mut newtup)
        };
        update(
            mcx,
            oldIndexId,
            &[
                (Anum_pg_index_indisprimary, false),
                (Anum_pg_index_indisexclusion, false),
                (Anum_pg_index_indimmediate, true),
                (Anum_pg_index_indisvalid, false),
                (Anum_pg_index_indisclustered, false),
                (Anum_pg_index_indisreplident, false),
            ],
        )?;
        update(
            mcx,
            newIndexId,
            &[
                (Anum_pg_index_indisprimary, old_isprimary),
                (Anum_pg_index_indisexclusion, old_isexclusion),
                (Anum_pg_index_indimmediate, old_immediate),
                (Anum_pg_index_indisclustered, old_clustered),
                (Anum_pg_index_indisreplident, old_replident),
                (Anum_pg_index_indisvalid, true),
            ],
        )?;
        pg_index.close(RowExclusiveLock)?;
    }

    // Move constraints and their triggers over to the new index.
    {
        let mut constraintOids = pg_depend::get_index_ref_constraints(mcx, oldIndexId)?;
        let indexConstraintOid = pg_depend::get_index_constraint(mcx, oldIndexId)?;
        if indexConstraintOid != InvalidOid {
            constraintOids.push(indexConstraintOid);
        }

        let pg_constraint = table::table_open(mcx, ConstraintRelationId, RowExclusiveLock)?;
        let pg_trigger = table::table_open(mcx, TriggerRelationId, RowExclusiveLock)?;

        for &constraintOid in constraintOids.iter() {
            {
                let desc = pg_constraint.descr();
                let natts = desc.natts as usize;
                let key = [oid_scankey(1, constraintOid)];
                let mut scan = genam::systable_beginscan(
                    mcx,
                    &pg_constraint,
                    ConstraintOidIndexId,
                    true,
                    None,
                    &key,
                )?;
                let tup = genam::systable_getnext(mcx, &mut scan)?.unwrap_or_else(|| {
                    panic!("could not find tuple for constraint {constraintOid}")
                });
                let (conindid, _) = getattr_null(
                    tup,
                    pg_constraint::Anum_pg_constraint_conindid as i32,
                    desc,
                );
                if conindid.as_oid() == oldIndexId {
                    let mut values: mcx::PgVec<'_, Datum> =
                        mcx::vec_with_capacity_in(mcx, natts)?;
                    let mut nulls: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
                    let mut replace: mcx::PgVec<'_, bool> =
                        mcx::vec_with_capacity_in(mcx, natts)?;
                    values.resize(natts, Datum::null());
                    nulls.resize(natts, false);
                    replace.resize(natts, false);
                    values[pg_constraint::Anum_pg_constraint_conindid as usize - 1] =
                        Datum::from_oid(newIndexId);
                    replace[pg_constraint::Anum_pg_constraint_conindid as usize - 1] = true;
                    let otid = tup.t_self;
                    let mut newtup =
                        heaptuple::heap_modify_tuple(mcx, tup, desc, &values, &nulls, &replace)?;
                    genam::systable_endscan(mcx, scan)?;
                    catalog_indexing::CatalogTupleUpdate(mcx, &pg_constraint, &otid, &mut newtup)?;
                } else {
                    genam::systable_endscan(mcx, scan)?;
                }
            }
            {
                let desc = pg_trigger.descr();
                let natts = desc.natts as usize;
                let key = [oid_scankey(Anum_pg_trigger_tgconstraint as usize, constraintOid)];
                let mut scan = genam::systable_beginscan(
                    mcx,
                    &pg_trigger,
                    TriggerConstraintIndexId,
                    true,
                    None,
                    &key,
                )?;
                let mut updates: mcx::PgVec<'_, types_tuple::ItemPointerData> =
                    mcx::PgVec::new_in(mcx);
                while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
                    let (tgconstrindid, _) = getattr_null(tup, Anum_pg_trigger_tgconstrindid, desc);
                    if tgconstrindid.as_oid() == oldIndexId {
                        updates.push(tup.t_self);
                    }
                }
                genam::systable_endscan(mcx, scan)?;
                for otid in updates.iter() {
                    let key = [oid_scankey(Anum_pg_trigger_tgconstraint as usize, constraintOid)];
                    let mut scan = genam::systable_beginscan(
                        mcx,
                        &pg_trigger,
                        TriggerConstraintIndexId,
                        true,
                        None,
                        &key,
                    )?;
                    let mut newtup_opt = None;
                    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
                        if tup.t_self != *otid {
                            continue;
                        }
                        let mut values: mcx::PgVec<'_, Datum> =
                            mcx::vec_with_capacity_in(mcx, natts)?;
                        let mut nulls: mcx::PgVec<'_, bool> =
                            mcx::vec_with_capacity_in(mcx, natts)?;
                        let mut replace: mcx::PgVec<'_, bool> =
                            mcx::vec_with_capacity_in(mcx, natts)?;
                        values.resize(natts, Datum::null());
                        nulls.resize(natts, false);
                        replace.resize(natts, false);
                        values[Anum_pg_trigger_tgconstrindid as usize - 1] =
                            Datum::from_oid(newIndexId);
                        replace[Anum_pg_trigger_tgconstrindid as usize - 1] = true;
                        newtup_opt = Some(heaptuple::heap_modify_tuple(
                            mcx, tup, desc, &values, &nulls, &replace,
                        )?);
                        break;
                    }
                    genam::systable_endscan(mcx, scan)?;
                    let mut newtup = newtup_opt.expect("trigger tuple vanished mid-swap");
                    catalog_indexing::CatalogTupleUpdate(mcx, &pg_trigger, otid, &mut newtup)?;
                }
            }
        }
        pg_constraint.close(RowExclusiveLock)?;
        pg_trigger.close(RowExclusiveLock)?;
    }

    // Move comment if any.
    {
        let description = table::table_open(mcx, DescriptionRelationId, RowExclusiveLock)?;
        let desc = description.descr();
        let natts = desc.natts as usize;
        let keys = [
            oid_scankey(Anum_pg_description_objoid, oldIndexId),
            oid_scankey(Anum_pg_description_classoid, RELATION_RELATION_ID),
            int4_scankey(Anum_pg_description_objsubid, 0),
        ];
        let mut scan =
            genam::systable_beginscan(mcx, &description, DescriptionObjIndexId, true, None, &keys)?;
        if let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
            let mut values: mcx::PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
            let mut nulls: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
            let mut replace: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
            values.resize(natts, Datum::null());
            nulls.resize(natts, false);
            replace.resize(natts, false);
            values[Anum_pg_description_objoid - 1] = Datum::from_oid(newIndexId);
            replace[Anum_pg_description_objoid - 1] = true;
            let otid = tup.t_self;
            let mut newtup = heaptuple::heap_modify_tuple(mcx, tup, desc, &values, &nulls, &replace)?;
            genam::systable_endscan(mcx, scan)?;
            catalog_indexing::CatalogTupleUpdate(mcx, &description, &otid, &mut newtup)?;
        } else {
            genam::systable_endscan(mcx, scan)?;
        }
        description.close(NoLock)?;
    }

    // Swap inheritance relationship with a parent index, if a partition.
    if lsyscache::get_rel_relispartition(oldIndexId)? {
        let ancestors = pg_inherits::get_partition_ancestors(mcx, oldIndexId)?;
        let parentIndexRelid = ancestors[0];
        pg_inherits::DeleteInheritsTuple(mcx, oldIndexId, parentIndexRelid, false, None)?;
        pg_inherits::StoreSingleInheritance(mcx, newIndexId, parentIndexRelid, 1)?;
    }

    // Swap all dependencies of and on the two indexes; no CCI in between.
    pg_depend::changeDependenciesOf(mcx, RELATION_RELATION_ID, newIndexId, oldIndexId)?;
    pg_depend::changeDependenciesOn(mcx, RELATION_RELATION_ID, newIndexId, oldIndexId)?;
    pg_depend::changeDependenciesOf(mcx, RELATION_RELATION_ID, oldIndexId, newIndexId)?;
    pg_depend::changeDependenciesOn(mcx, RELATION_RELATION_ID, oldIndexId, newIndexId)?;

    pgstat::relation::pgstat_copy_relation_stats(
        newClassRel.rd_id,
        newClassRel.rd_rel.relisshared,
        oldClassRel.rd_id,
        oldClassRel.rd_rel.relisshared,
    );
    catalog_heap::CopyStatistics(mcx, oldIndexId, newIndexId)?;

    indexam::index_close(oldClassRel, NoLock)?;
    indexam::index_close(newClassRel, NoLock)
}

// index_concurrently_set_dead (index.c:1823).
pub fn index_concurrently_set_dead<'mcx>(mcx: Mcx<'mcx>, heapId: Oid, indexId: Oid) -> PgResult<()> {
    let userHeapRelation = table::table_open(mcx, heapId, ShareUpdateExclusiveLock)?;
    let userIndexRelation = indexam::index_open(mcx, indexId, ShareUpdateExclusiveLock)?;

    predicate_seams::transfer_predicate_locks_to_heap_relation::call(&userIndexRelation)?;

    index_set_state_flags(mcx, indexId, IndexStateFlagsAction::DropSetDead)?;

    inval::invalidate::CacheInvalidateRelcache(&userHeapRelation)?;

    userHeapRelation.close(NoLock)?;
    indexam::index_close(userIndexRelation, NoLock)
}
