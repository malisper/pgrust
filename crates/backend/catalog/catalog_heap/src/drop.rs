// heap.c deletion half, plain-table lane: partitions, foreign tables,
// sequences, ON COMMIT actions and subscription states are loud or
// unreachable (their DDL lanes do not exist).
use datum::Datum;
use mcx::Mcx;
use types_core::{
    AttrNumber, Oid, ATTRIBUTE_RELATION_ID, RELATION_RELATION_ID,
};
use types_error::{PgError, PgResult, ERRCODE_OBJECT_IN_USE, ERROR};
use types_rel::{AccessExclusiveLock, NoLock, Relation, RowExclusiveLock, RELKIND_HAS_STORAGE};
use types_scan::scankey::{BTEqualStrategyNumber, BTLessEqualStrategyNumber, ScanKeyData};

const InheritsRelationId: Oid = 2611;
const InheritsRelidSeqnoIndexId: Oid = 2680;
const StatisticRelationId: Oid = 2619;
const StatisticRelidAttnumInhIndexId: Oid = 2696;
const AttributeRelidNumIndexId: Oid = 2659;
const Anum_pg_inherits_inhrelid: usize = 1;
const Anum_pg_statistic_starelid: usize = 1;
const Anum_pg_statistic_staattnum: usize = 2;
const Anum_pg_attribute_attrelid: usize = 1;
const Anum_pg_attribute_attname: usize = 2;
const Anum_pg_attribute_atttypid: usize = 3;
const Anum_pg_attribute_attnum: usize = 5;
const Anum_pg_attribute_attnotnull: usize = 12;
const Anum_pg_attribute_atthasmissing: usize = 14;
const Anum_pg_attribute_attgenerated: usize = 16;
const Anum_pg_attribute_attisdropped: usize = 17;
const Anum_pg_attribute_attstattarget: usize = 21;
const Anum_pg_attribute_attacl: usize = 22;
const Anum_pg_attribute_attoptions: usize = 23;
const Anum_pg_attribute_attfdwoptions: usize = 24;
const Anum_pg_attribute_attmissingval: usize = 25;
const Anum_pg_class_oid: usize = 1;

#[cold]
#[inline(never)]
fn unported(what: &str) -> ! {
    panic!("unported: heap.c {what}")
}

pub(crate) fn oid_scankey(attno: usize, oid: Oid) -> ScanKeyData {
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno as AttrNumber;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_OIDEQ) failed: {e:?}"));
    key.sk_argument = Datum::from_oid(oid);
    key
}

pub(crate) fn int2_scankey(attno: usize, v: i16) -> ScanKeyData {
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno as AttrNumber;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_INT2EQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_INT2EQ) failed: {e:?}"));
    key.sk_argument = Datum::from_i16(v);
    key
}

// CheckTableNotInUse (tablecmds.c): C compares rd_refcnt to expected (nailed
// is a flag here, not a count, so the expected user-ref count is always the
// caller's own handle). RelationUserRefcount survives entry rebuilds, which
// split the Rc identity a bare strong_count check relied on.
pub fn CheckTableNotInUse(rel: &Relation<'_>, stmt: &str) -> PgResult<()> {
    if relcache::RelationUserRefcount(rel.rd_id) != 1 {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!(
                    "cannot {stmt} \"{}\" because it is being used by active queries in this session",
                    rel.name()
                ),
            )
            .with_sqlstate(ERRCODE_OBJECT_IN_USE),
        ));
    }
    if rel.rd_rel.relkind != types_rel::RELKIND_INDEX
        && rel.rd_rel.relkind != types_rel::RELKIND_PARTITIONED_INDEX
        && trigger_seams::after_trigger_pending_on_rel::call(rel.rd_id)
    {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!(
                    "cannot {stmt} \"{}\" because it has pending trigger events",
                    rel.name()
                ),
            )
            .with_sqlstate(ERRCODE_OBJECT_IN_USE),
        ));
    }
    Ok(())
}

pub fn heap_drop_with_catalog<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<()> {
    let mut parent_oid = types_core::InvalidOid;
    let mut default_part_oid = types_core::InvalidOid;
    {
        let pg_class = table::table_open(mcx, RELATION_RELATION_ID, types_rel::AccessShareLock)?;
        let key = oid_scankey(Anum_pg_class_oid, relid);
        let mut scan = genam::systable_beginscan(
            mcx,
            &pg_class,
            catalog::ClassOidIndexId,
            true,
            None,
            &[key],
        )?;
        let tup = genam::systable_getnext(mcx, &mut scan)?
            .unwrap_or_else(|| panic!("cache lookup failed for relation {relid}"));
        let mut isnull = false;
        // SAFETY: relispartition (28) is a fixed NOT NULL pg_class column.
        let relispartition =
            unsafe { types_tuple::heap_getattr(tup, 28, pg_class.descr(), &mut isnull) }.as_bool();
        genam::systable_endscan(mcx, scan)?;
        pg_class.close(types_rel::AccessShareLock)?;
        // A concurrent query may hold a partition descriptor including this
        // partition: the parent lock fences it out until our inval commits.
        if relispartition {
            parent_oid = pg_inherits::get_partition_parent(mcx, relid, true)?;
            lmgr::LockRelationOid(parent_oid, AccessExclusiveLock)?;
            default_part_oid = partcache::get_default_partition_oid(parent_oid)?;
            if default_part_oid != types_core::InvalidOid && relid != default_part_oid {
                lmgr::LockRelationOid(default_part_oid, AccessExclusiveLock)?;
            }
        }
    }

    // C uses relation_open (sequences have no table AM).
    let rel = relation::relation_open(mcx, relid, AccessExclusiveLock)?;

    CheckTableNotInUse(&rel, "DROP TABLE")?;

    predicate_seams::check_table_for_serializable_conflict_in::call(&rel)?;

    match rel.rd_rel.relkind {
        types_rel::RELKIND_RELATION | types_rel::RELKIND_TOASTVALUE
        | types_rel::RELKIND_SEQUENCE | types_rel::RELKIND_VIEW
        | types_rel::RELKIND_MATVIEW | types_rel::RELKIND_PARTITIONED_TABLE
        | types_rel::RELKIND_COMPOSITE_TYPE => {}
        types_rel::RELKIND_FOREIGN_TABLE => delete_foreign_table_tuple(mcx, relid)?,
        other => unported(&format!(
            "heap_drop_with_catalog: relkind {:?} arm",
            other as char
        )),
    }

    if rel.rd_rel.relkind == types_rel::RELKIND_PARTITIONED_TABLE {
        crate::partition::RemovePartitionKeyByRelId(mcx, relid)?;
    }

    if relid == default_part_oid {
        crate::partition::update_default_partition_oid(mcx, parent_oid, types_core::InvalidOid)?;
    }

    if RELKIND_HAS_STORAGE(rel.rd_rel.relkind) {
        catalog_storage::RelationDropStorage(&rel)?;
    }

    pgstat::relation::pgstat_drop_relation(relid, rel.rd_rel.relisshared);

    rel.close(NoLock)?;

    // RemoveSubscriptionRel: no subscription lane exists.
    if tablecmds_seams::remove_on_commit_action::is_installed() {
        tablecmds_seams::remove_on_commit_action::call(relid);
    }

    relcache::invalidate::RelationForgetRelation(relid)?;

    RelationRemoveInheritance(mcx, relid)?;
    RemoveStatistics(mcx, relid, 0)?;
    DeleteAttributeTuples(mcx, relid)?;
    DeleteRelationTuple(mcx, relid)?;

    if parent_oid != types_core::InvalidOid {
        if default_part_oid != types_core::InvalidOid && relid != default_part_oid {
            inval::invalidate::CacheInvalidateRelcacheByRelid(default_part_oid)?;
        }
        // Drop the partition from the parent's cached partition descriptor;
        // the parent lock is held to commit.
        inval::invalidate::CacheInvalidateRelcacheByRelid(parent_oid)?;
    }
    Ok(())
}

// RelationRemoveInheritance: delete pg_inherits rows linking relid to parents.
fn RelationRemoveInheritance<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<()> {
    let rel = table::table_open(mcx, InheritsRelationId, RowExclusiveLock)?;
    let key = oid_scankey(Anum_pg_inherits_inhrelid, relid);
    let mut scan =
        genam::systable_beginscan(mcx, &rel, InheritsRelidSeqnoIndexId, true, None, &[key])?;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let tid = tup.t_self;
        catalog_indexing::CatalogTupleDelete(&rel, &tid)?;
    }
    genam::systable_endscan(mcx, scan)?;
    rel.close(RowExclusiveLock)
}

pub fn RemoveStatistics<'mcx>(mcx: Mcx<'mcx>, relid: Oid, attnum: AttrNumber) -> PgResult<()> {
    let rel = table::table_open(mcx, StatisticRelationId, RowExclusiveLock)?;
    let mut keys = [oid_scankey(Anum_pg_statistic_starelid, relid), ScanKeyData::empty()];
    let nkeys = if attnum == 0 {
        1
    } else {
        keys[1] = int2_scankey(Anum_pg_statistic_staattnum, attnum);
        2
    };
    let mut scan = genam::systable_beginscan(
        mcx,
        &rel,
        StatisticRelidAttnumInhIndexId,
        true,
        None,
        &keys[..nkeys],
    )?;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let tid = tup.t_self;
        catalog_indexing::CatalogTupleDelete(&rel, &tid)?;
    }
    genam::systable_endscan(mcx, scan)?;
    rel.close(RowExclusiveLock)
}

// CopyStatistics (heap.c): copy pg_statistic rows from one relation to another.
pub fn CopyStatistics<'mcx>(mcx: Mcx<'mcx>, fromrelid: Oid, torelid: Oid) -> PgResult<()> {
    let rel = table::table_open(mcx, StatisticRelationId, RowExclusiveLock)?;
    let key = oid_scankey(Anum_pg_statistic_starelid, fromrelid);
    let mut scan = genam::systable_beginscan(
        mcx,
        &rel,
        StatisticRelidAttnumInhIndexId,
        true,
        None,
        &[key],
    )?;
    let desc = rel.descr();
    let natts = desc.natts as usize;
    let mut indstate = None;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let mut values: mcx::PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
        let mut nulls: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
        let mut replace: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
        values.resize(natts, Datum::null());
        nulls.resize(natts, false);
        replace.resize(natts, false);
        values[Anum_pg_statistic_starelid - 1] = Datum::from_oid(torelid);
        replace[Anum_pg_statistic_starelid - 1] = true;
        let mut newtup = heaptuple::heap_modify_tuple(mcx, tup, desc, &values, &nulls, &replace)?;
        if indstate.is_none() {
            indstate = Some(catalog_indexing::CatalogOpenIndexes(mcx, &rel)?);
        }
        catalog_indexing::CatalogTupleInsertWithInfo(
            mcx,
            &rel,
            &mut newtup,
            indstate.as_mut().unwrap(),
        )?;
    }
    genam::systable_endscan(mcx, scan)?;
    if let Some(st) = indstate {
        catalog_indexing::CatalogCloseIndexes(st)?;
    }
    rel.close(RowExclusiveLock)
}

const F_INT2LE: types_core::primitive::RegProcedure = 148;

fn int2le_scankey(attno: usize, v: i16) -> ScanKeyData {
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno as AttrNumber;
    key.sk_strategy = BTLessEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(F_INT2LE)
        .unwrap_or_else(|e| panic!("fmgr_info(F_INT2LE) failed: {e:?}"));
    key.sk_argument = Datum::from_i16(v);
    key
}

pub fn DeleteSystemAttributeTuples<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<()> {
    let attrel = table::table_open(mcx, ATTRIBUTE_RELATION_ID, RowExclusiveLock)?;
    let keys = [
        oid_scankey(Anum_pg_attribute_attrelid, relid),
        int2le_scankey(Anum_pg_attribute_attnum, 0),
    ];
    let mut scan =
        genam::systable_beginscan(mcx, &attrel, AttributeRelidNumIndexId, true, None, &keys)?;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let tid = tup.t_self;
        catalog_indexing::CatalogTupleDelete(&attrel, &tid)?;
    }
    genam::systable_endscan(mcx, scan)?;
    attrel.close(RowExclusiveLock)
}

pub fn DeleteAttributeTuples<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<()> {
    let attrel = table::table_open(mcx, ATTRIBUTE_RELATION_ID, RowExclusiveLock)?;
    let key = oid_scankey(Anum_pg_attribute_attrelid, relid);
    let mut scan =
        genam::systable_beginscan(mcx, &attrel, AttributeRelidNumIndexId, true, None, &[key])?;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let tid = tup.t_self;
        catalog_indexing::CatalogTupleDelete(&attrel, &tid)?;
    }
    genam::systable_endscan(mcx, scan)?;
    attrel.close(RowExclusiveLock)
}

// RemoveAttributeById: mark dropped in place, never physically delete —
// the attribute row keeps attlen/attalign so stored tuples still deform.
pub fn RemoveAttributeById<'mcx>(mcx: Mcx<'mcx>, relid: Oid, attnum: AttrNumber) -> PgResult<()> {
    if attnum <= 0 {
        unported("RemoveAttributeById: system attributes (never dropped)");
    }
    // C: relation_open (heap.c:1700) — a cascaded drop can target a
    // composite type's column, which table_open rejects.
    let rel = relation::relation_open(mcx, relid, AccessExclusiveLock)?;
    let attr_rel = table::table_open(mcx, ATTRIBUTE_RELATION_ID, RowExclusiveLock)?;

    let keys = [
        oid_scankey(Anum_pg_attribute_attrelid, relid),
        int2_scankey(Anum_pg_attribute_attnum, attnum),
    ];
    let mut scan =
        genam::systable_beginscan(mcx, &attr_rel, AttributeRelidNumIndexId, true, None, &keys)?;
    let tup = genam::systable_getnext(mcx, &mut scan)?.unwrap_or_else(|| {
        panic!("cache lookup failed for attribute {attnum} of relation {relid}")
    });

    let natts = attr_rel.descr().natts as usize;
    let mut values: mcx::PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut isnull: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut replace: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    values.resize(natts, Datum::null());
    isnull.resize(natts, false);
    replace.resize(natts, false);

    let mut newname = types_tuple::NameData::default();
    let name = format!("........pg.dropped.{attnum}........");
    newname.namestrcpy(&name);

    let mut set = |anum: usize, v: Datum| {
        values[anum - 1] = v;
        replace[anum - 1] = true;
    };
    set(Anum_pg_attribute_attname, Datum::from_usize(newname.data.as_ptr() as usize));
    set(Anum_pg_attribute_atttypid, Datum::from_oid(types_core::InvalidOid));
    set(Anum_pg_attribute_attnotnull, Datum::from_bool(false));
    set(Anum_pg_attribute_attgenerated, Datum::from_char(0));
    set(Anum_pg_attribute_attisdropped, Datum::from_bool(true));
    set(Anum_pg_attribute_atthasmissing, Datum::from_bool(false));
    for anum in [
        Anum_pg_attribute_attmissingval,
        Anum_pg_attribute_attstattarget,
        Anum_pg_attribute_attacl,
        Anum_pg_attribute_attoptions,
        Anum_pg_attribute_attfdwoptions,
    ] {
        isnull[anum - 1] = true;
        replace[anum - 1] = true;
    }

    let mut newtup =
        heaptuple::heap_modify_tuple(mcx, tup, attr_rel.descr(), &values, &isnull, &replace)?;
    let otid = tup.t_self;
    genam::systable_endscan(mcx, scan)?;
    catalog_indexing::CatalogTupleUpdate(mcx, &attr_rel, &otid, &mut newtup)?;

    attr_rel.close(RowExclusiveLock)?;
    // The pg_attribute update fired the owning relation's relcache inval.
    rel.close(NoLock)?;
    RemoveStatistics(mcx, relid, attnum)
}

// C fetches the row via SearchSysCache1(RELOID) and deletes by t_self; the
// unique-index scan reaches the same tuple.
pub fn DeleteRelationTuple<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<()> {
    let pg_class = table::table_open(mcx, RELATION_RELATION_ID, RowExclusiveLock)?;
    let key = oid_scankey(Anum_pg_class_oid, relid);
    let mut scan =
        genam::systable_beginscan(mcx, &pg_class, catalog::ClassOidIndexId, true, None, &[key])?;
    let tup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for relation {relid}"));
    let tid = tup.t_self;
    catalog_indexing::CatalogTupleDelete(&pg_class, &tid)?;
    genam::systable_endscan(mcx, scan)?;
    pg_class.close(RowExclusiveLock)
}

// heap.c:1846: the pg_foreign_table tuple goes first on DROP FOREIGN TABLE.
fn delete_foreign_table_tuple<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<()> {
    let ftrel =
        table::table_open(mcx, types_core::FOREIGN_TABLE_RELATION_ID, RowExclusiveLock)?;
    let key = oid_scankey(1, relid);
    let mut scan = genam::systable_beginscan(
        mcx,
        &ftrel,
        types_core::FOREIGN_TABLE_RELID_INDEX_ID,
        true,
        None,
        core::slice::from_ref(&key),
    )?;
    let Some(tup) = genam::systable_getnext(mcx, &mut scan)? else {
        panic!("cache lookup failed for foreign table {relid}");
    };
    let tid = tup.t_self;
    catalog_indexing::CatalogTupleDelete(&ftrel, &tid)?;
    genam::systable_endscan(mcx, scan)?;
    ftrel.close(RowExclusiveLock)
}
