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
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};

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

// CheckTableNotInUse (tablecmds.c): C compares rd_refcnt to 1; the store's Rc
// plus our handle make the idle baseline 2.
pub fn CheckTableNotInUse(rel: &Relation<'_>, stmt: &str) -> PgResult<()> {
    if std::rc::Rc::strong_count(rel.data_rc()) > 2 {
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
    // C also errors on AfterTriggerPendingOnRel; no trigger-creating DDL
    // exists, so no pending events can exist.
    Ok(())
}

pub fn heap_drop_with_catalog<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<()> {
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
        if relispartition {
            unported("heap_drop_with_catalog: partition parent locking");
        }
    }

    // C uses relation_open (sequences have no table AM).
    let rel = relation::relation_open(mcx, relid, AccessExclusiveLock)?;

    CheckTableNotInUse(&rel, "DROP TABLE")?;

    if xact::IsolationIsSerializable() {
        unported("heap_drop_with_catalog: CheckTableForSerializableConflictIn (predicate.c)");
    }

    match rel.rd_rel.relkind {
        types_rel::RELKIND_RELATION | types_rel::RELKIND_TOASTVALUE
        | types_rel::RELKIND_SEQUENCE | types_rel::RELKIND_VIEW => {}
        other => unported(&format!(
            "heap_drop_with_catalog: relkind {:?} arm",
            other as char
        )),
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
    let rel = table::table_open(mcx, relid, AccessExclusiveLock)?;
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
