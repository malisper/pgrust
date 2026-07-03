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
const Anum_pg_attribute_attrelid: usize = 1;
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

    let rel = table::table_open(mcx, relid, AccessExclusiveLock)?;

    CheckTableNotInUse(&rel, "DROP TABLE")?;

    if xact::IsolationIsSerializable() {
        unported("heap_drop_with_catalog: CheckTableForSerializableConflictIn (predicate.c)");
    }

    match rel.rd_rel.relkind {
        types_rel::RELKIND_RELATION | types_rel::RELKIND_TOASTVALUE => {}
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

    // RemoveSubscriptionRel / remove_on_commit_action: no subscription or ON
    // COMMIT DDL lanes exist, so neither catalog can hold a row for relid.

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
        unported("RemoveStatistics: per-attribute deletion (F_INT2EQ key)");
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
