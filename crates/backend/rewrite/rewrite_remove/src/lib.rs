#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use datum::Datum;
use mcx::Mcx;
use relcache::schemapg::REWRITE_RELATION_ID;
use types_core::fmgr::F_OIDEQ;
use types_core::{AttrNumber, Oid};
use types_error::PgResult;
use types_rel::{AccessExclusiveLock, NoLock, RowExclusiveLock};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};

const REWRITE_OID_INDEX_ID: Oid = 2692;
const Anum_pg_rewrite_oid: AttrNumber = 1;
const Anum_pg_rewrite_ev_class: AttrNumber = 3;

fn oid_key(attno: AttrNumber, oid: Oid) -> ScanKeyData {
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_OIDEQ) failed: {e:?}"));
    key.sk_argument = Datum::from_oid(oid);
    key
}

// Divergence: the allowSystemTableMods refusal is a loud panic.
pub fn RemoveRewriteRuleById<'mcx>(mcx: Mcx<'mcx>, ruleOid: Oid) -> PgResult<()> {
    let rew_rel = table::table_open(mcx, REWRITE_RELATION_ID, RowExclusiveLock)?;
    let keys = [oid_key(Anum_pg_rewrite_oid, ruleOid)];
    let mut scan =
        genam::systable_beginscan(mcx, &rew_rel, REWRITE_OID_INDEX_ID, true, None, &keys)?;
    let tup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("could not find tuple for rule {ruleOid}"));
    let mut isnull = false;
    // SAFETY: ev_class is a fixed NOT NULL pg_rewrite column.
    let eventRelationOid = unsafe {
        types_tuple::heap_getattr(tup, Anum_pg_rewrite_ev_class as i32, rew_rel.descr(), &mut isnull)
    }
    .as_oid();
    let tid = tup.t_self;

    let event_relation = table::table_open(mcx, eventRelationOid, AccessExclusiveLock)?;
    if catalog::IsSystemRelation(&event_relation) {
        panic!("RemoveRewriteRuleById: allowSystemTableMods refusal lane unported");
    }

    catalog_indexing::CatalogTupleDelete(&rew_rel, &tid)?;
    genam::systable_endscan(mcx, scan)?;
    rew_rel.close(RowExclusiveLock)?;

    inval::invalidate::CacheInvalidateRelcache(&event_relation)?;
    event_relation.close(NoLock)
}
