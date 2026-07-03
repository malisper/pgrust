// CreateTriggerFiringOn (trigger.c), internal constraint-trigger slice:
// row-level AFTER RI triggers with a pre-resolved builtin tgfoid. Divergence:
// the pg_proc prorettype==trigger check is skipped (callers pass builtins).
use datum::Datum;
use mcx::{Mcx, PgVec};
use pg_depend::{DependencyType, ObjectAddress};
use types_core::fmgr::F_OIDEQ;
use types_core::{AttrNumber, InvalidOid, Oid, NAMEDATALEN, RELATION_RELATION_ID};
use types_error::PgResult;
use types_rel::RowExclusiveLock;
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_trigger::TRIGGER_FIRES_ON_ORIGIN;

pub const TRIGGER_RELATION_ID: Oid = 2620;
pub const TRIGGER_OID_INDEX_ID: Oid = 2702;

const PROCEDURE_RELATION_ID: Oid = 1255;
const CLASS_OID_INDEX_ID: Oid = 2662;

const Anum_pg_trigger_oid: AttrNumber = 1;
const Natts_pg_trigger: usize = 19;
const Anum_pg_class_relhastriggers: usize = 22;

pub struct InternalTriggerArgs<'a> {
    pub trigname_base: &'a str,
    pub relid: Oid,
    pub constrrelid: Oid,
    pub constraint_oid: Oid,
    pub index_oid: Oid,
    pub funcoid: Oid,
    pub tgtype: i16,
}

pub fn CreateTriggerInternal<'mcx>(
    mcx: Mcx<'mcx>,
    args: &InternalTriggerArgs<'_>,
) -> PgResult<Oid> {
    let tgrel = table::table_open(mcx, TRIGGER_RELATION_ID, RowExclusiveLock)?;
    let trigoid =
        catalog::GetNewOidWithIndex(mcx, &tgrel, TRIGGER_OID_INDEX_ID, Anum_pg_trigger_oid)?;

    let mut trigname = mcx::PgString::from_str_in(args.trigname_base, mcx)?;
    {
        use core::fmt::Write;
        write!(trigname, "_{trigoid}").expect("tgname suffix");
    }

    let mut values = [Datum::null(); Natts_pg_trigger];
    let mut nulls = [false; Natts_pg_trigger];
    let cname = name_arg(mcx, trigname.as_str())?;
    let tgattr = empty_int2vector(mcx)?;
    let tgargs = empty_bytea(mcx)?;
    values[0] = Datum::from_oid(trigoid);
    values[1] = Datum::from_oid(args.relid);
    values[2] = Datum::from_oid(InvalidOid);
    values[3] = Datum::from_usize(cname.as_ptr() as usize);
    values[4] = Datum::from_oid(args.funcoid);
    values[5] = Datum::from_i16(args.tgtype);
    values[6] = Datum::from_i8(TRIGGER_FIRES_ON_ORIGIN);
    values[7] = Datum::from_bool(true);
    values[8] = Datum::from_oid(args.constrrelid);
    values[9] = Datum::from_oid(args.index_oid);
    values[10] = Datum::from_oid(args.constraint_oid);
    values[11] = Datum::from_bool(false);
    values[12] = Datum::from_bool(false);
    values[13] = Datum::from_i16(0);
    values[14] = Datum::from_usize(tgattr.as_ptr() as usize);
    values[15] = Datum::from_usize(tgargs.as_ptr() as usize);
    nulls[16] = true;
    nulls[17] = true;
    nulls[18] = true;

    let mut tuple = heaptuple::heap_form_tuple(mcx, tgrel.descr(), &values, &nulls)?;
    catalog_indexing::CatalogTupleInsert(mcx, &tgrel, &mut tuple)?;
    tgrel.close(RowExclusiveLock)?;

    set_relation_has_triggers(mcx, args.relid)?;

    let myself = ObjectAddress::set(TRIGGER_RELATION_ID, trigoid);
    pg_depend::recordDependencyOn(
        mcx,
        &myself,
        &ObjectAddress::set(PROCEDURE_RELATION_ID, args.funcoid),
        DependencyType::Normal,
    )?;
    debug_assert!(args.constraint_oid != InvalidOid);
    pg_depend::recordDependencyOn(
        mcx,
        &myself,
        &ObjectAddress::set(types_core::CONSTRAINT_RELATION_ID, args.constraint_oid),
        DependencyType::Internal,
    )?;
    Ok(trigoid)
}

// The pg_class.relhastriggers update half of CreateTriggerFiringOn; the
// already-true arm's CacheInvalidateRelcacheByTuple is covered by
// CatalogTupleUpdate's inval on the first set.
fn set_relation_has_triggers<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<()> {
    let relrel = table::table_open(mcx, RELATION_RELATION_ID, RowExclusiveLock)?;
    let mut key = ScanKeyData::empty();
    key.sk_attno = 1;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = types_core::C_COLLATION_OID;
    key.sk_func = fmgr_seams::fmgr_info::call(F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_OIDEQ) failed: {e:?}"));
    key.sk_argument = Datum::from_oid(relid);
    let mut scan = genam::systable_beginscan(
        mcx,
        &relrel,
        CLASS_OID_INDEX_ID,
        true,
        None,
        core::slice::from_ref(&key),
    )?;
    let reltup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for relation {relid}"));
    let td = relrel.descr();
    let mut isnull = false;
    // SAFETY: pg_class row under its own descriptor; relhastriggers declared.
    let has = unsafe {
        types_tuple::heap_getattr(reltup, Anum_pg_class_relhastriggers as i32, td, &mut isnull)
    }
    .as_bool();
    if !has {
        let natts = td.natts as usize;
        let mut repl_values: PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
        let mut repl_isnull: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
        let mut repl: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
        repl_values.resize(natts, Datum::null());
        repl_isnull.resize(natts, false);
        repl.resize(natts, false);
        repl_values[Anum_pg_class_relhastriggers - 1] = Datum::from_bool(true);
        repl[Anum_pg_class_relhastriggers - 1] = true;
        let mut newtup =
            heaptuple::heap_modify_tuple(mcx, reltup, td, &repl_values, &repl_isnull, &repl)?;
        let otid = reltup.t_self;
        genam::systable_endscan(mcx, scan)?;
        catalog_indexing::CatalogTupleUpdate(mcx, &relrel, &otid, &mut newtup)?;
        xact::CommandCounterIncrement()?;
    } else {
        genam::systable_endscan(mcx, scan)?;
        inval::invalidate::CacheInvalidateRelcacheByRelid(relid)?;
    }
    relrel.close(RowExclusiveLock)
}

fn name_arg<'mcx>(mcx: Mcx<'mcx>, name: &str) -> PgResult<PgVec<'mcx, u8>> {
    let n = NAMEDATALEN as usize;
    assert!(name.len() < n, "trigger name overflows NAMEDATALEN: {name:?}");
    let mut buf: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, n)?;
    mcx::vec_append_bytes(&mut buf, name.as_bytes())?;
    mcx::vec_append_bytes(&mut buf, &[0u8; 64][..n - name.len()])?;
    Ok(buf)
}

// buildint2vector(NULL, 0): 24-byte 1-D header, dim1 = 0.
fn empty_int2vector<'mcx>(mcx: Mcx<'mcx>) -> PgResult<PgVec<'mcx, u32>> {
    let mut buf: PgVec<'mcx, u32> = mcx::vec_with_capacity_in(mcx, 6)?;
    buf.resize(6, 0);
    buf[0] = types_tuple::varatt::set_varsize_4b_word(24);
    buf[1] = 1;
    buf[2] = 0;
    buf[3] = types_core::INT2OID;
    buf[4] = 0;
    buf[5] = 0;
    Ok(buf)
}

// byteain(""): bare 4-byte varlena header.
fn empty_bytea<'mcx>(mcx: Mcx<'mcx>) -> PgResult<PgVec<'mcx, u32>> {
    let mut buf: PgVec<'mcx, u32> = mcx::vec_with_capacity_in(mcx, 1)?;
    buf.push(types_tuple::varatt::set_varsize_4b_word(4));
    Ok(buf)
}

// RemoveTriggerById (trigger.c). Divergence: the relkind gate accepts what the
// live trigger-creation lanes emit; other relkinds panic instead of 42809.
pub fn RemoveTriggerById<'mcx>(mcx: Mcx<'mcx>, trigOid: Oid) -> PgResult<()> {
    let tgrel = table::table_open(mcx, TRIGGER_RELATION_ID, RowExclusiveLock)?;
    let mut key = ScanKeyData::empty();
    key.sk_attno = Anum_pg_trigger_oid;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = types_core::C_COLLATION_OID;
    key.sk_func = fmgr_seams::fmgr_info::call(F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_OIDEQ) failed: {e:?}"));
    key.sk_argument = Datum::from_oid(trigOid);
    let mut scan = genam::systable_beginscan(
        mcx,
        &tgrel,
        TRIGGER_OID_INDEX_ID,
        true,
        None,
        core::slice::from_ref(&key),
    )?;
    let tup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("could not find tuple for trigger {trigOid}"));
    let td = tgrel.descr();
    let mut isnull = false;
    // SAFETY: tgrelid is a fixed NOT NULL pg_trigger column.
    let relid = unsafe { types_tuple::heap_getattr(tup, 2, td, &mut isnull) }.as_oid();
    let tid = tup.t_self;

    let rel = table::table_open(mcx, relid, types_rel::AccessExclusiveLock)?;
    let relkind = rel.rd_rel.relkind;
    if !matches!(
        relkind,
        types_rel::RELKIND_RELATION
            | types_rel::RELKIND_VIEW
            | types_rel::RELKIND_FOREIGN_TABLE
            | types_rel::RELKIND_PARTITIONED_TABLE
    ) {
        panic!("RemoveTriggerById: relation {relid} relkind {relkind} cannot have triggers");
    }
    if catalog::IsSystemRelation(&rel) && !init_small::globals::allowSystemTableMods() {
        return Err(Box::new(
            types_error::PgError::new(
                types_error::ERROR,
                format!("permission denied: \"{}\" is a system catalog", rel.name()),
            )
            .with_sqlstate(types_error::ERRCODE_INSUFFICIENT_PRIVILEGE),
        ));
    }

    catalog_indexing::CatalogTupleDelete(&tgrel, &tid)?;
    genam::systable_endscan(mcx, scan)?;
    tgrel.close(RowExclusiveLock)?;

    inval::invalidate::CacheInvalidateRelcacheByRelid(relid)?;
    rel.close(types_rel::NoLock)
}
