//! pg_constraint.c, CHECK/NOT NULL create lane (CreateConstraintEntry reduced
//! to the fields those contypes populate; FK/index/exclusion vocab arrives
//! with its DDL). Dependency recording is unported (pg_depend unit): DROP
//! leaves the constraint row behind.

#![allow(non_snake_case, non_upper_case_globals)]

use datum::Datum;
use mcx::{Mcx, PgVec};
use types_core::fmgr::{F_NAMEEQ, F_OIDEQ};
use types_core::{
    AttrNumber, Oid, RegProcedure, CONSTRAINT_NAME_NSP_INDEX_ID, CONSTRAINT_OID_INDEX_ID,
    CONSTRAINT_RELATION_ID, INT2OID, InvalidOid, NAMEDATALEN,
};
use types_error::PgResult;
use types_rel::{AccessShareLock, RowExclusiveLock};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};

pub const CONSTRAINT_CHECK: u8 = b'c';
pub const CONSTRAINT_NOTNULL: u8 = b'n';

pub const Anum_pg_constraint_oid: AttrNumber = 1;
pub const Anum_pg_constraint_conname: AttrNumber = 2;
pub const Anum_pg_constraint_connamespace: AttrNumber = 3;
pub const Anum_pg_constraint_contype: AttrNumber = 4;
pub const Anum_pg_constraint_condeferrable: AttrNumber = 5;
pub const Anum_pg_constraint_condeferred: AttrNumber = 6;
pub const Anum_pg_constraint_conenforced: AttrNumber = 7;
pub const Anum_pg_constraint_convalidated: AttrNumber = 8;
pub const Anum_pg_constraint_conrelid: AttrNumber = 9;
pub const Anum_pg_constraint_contypid: AttrNumber = 10;
pub const Anum_pg_constraint_conindid: AttrNumber = 11;
pub const Anum_pg_constraint_conparentid: AttrNumber = 12;
pub const Anum_pg_constraint_confrelid: AttrNumber = 13;
pub const Anum_pg_constraint_confupdtype: AttrNumber = 14;
pub const Anum_pg_constraint_confdeltype: AttrNumber = 15;
pub const Anum_pg_constraint_confmatchtype: AttrNumber = 16;
pub const Anum_pg_constraint_conislocal: AttrNumber = 17;
pub const Anum_pg_constraint_coninhcount: AttrNumber = 18;
pub const Anum_pg_constraint_connoinherit: AttrNumber = 19;
pub const Anum_pg_constraint_conperiod: AttrNumber = 20;
pub const Anum_pg_constraint_conkey: AttrNumber = 21;
pub const Anum_pg_constraint_conbin: AttrNumber = 28;
pub const Natts_pg_constraint: usize = 28;

fn eq_key(attno: AttrNumber, func: RegProcedure, arg: Datum) -> ScanKeyData {
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = types_core::C_COLLATION_OID;
    key.sk_func = fmgr_seams::fmgr_info::call(func)
        .unwrap_or_else(|e| panic!("fmgr_info({func}) failed: {e:?}"));
    key.sk_argument = arg;
    key
}

fn name_arg<'mcx>(mcx: Mcx<'mcx>, name: &str) -> PgResult<PgVec<'mcx, u8>> {
    let n = NAMEDATALEN as usize;
    assert!(name.len() < n, "makeObjectName truncation unported: {name:?}");
    let mut buf: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, n)?;
    mcx::vec_append_bytes(&mut buf, name.as_bytes())?;
    mcx::vec_append_bytes(&mut buf, &[0u8; 64][..n - name.len()])?;
    Ok(buf)
}

pub struct CheckOrNotNullEntry<'a> {
    pub name: &'a str,
    pub namespace_id: Oid,
    pub contype: u8,
    pub is_enforced: bool,
    pub is_validated: bool,
    pub relid: Oid,
    pub conkey: &'a [i16],
    pub conbin: Option<&'a str>,
    pub is_local: bool,
    pub inhcount: i16,
    pub is_no_inherit: bool,
}

// CreateConstraintEntry, CHECK/NOT NULL arm.
pub fn CreateConstraintEntry<'mcx>(
    mcx: Mcx<'mcx>,
    e: &CheckOrNotNullEntry<'_>,
) -> PgResult<Oid> {
    debug_assert!(e.contype == CONSTRAINT_CHECK || e.contype == CONSTRAINT_NOTNULL);
    let con_rel = table::table_open(mcx, CONSTRAINT_RELATION_ID, RowExclusiveLock)?;

    let mut values = [Datum::null(); Natts_pg_constraint];
    let mut nulls = [true; Natts_pg_constraint];
    let mut set = |anum: AttrNumber, v: Datum| {
        values[(anum - 1) as usize] = v;
        nulls[(anum - 1) as usize] = false;
    };
    let con_oid =
        catalog::GetNewOidWithIndex(mcx, &con_rel, CONSTRAINT_OID_INDEX_ID, Anum_pg_constraint_oid)?;
    let cname = name_arg(mcx, e.name)?;
    set(Anum_pg_constraint_oid, Datum::from_oid(con_oid));
    set(Anum_pg_constraint_conname, Datum::from_usize(cname.as_ptr() as usize));
    set(Anum_pg_constraint_connamespace, Datum::from_oid(e.namespace_id));
    set(Anum_pg_constraint_contype, Datum::from_i8(e.contype as i8));
    set(Anum_pg_constraint_condeferrable, Datum::from_bool(false));
    set(Anum_pg_constraint_condeferred, Datum::from_bool(false));
    set(Anum_pg_constraint_conenforced, Datum::from_bool(e.is_enforced));
    set(Anum_pg_constraint_convalidated, Datum::from_bool(e.is_validated));
    set(Anum_pg_constraint_conrelid, Datum::from_oid(e.relid));
    set(Anum_pg_constraint_contypid, Datum::from_oid(InvalidOid));
    set(Anum_pg_constraint_conindid, Datum::from_oid(InvalidOid));
    set(Anum_pg_constraint_conparentid, Datum::from_oid(InvalidOid));
    set(Anum_pg_constraint_confrelid, Datum::from_oid(InvalidOid));
    set(Anum_pg_constraint_confupdtype, Datum::from_i8(b' ' as i8));
    set(Anum_pg_constraint_confdeltype, Datum::from_i8(b' ' as i8));
    set(Anum_pg_constraint_confmatchtype, Datum::from_i8(b' ' as i8));
    set(Anum_pg_constraint_conislocal, Datum::from_bool(e.is_local));
    set(Anum_pg_constraint_coninhcount, Datum::from_i16(e.inhcount));
    set(Anum_pg_constraint_connoinherit, Datum::from_bool(e.is_no_inherit));
    set(Anum_pg_constraint_conperiod, Datum::from_bool(false));

    let conkey_datums: PgVec<'_, Datum> = {
        let mut v = mcx::vec_with_capacity_in(mcx, e.conkey.len())?;
        v.extend(e.conkey.iter().map(|&k| Datum::from_i16(k)));
        v
    };
    let conkey_image =
        datum::array_build::construct_array_image(mcx, &conkey_datums, INT2OID, 2, true, b's')?;
    set(Anum_pg_constraint_conkey, Datum::from_usize(conkey_image.as_ptr() as usize));

    let conbin_text = match e.conbin {
        Some(s) => Some(varlena::cstring_to_text(mcx, s.as_bytes())?),
        None => None,
    };
    if let Some(t) = &conbin_text {
        set(Anum_pg_constraint_conbin, Datum::from_usize(t.as_bytes().as_ptr() as usize));
    }

    let mut tuple = heaptuple::heap_form_tuple(mcx, con_rel.descr(), &values, &nulls)?;
    catalog_indexing::CatalogTupleInsert(mcx, &con_rel, &mut tuple)?;
    con_rel.close(RowExclusiveLock)?;
    Ok(con_oid)
}

// ChooseConstraintName (pg_constraint.c): "name1_name2_label[N]" probed
// against pg_constraint and the in-flight `others` list.
pub fn ChooseConstraintName<'mcx>(
    mcx: Mcx<'mcx>,
    name1: &str,
    name2: Option<&str>,
    label: &str,
    namespace_id: Oid,
    others: &[&str],
) -> PgResult<mcx::PgString<'mcx>> {
    let con_rel = table::table_open(mcx, CONSTRAINT_RELATION_ID, AccessShareLock)?;
    let mut pass = 0;
    let mut modlabel = mcx::PgString::from_str_in(label, mcx)?;
    let conname = loop {
        let conname = make_object_name(mcx, name1, name2, modlabel.as_str())?;
        let mut found = others.iter().any(|&o| o == conname.as_str());
        if !found {
            let cname = name_arg(mcx, conname.as_str())?;
            let keys = [
                eq_key(Anum_pg_constraint_conname, F_NAMEEQ, Datum::from_usize(cname.as_ptr() as usize)),
                eq_key(Anum_pg_constraint_connamespace, F_OIDEQ, Datum::from_oid(namespace_id)),
            ];
            let mut scan = genam::systable_beginscan(
                mcx,
                &con_rel,
                CONSTRAINT_NAME_NSP_INDEX_ID,
                true,
                None,
                &keys,
            )?;
            found = genam::systable_getnext(mcx, &mut scan)?.is_some();
            genam::systable_endscan(mcx, scan)?;
        }
        if !found {
            break conname;
        }
        pass += 1;
        modlabel = mcx::PgString::from_str_in(label, mcx)?;
        use core::fmt::Write;
        write!(modlabel, "{pass}").expect("label suffix");
    };
    con_rel.close(AccessShareLock)?;
    Ok(conname)
}

// makeObjectName without the truncation lane (loud on overflow).
fn make_object_name<'mcx>(
    mcx: Mcx<'mcx>,
    name1: &str,
    name2: Option<&str>,
    label: &str,
) -> PgResult<mcx::PgString<'mcx>> {
    let mut s = mcx::PgString::from_str_in(name1, mcx)?;
    if let Some(n2) = name2 {
        s.try_push_str("_")?;
        s.try_push_str(n2)?;
    }
    s.try_push_str("_")?;
    s.try_push_str(label)?;
    assert!(
        s.len() < NAMEDATALEN as usize,
        "makeObjectName (indexcmds.c): identifier truncation unported ({:?})",
        s.as_str()
    );
    Ok(s)
}
