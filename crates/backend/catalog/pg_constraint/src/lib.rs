//! pg_constraint.c create lane: CreateConstraintEntry full C surface
//! (CHECK/NOT NULL/PRIMARY/UNIQUE/FOREIGN; exclusion vocab arrives with its
//! DDL) with C's auto/normal dependency records. Divergence: CHECK
//! expression dependencies (recordDependencyOnSingleRelExpr) are not
//! recorded (dependency.c walker unported).

#![allow(non_snake_case, non_upper_case_globals)]

use datum::Datum;
use mcx::{Mcx, PgVec};
use types_core::fmgr::{F_NAMEEQ, F_OIDEQ};
use pg_depend::ObjectAddress;
use types_core::{
    AttrNumber, Oid, RegProcedure, CONSTRAINT_NAME_NSP_INDEX_ID, CONSTRAINT_OID_INDEX_ID,
    CONSTRAINT_RELATION_ID, INT2OID, InvalidOid, NAMEDATALEN, RELATION_RELATION_ID,
    TYPE_RELATION_ID,
};

pub const OPERATOR_RELATION_ID: Oid = 2617;
use types_error::PgResult;
use types_rel::{AccessShareLock, RowExclusiveLock};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};

pub const CONSTRAINT_CHECK: u8 = b'c';
pub const CONSTRAINT_FOREIGN: u8 = b'f';
pub const CONSTRAINT_NOTNULL: u8 = b'n';
pub const CONSTRAINT_FOREIGN: u8 = b'f';
pub const CONSTRAINT_PRIMARY: u8 = b'p';
pub const CONSTRAINT_UNIQUE: u8 = b'u';
pub const CONSTRAINT_EXCLUSION: u8 = b'x';

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
pub const Anum_pg_constraint_confkey: AttrNumber = 22;
pub const Anum_pg_constraint_conpfeqop: AttrNumber = 23;
pub const Anum_pg_constraint_conppeqop: AttrNumber = 24;
pub const Anum_pg_constraint_conffeqop: AttrNumber = 25;
pub const Anum_pg_constraint_confdelsetcols: AttrNumber = 26;
pub const Anum_pg_constraint_conexclop: AttrNumber = 27;
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

pub struct ConstraintEntry<'a> {
    pub name: &'a str,
    pub namespace_id: Oid,
    pub contype: u8,
    pub deferrable: bool,
    pub deferred: bool,
    pub is_enforced: bool,
    pub is_validated: bool,
    pub parent_constr_id: Oid,
    pub relid: Oid,
    /// C constraintKey with constraintNTotalKeys entries; n_keys is the
    /// key-column prefix (constraintNKeys).
    pub conkey: &'a [i16],
    pub n_keys: usize,
    pub domain_id: Oid,
    pub index_relid: Oid,
    pub foreign_relid: Oid,
    pub confkey: &'a [i16],
    pub pf_eq_op: &'a [Oid],
    pub pp_eq_op: &'a [Oid],
    pub ff_eq_op: &'a [Oid],
    pub fk_upd_type: u8,
    pub fk_del_type: u8,
    pub fk_del_set_cols: &'a [i16],
    pub fk_match_type: u8,
    pub conbin: Option<&'a str>,
    pub is_local: bool,
    pub inhcount: i16,
    pub is_no_inherit: bool,
    pub con_period: bool,
}

impl<'a> ConstraintEntry<'a> {
    pub fn base(name: &'a str, namespace_id: Oid, contype: u8, relid: Oid) -> Self {
        ConstraintEntry {
            name,
            namespace_id,
            contype,
            deferrable: false,
            deferred: false,
            is_enforced: true,
            is_validated: true,
            parent_constr_id: InvalidOid,
            relid,
            conkey: &[],
            n_keys: 0,
            domain_id: InvalidOid,
            index_relid: InvalidOid,
            foreign_relid: InvalidOid,
            confkey: &[],
            pf_eq_op: &[],
            pp_eq_op: &[],
            ff_eq_op: &[],
            fk_upd_type: b' ',
            fk_del_type: b' ',
            fk_del_set_cols: &[],
            fk_match_type: b' ',
            conbin: None,
            is_local: true,
            inhcount: 0,
            is_no_inherit: false,
            con_period: false,
        }
    }
}

pub fn CreateConstraintEntry<'mcx>(mcx: Mcx<'mcx>, e: &ConstraintEntry<'_>) -> PgResult<Oid> {
    use types_core::OIDOID;
    debug_assert!(
        e.is_enforced || e.contype == CONSTRAINT_CHECK || e.contype == CONSTRAINT_FOREIGN
    );
    debug_assert!(e.is_enforced || !e.is_validated);
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
    set(Anum_pg_constraint_condeferrable, Datum::from_bool(e.deferrable));
    set(Anum_pg_constraint_condeferred, Datum::from_bool(e.deferred));
    set(Anum_pg_constraint_conenforced, Datum::from_bool(e.is_enforced));
    set(Anum_pg_constraint_convalidated, Datum::from_bool(e.is_validated));
    set(Anum_pg_constraint_conrelid, Datum::from_oid(e.relid));
    set(Anum_pg_constraint_contypid, Datum::from_oid(e.domain_id));
    set(Anum_pg_constraint_conindid, Datum::from_oid(e.index_relid));
    set(Anum_pg_constraint_conparentid, Datum::from_oid(e.parent_constr_id));
    set(Anum_pg_constraint_confrelid, Datum::from_oid(e.foreign_relid));
    set(Anum_pg_constraint_confupdtype, Datum::from_i8(e.fk_upd_type as i8));
    set(Anum_pg_constraint_confdeltype, Datum::from_i8(e.fk_del_type as i8));
    set(Anum_pg_constraint_confmatchtype, Datum::from_i8(e.fk_match_type as i8));
    set(Anum_pg_constraint_conislocal, Datum::from_bool(e.is_local));
    set(Anum_pg_constraint_coninhcount, Datum::from_i16(e.inhcount));
    set(Anum_pg_constraint_connoinherit, Datum::from_bool(e.is_no_inherit));
    set(Anum_pg_constraint_conperiod, Datum::from_bool(e.con_period));

    let i16_array = |vals: &[i16]| -> PgResult<Option<PgVec<'mcx, u8>>> {
        if vals.is_empty() {
            return Ok(None);
        }
        let mut v: PgVec<'mcx, Datum> = mcx::vec_with_capacity_in(mcx, vals.len())?;
        v.extend(vals.iter().map(|&k| Datum::from_i16(k)));
        Ok(Some(datum::array_build::construct_array_image(mcx, &v, INT2OID, 2, true, b's')?))
    };
    let oid_array = |vals: &[Oid]| -> PgResult<Option<PgVec<'mcx, u8>>> {
        if vals.is_empty() {
            return Ok(None);
        }
        let mut v: PgVec<'mcx, Datum> = mcx::vec_with_capacity_in(mcx, vals.len())?;
        v.extend(vals.iter().map(|&k| Datum::from_oid(k)));
        Ok(Some(datum::array_build::construct_array_image(mcx, &v, OIDOID, 4, true, b'i')?))
    };
    let arrays = [
        (Anum_pg_constraint_conkey, i16_array(e.conkey)?),
        (Anum_pg_constraint_confkey, i16_array(e.confkey)?),
        (Anum_pg_constraint_conpfeqop, oid_array(e.pf_eq_op)?),
        (Anum_pg_constraint_conppeqop, oid_array(e.pp_eq_op)?),
        (Anum_pg_constraint_conffeqop, oid_array(e.ff_eq_op)?),
        (Anum_pg_constraint_confdelsetcols, i16_array(e.fk_del_set_cols)?),
    ];
    for (anum, img) in &arrays {
        if let Some(img) = img {
            set(*anum, Datum::from_usize(img.as_ptr() as usize));
        }
    }

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

    let conobject = ObjectAddress::set(CONSTRAINT_RELATION_ID, con_oid);

    let mut addrs_auto: PgVec<'mcx, ObjectAddress> = PgVec::new_in(mcx);
    if e.relid != InvalidOid {
        if !e.conkey.is_empty() {
            for &k in e.conkey {
                addrs_auto.push(ObjectAddress::sub_set(RELATION_RELATION_ID, e.relid, k as i32));
            }
        } else {
            addrs_auto.push(ObjectAddress::set(RELATION_RELATION_ID, e.relid));
        }
    }
    if e.domain_id != InvalidOid {
        addrs_auto.push(ObjectAddress::set(TYPE_RELATION_ID, e.domain_id));
    }
    pg_depend::record_object_address_dependencies(
        mcx,
        &conobject,
        &mut addrs_auto,
        pg_depend::DependencyType::Auto,
    )?;

    let mut addrs_normal: PgVec<'mcx, ObjectAddress> = PgVec::new_in(mcx);
    if e.foreign_relid != InvalidOid {
        if !e.confkey.is_empty() {
            for &k in e.confkey {
                addrs_normal
                    .push(ObjectAddress::sub_set(RELATION_RELATION_ID, e.foreign_relid, k as i32));
            }
        } else {
            addrs_normal.push(ObjectAddress::set(RELATION_RELATION_ID, e.foreign_relid));
        }
    }
    if e.index_relid != InvalidOid && e.contype == CONSTRAINT_FOREIGN {
        addrs_normal.push(ObjectAddress::set(RELATION_RELATION_ID, e.index_relid));
    }
    for i in 0..e.pf_eq_op.len() {
        addrs_normal.push(ObjectAddress::set(OPERATOR_RELATION_ID, e.pf_eq_op[i]));
        if e.pp_eq_op[i] != e.pf_eq_op[i] {
            addrs_normal.push(ObjectAddress::set(OPERATOR_RELATION_ID, e.pp_eq_op[i]));
        }
        if e.ff_eq_op[i] != e.pf_eq_op[i] {
            addrs_normal.push(ObjectAddress::set(OPERATOR_RELATION_ID, e.ff_eq_op[i]));
        }
    }
    pg_depend::record_object_address_dependencies(
        mcx,
        &conobject,
        &mut addrs_normal,
        pg_depend::DependencyType::Normal,
    )?;

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
