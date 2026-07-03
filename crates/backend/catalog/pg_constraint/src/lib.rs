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
    pub con_expr: Option<types_nodes::Node<'a>>,
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
            con_expr: None,
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

    if let Some(expr) = e.con_expr {
        record_check_expr_dependencies(mcx, &conobject, e.relid, expr)?;
    }
    Ok(con_oid)
}

// recordDependencyOnSingleRelExpr slice (CHECK conExpr): self-rel Var refs
// become NORMAL column deps; every other reference must be pinned.
fn record_check_expr_dependencies<'mcx>(
    mcx: Mcx<'mcx>,
    conobject: &pg_depend::ObjectAddress,
    relid: Oid,
    expr: types_nodes::Node<'mcx>,
) -> PgResult<()> {
    struct W<'m> {
        relid: Oid,
        addrs: PgVec<'m, pg_depend::ObjectAddress>,
    }
    impl<'mcx> nodes_core::NodeWalker<'mcx> for W<'mcx> {
        fn visit(&mut self, node: types_nodes::Node<'mcx>) -> PgResult<bool> {
            use types_nodes::NodeTag::*;
            const TYPE_CLASS: Oid = types_core::TYPE_RELATION_ID;
            const PROC_CLASS: Oid = 1255;
            const OPER_CLASS: Oid = 2617;
            const COLL_CLASS: Oid = 3456;
            let pinned = |class: Oid, oid: Oid| oid == 0 || catalog::IsPinnedObject(class, oid);
            let ok = match node.node_tag() {
                T_Var => {
                    let v = node.as_var().expect("Var");
                    debug_assert!(v.varno == 1);
                    self.addrs.push(pg_depend::ObjectAddress::sub_set(
                        types_core::RELATION_RELATION_ID,
                        self.relid,
                        v.varattno as i32,
                    ));
                    true
                }
                T_Const => {
                    let c = node.as_const().expect("Const");
                    pinned(TYPE_CLASS, c.consttype) && pinned(COLL_CLASS, c.constcollid)
                }
                T_FuncExpr => {
                    let f = node.as_func_expr().expect("FuncExpr");
                    pinned(PROC_CLASS, f.funcid) && pinned(TYPE_CLASS, f.funcresulttype)
                }
                T_OpExpr => {
                    let o = node.as_op_expr().expect("OpExpr");
                    pinned(OPER_CLASS, o.opno) && pinned(TYPE_CLASS, o.opresulttype)
                }
                T_RelabelType | T_CoerceViaIO | T_BoolExpr | T_CaseExpr | T_CaseWhen
                | T_NullTest | T_CoalesceExpr | T_MinMaxExpr | T_List => true,
                other => panic!(
                    "unported: recordDependencyOnSingleRelExpr over {other:?} CHECK expression"
                ),
            };
            if !ok {
                panic!(
                    "unported: recordDependencyOnSingleRelExpr non-pinned reference in \
                     CHECK expression"
                );
            }
            nodes_core::expression_tree_walker(node, self)
        }
    }
    let mut w = W { relid, addrs: mcx::PgVec::new_in(mcx) };
    nodes_core::NodeWalker::visit(&mut w, expr)?;
    let mut addrs = w.addrs;
    pg_depend::record_object_address_dependencies(
        mcx,
        conobject,
        &mut addrs,
        pg_depend::DependencyType::Normal,
    )
}

pub const ConstraintRelidTypidNameIndexId: Oid = 2665;

pub struct NotNullConTup {
    pub oid: Oid,
    pub conname: [u8; 64],
    pub coninhcount: i16,
    pub connoinherit: bool,
    pub conislocal: bool,
    pub convalidated: bool,
    pub attnum: AttrNumber,
}

impl NotNullConTup {
    pub fn name_str(&self) -> &str {
        let len = self.conname.iter().position(|&b| b == 0).unwrap_or(64);
        core::str::from_utf8(&self.conname[..len]).expect("conname UTF-8")
    }
}

// findNotNullConstraintAttnum (pg_constraint.c), decoded-form return.
pub fn findNotNullConstraintAttnum<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    attnum: AttrNumber,
) -> PgResult<Option<NotNullConTup>> {
    let con_rel = table::table_open(mcx, CONSTRAINT_RELATION_ID, AccessShareLock)?;
    let keys = [eq_key(Anum_pg_constraint_conrelid, F_OIDEQ, Datum::from_oid(relid))];
    let mut scan = genam::systable_beginscan(
        mcx,
        &con_rel,
        ConstraintRelidTypidNameIndexId,
        true,
        None,
        &keys,
    )?;
    let desc = con_rel.descr();
    let mut found: Option<NotNullConTup> = None;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let mut isnull = false;
        // SAFETY (each): fixed NOT NULL pg_constraint columns under its descriptor.
        let contype = unsafe {
            types_tuple::heap_getattr(tup, Anum_pg_constraint_contype as i32, desc, &mut isnull)
        }
        .as_i8() as u8;
        if contype != CONSTRAINT_NOTNULL {
            continue;
        }
        let conkey = extract_notnull_column(mcx, tup, desc)?;
        if conkey != attnum {
            continue;
        }
        let get = |anum: AttrNumber| {
            let mut isnull = false;
            // SAFETY: as above.
            unsafe { types_tuple::heap_getattr(tup, anum as i32, desc, &mut isnull) }
        };
        let mut conname = [0u8; 64];
        // SAFETY: NameData column is a 64-byte in-tuple buffer.
        let namebytes = unsafe {
            core::slice::from_raw_parts(get(Anum_pg_constraint_conname).as_usize() as *const u8, 64)
        };
        conname.copy_from_slice(namebytes);
        found = Some(NotNullConTup {
            oid: get(Anum_pg_constraint_oid).as_oid(),
            conname,
            coninhcount: get(Anum_pg_constraint_coninhcount).as_i16(),
            connoinherit: get(Anum_pg_constraint_connoinherit).as_bool(),
            conislocal: get(Anum_pg_constraint_conislocal).as_bool(),
            convalidated: get(Anum_pg_constraint_convalidated).as_bool(),
            attnum: conkey,
        });
        break;
    }
    genam::systable_endscan(mcx, scan)?;
    con_rel.close(AccessShareLock)?;
    Ok(found)
}

// extractNotNullColumn (pg_constraint.c): sole conkey element.
fn extract_notnull_column<'mcx>(
    mcx: Mcx<'mcx>,
    tup: &types_tuple::HeapTupleData<'mcx>,
    desc: &types_tuple::TupleDescData<'mcx>,
) -> PgResult<AttrNumber> {
    let mut isnull = false;
    // SAFETY: conkey is NOT NULL for relation constraints.
    let d = unsafe {
        types_tuple::heap_getattr(tup, Anum_pg_constraint_conkey as i32, desc, &mut isnull)
    };
    debug_assert!(!isnull);
    let p = d.as_usize() as *const u8;
    // SAFETY: live int2[] varlena image through its extent.
    let image = unsafe { core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) };
    let payload = varlena::open_image(mcx, image)?;
    // DatumGetArrayTypeP: rebuild the 4B-header form (image may be packed).
    let body = payload.as_bytes();
    let total = body.len() + 4;
    let mut full: PgVec<'_, u8> = mcx::vec_with_capacity_in(mcx, total)?;
    mcx::vec_append_bytes(&mut full, &(((total as u32) << 2).to_ne_bytes()))?;
    mcx::vec_append_bytes(&mut full, body)?;
    let elems = datum::array_build::deconstruct_array_image(mcx, &full, 2, true, b's')?;
    assert!(elems.len() == 1, "extractNotNullColumn: conkey with {} elements", elems.len());
    Ok(elems[0].as_i16())
}

// ConstraintNameIsUsed (pg_constraint.c), CONSTRAINT_RELATION arm.
pub fn ConstraintNameIsUsed<'mcx>(mcx: Mcx<'mcx>, relid: Oid, conname: &str) -> PgResult<bool> {
    let con_rel = table::table_open(mcx, CONSTRAINT_RELATION_ID, AccessShareLock)?;
    let cname = name_arg(mcx, conname)?;
    let keys = [
        eq_key(Anum_pg_constraint_conrelid, F_OIDEQ, Datum::from_oid(relid)),
        eq_key(Anum_pg_constraint_contypid, F_OIDEQ, Datum::from_oid(InvalidOid)),
        eq_key(Anum_pg_constraint_conname, F_NAMEEQ, Datum::from_usize(cname.as_ptr() as usize)),
    ];
    let mut scan = genam::systable_beginscan(
        mcx,
        &con_rel,
        ConstraintRelidTypidNameIndexId,
        true,
        None,
        &keys,
    )?;
    let found = genam::systable_getnext(mcx, &mut scan)?.is_some();
    genam::systable_endscan(mcx, scan)?;
    con_rel.close(AccessShareLock)?;
    Ok(found)
}

// get_relation_constraint_attnos-free slice of RemoveConstraintById
// (pg_constraint.c): CHECK decrements pg_class.relchecks.
pub fn RemoveConstraintById<'mcx>(mcx: Mcx<'mcx>, con_id: Oid) -> PgResult<()> {
    let con_rel = table::table_open(mcx, CONSTRAINT_RELATION_ID, RowExclusiveLock)?;
    let keys = [eq_key(Anum_pg_constraint_oid, F_OIDEQ, Datum::from_oid(con_id))];
    let mut scan =
        genam::systable_beginscan(mcx, &con_rel, CONSTRAINT_OID_INDEX_ID, true, None, &keys)?;
    let tup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for constraint {con_id}"));
    let desc = con_rel.descr();
    let mut isnull = false;
    // SAFETY (each): fixed NOT NULL pg_constraint columns under its descriptor.
    let contype = unsafe {
        types_tuple::heap_getattr(tup, Anum_pg_constraint_contype as i32, desc, &mut isnull)
    }
    .as_i8() as u8;
    // SAFETY: as above.
    let conrelid = unsafe {
        types_tuple::heap_getattr(tup, Anum_pg_constraint_conrelid as i32, desc, &mut isnull)
    }
    .as_oid();
    if conrelid == InvalidOid {
        panic!("unported: RemoveConstraintById domain constraints");
    }
    let rel = table::table_open(mcx, conrelid, types_rel::AccessExclusiveLock)?;
    if contype == CONSTRAINT_CHECK {
        decrement_relchecks(mcx, conrelid)?;
    }
    rel.close(types_rel::NoLock)?;
    let tid = tup.t_self;
    catalog_indexing::CatalogTupleDelete(&con_rel, &tid)?;
    genam::systable_endscan(mcx, scan)?;
    con_rel.close(RowExclusiveLock)
}

const Anum_pg_class_relchecks: AttrNumber = 20;

fn decrement_relchecks<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<()> {
    let pgrel = table::table_open(mcx, types_core::RELATION_RELATION_ID, RowExclusiveLock)?;
    let keys = [eq_key(1, F_OIDEQ, Datum::from_oid(relid))];
    let mut scan =
        genam::systable_beginscan(mcx, &pgrel, catalog::ClassOidIndexId, true, None, &keys)?;
    let reltup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for relation {relid}"));
    let desc = pgrel.descr();
    let mut isnull = false;
    // SAFETY: fixed NOT NULL pg_class column under pg_class's descriptor.
    let relchecks = unsafe {
        types_tuple::heap_getattr(reltup, Anum_pg_class_relchecks as i32, desc, &mut isnull)
    }
    .as_i16();
    assert!(relchecks > 0, "relation {relid} has relchecks = 0");
    let natts = desc.natts as usize;
    let mut repl_values: PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut repl_isnull: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut repl: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    repl_values.resize(natts, Datum::null());
    repl_isnull.resize(natts, false);
    repl.resize(natts, false);
    repl_values[(Anum_pg_class_relchecks - 1) as usize] = Datum::from_i16(relchecks - 1);
    repl[(Anum_pg_class_relchecks - 1) as usize] = true;
    let mut newtup =
        heaptuple::heap_modify_tuple(mcx, reltup, desc, &repl_values, &repl_isnull, &repl)?;
    let otid = reltup.t_self;
    genam::systable_endscan(mcx, scan)?;
    catalog_indexing::CatalogTupleUpdate(mcx, &pgrel, &otid, &mut newtup)?;
    pgrel.close(RowExclusiveLock)
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

fn conrelid_scan_keys(relid: Oid) -> [ScanKeyData; 1] {
    [eq_key(Anum_pg_constraint_conrelid, F_OIDEQ, Datum::from_oid(relid))]
}

fn getattr<'a>(
    rel: &types_rel::Relation<'_>,
    tup: &types_tuple::HeapTupleData<'a>,
    attno: AttrNumber,
) -> (Datum, bool) {
    let mut isnull = false;
    // SAFETY: pg_constraint column under pg_constraint's own descriptor.
    let d = unsafe { types_tuple::heap_getattr(tup, attno as i32, rel.descr(), &mut isnull) };
    (d, isnull)
}

fn name_str<'mcx>(mcx: Mcx<'mcx>, d: Datum) -> PgResult<&'mcx str> {
    let p = d.as_usize() as *const u8;
    // SAFETY: NOT NULL name column; 64-byte NameData in the live tuple.
    let bytes = unsafe { core::slice::from_raw_parts(p, NAMEDATALEN as usize) };
    let len = bytes.iter().position(|&b| b == 0).unwrap_or(NAMEDATALEN as usize);
    let mut v: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, len)?;
    mcx::vec_append_bytes(&mut v, &bytes[..len])?;
    Ok(core::str::from_utf8(v.leak()).expect("conname UTF-8"))
}

// extractNotNullColumn (pg_constraint.c): conkey[0] of a not-null row.
fn extract_not_null_column<'mcx>(mcx: Mcx<'mcx>, conkey: Datum) -> PgResult<AttrNumber> {
    let p = conkey.as_usize() as *const u8;
    // SAFETY: not-null int2[] column: live varlena image through its extent.
    let image = unsafe { core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) };
    let payload = varlena::open_image(mcx, image)?;
    let body = payload.as_bytes();
    let total = body.len() + 4;
    let mut full: PgVec<'_, u8> = mcx::vec_with_capacity_in(mcx, total)?;
    mcx::vec_append_bytes(&mut full, &(((total as u32) << 2).to_ne_bytes()))?;
    mcx::vec_append_bytes(&mut full, body)?;
    let elems = datum::array_build::deconstruct_array_image(mcx, &full, 2, true, b's')?;
    assert!(elems.len() == 1, "not-null constraint with {} conkey entries", elems.len());
    Ok(elems[0].as_i16())
}

// RelationGetNotNullConstraints, cooked=false arm (raw Constraint nodes).
pub fn RelationGetNotNullConstraints<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &types_rel::Relation<'mcx>,
    include_noinh: bool,
) -> PgResult<types_nodes::NodeList<'mcx>> {
    use types_nodes::rawnodes::{Constraint, ConstrType};
    let relid = rel.rd_id;
    let con_rel = table::table_open(mcx, CONSTRAINT_RELATION_ID, AccessShareLock)?;
    let keys = conrelid_scan_keys(relid);
    let mut scan = genam::systable_beginscan(
        mcx,
        &con_rel,
        types_core::catalog::CONSTRAINT_RELID_TYPID_NAME_INDEX_ID,
        true,
        None,
        &keys,
    )?;
    let mut notnulls = types_nodes::NodeList::nil();
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let contype = getattr(&con_rel, tup, Anum_pg_constraint_contype).0.as_i8() as u8;
        if contype != CONSTRAINT_NOTNULL {
            continue;
        }
        let noinherit = getattr(&con_rel, tup, Anum_pg_constraint_connoinherit).0.as_bool();
        if noinherit && !include_noinh {
            continue;
        }
        let colnum = extract_not_null_column(mcx, getattr(&con_rel, tup, Anum_pg_constraint_conkey).0)?;
        let conname = name_str(mcx, getattr(&con_rel, tup, Anum_pg_constraint_conname).0)?;
        let convalidated = getattr(&con_rel, tup, Anum_pg_constraint_convalidated).0.as_bool();
        let att = rel.rd_att.attr(colnum as usize - 1);
        let colname = {
            let raw = att.attname.name_str();
            let mut v: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, raw.len())?;
            mcx::vec_append_bytes(&mut v, raw)?;
            core::str::from_utf8(v.leak()).expect("attname UTF-8")
        };
        let keys1 = types_nodes::NodeList::make1(
            mcx,
            types_nodes::Node::mk_string(mcx, colname)?,
        )?;
        let constr = Constraint {
            contype: ConstrType::CONSTR_NOTNULL,
            conname: Some(conname),
            keys: keys1,
            is_enforced: true,
            skip_validation: !convalidated,
            initially_valid: true,
            is_no_inherit: noinherit,
            location: -1,
            ..Constraint::default()
        };
        notnulls.lappend(mcx, types_nodes::Node::mk(mcx, constr)?)?;
    }
    genam::systable_endscan(mcx, scan)?;
    con_rel.close(AccessShareLock)?;
    Ok(notnulls)
}

// get_relation_constraint_oid (pg_constraint.c), missing_ok=false lane.
pub fn get_relation_constraint_oid<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    conname: &str,
) -> PgResult<Oid> {
    let con_rel = table::table_open(mcx, CONSTRAINT_RELATION_ID, AccessShareLock)?;
    let keys = conrelid_scan_keys(relid);
    let mut scan = genam::systable_beginscan(
        mcx,
        &con_rel,
        types_core::catalog::CONSTRAINT_RELID_TYPID_NAME_INDEX_ID,
        true,
        None,
        &keys,
    )?;
    let mut found = InvalidOid;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let this_name = name_str(mcx, getattr(&con_rel, tup, Anum_pg_constraint_conname).0)?;
        if this_name == conname {
            found = getattr(&con_rel, tup, Anum_pg_constraint_oid).0.as_oid();
            break;
        }
    }
    genam::systable_endscan(mcx, scan)?;
    con_rel.close(AccessShareLock)?;
    assert!(found != InvalidOid, "constraint \"{conname}\" for table {relid} does not exist");
    Ok(found)
}

// ConstraintNameIsUsed(CONSTRAINT_RELATION) probe.
pub fn ConstraintNameIsUsed<'mcx>(mcx: Mcx<'mcx>, relid: Oid, conname: &str) -> PgResult<bool> {
    let con_rel = table::table_open(mcx, CONSTRAINT_RELATION_ID, AccessShareLock)?;
    let keys = conrelid_scan_keys(relid);
    let mut scan = genam::systable_beginscan(
        mcx,
        &con_rel,
        types_core::catalog::CONSTRAINT_RELID_TYPID_NAME_INDEX_ID,
        true,
        None,
        &keys,
    )?;
    let mut used = false;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let this_name = name_str(mcx, getattr(&con_rel, tup, Anum_pg_constraint_conname).0)?;
        if this_name == conname {
            used = true;
            break;
        }
    }
    genam::systable_endscan(mcx, scan)?;
    con_rel.close(AccessShareLock)?;
    Ok(used)
}

pub fn RemoveConstraintById<'mcx>(mcx: Mcx<'mcx>, con_id: Oid) -> PgResult<()> {
    const Anum_pg_class_relchecks: AttrNumber = 20;
    let con_rel = table::table_open(mcx, CONSTRAINT_RELATION_ID, RowExclusiveLock)?;
    let keys = [eq_key(Anum_pg_constraint_oid, F_OIDEQ, Datum::from_oid(con_id))];
    let mut scan =
        genam::systable_beginscan(mcx, &con_rel, CONSTRAINT_OID_INDEX_ID, true, None, &keys)?;
    let tup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for constraint {con_id}"));

    let conrelid = getattr(&con_rel, tup, Anum_pg_constraint_conrelid).0.as_oid();
    let contypid = getattr(&con_rel, tup, Anum_pg_constraint_contypid).0.as_oid();
    let contype = getattr(&con_rel, tup, Anum_pg_constraint_contype).0.as_i8() as u8;

    if conrelid != InvalidOid {
        let rel = table::table_open(mcx, conrelid, types_rel::AccessExclusiveLock)?;
        if contype == CONSTRAINT_CHECK {
            let relrel = table::table_open(mcx, RELATION_RELATION_ID, RowExclusiveLock)?;
            let relkeys = [eq_key(1, F_OIDEQ, Datum::from_oid(conrelid))];
            let mut relscan = genam::systable_beginscan(
                mcx,
                &relrel,
                catalog::ClassOidIndexId,
                true,
                None,
                &relkeys,
            )?;
            let reltup = genam::systable_getnext(mcx, &mut relscan)?
                .unwrap_or_else(|| panic!("cache lookup failed for relation {conrelid}"));
            let relchecks = getattr(&relrel, reltup, Anum_pg_class_relchecks).0.as_i16();
            assert!(relchecks != 0, "relation \"{}\" has relchecks = 0", rel.name());
            let natts = relrel.descr().natts as usize;
            let mut repl_values: PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
            let mut repl_isnull: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
            let mut repl: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
            repl_values.resize(natts, Datum::null());
            repl_isnull.resize(natts, false);
            repl.resize(natts, false);
            repl_values[(Anum_pg_class_relchecks - 1) as usize] =
                Datum::from_i16(relchecks - 1);
            repl[(Anum_pg_class_relchecks - 1) as usize] = true;
            let mut newtup = heaptuple::heap_modify_tuple(
                mcx,
                reltup,
                relrel.descr(),
                &repl_values,
                &repl_isnull,
                &repl,
            )?;
            let otid = reltup.t_self;
            genam::systable_endscan(mcx, relscan)?;
            catalog_indexing::CatalogTupleUpdate(mcx, &relrel, &otid, &mut newtup)?;
            relrel.close(RowExclusiveLock)?;
        }
        // Keep lock on constraint's rel until end of xact.
        rel.close(types_rel::NoLock)?;
    } else if contypid != InvalidOid {
        // C: no special processing for domain constraints.
    } else {
        panic!("constraint {con_id} is not of a known type");
    }

    let tid = tup.t_self;
    catalog_indexing::CatalogTupleDelete(&con_rel, &tid)?;
    genam::systable_endscan(mcx, scan)?;
    con_rel.close(RowExclusiveLock)?;
    Ok(())
}
