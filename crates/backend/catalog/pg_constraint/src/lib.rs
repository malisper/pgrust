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
use types_error::{PgError, PgResult, ERRCODE_DUPLICATE_OBJECT, ERROR};
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
    pub excl_op: &'a [Oid],
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
            excl_op: &[],
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
    // conkey holds the constraintNKeys key-column prefix (pg_constraint.c:
    // 117-127); included columns appear only in the dependency records below.
    let arrays = [
        (Anum_pg_constraint_conkey, i16_array(&e.conkey[..e.n_keys])?),
        (Anum_pg_constraint_confkey, i16_array(e.confkey)?),
        (Anum_pg_constraint_conpfeqop, oid_array(e.pf_eq_op)?),
        (Anum_pg_constraint_conppeqop, oid_array(e.pp_eq_op)?),
        (Anum_pg_constraint_conffeqop, oid_array(e.ff_eq_op)?),
        (Anum_pg_constraint_confdelsetcols, i16_array(e.fk_del_set_cols)?),
        (Anum_pg_constraint_conexclop, oid_array(e.excl_op)?),
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

// recordDependencyOnSingleRelExpr (dependency.c) over the CHECK conExpr,
// NORMAL/NORMAL, reverse_self=false (pg_constraint.c:387).
fn record_check_expr_dependencies<'mcx>(
    mcx: Mcx<'mcx>,
    conobject: &pg_depend::ObjectAddress,
    relid: Oid,
    expr: types_nodes::Node<'mcx>,
) -> PgResult<()> {
    pg_depend::recordDependencyOnSingleRelExpr(
        mcx,
        conobject,
        expr,
        relid,
        pg_depend::DependencyType::Normal,
        pg_depend::DependencyType::Normal,
        false,
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
pub fn extract_notnull_column<'mcx>(
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

pub struct ConShape {
    pub oid: Oid,
    pub contype: u8,
    pub conname: [u8; 64],
    pub coninhcount: i16,
    pub connoinherit: bool,
    pub conislocal: bool,
    pub condeferrable: bool,
    pub condeferred: bool,
    pub conenforced: bool,
    pub convalidated: bool,
    pub conparentid: Oid,
    pub conindid: Oid,
    pub confrelid: Oid,
    pub notnull_attnum: AttrNumber,
}

impl ConShape {
    pub fn name_str(&self) -> &str {
        let len = self.conname.iter().position(|&b| b == 0).unwrap_or(64);
        core::str::from_utf8(&self.conname[..len]).expect("conname UTF-8")
    }
}

// The ATExecDropConstraint / rename_constraint_internal lookup: the
// (conrelid, contypid=0, conname) row, decoded.
pub fn findConstraintByName<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    conname: &str,
) -> PgResult<Option<ConShape>> {
    let con_rel = table::table_open(mcx, CONSTRAINT_RELATION_ID, RowExclusiveLock)?;
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
    let desc = con_rel.descr();
    let found = match genam::systable_getnext(mcx, &mut scan)? {
        None => None,
        Some(tup) => {
            let get = |anum: AttrNumber| {
                let mut isnull = false;
                // SAFETY: fixed NOT NULL pg_constraint columns under its descriptor.
                unsafe { types_tuple::heap_getattr(tup, anum as i32, desc, &mut isnull) }
            };
            let contype = get(Anum_pg_constraint_contype).as_i8() as u8;
            let mut namebuf = [0u8; 64];
            // SAFETY: NameData column is a 64-byte in-tuple buffer.
            let namebytes = unsafe {
                core::slice::from_raw_parts(
                    get(Anum_pg_constraint_conname).as_usize() as *const u8,
                    64,
                )
            };
            namebuf.copy_from_slice(namebytes);
            let notnull_attnum = if contype == CONSTRAINT_NOTNULL {
                extract_notnull_column(mcx, tup, desc)?
            } else {
                0
            };
            Some(ConShape {
                oid: get(Anum_pg_constraint_oid).as_oid(),
                contype,
                conname: namebuf,
                coninhcount: get(Anum_pg_constraint_coninhcount).as_i16(),
                connoinherit: get(Anum_pg_constraint_connoinherit).as_bool(),
                conislocal: get(Anum_pg_constraint_conislocal).as_bool(),
                condeferrable: get(Anum_pg_constraint_condeferrable).as_bool(),
                condeferred: get(Anum_pg_constraint_condeferred).as_bool(),
                conenforced: get(Anum_pg_constraint_conenforced).as_bool(),
                convalidated: get(Anum_pg_constraint_convalidated).as_bool(),
                conparentid: get(Anum_pg_constraint_conparentid).as_oid(),
                conindid: get(Anum_pg_constraint_conindid).as_oid(),
                confrelid: get(Anum_pg_constraint_confrelid).as_oid(),
                notnull_attnum,
            })
        }
    };
    genam::systable_endscan(mcx, scan)?;
    con_rel.close(RowExclusiveLock)?;
    Ok(found)
}

// The convalidated=true flip shared by the Queue*ConstraintValidation arms
// (tablecmds.c); DIVERGENCE: C updates the tuple found by the caller's name
// scan, this re-finds the row by oid (catalog-only, same row).
pub fn SetConstraintValidated<'mcx>(mcx: Mcx<'mcx>, con_id: Oid) -> PgResult<()> {
    let con_rel = table::table_open(mcx, CONSTRAINT_RELATION_ID, RowExclusiveLock)?;
    let keys = [eq_key(Anum_pg_constraint_oid, F_OIDEQ, Datum::from_oid(con_id))];
    let mut scan =
        genam::systable_beginscan(mcx, &con_rel, CONSTRAINT_OID_INDEX_ID, true, None, &keys)?;
    let tup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for constraint {con_id}"));
    let desc = con_rel.descr();
    let natts = desc.natts as usize;
    let mut repl_values: PgVec<'_, Datum> = mcx::vec_from_elem_in(mcx, Datum::null(), natts);
    let repl_isnull: PgVec<'_, bool> = mcx::vec_from_elem_in(mcx, false, natts);
    let mut repl: PgVec<'_, bool> = mcx::vec_from_elem_in(mcx, false, natts);
    repl_values[(Anum_pg_constraint_convalidated - 1) as usize] = Datum::from_bool(true);
    repl[(Anum_pg_constraint_convalidated - 1) as usize] = true;
    let mut newtup =
        heaptuple::heap_modify_tuple(mcx, tup, desc, &repl_values, &repl_isnull, &repl)?;
    let otid = tup.t_self;
    genam::systable_endscan(mcx, scan)?;
    catalog_indexing::CatalogTupleUpdate(mcx, &con_rel, &otid, &mut newtup)?;
    con_rel.close(RowExclusiveLock)
}

// The QueueCheckConstraintValidation conbin fetch (tablecmds.c).
pub fn constraint_conbin<'mcx>(mcx: Mcx<'mcx>, con_id: Oid) -> PgResult<mcx::PgString<'mcx>> {
    let con_rel = table::table_open(mcx, CONSTRAINT_RELATION_ID, AccessShareLock)?;
    let keys = [eq_key(Anum_pg_constraint_oid, F_OIDEQ, Datum::from_oid(con_id))];
    let mut scan =
        genam::systable_beginscan(mcx, &con_rel, CONSTRAINT_OID_INDEX_ID, true, None, &keys)?;
    let tup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for constraint {con_id}"));
    let mut isnull = false;
    // SAFETY: varlena column under pg_constraint's descriptor; image live
    // through the open scan.
    let d = unsafe {
        types_tuple::heap_getattr(tup, Anum_pg_constraint_conbin as i32, con_rel.descr(), &mut isnull)
    };
    assert!(!isnull, "null conbin for constraint {con_id}");
    let p = d.as_usize() as *const u8;
    // SAFETY: live varlena image through its extent.
    let image = unsafe { core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) };
    let payload = varlena::open_image(mcx, image)?;
    let s = mcx::PgString::from_str_in(
        core::str::from_utf8(payload.as_bytes()).expect("conbin UTF-8"),
        mcx,
    )?;
    genam::systable_endscan(mcx, scan)?;
    con_rel.close(AccessShareLock)?;
    Ok(s)
}

// RenameConstraintById (pg_constraint.c) minus the object-access hook.
pub fn RenameConstraintById<'mcx>(mcx: Mcx<'mcx>, con_id: Oid, newname: &str) -> PgResult<()> {
    let con_rel = table::table_open(mcx, CONSTRAINT_RELATION_ID, RowExclusiveLock)?;
    let keys = [eq_key(Anum_pg_constraint_oid, F_OIDEQ, Datum::from_oid(con_id))];
    let mut scan =
        genam::systable_beginscan(mcx, &con_rel, CONSTRAINT_OID_INDEX_ID, true, None, &keys)?;
    let tup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for constraint {con_id}"));
    let desc = con_rel.descr();
    let mut isnull = false;
    // SAFETY (each): fixed NOT NULL pg_constraint columns under its descriptor.
    let conrelid = unsafe {
        types_tuple::heap_getattr(tup, Anum_pg_constraint_conrelid as i32, desc, &mut isnull)
    }
    .as_oid();
    // SAFETY: as above.
    let contypid = unsafe {
        types_tuple::heap_getattr(tup, Anum_pg_constraint_contypid as i32, desc, &mut isnull)
    }
    .as_oid();
    if conrelid != InvalidOid
        && ConstraintNameIsUsed(mcx, ConstraintCategory::Relation, conrelid, newname)?
    {
        let relname = rel_name_for_error(mcx, conrelid)?;
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!("constraint \"{newname}\" for relation \"{relname}\" already exists"),
            )
            .with_sqlstate(ERRCODE_DUPLICATE_OBJECT),
        ));
    }
    if contypid != InvalidOid
        && ConstraintNameIsUsed(mcx, ConstraintCategory::Domain, contypid, newname)?
    {
        let typname = format_type::format_type_be(contypid)?;
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!("constraint \"{newname}\" for domain {typname} already exists"),
            )
            .with_sqlstate(ERRCODE_DUPLICATE_OBJECT),
        ));
    }
    let natts = desc.natts as usize;
    let newbuf = name_arg(mcx, newname)?;
    let mut repl_values: PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut repl_isnull: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut repl: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    repl_values.resize(natts, Datum::null());
    repl_isnull.resize(natts, false);
    repl.resize(natts, false);
    repl_values[(Anum_pg_constraint_conname - 1) as usize] =
        Datum::from_usize(newbuf.as_ptr() as usize);
    repl[(Anum_pg_constraint_conname - 1) as usize] = true;
    let mut newtup =
        heaptuple::heap_modify_tuple(mcx, tup, desc, &repl_values, &repl_isnull, &repl)?;
    let otid = tup.t_self;
    genam::systable_endscan(mcx, scan)?;
    catalog_indexing::CatalogTupleUpdate(mcx, &con_rel, &otid, &mut newtup)?;
    con_rel.close(RowExclusiveLock)
}

pub enum ConstraintCategory {
    Relation,
    Domain,
}

// findDomainNotNullConstraint (pg_constraint.c): the validated NOTNULL row's
// oid (C returns the copied tuple; callers use only the oid).
pub fn findDomainNotNullConstraint<'mcx>(mcx: Mcx<'mcx>, typid: Oid) -> PgResult<Option<Oid>> {
    let con_rel = table::table_open(mcx, CONSTRAINT_RELATION_ID, AccessShareLock)?;
    // C keys only contypid (index column 2); the conrelid=0 prefix key is
    // added here so the scan stays a plain prefix scan (skip scan unported) —
    // domain constraints always carry conrelid=0, identical row set.
    let keys = [
        eq_key(Anum_pg_constraint_conrelid, F_OIDEQ, Datum::from_oid(InvalidOid)),
        eq_key(Anum_pg_constraint_contypid, F_OIDEQ, Datum::from_oid(typid)),
    ];
    let mut scan = genam::systable_beginscan(
        mcx,
        &con_rel,
        ConstraintRelidTypidNameIndexId,
        true,
        None,
        &keys,
    )?;
    let desc = con_rel.descr();
    let mut found = None;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let get = |anum: AttrNumber| {
            let mut isnull = false;
            // SAFETY: fixed NOT NULL pg_constraint columns under its descriptor.
            unsafe { types_tuple::heap_getattr(tup, anum as i32, desc, &mut isnull) }
        };
        if get(Anum_pg_constraint_contype).as_i8() as u8 != CONSTRAINT_NOTNULL {
            continue;
        }
        if !get(Anum_pg_constraint_convalidated).as_bool() {
            continue;
        }
        found = Some(get(Anum_pg_constraint_oid).as_oid());
        break;
    }
    genam::systable_endscan(mcx, scan)?;
    con_rel.close(AccessShareLock)?;
    Ok(found)
}

// get_domain_constraint_oid (pg_constraint.c).
pub fn get_domain_constraint_oid<'mcx>(
    mcx: Mcx<'mcx>,
    typid: Oid,
    conname: &str,
    missing_ok: bool,
) -> PgResult<Oid> {
    let con_rel = table::table_open(mcx, CONSTRAINT_RELATION_ID, AccessShareLock)?;
    let cname = name_arg(mcx, conname)?;
    let keys = [
        eq_key(Anum_pg_constraint_conrelid, F_OIDEQ, Datum::from_oid(InvalidOid)),
        eq_key(Anum_pg_constraint_contypid, F_OIDEQ, Datum::from_oid(typid)),
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
    let desc = con_rel.descr();
    let con_oid = match genam::systable_getnext(mcx, &mut scan)? {
        Some(tup) => {
            let mut isnull = false;
            // SAFETY: fixed NOT NULL pg_constraint oid column under its descriptor.
            unsafe {
                types_tuple::heap_getattr(tup, Anum_pg_constraint_oid as i32, desc, &mut isnull)
            }
            .as_oid()
        }
        None => InvalidOid,
    };
    genam::systable_endscan(mcx, scan)?;
    if con_oid == InvalidOid && !missing_ok {
        let typname = format_type::format_type_be(typid)?;
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!("constraint \"{conname}\" for domain {typname} does not exist"),
            )
            .with_sqlstate(types_error::ERRCODE_UNDEFINED_OBJECT),
        ));
    }
    con_rel.close(AccessShareLock)?;
    Ok(con_oid)
}

// AlterConstraintNamespaces (pg_constraint.c); objs_moved carries the
// caller's ObjectAddresses dedup set.
pub fn AlterConstraintNamespaces<'mcx>(
    mcx: Mcx<'mcx>,
    owner_id: Oid,
    old_nsp_id: Oid,
    new_nsp_id: Oid,
    is_type: bool,
    objs_moved: &mut PgVec<'mcx, ObjectAddress>,
) -> PgResult<()> {
    let con_rel = table::table_open(mcx, CONSTRAINT_RELATION_ID, RowExclusiveLock)?;
    let (relid, typid) = if is_type { (InvalidOid, owner_id) } else { (owner_id, InvalidOid) };
    let keys = [
        eq_key(Anum_pg_constraint_conrelid, F_OIDEQ, Datum::from_oid(relid)),
        eq_key(Anum_pg_constraint_contypid, F_OIDEQ, Datum::from_oid(typid)),
    ];
    let mut scan = genam::systable_beginscan(
        mcx,
        &con_rel,
        ConstraintRelidTypidNameIndexId,
        true,
        None,
        &keys,
    )?;
    let desc = con_rel.descr();
    let natts = desc.natts as usize;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let get = |anum: AttrNumber| {
            let mut isnull = false;
            // SAFETY: fixed NOT NULL pg_constraint columns under its descriptor.
            unsafe { types_tuple::heap_getattr(tup, anum as i32, desc, &mut isnull) }
        };
        let con_oid = get(Anum_pg_constraint_oid).as_oid();
        let thisobj = ObjectAddress::set(CONSTRAINT_RELATION_ID, con_oid);
        if objs_moved.iter().any(|a| a.classId == thisobj.classId && a.objectId == thisobj.objectId)
        {
            continue;
        }
        if get(Anum_pg_constraint_connamespace).as_oid() == old_nsp_id && old_nsp_id != new_nsp_id
        {
            let mut values: PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
            let mut nulls: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
            let mut replace: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
            values.resize(natts, Datum::null());
            nulls.resize(natts, false);
            replace.resize(natts, false);
            values[(Anum_pg_constraint_connamespace - 1) as usize] = Datum::from_oid(new_nsp_id);
            replace[(Anum_pg_constraint_connamespace - 1) as usize] = true;
            let mut newtup =
                heaptuple::heap_modify_tuple(mcx, tup, desc, &values, &nulls, &replace)?;
            let otid = tup.t_self;
            catalog_indexing::CatalogTupleUpdate(mcx, &con_rel, &otid, &mut newtup)?;
        }
        objs_moved.push(thisobj);
    }
    genam::systable_endscan(mcx, scan)?;
    con_rel.close(RowExclusiveLock)
}

pub fn ConstraintNameIsUsed<'mcx>(
    mcx: Mcx<'mcx>,
    con_cat: ConstraintCategory,
    obj_id: Oid,
    conname: &str,
) -> PgResult<bool> {
    let con_rel = table::table_open(mcx, CONSTRAINT_RELATION_ID, AccessShareLock)?;
    let cname = name_arg(mcx, conname)?;
    let (relid, typid) = match con_cat {
        ConstraintCategory::Relation => (obj_id, InvalidOid),
        ConstraintCategory::Domain => (InvalidOid, obj_id),
    };
    let keys = [
        eq_key(Anum_pg_constraint_conrelid, F_OIDEQ, Datum::from_oid(relid)),
        eq_key(Anum_pg_constraint_contypid, F_OIDEQ, Datum::from_oid(typid)),
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
    // SAFETY: as above.
    let contypid = unsafe {
        types_tuple::heap_getattr(tup, Anum_pg_constraint_contypid as i32, desc, &mut isnull)
    }
    .as_oid();
    if conrelid != InvalidOid {
        let rel = table::table_open(mcx, conrelid, types_rel::AccessExclusiveLock)?;
        if contype == CONSTRAINT_CHECK {
            decrement_relchecks(mcx, conrelid)?;
        }
        rel.close(types_rel::NoLock)?;
    } else if contypid == InvalidOid {
        panic!("constraint {con_id} is not of a known type");
    }
    let tid = tup.t_self;
    catalog_indexing::CatalogTupleDelete(&con_rel, &tid)?;
    genam::systable_endscan(mcx, scan)?;
    con_rel.close(RowExclusiveLock)
}

const Anum_pg_class_relchecks: AttrNumber = 20;
const Anum_pg_class_relname: AttrNumber = 2;

fn rel_name_for_error<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<String> {
    let pgrel = table::table_open(mcx, types_core::RELATION_RELATION_ID, AccessShareLock)?;
    let keys = [eq_key(1, F_OIDEQ, Datum::from_oid(relid))];
    let mut scan =
        genam::systable_beginscan(mcx, &pgrel, catalog::ClassOidIndexId, true, None, &keys)?;
    let reltup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for relation {relid}"));
    let mut isnull = false;
    // SAFETY: relname is a fixed NOT NULL NameData column under pg_class's descriptor.
    let d = unsafe {
        types_tuple::heap_getattr(reltup, Anum_pg_class_relname as i32, pgrel.descr(), &mut isnull)
    };
    // SAFETY: NameData column is a 64-byte NUL-terminated in-tuple buffer.
    let bytes =
        unsafe { core::slice::from_raw_parts(d.as_usize() as *const u8, NAMEDATALEN as usize) };
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(NAMEDATALEN as usize);
    let name = core::str::from_utf8(&bytes[..end]).expect("relname UTF-8").to_string();
    genam::systable_endscan(mcx, scan)?;
    pgrel.close(AccessShareLock)?;
    Ok(name)
}

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
    // C tries the unmodified label first UNLESS it's empty; an empty label
    // starts at "<label>1" (pg_constraint.c:867-871) — FK partition clones
    // (label "") are named parent_1, parent_2, never "parent_".
    let mut modlabel = if label.is_empty() {
        pass += 1;
        let mut m = mcx::PgString::from_str_in(label, mcx)?;
        use core::fmt::Write;
        write!(m, "{pass}").expect("label suffix");
        m
    } else {
        mcx::PgString::from_str_in(label, mcx)?
    };
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

// makeObjectName (indexcmds.c:2518-2577): truncate the longer of name1/name2
// (multibyte-aware) until "name1[_name2]_label" fits in NAMEDATALEN-1 bytes.
fn make_object_name<'mcx>(
    mcx: Mcx<'mcx>,
    name1: &str,
    name2: Option<&str>,
    label: &str,
) -> PgResult<mcx::PgString<'mcx>> {
    let mut overhead = label.len() + 1;
    if name2.is_some() {
        overhead += 1;
    }
    assert!(NAMEDATALEN as usize - 1 > overhead, "makeObjectName label too long ({label:?})");
    let availchars = NAMEDATALEN as usize - 1 - overhead;
    let mut name1chars = name1.len();
    let mut name2chars = name2.map_or(0, str::len);
    while name1chars + name2chars > availchars {
        if name1chars > name2chars {
            name1chars -= 1;
        } else {
            name2chars -= 1;
        }
    }
    name1chars = mbutils_seams::pg_mbcliplen::call(
        name1.as_bytes(),
        name1chars as i32,
        name1chars as i32,
    ) as usize;
    let mut s = mcx::PgString::from_str_in(&name1[..name1chars], mcx)?;
    if let Some(n2) = name2 {
        name2chars = mbutils_seams::pg_mbcliplen::call(
            n2.as_bytes(),
            name2chars as i32,
            name2chars as i32,
        ) as usize;
        s.try_push_str("_")?;
        s.try_push_str(&n2[..name2chars])?;
    }
    s.try_push_str("_")?;
    s.try_push_str(label)?;
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

// get_relation_constraint_oid / get_domain_constraint_oid (pg_constraint.c);
// both walk ConstraintRelidTypidNameIndexId with one of (conrelid, contypid)
// pinned to InvalidOid, matching conname in the loop.
fn constraint_oid_by_name<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    conname: &str,
) -> PgResult<Oid> {
    let con_rel = table::table_open(mcx, CONSTRAINT_RELATION_ID, AccessShareLock)?;
    let keys = [
        eq_key(Anum_pg_constraint_conrelid, F_OIDEQ, Datum::from_oid(relid)),
        eq_key(Anum_pg_constraint_contypid, F_OIDEQ, Datum::from_oid(InvalidOid)),
    ];
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
    Ok(found)
}

pub fn get_relation_constraint_oid<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    conname: &str,
    missing_ok: bool,
) -> PgResult<Oid> {
    let found = constraint_oid_by_name(mcx, relid, conname)?;
    if found == InvalidOid && !missing_ok {
        return Err(constraint_does_not_exist(mcx, relid, conname)?);
    }
    Ok(found)
}

#[cold]
#[inline(never)]
fn constraint_does_not_exist<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    conname: &str,
) -> PgResult<Box<PgError>> {
    let relname = lsyscache::relation::get_rel_name(mcx, relid)?
        .expect("constraint lookup relation has a pg_class row");
    Ok(Box::new(
        types_error::PgError::new(
            types_error::ERROR,
            format!("constraint \"{conname}\" for table \"{relname}\" does not exist"),
        )
        .with_sqlstate(types_error::ERRCODE_UNDEFINED_OBJECT),
    ))
}

// get_relation_constraint_attnos (pg_constraint.c): constraint oid + raw
// conkey attnums for the named relation constraint; C returns a Bitmapset
// offset by FirstLowInvalidHeapAttributeNumber — that offset is the caller's.
pub fn get_relation_constraint_attnos<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    conname: &str,
    missing_ok: bool,
) -> PgResult<(Oid, PgVec<'mcx, i16>)> {
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
    let mut constraint_oid = InvalidOid;
    let mut conattnos: PgVec<'mcx, i16> = PgVec::new_in(mcx);
    if let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let desc = con_rel.descr();
        constraint_oid = getattr(&con_rel, tup, Anum_pg_constraint_oid).0.as_oid();
        if let Some(img) = fk_array_image(mcx, tup, desc, Anum_pg_constraint_conkey)? {
            let mut out = [0i16; INDEX_MAX_KEYS];
            let n = fk_i16_array(&img, "conkey is not a 1-D smallint array", &mut out);
            for &attnum in &out[..n] {
                conattnos.push(attnum);
            }
        }
    }
    genam::systable_endscan(mcx, scan)?;
    con_rel.close(AccessShareLock)?;
    if constraint_oid == InvalidOid && !missing_ok {
        return Err(constraint_does_not_exist(mcx, relid, conname)?);
    }
    Ok((constraint_oid, conattnos))
}

// get_primary_key_attnos (pg_constraint.c:1450): the rel's PK column attnums
// and constraint OID, or None when there is no usable PK (a deferrable PK
// stops the search when deferrable_ok is false — a table has at most one PK).
pub fn get_primary_key_attnos<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    deferrable_ok: bool,
) -> PgResult<Option<(PgVec<'mcx, i16>, Oid)>> {
    let con_rel = table::table_open(mcx, CONSTRAINT_RELATION_ID, AccessShareLock)?;
    let keys = conrelid_scan_keys(relid);
    let mut scan = genam::systable_beginscan(
        mcx,
        &con_rel,
        ConstraintRelidTypidNameIndexId,
        true,
        None,
        &keys,
    )?;
    let mut result = None;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let contype = getattr(&con_rel, tup, Anum_pg_constraint_contype).0.as_i8() as u8;
        if contype != CONSTRAINT_PRIMARY {
            continue;
        }
        let condeferrable = getattr(&con_rel, tup, Anum_pg_constraint_condeferrable).0.as_bool();
        if condeferrable && !deferrable_ok {
            break;
        }
        let con_oid = getattr(&con_rel, tup, Anum_pg_constraint_oid).0.as_oid();
        let img = fk_array_image(mcx, tup, con_rel.descr(), Anum_pg_constraint_conkey)?
            .unwrap_or_else(|| panic!("null conkey for constraint {con_oid}"));
        let mut out = [0i16; INDEX_MAX_KEYS];
        let n = fk_i16_array(&img, "conkey is not a 1-D smallint array", &mut out);
        let mut pkattnos: PgVec<'mcx, i16> = PgVec::new_in(mcx);
        pkattnos.extend_from_slice(&out[..n]);
        result = Some((pkattnos, con_oid));
        break;
    }
    genam::systable_endscan(mcx, scan)?;
    con_rel.close(AccessShareLock)?;
    Ok(result)
}

// check_functional_grouping (pg_constraint.c:1740): can every column of relid
// be proven functionally dependent on grouping_columns? Only a non-deferrable
// PK that is a subset of the grouping columns qualifies; the proof's
// constraint OID is appended to constraint_deps.
pub fn check_functional_grouping<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    varno: i32,
    varlevelsup: types_core::Index,
    grouping_columns: &[types_nodes::Node<'mcx>],
    constraint_deps: &mut PgVec<'mcx, Oid>,
) -> PgResult<bool> {
    let Some((pkattnos, constraint_oid)) =
        syscache_seams::pg_constraint_primary_key_attnos::call(mcx, relid, false)?
    else {
        return Ok(false);
    };
    if pk_subset_of_grouping_columns(&pkattnos, varno, varlevelsup, grouping_columns) {
        constraint_deps.push(constraint_oid);
        return Ok(true);
    }
    Ok(false)
}

// The catalog-free core of check_functional_grouping: pkattnos ⊆ the attnos
// of grouping_columns Vars matching (varno, varlevelsup).
fn pk_subset_of_grouping_columns(
    pkattnos: &[i16],
    varno: i32,
    varlevelsup: types_core::Index,
    grouping_columns: &[types_nodes::Node<'_>],
) -> bool {
    pkattnos.iter().all(|&pkatt| {
        grouping_columns.iter().any(|gcol| {
            gcol.as_var().is_some_and(|gvar| {
                gvar.varno == varno && gvar.varlevelsup == varlevelsup && gvar.varattno == pkatt
            })
        })
    })
}

pub fn get_constraint_deferrability<'mcx>(mcx: Mcx<'mcx>, con_id: Oid) -> PgResult<(bool, bool)> {
    let con_rel = table::table_open(mcx, CONSTRAINT_RELATION_ID, AccessShareLock)?;
    let keys = [eq_key(Anum_pg_constraint_oid, F_OIDEQ, Datum::from_oid(con_id))];
    let mut scan =
        genam::systable_beginscan(mcx, &con_rel, CONSTRAINT_OID_INDEX_ID, true, None, &keys)?;
    let tup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for constraint {con_id}"));
    let deferrable = getattr(&con_rel, tup, Anum_pg_constraint_condeferrable).0.as_bool();
    let deferred = getattr(&con_rel, tup, Anum_pg_constraint_condeferred).0.as_bool();
    genam::systable_endscan(mcx, scan)?;
    con_rel.close(AccessShareLock)?;
    Ok((deferrable, deferred))
}

pub fn get_relation_idx_constraint_oid<'mcx>(
    mcx: Mcx<'mcx>,
    relation_id: Oid,
    index_id: Oid,
) -> PgResult<Oid> {
    let con_rel = table::table_open(mcx, CONSTRAINT_RELATION_ID, AccessShareLock)?;
    let keys = [eq_key(Anum_pg_constraint_conrelid, F_OIDEQ, Datum::from_oid(relation_id))];
    let mut scan = genam::systable_beginscan(
        mcx,
        &con_rel,
        ConstraintRelidTypidNameIndexId,
        true,
        None,
        &keys,
    )?;
    let mut constraint_id = InvalidOid;
    let desc = con_rel.descr();
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let mut isnull = false;
        // SAFETY (each): fixed NOT NULL pg_constraint columns under its
        // descriptor.
        let contype = unsafe {
            types_tuple::heap_getattr(tup, Anum_pg_constraint_contype as i32, desc, &mut isnull)
        }
        .as_i8() as u8;
        if contype != CONSTRAINT_PRIMARY && contype != CONSTRAINT_UNIQUE
            && contype != CONSTRAINT_EXCLUSION
        {
            continue;
        }
        // SAFETY: as above.
        let conindid = unsafe {
            types_tuple::heap_getattr(tup, Anum_pg_constraint_conindid as i32, desc, &mut isnull)
        }
        .as_oid();
        if conindid == index_id {
            // SAFETY: as above.
            constraint_id = unsafe {
                types_tuple::heap_getattr(tup, Anum_pg_constraint_oid as i32, desc, &mut isnull)
            }
            .as_oid();
            break;
        }
    }
    genam::systable_endscan(mcx, scan)?;
    con_rel.close(AccessShareLock)?;
    Ok(constraint_id)
}

// ConstraintSetParentConstraint, attach direction only (parent valid); the
// detach arm rides the DETACH PARTITION lane.
pub fn ConstraintSetParentConstraint<'mcx>(
    mcx: Mcx<'mcx>,
    child_constr_id: Oid,
    parent_constr_id: Oid,
    child_table_id: Oid,
) -> PgResult<()> {
    let con_rel = table::table_open(mcx, CONSTRAINT_RELATION_ID, RowExclusiveLock)?;
    let keys = [eq_key(Anum_pg_constraint_oid, F_OIDEQ, Datum::from_oid(child_constr_id))];
    let mut scan =
        genam::systable_beginscan(mcx, &con_rel, CONSTRAINT_OID_INDEX_ID, true, None, &keys)?;
    let tup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for constraint {child_constr_id}"));
    let desc = con_rel.descr();
    let mut isnull = false;
    // SAFETY: conparentid is a fixed NOT NULL pg_constraint column.
    let conparentid = unsafe {
        types_tuple::heap_getattr(tup, Anum_pg_constraint_conparentid as i32, desc, &mut isnull)
    }
    .as_oid();
    // SAFETY: coninhcount is a fixed NOT NULL pg_constraint column.
    let prior_inhcount = unsafe {
        types_tuple::heap_getattr(tup, Anum_pg_constraint_coninhcount as i32, desc, &mut isnull)
    }
    .as_i16();
    let natts = desc.natts as usize;
    let mut values: PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut nulls: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut replace: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    values.resize(natts, Datum::null());
    nulls.resize(natts, false);
    replace.resize(natts, false);
    let mut set = |anum: AttrNumber, v: Datum| {
        values[anum as usize - 1] = v;
        replace[anum as usize - 1] = true;
    };
    if parent_constr_id != InvalidOid {
        if conparentid != InvalidOid {
            panic!("constraint {child_constr_id} already has a parent constraint");
        }
        assert!(prior_inhcount == 0, "attach of constraint {child_constr_id} with coninhcount {prior_inhcount}");
        set(Anum_pg_constraint_conislocal, Datum::from_bool(false));
        set(Anum_pg_constraint_coninhcount, Datum::from_i16(1));
        set(Anum_pg_constraint_conparentid, Datum::from_oid(parent_constr_id));
    } else {
        assert!(prior_inhcount == 1, "detach of constraint {child_constr_id} with coninhcount {prior_inhcount}");
        set(Anum_pg_constraint_conislocal, Datum::from_bool(true));
        set(Anum_pg_constraint_coninhcount, Datum::from_i16(prior_inhcount - 1));
        set(Anum_pg_constraint_conparentid, Datum::from_oid(InvalidOid));
    }
    let mut newtup = heaptuple::heap_modify_tuple(mcx, tup, desc, &values, &nulls, &replace)?;
    let otid = tup.t_self;
    genam::systable_endscan(mcx, scan)?;
    catalog_indexing::CatalogTupleUpdate(mcx, &con_rel, &otid, &mut newtup)?;

    let depender = ObjectAddress::set(CONSTRAINT_RELATION_ID, child_constr_id);
    if parent_constr_id != InvalidOid {
        let parent = ObjectAddress::set(CONSTRAINT_RELATION_ID, parent_constr_id);
        pg_depend::recordDependencyOn(mcx, &depender, &parent, pg_depend::DependencyType::PartitionPri)?;
        let tbl = ObjectAddress::set(types_core::RELATION_RELATION_ID, child_table_id);
        pg_depend::recordDependencyOn(mcx, &depender, &tbl, pg_depend::DependencyType::PartitionSec)?;
    } else {
        pg_depend::deleteDependencyRecordsForClass(
            mcx,
            CONSTRAINT_RELATION_ID,
            child_constr_id,
            CONSTRAINT_RELATION_ID,
            pg_depend::DependencyType::PartitionPri,
        )?;
        pg_depend::deleteDependencyRecordsForClass(
            mcx,
            CONSTRAINT_RELATION_ID,
            child_constr_id,
            types_core::RELATION_RELATION_ID,
            pg_depend::DependencyType::PartitionSec,
        )?;
    }

    con_rel.close(RowExclusiveLock)
}

// extractNotNullColumn (pg_constraint.c), callable from external
// pg_constraint scans.
pub fn extractNotNullColumn<'mcx>(
    mcx: Mcx<'mcx>,
    tup: &types_tuple::HeapTupleData<'mcx>,
    desc: &types_tuple::TupleDescData<'mcx>,
) -> PgResult<AttrNumber> {
    extract_notnull_column(mcx, tup, desc)
}

// Single-row pg_constraint fixed-width field update by constraint OID.
pub fn update_constraint_fields<'mcx>(
    mcx: Mcx<'mcx>,
    con_id: Oid,
    fields: &[(AttrNumber, Datum)],
) -> PgResult<()> {
    let con_rel = table::table_open(mcx, CONSTRAINT_RELATION_ID, RowExclusiveLock)?;
    let keys = [eq_key(Anum_pg_constraint_oid, F_OIDEQ, Datum::from_oid(con_id))];
    let mut scan =
        genam::systable_beginscan(mcx, &con_rel, CONSTRAINT_OID_INDEX_ID, true, None, &keys)?;
    let tup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for constraint {con_id}"));
    let desc = con_rel.descr();
    let natts = desc.natts as usize;
    let mut values: PgVec<'_, Datum> = mcx::vec_from_elem_in(mcx, Datum::null(), natts);
    let nulls: PgVec<'_, bool> = mcx::vec_from_elem_in(mcx, false, natts);
    let mut replace: PgVec<'_, bool> = mcx::vec_from_elem_in(mcx, false, natts);
    for &(anum, v) in fields {
        values[anum as usize - 1] = v;
        replace[anum as usize - 1] = true;
    }
    let mut newtup = heaptuple::heap_modify_tuple(mcx, tup, desc, &values, &nulls, &replace)?;
    let otid = tup.t_self;
    genam::systable_endscan(mcx, scan)?;
    catalog_indexing::CatalogTupleUpdate(mcx, &con_rel, &otid, &mut newtup)?;
    con_rel.close(RowExclusiveLock)
}

// AdjustNotNullInheritance (pg_constraint.c): relname/attname supplied by the
// caller (C reads them via syscache getters).
pub fn AdjustNotNullInheritance<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    attnum: AttrNumber,
    new_conname: Option<&str>,
    is_local: bool,
    is_no_inherit: bool,
    is_notvalid: bool,
    relname: &str,
    attname: &str,
) -> PgResult<bool> {
    let Some(con) = findNotNullConstraintAttnum(mcx, relid, attnum)? else {
        return Ok(false);
    };
    if is_no_inherit != con.connoinherit {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!(
                    "cannot change NO INHERIT status of NOT NULL constraint \"{}\" on relation \"{relname}\"",
                    con.name_str()
                ),
            )
            .with_sqlstate(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .with_hint(
                "You might need to make the existing constraint inheritable using \
                 ALTER TABLE ... ALTER CONSTRAINT ... INHERIT.",
            ),
        ));
    }
    if !is_notvalid && !con.convalidated {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!(
                    "incompatible NOT VALID constraint \"{}\" on relation \"{relname}\"",
                    con.name_str()
                ),
            )
            .with_sqlstate(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .with_hint("You might need to validate it using ALTER TABLE ... VALIDATE CONSTRAINT."),
        ));
    }
    if is_local {
        if let Some(newname) = new_conname {
            if newname != con.name_str() {
                return Err(Box::new(
                    PgError::new(
                        ERROR,
                        format!(
                            "cannot create not-null constraint \"{newname}\" on column \
                             \"{attname}\" of table \"{relname}\""
                        ),
                    )
                    .with_sqlstate(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                    .with_detail(format!(
                        "A not-null constraint named \"{}\" already exists for this column.",
                        con.name_str()
                    )),
                ));
            }
        }
    }
    if !is_local {
        if con.coninhcount == i16::MAX {
            return Err(Box::new(
                PgError::new(ERROR, "too many inheritance parents".to_string())
                    .with_sqlstate(types_error::ERRCODE_PROGRAM_LIMIT_EXCEEDED),
            ));
        }
        update_constraint_fields(
            mcx,
            con.oid,
            &[(Anum_pg_constraint_coninhcount, Datum::from_i16(con.coninhcount + 1))],
        )?;
    } else if !con.conislocal {
        update_constraint_fields(
            mcx,
            con.oid,
            &[(Anum_pg_constraint_conislocal, Datum::from_bool(true))],
        )?;
    }
    Ok(true)
}

pub const INDEX_MAX_KEYS: usize = types_core::fmgr::INDEX_MAX_KEYS as usize;
const ARR_1D_HDRSZ: usize = 24;

pub struct FkConstraintArrays {
    pub numfks: usize,
    pub conkey: [i16; INDEX_MAX_KEYS],
    pub confkey: [i16; INDEX_MAX_KEYS],
    pub pf_eq_oprs: [Oid; INDEX_MAX_KEYS],
    pub pp_eq_oprs: [Oid; INDEX_MAX_KEYS],
    pub ff_eq_oprs: [Oid; INDEX_MAX_KEYS],
    pub num_fk_del_set_cols: usize,
    pub fk_del_set_cols: [i16; INDEX_MAX_KEYS],
}

fn fk_array_image<'mcx>(
    mcx: Mcx<'mcx>,
    tup: &types_tuple::HeapTupleData<'_>,
    desc: &types_tuple::TupleDescData<'_>,
    attnum: AttrNumber,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    let mut isnull = false;
    // SAFETY: varlena pg_constraint column under its descriptor.
    let d = unsafe { types_tuple::heap_getattr(tup, attnum as i32, desc, &mut isnull) };
    if isnull {
        return Ok(None);
    }
    let p = d.as_usize() as *const u8;
    // SAFETY: live varlena image through its extent.
    let image = unsafe { core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) };
    let payload = varlena::open_image(mcx, image)?;
    // DatumGetArrayTypeP: rebuild the 4B-header form (image may be packed).
    let body = payload.as_bytes();
    let total = body.len() + 4;
    let mut full: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, total)?;
    mcx::vec_append_bytes(&mut full, &(((total as u32) << 2).to_ne_bytes()))?;
    mcx::vec_append_bytes(&mut full, body)?;
    Ok(Some(full))
}

fn fk_array_nelems(img: &[u8], elemtype: Oid, errmsg: &str) -> usize {
    let rd = |off: usize| i32::from_ne_bytes(img[off..off + 4].try_into().unwrap());
    assert!(
        img.len() >= ARR_1D_HDRSZ && rd(4) == 1 && rd(8) == 0 && rd(12) as u32 == elemtype,
        "{errmsg}"
    );
    rd(16) as usize
}

fn fk_i16_array(img: &[u8], errmsg: &str, out: &mut [i16; INDEX_MAX_KEYS]) -> usize {
    let n = fk_array_nelems(img, INT2OID, errmsg);
    for (i, o) in out.iter_mut().enumerate().take(n) {
        let off = ARR_1D_HDRSZ + 2 * i;
        *o = i16::from_ne_bytes(img[off..off + 2].try_into().unwrap());
    }
    n
}

fn fk_oid_array(img: &[u8], errmsg: &str, out: &mut [Oid; INDEX_MAX_KEYS]) -> usize {
    let n = fk_array_nelems(img, types_core::OIDOID, errmsg);
    for (i, o) in out.iter_mut().enumerate().take(n) {
        let off = ARR_1D_HDRSZ + 4 * i;
        *o = u32::from_ne_bytes(img[off..off + 4].try_into().unwrap());
    }
    n
}

pub fn DeconstructFkConstraintRow<'mcx>(
    mcx: Mcx<'mcx>,
    tup: &types_tuple::HeapTupleData<'_>,
    desc: &types_tuple::TupleDescData<'_>,
) -> PgResult<FkConstraintArrays> {
    let mut out = FkConstraintArrays {
        numfks: 0,
        conkey: [0; INDEX_MAX_KEYS],
        confkey: [0; INDEX_MAX_KEYS],
        pf_eq_oprs: [InvalidOid; INDEX_MAX_KEYS],
        pp_eq_oprs: [InvalidOid; INDEX_MAX_KEYS],
        ff_eq_oprs: [InvalidOid; INDEX_MAX_KEYS],
        num_fk_del_set_cols: 0,
        fk_del_set_cols: [0; INDEX_MAX_KEYS],
    };

    let req = |attnum: AttrNumber, name: &str| -> PgResult<PgVec<'mcx, u8>> {
        Ok(fk_array_image(mcx, tup, desc, attnum)?
            .unwrap_or_else(|| panic!("unexpected null {name} in pg_constraint tuple")))
    };

    let conkey = req(Anum_pg_constraint_conkey, "conkey")?;
    let numkeys = fk_i16_array(&conkey, "conkey is not a 1-D smallint array", &mut out.conkey);
    assert!(
        numkeys > 0 && numkeys <= INDEX_MAX_KEYS,
        "foreign key constraint cannot have {numkeys} columns"
    );
    out.numfks = numkeys;

    let confkey = req(Anum_pg_constraint_confkey, "confkey")?;
    let bad_confkey = "confkey is not a 1-D smallint array";
    assert!(fk_i16_array(&confkey, bad_confkey, &mut out.confkey) == numkeys, "{bad_confkey}");

    for (attnum, name, slot) in [
        (Anum_pg_constraint_conpfeqop, "conpfeqop", &mut out.pf_eq_oprs),
        (Anum_pg_constraint_conppeqop, "conppeqop", &mut out.pp_eq_oprs),
        (Anum_pg_constraint_conffeqop, "conffeqop", &mut out.ff_eq_oprs),
    ] {
        let img = req(attnum, name)?;
        let bad = format!("{name} is not a 1-D Oid array");
        assert!(fk_oid_array(&img, &bad, slot) == numkeys, "{bad}");
    }

    if let Some(img) = fk_array_image(mcx, tup, desc, Anum_pg_constraint_confdelsetcols)? {
        out.num_fk_del_set_cols = fk_i16_array(
            &img,
            "confdelsetcols is not a 1-D smallint array",
            &mut out.fk_del_set_cols,
        );
    }

    Ok(out)
}

// pg_operator.dat
const OID_RANGE_INTERSECT_RANGE_OP: Oid = 3900;
const OID_MULTIRANGE_INTERSECT_MULTIRANGE_OP: Oid = 4394;

// GetOperatorFromCompareType (indexcmds.c), FindFKPeriodOpers callers' slice:
// the DDL-time lookup in DefineIndex keeps its own copy (richer errors);
// runtime RI lookups land here below the tablecmds/indexcmds cycle.
fn operator_from_compare_type(
    opclass: Oid,
    rhstype: Oid,
    cmptype: lsyscache::CompareType,
) -> PgResult<(Oid, u16)> {
    let amid = lsyscache::get_opclass_method(opclass)?;
    let (opfamily, opcintype) = lsyscache::get_opclass_opfamily_and_input_type(opclass)?
        .unwrap_or_else(|| panic!("cache lookup failed for opclass {opclass}"));
    let cannot_identify = |opcintype: Oid| -> PgResult<Box<PgError>> {
        let what = match cmptype {
            lsyscache::COMPARE_EQ => "an equality",
            lsyscache::COMPARE_OVERLAP => "an overlaps",
            _ => "a contained-by",
        };
        Ok(Box::new(
            PgError::error(format!(
                "could not identify {what} operator for type {}",
                format_type::format_type_be(opcintype)?
            ))
            .with_sqlstate(types_error::ERRCODE_UNDEFINED_OBJECT),
        ))
    };
    let strat = amapi::IndexAmTranslateCompareType(cmptype, amid, opfamily, true)?;
    if strat == 0 {
        return Err(cannot_identify(opcintype)?);
    }
    let rhstype = if rhstype == InvalidOid { opcintype } else { rhstype };
    let opid = lsyscache::get_opfamily_member(opfamily, opcintype, rhstype, strat as i16)?;
    if opid == InvalidOid {
        return Err(cannot_identify(opcintype)?);
    }
    Ok((opid, strat))
}

// FindFKPeriodOpers (pg_constraint.c): (containedby, agged containedby,
// intersect) operators for the PERIOD part of a temporal foreign key.
pub fn FindFKPeriodOpers(opclass: Oid) -> PgResult<(Oid, Oid, Oid)> {
    let (_, opcintype) = lsyscache::get_opclass_opfamily_and_input_type(opclass)?
        .unwrap_or_else(|| panic!("cache lookup failed for opclass {opclass}"));
    if opcintype != types_core::ANYRANGEOID && opcintype != types_core::ANYMULTIRANGEOID {
        return Err(Box::new(
            PgError::error("invalid type for PERIOD part of foreign key".to_string())
                .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
                .with_detail("Only range and multirange are supported.".to_string()),
        ));
    }
    let (containedbyoperoid, _) =
        operator_from_compare_type(opclass, InvalidOid, lsyscache::COMPARE_CONTAINED_BY)?;
    let (aggedcontainedbyoperoid, _) = operator_from_compare_type(
        opclass,
        types_core::ANYMULTIRANGEOID,
        lsyscache::COMPARE_CONTAINED_BY,
    )?;
    let intersectoperoid = match opcintype {
        types_core::ANYRANGEOID => OID_RANGE_INTERSECT_RANGE_OP,
        _ => OID_MULTIRANGE_INTERSECT_MULTIRANGE_OP,
    };
    Ok((containedbyoperoid, aggedcontainedbyoperoid, intersectoperoid))
}

#[cfg(test)]
mod tests {
    use super::pk_subset_of_grouping_columns;
    use mcx::MemoryContext;
    use types_core::catalog::INT4OID;
    use types_core::InvalidOid;
    use types_nodes::Node;

    #[test]
    fn pk_subset_matches_only_full_pk_at_varno_level() {
        let cx = MemoryContext::new("t");
        let mcx = cx.mcx();
        // GROUP BY t1.a, t1.b, t2.a  (varno 1 attnos {1,2}; varno 2 attno 1)
        let cols = [
            Node::mk_var(mcx, 1, 1, INT4OID, -1, InvalidOid, 0).unwrap(),
            Node::mk_var(mcx, 1, 2, INT4OID, -1, InvalidOid, 0).unwrap(),
            Node::mk_var(mcx, 2, 1, INT4OID, -1, InvalidOid, 0).unwrap(),
        ];
        // PK(a) and PK(a,b) of varno 1 are covered.
        assert!(pk_subset_of_grouping_columns(&[1], 1, 0, &cols));
        assert!(pk_subset_of_grouping_columns(&[1, 2], 1, 0, &cols));
        // Partial coverage fails: PK(a,b,c) of varno 1, PK(a,b) of varno 2.
        assert!(!pk_subset_of_grouping_columns(&[1, 2, 3], 1, 0, &cols));
        assert!(!pk_subset_of_grouping_columns(&[1, 2], 2, 0, &cols));
        // A grouping Var of another level does not count.
        let upper = [Node::mk_var(mcx, 1, 1, INT4OID, -1, InvalidOid, 1).unwrap()];
        assert!(!pk_subset_of_grouping_columns(&[1], 1, 0, &upper));
        assert!(pk_subset_of_grouping_columns(&[1], 1, 1, &upper));
    }
}

// Registered from seams_init (the parser sits above this crate only through
// the projection seam).
pub fn init_seams() {
    syscache_seams::pg_constraint_primary_key_attnos::set(get_primary_key_attnos);
}
mod truncation_tests {
    static SETUP: std::sync::Once = std::sync::Once::new();

    fn setup() {
        SETUP.call_once(|| {
            // UTF-8 boundary clip, enough for the test inputs.
            mbutils_seams::pg_mbcliplen::set(|s, len, limit| {
                let mut l = (limit as usize).min(len as usize);
                while l > 0 && l < s.len() && s[l] & 0xC0 == 0x80 {
                    l -= 1;
                }
                l as i32
            });
        });
    }

    // foreign_key.out oracle: three FKs on fktable2 over a 51-char column.
    #[test]
    fn make_object_name_matches_c_truncation() {
        setup();
        let ctx = mcx::MemoryContext::new("t");
        let long = "very_very_long_column_name_to_exceed_63_characters";
        let a_long = format!("a_{long}");
        for (name2, label, want) in [
            (long, "fkey", "fktable2_very_very_long_column_name_to_exceed_63_character_fkey"),
            (a_long.as_str(), "fkey", "fktable2_a_very_very_long_column_name_to_exceed_63_charact_fkey"),
            (a_long.as_str(), "fkey1", "fktable2_a_very_very_long_column_name_to_exceed_63_charac_fkey1"),
        ] {
            let got = super::make_object_name(ctx.mcx(), "fktable2", Some(name2), label).unwrap();
            assert_eq!(got.as_str(), want);
            assert!(got.len() < super::NAMEDATALEN as usize);
        }
    }
}
