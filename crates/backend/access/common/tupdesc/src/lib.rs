#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use core::alloc::Layout;
use core::cell::Cell;
use std::rc::Rc;

use ::datum::Datum;
use ::mcx::{alloc_in, vec_with_capacity_in, Allocator, Mcx, PgString, PgVec};
use ::types_core::{
    AttrNumber, InvalidOid, Oid, BOOLOID, DEFAULT_COLLATION_OID, FLOAT8PASSBYVAL, INT4OID, INT8OID,
    OIDOID, RECORDOID, TEXTARRAYOID, TEXTOID,
};
use ::types_error::{PgError, PgResult};
use ::types_tuple::varatt::varsize_any;
use ::types_tuple::{
    AttrDefault, AttrMissing, CompactAttribute, ConstrCheck, FormData_pg_attribute,
    InvalidCompressionMethod, NameData, PgTypeShape, TupleConstr, TupleDescData,
    ATTNULLABLE_VALID, MAXIMUM_ALIGNOF, TYPALIGN_CHAR, TYPALIGN_DOUBLE, TYPALIGN_INT,
    TYPSTORAGE_EXTENDED, TYPSTORAGE_PLAIN,
};

#[cfg(test)]
mod tests;

pub fn CreateTemplateTupleDesc<'mcx>(mcx: Mcx<'mcx>, natts: i32) -> PgResult<TupleDescData<'mcx>> {
    debug_assert!(natts >= 0);
    let n = natts as usize;
    let mut compact_attrs: PgVec<'mcx, CompactAttribute> = vec_with_capacity_in(mcx, n)?;
    let mut attrs: PgVec<'mcx, FormData_pg_attribute> = vec_with_capacity_in(mcx, n)?;
    for _ in 0..n {
        // C leaves the trailing arrays uninitialized; -1 keeps an unpopulated
        // entry from reading as a cached deform offset.
        compact_attrs.push(CompactAttribute {
            attcacheoff: Cell::new(-1),
            ..CompactAttribute::default()
        });
        attrs.push(FormData_pg_attribute::default());
    }
    Ok(TupleDescData {
        natts,
        tdtypeid: RECORDOID,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs,
        attrs,
    })
}

pub fn CreateTupleDesc<'mcx>(
    mcx: Mcx<'mcx>,
    attrs: &[FormData_pg_attribute],
) -> PgResult<TupleDescData<'mcx>> {
    let mut desc = CreateTemplateTupleDesc(mcx, attrs.len() as i32)?;
    desc.attrs.copy_from_slice(attrs);
    for i in 0..attrs.len() {
        populate_compact_attribute(&mut desc, i);
    }
    Ok(desc)
}

pub fn populate_compact_attribute(tupdesc: &mut TupleDescData<'_>, attnum: usize) {
    let src = &tupdesc.attrs[attnum];
    let mut dst = CompactAttribute::populate_from(src);
    if src.attnotnull && catalog_seams::is_catalog_relation_oid::call(src.attrelid) {
        dst.attnullability = ATTNULLABLE_VALID;
    }
    tupdesc.compact_attrs[attnum] = dst;
}

pub fn verify_compact_attribute(tupdesc: &TupleDescData<'_>, attnum: usize) {
    #[cfg(debug_assertions)]
    {
        let cattr = &tupdesc.compact_attrs[attnum];
        let tmp = CompactAttribute::populate_from(&tupdesc.attrs[attnum]);
        // attcacheoff/attnullability are stateful; match them before comparing.
        tmp.attcacheoff.set(cattr.attcacheoff.get());
        let tmp = CompactAttribute {
            attnullability: cattr.attnullability,
            ..tmp
        };
        assert!(tmp == *cattr, "stale CompactAttribute for attnum {attnum}");
    }
    #[cfg(not(debug_assertions))]
    let _ = (tupdesc, attnum);
}

pub fn CreateTupleDescCopy<'mcx>(
    mcx: Mcx<'mcx>,
    tupdesc: &TupleDescData<'_>,
) -> PgResult<TupleDescData<'mcx>> {
    CreateTupleDescTruncatedCopy(mcx, tupdesc, tupdesc.natts)
}

pub fn CreateTupleDescTruncatedCopy<'mcx>(
    mcx: Mcx<'mcx>,
    tupdesc: &TupleDescData<'_>,
    natts: i32,
) -> PgResult<TupleDescData<'mcx>> {
    debug_assert!(natts <= tupdesc.natts);
    let mut desc = CreateTemplateTupleDesc(mcx, natts)?;
    desc.attrs.copy_from_slice(&tupdesc.attrs[..natts as usize]);
    for i in 0..natts as usize {
        clear_constraint_fields(desc.attr_mut(i));
        populate_compact_attribute(&mut desc, i);
    }
    desc.tdtypeid = tupdesc.tdtypeid;
    desc.tdtypmod = tupdesc.tdtypmod;
    Ok(desc)
}

pub fn CreateTupleDescCopyConstr<'mcx>(
    mcx: Mcx<'mcx>,
    tupdesc: &TupleDescData<'_>,
) -> PgResult<TupleDescData<'mcx>> {
    let mut desc = CreateTemplateTupleDesc(mcx, tupdesc.natts)?;
    desc.attrs.copy_from_slice(&tupdesc.attrs);
    for i in 0..desc.natts as usize {
        populate_compact_attribute(&mut desc, i);
        desc.compact_attrs[i].attnullability = tupdesc.compact_attrs[i].attnullability;
    }

    if let Some(constr) = tupdesc.constr.as_deref() {
        let mut defval: PgVec<'mcx, AttrDefault<'mcx>> =
            vec_droppy_with_capacity(mcx, constr.defval.len())?;
        for d in constr.defval.iter() {
            defval.push(AttrDefault {
                adnum: d.adnum,
                adbin: clone_opt_string(mcx, &d.adbin)?,
            });
        }

        let mut missing: PgVec<'mcx, AttrMissing> =
            vec_with_capacity_in(mcx, constr.missing.len())?;
        for (i, m) in constr.missing.iter().enumerate() {
            let am_value = if m.am_present {
                let cattr = tupdesc.compact_attr(i);
                datum_copy_in(mcx, m.am_value, cattr.attbyval, cattr.attlen)?
            } else {
                m.am_value
            };
            missing.push(AttrMissing {
                am_present: m.am_present,
                am_value,
            });
        }

        let mut check: PgVec<'mcx, ConstrCheck<'mcx>> =
            vec_droppy_with_capacity(mcx, constr.check.len())?;
        for c in constr.check.iter() {
            check.push(ConstrCheck {
                ccname: clone_opt_string(mcx, &c.ccname)?,
                ccbin: clone_opt_string(mcx, &c.ccbin)?,
                ccenforced: c.ccenforced,
                ccvalid: c.ccvalid,
                ccnoinherit: c.ccnoinherit,
            });
        }

        desc.constr = Some(alloc_in(
            mcx,
            TupleConstr {
                defval,
                check,
                missing,
                num_defval: constr.num_defval,
                num_check: constr.num_check,
                has_not_null: constr.has_not_null,
                has_generated_stored: constr.has_generated_stored,
                has_generated_virtual: constr.has_generated_virtual,
            },
        )?);
    }

    desc.tdtypeid = tupdesc.tdtypeid;
    desc.tdtypmod = tupdesc.tdtypmod;
    Ok(desc)
}

pub fn TupleDescCopy(dst: &mut TupleDescData<'_>, src: &TupleDescData<'_>) {
    assert_eq!(dst.natts, src.natts);
    dst.tdtypeid = src.tdtypeid;
    dst.tdtypmod = src.tdtypmod;
    dst.attrs.copy_from_slice(&src.attrs);
    for i in 0..dst.natts as usize {
        clear_constraint_fields(dst.attr_mut(i));
        populate_compact_attribute(dst, i);
    }
    dst.constr = None;
    dst.tdrefcount = -1;
}

pub fn TupleDescCopyEntry(
    dst: &mut TupleDescData<'_>,
    dstAttno: AttrNumber,
    src: &TupleDescData<'_>,
    srcAttno: AttrNumber,
) {
    debug_assert!(srcAttno >= 1 && (srcAttno as i32) <= src.natts);
    debug_assert!(dstAttno >= 1 && (dstAttno as i32) <= dst.natts);
    let i = dstAttno as usize - 1;
    *dst.attr_mut(i) = src.attrs[srcAttno as usize - 1];
    let att = dst.attr_mut(i);
    att.attnum = dstAttno;
    clear_constraint_fields(att);
    populate_compact_attribute(dst, i);
}

pub fn FreeTupleDesc(tupdesc: TupleDescData<'_>) {
    debug_assert!(tupdesc.tdrefcount <= 0);
    // Drop reclaims constr/defval/missing/check + both attribute arrays; the
    // by-ref missing datum images free with their context.
    drop(tupdesc);
}

// C refcounts through tdrefcount + resource owner; here the Rc strong count is
// the refcount and tdrefcount >= 0 only marks a descriptor as refcounted.
pub fn IncrTupleDescRefCount<'mcx>(tupdesc: &Rc<TupleDescData<'mcx>>) -> Rc<TupleDescData<'mcx>> {
    debug_assert!(tupdesc.tdrefcount >= 0);
    Rc::clone(tupdesc)
}

pub fn DecrTupleDescRefCount(tupdesc: Rc<TupleDescData<'_>>) {
    debug_assert!(tupdesc.tdrefcount >= 0);
    drop(tupdesc);
}

pub fn equalTupleDescs(tupdesc1: &TupleDescData<'_>, tupdesc2: &TupleDescData<'_>) -> bool {
    if tupdesc1.natts != tupdesc2.natts || tupdesc1.tdtypeid != tupdesc2.tdtypeid {
        return false;
    }

    for i in 0..tupdesc1.natts as usize {
        let a1 = tupdesc1.attr(i);
        let a2 = tupdesc2.attr(i);
        // attrelid/attnum placed the rows; atthasmissing is not represented in
        // tupdescs — all three intentionally ignored (C parity).
        if a1.attname.name_str() != a2.attname.name_str()
            || a1.atttypid != a2.atttypid
            || a1.attlen != a2.attlen
            || a1.attndims != a2.attndims
            || a1.atttypmod != a2.atttypmod
            || a1.attbyval != a2.attbyval
            || a1.attalign != a2.attalign
            || a1.attstorage != a2.attstorage
            || a1.attcompression != a2.attcompression
            || a1.attnotnull != a2.attnotnull
        {
            return false;
        }
        if a1.attnotnull
            && tupdesc1.compact_attr(i).attnullability != tupdesc2.compact_attr(i).attnullability
        {
            return false;
        }
        if a1.atthasdef != a2.atthasdef
            || a1.attidentity != a2.attidentity
            || a1.attgenerated != a2.attgenerated
            || a1.attisdropped != a2.attisdropped
            || a1.attislocal != a2.attislocal
            || a1.attinhcount != a2.attinhcount
            || a1.attcollation != a2.attcollation
        {
            return false;
        }
    }

    match (tupdesc1.constr.as_deref(), tupdesc2.constr.as_deref()) {
        (None, None) => true,
        (Some(c1), Some(c2)) => {
            if c1.has_not_null != c2.has_not_null
                || c1.has_generated_stored != c2.has_generated_stored
                || c1.has_generated_virtual != c2.has_generated_virtual
                || c1.num_defval != c2.num_defval
            {
                return false;
            }
            for i in 0..c1.num_defval as usize {
                if c1.defval[i].adnum != c2.defval[i].adnum
                    || opt_bytes(&c1.defval[i].adbin) != opt_bytes(&c2.defval[i].adbin)
                {
                    return false;
                }
            }
            match (!c1.missing.is_empty(), !c2.missing.is_empty()) {
                (false, false) => {}
                (true, true) => {
                    for i in 0..tupdesc1.natts as usize {
                        let m1 = &c1.missing[i];
                        let m2 = &c2.missing[i];
                        if m1.am_present != m2.am_present {
                            return false;
                        }
                        if m1.am_present {
                            let cattr = tupdesc1.compact_attr(i);
                            if !datum_is_equal(m1.am_value, m2.am_value, cattr.attbyval, cattr.attlen)
                            {
                                return false;
                            }
                        }
                    }
                }
                _ => return false,
            }
            if c1.num_check != c2.num_check {
                return false;
            }
            // Relies on ConstrCheck entries being sorted by name (C parity).
            for i in 0..c1.num_check as usize {
                let k1 = &c1.check[i];
                let k2 = &c2.check[i];
                if opt_bytes(&k1.ccname) != opt_bytes(&k2.ccname)
                    || opt_bytes(&k1.ccbin) != opt_bytes(&k2.ccbin)
                    || k1.ccenforced != k2.ccenforced
                    || k1.ccvalid != k2.ccvalid
                    || k1.ccnoinherit != k2.ccnoinherit
                {
                    return false;
                }
            }
            true
        }
        _ => false,
    }
}

pub fn equalRowTypes(tupdesc1: &TupleDescData<'_>, tupdesc2: &TupleDescData<'_>) -> bool {
    if tupdesc1.natts != tupdesc2.natts || tupdesc1.tdtypeid != tupdesc2.tdtypeid {
        return false;
    }
    for i in 0..tupdesc1.natts as usize {
        let a1 = tupdesc1.attr(i);
        let a2 = tupdesc2.attr(i);
        if a1.attname.name_str() != a2.attname.name_str()
            || a1.atttypid != a2.atttypid
            || a1.atttypmod != a2.atttypmod
            || a1.attcollation != a2.attcollation
            || a1.attisdropped != a2.attisdropped
        {
            return false;
        }
    }
    true
}

pub fn hashRowType(desc: &TupleDescData<'_>) -> u32 {
    let mut s = hashfn::hash_combine(0, hashfn::hash_bytes_uint32(desc.natts as u32));
    s = hashfn::hash_combine(s, hashfn::hash_bytes_uint32(desc.tdtypeid));
    for i in 0..desc.natts as usize {
        s = hashfn::hash_combine(s, hashfn::hash_bytes_uint32(desc.attr(i).atttypid));
    }
    s
}

pub fn TupleDescInitEntry(
    desc: &mut TupleDescData<'_>,
    attributeNumber: AttrNumber,
    attributeName: Option<&str>,
    oidtypeid: Oid,
    typmod: i32,
    attdim: i32,
) -> PgResult<()> {
    // C looks up pg_type after the field writes; the error-path partial entry
    // is never observed, so lookup-first is equivalent.
    let shape = syscache_seams::lookup_pg_type_shape::call(oidtypeid)?
        .ok_or_else(|| type_lookup_failed(oidtypeid))?;
    init_entry(desc, attributeNumber, attributeName, oidtypeid, typmod, attdim, shape);
    populate_compact_attribute(desc, attributeNumber as usize - 1);
    Ok(())
}

pub fn TupleDescInitBuiltinEntry(
    desc: &mut TupleDescData<'_>,
    attributeNumber: AttrNumber,
    attributeName: &str,
    oidtypeid: Oid,
    typmod: i32,
    attdim: i32,
) -> PgResult<()> {
    let shape = builtin_type_shape(oidtypeid)?;
    init_entry(desc, attributeNumber, Some(attributeName), oidtypeid, typmod, attdim, shape);
    populate_compact_attribute(desc, attributeNumber as usize - 1);
    Ok(())
}

pub fn TupleDescInitEntryCollation(
    desc: &mut TupleDescData<'_>,
    attributeNumber: AttrNumber,
    collationid: Oid,
) {
    debug_assert!(attributeNumber >= 1 && (attributeNumber as i32) <= desc.natts);
    desc.attr_mut(attributeNumber as usize - 1).attcollation = collationid;
}

pub fn BuildDescFromLists<'mcx>(
    mcx: Mcx<'mcx>,
    names: &[&str],
    types: &[Oid],
    typmods: &[i32],
    collations: &[Oid],
) -> PgResult<TupleDescData<'mcx>> {
    let natts = names.len();
    debug_assert!(types.len() == natts && typmods.len() == natts && collations.len() == natts);
    let mut desc = CreateTemplateTupleDesc(mcx, natts as i32)?;
    for i in 0..natts {
        let attnum = (i + 1) as AttrNumber;
        TupleDescInitEntry(&mut desc, attnum, Some(names[i]), types[i], typmods[i], 0)?;
        TupleDescInitEntryCollation(&mut desc, attnum, collations[i]);
    }
    Ok(desc)
}

// TupleDescGetDefault minus stringToNode (nodes/read.c unported); the Node
// parse layers on when the read unit lands.
pub fn TupleDescGetDefaultBin<'a, 'mcx>(
    tupdesc: &'a TupleDescData<'mcx>,
    attnum: AttrNumber,
) -> Option<&'a PgString<'mcx>> {
    let constr = tupdesc.constr.as_deref()?;
    constr.defval[..constr.num_defval as usize]
        .iter()
        .find(|d| d.adnum == attnum)
        .and_then(|d| d.adbin.as_ref())
}

fn init_entry(
    desc: &mut TupleDescData<'_>,
    attributeNumber: AttrNumber,
    attributeName: Option<&str>,
    oidtypeid: Oid,
    typmod: i32,
    attdim: i32,
    shape: PgTypeShape,
) {
    debug_assert!(attributeNumber >= 1 && (attributeNumber as i32) <= desc.natts);
    debug_assert!((0..=i16::MAX as i32).contains(&attdim));
    let att = desc.attr_mut(attributeNumber as usize - 1);

    att.attrelid = InvalidOid;
    match attributeName {
        Some(name) => att.attname.namestrcpy(name),
        None => att.attname = NameData::default(),
    }
    att.atttypmod = typmod;
    att.attnum = attributeNumber;
    att.attndims = attdim as i16;
    att.attnotnull = false;
    att.atthasdef = false;
    att.atthasmissing = false;
    att.attidentity = 0;
    att.attgenerated = 0;
    att.attisdropped = false;
    att.attislocal = true;
    att.attinhcount = 0;

    att.atttypid = oidtypeid;
    att.attlen = shape.typlen;
    att.attbyval = shape.typbyval;
    att.attalign = shape.typalign;
    att.attstorage = shape.typstorage;
    att.attcompression = InvalidCompressionMethod;
    att.attcollation = shape.typcollation;
}

fn builtin_type_shape(oidtypeid: Oid) -> PgResult<PgTypeShape> {
    match oidtypeid {
        TEXTOID | TEXTARRAYOID => Ok(PgTypeShape {
            typlen: -1,
            typbyval: false,
            typalign: TYPALIGN_INT,
            typstorage: TYPSTORAGE_EXTENDED,
            typcollation: DEFAULT_COLLATION_OID,
        }),
        BOOLOID => Ok(PgTypeShape {
            typlen: 1,
            typbyval: true,
            typalign: TYPALIGN_CHAR,
            typstorage: TYPSTORAGE_PLAIN,
            typcollation: InvalidOid,
        }),
        INT4OID => Ok(PgTypeShape {
            typlen: 4,
            typbyval: true,
            typalign: TYPALIGN_INT,
            typstorage: TYPSTORAGE_PLAIN,
            typcollation: InvalidOid,
        }),
        INT8OID => Ok(PgTypeShape {
            typlen: 8,
            typbyval: FLOAT8PASSBYVAL != 0,
            typalign: TYPALIGN_DOUBLE,
            typstorage: TYPSTORAGE_PLAIN,
            typcollation: InvalidOid,
        }),
        OIDOID => Ok(PgTypeShape {
            typlen: 4,
            typbyval: true,
            typalign: TYPALIGN_INT,
            typstorage: TYPSTORAGE_PLAIN,
            typcollation: InvalidOid,
        }),
        _ => Err(unsupported_type(oidtypeid)),
    }
}

fn clear_constraint_fields(att: &mut FormData_pg_attribute) {
    att.attnotnull = false;
    att.atthasdef = false;
    att.atthasmissing = false;
    att.attidentity = 0;
    att.attgenerated = 0;
}

// vec_with_capacity_in forbids droppy T; AttrDefault/ConstrCheck own PgStrings.
fn vec_droppy_with_capacity<'mcx, T>(mcx: Mcx<'mcx>, n: usize) -> PgResult<PgVec<'mcx, T>> {
    let mut v = PgVec::new_in(mcx);
    v.try_reserve_exact(n)
        .map_err(|_| Box::new(mcx.oom(n.saturating_mul(core::mem::size_of::<T>()))))?;
    Ok(v)
}

// C: pstrdup of adbin/ccname/ccbin in CreateTupleDescCopyConstr.
fn clone_opt_string<'mcx>(
    mcx: Mcx<'mcx>,
    s: &Option<PgString<'_>>,
) -> PgResult<Option<PgString<'mcx>>> {
    match s {
        Some(s) => Ok(Some(s.clone_in(mcx)?)),
        None => Ok(None),
    }
}

fn opt_bytes<'a>(s: &'a Option<PgString<'_>>) -> &'a [u8] {
    match s {
        Some(s) => s.as_bytes(),
        None => &[],
    }
}

/// # Safety
/// `p` points at a live datum image of the layout `attlen` describes: a plain
/// (detoasted, non-expanded) varlena for `-1`, a NUL-terminated cstring for
/// `-2`, else `attlen` readable bytes. Tupdesc missing values satisfy this.
unsafe fn datum_size(p: *const u8, attlen: i16) -> usize {
    match attlen {
        -1 => varsize_any(p),
        -2 => {
            let mut n = 0usize;
            while *p.add(n) != 0 {
                n += 1;
            }
            n + 1
        }
        l => {
            debug_assert!(l > 0);
            l as usize
        }
    }
}

// datumCopy scoped to the attmissing payload (datum.c is not yet a unit here).
fn datum_copy_in<'mcx>(
    mcx: Mcx<'mcx>,
    value: Datum,
    attbyval: bool,
    attlen: i16,
) -> PgResult<Datum> {
    if attbyval {
        return Ok(value);
    }
    let p = value.as_usize() as *const u8;
    if p.is_null() {
        return Ok(Datum::null());
    }
    // SAFETY: non-null by-ref tupdesc missing value (see datum_size contract).
    let size = unsafe { datum_size(p, attlen) };
    // SAFETY: size >= 1; MAXIMUM_ALIGNOF is a power of two.
    let layout = unsafe { Layout::from_size_align_unchecked(size, MAXIMUM_ALIGNOF) };
    let dst = mcx
        .allocate(layout)
        .map_err(|_| Box::new(mcx.oom(size)))?
        .cast::<u8>();
    // SAFETY: dst freshly allocated for `size` bytes; src readable for `size`.
    unsafe { core::ptr::copy_nonoverlapping(p, dst.as_ptr(), size) };
    Ok(Datum::from_usize(dst.as_ptr() as usize))
}

// datumIsEqual over the attmissing payload: by-val word equality, by-ref byte
// image equality (no detoast, exactly C).
fn datum_is_equal(v1: Datum, v2: Datum, attbyval: bool, attlen: i16) -> bool {
    if attbyval {
        return v1 == v2;
    }
    let p1 = v1.as_usize() as *const u8;
    let p2 = v2.as_usize() as *const u8;
    // SAFETY: by-ref tupdesc missing values (see datum_size contract).
    unsafe {
        let s1 = datum_size(p1, attlen);
        let s2 = datum_size(p2, attlen);
        s1 == s2 && core::slice::from_raw_parts(p1, s1) == core::slice::from_raw_parts(p2, s2)
    }
}

#[track_caller]
#[cold]
#[inline(never)]
fn type_lookup_failed(oidtypeid: Oid) -> Box<PgError> {
    Box::new(PgError::error(format!("cache lookup failed for type {oidtypeid}")))
}

#[track_caller]
#[cold]
#[inline(never)]
fn unsupported_type(oidtypeid: Oid) -> Box<PgError> {
    Box::new(PgError::error(format!("unsupported type {oidtypeid}")))
}

#[cold]
#[inline(never)]
fn could_not_convert_row_type<'mcx>(
    attname: &str,
    indesc: &TupleDescData<'_>,
    outdesc: &TupleDescData<'_>,
    exists: bool,
) -> PgResult<Box<PgError>> {
    let outty = format_type::format_type_be(outdesc.tdtypeid)?;
    let inty = format_type::format_type_be(indesc.tdtypeid)?;
    let detail = if exists {
        format!("Attribute \"{attname}\" of type {outty} does not match corresponding attribute of type {inty}.")
    } else {
        format!("Attribute \"{attname}\" of type {outty} does not exist in type {inty}.")
    };
    Ok(Box::new(
        PgError::error("could not convert row type")
            .with_sqlstate(types_error::ERRCODE_DATATYPE_MISMATCH)
            .with_detail(detail),
    ))
}

// build_attrmap_by_name (attmap.c): attmap[out_attno-1] = the indesc attno
// with the same name.  attnums[i] stays 0 for a dropped outdesc column, or
// for a missing indesc match when `missing_ok`.
fn build_attrmap_by_name_impl<'mcx>(
    mcx: Mcx<'mcx>,
    indesc: &TupleDescData<'_>,
    outdesc: &TupleDescData<'_>,
    missing_ok: bool,
) -> PgResult<PgVec<'mcx, i16>> {
    let mut attmap: PgVec<'mcx, i16> = vec_with_capacity_in(mcx, outdesc.natts as usize)?;
    for i in 0..outdesc.natts as usize {
        let outatt = outdesc.attr(i);
        if outatt.attisdropped {
            attmap.push(0);
            continue;
        }
        let name_bytes = outatt.attname.name_str();
        let name = core::str::from_utf8(name_bytes).unwrap_or_else(|_| panic!("non-UTF-8 attname"));
        let mut mapped: i16 = 0;
        for j in 0..indesc.natts as usize {
            let inatt = indesc.attr(j);
            if !inatt.attisdropped && inatt.attname.name_str() == name_bytes {
                if inatt.atttypid != outatt.atttypid || inatt.atttypmod != outatt.atttypmod {
                    return Err(could_not_convert_row_type(name, indesc, outdesc, true)?);
                }
                mapped = inatt.attnum;
                break;
            }
        }
        if mapped == 0 && !missing_ok {
            return Err(could_not_convert_row_type(name, indesc, outdesc, false)?);
        }
        attmap.push(mapped);
    }
    Ok(attmap)
}

// build_attrmap_by_name (attmap.c), missing_ok=false shape.
pub fn build_attrmap_by_name<'mcx>(
    mcx: Mcx<'mcx>,
    indesc: &TupleDescData<'_>,
    outdesc: &TupleDescData<'_>,
) -> PgResult<PgVec<'mcx, i16>> {
    build_attrmap_by_name_impl(mcx, indesc, outdesc, false)
}

// build_attrmap_by_name (attmap.c), missing_ok=true shape.
pub fn build_attrmap_by_name_missing_ok<'mcx>(
    mcx: Mcx<'mcx>,
    indesc: &TupleDescData<'_>,
    outdesc: &TupleDescData<'_>,
) -> PgResult<PgVec<'mcx, i16>> {
    build_attrmap_by_name_impl(mcx, indesc, outdesc, true)
}

// check_attrmap_match (attmap.c): true when the map is a one-to-one identity,
// so the caller can skip runtime tuple conversion entirely.
fn check_attrmap_match(indesc: &TupleDescData<'_>, outdesc: &TupleDescData<'_>, attmap: &[i16]) -> bool {
    if indesc.natts != outdesc.natts {
        return false;
    }
    for i in 0..attmap.len() {
        let inatt = indesc.compact_attr(i);
        if inatt.atthasmissing {
            return false;
        }
        if attmap[i] == (i + 1) as i16 {
            continue;
        }
        let outatt = outdesc.compact_attr(i);
        if attmap[i] == 0
            && inatt.attisdropped
            && inatt.attlen == outatt.attlen
            && inatt.attalignby == outatt.attalignby
        {
            continue;
        }
        return false;
    }
    true
}

// build_attrmap_by_name_if_req (attmap.c): None when no runtime conversion
// is needed.
pub fn build_attrmap_by_name_if_req<'mcx>(
    mcx: Mcx<'mcx>,
    indesc: &TupleDescData<'_>,
    outdesc: &TupleDescData<'_>,
    missing_ok: bool,
) -> PgResult<Option<PgVec<'mcx, i16>>> {
    let attmap = build_attrmap_by_name_impl(mcx, indesc, outdesc, missing_ok)?;
    if check_attrmap_match(indesc, outdesc, &attmap) {
        return Ok(None);
    }
    Ok(Some(attmap))
}

// build_attrmap_by_position (attmap.c): positional match over non-dropped
// columns; indesc is the "returned" rowtype, outdesc the "expected" one in
// the errdetail texts, matching C's comment.
pub fn build_attrmap_by_position<'mcx>(
    mcx: Mcx<'mcx>,
    indesc: &TupleDescData<'_>,
    outdesc: &TupleDescData<'_>,
    msg: &str,
) -> PgResult<Option<PgVec<'mcx, i16>>> {
    let n = outdesc.natts as usize;
    let mut attmap: PgVec<'mcx, i16> = vec_with_capacity_in(mcx, n)?;
    for _ in 0..n {
        attmap.push(0);
    }

    let mut j = 0usize;
    let mut nincols = 0i32;
    let mut noutcols = 0i32;
    let mut same = true;
    for i in 0..n {
        let outatt = outdesc.attr(i);
        if outatt.attisdropped {
            continue;
        }
        noutcols += 1;
        while j < indesc.natts as usize {
            let inatt = indesc.attr(j);
            if inatt.attisdropped {
                j += 1;
                continue;
            }
            nincols += 1;
            if outatt.atttypid != inatt.atttypid
                || (outatt.atttypmod != inatt.atttypmod && outatt.atttypmod >= 0)
            {
                return Err(Box::new(
                    PgError::error(msg.to_string())
                        .with_sqlstate(types_error::ERRCODE_DATATYPE_MISMATCH)
                        .with_detail(format!(
                            "Returned type {} does not match expected type {} in column \"{}\" (position {}).",
                            format_type::format_type_with_typemod(inatt.atttypid, inatt.atttypmod)?,
                            format_type::format_type_with_typemod(outatt.atttypid, outatt.atttypmod)?,
                            core::str::from_utf8(outatt.attname.name_str())
                                .unwrap_or_else(|_| panic!("non-UTF-8 attname")),
                            noutcols,
                        )),
                ));
            }
            attmap[i] = (j + 1) as i16;
            j += 1;
            break;
        }
        if attmap[i] == 0 {
            same = false;
        }
    }

    // Unused input columns: mirrors C's for-loop, which still visits every
    // remaining j (via the increment clause) even when a dropped one
    // `continue`s past the count.
    for jj in j..indesc.natts as usize {
        if indesc.attr(jj).attisdropped {
            continue;
        }
        nincols += 1;
        same = false;
    }

    if !same {
        return Err(Box::new(
            PgError::error(msg.to_string())
                .with_sqlstate(types_error::ERRCODE_DATATYPE_MISMATCH)
                .with_detail(format!(
                    "Number of returned columns ({nincols}) does not match expected column count ({noutcols})."
                )),
        ));
    }

    if check_attrmap_match(indesc, outdesc, &attmap) {
        return Ok(None);
    }
    Ok(Some(attmap))
}

// convert_tuples_by_name (tupconvert.c): None when the by-name map is the
// identity (check_attrmap_match), so callers skip conversion entirely.
pub fn convert_tuples_by_name<'mcx>(
    mcx: Mcx<'mcx>,
    indesc: &TupleDescData<'_>,
    outdesc: &TupleDescData<'_>,
) -> PgResult<Option<PgVec<'mcx, i16>>> {
    build_attrmap_by_name_if_req(mcx, indesc, outdesc, false)
}

// convert_tuples_by_position (tupconvert.c): thin wrapper, matching the
// convert_tuples_by_name shape above.
pub fn convert_tuples_by_position<'mcx>(
    mcx: Mcx<'mcx>,
    indesc: &TupleDescData<'_>,
    outdesc: &TupleDescData<'_>,
    msg: &str,
) -> PgResult<Option<PgVec<'mcx, i16>>> {
    build_attrmap_by_position(mcx, indesc, outdesc, msg)
}

// convert_tuples_by_name_attrmap (tupconvert.c): C wraps a precomputed
// attrmap into a TupleConversionMap; this repo's map IS the bare attrmap
// vec, so wrapping is the identity.
pub fn convert_tuples_by_name_attrmap<'mcx>(
    _indesc: &TupleDescData<'_>,
    _outdesc: &TupleDescData<'_>,
    attrmap: PgVec<'mcx, i16>,
) -> PgVec<'mcx, i16> {
    attrmap
}
