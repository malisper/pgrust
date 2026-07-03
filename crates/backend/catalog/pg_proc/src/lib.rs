// pg_proc.c ProcedureCreate insert/replace slice. Loud: OUT/variadic
// parameter arrays, argument defaults, transforms, proconfig, prosqlbody,
// RECORD-tupdesc replace compare, named-argument replace compare,
// non-superuser owner check. pgstat_create_function is skipped (function
// stats unported); proacl is NULL (get_user_default_acl unported — initdb
// default is no pg_default_acl rows, same result).
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use datum::Datum;
use mcx::Mcx;
use types_core::{
    AttrNumber, InvalidOid, Oid, ANYARRAYOID, ANYCOMPATIBLEARRAYOID, ANYCOMPATIBLEMULTIRANGEOID,
    ANYCOMPATIBLENONARRAYOID, ANYCOMPATIBLEOID, ANYCOMPATIBLERANGEOID, ANYELEMENTOID, ANYENUMOID,
    ANYMULTIRANGEOID, ANYNONARRAYOID, ANYRANGEOID, INTERNALOID, LANGUAGE_RELATION_ID,
    NAMESPACE_RELATION_ID, PROCEDURE_RELATION_ID, RECORDOID, TYPE_RELATION_ID,
};
use types_error::{
    PgError, PgResult, ERRCODE_DUPLICATE_FUNCTION, ERRCODE_INVALID_FUNCTION_DEFINITION,
    ERRCODE_TOO_MANY_ARGUMENTS, ERRCODE_WRONG_OBJECT_TYPE, ERROR,
};
use types_rel::RowExclusiveLock;
use types_tuple::NameData;

pub use pg_depend::{DependencyType, ObjectAddress};

pub const ProcedureOidIndexId: Oid = 2690;
pub const ProcedureNameArgsNspIndexId: Oid = 2691;

pub const Natts_pg_proc: usize = 30;
pub const Anum_pg_proc_oid: AttrNumber = 1;
pub const Anum_pg_proc_proname: usize = 2;
pub const Anum_pg_proc_pronamespace: usize = 3;
pub const Anum_pg_proc_proowner: usize = 4;
pub const Anum_pg_proc_prolang: usize = 5;
pub const Anum_pg_proc_procost: usize = 6;
pub const Anum_pg_proc_prorows: usize = 7;
pub const Anum_pg_proc_provariadic: usize = 8;
pub const Anum_pg_proc_prosupport: usize = 9;
pub const Anum_pg_proc_prokind: usize = 10;
pub const Anum_pg_proc_prosecdef: usize = 11;
pub const Anum_pg_proc_proleakproof: usize = 12;
pub const Anum_pg_proc_proisstrict: usize = 13;
pub const Anum_pg_proc_proretset: usize = 14;
pub const Anum_pg_proc_provolatile: usize = 15;
pub const Anum_pg_proc_proparallel: usize = 16;
pub const Anum_pg_proc_pronargs: usize = 17;
pub const Anum_pg_proc_pronargdefaults: usize = 18;
pub const Anum_pg_proc_prorettype: usize = 19;
pub const Anum_pg_proc_proargtypes: usize = 20;
pub const Anum_pg_proc_proallargtypes: usize = 21;
pub const Anum_pg_proc_proargmodes: usize = 22;
pub const Anum_pg_proc_proargnames: usize = 23;
pub const Anum_pg_proc_proargdefaults: usize = 24;
pub const Anum_pg_proc_protrftypes: usize = 25;
pub const Anum_pg_proc_prosrc: usize = 26;
pub const Anum_pg_proc_probin: usize = 27;
pub const Anum_pg_proc_prosqlbody: usize = 28;
pub const Anum_pg_proc_proconfig: usize = 29;
pub const Anum_pg_proc_proacl: usize = 30;

pub const PROKIND_FUNCTION: i8 = b'f' as i8;
pub const PROKIND_AGGREGATE: i8 = b'a' as i8;
pub const PROKIND_WINDOW: i8 = b'w' as i8;
pub const PROKIND_PROCEDURE: i8 = b'p' as i8;

pub const PROVOLATILE_IMMUTABLE: i8 = b'i' as i8;
pub const PROVOLATILE_STABLE: i8 = b's' as i8;
pub const PROVOLATILE_VOLATILE: i8 = b'v' as i8;

pub const PROPARALLEL_SAFE: i8 = b's' as i8;
pub const PROPARALLEL_RESTRICTED: i8 = b'r' as i8;
pub const PROPARALLEL_UNSAFE: i8 = b'u' as i8;

pub const FUNC_MAX_ARGS: usize = 100;

pub const INTERNALlanguageId: Oid = 12;
pub const ClanguageId: Oid = 13;
pub const SQLlanguageId: Oid = 14;

#[cold]
#[inline(never)]
fn unported(what: &str) -> ! {
    panic!("unported: pg_proc {what}")
}

#[cold]
#[inline(never)]
fn err(msg: String, sqlstate: types_error::SqlState) -> Box<PgError> {
    Box::new(PgError::new(ERROR, msg).with_sqlstate(sqlstate))
}

pub struct ProcedureCreateArgs<'a> {
    pub procedureName: &'a str,
    pub procNamespace: Oid,
    pub replace: bool,
    pub returnsSet: bool,
    pub returnType: Oid,
    pub proowner: Oid,
    pub languageObjectId: Oid,
    pub languageValidator: Oid,
    pub prosrc: &'a str,
    pub probin: Option<&'a str>,
    pub prokind: i8,
    pub security_definer: bool,
    pub isLeakProof: bool,
    pub isStrict: bool,
    pub volatility: i8,
    pub parallel: i8,
    pub parameterTypes: &'a [Oid],
    // One entry per parameter, "" for unnamed; None when no parameter is named.
    pub parameterNames: Option<&'a [&'a str]>,
    pub procost: f32,
    pub prorows: f32,
}

// IsPolymorphicTypeFamily1/2 (pg_type.h).
fn family1(t: Oid) -> bool {
    matches!(
        t,
        ANYELEMENTOID | ANYARRAYOID | ANYNONARRAYOID | ANYENUMOID | ANYRANGEOID | ANYMULTIRANGEOID
    )
}

fn family2(t: Oid) -> bool {
    matches!(
        t,
        ANYCOMPATIBLEOID
            | ANYCOMPATIBLEARRAYOID
            | ANYCOMPATIBLENONARRAYOID
            | ANYCOMPATIBLERANGEOID
            | ANYCOMPATIBLEMULTIRANGEOID
    )
}

// check_valid_polymorphic_signature (parse_coerce.c).
pub fn check_valid_polymorphic_signature(ret_type: Oid, args: &[Oid]) -> PgResult<Option<String>> {
    let detail = if ret_type == ANYRANGEOID || ret_type == ANYMULTIRANGEOID {
        if args.iter().any(|&a| a == ANYRANGEOID || a == ANYMULTIRANGEOID) {
            return Ok(None);
        }
        format!(
            "A result of type {} requires at least one input of type anyrange or anymultirange.",
            format_type::format_type_be(ret_type)?
        )
    } else if ret_type == ANYCOMPATIBLERANGEOID || ret_type == ANYCOMPATIBLEMULTIRANGEOID {
        if args
            .iter()
            .any(|&a| a == ANYCOMPATIBLERANGEOID || a == ANYCOMPATIBLEMULTIRANGEOID)
        {
            return Ok(None);
        }
        format!(
            "A result of type {} requires at least one input of type anycompatiblerange or anycompatiblemultirange.",
            format_type::format_type_be(ret_type)?
        )
    } else if family1(ret_type) {
        if args.iter().any(|&a| family1(a)) {
            return Ok(None);
        }
        format!(
            "A result of type {} requires at least one input of type anyelement, anyarray, anynonarray, anyenum, anyrange, or anymultirange.",
            format_type::format_type_be(ret_type)?
        )
    } else if family2(ret_type) {
        if args.iter().any(|&a| family2(a)) {
            return Ok(None);
        }
        format!(
            "A result of type {} requires at least one input of type anycompatible, anycompatiblearray, anycompatiblenonarray, anycompatiblerange, or anycompatiblemultirange.",
            format_type::format_type_be(ret_type)?
        )
    } else {
        return Ok(None);
    };
    Ok(Some(detail))
}

// check_valid_internal_signature (parse_coerce.c).
pub fn check_valid_internal_signature(ret_type: Oid, args: &[Oid]) -> Option<&'static str> {
    if ret_type == INTERNALOID && !args.contains(&ret_type) {
        return Some("A result of type internal requires at least one input of type internal.");
    }
    None
}

// buildoidvector (oid.c): 1-D, lbound 0, dataoffset 0 — NOT construct_array's
// lbound-1 shape; pg_proc rows byte-compare against C on this.
pub fn build_oidvector_image<'mcx>(mcx: Mcx<'mcx>, oids: &[Oid]) -> PgResult<mcx::PgVec<'mcx, u8>> {
    let total = 24 + 4 * oids.len();
    let mut out: mcx::PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, total)?;
    out.resize(total, 0);
    let w = |out: &mut [u8], off: usize, v: i32| {
        out[off..off + 4].copy_from_slice(&v.to_ne_bytes());
    };
    w(&mut out, 0, (total as i32) << 2);
    w(&mut out, 4, 1);
    w(&mut out, 8, 0);
    w(&mut out, 12, types_core::OIDOID as i32);
    w(&mut out, 16, oids.len() as i32);
    w(&mut out, 20, 0);
    for (i, &o) in oids.iter().enumerate() {
        out[24 + 4 * i..28 + 4 * i].copy_from_slice(&o.to_ne_bytes());
    }
    Ok(out)
}

// format_procedure (regproc.c) minus the schema-qualification visibility
// walk; identical for search_path-visible names.
fn format_procedure_lite(name: &str, argtypes: &[Oid]) -> PgResult<String> {
    let mut sig = String::from(name);
    sig.push('(');
    for (i, &a) in argtypes.iter().enumerate() {
        if i > 0 {
            sig.push(',');
        }
        sig.push_str(&format_type::format_type_be(a)?);
    }
    sig.push(')');
    Ok(sig)
}

pub fn ProcedureCreate<'mcx>(
    mcx: Mcx<'mcx>,
    a: &ProcedureCreateArgs<'_>,
) -> PgResult<ObjectAddress> {
    let parameterCount = a.parameterTypes.len();
    if parameterCount > FUNC_MAX_ARGS {
        return Err(err(
            format!("functions cannot have more than {FUNC_MAX_ARGS} arguments"),
            ERRCODE_TOO_MANY_ARGUMENTS,
        ));
    }

    if let Some(detail) = check_valid_polymorphic_signature(a.returnType, a.parameterTypes)? {
        return Err(Box::new(
            PgError::new(ERROR, "cannot determine result data type".to_string())
                .with_sqlstate(ERRCODE_INVALID_FUNCTION_DEFINITION)
                .with_detail(detail),
        ));
    }
    if let Some(detail) = check_valid_internal_signature(a.returnType, a.parameterTypes) {
        return Err(Box::new(
            PgError::new(ERROR, "unsafe use of pseudo-type \"internal\"".to_string())
                .with_sqlstate(ERRCODE_INVALID_FUNCTION_DEFINITION)
                .with_detail(detail),
        ));
    }

    let mut procname = NameData::default();
    procname.namestrcpy(a.procedureName);
    let prosrc_text = varlena::cstring_to_text(mcx, a.prosrc.as_bytes())?;
    let probin_text = match a.probin {
        Some(s) => Some(varlena::cstring_to_text(mcx, s.as_bytes())?),
        None => None,
    };
    let argtypes_image = build_oidvector_image(mcx, a.parameterTypes)?;

    let mut values = [Datum::null(); Natts_pg_proc];
    let mut nulls = [false; Natts_pg_proc];
    let set = |values: &mut [Datum], attnum: usize, d: Datum| values[attnum - 1] = d;
    set(&mut values, Anum_pg_proc_proname, Datum::from_usize(procname.data.as_ptr() as usize));
    set(&mut values, Anum_pg_proc_pronamespace, Datum::from_oid(a.procNamespace));
    set(&mut values, Anum_pg_proc_proowner, Datum::from_oid(a.proowner));
    set(&mut values, Anum_pg_proc_prolang, Datum::from_oid(a.languageObjectId));
    set(&mut values, Anum_pg_proc_procost, Datum::from_f32(a.procost));
    set(&mut values, Anum_pg_proc_prorows, Datum::from_f32(a.prorows));
    set(&mut values, Anum_pg_proc_provariadic, Datum::from_oid(InvalidOid));
    set(&mut values, Anum_pg_proc_prosupport, Datum::from_oid(InvalidOid));
    set(&mut values, Anum_pg_proc_prokind, Datum::from_char(a.prokind));
    set(&mut values, Anum_pg_proc_prosecdef, Datum::from_bool(a.security_definer));
    set(&mut values, Anum_pg_proc_proleakproof, Datum::from_bool(a.isLeakProof));
    set(&mut values, Anum_pg_proc_proisstrict, Datum::from_bool(a.isStrict));
    set(&mut values, Anum_pg_proc_proretset, Datum::from_bool(a.returnsSet));
    set(&mut values, Anum_pg_proc_provolatile, Datum::from_char(a.volatility));
    set(&mut values, Anum_pg_proc_proparallel, Datum::from_char(a.parallel));
    set(&mut values, Anum_pg_proc_pronargs, Datum::from_i16(parameterCount as i16));
    set(&mut values, Anum_pg_proc_pronargdefaults, Datum::from_i16(0));
    set(&mut values, Anum_pg_proc_prorettype, Datum::from_oid(a.returnType));
    set(
        &mut values,
        Anum_pg_proc_proargtypes,
        Datum::from_usize(argtypes_image.as_ptr() as usize),
    );
    nulls[Anum_pg_proc_proallargtypes - 1] = true;
    nulls[Anum_pg_proc_proargmodes - 1] = true;
    let argnames_image = match a.parameterNames {
        Some(names) => {
            // std Vec: scratch holding droppy Varlena handles (PgVec's
            // !needs_drop gate rejects them); freed after the copy below.
            let mut texts = Vec::with_capacity(names.len());
            let mut elems: mcx::PgVec<'mcx, Datum> = mcx::vec_with_capacity_in(mcx, names.len())?;
            for &n in names {
                texts.push(varlena::cstring_to_text(mcx, n.as_bytes())?);
            }
            for t in texts.iter() {
                elems.push(Datum::from_usize(t.as_bytes().as_ptr() as usize));
            }
            Some(datum::array_build::construct_array_image(
                mcx,
                &elems,
                types_core::TEXTOID,
                -1,
                false,
                b'i',
            )?)
        }
        None => None,
    };
    match &argnames_image {
        Some(img) => set(
            &mut values,
            Anum_pg_proc_proargnames,
            Datum::from_usize(img.as_ptr() as usize),
        ),
        None => nulls[Anum_pg_proc_proargnames - 1] = true,
    }
    nulls[Anum_pg_proc_proargdefaults - 1] = true;
    nulls[Anum_pg_proc_protrftypes - 1] = true;
    set(
        &mut values,
        Anum_pg_proc_prosrc,
        Datum::from_usize(prosrc_text.as_bytes().as_ptr() as usize),
    );
    match &probin_text {
        Some(t) => set(
            &mut values,
            Anum_pg_proc_probin,
            Datum::from_usize(t.as_bytes().as_ptr() as usize),
        ),
        None => nulls[Anum_pg_proc_probin - 1] = true,
    }
    nulls[Anum_pg_proc_prosqlbody - 1] = true;
    nulls[Anum_pg_proc_proconfig - 1] = true;
    nulls[Anum_pg_proc_proacl - 1] = true;

    let rel = table::table_open(mcx, PROCEDURE_RELATION_ID, RowExclusiveLock)?;

    // SAFETY: Oid is u32; viewing the slice as bytes for the oidvector cache
    // key has no padding or aliasing hazard.
    let argbytes = unsafe {
        core::slice::from_raw_parts(a.parameterTypes.as_ptr() as *const u8, 4 * parameterCount)
    };
    let oldtup = cache_syscache::SearchSysCache3(
        cache_syscache::cacheinfo::PROCNAMEARGSNSP,
        cache_syscache::SysCacheKey::Str(a.procedureName),
        cache_syscache::SysCacheKey::Bytes(argbytes),
        cache_syscache::SysCacheKey::Value(Datum::from_oid(a.procNamespace)),
    )?;

    let (retval, is_update) = if let Some(oldtup) = oldtup {
        let t = oldtup.tuple();
        let desc = rel.descr();
        let getattr = |attnum: usize| -> (Datum, bool) {
            let mut isnull = false;
            // SAFETY: attnum is a valid pg_proc column under the relation's
            // descriptor; the tuple stays pinned until ReleaseSysCache.
            let d = unsafe { types_tuple::heap_getattr(&t, attnum as i32, desc, &mut isnull) };
            (d, isnull)
        };
        let old_oid = getattr(Anum_pg_proc_oid as usize).0.as_oid();
        let old_prokind = getattr(Anum_pg_proc_prokind).0.as_i8();
        let old_rettype = getattr(Anum_pg_proc_prorettype).0.as_oid();
        let old_retset = getattr(Anum_pg_proc_proretset).0.as_bool();
        let old_nargdefaults = getattr(Anum_pg_proc_pronargdefaults).0.as_i16();
        let (_, old_argnames_null) = getattr(Anum_pg_proc_proargnames);

        if !a.replace {
            return Err(err(
                format!(
                    "function \"{}\" already exists with same argument types",
                    a.procedureName
                ),
                ERRCODE_DUPLICATE_FUNCTION,
            ));
        }
        if !superuser::superuser()? {
            unported("ProcedureCreate: object_ownercheck for non-superusers");
        }
        if old_prokind != a.prokind {
            let detail = match old_prokind {
                PROKIND_AGGREGATE => format!("\"{}\" is an aggregate function.", a.procedureName),
                PROKIND_FUNCTION => format!("\"{}\" is a function.", a.procedureName),
                PROKIND_PROCEDURE => format!("\"{}\" is a procedure.", a.procedureName),
                PROKIND_WINDOW => format!("\"{}\" is a window function.", a.procedureName),
                _ => String::new(),
            };
            let mut e = PgError::new(ERROR, "cannot change routine kind".to_string())
                .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE);
            if !detail.is_empty() {
                e = e.with_detail(detail);
            }
            return Err(Box::new(e));
        }
        if a.returnType != old_rettype || a.returnsSet != old_retset {
            let dropcmd = match a.prokind {
                PROKIND_PROCEDURE => "DROP PROCEDURE",
                PROKIND_AGGREGATE => "DROP AGGREGATE",
                _ => "DROP FUNCTION",
            };
            let msg = if a.prokind == PROKIND_PROCEDURE {
                "cannot change whether a procedure has output parameters"
            } else {
                "cannot change return type of existing function"
            };
            return Err(Box::new(
                PgError::new(ERROR, msg.to_string())
                    .with_sqlstate(ERRCODE_INVALID_FUNCTION_DEFINITION)
                    .with_hint(format!(
                        "Use {dropcmd} {} first.",
                        format_procedure_lite(a.procedureName, a.parameterTypes)?
                    )),
            ));
        }
        if a.returnType == RECORDOID {
            unported("ProcedureCreate: RECORD-return tupdesc replace compare");
        }
        if !old_argnames_null {
            if !getattr(Anum_pg_proc_proargmodes).1 {
                unported("ProcedureCreate: replace of a function with proargmodes set");
            }
            let (d, _) = getattr(Anum_pg_proc_proargnames);
            // pg_detoast_datum: catalog arrays are inline, but may carry a
            // short (1-byte) header — expand to the plain image shape.
            // SAFETY: d points at a live inline varlena in the pinned tuple.
            let plain: mcx::PgVec<'mcx, u8>;
            let image: &[u8] = unsafe {
                let p = d.as_usize() as *const u8;
                if types_tuple::varatt::varatt_is_1b(p) {
                    assert!(
                        !types_tuple::varatt::varatt_is_1b_e(p),
                        "pg_proc.proargnames: external varlena"
                    );
                    let raw = types_tuple::varatt::varsize_1b(p);
                    let payload = core::slice::from_raw_parts(p.add(1), raw - 1);
                    let total = raw - 1 + 4;
                    let mut v: mcx::PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, total)?;
                    let hdr = types_tuple::varatt::set_varsize_4b_word(total as u32);
                    mcx::vec_append_bytes(&mut v, &hdr.to_ne_bytes())?;
                    mcx::vec_append_bytes(&mut v, payload)?;
                    plain = v;
                    &plain
                } else {
                    let raw = types_tuple::varatt::varsize_4b(p);
                    core::slice::from_raw_parts(p, raw)
                }
            };
            let olds = datum::array_build::deconstruct_array_image(mcx, image, -1, false, b'i')?;
            for (j, od) in olds.iter().enumerate() {
                // SAFETY: element datums point at text images inside `image`.
                let ob = unsafe {
                    let p = od.as_usize() as *const u8;
                    if types_tuple::varatt::varatt_is_1b(p) {
                        let raw = types_tuple::varatt::varsize_1b(p);
                        core::slice::from_raw_parts(p.add(1), raw - 1)
                    } else {
                        let raw = types_tuple::varatt::varsize_4b(p);
                        core::slice::from_raw_parts(p.add(4), raw - 4)
                    }
                };
                if ob.is_empty() {
                    continue;
                }
                let newname = a.parameterNames.and_then(|ns| ns.get(j).copied()).unwrap_or("");
                if newname.as_bytes() != ob {
                    let dropcmd = match a.prokind {
                        PROKIND_PROCEDURE => "DROP PROCEDURE",
                        PROKIND_AGGREGATE => "DROP AGGREGATE",
                        _ => "DROP FUNCTION",
                    };
                    return Err(Box::new(
                        PgError::new(
                            ERROR,
                            format!(
                                "cannot change name of input parameter \"{}\"",
                                String::from_utf8_lossy(ob)
                            ),
                        )
                        .with_sqlstate(ERRCODE_INVALID_FUNCTION_DEFINITION)
                        .with_hint(format!(
                            "Use {dropcmd} {} first.",
                            format_procedure_lite(a.procedureName, a.parameterTypes)?
                        )),
                    ));
                }
            }
        }
        if old_nargdefaults != 0 {
            unported("ProcedureCreate: parameter-default replace compare (old pronargdefaults set)");
        }

        let mut replaces = [true; Natts_pg_proc];
        replaces[Anum_pg_proc_oid as usize - 1] = false;
        replaces[Anum_pg_proc_proowner - 1] = false;
        replaces[Anum_pg_proc_proacl - 1] = false;

        let mut tup = heaptuple::heap_modify_tuple(mcx, &t, desc, &values, &nulls, &replaces)?;
        let otid = t.t_self;
        catalog_indexing::CatalogTupleUpdate(mcx, &rel, &otid, &mut tup)?;
        cache_syscache::ReleaseSysCache(oldtup);
        (old_oid, true)
    } else {
        let newOid =
            catalog::GetNewOidWithIndex(mcx, &rel, ProcedureOidIndexId, Anum_pg_proc_oid)?;
        values[Anum_pg_proc_oid as usize - 1] = Datum::from_oid(newOid);
        let mut tup = heaptuple::heap_form_tuple(mcx, rel.descr(), &values, &nulls)?;
        catalog_indexing::CatalogTupleInsert(mcx, &rel, &mut tup)?;
        (newOid, false)
    };

    if is_update {
        pg_depend::deleteDependencyRecordsFor(mcx, PROCEDURE_RELATION_ID, retval, true)?;
    }

    let myself = ObjectAddress::set(PROCEDURE_RELATION_ID, retval);
    let mut referenced: mcx::PgVec<'mcx, ObjectAddress> =
        mcx::vec_with_capacity_in(mcx, 3 + parameterCount)?;
    referenced.push(ObjectAddress::set(NAMESPACE_RELATION_ID, a.procNamespace));
    referenced.push(ObjectAddress::set(LANGUAGE_RELATION_ID, a.languageObjectId));
    referenced.push(ObjectAddress::set(TYPE_RELATION_ID, a.returnType));
    for &argtype in a.parameterTypes {
        referenced.push(ObjectAddress::set(TYPE_RELATION_ID, argtype));
    }
    pg_depend::record_object_address_dependencies(
        mcx,
        &myself,
        &mut referenced,
        DependencyType::Normal,
    )?;

    if !is_update {
        pg_depend::recordDependencyOnOwner(PROCEDURE_RELATION_ID, retval, a.proowner);
    }
    // recordDependencyOnCurrentExtension / recordDependencyOnNewAcl: no-ops —
    // CREATE EXTENSION is unported and proacl is always NULL here.

    rel.close(RowExclusiveLock)?;

    if a.languageValidator != InvalidOid {
        xact::CommandCounterIncrement()?;
        let mut flinfo = fmgr_core::fmgr_info(a.languageValidator)?;
        types_fmgr::function_call1_coll(&mut flinfo, InvalidOid, Datum::from_oid(retval))?;
    }
    // pgstat_create_function: skipped (per-function stats unported).

    Ok(myself)
}
