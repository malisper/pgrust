// pg_proc.c ProcedureCreate insert/replace slice. Loud: argument defaults,
// transforms, prosqlbody, RECORD-tupdesc replace compare,
// replace of a function with proargmodes+proargnames set.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use datum::Datum;
use mcx::Mcx;
use types_core::{
    AttrNumber, InvalidOid, Oid, OidIsValid, ANYARRAYOID, ANYCOMPATIBLEARRAYOID,
    ANYCOMPATIBLEMULTIRANGEOID,
    ANYCOMPATIBLENONARRAYOID, ANYCOMPATIBLEOID, ANYCOMPATIBLERANGEOID, ANYELEMENTOID, ANYENUMOID,
    ANYMULTIRANGEOID, ANYNONARRAYOID, ANYOID, ANYRANGEOID, CHAROID, INTERNALOID,
    LANGUAGE_RELATION_ID, NAMESPACE_RELATION_ID, OIDOID, PROCEDURE_RELATION_ID, RECORDOID,
    TYPE_RELATION_ID,
};
use types_error::{
    PgError, PgResult, ERRCODE_DUPLICATE_FUNCTION, ERRCODE_INSUFFICIENT_PRIVILEGE,
    ERRCODE_INVALID_FUNCTION_DEFINITION, ERRCODE_TOO_MANY_ARGUMENTS,
    ERRCODE_UNDEFINED_FUNCTION, ERRCODE_WRONG_OBJECT_TYPE, ERROR,
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

pub const PROARGMODE_IN: i8 = b'i' as i8;
pub const PROARGMODE_OUT: i8 = b'o' as i8;
pub const PROARGMODE_INOUT: i8 = b'b' as i8;
pub const PROARGMODE_VARIADIC: i8 = b'v' as i8;
pub const PROARGMODE_TABLE: i8 = b't' as i8;

pub const FUNC_MAX_ARGS: usize = 100;

pub const INTERNALlanguageId: Oid = 12;
pub const ClanguageId: Oid = 13;
pub const SQLlanguageId: Oid = 14;

#[cold]
#[inline(never)]
fn unported(what: &str) -> ! {
    panic!("unported: pg_proc {what}")
}

#[track_caller]
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
    // Analyzed body of an SQL-standard-body function (BEGIN ATOMIC / RETURN).
    pub prosqlbody: Option<types_nodes::Node<'a>>,
    pub prokind: i8,
    pub security_definer: bool,
    pub isLeakProof: bool,
    pub isStrict: bool,
    pub volatility: i8,
    pub parallel: i8,
    pub parameterTypes: &'a [Oid],
    // All parameters (including OUT) when any non-IN mode is present.
    pub allParameterTypes: Option<&'a [Oid]>,
    // PROARGMODE_* chars, parallel to allParameterTypes.
    pub parameterModes: Option<&'a [i8]>,
    // One entry per parameter, "" for unnamed; None when no parameter is named.
    pub parameterNames: Option<&'a [&'a str]>,
    // "name=value" GUC entries for pg_proc.proconfig; None stores NULL.
    pub proconfig: Option<&'a [String]>,
    pub procost: f32,
    pub prorows: f32,
    // OID of a planner support function, or InvalidOid.
    pub prosupport: Oid,
    // nodeToString image of the input-parameter defaults List and its length
    // (pg_proc.proargdefaults / pronargdefaults); caller serializes because
    // pg_proc sits below outfuncs.
    pub parameterDefaults: Option<&'a str>,
    pub numDefaults: i16,
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

// C: TextDatumGetCString — strip the 1B/4B varlena header (or detoast)
// before treating the payload as text.
fn text_datum_to_string(mcx: Mcx<'_>, d: Datum) -> PgResult<String> {
    let ptr = d.as_usize() as *const u8;
    // SAFETY: a live varlena readable through its full VARSIZE_ANY.
    let raw = unsafe { core::slice::from_raw_parts(ptr, types_tuple::varatt::varsize_any(ptr)) };
    let payload = varlena::open_image(mcx, raw)?;
    Ok(String::from_utf8_lossy(payload.as_bytes()).into_owned())
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

    if let Some(allParams) = a.allParameterTypes {
        debug_assert!(allParams.len() >= parameterCount);
        for (i, &t) in allParams.iter().enumerate() {
            match a.parameterModes.map(|ms| ms[i]) {
                None | Some(PROARGMODE_IN) | Some(PROARGMODE_VARIADIC) => continue,
                _ => {}
            }
            if let Some(detail) = check_valid_polymorphic_signature(t, a.parameterTypes)? {
                return Err(Box::new(
                    PgError::new(ERROR, "cannot determine result data type".to_string())
                        .with_sqlstate(ERRCODE_INVALID_FUNCTION_DEFINITION)
                        .with_detail(detail),
                ));
            }
            if let Some(detail) = check_valid_internal_signature(t, a.parameterTypes) {
                return Err(Box::new(
                    PgError::new(ERROR, "unsafe use of pseudo-type \"internal\"".to_string())
                        .with_sqlstate(ERRCODE_INVALID_FUNCTION_DEFINITION)
                        .with_detail(detail),
                ));
            }
        }
    }

    let mut variadicType = InvalidOid;
    if let Some(modes) = a.parameterModes {
        let allParams = a.allParameterTypes.unwrap_or(a.parameterTypes);
        debug_assert!(modes.len() == allParams.len());
        for (i, &m) in modes.iter().enumerate() {
            match m {
                PROARGMODE_IN | PROARGMODE_INOUT => {
                    if variadicType != InvalidOid {
                        panic!("variadic parameter must be last");
                    }
                }
                PROARGMODE_OUT => {
                    if variadicType != InvalidOid && a.prokind == PROKIND_PROCEDURE {
                        panic!("variadic parameter must be last");
                    }
                }
                PROARGMODE_TABLE => {}
                PROARGMODE_VARIADIC => {
                    if variadicType != InvalidOid {
                        panic!("variadic parameter must be last");
                    }
                    variadicType = match allParams[i] {
                        ANYOID => ANYOID,
                        ANYARRAYOID => ANYELEMENTOID,
                        ANYCOMPATIBLEARRAYOID => ANYCOMPATIBLEOID,
                        t => {
                            let elem = lsyscache::get_element_type(t)?;
                            if elem == InvalidOid {
                                panic!("variadic parameter is not an array");
                            }
                            elem
                        }
                    };
                }
                _ => panic!("invalid parameter mode '{}'", m as u8 as char),
            }
        }
    }

    let mut procname = NameData::default();
    procname.namestrcpy(a.procedureName);
    let prosrc_text = varlena::cstring_to_text(mcx, a.prosrc.as_bytes())?;
    let probin_text = match a.probin {
        Some(s) => Some(varlena::cstring_to_text(mcx, s.as_bytes())?),
        None => None,
    };
    // pg_proc.c:372-373: CStringGetTextDatum(nodeToString(prosqlbody)).
    let prosqlbody_text = match a.prosqlbody {
        Some(n) => Some(varlena::cstring_to_text(mcx, outfuncs::nodeToString(mcx, n)?.as_bytes())?),
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
    set(&mut values, Anum_pg_proc_provariadic, Datum::from_oid(variadicType));
    set(&mut values, Anum_pg_proc_prosupport, Datum::from_oid(a.prosupport));
    set(&mut values, Anum_pg_proc_prokind, Datum::from_char(a.prokind));
    set(&mut values, Anum_pg_proc_prosecdef, Datum::from_bool(a.security_definer));
    set(&mut values, Anum_pg_proc_proleakproof, Datum::from_bool(a.isLeakProof));
    set(&mut values, Anum_pg_proc_proisstrict, Datum::from_bool(a.isStrict));
    set(&mut values, Anum_pg_proc_proretset, Datum::from_bool(a.returnsSet));
    set(&mut values, Anum_pg_proc_provolatile, Datum::from_char(a.volatility));
    set(&mut values, Anum_pg_proc_proparallel, Datum::from_char(a.parallel));
    set(&mut values, Anum_pg_proc_pronargs, Datum::from_i16(parameterCount as i16));
    set(&mut values, Anum_pg_proc_pronargdefaults, Datum::from_i16(a.numDefaults));
    set(&mut values, Anum_pg_proc_prorettype, Datum::from_oid(a.returnType));
    set(
        &mut values,
        Anum_pg_proc_proargtypes,
        Datum::from_usize(argtypes_image.as_ptr() as usize),
    );
    let allargtypes_image = match a.allParameterTypes {
        Some(oids) => {
            let mut elems: mcx::PgVec<'mcx, Datum> = mcx::vec_with_capacity_in(mcx, oids.len())?;
            for &o in oids {
                elems.push(Datum::from_oid(o));
            }
            Some(datum::array_build::construct_array_image(mcx, &elems, OIDOID, 4, true, b'i')?)
        }
        None => None,
    };
    match &allargtypes_image {
        Some(img) => set(
            &mut values,
            Anum_pg_proc_proallargtypes,
            Datum::from_usize(img.as_ptr() as usize),
        ),
        None => nulls[Anum_pg_proc_proallargtypes - 1] = true,
    }
    let argmodes_image = match a.parameterModes {
        Some(modes) => {
            let mut elems: mcx::PgVec<'mcx, Datum> = mcx::vec_with_capacity_in(mcx, modes.len())?;
            for &m in modes {
                elems.push(Datum::from_char(m));
            }
            Some(datum::array_build::construct_array_image(mcx, &elems, CHAROID, 1, true, b'c')?)
        }
        None => None,
    };
    match &argmodes_image {
        Some(img) => set(
            &mut values,
            Anum_pg_proc_proargmodes,
            Datum::from_usize(img.as_ptr() as usize),
        ),
        None => nulls[Anum_pg_proc_proargmodes - 1] = true,
    }
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
    let argdefaults_text = match a.parameterDefaults {
        Some(d) => Some(varlena::cstring_to_text(mcx, d.as_bytes())?),
        None => None,
    };
    match &argdefaults_text {
        Some(t) => values[Anum_pg_proc_proargdefaults - 1] =
            Datum::from_usize(t.as_bytes().as_ptr() as usize),
        None => nulls[Anum_pg_proc_proargdefaults - 1] = true,
    }
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
    match &prosqlbody_text {
        Some(t) => set(
            &mut values,
            Anum_pg_proc_prosqlbody,
            Datum::from_usize(t.as_bytes().as_ptr() as usize),
        ),
        None => nulls[Anum_pg_proc_prosqlbody - 1] = true,
    }
    let proconfig_image = match a.proconfig {
        Some(entries) => {
            // std Vec: scratch holding droppy Varlena handles (as proargnames).
            let mut texts = Vec::with_capacity(entries.len());
            let mut elems: mcx::PgVec<'mcx, Datum> = mcx::vec_with_capacity_in(mcx, entries.len())?;
            for e in entries {
                texts.push(varlena::cstring_to_text(mcx, e.as_bytes())?);
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
    match &proconfig_image {
        Some(img) => set(
            &mut values,
            Anum_pg_proc_proconfig,
            Datum::from_usize(img.as_ptr() as usize),
        ),
        None => nulls[Anum_pg_proc_proconfig - 1] = true,
    }
    let proacl = aclchk_seams::get_user_default_acl::call(mcx, b'f', a.proowner, a.procNamespace)?;
    match proacl.as_deref() {
        Some(img) => set(
            &mut values,
            Anum_pg_proc_proacl,
            Datum::from_usize(img.as_ptr() as usize),
        ),
        None => nulls[Anum_pg_proc_proacl - 1] = true,
    }

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
        if !aclchk::object_ownercheck(PROCEDURE_RELATION_ID, old_oid, a.proowner)? {
            aclchk::aclcheck_error(
                aclchk::ACLCHECK_NOT_OWNER,
                types_nodes::parsenodes::ObjectType::OBJECT_FUNCTION,
                a.procedureName,
            )?;
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
        // RECORD returns: the OUT-parameter row type must not change
        // (pg_proc.c:412-441).
        if a.returnType == RECORDOID {
            let arrays = syscache_seams::pg_proc_result_arrays::call(mcx, old_oid)?
                .expect("pg_proc row visible above");
            let olddesc = match (&arrays.proallargtypes, &arrays.proargmodes) {
                (Some(ts), Some(ms)) => funcapi::build_function_result_tupdesc_d(
                    mcx,
                    a.prokind,
                    ts,
                    ms,
                    arrays.proargnames.as_deref(),
                )?,
                _ => None,
            };
            let newdesc = match (a.allParameterTypes, a.parameterModes) {
                (Some(ts), Some(ms)) => {
                    let names = match a.parameterNames {
                        None => None,
                        Some(ns) => {
                            let mut v: mcx::PgVec<'mcx, mcx::PgString<'mcx>> =
                                mcx::PgVec::new_in(mcx);
                            v.try_reserve_exact(ns.len()).map_err(|_| mcx.oom(ns.len()))?;
                            for n in ns {
                                v.push(mcx::PgString::from_str_in(n, mcx)?);
                            }
                            Some(v)
                        }
                    };
                    funcapi::build_function_result_tupdesc_d(
                        mcx,
                        a.prokind,
                        ts,
                        ms,
                        names.as_deref(),
                    )?
                }
                _ => None,
            };
            let same = match (&olddesc, &newdesc) {
                (None, None) => true,
                (Some(o), Some(n)) => tupdesc::equalRowTypes(o, n),
                _ => false,
            };
            if !same {
                let dropcmd = match a.prokind {
                    PROKIND_PROCEDURE => "DROP PROCEDURE",
                    PROKIND_AGGREGATE => "DROP AGGREGATE",
                    _ => "DROP FUNCTION",
                };
                return Err(Box::new(
                    PgError::new(
                        ERROR,
                        "cannot change return type of existing function".to_string(),
                    )
                    .with_sqlstate(ERRCODE_INVALID_FUNCTION_DEFINITION)
                    .with_detail("Row type defined by OUT parameters is different.".to_string())
                    .with_hint(format!(
                        "Use {dropcmd} {} first.",
                        format_procedure_lite(a.procedureName, a.parameterTypes)?
                    )),
                ));
            }
        }
        if !old_argnames_null {
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
            // get_func_input_arg_names (funcapi.c): both sides compare in
            // input-argument order, OUT/TABLE positions dropped.
            let old_arrays = syscache_seams::pg_proc_result_arrays::call(mcx, old_oid)?
                .expect("pg_proc row visible above");
            let old_modes = old_arrays.proargmodes.as_deref();
            let is_input = |m: Option<i8>| {
                !matches!(m, Some(m) if m == b'o' as i8 || m == b't' as i8)
            };
            let old_input: Vec<&[u8]> = olds
                .iter()
                .enumerate()
                .filter(|(j, _)| is_input(old_modes.and_then(|ms| ms.get(*j).copied())))
                .map(|(_, od)| {
                    // SAFETY: element datums point at text images inside
                    // `image`.
                    unsafe {
                        let p = od.as_usize() as *const u8;
                        if types_tuple::varatt::varatt_is_1b(p) {
                            let raw = types_tuple::varatt::varsize_1b(p);
                            core::slice::from_raw_parts(p.add(1), raw - 1)
                        } else {
                            let raw = types_tuple::varatt::varsize_4b(p);
                            core::slice::from_raw_parts(p.add(4), raw - 4)
                        }
                    }
                })
                .collect();
            let new_input: Vec<&str> = match a.parameterNames {
                None => Vec::new(),
                Some(ns) => ns
                    .iter()
                    .enumerate()
                    .filter(|(j, _)| {
                        is_input(a.parameterModes.and_then(|ms| ms.get(*j).copied()))
                    })
                    .map(|(_, &n)| n)
                    .collect(),
            };
            for (j, ob) in old_input.iter().copied().enumerate() {
                if ob.is_empty() {
                    continue;
                }
                let newname = new_input.get(j).copied().unwrap_or("");
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
        // pg_proc.c:533-575: existing defaults may not be removed, and each
        // retained default must keep its expression type (polymorphic
        // resolution of existing calls depends on it).
        if old_nargdefaults != 0 {
            let dropcmd = match a.prokind {
                PROKIND_PROCEDURE => "DROP PROCEDURE",
                PROKIND_AGGREGATE => "DROP AGGREGATE",
                _ => "DROP FUNCTION",
            };
            let ndefaults = a.numDefaults as usize;
            if ndefaults < old_nargdefaults as usize {
                return Err(Box::new(
                    PgError::new(
                        ERROR,
                        "cannot remove parameter defaults from existing function".to_string(),
                    )
                    .with_sqlstate(ERRCODE_INVALID_FUNCTION_DEFINITION)
                    .with_hint(format!(
                        "Use {dropcmd} {} first.",
                        format_procedure_lite(a.procedureName, a.parameterTypes)?
                    )),
                ));
            }
            let (old_defaults_d, old_defaults_null) = getattr(Anum_pg_proc_proargdefaults);
            assert!(!old_defaults_null, "pronargdefaults set but proargdefaults is null");
            let old_str = text_datum_to_string(mcx, old_defaults_d)?;
            let old_node = readfuncs::stringToNode(mcx, &old_str)?;
            let old_defaults = old_node.as_list().expect("proargdefaults holds a List");
            debug_assert_eq!(old_defaults.len(), old_nargdefaults as usize);
            // The caller hands defaults pre-serialized (train field shape);
            // deserialize for the per-default exprType comparison.
            let new_node = readfuncs::stringToNode(
                mcx,
                a.parameterDefaults.expect("ndefaults >= old_nargdefaults > 0"),
            )?;
            let newlist = new_node.as_list().expect("parameterDefaults holds a List");
            let skip = ndefaults - old_nargdefaults as usize;
            for (olddef, newdef) in old_defaults.iter().zip(newlist.iter().skip(skip)) {
                if nodes_core::node_funcs::expr_type(olddef)
                    != nodes_core::node_funcs::expr_type(newdef)
                {
                    return Err(Box::new(
                        PgError::new(
                            ERROR,
                            "cannot change data type of existing parameter default value"
                                .to_string(),
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

    let dep_param_types = a.allParameterTypes.unwrap_or(a.parameterTypes);
    let myself = ObjectAddress::set(PROCEDURE_RELATION_ID, retval);
    let mut referenced: mcx::PgVec<'mcx, ObjectAddress> =
        mcx::vec_with_capacity_in(mcx, 4 + dep_param_types.len())?;
    referenced.push(ObjectAddress::set(NAMESPACE_RELATION_ID, a.procNamespace));
    referenced.push(ObjectAddress::set(LANGUAGE_RELATION_ID, a.languageObjectId));
    referenced.push(ObjectAddress::set(TYPE_RELATION_ID, a.returnType));
    for &argtype in dep_param_types {
        referenced.push(ObjectAddress::set(TYPE_RELATION_ID, argtype));
    }
    // dependency on support function, if any
    if OidIsValid(a.prosupport) {
        referenced.push(ObjectAddress::set(PROCEDURE_RELATION_ID, a.prosupport));
    }
    pg_depend::record_object_address_dependencies(
        mcx,
        &myself,
        &mut referenced,
        DependencyType::Normal,
    )?;

    // pg_proc.c:665-666: dependencies on objects the SQL-standard body uses.
    if a.languageObjectId == SQLlanguageId {
        if let Some(body) = a.prosqlbody {
            dependency_seams::record_dependency_on_expr::call(
                mcx,
                &myself,
                body,
                &types_nodes::list::NodeList::nil(),
                DependencyType::Normal,
            )?;
        }
    }

    // pg_proc.c:669-670: dependencies on objects in parameter defaults.
    if let Some(defaults) = a.parameterDefaults {
        let defaults_node = readfuncs::stringToNode(mcx, defaults)?;
        dependency_seams::record_dependency_on_expr::call(
            mcx,
            &myself,
            defaults_node,
            &types_nodes::list::NodeList::nil(),
            DependencyType::Normal,
        )?;
    }

    if !is_update {
        pg_depend::recordDependencyOnOwner(mcx, PROCEDURE_RELATION_ID, retval, a.proowner)?;
        if let Some(img) = proacl.as_deref() {
            aclchk_seams::record_dependency_on_new_acl::call(
                mcx,
                PROCEDURE_RELATION_ID,
                retval,
                0,
                a.proowner,
                img,
            )?;
        }
    }
    pg_depend::recordDependencyOnCurrentExtension(mcx, &myself, is_update)?;

    rel.close(RowExclusiveLock)?;

    if a.languageValidator != InvalidOid {
        xact::CommandCounterIncrement()?;
        // pg_proc.c ProcedureCreate: apply proconfig (GUC_ACTION_SAVE) around the
        // validator only when check_function_bodies is on — applying it when off
        // would create dump ordering hazards (a SET clause may reference
        // not-yet-created objects).
        let set_items = if guc_tables::vars::check_function_bodies.read() {
            a.proconfig.filter(|items| !items.is_empty())
        } else {
            None
        };
        let mut save_nestlevel = 0;
        if let Some(items) = set_items {
            save_nestlevel = guc_seams::new_guc_nest_level::call();
            guc_seams::process_guc_array_secdef::call(items)?;
        }
        let mut flinfo = fmgr_core::fmgr_info(a.languageValidator)?;
        types_fmgr::function_call1_coll(&mut flinfo, InvalidOid, Datum::from_oid(retval))?;
        if set_items.is_some() {
            guc_seams::at_eoxact_guc::call(true, save_nestlevel)?;
        }
    }
    // ensure that stats are dropped if transaction aborts
    if !is_update {
        pgstat::function::pgstat_create_function(retval);
    }

    Ok(myself)
}

// function_parse_error_transpose (pg_proc.c): remap a validator error's
// cursor from function-body offsets onto the CREATE statement's literal.
// Positions are character-based; multibyte-aware via char counting.
pub fn function_parse_error_transpose(e: &mut types_error::PgError, prosrc: &str) -> bool {
    // C: geterrposition(), falling back to getinternalerrposition() for PLs
    // that report positions as internal errors to begin with.
    let origpos = match e.cursor_position.filter(|&p| p > 0) {
        Some(p) => p,
        None => match e.internal_position.filter(|&p| p > 0) {
            Some(p) => p,
            None => return false,
        },
    };
    // C requires ActivePortal->status == PORTAL_ACTIVE before trusting
    // sourceText.
    let query = pquery::ActivePortal()
        .filter(|p| p.borrow().status == types_portal::PortalStatus::PORTAL_ACTIVE)
        .and_then(|p| p.borrow().sourceText.as_ref().map(|s| s.as_str().to_string()));
    if let Some(q) = query {
        let newpos = match_prosrc_to_query(prosrc, &q, origpos);
        if newpos > 0 {
            e.cursor_position = Some(newpos);
            e.internal_position = None;
            e.internal_query = None;
            return true;
        }
    }
    e.cursor_position = None;
    e.internal_position = Some(origpos);
    e.internal_query = Some(prosrc.to_string());
    true
}

fn mbstrlen_with_len(s: &str, byte_len: usize) -> i32 {
    s[..byte_len.min(s.len())].chars().count() as i32
}

fn match_prosrc_to_query(prosrc: &str, query_text: &str, cursorpos: i32) -> i32 {
    let pb = prosrc.as_bytes();
    let qb = query_text.as_bytes();
    if qb.len() < pb.len() {
        return 0;
    }
    let mut matchpos = 0i32;
    for curpos in 0..(qb.len() - pb.len()) {
        if qb[curpos] == b'$'
            && qb[curpos + 1..].starts_with(pb)
            && qb.get(curpos + 1 + pb.len()) == Some(&b'$')
        {
            if matchpos != 0 {
                return 0;
            }
            matchpos = mbstrlen_with_len(query_text, curpos + 1) + cursorpos;
        } else if qb[curpos] == b'\'' {
            if let Some(newcursorpos) =
                match_prosrc_to_literal(pb, &qb[curpos + 1..], cursorpos)
            {
                if matchpos != 0 {
                    return 0;
                }
                matchpos = mbstrlen_with_len(query_text, curpos + 1) + newcursorpos;
            }
        }
    }
    matchpos
}

fn match_prosrc_to_literal(prosrc: &[u8], literal: &[u8], mut cursorpos: i32) -> Option<i32> {
    let mut newcp = cursorpos;
    let mut pi = 0usize;
    let mut li = 0usize;
    while pi < prosrc.len() {
        cursorpos -= 1;
        if literal.get(li) == Some(&b'\\') {
            li += 1;
            if cursorpos > 0 {
                newcp += 1;
            }
        } else if literal.get(li) == Some(&b'\'') {
            if literal.get(li + 1) != Some(&b'\'') {
                return None;
            }
            li += 1;
            if cursorpos > 0 {
                newcp += 1;
            }
        }
        // One character (multibyte-aware): consume the full UTF-8 sequence.
        let chlen = utf8_len(prosrc[pi]);
        if literal.len() < li + chlen || prosrc[pi..pi + chlen] != literal[li..li + chlen] {
            return None;
        }
        pi += chlen;
        li += chlen;
    }
    if literal.get(li) == Some(&b'\'') && literal.get(li + 1) != Some(&b'\'') {
        Some(newcp)
    } else {
        None
    }
}

fn utf8_len(b: u8) -> usize {
    if b < 0x80 {
        1
    } else if b >> 5 == 0b110 {
        2
    } else if b >> 4 == 0b1110 {
        3
    } else {
        4
    }
}

// CheckFunctionValidatorAccess (fmgr.c), the two catalog gates: validators
// are called with user-specified OIDs, so a bad OID must be a user-facing
// error, and a function of another language is rejected against that
// language's lanvalidator. DIVERGENCE (pre-existing scope): the two
// object_aclcheck permission gates (language USAGE, function EXECUTE) stay
// unported; they only bite non-superuser callers.
fn check_function_validator_access(validator_oid: Oid, funcoid: Oid) -> PgResult<()> {
    let Some(proc_shape) = syscache_seams::lookup_pg_proc_fmgr::call(funcoid)? else {
        return Err(PgError::error(format!("function with OID {funcoid} does not exist"))
            .with_sqlstate(ERRCODE_UNDEFINED_FUNCTION)
            .into());
    };
    let Some(lang) = syscache_seams::lookup_pg_language_fmgr::call(proc_shape.prolang)? else {
        return Err(PgError::error(format!(
            "cache lookup failed for language {}",
            proc_shape.prolang
        ))
        .into());
    };
    if lang.lanvalidator != validator_oid {
        return Err(PgError::error(format!(
            "language validation function {validator_oid} called for language {} instead of {}",
            proc_shape.prolang, lang.lanvalidator
        ))
        .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE)
        .into());
    }
    Ok(())
}

// fmgr_c_validator (pg_proc.c). The load runs regardless of
// check_function_bodies, exactly as C ("for pg_dump loading it's much
// better if we *do* check").
fn fc_fmgr_c_validator(
    flinfo: Option<&mut types_fmgr::FmgrInfo>,
    fcinfo: &mut types_fmgr::FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let funcoid = fcinfo.arg(0).as_oid();
    // C reads the validator's own OID off flinfo->fn_oid; a builtin carrier
    // always has it, but fall back to this function's catalog OID.
    let validator_oid = flinfo.as_deref().map_or(FMGR_C_VALIDATOR_OID, |f| f.fn_oid);
    check_function_validator_access(validator_oid, funcoid)?;
    let cx = mcx::MemoryContext::new("fmgr_c_validator");
    // Past the gate the function IS a C-language function; pg_proc rows for
    // those always carry prosrc+probin. C's SysCacheGetAttrNotNull turns a
    // corrupt NULL into ERROR, not a crash — mirror that (was a panic:
    // fnconf campaign-2 ledger, OID 2247, pg_proc:1058).
    let prosrc = syscache_seams::lookup_pg_proc_prosrc::call(cx.mcx(), funcoid)?
        .ok_or_else(|| null_proc_column_err("prosrc"))?;
    let probin = syscache_seams::lookup_pg_proc_probin::call(cx.mcx(), funcoid)?
        .ok_or_else(|| null_proc_column_err("probin"))?;
    dfmgr::load_external_function(&probin, &prosrc, true)?;
    Ok(Datum::null())
}

const FMGR_C_VALIDATOR_OID: Oid = 2247;

// C SysCacheGetAttrNotNull's elog text (syscache.c).
#[track_caller]
#[cold]
fn null_proc_column_err(column: &str) -> Box<PgError> {
    PgError::error(format!(
        "unexpected null value in cached tuple for catalog pg_proc column {column}"
    ))
    .into()
}

static PG_PROC_BUILTINS: &[types_fmgr::FmgrBuiltin] = &[types_fmgr::FmgrBuiltin {
    foid: 2247,
    name: "fmgr_c_validator",
    nargs: 1,
    strict: true,
    retset: false,
    func: fc_fmgr_c_validator,
}];

pub fn init_seams() {
    fmgr_core::register_late_builtins(PG_PROC_BUILTINS);
    catalog_seams::function_parse_error_transpose::set(function_parse_error_transpose);
}

// IsThereFunctionInNamespace (pg_proc.c) with funcname_signature_string
// (parse_func.c) inlined for the no-argnames call shape.
pub fn IsThereFunctionInNamespace(
    mcx: Mcx<'_>,
    proname: &str,
    proargtypes: &[Oid],
    nsp_oid: Oid,
) -> PgResult<()> {
    // SAFETY: Oid is u32; viewing the slice as bytes for the oidvector cache
    // key has no padding or aliasing hazard.
    let argbytes = unsafe {
        core::slice::from_raw_parts(proargtypes.as_ptr() as *const u8, 4 * proargtypes.len())
    };
    if cache_syscache::SearchSysCacheExists(
        cache_syscache::cacheinfo::PROCNAMEARGSNSP,
        cache_syscache::SysCacheKey::Str(proname),
        cache_syscache::SysCacheKey::Bytes(argbytes),
        cache_syscache::SysCacheKey::Value(Datum::from_oid(nsp_oid)),
        cache_syscache::SysCacheKey::UNUSED,
    )? {
        let mut sig = String::new();
        sig.push_str(proname);
        sig.push('(');
        for (i, t) in proargtypes.iter().enumerate() {
            if i > 0 {
                sig.push_str(", ");
            }
            sig.push_str(&format_type::format_type_be(*t)?);
        }
        sig.push(')');
        let nspname = lsyscache::get_namespace_name(mcx, nsp_oid)?
            .map(|n| n.to_string())
            .unwrap_or_default();
        return Err(err(
            format!("function {sig} already exists in schema \"{nspname}\""),
            ERRCODE_DUPLICATE_FUNCTION,
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proargdefaults_text_excludes_varlena_header() {
        let ctx = mcx::MemoryContext::new("t");
        let mcx = ctx.mcx();
        let node_text = "({CONST :consttype 23 :consttypmod -1 :constcollid 0 :constlen 4 :constbyval true :constisnull false :location -1 :constvalue 4 [ 42 0 0 0 0 0 0 0 ]})";
        let t = varlena::cstring_to_text(mcx, node_text.as_bytes()).unwrap();
        let d = Datum::from_usize(t.as_bytes().as_ptr() as usize);
        assert_eq!(text_datum_to_string(mcx, d).unwrap(), node_text);
        let short_payload = b"({X})";
        let mut short = vec![(((short_payload.len() + 1) << 1) | 1) as u8];
        short.extend_from_slice(short_payload);
        let d = Datum::from_usize(short.as_ptr() as usize);
        assert_eq!(text_datum_to_string(mcx, d).unwrap(), "({X})");
    }
}
