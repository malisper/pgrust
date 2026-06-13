//! `type` family — `lsyscache.c` lookups keyed on `pg_type` / `pg_range`
//! (`TYPEOID` / `RANGETYPE` syscaches and the type-I/O helpers).
//!
//! Ported 1:1 from `utils/cache/lsyscache.c`. Each function mirrors its C
//! entry point's `SearchSysCache1(...)` + `GETSTRUCT(...)` shape; the
//! `SearchSysCache` / `GetSysCacheHashValue1` primitives belong to the
//! `backend-utils-cache-syscache` owner and are reached through that unit's
//! per-owner seams (they panic loudly until the syscache unit lands).
//!
//! C entry points covered here: `get_typlenbyvalalign`, `get_type_io_data`,
//! `getTypeOutputInfo`, `getTypeInputInfo`, `getTypeBinaryOutputInfo`,
//! `getBaseType`, `getBaseTypeAndTypmod`, `get_base_element_type`,
//! `get_element_type`, `get_array_type`, `get_multirange_range`, plus the
//! typcache-driven `RANGETYPE` / `TYPEOID` row probes and the arrayfuncs.c
//! element-I/O projection.

use backend_utils_cache_lsyscache_seams::{IOFuncSelector, TypLenByValAlign, TypeIoData};
use backend_utils_cache_syscache_seams as syscache;
use types_array::{ArrayElementIoData, ArrayIoFuncSelector};
use types_cache::typcache::{PgRangeRow, PgTypeRow};
use types_core::primitive::{InvalidOid, Oid, OidIsValid};
use types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED};
use types_tuple::pg_type::FormData_pg_type;

/// `TYPTYPE_DOMAIN` (`catalog/pg_type.h`): `typtype == 'd'`.
const TYPTYPE_DOMAIN: i8 = b'd' as i8;

/// `elog(ERROR, "cache lookup failed for type %u", typid)`.
fn cache_lookup_failed_for_type<T>(typid: Oid) -> PgResult<T> {
    Err(PgError::error(format!(
        "cache lookup failed for type {typid}"
    )))
}

/// `getTypeIOParam(typeTuple)` (lsyscache.c): the I/O parameter OID a type's
/// I/O functions need. For array types this is the element type; otherwise it
/// is the type's own OID.
///
/// ```c
/// Oid getTypeIOParam(HeapTuple typeTuple) {
///     Form_pg_type typeStruct = (Form_pg_type) GETSTRUCT(typeTuple);
///     return OidIsValid(typeStruct->typelem) ? typeStruct->typelem : typeStruct->oid;
/// }
/// ```
fn get_type_io_param(type_struct: &FormData_pg_type) -> Oid {
    if OidIsValid(type_struct.typelem) {
        type_struct.typelem
    } else {
        type_struct.oid
    }
}

/// `get_typlenbyvalalign(typid, &typlen, &typbyval, &typalign)` (lsyscache.c).
pub fn get_typlenbyvalalign(typid: Oid) -> PgResult<TypLenByValAlign> {
    // tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    // if (!HeapTupleIsValid(tp)) elog(ERROR, "cache lookup failed for type %u", typid);
    let typtup = match syscache::pg_type_form::call(typid)? {
        Some(t) => t,
        None => return cache_lookup_failed_for_type(typid),
    };
    Ok(TypLenByValAlign {
        typlen: typtup.typlen,
        typbyval: typtup.typbyval,
        typalign: typtup.typalign,
    })
}

/// `get_type_io_data(typid, which_func, ...)` (lsyscache.c) — canonical
/// `IOFuncSelector` / `TypeIoData` shape.
pub fn get_type_io_data(typid: Oid, which_func: IOFuncSelector) -> PgResult<TypeIoData> {
    // typeTuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    // if (!HeapTupleIsValid(typeTuple)) elog(ERROR, "cache lookup failed for type %u", typid);
    let type_struct = match syscache::pg_type_form::call(typid)? {
        Some(t) => t,
        None => return cache_lookup_failed_for_type(typid),
    };

    let func = match which_func {
        IOFuncSelector::Input => type_struct.typinput,
        IOFuncSelector::Output => type_struct.typoutput,
        IOFuncSelector::Receive => type_struct.typreceive,
        IOFuncSelector::Send => type_struct.typsend,
    };

    Ok(TypeIoData {
        typlen: type_struct.typlen,
        typbyval: type_struct.typbyval,
        typalign: type_struct.typalign,
        typdelim: type_struct.typdelim,
        typioparam: get_type_io_param(&type_struct),
        func,
    })
}

/// `getTypeOutputInfo(type, &typOutput, &typIsVarlena)` (lsyscache.c).
///
/// ```c
/// void getTypeOutputInfo(Oid type, Oid *typOutput, bool *typIsVarlena) {
///     HeapTuple typeTuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type));
///     if (!HeapTupleIsValid(typeTuple)) elog(ERROR, "cache lookup failed for type %u", type);
///     pt = (Form_pg_type) GETSTRUCT(typeTuple);
///     if (!pt->typisdefined)
///         ereport(ERROR, errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
///                 errmsg("cannot display a value of type %s", ...));  -- shell type
///     *typOutput = pt->typoutput;
///     *typIsVarlena = (!pt->typbyval) && (pt->typlen == -1);
///     ...
///     ReleaseSysCache(typeTuple);
/// }
/// ```
pub fn get_type_output_info(typid: Oid) -> PgResult<(Oid, bool)> {
    let pt = match syscache::pg_type_form::call(typid)? {
        Some(t) => t,
        None => return cache_lookup_failed_for_type(typid),
    };

    // if (!pt->typisdefined) ereport(ERROR, "cannot output a value of type %s, which is still being defined");
    if !pt.typisdefined {
        return Err(PgError::error(format!(
            "cannot output a value of type {}, which is still being defined",
            String::from_utf8_lossy(pt.typname.name_str())
        ))
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
    }

    let typ_output = pt.typoutput;
    let typ_is_varlena = (!pt.typbyval) && (pt.typlen == -1);

    // if (!OidIsValid(typOutput)) ereport(ERROR, "no output function available for type %s");
    if !OidIsValid(typ_output) {
        return Err(PgError::error(format!(
            "no output function available for type {}",
            String::from_utf8_lossy(pt.typname.name_str())
        ))
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
    }

    Ok((typ_output, typ_is_varlena))
}

/// `getTypeInputInfo(type, &typInput, &typIOParam)` (lsyscache.c).
///
/// ```c
/// void getTypeInputInfo(Oid type, Oid *typInput, Oid *typIOParam) {
///     HeapTuple typeTuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type));
///     if (!HeapTupleIsValid(typeTuple)) elog(ERROR, "cache lookup failed for type %u", type);
///     pt = (Form_pg_type) GETSTRUCT(typeTuple);
///     if (!pt->typisdefined) ereport(ERROR, "cannot accept a value of type %s, which is still being defined");
///     if (!OidIsValid(pt->typinput)) ereport(ERROR, "no input function available for type %s");
///     *typInput = pt->typinput;
///     *typIOParam = getTypeIOParam(typeTuple);
///     ReleaseSysCache(typeTuple);
/// }
/// ```
pub fn get_type_input_info(typ: Oid) -> PgResult<(Oid, Oid)> {
    let pt = match syscache::pg_type_form::call(typ)? {
        Some(t) => t,
        None => return cache_lookup_failed_for_type(typ),
    };

    if !pt.typisdefined {
        return Err(PgError::error(format!(
            "cannot accept a value of type {}, which is still being defined",
            String::from_utf8_lossy(pt.typname.name_str())
        ))
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
    }
    if !OidIsValid(pt.typinput) {
        return Err(PgError::error(format!(
            "no input function available for type {}",
            String::from_utf8_lossy(pt.typname.name_str())
        ))
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
    }

    Ok((pt.typinput, get_type_io_param(&pt)))
}

/// `getTypeBinaryOutputInfo(type, &typSend, &typIsVarlena)` (lsyscache.c).
///
/// ```c
/// void getTypeBinaryOutputInfo(Oid type, Oid *typSend, bool *typIsVarlena) {
///     HeapTuple typeTuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type));
///     if (!HeapTupleIsValid(typeTuple)) elog(ERROR, "cache lookup failed for type %u", type);
///     pt = (Form_pg_type) GETSTRUCT(typeTuple);
///     if (!pt->typisdefined) ereport(ERROR, "cannot send a value of type %s, which is still being defined");
///     *typSend = pt->typsend;
///     *typIsVarlena = (!pt->typbyval) && (pt->typlen == -1);
///     if (!OidIsValid(*typSend)) ereport(ERROR, "no binary output function available for type %s");
///     ReleaseSysCache(typeTuple);
/// }
/// ```
pub fn get_type_binary_output_info(type_oid: Oid) -> PgResult<(Oid, bool)> {
    let pt = match syscache::pg_type_form::call(type_oid)? {
        Some(t) => t,
        None => return cache_lookup_failed_for_type(type_oid),
    };

    if !pt.typisdefined {
        return Err(PgError::error(format!(
            "cannot send a value of type {}, which is still being defined",
            String::from_utf8_lossy(pt.typname.name_str())
        ))
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
    }

    let typ_send = pt.typsend;
    let typ_is_varlena = (!pt.typbyval) && (pt.typlen == -1);

    if !OidIsValid(typ_send) {
        return Err(PgError::error(format!(
            "no binary output function available for type {}",
            String::from_utf8_lossy(pt.typname.name_str())
        ))
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
    }

    Ok((typ_send, typ_is_varlena))
}

/// `getBaseType(typid)` (lsyscache.c): `getBaseTypeAndTypmod` with
/// `typmod = -1`.
///
/// ```c
/// Oid getBaseType(Oid typid) { int32 typmod = -1; return getBaseTypeAndTypmod(typid, &typmod); }
/// ```
pub fn get_base_type(typid: Oid) -> PgResult<Oid> {
    let (base, _typmod) = get_base_type_and_typmod(typid)?;
    Ok(base)
}

/// `getBaseTypeAndTypmod(type_id, &typmod)` (lsyscache.c).
///
/// ```c
/// Oid getBaseTypeAndTypmod(Oid typid, int32 *typmod) {
///     for (;;) {
///         HeapTuple tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
///         if (!HeapTupleIsValid(tup)) elog(ERROR, "cache lookup failed for type %u", typid);
///         typTup = (Form_pg_type) GETSTRUCT(tup);
///         if (typTup->typtype != TYPTYPE_DOMAIN) { ReleaseSysCache(tup); break; }
///         Assert(*typmod == -1);
///         typid = typTup->typbasetype;
///         *typmod = typTup->typtypmod;
///         ReleaseSysCache(tup);
///     }
///     return typid;
/// }
/// ```
pub fn get_base_type_and_typmod(type_id: Oid) -> PgResult<(Oid, i32)> {
    let mut typid = type_id;
    let mut typmod: i32 = -1;
    loop {
        let typ_tup = match syscache::pg_type_form::call(typid)? {
            Some(t) => t,
            None => return cache_lookup_failed_for_type(typid),
        };
        if typ_tup.typtype != TYPTYPE_DOMAIN {
            // Not a domain, so done
            break;
        }
        // Else, use parameters of parent type
        // Assert(*typmod == -1); -- (invariant; domains never set a typmod twice)
        typid = typ_tup.typbasetype;
        typmod = typ_tup.typtypmod;
    }
    Ok((typid, typmod))
}

/// `get_base_element_type(type_id)` (lsyscache.c).
///
/// ```c
/// Oid get_base_element_type(Oid typid) {
///     for (;;) {
///         HeapTuple tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
///         if (!HeapTupleIsValid(tup)) break;
///         typTup = (Form_pg_type) GETSTRUCT(tup);
///         if (typTup->typtype != TYPTYPE_DOMAIN) {
///             Oid result;
///             if (typTup->typlen == -1) result = typTup->typelem; else result = InvalidOid;
///             ReleaseSysCache(tup);
///             return result;
///         }
///         typid = typTup->typbasetype;
///         ReleaseSysCache(tup);
///     }
///     return InvalidOid;
/// }
/// ```
pub fn get_base_element_type(type_id: Oid) -> PgResult<Oid> {
    let mut typid = type_id;
    loop {
        // We loop to find the bottom base type in a stack of domains.
        let typ_tup = match syscache::pg_type_form::call(typid)? {
            Some(t) => t,
            None => break, // shouldn't happen, but treat like base type
        };

        if typ_tup.typtype != TYPTYPE_DOMAIN {
            // Not a domain, so stop descending; return element type if any
            let result = if typ_tup.typlen == -1 {
                typ_tup.typelem
            } else {
                InvalidOid
            };
            return Ok(result);
        }

        typid = typ_tup.typbasetype;
    }
    Ok(InvalidOid)
}

/// `get_element_type(array_type)` (lsyscache.c).
///
/// ```c
/// Oid get_element_type(Oid typid) {
///     HeapTuple tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
///     if (HeapTupleIsValid(tp)) {
///         Form_pg_type typtup = (Form_pg_type) GETSTRUCT(tp);
///         Oid result;
///         if (typtup->typlen == -1) result = typtup->typelem; else result = InvalidOid;
///         ReleaseSysCache(tp);
///         return result;
///     }
///     return InvalidOid;
/// }
/// ```
///
/// `None` mirrors the C `InvalidOid` return.
pub fn get_element_type(array_type: Oid) -> PgResult<Option<Oid>> {
    match syscache::pg_type_form::call(array_type)? {
        Some(typtup) => {
            let result = if typtup.typlen == -1 {
                typtup.typelem
            } else {
                InvalidOid
            };
            // Caller treats InvalidOid as "no element type".
            Ok(oid_to_option(result))
        }
        None => Ok(None),
    }
}

/// `get_array_type(input_type)` (lsyscache.c).
///
/// ```c
/// Oid get_array_type(Oid typid) {
///     HeapTuple tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
///     Oid result = InvalidOid;
///     if (HeapTupleIsValid(tp)) {
///         result = ((Form_pg_type) GETSTRUCT(tp))->typarray;
///         ReleaseSysCache(tp);
///     }
///     return result;
/// }
/// ```
///
/// `None` mirrors the C `InvalidOid` return.
pub fn get_array_type(input_type: Oid) -> PgResult<Option<Oid>> {
    let result = match syscache::pg_type_form::call(input_type)? {
        Some(tp) => tp.typarray,
        None => InvalidOid,
    };
    Ok(oid_to_option(result))
}

/// `get_type_io_data(...)` array-element projection consumed by arrayfuncs.c.
///
/// This is the same `get_type_io_data` C entry point, projected to the
/// array-element shape arrayfuncs.c reads (`typalign`/`typdelim` carried as
/// `u8` in the element vocabulary).
pub fn get_array_element_io_data(
    element_type: Oid,
    which: ArrayIoFuncSelector,
) -> PgResult<ArrayElementIoData> {
    let which_func = match which {
        ArrayIoFuncSelector::Input => IOFuncSelector::Input,
        ArrayIoFuncSelector::Output => IOFuncSelector::Output,
        ArrayIoFuncSelector::Receive => IOFuncSelector::Receive,
        ArrayIoFuncSelector::Send => IOFuncSelector::Send,
    };
    let io = get_type_io_data(element_type, which_func)?;
    Ok(ArrayElementIoData {
        typlen: io.typlen,
        typbyval: io.typbyval,
        typalign: io.typalign as u8,
        typdelim: io.typdelim as u8,
        typioparam: io.typioparam,
        typiofunc: io.func,
    })
}

/// `get_multirange_range(multirange_type_id)` (lsyscache.c).
///
/// ```c
/// Oid get_multirange_range(Oid multirangeOid) {
///     HeapTuple tp = SearchSysCache1(RANGEMULTIRANGE, ObjectIdGetDatum(multirangeOid));
///     if (HeapTupleIsValid(tp)) {
///         Form_pg_range rngtup = (Form_pg_range) GETSTRUCT(tp);
///         Oid result = rngtup->rngtypid;
///         ReleaseSysCache(tp);
///         return result;
///     }
///     return InvalidOid;
/// }
/// ```
pub fn get_multirange_range(multirange_type_id: Oid) -> PgResult<Oid> {
    let result = match syscache::pg_range_rngtypid_of_multirange::call(multirange_type_id)? {
        Some(rngtypid) => rngtypid,
        None => InvalidOid,
    };
    Ok(result)
}

/// `SearchSysCache1(RANGETYPE, ...)` row probe for `load_rangetype_info`.
///
/// `None` mirrors the C `!HeapTupleIsValid` path; the typcache caller raises
/// its own `cache lookup failed for range type %u`.
pub fn lookup_pg_range(range_type_id: Oid) -> PgResult<Option<PgRangeRow>> {
    syscache::pg_range_form::call(range_type_id)
}

/// `SearchSysCache1(TYPEOID, ...)` row probe for `TypeCacheEntry` build.
///
/// Projects the `Form_pg_type` fields the typcache reads into `PgTypeRow`
/// (`typname` decoded for the shell-type error message). `None` mirrors the C
/// `!HeapTupleIsValid` path.
pub fn lookup_pg_type(type_id: Oid) -> PgResult<Option<PgTypeRow>> {
    match syscache::pg_type_form::call(type_id)? {
        Some(t) => Ok(Some(PgTypeRow {
            typname: String::from_utf8_lossy(t.typname.name_str()).into_owned(),
            typlen: t.typlen,
            typbyval: t.typbyval,
            typalign: t.typalign,
            typstorage: t.typstorage,
            typtype: t.typtype,
            typisdefined: t.typisdefined,
            typrelid: t.typrelid,
            typsubscript: t.typsubscript,
            typelem: t.typelem,
            typarray: t.typarray,
            typcollation: t.typcollation,
        })),
        None => Ok(None),
    }
}

/// `GetSysCacheHashValue1(TYPEOID, ObjectIdGetDatum(type_id))` (typcache).
pub fn syscache_hash_value_typeoid(type_id: Oid) -> PgResult<u32> {
    syscache::get_syscache_hash_value_typeoid::call(type_id)
}

/// Map a possibly-`InvalidOid` result to `Option<Oid>` (the `None` ==
/// `InvalidOid` convention the array-element seams use).
fn oid_to_option(oid: Oid) -> Option<Oid> {
    if OidIsValid(oid) {
        Some(oid)
    } else {
        None
    }
}
