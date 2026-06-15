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

use backend_bootstrap_bootstrap_seams as bootstrap;
use backend_nodes_makefuncs_seams as makefuncs;
use backend_nodes_read_seams as nodes_read;
use backend_utils_adt_format_type_seams as format_type;
use backend_utils_init_miscinit_seams as miscinit;
use backend_utils_cache_lsyscache_seams::{IOFuncSelector, TypLenByValAlign, TypeIoData};
use backend_utils_cache_syscache_seams as syscache;
use backend_utils_fmgr_fmgr_seams as fmgr;
use mcx::{Mcx, PgBox};
use types_nodes::nodes::Node;
use types_array::{ArrayElementIoData, ArrayIoFuncSelector};
use types_cache::typcache::{PgRangeRow, PgTypeRow};
use types_core::primitive::{InvalidOid, Oid, OidIsValid};
use types_datum::Datum;
use types_error::{
    PgError, PgResult, ERRCODE_UNDEFINED_FUNCTION, ERRCODE_UNDEFINED_OBJECT,
};
use types_tuple::pg_type::FormData_pg_type;

/// `TYPTYPE_*` (`catalog/pg_type.h`).
const TYPTYPE_DOMAIN: i8 = b'd' as i8;
const TYPTYPE_COMPOSITE: i8 = b'c' as i8;
const TYPTYPE_ENUM: i8 = b'e' as i8;
const TYPTYPE_RANGE: i8 = b'r' as i8;
const TYPTYPE_MULTIRANGE: i8 = b'm' as i8;

/// `TYPSTORAGE_PLAIN` (`catalog/pg_type.h`): `typstorage == 'p'`.
const TYPSTORAGE_PLAIN: i8 = b'p' as i8;

/// `RECORDOID` / `BPCHAROID` (`catalog/pg_type.dat`).
const RECORDOID: Oid = 2249;
const BPCHAROID: Oid = 1042;
/// `F_ARRAY_SUBSCRIPT_HANDLER` (`fmgroids.h`) — the `array_subscript_handler`
/// builtin OID; the `IsTrueArrayType` test.
const F_ARRAY_SUBSCRIPT_HANDLER: Oid = 6179;

/// `IsTrueArrayType(typeForm)` (`catalog/pg_type.h`): a "true" array type has a
/// valid `typelem` and the `array_subscript_handler` as its `typsubscript`.
fn is_true_array_type(typtup: &FormData_pg_type) -> bool {
    OidIsValid(typtup.typelem) && typtup.typsubscript == F_ARRAY_SUBSCRIPT_HANDLER
}

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
///
/// ```c
/// void get_type_io_data(Oid typid, IOFuncSelector which_func, int16 *typlen,
///                       bool *typbyval, char *typalign, char *typdelim,
///                       Oid *typioparam, Oid *func) {
///     /* In bootstrap mode, pass it off to bootstrap.c.  This hack allows us
///      * to use array_in and array_out during bootstrap. */
///     if (IsBootstrapProcessingMode()) {
///         Oid typinput, typoutput;
///         boot_get_type_io_data(typid, typlen, typbyval, typalign, typdelim,
///                               typioparam, &typinput, &typoutput);
///         switch (which_func) {
///             case IOFunc_input:  *func = typinput;  break;
///             case IOFunc_output: *func = typoutput; break;
///             default: elog(ERROR, "binary I/O not supported during bootstrap"); break;
///         }
///         return;
///     }
///     typeTuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
///     if (!HeapTupleIsValid(typeTuple)) elog(ERROR, "cache lookup failed for type %u", typid);
///     ...
/// }
/// ```
pub fn get_type_io_data(typid: Oid, which_func: IOFuncSelector) -> PgResult<TypeIoData> {
    // In bootstrap mode, pass it off to bootstrap.c.  This hack allows us to
    // use array_in and array_out during bootstrap.
    if miscinit::is_bootstrap_processing_mode::call() {
        let boot = bootstrap::boot_get_type_io_data::call(typid)?;
        let func = match which_func {
            IOFuncSelector::Input => boot.typinput,
            IOFuncSelector::Output => boot.typoutput,
            // case IOFunc_receive / IOFunc_send:
            //   elog(ERROR, "binary I/O not supported during bootstrap");
            IOFuncSelector::Receive | IOFuncSelector::Send => {
                return Err(PgError::error(
                    "binary I/O not supported during bootstrap".to_string(),
                ));
            }
        };
        return Ok(TypeIoData {
            typlen: boot.typlen,
            typbyval: boot.typbyval,
            typalign: boot.typalign,
            typdelim: boot.typdelim,
            typioparam: boot.typioparam,
            func,
        });
    }

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
///         ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
///                 errmsg("type %s is only a shell", format_type_be(type))));
///     if (!OidIsValid(pt->typoutput))
///         ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
///                 errmsg("no output function available for type %s", format_type_be(type))));
///     *typOutput = pt->typoutput;
///     *typIsVarlena = (!pt->typbyval) && (pt->typlen == -1);
///     ReleaseSysCache(typeTuple);
/// }
/// ```
pub fn get_type_output_info(typid: Oid) -> PgResult<(Oid, bool)> {
    let pt = match syscache::pg_type_form::call(typid)? {
        Some(t) => t,
        None => return cache_lookup_failed_for_type(typid),
    };

    // if (!pt->typisdefined)
    //     ereport(ERROR, errcode(ERRCODE_UNDEFINED_OBJECT),
    //             errmsg("type %s is only a shell", format_type_be(type)));
    if !pt.typisdefined {
        return Err(PgError::error(format!(
            "type {} is only a shell",
            format_type::format_type_be_str::call(typid)?
        ))
        .with_sqlstate(ERRCODE_UNDEFINED_OBJECT));
    }

    // if (!OidIsValid(pt->typoutput))
    //     ereport(ERROR, errcode(ERRCODE_UNDEFINED_FUNCTION),
    //             errmsg("no output function available for type %s", format_type_be(type)));
    if !OidIsValid(pt.typoutput) {
        return Err(PgError::error(format!(
            "no output function available for type {}",
            format_type::format_type_be_str::call(typid)?
        ))
        .with_sqlstate(ERRCODE_UNDEFINED_FUNCTION));
    }

    let typ_output = pt.typoutput;
    let typ_is_varlena = (!pt.typbyval) && (pt.typlen == -1);

    Ok((typ_output, typ_is_varlena))
}

/// `getTypeInputInfo(type, &typInput, &typIOParam)` (lsyscache.c).
///
/// ```c
/// void getTypeInputInfo(Oid type, Oid *typInput, Oid *typIOParam) {
///     HeapTuple typeTuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type));
///     if (!HeapTupleIsValid(typeTuple)) elog(ERROR, "cache lookup failed for type %u", type);
///     pt = (Form_pg_type) GETSTRUCT(typeTuple);
///     if (!pt->typisdefined)
///         ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
///                 errmsg("type %s is only a shell", format_type_be(type))));
///     if (!OidIsValid(pt->typinput))
///         ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
///                 errmsg("no input function available for type %s", format_type_be(type))));
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

    // if (!pt->typisdefined)
    //     ereport(ERROR, errcode(ERRCODE_UNDEFINED_OBJECT),
    //             errmsg("type %s is only a shell", format_type_be(type)));
    if !pt.typisdefined {
        return Err(PgError::error(format!(
            "type {} is only a shell",
            format_type::format_type_be_str::call(typ)?
        ))
        .with_sqlstate(ERRCODE_UNDEFINED_OBJECT));
    }
    // if (!OidIsValid(pt->typinput))
    //     ereport(ERROR, errcode(ERRCODE_UNDEFINED_FUNCTION),
    //             errmsg("no input function available for type %s", format_type_be(type)));
    if !OidIsValid(pt.typinput) {
        return Err(PgError::error(format!(
            "no input function available for type {}",
            format_type::format_type_be_str::call(typ)?
        ))
        .with_sqlstate(ERRCODE_UNDEFINED_FUNCTION));
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
///     if (!pt->typisdefined)
///         ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
///                 errmsg("type %s is only a shell", format_type_be(type))));
///     if (!OidIsValid(pt->typsend))
///         ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
///                 errmsg("no binary output function available for type %s", format_type_be(type))));
///     *typSend = pt->typsend;
///     *typIsVarlena = (!pt->typbyval) && (pt->typlen == -1);
///     ReleaseSysCache(typeTuple);
/// }
/// ```
pub fn get_type_binary_output_info(type_oid: Oid) -> PgResult<(Oid, bool)> {
    let pt = match syscache::pg_type_form::call(type_oid)? {
        Some(t) => t,
        None => return cache_lookup_failed_for_type(type_oid),
    };

    // if (!pt->typisdefined)
    //     ereport(ERROR, errcode(ERRCODE_UNDEFINED_OBJECT),
    //             errmsg("type %s is only a shell", format_type_be(type)));
    if !pt.typisdefined {
        return Err(PgError::error(format!(
            "type {} is only a shell",
            format_type::format_type_be_str::call(type_oid)?
        ))
        .with_sqlstate(ERRCODE_UNDEFINED_OBJECT));
    }
    // if (!OidIsValid(pt->typsend))
    //     ereport(ERROR, errcode(ERRCODE_UNDEFINED_FUNCTION),
    //             errmsg("no binary output function available for type %s", format_type_be(type)));
    if !OidIsValid(pt.typsend) {
        return Err(PgError::error(format!(
            "no binary output function available for type {}",
            format_type::format_type_be_str::call(type_oid)?
        ))
        .with_sqlstate(ERRCODE_UNDEFINED_FUNCTION));
    }

    let typ_send = pt.typsend;
    let typ_is_varlena = (!pt.typbyval) && (pt.typlen == -1);

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
            // Not a domain, so stop descending; return element type if any.
            // This test must match get_element_type (IsTrueArrayType).
            let result = if is_true_array_type(&typ_tup) {
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
            // NB: only "true" arrays (array_subscript_handler) succeed, exactly
            // as C's IsTrueArrayType, independent of typelem/typsubscript on
            // other types.
            let result = if is_true_array_type(&typtup) {
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

// ===========================================================================
// Remaining pg_type scalar reads (PG 18.3).
// ===========================================================================

/// `get_typisdefined(typid)` (lsyscache.c): `typisdefined`, or `false` if
/// absent.
pub fn get_typisdefined(typid: Oid) -> PgResult<bool> {
    match syscache::pg_type_form::call(typid)? {
        Some(typtup) => Ok(typtup.typisdefined),
        None => Ok(false),
    }
}

/// `get_typlen(typid)` (lsyscache.c): `typlen`, or `0` if absent.
pub fn get_typlen(typid: Oid) -> PgResult<i16> {
    match syscache::pg_type_form::call(typid)? {
        Some(typtup) => Ok(typtup.typlen),
        None => Ok(0),
    }
}

/// `get_typbyval(typid)` (lsyscache.c): `typbyval`, or `false` if absent.
pub fn get_typbyval(typid: Oid) -> PgResult<bool> {
    match syscache::pg_type_form::call(typid)? {
        Some(typtup) => Ok(typtup.typbyval),
        None => Ok(false),
    }
}

/// `get_typlenbyval(typid, &typlen, &typbyval)` (lsyscache.c): `(typlen,
/// typbyval)`; a missing type is `elog(ERROR, "cache lookup failed for type
/// %u")`.
pub fn get_typlenbyval(typid: Oid) -> PgResult<(i16, bool)> {
    match syscache::pg_type_form::call(typid)? {
        Some(typtup) => Ok((typtup.typlen, typtup.typbyval)),
        None => cache_lookup_failed_for_type(typid),
    }
}

/// `get_typstorage(typid)` (lsyscache.c): `typstorage`, or `TYPSTORAGE_PLAIN`
/// (`'p'`) if absent.
pub fn get_typstorage(typid: Oid) -> PgResult<u8> {
    match syscache::pg_type_form::call(typid)? {
        Some(typtup) => Ok(typtup.typstorage as u8),
        None => Ok(TYPSTORAGE_PLAIN as u8),
    }
}

/// `get_typtype(typid)` (lsyscache.c): `typtype`, or `'\0'` if absent.
pub fn get_typtype(typid: Oid) -> PgResult<u8> {
    match syscache::pg_type_form::call(typid)? {
        Some(typtup) => Ok(typtup.typtype as u8),
        None => Ok(b'\0'),
    }
}

/// `type_is_rowtype(typid)` (lsyscache.c): RECORD, or a (possibly domain-over)
/// named composite type.
pub fn type_is_rowtype(typid: Oid) -> PgResult<bool> {
    if typid == RECORDOID {
        return Ok(true); // easy case
    }
    let typtype = get_typtype(typid)? as i8;
    match typtype {
        TYPTYPE_COMPOSITE => Ok(true),
        TYPTYPE_DOMAIN => {
            if (get_typtype(get_base_type(typid)?)? as i8) == TYPTYPE_COMPOSITE {
                Ok(true)
            } else {
                Ok(false)
            }
        }
        _ => Ok(false),
    }
}

/// `type_is_enum(typid)` (lsyscache.c).
pub fn type_is_enum(typid: Oid) -> PgResult<bool> {
    Ok((get_typtype(typid)? as i8) == TYPTYPE_ENUM)
}

/// `type_is_range(typid)` (lsyscache.c).
pub fn type_is_range(typid: Oid) -> PgResult<bool> {
    Ok((get_typtype(typid)? as i8) == TYPTYPE_RANGE)
}

/// `type_is_multirange(typid)` (lsyscache.c).
pub fn type_is_multirange(typid: Oid) -> PgResult<bool> {
    Ok((get_typtype(typid)? as i8) == TYPTYPE_MULTIRANGE)
}

/// `get_type_category_preferred(typid, &typcategory, &typispreferred)`
/// (lsyscache.c): `(typcategory, typispreferred)`; a missing type is
/// `elog(ERROR)`.
pub fn get_type_category_preferred(typid: Oid) -> PgResult<(u8, bool)> {
    match syscache::pg_type_form::call(typid)? {
        Some(typtup) => Ok((typtup.typcategory as u8, typtup.typispreferred)),
        None => cache_lookup_failed_for_type(typid),
    }
}

/// `get_typ_typrelid(typid)` (lsyscache.c): `typrelid`, or `InvalidOid` if
/// absent or not a complex type.
pub fn get_typ_typrelid(typid: Oid) -> PgResult<Oid> {
    match syscache::pg_type_form::call(typid)? {
        Some(typtup) => Ok(typtup.typrelid),
        None => Ok(InvalidOid),
    }
}

/// `get_promoted_array_type(typid)` (lsyscache.c): the "true" array type of a
/// scalar, or the type itself if already a true array, else `InvalidOid`.
///
/// ```c
/// Oid array_type = get_array_type(typid);
/// if (OidIsValid(array_type)) return array_type;
/// if (OidIsValid(get_element_type(typid))) return typid;
/// return InvalidOid;
/// ```
pub fn get_promoted_array_type(typid: Oid) -> PgResult<Oid> {
    let array_type = get_array_type(typid)?; // Option<Oid> (None == InvalidOid)
    if let Some(at) = array_type {
        return Ok(at);
    }
    if get_element_type(typid)?.is_some() {
        return Ok(typid);
    }
    Ok(InvalidOid)
}

/// `getTypeBinaryInputInfo(type, &typReceive, &typIOParam)` (lsyscache.c).
///
/// ```c
/// if (!pt->typisdefined) ereport(ERROR, ERRCODE_UNDEFINED_OBJECT, "type %s is only a shell");
/// if (!OidIsValid(pt->typreceive)) ereport(ERROR, ERRCODE_UNDEFINED_FUNCTION, "no binary input function available for type %s");
/// *typReceive = pt->typreceive;
/// *typIOParam = getTypeIOParam(typeTuple);
/// ```
pub fn get_type_binary_input_info(typ: Oid) -> PgResult<(Oid, Oid)> {
    let pt = match syscache::pg_type_form::call(typ)? {
        Some(t) => t,
        None => return cache_lookup_failed_for_type(typ),
    };

    if !pt.typisdefined {
        return Err(PgError::error(format!(
            "type {} is only a shell",
            format_type::format_type_be_str::call(typ)?
        ))
        .with_sqlstate(ERRCODE_UNDEFINED_OBJECT));
    }
    if !OidIsValid(pt.typreceive) {
        return Err(PgError::error(format!(
            "no binary input function available for type {}",
            format_type::format_type_be_str::call(typ)?
        ))
        .with_sqlstate(ERRCODE_UNDEFINED_FUNCTION));
    }

    Ok((pt.typreceive, get_type_io_param(&pt)))
}

/// `get_typmodin(typid)` (lsyscache.c): `typmodin`, or `InvalidOid`.
pub fn get_typmodin(typid: Oid) -> PgResult<Oid> {
    match syscache::pg_type_form::call(typid)? {
        Some(typtup) => Ok(typtup.typmodin),
        None => Ok(InvalidOid),
    }
}

/// `get_typmodout(typid)` (lsyscache.c): `typmodout`, or `InvalidOid`. (PG
/// marks this `#ifdef NOT_USED`; ported for C-source completeness.)
pub fn get_typmodout(typid: Oid) -> PgResult<Oid> {
    match syscache::pg_type_form::call(typid)? {
        Some(typtup) => Ok(typtup.typmodout),
        None => Ok(InvalidOid),
    }
}

/// `get_typcollation(typid)` (lsyscache.c): `typcollation`, or `InvalidOid`.
pub fn get_typcollation(typid: Oid) -> PgResult<Oid> {
    match syscache::pg_type_form::call(typid)? {
        Some(typtup) => Ok(typtup.typcollation),
        None => Ok(InvalidOid),
    }
}

/// `type_is_collatable(typid)` (lsyscache.c): `OidIsValid(get_typcollation)`.
pub fn type_is_collatable(typid: Oid) -> PgResult<bool> {
    Ok(OidIsValid(get_typcollation(typid)?))
}

/// `get_typsubscript(typid, &typelem)` (lsyscache.c): `(typsubscript,
/// typelem)`; `(InvalidOid, InvalidOid)` if absent.
pub fn get_typsubscript(typid: Oid) -> PgResult<(Oid, Oid)> {
    match syscache::pg_type_form::call(typid)? {
        Some(typform) => Ok((typform.typsubscript, typform.typelem)),
        None => Ok((InvalidOid, InvalidOid)),
    }
}

/// `getSubscriptingRoutines(typid, &typelem)` (lsyscache.c).
///
/// ```c
/// RegProcedure typsubscript = get_typsubscript(typid, typelemp);
/// if (!OidIsValid(typsubscript)) return NULL;
/// return (const struct SubscriptRoutines *) DatumGetPointer(OidFunctionCall0(typsubscript));
/// ```
///
/// The `OidFunctionCall0` routes through the fmgr owner's seam; its returned
/// `Datum` is the `const SubscriptRoutines *` pointer word, kept opaque (the
/// struct lives in `nodes/subscripting.h`, outside this unit, and no ported
/// caller consumes it yet). `None` is the C NULL (not subscriptable).
pub fn get_subscripting_routines(typid: Oid) -> PgResult<Option<(Datum, Oid)>> {
    let (typsubscript, typelem) = get_typsubscript(typid)?;
    if !OidIsValid(typsubscript) {
        return Ok(None);
    }
    let routines = fmgr::oid_function_call0::call(typsubscript)?;
    Ok(Some((routines, typelem)))
}

/// `get_typavgwidth(typid, typmod)` (lsyscache.c): the planner's estimated
/// average value width for the type.
///
/// ```c
/// int typlen = get_typlen(typid);
/// if (typlen > 0) return typlen;
/// maxwidth = type_maximum_size(typid, typmod);
/// if (maxwidth > 0) {
///     if (typid == BPCHAROID) return maxwidth;
///     if (maxwidth <= 32) return maxwidth;
///     if (maxwidth < 1000) return 32 + (maxwidth - 32) / 2;
///     return 32 + (1000 - 32) / 2;
/// }
/// return 32;
/// ```
///
/// The `get_attavgwidth_hook` planner hook is never installed in this port, so
/// (as in C with a NULL hook) only the catalog path runs.
pub fn get_typavgwidth(typid: Oid, typmod: i32) -> PgResult<i32> {
    let typlen = get_typlen(typid)? as i32;

    // Easy if it's a fixed-width type
    if typlen > 0 {
        return Ok(typlen);
    }

    // type_maximum_size knows the encoding of typmod for some datatypes.
    let maxwidth = format_type::type_maximum_size::call(typid, typmod)?;
    if maxwidth > 0 {
        // For BPCHAR, the max width is also the only width.
        if typid == BPCHAROID {
            return Ok(maxwidth);
        }
        if maxwidth <= 32 {
            return Ok(maxwidth); // assume full width
        }
        if maxwidth < 1000 {
            return Ok(32 + (maxwidth - 32) / 2); // assume 50%
        }
        // Beyond 1000, use a fixed estimate.
        return Ok(32 + (1000 - 32) / 2);
    }

    // Oops, we have no idea ... wild guess time.
    Ok(32)
}

/// `get_typdefault(typid)` (lsyscache.c): the type's default-value expression
/// node tree, or `None` if there is no defined default.
///
/// ```c
/// typeTuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
/// if (!HeapTupleIsValid(typeTuple)) elog(ERROR, "cache lookup failed for type %u", typid);
/// type = (Form_pg_type) GETSTRUCT(typeTuple);
/// datum = SysCacheGetAttr(TYPEOID, typeTuple, Anum_pg_type_typdefaultbin, &isNull);
/// if (!isNull) {
///     expr = stringToNode(TextDatumGetCString(datum));
/// } else {
///     datum = SysCacheGetAttr(TYPEOID, typeTuple, Anum_pg_type_typdefault, &isNull);
///     if (!isNull) {
///         strDefaultVal = TextDatumGetCString(datum);
///         datum = OidInputFunctionCall(type->typinput, strDefaultVal, getTypeIOParam(typeTuple), -1);
///         expr = (Node *) makeConst(typid, -1, type->typcollation, type->typlen,
///                                   datum, false, type->typbyval);
///         pfree(strDefaultVal);
///     } else {
///         expr = NULL;
///     }
/// }
/// ReleaseSysCache(typeTuple);
/// return expr;
/// ```
///
/// The two `SysCacheGetAttr` + `TextDatumGetCString` extractions are folded
/// into the syscache `pg_type_default` projection (they return owned default
/// text); the `stringToNode` / `OidInputFunctionCall` / `makeConst` callees
/// route through their owners' seams. The branch structure is reproduced here.
pub fn get_typdefault<'mcx>(mcx: Mcx<'mcx>, typid: Oid) -> PgResult<Option<PgBox<'mcx, Node<'mcx>>>> {
    let t = match syscache::pg_type_default::call(mcx, typid)? {
        Some(t) => t,
        None => return cache_lookup_failed_for_type(typid),
    };

    // typdefaultbin and typdefault are potentially null; the projection carries
    // each as Option<String> (the C SysCacheGetAttr + isNull test).
    if let Some(bin) = t.typdefaultbin {
        // We have an expression default.
        let expr = nodes_read::string_to_node::call(mcx, &bin)?;
        Ok(Some(expr))
    } else if let Some(str_default_val) = t.typdefault {
        // Perhaps we have a plain literal default. Convert the string to a value
        // of the given type, then build a Const node containing it.
        // `oid_input_function_call` yields the canonical unified value (its
        // `ByVal` arm is C's by-value `Datum`; `ByRef` carries the referent
        // bytes) — `make_const_node` (make_const's `Const.constvalue`) takes it
        // directly.
        let datum = fmgr::oid_input_function_call::call(
            mcx,
            t.typinput,
            &str_default_val,
            t.typioparam,
            -1,
        )?;
        let expr = makefuncs::make_const_node::call(
            mcx,
            typid,
            -1,
            t.typcollation,
            t.typlen as i32,
            datum,
            false,
            t.typbyval,
        )?;
        Ok(Some(expr))
    } else {
        // No default
        Ok(None)
    }
}

/// `agg_args_support_sendreceive`'s `pg_type` probe (parse_agg.c) — fetch a
/// type's `typbyval`/`typsend`/`typreceive` via the TYPEOID syscache.
///
/// ```c
/// typeTuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type));
/// if (!HeapTupleIsValid(typeTuple)) elog(ERROR, "cache lookup failed for type %u", type);
/// pt = (Form_pg_type) GETSTRUCT(typeTuple);
/// /* read pt->typbyval, pt->typsend, pt->typreceive */
/// ```
pub fn get_type_sendreceive_byval(
    type_oid: Oid,
) -> PgResult<backend_utils_cache_lsyscache_seams::TypeSendReceive> {
    let pt = match syscache::pg_type_form::call(type_oid)? {
        Some(t) => t,
        None => return cache_lookup_failed_for_type(type_oid),
    };
    Ok(backend_utils_cache_lsyscache_seams::TypeSendReceive {
        typbyval: pt.typbyval,
        typsend: pt.typsend,
        typreceive: pt.typreceive,
    })
}
