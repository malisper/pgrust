//! `pg_proc`-row projection — `funcapi.c` lines 1379–1869.
//!
//! Extract argument types/names/modes, TRF types, the result column name, and
//! build the result `TupleDesc` of an OUT-parameter or RETURNS-TABLE function,
//! all from a `pg_proc` row.
//!
//! The C source reads the row's columns with `SysCacheGetAttr` /
//! `deconstruct_array_builtin` and validates the array shapes in place. Here
//! the `pg_proc` fetch and the per-column detoast / deconstruction are routed
//! through the owning units' seams (`backend-utils-cache-syscache` for the
//! `SearchSysCache1(PROCOID, ..)` projection, `backend-utils-adt-array-more`
//! for the `ArrayType` header / element projection, `backend-access-common`'s
//! `tupdesc.c` constructors for `CreateTemplateTupleDesc` /
//! `TupleDescInitEntry`); the shape-validity checks and all the business logic
//! (mode filtering, column naming, the OUT-arg count rules) stay here, 1:1 with
//! the C.

use mcx::{Mcx, MemoryContext, PgString, PgVec};
use ::types_core::primitive::AttrNumber;
use ::types_core::Oid;
// Bare-word machine-word `Datum` (`::datum::Datum`), aliased `ScalarWord`.
// The `proargnames` / `proargmodes` / `proallargtypes` parameters are the raw
// `pg_proc` column words the SQL caller passes in (C: `Datum`), forwarded
// untouched to the array owner's detoast/project seams (`text_array_datum`,
// `char_array_datum`, `oid_array_datum` — all still `::datum::Datum`); the
// `ScalarWord::null()` checks are the C `PointerGetDatum(NULL)` tests on those raw
// words. funcapi never owns the bytes, so the word stays at this audited
// column-input ABI edge until the arrayfuncs seams migrate.
use ::datum::Datum as ScalarWord;
use ::types_error::PgResult;
use types_namespace::{
    CharArrayDatum, FuncArgInfo, FuncProcAttrs, OidArrayDatum, TextArrayDatum,
};
use ::types_tuple::heaptuple::{TupleDesc, OIDOID, RECORDOID, TEXTOID};

use toastdesc_seams as toastdesc;
use arrayfuncs_seams as arrayfuncs;
use syscache_seams as syscache;

/// `CHAROID` (`catalog/pg_type_d.h`) — the OID of the `"char"` type. Not in
/// `::types_tuple::heaptuple`, so spelled here as in the C header.
const CHAROID: Oid = 18;

// `pg_proc.h` `proargmode` codes (`PROARGMODE_*`). The raw projection carries
// the bytes verbatim, so these are compared as `u8`.
const PROARGMODE_IN: u8 = b'i';
const PROARGMODE_OUT: u8 = b'o';
const PROARGMODE_INOUT: u8 = b'b';
const PROARGMODE_VARIADIC: u8 = b'v';
const PROARGMODE_TABLE: u8 = b't';

/// `PROKIND_PROCEDURE` (`pg_proc.h`).
const PROKIND_PROCEDURE: u8 = b'p';

/// `elog(ERROR, msg)` — internal error with the elog default errcode
/// (`ERRCODE_INTERNAL_ERROR`), matching the `elog(ERROR, ...)` call sites here.
fn elog_internal(msg: impl Into<String>) -> utils_error::PgError {
    utils_error::ereport(::types_error::ERROR)
        .errmsg(msg.into())
        .into_error()
}

/// `SearchSysCache1(PROCOID, ObjectIdGetDatum(functionId))` +
/// `if (!HeapTupleIsValid(procTuple)) elog(ERROR, "cache lookup failed for
/// function %u", functionId)`, projected to the [`FuncProcAttrs`] columns via
/// the syscache owner's seam.
fn fetch_proc_attrs<'mcx>(mcx: Mcx<'mcx>, function_id: Oid) -> PgResult<FuncProcAttrs<'mcx>> {
    match syscache::proc_arg_attrs::call(mcx, function_id)? {
        Some(attrs) => Ok(attrs),
        None => Err(elog_internal(format!(
            "cache lookup failed for function {function_id}"
        ))),
    }
}

/// `get_func_arg_info(procTup, p_argtypes, p_argnames, p_argmodes)`
/// (funcapi.c:1379) — extract the all-argument type OID array, the per-argument
/// names (NULL where unnamed), and the per-argument modes from a `pg_proc` row;
/// returns the total argument count via the [`FuncArgInfo`] vectors' length.
pub fn get_func_arg_info<'mcx>(mcx: Mcx<'mcx>, proc_tuple_oid: Oid) -> PgResult<FuncArgInfo<'mcx>> {
    let attrs = fetch_proc_attrs(mcx, proc_tuple_oid)?;

    /* First discover the total number of parameters and get their types */
    // C: proallargtypes = SysCacheGetAttr(PROCOID, .., proallargtypes, &isNull);
    let numargs: i32;
    let mut argtypes: PgVec<Oid> = PgVec::new_in(mcx);
    match attrs.proallargtypes {
        Some(arr) => {
            /*
             * We expect the arrays to be 1-D arrays of the right types; verify
             * that.  For the OID array, the array data is just a C array of
             * values.
             */
            numargs = arr.dim0;
            if arr.ndim != 1 || numargs < 0 || arr.hasnull || arr.elemtype != OIDOID {
                return Err(elog_internal(
                    "proallargtypes is not a 1-D Oid array or it contains nulls",
                ));
            }
            debug_assert!(numargs >= attrs.pronargs);
            // C: *p_argtypes = palloc(numargs * sizeof(Oid));
            //    memcpy(*p_argtypes, ARR_DATA_PTR(arr), numargs * sizeof(Oid));
            argtypes
                .try_reserve(numargs as usize)
                .map_err(|_| mcx.oom(numargs as usize * core::mem::size_of::<Oid>()))?;
            for i in 0..numargs as usize {
                argtypes.push(arr.values[i]);
            }
        }
        None => {
            /* If no proallargtypes, use proargtypes */
            numargs = attrs.proargtypes.len() as i32;
            debug_assert_eq!(numargs, attrs.pronargs);
            argtypes
                .try_reserve(numargs as usize)
                .map_err(|_| mcx.oom(numargs as usize * core::mem::size_of::<Oid>()))?;
            for i in 0..numargs as usize {
                argtypes.push(attrs.proargtypes[i]);
            }
        }
    }

    /* Get argument names, if available */
    // C: proargnames = SysCacheGetAttr(PROCOID, .., proargnames, &isNull);
    let mut argnames: PgVec<Option<PgString>> = PgVec::new_in(mcx);
    if let Some(names) = attrs.proargnames {
        // C: deconstruct_array_builtin(.., TEXTOID, &elems, NULL, &nelems);
        //    if (nelems != numargs) elog(ERROR, ...);
        let nelems = names.values.len() as i32;
        if nelems != numargs {
            return Err(elog_internal(
                "proargnames must have the same number of elements as the function has arguments",
            ));
        }
        argnames
            .try_reserve(numargs as usize)
            .map_err(|_| mcx.oom(numargs as usize * core::mem::size_of::<usize>()))?;
        // C: for (i = 0; i < numargs; i++)
        //        (*p_argnames)[i] = TextDatumGetCString(elems[i]);
        // (The C array entry is always a non-NULL palloc'd string; the
        // idiomatic FuncArgInfo carries it as Some(name).)
        for i in 0..numargs as usize {
            let s = PgString::from_str_in(names.values[i].as_str(), mcx)?;
            argnames.push(Some(s));
        }
    }
    // else: C leaves *p_argnames = NULL — here an empty argnames vector.

    /* Get argument modes, if available */
    // C: proargmodes = SysCacheGetAttr(PROCOID, .., proargmodes, &isNull);
    let mut argmodes: PgVec<u8> = PgVec::new_in(mcx);
    if let Some(modes) = attrs.proargmodes {
        if modes.ndim != 1 || modes.dim0 != numargs || modes.hasnull || modes.elemtype != CHAROID {
            return Err(elog_internal(format!(
                "proargmodes is not a 1-D char array of length {numargs} or it contains nulls"
            )));
        }
        // C: *p_argmodes = palloc(numargs * sizeof(char));
        //    memcpy(*p_argmodes, ARR_DATA_PTR(arr), numargs * sizeof(char));
        argmodes
            .try_reserve(numargs as usize)
            .map_err(|_| mcx.oom(numargs as usize))?;
        for i in 0..numargs as usize {
            argmodes.push(modes.values[i]);
        }
    }
    // else: C leaves *p_argmodes = NULL — here an empty argmodes vector.

    Ok(FuncArgInfo {
        argtypes,
        argnames,
        argmodes,
    })
}

/// Inward-seam adapter for [`get_func_arg_info`]: matches the
/// `backend-utils-fmgr-funcapi-seams::get_func_arg_info` signature
/// (`(mcx, func_oid) -> PgResult<FuncArgInfo>`), which re-fetches the `pg_proc`
/// row by OID rather than taking the C caller's `HeapTuple`.
pub fn get_func_arg_info_seam<'mcx>(mcx: Mcx<'mcx>, func_oid: Oid) -> PgResult<FuncArgInfo<'mcx>> {
    get_func_arg_info(mcx, func_oid)
}

/// `get_func_trftypes(procTup, p_trftypes)` (funcapi.c:1475) — extract the
/// transform-function type OID array (`protrftypes`) from a `pg_proc` row;
/// empty when the function declares no transforms.
pub fn get_func_trftypes<'mcx>(mcx: Mcx<'mcx>, proc_tuple_oid: Oid) -> PgResult<PgVec<'mcx, Oid>> {
    let attrs = fetch_proc_attrs(mcx, proc_tuple_oid)?;

    // C: protrftypes = SysCacheGetAttr(PROCOID, .., protrftypes, &isNull);
    let mut out: PgVec<Oid> = PgVec::new_in(mcx);
    if let Some(arr) = attrs.protrftypes {
        /*
         * We expect the arrays to be 1-D arrays of the right types; verify
         * that.  For the OID array, the array data is just a C array of values.
         */
        let nelems = arr.dim0;
        if arr.ndim != 1 || nelems < 0 || arr.hasnull || arr.elemtype != OIDOID {
            return Err(elog_internal(
                "protrftypes is not a 1-D Oid array or it contains nulls",
            ));
        }
        // C: *p_trftypes = palloc(nelems * sizeof(Oid));
        //    memcpy(*p_trftypes, ARR_DATA_PTR(arr), nelems * sizeof(Oid));
        //    return nelems;
        out.try_reserve(nelems as usize)
            .map_err(|_| mcx.oom(nelems as usize * core::mem::size_of::<Oid>()))?;
        for i in 0..nelems as usize {
            out.push(arr.values[i]);
        }
    }
    // else: C returns 0 — here an empty vector.
    Ok(out)
}

/// `get_func_input_arg_names(proargnames, proargmodes, arg_names)`
/// (funcapi.c:1522) — derive the input-argument names array from the
/// `proargnames`/`proargmodes` arrays (skipping OUT-only modes), returning
/// `None` per unnamed input.
pub fn get_func_input_arg_names<'mcx>(
    mcx: Mcx<'mcx>,
    proargnames: ScalarWord,
    proargmodes: ScalarWord,
) -> PgResult<PgVec<'mcx, Option<PgString<'mcx>>>> {
    /* Do nothing if null proargnames */
    // C: if (proargnames == PointerGetDatum(NULL)) { *arg_names = NULL; return 0; }
    if proargnames == ScalarWord::null() {
        return Ok(PgVec::new_in(mcx));
    }

    /*
     * We expect the arrays to be 1-D arrays of the right types; verify that.
     */
    // C: arr = DatumGetArrayTypeP(proargnames);  (detoast + project)
    let names: TextArrayDatum = arrayfuncs::text_array_datum::call(mcx, proargnames)?;
    if names.ndim != 1 || names.hasnull || names.elemtype != TEXTOID {
        return Err(elog_internal(
            "proargnames is not a 1-D text array or it contains nulls",
        ));
    }
    // C: deconstruct_array_builtin(arr, TEXTOID, &argnames, NULL, &numargs);
    let numargs = names.values.len() as i32;

    // C: if (proargmodes != PointerGetDatum(NULL)) { ... argmodes = ARR_DATA_PTR; }
    //    else argmodes = NULL;
    let argmodes: Option<CharArrayDatum> = if proargmodes != ScalarWord::null() {
        let modes: CharArrayDatum = arrayfuncs::char_array_datum::call(mcx, proargmodes)?;
        if modes.ndim != 1 || modes.dim0 != numargs || modes.hasnull || modes.elemtype != CHAROID {
            return Err(elog_internal(format!(
                "proargmodes is not a 1-D char array of length {numargs} or it contains nulls"
            )));
        }
        Some(modes)
    } else {
        None
    };

    /* zero elements probably shouldn't happen, but handle it gracefully */
    // C: if (numargs <= 0) { *arg_names = NULL; return 0; }
    if numargs <= 0 {
        return Ok(PgVec::new_in(mcx));
    }

    /* extract input-argument names */
    // C: inargnames = palloc(numargs * sizeof(char *)); numinargs = 0;
    let mut inargnames: PgVec<Option<PgString>> = PgVec::new_in(mcx);
    inargnames
        .try_reserve(numargs as usize)
        .map_err(|_| mcx.oom(numargs as usize * core::mem::size_of::<usize>()))?;
    for i in 0..numargs as usize {
        // C: if (argmodes == NULL || argmodes[i] == PROARGMODE_IN ||
        //        argmodes[i] == PROARGMODE_INOUT || argmodes[i] == PROARGMODE_VARIADIC)
        let mode = argmodes.as_ref().map(|m| m.values[i]);
        if mode.is_none()
            || mode == Some(PROARGMODE_IN)
            || mode == Some(PROARGMODE_INOUT)
            || mode == Some(PROARGMODE_VARIADIC)
        {
            // C: char *pname = TextDatumGetCString(argnames[i]);
            //    if (pname[0] != '\0') inargnames[numinargs] = pname;
            //    else inargnames[numinargs] = NULL;
            //    numinargs++;
            if !names.values[i].as_str().is_empty() {
                let s = PgString::from_str_in(names.values[i].as_str(), mcx)?;
                inargnames.push(Some(s));
            } else {
                inargnames.push(None);
            }
        }
    }

    // C: *arg_names = inargnames; return numinargs;
    Ok(inargnames)
}

/// `get_func_input_arg_names` (funcapi.c:1670) over already-decoded argument
/// arrays: the subset of parameter names whose mode is IN/INOUT/VARIADIC (or
/// when no modes array is given, all of them). An empty name yields `None`,
/// matching the C `pname[0] == '\0'` case. Returns an empty list when there are
/// no names.
fn input_arg_names_decoded(
    names: &Option<Vec<Option<String>>>,
    modes: &Option<Vec<i8>>,
) -> Vec<Option<String>> {
    // C: if (proargnames == PointerGetDatum(NULL)) { *arg_names = NULL; return 0; }
    let Some(names) = names else {
        return Vec::new();
    };
    let mut out: Vec<Option<String>> = Vec::with_capacity(names.len());
    for (i, name) in names.iter().enumerate() {
        // C: if (argmodes == NULL || argmodes[i] == PROARGMODE_IN ||
        //        argmodes[i] == PROARGMODE_INOUT || argmodes[i] == PROARGMODE_VARIADIC)
        let mode = modes.as_ref().and_then(|m| m.get(i).copied());
        let is_input = match mode {
            None => true,
            Some(m) => {
                let m = m as u8;
                m == PROARGMODE_IN || m == PROARGMODE_INOUT || m == PROARGMODE_VARIADIC
            }
        };
        if is_input {
            // C: if (pname[0] != '\0') inargnames[numinargs] = pname; else NULL;
            match name {
                Some(s) if !s.is_empty() => out.push(Some(s.clone())),
                _ => out.push(None),
            }
        }
    }
    out
}

/// `build_function_result_tupdesc_d` (funcapi.c:1751) over already-decoded
/// argument arrays — the OUT/INOUT/TABLE column projection used when the caller
/// already holds the new function's parameter vectors (CREATE OR REPLACE
/// FUNCTION's record-type compatibility check). Mirrors the shared body's
/// out-arg extraction and gin-up-column-name rule; `None` when there is no
/// composite result.
fn build_function_result_tupdesc_d_from_decoded<'mcx>(
    mcx: Mcx<'mcx>,
    prokind: u8,
    proallargtypes: &Option<Vec<Oid>>,
    proargmodes: &Option<Vec<i8>>,
    proargnames: &Option<Vec<Option<String>>>,
) -> PgResult<TupleDesc<'mcx>> {
    /* Can't have output args if columns are null */
    let (Some(argtypes), Some(argmodes)) = (proallargtypes, proargmodes) else {
        return Ok(None);
    };
    let numargs = argtypes.len();
    if argmodes.len() != numargs {
        return Ok(None);
    }
    let argnames = proargnames.as_ref().filter(|n| n.len() == numargs);

    /* zero elements probably shouldn't happen, but handle it gracefully */
    if numargs == 0 {
        return Ok(None);
    }

    /* extract output-argument types and names */
    let mut outargtypes: Vec<Oid> = Vec::with_capacity(numargs);
    let mut outargnames: Vec<PgString> = Vec::with_capacity(numargs);
    let mut numoutargs: usize = 0;
    for i in 0..numargs {
        let mode = argmodes[i] as u8;
        if mode == PROARGMODE_IN || mode == PROARGMODE_VARIADIC {
            continue;
        }
        debug_assert!(
            mode == PROARGMODE_OUT || mode == PROARGMODE_INOUT || mode == PROARGMODE_TABLE
        );
        outargtypes.push(argtypes[i]);
        let pname = argnames
            .and_then(|n| n[i].as_deref())
            .unwrap_or("");
        let name = if pname.is_empty() {
            PgString::from_str_in(&format!("column{}", numoutargs + 1), mcx)?
        } else {
            PgString::from_str_in(pname, mcx)?
        };
        outargnames.push(name);
        numoutargs += 1;
    }

    /* If there is no output argument, or only one, no tuple result. */
    if numoutargs < 2 && prokind != PROKIND_PROCEDURE {
        return Ok(None);
    }

    let mut desc = toastdesc::create_template_tuple_desc::call(mcx, numoutargs as i32)?;
    for i in 0..numoutargs {
        toastdesc::tuple_desc_init_entry::call(
            &mut desc,
            (i + 1) as AttrNumber,
            outargnames[i].as_str(),
            outargtypes[i],
            -1,
            0,
        )?;
    }
    Ok(Some(::mcx::alloc_in(mcx, desc)?))
}

/// `record_type_change` body (pg_proc.c:455-477): when the replaced function
/// returns RECORD, compare the OUT-parameter row type of the old definition
/// (`build_function_result_tupdesc_t` over the old proc OID) against the new
/// definition (`build_function_result_tupdesc_d` over the new decoded OUT
/// arrays), classified by `equalRowTypes`.
pub fn record_type_change(
    old_funcoid: Oid,
    prokind: i8,
    all_parameter_types: Option<Vec<Oid>>,
    parameter_modes: Option<Vec<i8>>,
    parameter_names: Option<Vec<Option<String>>>,
) -> PgResult<::pg_proc_seams::RecordTypeChange> {
    use ::pg_proc_seams::RecordTypeChange;

    // The two descriptors are only compared (equalRowTypes returns a bool); a
    // private scratch context holds them for the duration of the call.
    let scratch = MemoryContext::new("record_type_change");
    let mcx = scratch.mcx();

    let olddesc = build_function_result_tupdesc_t(mcx, old_funcoid)?;
    let newdesc = build_function_result_tupdesc_d_from_decoded(
        mcx,
        prokind as u8,
        &all_parameter_types,
        &parameter_modes,
        &parameter_names,
    )?;

    // C: if (olddesc == NULL && newdesc == NULL) /* both runtime RECORDs */;
    //    else if (olddesc == NULL || newdesc == NULL || !equalRowTypes(...)) error.
    match (olddesc.as_ref(), newdesc.as_ref()) {
        (None, None) => Ok(RecordTypeChange::BothRuntime),
        (Some(o), Some(n)) => {
            if tupdesc::equalRowTypes(o, n) {
                Ok(RecordTypeChange::Equal)
            } else {
                Ok(RecordTypeChange::Different)
            }
        }
        _ => Ok(RecordTypeChange::Different),
    }
}

/// `check_input_param_names_unchanged` body (pg_proc.c:484-523): compare the
/// old vs new input-parameter name lists (filtered by mode). Returns the first
/// old input-parameter name that was renamed (the C
/// `cannot change name of input parameter` trigger), or `None` when every
/// retained name is unchanged. Adding a name to a formerly-unnamed parameter is
/// allowed (old name `None` is skipped).
pub fn check_input_param_names_unchanged(
    old_proargnames: Option<Vec<Option<String>>>,
    old_proargmodes: Option<Vec<i8>>,
    new_parameter_names: Option<Vec<Option<String>>>,
    new_parameter_modes: Option<Vec<i8>>,
) -> PgResult<Option<String>> {
    let old_arg_names = input_arg_names_decoded(&old_proargnames, &old_proargmodes);
    let new_arg_names = input_arg_names_decoded(&new_parameter_names, &new_parameter_modes);

    // C: for (j = 0; j < n_old_arg_names; j++) { if (old_arg_names[j] == NULL) continue;
    //      if (j >= n_new_arg_names || new_arg_names[j] == NULL ||
    //          strcmp(old_arg_names[j], new_arg_names[j]) != 0) ereport(...); }
    for (j, old_name) in old_arg_names.iter().enumerate() {
        let Some(old_name) = old_name else {
            continue;
        };
        let renamed = match new_arg_names.get(j) {
            None | Some(None) => true,
            Some(Some(new_name)) => new_name != old_name,
        };
        if renamed {
            return Ok(Some(old_name.clone()));
        }
    }
    Ok(None)
}

/// `get_func_result_name(functionId)` (funcapi.c:1607) — the column name of a
/// single-OUT-parameter function's result, or `None` if the function has no
/// single named result column.
pub fn get_func_result_name<'mcx>(
    mcx: Mcx<'mcx>,
    function_id: Oid,
) -> PgResult<Option<PgString<'mcx>>> {
    /* First fetch the function's pg_proc row */
    // C: procTuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(functionId));
    //    if (!HeapTupleIsValid(procTuple)) elog(ERROR, "cache lookup failed ...");
    let attrs = fetch_proc_attrs(mcx, function_id)?;

    /* If there are no named OUT parameters, return NULL */
    // C: if (heap_attisnull(procTuple, proargmodes) ||
    //        heap_attisnull(procTuple, proargnames)) result = NULL;
    let (Some(modes), Some(names)) = (attrs.proargmodes, attrs.proargnames) else {
        return Ok(None);
    };

    /*
     * We expect the arrays to be 1-D arrays of the right types; verify that.
     */
    // C: arr = DatumGetArrayTypeP(proargmodes); numargs = ARR_DIMS(arr)[0];
    let numargs = modes.dim0;
    if modes.ndim != 1 || numargs < 0 || modes.hasnull || modes.elemtype != CHAROID {
        return Err(elog_internal(
            "proargmodes is not a 1-D char array or it contains nulls",
        ));
    }
    // C: arr = DatumGetArrayTypeP(proargnames);
    if names.ndim != 1 || names.dim0 != numargs || names.hasnull || names.elemtype != TEXTOID {
        return Err(elog_internal(format!(
            "proargnames is not a 1-D text array of length {numargs} or it contains nulls"
        )));
    }
    // C: deconstruct_array_builtin(arr, TEXTOID, &argnames, NULL, &nargnames);
    //    Assert(nargnames == numargs);
    debug_assert_eq!(names.values.len(), numargs.max(0) as usize);

    /* scan for output argument(s) */
    // C: result = NULL; numoutargs = 0;
    let mut result: Option<PgString> = None;
    let mut numoutargs = 0;
    for i in 0..numargs.max(0) as usize {
        // C: if (argmodes[i] == PROARGMODE_IN || argmodes[i] == PROARGMODE_VARIADIC) continue;
        if modes.values[i] == PROARGMODE_IN || modes.values[i] == PROARGMODE_VARIADIC {
            continue;
        }
        debug_assert!(
            modes.values[i] == PROARGMODE_OUT
                || modes.values[i] == PROARGMODE_INOUT
                || modes.values[i] == PROARGMODE_TABLE
        );
        // C: if (++numoutargs > 1) { result = NULL; break; }
        numoutargs += 1;
        if numoutargs > 1 {
            /* multiple out args, so forget it */
            result = None;
            break;
        }
        // C: result = TextDatumGetCString(argnames[i]);
        //    if (result == NULL || result[0] == '\0') { result = NULL; break; }
        if names.values[i].as_str().is_empty() {
            /* Parameter is not named, so forget it */
            result = None;
            break;
        }
        result = Some(PgString::from_str_in(names.values[i].as_str(), mcx)?);
    }

    // C: ReleaseSysCache(procTuple); return result;
    Ok(result)
}

/// `build_function_result_tupdesc_t(procTuple)` (funcapi.c:1705) — build the
/// result `TupleDesc` for an OUT/INOUT/TABLE function from its `pg_proc` row
/// (delegating the array decoding to [`build_function_result_tupdesc_d`]);
/// `None` when the function returns no composite.
pub fn build_function_result_tupdesc_t<'mcx>(
    mcx: Mcx<'mcx>,
    proc_tuple_oid: Oid,
) -> PgResult<TupleDesc<'mcx>> {
    let attrs = fetch_proc_attrs(mcx, proc_tuple_oid)?;

    /* Return NULL if the function isn't declared to return RECORD */
    // C: if (procform->prorettype != RECORDOID) return NULL;
    if attrs.prorettype != RECORDOID {
        return Ok(None);
    }

    /* If there are no OUT parameters, return NULL */
    // C: if (heap_attisnull(procTuple, proallargtypes) ||
    //        heap_attisnull(procTuple, proargmodes)) return NULL;
    let (Some(proallargtypes), Some(proargmodes)) = (attrs.proallargtypes, attrs.proargmodes)
    else {
        return Ok(None);
    };

    // C: proargnames = SysCacheGetAttr(PROCOID, .., proargnames, &isnull);
    //    if (isnull) proargnames = PointerGetDatum(NULL);
    let proargnames = attrs.proargnames;

    // C: return build_function_result_tupdesc_d(procform->prokind,
    //        proallargtypes, proargmodes, proargnames);
    build_function_result_tupdesc_d_projected(
        mcx,
        attrs.prokind,
        Some(proallargtypes),
        Some(proargmodes),
        proargnames,
    )
}

/// `build_function_result_tupdesc_d(prokind, proallargtypes, proargmodes,
/// proargnames)` (funcapi.c:1751) — build the result `TupleDesc` from the
/// decoded OUT/INOUT/TABLE columns of the `pg_proc` argument arrays; `None`
/// when there is no composite result.
///
/// The caller passes the raw column `Datum`s (the C NULL Datum == [`Datum::null`]);
/// this entrypoint detoasts / projects each via the array owner's seams and
/// then runs the shared body.
pub fn build_function_result_tupdesc_d<'mcx>(
    mcx: Mcx<'mcx>,
    prokind: u8,
    proallargtypes: ScalarWord,
    proargmodes: ScalarWord,
    proargnames: ScalarWord,
) -> PgResult<TupleDesc<'mcx>> {
    /* Can't have output args if columns are null */
    // C: if (proallargtypes == PointerGetDatum(NULL) ||
    //        proargmodes == PointerGetDatum(NULL)) return NULL;
    if proallargtypes == ScalarWord::null() || proargmodes == ScalarWord::null() {
        return Ok(None);
    }

    // C: arr = DatumGetArrayTypeP(proallargtypes);  (detoast + project)
    let alltypes: OidArrayDatum = arrayfuncs::oid_array_datum::call(mcx, proallargtypes)?;
    let modes: CharArrayDatum = arrayfuncs::char_array_datum::call(mcx, proargmodes)?;
    let names: Option<TextArrayDatum> = if proargnames != ScalarWord::null() {
        Some(arrayfuncs::text_array_datum::call(mcx, proargnames)?)
    } else {
        None
    };

    build_function_result_tupdesc_d_projected(mcx, prokind, Some(alltypes), Some(modes), names)
}

/// The shared body of `build_function_result_tupdesc_d` once each column has
/// been projected (a `None` projection == the C NULL Datum).
fn build_function_result_tupdesc_d_projected<'mcx>(
    mcx: Mcx<'mcx>,
    prokind: u8,
    proallargtypes: Option<OidArrayDatum<'mcx>>,
    proargmodes: Option<CharArrayDatum<'mcx>>,
    proargnames: Option<TextArrayDatum<'mcx>>,
) -> PgResult<TupleDesc<'mcx>> {
    /* Can't have output args if columns are null */
    let (Some(alltypes), Some(modes)) = (proallargtypes, proargmodes) else {
        return Ok(None);
    };

    /*
     * We expect the arrays to be 1-D arrays of the right types; verify that.
     */
    // C: arr = DatumGetArrayTypeP(proallargtypes); numargs = ARR_DIMS(arr)[0];
    let numargs = alltypes.dim0;
    if alltypes.ndim != 1 || numargs < 0 || alltypes.hasnull || alltypes.elemtype != OIDOID {
        return Err(elog_internal(
            "proallargtypes is not a 1-D Oid array or it contains nulls",
        ));
    }
    // C: argtypes = (Oid *) ARR_DATA_PTR(arr);
    let argtypes = &alltypes.values;
    // C: arr = DatumGetArrayTypeP(proargmodes);
    if modes.ndim != 1 || modes.dim0 != numargs || modes.hasnull || modes.elemtype != CHAROID {
        return Err(elog_internal(format!(
            "proargmodes is not a 1-D char array of length {numargs} or it contains nulls"
        )));
    }
    // C: argmodes = (char *) ARR_DATA_PTR(arr);
    let argmodes = &modes.values;
    // C: if (proargnames != PointerGetDatum(NULL)) { ... deconstruct ... }
    let argnames: Option<&PgVec<PgString>> = match &proargnames {
        Some(names) => {
            if names.ndim != 1 || names.dim0 != numargs || names.hasnull || names.elemtype != TEXTOID
            {
                return Err(elog_internal(format!(
                    "proargnames is not a 1-D text array of length {numargs} or it contains nulls"
                )));
            }
            // C: deconstruct_array_builtin(arr, TEXTOID, &argnames, NULL, &nargnames);
            //    Assert(nargnames == numargs);
            debug_assert_eq!(names.values.len(), numargs.max(0) as usize);
            Some(&names.values)
        }
        None => None,
    };

    /* zero elements probably shouldn't happen, but handle it gracefully */
    // C: if (numargs <= 0) return NULL;
    if numargs <= 0 {
        return Ok(None);
    }

    /* extract output-argument types and names */
    // C: outargtypes = palloc(numargs * sizeof(Oid));
    //    outargnames = palloc(numargs * sizeof(char *));
    //    numoutargs = 0;
    let mut outargtypes: PgVec<Oid> = PgVec::new_in(mcx);
    outargtypes
        .try_reserve(numargs as usize)
        .map_err(|_| mcx.oom(numargs as usize * core::mem::size_of::<Oid>()))?;
    let mut outargnames: PgVec<PgString> = PgVec::new_in(mcx);
    outargnames
        .try_reserve(numargs as usize)
        .map_err(|_| mcx.oom(numargs as usize * core::mem::size_of::<usize>()))?;
    let mut numoutargs: usize = 0;
    for i in 0..numargs as usize {
        // C: if (argmodes[i] == PROARGMODE_IN || argmodes[i] == PROARGMODE_VARIADIC) continue;
        if argmodes[i] == PROARGMODE_IN || argmodes[i] == PROARGMODE_VARIADIC {
            continue;
        }
        debug_assert!(
            argmodes[i] == PROARGMODE_OUT
                || argmodes[i] == PROARGMODE_INOUT
                || argmodes[i] == PROARGMODE_TABLE
        );
        // C: outargtypes[numoutargs] = argtypes[i];
        outargtypes.push(argtypes[i]);
        // C: if (argnames) pname = TextDatumGetCString(argnames[i]); else pname = NULL;
        //    if (pname == NULL || pname[0] == '\0') pname = psprintf("column%d", numoutargs + 1);
        let pname: &str = argnames.map(|n| n[i].as_str()).unwrap_or("");
        let name = if pname.is_empty() {
            /* Parameter is not named, so gin up a column name */
            PgString::from_str_in(&format!("column{}", numoutargs + 1), mcx)?
        } else {
            PgString::from_str_in(pname, mcx)?
        };
        // C: outargnames[numoutargs] = pname;
        outargnames.push(name);
        numoutargs += 1;
    }

    /*
     * If there is no output argument, or only one, the function does not
     * return tuples.
     */
    // C: if (numoutargs < 2 && prokind != PROKIND_PROCEDURE) return NULL;
    if numoutargs < 2 && prokind != PROKIND_PROCEDURE {
        return Ok(None);
    }

    // C: desc = CreateTemplateTupleDesc(numoutargs);
    let mut desc = toastdesc::create_template_tuple_desc::call(mcx, numoutargs as i32)?;
    // C: for (i = 0; i < numoutargs; i++)
    //        TupleDescInitEntry(desc, i + 1, outargnames[i], outargtypes[i], -1, 0);
    for i in 0..numoutargs {
        toastdesc::tuple_desc_init_entry::call(
            &mut desc,
            (i + 1) as AttrNumber,
            outargnames[i].as_str(),
            outargtypes[i],
            -1,
            0,
        )?;
    }

    Ok(Some(::mcx::alloc_in(mcx, desc)?))
}
