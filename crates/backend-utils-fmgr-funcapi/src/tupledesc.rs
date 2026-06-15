//! Descriptor builders + VARIADIC unpacking — `funcapi.c` lines 1870–2256.
//!
//! Build a result `TupleDesc` from a relation name or a (possibly composite)
//! type OID, and unpack a function's VARIADIC argument run into per-element
//! `(value, type, isnull)` triples.
//!
//! Unported neighbors are routed through their owners' seams:
//!   * relation open/close — `backend-access-common-relation-seams` /
//!     the [`Relation`] handle's own close path.
//!   * `stringToQualifiedNameList` — `backend-utils-adt-regproc-seams`.
//!   * `makeRangeVarFromNameList` — `backend-catalog-namespace-seams`.
//!   * `CreateTupleDescCopy` — `backend-access-common-tupdesc-seams`.
//!   * `lookup_rowtype_tupdesc_copy` — `backend-utils-cache-typcache-seams`.
//!   * `get_type_func_class` — the sibling [`crate::polymorphic`] family
//!     (same crate, filled by its own pass).
//!   * `get_typlenbyvalalign` — `backend-utils-cache-lsyscache-seams`.
//!   * `ARR_ELEMTYPE` / `deconstruct_array` — `backend-utils-adt-arrayfuncs-seams`.
//!   * the fmgr call-frame reads (`PG_NARGS`, `PG_ARGISNULL`, `PG_GETARG_*`,
//!     `get_fn_expr_variadic` / `_argtype` / `_arg_stable`) —
//!     `backend-utils-fmgr-fmgr-seams` (fmgr owns the trimmed frame).
//!
//! `CreateTemplateTupleDesc` / `TupleDescInitEntry` are called directly on the
//! ported `backend-access-common-tupdesc` crate.

use backend_utils_error::ereport;
use mcx::{vec_with_capacity_in, Mcx, PgString};
use types_core::{Oid, OidIsValid};
// The canonical unified value type (Datum-unification keystone) — what
// `ExtractedVariadicArgs.values` carries (a `PgVec<Datum<'mcx>>` owned by
// `types-nodes`). `extract_variadic_args` builds each element directly as this
// canonical value: the raw argument words from `pg_getarg_datum` /
// `deconstruct_array` (still bare-word at the sanctioned fmgr PG_GETARG ABI
// edge) flow into its by-value arm, while `cstring_get_text_datum` already
// returns the canonical value verbatim.
use types_tuple::backend_access_common_heaptuple::Datum as DatumV;
use types_error::{PgResult, ERRCODE_DATATYPE_MISMATCH, ERRCODE_INVALID_PARAMETER_VALUE, ERROR};
use types_nodes::fmgr::FunctionCallInfoBaseData;
use types_nodes::funcapi::{ExtractedVariadicArgs, TypeFuncClass};
use types_storage::lock::AccessShareLock;
use types_tuple::heaptuple::{TupleDesc, RECORDOID, TEXTOID, UNKNOWNOID};

/// `RelationNameGetTupleDesc(relname)` (funcapi.c:1870) — look up the relation
/// by (possibly qualified) name and return a copy of its row `TupleDesc`.
pub fn RelationNameGetTupleDesc<'mcx>(mcx: Mcx<'mcx>, relname: &str) -> PgResult<TupleDesc<'mcx>> {
    // Open relation and copy the tuple description.
    // relname_list = stringToQualifiedNameList(relname, NULL);
    // C passes escontext == NULL, so soft = false (bad syntax -> ERROR), and
    // the result is never NIL on success.
    let relname_list = backend_utils_adt_regproc_seams::string_to_qualified_name_list::call(
        mcx, relname, false,
    )?
    .expect("stringToQualifiedNameList(relname, NULL) returns a name list, not NIL");

    // relvar = makeRangeVarFromNameList(relname_list);
    // The seam takes the parts as `&[&str]`.
    let parts: mcx::PgVec<'mcx, &str> = {
        let mut v = vec_with_capacity_in(mcx, relname_list.len())?;
        for s in relname_list.iter() {
            v.push(s.as_str());
        }
        v
    };
    let relvar = backend_catalog_namespace_seams::make_range_var_from_name_list::call(&parts)?;

    // rel = relation_openrv(relvar, AccessShareLock);
    let rel = backend_access_common_relation_seams::relation_openrv::call(
        mcx,
        &relvar,
        AccessShareLock,
    )?;

    // tupdesc = CreateTupleDescCopy(RelationGetDescr(rel));
    let tupdesc =
        backend_access_common_tupdesc_seams::create_tupledesc_copy::call(mcx, &rel.rd_att)?;

    // relation_close(rel, AccessShareLock);
    rel.close(AccessShareLock)?;

    Ok(Some(tupdesc))
}

/// `TypeGetTupleDesc(typeoid, colaliases)` (funcapi.c:1903) — build a
/// `TupleDesc` for `typeoid`: for a composite type its row descriptor (renamed
/// per `colaliases`), otherwise a single-column descriptor of that base type.
pub fn TypeGetTupleDesc<'mcx>(
    mcx: Mcx<'mcx>,
    typeoid: Oid,
    colaliases: Option<&[PgString<'mcx>]>,
) -> PgResult<TupleDesc<'mcx>> {
    // TypeFuncClass functypclass = get_type_func_class(typeoid, &base_typeoid);
    let (functypclass, base_typeoid) = crate::polymorphic::get_type_func_class(typeoid)?;

    // TupleDesc tupdesc = NULL; — each non-error branch overwrites it, so the
    // owned model produces it as the value of the classifying match.
    // We intentionally do not support TYPEFUNC_COMPOSITE_DOMAIN here.
    let tupdesc: TupleDesc<'mcx> = match functypclass {
        TypeFuncClass::Composite => {
            // Composite data type, e.g. a table's row type.
            // tupdesc = lookup_rowtype_tupdesc_copy(base_typeoid, -1);
            let mut td = backend_utils_cache_typcache_seams::lookup_rowtype_tupdesc_copy::call(
                mcx,
                base_typeoid,
                -1,
            )?;

            // if (colaliases != NIL)
            if let Some(colaliases) = colaliases.filter(|c| !c.is_empty()) {
                let natts = td.natts;

                // does the list length match the number of attributes?
                if colaliases.len() as i32 != natts {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_DATATYPE_MISMATCH)
                        .errmsg("number of aliases does not match number of columns")
                        .into_error());
                }

                // OK, use the aliases instead.
                for varattno in 0..natts as usize {
                    // char *label = strVal(list_nth(colaliases, varattno));
                    let label = &colaliases[varattno];
                    // Form_pg_attribute attr = TupleDescAttr(tupdesc, varattno);
                    // if (label != NULL) namestrcpy(&(attr->attname), label);
                    // An owned PgString is always present (never the C NULL).
                    td.attr_mut(varattno).attname.namestrcpy(label.as_str());
                }

                // The tuple type is now an anonymous record type.
                td.tdtypeid = RECORDOID;
                td.tdtypmod = -1;
            }

            Some(td)
        }
        TypeFuncClass::Scalar => {
            // Base data type, i.e. scalar.
            // the alias list is required for base types
            let colaliases = match colaliases.filter(|c| !c.is_empty()) {
                Some(c) => c,
                None => {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_DATATYPE_MISMATCH)
                        .errmsg("no column alias was provided")
                        .into_error());
                }
            };

            // the alias list length must be 1
            if colaliases.len() != 1 {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_DATATYPE_MISMATCH)
                    .errmsg("number of aliases does not match number of columns")
                    .into_error());
            }

            // OK, get the column alias.
            // attname = strVal(linitial(colaliases));
            let attname = &colaliases[0];

            // tupdesc = CreateTemplateTupleDesc(1);
            let mut td = backend_access_common_tupdesc::CreateTemplateTupleDesc(mcx, 1)?;
            // TupleDescInitEntry(tupdesc, 1, attname, typeoid, -1, 0);
            backend_access_common_tupdesc::TupleDescInitEntry(
                &mut td,
                1,
                Some(attname.as_str()),
                typeoid,
                -1,
                0,
            )?;

            // CreateTemplateTupleDesc returns the descriptor by value; the
            // owned TupleDesc carrier is a context-allocated PgBox.
            Some(mcx::alloc_in(mcx, td)?)
        }
        TypeFuncClass::Record => {
            // XXX can't support this because typmod wasn't passed in ...
            return Err(ereport(ERROR)
                .errcode(ERRCODE_DATATYPE_MISMATCH)
                .errmsg("could not determine row description for function returning record")
                .into_error());
        }
        TypeFuncClass::CompositeDomain | TypeFuncClass::Other => {
            // crummy error message, but parser should have caught this
            // elog(ERROR, "function in FROM has unsupported return type");
            return Err(ereport(ERROR)
                .errmsg_internal("function in FROM has unsupported return type")
                .into_error());
        }
    };

    Ok(tupdesc)
}

/// `extract_variadic_args(fcinfo, variadic_start, convert_unknown, values,
/// types, nulls)` (funcapi.c:2005) — unpack the function's VARIADIC argument
/// run starting at `variadic_start` into per-element triples. A real VARIADIC
/// array argument is deconstructed; otherwise the trailing scalar args are
/// gathered (optionally converting `unknown` literals to `text`). Returns the
/// element count via the [`ExtractedVariadicArgs`] vectors' length, or `None`
/// for a NULL VARIADIC array (the C `return -1`).
pub fn extract_variadic_args<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &FunctionCallInfoBaseData<'mcx>,
    variadic_start: i32,
    convert_unknown: bool,
) -> PgResult<Option<ExtractedVariadicArgs<'mcx>>> {
    // bool variadic = get_fn_expr_variadic(fcinfo->flinfo);
    let variadic = backend_utils_fmgr_fmgr_seams::get_fn_expr_variadic::call(fcinfo);

    // *args = NULL; *types = NULL; *nulls = NULL; (no-op in the owned model).

    // The extracted argument values are returned as the canonical unified
    // value type (`ExtractedVariadicArgs.values`); the per-element words from
    // deconstruct_array / PG_GETARG cross into its by-value arm.
    let mut args_res: mcx::PgVec<'mcx, DatumV<'mcx>>;
    let mut nulls_res: mcx::PgVec<'mcx, bool>;
    let mut types_res: mcx::PgVec<'mcx, Oid>;

    let nargs: i32;

    if variadic {
        // Assert(PG_NARGS() == variadic_start + 1);
        debug_assert_eq!(
            backend_utils_fmgr_fmgr_seams::pg_nargs::call(fcinfo),
            variadic_start + 1
        );

        // if (PG_ARGISNULL(variadic_start)) return -1;
        if backend_utils_fmgr_fmgr_seams::pg_argisnull::call(fcinfo, variadic_start as usize) {
            return Ok(None);
        }

        // array_in = PG_GETARG_ARRAYTYPE_P(variadic_start);
        // (the seam reads the raw datum; ARR_ELEMTYPE / deconstruct_array
        // detoast it internally.)
        let array_in =
            backend_utils_fmgr_fmgr_seams::pg_getarg_datum::call(fcinfo, variadic_start as usize);

        // element_type = ARR_ELEMTYPE(array_in);
        let element_type =
            backend_utils_adt_arrayfuncs_seams::array_get_elemtype::call(mcx, array_in)?;

        // get_typlenbyvalalign(element_type, &typlen, &typbyval, &typalign);
        let tlbva = backend_utils_cache_lsyscache_seams::get_typlenbyvalalign::call(element_type)?;

        // deconstruct_array(array_in, element_type, typlen, typbyval, typalign,
        //                   &args_res, &nulls_res, &nargs);
        let elems = backend_utils_adt_arrayfuncs_seams::deconstruct_array::call(
            mcx,
            array_in,
            element_type,
            tlbva.typlen,
            tlbva.typbyval,
            tlbva.typalign as core::ffi::c_char,
        )?;

        nargs = elems.len() as i32;

        args_res = vec_with_capacity_in(mcx, elems.len())?;
        nulls_res = vec_with_capacity_in(mcx, elems.len())?;
        // types_res = (Oid *) palloc0(nargs * sizeof(Oid));
        types_res = vec_with_capacity_in(mcx, elems.len())?;

        for (value, isnull) in elems.iter().copied() {
            args_res.push(DatumV::ByVal(value.as_usize()));
            nulls_res.push(isnull);
            // All the elements of the array have the same type.
            types_res.push(element_type);
        }
    } else {
        // nargs = PG_NARGS() - variadic_start;
        let total = backend_utils_fmgr_fmgr_seams::pg_nargs::call(fcinfo);
        nargs = total - variadic_start;
        // Assert(nargs > 0);
        debug_assert!(nargs > 0);

        let n = nargs as usize;
        nulls_res = vec_with_capacity_in(mcx, n)?;
        args_res = vec_with_capacity_in(mcx, n)?;
        types_res = vec_with_capacity_in(mcx, n)?;

        for i in 0..nargs {
            let argnum = i + variadic_start;

            // nulls_res[i] = PG_ARGISNULL(i + variadic_start);
            let isnull =
                backend_utils_fmgr_fmgr_seams::pg_argisnull::call(fcinfo, argnum as usize);
            // types_res[i] = get_fn_expr_argtype(fcinfo->flinfo, i + variadic_start);
            let mut argtype =
                backend_utils_fmgr_fmgr_seams::get_fn_expr_argtype::call(fcinfo, argnum);
            let value: DatumV<'mcx>;

            // Turn a constant (more or less literal) value that's of unknown
            // type into text if required. Unknowns come in as a cstring
            // pointer.
            if convert_unknown
                && argtype == UNKNOWNOID
                && backend_utils_fmgr_fmgr_seams::get_fn_expr_arg_stable::call(fcinfo, argnum)
            {
                argtype = TEXTOID;

                if backend_utils_fmgr_fmgr_seams::pg_argisnull::call(fcinfo, argnum as usize) {
                    // args_res[i] = (Datum) 0;
                    value = DatumV::null();
                } else {
                    // args_res[i] = CStringGetTextDatum(PG_GETARG_POINTER(...));
                    // The pointer read is fmgr's; the text construction is the
                    // funcapi-owned CStringGetTextDatum seam, which already
                    // yields the canonical value.
                    let s = backend_utils_fmgr_fmgr_seams::pg_getarg_cstring::call(
                        fcinfo,
                        argnum as usize,
                    );
                    value = backend_utils_fmgr_funcapi_seams::cstring_get_text_datum::call(mcx, s)?;
                }
            } else {
                // no conversion needed, just take the datum as given. The fmgr
                // PG_GETARG seam returns the bare machine word (the sanctioned
                // PGFunction-argument ABI edge); carry it in the by-value arm
                // of the canonical value.
                value = DatumV::ByVal(
                    backend_utils_fmgr_fmgr_seams::pg_getarg_datum::call(fcinfo, argnum as usize)
                        .as_usize(),
                );
            }
            // (C keeps nulls_res[i] as captured from PG_ARGISNULL — the value
            // is taken as given regardless of NULL, matching funcapi.c.)

            // if (!OidIsValid(types_res[i]) ||
            //     (convert_unknown && types_res[i] == UNKNOWNOID))
            if !OidIsValid(argtype) || (convert_unknown && argtype == UNKNOWNOID) {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                    .errmsg(format!(
                        "could not determine data type for argument {}",
                        i + 1
                    ))
                    .into_error());
            }

            args_res.push(value);
            nulls_res.push(isnull);
            types_res.push(argtype);
        }
    }

    // Fill in results.
    let _ = nargs;
    Ok(Some(ExtractedVariadicArgs {
        values: args_res,
        types: types_res,
        nulls: nulls_res,
    }))
}

/// Build an anonymous-record `Datum` from a row of `values`/`nulls` whose
/// columns have the given type OIDs (`coltypes[i]` is column `i+1`). This is the
/// `CreateTemplateTupleDesc(n)` + per-column `TupleDescInitEntry(..., typ, -1, 0)`
/// + `BlessTupleDesc` + `heap_form_tuple` + `HeapTupleGetDatum` idiom used by
/// record-returning builtins (e.g. genfile.c's `pg_stat_file`).
///
/// The terminal `HeapTupleGetDatum` step — turning the formed tuple into a
/// composite record `Datum` — crosses the composite/record-Datum carrier bridge
/// (task #161): a composite value is an ordinary pass-by-reference (varlena)
/// `Datum` whose bytes are a `HeapTupleHeader`, produced by
/// [`backend_access_common_heaptuple::HeapTupleGetDatum`] (NO new Datum variant,
/// NO forged pointer — datum-redesign-plan Option A). The descriptor / tuple are
/// allocated in `mcx`; `Err` carries OOM from forming the tuple.
pub fn record_from_values<'mcx>(
    mcx: Mcx<'mcx>,
    coltypes: &[Oid],
    values: &[DatumV<'mcx>],
    nulls: &[bool],
) -> PgResult<DatumV<'mcx>> {
    let natts = coltypes.len();
    debug_assert_eq!(values.len(), natts);
    debug_assert_eq!(nulls.len(), natts);

    // C: tupdesc = CreateTemplateTupleDesc(n);
    let mut td = backend_access_common_tupdesc::CreateTemplateTupleDesc(mcx, natts as i32)?;
    // C: TupleDescInitEntry(tupdesc, i+1, "...", coltypes[i], -1, 0);
    // The column names are immaterial for an anonymous record built purely to
    // wrap a value row (the producing builtin's pg_proc OUT-parameter names
    // already describe the row); C's genfile.c passes literal names only for
    // documentation. We pass `None` (TupleDescInitEntry fills a NameData; the
    // name is not consulted when forming/deforming by position).
    for (i, &typ) in coltypes.iter().enumerate() {
        backend_access_common_tupdesc::TupleDescInitEntry(
            &mut td,
            (i + 1) as i16,
            None,
            typ,
            -1,
            0,
        )?;
    }

    // C: BlessTupleDesc(tupdesc) — assign a transient typmod to the anonymous
    // RECORD descriptor via the typcache (execTuples.c). Mirrored inline (the
    // only owned step is the RECORDOID/typmod<0 guard around
    // assign_record_type_typmod), exactly as srf_support's InitMaterializedSRF.
    if td.tdtypeid == RECORDOID && td.tdtypmod < 0 {
        backend_utils_cache_typcache_seams::assign_record_type_typmod::call(&mut td)?;
    }

    // C: tuple = heap_form_tuple(tupdesc, values, isnull);
    let formed = backend_access_common_heaptuple::heap_form_tuple(mcx, &td, values, nulls)
        .map_err(|e| {
            ereport(ERROR)
                .errcode(types_error::ERRCODE_OUT_OF_MEMORY)
                .errmsg(format!("record_from_values: heap_form_tuple failed: {e:?}"))
                .into_error()
        })?;

    // C: PG_RETURN_DATUM(HeapTupleGetDatum(tuple)) — the composite/record-Datum
    // carrier bridge (task #161).
    backend_access_common_heaptuple::HeapTupleGetDatum(mcx, &formed, &td)
}
