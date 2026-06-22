//! Port of `backend/statistics/relation_stats.c`, `attribute_stats.c`, and
//! `stat_utils.c` — the direct statistics import/restore SQL functions
//! (`pg_restore_relation_stats` / `pg_restore_attribute_stats` /
//! `pg_clear_relation_stats` / `pg_clear_attribute_stats`) used by
//! `pg_dump --with-statistics` and the stats import/export tests.
//!
//! These functions are declared `provariadic => 'any'` and take name/value
//! pairs; the `stat_utils.c` machinery (`extract_variadic_args` /
//! `stats_fill_fcinfo_from_arg_pairs`) translates the pairs into a positional
//! argument map, then `relation_statistics_update` / `attribute_statistics_update`
//! write the corresponding `pg_class.relpages/reltuples/...` and `pg_statistic`
//! rows, exactly mirroring `vac_update_relstats` / `update_attstats`.
//!
//! The whole of all three C files is ported here (every branch, every stat
//! kind). The fmgr builtins are registered as Result-native from [`init_seams`].

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]

extern crate alloc;

use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use mcx::{Mcx, MemoryContext};

use types_core::primitive::{AttrNumber, BlockNumber, InvalidOid, Oid};
use types_datum::Datum as KeyDatum;
use types_error::{
    ErrorLocation, PgError, PgResult, SoftErrorContext, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
    ERRCODE_UNDEFINED_COLUMN, ERRCODE_UNDEFINED_OBJECT, ERRCODE_WRONG_OBJECT_TYPE, WARNING,
};
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};
use types_tuple::backend_access_common_heaptuple::Datum;

use types_acl::acl::{AclResult, ACL_MAINTAIN};
use types_cache::syscache::SysCacheKey;
use types_catalog::pg_class::{
    Anum_pg_class_relallfrozen, Anum_pg_class_relallvisible, Anum_pg_class_relisshared,
    Anum_pg_class_relkind, Anum_pg_class_relname, Anum_pg_class_relpages, Anum_pg_class_reltuples,
    RelationRelationId,
};
use types_catalog::pg_database::DatabaseRelationId;
use types_catalog::pg_type::{TYPTYPE_MULTIRANGE, TYPTYPE_RANGE};
use types_tuple::access::RangeVar;
use types_statistics::{
    Anum_pg_statistic_staattnum, Anum_pg_statistic_stacoll1, Anum_pg_statistic_stadistinct,
    Anum_pg_statistic_stainherit, Anum_pg_statistic_stakind1, Anum_pg_statistic_stanullfrac,
    Anum_pg_statistic_stanumbers1, Anum_pg_statistic_staop1, Anum_pg_statistic_starelid,
    Anum_pg_statistic_stavalues1, Anum_pg_statistic_stawidth, Natts_pg_statistic,
    StatisticRelationId, STATISTIC_KIND_CORRELATION, STATISTIC_KIND_HISTOGRAM, STATISTIC_KIND_MCV,
    STATISTIC_NUM_SLOTS,
};
use types_storage::lock::{AccessShareLock, NoLock, RowExclusiveLock, ShareUpdateExclusiveLock};

use backend_utils_error::ereport as ereport_builder;

use backend_access_common_heaptuple::{
    heap_deform_tuple, heap_form_tuple, heap_modify_tuple, heap_modify_tuple_by_cols,
};
use backend_access_table_table::{table_close, table_open};
use backend_access_transam_xact::CommandCounterIncrement;
use backend_catalog_index::IndexGetRelation;
use backend_catalog_indexing::keystone::{CatalogTupleDelete, CatalogTupleInsert, CatalogTupleUpdate};
use backend_catalog_namespace::{RangeVarGetRelidExtended, RangeVarGetRelidCallback};
use backend_catalog_objectaddress::resolve::{get_relkind_objtype, object_ownercheck};
use backend_catalog_pg_class::errdetail_relkind_not_supported;
use backend_nodes_core::makefuncs::make_range_var;
use backend_utils_adt_arrayfuncs::construct::{array_contains_nulls, construct_array_values};
use backend_utils_adt_arrayfuncs::io::array_in;
use backend_utils_adt_format_type::format_type_be_str;
use backend_utils_cache_lsyscache::attribute::{get_attname, get_attnum};
use backend_utils_cache_lsyscache::relation::{get_rel_name, get_rel_relkind};
use backend_utils_cache_lsyscache::type_::{
    get_base_element_type, get_multirange_range, type_is_multirange,
};
use backend_utils_cache_relcache_seams::relation_get_index_expressions;
use backend_utils_cache_syscache as syscache;
use backend_utils_cache_syscache::{ReleaseSysCache, SearchSysCacheExistsAttName};
use backend_utils_init_miscinit::GetUserId;
use backend_access_common_relation_seams::relation_open;

use backend_nodes_nodeFuncs_seams::expr_type_info;

/// Emit a WARNING (logs and continues, the C `ereport(WARNING, ...)`); never
/// returns `Err`.  `code` is the optional SQLSTATE; `detail`/`hint` optional.
fn warn(msg: String, code: Option<types_error::SqlState>, detail: Option<&str>) {
    let mut b = ereport_builder(WARNING).errmsg(msg);
    if let Some(c) = code {
        b = b.errcode(c);
    }
    if let Some(d) = detail {
        b = b.errdetail(d.to_string());
    }
    // WARNING `finish` returns Ok(()); discard.
    let _ = b.finish(here("stats_import"));
}

fn here(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("../src/backend/statistics/stat_utils.c", 0, funcname)
}

// ---------------------------------------------------------------------------
// Catalog / type OID constants used here (verified against PostgreSQL 18.3
// headers).  Several are not centralized in a `types-*` crate yet, so they are
// pinned locally with their `pg_type.dat` / `pg_operator.dat` / `pg_statistic.h`
// citation.
// ---------------------------------------------------------------------------

const TEXTOID: Oid = 25;
const BOOLOID: Oid = 16;
const INT2OID: Oid = 21;
const INT4OID: Oid = 23;
const FLOAT4OID: Oid = 700;
const FLOAT8OID: Oid = 701;
/// `_float4` (`pg_type.dat`: float4 `array_type_oid => '1021'`).
const FLOAT4ARRAYOID: Oid = 1021;
const UNKNOWNOID: Oid = 705;
/// `tsvector` (`pg_type.dat`).
const TSVECTOROID: Oid = 3614;
/// `DEFAULT_COLLATION_OID` (`pg_collation.dat`).
const DEFAULT_COLLATION_OID: Oid = 100;
/// `Float8LessOperator` (`pg_operator.dat`).
const Float8LessOperator: Oid = 672;

/// `STATISTIC_KIND_MCELEM` (`pg_statistic.h:247`).
const STATISTIC_KIND_MCELEM: i16 = 4;
/// `STATISTIC_KIND_DECHIST` (`pg_statistic.h:261`).
const STATISTIC_KIND_DECHIST: i16 = 5;
/// `STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM` (`pg_statistic.h:273`).
const STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM: i16 = 6;
/// `STATISTIC_KIND_BOUNDS_HISTOGRAM` (`pg_statistic.h:284`).
const STATISTIC_KIND_BOUNDS_HISTOGRAM: i16 = 7;

/// `TYPECACHE_LT_OPR | TYPECACHE_EQ_OPR` are resolved through the dedicated
/// `lookup_type_cache_lt_opr` / `lookup_type_cache_eq_opr` seams plus the
/// trimmed `lookup_type_cache(.., 0)` for `typtype`; no flag bits needed here.

// `pg_class` attnum constants are `i16` in `types-catalog`; convert at use.
const RELKIND_RELATION: u8 = b'r';
const RELKIND_INDEX: u8 = b'i';
const RELKIND_MATVIEW: u8 = b'm';
const RELKIND_FOREIGN_TABLE: u8 = b'f';
const RELKIND_PARTITIONED_TABLE: u8 = b'p';
const RELKIND_PARTITIONED_INDEX: u8 = b'I';

const InvalidAttrNumber: AttrNumber = 0;

/// `DEFAULT_NULL_FRAC` / `DEFAULT_AVG_WIDTH` / `DEFAULT_N_DISTINCT`
/// (attribute_stats.c).
fn default_null_frac() -> Datum<'static> {
    Datum::from_f32(0.0)
}
fn default_avg_width() -> Datum<'static> {
    Datum::from_i32(0)
}
fn default_n_distinct() -> Datum<'static> {
    Datum::from_f32(0.0)
}

// ===========================================================================
// stat_utils.c — argument metadata + variadic-pair machinery.
// ===========================================================================

/// `struct StatsArgInfo` (stat_utils.h): the name + expected type for one
/// positional argument.
#[derive(Clone, Copy)]
struct StatsArgInfo {
    argname: &'static str,
    argtype: Oid,
}

/// Positional arguments of `relation_statistics_update()` (relation_stats.c).
const RELSCHEMA_ARG: usize = 0;
const RELNAME_ARG: usize = 1;
const RELPAGES_ARG: usize = 2;
const RELTUPLES_ARG: usize = 3;
const RELALLVISIBLE_ARG: usize = 4;
const RELALLFROZEN_ARG: usize = 5;
const NUM_RELATION_STATS_ARGS: usize = 6;

fn relarginfo() -> [StatsArgInfo; NUM_RELATION_STATS_ARGS] {
    [
        StatsArgInfo { argname: "schemaname", argtype: TEXTOID },
        StatsArgInfo { argname: "relname", argtype: TEXTOID },
        StatsArgInfo { argname: "relpages", argtype: INT4OID },
        StatsArgInfo { argname: "reltuples", argtype: FLOAT4OID },
        StatsArgInfo { argname: "relallvisible", argtype: INT4OID },
        StatsArgInfo { argname: "relallfrozen", argtype: INT4OID },
    ]
}

/// Positional arguments of `attribute_statistics_update()` (attribute_stats.c).
const ATTRELSCHEMA_ARG: usize = 0;
const ATTRELNAME_ARG: usize = 1;
const ATTNAME_ARG: usize = 2;
const ATTNUM_ARG: usize = 3;
const INHERITED_ARG: usize = 4;
const NULL_FRAC_ARG: usize = 5;
const AVG_WIDTH_ARG: usize = 6;
const N_DISTINCT_ARG: usize = 7;
const MOST_COMMON_VALS_ARG: usize = 8;
const MOST_COMMON_FREQS_ARG: usize = 9;
const HISTOGRAM_BOUNDS_ARG: usize = 10;
const CORRELATION_ARG: usize = 11;
const MOST_COMMON_ELEMS_ARG: usize = 12;
const MOST_COMMON_ELEM_FREQS_ARG: usize = 13;
const ELEM_COUNT_HISTOGRAM_ARG: usize = 14;
const RANGE_LENGTH_HISTOGRAM_ARG: usize = 15;
const RANGE_EMPTY_FRAC_ARG: usize = 16;
const RANGE_BOUNDS_HISTOGRAM_ARG: usize = 17;
const NUM_ATTRIBUTE_STATS_ARGS: usize = 18;

fn attarginfo() -> [StatsArgInfo; NUM_ATTRIBUTE_STATS_ARGS] {
    [
        StatsArgInfo { argname: "schemaname", argtype: TEXTOID },
        StatsArgInfo { argname: "relname", argtype: TEXTOID },
        StatsArgInfo { argname: "attname", argtype: TEXTOID },
        StatsArgInfo { argname: "attnum", argtype: INT2OID },
        StatsArgInfo { argname: "inherited", argtype: BOOLOID },
        StatsArgInfo { argname: "null_frac", argtype: FLOAT4OID },
        StatsArgInfo { argname: "avg_width", argtype: INT4OID },
        StatsArgInfo { argname: "n_distinct", argtype: FLOAT4OID },
        StatsArgInfo { argname: "most_common_vals", argtype: TEXTOID },
        StatsArgInfo { argname: "most_common_freqs", argtype: FLOAT4ARRAYOID },
        StatsArgInfo { argname: "histogram_bounds", argtype: TEXTOID },
        StatsArgInfo { argname: "correlation", argtype: FLOAT4OID },
        StatsArgInfo { argname: "most_common_elems", argtype: TEXTOID },
        StatsArgInfo { argname: "most_common_elem_freqs", argtype: FLOAT4ARRAYOID },
        StatsArgInfo { argname: "elem_count_histogram", argtype: FLOAT4ARRAYOID },
        StatsArgInfo { argname: "range_length_histogram", argtype: TEXTOID },
        StatsArgInfo { argname: "range_empty_frac", argtype: FLOAT4OID },
        StatsArgInfo { argname: "range_bounds_histogram", argtype: TEXTOID },
    ]
}

/// The positional argument map the C `LOCAL_FCINFO(positional_fcinfo, N)` plays:
/// each slot holds either a present `Datum` (non-null arg) or `None` (the C
/// `args[i].isnull == true`). The unified [`Datum`] carries by-value scalars and
/// by-reference text/array images alike, so the per-kind handlers read whichever
/// shape they need.
struct PositionalArgs<'mcx> {
    args: Vec<Option<Datum<'mcx>>>,
}

impl<'mcx> PositionalArgs<'mcx> {
    fn new(n: usize) -> Self {
        PositionalArgs { args: (0..n).map(|_| None).collect() }
    }
    /// `PG_ARGISNULL(i)`.
    fn isnull(&self, i: usize) -> bool {
        self.args[i].is_none()
    }
    /// `PG_GETARG_DATUM(i)`.
    fn datum(&self, i: usize) -> &Datum<'mcx> {
        self.args[i].as_ref().expect("PositionalArgs::datum on a NULL arg")
    }
}

/// `stats_check_required_arg(fcinfo, arginfo, argnum)` (stat_utils.c).
fn stats_check_required_arg(
    args: &PositionalArgs,
    arginfo: &[StatsArgInfo],
    argnum: usize,
) -> PgResult<()> {
    if args.isnull(argnum) {
        return Err(PgError::error(format!(
            "argument \"{}\" must not be null",
            arginfo[argnum].argname
        ))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    }
    Ok(())
}

/// `stats_check_arg_array(fcinfo, arginfo, argnum)` (stat_utils.c). Emits a
/// WARNING and returns false on a problem.  The array arrives as its on-disk
/// varlena image on the by-ref lane.
fn stats_check_arg_array(
    args: &PositionalArgs,
    arginfo: &[StatsArgInfo],
    argnum: usize,
) -> PgResult<bool> {
    if args.isnull(argnum) {
        return Ok(true);
    }

    let image = args.datum(argnum).as_ref_bytes();

    // ARR_NDIM(arr) != 1
    if array_ndim(image) != 1 {
        warn(
            format!(
                "argument \"{}\" must not be a multidimensional array",
                arginfo[argnum].argname
            ),
            Some(ERRCODE_INVALID_PARAMETER_VALUE),
            None,
        );
        return Ok(false);
    }

    // array_contains_nulls(arr)
    if array_contains_nulls(image) {
        warn(
            format!(
                "argument \"{}\" array must not contain null values",
                arginfo[argnum].argname
            ),
            Some(ERRCODE_INVALID_PARAMETER_VALUE),
            None,
        );
        return Ok(false);
    }

    Ok(true)
}

/// `ARR_NDIM(arr)` — the first int32 of the array varlena header (after the 4
/// byte `vl_len_`).  The image on the lane is the full varlena (with header).
fn array_ndim(image: &[u8]) -> i32 {
    // ArrayType layout: int32 vl_len_(4) ; int32 ndim(4) ; ...
    if image.len() < 8 {
        return 0;
    }
    i32::from_ne_bytes([image[4], image[5], image[6], image[7]])
}

/// `stats_check_arg_pair(fcinfo, arginfo, argnum1, argnum2)` (stat_utils.c).
fn stats_check_arg_pair(
    args: &PositionalArgs,
    arginfo: &[StatsArgInfo],
    argnum1: usize,
    argnum2: usize,
) -> PgResult<bool> {
    if args.isnull(argnum1) && args.isnull(argnum2) {
        return Ok(true);
    }

    if args.isnull(argnum1) || args.isnull(argnum2) {
        let (nullarg, otherarg) = if args.isnull(argnum1) {
            (argnum1, argnum2)
        } else {
            (argnum2, argnum1)
        };

        warn(
            format!(
                "argument \"{}\" must be specified when argument \"{}\" is specified",
                arginfo[nullarg].argname, arginfo[otherarg].argname
            ),
            Some(ERRCODE_INVALID_PARAMETER_VALUE),
            None,
        );

        return Ok(false);
    }

    Ok(true)
}

/// `get_arg_by_name(argname, arginfo)` (stat_utils.c). Returns `None` (the C
/// `-1`) with a WARNING for an unrecognized name.
fn get_arg_by_name(argname: &str, arginfo: &[StatsArgInfo]) -> Option<usize> {
    for (argnum, info) in arginfo.iter().enumerate() {
        if argname.eq_ignore_ascii_case(info.argname) {
            return Some(argnum);
        }
    }

    warn(format!("unrecognized argument name: \"{argname}\""), None, None);

    None
}

/// `stats_check_arg_type(argname, argtype, expectedtype)` (stat_utils.c).
fn stats_check_arg_type(argname: &str, argtype: Oid, expectedtype: Oid) -> PgResult<bool> {
    if argtype != expectedtype {
        warn(
            format!(
                "argument \"{}\" has type {}, expected type {}",
                argname,
                format_type_be_str(argtype)?,
                format_type_be_str(expectedtype)?
            ),
            None,
            None,
        );
        return Ok(false);
    }
    Ok(true)
}

/// One name/value pair extracted from the variadic argument run.
struct VariadicElem<'mcx> {
    value: Datum<'mcx>,
    typ: Oid,
    isnull: bool,
}

/// `extract_variadic_args(fcinfo, 0, true, ...)` (funcapi.c). Returns the
/// per-element `(value, type, isnull)` triples. `None` is the C `return -1`
/// (`VARIADIC NULL`).
fn extract_variadic_args<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &FunctionCallInfoBaseData,
) -> PgResult<Option<Vec<VariadicElem<'mcx>>>> {
    let variadic = backend_utils_fmgr_core::get_fn_expr_variadic(fcinfo.flinfo.as_deref());

    if variadic {
        // Assert(PG_NARGS() == variadic_start + 1);
        // if (PG_ARGISNULL(variadic_start)) return -1;
        if fcinfo.arg(0).map(|d| d.isnull).unwrap_or(true) {
            return Ok(None);
        }
        let array_image = arg_value(mcx, fcinfo, 0)?;
        let (element_type, elems): (Oid, Vec<(Datum<'mcx>, bool)>) =
            backend_utils_adt_jsonb_seams::extract_variadic_array::call(mcx, &array_image)?;
        let mut out = Vec::with_capacity(elems.len());
        for (d, isnull) in elems {
            out.push(VariadicElem { value: d, typ: element_type, isnull });
        }
        Ok(Some(out))
    } else {
        let nargs = fcinfo.nargs();
        let mut out = Vec::with_capacity(nargs);
        for i in 0..nargs {
            let is_null = fcinfo.arg(i).map(|d| d.isnull).unwrap_or(false);
            let mut typ = backend_utils_fmgr_core::get_fn_expr_argtype(fcinfo.flinfo.as_deref(), i as i32);

            // Turn an unknown-type constant (a cstring on the by-ref lane) into
            // text.
            let value: Datum<'mcx> = if typ == UNKNOWNOID {
                typ = TEXTOID;
                if is_null {
                    Datum::null()
                } else if let Some(s) = fcinfo.ref_arg(i).and_then(|p| p.as_cstring()) {
                    // CStringGetTextDatum(PG_GETARG_POINTER(i)): build a real
                    // header-ful `text` varlena image (the canonical by-ref
                    // `text` Datum representation), matching the header-ful
                    // elements that `extract_variadic_array` hands back for a
                    // genuine `text` array. `text_datum_to_string` strips the
                    // header (VARDATA_ANY) on the way out.
                    backend_utils_adt_varlena_seams::cstring_to_text_v::call(mcx, s)?
                } else {
                    Datum::null()
                }
            } else if is_null {
                Datum::null()
            } else {
                arg_value(mcx, fcinfo, i)?
            };

            if typ == 0 || typ == UNKNOWNOID {
                return Err(PgError::error(format!(
                    "could not determine data type for argument {}",
                    i + 1
                ))
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
            }

            out.push(VariadicElem { value, typ, isnull: is_null });
        }
        Ok(Some(out))
    }
}

/// Materialize fmgr argument `i` as a unified [`Datum`] (by-value word,
/// by-reference varlena image, or cstring).
fn arg_value<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &FunctionCallInfoBaseData,
    i: usize,
) -> PgResult<Datum<'mcx>> {
    use types_fmgr::boundary::RefPayload;
    Ok(match fcinfo.ref_arg(i) {
        Some(RefPayload::Varlena(b)) => Datum::ByRef(mcx::slice_in(mcx, b)?),
        Some(RefPayload::Cstring(s)) => Datum::Cstring(s.clone()),
        Some(RefPayload::Composite(image)) => {
            Datum::Composite(types_tuple::FormedTuple::from_datum_image(mcx, image)?)
        }
        Some(RefPayload::Expanded(eo)) => {
            Datum::ByRef(mcx::slice_in(mcx, &types_datum::flatten_expanded(eo.as_ref()))?)
        }
        Some(RefPayload::Internal(_)) => {
            return Err(PgError::error("stats import: unexpected `internal` argument"));
        }
        None => Datum::ByVal(
            fcinfo
                .arg(i)
                .expect("stats import: missing by-value arg")
                .value
                .as_usize(),
        ),
    })
}

/// `stats_fill_fcinfo_from_arg_pairs(pairs_fcinfo, positional_fcinfo, arginfo)`
/// (stat_utils.c). Translates the variadic name/value pairs into the positional
/// argument map.  Returns false (with the relevant WARNINGs already emitted) if
/// any pair was unusable; raises ERROR for the structural problems.
fn stats_fill_fcinfo_from_arg_pairs<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &FunctionCallInfoBaseData,
    positional: &mut PositionalArgs<'mcx>,
    arginfo: &[StatsArgInfo],
) -> PgResult<bool> {
    let mut result = true;

    // positional args already cleared (all None) by PositionalArgs::new.

    let elems = match extract_variadic_args(mcx, fcinfo)? {
        Some(e) => e,
        // VARIADIC NULL -> nargs < 0; C treats nargs as -1, then `nargs % 2`
        // is non-zero and it raises the even-pairs error.  Reproduce by
        // treating as an odd count.
        None => {
            return Err(PgError::error("variadic arguments must be name/value pairs")
                .with_hint(
                    "Provide an even number of variadic arguments that can be divided into pairs.",
                ));
        }
    };

    let nargs = elems.len();
    if nargs % 2 != 0 {
        return Err(PgError::error("variadic arguments must be name/value pairs")
            .with_hint(
                "Provide an even number of variadic arguments that can be divided into pairs.",
            ));
    }

    let mut i = 0;
    while i < nargs {
        if elems[i].isnull {
            return Err(PgError::error(format!(
                "name at variadic position {} is null",
                i + 1
            )));
        }

        if elems[i].typ != TEXTOID {
            return Err(PgError::error(format!(
                "name at variadic position {} has type {}, expected type {}",
                i + 1,
                format_type_be_str(elems[i].typ)?,
                format_type_be_str(TEXTOID)?
            )));
        }

        if elems[i + 1].isnull {
            i += 2;
            continue;
        }

        let argname = text_datum_to_string(&elems[i].value)?;

        // The 'version' argument is accepted but ignored.
        if argname.eq_ignore_ascii_case("version") {
            i += 2;
            continue;
        }

        let argnum = get_arg_by_name(&argname, arginfo);

        match argnum {
            Some(argnum)
                if stats_check_arg_type(&argname, elems[i + 1].typ, arginfo[argnum].argtype)? =>
            {
                positional.args[argnum] = Some(elems[i + 1].value.clone());
            }
            _ => {
                result = false;
            }
        }

        i += 2;
    }

    Ok(result)
}

// ===========================================================================
// RangeVarCallbackForStats (stat_utils.c)
// ===========================================================================

/// `RangeVarCallbackForStats(relation, relId, oldRelId, arg)` (stat_utils.c).
/// `locked_oid` is the C `*(Oid *) arg`; the closure that calls this carries it
/// by `&mut`.
fn range_var_callback_for_stats(
    mcx: Mcx<'_>,
    relation: &RangeVar,
    rel_id: Oid,
    old_rel_id: Oid,
    locked_oid: &mut Oid,
) -> PgResult<()> {
    let mut table_oid = rel_id;

    // Release a now-useless lock from a previous attempt.
    if rel_id != old_rel_id && *locked_oid != InvalidOid {
        backend_storage_lmgr_lmgr_seams::unlock_relation_oid::call(
            *locked_oid,
            ShareUpdateExclusiveLock,
        )?;
        *locked_oid = InvalidOid;
    }

    // If the relation does not exist, there's nothing more to do.
    if rel_id == InvalidOid {
        return Ok(());
    }

    // If the relation does exist, check whether it's an index.
    let relkind = get_rel_relkind(rel_id)?;
    if relkind == RELKIND_INDEX || relkind == RELKIND_PARTITIONED_INDEX {
        table_oid = IndexGetRelation(rel_id, false)?;
    }

    if rel_id == old_rel_id {
        if table_oid == rel_id && *locked_oid != InvalidOid {
            return Err(PgError::error(format!(
                "index \"{}\" was concurrently dropped",
                rangevar_relname(relation)
            ))
            .with_sqlstate(ERRCODE_UNDEFINED_OBJECT));
        }

        if table_oid != rel_id && table_oid != *locked_oid {
            return Err(PgError::error(format!(
                "index \"{}\" was concurrently created",
                rangevar_relname(relation)
            ))
            .with_sqlstate(ERRCODE_UNDEFINED_OBJECT));
        }
    }

    let tuple = syscache::SearchSysCache1(
        mcx,
        syscache::RELOID,
        SysCacheKey::Value(KeyDatum::from_oid(table_oid)),
    )?
    .ok_or_else(|| PgError::error(format!("cache lookup failed for OID {table_oid}")))?;

    // GETSTRUCT(tuple): the pg_class form fields we read.
    let form = pg_class_form(mcx, &tuple)?;

    // the relkinds that can be used with ANALYZE
    match form.relkind {
        RELKIND_RELATION | RELKIND_MATVIEW | RELKIND_FOREIGN_TABLE | RELKIND_PARTITIONED_TABLE => {}
        other => {
            let detail = errdetail_relkind_not_supported(other)?;
            return Err(PgError::error(format!(
                "cannot modify statistics for relation \"{}\"",
                form.relname
            ))
            .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE)
            .with_detail(detail));
        }
    }

    if form.relisshared {
        return Err(
            PgError::error("cannot modify statistics for shared relation")
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        );
    }

    // Check permissions.
    if !object_ownercheck(DatabaseRelationId, my_database_id(), GetUserId())? {
        let aclresult = backend_catalog_aclchk::pg_class_aclcheck(
            mcx,
            table_oid,
            GetUserId(),
            ACL_MAINTAIN,
        )?;
        if aclresult != AclResult::AclcheckOk {
            backend_catalog_aclchk_seams::aclcheck_error::call(
                aclresult,
                get_relkind_objtype(form.relkind),
                Some(form.relname.clone()),
            )?;
        }
    }

    ReleaseSysCache(tuple);

    // Lock heap before index to avoid deadlock.
    if rel_id != old_rel_id && table_oid != rel_id {
        // C: LockRelationOid(table_oid, ShareUpdateExclusiveLock).  The lock
        // must outlive the callback; the returned RAII guard is leaked so the
        // lock persists (released at transaction end / by the explicit
        // unlock above), exactly mirroring the C manual lock/unlock.
        let guard = backend_storage_lmgr_lmgr_seams::lock_relation_oid::call(
            table_oid,
            ShareUpdateExclusiveLock,
        )?;
        core::mem::forget(guard);
        *locked_oid = table_oid;
    }

    Ok(())
}

/// The trimmed `Form_pg_class` fields `RangeVarCallbackForStats` reads.
struct PgClassFields {
    relkind: u8,
    relisshared: bool,
    relname: String,
}

/// `(Form_pg_class) GETSTRUCT(tuple)` for the fields the callback needs, read by
/// deforming the pg_class tuple against the open pg_class descriptor.
fn pg_class_form(mcx: Mcx<'_>, tuple: &types_tuple::FormedTuple<'_>) -> PgResult<PgClassFields> {
    // pg_class column order (pg_class.h): oid(1) relname(2) ... relisshared(16)
    // relkind(17) ...  Deform against the pg_class descriptor.
    let crel = relation_open::call(mcx, RelationRelationId, AccessShareLock)?;
    let deformed = heap_deform_tuple(mcx, &tuple.tuple, &crel.rd_att, &tuple.data)?;

    let relname = read_name_datum(&deformed[Anum_pg_class_relname as usize - 1].0);
    let relisshared = deformed[Anum_pg_class_relisshared as usize - 1].0.as_bool();
    let relkind = deformed[Anum_pg_class_relkind as usize - 1].0.as_char() as u8;

    crel.close(AccessShareLock)?;

    Ok(PgClassFields { relkind, relisshared, relname })
}

/// `RangeVar->relname` as a `&str`.
fn rangevar_relname(rv: &RangeVar) -> &str {
    rv.relname.as_str()
}

/// `RelationGetRelationName(rel)` — the open relation's name (`rd_rel.relname`).
fn rangevar_relname_from_rel(rel: &types_rel::Relation<'_>) -> String {
    rel.rd_rel.relname.as_str().to_string()
}

/// `NameStr(...)` for a by-reference `name` Datum (NUL-padded fixed buffer).
fn read_name_datum(d: &Datum) -> String {
    let bytes = d.as_ref_bytes();
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    String::from_utf8_lossy(&bytes[..end]).into_owned()
}

/// `MyDatabaseId`.
fn my_database_id() -> Oid {
    backend_utils_init_small_seams::my_database_id::call()
}

// ===========================================================================
// relation_stats.c
// ===========================================================================

/// `relation_statistics_update(fcinfo)` (relation_stats.c).
fn relation_statistics_update(mcx: Mcx<'_>, args: &PositionalArgs) -> PgResult<bool> {
    let mut result = true;
    let arginfo = relarginfo();

    stats_check_required_arg(args, &arginfo, RELSCHEMA_ARG)?;
    stats_check_required_arg(args, &arginfo, RELNAME_ARG)?;

    let nspname = text_datum_to_string(args.datum(RELSCHEMA_ARG))?;
    let relname = text_datum_to_string(args.datum(RELNAME_ARG))?;

    if backend_access_transam_xlog::RecoveryInProgress() {
        return Err(PgError::error("recovery is in progress")
            .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .with_hint("Statistics cannot be modified during recovery."));
    }

    let reloid = lookup_relation(mcx, &nspname, &relname)?;

    let mut relpages: BlockNumber = 0;
    let mut update_relpages = false;
    let mut reltuples: f32 = 0.0;
    let mut update_reltuples = false;
    let mut relallvisible: BlockNumber = 0;
    let mut update_relallvisible = false;
    let mut relallfrozen: BlockNumber = 0;
    let mut update_relallfrozen = false;

    if !args.isnull(RELPAGES_ARG) {
        relpages = args.datum(RELPAGES_ARG).as_u32();
        update_relpages = true;
    }

    if !args.isnull(RELTUPLES_ARG) {
        reltuples = args.datum(RELTUPLES_ARG).as_f32();
        if reltuples < -1.0 {
            warn(
                "argument \"reltuples\" must not be less than -1.0".to_string(),
                Some(ERRCODE_INVALID_PARAMETER_VALUE),
                None,
            );
            result = false;
        } else {
            update_reltuples = true;
        }
    }

    if !args.isnull(RELALLVISIBLE_ARG) {
        relallvisible = args.datum(RELALLVISIBLE_ARG).as_u32();
        update_relallvisible = true;
    }

    if !args.isnull(RELALLFROZEN_ARG) {
        relallfrozen = args.datum(RELALLFROZEN_ARG).as_u32();
        update_relallfrozen = true;
    }

    // Take RowExclusiveLock on pg_class, consistent with vac_update_relstats().
    let crel = table_open(mcx, RelationRelationId, RowExclusiveLock)?;

    let ctup = syscache::SearchSysCache1(
        mcx,
        syscache::RELOID,
        SysCacheKey::Value(KeyDatum::from_oid(reloid)),
    )?
    .ok_or_else(|| PgError::error(format!("pg_class entry for relid {reloid} not found")))?;

    // GETSTRUCT(ctup): the current relpages/reltuples/relallvisible/relallfrozen.
    let cur = heap_deform_tuple(mcx, &ctup.tuple, &crel.rd_att, &ctup.data)?;
    let cur_relpages = cur[Anum_pg_class_relpages as usize - 1].0.as_u32();
    let cur_reltuples = cur[Anum_pg_class_reltuples as usize - 1].0.as_f32();
    let cur_relallvisible = cur[Anum_pg_class_relallvisible as usize - 1].0.as_u32();
    let cur_relallfrozen = cur[Anum_pg_class_relallfrozen as usize - 1].0.as_u32();

    let mut replaces: Vec<i32> = Vec::with_capacity(4);
    let mut values: Vec<Datum> = Vec::with_capacity(4);
    let nulls: Vec<bool> = alloc::vec![false; 4];

    if update_relpages && relpages != cur_relpages {
        replaces.push(Anum_pg_class_relpages as i32);
        values.push(Datum::from_u32(relpages));
    }
    if update_reltuples && reltuples != cur_reltuples {
        replaces.push(Anum_pg_class_reltuples as i32);
        values.push(Datum::from_f32(reltuples));
    }
    if update_relallvisible && relallvisible != cur_relallvisible {
        replaces.push(Anum_pg_class_relallvisible as i32);
        values.push(Datum::from_u32(relallvisible));
    }
    if update_relallfrozen && relallfrozen != cur_relallfrozen {
        replaces.push(Anum_pg_class_relallfrozen as i32);
        values.push(Datum::from_u32(relallfrozen));
    }

    let nreplaces = replaces.len();
    if nreplaces > 0 {
        let mut newtup = heap_modify_tuple_by_cols(
            mcx,
            &ctup,
            &crel.rd_att,
            nreplaces as i32,
            &replaces,
            &values,
            &nulls[..nreplaces],
        )
        .map_err(|_| PgError::error("heap_modify_tuple_by_cols failed in relation_statistics_update"))?;
        let otid = newtup.tuple.t_self;
        CatalogTupleUpdate(mcx, &crel, otid, &mut newtup)?;
    }

    ReleaseSysCache(ctup);

    // release the lock, consistent with vac_update_relstats()
    table_close(crel, RowExclusiveLock)?;

    CommandCounterIncrement()?;

    Ok(result)
}

// ===========================================================================
// attribute_stats.c
// ===========================================================================

/// `attribute_statistics_update(fcinfo)` (attribute_stats.c). Inserts or updates
/// the `pg_statistic` row for one relation attribute, one stat kind at a time.
fn attribute_statistics_update(mcx: Mcx<'_>, args: &PositionalArgs) -> PgResult<bool> {
    let arginfo = attarginfo();
    let mut result = true;

    let mut do_mcv =
        !args.isnull(MOST_COMMON_FREQS_ARG) && !args.isnull(MOST_COMMON_VALS_ARG);
    let mut do_histogram = !args.isnull(HISTOGRAM_BOUNDS_ARG);
    let mut do_correlation = !args.isnull(CORRELATION_ARG);
    let mut do_mcelem =
        !args.isnull(MOST_COMMON_ELEMS_ARG) && !args.isnull(MOST_COMMON_ELEM_FREQS_ARG);
    let mut do_dechist = !args.isnull(ELEM_COUNT_HISTOGRAM_ARG);
    let mut do_bounds_histogram = !args.isnull(RANGE_BOUNDS_HISTOGRAM_ARG);
    let mut do_range_length_histogram =
        !args.isnull(RANGE_LENGTH_HISTOGRAM_ARG) && !args.isnull(RANGE_EMPTY_FRAC_ARG);

    stats_check_required_arg(args, &arginfo, ATTRELSCHEMA_ARG)?;
    stats_check_required_arg(args, &arginfo, ATTRELNAME_ARG)?;

    let nspname = text_datum_to_string(args.datum(ATTRELSCHEMA_ARG))?;
    let relname = text_datum_to_string(args.datum(ATTRELNAME_ARG))?;

    if backend_access_transam_xlog::RecoveryInProgress() {
        return Err(PgError::error("recovery is in progress")
            .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .with_hint("Statistics cannot be modified during recovery."));
    }

    // lock before looking up attribute
    let reloid = lookup_relation(mcx, &nspname, &relname)?;

    // user can specify either attname or attnum, but not both
    let (attname, attnum): (String, AttrNumber) = if !args.isnull(ATTNAME_ARG) {
        if !args.isnull(ATTNUM_ARG) {
            return Err(PgError::error("cannot specify both \"attname\" and \"attnum\"")
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
        }
        let attname = text_datum_to_string(args.datum(ATTNAME_ARG))?;
        let attnum = get_attnum(reloid, &attname)?;
        if attnum == InvalidAttrNumber {
            return Err(PgError::error(format!(
                "column \"{attname}\" of relation \"{relname}\" does not exist"
            ))
            .with_sqlstate(ERRCODE_UNDEFINED_COLUMN));
        }
        (attname, attnum)
    } else if !args.isnull(ATTNUM_ARG) {
        let attnum = args.datum(ATTNUM_ARG).as_i16() as AttrNumber;
        let attname_opt = get_attname(mcx, reloid, attnum, true)?;
        let attname = match &attname_opt {
            Some(s) => s.as_str().to_string(),
            None => String::new(),
        };
        // get_attname doesn't check attisdropped.
        if attname_opt.is_none() || !SearchSysCacheExistsAttName(mcx, reloid, &attname)? {
            return Err(PgError::error(format!(
                "column {attnum} of relation \"{relname}\" does not exist"
            ))
            .with_sqlstate(ERRCODE_UNDEFINED_COLUMN));
        }
        (attname, attnum)
    } else {
        return Err(PgError::error("must specify either \"attname\" or \"attnum\"")
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    };

    if attnum < 0 {
        return Err(PgError::error(format!(
            "cannot modify statistics on system column \"{attname}\""
        ))
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
    }

    stats_check_required_arg(args, &arginfo, INHERITED_ARG)?;
    let inherited = args.datum(INHERITED_ARG).as_bool();

    // Check argument sanity. If some arguments are unusable, emit a WARNING and
    // disable the corresponding stat kind.
    if !stats_check_arg_array(args, &arginfo, MOST_COMMON_FREQS_ARG)? {
        do_mcv = false;
        result = false;
    }
    if !stats_check_arg_array(args, &arginfo, MOST_COMMON_ELEM_FREQS_ARG)? {
        do_mcelem = false;
        result = false;
    }
    if !stats_check_arg_array(args, &arginfo, ELEM_COUNT_HISTOGRAM_ARG)? {
        do_dechist = false;
        result = false;
    }
    if !stats_check_arg_pair(args, &arginfo, MOST_COMMON_VALS_ARG, MOST_COMMON_FREQS_ARG)? {
        do_mcv = false;
        result = false;
    }
    if !stats_check_arg_pair(args, &arginfo, MOST_COMMON_ELEMS_ARG, MOST_COMMON_ELEM_FREQS_ARG)? {
        do_mcelem = false;
        result = false;
    }
    if !stats_check_arg_pair(
        args,
        &arginfo,
        RANGE_LENGTH_HISTOGRAM_ARG,
        RANGE_EMPTY_FRAC_ARG,
    )? {
        do_range_length_histogram = false;
        result = false;
    }

    // derive information from attribute
    let StatType {
        atttypid,
        atttypmod,
        atttyptype,
        atttypcoll,
        eq_opr,
        lt_opr,
    } = get_attr_stat_type(mcx, reloid, attnum)?;

    // if needed, derive element type
    let mut elemtypid = InvalidOid;
    let mut elem_eq_opr = InvalidOid;
    if do_mcelem || do_dechist {
        match get_elem_stat_type(atttypid, atttyptype)? {
            Some((etid, eeo)) => {
                elemtypid = etid;
                elem_eq_opr = eeo;
            }
            None => {
                warn(
                    format!("could not determine element type of column \"{attname}\""),
                    None,
                    Some("Cannot set STATISTIC_KIND_MCELEM or STATISTIC_KIND_DECHIST."),
                );
                do_mcelem = false;
                do_dechist = false;
                result = false;
            }
        }
    }

    // histogram and correlation require less-than operator
    if (do_histogram || do_correlation) && lt_opr == InvalidOid {
        warn(
            format!("could not determine less-than operator for column \"{attname}\""),
            Some(ERRCODE_INVALID_PARAMETER_VALUE),
            Some("Cannot set STATISTIC_KIND_HISTOGRAM or STATISTIC_KIND_CORRELATION."),
        );
        do_histogram = false;
        do_correlation = false;
        result = false;
    }

    // only range types can have range stats
    if (do_range_length_histogram || do_bounds_histogram)
        && !(atttyptype == TYPTYPE_RANGE || atttyptype == TYPTYPE_MULTIRANGE)
    {
        warn(
            format!("column \"{attname}\" is not a range type"),
            Some(ERRCODE_INVALID_PARAMETER_VALUE),
            Some("Cannot set STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM or STATISTIC_KIND_BOUNDS_HISTOGRAM."),
        );
        do_bounds_histogram = false;
        do_range_length_histogram = false;
        result = false;
    }

    let starel = table_open(mcx, StatisticRelationId, RowExclusiveLock)?;

    let statup = syscache::SearchSysCache3(
        mcx,
        syscache::STATRELATTINH,
        SysCacheKey::Value(KeyDatum::from_oid(reloid)),
        SysCacheKey::Value(KeyDatum::from_i16(attnum as i16)),
        SysCacheKey::Value(KeyDatum::from_bool(inherited)),
    )?;

    // values/nulls/replaces over Natts_pg_statistic columns.
    let mut values: Vec<Datum> = (0..Natts_pg_statistic).map(|_| Datum::null()).collect();
    let mut nulls: Vec<bool> = alloc::vec![false; Natts_pg_statistic];
    let mut replaces: Vec<bool> = alloc::vec![false; Natts_pg_statistic];

    // initialize from existing tuple if it exists
    if let Some(ref statup) = statup {
        let deformed = heap_deform_tuple(mcx, &statup.tuple, &starel.rd_att, &statup.data)?;
        for (i, (v, n)) in deformed.into_iter().enumerate() {
            values[i] = v;
            nulls[i] = n;
        }
    } else {
        init_empty_stats_tuple(reloid, attnum as i16, inherited, &mut values, &mut nulls, &mut replaces);
    }

    // if specified, set to argument values
    if !args.isnull(NULL_FRAC_ARG) {
        values[Anum_pg_statistic_stanullfrac - 1] = args.datum(NULL_FRAC_ARG).clone();
        replaces[Anum_pg_statistic_stanullfrac - 1] = true;
    }
    if !args.isnull(AVG_WIDTH_ARG) {
        values[Anum_pg_statistic_stawidth - 1] = args.datum(AVG_WIDTH_ARG).clone();
        replaces[Anum_pg_statistic_stawidth - 1] = true;
    }
    if !args.isnull(N_DISTINCT_ARG) {
        values[Anum_pg_statistic_stadistinct - 1] = args.datum(N_DISTINCT_ARG).clone();
        replaces[Anum_pg_statistic_stadistinct - 1] = true;
    }

    // STATISTIC_KIND_MCV
    if do_mcv {
        let stanumbers = args.datum(MOST_COMMON_FREQS_ARG).clone();
        match text_to_stavalues(
            mcx,
            "most_common_vals",
            args.datum(MOST_COMMON_VALS_ARG),
            atttypid,
            atttypmod,
        )? {
            Some(stavalues) => set_stats_slot(
                &mut values,
                &mut nulls,
                &mut replaces,
                STATISTIC_KIND_MCV,
                eq_opr,
                atttypcoll,
                Some(stanumbers),
                Some(stavalues),
            )?,
            None => result = false,
        }
    }

    // STATISTIC_KIND_HISTOGRAM
    if do_histogram {
        match text_to_stavalues(
            mcx,
            "histogram_bounds",
            args.datum(HISTOGRAM_BOUNDS_ARG),
            atttypid,
            atttypmod,
        )? {
            Some(stavalues) => set_stats_slot(
                &mut values,
                &mut nulls,
                &mut replaces,
                STATISTIC_KIND_HISTOGRAM,
                lt_opr,
                atttypcoll,
                None,
                Some(stavalues),
            )?,
            None => result = false,
        }
    }

    // STATISTIC_KIND_CORRELATION
    if do_correlation {
        let elems = [args.datum(CORRELATION_ARG).clone()];
        let arry = construct_array_values(mcx, &elems, FLOAT4OID, 4, true, b'i')?;
        let stanumbers = Datum::ByRef(arry);
        set_stats_slot(
            &mut values,
            &mut nulls,
            &mut replaces,
            STATISTIC_KIND_CORRELATION,
            lt_opr,
            atttypcoll,
            Some(stanumbers),
            None,
        )?;
    }

    // STATISTIC_KIND_MCELEM
    if do_mcelem {
        let stanumbers = args.datum(MOST_COMMON_ELEM_FREQS_ARG).clone();
        match text_to_stavalues(
            mcx,
            "most_common_elems",
            args.datum(MOST_COMMON_ELEMS_ARG),
            elemtypid,
            atttypmod,
        )? {
            Some(stavalues) => set_stats_slot(
                &mut values,
                &mut nulls,
                &mut replaces,
                STATISTIC_KIND_MCELEM,
                elem_eq_opr,
                atttypcoll,
                Some(stanumbers),
                Some(stavalues),
            )?,
            None => result = false,
        }
    }

    // STATISTIC_KIND_DECHIST
    if do_dechist {
        let stanumbers = args.datum(ELEM_COUNT_HISTOGRAM_ARG).clone();
        set_stats_slot(
            &mut values,
            &mut nulls,
            &mut replaces,
            STATISTIC_KIND_DECHIST,
            elem_eq_opr,
            atttypcoll,
            Some(stanumbers),
            None,
        )?;
    }

    // STATISTIC_KIND_BOUNDS_HISTOGRAM (appears before RANGE_LENGTH for the C
    // quirk; preserved).
    if do_bounds_histogram {
        match text_to_stavalues(
            mcx,
            "range_bounds_histogram",
            args.datum(RANGE_BOUNDS_HISTOGRAM_ARG),
            atttypid,
            atttypmod,
        )? {
            Some(stavalues) => set_stats_slot(
                &mut values,
                &mut nulls,
                &mut replaces,
                STATISTIC_KIND_BOUNDS_HISTOGRAM,
                InvalidOid,
                InvalidOid,
                None,
                Some(stavalues),
            )?,
            None => result = false,
        }
    }

    // STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM
    if do_range_length_histogram {
        // The anyarray is always a float8[] for this stakind; the stanumbers is
        // a float4[] of the empty-fraction value.
        let elems = [args.datum(RANGE_EMPTY_FRAC_ARG).clone()];
        let arry = construct_array_values(mcx, &elems, FLOAT4OID, 4, true, b'i')?;
        let stanumbers = Datum::ByRef(arry);

        match text_to_stavalues(
            mcx,
            "range_length_histogram",
            args.datum(RANGE_LENGTH_HISTOGRAM_ARG),
            FLOAT8OID,
            0,
        )? {
            Some(stavalues) => set_stats_slot(
                &mut values,
                &mut nulls,
                &mut replaces,
                STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM,
                Float8LessOperator,
                InvalidOid,
                Some(stanumbers),
                Some(stavalues),
            )?,
            None => result = false,
        }
    }

    upsert_pg_statistic(mcx, &starel, statup.as_ref(), &values, &nulls, &replaces)?;

    if let Some(statup) = statup {
        ReleaseSysCache(statup);
    }
    table_close(starel, RowExclusiveLock)?;

    Ok(result)
}

/// Derived attribute type information from `get_attr_stat_type`.
struct StatType {
    atttypid: Oid,
    atttypmod: i32,
    atttyptype: i8,
    atttypcoll: Oid,
    eq_opr: Oid,
    lt_opr: Oid,
}

/// `get_attr_expr(rel, attnum)` (attribute_stats.c) — resolve an index
/// expression attribute's defining expression tree, or `None` for a plain
/// column.
fn get_attr_expr<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &types_rel::Relation<'mcx>,
    attnum: i32,
) -> PgResult<Option<types_nodes::primnodes::Expr<'mcx>>> {
    // relation is not an index
    if rel.rd_rel.relkind != RELKIND_INDEX && rel.rd_rel.relkind != RELKIND_PARTITIONED_INDEX {
        return Ok(None);
    }

    let index_exprs = relation_get_index_expressions::call(mcx, rel)?;

    // index has no expressions to give
    let index_exprs = match index_exprs {
        None => return Ok(None),
        Some(e) if e.is_empty() => return Ok(None),
        Some(e) => e,
    };

    let rd_index = rel
        .rd_index
        .as_ref()
        .ok_or_else(|| PgError::error("get_attr_expr: index relation has no rd_index"))?;

    // The index attnum points directly to a relation attnum -> not an
    // expression attribute.
    if rd_index.indkey[(attnum - 1) as usize] != 0 {
        return Ok(None);
    }

    // Walk the expression list to the right position: one entry per zero-keyed
    // index column up to attnum.
    let mut indexpr_item = 0usize;
    for i in 0..(attnum - 1) as usize {
        if rd_index.indkey[i] == 0 {
            indexpr_item += 1;
        }
    }

    if indexpr_item >= index_exprs.len() {
        return Err(PgError::error("too few entries in indexprs list"));
    }

    Ok(Some(index_exprs[indexpr_item].clone()))
}

/// `get_attr_stat_type(reloid, attnum, ...)` (attribute_stats.c).
fn get_attr_stat_type(mcx: Mcx<'_>, reloid: Oid, attnum: AttrNumber) -> PgResult<StatType> {
    let rel = relation_open::call(mcx, reloid, AccessShareLock)?;

    let atup = syscache::SearchSysCache2(
        mcx,
        syscache::ATTNUM,
        SysCacheKey::Value(KeyDatum::from_oid(reloid)),
        SysCacheKey::Value(KeyDatum::from_i16(attnum as i16)),
    )?;

    let atup = match atup {
        Some(t) => t,
        None => {
            return Err(PgError::error(format!(
                "column {attnum} of relation \"{}\" does not exist",
                rangevar_relname_from_rel(&rel)
            ))
            .with_sqlstate(ERRCODE_UNDEFINED_COLUMN));
        }
    };

    let attr = pg_attribute_form(mcx, &atup)?;

    if attr.attisdropped {
        ReleaseSysCache(atup);
        return Err(PgError::error(format!(
            "column {attnum} of relation \"{}\" does not exist",
            rangevar_relname_from_rel(&rel)
        ))
        .with_sqlstate(ERRCODE_UNDEFINED_COLUMN));
    }

    let expr = get_attr_expr(mcx, &rel, attr.attnum as i32)?;

    let mut atttypid;
    let atttypmod;
    let mut atttypcoll;

    if expr.is_none() {
        atttypid = attr.atttypid;
        atttypmod = attr.atttypmod;
        atttypcoll = attr.attcollation;
    } else {
        let ti = expr_type_info::call(expr.as_ref().unwrap())?;
        atttypid = ti.typid;
        atttypmod = ti.typmod;
        atttypcoll = if attr.attcollation != InvalidOid {
            attr.attcollation
        } else {
            ti.collation
        };
    }
    ReleaseSysCache(atup);

    // If it's a multirange, step down to the range type.
    if type_is_multirange(atttypid)? {
        atttypid = get_multirange_range(atttypid)?;
    }

    // finds the right operators even if atttypid is a domain
    let tce = backend_utils_cache_typcache_seams::lookup_type_cache::call(atttypid, 0)?;
    let atttyptype = tce.typtype;
    let eq_opr = backend_utils_cache_typcache_seams::lookup_type_cache_eq_opr::call(atttypid)?;
    let lt_opr = backend_utils_cache_typcache_seams::lookup_type_cache_lt_opr::call(atttypid)?;

    // Special case: collation for tsvector is DEFAULT_COLLATION_OID.
    if atttypid == TSVECTOROID {
        atttypcoll = DEFAULT_COLLATION_OID;
    }

    rel.close(NoLock)?;

    Ok(StatType { atttypid, atttypmod, atttyptype, atttypcoll, eq_opr, lt_opr })
}

/// `get_elem_stat_type(atttypid, atttyptype, ...)` (attribute_stats.c). Returns
/// `None` for the C `return false`.
fn get_elem_stat_type(atttypid: Oid, _atttyptype: i8) -> PgResult<Option<(Oid, Oid)>> {
    let elemtypid = if atttypid == TSVECTOROID {
        // Special case: element type for tsvector is text.
        TEXTOID
    } else {
        // find underlying element type through any domain
        get_base_element_type(atttypid)?
    };

    if elemtypid == InvalidOid {
        return Ok(None);
    }

    // finds the right operator even if elemtypid is a domain
    let elem_eq_opr =
        backend_utils_cache_typcache_seams::lookup_type_cache_eq_opr::call(elemtypid)?;
    if elem_eq_opr == InvalidOid {
        return Ok(None);
    }

    Ok(Some((elemtypid, elem_eq_opr)))
}

/// The `Form_pg_attribute` fields `get_attr_stat_type` reads.
struct PgAttributeFields {
    attnum: AttrNumber,
    atttypid: Oid,
    atttypmod: i32,
    attcollation: Oid,
    attisdropped: bool,
}

/// `(Form_pg_attribute) GETSTRUCT(atup)` for the fields we need.  pg_attribute
/// (`pg_attribute.h`): attrelid(1) attname(2) atttypid(3) attlen(4) attnum(5)
/// atttypmod(6) ... attisdropped(17) ... attcollation(20).
fn pg_attribute_form(mcx: Mcx<'_>, atup: &types_tuple::FormedTuple<'_>) -> PgResult<PgAttributeFields> {
    let arel = relation_open::call(mcx, AttributeRelationId, AccessShareLock)?;
    let deformed = heap_deform_tuple(mcx, &atup.tuple, &arel.rd_att, &atup.data)?;

    let atttypid = deformed[Anum_pg_attribute_atttypid as usize - 1].0.as_oid();
    let attnum = deformed[Anum_pg_attribute_attnum as usize - 1].0.as_i16() as AttrNumber;
    let atttypmod = deformed[Anum_pg_attribute_atttypmod as usize - 1].0.as_i32();
    let attcollation = deformed[Anum_pg_attribute_attcollation as usize - 1].0.as_oid();
    let attisdropped = deformed[Anum_pg_attribute_attisdropped as usize - 1].0.as_bool();

    arel.close(AccessShareLock)?;

    Ok(PgAttributeFields { attnum, atttypid, atttypmod, attcollation, attisdropped })
}

const AttributeRelationId: Oid = 1249;
const Anum_pg_attribute_atttypid: i16 = 3;
const Anum_pg_attribute_attnum: i16 = 5;
const Anum_pg_attribute_atttypmod: i16 = 6;
const Anum_pg_attribute_attisdropped: i16 = 17;
const Anum_pg_attribute_attcollation: i16 = 20;

/// `text_to_stavalues(staname, array_in, d, typid, typmod, ok)`
/// (attribute_stats.c).  Casts the text datum into an array of `typid` via
/// `array_in` with a SOFT error context, so a conversion failure becomes a
/// WARNING (returning `None`) rather than aborting; a NULL-containing result is
/// likewise a WARNING.  `Some(array_image)` carries the converted by-reference
/// array.
fn text_to_stavalues<'mcx>(
    mcx: Mcx<'mcx>,
    staname: &str,
    d: &Datum,
    typid: Oid,
    typmod: i32,
) -> PgResult<Option<Datum<'mcx>>> {
    // The array-typed `text` value (`most_common_vals`/`histogram_bounds`/…)
    // is an explicitly-typed argument, so it crosses the fmgr by-ref lane as a
    // header-ful varlena (unlike the undecorated-literal argument NAMES, which
    // arrive `unknown`/cstring and are already header-free). Strip the varlena
    // header (`TextDatumGetCString` = `text_to_cstring(DatumGetTextPP(d))`)
    // before handing the cstring to `array_in`.
    let s = direct_text_datum_to_string(d)?;

    let mut escontext = SoftErrorContext::new(true);
    let array = array_in(mcx, &s, typid, typmod, Some(&mut escontext))?;

    // A soft error was captured -> re-throw as WARNING, return None.
    if escontext.error_occurred() {
        if let Some(err) = escontext.take_error() {
            // ThrowErrorData(escontext.error_data) with elevel = WARNING.
            warn(err.message().to_string(), Some(err.sqlstate()), None);
        }
        return Ok(None);
    }

    let image = match array {
        Some(img) => img,
        // Defensive: a soft path that returned None without a flagged error.
        None => return Ok(None),
    };

    if array_contains_nulls(&image) {
        warn(
            format!("\"{staname}\" array must not contain null values"),
            Some(ERRCODE_INVALID_PARAMETER_VALUE),
            None,
        );
        return Ok(None);
    }

    Ok(Some(Datum::ByRef(image)))
}

/// `set_stats_slot(...)` (attribute_stats.c). Find the slot with the given
/// stakind, or the first empty slot; write stakind/staop/stacoll and the
/// optional stanumbers/stavalues.
fn set_stats_slot<'mcx>(
    values: &mut [Datum<'mcx>],
    nulls: &mut [bool],
    replaces: &mut [bool],
    stakind: i16,
    staop: Oid,
    stacoll: Oid,
    stanumbers: Option<Datum<'mcx>>,
    stavalues: Option<Datum<'mcx>>,
) -> PgResult<()> {
    let mut slotidx = 0usize;
    let mut first_empty: i32 = -1;

    while slotidx < STATISTIC_NUM_SLOTS {
        let stakind_attnum = Anum_pg_statistic_stakind1 - 1 + slotidx;
        if first_empty < 0 && values[stakind_attnum].as_i16() == 0 {
            first_empty = slotidx as i32;
        }
        if values[stakind_attnum].as_i16() == stakind {
            break;
        }
        slotidx += 1;
    }

    if slotidx >= STATISTIC_NUM_SLOTS && first_empty >= 0 {
        slotidx = first_empty as usize;
    }

    if slotidx >= STATISTIC_NUM_SLOTS {
        // C: ereport(ERROR, ...). Reached only if all slots are full of other
        // kinds.
        return Err(PgError::error(format!(
            "maximum number of statistics slots exceeded: {}",
            slotidx + 1
        )));
    }

    let stakind_attnum = Anum_pg_statistic_stakind1 - 1 + slotidx;
    let staop_attnum = Anum_pg_statistic_staop1 - 1 + slotidx;
    let stacoll_attnum = Anum_pg_statistic_stacoll1 - 1 + slotidx;

    if values[stakind_attnum].as_i16() != stakind {
        values[stakind_attnum] = Datum::from_i16(stakind);
        replaces[stakind_attnum] = true;
    }
    if values[staop_attnum].as_oid() != staop {
        values[staop_attnum] = Datum::from_oid(staop);
        replaces[staop_attnum] = true;
    }
    if values[stacoll_attnum].as_oid() != stacoll {
        values[stacoll_attnum] = Datum::from_oid(stacoll);
        replaces[stacoll_attnum] = true;
    }
    if let Some(sn) = stanumbers {
        let idx = Anum_pg_statistic_stanumbers1 - 1 + slotidx;
        values[idx] = sn;
        nulls[idx] = false;
        replaces[idx] = true;
    }
    if let Some(sv) = stavalues {
        let idx = Anum_pg_statistic_stavalues1 - 1 + slotidx;
        values[idx] = sv;
        nulls[idx] = false;
        replaces[idx] = true;
    }
    Ok(())
}

/// `upsert_pg_statistic(starel, oldtup, values, nulls, replaces)`
/// (attribute_stats.c).
fn upsert_pg_statistic<'mcx>(
    mcx: Mcx<'mcx>,
    starel: &types_rel::Relation<'mcx>,
    oldtup: Option<&types_tuple::FormedTuple<'mcx>>,
    values: &[Datum<'mcx>],
    nulls: &[bool],
    replaces: &[bool],
) -> PgResult<()> {
    if let Some(oldtup) = oldtup {
        let mut newtup = heap_modify_tuple(mcx, oldtup, &starel.rd_att, values, nulls, replaces)
            .map_err(|_| PgError::error("heap_modify_tuple failed in upsert_pg_statistic"))?;
        let otid = newtup.tuple.t_self;
        CatalogTupleUpdate(mcx, starel, otid, &mut newtup)?;
    } else {
        let mut newtup = heap_form_tuple(mcx, &starel.rd_att, values, nulls)
            .map_err(|_| PgError::error("heap_form_tuple failed in upsert_pg_statistic"))?;
        CatalogTupleInsert(mcx, starel, &mut newtup)?;
    }

    CommandCounterIncrement()?;
    Ok(())
}

/// `delete_pg_statistic(reloid, attnum, stainherit)` (attribute_stats.c).
fn delete_pg_statistic(mcx: Mcx<'_>, reloid: Oid, attnum: AttrNumber, stainherit: bool) -> PgResult<bool> {
    let sd = table_open(mcx, StatisticRelationId, RowExclusiveLock)?;
    let mut result = false;

    let oldtup = syscache::SearchSysCache3(
        mcx,
        syscache::STATRELATTINH,
        SysCacheKey::Value(KeyDatum::from_oid(reloid)),
        SysCacheKey::Value(KeyDatum::from_i16(attnum as i16)),
        SysCacheKey::Value(KeyDatum::from_bool(stainherit)),
    )?;

    if let Some(oldtup) = oldtup {
        let tid = oldtup.tuple.t_self;
        CatalogTupleDelete(mcx, &sd, tid)?;
        ReleaseSysCache(oldtup);
        result = true;
    }

    table_close(sd, RowExclusiveLock)?;

    CommandCounterIncrement()?;

    Ok(result)
}

/// `init_empty_stats_tuple(reloid, attnum, inherited, ...)` (attribute_stats.c).
fn init_empty_stats_tuple<'mcx>(
    reloid: Oid,
    attnum: i16,
    inherited: bool,
    values: &mut [Datum<'mcx>],
    nulls: &mut [bool],
    replaces: &mut [bool],
) {
    for n in nulls.iter_mut() {
        *n = true;
    }
    for r in replaces.iter_mut() {
        *r = true;
    }

    // must initialize non-NULL attributes
    values[Anum_pg_statistic_starelid - 1] = Datum::from_oid(reloid);
    nulls[Anum_pg_statistic_starelid - 1] = false;
    values[Anum_pg_statistic_staattnum - 1] = Datum::from_i16(attnum);
    nulls[Anum_pg_statistic_staattnum - 1] = false;
    values[Anum_pg_statistic_stainherit - 1] = Datum::from_bool(inherited);
    nulls[Anum_pg_statistic_stainherit - 1] = false;

    values[Anum_pg_statistic_stanullfrac - 1] = default_null_frac();
    nulls[Anum_pg_statistic_stanullfrac - 1] = false;
    values[Anum_pg_statistic_stawidth - 1] = default_avg_width();
    nulls[Anum_pg_statistic_stawidth - 1] = false;
    values[Anum_pg_statistic_stadistinct - 1] = default_n_distinct();
    nulls[Anum_pg_statistic_stadistinct - 1] = false;

    // initialize stakind, staop, and stacoll slots
    for slotnum in 0..STATISTIC_NUM_SLOTS {
        let ik = Anum_pg_statistic_stakind1 + slotnum - 1;
        values[ik] = Datum::from_i16(0);
        nulls[ik] = false;
        let io = Anum_pg_statistic_staop1 + slotnum - 1;
        values[io] = Datum::from_oid(InvalidOid);
        nulls[io] = false;
        let ic = Anum_pg_statistic_stacoll1 + slotnum - 1;
        values[ic] = Datum::from_oid(InvalidOid);
        nulls[ic] = false;
    }
}

// ===========================================================================
// Shared helpers
// ===========================================================================

/// `RangeVarGetRelidExtended(makeRangeVar(nspname, relname, -1),
/// ShareUpdateExclusiveLock, 0, RangeVarCallbackForStats, &locked_table)`.
fn lookup_relation(mcx: Mcx<'_>, nspname: &str, relname: &str) -> PgResult<Oid> {
    let rv = make_range_var(Some(nspname.to_string()), relname.to_string(), -1);
    let locked = core::cell::Cell::new(InvalidOid);

    let mut callback = |rel: &RangeVar, rel_id: Oid, old_rel_id: Oid| -> PgResult<()> {
        let mut locked_oid = locked.get();
        let r = range_var_callback_for_stats(mcx, rel, rel_id, old_rel_id, &mut locked_oid);
        locked.set(locked_oid);
        r
    };

    let cb: RangeVarGetRelidCallback = Some(&mut callback);
    RangeVarGetRelidExtended(mcx, &rv, ShareUpdateExclusiveLock, 0, cb)
}

/// `TextDatumGetCString(d)` = `text_to_cstring(DatumGetTextPP(d))` — the text
/// payload as a UTF-8 `String`. The canonical by-reference `text` Datum is a
/// header-ful varlena image (both the genuine `text` array elements that
/// `extract_variadic_array`/`deconstruct_array` return AND the
/// `CStringGetTextDatum`-converted `unknown` literals), so its `VARHDRSZ`/short
/// header must be skipped (`VARDATA_ANY`). A `Cstring` carries the bytes as-is.
fn text_datum_to_string(d: &Datum) -> PgResult<String> {
    match d {
        Datum::Cstring(s) => Ok(s.clone()),
        _ => {
            let payload = backend_utils_adt_varlena::vardata_any_slice(d.as_ref_bytes());
            Ok(String::from_utf8_lossy(payload).into_owned())
        }
    }
}

/// `TextDatumGetCString(d)` for a DIRECT `text` fmgr argument (the
/// `text text` / `text text text bool` clear functions). Unlike the
/// variadic-"any" array elements — which `deconstruct_array` hands back already
/// header-stripped — a direct by-ref `text` argument arrives as a header-ful
/// varlena image, so its `VARHDRSZ`/short header must be skipped
/// (`VARDATA_ANY`). A `Cstring` payload is already the bytes themselves.
fn direct_text_datum_to_string(d: &Datum) -> PgResult<String> {
    match d {
        Datum::Cstring(s) => Ok(s.clone()),
        _ => {
            let image = d.as_ref_bytes();
            let payload = backend_utils_adt_varlena::vardata_any_slice(image);
            Ok(String::from_utf8_lossy(payload).into_owned())
        }
    }
}

/// Normalize a direct (header-ful) `text` argument into the canonical
/// header-ful `Datum::ByRef` `text` representation that `text_datum_to_string`
/// consumes (it strips the header via `VARDATA_ANY`). A header-ful varlena
/// passes through verbatim; a `Cstring` is promoted to a real `text` image via
/// `CStringGetTextDatum`.
fn strip_direct_text<'mcx>(mcx: Mcx<'mcx>, d: &Datum<'mcx>) -> PgResult<Datum<'mcx>> {
    match d {
        Datum::Cstring(s) => backend_utils_adt_varlena_seams::cstring_to_text_v::call(mcx, s),
        _ => Ok(Datum::ByRef(mcx::slice_in(mcx, d.as_ref_bytes())?)),
    }
}

// ===========================================================================
// fmgr builtins (relation_stats.c / attribute_stats.c entry points)
// ===========================================================================

/// `pg_restore_relation_stats(PG_FUNCTION_ARGS)` (relation_stats.c) — oid 6362.
fn fc_pg_restore_relation_stats(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<types_datum::Datum> {
    let m = scratch_mcx();
    let mcx = m.mcx();

    let mut result = true;
    let mut positional = PositionalArgs::new(NUM_RELATION_STATS_ARGS);
    let arginfo = relarginfo();

    if !stats_fill_fcinfo_from_arg_pairs(mcx, fcinfo, &mut positional, &arginfo)? {
        result = false;
    }
    if !relation_statistics_update(mcx, &positional)? {
        result = false;
    }

    Ok(types_datum::Datum::from_bool(result))
}

/// `pg_restore_attribute_stats(PG_FUNCTION_ARGS)` (attribute_stats.c) — oid 6363.
fn fc_pg_restore_attribute_stats(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<types_datum::Datum> {
    let m = scratch_mcx();
    let mcx = m.mcx();

    let mut result = true;
    let mut positional = PositionalArgs::new(NUM_ATTRIBUTE_STATS_ARGS);
    let arginfo = attarginfo();

    if !stats_fill_fcinfo_from_arg_pairs(mcx, fcinfo, &mut positional, &arginfo)? {
        result = false;
    }
    if !attribute_statistics_update(mcx, &positional)? {
        result = false;
    }

    Ok(types_datum::Datum::from_bool(result))
}

/// `pg_clear_relation_stats(PG_FUNCTION_ARGS)` (relation_stats.c) — oid 6397.
fn fc_pg_clear_relation_stats(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<types_datum::Datum> {
    let m = scratch_mcx();
    let mcx = m.mcx();

    // newfcinfo: positional update with the cleared defaults. The direct `text`
    // schemaname/relname arrive header-ful; re-wrap them header-stripped so the
    // shared `relation_statistics_update`/`text_datum_to_string` decode (which
    // expects the variadic, already-stripped representation) reads them right.
    let mut positional = PositionalArgs::new(NUM_RELATION_STATS_ARGS);
    positional.args[0] = Some(strip_direct_text(mcx, &arg0_datum(mcx, fcinfo, 0)?)?);
    positional.args[1] = Some(strip_direct_text(mcx, &arg0_datum(mcx, fcinfo, 1)?)?);
    positional.args[RELPAGES_ARG] = Some(Datum::from_u32(0));
    positional.args[RELTUPLES_ARG] = Some(Datum::from_f32(-1.0));
    positional.args[RELALLVISIBLE_ARG] = Some(Datum::from_u32(0));
    positional.args[RELALLFROZEN_ARG] = Some(Datum::from_u32(0));

    relation_statistics_update(mcx, &positional)?;
    Ok(types_datum::Datum::null())
}

/// `pg_clear_attribute_stats(PG_FUNCTION_ARGS)` (attribute_stats.c) — oid 6398.
fn fc_pg_clear_attribute_stats(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<types_datum::Datum> {
    let m = scratch_mcx();
    let mcx = m.mcx();

    let cleararginfo = [
        StatsArgInfo { argname: "relation", argtype: TEXTOID },
        StatsArgInfo { argname: "relation", argtype: TEXTOID },
        StatsArgInfo { argname: "attname", argtype: TEXTOID },
        StatsArgInfo { argname: "inherited", argtype: BOOLOID },
    ];

    // Required args (schemaname, relname, attname, inherited).
    let mut clear = PositionalArgs::new(4);
    for i in 0..4 {
        clear.args[i] = arg_opt_datum(mcx, fcinfo, i)?;
    }

    const C_ATTRELSCHEMA_ARG: usize = 0;
    const C_ATTRELNAME_ARG: usize = 1;
    const C_ATTNAME_ARG: usize = 2;
    const C_INHERITED_ARG: usize = 3;

    stats_check_required_arg(&clear, &cleararginfo, C_ATTRELSCHEMA_ARG)?;
    stats_check_required_arg(&clear, &cleararginfo, C_ATTRELNAME_ARG)?;
    stats_check_required_arg(&clear, &cleararginfo, C_ATTNAME_ARG)?;
    stats_check_required_arg(&clear, &cleararginfo, C_INHERITED_ARG)?;

    let nspname = direct_text_datum_to_string(clear.datum(C_ATTRELSCHEMA_ARG))?;
    let relname = direct_text_datum_to_string(clear.datum(C_ATTRELNAME_ARG))?;

    if backend_access_transam_xlog::RecoveryInProgress() {
        return Err(PgError::error("recovery is in progress")
            .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .with_hint("Statistics cannot be modified during recovery."));
    }

    let reloid = lookup_relation(mcx, &nspname, &relname)?;

    let attname = direct_text_datum_to_string(clear.datum(C_ATTNAME_ARG))?;
    let attnum = get_attnum(reloid, &attname)?;

    if attnum < 0 {
        return Err(PgError::error(format!(
            "cannot clear statistics on system column \"{attname}\""
        ))
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
    }

    if attnum == InvalidAttrNumber {
        let relnm = get_rel_name(mcx, reloid)?
            .map(|s| s.as_str().to_string())
            .unwrap_or_default();
        return Err(PgError::error(format!(
            "column \"{attname}\" of relation \"{relnm}\" does not exist"
        ))
        .with_sqlstate(ERRCODE_UNDEFINED_COLUMN));
    }

    let inherited = clear.datum(C_INHERITED_ARG).as_bool();

    delete_pg_statistic(mcx, reloid, attnum, inherited)?;
    Ok(types_datum::Datum::null())
}

/// Materialize a (non-strict, possibly-null) positional argument as `Option`.
fn arg_opt_datum<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &FunctionCallInfoBaseData,
    i: usize,
) -> PgResult<Option<Datum<'mcx>>> {
    if fcinfo.arg(i).map(|d| d.isnull).unwrap_or(true) {
        Ok(None)
    } else {
        Ok(Some(arg_value(mcx, fcinfo, i)?))
    }
}

/// Materialize positional argument `i` whether null or not (the clear-relation
/// path passes args 0/1 straight through, including NULL).
fn arg0_datum<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &FunctionCallInfoBaseData,
    i: usize,
) -> PgResult<Datum<'mcx>> {
    // C copies PG_GETARG_DATUM(i)/PG_ARGISNULL(i); a NULL becomes None in our
    // positional map.  Here args 0/1 of pg_clear_relation_stats are the
    // schema/relname which the caller is expected to provide.
    arg_value(mcx, fcinfo, i)
}

fn scratch_mcx() -> MemoryContext {
    MemoryContext::new("stats import fmgr scratch")
}

// ===========================================================================
// Registration
// ===========================================================================

fn builtin(
    foid: u32,
    name: &str,
    nargs: i16,
    strict: bool,
    retset: bool,
    native: PgFnNative,
) -> (BuiltinFunction, PgFnNative) {
    (
        BuiltinFunction { foid, name: name.to_string(), nargs, strict, retset, func: None },
        native,
    )
}

/// Register the four stats import/restore builtins (C: their `fmgr_builtins[]`
/// rows). Called from this crate's `init_seams()`.
pub fn init_seams() {
    backend_utils_fmgr_core::register_builtins_native([
        builtin(6362, "pg_restore_relation_stats", 1, false, false, fc_pg_restore_relation_stats),
        builtin(6363, "pg_restore_attribute_stats", 1, false, false, fc_pg_restore_attribute_stats),
        builtin(6397, "pg_clear_relation_stats", 2, false, false, fc_pg_clear_relation_stats),
        builtin(6398, "pg_clear_attribute_stats", 4, false, false, fc_pg_clear_attribute_stats),
    ]);
}
