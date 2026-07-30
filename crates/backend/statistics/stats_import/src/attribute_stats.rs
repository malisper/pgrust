use crate::{
    lookup_relation, recovery_in_progress_error, stats_check_arg_array, stats_check_arg_pair,
    stats_check_required_arg, stats_fill_fcinfo_from_arg_pairs, text_datum_string, warn,
    warn_error_data, Arg, StatsArgInfo, RELKIND_INDEX, RELKIND_PARTITIONED_INDEX,
};
use datum::Datum;
use mcx::{Mcx, MemoryContext, PgVec};
use types_core::fmgr::{F_BOOLEQ, F_INT2EQ, F_OIDEQ};
use types_core::{
    AttrNumber, InvalidOid, Oid, BOOLOID, DEFAULT_COLLATION_OID, FLOAT4OID, FLOAT8OID, INT2OID,
    INT4OID, TEXTOID,
};
use types_error::{
    PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_UNDEFINED_COLUMN,
};
use types_fmgr::{ErrorSaveNode, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};
use types_nodes::Node;
use types_rel::lock::{AccessShareLock, NoLock, RowExclusiveLock, LOCKMODE};
use types_rel::Relation;
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};

// pg_type.dat: _float4 / tsvector; pg_operator.dat: float8lt.
const FLOAT4ARRAYOID: Oid = 1021;
const TSVECTOROID: Oid = 3614;
const Float8LessOperator: Oid = 672;

const TYPTYPE_RANGE: i8 = b'r' as i8;
const TYPTYPE_MULTIRANGE: i8 = b'm' as i8;

const STATISTIC_RELATION_ID: Oid = 2619;
const STATISTIC_NUM_SLOTS: usize = 5;

const STATISTIC_KIND_MCV: i16 = 1;
const STATISTIC_KIND_HISTOGRAM: i16 = 2;
const STATISTIC_KIND_CORRELATION: i16 = 3;
const STATISTIC_KIND_MCELEM: i16 = 4;
const STATISTIC_KIND_DECHIST: i16 = 5;
const STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM: i16 = 6;
const STATISTIC_KIND_BOUNDS_HISTOGRAM: i16 = 7;

const Natts_pg_statistic: usize = 31;
const Anum_pg_statistic_starelid: usize = 1;
const Anum_pg_statistic_staattnum: usize = 2;
const Anum_pg_statistic_stainherit: usize = 3;
const Anum_pg_statistic_stanullfrac: usize = 4;
const Anum_pg_statistic_stawidth: usize = 5;
const Anum_pg_statistic_stadistinct: usize = 6;
const Anum_pg_statistic_stakind1: usize = 7;
const Anum_pg_statistic_staop1: usize = 12;
const Anum_pg_statistic_stacoll1: usize = 17;
const Anum_pg_statistic_stanumbers1: usize = 22;
const Anum_pg_statistic_stavalues1: usize = 27;

const InvalidAttrNumber: AttrNumber = 0;

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

static ATTARGINFO: [StatsArgInfo; NUM_ATTRIBUTE_STATS_ARGS] = [
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
];

const C_ATTRELSCHEMA_ARG: usize = 0;
const C_ATTRELNAME_ARG: usize = 1;
const C_ATTNAME_ARG: usize = 2;
const C_INHERITED_ARG: usize = 3;
const C_NUM_ATTRIBUTE_STATS_ARGS: usize = 4;

static CLEARARGINFO: [StatsArgInfo; C_NUM_ATTRIBUTE_STATS_ARGS] = [
    StatsArgInfo { argname: "relation", argtype: TEXTOID },
    StatsArgInfo { argname: "relation", argtype: TEXTOID },
    StatsArgInfo { argname: "attname", argtype: TEXTOID },
    StatsArgInfo { argname: "inherited", argtype: BOOLOID },
];

#[track_caller]
#[cold]
fn column_name_missing(attname: &str, relname: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "column \"{attname}\" of relation \"{relname}\" does not exist"
        ))
        .with_sqlstate(ERRCODE_UNDEFINED_COLUMN),
    )
}

#[track_caller]
#[cold]
fn column_num_missing(attnum: AttrNumber, relname: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "column {attnum} of relation \"{relname}\" does not exist"
        ))
        .with_sqlstate(ERRCODE_UNDEFINED_COLUMN),
    )
}

fn attribute_statistics_update(mcx: Mcx<'_>, args: &[Arg]) -> PgResult<bool> {
    let mut result = true;

    let mut do_mcv = !args[MOST_COMMON_FREQS_ARG].isnull && !args[MOST_COMMON_VALS_ARG].isnull;
    let mut do_histogram = !args[HISTOGRAM_BOUNDS_ARG].isnull;
    let mut do_correlation = !args[CORRELATION_ARG].isnull;
    let mut do_mcelem =
        !args[MOST_COMMON_ELEMS_ARG].isnull && !args[MOST_COMMON_ELEM_FREQS_ARG].isnull;
    let mut do_dechist = !args[ELEM_COUNT_HISTOGRAM_ARG].isnull;
    let mut do_bounds_histogram = !args[RANGE_BOUNDS_HISTOGRAM_ARG].isnull;
    let mut do_range_length_histogram =
        !args[RANGE_LENGTH_HISTOGRAM_ARG].isnull && !args[RANGE_EMPTY_FRAC_ARG].isnull;

    stats_check_required_arg(args, &ATTARGINFO, ATTRELSCHEMA_ARG)?;
    stats_check_required_arg(args, &ATTARGINFO, ATTRELNAME_ARG)?;

    let nspname = text_datum_string(mcx, args[ATTRELSCHEMA_ARG].value)?;
    let relname = text_datum_string(mcx, args[ATTRELNAME_ARG].value)?;

    if transam_xlog::RecoveryInProgress() {
        return Err(recovery_in_progress_error());
    }

    // lock before looking up attribute
    let reloid = lookup_relation(mcx, &nspname, &relname)?;

    // user can specify either attname or attnum, but not both
    let (attname, attnum): (String, AttrNumber) = if !args[ATTNAME_ARG].isnull {
        if !args[ATTNUM_ARG].isnull {
            return Err(Box::new(
                PgError::error("cannot specify both \"attname\" and \"attnum\"")
                    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
            ));
        }
        let attname = text_datum_string(mcx, args[ATTNAME_ARG].value)?;
        let attnum = lsyscache::get_attnum(reloid, &attname)?;
        if attnum == InvalidAttrNumber {
            return Err(column_name_missing(&attname, &relname));
        }
        (attname, attnum)
    } else if !args[ATTNUM_ARG].isnull {
        let attnum = args[ATTNUM_ARG].value.as_i16();
        let attname_opt = lsyscache::get_attname(mcx, reloid, attnum, true)?;
        let attname = match &attname_opt {
            Some(s) => s.as_str().to_string(),
            None => String::new(),
        };
        // get_attname doesn't check attisdropped
        let exists = if attname_opt.is_some() {
            match cache_syscache::SearchSysCacheAttName(reloid, &attname)? {
                Some(t) => {
                    cache_syscache::ReleaseSysCache(t);
                    true
                }
                None => false,
            }
        } else {
            false
        };
        if !exists {
            return Err(column_num_missing(attnum, &relname));
        }
        (attname, attnum)
    } else {
        return Err(Box::new(
            PgError::error("must specify either \"attname\" or \"attnum\"")
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        ));
    };

    if attnum < 0 {
        return Err(Box::new(
            PgError::error(format!(
                "cannot modify statistics on system column \"{attname}\""
            ))
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }

    stats_check_required_arg(args, &ATTARGINFO, INHERITED_ARG)?;
    let inherited = args[INHERITED_ARG].value.as_bool();

    if !stats_check_arg_array(mcx, args, &ATTARGINFO, MOST_COMMON_FREQS_ARG)? {
        do_mcv = false;
        result = false;
    }
    if !stats_check_arg_array(mcx, args, &ATTARGINFO, MOST_COMMON_ELEM_FREQS_ARG)? {
        do_mcelem = false;
        result = false;
    }
    if !stats_check_arg_array(mcx, args, &ATTARGINFO, ELEM_COUNT_HISTOGRAM_ARG)? {
        do_dechist = false;
        result = false;
    }
    if !stats_check_arg_pair(args, &ATTARGINFO, MOST_COMMON_VALS_ARG, MOST_COMMON_FREQS_ARG)? {
        do_mcv = false;
        result = false;
    }
    if !stats_check_arg_pair(
        args,
        &ATTARGINFO,
        MOST_COMMON_ELEMS_ARG,
        MOST_COMMON_ELEM_FREQS_ARG,
    )? {
        do_mcelem = false;
        result = false;
    }
    if !stats_check_arg_pair(
        args,
        &ATTARGINFO,
        RANGE_LENGTH_HISTOGRAM_ARG,
        RANGE_EMPTY_FRAC_ARG,
    )? {
        do_range_length_histogram = false;
        result = false;
    }

    let StatType { atttypid, atttypmod, atttyptype, mut atttypcoll, eq_opr, lt_opr } =
        get_attr_stat_type(mcx, reloid, attnum)?;
    let _ = &mut atttypcoll;

    let mut elemtypid = InvalidOid;
    let mut elem_eq_opr = InvalidOid;
    if do_mcelem || do_dechist {
        match get_elem_stat_type(atttypid)? {
            Some((etid, eeo)) => {
                elemtypid = etid;
                elem_eq_opr = eeo;
            }
            None => {
                warn(
                    "attribute_statistics_update",
                    format!("could not determine element type of column \"{attname}\""),
                    None,
                    Some(
                        "Cannot set STATISTIC_KIND_MCELEM or STATISTIC_KIND_DECHIST.".to_string(),
                    ),
                    None,
                )?;
                do_mcelem = false;
                do_dechist = false;
                result = false;
            }
        }
    }

    if (do_histogram || do_correlation) && lt_opr == InvalidOid {
        warn(
            "attribute_statistics_update",
            format!("could not determine less-than operator for column \"{attname}\""),
            Some(ERRCODE_INVALID_PARAMETER_VALUE),
            Some("Cannot set STATISTIC_KIND_HISTOGRAM or STATISTIC_KIND_CORRELATION.".to_string()),
            None,
        )?;
        do_histogram = false;
        do_correlation = false;
        result = false;
    }

    if (do_range_length_histogram || do_bounds_histogram)
        && !(atttyptype == TYPTYPE_RANGE || atttyptype == TYPTYPE_MULTIRANGE)
    {
        warn(
            "attribute_statistics_update",
            format!("column \"{attname}\" is not a range type"),
            Some(ERRCODE_INVALID_PARAMETER_VALUE),
            Some(
                "Cannot set STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM or STATISTIC_KIND_BOUNDS_HISTOGRAM."
                    .to_string(),
            ),
            None,
        )?;
        do_bounds_histogram = false;
        do_range_length_histogram = false;
        result = false;
    }

    let starel = table::table_open(mcx, STATISTIC_RELATION_ID, RowExclusiveLock as LOCKMODE)?;

    let statup = find_stats_tuple(mcx, &starel, reloid, attnum, inherited)?;

    let mut values: [Datum; Natts_pg_statistic] = [Datum::null(); Natts_pg_statistic];
    let mut nulls: [bool; Natts_pg_statistic] = [false; Natts_pg_statistic];
    let mut replaces: [bool; Natts_pg_statistic] = [false; Natts_pg_statistic];

    match &statup {
        Some((_, oldtup)) => {
            let desc = starel.descr();
            for i in 0..Natts_pg_statistic {
                let mut isnull = false;
                // SAFETY: pg_statistic tuple read under pg_statistic's descriptor.
                let d = unsafe {
                    types_tuple::heap_getattr(oldtup, (i + 1) as i32, desc, &mut isnull)
                };
                values[i] = d;
                nulls[i] = isnull;
            }
        }
        None => init_empty_stats_tuple(reloid, attnum, inherited, &mut values, &mut nulls, &mut replaces),
    }

    if !args[NULL_FRAC_ARG].isnull {
        values[Anum_pg_statistic_stanullfrac - 1] = args[NULL_FRAC_ARG].value;
        replaces[Anum_pg_statistic_stanullfrac - 1] = true;
    }
    if !args[AVG_WIDTH_ARG].isnull {
        values[Anum_pg_statistic_stawidth - 1] = args[AVG_WIDTH_ARG].value;
        replaces[Anum_pg_statistic_stawidth - 1] = true;
    }
    if !args[N_DISTINCT_ARG].isnull {
        values[Anum_pg_statistic_stadistinct - 1] = args[N_DISTINCT_ARG].value;
        replaces[Anum_pg_statistic_stadistinct - 1] = true;
    }

    // The converted array images must stay live until the tuple is formed.
    let mut images: Vec<PgVec<'_, u8>> = Vec::new();

    if do_mcv {
        let stanumbers = args[MOST_COMMON_FREQS_ARG].value;
        match text_to_stavalues(
            mcx,
            "most_common_vals",
            args[MOST_COMMON_VALS_ARG].value,
            atttypid,
            atttypmod,
        )? {
            Some(img) => {
                let stavalues = Datum::from_usize(img.as_ptr() as usize);
                images.push(img);
                set_stats_slot(
                    &mut values,
                    &mut nulls,
                    &mut replaces,
                    STATISTIC_KIND_MCV,
                    eq_opr,
                    atttypcoll,
                    Some(stanumbers),
                    Some(stavalues),
                )?;
            }
            None => result = false,
        }
    }

    if do_histogram {
        match text_to_stavalues(
            mcx,
            "histogram_bounds",
            args[HISTOGRAM_BOUNDS_ARG].value,
            atttypid,
            atttypmod,
        )? {
            Some(img) => {
                let stavalues = Datum::from_usize(img.as_ptr() as usize);
                images.push(img);
                set_stats_slot(
                    &mut values,
                    &mut nulls,
                    &mut replaces,
                    STATISTIC_KIND_HISTOGRAM,
                    lt_opr,
                    atttypcoll,
                    None,
                    Some(stavalues),
                )?;
            }
            None => result = false,
        }
    }

    if do_correlation {
        let elems = [args[CORRELATION_ARG].value];
        let img = datum::array_build::construct_array_image(mcx, &elems, FLOAT4OID, 4, true, b'i')?;
        let stanumbers = Datum::from_usize(img.as_ptr() as usize);
        images.push(img);
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

    if do_mcelem {
        let stanumbers = args[MOST_COMMON_ELEM_FREQS_ARG].value;
        match text_to_stavalues(
            mcx,
            "most_common_elems",
            args[MOST_COMMON_ELEMS_ARG].value,
            elemtypid,
            atttypmod,
        )? {
            Some(img) => {
                let stavalues = Datum::from_usize(img.as_ptr() as usize);
                images.push(img);
                set_stats_slot(
                    &mut values,
                    &mut nulls,
                    &mut replaces,
                    STATISTIC_KIND_MCELEM,
                    elem_eq_opr,
                    atttypcoll,
                    Some(stanumbers),
                    Some(stavalues),
                )?;
            }
            None => result = false,
        }
    }

    if do_dechist {
        let stanumbers = args[ELEM_COUNT_HISTOGRAM_ARG].value;
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

    // BOUNDS_HISTOGRAM appears before RANGE_LENGTH_HISTOGRAM even though it is
    // numerically greater (C quirk, preserved).
    if do_bounds_histogram {
        match text_to_stavalues(
            mcx,
            "range_bounds_histogram",
            args[RANGE_BOUNDS_HISTOGRAM_ARG].value,
            atttypid,
            atttypmod,
        )? {
            Some(img) => {
                let stavalues = Datum::from_usize(img.as_ptr() as usize);
                images.push(img);
                set_stats_slot(
                    &mut values,
                    &mut nulls,
                    &mut replaces,
                    STATISTIC_KIND_BOUNDS_HISTOGRAM,
                    InvalidOid,
                    InvalidOid,
                    None,
                    Some(stavalues),
                )?;
            }
            None => result = false,
        }
    }

    if do_range_length_histogram {
        let elems = [args[RANGE_EMPTY_FRAC_ARG].value];
        let img = datum::array_build::construct_array_image(mcx, &elems, FLOAT4OID, 4, true, b'i')?;
        let stanumbers = Datum::from_usize(img.as_ptr() as usize);
        images.push(img);

        match text_to_stavalues(
            mcx,
            "range_length_histogram",
            args[RANGE_LENGTH_HISTOGRAM_ARG].value,
            FLOAT8OID,
            0,
        )? {
            Some(img) => {
                let stavalues = Datum::from_usize(img.as_ptr() as usize);
                images.push(img);
                set_stats_slot(
                    &mut values,
                    &mut nulls,
                    &mut replaces,
                    STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM,
                    Float8LessOperator,
                    InvalidOid,
                    Some(stanumbers),
                    Some(stavalues),
                )?;
            }
            None => result = false,
        }
    }

    upsert_pg_statistic(mcx, &starel, &statup, &values, &nulls, &replaces)?;
    drop(images);

    table::table_close(starel, RowExclusiveLock as LOCKMODE)?;

    Ok(result)
}

struct StatType {
    atttypid: Oid,
    atttypmod: i32,
    atttyptype: i8,
    atttypcoll: Oid,
    eq_opr: Oid,
    lt_opr: Oid,
}

fn get_attr_expr<'m>(mcx: Mcx<'m>, rel: &Relation<'m>, attnum: i32) -> PgResult<Option<Node<'m>>> {
    if rel.rd_rel.relkind != RELKIND_INDEX && rel.rd_rel.relkind != RELKIND_PARTITIONED_INDEX {
        return Ok(None);
    }

    let index_exprs = execindexing::RelationGetIndexExpressions(mcx, rel)?;
    if index_exprs.len() == 0 {
        return Ok(None);
    }

    let rd_index = rel.rd_index.as_ref().expect("index relation has rd_index");
    if rd_index.indkey[(attnum - 1) as usize] != 0 {
        return Ok(None);
    }

    let mut indexpr_item = 0usize;
    for i in 0..(attnum - 1) as usize {
        if rd_index.indkey[i] == 0 {
            indexpr_item += 1;
        }
    }

    match index_exprs.iter().nth(indexpr_item) {
        Some(e) => Ok(Some(e)),
        None => Err(Box::new(PgError::error("too few entries in indexprs list"))),
    }
}

fn get_attr_stat_type(mcx: Mcx<'_>, reloid: Oid, attnum: AttrNumber) -> PgResult<StatType> {
    let rel = relation_seams::relation_open::call(mcx, reloid, AccessShareLock as LOCKMODE)?;

    // lookup_pg_attribute_shape folds dropped columns into None, matching the
    // identical C error for missing and dropped.
    let attr = syscache_seams::lookup_pg_attribute_shape::call(reloid, attnum)?
        .ok_or_else(|| column_num_missing(attnum, rel.name()))?;

    let expr = get_attr_expr(mcx, &rel, attnum as i32)?;

    let (mut atttypid, atttypmod, mut atttypcoll) = match expr {
        None => (attr.atttypid, attr.atttypmod, attr.attcollation),
        Some(e) => {
            let coll = if attr.attcollation != InvalidOid {
                attr.attcollation
            } else {
                nodes_core::node_funcs::expr_collation(e)
            };
            (
                nodes_core::node_funcs::expr_type(e),
                nodes_core::node_funcs::expr_typmod(e),
                coll,
            )
        }
    };

    // A multirange steps down to its range type, as multirange_typanalyze does.
    if lsyscache::type_is_multirange(atttypid)? {
        atttypid = lsyscache::get_multirange_range(atttypid)?;
    }

    // finds the right operators even if atttypid is a domain
    let tce = typcache::lookup_type_cache(
        atttypid,
        typcache::TYPECACHE_LT_OPR | typcache::TYPECACHE_EQ_OPR,
    )?;
    let atttyptype = tce.typtype();
    let eq_opr = tce.eq_opr();
    let lt_opr = tce.lt_opr();

    // Collation for tsvector is DEFAULT_COLLATION_OID (compute_tsvector_stats).
    if atttypid == TSVECTOROID {
        atttypcoll = DEFAULT_COLLATION_OID;
    }

    rel.close(NoLock as LOCKMODE)?;

    Ok(StatType { atttypid, atttypmod, atttyptype, atttypcoll, eq_opr, lt_opr })
}

fn get_elem_stat_type(atttypid: Oid) -> PgResult<Option<(Oid, Oid)>> {
    let elemtypid = if atttypid == TSVECTOROID {
        // element type for tsvector is text (compute_tsvector_stats)
        TEXTOID
    } else {
        lsyscache::get_base_element_type(atttypid)?
    };

    if elemtypid == InvalidOid {
        return Ok(None);
    }

    let tce = typcache::lookup_type_cache(elemtypid, typcache::TYPECACHE_EQ_OPR)?;
    let elem_eq_opr = tce.eq_opr();
    if elem_eq_opr == InvalidOid {
        return Ok(None);
    }

    Ok(Some((elemtypid, elem_eq_opr)))
}

fn text_to_stavalues<'m>(
    mcx: Mcx<'m>,
    staname: &str,
    d: Datum,
    typid: Oid,
    typmod: i32,
) -> PgResult<Option<PgVec<'m, u8>>> {
    let s = text_datum_string(mcx, d)?;

    let io = lsyscache::get_type_io_data(typid, lsyscache::IOFuncSelector::IOFunc_input)?;
    let mut proc = fmgr_seams::fmgr_info::call(io.func)?;
    let meta = arrayfuncs::io::ArrayIoMeta {
        element_type: typid,
        typlen: io.typlen as i32,
        typbyval: io.typbyval,
        typalign: io.typalign as u8,
        typdelim: io.typdelim as u8,
        typioparam: io.typioparam,
    };

    let mut esn = ErrorSaveNode::new(true);
    let arr = arrayfuncs::io::array_in(mcx, &s, &meta, &mut proc, typmod, Some(&mut esn))?;

    if esn.ctx.error_occurred() {
        if let Some(err) = esn.ctx.take_error() {
            warn_error_data("text_to_stavalues", err)?;
        }
        return Ok(None);
    }

    let Some(img) = arr else { return Ok(None) };

    if arrayfuncs::array_contains_nulls(&img) {
        warn(
            "text_to_stavalues",
            format!("\"{staname}\" array must not contain null values"),
            Some(ERRCODE_INVALID_PARAMETER_VALUE),
            None,
            None,
        )?;
        return Ok(None);
    }

    Ok(Some(img))
}

#[allow(clippy::too_many_arguments)]
fn set_stats_slot(
    values: &mut [Datum],
    nulls: &mut [bool],
    replaces: &mut [bool],
    stakind: i16,
    staop: Oid,
    stacoll: Oid,
    stanumbers: Option<Datum>,
    stavalues: Option<Datum>,
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
        return Err(Box::new(PgError::error(format!(
            "maximum number of statistics slots exceeded: {}",
            slotidx + 1
        ))));
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

type FoundStats<'m> = (types_tuple::ItemPointerData, heaptuple::HeapTuple<'m>);

fn find_stats_tuple<'m>(
    mcx: Mcx<'m>,
    sd: &Relation<'m>,
    relid: Oid,
    attnum: AttrNumber,
    inh: bool,
) -> PgResult<Option<FoundStats<'m>>> {
    let keys = [
        stat_key(Anum_pg_statistic_starelid as i32, F_OIDEQ, Datum::from_oid(relid)),
        stat_key(Anum_pg_statistic_staattnum as i32, F_INT2EQ, Datum::from_i16(attnum)),
        stat_key(Anum_pg_statistic_stainherit as i32, F_BOOLEQ, Datum::from_bool(inh)),
    ];
    let mut scan = genam::systable_beginscan(mcx, sd, InvalidOid, false, None, &keys)?;
    let found = match genam::systable_getnext(mcx, &mut scan)? {
        Some(tup) => Some((tup.t_self, heaptuple::heap_copytuple(mcx, tup)?)),
        None => None,
    };
    genam::systable_endscan(mcx, scan)?;
    Ok(found)
}

fn stat_key(attno: i32, func: types_core::primitive::RegProcedure, arg: Datum) -> ScanKeyData {
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno as i16;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(func)
        .unwrap_or_else(|e| panic!("fmgr_info({func}) failed: {e:?}"));
    key.sk_argument = arg;
    key
}

fn upsert_pg_statistic<'m>(
    mcx: Mcx<'m>,
    starel: &Relation<'m>,
    oldtup: &Option<FoundStats<'m>>,
    values: &[Datum],
    nulls: &[bool],
    replaces: &[bool],
) -> PgResult<()> {
    match oldtup {
        Some((otid, old)) => {
            let mut newtup =
                heaptuple::heap_modify_tuple(mcx, old, starel.descr(), values, nulls, replaces)?;
            catalog_indexing::CatalogTupleUpdate(mcx, starel, otid, &mut newtup)?;
        }
        None => {
            let mut newtup = heaptuple::heap_form_tuple(mcx, starel.descr(), values, nulls)?;
            catalog_indexing::CatalogTupleInsert(mcx, starel, &mut newtup)?;
        }
    }

    xact::CommandCounterIncrement()?;
    Ok(())
}

fn delete_pg_statistic(mcx: Mcx<'_>, reloid: Oid, attnum: AttrNumber, stainherit: bool) -> PgResult<bool> {
    let sd = table::table_open(mcx, STATISTIC_RELATION_ID, RowExclusiveLock as LOCKMODE)?;
    let mut result = false;

    if let Some((tid, _)) = find_stats_tuple(mcx, &sd, reloid, attnum, stainherit)? {
        catalog_indexing::CatalogTupleDelete(&sd, &tid)?;
        result = true;
    }

    table::table_close(sd, RowExclusiveLock as LOCKMODE)?;

    xact::CommandCounterIncrement()?;

    Ok(result)
}

fn init_empty_stats_tuple(
    reloid: Oid,
    attnum: AttrNumber,
    inherited: bool,
    values: &mut [Datum],
    nulls: &mut [bool],
    replaces: &mut [bool],
) {
    for n in nulls.iter_mut() {
        *n = true;
    }
    for r in replaces.iter_mut() {
        *r = true;
    }

    values[Anum_pg_statistic_starelid - 1] = Datum::from_oid(reloid);
    nulls[Anum_pg_statistic_starelid - 1] = false;
    values[Anum_pg_statistic_staattnum - 1] = Datum::from_i16(attnum);
    nulls[Anum_pg_statistic_staattnum - 1] = false;
    values[Anum_pg_statistic_stainherit - 1] = Datum::from_bool(inherited);
    nulls[Anum_pg_statistic_stainherit - 1] = false;

    values[Anum_pg_statistic_stanullfrac - 1] = Datum::from_f32(0.0);
    nulls[Anum_pg_statistic_stanullfrac - 1] = false;
    values[Anum_pg_statistic_stawidth - 1] = Datum::from_i32(0);
    nulls[Anum_pg_statistic_stawidth - 1] = false;
    values[Anum_pg_statistic_stadistinct - 1] = Datum::from_f32(0.0);
    nulls[Anum_pg_statistic_stadistinct - 1] = false;

    for slotnum in 0..STATISTIC_NUM_SLOTS {
        values[Anum_pg_statistic_stakind1 + slotnum - 1] = Datum::from_i16(0);
        nulls[Anum_pg_statistic_stakind1 + slotnum - 1] = false;
        values[Anum_pg_statistic_staop1 + slotnum - 1] = Datum::from_oid(InvalidOid);
        nulls[Anum_pg_statistic_staop1 + slotnum - 1] = false;
        values[Anum_pg_statistic_stacoll1 + slotnum - 1] = Datum::from_oid(InvalidOid);
        nulls[Anum_pg_statistic_stacoll1 + slotnum - 1] = false;
    }
}

pub fn fc_pg_restore_attribute_stats(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let cx = MemoryContext::new("pg_restore_attribute_stats");
    let mcx = cx.mcx();

    let mut result = true;
    let mut positional = [Arg::NULL; NUM_ATTRIBUTE_STATS_ARGS];

    if !stats_fill_fcinfo_from_arg_pairs(
        mcx,
        flinfo.as_deref(),
        fcinfo,
        &ATTARGINFO,
        &mut positional,
    )? {
        result = false;
    }
    if !attribute_statistics_update(mcx, &positional)? {
        result = false;
    }

    Ok(Datum::from_bool(result))
}

pub fn fc_pg_clear_attribute_stats(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let cx = MemoryContext::new("pg_clear_attribute_stats");
    let mcx = cx.mcx();

    let mut clear = [Arg::NULL; C_NUM_ATTRIBUTE_STATS_ARGS];
    for (i, slot) in clear.iter_mut().enumerate() {
        *slot = Arg { value: fcinfo.arg(i), isnull: fcinfo.argisnull(i) };
    }

    stats_check_required_arg(&clear, &CLEARARGINFO, C_ATTRELSCHEMA_ARG)?;
    stats_check_required_arg(&clear, &CLEARARGINFO, C_ATTRELNAME_ARG)?;
    stats_check_required_arg(&clear, &CLEARARGINFO, C_ATTNAME_ARG)?;
    stats_check_required_arg(&clear, &CLEARARGINFO, C_INHERITED_ARG)?;

    let nspname = text_datum_string(mcx, clear[C_ATTRELSCHEMA_ARG].value)?;
    let relname = text_datum_string(mcx, clear[C_ATTRELNAME_ARG].value)?;

    if transam_xlog::RecoveryInProgress() {
        return Err(recovery_in_progress_error());
    }

    let reloid = lookup_relation(mcx, &nspname, &relname)?;

    let attname = text_datum_string(mcx, clear[C_ATTNAME_ARG].value)?;
    let attnum = lsyscache::get_attnum(reloid, &attname)?;

    if attnum < 0 {
        return Err(Box::new(
            PgError::error(format!(
                "cannot clear statistics on system column \"{attname}\""
            ))
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }

    if attnum == InvalidAttrNumber {
        let relnm = lsyscache::get_rel_name(mcx, reloid)?
            .map(|s| s.as_str().to_string())
            .unwrap_or_default();
        return Err(column_name_missing(&attname, &relnm));
    }

    let inherited = clear[C_INHERITED_ARG].value.as_bool();

    delete_pg_statistic(mcx, reloid, attnum, inherited)?;
    Ok(Datum::null())
}
