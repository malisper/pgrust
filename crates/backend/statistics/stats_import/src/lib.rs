#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

pub mod attribute_stats;
pub mod relation_stats;

use datum::Datum;
use mcx::Mcx;
use types_core::catalog::DATABASE_RELATION_ID;
use types_core::{InvalidOid, Oid, TEXTOID};
use types_error::{
    ErrorLocation, PgError, PgResult, SqlState, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_UNDEFINED_OBJECT, ERRCODE_WRONG_OBJECT_TYPE, WARNING,
};
use types_fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};
use rel_vocab::RangeVar;
use types_nodes::parsenodes::ObjectType;
use types_rel::lock::{ShareUpdateExclusiveLock, LOCKMODE};

pub const STATS_IMPORT_BUILTINS: &[FmgrBuiltin] = &[
    FmgrBuiltin {
        foid: 6362,
        name: "pg_restore_relation_stats",
        nargs: 1,
        strict: false,
        retset: false,
        func: relation_stats::fc_pg_restore_relation_stats,
    },
    FmgrBuiltin {
        foid: 6363,
        name: "pg_restore_attribute_stats",
        nargs: 1,
        strict: false,
        retset: false,
        func: attribute_stats::fc_pg_restore_attribute_stats,
    },
    FmgrBuiltin {
        foid: 6397,
        name: "pg_clear_relation_stats",
        nargs: 2,
        strict: false,
        retset: false,
        func: relation_stats::fc_pg_clear_relation_stats,
    },
    FmgrBuiltin {
        foid: 6398,
        name: "pg_clear_attribute_stats",
        nargs: 4,
        strict: false,
        retset: false,
        func: attribute_stats::fc_pg_clear_attribute_stats,
    },
];

pub(crate) struct StatsArgInfo {
    pub argname: &'static str,
    pub argtype: Oid,
}

#[derive(Clone, Copy)]
pub(crate) struct Arg {
    pub value: Datum,
    pub isnull: bool,
}

impl Arg {
    pub const NULL: Arg = Arg { value: Datum::null(), isnull: true };

    pub fn present(value: Datum) -> Arg {
        Arg { value, isnull: false }
    }
}

pub(crate) const RELKIND_RELATION: u8 = b'r';
pub(crate) const RELKIND_INDEX: u8 = b'i';
pub(crate) const RELKIND_SEQUENCE: u8 = b'S';
pub(crate) const RELKIND_VIEW: u8 = b'v';
pub(crate) const RELKIND_MATVIEW: u8 = b'm';
pub(crate) const RELKIND_FOREIGN_TABLE: u8 = b'f';
pub(crate) const RELKIND_PARTITIONED_TABLE: u8 = b'p';
pub(crate) const RELKIND_PARTITIONED_INDEX: u8 = b'I';

const ACLCHECK_OK: i32 = 0;

#[track_caller]
pub(crate) fn loc(funcname: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, funcname)
}

pub(crate) fn warn(
    funcname: &'static str,
    msg: String,
    code: Option<SqlState>,
    detail: Option<String>,
    hint: Option<String>,
) -> PgResult<()> {
    let mut b = elog::ereport(WARNING).errmsg(msg);
    if let Some(c) = code {
        b = b.errcode(c);
    }
    if let Some(d) = detail {
        b = b.errdetail(d);
    }
    if let Some(h) = hint {
        b = b.errhint(h);
    }
    b.finish(loc(funcname))
}

// ThrowErrorData with elevel forced to WARNING (text_to_stavalues soft path).
pub(crate) fn warn_error_data(funcname: &'static str, err: PgError) -> PgResult<()> {
    warn(
        funcname,
        err.message().to_string(),
        Some(err.sqlstate()),
        err.detail().map(|s| s.to_string()),
        err.hint().map(|s| s.to_string()),
    )
}

pub(crate) fn text_datum_string(mcx: Mcx<'_>, d: Datum) -> PgResult<String> {
    let p = d.as_usize() as *const u8;
    // SAFETY: TEXTOID-typed by-ref datum: a live varlena image readable
    // through its full VARSIZE_ANY.
    let image = unsafe { core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) };
    let payload = varlena::open_image(mcx, image)?;
    Ok(String::from_utf8_lossy(payload.as_bytes()).into_owned())
}

pub(crate) fn detoast_array_datum<'m>(mcx: Mcx<'m>, d: Datum) -> PgResult<mcx::PgVec<'m, u8>> {
    let p = d.as_usize() as *const u8;
    // SAFETY: array-typed by-ref datum: a live varlena image.
    let raw = unsafe { core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) };
    detoast_seams::detoast_attr::call(mcx, raw)
}

pub(crate) fn stats_check_required_arg(
    args: &[Arg],
    arginfo: &[StatsArgInfo],
    argnum: usize,
) -> PgResult<()> {
    if args[argnum].isnull {
        return Err(Box::new(
            PgError::error(format!(
                "argument \"{}\" must not be null",
                arginfo[argnum].argname
            ))
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        ));
    }
    Ok(())
}

pub(crate) fn stats_check_arg_array(
    mcx: Mcx<'_>,
    args: &[Arg],
    arginfo: &[StatsArgInfo],
    argnum: usize,
) -> PgResult<bool> {
    if args[argnum].isnull {
        return Ok(true);
    }

    let arr = detoast_array_datum(mcx, args[argnum].value)?;

    if arrayfuncs::arr_ndim(&arr) != 1 {
        warn(
            "stats_check_arg_array",
            format!(
                "argument \"{}\" must not be a multidimensional array",
                arginfo[argnum].argname
            ),
            Some(ERRCODE_INVALID_PARAMETER_VALUE),
            None,
            None,
        )?;
        return Ok(false);
    }

    if arrayfuncs::array_contains_nulls(&arr) {
        warn(
            "stats_check_arg_array",
            format!(
                "argument \"{}\" array must not contain null values",
                arginfo[argnum].argname
            ),
            Some(ERRCODE_INVALID_PARAMETER_VALUE),
            None,
            None,
        )?;
        return Ok(false);
    }

    Ok(true)
}

pub(crate) fn stats_check_arg_pair(
    args: &[Arg],
    arginfo: &[StatsArgInfo],
    argnum1: usize,
    argnum2: usize,
) -> PgResult<bool> {
    if args[argnum1].isnull && args[argnum2].isnull {
        return Ok(true);
    }

    if args[argnum1].isnull || args[argnum2].isnull {
        let (nullarg, otherarg) = if args[argnum1].isnull {
            (argnum1, argnum2)
        } else {
            (argnum2, argnum1)
        };

        warn(
            "stats_check_arg_pair",
            format!(
                "argument \"{}\" must be specified when argument \"{}\" is specified",
                arginfo[nullarg].argname, arginfo[otherarg].argname
            ),
            Some(ERRCODE_INVALID_PARAMETER_VALUE),
            None,
            None,
        )?;

        return Ok(false);
    }

    Ok(true)
}

fn get_arg_by_name(argname: &str, arginfo: &[StatsArgInfo]) -> PgResult<Option<usize>> {
    for (argnum, info) in arginfo.iter().enumerate() {
        if argname.eq_ignore_ascii_case(info.argname) {
            return Ok(Some(argnum));
        }
    }

    warn(
        "get_arg_by_name",
        format!("unrecognized argument name: \"{argname}\""),
        None,
        None,
        None,
    )?;

    Ok(None)
}

fn stats_check_arg_type(argname: &str, argtype: Oid, expectedtype: Oid) -> PgResult<bool> {
    if argtype != expectedtype {
        warn(
            "stats_check_arg_type",
            format!(
                "argument \"{}\" has type {}, expected type {}",
                argname,
                format_type::format_type_be(argtype)?,
                format_type::format_type_be(expectedtype)?
            ),
            None,
            None,
            None,
        )?;
        return Ok(false);
    }
    Ok(true)
}

#[track_caller]
#[cold]
fn pairs_error() -> Box<PgError> {
    Box::new(
        PgError::error("variadic arguments must be name/value pairs").with_hint(
            "Provide an even number of variadic arguments that can be divided into pairs.",
        ),
    )
}

pub(crate) fn stats_fill_fcinfo_from_arg_pairs(
    mcx: Mcx<'_>,
    flinfo: Option<&FmgrInfo>,
    fcinfo: &Fcinfo,
    arginfo: &[StatsArgInfo],
    positional: &mut [Arg],
) -> PgResult<bool> {
    let mut result = true;

    // extract_variadic_args returning None is C's nargs == -1 (an explicit
    // VARIADIC NULL): -1 % 2 != 0 raises the even-pairs error.
    let va = match funcapi::extract_variadic_args(mcx, flinfo, fcinfo, 0, true)? {
        Some(va) => va,
        None => return Err(pairs_error()),
    };

    let nargs = va.args.len();
    if nargs % 2 != 0 {
        return Err(pairs_error());
    }

    let mut i = 0;
    while i < nargs {
        if va.nulls[i] {
            return Err(Box::new(PgError::error(format!(
                "name at variadic position {} is null",
                i + 1
            ))));
        }

        if va.types[i] != TEXTOID {
            return Err(Box::new(PgError::error(format!(
                "name at variadic position {} has type {}, expected type {}",
                i + 1,
                format_type::format_type_be(va.types[i])?,
                format_type::format_type_be(TEXTOID)?
            ))));
        }

        if va.nulls[i + 1] {
            i += 2;
            continue;
        }

        let argname = text_datum_string(mcx, va.args[i])?;

        // 'version' is accepted but ignored; not a positional argument.
        if argname.eq_ignore_ascii_case("version") {
            i += 2;
            continue;
        }

        match get_arg_by_name(&argname, arginfo)? {
            Some(argnum)
                if stats_check_arg_type(&argname, va.types[i + 1], arginfo[argnum].argtype)? =>
            {
                positional[argnum] = Arg::present(va.args[i + 1]);
            }
            _ => result = false,
        }

        i += 2;
    }

    Ok(result)
}

// get_relkind_objtype (objectaddress.c), the reachable kinds.
fn get_relkind_objtype(relkind: u8) -> ObjectType {
    match relkind {
        RELKIND_RELATION | RELKIND_PARTITIONED_TABLE => ObjectType::OBJECT_TABLE,
        RELKIND_INDEX | RELKIND_PARTITIONED_INDEX => ObjectType::OBJECT_INDEX,
        RELKIND_SEQUENCE => ObjectType::OBJECT_SEQUENCE,
        RELKIND_VIEW => ObjectType::OBJECT_VIEW,
        RELKIND_MATVIEW => ObjectType::OBJECT_MATVIEW,
        RELKIND_FOREIGN_TABLE => ObjectType::OBJECT_FOREIGN_TABLE,
        _ => ObjectType::OBJECT_TABLE,
    }
}

fn RangeVarCallbackForStats(
    mcx: Mcx<'_>,
    relation: &RangeVar<'_>,
    rel_id: Oid,
    old_rel_id: Oid,
    locked_oid: &mut Oid,
) -> PgResult<()> {
    let mut table_oid = rel_id;

    if rel_id != old_rel_id && *locked_oid != InvalidOid {
        lmgr::UnlockRelationOid(*locked_oid, ShareUpdateExclusiveLock as LOCKMODE)?;
        *locked_oid = InvalidOid;
    }

    if rel_id == InvalidOid {
        return Ok(());
    }

    let relkind = lsyscache::get_rel_relkind(rel_id)? as u8;
    if relkind == RELKIND_INDEX || relkind == RELKIND_PARTITIONED_INDEX {
        table_oid = catalog_index::IndexGetRelation(mcx, rel_id, false)?;
    }

    if rel_id == old_rel_id {
        if table_oid == rel_id && *locked_oid != InvalidOid {
            return Err(Box::new(
                PgError::error(format!(
                    "index \"{}\" was concurrently dropped",
                    relation.relname
                ))
                .with_sqlstate(ERRCODE_UNDEFINED_OBJECT),
            ));
        }

        if table_oid != rel_id && table_oid != *locked_oid {
            return Err(Box::new(
                PgError::error(format!(
                    "index \"{}\" was concurrently created",
                    relation.relname
                ))
                .with_sqlstate(ERRCODE_UNDEFINED_OBJECT),
            ));
        }
    }

    let shape = syscache_seams::lookup_pg_class_by_relid::call(table_oid)?
        .ok_or_else(|| cache_lookup_failed(table_oid))?;
    let form_relkind = lsyscache::get_rel_relkind(table_oid)? as u8;
    let form_relname = lsyscache::get_rel_name(mcx, table_oid)?
        .ok_or_else(|| cache_lookup_failed(table_oid))?;

    match form_relkind {
        RELKIND_RELATION | RELKIND_MATVIEW | RELKIND_FOREIGN_TABLE
        | RELKIND_PARTITIONED_TABLE => {}
        other => {
            return Err(Box::new(
                PgError::error(format!(
                    "cannot modify statistics for relation \"{}\"",
                    form_relname.as_str()
                ))
                .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE)
                .with_detail(pg_class::errdetail_relkind_not_supported(other)?),
            ));
        }
    }

    if shape.relisshared {
        return Err(Box::new(
            PgError::error("cannot modify statistics for shared relation")
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }

    if !aclchk::object_ownercheck(
        DATABASE_RELATION_ID,
        init_small::globals::MyDatabaseId(),
        miscinit::GetUserId(),
    )? {
        let aclresult = aclchk::pg_class_aclcheck(
            table_oid,
            miscinit::GetUserId(),
            types_nodes::parsenodes::ACL_MAINTAIN as u64,
        )?;
        if aclresult != ACLCHECK_OK {
            aclchk::aclcheck_error(
                aclresult,
                get_relkind_objtype(form_relkind),
                form_relname.as_str(),
            )?;
        }
    }

    // Lock heap before index to avoid deadlock.
    if rel_id != old_rel_id && table_oid != rel_id {
        lmgr::LockRelationOid(table_oid, ShareUpdateExclusiveLock as LOCKMODE)?;
        *locked_oid = table_oid;
    }

    Ok(())
}

#[track_caller]
#[cold]
fn cache_lookup_failed(oid: Oid) -> Box<PgError> {
    Box::new(PgError::error(format!("cache lookup failed for OID {oid}")))
}

// RangeVarGetRelidExtended(makeRangeVar(nspname, relname, -1),
// ShareUpdateExclusiveLock, 0, RangeVarCallbackForStats, &locked_table).
pub(crate) fn lookup_relation(mcx: Mcx<'_>, nspname: &str, relname: &str) -> PgResult<Oid> {
    let rv = RangeVar {
        catalogname: None,
        schemaname: Some(nspname),
        relname,
        inh: true,
        relpersistence: b'p',
        location: -1,
    };

    let mut locked_oid = InvalidOid;
    let mut callback = |rel: &RangeVar<'_>, rel_id: Oid, old_rel_id: Oid| {
        RangeVarCallbackForStats(mcx, rel, rel_id, old_rel_id, &mut locked_oid)
    };

    catalog_namespace::RangeVarGetRelidExtended(
        &rv,
        ShareUpdateExclusiveLock as LOCKMODE,
        0,
        Some(&mut callback),
    )
}

#[cold]
pub(crate) fn recovery_in_progress_error() -> Box<PgError> {
    Box::new(
        PgError::error("recovery is in progress")
            .with_sqlstate(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .with_hint("Statistics cannot be modified during recovery."),
    )
}
