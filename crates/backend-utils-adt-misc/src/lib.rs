//! Port of PostgreSQL `src/backend/utils/adt/misc.c` — miscellaneous
//! SQL-callable utility functions.
//!
//! Every `misc.c` function is ported with its original logic, branch order,
//! message text and SQLSTATE preserved against the C source. The computational
//! cores live here: `count_nulls`'s variadic null-bitmap scan,
//! `is_ident_start`/`is_ident_cont`, `scanner_isspace`, the whole `parse_ident`
//! scanner, `pg_sleep`'s sleep loop, `pg_basetype`'s domain-stack walk,
//! `pg_get_keywords`'s keyword-table render, the `pg_column_is_updatable`
//! `REQ_EVENTS` test, the soft-error plumbing of `pg_input_*`, and
//! `pg_current_logfile`'s format validation.
//!
//! Genuinely-external work — catalog/syscache lookups, the fmgr calling
//! convention for type I/O, the rewriteHandler view-tree probe, the relcache
//! relation open, fd.c directory/file walking, the latch (`pg_sleep`), and the
//! `system_fk_info` catalog table — is routed through the owning unit's seam
//! crate (panic until the owner lands). The two pure leaves it can reach
//! directly (the grammar keyword table, identifier down-casing, the database
//! name and replica-index lookups) are direct dependencies.
//!
//! Public surface: owned/borrowed values, `Option`, `Result`, `PgVec`. The bare
//! `PGFunction`/`fcinfo` shim (argument fetch, `get_fn_expr_argtype`,
//! `PG_GET_COLLATION`, SRF tuplestore/array assembly) is the deferred fmgr
//! boundary; functions that need a resolved arg type or collation take it as a
//! parameter, exactly as the fmgr shim will supply it.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use mcx::{Mcx, PgVec};
use types_core::{AttrNumber, InvalidOid, Oid};
use types_error::{
    PgError, PgResult, SoftErrorContext, ERRCODE_DATATYPE_MISMATCH,
    ERRCODE_INVALID_PARAMETER_VALUE,
};
use types_tuple::heaptuple::{FirstLowInvalidHeapAttributeNumber, UNKNOWNOID};

pub use types_adt_misc::{CatalogForeignKeyRow, KeywordRow, TypeBaseStep};

use backend_utils_adt_misc_seams::{
    catalog_foreign_keys as catalog_foreign_keys_seam, current_logfile as current_logfile_seam,
    tablespace_databases as tablespace_databases_seam,
    tablespace_location as tablespace_location_seam,
};
use backend_rewrite_rewritehandler_seams::relation_is_updatable as relation_is_updatable_seam;
use backend_utils_adt_format_type_seams::format_type_be as format_type_be_seam;
use backend_utils_adt_ruleutils_seams::generate_collation_name as generate_collation_name_seam;
use backend_utils_adt_timestamp_seams::get_current_timestamp as get_current_timestamp_seam;
use backend_utils_cache_lsyscache_seams::type_is_collatable as type_is_collatable_seam;
use backend_utils_fmgr_fmgr_seams::input_is_valid_by_type as input_is_valid_by_type_seam;
use backend_utils_init_miscinit_seams::check_for_interrupts as check_for_interrupts_seam;
use backend_parser_parse_type_seams::parse_type_string as parse_type_string_seam;
use backend_storage_ipc_latch_seams::{
    reset_latch_my_latch as reset_latch_my_latch_seam, wait_latch_my_latch as wait_latch_my_latch_seam,
};

pub mod fmgr_builtins;
mod system_fk_info;

pub use system_fk_info::build_sys_fk_rows;

#[cfg(test)]
mod tests;

// ===========================================================================
// num_nulls() / num_nonnulls() (misc.c:75-187)
// ===========================================================================

/// The resolved argument shape `count_nulls` (misc.c:75) operates on, after the
/// `get_fn_expr_variadic(fcinfo->flinfo)` branch (misc.c:83).
pub enum CountNullsArgs<'a> {
    /// The non-variadic case (misc.c:142): one `isnull` flag per argument.
    Separate(&'a [bool]),
    /// The VARIADIC-array case (misc.c:84). `arg_is_null` is `PG_ARGISNULL(0)`
    /// (misc.c:98); when false, `nitems` is `ArrayGetNItems(...)` and `bitmap`
    /// is `ARR_NULLBITMAP(arr)` (misc.c:114-119).
    Variadic {
        /// `PG_ARGISNULL(0)` (misc.c:98).
        arg_is_null: bool,
        /// `ArrayGetNItems(ndims, dims)` (misc.c:116).
        nitems: i32,
        /// `ARR_NULLBITMAP(arr)` (misc.c:119): `None` when the array has no null
        /// bitmap (all elements non-null).
        bitmap: Option<&'a [u8]>,
    },
}

/// `count_nulls()` (misc.c:75): common subroutine for `num_nulls()` and
/// `num_nonnulls()`. Returns `Some((nargs, nulls))` on success, or `None` when
/// the function should return SQL NULL (the variadic-null case, misc.c:98).
pub fn count_nulls(args: &CountNullsArgs<'_>) -> Option<(i32, i32)> {
    let mut count: i32 = 0;

    // Did we get a VARIADIC array argument, or separate arguments? (misc.c:83)
    match args {
        CountNullsArgs::Variadic {
            arg_is_null,
            nitems,
            bitmap,
        } => {
            // If we get a null as VARIADIC array argument, we can't say anything
            // useful about the number of elements, so return NULL. (misc.c:98)
            if *arg_is_null {
                return None;
            }

            let nitems = *nitems;

            // Count those that are NULL (misc.c:119).
            if let Some(bitmap) = bitmap {
                let mut bitmask: i32 = 1;
                let mut byte_index: usize = 0;
                let mut i = 0;
                while i < nitems {
                    if (bitmap[byte_index] as i32 & bitmask) == 0 {
                        count += 1;
                    }
                    bitmask <<= 1;
                    if bitmask == 0x100 {
                        byte_index += 1;
                        bitmask = 1;
                    }
                    i += 1;
                }
            }

            Some((nitems, count))
        }
        CountNullsArgs::Separate(isnull) => {
            // Separate arguments, so just count 'em (misc.c:144).
            for &is_null in *isnull {
                if is_null {
                    count += 1;
                }
            }
            Some((isnull.len() as i32, count))
        }
    }
}

/// `pg_num_nulls()` (misc.c:161): count the number of NULL arguments. Returns
/// `None` for the SQL NULL result (variadic-null, misc.c:168).
pub fn pg_num_nulls(args: &CountNullsArgs<'_>) -> Option<i32> {
    let (_nargs, nulls) = count_nulls(args)?;
    Some(nulls)
}

/// `pg_num_nonnulls()` (misc.c:177): count the number of non-NULL arguments.
pub fn pg_num_nonnulls(args: &CountNullsArgs<'_>) -> Option<i32> {
    let (nargs, nulls) = count_nulls(args)?;
    Some(nargs - nulls)
}

// ===========================================================================
// current_database() / current_query() (misc.c:194-219)
// ===========================================================================

/// `current_database()` (misc.c:194): expose the current database name.
/// C: `namestrcpy(db, get_database_name(MyDatabaseId))`. `MyDatabaseId` is the
/// backend global the fmgr shim supplies as `my_database_id`; the name bytes are
/// the `NameStr` content allocated in `mcx`.
pub fn current_database<'mcx>(mcx: Mcx<'mcx>, my_database_id: Oid) -> PgResult<PgVec<'mcx, u8>> {
    // get_database_name returns None for a nonexistent OID; the C namestrcpy of
    // a NULL would crash, but MyDatabaseId is always valid in a live backend.
    // Reproduce the always-valid contract: a missing name is an internal error.
    match backend_commands_dbcommands::get_database_name(mcx, my_database_id)? {
        Some(name) => {
            let mut out = mcx::vec_with_capacity_in(mcx, name.as_bytes().len())?;
            out.extend_from_slice(name.as_bytes());
            Ok(out)
        }
        None => Err(PgError::error(
            "current_database: no database with the backend's MyDatabaseId",
        )),
    }
}

/// `current_query()` (misc.c:211): expose the current query string, or `None`
/// (SQL NULL) when `debug_query_string` is unset (misc.c:215).
///
/// `debug_query_string` is a per-backend global owned by tcop/postgres.c; the
/// fmgr shim supplies its current value (or `None`). The branch is ported 1:1.
pub fn current_query<'mcx>(
    mcx: Mcx<'mcx>,
    debug_query_string: Option<&[u8]>,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    match debug_query_string {
        Some(q) => {
            let mut out = mcx::vec_with_capacity_in(mcx, q.len())?;
            out.extend_from_slice(q);
            Ok(Some(out))
        }
        None => Ok(None),
    }
}

// ===========================================================================
// pg_tablespace_databases() / pg_tablespace_location() (misc.c:223-364)
// ===========================================================================

/// `pg_tablespace_databases()` (misc.c:223): the OIDs of databases that make
/// use of a tablespace. The `AllocateDir`/`ReadDir`/`directory_is_empty` walk
/// (and the `GLOBALTABLESPACE_OID` / "not a tablespace OID" WARNING + empty
/// cases) are seamed; an empty result (the C empty tuplestore) is the empty Vec.
pub fn pg_tablespace_databases(tablespace_oid: Oid) -> PgResult<Vec<Oid>> {
    Ok(tablespace_databases_seam::call(tablespace_oid)?.unwrap_or_default())
}

/// `pg_tablespace_location()` (misc.c:300): the on-disk location of a
/// tablespace. The `InvalidOid`/`DEFAULTTABLESPACE_OID`/`GLOBALTABLESPACE_OID`
/// special cases and the `lstat`/`readlink` resolution all touch
/// `MyDatabaseTableSpace`/fd.c, so they are seamed; the resolved path bytes are
/// returned (allocated in `mcx`).
pub fn pg_tablespace_location<'mcx>(
    mcx: Mcx<'mcx>,
    tablespace_oid: Oid,
) -> PgResult<PgVec<'mcx, u8>> {
    tablespace_location_seam::call(mcx, tablespace_oid)
}

// ===========================================================================
// pg_sleep() (misc.c:369)
// ===========================================================================

/// `pg_sleep()` (misc.c:369): delay for `secs` seconds.
///
/// ```c
/// endtime = GetNowFloat() + secs;        /* GetNowFloat = GetCurrentTimestamp()/1e6 */
/// for (;;) {
///     CHECK_FOR_INTERRUPTS();
///     delay = endtime - GetNowFloat();
///     if (delay >= 600.0)      delay_ms = 600000;
///     else if (delay > 0.0)    delay_ms = (long) ceil(delay * 1000.0);
///     else                     break;
///     WaitLatch(MyLatch, WL_LATCH_SET|WL_TIMEOUT|WL_EXIT_ON_PM_DEATH, delay_ms, WAIT_EVENT_PG_SLEEP);
///     ResetLatch(MyLatch);
/// }
/// ```
///
/// The clock read, the interrupt check and the latch wait/reset are seamed; the
/// sleep-loop control flow and the `delay`/`delay_ms` arithmetic are ported 1:1.
pub fn pg_sleep(secs: f64) -> PgResult<()> {
    // GetNowFloat(): (float8) GetCurrentTimestamp() / 1000000.0 (misc.c:387)
    let get_now_float = || -> f64 { get_current_timestamp_seam::call() as f64 / 1_000_000.0 };

    let endtime = get_now_float() + secs;

    loop {
        check_for_interrupts_seam::call()?;

        let delay = endtime - get_now_float();
        let delay_ms: i64 = if delay >= 600.0 {
            600_000
        } else if delay > 0.0 {
            (delay * 1000.0).ceil() as i64
        } else {
            break;
        };

        wait_latch_my_latch_seam::call(
            types_storage::waiteventset::WL_LATCH_SET
                | types_storage::waiteventset::WL_TIMEOUT
                | types_storage::waiteventset::WL_EXIT_ON_PM_DEATH,
            delay_ms,
            types_pgstat::wait_event::WAIT_EVENT_PG_SLEEP,
        )?;
        reset_latch_my_latch_seam::call();
    }

    Ok(())
}

// ===========================================================================
// pg_get_keywords() / pg_get_catalog_foreign_keys() (misc.c:417-557)
// ===========================================================================

/// `pg_get_keywords()` (misc.c:417): the list of grammar keywords, one
/// [`KeywordRow`] per `ScanKeywords.num_keywords`. Reads the grammar keyword
/// table directly (pure static data); the SRF tuplestore plumbing is the fmgr
/// shim's job. The category-letter/description and bare-label rendering are
/// ported 1:1 from the C `switch`.
pub fn pg_get_keywords() -> Vec<KeywordRow> {
    use common_keywords::{
        GetScanKeyword, ScanKeywordBareLabel, ScanKeywordCategories, ScanKeywords,
    };
    use types_core::keywords::KeywordCategory;

    let n = ScanKeywords.num_keywords();
    let mut rows = Vec::with_capacity(n);

    for i in 0..n {
        // values[0] = GetScanKeyword(i, &ScanKeywords) (misc.c:446).
        let word = GetScanKeyword(i, &ScanKeywords)
            .expect("GetScanKeyword in 0..num_keywords")
            .as_bytes()
            .to_vec();

        // values[1]/values[3]: category letter + description (misc.c:450).
        let (catcode, catdesc): (Option<&'static str>, Option<&'static str>) =
            match ScanKeywordCategories[i] {
                KeywordCategory::Unreserved => (Some("U"), Some("unreserved")),
                KeywordCategory::ColumnName => {
                    (Some("C"), Some("unreserved (cannot be function or type name)"))
                }
                KeywordCategory::TypeOrFunctionName => {
                    (Some("T"), Some("reserved (can be function or type name)"))
                }
                KeywordCategory::Reserved => (Some("R"), Some("reserved")),
            };

        // values[2]/values[4]: bare-label flag + description (misc.c:474).
        let (barelabel, baredesc) = if ScanKeywordBareLabel[i] {
            ("true", "can be bare label")
        } else {
            ("false", "requires AS")
        };

        rows.push(KeywordRow {
            word,
            catcode,
            catdesc,
            barelabel,
            baredesc,
        });
    }

    rows
}

/// `pg_get_catalog_foreign_keys()` (misc.c:495): the catalog foreign-key
/// relationships, one [`CatalogForeignKeyRow`] per `sys_fk_relationships[]`
/// entry (with `fk_columns`/`pk_columns` already passed through `array_in`).
/// The generated `system_fk_info.h` table and the `array_in` fmgr dispatch are
/// unported, so the whole row set is produced by the seam.
pub fn pg_get_catalog_foreign_keys() -> PgResult<Vec<CatalogForeignKeyRow>> {
    catalog_foreign_keys_seam::call()
}

// ===========================================================================
// pg_typeof() / pg_basetype() / pg_collation_for() (misc.c:563-637)
// ===========================================================================

/// `pg_typeof()` (misc.c:563): `get_fn_expr_argtype(fcinfo->flinfo, 0)`. The
/// argument-type resolution needs the call expression (the fmgr shim's job), so
/// the resolved arg-type OID is the input; the function is the identity over it.
pub fn pg_typeof(arg0_type: Oid) -> Oid {
    arg0_type
}

/// `pg_basetype()` (misc.c:582): the base type of a (possibly nested) domain,
/// or the type's own OID; SQL NULL for a bogus/non-existent type OID.
///
/// ```c
/// for (;;) {
///     tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
///     if (!HeapTupleIsValid(tup)) PG_RETURN_NULL();   /* bogus OID */
///     typTup = GETSTRUCT(tup);
///     if (typTup->typtype != TYPTYPE_DOMAIN) { ReleaseSysCache(tup); break; }
///     typid = typTup->typbasetype;
///     ReleaseSysCache(tup);
/// }
/// PG_RETURN_OID(typid);
/// ```
///
/// The per-step syscache lookup is produced by `step_lookup` (the
/// `SearchSysCache1(TYPEOID)` projection the fmgr shim/syscache supplies); the
/// domain-stack loop is ported 1:1. Returns `None` for the bogus-OID
/// `PG_RETURN_NULL`.
pub fn pg_basetype(
    typid: Oid,
    mut step_lookup: impl FnMut(Oid) -> PgResult<Option<TypeBaseStep>>,
) -> PgResult<Option<Oid>> {
    let mut typid = typid;
    // We loop to find the bottom base type in a stack of domains. (misc.c:589)
    loop {
        let Some(step) = step_lookup(typid)? else {
            // return NULL for bogus OID (misc.c:597)
            return Ok(None);
        };
        if !step.is_domain {
            // Not a domain, so done (misc.c:601).
            break;
        }
        typid = step.typbasetype;
    }
    Ok(Some(typid))
}

/// `pg_collation_for()` (misc.c:618): the collation name of the argument (the
/// COLLATE FOR expression), or SQL NULL.
///
/// ```c
/// typeid = get_fn_expr_argtype(fcinfo->flinfo, 0);
/// if (!typeid) PG_RETURN_NULL();
/// if (!type_is_collatable(typeid) && typeid != UNKNOWNOID)
///     ereport(ERROR, errcode(ERRCODE_DATATYPE_MISMATCH),
///             errmsg("collations are not supported by type %s", format_type_be(typeid)));
/// collid = PG_GET_COLLATION();
/// if (!collid) PG_RETURN_NULL();
/// PG_RETURN_TEXT_P(cstring_to_text(generate_collation_name(collid)));
/// ```
///
/// The arg type OID (`get_fn_expr_argtype`) and the collation OID
/// (`PG_GET_COLLATION`) come from the fmgr shim; the `type_is_collatable` check,
/// the DATATYPE_MISMATCH error (with `format_type_be`) and `generate_collation_name`
/// are seamed. Branch order and message text are ported 1:1. Returns `None` for
/// the two `PG_RETURN_NULL` cases.
pub fn pg_collation_for<'mcx>(
    mcx: Mcx<'mcx>,
    arg0_type: Oid,
    collation: Oid,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    let typeid = arg0_type;
    if typeid == InvalidOid {
        return Ok(None);
    }
    if !type_is_collatable_seam::call(typeid)? && typeid != UNKNOWNOID {
        let type_name = format_type_be_seam::call(mcx, typeid)?;
        return Err(PgError::error(format!(
            "collations are not supported by type {}",
            type_name.as_str()
        ))
        .with_sqlstate(ERRCODE_DATATYPE_MISMATCH));
    }

    let collid = collation;
    if collid == InvalidOid {
        return Ok(None);
    }
    let name = generate_collation_name_seam::call(mcx, collid)?;
    let mut out = mcx::vec_with_capacity_in(mcx, name.as_bytes().len())?;
    out.extend_from_slice(name.as_bytes());
    Ok(Some(out))
}

// ===========================================================================
// pg_relation_is_updatable() / pg_column_is_updatable() (misc.c:647-684)
// ===========================================================================

/// `pg_relation_is_updatable()` (misc.c:647): the bitmask of update events the
/// relation supports. `relation_is_updatable(reloid, NIL, include_triggers,
/// NULL)` walks the view rewrite tree, so it is seamed.
pub fn pg_relation_is_updatable(reloid: Oid, include_triggers: bool) -> PgResult<i32> {
    relation_is_updatable_seam::call(reloid, include_triggers, None)
}

// REQ_EVENTS: we require both updatability and deletability of the relation
// (misc.c:681): (1 << CMD_UPDATE) | (1 << CMD_DELETE). CmdType: CMD_UPDATE=2,
// CMD_DELETE=4 (nodes.h), so REQ_EVENTS == (1<<2)|(1<<4) == 0x14.
const REQ_EVENTS: i32 = (1 << 2) | (1 << 4);

/// `pg_column_is_updatable()` (misc.c:664): whether a column is updatable
/// (information_schema.columns.is_updatable).
///
/// ```c
/// AttrNumber col = attnum - FirstLowInvalidHeapAttributeNumber;
/// if (attnum <= 0) PG_RETURN_BOOL(false);   /* system columns never updatable */
/// events = relation_is_updatable(reloid, NIL, include_triggers, bms_make_singleton(col));
/// PG_RETURN_BOOL((events & REQ_EVENTS) == REQ_EVENTS);
/// ```
///
/// `relation_is_updatable` (with the singleton bitmapset) is seamed; the
/// system-column short-circuit, the `col` mapping and the `REQ_EVENTS` test are
/// ported 1:1.
pub fn pg_column_is_updatable(
    reloid: Oid,
    attnum: AttrNumber,
    include_triggers: bool,
) -> PgResult<bool> {
    let col = (attnum as i32) - (FirstLowInvalidHeapAttributeNumber as i32);

    // System columns are never updatable (misc.c:674).
    if attnum <= 0 {
        return Ok(false);
    }

    let events = relation_is_updatable_seam::call(reloid, include_triggers, Some(col))?;

    // We require both updatability and deletability of the relation (misc.c:680).
    Ok((events & REQ_EVENTS) == REQ_EVENTS)
}

// ===========================================================================
// pg_input_is_valid() / pg_input_error_info() (misc.c:688-820)
// ===========================================================================

/// The four columns of `pg_input_error_info()` (misc.c:716): `message`,
/// `detail`, `hint`, `sql_error_code`. All `None` (every column SQL NULL) when
/// the input was valid (misc.c:733).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct InputErrorInfo {
    /// `values[0]` — `escontext.error_data->message` (never NULL on error).
    pub message: Option<Vec<u8>>,
    /// `values[1]` — `escontext.error_data->detail`.
    pub detail: Option<Vec<u8>>,
    /// `values[2]` — `escontext.error_data->hint`.
    pub hint: Option<Vec<u8>>,
    /// `values[3]` — `unpack_sql_state(escontext.error_data->sqlerrcode)`.
    pub sql_error_code: Option<Vec<u8>>,
}

/// `pg_input_is_valid()` (misc.c:695): test whether `str` is valid input for
/// the type named `typname`. Returns the C `bool`.
pub fn pg_input_is_valid(str: &[u8], typname: &[u8]) -> PgResult<bool> {
    // ErrorSaveContext escontext = {T_ErrorSaveContext};  (details_wanted = false)
    let mut escontext = SoftErrorContext::new(false);
    pg_input_is_valid_common(str, typname, &mut escontext)
}

/// `pg_input_error_info()` (misc.c:716): test whether `str` is valid input for
/// the type named `typname`, returning the error fields when not.
///
/// Enables `details_wanted`, runs the common subroutine, and — on the error
/// path — reads `message`/`detail`/`hint`/`unpack_sql_state(sqlerrcode)` from
/// the saved error in the exact C field order (misc.c:744-757). All-NULL when
/// the input was valid (misc.c:733).
pub fn pg_input_error_info(str: &[u8], typname: &[u8]) -> PgResult<InputErrorInfo> {
    // Enable details_wanted (misc.c:729).
    let mut escontext = SoftErrorContext::new(true);

    if pg_input_is_valid_common(str, typname, &mut escontext)? {
        // memset(isnull, true, ...) — all columns NULL (misc.c:733).
        Ok(InputErrorInfo::default())
    } else {
        // Assert error_occurred / error_data / message != NULL (misc.c:738).
        let error = escontext.take_error().ok_or_else(|| {
            PgError::error("pg_input_error_info: soft error must be set when not valid")
        })?;

        Ok(InputErrorInfo {
            // values[0] = message (misc.c:744)
            message: Some(error.message().as_bytes().to_vec()),
            // values[1] = detail, else NULL (misc.c:746)
            detail: error.detail().map(|d| d.as_bytes().to_vec()),
            // values[2] = hint, else NULL (misc.c:751)
            hint: error.hint().map(|h| h.as_bytes().to_vec()),
            // values[3] = unpack_sql_state(sqlerrcode) (misc.c:756)
            sql_error_code: Some(unpack_sql_state(error.sqlstate().0).into_bytes()),
        })
    }
}

/// `pg_input_is_valid_common()` (misc.c:764): shared implementation of
/// `pg_input_is_valid`/`pg_input_error_info`.
///
/// The per-call I/O caching (`ValidIOData`/`fn_extra`, misc.c:777) is the fmgr
/// shim's concern; the `parseTypeString` (misc.c:799) resolves the type OID and
/// typmod, and `getTypeInputInfo` + `fmgr_info_cxt` + `InputFunctionCallSafe`
/// (misc.c:804-814) are seamed together into `input_is_valid_by_type`. The
/// control flow — resolve the type, then attempt the soft conversion — is ported
/// 1:1.
pub fn pg_input_is_valid_common(
    str: &[u8],
    typname: &[u8],
    escontext: &mut SoftErrorContext,
) -> PgResult<bool> {
    // Parse type-name argument to obtain type OID and encoded typmod (misc.c:799).
    // parseTypeString here is the hard-error caller (C passes NULL escontext for
    // the type-name parse: a bad type name is a hard error), so soft = false.
    let typnamestr = std::str::from_utf8(typname)
        .map_err(|_| PgError::error("invalid type name").with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE))?;
    let (typoid, typmod) = parse_type_string_seam::call(typnamestr, false)?
        .expect("parse_type_string with soft=false never returns None");

    // Now we can try to perform the conversion (misc.c:813).
    input_is_valid_by_type_seam::call(typoid, typmod, str, escontext)
}

/// `unpack_sql_state(sqlerrcode)` (elog.c): render a `MAKE_SQLSTATE`-packed
/// 5-character SQLSTATE (six bits per char, biased by '0') as its text. Used by
/// `pg_input_error_info` (misc.c:756). Ported in-crate (pure bit-twiddling).
fn unpack_sql_state(sql_state: i32) -> String {
    // PGSIXBIT decode: each of the 5 characters is 6 bits, value + '0'.
    let mut out = String::with_capacity(5);
    let mut v = sql_state;
    for _ in 0..5 {
        out.push((b'0' + (v & 0x3F) as u8) as char);
        v >>= 6;
    }
    out
}

// ===========================================================================
// parse_ident() + identifier-class helpers (misc.c:827-992)
// ===========================================================================

/// `IS_HIGHBIT_SET(c)` (c.h): the high bit of a byte is set (might be part of a
/// multibyte char).
#[inline]
fn is_highbit_set(c: u8) -> bool {
    c & 0x80 != 0
}

/// `is_ident_start()` (misc.c:827): is `c` a valid identifier start? Must match
/// scan.l's `{ident_start}` class: `_`, ASCII letters, or any high-bit byte.
pub fn is_ident_start(c: u8) -> bool {
    // Underscores and ASCII letters are OK (misc.c:831).
    if c == b'_' {
        return true;
    }
    if c.is_ascii_lowercase() || c.is_ascii_uppercase() {
        return true;
    }
    // Any high-bit-set character is OK (misc.c:836).
    if is_highbit_set(c) {
        return true;
    }
    false
}

/// `is_ident_cont()` (misc.c:845): is `c` a valid identifier continuation? Must
/// match scan.l's `{ident_cont}` class: a digit, `$`, or an identifier-start
/// character.
pub fn is_ident_cont(c: u8) -> bool {
    // Can be digit or dollar sign ... (misc.c:849)
    if c.is_ascii_digit() || c == b'$' {
        return true;
    }
    // ... or an identifier start character (misc.c:852)
    is_ident_start(c)
}

/// `scanner_isspace()` (scansup.c:117): true if the flex scanner considers `ch`
/// whitespace. Must match scan.l's `{space}` list.
pub fn scanner_isspace(ch: u8) -> bool {
    matches!(ch, b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c)
}

/// `parse_ident()` (misc.c:860): parse a SQL qualified identifier into separate
/// identifiers. Returns the parsed parts as `text[]` element bytes; the
/// `accumArrayResult`/`makeArrayResult` assembly into a `TEXTOID` array is the
/// arrayfuncs/fmgr shim's job (deferred). When `strict`, any chars after the
/// last identifier are disallowed.
///
/// The entire scanner — the quoted-identifier `""`-unescape `memmove` loop, the
/// unquoted identifier scan + `downcase_identifier`, the
/// `missing_ident`/`after_dot` error selection (each with its exact errdetail
/// text), the whitespace skipping and the dot/end/strict-trailing handling — is
/// ported 1:1. `downcase_identifier` is the only cross-crate call.
pub fn parse_ident<'mcx>(
    mcx: Mcx<'mcx>,
    qualname: &[u8],
    strict: bool,
) -> PgResult<Vec<PgVec<'mcx, u8>>> {
    // The original (unmodified) string, for error messages (misc.c:872).
    let original = qualname;

    // The code below scribbles on qualname_str, so we copy it (misc.c:865). The
    // quoted-unescape loop mutates the working buffer in place, exactly like the
    // C memmove; keep it as an mcx-charged PgVec we index into.
    let mut buf: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, qualname.len())?;
    buf.extend_from_slice(qualname);

    let mut parts: Vec<PgVec<'mcx, u8>> = Vec::new();
    let mut nextp: usize = 0;
    let mut after_dot = false;

    let invalid = |original: &[u8], detail: Option<&str>| -> PgError {
        let e = PgError::error(format!(
            "string is not a valid identifier: \"{}\"",
            String::from_utf8_lossy(original)
        ))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE);
        match detail {
            Some(d) => e.with_detail(d.to_string()),
            None => e,
        }
    };

    let at = |buf: &[u8], i: usize| -> u8 { buf.get(i).copied().unwrap_or(0) };

    // skip leading whitespace (misc.c:878)
    while scanner_isspace(at(&buf, nextp)) {
        nextp += 1;
    }

    loop {
        let mut missing_ident = true;

        if at(&buf, nextp) == b'"' {
            // Quoted identifier (misc.c:886).
            let curname = nextp + 1;
            let endp;
            loop {
                // endp = strchr(nextp + 1, '"')
                let search_start = nextp + 1;
                let found = buf[search_start..].iter().position(|&c| c == b'"');
                match found {
                    None => {
                        return Err(invalid(original, Some("String has unclosed double quotes.")));
                    }
                    Some(rel) => {
                        let e = search_start + rel;
                        // if (endp[1] != '"') break;  (misc.c:900)
                        if at(&buf, e + 1) != b'"' {
                            endp = e;
                            break;
                        }
                        // memmove(endp, endp + 1, strlen(endp)); -- drop one of the
                        // doubled quotes, then nextp = endp (misc.c:902). On the
                        // PgVec this is copy_within (shift the tail down) plus a pop
                        // (drop the now-duplicated last byte).
                        buf.copy_within(e + 1.., e);
                        buf.pop();
                        nextp = e;
                    }
                }
            }
            // nextp = endp + 1; *endp = '\0';  (misc.c:905)
            nextp = endp + 1;

            // if (endp - curname == 0) -- empty quoted identifier (misc.c:908)
            if endp == curname {
                return Err(invalid(original, Some("Quoted identifier must not be empty.")));
            }

            // accumArrayResult(CStringGetTextDatum(curname)) (misc.c:915). The
            // terminator at endp truncates curname; the identifier bytes are
            // buf[curname..endp].
            let mut part = mcx::vec_with_capacity_in(mcx, endp - curname)?;
            part.extend_from_slice(&buf[curname..endp]);
            parts.push(part);
            missing_ident = false;
        } else if is_ident_start(at(&buf, nextp)) {
            // Unquoted identifier (misc.c:919).
            let curname = nextp;
            nextp += 1;
            while is_ident_cont(at(&buf, nextp)) {
                nextp += 1;
            }
            let len = nextp - curname;

            // downcase_identifier(curname, len, false, false) (misc.c:937): we do
            // NOT implicitly truncate identifiers. With truncate=false the
            // translation is byte-for-byte and preserves length (scansup.c:46).
            let downname =
                backend_parser_small1::downcase_identifier(mcx, &buf[curname..curname + len], false, false)?;
            debug_assert_eq!(
                downname.len(),
                len,
                "downcase_identifier must preserve length with truncate=false"
            );
            // cstring_to_text_with_len(downname, len) (misc.c:938): the first len
            // bytes of the downcased name.
            let mut part = mcx::vec_with_capacity_in(mcx, len)?;
            part.extend_from_slice(&downname[..len]);
            parts.push(part);
            missing_ident = false;
        }

        if missing_ident {
            // Different error messages based on where we failed (misc.c:946).
            let err = if at(&buf, nextp) == b'.' {
                invalid(original, Some("No valid identifier before \".\"."))
            } else if after_dot {
                invalid(original, Some("No valid identifier after \".\"."))
            } else {
                invalid(original, None)
            };
            return Err(err);
        }

        // while (scanner_isspace(*nextp)) nextp++;  (misc.c:966)
        while scanner_isspace(at(&buf, nextp)) {
            nextp += 1;
        }

        if at(&buf, nextp) == b'.' {
            // (misc.c:969)
            after_dot = true;
            nextp += 1;
            while scanner_isspace(at(&buf, nextp)) {
                nextp += 1;
            }
        } else if at(&buf, nextp) == 0 {
            // *nextp == '\0' (misc.c:976)
            break;
        } else {
            // Trailing junk after the last identifier (misc.c:980).
            if strict {
                return Err(invalid(original, None));
            }
            break;
        }
    }

    // makeArrayResult(astate, ...) (misc.c:991): the element bytes; the array
    // assembly is the arrayfuncs/fmgr shim's responsibility (deferred).
    Ok(parts)
}

// ===========================================================================
// pg_current_logfile() (misc.c:999-1095)
// ===========================================================================

/// `pg_current_logfile()` (misc.c:999): the current log file used by the log
/// collector, for the optional log-format `logfmt` (`stderr`/`csvlog`/`jsonlog`,
/// else error). `None` (SQL NULL) when no matching file.
///
/// The format validation (and its INVALID_PARAMETER_VALUE message/hint) is
/// ported 1:1; the `current_logfiles` scan (`AllocateFile`/`fgets`, the
/// corrupted-file `elog`s, the format matching) is seamed. `logfmt == None`
/// models both the 0-arg overload and a SQL NULL argument.
pub fn pg_current_logfile<'mcx>(
    mcx: Mcx<'mcx>,
    logfmt: Option<&[u8]>,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    if let Some(logfmt) = logfmt {
        if logfmt != b"stderr" && logfmt != b"csvlog" && logfmt != b"jsonlog" {
            return Err(PgError::error(format!(
                "log format \"{}\" is not supported",
                String::from_utf8_lossy(logfmt)
            ))
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
            .with_hint(
                "The supported log formats are \"stderr\", \"csvlog\", and \"jsonlog\".",
            ));
        }
    }

    current_logfile_seam::call(mcx, logfmt)
}

/// `pg_current_logfile_1arg()` (misc.c:1091): the 1-argument wrapper, needed for
/// the opr_sanity arg-count check; `return pg_current_logfile(fcinfo)`.
pub fn pg_current_logfile_1arg<'mcx>(
    mcx: Mcx<'mcx>,
    logfmt: Option<&[u8]>,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    pg_current_logfile(mcx, logfmt)
}

// ===========================================================================
// pg_get_replica_identity_index() (misc.c:1100)
// ===========================================================================

/// `pg_get_replica_identity_index()` (misc.c:1100): the OID of the relation's
/// replica-identity index, or `None` (SQL NULL) if there is none.
///
/// ```c
/// rel = table_open(reloid, AccessShareLock);
/// idxoid = RelationGetReplicaIndex(rel);
/// table_close(rel, AccessShareLock);
/// if (OidIsValid(idxoid)) PG_RETURN_OID(idxoid); else PG_RETURN_NULL();
/// ```
///
/// `RelationGetReplicaIndex` (repo: takes the relation OID and does the
/// open/close internally) is called directly; the `OidIsValid` -> NULL branch is
/// ported 1:1.
pub fn pg_get_replica_identity_index(reloid: Oid) -> PgResult<Option<Oid>> {
    let idxoid = backend_utils_cache_relcache::derived::RelationGetReplicaIndex(reloid)?;
    // if (OidIsValid(idxoid)) PG_RETURN_OID(idxoid); else PG_RETURN_NULL();
    if idxoid != InvalidOid {
        Ok(Some(idxoid))
    } else {
        Ok(None)
    }
}

// ===========================================================================
// any_value_transfn() (misc.c:1120)
// ===========================================================================

/// `any_value_transfn()` (misc.c:1120): transition function for the `ANY_VALUE`
/// aggregate — `PG_RETURN_DATUM(PG_GETARG_DATUM(0))`, i.e. keep the running
/// state datum unchanged. Pure identity over the (already-collected) state.
pub fn any_value_transfn<T>(state: T) -> T {
    state
}

// `init_seams()` registers this crate's `fmgr_builtins` rows into the fmgr-core
// builtin table (C: `fmgr_builtins[]`), matching the other adt crates
// (`oid`/`dbsize`/`mcxtfuncs`/...), and installs the one inward data seam this
// crate owns — `catalog_foreign_keys`, the generated `sys_fk_relationships[]`
// table (catalog/system_fk_info.h) that `pg_get_catalog_foreign_keys()`
// consumes. `seams-init::init_all` calls it alongside the rest.

/// Register this crate's fmgr builtins into the fmgr-core builtin table and
/// install the `catalog_foreign_keys` data seam. Called once at startup by
/// `seams-init::init_all`.
pub fn init_seams() {
    fmgr_builtins::register_misc_builtins();
    backend_utils_adt_misc_seams::catalog_foreign_keys::set(|| Ok(build_sys_fk_rows()));
}
