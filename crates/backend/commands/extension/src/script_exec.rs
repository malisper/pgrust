//! The extension install/update SCRIPT-EXECUTION pipeline of `extension.c`
//! (C 870-1458): `read_extension_script_file`, `execute_sql_string`, and
//! `execute_extension_script`.
//!
//! This is the genuinely-gated half F0 left behind: it drives the
//! parser/analyzer/planner/executor/utility pipeline (`pg_parse_query` →
//! `pg_analyze_and_rewrite_fixedparams` → `pg_plan_queries` →
//! `CreateQueryDesc`/`ExecutorStart`/…/`ExecutorEnd` or `ProcessUtility`), the
//! superuser/GUC security-context switch, and the `@extschema@`/`@extowner@`/
//! `MODULE_PATHNAME` text substitutions. Ported 1:1 with C in branch order /
//! SQLSTATE / messages.

use ::mcx::{Mcx, MemoryContext};
use ::types_core::primitive::Oid;
use ::types_core::InvalidOid;
use ::types_error::{
    PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INSUFFICIENT_PRIVILEGE,
    ERRCODE_INVALID_TEXT_REPRESENTATION, ERROR,
};

use ::utils_error::ereport;

use ::nodes::nodeindexscan::PlannedStmt;
use ::nodes::nodes::T_TransactionStmt;
use ::nodes::parsestmt::ProcessUtilityContext;

use transam_xact_seams as xact_seams;
use dest_seams as dest_seams;
use utility_seams as utility_seams;
use lsyscache_seams as lsyscache_seams;
use snapmgr_seams as snapmgr;

use crate::{
    extension_is_trusted, get_extension_script_filename, set_creating_extension,
    set_current_extension_object, ExtensionControlFile,
};

/// `C_COLLATION_OID` (pg_collation.dat oid 950) — the substitution `replace_text`
/// / `textregexreplace` calls run under the `C` collation (C `C_COLLATION_OID`).
const C_COLLATION_OID: Oid = 950;

/// The `quoting_relevant_chars` set (C 1334): characters that cannot be
/// substituted safely inside dollar-/single-quoted literals.
const QUOTING_RELEVANT_CHARS: &[u8] = b"\"$'\\";

/// `pg_strpbrk`-style test: does `s` contain any byte from `set`?
fn strpbrk(s: &str, set: &[u8]) -> bool {
    s.bytes().any(|b| set.contains(&b))
}

// ===========================================================================
// read_extension_script_file (C 867-898)
// ===========================================================================

/// `read_extension_script_file(control, filename)` (C 867-898) — read an SQL
/// script file into a string, and convert to the database encoding.
///
/// The C `read_whole_file` + `pg_verify_mbstr` + `pg_any_to_server` chain. The
/// script bytes are raw server-encoding text; [`crate::read_whole_file`] returns
/// them as a (lossy-decoded) `String` whose bytes we re-verify and transcode.
pub fn read_extension_script_file(
    control: &ExtensionControlFile,
    filename: &str,
) -> PgResult<String> {
    // src_str = read_whole_file(filename, &len);
    let src_str = crate::read_whole_file(filename)?;
    let src_bytes = src_str.as_bytes();
    let len = src_bytes.len();

    // use database encoding if not given
    //   if (control->encoding < 0) src_encoding = GetDatabaseEncoding();
    //   else src_encoding = control->encoding;
    let src_encoding = if control.encoding < 0 {
        mbutils::GetDatabaseEncoding()
    } else {
        control.encoding
    };

    // make sure that source string is valid in the expected encoding
    //   (void) pg_verify_mbstr(src_encoding, src_str, len, false);
    let _ = mbutils::pg_verify_mbstr(src_encoding, src_bytes, false)?;

    // Convert the encoding to the database encoding. read_whole_file
    // null-terminated the string, so if no conversion happens the string is
    // valid as is.
    //   dest_str = pg_any_to_server(src_str, len, src_encoding);
    let scratch = MemoryContext::new("read_extension_script_file transcode");
    let dest = mbutils::pg_any_to_server(scratch.mcx(), src_bytes, src_encoding)?;
    let dest_str = match dest {
        // pg_any_to_server returns the unchanged source when no conversion is
        // needed (database encoding == source encoding); model the C `char *`
        // by returning the original bytes.
        None => src_str,
        Some(bytes) => String::from_utf8_lossy(bytes.as_slice()).into_owned(),
    };
    let _ = len;

    Ok(dest_str)
}

// ===========================================================================
// execute_sql_string (C 1045-1165)
// ===========================================================================

/// `execute_sql_string(sql, filename)` (C 1045-1165) — execute the given SQL
/// string.
///
/// Note (C 1037-1043): SPI is deliberately not used — SPI would parse/analyze/
/// plan the WHOLE string before executing any of it, which fails when later
/// statements refer to objects created earlier in the script. So each raw parse
/// tree is fully executed before the next is analyzed.
///
/// `filename` is carried only for the C `script_error_callback` errcontext
/// (which is not threaded here — the executor's own error positions suffice).
pub fn execute_sql_string(sql: &str, _filename: &str) -> PgResult<()> {
    // The whole run lives in one arena: the parse trees, analyzed/rewritten
    // queries and plans are all charged here (the C per-statement contexts are a
    // memory-bounding refinement; the owned arena keeps the statement node tree
    // alive across the analyze→plan→execute of each statement). The source text
    // must outlive the parse trees, so it is interned into the arena first.
    let arena = MemoryContext::new("execute_sql_string");
    let mcx = arena.mcx();
    let sql_owned = ::mcx::PgString::from_str_in(sql, mcx)?;
    let sql_ref: &str = sql_owned.as_str();

    // Parse the SQL string into a list of raw parse trees.
    //   raw_parsetree_list = pg_parse_query(sql);
    let raw_parsetree_list = postgres::simple_query::pg_parse_query(mcx, sql_ref)?;

    // All output from SELECTs goes to the bit bucket.
    //   dest = CreateDestReceiver(DestNone);
    let dest = dest_seams::create_dest_receiver::call(types_dest::CommandDest::None);

    // Do parse analysis, rule rewrite, planning, and execution for each raw
    // parsetree. We must fully execute each query before beginning parse analysis
    // on the next one, since there may be interdependencies.
    for parsetree in raw_parsetree_list.iter() {
        // Be sure parser can see any DDL done so far.
        //   CommandCounterIncrement();
        xact_seams::command_counter_increment::call()?;

        // stmt_list = pg_analyze_and_rewrite_fixedparams(parsetree, sql, NULL, 0, NULL);
        let query_list = postgres::simple_query::pg_analyze_and_rewrite_fixedparams(
            mcx, parsetree, sql_ref, &[],
        )?;

        // stmt_list = pg_plan_queries(stmt_list, sql, CURSOR_OPT_PARALLEL_OK, NULL);
        let stmt_list = postgres::simple_query::pg_plan_queries(
            mcx,
            query_list,
            sql_ref,
            CURSOR_OPT_PARALLEL_OK,
            None,
        )?;

        for stmt in stmt_list.iter() {
            //   CommandCounterIncrement();
            xact_seams::command_counter_increment::call()?;

            //   PushActiveSnapshot(GetTransactionSnapshot());
            let snap = snapmgr::get_transaction_snapshot::call()?;
            snapmgr::push_active_snapshot::call(std::rc::Rc::new(snap))?;

            if stmt.utilityStmt.is_none() {
                // An optimizable statement: drive the executor directly.
                run_executor(mcx, stmt, sql_ref, dest)?;
            } else {
                // A utility statement: ProcessUtility.
                //   if (IsA(stmt->utilityStmt, TransactionStmt)) ereport(ERROR, ...);
                let is_txn = stmt
                    .utilityStmt
                    .as_deref()
                    .map(|u| u.node_tag() == T_TransactionStmt)
                    .unwrap_or(false);
                if is_txn {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                        .errmsg(
                            "transaction control statements are not allowed within an extension script",
                        )
                        .into_error());
                }

                //   ProcessUtility(stmt, sql, false, PROCESS_UTILITY_QUERY,
                //                  NULL, NULL, dest, NULL);
                let mut qc = portal::QueryCompletion::default();
                utility_seams::process_utility::call(
                    mcx,
                    stmt,
                    sql_ref,
                    false, // readOnlyTree
                    ProcessUtilityContext::PROCESS_UTILITY_QUERY,
                    None, // params
                    dest,
                    &mut qc,
                )?;
            }

            //   PopActiveSnapshot();
            snapmgr::pop_active_snapshot::call()?;
        }
    }

    // Be sure to advance the command counter after the last script command.
    //   CommandCounterIncrement();
    xact_seams::command_counter_increment::call()?;

    Ok(())
}

/// `CURSOR_OPT_PARALLEL_OK` (parsenodes.h) — pg_plan_queries cursor option.
const CURSOR_OPT_PARALLEL_OK: i32 = 0x0400;

/// The `stmt->utilityStmt == NULL` leg of [`execute_sql_string`] (C 1120-1135):
/// CreateQueryDesc → ExecutorStart/Run/Finish/End → FreeQueryDesc, to the
/// DestNone receiver.
fn run_executor(
    _mcx: Mcx<'_>,
    stmt: &PlannedStmt<'_>,
    sql: &str,
    dest: ::nodes::parsestmt::DestReceiverHandle,
) -> PgResult<()> {
    // qdesc = CreateQueryDesc(stmt, sql, GetActiveSnapshot(), NULL, dest, NULL, NULL, 0);
    let snap = snapmgr::get_active_snapshot::call()?;
    let parent = MemoryContext::new("execute_sql_string QueryDesc");
    let mut qdesc =
        execMain::CreateQueryDesc(&parent, stmt, sql, snap, None, dest, None, 0)?;

    // ExecutorStart(qdesc, 0);
    execMain::ExecutorStart(&mut qdesc, 0)?;
    // ExecutorRun(qdesc, ForwardScanDirection, 0); (count==0 ⇒ all rows)
    execMain::ExecutorRun(&mut qdesc, ::types_scan::sdir::ForwardScanDirection, 0)?;
    // ExecutorFinish(qdesc);
    execMain::ExecutorFinish(&mut qdesc)?;
    // ExecutorEnd(qdesc);
    execMain::ExecutorEnd(&mut qdesc)?;
    // FreeQueryDesc(qdesc);
    execMain::FreeQueryDesc(qdesc)?;

    Ok(())
}

// ===========================================================================
// execute_extension_script (C 1188-1458)
// ===========================================================================

/// `execute_extension_script(extensionOid, control, from_version, version,
/// requiredSchemas, schemaName)` (C 1188-1458) — execute the appropriate script
/// file for installing or updating the extension. If `from_version` is `Some`,
/// it's an update.
///
/// `required_schemas` must be one-for-one with `control.requires`.
#[allow(clippy::too_many_arguments)]
pub fn execute_extension_script(
    mcx: Mcx<'_>,
    extension_oid: Oid,
    control: &ExtensionControlFile,
    from_version: Option<&str>,
    version: &str,
    required_schemas: &[Oid],
    schema_name: &str,
) -> PgResult<()> {
    // Enforce superuser-ness if appropriate. We postpone these checks until here
    // so that the control flags are correctly associated with the right script(s)
    // if they happen to be set in secondary control files.
    let mut switch_to_superuser = false;
    if control.superuser && !superuser_seams::superuser::call()? {
        if extension_is_trusted(control)? {
            switch_to_superuser = true;
        } else if from_version.is_none() {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                .errmsg(format!(
                    "permission denied to create extension \"{}\"",
                    control.name
                ))
                .errhint(if control.trusted {
                    "Must have CREATE privilege on current database to create this extension."
                } else {
                    "Must be superuser to create this extension."
                })
                .into_error());
        } else {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                .errmsg(format!(
                    "permission denied to update extension \"{}\"",
                    control.name
                ))
                .errhint(if control.trusted {
                    "Must have CREATE privilege on current database to update this extension."
                } else {
                    "Must be superuser to update this extension."
                })
                .into_error());
        }
    }

    let filename = get_extension_script_filename(control, from_version, version)?;

    // (elog(DEBUG1, "executing extension script ...") omitted: pure trace.)

    // If installing a trusted extension on behalf of a non-superuser, become the
    // bootstrap superuser. (This switch will be cleaned up automatically if the
    // transaction aborts, as will the GUC changes below.)
    let mut save_userid: Oid = InvalidOid;
    let mut save_sec_context: i32 = 0;
    if switch_to_superuser {
        let (uid, sec) = miscinit::GetUserIdAndSecContext();
        save_userid = uid;
        save_sec_context = sec;
        miscinit::SetUserIdAndSecContext(
            ::types_core::catalog::BOOTSTRAP_SUPERUSERID,
            save_sec_context | SECURITY_LOCAL_USERID_CHANGE,
        );
    }

    // Force client_min_messages and log_min_messages to be at least WARNING, so
    // that we won't spam the user with useless NOTICE messages from common script
    // actions like creating shell types. We use the equivalent of a function SET
    // option to allow the setting to persist for exactly the duration of the
    // script execution. guc.c also takes care of undoing the setting on error.
    //
    // log_min_messages can't be set by ordinary users, so for that one we pretend
    // to be superuser.
    let save_nestlevel = misc_guc::NewGUCNestLevel();

    // if (client_min_messages < WARNING) set_config_option("client_min_messages",
    //     "warning", PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SAVE, true, 0, false);
    if guc_below_warning("client_min_messages") {
        misc_guc::live::set_config_option_global(
            "client_min_messages",
            Some("warning"),
            types_guc::PGC_USERSET,
            types_guc::PGC_S_SESSION,
            miscinit::GetUserId(),
            misc_guc::GUC_ACTION_SAVE,
            true,
            ::types_error::error::ErrorLevel(0),
            false,
        )?;
    }
    // if (log_min_messages < WARNING) set_config_option_ext("log_min_messages",
    //     "warning", PGC_SUSET, PGC_S_SESSION, BOOTSTRAP_SUPERUSERID,
    //     GUC_ACTION_SAVE, true, 0, false);
    if guc_below_warning("log_min_messages") {
        misc_guc::live::set_config_option_global(
            "log_min_messages",
            Some("warning"),
            types_guc::PGC_SUSET,
            types_guc::PGC_S_SESSION,
            ::types_core::catalog::BOOTSTRAP_SUPERUSERID,
            misc_guc::GUC_ACTION_SAVE,
            true,
            ::types_error::error::ErrorLevel(0),
            false,
        )?;
    }

    // Similarly disable check_function_bodies, to ensure that SQL functions won't
    // be parsed during creation.
    if misc_guc::live::get_bool("check_function_bodies").unwrap_or(true) {
        misc_guc::live::set_config_option_global(
            "check_function_bodies",
            Some("off"),
            types_guc::PGC_USERSET,
            types_guc::PGC_S_SESSION,
            miscinit::GetUserId(),
            misc_guc::GUC_ACTION_SAVE,
            true,
            ::types_error::error::ErrorLevel(0),
            false,
        )?;
    }

    // Set up the search path to have the target schema first, making it be the
    // default creation target namespace. Then add the schemas of any prerequisite
    // extensions, unless they are in pg_catalog which would be searched anyway.
    // Finally add pg_temp to ensure that temp objects can't take precedence over
    // others.
    let mut pathbuf = String::new();
    pathbuf.push_str(quote_identifier_cstr(mcx, schema_name)?.as_str());
    for &reqschema in required_schemas {
        if let Some(reqname) = lsyscache_seams::get_namespace_name::call(mcx, reqschema)? {
            let reqname = reqname.as_str().to_string();
            if reqname != "pg_catalog" {
                pathbuf.push_str(", ");
                pathbuf.push_str(quote_identifier_cstr(mcx, &reqname)?.as_str());
            }
        }
    }
    pathbuf.push_str(", pg_temp");

    misc_guc::live::set_config_option_global(
        "search_path",
        Some(&pathbuf),
        types_guc::PGC_USERSET,
        types_guc::PGC_S_SESSION,
        miscinit::GetUserId(),
        misc_guc::GUC_ACTION_SAVE,
        true,
        ::types_error::error::ErrorLevel(0),
        false,
    )?;

    // Set creating_extension and related variables so that
    // recordDependencyOnCurrentExtension and other functions do the right things.
    // On failure, ensure we reset these variables (the C PG_FINALLY).
    set_creating_extension(true);
    set_current_extension_object(extension_oid);

    let result = run_script_body(
        mcx,
        control,
        &filename,
        switch_to_superuser,
        save_userid,
        schema_name,
        required_schemas,
    );

    // PG_FINALLY: reset the globals regardless of success/failure.
    set_creating_extension(false);
    set_current_extension_object(InvalidOid);

    // Propagate any error from the body now that the globals are reset (the C
    // PG_FINALLY re-throws after running). On the error path the GUC/userid
    // restores below are skipped, exactly as C's normal control flow does (the
    // transaction abort cleans them up).
    result?;

    // Restore the GUC variables we set above.
    //   AtEOXact_GUC(true, save_nestlevel);
    misc_guc::at_eoxact_guc(true, save_nestlevel);

    // Restore authentication state if needed.
    if switch_to_superuser {
        miscinit::SetUserIdAndSecContext(save_userid, save_sec_context);
    }

    Ok(())
}

/// `SECURITY_LOCAL_USERID_CHANGE` (miscadmin.h).
const SECURITY_LOCAL_USERID_CHANGE: i32 = 0x0001;

/// Is GUC `name` (an elevel enum) currently below `WARNING`? C compares the int
/// enum value `client_min_messages < WARNING`. The GUC store holds the elevel as
/// the enum's integer code.
fn guc_below_warning(name: &str) -> bool {
    match misc_guc::live::get_enum(name) {
        Some(v) => v < ::types_error::WARNING.0,
        // Absent ⇒ treat as not-below (the C reads a live int; a missing GUC is a
        // wiring bug, but we conservatively skip the override).
        None => false,
    }
}

/// `quote_identifier(s)` returning an owned `String` (the substitution loop
/// concatenates them into the search_path / replacement tokens).
fn quote_identifier_cstr(mcx: Mcx<'_>, s: &str) -> PgResult<String> {
    Ok(ruleutils::quote_identifier(mcx, s)?
        .as_str()
        .to_string())
}

/// The C `PG_TRY` body of `execute_extension_script` (C 1322-1440): read the
/// script file, run the `@extschema@`/`@extowner@`/`MODULE_PATHNAME`
/// substitutions, then `execute_sql_string`.
fn run_script_body(
    mcx: Mcx<'_>,
    control: &ExtensionControlFile,
    filename: &str,
    switch_to_superuser: bool,
    save_userid: Oid,
    schema_name: &str,
    required_schemas: &[Oid],
) -> PgResult<()> {
    // char *c_sql = read_extension_script_file(control, filename);
    let c_sql = read_extension_script_file(control, filename)?;

    // We use various functions that want to operate on text datums; here we keep
    // the working value as raw text payload bytes (the header-less varlena the
    // replace_text / textregexreplace seams consume and produce).
    let mut t_sql: Vec<u8> = c_sql.as_bytes().to_vec();

    // Reduce any lines beginning with "\echo" to empty. This allows scripts to
    // contain messages telling people not to run them via psql.
    //   t_sql = textregexreplace(t_sql, "^\\echo.*$", "", "ng");
    // The "ng" flags = REG_NEWLINE (n) + global (g ⇒ n==0). The pattern matches a
    // backslash-echo line. (REG_ADVANCED is the default base cflags.)
    {
        let scratch = MemoryContext::new("execute_extension_script \\echo strip");
        let out = varlena_seams::replace_text_regexp::call(
            scratch.mcx(),
            &t_sql,
            br"^\\echo.*$",
            b"",
            regex::REG_ADVANCED | regex::REG_NEWLINE,
            C_COLLATION_OID,
            0, // search_start
            0, // n == 0 ⇒ replace all (global)
        )?;
        t_sql = out.as_slice().to_vec();
    }

    // If the script uses @extowner@, substitute the calling username.
    if c_sql.contains("@extowner@") {
        let uid = if switch_to_superuser {
            save_userid
        } else {
            miscinit::GetUserId()
        };
        let user_name = match miscinit::GetUserNameFromId(mcx, uid, false)? {
            Some(n) => n.as_str().to_string(),
            None => String::new(),
        };
        let q_user_name = quote_identifier_cstr(mcx, &user_name)?;
        t_sql = replace_literal(mcx, &t_sql, b"@extowner@", q_user_name.as_bytes())?;
        if strpbrk(&user_name, QUOTING_RELEVANT_CHARS) {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_TEXT_REPRESENTATION)
                .errmsg(format!(
                    "invalid character in extension owner: must not contain any of \"{}\"",
                    quoting_chars_str()
                ))
                .into_error());
        }
    }

    // If it's not relocatable, substitute the target schema name for occurrences
    // of @extschema@. For a relocatable extension, we needn't do this.
    if !control.relocatable {
        let old = t_sql.clone();
        let q_schema_name = quote_identifier_cstr(mcx, schema_name)?;
        t_sql = replace_literal(mcx, &t_sql, b"@extschema@", q_schema_name.as_bytes())?;
        if t_sql != old && strpbrk(schema_name, QUOTING_RELEVANT_CHARS) {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_TEXT_REPRESENTATION)
                .errmsg(format!(
                    "invalid character in extension \"{}\" schema: must not contain any of \"{}\"",
                    control.name,
                    quoting_chars_str()
                ))
                .into_error());
        }
    }

    // Likewise, substitute required extensions' schema names for occurrences of
    // @extschema:extension_name@.
    debug_assert_eq!(control.requires.len(), required_schemas.len());
    for (reqextname, &reqschema) in control.requires.iter().zip(required_schemas.iter()) {
        let old = t_sql.clone();
        let req_schema_name = match lsyscache_seams::get_namespace_name::call(mcx, reqschema)? {
            Some(n) => n.as_str().to_string(),
            None => String::new(),
        };
        let q_schema_name = quote_identifier_cstr(mcx, &req_schema_name)?;
        let repltoken = format!("@extschema:{reqextname}@");
        t_sql = replace_literal(mcx, &t_sql, repltoken.as_bytes(), q_schema_name.as_bytes())?;
        if t_sql != old && strpbrk(&req_schema_name, QUOTING_RELEVANT_CHARS) {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_TEXT_REPRESENTATION)
                .errmsg(format!(
                    "invalid character in extension \"{reqextname}\" schema: must not contain any of \"{}\"",
                    quoting_chars_str()
                ))
                .into_error());
        }
    }

    // If module_pathname was set in the control file, substitute its value for
    // occurrences of MODULE_PATHNAME.
    if let Some(module_pathname) = &control.module_pathname {
        t_sql = replace_literal(mcx, &t_sql, b"MODULE_PATHNAME", module_pathname.as_bytes())?;
    }

    // And now back to C string.
    //   c_sql = text_to_cstring(DatumGetTextPP(t_sql));
    let final_sql = String::from_utf8_lossy(&t_sql).into_owned();

    execute_sql_string(&final_sql, filename)
}

/// `replace_text(src, from, to)` (varlena.c) — replace ALL occurrences of the
/// literal `from` in `src` with `to`. C runs this through the `replace_text`
/// SQL builtin (literal, case-sensitive, global); for the ASCII tokens the
/// extension machinery substitutes (`@extschema@`, `@extowner@`,
/// `@extschema:ext@`, `MODULE_PATHNAME`) a byte-literal global replace is exactly
/// equivalent. C returns `src` unchanged when `from` or `src` is empty, or when
/// `from` is not found.
fn replace_literal(_mcx: Mcx<'_>, src: &[u8], from: &[u8], to: &[u8]) -> PgResult<Vec<u8>> {
    // Return unmodified source if empty source or pattern (varlena.c replace_text).
    if src.is_empty() || from.is_empty() {
        return Ok(src.to_vec());
    }
    let mut out: Vec<u8> = Vec::with_capacity(src.len());
    let mut i = 0usize;
    while i < src.len() {
        if i + from.len() <= src.len() && &src[i..i + from.len()] == from {
            out.extend_from_slice(to);
            i += from.len();
        } else {
            out.push(src[i]);
            i += 1;
        }
    }
    Ok(out)
}

/// The displayable form of [`QUOTING_RELEVANT_CHARS`] for the error message.
fn quoting_chars_str() -> &'static str {
    // C: "\"$'\\" — the literal four characters.
    "\"$'\\"
}

// ===========================================================================
// ApplyExtensionUpdates (C 3554-3702)
// ===========================================================================

/// `ApplyExtensionUpdates(extensionOid, pcontrol, initialVersion,
/// updateVersions, origSchemaName, cascade, is_create)` (C 3554-3702) — apply the
/// chain of update scripts after an install, as though a series of ALTER
/// EXTENSION UPDATE commands were given.
#[allow(clippy::too_many_arguments)]
pub fn ApplyExtensionUpdates(
    mcx: Mcx<'_>,
    extension_oid: Oid,
    pcontrol: &ExtensionControlFile,
    initial_version: &str,
    update_versions: &[String],
    orig_schema_name: Option<&str>,
    cascade: bool,
    is_create: bool,
) -> PgResult<()> {
    use ::heaptuple::{heap_copytuple, heap_deform_tuple, heap_modify_tuple};
    use ::scankey::ScanKeyInit;
    use dependency_seams as dependency_seams;
    use ::indexing::keystone::CatalogTupleUpdate;
    use objectaccess_seams as objectaccess_seams;
    use ::types_catalog::catalog_dependency::{ObjectAddress, DEPENDENCY_NORMAL};
    use ::types_catalog::pg_extension as cat;
    use ::types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
    use ::types_storage::lock::RowExclusiveLock;
    use types_tuple::heaptuple::Datum;

    use genam_seams as genam_seams;
    use table_seams as table_seams;

    let mut old_version_name = initial_version.to_string();

    for version_name in update_versions {
        // Fetch parameters for specific version (pcontrol is not changed).
        let control = crate::read_extension_aux_control_file(pcontrol, version_name)?;

        // Find the pg_extension tuple.
        let ext_rel =
            table_seams::table_open::call(mcx, cat::ExtensionRelationId, RowExclusiveLock)?;

        let mut key = ScanKeyData::empty();
        ScanKeyInit(
            &mut key,
            cat::Anum_pg_extension_oid as ::types_core::AttrNumber,
            BTEqualStrategyNumber,
            ::types_core::fmgr::F_OIDEQ,
            Datum::from_oid(extension_oid),
        )?;
        let mut ext_scan = genam_seams::systable_beginscan::call(
            &ext_rel,
            cat::ExtensionOidIndexId,
            true,
            None,
            &[key],
        )?;

        let ext_tup = genam_seams::systable_getnext::call(mcx, ext_scan.desc_mut())?;
        let Some(ext_tup) = ext_tup else {
            // should not happen
            return Err(PgError::error(format!(
                "could not find tuple for extension {extension_oid}"
            )));
        };

        // Copy the tuple so we can modify it (the scan/relcache copy is read-only).
        let ext_tup = heap_copytuple(mcx, Some(&ext_tup))?
            .ok_or_else(|| PgError::error("heap_copytuple returned null"))?;

        // Determine the target schema (set by original install).
        let cols = heap_deform_tuple(mcx, &ext_tup.tuple, &ext_rel.rd_att, &ext_tup.data)?;
        let schema_oid = match &cols[cat::Anum_pg_extension_extnamespace as usize - 1].0 {
            Datum::ByVal(v) => *v as Oid,
            _ => return Err(PgError::error("extnamespace is not by-value")),
        };
        let schema_name = match lsyscache_seams::get_namespace_name::call(mcx, schema_oid)? {
            Some(s) => s.as_str().to_string(),
            None => String::new(),
        };

        // Modify extrelocatable and extversion in the pg_extension tuple.
        let natts = cat::Natts_pg_extension as usize;
        let mut values: Vec<Datum> = (0..natts).map(|_| Datum::from_oid(InvalidOid)).collect();
        let nulls = vec![false; natts];
        let mut repl = vec![false; natts];

        values[cat::Anum_pg_extension_extrelocatable as usize - 1] =
            Datum::from_bool(control.relocatable);
        repl[cat::Anum_pg_extension_extrelocatable as usize - 1] = true;
        values[cat::Anum_pg_extension_extversion as usize - 1] =
            varlena_seams::cstring_to_text_v::call(mcx, version_name)?;
        repl[cat::Anum_pg_extension_extversion as usize - 1] = true;

        let mut new_tup =
            heap_modify_tuple(mcx, &ext_tup, &ext_rel.rd_att, &values, &nulls, &repl)
                .map_err(|e| PgError::error(format!("heap_modify_tuple failed: {e:?}")))?;
        new_tup.tuple.t_self = ext_tup.tuple.t_self;

        CatalogTupleUpdate(mcx, &ext_rel, ext_tup.tuple.t_self, &mut new_tup)?;

        ext_scan.end()?;
        ext_rel.close(RowExclusiveLock)?;

        // Look up the prerequisite extensions for this version, install them if
        // necessary, and build lists of their OIDs and target-schema OIDs.
        let mut required_extensions: Vec<Oid> = Vec::new();
        let mut required_schemas: Vec<Oid> = Vec::new();
        for curreq in control.requires.iter() {
            let reqext = crate::get_required_extension(
                mcx,
                curreq,
                &control.name,
                orig_schema_name,
                cascade,
                &[], // NIL parents
                is_create,
            )?;
            let reqschema = crate::get_extension_schema(reqext)?;
            required_extensions.push(reqext);
            required_schemas.push(reqschema);
        }

        // Remove and recreate dependencies on prerequisite extensions.
        dependency_seams::delete_dependency_records_for_class::call(
            cat::ExtensionRelationId,
            extension_oid,
            cat::ExtensionRelationId,
            DEPENDENCY_NORMAL,
        )?;

        let myself = ObjectAddress {
            classId: cat::ExtensionRelationId,
            objectId: extension_oid,
            objectSubId: 0,
        };
        for &reqext in &required_extensions {
            let otherext = ObjectAddress {
                classId: cat::ExtensionRelationId,
                objectId: reqext,
                objectSubId: 0,
            };
            dependency_seams::record_dependency_on::call(myself, otherext, DEPENDENCY_NORMAL)?;
        }

        objectaccess_seams::invoke_object_post_alter_hook::call(
            cat::ExtensionRelationId,
            extension_oid,
            0,
        )?;

        // Finally, execute the update script file.
        execute_extension_script(
            mcx,
            extension_oid,
            &control,
            Some(&old_version_name),
            version_name,
            &required_schemas,
            &schema_name,
        )?;

        // Update prior-version name and loop around. Since execute_sql_string did
        // a final CommandCounterIncrement, we can update the pg_extension row again.
        old_version_name = version_name.clone();
    }

    Ok(())
}
