// Install/update script execution (extension.c:867-1458).
use elog::ereport;
use mcx::Mcx;
use types_core::{InvalidOid, Oid, SECURITY_LOCAL_USERID_CHANGE};
use types_error::{
    PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INSUFFICIENT_PRIVILEGE,
    ERRCODE_INVALID_TEXT_REPRESENTATION, ERROR, WARNING,
};
use types_nodes::NodeTag;
use types_portal::{ParamListHandle, QueryCompletion, QueryEnvHandle, CURSOR_OPT_PARALLEL_OK};

use crate::control::{get_extension_script_filename, read_whole_file, ExtensionControlFile};

const QUOTING_RELEVANT_CHARS: &str = "\"$'\\";

fn strpbrk(s: &str, set: &str) -> bool {
    s.bytes().any(|b| set.as_bytes().contains(&b))
}

pub(crate) fn read_extension_script_file(
    control: &ExtensionControlFile,
    filename: &str,
) -> PgResult<String> {
    let src = read_whole_file(filename)?;

    let src_encoding = if control.encoding < 0 {
        mbutils::GetDatabaseEncoding()
    } else {
        control.encoding
    };
    mbutils::pg_verify_mbstr(src_encoding, &src, false)?;

    let scratch = mcx::MemoryContext::new("read_extension_script_file transcode");
    let dest = mbutils::pg_any_to_server(scratch.mcx(), &src, src_encoding)?;
    let bytes = match &dest {
        None => src.as_slice(),
        Some(converted) => converted.as_slice(),
    };
    Ok(core::str::from_utf8(bytes)
        .expect("extension script is server-encoding text")
        .to_string())
}

// CleanQuerytext (postgres.c): trim to the statement bounds, then strip
// leading whitespace and trailing junk.
fn clean_querytext(query: &str, location: &mut i32, len: &mut i32) -> (usize, usize) {
    let bytes = query.as_bytes();
    let mut begin = (*location).max(0) as usize;
    let mut end = if *len > 0 {
        (begin + *len as usize).min(bytes.len())
    } else {
        bytes.len()
    };
    while begin < end && matches!(bytes[begin], b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c) {
        begin += 1;
    }
    while end > begin && matches!(bytes[end - 1], b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c | b';')
    {
        end -= 1;
    }
    *location = begin as i32;
    *len = (end - begin) as i32;
    (begin, end)
}

// script_error_callback (extension.c:904-1030), applied on the propagation
// path instead of C's error_context_stack (repo error-context divergence).
fn apply_script_error_context(
    mut e: Box<PgError>,
    sql: &str,
    filename: &str,
    stmt_location: i32,
    stmt_len: i32,
) -> Box<PgError> {
    let mut location = stmt_location;
    let mut len = stmt_len;

    let syntaxerrposition = e.cursor_position.unwrap_or(0);
    if syntaxerrposition > 0 {
        if location < 0
            || syntaxerrposition < location
            || (len > 0 && syntaxerrposition > location + len)
        {
            // Semicolon-newline heuristic for unknown statement bounds.
            let bytes = sql.as_bytes();
            location = 0;
            len = 0;
            let mut loc = 0usize;
            while loc < bytes.len() {
                if bytes[loc] != b';' {
                    loc += 1;
                    continue;
                }
                let mut probe = loc + 1;
                if probe < bytes.len() && bytes[probe] == b'\r' {
                    probe += 1;
                }
                if probe < bytes.len() && bytes[probe] == b'\n' {
                    let bkpt = (probe + 1) as i32;
                    if bkpt < syntaxerrposition {
                        location = bkpt;
                    } else if bkpt > syntaxerrposition {
                        len = bkpt - location;
                        break;
                    }
                }
                loc += 1;
            }
        }

        let (begin, end) = clean_querytext(sql, &mut location, &mut len);
        let mut pos = syntaxerrposition - location;
        pos = pos.clamp(0, len);

        e.cursor_position = None;
        e.internal_position = if pos != 0 { Some(pos) } else { None };
        e.internal_query = Some(sql[begin..end].to_string());
    } else if location >= 0 {
        let (begin, end) = clean_querytext(sql, &mut location, &mut len);
        e.add_context_line(format!("SQL statement \"{}\"", &sql[begin..end]));
    }

    let lastslash = match filename.rfind('/') {
        Some(p) => &filename[p + 1..],
        None => filename,
    };

    if location >= 0 {
        let mut linenumber = 1;
        for (i, b) in sql.bytes().enumerate() {
            if i as i32 >= location {
                break;
            }
            if b == b'\n' {
                linenumber += 1;
            }
        }
        e.add_context_line(format!(
            "extension script file \"{lastslash}\", near line {linenumber}"
        ));
    } else {
        e.add_context_line(format!("extension script file \"{lastslash}\""));
    }
    e
}

// execute_sql_string (extension.c:1045-1165). SPI is deliberately not used
// (C's note): each raw parsetree is fully executed before the next is
// analyzed.
pub(crate) fn execute_sql_string(sql: &str, filename: &str) -> PgResult<()> {
    let arena = mcx::MemoryContext::new("execute_sql_string");
    let mcx = arena.mcx();
    let sql_owned = mcx::PgString::from_str_in(sql, mcx)?;
    let sql_ref: &str = sql_owned.as_str();

    let mut stmt_location: i32 = -1;
    let mut stmt_len: i32 = 0;

    let result = (|| -> PgResult<()> {
        let raw_parsetree_list = parser_seams::raw_parser::call(
            mcx,
            sql_ref,
            parser_seams::RawParseMode::RAW_PARSE_DEFAULT,
        )?;

        let mut dest = tcop_dest::CreateDestReceiver(types_dest::CommandDest::None);

        for parsetree in raw_parsetree_list.iter() {
            stmt_location = parsetree.stmt_location;
            stmt_len = parsetree.stmt_len;

            xact::CommandCounterIncrement()?;

            let query = analyze_seams::parse_analyze_fixedparams::call(
                mcx,
                parsetree,
                sql_ref,
                &[],
                QueryEnvHandle::NULL,
            )?;
            let query_list = if query.commandType == types_nodes::nodes_enums::CmdType::CMD_UTILITY
            {
                let mut v = mcx::PgVec::new_in(mcx);
                v.try_reserve_exact(1).map_err(|_| mcx.oom(1))?;
                v.push(query);
                v
            } else {
                rewrite_handler_seams::query_rewrite::call(mcx, query)?
            };

            let mut stmt_list = mcx::PgVec::new_in(mcx);
            stmt_list.try_reserve_exact(query_list.len()).map_err(|_| mcx.oom(query_list.len()))?;
            for query in query_list {
                if query.commandType == types_nodes::nodes_enums::CmdType::CMD_UTILITY {
                    stmt_list.push(types_nodes::plannodes::PlannedStmt {
                        commandType: types_nodes::nodes_enums::CmdType::CMD_UTILITY,
                        canSetTag: query.canSetTag,
                        utilityStmt: query.utilityStmt,
                        stmt_location: query.stmt_location,
                        stmt_len: query.stmt_len,
                        queryId: types_nodes::SyncCell::new(query.queryId),
                        ..types_nodes::plannodes::PlannedStmt::default()
                    });
                } else {
                    stmt_list.push(planner_seams::planner::call(
                        mcx,
                        mcx::leak_in(mcx::alloc_in(mcx, query)?),
                        sql_ref,
                        CURSOR_OPT_PARALLEL_OK,
                        ParamListHandle::NULL,
                    )?);
                }
            }

            for stmt in stmt_list.iter() {
                xact::CommandCounterIncrement()?;

                let snap = snapmgr::GetTransactionSnapshot()?;
                snapmgr::PushActiveSnapshot(&snap)?;

                if stmt.utilityStmt.is_none() {
                    let active = snapmgr::GetActiveSnapshot();
                    let qd = execmain_seams::create_query_desc::call(
                        stmt,
                        sql_ref,
                        Some(active),
                        None,
                        dest.mydest(),
                        ParamListHandle::NULL,
                        QueryEnvHandle::NULL,
                        0,
                    )?;
                    let run = (|| -> PgResult<()> {
                        execmain_seams::executor_start::call(qd, 0)?;
                        execmain_seams::executor_run::call(
                            qd,
                            types_scan::sdir::ForwardScanDirection,
                            0,
                            &mut dest,
                        )?;
                        execmain_seams::executor_finish::call(qd)?;
                        execmain_seams::executor_end::call(qd)?;
                        Ok(())
                    })();
                    match run {
                        Ok(()) => execmain_seams::free_query_desc::call(qd),
                        Err(e) => {
                            execmain_seams::release_query_desc::call(qd);
                            return Err(e);
                        }
                    }
                } else {
                    let is_txn = stmt
                        .utilityStmt
                        .as_ref()
                        .is_some_and(|u| u.node_tag() == NodeTag::T_TransactionStmt);
                    if is_txn {
                        return Err(ereport(ERROR)
                            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                            .errmsg(
                                "transaction control statements are not allowed within an extension script",
                            )
                            .into_error()
                            .into());
                    }

                    let mut qc = QueryCompletion::default();
                    utility_seams::process_utility::call(
                        mcx,
                        stmt,
                        sql_ref,
                        false,
                        utility_seams::ProcessUtilityContext::PROCESS_UTILITY_QUERY,
                        ParamListHandle::NULL,
                        QueryEnvHandle::NULL,
                        &mut dest,
                        Some(&mut qc),
                    )?;
                }

                snapmgr::PopActiveSnapshot()?;
            }
        }

        xact::CommandCounterIncrement()?;
        Ok(())
    })();

    result.map_err(|e| apply_script_error_context(e, sql, filename, stmt_location, stmt_len))
}

// extension_is_trusted (extension.c:1174).
fn extension_is_trusted(control: &ExtensionControlFile) -> PgResult<bool> {
    if !control.trusted {
        return Ok(false);
    }
    let aclresult = aclchk::object_aclcheck(
        types_core::DATABASE_RELATION_ID,
        init_small::globals::MyDatabaseId(),
        miscinit::GetUserId(),
        adt_acl::ACL_CREATE,
    )?;
    Ok(aclresult == aclchk::ACLCHECK_OK)
}

// execute_extension_script (extension.c:1188-1458).
pub(crate) fn execute_extension_script(
    mcx: Mcx<'_>,
    extension_oid: Oid,
    control: &ExtensionControlFile,
    from_version: Option<&str>,
    version: &str,
    required_schemas: &[Oid],
    schema_name: &str,
) -> PgResult<()> {
    let mut switch_to_superuser = false;
    if control.superuser && !superuser::superuser()? {
        if extension_is_trusted(control)? {
            switch_to_superuser = true;
        } else if from_version.is_none() {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                .errmsg(format!("permission denied to create extension \"{}\"", control.name))
                .errhint(if control.trusted {
                    "Must have CREATE privilege on current database to create this extension."
                } else {
                    "Must be superuser to create this extension."
                })
                .into_error()
                .into());
        } else {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                .errmsg(format!("permission denied to update extension \"{}\"", control.name))
                .errhint(if control.trusted {
                    "Must have CREATE privilege on current database to update this extension."
                } else {
                    "Must be superuser to update this extension."
                })
                .into_error()
                .into());
        }
    }

    let filename = get_extension_script_filename(control, from_version, version);

    let mut save_userid: Oid = InvalidOid;
    let mut save_sec_context: i32 = 0;
    if switch_to_superuser {
        let (uid, sec) = miscinit::GetUserIdAndSecContext();
        save_userid = uid;
        save_sec_context = sec;
        miscinit::SetUserIdAndSecContext(
            types_core::BOOTSTRAP_SUPERUSERID,
            save_sec_context | SECURITY_LOCAL_USERID_CHANGE,
        );
    }

    let save_nestlevel = guc::NewGUCNestLevel();

    if guc_enum_below_warning(&guc_tables::vars::client_min_messages) {
        guc::set_config_option(
            "client_min_messages",
            Some("warning"),
            types_guc::PGC_USERSET,
            types_guc::PGC_S_SESSION,
            guc::GUC_ACTION_SAVE,
            true,
            types_error::ErrorLevel(0),
            false,
        )?;
    }
    if guc_enum_below_warning(&guc_tables::vars::log_min_messages) {
        guc::set_config_option_ext(
            "log_min_messages",
            Some("warning"),
            types_guc::PGC_SUSET,
            types_guc::PGC_S_SESSION,
            types_core::BOOTSTRAP_SUPERUSERID,
            guc::GUC_ACTION_SAVE,
            true,
            types_error::ErrorLevel(0),
            false,
        )?;
    }

    if (guc_tables::vars::check_function_bodies.get().get)() {
        guc::set_config_option(
            "check_function_bodies",
            Some("off"),
            types_guc::PGC_USERSET,
            types_guc::PGC_S_SESSION,
            guc::GUC_ACTION_SAVE,
            true,
            types_error::ErrorLevel(0),
            false,
        )?;
    }

    // search_path: target schema first, then non-pg_catalog prerequisite
    // schemas, then pg_temp.
    let mut pathbuf = String::new();
    pathbuf.push_str(quote_ident(mcx, schema_name)?.as_str());
    for &reqschema in required_schemas {
        if let Some(reqname) = lsyscache::get_namespace_name(mcx, reqschema)? {
            if reqname.as_str() != "pg_catalog" {
                pathbuf.push_str(", ");
                pathbuf.push_str(quote_ident(mcx, reqname.as_str())?.as_str());
            }
        }
    }
    pathbuf.push_str(", pg_temp");

    guc::set_config_option(
        "search_path",
        Some(&pathbuf),
        types_guc::PGC_USERSET,
        types_guc::PGC_S_SESSION,
        guc::GUC_ACTION_SAVE,
        true,
        types_error::ErrorLevel(0),
        false,
    )?;

    pg_depend::set_creating_extension(true);
    pg_depend::set_current_extension_object(extension_oid);

    let result = run_script_body(
        mcx,
        control,
        &filename,
        switch_to_superuser,
        save_userid,
        schema_name,
        required_schemas,
    );

    // PG_FINALLY: reset the globals; on error the GUC/userid restores below
    // are skipped exactly as in C (transaction abort cleans them up).
    pg_depend::set_creating_extension(false);
    pg_depend::set_current_extension_object(InvalidOid);
    result?;

    guc::AtEOXact_GUC(true, save_nestlevel);

    if switch_to_superuser {
        miscinit::SetUserIdAndSecContext(save_userid, save_sec_context);
    }

    Ok(())
}

fn guc_enum_below_warning(var: &guc_tables::GucEnumVar) -> bool {
    (var.get().get)() < WARNING.0
}

fn quote_ident(mcx: Mcx<'_>, s: &str) -> PgResult<String> {
    let q = adt_quote::quote_identifier(mcx, s.as_bytes())?;
    Ok(core::str::from_utf8(q.as_bytes())
        .expect("quoted identifier is server-encoding text")
        .to_string())
}

// The C PG_TRY body (extension.c:1322-1440): read the script, apply the
// \echo / @extowner@ / @extschema@ / MODULE_PATHNAME substitutions, execute.
fn run_script_body(
    mcx: Mcx<'_>,
    control: &ExtensionControlFile,
    filename: &str,
    switch_to_superuser: bool,
    save_userid: Oid,
    schema_name: &str,
    required_schemas: &[Oid],
) -> PgResult<()> {
    let c_sql = read_extension_script_file(control, filename)?;

    // textregexreplace(t_sql, "^\\echo.*$", "", "ng"): blank every line whose
    // text begins with \echo, keeping the newline (REG_NEWLINE + global).
    let mut t_sql = strip_echo_lines(&c_sql);

    if c_sql.contains("@extowner@") {
        let uid = if switch_to_superuser { save_userid } else { miscinit::GetUserId() };
        let user_name = miscinit::GetUserNameFromId(mcx, uid, false)?
            .expect("noerr=false returns a name")
            .as_str()
            .to_string();
        let q_user_name = quote_ident(mcx, &user_name)?;
        t_sql = t_sql.replace("@extowner@", q_user_name.as_str());
        if strpbrk(&user_name, QUOTING_RELEVANT_CHARS) {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_TEXT_REPRESENTATION)
                .errmsg(format!(
                    "invalid character in extension owner: must not contain any of \"{QUOTING_RELEVANT_CHARS}\""
                ))
                .into_error()
                .into());
        }
    }

    if !control.relocatable {
        let q_schema_name = quote_ident(mcx, schema_name)?;
        let replaced = t_sql.replace("@extschema@", q_schema_name.as_str());
        let changed = replaced != t_sql;
        t_sql = replaced;
        if changed && strpbrk(schema_name, QUOTING_RELEVANT_CHARS) {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_TEXT_REPRESENTATION)
                .errmsg(format!(
                    "invalid character in extension \"{}\" schema: must not contain any of \"{QUOTING_RELEVANT_CHARS}\"",
                    control.name
                ))
                .into_error()
                .into());
        }
    }

    debug_assert_eq!(control.requires.len(), required_schemas.len());
    for (reqextname, &reqschema) in control.requires.iter().zip(required_schemas.iter()) {
        let req_schema_name = lsyscache::get_namespace_name(mcx, reqschema)?
            .map(|s| s.as_str().to_string())
            .unwrap_or_default();
        let q_schema_name = quote_ident(mcx, &req_schema_name)?;
        let repltoken = format!("@extschema:{reqextname}@");
        let replaced = t_sql.replace(&repltoken, q_schema_name.as_str());
        let changed = replaced != t_sql;
        t_sql = replaced;
        if changed && strpbrk(&req_schema_name, QUOTING_RELEVANT_CHARS) {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_TEXT_REPRESENTATION)
                .errmsg(format!(
                    "invalid character in extension \"{reqextname}\" schema: must not contain any of \"{QUOTING_RELEVANT_CHARS}\""
                ))
                .into_error()
                .into());
        }
    }

    if let Some(module_pathname) = &control.module_pathname {
        t_sql = t_sql.replace("MODULE_PATHNAME", module_pathname);
    }

    execute_sql_string(&t_sql, filename)
}

fn strip_echo_lines(sql: &str) -> String {
    if !sql.contains("\\echo") {
        return sql.to_string();
    }
    let mut out = String::with_capacity(sql.len());
    for line in sql.split_inclusive('\n') {
        let content_end = line.len()
            - if line.ends_with("\r\n") {
                2
            } else if line.ends_with('\n') {
                1
            } else {
                0
            };
        if line[..content_end].starts_with("\\echo") {
            // REG_NEWLINE `.*$` consumed the content and any \r; the newline
            // stays.
            if line.ends_with('\n') {
                out.push('\n');
            }
        } else {
            out.push_str(line);
        }
    }
    out
}
