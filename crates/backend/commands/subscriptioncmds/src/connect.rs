// The publisher-connection legs of subscriptioncmds.c: check_publications,
// check_publications_origin, fetch_table_list, and the walrcv_create_slot /
// walrcv_drop_slot wrappers — all speaking over walreceiver::client's
// replication=database connection (libpqwalreceiver's walrcv_exec runs plain
// SQL through the walsender's simple-query fallthrough).
#![allow(non_snake_case)]

use mcx::Mcx;
use types_core::Oid;
use types_error::{
    PgError, PgResult, ERRCODE_CONNECTION_FAILURE, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INTERNAL_ERROR, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_UNDEFINED_OBJECT,
    LOG, WARNING,
};

use walreceiver::client::{ExecStatus, PgConn, QueryResult};

fn err(msg: String, sqlstate: types_error::SqlState) -> Box<PgError> {
    Box::new(PgError::error(msg).with_sqlstate(sqlstate))
}

fn row_text(r: &[Option<Vec<u8>>], i: usize) -> String {
    r.get(i)
        .and_then(|c| c.as_ref())
        .map(|b| String::from_utf8_lossy(b).into_owned())
        .unwrap_or_default()
}

// C stamps these per call site: fetch_table_list/check_publications_origin
// use ERRCODE_CONNECTION_FAILURE (subscriptioncmds.c:2150,2274) but
// check_publications raises a bare errmsg — XX000 (subscriptioncmds.c:465).
fn exec_or_fail(
    conn: &mut PgConn,
    cmd: &str,
    what: &str,
    sqlstate: types_error::SqlState,
) -> PgResult<QueryResult> {
    let res = conn.exec(cmd)?;
    if res.status != ExecStatus::TuplesOk && res.status != ExecStatus::CommandOk {
        return Err(err(format!("could not {what}: {}", res.err.clone()), sqlstate));
    }
    Ok(res)
}

// quote_literal_cstr (quote.c:103 -> quote_literal_internal:47): a backslash
// anywhere forces the E'' form with backslashes doubled, so a publisher
// running standard_conforming_strings = off decodes the name correctly.
pub(crate) fn quote_literal_cstr(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len() * 2 + 3);
    if raw.contains('\\') {
        out.push('E');
    }
    out.push('\'');
    for ch in raw.chars() {
        if ch == '\'' || ch == '\\' {
            out.push(ch);
        }
        out.push(ch);
    }
    out.push('\'');
    out
}

// GetPublicationsStr (pg_subscription.c:41) with quote_literal = true:
// comma-separated quote_literal_cstr renderings.
fn publications_str(publications: &[&str]) -> String {
    publications
        .iter()
        .map(|p| quote_literal_cstr(p))
        .collect::<Vec<_>>()
        .join(", ")
}

// check_publications (subscriptioncmds.c): WARN about missing publications.
pub(crate) fn check_publications(conn: &mut PgConn, publications: &[&str]) -> PgResult<()> {
    let cmd = format!(
        "SELECT t.pubname FROM pg_catalog.pg_publication t WHERE t.pubname IN ({})",
        publications_str(publications)
    );
    let res = exec_or_fail(
        conn,
        &cmd,
        "receive list of publications from the publisher",
        ERRCODE_INTERNAL_ERROR,
    )?;

    let found: Vec<String> = res.rows.iter().map(|r| row_text(r, 0)).collect();
    let missing: Vec<&&str> =
        publications.iter().filter(|p| !found.iter().any(|f| f == **p)).collect();
    if !missing.is_empty() {
        let list = missing.iter().map(|p| format!("\"{p}\"")).collect::<Vec<_>>().join(", ");
        elog::ereport(WARNING)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(if missing.len() == 1 {
                format!("publication {list} does not exist on the publisher")
            } else {
                format!("publications {list} do not exist on the publisher")
            })
            .finish(types_error::ErrorLocation::new(
                "src/backend/commands/subscriptioncmds.c",
                0,
                "check_publications",
            ))?;
    }
    Ok(())
}

// check_publications_origin (subscriptioncmds.c:2130): with origin = NONE
// (pg_strcasecmp, 2143) and copy_data, warn when the publisher itself
// subscribes to the same tables — or to a partition / ancestor of them
// (2149-2153) — excluding the relations already present locally
// (subrel_local_oids, the ALTER ... REFRESH arm, 2164-2178). No-op otherwise.
pub(crate) fn check_publications_origin(
    mcx: Mcx<'_>,
    conn: &mut PgConn,
    publications: &[&str],
    copydata: bool,
    origin: Option<&str>,
    subrel_local_oids: &[Oid],
    subname: &str,
) -> PgResult<()> {
    let Some(origin) = origin else {
        return Ok(());
    };
    if !copydata || !origin.eq_ignore_ascii_case(pg_subscription::LOGICALREP_ORIGIN_NONE) {
        return Ok(());
    }
    let mut cmd = format!(
        "SELECT DISTINCT P.pubname AS pubname\n\
         FROM pg_publication P,\n\
         LATERAL pg_get_publication_tables(P.pubname) GPT\n\
         JOIN pg_subscription_rel PS ON (GPT.relid = PS.srrelid OR \
         GPT.relid IN (SELECT relid FROM pg_partition_ancestors(PS.srrelid) UNION \
         SELECT relid FROM pg_partition_tree(PS.srrelid))),\n\
         pg_class C JOIN pg_namespace N ON (N.oid = C.relnamespace)\n\
         WHERE C.oid = GPT.relid AND P.pubname IN ({})\n",
        publications_str(publications)
    );
    for &relid in subrel_local_oids {
        let schemaname = lsyscache::misc::get_namespace_name(
            mcx,
            lsyscache::relation::get_rel_namespace(relid)?,
        )?
        .map(|s| s.as_str().to_string())
        .unwrap_or_default();
        let tablename = lsyscache::relation::get_rel_name(mcx, relid)?
            .map(|s| s.as_str().to_string())
            .unwrap_or_default();
        cmd.push_str(&format!(
            "AND NOT (N.nspname = {} AND C.relname = {})\n",
            quote_literal_cstr(&schemaname),
            quote_literal_cstr(&tablename)
        ));
    }
    let res = exec_or_fail(
        conn,
        &cmd,
        "receive list of replicated tables from the publisher",
        ERRCODE_CONNECTION_FAILURE,
    )?;
    // list_append_unique over the DISTINCT rows.
    let mut publist: Vec<String> = Vec::new();
    for r in &res.rows {
        let pubname = row_text(r, 0);
        if !publist.contains(&pubname) {
            publist.push(pubname);
        }
    }
    if !publist.is_empty() {
        // GetPublicationsStr(publist, pubnames, false): "name" renderings.
        let list = publist.iter().map(|p| format!("\"{p}\"")).collect::<Vec<_>>().join(", ");
        elog::ereport(WARNING)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(format!(
                "subscription \"{subname}\" requested copy_data with origin = NONE but might copy \
                 data that had a different origin"
            ))
            .errdetail_plural(
                format!(
                    "The subscription being created subscribes to a publication ({list}) that \
                     contains tables that are written to by other subscriptions."
                ),
                format!(
                    "The subscription being created subscribes to publications ({list}) that \
                     contain tables that are written to by other subscriptions."
                ),
                publist.len() as u64,
            )
            .errhint("Verify that initial data copied from the publisher tables did not come from other origins.")
            .finish(types_error::ErrorLocation::new(
                "src/backend/commands/subscriptioncmds.c",
                0,
                "check_publications_origin",
            ))?;
    }
    Ok(())
}

// C list_member(tablelist, rv): a second (nsp, rel) row means DISTINCT kept
// two attrs values — different column lists for the same table (0A000).
fn note_published_table(
    tablelist: &mut Vec<(String, String)>,
    nspname: String,
    relname: String,
) -> PgResult<()> {
    if tablelist
        .iter()
        .any(|(n, r)| n == &nspname && r == &relname)
    {
        return Err(err(
            format!(
                "cannot use different column lists for table \"{nspname}.{relname}\" in different \
                 publications"
            ),
            ERRCODE_FEATURE_NOT_SUPPORTED,
        ));
    }
    tablelist.push((nspname, relname));
    Ok(())
}

// fetch_table_list (subscriptioncmds.c), publisher >= 16 arm.
pub(crate) fn fetch_table_list(
    conn: &mut PgConn,
    publications: &[&str],
) -> PgResult<Vec<(String, String)>> {
    let cmd = format!(
        "SELECT DISTINCT n.nspname, c.relname, gpt.attrs\n       FROM pg_class c\n         \
         JOIN pg_namespace n ON n.oid = c.relnamespace\n         \
         JOIN ( SELECT (pg_get_publication_tables(VARIADIC array_agg(pubname::text))).*\n                \
         FROM pg_publication\n                WHERE pubname IN ( {} )) AS gpt\n             \
         ON gpt.relid = c.oid\n",
        publications_str(publications)
    );
    let res = exec_or_fail(
        conn,
        &cmd,
        "receive list of replicated tables from the publisher",
        ERRCODE_CONNECTION_FAILURE,
    )?;
    let mut tablelist = Vec::with_capacity(res.rows.len());
    for r in &res.rows {
        note_published_table(&mut tablelist, row_text(r, 0), row_text(r, 1))?;
    }
    Ok(tablelist)
}

// libpqrcv_create_slot (libpqwalreceiver.c), logical arm with CRS_NOEXPORT_SNAPSHOT.
pub(crate) fn walrcv_create_slot(
    conn: &mut PgConn,
    slotname: &str,
    two_phase: bool,
    failover: bool,
) -> PgResult<()> {
    let mut opts: Vec<&str> = vec!["SNAPSHOT 'nothing'"];
    if two_phase {
        opts.push("TWO_PHASE");
    }
    if failover {
        opts.push("FAILOVER");
    }
    let cmd = format!(
        "CREATE_REPLICATION_SLOT \"{}\" LOGICAL pgoutput ({})",
        slotname.replace('"', "\"\""),
        opts.join(", ")
    );
    let res = conn.exec(&cmd)?;
    if res.status != ExecStatus::TuplesOk {
        // libpqwalreceiver.c:1036: ERRCODE_PROTOCOL_VIOLATION.
        return Err(err(
            format!(
                "could not create replication slot \"{slotname}\": {}",
                res.err.clone()
            ),
            types_error::ERRCODE_PROTOCOL_VIOLATION,
        ));
    }
    // upstream a6a2eb9f6024 (18.6): Check CREATE_REPLICATION_SLOT response shape in libpqwalreceiver
    walreceiver::client::check_create_slot_result(&res, slotname)?;
    Ok(())
}

// ReplicationSlotDropAtPubNode (subscriptioncmds.c:1938): DROP_REPLICATION_SLOT
// on the publisher. With missing_ok, a 42704 (ERRCODE_UNDEFINED_OBJECT)
// failure is a server-log LOG line (1966-1972), never client-visible at the
// default client_min_messages.
pub(crate) fn drop_slot_at_pub_node(
    conn: &mut PgConn,
    slotname: &str,
    missing_ok: bool,
) -> PgResult<()> {
    let cmd = format!("DROP_REPLICATION_SLOT \"{}\" WAIT", slotname.replace('"', "\"\""));
    let res = conn.exec(&cmd)?;
    if res.status == ExecStatus::CommandOk || res.status == ExecStatus::TuplesOk {
        let _ = elog::elog(
            types_error::NOTICE,
            format!("dropped replication slot \"{slotname}\" on publisher"),
        );
        return Ok(());
    }
    let msg = res.err.clone();
    // res->sqlstate == ERRCODE_UNDEFINED_OBJECT (42704).
    let undefined_object = res.diag.as_ref().is_some_and(|d| d.sqlstate == "42704");
    if res.status == ExecStatus::Error && missing_ok && undefined_object {
        elog::ereport(LOG)
            .errmsg(format!("could not drop replication slot \"{slotname}\" on publisher: {msg}"))
            .finish(types_error::ErrorLocation::new(
                "src/backend/commands/subscriptioncmds.c",
                0,
                "ReplicationSlotDropAtPubNode",
            ))?;
        return Ok(());
    }
    Err(err(
        format!("could not drop replication slot \"{slotname}\" on publisher: {msg}"),
        ERRCODE_CONNECTION_FAILURE,
    ))
}

// walrcv_connect for the subscription path: the real client, logical mode.
pub(crate) fn connect(
    _mcx: Mcx<'_>,
    conninfo: &str,
    must_use_password: bool,
    appname: &str,
) -> PgResult<Result<PgConn, String>> {
    walreceiver::client::connect_extended(conninfo, true, true, must_use_password, appname)
}

// AlterSubscription_refresh (subscriptioncmds.c): diff the publisher's
// published-table set against pg_subscription_rel; add new tables (INIT when
// copy_data, READY otherwise), remove vanished ones (stop their sync workers,
// drop their origins and — for pre-SYNCDONE states — their tablesync slots on
// the publisher).
#[allow(non_snake_case)]
pub(crate) fn AlterSubscription_refresh<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    sub: &pg_subscription::Subscription<'_>,
    copy_data: bool,
    publications: &[&str],
    validate_publications: Option<&[&str]>,
) -> PgResult<()> {
    use types_error::DEBUG1;

    let must_use_password = sub.passwordrequired && !superuser::superuser_arg(sub.owner)?;
    let subname: &str = &sub.name;
    let mut wrconn = match connect(mcx, &sub.conninfo, must_use_password, subname)? {
        Ok(c) => c,
        Err(errmsg) => {
            return Err(err(
                format!("subscription \"{subname}\" could not connect to the publisher: {errmsg}"),
                ERRCODE_CONNECTION_FAILURE,
            ));
        }
    };

    // C 986-987: pg_subscription_rel is opened with AccessExclusiveLock before
    // the first removal and held until commit (1065 table_close NoLock), so a
    // concurrent lock holder blocks the refresh and the rel states cannot
    // change underneath it.
    let mut subrel_lock: Option<types_rel::Relation<'mcx>> = None;

    let refreshed = (|| -> PgResult<Vec<(types_core::Oid, u8)>> {
        if let Some(v) = validate_publications {
            check_publications(&mut wrconn, v)?;
        }

        let pubrels = fetch_table_list(&mut wrconn, publications)?;

        let subrel_states = pg_subscription::GetSubscriptionRelations(mcx, sub.oid, false)?;
        let mut subrel_local_oids: Vec<types_core::Oid> =
            subrel_states.iter().map(|r| r.relid).collect();
        subrel_local_oids.sort_unstable();

        check_publications_origin(
            mcx,
            &mut wrconn,
            publications,
            copy_data,
            Some(&sub.origin),
            &subrel_local_oids,
            subname,
        )?;

        // Add remote tables missing locally.
        let mut pubrel_local_oids: Vec<types_core::Oid> = Vec::with_capacity(pubrels.len());
        for (nspname, relname) in &pubrels {
            let rv = rel_vocab::RangeVar {
                catalogname: None,
                schemaname: Some(nspname.as_str()),
                relname: relname.as_str(),
                inh: true,
                relpersistence: b'p',
                location: -1,
            };
            let relid = catalog_namespace::RangeVarGetRelid(&rv, types_rel::AccessShareLock, false)?;
            crate::CheckSubscriptionRelkind(
                lsyscache::get_rel_relkind(relid)? as u8,
                nspname,
                relname,
            )?;
            pubrel_local_oids.push(relid);

            if subrel_local_oids.binary_search(&relid).is_err() {
                pg_subscription::AddSubscriptionRelState(
                    mcx,
                    sub.oid,
                    relid,
                    if copy_data {
                        pg_subscription::SUBREL_STATE_INIT
                    } else {
                        pg_subscription::SUBREL_STATE_READY
                    },
                    types_core::InvalidXLogRecPtr,
                    true,
                )?;
                let _ = elog::elog(
                    DEBUG1,
                    format!("table \"{nspname}.{relname}\" added to subscription \"{subname}\""),
                );
            }
        }

        // Remove local entries whose tables vanished from the publications.
        pubrel_local_oids.sort_unstable();
        let mut removed: Vec<(types_core::Oid, u8)> = Vec::new();
        for rstate in subrel_states.iter() {
            let relid = rstate.relid;
            if pubrel_local_oids.binary_search(&relid).is_ok() {
                continue;
            }
            if subrel_lock.is_none() {
                subrel_lock = Some(table::table_open(
                    mcx,
                    pg_subscription::SubscriptionRelRelationId,
                    types_rel::AccessExclusiveLock,
                )?);
            }
            let (state, _lsn) = pg_subscription::GetSubscriptionRelState(mcx, sub.oid, relid)?;
            removed.push((relid, state));
            pg_subscription::RemoveSubscriptionRel(mcx, sub.oid, relid)?;
            launcher::logicalrep_worker_stop(sub.oid, relid)?;
            if state != pg_subscription::SUBREL_STATE_READY {
                let originname = format!("pg_{}_{relid}", sub.oid);
                origin::replorigin_drop_by_name(mcx, &originname, true, false)?;
            }
            // C 1023-1027: get_namespace_name(get_rel_namespace(relid)) and
            // get_rel_name(relid).
            let nspname = lsyscache::get_namespace_name(mcx, lsyscache::get_rel_namespace(relid)?)?
                .map(|s| s.as_str().to_string())
                .unwrap_or_default();
            let relname = lsyscache::get_rel_name(mcx, relid)?
                .map(|s| s.as_str().to_string())
                .unwrap_or_default();
            let _ = elog::elog(
                DEBUG1,
                format!("table \"{nspname}.{relname}\" removed from subscription \"{subname}\""),
            );
        }
        Ok(removed)
    })();

    // Drop tablesync slots for removed pre-SYNCDONE tables last (C: cannot
    // roll back dropped slots).
    let result = match refreshed {
        Ok(removed) => {
            let mut r = Ok(());
            for (relid, state) in removed {
                if state != pg_subscription::SUBREL_STATE_READY
                    && state != pg_subscription::SUBREL_STATE_SYNCDONE
                {
                    // ReplicationSlotNameForTablesync (tablesync.c:1302); the
                    // canonical impl + format test live in logicalworker.
                    let syncslot = format!(
                        "pg_{}_sync_{relid}_{}",
                        sub.oid,
                        transam_xlog::control_file::GetSystemIdentifier()
                    );
                    if let Err(e) = drop_slot_at_pub_node(&mut wrconn, &syncslot, true) {
                        r = Err(e);
                        break;
                    }
                }
            }
            r
        }
        Err(e) => Err(e),
    };
    drop(wrconn);
    // C 1064-1065: table_close(rel, NoLock) — the AccessExclusiveLock is
    // held till the end of the transaction.
    if let Some(rel) = subrel_lock {
        rel.close(types_rel::NoLock)?;
    }
    result
}

// libpqrcv_alter_slot (libpqwalreceiver.c): ALTER_REPLICATION_SLOT with
// FAILOVER and/or TWO_PHASE options.
pub(crate) fn walrcv_alter_slot(
    conn: &mut PgConn,
    slotname: &str,
    failover: Option<bool>,
    two_phase: Option<bool>,
) -> PgResult<()> {
    let mut opts: Vec<String> = Vec::new();
    if let Some(f) = failover {
        opts.push(format!("FAILOVER {}", if f { "true" } else { "false" }));
    }
    if let Some(t) = two_phase {
        opts.push(format!("TWO_PHASE {}", if t { "true" } else { "false" }));
    }
    let cmd = format!(
        "ALTER_REPLICATION_SLOT \"{}\" ( {} );",
        slotname.replace('"', "\"\""),
        opts.join(", ")
    );
    let res = conn.exec(&cmd)?;
    if res.status != ExecStatus::CommandOk {
        return Err(err(
            format!(
                "could not alter replication slot \"{slotname}\": {}",
                res.err.clone()
            ),
            types_error::ERRCODE_PROTOCOL_VIOLATION,
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // fetch_table_list (subscriptioncmds.c:2295): DISTINCT (nsp, rel, attrs)
    // yields a second row only when column lists differ. Unfixed pgrust
    // skipped the check and later AddSubscriptionRelState'd XX000.
    #[test]
    fn different_column_lists_same_table_is_0a000() {
        let mut tablelist = Vec::new();
        note_published_table(&mut tablelist, "public".into(), "t".into()).unwrap();
        let err = note_published_table(&mut tablelist, "public".into(), "t".into()).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_FEATURE_NOT_SUPPORTED);
        assert_eq!(
            err.message(),
            "cannot use different column lists for table \"public.t\" in different publications"
        );

        note_published_table(&mut tablelist, "public".into(), "u".into()).unwrap();
        note_published_table(&mut tablelist, "other".into(), "t".into()).unwrap();
        assert_eq!(tablelist.len(), 3);
    }

    // quote_literal_cstr (quote.c:47-71): a backslash anywhere selects the
    // E'' form with backslashes doubled; quotes are always doubled. Unfixed
    // pgrust sent '<name>' with only quotes doubled, which a publisher at
    // standard_conforming_strings = off decoded as an escape string.
    #[test]
    fn publication_names_are_quote_literal_cstr() {
        assert_eq!(quote_literal_cstr("pub1"), "'pub1'");
        assert_eq!(quote_literal_cstr("it's"), "'it''s'");
        assert_eq!(quote_literal_cstr("p\\n"), "E'p\\\\n'");
        assert_eq!(quote_literal_cstr("a'\\b"), "E'a''\\\\b'");
        assert_eq!(quote_literal_cstr(""), "''");
        assert_eq!(publications_str(&["pub1", "p\\n"]), "'pub1', E'p\\\\n'");
    }
}
