// The publisher-connection legs of subscriptioncmds.c: check_publications,
// check_publications_origin, fetch_table_list, and the walrcv_create_slot /
// walrcv_drop_slot wrappers — all speaking over walreceiver::client's
// replication=database connection (libpqwalreceiver's walrcv_exec runs plain
// SQL through the walsender's simple-query fallthrough).
#![allow(non_snake_case)]

use mcx::Mcx;
use types_error::{PgError, PgResult, ERRCODE_CONNECTION_FAILURE, ERRCODE_UNDEFINED_OBJECT, WARNING};

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

fn exec_or_fail(conn: &mut PgConn, cmd: &str, what: &str) -> PgResult<QueryResult> {
    let res = conn.exec(cmd)?;
    if res.status != ExecStatus::TuplesOk && res.status != ExecStatus::CommandOk {
        return Err(err(
            format!("could not {what}: {}", res.err.clone()),
            ERRCODE_CONNECTION_FAILURE,
        ));
    }
    Ok(res)
}

// GetPublicationsStr (pg_publication.c): comma-separated quoted literals.
fn publications_str(publications: &[&str]) -> String {
    publications
        .iter()
        .map(|p| format!("'{}'", p.replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(", ")
}

// check_publications (subscriptioncmds.c): WARN about missing publications.
pub(crate) fn check_publications(conn: &mut PgConn, publications: &[&str]) -> PgResult<()> {
    let cmd = format!(
        "SELECT t.pubname FROM pg_catalog.pg_publication t WHERE t.pubname IN ({})",
        publications_str(publications)
    );
    let res = exec_or_fail(conn, &cmd, "receive list of publications from the publisher")?;

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

// check_publications_origin (subscriptioncmds.c): with origin=NONE and
// copy_data, warn when the publisher itself subscribes to the same tables
// (potential non-local origins in the initial copy). No-op otherwise, like C.
pub(crate) fn check_publications_origin(
    conn: &mut PgConn,
    publications: &[&str],
    copydata: bool,
    origin: Option<&str>,
    subname: &str,
) -> PgResult<()> {
    if !copydata || origin != Some("none") {
        return Ok(());
    }
    let cmd = format!(
        "SELECT DISTINCT P.pubname AS pubname FROM pg_publication P, LATERAL \
         pg_get_publication_tables(P.pubname) GPT JOIN pg_subscription_rel PS ON \
         (GPT.relid = PS.srrelid), pg_class C JOIN pg_namespace N ON (N.oid = \
         C.relnamespace) WHERE C.oid = GPT.relid AND P.pubname IN ({})",
        publications_str(publications)
    );
    let res = exec_or_fail(conn, &cmd, "receive list of replicated tables from the publisher")?;
    if !res.rows.is_empty() {
        let list =
            res.rows.iter().map(|r| format!("\"{}\"", row_text(r, 0))).collect::<Vec<_>>().join(", ");
        elog::ereport(WARNING)
            .errmsg(format!(
                "subscription \"{subname}\" requested copy_data with origin = NONE but might copy \
                 data that had a different origin"
            ))
            .errdetail(format!(
                "The subscription being created subscribes to a publication ({list}) that contains \
                 tables that are written to by other subscriptions."
            ))
            .errhint("Verify that initial data copied from the publisher tables did not come from other origins.")
            .finish(types_error::ErrorLocation::new(
                "src/backend/commands/subscriptioncmds.c",
                0,
                "check_publications_origin",
            ))?;
    }
    Ok(())
}

// fetch_table_list (subscriptioncmds.c), publisher >= 16 arm: schema/table
// pairs published by the given publications (column lists ignored until the
// column-list subscriber support lands; C reads gpt.attrs for a later check).
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
    let res = exec_or_fail(conn, &cmd, "receive list of replicated tables from the publisher")?;
    Ok(res.rows.iter().map(|r| (row_text(r, 0), row_text(r, 1))).collect())
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
        return Err(err(
            format!(
                "could not create replication slot \"{slotname}\": {}",
                res.err.clone()
            ),
            ERRCODE_CONNECTION_FAILURE,
        ));
    }
    Ok(())
}

// ReplicationSlotDropAtPubNode (subscriptioncmds.c): DROP_REPLICATION_SLOT on
// the publisher; missing_ok downgrades the error to a WARNING like C.
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
    if missing_ok && msg.contains("does not exist") {
        elog::ereport(WARNING)
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
            &mut wrconn,
            publications,
            copy_data,
            Some(&sub.origin),
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
            let (state, _lsn) = pg_subscription::GetSubscriptionRelState(mcx, sub.oid, relid)?;
            removed.push((relid, state));
            pg_subscription::RemoveSubscriptionRel(mcx, sub.oid, relid)?;
            launcher::logicalrep_worker_stop(sub.oid, relid)?;
            if state != pg_subscription::SUBREL_STATE_READY {
                let originname = format!("pg_{}_{relid}", sub.oid);
                origin::replorigin_drop_by_name(mcx, &originname, true, false)?;
            }
            let _ = elog::elog(
                DEBUG1,
                format!("table with OID {relid} removed from subscription \"{subname}\""),
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
