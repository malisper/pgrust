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
