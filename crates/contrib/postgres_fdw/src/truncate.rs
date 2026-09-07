//! postgresExecForeignTruncate (postgres_fdw.c:2996-3090): TRUNCATE of one
//! server's foreign tables, pushed to the remote as a single TRUNCATE.
use foreigncmds::foreign::{GetForeignServer, GetForeignTable, GetUserMapping};
use mcx::{Mcx, PgString};
use types_core::{InvalidOid, Oid};
use types_error::{PgError, PgResult, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE};
use types_nodes::parsenodes::DropBehavior;
use types_rel::Relation;

use crate::connection;
use crate::deparse;
use crate::modify::parse_bool;

/// postgresExecForeignTruncate (postgres_fdw.c:3000). `rels` all belong to one
/// foreign server (ExecuteTruncateGuts groups by serverid).
pub fn postgresExecForeignTruncate<'mcx>(
    mcx: Mcx<'mcx>,
    rels: &[&Relation<'mcx>],
    behavior: DropBehavior,
    restart_seqs: bool,
) -> PgResult<()> {
    let mut serverid: Oid = InvalidOid;
    let mut server_truncatable = true;

    // By default, all postgres_fdw foreign tables are assumed truncatable.
    // This can be overridden by a per-server setting, which in turn can be
    // overridden by a per-table setting (postgres_fdw.c:3011-3066).
    for rel in rels {
        let table = GetForeignTable(mcx, rel.rd_id)?;

        // First time through, determine whether the foreign server allows
        // truncates; all specified foreign tables belong to the same server.
        if serverid == InvalidOid {
            serverid = table.serverid;
            let server = GetForeignServer(mcx, serverid)?;
            for opt in server.options.iter() {
                if opt.name == "truncatable" {
                    server_truncatable = parse_bool(opt.require_value()?);
                    break;
                }
            }
        }
        debug_assert_eq!(table.serverid, serverid);

        // Determine whether this foreign table allows truncations.
        let mut truncatable = server_truncatable;
        for opt in table.options.iter() {
            if opt.name == "truncatable" {
                truncatable = parse_bool(opt.require_value()?);
                break;
            }
        }
        if !truncatable {
            return Err(Box::new(
                PgError::error(format!(
                    "foreign table \"{}\" does not allow truncates",
                    rel.name()
                ))
                .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            ));
        }
    }
    debug_assert_ne!(serverid, InvalidOid);

    // Get connection to the foreign server. Connection manager will establish
    // new connection if necessary (postgres_fdw.c:3073-3074).
    let user = GetUserMapping(mcx, miscinit::GetUserId(), serverid)?;
    let conn_key = connection::get_connection(mcx, &user, false)?;

    // Construct the TRUNCATE command string.
    let mut sql = PgString::new_in(mcx);
    deparse::deparse_truncate_sql(&mut sql, mcx, rels, behavior, restart_seqs)?;

    // Issue the TRUNCATE command to remote server (do_sql_command).
    let res = connection::exec_query(conn_key, sql.as_str())?;
    if res.status != pgclient::ExecStatus::CommandOk {
        return Err(connection::remote_error(&res, Some(sql.as_str())));
    }
    Ok(())
}
