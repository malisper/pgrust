#![allow(non_snake_case)]

use types_error::PgResult;
use types_nodes::parsenodes::{DiscardMode, DiscardStmt};
use types_storage::lock::USER_LOCKMETHOD;

pub fn DiscardCommand(stmt: &DiscardStmt, is_top_level: bool) -> PgResult<()> {
    match stmt.target {
        DiscardMode::DISCARD_ALL => DiscardAll(is_top_level),
        DiscardMode::DISCARD_PLANS => {
            plancache::ResetPlanCache();
            Ok(())
        }
        DiscardMode::DISCARD_SEQUENCES => {
            sequence::ResetSequenceCaches();
            Ok(())
        }
        DiscardMode::DISCARD_TEMP => {
            catalog_namespace::ResetTempTableNamespace()?;
            Ok(())
        }
    }
}

fn DiscardAll(is_top_level: bool) -> PgResult<()> {
    xact::PreventInTransactionBlock(is_top_level, "DISCARD ALL")?;
    portalmem::PortalHashTableDeleteAll()?;
    guc_funcs::SetPGVariable("session_authorization", None, false)?;
    guc::ResetAllOptions();
    prepare::DropAllPreparedStatements()?;
    commands_async::Async_UnlistenAll()?;
    lock::LockReleaseAll(USER_LOCKMETHOD.into(), true)?;
    plancache::ResetPlanCache();
    catalog_namespace::ResetTempTableNamespace()?;
    sequence::ResetSequenceCaches();
    Ok(())
}
