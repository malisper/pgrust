//! Read-only command classification + the read-only / parallel / recovery /
//! security guards (utility.c:93-467).

use utility_out_seams as rt;
use ::utils_error::ereport;
use ::types_error::{
    PgResult, ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INVALID_TRANSACTION_STATE,
    ERRCODE_READ_ONLY_SQL_TRANSACTION, ERROR, WARNING,
};
use ::nodes::nodes::Node;
use ::nodes::nodeindexscan::PlannedStmt;
use ::nodes::nodes::{CMD_DELETE, CMD_INSERT, CMD_MERGE, CMD_SELECT, CMD_UPDATE, CMD_UTILITY};

use crate::consts::*;

/// `CommandIsReadOnly` (utility.c:93-120) — is an executable query read-only?
///
/// This is a much stricter test than we apply for `XactReadOnly` mode; the query
/// must be *in truth* read-only, because the caller wishes not to do
/// `CommandCounterIncrement` for it.
pub fn CommandIsReadOnly(pstmt: &PlannedStmt) -> PgResult<bool> {
    match pstmt.commandType {
        CMD_SELECT => {
            if pstmt.rowMarks.as_ref().is_some_and(|m| !m.is_empty()) {
                Ok(false) // SELECT FOR [KEY] UPDATE/SHARE
            } else if pstmt.hasModifyingCTE {
                Ok(false) // data-modifying CTE
            } else {
                Ok(true)
            }
        }
        CMD_UPDATE | CMD_INSERT | CMD_DELETE | CMD_MERGE => Ok(false),
        CMD_UTILITY => {
            // For now, treat all utility commands as read/write
            Ok(false)
        }
        other => {
            // elog(WARNING, ...) returns Ok(()); fall through to `return false`.
            ::utils_error::elog(
                WARNING,
                format!("unrecognized commandType: {}", other as i32),
            )?;
            Ok(false)
        }
    }
}

/// `ClassifyUtilityCommandAsReadOnly` (utility.c:127-395, static) — classify a
/// utility parse tree's read-only status (`COMMAND_*` flags).
pub fn ClassifyUtilityCommandAsReadOnly(parsetree: &Node) -> PgResult<i32> {
    use ::nodes::nodes as ntag;
    use ::nodes::ddlnodes::TransactionStmtKind::*;

    let flags = match parsetree.node_tag() {
        // DDL is not read-only, and neither is TRUNCATE.
        t if t == ntag::T_AlterCollationStmt
            || t == ntag::T_AlterDatabaseRefreshCollStmt
            || t == ntag::T_AlterDatabaseSetStmt
            || t == ntag::T_AlterDatabaseStmt
            || t == ntag::T_AlterDefaultPrivilegesStmt
            || t == ntag::T_AlterDomainStmt
            || t == ntag::T_AlterEnumStmt
            || t == ntag::T_AlterEventTrigStmt
            || t == ntag::T_AlterExtensionContentsStmt
            || t == ntag::T_AlterExtensionStmt
            || t == ntag::T_AlterFdwStmt
            || t == ntag::T_AlterForeignServerStmt
            || t == ntag::T_AlterFunctionStmt
            || t == ntag::T_AlterObjectDependsStmt
            || t == ntag::T_AlterObjectSchemaStmt
            || t == ntag::T_AlterOpFamilyStmt
            || t == ntag::T_AlterOperatorStmt
            || t == ntag::T_AlterOwnerStmt
            || t == ntag::T_AlterPolicyStmt
            || t == ntag::T_AlterPublicationStmt
            || t == ntag::T_AlterRoleSetStmt
            || t == ntag::T_AlterRoleStmt
            || t == ntag::T_AlterSeqStmt
            || t == ntag::T_AlterStatsStmt
            || t == ntag::T_AlterSubscriptionStmt
            || t == ntag::T_AlterTSConfigurationStmt
            || t == ntag::T_AlterTSDictionaryStmt
            || t == ntag::T_AlterTableMoveAllStmt
            || t == ntag::T_AlterTableSpaceOptionsStmt
            || t == ntag::T_AlterTableStmt
            || t == ntag::T_AlterTypeStmt
            || t == ntag::T_AlterUserMappingStmt
            || t == ntag::T_CommentStmt
            || t == ntag::T_CompositeTypeStmt
            || t == ntag::T_CreateAmStmt
            || t == ntag::T_CreateCastStmt
            || t == ntag::T_CreateConversionStmt
            || t == ntag::T_CreateDomainStmt
            || t == ntag::T_CreateEnumStmt
            || t == ntag::T_CreateEventTrigStmt
            || t == ntag::T_CreateExtensionStmt
            || t == ntag::T_CreateFdwStmt
            || t == ntag::T_CreateForeignServerStmt
            || t == ntag::T_CreateForeignTableStmt
            || t == ntag::T_CreateFunctionStmt
            || t == ntag::T_CreateOpClassStmt
            || t == ntag::T_CreateOpFamilyStmt
            || t == ntag::T_CreatePLangStmt
            || t == ntag::T_CreatePolicyStmt
            || t == ntag::T_CreatePublicationStmt
            || t == ntag::T_CreateRangeStmt
            || t == ntag::T_CreateRoleStmt
            || t == ntag::T_CreateSchemaStmt
            || t == ntag::T_CreateSeqStmt
            || t == ntag::T_CreateStatsStmt
            || t == ntag::T_CreateStmt
            || t == ntag::T_CreateSubscriptionStmt
            || t == ntag::T_CreateTableAsStmt
            || t == ntag::T_CreateTableSpaceStmt
            || t == ntag::T_CreateTransformStmt
            || t == ntag::T_CreateTrigStmt
            || t == ntag::T_CreateUserMappingStmt
            || t == ntag::T_CreatedbStmt
            || t == ntag::T_DefineStmt
            || t == ntag::T_DropOwnedStmt
            || t == ntag::T_DropRoleStmt
            || t == ntag::T_DropStmt
            || t == ntag::T_DropSubscriptionStmt
            || t == ntag::T_DropTableSpaceStmt
            || t == ntag::T_DropUserMappingStmt
            || t == ntag::T_DropdbStmt
            || t == ntag::T_GrantRoleStmt
            || t == ntag::T_GrantStmt
            || t == ntag::T_ImportForeignSchemaStmt
            || t == ntag::T_IndexStmt
            || t == ntag::T_ReassignOwnedStmt
            || t == ntag::T_RefreshMatViewStmt
            || t == ntag::T_RenameStmt
            || t == ntag::T_RuleStmt
            || t == ntag::T_SecLabelStmt
            || t == ntag::T_TruncateStmt
            || t == ntag::T_ViewStmt =>
        {
            COMMAND_IS_NOT_READ_ONLY
        }

        // Surprisingly, ALTER SYSTEM meets all our definitions of read-only.
        t if t == ntag::T_AlterSystemStmt => COMMAND_IS_STRICTLY_READ_ONLY,

        // Commands inside the DO block or the called procedure might not be read
        // only, but they'll be checked separately when we try to execute them.
        t if t == ntag::T_CallStmt || t == ntag::T_DoStmt => COMMAND_IS_STRICTLY_READ_ONLY,

        // A CHECKPOINT command during recovery is interpreted as a request for a
        // restartpoint instead.
        t if t == ntag::T_CheckPointStmt => COMMAND_IS_STRICTLY_READ_ONLY,

        // Modify only backend-local state: OK in read-only txn / on a standby, but
        // disallowed in parallel mode.
        t if t == ntag::T_ClosePortalStmt
            || t == ntag::T_ConstraintsSetStmt
            || t == ntag::T_DeallocateStmt
            || t == ntag::T_DeclareCursorStmt
            || t == ntag::T_DiscardStmt
            || t == ntag::T_ExecuteStmt
            || t == ntag::T_FetchStmt
            || t == ntag::T_LoadStmt
            || t == ntag::T_PrepareStmt
            || t == ntag::T_UnlistenStmt
            || t == ntag::T_VariableSetStmt =>
        {
            COMMAND_OK_IN_RECOVERY | COMMAND_OK_IN_READ_ONLY_TXN
        }

        // Write WAL (not strictly read-only, no parallel workers) but don't affect
        // pg_dump output, so OK in a read-only transaction.
        t if t == ntag::T_ClusterStmt
            || t == ntag::T_ReindexStmt
            || t == ntag::T_VacuumStmt =>
        {
            COMMAND_OK_IN_READ_ONLY_TXN
        }

        t if t == ntag::T_CopyStmt => {
            let stmt = parsetree.expect_copystmt();
            // COPY FROM into a temp table doesn't change pg_dump output; DoCopy
            // calls PreventCommandIfReadOnly itself for non-temp targets.
            if stmt.is_from {
                COMMAND_OK_IN_READ_ONLY_TXN
            } else {
                COMMAND_IS_STRICTLY_READ_ONLY
            }
        }

        // Don't modify any data and are safe to run in a parallel worker.
        t if t == ntag::T_ExplainStmt || t == ntag::T_VariableShowStmt => {
            COMMAND_IS_STRICTLY_READ_ONLY
        }

        // NOTIFY requires an XID assignment, so can't be permitted on a standby;
        // LISTEN is prohibited lest the user get the wrong idea.
        t if t == ntag::T_ListenStmt || t == ntag::T_NotifyStmt => COMMAND_OK_IN_READ_ONLY_TXN,

        t if t == ntag::T_LockStmt => {
            let stmt = parsetree.expect_lockstmt();
            // Only weaker locker modes are allowed during recovery (must match
            // LockAcquireExtended()).
            if stmt.mode > ROW_EXCLUSIVE_LOCK {
                COMMAND_OK_IN_READ_ONLY_TXN
            } else {
                COMMAND_IS_STRICTLY_READ_ONLY
            }
        }

        t if t == ntag::T_TransactionStmt => {
            let stmt = parsetree.expect_transactionstmt();
            match stmt.kind {
                TRANS_STMT_BEGIN | TRANS_STMT_START | TRANS_STMT_COMMIT | TRANS_STMT_ROLLBACK
                | TRANS_STMT_SAVEPOINT | TRANS_STMT_RELEASE | TRANS_STMT_ROLLBACK_TO => {
                    COMMAND_IS_STRICTLY_READ_ONLY
                }

                TRANS_STMT_PREPARE | TRANS_STMT_COMMIT_PREPARED | TRANS_STMT_ROLLBACK_PREPARED => {
                    COMMAND_OK_IN_READ_ONLY_TXN
                }
            }
        }

        other => {
            return Err(ereport(ERROR)
                .errmsg_internal(format!("unrecognized node type: {}", other.0))
                .into_error());
        }
    };

    Ok(flags)
}

/// `PreventCommandIfReadOnly` (utility.c:403-412) — throw an error if
/// `XactReadOnly`.
pub fn PreventCommandIfReadOnly(cmdname: &str) -> PgResult<()> {
    if rt::xact_read_only::call() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION)
            .errmsg(format!(
                "cannot execute {cmdname} in a read-only transaction"
            ))
            .into_error());
    }
    Ok(())
}

/// `PreventCommandIfParallelMode` (utility.c:421-430) — throw an error if the
/// current (sub)transaction is in parallel mode.
pub fn PreventCommandIfParallelMode(cmdname: &str) -> PgResult<()> {
    if rt::is_in_parallel_mode::call() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_TRANSACTION_STATE)
            .errmsg(format!(
                "cannot execute {cmdname} during a parallel operation"
            ))
            .into_error());
    }
    Ok(())
}

/// `PreventCommandDuringRecovery` (utility.c:440-449) — throw an error if
/// `RecoveryInProgress`.
pub fn PreventCommandDuringRecovery(cmdname: &str) -> PgResult<()> {
    if rt::recovery_in_progress::call() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION)
            .errmsg(format!("cannot execute {cmdname} during recovery"))
            .into_error());
    }
    Ok(())
}

/// `CheckRestrictedOperation` (utility.c:458-467, static) — throw an error for a
/// hazardous command inside a security-restriction context.
pub fn CheckRestrictedOperation(cmdname: &str) -> PgResult<()> {
    if rt::in_security_restricted_operation::call() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!(
                "cannot execute {cmdname} within security-restricted operation"
            ))
            .into_error());
    }
    Ok(())
}
