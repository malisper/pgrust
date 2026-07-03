use types_error::{PgResult, ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_READ_ONLY_SQL_TRANSACTION, ERROR, WARNING};
use types_nodes::node_tree::Node;
use types_nodes::nodes_enums::CmdType;
use types_nodes::parsenodes::TransactionStmtKind::*;
use types_nodes::plannodes::PlannedStmt;
use types_nodes::NodeTag;

use crate::consts::*;
use crate::{loc, payload_gap};

pub fn CommandIsReadOnly(pstmt: &PlannedStmt<'_>) -> bool {
    match pstmt.commandType {
        CmdType::CMD_SELECT => {
            if pstmt.rowMarks.len() != 0 {
                false
            } else {
                !pstmt.hasModifyingCTE
            }
        }
        CmdType::CMD_UPDATE | CmdType::CMD_INSERT | CmdType::CMD_DELETE | CmdType::CMD_MERGE => {
            false
        }
        CmdType::CMD_UTILITY => false,
        other => {
            let _ = ::elog::ereport(WARNING)
                .errmsg(format!("unrecognized commandType: {}", other as i32))
                .finish(loc("CommandIsReadOnly"));
            false
        }
    }
}

pub fn ClassifyUtilityCommandAsReadOnly(parsetree: Node<'_>) -> PgResult<i32> {
    use NodeTag::*;
    let flags = match parsetree.node_tag() {
        T_AlterCollationStmt
        | T_AlterDatabaseRefreshCollStmt
        | T_AlterDatabaseSetStmt
        | T_AlterDatabaseStmt
        | T_AlterDefaultPrivilegesStmt
        | T_AlterDomainStmt
        | T_AlterEnumStmt
        | T_AlterEventTrigStmt
        | T_AlterExtensionContentsStmt
        | T_AlterExtensionStmt
        | T_AlterFdwStmt
        | T_AlterForeignServerStmt
        | T_AlterFunctionStmt
        | T_AlterObjectDependsStmt
        | T_AlterObjectSchemaStmt
        | T_AlterOpFamilyStmt
        | T_AlterOperatorStmt
        | T_AlterOwnerStmt
        | T_AlterPolicyStmt
        | T_AlterPublicationStmt
        | T_AlterRoleSetStmt
        | T_AlterRoleStmt
        | T_AlterSeqStmt
        | T_AlterStatsStmt
        | T_AlterSubscriptionStmt
        | T_AlterTSConfigurationStmt
        | T_AlterTSDictionaryStmt
        | T_AlterTableMoveAllStmt
        | T_AlterTableSpaceOptionsStmt
        | T_AlterTableStmt
        | T_AlterTypeStmt
        | T_AlterUserMappingStmt
        | T_CommentStmt
        | T_CompositeTypeStmt
        | T_CreateAmStmt
        | T_CreateCastStmt
        | T_CreateConversionStmt
        | T_CreateDomainStmt
        | T_CreateEnumStmt
        | T_CreateEventTrigStmt
        | T_CreateExtensionStmt
        | T_CreateFdwStmt
        | T_CreateForeignServerStmt
        | T_CreateForeignTableStmt
        | T_CreateFunctionStmt
        | T_CreateOpClassStmt
        | T_CreateOpFamilyStmt
        | T_CreatePLangStmt
        | T_CreatePolicyStmt
        | T_CreatePublicationStmt
        | T_CreateRangeStmt
        | T_CreateRoleStmt
        | T_CreateSchemaStmt
        | T_CreateSeqStmt
        | T_CreateStatsStmt
        | T_CreateStmt
        | T_CreateSubscriptionStmt
        | T_CreateTableAsStmt
        | T_CreateTableSpaceStmt
        | T_CreateTransformStmt
        | T_CreateTrigStmt
        | T_CreateUserMappingStmt
        | T_CreatedbStmt
        | T_DefineStmt
        | T_DropOwnedStmt
        | T_DropRoleStmt
        | T_DropStmt
        | T_DropSubscriptionStmt
        | T_DropTableSpaceStmt
        | T_DropUserMappingStmt
        | T_DropdbStmt
        | T_GrantRoleStmt
        | T_GrantStmt
        | T_ImportForeignSchemaStmt
        | T_IndexStmt
        | T_ReassignOwnedStmt
        | T_RefreshMatViewStmt
        | T_RenameStmt
        | T_RuleStmt
        | T_SecLabelStmt
        | T_TruncateStmt
        | T_ViewStmt => COMMAND_IS_NOT_READ_ONLY,

        T_AlterSystemStmt => COMMAND_IS_STRICTLY_READ_ONLY,

        T_CallStmt | T_DoStmt => COMMAND_IS_STRICTLY_READ_ONLY,

        T_CheckPointStmt => COMMAND_IS_STRICTLY_READ_ONLY,

        T_ClosePortalStmt
        | T_ConstraintsSetStmt
        | T_DeallocateStmt
        | T_DeclareCursorStmt
        | T_DiscardStmt
        | T_ExecuteStmt
        | T_FetchStmt
        | T_LoadStmt
        | T_PrepareStmt
        | T_UnlistenStmt
        | T_VariableSetStmt => COMMAND_OK_IN_RECOVERY | COMMAND_OK_IN_READ_ONLY_TXN,

        T_ClusterStmt | T_ReindexStmt | T_VacuumStmt => COMMAND_OK_IN_READ_ONLY_TXN,

        // COPY FROM into a temp table is fine read-only; DoCopy itself calls
        // PreventCommandIfReadOnly for non-temp targets.
        T_CopyStmt => {
            if parsetree.as_copy_stmt().unwrap().is_from {
                COMMAND_OK_IN_READ_ONLY_TXN
            } else {
                COMMAND_IS_STRICTLY_READ_ONLY
            }
        }

        T_ExplainStmt | T_VariableShowStmt => COMMAND_IS_STRICTLY_READ_ONLY,

        T_ListenStmt | T_NotifyStmt => COMMAND_OK_IN_READ_ONLY_TXN,

        // Only weaker lock modes are allowed during recovery (must match
        // LockAcquireExtended's restrictions).
        T_LockStmt => {
            let stmt = parsetree.as_lock_stmt().unwrap();
            if stmt.mode > types_storage::RowExclusiveLock {
                COMMAND_OK_IN_READ_ONLY_TXN
            } else {
                COMMAND_IS_STRICTLY_READ_ONLY
            }
        }

        T_TransactionStmt => {
            let stmt = parsetree.as_transaction_stmt().unwrap();
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
            return Err(::elog::ereport(ERROR)
                .errmsg_internal(format!("unrecognized node type: {}", other as u16))
                .into_error()
                .into());
        }
    };
    Ok(flags)
}

pub fn PreventCommandDuringRecovery(cmdname: &str) -> PgResult<()> {
    if transam_xlog::RecoveryInProgress() {
        return Err(::elog::ereport(ERROR)
            .errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION)
            .errmsg(format!("cannot execute {cmdname} during recovery"))
            .into_error()
            .into());
    }
    Ok(())
}

pub fn CheckRestrictedOperation(cmdname: &str) -> PgResult<()> {
    if miscinit::InSecurityRestrictedOperation() {
        return Err(::elog::ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!(
                "cannot execute {cmdname} within security-restricted operation"
            ))
            .into_error()
            .into());
    }
    Ok(())
}
