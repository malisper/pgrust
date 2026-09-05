use guc_tables::consts::{LOGSTMT_ALL, LOGSTMT_DDL, LOGSTMT_MOD};
use types_error::WARNING;
use types_nodes::node_tree::Node;
use types_nodes::nodes_enums::CmdType;
use types_nodes::parsenodes::Query;
use types_nodes::plannodes::PlannedStmt;
use types_error::PgResult;
use types_nodes::rawnodes::{RawStmt, SelectStmt};
use types_nodes::NodeTag;

use crate::loc;

// GetCommandLogLevel (utility.c): C ereports out of the EXPLAIN option probe
// (defGetBoolean) and the EXECUTE look-through (FetchPreparedStatement), so
// the level is a PgResult here and check_log_statement propagates it.
pub fn GetCommandLogLevel(parsetree: Node<'_>) -> PgResult<i32> {
    use NodeTag::*;
    let lev = match parsetree.node_tag() {
        T_RawStmt => {
            let raw: &RawStmt<'_> = parsetree.as_variant().unwrap();
            GetCommandLogLevel(raw.stmt.expect("RawStmt.stmt is NULL"))?
        }

        T_InsertStmt | T_DeleteStmt | T_UpdateStmt | T_MergeStmt => LOGSTMT_MOD,

        T_SelectStmt => {
            let stmt: &SelectStmt<'_> = parsetree.as_variant().unwrap();
            if stmt.intoClause.is_some() {
                LOGSTMT_DDL
            } else {
                LOGSTMT_ALL
            }
        }

        T_PLAssignStmt => LOGSTMT_ALL,

        T_TransactionStmt
        | T_DeclareCursorStmt
        | T_ClosePortalStmt
        | T_FetchStmt
        | T_DeallocateStmt
        | T_DoStmt
        | T_NotifyStmt
        | T_ListenStmt
        | T_UnlistenStmt
        | T_LoadStmt
        | T_CallStmt
        | T_VacuumStmt
        | T_VariableSetStmt
        | T_VariableShowStmt
        | T_DiscardStmt
        | T_LockStmt
        | T_ConstraintsSetStmt
        | T_CheckPointStmt
        | T_ReindexStmt => LOGSTMT_ALL,

        T_PrepareStmt => {
            let stmt = parsetree.as_prepare_stmt().unwrap();
            GetCommandLogLevel(stmt.query.expect("PREPARE has a query"))?
        }

        T_ExecuteStmt => {
            // Look through an EXECUTE to the referenced stmt: the entry's
            // retained raw parse tree (plansource->raw_parse_tree), else
            // LOGSTMT_ALL (no such statement, or no raw tree retained).
            let stmt = parsetree.as_execute_stmt().unwrap();
            let ps = prepare::FetchPreparedStatement(stmt.name.unwrap_or(""), false)?;
            match ps.and_then(|ps| plancache::CachedPlanRawParseTree(ps.plansource)) {
                Some(raw) => GetCommandLogLevel(raw.stmt.expect("RawStmt.stmt is NULL"))?,
                None => LOGSTMT_ALL,
            }
        }

        T_ExplainStmt => {
            let stmt = parsetree.as_explain_stmt().unwrap();
            let mut analyze = false;
            // Look through an EXPLAIN ANALYZE to the contained stmt.
            for opt in stmt.options.iter() {
                let opt = opt.as_def_elem().expect("EXPLAIN options are DefElems");
                if opt.defname == Some("analyze") {
                    // C ereports (42601 "analyze requires a Boolean value")
                    // out of this probe, ahead of parse analysis.
                    analyze = explain::defGetBoolean(opt)?;
                }
                // don't break: explain.c will use the last value.
            }
            if analyze {
                GetCommandLogLevel(stmt.query.expect("ExplainStmt.query is NULL"))?
            } else {
                LOGSTMT_ALL
            }
        }

        T_CopyStmt => {
            if parsetree.as_copy_stmt().unwrap().is_from {
                LOGSTMT_MOD
            } else {
                LOGSTMT_ALL
            }
        }

        T_CreateSchemaStmt
        | T_CreateStmt
        | T_CreateForeignTableStmt
        | T_CreateTableSpaceStmt
        | T_DropTableSpaceStmt
        | T_AlterTableSpaceOptionsStmt
        | T_CreateExtensionStmt
        | T_AlterExtensionStmt
        | T_AlterExtensionContentsStmt
        | T_CreateFdwStmt
        | T_AlterFdwStmt
        | T_CreateForeignServerStmt
        | T_AlterForeignServerStmt
        | T_CreateUserMappingStmt
        | T_AlterUserMappingStmt
        | T_DropUserMappingStmt
        | T_ImportForeignSchemaStmt
        | T_DropStmt
        | T_CommentStmt
        | T_SecLabelStmt
        | T_RenameStmt
        | T_AlterObjectDependsStmt
        | T_AlterObjectSchemaStmt
        | T_AlterOwnerStmt
        | T_AlterOperatorStmt
        | T_AlterTypeStmt
        | T_AlterTableMoveAllStmt
        | T_AlterTableStmt
        | T_AlterDomainStmt
        | T_GrantStmt
        | T_GrantRoleStmt
        | T_AlterDefaultPrivilegesStmt
        | T_DefineStmt
        | T_CompositeTypeStmt
        | T_CreateEnumStmt
        | T_CreateRangeStmt
        | T_AlterEnumStmt
        | T_ViewStmt
        | T_CreateFunctionStmt
        | T_AlterFunctionStmt
        | T_IndexStmt
        | T_RuleStmt
        | T_CreateSeqStmt
        | T_AlterSeqStmt
        | T_CreatedbStmt
        | T_AlterDatabaseStmt
        | T_AlterDatabaseRefreshCollStmt
        | T_AlterDatabaseSetStmt
        | T_DropdbStmt
        | T_ClusterStmt
        | T_CreateTableAsStmt
        | T_RefreshMatViewStmt
        | T_AlterSystemStmt
        | T_CreateTrigStmt
        | T_CreateEventTrigStmt
        | T_AlterEventTrigStmt
        | T_CreatePLangStmt
        | T_CreateDomainStmt
        | T_CreateRoleStmt
        | T_AlterRoleStmt
        | T_AlterRoleSetStmt
        | T_DropRoleStmt
        | T_DropOwnedStmt
        | T_ReassignOwnedStmt
        | T_CreateConversionStmt
        | T_CreateCastStmt
        | T_CreateOpClassStmt
        | T_CreateOpFamilyStmt
        | T_CreateTransformStmt
        | T_AlterOpFamilyStmt
        | T_CreatePolicyStmt
        | T_AlterPolicyStmt
        | T_AlterTSDictionaryStmt
        | T_AlterTSConfigurationStmt
        | T_CreateAmStmt
        | T_CreatePublicationStmt
        | T_AlterPublicationStmt
        | T_CreateSubscriptionStmt
        | T_AlterSubscriptionStmt
        | T_DropSubscriptionStmt
        | T_CreateStatsStmt
        | T_AlterStatsStmt
        | T_AlterCollationStmt => LOGSTMT_DDL,

        T_TruncateStmt => LOGSTMT_MOD,

        T_PlannedStmt => {
            let stmt: &PlannedStmt<'_> = parsetree.as_variant().unwrap();
            level_for_command_type(stmt.commandType, stmt.utilityStmt)?
        }

        T_Query => {
            let stmt: &Query<'_> = parsetree.as_variant().unwrap();
            level_for_command_type(stmt.commandType, stmt.utilityStmt)?
        }

        other => {
            let _ = ::elog::ereport(WARNING)
                .errmsg(format!("unrecognized node type: {}", other as u16))
                .finish(loc("GetCommandLogLevel"));
            LOGSTMT_ALL
        }
    };
    Ok(lev)
}

fn level_for_command_type(command_type: CmdType, utility_stmt: Option<Node<'_>>) -> PgResult<i32> {
    match command_type {
        CmdType::CMD_SELECT => Ok(LOGSTMT_ALL),
        CmdType::CMD_UPDATE | CmdType::CMD_INSERT | CmdType::CMD_DELETE | CmdType::CMD_MERGE => {
            Ok(LOGSTMT_MOD)
        }
        CmdType::CMD_UTILITY => {
            GetCommandLogLevel(utility_stmt.expect("CMD_UTILITY with NULL utilityStmt"))
        }
        other => {
            let _ = ::elog::ereport(WARNING)
                .errmsg(format!("unrecognized commandType: {}", other as i32))
                .finish(loc("GetCommandLogLevel"));
            Ok(LOGSTMT_ALL)
        }
    }
}
