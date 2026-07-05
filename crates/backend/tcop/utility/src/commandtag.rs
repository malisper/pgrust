use types_core::CommandTag;
use types_error::WARNING;
use types_nodes::node_tree::Node;
use types_nodes::nodes_enums::CmdType;
use types_nodes::parsenodes::{Query, TransactionStmtKind::*};
use types_nodes::plannodes::PlannedStmt;
use types_nodes::rawnodes::RawStmt;
use types_nodes::NodeTag;

use crate::consts::*;
use crate::{loc, payload_gap};

pub fn CreateCommandTag(parsetree: Node<'_>) -> CommandTag {
    use NodeTag::*;
    match parsetree.node_tag() {
        T_RawStmt => {
            let raw: &RawStmt<'_> = parsetree.as_variant().unwrap();
            CreateCommandTag(raw.stmt.expect("RawStmt.stmt is NULL"))
        }

        T_InsertStmt => CMDTAG_INSERT,
        T_DeleteStmt => CMDTAG_DELETE,
        T_UpdateStmt => CMDTAG_UPDATE,
        T_MergeStmt => CMDTAG_MERGE,
        T_SelectStmt => CMDTAG_SELECT,
        T_PLAssignStmt => CMDTAG_SELECT,

        T_TransactionStmt => {
            let stmt = parsetree.as_transaction_stmt().unwrap();
            match stmt.kind {
                TRANS_STMT_BEGIN => CMDTAG_BEGIN,
                TRANS_STMT_START => CMDTAG_START_TRANSACTION,
                TRANS_STMT_COMMIT => CMDTAG_COMMIT,
                TRANS_STMT_ROLLBACK | TRANS_STMT_ROLLBACK_TO => CMDTAG_ROLLBACK,
                TRANS_STMT_SAVEPOINT => CMDTAG_SAVEPOINT,
                TRANS_STMT_RELEASE => CMDTAG_RELEASE,
                TRANS_STMT_PREPARE => CMDTAG_PREPARE_TRANSACTION,
                TRANS_STMT_COMMIT_PREPARED => CMDTAG_COMMIT_PREPARED,
                TRANS_STMT_ROLLBACK_PREPARED => CMDTAG_ROLLBACK_PREPARED,
            }
        }

        T_DeclareCursorStmt => CMDTAG_DECLARE_CURSOR,
        T_ClosePortalStmt => {
            if parsetree.as_close_portal_stmt().unwrap().portalname.is_none() {
                CMDTAG_CLOSE_CURSOR_ALL
            } else {
                CMDTAG_CLOSE_CURSOR
            }
        }
        T_FetchStmt => {
            if parsetree.as_fetch_stmt().unwrap().ismove {
                CMDTAG_MOVE
            } else {
                CMDTAG_FETCH
            }
        }
        T_CreateDomainStmt => CMDTAG_CREATE_DOMAIN,
        T_CreateSchemaStmt => CMDTAG_CREATE_SCHEMA,
        T_CreateStmt => CMDTAG_CREATE_TABLE,
        T_CreateTableSpaceStmt => CMDTAG_CREATE_TABLESPACE,
        T_DropTableSpaceStmt => CMDTAG_DROP_TABLESPACE,
        T_AlterTableSpaceOptionsStmt => CMDTAG_ALTER_TABLESPACE,
        T_CreateExtensionStmt => CMDTAG_CREATE_EXTENSION,
        T_AlterExtensionStmt => CMDTAG_ALTER_EXTENSION,
        T_AlterExtensionContentsStmt => CMDTAG_ALTER_EXTENSION,
        T_CreateFdwStmt => CMDTAG_CREATE_FOREIGN_DATA_WRAPPER,
        T_AlterFdwStmt => CMDTAG_ALTER_FOREIGN_DATA_WRAPPER,
        T_CreateForeignServerStmt => CMDTAG_CREATE_SERVER,
        T_AlterForeignServerStmt => CMDTAG_ALTER_SERVER,
        T_CreateUserMappingStmt => CMDTAG_CREATE_USER_MAPPING,
        T_AlterUserMappingStmt => CMDTAG_ALTER_USER_MAPPING,
        T_DropUserMappingStmt => CMDTAG_DROP_USER_MAPPING,
        T_CreateForeignTableStmt => CMDTAG_CREATE_FOREIGN_TABLE,
        T_ImportForeignSchemaStmt => CMDTAG_IMPORT_FOREIGN_SCHEMA,
        T_DropStmt => {
            use types_nodes::parsenodes::ObjectType::*;
            match parsetree.as_drop_stmt().expect("T_DropStmt payload").removeType {
                OBJECT_TABLE => CMDTAG_DROP_TABLE,
                OBJECT_SEQUENCE => CMDTAG_DROP_SEQUENCE,
                OBJECT_VIEW => CMDTAG_DROP_VIEW,
                OBJECT_MATVIEW => CMDTAG_DROP_MATERIALIZED_VIEW,
                OBJECT_INDEX => CMDTAG_DROP_INDEX,
                OBJECT_TYPE => CMDTAG_DROP_TYPE,
                OBJECT_DOMAIN => CMDTAG_DROP_DOMAIN,
                OBJECT_COLLATION => CMDTAG_DROP_COLLATION,
                OBJECT_CONVERSION => CMDTAG_DROP_CONVERSION,
                OBJECT_SCHEMA => CMDTAG_DROP_SCHEMA,
                OBJECT_TSPARSER => CMDTAG_DROP_TEXT_SEARCH_PARSER,
                OBJECT_TSDICTIONARY => CMDTAG_DROP_TEXT_SEARCH_DICTIONARY,
                OBJECT_TSTEMPLATE => CMDTAG_DROP_TEXT_SEARCH_TEMPLATE,
                OBJECT_TSCONFIGURATION => CMDTAG_DROP_TEXT_SEARCH_CONFIGURATION,
                OBJECT_FOREIGN_TABLE => CMDTAG_DROP_FOREIGN_TABLE,
                OBJECT_EXTENSION => CMDTAG_DROP_EXTENSION,
                OBJECT_FUNCTION => CMDTAG_DROP_FUNCTION,
                OBJECT_PROCEDURE => CMDTAG_DROP_PROCEDURE,
                OBJECT_ROUTINE => CMDTAG_DROP_ROUTINE,
                OBJECT_AGGREGATE => CMDTAG_DROP_AGGREGATE,
                OBJECT_OPERATOR => CMDTAG_DROP_OPERATOR,
                OBJECT_LANGUAGE => CMDTAG_DROP_LANGUAGE,
                OBJECT_CAST => CMDTAG_DROP_CAST,
                OBJECT_TRIGGER => CMDTAG_DROP_TRIGGER,
                OBJECT_EVENT_TRIGGER => CMDTAG_DROP_EVENT_TRIGGER,
                OBJECT_RULE => CMDTAG_DROP_RULE,
                OBJECT_FDW => CMDTAG_DROP_FOREIGN_DATA_WRAPPER,
                OBJECT_FOREIGN_SERVER => CMDTAG_DROP_SERVER,
                OBJECT_OPCLASS => CMDTAG_DROP_OPERATOR_CLASS,
                OBJECT_OPFAMILY => CMDTAG_DROP_OPERATOR_FAMILY,
                OBJECT_POLICY => CMDTAG_DROP_POLICY,
                OBJECT_TRANSFORM => CMDTAG_DROP_TRANSFORM,
                OBJECT_ACCESS_METHOD => CMDTAG_DROP_ACCESS_METHOD,
                OBJECT_PUBLICATION => CMDTAG_DROP_PUBLICATION,
                OBJECT_STATISTIC_EXT => CMDTAG_DROP_STATISTICS,
                _ => CMDTAG_UNKNOWN,
            }
        }
        T_TruncateStmt => CMDTAG_TRUNCATE_TABLE,
        T_CommentStmt => CMDTAG_COMMENT,
        T_SecLabelStmt => CMDTAG_SECURITY_LABEL,
        T_CopyStmt => CMDTAG_COPY,
        // AlterObjectTypeCommandTag over renameType (relationType for columns).
        T_RenameStmt => {
            let stmt = parsetree
                .as_variant::<types_nodes::parsenodes::RenameStmt>()
                .expect("RenameStmt");
            let objtype = if stmt.renameType
                == types_nodes::parsenodes::ObjectType::OBJECT_COLUMN
                && stmt.relationType != types_nodes::parsenodes::ObjectType::OBJECT_TABLE
                && stmt.relationType as i32 != 0
            {
                stmt.relationType
            } else {
                stmt.renameType
            };
            alter_object_type_command_tag(objtype)
        }
        T_AlterObjectDependsStmt => payload_gap("CreateCommandTag", "AlterObjectDependsStmt"),
        T_AlterObjectSchemaStmt => {
            let stmt = parsetree
                .as_variant::<types_nodes::parsenodes::AlterObjectSchemaStmt>()
                .expect("AlterObjectSchemaStmt");
            alter_object_type_command_tag(stmt.objectType)
        }
        T_AlterOwnerStmt => {
            let stmt = parsetree.as_alter_owner_stmt().expect("AlterOwnerStmt");
            alter_object_type_command_tag(stmt.objectType)
        }
        T_AlterTableMoveAllStmt => {
            let stmt = parsetree
                .as_variant::<types_nodes::parsenodes::AlterTableMoveAllStmt>()
                .expect("AlterTableMoveAllStmt");
            alter_object_type_command_tag(stmt.objtype)
        }
        // AlterObjectTypeCommandTag over stmt->objtype.
        T_AlterTableStmt => {
            let stmt = parsetree
                .as_variant::<types_nodes::parsenodes::AlterTableStmt>()
                .expect("AlterTableStmt");
            alter_object_type_command_tag(stmt.objtype)
        }
        T_AlterDomainStmt => CMDTAG_ALTER_DOMAIN,
        T_AlterFunctionStmt => {
            let stmt = parsetree
                .as_alter_function_stmt()
                .expect("AlterFunctionStmt");
            match stmt.objtype {
                types_nodes::parsenodes::ObjectType::OBJECT_PROCEDURE => CMDTAG_ALTER_PROCEDURE,
                types_nodes::parsenodes::ObjectType::OBJECT_ROUTINE => CMDTAG_ALTER_ROUTINE,
                _ => CMDTAG_ALTER_FUNCTION,
            }
        }
        T_GrantStmt => {
            let stmt = parsetree
                .as_variant::<types_nodes::parsenodes::GrantStmt>()
                .expect("GrantStmt");
            if stmt.is_grant { CMDTAG_GRANT } else { CMDTAG_REVOKE }
        }
        T_GrantRoleStmt => {
            let stmt = parsetree
                .as_variant::<types_nodes::parsenodes::GrantRoleStmt>()
                .expect("GrantRoleStmt");
            if stmt.is_grant { CMDTAG_GRANT_ROLE } else { CMDTAG_REVOKE_ROLE }
        }
        T_AlterDefaultPrivilegesStmt => CMDTAG_ALTER_DEFAULT_PRIVILEGES,
        T_DefineStmt => {
            use types_nodes::parsenodes::ObjectType::*;
            let stmt = parsetree
                .as_variant::<types_nodes::parsenodes::DefineStmt>()
                .expect("DefineStmt");
            match stmt.kind {
                OBJECT_AGGREGATE => CMDTAG_CREATE_AGGREGATE,
                OBJECT_OPERATOR => CMDTAG_CREATE_OPERATOR,
                OBJECT_TYPE => CMDTAG_CREATE_TYPE,
                OBJECT_TSPARSER => CMDTAG_CREATE_TEXT_SEARCH_PARSER,
                OBJECT_TSDICTIONARY => CMDTAG_CREATE_TEXT_SEARCH_DICTIONARY,
                OBJECT_TSTEMPLATE => CMDTAG_CREATE_TEXT_SEARCH_TEMPLATE,
                OBJECT_TSCONFIGURATION => CMDTAG_CREATE_TEXT_SEARCH_CONFIGURATION,
                OBJECT_COLLATION => CMDTAG_CREATE_COLLATION,
                OBJECT_ACCESS_METHOD => CMDTAG_CREATE_ACCESS_METHOD,
                _ => payload_gap("CreateCommandTag", "DefineStmt"),
            }
        }
        T_CompositeTypeStmt => CMDTAG_CREATE_TYPE,
        T_CreateEnumStmt => CMDTAG_CREATE_TYPE,
        T_CreateRangeStmt => CMDTAG_CREATE_TYPE,
        T_AlterEnumStmt => CMDTAG_ALTER_TYPE,
        T_ViewStmt => CMDTAG_CREATE_VIEW,
        T_CreateFunctionStmt => {
            if parsetree.as_create_function_stmt().unwrap().is_procedure {
                CMDTAG_CREATE_PROCEDURE
            } else {
                CMDTAG_CREATE_FUNCTION
            }
        }
        T_IndexStmt => CMDTAG_CREATE_INDEX,
        T_RuleStmt => CMDTAG_CREATE_RULE,
        T_CreateSeqStmt => CMDTAG_CREATE_SEQUENCE,
        T_AlterSeqStmt => CMDTAG_ALTER_SEQUENCE,
        T_DoStmt => CMDTAG_DO,
        T_CreatedbStmt => CMDTAG_CREATE_DATABASE,
        T_AlterDatabaseStmt | T_AlterDatabaseRefreshCollStmt | T_AlterDatabaseSetStmt => {
            CMDTAG_ALTER_DATABASE
        }
        T_DropdbStmt => CMDTAG_DROP_DATABASE,
        T_NotifyStmt => CMDTAG_NOTIFY,
        T_ListenStmt => CMDTAG_LISTEN,
        T_UnlistenStmt => CMDTAG_UNLISTEN,
        T_LoadStmt => CMDTAG_LOAD,
        T_CallStmt => CMDTAG_CALL,
        T_ClusterStmt => CMDTAG_CLUSTER,
        T_VacuumStmt => {
            if parsetree.as_vacuum_stmt().unwrap().is_vacuumcmd {
                CMDTAG_VACUUM
            } else {
                CMDTAG_ANALYZE
            }
        }
        T_ExplainStmt => CMDTAG_EXPLAIN,
        T_CreateTableAsStmt => {
            let stmt = parsetree
                .as_variant::<types_nodes::rawnodes::CreateTableAsStmt>()
                .expect("CreateTableAsStmt");
            match stmt.objtype {
                types_nodes::parsenodes::ObjectType::OBJECT_TABLE => {
                    if stmt.is_select_into {
                        CMDTAG_SELECT_INTO
                    } else {
                        CMDTAG_CREATE_TABLE_AS
                    }
                }
                types_nodes::parsenodes::ObjectType::OBJECT_MATVIEW => {
                    CMDTAG_CREATE_MATERIALIZED_VIEW
                }
                other => panic!("unexpected CreateTableAsStmt.objtype {other:?}"),
            }
        }
        T_RefreshMatViewStmt => CMDTAG_REFRESH_MATERIALIZED_VIEW,
        T_AlterSystemStmt => CMDTAG_ALTER_SYSTEM,
        T_VariableSetStmt => {
            use types_nodes::parsenodes::VariableSetKind::*;
            match parsetree.as_variable_set_stmt().unwrap().kind {
                VAR_SET_VALUE | VAR_SET_CURRENT | VAR_SET_DEFAULT | VAR_SET_MULTI => CMDTAG_SET,
                VAR_RESET | VAR_RESET_ALL => CMDTAG_RESET,
            }
        }
        T_VariableShowStmt => CMDTAG_SHOW,
        T_DiscardStmt => {
            use types_nodes::parsenodes::DiscardMode::*;
            match parsetree.as_discard_stmt().unwrap().target {
                DISCARD_ALL => CMDTAG_DISCARD_ALL,
                DISCARD_PLANS => CMDTAG_DISCARD_PLANS,
                DISCARD_SEQUENCES => CMDTAG_DISCARD_SEQUENCES,
                DISCARD_TEMP => CMDTAG_DISCARD_TEMP,
            }
        }
        T_CreateTransformStmt => CMDTAG_CREATE_TRANSFORM,
        T_CreateTrigStmt => CMDTAG_CREATE_TRIGGER,
        T_CreateEventTrigStmt => CMDTAG_CREATE_EVENT_TRIGGER,
        T_AlterEventTrigStmt => CMDTAG_ALTER_EVENT_TRIGGER,
        T_CreatePLangStmt => CMDTAG_CREATE_LANGUAGE,
        T_CreateRoleStmt => CMDTAG_CREATE_ROLE,
        T_AlterRoleStmt => CMDTAG_ALTER_ROLE,
        T_AlterRoleSetStmt => CMDTAG_ALTER_ROLE,
        T_DropRoleStmt => CMDTAG_DROP_ROLE,
        T_DropOwnedStmt => CMDTAG_DROP_OWNED,
        T_ReassignOwnedStmt => CMDTAG_REASSIGN_OWNED,
        T_LockStmt => CMDTAG_LOCK_TABLE,
        T_ConstraintsSetStmt => CMDTAG_SET_CONSTRAINTS,
        T_CheckPointStmt => CMDTAG_CHECKPOINT,
        T_ReindexStmt => CMDTAG_REINDEX,
        T_CreateConversionStmt => CMDTAG_CREATE_CONVERSION,
        T_CreateCastStmt => CMDTAG_CREATE_CAST,
        T_CreateOpClassStmt => CMDTAG_CREATE_OPERATOR_CLASS,
        T_CreateOpFamilyStmt => CMDTAG_CREATE_OPERATOR_FAMILY,
        T_AlterOpFamilyStmt => CMDTAG_ALTER_OPERATOR_FAMILY,
        T_AlterOperatorStmt => CMDTAG_ALTER_OPERATOR,
        T_AlterTypeStmt => CMDTAG_ALTER_TYPE,
        T_AlterTSDictionaryStmt => CMDTAG_ALTER_TEXT_SEARCH_DICTIONARY,
        T_AlterTSConfigurationStmt => CMDTAG_ALTER_TEXT_SEARCH_CONFIGURATION,
        T_CreatePolicyStmt => CMDTAG_CREATE_POLICY,
        T_AlterPolicyStmt => CMDTAG_ALTER_POLICY,
        T_CreateAmStmt => CMDTAG_CREATE_ACCESS_METHOD,
        T_CreatePublicationStmt => CMDTAG_CREATE_PUBLICATION,
        T_AlterPublicationStmt => CMDTAG_ALTER_PUBLICATION,
        T_CreateSubscriptionStmt => CMDTAG_CREATE_SUBSCRIPTION,
        T_AlterSubscriptionStmt => CMDTAG_ALTER_SUBSCRIPTION,
        T_DropSubscriptionStmt => CMDTAG_DROP_SUBSCRIPTION,
        T_AlterCollationStmt => CMDTAG_ALTER_COLLATION,
        T_PrepareStmt => CMDTAG_PREPARE,
        T_ExecuteStmt => CMDTAG_EXECUTE,
        T_CreateStatsStmt => CMDTAG_CREATE_STATISTICS,
        T_AlterStatsStmt => CMDTAG_ALTER_STATISTICS,
        T_DeallocateStmt => {
            let stmt = parsetree.as_deallocate_stmt().unwrap();
            if stmt.isall {
                CMDTAG_DEALLOCATE_ALL
            } else {
                CMDTAG_DEALLOCATE
            }
        }

        T_PlannedStmt => {
            let stmt: &PlannedStmt<'_> = parsetree.as_variant().unwrap();
            tag_for_command_type(stmt.commandType, stmt.rowMarks.len() != 0, stmt.utilityStmt)
        }

        T_Query => {
            let stmt: &Query<'_> = parsetree.as_variant().unwrap();
            tag_for_command_type(stmt.commandType, stmt.rowMarks.len() != 0, stmt.utilityStmt)
        }

        other => {
            let _ = ::elog::ereport(WARNING)
                .errmsg(format!("unrecognized node type: {}", other as u16))
                .finish(loc("CreateCommandTag"));
            CMDTAG_UNKNOWN
        }
    }
}

// The shared CMD_* body of the T_PlannedStmt / T_Query arms. The rowMarks
// refinement (SELECT FOR ... variants) needs RowMarkClause/PlanRowMark, which
// the FOR UPDATE grammar lane owns.
fn tag_for_command_type(
    command_type: CmdType,
    has_row_marks: bool,
    utility_stmt: Option<Node<'_>>,
) -> CommandTag {
    match command_type {
        CmdType::CMD_SELECT => {
            if has_row_marks {
                payload_gap("CreateCommandTag", "RowMarkClause/PlanRowMark")
            } else {
                CMDTAG_SELECT
            }
        }
        CmdType::CMD_UPDATE => CMDTAG_UPDATE,
        CmdType::CMD_INSERT => CMDTAG_INSERT,
        CmdType::CMD_DELETE => CMDTAG_DELETE,
        CmdType::CMD_MERGE => CMDTAG_MERGE,
        CmdType::CMD_UTILITY => {
            CreateCommandTag(utility_stmt.expect("CMD_UTILITY with NULL utilityStmt"))
        }
        other => {
            let _ = ::elog::ereport(WARNING)
                .errmsg(format!("unrecognized commandType: {}", other as i32))
                .finish(loc("CreateCommandTag"));
            CMDTAG_UNKNOWN
        }
    }
}

// AlterObjectTypeCommandTag (utility.c); C's default arm is CMDTAG_UNKNOWN.
fn alter_object_type_command_tag(objtype: types_nodes::parsenodes::ObjectType) -> CommandTag {
    use types_nodes::parsenodes::ObjectType::*;
    match objtype {
        OBJECT_AGGREGATE => CMDTAG_ALTER_AGGREGATE,
        OBJECT_CAST => CMDTAG_ALTER_CAST,
        OBJECT_ATTRIBUTE | OBJECT_TYPE => CMDTAG_ALTER_TYPE,
        OBJECT_COLUMN | OBJECT_TABLE | OBJECT_TABCONSTRAINT => CMDTAG_ALTER_TABLE,
        OBJECT_COLLATION => CMDTAG_ALTER_COLLATION,
        OBJECT_CONVERSION => CMDTAG_ALTER_CONVERSION,
        OBJECT_DATABASE => CMDTAG_ALTER_DATABASE,
        OBJECT_DOMAIN | OBJECT_DOMCONSTRAINT => CMDTAG_ALTER_DOMAIN,
        OBJECT_EXTENSION => CMDTAG_ALTER_EXTENSION,
        OBJECT_EVENT_TRIGGER => CMDTAG_ALTER_EVENT_TRIGGER,
        OBJECT_FDW => CMDTAG_ALTER_FOREIGN_DATA_WRAPPER,
        OBJECT_FOREIGN_SERVER => CMDTAG_ALTER_SERVER,
        OBJECT_FOREIGN_TABLE => CMDTAG_ALTER_FOREIGN_TABLE,
        OBJECT_FUNCTION => CMDTAG_ALTER_FUNCTION,
        OBJECT_INDEX => CMDTAG_ALTER_INDEX,
        OBJECT_LANGUAGE => CMDTAG_ALTER_LANGUAGE,
        OBJECT_LARGEOBJECT => CMDTAG_ALTER_LARGE_OBJECT,
        OBJECT_MATVIEW => CMDTAG_ALTER_MATERIALIZED_VIEW,
        OBJECT_OPCLASS => CMDTAG_ALTER_OPERATOR_CLASS,
        OBJECT_OPERATOR => CMDTAG_ALTER_OPERATOR,
        OBJECT_OPFAMILY => CMDTAG_ALTER_OPERATOR_FAMILY,
        OBJECT_POLICY => CMDTAG_ALTER_POLICY,
        OBJECT_PROCEDURE => CMDTAG_ALTER_PROCEDURE,
        OBJECT_PUBLICATION => CMDTAG_ALTER_PUBLICATION,
        OBJECT_ROLE => CMDTAG_ALTER_ROLE,
        OBJECT_ROUTINE => CMDTAG_ALTER_ROUTINE,
        OBJECT_RULE => CMDTAG_ALTER_RULE,
        OBJECT_SCHEMA => CMDTAG_ALTER_SCHEMA,
        OBJECT_SEQUENCE => CMDTAG_ALTER_SEQUENCE,
        OBJECT_SUBSCRIPTION => CMDTAG_ALTER_SUBSCRIPTION,
        OBJECT_TABLESPACE => CMDTAG_ALTER_TABLESPACE,
        OBJECT_TRIGGER => CMDTAG_ALTER_TRIGGER,
        OBJECT_TSCONFIGURATION => CMDTAG_ALTER_TEXT_SEARCH_CONFIGURATION,
        OBJECT_TSDICTIONARY => CMDTAG_ALTER_TEXT_SEARCH_DICTIONARY,
        OBJECT_TSPARSER => CMDTAG_ALTER_TEXT_SEARCH_PARSER,
        OBJECT_TSTEMPLATE => CMDTAG_ALTER_TEXT_SEARCH_TEMPLATE,
        OBJECT_VIEW => CMDTAG_ALTER_VIEW,
        OBJECT_STATISTIC_EXT => CMDTAG_ALTER_STATISTICS,
        _ => CMDTAG_UNKNOWN,
    }
}
