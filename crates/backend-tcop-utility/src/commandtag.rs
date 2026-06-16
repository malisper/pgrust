//! Command-tag derivation (utility.c:2215-3236).
//!
//! Faithful port of `AlterObjectTypeCommandTag` (static, utility.c:2215-2350)
//! and `CreateCommandTag` (utility.c:2362-3236) reconciled to this repo's
//! split-crate node model.

use backend_utils_error::PgResult;
use types_error::WARNING;
use types_core::cmdtag::CommandTag;
use types_nodes::nodes::Node;
use types_nodes::parsenodes::*;
use types_nodes::rawnodes::{
    LockClauseStrength, LCS_FORKEYSHARE, LCS_FORNOKEYUPDATE, LCS_FORSHARE, LCS_FORUPDATE,
};
use types_nodes::ddlnodes::DiscardMode::*;
use types_nodes::ddlnodes::TransactionStmtKind::*;
use types_nodes::ddlnodes::VariableSetKind::*;
use types_nodes::nodes::{
    CMD_DELETE, CMD_INSERT, CMD_MERGE, CMD_SELECT, CMD_UPDATE, CMD_UTILITY,
};
use types_nodes::nodes::NodePtr;

use crate::consts::*;

/// `AlterObjectTypeCommandTag` (utility.c:2215-2350, static) — map an
/// `ObjectType` to the `ALTER …` command tag.
pub fn AlterObjectTypeCommandTag(objtype: ObjectType) -> CommandTag {
    match objtype {
        OBJECT_AGGREGATE => CMDTAG_ALTER_AGGREGATE,
        OBJECT_ATTRIBUTE => CMDTAG_ALTER_TYPE,
        OBJECT_CAST => CMDTAG_ALTER_CAST,
        OBJECT_COLLATION => CMDTAG_ALTER_COLLATION,
        OBJECT_COLUMN => CMDTAG_ALTER_TABLE,
        OBJECT_CONVERSION => CMDTAG_ALTER_CONVERSION,
        OBJECT_DATABASE => CMDTAG_ALTER_DATABASE,
        OBJECT_DOMAIN | OBJECT_DOMCONSTRAINT => CMDTAG_ALTER_DOMAIN,
        OBJECT_EXTENSION => CMDTAG_ALTER_EXTENSION,
        OBJECT_FDW => CMDTAG_ALTER_FOREIGN_DATA_WRAPPER,
        OBJECT_FOREIGN_SERVER => CMDTAG_ALTER_SERVER,
        OBJECT_FOREIGN_TABLE => CMDTAG_ALTER_FOREIGN_TABLE,
        OBJECT_FUNCTION => CMDTAG_ALTER_FUNCTION,
        OBJECT_INDEX => CMDTAG_ALTER_INDEX,
        OBJECT_LANGUAGE => CMDTAG_ALTER_LANGUAGE,
        OBJECT_LARGEOBJECT => CMDTAG_ALTER_LARGE_OBJECT,
        OBJECT_OPCLASS => CMDTAG_ALTER_OPERATOR_CLASS,
        OBJECT_OPERATOR => CMDTAG_ALTER_OPERATOR,
        OBJECT_OPFAMILY => CMDTAG_ALTER_OPERATOR_FAMILY,
        OBJECT_POLICY => CMDTAG_ALTER_POLICY,
        OBJECT_PROCEDURE => CMDTAG_ALTER_PROCEDURE,
        OBJECT_ROLE => CMDTAG_ALTER_ROLE,
        OBJECT_ROUTINE => CMDTAG_ALTER_ROUTINE,
        OBJECT_RULE => CMDTAG_ALTER_RULE,
        OBJECT_SCHEMA => CMDTAG_ALTER_SCHEMA,
        OBJECT_SEQUENCE => CMDTAG_ALTER_SEQUENCE,
        OBJECT_TABLE | OBJECT_TABCONSTRAINT => CMDTAG_ALTER_TABLE,
        OBJECT_TABLESPACE => CMDTAG_ALTER_TABLESPACE,
        OBJECT_TRIGGER => CMDTAG_ALTER_TRIGGER,
        OBJECT_EVENT_TRIGGER => CMDTAG_ALTER_EVENT_TRIGGER,
        OBJECT_TSCONFIGURATION => CMDTAG_ALTER_TEXT_SEARCH_CONFIGURATION,
        OBJECT_TSDICTIONARY => CMDTAG_ALTER_TEXT_SEARCH_DICTIONARY,
        OBJECT_TSPARSER => CMDTAG_ALTER_TEXT_SEARCH_PARSER,
        OBJECT_TSTEMPLATE => CMDTAG_ALTER_TEXT_SEARCH_TEMPLATE,
        OBJECT_TYPE => CMDTAG_ALTER_TYPE,
        OBJECT_VIEW => CMDTAG_ALTER_VIEW,
        OBJECT_MATVIEW => CMDTAG_ALTER_MATERIALIZED_VIEW,
        OBJECT_PUBLICATION => CMDTAG_ALTER_PUBLICATION,
        OBJECT_SUBSCRIPTION => CMDTAG_ALTER_SUBSCRIPTION,
        OBJECT_STATISTIC_EXT => CMDTAG_ALTER_STATISTICS,
        _ => CMDTAG_UNKNOWN,
    }
}

/// First row-mark strength of a `List` of `RowMarkClause` nodes
/// (`((RowMarkClause *) linitial(rowMarks))->strength`), as an `i32`.  Returns
/// `LCS_NONE` for an empty list — never reached by C because the caller
/// already checked `rowMarks != NIL`.
fn first_rowmark_strength(rowmarks: &[NodePtr<'_>]) -> LockClauseStrength {
    match rowmarks.first() {
        Some(n) => match &**n {
            Node::RowMarkClause(r) => r.strength,
            _ => LockClauseStrength::LCS_NONE,
        },
        None => LockClauseStrength::LCS_NONE,
    }
}

/// `CreateCommandTag` (utility.c:2361-3236) — determine the command tag for a
/// parse tree (raw, analyzed `Query`, or `PlannedStmt`).
pub fn CreateCommandTag(parsetree: &Node) -> PgResult<CommandTag> {
    let tag: CommandTag = match parsetree {
        // NOTE: the C `case T_RawStmt:` arm (recurse into `((RawStmt *)
        // parsetree)->stmt`) cannot be reached in this repo's node model:
        // `RawStmt` is not a `Node` enum variant, so a `&Node` never carries one.

        // raw plannable queries
        Node::InsertStmt(_) => CMDTAG_INSERT,
        Node::DeleteStmt(_) => CMDTAG_DELETE,
        Node::UpdateStmt(_) => CMDTAG_UPDATE,
        Node::MergeStmt(_) => CMDTAG_MERGE,
        Node::SelectStmt(_) => CMDTAG_SELECT,
        Node::PLAssignStmt(_) => CMDTAG_SELECT,

        // utility statements --- same whether raw or cooked
        Node::TransactionStmt(stmt) => match stmt.kind {
            TRANS_STMT_BEGIN => CMDTAG_BEGIN,
            TRANS_STMT_START => CMDTAG_START_TRANSACTION,
            TRANS_STMT_COMMIT => CMDTAG_COMMIT,
            TRANS_STMT_ROLLBACK | TRANS_STMT_ROLLBACK_TO => CMDTAG_ROLLBACK,
            TRANS_STMT_SAVEPOINT => CMDTAG_SAVEPOINT,
            TRANS_STMT_RELEASE => CMDTAG_RELEASE,
            TRANS_STMT_PREPARE => CMDTAG_PREPARE_TRANSACTION,
            TRANS_STMT_COMMIT_PREPARED => CMDTAG_COMMIT_PREPARED,
            TRANS_STMT_ROLLBACK_PREPARED => CMDTAG_ROLLBACK_PREPARED,
            _ => CMDTAG_UNKNOWN,
        },

        Node::DeclareCursorStmt(_) => CMDTAG_DECLARE_CURSOR,
        Node::ClosePortalStmt(stmt) => {
            if stmt.portalname.is_none() {
                CMDTAG_CLOSE_CURSOR_ALL
            } else {
                CMDTAG_CLOSE_CURSOR
            }
        }
        Node::FetchStmt(stmt) => {
            if stmt.ismove {
                CMDTAG_MOVE
            } else {
                CMDTAG_FETCH
            }
        }
        Node::CreateDomainStmt(_) => CMDTAG_CREATE_DOMAIN,
        Node::CreateSchemaStmt(_) => CMDTAG_CREATE_SCHEMA,
        Node::CreateStmt(_) => CMDTAG_CREATE_TABLE,
        Node::CreateTableSpaceStmt(_) => CMDTAG_CREATE_TABLESPACE,
        Node::DropTableSpaceStmt(_) => CMDTAG_DROP_TABLESPACE,
        Node::AlterTableSpaceOptionsStmt(_) => CMDTAG_ALTER_TABLESPACE,
        Node::CreateExtensionStmt(_) => CMDTAG_CREATE_EXTENSION,
        Node::AlterExtensionStmt(_) => CMDTAG_ALTER_EXTENSION,
        Node::AlterExtensionContentsStmt(_) => CMDTAG_ALTER_EXTENSION,
        Node::CreateFdwStmt(_) => CMDTAG_CREATE_FOREIGN_DATA_WRAPPER,
        Node::AlterFdwStmt(_) => CMDTAG_ALTER_FOREIGN_DATA_WRAPPER,
        Node::CreateForeignServerStmt(_) => CMDTAG_CREATE_SERVER,
        Node::AlterForeignServerStmt(_) => CMDTAG_ALTER_SERVER,
        Node::CreateUserMappingStmt(_) => CMDTAG_CREATE_USER_MAPPING,
        Node::AlterUserMappingStmt(_) => CMDTAG_ALTER_USER_MAPPING,
        Node::DropUserMappingStmt(_) => CMDTAG_DROP_USER_MAPPING,
        Node::CreateForeignTableStmt(_) => CMDTAG_CREATE_FOREIGN_TABLE,
        Node::ImportForeignSchemaStmt(_) => CMDTAG_IMPORT_FOREIGN_SCHEMA,
        Node::DropStmt(stmt) => match stmt.removeType {
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
        },
        Node::TruncateStmt(_) => CMDTAG_TRUNCATE_TABLE,
        Node::CommentStmt(_) => CMDTAG_COMMENT,
        Node::SecLabelStmt(_) => CMDTAG_SECURITY_LABEL,
        Node::CopyStmt(_) => CMDTAG_COPY,
        Node::RenameStmt(stmt) => {
            // When a column is renamed, the command tag is created from its
            // relation type.
            let objtype = if stmt.renameType == OBJECT_COLUMN {
                stmt.relationType
            } else {
                stmt.renameType
            };
            AlterObjectTypeCommandTag(objtype)
        }
        Node::AlterObjectDependsStmt(stmt) => AlterObjectTypeCommandTag(stmt.objectType),
        Node::AlterObjectSchemaStmt(stmt) => AlterObjectTypeCommandTag(stmt.objectType),
        Node::AlterOwnerStmt(stmt) => AlterObjectTypeCommandTag(stmt.objectType),
        Node::AlterTableMoveAllStmt(stmt) => AlterObjectTypeCommandTag(stmt.objtype),
        Node::AlterTableStmt(stmt) => AlterObjectTypeCommandTag(stmt.objtype),
        Node::AlterDomainStmt(_) => CMDTAG_ALTER_DOMAIN,
        Node::AlterFunctionStmt(stmt) => match stmt.objtype {
            OBJECT_FUNCTION => CMDTAG_ALTER_FUNCTION,
            OBJECT_PROCEDURE => CMDTAG_ALTER_PROCEDURE,
            OBJECT_ROUTINE => CMDTAG_ALTER_ROUTINE,
            _ => CMDTAG_UNKNOWN,
        },
        Node::GrantStmt(stmt) => {
            if stmt.is_grant {
                CMDTAG_GRANT
            } else {
                CMDTAG_REVOKE
            }
        }
        Node::GrantRoleStmt(stmt) => {
            if stmt.is_grant {
                CMDTAG_GRANT_ROLE
            } else {
                CMDTAG_REVOKE_ROLE
            }
        }
        Node::AlterDefaultPrivilegesStmt(_) => CMDTAG_ALTER_DEFAULT_PRIVILEGES,
        Node::DefineStmt(stmt) => match stmt.kind {
            OBJECT_AGGREGATE => CMDTAG_CREATE_AGGREGATE,
            OBJECT_OPERATOR => CMDTAG_CREATE_OPERATOR,
            OBJECT_TYPE => CMDTAG_CREATE_TYPE,
            OBJECT_TSPARSER => CMDTAG_CREATE_TEXT_SEARCH_PARSER,
            OBJECT_TSDICTIONARY => CMDTAG_CREATE_TEXT_SEARCH_DICTIONARY,
            OBJECT_TSTEMPLATE => CMDTAG_CREATE_TEXT_SEARCH_TEMPLATE,
            OBJECT_TSCONFIGURATION => CMDTAG_CREATE_TEXT_SEARCH_CONFIGURATION,
            OBJECT_COLLATION => CMDTAG_CREATE_COLLATION,
            OBJECT_ACCESS_METHOD => CMDTAG_CREATE_ACCESS_METHOD,
            _ => CMDTAG_UNKNOWN,
        },
        Node::CompositeTypeStmt(_) => CMDTAG_CREATE_TYPE,
        Node::CreateEnumStmt(_) => CMDTAG_CREATE_TYPE,
        Node::CreateRangeStmt(_) => CMDTAG_CREATE_TYPE,
        Node::AlterEnumStmt(_) => CMDTAG_ALTER_TYPE,
        Node::ViewStmt(_) => CMDTAG_CREATE_VIEW,
        Node::CreateFunctionStmt(stmt) => {
            if stmt.is_procedure {
                CMDTAG_CREATE_PROCEDURE
            } else {
                CMDTAG_CREATE_FUNCTION
            }
        }
        Node::IndexStmt(_) => CMDTAG_CREATE_INDEX,
        Node::RuleStmt(_) => CMDTAG_CREATE_RULE,
        Node::CreateSeqStmt(_) => CMDTAG_CREATE_SEQUENCE,
        Node::AlterSeqStmt(_) => CMDTAG_ALTER_SEQUENCE,
        Node::DoStmt(_) => CMDTAG_DO,
        Node::CreatedbStmt(_) => CMDTAG_CREATE_DATABASE,
        Node::AlterDatabaseStmt(_)
        | Node::AlterDatabaseRefreshCollStmt(_)
        | Node::AlterDatabaseSetStmt(_) => CMDTAG_ALTER_DATABASE,
        Node::DropdbStmt(_) => CMDTAG_DROP_DATABASE,
        Node::NotifyStmt(_) => CMDTAG_NOTIFY,
        Node::ListenStmt(_) => CMDTAG_LISTEN,
        Node::UnlistenStmt(_) => CMDTAG_UNLISTEN,
        Node::LoadStmt(_) => CMDTAG_LOAD,
        Node::CallStmt(_) => CMDTAG_CALL,
        Node::ClusterStmt(_) => CMDTAG_CLUSTER,
        Node::VacuumStmt(stmt) => {
            if stmt.is_vacuumcmd {
                CMDTAG_VACUUM
            } else {
                CMDTAG_ANALYZE
            }
        }
        Node::ExplainStmt(_) => CMDTAG_EXPLAIN,
        Node::CreateTableAsStmt(stmt) => match stmt.objtype {
            OBJECT_TABLE => {
                if stmt.is_select_into {
                    CMDTAG_SELECT_INTO
                } else {
                    CMDTAG_CREATE_TABLE_AS
                }
            }
            OBJECT_MATVIEW => CMDTAG_CREATE_MATERIALIZED_VIEW,
            _ => CMDTAG_UNKNOWN,
        },
        Node::RefreshMatViewStmt(_) => CMDTAG_REFRESH_MATERIALIZED_VIEW,
        Node::AlterSystemStmt(_) => CMDTAG_ALTER_SYSTEM,
        Node::VariableSetStmt(stmt) => match stmt.kind {
            VAR_SET_VALUE | VAR_SET_CURRENT | VAR_SET_DEFAULT | VAR_SET_MULTI => CMDTAG_SET,
            VAR_RESET | VAR_RESET_ALL => CMDTAG_RESET,
        },
        Node::VariableShowStmt(_) => CMDTAG_SHOW,
        Node::DiscardStmt(stmt) => match stmt.target {
            DISCARD_ALL => CMDTAG_DISCARD_ALL,
            DISCARD_PLANS => CMDTAG_DISCARD_PLANS,
            DISCARD_TEMP => CMDTAG_DISCARD_TEMP,
            DISCARD_SEQUENCES => CMDTAG_DISCARD_SEQUENCES,
        },
        Node::CreateTransformStmt(_) => CMDTAG_CREATE_TRANSFORM,
        Node::CreateTrigStmt(_) => CMDTAG_CREATE_TRIGGER,
        Node::CreateEventTrigStmt(_) => CMDTAG_CREATE_EVENT_TRIGGER,
        Node::AlterEventTrigStmt(_) => CMDTAG_ALTER_EVENT_TRIGGER,
        Node::CreatePLangStmt(_) => CMDTAG_CREATE_LANGUAGE,
        Node::CreateRoleStmt(_) => CMDTAG_CREATE_ROLE,
        Node::AlterRoleStmt(_) => CMDTAG_ALTER_ROLE,
        Node::AlterRoleSetStmt(_) => CMDTAG_ALTER_ROLE,
        Node::DropRoleStmt(_) => CMDTAG_DROP_ROLE,
        Node::DropOwnedStmt(_) => CMDTAG_DROP_OWNED,
        Node::ReassignOwnedStmt(_) => CMDTAG_REASSIGN_OWNED,
        Node::LockStmt(_) => CMDTAG_LOCK_TABLE,
        Node::ConstraintsSetStmt(_) => CMDTAG_SET_CONSTRAINTS,
        Node::CheckPointStmt(_) => CMDTAG_CHECKPOINT,
        Node::ReindexStmt(_) => CMDTAG_REINDEX,
        Node::CreateConversionStmt(_) => CMDTAG_CREATE_CONVERSION,
        Node::CreateCastStmt(_) => CMDTAG_CREATE_CAST,
        Node::CreateOpClassStmt(_) => CMDTAG_CREATE_OPERATOR_CLASS,
        Node::CreateOpFamilyStmt(_) => CMDTAG_CREATE_OPERATOR_FAMILY,
        Node::AlterOpFamilyStmt(_) => CMDTAG_ALTER_OPERATOR_FAMILY,
        Node::AlterOperatorStmt(_) => CMDTAG_ALTER_OPERATOR,
        Node::AlterTypeStmt(_) => CMDTAG_ALTER_TYPE,
        Node::AlterTSDictionaryStmt(_) => CMDTAG_ALTER_TEXT_SEARCH_DICTIONARY,
        Node::AlterTSConfigurationStmt(_) => CMDTAG_ALTER_TEXT_SEARCH_CONFIGURATION,
        Node::CreatePolicyStmt(_) => CMDTAG_CREATE_POLICY,
        Node::AlterPolicyStmt(_) => CMDTAG_ALTER_POLICY,
        Node::CreateAmStmt(_) => CMDTAG_CREATE_ACCESS_METHOD,
        Node::CreatePublicationStmt(_) => CMDTAG_CREATE_PUBLICATION,
        Node::AlterPublicationStmt(_) => CMDTAG_ALTER_PUBLICATION,
        Node::CreateSubscriptionStmt(_) => CMDTAG_CREATE_SUBSCRIPTION,
        Node::AlterSubscriptionStmt(_) => CMDTAG_ALTER_SUBSCRIPTION,
        Node::DropSubscriptionStmt(_) => CMDTAG_DROP_SUBSCRIPTION,
        Node::AlterCollationStmt(_) => CMDTAG_ALTER_COLLATION,
        Node::PrepareStmt(_) => CMDTAG_PREPARE,
        Node::ExecuteStmt(_) => CMDTAG_EXECUTE,
        Node::CreateStatsStmt(_) => CMDTAG_CREATE_STATISTICS,
        Node::AlterStatsStmt(_) => CMDTAG_ALTER_STATISTICS,
        Node::DeallocateStmt(stmt) => {
            if stmt.name.is_none() {
                CMDTAG_DEALLOCATE_ALL
            } else {
                CMDTAG_DEALLOCATE
            }
        }

        // NOTE: the C `case T_PlannedStmt:` arm cannot be reached in this repo's
        // node model: `PlannedStmt` is not a `Node` enum variant, so a `&Node`
        // never carries one. `CreateCommandTag` is invoked on raw/analyzed parse
        // trees here (`Query` covers the analyzed case below); a `PlannedStmt`
        // command tag is derived from its `utilityStmt`/commandType by the
        // dispatcher, not through this `&Node` entrypoint.

        // parsed-and-rewritten-but-not-planned queries
        Node::Query(stmt) => match stmt.commandType {
            CMD_SELECT => {
                // We take a little extra care here so that the result will be
                // useful for complaints about read-only statements.
                if !stmt.rowMarks.is_empty() {
                    // not 100% but probably close enough
                    match first_rowmark_strength(&stmt.rowMarks) {
                        LCS_FORKEYSHARE => CMDTAG_SELECT_FOR_KEY_SHARE,
                        LCS_FORSHARE => CMDTAG_SELECT_FOR_SHARE,
                        LCS_FORNOKEYUPDATE => CMDTAG_SELECT_FOR_NO_KEY_UPDATE,
                        LCS_FORUPDATE => CMDTAG_SELECT_FOR_UPDATE,
                        _ => CMDTAG_UNKNOWN,
                    }
                } else {
                    CMDTAG_SELECT
                }
            }
            CMD_UPDATE => CMDTAG_UPDATE,
            CMD_INSERT => CMDTAG_INSERT,
            CMD_DELETE => CMDTAG_DELETE,
            CMD_MERGE => CMDTAG_MERGE,
            CMD_UTILITY => match &stmt.utilityStmt {
                Some(inner) => CreateCommandTag(inner)?,
                None => CMDTAG_UNKNOWN,
            },
            other => {
                backend_utils_error::elog(
                    WARNING,
                    format!("unrecognized commandType: {}", other as i32),
                )?;
                CMDTAG_UNKNOWN
            }
        },

        _ => {
            backend_utils_error::elog(
                WARNING,
                format!("unrecognized node type: {}", parsetree.node_tag().0),
            )?;
            CMDTAG_UNKNOWN
        }
    };

    Ok(tag)
}
