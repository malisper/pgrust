//! Command-tag derivation (utility.c:2215-3236).
//!
//! Faithful port of `AlterObjectTypeCommandTag` (static, utility.c:2215-2350)
//! and `CreateCommandTag` (utility.c:2362-3236) reconciled to this repo's
//! split-crate node model.

use utils_error::PgResult;
use types_error::WARNING;
use types_core::cmdtag::CommandTag;
use nodes::nodes::Node;
use nodes::parsenodes::*;
use nodes::rawnodes::{
    LockClauseStrength, LCS_FORKEYSHARE, LCS_FORNOKEYUPDATE, LCS_FORSHARE, LCS_FORUPDATE,
};
use nodes::ddlnodes::DiscardMode::*;
use nodes::ddlnodes::TransactionStmtKind::*;
use nodes::ddlnodes::VariableSetKind::*;
use nodes::nodes::{
    CMD_DELETE, CMD_INSERT, CMD_MERGE, CMD_SELECT, CMD_UPDATE, CMD_UTILITY,
};
use nodes::nodes::NodePtr;
use nodes::nodes as ntag;

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
        Some(n) => match (&**n).node_tag() {
            t if t == ntag::T_RowMarkClause => {
                let r = n.expect_rowmarkclause();
                r.strength
            }
            _ => LockClauseStrength::LCS_NONE,
        },
        None => LockClauseStrength::LCS_NONE,
    }
}

/// `CreateCommandTag` (utility.c:2361-3236) — determine the command tag for a
/// parse tree (raw, analyzed `Query`, or `PlannedStmt`).
pub fn CreateCommandTag(parsetree: &Node) -> PgResult<CommandTag> {
    let tag: CommandTag = match parsetree.node_tag() {
        // NOTE: the C `case T_RawStmt:` arm (recurse into `((RawStmt *)
        // parsetree)->stmt`) cannot be reached in this repo's node model:
        // `RawStmt` is not a `Node` enum variant, so a `&Node` never carries one.

        // raw plannable queries
        t if t == ntag::T_InsertStmt => CMDTAG_INSERT,
        t if t == ntag::T_DeleteStmt => CMDTAG_DELETE,
        t if t == ntag::T_UpdateStmt => CMDTAG_UPDATE,
        t if t == ntag::T_MergeStmt => CMDTAG_MERGE,
        t if t == ntag::T_SelectStmt => CMDTAG_SELECT,
        t if t == ntag::T_PLAssignStmt => CMDTAG_SELECT,

        // utility statements --- same whether raw or cooked
        t if t == ntag::T_TransactionStmt => { let stmt = parsetree.expect_transactionstmt(); match stmt.kind {
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
        } }

        t if t == ntag::T_DeclareCursorStmt => CMDTAG_DECLARE_CURSOR,
        t if t == ntag::T_ClosePortalStmt => {
            let stmt = parsetree.expect_closeportalstmt();
            if stmt.portalname.is_none() {
                CMDTAG_CLOSE_CURSOR_ALL
            } else {
                CMDTAG_CLOSE_CURSOR
            }
        }
        t if t == ntag::T_FetchStmt => {
            let stmt = parsetree.expect_fetchstmt();
            if stmt.ismove {
                CMDTAG_MOVE
            } else {
                CMDTAG_FETCH
            }
        }
        t if t == ntag::T_CreateDomainStmt => CMDTAG_CREATE_DOMAIN,
        t if t == ntag::T_CreateSchemaStmt => CMDTAG_CREATE_SCHEMA,
        t if t == ntag::T_CreateStmt => CMDTAG_CREATE_TABLE,
        t if t == ntag::T_CreateTableSpaceStmt => CMDTAG_CREATE_TABLESPACE,
        t if t == ntag::T_DropTableSpaceStmt => CMDTAG_DROP_TABLESPACE,
        t if t == ntag::T_AlterTableSpaceOptionsStmt => CMDTAG_ALTER_TABLESPACE,
        t if t == ntag::T_CreateExtensionStmt => CMDTAG_CREATE_EXTENSION,
        t if t == ntag::T_AlterExtensionStmt => CMDTAG_ALTER_EXTENSION,
        t if t == ntag::T_AlterExtensionContentsStmt => CMDTAG_ALTER_EXTENSION,
        t if t == ntag::T_CreateFdwStmt => CMDTAG_CREATE_FOREIGN_DATA_WRAPPER,
        t if t == ntag::T_AlterFdwStmt => CMDTAG_ALTER_FOREIGN_DATA_WRAPPER,
        t if t == ntag::T_CreateForeignServerStmt => CMDTAG_CREATE_SERVER,
        t if t == ntag::T_AlterForeignServerStmt => CMDTAG_ALTER_SERVER,
        t if t == ntag::T_CreateUserMappingStmt => CMDTAG_CREATE_USER_MAPPING,
        t if t == ntag::T_AlterUserMappingStmt => CMDTAG_ALTER_USER_MAPPING,
        t if t == ntag::T_DropUserMappingStmt => CMDTAG_DROP_USER_MAPPING,
        t if t == ntag::T_CreateForeignTableStmt => CMDTAG_CREATE_FOREIGN_TABLE,
        t if t == ntag::T_ImportForeignSchemaStmt => CMDTAG_IMPORT_FOREIGN_SCHEMA,
        t if t == ntag::T_DropStmt => { let stmt = parsetree.expect_dropstmt(); match stmt.removeType {
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
        } }
        t if t == ntag::T_TruncateStmt => CMDTAG_TRUNCATE_TABLE,
        t if t == ntag::T_CommentStmt => CMDTAG_COMMENT,
        t if t == ntag::T_SecLabelStmt => CMDTAG_SECURITY_LABEL,
        t if t == ntag::T_CopyStmt => CMDTAG_COPY,
        t if t == ntag::T_RenameStmt => {
            let stmt = parsetree.expect_renamestmt();
            // When a column is renamed, the command tag is created from its
            // relation type.
            let objtype = if stmt.renameType == OBJECT_COLUMN {
                stmt.relationType
            } else {
                stmt.renameType
            };
            AlterObjectTypeCommandTag(objtype)
        }
        t if t == ntag::T_AlterObjectDependsStmt => { let stmt = parsetree.expect_alterobjectdependsstmt(); AlterObjectTypeCommandTag(stmt.objectType) },
        t if t == ntag::T_AlterObjectSchemaStmt => { let stmt = parsetree.expect_alterobjectschemastmt(); AlterObjectTypeCommandTag(stmt.objectType) },
        t if t == ntag::T_AlterOwnerStmt => { let stmt = parsetree.expect_alterownerstmt(); AlterObjectTypeCommandTag(stmt.objectType) },
        t if t == ntag::T_AlterTableMoveAllStmt => { let stmt = parsetree.expect_altertablemoveallstmt(); AlterObjectTypeCommandTag(stmt.objtype) },
        t if t == ntag::T_AlterTableStmt => { let stmt = parsetree.expect_altertablestmt(); AlterObjectTypeCommandTag(stmt.objtype) },
        t if t == ntag::T_AlterDomainStmt => CMDTAG_ALTER_DOMAIN,
        t if t == ntag::T_AlterFunctionStmt => {
            let stmt = parsetree.expect_alterfunctionstmt();
            match stmt.objtype {
            OBJECT_FUNCTION => CMDTAG_ALTER_FUNCTION,
            OBJECT_PROCEDURE => CMDTAG_ALTER_PROCEDURE,
            OBJECT_ROUTINE => CMDTAG_ALTER_ROUTINE,
            _ => CMDTAG_UNKNOWN,
        }
        }
        t if t == ntag::T_GrantStmt => {
            let stmt = parsetree.expect_grantstmt();
            if stmt.is_grant {
                CMDTAG_GRANT
            } else {
                CMDTAG_REVOKE
            }
        }
        t if t == ntag::T_GrantRoleStmt => {
            let stmt = parsetree.expect_grantrolestmt();
            if stmt.is_grant {
                CMDTAG_GRANT_ROLE
            } else {
                CMDTAG_REVOKE_ROLE
            }
        }
        t if t == ntag::T_AlterDefaultPrivilegesStmt => CMDTAG_ALTER_DEFAULT_PRIVILEGES,
        t if t == ntag::T_DefineStmt => {
            let stmt = parsetree.expect_definestmt();
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
            _ => CMDTAG_UNKNOWN,
        }
        }
        t if t == ntag::T_CompositeTypeStmt => CMDTAG_CREATE_TYPE,
        t if t == ntag::T_CreateEnumStmt => CMDTAG_CREATE_TYPE,
        t if t == ntag::T_CreateRangeStmt => CMDTAG_CREATE_TYPE,
        t if t == ntag::T_AlterEnumStmt => CMDTAG_ALTER_TYPE,
        t if t == ntag::T_ViewStmt => CMDTAG_CREATE_VIEW,
        t if t == ntag::T_CreateFunctionStmt => {
            let stmt = parsetree.expect_createfunctionstmt();
            if stmt.is_procedure {
                CMDTAG_CREATE_PROCEDURE
            } else {
                CMDTAG_CREATE_FUNCTION
            }
        }
        t if t == ntag::T_IndexStmt => CMDTAG_CREATE_INDEX,
        t if t == ntag::T_RuleStmt => CMDTAG_CREATE_RULE,
        t if t == ntag::T_CreateSeqStmt => CMDTAG_CREATE_SEQUENCE,
        t if t == ntag::T_AlterSeqStmt => CMDTAG_ALTER_SEQUENCE,
        t if t == ntag::T_DoStmt => CMDTAG_DO,
        t if t == ntag::T_CreatedbStmt => CMDTAG_CREATE_DATABASE,
        t if t == ntag::T_AlterDatabaseStmt || t == ntag::T_AlterDatabaseRefreshCollStmt || t == ntag::T_AlterDatabaseSetStmt => CMDTAG_ALTER_DATABASE,
        t if t == ntag::T_DropdbStmt => CMDTAG_DROP_DATABASE,
        t if t == ntag::T_NotifyStmt => CMDTAG_NOTIFY,
        t if t == ntag::T_ListenStmt => CMDTAG_LISTEN,
        t if t == ntag::T_UnlistenStmt => CMDTAG_UNLISTEN,
        t if t == ntag::T_LoadStmt => CMDTAG_LOAD,
        t if t == ntag::T_CallStmt => CMDTAG_CALL,
        t if t == ntag::T_ClusterStmt => CMDTAG_CLUSTER,
        t if t == ntag::T_VacuumStmt => {
            let stmt = parsetree.expect_vacuumstmt();
            if stmt.is_vacuumcmd {
                CMDTAG_VACUUM
            } else {
                CMDTAG_ANALYZE
            }
        }
        t if t == ntag::T_ExplainStmt => CMDTAG_EXPLAIN,
        t if t == ntag::T_CreateTableAsStmt => {
            let stmt = parsetree.expect_createtableasstmt();
            match stmt.objtype {
            OBJECT_TABLE => {
                if stmt.is_select_into {
                    CMDTAG_SELECT_INTO
                } else {
                    CMDTAG_CREATE_TABLE_AS
                }
            }
            OBJECT_MATVIEW => CMDTAG_CREATE_MATERIALIZED_VIEW,
            _ => CMDTAG_UNKNOWN,
        }
        }
        t if t == ntag::T_RefreshMatViewStmt => CMDTAG_REFRESH_MATERIALIZED_VIEW,
        t if t == ntag::T_AlterSystemStmt => CMDTAG_ALTER_SYSTEM,
        t if t == ntag::T_VariableSetStmt => {
            let stmt = parsetree.expect_variablesetstmt();
            match stmt.kind {
            VAR_SET_VALUE | VAR_SET_CURRENT | VAR_SET_DEFAULT | VAR_SET_MULTI => CMDTAG_SET,
            VAR_RESET | VAR_RESET_ALL => CMDTAG_RESET,
        }
        }
        t if t == ntag::T_VariableShowStmt => CMDTAG_SHOW,
        t if t == ntag::T_DiscardStmt => {
            let stmt = parsetree.expect_discardstmt();
            match stmt.target {
            DISCARD_ALL => CMDTAG_DISCARD_ALL,
            DISCARD_PLANS => CMDTAG_DISCARD_PLANS,
            DISCARD_TEMP => CMDTAG_DISCARD_TEMP,
            DISCARD_SEQUENCES => CMDTAG_DISCARD_SEQUENCES,
        }
        }
        t if t == ntag::T_CreateTransformStmt => CMDTAG_CREATE_TRANSFORM,
        t if t == ntag::T_CreateTrigStmt => CMDTAG_CREATE_TRIGGER,
        t if t == ntag::T_CreateEventTrigStmt => CMDTAG_CREATE_EVENT_TRIGGER,
        t if t == ntag::T_AlterEventTrigStmt => CMDTAG_ALTER_EVENT_TRIGGER,
        t if t == ntag::T_CreatePLangStmt => CMDTAG_CREATE_LANGUAGE,
        t if t == ntag::T_CreateRoleStmt => CMDTAG_CREATE_ROLE,
        t if t == ntag::T_AlterRoleStmt => CMDTAG_ALTER_ROLE,
        t if t == ntag::T_AlterRoleSetStmt => CMDTAG_ALTER_ROLE,
        t if t == ntag::T_DropRoleStmt => CMDTAG_DROP_ROLE,
        t if t == ntag::T_DropOwnedStmt => CMDTAG_DROP_OWNED,
        t if t == ntag::T_ReassignOwnedStmt => CMDTAG_REASSIGN_OWNED,
        t if t == ntag::T_LockStmt => CMDTAG_LOCK_TABLE,
        t if t == ntag::T_ConstraintsSetStmt => CMDTAG_SET_CONSTRAINTS,
        t if t == ntag::T_CheckPointStmt => CMDTAG_CHECKPOINT,
        t if t == ntag::T_ReindexStmt => CMDTAG_REINDEX,
        t if t == ntag::T_CreateConversionStmt => CMDTAG_CREATE_CONVERSION,
        t if t == ntag::T_CreateCastStmt => CMDTAG_CREATE_CAST,
        t if t == ntag::T_CreateOpClassStmt => CMDTAG_CREATE_OPERATOR_CLASS,
        t if t == ntag::T_CreateOpFamilyStmt => CMDTAG_CREATE_OPERATOR_FAMILY,
        t if t == ntag::T_AlterOpFamilyStmt => CMDTAG_ALTER_OPERATOR_FAMILY,
        t if t == ntag::T_AlterOperatorStmt => CMDTAG_ALTER_OPERATOR,
        t if t == ntag::T_AlterTypeStmt => CMDTAG_ALTER_TYPE,
        t if t == ntag::T_AlterTSDictionaryStmt => CMDTAG_ALTER_TEXT_SEARCH_DICTIONARY,
        t if t == ntag::T_AlterTSConfigurationStmt => CMDTAG_ALTER_TEXT_SEARCH_CONFIGURATION,
        t if t == ntag::T_CreatePolicyStmt => CMDTAG_CREATE_POLICY,
        t if t == ntag::T_AlterPolicyStmt => CMDTAG_ALTER_POLICY,
        t if t == ntag::T_CreateAmStmt => CMDTAG_CREATE_ACCESS_METHOD,
        t if t == ntag::T_CreatePublicationStmt => CMDTAG_CREATE_PUBLICATION,
        t if t == ntag::T_AlterPublicationStmt => CMDTAG_ALTER_PUBLICATION,
        t if t == ntag::T_CreateSubscriptionStmt => CMDTAG_CREATE_SUBSCRIPTION,
        t if t == ntag::T_AlterSubscriptionStmt => CMDTAG_ALTER_SUBSCRIPTION,
        t if t == ntag::T_DropSubscriptionStmt => CMDTAG_DROP_SUBSCRIPTION,
        t if t == ntag::T_AlterCollationStmt => CMDTAG_ALTER_COLLATION,
        t if t == ntag::T_PrepareStmt => CMDTAG_PREPARE,
        t if t == ntag::T_ExecuteStmt => CMDTAG_EXECUTE,
        t if t == ntag::T_CreateStatsStmt => CMDTAG_CREATE_STATISTICS,
        t if t == ntag::T_AlterStatsStmt => CMDTAG_ALTER_STATISTICS,
        t if t == ntag::T_DeallocateStmt => {
            let stmt = parsetree.expect_deallocatestmt();
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
        t if t == ntag::T_Query => {
            let stmt = parsetree.expect_query();
            match stmt.commandType {
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
                utils_error::elog(
                    WARNING,
                    format!("unrecognized commandType: {}", other as i32),
                )?;
                CMDTAG_UNKNOWN
            }
        }
        }

        _ => {
            utils_error::elog(
                WARNING,
                format!("unrecognized node type: {}", parsetree.node_tag().0),
            )?;
            CMDTAG_UNKNOWN
        }
    };

    Ok(tag)
}

/// `CreateCommandTag` (utility.c) — the `case T_PlannedStmt:` arm. A
/// `PlannedStmt` is not a `&Node` enum variant in this repo's node model, so
/// the planned-statement command tag has its own entrypoint (this is what
/// `CreateCommandName((Node *) pstmt)` reaches in `SPI_cursor_open_internal`'s
/// "cannot open %s query as cursor" / read-only-violation messages). For the
/// `CMD_SELECT` case it takes the same extra care the C arm does so the result
/// is useful for read-only complaints (rendering the `SELECT FOR …` variant
/// from the first row mark's strength).
pub fn CreateCommandTagForPlannedStmt(
    stmt: &nodes::nodeindexscan::PlannedStmt<'_>,
) -> PgResult<CommandTag> {
    let tag = match stmt.commandType {
        CMD_SELECT => {
            // We take a little extra care here so that the result will be useful
            // for complaints about read-only statements.
            // `PlanRowMark.strength` is the `LockClauseStrength` i32 alias
            // (nodelockrows); compare against the enum discriminants.
            match stmt.rowMarks.as_ref().and_then(|m| m.first()) {
                Some(rm) => {
                    let s = rm.strength;
                    // not 100% but probably close enough
                    if s == LCS_FORKEYSHARE as i32 {
                        CMDTAG_SELECT_FOR_KEY_SHARE
                    } else if s == LCS_FORSHARE as i32 {
                        CMDTAG_SELECT_FOR_SHARE
                    } else if s == LCS_FORNOKEYUPDATE as i32 {
                        CMDTAG_SELECT_FOR_NO_KEY_UPDATE
                    } else if s == LCS_FORUPDATE as i32 {
                        CMDTAG_SELECT_FOR_UPDATE
                    } else {
                        CMDTAG_SELECT
                    }
                }
                None => CMDTAG_SELECT,
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
            utils_error::elog(
                WARNING,
                format!("unrecognized commandType: {}", other as i32),
            )?;
            CMDTAG_UNKNOWN
        }
    };
    Ok(tag)
}
