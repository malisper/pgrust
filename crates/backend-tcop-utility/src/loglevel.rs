//! Log-statement-level derivation (`GetCommandLogLevel`, utility.c:3249-3769).

use backend_tcop_utility_out_seams as rt;
use types_error::{PgResult, WARNING};
use types_nodes::nodes::Node;
use types_nodes::nodes::{CMD_DELETE, CMD_INSERT, CMD_MERGE, CMD_SELECT, CMD_UPDATE, CMD_UTILITY};

use crate::consts::*;

/// `GetCommandLogLevel` (utility.c:3249-3769) — determine the [`LogStmtLevel`]
/// for a parse tree (used by `check_log_statement` to decide whether to log).
///
/// Mirrors the C switch on `nodeTag(parsetree)`. Dispatch uses `node_tag()`
/// tag-comparison guards for the bulk DDL arms (matching the idiom used by the
/// rest of this crate); arms that inspect a payload field rebind through the
/// `Node` enum.
pub fn GetCommandLogLevel(parsetree: &Node) -> PgResult<LogStmtLevel> {
    use types_nodes::nodes as ntag;

    let lev: LogStmtLevel = match parsetree.node_tag() {
        // NOTE: the C `case T_RawStmt:` arm (recurse into `((RawStmt *)
        // parsetree)->stmt`) cannot be reached in this repo's node model:
        // `RawStmt` is not a `Node` enum variant and has no `T_RawStmt` tag, so
        // a `&Node` can never carry one. (The src-idiomatic reference recurses
        // here via `parsetree.expect_rawstmt()`.)

        // raw plannable queries
        t if t == ntag::T_InsertStmt
            || t == ntag::T_DeleteStmt
            || t == ntag::T_UpdateStmt
            || t == ntag::T_MergeStmt =>
        {
            LOGSTMT_MOD
        }

        t if t == ntag::T_SelectStmt => {
            let Node::SelectStmt(stmt) = parsetree else { unreachable!() };
            if stmt.intoClause.is_some() {
                LOGSTMT_DDL // SELECT INTO
            } else {
                LOGSTMT_ALL
            }
        }

        t if t == ntag::T_PLAssignStmt => LOGSTMT_ALL,

        // utility statements --- same whether raw or cooked
        t if t == ntag::T_TransactionStmt => LOGSTMT_ALL,
        t if t == ntag::T_DeclareCursorStmt => LOGSTMT_ALL,
        t if t == ntag::T_ClosePortalStmt => LOGSTMT_ALL,
        t if t == ntag::T_FetchStmt => LOGSTMT_ALL,
        t if t == ntag::T_CreateSchemaStmt => LOGSTMT_DDL,
        t if t == ntag::T_CreateStmt || t == ntag::T_CreateForeignTableStmt => LOGSTMT_DDL,
        t if t == ntag::T_CreateTableSpaceStmt
            || t == ntag::T_DropTableSpaceStmt
            || t == ntag::T_AlterTableSpaceOptionsStmt =>
        {
            LOGSTMT_DDL
        }
        t if t == ntag::T_CreateExtensionStmt
            || t == ntag::T_AlterExtensionStmt
            || t == ntag::T_AlterExtensionContentsStmt =>
        {
            LOGSTMT_DDL
        }
        t if t == ntag::T_CreateFdwStmt
            || t == ntag::T_AlterFdwStmt
            || t == ntag::T_CreateForeignServerStmt
            || t == ntag::T_AlterForeignServerStmt
            || t == ntag::T_CreateUserMappingStmt
            || t == ntag::T_AlterUserMappingStmt
            || t == ntag::T_DropUserMappingStmt
            || t == ntag::T_ImportForeignSchemaStmt =>
        {
            LOGSTMT_DDL
        }
        t if t == ntag::T_DropStmt => LOGSTMT_DDL,
        t if t == ntag::T_TruncateStmt => LOGSTMT_MOD,
        t if t == ntag::T_CommentStmt => LOGSTMT_DDL,
        t if t == ntag::T_SecLabelStmt => LOGSTMT_DDL,

        t if t == ntag::T_CopyStmt => {
            let Node::CopyStmt(stmt) = parsetree else { unreachable!() };
            if stmt.is_from {
                LOGSTMT_MOD
            } else {
                LOGSTMT_ALL
            }
        }

        // Look through a PREPARE to the contained stmt.
        t if t == ntag::T_PrepareStmt => {
            let Node::PrepareStmt(stmt) = parsetree else { unreachable!() };
            match &stmt.query {
                Some(inner) => GetCommandLogLevel(inner)?,
                None => LOGSTMT_ALL,
            }
        }

        // Look through an EXECUTE to the referenced stmt.  The C does
        // `ps = FetchPreparedStatement(stmt->name, false)` then, if it found a
        // prepared statement with a non-NULL raw parse tree, recurses into
        // `ps->plansource->raw_parse_tree->stmt`; otherwise `LOGSTMT_ALL`. That
        // prepared-statement-cache lookup + raw-parse-tree access crosses the
        // out-seam, returning the referenced raw parse-tree node (or `None` for
        // a missing entry / NULL raw_parse_tree).
        t if t == ntag::T_ExecuteStmt => match rt::execute_stmt_raw_parse_tree::call(parsetree) {
            Some(raw) => GetCommandLogLevel(&raw)?,
            None => LOGSTMT_ALL,
        },

        t if t == ntag::T_DeallocateStmt => LOGSTMT_ALL,
        t if t == ntag::T_RenameStmt => LOGSTMT_DDL,
        t if t == ntag::T_AlterObjectDependsStmt => LOGSTMT_DDL,
        t if t == ntag::T_AlterObjectSchemaStmt => LOGSTMT_DDL,
        t if t == ntag::T_AlterOwnerStmt => LOGSTMT_DDL,
        t if t == ntag::T_AlterOperatorStmt => LOGSTMT_DDL,
        t if t == ntag::T_AlterTypeStmt => LOGSTMT_DDL,
        t if t == ntag::T_AlterTableMoveAllStmt || t == ntag::T_AlterTableStmt => LOGSTMT_DDL,
        t if t == ntag::T_AlterDomainStmt => LOGSTMT_DDL,
        t if t == ntag::T_GrantStmt => LOGSTMT_DDL,
        t if t == ntag::T_GrantRoleStmt => LOGSTMT_DDL,
        t if t == ntag::T_AlterDefaultPrivilegesStmt => LOGSTMT_DDL,
        t if t == ntag::T_DefineStmt => LOGSTMT_DDL,
        t if t == ntag::T_CompositeTypeStmt => LOGSTMT_DDL,
        t if t == ntag::T_CreateEnumStmt => LOGSTMT_DDL,
        t if t == ntag::T_CreateRangeStmt => LOGSTMT_DDL,
        t if t == ntag::T_AlterEnumStmt => LOGSTMT_DDL,
        t if t == ntag::T_ViewStmt => LOGSTMT_DDL,
        t if t == ntag::T_CreateFunctionStmt => LOGSTMT_DDL,
        t if t == ntag::T_AlterFunctionStmt => LOGSTMT_DDL,
        t if t == ntag::T_IndexStmt => LOGSTMT_DDL,
        t if t == ntag::T_RuleStmt => LOGSTMT_DDL,
        t if t == ntag::T_CreateSeqStmt => LOGSTMT_DDL,
        t if t == ntag::T_AlterSeqStmt => LOGSTMT_DDL,
        t if t == ntag::T_DoStmt => LOGSTMT_ALL,
        t if t == ntag::T_CreatedbStmt => LOGSTMT_DDL,
        t if t == ntag::T_AlterDatabaseStmt
            || t == ntag::T_AlterDatabaseRefreshCollStmt
            || t == ntag::T_AlterDatabaseSetStmt =>
        {
            LOGSTMT_DDL
        }
        t if t == ntag::T_DropdbStmt => LOGSTMT_DDL,
        t if t == ntag::T_NotifyStmt => LOGSTMT_ALL,
        t if t == ntag::T_ListenStmt => LOGSTMT_ALL,
        t if t == ntag::T_UnlistenStmt => LOGSTMT_ALL,
        t if t == ntag::T_LoadStmt => LOGSTMT_ALL,
        t if t == ntag::T_CallStmt => LOGSTMT_ALL,
        t if t == ntag::T_ClusterStmt => LOGSTMT_DDL,
        t if t == ntag::T_VacuumStmt => LOGSTMT_ALL,

        t if t == ntag::T_ExplainStmt => {
            let Node::ExplainStmt(stmt) = parsetree else { unreachable!() };
            let mut analyze = false;
            // Look through an EXPLAIN ANALYZE to the contained stmt.
            for opt in &stmt.options {
                if let Node::DefElem(de) = &**opt {
                    if de.defname.as_deref() == Some("analyze") {
                        analyze = rt::def_get_boolean::call(opt);
                    }
                }
                // don't "break", as explain.c will use the last value
            }
            if analyze {
                return match &stmt.query {
                    Some(inner) => GetCommandLogLevel(inner),
                    None => Ok(LOGSTMT_ALL),
                };
            }
            // Plain EXPLAIN isn't so interesting.
            LOGSTMT_ALL
        }

        t if t == ntag::T_CreateTableAsStmt => LOGSTMT_DDL,
        t if t == ntag::T_RefreshMatViewStmt => LOGSTMT_DDL,
        t if t == ntag::T_AlterSystemStmt => LOGSTMT_DDL,
        t if t == ntag::T_VariableSetStmt => LOGSTMT_ALL,
        t if t == ntag::T_VariableShowStmt => LOGSTMT_ALL,
        t if t == ntag::T_DiscardStmt => LOGSTMT_ALL,
        t if t == ntag::T_CreateTrigStmt => LOGSTMT_DDL,
        t if t == ntag::T_CreateEventTrigStmt => LOGSTMT_DDL,
        t if t == ntag::T_AlterEventTrigStmt => LOGSTMT_DDL,
        t if t == ntag::T_CreatePLangStmt => LOGSTMT_DDL,
        t if t == ntag::T_CreateDomainStmt => LOGSTMT_DDL,
        t if t == ntag::T_CreateRoleStmt => LOGSTMT_DDL,
        t if t == ntag::T_AlterRoleStmt => LOGSTMT_DDL,
        t if t == ntag::T_AlterRoleSetStmt => LOGSTMT_DDL,
        t if t == ntag::T_DropRoleStmt => LOGSTMT_DDL,
        t if t == ntag::T_DropOwnedStmt => LOGSTMT_DDL,
        t if t == ntag::T_ReassignOwnedStmt => LOGSTMT_DDL,
        t if t == ntag::T_LockStmt => LOGSTMT_ALL,
        t if t == ntag::T_ConstraintsSetStmt => LOGSTMT_ALL,
        t if t == ntag::T_CheckPointStmt => LOGSTMT_ALL,
        t if t == ntag::T_ReindexStmt => LOGSTMT_ALL, // should this be DDL?
        t if t == ntag::T_CreateConversionStmt => LOGSTMT_DDL,
        t if t == ntag::T_CreateCastStmt => LOGSTMT_DDL,
        t if t == ntag::T_CreateOpClassStmt => LOGSTMT_DDL,
        t if t == ntag::T_CreateOpFamilyStmt => LOGSTMT_DDL,
        t if t == ntag::T_CreateTransformStmt => LOGSTMT_DDL,
        t if t == ntag::T_AlterOpFamilyStmt => LOGSTMT_DDL,
        t if t == ntag::T_CreatePolicyStmt => LOGSTMT_DDL,
        t if t == ntag::T_AlterPolicyStmt => LOGSTMT_DDL,
        t if t == ntag::T_AlterTSDictionaryStmt => LOGSTMT_DDL,
        t if t == ntag::T_AlterTSConfigurationStmt => LOGSTMT_DDL,
        t if t == ntag::T_CreateAmStmt => LOGSTMT_DDL,
        t if t == ntag::T_CreatePublicationStmt => LOGSTMT_DDL,
        t if t == ntag::T_AlterPublicationStmt => LOGSTMT_DDL,
        t if t == ntag::T_CreateSubscriptionStmt => LOGSTMT_DDL,
        t if t == ntag::T_AlterSubscriptionStmt => LOGSTMT_DDL,
        t if t == ntag::T_DropSubscriptionStmt => LOGSTMT_DDL,
        t if t == ntag::T_CreateStatsStmt => LOGSTMT_DDL,
        t if t == ntag::T_AlterStatsStmt => LOGSTMT_DDL,
        t if t == ntag::T_AlterCollationStmt => LOGSTMT_DDL,

        // NOTE: the C `case T_PlannedStmt:` arm (switch on
        // `((PlannedStmt *) parsetree)->commandType`) cannot be reached in this
        // repo's node model: `PlannedStmt` (types_nodes::nodeindexscan) is not a
        // `Node` enum variant and has no `T_PlannedStmt` tag, so a `&Node` can
        // never carry one. (The src-idiomatic reference reaches it via
        // `parsetree.expect_plannedstmt()`.) An already-planned statement would
        // therefore fall through to the default WARNING arm below — matching the
        // C behaviour for any node type this function does not understand.

        // parsed-and-rewritten-but-not-planned queries
        t if t == ntag::T_Query => {
            let Node::Query(stmt) = parsetree else { unreachable!() };
            match stmt.commandType {
                CMD_SELECT => LOGSTMT_ALL,
                CMD_UPDATE | CMD_INSERT | CMD_DELETE | CMD_MERGE => LOGSTMT_MOD,
                CMD_UTILITY => match &stmt.utilityStmt {
                    Some(inner) => GetCommandLogLevel(inner)?,
                    None => LOGSTMT_ALL,
                },
                other => {
                    backend_utils_error::elog(
                        WARNING,
                        format!("unrecognized commandType: {}", other as i32),
                    )?;
                    LOGSTMT_ALL
                }
            }
        }

        _ => {
            backend_utils_error::elog(
                WARNING,
                format!("unrecognized node type: {}", parsetree.node_tag().0),
            )?;
            LOGSTMT_ALL
        }
    };

    Ok(lev)
}
