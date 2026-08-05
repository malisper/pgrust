use mcx::MemoryContext;
use types_nodes::node_tree::Node;
use types_nodes::nodes_enums::CmdType;
use types_nodes::parsenodes::{
    Query, TransactionStmt, TransactionStmtKind, TransactionStmtKind::*,
};
use types_nodes::plannodes::PlannedStmt;
use types_nodes::rawnodes::{RawStmt, SelectStmt};

use crate::consts::*;
use crate::*;

#[test]
fn cmdtag_consts_match_cmdtag_table() {
    for &(tag, name) in CMDTAG_NAMES {
        assert_eq!(cmdtag::GetCommandTagName(tag), name, "index {}", tag.0);
    }
    assert_eq!(CMDTAG_NAMES.len(), 193);
    assert_eq!(CMDTAG_SELECT, types_core::CommandTag::SELECT);
}

#[test]
fn readonly_flags_match_utility_h() {
    assert_eq!(COMMAND_OK_IN_READ_ONLY_TXN, 0x0001);
    assert_eq!(COMMAND_OK_IN_PARALLEL_MODE, 0x0002);
    assert_eq!(COMMAND_OK_IN_RECOVERY, 0x0004);
    assert_eq!(COMMAND_IS_STRICTLY_READ_ONLY, 0x0007);
    assert_eq!(COMMAND_IS_NOT_READ_ONLY, 0);
}

#[test]
fn transaction_stmt_kind_values_match_parsenodes_h() {
    // parsenodes.h assigns TRANS_STMT_* sequentially from 0.
    let order = [
        TRANS_STMT_BEGIN,
        TRANS_STMT_START,
        TRANS_STMT_COMMIT,
        TRANS_STMT_ROLLBACK,
        TRANS_STMT_SAVEPOINT,
        TRANS_STMT_RELEASE,
        TRANS_STMT_ROLLBACK_TO,
        TRANS_STMT_PREPARE,
        TRANS_STMT_COMMIT_PREPARED,
        TRANS_STMT_ROLLBACK_PREPARED,
    ];
    for (i, k) in order.into_iter().enumerate() {
        assert_eq!(k as u32, i as u32);
    }
    // Field completeness vs the C struct (kind/options/savepoint_name/gid/chain/location).
    let TransactionStmt { kind: _, options: _, savepoint_name: _, gid: _, chain: _, location: _ } =
        TransactionStmt::default();
}

fn trans_node(ctx: &MemoryContext, kind: TransactionStmtKind) -> Node<'_> {
    Node::mk(ctx.mcx(), TransactionStmt { kind, ..TransactionStmt::default() }).unwrap()
}

#[test]
fn create_command_tag_transaction_kinds() {
    let ctx = MemoryContext::new("t");
    let cases = [
        (TRANS_STMT_BEGIN, CMDTAG_BEGIN),
        (TRANS_STMT_START, CMDTAG_START_TRANSACTION),
        (TRANS_STMT_COMMIT, CMDTAG_COMMIT),
        (TRANS_STMT_ROLLBACK, CMDTAG_ROLLBACK),
        (TRANS_STMT_ROLLBACK_TO, CMDTAG_ROLLBACK),
        (TRANS_STMT_SAVEPOINT, CMDTAG_SAVEPOINT),
        (TRANS_STMT_RELEASE, CMDTAG_RELEASE),
        (TRANS_STMT_PREPARE, CMDTAG_PREPARE_TRANSACTION),
        (TRANS_STMT_COMMIT_PREPARED, CMDTAG_COMMIT_PREPARED),
        (TRANS_STMT_ROLLBACK_PREPARED, CMDTAG_ROLLBACK_PREPARED),
    ];
    for (kind, want) in cases {
        assert_eq!(CreateCommandTag(trans_node(&ctx, kind)), want);
    }
}

#[test]
fn create_command_tag_select_path() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();

    let select = Node::mk(mcx, SelectStmt::default()).unwrap();
    assert_eq!(CreateCommandTag(select), CMDTAG_SELECT);

    let raw =
        Node::mk(mcx, RawStmt { stmt: Some(select), stmt_location: 0, stmt_len: 0 }).unwrap();
    assert_eq!(CreateCommandTag(raw), CMDTAG_SELECT);

    let query =
        Node::mk(mcx, Query { commandType: CmdType::CMD_SELECT, ..Query::default() }).unwrap();
    assert_eq!(CreateCommandTag(query), CMDTAG_SELECT);

    let pstmt = Node::mk(
        mcx,
        PlannedStmt { commandType: CmdType::CMD_SELECT, ..PlannedStmt::default() },
    )
    .unwrap();
    assert_eq!(CreateCommandTag(pstmt), CMDTAG_SELECT);
}

#[test]
fn create_command_tag_utility_query_recurses() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let begin = trans_node(&ctx, TRANS_STMT_BEGIN);
    let query = Node::mk(
        mcx,
        Query { commandType: CmdType::CMD_UTILITY, utilityStmt: Some(begin), ..Query::default() },
    )
    .unwrap();
    assert_eq!(CreateCommandTag(query), CMDTAG_BEGIN);

    let pstmt = Node::mk(
        mcx,
        PlannedStmt {
            commandType: CmdType::CMD_UTILITY,
            utilityStmt: Some(begin),
            ..PlannedStmt::default()
        },
    )
    .unwrap();
    assert_eq!(CreateCommandTag(pstmt), CMDTAG_BEGIN);
}

#[test]
fn command_log_levels() {
    use guc_tables::consts::{LOGSTMT_ALL, LOGSTMT_MOD};
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();

    let select = Node::mk(mcx, SelectStmt::default()).unwrap();
    assert_eq!(GetCommandLogLevel(select), LOGSTMT_ALL);

    assert_eq!(GetCommandLogLevel(trans_node(&ctx, TRANS_STMT_COMMIT)), LOGSTMT_ALL);

    let insert_query =
        Node::mk(mcx, Query { commandType: CmdType::CMD_INSERT, ..Query::default() }).unwrap();
    assert_eq!(GetCommandLogLevel(insert_query), LOGSTMT_MOD);

    let util_query = Node::mk(
        mcx,
        Query {
            commandType: CmdType::CMD_UTILITY,
            utilityStmt: Some(trans_node(&ctx, TRANS_STMT_BEGIN)),
            ..Query::default()
        },
    )
    .unwrap();
    assert_eq!(GetCommandLogLevel(util_query), LOGSTMT_ALL);
}

#[test]
fn classify_transaction_stmt_read_only() {
    let ctx = MemoryContext::new("t");
    for kind in [
        TRANS_STMT_BEGIN,
        TRANS_STMT_START,
        TRANS_STMT_COMMIT,
        TRANS_STMT_ROLLBACK,
        TRANS_STMT_SAVEPOINT,
        TRANS_STMT_RELEASE,
        TRANS_STMT_ROLLBACK_TO,
    ] {
        assert_eq!(
            ClassifyUtilityCommandAsReadOnly(trans_node(&ctx, kind)).unwrap(),
            COMMAND_IS_STRICTLY_READ_ONLY
        );
    }
    for kind in [TRANS_STMT_PREPARE, TRANS_STMT_COMMIT_PREPARED, TRANS_STMT_ROLLBACK_PREPARED] {
        assert_eq!(
            ClassifyUtilityCommandAsReadOnly(trans_node(&ctx, kind)).unwrap(),
            COMMAND_OK_IN_READ_ONLY_TXN
        );
    }
}

#[test]
fn utility_returns_tuples_and_descriptor_defaults() {
    let ctx = MemoryContext::new("t");
    let stmt = trans_node(&ctx, TRANS_STMT_BEGIN);
    assert!(!UtilityReturnsTuples(stmt));
    assert!(UtilityTupleDescriptor(stmt).unwrap().is_none());
}

#[test]
fn command_is_read_only_shapes() {
    let select = PlannedStmt { commandType: CmdType::CMD_SELECT, ..PlannedStmt::default() };
    assert!(CommandIsReadOnly(&select));

    let modifying = PlannedStmt {
        commandType: CmdType::CMD_SELECT,
        hasModifyingCTE: true,
        ..PlannedStmt::default()
    };
    assert!(!CommandIsReadOnly(&modifying));

    let insert = PlannedStmt { commandType: CmdType::CMD_INSERT, ..PlannedStmt::default() };
    assert!(!CommandIsReadOnly(&insert));

    let utility = PlannedStmt { commandType: CmdType::CMD_UTILITY, ..PlannedStmt::default() };
    assert!(!CommandIsReadOnly(&utility));
}

fn run_utility(ctx: &MemoryContext, kind: TransactionStmtKind) -> types_error::PgResult<()> {
    let stmt = trans_node(ctx, kind);
    let pstmt = PlannedStmt {
        commandType: CmdType::CMD_UTILITY,
        utilityStmt: Some(stmt),
        ..PlannedStmt::default()
    };
    let mut receiver = tcop_dest::CreateDestReceiver(types_dest::CommandDest::None);
    let mut qc = types_portal::QueryCompletion::default();
    ProcessUtility(
        ctx.mcx(),
        &pstmt,
        "test",
        false,
        utility_seams::PROCESS_UTILITY_TOPLEVEL,
        types_portal::ParamListHandle::NULL,
        types_portal::QueryEnvHandle::NULL,
        &mut receiver,
        Some(&mut qc),
    )
}

// The full BEGIN/COMMIT round trip needs a live StartTransactionCommand (the
// whole backend seam fleet); it rides the M1 statement gate. Here: the
// SAVEPOINT-family dispatch reaches xact's transaction-block guard with C's
// exact SQLSTATE from outside a block.
#[test]
fn dispatch_savepoint_family_requires_transaction_block() {
    install_xact_test_seams();
    let ctx = MemoryContext::new("t");

    for kind in [TRANS_STMT_SAVEPOINT, TRANS_STMT_RELEASE, TRANS_STMT_ROLLBACK_TO] {
        xact::reset_xact_state_for_tests();
        let err = run_utility(&ctx, kind).unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_NO_ACTIVE_SQL_TRANSACTION);
    }
}

#[test]
fn seams_installed() {
    crate::tests::INIT_SEAMS.call_once(init_seams);
    let ctx = MemoryContext::new("t");
    let node = trans_node(&ctx, TRANS_STMT_BEGIN);
    assert_eq!(utility_seams::create_command_tag::call(node), CMDTAG_BEGIN);
    assert_eq!(utility_seams::get_command_log_level::call(node), guc_tables::consts::LOGSTMT_ALL);
    assert!(!utility_seams::utility_returns_tuples::call(node));
    assert!(utility_seams::utility_tuple_descriptor::call(node).unwrap().is_none());
    assert!(utility_seams::process_utility::is_installed());
}

static INIT_SEAMS: std::sync::Once = std::sync::Once::new();

fn install_xact_test_seams() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        guc_tables::init_seams();
        miscinit::init_seams();
        parallel_seams::is_parallel_worker::set(|| false);
        transam_xlog_seams::recovery_in_progress::set(|| false);
    });
}

#[test]
fn create_command_tag_variable_set() {
    use types_nodes::parsenodes::{VariableSetKind, VariableSetStmt};
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    for (kind, tag) in [
        (VariableSetKind::VAR_SET_VALUE, CMDTAG_SET),
        (VariableSetKind::VAR_SET_CURRENT, CMDTAG_SET),
        (VariableSetKind::VAR_SET_DEFAULT, CMDTAG_SET),
        (VariableSetKind::VAR_SET_MULTI, CMDTAG_SET),
        (VariableSetKind::VAR_RESET, CMDTAG_RESET),
        (VariableSetKind::VAR_RESET_ALL, CMDTAG_RESET),
    ] {
        let node = Node::mk(
            mcx,
            VariableSetStmt { kind, name: Some("x"), ..VariableSetStmt::default() },
        )
        .unwrap();
        assert_eq!(CreateCommandTag(node), tag);
    }
}

// AlterObjectTypeCommandTag arms over the ALTER statement vocabulary,
// including non-table object types (C utility.c: default = CMDTAG_UNKNOWN).
#[test]
fn create_command_tag_alter_object_types() {
    use types_nodes::parsenodes::{
        AlterObjectSchemaStmt, AlterOwnerStmt, AlterTableStmt, ObjectType, RenameStmt,
    };
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();

    for (objtype, tag) in [
        (ObjectType::OBJECT_TABLE, CMDTAG_ALTER_TABLE),
        (ObjectType::OBJECT_INDEX, CMDTAG_ALTER_INDEX),
        (ObjectType::OBJECT_SEQUENCE, CMDTAG_ALTER_SEQUENCE),
        (ObjectType::OBJECT_VIEW, CMDTAG_ALTER_VIEW),
        (ObjectType::OBJECT_MATVIEW, CMDTAG_ALTER_MATERIALIZED_VIEW),
        (ObjectType::OBJECT_FOREIGN_TABLE, CMDTAG_ALTER_FOREIGN_TABLE),
        (ObjectType::OBJECT_TYPE, CMDTAG_ALTER_TYPE),
    ] {
        let node = Node::mk(mcx, AlterTableStmt { objtype, ..Default::default() }).unwrap();
        assert_eq!(CreateCommandTag(node), tag);
    }

    for (objtype, tag) in [
        (ObjectType::OBJECT_FUNCTION, CMDTAG_ALTER_FUNCTION),
        (ObjectType::OBJECT_CAST, CMDTAG_ALTER_CAST),
        (ObjectType::OBJECT_TSCONFIGURATION, CMDTAG_ALTER_TEXT_SEARCH_CONFIGURATION),
        (ObjectType::OBJECT_LARGEOBJECT, CMDTAG_ALTER_LARGE_OBJECT),
    ] {
        let node =
            Node::mk(mcx, AlterOwnerStmt { objectType: objtype, ..Default::default() }).unwrap();
        assert_eq!(CreateCommandTag(node), tag);
    }

    let node = Node::mk(
        mcx,
        AlterObjectSchemaStmt {
            objectType: ObjectType::OBJECT_OPERATOR,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(CreateCommandTag(node), CMDTAG_ALTER_OPERATOR);

    // Column rename tags from the relation type; trigger rename is non-table.
    let node = Node::mk(
        mcx,
        RenameStmt {
            renameType: ObjectType::OBJECT_COLUMN,
            relationType: ObjectType::OBJECT_FOREIGN_TABLE,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(CreateCommandTag(node), CMDTAG_ALTER_FOREIGN_TABLE);
    let node = Node::mk(
        mcx,
        RenameStmt { renameType: ObjectType::OBJECT_TRIGGER, ..Default::default() },
    )
    .unwrap();
    assert_eq!(CreateCommandTag(node), CMDTAG_ALTER_TRIGGER);
}

#[test]
fn explain_log_level_and_descriptor() {
    use guc_tables::consts::{LOGSTMT_ALL, LOGSTMT_MOD};
    use types_nodes::parsenodes::{DefElem, ExplainStmt};

    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        syscache_seams::lookup_pg_type_shape::set(|typid| {
            Ok((typid == types_core::TEXTOID).then_some(types_tuple::PgTypeShape {
                typlen: -1,
                typbyval: false,
                typalign: b'i' as i8,
                typstorage: b'x' as i8,
                typcollation: 100,
            }))
        });
    });

    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();

    let insert_query =
        Node::mk(mcx, Query { commandType: CmdType::CMD_INSERT, ..Query::default() }).unwrap();
    let plain = Node::mk(
        mcx,
        ExplainStmt { query: Some(insert_query), ..ExplainStmt::default() },
    )
    .unwrap();
    // Plain EXPLAIN never recurses; EXPLAIN ANALYZE takes the inner level.
    assert_eq!(GetCommandLogLevel(plain), LOGSTMT_ALL);

    let analyze = Node::mk(
        mcx,
        DefElem { defname: Some("analyze"), ..DefElem::default() },
    )
    .unwrap();
    let analyzed = Node::mk(
        mcx,
        ExplainStmt {
            query: Some(insert_query),
            options: types_nodes::list::NodeList::make1(mcx, analyze).unwrap(),
        },
    )
    .unwrap();
    assert_eq!(GetCommandLogLevel(analyzed), LOGSTMT_MOD);

    assert!(UtilityReturnsTuples(plain));
    let desc = UtilityTupleDescriptor(plain).unwrap().unwrap();
    assert_eq!(desc.natts, 1);
    assert_eq!(desc.attr(0).atttypid, types_core::TEXTOID);
    assert_eq!(desc.attr(0).attname.name_str(), b"QUERY PLAN");
}

#[test]
fn fetch_stmt_tag_returns_and_descriptor() {
    use types_nodes::parsenodes::FetchStmt;

    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();

    let fetch = Node::mk(
        mcx,
        FetchStmt { portalname: Some("nope"), ..FetchStmt::default() },
    )
    .unwrap();
    assert_eq!(CreateCommandTag(fetch), CMDTAG_FETCH);
    // Unknown portal is not our error to raise here (C returns false/NULL).
    assert!(!UtilityReturnsTuples(fetch));
    assert!(UtilityTupleDescriptor(fetch).unwrap().is_none());

    let mv = Node::mk(
        mcx,
        FetchStmt { portalname: Some("nope"), ismove: true, ..FetchStmt::default() },
    )
    .unwrap();
    assert_eq!(CreateCommandTag(mv), CMDTAG_MOVE);
    assert!(!UtilityReturnsTuples(mv));
    assert!(UtilityTupleDescriptor(mv).unwrap().is_none());
}
