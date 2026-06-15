//! End-to-end: SQL text → `base_yyparse` → owned `RawStmt` (shape asserts).

use mcx::{Mcx, MemoryContext};
use types_nodes::nodes::Node;
use types_parsenodes::RawParseMode;

/// Run the converter (not the seam) directly over a query and return the owned
/// statements.
fn parse<'mcx>(
    mcx: Mcx<'mcx>,
    sql: &'mcx str,
) -> Vec<types_nodes::parsestmt::RawStmt<'mcx>> {
    super::base_yyparse(mcx, sql, RawParseMode::RAW_PARSE_DEFAULT)
        .expect("parse should succeed")
        .into_iter()
        .collect()
}

#[test]
fn dml_statements_convert_to_owned_rawstmt() {
    let ctx = MemoryContext::new("gram-test");
    let mcx = ctx.mcx();
        // SELECT with FROM/WHERE(simple op)/target list.
        let q = "SELECT a, b FROM t WHERE a > 1";
        let stmts = parse(mcx, q);
        assert_eq!(stmts.len(), 1);
        match &*stmts[0].stmt {
            Node::SelectStmt(s) => {
                assert_eq!(s.targetList.len(), 2, "two target columns");
                assert_eq!(s.fromClause.len(), 1, "one FROM item");
                assert!(s.whereClause.is_some(), "has WHERE");
                // WHERE a > 1 is an A_Expr (operator).
                match s.whereClause.as_deref() {
                    Some(Node::A_Expr(_)) => {}
                    other => panic!("WHERE should be A_Expr, got {:?}", other.map(|n| n.node_tag())),
                }
            }
            other => panic!("expected SelectStmt, got {:?}", other.node_tag()),
        }

        // INSERT ... VALUES.
        let stmts = parse(mcx, "INSERT INTO t (a, b) VALUES (1, 'x')");
        assert_eq!(stmts.len(), 1);
        match &*stmts[0].stmt {
            Node::InsertStmt(s) => {
                assert_eq!(s.cols.len(), 2, "two insert columns");
                assert!(s.relation.is_some(), "has relation");
                assert!(s.selectStmt.is_some(), "VALUES is a SelectStmt");
            }
            other => panic!("expected InsertStmt, got {:?}", other.node_tag()),
        }

        // UPDATE ... SET ... WHERE.
        let stmts = parse(mcx, "UPDATE t SET a = 2 WHERE b > 0");
        match &*stmts[0].stmt {
            Node::UpdateStmt(s) => {
                assert_eq!(s.targetList.len(), 1);
                assert!(s.whereClause.is_some());
            }
            other => panic!("expected UpdateStmt, got {:?}", other.node_tag()),
        }

        // DELETE ... WHERE.
        let stmts = parse(mcx, "DELETE FROM t WHERE a > 1");
        match &*stmts[0].stmt {
            Node::DeleteStmt(s) => {
                assert!(s.relation.is_some());
                assert!(s.whereClause.is_some());
            }
            other => panic!("expected DeleteStmt, got {:?}", other.node_tag()),
        }

        // Multiple statements.
        let stmts = parse(mcx, "SELECT 1; SELECT 2");
        assert_eq!(stmts.len(), 2, "two statements");

        // A function call + constant literal.
        let stmts = parse(mcx, "SELECT count(a) FROM t");
        match &*stmts[0].stmt {
            Node::SelectStmt(s) => {
                let tgt = &s.targetList[0];
                match &**tgt {
                    Node::ResTarget(rt) => match rt.val.as_deref() {
                        Some(Node::FuncCall(_)) => {}
                        other => panic!("target val should be FuncCall, got {:?}", other.map(|n| n.node_tag())),
                    },
                    other => panic!("target should be ResTarget, got {:?}", other.node_tag()),
                }
            }
            other => panic!("expected SelectStmt, got {:?}", other.node_tag()),
        }
}

#[test]
fn syntax_error_is_an_err() {
    let ctx = MemoryContext::new("gram-test");
    let mcx = ctx.mcx();
        let r = super::base_yyparse(mcx, "SELECT FROM WHERE", RawParseMode::RAW_PARSE_DEFAULT);
        assert!(r.is_err(), "malformed SQL should error");
}

#[test]
fn empty_input_is_empty_vec() {
    let ctx = MemoryContext::new("gram-test");
    let mcx = ctx.mcx();
        let r = super::base_yyparse(mcx, "   ", RawParseMode::RAW_PARSE_DEFAULT)
            .expect("blank input parses to empty");
        assert_eq!(r.len(), 0);
}
