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
fn create_family_statements_convert_to_owned_rawstmt() {
    let ctx = MemoryContext::new("gram-test");
    let mcx = ctx.mcx();

    // CREATE TABLE with two columns and a column constraint.
    let stmts = parse(mcx, "CREATE TABLE t (a int PRIMARY KEY, b text NOT NULL)");
    assert_eq!(stmts.len(), 1);
    match &*stmts[0].stmt {
        Node::CreateStmt(s) => {
            assert!(s.relation.is_some(), "has relation");
            assert_eq!(s.tableElts.len(), 2, "two table elements");
            match &*s.tableElts[0] {
                Node::ColumnDef(c) => {
                    assert!(c.typeName.is_some(), "column has a type");
                    // PRIMARY KEY is a Constraint in the column's constraints.
                    assert_eq!(c.constraints.len(), 1, "one column constraint");
                    match &*c.constraints[0] {
                        Node::Constraint(_) => {}
                        other => panic!("expected Constraint, got {:?}", other.node_tag()),
                    }
                }
                other => panic!("expected ColumnDef, got {:?}", other.node_tag()),
            }
        }
        other => panic!("expected CreateStmt, got {:?}", other.node_tag()),
    }

    // CREATE INDEX with one index column.
    let stmts = parse(mcx, "CREATE INDEX ix ON t (a)");
    match &*stmts[0].stmt {
        Node::IndexStmt(s) => {
            assert!(s.relation.is_some());
            assert_eq!(s.indexParams.len(), 1, "one index column");
            match &*s.indexParams[0] {
                Node::IndexElem(_) => {}
                other => panic!("expected IndexElem, got {:?}", other.node_tag()),
            }
        }
        other => panic!("expected IndexStmt, got {:?}", other.node_tag()),
    }

    // CREATE SEQUENCE.
    let stmts = parse(mcx, "CREATE SEQUENCE s START 1");
    match &*stmts[0].stmt {
        Node::CreateSeqStmt(s) => {
            assert!(s.sequence.is_some());
            assert!(!s.options.is_empty(), "START produces an option DefElem");
        }
        other => panic!("expected CreateSeqStmt, got {:?}", other.node_tag()),
    }

    // CREATE VIEW over a SELECT.
    let stmts = parse(mcx, "CREATE VIEW v AS SELECT a FROM t");
    match &*stmts[0].stmt {
        Node::ViewStmt(s) => {
            assert!(s.view.is_some());
            assert!(s.query.is_some(), "view has a query");
        }
        other => panic!("expected ViewStmt, got {:?}", other.node_tag()),
    }

    // CREATE FUNCTION with a parameter.
    let stmts = parse(
        mcx,
        "CREATE FUNCTION f(x int) RETURNS int LANGUAGE sql AS 'SELECT $1'",
    );
    match &*stmts[0].stmt {
        Node::CreateFunctionStmt(s) => {
            assert_eq!(s.funcname.len(), 1, "function name");
            assert_eq!(s.parameters.len(), 1, "one parameter");
            match &*s.parameters[0] {
                Node::FunctionParameter(_) => {}
                other => panic!("expected FunctionParameter, got {:?}", other.node_tag()),
            }
            assert!(s.returnType.is_some());
        }
        other => panic!("expected CreateFunctionStmt, got {:?}", other.node_tag()),
    }

    // CREATE SCHEMA AUTHORIZATION (RoleSpec).
    let stmts = parse(mcx, "CREATE SCHEMA myschema");
    match &*stmts[0].stmt {
        Node::CreateSchemaStmt(s) => {
            assert!(s.schemaname.is_some());
        }
        other => panic!("expected CreateSchemaStmt, got {:?}", other.node_tag()),
    }

    // CREATE TYPE ... AS ENUM.
    let stmts = parse(mcx, "CREATE TYPE color AS ENUM ('red', 'green')");
    match &*stmts[0].stmt {
        Node::CreateEnumStmt(s) => {
            assert_eq!(s.vals.len(), 2, "two enum labels");
        }
        other => panic!("expected CreateEnumStmt, got {:?}", other.node_tag()),
    }

    // CREATE TABLE AS (IntoClause).
    let stmts = parse(mcx, "CREATE TABLE ct AS SELECT a FROM t");
    match &*stmts[0].stmt {
        Node::CreateTableAsStmt(s) => {
            assert!(s.query.is_some());
            assert!(s.into.is_some(), "has an IntoClause");
            match s.into.as_deref() {
                Some(Node::IntoClause(_)) => {}
                other => panic!("into should be IntoClause, got {:?}", other.map(|n| n.node_tag())),
            }
        }
        other => panic!("expected CreateTableAsStmt, got {:?}", other.node_tag()),
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
