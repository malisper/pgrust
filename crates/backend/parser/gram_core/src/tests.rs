//! End-to-end: SQL text → `base_yyparse` → owned `RawStmt` (shape asserts).

use mcx::{Mcx, MemoryContext};
use ::parsenodes::RawParseMode;

/// Run the converter (not the seam) directly over a query and return the owned
/// statements.
fn parse<'mcx>(
    mcx: Mcx<'mcx>,
    sql: &'mcx str,
) -> Vec<nodes::parsestmt::RawStmt<'mcx>> {
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
        {
            let n = &*stmts[0].stmt;
            let s = n
                .as_selectstmt()
                .unwrap_or_else(|| panic!("expected SelectStmt, got {:?}", n.node_tag()));
            assert_eq!(s.targetList.len(), 2, "two target columns");
            assert_eq!(s.fromClause.len(), 1, "one FROM item");
            assert!(s.whereClause.is_some(), "has WHERE");
            // WHERE a > 1 is an A_Expr (operator).
            let w = s.whereClause.as_deref();
            assert!(
                w.map(|n| n.node_tag()) == Some(nodes::nodes::ntag::T_A_Expr),
                "WHERE should be A_Expr, got {:?}",
                w.map(|n| n.node_tag())
            );
        }

        // INSERT ... VALUES.
        let stmts = parse(mcx, "INSERT INTO t (a, b) VALUES (1, 'x')");
        assert_eq!(stmts.len(), 1);
        { let n = &*stmts[0].stmt; let s = n.as_insertstmt().unwrap_or_else(|| panic!("expected InsertStmt, got {:?}", n.node_tag())); assert_eq!(s.cols.len(), 2, "two insert columns");
                assert!(s.relation.is_some(), "has relation");
                assert!(s.selectStmt.is_some(), "VALUES is a SelectStmt"); }

        // UPDATE ... SET ... WHERE.
        let stmts = parse(mcx, "UPDATE t SET a = 2 WHERE b > 0");
        { let n = &*stmts[0].stmt; let s = n.as_updatestmt().unwrap_or_else(|| panic!("expected UpdateStmt, got {:?}", n.node_tag())); assert_eq!(s.targetList.len(), 1);
                assert!(s.whereClause.is_some()); }

        // DELETE ... WHERE.
        let stmts = parse(mcx, "DELETE FROM t WHERE a > 1");
        { let n = &*stmts[0].stmt; let s = n.as_deletestmt().unwrap_or_else(|| panic!("expected DeleteStmt, got {:?}", n.node_tag())); assert!(s.relation.is_some());
                assert!(s.whereClause.is_some()); }

        // Multiple statements.
        let stmts = parse(mcx, "SELECT 1; SELECT 2");
        assert_eq!(stmts.len(), 2, "two statements");

        // A function call + constant literal.
        let stmts = parse(mcx, "SELECT count(a) FROM t");
        { let n = &*stmts[0].stmt; let s = n.as_selectstmt().unwrap_or_else(|| panic!("expected SelectStmt, got {:?}", n.node_tag())); let tgt = &s.targetList[0];
                { let n = &**tgt; let rt = n.as_restarget().unwrap_or_else(|| panic!("target should be ResTarget, got {:?}", n.node_tag())); let __v = rt.val.as_deref(); assert!(__v.map(|n| n.node_tag()) == Some(nodes::nodes::ntag::T_FuncCall), "target val should be FuncCall, got {:?}", __v.map(|n| n.node_tag())); } }
}

#[test]
fn create_family_statements_convert_to_owned_rawstmt() {
    let ctx = MemoryContext::new("gram-test");
    let mcx = ctx.mcx();

    // CREATE TABLE with two columns and a column constraint.
    let stmts = parse(mcx, "CREATE TABLE t (a int PRIMARY KEY, b text NOT NULL)");
    assert_eq!(stmts.len(), 1);
    { let n = &*stmts[0].stmt; let s = n.as_createstmt().unwrap_or_else(|| panic!("expected CreateStmt, got {:?}", n.node_tag())); assert!(s.relation.is_some(), "has relation");
            assert_eq!(s.tableElts.len(), 2, "two table elements");
            { let n = &*s.tableElts[0]; let c = n.as_columndef().unwrap_or_else(|| panic!("expected ColumnDef, got {:?}", n.node_tag())); assert!(c.typeName.is_some(), "column has a type");
                    // PRIMARY KEY is a Constraint in the column's constraints.
                    assert_eq!(c.constraints.len(), 1, "one column constraint");
                    { let n = &*c.constraints[0]; assert!(n.as_constraint().is_some(), "expected Constraint, got {:?}", n.node_tag()); } } }

    // CREATE INDEX with one index column.
    let stmts = parse(mcx, "CREATE INDEX ix ON t (a)");
    { let n = &*stmts[0].stmt; let s = n.as_indexstmt().unwrap_or_else(|| panic!("expected IndexStmt, got {:?}", n.node_tag())); assert!(s.relation.is_some());
            assert_eq!(s.indexParams.len(), 1, "one index column");
            { let n = &*s.indexParams[0]; assert!(n.as_indexelem().is_some(), "expected IndexElem, got {:?}", n.node_tag()); } }

    // CREATE SEQUENCE.
    let stmts = parse(mcx, "CREATE SEQUENCE s START 1");
    { let n = &*stmts[0].stmt; let s = n.as_createseqstmt().unwrap_or_else(|| panic!("expected CreateSeqStmt, got {:?}", n.node_tag())); assert!(s.sequence.is_some());
            assert!(!s.options.is_empty(), "START produces an option DefElem"); }

    // CREATE VIEW over a SELECT.
    let stmts = parse(mcx, "CREATE VIEW v AS SELECT a FROM t");
    { let n = &*stmts[0].stmt; let s = n.as_viewstmt().unwrap_or_else(|| panic!("expected ViewStmt, got {:?}", n.node_tag())); assert!(s.view.is_some());
            assert!(s.query.is_some(), "view has a query"); }

    // CREATE FUNCTION with a parameter.
    let stmts = parse(
        mcx,
        "CREATE FUNCTION f(x int) RETURNS int LANGUAGE sql AS 'SELECT $1'",
    );
    { let n = &*stmts[0].stmt; let s = n.as_createfunctionstmt().unwrap_or_else(|| panic!("expected CreateFunctionStmt, got {:?}", n.node_tag())); assert_eq!(s.funcname.len(), 1, "function name");
            assert_eq!(s.parameters.len(), 1, "one parameter");
            { let n = &*s.parameters[0]; assert!(n.as_functionparameter().is_some(), "expected FunctionParameter, got {:?}", n.node_tag()); }
            assert!(s.returnType.is_some()); }

    // CREATE SCHEMA AUTHORIZATION (RoleSpec).
    let stmts = parse(mcx, "CREATE SCHEMA myschema");
    { let n = &*stmts[0].stmt; let s = n.as_createschemastmt().unwrap_or_else(|| panic!("expected CreateSchemaStmt, got {:?}", n.node_tag())); assert!(s.schemaname.is_some()); }

    // CREATE TYPE ... AS ENUM.
    let stmts = parse(mcx, "CREATE TYPE color AS ENUM ('red', 'green')");
    { let n = &*stmts[0].stmt; let s = n.as_createenumstmt().unwrap_or_else(|| panic!("expected CreateEnumStmt, got {:?}", n.node_tag())); assert_eq!(s.vals.len(), 2, "two enum labels"); }

    // CREATE TABLE AS (IntoClause).
    let stmts = parse(mcx, "CREATE TABLE ct AS SELECT a FROM t");
    { let n = &*stmts[0].stmt; let s = n.as_createtableasstmt().unwrap_or_else(|| panic!("expected CreateTableAsStmt, got {:?}", n.node_tag())); assert!(s.query.is_some());
            assert!(s.into.is_some(), "has an IntoClause");
            { let __v = s.into.as_deref(); assert!(__v.map(|n| n.node_tag()) == Some(nodes::nodes::ntag::T_IntoClause), "into should be IntoClause, got {:?}", __v.map(|n| n.node_tag())); } }
}

#[test]
fn alter_drop_family_statements_convert_to_owned_rawstmt() {
    let ctx = MemoryContext::new("gram-test");
    let mcx = ctx.mcx();

    // ALTER TABLE ... ADD COLUMN ... , DROP COLUMN ...
    let stmts = parse(mcx, "ALTER TABLE t ADD COLUMN c int, DROP COLUMN b");
    assert_eq!(stmts.len(), 1);
    { let n = &*stmts[0].stmt; let s = n.as_altertablestmt().unwrap_or_else(|| panic!("expected AlterTableStmt, got {:?}", n.node_tag())); assert!(s.relation.is_some(), "has relation");
            assert_eq!(s.cmds.len(), 2, "two subcommands");
            for cmd in s.cmds.iter() {
                { let n = &**cmd; assert!(n.as_altertablecmd().is_some(), "expected AlterTableCmd, got {:?}", n.node_tag()); }
            } }

    // ALTER TABLE ... RENAME COLUMN ... TO ...
    let stmts = parse(mcx, "ALTER TABLE t RENAME COLUMN a TO z");
    { let n = &*stmts[0].stmt; let s = n.as_renamestmt().unwrap_or_else(|| panic!("expected RenameStmt, got {:?}", n.node_tag())); assert!(s.relation.is_some());
            assert!(s.subname.is_some(), "old column name");
            assert!(s.newname.is_some(), "new column name"); }

    // DROP TABLE ... (objects list)
    let stmts = parse(mcx, "DROP TABLE t1, t2 CASCADE");
    { let n = &*stmts[0].stmt; let s = n.as_dropstmt().unwrap_or_else(|| panic!("expected DropStmt, got {:?}", n.node_tag())); assert_eq!(s.objects.len(), 2, "two dropped objects");
            assert_eq!(s.behavior, nodes::parsenodes::DROP_CASCADE); }

    // ALTER SEQUENCE ... RESTART
    let stmts = parse(mcx, "ALTER SEQUENCE s RESTART WITH 5");
    { let n = &*stmts[0].stmt; let s = n.as_alterseqstmt().unwrap_or_else(|| panic!("expected AlterSeqStmt, got {:?}", n.node_tag())); assert!(s.sequence.is_some());
            assert_eq!(s.options.len(), 1, "one sequence option"); }

    // ALTER TABLE ... OWNER TO  (object-form RenameStmt sibling: AlterOwnerStmt
    // is produced by ALTER <other-object> OWNER; for tables it is an
    // AlterTableCmd AT_ChangeOwner. Use ALTER SCHEMA RENAME for AlterOwner path
    // via ALTER TYPE ... OWNER TO.)
    let stmts = parse(mcx, "ALTER TYPE ty OWNER TO bob");
    { let n = &*stmts[0].stmt; let s = n.as_alterownerstmt().unwrap_or_else(|| panic!("expected AlterOwnerStmt, got {:?}", n.node_tag())); assert!(s.newowner.is_some(), "has new owner RoleSpec"); }
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

#[test]
fn utility_statements_convert_to_owned_rawstmt() {
    let ctx = MemoryContext::new("gram-test-f4");
    let mcx = ctx.mcx();

    { let n = &*parse(mcx, "GRANT SELECT, INSERT ON t TO alice")[0].stmt; let s = n.as_grantstmt().unwrap_or_else(|| panic!("expected GrantStmt, got {:?}", n.node_tag())); assert!(s.is_grant);
            assert_eq!(s.privileges.len(), 2);
            assert_eq!(s.grantees.len(), 1); }
    { let n = &*parse(mcx, "GRANT admins TO alice")[0].stmt; let s = n.as_grantrolestmt().unwrap_or_else(|| panic!("expected GrantRoleStmt, got {:?}", n.node_tag())); assert!(s.is_grant);
            assert_eq!(s.granted_roles.len(), 1);
            assert_eq!(s.grantee_roles.len(), 1); }
    { let n = &*parse(mcx, "SET work_mem = '64MB'")[0].stmt; let s = n.as_variablesetstmt().unwrap_or_else(|| panic!("expected VariableSetStmt, got {:?}", n.node_tag())); assert_eq!(s.kind, nodes::ddlnodes::VAR_SET_VALUE);
            assert!(s.name.is_some());
            assert_eq!(s.args.len(), 1); }
    { let n = &*parse(mcx, "SHOW work_mem")[0].stmt; let s = n.as_variableshowstmt().unwrap_or_else(|| panic!("expected VariableShowStmt, got {:?}", n.node_tag())); assert!(s.name.is_some()) }
    { let n = &*parse(mcx, "BEGIN")[0].stmt; let s = n.as_transactionstmt().unwrap_or_else(|| panic!("expected TransactionStmt, got {:?}", n.node_tag())); assert_eq!(s.kind, nodes::ddlnodes::TRANS_STMT_BEGIN) }
    { let n = &*parse(mcx, "COMMIT")[0].stmt; let s = n.as_transactionstmt().unwrap_or_else(|| panic!("expected TransactionStmt, got {:?}", n.node_tag())); assert_eq!(s.kind, nodes::ddlnodes::TRANS_STMT_COMMIT) }
    { let n = &*parse(mcx, "COPY t (a, b) FROM '/tmp/x'")[0].stmt; let s = n.as_copystmt().unwrap_or_else(|| panic!("expected CopyStmt, got {:?}", n.node_tag())); assert!(s.relation.is_some());
            assert!(s.is_from);
            assert_eq!(s.attlist.len(), 2);
            assert!(s.filename.is_some()); }
    { let n = &*parse(mcx, "EXPLAIN SELECT 1")[0].stmt; let s = n.as_explainstmt().unwrap_or_else(|| panic!("expected ExplainStmt, got {:?}", n.node_tag())); assert!(s.query.is_some()) }
    { let n = &*parse(mcx, "VACUUM t")[0].stmt; let s = n.as_vacuumstmt().unwrap_or_else(|| panic!("expected VacuumStmt, got {:?}", n.node_tag())); assert!(s.is_vacuumcmd);
            assert_eq!(s.rels.len(), 1); }
    { let n = &*parse(mcx, "PREPARE p (int) AS SELECT $1")[0].stmt; let s = n.as_preparestmt().unwrap_or_else(|| panic!("expected PrepareStmt, got {:?}", n.node_tag())); assert!(s.name.is_some());
            assert_eq!(s.argtypes.len(), 1);
            assert!(s.query.is_some()); }
    { let n = &*parse(mcx, "EXECUTE p (1)")[0].stmt; let s = n.as_executestmt().unwrap_or_else(|| panic!("expected ExecuteStmt, got {:?}", n.node_tag())); assert!(s.name.is_some());
            assert_eq!(s.params.len(), 1); }
    { let n = &*parse(mcx, "COMMENT ON TABLE t IS 'hi'")[0].stmt; let s = n.as_commentstmt().unwrap_or_else(|| panic!("expected CommentStmt, got {:?}", n.node_tag())); assert!(s.object.is_some());
            assert!(s.comment.is_some()); }
    { let n = &*parse(mcx, "CHECKPOINT")[0].stmt; assert!(n.as_checkpointstmt().is_some(), "expected CheckPointStmt, got {:?}", n.node_tag()); }
    { let n = &*parse(mcx, "DISCARD ALL")[0].stmt; let s = n.as_discardstmt().unwrap_or_else(|| panic!("expected DiscardStmt, got {:?}", n.node_tag())); assert_eq!(s.target, nodes::ddlnodes::DISCARD_ALL) }
    { let n = &*parse(mcx, "LOCK TABLE t")[0].stmt; let s = n.as_lockstmt().unwrap_or_else(|| panic!("expected LockStmt, got {:?}", n.node_tag())); assert_eq!(s.relations.len(), 1) }
    { let n = &*parse(mcx, "NOTIFY chan, 'payload'")[0].stmt; let s = n.as_notifystmt().unwrap_or_else(|| panic!("expected NotifyStmt, got {:?}", n.node_tag())); assert!(s.conditionname.is_some());
            assert!(s.payload.is_some()); }
    { let n = &*parse(mcx, "TRUNCATE t")[0].stmt; let s = n.as_truncatestmt().unwrap_or_else(|| panic!("expected TruncateStmt, got {:?}", n.node_tag())); assert_eq!(s.relations.len(), 1) }
    { let n = &*parse(mcx, "DECLARE c CURSOR FOR SELECT 1")[0].stmt; let s = n.as_declarecursorstmt().unwrap_or_else(|| panic!("expected DeclareCursorStmt, got {:?}", n.node_tag())); assert!(s.portalname.is_some());
            assert!(s.query.is_some()); }
    { let n = &*parse(mcx, "FETCH 5 FROM c")[0].stmt; let s = n.as_fetchstmt().unwrap_or_else(|| panic!("expected FetchStmt, got {:?}", n.node_tag())); assert!(s.portalname.is_some()) }
    { let n = &*parse(mcx, "DO $$ BEGIN END $$")[0].stmt; let s = n.as_dostmt().unwrap_or_else(|| panic!("expected DoStmt, got {:?}", n.node_tag())); assert!(!s.args.is_empty()) }
    { let n = &*parse(mcx, "CALL p(1)")[0].stmt; let s = n.as_callstmt().unwrap_or_else(|| panic!("expected CallStmt, got {:?}", n.node_tag())); assert!(s.funccall.is_some()) }
    { let n = &*parse(mcx, "REINDEX TABLE t")[0].stmt; let s = n.as_reindexstmt().unwrap_or_else(|| panic!("expected ReindexStmt, got {:?}", n.node_tag())); assert_eq!(s.kind, nodes::ddlnodes::REINDEX_OBJECT_TABLE);
            assert!(s.relation.is_some()); };
}
