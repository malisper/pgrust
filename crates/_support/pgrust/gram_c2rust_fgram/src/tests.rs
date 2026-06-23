//! Broad SQL parse suite: drives the productionized `raw_parser` over a variety
//! of statements and checks each parses to the correct top-level repo NodeTag.

use crate::raw_parser;
use backend_nodes_types::node_tags::*;
use backend_nodes_types::parsenodes_stmts::RawStmt;
use ::pg_ffi_fgram::spi::RAW_PARSE_DEFAULT;
use pg_ffi_fgram::{List, Node, NodeTag};

/// Parse `sql` and return the top NodeTag of the first statement (the `stmt`
/// inside the first `RawStmt`), or `None` on a parse failure / empty result.
unsafe fn top_tag(sql: &str) -> Option<NodeTag> {
    let list = raw_parser(sql, RAW_PARSE_DEFAULT);
    if list.is_null() {
        return None;
    }
    let list: &List = &*list;
    if list.len() < 1 {
        return None;
    }
    // First cell -> RawStmt*.
    let cell = &list.cells()[0];
    let rawstmt = cell.ptr::<RawStmt>();
    if rawstmt.is_null() {
        return None;
    }
    let stmt: *mut Node = (*rawstmt).stmt;
    if stmt.is_null() {
        return None;
    }
    Some((*stmt).type_)
}

fn tag_name(tag: NodeTag) -> &'static str {
    match tag {
        T_SelectStmt => "SelectStmt",
        T_InsertStmt => "InsertStmt",
        T_UpdateStmt => "UpdateStmt",
        T_DeleteStmt => "DeleteStmt",
        T_CreateStmt => "CreateStmt",
        T_IndexStmt => "IndexStmt",
        _ => "OTHER",
    }
}

#[test]
fn broad_sql_parse_suite() {
    let cases: &[(&str, NodeTag)] = &[
        (
            "CREATE TABLE t (a int PRIMARY KEY, b text NOT NULL)",
            T_CreateStmt,
        ),
        ("INSERT INTO t VALUES (1,'x')", T_InsertStmt),
        (
            "SELECT a,count(*) FROM t JOIN u ON t.a=u.a WHERE a>1 GROUP BY a HAVING count(*)>0 ORDER BY a",
            T_SelectStmt,
        ),
        ("SELECT a FROM t WHERE a IN (SELECT b FROM u)", T_SelectStmt),
        ("WITH c AS (SELECT 1 x) SELECT x FROM c", T_SelectStmt),
        ("UPDATE t SET a=2 WHERE b='x'", T_UpdateStmt),
        ("DELETE FROM t WHERE a=1", T_DeleteStmt),
        ("CREATE INDEX i ON t(a)", T_IndexStmt),
    ];

    let mut failures = Vec::new();
    for (sql, want) in cases {
        let got = unsafe { top_tag(sql) };
        match got {
            Some(tag) if tag == *want => {
                println!("OK   [{}]  {}", tag_name(tag), sql);
            }
            Some(tag) => {
                println!(
                    "BAD  got [{}] want [{}]  {}",
                    tag_name(tag),
                    tag_name(*want),
                    sql
                );
                failures.push((*sql, tag_name(tag), tag_name(*want)));
            }
            None => {
                println!("FAIL parse returned NIL  {}", sql);
                failures.push((*sql, "NIL", tag_name(*want)));
            }
        }
    }

    assert!(failures.is_empty(), "parse failures: {:?}", failures);
}

/// Return the first statement node (the `stmt` of the first `RawStmt`).
unsafe fn first_stmt(sql: &str) -> *mut Node {
    let list = raw_parser(sql, RAW_PARSE_DEFAULT);
    assert!(!list.is_null(), "parse returned NIL for: {sql}");
    let list: &List = &*list;
    let rawstmt = list.cells()[0].ptr::<RawStmt>();
    (*rawstmt).stmt
}

#[test]
fn create_table_tree_is_populated() {
    use backend_nodes_types::parsenodes_ddl::CreateStmt;
    let stmt = unsafe { first_stmt("CREATE TABLE t (a int PRIMARY KEY, b text NOT NULL)") };
    let cs = stmt.cast::<CreateStmt>();
    unsafe {
        // Relation name "t".
        let rv = (*cs).relation;
        assert!(!rv.is_null());
        let name = std::ffi::CStr::from_ptr((*rv).relname).to_str().unwrap();
        assert_eq!(name, "t");
        // Two column definitions (a, b).
        let elts = (*cs).tableElts;
        assert!(!elts.is_null());
        assert_eq!((*elts).len(), 2, "expected 2 column defs");
    }
}

#[test]
fn select_join_tree_is_populated() {
    use backend_nodes_types::parsenodes_stmts::SelectStmt;
    let stmt = unsafe {
        first_stmt(
            "SELECT a,count(*) FROM t JOIN u ON t.a=u.a WHERE a>1 GROUP BY a HAVING count(*)>0 ORDER BY a",
        )
    };
    let ss = stmt.cast::<SelectStmt>();
    unsafe {
        // Two target-list entries (a, count(*)).
        assert_eq!((*(*ss).targetList).len(), 2, "expected 2 targets");
        // A FROM clause with one (join) item, a WHERE, a GROUP BY, a HAVING.
        assert!(!(*ss).fromClause.is_null());
        assert_eq!((*(*ss).fromClause).len(), 1, "expected 1 from item (join)");
        assert!(!(*ss).whereClause.is_null(), "expected WHERE");
        assert!(!(*ss).groupClause.is_null(), "expected GROUP BY");
        assert!(!(*ss).havingClause.is_null(), "expected HAVING");
        assert!(!(*ss).sortClause.is_null(), "expected ORDER BY");
    }
}

#[test]
fn extended_coverage_parses() {
    // A wider variety of statement shapes all parse without error.
    let cases: &[&str] = &[
        "CREATE TABLE t (a int PRIMARY KEY, b text NOT NULL, c numeric DEFAULT 0, UNIQUE(a,b))",
        "INSERT INTO t (a,b) SELECT x, y FROM s WHERE x > 0 RETURNING a",
        "SELECT a, sum(b) AS s FROM t GROUP BY a HAVING sum(b) > 10 ORDER BY s DESC LIMIT 5",
        "SELECT * FROM t WHERE a IN (SELECT b FROM u WHERE u.c = t.c)",
        "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 10) SELECT * FROM r",
        "UPDATE t SET a = a + 1, b = 'z' WHERE c IS NOT NULL",
        "DELETE FROM t USING u WHERE t.a = u.a",
        "CREATE UNIQUE INDEX CONCURRENTLY i ON t USING btree (a, lower(b))",
        "SELECT a FROM t LEFT JOIN u ON t.a = u.a FULL JOIN v ON v.a = t.a",
        "SELECT CASE WHEN a > 0 THEN 'pos' ELSE 'neg' END FROM t",
    ];
    for sql in cases {
        let list = unsafe { raw_parser(sql, RAW_PARSE_DEFAULT) };
        assert!(!list.is_null(), "parse returned NIL for: {sql}");
    }
}

#[test]
fn parses_multiple_statements() {
    // A multi-statement string -> a List of two RawStmts.
    let list = unsafe { raw_parser("SELECT 1; SELECT 2", RAW_PARSE_DEFAULT) };
    assert!(!list.is_null());
    let list: &List = unsafe { &*list };
    assert_eq!(list.len(), 2, "expected two statements");
}

#[test]
fn syntax_error_returns_nil() {
    // A bogus statement -> NIL (the C contract for a parse error).
    let list = unsafe { raw_parser("SELECT FROM WHERE", RAW_PARSE_DEFAULT) };
    assert!(list.is_null(), "syntax error should return NIL");
}

#[test]
fn syntax_error_records_pg_message_sqlstate_cursor() {
    // Lexer-level yyerror (scan.l integer_junk): C renders
    //   trailing junk after numeric literal at or near "123abc"
    // with ERRCODE_SYNTAX_ERROR and a 1-based cursor at the token start.
    let list = unsafe { raw_parser("SELECT 123abc;", RAW_PARSE_DEFAULT) };
    assert!(list.is_null());
    let (msg, state, pos) = crate::support::last_error().expect("error recorded");
    assert_eq!(msg, "trailing junk after numeric literal at or near \"123abc\"");
    assert_eq!(&state, b"42601");
    assert_eq!(pos, 8);

    // Parser-level yyerror ("syntax error") points at the offending token.
    let list = unsafe { raw_parser("SELECT _1_000.5;", RAW_PARSE_DEFAULT) };
    assert!(list.is_null());
    let (msg, state, pos) = crate::support::last_error().expect("error recorded");
    assert_eq!(msg, "syntax error at or near \".5\"");
    assert_eq!(&state, b"42601");
    assert_eq!(pos, 14);

    // Param overflow (scan.l param rule).
    let list = unsafe { raw_parser("PREPARE p1 AS SELECT $2147483648;", RAW_PARSE_DEFAULT) };
    assert!(list.is_null());
    let (msg, ..) = crate::support::last_error().expect("error recorded");
    assert_eq!(msg, "parameter number too large at or near \"$2147483648\"");

    // A successful parse leaves no recorded error (NIL would mean empty input).
    let list = unsafe { raw_parser("SELECT 1", RAW_PARSE_DEFAULT) };
    assert!(!list.is_null());
    assert!(crate::support::last_error().is_none());
}
