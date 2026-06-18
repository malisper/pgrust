//! The SQL-`EXPLAIN` result descriptor of `commands/explain.c`:
//! `ExplainResultDesc` (the `explain_result_desc` `tcop`-utility out-seam).
//!
//! STOP — the `ExplainQuery` / `ExplainOneQuery` / `standard_ExplainOneQuery`
//! SQL entry points are blocked on an option-parser node-universe mismatch, NOT
//! on ruleutils deparse. The executable `ExplainStmt` (produced by the parser's
//! `conv_explainstmt`) carries `types_nodes::ddlnodes::DefElem<'mcx>` options
//! (`arg: Option<Node<'mcx>>`), but the real option parser
//! `ParseExplainOptionList` (owned by `backend-commands-explain-state`) is
//! written against the *raw-grammar* `types_parsenodes::DefElem` (a different
//! Node universe, `arg: Option<Box<types_parsenodes::Node>>`). There is no
//! converter between the two universes and no analyzed-tree option parser.
//! Wiring `ExplainQuery` faithfully requires re-homing `ParseExplainOptionList`
//! onto `ddlnodes::DefElem` (an out-of-lane signature change to the state crate
//! + a Node-universe re-port). `ExplainResultDesc` only reads the `format`
//! option's string value, which the `def_get_string` seam handles directly, so
//! it lands here.

extern crate alloc;

use alloc::string::String;

use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;
use types_nodes::nodes::Node;
use types_tuple::heaptuple::TupleDesc;

use backend_commands_define_seams::DefElemArg;

// TEXTOID / XMLOID / JSONOID (pg_type.h).
const TEXTOID: Oid = 25;
const XMLOID: Oid = 142;
const JSONOID: Oid = 114;

/// `defGetString`'s value projection: map a `ddlnodes::DefElem` arg `Node` to
/// the `DefElemArg` the `def_get_string` seam consumes (mirrors the per-crate
/// adapters, e.g. collationcmds). EXPLAIN's `format` option arg is a bare
/// identifier (`makeString`), so the String/TypeName cases are the ones reached.
fn def_elem_arg(node: &Node<'_>) -> DefElemArg {
    match node {
        Node::Integer(i) => DefElemArg::Integer(i.ival as i64),
        Node::Float(f) => DefElemArg::Float(String::from(f.fval.as_str())),
        Node::Boolean(b) => DefElemArg::Boolean(b.boolval),
        Node::String(s) => DefElemArg::String(String::from(s.sval.as_str())),
        Node::A_Star(_) => DefElemArg::AStar,
        other => panic!("EXPLAIN def_elem_arg: unsupported option arg node {other:?}"),
    }
}

/// `ExplainResultDesc(stmt)` (explain.c:254) — the single-column "QUERY PLAN"
/// result tuple descriptor. Its column type is TEXT / XML / JSON per the last
/// `format` option (the C "don't break, last value wins").
pub fn ExplainResultDesc<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<TupleDesc<'mcx>> {
    let explain = match stmt {
        Node::ExplainStmt(e) => e,
        other => panic!("ExplainResultDesc: not an ExplainStmt: {other:?}"),
    };

    // Check for XML/JSON format option (last value wins).
    let mut result_type = TEXTOID;
    for opt in explain.options.iter() {
        if let Node::DefElem(d) = &**opt {
            if d.defname.as_ref().map(|s| s.as_str()) == Some("format") {
                let defname = d
                    .defname
                    .as_ref()
                    .map(|s| String::from(s.as_str()))
                    .unwrap_or_default();
                let arg = d.arg.as_deref().map(def_elem_arg);
                let p = backend_commands_define_seams::def_get_string::call(mcx, defname, arg)?;
                result_type = match p.as_str() {
                    "xml" => XMLOID,
                    "json" => JSONOID,
                    _ => TEXTOID,
                };
            }
        }
    }

    // Single TEXT/XML/JSON column named "QUERY PLAN".
    let mut tupdesc = backend_access_common_tupdesc::CreateTemplateTupleDesc(mcx, 1)?;
    backend_access_common_tupdesc::TupleDescInitEntry(
        &mut tupdesc,
        1,
        Some("QUERY PLAN"),
        result_type,
        -1,
        0,
    )?;
    Ok(Some(mcx::alloc_in(mcx, tupdesc)?))
}
