//! Focused tests for the jsonpath executor's pure-logic paths (no datetime /
//! regex / fmgr seam required): root/key/array accessors, comparisons,
//! arithmetic, `.size()`/`.type()`, EXISTS and the `@?`/`@@` operators, and the
//! `.keyvalue()` document-offset id golden tests.
//!
//! Documents are built via `JsonbValueToJsonb` (pure serialization). Jsonpaths
//! are built by hand as a `JsonPathParseResult` tree and flattened by the real
//! `jsonpath_in` parser entrypoint, driven through a test `parse` seam that
//! returns the hand-built tree (mirroring how the sibling `backend-utils-adt-
//! jsonpath` crate's own round-trip tests work). Numeric scalars use the REAL
//! on-disk `Numeric` encoding, so the executor's numeric crate reads them back
//! exactly.

use std::sync::Once;

use ::mcx::{Mcx, MemoryContext};

use ::adt_jsonb::jsonb_out;
use ::jsonb_util::JsonbValueToJsonb;
use ::adt_jsonpath::jsonpath_in;
use ::adt_jsonpath::JsonPathItemType;
use types_jsonb::jsonb_util::{JsonbPair, JsonbValue, JsonbValueData};
use ::types_jsonb::jsonb::jbvType;
use ::types_jsonpath::parse::{
    JsonPathParseItem, JsonPathParseResult, JsonPathParseValue, JsonPathSubscript,
};
use JsonPathItemType::*;

use crate::{
    jsonb_path_exists, jsonb_path_match, jsonb_path_query, PathExistsResult, PathMatchResult,
};

// --- seam providers --------------------------------------------------------

/// The test `parse` seam returns the parse tree stashed by [`flatten`]. The
/// round-trip tests never parse text; they build trees by hand.
fn test_parse(
    _str: &[u8],
    _escontext: Option<&mut types_error::SoftErrorContext>,
) -> types_error::PgResult<Option<JsonPathParseResult>> {
    NEXT_TREE.with(|slot| Ok(slot.borrow_mut().take()))
}

thread_local! {
    static NEXT_TREE: core::cell::RefCell<Option<JsonPathParseResult>> =
        const { core::cell::RefCell::new(None) };
}

static INIT: Once = Once::new();

/// Real numeric comparison provider for the jsonb-util `numeric_cmp` seam.
fn real_numeric_cmp(a: &[u8], b: &[u8]) -> i32 {
    use ::adt_numeric::ops_sql::numeric_cmp;
    use core::cmp::Ordering;
    match numeric_cmp(a, b) {
        Ordering::Less => -1,
        Ordering::Equal => 0,
        Ordering::Greater => 1,
    }
}

fn real_numeric_eq(a: &[u8], b: &[u8]) -> bool {
    real_numeric_cmp(a, b) == 0
}

fn install_seams() {
    INIT.call_once(|| {
        // The jsonpath text parser (grammar/scanner) is unported; the test
        // `parse` seam returns the hand-built tree.
        jsonpath_gram_seams::parse::set(test_parse);

        // Executor interrupt guard (no-op in unit context); the recursion guard
        // is installed below via its canonical owner-seams crate.
        postgres_seams::check_for_interrupts::set(|| Ok(()));

        // The jsonb / jsonb-util serialization path (JsonbValueToJsonb,
        // pushJsonbValue, the iterator, jsonb_out) routes its recursion guard
        // and scalar comparisons through sibling seams. Install the recursion
        // guard as a no-op and the numeric comparisons via the real numeric
        // crate (bit-exact). String collation reduces to a byte compare at the
        // database-default (C) collation in a unit context.
        stack_depth_seams::check_stack_depth::set(|| Ok(()));
        numeric_seams::numeric_eq::set(real_numeric_eq);
        numeric_seams::numeric_cmp::set(real_numeric_cmp);
        varlena_seams::varstr_cmp::set(|a, b, _coll| {
            Ok(match a.cmp(b) {
                core::cmp::Ordering::Less => -1,
                core::cmp::Ordering::Equal => 0,
                core::cmp::Ordering::Greater => 1,
            })
        });
    });
}

// --- jsonb document builders -----------------------------------------------

fn num_bytes(mcx: Mcx<'_>, n: i64) -> Vec<u8> {
    ::adt_numeric::convert::int64_to_numeric(mcx, n)
        .unwrap()
        .to_vec()
}

fn jnum(mcx: Mcx<'_>, n: i64) -> JsonbValue {
    JsonbValue {
        typ: jbvType::jbvNumeric,
        val: JsonbValueData::Numeric(num_bytes(mcx, n)),
    }
}

fn jstr(s: &str) -> JsonbValue {
    JsonbValue {
        typ: jbvType::jbvString,
        val: JsonbValueData::String(s.as_bytes().to_vec()),
    }
}

fn jarr(elems: Vec<JsonbValue>) -> JsonbValue {
    JsonbValue {
        typ: jbvType::jbvArray,
        val: JsonbValueData::Array {
            elems,
            raw_scalar: false,
        },
    }
}

fn jobj(pairs: Vec<(&str, JsonbValue)>) -> JsonbValue {
    let pairs = pairs
        .into_iter()
        .enumerate()
        .map(|(i, (k, v))| JsonbPair {
            key: jstr(k),
            value: v,
            order: i as u32,
        })
        .collect();
    JsonbValue {
        typ: jbvType::jbvObject,
        val: JsonbValueData::Object(pairs),
    }
}

fn to_jsonb(mcx: Mcx<'_>, v: &JsonbValue) -> Vec<u8> {
    install_seams();
    JsonbValueToJsonb(mcx, v).unwrap().to_vec()
}

// --- jsonpath parse-tree builders ------------------------------------------

fn leaf(typ: JsonPathItemType, value: JsonPathParseValue) -> Box<JsonPathParseItem> {
    Box::new(JsonPathParseItem {
        typ,
        next: None,
        value,
    })
}

fn root() -> Box<JsonPathParseItem> {
    leaf(jpiRoot, JsonPathParseValue::None)
}

fn key(name: &str) -> Box<JsonPathParseItem> {
    leaf(jpiKey, JsonPathParseValue::String(name.as_bytes().to_vec()))
}

fn current() -> Box<JsonPathParseItem> {
    leaf(jpiCurrent, JsonPathParseValue::None)
}

fn num_item(mcx: Mcx<'_>, n: i64) -> Box<JsonPathParseItem> {
    leaf(jpiNumeric, JsonPathParseValue::Numeric(num_bytes(mcx, n)))
}

// The boxes are load-bearing: each item is linked via `next:
// Option<Box<JsonPathParseItem>>`, so the elements must already be boxed.
#[allow(clippy::vec_box)]
fn chain(mut items: Vec<Box<JsonPathParseItem>>) -> Box<JsonPathParseItem> {
    let mut head = items.remove(0);
    let mut cur = &mut head;
    for it in items {
        cur.next = Some(it);
        cur = cur.next.as_mut().unwrap();
    }
    head
}

fn index_array(from: Box<JsonPathParseItem>) -> Box<JsonPathParseItem> {
    leaf(
        jpiIndexArray,
        JsonPathParseValue::Array(vec![JsonPathSubscript {
            from: Some(from),
            to: None,
        }]),
    )
}

fn binary(
    typ: JsonPathItemType,
    left: Box<JsonPathParseItem>,
    right: Box<JsonPathParseItem>,
) -> Box<JsonPathParseItem> {
    leaf(
        typ,
        JsonPathParseValue::Args {
            left: Some(left),
            right: Some(right),
        },
    )
}

/// Build the flattened on-disk jsonpath bytes for the given expression by
/// stashing the parse tree and driving the real `jsonpath_in` entrypoint
/// (which calls the test `parse` seam, then `flattenJsonPathParseItem`).
fn flatten(mcx: Mcx<'_>, lax: bool, expr: Box<JsonPathParseItem>) -> Vec<u8> {
    install_seams();
    NEXT_TREE.with(|slot| {
        *slot.borrow_mut() = Some(JsonPathParseResult {
            lax,
            expr: Some(expr),
        });
    });
    jsonpath_in(mcx, b"<test>", None).unwrap().unwrap().to_vec()
}

fn keyvalue() -> Box<JsonPathParseItem> {
    leaf(jpiKeyValue, JsonPathParseValue::None)
}

fn any_array() -> Box<JsonPathParseItem> {
    leaf(jpiAnyArray, JsonPathParseValue::None)
}

/// Render a jsonb-query result to its canonical text (C: `jsonb_out`).
fn render(mcx: Mcx<'_>, out: &[u8]) -> String {
    String::from_utf8(jsonb_out(mcx, out).unwrap().to_vec()).unwrap()
}

// --- tests -----------------------------------------------------------------

#[test]
fn root_returns_document() {
    let ctx = MemoryContext::new("jsonpath-exec-test");
    let mcx = ctx.mcx();
    // doc: {"a": 1}
    let doc = to_jsonb(mcx, &jobj(vec![("a", jnum(mcx, 1))]));
    let jp = flatten(mcx, true, root());
    let out = jsonb_path_query(mcx, &doc, &jp, None, false).unwrap();
    assert_eq!(out.len(), 1);
    // The single result is the whole document, round-tripping to the same bytes.
    assert_eq!(out[0], doc);
}

#[test]
fn key_accessor() {
    let ctx = MemoryContext::new("jsonpath-exec-test");
    let mcx = ctx.mcx();
    // doc: {"a": 42, "b": "x"}; path: $.a
    let doc = to_jsonb(mcx, &jobj(vec![("a", jnum(mcx, 42)), ("b", jstr("x"))]));
    let jp = flatten(mcx, true, chain(vec![root(), key("a")]));
    let out = jsonb_path_query(mcx, &doc, &jp, None, false).unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0], to_jsonb(mcx, &jnum(mcx, 42)));
}

#[test]
fn key_missing_lax_returns_empty() {
    let ctx = MemoryContext::new("jsonpath-exec-test");
    let mcx = ctx.mcx();
    // doc: {"a": 1}; path: $.zzz  (lax: structural error ignored -> empty)
    let doc = to_jsonb(mcx, &jobj(vec![("a", jnum(mcx, 1))]));
    let jp = flatten(mcx, true, chain(vec![root(), key("zzz")]));
    let out = jsonb_path_query(mcx, &doc, &jp, None, true).unwrap();
    assert!(out.is_empty());
}

#[test]
fn array_subscript() {
    let ctx = MemoryContext::new("jsonpath-exec-test");
    let mcx = ctx.mcx();
    // doc: [10, 20, 30]; path: $[1]
    let doc = to_jsonb(mcx, &jarr(vec![jnum(mcx, 10), jnum(mcx, 20), jnum(mcx, 30)]));
    let jp = flatten(mcx, true, chain(vec![root(), index_array(num_item(mcx, 1))]));
    let out = jsonb_path_query(mcx, &doc, &jp, None, false).unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0], to_jsonb(mcx, &jnum(mcx, 20)));
}

#[test]
fn arithmetic_add() {
    let ctx = MemoryContext::new("jsonpath-exec-test");
    let mcx = ctx.mcx();
    // doc: 5 (raw scalar); path: $ + 3  -> 8
    let doc = to_jsonb(mcx, &jnum(mcx, 5));
    let jp = flatten(mcx, true, binary(jpiAdd, root(), num_item(mcx, 3)));
    let out = jsonb_path_query(mcx, &doc, &jp, None, false).unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0], to_jsonb(mcx, &jnum(mcx, 8)));
}

#[test]
fn match_comparison_true() {
    let ctx = MemoryContext::new("jsonpath-exec-test");
    let mcx = ctx.mcx();
    // doc: {"a": 7}; predicate: $.a == 7  (via @@ / jsonb_path_match)
    let doc = to_jsonb(mcx, &jobj(vec![("a", jnum(mcx, 7))]));
    let pred = binary(jpiEqual, chain(vec![root(), key("a")]), num_item(mcx, 7));
    let jp = flatten(mcx, true, pred);
    assert_eq!(
        jsonb_path_match(mcx, &doc, &jp, None, false).unwrap(),
        PathMatchResult::True
    );
}

#[test]
fn match_comparison_false() {
    let ctx = MemoryContext::new("jsonpath-exec-test");
    let mcx = ctx.mcx();
    let doc = to_jsonb(mcx, &jobj(vec![("a", jnum(mcx, 7))]));
    let pred = binary(jpiEqual, chain(vec![root(), key("a")]), num_item(mcx, 8));
    let jp = flatten(mcx, true, pred);
    assert_eq!(
        jsonb_path_match(mcx, &doc, &jp, None, false).unwrap(),
        PathMatchResult::False
    );
}

#[test]
fn exists_true_and_false() {
    let ctx = MemoryContext::new("jsonpath-exec-test");
    let mcx = ctx.mcx();
    let doc = to_jsonb(mcx, &jobj(vec![("a", jnum(mcx, 1))]));

    // $.a exists
    let jp_a = flatten(mcx, true, chain(vec![root(), key("a")]));
    assert_eq!(
        jsonb_path_exists(mcx, &doc, &jp_a, None, false).unwrap(),
        PathExistsResult::True
    );

    // $.zzz does not exist (lax -> False, not error)
    let jp_z = flatten(mcx, true, chain(vec![root(), key("zzz")]));
    assert_eq!(
        jsonb_path_exists(mcx, &doc, &jp_z, None, false).unwrap(),
        PathExistsResult::False
    );
}

#[test]
fn size_method() {
    let ctx = MemoryContext::new("jsonpath-exec-test");
    let mcx = ctx.mcx();
    // doc: [1,2,3]; path: $.size()  -> 3
    let doc = to_jsonb(mcx, &jarr(vec![jnum(mcx, 1), jnum(mcx, 2), jnum(mcx, 3)]));
    let jp = flatten(
        mcx,
        true,
        chain(vec![root(), leaf(jpiSize, JsonPathParseValue::None)]),
    );
    let out = jsonb_path_query(mcx, &doc, &jp, None, false).unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0], to_jsonb(mcx, &jnum(mcx, 3)));
}

#[test]
fn type_method() {
    let ctx = MemoryContext::new("jsonpath-exec-test");
    let mcx = ctx.mcx();
    // doc: {"a": 1}; path: $.type()  -> "object"
    let doc = to_jsonb(mcx, &jobj(vec![("a", jnum(mcx, 1))]));
    let jp = flatten(
        mcx,
        true,
        chain(vec![root(), leaf(jpiType, JsonPathParseValue::None)]),
    );
    let out = jsonb_path_query(mcx, &doc, &jp, None, false).unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0], to_jsonb(mcx, &jstr("object")));
}

/// Golden test for `.keyvalue()` ids — the byte offset of an object's container
/// within its base document. Mirrors `jsonb_jsonpath.out`:
///
/// ```text
/// select jsonb_path_query('{"a": 1, "b": [1, 2], "c": {"a": "bbb"}}', '$.keyvalue()');
///  {"id": 0, "key": "a", "value": 1}
///  {"id": 0, "key": "b", "value": [1, 2]}
///  {"id": 0, "key": "c", "value": {"a": "bbb"}}
/// ```
#[test]
fn keyvalue_id_root_object_is_zero() {
    let ctx = MemoryContext::new("jsonpath-exec-test");
    let mcx = ctx.mcx();
    // doc: {"a": 1, "b": [1, 2], "c": {"a": "bbb"}}
    let doc = to_jsonb(
        mcx,
        &jobj(vec![
            ("a", jnum(mcx, 1)),
            ("b", jarr(vec![jnum(mcx, 1), jnum(mcx, 2)])),
            ("c", jobj(vec![("a", jstr("bbb"))])),
        ]),
    );
    // path: $.keyvalue()
    let jp = flatten(mcx, true, chain(vec![root(), keyvalue()]));
    let out = jsonb_path_query(mcx, &doc, &jp, None, false).unwrap();
    assert_eq!(out.len(), 3);
    assert_eq!(render(mcx, &out[0]), r#"{"id": 0, "key": "a", "value": 1}"#);
    assert_eq!(
        render(mcx, &out[1]),
        r#"{"id": 0, "key": "b", "value": [1, 2]}"#
    );
    assert_eq!(
        render(mcx, &out[2]),
        r#"{"id": 0, "key": "c", "value": {"a": "bbb"}}"#
    );
}

/// Golden test for `$[*].keyvalue()` ids. Mirrors `jsonb_jsonpath.out`:
///
/// ```text
/// select jsonb_path_query('[{"a": 1, "b": [1, 2]}, {"c": {"a": "bbb"}}]', '$[*].keyvalue()');
///  {"id": 12, "key": "a", "value": 1}
///  {"id": 12, "key": "b", "value": [1, 2]}
///  {"id": 72, "key": "c", "value": {"a": "bbb"}}
/// ```
#[test]
fn keyvalue_id_array_elements_are_document_offsets() {
    let ctx = MemoryContext::new("jsonpath-exec-test");
    let mcx = ctx.mcx();
    // doc: [{"a": 1, "b": [1, 2]}, {"c": {"a": "bbb"}}]
    let doc = to_jsonb(
        mcx,
        &jarr(vec![
            jobj(vec![
                ("a", jnum(mcx, 1)),
                ("b", jarr(vec![jnum(mcx, 1), jnum(mcx, 2)])),
            ]),
            jobj(vec![("c", jobj(vec![("a", jstr("bbb"))]))]),
        ]),
    );
    // path: $[*].keyvalue()
    let jp = flatten(mcx, true, chain(vec![root(), any_array(), keyvalue()]));
    let out = jsonb_path_query(mcx, &doc, &jp, None, false).unwrap();
    assert_eq!(out.len(), 3);
    assert_eq!(render(mcx, &out[0]), r#"{"id": 12, "key": "a", "value": 1}"#);
    assert_eq!(
        render(mcx, &out[1]),
        r#"{"id": 12, "key": "b", "value": [1, 2]}"#
    );
    assert_eq!(
        render(mcx, &out[2]),
        r#"{"id": 72, "key": "c", "value": {"a": "bbb"}}"#
    );
}

/// Non-object `vars` must error eagerly with SQLSTATE 22023 (matching C's
/// `countVariablesFromJsonb`).
#[test]
fn vars_not_an_object_errors_22023() {
    let ctx = MemoryContext::new("jsonpath-exec-test");
    let mcx = ctx.mcx();
    // doc: 1 ; path: $ ; vars: [1, 2]  (an array, not an object)
    let doc = to_jsonb(mcx, &jnum(mcx, 1));
    let jp = flatten(mcx, true, root());
    let vars = to_jsonb(mcx, &jarr(vec![jnum(mcx, 1), jnum(mcx, 2)]));

    let err = jsonb_path_query(mcx, &doc, &jp, Some(&vars), false).unwrap_err();
    assert_eq!(err.message(), "\"vars\" argument is not an object");
    // ERRCODE_INVALID_PARAMETER_VALUE == 22023.
    assert_eq!(err.sqlstate(), types_error::ERRCODE_INVALID_PARAMETER_VALUE);
}

/// An *object* `vars` is accepted (the validation only rejects non-objects).
#[test]
fn vars_object_is_accepted() {
    let ctx = MemoryContext::new("jsonpath-exec-test");
    let mcx = ctx.mcx();
    let doc = to_jsonb(mcx, &jnum(mcx, 1));
    let jp = flatten(mcx, true, root());
    let vars = to_jsonb(mcx, &jobj(vec![("x", jnum(mcx, 1))]));
    let out = jsonb_path_query(mcx, &doc, &jp, Some(&vars), false).unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0], doc);
}

#[test]
fn filter_with_current() {
    let ctx = MemoryContext::new("jsonpath-exec-test");
    let mcx = ctx.mcx();
    // doc: [1, 5, 9]; path: $[*] ? (@ > 4)  -> [5, 9]
    let doc = to_jsonb(mcx, &jarr(vec![jnum(mcx, 1), jnum(mcx, 5), jnum(mcx, 9)]));
    let any = leaf(jpiAnyArray, JsonPathParseValue::None);
    let filter = leaf(
        jpiFilter,
        JsonPathParseValue::Arg(Some(binary(jpiGreater, current(), num_item(mcx, 4)))),
    );
    let jp = flatten(mcx, true, chain(vec![root(), any, filter]));
    let out = jsonb_path_query(mcx, &doc, &jp, None, false).unwrap();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0], to_jsonb(mcx, &jnum(mcx, 5)));
    assert_eq!(out[1], to_jsonb(mcx, &jnum(mcx, 9)));
}
