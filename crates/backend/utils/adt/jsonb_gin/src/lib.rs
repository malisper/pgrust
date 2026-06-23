//! `jsonb_gin.c` — GIN index support functions for the `jsonb_ops` and
//! `jsonb_path_ops` operator classes.
//!
//! The on-disk `jsonb` format and the container iterate/hash engine come from
//! [`jsonb_util`]; the `jsonpath` node-tree traversal from
//! [`adt_jsonpath`]; numeric rendering (`numeric_normalize`) is
//! ported in-crate on top of [`adt_numeric`]; the C-collation text
//! comparison (`gin_compare_jsonb`) reduces to a plain byte compare; overlength
//! hashing uses [`hashfn`].
//!
//! ## Calling convention / wiring
//!
//! The C entry points take `PG_FUNCTION_ARGS`; the fmgr GIN opclass-proc
//! dispatch is the cross-crate boundary. The nine support procs are exposed as
//! ordinary Rust functions (jsonb arguments are the on-disk root
//! [`JsonbContainer`] bytes, jsonpath arguments are the on-disk jsonpath
//! varlena bytes; the `check` / `extra_data` / `searchMode` out-parameters
//! become return values) and installed into
//! [`jsonb_gin_seams`] from [`init_seams`]. A future GIN
//! by-OID dispatcher (the analog of `backend-access-gist-proc`, not yet built)
//! routes the jsonb GIN support-proc OIDs to these seams with the `Datum`
//! marshaling. The cross-boundary vocabulary ([`GinEntries`],
//! [`JsonPathGinNode`], [`GinQueryExtraction`], [`GinJsonbQuery`]) lives in
//! [`::types_jsonb::jsonb_gin`].

#![no_std]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

extern crate alloc;

use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec;
use alloc::vec::Vec;

use jsonb_util::{
    jbvType, json_container_size, JsonbHashScalarValue, JsonbIteratorInit, JsonbIteratorNext,
    JsonbIteratorToken, JsonbValue, JsonbValueData, VARHDRSZ,
};
use adt_jsonpath::{
    jsonpath_is_lax, jspGetArg, jspGetBool, jspGetLeftArg, jspGetNext, jspGetNumeric,
    jspGetRightArg, jspGetString, jspInit, JsonPathItem,
};
use ::mcx::Mcx;
use ::types_error::error::{ERRCODE_INTERNAL_ERROR, ERRCODE_OUT_OF_MEMORY};
use types_error::{PgError, PgResult};
use ::types_jsonb::jsonb::{
    GinTernaryValue, JsonbContainsStrategyNumber, JsonbExistsAllStrategyNumber,
    JsonbExistsAnyStrategyNumber, JsonbExistsStrategyNumber, JsonbJsonpathExistsStrategyNumber,
    JsonbJsonpathPredicateStrategyNumber, GIN_FALSE, GIN_MAYBE, GIN_TRUE, JGINFLAG_BOOL,
    JGINFLAG_HASHED, JGINFLAG_KEY, JGINFLAG_NULL, JGINFLAG_NUM, JGINFLAG_STR, JGIN_MAXLENGTH,
};
pub use ::types_jsonb::jsonb_gin::{
    GinEntries, GinJsonbQuery, GinQueryExtraction, JsonPathGinNode, JsonPathGinNodeType, JspQuery,
};
use ::types_jsonpath::jsonpath::{jsp_is_scalar, JsonPathItemType};

mod fmgr_builtins;

#[cfg(test)]
mod tests;

// ---------------------------------------------------------------------------
// pg_proc.dat support-proc OIDs for the jsonb_ops / jsonb_path_ops opclasses.
// ---------------------------------------------------------------------------

/// `gin_compare_jsonb` (pg_proc.dat oid 3480).
pub const F_GIN_COMPARE_JSONB: types_core::Oid = 3480;
/// `gin_extract_jsonb` (pg_proc.dat oid 3482).
pub const F_GIN_EXTRACT_JSONB: types_core::Oid = 3482;
/// `gin_extract_jsonb_query` (pg_proc.dat oid 3483).
pub const F_GIN_EXTRACT_JSONB_QUERY: types_core::Oid = 3483;
/// `gin_consistent_jsonb` (pg_proc.dat oid 3484).
pub const F_GIN_CONSISTENT_JSONB: types_core::Oid = 3484;
/// `gin_triconsistent_jsonb` (pg_proc.dat oid 3488).
pub const F_GIN_TRICONSISTENT_JSONB: types_core::Oid = 3488;
/// `gin_extract_jsonb_path` (pg_proc.dat oid 3485).
pub const F_GIN_EXTRACT_JSONB_PATH: types_core::Oid = 3485;
/// `gin_extract_jsonb_query_path` (pg_proc.dat oid 3486).
pub const F_GIN_EXTRACT_JSONB_QUERY_PATH: types_core::Oid = 3486;
/// `gin_consistent_jsonb_path` (pg_proc.dat oid 3487).
pub const F_GIN_CONSISTENT_JSONB_PATH: types_core::Oid = 3487;
/// `gin_triconsistent_jsonb_path` (pg_proc.dat oid 3489).
pub const F_GIN_TRICONSISTENT_JSONB_PATH: types_core::Oid = 3489;

/// Install every jsonb GIN opclass support-proc body into
/// [`jsonb_gin_seams`]. The future GIN by-OID dispatcher
/// routes the support-proc OIDs to these.
pub fn init_seams() {
    use jsonb_gin_seams as seams;
    seams::gin_compare_jsonb::set(gin_compare_jsonb);
    seams::gin_extract_jsonb::set(gin_extract_jsonb);
    seams::gin_extract_jsonb_query::set(gin_extract_jsonb_query);
    seams::gin_consistent_jsonb::set(gin_consistent_jsonb);
    seams::gin_triconsistent_jsonb::set(gin_triconsistent_jsonb);
    seams::gin_extract_jsonb_path::set(gin_extract_jsonb_path);
    seams::gin_extract_jsonb_query_path::set(gin_extract_jsonb_query_path);
    seams::gin_consistent_jsonb_path::set(gin_consistent_jsonb_path);
    seams::gin_triconsistent_jsonb_path::set(gin_triconsistent_jsonb_path);

    // Register the scalar fmgr builtins (C: fmgr_builtins[] rows) into the
    // fmgr-core builtin table for by-OID fast-path dispatch.
    fmgr_builtins::register_jsonb_gin_builtins();
}

// ===========================================================================
// Root-container helpers (C: JB_ROOT_COUNT).
// ===========================================================================

/// Read the leading `JsonbContainer.header` word from the root bytes.
#[inline]
fn container_header(root: &[u8]) -> u32 {
    u32::from_ne_bytes([root[0], root[1], root[2], root[3]])
}

/// C: `JB_ROOT_COUNT(jbp)` == `JsonContainerSize(&(jbp)->root)`.
#[inline]
fn jb_root_count(root: &[u8]) -> u32 {
    json_container_size(container_header(root))
}

// ===========================================================================
// GinEntries: buffer for GIN entries (C: struct GinEntries / init_gin_entries /
// add_gin_entry). The C `allocated`/doubling bookkeeping is `Vec` growth here.
// ===========================================================================

/// C: `init_gin_entries(entries, preallocated)`. `preallocated` is `2 * root
/// count`, bounded by the jsonb engine; reserved fallibly with OOM surfaced.
fn gin_entries_init(preallocated: usize) -> PgResult<GinEntries> {
    let mut buf: Vec<Vec<u8>> = Vec::new();
    buf.try_reserve(preallocated).map_err(|_| out_of_memory())?;
    Ok(GinEntries { buf })
}

/// C: `add_gin_entry(entries, entry)` — append and return the new entry's index.
fn gin_entries_add(entries: &mut GinEntries, entry: Vec<u8>) -> usize {
    let id = entries.buf.len();
    entries.buf.push(entry);
    id
}

/// C: `entries->count`.
#[inline]
fn gin_entries_count(entries: &GinEntries) -> usize {
    entries.buf.len()
}

// ===========================================================================
// Node constructors (C: make_jsp_entry_node / make_jsp_entry_node_scalar /
// make_jsp_expr_node{,_args,_binary}).
// ===========================================================================

/// C: `make_jsp_entry_node(Datum entry)`.
fn make_jsp_entry_node(entry: Vec<u8>) -> JsonPathGinNode {
    JsonPathGinNode::EntryDatum(entry)
}

/// C: `make_jsp_entry_node_scalar(JsonbValue *scalar, bool iskey)`.
fn make_jsp_entry_node_scalar<'mcx>(
    mcx: Mcx<'mcx>,
    scalar: &JsonbValue,
    iskey: bool,
) -> PgResult<JsonPathGinNode> {
    Ok(make_jsp_entry_node(make_scalar_key(mcx, scalar, iskey)?))
}

/// C: `make_jsp_expr_node_args(JsonPathGinNodeType type, List *args)`.
fn make_jsp_expr_node_args(and: bool, args: Vec<JsonPathGinNode>) -> JsonPathGinNode {
    JsonPathGinNode::Logic { and, args }
}

/// C: `make_jsp_expr_node_binary(JsonPathGinNodeType type, arg1, arg2)`.
fn make_jsp_expr_node_binary(
    and: bool,
    arg1: JsonPathGinNode,
    arg2: JsonPathGinNode,
) -> JsonPathGinNode {
    JsonPathGinNode::Logic {
        and,
        args: vec![arg1, arg2],
    }
}

// ===========================================================================
// JsonPathGinPath / JsonPathGinPathItem / context (opclass-internal).
// ===========================================================================

/// C: `struct JsonPathGinPathItem`. The C `parent` linked list is modeled by
/// position within the [`JsonPathGinPath::Items`] `Vec`: items are appended
/// innermost-last, so the "current" item is the last element and the parent
/// chain is the reversed iteration order.
#[derive(Clone, Debug)]
struct JsonPathGinPathItem {
    /// C: `Datum keyName` — the GIN key bytes for a `.key` item, else `None`.
    key_name: Option<Vec<u8>>,
    /// C: `JsonPathItemType type`.
    typ: JsonPathItemType,
}

/// C: `union JsonPathGinPath` — a list of path items (`jsonb_ops`) or a running
/// hash (`jsonb_path_ops`).
#[derive(Clone, Debug)]
enum JsonPathGinPath {
    /// C: `JsonPathGinPathItem *items` (jsonb_ops).
    Items(Vec<JsonPathGinPathItem>),
    /// C: `uint32 hash` (jsonb_path_ops).
    Hash(u32),
}

/// C: `struct JsonPathGinContext`. The two C callback families
/// (`add_path_item` / `extract_nodes`) are selected by `path_ops`.
struct JsonPathGinContext {
    /// false == `jsonb_ops`, true == `jsonb_path_ops`.
    path_ops: bool,
    /// C: `bool lax`.
    lax: bool,
}

// ===========================================================================
// make_text_key / make_scalar_key / numeric_normalize (jsonb_gin.c).
// ===========================================================================

/// C: `make_text_key(char flag, const char *str, int len)`. Construct a
/// `jsonb_ops` GIN key from a flag byte and a textual representation; overlength
/// text is hashed (`hash_any`) and tagged `JGINFLAG_HASHED`. Returns the
/// assembled 4-byte-header varlena `text` bytes (header + flag + payload).
pub fn make_text_key(mut flag: u8, str_in: &[u8]) -> Vec<u8> {
    let mut str_bytes: &[u8] = str_in;
    let hashbuf: Vec<u8>;

    if str_bytes.len() as i32 > JGIN_MAXLENGTH {
        // hashval = DatumGetUInt32(hash_any(str, len));
        let hashval = hashfn::hash_bytes(str_bytes);
        // snprintf(hashbuf, sizeof(hashbuf), "%08x", hashval); len = 8;
        hashbuf = format!("{hashval:08x}").into_bytes();
        str_bytes = &hashbuf;
        flag |= JGINFLAG_HASHED;
    }

    // Build a 4-byte-header varlena text Datum: VARHDRSZ + len + 1 bytes; the
    // first payload byte is the flag, the rest is the text.
    let len = str_bytes.len();
    let mut item = vec![0u8; VARHDRSZ + len + 1];
    set_varsize(&mut item, (VARHDRSZ + len + 1) as u32);
    item[VARHDRSZ] = flag;
    item[VARHDRSZ + 1..VARHDRSZ + 1 + len].copy_from_slice(str_bytes);
    item
}

/// C: `make_scalar_key(const JsonbValue *scalarVal, bool is_key)`. Create a
/// textual GIN key for a [`JsonbValue`] scalar.
pub fn make_scalar_key<'mcx>(
    mcx: Mcx<'mcx>,
    scalar_val: &JsonbValue,
    is_key: bool,
) -> PgResult<Vec<u8>> {
    let item = match &scalar_val.val {
        JsonbValueData::Null => {
            debug_assert!(!is_key);
            make_text_key(JGINFLAG_NULL, b"")
        }
        JsonbValueData::Bool(b) => {
            debug_assert!(!is_key);
            make_text_key(JGINFLAG_BOOL, if *b { b"t" } else { b"f" })
        }
        JsonbValueData::Numeric(numeric) => {
            debug_assert!(!is_key);
            // A normalized textual representation, free of trailing zeroes, is
            // required so numerically-equal values produce equal strings.
            let cstr = numeric_normalize(mcx, numeric)?;
            make_text_key(JGINFLAG_NUM, cstr.as_bytes())
        }
        JsonbValueData::String(s) => {
            make_text_key(if is_key { JGINFLAG_KEY } else { JGINFLAG_STR }, s)
        }
        _ => {
            return Err(unrecognized_scalar_type(scalar_val));
        }
    };

    Ok(item)
}

/// C: `numeric_normalize(Numeric num)` (numeric.c:1026). Render `num` (on-disk
/// varlena bytes) to its canonical text with insignificant trailing fractional
/// zeroes — and then any trailing decimal point — suppressed. Ported on top of
/// [`adt_numeric`] (`set_var_from_num` + `get_str_from_var`); the
/// special values short-circuit exactly as C's `NUMERIC_IS_*` branches.
///
/// The C `numeric_normalize` allocates a working `NumericVar` and a result
/// `char *` from the calling memory context; the repo's
/// `set_var_from_num`/`get_str_from_var` allocate against the caller-supplied
/// `Mcx` (threaded down from the GIN extract entry point — there is no ambient
/// context). The scratch `NumericVar` lives in `mcx`; the rendered `String` is
/// owned.
fn numeric_normalize<'mcx>(mcx: Mcx<'mcx>, num: &[u8]) -> PgResult<String> {
    use ::adt_numeric::convert::set_var_from_num;
    use ::adt_numeric::io::get_str_from_var;
    use ::adt_numeric::on_disk::var::NumericSign;

    let x = set_var_from_num(mcx, num)?;

    // Handle NaN and infinities.
    if x.sign.is_special() {
        return Ok(match x.sign {
            NumericSign::PInf => "Infinity".to_string(),
            NumericSign::NInf => "-Infinity".to_string(),
            _ => "NaN".to_string(),
        });
    }

    let mut str = get_str_from_var(&x).into_bytes();

    // If there's no decimal point, there's nothing to remove.
    if str.iter().any(|&c| c == b'.') {
        // Back up over trailing fractional zeroes. Since there is a decimal
        // point, this loop terminates safely.
        let mut last = str.len() - 1;
        while str[last] == b'0' {
            last -= 1;
        }

        // We want to get rid of the decimal point too, if it's now last.
        if str[last] == b'.' {
            last -= 1;
        }

        // Delete whatever we backed up over.
        str.truncate(last + 1);
    }

    // get_str_from_var always returns valid ASCII decimal text.
    String::from_utf8(str)
        .map_err(|_| PgError::error("numeric text is not ASCII").with_sqlstate(ERRCODE_INTERNAL_ERROR))
}

// ===========================================================================
// jsonb_ops opclass support functions
// ===========================================================================

/// C: `gin_compare_jsonb(PG_FUNCTION_ARGS)`. Compare two GIN keys as
/// `bttextcmp` does but under the C collation, which reduces to a plain unsigned
/// byte compare (memcmp + length tiebreak == `Ord` over `&[u8]`). `a`/`b` are the
/// `VARDATA_ANY` payloads (the fmgr boundary `arg_text` strips the header
/// header-form-agnostically); comparing the header-stripped payloads is what
/// makes a short-packed stored key compare equal to its 4-byte query twin.
pub fn gin_compare_jsonb(a: &[u8], b: &[u8]) -> i32 {
    use core::cmp::Ordering;
    match a.cmp(b) {
        Ordering::Less => -1,
        Ordering::Equal => 0,
        Ordering::Greater => 1,
    }
}

/// C: `gin_extract_jsonb(PG_FUNCTION_ARGS)`. Extract the GIN keys of a jsonb
/// value (`jsonb_ops`). An empty root returns an empty vector (C: `*nentries =
/// 0; return NULL`).
pub fn gin_extract_jsonb<'mcx>(mcx: Mcx<'mcx>, jb_root: &[u8]) -> PgResult<Vec<Vec<u8>>> {
    let total = jb_root_count(jb_root) as i32;

    // If the root level is empty, we certainly have no keys.
    if total == 0 {
        return Ok(Vec::new());
    }

    // Otherwise, use 2 * root count as initial estimate of result size.
    let mut entries = gin_entries_init(2 * total as usize)?;

    let mut it = JsonbIteratorInit(jb_root);
    let mut v = JsonbValue::null();

    loop {
        let r = JsonbIteratorNext(&mut it, &mut v, false)?;
        if r == JsonbIteratorToken::WJB_DONE {
            break;
        }
        match r {
            JsonbIteratorToken::WJB_KEY => {
                let key = make_scalar_key(mcx, &v, true)?;
                gin_entries_add(&mut entries, key);
            }
            JsonbIteratorToken::WJB_ELEM => {
                // Pretend string array elements are keys, see jsonb.h.
                let is_key = v.typ == jbvType::jbvString;
                let key = make_scalar_key(mcx, &v, is_key)?;
                gin_entries_add(&mut entries, key);
            }
            JsonbIteratorToken::WJB_VALUE => {
                let key = make_scalar_key(mcx, &v, false)?;
                gin_entries_add(&mut entries, key);
            }
            _ => {
                // we can ignore structural items
            }
        }
    }

    Ok(entries.buf)
}

/// C: `jsonb_ops__add_path_item(JsonPathGinPath *path, JsonPathItem *jsp)`.
/// Append a [`JsonPathGinPathItem`] to the path; returns false for an
/// unsupported path item.
fn jsonb_ops__add_path_item(path: &mut JsonPathGinPath, jsp: &JsonPathItem<'_>) -> bool {
    let items = match path {
        JsonPathGinPath::Items(items) => items,
        JsonPathGinPath::Hash(_) => unreachable!("jsonb_ops path is Items"),
    };

    let key_name = match jsp.typ {
        JsonPathItemType::jpiRoot => {
            items.clear(); // reset path
            return true;
        }

        JsonPathItemType::jpiKey => {
            let key = jspGetString(jsp);
            Some(make_text_key(JGINFLAG_KEY, key))
        }

        JsonPathItemType::jpiAny
        | JsonPathItemType::jpiAnyKey
        | JsonPathItemType::jpiAnyArray
        | JsonPathItemType::jpiIndexArray => None, // PointerGetDatum(NULL)

        _ => {
            // other path items like item methods are not supported
            return false;
        }
    };

    items.push(JsonPathGinPathItem {
        key_name,
        typ: jsp.typ,
    });

    true
}

/// C: `jsonb_path_ops__add_path_item(JsonPathGinPath *path, JsonPathItem *jsp)`.
/// Combine the existing path hash with the next key hash (jsonb_path_ops).
fn jsonb_path_ops__add_path_item(
    path: &mut JsonPathGinPath,
    jsp: &JsonPathItem<'_>,
) -> PgResult<bool> {
    let hash = match path {
        JsonPathGinPath::Hash(hash) => hash,
        JsonPathGinPath::Items(_) => unreachable!("jsonb_path_ops path is Hash"),
    };

    match jsp.typ {
        JsonPathItemType::jpiRoot => {
            *hash = 0; // reset path hash
            Ok(true)
        }

        JsonPathItemType::jpiKey => {
            // C: jbv.type = jbvString; jbv.val.string.val = jspGetString(...);
            let jbv = JsonbValue {
                typ: jbvType::jbvString,
                val: JsonbValueData::String(jspGetString(jsp).to_vec()),
            };
            JsonbHashScalarValue(&jbv, hash)?;
            Ok(true)
        }

        JsonPathItemType::jpiIndexArray | JsonPathItemType::jpiAnyArray => Ok(true), // unchanged

        _ => {
            // other items (wildcard paths, item methods) are not supported
            Ok(false)
        }
    }
}

/// Dispatch `cxt->add_path_item(path, jsp)` to the opclass-specific callback.
fn add_path_item(
    cxt: &JsonPathGinContext,
    path: &mut JsonPathGinPath,
    jsp: &JsonPathItem<'_>,
) -> PgResult<bool> {
    if cxt.path_ops {
        jsonb_path_ops__add_path_item(path, jsp)
    } else {
        Ok(jsonb_ops__add_path_item(path, jsp))
    }
}

/// C: `jsonb_ops__extract_nodes(cxt, path, scalar, nodes)`.
fn jsonb_ops__extract_nodes<'mcx>(
    mcx: Mcx<'mcx>,
    cxt: &JsonPathGinContext,
    path: &JsonPathGinPath,
    scalar: Option<&JsonbValue>,
    nodes: &mut Vec<JsonPathGinNode>,
) -> PgResult<()> {
    let items = match path {
        JsonPathGinPath::Items(items) => items,
        JsonPathGinPath::Hash(_) => unreachable!("jsonb_ops path is Items"),
    };

    if let Some(scalar) = scalar {
        // Append path entry nodes only if scalar is provided. C iterates the
        // parent chain innermost-first; our Vec is innermost-last, so reverse.
        for pentry in items.iter().rev() {
            if pentry.typ == JsonPathItemType::jpiKey {
                // only keys are indexed
                let key = pentry.key_name.clone().ok_or_else(|| {
                    PgError::error("jsonb_ops__extract_nodes: jpiKey path item carries no keyName")
                        .with_sqlstate(ERRCODE_INTERNAL_ERROR)
                })?;
                nodes.push(make_jsp_entry_node(key));
            }
        }

        let node;

        // Append scalar node for equality queries.
        if scalar.typ == jbvType::jbvString {
            // C: `JsonPathGinPathItem *last = path.items;` — the innermost item.
            let last = items.last();
            let key_entry: GinTernaryValue;

            // Assuming jsonb_ops interprets string array elements as keys, we
            // may extract key or non-key entry or both (the latter => OR-node):
            // possible in lax mode (arrays auto-unwrapped) or strict-mode jpiAny.
            if cxt.lax {
                key_entry = GIN_MAYBE;
            } else if last.is_none() {
                // root ($)
                key_entry = GIN_FALSE;
            } else {
                let last = last.ok_or_else(|| {
                    PgError::error("jsonb_ops__extract_nodes: last path item is NULL")
                        .with_sqlstate(ERRCODE_INTERNAL_ERROR)
                })?;
                if last.typ == JsonPathItemType::jpiAnyArray
                    || last.typ == JsonPathItemType::jpiIndexArray
                {
                    key_entry = GIN_TRUE;
                } else if last.typ == JsonPathItemType::jpiAny {
                    key_entry = GIN_MAYBE;
                } else {
                    key_entry = GIN_FALSE;
                }
            }

            if key_entry == GIN_MAYBE {
                let n1 = make_jsp_entry_node_scalar(mcx, scalar, true)?;
                let n2 = make_jsp_entry_node_scalar(mcx, scalar, false)?;

                node = make_jsp_expr_node_binary(false, n1, n2); // JSP_GIN_OR
            } else {
                node = make_jsp_entry_node_scalar(mcx, scalar, key_entry == GIN_TRUE)?;
            }
        } else {
            node = make_jsp_entry_node_scalar(mcx, scalar, false)?;
        }

        nodes.push(node);
    }

    Ok(())
}

/// C: `jsonb_path_ops__extract_nodes(cxt, path, scalar, nodes)`.
fn jsonb_path_ops__extract_nodes(
    path: &JsonPathGinPath,
    scalar: Option<&JsonbValue>,
    nodes: &mut Vec<JsonPathGinNode>,
) -> PgResult<()> {
    let path_hash = match path {
        JsonPathGinPath::Hash(hash) => *hash,
        JsonPathGinPath::Items(_) => unreachable!("jsonb_path_ops path is Hash"),
    };

    if let Some(scalar) = scalar {
        // append path hash node for equality queries
        let mut hash = path_hash;

        JsonbHashScalarValue(scalar, &mut hash)?;

        nodes.push(make_jsp_entry_node(uint32_get_datum(hash)));
    }
    // else: jsonb_path_ops doesn't support EXISTS queries => nothing to append
    Ok(())
}

/// Dispatch `cxt->extract_nodes(cxt, path, scalar, nodes)`.
fn extract_nodes<'mcx>(
    mcx: Mcx<'mcx>,
    cxt: &JsonPathGinContext,
    path: &JsonPathGinPath,
    scalar: Option<&JsonbValue>,
    nodes: &mut Vec<JsonPathGinNode>,
) -> PgResult<()> {
    if cxt.path_ops {
        jsonb_path_ops__extract_nodes(path, scalar, nodes)
    } else {
        jsonb_ops__extract_nodes(mcx, cxt, path, scalar, nodes)
    }
}

/// C: `extract_jsp_path_expr_nodes(cxt, path, jsp, scalar)`. `path` is taken by
/// value (the C union is passed by value).
fn extract_jsp_path_expr_nodes<'mcx>(
    mcx: Mcx<'mcx>,
    cxt: &JsonPathGinContext,
    mut path: JsonPathGinPath,
    jsp: &JsonPathItem<'_>,
    scalar: Option<&JsonbValue>,
) -> PgResult<Vec<JsonPathGinNode>> {
    let mut nodes: Vec<JsonPathGinNode> = Vec::new();
    let mut cur: JsonPathItem<'_> = jsp.clone();

    loop {
        match cur.typ {
            JsonPathItemType::jpiCurrent => {}

            JsonPathItemType::jpiFilter => {
                let arg = jspGetArg(&cur);

                // C passes the current `path` by value (a copy); the loop's own
                // `path` continues to be extended afterwards.
                let filter = extract_jsp_bool_expr(mcx, cxt, path.clone(), &arg, false)?;

                if let Some(filter) = filter {
                    nodes.push(filter);
                }
            }

            _ => {
                if !add_path_item(cxt, &mut path, &cur)? {
                    // Path is not supported by the index opclass, return only
                    // the extracted filter nodes.
                    return Ok(nodes);
                }
            }
        }

        match jspGetNext(&cur) {
            Some(next) => cur = next,
            None => break,
        }
    }

    // Append nodes from the path expression itself to the extracted filter list.
    extract_nodes(mcx, cxt, &path, scalar, &mut nodes)?;
    Ok(nodes)
}

/// C: `extract_jsp_path_expr(cxt, path, jsp, scalar)`. Extract a node for
/// `EXISTS(jsp)` (scalar == None) or `jsp == scalar` (scalar == Some).
fn extract_jsp_path_expr<'mcx>(
    mcx: Mcx<'mcx>,
    cxt: &JsonPathGinContext,
    path: JsonPathGinPath,
    jsp: &JsonPathItem<'_>,
    scalar: Option<&JsonbValue>,
) -> PgResult<Option<JsonPathGinNode>> {
    // extract a list of nodes to be AND-ed
    let mut nodes = extract_jsp_path_expr_nodes(mcx, cxt, path, jsp, scalar)?;

    if nodes.is_empty() {
        // no nodes were extracted => full scan is needed for this path
        return Ok(None);
    }

    if nodes.len() == 1 {
        let node = nodes
            .pop()
            .ok_or_else(|| PgError::error("extract_jsp_path_expr: nodes is empty"))?;
        return Ok(Some(node)); // avoid extra AND-node
    }

    // construct AND-node for path with filters
    Ok(Some(make_jsp_expr_node_args(true, nodes)))
}

/// C: `extract_jsp_bool_expr(cxt, path, jsp, not)`. `path` is passed by value
/// (each recursion works on an independent copy).
fn extract_jsp_bool_expr<'mcx>(
    mcx: Mcx<'mcx>,
    cxt: &JsonPathGinContext,
    path: JsonPathGinPath,
    jsp: &JsonPathItem<'_>,
    not: bool,
) -> PgResult<Option<JsonPathGinNode>> {
    stack_depth_seams::check_stack_depth::call()?;

    match jsp.typ {
        // expr && expr / expr || expr
        JsonPathItemType::jpiAnd | JsonPathItemType::jpiOr => {
            let arg = jspGetLeftArg(jsp);
            let larg = extract_jsp_bool_expr(mcx, cxt, path.clone(), &arg, not)?;

            let arg = jspGetRightArg(jsp);
            let rarg = extract_jsp_bool_expr(mcx, cxt, path, &arg, not)?;

            if larg.is_none() || rarg.is_none() {
                if jsp.typ == JsonPathItemType::jpiOr {
                    return Ok(None);
                }
                return Ok(larg.or(rarg));
            }

            // type = not ^ (jsp->type == jpiAnd) ? JSP_GIN_AND : JSP_GIN_OR;
            let and = not ^ (jsp.typ == JsonPathItemType::jpiAnd);

            Ok(Some(make_jsp_expr_node_binary(
                and,
                larg.ok_or_else(|| PgError::error("extract_jsp_bool_expr: larg is NULL"))?,
                rarg.ok_or_else(|| PgError::error("extract_jsp_bool_expr: rarg is NULL"))?,
            )))
        }

        // !expr
        JsonPathItemType::jpiNot => {
            let arg = jspGetArg(jsp);
            // extract child expression inverting 'not' flag
            extract_jsp_bool_expr(mcx, cxt, path, &arg, !not)
        }

        // EXISTS(path)
        JsonPathItemType::jpiExists => {
            if not {
                return Ok(None); // NOT EXISTS is not supported
            }

            let arg = jspGetArg(jsp);

            extract_jsp_path_expr(mcx, cxt, path, &arg, None)
        }

        JsonPathItemType::jpiNotEqual => {
            // 'not' == true is not supported here ('!(path != scalar)' is not
            // equivalent to 'path == scalar' in general, and 'EMPTY(path)'
            // queries are not supported by either jsonb opclass).
            Ok(None)
        }

        // path == scalar
        JsonPathItemType::jpiEqual => {
            if not {
                return Ok(None);
            }

            let left_item = jspGetLeftArg(jsp);
            let right_item = jspGetRightArg(jsp);
            let scalar_item;
            let path_item;

            if jsp_is_scalar(left_item.typ) {
                scalar_item = &left_item;
                path_item = &right_item;
            } else if jsp_is_scalar(right_item.typ) {
                scalar_item = &right_item;
                path_item = &left_item;
            } else {
                return Ok(None); // at least one operand should be a scalar
            }

            let scalar = scalar_from_path_item(scalar_item)?;

            extract_jsp_path_expr(mcx, cxt, path, path_item, Some(&scalar))
        }

        _ => Ok(None), // not a boolean expression
    }
}

/// Build the [`JsonbValue`] scalar from a scalar jsonpath item (C: the
/// `switch (scalar_item->type)` block inside the `jpiEqual` case).
fn scalar_from_path_item(scalar_item: &JsonPathItem<'_>) -> PgResult<JsonbValue> {
    let v = match scalar_item.typ {
        JsonPathItemType::jpiNull => JsonbValue::null(),
        JsonPathItemType::jpiBool => JsonbValue {
            typ: jbvType::jbvBool,
            // scalar.val.boolean = !!*scalar_item->content.value.data;
            val: JsonbValueData::Bool(jspGetBool(scalar_item)),
        },
        JsonPathItemType::jpiNumeric => JsonbValue {
            typ: jbvType::jbvNumeric,
            // scalar.val.numeric = (Numeric) scalar_item->content.value.data;
            val: JsonbValueData::Numeric(jspGetNumeric(scalar_item).to_vec()),
        },
        JsonPathItemType::jpiString => JsonbValue {
            typ: jbvType::jbvString,
            // scalar.val.string.{val,len} = content.value.{data,datalen};
            val: JsonbValueData::String(jspGetString(scalar_item).to_vec()),
        },
        _ => {
            return Err(PgError::error(format!(
                "invalid scalar jsonpath item type: {}",
                scalar_item.typ as i32
            ))
            .with_sqlstate(ERRCODE_INTERNAL_ERROR));
        }
    };

    Ok(v)
}

/// C: `emit_jsp_gin_entries(JsonPathGinNode *node, GinEntries *entries)`.
/// Recursively emit all GIN entries in the tree, replacing each entry node's
/// datum with its index in `entries`.
pub fn emit_jsp_gin_entries(node: &mut JsonPathGinNode, entries: &mut GinEntries) -> PgResult<()> {
    stack_depth_seams::check_stack_depth::call()?;

    match node {
        JsonPathGinNode::EntryDatum(_) => {
            // replace datum with its index in the array
            let JsonPathGinNode::EntryDatum(datum) =
                core::mem::replace(node, JsonPathGinNode::EntryIndex(0))
            else {
                unreachable!()
            };
            let idx = gin_entries_add(entries, datum);
            *node = JsonPathGinNode::EntryIndex(idx);
        }
        JsonPathGinNode::EntryIndex(_) => {}
        JsonPathGinNode::Logic { args, .. } => {
            for arg in args.iter_mut() {
                emit_jsp_gin_entries(arg, entries)?;
            }
        }
    }
    Ok(())
}

/// C: `extract_jsp_query(JsonPath *jp, StrategyNumber strat, bool pathOps, ...)`.
/// Recursively extract GIN entries from a jsonpath query. Returns `None` when
/// no node is extracted (full scan needed); otherwise the entries + root node.
pub fn extract_jsp_query<'mcx>(
    mcx: Mcx<'mcx>,
    jp: &[u8],
    strat: u16,
    path_ops: bool,
) -> PgResult<Option<JspQuery>> {
    let cxt = JsonPathGinContext {
        path_ops,
        lax: jsonpath_is_lax(jp),
    };

    let path = if path_ops {
        JsonPathGinPath::Hash(0)
    } else {
        JsonPathGinPath::Items(Vec::new())
    };

    let root = jspInit(jp);

    let node = if strat == JsonbJsonpathExistsStrategyNumber {
        extract_jsp_path_expr(mcx, &cxt, path, &root, None)?
    } else {
        extract_jsp_bool_expr(mcx, &cxt, path, &root, false)?
    };

    let Some(mut node) = node else {
        return Ok(None);
    };

    let mut entries = GinEntries::default();
    emit_jsp_gin_entries(&mut node, &mut entries)?;

    if gin_entries_count(&entries) == 0 {
        return Ok(None);
    }

    Ok(Some(JspQuery {
        entries: entries.buf,
        node,
    }))
}

/// C: `execute_jsp_gin_node(JsonPathGinNode *node, void *check, bool ternary)`.
/// `check` is the GIN per-entry result vector, normalized to [`GinTernaryValue`]
/// form (the C `bool[]` case maps `true`/`false` to `GIN_TRUE`/`GIN_FALSE`
/// up front).
pub fn execute_jsp_gin_node(
    node: &JsonPathGinNode,
    check: &[GinTernaryValue],
) -> PgResult<GinTernaryValue> {
    match node {
        JsonPathGinNode::Logic { and: true, args } => {
            let mut res = GIN_TRUE;
            for arg in args {
                let v = execute_jsp_gin_node(arg, check)?;
                if v == GIN_FALSE {
                    return Ok(GIN_FALSE);
                } else if v == GIN_MAYBE {
                    res = GIN_MAYBE;
                }
            }
            Ok(res)
        }
        JsonPathGinNode::Logic { and: false, args } => {
            let mut res = GIN_FALSE;
            for arg in args {
                let v = execute_jsp_gin_node(arg, check)?;
                if v == GIN_TRUE {
                    return Ok(GIN_TRUE);
                } else if v == GIN_MAYBE {
                    res = GIN_MAYBE;
                }
            }
            Ok(res)
        }
        JsonPathGinNode::EntryIndex(index) => Ok(check[*index]),
        JsonPathGinNode::EntryDatum(_) => Err(PgError::error(
            "invalid jsonpath gin node type: entry datum not yet emitted",
        )
        .with_sqlstate(ERRCODE_INTERNAL_ERROR)),
    }
}

/// C: `gin_extract_jsonb_query(PG_FUNCTION_ARGS)`. Extract the GIN keys of a
/// query for the `jsonb_ops` opclass.
pub fn gin_extract_jsonb_query<'mcx>(
    mcx: Mcx<'mcx>,
    query: GinJsonbQuery<'_>,
    strategy: u16,
) -> PgResult<GinQueryExtraction> {
    let mut out = GinQueryExtraction::default();

    if strategy == JsonbContainsStrategyNumber {
        let GinJsonbQuery::Contains(jb_root) = query else {
            return Err(query_strategy_mismatch());
        };
        // Query is a jsonb, so just apply gin_extract_jsonb...
        out.entries = gin_extract_jsonb(mcx, jb_root)?;
        // ...although "contains {}" requires a full index scan
        if out.entries.is_empty() {
            out.search_mode_all = true;
        }
    } else if strategy == JsonbExistsStrategyNumber {
        let GinJsonbQuery::Exists(key) = query else {
            return Err(query_strategy_mismatch());
        };
        // Query is a text string, which we treat as a key
        out.entries = vec![make_text_key(JGINFLAG_KEY, key)];
    } else if strategy == JsonbExistsAnyStrategyNumber || strategy == JsonbExistsAllStrategyNumber {
        let GinJsonbQuery::ExistsArray(key_data) = query else {
            return Err(query_strategy_mismatch());
        };
        // Query is a text array; each element is treated as a key.
        let mut entries: Vec<Vec<u8>> = Vec::new();
        entries
            .try_reserve(key_data.len())
            .map_err(|_| out_of_memory())?;
        for elem in key_data {
            // Nulls in the array are ignored.
            match elem {
                // We rely on the array elements not being toasted.
                Some(bytes) => entries.push(make_text_key(JGINFLAG_KEY, bytes)),
                None => continue,
            }
        }

        let j = entries.len();
        out.entries = entries;
        // ExistsAll with no keys should match everything
        if j == 0 && strategy == JsonbExistsAllStrategyNumber {
            out.search_mode_all = true;
        }
    } else if strategy == JsonbJsonpathPredicateStrategyNumber
        || strategy == JsonbJsonpathExistsStrategyNumber
    {
        let GinJsonbQuery::Jsonpath(jp) = query else {
            return Err(query_strategy_mismatch());
        };
        match extract_jsp_query(mcx, jp, strategy, false)? {
            Some(q) => {
                out.entries = q.entries;
                out.node = Some(q.node);
            }
            None => out.search_mode_all = true,
        }
    } else {
        return Err(unrecognized_strategy(strategy));
    }

    Ok(out)
}

/// C: `gin_consistent_jsonb(PG_FUNCTION_ARGS)`. `check` is the per-entry boolean
/// result vector; `extra_data` is the root expression node (jsonpath strategies
/// only). Returns `(res, recheck)`.
pub fn gin_consistent_jsonb(
    check: &[bool],
    strategy: u16,
    nkeys: i32,
    extra_data: Option<&JsonPathGinNode>,
) -> PgResult<(bool, bool)> {
    let mut res = true;
    let recheck;

    if strategy == JsonbContainsStrategyNumber {
        // We must always recheck (can't tell from the index whether the matched
        // items' positions match the query object structure). However, the
        // tuple certainly doesn't match unless it contains all the query keys.
        recheck = true;
        // C: for (i = 0; i < nkeys; i++) if (!check[i]) { res = false; break; }
        if check[..nkeys as usize].iter().any(|&c| !c) {
            res = false;
        }
    } else if strategy == JsonbExistsStrategyNumber {
        // Although the key is certainly present in the index, we must recheck
        // (the key might be hashed, and the index match might be for a key
        // that's not at top level of the JSON object).
        recheck = true;
        res = true;
    } else if strategy == JsonbExistsAnyStrategyNumber {
        // As for plain exists, we must recheck
        recheck = true;
        res = true;
    } else if strategy == JsonbExistsAllStrategyNumber {
        // As for plain exists, we must recheck
        recheck = true;
        // ... but unless all the keys are present, we can say "false"
        if check[..nkeys as usize].iter().any(|&c| !c) {
            res = false;
        }
    } else if strategy == JsonbJsonpathPredicateStrategyNumber
        || strategy == JsonbJsonpathExistsStrategyNumber
    {
        recheck = true;

        if nkeys > 0 {
            let node = extra_data.ok_or_else(extra_data_assertion)?;
            let ternary = bool_check_to_ternary(check, nkeys as usize);
            res = execute_jsp_gin_node(node, &ternary)? != GIN_FALSE;
        }
    } else {
        return Err(unrecognized_strategy(strategy));
    }

    Ok((res, recheck))
}

/// C: `gin_triconsistent_jsonb(PG_FUNCTION_ARGS)`. `check` is the per-entry
/// [`GinTernaryValue`] result vector. Never returns `GIN_TRUE`.
pub fn gin_triconsistent_jsonb(
    check: &[GinTernaryValue],
    strategy: u16,
    nkeys: i32,
    extra_data: Option<&JsonPathGinNode>,
) -> PgResult<GinTernaryValue> {
    let mut res = GIN_MAYBE;

    if strategy == JsonbContainsStrategyNumber || strategy == JsonbExistsAllStrategyNumber {
        // All extracted keys must be present
        // C: for (i = 0; i < nkeys; i++) if (check[i] == GIN_FALSE) ...
        if check[..nkeys as usize].contains(&GIN_FALSE) {
            res = GIN_FALSE;
        }
    } else if strategy == JsonbExistsStrategyNumber || strategy == JsonbExistsAnyStrategyNumber {
        // At least one extracted key must be present.
        // C: for (i...) if (check[i] == GIN_TRUE || check[i] == GIN_MAYBE) ...
        res = GIN_FALSE;
        if check[..nkeys as usize]
            .iter()
            .any(|&c| c == GIN_TRUE || c == GIN_MAYBE)
        {
            res = GIN_MAYBE;
        }
    } else if strategy == JsonbJsonpathPredicateStrategyNumber
        || strategy == JsonbJsonpathExistsStrategyNumber
    {
        if nkeys > 0 {
            let node = extra_data.ok_or_else(extra_data_assertion)?;
            res = execute_jsp_gin_node(node, &check[..nkeys as usize])?;

            // Should always recheck the result
            if res == GIN_TRUE {
                res = GIN_MAYBE;
            }
        }
    } else {
        return Err(unrecognized_strategy(strategy));
    }

    Ok(res)
}

// ===========================================================================
// jsonb_path_ops opclass support functions
// ===========================================================================

/// A partial-hash stack level (C: `struct PathHashStack`). The C parent pointer
/// becomes the position within the explicit `Vec` stack.
#[derive(Clone, Copy, Debug)]
struct PathHashStack {
    hash: u32,
}

/// C: `gin_extract_jsonb_path(PG_FUNCTION_ARGS)`. Extract the GIN keys of a
/// jsonb value (`jsonb_path_ops`) — `uint32` hashes, one per JSON value, with
/// the leading key(s) folded into each value's hash.
pub fn gin_extract_jsonb_path(jb_root: &[u8]) -> PgResult<Vec<Vec<u8>>> {
    let total = jb_root_count(jb_root) as i32;

    // If the root level is empty, we certainly have no keys.
    if total == 0 {
        return Ok(Vec::new());
    }

    // Otherwise, use 2 * root count as initial estimate of result size.
    let mut entries = gin_entries_init(2 * total as usize)?;

    // Stack of partial hashes corresponding to parent key levels. C:
    // `tail.parent = NULL; tail.hash = 0; stack = &tail;`. The bottom entry
    // models `tail`; a missing parent (index 0's "parent") is the C NULL parent.
    let mut stack: Vec<PathHashStack> = vec![PathHashStack { hash: 0 }];

    let mut it = JsonbIteratorInit(jb_root);
    let mut v = JsonbValue::null();

    loop {
        let r = JsonbIteratorNext(&mut it, &mut v, false)?;
        if r == JsonbIteratorToken::WJB_DONE {
            break;
        }

        match r {
            JsonbIteratorToken::WJB_BEGIN_ARRAY | JsonbIteratorToken::WJB_BEGIN_OBJECT => {
                // Push a stack level for this object. We pass forward hashes from
                // outer nesting levels so nested values' hashes include outer
                // keys as well as their own keys.
                let parent_hash = stack
                    .last()
                    .ok_or_else(hash_stack_empty)?
                    .hash;
                stack.push(PathHashStack { hash: parent_hash });
            }
            JsonbIteratorToken::WJB_KEY => {
                // mix this key into the current outer hash; hash is now ready to
                // incorporate the value
                let top = stack.last_mut().ok_or_else(hash_stack_empty)?;
                JsonbHashScalarValue(&v, &mut top.hash)?;
            }
            JsonbIteratorToken::WJB_ELEM | JsonbIteratorToken::WJB_VALUE => {
                // mix the element or value's hash into the prepared hash
                {
                    let top = stack.last_mut().ok_or_else(hash_stack_empty)?;
                    JsonbHashScalarValue(&v, &mut top.hash)?;
                    // and emit an index entry
                    let entry = uint32_get_datum(top.hash);
                    gin_entries_add(&mut entries, entry);
                }
                // reset hash for next key, value, or sub-object:
                // stack->hash = stack->parent->hash;
                let parent_hash = parent_hash_of_top(&stack);
                stack
                    .last_mut()
                    .ok_or_else(hash_stack_empty)?
                    .hash = parent_hash;
            }
            JsonbIteratorToken::WJB_END_ARRAY | JsonbIteratorToken::WJB_END_OBJECT => {
                // Pop the stack
                stack.pop();
                // reset hash for next key, value, or sub-object:
                //   if (stack->parent) stack->hash = stack->parent->hash;
                //   else stack->hash = 0;
                let new_hash = parent_hash_of_top(&stack);
                if let Some(top) = stack.last_mut() {
                    top.hash = new_hash;
                }
            }
            _ => {
                return Err(invalid_iterator_rc(r));
            }
        }
    }

    Ok(entries.buf)
}

/// C: `stack->parent ? stack->parent->hash : 0` for the current top of stack.
/// The top is `stack[len-1]`; its parent is `stack[len-2]` (or "NULL" => 0).
#[inline]
fn parent_hash_of_top(stack: &[PathHashStack]) -> u32 {
    if stack.len() >= 2 {
        stack[stack.len() - 2].hash
    } else {
        0
    }
}

/// C: `gin_extract_jsonb_query_path(PG_FUNCTION_ARGS)`. Extract the GIN keys of
/// a query for the `jsonb_path_ops` opclass.
pub fn gin_extract_jsonb_query_path<'mcx>(
    mcx: Mcx<'mcx>,
    query: GinJsonbQuery<'_>,
    strategy: u16,
) -> PgResult<GinQueryExtraction> {
    let mut out = GinQueryExtraction::default();

    if strategy == JsonbContainsStrategyNumber {
        let GinJsonbQuery::Contains(jb_root) = query else {
            return Err(query_strategy_mismatch());
        };
        // Query is a jsonb, so just apply gin_extract_jsonb_path ...
        out.entries = gin_extract_jsonb_path(jb_root)?;
        // ... although "contains {}" requires a full index scan
        if out.entries.is_empty() {
            out.search_mode_all = true;
        }
    } else if strategy == JsonbJsonpathPredicateStrategyNumber
        || strategy == JsonbJsonpathExistsStrategyNumber
    {
        let GinJsonbQuery::Jsonpath(jp) = query else {
            return Err(query_strategy_mismatch());
        };
        match extract_jsp_query(mcx, jp, strategy, true)? {
            Some(q) => {
                out.entries = q.entries;
                out.node = Some(q.node);
            }
            None => out.search_mode_all = true,
        }
    } else {
        return Err(unrecognized_strategy(strategy));
    }

    Ok(out)
}

/// C: `gin_consistent_jsonb_path(PG_FUNCTION_ARGS)`. Returns `(res, recheck)`.
pub fn gin_consistent_jsonb_path(
    check: &[bool],
    strategy: u16,
    nkeys: i32,
    extra_data: Option<&JsonPathGinNode>,
) -> PgResult<(bool, bool)> {
    let mut res = true;
    let recheck;

    if strategy == JsonbContainsStrategyNumber {
        // jsonb_path_ops is necessarily lossy, so we must always recheck a
        // match. However, if not all of the keys are present, the tuple
        // certainly doesn't match.
        recheck = true;
        if check[..nkeys as usize].iter().any(|&c| !c) {
            res = false;
        }
    } else if strategy == JsonbJsonpathPredicateStrategyNumber
        || strategy == JsonbJsonpathExistsStrategyNumber
    {
        recheck = true;

        if nkeys > 0 {
            let node = extra_data.ok_or_else(extra_data_assertion)?;
            let ternary = bool_check_to_ternary(check, nkeys as usize);
            res = execute_jsp_gin_node(node, &ternary)? != GIN_FALSE;
        }
    } else {
        return Err(unrecognized_strategy(strategy));
    }

    Ok((res, recheck))
}

/// C: `gin_triconsistent_jsonb_path(PG_FUNCTION_ARGS)`.
pub fn gin_triconsistent_jsonb_path(
    check: &[GinTernaryValue],
    strategy: u16,
    nkeys: i32,
    extra_data: Option<&JsonPathGinNode>,
) -> PgResult<GinTernaryValue> {
    let mut res = GIN_MAYBE;

    if strategy == JsonbContainsStrategyNumber {
        // Note that we never return GIN_TRUE, only GIN_MAYBE or GIN_FALSE.
        if check[..nkeys as usize].contains(&GIN_FALSE) {
            res = GIN_FALSE;
        }
    } else if strategy == JsonbJsonpathPredicateStrategyNumber
        || strategy == JsonbJsonpathExistsStrategyNumber
    {
        if nkeys > 0 {
            let node = extra_data.ok_or_else(extra_data_assertion)?;
            res = execute_jsp_gin_node(node, &check[..nkeys as usize])?;

            // Should always recheck the result
            if res == GIN_TRUE {
                res = GIN_MAYBE;
            }
        }
    } else {
        return Err(unrecognized_strategy(strategy));
    }

    Ok(res)
}

// ===========================================================================
// Small helpers
// ===========================================================================

/// C: `SET_VARSIZE(item, size)` — store `(size << 2)` in the header word on
/// little-endian; on big-endian the high bit is the 4B-uncompressed flag (0),
/// matching `SET_VARSIZE_4B`.
#[inline]
fn set_varsize(item: &mut [u8], size: u32) {
    let w = if cfg!(target_endian = "big") {
        size.to_ne_bytes()
    } else {
        (size << 2).to_ne_bytes()
    };
    item[0..4].copy_from_slice(&w);
}

/// C: `UInt32GetDatum(hash)` rendered as the GIN key bytes. In a
/// `jsonb_path_ops` index the GIN keys are bare `uint32` Datums (passbyval), not
/// varlenas, so the key is just the 4 native-endian hash bytes.
#[inline]
fn uint32_get_datum(hash: u32) -> Vec<u8> {
    hash.to_ne_bytes().to_vec()
}

/// Map a `bool[]` `check` vector to the [`GinTernaryValue`] form
/// [`execute_jsp_gin_node`] consumes (C's `((bool *) check)[index] ? GIN_TRUE :
/// GIN_FALSE` mapping done up front).
#[inline]
fn bool_check_to_ternary(check: &[bool], nkeys: usize) -> Vec<GinTernaryValue> {
    check[..nkeys]
        .iter()
        .map(|&b| if b { GIN_TRUE } else { GIN_FALSE })
        .collect()
}

/// OOM building one of the data-derived buffers.
fn out_of_memory() -> PgError {
    PgError::error("out of memory").with_sqlstate(ERRCODE_OUT_OF_MEMORY)
}

/// C: `elog(ERROR, "invalid JsonbIteratorNext rc")` for an empty hash stack.
fn hash_stack_empty() -> PgError {
    PgError::error("gin_extract_jsonb_path: hash stack is empty")
        .with_sqlstate(ERRCODE_INTERNAL_ERROR)
}

/// C: `Assert(extra_data && extra_data[0])` — the jsonpath strategies always
/// store the root node in `extra_data[0]` when `nkeys > 0`.
fn extra_data_assertion() -> PgError {
    PgError::error("jsonb_gin consistent: missing extra_data root node")
        .with_sqlstate(ERRCODE_INTERNAL_ERROR)
}

/// The supplied [`GinJsonbQuery`] variant does not match the strategy number.
fn query_strategy_mismatch() -> PgError {
    PgError::error("jsonb_gin query argument does not match the strategy number")
        .with_sqlstate(ERRCODE_INTERNAL_ERROR)
}

/// C: `elog(ERROR, "unrecognized strategy number: %d", strategy)`.
fn unrecognized_strategy(strategy: u16) -> PgError {
    PgError::error(format!("unrecognized strategy number: {strategy}"))
        .with_sqlstate(ERRCODE_INTERNAL_ERROR)
}

/// C: `elog(ERROR, "invalid JsonbIteratorNext rc: %d", (int) r)`.
fn invalid_iterator_rc(r: JsonbIteratorToken) -> PgError {
    PgError::error(format!("invalid JsonbIteratorNext rc: {}", r as i32))
        .with_sqlstate(ERRCODE_INTERNAL_ERROR)
}

/// C: `elog(ERROR, "unrecognized jsonb scalar type: %d", scalarVal->type)`.
fn unrecognized_scalar_type(v: &JsonbValue) -> PgError {
    PgError::error(format!("unrecognized jsonb scalar type: {}", v.typ as i32))
        .with_sqlstate(ERRCODE_INTERNAL_ERROR)
}
