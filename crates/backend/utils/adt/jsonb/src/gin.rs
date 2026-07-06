//! jsonb_gin.c: jsonb_ops + jsonb_path_ops GIN support, including the
//! jsonpath (@? / @@) query extraction tree.

extern crate alloc;

use crate::container::JsonbItem;
use crate::iter::{JsonbIterator, WjbToken};
use adt_jsonpath::path::{jsp_init_by_buffer, ItemType, JsonPathItem, JSONPATH_LAX};
use adt_numeric::{get_str_from_var, Num};
use datum::Datum;
use gin_vocab::{JspGinOp, JSP_GIN_AND, JSP_GIN_ENTRY, JSP_GIN_OR};
use mcx::{Mcx, PgVec};
use types_error::PgResult;

pub const JsonbContainsStrategyNumber: u16 = 7;
pub const JsonbExistsStrategyNumber: u16 = 9;
pub const JsonbExistsAnyStrategyNumber: u16 = 10;
pub const JsonbExistsAllStrategyNumber: u16 = 11;
pub const JsonbJsonpathExistsStrategyNumber: u16 = 15;
pub const JsonbJsonpathPredicateStrategyNumber: u16 = 16;

const JGINFLAG_KEY: u8 = 0x01;
const JGINFLAG_NULL: u8 = 0x02;
const JGINFLAG_BOOL: u8 = 0x03;
const JGINFLAG_NUM: u8 = 0x04;
const JGINFLAG_STR: u8 = 0x05;
const JGINFLAG_HASHED: u8 = 0x10;
const JGIN_MAXLENGTH: usize = 125;

pub const GIN_SEARCH_MODE_DEFAULT: i32 = 0;
pub const GIN_SEARCH_MODE_ALL: i32 = 2;

const GIN_FALSE: i8 = 0;
const GIN_TRUE: i8 = 1;
const GIN_MAYBE: i8 = 2;

/// gin_compare_jsonb over text payloads (header already stripped). C uses
/// varstr_cmp with C collation == memcmp + length tiebreak.
pub fn gin_compare_jsonb(a: &[u8], b: &[u8]) -> i32 {
    varlena::varstrfastcmp_c(a, b)
}

/// make_text_key: 4-byte-header text datum `flag || str` (hashing overlength
/// keys), allocated in `mcx`.
fn make_text_key<'m>(mcx: Mcx<'m>, mut flag: u8, s: &[u8]) -> PgResult<Datum> {
    let mut hashbuf = [0u8; 8];
    let str_: &[u8] = if s.len() > JGIN_MAXLENGTH {
        let hashval = hashfn::hash_bytes(s);
        let hex = alloc::format!("{hashval:08x}");
        hashbuf.copy_from_slice(hex.as_bytes());
        flag |= JGINFLAG_HASHED;
        &hashbuf
    } else {
        s
    };

    let len = str_.len();
    let total = 4 + len + 1;
    let mut item: PgVec<'m, u8> = mcx::vec_with_capacity_in(mcx, total)?;
    mcx::vec_append_bytes(&mut item, &::types_tuple::varatt::set_varsize_4b_word(total as u32).to_ne_bytes())?;
    mcx::vec_append_bytes(&mut item, &[flag])?;
    mcx::vec_append_bytes(&mut item, str_)?;
    let p = item.as_ptr();
    core::mem::forget(item);
    Ok(Datum::from_usize(p as usize))
}

/// numeric_normalize: render with dscale, then strip trailing fractional
/// zeroes (and a bare trailing '.') textually, matching C exactly so that
/// 25 and 25.0 produce the same GIN key.
fn numeric_normalize(image: &[u8], out: &mut alloc::vec::Vec<u8>) {
    // JsonbItem::Numeric carries the full 4-byte-header numeric image.
    let num = Num::from_payload(&image[4..]);
    get_str_from_var(num.view(), out);
    if out.contains(&b'.') {
        let mut last = out.len();
        while out[last - 1] == b'0' {
            last -= 1;
        }
        if out[last - 1] == b'.' {
            last -= 1;
        }
        out.truncate(last);
    }
}

/// make_scalar_key.
fn make_scalar_key<'m>(mcx: Mcx<'m>, v: &JsonbItem<'_>, is_key: bool) -> PgResult<Datum> {
    match v {
        JsonbItem::Null => {
            debug_assert!(!is_key);
            make_text_key(mcx, JGINFLAG_NULL, b"")
        }
        JsonbItem::Bool(b) => {
            debug_assert!(!is_key);
            make_text_key(mcx, JGINFLAG_BOOL, if *b { b"t" } else { b"f" })
        }
        JsonbItem::Numeric(image) => {
            debug_assert!(!is_key);
            let mut cstr = alloc::vec::Vec::new();
            numeric_normalize(image, &mut cstr);
            make_text_key(mcx, JGINFLAG_NUM, &cstr)
        }
        JsonbItem::String(s) => {
            make_text_key(mcx, if is_key { JGINFLAG_KEY } else { JGINFLAG_STR }, s)
        }
        other => panic!("unrecognized jsonb scalar type: {}", other.type_ord()),
    }
}

/// gin_extract_jsonb over a detoasted jsonb payload.
pub fn gin_extract_jsonb<'m>(mcx: Mcx<'m>, payload: &[u8]) -> PgResult<PgVec<'m, Datum>> {
    let mut entries: PgVec<'m, Datum> = mcx::vec_new_in(mcx);

    let mut it = JsonbIterator::init(mcx, payload)?;
    loop {
        let (tok, v) = it.next(false);
        match tok {
            WjbToken::Done => break,
            WjbToken::Key => entries.push(make_scalar_key(mcx, &v, true)?),
            WjbToken::Elem => {
                let is_key = matches!(v, JsonbItem::String(_));
                entries.push(make_scalar_key(mcx, &v, is_key)?);
            }
            WjbToken::Value => entries.push(make_scalar_key(mcx, &v, false)?),
            _ => {}
        }
    }
    Ok(entries)
}

/// gin_extract_jsonb_path: one hash per value, keys mixed into the running
/// path hash (PathHashStack).
pub fn gin_extract_jsonb_path<'m>(mcx: Mcx<'m>, payload: &[u8]) -> PgResult<PgVec<'m, Datum>> {
    let mut entries: PgVec<'m, Datum> = mcx::vec_new_in(mcx);
    let mut stack: PgVec<'m, u32> = mcx::vec_with_capacity_in(mcx, 8)?;
    stack.push(0);

    let mut it = JsonbIterator::init(mcx, payload)?;
    loop {
        let (tok, v) = it.next(false);
        match tok {
            WjbToken::Done => break,
            WjbToken::BeginArray | WjbToken::BeginObject => {
                let parent = *stack.last().expect("stack has the sentinel");
                stack.push(parent);
            }
            WjbToken::Key => {
                let top = stack.last_mut().expect("stack has the sentinel");
                crate::ops::hash_scalar_value(&v, top);
            }
            WjbToken::Elem | WjbToken::Value => {
                let top = stack.len() - 1;
                crate::ops::hash_scalar_value(&v, &mut stack[top]);
                entries.push(Datum::from_u32(stack[top]));
                stack[top] = if top > 0 { stack[top - 1] } else { 0 };
            }
            WjbToken::EndArray | WjbToken::EndObject => {
                stack.pop();
                let top = stack.len() - 1;
                stack[top] = if top > 0 { stack[top - 1] } else { 0 };
            }
        }
    }
    Ok(entries)
}

const NO_ITEM: u32 = u32::MAX;

struct JspPathItem {
    parent: u32,
    key_name: Datum,
    typ: ItemType,
}

/// JsonPathGinPath: persistent path-item chain head (jsonb_ops) or running
/// hash (jsonb_path_ops).
#[derive(Clone, Copy)]
enum JspGinPath {
    Items(u32),
    Hash(u32),
}

// Tree nodes hold (start, len) into the extractor's args pool; a nested
// PgVec would be droppy and barred from the arena helpers.
struct TreeNode {
    kind: u8,
    entry: Datum,
    args_start: u32,
    args_len: u32,
}

struct JspExtractor<'m> {
    mcx: Mcx<'m>,
    lax: bool,
    path_items: PgVec<'m, JspPathItem>,
    nodes: PgVec<'m, TreeNode>,
    args_pool: PgVec<'m, u32>,
}

impl<'m> JspExtractor<'m> {
    fn entry_node(&mut self, entry: Datum) -> u32 {
        self.nodes.push(TreeNode {
            kind: JSP_GIN_ENTRY,
            entry,
            args_start: 0,
            args_len: 0,
        });
        (self.nodes.len() - 1) as u32
    }

    fn entry_node_scalar(&mut self, scalar: &JsonbItem<'_>, is_key: bool) -> PgResult<u32> {
        let d = make_scalar_key(self.mcx, scalar, is_key)?;
        Ok(self.entry_node(d))
    }

    fn expr_node(&mut self, kind: u8, args: &[u32]) -> u32 {
        let start = self.args_pool.len() as u32;
        for &a in args {
            self.args_pool.push(a);
        }
        self.nodes.push(TreeNode {
            kind,
            entry: Datum::null(),
            args_start: start,
            args_len: args.len() as u32,
        });
        (self.nodes.len() - 1) as u32
    }

    fn expr_node_binary(&mut self, kind: u8, a: u32, b: u32) -> PgResult<u32> {
        Ok(self.expr_node(kind, &[a, b]))
    }

    /// jsonb_ops__add_path_item / jsonb_path_ops__add_path_item.
    fn add_path_item(&mut self, path: &mut JspGinPath, jsp: &JsonPathItem<'_>) -> PgResult<bool> {
        match path {
            JspGinPath::Items(head) => {
                let key_name = match jsp.typ {
                    ItemType::Root => {
                        *head = NO_ITEM;
                        return Ok(true);
                    }
                    ItemType::Key => make_text_key(self.mcx, JGINFLAG_KEY, jsp.get_string())?,
                    ItemType::Any
                    | ItemType::AnyKey
                    | ItemType::AnyArray
                    | ItemType::IndexArray => Datum::null(),
                    _ => return Ok(false),
                };
                self.path_items.push(JspPathItem {
                    parent: *head,
                    key_name,
                    typ: jsp.typ,
                });
                *head = (self.path_items.len() - 1) as u32;
                Ok(true)
            }
            JspGinPath::Hash(hash) => match jsp.typ {
                ItemType::Root => {
                    *hash = 0;
                    Ok(true)
                }
                ItemType::Key => {
                    crate::ops::hash_scalar_value(&JsonbItem::String(jsp.get_string()), hash);
                    Ok(true)
                }
                ItemType::IndexArray | ItemType::AnyArray => Ok(true),
                _ => Ok(false),
            },
        }
    }

    /// jsonb_ops__extract_nodes / jsonb_path_ops__extract_nodes.
    fn extract_nodes(
        &mut self,
        path: JspGinPath,
        scalar: Option<&JsonbItem<'_>>,
        nodes: &mut PgVec<'m, u32>,
    ) -> PgResult<()> {
        match path {
            JspGinPath::Items(head) => {
                let Some(scalar) = scalar else {
                    return Ok(());
                };
                let mut pentry = head;
                while pentry != NO_ITEM {
                    let item = &self.path_items[pentry as usize];
                    let (typ, key_name, parent) = (item.typ, item.key_name, item.parent);
                    if typ == ItemType::Key {
                        let n = self.entry_node(key_name);
                        nodes.push(n);
                    }
                    pentry = parent;
                }

                let node = if matches!(scalar, JsonbItem::String(_)) {
                    // String consts may match key entries (string array
                    // elements are indexed as keys): lax mode or jpiAny yield
                    // MAYBE (OR of both), array accessors yield key-entry.
                    let key_entry = if self.lax {
                        GIN_MAYBE
                    } else if head == NO_ITEM {
                        GIN_FALSE
                    } else {
                        match self.path_items[head as usize].typ {
                            ItemType::AnyArray | ItemType::IndexArray => GIN_TRUE,
                            ItemType::Any => GIN_MAYBE,
                            _ => GIN_FALSE,
                        }
                    };
                    if key_entry == GIN_MAYBE {
                        let n1 = self.entry_node_scalar(scalar, true)?;
                        let n2 = self.entry_node_scalar(scalar, false)?;
                        self.expr_node_binary(JSP_GIN_OR, n1, n2)?
                    } else {
                        self.entry_node_scalar(scalar, key_entry == GIN_TRUE)?
                    }
                } else {
                    self.entry_node_scalar(scalar, false)?
                };
                nodes.push(node);
                Ok(())
            }
            JspGinPath::Hash(hash) => {
                // jsonb_path_ops doesn't support EXISTS => nothing to append.
                if let Some(scalar) = scalar {
                    let mut h = hash;
                    crate::ops::hash_scalar_value(scalar, &mut h);
                    let n = self.entry_node(Datum::from_u32(h));
                    nodes.push(n);
                }
                Ok(())
            }
        }
    }

    /// extract_jsp_path_expr_nodes + extract_jsp_path_expr.
    fn extract_path_expr(
        &mut self,
        mut path: JspGinPath,
        jsp: &JsonPathItem<'_>,
        scalar: Option<&JsonbItem<'_>>,
    ) -> PgResult<Option<u32>> {
        let mut nodes: PgVec<'m, u32> = mcx::vec_new_in(self.mcx);
        let mut cur = jsp.clone();
        loop {
            match cur.typ {
                ItemType::Current => {}
                ItemType::Filter => {
                    let arg = cur.arg();
                    if let Some(filter) = self.extract_bool_expr(path, &arg, false)? {
                        nodes.push(filter);
                    }
                }
                _ => {
                    if !self.add_path_item(&mut path, &cur)? {
                        // Path unsupported by the opclass: only filter nodes.
                        return self.and_nodes(nodes);
                    }
                }
            }
            match cur.next() {
                Some(next) => cur = next,
                None => break,
            }
        }
        self.extract_nodes(path, scalar, &mut nodes)?;
        self.and_nodes(nodes)
    }

    fn and_nodes(&mut self, nodes: PgVec<'m, u32>) -> PgResult<Option<u32>> {
        match nodes.len() {
            0 => Ok(None),
            1 => Ok(Some(nodes[0])),
            _ => Ok(Some(self.expr_node(JSP_GIN_AND, nodes.as_slice()))),
        }
    }

    /// extract_jsp_bool_expr.
    fn extract_bool_expr(
        &mut self,
        path: JspGinPath,
        jsp: &JsonPathItem<'_>,
        not: bool,
    ) -> PgResult<Option<u32>> {
        stack_depth::check_stack_depth()?;

        match jsp.typ {
            ItemType::And | ItemType::Or => {
                let larg = self.extract_bool_expr(path, &jsp.left_arg(), not)?;
                let rarg = self.extract_bool_expr(path, &jsp.right_arg(), not)?;
                let (Some(l), Some(r)) = (larg, rarg) else {
                    if jsp.typ == ItemType::Or {
                        return Ok(None);
                    }
                    return Ok(larg.or(rarg));
                };
                let kind = if not ^ (jsp.typ == ItemType::And) {
                    JSP_GIN_AND
                } else {
                    JSP_GIN_OR
                };
                Ok(Some(self.expr_node_binary(kind, l, r)?))
            }
            ItemType::Not => self.extract_bool_expr(path, &jsp.arg(), !not),
            ItemType::Exists => {
                if not {
                    return Ok(None);
                }
                self.extract_path_expr(path, &jsp.arg(), None)
            }
            // '!(path != scalar)' is not 'path == scalar' under sequence
            // comparison semantics; not extractable.
            ItemType::NotEqual => Ok(None),
            ItemType::Equal => {
                if not {
                    return Ok(None);
                }
                let left = jsp.left_arg();
                let right = jsp.right_arg();
                let (scalar_item, path_item) = if jsp_is_scalar(left.typ) {
                    (left, right)
                } else if jsp_is_scalar(right.typ) {
                    (right, left)
                } else {
                    return Ok(None);
                };
                let scalar = match scalar_item.typ {
                    ItemType::Null => JsonbItem::Null,
                    ItemType::Bool => JsonbItem::Bool(scalar_item.get_bool()),
                    ItemType::Numeric => JsonbItem::Numeric(scalar_item.get_numeric()),
                    ItemType::String => JsonbItem::String(scalar_item.get_string()),
                    other => panic!("invalid scalar jsonpath item type: {}", other as i32),
                };
                self.extract_path_expr(path, &path_item, Some(&scalar))
            }
            _ => Ok(None),
        }
    }

    /// emit_jsp_gin_entries: preorder emit assigns entry indices and
    /// flattens the tree into `ops`.
    fn emit(
        &self,
        node: u32,
        entries: &mut PgVec<'m, Datum>,
        ops: &mut PgVec<'m, JspGinOp>,
    ) -> PgResult<()> {
        stack_depth::check_stack_depth()?;
        let n = &self.nodes[node as usize];
        match n.kind {
            JSP_GIN_ENTRY => {
                ops.push(JspGinOp {
                    kind: JSP_GIN_ENTRY,
                    val: entries.len() as u32,
                });
                entries.push(n.entry);
            }
            _ => {
                let (start, len) = (n.args_start as usize, n.args_len as usize);
                ops.push(JspGinOp {
                    kind: n.kind,
                    val: n.args_len,
                });
                for i in start..start + len {
                    self.emit(self.args_pool[i], entries, ops)?;
                }
            }
        }
        Ok(())
    }
}

#[inline]
fn jsp_is_scalar(t: ItemType) -> bool {
    matches!(
        t,
        ItemType::Null | ItemType::String | ItemType::Numeric | ItemType::Bool
    )
}

/// extract_jsp_query over a detoasted jsonpath payload (varlena header
/// stripped: header word at [0..4], flattened data at [4..]). Empty entries
/// means "full scan needed".
pub fn extract_jsp_query<'m>(
    mcx: Mcx<'m>,
    jp_payload: &[u8],
    strategy: u16,
    path_ops: bool,
) -> PgResult<(PgVec<'m, Datum>, PgVec<'m, JspGinOp>)> {
    let header = u32::from_ne_bytes(jp_payload[0..4].try_into().expect("jsonpath header word"));
    let mut ext = JspExtractor {
        mcx,
        lax: header & JSONPATH_LAX != 0,
        path_items: mcx::vec_new_in(mcx),
        nodes: mcx::vec_new_in(mcx),
        args_pool: mcx::vec_new_in(mcx),
    };
    let path = if path_ops {
        JspGinPath::Hash(0)
    } else {
        JspGinPath::Items(NO_ITEM)
    };
    let root = jsp_init_by_buffer(&jp_payload[4..], 0);

    let node = if strategy == JsonbJsonpathExistsStrategyNumber {
        ext.extract_path_expr(path, &root, None)?
    } else {
        ext.extract_bool_expr(path, &root, false)?
    };

    let mut entries: PgVec<'m, Datum> = mcx::vec_new_in(mcx);
    let mut ops: PgVec<'m, JspGinOp> = mcx::vec_new_in(mcx);
    if let Some(node) = node {
        ext.emit(node, &mut entries, &mut ops)?;
    }
    Ok((entries, ops))
}

/// execute_jsp_gin_node over the preorder ops. Unlike C's pointer tree, the
/// flat walk consumes every child to keep sibling offsets aligned.
pub fn execute_jsp_gin_ops(ops: &[JspGinOp], check: &[i8], ternary: bool) -> i8 {
    let mut pos = 0usize;
    let res = exec_jsp_node(ops, &mut pos, check, ternary);
    debug_assert!(pos == ops.len());
    res
}

fn exec_jsp_node(ops: &[JspGinOp], pos: &mut usize, check: &[i8], ternary: bool) -> i8 {
    let op = ops[*pos];
    *pos += 1;
    match op.kind {
        JSP_GIN_ENTRY => {
            let c = check[op.val as usize];
            if ternary {
                c
            } else if c != GIN_FALSE {
                GIN_TRUE
            } else {
                GIN_FALSE
            }
        }
        JSP_GIN_AND => {
            let mut res = GIN_TRUE;
            for _ in 0..op.val {
                let v = exec_jsp_node(ops, pos, check, ternary);
                if v == GIN_FALSE {
                    res = GIN_FALSE;
                } else if v == GIN_MAYBE && res != GIN_FALSE {
                    res = GIN_MAYBE;
                }
            }
            res
        }
        JSP_GIN_OR => {
            let mut res = GIN_FALSE;
            for _ in 0..op.val {
                let v = exec_jsp_node(ops, pos, check, ternary);
                if v == GIN_TRUE {
                    res = GIN_TRUE;
                } else if v == GIN_MAYBE && res != GIN_TRUE {
                    res = GIN_MAYBE;
                }
            }
            res
        }
        other => panic!("invalid jsonpath gin node type: {other}"),
    }
}

fn extract_text_array_keys<'m>(mcx: Mcx<'m>, query_image: &[u8]) -> PgResult<PgVec<'m, Datum>> {
    let (elems, nulls) =
        arrayfuncs::deconstruct_array_builtin(mcx, query_image, types_core::TEXTOID, true)?;
    let mut entries: PgVec<'m, Datum> = mcx::vec_with_capacity_in(mcx, elems.len())?;
    for (d, isnull) in elems.iter().zip(nulls.iter()) {
        if *isnull {
            continue;
        }
        // SAFETY: non-null text element datums point into the flat image.
        let pv = unsafe { types_fmgr::PackedVarlena::from_ptr(d.as_usize() as *const u8) };
        entries.push(make_text_key(mcx, JGINFLAG_KEY, pv.data())?);
    }
    Ok(entries)
}

/// gin_extract_jsonb_query. `query_image` is the detoasted flat 4-byte-header
/// image of the right-hand operand (jsonb for @>, text for ?, text[] for
/// ?| / ?&, jsonpath for @? / @@).
pub fn gin_extract_jsonb_query<'m>(
    mcx: Mcx<'m>,
    query_image: &[u8],
    strategy: u16,
) -> PgResult<(PgVec<'m, Datum>, i32, PgVec<'m, JspGinOp>)> {
    let mut search_mode = GIN_SEARCH_MODE_DEFAULT;
    let mut ops: PgVec<'m, JspGinOp> = mcx::vec_new_in(mcx);
    let entries = match strategy {
        JsonbContainsStrategyNumber => {
            let entries = gin_extract_jsonb(mcx, &query_image[4..])?;
            if entries.is_empty() {
                search_mode = GIN_SEARCH_MODE_ALL;
            }
            entries
        }
        JsonbExistsStrategyNumber => {
            let mut entries: PgVec<'m, Datum> = mcx::vec_with_capacity_in(mcx, 1)?;
            entries.push(make_text_key(mcx, JGINFLAG_KEY, &query_image[4..])?);
            entries
        }
        JsonbExistsAnyStrategyNumber => extract_text_array_keys(mcx, query_image)?,
        JsonbExistsAllStrategyNumber => {
            let entries = extract_text_array_keys(mcx, query_image)?;
            if entries.is_empty() {
                search_mode = GIN_SEARCH_MODE_ALL;
            }
            entries
        }
        JsonbJsonpathExistsStrategyNumber | JsonbJsonpathPredicateStrategyNumber => {
            let (entries, jsp_ops) = extract_jsp_query(mcx, &query_image[4..], strategy, false)?;
            if entries.is_empty() {
                search_mode = GIN_SEARCH_MODE_ALL;
            }
            ops = jsp_ops;
            entries
        }
        other => panic!("unrecognized strategy number: {other}"),
    };
    Ok((entries, search_mode, ops))
}

/// gin_extract_jsonb_query_path.
pub fn gin_extract_jsonb_query_path<'m>(
    mcx: Mcx<'m>,
    query_image: &[u8],
    strategy: u16,
) -> PgResult<(PgVec<'m, Datum>, i32, PgVec<'m, JspGinOp>)> {
    let mut search_mode = GIN_SEARCH_MODE_DEFAULT;
    let mut ops: PgVec<'m, JspGinOp> = mcx::vec_new_in(mcx);
    let entries = match strategy {
        JsonbContainsStrategyNumber => {
            let entries = gin_extract_jsonb_path(mcx, &query_image[4..])?;
            if entries.is_empty() {
                search_mode = GIN_SEARCH_MODE_ALL;
            }
            entries
        }
        JsonbJsonpathExistsStrategyNumber | JsonbJsonpathPredicateStrategyNumber => {
            let (entries, jsp_ops) = extract_jsp_query(mcx, &query_image[4..], strategy, true)?;
            if entries.is_empty() {
                search_mode = GIN_SEARCH_MODE_ALL;
            }
            ops = jsp_ops;
            entries
        }
        other => panic!("unrecognized strategy number: {other}"),
    };
    Ok((entries, search_mode, ops))
}

/// gin_consistent_jsonb.
pub fn gin_consistent_jsonb(
    check: &[i8],
    strategy: u16,
    nkeys: usize,
    recheck: &mut bool,
    jsp_ops: &[JspGinOp],
) -> bool {
    match strategy {
        JsonbContainsStrategyNumber => {
            *recheck = true;
            check[..nkeys].iter().all(|&c| c != 0)
        }
        JsonbExistsStrategyNumber | JsonbExistsAnyStrategyNumber => {
            *recheck = true;
            true
        }
        JsonbExistsAllStrategyNumber => {
            *recheck = true;
            check[..nkeys].iter().all(|&c| c != 0)
        }
        JsonbJsonpathExistsStrategyNumber | JsonbJsonpathPredicateStrategyNumber => {
            *recheck = true;
            if nkeys > 0 {
                debug_assert!(!jsp_ops.is_empty());
                execute_jsp_gin_ops(jsp_ops, check, false) != GIN_FALSE
            } else {
                true
            }
        }
        other => panic!("unrecognized strategy number: {other}"),
    }
}

/// gin_triconsistent_jsonb: never GIN_TRUE (recheck always required).
pub fn gin_triconsistent_jsonb(
    check: &[i8],
    strategy: u16,
    nkeys: usize,
    jsp_ops: &[JspGinOp],
) -> i8 {
    match strategy {
        JsonbContainsStrategyNumber | JsonbExistsAllStrategyNumber => {
            for &c in &check[..nkeys] {
                if c == GIN_FALSE {
                    return GIN_FALSE;
                }
            }
            GIN_MAYBE
        }
        JsonbExistsStrategyNumber | JsonbExistsAnyStrategyNumber => {
            for &c in &check[..nkeys] {
                if c == GIN_TRUE || c == GIN_MAYBE {
                    return GIN_MAYBE;
                }
            }
            GIN_FALSE
        }
        JsonbJsonpathExistsStrategyNumber | JsonbJsonpathPredicateStrategyNumber => {
            if nkeys > 0 {
                debug_assert!(!jsp_ops.is_empty());
                let res = execute_jsp_gin_ops(jsp_ops, check, true);
                if res == GIN_TRUE {
                    GIN_MAYBE
                } else {
                    res
                }
            } else {
                GIN_MAYBE
            }
        }
        other => panic!("unrecognized strategy number: {other}"),
    }
}

/// gin_consistent_jsonb_path.
pub fn gin_consistent_jsonb_path(
    check: &[i8],
    strategy: u16,
    nkeys: usize,
    recheck: &mut bool,
    jsp_ops: &[JspGinOp],
) -> bool {
    match strategy {
        JsonbContainsStrategyNumber => {
            // Hash entries are lossy in structure and collisions; always
            // recheck, but missing keys are a certain miss.
            *recheck = true;
            check[..nkeys].iter().all(|&c| c != 0)
        }
        JsonbJsonpathExistsStrategyNumber | JsonbJsonpathPredicateStrategyNumber => {
            *recheck = true;
            if nkeys > 0 {
                debug_assert!(!jsp_ops.is_empty());
                execute_jsp_gin_ops(jsp_ops, check, false) != GIN_FALSE
            } else {
                true
            }
        }
        other => panic!("unrecognized strategy number: {other}"),
    }
}

/// gin_triconsistent_jsonb_path: never GIN_TRUE.
pub fn gin_triconsistent_jsonb_path(
    check: &[i8],
    strategy: u16,
    nkeys: usize,
    jsp_ops: &[JspGinOp],
) -> i8 {
    match strategy {
        JsonbContainsStrategyNumber => {
            for &c in &check[..nkeys] {
                if c == GIN_FALSE {
                    return GIN_FALSE;
                }
            }
            GIN_MAYBE
        }
        JsonbJsonpathExistsStrategyNumber | JsonbJsonpathPredicateStrategyNumber => {
            if nkeys > 0 {
                debug_assert!(!jsp_ops.is_empty());
                let res = execute_jsp_gin_ops(jsp_ops, check, true);
                if res == GIN_TRUE {
                    GIN_MAYBE
                } else {
                    res
                }
            } else {
                GIN_MAYBE
            }
        }
        other => panic!("unrecognized strategy number: {other}"),
    }
}
