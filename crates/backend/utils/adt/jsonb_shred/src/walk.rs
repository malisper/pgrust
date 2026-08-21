//! Path walker (memo §2.2/§4): a single linear walk over the canonical
//! JEntry tree, in place — never text. jsonb_in already deduped (last-wins)
//! and sorted keys, so positions are emitted in canonical order and each
//! (row, path) occurs at most once.

use adt_jsonb::container::{
    container_is_object, container_is_scalar, container_size, fill_item, get_jsonb_length,
    get_jsonb_offset, JsonbItem,
};

/// One emitted position of a row's walk. Paths are borrowed key slices into
/// the document.
pub enum PathValue<'a> {
    /// A scalar leaf (String / Numeric / Bool / Null) at an object-key chain
    /// of depth <= max_depth.
    Scalar(JsonbItem<'a>),
    /// A subtree routed to the residual whole (memo §2.10 v1 scoping): an
    /// array, an empty object, an object at the depth budget, or an object
    /// sitting AT an elected path (the §2.4 non-scalar exception). The slice
    /// is the nested container window.
    Subtree(&'a [u8]),
    /// A non-object root document (array root or raw-scalar root), emitted
    /// with the empty path: the whole payload rides the residual verbatim.
    Root,
}

/// Walk one document payload (container bytes, varlena header already
/// stripped — detoast is the caller's paid cost per the A6a contract).
///
/// `stop_at` gates recursion into non-empty objects: when it returns true
/// for a key chain, the object at that chain is emitted as a
/// [`PathValue::Subtree`] instead of being descended into. Election passes
/// `|_| false` (count every reachable position); the shredder passes the
/// elected-path predicate so a non-scalar value at an elected path becomes
/// one whole exception (memo §2.4), never a scatter of descendants.
///
/// Every emitted path has depth in 1..=max_depth ([`PathValue::Root`] is the
/// depth-0 exception), and per row each path is emitted at most once.
pub fn walk_row<'a>(
    payload: &'a [u8],
    max_depth: u8,
    stop_at: &mut dyn FnMut(&[&'a [u8]]) -> bool,
    emit: &mut dyn FnMut(&[&'a [u8]], PathValue<'a>),
) {
    if !container_is_object(payload) || container_is_scalar(payload) {
        // Array root, raw-scalar root. (A scalar root is a raw-scalar
        // pseudo-array: JB_FARRAY|JB_FSCALAR.)
        emit(&[], PathValue::Root);
        return;
    }
    let mut prefix: Vec<&'a [u8]> = Vec::with_capacity(max_depth as usize);
    walk_object(payload, max_depth, &mut prefix, stop_at, emit);
}

fn walk_object<'a>(
    c: &'a [u8],
    max_depth: u8,
    prefix: &mut Vec<&'a [u8]>,
    stop_at: &mut dyn FnMut(&[&'a [u8]]) -> bool,
    emit: &mut dyn FnMut(&[&'a [u8]], PathValue<'a>),
) {
    debug_assert!(container_is_object(c));
    let n = container_size(c);
    let base_off = 4 + 8 * n;
    for i in 0..n {
        let key = {
            let start = (base_off + get_jsonb_offset(c, i)) as usize;
            let len = get_jsonb_length(c, i) as usize;
            &c[start..start + len]
        };
        let vi = i + n;
        let item = fill_item(c, vi, base_off, get_jsonb_offset(c, vi));
        prefix.push(key);
        let depth = prefix.len() as u8;
        debug_assert!(depth <= max_depth);
        match item {
            JsonbItem::String(_)
            | JsonbItem::Numeric(_)
            | JsonbItem::Bool(_)
            | JsonbItem::Null => emit(prefix, PathValue::Scalar(item)),
            JsonbItem::Binary(child) => {
                let recursable = container_is_object(child)
                    && container_size(child) > 0
                    && depth < max_depth
                    && !stop_at(prefix);
                if recursable {
                    walk_object(child, max_depth, prefix, stop_at, emit);
                } else {
                    // Array, empty object, depth-capped object, or an
                    // object at an elected path.
                    emit(prefix, PathValue::Subtree(child));
                }
            }
            JsonbItem::Array { .. } | JsonbItem::Object { .. } => {
                unreachable!("fill_item never yields begin tokens")
            }
        }
        prefix.pop();
    }
}
