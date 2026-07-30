//! jsonfuncs.c mutation slice: jsonb_concat (IteratorConcat), the delete
//! family, and setPath (jsonb_set/jsonb_insert/jsonb_delete_path).

extern crate alloc;

use crate::build::{convert_to_jsonb, item_to_jsonb_image, JsonbBuildState, JsonbValue};
use crate::container::*;
use crate::iter::{JsonbIterator, WjbToken};
use mcx::{Mcx, PgVec};
use stack_depth::check_stack_depth;
use types_error::{
    PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_INVALID_TEXT_REPRESENTATION,
    ERRCODE_NULL_VALUE_NOT_ALLOWED,
};

pub const JB_PATH_CREATE: u32 = 0x0001;
pub const JB_PATH_DELETE: u32 = 0x0002;
pub const JB_PATH_REPLACE: u32 = 0x0004;
pub const JB_PATH_INSERT_BEFORE: u32 = 0x0008;
pub const JB_PATH_INSERT_AFTER: u32 = 0x0010;
pub const JB_PATH_CREATE_OR_INSERT: u32 =
    JB_PATH_INSERT_BEFORE | JB_PATH_INSERT_AFTER | JB_PATH_CREATE;
pub const JB_PATH_FILL_GAPS: u32 = 0x0020;
pub const JB_PATH_CONSISTENT_POSITION: u32 = 0x0040;

#[track_caller]
#[cold]
#[inline(never)]
fn invalid_param(msg: &'static str) -> Box<PgError> {
    Box::new(PgError::error(msg).with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE))
}

/// C: pushJsonbValue over a JsonbParseState, including the jbvBinary
/// unpacking arms; `res` captures the finished root.
pub struct JsonbPush<'mcx> {
    mcx: Mcx<'mcx>,
    st: JsonbBuildState<'mcx>,
    res: Option<JsonbValue<'mcx>>,
}

impl<'mcx> JsonbPush<'mcx> {
    pub fn new(mcx: Mcx<'mcx>) -> PgResult<JsonbPush<'mcx>> {
        Ok(JsonbPush {
            mcx,
            st: JsonbBuildState::new(mcx)?,
            res: None,
        })
    }

    pub fn depth(&self) -> usize {
        self.st.depth()
    }

    pub fn in_array(&self) -> bool {
        self.st.in_array()
    }

    pub(crate) fn parts(
        &mut self,
    ) -> (&mut JsonbBuildState<'mcx>, &mut Option<JsonbValue<'mcx>>) {
        (&mut self.st, &mut self.res)
    }

    /// C: WJB_BEGIN_OBJECT + parseState->unique_keys (jsonb_build_object,
    /// jsonb_object_agg — their uniqueness check rides uniqueifyJsonbObject).
    pub fn push_object_start(&mut self, unique_keys: bool, skip_nulls: bool) -> PgResult<()> {
        self.st.begin_object_flags(unique_keys, skip_nulls)
    }

    /// C: pushJsonbValue with a NULL JsonbValue (container tokens).
    pub fn push_token(&mut self, tok: WjbToken) -> PgResult<()> {
        match tok {
            WjbToken::BeginArray => self.st.begin_array(false),
            WjbToken::BeginObject => self.st.begin_object(false),
            WjbToken::EndArray => {
                if let Some(v) = self.st.end_array()? {
                    self.res = Some(v);
                }
                Ok(())
            }
            WjbToken::EndObject => {
                if let Some(v) = self.st.end_object()? {
                    self.res = Some(v);
                }
                Ok(())
            }
            _ => panic!("push_token: value-carrying token without a value"),
        }
    }

    /// C: pushJsonbValue(state, tok, &v). Container begin/end tokens ignore
    /// the item except BeginArray's rawScalar flag.
    pub fn push(&mut self, tok: WjbToken, item: JsonbItem<'mcx>) -> PgResult<()> {
        match tok {
            WjbToken::BeginArray => {
                let raw = matches!(
                    item,
                    JsonbItem::Array {
                        raw_scalar: true,
                        ..
                    }
                );
                self.st.begin_array(raw)
            }
            WjbToken::BeginObject | WjbToken::EndArray | WjbToken::EndObject => {
                self.push_token(tok)
            }
            WjbToken::Key => {
                let JsonbItem::String(s) = item else {
                    panic!("object key is not a string");
                };
                self.st.push_key(s)
            }
            WjbToken::Value | WjbToken::Elem => self.push_value_item(tok, item),
            WjbToken::Done => panic!("push: WJB_DONE"),
        }
    }

    // C: the WJB_ELEM/WJB_VALUE arms of pushJsonbValue — jbvBinary unpacks,
    // raw-scalar binaries below the root push the bare scalar.
    fn push_value_item(&mut self, tok: WjbToken, item: JsonbItem<'mcx>) -> PgResult<()> {
        let JsonbItem::Binary(data) = item else {
            return self.push_scalar(tok, JsonbValue::from_item(item));
        };
        if container_is_scalar(data) && self.st.depth() > 0 {
            let v = get_ith_value(data, 0).expect("raw-scalar container has one element");
            return self.push_scalar(tok, JsonbValue::from_item(v));
        }
        let mut it = JsonbIterator::init(self.mcx, data)?;
        loop {
            let (t, v) = it.next(false);
            if t == WjbToken::Done {
                break;
            }
            self.push(t, v)?;
        }
        Ok(())
    }

    fn push_scalar(&mut self, tok: WjbToken, v: JsonbValue<'mcx>) -> PgResult<()> {
        match tok {
            WjbToken::Value => {
                self.st.push_value(v);
                Ok(())
            }
            WjbToken::Elem => self.st.push_elem(v),
            _ => unreachable!(),
        }
    }

    /// C: clone_parse_state (finalfn re-entrancy).
    pub fn clone_shallow(&self) -> PgResult<JsonbPush<'mcx>> {
        Ok(JsonbPush {
            mcx: self.mcx,
            st: self.st.clone_shallow()?,
            res: self.res,
        })
    }

    pub fn finish(self) -> JsonbValue<'mcx> {
        self.res.expect("push sequence did not close the root")
    }
}

fn input_image<'mcx>(mcx: Mcx<'mcx>, payload: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    item_to_jsonb_image(mcx, JsonbItem::Binary(payload))
}

/// C: jsonb_concat + IteratorConcat.
pub fn concat<'mcx>(
    mcx: Mcx<'mcx>,
    jb1: &'mcx [u8],
    jb2: &'mcx [u8],
) -> PgResult<PgVec<'mcx, u8>> {
    if container_is_object(jb1) == container_is_object(jb2) {
        if container_size(jb1) == 0 && !container_is_scalar(jb2) {
            return input_image(mcx, jb2);
        }
        if container_size(jb2) == 0 && !container_is_scalar(jb1) {
            return input_image(mcx, jb1);
        }
    }

    let mut it1 = JsonbIterator::init(mcx, jb1)?;
    let mut it2 = JsonbIterator::init(mcx, jb2)?;
    let mut ps = JsonbPush::new(mcx)?;

    let (rk1, _) = it1.next(false);
    let (rk2, _) = it2.next(false);

    if rk1 == WjbToken::BeginObject && rk2 == WjbToken::BeginObject {
        ps.push_token(WjbToken::BeginObject)?;
        loop {
            let (r, v) = it1.next(true);
            if r == WjbToken::EndObject {
                break;
            }
            ps.push(r, v)?;
        }
        loop {
            let (r, v) = it2.next(true);
            if r == WjbToken::Done {
                break;
            }
            if r == WjbToken::EndObject {
                ps.push_token(r)?;
            } else {
                ps.push(r, v)?;
            }
        }
    } else if rk1 == WjbToken::BeginArray && rk2 == WjbToken::BeginArray {
        ps.push_token(WjbToken::BeginArray)?;
        loop {
            let (r, v) = it1.next(true);
            if r == WjbToken::EndArray {
                break;
            }
            ps.push(r, v)?;
        }
        loop {
            let (r, v) = it2.next(true);
            if r == WjbToken::EndArray {
                break;
            }
            ps.push(WjbToken::Elem, v)?;
        }
        ps.push_token(WjbToken::EndArray)?;
    } else if rk1 == WjbToken::BeginObject {
        // object || array
        ps.push_token(WjbToken::BeginArray)?;
        ps.push_token(WjbToken::BeginObject)?;
        loop {
            let (r, v) = it1.next(true);
            if r == WjbToken::Done {
                break;
            }
            if r == WjbToken::EndObject {
                ps.push_token(r)?;
            } else {
                ps.push(r, v)?;
            }
        }
        loop {
            let (r, v) = it2.next(true);
            if r == WjbToken::Done {
                break;
            }
            if r == WjbToken::EndArray {
                ps.push_token(r)?;
            } else {
                ps.push(r, v)?;
            }
        }
    } else {
        // array || object
        ps.push_token(WjbToken::BeginArray)?;
        loop {
            let (r, v) = it1.next(true);
            if r == WjbToken::EndArray {
                break;
            }
            ps.push(r, v)?;
        }
        ps.push_token(WjbToken::BeginObject)?;
        loop {
            let (r, v) = it2.next(true);
            if r == WjbToken::Done {
                break;
            }
            if r == WjbToken::EndObject {
                ps.push_token(r)?;
            } else {
                ps.push(r, v)?;
            }
        }
        ps.push_token(WjbToken::EndArray)?;
    }

    convert_to_jsonb(mcx, &ps.finish())
}

fn item_matches_key(tok: WjbToken, v: &JsonbItem<'_>, key: &[u8]) -> bool {
    matches!(tok, WjbToken::Elem | WjbToken::Key)
        && matches!(v, JsonbItem::String(s) if *s == key)
}

/// C: jsonb_delete (jsonb, text).
pub fn delete_key<'mcx>(
    mcx: Mcx<'mcx>,
    payload: &'mcx [u8],
    key: &[u8],
) -> PgResult<PgVec<'mcx, u8>> {
    if container_is_scalar(payload) {
        return Err(invalid_param("cannot delete from scalar"));
    }
    if container_size(payload) == 0 {
        return input_image(mcx, payload);
    }

    let mut it = JsonbIterator::init(mcx, payload)?;
    let mut ps = JsonbPush::new(mcx)?;
    let mut skip_nested = false;
    loop {
        let (r, v) = it.next(skip_nested);
        if r == WjbToken::Done {
            break;
        }
        skip_nested = true;
        if item_matches_key(r, &v, key) {
            if r == WjbToken::Key {
                it.next(true);
            }
            continue;
        }
        ps.push(r, v)?;
    }
    convert_to_jsonb(mcx, &ps.finish())
}

/// C: jsonb_delete_array (jsonb, text[]); `keys` excludes SQL NULL elements.
pub fn delete_keys<'mcx>(
    mcx: Mcx<'mcx>,
    payload: &'mcx [u8],
    keys: &[&[u8]],
) -> PgResult<PgVec<'mcx, u8>> {
    if container_is_scalar(payload) {
        return Err(invalid_param("cannot delete from scalar"));
    }
    if container_size(payload) == 0 || keys.is_empty() {
        return input_image(mcx, payload);
    }

    let mut it = JsonbIterator::init(mcx, payload)?;
    let mut ps = JsonbPush::new(mcx)?;
    let mut skip_nested = false;
    loop {
        let (r, v) = it.next(skip_nested);
        if r == WjbToken::Done {
            break;
        }
        skip_nested = true;
        if matches!(r, WjbToken::Elem | WjbToken::Key) {
            if let JsonbItem::String(s) = v {
                if keys.iter().any(|k| *k == s) {
                    if r == WjbToken::Key {
                        it.next(true);
                    }
                    continue;
                }
            }
        }
        ps.push(r, v)?;
    }
    convert_to_jsonb(mcx, &ps.finish())
}

/// C: jsonb_delete_idx (jsonb, int4).
pub fn delete_idx<'mcx>(
    mcx: Mcx<'mcx>,
    payload: &'mcx [u8],
    idx: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    if container_is_scalar(payload) {
        return Err(invalid_param("cannot delete from scalar"));
    }
    if container_is_object(payload) {
        return Err(invalid_param("cannot delete from object using integer index"));
    }
    if container_size(payload) == 0 {
        return input_image(mcx, payload);
    }

    let mut it = JsonbIterator::init(mcx, payload)?;
    let (r, v) = it.next(false);
    debug_assert_eq!(r, WjbToken::BeginArray);
    let JsonbItem::Array { n_elems: n, .. } = v else {
        unreachable!()
    };

    let idx = if idx < 0 {
        if idx.unsigned_abs() > n {
            n
        } else {
            n - idx.unsigned_abs()
        }
    } else {
        idx as u32
    };
    if idx >= n {
        return input_image(mcx, payload);
    }

    let mut ps = JsonbPush::new(mcx)?;
    ps.push_token(WjbToken::BeginArray)?;
    let mut i = 0u32;
    loop {
        let (r, v) = it.next(true);
        if r == WjbToken::Done {
            break;
        }
        if r == WjbToken::Elem {
            let cur = i;
            i += 1;
            if cur == idx {
                continue;
            }
        }
        ps.push(r, v)?;
    }
    convert_to_jsonb(mcx, &ps.finish())
}

#[track_caller]
#[cold]
#[inline(never)]
fn null_path_elem(level: usize) -> Box<PgError> {
    Box::new(
        PgError::error(alloc::format!(
            "path element at position {} is null",
            level + 1
        ))
        .with_sqlstate(ERRCODE_NULL_VALUE_NOT_ALLOWED),
    )
}

// C: setPathArray's strtoint of the path element.
fn path_elem_as_index(elem: &[u8], level: usize) -> PgResult<i32> {
    let bad = || -> Box<PgError> {
        Box::new(
            PgError::error(alloc::format!(
                "path element at position {} is not an integer: \"{}\"",
                level + 1,
                String::from_utf8_lossy(elem)
            ))
            .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION),
        )
    };
    let s = core::str::from_utf8(elem).map_err(|_| bad())?;
    let t = s.trim_ascii_start();
    if t.is_empty() {
        return Err(bad());
    }
    let v: i64 = t.parse().map_err(|_| bad())?;
    if v > i32::MAX as i64 || v < i32::MIN as i64 {
        return Err(bad());
    }
    Ok(v as i32)
}

pub struct SetPathArgs<'p, 'mcx> {
    pub path: &'p [Option<&'mcx [u8]>],
    pub newval: Option<JsonbItem<'mcx>>,
    pub op_type: u32,
}

/// C: setPath — the shared walker behind jsonb_set/jsonb_insert/
/// jsonb_delete_path (and subscripting's FILL_GAPS lanes).
pub fn set_path<'mcx>(
    mcx: Mcx<'mcx>,
    payload: &'mcx [u8],
    args: &SetPathArgs<'_, 'mcx>,
) -> PgResult<PgVec<'mcx, u8>> {
    let mut it = JsonbIterator::init(mcx, payload)?;
    let mut ps = JsonbPush::new(mcx)?;
    set_path_rec(&mut it, &mut ps, args, 0)?;
    convert_to_jsonb(mcx, &ps.finish())
}

fn set_path_rec<'mcx>(
    it: &mut JsonbIterator<'mcx, 'mcx>,
    ps: &mut JsonbPush<'mcx>,
    args: &SetPathArgs<'_, 'mcx>,
    level: usize,
) -> PgResult<()> {
    check_stack_depth()?;

    if args.path[level].is_none() {
        return Err(null_path_elem(level));
    }

    let (r, v) = it.next(false);
    match r {
        WjbToken::BeginArray => {
            let JsonbItem::Array {
                n_elems,
                raw_scalar,
            } = v
            else {
                unreachable!()
            };
            if args.op_type & JB_PATH_FILL_GAPS != 0 && level <= args.path.len() - 1 && raw_scalar
            {
                return Err(cannot_replace_scalar());
            }
            ps.push(r, v)?;
            set_path_array(it, ps, args, level, n_elems)?;
            let (r, _) = it.next(false);
            debug_assert_eq!(r, WjbToken::EndArray);
            ps.push_token(r)
        }
        WjbToken::BeginObject => {
            let JsonbItem::Object { n_pairs } = v else {
                unreachable!()
            };
            ps.push_token(r)?;
            set_path_object(it, ps, args, level, n_pairs)?;
            let (r, _) = it.next(true);
            debug_assert_eq!(r, WjbToken::EndObject);
            ps.push_token(r)
        }
        WjbToken::Elem | WjbToken::Value => {
            if args.op_type & JB_PATH_FILL_GAPS != 0 && level <= args.path.len() - 1 {
                return Err(cannot_replace_scalar());
            }
            ps.push(r, v)
        }
        _ => panic!("unrecognized iterator result: {r:?}"),
    }
}

#[track_caller]
#[cold]
#[inline(never)]
fn cannot_replace_scalar() -> Box<PgError> {
    Box::new(
        PgError::error("cannot replace existing key")
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
            .with_detail("The path assumes key is a composite object, but it is a scalar value."),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn cannot_replace_existing_key() -> Box<PgError> {
    Box::new(
        PgError::error("cannot replace existing key")
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
            .with_hint("Try using the function jsonb_set to replace key value."),
    )
}

fn push_newval<'mcx>(
    ps: &mut JsonbPush<'mcx>,
    tok: WjbToken,
    args: &SetPathArgs<'_, 'mcx>,
) -> PgResult<()> {
    ps.push(tok, args.newval.expect("mutation op requires a new value"))
}

/// C: setPathObject.
fn set_path_object<'mcx>(
    it: &mut JsonbIterator<'mcx, 'mcx>,
    ps: &mut JsonbPush<'mcx>,
    args: &SetPathArgs<'_, 'mcx>,
    level: usize,
    npairs: u32,
) -> PgResult<()> {
    let path_len = args.path.len();
    let mut done = false;
    let pathelem: Option<&[u8]> = if level >= path_len || args.path[level].is_none() {
        done = true;
        None
    } else {
        args.path[level]
    };

    if npairs == 0
        && args.op_type & JB_PATH_CREATE_OR_INSERT != 0
        && level == path_len - 1
        && !done
    {
        let key = pathelem.expect("checked non-null above");
        ps.push(WjbToken::Key, JsonbItem::String(key))?;
        push_newval(ps, WjbToken::Value, args)?;
    }

    for i in 0..npairs {
        let (r, k) = it.next(true);
        debug_assert_eq!(r, WjbToken::Key);
        let key_matches =
            !done && matches!((k, pathelem), (JsonbItem::String(s), Some(p)) if s == p);
        if key_matches {
            done = true;
            if level == path_len - 1 {
                if args.op_type & (JB_PATH_INSERT_BEFORE | JB_PATH_INSERT_AFTER) != 0 {
                    return Err(cannot_replace_existing_key());
                }
                it.next(true); // skip value
                if args.op_type & JB_PATH_DELETE == 0 {
                    ps.push(WjbToken::Key, k)?;
                    push_newval(ps, WjbToken::Value, args)?;
                }
            } else {
                ps.push(r, k)?;
                set_path_rec(it, ps, args, level + 1)?;
            }
        } else {
            if args.op_type & JB_PATH_CREATE_OR_INSERT != 0
                && !done
                && level == path_len - 1
                && i == npairs - 1
            {
                let key = pathelem.expect("!done implies non-null path element");
                ps.push(WjbToken::Key, JsonbItem::String(key))?;
                push_newval(ps, WjbToken::Value, args)?;
            }
            ps.push(r, k)?;
            copy_subtree(it, ps)?;
        }
    }

    if !done && args.op_type & JB_PATH_FILL_GAPS != 0 && level < path_len - 1 {
        let key = pathelem.expect("!done implies non-null path element");
        ps.push(WjbToken::Key, JsonbItem::String(key))?;
        push_path(ps, args, level)?;
    }
    Ok(())
}

// C: the "(void) pushJsonbValue(st, r, ...); if begin { walking_level loop }"
// verbatim-copy idiom shared by both walkers.
fn copy_subtree<'mcx>(
    it: &mut JsonbIterator<'mcx, 'mcx>,
    ps: &mut JsonbPush<'mcx>,
) -> PgResult<()> {
    let (r, v) = it.next(false);
    ps.push(r, v)?;
    if matches!(r, WjbToken::BeginArray | WjbToken::BeginObject) {
        let mut walking_level = 1u32;
        while walking_level != 0 {
            let (r, v) = it.next(false);
            match r {
                WjbToken::BeginArray | WjbToken::BeginObject => walking_level += 1,
                WjbToken::EndArray | WjbToken::EndObject => walking_level -= 1,
                _ => {}
            }
            ps.push(r, v)?;
        }
    }
    Ok(())
}

/// C: setPathArray.
fn set_path_array<'mcx>(
    it: &mut JsonbIterator<'mcx, 'mcx>,
    ps: &mut JsonbPush<'mcx>,
    args: &SetPathArgs<'_, 'mcx>,
    level: usize,
    nelems: u32,
) -> PgResult<()> {
    let path_len = args.path.len();
    let nelems_i = nelems as i32;

    let mut idx: i32 = match (level < path_len, args.path.get(level).copied().flatten()) {
        (true, Some(elem)) => path_elem_as_index(elem, level)?,
        _ => nelems_i,
    };

    if idx < 0 {
        if idx.unsigned_abs() > nelems {
            if args.op_type & JB_PATH_CONSISTENT_POSITION != 0 {
                return Err(Box::new(
                    PgError::error(alloc::format!(
                        "path element at position {} is out of range: {}",
                        level + 1,
                        idx
                    ))
                    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
                ));
            }
            idx = i32::MIN;
        } else {
            idx = nelems_i + idx;
        }
    }

    if args.op_type & JB_PATH_FILL_GAPS == 0 && idx > 0 && idx > nelems_i {
        idx = nelems_i;
    }

    let mut done = false;

    if (idx == i32::MIN || nelems == 0)
        && level == path_len - 1
        && args.op_type & JB_PATH_CREATE_OR_INSERT != 0
    {
        if args.op_type & JB_PATH_FILL_GAPS != 0 && nelems == 0 && idx > 0 {
            push_null_elements(ps, idx as usize)?;
        }
        push_newval(ps, WjbToken::Elem, args)?;
        done = true;
    }

    for i in 0..nelems_i {
        if i == idx && level < path_len {
            done = true;
            if level == path_len - 1 {
                let (r, v) = it.next(true); // skip
                if args.op_type & (JB_PATH_INSERT_BEFORE | JB_PATH_CREATE) != 0 {
                    push_newval(ps, WjbToken::Elem, args)?;
                }
                if args.op_type & (JB_PATH_INSERT_AFTER | JB_PATH_INSERT_BEFORE) != 0 {
                    ps.push(r, v)?;
                }
                if args.op_type & (JB_PATH_INSERT_AFTER | JB_PATH_REPLACE) != 0 {
                    push_newval(ps, WjbToken::Elem, args)?;
                }
            } else {
                set_path_rec(it, ps, args, level + 1)?;
            }
        } else {
            copy_subtree(it, ps)?;
        }
    }

    if args.op_type & JB_PATH_CREATE_OR_INSERT != 0 && !done && level == path_len - 1 {
        if args.op_type & JB_PATH_FILL_GAPS != 0 && idx > nelems_i {
            push_null_elements(ps, (idx - nelems_i) as usize)?;
        }
        push_newval(ps, WjbToken::Elem, args)?;
        done = true;
    }

    if !done && args.op_type & JB_PATH_FILL_GAPS != 0 && level < path_len - 1 {
        if idx > 0 {
            push_null_elements(ps, (idx - nelems_i) as usize)?;
        }
        push_path(ps, args, level)?;
    }
    Ok(())
}

fn push_null_elements(ps: &mut JsonbPush<'_>, num: usize) -> PgResult<()> {
    for _ in 0..num {
        ps.push(WjbToken::Elem, JsonbItem::Null)?;
    }
    Ok(())
}

/// C: push_path — build the chain of empty objects/arrays for FILL_GAPS
/// missing-path creation.
fn push_path<'mcx>(
    ps: &mut JsonbPush<'mcx>,
    args: &SetPathArgs<'_, 'mcx>,
    level: usize,
) -> PgResult<()> {
    let path_len = args.path.len();
    // true = array level (C tpath jbvArray).
    let mut tpath: PgVec<'_, bool> = mcx::vec_with_capacity_in(ps.mcx, path_len - level)?;
    tpath.resize(path_len - level, false);

    for i in level + 1..path_len {
        let Some(elem) = args.path[i] else {
            break;
        };
        match path_elem_as_index(elem, i) {
            Ok(lindex) => {
                ps.push_token(WjbToken::BeginArray)?;
                push_null_elements(ps, lindex.max(0) as usize)?;
                tpath[i - level] = true;
            }
            Err(_) => {
                ps.push_token(WjbToken::BeginObject)?;
                ps.push(WjbToken::Key, JsonbItem::String(elem))?;
                tpath[i - level] = false;
            }
        }
    }

    if tpath[path_len - level - 1] {
        push_newval(ps, WjbToken::Elem, args)?;
    } else {
        push_newval(ps, WjbToken::Value, args)?;
    }

    for i in (level + 1..path_len).rev() {
        if args.path[i].is_none() {
            break;
        }
        if tpath[i - level] {
            ps.push_token(WjbToken::EndArray)?;
        } else {
            ps.push_token(WjbToken::EndObject)?;
        }
    }
    Ok(())
}

/// C: jsonb_strip_nulls. A scalar root passes through unchanged.
pub fn strip_nulls<'mcx>(
    mcx: Mcx<'mcx>,
    payload: &'mcx [u8],
    strip_in_arrays: bool,
) -> PgResult<PgVec<'mcx, u8>> {
    if container_is_scalar(payload) {
        return input_image(mcx, payload);
    }
    let mut it = JsonbIterator::init(mcx, payload)?;
    let mut ps = JsonbPush::new(mcx)?;
    // C last_was_key: a key is held back until its value is known non-null.
    let mut pending_key: Option<JsonbItem<'_>> = None;
    loop {
        let (tok, v) = it.next(false);
        if tok == WjbToken::Done {
            break;
        }
        if tok == WjbToken::Key {
            debug_assert!(pending_key.is_none());
            pending_key = Some(v);
            continue;
        }
        if let Some(k) = pending_key.take() {
            if tok == WjbToken::Value && matches!(v, JsonbItem::Null) {
                continue;
            }
            ps.push(WjbToken::Key, k)?;
        }
        if strip_in_arrays && tok == WjbToken::Elem && matches!(v, JsonbItem::Null) {
            continue;
        }
        match tok {
            WjbToken::Value | WjbToken::Elem => ps.push(tok, v)?,
            _ => ps.push_token(tok)?,
        }
    }
    convert_to_jsonb(mcx, &ps.finish())
}
