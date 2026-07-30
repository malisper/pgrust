//! Build-side JsonbValue tree + convertToJsonb serialization. The serialized
//! image is on-disk data: every byte (JEntry stride offsets, alignment
//! padding, embedded numeric varlenas) must match C's convertToJsonb exactly.

extern crate alloc;

use core::alloc::Layout;
use core::marker::PhantomData;

use crate::container::*;
use mcx::{Allocator, Mcx, PgVec};
use stack_depth::check_stack_depth;
use types_error::{
    PgError, PgResult, ERRCODE_DUPLICATE_JSON_OBJECT_KEY_VALUE, ERRCODE_PROGRAM_LIMIT_EXCEEDED,
};

// C: Min(MaxAllocSize/sizeof(JsonbValue|JsonbPair), JB_CMASK) with C's 32/72
// byte struct sizes — the values appear verbatim in user-facing errors.
const JSONB_MAX_ELEMS: usize = 33554431;
const JSONB_MAX_PAIRS: usize = 14913080;

/// palloc/repalloc-grown array in the arena; no Drop by construction (the
/// arena reclaims wholesale), so it can live inside arena-resident values.
pub struct ArenaVec<'mcx, T> {
    ptr: *mut T,
    len: u32,
    cap: u32,
    _life: PhantomData<&'mcx ()>,
}

impl<T> Clone for ArenaVec<'_, T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T> Copy for ArenaVec<'_, T> {}

impl<'mcx, T: Copy> ArenaVec<'mcx, T> {
    const NO_DROP: () = assert!(!core::mem::needs_drop::<T>());

    pub fn with_capacity(mcx: Mcx<'mcx>, cap: usize) -> PgResult<ArenaVec<'mcx, T>> {
        #[allow(clippy::let_unit_value)]
        let _ = Self::NO_DROP;
        let layout = Layout::array::<T>(cap).expect("ArenaVec layout");
        let ptr = mcx
            .allocate(layout)
            .map_err(|_| mcx.oom(layout.size()))?
            .cast::<T>()
            .as_ptr();
        Ok(ArenaVec {
            ptr,
            len: 0,
            cap: cap as u32,
            _life: PhantomData,
        })
    }

    pub fn push(&mut self, mcx: Mcx<'mcx>, v: T) -> PgResult<()> {
        if self.len == self.cap {
            // C: repalloc doubling.
            let new_cap = (self.cap as usize).max(1) * 2;
            let layout = Layout::array::<T>(new_cap).expect("ArenaVec layout");
            let new_ptr = mcx
                .allocate(layout)
                .map_err(|_| mcx.oom(layout.size()))?
                .cast::<T>()
                .as_ptr();
            // SAFETY: both regions live, disjoint, len elements initialized.
            unsafe {
                core::ptr::copy_nonoverlapping(self.ptr, new_ptr, self.len as usize);
            }
            self.ptr = new_ptr;
            self.cap = new_cap as u32;
        }
        // SAFETY: len < cap; slot is within the allocation.
        unsafe {
            self.ptr.add(self.len as usize).write(v);
        }
        self.len += 1;
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.len as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn truncate(&mut self, n: usize) {
        debug_assert!(n <= self.len as usize);
        self.len = n as u32;
    }

    pub fn as_slice(&self) -> &[T] {
        // SAFETY: len elements initialized at ptr.
        unsafe { core::slice::from_raw_parts(self.ptr, self.len as usize) }
    }

    pub fn as_mut_slice(&mut self) -> &mut [T] {
        // SAFETY: len elements initialized at ptr; &mut self gives uniqueness.
        unsafe { core::slice::from_raw_parts_mut(self.ptr, self.len as usize) }
    }
}

/// Build-side C JsonbValue: arena-slice leaves, Copy, no drop glue.
#[derive(Clone, Copy)]
pub enum JsonbValue<'mcx> {
    Null,
    String(&'mcx [u8]),
    Numeric(&'mcx [u8]),
    Bool(bool),
    Array {
        elems: ArenaVec<'mcx, JsonbValue<'mcx>>,
        raw_scalar: bool,
    },
    Object {
        pairs: ArenaVec<'mcx, JsonbPair<'mcx>>,
    },
}

#[derive(Clone, Copy)]
pub struct JsonbPair<'mcx> {
    pub key: &'mcx [u8],
    pub value: JsonbValue<'mcx>,
    pub order: u32,
}

impl<'mcx> JsonbValue<'mcx> {
    pub fn is_scalar(&self) -> bool {
        matches!(
            self,
            JsonbValue::Null
                | JsonbValue::String(_)
                | JsonbValue::Numeric(_)
                | JsonbValue::Bool(_)
        )
    }

    pub fn from_item(item: JsonbItem<'mcx>) -> JsonbValue<'mcx> {
        match item {
            JsonbItem::Null => JsonbValue::Null,
            JsonbItem::String(s) => JsonbValue::String(s),
            JsonbItem::Numeric(n) => JsonbValue::Numeric(n),
            JsonbItem::Bool(b) => JsonbValue::Bool(b),
            _ => panic!("from_item: not a scalar jsonb item"),
        }
    }
}

#[derive(Clone, Copy)]
struct Frame<'mcx> {
    val: JsonbValue<'mcx>,
    unique_keys: bool,
    skip_nulls: bool,
}

/// C: JsonbParseState + pushJsonbValueScalar sequencing.
pub struct JsonbBuildState<'mcx> {
    mcx: Mcx<'mcx>,
    stack: PgVec<'mcx, Frame<'mcx>>,
}

#[track_caller]
#[cold]
#[inline(never)]
fn limit_error(msg: alloc::string::String) -> Box<PgError> {
    Box::new(PgError::error(msg).with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED))
}

impl<'mcx> JsonbBuildState<'mcx> {
    pub fn new(mcx: Mcx<'mcx>) -> PgResult<JsonbBuildState<'mcx>> {
        Ok(JsonbBuildState {
            mcx,
            stack: mcx::vec_with_capacity_in(mcx, 4)?,
        })
    }

    pub fn depth(&self) -> usize {
        self.stack.len()
    }

    /// C: clone_parse_state — shallow: frames alias the ArenaVec buffers, so
    /// a repeated finalfn re-runs the idempotent uniqueify exactly as C.
    /// The clone lives in the same arena (C uses the calling context; one
    /// small stack per finalization is retained until the agg ends).
    pub fn clone_shallow(&self) -> PgResult<JsonbBuildState<'mcx>> {
        let mut stack: PgVec<'mcx, Frame<'mcx>> =
            mcx::vec_with_capacity_in(self.mcx, self.stack.len().max(1))?;
        for f in self.stack.iter() {
            stack.push(*f);
        }
        Ok(JsonbBuildState { mcx: self.mcx, stack })
    }

    pub fn in_array(&self) -> bool {
        matches!(
            self.stack.last().map(|f| &f.val),
            Some(JsonbValue::Array { .. })
        )
    }

    pub fn begin_array(&mut self, raw_scalar: bool) -> PgResult<()> {
        let elems = ArenaVec::with_capacity(self.mcx, if raw_scalar { 1 } else { 4 })?;
        self.stack.push(Frame {
            val: JsonbValue::Array { elems, raw_scalar },
            unique_keys: false,
            skip_nulls: false,
        });
        Ok(())
    }

    pub fn begin_object(&mut self, unique_keys: bool) -> PgResult<()> {
        self.begin_object_flags(unique_keys, false)
    }

    /// C: WJB_BEGIN_OBJECT + parseState->{unique_keys,skip_nulls}.
    pub fn begin_object_flags(&mut self, unique_keys: bool, skip_nulls: bool) -> PgResult<()> {
        let pairs = ArenaVec::with_capacity(self.mcx, 4)?;
        self.stack.push(Frame {
            val: JsonbValue::Object { pairs },
            unique_keys,
            skip_nulls,
        });
        Ok(())
    }

    /// C: appendKey.
    pub fn push_key(&mut self, key: &'mcx [u8]) -> PgResult<()> {
        let mcx = self.mcx;
        let frame = self.stack.last_mut().expect("key outside object");
        let JsonbValue::Object { pairs } = &mut frame.val else {
            panic!("key outside object");
        };
        if pairs.len() >= JSONB_MAX_PAIRS {
            return Err(limit_error(alloc::format!(
                "number of jsonb object pairs exceeds the maximum allowed ({JSONB_MAX_PAIRS})"
            )));
        }
        let order = pairs.len() as u32;
        pairs.push(
            mcx,
            JsonbPair {
                key,
                value: JsonbValue::Null,
                order,
            },
        )
    }

    /// C: appendValue.
    pub fn push_value(&mut self, value: JsonbValue<'mcx>) {
        let frame = self.stack.last_mut().expect("value outside object");
        let JsonbValue::Object { pairs } = &mut frame.val else {
            panic!("value outside object");
        };
        pairs
            .as_mut_slice()
            .last_mut()
            .expect("value without key")
            .value = value;
    }

    /// C: appendElement.
    pub fn push_elem(&mut self, value: JsonbValue<'mcx>) -> PgResult<()> {
        let mcx = self.mcx;
        let frame = self.stack.last_mut().expect("element outside array");
        let JsonbValue::Array { elems, .. } = &mut frame.val else {
            panic!("element outside array");
        };
        if elems.len() >= JSONB_MAX_ELEMS {
            return Err(limit_error(alloc::format!(
                "number of jsonb array elements exceeds the maximum allowed ({JSONB_MAX_ELEMS})"
            )));
        }
        elems.push(mcx, value)
    }

    /// C: WJB_END_OBJECT arm — uniqueify + pop; Some = finished tree.
    pub fn end_object(&mut self) -> PgResult<Option<JsonbValue<'mcx>>> {
        let mut frame = self.stack.pop().expect("end_object without begin");
        if let JsonbValue::Object { pairs } = &mut frame.val {
            uniqueify_object(pairs, frame.unique_keys, frame.skip_nulls)?;
        } else {
            panic!("end_object on non-object");
        }
        self.append_to_parent(frame.val)
    }

    pub fn end_array(&mut self) -> PgResult<Option<JsonbValue<'mcx>>> {
        let frame = self.stack.pop().expect("end_array without begin");
        debug_assert!(matches!(frame.val, JsonbValue::Array { .. }));
        self.append_to_parent(frame.val)
    }

    fn append_to_parent(&mut self, val: JsonbValue<'mcx>) -> PgResult<Option<JsonbValue<'mcx>>> {
        match self.stack.last() {
            None => Ok(Some(val)),
            Some(parent) => {
                match &parent.val {
                    JsonbValue::Array { .. } => self.push_elem(val)?,
                    JsonbValue::Object { .. } => self.push_value(val),
                    _ => panic!("invalid jsonb container type"),
                }
                Ok(None)
            }
        }
    }
}

/// C: uniqueifyJsonbObject; the reversed `order` tiebreak makes keep-first
/// mean last-observed-wins.
fn uniqueify_object(
    pairs: &mut ArenaVec<'_, JsonbPair<'_>>,
    unique_keys: bool,
    skip_nulls: bool,
) -> PgResult<()> {
    let mut has_non_uniq = false;
    if pairs.len() > 1 {
        // Strict total order (order tiebreak): unstable sort == C qsort_arg.
        pairs
            .as_mut_slice()
            .sort_unstable_by(|a, b| {
                length_compare_jsonb_string(a.key, b.key).then_with(|| b.order.cmp(&a.order))
            });
        for w in pairs.as_slice().windows(2) {
            if length_compare_jsonb_string(w[0].key, w[1].key) == core::cmp::Ordering::Equal {
                has_non_uniq = true;
                break;
            }
        }
    }
    if has_non_uniq && unique_keys {
        return Err(Box::new(
            PgError::error("duplicate JSON object key value")
                .with_sqlstate(ERRCODE_DUPLICATE_JSON_OBJECT_KEY_VALUE),
        ));
    }
    if has_non_uniq || skip_nulls {
        let s = pairs.as_mut_slice();
        let mut start = 0usize;
        while skip_nulls && start < s.len() && matches!(s[start].value, JsonbValue::Null) {
            start += 1;
        }
        if start == s.len() {
            pairs.truncate(0);
            return Ok(());
        }
        let mut res = start;
        for ptr in (start + 1)..s.len() {
            if length_compare_jsonb_string(s[ptr].key, s[res].key) != core::cmp::Ordering::Equal
                && (!skip_nulls || !matches!(s[ptr].value, JsonbValue::Null))
            {
                res += 1;
                if ptr != res {
                    s[res] = s[ptr];
                }
            }
        }
        if start > 0 {
            for k in start..=res {
                s[k - start] = s[k];
            }
        }
        pairs.truncate(res + 1 - start);
    }
    Ok(())
}

// C: the convertToJsonb StringInfo; reserved space zero-filled (JEntry slots
// are back-patched, pad bytes must be zero on disk).
struct ConvertBuffer<'mcx> {
    data: PgVec<'mcx, u8>,
}

impl ConvertBuffer<'_> {
    fn reserve(&mut self, len: usize) -> usize {
        let offset = self.data.len();
        self.data.resize(offset + len, 0);
        offset
    }

    fn append(&mut self, bytes: &[u8]) -> PgResult<()> {
        mcx::vec_append_bytes(&mut self.data, bytes)
    }

    fn copy_to(&mut self, offset: usize, bytes: &[u8]) {
        self.data[offset..offset + bytes.len()].copy_from_slice(bytes);
    }

    fn pad_to_int(&mut self) -> usize {
        let padlen = intalign(self.data.len() as u32) as usize - self.data.len();
        self.reserve(padlen);
        padlen
    }
}

/// C: convertToJsonb — returns the full 4B-header varlena image.
pub fn convert_to_jsonb<'mcx>(mcx: Mcx<'mcx>, val: &JsonbValue<'_>) -> PgResult<PgVec<'mcx, u8>> {
    let mut buffer = ConvertBuffer {
        data: mcx::vec_with_capacity_in(mcx, 128)?,
    };
    buffer.reserve(4);
    let mut header = 0;
    convert_value(&mut buffer, &mut header, val, 0)?;
    let len = buffer.data.len();
    buffer.copy_to(0, &((len as u32) << 2).to_ne_bytes());
    Ok(buffer.data)
}

fn convert_value(
    buffer: &mut ConvertBuffer<'_>,
    header: &mut JEntry,
    val: &JsonbValue<'_>,
    level: u32,
) -> PgResult<()> {
    check_stack_depth()?;
    match val {
        JsonbValue::Array { .. } => convert_array(buffer, header, val, level),
        JsonbValue::Object { .. } => convert_object(buffer, header, val, level),
        _ => convert_scalar(buffer, header, val),
    }
}

#[track_caller]
#[cold]
#[inline(never)]
fn total_size_error(kind: &str) -> Box<PgError> {
    limit_error(alloc::format!(
        "total size of jsonb {kind} elements exceeds the maximum of {JENTRY_OFFLENMASK} bytes"
    ))
}

fn convert_array(
    buffer: &mut ConvertBuffer<'_>,
    header: &mut JEntry,
    val: &JsonbValue<'_>,
    level: u32,
) -> PgResult<()> {
    let JsonbValue::Array { elems, raw_scalar } = val else {
        unreachable!()
    };
    let elems = elems.as_slice();
    let n_elems = elems.len();
    let base_offset = buffer.data.len();
    buffer.pad_to_int();

    let mut containerhead = n_elems as u32 | JB_FARRAY;
    if *raw_scalar {
        debug_assert!(n_elems == 1 && level == 0);
        containerhead |= JB_FSCALAR;
    }
    buffer.append(&containerhead.to_ne_bytes())?;
    let mut jentry_offset = buffer.reserve(4 * n_elems);

    let mut totallen: u64 = 0;
    for (i, elem) in elems.iter().enumerate() {
        let mut meta = 0;
        convert_value(buffer, &mut meta, elem, level + 1)?;
        totallen += u64::from(meta & JENTRY_OFFLENMASK);
        if totallen > u64::from(JENTRY_OFFLENMASK) {
            return Err(total_size_error("array"));
        }
        if i % JB_OFFSET_STRIDE as usize == 0 {
            meta = (meta & JENTRY_TYPEMASK) | totallen as u32 | JENTRY_HAS_OFF;
        }
        buffer.copy_to(jentry_offset, &meta.to_ne_bytes());
        jentry_offset += 4;
    }

    let totallen = buffer.data.len() - base_offset;
    if totallen > JENTRY_OFFLENMASK as usize {
        return Err(total_size_error("array"));
    }
    *header = JENTRY_ISCONTAINER | totallen as u32;
    Ok(())
}

fn convert_object(
    buffer: &mut ConvertBuffer<'_>,
    header: &mut JEntry,
    val: &JsonbValue<'_>,
    level: u32,
) -> PgResult<()> {
    let JsonbValue::Object { pairs } = val else {
        unreachable!()
    };
    let pairs = pairs.as_slice();
    let n_pairs = pairs.len();
    let base_offset = buffer.data.len();
    buffer.pad_to_int();

    let containerheader = n_pairs as u32 | JB_FOBJECT;
    buffer.append(&containerheader.to_ne_bytes())?;
    let mut jentry_offset = buffer.reserve(4 * n_pairs * 2);

    // Keys first, then values (the on-disk pair layout).
    let mut totallen: u64 = 0;
    for (i, pair) in pairs.iter().enumerate() {
        let mut meta = 0;
        let key = JsonbValue::String(pair.key);
        convert_scalar(buffer, &mut meta, &key)?;
        totallen += u64::from(meta & JENTRY_OFFLENMASK);
        if totallen > u64::from(JENTRY_OFFLENMASK) {
            return Err(total_size_error("object"));
        }
        if i % JB_OFFSET_STRIDE as usize == 0 {
            meta = (meta & JENTRY_TYPEMASK) | totallen as u32 | JENTRY_HAS_OFF;
        }
        buffer.copy_to(jentry_offset, &meta.to_ne_bytes());
        jentry_offset += 4;
    }
    for (i, pair) in pairs.iter().enumerate() {
        let mut meta = 0;
        convert_value(buffer, &mut meta, &pair.value, level + 1)?;
        totallen += u64::from(meta & JENTRY_OFFLENMASK);
        if totallen > u64::from(JENTRY_OFFLENMASK) {
            return Err(total_size_error("object"));
        }
        if (i + n_pairs) % JB_OFFSET_STRIDE as usize == 0 {
            meta = (meta & JENTRY_TYPEMASK) | totallen as u32 | JENTRY_HAS_OFF;
        }
        buffer.copy_to(jentry_offset, &meta.to_ne_bytes());
        jentry_offset += 4;
    }

    let totallen = buffer.data.len() - base_offset;
    if totallen > JENTRY_OFFLENMASK as usize {
        return Err(total_size_error("object"));
    }
    *header = JENTRY_ISCONTAINER | totallen as u32;
    Ok(())
}

fn convert_scalar(
    buffer: &mut ConvertBuffer<'_>,
    header: &mut JEntry,
    val: &JsonbValue<'_>,
) -> PgResult<()> {
    match val {
        JsonbValue::Null => *header = JENTRY_ISNULL,
        JsonbValue::String(s) => {
            buffer.append(s)?;
            *header = s.len() as u32;
        }
        JsonbValue::Numeric(image) => {
            let padlen = buffer.pad_to_int();
            buffer.append(image)?;
            *header = JENTRY_ISNUMERIC | (padlen + image.len()) as u32;
        }
        JsonbValue::Bool(true) => *header = JENTRY_ISBOOL_TRUE,
        JsonbValue::Bool(false) => *header = JENTRY_ISBOOL_FALSE,
        _ => panic!("invalid jsonb scalar type"),
    }
    Ok(())
}

/// C: JsonbValueToJsonb over live shapes: scalar (raw-scalar wrap) or binary
/// container (verbatim copy).
pub fn item_to_jsonb_image<'mcx>(mcx: Mcx<'mcx>, item: JsonbItem<'_>) -> PgResult<PgVec<'mcx, u8>> {
    match item {
        JsonbItem::Binary(data) => {
            let mut out = mcx::vec_with_capacity_in(mcx, 4 + data.len())?;
            mcx::vec_append_bytes(&mut out, &((4 + data.len() as u32) << 2).to_ne_bytes())?;
            mcx::vec_append_bytes(&mut out, data)?;
            Ok(out)
        }
        JsonbItem::Array { .. } | JsonbItem::Object { .. } => {
            panic!("item_to_jsonb_image: begin-token item is not a value")
        }
        scalar => {
            let mut elems = ArenaVec::with_capacity(mcx, 1)?;
            elems.push(mcx, JsonbValue::from_item(scalar))?;
            let val = JsonbValue::Array {
                elems,
                raw_scalar: true,
            };
            convert_to_jsonb(mcx, &val)
        }
    }
}
