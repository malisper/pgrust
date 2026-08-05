//! On-disk JsonbContainer read path over raw payload bytes; the serialized
//! form is never materialized as a typed tree (jsonb.h JEntry encoding).

pub type JEntry = u32;

pub const JENTRY_OFFLENMASK: u32 = 0x0FFF_FFFF;
pub const JENTRY_TYPEMASK: u32 = 0x7000_0000;
pub const JENTRY_HAS_OFF: u32 = 0x8000_0000;

pub const JENTRY_ISSTRING: u32 = 0x0000_0000;
pub const JENTRY_ISNUMERIC: u32 = 0x1000_0000;
pub const JENTRY_ISBOOL_FALSE: u32 = 0x2000_0000;
pub const JENTRY_ISBOOL_TRUE: u32 = 0x3000_0000;
pub const JENTRY_ISNULL: u32 = 0x4000_0000;
pub const JENTRY_ISCONTAINER: u32 = 0x5000_0000;

pub const JB_OFFSET_STRIDE: u32 = 32;

pub const JB_CMASK: u32 = 0x0FFF_FFFF;
pub const JB_FSCALAR: u32 = 0x1000_0000;
pub const JB_FOBJECT: u32 = 0x2000_0000;
pub const JB_FARRAY: u32 = 0x4000_0000;

#[inline]
pub fn intalign(n: u32) -> u32 {
    (n + 3) & !3
}

#[inline]
fn u32_at(b: &[u8], off: usize) -> u32 {
    u32::from_ne_bytes(b[off..off + 4].try_into().unwrap())
}

#[inline]
pub fn container_header(c: &[u8]) -> u32 {
    u32_at(c, 0)
}

#[inline]
pub fn container_size(c: &[u8]) -> u32 {
    container_header(c) & JB_CMASK
}

#[inline]
pub fn container_is_scalar(c: &[u8]) -> bool {
    container_header(c) & JB_FSCALAR != 0
}

#[inline]
pub fn container_is_object(c: &[u8]) -> bool {
    container_header(c) & JB_FOBJECT != 0
}

#[inline]
pub fn container_is_array(c: &[u8]) -> bool {
    container_header(c) & JB_FARRAY != 0
}

#[inline]
pub fn child_jentry(c: &[u8], index: u32) -> JEntry {
    u32_at(c, 4 + 4 * index as usize)
}

// C's base_addr: past the header word and the JEntry array.
#[inline]
pub fn container_base_off(c: &[u8]) -> u32 {
    let n = container_size(c);
    if container_is_object(c) {
        4 + 8 * n
    } else {
        4 + 4 * n
    }
}

#[inline]
pub fn jbe_advance_offset(offset: &mut u32, je: JEntry) {
    if je & JENTRY_HAS_OFF != 0 {
        *offset = je & JENTRY_OFFLENMASK;
    } else {
        *offset += je & JENTRY_OFFLENMASK;
    }
}

pub fn get_jsonb_offset(c: &[u8], index: u32) -> u32 {
    let mut offset = 0;
    let mut i = index as i64 - 1;
    while i >= 0 {
        let je = child_jentry(c, i as u32);
        offset += je & JENTRY_OFFLENMASK;
        if je & JENTRY_HAS_OFF != 0 {
            break;
        }
        i -= 1;
    }
    offset
}

pub fn get_jsonb_length(c: &[u8], index: u32) -> u32 {
    let je = child_jentry(c, index);
    if je & JENTRY_HAS_OFF != 0 {
        (je & JENTRY_OFFLENMASK) - get_jsonb_offset(c, index)
    } else {
        je & JENTRY_OFFLENMASK
    }
}

// VARSIZE_ANY over an embedded numeric image (1B-short or 4B header).
#[inline]
pub fn varsize_any(b: &[u8]) -> usize {
    if b[0] & 1 == 1 {
        ((b[0] >> 1) & 0x7F) as usize
    } else {
        (u32_at(b, 0) >> 2) as usize
    }
}

/// C: JsonbValue, read-side shim: leaf byte runs borrow the document.
/// `Numeric` is the full embedded numeric varlena image (C measures it with
/// VARSIZE_ANY); `Binary` is the nested container window.
#[derive(Clone, Copy, Debug)]
pub enum JsonbItem<'a> {
    Null,
    String(&'a [u8]),
    Numeric(&'a [u8]),
    Bool(bool),
    Array { n_elems: u32, raw_scalar: bool },
    Object { n_pairs: u32 },
    Binary(&'a [u8]),
}

impl JsonbItem<'_> {
    // enum jbvType discriminants; compareJsonbContainers' type-defined order.
    #[inline]
    pub fn type_ord(&self) -> u32 {
        match self {
            JsonbItem::Null => 0x0,
            JsonbItem::String(_) => 0x1,
            JsonbItem::Numeric(_) => 0x2,
            JsonbItem::Bool(_) => 0x3,
            JsonbItem::Array { .. } => 0x10,
            JsonbItem::Object { .. } => 0x11,
            JsonbItem::Binary(_) => 0x12,
        }
    }

    #[inline]
    pub fn is_scalar(&self) -> bool {
        self.type_ord() <= 0x3
    }
}

/// C: fillJsonbValue. `offset` is relative to the container's variable-length
/// portion at `base_off`.
pub fn fill_item<'a>(c: &'a [u8], index: u32, base_off: u32, offset: u32) -> JsonbItem<'a> {
    let entry = child_jentry(c, index);
    match entry & JENTRY_TYPEMASK {
        JENTRY_ISNULL => JsonbItem::Null,
        JENTRY_ISSTRING => {
            let start = (base_off + offset) as usize;
            let len = get_jsonb_length(c, index) as usize;
            JsonbItem::String(&c[start..start + len])
        }
        JENTRY_ISNUMERIC => {
            let start = (base_off + intalign(offset)) as usize;
            let size = varsize_any(&c[start..]);
            JsonbItem::Numeric(&c[start..start + size])
        }
        JENTRY_ISBOOL_TRUE => JsonbItem::Bool(true),
        JENTRY_ISBOOL_FALSE => JsonbItem::Bool(false),
        JENTRY_ISCONTAINER => {
            let start = (base_off + intalign(offset)) as usize;
            let len = get_jsonb_length(c, index) - (intalign(offset) - offset);
            JsonbItem::Binary(&c[start..start + len as usize])
        }
        _ => panic!("invalid jsonb JEntry type: 0x{entry:08x}"),
    }
}

/// C: lengthCompareJsonbString — length-first then memcmp (key order).
#[inline]
pub fn length_compare_jsonb_string(a: &[u8], b: &[u8]) -> core::cmp::Ordering {
    a.len().cmp(&b.len()).then_with(|| a.cmp(b))
}

/// C: getKeyJsonValueFromContainer — binary search over the key half.
pub fn get_key_value<'a>(c: &'a [u8], key: &[u8]) -> Option<JsonbItem<'a>> {
    debug_assert!(container_is_object(c));
    let count = container_size(c);
    if count == 0 {
        return None;
    }
    let base_off = 4 + 8 * count;
    let mut lo = 0u32;
    let mut hi = count;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let start = (base_off + get_jsonb_offset(c, mid)) as usize;
        let len = get_jsonb_length(c, mid) as usize;
        match length_compare_jsonb_string(&c[start..start + len], key) {
            core::cmp::Ordering::Equal => {
                let index = mid + count;
                return Some(fill_item(c, index, base_off, get_jsonb_offset(c, index)));
            }
            core::cmp::Ordering::Less => lo = mid + 1,
            core::cmp::Ordering::Greater => hi = mid,
        }
    }
    None
}

/// C: jsonb_array_length's value/verdict core, factored out of
/// fc_jsonb_array_length for proofs/jsonb-probe (behavior unchanged; the
/// wrapper maps Err(msg) to the same PgError it previously built inline).
pub fn array_length(c: &[u8]) -> Result<i32, &'static str> {
    if container_is_scalar(c) {
        return Err("cannot get array length of a scalar");
    }
    if !container_is_array(c) {
        return Err("cannot get array length of a non-array");
    }
    Ok(container_size(c) as i32)
}

/// C: getIthJsonbValueFromContainer.
pub fn get_ith_value(c: &[u8], i: u32) -> Option<JsonbItem<'_>> {
    assert!(container_is_array(c), "not a jsonb array");
    let n = container_size(c);
    if i >= n {
        return None;
    }
    let base_off = 4 + 4 * n;
    Some(fill_item(c, i, base_off, get_jsonb_offset(c, i)))
}
