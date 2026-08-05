use crate::container::*;
use crate::iter::{JsonbIterator, WjbToken};
use adt_numeric::Num;
use mcx::Mcx;
use stack_depth::check_stack_depth;
use types_core::DEFAULT_COLLATION_OID;
use types_error::PgResult;

/// C: equalsJsonbScalarValue.
pub fn equals_scalar(a: &JsonbItem<'_>, b: &JsonbItem<'_>) -> bool {
    match (a, b) {
        (JsonbItem::Null, JsonbItem::Null) => true,
        (JsonbItem::String(x), JsonbItem::String(y)) => {
            length_compare_jsonb_string(x, y) == core::cmp::Ordering::Equal
        }
        (JsonbItem::Numeric(x), JsonbItem::Numeric(y)) => {
            adt_numeric::numeric_eq(Num::from_payload(&x[4..]), Num::from_payload(&y[4..]))
        }
        (JsonbItem::Bool(x), JsonbItem::Bool(y)) => x == y,
        _ => panic!("jsonb scalar type mismatch"),
    }
}

/// C: compareJsonbScalarValue — btree order (collation-aware strings).
fn compare_scalar(a: &JsonbItem<'_>, b: &JsonbItem<'_>) -> PgResult<i32> {
    match (a, b) {
        (JsonbItem::Null, JsonbItem::Null) => Ok(0),
        (JsonbItem::String(x), JsonbItem::String(y)) => {
            varlena::varstr_cmp(x, y, DEFAULT_COLLATION_OID)
        }
        (JsonbItem::Numeric(x), JsonbItem::Numeric(y)) => Ok(adt_numeric::cmp_numerics(
            Num::from_payload(&x[4..]),
            Num::from_payload(&y[4..]),
        )),
        (JsonbItem::Bool(x), JsonbItem::Bool(y)) => Ok((x > y) as i32 - (x < y) as i32),
        _ => panic!("jsonb scalar type mismatch"),
    }
}

/// Outcome of one compareJsonbContainers loop iteration.
enum CmpStep {
    /// Both iterators returned Done with res still 0: comparison finished.
    Break,
    /// Keep walking (res may have been set, ending the loop).
    Continue,
}

/// The loop body of C's compareJsonbContainers, shared by the allocating walk
/// and the non-allocating core (proofs/jsonb-probe cmp family).
fn compare_step(
    ra: WjbToken,
    va: &JsonbItem<'_>,
    rb: WjbToken,
    vb: &JsonbItem<'_>,
    res: &mut i32,
) -> PgResult<CmpStep> {
    if ra == rb {
        if ra == WjbToken::Done {
            return Ok(CmpStep::Break);
        }
        if ra == WjbToken::EndArray || ra == WjbToken::EndObject {
            return Ok(CmpStep::Continue);
        }
        if va.type_ord() == vb.type_ord() {
            match (va, vb) {
                (JsonbItem::Array { n_elems: na, raw_scalar: rsa },
                 JsonbItem::Array { n_elems: nb, raw_scalar: rsb }) => {
                    // C quirk preserved: the raw-scalar result may be
                    // overridden by the nElems check (no else) — an empty
                    // top-level array sorts less than null.
                    if rsa != rsb {
                        *res = if *rsa { -1 } else { 1 };
                    }
                    if na != nb {
                        *res = if na > nb { 1 } else { -1 };
                    }
                }
                (JsonbItem::Object { n_pairs: na }, JsonbItem::Object { n_pairs: nb }) => {
                    if na != nb {
                        *res = if na > nb { 1 } else { -1 };
                    }
                }
                (JsonbItem::Binary(_), _) => panic!("unexpected jbvBinary value"),
                _ => *res = compare_scalar(va, vb)?,
            }
        } else {
            // Type-defined order.
            *res = if va.type_ord() > vb.type_ord() { 1 } else { -1 };
        }
    } else {
        debug_assert!(ra != WjbToken::EndArray && ra != WjbToken::EndObject);
        debug_assert!(rb != WjbToken::EndArray && rb != WjbToken::EndObject);
        *res = if va.type_ord() > vb.type_ord() { 1 } else { -1 };
    }
    Ok(CmpStep::Continue)
}

/// Nesting depth (frames, root included) the non-allocating compare core
/// handles before `compare_containers` falls back to the allocating walk.
pub const CMP_FIXED_DEPTH: usize = 32;

/// C: compareJsonbContainers — non-allocating core for the proofs/jsonb-probe
/// cmp family. No Mcx, no heap: the iterator frame stacks are inline
/// `[Frame; N]` arrays. Returns `None` iff either input nests deeper than N
/// frames; otherwise identical to the allocating walk on the same input.
pub fn compare_containers_fixed<const N: usize>(a: &[u8], b: &[u8]) -> Option<PgResult<i32>> {
    let mut ita = crate::iter::FixedJsonbIterator::<N>::init(a);
    let mut itb = crate::iter::FixedJsonbIterator::<N>::init(b);
    let mut res: i32 = 0;

    while res == 0 {
        let (ra, va) = ita.next(false)?;
        let (rb, vb) = itb.next(false)?;
        match compare_step(ra, &va, rb, &vb, &mut res) {
            Ok(CmpStep::Break) => break,
            Ok(CmpStep::Continue) => {}
            Err(e) => return Some(Err(e)),
        }
    }

    Some(Ok(res))
}

/// C: compareJsonbContainers — btree support worker. Runs the non-allocating
/// core first; inputs nesting deeper than CMP_FIXED_DEPTH take the original
/// Mcx-backed walk (C pallocs one iterator per level with no depth limit, so
/// the deep path must stay unbounded).
pub fn compare_containers(mcx: Mcx<'_>, a: &[u8], b: &[u8]) -> PgResult<i32> {
    if let Some(res) = compare_containers_fixed::<CMP_FIXED_DEPTH>(a, b) {
        return res;
    }

    let mut ita = JsonbIterator::init(mcx, a)?;
    let mut itb = JsonbIterator::init(mcx, b)?;
    let mut res: i32 = 0;

    while res == 0 {
        let (ra, va) = ita.next(false);
        let (rb, vb) = itb.next(false);
        match compare_step(ra, &va, rb, &vb, &mut res)? {
            CmpStep::Break => break,
            CmpStep::Continue => {}
        }
    }

    Ok(res)
}

/// C: findJsonbValueFromContainer, JB_FARRAY arm (element equality scan).
pub fn find_in_array(c: &[u8], key: &JsonbItem<'_>) -> bool {
    debug_assert!(key.is_scalar());
    let count = container_size(c);
    let base_off = 4 + 4 * count;
    let mut offset = 0u32;
    for i in 0..count {
        let item = fill_item(c, i, base_off, offset);
        if key.type_ord() == item.type_ord() && equals_scalar(key, &item) {
            return true;
        }
        jbe_advance_offset(&mut offset, child_jentry(c, i));
    }
    false
}

/// C: findJsonbValueFromContainer(JB_FOBJECT|JB_FARRAY), the `?` shape.
pub fn exists_key(c: &[u8], key: &[u8]) -> bool {
    if container_size(c) == 0 {
        return false;
    }
    if container_is_array(c) {
        find_in_array(c, &JsonbItem::String(key))
    } else if container_is_object(c) {
        get_key_value(c, key).is_some()
    } else {
        false
    }
}

/// C: jsonb_contains — mismatched root kinds are never contained.
pub fn jsonb_contains(mcx: Mcx<'_>, val: &[u8], tmpl: &[u8]) -> PgResult<bool> {
    if (container_header(val) & JB_FOBJECT) != (container_header(tmpl) & JB_FOBJECT) {
        return Ok(false);
    }
    let mut it1 = JsonbIterator::init(mcx, val)?;
    let mut it2 = JsonbIterator::init(mcx, tmpl)?;
    deep_contains(mcx, &mut it1, &mut it2)
}

/// C: JsonbDeepContains (iterators just before their begin tokens).
pub fn deep_contains(
    mcx: Mcx<'_>,
    val: &mut JsonbIterator<'_, '_>,
    contained: &mut JsonbIterator<'_, '_>,
) -> PgResult<bool> {
    check_stack_depth()?;

    let val_container = val.current_container();
    let (rval, vval) = val.next(false);
    let (rcont, _vcont) = contained.next(false);

    if rval != rcont {
        return Ok(false);
    }
    match rcont {
        WjbToken::BeginObject => {
            let JsonbItem::Object { n_pairs: nval } = vval else {
                panic!("expected object item")
            };
            let JsonbItem::Object { n_pairs: ncont } = _vcont else {
                panic!("expected object item")
            };
            // Fewer lhs pairs cannot contain rhs (keys are de-duplicated).
            if nval < ncont {
                return Ok(false);
            }
            loop {
                let (rcont, vcont) = contained.next(false);
                if rcont == WjbToken::EndObject {
                    return Ok(true);
                }
                debug_assert_eq!(rcont, WjbToken::Key);
                let JsonbItem::String(key) = vcont else {
                    panic!("expected string key")
                };
                let Some(lhs_val) = get_key_value(val_container, key) else {
                    return Ok(false);
                };
                let (rcont, vcont) = contained.next(true);
                debug_assert_eq!(rcont, WjbToken::Value);
                if lhs_val.type_ord() != vcont.type_ord() {
                    return Ok(false);
                } else if lhs_val.is_scalar() {
                    if !equals_scalar(&lhs_val, &vcont) {
                        return Ok(false);
                    }
                } else {
                    let (JsonbItem::Binary(lhs_c), JsonbItem::Binary(rhs_c)) = (lhs_val, vcont)
                    else {
                        panic!("expected binary containers")
                    };
                    let mut nestval = JsonbIterator::init(mcx, lhs_c)?;
                    let mut nestcont = JsonbIterator::init(mcx, rhs_c)?;
                    if !deep_contains(mcx, &mut nestval, &mut nestcont)? {
                        return Ok(false);
                    }
                }
            }
        }
        WjbToken::BeginArray => {
            let JsonbItem::Array { n_elems: n_lhs_elems, raw_scalar: lhs_raw } = vval else {
                panic!("expected array item")
            };
            let JsonbItem::Array { raw_scalar: cont_raw, .. } = _vcont else {
                panic!("expected array item")
            };
            // A raw scalar may not contain a real array.
            if lhs_raw && !cont_raw {
                return Ok(false);
            }
            let mut lhs_conts: Option<mcx::PgVec<'_, &[u8]>> = None;
            loop {
                let (rcont, vcont) = contained.next(true);
                if rcont == WjbToken::EndArray {
                    return Ok(true);
                }
                debug_assert_eq!(rcont, WjbToken::Elem);
                if vcont.is_scalar() {
                    if !find_in_array(val_container, &vcont) {
                        return Ok(false);
                    }
                } else {
                    // Lazily collect lhs container elements (C: lhsConts).
                    if lhs_conts.is_none() {
                        let mut v = mcx::vec_with_capacity_in(mcx, n_lhs_elems as usize)?;
                        for _ in 0..n_lhs_elems {
                            let (r, e) = val.next(true);
                            debug_assert_eq!(r, WjbToken::Elem);
                            if let JsonbItem::Binary(c) = e {
                                v.push(c);
                            }
                        }
                        if v.is_empty() {
                            return Ok(false);
                        }
                        lhs_conts = Some(v);
                    }
                    let JsonbItem::Binary(rhs_c) = vcont else {
                        panic!("expected binary container")
                    };
                    // C keeps the note: nested array containment is O(N^2).
                    let mut found = false;
                    for lhs_c in lhs_conts.as_ref().unwrap().iter() {
                        let mut nestval = JsonbIterator::init(mcx, lhs_c)?;
                        let mut nestcont = JsonbIterator::init(mcx, rhs_c)?;
                        if deep_contains(mcx, &mut nestval, &mut nestcont)? {
                            found = true;
                            break;
                        }
                    }
                    if !found {
                        return Ok(false);
                    }
                }
            }
        }
        _ => panic!("invalid jsonb container type"),
    }
}

// C: hash_numeric over the embedded image (unported in adt_numeric).
fn hash_numeric(image: &[u8]) -> u32 {
    let num = Num::from_payload(&image[4..]);
    if num.is_special() {
        return 0;
    }
    let digits = num.digits();
    let mut weight = num.weight();
    let mut start = 0usize;
    while start < digits.len() && digits[start] == 0 {
        start += 1;
        weight -= 1;
    }
    if start == digits.len() {
        return u32::MAX; // C: PG_RETURN_UINT32(-1)
    }
    let mut end = digits.len();
    while end > start && digits[end - 1] == 0 {
        end -= 1;
    }
    let bytes = unsafe {
        // SAFETY: NumericDigit is i16; reinterpreting the live digit slice as
        // bytes for hashing, exactly C's hash_any over the digit array.
        core::slice::from_raw_parts(digits[start..end].as_ptr().cast::<u8>(), (end - start) * 2)
    };
    hashfn::hash_bytes(bytes) ^ weight as u32
}

fn hash_numeric_extended(image: &[u8], seed: u64) -> u64 {
    let num = Num::from_payload(&image[4..]);
    if num.is_special() {
        return seed;
    }
    let digits = num.digits();
    let mut weight = num.weight();
    let mut start = 0usize;
    while start < digits.len() && digits[start] == 0 {
        start += 1;
        weight -= 1;
    }
    if start == digits.len() {
        return seed.wrapping_sub(1); // C: seed - 1
    }
    let mut end = digits.len();
    while end > start && digits[end - 1] == 0 {
        end -= 1;
    }
    let bytes = unsafe {
        // SAFETY: as hash_numeric.
        core::slice::from_raw_parts(digits[start..end].as_ptr().cast::<u8>(), (end - start) * 2)
    };
    hashfn::hash_bytes_extended(bytes, seed) ^ weight as u64
}

/// C: JsonbHashScalarValue.
pub fn hash_scalar_value(v: &JsonbItem<'_>, hash: &mut u32) {
    let tmp = match v {
        JsonbItem::Null => 0x01,
        JsonbItem::String(s) => hashfn::hash_bytes(s),
        JsonbItem::Numeric(image) => hash_numeric(image),
        JsonbItem::Bool(true) => 0x02,
        JsonbItem::Bool(false) => 0x04,
        _ => panic!("invalid jsonb scalar type"),
    };
    *hash = hash.rotate_left(1) ^ tmp;
}

// C: ROTATE_HIGH_AND_LOW_32BITS.
#[inline]
fn rotate_high_and_low_32bits(x: u64) -> u64 {
    ((x << 1) & 0xffff_fffe_ffff_fffe) | ((x >> 31) & 0x0000_0001_0000_0001)
}

/// C: JsonbHashScalarValueExtended.
pub fn hash_scalar_value_extended(v: &JsonbItem<'_>, hash: &mut u64, seed: u64) {
    let tmp = match v {
        JsonbItem::Null => seed.wrapping_add(0x01),
        JsonbItem::String(s) => hashfn::hash_bytes_extended(s, seed),
        JsonbItem::Numeric(image) => hash_numeric_extended(image, seed),
        JsonbItem::Bool(b) => {
            if seed != 0 {
                // C: hashcharextended(bool) = hash_uint32_extended((int32) char).
                hashfn::hash_bytes_uint32_extended(*b as u32, seed)
            } else if *b {
                0x02
            } else {
                0x04
            }
        }
        _ => panic!("invalid jsonb scalar type"),
    };
    *hash = rotate_high_and_low_32bits(*hash) ^ tmp;
}

/// C: jsonb_hash.
pub fn jsonb_hash(mcx: Mcx<'_>, payload: &[u8]) -> PgResult<u32> {
    if container_size(payload) == 0 {
        return Ok(0);
    }
    let mut it = JsonbIterator::init(mcx, payload)?;
    let mut hash: u32 = 0;
    loop {
        let (r, v) = it.next(false);
        match r {
            WjbToken::Done => break,
            WjbToken::BeginArray => hash ^= JB_FARRAY,
            WjbToken::BeginObject => hash ^= JB_FOBJECT,
            WjbToken::Key | WjbToken::Value | WjbToken::Elem => hash_scalar_value(&v, &mut hash),
            WjbToken::EndArray | WjbToken::EndObject => {}
        }
    }
    Ok(hash)
}

/// C: jsonb_hash_extended.
pub fn jsonb_hash_extended(mcx: Mcx<'_>, payload: &[u8], seed: u64) -> PgResult<u64> {
    if container_size(payload) == 0 {
        return Ok(seed);
    }
    let mut it = JsonbIterator::init(mcx, payload)?;
    let mut hash: u64 = 0;
    loop {
        let (r, v) = it.next(false);
        match r {
            WjbToken::Done => break,
            WjbToken::BeginArray => hash ^= (u64::from(JB_FARRAY) << 32) | u64::from(JB_FARRAY),
            WjbToken::BeginObject => {
                hash ^= (u64::from(JB_FOBJECT) << 32) | u64::from(JB_FOBJECT)
            }
            WjbToken::Key | WjbToken::Value | WjbToken::Elem => {
                hash_scalar_value_extended(&v, &mut hash, seed)
            }
            WjbToken::EndArray | WjbToken::EndObject => {}
        }
    }
    Ok(hash)
}
