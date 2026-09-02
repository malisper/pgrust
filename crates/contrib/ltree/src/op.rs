//! `contrib/ltree/ltree_op.c`, `lquery_op.c`, `ltxtquery_op.c` — the scalar
//! operators/functions over `ltree` / `lquery` / `ltxtquery` (everything that
//! works on a sequential scan). Each function operates on the borrowed varlena

use ::types_error::{
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_PROGRAM_LIMIT_EXCEEDED,
};
use ::types_error::PgError;

use crate::crc::fold;
use crate::repr::*;


// upstream c3e36a9a5f19 (18.6): Fix int32 overflow in ltree_compare()
/// btree-comparison function, sign-only: memcmp value, label-length
/// difference, level-count difference. The old `* 10 * (an + 1)` scaling
/// overflowed int32 past ~14,653 levels; `ltree_compare_distance` keeps it.
pub fn ltree_compare(a: &[u8], b: &[u8]) -> i32 {
    let ta = Ltree::new(a);
    let tb = Ltree::new(b);
    let mut an = ta.numlevel() as i32;
    let mut bn = tb.numlevel() as i32;
    let mut ai = ta.levels();
    let mut bi = tb.levels();
    while an > 0 && bn > 0 {
        let al = ai.next().unwrap();
        let bl = bi.next().unwrap();
        let res = memcmp(al.name, bl.name);
        if res == 0 {
            if al.name.len() != bl.name.len() {
                return al.name.len() as i32 - bl.name.len() as i32;
            }
        } else {
            return res;
        }
        an -= 1;
        bn -= 1;
    }
    ta.numlevel() as i32 - tb.numlevel() as i32
}

/// A signed "distance" between `a` and `b`, ordered like `ltree_compare`, in
/// float so the `10 * (an + 1)` scaling cannot overflow (the GiST penalty).
pub fn ltree_compare_distance(a: &[u8], b: &[u8]) -> f32 {
    let ta = Ltree::new(a);
    let tb = Ltree::new(b);
    let mut an = ta.numlevel() as i32;
    let mut bn = tb.numlevel() as i32;
    let mut ai = ta.levels();
    let mut bi = tb.levels();
    while an > 0 && bn > 0 {
        let al = ai.next().unwrap();
        let bl = bi.next().unwrap();
        let res = memcmp(al.name, bl.name);
        if res == 0 {
            if al.name.len() != bl.name.len() {
                return scaled(al.name.len() as i32 - bl.name.len() as i32, an);
            }
        } else {
            return scaled(res.signum(), an);
        }
        an -= 1;
        bn -= 1;
    }
    scaled(ta.numlevel() as i32 - tb.numlevel() as i32, an)
}

/// C's `(float) delta * 10.0 * (an + 1)`: a double product narrowed to float.
#[inline]
fn scaled(delta: i32, an: i32) -> f32 {
    (delta as f64 * 10.0 * (an + 1) as f64) as f32
}

/// C's `memcmp(a, b, Min(a_len, b_len))`. Its value is implementation-defined
/// beyond the sign; macOS libSystem and glibc return the first differing byte
/// difference, which is what the oracles pgrust is compared against produce.
#[inline]
fn memcmp(a: &[u8], b: &[u8]) -> i32 {
    let n = a.len().min(b.len());
    for i in 0..n {
        if a[i] != b[i] {
            return a[i] as i32 - b[i] as i32;
        }
    }
    0
}

/// `hash_ltree(a)` — `hash_any` per level, combined `result = result*31 + h`.
pub fn hash_ltree(a: &[u8]) -> u32 {
    let t = Ltree::new(a);
    let mut result: u32 = 1;
    for lvl in t.levels() {
        let level_hash = ::hashfn::hash_bytes(lvl.name);
        result = (result << 5).wrapping_sub(result).wrapping_add(level_hash);
    }
    result
}

pub fn hash_ltree_extended(a: &[u8], seed: u64) -> u64 {
    let t = Ltree::new(a);
    if t.numlevel() == 0 {
        return 1u64.wrapping_add(seed);
    }
    let mut result: u64 = 1;
    for lvl in t.levels() {
        let level_hash = ::hashfn::hash_bytes_extended(lvl.name, seed);
        result = (result << 5).wrapping_sub(result).wrapping_add(level_hash);
    }
    result
}

pub fn nlevel(a: &[u8]) -> i32 {
    Ltree::new(a).numlevel() as i32
}

pub fn inner_isparent(c: &[u8], p: &[u8]) -> bool {
    let tc = Ltree::new(c);
    let tp = Ltree::new(p);
    let pn = tp.numlevel();
    if pn > tc.numlevel() {
        return false;
    }
    let mut ci = tc.levels();
    for pl in tp.levels() {
        let cl = ci.next().unwrap();
        if cl.name != pl.name {
            return false;
        }
    }
    true
}


pub fn inner_subltree(t: &[u8], startpos: i32, endpos_in: i32) -> Result<Vec<u8>, PgError> {
    let tt = Ltree::new(t);
    let numlevel = tt.numlevel() as i32;
    if startpos < 0 || endpos_in < 0 || startpos >= numlevel || startpos > endpos_in {
        return Err(PgError::error("invalid positions").with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    }
    let endpos = endpos_in.min(numlevel);

    // C walks POINTERS and copies the raw byte range [start, end), setting
    // numlevel = endpos - startpos independently. When startpos == endpos > 0
    // the `i == startpos` arm never fires, so `start` keeps its initial value
    // (the first level) while `end` advances to level endpos: the result is a
    // header claiming 0 levels over a payload of `endpos` levels. That image
    // is what PostgreSQL stores (subltree('a.b.c.d',3,3) is 32 bytes, not 8),
    // and ltree images are on-disk format, so reproduce the walk exactly
    // rather than emitting a tidier empty ltree.
    let lvl_offs: Vec<usize> = {
        let mut offs = Vec::with_capacity(numlevel as usize + 1);
        let mut off = 0usize;
        for l in tt.levels() {
            offs.push(off);
            off += maxalign(l.name.len() + LEVEL_HDRSIZE);
        }
        offs.push(off);
        offs
    };
    let (mut start, mut end) = (0usize, 0usize);
    for i in 0..endpos {
        if i == startpos {
            start = lvl_offs[i as usize];
        }
        if i == endpos - 1 {
            end = lvl_offs[i as usize + 1];
            break;
        }
    }
    let body = &t[LTREE_HDRSIZE + start..LTREE_HDRSIZE + end];
    let total = LTREE_HDRSIZE + body.len();
    let mut res = vec![0u8; total];
    set_varsize(&mut res, total);
    write_u16(&mut res, 4, (endpos - startpos) as u16);
    res[LTREE_HDRSIZE..].copy_from_slice(body);
    Ok(res)
}

pub fn subpath(t: &[u8], start_in: i32, len_opt: Option<i32>) -> Result<Vec<u8>, PgError> {
    let numlevel = Ltree::new(t).numlevel() as i32;
    let len = len_opt.unwrap_or(0);
    let three = len_opt.is_some();
    // Same -fwrapv arithmetic as ltree_index: subpath('a.b', 1, 2147483647)
    // and subpath('a.b', -2147483648) are SQL-reachable and overflow every
    // one of these adds in C, which wraps; a checked add panics the backend.
    let mut start = start_in;
    let mut end = start.wrapping_add(len);

    if start < 0 {
        start = numlevel.wrapping_add(start);
        end = start.wrapping_add(len);
    }
    if start < 0 {
        start = numlevel.wrapping_add(start);
        end = start.wrapping_add(len);
    }
    if len < 0 {
        end = numlevel.wrapping_add(len);
    } else if len == 0 {
        end = if three { start } else { 0xffff };
    }
    inner_subltree(t, start, end)
}

pub fn ltree_concat(a: &[u8], b: &[u8]) -> Result<Vec<u8>, PgError> {
    let ta = Ltree::new(a);
    let tb = Ltree::new(b);
    let numlevel = ta.numlevel() as i32 + tb.numlevel() as i32;
    if numlevel > LTREE_MAX_LEVELS {
        return Err(PgError::error(format!(
            "number of ltree levels ({}) exceeds the maximum allowed ({})",
            numlevel, LTREE_MAX_LEVELS
        ))
        .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED));
    }
    let mut labels: Vec<&[u8]> = Vec::new();
    for l in ta.levels() {
        labels.push(l.name);
    }
    for l in tb.levels() {
        labels.push(l.name);
    }
    Ok(build_ltree(&labels))
}

pub fn ltree_index(a: &[u8], b: &[u8], start_in: Option<i32>) -> i32 {
    let ta = Ltree::new(a);
    let tb = Ltree::new(b);
    let an = ta.numlevel() as i32;
    let bn = tb.numlevel() as i32;
    let mut start = start_in.unwrap_or(0);

    // C ltree_op.c does this arithmetic in plain `int` and PostgreSQL builds
    // with -fwrapv, so every step here is a defined two's-complement wrap.
    // start = INT_MIN is SQL-reachable (ltree_index(a, b, -2147483648)) and
    // makes both `-start` and `an - start` overflow; a checked negate panics
    // the backend where C returns -1.
    if start < 0 {
        if start.wrapping_neg() >= an {
            start = 0;
        } else {
            start = an.wrapping_add(start);
        }
    }

    if an.wrapping_sub(start) < bn || an == 0 || bn == 0 {
        return -1;
    }

    let a_levels: Vec<&[u8]> = ta.levels().map(|l| l.name).collect();
    let b_levels: Vec<&[u8]> = tb.levels().map(|l| l.name).collect();

    let mut i = 0i32;
    let mut found = false;
    while i <= an - bn {
        if i >= start {
            let mut j = 0i32;
            while j < bn {
                if a_levels[(i + j) as usize] != b_levels[j as usize] {
                    break;
                }
                j += 1;
            }
            if j == bn {
                found = true;
                break;
            }
        }
        i += 1;
    }
    if !found {
        -1
    } else {
        i
    }
}

pub fn lca_inner(a: &[&[u8]]) -> Option<Vec<u8>> {
    let len = a.len();
    if len == 0 {
        return None;
    }
    let first = Ltree::new(a[0]);
    if first.numlevel() == 0 {
        return None;
    }
    let first_levels: Vec<&[u8]> = first.levels().map(|l| l.name).collect();

    // num = length of longest common ancestor so far
    let mut num = first.numlevel() - 1;

    for img in &a[1..] {
        let t = Ltree::new(img);
        let nl = t.numlevel();
        if nl == 0 {
            return None;
        } else if nl == 1 {
            num = 0;
        } else {
            let other_levels: Vec<&[u8]> = t.levels().map(|l| l.name).collect();
            let tmp = num.min(nl - 1);
            num = 0;
            for i in 0..tmp {
                if first_levels[i] == other_levels[i] {
                    num = i + 1;
                } else {
                    break;
                }
            }
        }
    }

    let labels: Vec<&[u8]> = first_levels[..num].to_vec();
    Some(build_ltree(&labels))
}


// upstream b3c2a3d386fa (18.4): Fix more multibyte issues in ltree.
/// C `ltree_label_match`: does `label` match the predicate `pred`? With
/// `prefix` ('*') the predicate is a prefix; with `ci` ('@') the comparison
/// is case-insensitive under the default collation's ctype.
fn ltree_label_match(pred: &[u8], label: &[u8], prefix: bool, ci: bool) -> bool {
    label_match_with(pred, label, prefix, ci, ::pg_locale::database_ctype_is_c(), fold)
}

/// The matcher over an explicit ctype flag and fold primitive. A casefold can
/// change the byte length, so the exact/prefix rule applies to FOLDED lengths.
fn label_match_with<F: Fn(&[u8]) -> Vec<u8>>(
    pred: &[u8],
    label: &[u8],
    prefix: bool,
    ci: bool,
    ctype_is_c: bool,
    fold: F,
) -> bool {
    if (pred.len() == label.len() || (prefix && pred.len() < label.len()))
        && label.starts_with(pred)
    {
        return true;
    } else if !ci {
        return false;
    }

    if ctype_is_c {
        // upstream 53a57cae1c89 (18.4): Yet another ltree fix for REL_18_STABLE.
        if pred.len() > label.len() || (!prefix && pred.len() != label.len()) {
            return false;
        }
        return pred
            .iter()
            .zip(label)
            .all(|(p, l)| p.to_ascii_lowercase() == l.to_ascii_lowercase());
    }

    let fpred = fold(pred);
    let flabel = fold(label);
    (fpred.len() == flabel.len() || (prefix && fpred.len() < flabel.len()))
        && flabel.starts_with(&fpred)
}

fn getlexeme(s: &[u8], mut start: usize) -> Option<(usize, usize)> {
    let end = s.len();
    // skip leading '_' (mblen-stepped, but '_' is single byte)
    while start < end && s[start] == b'_' {
        start += pg_mblen_range(s, start);
    }
    if start >= end {
        return None;
    }
    let mut ptr = start;
    while ptr < end && s[ptr] != b'_' {
        ptr += pg_mblen_range(s, ptr);
    }
    Some((start, ptr - start))
}

/// `pg_mblen_range(p, end)` — byte length of the char at offset `i` within `s`.
fn pg_mblen_range(s: &[u8], i: usize) -> usize {
    (::mbutils::pg_mblen(&s[i..]).max(1)) as usize
}

fn compare_subnode(t_name: &[u8], qn: &[u8], prefix: bool, ci: bool) -> bool {
    let mut qpos = 0usize;
    while let Some((qs, qlen)) = getlexeme(qn, qpos) {
        let q = &qn[qs..qs + qlen];
        let mut isok = false;
        let mut tpos = 0usize;
        while let Some((ts, tlen)) = getlexeme(t_name, tpos) {
            let tt = &t_name[ts..ts + tlen];
            if ltree_label_match(q, tt, prefix, ci) {
                isok = true;
                break;
            }
            tpos = ts + tlen;
        }
        if !isok {
            return false;
        }
        qpos = qs + qlen;
    }
    true
}

fn check_level(curq: &LqlView, t_name: &[u8]) -> bool {
    let success = curq.flag() & LQL_NOT == 0;
    if curq.numvar() == 0 {
        // '*' matches anything
        return success;
    }
    for v in curq.variants() {
        let prefix = v.flag & LVAR_ANYEND != 0;
        let ci = v.flag & LVAR_INCASE != 0;
        if v.flag & LVAR_SUBLEXEME != 0 {
            if compare_subnode(t_name, v.name, prefix, ci) {
                return success;
            }
        } else if ltree_label_match(v.name, t_name, prefix, ci) {
            return success;
        }
    }
    !success
}

// C CHECK_FOR_INTERRUPTS (heapam's gated-helper shape), unboxed to match
// check_cond's Result<_, PgError> error type.
#[inline(never)]
fn process_interrupts() -> Result<(), PgError> {
    postgres_seams::check_for_interrupts::call().map_err(|e| *e)
}

#[inline(always)]
fn check_for_interrupts() -> Result<(), PgError> {
    if init_small::globals::InterruptPending() {
        return process_interrupts();
    }
    Ok(())
}

fn check_cond(
    levels: &[LqlView],
    qi: usize,
    qlen: usize,
    t_names: &[&[u8]],
    ti: usize,
    tlen: usize,
) -> Result<bool, PgError> {
    // C lquery_op.c checkCond(): "Since this function recurses, it could be
    // driven to stack overflow" -> check_stack_depth(), plus
    // CHECK_FOR_INTERRUPTS() for "pathological patterns could take awhile"
    // (lquery_op.c:204-209).
    // The guard must be BYTE-based like C's: a frame-count cap cannot bound
    // stack bytes and at real frame sizes a 100_000-frame cap is unreachable
    // behind any backend stack, i.e. dead code.
    stack_depth::check_stack_depth()?;
    check_for_interrupts()?;
    let mut qi = qi;
    let mut qlen = qlen;
    let mut ti = ti;
    let mut tlen = tlen as i32;

    while qlen > 0 {
        let curq = &levels[qi];
        let (low, high0) = if (curq.flag() & LQL_COUNT != 0) || curq.numvar() == 0 {
            (curq.low() as i32, curq.high() as i32)
        } else {
            (1, 1)
        };
        let mut high = high0;
        if high > tlen {
            high = tlen;
        }
        if high < low {
            return Ok(false);
        }
        let nextqi = qi + 1;
        qlen -= 1;

        let mut matchcnt = 0i32;
        while matchcnt < high {
            if matchcnt >= low
                && check_cond(levels, nextqi, qlen, t_names, ti, tlen as usize)?
            {
                return Ok(true);
            }
            if !check_level(curq, t_names[ti]) {
                return Ok(false);
            }
            ti += 1;
            tlen -= 1;
            matchcnt += 1;
        }
        qi = nextqi;
    }
    Ok(tlen == 0)
}

pub fn ltq_regex(tree: &[u8], query: &[u8]) -> Result<bool, PgError> {
    let t = Ltree::new(tree);
    let q = Lquery::new(query);
    let t_names: Vec<&[u8]> = t.levels().map(|l| l.name).collect();
    let levels: Vec<LqlView> = q.levels().collect();
    check_cond(
        &levels,
        0,
        q.numlevel(),
        &t_names,
        0,
        t.numlevel(),
    )
}


fn checkcondition_str(t_names: &[&[u8]], operand: &[u8], it: &Item) -> bool {
    let start = it.distance as usize;
    // operand is NUL-terminated at op+distance; the C compares val->length bytes
    let oplen = it.length as usize;
    let op = &operand[start..start + oplen];
    let prefix = it.flag & LVAR_ANYEND != 0;
    let ci = it.flag & LVAR_INCASE != 0;
    let sublex = it.flag & LVAR_SUBLEXEME != 0;
    for name in t_names {
        if sublex {
            if compare_subnode(name, op, prefix, ci) {
                return true;
            }
        } else if ltree_label_match(op, *name, prefix, ci) {
            return true;
        }
    }
    false
}

fn ltree_execute(
    items: &[Item],
    cur: usize,
    t_names: &[&[u8]],
    operand: &[u8],
    calcnot: bool,
) -> Result<bool, PgError> {
    // C ltxtquery_op.c ltree_execute(): check_stack_depth(). Returning a bare
    // `false` on depth exhaustion (the previous frame-count shape) is a
    // SILENTLY WRONG answer where C raises 54001, so this propagates instead.
    stack_depth::check_stack_depth()?;
    let it = &items[cur];
    if it.typ as i32 == VAL {
        Ok(checkcondition_str(t_names, operand, it))
    } else if it.val == b'!' as i32 {
        if calcnot {
            Ok(!ltree_execute(items, cur + 1, t_names, operand, calcnot)?)
        } else {
            Ok(true)
        }
    } else if it.val == b'&' as i32 {
        if ltree_execute(items, cur + it.left as usize, t_names, operand, calcnot)? {
            ltree_execute(items, cur + 1, t_names, operand, calcnot)
        } else {
            Ok(false)
        }
    } else {
        // |-operator
        if ltree_execute(items, cur + it.left as usize, t_names, operand, calcnot)? {
            Ok(true)
        } else {
            ltree_execute(items, cur + 1, t_names, operand, calcnot)
        }
    }
}

pub fn ltxtq_exec(tree: &[u8], query: &[u8]) -> Result<bool, PgError> {
    let t = Ltree::new(tree);
    let q = Ltxtquery::new(query);
    let t_names: Vec<&[u8]> = t.levels().map(|l| l.name).collect();
    let items: Vec<Item> = (0..q.size()).map(|i| q.item(i)).collect();
    let operand = q.operand();
    ltree_execute(&items, 0, &t_names, operand, true)
}

pub fn ltxtq_exec_sign(
    query: &[u8],
    canlooksign: &dyn Fn(u8) -> bool,
    bit_set: &dyn Fn(i32) -> bool,
) -> Result<bool, PgError> {
    let q = Ltxtquery::new(query);
    let items: Vec<Item> = (0..q.size()).map(|i| q.item(i)).collect();
    ltree_execute_sign(&items, 0, canlooksign, bit_set)
}

/// `ltree_execute` with the `checkcondition_bit` callback and `calcnot = false`.
fn ltree_execute_sign(
    items: &[Item],
    cur: usize,
    canlooksign: &dyn Fn(u8) -> bool,
    bit_set: &dyn Fn(i32) -> bool,
) -> Result<bool, PgError> {
    // C ltxtquery_op.c ltree_execute() via checkcondition_bit (see above).
    stack_depth::check_stack_depth()?;
    let it = &items[cur];
    if it.typ as i32 == VAL {
        // checkcondition_bit: FLG_CANLOOKSIGN(val->flag) ? GETBIT(sign, HASHVAL(val->val)) : true
        if canlooksign(it.flag) {
            Ok(bit_set(it.val))
        } else {
            Ok(true)
        }
    } else if it.val == b'!' as i32 {
        // calcnot == false -> a NOT node optimistically matches.
        Ok(true)
    } else if it.val == b'&' as i32 {
        if ltree_execute_sign(items, cur + it.left as usize, canlooksign, bit_set)? {
            ltree_execute_sign(items, cur + 1, canlooksign, bit_set)
        } else {
            Ok(false)
        }
    } else {
        // |-operator
        if ltree_execute_sign(items, cur + it.left as usize, canlooksign, bit_set)? {
            Ok(true)
        } else {
            ltree_execute_sign(items, cur + 1, canlooksign, bit_set)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A byte-length-changing fold: KELVIN SIGN (U+212A, 3 bytes) -> 'k'.
    fn fold_kelvin(s: &[u8]) -> Vec<u8> {
        let kelvin = "\u{212A}".as_bytes();
        let mut out = Vec::new();
        let mut i = 0;
        while i < s.len() {
            if s[i..].starts_with(kelvin) {
                out.push(b'k');
                i += kelvin.len();
            } else {
                out.push(s[i].to_ascii_lowercase());
                i += 1;
            }
        }
        out
    }

    /// upstream b3c2a3d386fa: raw byte lengths never decide a '@' match.
    #[test]
    fn label_match_applies_length_rule_to_folded_strings() {
        let kelvin = "\u{212A}".as_bytes();
        let m = |pred: &[u8], label: &[u8], prefix: bool, ci: bool| {
            label_match_with(pred, label, prefix, ci, false, fold_kelvin)
        };
        // predicate shorter than the label in bytes, equal once folded
        assert!(m(b"k", kelvin, false, true));
        assert!(m(b"k", kelvin, true, true));
        // predicate longer than the label in bytes, equal once folded
        assert!(m(kelvin, b"k", false, true));
        // folded prefix rule: 'K' (3 bytes) is a prefix of 'kx' (2 bytes)
        assert!(m(kelvin, b"kx", true, true));
        assert!(!m(kelvin, b"kx", false, true));
        assert!(!m(b"kx", kelvin, true, true));
        // binary predicates stay binary
        assert!(!m(b"k", kelvin, false, false));
        assert!(!m(kelvin, b"k", true, false));
        assert!(m(b"ab", b"abc", true, false));
        assert!(!m(b"ab", b"abc", false, false));
        assert!(m(b"abc", b"abc", false, false));
    }

    /// upstream 53a57cae1c89: the C-ctype arm keeps the exact-length rule for
    /// non-prefix predicates ('abc' does not match 'ab@').
    #[test]
    fn label_match_c_ctype_keeps_exact_length_rule() {
        let m = |pred: &[u8], label: &[u8], prefix: bool, ci: bool| {
            label_match_with(pred, label, prefix, ci, true, |s: &[u8]| s.to_vec())
        };
        assert!(!m(b"ab", b"abc", false, true));
        assert!(m(b"ab", b"abc", true, true));
        assert!(m(b"AB", b"abc", true, true));
        assert!(m(b"abc", b"ABC", false, true));
        assert!(!m(b"abc", b"ab", false, true));
        assert!(!m(b"abc", b"ab", true, true));
        assert!(!m(b"ab", b"abc", false, false));
    }

    /// upstream c3e36a9a5f19: the GiST penalty's distance keeps C's
    /// `delta * 10 * (an + 1)` scaling, computed in float.
    #[test]
    fn ltree_compare_distance_keeps_c_scaling() {
        let t = |s: &str| crate::io::parse_ltree(s.as_bytes()).unwrap();
        let deep = t(&format!("{}a", "a.".repeat(14999)));
        assert_eq!(ltree_compare_distance(&deep, &t("a")), (14999.0f64 * 10.0 * 15000.0) as f32);
        // one equal level leaves an = 0 on the short side: (1 - 15000) * 10 * 1
        assert_eq!(ltree_compare_distance(&t("a"), &deep), -149990.0);
        assert_eq!(ltree_compare_distance(&t("a.b"), &t("a.z")), -20.0);
        assert_eq!(ltree_compare_distance(&t("a.z"), &t("a.b")), 20.0);
        assert_eq!(ltree_compare_distance(&t("a.bb"), &t("a.b")), 20.0);
        assert_eq!(ltree_compare_distance(&t("a"), &t("a.b.c")), -20.0);
        assert_eq!(ltree_compare_distance(&t("a.b"), &t("a.b")), 0.0);
    }
}
