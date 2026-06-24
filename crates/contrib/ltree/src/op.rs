//! `contrib/ltree/ltree_op.c`, `lquery_op.c`, `ltxtquery_op.c` — the scalar
//! operators/functions over `ltree` / `lquery` / `ltxtquery` (everything that
//! works on a sequential scan). Each function operates on the borrowed varlena
//! images; results are freshly built images.

use ::types_error::error::{
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_PROGRAM_LIMIT_EXCEEDED,
};
use ::types_error::PgError;

use crate::crc::fold;
use crate::repr::*;

// ---------------------------------------------------------------------------
// ltree compare / hash
// ---------------------------------------------------------------------------

/// `ltree_compare(a, b)`.
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
        let minlen = al.name.len().min(bl.name.len());
        let res = al.name[..minlen].cmp(&bl.name[..minlen]);
        match res {
            core::cmp::Ordering::Equal => {
                if al.name.len() != bl.name.len() {
                    return (al.name.len() as i32 - bl.name.len() as i32) * 10 * (an + 1);
                }
            }
            core::cmp::Ordering::Less => return -10 * (an + 1),
            core::cmp::Ordering::Greater => return 10 * (an + 1),
        }
        an -= 1;
        bn -= 1;
    }
    (ta.numlevel() as i32 - tb.numlevel() as i32) * 10 * (an + 1)
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

/// `hash_ltree_extended(a, seed)`.
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

/// `nlevel(a)`.
pub fn nlevel(a: &[u8]) -> i32 {
    Ltree::new(a).numlevel() as i32
}

/// `inner_isparent(c, p)` — is `p` an ancestor-or-equal prefix of `c`?
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

// ---------------------------------------------------------------------------
// subltree / subpath
// ---------------------------------------------------------------------------

/// `inner_subltree(t, startpos, endpos)`.
pub fn inner_subltree(t: &[u8], startpos: i32, endpos_in: i32) -> Result<Vec<u8>, PgError> {
    let tt = Ltree::new(t);
    let numlevel = tt.numlevel() as i32;
    if startpos < 0 || endpos_in < 0 || startpos >= numlevel || startpos > endpos_in {
        return Err(PgError::error("invalid positions").with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    }
    let endpos = endpos_in.min(numlevel);

    // Collect level name slices in [startpos, endpos).
    let labels: Vec<&[u8]> = tt
        .levels()
        .skip(startpos as usize)
        .take((endpos - startpos) as usize)
        .map(|l| l.name)
        .collect();
    Ok(build_ltree(&labels))
}

/// `subpath(t, start, len?)`.
pub fn subpath(t: &[u8], start_in: i32, len_opt: Option<i32>) -> Result<Vec<u8>, PgError> {
    let numlevel = Ltree::new(t).numlevel() as i32;
    let len = len_opt.unwrap_or(0);
    let three = len_opt.is_some();
    let mut start = start_in;
    let mut end = start + len;

    if start < 0 {
        start = numlevel + start;
        end = start + len;
    }
    if start < 0 {
        start = numlevel + start;
        end = start + len;
    }
    if len < 0 {
        end = numlevel + len;
    } else if len == 0 {
        end = if three { start } else { 0xffff };
    }
    inner_subltree(t, start, end)
}

/// `ltree_concat(a, b)` — append two ltrees.
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

/// `ltree_index(a, b, start?)` — find `b` as a contiguous sub-path of `a`.
pub fn ltree_index(a: &[u8], b: &[u8], start_in: Option<i32>) -> i32 {
    let ta = Ltree::new(a);
    let tb = Ltree::new(b);
    let an = ta.numlevel() as i32;
    let bn = tb.numlevel() as i32;
    let mut start = start_in.unwrap_or(0);

    if start < 0 {
        if -start >= an {
            start = 0;
        } else {
            start = an + start;
        }
    }

    if an - start < bn || an == 0 || bn == 0 {
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

/// `lca_inner(a_list)` — longest common ancestor. Returns `None` for the C
/// NULL result (empty LCA).
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

// ---------------------------------------------------------------------------
// lquery matching (lquery_op.c)
// ---------------------------------------------------------------------------

/// `ltree_prefix_eq(a, b)` — is `a` a prefix of `b`? (byte exact)
fn prefix_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() <= b.len() && b[..a.len()] == *a
}

/// `ltree_prefix_eq_ci(a, b)` — case-insensitive prefix (case-fold both).
fn prefix_eq_ci(a: &[u8], b: &[u8]) -> bool {
    let al = fold(a);
    let bl = fold(b);
    al.len() <= bl.len() && bl[..al.len()] == *al
}

/// Dispatch prefix-eq by the INCASE flag.
fn prefix_eq_dispatch(incase: bool, a: &[u8], b: &[u8]) -> bool {
    if incase {
        prefix_eq_ci(a, b)
    } else {
        prefix_eq(a, b)
    }
}

/// `getlexeme` — split on '_' boundaries: return the next lexeme slice.
/// Returns `(start, len)` relative to the input `s`, or None at end.
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

/// `compare_subnode(t_name, qn, prefix_eq, anyend)`.
fn compare_subnode(t_name: &[u8], qn: &[u8], incase: bool, anyend: bool) -> bool {
    let mut qpos = 0usize;
    while let Some((qs, qlen)) = getlexeme(qn, qpos) {
        let q = &qn[qs..qs + qlen];
        let mut isok = false;
        let mut tpos = 0usize;
        while let Some((ts, tlen)) = getlexeme(t_name, tpos) {
            let tt = &t_name[ts..ts + tlen];
            if (tlen == qlen || (tlen > qlen && anyend)) && prefix_eq_dispatch(incase, q, tt) {
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

/// `checkLevel(curq, curt)`.
fn check_level(curq: &LqlView, t_name: &[u8]) -> bool {
    let success = curq.flag() & LQL_NOT == 0;
    if curq.numvar() == 0 {
        // '*' matches anything
        return success;
    }
    for v in curq.variants() {
        let incase = v.flag & LVAR_INCASE != 0;
        let anyend = v.flag & LVAR_ANYEND != 0;
        if v.flag & LVAR_SUBLEXEME != 0 {
            if compare_subnode(t_name, v.name, incase, anyend) {
                return success;
            }
        } else if (v.name.len() == t_name.len()
            || (t_name.len() > v.name.len() && anyend))
            && prefix_eq_dispatch(incase, v.name, t_name)
        {
            return success;
        }
    }
    !success
}

/// `checkCond(curq, qlen, curt, tlen)` — recursive lquery match.
fn check_cond(
    levels: &[LqlView],
    qi: usize,
    qlen: usize,
    t_names: &[&[u8]],
    ti: usize,
    tlen: usize,
    depth: u32,
) -> Result<bool, PgError> {
    if depth > 100_000 {
        return Err(PgError::error("stack depth limit exceeded")
            .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED));
    }
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
                && check_cond(levels, nextqi, qlen, t_names, ti, tlen as usize, depth + 1)?
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

/// `ltq_regex(tree, query)` — `tree ~ query`.
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
        0,
    )
}

// ---------------------------------------------------------------------------
// ltxtquery matching (ltxtquery_op.c)
// ---------------------------------------------------------------------------

/// `checkcondition_str` — does any ltree level satisfy the ITEM?
fn checkcondition_str(t_names: &[&[u8]], operand: &[u8], it: &Item) -> bool {
    let start = it.distance as usize;
    // operand is NUL-terminated at op+distance; the C compares val->length bytes
    let oplen = it.length as usize;
    let op = &operand[start..start + oplen];
    let incase = it.flag & LVAR_INCASE != 0;
    let anyend = it.flag & LVAR_ANYEND != 0;
    let sublex = it.flag & LVAR_SUBLEXEME != 0;
    for name in t_names {
        if sublex {
            if compare_subnode(name, op, incase, anyend) {
                return true;
            }
        } else if (oplen == name.len() || (name.len() > oplen && anyend))
            && prefix_eq_dispatch(incase, op, name)
        {
            return true;
        }
    }
    false
}

/// `ltree_execute` — evaluate the polish-notation tree.
fn ltree_execute(
    items: &[Item],
    cur: usize,
    t_names: &[&[u8]],
    operand: &[u8],
    calcnot: bool,
    depth: u32,
) -> bool {
    if depth > 100_000 {
        return false;
    }
    let it = &items[cur];
    if it.typ as i32 == VAL {
        checkcondition_str(t_names, operand, it)
    } else if it.val == b'!' as i32 {
        if calcnot {
            !ltree_execute(items, cur + 1, t_names, operand, calcnot, depth + 1)
        } else {
            true
        }
    } else if it.val == b'&' as i32 {
        if ltree_execute(items, cur + it.left as usize, t_names, operand, calcnot, depth + 1) {
            ltree_execute(items, cur + 1, t_names, operand, calcnot, depth + 1)
        } else {
            false
        }
    } else {
        // |-operator
        if ltree_execute(items, cur + it.left as usize, t_names, operand, calcnot, depth + 1) {
            true
        } else {
            ltree_execute(items, cur + 1, t_names, operand, calcnot, depth + 1)
        }
    }
}

/// `ltxtq_exec(tree, query)` — `tree @ query`.
pub fn ltxtq_exec(tree: &[u8], query: &[u8]) -> bool {
    let t = Ltree::new(tree);
    let q = Ltxtquery::new(query);
    let t_names: Vec<&[u8]> = t.levels().map(|l| l.name).collect();
    let items: Vec<Item> = (0..q.size()).map(|i| q.item(i)).collect();
    let operand = q.operand();
    ltree_execute(&items, 0, &t_names, operand, true, 0)
}

/// `ltree_execute(GETQUERY(query), &sig, false, checkcondition_bit)` — the GiST
/// inner-node ltxtquery evaluator. Mirrors `gist_qtxt`'s use of `ltree_execute`
/// with `calcnot = false` (NOT-nodes optimistically match an inner key) and a
/// signature-bitmap leaf check: each VAL `ITEM` is "present" iff its CRC's bit
/// is set in `sign` (when the operand can look at the sign — `FLG_CANLOOKSIGN`).
///
/// `getbit(i)` returns the i-th signature bit; `hashval(crc)` maps an ITEM's CRC
/// to its bit index. This keeps the bit-math (HASHVAL with the opclass's
/// `SIGLENBIT`) in the GiST module while reusing the polish-notation walk.
pub fn ltxtq_exec_sign(
    query: &[u8],
    canlooksign: &dyn Fn(u8) -> bool,
    bit_set: &dyn Fn(i32) -> bool,
) -> bool {
    let q = Ltxtquery::new(query);
    let items: Vec<Item> = (0..q.size()).map(|i| q.item(i)).collect();
    ltree_execute_sign(&items, 0, canlooksign, bit_set, 0)
}

/// `ltree_execute` with the `checkcondition_bit` callback and `calcnot = false`.
fn ltree_execute_sign(
    items: &[Item],
    cur: usize,
    canlooksign: &dyn Fn(u8) -> bool,
    bit_set: &dyn Fn(i32) -> bool,
    depth: u32,
) -> bool {
    if depth > 100_000 {
        return false;
    }
    let it = &items[cur];
    if it.typ as i32 == VAL {
        // checkcondition_bit: FLG_CANLOOKSIGN(val->flag) ? GETBIT(sign, HASHVAL(val->val)) : true
        if canlooksign(it.flag) {
            bit_set(it.val)
        } else {
            true
        }
    } else if it.val == b'!' as i32 {
        // calcnot == false → a NOT node optimistically matches.
        true
    } else if it.val == b'&' as i32 {
        if ltree_execute_sign(items, cur + it.left as usize, canlooksign, bit_set, depth + 1) {
            ltree_execute_sign(items, cur + 1, canlooksign, bit_set, depth + 1)
        } else {
            false
        }
    } else {
        // |-operator
        if ltree_execute_sign(items, cur + it.left as usize, canlooksign, bit_set, depth + 1) {
            true
        } else {
            ltree_execute_sign(items, cur + 1, canlooksign, bit_set, depth + 1)
        }
    }
}
