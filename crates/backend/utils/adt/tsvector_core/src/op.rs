use ::mcx::{vec_with_capacity_in, Mcx, PgVec};
use ::types_error::{PgError, PgResult, ERRCODE_PROGRAM_LIMIT_EXCEEDED};

use crate::execute::{ExecPhraseData, Ternary};
use crate::layout::*;
use crate::query::{Operand, TsQueryRef};

pub fn silly_cmp_tsvector(a: TsVec<'_>, b: TsVec<'_>) -> i32 {
    if a.payload.len() != b.payload.len() {
        return if a.payload.len() < b.payload.len() { -1 } else { 1 };
    }
    if a.size() != b.size() {
        return if a.size() < b.size() { -1 } else { 1 };
    }
    for i in 0..a.size() {
        let ea = a.entry(i);
        let eb = b.entry(i);
        if ea.haspos() != eb.haspos() {
            return if ea.haspos() { -1 } else { 1 };
        }
        let res = ts_compare_string(a.lexeme(ea), b.lexeme(eb), false);
        if res != 0 {
            return res;
        }
        if ea.haspos() {
            let pa = a.positions(ea);
            let pb = b.positions(eb);
            if pa.len() != pb.len() {
                return if pa.len() > pb.len() { -1 } else { 1 };
            }
            for (x, y) in pa.iter().zip(pb.iter()) {
                if wep_getpos(*x) != wep_getpos(*y) {
                    return if wep_getpos(*x) > wep_getpos(*y) { -1 } else { 1 };
                }
                if wep_getweight(*x) != wep_getweight(*y) {
                    return if wep_getweight(*x) > wep_getweight(*y) { -1 } else { 1 };
                }
            }
        }
    }
    0
}

pub fn tsvector_strip_core<'mcx>(mcx: Mcx<'mcx>, v: TsVec<'_>) -> PgResult<PgVec<'mcx, u8>> {
    let mut b = TsVecBuilder::with_capacity(mcx, v.size(), v.payload.len())?;
    for i in 0..v.size() {
        let e = v.entry(i);
        b.push(v.lexeme(e), &[])?;
    }
    b.finish(mcx)
}

pub fn weight_code(cw: u8) -> PgResult<u16> {
    match cw {
        b'A' | b'a' => Ok(3),
        b'B' | b'b' => Ok(2),
        b'C' | b'c' => Ok(1),
        b'D' | b'd' => Ok(0),
        other => Err(PgError::error(format!("unrecognized weight: {}", other as i8)).into()),
    }
}

pub fn tsvector_setweight_core<'mcx>(
    mcx: Mcx<'mcx>,
    v: TsVec<'_>,
    w: u16,
) -> PgResult<PgVec<'mcx, u8>> {
    let mut img = vec_with_capacity_in(mcx, v.payload.len() + 4)?;
    img.extend_from_slice(&[0u8; 4]);
    ::mcx::vec_append_bytes(&mut img, v.payload)?;
    let out = TsVec { payload: &img[4..] };
    let str_off = out.str_off();
    let mut spans: PgVec<(usize, usize)> = PgVec::new_in(mcx);
    for i in 0..out.size() {
        let e = out.entry(i);
        if e.haspos() {
            let off = str_off + shortalign(e.pos() + e.len());
            let npos =
                u16::from_ne_bytes(out.payload[off..off + 2].try_into().unwrap()) as usize;
            spans.push((4 + off + 2, npos));
        }
    }
    for (start, npos) in spans {
        for j in 0..npos {
            let o = start + j * 2;
            let mut p = u16::from_ne_bytes(img[o..o + 2].try_into().unwrap());
            wep_setweight(&mut p, w);
            img[o..o + 2].copy_from_slice(&p.to_ne_bytes());
        }
    }
    Ok(img)
}

pub fn tsvector_bsearch(v: TsVec<'_>, lexeme: &[u8]) -> Option<usize> {
    let mut lo = 0usize;
    let mut hi = v.size();
    while lo < hi {
        let mid = (lo + hi) / 2;
        let cmp = ts_compare_string(lexeme, v.lexeme(v.entry(mid)), false);
        if cmp < 0 {
            hi = mid;
        } else if cmp > 0 {
            lo = mid + 1;
        } else {
            return Some(mid);
        }
    }
    None
}

pub fn tsvector_setweight_by_filter_core<'mcx>(
    mcx: Mcx<'mcx>,
    v: TsVec<'_>,
    w: u16,
    lexemes: &[&[u8]],
) -> PgResult<PgVec<'mcx, u8>> {
    let mut img = vec_with_capacity_in(mcx, v.payload.len() + 4)?;
    img.extend_from_slice(&[0u8; 4]);
    ::mcx::vec_append_bytes(&mut img, v.payload)?;
    let mut spans: PgVec<(usize, usize)> = PgVec::new_in(mcx);
    {
        let out = TsVec { payload: &img[4..] };
        let str_off = out.str_off();
        for lex in lexemes {
            if let Some(i) = tsvector_bsearch(out, lex) {
                let e = out.entry(i);
                if e.haspos() {
                    let off = str_off + shortalign(e.pos() + e.len());
                    let npos =
                        u16::from_ne_bytes(out.payload[off..off + 2].try_into().unwrap())
                            as usize;
                    spans.push((4 + off + 2, npos));
                }
            }
        }
    }
    for (start, npos) in spans {
        for j in 0..npos {
            let o = start + j * 2;
            let mut p = u16::from_ne_bytes(img[o..o + 2].try_into().unwrap());
            wep_setweight(&mut p, w);
            img[o..o + 2].copy_from_slice(&p.to_ne_bytes());
        }
    }
    Ok(img)
}

pub fn tsvector_delete_by_indices<'mcx>(
    mcx: Mcx<'mcx>,
    v: TsVec<'_>,
    skip: &mut PgVec<'_, usize>,
) -> PgResult<PgVec<'mcx, u8>> {
    skip.sort_unstable();
    skip.dedup();
    let mut b = TsVecBuilder::with_capacity(mcx, v.size().saturating_sub(skip.len()), v.payload.len())?;
    let mut k = 0usize;
    for i in 0..v.size() {
        if k < skip.len() && i == skip[k] {
            k += 1;
            continue;
        }
        let e = v.entry(i);
        b.push_raw(v.lexeme(e), v.posblock(e))?;
    }
    b.finish(mcx)
}

// add_pos: append src positions offset by maxpos, respecting MAXNUMPOS and the
// position ceiling; dest may already hold positions.
fn add_pos(
    dest: &mut PgVec<'_, WordEntryPos>,
    src: &[WordEntryPos],
    maxpos: u32,
) -> usize {
    let startlen = dest.len();
    for &sp in src {
        if dest.len() >= MAXNUMPOS
            || (!dest.is_empty() && wep_getpos(dest[dest.len() - 1]) as u32 == MAXENTRYPOS - 1)
        {
            break;
        }
        let mut p: WordEntryPos = 0;
        wep_setweight(&mut p, wep_getweight(sp));
        wep_setpos(&mut p, limitpos(wep_getpos(sp) as u32 + maxpos));
        dest.push(p);
    }
    dest.len() - startlen
}

pub fn tsvector_concat_core<'mcx>(
    mcx: Mcx<'mcx>,
    in1: TsVec<'_>,
    in2: TsVec<'_>,
) -> PgResult<PgVec<'mcx, u8>> {
    let mut maxpos = 0u32;
    for i in 0..in1.size() {
        for &p in in1.positions(in1.entry(i)) {
            maxpos = maxpos.max(wep_getpos(p) as u32);
        }
    }

    let mut b = TsVecBuilder::with_capacity(
        mcx,
        in1.size() + in2.size(),
        in1.payload.len() + in2.payload.len(),
    )?;
    let mut scratch: PgVec<WordEntryPos> = PgVec::new_in(mcx);
    let (mut i1, mut i2) = (0usize, 0usize);
    while i1 < in1.size() && i2 < in2.size() {
        let e1 = in1.entry(i1);
        let e2 = in2.entry(i2);
        let cmp = ts_compare_string(in1.lexeme(e1), in2.lexeme(e2), false);
        if cmp < 0 {
            b.push_raw(in1.lexeme(e1), in1.posblock(e1))?;
            i1 += 1;
        } else if cmp > 0 {
            scratch.clear();
            add_pos(&mut scratch, in2.positions(e2), maxpos);
            b.push(in2.lexeme(e2), &scratch)?;
            i2 += 1;
        } else {
            scratch.clear();
            scratch.extend_from_slice(in1.positions(e1));
            add_pos(&mut scratch, in2.positions(e2), maxpos);
            b.push(in1.lexeme(e1), &scratch)?;
            i1 += 1;
            i2 += 1;
        }
    }
    while i1 < in1.size() {
        let e1 = in1.entry(i1);
        b.push_raw(in1.lexeme(e1), in1.posblock(e1))?;
        i1 += 1;
    }
    while i2 < in2.size() {
        let e2 = in2.entry(i2);
        scratch.clear();
        add_pos(&mut scratch, in2.positions(e2), maxpos);
        b.push(in2.lexeme(e2), &scratch)?;
        i2 += 1;
    }

    if b.strlen() > MAXSTRPOS {
        return Err(PgError::error(format!(
            "string is too long for tsvector ({} bytes, max {MAXSTRPOS} bytes)",
            b.strlen()
        ))
        .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
        .into());
    }
    b.finish(mcx)
}

pub fn tsvector_filter_core<'mcx>(
    mcx: Mcx<'mcx>,
    v: TsVec<'_>,
    mask: u8,
) -> PgResult<PgVec<'mcx, u8>> {
    let mut b = TsVecBuilder::with_capacity(mcx, v.size(), v.payload.len())?;
    let mut keep: PgVec<WordEntryPos> = PgVec::new_in(mcx);
    for i in 0..v.size() {
        let e = v.entry(i);
        if !e.haspos() {
            continue;
        }
        keep.clear();
        for &p in v.positions(e) {
            if mask & (1 << wep_getweight(p)) != 0 {
                keep.push(p);
            }
        }
        if keep.is_empty() {
            continue;
        }
        b.push(v.lexeme(e), &keep)?;
    }
    b.finish(mcx)
}

// checkcondition_str over a tsvector image (chkval of ts_match_vq).
pub fn checkcondition_str<'mcx>(
    mcx: Mcx<'mcx>,
    v: TsVec<'_>,
    q: TsQueryRef<'_>,
    val: &Operand,
    mut data: Option<&mut ExecPhraseData<'mcx>>,
) -> Ternary {
    let operand = q.operand_str(val);

    let mut lo = 0usize;
    let mut hi = v.size();
    let mut mid = hi;
    let mut res = Ternary::No;
    while lo < hi {
        mid = lo + (hi - lo) / 2;
        let difference = ts_compare_string(operand, v.lexeme(v.entry(mid)), false);
        if difference == 0 {
            res = checkclass_str(v, v.entry(mid), val, data.as_deref_mut());
            break;
        } else if difference > 0 {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }

    if val.prefix && (res != Ternary::Yes || data.is_some()) {
        if lo >= hi {
            mid = hi;
        }
        if let Some(d) = data.as_deref_mut() {
            d.pos.clear();
        }
        res = Ternary::No;
        let mut allpos: PgVec<WordEntryPos> = PgVec::new_in(mcx);
        let mut maybe_broke = false;
        while (res != Ternary::Yes || data.is_some())
            && mid < v.size()
            && ts_compare_string(operand, v.lexeme(v.entry(mid)), true) == 0
        {
            let subres = checkclass_str(v, v.entry(mid), val, data.as_deref_mut());
            if subres != Ternary::No {
                if let Some(d) = data.as_deref_mut() {
                    if subres == Ternary::Maybe {
                        res = Ternary::Maybe;
                        allpos.clear();
                        maybe_broke = true;
                        d.pos.clear();
                        break;
                    }
                    allpos.extend_from_slice(&d.pos);
                    d.pos.clear();
                } else if subres == Ternary::Yes || res == Ternary::No {
                    res = subres;
                }
            }
            mid += 1;
        }
        if let Some(d) = data {
            if !maybe_broke && !allpos.is_empty() {
                allpos.sort_by_key(|&p| wep_getpos(p));
                allpos.dedup_by_key(|p| wep_getpos(*p));
                d.pos.clear();
                d.pos.extend_from_slice(&allpos);
                res = Ternary::Yes;
            }
        }
    }

    res
}

fn checkclass_str<'mcx>(
    v: TsVec<'_>,
    e: WordEntry,
    val: &Operand,
    data: Option<&mut ExecPhraseData<'mcx>>,
) -> Ternary {
    if e.haspos() {
        let posvec = v.positions(e);
        match (val.weight != 0, data) {
            (true, Some(d)) => {
                for &p in posvec {
                    if val.weight & (1 << wep_getweight(p)) != 0 {
                        d.pos.push(wep_getpos(p));
                    }
                }
                if d.pos.is_empty() {
                    Ternary::No
                } else {
                    Ternary::Yes
                }
            }
            (true, None) => {
                for &p in posvec {
                    if val.weight & (1 << wep_getweight(p)) != 0 {
                        return Ternary::Yes;
                    }
                }
                Ternary::No
            }
            (false, Some(d)) => {
                d.pos.extend_from_slice(posvec);
                Ternary::Yes
            }
            (false, None) => Ternary::Yes,
        }
    } else if data.is_some() {
        Ternary::Maybe
    } else {
        Ternary::Yes
    }
}

pub fn ts_match_vq_core<'mcx>(mcx: Mcx<'mcx>, v: TsVec<'_>, q: TsQueryRef<'_>) -> PgResult<bool> {
    if q.size() == 0 {
        return Ok(false);
    }
    let mut chk = |_idx: usize, val: &Operand, data: Option<&mut ExecPhraseData<'mcx>>| {
        Ok(checkcondition_str(mcx, v, q, val, data))
    };
    crate::execute::ts_execute(mcx, q, crate::execute::TS_EXEC_EMPTY, &mut chk)
}

pub fn tsquery_requires_match(q: TsQueryRef<'_>, idx: usize) -> bool {
    use crate::query::{Item, OP_AND, OP_NOT, OP_OR, OP_PHRASE};
    match q.item(idx) {
        Item::Val(_) => true,
        Item::ValStop => panic!("tsquery_requires_match: QI_VALSTOP in stored tsquery"),
        Item::Opr(opr) => match opr.oper {
            OP_NOT => false,
            OP_PHRASE | OP_AND => {
                tsquery_requires_match(q, idx + opr.left as usize)
                    || tsquery_requires_match(q, idx + 1)
            }
            OP_OR => {
                tsquery_requires_match(q, idx + opr.left as usize)
                    && tsquery_requires_match(q, idx + 1)
            }
            other => panic!("unrecognized operator: {other}"),
        },
    }
}
