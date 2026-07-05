use ::adt_tsvector_core::execute::{ts_execute, ExecPhraseData, Ternary, TS_EXEC_EMPTY};
use ::adt_tsvector_core::layout::*;
use ::adt_tsvector_core::query::{Item, TsQueryRef};
use ::mcx::{vec_with_capacity_in, Mcx, PgVec};
use ::types_error::PgResult;

use crate::rank::{cnt_length, find_wordentry, DEFAULT_WEIGHTS, NUM_WEIGHTS};
use crate::rank::{
    RANK_NORM_EXTDIST, RANK_NORM_LENGTH, RANK_NORM_LOGLENGTH, RANK_NORM_LOGUNIQ, RANK_NORM_RDIVRPLUS1,
    RANK_NORM_UNIQ,
};

struct DocRep<'mcx> {
    pos: WordEntryPos,
    entry: usize,
    items: PgVec<'mcx, usize>,
}

struct QrOperand<'mcx> {
    exists: bool,
    reverseinsert: bool,
    pos: PgVec<'mcx, WordEntryPos>,
}

struct QueryRep<'a, 'mcx> {
    q: TsQueryRef<'a>,
    // operand data per query item index; distance -> item index resolves the
    // C QR_GET_OPERAND_DATA pointer arithmetic.
    op_data: PgVec<'mcx, QrOperand<'mcx>>,
    by_distance: PgVec<'mcx, (usize, usize)>,
}

impl<'a, 'mcx> QueryRep<'a, 'mcx> {
    fn reset(&mut self, reverseinsert: bool) {
        for od in self.op_data.iter_mut() {
            od.exists = false;
            od.reverseinsert = reverseinsert;
            od.pos.clear();
        }
    }

    fn item_index(&self, distance: usize) -> usize {
        let i = self
            .by_distance
            .binary_search_by_key(&distance, |&(d, _)| d)
            .expect("operand distance maps to a query item");
        self.by_distance[i].1
    }

    fn fill(&mut self, entry: &DocRep<'_>) {
        for &item_idx in entry.items.iter() {
            let od = &mut self.op_data[item_idx];
            od.exists = true;
            match od.pos.last() {
                None => od.pos.push(entry.pos),
                Some(&last) => {
                    if wep_getpos(last) != wep_getpos(entry.pos) {
                        od.pos.push(entry.pos);
                    }
                }
            }
        }
    }
}

fn check_query_rep(
    qr: &QueryRep<'_, '_>,
    val_distance: usize,
    data: Option<&mut ExecPhraseData<'_>>,
) -> Ternary {
    let od = &qr.op_data[qr.item_index(val_distance)];
    if !od.exists {
        return Ternary::No;
    }
    if let Some(d) = data {
        d.pos.clear();
        if od.reverseinsert {
            for &p in od.pos.iter().rev() {
                d.pos.push(p);
            }
        } else {
            d.pos.extend_from_slice(&od.pos);
        }
    }
    Ternary::Yes
}

struct CoverExt {
    pos: usize,
    p: i32,
    q: i32,
    begin: usize,
    end: usize,
}

fn cover(
    mcx: Mcx<'_>,
    doc: &[DocRep<'_>],
    qr: &mut QueryRep<'_, '_>,
    ext: &mut CoverExt,
) -> PgResult<bool> {
    loop {
        let mut lastpos = ext.pos;
        let mut found = false;

        qr.reset(false);
        ext.p = i32::MAX;
        ext.q = 0;
        let mut ptr = ext.pos;

        while ptr < doc.len() {
            qr.fill(&doc[ptr]);
            let matched = {
                let q = qr.q;
                let mut chk = |_idx: usize,
                               val: &::adt_tsvector_core::query::Operand,
                               data: Option<&mut ExecPhraseData<'_>>| {
                    Ok(check_query_rep(qr, val.distance, data))
                };
                ts_execute(mcx, q, TS_EXEC_EMPTY, &mut chk)?
            };
            if matched {
                if wep_getpos(doc[ptr].pos) as i32 > ext.q {
                    ext.q = wep_getpos(doc[ptr].pos) as i32;
                    ext.end = ptr;
                    lastpos = ptr;
                    found = true;
                }
                break;
            }
            ptr += 1;
        }

        if !found {
            return Ok(false);
        }

        qr.reset(true);
        let mut ptr = lastpos as isize;
        while ptr >= ext.pos as isize {
            qr.fill(&doc[ptr as usize]);
            let matched = {
                let q = qr.q;
                let mut chk = |_idx: usize,
                               val: &::adt_tsvector_core::query::Operand,
                               data: Option<&mut ExecPhraseData<'_>>| {
                    Ok(check_query_rep(qr, val.distance, data))
                };
                ts_execute(mcx, q, TS_EXEC_EMPTY, &mut chk)?
            };
            if matched {
                if (wep_getpos(doc[ptr as usize].pos) as i32) < ext.p {
                    ext.begin = ptr as usize;
                    ext.p = wep_getpos(doc[ptr as usize].pos) as i32;
                }
                break;
            }
            ptr -= 1;
        }

        if ext.p <= ext.q {
            ext.pos = (ptr + 1) as usize;
            return Ok(true);
        }
        ext.pos += 1;
    }
}

fn get_docrep<'mcx>(
    mcx: Mcx<'mcx>,
    txt: TsVec<'_>,
    qr: &QueryRep<'_, '_>,
) -> PgResult<Option<PgVec<'mcx, DocRep<'mcx>>>> {
    let q = qr.q;
    let mut raw: PgVec<(WordEntryPos, usize, usize)> = PgVec::new_in(mcx);

    for i in 0..q.size() {
        let Item::Val(curoperand) = q.item(i) else { continue };
        let Some((first, nitem)) = find_wordentry(txt, q, &curoperand) else {
            continue;
        };
        for entry in first..first + nitem {
            let e = txt.entry(entry);
            if !e.haspos() {
                continue;
            }
            for &p in txt.positions(e) {
                if curoperand.weight == 0
                    || curoperand.weight & (1 << wep_getweight(p)) != 0
                {
                    raw.push((p, entry, i));
                }
            }
        }
    }

    if raw.is_empty() {
        return Ok(None);
    }

    raw.sort_by(|a, b| {
        wep_getpos(a.0)
            .cmp(&wep_getpos(b.0))
            .then(wep_getweight(a.0).cmp(&wep_getweight(b.0)))
            .then(a.1.cmp(&b.1))
    });

    let mut doc: PgVec<DocRep> = PgVec::new_in(mcx);
    doc.try_reserve_exact(raw.len()).map_err(|_| mcx.oom(raw.len()))?;
    let mut cur: Option<DocRep> = None;
    for (p, entry, item) in raw.iter().copied() {
        match cur.as_mut() {
            Some(c) if c.pos == p && c.entry == entry => {
                c.items.push(item);
            }
            _ => {
                if let Some(c) = cur.take() {
                    doc.push(c);
                }
                let mut items = PgVec::new_in(mcx);
                items.push(item);
                cur = Some(DocRep { pos: p, entry, items });
            }
        }
    }
    if let Some(c) = cur.take() {
        doc.push(c);
    }
    Ok(Some(doc))
}

pub fn calc_rank_cd(
    mcx: Mcx<'_>,
    arrdata: &[f32; NUM_WEIGHTS],
    txt: TsVec<'_>,
    query: TsQueryRef<'_>,
    method: i32,
) -> PgResult<f32> {
    let mut invws = [0f64; NUM_WEIGHTS];
    for i in 0..NUM_WEIGHTS {
        let v = if arrdata[i] >= 0.0 { arrdata[i] } else { DEFAULT_WEIGHTS[i] };
        if v > 1.0 {
            return Err(::types_error::PgError::error("weight out of range")
                .with_sqlstate(::types_error::ERRCODE_INVALID_PARAMETER_VALUE)
                .into());
        }
        invws[i] = 1.0 / v as f64;
    }

    let mut op_data: PgVec<QrOperand> = PgVec::new_in(mcx);
    op_data
        .try_reserve_exact(query.size())
        .map_err(|_| mcx.oom(query.size()))?;
    let mut by_distance: PgVec<(usize, usize)> = PgVec::new_in(mcx);
    for i in 0..query.size() {
        op_data.push(QrOperand { exists: false, reverseinsert: false, pos: PgVec::new_in(mcx) });
        if let Item::Val(op) = query.item(i) {
            by_distance.push((op.distance, i));
        }
    }
    by_distance.sort_by_key(|&(d, _)| d);
    let mut qr = QueryRep { q: query, op_data, by_distance };

    let Some(doc) = get_docrep(mcx, txt, &qr)? else {
        return Ok(0.0);
    };
    let doclen = doc.len();

    let mut wdoc = 0.0f64;
    let mut sumdist = 0.0f64;
    let mut prevextpos = 0.0f64;
    let mut nextent = 0i32;
    let mut ext = CoverExt { pos: 0, p: 0, q: 0, begin: 0, end: 0 };

    while cover(mcx, &doc[..doclen], &mut qr, &mut ext)? {
        let mut invsum = 0.0f64;
        for d in &doc[ext.begin..=ext.end] {
            invsum += invws[wep_getweight(d.pos) as usize];
        }
        let cpos = (ext.end - ext.begin + 1) as f64 / invsum;
        let mut nnoise = (ext.q - ext.p) - (ext.end as i32 - ext.begin as i32);
        if nnoise < 0 {
            nnoise = (ext.end as i32 - ext.begin as i32) / 2;
        }
        wdoc += cpos / (1 + nnoise) as f64;

        let curextpos = (ext.q + ext.p) as f64 / 2.0;
        if nextent > 0 && curextpos > prevextpos {
            sumdist += 1.0 / (curextpos - prevextpos);
        }
        prevextpos = curextpos;
        nextent += 1;
    }

    if method & RANK_NORM_LOGLENGTH != 0 && txt.size() > 0 {
        wdoc /= ((cnt_length(txt) + 1) as f64).ln();
    }
    if method & RANK_NORM_LENGTH != 0 {
        let len = cnt_length(txt);
        if len > 0 {
            wdoc /= len as f64;
        }
    }
    if method & RANK_NORM_EXTDIST != 0 && nextent > 0 && sumdist > 0.0 {
        wdoc /= nextent as f64 / sumdist;
    }
    if method & RANK_NORM_UNIQ != 0 && txt.size() > 0 {
        wdoc /= txt.size() as f64;
    }
    if method & RANK_NORM_LOGUNIQ != 0 && txt.size() > 0 {
        wdoc /= ((txt.size() + 1) as f64).ln() / 2.0f64.ln();
    }
    if method & RANK_NORM_RDIVRPLUS1 != 0 {
        wdoc /= wdoc + 1.0;
    }

    Ok(wdoc as f32)
}
