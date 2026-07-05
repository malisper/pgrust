use ::mcx::{Mcx, PgVec};
use ::types_error::PgResult;

use crate::layout::{wep_getpos, WordEntryPos};
use crate::query::{Item, Operand, TsQueryRef, OP_AND, OP_NOT, OP_OR, OP_PHRASE};

pub const TS_EXEC_EMPTY: u32 = 0x00;
pub const TS_EXEC_SKIP_NOT: u32 = 0x01;
pub const TS_EXEC_PHRASE_NO_POS: u32 = 0x02;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ternary {
    No,
    Yes,
    Maybe,
}

pub struct ExecPhraseData<'mcx> {
    pub pos: PgVec<'mcx, WordEntryPos>,
    pub negate: bool,
    pub width: i32,
}

impl<'mcx> ExecPhraseData<'mcx> {
    pub fn new(mcx: Mcx<'mcx>) -> Self {
        ExecPhraseData { pos: PgVec::new_in(mcx), negate: false, width: 0 }
    }

    #[inline]
    pub fn npos(&self) -> usize {
        self.pos.len()
    }
}

// The usize is the operand's QueryItem index (C passes QueryItem pointers;
// GIN's checkcondition needs `val - first_item`).
pub type ChkCond<'c, 'mcx> =
    &'c mut dyn FnMut(usize, &Operand, Option<&mut ExecPhraseData<'mcx>>) -> PgResult<Ternary>;

const TSPO_L_ONLY: u32 = 0x01;
const TSPO_R_ONLY: u32 = 0x02;
const TSPO_BOTH: u32 = 0x04;

pub fn ts_phrase_output(
    data: Option<&mut ExecPhraseData<'_>>,
    ldata: &ExecPhraseData<'_>,
    rdata: &ExecPhraseData<'_>,
    emit: u32,
    loffset: i32,
    roffset: i32,
) -> Ternary {
    let mut lindex = 0usize;
    let mut rindex = 0usize;
    let mut wrote = false;
    let mut data = data;
    while lindex < ldata.npos() || rindex < rdata.npos() {
        let lpos = if lindex < ldata.npos() {
            wep_getpos(ldata.pos[lindex]) as i32 + loffset
        } else {
            if emit & TSPO_R_ONLY == 0 {
                break;
            }
            i32::MAX
        };
        let rpos = if rindex < rdata.npos() {
            wep_getpos(rdata.pos[rindex]) as i32 + roffset
        } else {
            if emit & TSPO_L_ONLY == 0 {
                break;
            }
            i32::MAX
        };

        let mut output_pos = 0i32;
        if lpos < rpos {
            if emit & TSPO_L_ONLY != 0 {
                output_pos = lpos;
            }
            lindex += 1;
        } else if lpos == rpos {
            if emit & TSPO_BOTH != 0 {
                output_pos = rpos;
            }
            lindex += 1;
            rindex += 1;
        } else {
            if emit & TSPO_R_ONLY != 0 {
                output_pos = rpos;
            }
            rindex += 1;
        }

        if output_pos > 0 {
            match data.as_deref_mut() {
                Some(d) => {
                    d.pos.push(output_pos as WordEntryPos);
                    wrote = true;
                }
                None => return Ternary::Yes,
            }
        }
    }
    if wrote {
        Ternary::Yes
    } else {
        Ternary::No
    }
}

pub fn ts_phrase_execute<'mcx>(
    mcx: Mcx<'mcx>,
    q: TsQueryRef<'_>,
    idx: usize,
    flags: u32,
    chkcond: ChkCond<'_, 'mcx>,
    mut data: Option<&mut ExecPhraseData<'mcx>>,
) -> PgResult<Ternary> {
    match q.item(idx) {
        Item::Val(op) => chkcond(idx, &op, data),
        Item::ValStop => panic!("ts_phrase_execute: QI_VALSTOP in stored tsquery"),
        Item::Opr(opr) => match opr.oper {
            OP_NOT => {
                let data = data.as_deref_mut().expect("phrase NOT needs a data frame");
                if flags & TS_EXEC_SKIP_NOT != 0 {
                    data.negate = true;
                    return Ok(Ternary::Yes);
                }
                match ts_phrase_execute(mcx, q, idx + 1, flags, chkcond, Some(&mut *data))? {
                    Ternary::No => {
                        data.negate = true;
                        Ok(Ternary::Yes)
                    }
                    Ternary::Yes => {
                        if data.npos() > 0 {
                            data.negate = !data.negate;
                            Ok(Ternary::Yes)
                        } else if data.negate {
                            data.negate = false;
                            Ok(Ternary::No)
                        } else {
                            unreachable!("TS_YES with no positions and no negate")
                        }
                    }
                    Ternary::Maybe => Ok(Ternary::Maybe),
                }
            }
            OP_PHRASE | OP_AND => {
                let mut ldata = ExecPhraseData::new(mcx);
                let mut rdata = ExecPhraseData::new(mcx);
                let lmatch = ts_phrase_execute(
                    mcx,
                    q,
                    idx + opr.left as usize,
                    flags,
                    chkcond,
                    Some(&mut ldata),
                )?;
                if lmatch == Ternary::No {
                    return Ok(Ternary::No);
                }
                let rmatch =
                    ts_phrase_execute(mcx, q, idx + 1, flags, chkcond, Some(&mut rdata))?;
                if rmatch == Ternary::No {
                    return Ok(Ternary::No);
                }
                if lmatch == Ternary::Maybe || rmatch == Ternary::Maybe {
                    return Ok(Ternary::Maybe);
                }

                let (loffset, roffset);
                if opr.oper == OP_PHRASE {
                    loffset = opr.distance as i32 + rdata.width;
                    roffset = 0;
                    if let Some(d) = data.as_deref_mut() {
                        d.width = opr.distance as i32 + ldata.width + rdata.width;
                    }
                } else {
                    let maxwidth = ldata.width.max(rdata.width);
                    loffset = maxwidth - ldata.width;
                    roffset = maxwidth - rdata.width;
                    if let Some(d) = data.as_deref_mut() {
                        d.width = maxwidth;
                    }
                }

                if ldata.negate && rdata.negate {
                    ts_phrase_output(
                        data.as_deref_mut(),
                        &ldata,
                        &rdata,
                        TSPO_BOTH | TSPO_L_ONLY | TSPO_R_ONLY,
                        loffset,
                        roffset,
                    );
                    if let Some(d) = data {
                        d.negate = true;
                    }
                    Ok(Ternary::Yes)
                } else if ldata.negate {
                    Ok(ts_phrase_output(data, &ldata, &rdata, TSPO_R_ONLY, loffset, roffset))
                } else if rdata.negate {
                    Ok(ts_phrase_output(data, &ldata, &rdata, TSPO_L_ONLY, loffset, roffset))
                } else {
                    Ok(ts_phrase_output(data, &ldata, &rdata, TSPO_BOTH, loffset, roffset))
                }
            }
            OP_OR => {
                let mut ldata = ExecPhraseData::new(mcx);
                let mut rdata = ExecPhraseData::new(mcx);
                let lmatch = ts_phrase_execute(
                    mcx,
                    q,
                    idx + opr.left as usize,
                    flags,
                    chkcond,
                    Some(&mut ldata),
                )?;
                let rmatch =
                    ts_phrase_execute(mcx, q, idx + 1, flags, chkcond, Some(&mut rdata))?;
                if lmatch == Ternary::No && rmatch == Ternary::No {
                    return Ok(Ternary::No);
                }
                if lmatch == Ternary::Maybe || rmatch == Ternary::Maybe {
                    return Ok(Ternary::Maybe);
                }
                if lmatch == Ternary::No {
                    ldata.width = 0;
                }
                if rmatch == Ternary::No {
                    rdata.width = 0;
                }
                let maxwidth = ldata.width.max(rdata.width);
                let loffset = maxwidth - ldata.width;
                let roffset = maxwidth - rdata.width;
                let d = data.expect("phrase OR needs a data frame");
                d.width = maxwidth;

                if ldata.negate && rdata.negate {
                    ts_phrase_output(Some(d), &ldata, &rdata, TSPO_BOTH, loffset, roffset);
                    d.negate = true;
                    Ok(Ternary::Yes)
                } else if ldata.negate {
                    ts_phrase_output(Some(d), &ldata, &rdata, TSPO_L_ONLY, loffset, roffset);
                    d.negate = true;
                    Ok(Ternary::Yes)
                } else if rdata.negate {
                    ts_phrase_output(Some(d), &ldata, &rdata, TSPO_R_ONLY, loffset, roffset);
                    d.negate = true;
                    Ok(Ternary::Yes)
                } else {
                    Ok(ts_phrase_output(
                        Some(d),
                        &ldata,
                        &rdata,
                        TSPO_BOTH | TSPO_L_ONLY | TSPO_R_ONLY,
                        loffset,
                        roffset,
                    ))
                }
            }
            other => panic!("unrecognized operator: {other}"),
        },
    }
}

fn ts_execute_recurse<'mcx>(
    mcx: Mcx<'mcx>,
    q: TsQueryRef<'_>,
    idx: usize,
    flags: u32,
    chkcond: ChkCond<'_, 'mcx>,
) -> PgResult<Ternary> {
    match q.item(idx) {
        Item::Val(op) => chkcond(idx, &op, None),
        Item::ValStop => panic!("TS_execute: QI_VALSTOP in stored tsquery"),
        Item::Opr(opr) => match opr.oper {
            OP_NOT => {
                if flags & TS_EXEC_SKIP_NOT != 0 {
                    return Ok(Ternary::Yes);
                }
                Ok(match ts_execute_recurse(mcx, q, idx + 1, flags, chkcond)? {
                    Ternary::No => Ternary::Yes,
                    Ternary::Yes => Ternary::No,
                    Ternary::Maybe => Ternary::Maybe,
                })
            }
            OP_AND => {
                let lmatch =
                    ts_execute_recurse(mcx, q, idx + opr.left as usize, flags, chkcond)?;
                if lmatch == Ternary::No {
                    return Ok(Ternary::No);
                }
                Ok(match ts_execute_recurse(mcx, q, idx + 1, flags, chkcond)? {
                    Ternary::No => Ternary::No,
                    Ternary::Yes => lmatch,
                    Ternary::Maybe => Ternary::Maybe,
                })
            }
            OP_OR => {
                let lmatch =
                    ts_execute_recurse(mcx, q, idx + opr.left as usize, flags, chkcond)?;
                if lmatch == Ternary::Yes {
                    return Ok(Ternary::Yes);
                }
                Ok(match ts_execute_recurse(mcx, q, idx + 1, flags, chkcond)? {
                    Ternary::No => lmatch,
                    Ternary::Yes => Ternary::Yes,
                    Ternary::Maybe => Ternary::Maybe,
                })
            }
            OP_PHRASE => Ok(match ts_phrase_execute(mcx, q, idx, flags, chkcond, None)? {
                Ternary::No => Ternary::No,
                Ternary::Yes => Ternary::Yes,
                Ternary::Maybe => {
                    if flags & TS_EXEC_PHRASE_NO_POS != 0 {
                        Ternary::Maybe
                    } else {
                        Ternary::No
                    }
                }
            }),
            other => panic!("unrecognized operator: {other}"),
        },
    }
}

pub fn ts_execute<'mcx>(
    mcx: Mcx<'mcx>,
    q: TsQueryRef<'_>,
    flags: u32,
    chkcond: ChkCond<'_, 'mcx>,
) -> PgResult<bool> {
    Ok(ts_execute_recurse(mcx, q, 0, flags, chkcond)? != Ternary::No)
}

pub fn ts_execute_ternary<'mcx>(
    mcx: Mcx<'mcx>,
    q: TsQueryRef<'_>,
    flags: u32,
    chkcond: ChkCond<'_, 'mcx>,
) -> PgResult<Ternary> {
    ts_execute_recurse(mcx, q, 0, flags, chkcond)
}

// TS_execute_locations (tsvector_op.c): per-AND'able-term ExecPhraseData
// lists over operators above any phrase operator; headline cover selection
// consumes the position lists. Empty result = no match (or a NOT that
// reports no locations).
pub fn ts_execute_locations<'mcx>(
    mcx: Mcx<'mcx>,
    q: TsQueryRef<'_>,
    chkcond: ChkCond<'_, 'mcx>,
) -> PgResult<Vec<ExecPhraseData<'mcx>>> {
    let mut locations = Vec::new();
    if ts_execute_locations_recurse(mcx, q, 0, chkcond, &mut locations)? {
        Ok(locations)
    } else {
        Ok(Vec::new())
    }
}

fn ts_execute_locations_recurse<'mcx>(
    mcx: Mcx<'mcx>,
    q: TsQueryRef<'_>,
    idx: usize,
    chkcond: ChkCond<'_, 'mcx>,
    locations: &mut Vec<ExecPhraseData<'mcx>>,
) -> PgResult<bool> {
    locations.clear();
    match q.item(idx) {
        Item::Val(op) => {
            let mut data = ExecPhraseData::new(mcx);
            if chkcond(idx, &op, Some(&mut data))? == Ternary::Yes {
                locations.push(data);
                return Ok(true);
            }
            Ok(false)
        }
        Item::ValStop => panic!("TS_execute_locations: QI_VALSTOP in stored tsquery"),
        Item::Opr(opr) => match opr.oper {
            OP_NOT => {
                let mut l = Vec::new();
                // A failed NOT-arm matches; we pass back no locations.
                Ok(!ts_execute_locations_recurse(mcx, q, idx + 1, chkcond, &mut l)?)
            }
            OP_AND => {
                let mut l = Vec::new();
                if !ts_execute_locations_recurse(
                    mcx,
                    q,
                    idx + opr.left as usize,
                    chkcond,
                    &mut l,
                )? {
                    return Ok(false);
                }
                let mut r = Vec::new();
                if !ts_execute_locations_recurse(mcx, q, idx + 1, chkcond, &mut r)? {
                    return Ok(false);
                }
                locations.append(&mut l);
                locations.append(&mut r);
                Ok(true)
            }
            OP_OR => {
                let mut l = Vec::new();
                let mut r = Vec::new();
                let lmatch =
                    ts_execute_locations_recurse(mcx, q, idx + opr.left as usize, chkcond, &mut l)?;
                let rmatch = ts_execute_locations_recurse(mcx, q, idx + 1, chkcond, &mut r)?;
                if !(lmatch || rmatch) {
                    return Ok(false);
                }
                // (A & B) | (C & D) = (A|C) & (A|D) & (B|C) & (B|D); an
                // input with no locations (failed or NOT) yields the other
                // list unchanged.
                if l.is_empty() {
                    *locations = r;
                } else if r.is_empty() {
                    *locations = l;
                } else {
                    for ldata in &l {
                        for rdata in &r {
                            let mut data = ExecPhraseData::new(mcx);
                            ts_phrase_output(
                                Some(&mut data),
                                ldata,
                                rdata,
                                TSPO_BOTH | TSPO_L_ONLY | TSPO_R_ONLY,
                                0,
                                0,
                            );
                            data.width = ldata.width.max(rdata.width);
                            locations.push(data);
                        }
                    }
                }
                Ok(true)
            }
            OP_PHRASE => {
                let mut data = ExecPhraseData::new(mcx);
                if ts_phrase_execute(mcx, q, idx, TS_EXEC_EMPTY, chkcond, Some(&mut data))?
                    == Ternary::Yes
                {
                    if !data.negate {
                        locations.push(data);
                    }
                    return Ok(true);
                }
                Ok(false)
            }
            other => panic!("unrecognized operator: {other}"),
        },
    }
}
