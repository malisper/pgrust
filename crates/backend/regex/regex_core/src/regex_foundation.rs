
extern crate alloc;

use ::mcx::Mcx;

use crate::regex_consts::REG_ECOLORS;
use crate::regex_error::{RegError, RegResult};
use crate::regguts::{
    chr, color, ColorDesc, ColorMap, ColorMapRange, Cvec, CvecRange, Nfa, StateId, CHR_MIN,
    COLORLESS, FREECOL, MAX_COLOR, MAX_SIMPLE_CHR, NOSUB, PLAIN, PSEUDO, WHITE,
};


pub fn newcvec<'mcx>(_mcx: Mcx<'mcx>, nchrs: i32, nranges: i32) -> RegResult<Cvec> {
    let mut chrs: alloc::vec::Vec<chr> = alloc::vec::Vec::new();
    chrs.try_reserve_exact(nchrs as usize)?;
    let mut ranges: alloc::vec::Vec<CvecRange> = alloc::vec::Vec::new();
    ranges.try_reserve_exact(nranges as usize)?;
    let mut cv = Cvec {
        chrs,
        ranges,
        cclasscode: -1,
    };
    clearcvec(&mut cv);
    Ok(cv)
}

pub fn clearcvec(cv: &mut Cvec) {
    cv.chrs.clear();
    cv.ranges.clear();
    cv.cclasscode = -1;
}

pub fn addchr(cv: &mut Cvec, c: chr) {
    debug_assert!(cv.chrs.len() < cv.chrs.capacity());
    cv.chrs.push(c);
}

pub fn addrange(cv: &mut Cvec, from: chr, to: chr) {
    debug_assert!(cv.ranges.len() < cv.ranges.capacity());
    cv.ranges.push(CvecRange { from, to });
}

pub fn getcvec<'mcx>(
    mcx: Mcx<'mcx>,
    reuse: Option<Cvec>,
    nchrs: i32,
    nranges: i32,
) -> RegResult<Cvec> {
    if let Some(mut cv) = reuse {
        if (nchrs as usize) <= cv.chrs.capacity() && (nranges as usize) <= cv.ranges.capacity() {
            clearcvec(&mut cv);
            return Ok(cv);
        }
        freecvec(cv);
    }

    newcvec(mcx, nchrs, nranges)
}

pub fn freecvec(cv: Cvec) {
    drop(cv);
}


#[inline]
fn unusedcolor(cd: &ColorDesc) -> bool {
    (cd.flags & FREECOL) != 0
}

pub fn initcm<'mcx>(_mcx: Mcx<'mcx>, cm: &mut ColorMap) -> RegResult<()> {
    let white = ColorDesc {
        nschrs: (MAX_SIMPLE_CHR - CHR_MIN + 1) as i32,
        nuchrs: 1,
        sub: NOSUB,
        arcs: None,
        firstchr: CHR_MIN,
        flags: 0,
    };
    cm.cd.clear();
    cm.cd.try_reserve(1)?;
    cm.cd.push(white);
    cm.max = 0;
    cm.free = 0;

    let losize = (MAX_SIMPLE_CHR - CHR_MIN + 1) as usize;
    cm.locolormap.clear();
    cm.locolormap.try_reserve_exact(losize)?;
    cm.locolormap.resize(losize, WHITE);

    cm.classbits = [0; crate::regex_consts::NUM_CCLASSES as usize];

    cm.cmranges.clear();
    cm.cmranges.shrink_to_fit();

    cm.hiarrayrows = 1;
    cm.hiarraycols = 1;
    cm.hicolormap.clear();
    cm.hicolormap.try_reserve(1)?;
    cm.hicolormap.push(WHITE);

    Ok(())
}

pub fn freecm(cm: &mut ColorMap) {
    cm.cd = alloc::vec::Vec::new();
    cm.locolormap = alloc::vec::Vec::new();
    cm.cmranges = alloc::vec::Vec::new();
    cm.hicolormap = alloc::vec::Vec::new();
}

pub fn pg_reg_getcolor(cm: &ColorMap, c: chr) -> color {
    debug_assert!(c > MAX_SIMPLE_CHR);

    let mut rownum: i32 = 0; // if no match, use array row zero
    let mut low: i32 = 0;
    let mut high: i32 = cm.cmranges.len() as i32;
    while low < high {
        let middle = low + (high - low) / 2;
        let cmr = &cm.cmranges[middle as usize];
        if c < cmr.cmin {
            high = middle;
        } else if c > cmr.cmax {
            low = middle + 1;
        } else {
            rownum = cmr.rownum; // found a match
            break;
        }
    }

    if cm.hiarraycols > 1 {
        let colnum = crate::regex_locale::cclass_column_index(cm, c);
        cm.hicolormap[(rownum * cm.hiarraycols + colnum) as usize]
    } else {
        cm.hicolormap[rownum as usize]
    }
}

pub fn maxcolor(cm: &ColorMap) -> color {
    cm.max as color
}

pub fn newcolor<'mcx>(_mcx: Mcx<'mcx>, cm: &mut ColorMap) -> RegResult<color> {
    let co: color;

    if cm.free != 0 {
        debug_assert!(cm.free > 0);
        debug_assert!((cm.free as usize) < cm.cd.len());
        let f = cm.free as usize;
        debug_assert!(unusedcolor(&cm.cd[f]));
        debug_assert!(cm.cd[f].arcs.is_none());
        cm.free = cm.cd[f].sub;
        co = f as color;
    } else if cm.max < cm.cd.len() - 1 {
        cm.max += 1;
        co = cm.max as color;
    } else {
        if cm.max == MAX_COLOR as usize {
            return Err(RegError(REG_ECOLORS)); // too many colors
        }
        cm.max += 1;
        debug_assert_eq!(cm.max, cm.cd.len());
        cm.cd.try_reserve(1)?;
        cm.cd.push(ColorDesc {
            nschrs: 0,
            nuchrs: 0,
            sub: NOSUB,
            arcs: None,
            firstchr: CHR_MIN,
            flags: 0,
        });
        co = cm.max as color;
    }

    let cd = &mut cm.cd[co as usize];
    cd.nschrs = 0;
    cd.nuchrs = 0;
    cd.sub = NOSUB;
    cd.arcs = None;
    cd.firstchr = CHR_MIN; // in case never set otherwise
    cd.flags = 0;

    Ok(co)
}

pub fn freecolor(cm: &mut ColorMap, co: color) {
    debug_assert!(co >= 0);
    if co == WHITE {
        return;
    }

    {
        let cd = &mut cm.cd[co as usize];
        debug_assert!(cd.arcs.is_none());
        debug_assert_eq!(cd.sub, NOSUB);
        debug_assert_eq!(cd.nschrs, 0);
        debug_assert_eq!(cd.nuchrs, 0);
        cd.flags = FREECOL;
    }

    if co as usize == cm.max {
        while cm.max > WHITE as usize && unusedcolor(&cm.cd[cm.max]) {
            cm.max -= 1;
        }
        debug_assert!(cm.free >= 0);
        while (cm.free as usize) > cm.max {
            cm.free = cm.cd[cm.free as usize].sub;
        }
        if cm.free > 0 {
            debug_assert!((cm.free as usize) < cm.max);
            let mut pco = cm.free;
            let mut nco = cm.cd[pco as usize].sub;
            while nco > 0 {
                if (nco as usize) > cm.max {
                    nco = cm.cd[nco as usize].sub;
                    cm.cd[pco as usize].sub = nco;
                } else {
                    debug_assert!((nco as usize) < cm.max);
                    pco = nco;
                    nco = cm.cd[pco as usize].sub;
                }
            }
        }
    } else {
        cm.cd[co as usize].sub = cm.free;
        cm.free = co;
    }
}

pub fn pseudocolor<'mcx>(mcx: Mcx<'mcx>, cm: &mut ColorMap) -> RegResult<color> {
    let co = newcolor(mcx, cm)?;
    let cd = &mut cm.cd[co as usize];
    cd.nschrs = 0;
    cd.nuchrs = 1; // pretend it is in the upper map
    cd.sub = NOSUB;
    cd.arcs = None;
    cd.firstchr = CHR_MIN;
    cd.flags = PSEUDO;
    Ok(co)
}

pub fn subcolor<'mcx>(mcx: Mcx<'mcx>, cm: &mut ColorMap, c: chr) -> RegResult<color> {
    debug_assert!(c <= MAX_SIMPLE_CHR);

    let co = cm.locolormap[(c - CHR_MIN) as usize]; // current color of c
    let sco = newsub(mcx, cm, co)?; // new subcolor
    debug_assert!(sco != COLORLESS);

    if co == sco {
        return Ok(co); // rest is redundant
    }
    cm.cd[co as usize].nschrs -= 1;
    if cm.cd[sco as usize].nschrs == 0 {
        cm.cd[sco as usize].firstchr = c;
    }
    cm.cd[sco as usize].nschrs += 1;
    cm.locolormap[(c - CHR_MIN) as usize] = sco;
    Ok(sco)
}

pub fn subcolorhi<'mcx>(mcx: Mcx<'mcx>, cm: &mut ColorMap, hi_idx: usize) -> RegResult<color> {
    let co = cm.hicolormap[hi_idx]; // current color of entry
    let sco = newsub(mcx, cm, co)?; // new subcolor
    debug_assert!(sco != COLORLESS);

    if co == sco {
        return Ok(co); // rest is redundant
    }
    cm.cd[co as usize].nuchrs -= 1;
    cm.cd[sco as usize].nuchrs += 1;
    cm.hicolormap[hi_idx] = sco;
    Ok(sco)
}

pub fn newsub<'mcx>(mcx: Mcx<'mcx>, cm: &mut ColorMap, co: color) -> RegResult<color> {
    let mut sco = cm.cd[co as usize].sub;
    if sco == NOSUB {
        let cd = &cm.cd[co as usize];
        if (cd.nschrs + cd.nuchrs) == 1 {
            return Ok(co);
        }
        sco = newcolor(mcx, cm)?; // must create subcolor
        cm.cd[co as usize].sub = sco;
        cm.cd[sco as usize].sub = sco; // open subcolor points to self
    }
    debug_assert!(sco != NOSUB);
    Ok(sco)
}

pub fn newhicolorrow<'mcx>(_mcx: Mcx<'mcx>, cm: &mut ColorMap, oldrow: i32) -> RegResult<i32> {
    let newrow = cm.hiarrayrows;
    let cols = cm.hiarraycols as usize;

    let oldbase = (oldrow as usize) * cols;
    let mut rowdata: alloc::vec::Vec<color> = alloc::vec::Vec::new();
    rowdata.try_reserve_exact(cols)?;
    rowdata.extend_from_slice(&cm.hicolormap[oldbase..oldbase + cols]);

    cm.hicolormap.try_reserve(cols)?;
    cm.hicolormap.extend_from_slice(&rowdata);
    cm.hiarrayrows += 1;

    for &co in &rowdata {
        cm.cd[co as usize].nuchrs += 1;
    }

    Ok(newrow)
}

pub fn newhicolorcols<'mcx>(_mcx: Mcx<'mcx>, cm: &mut ColorMap) -> RegResult<()> {
    let rows = cm.hiarrayrows as usize;
    let oldcols = cm.hiarraycols as usize;
    let newcols = oldcols * 2;

    let mut newarray: alloc::vec::Vec<color> = alloc::vec::Vec::new();
    newarray.try_reserve_exact(rows * newcols)?;
    for r in 0..rows {
        let oldbase = r * oldcols;
        for c in 0..oldcols {
            newarray.push(cm.hicolormap[oldbase + c]);
        }
        for c in 0..oldcols {
            newarray.push(cm.hicolormap[oldbase + c]);
        }
        for c in 0..oldcols {
            let co = cm.hicolormap[oldbase + c];
            cm.cd[co as usize].nuchrs += 1;
        }
    }

    cm.hicolormap = newarray;
    cm.hiarraycols = newcols as i32;
    Ok(())
}

pub fn subcolorcvec<'mcx>(
    mcx: Mcx<'mcx>,
    nfa: &mut Nfa,
    cm: &mut ColorMap,
    cv: &Cvec,
    lp: StateId,
    rp: StateId,
) -> RegResult<()> {
    let mut lastsubcolor: color = COLORLESS;

    for ch in cv.chrs.iter().copied() {
        subcoloronechr(mcx, nfa, cm, ch, lp, rp, &mut lastsubcolor)?;
    }

    for r in cv.ranges.iter().copied() {
        let to = r.to;
        let mut from = r.from;
        if from <= MAX_SIMPLE_CHR {
            let lim = if to <= MAX_SIMPLE_CHR {
                to
            } else {
                MAX_SIMPLE_CHR
            };
            while from <= lim {
                let sco = subcolor(mcx, cm, from)?;
                if sco != lastsubcolor {
                    crate::regex_nfa::newarc(mcx, nfa, cm, false, PLAIN, sco, lp, rp)?;
                    lastsubcolor = sco;
                }
                from += 1;
            }
        }
        if from < to {
            subcoloronerange(mcx, nfa, cm, from, to, lp, rp, &mut lastsubcolor)?;
        } else if from == to {
            subcoloronechr(mcx, nfa, cm, from, lp, rp, &mut lastsubcolor)?;
        }
    }

    if cv.cclasscode >= 0 {
        let cc = cv.cclasscode as usize;
        if cm.classbits[cc] == 0 {
            cm.classbits[cc] = cm.hiarraycols;
            newhicolorcols(mcx, cm)?;
        }
        let classbit = cm.classbits[cc];
        let rows = cm.hiarrayrows;
        let cols = cm.hiarraycols;
        for r in 0..rows {
            for c in 0..cols {
                if (c & classbit) != 0 {
                    let hi_idx = (r * cols + c) as usize;
                    let sco = subcolorhi(mcx, cm, hi_idx)?;
                    if sco != lastsubcolor {
                        crate::regex_nfa::newarc(mcx, nfa, cm, false, PLAIN, sco, lp, rp)?;
                        lastsubcolor = sco;
                    }
                }
            }
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn subcoloronechr<'mcx>(
    mcx: Mcx<'mcx>,
    nfa: &mut Nfa,
    cm: &mut ColorMap,
    ch: chr,
    lp: StateId,
    rp: StateId,
    lastsubcolor: &mut color,
) -> RegResult<()> {
    if ch <= MAX_SIMPLE_CHR {
        let sco = subcolor(mcx, cm, ch)?;
        if sco != *lastsubcolor {
            crate::regex_nfa::newarc(mcx, nfa, cm, false, PLAIN, sco, lp, rp)?;
            *lastsubcolor = sco;
        }
        return Ok(());
    }

    let oldranges = core::mem::take(&mut cm.cmranges);
    let numold = oldranges.len();
    let mut newranges: alloc::vec::Vec<ColorMapRange> = alloc::vec::Vec::new();
    newranges.try_reserve_exact(numold + 2)?;
    let mut oldrangen: usize = 0;
    let newrow: i32;

    while oldrangen < numold {
        if oldranges[oldrangen].cmax >= ch {
            break;
        }
        newranges.push(oldranges[oldrangen]);
        oldrangen += 1;
    }

    if oldrangen >= numold || oldranges[oldrangen].cmin > ch {
        newrow = newhicolorrow(mcx, cm, 0)?;
        newranges.push(ColorMapRange {
            cmin: ch,
            cmax: ch,
            rownum: newrow,
        });
    } else if oldranges[oldrangen].cmin == oldranges[oldrangen].cmax {
        let old = oldranges[oldrangen];
        newranges.push(old);
        newrow = old.rownum;
        oldrangen += 1; // we've now fully processed this old range
    } else {
        let old = oldranges[oldrangen];
        if ch > old.cmin {
            newranges.push(ColorMapRange {
                cmin: old.cmin,
                cmax: ch - 1,
                rownum: old.rownum,
            });
        }
        newrow = newhicolorrow(mcx, cm, old.rownum)?;
        newranges.push(ColorMapRange {
            cmin: ch,
            cmax: ch,
            rownum: newrow,
        });
        if ch < old.cmax {
            let rownum = if ch > old.cmin {
                newhicolorrow(mcx, cm, old.rownum)?
            } else {
                old.rownum
            };
            newranges.push(ColorMapRange {
                cmin: ch + 1,
                cmax: old.cmax,
                rownum,
            });
        }
        oldrangen += 1; // we've now fully processed this old range
    }

    subcoloronerow(mcx, nfa, cm, newrow, lp, rp, lastsubcolor)?;

    while oldrangen < numold {
        newranges.push(oldranges[oldrangen]);
        oldrangen += 1;
    }

    debug_assert!(newranges.len() <= numold + 2);

    cm.cmranges = newranges;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn subcoloronerange<'mcx>(
    mcx: Mcx<'mcx>,
    nfa: &mut Nfa,
    cm: &mut ColorMap,
    from_in: chr,
    to: chr,
    lp: StateId,
    rp: StateId,
    lastsubcolor: &mut color,
) -> RegResult<()> {
    debug_assert!(from_in > MAX_SIMPLE_CHR);
    debug_assert!(from_in < to);

    let mut from = from_in;

    let oldranges = core::mem::take(&mut cm.cmranges);
    let numold = oldranges.len();
    let mut newranges: alloc::vec::Vec<ColorMapRange> = alloc::vec::Vec::new();
    newranges.try_reserve_exact(numold * 2 + 1)?;
    let mut oldrangen: usize = 0;

    while oldrangen < numold {
        if oldranges[oldrangen].cmax >= from {
            break;
        }
        newranges.push(oldranges[oldrangen]);
        oldrangen += 1;
    }

    while oldrangen < numold && oldranges[oldrangen].cmin <= to {
        let old = oldranges[oldrangen];
        let mut newrow: i32;

        if from < old.cmin {
            newrow = newhicolorrow(mcx, cm, 0)?;
            newranges.push(ColorMapRange {
                cmin: from,
                cmax: old.cmin - 1,
                rownum: newrow,
            });
            subcoloronerow(mcx, nfa, cm, newrow, lp, rp, lastsubcolor)?;
            from = old.cmin;
        }

        if from <= old.cmin && to >= old.cmax {
            newranges.push(old);
            newrow = old.rownum;
            from = old.cmax + 1;
        } else {
            if from > old.cmin {
                newranges.push(ColorMapRange {
                    cmin: old.cmin,
                    cmax: from - 1,
                    rownum: old.rownum,
                });
            }
            newrow = newhicolorrow(mcx, cm, old.rownum)?;
            newranges.push(ColorMapRange {
                cmin: from,
                cmax: if to < old.cmax { to } else { old.cmax },
                rownum: newrow,
            });
            if to < old.cmax {
                let rownum = if from > old.cmin {
                    newhicolorrow(mcx, cm, old.rownum)?
                } else {
                    old.rownum
                };
                newranges.push(ColorMapRange {
                    cmin: to + 1,
                    cmax: old.cmax,
                    rownum,
                });
            }
            from = old.cmax + 1;
        }
        subcoloronerow(mcx, nfa, cm, newrow, lp, rp, lastsubcolor)?;
        oldrangen += 1; // we've now fully processed this old range
    }

    if from <= to {
        let newrow = newhicolorrow(mcx, cm, 0)?;
        newranges.push(ColorMapRange {
            cmin: from,
            cmax: to,
            rownum: newrow,
        });
        subcoloronerow(mcx, nfa, cm, newrow, lp, rp, lastsubcolor)?;
    }

    while oldrangen < numold {
        newranges.push(oldranges[oldrangen]);
        oldrangen += 1;
    }

    debug_assert!(newranges.len() <= numold * 2 + 1);

    cm.cmranges = newranges;
    Ok(())
}

pub fn subcoloronerow<'mcx>(
    mcx: Mcx<'mcx>,
    nfa: &mut Nfa,
    cm: &mut ColorMap,
    rownum: i32,
    lp: StateId,
    rp: StateId,
    lastsubcolor: &mut color,
) -> RegResult<()> {
    let cols = cm.hiarraycols;
    let base = (rownum * cols) as usize;
    for i in 0..cols as usize {
        let sco = subcolorhi(mcx, cm, base + i)?;
        if sco != *lastsubcolor {
            crate::regex_nfa::newarc(mcx, nfa, cm, false, PLAIN, sco, lp, rp)?;
            *lastsubcolor = sco;
        }
    }
    Ok(())
}
