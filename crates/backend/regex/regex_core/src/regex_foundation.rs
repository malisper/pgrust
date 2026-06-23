//! Family: **regex-foundation** — `regc_cvec.c` (cvec utilities) and
//! `regc_color.c` (the compile-time colormap engine).
//!
//! The `regguts` type contract, `regex_consts`, and `regex_error` live in their
//! own crate-root modules (so every family can use them); this module owns the
//! *behavior* of the cvec + colormap layer.
//!
//! Allocating functions take `Mcx<'mcx>` (C: `MALLOC`/`REALLOC` out of the
//! compile context) and return `RegResult` (true-OOM -> `REG_ESPACE`). The C
//! arrays (`struct cvec` flexible-array tail; `cd`/`locolormap`/`cmranges`/
//! `hicolormap`) are the [`crate::regguts`] `Vec`s; the C `chrspace`/
//! `rangespace`/`ncds`/`maxarrayrows` capacities are subsumed by the `Vec`
//! capacities/lengths. A `MALLOC` returning NULL (C: `-> REG_ESPACE`) maps to a
//! [`Vec::try_reserve`] failure surfaced as `REG_ESPACE`.
//!
//! The C `struct vars` sticky-error latch (`v->err`, the `CERR`/`CISERR`/
//! `NOERR` macros) is replaced by ordinary `RegResult` propagation: every
//! `CISERR()`/`NOERR()` checkpoint becomes a `?`, and every `CERR(e)` becomes
//! `return Err(RegError(e))`.
//!
//! The five colormap<->NFA-arc functions in `regc_color.c`
//! (`okcolors`/`colorchain`/`uncolorchain`/`rainbow`/`colorcomplement`)
//! operate on the NFA arc arena and live in [`crate::regex_nfa`]. The
//! `subcolor*` family here *requests* arcs through that family's `newarc`
//! (C: `newarc(v->nfa, PLAIN, sco, lp, rp)`), threading `&mut Nfa` plus the two
//! endpoint states `lp`/`rp` as [`StateId`] arena handles. `pg_reg_getcolor`
//! consults the locale-dependent column index owned by [`crate::regex_locale`]
//! (`cclass_column_index`).

extern crate alloc;

use ::mcx::Mcx;

use crate::regex_consts::REG_ECOLORS;
use crate::regex_error::{RegError, RegResult};
use crate::regguts::{
    chr, color, ColorDesc, ColorMap, ColorMapRange, Cvec, CvecRange, Nfa, StateId, CHR_MIN,
    COLORLESS, FREECOL, MAX_COLOR, MAX_SIMPLE_CHR, NOSUB, PLAIN, PSEUDO, WHITE,
};

// ---------------------------------------------------------------------------
// regc_cvec.c — character-vector utilities
// ---------------------------------------------------------------------------

/// `newcvec(int nchrs, int nranges)` — allocate a new, empty cvec with room
/// for `nchrs` chrs and `nranges` ranges.
///
/// C reserves `chrspace`/`rangespace` in a single allocation; here the two
/// `Vec`s reserve their capacities (the role of `chrspace`/`rangespace`). The C
/// `MALLOC` returning NULL (`-> NULL`, surfaced by `getcvec` as `REG_ESPACE`)
/// maps to a `try_reserve` failure.
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

/// `clearcvec(struct cvec *cv)` — empty a cvec (set counts to zero, cclasscode
/// to -1), returning it for chaining.
///
/// C sets `nchrs = 0; nranges = 0; cclasscode = -1`, keeping `chrspace`/
/// `rangespace` reserved. Here `Vec::clear` drops the (POD) elements but RETAINS
/// the allocated capacity, exactly mirroring that.
pub fn clearcvec(cv: &mut Cvec) {
    cv.chrs.clear();
    cv.ranges.clear();
    cv.cclasscode = -1;
}

/// `addchr(struct cvec *cv, chr c)` — add a chr to a cvec.
///
/// C asserts `nchrs < chrspace` and writes into the preallocated slot. The
/// capacity was reserved by [`newcvec`]/[`getcvec`], so the push stays within
/// the reserved space (no reallocation), matching the C invariant.
pub fn addchr(cv: &mut Cvec, c: chr) {
    debug_assert!(cv.chrs.len() < cv.chrs.capacity());
    cv.chrs.push(c);
}

/// `addrange(struct cvec *cv, chr from, chr to)` — add a range to a cvec.
///
/// C stores `from`/`to` into the flat `ranges[nranges*2 .. nranges*2+1]` slots;
/// here a range is the pair [`CvecRange`]. Same reserved-capacity invariant as
/// [`addchr`].
pub fn addrange(cv: &mut Cvec, from: chr, to: chr) {
    debug_assert!(cv.ranges.len() < cv.ranges.capacity());
    cv.ranges.push(CvecRange { from, to });
}

/// `getcvec(struct vars *v, int nchrs, int nranges)` — get a transient cvec,
/// initialized to empty, reusing the compile context's spare cvec if it is big
/// enough, else allocating a fresh one.
///
/// C recycles the single transient cvec held in `v->cv`: if it is large enough
/// it is cleared and reused, otherwise the old one is freed and a new one is
/// allocated (`v->cv == NULL` -> `ERR(REG_ESPACE)`). `struct vars` is not
/// modeled here, so the existing transient cvec (C's `v->cv`) is passed in as
/// `reuse` and the caller stores the returned cvec back; the recycling test
/// compares the requested sizes against the reusable cvec's *capacities*
/// (`chrspace`/`rangespace`), exactly as C does.
pub fn getcvec<'mcx>(
    mcx: Mcx<'mcx>,
    reuse: Option<Cvec>,
    nchrs: i32,
    nranges: i32,
) -> RegResult<Cvec> {
    // recycle existing transient cvec if large enough
    if let Some(mut cv) = reuse {
        if (nchrs as usize) <= cv.chrs.capacity() && (nranges as usize) <= cv.ranges.capacity() {
            clearcvec(&mut cv);
            return Ok(cv);
        }
        // nope, free the old one (C: freecvec(v->cv)) and make a new one.
        freecvec(cv);
    }

    // make a new one (C: v->cv = newcvec(...); if NULL -> ERR(REG_ESPACE)).
    newcvec(mcx, nchrs, nranges)
}

/// `freecvec(struct cvec *cv)` — free a cvec (C: `FREE(cv)`).
///
/// Under Rust ownership the `Cvec`'s two `Vec`s are dropped when `cv` goes out
/// of scope here, which is the analogue of `FREE(cv)`.
pub fn freecvec(cv: Cvec) {
    drop(cv);
}

// ---------------------------------------------------------------------------
// regc_color.c — the compile-time colormap engine
// ---------------------------------------------------------------------------

/// `UNUSEDCOLOR(cd)` (regguts.h) — true iff the color is currently free.
#[inline]
fn unusedcolor(cd: &ColorDesc) -> bool {
    (cd.flags & FREECOL) != 0
}

/// `initcm(struct vars *v, struct colormap *cm)` — set up a new colormap.
///
/// Builds the initial colormap: a single WHITE colordesc owning all simple
/// chrs, a `locolormap` of length `MAX_SIMPLE_CHR - CHR_MIN + 1` filled with
/// WHITE (C relies on `WHITE == 0` via memset; we fill explicitly), no high
/// ranges, and a one-entry hicolormap ("all other characters" row) set to
/// WHITE. C `cm->magic`/`cm->v` have no analogue here. The two `MALLOC`
/// failures (`CERR(REG_ESPACE)`) map to `try_reserve` failures.
pub fn initcm<'mcx>(_mcx: Mcx<'mcx>, cm: &mut ColorMap) -> RegResult<()> {
    // cm->cd[WHITE]: the single colordesc that owns all simple chrs.
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

    // locolormap: all WHITE. (C memsets to WHITE, relying on WHITE == 0.)
    let losize = (MAX_SIMPLE_CHR - CHR_MIN + 1) as usize;
    cm.locolormap.clear();
    cm.locolormap.try_reserve_exact(losize)?;
    cm.locolormap.resize(losize, WHITE);

    cm.classbits = [0; crate::regex_consts::NUM_CCLASSES as usize];

    cm.cmranges.clear();
    cm.cmranges.shrink_to_fit();

    // One row, one column initially.
    cm.hiarrayrows = 1;
    cm.hiarraycols = 1;
    // The "all other characters" row, initialized to WHITE.
    cm.hicolormap.clear();
    cm.hicolormap.try_reserve(1)?;
    cm.hicolormap.push(WHITE);

    Ok(())
}

/// `freecm(struct colormap *cm)` — free a colormap's dynamically-allocated
/// storage (C: `FREE`s `cd`/`locolormap`/`cmranges`/`hicolormap`).
///
/// Each is a `Vec` here; dropping its contents and freeing the spine is the
/// analogue. C's `cm->magic = 0` and the inline-cdspace distinction have no
/// counterpart.
pub fn freecm(cm: &mut ColorMap) {
    cm.cd = alloc::vec::Vec::new();
    cm.locolormap = alloc::vec::Vec::new();
    cm.cmranges = alloc::vec::Vec::new();
    cm.hicolormap = alloc::vec::Vec::new();
}

/// `pg_reg_getcolor(struct colormap *cm, chr c)` — slow case of `GETCOLOR()`:
/// look up the color of a chr above MAX_SIMPLE_CHR via the high-colormap
/// range/row/column machinery.
///
/// Binary-searches `cmranges` for the row (defaulting to row 0 on no match),
/// then either uses the locale-dependent class-bit column index (when there is
/// more than one column) or the single-column fast path. Must not be used for
/// chrs in the locolormap.
pub fn pg_reg_getcolor(cm: &ColorMap, c: chr) -> color {
    // Should not be used for chrs in the locolormap.
    debug_assert!(c > MAX_SIMPLE_CHR);

    // Find which row it's in.  The colormapranges are in order, binary search.
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

    // Find which column it's in --- this is all locale-dependent.
    if cm.hiarraycols > 1 {
        let colnum = crate::regex_locale::cclass_column_index(cm, c);
        cm.hicolormap[(rownum * cm.hiarraycols + colnum) as usize]
    } else {
        // fast path if no relevant cclasses
        cm.hicolormap[rownum as usize]
    }
}

/// `maxcolor(struct colormap *cm)` — the maximum color number currently in use.
///
/// C first checks the sticky error latch (`CISERR()` -> COLORLESS); here errors
/// thread via `RegResult`, so this just reports `cm->max`.
pub fn maxcolor(cm: &ColorMap) -> color {
    cm.max as color
}

/// `newcolor(struct colormap *cm)` — find a new color (must be assigned at
/// once). Beware: may relocate the colordescs.
///
/// Pops the free chain (`cm->free`) first; otherwise grows. C distinguishes
/// "still room in `ncds`" from "must reallocate"; with a `Vec`-backed `cd` the
/// two collapse into a single append, but the `MAX_COLOR` cap is still enforced
/// before appending. `MAX_COLOR` overflow -> `REG_ECOLORS`; a `try_reserve`
/// failure -> `REG_ESPACE`.
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
        // C: else if (cm->max < cm->ncds - 1) — a slot already exists past
        // `max` (left behind when freecolor shrank `max` without truncating the
        // backing array). Reuse it without growing. With a Vec the role of
        // `ncds` (allocated rows) is played by `cm.cd.len()`.
        cm.max += 1;
        co = cm.max as color;
    } else {
        // C: else — must allocate more. Enforce the MAX_COLOR cap first, then
        // append a fresh slot. `cm.max` is the highest valid color index and
        // `cm.cd.len() == cm.max + 1` on this branch, so the push keeps
        // `co == cm.max` consistent.
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

    // (Re)initialize the chosen colordesc.
    let cd = &mut cm.cd[co as usize];
    cd.nschrs = 0;
    cd.nuchrs = 0;
    cd.sub = NOSUB;
    cd.arcs = None;
    cd.firstchr = CHR_MIN; // in case never set otherwise
    cd.flags = 0;

    Ok(co)
}

/// `freecolor(struct colormap *cm, color co)` — free a color (must have no arcs
/// or subcolor), returning it to the free chain.
///
/// WHITE is never freed. If `co == cm->max`, the top of the array is shrunk
/// past now-unused colors and any free-chain entries that fell above the new
/// `max` are pruned; otherwise `co` is pushed onto the free chain. As in C, the
/// `cd` array keeps its slots; only `max` shrinks.
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
                    // take this one out of freelist
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

/// `pseudocolor(struct colormap *cm)` — allocate a false color (BOS/EOS etc.),
/// to be managed by other means, marked PSEUDO.
///
/// Allocates a color via [`newcolor`] and marks it `PSEUDO`, pretending it has
/// one upper-map entry so it is never considered unused.
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

/// `subcolor(struct colormap *cm, chr c)` — get the subcolor for a simple chr,
/// splitting its color if needed. Works only for chrs in the low color map
/// (`c <= MAX_SIMPLE_CHR`). Keeps the per-color `nschrs` reference counts in
/// sync.
pub fn subcolor<'mcx>(mcx: Mcx<'mcx>, cm: &mut ColorMap, c: chr) -> RegResult<color> {
    debug_assert!(c <= MAX_SIMPLE_CHR);

    let co = cm.locolormap[(c - CHR_MIN) as usize]; // current color of c
    let sco = newsub(mcx, cm, co)?; // new subcolor
    debug_assert!(sco != COLORLESS);

    if co == sco {
        // already in an open subcolor
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

/// `subcolorhi(struct colormap *cm, color *pco)` — get the subcolor for a
/// high-colormap entry, splitting if needed. Same processing as [`subcolor`]
/// but for the high colormap (an entry need not be exactly one chr code); keeps
/// `nuchrs` in sync.
///
/// PORT NOTE: C takes `color *pco` pointing directly into `hicolormap`. To
/// avoid aliasing `cm` mutably twice, this takes the INDEX into `hicolormap`
/// (`hi_idx = rownum*cols + c`) and updates the entry in place.
pub fn subcolorhi<'mcx>(mcx: Mcx<'mcx>, cm: &mut ColorMap, hi_idx: usize) -> RegResult<color> {
    let co = cm.hicolormap[hi_idx]; // current color of entry
    let sco = newsub(mcx, cm, co)?; // new subcolor
    debug_assert!(sco != COLORLESS);

    if co == sco {
        // already in an open subcolor
        return Ok(co); // rest is redundant
    }
    cm.cd[co as usize].nuchrs -= 1;
    cm.cd[sco as usize].nuchrs += 1;
    cm.hicolormap[hi_idx] = sco;
    Ok(sco)
}

/// `newsub(struct colormap *cm, color co)` — create or return the open subcolor
/// of a color.
///
/// If the color already has an open subcolor, returns it. Otherwise the
/// singly-referenced optimization returns `co` itself, or a fresh subcolor is
/// created and cross-linked (`cd[co].sub = sco; cd[sco].sub = sco`).
pub fn newsub<'mcx>(mcx: Mcx<'mcx>, cm: &mut ColorMap, co: color) -> RegResult<color> {
    let mut sco = cm.cd[co as usize].sub;
    if sco == NOSUB {
        // color has no open subcolor
        // optimization: singly-referenced color need not be subcolored
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

/// `newhicolorrow(struct colormap *cm, int oldrow)` — clone a high-colormap row
/// for a new range, growing `hicolormap` if needed. Returns the array index of
/// the new row, and increases the `nuchrs` ref count of every color copied into
/// it.
///
/// C's `maxarrayrows` capacity / `REALLOC` growth is subsumed by extending the
/// `hicolormap` `Vec` by one row's worth of columns. The new row is built by
/// snapshotting the old row first (so the `cm.hicolormap` borrow ends before we
/// push), then pushing it; the snapshot uses `try_reserve` -> `REG_ESPACE`.
pub fn newhicolorrow<'mcx>(_mcx: Mcx<'mcx>, cm: &mut ColorMap, oldrow: i32) -> RegResult<i32> {
    let newrow = cm.hiarrayrows;
    let cols = cm.hiarraycols as usize;

    // Snapshot the old row's colors (the borrow of cm.hicolormap ends here).
    let oldbase = (oldrow as usize) * cols;
    let mut rowdata: alloc::vec::Vec<color> = alloc::vec::Vec::new();
    rowdata.try_reserve_exact(cols)?;
    rowdata.extend_from_slice(&cm.hicolormap[oldbase..oldbase + cols]);

    // Append the fresh row.
    cm.hicolormap.try_reserve(cols)?;
    cm.hicolormap.extend_from_slice(&rowdata);
    cm.hiarrayrows += 1;

    // Increase color reference counts to reflect new colormap entries.
    for &co in &rowdata {
        cm.cd[co as usize].nuchrs += 1;
    }

    Ok(newrow)
}

/// `newhicolorcols(struct colormap *cm)` — double the number of high-colormap
/// columns (extend the 2-D array to the right with a copy of itself) and bump
/// the `nuchrs` ref count of every duplicated entry.
///
/// PORT NOTE: C reallocs in place and copies *backwards* to avoid clobbering
/// not-yet-read entries. Here we build a FRESH forward buffer (the wider array)
/// and swap it in, which is the same result without the in-place hazard.
pub fn newhicolorcols<'mcx>(_mcx: Mcx<'mcx>, cm: &mut ColorMap) -> RegResult<()> {
    let rows = cm.hiarrayrows as usize;
    let oldcols = cm.hiarraycols as usize;
    let newcols = oldcols * 2;

    let mut newarray: alloc::vec::Vec<color> = alloc::vec::Vec::new();
    newarray.try_reserve_exact(rows * newcols)?;
    for r in 0..rows {
        let oldbase = r * oldcols;
        // Duplicate existing columns to the right (left half == right half).
        for c in 0..oldcols {
            newarray.push(cm.hicolormap[oldbase + c]);
        }
        for c in 0..oldcols {
            newarray.push(cm.hicolormap[oldbase + c]);
        }
        // Increase ref counts: each entry is now present twice instead of once.
        for c in 0..oldcols {
            let co = cm.hicolormap[oldbase + c];
            cm.cd[co as usize].nuchrs += 1;
        }
    }

    cm.hicolormap = newarray;
    cm.hiarraycols = newcols as i32;
    Ok(())
}

/// `subcolorcvec(struct vars *v, struct cvec *cv, struct state *lp, struct state
/// *rp)` — allocate new subcolors to cvec members and fill in arcs.
///
/// For each chr represented by the cvec, does the equivalent of
/// `newarc(v->nfa, PLAIN, subcolor(v->cm, c), lp, rp)`, deduplicating adjacent
/// identical subcolors via `lastsubcolor`. Iterates the cvec's `chrs`, then its
/// `ranges`, then its locale cclass (if any). Arc emission routes through
/// [`crate::regex_nfa::newarc`] (the NFA family that owns `regc_nfa.c`).
pub fn subcolorcvec<'mcx>(
    mcx: Mcx<'mcx>,
    nfa: &mut Nfa,
    cm: &mut ColorMap,
    cv: &Cvec,
    lp: StateId,
    rp: StateId,
) -> RegResult<()> {
    let mut lastsubcolor: color = COLORLESS;

    // ordinary characters
    for ch in cv.chrs.iter().copied() {
        subcoloronechr(mcx, nfa, cm, ch, lp, rp, &mut lastsubcolor)?;
    }

    // and the ranges
    for r in cv.ranges.iter().copied() {
        let to = r.to;
        let mut from = r.from;
        if from <= MAX_SIMPLE_CHR {
            // deal with simple chars one at a time
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
        // deal with any part of the range that's above MAX_SIMPLE_CHR
        if from < to {
            subcoloronerange(mcx, nfa, cm, from, to, lp, rp, &mut lastsubcolor)?;
        } else if from == to {
            subcoloronechr(mcx, nfa, cm, from, lp, rp, &mut lastsubcolor)?;
        }
    }

    // and deal with cclass if any
    if cv.cclasscode >= 0 {
        let cc = cv.cclasscode as usize;
        // Enlarge array if we don't have a column bit assignment for cclass.
        if cm.classbits[cc] == 0 {
            cm.classbits[cc] = cm.hiarraycols;
            newhicolorcols(mcx, cm)?;
        }
        // Apply subcolorhi() and make arc for each entry in relevant cols.
        let classbit = cm.classbits[cc];
        let rows = cm.hiarrayrows;
        let cols = cm.hiarraycols;
        for r in 0..rows {
            for c in 0..cols {
                if (c & classbit) != 0 {
                    let hi_idx = (r * cols + c) as usize;
                    let sco = subcolorhi(mcx, cm, hi_idx)?;
                    // add the arc if needed
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

/// `subcoloronechr(...)` — do `subcolorcvec`'s work for a singleton chr.
///
/// Handles both low and high chr codes. The low case is the easy `subcolor`
/// path; the high case splits at most one existing range into the pieces
/// before/at/after `ch` (cloning rows as needed) and rebuilds `cmranges`.
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
    // Easy case for low chr codes.
    if ch <= MAX_SIMPLE_CHR {
        let sco = subcolor(mcx, cm, ch)?;
        if sco != *lastsubcolor {
            crate::regex_nfa::newarc(mcx, nfa, cm, false, PLAIN, sco, lp, rp)?;
            *lastsubcolor = sco;
        }
        return Ok(());
    }

    // Snapshot the old ranges; build a fresh list and store it back at the end,
    // exactly as C builds a separate `newranges` allocation. The snapshot is
    // required because newhicolorrow()/subcoloronerow() mutate `cm` while we
    // still read the old ranges. C MALLOCs (cm->numcmranges + 2) ranges.
    let oldranges = core::mem::take(&mut cm.cmranges);
    let numold = oldranges.len();
    let mut newranges: alloc::vec::Vec<ColorMapRange> = alloc::vec::Vec::new();
    newranges.try_reserve_exact(numold + 2)?;
    let mut oldrangen: usize = 0;
    let newrow: i32;

    // Ranges before target are unchanged.
    while oldrangen < numold {
        if oldranges[oldrangen].cmax >= ch {
            break;
        }
        newranges.push(oldranges[oldrangen]);
        oldrangen += 1;
    }

    // Match target chr against current range.
    if oldrangen >= numold || oldranges[oldrangen].cmin > ch {
        // chr does not belong to any existing range, make a new one.
        // row state should be cloned from the "all others" row
        newrow = newhicolorrow(mcx, cm, 0)?;
        newranges.push(ColorMapRange {
            cmin: ch,
            cmax: ch,
            rownum: newrow,
        });
    } else if oldranges[oldrangen].cmin == oldranges[oldrangen].cmax {
        // we have an existing singleton range matching the chr
        let old = oldranges[oldrangen];
        newranges.push(old);
        newrow = old.rownum;
        oldrangen += 1; // we've now fully processed this old range
    } else {
        // chr is a subset of this existing range, must split it
        let old = oldranges[oldrangen];
        if ch > old.cmin {
            // emit portion of old range before chr
            newranges.push(ColorMapRange {
                cmin: old.cmin,
                cmax: ch - 1,
                rownum: old.rownum,
            });
        }
        // emit chr as singleton range, initially cloning from range
        newrow = newhicolorrow(mcx, cm, old.rownum)?;
        newranges.push(ColorMapRange {
            cmin: ch,
            cmax: ch,
            rownum: newrow,
        });
        if ch < old.cmax {
            // emit portion of old range after chr; must clone the row if we are
            // making two new ranges from old.
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

    // Update colors in newrow and create arcs as needed.
    subcoloronerow(mcx, nfa, cm, newrow, lp, rp, lastsubcolor)?;

    // Ranges after target are unchanged.
    while oldrangen < numold {
        newranges.push(oldranges[oldrangen]);
        oldrangen += 1;
    }

    // Assert our original space estimate was adequate.
    debug_assert!(newranges.len() <= numold + 2);

    // And finally, store back the updated list of ranges (the old one is
    // dropped here, which is C's FREE(cm->cmranges)).
    cm.cmranges = newranges;
    Ok(())
}

/// `subcoloronerange(...)` — do `subcolorcvec`'s work for a high range.
///
/// Walks the existing ranges that overlap `[from, to]`, splitting each into the
/// up-to-three pieces (before / common / after) exactly as C does, advancing
/// `from` as characters are consumed, and emitting "gap" ranges for the parts
/// of `[from, to]` not covered by any old range. The worst-case result size is
/// `2N+1` ranges (C MALLOCs `cm->numcmranges * 2 + 1`).
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
    // Caller should take care of non-high-range cases.
    debug_assert!(from_in > MAX_SIMPLE_CHR);
    debug_assert!(from_in < to);

    let mut from = from_in;

    // Snapshot the old ranges (see subcoloronechr for why).
    let oldranges = core::mem::take(&mut cm.cmranges);
    let numold = oldranges.len();
    let mut newranges: alloc::vec::Vec<ColorMapRange> = alloc::vec::Vec::new();
    newranges.try_reserve_exact(numold * 2 + 1)?;
    let mut oldrangen: usize = 0;

    // Ranges before target are unchanged.
    while oldrangen < numold {
        if oldranges[oldrangen].cmax >= from {
            break;
        }
        newranges.push(oldranges[oldrangen]);
        oldrangen += 1;
    }

    // Deal with ranges that (partially) overlap the target.  As we process each
    // such range, increase "from" to remove the dealt-with characters.
    while oldrangen < numold && oldranges[oldrangen].cmin <= to {
        let old = oldranges[oldrangen];
        let mut newrow: i32;

        if from < old.cmin {
            // Handle portion of new range that corresponds to no old range.
            // row state should be cloned from the "all others" row
            newrow = newhicolorrow(mcx, cm, 0)?;
            newranges.push(ColorMapRange {
                cmin: from,
                cmax: old.cmin - 1,
                rownum: newrow,
            });
            // Update colors in newrow and create arcs as needed.
            subcoloronerow(mcx, nfa, cm, newrow, lp, rp, lastsubcolor)?;
            // We've now fully processed the part of new range before old.
            from = old.cmin;
        }

        if from <= old.cmin && to >= old.cmax {
            // old range is fully contained in new, process it in-place
            newranges.push(old);
            newrow = old.rownum;
            from = old.cmax + 1;
        } else {
            // some part of old range does not overlap new range
            if from > old.cmin {
                // emit portion of old range before new range
                newranges.push(ColorMapRange {
                    cmin: old.cmin,
                    cmax: from - 1,
                    rownum: old.rownum,
                });
            }
            // emit common subrange, initially cloning from old range
            newrow = newhicolorrow(mcx, cm, old.rownum)?;
            newranges.push(ColorMapRange {
                cmin: from,
                cmax: if to < old.cmax { to } else { old.cmax },
                rownum: newrow,
            });
            if to < old.cmax {
                // emit portion of old range after new range; must clone the row
                // if we are making two new ranges from old.
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
        // Update colors in newrow and create arcs as needed.
        subcoloronerow(mcx, nfa, cm, newrow, lp, rp, lastsubcolor)?;
        oldrangen += 1; // we've now fully processed this old range
    }

    if from <= to {
        // Handle portion of new range that corresponds to no old range.
        // row state should be cloned from the "all others" row
        let newrow = newhicolorrow(mcx, cm, 0)?;
        newranges.push(ColorMapRange {
            cmin: from,
            cmax: to,
            rownum: newrow,
        });
        // Update colors in newrow and create arcs as needed.
        subcoloronerow(mcx, nfa, cm, newrow, lp, rp, lastsubcolor)?;
    }

    // Ranges after target are unchanged.
    while oldrangen < numold {
        newranges.push(oldranges[oldrangen]);
        oldrangen += 1;
    }

    // Assert our original space estimate was adequate.
    debug_assert!(newranges.len() <= numold * 2 + 1);

    // And finally, store back the updated list of ranges (old one dropped here).
    cm.cmranges = newranges;
    Ok(())
}

/// `subcoloronerow(...)` — do `subcolorcvec`'s work for one new row in the high
/// colormap. Applies [`subcolorhi`] to every entry in the row and emits a
/// deduplicated arc for each distinct subcolor.
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
    // Apply subcolorhi() and make arc for each entry in row.
    for i in 0..cols as usize {
        let sco = subcolorhi(mcx, cm, base + i)?;
        // make the arc if needed
        if sco != *lastsubcolor {
            crate::regex_nfa::newarc(mcx, nfa, cm, false, PLAIN, sco, lp, rp)?;
            *lastsubcolor = sco;
        }
    }
    Ok(())
}
