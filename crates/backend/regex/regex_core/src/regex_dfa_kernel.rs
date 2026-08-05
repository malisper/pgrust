//! Pointer-shaped sset access — the DFA exec inner loops, mirroring C's
//! `struct sset` raw `outs`/`inchain` pointers (rege_dfa.c). This module is
//! the only unsafe in regex_core; everything else stays fully checked.
//!
//! Soundness rests on invariants established by the safe engine
//! (regex_exec.rs). `Rows::new` re-checks the length relations with real
//! asserts once per entry (per miss/getvacant/scan-segment, never per char);
//! every raw index step carries a debug_assert of the per-index invariant.
//!
//! INV-1 (lengths): `nssused <= nssets <= ssets.len()` and
//!   `nssets * ncolors <= outs.len(), incarea.len()` — dfa_from_parts slices
//!   each array to exactly nss >= nssets rows.
//! INV-2 (bases): for every i < nssused, `ssets[i].outs_base ==
//!   ssets[i].inchain_base == i * ncolors` — written only by pickss.
//! INV-3 (out entries): every non-NOSS value in a live outs row is
//!   `p as u32` with p < nssused — written only by `install_out`; pickss
//!   resets the whole row to NOSS before the sset goes live (so dirty
//!   thread-local scratch is never read), getvacant writes only NOSS.
//! INV-4 (colors): every color indexing an outs/incarea row comes from the
//!   colormap (getcolor / cnfa.bos / cnfa.eos) of the same compile that set
//!   `cnfa.ncolors = maxcolor(cm) + 1`, so `0 <= co < ncolors`. LACON
//!   colors (>= ncolors) never reach an index: miss() filters them in its
//!   arc walk and resolves them without touching outs.
//! INV-5 (chains): every non-NOSS Arcp in `ins`/`incarea` carries
//!   `ss < nssused` and `0 <= co < ncolors` — written only by
//!   `install_out` from values that satisfy INV-3/INV-4.
//!
//! The stateset builder additionally trusts the compile-produced Cnfa
//! (regex_nfa.rs compact()), re-checking the cheap length relations on
//! entry:
//!
//! INV-6 (arc targets): every `Carc.to` satisfies `0 <= to < nstates`,
//!   `stflags.len() == states.len() == nstates <= wordsper * UBITS`.
//! INV-7 (arc ranges): every `states[i]` range is in-bounds of `arcs`.
//! INV-8 (bitmaps): a live sset's states bitmap only has bits < nstates set
//!   — written whole-word from cnfa.pre (initialize) or from d.work, whose
//!   bits are arc targets (INV-6).

use core::marker::PhantomData;

use crate::regex_consts::REG_ASSERT;
use crate::regex_error::{RegError, RegResult};
use crate::regex_exec::{pickss, Arcp, Dfa, Pos, Sset, LOCKED, NOPROGRESS, NOSS, POSTSTATE, UBITS};
use crate::regguts::{chr, color, Cnfa, ColorMap, CHR_MIN, CNFA_NOPROGRESS, MAX_SIMPLE_CHR, RAINBOW};

struct Rows<'a> {
    ssets: *mut Sset,
    outs: *mut u32,
    incarea: *mut Arcp,
    nssused: usize,
    ncolors: usize,
    _pd: PhantomData<&'a mut ()>,
}

impl<'a> Rows<'a> {
    #[inline(always)]
    fn new(d: &'a mut Dfa<'_>) -> Rows<'a> {
        // INV-1 was asserted at construction (dfa_from_parts): all raw
        // offsets below stay inside the allocations for any i < nssused,
        // co < ncolors.
        debug_assert!(d.nssused <= d.nssets && d.nssets <= d.ssets.len());
        debug_assert!(d.nssets * d.ncolors <= d.outs.len());
        debug_assert!(d.nssets * d.ncolors <= d.incarea.len());
        Rows {
            ssets: d.ssets.as_mut_ptr(),
            outs: d.outs.as_mut_ptr(),
            incarea: d.incarea.as_mut_ptr(),
            nssused: d.nssused,
            ncolors: d.ncolors,
            _pd: PhantomData,
        }
    }

    /// SAFETY: caller guarantees i < nssused (INV-3/INV-5 provenance).
    #[inline(always)]
    unsafe fn sset(&self, i: usize) -> *mut Sset {
        debug_assert!(i < self.nssused);
        unsafe { self.ssets.add(i) }
    }

    /// SAFETY: i < nssused and 0 <= co < ncolors (INV-4). Colors are
    /// non-negative here, so the u16 hop zero-extends (no sxth per access).
    #[inline(always)]
    unsafe fn row(&self, i: usize, co: color) -> usize {
        debug_assert!(co >= 0 && (co as usize) < self.ncolors);
        let base = unsafe { (*self.sset(i)).outs_base };
        debug_assert!(base == i * self.ncolors);
        debug_assert_eq!(base, unsafe { (*self.sset(i)).inchain_base });
        base + co as u16 as usize
    }

    /// SAFETY: i < nssused, 0 <= co < ncolors.
    #[inline(always)]
    unsafe fn out(&self, i: usize, co: color) -> u32 {
        unsafe { *self.outs.add(self.row(i, co)) }
    }

    /// SAFETY: i < nssused, 0 <= co < ncolors; v maintains INV-3.
    #[inline(always)]
    unsafe fn set_out(&mut self, i: usize, co: color, v: u32) {
        debug_assert!(v == NOSS || (v as usize) < self.nssused);
        unsafe { *self.outs.add(self.row(i, co)) = v }
    }

    /// SAFETY: i < nssused, 0 <= co < ncolors.
    #[inline(always)]
    unsafe fn inchain(&self, i: usize, co: color) -> Arcp {
        unsafe { *self.incarea.add(self.row(i, co)) }
    }

    /// SAFETY: i < nssused, 0 <= co < ncolors; v maintains INV-5.
    #[inline(always)]
    unsafe fn set_inchain(&mut self, i: usize, co: color, v: Arcp) {
        debug_assert!(v.ss == NOSS || (v.ss as usize) < self.nssused);
        unsafe { *self.incarea.add(self.row(i, co)) = v }
    }

    /// SAFETY: i < nssused, 0 <= co < ncolors.
    #[inline(always)]
    unsafe fn set_inchain_ss(&mut self, i: usize, co: color, ss: u32) {
        unsafe { (*self.incarea.add(self.row(i, co))).ss = ss }
    }

    /// SAFETY: i < nssused.
    #[inline(always)]
    unsafe fn ins(&self, i: usize) -> Arcp {
        unsafe { (*self.sset(i)).ins }
    }

    /// SAFETY: i < nssused; v maintains INV-5.
    #[inline(always)]
    unsafe fn set_ins(&mut self, i: usize, v: Arcp) {
        debug_assert!(v.ss == NOSS || (v.ss as usize) < self.nssused);
        unsafe { (*self.sset(i)).ins = v }
    }

    /// SAFETY: i < nssused.
    #[inline(always)]
    unsafe fn flags(&self, i: usize) -> i32 {
        unsafe { (*self.sset(i)).flags }
    }

    /// SAFETY: i < nssused.
    #[inline(always)]
    unsafe fn set_lastseen(&mut self, i: usize, v: Pos) {
        unsafe { (*self.sset(i)).lastseen = v }
    }
}

pub(crate) enum Scan {
    Miss { cp: usize, css: usize, co: color },
    End { cp: usize, css: usize },
    Post { cp: usize, css: usize },
}

const LOCO_LEN: usize = (MAX_SIMPLE_CHR - CHR_MIN + 1) as usize;

/// C's main text-scanning loop in longest() (rege_dfa.c): follow cached
/// outs until a miss or end of input. `input` is pre-sliced to realstop.
pub(crate) fn scan_longest(
    d: &mut Dfa<'_>,
    cm: &ColorMap,
    input: &[chr],
    mut cp: usize,
    mut css: usize,
) -> Scan {
    assert!(css < d.nssused);
    let mut r = Rows::new(d);
    let locolormap = &cm.locolormap[..LOCO_LEN];
    while cp < input.len() {
        let c = input[cp];
        let co = if c <= MAX_SIMPLE_CHR {
            locolormap[(c - CHR_MIN) as usize]
        } else {
            crate::regex_foundation::pg_reg_getcolor(cm, c)
        };
        // SAFETY: css < nssused (asserted on entry, then INV-3 provenance);
        // co < ncolors (INV-4).
        let hit = unsafe { r.out(css, co) };
        if hit == NOSS {
            return Scan::Miss { cp, css, co };
        }
        cp += 1;
        let ss = hit as usize;
        // SAFETY: hit != NOSS so ss < nssused (INV-3).
        unsafe { r.set_lastseen(ss, Pos::at(cp)) };
        css = ss;
    }
    Scan::End { cp, css }
}

/// shortest()'s variant: also breaks on a POSTSTATE sset once cp >= realmin.
pub(crate) fn scan_shortest(
    d: &mut Dfa<'_>,
    cm: &ColorMap,
    input: &[chr],
    mut cp: usize,
    mut css: usize,
    realmin: usize,
) -> Scan {
    assert!(css < d.nssused);
    let mut r = Rows::new(d);
    let locolormap = &cm.locolormap[..LOCO_LEN];
    while cp < input.len() {
        let c = input[cp];
        let co = if c <= MAX_SIMPLE_CHR {
            locolormap[(c - CHR_MIN) as usize]
        } else {
            crate::regex_foundation::pg_reg_getcolor(cm, c)
        };
        // SAFETY: css < nssused; co < ncolors (INV-3/INV-4).
        let hit = unsafe { r.out(css, co) };
        if hit == NOSS {
            return Scan::Miss { cp, css, co };
        }
        cp += 1;
        let ss = hit as usize;
        // SAFETY: hit != NOSS so ss < nssused (INV-3).
        unsafe {
            r.set_lastseen(ss, Pos::at(cp));
            css = ss;
            if (r.flags(ss) & POSTSTATE) != 0 && cp >= realmin {
                return Scan::Post { cp, css };
            }
        }
    }
    Scan::End { cp, css }
}

/// C's getvacant(): pick a replaceable sset and unlink it from all chains.
pub(crate) fn getvacant(d: &mut Dfa<'_>, cp: usize, start: usize) -> RegResult<usize> {
    let ss = pickss(d, cp, start)?;
    debug_assert!((d.ssets[ss].flags & LOCKED) == 0);
    let ncolors = d.ncolors;
    {
        let mut r = Rows::new(d);
        // SAFETY: ss < nssused (pickss); every ap traversed satisfies INV-5,
        // every color index below is < ncolors (loop bound / INV-5).
        unsafe {
            let mut ap = r.ins(ss);
            while ap.ss != NOSS {
                let p = ap.ss as usize;
                let coi = ap.co;
                r.set_out(p, coi, NOSS);
                ap = r.inchain(p, coi);
                r.set_inchain_ss(p, coi, NOSS);
            }
            (*r.sset(ss)).ins.ss = NOSS;

            for i in 0..ncolors {
                let i = i as color;
                let p32 = r.out(ss, i);
                if p32 == NOSS {
                    continue; // NOTE CONTINUE
                }
                let p = p32 as usize;
                debug_assert!(p != ss); // not self-referential

                let pins = r.ins(p);
                if pins.ss == ss as u32 && pins.co == i {
                    r.set_ins(p, r.inchain(ss, i));
                } else {
                    let mut lastap = Arcp::null();
                    debug_assert!(pins.ss != NOSS);
                    let mut ap = pins;
                    while ap.ss != NOSS {
                        if ap.ss as usize == ss && ap.co == i {
                            break;
                        }
                        lastap = ap;
                        ap = r.inchain(ap.ss as usize, ap.co);
                    }
                    debug_assert!(ap.ss != NOSS);
                    if lastap.ss == NOSS {
                        return Err(RegError(REG_ASSERT));
                    }
                    let val = r.inchain(ss, i);
                    r.set_inchain(lastap.ss as usize, lastap.co, val);
                }
                r.set_out(ss, i, NOSS);
                r.set_inchain_ss(ss, i, NOSS);
            }
        }
    }

    if (d.ssets[ss].flags & POSTSTATE) != 0 && d.lastpost < d.ssets[ss].lastseen {
        d.lastpost = d.ssets[ss].lastseen;
    }
    if (d.ssets[ss].flags & NOPROGRESS) != 0 && d.lastnopr < d.ssets[ss].lastseen {
        d.lastnopr = d.ssets[ss].lastseen;
    }

    Ok(ss)
}

pub(crate) struct Built {
    pub gotstate: bool,
    pub ispost: bool,
    pub noprogress: bool,
}

/// miss()'s stateset construction: union of arcs colored `co` out of css's
/// states, built into d.work. C walks css->states / cnfa arc chains as raw
/// pointers; word-wise set-bit iteration additionally skips dead states.
pub(crate) fn build_stateset(
    d: &mut Dfa<'_>,
    cnfa: &Cnfa,
    css: usize,
    co: color,
    ispseudocolor: bool,
) -> Built {
    let wordsper = d.wordsper;
    let nstates = cnfa.nstates as usize;
    assert!(css < d.nssused);
    assert!(cnfa.states.len() == nstates && cnfa.stflags.len() == nstates);
    assert!(nstates <= wordsper * UBITS);
    let base = d.ssets[css].states_base;
    assert!(base + wordsper <= d.statesarea.len() && d.work.len() == wordsper);
    debug_assert!(base == css * wordsper);

    let statesarea: *const u32 = d.statesarea.as_ptr();
    let work = &mut d.work[..];
    if let [w] = &mut work[..] {
        *w = 0;
    } else {
        work.fill(0);
    }
    let arcs = cnfa.arcs.as_ptr();
    let states = cnfa.states.as_ptr();
    let stflags = cnfa.stflags.as_ptr();
    let post = cnfa.post;
    let mut gotstate = false;
    let mut ispost = false;
    let mut noprogress = true;
    // SAFETY: word reads are within base..base+wordsper (asserted); set bits
    // are < nstates (INV-8) indexing states/stflags of len nstates
    // (asserted); arc ranges are in-bounds of arcs (INV-7); work writes are
    // at arc targets < nstates <= wordsper*UBITS (INV-6, asserted bound).
    unsafe {
        for w in 0..wordsper {
            let mut bits = *statesarea.add(base + w);
            while bits != 0 {
                let i = w * UBITS + bits.trailing_zeros() as usize;
                bits &= bits - 1;
                debug_assert!(i < nstates);
                let r = (*states.add(i)).clone();
                debug_assert!(r.start <= r.end && r.end <= cnfa.arcs.len());
                let mut a = arcs.add(r.start);
                let aend = arcs.add(r.end);
                while a != aend {
                    let ca = *a;
                    a = a.add(1);
                    if ca.co == co || (ca.co == RAINBOW && !ispseudocolor) {
                        let t = ca.to as usize;
                        debug_assert!(t < nstates);
                        *work.get_unchecked_mut(t / UBITS) |= 1u32 << (t % UBITS);
                        gotstate = true;
                        if ca.to == post {
                            ispost = true;
                        }
                        if (*stflags.add(t) & CNFA_NOPROGRESS) == 0 {
                            noprogress = false;
                        }
                    }
                }
            }
        }
    }
    Built { gotstate, ispost, noprogress }
}

/// miss()'s entry probe: the cached transition, NOSS on a true miss.
#[inline(always)]
pub(crate) fn cached_out(d: &mut Dfa<'_>, css: usize, co: color) -> u32 {
    assert!(css < d.nssused);
    let r = Rows::new(d);
    // SAFETY: css < nssused (asserted); co < ncolors (INV-4).
    unsafe { r.out(css, co) }
}

/// miss()'s cache-install tail: link p as css's out for co.
pub(crate) fn install_out(d: &mut Dfa<'_>, css: usize, co: color, p: usize) {
    assert!(css < d.nssused && p < d.nssused);
    let mut r = Rows::new(d);
    // SAFETY: css, p < nssused (asserted); co < ncolors (INV-4); the writes
    // are exactly the INV-3/INV-5 producers.
    unsafe {
        r.set_out(css, co, p as u32);
        let pins = r.ins(p);
        r.set_inchain(css, co, pins);
        r.set_ins(p, Arcp { ss: css as u32, co });
    }
}
