
extern crate alloc;

use alloc::boxed::Box;
use alloc::vec::Vec;

use ::mcx::Mcx;

use crate::regex_consts::{
    latype_is_ahead, latype_is_pos, DUPINF, REG_ASSERT, REG_ESPACE, REG_ETOOBIG, REG_EXACT,
    REG_EXPECT, REG_NOMATCH, REG_NOSUB, REG_NOTBOL, REG_NOTEOL, REG_OKAY, REG_PREFIX, REG_SMALL,
    REG_UBACKREF, REG_UIMPOSSIBLE,
};
use crate::regex_error::{RegError, RegResult};
use crate::regguts::{
    chr, color, Cnfa, ColorMap, Guts, NodeId, Subre, BACKR, CHR_MIN, CNFA_NOPROGRESS, COLORLESS,
    HASLACONS, MATCHALL, MAX_SIMPLE_CHR, PSEUDO, RAINBOW, SHORTER, WHITE,
};
use ::regex::{pg_regoff_t, RegMatch};

pub const DEFAULT_MAX_DEPTH: u32 = 10_000;


pub const UBITS: usize = 32;

#[inline]
pub fn bset(uv: &mut [u32], sn: usize) {
    uv[sn / UBITS] |= 1u32 << (sn % UBITS);
}

#[inline]
pub fn isbset(uv: &[u32], sn: usize) -> bool {
    (uv[sn / UBITS] & (1u32 << (sn % UBITS))) != 0
}


pub const STARTER: i32 = 0o01;
pub const POSTSTATE: i32 = 0o02;
pub const LOCKED: i32 = 0o04;
pub const NOPROGRESS: i32 = 0o010;


pub const WORK: usize = 1;

pub const REG_SMALL_NSSETS: usize = 7;


// C moves 8-byte pointers with NULL sentinels through the DFA exec loop;
// Option<usize> here cost 16-byte moves + decode branches per transition
// (re_* lanes). NOSS mirrors NULL for sset indices, Pos(0) for chr*.
pub const NOSS: u32 = u32::MAX;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Arcp {
    pub ss: u32,
    pub co: color,
}

impl Arcp {
    #[inline]
    pub const fn null() -> Self {
        Arcp { ss: NOSS, co: WHITE }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Pos(usize);

impl Pos {
    pub const NONE: Pos = Pos(0);

    #[inline]
    pub fn at(cp: usize) -> Pos {
        Pos(cp + 1)
    }

    #[inline]
    pub fn is_none(self) -> bool {
        self.0 == 0
    }

    #[inline]
    pub fn get(self) -> usize {
        debug_assert!(self.0 != 0);
        self.0 - 1
    }
}

#[derive(Copy, Clone, Debug)]
pub struct Sset {
    pub states_base: usize,
    pub hash: u32,
    pub flags: i32,
    pub ins: Arcp,
    pub lastseen: Pos,
    pub outs_base: usize,
    pub inchain_base: usize,
}

impl Sset {
    #[inline]
    const fn blank() -> Self {
        Sset {
            states_base: 0,
            hash: 0,
            flags: 0,
            ins: Arcp::null(),
            lastseen: Pos::NONE,
            outs_base: 0,
            inchain_base: 0,
        }
    }
}

// C's smalldfa (regexec.c): DFAs whose NFA fits these bounds run in
// caller-owned fixed arrays (stack for find/cfind, one boxed block for
// sub-DFAs) — a warm pg_regexec on a small pattern allocates zero. Same
// byte budget as C's smalldfa (its arrays are FEWSTATES*2 = 40 ssets); we
// admit up to the physical capacity where C's guard stops at nss <= 20.
pub const FEWSTATES: usize = 40;
pub const FEWCOLORS: usize = 15;

pub struct SmallDfaSpace {
    ssets: [Sset; FEWSTATES],
    statesarea: [u32; FEWSTATES + WORK],
    outs: [u32; FEWSTATES * FEWCOLORS],
    incarea: [Arcp; FEWSTATES * FEWCOLORS],
}

impl SmallDfaSpace {
    // All-zero contents (one memset; pickss initializes every row before use).
    #[inline(always)]
    fn new() -> Self {
        let zsset = Sset {
            states_base: 0,
            hash: 0,
            flags: 0,
            ins: Arcp { ss: 0, co: WHITE },
            lastseen: Pos::NONE,
            outs_base: 0,
            inchain_base: 0,
        };
        SmallDfaSpace {
            ssets: [zsset; FEWSTATES],
            statesarea: [0; FEWSTATES + WORK],
            outs: [0; FEWSTATES * FEWCOLORS],
            incarea: [Arcp { ss: 0, co: WHITE }; FEWSTATES * FEWCOLORS],
        }
    }
}

// C's vars carries two stack smalldfas (dfa1/dfa2, uninitialized memory).
// The safe-Rust equivalent of that zero-cost space is a retained per-thread
// pair: zeroed once per thread, dirty reuse after (every row is written by
// pickss before it is read, same discipline C relies on for its stack).
pub struct ExecScratch {
    s1: SmallDfaSpace,
    s2: SmallDfaSpace,
}

impl ExecScratch {
    fn new() -> ExecScratch {
        ExecScratch {
            s1: SmallDfaSpace::new(),
            s2: SmallDfaSpace::new(),
        }
    }
}

std::thread_local! {
    static EXEC_SCRATCH: core::cell::RefCell<Option<Box<ExecScratch>>> =
        const { core::cell::RefCell::new(None) };
}

fn small_ok(cnfa: &Cnfa) -> bool {
    (cnfa.nstates as usize) * 2 <= FEWSTATES && (cnfa.ncolors as usize) <= FEWCOLORS
}

fn zeroed_u32_vec(n: usize) -> RegResult<Vec<u32>> {
    if usize::BITS < 64 && n > isize::MAX as usize / 4 {
        return Err(RegError(REG_ESPACE));
    }
    Ok(alloc::vec![0u32; n])
}

pub struct HeapSpace {
    ssets: Vec<Sset>,
    statesarea: Vec<u32>,
    outs: Vec<u32>,
    incarea: Vec<Arcp>,
}

impl HeapSpace {
    fn for_cnfa(cnfa: &Cnfa) -> RegResult<HeapSpace> {
        let nss: usize = (cnfa.nstates as usize)
            .checked_mul(2)
            .ok_or(RegError(REG_ESPACE))?;
        let wordsper: usize = (cnfa.nstates as usize).div_ceil(UBITS);
        let ncolors: usize = cnfa.ncolors as usize;
        let statesarea_words = nss
            .checked_add(WORK)
            .and_then(|n| n.checked_mul(wordsper))
            .ok_or(RegError(REG_ESPACE))?;
        let vec_area = nss.checked_mul(ncolors).ok_or(RegError(REG_ESPACE))?;
        Ok(HeapSpace {
            ssets: fill_vec(nss, Sset::blank())?,
            statesarea: zeroed_u32_vec(statesarea_words)?,
            outs: zeroed_u32_vec(vec_area)?,
            incarea: fill_vec(vec_area, Arcp::null())?,
        })
    }
}

pub enum DfaSpace {
    Small(SmallDfaSpace),
    Heap(HeapSpace),
}

impl DfaSpace {
    fn for_cnfa(cnfa: &Cnfa) -> RegResult<DfaSpace> {
        debug_assert!(cnfa.nstates != 0);
        if small_ok(cnfa) {
            Ok(DfaSpace::Small(SmallDfaSpace::new()))
        } else {
            Ok(DfaSpace::Heap(HeapSpace::for_cnfa(cnfa)?))
        }
    }
}

#[derive(Copy, Clone)]
pub struct DfaMeta {
    pub nssets: usize,
    pub nssused: usize,
    pub nstates: usize,
    pub ncolors: usize,
    pub wordsper: usize,
    pub lastpost: Pos,
    pub lastnopr: Pos,
    pub search: usize,
    pub backno: i32,
    pub backmin: i16,
    pub backmax: i16,
}

impl DfaMeta {
    fn new(eflags: i32, cnfa: &Cnfa) -> DfaMeta {
        let nss = (cnfa.nstates as usize) * 2;
        DfaMeta {
            nssets: if (eflags & REG_SMALL) != 0 {
                REG_SMALL_NSSETS
            } else {
                nss
            },
            nssused: 0,
            nstates: cnfa.nstates as usize,
            ncolors: cnfa.ncolors as usize,
            wordsper: (cnfa.nstates as usize).div_ceil(UBITS),
            lastpost: Pos::NONE,
            lastnopr: Pos::NONE,
            search: 0,
            backno: -1,
            backmin: 0,
            backmax: 0,
        }
    }
}

pub struct SubDfa {
    meta: DfaMeta,
    space: Box<DfaSpace>,
}

impl SubDfa {
    fn new(eflags: i32, cnfa: &Cnfa) -> RegResult<SubDfa> {
        Ok(SubDfa {
            meta: DfaMeta::new(eflags, cnfa),
            space: Box::new(DfaSpace::for_cnfa(cnfa)?),
        })
    }

    fn with<R>(&mut self, f: impl FnOnce(&mut Dfa) -> R) -> R {
        let mut d = make_dfa(self.meta, &mut self.space);
        let r = f(&mut d);
        self.meta = d.meta();
        r
    }
}

pub struct Dfa<'s> {
    pub nssets: usize,
    pub nssused: usize,
    pub nstates: usize,
    pub ncolors: usize,
    pub wordsper: usize,
    pub ssets: &'s mut [Sset],
    pub statesarea: &'s mut [u32],
    pub work: &'s mut [u32],
    pub outs: &'s mut [u32],
    pub incarea: &'s mut [Arcp],
    pub lastpost: Pos,
    pub lastnopr: Pos,
    pub search: usize,
    pub backno: i32,
    pub backmin: i16,
    pub backmax: i16,
}

impl Dfa<'_> {
    fn meta(&self) -> DfaMeta {
        DfaMeta {
            nssets: self.nssets,
            nssused: self.nssused,
            nstates: self.nstates,
            ncolors: self.ncolors,
            wordsper: self.wordsper,
            lastpost: self.lastpost,
            lastnopr: self.lastnopr,
            search: self.search,
            backno: self.backno,
            backmin: self.backmin,
            backmax: self.backmax,
        }
    }
    #[inline]
    fn hashbits(&self, bv: &[u32]) -> u32 {
        if self.wordsper == 1 {
            bv[0]
        } else {
            hash(bv, self.wordsper)
        }
    }

    #[inline]
    fn sset_states(&self, i: usize) -> &[u32] {
        let base = self.ssets[i].states_base;
        &self.statesarea[base..base + self.wordsper]
    }

    #[inline]
    fn sset_states_mut(&mut self, i: usize) -> &mut [u32] {
        let base = self.ssets[i].states_base;
        &mut self.statesarea[base..base + self.wordsper]
    }

    #[inline]
    fn out(&self, css: usize, co: color) -> u32 {
        self.outs[self.ssets[css].outs_base + co as usize]
    }
}


pub struct ExecVars<'a> {
    pub eflags: i32,
    pub nmatch: usize,
    pub pmatch: &'a mut [(isize, isize)],
    pub input: &'a [chr],
    pub start: usize,
    pub search_start: usize,
    pub stop: usize,
    pub depth: u32,
    pub ntree: usize,
    pub subdfas: Vec<Option<SubDfa>>,
    pub ladfas: Vec<Option<SubDfa>>,
    pub lblastcss: Vec<Option<usize>>,
    pub lblastcp: Vec<Option<usize>>,
    pub max_depth: u32,
}


fn dfa_from_parts<'s>(
    meta: DfaMeta,
    ssets: &'s mut [Sset],
    statesarea_full: &'s mut [u32],
    outs: &'s mut [u32],
    incarea: &'s mut [Arcp],
) -> Dfa<'s> {
    let nss = meta.nstates * 2;
    let nssw = nss * meta.wordsper;
    let (statesarea, work) = statesarea_full[..nssw + WORK * meta.wordsper].split_at_mut(nssw);
    Dfa {
        nssets: meta.nssets,
        nssused: meta.nssused,
        nstates: meta.nstates,
        ncolors: meta.ncolors,
        wordsper: meta.wordsper,
        ssets: &mut ssets[..nss],
        statesarea,
        work,
        outs: &mut outs[..nss * meta.ncolors],
        incarea: &mut incarea[..nss * meta.ncolors],
        lastpost: meta.lastpost,
        lastnopr: meta.lastnopr,
        search: meta.search,
        backno: meta.backno,
        backmin: meta.backmin,
        backmax: meta.backmax,
    }
}

fn make_dfa<'s>(meta: DfaMeta, space: &'s mut DfaSpace) -> Dfa<'s> {
    match space {
        DfaSpace::Small(sml) => {
            debug_assert!(meta.wordsper == 1);
            dfa_from_parts(
                meta,
                &mut sml.ssets,
                &mut sml.statesarea,
                &mut sml.outs,
                &mut sml.incarea,
            )
        }
        DfaSpace::Heap(h) => dfa_from_parts(
            meta,
            &mut h.ssets,
            &mut h.statesarea,
            &mut h.outs,
            &mut h.incarea,
        ),
    }
}

pub fn newdfa<'s>(
    eflags: i32,
    cnfa: &Cnfa,
    sml: &'s mut SmallDfaSpace,
    heap: &'s mut Option<HeapSpace>,
) -> RegResult<Dfa<'s>> {
    debug_assert!(cnfa.nstates != 0);
    let meta = DfaMeta::new(eflags, cnfa);
    if small_ok(cnfa) {
        debug_assert!(meta.wordsper == 1);
        Ok(dfa_from_parts(
            meta,
            &mut sml.ssets,
            &mut sml.statesarea,
            &mut sml.outs,
            &mut sml.incarea,
        ))
    } else {
        let h = heap.insert(HeapSpace::for_cnfa(cnfa)?);
        Ok(dfa_from_parts(
            meta,
            &mut h.ssets,
            &mut h.statesarea,
            &mut h.outs,
            &mut h.incarea,
        ))
    }
}

fn fill_vec<T: Copy>(n: usize, val: T) -> RegResult<Vec<T>> {
    let mut v: Vec<T> = Vec::new();
    v.try_reserve_exact(n)?;
    v.resize(n, val);
    Ok(v)
}


pub fn hash(uv: &[u32], n: usize) -> u32 {
    uv[..n].iter().fold(0u32, |h, &w| h ^ w)
}


pub fn initialize(d: &mut Dfa<'_>, cnfa: &Cnfa, start: usize) -> RegResult<Option<usize>> {
    let ss: usize = if d.nssused > 0 && (d.ssets[0].flags & STARTER) != 0 {
        0
    } else {
        let ss = match getvacant(d, start, start)? {
            Some(ss) => ss,
            None => return Ok(None),
        };
        for i in 0..d.wordsper {
            d.sset_states_mut(ss)[i] = 0;
        }
        bset(d.sset_states_mut(ss), cnfa.pre as usize);
        let h = d.hashbits(d.sset_states(ss));
        d.ssets[ss].hash = h;
        debug_assert!(cnfa.pre != cnfa.post);
        d.ssets[ss].flags = STARTER | LOCKED | NOPROGRESS;
        ss
    };

    for i in 0..d.nssused {
        d.ssets[i].lastseen = Pos::NONE;
    }
    d.ssets[ss].lastseen = Pos::at(start); // maybe untrue, but harmless
    d.lastpost = Pos::NONE;
    d.lastnopr = Pos::NONE;
    Ok(Some(ss))
}


pub fn getvacant(d: &mut Dfa<'_>, cp: usize, start: usize) -> RegResult<Option<usize>> {
    let ss = match pickss(d, cp, start)? {
        Some(ss) => ss,
        None => return Ok(None),
    };
    debug_assert!((d.ssets[ss].flags & LOCKED) == 0);

    let mut ap: Arcp = d.ssets[ss].ins;
    while ap.ss != NOSS {
        let p = ap.ss as usize;
        let co_idx = ap.co as usize;
        d.outs[d.ssets[p].outs_base + co_idx] = NOSS;
        ap = d.incarea[d.ssets[p].inchain_base + co_idx];
        d.incarea[d.ssets[p].inchain_base + co_idx].ss = NOSS;
    }
    d.ssets[ss].ins.ss = NOSS;

    for i in 0..d.ncolors {
        let p = d.outs[d.ssets[ss].outs_base + i];
        if p == NOSS {
            continue; // NOTE CONTINUE
        }
        let p = p as usize;
        debug_assert!(p != ss); // not self-referential

        if d.ssets[p].ins.ss == ss as u32 && d.ssets[p].ins.co as usize == i {
            d.ssets[p].ins = d.incarea[d.ssets[ss].inchain_base + i];
        } else {
            let mut lastap: Arcp = Arcp::null();
            debug_assert!(d.ssets[p].ins.ss != NOSS);
            let mut ap = d.ssets[p].ins;
            while ap.ss != NOSS {
                if ap.ss as usize == ss && ap.co as usize == i {
                    break;
                }
                lastap = ap;
                ap = d.incarea[d.ssets[ap.ss as usize].inchain_base + ap.co as usize];
            }
            debug_assert!(ap.ss != NOSS);
            if lastap.ss == NOSS {
                return Err(RegError(REG_ASSERT));
            }
            let lastap_ss = lastap.ss as usize;
            let val = d.incarea[d.ssets[ss].inchain_base + i];
            d.incarea[d.ssets[lastap_ss].inchain_base + lastap.co as usize] = val;
        }
        d.outs[d.ssets[ss].outs_base + i] = NOSS;
        d.incarea[d.ssets[ss].inchain_base + i].ss = NOSS;
    }

    if (d.ssets[ss].flags & POSTSTATE) != 0 && d.lastpost < d.ssets[ss].lastseen {
        d.lastpost = d.ssets[ss].lastseen;
    }

    if (d.ssets[ss].flags & NOPROGRESS) != 0 && d.lastnopr < d.ssets[ss].lastseen {
        d.lastnopr = d.ssets[ss].lastseen;
    }

    Ok(Some(ss))
}


pub fn pickss(d: &mut Dfa<'_>, cp: usize, start: usize) -> RegResult<Option<usize>> {
    debug_assert!(cp >= start);

    if d.nssused < d.nssets {
        let i = d.nssused;
        d.nssused += 1;
        let states_base = i * d.wordsper;
        let outs_base = i * d.ncolors;
        let inchain_base = i * d.ncolors;
        d.ssets[i] = Sset {
            states_base,
            hash: 0,
            flags: 0,
            ins: Arcp::null(),
            lastseen: Pos::NONE,
            outs_base,
            inchain_base,
        };
        for j in 0..d.ncolors {
            d.outs[outs_base + j] = NOSS;
            d.incarea[inchain_base + j] = Arcp::null();
        }
        return Ok(Some(i));
    }

    let span = d.nssets * 2 / 3;
    let ancient: usize = if cp - start > span { cp - span } else { start };
    let ancient = Pos::at(ancient);

    let is_vacant = |s: &Sset| s.lastseen < ancient && (s.flags & LOCKED) == 0;
    if let Some(off) = d.ssets[d.search..d.nssets].iter().position(is_vacant) {
        let ss = d.search + off;
        d.search = ss + 1;
        return Ok(Some(ss));
    }
    if let Some(ss) = d.ssets[..d.search].iter().position(is_vacant) {
        d.search = ss + 1;
        return Ok(Some(ss));
    }

    Err(RegError(REG_ASSERT))
}


#[inline]
fn getcolor(cm: &ColorMap, c: chr) -> color {
    if c <= MAX_SIMPLE_CHR {
        cm.locolormap[(c - CHR_MIN) as usize]
    } else {
        crate::regex_foundation::pg_reg_getcolor(cm, c)
    }
}


pub fn getsubdfa(v: &mut ExecVars, t: &Subre) -> RegResult<usize> {
    if v.subdfas.is_empty() {
        v.subdfas.try_reserve_exact(v.ntree)?;
        v.subdfas.resize_with(v.ntree, || None);
    }
    let id = t.id as usize;
    if v.subdfas[id].is_none() {
        let cnfa = t
            .cnfa
            .as_ref()
            .expect("getsubdfa: subre has no cnfa (NULLCNFA)");
        let mut sub = SubDfa::new(v.eflags, cnfa)?;
        if t.op == b'b' {
            sub.meta.backno = t.backno;
            sub.meta.backmin = t.min;
            sub.meta.backmax = t.max;
        }
        v.subdfas[id] = Some(sub);
    }
    Ok(id)
}

pub fn getladfa(v: &mut ExecVars, g: &Guts, n: usize) -> RegResult<usize> {
    debug_assert!(n > 0 && (n as i32) < g.nlacons);
    if v.ladfas[n].is_none() {
        let cnfa = g.lacons[n]
            .cnfa
            .as_ref()
            .expect("getladfa: lacon has no cnfa (NULLCNFA)");
        v.ladfas[n] = Some(SubDfa::new(v.eflags, cnfa)?);
    }
    Ok(n)
}


#[allow(clippy::too_many_arguments)]
fn miss(
    v: &mut ExecVars,
    g: &Guts,
    d: &mut Dfa<'_>,
    cnfa: &Cnfa,
    cm: &ColorMap,
    css: usize,
    co: color,
    cp: usize,
    start: usize,
) -> RegResult<Option<usize>> {
    let hit = d.out(css, co);
    if hit != NOSS {
        return Ok(Some(hit as usize));
    }


    let ispseudocolor = (cm.cd[co as usize].flags & PSEUDO) != 0;
    let mut ispost = false;
    let mut noprogress = true;
    let mut gotstate = false;
    {
        // Hoisted borrows: C walks css->states/d->work as raw pointers; the
        // per-state slice re-derivation was ~1/3 of miss()'s instructions.
        let base = d.ssets[css].states_base;
        let css_states = &d.statesarea[base..base + d.wordsper];
        let work = &mut d.work[..];
        for w in work.iter_mut() {
            *w = 0; // build new stateset bitmap in d.work
        }
        let arcs = &cnfa.arcs[..];
        let stflags = &cnfa.stflags[..];
        for i in 0..css_states.len() * UBITS {
            if isbset(css_states, i) {
                for ca in &arcs[cnfa.states[i].clone()] {
                    if ca.co == co || (ca.co == RAINBOW && !ispseudocolor) {
                        bset(work, ca.to as usize);
                        gotstate = true;
                        if ca.to == cnfa.post {
                            ispost = true;
                        }
                        if (stflags[ca.to as usize] & CNFA_NOPROGRESS) == 0 {
                            noprogress = false;
                        }
                    }
                }
            }
        }
    }
    if !gotstate {
        return Ok(None); // character cannot reach any new state
    }
    let mut dolacons = (cnfa.flags & HASLACONS) != 0;
    let mut sawlacons = false;
    while dolacons {
        dolacons = false;
        for i in 0..d.nstates {
            if isbset(&d.work, i) {
                let arc_range = cnfa.states[i].clone();
                for ai in arc_range {
                    let ca = cnfa.arcs[ai];
                    if (ca.co as i32) < cnfa.ncolors {
                        continue; // not a LACON arc
                    }
                    if isbset(&d.work, ca.to as usize) {
                        continue; // arc would be a no-op anyway
                    }
                    sawlacons = true; // this LACON affects our result
                    if !lacon(v, g, cnfa, cp, ca.co)? {
                        continue; // LACON arc cannot be traversed
                    }
                    bset(&mut d.work, ca.to as usize);
                    dolacons = true;
                    if ca.to == cnfa.post {
                        ispost = true;
                    }
                    if (cnfa.stflags[ca.to as usize] & CNFA_NOPROGRESS) == 0 {
                        noprogress = false;
                    }
                }
            }
        }
    }
    let h = d.hashbits(&d.work);

    let mut found: Option<usize> = None;
    {
        let wordsper = d.wordsper;
        let used = &d.ssets[..d.nssused];
        for (p, s) in used.iter().enumerate() {
            if s.hash == h
                && (wordsper == 1
                    || d.statesarea[s.states_base..s.states_base + wordsper] == d.work[..])
            {
                found = Some(p);
                break;
            }
        }
    }
    let p = match found {
        Some(p) => p,
        None => {
            let p = match getvacant(d, cp, start)? {
                Some(p) => p,
                None => return Ok(None),
            };
            debug_assert!(p != css);
            for i in 0..d.wordsper {
                let val = d.work[i];
                d.sset_states_mut(p)[i] = val;
            }
            d.ssets[p].hash = h;
            d.ssets[p].flags = if ispost { POSTSTATE } else { 0 };
            if noprogress {
                d.ssets[p].flags |= NOPROGRESS;
            }
            p
        }
    };

    if !sawlacons {
        d.outs[d.ssets[css].outs_base + co as usize] = p as u32;
        d.incarea[d.ssets[css].inchain_base + co as usize] = d.ssets[p].ins;
        d.ssets[p].ins.ss = css as u32;
        d.ssets[p].ins.co = co;
    }
    Ok(Some(p))
}


fn lacon(
    v: &mut ExecVars,
    g: &Guts,
    pcnfa: &Cnfa,
    cp: usize,
    co: color,
) -> RegResult<bool> {
    if v.depth >= v.max_depth {
        return Err(RegError(REG_ETOOBIG));
    }
    v.depth += 1;
    let result = lacon_inner(v, g, pcnfa, cp, co);
    v.depth -= 1;
    result
}

fn lacon_inner(
    v: &mut ExecVars,
    g: &Guts,
    pcnfa: &Cnfa,
    cp: usize,
    co: color,
) -> RegResult<bool> {
    let n = (co as i32 - pcnfa.ncolors) as usize;
    debug_assert!(n > 0 && (n as i32) < g.nlacons);
    let latype = g.lacons[n].latype as i32;

    let d_idx = getladfa(v, g, n)?;

    if latype_is_ahead(latype) {
        let stop = v.stop;
        let mut sub = v.ladfas[d_idx].take().expect("ladfa present");
        let cnfa = g.lacons[n]
            .cnfa
            .as_ref()
            .expect("lacon has no cnfa (NULLCNFA)");
        let end = sub.with(|d| shortest(v, g, d, cnfa, &g.cmap, cp, cp, stop, None, None));
        v.ladfas[d_idx] = Some(sub);
        let end = end?;
        let satisfied = if latype_is_pos(latype) {
            end.is_some()
        } else {
            end.is_none()
        };
        Ok(satisfied)
    } else {
        let mut sub = v.ladfas[d_idx].take().expect("ladfa present");
        let cnfa = g.lacons[n]
            .cnfa
            .as_ref()
            .expect("lacon has no cnfa (NULLCNFA)");
        let r = sub.with(|d| matchuntil(v, g, d, cnfa, &g.cmap, cp, n));
        v.ladfas[d_idx] = Some(sub);
        let mut satisfied = r?;
        if !latype_is_pos(latype) {
            satisfied = !satisfied;
        }
        Ok(satisfied)
    }
}


#[allow(clippy::too_many_arguments)]
fn longest(
    v: &mut ExecVars,
    g: &Guts,
    d: &mut Dfa<'_>,
    cnfa: &Cnfa,
    cm: &ColorMap,
    start: usize,
    stop: usize,
    mut hitstopp: Option<&mut bool>,
) -> RegResult<Option<usize>> {
    let realstop = if stop == v.stop { stop } else { stop + 1 };

    if let Some(hs) = hitstopp.as_deref_mut() {
        *hs = false;
    }

    if d.backno >= 0 {
        debug_assert!((d.backno as usize) < v.nmatch);
        if v.pmatch[d.backno as usize].0 >= 0 {
            let cp = dfa_backref(v, g, d, start, start, stop, false)?;
            if cp == Some(v.stop) && stop == v.stop {
                if let Some(hs) = hitstopp.as_deref_mut() {
                    *hs = true;
                }
            }
            return Ok(cp);
        }
    }

    if (cnfa.flags & MATCHALL) != 0 {
        let nchr = stop - start;
        let maxmatchall = cnfa.maxmatchall;
        if nchr < cnfa.minmatchall as usize {
            return Ok(None);
        }
        if maxmatchall == DUPINF {
            if stop == v.stop {
                if let Some(hs) = hitstopp.as_deref_mut() {
                    *hs = true;
                }
            }
        } else {
            if stop == v.stop && nchr <= maxmatchall as usize + 1 {
                if let Some(hs) = hitstopp.as_deref_mut() {
                    *hs = true;
                }
            }
            if nchr > maxmatchall as usize {
                return Ok(Some(start + maxmatchall as usize));
            }
        }
        return Ok(Some(stop));
    }

    let mut css = match initialize(d, cnfa, start)? {
        Some(css) => css,
        None => return Ok(None),
    };
    let mut cp = start;

    let co = if cp == v.start {
        cnfa.bos[if (v.eflags & REG_NOTBOL) != 0 { 0 } else { 1 }]
    } else {
        getcolor(cm, v.input[cp - 1])
    };
    css = match miss(v, g, d, cnfa, cm, css, co, cp, start)? {
        Some(css) => css,
        None => return Ok(None),
    };
    d.ssets[css].lastseen = Pos::at(cp);

    // Slice pinned to the loop bound so the per-char input bounds check folds
    // into the loop condition (C reads *cp bare). Fast scan holds d's arrays
    // as locals: a lastseen store through &mut d otherwise forces ptr/len
    // reloads of every other array each char (all derive from one noalias d).
    let input = v.input;
    debug_assert!(realstop <= input.len());
    let input = &input[..realstop];
    let locolormap = &cm.locolormap[..(MAX_SIMPLE_CHR - CHR_MIN + 1) as usize];
    'scan: while cp < input.len() {
        let pend_co: color;
        {
            let ssets = &mut *d.ssets;
            let outs = &*d.outs;
            loop {
                let c = input[cp];
                let co = if c <= MAX_SIMPLE_CHR {
                    locolormap[(c - CHR_MIN) as usize]
                } else {
                    crate::regex_foundation::pg_reg_getcolor(cm, c)
                };
                let hit = outs[ssets[css].outs_base + co as usize];
                if hit == NOSS {
                    pend_co = co;
                    break;
                }
                cp += 1;
                let ss = hit as usize;
                ssets[ss].lastseen = Pos::at(cp);
                css = ss;
                if cp >= input.len() {
                    break 'scan;
                }
            }
        }
        match miss(v, g, d, cnfa, cm, css, pend_co, cp + 1, start)? {
            Some(ss) => {
                cp += 1;
                d.ssets[ss].lastseen = Pos::at(cp);
                css = ss;
            }
            None => break, // NOTE BREAK OUT
        }
    }

    if cp == v.stop && stop == v.stop {
        if let Some(hs) = hitstopp {
            *hs = true;
        }
        let co = cnfa.eos[if (v.eflags & REG_NOTEOL) != 0 { 0 } else { 1 }];
        let ss = miss(v, g, d, cnfa, cm, css, co, cp, start)?;
        match ss {
            Some(ss) if (d.ssets[ss].flags & POSTSTATE) != 0 => return Ok(Some(cp)),
            Some(ss) => d.ssets[ss].lastseen = Pos::at(cp), // to be tidy
            None => {}
        }
    }

    let mut post = d.lastpost;
    for ss in 0..d.nssused {
        if (d.ssets[ss].flags & POSTSTATE) != 0 && post < d.ssets[ss].lastseen {
            post = d.ssets[ss].lastseen;
        }
    }
    if !post.is_none() {
        return Ok(Some(post.get() - 1));
    }

    Ok(None)
}


#[allow(clippy::too_many_arguments)]
fn shortest(
    v: &mut ExecVars,
    g: &Guts,
    d: &mut Dfa<'_>,
    cnfa: &Cnfa,
    cm: &ColorMap,
    start: usize,
    mut min: usize,
    max: usize,
    mut coldp: Option<&mut Option<usize>>,
    mut hitstopp: Option<&mut bool>,
) -> RegResult<Option<usize>> {
    let record_coldp = coldp.is_some();
    if let Some(cd) = coldp.as_deref_mut() {
        *cd = None;
    }
    if let Some(hs) = hitstopp.as_deref_mut() {
        *hs = false;
    }
    let realmin = if min == v.stop { min } else { min + 1 };
    let realmax = if max == v.stop { max } else { max + 1 };

    if d.backno >= 0 {
        debug_assert!((d.backno as usize) < v.nmatch);
        if v.pmatch[d.backno as usize].0 >= 0 {
            let cp = dfa_backref(v, g, d, start, min, max, true)?;
            if cp.is_some() {
                if let Some(cd) = coldp.as_deref_mut() {
                    *cd = Some(start);
                }
            }
            return Ok(cp);
        }
    }

    if (cnfa.flags & MATCHALL) != 0 {
        let nchr = min - start;
        if cnfa.maxmatchall != DUPINF && nchr > cnfa.maxmatchall as usize {
            return Ok(None);
        }
        if (max - start) < cnfa.minmatchall as usize {
            return Ok(None);
        }
        if nchr < cnfa.minmatchall as usize {
            min = start + cnfa.minmatchall as usize;
        }
        if let Some(cd) = coldp.as_deref_mut() {
            *cd = Some(start);
        }
        return Ok(Some(min));
    }

    let mut css = match initialize(d, cnfa, start)? {
        Some(css) => css,
        None => return Ok(None),
    };
    let mut cp = start;

    let co = if cp == v.start {
        cnfa.bos[if (v.eflags & REG_NOTBOL) != 0 { 0 } else { 1 }]
    } else {
        getcolor(cm, v.input[cp - 1])
    };
    css = match miss(v, g, d, cnfa, cm, css, co, cp, start)? {
        Some(css) => css,
        None => return Ok(None),
    };
    d.ssets[css].lastseen = Pos::at(cp);
    let mut ss: Option<usize> = Some(css);

    // Same bounds-check fold and fast-scan hoist as longest().
    let input = v.input;
    debug_assert!(realmax <= input.len());
    let input = &input[..realmax];
    let locolormap = &cm.locolormap[..(MAX_SIMPLE_CHR - CHR_MIN + 1) as usize];
    'scan: while cp < input.len() {
        let pend_co: color;
        {
            let ssets = &mut *d.ssets;
            let outs = &*d.outs;
            loop {
                let c = input[cp];
                let co = if c <= MAX_SIMPLE_CHR {
                    locolormap[(c - CHR_MIN) as usize]
                } else {
                    crate::regex_foundation::pg_reg_getcolor(cm, c)
                };
                let hit = outs[ssets[css].outs_base + co as usize];
                if hit == NOSS {
                    pend_co = co;
                    break;
                }
                cp += 1;
                let next = hit as usize;
                ssets[next].lastseen = Pos::at(cp);
                css = next;
                ss = Some(next);
                if (ssets[next].flags & POSTSTATE) != 0 && cp >= realmin {
                    break 'scan; // NOTE BREAK OUT
                }
                if cp >= input.len() {
                    break 'scan;
                }
            }
        }
        match miss(v, g, d, cnfa, cm, css, pend_co, cp + 1, start)? {
            Some(next) => {
                cp += 1;
                d.ssets[next].lastseen = Pos::at(cp);
                css = next;
                ss = Some(next);
                if (d.ssets[next].flags & POSTSTATE) != 0 && cp >= realmin {
                    break; // NOTE BREAK OUT
                }
            }
            None => {
                ss = None;
                break; // NOTE BREAK OUT
            }
        }
    }

    let ss = match ss {
        Some(ss) => ss,
        None => return Ok(None),
    };

    if record_coldp {
        let lc = lastcold(v, d);
        if let Some(cd) = coldp {
            *cd = Some(lc);
        }
    }

    let mut ss_opt: Option<usize> = Some(ss);

    if (d.ssets[ss].flags & POSTSTATE) != 0 && cp > min {
        debug_assert!(cp >= realmin);
        cp -= 1;
    } else if cp == v.stop && max == v.stop {
        let co = cnfa.eos[if (v.eflags & REG_NOTEOL) != 0 { 0 } else { 1 }];
        ss_opt = miss(v, g, d, cnfa, cm, css, co, cp, start)?;
        let not_post = match ss_opt {
            None => true,
            Some(s) => (d.ssets[s].flags & POSTSTATE) == 0,
        };
        if not_post {
            if let Some(hs) = hitstopp {
                *hs = true;
            }
        }
    }

    match ss_opt {
        Some(s) if (d.ssets[s].flags & POSTSTATE) != 0 => Ok(Some(cp)),
        _ => Ok(None),
    }
}


#[allow(clippy::too_many_arguments)]
fn matchuntil(
    v: &mut ExecVars,
    g: &Guts,
    d: &mut Dfa<'_>,
    cnfa: &Cnfa,
    cm: &ColorMap,
    probe: usize,
    lac: usize,
) -> RegResult<bool> {
    let mut cp = v.lblastcp[lac];
    let mut css = v.lblastcss[lac];

    if (cnfa.flags & MATCHALL) != 0 {
        let nchr = probe - v.start;
        if nchr < cnfa.minmatchall as usize {
            return Ok(false);
        }
        debug_assert!(cnfa.maxmatchall == DUPINF);
        return Ok(true);
    }

    if cp.is_none() || cp > Some(probe) {
        let start = v.start;
        cp = Some(start);
        let init = match initialize(d, cnfa, start)? {
            Some(s) => s,
            None => return Ok(false),
        };
        let co = cnfa.bos[if (v.eflags & REG_NOTBOL) != 0 { 0 } else { 1 }];
        let m = miss(v, g, d, cnfa, cm, init, co, start, start)?;
        let css_i = match m {
            Some(s) => s,
            None => {
                return Ok(false);
            }
        };
        css = Some(css_i);
        d.ssets[css_i].lastseen = Pos::at(start);
    } else if css.is_none() {
        return Ok(false);
    }
    let mut ss = css;
    let mut cp_v = cp.expect("cp set");
    let mut css_v = css.expect("css set");

    while cp_v < probe {
        let co = getcolor(cm, v.input[cp_v]);
        let hit = d.out(css_v, co);
        let next = if hit != NOSS {
            hit as usize
        } else {
            match miss(v, g, d, cnfa, cm, css_v, co, cp_v + 1, v.start)? {
                Some(s) => s,
                None => {
                    ss = None;
                    break; // NOTE BREAK OUT
                }
            }
        };
        cp_v += 1;
        d.ssets[next].lastseen = Pos::at(cp_v);
        css_v = next;
        ss = Some(next);
    }

    v.lblastcss[lac] = ss;
    v.lblastcp[lac] = Some(cp_v);

    let ss = match ss {
        Some(s) => s,
        None => return Ok(false), // impossible match, or internal error
    };
    css_v = ss;

    let ss = if cp_v < v.stop {
        let co = getcolor(cm, v.input[cp_v]);
        let hit = d.out(css_v, co);
        if hit != NOSS {
            Some(hit as usize)
        } else {
            miss(v, g, d, cnfa, cm, css_v, co, cp_v + 1, v.start)?
        }
    } else {
        debug_assert!(cp_v == v.stop);
        let co = cnfa.eos[if (v.eflags & REG_NOTEOL) != 0 { 0 } else { 1 }];
        miss(v, g, d, cnfa, cm, css_v, co, cp_v, v.start)?
    };

    match ss {
        Some(s) if (d.ssets[s].flags & POSTSTATE) != 0 => Ok(true),
        _ => Ok(false),
    }
}


fn dfa_backref(
    v: &ExecVars,
    g: &Guts,
    d: &Dfa<'_>,
    start: usize,
    min: usize,
    max: usize,
    shortest: bool,
) -> RegResult<Option<usize>> {
    let n = d.backno as usize;
    let backmin = d.backmin as i32;
    let backmax = d.backmax as i32;

    if v.pmatch[n].0 == -1 {
        return Ok(None);
    }
    let br_so = v.pmatch[n].0 as usize;
    let br_eo = v.pmatch[n].1 as usize;
    let brstring = v.start + br_so;
    let brlen = br_eo - br_so;

    if brlen == 0 {
        if min == start && backmin <= backmax {
            return Ok(Some(start));
        }
        return Ok(None);
    }

    let mut minreps: i64 = if min <= start {
        0
    } else {
        ((min - start - 1) / brlen + 1) as i64
    };
    let mut maxreps: i64 = ((max - start) / brlen) as i64;

    if minreps < backmin as i64 {
        minreps = backmin as i64;
    }
    if backmax != DUPINF && maxreps > backmax as i64 {
        maxreps = backmax as i64;
    }
    if maxreps < minreps {
        return Ok(None);
    }

    if shortest && minreps == 0 {
        return Ok(Some(start));
    }

    let compare = g.compare.expect("dfa_backref: g.compare is None");
    let mut p = start;
    let mut numreps: i64 = 0;
    while numreps < maxreps {
        if compare(&v.input[brstring..], &v.input[p..], brlen) != 0 {
            break;
        }
        p += brlen;
        numreps += 1;
        if shortest && numreps >= minreps {
            break;
        }
    }

    if numreps >= minreps {
        Ok(Some(p))
    } else {
        Ok(None)
    }
}


fn lastcold(v: &ExecVars, d: &Dfa<'_>) -> usize {
    let mut nopr = if d.lastnopr.is_none() {
        Pos::at(v.start)
    } else {
        d.lastnopr
    };
    for ss in 0..d.nssused {
        if (d.ssets[ss].flags & NOPROGRESS) != 0 && nopr < d.ssets[ss].lastseen {
            nopr = d.ssets[ss].lastseen;
        }
    }
    nopr.get()
}


#[inline]
fn off(v: &ExecVars, p: usize) -> isize {
    (p - v.start) as isize
}

fn find(
    v: &mut ExecVars,
    g: &Guts,
    cnfa: &Cnfa,
    cm: &ColorMap,
    scratch: &mut ExecScratch,
) -> RegResult<i32> {
    let troot = g.tree.expect("find: tree root present");
    let shorter = (g.tree_nodes[troot.0 as usize].flags & SHORTER) != 0;

    let search_start = v.search_start;
    let stop = v.stop;
    let mut cold: Option<usize> = None;
    let close = {
        let mut sheap = None;
        let mut s = newdfa(v.eflags, &g.search, &mut scratch.s1, &mut sheap)?;
        shortest(v,
            g,
            &mut s,
            &g.search,
            cm,
            search_start,
            search_start,
            stop,
            Some(&mut cold),
            None,
        )
    };
    let close = close?;

    let _ = REG_EXPECT;

    let close = match close {
        Some(c) => c,
        None => return Ok(REG_NOMATCH), // not found
    };
    if v.nmatch == 0 {
        return Ok(REG_OKAY);
    }

    let open = cold.expect("find: cold set when close found");
    let mut cold: Option<usize> = None;

    let mut dheap = None;
    let mut d = newdfa(v.eflags, cnfa, &mut scratch.s1, &mut dheap)?;
    let mut begin = open;
    let mut end: Option<usize> = None;
    while begin <= close {
        let mut hitend = false;
        let r = if shorter {
            shortest(v, g, &mut d, cnfa, cm, begin, begin, stop, None, Some(&mut hitend))
        } else {
            longest(v, g, &mut d, cnfa, cm, begin, stop, Some(&mut hitend))
        };
        end = r?;
        if hitend && cold.is_none() {
            cold = Some(begin);
        }
        if end.is_some() {
            break; // NOTE BREAK OUT
        }
        begin += 1;
    }
    let end = end.expect("find: search RE succeeded so loop should find an end");
    drop(d);
    let _ = cold;

    debug_assert!(v.nmatch > 0);
    v.pmatch[0].0 = off(v, begin);
    v.pmatch[0].1 = off(v, end);
    if v.nmatch == 1 {
        return Ok(REG_OKAY);
    }

    cdissect(v, g, troot, begin, end)
}

fn cfind(
    v: &mut ExecVars,
    g: &Guts,
    cnfa: &Cnfa,
    cm: &ColorMap,
    scratch: &mut ExecScratch,
) -> RegResult<i32> {
    let mut sheap = None;
    let mut dheap = None;
    let (sml1, sml2) = (&mut scratch.s1, &mut scratch.s2);
    let mut s = newdfa(v.eflags, &g.search, sml1, &mut sheap)?;
    let mut d = newdfa(v.eflags, cnfa, sml2, &mut dheap)?;

    let mut cold: Option<usize> = None;
    let ret = cfindloop(v, g, cnfa, cm, &mut d, &mut s, &mut cold);

    drop(d);
    drop(s);
    let ret = ret?;
    let _ = cold; // C surfaces it via details.rm_extend (no out-param here).
    Ok(ret)
}

#[allow(clippy::too_many_arguments)]
fn cfindloop(
    v: &mut ExecVars,
    g: &Guts,
    cnfa: &Cnfa,
    cm: &ColorMap,
    d: &mut Dfa<'_>,
    s: &mut Dfa<'_>,
    coldp: &mut Option<usize>,
) -> RegResult<i32> {
    let troot = g.tree.expect("cfindloop: tree root present");
    let shorter = (g.tree_nodes[troot.0 as usize].flags & SHORTER) != 0;

    let stop = v.stop;
    let mut cold: Option<usize> = None;
    let mut close = v.search_start;

    loop {
        let close_opt = shortest(v,
            g,
            s,
            &g.search,
            cm,
            close,
            close,
            stop,
            Some(&mut cold),
            None,
        );
        let close_res = match close_opt {
            Ok(c) => c,
            Err(e) => {
                *coldp = cold;
                return Err(e);
            }
        };
        let close_pos = match close_res {
            Some(c) => c,
            None => break, // no more possible match anywhere
        };
        close = close_pos;
        let open = cold.expect("cfindloop: cold set when close found");
        cold = None;

        let mut begin = open;
        while begin <= close {
            let mut estart = begin;
            let mut estop = v.stop;
            loop {
                let mut hitend = false;
                let end_res = if shorter {
                    shortest(v, g, d, cnfa, cm, begin, estart, estop, None, Some(&mut hitend))
                } else {
                    longest(v, g, d, cnfa, cm, begin, estop, Some(&mut hitend))
                };
                let end = match end_res {
                    Ok(e) => e,
                    Err(e) => {
                        *coldp = cold;
                        return Err(e);
                    }
                };
                if hitend && cold.is_none() {
                    cold = Some(begin);
                }
                let end = match end {
                    Some(e) => e,
                    None => break, // no match with this begin point, try next
                };
                let er = cdissect(v, g, troot, begin, end)?;
                if er == REG_OKAY {
                    if v.nmatch > 0 {
                        v.pmatch[0].0 = off(v, begin);
                        v.pmatch[0].1 = off(v, end);
                    }
                    *coldp = cold;
                    return Ok(REG_OKAY);
                }
                if er != REG_NOMATCH {
                    *coldp = cold;
                    return Ok(er);
                }
                if shorter {
                    if end == estop {
                        break; // no more, so try next begin point
                    }
                    estart = end + 1;
                } else {
                    if end == begin {
                        break; // no more, so try next begin point
                    }
                    estop = end - 1;
                }
            } // end loop over endpoint positions
            begin += 1;
        } // end loop over beginning positions

        close += 1;
        if close >= v.stop {
            break;
        }
    }

    *coldp = cold;
    Ok(REG_NOMATCH)
}


pub fn pg_regexec<'mcx>(
    _mcx: Mcx<'mcx>,
    guts: &Guts,
    data: &[chr],
    search_start: i32,
    pmatch: &mut [RegMatch],
    eflags: i32,
) -> RegResult<bool> {
    let code = pg_regexec_code(guts, data, search_start, pmatch, eflags);
    match code {
        REG_OKAY => Ok(true),
        REG_NOMATCH => Ok(false),
        other => Err(RegError::new(other)),
    }
}

fn pg_regexec_code(
    g: &Guts,
    string: &[chr],
    search_start: i32,
    pmatch: &mut [RegMatch],
    flags: i32,
) -> i32 {
    let len = string.len();
    debug_assert!(search_start >= 0);
    let search_start = search_start as usize;
    if search_start > len {
        return REG_NOMATCH;
    }

    let nmatch = pmatch.len();

    if (g.cflags & REG_EXPECT) != 0 {
        return crate::regex_consts::REG_INVARG;
    }
    if (g.info & REG_UIMPOSSIBLE as i64) != 0 {
        return REG_NOMATCH;
    }
    let backref = (g.info & REG_UBACKREF as i64) != 0;

    let v_nmatch: usize;
    let mut work: Vec<(isize, isize)>;

    if backref && nmatch <= g.nsub {
        v_nmatch = g.nsub + 1;
        work = alloc::vec![(-1isize, -1isize); v_nmatch];
        zapallsubs(&mut work, v_nmatch);
    } else {
        if nmatch > 0 {
            let mut tmp: Vec<(isize, isize)> = pmatch[..nmatch]
                .iter()
                .map(|m| (m.rm_so as isize, m.rm_eo as isize))
                .collect();
            zapallsubs(&mut tmp, nmatch);
            for i in 0..nmatch {
                pmatch[i].rm_so = tmp[i].0 as pg_regoff_t;
                pmatch[i].rm_eo = tmp[i].1 as pg_regoff_t;
            }
            work = tmp;
        } else {
            work = Vec::new();
        }
        v_nmatch = if nmatch > g.nsub + 1 {
            g.nsub + 1
        } else {
            nmatch
        };
    }

    let stop = len;
    debug_assert!(g.ntree >= 0);
    let ntree = g.ntree as usize;
    debug_assert!(g.nlacons >= 0);
    let nlacons = g.nlacons as usize;
    let ladfas: Vec<Option<SubDfa>> = (0..nlacons).map(|_| None).collect();
    let lblastcss: Vec<Option<usize>> = alloc::vec![None; nlacons];
    let lblastcp: Vec<Option<usize>> = alloc::vec![None; nlacons];

    let mut v = ExecVars {
        eflags: flags,
        nmatch: v_nmatch,
        pmatch: &mut work,
        input: string,
        start: 0,
        search_start,
        stop,
        depth: 0,
        ntree,
        subdfas: Vec::new(), // sized on first getsubdfa; dissect-only
        ladfas,
        lblastcss,
        lblastcp,
        max_depth: DEFAULT_MAX_DEPTH,
    };

    debug_assert!(g.tree.is_some());
    let troot = g.tree.expect("pg_regexec: tree root present");
    let main_cnfa = g.tree_nodes[troot.0 as usize]
        .cnfa
        .as_ref()
        .expect("pg_regexec: tree root has a cnfa");

    let mut slot = EXEC_SCRATCH.with(|c| c.borrow_mut().take());
    let scratch = slot.get_or_insert_with(|| Box::new(ExecScratch::new()));
    let st = if backref {
        cfind(&mut v, g, main_cnfa, &g.cmap, scratch)
    } else {
        find(&mut v, g, main_cnfa, &g.cmap, scratch)
    };
    EXEC_SCRATCH.with(|c| *c.borrow_mut() = slot);
    let st = match st {
        Ok(code) => code,
        Err(e) => e.0,
    };

    drop(v);

    if st == REG_OKAY && nmatch > 0 {
        let ncopy = nmatch.min(work.len());
        for (dst, src) in pmatch[..ncopy].iter_mut().zip(&work[..ncopy]) {
            dst.rm_so = src.0 as pg_regoff_t;
            dst.rm_eo = src.1 as pg_regoff_t;
        }
        if (g.cflags & REG_NOSUB) != 0 {
            let mut tmp: Vec<(isize, isize)> = pmatch[..nmatch]
                .iter()
                .map(|m| (m.rm_so as isize, m.rm_eo as isize))
                .collect();
            zapallsubs(&mut tmp, nmatch);
            for i in 0..nmatch {
                pmatch[i].rm_so = tmp[i].0 as pg_regoff_t;
                pmatch[i].rm_eo = tmp[i].1 as pg_regoff_t;
            }
        }
    }

    st
}


fn longest_sub(
    v: &mut ExecVars,
    g: &Guts,
    nid: NodeId,
    start: usize,
    stop: usize,
    hitstopp: Option<&mut bool>,
) -> RegResult<Option<usize>> {
    let arena = nid.0 as usize;
    let id = g.tree_nodes[arena].id as usize;
    let cnfa = g.tree_nodes[arena]
        .cnfa
        .as_ref()
        .expect("longest_sub: subre has no cnfa (NULLCNFA)");
    let mut sub = v.subdfas[id].take().expect("longest_sub: subdfa present");
    let r = sub.with(|d| longest(v, g, d, cnfa, &g.cmap, start, stop, hitstopp));
    v.subdfas[id] = Some(sub);
    r
}

fn shortest_sub(
    v: &mut ExecVars,
    g: &Guts,
    nid: NodeId,
    start: usize,
    min: usize,
    max: usize,
    record_coldp: bool,
) -> RegResult<Option<usize>> {
    let arena = nid.0 as usize;
    let id = g.tree_nodes[arena].id as usize;
    let cnfa = g.tree_nodes[arena]
        .cnfa
        .as_ref()
        .expect("shortest_sub: subre has no cnfa (NULLCNFA)");
    let mut sub = v.subdfas[id].take().expect("shortest_sub: subdfa present");
    let mut scratch: Option<usize> = None;
    let coldp = if record_coldp {
        Some(&mut scratch)
    } else {
        None
    };
    let r = sub.with(|d| shortest(v, g, d, cnfa, &g.cmap, start, min, max, coldp, None));
    v.subdfas[id] = Some(sub);
    r
}


pub fn zapallsubs(p: &mut [(isize, isize)], n: usize) {
    let mut i = n.wrapping_sub(1);
    while i > 0 {
        p[i].0 = -1;
        p[i].1 = -1;
        i -= 1;
    }
}

fn zaptreesubs(v: &mut ExecVars, g: &Guts, t: NodeId) {
    let id = t.0 as usize;
    let n = g.tree_nodes[id].capno;
    if n > 0 && (n as usize) < v.nmatch {
        v.pmatch[n as usize].0 = -1;
        v.pmatch[n as usize].1 = -1;
    }

    let mut t2 = g.tree_nodes[id].child;
    while let Some(c) = t2 {
        zaptreesubs(v, g, c);
        t2 = g.tree_nodes[c.0 as usize].sibling;
    }
}

fn subset(v: &mut ExecVars, g: &Guts, sub: NodeId, begin: usize, end: usize) {
    let n = g.tree_nodes[sub.0 as usize].capno;
    debug_assert!(n > 0);
    if (n as usize) >= v.nmatch {
        return;
    }
    v.pmatch[n as usize].0 = (begin - v.start) as isize;
    v.pmatch[n as usize].1 = (end - v.start) as isize;
}


fn cdissect(
    v: &mut ExecVars,
    g: &Guts,
    t: NodeId,
    begin: usize,
    end: usize,
) -> RegResult<i32> {
    if v.depth >= v.max_depth {
        return Ok(REG_ETOOBIG);
    }
    v.depth += 1;
    let r = cdissect_inner(v, g, t, begin, end);
    v.depth -= 1;
    r
}

fn cdissect_inner(
    v: &mut ExecVars,
    g: &Guts,
    t: NodeId,
    begin: usize,
    end: usize,
) -> RegResult<i32> {
    let id = t.0 as usize;
    let op = g.tree_nodes[id].op;
    let capno = g.tree_nodes[id].capno;


    let er: i32 = match op {
        b'=' => {
            debug_assert!(g.tree_nodes[id].child.is_none());
            REG_OKAY
        }
        b'b' => {
            debug_assert!(g.tree_nodes[id].child.is_none());
            cbrdissect(v, g, t, begin, end)?
        }
        b'.' => {
            let child = g.tree_nodes[id].child.expect("concat has child");
            if (g.tree_nodes[child.0 as usize].flags & SHORTER) != 0 {
                crevcondissect(v, g, t, begin, end)? // reverse scan
            } else {
                ccondissect(v, g, t, begin, end)?
            }
        }
        b'|' => {
            debug_assert!(g.tree_nodes[id].child.is_some());
            caltdissect(v, g, t, begin, end)?
        }
        b'*' => {
            let child = g.tree_nodes[id].child.expect("iter has child");
            if (g.tree_nodes[child.0 as usize].flags & SHORTER) != 0 {
                creviterdissect(v, g, t, begin, end)? // reverse scan
            } else {
                citerdissect(v, g, t, begin, end)?
            }
        }
        b'(' => {
            let child = g.tree_nodes[id].child.expect("capture has child");
            cdissect(v, g, child, begin, end)?
        }
        _ => REG_ASSERT,
    };

    debug_assert!(er != REG_NOMATCH || (g.tree_nodes[id].flags & BACKR) != 0);

    if capno > 0 && er == REG_OKAY {
        subset(v, g, t, begin, end);
    }

    Ok(er)
}


fn ccondissect(
    v: &mut ExecVars,
    g: &Guts,
    t: NodeId,
    begin: usize,
    end: usize,
) -> RegResult<i32> {
    let id = t.0 as usize;
    let left = g.tree_nodes[id].child.expect("concat left");
    let right = g.tree_nodes[left.0 as usize].sibling.expect("concat right");

    debug_assert!(g.tree_nodes[id].op == b'.');
    debug_assert!(g.tree_nodes[right.0 as usize].sibling.is_none());
    debug_assert!((g.tree_nodes[left.0 as usize].flags & SHORTER) == 0);

    getsubdfa(v, &g.tree_nodes[left.0 as usize])?;
    getsubdfa(v, &g.tree_nodes[right.0 as usize])?;

    let mut mid = match longest_sub(v, g, left, begin, end, None)? {
        Some(m) => m,
        None => return Ok(REG_NOMATCH),
    };

    loop {
        if longest_sub(v, g, right, mid, end, None)? == Some(end) {
            let mut er = cdissect(v, g, left, begin, mid)?;
            if er == REG_OKAY {
                er = cdissect(v, g, right, mid, end)?;
                if er == REG_OKAY {
                    return Ok(REG_OKAY);
                }
                zaptreesubs(v, g, left);
            }
            if er != REG_NOMATCH {
                return Ok(er);
            }
        }

        if mid == begin {
            return Ok(REG_NOMATCH);
        }
        mid = match longest_sub(v, g, left, begin, mid - 1, None)? {
            Some(m) => m,
            None => {
                return Ok(REG_NOMATCH);
            }
        };
    }
}


fn crevcondissect(
    v: &mut ExecVars,
    g: &Guts,
    t: NodeId,
    begin: usize,
    end: usize,
) -> RegResult<i32> {
    let id = t.0 as usize;
    let left = g.tree_nodes[id].child.expect("concat left");
    let right = g.tree_nodes[left.0 as usize].sibling.expect("concat right");

    debug_assert!(g.tree_nodes[id].op == b'.');
    debug_assert!(g.tree_nodes[right.0 as usize].sibling.is_none());
    debug_assert!((g.tree_nodes[left.0 as usize].flags & SHORTER) != 0);

    getsubdfa(v, &g.tree_nodes[left.0 as usize])?;
    getsubdfa(v, &g.tree_nodes[right.0 as usize])?;

    let mut mid = match shortest_sub(v, g, left, begin, begin, end, false)? {
        Some(m) => m,
        None => return Ok(REG_NOMATCH),
    };

    loop {
        if longest_sub(v, g, right, mid, end, None)? == Some(end) {
            let mut er = cdissect(v, g, left, begin, mid)?;
            if er == REG_OKAY {
                er = cdissect(v, g, right, mid, end)?;
                if er == REG_OKAY {
                    return Ok(REG_OKAY);
                }
                zaptreesubs(v, g, left);
            }
            if er != REG_NOMATCH {
                return Ok(er);
            }
        }

        if mid == end {
            return Ok(REG_NOMATCH);
        }
        mid = match shortest_sub(v, g, left, begin, mid + 1, end, false)? {
            Some(m) => m,
            None => {
                return Ok(REG_NOMATCH);
            }
        };
    }
}


fn cbrdissect(v: &mut ExecVars, g: &Guts, t: NodeId, begin: usize, end: usize) -> RegResult<i32> {
    let id = t.0 as usize;
    let n = g.tree_nodes[id].backno;
    let min = g.tree_nodes[id].min as i32;
    let max = g.tree_nodes[id].max as i32;

    debug_assert!(g.tree_nodes[id].op == b'b');
    debug_assert!(n >= 0);
    debug_assert!((n as usize) < v.nmatch);

    if v.pmatch[n as usize].0 == -1 {
        return Ok(REG_NOMATCH);
    }
    let br_so = v.pmatch[n as usize].0 as usize;
    let br_eo = v.pmatch[n as usize].1 as usize;
    let brstring = v.start + br_so;
    let brlen = br_eo - br_so;

    if brlen == 0 {
        if begin == end && min <= max {
            return Ok(REG_OKAY);
        }
        return Ok(REG_NOMATCH);
    }
    if begin == end {
        if min == 0 {
            return Ok(REG_OKAY);
        }
        return Ok(REG_NOMATCH);
    }

    debug_assert!(end > begin);
    let tlen = end - begin;
    if !tlen.is_multiple_of(brlen) {
        return Ok(REG_NOMATCH);
    }
    let mut numreps = tlen / brlen;
    if numreps < min as usize || (numreps > max as usize && max != DUPINF) {
        return Ok(REG_NOMATCH);
    }

    let compare = g.compare.expect("cbrdissect: g.compare is None");
    let mut p = begin;
    while numreps > 0 {
        numreps -= 1;
        if compare(&v.input[brstring..], &v.input[p..], brlen) != 0 {
            return Ok(REG_NOMATCH);
        }
        p += brlen;
    }

    Ok(REG_OKAY)
}


fn caltdissect(
    v: &mut ExecVars,
    g: &Guts,
    t: NodeId,
    begin: usize,
    end: usize,
) -> RegResult<i32> {
    debug_assert!(g.tree_nodes[t.0 as usize].op == b'|');

    let mut tcur = g.tree_nodes[t.0 as usize].child;
    debug_assert!(tcur.is_some() && g.tree_nodes[tcur.unwrap().0 as usize].sibling.is_some());

    while let Some(node) = tcur {
        debug_assert!(
            g.tree_nodes[node.0 as usize]
                .cnfa
                .as_ref()
                .map(|c| c.nstates > 0)
                .unwrap_or(false)
        );

        getsubdfa(v, &g.tree_nodes[node.0 as usize])?;
        if longest_sub(v, g, node, begin, end, None)? == Some(end) {
            let er = cdissect(v, g, node, begin, end)?;
            if er != REG_NOMATCH {
                return Ok(er);
            }
        }

        tcur = g.tree_nodes[node.0 as usize].sibling;
    }

    Ok(REG_NOMATCH)
}


fn citerdissect(
    v: &mut ExecVars,
    g: &Guts,
    t: NodeId,
    begin: usize,
    end: usize,
) -> RegResult<i32> {
    let id = t.0 as usize;
    let child = g.tree_nodes[id].child.expect("iter child");
    let t_min = g.tree_nodes[id].min as i32;
    let t_max = g.tree_nodes[id].max as i32;

    debug_assert!(g.tree_nodes[id].op == b'*');
    debug_assert!((g.tree_nodes[child.0 as usize].flags & SHORTER) == 0);
    debug_assert!(begin <= end);

    let mut min_matches = t_min;
    if min_matches <= 0 {
        min_matches = 1;
    }

    let mut max_matches: usize = end - begin;
    if max_matches > t_max as usize && t_max != DUPINF {
        max_matches = t_max as usize;
    }
    if max_matches < min_matches as usize {
        max_matches = min_matches as usize;
    }
    let mut endpts: Vec<usize> = fill_vec(max_matches + 1, 0usize)?;
    endpts[0] = begin;

    getsubdfa(v, &g.tree_nodes[child.0 as usize])?;

    let mut nverified: i32 = 0;
    let mut k: i32 = 1;
    let mut limit = end;

    'outer: while k > 0 {
        let ep = longest_sub(v, g, child, endpts[(k - 1) as usize], limit, None)?;
        match ep {
            None => {
                k -= 1;
            }
            Some(ep) => {
                endpts[k as usize] = ep;

                if nverified >= k {
                    nverified = k - 1;
                }

                if endpts[k as usize] != end {
                    if k >= max_matches as i32 {
                        k -= 1;
                    } else if endpts[k as usize] == endpts[(k - 1) as usize]
                        && (k >= min_matches
                            || ((min_matches - k) as i64) < (end - endpts[k as usize]) as i64)
                    {
                    } else {
                        k += 1;
                        limit = end;
                        continue 'outer;
                    }
                } else if k < min_matches {
                } else {
                    let mut i = nverified + 1;
                    while i <= k {
                        zaptreesubs(v, g, child);
                        let er = cdissect(v,
                            g,
                            child,
                            endpts[(i - 1) as usize],
                            endpts[i as usize],
                        )?;
                        if er == REG_OKAY {
                            nverified = i;
                            i += 1;
                            continue;
                        }
                        if er == REG_NOMATCH {
                            break;
                        }
                        return Ok(er);
                    }

                    if i > k {
                        return Ok(REG_OKAY);
                    }

                    k = i;
                }
            }
        }

        while k > 0 {
            let prev_end = endpts[(k - 1) as usize];
            if endpts[k as usize] > prev_end {
                limit = endpts[k as usize] - 1;
                if limit > prev_end
                    || (k < min_matches && (min_matches - k) as i64 >= (end - prev_end) as i64)
                {
                    break;
                }
            }
            k -= 1;
        }
    }


    if t_min == 0 && begin == end {
        return Ok(REG_OKAY);
    }

    Ok(REG_NOMATCH)
}


fn creviterdissect(
    v: &mut ExecVars,
    g: &Guts,
    t: NodeId,
    begin: usize,
    end: usize,
) -> RegResult<i32> {
    let id = t.0 as usize;
    let child = g.tree_nodes[id].child.expect("iter child");
    let t_min = g.tree_nodes[id].min as i32;
    let t_max = g.tree_nodes[id].max as i32;

    debug_assert!(g.tree_nodes[id].op == b'*');
    debug_assert!((g.tree_nodes[child.0 as usize].flags & SHORTER) != 0);
    debug_assert!(begin <= end);

    let mut min_matches = t_min;
    if min_matches <= 0 {
        if begin == end {
            return Ok(REG_OKAY);
        }
        min_matches = 1;
    }

    let mut max_matches: usize = end - begin;
    if max_matches > t_max as usize && t_max != DUPINF {
        max_matches = t_max as usize;
    }
    if max_matches < min_matches as usize {
        max_matches = min_matches as usize;
    }
    let mut endpts: Vec<usize> = fill_vec(max_matches + 1, 0usize)?;
    endpts[0] = begin;

    getsubdfa(v, &g.tree_nodes[child.0 as usize])?;

    let mut nverified: i32 = 0;
    let mut k: i32 = 1;
    let mut limit = begin;

    'outer: while k > 0 {
        if limit == endpts[(k - 1) as usize]
            && limit != end
            && (k >= min_matches || ((min_matches - k) as i64) < (end - limit) as i64)
        {
            limit += 1;
        }

        if k >= max_matches as i32 {
            limit = end;
        }

        let ep = shortest_sub(v, g, child, endpts[(k - 1) as usize], limit, end, false)?;
        match ep {
            None => {
                k -= 1;
            }
            Some(ep) => {
                endpts[k as usize] = ep;

                if nverified >= k {
                    nverified = k - 1;
                }

                if endpts[k as usize] != end {
                    if k >= max_matches as i32 {
                        k -= 1;
                    } else {
                        k += 1;
                        limit = endpts[(k - 1) as usize];
                        continue 'outer;
                    }
                } else if k < min_matches {
                } else {
                    let mut i = nverified + 1;
                    while i <= k {
                        zaptreesubs(v, g, child);
                        let er = cdissect(v,
                            g,
                            child,
                            endpts[(i - 1) as usize],
                            endpts[i as usize],
                        )?;
                        if er == REG_OKAY {
                            nverified = i;
                            i += 1;
                            continue;
                        }
                        if er == REG_NOMATCH {
                            break;
                        }
                        return Ok(er);
                    }

                    if i > k {
                        return Ok(REG_OKAY);
                    }

                    k = i;
                }
            }
        }

        while k > 0 {
            if endpts[k as usize] < end {
                limit = endpts[k as usize] + 1;
                break;
            }
            k -= 1;
        }
    }

    Ok(REG_NOMATCH)
}


pub struct PrefixResult {
    pub code: i32,
    pub prefix: alloc::vec::Vec<chr>,
}

pub fn pg_regprefix<'mcx>(_mcx: Mcx<'mcx>, guts: &Guts) -> RegResult<PrefixResult> {
    if (guts.info & REG_UIMPOSSIBLE as i64) != 0 {
        return Ok(PrefixResult {
            code: REG_NOMATCH,
            prefix: Vec::new(),
        });
    }

    let troot = guts.tree.expect("pg_regprefix: tree root present");
    let cnfa: &Cnfa = guts.tree_nodes[troot.0 as usize]
        .cnfa
        .as_ref()
        .expect("pg_regprefix: tree root has a cnfa");

    if (cnfa.flags & MATCHALL) != 0 {
        return Ok(PrefixResult {
            code: REG_NOMATCH,
            prefix: Vec::new(),
        });
    }

    let mut string: Vec<chr> = Vec::new();
    string.try_reserve(cnfa.nstates as usize)?;

    let res = findprefix(cnfa, &guts.cmap)?;

    debug_assert!(res.prefix.len() <= cnfa.nstates as usize);
    string = res.prefix;

    match res.code {
        x if x == REG_PREFIX || x == REG_EXACT => Ok(PrefixResult { code: x, prefix: string }),
        other => Ok(PrefixResult {
            code: other,
            prefix: Vec::new(),
        }),
    }
}

pub fn findprefix(cnfa: &Cnfa, cm: &ColorMap) -> RegResult<PrefixResult> {
    let mut string: Vec<chr> = Vec::new();

    let mut st = cnfa.pre;
    let mut nextst: i32 = -1;
    for ai in cnfa.states[st as usize].clone() {
        let ca = cnfa.arcs[ai];
        if ca.co == cnfa.bos[0] || ca.co == cnfa.bos[1] {
            if nextst == -1 {
                nextst = ca.to;
            } else if nextst != ca.to {
                return Ok(PrefixResult { code: REG_NOMATCH, prefix: string });
            }
        } else {
            return Ok(PrefixResult { code: REG_NOMATCH, prefix: string });
        }
    }
    if nextst == -1 {
        return Ok(PrefixResult { code: REG_NOMATCH, prefix: string });
    }

    loop {
        st = nextst;
        nextst = -1;
        let mut thiscolor: color = COLORLESS;
        for ai in cnfa.states[st as usize].clone() {
            let ca = cnfa.arcs[ai];
            if ca.co == cnfa.bos[0] || ca.co == cnfa.bos[1] {
                continue;
            }
            if ca.co == cnfa.eos[0]
                || ca.co == cnfa.eos[1]
                || ca.co == RAINBOW
                || (ca.co as i32) >= cnfa.ncolors
            {
                thiscolor = COLORLESS;
                break;
            }
            if thiscolor == COLORLESS {
                thiscolor = ca.co;
                nextst = ca.to;
            } else if thiscolor == ca.co {
                nextst = -1;
            } else {
                thiscolor = COLORLESS;
                break;
            }
        }
        if thiscolor == COLORLESS {
            break;
        }
        if cm.cd[thiscolor as usize].nschrs != 1 {
            break;
        }
        if cm.cd[thiscolor as usize].nuchrs != 0 {
            break;
        }

        let c = cm.cd[thiscolor as usize].firstchr;
        if getcolor(cm, c) != thiscolor {
            break;
        }

        string.push(c);

        if nextst == -1 {
            break;
        }
    }

    nextst = -1;
    for ai in cnfa.states[st as usize].clone() {
        let ca = cnfa.arcs[ai];
        if ca.co == cnfa.eos[0] || ca.co == cnfa.eos[1] {
            if nextst == -1 {
                nextst = ca.to;
            } else if nextst != ca.to {
                nextst = -1;
                break;
            }
        } else {
            nextst = -1;
            break;
        }
    }
    if nextst == cnfa.post {
        return Ok(PrefixResult { code: REG_EXACT, prefix: string });
    }

    if !string.is_empty() {
        return Ok(PrefixResult { code: REG_PREFIX, prefix: string });
    }

    Ok(PrefixResult { code: REG_NOMATCH, prefix: string })
}
