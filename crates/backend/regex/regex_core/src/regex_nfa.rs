
extern crate alloc;

use alloc::vec::Vec;

use ::mcx::Mcx;

use crate::regex_consts::{DUPINF, REG_UEMPTYMATCH, REG_UIMPOSSIBLE};
use crate::regex_error::{err_assert, err_etoobig, RegResult};
use crate::regex_foundation::{maxcolor, pseudocolor};
use crate::regguts::{
    chr, color, Arc, ArcId, Carc, Cnfa, ColorMap, Nfa, State, StateId, AHEAD, ARC_BOS, ARC_EOS,
    BEHIND, CANTMATCH, CNFA_NOPROGRESS, COLORLESS, EMPTY, HASCANTMATCH, HASLACONS, LACON, MATCHALL,
    PLAIN, RAINBOW,
};

use self::nfacolor::{colorchain, uncolorchain};

pub mod nfacolor;

pub use self::nfacolor::{colorcomplement, okcolors, rainbow};


pub const INCOMPATIBLE: i32 = 1;
pub const SATISFIED: i32 = 2;
pub const COMPATIBLE: i32 = 3;
pub const REPLACEARC: i32 = 4;


#[inline]
pub fn reg_max_compile_space() -> usize {
    500_000 * (core::mem::size_of::<State>() + 4 * core::mem::size_of::<Arc>())
}

pub const MAX_RECURSION_DEPTH: u32 = 10_000;


#[inline]
fn check_interrupt() {
}


#[inline]
fn colored(type_: i32, co: color) -> bool {
    co >= 0 && (type_ == PLAIN || type_ == AHEAD || type_ == BEHIND)
}


impl Nfa {
    #[inline]
    fn st(&self, s: StateId) -> &State {
        &self.state_arena[s.0 as usize]
    }
    #[inline]
    fn st_mut(&mut self, s: StateId) -> &mut State {
        &mut self.state_arena[s.0 as usize]
    }
    #[inline]
    fn ar(&self, a: ArcId) -> &Arc {
        &self.arc_arena[a.0 as usize]
    }
    #[inline]
    fn ar_mut(&mut self, a: ArcId) -> &mut Arc {
        &mut self.arc_arena[a.0 as usize]
    }
}


pub fn newnfa<'mcx>(mcx: Mcx<'mcx>, cm: &mut ColorMap, has_parent: bool) -> RegResult<Nfa> {
    let placeholder = StateId(0);
    let mut nfa = Nfa {
        state_arena: Vec::new(),
        arc_arena: Vec::new(),
        live_states: None,
        free_states: None,
        free_arcs: None,
        pre: placeholder,
        init: placeholder,
        final_: placeholder,
        post: placeholder,
        nstates: 0,
        slast: None,
        bos: [COLORLESS, COLORLESS],
        eos: [COLORLESS, COLORLESS],
        flags: 0,
        minmatchall: -1,
        maxmatchall: -1,
        spaceused: 0,
    };

    nfa.post = newfstate(mcx, &mut nfa, b'@')?; // number 0
    nfa.pre = newfstate(mcx, &mut nfa, b'>')?; // number 1
    nfa.init = newstate(mcx, &mut nfa)?; // may become invalid later
    nfa.final_ = newstate(mcx, &mut nfa)?;

    let pre = nfa.pre;
    let init = nfa.init;
    let final_ = nfa.final_;
    let post = nfa.post;

    rainbow(mcx, &mut nfa, cm, has_parent, PLAIN, COLORLESS, pre, init)?;
    newarc(mcx, &mut nfa, cm, has_parent, ARC_BOS, 1, pre, init)?;
    newarc(mcx, &mut nfa, cm, has_parent, ARC_BOS, 0, pre, init)?;
    rainbow(mcx, &mut nfa, cm, has_parent, PLAIN, COLORLESS, final_, post)?;
    newarc(mcx, &mut nfa, cm, has_parent, ARC_EOS, 1, final_, post)?;
    newarc(mcx, &mut nfa, cm, has_parent, ARC_EOS, 0, final_, post)?;

    Ok(nfa)
}

pub fn freenfa(mut nfa: Nfa) {
    nfa.state_arena = Vec::new();
    nfa.arc_arena = Vec::new();
    nfa.spaceused = 0;
    nfa.live_states = None;
    nfa.slast = None;
    nfa.free_states = None;
    nfa.free_arcs = None;
    nfa.nstates = -1;
}


pub fn newstate<'mcx>(_mcx: Mcx<'mcx>, nfa: &mut Nfa) -> RegResult<StateId> {
    check_interrupt();

    let s: StateId;

    if let Some(f) = nfa.free_states {
        nfa.free_states = nfa.st(f).next;
        s = f;
    } else {
        if nfa.spaceused >= reg_max_compile_space() {
            return Err(err_etoobig());
        }
        nfa.spaceused += core::mem::size_of::<State>();
        let idx = nfa.state_arena.len() as u32;
        nfa.state_arena.try_reserve(1)?;
        nfa.state_arena.push(State {
            no: 0,
            flag: 0,
            nins: 0,
            nouts: 0,
            ins: None,
            outs: None,
            tmp: None,
            next: None,
            prev: None,
        });
        s = StateId(idx);
    }

    debug_assert!(nfa.nstates >= 0);
    {
        let no = nfa.nstates;
        let st = nfa.st_mut(s);
        st.no = no;
        st.flag = 0;
        st.nins = 0;
        st.ins = None;
        st.nouts = 0;
        st.outs = None;
        st.tmp = None;
        st.next = None;
    }
    nfa.nstates += 1;

    if nfa.live_states.is_none() {
        nfa.live_states = Some(s);
    }
    if let Some(last) = nfa.slast {
        debug_assert!(nfa.st(last).next.is_none());
        nfa.st_mut(last).next = Some(s);
    }
    nfa.st_mut(s).prev = nfa.slast;
    nfa.slast = Some(s);
    Ok(s)
}

pub fn newfstate<'mcx>(mcx: Mcx<'mcx>, nfa: &mut Nfa, flag: u8) -> RegResult<StateId> {
    let s = newstate(mcx, nfa)?;
    nfa.st_mut(s).flag = flag;
    Ok(s)
}

pub fn dropstate(
    nfa: &mut Nfa,
    cm: &mut ColorMap,
    has_parent: bool,
    s: StateId,
) -> RegResult<()> {
    while let Some(a) = nfa.st(s).ins {
        freearc(nfa, cm, has_parent, a);
    }
    while let Some(a) = nfa.st(s).outs {
        freearc(nfa, cm, has_parent, a);
    }
    freestate(nfa, s);
    Ok(())
}

pub fn freestate(nfa: &mut Nfa, s: StateId) {
    debug_assert_eq!(nfa.st(s).nins, 0);
    debug_assert_eq!(nfa.st(s).nouts, 0);

    let next = nfa.st(s).next;
    let prev = nfa.st(s).prev;

    nfa.st_mut(s).no = -1; // FREESTATE
    nfa.st_mut(s).flag = 0;

    if let Some(n) = next {
        nfa.st_mut(n).prev = prev;
    } else {
        debug_assert_eq!(nfa.slast, Some(s));
        nfa.slast = prev;
    }
    if let Some(p) = prev {
        nfa.st_mut(p).next = next;
    } else {
        debug_assert_eq!(nfa.live_states, Some(s));
        nfa.live_states = next;
    }
    nfa.st_mut(s).prev = None;
    nfa.st_mut(s).next = nfa.free_states;
    nfa.free_states = Some(s);
}


fn allocarc<'mcx>(_mcx: Mcx<'mcx>, nfa: &mut Nfa) -> RegResult<ArcId> {
    if let Some(a) = nfa.free_arcs {
        nfa.free_arcs = nfa.ar(a).outchain;
        return Ok(a);
    }
    if nfa.spaceused >= reg_max_compile_space() {
        return Err(err_etoobig());
    }
    nfa.spaceused += core::mem::size_of::<Arc>();
    let idx = nfa.arc_arena.len() as u32;
    nfa.arc_arena.try_reserve(1)?;
    nfa.arc_arena.push(Arc {
        type_: 0,
        co: COLORLESS,
        from: None,
        to: None,
        outchain: None,
        outchainRev: None,
        inchain: None,
        inchainRev: None,
        colorchain: None,
        colorchainRev: None,
    });
    Ok(ArcId(idx))
}

pub fn createarc<'mcx>(
    mcx: Mcx<'mcx>,
    nfa: &mut Nfa,
    cm: &mut ColorMap,
    has_parent: bool,
    t: i32,
    co: color,
    from: StateId,
    to: StateId,
) -> RegResult<()> {
    let a = createarc_nochain(mcx, nfa, t, co, from, to)?;

    if colored(t, co) && !has_parent {
        colorchain(nfa, cm, a);
    }
    Ok(())
}

fn createarc_nochain<'mcx>(
    mcx: Mcx<'mcx>,
    nfa: &mut Nfa,
    t: i32,
    co: color,
    from: StateId,
    to: StateId,
) -> RegResult<ArcId> {
    let a = allocarc(mcx, nfa)?;

    {
        let arc = nfa.ar_mut(a);
        arc.type_ = t;
        arc.co = co;
        arc.to = Some(to);
        arc.from = Some(from);
    }

    {
        let to_ins = nfa.st(to).ins;
        let arc = nfa.ar_mut(a);
        arc.inchain = to_ins;
        arc.inchainRev = None;
    }
    if let Some(old) = nfa.st(to).ins {
        nfa.ar_mut(old).inchainRev = Some(a);
    }
    nfa.st_mut(to).ins = Some(a);

    {
        let from_outs = nfa.st(from).outs;
        let arc = nfa.ar_mut(a);
        arc.outchain = from_outs;
        arc.outchainRev = None;
    }
    if let Some(old) = nfa.st(from).outs {
        nfa.ar_mut(old).outchainRev = Some(a);
    }
    nfa.st_mut(from).outs = Some(a);

    nfa.st_mut(from).nouts += 1;
    nfa.st_mut(to).nins += 1;

    Ok(a)
}

pub fn newarc<'mcx>(
    mcx: Mcx<'mcx>,
    nfa: &mut Nfa,
    cm: &mut ColorMap,
    has_parent: bool,
    t: i32,
    co: color,
    from: StateId,
    to: StateId,
) -> RegResult<()> {
    check_interrupt();

    if nfa.st(from).nouts <= nfa.st(to).nins {
        let mut cur = nfa.st(from).outs;
        while let Some(a) = cur {
            let arc = nfa.ar(a);
            if arc.to == Some(to) && arc.co == co && arc.type_ == t {
                return Ok(());
            }
            cur = arc.outchain;
        }
    } else {
        let mut cur = nfa.st(to).ins;
        while let Some(a) = cur {
            let arc = nfa.ar(a);
            if arc.from == Some(from) && arc.co == co && arc.type_ == t {
                return Ok(());
            }
            cur = arc.inchain;
        }
    }

    createarc(mcx, nfa, cm, has_parent, t, co, from, to)
}

pub fn freearc(nfa: &mut Nfa, cm: &mut ColorMap, has_parent: bool, victim: ArcId) {
    debug_assert_ne!(nfa.ar(victim).type_, 0);

    let from = nfa.ar(victim).from.expect("freearc: arc has no from");
    let to = nfa.ar(victim).to.expect("freearc: arc has no to");

    if colored(nfa.ar(victim).type_, nfa.ar(victim).co) && !has_parent {
        uncolorchain(nfa, cm, victim);
    }

    let pred = nfa.ar(victim).outchainRev;
    let outchain = nfa.ar(victim).outchain;
    match pred {
        None => {
            debug_assert_eq!(nfa.st(from).outs, Some(victim));
            nfa.st_mut(from).outs = outchain;
        }
        Some(p) => {
            debug_assert_eq!(nfa.ar(p).outchain, Some(victim));
            nfa.ar_mut(p).outchain = outchain;
        }
    }
    if let Some(oc) = outchain {
        debug_assert_eq!(nfa.ar(oc).outchainRev, Some(victim));
        nfa.ar_mut(oc).outchainRev = pred;
    }
    nfa.st_mut(from).nouts -= 1;

    let pred = nfa.ar(victim).inchainRev;
    let inchain = nfa.ar(victim).inchain;
    match pred {
        None => {
            debug_assert_eq!(nfa.st(to).ins, Some(victim));
            nfa.st_mut(to).ins = inchain;
        }
        Some(p) => {
            debug_assert_eq!(nfa.ar(p).inchain, Some(victim));
            nfa.ar_mut(p).inchain = inchain;
        }
    }
    if let Some(ic) = inchain {
        debug_assert_eq!(nfa.ar(ic).inchainRev, Some(victim));
        nfa.ar_mut(ic).inchainRev = pred;
    }
    nfa.st_mut(to).nins -= 1;

    let free_arcs = nfa.free_arcs;
    let arc = nfa.ar_mut(victim);
    arc.type_ = 0;
    arc.from = None;
    arc.to = None;
    arc.inchain = None;
    arc.inchainRev = None;
    arc.outchainRev = None;
    arc.colorchain = None;
    arc.colorchainRev = None;
    arc.outchain = free_arcs; // freechain aliases outchain
    nfa.free_arcs = Some(victim);
}

pub fn changearcsource(nfa: &mut Nfa, a: ArcId, newfrom: StateId) {
    let oldfrom = nfa.ar(a).from.expect("changearcsource: arc has no from");
    debug_assert_ne!(oldfrom, newfrom);

    let pred = nfa.ar(a).outchainRev;
    let outchain = nfa.ar(a).outchain;
    match pred {
        None => {
            debug_assert_eq!(nfa.st(oldfrom).outs, Some(a));
            nfa.st_mut(oldfrom).outs = outchain;
        }
        Some(p) => {
            debug_assert_eq!(nfa.ar(p).outchain, Some(a));
            nfa.ar_mut(p).outchain = outchain;
        }
    }
    if let Some(oc) = outchain {
        debug_assert_eq!(nfa.ar(oc).outchainRev, Some(a));
        nfa.ar_mut(oc).outchainRev = pred;
    }
    nfa.st_mut(oldfrom).nouts -= 1;

    nfa.ar_mut(a).from = Some(newfrom);

    let newouts = nfa.st(newfrom).outs;
    {
        let arc = nfa.ar_mut(a);
        arc.outchain = newouts;
        arc.outchainRev = None;
    }
    if let Some(old) = newouts {
        nfa.ar_mut(old).outchainRev = Some(a);
    }
    nfa.st_mut(newfrom).outs = Some(a);
    nfa.st_mut(newfrom).nouts += 1;
}

pub fn changearctarget(nfa: &mut Nfa, a: ArcId, newto: StateId) {
    let oldto = nfa.ar(a).to.expect("changearctarget: arc has no to");
    debug_assert_ne!(oldto, newto);

    let pred = nfa.ar(a).inchainRev;
    let inchain = nfa.ar(a).inchain;
    match pred {
        None => {
            debug_assert_eq!(nfa.st(oldto).ins, Some(a));
            nfa.st_mut(oldto).ins = inchain;
        }
        Some(p) => {
            debug_assert_eq!(nfa.ar(p).inchain, Some(a));
            nfa.ar_mut(p).inchain = inchain;
        }
    }
    if let Some(ic) = inchain {
        debug_assert_eq!(nfa.ar(ic).inchainRev, Some(a));
        nfa.ar_mut(ic).inchainRev = pred;
    }
    nfa.st_mut(oldto).nins -= 1;

    nfa.ar_mut(a).to = Some(newto);

    let newins = nfa.st(newto).ins;
    {
        let arc = nfa.ar_mut(a);
        arc.inchain = newins;
        arc.inchainRev = None;
    }
    if let Some(old) = newins {
        nfa.ar_mut(old).inchainRev = Some(a);
    }
    nfa.st_mut(newto).ins = Some(a);
    nfa.st_mut(newto).nins += 1;
}

fn hasnonemptyout(nfa: &Nfa, s: StateId) -> bool {
    let mut cur = nfa.st(s).outs;
    while let Some(a) = cur {
        if nfa.ar(a).type_ != EMPTY {
            return true;
        }
        cur = nfa.ar(a).outchain;
    }
    false
}

fn findarc(nfa: &Nfa, s: StateId, type_: i32, co: color) -> Option<ArcId> {
    let mut cur = nfa.st(s).outs;
    while let Some(a) = cur {
        let arc = nfa.ar(a);
        if arc.type_ == type_ && arc.co == co {
            return Some(a);
        }
        cur = arc.outchain;
    }
    None
}

pub fn cparc<'mcx>(
    mcx: Mcx<'mcx>,
    nfa: &mut Nfa,
    cm: &mut ColorMap,
    has_parent: bool,
    oa: ArcId,
    from: StateId,
    to: StateId,
) -> RegResult<()> {
    let (t, co) = (nfa.ar(oa).type_, nfa.ar(oa).co);
    newarc(mcx, nfa, cm, has_parent, t, co, from, to)
}


fn sortins_key(nfa: &Nfa, a: ArcId) -> RegResult<(i32, color, i32)> {
    let aa = nfa.ar(a);
    let f = aa.from.ok_or(err_assert())?;
    Ok((nfa.st(f).no, aa.co, aa.type_))
}

fn sortins_cmp(nfa: &Nfa, a: ArcId, b: ArcId) -> RegResult<core::cmp::Ordering> {
    Ok(sortins_key(nfa, a)?.cmp(&sortins_key(nfa, b)?))
}

fn sortouts_key(nfa: &Nfa, a: ArcId) -> RegResult<(i32, color, i32)> {
    let aa = nfa.ar(a);
    let t = aa.to.ok_or(err_assert())?;
    Ok((nfa.st(t).no, aa.co, aa.type_))
}

fn sortouts_cmp(nfa: &Nfa, a: ArcId, b: ArcId) -> RegResult<core::cmp::Ordering> {
    Ok(sortouts_key(nfa, a)?.cmp(&sortouts_key(nfa, b)?))
}

fn sort_arcids_by_key(
    nfa: &Nfa,
    arr: &mut [ArcId],
    key: fn(&Nfa, ArcId) -> RegResult<(i32, color, i32)>,
) -> RegResult<()> {
    let mut keyed: Vec<((i32, color, i32), ArcId)> = Vec::new();
    keyed.try_reserve_exact(arr.len())?;
    for &a in arr.iter() {
        keyed.push((key(nfa, a)?, a));
    }
    keyed.sort_unstable_by(|x, y| x.0.cmp(&y.0));
    for (slot, &(_, a)) in arr.iter_mut().zip(keyed.iter()) {
        *slot = a;
    }
    Ok(())
}

fn collect_chain(nfa: &Nfa, head: Option<ArcId>, n: i32, in_chain: bool) -> RegResult<Vec<ArcId>> {
    let mut arr: Vec<ArcId> = Vec::new();
    arr.try_reserve(n as usize)?;
    let mut cur = head;
    while let Some(a) = cur {
        arr.push(a);
        cur = if in_chain {
            nfa.ar(a).inchain
        } else {
            nfa.ar(a).outchain
        };
    }
    debug_assert_eq!(arr.len(), n as usize);
    Ok(arr)
}

pub fn sortins<'mcx>(_mcx: Mcx<'mcx>, nfa: &mut Nfa, s: StateId) -> RegResult<()> {
    let n = nfa.st(s).nins;
    if n <= 1 {
        return Ok(()); // nothing to do
    }
    let ins = nfa.st(s).ins;
    let mut arr = collect_chain(nfa, ins, n, true)?;

    sort_arcids_by_key(nfa, &mut arr, sortins_key)?;

    let last = arr.len() - 1;
    nfa.st_mut(s).ins = Some(arr[0]);
    {
        let a = arr[0];
        nfa.ar_mut(a).inchain = Some(arr[1]);
        nfa.ar_mut(a).inchainRev = None;
    }
    for i in 1..last {
        let a = arr[i];
        nfa.ar_mut(a).inchain = Some(arr[i + 1]);
        nfa.ar_mut(a).inchainRev = Some(arr[i - 1]);
    }
    {
        let a = arr[last];
        nfa.ar_mut(a).inchain = None;
        nfa.ar_mut(a).inchainRev = Some(arr[last - 1]);
    }
    Ok(())
}

pub fn sortouts<'mcx>(_mcx: Mcx<'mcx>, nfa: &mut Nfa, s: StateId) -> RegResult<()> {
    let n = nfa.st(s).nouts;
    if n <= 1 {
        return Ok(());
    }
    let outs = nfa.st(s).outs;
    let mut arr = collect_chain(nfa, outs, n, false)?;

    sort_arcids_by_key(nfa, &mut arr, sortouts_key)?;

    let last = arr.len() - 1;
    nfa.st_mut(s).outs = Some(arr[0]);
    {
        let a = arr[0];
        nfa.ar_mut(a).outchain = Some(arr[1]);
        nfa.ar_mut(a).outchainRev = None;
    }
    for i in 1..last {
        let a = arr[i];
        nfa.ar_mut(a).outchain = Some(arr[i + 1]);
        nfa.ar_mut(a).outchainRev = Some(arr[i - 1]);
    }
    {
        let a = arr[last];
        nfa.ar_mut(a).outchain = None;
        nfa.ar_mut(a).outchainRev = Some(arr[last - 1]);
    }
    Ok(())
}


#[inline]
fn bulk_arc_op_use_sort(nsrcarcs: i32, ndestarcs: i32) -> bool {
    if nsrcarcs < 4 {
        false
    } else {
        nsrcarcs > 32 || ndestarcs > 32
    }
}

pub fn moveins<'mcx>(
    mcx: Mcx<'mcx>,
    nfa: &mut Nfa,
    cm: &mut ColorMap,
    has_parent: bool,
    old: StateId,
    new: StateId,
) -> RegResult<()> {
    debug_assert_ne!(old, new);

    if nfa.st(new).nins == 0 {
        while let Some(a) = nfa.st(old).ins {
            let (t, co, from) = (nfa.ar(a).type_, nfa.ar(a).co, nfa.ar(a).from.unwrap());
            createarc(mcx, nfa, cm, has_parent, t, co, from, new)?;
            freearc(nfa, cm, has_parent, a);
        }
    } else if !bulk_arc_op_use_sort(nfa.st(old).nins, nfa.st(new).nins) {
        while let Some(a) = nfa.st(old).ins {
            let from = nfa.ar(a).from.unwrap();
            cparc(mcx, nfa, cm, has_parent, a, from, new)?;
            freearc(nfa, cm, has_parent, a);
        }
    } else {
        check_interrupt();

        sortins(mcx, nfa, old)?;
        sortins(mcx, nfa, new)?;

        let mut oa = nfa.st(old).ins;
        let mut na = nfa.st(new).ins;
        while let (Some(o), Some(n)) = (oa, na) {
            match sortins_cmp(nfa, o, n)? {
                core::cmp::Ordering::Less => {
                    let nexto = nfa.ar(o).inchain; // SNAPSHOT next before relink
                    oa = nexto;
                    changearctarget(nfa, o, new);
                }
                core::cmp::Ordering::Equal => {
                    oa = nfa.ar(o).inchain;
                    na = nfa.ar(n).inchain;
                    freearc(nfa, cm, has_parent, o);
                }
                core::cmp::Ordering::Greater => {
                    na = nfa.ar(n).inchain;
                }
            }
        }
        while let Some(o) = oa {
            let nexto = nfa.ar(o).inchain; // SNAPSHOT next before relink
            oa = nexto;
            changearctarget(nfa, o, new);
        }
    }

    debug_assert_eq!(nfa.st(old).nins, 0);
    debug_assert!(nfa.st(old).ins.is_none());
    Ok(())
}

pub fn copyins<'mcx>(
    mcx: Mcx<'mcx>,
    nfa: &mut Nfa,
    cm: &mut ColorMap,
    has_parent: bool,
    old: StateId,
    new: StateId,
) -> RegResult<()> {
    debug_assert_ne!(old, new);
    debug_assert_eq!(nfa.st(new).nins, 0);

    let mut cur = nfa.st(old).ins;
    while let Some(a) = cur {
        let next = nfa.ar(a).inchain;
        let (t, co, from) = (nfa.ar(a).type_, nfa.ar(a).co, nfa.ar(a).from.unwrap());
        createarc(mcx, nfa, cm, has_parent, t, co, from, new)?;
        cur = next;
    }
    Ok(())
}

pub fn moveouts<'mcx>(
    mcx: Mcx<'mcx>,
    nfa: &mut Nfa,
    cm: &mut ColorMap,
    has_parent: bool,
    old: StateId,
    new: StateId,
) -> RegResult<()> {
    debug_assert_ne!(old, new);

    if nfa.st(new).nouts == 0 {
        while let Some(a) = nfa.st(old).outs {
            let (t, co, to) = (nfa.ar(a).type_, nfa.ar(a).co, nfa.ar(a).to.unwrap());
            createarc(mcx, nfa, cm, has_parent, t, co, new, to)?;
            freearc(nfa, cm, has_parent, a);
        }
    } else if !bulk_arc_op_use_sort(nfa.st(old).nouts, nfa.st(new).nouts) {
        while let Some(a) = nfa.st(old).outs {
            let to = nfa.ar(a).to.unwrap();
            cparc(mcx, nfa, cm, has_parent, a, new, to)?;
            freearc(nfa, cm, has_parent, a);
        }
    } else {
        check_interrupt();

        sortouts(mcx, nfa, old)?;
        sortouts(mcx, nfa, new)?;

        let mut oa = nfa.st(old).outs;
        let mut na = nfa.st(new).outs;
        while let (Some(o), Some(n)) = (oa, na) {
            match sortouts_cmp(nfa, o, n)? {
                core::cmp::Ordering::Less => {
                    let nexto = nfa.ar(o).outchain; // SNAPSHOT next before relink
                    oa = nexto;
                    changearcsource(nfa, o, new);
                }
                core::cmp::Ordering::Equal => {
                    oa = nfa.ar(o).outchain;
                    na = nfa.ar(n).outchain;
                    freearc(nfa, cm, has_parent, o);
                }
                core::cmp::Ordering::Greater => {
                    na = nfa.ar(n).outchain;
                }
            }
        }
        while let Some(o) = oa {
            let nexto = nfa.ar(o).outchain; // SNAPSHOT next before relink
            oa = nexto;
            changearcsource(nfa, o, new);
        }
    }

    debug_assert_eq!(nfa.st(old).nouts, 0);
    debug_assert!(nfa.st(old).outs.is_none());
    Ok(())
}

pub fn copyouts<'mcx>(
    mcx: Mcx<'mcx>,
    nfa: &mut Nfa,
    cm: &mut ColorMap,
    has_parent: bool,
    old: StateId,
    new: StateId,
) -> RegResult<()> {
    debug_assert_ne!(old, new);
    debug_assert_eq!(nfa.st(new).nouts, 0);

    let mut cur = nfa.st(old).outs;
    while let Some(a) = cur {
        let next = nfa.ar(a).outchain;
        let (t, co, to) = (nfa.ar(a).type_, nfa.ar(a).co, nfa.ar(a).to.unwrap());
        createarc(mcx, nfa, cm, has_parent, t, co, new, to)?;
        cur = next;
    }
    Ok(())
}

pub fn mergeins<'mcx>(
    mcx: Mcx<'mcx>,
    nfa: &mut Nfa,
    cm: &mut ColorMap,
    has_parent: bool,
    s: StateId,
    mut arcarray: Vec<ArcId>,
) -> RegResult<()> {
    let mut arccount = arcarray.len() as i32;
    if arccount <= 0 {
        return Ok(());
    }

    check_interrupt();

    sortins(mcx, nfa, s)?;
    sort_arcids_by_key(nfa, &mut arcarray, sortins_key)?;

    let mut j: usize = 0;
    for i in 1..arccount as usize {
        match sortins_cmp(nfa, arcarray[j], arcarray[i])? {
            core::cmp::Ordering::Less => {
                j += 1;
                arcarray[j] = arcarray[i];
            }
            core::cmp::Ordering::Equal => {
            }
            core::cmp::Ordering::Greater => {
                debug_assert!(false, "mergeins: array not sorted");
            }
        }
    }
    arccount = (j + 1) as i32;

    let mut i: usize = 0;
    let mut na = nfa.st(s).ins;
    while i < arccount as usize {
        let n = match na {
            Some(n) => n,
            None => break,
        };
        let a = arcarray[i];
        match sortins_cmp(nfa, a, n)? {
            core::cmp::Ordering::Less => {
                let (t, co, from) = (nfa.ar(a).type_, nfa.ar(a).co, nfa.ar(a).from.unwrap());
                createarc(mcx, nfa, cm, has_parent, t, co, from, s)?;
                i += 1;
            }
            core::cmp::Ordering::Equal => {
                i += 1;
                na = nfa.ar(n).inchain;
            }
            core::cmp::Ordering::Greater => {
                na = nfa.ar(n).inchain;
            }
        }
    }
    while i < arccount as usize {
        let a = arcarray[i];
        let (t, co, from) = (nfa.ar(a).type_, nfa.ar(a).co, nfa.ar(a).from.unwrap());
        createarc(mcx, nfa, cm, has_parent, t, co, from, s)?;
        i += 1;
    }
    Ok(())
}

pub fn cloneouts<'mcx>(
    mcx: Mcx<'mcx>,
    nfa: &mut Nfa,
    cm: &mut ColorMap,
    has_parent: bool,
    old: StateId,
    from: StateId,
    to: StateId,
    type_: i32,
) -> RegResult<()> {
    debug_assert_ne!(old, from);
    debug_assert!(type_ == AHEAD || type_ == BEHIND);

    let mut cur = nfa.st(old).outs;
    while let Some(a) = cur {
        debug_assert_eq!(nfa.ar(a).type_, PLAIN);
        let next = nfa.ar(a).outchain; // SNAPSHOT (newarc prepends elsewhere)
        let co = nfa.ar(a).co;
        newarc(mcx, nfa, cm, has_parent, type_, co, from, to)?;
        cur = next;
    }
    Ok(())
}


pub fn delsub(
    nfa: &mut Nfa,
    cm: &mut ColorMap,
    has_parent: bool,
    lp: StateId,
    rp: StateId,
) -> RegResult<()> {
    debug_assert_ne!(lp, rp);

    nfa.st_mut(rp).tmp = Some(rp); // mark end

    deltraverse(nfa, cm, has_parent, lp, lp, 0)?;
    debug_assert_eq!(nfa.st(lp).nouts, 0);
    debug_assert_eq!(nfa.st(rp).nins, 0);

    nfa.st_mut(rp).tmp = None; // unmark end
    nfa.st_mut(lp).tmp = None; // and begin, marked by deltraverse
    Ok(())
}

fn deltraverse(
    nfa: &mut Nfa,
    cm: &mut ColorMap,
    has_parent: bool,
    leftend: StateId,
    s: StateId,
    depth: u32,
) -> RegResult<()> {
    if depth >= MAX_RECURSION_DEPTH {
        return Err(err_etoobig());
    }

    if nfa.st(s).nouts == 0 {
        return Ok(()); // nothing to do
    }
    if nfa.st(s).tmp.is_some() {
        return Ok(()); // already in progress
    }

    nfa.st_mut(s).tmp = Some(s); // mark as in progress

    while let Some(a) = nfa.st(s).outs {
        let to = nfa.ar(a).to.ok_or(err_assert())?;
        deltraverse(nfa, cm, has_parent, leftend, to, depth + 1)?;
        debug_assert!(nfa.st(to).nouts == 0 || nfa.st(to).tmp.is_some());
        freearc(nfa, cm, has_parent, a);
        if nfa.st(to).nins == 0 && nfa.st(to).tmp.is_none() {
            debug_assert_eq!(nfa.st(to).nouts, 0);
            freestate(nfa, to);
        }
    }

    debug_assert!(s == leftend || nfa.st(s).nins != 0);
    debug_assert_eq!(nfa.st(s).nouts, 0);

    nfa.st_mut(s).tmp = None; // we're done here
    Ok(())
}

pub fn dupnfa<'mcx>(
    mcx: Mcx<'mcx>,
    nfa: &mut Nfa,
    cm: &mut ColorMap,
    has_parent: bool,
    start: StateId,
    stop: StateId,
    from: StateId,
    to: StateId,
) -> RegResult<()> {
    if start == stop {
        newarc(mcx, nfa, cm, has_parent, EMPTY, 0, from, to)?;
        return Ok(());
    }

    nfa.st_mut(stop).tmp = Some(to);
    let res = duptraverse(mcx, nfa, cm, has_parent, start, Some(from), 0);
    nfa.st_mut(stop).tmp = None;
    let clear_res = cleartraverse(nfa, start, 0);
    res?;
    clear_res?;
    Ok(())
}

fn duptraverse<'mcx>(
    mcx: Mcx<'mcx>,
    nfa: &mut Nfa,
    cm: &mut ColorMap,
    has_parent: bool,
    s: StateId,
    stmp: Option<StateId>,
    depth: u32,
) -> RegResult<()> {
    if depth >= MAX_RECURSION_DEPTH {
        return Err(err_etoobig());
    }

    if nfa.st(s).tmp.is_some() {
        return Ok(()); // already done
    }

    let dup = match stmp {
        Some(t) => t,
        None => newstate(mcx, nfa)?,
    };
    nfa.st_mut(s).tmp = Some(dup);

    let mut cur = nfa.st(s).outs;
    while let Some(a) = cur {
        let to = nfa.ar(a).to.ok_or(err_assert())?;
        duptraverse(mcx, nfa, cm, has_parent, to, None, depth + 1)?;
        let todup = nfa.st(to).tmp.expect("duptraverse: dup not set");
        let sdup = nfa.st(s).tmp.expect("duptraverse: s dup not set");
        cparc(mcx, nfa, cm, has_parent, a, sdup, todup)?;
        cur = nfa.ar(a).outchain;
    }
    Ok(())
}

pub fn removeconstraints<'mcx>(
    mcx: Mcx<'mcx>,
    nfa: &mut Nfa,
    cm: &mut ColorMap,
    has_parent: bool,
    start: StateId,
    stop: StateId,
) -> RegResult<()> {
    if start == stop {
        return Ok(());
    }

    nfa.st_mut(stop).tmp = Some(stop);
    let res = removetraverse(mcx, nfa, cm, has_parent, start, 0);
    nfa.st_mut(stop).tmp = None;
    let clear_res = cleartraverse(nfa, start, 0);
    res?;
    clear_res?;
    Ok(())
}

fn removetraverse<'mcx>(
    mcx: Mcx<'mcx>,
    nfa: &mut Nfa,
    cm: &mut ColorMap,
    has_parent: bool,
    s: StateId,
    depth: u32,
) -> RegResult<()> {
    if depth >= MAX_RECURSION_DEPTH {
        return Err(err_etoobig());
    }

    if nfa.st(s).tmp.is_some() {
        return Ok(()); // already done
    }

    nfa.st_mut(s).tmp = Some(s);
    let mut cur = nfa.st(s).outs;
    while let Some(a) = cur {
        let to = nfa.ar(a).to.ok_or(err_assert())?;
        removetraverse(mcx, nfa, cm, has_parent, to, depth + 1)?;
        let oa = nfa.ar(a).outchain; // SNAPSHOT next before possible relink
        let t = nfa.ar(a).type_;
        if t == PLAIN || t == EMPTY || t == CANTMATCH {
        } else if t == AHEAD || t == BEHIND || t == ARC_BOS || t == ARC_EOS || t == LACON {
            newarc(mcx, nfa, cm, has_parent, EMPTY, 0, s, to)?;
            freearc(nfa, cm, has_parent, a);
        } else {
            return Err(err_assert());
        }
        cur = oa;
    }
    Ok(())
}

fn cleartraverse(nfa: &mut Nfa, s: StateId, depth: u32) -> RegResult<()> {
    if depth >= MAX_RECURSION_DEPTH {
        return Err(err_etoobig());
    }

    if nfa.st(s).tmp.is_none() {
        return Ok(());
    }
    nfa.st_mut(s).tmp = None;

    let mut cur = nfa.st(s).outs;
    while let Some(a) = cur {
        let to = nfa.ar(a).to.ok_or(err_assert())?;
        cleartraverse(nfa, to, depth + 1)?;
        cur = nfa.ar(a).outchain;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn dupnfa_cross<'mcx>(
    mcx: Mcx<'mcx>,
    dst: &mut Nfa,
    src: &mut Nfa,
    cm: &mut ColorMap,
    start: StateId,
    stop: StateId,
    from: StateId,
    to: StateId,
) -> RegResult<()> {
    if start == stop {
        newarc(mcx, dst, cm, true, EMPTY, 0, from, to)?;
        return Ok(());
    }

    src.st_mut(stop).tmp = Some(to);
    let res = duptraverse_cross(mcx, dst, src, cm, start, Some(from), 0);
    src.st_mut(stop).tmp = None;
    let clear_res = cleartraverse(src, start, 0);
    res?;
    clear_res?;
    Ok(())
}

fn duptraverse_cross<'mcx>(
    mcx: Mcx<'mcx>,
    dst: &mut Nfa,
    src: &mut Nfa,
    cm: &mut ColorMap,
    s: StateId,
    stmp: Option<StateId>,
    depth: u32,
) -> RegResult<()> {
    if depth >= MAX_RECURSION_DEPTH {
        return Err(err_etoobig());
    }

    if src.st(s).tmp.is_some() {
        return Ok(()); // already done
    }

    let dup = match stmp {
        Some(t) => t,
        None => newstate(mcx, dst)?,
    };
    src.st_mut(s).tmp = Some(dup);

    let mut cur = src.st(s).outs;
    while let Some(a) = cur {
        let to = src.ar(a).to.ok_or(err_assert())?;
        duptraverse_cross(mcx, dst, src, cm, to, None, depth + 1)?;
        let todup = src.st(to).tmp.expect("duptraverse_cross: dup not set");
        let sdup = src.st(s).tmp.expect("duptraverse_cross: s dup not set");
        let (t, co) = (src.ar(a).type_, src.ar(a).co);
        newarc(mcx, dst, cm, true, t, co, sdup, todup)?;
        cur = src.ar(a).outchain;
    }
    Ok(())
}


fn markreachable(
    nfa: &mut Nfa,
    s: StateId,
    okay: Option<StateId>,
    mark: Option<StateId>,
    depth: u32,
) -> RegResult<()> {
    if depth >= MAX_RECURSION_DEPTH {
        return Err(err_etoobig());
    }

    if nfa.st(s).tmp != okay {
        return Ok(());
    }
    nfa.st_mut(s).tmp = mark;

    let mut cur = nfa.st(s).outs;
    while let Some(a) = cur {
        let to = nfa.ar(a).to.ok_or(err_assert())?;
        markreachable(nfa, to, okay, mark, depth + 1)?;
        cur = nfa.ar(a).outchain;
    }
    Ok(())
}

fn markcanreach(
    nfa: &mut Nfa,
    s: StateId,
    okay: Option<StateId>,
    mark: Option<StateId>,
    depth: u32,
) -> RegResult<()> {
    if depth >= MAX_RECURSION_DEPTH {
        return Err(err_etoobig());
    }

    if nfa.st(s).tmp != okay {
        return Ok(());
    }
    nfa.st_mut(s).tmp = mark;

    let mut cur = nfa.st(s).ins;
    while let Some(a) = cur {
        let from = nfa.ar(a).from.ok_or(err_assert())?;
        markcanreach(nfa, from, okay, mark, depth + 1)?;
        cur = nfa.ar(a).inchain;
    }
    Ok(())
}

pub fn cleanup(nfa: &mut Nfa, cm: &mut ColorMap, has_parent: bool) -> RegResult<()> {
    let pre = nfa.pre;
    let post = nfa.post;

    markreachable(nfa, pre, None, Some(pre), 0)?;
    markcanreach(nfa, post, Some(pre), Some(post), 0)?;

    let mut s_opt = nfa.live_states;
    while let Some(s) = s_opt {
        let nexts = nfa.st(s).next; // SNAPSHOT next before possible dropstate
        if nfa.st(s).tmp != Some(post) && nfa.st(s).flag == 0 {
            dropstate(nfa, cm, has_parent, s)?;
        }
        s_opt = nexts;
    }
    debug_assert!(nfa.st(post).nins == 0 || nfa.st(post).tmp == Some(post));
    cleartraverse(nfa, pre, 0)?;
    debug_assert!(nfa.st(post).nins == 0 || nfa.st(post).tmp.is_none());

    let mut n = 0;
    let mut s_opt = nfa.live_states;
    while let Some(s) = s_opt {
        nfa.st_mut(s).no = n;
        n += 1;
        s_opt = nfa.st(s).next;
    }
    nfa.nstates = n;
    Ok(())
}


pub fn single_color_transition(nfa: &Nfa, s1: StateId, s2: StateId) -> Option<StateId> {
    let mut s1 = s1;
    let mut s2 = s2;

    if nfa.st(s1).nouts == 1 {
        let a = nfa.st(s1).outs.unwrap();
        if nfa.ar(a).type_ == EMPTY {
            s1 = nfa.ar(a).to.unwrap();
        }
    }
    if nfa.st(s2).nins == 1 {
        let a = nfa.st(s2).ins.unwrap();
        if nfa.ar(a).type_ == EMPTY {
            s2 = nfa.ar(a).from.unwrap();
        }
    }
    if s1 == s2 {
        return None;
    }
    nfa.st(s1).outs?;
    let mut cur = nfa.st(s1).outs;
    while let Some(a) = cur {
        if nfa.ar(a).type_ != PLAIN || nfa.ar(a).to != Some(s2) {
            return None;
        }
        cur = nfa.ar(a).outchain;
    }
    Some(s1)
}

pub fn specialcolors<'mcx>(
    mcx: Mcx<'mcx>,
    nfa: &mut Nfa,
    cm: &mut ColorMap,
    parent: Option<([color; 2], [color; 2])>,
) -> RegResult<()> {
    match parent {
        None => {
            nfa.bos[0] = pseudocolor(mcx, cm)?;
            nfa.bos[1] = pseudocolor(mcx, cm)?;
            nfa.eos[0] = pseudocolor(mcx, cm)?;
            nfa.eos[1] = pseudocolor(mcx, cm)?;
        }
        Some((pbos, peos)) => {
            debug_assert_ne!(pbos[0], COLORLESS);
            nfa.bos[0] = pbos[0];
            debug_assert_ne!(pbos[1], COLORLESS);
            nfa.bos[1] = pbos[1];
            debug_assert_ne!(peos[0], COLORLESS);
            nfa.eos[0] = peos[0];
            debug_assert_ne!(peos[1], COLORLESS);
            nfa.eos[1] = peos[1];
        }
    }
    Ok(())
}

pub fn removecantmatch(nfa: &mut Nfa, cm: &mut ColorMap, has_parent: bool) -> RegResult<()> {
    let mut s_opt = nfa.live_states;
    while let Some(s) = s_opt {
        let mut cur = nfa.st(s).outs;
        while let Some(a) = cur {
            let nexta = nfa.ar(a).outchain; // SNAPSHOT next before possible free
            if nfa.ar(a).type_ == CANTMATCH {
                freearc(nfa, cm, has_parent, a);
            }
            cur = nexta;
        }
        s_opt = nfa.st(s).next;
    }
    Ok(())
}

pub fn combine(nfa: &Nfa, cm: &ColorMap, con: ArcId, a: ArcId) -> i32 {
    #[inline]
    fn ca(ct: i32, at: i32) -> i32 {
        (ct << 8) | at
    }

    let con_type = nfa.ar(con).type_;
    let con_co = nfa.ar(con).co;
    let a_type = nfa.ar(a).type_;
    let a_co = nfa.ar(a).co;

    let key = ca(con_type, a_type);

    if key == ca(ARC_BOS, PLAIN) || key == ca(ARC_EOS, PLAIN) {
        return INCOMPATIBLE;
    }
    if key == ca(AHEAD, PLAIN) || key == ca(BEHIND, PLAIN) {
        if con_co == a_co {
            return SATISFIED;
        }
        if con_co == RAINBOW {
            if (cm.cd[a_co as usize].flags & crate::regguts::PSEUDO) == 0 {
                return SATISFIED;
            }
        } else if a_co == RAINBOW {
            if (cm.cd[con_co as usize].flags & crate::regguts::PSEUDO) != 0 {
                return INCOMPATIBLE;
            }
            return REPLACEARC;
        }
        return INCOMPATIBLE;
    }
    if key == ca(ARC_BOS, ARC_BOS) || key == ca(ARC_EOS, ARC_EOS) {
        if con_co == a_co {
            return SATISFIED; // true duplication
        }
        return INCOMPATIBLE;
    }
    if key == ca(AHEAD, AHEAD) || key == ca(BEHIND, BEHIND) {
        if con_co == a_co {
            return SATISFIED; // true duplication
        }
        if con_co == RAINBOW {
            if (cm.cd[a_co as usize].flags & crate::regguts::PSEUDO) == 0 {
                return SATISFIED;
            }
        } else if a_co == RAINBOW {
            if (cm.cd[con_co as usize].flags & crate::regguts::PSEUDO) != 0 {
                return INCOMPATIBLE;
            }
            return REPLACEARC;
        }
        return INCOMPATIBLE;
    }
    if key == ca(ARC_BOS, BEHIND)
        || key == ca(BEHIND, ARC_BOS)
        || key == ca(ARC_EOS, AHEAD)
        || key == ca(AHEAD, ARC_EOS)
    {
        return INCOMPATIBLE;
    }
    if key == ca(ARC_BOS, ARC_EOS)
        || key == ca(ARC_BOS, AHEAD)
        || key == ca(BEHIND, ARC_EOS)
        || key == ca(BEHIND, AHEAD)
        || key == ca(ARC_EOS, ARC_BOS)
        || key == ca(ARC_EOS, BEHIND)
        || key == ca(AHEAD, ARC_BOS)
        || key == ca(AHEAD, BEHIND)
        || key == ca(ARC_BOS, LACON)
        || key == ca(BEHIND, LACON)
        || key == ca(ARC_EOS, LACON)
        || key == ca(AHEAD, LACON)
    {
        return COMPATIBLE;
    }

    debug_assert!(false, "combine: NOTREACHED");
    INCOMPATIBLE // for benefit of blind compilers
}


pub fn pullback<'mcx>(
    mcx: Mcx<'mcx>,
    nfa: &mut Nfa,
    cm: &mut ColorMap,
    has_parent: bool,
) -> RegResult<()> {
    loop {
        let mut progress = false;
        let mut s_opt = nfa.live_states;
        while let Some(s) = s_opt {
            let nexts = nfa.st(s).next; // SNAPSHOT next before possible dropstate
            let mut intermediates: Option<StateId> = None;
            let mut a_opt = nfa.st(s).outs;
            while let Some(a) = a_opt {
                let nexta = nfa.ar(a).outchain; // SNAPSHOT next before relink
                let t = nfa.ar(a).type_;
                if (t == ARC_BOS || t == BEHIND)
                    && pull(mcx, nfa, cm, has_parent, a, &mut intermediates)?
                {
                    progress = true;
                }
                a_opt = nexta;
            }
            while let Some(im) = intermediates {
                let ns = nfa.st(im).tmp;
                nfa.st_mut(im).tmp = None;
                intermediates = ns;
            }
            if (nfa.st(s).nins == 0 || nfa.st(s).nouts == 0) && nfa.st(s).flag == 0 {
                dropstate(nfa, cm, has_parent, s)?;
            }
            s_opt = nexts;
        }
        if !progress {
            break;
        }
    }

    let pre = nfa.pre;
    let mut a_opt = nfa.st(pre).outs;
    while let Some(a) = a_opt {
        let nexta = nfa.ar(a).outchain; // SNAPSHOT next before possible free
        if nfa.ar(a).type_ == ARC_BOS {
            let co = nfa.ar(a).co;
            debug_assert!(co == 0 || co == 1);
            let from = nfa.ar(a).from.unwrap();
            let to = nfa.ar(a).to.unwrap();
            let bos = nfa.bos[co as usize];
            newarc(mcx, nfa, cm, has_parent, PLAIN, bos, from, to)?;
            freearc(nfa, cm, has_parent, a);
        }
        a_opt = nexta;
    }
    Ok(())
}

fn pull<'mcx>(
    mcx: Mcx<'mcx>,
    nfa: &mut Nfa,
    cm: &mut ColorMap,
    has_parent: bool,
    con: ArcId,
    intermediates: &mut Option<StateId>,
) -> RegResult<bool> {
    let mut con = con;
    let mut from = nfa.ar(con).from.ok_or(err_assert())?;
    let to = nfa.ar(con).to.ok_or(err_assert())?;

    debug_assert_ne!(from, to); // should have gotten rid of this earlier
    if nfa.st(from).flag != 0 {
        return Ok(false);
    }
    if nfa.st(from).nins == 0 {
        freearc(nfa, cm, has_parent, con);
        return Ok(true);
    }

    if nfa.st(from).nouts > 1 {
        let s = newstate(mcx, nfa)?;
        copyins(mcx, nfa, cm, has_parent, from, s)?; // duplicate inarcs
        cparc(mcx, nfa, cm, has_parent, con, s, to)?; // move constraint arc
        freearc(nfa, cm, has_parent, con);
        from = s;
        con = nfa.st(from).outs.ok_or(err_assert())?;
    }
    debug_assert_eq!(nfa.st(from).nouts, 1);

    let mut a_opt = nfa.st(from).ins;
    while let Some(a) = a_opt {
        let nexta = nfa.ar(a).inchain; // SNAPSHOT next before relink
        match combine(nfa, cm, con, a) {
            INCOMPATIBLE => {
                freearc(nfa, cm, has_parent, a);
            }
            SATISFIED => {
            }
            COMPATIBLE => {
                let afrom = nfa.ar(a).from.ok_or(err_assert())?;
                let mut s_opt = *intermediates;
                let mut found: Option<StateId> = None;
                while let Some(s) = s_opt {
                    debug_assert!(nfa.st(s).nins > 0 && nfa.st(s).nouts > 0);
                    let s_ins = nfa.st(s).ins.ok_or(err_assert())?;
                    let s_in_from = nfa.ar(s_ins).from.ok_or(err_assert())?;
                    let s_outs = nfa.st(s).outs.ok_or(err_assert())?;
                    let s_out_to = nfa.ar(s_outs).to.ok_or(err_assert())?;
                    if s_in_from == afrom && s_out_to == to {
                        found = Some(s);
                        break;
                    }
                    s_opt = nfa.st(s).tmp;
                }
                let s = match found {
                    Some(s) => s,
                    None => {
                        let s = newstate(mcx, nfa)?;
                        nfa.st_mut(s).tmp = *intermediates;
                        *intermediates = Some(s);
                        s
                    }
                };
                cparc(mcx, nfa, cm, has_parent, con, afrom, s)?;
                cparc(mcx, nfa, cm, has_parent, a, s, to)?;
                freearc(nfa, cm, has_parent, a);
            }
            REPLACEARC => {
                let at = nfa.ar(a).type_;
                let conco = nfa.ar(con).co;
                let afrom = nfa.ar(a).from.ok_or(err_assert())?;
                newarc(mcx, nfa, cm, has_parent, at, conco, afrom, to)?;
                freearc(nfa, cm, has_parent, a);
            }
            _ => {
                debug_assert!(false, "pull: combine returned NOTREACHED value");
            }
        }
        a_opt = nexta;
    }

    moveins(mcx, nfa, cm, has_parent, from, to)?;
    freearc(nfa, cm, has_parent, con);
    Ok(true)
}

pub fn pushfwd<'mcx>(
    mcx: Mcx<'mcx>,
    nfa: &mut Nfa,
    cm: &mut ColorMap,
    has_parent: bool,
) -> RegResult<()> {
    loop {
        let mut progress = false;
        let mut s_opt = nfa.live_states;
        while let Some(s) = s_opt {
            let nexts = nfa.st(s).next; // SNAPSHOT next before possible dropstate
            let mut intermediates: Option<StateId> = None;
            let mut a_opt = nfa.st(s).ins;
            while let Some(a) = a_opt {
                let nexta = nfa.ar(a).inchain; // SNAPSHOT next before relink
                let t = nfa.ar(a).type_;
                if (t == ARC_EOS || t == AHEAD)
                    && push(mcx, nfa, cm, has_parent, a, &mut intermediates)?
                {
                    progress = true;
                }
                a_opt = nexta;
            }
            while let Some(im) = intermediates {
                let ns = nfa.st(im).tmp;
                nfa.st_mut(im).tmp = None;
                intermediates = ns;
            }
            if (nfa.st(s).nins == 0 || nfa.st(s).nouts == 0) && nfa.st(s).flag == 0 {
                dropstate(nfa, cm, has_parent, s)?;
            }
            s_opt = nexts;
        }
        if !progress {
            break;
        }
    }

    let post = nfa.post;
    let mut a_opt = nfa.st(post).ins;
    while let Some(a) = a_opt {
        let nexta = nfa.ar(a).inchain; // SNAPSHOT next before possible free
        if nfa.ar(a).type_ == ARC_EOS {
            let co = nfa.ar(a).co;
            debug_assert!(co == 0 || co == 1);
            let from = nfa.ar(a).from.unwrap();
            let to = nfa.ar(a).to.unwrap();
            let eos = nfa.eos[co as usize];
            newarc(mcx, nfa, cm, has_parent, PLAIN, eos, from, to)?;
            freearc(nfa, cm, has_parent, a);
        }
        a_opt = nexta;
    }
    Ok(())
}

fn push<'mcx>(
    mcx: Mcx<'mcx>,
    nfa: &mut Nfa,
    cm: &mut ColorMap,
    has_parent: bool,
    con: ArcId,
    intermediates: &mut Option<StateId>,
) -> RegResult<bool> {
    let mut con = con;
    let from = nfa.ar(con).from.ok_or(err_assert())?;
    let mut to = nfa.ar(con).to.ok_or(err_assert())?;

    debug_assert_ne!(to, from); // should have gotten rid of this earlier
    if nfa.st(to).flag != 0 {
        return Ok(false);
    }
    if nfa.st(to).nouts == 0 {
        freearc(nfa, cm, has_parent, con);
        return Ok(true);
    }

    if nfa.st(to).nins > 1 {
        let s = newstate(mcx, nfa)?;
        copyouts(mcx, nfa, cm, has_parent, to, s)?; // duplicate outarcs
        cparc(mcx, nfa, cm, has_parent, con, from, s)?; // move constraint arc
        freearc(nfa, cm, has_parent, con);
        to = s;
        con = nfa.st(to).ins.ok_or(err_assert())?;
    }
    debug_assert_eq!(nfa.st(to).nins, 1);

    let mut a_opt = nfa.st(to).outs;
    while let Some(a) = a_opt {
        let nexta = nfa.ar(a).outchain; // SNAPSHOT next before relink
        match combine(nfa, cm, con, a) {
            INCOMPATIBLE => {
                freearc(nfa, cm, has_parent, a);
            }
            SATISFIED => {
            }
            COMPATIBLE => {
                let ato = nfa.ar(a).to.ok_or(err_assert())?;
                let mut s_opt = *intermediates;
                let mut found: Option<StateId> = None;
                while let Some(s) = s_opt {
                    debug_assert!(nfa.st(s).nins > 0 && nfa.st(s).nouts > 0);
                    let s_ins = nfa.st(s).ins.ok_or(err_assert())?;
                    let s_in_from = nfa.ar(s_ins).from.ok_or(err_assert())?;
                    let s_outs = nfa.st(s).outs.ok_or(err_assert())?;
                    let s_out_to = nfa.ar(s_outs).to.ok_or(err_assert())?;
                    if s_in_from == from && s_out_to == ato {
                        found = Some(s);
                        break;
                    }
                    s_opt = nfa.st(s).tmp;
                }
                let s = match found {
                    Some(s) => s,
                    None => {
                        let s = newstate(mcx, nfa)?;
                        nfa.st_mut(s).tmp = *intermediates;
                        *intermediates = Some(s);
                        s
                    }
                };
                cparc(mcx, nfa, cm, has_parent, con, s, ato)?;
                cparc(mcx, nfa, cm, has_parent, a, from, s)?;
                freearc(nfa, cm, has_parent, a);
            }
            REPLACEARC => {
                let at = nfa.ar(a).type_;
                let conco = nfa.ar(con).co;
                let ato = nfa.ar(a).to.ok_or(err_assert())?;
                newarc(mcx, nfa, cm, has_parent, at, conco, from, ato)?;
                freearc(nfa, cm, has_parent, a);
            }
            _ => {
                debug_assert!(false, "push: combine returned NOTREACHED value");
            }
        }
        a_opt = nexta;
    }

    moveouts(mcx, nfa, cm, has_parent, to, from)?;
    freearc(nfa, cm, has_parent, con);
    Ok(true)
}

pub fn fixempties<'mcx>(
    mcx: Mcx<'mcx>,
    nfa: &mut Nfa,
    cm: &mut ColorMap,
    has_parent: bool,
) -> RegResult<()> {
    let mut s_opt = nfa.live_states;
    while let Some(s) = s_opt {
        let nexts = nfa.st(s).next; // SNAPSHOT next before possible dropstate
        if nfa.st(s).flag != 0 || nfa.st(s).nouts != 1 {
            s_opt = nexts;
            continue;
        }
        let a = nfa.st(s).outs.unwrap();
        debug_assert!(nfa.ar(a).outchain.is_none());
        if nfa.ar(a).type_ != EMPTY {
            s_opt = nexts;
            continue;
        }
        let ato = nfa.ar(a).to.unwrap();
        if s != ato {
            moveins(mcx, nfa, cm, has_parent, s, ato)?;
        }
        dropstate(nfa, cm, has_parent, s)?;
        s_opt = nexts;
    }

    let mut s_opt = nfa.live_states;
    while let Some(s) = s_opt {
        let nexts = nfa.st(s).next; // SNAPSHOT next before possible dropstate
        debug_assert!(nfa.st(s).tmp.is_none());
        if nfa.st(s).flag != 0 || nfa.st(s).nins != 1 {
            s_opt = nexts;
            continue;
        }
        let a = nfa.st(s).ins.unwrap();
        debug_assert!(nfa.ar(a).inchain.is_none());
        if nfa.ar(a).type_ != EMPTY {
            s_opt = nexts;
            continue;
        }
        let afrom = nfa.ar(a).from.unwrap();
        if s != afrom {
            moveouts(mcx, nfa, cm, has_parent, s, afrom)?;
        }
        dropstate(nfa, cm, has_parent, s)?;
        s_opt = nexts;
    }

    let nstates = nfa.nstates as usize;
    let mut inarcsorig: Vec<Option<ArcId>> = Vec::new();
    inarcsorig.try_reserve_exact(nstates)?;
    inarcsorig.resize(nstates, None);
    let mut totalinarcs: usize = 0;
    let mut s_opt = nfa.live_states;
    while let Some(s) = s_opt {
        let no = nfa.st(s).no as usize;
        inarcsorig[no] = nfa.st(s).ins;
        totalinarcs += nfa.st(s).nins as usize;
        s_opt = nfa.st(s).next;
    }

    let mut s_opt = nfa.live_states;
    while let Some(s) = s_opt {
        if nfa.st(s).flag == 0 && !hasnonemptyout(nfa, s) {
            s_opt = nfa.st(s).next;
            continue;
        }

        let mut arcarray: Vec<ArcId> = Vec::new();
        arcarray.try_reserve(totalinarcs)?;
        let mut s2_opt = Some(emptyreachable(nfa, s, s, &inarcsorig, 0)?);
        while let Some(s2) = s2_opt {
            if s2 == s {
                break;
            }
            let mut a_opt = inarcsorig[nfa.st(s2).no as usize];
            while let Some(a) = a_opt {
                if nfa.ar(a).type_ != EMPTY {
                    arcarray.push(a);
                }
                a_opt = nfa.ar(a).inchain;
            }
            let nexts = nfa.st(s2).tmp;
            nfa.st_mut(s2).tmp = None;
            s2_opt = nexts;
        }
        nfa.st_mut(s).tmp = None;
        debug_assert!(arcarray.len() <= totalinarcs);

        let prevnins = nfa.st(s).nins;

        mergeins(mcx, nfa, cm, has_parent, s, arcarray)?;

        let mut nskip = nfa.st(s).nins - prevnins;
        let mut a = nfa.st(s).ins;
        while nskip > 0 {
            a = nfa.ar(a.unwrap()).inchain;
            nskip -= 1;
        }
        inarcsorig[nfa.st(s).no as usize] = a;

        s_opt = nfa.st(s).next;
    }

    let mut s_opt = nfa.live_states;
    while let Some(s) = s_opt {
        let mut a_opt = nfa.st(s).outs;
        while let Some(a) = a_opt {
            let nexta = nfa.ar(a).outchain; // SNAPSHOT next before possible free
            if nfa.ar(a).type_ == EMPTY {
                freearc(nfa, cm, has_parent, a);
            }
            a_opt = nexta;
        }
        s_opt = nfa.st(s).next;
    }

    let mut s_opt = nfa.live_states;
    while let Some(s) = s_opt {
        let nexts = nfa.st(s).next; // SNAPSHOT next before possible dropstate
        if (nfa.st(s).nins == 0 || nfa.st(s).nouts == 0) && nfa.st(s).flag == 0 {
            dropstate(nfa, cm, has_parent, s)?;
        }
        s_opt = nexts;
    }
    Ok(())
}

fn emptyreachable(
    nfa: &mut Nfa,
    s: StateId,
    lastfound: StateId,
    inarcsorig: &[Option<ArcId>],
    depth: u32,
) -> RegResult<StateId> {
    if depth >= MAX_RECURSION_DEPTH {
        return Err(err_etoobig());
    }

    nfa.st_mut(s).tmp = Some(lastfound);
    let mut lastfound = s;
    let mut a_opt = inarcsorig[nfa.st(s).no as usize];
    while let Some(a) = a_opt {
        let from = nfa.ar(a).from.ok_or(err_assert())?;
        if nfa.ar(a).type_ == EMPTY && nfa.st(from).tmp.is_none() {
            lastfound = emptyreachable(nfa, from, lastfound, inarcsorig, depth + 1)?;
        }
        a_opt = nfa.ar(a).inchain;
    }
    Ok(lastfound)
}


pub fn checkmatchall(nfa: &mut Nfa, cm: &ColorMap) {
    if nfa.nstates > DUPINF * 2 {
        return;
    }

    let mut s_opt = nfa.live_states;
    while let Some(s) = s_opt {
        let mut a_opt = nfa.st(s).outs;
        while let Some(a) = a_opt {
            if nfa.ar(a).type_ != PLAIN {
                return; // any LACONs make it non-matchall
            }
            if nfa.ar(a).co != RAINBOW {
                let co = nfa.ar(a).co;
                if (cm.cd[co as usize].flags & crate::regguts::PSEUDO) != 0 {
                    let ato = nfa.ar(a).to.unwrap();
                    if s == nfa.pre && (co == nfa.bos[0] || co == nfa.bos[1]) {
                    } else if ato == nfa.post && (co == nfa.eos[0] || co == nfa.eos[1]) {
                    } else {
                        return; // unexpected pseudocolor arc
                    }
                } else {
                    return; // any other color makes it non-matchall
                }
            }
            a_opt = nfa.ar(a).outchain;
        }
        debug_assert!(nfa.st(s).tmp.is_none());
        s_opt = nfa.st(s).next;
    }

    let pre = nfa.pre;
    let post = nfa.post;
    if !check_out_colors_match(nfa, pre, RAINBOW, nfa.bos[0])
        || !check_out_colors_match(nfa, pre, RAINBOW, nfa.bos[1])
        || !check_in_colors_match(nfa, post, RAINBOW, nfa.eos[0])
        || !check_in_colors_match(nfa, post, RAINBOW, nfa.eos[1])
    {
        return;
    }

    let nstates = nfa.nstates as usize;
    let mut haspaths: Vec<Option<Vec<bool>>> = Vec::new();
    if haspaths.try_reserve_exact(nstates).is_err() {
        return; // soft-fail: treat as non-matchall
    }
    haspaths.resize_with(nstates, || None);

    if checkmatchall_recurse(nfa, pre, &mut haspaths, 0) {
        let pre_no = nfa.st(pre).no as usize;
        let haspath = haspaths[pre_no]
            .as_ref()
            .expect("checkmatchall: pre haspath must be set on success");

        let mut minmatch: i32 = 0;
        while minmatch <= DUPINF + 1 {
            if haspath[minmatch as usize] {
                break;
            }
            minmatch += 1;
        }
        debug_assert!(minmatch <= DUPINF + 1); // else checkmatchall_recurse lied

        let mut maxmatch: i32 = minmatch;
        while maxmatch < DUPINF + 1 {
            if !haspath[(maxmatch + 1) as usize] {
                break;
            }
            maxmatch += 1;
        }

        let mut ok = true;
        let mut morematch: i32 = maxmatch + 1;
        while morematch <= DUPINF + 1 {
            if haspath[morematch as usize] {
                ok = false; // fail, there are nonconsecutive lengths
                break;
            }
            morematch += 1;
        }

        if ok {
            debug_assert!(minmatch > 0); // else pre and post states were adjacent
            nfa.minmatchall = minmatch - 1;
            nfa.maxmatchall = maxmatch - 1;
            nfa.flags |= MATCHALL;
        }
    }
}

fn checkmatchall_recurse(
    nfa: &mut Nfa,
    s: StateId,
    haspaths: &mut Vec<Option<Vec<bool>>>,
    depth: u32,
) -> bool {
    if depth >= MAX_RECURSION_DEPTH {
        return false;
    }

    check_interrupt();

    let mut haspath: Vec<bool> = Vec::new();
    if haspath.try_reserve_exact((DUPINF + 2) as usize).is_err() {
        return false;
    }
    haspath.resize((DUPINF + 2) as usize, false);

    debug_assert!(nfa.st(s).tmp.is_none());
    nfa.st_mut(s).tmp = Some(s);

    let mut result = false;
    let mut foundloop = false;

    let mut a_opt = nfa.st(s).outs;
    while let Some(a) = a_opt {
        let nexta = nfa.ar(a).outchain; // capture before any mutation
        if nfa.ar(a).co != RAINBOW {
            a_opt = nexta;
            continue; // ignore pseudocolor arcs
        }
        let ato = match nfa.ar(a).to {
            Some(t) => t,
            None => {
                result = false;
                break;
            }
        };
        if ato == nfa.post {
            result = true;
            haspath[0] = true;
        } else if ato == s {
            foundloop = true;
        } else if nfa.st(ato).tmp.is_some() {
            result = false;
            break;
        } else {
            let ato_no = nfa.st(ato).no as usize;

            if haspaths[ato_no].is_none() {
                result = checkmatchall_recurse(nfa, ato, haspaths, depth + 1);
                if !result {
                    break;
                }
            } else {
                result = true;
            }
            debug_assert!(nfa.st(ato).tmp.is_none());
            let nexthaspath = haspaths[ato_no]
                .as_ref()
                .expect("checkmatchall_recurse: visited state must have a haspath");

            if nexthaspath[DUPINF as usize] != nexthaspath[(DUPINF + 1) as usize] {
                result = false;
                break;
            }
            for i in 0..DUPINF as usize {
                haspath[i + 1] |= nexthaspath[i];
            }
            haspath[(DUPINF + 1) as usize] |= nexthaspath[(DUPINF + 1) as usize];
        }
        a_opt = nfa.ar(a).outchain;
    }

    if result && foundloop {
        let mut i: i32 = 0;
        while i <= DUPINF {
            if haspath[i as usize] {
                break;
            }
            i += 1;
        }
        i += 1;
        while i <= DUPINF + 1 {
            haspath[i as usize] = true;
            i += 1;
        }
    }

    let s_no = nfa.st(s).no;
    debug_assert!(s_no < nfa.nstates);
    debug_assert!(haspaths[s_no as usize].is_none());
    haspaths[s_no as usize] = Some(haspath);

    nfa.st_mut(s).tmp = None;

    result
}

fn check_out_colors_match(nfa: &mut Nfa, s: StateId, co1: color, co2: color) -> bool {
    let mut result = true;

    let mut a_opt = nfa.st(s).outs;
    while let Some(a) = a_opt {
        if nfa.ar(a).co == co1 {
            match nfa.ar(a).to {
                Some(to) => {
                    debug_assert!(nfa.st(to).tmp.is_none());
                    nfa.st_mut(to).tmp = Some(to);
                }
                None => result = false,
            }
        }
        a_opt = nfa.ar(a).outchain;
    }
    let mut a_opt = nfa.st(s).outs;
    while let Some(a) = a_opt {
        if nfa.ar(a).co == co2 {
            match nfa.ar(a).to {
                Some(to) => {
                    if nfa.st(to).tmp.is_some() {
                        nfa.st_mut(to).tmp = None;
                    } else {
                        result = false; // unmatched co2 arc
                    }
                }
                None => result = false,
            }
        }
        a_opt = nfa.ar(a).outchain;
    }
    let mut a_opt = nfa.st(s).outs;
    while let Some(a) = a_opt {
        if nfa.ar(a).co == co1 {
            match nfa.ar(a).to {
                Some(to) => {
                    if nfa.st(to).tmp.is_some() {
                        result = false; // unmatched co1 arc
                        nfa.st_mut(to).tmp = None;
                    }
                }
                None => result = false,
            }
        }
        a_opt = nfa.ar(a).outchain;
    }
    result
}

fn check_in_colors_match(nfa: &mut Nfa, s: StateId, co1: color, co2: color) -> bool {
    let mut result = true;

    let mut a_opt = nfa.st(s).ins;
    while let Some(a) = a_opt {
        if nfa.ar(a).co == co1 {
            match nfa.ar(a).from {
                Some(from) => {
                    debug_assert!(nfa.st(from).tmp.is_none());
                    nfa.st_mut(from).tmp = Some(from);
                }
                None => result = false,
            }
        }
        a_opt = nfa.ar(a).inchain;
    }
    let mut a_opt = nfa.st(s).ins;
    while let Some(a) = a_opt {
        if nfa.ar(a).co == co2 {
            match nfa.ar(a).from {
                Some(from) => {
                    if nfa.st(from).tmp.is_some() {
                        nfa.st_mut(from).tmp = None;
                    } else {
                        result = false; // unmatched co2 arc
                    }
                }
                None => result = false,
            }
        }
        a_opt = nfa.ar(a).inchain;
    }
    let mut a_opt = nfa.st(s).ins;
    while let Some(a) = a_opt {
        if nfa.ar(a).co == co1 {
            match nfa.ar(a).from {
                Some(from) => {
                    if nfa.st(from).tmp.is_some() {
                        result = false; // unmatched co1 arc
                        nfa.st_mut(from).tmp = None;
                    }
                }
                None => result = false,
            }
        }
        a_opt = nfa.ar(a).inchain;
    }
    result
}


#[inline]
fn isconstraintarc(nfa: &Nfa, a: ArcId) -> bool {
    let t = nfa.ar(a).type_;
    t == ARC_BOS || t == ARC_EOS || t == BEHIND || t == AHEAD || t == LACON
}

fn hasconstraintout(nfa: &Nfa, s: StateId) -> bool {
    let mut cur = nfa.st(s).outs;
    while let Some(a) = cur {
        if isconstraintarc(nfa, a) {
            return true;
        }
        cur = nfa.ar(a).outchain;
    }
    false
}


pub fn fixconstraintloops<'mcx>(
    mcx: Mcx<'mcx>,
    nfa: &mut Nfa,
    cm: &mut ColorMap,
    has_parent: bool,
) -> RegResult<()> {
    let mut hasconstraints = false;
    let mut s_opt = nfa.live_states;
    while let Some(s) = s_opt {
        let nexts = nfa.st(s).next; // SNAPSHOT next before possible dropstate
        debug_assert!(nfa.st(s).tmp.is_none());
        let mut a_opt = nfa.st(s).outs;
        while let Some(a) = a_opt {
            let nexta = nfa.ar(a).outchain; // SNAPSHOT next before possible free
            if isconstraintarc(nfa, a) {
                if nfa.ar(a).to == Some(s) {
                    freearc(nfa, cm, has_parent, a);
                } else {
                    hasconstraints = true;
                }
            }
            a_opt = nexta;
        }
        if nfa.st(s).nouts == 0 && nfa.st(s).flag == 0 {
            dropstate(nfa, cm, has_parent, s)?;
        }
        s_opt = nexts;
    }

    if !hasconstraints {
        return Ok(());
    }

    'restart: loop {
        let mut s_opt = nfa.live_states;
        while let Some(s) = s_opt {
            if findconstraintloop(mcx, nfa, cm, has_parent, s, 0)? {
                continue 'restart;
            }
            s_opt = nfa.st(s).next;
        }
        break;
    }

    let mut s_opt = nfa.live_states;
    while let Some(s) = s_opt {
        let nexts = nfa.st(s).next; // SNAPSHOT next before possible dropstate
        nfa.st_mut(s).tmp = None;
        if (nfa.st(s).nins == 0 || nfa.st(s).nouts == 0) && nfa.st(s).flag == 0 {
            dropstate(nfa, cm, has_parent, s)?;
        }
        s_opt = nexts;
    }
    Ok(())
}

fn findconstraintloop<'mcx>(
    mcx: Mcx<'mcx>,
    nfa: &mut Nfa,
    cm: &mut ColorMap,
    has_parent: bool,
    s: StateId,
    depth: u32,
) -> RegResult<bool> {
    if depth >= MAX_RECURSION_DEPTH {
        return Err(err_etoobig());
    }

    if let Some(tmp) = nfa.st(s).tmp {
        if tmp == s {
            return Ok(false);
        }
        breakconstraintloop(mcx, nfa, cm, has_parent, s)?;
        return Ok(true);
    }
    let mut a_opt = nfa.st(s).outs;
    while let Some(a) = a_opt {
        if isconstraintarc(nfa, a) {
            let sto = nfa.ar(a).to.ok_or(err_assert())?;
            debug_assert_ne!(sto, s);
            nfa.st_mut(s).tmp = Some(sto);
            if findconstraintloop(mcx, nfa, cm, has_parent, sto, depth + 1)? {
                return Ok(true);
            }
        }
        a_opt = nfa.ar(a).outchain;
    }

    nfa.st_mut(s).tmp = Some(s);
    Ok(false)
}

fn breakconstraintloop<'mcx>(
    mcx: Mcx<'mcx>,
    nfa: &mut Nfa,
    cm: &mut ColorMap,
    has_parent: bool,
    sinitial: StateId,
) -> RegResult<()> {
    let mut refarc: Option<ArcId> = None;
    let mut s = sinitial;
    loop {
        let nexts = nfa
            .st(s)
            .tmp
            .expect("breakconstraintloop: loop member has no tmp");
        debug_assert_ne!(nexts, s); // should not see any one-element loops
        if refarc.is_none() {
            let mut narcs = 0;
            let mut a_opt = nfa.st(s).outs;
            while let Some(a) = a_opt {
                if nfa.ar(a).to == Some(nexts) && isconstraintarc(nfa, a) {
                    refarc = Some(a);
                    narcs += 1;
                }
                a_opt = nfa.ar(a).outchain;
            }
            debug_assert!(narcs > 0);
            if narcs > 1 {
                refarc = None; // multiple constraint arcs here, no good
            }
        }
        s = nexts;
        if s == sinitial {
            break;
        }
    }

    let shead;
    let stail;
    if let Some(ra) = refarc {
        shead = nfa.ar(ra).from.ok_or(err_assert())?;
        stail = nfa.ar(ra).to.ok_or(err_assert())?;
        debug_assert_eq!(Some(stail), nfa.st(shead).tmp);
    } else {
        shead = sinitial;
        stail = nfa.st(sinitial).tmp.ok_or(err_assert())?;
    }

    let mut s_opt = nfa.live_states;
    while let Some(st) = s_opt {
        nfa.st_mut(st).tmp = None;
        s_opt = nfa.st(st).next;
    }

    let new_sc = newstate(mcx, nfa)?;
    let mut sclone: Option<StateId> = Some(new_sc);

    let nstates = nfa.nstates;
    clonesuccessorstates(
        mcx, nfa, cm, has_parent, stail, new_sc, shead, refarc, None, None, nstates, 0,
    )?;

    if nfa.st(new_sc).nouts == 0 {
        freestate(nfa, new_sc);
        sclone = None;
    }

    let mut a_opt = nfa.st(shead).outs;
    while let Some(a) = a_opt {
        let nexta = nfa.ar(a).outchain; // SNAPSHOT next before possible free
        if nfa.ar(a).to == Some(stail) && isconstraintarc(nfa, a) {
            let cparc_res = match sclone {
                Some(sc) => cparc(mcx, nfa, cm, has_parent, a, shead, sc),
                None => Ok(()),
            };
            freearc(nfa, cm, has_parent, a);
            cparc_res?;
        }
        a_opt = nexta;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn clonesuccessorstates<'mcx>(
    mcx: Mcx<'mcx>,
    nfa: &mut Nfa,
    cm: &mut ColorMap,
    has_parent: bool,
    ssource: StateId,
    sclone: StateId,
    spredecessor: StateId,
    refarc: Option<ArcId>,
    curdonemap: Option<&mut Vec<u8>>,
    outerdonemap: Option<&[u8]>,
    nstates: i32,
    depth: u32,
) -> RegResult<()> {
    if depth >= MAX_RECURSION_DEPTH {
        return Err(err_etoobig());
    }

    match curdonemap {
        Some(dm) => {
            clonesuccessorstates_fill(
                mcx, nfa, cm, has_parent, ssource, sclone, spredecessor, refarc, dm, outerdonemap,
                nstates, depth,
            )
        }
        None => {
            let mut donemap: Vec<u8> = Vec::new();
            donemap.try_reserve_exact(nstates as usize)?;
            if let Some(outer) = outerdonemap {
                debug_assert_eq!(outer.len(), nstates as usize);
                donemap.extend_from_slice(outer);
            } else {
                donemap.resize(nstates as usize, 0);
                debug_assert!((nfa.st(spredecessor).no as i64) < nstates as i64);
                donemap[nfa.st(spredecessor).no as usize] = 1;
            }

            clonesuccessorstates_fill(
                mcx,
                nfa,
                cm,
                has_parent,
                ssource,
                sclone,
                spredecessor,
                refarc,
                &mut donemap,
                outerdonemap,
                nstates,
                depth,
            )?;

            let mut a_opt = nfa.st(sclone).outs;
            while let Some(a) = a_opt {
                let stoclone = nfa.ar(a).to.ok_or(err_assert())?;
                let sto = nfa.st(stoclone).tmp;
                if let Some(sto) = sto {
                    nfa.st_mut(stoclone).tmp = None;
                    clonesuccessorstates(
                        mcx,
                        nfa,
                        cm,
                        has_parent,
                        sto,
                        stoclone,
                        spredecessor,
                        refarc,
                        None,
                        Some(&donemap),
                        nstates,
                        depth + 1,
                    )?;
                }
                a_opt = nfa.ar(a).outchain;
            }
            Ok(())
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn clonesuccessorstates_fill<'mcx>(
    mcx: Mcx<'mcx>,
    nfa: &mut Nfa,
    cm: &mut ColorMap,
    has_parent: bool,
    ssource: StateId,
    sclone: StateId,
    spredecessor: StateId,
    refarc: Option<ArcId>,
    donemap: &mut Vec<u8>,
    outerdonemap: Option<&[u8]>,
    nstates: i32,
    depth: u32,
) -> RegResult<()> {
    debug_assert!((nfa.st(ssource).no as i64) < nstates as i64);
    debug_assert_eq!(donemap[nfa.st(ssource).no as usize], 0);
    donemap[nfa.st(ssource).no as usize] = 1;

    let mut a_opt = nfa.st(ssource).outs;
    while let Some(a) = a_opt {
        let nexta = nfa.ar(a).outchain;
        let sto = nfa.ar(a).to.ok_or(err_assert())?;

        if isconstraintarc(nfa, a) && hasconstraintout(nfa, sto) {
            debug_assert!((nfa.st(sto).no as i64) < nstates as i64);
            if donemap[nfa.st(sto).no as usize] != 0 {
                a_opt = nexta;
                continue;
            }

            let mut prevclone: Option<StateId> = None;
            let mut a2_opt = nfa.st(sclone).outs;
            while let Some(a2) = a2_opt {
                let a2to = nfa.ar(a2).to.ok_or(err_assert())?;
                if nfa.st(a2to).tmp == Some(sto) {
                    prevclone = Some(a2to);
                    break;
                }
                a2_opt = nfa.ar(a2).outchain;
            }

            let (a_type, a_co) = (nfa.ar(a).type_, nfa.ar(a).co);
            let canmerge = if let Some(ra) = refarc {
                if a_type == nfa.ar(ra).type_ && a_co == nfa.ar(ra).co {
                    true
                } else {
                    inarc_chain_canmerge(nfa, sclone, a_type, a_co)?
                }
            } else {
                inarc_chain_canmerge(nfa, sclone, a_type, a_co)?
            };

            if canmerge {
                if let Some(pc) = prevclone {
                    dropstate(nfa, cm, has_parent, pc)?; // kills our outarc, too
                }

                clonesuccessorstates(
                    mcx,
                    nfa,
                    cm,
                    has_parent,
                    sto,
                    sclone,
                    spredecessor,
                    refarc,
                    Some(&mut *donemap),
                    outerdonemap,
                    nstates,
                    depth + 1,
                )?;
                debug_assert_eq!(donemap[nfa.st(sto).no as usize], 1);
            } else if let Some(pc) = prevclone {
                cparc(mcx, nfa, cm, has_parent, a, sclone, pc)?;
            } else {
                let stoclone = newstate(mcx, nfa)?;
                nfa.st_mut(stoclone).tmp = Some(sto);
                cparc(mcx, nfa, cm, has_parent, a, sclone, stoclone)?;
            }
        } else {
            cparc(mcx, nfa, cm, has_parent, a, sclone, sto)?;
        }

        a_opt = nexta;
    }
    Ok(())
}

fn inarc_chain_canmerge(nfa: &Nfa, sclone: StateId, a_type: i32, a_co: color) -> RegResult<bool> {
    let mut s = sclone;
    while let Some(ins) = nfa.st(s).ins {
        if nfa.st(s).nins == 1 && a_type == nfa.ar(ins).type_ && a_co == nfa.ar(ins).co {
            return Ok(true);
        }
        s = nfa.ar(ins).from.ok_or(err_assert())?;
    }
    Ok(false)
}


pub fn optimize<'mcx>(
    mcx: Mcx<'mcx>,
    nfa: &mut Nfa,
    cm: &mut ColorMap,
    has_parent: bool,
) -> RegResult<i64> {
    if nfa.flags & HASCANTMATCH != 0 {
        removecantmatch(nfa, cm, has_parent)?;
        nfa.flags &= !HASCANTMATCH;
    }
    cleanup(nfa, cm, has_parent)?; // may simplify situation
    fixempties(mcx, nfa, cm, has_parent)?; // get rid of EMPTY arcs
    fixconstraintloops(mcx, nfa, cm, has_parent)?; // get rid of constraint loops
    pullback(mcx, nfa, cm, has_parent)?; // pull back constraints backward
    pushfwd(mcx, nfa, cm, has_parent)?; // push fwd constraints forward
    cleanup(nfa, cm, has_parent)?; // final tidying
    analyze(nfa, cm) // and analysis
}

pub fn analyze(nfa: &mut Nfa, cm: &mut ColorMap) -> RegResult<i64> {

    if nfa.st(nfa.pre).outs.is_none() {
        return Ok(REG_UIMPOSSIBLE as i64);
    }

    checkmatchall(nfa, cm);

    let post = nfa.post;
    let mut a_opt = nfa.st(nfa.pre).outs;
    while let Some(a) = a_opt {
        let ato = nfa.ar(a).to.ok_or(err_assert())?;
        let mut aa_opt = nfa.st(ato).outs;
        while let Some(aa) = aa_opt {
            if nfa.ar(aa).to == Some(post) {
                return Ok(REG_UEMPTYMATCH as i64);
            }
            aa_opt = nfa.ar(aa).outchain;
        }
        a_opt = nfa.ar(a).outchain;
    }
    Ok(0)
}


pub fn compact<'mcx>(mcx: Mcx<'mcx>, nfa: &Nfa, cm: &ColorMap, cnfa: &mut Cnfa) -> RegResult<()> {
    let _ = mcx; // arena is plain Vec at this stage; mcx threaded for parity
    let mut nstates: usize = 0;
    let mut narcs: usize = 0;
    let mut s_opt = nfa.live_states;
    while let Some(s) = s_opt {
        nstates += 1;
        let nouts = nfa.st(s).nouts as usize;
        narcs = narcs
            .checked_add(nouts)
            .and_then(|n| n.checked_add(1))
            .ok_or(crate::regex_error::err_espace())?;
        s_opt = nfa.st(s).next;
    }

    let mut stflags: Vec<u8> = Vec::new();
    stflags.try_reserve_exact(nstates)?;
    let mut states: Vec<core::ops::Range<usize>> = Vec::new();
    states.try_reserve_exact(nstates)?;
    let mut arcs: Vec<Carc> = Vec::new();
    arcs.try_reserve_exact(narcs)?;

    for _ in 0..nstates {
        stflags.push(0);
        states.push(0..0);
    }

    let ncolors = (maxcolor(cm) as i32) + 1;

    cnfa.nstates = nstates as i32;
    cnfa.pre = nfa.st(nfa.pre).no;
    cnfa.post = nfa.st(nfa.post).no;
    cnfa.bos[0] = nfa.bos[0];
    cnfa.bos[1] = nfa.bos[1];
    cnfa.eos[0] = nfa.eos[0];
    cnfa.eos[1] = nfa.eos[1];
    cnfa.ncolors = ncolors;
    cnfa.flags = nfa.flags;
    cnfa.minmatchall = nfa.minmatchall;
    cnfa.maxmatchall = nfa.maxmatchall;

    let mut s_opt = nfa.live_states;
    while let Some(s) = s_opt {
        let s_no = nfa.st(s).no;
        debug_assert!((s_no as usize) < nstates);
        stflags[s_no as usize] = 0;
        let first = arcs.len();
        let mut a_opt = nfa.st(s).outs;
        while let Some(a) = a_opt {
            let arc = nfa.ar(a);
            let to_no = nfa.st(arc.to.unwrap()).no;
            if arc.type_ == PLAIN {
                arcs.push(Carc {
                    co: arc.co,
                    to: to_no,
                });
            } else if arc.type_ == LACON {
                debug_assert!(s_no != cnfa.pre);
                debug_assert!(arc.co >= 0);
                arcs.push(Carc {
                    co: (ncolors + arc.co as i32) as color,
                    to: to_no,
                });
                cnfa.flags |= HASLACONS;
            } else {
                return Err(err_assert());
            }
            a_opt = arc.outchain;
        }
        let end = arcs.len();
        carcsort(&mut arcs[first..end]);
        states[s_no as usize] = first..end;
        arcs.push(Carc {
            co: COLORLESS,
            to: 0,
        });
        s_opt = nfa.st(s).next;
    }
    debug_assert_eq!(arcs.len(), narcs);
    debug_assert_ne!(cnfa.nstates, 0);

    let mut a_opt = nfa.st(nfa.pre).outs;
    while let Some(a) = a_opt {
        let to_no = nfa.st(nfa.ar(a).to.unwrap()).no;
        stflags[to_no as usize] = CNFA_NOPROGRESS;
        a_opt = nfa.ar(a).outchain;
    }
    stflags[nfa.st(nfa.pre).no as usize] = CNFA_NOPROGRESS;

    cnfa.stflags = stflags;
    cnfa.states = states;
    cnfa.arcs = arcs;
    Ok(())
}

fn carcsort(run: &mut [Carc]) {
    if run.len() > 1 {
        run.sort_unstable_by(carc_cmp);
    }
}

fn carc_cmp(aa: &Carc, bb: &Carc) -> core::cmp::Ordering {
    use core::cmp::Ordering;
    if aa.co < bb.co {
        return Ordering::Less;
    }
    if aa.co > bb.co {
        return Ordering::Greater;
    }
    if aa.to < bb.to {
        return Ordering::Less;
    }
    if aa.to > bb.to {
        return Ordering::Greater;
    }
    Ordering::Equal
}

pub fn freecnfa(cnfa: &mut Cnfa) {
    debug_assert_ne!(cnfa.nstates, 0); // not empty already (C: assert(!NULLCNFA))
    cnfa.stflags = Vec::new();
    cnfa.states = Vec::new();
    cnfa.arcs = Vec::new();
    cnfa.nstates = 0; // ZAPCNFA semantics (production: nstates = 0)
}


#[inline]
pub fn set_hascantmatch(nfa: &mut Nfa) {
    nfa.flags |= HASCANTMATCH;
}


pub fn getcolor(cm: &ColorMap, c: chr) -> color {
    crate::regex_foundation::pg_reg_getcolor(cm, c)
}
