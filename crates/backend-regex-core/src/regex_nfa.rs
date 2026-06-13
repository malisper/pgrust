//! Family: **regex-nfa** — `regc_nfa.c`: the NFA graph machinery plus the five
//! colormap<->NFA-arc functions split out of `regc_color.c`
//! (`okcolors`/`colorchain`/`uncolorchain`/`rainbow`/`colorcomplement`).
//!
//! C uses raw pointer chains; the port uses the [`crate::regguts`] arena
//! ([`Nfa::state_arena`]/[`Nfa::arc_arena`]) indexed by [`StateId`]/[`ArcId`].
//! Allocating functions take `Mcx<'mcx>` and return `RegResult` (NFA-complexity
//! overflow -> `REG_ETOOBIG`; true-OOM -> `REG_ESPACE`).
//!
//! [`Nfa::state_arena`]: crate::regguts::Nfa::state_arena
//! [`Nfa::arc_arena`]: crate::regguts::Nfa::arc_arena
//!
//! # Colormap / parent
//!
//! The C `struct nfa` carries `cm` (colormap) and `parent` pointers; the port's
//! [`Nfa`] omits both (per opacity-inherited / arena rules). Functions whose C
//! bodies touch `nfa->cm` or `nfa->parent` thread the colormap in as an explicit
//! `&mut ColorMap` argument and the parent as a `has_parent: bool` flag, exactly
//! as the proven idiomatic port does; the colorchain bookkeeping runs when
//! `COLORED(a) && !has_parent` (C: `COLORED(a) && nfa->parent == NULL`). These
//! arguments are absent from the scaffold's draft signatures but are required to
//! mirror `regc_nfa.c`/`regc_color.c` faithfully.
//!
//! # Routing to unported neighbors
//!
//! The colormap *allocators* (`maxcolor`/`pseudocolor`/`freecolor`) are owned by
//! the [`crate::regex_foundation`] family; calls route to that owner (its bodies
//! panic loudly until it lands). `getcolor` re-exports
//! [`crate::regex_foundation::pg_reg_getcolor`].
//!
//! # Recursion guards
//!
//! The recursive traversals carry an explicit depth counter that fails with
//! `REG_ETOOBIG` past [`MAX_RECURSION_DEPTH`], reproducing C's
//! `STACK_TOO_DEEP -> REG_ETOOBIG`.

extern crate alloc;

use alloc::vec::Vec;

use mcx::Mcx;

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

// =============================================================================
// combine() result codes  (regcomp.c)
// =============================================================================

/// combine result: destroys arc.
pub const INCOMPATIBLE: i32 = 1;
/// combine result: constraint satisfied.
pub const SATISFIED: i32 = 2;
/// combine result: compatible but not satisfied yet.
pub const COMPATIBLE: i32 = 3;
/// combine result: replace arc's color with constraint color.
pub const REPLACEARC: i32 = 4;

// =============================================================================
// space metering  (regguts.h / regcustom.h)
// =============================================================================

/// `REG_MAX_COMPILE_SPACE` analogue: cap on transient compile-time space.
///
/// C: `500000 * (sizeof(struct state) + 4 * sizeof(struct arc))`. The arena
/// model charges `size_of::<State>()` per state and `size_of::<Arc>()` per arc;
/// the limit uses the same 500000 * (one state + four arcs) formula so the
/// failure point scales the same way.
#[inline]
pub fn reg_max_compile_space() -> usize {
    500_000 * (core::mem::size_of::<State>() + 4 * core::mem::size_of::<Arc>())
}

/// Recursion-depth ceiling for the recursive traversals; mirrors C's
/// `STACK_TOO_DEEP -> REG_ETOOBIG`.
pub const MAX_RECURSION_DEPTH: u32 = 10_000;

// =============================================================================
// cancel hook  (INTERRUPT / CHECK_FOR_INTERRUPTS)
// =============================================================================

/// Cancel-check hook. C calls `INTERRUPT(nfa->v->re)` at the state/arc creation
/// sites and in the bulk fast paths. The interrupt facility is not yet ported,
/// so this is a documented no-op wired in at the same call sites.
#[inline]
fn check_interrupt() {
    // no-op until the interrupt facility is ported.
}

// =============================================================================
// COLORED predicate  (regcomp.c macro)
// =============================================================================

/// `COLORED(a)` -- is an arc colored, and hence should belong to a color chain?
/// The `co >= 0` test eliminates RAINBOW (and COLORLESS) arcs.
#[inline]
fn colored(type_: i32, co: color) -> bool {
    co >= 0 && (type_ == PLAIN || type_ == AHEAD || type_ == BEHIND)
}

// =============================================================================
// arena field accessors (read/write a single State/Arc through the arena Vec)
// =============================================================================

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

// =============================================================================
// newnfa / freenfa
// =============================================================================

/// `newnfa(struct vars *v, struct colormap *cm, struct nfa *parent)` — create a
/// new NFA, sharing the colormap with its parent (if any). Builds the minimal
/// infrastructure (post/pre/init/final states plus the BOS/EOS rainbow and
/// `^`/`$` arcs). On error the partially built NFA is dropped (C: `freenfa`).
pub fn newnfa<'mcx>(mcx: Mcx<'mcx>, cm: &mut ColorMap, has_parent: bool) -> RegResult<Nfa> {
    // Make the NFA minimally valid (StateId fields get real values below; use a
    // placeholder until the infrastructure states exist).
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

    // Create required infrastructure.
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

/// `freenfa(struct nfa *nfa)` — free an NFA and all its states/arcs. The owned
/// arena `Vec`s are dropped; C's post-condition (`nstates = -1`, space released)
/// is mirrored on the passed-in struct.
pub fn freenfa(mut nfa: Nfa) {
    nfa.state_arena = Vec::new();
    nfa.arc_arena = Vec::new();
    nfa.spaceused = 0;
    nfa.live_states = None;
    nfa.slast = None;
    nfa.free_states = None;
    nfa.free_arcs = None;
    nfa.nstates = -1;
    // nfa is dropped here.
}

// =============================================================================
// newstate / newfstate / dropstate / freestate
// =============================================================================

/// `newstate(struct nfa *nfa)` — allocate a new (live) state, zero flag.
/// Recycles a free state if available, else allocates a fresh arena slot
/// (charging `size_of::<State>()` against `spaceused`, failing with
/// `REG_ETOOBIG` at the space cap, or `REG_ESPACE` if the underlying `Vec`
/// allocation fails). Threads the new state onto the tail of the live chain.
pub fn newstate<'mcx>(_mcx: Mcx<'mcx>, nfa: &mut Nfa) -> RegResult<StateId> {
    // Handy place to check for operation cancel during compilation.
    check_interrupt();

    let s: StateId;

    // First, recycle anything that's on the freelist.
    if let Some(f) = nfa.free_states {
        nfa.free_states = nfa.st(f).next;
        s = f;
    } else {
        // Otherwise, need to allocate a fresh arena slot. Charge complexity space
        // and enforce the cap exactly as C does (>= limit before allocating).
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

/// `newfstate(struct nfa *nfa, int flag)` — allocate a new state with a flag.
pub fn newfstate<'mcx>(mcx: Mcx<'mcx>, nfa: &mut Nfa, flag: u8) -> RegResult<StateId> {
    let s = newstate(mcx, nfa)?;
    nfa.st_mut(s).flag = flag;
    Ok(s)
}

/// `dropstate(struct nfa *nfa, struct state *s)` — delete a state's arcs, then
/// free it.
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

/// `freestate(struct nfa *nfa, struct state *s)` — free a (already-arcless)
/// state onto the free chain. Unlinks from the live chain and pushes onto the
/// free-state chain (via the `next` link, which doubles as the free chain in C).
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
    // Don't delete it; put it on the free list (reusing the `next` link).
    nfa.st_mut(s).next = nfa.free_states;
    nfa.free_states = Some(s);
}

// =============================================================================
// allocarc / freearc / newarc / createarc / cparc
// =============================================================================

/// `allocarc(struct nfa *nfa)` — allocate a new arc within an NFA. Recycles a
/// free arc (off `free_arcs`, threaded via the `outchain` link which aliases C's
/// `freechain`), else allocates a fresh arena slot, charging `size_of::<Arc>()`
/// and failing with `REG_ETOOBIG` at the space cap (or `REG_ESPACE` on a `Vec`
/// allocation failure).
fn allocarc<'mcx>(_mcx: Mcx<'mcx>, nfa: &mut Nfa) -> RegResult<ArcId> {
    // First, recycle anything that's on the freelist (freechain == outchain).
    if let Some(a) = nfa.free_arcs {
        nfa.free_arcs = nfa.ar(a).outchain;
        return Ok(a);
    }
    // Otherwise, allocate a fresh arena slot.
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

/// `createarc(struct nfa *nfa, int t, color co, struct state *from, struct state
/// *to)` — unconditionally create an arc. Must *only* be used after verifying
/// there is no existing identical arc.
///
/// Prepends the new arc to BOTH the from-state out-chain and the to-state
/// in-chain (load-bearing: later passes rely on "original arcs are last"), and
/// colorchains it when colored and the NFA has no parent.
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

/// `createarc_nochain` — the colormap-free core of [`createarc`]. Allocates the
/// arc and threads it onto the from-out and to-in chains, but does NOT
/// colorchain it (so it needs no `&mut ColorMap`), returning the new [`ArcId`].
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

    // Prepend to the to-state in-chain.
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

    // Prepend to the from-state out-chain.
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

/// `newarc(struct nfa *nfa, int t, color co, struct state *from, struct state
/// *to)` — add an arc (dedup against existing). Scans whichever of the two
/// chains is shorter for an identical arc and returns early if found; otherwise
/// calls [`createarc`].
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
    // Handy place to check for operation cancel during compilation.
    check_interrupt();

    // Check for duplicate arc, using whichever chain is shorter.
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

    // No dup, so create the arc.
    createarc(mcx, nfa, cm, has_parent, t, co, from, to)
}

/// `freearc(struct nfa *nfa, struct arc *victim)` — unlink and free an arc.
/// Removes the arc from the color chain (if colored and no parent), the source
/// out-chain, and the target in-chain, then pushes it onto the free-arc list
/// (via the `outchain` link, which aliases C's `freechain`).
pub fn freearc(nfa: &mut Nfa, cm: &mut ColorMap, has_parent: bool, victim: ArcId) {
    debug_assert_ne!(nfa.ar(victim).type_, 0);

    let from = nfa.ar(victim).from.expect("freearc: arc has no from");
    let to = nfa.ar(victim).to.expect("freearc: arc has no to");

    // Take it off the color chain if necessary.
    if colored(nfa.ar(victim).type_, nfa.ar(victim).co) && !has_parent {
        uncolorchain(nfa, cm, victim);
    }

    // Take it off source's out-chain.
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

    // Take it off target's in-chain.
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

    // Clean up and place on the NFA's free list (freechain == outchain).
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

/// `changearcsource(struct arc *a, struct state *newfrom)`. Caller must have
/// verified there is no pre-existing duplicate arc. Unlinks from the old
/// source's out-chain and prepends to the new source's.
pub fn changearcsource(nfa: &mut Nfa, a: ArcId, newfrom: StateId) {
    let oldfrom = nfa.ar(a).from.expect("changearcsource: arc has no from");
    debug_assert_ne!(oldfrom, newfrom);

    // Take it off old source's out-chain.
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

    // Prepend it to the new source's out-chain.
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

/// `changearctarget(struct arc *a, struct state *newto)`. Caller must have
/// verified there is no pre-existing duplicate arc.
pub fn changearctarget(nfa: &mut Nfa, a: ArcId, newto: StateId) {
    let oldto = nfa.ar(a).to.expect("changearctarget: arc has no to");
    debug_assert_ne!(oldto, newto);

    // Take it off old target's in-chain.
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

    // Prepend it to the new target's in-chain.
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

/// `hasnonemptyout(struct state *s)` — does state have a non-EMPTY out arc?
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

/// `findarc(struct state *s, int type, color co)` — find arc, if any, from a
/// given source with given type and color (first in chain order).
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

/// `cparc(struct nfa *nfa, struct arc *oa, struct state *from, struct state
/// *to)` — copy an arc onto a new from/to pair.
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

// =============================================================================
// sort helpers
// =============================================================================

/// The (from->no, co, type) sort key. Fails with REG_ASSERT if the chained
/// in-arc has no from state ("can't happen").
fn sortins_key(nfa: &Nfa, a: ArcId) -> RegResult<(i32, color, i32)> {
    let aa = nfa.ar(a);
    let f = aa.from.ok_or(err_assert())?;
    Ok((nfa.st(f).no, aa.co, aa.type_))
}

/// `sortins_cmp(const void *a, const void *b)` — compare two in-arcs by
/// (from->no, co, type), lexicographically (same field order as C).
fn sortins_cmp(nfa: &Nfa, a: ArcId, b: ArcId) -> RegResult<core::cmp::Ordering> {
    Ok(sortins_key(nfa, a)?.cmp(&sortins_key(nfa, b)?))
}

/// The (to->no, co, type) sort key. Fails with REG_ASSERT if the chained out-arc
/// has no to state.
fn sortouts_key(nfa: &Nfa, a: ArcId) -> RegResult<(i32, color, i32)> {
    let aa = nfa.ar(a);
    let t = aa.to.ok_or(err_assert())?;
    Ok((nfa.st(t).no, aa.co, aa.type_))
}

/// `sortouts_cmp(const void *a, const void *b)` — compare two out-arcs by
/// (to->no, co, type).
fn sortouts_cmp(nfa: &Nfa, a: ArcId, b: ArcId) -> RegResult<core::cmp::Ordering> {
    Ok(sortouts_key(nfa, a)?.cmp(&sortouts_key(nfa, b)?))
}

/// Sort a slice of arc handles by a fallible per-arc sort key. The comparator
/// passed to `sort_unstable_by` cannot propagate errors, so the keys are
/// extracted (fallibly) up front into a keyed workspace (`try_reserve`), sorted,
/// and written back.
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

/// Collect the arc handles of a chain (in or out) into a fresh `Vec`, sizing the
/// capacity from the known count `n` with a fallible `try_reserve`.
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

/// `sortins(struct nfa *nfa, struct state *s)` — sort a state's in-arcs by
/// from/color/type. Collects the in-arcs into an array, sorts, rewrites the
/// in-chain.
pub fn sortins<'mcx>(_mcx: Mcx<'mcx>, nfa: &mut Nfa, s: StateId) -> RegResult<()> {
    let n = nfa.st(s).nins;
    if n <= 1 {
        return Ok(()); // nothing to do
    }
    let ins = nfa.st(s).ins;
    let mut arr = collect_chain(nfa, ins, n, true)?;

    sort_arcids_by_key(nfa, &mut arr, sortins_key)?;

    // Rebuild arc list in order. Special-case first and last items.
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

/// `sortouts(struct nfa *nfa, struct state *s)` — sort a state's out-arcs by
/// to/color/type.
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

// =============================================================================
// bulk arc operations
// =============================================================================

/// `BULK_ARC_OP_USE_SORT(nsrcarcs, ndestarcs)` -- decide arc-by-arc vs
/// sort/merge.
#[inline]
fn bulk_arc_op_use_sort(nsrcarcs: i32, ndestarcs: i32) -> bool {
    if nsrcarcs < 4 {
        false
    } else {
        nsrcarcs > 32 || ndestarcs > 32
    }
}

/// `moveins(struct nfa *nfa, struct state *oldState, struct state *newState)`.
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
        // No need for de-duplication.
        while let Some(a) = nfa.st(old).ins {
            let (t, co, from) = (nfa.ar(a).type_, nfa.ar(a).co, nfa.ar(a).from.unwrap());
            createarc(mcx, nfa, cm, has_parent, t, co, from, new)?;
            freearc(nfa, cm, has_parent, a);
        }
    } else if !bulk_arc_op_use_sort(nfa.st(old).nins, nfa.st(new).nins) {
        // With not too many arcs, just do them one at a time.
        while let Some(a) = nfa.st(old).ins {
            let from = nfa.ar(a).from.unwrap();
            cparc(mcx, nfa, cm, has_parent, a, from, new)?;
            freearc(nfa, cm, has_parent, a);
        }
    } else {
        // Sort-merge approach. changearctarget() prepends to newState's chain,
        // which does not break our walk through the sorted part of the chain.
        check_interrupt();

        sortins(mcx, nfa, old)?;
        sortins(mcx, nfa, new)?;

        let mut oa = nfa.st(old).ins;
        let mut na = nfa.st(new).ins;
        while let (Some(o), Some(n)) = (oa, na) {
            match sortins_cmp(nfa, o, n)? {
                core::cmp::Ordering::Less => {
                    // newState does not have anything matching oa.
                    let nexto = nfa.ar(o).inchain; // SNAPSHOT next before relink
                    oa = nexto;
                    changearctarget(nfa, o, new);
                }
                core::cmp::Ordering::Equal => {
                    // Match, advance in both lists, drop dup from oldState.
                    oa = nfa.ar(o).inchain;
                    na = nfa.ar(n).inchain;
                    freearc(nfa, cm, has_parent, o);
                }
                core::cmp::Ordering::Greater => {
                    // Advance only na; oa might have a match later.
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

/// `copyins(struct nfa *nfa, struct state *oldState, struct state *newState)`.
/// In current usage this is *only* called with brand-new target states, so only
/// the "no need for de-duplication" path is live; the sort-merge path is
/// `#ifdef NOT_USED` in C and omitted.
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

/// `moveouts(struct nfa *nfa, struct state *oldState, struct state *newState)`.
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
        // No need for de-duplication.
        while let Some(a) = nfa.st(old).outs {
            let (t, co, to) = (nfa.ar(a).type_, nfa.ar(a).co, nfa.ar(a).to.unwrap());
            createarc(mcx, nfa, cm, has_parent, t, co, new, to)?;
            freearc(nfa, cm, has_parent, a);
        }
    } else if !bulk_arc_op_use_sort(nfa.st(old).nouts, nfa.st(new).nouts) {
        // With not too many arcs, just do them one at a time.
        while let Some(a) = nfa.st(old).outs {
            let to = nfa.ar(a).to.unwrap();
            cparc(mcx, nfa, cm, has_parent, a, new, to)?;
            freearc(nfa, cm, has_parent, a);
        }
    } else {
        // Sort-merge approach. changearcsource() prepends to newState's chain.
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

/// `copyouts(struct nfa *nfa, struct state *oldState, struct state *newState)`.
/// Only the "no need for de-duplication" path is live (see [`copyins`]).
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

/// `mergeins(struct nfa *nfa, struct state *s, struct arc **arcs, int n)`.
/// Like [`copyins`], but the source arcs are listed in an array and are not
/// guaranteed unique. It is OK to clobber the array contents.
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

    // Sort existing inarcs as well as proposed new ones.
    sortins(mcx, nfa, s)?;
    sort_arcids_by_key(nfa, &mut arcarray, sortins_key)?;

    // arcarray very likely includes dups; eliminate them.
    let mut j: usize = 0;
    for i in 1..arccount as usize {
        match sortins_cmp(nfa, arcarray[j], arcarray[i])? {
            core::cmp::Ordering::Less => {
                // non-dup
                j += 1;
                arcarray[j] = arcarray[i];
            }
            core::cmp::Ordering::Equal => {
                // dup
            }
            core::cmp::Ordering::Greater => {
                // trouble (NOTREACHED): array was sorted, so this can't happen.
                debug_assert!(false, "mergeins: array not sorted");
            }
        }
    }
    arccount = (j + 1) as i32;

    // Now merge into s' inchain. createarc() prepends to s's chain, so it does
    // not break our walk through the sorted part of the chain.
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
                // s does not have anything matching a.
                let (t, co, from) = (nfa.ar(a).type_, nfa.ar(a).co, nfa.ar(a).from.unwrap());
                createarc(mcx, nfa, cm, has_parent, t, co, from, s)?;
                i += 1;
            }
            core::cmp::Ordering::Equal => {
                // match, advance in both lists
                i += 1;
                na = nfa.ar(n).inchain;
            }
            core::cmp::Ordering::Greater => {
                // advance only na; array might have a match later
                na = nfa.ar(n).inchain;
            }
        }
    }
    while i < arccount as usize {
        // s does not have anything matching a.
        let a = arcarray[i];
        let (t, co, from) = (nfa.ar(a).type_, nfa.ar(a).co, nfa.ar(a).from.unwrap());
        createarc(mcx, nfa, cm, has_parent, t, co, from, s)?;
        i += 1;
    }
    Ok(())
}

/// `cloneouts(struct nfa *nfa, struct state *old, struct state *from, struct
/// state *to, int type)`. Only used to convert PLAIN arcs to AHEAD/BEHIND arcs.
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

// =============================================================================
// simple traversals
// =============================================================================

/// `delsub(struct nfa *nfa, struct state *lp, struct state *rp)` — delete a
/// sub-NFA between two states. Recursive traversal marking already-seen states
/// via `tmp`.
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

/// `deltraverse(struct nfa *nfa, struct state *leftend, struct state *s)` — the
/// recursive heart of delsub; destroys all out-arcs of the state.
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

/// `dupnfa(struct nfa *nfa, struct state *start, struct state *stop, struct
/// state *from, struct state *to)` — duplicate a sub-NFA. Uses `tmp` both to
/// mark already-seen states and to point at their duplicates.
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
    // done, except for clearing out the tmp pointers
    nfa.st_mut(stop).tmp = None;
    // Match C (regc_nfa.c dupnfa): cleartraverse runs UNCONDITIONALLY after
    // duptraverse, even on error -- C has no error check between the two, since
    // NERR only records the first error and never aborts the function body. We
    // therefore always run cleartraverse, then propagate the first error: the
    // duptraverse error takes precedence (set first), and any cleartraverse
    // error only surfaces when duptraverse succeeded.
    let clear_res = cleartraverse(nfa, start, 0);
    res?;
    clear_res?;
    Ok(())
}

/// `duptraverse(struct nfa *nfa, struct state *s, struct state *stmp)` —
/// recursive heart of dupnfa; `stmp` is s's duplicate, or None.
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

/// `removeconstraints(struct nfa *nfa, struct state *start, struct state
/// *stop)`. Constraint arcs are replaced by EMPTY arcs.
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
    // Match C: cleartraverse runs UNCONDITIONALLY after removetraverse, even on
    // error (removetraverse error takes precedence as it was set first).
    let clear_res = cleartraverse(nfa, start, 0);
    res?;
    clear_res?;
    Ok(())
}

/// `removetraverse(struct nfa *nfa, struct state *s)` — recursive heart of
/// removeconstraints.
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
            // nothing to do
        } else if t == AHEAD || t == BEHIND || t == ARC_BOS || t == ARC_EOS || t == LACON {
            // replace it
            newarc(mcx, nfa, cm, has_parent, EMPTY, 0, s, to)?;
            freearc(nfa, cm, has_parent, a);
        } else {
            return Err(err_assert());
        }
        cur = oa;
    }
    Ok(())
}

/// `cleartraverse(struct nfa *nfa, struct state *s)` — recursive cleanup for
/// algorithms that leave tmp ptrs set.
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

/// Cross-NFA `dupnfa`: copy the sub-NFA reachable from `start`..`stop` in `src`
/// into `dst`, hanging it from `from`..`to`. Both NFAs share the colormap `cm`.
///
/// In C, `dupnfa(struct nfa *nfa, start, stop, from, to)` is inherently
/// cross-NFA — `start`/`stop` may be states of a *different* NFA than `nfa`
/// (this is exactly how `nfanode` copies a parent sub-NFA into a fresh child
/// NFA), because states are heap pointers and `duptraverse` scribbles `s->tmp`
/// on the source states. The arena split forces the two NFAs to be distinct
/// arguments; the single-NFA [`dupnfa`] is the `src == dst` special case.
///
/// The destination NFA (the child built by `newnfa(v, v->cm, v->nfa)`) always
/// has a parent — it shares the colormap — so every arc created in `dst` runs
/// with `has_parent = true` (C: `COLORED(a) && nfa->parent == NULL` is false,
/// so the colorchain bookkeeping is skipped). Mirrors C `dupnfa`: scribble
/// `tmp` on the *source* states (pointing at their `dst` duplicates), run the
/// recursive duptraverse, then clear unconditionally (the first error wins).
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
        // newarc(nfa, EMPTY, 0, from, to) — into the destination NFA.
        newarc(mcx, dst, cm, true, EMPTY, 0, from, to)?;
        return Ok(());
    }

    // stop->tmp = to; (source state `stop` records its destination duplicate `to`)
    src.st_mut(stop).tmp = Some(to);
    let res = duptraverse_cross(mcx, dst, src, cm, start, Some(from), 0);
    // done, except for clearing out the tmp pointers
    src.st_mut(stop).tmp = None;
    // C runs cleartraverse unconditionally after duptraverse; the duptraverse
    // error takes precedence (set first), a cleartraverse error surfaces only
    // when duptraverse succeeded. cleartraverse walks the SOURCE states.
    let clear_res = cleartraverse(src, start, 0);
    res?;
    clear_res?;
    Ok(())
}

/// Cross-NFA `duptraverse`: recursive heart of [`dupnfa_cross`]. Reads arcs from
/// the SOURCE NFA, scribbles `tmp` on source states, and creates duplicate
/// states/arcs in the DESTINATION NFA. `stmp` is `s`'s duplicate (a `dst`
/// state id), or `None` (allocate a fresh `dst` state).
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
        // cparc(nfa, a, s->tmp, a->to->tmp): copy the source arc's (type, co)
        // onto the new from/to pair in the destination NFA.
        let (t, co) = (src.ar(a).type_, src.ar(a).co);
        newarc(mcx, dst, cm, true, t, co, sdup, todup)?;
        cur = src.ar(a).outchain;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// optimization / analysis passes
// ---------------------------------------------------------------------------

/// `markreachable(struct nfa *nfa, struct state *s, struct state *okay, struct
/// state *mark)` — recursive marking of reachable states.
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

/// `markcanreach(struct nfa *nfa, struct state *s, struct state *okay, struct
/// state *mark)` — recursive marking of states which can reach here (along
/// in-arcs).
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

/// `cleanup(struct nfa *nfa)` — remove dead states/arcs and renumber. Removes
/// unreachable or dead-end states (using `pre` to mark reachable, `post` to mark
/// can-reach-post), then renumbers the survivors.
pub fn cleanup(nfa: &mut Nfa, cm: &mut ColorMap, has_parent: bool) -> RegResult<()> {
    let pre = nfa.pre;
    let post = nfa.post;

    // Clear out unreachable or dead-end states.
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

    // Renumber surviving states.
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

// =============================================================================
// single_color_transition / specialcolors / removecantmatch / combine
// =============================================================================

/// `single_color_transition(struct state *s1, struct state *s2)` — does getting
/// from s1 to s2 cross one PLAIN arc? Returns the state whose out-arcs are
/// exactly the relevant parallel PLAIN arcs to s2, or None. Ignores a single
/// leading/trailing EMPTY arc.
pub fn single_color_transition(nfa: &Nfa, s1: StateId, s2: StateId) -> Option<StateId> {
    let mut s1 = s1;
    let mut s2 = s2;

    // Ignore leading EMPTY arc, if any.
    if nfa.st(s1).nouts == 1 {
        let a = nfa.st(s1).outs.unwrap();
        if nfa.ar(a).type_ == EMPTY {
            s1 = nfa.ar(a).to.unwrap();
        }
    }
    // Likewise for any trailing EMPTY arc.
    if nfa.st(s2).nins == 1 {
        let a = nfa.st(s2).ins.unwrap();
        if nfa.ar(a).type_ == EMPTY {
            s2 = nfa.ar(a).from.unwrap();
        }
    }
    // Perhaps we could have a single-state loop in between; if so reject.
    if s1 == s2 {
        return None;
    }
    // s1 must have at least one outarc...
    nfa.st(s1).outs?;
    // ... and they must all be PLAIN arcs to s2.
    let mut cur = nfa.st(s1).outs;
    while let Some(a) = cur {
        if nfa.ar(a).type_ != PLAIN || nfa.ar(a).to != Some(s2) {
            return None;
        }
        cur = nfa.ar(a).outchain;
    }
    Some(s1)
}

/// `specialcolors(struct nfa *nfa)` — assign BOS/EOS pseudocolors. For a
/// top-level NFA (no parent) allocates fresh pseudocolors; for a sub-NFA it
/// inherits the parent's. The parent BOS/EOS colors are passed explicitly here
/// since the [`Nfa`] has no parent link. `pseudocolor` is foundation-owned.
pub fn specialcolors<'mcx>(
    mcx: Mcx<'mcx>,
    nfa: &mut Nfa,
    cm: &mut ColorMap,
    parent: Option<([color; 2], [color; 2])>,
) -> RegResult<()> {
    match parent {
        None => {
            // false colors for BOS, BOL, EOS, EOL
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

/// `removecantmatch(struct nfa *nfa)` — drop CANTMATCH arcs (and let `cleanup`
/// pick up unreachable states). Walks every live state and frees its CANTMATCH
/// out-arcs (snapshotting the next link first).
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

/// `combine(struct nfa *nfa, struct arc *con, struct arc *a)` — decide how a
/// constraint arc and another arc interact. Returns one of
/// INCOMPATIBLE/SATISFIED/COMPATIBLE/REPLACEARC, porting the `CA(con->type,
/// a->type)` truth table case-for-case, including RAINBOW/PSEUDO special cases.
pub fn combine(nfa: &Nfa, cm: &ColorMap, con: ArcId, a: ArcId) -> i32 {
    // CA(ct, at) = (ct << CHAR_BIT) | at, with CHAR_BIT == 8.
    #[inline]
    fn ca(ct: i32, at: i32) -> i32 {
        (ct << 8) | at
    }

    let con_type = nfa.ar(con).type_;
    let con_co = nfa.ar(con).co;
    let a_type = nfa.ar(a).type_;
    let a_co = nfa.ar(a).co;

    let key = ca(con_type, a_type);

    // newlines are handled separately
    if key == ca(ARC_BOS, PLAIN) || key == ca(ARC_EOS, PLAIN) {
        return INCOMPATIBLE;
    }
    // color constraints meet colors
    if key == ca(AHEAD, PLAIN) || key == ca(BEHIND, PLAIN) {
        if con_co == a_co {
            return SATISFIED;
        }
        if con_co == RAINBOW {
            // con is satisfied unless arc's color is a pseudocolor
            if (cm.cd[a_co as usize].flags & crate::regguts::PSEUDO) == 0 {
                return SATISFIED;
            }
        } else if a_co == RAINBOW {
            // con is incompatible if it's for a pseudocolor (hypothetical)
            if (cm.cd[con_co as usize].flags & crate::regguts::PSEUDO) != 0 {
                return INCOMPATIBLE;
            }
            // otherwise, constraint constrains arc to be only its color
            return REPLACEARC;
        }
        return INCOMPATIBLE;
    }
    // collision, similar constraints
    if key == ca(ARC_BOS, ARC_BOS) || key == ca(ARC_EOS, ARC_EOS) {
        if con_co == a_co {
            return SATISFIED; // true duplication
        }
        return INCOMPATIBLE;
    }
    // collision, similar constraints
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
    // collision, dissimilar constraints
    if key == ca(ARC_BOS, BEHIND)
        || key == ca(BEHIND, ARC_BOS)
        || key == ca(ARC_EOS, AHEAD)
        || key == ca(AHEAD, ARC_EOS)
    {
        return INCOMPATIBLE;
    }
    // constraints passing each other
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

// =============================================================================
// empty-arc elimination passes: pullback/pull, pushfwd/push, fixempties
// =============================================================================

/// `pullback(struct nfa *nfa, FILE *f)` — pull constraints back through the NFA.
/// Repeatedly walks every state, attempting to [`pull`] each `^`/BEHIND out-arc
/// backward across its source state, until a full sweep makes no progress.
/// Intermediate states created during one source-state's processing are tracked
/// in a worklist threaded through `State::tmp`. Useless states are dropped here.
pub fn pullback<'mcx>(
    mcx: Mcx<'mcx>,
    nfa: &mut Nfa,
    cm: &mut ColorMap,
    has_parent: bool,
) -> RegResult<()> {
    // find and pull until there are no more
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
            // clear tmp fields of intermediate states created here
            while let Some(im) = intermediates {
                let ns = nfa.st(im).tmp;
                nfa.st_mut(im).tmp = None;
                intermediates = ns;
            }
            // if s is now useless, get rid of it
            if (nfa.st(s).nins == 0 || nfa.st(s).nouts == 0) && nfa.st(s).flag == 0 {
                dropstate(nfa, cm, has_parent, s)?;
            }
            s_opt = nexts;
        }
        if !progress {
            break;
        }
    }

    // Any ^ constraints we were able to pull to the start state can now be
    // replaced by PLAIN arcs referencing the BOS or BOL colors.
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

/// `pull(struct nfa *nfa, struct arc *con, struct state **intermediates)` — pull
/// a back constraint backward past its source state. Returns `true` if
/// successful, `false` if nothing happened. Deletes no pre-existing states (and
/// no outarcs of the constraint's from state other than the given constraint
/// arc), making [`pullback`]'s loops safe.
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
        // can't pull back beyond start
        return Ok(false);
    }
    if nfa.st(from).nins == 0 {
        // unreachable
        freearc(nfa, cm, has_parent, con);
        return Ok(true);
    }

    // First, clone from state if necessary to avoid other outarcs.
    if nfa.st(from).nouts > 1 {
        let s = newstate(mcx, nfa)?;
        copyins(mcx, nfa, cm, has_parent, from, s)?; // duplicate inarcs
        cparc(mcx, nfa, cm, has_parent, con, s, to)?; // move constraint arc
        freearc(nfa, cm, has_parent, con);
        from = s;
        con = nfa.st(from).outs.ok_or(err_assert())?;
    }
    debug_assert_eq!(nfa.st(from).nouts, 1);

    // propagate the constraint into the from state's inarcs
    let mut a_opt = nfa.st(from).ins;
    while let Some(a) = a_opt {
        let nexta = nfa.ar(a).inchain; // SNAPSHOT next before relink
        match combine(nfa, cm, con, a) {
            INCOMPATIBLE => {
                // destroy the arc
                freearc(nfa, cm, has_parent, a);
            }
            SATISFIED => {
                // no action needed
            }
            COMPATIBLE => {
                // swap the two arcs, more or less
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
                // replace arc's color
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

    // remaining inarcs, if any, incorporate the constraint
    moveins(mcx, nfa, cm, has_parent, from, to)?;
    freearc(nfa, cm, has_parent, con);
    // from state is now useless, but we leave it to pullback() to clean up
    Ok(true)
}

/// `pushfwd(struct nfa *nfa, FILE *f)` — push constraints forward through the
/// NFA (mirror image of [`pullback`]).
pub fn pushfwd<'mcx>(
    mcx: Mcx<'mcx>,
    nfa: &mut Nfa,
    cm: &mut ColorMap,
    has_parent: bool,
) -> RegResult<()> {
    // find and push until there are no more
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
            // clear tmp fields of intermediate states created here
            while let Some(im) = intermediates {
                let ns = nfa.st(im).tmp;
                nfa.st_mut(im).tmp = None;
                intermediates = ns;
            }
            // if s is now useless, get rid of it
            if (nfa.st(s).nins == 0 || nfa.st(s).nouts == 0) && nfa.st(s).flag == 0 {
                dropstate(nfa, cm, has_parent, s)?;
            }
            s_opt = nexts;
        }
        if !progress {
            break;
        }
    }

    // Any $ constraints we were able to push to the post state can now be
    // replaced by PLAIN arcs referencing the EOS or EOL colors.
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

/// `push(struct nfa *nfa, struct arc *con, struct state **intermediates)` — push
/// a forward constraint forward past its destination state (mirror of [`pull`]).
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
        // can't push forward beyond end
        return Ok(false);
    }
    if nfa.st(to).nouts == 0 {
        // dead end
        freearc(nfa, cm, has_parent, con);
        return Ok(true);
    }

    // First, clone to state if necessary to avoid other inarcs.
    if nfa.st(to).nins > 1 {
        let s = newstate(mcx, nfa)?;
        copyouts(mcx, nfa, cm, has_parent, to, s)?; // duplicate outarcs
        cparc(mcx, nfa, cm, has_parent, con, from, s)?; // move constraint arc
        freearc(nfa, cm, has_parent, con);
        to = s;
        con = nfa.st(to).ins.ok_or(err_assert())?;
    }
    debug_assert_eq!(nfa.st(to).nins, 1);

    // propagate the constraint into the to state's outarcs
    let mut a_opt = nfa.st(to).outs;
    while let Some(a) = a_opt {
        let nexta = nfa.ar(a).outchain; // SNAPSHOT next before relink
        match combine(nfa, cm, con, a) {
            INCOMPATIBLE => {
                // destroy the arc
                freearc(nfa, cm, has_parent, a);
            }
            SATISFIED => {
                // no action needed
            }
            COMPATIBLE => {
                // swap the two arcs, more or less
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
                // replace arc's color
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

    // remaining outarcs, if any, incorporate the constraint
    moveouts(mcx, nfa, cm, has_parent, to, from)?;
    freearc(nfa, cm, has_parent, con);
    // to state is now useless, but we leave it to pushfwd() to clean up
    Ok(true)
}

/// `fixempties(struct nfa *nfa, FILE *f)` — eliminate EMPTY arcs.
///
/// THE SUBTLEST INVARIANT IN THE ENGINE -- the `inarcsorig` boundary accounting.
/// Only the ORIGINAL in-arcs of a target state are candidates to be pushed
/// forward. We rely on [`newarc`]/[`createarc`] putting new arcs on the FRONT of
/// their to-states' in-chains, and that this phase never deletes arcs, so the
/// original arcs are the LAST arcs in their to-states' in-chains. After
/// [`mergeins`] adds arcs to a state, we recompute the original-arcs boundary by
/// skipping `nskip = nins - prevnins` arcs from the (new) front of the in-chain.
pub fn fixempties<'mcx>(
    mcx: Mcx<'mcx>,
    nfa: &mut Nfa,
    cm: &mut ColorMap,
    has_parent: bool,
) -> RegResult<()> {
    // First, get rid of any states whose sole out-arc is an EMPTY.
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

    // Similarly, get rid of any state with a single EMPTY in-arc.
    let mut s_opt = nfa.live_states;
    while let Some(s) = s_opt {
        let nexts = nfa.st(s).next; // SNAPSHOT next before possible dropstate
                                    // while we're at it, ensure tmp fields are clear for next step
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

    // For each remaining NFA state, find all other states from which it is
    // reachable by a chain of one or more EMPTY arcs, then generate new arcs that
    // eliminate the need for each such chain.
    //
    // Remember the states' first original inarcs (indexed by state->no), and
    // count how many old inarcs there are altogether. The `no` values were
    // assigned by newstate and are < nfa.nstates, and the dropstate passes above
    // do not renumber, so the Vec is sized to nfa.nstates and indexed by `s->no`.
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

    // And iterate over the target states.
    let mut s_opt = nfa.live_states;
    while let Some(s) = s_opt {
        // Ignore target states without non-EMPTY outarcs.
        if nfa.st(s).flag == 0 && !hasnonemptyout(nfa, s) {
            s_opt = nfa.st(s).next;
            continue;
        }

        // Find predecessor states and accumulate their original inarcs. The
        // arcarray cannot exceed totalinarcs entries; reserve that up front.
        let mut arcarray: Vec<ArcId> = Vec::new();
        arcarray.try_reserve(totalinarcs)?;
        let mut s2_opt = Some(emptyreachable(nfa, s, s, &inarcsorig, 0)?);
        while let Some(s2) = s2_opt {
            if s2 == s {
                break;
            }
            // Add s2's original inarcs to arcarray[], but ignore empties.
            let mut a_opt = inarcsorig[nfa.st(s2).no as usize];
            while let Some(a) = a_opt {
                if nfa.ar(a).type_ != EMPTY {
                    arcarray.push(a);
                }
                a_opt = nfa.ar(a).inchain;
            }
            // Reset the tmp fields as we walk back.
            let nexts = nfa.st(s2).tmp;
            nfa.st_mut(s2).tmp = None;
            s2_opt = nexts;
        }
        nfa.st_mut(s).tmp = None;
        debug_assert!(arcarray.len() <= totalinarcs);

        // Remember how many original inarcs this state has.
        let prevnins = nfa.st(s).nins;

        // Add non-duplicate inarcs to target state.
        mergeins(mcx, nfa, cm, has_parent, s, arcarray)?;

        // Now we must update the state's inarcsorig pointer. New arcs are
        // prepended, so skip nskip = (new nins - prevnins) arcs from the front to
        // land on the first ORIGINAL inarc.
        let mut nskip = nfa.st(s).nins - prevnins;
        let mut a = nfa.st(s).ins;
        while nskip > 0 {
            a = nfa.ar(a.unwrap()).inchain;
            nskip -= 1;
        }
        inarcsorig[nfa.st(s).no as usize] = a;

        s_opt = nfa.st(s).next;
    }

    // Now remove all the EMPTY arcs, since we don't need them anymore.
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

    // And remove any states that have become useless.
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

/// `emptyreachable(struct nfa *nfa, struct state *s, struct state *lastfound,
/// struct arc **inarcsorig)` — recursively find all states that can reach s by
/// EMPTY arcs. The return value is the last such state found; its `tmp` field
/// links back to the next-to-last, and so on back to `s`.
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

// =============================================================================
// matchall analysis: checkmatchall / checkmatchall_recurse /
// check_out_colors_match / check_in_colors_match
// =============================================================================

/// `checkmatchall(struct nfa *nfa)` — detect a "matches all strings of a length
/// range" NFA and set min/maxmatchall + MATCHALL.
///
/// SOFT-FAIL CONTRACT: this routine never hard-errors. C uses MALLOC and returns
/// quietly (concluding "not matchall") on allocation trouble, and treats stack
/// overflow inside the recursion as a non-matchall result rather than an error.
/// Requires `cm` to test colordesc PSEUDO flags; leaves all `state.tmp` clean.
pub fn checkmatchall(nfa: &mut Nfa, cm: &ColorMap) {
    // If there are too many states, don't bother trying to detect matchall.
    if nfa.nstates > DUPINF * 2 {
        return;
    }

    // First, scan all the states to verify that only RAINBOW arcs appear, plus
    // pseudocolor arcs adjacent to the pre and post states.
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
                    // Pseudocolor arc: verify it's in a valid place.
                    let ato = nfa.ar(a).to.unwrap();
                    if s == nfa.pre && (co == nfa.bos[0] || co == nfa.bos[1]) {
                        // okay BOS/BOL arc
                    } else if ato == nfa.post && (co == nfa.eos[0] || co == nfa.eos[1]) {
                        // okay EOS/EOL arc
                    } else {
                        return; // unexpected pseudocolor arc
                    }
                } else {
                    return; // any other color makes it non-matchall
                }
            }
            a_opt = nfa.ar(a).outchain;
        }
        // Also, assert that the tmp fields are available for use.
        debug_assert!(nfa.st(s).tmp.is_none());
        s_opt = nfa.st(s).next;
    }

    // Verify that the BOS/BOL outarcs of the pre state reach the same states as
    // its RAINBOW outarcs, and likewise EOS/EOL inarcs of the post state.
    let pre = nfa.pre;
    let post = nfa.post;
    if !check_out_colors_match(nfa, pre, RAINBOW, nfa.bos[0])
        || !check_out_colors_match(nfa, pre, RAINBOW, nfa.bos[1])
        || !check_in_colors_match(nfa, post, RAINBOW, nfa.eos[0])
        || !check_in_colors_match(nfa, post, RAINBOW, nfa.eos[1])
    {
        return;
    }

    // Initialize an array of path-length arrays for memoization.
    let nstates = nfa.nstates as usize;
    let mut haspaths: Vec<Option<Vec<bool>>> = Vec::new();
    if haspaths.try_reserve_exact(nstates).is_err() {
        return; // soft-fail: treat as non-matchall
    }
    haspaths.resize_with(nstates, || None);

    // Recursively search the graph for all-RAINBOW paths to the "post" state.
    if checkmatchall_recurse(nfa, pre, &mut haspaths, 0) {
        // The useful result is the path length array for the pre state.
        let pre_no = nfa.st(pre).no as usize;
        let haspath = haspaths[pre_no]
            .as_ref()
            .expect("checkmatchall: pre haspath must be set on success");

        // Reduce the set of possible path lengths to a min and max value.
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
            // The path length from the pre state includes the pre-to-initial
            // transition, so it's one more than the matched string length. This
            // decrement also converts "DUPINF+1" infinity to our "DUPINF".
            debug_assert!(minmatch > 0); // else pre and post states were adjacent
            nfa.minmatchall = minmatch - 1;
            nfa.maxmatchall = maxmatch - 1;
            nfa.flags |= MATCHALL;
        }
    }
    // haspaths is dropped here (C FREEs each haspaths[i] then haspaths itself).
}

/// `checkmatchall_recurse(struct nfa *nfa, struct state *s, bool **haspaths)` —
/// recursive search for checkmatchall. Returns true if performed successfully,
/// false if we had to fail (multi-state loops or internal reasons). On success
/// stores a result array in `haspaths[s->no]`.
fn checkmatchall_recurse(
    nfa: &mut Nfa,
    s: StateId,
    haspaths: &mut Vec<Option<Vec<bool>>>,
    depth: u32,
) -> bool {
    // Soft-fail (return false) at the recursion depth limit, not REG_ETOOBIG.
    if depth >= MAX_RECURSION_DEPTH {
        return false;
    }

    // In case the search takes a long time, check for cancel.
    check_interrupt();

    // Create a haspath array for this state; soft-fail on alloc trouble.
    let mut haspath: Vec<bool> = Vec::new();
    if haspath.try_reserve_exact((DUPINF + 2) as usize).is_err() {
        return false;
    }
    haspath.resize((DUPINF + 2) as usize, false);

    // Mark this state as being visited.
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
                // soft-fail to non-matchall via the normal break path so
                // tmp/haspaths bookkeeping completes.
                result = false;
                break;
            }
        };
        if ato == nfa.post {
            // We found an all-RAINBOW path to the post state.
            result = true;
            // Mark this state as being zero steps away from the string end.
            haspath[0] = true;
        } else if ato == s {
            // We found a cycle of length 1, which we'll deal with below.
            foundloop = true;
        } else if nfa.st(ato).tmp.is_some() {
            // It's busy, so we found a cycle of length > 1, so fail.
            result = false;
            break;
        } else {
            // Consider paths forward through this to-state.
            let ato_no = nfa.st(ato).no as usize;

            // If to-state was not already visited, recurse.
            if haspaths[ato_no].is_none() {
                result = checkmatchall_recurse(nfa, ato, haspaths, depth + 1);
                // Fail if any recursive path fails.
                if !result {
                    break;
                }
            } else {
                // The previous visit must have found path(s) to the end.
                result = true;
            }
            debug_assert!(nfa.st(ato).tmp.is_none());
            let nexthaspath = haspaths[ato_no]
                .as_ref()
                .expect("checkmatchall_recurse: visited state must have a haspath");

            // For every path of length i from a->to to the end, there is a path of
            // length i + 1 from s to the end.
            if nexthaspath[DUPINF as usize] != nexthaspath[(DUPINF + 1) as usize] {
                // a->to has a path of length exactly DUPINF but not longer, or all
                // lengths > DUPINF but not exactly that. Either way, fail.
                result = false;
                break;
            }
            // Merge knowledge of these path lengths into what we have.
            for i in 0..DUPINF as usize {
                haspath[i + 1] |= nexthaspath[i];
            }
            // Infinity + 1 is still infinity.
            haspath[(DUPINF + 1) as usize] |= nexthaspath[(DUPINF + 1) as usize];
        }
        a_opt = nfa.ar(a).outchain;
    }

    if result && foundloop {
        // If there is a length-1 loop at this state, find the shortest known path
        // length to the end. The loop means every larger length is possible too.
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

    // Report out the completed path length map.
    let s_no = nfa.st(s).no;
    debug_assert!(s_no < nfa.nstates);
    debug_assert!(haspaths[s_no as usize].is_none());
    haspaths[s_no as usize] = Some(haspath);

    // Mark state no longer busy.
    nfa.st_mut(s).tmp = None;

    result
}

/// `check_out_colors_match(struct state *s, color co1, color co2)` — check
/// whether the set of states reachable from `s` by `co1` arcs equals the set
/// reachable by `co2` arcs. Leaves all touched `tmp` fields reset to None.
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

/// `check_in_colors_match(struct state *s, color co1, color co2)` — identical to
/// [`check_out_colors_match`] but over the from-states of `s`'s inarcs.
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

// =============================================================================
// constraint-arc predicates  (regc_nfa.c)
// =============================================================================

/// `isconstraintarc(struct arc *a)` — detect whether an arc is a constraint type.
#[inline]
fn isconstraintarc(nfa: &Nfa, a: ArcId) -> bool {
    let t = nfa.ar(a).type_;
    t == ARC_BOS || t == ARC_EOS || t == BEHIND || t == AHEAD || t == LACON
}

/// `hasconstraintout(struct state *s)` — does state have a constraint out arc?
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

// =============================================================================
// fixconstraintloops / findconstraintloop / breakconstraintloop /
// clonesuccessorstates  (regc_nfa.c)
// =============================================================================

/// `fixconstraintloops(struct nfa *nfa, FILE *f)` — break constraint loops. A
/// loop of states containing only constraint arcs is useless and would cause
/// infinite looping in [`pullback`]/[`pushfwd`], so we get rid of such loops
/// first. The C `restart:`/`goto restart` retry is the outer `'restart` loop.
pub fn fixconstraintloops<'mcx>(
    mcx: Mcx<'mcx>,
    nfa: &mut Nfa,
    cm: &mut ColorMap,
    has_parent: bool,
) -> RegResult<()> {
    // Trivial case: a state that loops to itself can just drop the constraint
    // arc. While we're at it, note whether any constraint arcs survive.
    let mut hasconstraints = false;
    let mut s_opt = nfa.live_states;
    while let Some(s) = s_opt {
        let nexts = nfa.st(s).next; // SNAPSHOT next before possible dropstate
                                    // while we're at it, ensure tmp fields are clear for next step
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
        // If we removed all the outarcs, the state is useless.
        if nfa.st(s).nouts == 0 && nfa.st(s).flag == 0 {
            dropstate(nfa, cm, has_parent, s)?;
        }
        s_opt = nexts;
    }

    // Nothing to do if no remaining constraint arcs.
    if !hasconstraints {
        return Ok(());
    }

    // Starting from each remaining NFA state, search outwards for a constraint
    // loop. If we find a loop, break it, then start the search over.
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

    // Now remove any states that have become useless. Because findconstraintloop
    // intentionally doesn't reset all tmp fields, we clear them after it's done.
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

/// `findconstraintloop(struct nfa *nfa, struct state *s)` — recursively find a
/// loop of constraint arcs; break it via [`breakconstraintloop`] and return
/// true, else false. On success all `tmp` are None; on failure each state known
/// not to be in a loop is marked `s.tmp == Some(s)`.
fn findconstraintloop<'mcx>(
    mcx: Mcx<'mcx>,
    nfa: &mut Nfa,
    cm: &mut ColorMap,
    has_parent: bool,
    s: StateId,
    depth: u32,
) -> RegResult<bool> {
    if depth >= MAX_RECURSION_DEPTH {
        // C: NERR(REG_ETOOBIG); return 1; (to exit as quickly as possible).
        return Err(err_etoobig());
    }

    if let Some(tmp) = nfa.st(s).tmp {
        // Already proven uninteresting?
        if tmp == s {
            return Ok(false);
        }
        // Found a loop involving s.
        breakconstraintloop(mcx, nfa, cm, has_parent, s)?;
        // The tmp fields have been cleaned up by breakconstraintloop.
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

    // No constraint loop leads out from s. Mark it so we need not rediscover.
    nfa.st_mut(s).tmp = Some(s);
    Ok(false)
}

/// `breakconstraintloop(struct nfa *nfa, struct state *sinitial)` — break a loop
/// of constraint arcs. `sinitial` is any one member state; each loop member's
/// `tmp` links to its successor within the loop. Resets all `tmp` to None.
fn breakconstraintloop<'mcx>(
    mcx: Mcx<'mcx>,
    nfa: &mut Nfa,
    cm: &mut ColorMap,
    has_parent: bool,
    sinitial: StateId,
) -> RegResult<()> {
    // Identify which loop step to break at. Preferentially one with only one
    // constraint arc.
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
        // break at the refarc
        shead = nfa.ar(ra).from.ok_or(err_assert())?;
        stail = nfa.ar(ra).to.ok_or(err_assert())?;
        debug_assert_eq!(Some(stail), nfa.st(shead).tmp);
    } else {
        // for lack of a better idea, break after sinitial
        shead = sinitial;
        stail = nfa.st(sinitial).tmp.ok_or(err_assert())?;
    }

    // Reset the tmp fields so we can use them for local storage in
    // clonesuccessorstates.
    let mut s_opt = nfa.live_states;
    while let Some(st) = s_opt {
        nfa.st_mut(st).tmp = None;
        s_opt = nfa.st(st).next;
    }

    // Recursively build clone state(s) as needed.
    let new_sc = newstate(mcx, nfa)?;
    let mut sclone: Option<StateId> = Some(new_sc);

    let nstates = nfa.nstates;
    clonesuccessorstates(
        mcx, nfa, cm, has_parent, stail, new_sc, shead, refarc, None, None, nstates, 0,
    )?;

    // It's possible that sclone has no outarcs at all, in which case it's
    // useless.
    if nfa.st(new_sc).nouts == 0 {
        freestate(nfa, new_sc);
        sclone = None;
    }

    // Move shead's constraint-loop arcs to point to sclone, or just drop them if
    // we discovered we don't need sclone.
    //
    // PORT NOTE: C runs cparc THEN freearc(a) THEN `if (NISERR()) break;`, so even
    // when cparc fails, arc `a` is still freed before bailing out. Mirror that
    // statement order: capture the cparc Result, always run freearc(a), then `?`.
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

/// `clonesuccessorstates(...)` — create a tree of constraint-arc successor
/// states.
///
/// CRITICAL invariants (see the C source comment):
/// * `donemap` is sized to `nstates` == `nfa.nstates` AT THE START of the
///   recursion. Clone states created during recursion have `no >= nstates` and
///   thus NO donemap slot; the `no < nstates` guards are LOAD-BEARING.
/// * `state.tmp` is multiplexed as a clone-back-pointer: a child clone state's
///   `tmp` points to the original state it was cloned from.
///
/// PORT NOTE on the donemap threading: the per-clone donemap is an owned
/// `Vec<u8>`. `curdonemap` is `Some(..)` only in the merge recursion (shares and
/// mutates the parent frame's map); `None` means "allocate a fresh map".
/// `outerdonemap` is read-only and copied into a freshly allocated map.
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
            // Not at the outer level for this clone state: reuse the caller's map
            // (the merge recursion). No fresh allocation, no child-recursion pass.
            clonesuccessorstates_fill(
                mcx, nfa, cm, has_parent, ssource, sclone, spredecessor, refarc, dm, outerdonemap,
                nstates, depth,
            )
        }
        None => {
            // Outer level for this clone state: allocate a fresh donemap.
            let mut donemap: Vec<u8> = Vec::new();
            donemap.try_reserve_exact(nstates as usize)?;
            if let Some(outer) = outerdonemap {
                // Not at outermost recursion level, so copy the outer level's map;
                // this ensures we see states in process of being visited at outer
                // levels, or already merged, as ones we shouldn't traverse back to.
                debug_assert_eq!(outer.len(), nstates as usize);
                donemap.extend_from_slice(outer);
            } else {
                // At outermost level, only spredecessor is off-limits.
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

            // If we are at outer level for this clone state, recurse to all its
            // child clone states, clearing their tmp fields as we go.
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
            // sclone's donemap (the owned Vec) is dropped here.
            Ok(())
        }
    }
}

/// Inner worker for [`clonesuccessorstates`]: mark `ssource` as visited in
/// `donemap`, then clone all of `ssource`'s outarcs into `sclone`, creating new
/// clone states (with `tmp` back-pointers) as needed but not yet recursing.
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
    // Mark ssource as visited in the donemap.
    debug_assert!((nfa.st(ssource).no as i64) < nstates as i64);
    debug_assert_eq!(donemap[nfa.st(ssource).no as usize], 0);
    donemap[nfa.st(ssource).no as usize] = 1;

    // First clone all of ssource's outarcs, creating new clone states as needed
    // but not doing more with them. The caller recurses into the child clones.
    let mut a_opt = nfa.st(ssource).outs;
    while let Some(a) = a_opt {
        let nexta = nfa.ar(a).outchain;
        let sto = nfa.ar(a).to.ok_or(err_assert())?;

        // We do not consider cloning successor states that have no constraint
        // outarcs; just link to them as-is. This keeps us from cloning the post
        // state, which would be a bad idea.
        if isconstraintarc(nfa, a) && hasconstraintout(nfa, sto) {
            // Back-link constraint arcs must not be followed. Nor revisit states
            // previously merged into this clone.
            debug_assert!((nfa.st(sto).no as i64) < nstates as i64);
            if donemap[nfa.st(sto).no as usize] != 0 {
                a_opt = nexta;
                continue;
            }

            // Check whether we already have a child clone state for this source.
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

            // If this arc is labeled the same as refarc, or as any arc we must
            // have traversed to get to sclone, then no additional constraints need
            // be met to get to sto, so just merge its outarcs into sclone.
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
                // We can merge into sclone. If we previously made a child clone
                // state, drop it; there's no need to visit it.
                if let Some(pc) = prevclone {
                    dropstate(nfa, cm, has_parent, pc)?; // kills our outarc, too
                }

                // Recurse to merge sto's outarcs into sclone.
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
                // sto should now be marked as previously visited.
                debug_assert_eq!(donemap[nfa.st(sto).no as usize], 1);
            } else if let Some(pc) = prevclone {
                // We already have a clone state for this successor, so just make
                // another arc to it.
                cparc(mcx, nfa, cm, has_parent, a, sclone, pc)?;
            } else {
                // We need to create a new successor clone state.
                let stoclone = newstate(mcx, nfa)?;
                // Mark it as to what it's a clone of.
                nfa.st_mut(stoclone).tmp = Some(sto);
                // ... and add the outarc leading to it.
                cparc(mcx, nfa, cm, has_parent, a, sclone, stoclone)?;
            }
        } else {
            // Non-constraint outarcs just get copied to sclone, as do outarcs
            // leading to states with no constraint outarc.
            cparc(mcx, nfa, cm, has_parent, a, sclone, sto)?;
        }

        a_opt = nexta;
    }
    Ok(())
}

/// Walk `sclone`'s inarcs back to the root, testing the C `canmerge` condition:
/// any single-inarc ancestor whose lone inarc is labeled `(a_type, a_co)` means
/// the constraint is already known valid, so we can merge.
///
/// Mirrors:
/// ```c
/// for (s = sclone; s->ins; s = s->ins->from)
///     if (s->nins == 1 && a->type == s->ins->type && a->co == s->ins->co)
///         { canmerge = 1; break; }
/// ```
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

// =============================================================================
// optimize / analyze  (regc_nfa.c)
// =============================================================================

/// `optimize(struct nfa *nfa, FILE *f)` — top-level NFA optimization driver.
///
/// Reduces the NFA to a form the executor can handle (PLAIN/LACON arcs only),
/// getting rid of EMPTY, ^, $, AHEAD, BEHIND arcs. The C call ORDER is mirrored
/// exactly: removecantmatch (if HASCANTMATCH) -> cleanup -> fixempties ->
/// fixconstraintloops -> pullback -> pushfwd -> cleanup -> analyze. Returns the
/// `re_info` bits from [`analyze`].
pub fn optimize<'mcx>(
    mcx: Mcx<'mcx>,
    nfa: &mut Nfa,
    cm: &mut ColorMap,
    has_parent: bool,
) -> RegResult<i64> {
    // If we have any CANTMATCH arcs, drop them; but this is uncommon.
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

/// `analyze(struct nfa *nfa)` — set the `re_info` bits the NFA shape implies
/// (REG_UIMPOSSIBLE / REG_UEMPTYMATCH), running [`checkmatchall`].
pub fn analyze(nfa: &mut Nfa, cm: &mut ColorMap) -> RegResult<i64> {
    // C: if (NISERR()) return 0; -- errors are already propagated by `?`.

    // Detect whether NFA can't match anything.
    if nfa.st(nfa.pre).outs.is_none() {
        return Ok(REG_UIMPOSSIBLE as i64);
    }

    // Detect whether NFA matches all strings (possibly with length bounds).
    checkmatchall(nfa, cm);

    // Detect whether NFA can possibly match a zero-length string.
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

// =============================================================================
// compact / carcsort / carc_cmp / freecnfa  (NFA -> Cnfa lowering)
// =============================================================================

/// `compact(struct nfa *nfa, struct cnfa *cnfa)` — build the compacted NFA.
///
/// LAYOUT CONVENTION: `cnfa.arcs` is a single flat arena holding every state's
/// out-arc list back to back, laid out in *live-state iteration order*. Each
/// state's list is its real out-arcs (PLAIN/LACON, in `carcsort` (co, to) order)
/// followed by one terminator `Carc { co: COLORLESS, to: 0 }`. EMPTY/constraint
/// arcs were removed by `optimize` before this runs, so only PLAIN and LACON
/// arcs are emitted. `cnfa.states[s.no]` is the HALF-OPEN range `start..end` of
/// state `s.no`'s real arcs; `arcs[end]` is that state's terminator.
///
/// Arc encoding: PLAIN -> `Carc { co: a.co, to: a.to.no }`. LACON ->
/// `Carc { co: ncolors + a.co, to: a.to.no }` (so `co >= ncolors`), and the
/// first LACON sets HASLACONS. `ncolors = maxcolor(cm) + 1`. CNFA_NOPROGRESS is
/// set on `pre` and on every target of a pre-out arc. `maxcolor` is
/// foundation-owned.
pub fn compact<'mcx>(mcx: Mcx<'mcx>, nfa: &Nfa, cm: &ColorMap, cnfa: &mut Cnfa) -> RegResult<()> {
    let _ = mcx; // arena is plain Vec at this stage; mcx threaded for parity
    // Pass 1: count states and total arcs (one extra per state for the endmarker).
    let mut nstates: usize = 0;
    let mut narcs: usize = 0;
    let mut s_opt = nfa.live_states;
    while let Some(s) = s_opt {
        nstates += 1;
        // narcs += s->nouts + 1; guard against usize overflow (REG_ESPACE).
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

    // stflags[s.no] and states[s.no] are indexed by state number, so pre-fill to
    // length nstates; the loop assigns each by s.no.
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

    // Pass 2: lay out the arc runs in live-state iteration order. `arcs.len()` IS
    // the C cursor `ca` into cnfa->arcs (we push in order).
    let mut s_opt = nfa.live_states;
    while let Some(s) = s_opt {
        let s_no = nfa.st(s).no;
        debug_assert!((s_no as usize) < nstates);
        stflags[s_no as usize] = 0;
        // `first` = cursor at the start of this state's run (C: first = ca).
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
        // Sort this state's real-arc run by (co, to). The run is arcs[first..len].
        let end = arcs.len();
        carcsort(&mut arcs[first..end]);
        // Record the half-open range of REAL arcs; the terminator goes at `end`.
        states[s_no as usize] = first..end;
        // Append the COLORLESS terminator carc (C: ca->co = COLORLESS; ca->to = 0).
        arcs.push(Carc {
            co: COLORLESS,
            to: 0,
        });
        s_opt = nfa.st(s).next;
    }
    debug_assert_eq!(arcs.len(), narcs);
    debug_assert_ne!(cnfa.nstates, 0);

    // Mark no-progress states: every target of a pre-out arc, plus pre itself.
    let mut a_opt = nfa.st(nfa.pre).outs;
    while let Some(a) = a_opt {
        let to_no = nfa.st(nfa.ar(a).to.unwrap()).no;
        stflags[to_no as usize] = CNFA_NOPROGRESS;
        a_opt = nfa.ar(a).outchain;
    }
    stflags[nfa.st(nfa.pre).no as usize] = CNFA_NOPROGRESS;

    // Replace the cnfa's (empty) arenas with the freshly built ones.
    cnfa.stflags = stflags;
    cnfa.states = states;
    cnfa.arcs = arcs;
    Ok(())
}

/// `carcsort(struct carc *first, size_t n)` — sort compacted-NFA arcs by color.
/// C calls qsort only when `n > 1`; the caller passes the state's real-arc run
/// (excluding the terminator), sorted by (co, to) via [`carc_cmp`].
fn carcsort(run: &mut [Carc]) {
    if run.len() > 1 {
        run.sort_unstable_by(carc_cmp);
    }
}

/// `carc_cmp(const void *a, const void *b)` — order two compacted-NFA arcs by
/// (co, to). The final Equal arm is unreached (no duplicate arcs by this point).
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
    // Unreached: there should be no duplicate arcs now.
    Ordering::Equal
}

/// `freecnfa(struct cnfa *cnfa)` — free a compacted NFA's storage. C asserts the
/// cnfa is not already empty, FREEs storage, then ZAPCNFA (nstates = 0). The
/// owned `Vec`s are dropped and `nstates` reset to 0 (NULLCNFA/empty state).
pub fn freecnfa(cnfa: &mut Cnfa) {
    debug_assert_ne!(cnfa.nstates, 0); // not empty already (C: assert(!NULLCNFA))
    cnfa.stflags = Vec::new();
    cnfa.states = Vec::new();
    cnfa.arcs = Vec::new();
    cnfa.nstates = 0; // ZAPCNFA semantics (production: nstates = 0)
}

// =============================================================================
// HASCANTMATCH helper (used by colorcomplement)
// =============================================================================

/// Set the HASCANTMATCH flag on an NFA (used by `colorcomplement`).
#[inline]
pub fn set_hascantmatch(nfa: &mut Nfa) {
    nfa.flags |= HASCANTMATCH;
}

// =============================================================================
// getcolor re-export  (regc_color.c — NFA-side color lookup)
// =============================================================================

/// `pg_reg_getcolor`'s high-side helper is shared from the foundation family;
/// the simple-chr fast path (`GETCOLOR`) is inlined by callers. Re-exported here
/// for NFA-side callers that look colors up while building arcs. Routes to the
/// [`crate::regex_foundation`] owner.
pub fn getcolor(cm: &ColorMap, c: chr) -> color {
    crate::regex_foundation::pg_reg_getcolor(cm, c)
}
