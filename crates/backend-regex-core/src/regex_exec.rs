//! Family: **regex-exec** — `regexec.c` + `rege_dfa.c` (the lazy-DFA matcher)
//! and `regprefix.c` (the fixed-prefix extractor).
//!
//! These read the compiled structures ([`Cnfa`]/[`ColorMap`]/[`Guts`]/
//! [`Subre`]) read-only; they are node-independent. The matcher is a lazy DFA
//! built on demand from the compacted NFA, with the recursive dissectors
//! splitting a match across the subexpression tree to fill the caller's
//! `pmatch` array.
//!
//! Faithful port of `src/backend/regex/regexec.c`, the DFA cache machinery in
//! `rege_dfa.c` (which is `#include`d into regexec.c), and `regprefix.c`.
//!
//! # The two big design decisions (inherited from the engine port)
//!
//! 1. **Pointers -> arena indices.** The C executor builds a self-referential
//!    raw-pointer graph (`sset.outs`/`inchain`/`ins`, `arcp.ss` all point into
//!    shared `dfa` arrays). This port replaces every such pointer with an
//!    INDEX-based reference: `Option<usize>` indices into [`Dfa::ssets`] and the
//!    work arenas. Input positions are `usize` indices into the input `&[chr]`,
//!    with `Option<usize>` standing in for the C `NULL` ("no position").
//!
//! 2. **`g: &Guts` is a SEPARATE parameter, not held in [`ExecVars`].** The
//!    compiled types own arenas, so the read-only compiled regex `g: &Guts` is
//!    threaded as its own borrow alongside `v: &mut ExecVars` (which holds only
//!    the *mutable* exec state). Then `&g.tree_nodes[id].cnfa` (a borrow of
//!    `*g`) and `&mut v.subdfas` are provably disjoint, so no clone is needed.
//!
//! # Memory model
//!
//! Following the family convention (cf. `regex_foundation`), the growable work
//! areas are plain [`alloc::vec::Vec`]s; data-derived growth uses `try_reserve`
//! (surfacing `REG_ESPACE` on failure, mirroring C's `MALLOC == NULL`). The
//! allocating entry points carry `Mcx<'mcx>` per the project contract.
//!
//! Error channel: `i32` `REG_*` codes via [`crate::regex_error`]
//! (`RegError`/`RegResult`), never the backend `PgError`/panic.

extern crate alloc;

use alloc::vec::Vec;

use mcx::Mcx;

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
use types_regex::{pg_regoff_t, RegMatch};

/// Default recursion-depth cap for the `STACK_TOO_DEEP` analogue used by
/// [`lacon`] and [`cdissect`]. C uses the backend's `max_stack_depth` GUC via
/// `re->re_fns->stack_too_deep`; this fixed cap keeps the matcher self-contained
/// while still bounding the `lacon -> shortest -> miss -> lacon` and the
/// `cdissect` recursion to a `REG_ETOOBIG` failure rather than an actual stack
/// overflow.
pub const DEFAULT_MAX_DEPTH: u32 = 10_000;

// =============================================================================
// bitmap manipulation  (regguts.h: UBITS / BSET / ISBSET)
// =============================================================================

/// `UBITS` -- bits in one `unsigned` word of a state bitvector (C `unsigned` is
/// the `u32` used for `statesarea`, so 32).
pub const UBITS: usize = 32;

/// `BSET(uv, sn)` -- set bit `sn` in the bitvector slice `uv`.
#[inline]
pub fn bset(uv: &mut [u32], sn: usize) {
    uv[sn / UBITS] |= 1u32 << (sn % UBITS);
}

/// `ISBSET(uv, sn)` -- is bit `sn` set in the bitvector slice `uv`?
#[inline]
pub fn isbset(uv: &[u32], sn: usize) -> bool {
    (uv[sn / UBITS] & (1u32 << (sn % UBITS))) != 0
}

// =============================================================================
// sset flags  (regexec.c)
// =============================================================================

/// sset flag: the initial state set (regexec.c: STARTER = 01).
pub const STARTER: i32 = 0o01;
/// sset flag: includes the goal state (regexec.c: POSTSTATE = 02).
pub const POSTSTATE: i32 = 0o02;
/// sset flag: locked in cache (regexec.c: LOCKED = 04).
pub const LOCKED: i32 = 0o04;
/// sset flag: zero-progress state set (regexec.c: NOPROGRESS = 010).
pub const NOPROGRESS: i32 = 0o010;

// =============================================================================
// non-malloc allocation sizing  (regexec.c)
// =============================================================================

/// `WORK` -- number of work bitvectors needed (regexec.c: WORK = 1).
pub const WORK: usize = 1;

/// `d->nssets` cap for `REG_SMALL` (rege_dfa.c newdfa: 7 entries).
pub const REG_SMALL_NSSETS: usize = 7;

// =============================================================================
// lazy-DFA representation  (regexec.c: struct arcp / struct sset / struct dfa)
// =============================================================================

/// `struct arcp` -- a "pointer" to an outarc.
///
/// In C this is `{ struct sset *ss; color co; }`. Here `ss` becomes an
/// `Option<usize>` index into [`Dfa::ssets`] (`None` == C `NULL`).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Arcp {
    /// index of the source stateset (C: `struct sset *ss`); `None` == NULL.
    pub ss: Option<usize>,
    /// color of the outarc.
    pub co: color,
}

impl Arcp {
    /// A null arcp: `{ ss: None, co: WHITE }` (C `{NULL, 0}`, WHITE == 0).
    #[inline]
    pub const fn null() -> Self {
        Arcp { ss: None, co: WHITE }
    }
}

/// `struct sset` -- one state set (a cache entry).
///
/// The C struct's raw pointers into shared `dfa` arrays become INDEX bases into
/// the `Dfa`-level arenas. `lastseen` (C `chr *`) becomes an `Option<usize>`
/// input position (`None` == NULL).
#[derive(Copy, Clone, Debug)]
pub struct Sset {
    /// base index of this sset's bitvector in [`Dfa::statesarea`].
    pub states_base: usize,
    /// hash of the bitvector (C: `unsigned hash`).
    pub hash: u32,
    /// bitmask of STARTER/POSTSTATE/LOCKED/NOPROGRESS (C: `int flags`).
    pub flags: i32,
    /// chain of inarcs pointing here (C: `struct arcp ins`).
    pub ins: Arcp,
    /// last position entered on arrival here (C: `chr *lastseen`); `None` == NULL.
    pub lastseen: Option<usize>,
    /// base index of this sset's outarc vector in [`Dfa::outs`].
    pub outs_base: usize,
    /// base index of this sset's inchain vector in [`Dfa::incarea`].
    pub inchain_base: usize,
}

impl Sset {
    /// A blank, unwired sset shell. Real bases are assigned by `pickss`.
    #[inline]
    const fn blank() -> Self {
        Sset {
            states_base: 0,
            hash: 0,
            flags: 0,
            ins: Arcp::null(),
            lastseen: None,
            outs_base: 0,
            inchain_base: 0,
        }
    }
}

/// `struct dfa` -- the lazy-DFA cache.
///
/// The C raw-pointer arrays (`ssets`, `statesarea`/`work`, `outsarea`,
/// `incarea`) become owned `Vec` arenas; `work` is a separate `Vec` of length
/// `wordsper`. `search` (C `struct sset *`) becomes a `usize` index into
/// `ssets`. `lastpost`/`lastnopr` (C `chr *`) become `Option<usize>` positions.
/// The C allocator bookkeeping (`smalldfa`/`DOMALLOC`) is dropped: the arenas
/// are sized once in [`newdfa`] and dropped at end of scope (C `freedfa`).
pub struct Dfa {
    /// size of cache (C: `int nssets`).
    pub nssets: usize,
    /// how many entries occupied yet (C: `int nssused`).
    pub nssused: usize,
    /// number of NFA states (C: `int nstates`).
    pub nstates: usize,
    /// length of outarc and inchain vectors (C: `int ncolors`).
    pub ncolors: usize,
    /// length of state-set bitvectors, in words (C: `int wordsper`).
    pub wordsper: usize,
    /// state-set cache (C: `struct sset *ssets`).
    pub ssets: Vec<Sset>,
    /// bitvector storage backing every sset's `states` (C: `unsigned *statesarea`).
    pub statesarea: Vec<u32>,
    /// scratch work bitvector of length `wordsper` (C: `unsigned *work`).
    pub work: Vec<u32>,
    /// outarc-vector storage backing every sset's `outs` (C: `struct sset **outsarea`).
    pub outs: Vec<Option<usize>>,
    /// inchain storage backing every sset's `inchain` (C: `struct arcp *incarea`).
    pub incarea: Vec<Arcp>,
    /// location of last cache-flushed success (C: `chr *lastpost`); `None` == NULL.
    pub lastpost: Option<usize>,
    /// location of last cache-flushed NOPROGRESS (C: `chr *lastnopr`); `None` == NULL.
    pub lastnopr: Option<usize>,
    /// replacement-search rotation cursor, index into `ssets` (C: `struct sset *search`).
    pub search: usize,
    /// if DFA for a backref, subno it refers to (C: `int backno`); -1 if not.
    pub backno: i32,
    /// min repetitions for backref (C: `short backmin`).
    pub backmin: i16,
    /// max repetitions for backref (C: `short backmax`).
    pub backmax: i16,
}

impl Dfa {
    /// `HASH(bv, nw)` macro: `(nw == 1) ? *bv : hash(bv, nw)`.
    #[inline]
    fn hashbits(&self, bv: &[u32]) -> u32 {
        if self.wordsper == 1 {
            bv[0]
        } else {
            hash(bv, self.wordsper)
        }
    }

    /// Borrow sset `i`'s bitvector slice (`states[i*wordsper .. +wordsper]`).
    #[inline]
    fn sset_states(&self, i: usize) -> &[u32] {
        let base = self.ssets[i].states_base;
        &self.statesarea[base..base + self.wordsper]
    }

    /// Mutably borrow sset `i`'s bitvector slice.
    #[inline]
    fn sset_states_mut(&mut self, i: usize) -> &mut [u32] {
        let base = self.ssets[i].states_base;
        &mut self.statesarea[base..base + self.wordsper]
    }

    /// `css->outs[co]` -- the cached outarc target sset for color `co`, or `None`.
    #[inline]
    fn out(&self, css: usize, co: color) -> Option<usize> {
        self.outs[self.ssets[css].outs_base + co as usize]
    }
}

// =============================================================================
// the exec-time context  (regexec.c: struct vars)
// =============================================================================

/// `struct vars` -- the exec-time MUTABLE context, bundled for easy passing.
///
/// Renamed `ExecVars` to avoid clashing with the compile-time vars. Does NOT
/// hold the compiled regex `g`: the read-only `g: &Guts` is threaded as a
/// separate borrow into every engine function (see the module-level design
/// note). The per-DFA `cnfa`/`cm` are passed explicitly rather than stored.
///
/// Positions are `usize` indices into `input`; `start` is normally 0 and
/// positions are already offsets, but it is kept explicit to preserve the exact
/// `cp == v->start` / `cp == v->stop` boundary tests.
pub struct ExecVars<'a> {
    /// copy of the `flags` argument (C: `int eflags`).
    pub eflags: i32,
    /// size of the `pmatch` array (C: `size_t nmatch`).
    pub nmatch: usize,
    /// the match-detail output work area (C: `regmatch_t *pmatch`), modeled as a
    /// borrowed `(rm_so, rm_eo)` slice; populated by the dissectors.
    pub pmatch: &'a mut [(isize, isize)],
    /// the input string (C: `chr *start` is `&input[0]`).
    pub input: &'a [chr],
    /// start of string position (C: `chr *start`); normally 0.
    pub start: usize,
    /// search start position (C: `chr *search_start`).
    pub search_start: usize,
    /// just-past-end position (C: `chr *stop`), i.e. `input.len()`.
    pub stop: usize,
    /// recursion-depth counter for the `STACK_TOO_DEEP` guard.
    pub depth: u32,
    /// per-tree-subre DFAs (C: `struct dfa **subdfas`). Indexed by `subre.id`;
    /// `None` == NULL. Built lazily by [`getsubdfa`].
    pub subdfas: Vec<Option<Dfa>>,
    /// per-lacon-subre DFAs (C: `struct dfa **ladfas`). Indexed by lacon number.
    pub ladfas: Vec<Option<Dfa>>,
    /// per-lacon-subre lookbehind restart state (C: `struct sset **lblastcss`).
    pub lblastcss: Vec<Option<usize>>,
    /// per-lacon-subre lookbehind restart position (C: `chr **lblastcp`).
    pub lblastcp: Vec<Option<usize>>,
    /// recursion-depth cap mirroring C's `STACK_TOO_DEEP`.
    pub max_depth: u32,
}

// =============================================================================
// newdfa  (rege_dfa.c)
// =============================================================================

/// newdfa - set up a fresh DFA.
///
/// C signature: `static struct dfa *newdfa(struct vars *v, struct cnfa *cnfa,
/// struct colormap *cm, struct smalldfa *sml)`.
///
/// The C function either carves the DFA out of a preallocated `smalldfa` or
/// `MALLOC`s the four subsidiary arrays, hard-failing with `REG_ESPACE`. This
/// port drops the `smalldfa`/malloc distinction: it always allocates the `Vec`
/// arenas sized to the cnfa, using `try_reserve`/`resize` so a too-large
/// request maps to the same `REG_ESPACE`. `nss == cnfa.nstates * 2` and
/// `wordsper == ceil(nstates/UBITS)` exactly as in C. C `assert(cnfa != NULL &&
/// cnfa->nstates != 0)`.
pub fn newdfa<'mcx>(_mcx: Mcx<'mcx>, eflags: i32, cnfa: &Cnfa) -> RegResult<Dfa> {
    debug_assert!(cnfa.nstates != 0);

    let nss: usize = (cnfa.nstates as usize)
        .checked_mul(2)
        .ok_or(RegError(REG_ESPACE))?;
    let wordsper: usize = (cnfa.nstates as usize).div_ceil(UBITS);
    let ncolors: usize = cnfa.ncolors as usize;

    // Allocate the four subsidiary arenas, sized exactly as the malloc'd C
    // arrays: ssets[nss], statesarea[(nss + WORK) * wordsper], outsarea[nss *
    // ncolors], incarea[nss * ncolors]. `work` is a separate Vec of length
    // wordsper (in C it aliases the tail of statesarea).
    let statesarea_words = nss
        .checked_add(WORK)
        .and_then(|n| n.checked_mul(wordsper))
        .ok_or(RegError(REG_ESPACE))?;
    let vec_area = nss.checked_mul(ncolors).ok_or(RegError(REG_ESPACE))?;

    let ssets = fill_vec(nss, Sset::blank())?;
    let statesarea = fill_vec(statesarea_words, 0u32)?;
    let work = fill_vec(wordsper, 0u32)?;
    let outs = fill_vec::<Option<usize>>(vec_area, None)?;
    let incarea = fill_vec(vec_area, Arcp::null())?;

    let nssets = if (eflags & REG_SMALL) != 0 {
        REG_SMALL_NSSETS
    } else {
        nss
    };

    Ok(Dfa {
        nssets,
        nssused: 0,
        nstates: cnfa.nstates as usize,
        ncolors,
        wordsper,
        ssets,
        statesarea,
        work,
        outs,
        incarea,
        lastpost: None,
        lastnopr: None,
        search: 0,  // d->search = d->ssets (first entry)
        backno: -1, // may be set by caller
        backmin: 0,
        backmax: 0,
    })
}

/// Allocate a `Vec` of `n` copies of `val` via `try_reserve` (C's
/// `MALLOC(n * sizeof) ; memset/init`). A `try_reserve` failure surfaces
/// `REG_ESPACE` (C's `MALLOC == NULL`).
fn fill_vec<T: Copy>(n: usize, val: T) -> RegResult<Vec<T>> {
    let mut v: Vec<T> = Vec::new();
    v.try_reserve_exact(n)?;
    v.resize(n, val);
    Ok(v)
}

// =============================================================================
// hash  (rege_dfa.c)
// =============================================================================

/// hash - construct a hash code for a bitvector.
///
/// C: `static unsigned hash(unsigned *uv, int n)`. XORs the `n` words. The
/// `nw == 1` fast path is the caller's responsibility (see [`Dfa::hashbits`]).
pub fn hash(uv: &[u32], n: usize) -> u32 {
    uv[..n].iter().fold(0u32, |h, &w| h ^ w)
}

// =============================================================================
// initialize  (rege_dfa.c)
// =============================================================================

/// initialize - hand-craft a cache entry for startup, otherwise get ready.
///
/// C: `static struct sset *initialize(struct vars *v, struct dfa *d, chr *start)`.
///
/// Returns the index of the STARTER sset (`Ok(Some(i))`), or `Ok(None)` on the
/// no-match path (C `NULL` from `getvacant`). A real failure surfaces as `Err`.
pub fn initialize(d: &mut Dfa, cnfa: &Cnfa, start: usize) -> RegResult<Option<usize>> {
    // is previous one still there?
    let ss: usize = if d.nssused > 0 && (d.ssets[0].flags & STARTER) != 0 {
        0
    } else {
        // no, must (re)build it
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
        // lastseen dealt with below
        ss
    };

    for i in 0..d.nssused {
        d.ssets[i].lastseen = None;
    }
    d.ssets[ss].lastseen = Some(start); // maybe untrue, but harmless
    d.lastpost = None;
    d.lastnopr = None;
    Ok(Some(ss))
}

// =============================================================================
// getvacant  (rege_dfa.c)  -- the trickiest: index-based chain splice
// =============================================================================

/// getvacant - get a vacant state set.
///
/// C: `static struct sset *getvacant(struct vars *v, struct dfa *d, chr *cp,
/// chr *start)`. Picks a slot via [`pickss`], then unwires it from the cache's
/// in/out arc chains so it can be reused. Reproduced in INDEX form (every C
/// `struct sset *`/`arcp.ss` is an `Option<usize>` into `d.ssets`).
pub fn getvacant(d: &mut Dfa, cp: usize, start: usize) -> RegResult<Option<usize>> {
    let ss = match pickss(d, cp, start)? {
        Some(ss) => ss,
        None => return Ok(None),
    };
    debug_assert!((d.ssets[ss].flags & LOCKED) == 0);

    // clear out its inarcs, including self-referential ones
    let mut ap: Arcp = d.ssets[ss].ins;
    while let Some(p) = ap.ss {
        let co = ap.co;
        let co_idx = co as usize;
        // p->outs[co] = NULL;
        d.outs[d.ssets[p].outs_base + co_idx] = None;
        // ap = p->inchain[co];
        ap = d.incarea[d.ssets[p].inchain_base + co_idx];
        // p->inchain[co].ss = NULL;  (paranoia)
        d.incarea[d.ssets[p].inchain_base + co_idx].ss = None;
    }
    // ss->ins.ss = NULL;
    d.ssets[ss].ins.ss = None;

    // take it off the inarc chains of the ssets reached by its outarcs
    for i in 0..d.ncolors {
        // p = ss->outs[i];
        let p = match d.outs[d.ssets[ss].outs_base + i] {
            Some(p) => p,
            None => continue, // NOTE CONTINUE
        };
        debug_assert!(p != ss); // not self-referential

        // if (p->ins.ss == ss && p->ins.co == i)
        if d.ssets[p].ins.ss == Some(ss) && d.ssets[p].ins.co as usize == i {
            // p->ins = ss->inchain[i];
            d.ssets[p].ins = d.incarea[d.ssets[ss].inchain_base + i];
        } else {
            // struct arcp lastap = {NULL, 0};
            let mut lastap: Arcp = Arcp::null();
            debug_assert!(d.ssets[p].ins.ss.is_some());
            // for (ap = p->ins; ap.ss != NULL && !(ap.ss == ss && ap.co == i);
            //      ap = ap.ss->inchain[ap.co])  lastap = ap;
            let mut ap = d.ssets[p].ins;
            while let Some(ap_ss) = ap.ss {
                if ap_ss == ss && ap.co as usize == i {
                    break;
                }
                lastap = ap;
                ap = d.incarea[d.ssets[ap_ss].inchain_base + ap.co as usize];
            }
            debug_assert!(ap.ss.is_some());
            // lastap.ss->inchain[lastap.co] = ss->inchain[i];
            // C asserts lastap.ss != NULL here; on a violated invariant route
            // through the REG_ASSERT error channel rather than panicking.
            let lastap_ss = match lastap.ss {
                Some(s) => s,
                None => return Err(RegError(REG_ASSERT)),
            };
            let val = d.incarea[d.ssets[ss].inchain_base + i];
            d.incarea[d.ssets[lastap_ss].inchain_base + lastap.co as usize] = val;
        }
        // ss->outs[i] = NULL;
        d.outs[d.ssets[ss].outs_base + i] = None;
        // ss->inchain[i].ss = NULL;
        d.incarea[d.ssets[ss].inchain_base + i].ss = None;
    }

    // if ss was a success state, may need to remember location.
    // The Option<usize> ordering reproduces C's `d->lastpost == NULL ||
    // d->lastpost < ss->lastseen`: `None` sorts below every `Some`.
    if (d.ssets[ss].flags & POSTSTATE) != 0
        && d.ssets[ss].lastseen != d.lastpost
        && (d.lastpost.is_none() || d.lastpost < d.ssets[ss].lastseen)
    {
        d.lastpost = d.ssets[ss].lastseen;
    }

    // likewise for a no-progress state
    if (d.ssets[ss].flags & NOPROGRESS) != 0
        && d.ssets[ss].lastseen != d.lastnopr
        && (d.lastnopr.is_none() || d.lastnopr < d.ssets[ss].lastseen)
    {
        d.lastnopr = d.ssets[ss].lastseen;
    }

    Ok(Some(ss))
}

// =============================================================================
// pickss  (rege_dfa.c)
// =============================================================================

/// pickss - pick the next stateset to be used.
///
/// C: `static struct sset *pickss(struct vars *v, struct dfa *d, chr *cp,
/// chr *start)`. The `d.search` rotation cursor and the exact `ancient`
/// arithmetic are preserved bit-for-bit. `None` sorts below every `Some`,
/// matching C `lastseen == NULL || lastseen < ancient`.
pub fn pickss(d: &mut Dfa, cp: usize, start: usize) -> RegResult<Option<usize>> {
    // cp is always at or past start in every caller.
    debug_assert!(cp >= start);

    // shortcut for cases where cache isn't full
    if d.nssused < d.nssets {
        let i = d.nssused;
        d.nssused += 1;
        // set up innards: assign this slot's index bases into the arenas.
        let states_base = i * d.wordsper;
        let outs_base = i * d.ncolors;
        let inchain_base = i * d.ncolors;
        {
            let ss = &mut d.ssets[i];
            ss.states_base = states_base;
            ss.flags = 0;
            ss.ins.ss = None;
            ss.ins.co = WHITE; // give it some value
            ss.outs_base = outs_base;
            ss.inchain_base = inchain_base;
        }
        for k in 0..d.ncolors {
            d.outs[outs_base + k] = None;
            d.incarea[inchain_base + k].ss = None;
        }
        return Ok(Some(i));
    }

    // look for oldest, or old enough anyway
    // ancient: oldest 33% are expendable
    let span = d.nssets * 2 / 3;
    let ancient: usize = if cp - start > span { cp - span } else { start };

    // for (ss = d->search, end = &d->ssets[d->nssets]; ss < end; ss++)
    for ss in d.search..d.nssets {
        let expendable = match d.ssets[ss].lastseen {
            None => true,
            Some(ls) => ls < ancient,
        };
        if expendable && (d.ssets[ss].flags & LOCKED) == 0 {
            d.search = ss + 1;
            return Ok(Some(ss));
        }
    }
    // for (ss = d->ssets, end = d->search; ss < end; ss++)
    for ss in 0..d.search {
        let expendable = match d.ssets[ss].lastseen {
            None => true,
            Some(ls) => ls < ancient,
        };
        if expendable && (d.ssets[ss].flags & LOCKED) == 0 {
            d.search = ss + 1;
            return Ok(Some(ss));
        }
    }

    // nobody's old enough?!? -- something's really wrong
    Err(RegError(REG_ASSERT))
}

// =============================================================================
// GETCOLOR  (regguts.h: macro)
// =============================================================================

/// `GETCOLOR(cm, c)` -- fetch the color of chr `c`.
///
/// C macro: `(c) <= MAX_SIMPLE_CHR ? (cm)->locolormap[(c) - CHR_MIN]
/// : pg_reg_getcolor(cm, c)`. `CHR_MIN` is 0, so the fast-path index is `c`.
/// The slow path delegates to the colormap module's `pg_reg_getcolor` (owned by
/// `regex_foundation`, an intra-crate dependency).
#[inline]
fn getcolor(cm: &ColorMap, c: chr) -> color {
    if c <= MAX_SIMPLE_CHR {
        cm.locolormap[(c - CHR_MIN) as usize]
    } else {
        crate::regex_foundation::pg_reg_getcolor(cm, c)
    }
}

// =============================================================================
// getsubdfa / getladfa  (regexec.c)
// =============================================================================

/// getsubdfa - create or re-fetch the DFA for a tree subre node.
///
/// C: `static struct dfa *getsubdfa(struct vars *v, struct subre *t)`. The C
/// version stashes the built DFA in `v->subdfas[t->id]`; this port returns the
/// cache **index** (`t.id`). A real allocation failure surfaces as `Err`.
pub fn getsubdfa<'mcx>(mcx: Mcx<'mcx>, v: &mut ExecVars, t: &Subre) -> RegResult<usize> {
    let id = t.id as usize;
    if v.subdfas[id].is_none() {
        let cnfa = t
            .cnfa
            .as_ref()
            .expect("getsubdfa: subre has no cnfa (NULLCNFA)");
        let mut d = newdfa(mcx, v.eflags, cnfa)?;
        // set up additional info if this is a backref node
        if t.op == b'b' {
            d.backno = t.backno;
            d.backmin = t.min;
            d.backmax = t.max;
        }
        v.subdfas[id] = Some(d);
    }
    Ok(id)
}

/// getladfa - create or re-fetch the DFA for a LACON subre node.
///
/// C: `static struct dfa *getladfa(struct vars *v, int n)`. Same as
/// [`getsubdfa`] but for LACONs, keyed by lacon number `n` into `v.ladfas`.
pub fn getladfa<'mcx>(mcx: Mcx<'mcx>, v: &mut ExecVars, g: &Guts, n: usize) -> RegResult<usize> {
    debug_assert!(n > 0 && (n as i32) < g.nlacons);
    if v.ladfas[n].is_none() {
        let cnfa = g.lacons[n]
            .cnfa
            .as_ref()
            .expect("getladfa: lacon has no cnfa (NULLCNFA)");
        let d = newdfa(mcx, v.eflags, cnfa)?;
        v.ladfas[n] = Some(d);
    }
    Ok(n)
}

// =============================================================================
// miss - handle a stateset cache miss  (rege_dfa.c)
// =============================================================================

/// miss - handle a stateset cache miss.
///
/// C: `static struct sset *miss(struct vars *v, struct dfa *d, struct sset *css,
/// color co, chr *cp, chr *start)`. Returns `Ok(Some(p))` for the next
/// stateset, `Ok(None)` for a certain match failure (C `NULL`), or `Err`.
///
/// CRITICAL: the `sawlacons` link-SUPPRESSION rule. When any LACON arc was
/// traversed to build the new stateset, we do NOT cache the `css --co--> p`
/// transition, forcing future transitions across the same edge back through
/// `miss` so the LACON(s) get rechecked in the new context.
#[allow(clippy::too_many_arguments)]
fn miss<'mcx>(
    mcx: Mcx<'mcx>,
    v: &mut ExecVars,
    g: &Guts,
    d: &mut Dfa,
    cnfa: &Cnfa,
    cm: &ColorMap,
    css: usize,
    co: color,
    cp: usize,
    start: usize,
) -> RegResult<Option<usize>> {
    // for convenience, we can be called even if it might not be a miss
    if let Some(out) = d.out(css, co) {
        return Ok(Some(out));
    }

    // INTERRUPT(v->re): operation-cancel check happens here in C; no analogue.

    // What set of states would we end up in after consuming the co character?
    for i in 0..d.wordsper {
        d.work[i] = 0; // build new stateset bitmap in d.work
    }
    let ispseudocolor = (cm.cd[co as usize].flags & PSEUDO) != 0;
    let mut ispost = false;
    let mut noprogress = true;
    let mut gotstate = false;
    for i in 0..d.nstates {
        if isbset(d.sset_states(css), i) {
            let arc_range = cnfa.states[i].clone();
            for ai in arc_range {
                let ca = cnfa.arcs[ai];
                if ca.co == co || (ca.co == RAINBOW && !ispseudocolor) {
                    bset(&mut d.work, ca.to as usize);
                    gotstate = true;
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
    if !gotstate {
        return Ok(None); // character cannot reach any new state
    }
    let mut dolacons = (cnfa.flags & HASLACONS) != 0;
    let mut sawlacons = false;
    // outer loop handles transitive closure of reachable-by-LACON states
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
                    if !lacon(mcx, v, g, cnfa, cp, ca.co)? {
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

    // Is this stateset already in the cache?
    let mut found: Option<usize> = None;
    for p in 0..d.nssused {
        // HIT(h, work, p, wordsper): hash equal AND (wordsper==1 || states equal)
        if d.ssets[p].hash == h && (d.wordsper == 1 || d.sset_states(p) == &d.work[..]) {
            found = Some(p);
            break;
        }
    }
    let p = match found {
        Some(p) => p,
        None => {
            // nope, need a new cache entry
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
            // lastseen to be dealt with by caller
            p
        }
    };

    // Link new stateset to old, unless a LACON affected the result.
    if !sawlacons {
        // css->outs[co] = p;
        d.outs[d.ssets[css].outs_base + co as usize] = Some(p);
        // css->inchain[co] = p->ins;
        d.incarea[d.ssets[css].inchain_base + co as usize] = d.ssets[p].ins;
        // p->ins.ss = css; p->ins.co = co;
        d.ssets[p].ins.ss = Some(css);
        d.ssets[p].ins.co = co;
    }
    Ok(Some(p))
}

// =============================================================================
// lacon - lookaround-constraint checker for miss()  (rege_dfa.c)
// =============================================================================

/// lacon - lookaround-constraint checker for miss().
///
/// C: `static int lacon(struct vars *v, struct cnfa *pcnfa, chr *cp, color co)`.
/// Returns `Ok(true)`/`Ok(false)` for satisfied/not-satisfied, or `Err`.
///
/// RECURSION: this re-invokes the matcher on the lacon's sub-cNFA, so `lacon ->
/// shortest -> miss -> lacon` can recurse. The `v.depth`/`v.max_depth` counter
/// is the `STACK_TOO_DEEP` analogue; exceeding it yields `Err(REG_ETOOBIG)`.
fn lacon<'mcx>(
    mcx: Mcx<'mcx>,
    v: &mut ExecVars,
    g: &Guts,
    pcnfa: &Cnfa,
    cp: usize,
    co: color,
) -> RegResult<bool> {
    // Since this is recursive, it could be driven to stack overflow.
    if v.depth >= v.max_depth {
        return Err(RegError(REG_ETOOBIG));
    }
    v.depth += 1;
    let result = lacon_inner(mcx, v, g, pcnfa, cp, co);
    v.depth -= 1;
    result
}

/// Inner body of [`lacon`], split out so the depth counter is decremented on
/// every return path (the `?` operator included).
fn lacon_inner<'mcx>(
    mcx: Mcx<'mcx>,
    v: &mut ExecVars,
    g: &Guts,
    pcnfa: &Cnfa,
    cp: usize,
    co: color,
) -> RegResult<bool> {
    let n = (co as i32 - pcnfa.ncolors) as usize;
    debug_assert!(n > 0 && (n as i32) < g.nlacons);
    // latype is read from the lacon subre (lives in g, immutable).
    let latype = g.lacons[n].latype as i32;

    let d_idx = getladfa(mcx, v, g, n)?;

    if latype_is_ahead(latype) {
        // used to use longest() here, but shortest() could be much cheaper
        let stop = v.stop;
        // Take the ladfa out of v to break the alias while stepping it.
        let mut d = v.ladfas[d_idx].take().expect("ladfa present");
        let cnfa = g.lacons[n]
            .cnfa
            .as_ref()
            .expect("lacon has no cnfa (NULLCNFA)");
        let end = shortest(mcx, v, g, &mut d, cnfa, &g.cmap, cp, cp, stop, None, None);
        v.ladfas[d_idx] = Some(d);
        let end = end?;
        let satisfied = if latype_is_pos(latype) {
            end.is_some()
        } else {
            end.is_none()
        };
        Ok(satisfied)
    } else {
        // To avoid O(N^2) work, use matchuntil() which caches the DFA state
        // across calls. We only need to restart if the probe point decreases.
        let mut d = v.ladfas[d_idx].take().expect("ladfa present");
        let cnfa = g.lacons[n]
            .cnfa
            .as_ref()
            .expect("lacon has no cnfa (NULLCNFA)");
        let r = matchuntil(mcx, v, g, &mut d, cnfa, &g.cmap, cp, n);
        v.ladfas[d_idx] = Some(d);
        let mut satisfied = r?;
        if !latype_is_pos(latype) {
            satisfied = !satisfied;
        }
        Ok(satisfied)
    }
}

// =============================================================================
// longest - longest-preferred matching engine  (rege_dfa.c)
// =============================================================================

/// longest - longest-preferred matching engine.
///
/// C: `static chr *longest(struct vars *v, struct dfa *d, chr *start, chr *stop,
/// int *hitstopp)`. Returns `Ok(Some(p))` for the match endpoint, `Ok(None)`
/// for no match (C `NULL`), or `Err`. `hitstopp` (C `int *`, optional) is
/// `&mut Option<bool>` -- pass `Some(&mut flag)` or `None`.
#[allow(clippy::too_many_arguments)]
fn longest<'mcx>(
    mcx: Mcx<'mcx>,
    v: &mut ExecVars,
    g: &Guts,
    d: &mut Dfa,
    cnfa: &Cnfa,
    cm: &ColorMap,
    start: usize,
    stop: usize,
    mut hitstopp: Option<&mut bool>,
) -> RegResult<Option<usize>> {
    let realstop = if stop == v.stop { stop } else { stop + 1 };

    // prevent "uninitialized variable" warnings
    if let Some(hs) = hitstopp.as_deref_mut() {
        *hs = false;
    }

    // if this is a backref to a known string, just match against that
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

    // fast path for matchall NFAs
    if (cnfa.flags & MATCHALL) != 0 {
        // C compares size_t nchr against int min/maxmatchall; mirror that by
        // comparing usize nchr against the ints widened to usize.
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

    // initialize
    let mut css = match initialize(d, cnfa, start)? {
        Some(css) => css,
        None => return Ok(None),
    };
    let mut cp = start;

    // startup
    let co = if cp == v.start {
        cnfa.bos[if (v.eflags & REG_NOTBOL) != 0 { 0 } else { 1 }]
    } else {
        getcolor(cm, v.input[cp - 1])
    };
    css = match miss(mcx, v, g, d, cnfa, cm, css, co, cp, start)? {
        Some(css) => css,
        None => return Ok(None),
    };
    d.ssets[css].lastseen = Some(cp);

    // main text-scanning loop
    while cp < realstop {
        let co = getcolor(cm, v.input[cp]);
        let ss = match d.out(css, co) {
            Some(ss) => ss,
            None => match miss(mcx, v, g, d, cnfa, cm, css, co, cp + 1, start)? {
                Some(ss) => ss,
                None => break, // NOTE BREAK OUT
            },
        };
        cp += 1;
        d.ssets[ss].lastseen = Some(cp);
        css = ss;
    }

    // shutdown
    if cp == v.stop && stop == v.stop {
        if let Some(hs) = hitstopp {
            *hs = true;
        }
        let co = cnfa.eos[if (v.eflags & REG_NOTEOL) != 0 { 0 } else { 1 }];
        let ss = miss(mcx, v, g, d, cnfa, cm, css, co, cp, start)?;
        // special case: match ended at eol?
        match ss {
            Some(ss) if (d.ssets[ss].flags & POSTSTATE) != 0 => return Ok(Some(cp)),
            Some(ss) => d.ssets[ss].lastseen = Some(cp), // to be tidy
            None => {}
        }
    }

    // find last match, if any.
    let mut post = d.lastpost;
    for ss in 0..d.nssused {
        if (d.ssets[ss].flags & POSTSTATE) != 0
            && post != d.ssets[ss].lastseen
            && (post.is_none() || post < d.ssets[ss].lastseen)
        {
            post = d.ssets[ss].lastseen;
        }
    }
    if let Some(post) = post {
        // found one
        return Ok(Some(post - 1));
    }

    Ok(None)
}

// =============================================================================
// shortest - shortest-preferred matching engine  (rege_dfa.c)
// =============================================================================

/// shortest - shortest-preferred matching engine.
///
/// C: `static chr *shortest(struct vars *v, struct dfa *d, chr *start, chr *min,
/// chr *max, chr **coldp, int *hitstopp)`. Returns `Ok(Some(p))` for the match
/// endpoint, `Ok(None)` for no match, or `Err`. `coldp` is
/// `Option<&mut Option<usize>>`; `hitstopp` is `Option<&mut bool>`.
#[allow(clippy::too_many_arguments)]
fn shortest<'mcx>(
    mcx: Mcx<'mcx>,
    v: &mut ExecVars,
    g: &Guts,
    d: &mut Dfa,
    cnfa: &Cnfa,
    cm: &ColorMap,
    start: usize,
    mut min: usize,
    max: usize,
    mut coldp: Option<&mut Option<usize>>,
    mut hitstopp: Option<&mut bool>,
) -> RegResult<Option<usize>> {
    let record_coldp = coldp.is_some();
    // prevent "uninitialized variable" warnings; the coldp reset is load-bearing
    // (cfindloop reuses one `cold` slot across iterations, and shortest has early
    // returns that never reach the lastcold() store below).
    if let Some(cd) = coldp.as_deref_mut() {
        *cd = None;
    }
    if let Some(hs) = hitstopp.as_deref_mut() {
        *hs = false;
    }
    let realmin = if min == v.stop { min } else { min + 1 };
    let realmax = if max == v.stop { max } else { max + 1 };

    // if this is a backref to a known string, just match against that
    if d.backno >= 0 {
        debug_assert!((d.backno as usize) < v.nmatch);
        if v.pmatch[d.backno as usize].0 >= 0 {
            let cp = dfa_backref(v, g, d, start, min, max, true)?;
            if cp.is_some() {
                if let Some(cd) = coldp.as_deref_mut() {
                    *cd = Some(start);
                }
            }
            // there is no case where we should set *hitstopp
            return Ok(cp);
        }
    }

    // fast path for matchall NFAs
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
        // there is no case where we should set *hitstopp
        return Ok(Some(min));
    }

    // initialize
    let mut css = match initialize(d, cnfa, start)? {
        Some(css) => css,
        None => return Ok(None),
    };
    let mut cp = start;

    // startup
    let co = if cp == v.start {
        cnfa.bos[if (v.eflags & REG_NOTBOL) != 0 { 0 } else { 1 }]
    } else {
        getcolor(cm, v.input[cp - 1])
    };
    css = match miss(mcx, v, g, d, cnfa, cm, css, co, cp, start)? {
        Some(css) => css,
        None => return Ok(None),
    };
    d.ssets[css].lastseen = Some(cp);
    let mut ss: Option<usize> = Some(css);

    // main text-scanning loop
    while cp < realmax {
        let co = getcolor(cm, v.input[cp]);
        let next = match d.out(css, co) {
            Some(ss) => ss,
            None => match miss(mcx, v, g, d, cnfa, cm, css, co, cp + 1, start)? {
                Some(ss) => ss,
                None => {
                    ss = None;
                    break; // NOTE BREAK OUT
                }
            },
        };
        cp += 1;
        d.ssets[next].lastseen = Some(cp);
        css = next;
        ss = Some(next);
        if (d.ssets[next].flags & POSTSTATE) != 0 && cp >= realmin {
            break; // NOTE BREAK OUT
        }
    }

    let ss = match ss {
        Some(ss) => ss,
        None => return Ok(None),
    };

    if record_coldp {
        // report last no-progress state set, if any (C: *coldp = lastcold(v, d))
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
        ss_opt = miss(mcx, v, g, d, cnfa, cm, css, co, cp, start)?;
        // match might have ended at eol:
        //   if ((ss == NULL || !(ss->flags & POSTSTATE)) && hitstopp) *hitstopp = 1;
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

    // C's final `if (ss == NULL || !(ss->flags & POSTSTATE)) return NULL;`.
    match ss_opt {
        Some(s) if (d.ssets[s].flags & POSTSTATE) != 0 => Ok(Some(cp)),
        _ => Ok(None),
    }
}

// =============================================================================
// matchuntil - incremental matching engine  (rege_dfa.c)
// =============================================================================

/// matchuntil - incremental matching engine.
///
/// C: `static int matchuntil(struct vars *v, struct dfa *d, chr *probe,
/// struct sset **lastcss, chr **lastcp)`. Determines whether a match exists
/// starting at `v.start` and ending at `probe`. The restart state is the
/// `(lblastcss[lac], lblastcp[lac])` pair indexed by lacon number `lac`.
/// Returns `Ok(true)`/`Ok(false)`, or `Err`.
#[allow(clippy::too_many_arguments)]
fn matchuntil<'mcx>(
    mcx: Mcx<'mcx>,
    v: &mut ExecVars,
    g: &Guts,
    d: &mut Dfa,
    cnfa: &Cnfa,
    cm: &ColorMap,
    probe: usize,
    lac: usize,
) -> RegResult<bool> {
    let mut cp = v.lblastcp[lac];
    let mut css = v.lblastcss[lac];

    // fast path for matchall NFAs
    if (cnfa.flags & MATCHALL) != 0 {
        let nchr = probe - v.start;
        if nchr < cnfa.minmatchall as usize {
            return Ok(false);
        }
        // maxmatchall will always be infinity, cf. makesearch()
        debug_assert!(cnfa.maxmatchall == DUPINF);
        return Ok(true);
    }

    // initialize and startup, or restart, if necessary
    if cp.is_none() || cp > Some(probe) {
        let start = v.start;
        cp = Some(start);
        let init = match initialize(d, cnfa, start)? {
            Some(s) => s,
            None => return Ok(false),
        };
        let co = cnfa.bos[if (v.eflags & REG_NOTBOL) != 0 { 0 } else { 1 }];
        let m = miss(mcx, v, g, d, cnfa, cm, init, co, start, start)?;
        let css_i = match m {
            Some(s) => s,
            None => {
                // C sets css to the miss() result (NULL) but does not write it
                // back to *lastcss before returning 0.
                return Ok(false);
            }
        };
        css = Some(css_i);
        d.ssets[css_i].lastseen = cp;
    } else if css.is_none() {
        // we previously found that no match is possible beyond *lastcp
        return Ok(false);
    }
    // ss = css
    let mut ss = css;
    let mut cp_v = cp.expect("cp set");
    let mut css_v = css.expect("css set");

    // main text-scanning loop
    while cp_v < probe {
        let co = getcolor(cm, v.input[cp_v]);
        let next = match d.out(css_v, co) {
            Some(s) => s,
            None => match miss(mcx, v, g, d, cnfa, cm, css_v, co, cp_v + 1, v.start)? {
                Some(s) => s,
                None => {
                    ss = None;
                    break; // NOTE BREAK OUT
                }
            },
        };
        cp_v += 1;
        d.ssets[next].lastseen = Some(cp_v);
        css_v = next;
        ss = Some(next);
    }

    // *lastcss = ss; *lastcp = cp;
    v.lblastcss[lac] = ss;
    v.lblastcp[lac] = Some(cp_v);

    let ss = match ss {
        Some(s) => s,
        None => return Ok(false), // impossible match, or internal error
    };
    // css now equals ss for the trailing check (C uses css == ss here)
    css_v = ss;

    // We need to process one more chr, or the EOS symbol, to check match.
    let ss = if cp_v < v.stop {
        let co = getcolor(cm, v.input[cp_v]);
        match d.out(css_v, co) {
            Some(s) => Some(s),
            None => miss(mcx, v, g, d, cnfa, cm, css_v, co, cp_v + 1, v.start)?,
        }
    } else {
        debug_assert!(cp_v == v.stop);
        let co = cnfa.eos[if (v.eflags & REG_NOTEOL) != 0 { 0 } else { 1 }];
        miss(mcx, v, g, d, cnfa, cm, css_v, co, cp_v, v.start)?
    };

    match ss {
        Some(s) if (d.ssets[s].flags & POSTSTATE) != 0 => Ok(true),
        _ => Ok(false),
    }
}

// =============================================================================
// dfa_backref - find best match length for a known backref string  (rege_dfa.c)
// =============================================================================

/// dfa_backref - find best match length for a known backref string.
///
/// C: `static chr *dfa_backref(struct vars *v, struct dfa *d, chr *start,
/// chr *min, chr *max, bool shortest)`. Returns `Ok(Some(p))` for the
/// longest/shortest valid repeated-match endpoint, or `Ok(None)`. Should stay
/// in sync with `cbrdissect`.
fn dfa_backref(
    v: &ExecVars,
    g: &Guts,
    d: &Dfa,
    start: usize,
    min: usize,
    max: usize,
    shortest: bool,
) -> RegResult<Option<usize>> {
    let n = d.backno as usize;
    let backmin = d.backmin as i32;
    let backmax = d.backmax as i32;

    // get the backreferenced string (caller should have checked this)
    if v.pmatch[n].0 == -1 {
        return Ok(None);
    }
    let br_so = v.pmatch[n].0 as usize;
    let br_eo = v.pmatch[n].1 as usize;
    let brstring = v.start + br_so;
    let brlen = br_eo - br_so;

    // special-case zero-length backreference to avoid divide by zero
    if brlen == 0 {
        // matches only a zero-length string, but any number of repetitions can
        // be considered to be present
        if min == start && backmin <= backmax {
            return Ok(Some(start));
        }
        return Ok(None);
    }

    // convert min and max into numbers of possible repetitions of the backref
    // string, rounding appropriately
    let mut minreps: i64 = if min <= start {
        0
    } else {
        ((min - start - 1) / brlen + 1) as i64
    };
    let mut maxreps: i64 = ((max - start) / brlen) as i64;

    // apply bounds, then see if there is any allowed match length
    if minreps < backmin as i64 {
        minreps = backmin as i64;
    }
    if backmax != DUPINF && maxreps > backmax as i64 {
        maxreps = backmax as i64;
    }
    if maxreps < minreps {
        return Ok(None);
    }

    // quick exit if zero-repetitions match is valid and preferred
    if shortest && minreps == 0 {
        return Ok(Some(start));
    }

    // okay, compare the actual string contents
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

// =============================================================================
// lastcold - determine last point at which no progress had been made  (rege_dfa.c)
// =============================================================================

/// lastcold - determine last point at which no progress had been made.
///
/// C: `static chr *lastcold(struct vars *v, struct dfa *d)`. Returns the
/// endpoint position (never NULL once defaulted to `v.start`).
fn lastcold(v: &ExecVars, d: &Dfa) -> usize {
    let mut nopr = d.lastnopr.unwrap_or(v.start);
    for ss in 0..d.nssused {
        if (d.ssets[ss].flags & NOPROGRESS) != 0 {
            if let Some(ls) = d.ssets[ss].lastseen {
                if nopr < ls {
                    nopr = ls;
                }
            }
        }
    }
    nopr
}

// =============================================================================
// find / cfind / cfindloop  -- the matcher entry points  (regexec.c)
// =============================================================================

/// `OFF(p)` -- offset of position `p` relative to the start of string.
///
/// C macro: `OFF(p) == (p) - v->start`.
#[inline]
fn off(v: &ExecVars, p: usize) -> isize {
    (p - v.start) as isize
}

/// find - find a match for the main NFA (no-complications case).
///
/// C: `static int find(struct vars *v, struct cnfa *cnfa, struct colormap *cm)`.
/// First scans with the SEARCH NFA (`g.search`) to localize the possible-start
/// range, then walks candidate begin positions running the main DFA
/// (`longest`/`shortest`) and, on the first endpoint found, dissects captures
/// via [`cdissect`]. C's REG_EXPECT detail reporting (`v->details`) is dropped
/// in this family's surface (the matcher is always called with `details ==
/// NULL`); the `REG_EXPECT` flag therefore has no live effect here.
fn find<'mcx>(mcx: Mcx<'mcx>, v: &mut ExecVars, g: &Guts, cnfa: &Cnfa, cm: &ColorMap) -> RegResult<i32> {
    // tree-root flags: shorter-preferred?
    let troot = g.tree.expect("find: tree root present");
    let shorter = (g.tree_nodes[troot.0 as usize].flags & SHORTER) != 0;

    // first, a shot with the search RE
    let mut s = newdfa(mcx, v.eflags, &g.search)?;
    let search_start = v.search_start;
    let stop = v.stop;
    let mut cold: Option<usize> = None;
    let close = shortest(
        mcx,
        v,
        g,
        &mut s,
        &g.search,
        cm,
        search_start,
        search_start,
        stop,
        Some(&mut cold),
        None,
    );
    drop(s); // freedfa(s)
    let close = close?;

    // (C surfaces cold via details.rm_extend when REG_EXPECT is set; this family
    // carries no details out-param, so the REG_EXPECT branch is a no-op.)
    let _ = REG_EXPECT;

    let close = match close {
        Some(c) => c,
        None => return Ok(REG_NOMATCH), // not found
    };
    if v.nmatch == 0 {
        // found, don't need exact location
        return Ok(REG_OKAY);
    }

    // find starting point and match
    let open = cold.expect("find: cold set when close found");
    let mut cold: Option<usize> = None;

    let mut d = newdfa(mcx, v.eflags, cnfa)?;
    let mut begin = open;
    let mut end: Option<usize> = None;
    while begin <= close {
        let mut hitend = false;
        let r = if shorter {
            shortest(mcx, v, g, &mut d, cnfa, cm, begin, begin, stop, None, Some(&mut hitend))
        } else {
            longest(mcx, v, g, &mut d, cnfa, cm, begin, stop, Some(&mut hitend))
        };
        end = match r {
            Ok(e) => e,
            Err(e) => {
                drop(d); // freedfa(d) on error
                return Err(e);
            }
        };
        if hitend && cold.is_none() {
            cold = Some(begin);
        }
        if end.is_some() {
            break; // NOTE BREAK OUT
        }
        begin += 1;
    }
    let end = end.expect("find: search RE succeeded so loop should find an end");
    drop(d); // freedfa(d)
    let _ = cold;

    // and pin down details
    debug_assert!(v.nmatch > 0);
    v.pmatch[0].0 = off(v, begin);
    v.pmatch[0].1 = off(v, end);
    if v.nmatch == 1 {
        // no need for submatches
        return Ok(REG_OKAY);
    }

    // find submatches
    cdissect(mcx, v, g, troot, begin, end)
}

/// cfind - find a match for the main NFA (with complications).
///
/// C: `static int cfind(struct vars *v, struct cnfa *cnfa, struct colormap *cm)`.
/// The backref/lookaround path. Builds the search DFA `s` and main DFA `d` and
/// delegates to [`cfindloop`].
fn cfind<'mcx>(
    mcx: Mcx<'mcx>,
    v: &mut ExecVars,
    g: &Guts,
    cnfa: &Cnfa,
    cm: &ColorMap,
) -> RegResult<i32> {
    let mut s = newdfa(mcx, v.eflags, &g.search)?;
    let mut d = newdfa(mcx, v.eflags, cnfa)?;

    let mut cold: Option<usize> = None;
    let ret = cfindloop(mcx, v, g, cnfa, cm, &mut d, &mut s, &mut cold);

    drop(d); // freedfa(d)
    drop(s); // freedfa(s)
    let ret = ret?;
    let _ = cold; // C surfaces it via details.rm_extend (no out-param here).
    Ok(ret)
}

/// cfindloop - the heart of cfind.
///
/// C: `static int cfindloop(struct vars *v, struct cnfa *cnfa,
/// struct colormap *cm, struct dfa *d, struct dfa *s, chr **coldp)`. Repeatedly
/// scan with the search DFA `s` for a match range at/beyond `close`, then over
/// every candidate begin position in `[open, close]` run the main DFA `d` and
/// [`cdissect`] the tentative match. On the first dissect success fill
/// `pmatch[0]` and return `REG_OKAY`.
#[allow(clippy::too_many_arguments)]
fn cfindloop<'mcx>(
    mcx: Mcx<'mcx>,
    v: &mut ExecVars,
    g: &Guts,
    cnfa: &Cnfa,
    cm: &ColorMap,
    d: &mut Dfa,
    s: &mut Dfa,
    coldp: &mut Option<usize>,
) -> RegResult<i32> {
    let troot = g.tree.expect("cfindloop: tree root present");
    let shorter = (g.tree_nodes[troot.0 as usize].flags & SHORTER) != 0;

    let stop = v.stop;
    let mut cold: Option<usize> = None;
    let mut close = v.search_start;

    // C: do { ... close++; } while (close < v->stop);
    loop {
        // Search with the search RE for match range at/beyond "close"
        let close_opt = shortest(
            mcx,
            v,
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

        // Search for matches starting between "open" and "close" inclusive
        let mut begin = open;
        while begin <= close {
            let mut estart = begin;
            let mut estop = v.stop;
            loop {
                // Here we use the top node's detailed RE
                let mut hitend = false;
                let end_res = if shorter {
                    shortest(mcx, v, g, d, cnfa, cm, begin, estart, estop, None, Some(&mut hitend))
                } else {
                    longest(mcx, v, g, d, cnfa, cm, begin, estop, Some(&mut hitend))
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
                // Dissect the potential match to see if it really matches
                let er = cdissect(mcx, v, g, troot, begin, end)?;
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
                // Try next longer/shorter match with same begin point
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

        // No possible match starting at or before "close"; consider matches
        // beyond that with a fresh search RE scan.
        close += 1;
        if close >= v.stop {
            break;
        }
    }

    *coldp = cold;
    Ok(REG_NOMATCH)
}

// =============================================================================
// pg_regexec - the public entry point  (regexec.c)
// =============================================================================

/// `pg_regexec(regex_t *re, const chr *string, size_t len, size_t search_start,
/// rm_detail_t *details, size_t nmatch, regmatch_t pmatch[], int eflags)` --
/// match a compiled regex against `data` starting at `search_start`, filling
/// `pmatch` on a match.
///
/// This family's surface takes the already-validated compiled `guts` directly
/// (the seam adapter in `regex_export_free_error` does the `re_magic`/`re_csize`
/// validation and `pg_set_regex_collation`); `nmatch` is `pmatch.len()`; and
/// the C `rm_detail_t *details` out-param (REG_EXPECT reporting) is not part of
/// this surface, so the matcher always runs as if `details == NULL`.
///
/// Returns `Ok(true)` on a match (`REG_OKAY`, `pmatch` filled), `Ok(false)` on
/// `REG_NOMATCH`, and `Err(RegError(code))` for any other `REG_*` code.
pub fn pg_regexec<'mcx>(
    mcx: Mcx<'mcx>,
    guts: &Guts,
    data: &[chr],
    search_start: i32,
    pmatch: &mut [RegMatch],
    eflags: i32,
) -> RegResult<bool> {
    let code = pg_regexec_code(mcx, guts, data, search_start, pmatch, eflags);
    match code {
        REG_OKAY => Ok(true),
        REG_NOMATCH => Ok(false),
        other => Err(RegError::new(other)),
    }
}

/// The body of [`pg_regexec`], returning the raw `REG_*` code (so the
/// REG_OKAY/REG_NOMATCH/other split matches C's `int` return convention).
fn pg_regexec_code<'mcx>(
    mcx: Mcx<'mcx>,
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

    // REG_EXPECT requires a details out-param, which this surface lacks.
    if (g.cflags & REG_EXPECT) != 0 {
        return crate::regex_consts::REG_INVARG;
    }
    if (g.info & REG_UIMPOSSIBLE as i64) != 0 {
        return REG_NOMATCH;
    }
    let backref = (g.info & REG_UBACKREF as i64) != 0;

    // Work area for the match vector.  C either uses the caller's array directly
    // (`v->pmatch == pmatch`) or (when backref && nmatch <= g->nsub) a larger
    // LOCALMAT[20]/MALLOC scratch that is zapallsubs'd and partially copied back.
    let v_nmatch: usize;
    let mut work: Vec<(isize, isize)>;

    if backref && nmatch <= g.nsub {
        // need larger work area (C: v->pmatch = mat / MALLOC; zapallsubs).
        v_nmatch = g.nsub + 1;
        work = alloc::vec![(-1isize, -1isize); v_nmatch];
        zapallsubs(&mut work, v_nmatch);
    } else {
        // store results directly in caller's array (C: v->pmatch = pmatch). C
        // zaps the caller's array over `nmatch` UP FRONT, so do that here, then
        // model the in-place writes via a work area seeded from the caller's
        // (now-zapped) array.
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
        // then forget about extra entries, to avoid useless work in find()
        v_nmatch = if nmatch > g.nsub + 1 {
            g.nsub + 1
        } else {
            nmatch
        };
    }

    let stop = len;
    // size the per-tree / per-lacon caches (C: subdfas[ntree], ladfas[nlacons]).
    debug_assert!(g.ntree >= 0);
    let ntree = g.ntree as usize;
    let subdfas: Vec<Option<Dfa>> = (0..ntree).map(|_| None).collect();
    debug_assert!(g.nlacons >= 0);
    let nlacons = g.nlacons as usize;
    let ladfas: Vec<Option<Dfa>> = (0..nlacons).map(|_| None).collect();
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
        subdfas,
        ladfas,
        lblastcss,
        lblastcp,
        max_depth: DEFAULT_MAX_DEPTH,
    };

    // do it: select the TREE-ROOT cNFA for dissection; find/cfind use g.search
    // internally for the initial scan.
    debug_assert!(g.tree.is_some());
    let troot = g.tree.expect("pg_regexec: tree root present");
    let main_cnfa = g.tree_nodes[troot.0 as usize]
        .cnfa
        .as_ref()
        .expect("pg_regexec: tree root has a cnfa");

    let st = if backref {
        cfind(mcx, &mut v, g, main_cnfa, &g.cmap)
    } else {
        find(mcx, &mut v, g, main_cnfa, &g.cmap)
    };
    let st = match st {
        Ok(code) => code,
        Err(e) => e.0,
    };

    // The per-tree/per-lacon DFAs drop with `v` (C's cleanup freedfa loop).
    drop(v);

    // on success, ensure caller's match vector is filled correctly
    if st == REG_OKAY && nmatch > 0 {
        let ncopy = nmatch.min(work.len());
        for (dst, src) in pmatch[..ncopy].iter_mut().zip(&work[..ncopy]) {
            dst.rm_so = src.0 as pg_regoff_t;
            dst.rm_eo = src.1 as pg_regoff_t;
        }
        if (g.cflags & REG_NOSUB) != 0 {
            // don't expose possibly-partial sub-match results to caller
            // (C: zapallsubs(pmatch, nmatch) over the caller's nmatch entries).
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

// =============================================================================
// the DISSECT / capture family  (regexec.c)
//
// These recursively assign capture sub-match boundaries (pmatch) over a matched
// span using the subre tree (`g.tree_nodes`, referenced by `NodeId`). Positions
// `begin`/`end` are `usize` offsets into the input. The C goto-backtracking in
// citerdissect/creviterdissect is converted to labeled loops; `endpts` becomes
// a `Vec`. The recursive cdissect tree-walk is bounded by the
// `v.depth`/`v.max_depth` STACK_TOO_DEEP analogue (REG_ETOOBIG when exceeded).
// =============================================================================

/// Run [`longest`] over the tree-subre DFA identified by `nid`, returning the
/// match endpoint (`Ok(None)` == C `NULL`). `nid` must already have its DFA
/// built by [`getsubdfa`].
fn longest_sub<'mcx>(
    mcx: Mcx<'mcx>,
    v: &mut ExecVars,
    g: &Guts,
    nid: NodeId,
    start: usize,
    stop: usize,
    hitstopp: Option<&mut bool>,
) -> RegResult<Option<usize>> {
    let arena = nid.0 as usize;
    // The subdfa cache is keyed by the subre's `id` field (cf. getsubdfa), NOT
    // the tree-node arena index.
    let id = g.tree_nodes[arena].id as usize;
    let cnfa = g.tree_nodes[arena]
        .cnfa
        .as_ref()
        .expect("longest_sub: subre has no cnfa (NULLCNFA)");
    let mut d = v.subdfas[id].take().expect("longest_sub: subdfa present");
    let r = longest(mcx, v, g, &mut d, cnfa, &g.cmap, start, stop, hitstopp);
    v.subdfas[id] = Some(d);
    r
}

/// Run [`shortest`] over the tree-subre DFA identified by `nid`. The dissectors
/// pass `record_coldp == false` (C passes NULL for both `coldp` and `hitstopp`).
fn shortest_sub<'mcx>(
    mcx: Mcx<'mcx>,
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
    let mut d = v.subdfas[id].take().expect("shortest_sub: subdfa present");
    let mut scratch: Option<usize> = None;
    let coldp = if record_coldp {
        Some(&mut scratch)
    } else {
        None
    };
    let r = shortest(mcx, v, g, &mut d, cnfa, &g.cmap, start, min, max, coldp, None);
    v.subdfas[id] = Some(d);
    r
}

// -----------------------------------------------------------------------------
// zapallsubs / zaptreesubs / subset  (regexec.c)
// -----------------------------------------------------------------------------

/// zapallsubs - initialize all subexpression matches to "no match".
///
/// C: `static void zapallsubs(regmatch_t *p, size_t n)`. `p[0]` (the
/// overall-match location) is not touched: the loop runs from `n - 1` down to
/// `1`. "No match" is `(-1, -1)`.
pub fn zapallsubs(p: &mut [(isize, isize)], n: usize) {
    // for (i = n - 1; i > 0; i--)  -- p[0] left alone.
    let mut i = n.wrapping_sub(1);
    while i > 0 {
        p[i].0 = -1;
        p[i].1 = -1;
        i -= 1;
    }
}

/// zaptreesubs - initialize subexpressions within subtree to "no match".
///
/// C: `static void zaptreesubs(struct vars *v, struct subre *t)`. Clears `t`'s
/// own capture slot (if `t->capno > 0` and within `v.nmatch`), then recurses
/// over every child via the `child`/`sibling` chain.
fn zaptreesubs(v: &mut ExecVars, g: &Guts, t: NodeId) {
    let id = t.0 as usize;
    let n = g.tree_nodes[id].capno;
    if n > 0 && (n as usize) < v.nmatch {
        v.pmatch[n as usize].0 = -1;
        v.pmatch[n as usize].1 = -1;
    }

    // for (t2 = t->child; t2 != NULL; t2 = t2->sibling)
    let mut t2 = g.tree_nodes[id].child;
    while let Some(c) = t2 {
        zaptreesubs(v, g, c);
        t2 = g.tree_nodes[c.0 as usize].sibling;
    }
}

/// subset - set subexpression match data for a successful subre.
///
/// C: `static void subset(struct vars *v, struct subre *sub, chr *begin,
/// chr *end)`. Records `OFF(begin)`/`OFF(end)` into `v.pmatch[sub->capno]` when
/// the slot is within `v.nmatch`. C `assert(n > 0)`.
fn subset(v: &mut ExecVars, g: &Guts, sub: NodeId, begin: usize, end: usize) {
    let n = g.tree_nodes[sub.0 as usize].capno;
    debug_assert!(n > 0);
    if (n as usize) >= v.nmatch {
        return;
    }
    v.pmatch[n as usize].0 = (begin - v.start) as isize;
    v.pmatch[n as usize].1 = (end - v.start) as isize;
}

// -----------------------------------------------------------------------------
// cdissect - the dispatcher  (regexec.c)
// -----------------------------------------------------------------------------

/// cdissect - check backrefs and determine subexpression matches.
///
/// C: `static int cdissect(struct vars *v, struct subre *t, chr *begin,
/// chr *end)`. Recursively processes a subre tree to check matching of backrefs
/// and/or identify submatch boundaries for capture nodes. The proposed match
/// runs from `begin` to `end` (not including `end`).
///
/// The C `STACK_TOO_DEEP(v->re)` guard becomes the `v.depth`/`v.max_depth`
/// counter (REG_ETOOBIG when exceeded).
fn cdissect<'mcx>(
    mcx: Mcx<'mcx>,
    v: &mut ExecVars,
    g: &Guts,
    t: NodeId,
    begin: usize,
    end: usize,
) -> RegResult<i32> {
    // handy place to check ... stack overrun (C: STACK_TOO_DEEP -> REG_ETOOBIG).
    if v.depth >= v.max_depth {
        return Ok(REG_ETOOBIG);
    }
    v.depth += 1;
    let r = cdissect_inner(mcx, v, g, t, begin, end);
    v.depth -= 1;
    r
}

/// Inner body of [`cdissect`], split out so the depth counter is decremented on
/// every return path (the `?` operator included).
fn cdissect_inner<'mcx>(
    mcx: Mcx<'mcx>,
    v: &mut ExecVars,
    g: &Guts,
    t: NodeId,
    begin: usize,
    end: usize,
) -> RegResult<i32> {
    let id = t.0 as usize;
    let op = g.tree_nodes[id].op;
    let capno = g.tree_nodes[id].capno;

    // INTERRUPT(v->re): operation-cancel check happens here in C; no analogue.

    let er: i32 = match op {
        b'=' => {
            // terminal node -- no action, parent did the work
            debug_assert!(g.tree_nodes[id].child.is_none());
            REG_OKAY
        }
        b'b' => {
            // back reference
            debug_assert!(g.tree_nodes[id].child.is_none());
            cbrdissect(v, g, t, begin, end)?
        }
        b'.' => {
            // concatenation
            let child = g.tree_nodes[id].child.expect("concat has child");
            if (g.tree_nodes[child.0 as usize].flags & SHORTER) != 0 {
                crevcondissect(mcx, v, g, t, begin, end)? // reverse scan
            } else {
                ccondissect(mcx, v, g, t, begin, end)?
            }
        }
        b'|' => {
            // alternation
            debug_assert!(g.tree_nodes[id].child.is_some());
            caltdissect(mcx, v, g, t, begin, end)?
        }
        b'*' => {
            // iteration
            let child = g.tree_nodes[id].child.expect("iter has child");
            if (g.tree_nodes[child.0 as usize].flags & SHORTER) != 0 {
                creviterdissect(mcx, v, g, t, begin, end)? // reverse scan
            } else {
                citerdissect(mcx, v, g, t, begin, end)?
            }
        }
        b'(' => {
            // no-op capture node
            let child = g.tree_nodes[id].child.expect("capture has child");
            cdissect(mcx, v, g, child, begin, end)?
        }
        _ => REG_ASSERT,
    };

    // We should never have a match failure unless backrefs lurk below.
    debug_assert!(er != REG_NOMATCH || (g.tree_nodes[id].flags & BACKR) != 0);

    // If this node is marked as capturing, save successful match's location.
    if capno > 0 && er == REG_OKAY {
        subset(v, g, t, begin, end);
    }

    Ok(er)
}

// -----------------------------------------------------------------------------
// ccondissect - dissect match for concatenation node  (regexec.c)
// -----------------------------------------------------------------------------

/// ccondissect - dissect match for concatenation node.
///
/// C: `static int ccondissect(struct vars *v, struct subre *t, chr *begin,
/// chr *end)`. Splits the span at a tentative `mid`point (via [`longest`] on
/// `left`'s DFA), checks `right`'s DFA reaches exactly `end`, then recurses on
/// both halves. On a failed `right` recursion it resets `left`'s matches
/// ([`zaptreesubs`], rule 6) before backing the midpoint up by one and retrying.
fn ccondissect<'mcx>(
    mcx: Mcx<'mcx>,
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

    // d = getsubdfa(v, left); d2 = getsubdfa(v, right);
    getsubdfa(mcx, v, &g.tree_nodes[left.0 as usize])?;
    getsubdfa(mcx, v, &g.tree_nodes[right.0 as usize])?;

    // pick a tentative midpoint
    let mut mid = match longest_sub(mcx, v, g, left, begin, end, None)? {
        Some(m) => m,
        None => return Ok(REG_NOMATCH),
    };

    // iterate until satisfaction or failure
    loop {
        // try this midpoint on for size
        if longest_sub(mcx, v, g, right, mid, end, None)? == Some(end) {
            let mut er = cdissect(mcx, v, g, left, begin, mid)?;
            if er == REG_OKAY {
                er = cdissect(mcx, v, g, right, mid, end)?;
                if er == REG_OKAY {
                    // satisfaction
                    return Ok(REG_OKAY);
                }
                // Reset left's matches (right should have done so itself)
                zaptreesubs(v, g, left);
            }
            if er != REG_NOMATCH {
                return Ok(er);
            }
        }

        // that midpoint didn't work, find a new one
        if mid == begin {
            // all possibilities exhausted
            return Ok(REG_NOMATCH);
        }
        mid = match longest_sub(mcx, v, g, left, begin, mid - 1, None)? {
            Some(m) => m,
            None => {
                // failed to find a new one
                return Ok(REG_NOMATCH);
            }
        };
    }
}

// -----------------------------------------------------------------------------
// crevcondissect - dissect match for concatenation node, shortest-first
// -----------------------------------------------------------------------------

/// crevcondissect - dissect match for concatenation node, shortest-first.
///
/// C: `static int crevcondissect(struct vars *v, struct subre *t, chr *begin,
/// chr *end)`. Same shape as [`ccondissect`] but `left` prefers SHORTER, so the
/// tentative midpoint is found via [`shortest`] and is grown by one (`mid + 1`)
/// on retry; the exhaustion test is `mid == end`.
fn crevcondissect<'mcx>(
    mcx: Mcx<'mcx>,
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

    getsubdfa(mcx, v, &g.tree_nodes[left.0 as usize])?;
    getsubdfa(mcx, v, &g.tree_nodes[right.0 as usize])?;

    // pick a tentative midpoint
    let mut mid = match shortest_sub(mcx, v, g, left, begin, begin, end, false)? {
        Some(m) => m,
        None => return Ok(REG_NOMATCH),
    };

    // iterate until satisfaction or failure
    loop {
        // try this midpoint on for size
        if longest_sub(mcx, v, g, right, mid, end, None)? == Some(end) {
            let mut er = cdissect(mcx, v, g, left, begin, mid)?;
            if er == REG_OKAY {
                er = cdissect(mcx, v, g, right, mid, end)?;
                if er == REG_OKAY {
                    // satisfaction
                    return Ok(REG_OKAY);
                }
                // Reset left's matches (right should have done so itself)
                zaptreesubs(v, g, left);
            }
            if er != REG_NOMATCH {
                return Ok(er);
            }
        }

        // that midpoint didn't work, find a new one
        if mid == end {
            // all possibilities exhausted
            return Ok(REG_NOMATCH);
        }
        mid = match shortest_sub(mcx, v, g, left, begin, mid + 1, end, false)? {
            Some(m) => m,
            None => {
                // failed to find a new one
                return Ok(REG_NOMATCH);
            }
        };
    }
}

// -----------------------------------------------------------------------------
// cbrdissect - dissect match for backref node  (regexec.c)
// -----------------------------------------------------------------------------

/// cbrdissect - dissect match for backref node.
///
/// C: `static int cbrdissect(struct vars *v, struct subre *t, chr *begin,
/// chr *end)`. The backref match might already have been verified by
/// [`dfa_backref`], but we must check it here against `g.compare`.
fn cbrdissect(v: &mut ExecVars, g: &Guts, t: NodeId, begin: usize, end: usize) -> RegResult<i32> {
    let id = t.0 as usize;
    let n = g.tree_nodes[id].backno;
    let min = g.tree_nodes[id].min as i32;
    let max = g.tree_nodes[id].max as i32;

    debug_assert!(g.tree_nodes[id].op == b'b');
    debug_assert!(n >= 0);
    debug_assert!((n as usize) < v.nmatch);

    // get the backreferenced string
    if v.pmatch[n as usize].0 == -1 {
        return Ok(REG_NOMATCH);
    }
    let br_so = v.pmatch[n as usize].0 as usize;
    let br_eo = v.pmatch[n as usize].1 as usize;
    let brstring = v.start + br_so;
    let brlen = br_eo - br_so;

    // special cases for zero-length strings
    if brlen == 0 {
        // matches only if target is zero length, but any number of repetitions
        // can be considered to be present
        if begin == end && min <= max {
            return Ok(REG_OKAY);
        }
        return Ok(REG_NOMATCH);
    }
    if begin == end {
        // matches only if zero repetitions are okay
        if min == 0 {
            return Ok(REG_OKAY);
        }
        return Ok(REG_NOMATCH);
    }

    // check target length to see if it could possibly be an allowed number of
    // repetitions of brstring
    debug_assert!(end > begin);
    let tlen = end - begin;
    if !tlen.is_multiple_of(brlen) {
        return Ok(REG_NOMATCH);
    }
    let mut numreps = tlen / brlen;
    // C: numreps < min || (numreps > max && max != DUPINF), with min/max as int
    // promoted to size_t. numreps/min/max are all >= 0 here.
    if numreps < min as usize || (numreps > max as usize && max != DUPINF) {
        return Ok(REG_NOMATCH);
    }

    // okay, compare the actual string contents
    let compare = g.compare.expect("cbrdissect: g.compare is None");
    let mut p = begin;
    // while (numreps-- > 0)
    while numreps > 0 {
        numreps -= 1;
        if compare(&v.input[brstring..], &v.input[p..], brlen) != 0 {
            return Ok(REG_NOMATCH);
        }
        p += brlen;
    }

    Ok(REG_OKAY)
}

// -----------------------------------------------------------------------------
// caltdissect - dissect match for alternation node  (regexec.c)
// -----------------------------------------------------------------------------

/// caltdissect - dissect match for alternation node.
///
/// C: `static int caltdissect(struct vars *v, struct subre *t, chr *begin,
/// chr *end)`. Walks the alternatives (`t->child` then each `sibling`); for each
/// whose DFA reaches exactly `end`, recurse via [`cdissect`] and return its
/// result unless it is `REG_NOMATCH`.
fn caltdissect<'mcx>(
    mcx: Mcx<'mcx>,
    v: &mut ExecVars,
    g: &Guts,
    t: NodeId,
    begin: usize,
    end: usize,
) -> RegResult<i32> {
    debug_assert!(g.tree_nodes[t.0 as usize].op == b'|');

    // t = t->child;
    let mut tcur = g.tree_nodes[t.0 as usize].child;
    // there should be at least 2 alternatives
    debug_assert!(tcur.is_some() && g.tree_nodes[tcur.unwrap().0 as usize].sibling.is_some());

    while let Some(node) = tcur {
        debug_assert!(
            g.tree_nodes[node.0 as usize]
                .cnfa
                .as_ref()
                .map(|c| c.nstates > 0)
                .unwrap_or(false)
        );

        getsubdfa(mcx, v, &g.tree_nodes[node.0 as usize])?;
        if longest_sub(mcx, v, g, node, begin, end, None)? == Some(end) {
            let er = cdissect(mcx, v, g, node, begin, end)?;
            if er != REG_NOMATCH {
                return Ok(er);
            }
        }

        tcur = g.tree_nodes[node.0 as usize].sibling;
    }

    Ok(REG_NOMATCH)
}

// -----------------------------------------------------------------------------
// citerdissect - dissect match for iteration node  (regexec.c)
// -----------------------------------------------------------------------------

/// citerdissect - dissect match for iteration node.
///
/// C: `static int citerdissect(struct vars *v, struct subre *t, chr *begin,
/// chr *end)`. Finds a set of sub-match endpoints valid per the child DFA
/// (longest-first), then recursively dissects each to confirm validity,
/// backtracking on failure. The C `endpts` MALLOC array becomes a `Vec`; the
/// `goto backtrack` becomes a labeled inner `while` loop.
fn citerdissect<'mcx>(
    mcx: Mcx<'mcx>,
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

    // For the moment, assume the minimum number of matches is 1. (Zero-matches
    // case is handled at the bottom.)
    let mut min_matches = t_min;
    if min_matches <= 0 {
        min_matches = 1;
    }

    // workspace to track the endpoints of each sub-match.
    let mut max_matches: usize = end - begin;
    if max_matches > t_max as usize && t_max != DUPINF {
        max_matches = t_max as usize;
    }
    if max_matches < min_matches as usize {
        max_matches = min_matches as usize;
    }
    // endpts[0] == begin; sub-match endpoints in endpts[1..max_matches].
    let mut endpts: Vec<usize> = fill_vec(max_matches + 1, 0usize)?;
    endpts[0] = begin;

    getsubdfa(mcx, v, &g.tree_nodes[child.0 as usize])?;

    // initialize to consider first sub-match
    let mut nverified: i32 = 0;
    let mut k: i32 = 1;
    let mut limit = end;

    // iterate until satisfaction or failure
    'outer: while k > 0 {
        // try to find an endpoint for the k'th sub-match
        let ep = longest_sub(mcx, v, g, child, endpts[(k - 1) as usize], limit, None)?;
        match ep {
            None => {
                // no match possible, so see if we can shorten previous one
                k -= 1;
                // goto backtrack
            }
            Some(ep) => {
                endpts[k as usize] = ep;

                // k'th sub-match can no longer be considered verified
                if nverified >= k {
                    nverified = k - 1;
                }

                if endpts[k as usize] != end {
                    // haven't reached end yet, try another iteration if allowed
                    if k >= max_matches as i32 {
                        // must try to shorten some previous match
                        k -= 1;
                        // goto backtrack
                    } else if endpts[k as usize] == endpts[(k - 1) as usize]
                        && (k >= min_matches
                            || ((min_matches - k) as i64) < (end - endpts[k as usize]) as i64)
                    {
                        // reject zero-length match unless necessary to achieve min
                        // goto backtrack
                    } else {
                        k += 1;
                        limit = end;
                        continue 'outer;
                    }
                } else if k < min_matches {
                    // reached end but too few matches
                    // goto backtrack
                } else {
                    // We've identified a way to divide the string into k
                    // sub-matches that works so far as the child DFA can tell.
                    // Recurse to verify each sub-match.
                    let mut i = nverified + 1;
                    while i <= k {
                        // zap any match data from a non-last iteration
                        zaptreesubs(v, g, child);
                        let er = cdissect(
                            mcx,
                            v,
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
                        // oops, something failed
                        return Ok(er);
                    }

                    if i > k {
                        // satisfaction
                        return Ok(REG_OKAY);
                    }

                    // i'th match failed to verify, so backtrack it
                    k = i;
                    // fall through to backtrack
                }
            }
        }

        // backtrack:
        // Must consider shorter versions of the k'th sub-match. However, we'll
        // only ask for a zero-length match if necessary.
        while k > 0 {
            let prev_end = endpts[(k - 1) as usize];
            if endpts[k as usize] > prev_end {
                limit = endpts[k as usize] - 1;
                if limit > prev_end
                    || (k < min_matches && (min_matches - k) as i64 >= (end - prev_end) as i64)
                {
                    // break out of backtrack loop, continue the outer one
                    break;
                }
            }
            // can't shorten k'th sub-match any more, consider previous one
            k -= 1;
        }
    }

    // all possibilities exhausted

    // Now consider the possibility that we can match to a zero-length string
    // by using zero repetitions.
    if t_min == 0 && begin == end {
        return Ok(REG_OKAY);
    }

    Ok(REG_NOMATCH)
}

// -----------------------------------------------------------------------------
// creviterdissect - dissect match for iteration node, shortest-first
// -----------------------------------------------------------------------------

/// creviterdissect - dissect match for iteration node, shortest-first.
///
/// C: `static int creviterdissect(struct vars *v, struct subre *t, chr *begin,
/// chr *end)`. Same strategy as [`citerdissect`] but endpoints are found
/// shortest-first (via [`shortest`]) and grown (`endpts[k] + 1`) on backtrack
/// instead of shrunk.
fn creviterdissect<'mcx>(
    mcx: Mcx<'mcx>,
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

    // If zero matches are allowed and the target string is empty, just declare
    // victory. Otherwise pretend the min is 1.
    let mut min_matches = t_min;
    if min_matches <= 0 {
        if begin == end {
            return Ok(REG_OKAY);
        }
        min_matches = 1;
    }

    // workspace to track the endpoints of each sub-match.
    let mut max_matches: usize = end - begin;
    if max_matches > t_max as usize && t_max != DUPINF {
        max_matches = t_max as usize;
    }
    if max_matches < min_matches as usize {
        max_matches = min_matches as usize;
    }
    let mut endpts: Vec<usize> = fill_vec(max_matches + 1, 0usize)?;
    endpts[0] = begin;

    getsubdfa(mcx, v, &g.tree_nodes[child.0 as usize])?;

    // initialize to consider first sub-match
    let mut nverified: i32 = 0;
    let mut k: i32 = 1;
    let mut limit = begin;

    // iterate until satisfaction or failure
    'outer: while k > 0 {
        // disallow zero-length match unless necessary to achieve min
        if limit == endpts[(k - 1) as usize]
            && limit != end
            && (k >= min_matches || ((min_matches - k) as i64) < (end - limit) as i64)
        {
            limit += 1;
        }

        // if this is the last allowed sub-match, it must reach to the end
        if k >= max_matches as i32 {
            limit = end;
        }

        // try to find an endpoint for the k'th sub-match
        let ep = shortest_sub(mcx, v, g, child, endpts[(k - 1) as usize], limit, end, false)?;
        match ep {
            None => {
                // no match possible, so see if we can lengthen previous one
                k -= 1;
                // goto backtrack
            }
            Some(ep) => {
                endpts[k as usize] = ep;

                // k'th sub-match can no longer be considered verified
                if nverified >= k {
                    nverified = k - 1;
                }

                if endpts[k as usize] != end {
                    // haven't reached end yet, try another iteration if allowed
                    if k >= max_matches as i32 {
                        // must try to lengthen some previous match
                        k -= 1;
                        // goto backtrack
                    } else {
                        k += 1;
                        limit = endpts[(k - 1) as usize];
                        continue 'outer;
                    }
                } else if k < min_matches {
                    // reached end but too few matches
                    // goto backtrack
                } else {
                    // verify each sub-match
                    let mut i = nverified + 1;
                    while i <= k {
                        // zap any match data from a non-last iteration
                        zaptreesubs(v, g, child);
                        let er = cdissect(
                            mcx,
                            v,
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
                        // oops, something failed
                        return Ok(er);
                    }

                    if i > k {
                        // satisfaction
                        return Ok(REG_OKAY);
                    }

                    // i'th match failed to verify, so backtrack it
                    k = i;
                    // fall through to backtrack
                }
            }
        }

        // backtrack:
        // Must consider longer versions of the k'th sub-match.
        while k > 0 {
            if endpts[k as usize] < end {
                limit = endpts[k as usize] + 1;
                // break out of backtrack loop, continue the outer one
                break;
            }
            // can't lengthen k'th sub-match any more, consider previous one
            k -= 1;
        }
    }

    // all possibilities exhausted
    Ok(REG_NOMATCH)
}

// =============================================================================
// regprefix.c — fixed-prefix extraction
// =============================================================================

/// The outcome of `findprefix`: which `REG_*` code it returns plus the prefix
/// chrs it accumulated. C returns the code and writes `*string`/`*slen`.
pub struct PrefixResult {
    /// `REG_NOMATCH` / `REG_PREFIX` / `REG_EXACT` (or an error code).
    pub code: i32,
    /// the extracted prefix chrs (empty for `REG_NOMATCH`).
    pub prefix: alloc::vec::Vec<chr>,
}

/// `pg_regprefix(regex_t *re, chr **string, size_t *slen)` -- extract a fixed
/// prefix common to all matches. The prefix chrs are allocated in `mcx` (C:
/// palloc in the caller's current context).
///
/// This family's surface takes the already-validated compiled `guts` directly
/// (the seam adapter in `regex_export_free_error` does the `re_magic`/`re_csize`
/// validation and `pg_set_regex_collation`).
///
/// Returns a [`PrefixResult`] whose `code` is `REG_NOMATCH`/`REG_PREFIX`/
/// `REG_EXACT`; only PREFIX/EXACT carry a non-empty prefix. Real internal
/// failures surface as `Err`.
pub fn pg_regprefix<'mcx>(_mcx: Mcx<'mcx>, guts: &Guts) -> RegResult<PrefixResult> {
    if (guts.info & REG_UIMPOSSIBLE as i64) != 0 {
        return Ok(PrefixResult {
            code: REG_NOMATCH,
            prefix: Vec::new(),
        });
    }

    // This implementation considers only the search NFA for the topmost regex
    // tree node. Therefore, constraints such as backrefs are not fully applied,
    // which is allowed per the function's API spec.
    let troot = guts.tree.expect("pg_regprefix: tree root present");
    // C: cnfa = &g->tree->cnfa;  (the embedded cnfa is `Option<Cnfa>` here).
    let cnfa: &Cnfa = guts.tree_nodes[troot.0 as usize]
        .cnfa
        .as_ref()
        .expect("pg_regprefix: tree root has a cnfa");

    // matchall NFAs never have a fixed prefix
    if (cnfa.flags & MATCHALL) != 0 {
        return Ok(PrefixResult {
            code: REG_NOMATCH,
            prefix: Vec::new(),
        });
    }

    // Since a correct NFA should never contain any exit-free loops, it should
    // not be possible for our traversal to return to a previously visited NFA
    // state. Hence we need at most nstates chrs in the output string.
    let mut string: Vec<chr> = Vec::new();
    string.try_reserve(cnfa.nstates as usize)?;

    // do it
    let res = findprefix(cnfa, &guts.cmap)?;

    debug_assert!(res.prefix.len() <= cnfa.nstates as usize);
    string = res.prefix;

    // clean up: only PREFIX/EXACT keep the prefix; everything else returns the
    // code with an empty prefix (C frees *string and sets *slength = 0).
    match res.code {
        x if x == REG_PREFIX || x == REG_EXACT => Ok(PrefixResult { code: x, prefix: string }),
        other => Ok(PrefixResult {
            code: other,
            prefix: Vec::new(),
        }),
    }
}

/// `findprefix(struct cnfa *cnfa, struct colormap *cm, chr *string, size_t
/// *slen)` -- walk the search cNFA accumulating the forced prefix.
///
/// Results are appended to the [`PrefixResult`] prefix; C's `*slength` (preset
/// to zero) is here the prefix `len()`. Returns the `regprefix` return code
/// (`REG_PREFIX`/`REG_EXACT`/`REG_NOMATCH`).
///
/// PORT NOTE: C iterates each state's out-arc list `for (ca = cnfa->states[st];
/// ca->co != COLORLESS; ca++)`; the [`Cnfa`] arena stores `states[st]` as the
/// half-open range of REAL arcs (the trailing `COLORLESS` terminator is
/// excluded from the range), so the loops iterate that range directly.
pub fn findprefix(cnfa: &Cnfa, cm: &ColorMap) -> RegResult<PrefixResult> {
    let mut string: Vec<chr> = Vec::new();

    // The "pre" state must have only BOS/BOL outarcs, else pattern isn't
    // anchored left. If we have both BOS and BOL, they must go to the same next
    // state.
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

    // Scan through successive states, stopping as soon as we find one with more
    // than one acceptable transition character.
    loop {
        st = nextst;
        nextst = -1;
        let mut thiscolor: color = COLORLESS;
        for ai in cnfa.states[st as usize].clone() {
            let ca = cnfa.arcs[ai];
            // We can ignore BOS/BOL arcs
            if ca.co == cnfa.bos[0] || ca.co == cnfa.bos[1] {
                continue;
            }
            // ... but EOS/EOL arcs terminate the search, as do RAINBOW arcs and
            // LACONs
            if ca.co == cnfa.eos[0]
                || ca.co == cnfa.eos[1]
                || ca.co == RAINBOW
                || (ca.co as i32) >= cnfa.ncolors
            {
                thiscolor = COLORLESS;
                break;
            }
            if thiscolor == COLORLESS {
                // First plain outarc
                thiscolor = ca.co;
                nextst = ca.to;
            } else if thiscolor == ca.co {
                // Another plain outarc for same color
                nextst = -1;
            } else {
                // More than one plain outarc color terminates the search
                thiscolor = COLORLESS;
                break;
            }
        }
        // Done if we didn't find exactly one color on plain outarcs
        if thiscolor == COLORLESS {
            break;
        }
        // The color must be a singleton
        if cm.cd[thiscolor as usize].nschrs != 1 {
            break;
        }
        // Must not have any high-color-map entries
        if cm.cd[thiscolor as usize].nuchrs != 0 {
            break;
        }

        // Identify the color's sole member chr and add it to the prefix string.
        // For the cases we care about it should be sufficient to test the
        // "firstchr" value. If we hit a corner case where firstchr is no longer
        // a member of the color, we just fall out without adding anything.
        let c = cm.cd[thiscolor as usize].firstchr;
        if getcolor(cm, c) != thiscolor {
            break;
        }

        string.push(c);

        // Advance to next state, but only if we have a unique next state
        if nextst == -1 {
            break;
        }
    }

    // If we ended at a state that only has EOS/EOL outarcs leading to the
    // "post" state, then we have an exact-match string. Note this is true even
    // if the string is of zero length.
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

    // Otherwise, if we were unable to identify any prefix characters, say
    // NOMATCH --- the pattern is anchored left, but doesn't specify any
    // particular first character.
    if !string.is_empty() {
        return Ok(PrefixResult { code: REG_PREFIX, prefix: string });
    }

    Ok(PrefixResult { code: REG_NOMATCH, prefix: string })
}
