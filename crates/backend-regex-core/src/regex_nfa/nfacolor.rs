//! The NFA front's slice of the color machinery (`regc_color.c`).
//!
//! Faithful Rust port of the `regc_color.c` functions that manipulate the
//! **NFA arc arena** and its per-color arc chains:
//! `colorchain`/`uncolorchain`/`okcolors`/`rainbow`/`colorcomplement`. In the C
//! source these live alongside the rest of the colormap engine in
//! `regc_color.c`, but they reach into the NFA (`struct nfa`'s arc arena and
//! color chains), so this decomposition assigns them to the NFA family
//! ([`crate::regex_nfa`]).
//!
//! The colormap *allocators* these functions call -- `maxcolor`, `pseudocolor`,
//! `newcolor`, `freecolor` -- are owned by the [`crate::regex_foundation`]
//! family (`regc_color.c`'s colormap engine), so calls to them are routed to
//! that owner; until that family lands its `todo!()` bodies panic loudly,
//! exactly as the per-owner seam contract requires.
//!
//! # Colormap / parent
//!
//! The C `struct nfa` carries `cm` (colormap) and `parent` pointers; the
//! idiomatic [`Nfa`] omits both. Functions that touch the colormap take it as an
//! explicit `&mut ColorMap` argument, and the parent flag as `has_parent: bool`.
//! Color-chain bookkeeping in `newarc`/`createarc`/`freearc` runs exactly when
//! `COLORED(a) && !has_parent`, matching C's `COLORED(a) && nfa->parent == NULL`.

use mcx::Mcx;

use crate::regex_error::RegResult;
use crate::regex_foundation::freecolor;
use crate::regguts::{
    color, ArcId, ColorDesc, ColorMap, Nfa, StateId, CANTMATCH, COLMARK, COLORLESS, FREECOL, NOSUB,
    PLAIN, PSEUDO, RAINBOW,
};

// =============================================================================
// internal colordesc helper (replacing the C `UNUSEDCOLOR` macro)
// =============================================================================

/// `UNUSEDCOLOR(cd)` -- true iff the color is currently free.
#[inline]
pub(crate) fn unusedcolor(cd: &ColorDesc) -> bool {
    (cd.flags & FREECOL) != 0
}

// =============================================================================
// colorchain / uncolorchain  (NFA arc color chains)
// =============================================================================

/// colorchain - add this arc to the color chain of its color.
///
/// C signature: `static void colorchain(struct colormap *cm, struct arc *a)`.
pub fn colorchain(nfa: &mut Nfa, cm: &mut ColorMap, a: ArcId) {
    let co = nfa.arc_arena[a.0 as usize].co;
    debug_assert!(co >= 0);
    let head = cm.cd[co as usize].arcs;
    if let Some(h) = head {
        nfa.arc_arena[h.0 as usize].colorchainRev = Some(a);
    }
    {
        let arc = &mut nfa.arc_arena[a.0 as usize];
        arc.colorchain = head;
        arc.colorchainRev = None;
    }
    cm.cd[co as usize].arcs = Some(a);
}

/// uncolorchain - delete this arc from the color chain of its color.
///
/// C signature: `static void uncolorchain(struct colormap *cm, struct arc *a)`.
pub fn uncolorchain(nfa: &mut Nfa, cm: &mut ColorMap, a: ArcId) {
    let co = nfa.arc_arena[a.0 as usize].co;
    debug_assert!(co >= 0);
    let aa = nfa.arc_arena[a.0 as usize].colorchainRev;
    let chain = nfa.arc_arena[a.0 as usize].colorchain;
    match aa {
        None => {
            debug_assert_eq!(cm.cd[co as usize].arcs, Some(a));
            cm.cd[co as usize].arcs = chain;
        }
        Some(p) => {
            debug_assert_eq!(nfa.arc_arena[p.0 as usize].colorchain, Some(a));
            nfa.arc_arena[p.0 as usize].colorchain = chain;
        }
    }
    if let Some(c) = chain {
        nfa.arc_arena[c.0 as usize].colorchainRev = aa;
    }
    let arc = &mut nfa.arc_arena[a.0 as usize];
    arc.colorchain = None; // paranoia
    arc.colorchainRev = None;
}

// =============================================================================
// okcolors  (promote subcolors to full colors)
// =============================================================================

/// okcolors - promote subcolors to full colors.
///
/// C signature: `static void okcolors(struct nfa *nfa, struct colormap *cm)`.
/// Walks colors 0..=cm.max. For each color with an open subcolor that is not
/// itself the subcolor:
///   * if the parent is now empty (nschrs == nuchrs == 0), relabel its arcs to
///     the subcolor and free the parent;
///   * otherwise, add parallel subcolor arcs alongside the parent's arcs.
///
/// `has_parent` controls colorchain bookkeeping in the `newarc` calls (false for
/// a top-level NFA). Relabeling uses [`uncolorchain`]/[`colorchain`] directly.
/// `freecolor` is owned by [`crate::regex_foundation`].
pub fn okcolors<'mcx>(
    mcx: Mcx<'mcx>,
    nfa: &mut Nfa,
    cm: &mut ColorMap,
    has_parent: bool,
) -> RegResult<()> {
    // Iterate co = 0 .. cm.max inclusive (C: cd <= CDEND(cm), i.e. &cd[max]).
    for co in 0..=(cm.max as color) {
        let sco = cm.cd[co as usize].sub;
        if unusedcolor(&cm.cd[co as usize]) || sco == NOSUB {
            // has no subcolor, no further action
        } else if sco == co {
            // is subcolor, let parent deal with it
        } else if cm.cd[co as usize].nschrs == 0 && cm.cd[co as usize].nuchrs == 0 {
            // Parent is now empty: relabel all its arcs to the subcolor, then free
            // the parent.
            cm.cd[co as usize].sub = NOSUB;
            debug_assert!(cm.cd[sco as usize].nschrs > 0 || cm.cd[sco as usize].nuchrs > 0);
            debug_assert_eq!(cm.cd[sco as usize].sub, sco);
            cm.cd[sco as usize].sub = NOSUB;
            while let Some(a) = cm.cd[co as usize].arcs {
                debug_assert_eq!(nfa.arc_arena[a.0 as usize].co, co);
                uncolorchain(nfa, cm, a);
                nfa.arc_arena[a.0 as usize].co = sco;
                colorchain(nfa, cm, a);
            }
            freecolor(cm, co);
        } else {
            // Parent's arcs must gain parallel subcolor arcs.
            cm.cd[co as usize].sub = NOSUB;
            debug_assert!(cm.cd[sco as usize].nschrs > 0 || cm.cd[sco as usize].nuchrs > 0);
            debug_assert_eq!(cm.cd[sco as usize].sub, sco);
            cm.cd[sco as usize].sub = NOSUB;
            // Walk the parent's color chain, snapshotting the next link before each
            // newarc (newarc may colorchain the new sco arc onto sco's chain, never
            // co's, but we snapshot defensively as C reads a->colorchain after).
            let mut cur = cm.cd[co as usize].arcs;
            while let Some(a) = cur {
                debug_assert_eq!(nfa.arc_arena[a.0 as usize].co, co);
                let next = nfa.arc_arena[a.0 as usize].colorchain;
                let (t, from, to) = (
                    nfa.arc_arena[a.0 as usize].type_,
                    nfa.arc_arena[a.0 as usize].from.unwrap(),
                    nfa.arc_arena[a.0 as usize].to.unwrap(),
                );
                super::newarc(mcx, nfa, cm, has_parent, t, sco, from, to)?;
                cur = next;
            }
        }
    }
    Ok(())
}

// =============================================================================
// rainbow / colorcomplement  (NFA arc generators)
// =============================================================================

/// rainbow - add arcs of all full colors (but one) between specified states.
///
/// C signature: `static void rainbow(struct nfa *nfa, struct colormap *cm, int
/// type, color but, struct state *from, struct state *to)`. With no exception
/// color, a single RAINBOW-labeled arc is generated; otherwise one arc per
/// eligible full color (skipping subcolors, pseudocolors, and `but`).
#[allow(clippy::too_many_arguments)]
pub fn rainbow<'mcx>(
    mcx: Mcx<'mcx>,
    nfa: &mut Nfa,
    cm: &mut ColorMap,
    has_parent: bool,
    type_: i32,
    but: color,
    from: StateId,
    to: StateId,
) -> RegResult<()> {
    if but == COLORLESS {
        super::newarc(mcx, nfa, cm, has_parent, type_, RAINBOW, from, to)?;
        return Ok(());
    }

    // Gotta do it the hard way. Skip subcolors, pseudocolors, and "but".
    for co in 0..=(cm.max as color) {
        let cd = cm.cd[co as usize];
        if !unusedcolor(&cd) && cd.sub != co && co != but && (cd.flags & PSEUDO) == 0 {
            super::newarc(mcx, nfa, cm, has_parent, type_, co, from, to)?;
        }
    }
    Ok(())
}

/// colorcomplement - add arcs of complementary colors.
///
/// C signature: `static void colorcomplement(struct nfa *nfa, struct colormap
/// *cm, int type, struct state *of, struct state *from, struct state *to)`. Adds
/// arcs of all colors that are not pseudocolors and do not match any of `of`'s
/// PLAIN outarcs. If `of` has a RAINBOW out-arc the complement is empty, so a
/// CANTMATCH arc is made and the HASCANTMATCH flag set.
#[allow(clippy::too_many_arguments)]
pub fn colorcomplement<'mcx>(
    mcx: Mcx<'mcx>,
    nfa: &mut Nfa,
    cm: &mut ColorMap,
    has_parent: bool,
    type_: i32,
    of: StateId,
    from: StateId,
    to: StateId,
) -> RegResult<()> {
    debug_assert_ne!(of, from);

    // A RAINBOW arc matches all colors, making the complement empty. Make a
    // CANTMATCH arc to keep the NFA connected, and set HASCANTMATCH.
    if super::findarc(nfa, of, PLAIN, RAINBOW).is_some() {
        super::newarc(mcx, nfa, cm, has_parent, CANTMATCH, 0, from, to)?;
        super::set_hascantmatch(nfa);
        return Ok(());
    }

    // Otherwise, transiently mark the colors that appear in of's out-arcs.
    let mut cur = nfa.state_arena[of.0 as usize].outs;
    while let Some(a) = cur {
        let arc = nfa.arc_arena[a.0 as usize];
        if arc.type_ == PLAIN {
            debug_assert!(arc.co >= 0);
            debug_assert!(!unusedcolor(&cm.cd[arc.co as usize]));
            cm.cd[arc.co as usize].flags |= COLMARK;
        }
        // There's no syntax for re-complementing a color set, so we cannot see
        // CANTMATCH arcs here.
        debug_assert_ne!(arc.type_, CANTMATCH);
        cur = arc.outchain;
    }

    // Scan colors, clear transient marks, add arcs for unmarked colors.
    for co in 0..=(cm.max as color) {
        if (cm.cd[co as usize].flags & COLMARK) != 0 {
            cm.cd[co as usize].flags &= !COLMARK;
        } else if !unusedcolor(&cm.cd[co as usize]) && (cm.cd[co as usize].flags & PSEUDO) == 0 {
            super::newarc(mcx, nfa, cm, has_parent, type_, co, from, to)?;
        }
    }
    Ok(())
}
