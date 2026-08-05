
use ::mcx::Mcx;

use crate::regex_error::RegResult;
use crate::regex_foundation::freecolor;
use crate::regguts::{
    color, ArcId, ColorDesc, ColorMap, Nfa, StateId, CANTMATCH, COLMARK, COLORLESS, FREECOL, NOSUB,
    PLAIN, PSEUDO, RAINBOW,
};


#[inline]
pub(crate) fn unusedcolor(cd: &ColorDesc) -> bool {
    (cd.flags & FREECOL) != 0
}


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


pub fn okcolors<'mcx>(
    mcx: Mcx<'mcx>,
    nfa: &mut Nfa,
    cm: &mut ColorMap,
    has_parent: bool,
) -> RegResult<()> {
    for co in 0..=(cm.max as color) {
        let sco = cm.cd[co as usize].sub;
        if unusedcolor(&cm.cd[co as usize]) || sco == NOSUB {
        } else if sco == co {
        } else if cm.cd[co as usize].nschrs == 0 && cm.cd[co as usize].nuchrs == 0 {
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
            cm.cd[co as usize].sub = NOSUB;
            debug_assert!(cm.cd[sco as usize].nschrs > 0 || cm.cd[sco as usize].nuchrs > 0);
            debug_assert_eq!(cm.cd[sco as usize].sub, sco);
            cm.cd[sco as usize].sub = NOSUB;
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

    for co in 0..=(cm.max as color) {
        let cd = cm.cd[co as usize];
        if !unusedcolor(&cd) && cd.sub != co && co != but && (cd.flags & PSEUDO) == 0 {
            super::newarc(mcx, nfa, cm, has_parent, type_, co, from, to)?;
        }
    }
    Ok(())
}

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

    if super::findarc(nfa, of, PLAIN, RAINBOW).is_some() {
        super::newarc(mcx, nfa, cm, has_parent, CANTMATCH, 0, from, to)?;
        super::set_hascantmatch(nfa);
        return Ok(());
    }

    let mut cur = nfa.state_arena[of.0 as usize].outs;
    while let Some(a) = cur {
        let arc = nfa.arc_arena[a.0 as usize];
        if arc.type_ == PLAIN {
            debug_assert!(arc.co >= 0);
            debug_assert!(!unusedcolor(&cm.cd[arc.co as usize]));
            cm.cd[arc.co as usize].flags |= COLMARK;
        }
        debug_assert_ne!(arc.type_, CANTMATCH);
        cur = arc.outchain;
    }

    for co in 0..=(cm.max as color) {
        if (cm.cd[co as usize].flags & COLMARK) != 0 {
            cm.cd[co as usize].flags &= !COLMARK;
        } else if !unusedcolor(&cm.cd[co as usize]) && (cm.cd[co as usize].flags & PSEUDO) == 0 {
            super::newarc(mcx, nfa, cm, has_parent, type_, co, from, to)?;
        }
    }
    Ok(())
}
