//! trgm_regexp.c: transform a regex into a set of required trigrams plus a
//! packed graph evaluated per index entry (createTrgmNFA /
//! trigramsMatchGraph). States live in a Vec, shared by u32 id where C
//! shares TrgmState pointers; the dynahash is an FxHash map keyed by
//! (prefix, nstate). IGNORECASE (trgm.h) is always on: the regex compiles
//! with REG_ICASE and uppercase color members are dropped.

use std::collections::HashMap;

use gin_vocab::{TrgmPackedArc, TrgmPackedGraph};
use mcx::MemoryContext;
use regex::{RegcompResult, REG_ADVANCED, REG_ICASE, REG_NOSUB};
use regex_core::regex_export_free_error as rex;
use regex_core::regex_export_free_error::RegexArc;
use regex_core::regguts::RegexT;
use rustc_hash::FxBuildHasher;
use types_core::Oid;
use types_error::{PgError, PgResult, ERRCODE_INVALID_REGULAR_EXPRESSION};

use crate::trgm::{compact_trigram, Trgm, TrgmEnv};

const MAX_EXPANDED_STATES: usize = 128;
const MAX_EXPANDED_ARCS: i32 = 1024;
const MAX_TRGM_COUNT: i64 = 256;
const WISH_TRGM_PENALTY: f32 = 16.0;
const COLOR_COUNT_LIMIT: i32 = 256;

// Penalty multipliers by whitespace shape ("aaa".."   ").
const PENALTIES: [f32; 8] = [1.0, 3.5, 0.0, 0.0, 4.2, 2.1, 25.0, 0.0];

const MAX_MULTIBYTE_CHAR_LEN: usize = 4;

type TrgmColor = i32;
const COLOR_UNKNOWN: TrgmColor = -3;
const COLOR_BLANK: TrgmColor = -4;

type MbChar = [u8; MAX_MULTIBYTE_CHAR_LEN];
type ColorTrgm = [TrgmColor; 3];

struct ColorInfo {
    expandable: bool,
    contains_non_word: bool,
    word_chars: Vec<MbChar>,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct StateKey {
    prefix: [TrgmColor; 2],
    nstate: i32,
}

const TSTATE_INIT: u32 = 0x01;
const TSTATE_FIN: u32 = 0x02;

struct State {
    key: StateKey,
    arcs: Vec<(ColorTrgm, u32)>,
    enter_keys: Vec<StateKey>,
    flags: u32,
    snumber: i32,
    parent: Option<u32>,
    tent_flags: u32,
    tent_parent: Option<u32>,
}

struct ColorTrgmInfo {
    ctrgm: ColorTrgm,
    cnumber: i32,
    count: i32,
    penalty: f32,
    expanded: bool,
    arcs: Vec<(u32, u32)>, // (source, target) state ids
}

struct Nfa<'a> {
    regex: &'a RegexT,
    colors: Vec<ColorInfo>,
    map: HashMap<StateKey, u32, FxBuildHasher>,
    states: Vec<State>,
    init_state: u32,
    queue: Vec<u32>,
    keys_queue: Vec<StateKey>,
    arcs_count: i32,
    overflowed: bool,
}

/// createTrgmNFA. `None` = the regex is too complex / trivial to extract
/// index trigrams from (C's NULL return): fall back to a full scan.
pub fn create_trgm_nfa(
    pattern: &[u8],
    collation: Oid,
    env: &TrgmEnv<'_>,
    legacy_crc32: &dyn Fn(&[u8]) -> u32,
) -> PgResult<Option<(Vec<Trgm>, TrgmPackedGraph)>> {
    let scratch = MemoryContext::new("createTrgmNFA temporary context");
    let wide = mbutils::pg_mb2wchar_with_len(scratch.mcx(), pattern)?;
    let compiled =
        match rex::seam_pg_regcomp(&wide, REG_ADVANCED | REG_NOSUB | REG_ICASE, collation)? {
            RegcompResult::Compiled(c) => c,
            RegcompResult::Failed(f) => {
                return Err(PgError::error(format!(
                    "invalid regular expression: {}",
                    f.message
                ))
                .with_sqlstate(ERRCODE_INVALID_REGULAR_EXPRESSION)
                .into())
            }
        };
    drop(wide);
    let regex: &RegexT = compiled
        .engine
        .downcast_ref::<RegexT>()
        .expect("pg_trgm: RegexCompiled engine is not a RegexT");

    let colors = get_color_info(regex, env, &scratch)?;

    let mut nfa = Nfa {
        regex,
        colors,
        map: HashMap::with_hasher(FxBuildHasher),
        states: Vec::new(),
        init_state: 0,
        queue: Vec::new(),
        keys_queue: Vec::new(),
        arcs_count: 0,
        overflowed: false,
    };

    nfa.transform_graph();

    // A trivial graph (final state reachable without any predictable
    // trigram) is useless for the index.
    if nfa.states[nfa.init_state as usize].flags & TSTATE_FIN != 0 {
        return Ok(None);
    }

    let Some((mut ctrgms, total_count)) = nfa.select_color_trigrams() else {
        return Ok(None);
    };

    let trigrams = nfa.expand_color_trigrams(&ctrgms, total_count, legacy_crc32);
    let graph = nfa.pack_graph(&mut ctrgms);
    Ok(Some((trigrams, graph)))
}

fn get_color_info(
    regex: &RegexT,
    env: &TrgmEnv<'_>,
    scratch: &MemoryContext,
) -> PgResult<Vec<ColorInfo>> {
    let ncolors = rex::pg_reg_getnumcolors(regex);
    let mut colors = Vec::with_capacity(ncolors as usize);
    for co in 0..ncolors {
        let chars_count = rex::pg_reg_getnumcharacters(regex, co);
        if !(0..=COLOR_COUNT_LIMIT).contains(&chars_count) {
            colors.push(ColorInfo {
                expandable: false,
                contains_non_word: false,
                word_chars: Vec::new(),
            });
            continue;
        }
        let mut chars = vec![0 as types_core::PgWChar; chars_count as usize];
        rex::pg_reg_getcharacters(regex, co, &mut chars);
        let mut info = ColorInfo {
            expandable: true,
            contains_non_word: false,
            word_chars: Vec::with_capacity(chars_count as usize),
        };
        for &c in &chars {
            let Some(mb) = convert_pg_wchar(c, env, scratch)? else {
                continue;
            };
            let len = mb.iter().position(|&b| b == 0).unwrap_or(MAX_MULTIBYTE_CHAR_LEN);
            if (env.isalnum)(&mb[..len]) {
                info.word_chars.push(mb);
            } else {
                info.contains_non_word = true;
            }
        }
        colors.push(info);
    }
    Ok(colors)
}

// convertPgWchar: NUL is ignored; IGNORECASE drops chars that aren't their
// own str_tolower image (the lowercase twin is in the same color).
fn convert_pg_wchar(
    c: types_core::PgWChar,
    env: &TrgmEnv<'_>,
    scratch: &MemoryContext,
) -> PgResult<Option<MbChar>> {
    if c == 0 {
        return Ok(None);
    }
    let bytes = mbutils::pg_wchar2mb_with_len(scratch.mcx(), &[c])?;
    if bytes.is_empty() || bytes.len() > MAX_MULTIBYTE_CHAR_LEN {
        return Ok(None);
    }
    let lowered = (env.tolower)(&bytes);
    if lowered.as_slice() != bytes.as_slice() {
        return Ok(None);
    }
    let mut mb: MbChar = [0; MAX_MULTIBYTE_CHAR_LEN];
    mb[..bytes.len()].copy_from_slice(&bytes);
    Ok(Some(mb))
}

fn prefix_contains(p1: [TrgmColor; 2], p2: [TrgmColor; 2]) -> bool {
    if p1[1] == COLOR_UNKNOWN {
        true
    } else if p1[0] == COLOR_UNKNOWN {
        p1[1] == p2[1]
    } else {
        p1[0] == p2[0] && p1[1] == p2[1]
    }
}

fn valid_arc_label(key: &StateKey, co: TrgmColor) -> bool {
    if key.prefix[0] == COLOR_UNKNOWN {
        return false;
    }
    debug_assert!(key.prefix[1] != COLOR_UNKNOWN && co != COLOR_UNKNOWN);
    if key.prefix[0] == COLOR_BLANK && key.prefix[1] == COLOR_BLANK && co == COLOR_BLANK {
        return false;
    }
    // nonblank-blank-anything never matches an extracted trigram (RPADDING 1).
    if key.prefix[0] != COLOR_BLANK && key.prefix[1] == COLOR_BLANK {
        return false;
    }
    true
}

impl Nfa<'_> {
    fn out_arcs(&self, nstate: i32) -> Vec<RegexArc> {
        let n = rex::pg_reg_getnumoutarcs(self.regex, nstate);
        let mut arcs = vec![RegexArc { co: 0, to: 0 }; n as usize];
        rex::pg_reg_getoutarcs(self.regex, nstate, &mut arcs);
        arcs
    }

    fn get_state(&mut self, key: StateKey) -> u32 {
        if let Some(&id) = self.map.get(&key) {
            return id;
        }
        let id = self.states.len() as u32;
        self.states.push(State {
            key,
            arcs: Vec::new(),
            enter_keys: Vec::new(),
            flags: 0,
            snumber: -(id as i32 + 1),
            parent: None,
            tent_flags: 0,
            tent_parent: None,
        });
        self.map.insert(key, id);
        self.queue.push(id);
        id
    }

    fn transform_graph(&mut self) {
        let init_key = StateKey {
            prefix: [COLOR_UNKNOWN, COLOR_UNKNOWN],
            nstate: rex::pg_reg_getinitialstate(self.regex),
        };
        let init = self.get_state(init_key);
        self.states[init as usize].flags |= TSTATE_INIT;
        self.init_state = init;

        let mut qi = 0usize;
        while qi < self.queue.len() {
            let sid = self.queue[qi];
            qi += 1;
            if self.overflowed {
                self.states[sid as usize].flags |= TSTATE_FIN;
            } else {
                self.process_state(sid);
            }
            if self.arcs_count > MAX_EXPANDED_ARCS || self.states.len() > MAX_EXPANDED_STATES {
                self.overflowed = true;
            }
        }
    }

    fn process_state(&mut self, sid: u32) {
        self.keys_queue.clear();
        let own_key = self.states[sid as usize].key;
        self.add_key(sid, own_key);
        let mut ki = 0usize;
        while ki < self.keys_queue.len() {
            if self.states[sid as usize].flags & TSTATE_FIN != 0 {
                break;
            }
            let key = self.keys_queue[ki];
            ki += 1;
            self.add_key(sid, key);
        }
        if self.states[sid as usize].flags & TSTATE_FIN == 0 {
            self.add_arcs(sid);
        }
    }

    fn add_key(&mut self, sid: u32, key: StateKey) {
        {
            let state = &mut self.states[sid as usize];
            let mut i = 0usize;
            while i < state.enter_keys.len() {
                let existing = state.enter_keys[i];
                if existing.nstate == key.nstate {
                    if prefix_contains(existing.prefix, key.prefix) {
                        return;
                    }
                    if prefix_contains(key.prefix, existing.prefix) {
                        state.enter_keys.remove(i);
                        continue;
                    }
                }
                i += 1;
            }
            state.enter_keys.push(key);
        }

        if key.nstate == rex::pg_reg_getfinalstate(self.regex) {
            self.states[sid as usize].flags |= TSTATE_FIN;
            return;
        }

        for arc in self.out_arcs(key.nstate) {
            let dest = if rex::pg_reg_colorisbegin(self.regex, arc.co) {
                // ^ reads like a word start: all-blank prefix.
                Some([COLOR_BLANK, COLOR_BLANK])
            } else if rex::pg_reg_colorisend(self.regex, arc.co) {
                Some([COLOR_UNKNOWN, COLOR_UNKNOWN])
            } else if arc.co >= 0 {
                let info = &self.colors[arc.co as usize];
                let (expandable, contains_non_word, has_word_chars) =
                    (info.expandable, info.contains_non_word, !info.word_chars.is_empty());
                if expandable {
                    if contains_non_word && !valid_arc_label(&key, COLOR_BLANK) {
                        self.keys_queue.push(StateKey {
                            prefix: [COLOR_BLANK, COLOR_BLANK],
                            nstate: arc.to,
                        });
                    }
                    if has_word_chars && !valid_arc_label(&key, arc.co) {
                        self.keys_queue.push(StateKey {
                            prefix: [key.prefix[1], arc.co],
                            nstate: arc.to,
                        });
                    }
                    None
                } else {
                    Some([COLOR_UNKNOWN, COLOR_UNKNOWN])
                }
            } else {
                // RAINBOW: as unexpandable.
                Some([COLOR_UNKNOWN, COLOR_UNKNOWN])
            };
            if let Some(prefix) = dest {
                self.keys_queue.push(StateKey { prefix, nstate: arc.to });
            }
        }
    }

    fn add_arcs(&mut self, sid: u32) {
        let enter_keys = self.states[sid as usize].enter_keys.clone();
        for key in enter_keys {
            for arc in self.out_arcs(key.nstate) {
                if arc.co < 0 {
                    continue;
                }
                debug_assert!((arc.co as usize) < self.colors.len());
                let (expandable, contains_non_word, has_word_chars) = {
                    let info = &self.colors[arc.co as usize];
                    (info.expandable, info.contains_non_word, !info.word_chars.is_empty())
                };
                if !expandable {
                    continue;
                }
                if contains_non_word {
                    let dest = StateKey {
                        prefix: [key.prefix[1], COLOR_BLANK],
                        nstate: arc.to,
                    };
                    self.add_arc(sid, &key, COLOR_BLANK, dest);
                }
                if has_word_chars {
                    let dest = StateKey {
                        prefix: [key.prefix[1], arc.co],
                        nstate: arc.to,
                    };
                    self.add_arc(sid, &key, arc.co, dest);
                }
            }
        }
    }

    fn add_arc(&mut self, sid: u32, key: &StateKey, co: TrgmColor, dest: StateKey) {
        if !valid_arc_label(key, co) {
            return;
        }
        // Useless if the destination is already reachable trigram-free.
        for existing in &self.states[sid as usize].enter_keys {
            if existing.nstate == dest.nstate && prefix_contains(existing.prefix, dest.prefix) {
                return;
            }
        }
        let target = self.get_state(dest);
        self.states[sid as usize]
            .arcs
            .push(([key.prefix[0], key.prefix[1], co], target));
        self.arcs_count += 1;
    }

    fn resolve(&self, mut id: u32) -> u32 {
        while let Some(p) = self.states[id as usize].parent {
            id = p;
        }
        id
    }

    // Some(sorted ctrgms, total simple-trigram count), or None on overflow.
    fn select_color_trigrams(&mut self) -> Option<(Vec<ColorTrgmInfo>, i64)> {
        let mut ctrgms: Vec<ColorTrgmInfo> = Vec::with_capacity(self.arcs_count as usize);
        for (sid, state) in self.states.iter().enumerate() {
            for (ctrgm, target) in &state.arcs {
                ctrgms.push(ColorTrgmInfo {
                    ctrgm: *ctrgm,
                    cnumber: -1,
                    count: 0,
                    penalty: 0.0,
                    expanded: true,
                    arcs: vec![(sid as u32, *target)],
                });
            }
        }

        // Dedup, merging arc lists.
        ctrgms.sort_by(|a, b| a.ctrgm.cmp(&b.ctrgm));
        let mut merged: Vec<ColorTrgmInfo> = Vec::with_capacity(ctrgms.len());
        for ct in ctrgms {
            match merged.last_mut() {
                Some(last) if last.ctrgm == ct.ctrgm => last.arcs.extend(ct.arcs),
                _ => merged.push(ct),
            }
        }
        let mut ctrgms = merged;

        let mut total_count: i64 = 0;
        let mut total_penalty: f32 = 0.0;
        for info in &mut ctrgms {
            let mut count: i32 = 1;
            let mut type_index = 0usize;
            for &c in &info.ctrgm {
                type_index *= 2;
                if c == COLOR_BLANK {
                    type_index += 1;
                } else {
                    count *= self.colors[c as usize].word_chars.len() as i32;
                }
            }
            info.count = count;
            total_count += count as i64;
            info.penalty = PENALTIES[type_index] * count as f32;
            total_penalty += info.penalty;
        }

        // Remove highest-penalty trigrams while over budget, merging the
        // states their arcs connect — unless that would merge INIT with FIN.
        ctrgms.sort_by(|a, b| b.penalty.partial_cmp(&a.penalty).unwrap());
        for i in 0..ctrgms.len() {
            if total_penalty <= WISH_TRGM_PENALTY {
                break;
            }
            let mut can_remove = true;

            for &(src, tgt) in &ctrgms[i].arcs {
                let mut source = self.resolve(src);
                let mut target = self.resolve(tgt);

                let mut source_flags =
                    self.states[source as usize].flags | self.states[source as usize].tent_flags;
                while let Some(tp) = self.states[source as usize].tent_parent {
                    source = tp;
                    source_flags |=
                        self.states[source as usize].flags | self.states[source as usize].tent_flags;
                }
                let mut target_flags =
                    self.states[target as usize].flags | self.states[target as usize].tent_flags;
                while let Some(tp) = self.states[target as usize].tent_parent {
                    target = tp;
                    target_flags |=
                        self.states[target as usize].flags | self.states[target as usize].tent_flags;
                }

                if (source_flags | target_flags) & (TSTATE_INIT | TSTATE_FIN)
                    == (TSTATE_INIT | TSTATE_FIN)
                {
                    can_remove = false;
                    break;
                }

                if source != target {
                    self.states[target as usize].tent_parent = Some(source);
                    self.states[source as usize].tent_flags |= target_flags;
                }
            }

            // Reset all tentative merge bookkeeping.
            for &(src, tgt) in &ctrgms[i].arcs {
                let mut source = self.resolve(src);
                let mut target = self.resolve(tgt);
                loop {
                    self.states[source as usize].tent_flags = 0;
                    match self.states[source as usize].tent_parent {
                        Some(p) => source = p,
                        None => break,
                    }
                }
                while let Some(ttarget) = self.states[target as usize].tent_parent {
                    self.states[target as usize].tent_parent = None;
                    self.states[target as usize].tent_flags = 0;
                    target = ttarget;
                }
            }

            if !can_remove {
                continue;
            }

            for &(src, tgt) in &ctrgms[i].arcs {
                let source = self.resolve(src);
                let target = self.resolve(tgt);
                if source != target {
                    // state1 absorbs state2's flags; state2 becomes a child.
                    self.states[source as usize].flags |= self.states[target as usize].flags;
                    self.states[target as usize].parent = Some(source);
                    debug_assert!(
                        self.states[source as usize].flags & (TSTATE_INIT | TSTATE_FIN)
                            != (TSTATE_INIT | TSTATE_FIN)
                    );
                }
            }

            ctrgms[i].expanded = false;
            total_count -= ctrgms[i].count as i64;
            total_penalty -= ctrgms[i].penalty;
        }

        if total_count > MAX_TRGM_COUNT {
            return None;
        }

        // ctrgm order (for the pack-stage bsearch); number the survivors.
        ctrgms.sort_by(|a, b| a.ctrgm.cmp(&b.ctrgm));
        let mut cnumber = 0;
        for info in &mut ctrgms {
            if info.expanded {
                info.cnumber = cnumber;
                cnumber += 1;
            }
        }
        Some((ctrgms, total_count))
    }

    fn expand_color_trigrams(
        &self,
        ctrgms: &[ColorTrgmInfo],
        total_count: i64,
        legacy_crc32: &dyn Fn(&[u8]) -> u32,
    ) -> Vec<Trgm> {
        let blank: Vec<MbChar> = vec![[0; MAX_MULTIBYTE_CHAR_LEN]];
        let mut out: Vec<Trgm> = Vec::with_capacity(total_count as usize);
        for info in ctrgms {
            if !info.expanded {
                continue;
            }
            let chars_of = |c: TrgmColor| -> &[MbChar] {
                if c == COLOR_BLANK {
                    &blank
                } else {
                    &self.colors[c as usize].word_chars
                }
            };
            for c0 in chars_of(info.ctrgm[0]) {
                for c1 in chars_of(info.ctrgm[1]) {
                    for c2 in chars_of(info.ctrgm[2]) {
                        out.push(fill_trgm([c0, c1, c2], legacy_crc32));
                    }
                }
            }
        }
        debug_assert_eq!(out.len() as i64, total_count);
        out
    }

    fn pack_graph(&mut self, ctrgms: &mut [ColorTrgmInfo]) -> TrgmPackedGraph {
        // Number surviving states; 0 = initial, 1 = final.
        let mut snumber = 2i32;
        for i in 0..self.states.len() {
            let root = self.resolve(i as u32) as usize;
            if self.states[root].snumber < 0 {
                self.states[root].snumber = if self.states[root].flags & TSTATE_INIT != 0 {
                    0
                } else if self.states[root].flags & TSTATE_FIN != 0 {
                    1
                } else {
                    let n = snumber;
                    snumber += 1;
                    n
                };
            }
        }

        // (source snumber, ctrgm cnumber, target snumber) for surviving arcs.
        let mut arcs: Vec<(i32, i32, i32)> = Vec::with_capacity(self.arcs_count as usize);
        for i in 0..self.states.len() {
            let source = self.resolve(i as u32) as usize;
            for (ctrgm, tgt) in &self.states[i].arcs {
                let target = self.resolve(*tgt) as usize;
                if self.states[source].snumber != self.states[target].snumber {
                    let idx = ctrgms
                        .binary_search_by(|probe| probe.ctrgm.cmp(ctrgm))
                        .expect("pg_trgm regexp: arc color trigram not found");
                    debug_assert!(ctrgms[idx].expanded);
                    arcs.push((
                        self.states[source].snumber,
                        ctrgms[idx].cnumber,
                        self.states[target].snumber,
                    ));
                }
            }
        }
        arcs.sort();
        arcs.dedup();

        let color_trigram_groups: Vec<i32> =
            ctrgms.iter().filter(|c| c.expanded).map(|c| c.count).collect();

        let mut packed_arcs: Vec<TrgmPackedArc> = Vec::with_capacity(arcs.len());
        let mut states: Vec<(u32, u32)> = Vec::with_capacity(snumber as usize);
        let mut j = 0usize;
        for i in 0..snumber {
            let off = packed_arcs.len() as u32;
            while j < arcs.len() && arcs[j].0 == i {
                packed_arcs.push(TrgmPackedArc {
                    target_state: arcs[j].2,
                    color_trgm: arcs[j].1,
                });
                j += 1;
            }
            states.push((off, packed_arcs.len() as u32 - off));
        }

        TrgmPackedGraph::new(color_trigram_groups, states, packed_arcs)
    }
}

fn fill_trgm(s: [&MbChar; 3], legacy_crc32: &dyn Fn(&[u8]) -> u32) -> Trgm {
    let mut bytes: Vec<u8> = Vec::with_capacity(3 * MAX_MULTIBYTE_CHAR_LEN);
    for mb in s {
        if mb[0] != 0 {
            for &b in mb.iter().take_while(|&&b| b != 0) {
                bytes.push(b);
            }
        } else {
            // COLOR_BLANK renders as a space.
            bytes.push(b' ');
        }
    }
    compact_trigram(&bytes, legacy_crc32)
}
