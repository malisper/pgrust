//! Extractions from the harness kernels.rs (port-study/port-map.md §3.16):
//! the hot-shape top-K `Winner` machinery + typed winner hydration, and
//! `referer_key` (hot-shape REGEXP_REPLACE host-extraction walk — hand-
//! implemented, no regex crate; risks.md §13). The escape law moved to
//! `render::esc` (the ONE render seam).

use crate::answer::{AnswerCol, AnswerSet, BytesBuild};
use crate::bank::Bank;
use crate::scan::{open_cursor, varlena_payload, Scratch};

/// One ORDER BY EventTime LIMIT k winner row (derived Ord = (etime, grow)
/// — the canonical tie law: global row order breaks time ties).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Winner {
    pub etime: i64,
    pub grow: u64,
    pub pi: u32,
    pub g: u32,
    pub rowin: u16,
}

pub fn top10_insert(w: &mut Vec<Winner>, cand: Winner) {
    if w.len() < 10 {
        w.push(cand);
        w.sort();
    } else if cand < w[9] {
        w[9] = cand;
        w.sort();
    }
}

/// Hydrate the ~100 remaining columns for exactly the winner rows —
/// typed: one AnswerCol per schema column (SELECT * shape), byval lanes
/// sign-folded through the column's width, varlena lanes copied into the
/// answer arena (the answer outlives the scratch).
pub fn hydrate_winners(bank: &Bank, winners: &[Winner]) -> AnswerSet {
    let mut ints: Vec<Option<Vec<i64>>> = Vec::new();
    let mut bytes: Vec<Option<BytesBuild>> = Vec::new();
    for col in &bank.schema {
        if col.typ.is_varlena() {
            ints.push(None);
            bytes.push(Some(BytesBuild::new()));
        } else {
            ints.push(Some(Vec::with_capacity(winners.len())));
            bytes.push(None);
        }
    }
    let mut scratch = Scratch::new();
    for w in winners {
        for (ci, col) in bank.schema.iter().enumerate() {
            let mut cur = open_cursor(bank, w.pi as usize, col.attno);
            let d = scratch.decode_sel(&mut cur, w.g, &[w.rowin]);
            let v = d[0];
            if col.typ.is_varlena() {
                bytes[ci].as_mut().unwrap().push(unsafe { varlena_payload(v) });
            } else {
                ints[ci].as_mut().unwrap().push(v as i64);
            }
        }
    }
    let cols: Vec<AnswerCol> = bank
        .schema
        .iter()
        .enumerate()
        .map(|(ci, col)| {
            if col.typ.is_varlena() {
                bytes[ci].take().unwrap().finish(col.typ)
            } else {
                AnswerCol::i64s(col.typ, ints[ci].take().unwrap())
            }
        })
        .collect();
    AnswerSet::from_cols(cols)
}

/// The exact string walk of the hot-shape REGEXP_REPLACE pattern, backtracking
/// included: try the `www.`-consumed arm first; if the host would be
/// empty before '/', retry without consuming. No match => the input.
#[inline(always)]
pub fn referer_key(s: &[u8]) -> &[u8] {
    let rest: &[u8] = if s.starts_with(b"https://") {
        &s[8..]
    } else if s.starts_with(b"http://") {
        &s[7..]
    } else {
        return s;
    };
    let arms: [usize; 2] = [if rest.starts_with(b"www.") { 4 } else { 0 }, 0];
    for &skip in arms.iter() {
        let r = &rest[skip..];
        if let Some(pos) = r.iter().position(|&c| c == b'/') {
            if pos >= 1 {
                return &r[..pos];
            }
        }
        if skip == 0 {
            break;
        }
    }
    s
}
