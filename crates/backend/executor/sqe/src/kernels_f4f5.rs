//! Extractions from the harness kernels_f4f5.rs (port-study/port-map.md
//! §3.12): the SearchPhrase-`<>''` filter face and the bounded
//! phrase-map prune. The 128-bit string-identity hash that used to live
//! here (url_hash128, an unkeyed wordwise mixer) is superseded by the
//! keyed `crate::fp::entry_fp128`. `phrase_map_emit` (a render site) died into
//! the zone_order stencil's typed emit.

use crate::bank::Bank;
use crate::scan::{dict_handle, is_dict};
use std::collections::BTreeMap;

/// Per-part filter plan for `col <> ''`.
pub enum EmptyFilter {
    /// Dict part: code of the empty entry (None = no empty entry exists in
    /// this part's dict ⇒ every row passes).
    Dict(Option<u32>),
    /// Not dict-published: test payload emptiness per row.
    Raw,
}

pub fn empty_filter(bank: &Bank, pi: usize, attno: u32) -> EmptyFilter {
    if is_dict(bank, pi, attno) {
        let dh = dict_handle(bank, pi, attno);
        let n = dh.ncodes();
        let mut empty = None;
        if crate::engine::bulk_entries_on() {
            // [bulkentries] Bulk sequential scan of the full dictionary.
            let mut cur = dh.entries(0, n).expect("dict cursor");
            while let Some((c, e)) = cur.next_entry().expect("dict entry") {
                if e.bytes.is_empty() {
                    empty = Some(c);
                    break;
                }
            }
        } else {
            for c in 0..n {
                if dh.entry(c).expect("dict entry").bytes.is_empty() {
                    empty = Some(c);
                    break;
                }
            }
        }
        EmptyFilter::Dict(empty)
    } else {
        EmptyFilter::Raw
    }
}


/// Keep only the smallest keys whose cumulative count reaches `need`.
pub fn prune_phrase_map(m: &mut BTreeMap<Vec<u8>, u64>, need: u64) {
    let mut cum = 0u64;
    let mut cut: Option<Vec<u8>> = None;
    for (k, &c) in m.iter() {
        cum += c;
        if cum >= need {
            cut = Some(k.clone());
            break;
        }
    }
    if let Some(cut) = cut {
        let tail: Vec<Vec<u8>> = m
            .range((std::ops::Bound::Excluded(cut), std::ops::Bound::Unbounded))
            .map(|(k, _)| k.clone())
            .collect();
        for k in tail {
            m.remove(&k);
        }
    }
}
