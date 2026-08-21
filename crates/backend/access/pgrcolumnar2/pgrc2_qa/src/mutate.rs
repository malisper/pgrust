//! The structure-aware part mutator — the regrown read-fuzzer's engine
//! (the #66/#340 incident class; chunk row: "the regrown read-path fuzzer
//! (standing CI gate)").
//!
//! A region map is parsed from the PRISTINE image (the mutator trusts only
//! bytes the corpus builder just wrote), then one mutation is drawn
//! deterministically from a seed. Two tiers:
//!
//! - **Raw** (`crc_fixed = false`): the mutation lands anywhere in a
//!   structural region WITHOUT checksum repair. Contract under test: the
//!   reader yields a TYPED refusal, or — when the flip landed in bytes no
//!   structure covers (inter-section padding) — the byte-identical oracle
//!   answer. Never a panic, never a silently wrong answer.
//! - **Semantic** (`crc_fixed = true`): the mutation targets the decode
//!   pipeline's OWN validation (stream section headers, frame/gcount
//!   tables, stream directory entries, extent records, section kinds) and
//!   then REPAIRS the checksum chain (section crc → section-table crc →
//!   footer crc), so the hostile bytes arrive checksummed — exactly the
//!   adversarial frame tables M3-F's refusal battery left to this lane.
//!   Contract: typed refusal or a well-formed answer (cross-checked by the
//!   fuzzer's decode_sel ≡ decode_full and arena-containment invariants);
//!   never a panic/UB.

use pgrc2_format::part::{
    FooterFixed, PartTail, SectionEntry, SectionKind, StreamSectionHdr, FOOTER_FIXED_LEN,
    PART_TAIL_LEN, SECTION_ENTRY_LEN, STREAM_SECTION_HDR_LEN,
};
use pgrc2_format::wire::crc32c;

use crate::XorShift;

/// What a mutation did (minimization + reporting currency).
#[derive(Debug, Clone)]
pub struct MutationDesc {
    pub seed: u64,
    pub kind: &'static str,
    pub at: usize,
    pub crc_fixed: bool,
}

struct PartMap {
    footer_off: usize,
    section_table_off: usize,
    entries: Vec<SectionEntry>,
}

fn parse_map(bytes: &[u8]) -> Option<PartMap> {
    let tail = PartTail::decode_at_eof(bytes).ok()?;
    let footer_off = tail.footer_off as usize;
    let footer = FooterFixed::decode(bytes.get(footer_off..)?).ok()?;
    let st_off = footer.section_table_off as usize;
    let st_len = footer.section_count as usize * SECTION_ENTRY_LEN;
    let st = bytes.get(st_off..st_off + st_len)?;
    let mut c = pgrc2_format::wire::Cur::new(st);
    let mut entries = Vec::with_capacity(footer.section_count as usize);
    for _ in 0..footer.section_count {
        entries.push(SectionEntry::decode(&mut c).ok()?);
    }
    Some(PartMap {
        footer_off,
        section_table_off: st_off,
        entries,
    })
}

/// Repair the checksum chain after editing section `idx`'s bytes (or after
/// editing the section table itself when `idx` is None): recompute the
/// entry crc, rewrite the table, recompute `section_table_crc`, re-encode
/// the footer (which recomputes `footer_crc`).
fn fix_crc_chain(bytes: &mut Vec<u8>, map: &mut PartMap, idx: Option<usize>) {
    if let Some(i) = idx {
        let e = &mut map.entries[i];
        let body = &bytes[e.off as usize..(e.off + e.len) as usize];
        e.crc = crc32c(body);
    }
    // Rewrite the whole table from the (possibly edited) entries.
    let mut table = Vec::with_capacity(map.entries.len() * SECTION_ENTRY_LEN);
    for e in &map.entries {
        e.encode_into(&mut table);
    }
    let st = map.section_table_off;
    bytes[st..st + table.len()].copy_from_slice(&table);
    // Footer: recompute the table crc + its own trailing crc.
    let mut footer =
        FooterFixed::decode(&bytes[map.footer_off..]).expect("pristine footer decodes");
    footer.section_table_crc = crc32c(&table);
    let mut fbytes = Vec::with_capacity(FOOTER_FIXED_LEN);
    footer.encode_into(&mut fbytes);
    bytes[map.footer_off..map.footer_off + FOOTER_FIXED_LEN].copy_from_slice(&fbytes);
}

fn flip_at(bytes: &mut [u8], at: usize, rng: &mut XorShift) {
    let bit = 1u8 << rng.below(8);
    bytes[at] ^= if bit == 0 { 1 } else { bit };
}

fn splice_u32(bytes: &mut [u8], at: usize, v: u32) {
    if at + 4 <= bytes.len() {
        bytes[at..at + 4].copy_from_slice(&v.to_le_bytes());
    }
}

/// Adversarial u32 palette (bounds-shaped values).
fn evil_u32(rng: &mut XorShift, len: usize) -> u32 {
    match rng.below(6) {
        0 => 0,
        1 => 1,
        2 => u32::MAX,
        3 => len as u32,
        4 => (len as u32).wrapping_sub(1),
        _ => rng.next() as u32,
    }
}

/// Apply one seeded mutation to a copy of `pristine`. Returns the mutant
/// and its description. Deterministic in `seed`.
pub fn mutate(pristine: &[u8], seed: u64) -> (Vec<u8>, MutationDesc) {
    let mut rng = XorShift::new(seed);
    let mut bytes = pristine.to_vec();
    let map = parse_map(pristine);

    // No parsable map (tiny/degenerate input): raw flip only.
    let Some(mut map) = map else {
        let at = (rng.below(bytes.len().max(1) as u64) as usize).min(bytes.len().saturating_sub(1));
        flip_at(&mut bytes, at, &mut rng);
        return (
            bytes,
            MutationDesc {
                seed,
                kind: "raw_flip_unmapped",
                at,
                crc_fixed: false,
            },
        );
    };

    let strategy = rng.below(12);
    let (kind, at, crc_fixed) = match strategy {
        // ---- raw tier -----------------------------------------------------
        0 => {
            // Anywhere.
            let at = rng.below(bytes.len() as u64) as usize;
            flip_at(&mut bytes, at, &mut rng);
            ("raw_flip_any", at, false)
        }
        1 => {
            // Tail (last 16 bytes).
            let at = bytes.len() - PART_TAIL_LEN + rng.below(PART_TAIL_LEN as u64) as usize;
            flip_at(&mut bytes, at, &mut rng);
            ("raw_flip_tail", at, false)
        }
        2 => {
            // Footer.
            let at = map.footer_off + rng.below(FOOTER_FIXED_LEN as u64) as usize;
            flip_at(&mut bytes, at, &mut rng);
            ("raw_flip_footer", at, false)
        }
        3 => {
            // Section table.
            let len = map.entries.len() * SECTION_ENTRY_LEN;
            let at = map.section_table_off + rng.below(len.max(1) as u64) as usize;
            flip_at(&mut bytes, at, &mut rng);
            ("raw_flip_section_table", at, false)
        }
        4 => {
            // A section body.
            let e = &map.entries[rng.below(map.entries.len() as u64) as usize];
            let at = e.off as usize + rng.below(e.len.max(1)) as usize;
            flip_at(&mut bytes, at, &mut rng);
            ("raw_flip_section_body", at, false)
        }
        5 => {
            // Truncate.
            let keep = rng.below(bytes.len() as u64) as usize;
            bytes.truncate(keep);
            ("truncate", keep, false)
        }
        6 => {
            // Extend with garbage (tail then no longer at EOF).
            let extra = 1 + rng.below(4096) as usize;
            let at = bytes.len();
            for k in 0..extra {
                bytes.push((rng.next() ^ k as u64) as u8);
            }
            ("extend", at, false)
        }
        7 => {
            // Zero a run.
            let at = rng.below(bytes.len() as u64) as usize;
            let run = (1 + rng.below(64) as usize).min(bytes.len() - at);
            bytes[at..at + run].fill(0);
            ("zero_run", at, false)
        }
        // ---- semantic tier (checksummed hostility) ------------------------
        8 => {
            // Flip inside a section body, then repair the crc chain — the
            // payload arrives checksummed and the decode kernels' own
            // bounds checks are on trial.
            let i = rng.below(map.entries.len() as u64) as usize;
            let e = map.entries[i];
            let at = e.off as usize + rng.below(e.len.max(1)) as usize;
            flip_at(&mut bytes, at, &mut rng);
            fix_crc_chain(&mut bytes, &mut map, Some(i));
            ("sem_flip_section_body", at, true)
        }
        9 => {
            // Stream-section header hostility: pick a Stream-kind section,
            // splice an adversarial u32 into one of its header fields
            // (frame_count / frame_table_off / gcount_table_off /
            // uncompressed_len / value_count) or its width/wrapper bytes.
            let stream_idx: Vec<usize> = map
                .entries
                .iter()
                .enumerate()
                .filter(|(_, e)| e.kind == SectionKind::Stream.as_u16())
                .map(|(i, _)| i)
                .collect();
            if stream_idx.is_empty() {
                let at = rng.below(bytes.len() as u64) as usize;
                flip_at(&mut bytes, at, &mut rng);
                ("raw_flip_any", at, false)
            } else {
                let i = stream_idx[rng.below(stream_idx.len() as u64) as usize];
                let e = map.entries[i];
                let base = e.off as usize;
                let slen = e.len as usize;
                let at = match rng.below(6) {
                    0 => base + 8,  // frame_count
                    1 => base + 12, // frame_table_off
                    2 => base + 16, // gcount_table_off
                    3 => base + 20, // uncompressed_len
                    4 => base + 24, // value_count
                    _ => base + 6,  // width+wrapper bytes
                };
                let v = evil_u32(&mut rng, slen);
                splice_u32(&mut bytes, at, v);
                fix_crc_chain(&mut bytes, &mut map, Some(i));
                ("sem_stream_hdr", at, true)
            }
        }
        10 => {
            // Frame/gcount-table hostility: splice adversarial u32s into
            // the tables past the stream header (the adversarial frame
            // tables). Falls back to a header splice when the section has
            // no table region.
            let stream_idx: Vec<usize> = map
                .entries
                .iter()
                .enumerate()
                .filter(|(_, e)| {
                    e.kind == SectionKind::Stream.as_u16()
                        && e.len as usize > STREAM_SECTION_HDR_LEN + 4
                })
                .map(|(i, _)| i)
                .collect();
            if stream_idx.is_empty() {
                let at = rng.below(bytes.len() as u64) as usize;
                flip_at(&mut bytes, at, &mut rng);
                ("raw_flip_any", at, false)
            } else {
                let i = stream_idx[rng.below(stream_idx.len() as u64) as usize];
                let e = map.entries[i];
                let base = e.off as usize;
                let slen = e.len as usize;
                // Target the table region when the header declares one;
                // otherwise anywhere past the header.
                let hdr = StreamSectionHdr::decode(&bytes[base..base + slen]).ok();
                let (lo, hi) = match hdr {
                    Some(h) if h.frame_table_off != 0 && (h.frame_table_off as usize) < slen => {
                        (h.frame_table_off as usize, slen)
                    }
                    Some(h) if h.gcount_table_off != 0 && (h.gcount_table_off as usize) < slen => {
                        (h.gcount_table_off as usize, slen)
                    }
                    _ => (STREAM_SECTION_HDR_LEN, slen),
                };
                let span = (hi - lo).max(4);
                let at = base + lo + (rng.below(span as u64) as usize / 4) * 4;
                let v = evil_u32(&mut rng, slen);
                splice_u32(&mut bytes, at.min(base + slen - 4), v);
                fix_crc_chain(&mut bytes, &mut map, Some(i));
                ("sem_frame_table", at, true)
            }
        }
        _ => {
            // Section-entry hostility: rewrite one directory row's fields
            // (kind / off / len / attno / path_ord) with adversarial
            // values, table + footer crc repaired (entry crc left matching
            // the ORIGINAL body — off/len lies must be caught by bounds
            // validation before any crc pass can matter).
            let i = rng.below(map.entries.len() as u64) as usize;
            let flen = bytes.len();
            {
                let e = &mut map.entries[i];
                match rng.below(5) {
                    0 => e.kind = [0u16, 10, 999, 0xBEEF][rng.below(4) as usize],
                    1 => e.off = [0u64, 1, flen as u64, u64::MAX / 2][rng.below(4) as usize],
                    2 => e.len = [0u64, 1, flen as u64, u64::MAX / 2][rng.below(4) as usize],
                    3 => e.attno = rng.next() as u32,
                    _ => e.path_ord = rng.next() as u32,
                }
            }
            fix_crc_chain(&mut bytes, &mut map, None);
            ("sem_section_entry", map.section_table_off + i * SECTION_ENTRY_LEN, true)
        }
    };
    (
        bytes,
        MutationDesc {
            seed,
            kind,
            at,
            crc_fixed,
        },
    )
}
