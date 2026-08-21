//! Extractions from the harness kernels_dec.rs (port-study/port-map.md
//! §3.9): FcCache, SmaFlat + sma_build, VerdictWords. The ~2.8k lines of
//! handwritten query bodies around them stay in the reference tree.

use crate::bank::Bank;
use crate::fused::FusedCodes;
use crate::scan::par_range;

/// Thread-local FusedCodes cache (the CurCache idiom): reopen only on part
/// change; `None` cached too (parts where the face refuses).
pub struct FcCache {
    attno: u32,
    e: Option<(usize, Option<FusedCodes>)>,
}

impl FcCache {
    pub fn new(attno: u32) -> FcCache {
        FcCache { attno, e: None }
    }
    pub fn get(&mut self, bank: &Bank, pi: usize) -> Option<&FusedCodes> {
        if self.e.as_ref().map(|(p, _)| *p) != Some(pi) {
            self.e = Some((pi, FusedCodes::open(bank, pi, self.attno)));
        }
        self.e.as_ref().unwrap().1.as_ref()
    }
}

// ---------------------------------------------------------------------------
// The FLAT SMA ARRAY (Michael directive 2026-08-15): contiguous SoA
// min/max arrays indexed by global granule id (~16B/granule, L2-resident).
// The consult is a tight autovectorizable loop; the build cost is
// coldledger-attributed (amortized across every range/eq predicate on the
// column).
// ---------------------------------------------------------------------------

pub struct SmaFlat {
    pub mins: Vec<i64>,
    pub maxs: Vec<i64>,
}

/// Open-time build: flat SoA arrays aligned with the granule_walk unit
/// list (non-exact/missing records degrade to the full-range sentinel,
/// i.e. "must scan"). pgrc2.1: served by the seal-time FlatStats section
/// when present and by the §8.1 parse when absent.
pub fn sma_build(bank: &Bank, attno: u32, units: &[(usize, u32, u32, u64)]) -> SmaFlat {
    use crate::flatface::FlatFace;
    // Face-build width: sma_build runs inside a Faces memo build; the
    // FlatFace part fan-out uses the same F5-capped law via par_parts.
    let face = FlatFace::open(bank, attno, crate::engine::FACE_CONC);
    let mut mins = Vec::with_capacity(units.len());
    let mut maxs = Vec::with_capacity(units.len());
    for &(pi, g, _, _) in units {
        match face.zones_exact(pi) {
            Some((lo, hi)) => {
                mins.push(lo[g as usize]);
                maxs.push(hi[g as usize]);
            }
            None => {
                mins.push(i64::MIN);
                maxs.push(i64::MAX);
            }
        }
    }
    SmaFlat { mins, maxs }
}

// ---------------------------------------------------------------------------
// Bit-packed dict-entry verdict words: the verdict gather is the DRAM
// cost; packed to u64 words the verdict plane is 8× smaller than
// Vec<bool> and L2-resident per part.
// ---------------------------------------------------------------------------

/// Per-part verdict bitwords with an entry-grain parallel recompute.
/// Buffers allocated once; every compute call redoes ALL verdict work.
/// (The per-worker dict-handle-opening `compute` arm was the measured-
/// settled control — deleted at port; `compute_with` over the shared sqe
/// dict faces is the only arm.)
pub struct VerdictWords {
    #[allow(dead_code)]
    attno: u32,
    /// (pi, code_start, code_end); starts are multiples of 64 ⇒ every
    /// unit owns a disjoint word range of its part's buffer.
    units: Vec<(usize, u32, u32)>,
    ncodes: Vec<u32>, // 0 ⇒ column not dict-published in that part
    pub words: Vec<Vec<u64>>,
}

const VCHUNK: u32 = 16 * 1024;

impl VerdictWords {
    /// [coldstart] Build from a per-part ncodes vector the caller already
    /// holds (the sqe dict faces, built part-parallel).
    pub fn from_ncodes(attno: u32, nc: &[u32]) -> VerdictWords {
        let mut units = Vec::new();
        let mut ncodes = Vec::new();
        let mut words = Vec::new();
        for pi in 0..nc.len() {
            if nc[pi] > 0 {
                let n = nc[pi];
                ncodes.push(n);
                words.push(vec![0u64; (n as usize).div_ceil(64)]);
                let mut c = 0u32;
                while c < n {
                    units.push((pi, c, (c + VCHUNK).min(n)));
                    c += VCHUNK;
                }
            } else {
                ncodes.push(0);
                words.push(Vec::new());
            }
        }
        VerdictWords { attno, units, ncodes, words }
    }

    #[inline]
    pub fn part_words(&self, pi: usize) -> Option<&[u64]> {
        if self.ncodes[pi] == 0 {
            None
        } else {
            Some(&self.words[pi])
        }
    }

    /// [cold2] compute over SHARED dict handles (the sqe faces): one
    /// handle per part = one payload copy, and frames the prefetch wave
    /// already landed serve every worker.
    pub fn compute_with(
        &mut self,
        dhs: &[Option<std::sync::Arc<pgrc2_read::dicthandle::DictHandle>>],
        t: usize,
        pred: impl Fn(&[u8]) -> bool + Sync,
    ) {
        struct Ptrs(Vec<*mut u64>);
        unsafe impl Sync for Ptrs {}
        let ptrs = Ptrs(self.words.iter_mut().map(|w| w.as_mut_ptr()).collect());
        let ptrs = &ptrs;
        let units = &self.units;
        let tt = t.min(units.len()).max(1);
        let bulk = crate::engine::bulk_entries_on();
        par_range(
            units.len(),
            tt,
            |_| (),
            |_s: &mut (), i| {
                let (pi, c0, c1) = units[i];
                let dh = dhs[pi].as_ref().expect("dict handle for verdict part");
                let wp = ptrs.0[pi];
                crate::engine::census_entries((c1 - c0) as u64);
                if bulk {
                    // [bulkentries] Bulk sequential decode over the unit's
                    // range; the word-grain write pattern is unchanged.
                    let mut cur = dh.entries(c0, c1).expect("dict cursor");
                    let mut c = c0;
                    while c < c1 {
                        let wend = ((c | 63) + 1).min(c1);
                        let w = (c >> 6) as usize;
                        let mut acc = 0u64;
                        while c < wend {
                            let (cc, e) = cur
                                .next_entry()
                                .expect("dict entry")
                                .expect("cursor covers [c0, c1)");
                            if pred(e.bytes) {
                                acc |= 1u64 << (cc & 63);
                            }
                            c += 1;
                        }
                        // SAFETY: this unit owns words [c0/64, ...] exclusively.
                        unsafe { *wp.add(w) = acc };
                    }
                } else {
                    let mut c = c0;
                    while c < c1 {
                        let wend = ((c | 63) + 1).min(c1);
                        let mut acc = 0u64;
                        for cc in c..wend {
                            if pred(dh.entry(cc).expect("dict entry").bytes) {
                                acc |= 1u64 << (cc & 63);
                            }
                        }
                        // SAFETY: this unit owns words [c0/64, ...] exclusively.
                        unsafe { *wp.add((c >> 6) as usize) = acc };
                        c = wend;
                    }
                }
            },
        );
    }
}
