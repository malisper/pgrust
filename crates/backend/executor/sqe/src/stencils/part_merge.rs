//! [noglobaldict] per-part local-code processing + string-keyed combine.
//!
//! The sqe engine's dict-plane consumers used to lean on GLOBAL code
//! spaces (kernels_f6::GlobalDict — the stitched k-way merge of per-part
//! sorted dicts + per-part local→global remap arrays — and the TextReg
//! registry faces). Those spaces are coupled to the visible part set
//! (DML/snapshot invalidation hazard) and are a large open-time face
//! build. This module holds the replacement vocabulary:
//!
//!   - part-scoped codes (`psk`): (pi+1) << 32 | local_code — a 40-bit
//!     part-local identity that needs NO cross-part coordination;
//!   - dense per-part count arrays indexed by local code;
//!   - a combine phase that k-way merges per-part results comparing
//!     ACTUAL STRINGS. Per-part dicts are byte-sorted (the rank contract
//!     holds per part), so the merge needs no hashing; string compares
//!     happen only at merge boundaries — at GROUP grain, never per row.
//!
//! [risks.md §11] The old global paths are DELETED at port — this module
//! IS the vocabulary (global stitched dictionaries are prohibited).

use crate::answer::{AnswerCol, AnswerSet, BytesBuild};
use crate::bank::Bank;
use crate::engine::{DictFace, SqeCtx};
use crate::ir::*;
use crate::kernels_f6::FxHasher;
use crate::pool::Pool;
use crate::scan::Scratch;
use crate::typmeta::TypMeta;
use std::collections::HashMap;
use std::sync::Arc;

type Fx = std::hash::BuildHasherDefault<FxHasher>;
pub(crate) type FxBytesMap = HashMap<Vec<u8>, u64, Fx>;

/// Part-scoped code (psk): (pi+1) << 24 | local_code — 16 bits of part,
/// 24 bits of code (a part's dict holds at most its row count of entries;
/// asserted <= 2^24 by `dict_faces`). Nonzero by construction (0 stays a
/// sentinel), fits 40 bits. Within one part, psk ASC == dict-byte ASC
/// (the per-part rank contract); across parts psks carry NO order — that
/// is the point.
pub(crate) const PSK_CODE_BITS: u32 = 24;
pub(crate) const PSK_CODE_MASK: u64 = (1u64 << PSK_CODE_BITS) - 1;

#[inline(always)]
pub(crate) fn psk(pi: usize, code: u32) -> u64 {
    ((pi as u64 + 1) << PSK_CODE_BITS) | code as u64
}

#[inline(always)]
pub(crate) fn psk_part(k: u64) -> usize {
    ((k >> PSK_CODE_BITS) as usize) - 1
}

#[inline(always)]
pub(crate) fn psk_code(k: u64) -> u32 {
    (k & PSK_CODE_MASK) as u32
}

/// The par registry's gid ORDER, emulated without the registry: build_par
/// buckets by radix_of(hash_bytes(s)) — the hash's TOP BYTE — and assigns
/// bucket-local ids in (hash, bytes)-sorted order, so gid ASC is exactly
/// (hash_bytes(s), s) ASC: a pure function of the string. KeyAsc shapes
/// and gid-tie boundaries reproduce the old arm's answers through this
/// key, touching bytes only for boundary candidates.
#[inline(always)]
pub(crate) fn reg_ord(b: &[u8]) -> u64 {
    crate::grouped::hash_bytes(b)
}

/// Hard precondition for the 40-bit psk pack (checked once per query).
pub(crate) fn assert_psk_fits(bank: &Bank) {
    assert!(
        bank.parts.len() < (1 << 16) - 1,
        "part-scoped codes hold < 65535 parts (got {})",
        bank.parts.len()
    );
}

/// Per-part dict faces for one column, fetched once so merge loops can
/// borrow entry bytes freely (faces() is a mutex — never in a hot loop).
/// Asserts the 24-bit code bound of the psk pack per part.
pub(crate) fn dict_faces(ctx: &SqeCtx, attno: u32) -> Vec<Arc<DictFace>> {
    let bank = ctx.bank;
    let v: Vec<Arc<DictFace>> = ctx.faces.dicts_all(bank, attno).as_ref().clone();
    // [cold2 C7] every dict_faces consumer folds entry BYTES — prewarm.
    ctx.faces.prewarm_payloads(&v);
    for (pi, df) in v.iter().enumerate() {
        assert!(
            (df.ncodes as u64) <= PSK_CODE_MASK + 1,
            "part {pi} attno {attno}: {} dict codes exceed the 24-bit psk code field",
            df.ncodes
        );
    }
    v
}

#[inline(always)]
/// [coldstart] Dict faces for a TOUCHED part set only (class (a): a frame
/// query touching a handful of parts must not open 511 dict faces — on
/// hot-shape that opened URL+Referer faces bank-wide and the prefetcher chained
/// 1.2GB of never-consumed dict payload behind them). Untouched slots hold
/// a raw placeholder; consumers only index parts of the frame's granules.
pub(crate) fn dict_faces_for(ctx: &SqeCtx, attno: u32, parts: &[usize]) -> Vec<Arc<DictFace>> {
    let bank = ctx.bank;
    let f = ctx.faces;
    f.dicts_for(bank, attno, parts);
    let placeholder = Arc::new(DictFace { dh: None, ncodes: 0, empty_code: None });
    let mut v: Vec<Arc<DictFace>> = vec![placeholder; bank.parts.len()];
    for &pi in parts {
        v[pi] = f.dict(bank, pi, attno);
        assert!(
            (v[pi].ncodes as u64) <= PSK_CODE_MASK + 1,
            "part {pi} attno {attno}: {} dict codes exceed the 24-bit psk code field",
            v[pi].ncodes
        );
    }
    v
}

pub(crate) fn face_bytes<'a>(dfs: &'a [Arc<DictFace>], pi: usize, code: u32) -> &'a [u8] {
    dfs[pi].dh.as_ref().expect("dict part").entry(code).expect("dict entry").bytes
}

// ---------------------------------------------------------------------------
// Side-key interning: rows from NON-dict parts have no local code — their
// key bytes are interned per WORKER and carried as a side key alongside
// psks. Layout (41 bits total, disjoint from every psk):
//   bit 40 (SIDE_BIT) | worker (15 bits, <<25) | intern index (25 bits).
// Never the hot shape on the measured banks (all parts publish dicts).
// ---------------------------------------------------------------------------

pub(crate) const SIDE_BIT: u64 = 1 << 40;

pub(crate) struct Intern {
    worker: u64,
    pub tab: Vec<Vec<u8>>,
    map: HashMap<Vec<u8>, u32, Fx>,
}

impl Intern {
    pub fn new(worker: usize) -> Intern {
        assert!(worker < (1 << 15), "side keys hold < 32768 workers");
        Intern { worker: worker as u64, tab: Vec::new(), map: Default::default() }
    }
    #[inline]
    pub fn key(&mut self, b: &[u8]) -> u64 {
        let idx = match self.map.get(b) {
            Some(&i) => i,
            None => {
                let i = self.tab.len() as u32;
                assert!(i < (1 << 25), "side intern overflow");
                self.tab.push(b.to_vec());
                self.map.insert(b.to_vec(), i);
                i
            }
        };
        SIDE_BIT | (self.worker << 25) | idx as u64
    }
}

/// Byte resolution + equality over the mixed psk/side key space.
pub(crate) struct KeyBytes<'a> {
    pub pf: &'a [Arc<DictFace>],
    pub interns: Vec<&'a [Vec<u8>]>,
}

/// [emitcap-audit rider] Worker-SLOT-indexed intern table view: a side
/// key embeds the WORKER index that interned it (`Intern::new(t)`), but
/// pool state vecs are COMPACTED to the engaged workers — a positional
/// `pass1.iter().map(...)` collect misindexes whenever engagement is
/// narrower than the pool (claim-depth guard: n < 4·threads), reading
/// ANOTHER worker's table when the index stays in range (silent wrong
/// bytes) and panicking when it does not. Every KeyBytes built from
/// per-worker interns must place each table at its OWN worker slot.
pub(crate) fn interns_by_slot<'a>(
    nthreads: usize,
    it: impl Iterator<Item = &'a Intern>,
) -> Vec<&'a [Vec<u8>]> {
    const EMPTY: &[Vec<u8>] = &[];
    let mut v: Vec<&'a [Vec<u8>]> = vec![EMPTY; nthreads];
    for i in it {
        v[i.worker as usize] = i.tab.as_slice();
    }
    v
}

impl<'a> KeyBytes<'a> {
    #[inline(always)]
    pub fn bytes(&self, k: u64) -> &'a [u8] {
        if k & SIDE_BIT != 0 {
            &self.interns[((k >> 25) & 0x7fff) as usize][(k & 0x1ff_ffff) as usize]
        } else {
            face_bytes(self.pf, psk_part(k), psk_code(k))
        }
    }
    /// Byte equality with the cheap short-circuits: identical keys are
    /// equal; two psks of the SAME part are distinct by the dict contract.
    #[inline(always)]
    pub fn same(&self, a: u64, b: u64) -> bool {
        if a == b {
            return true;
        }
        if a & SIDE_BIT == 0 && b & SIDE_BIT == 0 && psk_part(a) == psk_part(b) {
            return false;
        }
        self.bytes(a) == self.bytes(b)
    }
}

// ---------------------------------------------------------------------------
// Pair-distinct engine: COUNT(DISTINCT user) grouped by a
// varlena key (+ optional packed int lane). Pairs scatter by USER hash, so
// every (group, user) pair — from whichever part — meets in ONE owner.
// First-level dedupe is exact on (key, aux, user); the cross-part boundary
// (same string, different part ⇒ different psk) is closed by a per-user
// candidate list with byte compares ONLY when a user's key set spans
// parts. Distinct counts attribute to the first-seen key of each string;
// the caller's string-keyed combine re-joins the attributions.
// ---------------------------------------------------------------------------

/// Per-owner sorted (key, aux, distinct_count) runs — psk keys only; side
/// (raw-part) keys are returned separately, bytes-resolved.
pub(crate) type PairRuns = Vec<Vec<(u64, u64, u64)>>;

pub(crate) fn pair_distinct_partlocal(
    ctx: &SqeCtx,
    a_t: u32,
    aux: Option<(u32, u64)>, // (attno, low mask)
    a_d: u32,
    drop_empty: bool,
) -> (PairRuns, Vec<((Vec<u8>, u64), u64)>) {
    use crate::grouped::{hash64, radix_of, Cnt128, RADIX_P};
    use crate::kernels_g::ColState;
    use crate::scan::CurCache;
    let (bank, pool) = (ctx.bank, ctx.pool);
    assert_psk_fits(bank);
    let pf = dict_faces(ctx, a_t);
    let units = ctx.faces.walk(bank, a_t);
    struct S {
        kc: CurCache,
        ks: Scratch,
        codes: Vec<u32>,
        ca: Option<ColState>,
        cd: ColState,
        ds: Scratch,
        dc: CurCache,
        surv: Vec<u16>,
        buckets: Vec<Vec<(u64, u64, u64)>>, // (key, aux, user)
        intern: Intern,
    }
    struct SK {
        buckets: Vec<Vec<(u64, u64, u64)>>,
        intern: Intern,
    }
    let pf2 = &pf;
    // [spill-2, shrink law] formerly UNCAPPED and column-keyed — the
    // parked states are plain data buckets, cleared/re-armed on fetch
    // (query-agnostic by the park law); scatter-arena class cap.
    static PARKPD: crate::stencils::statepark::StatePark<Vec<Vec<(u64, u64, u64)>>> = crate::stencils::statepark::StatePark::new(256 << 20);
    let parked: std::sync::Mutex<Vec<Vec<Vec<(u64, u64, u64)>>>> =
        std::sync::Mutex::new(PARKPD.fetch_up_to(pool.threads()));
    let pass1 = pool.run_finish(
        units.len(),
        |t| {
            let mut buckets = parked.lock().unwrap().pop().unwrap_or_default();
            if buckets.len() != RADIX_P {
                buckets = (0..RADIX_P).map(|_| Vec::new()).collect();
            } else {
                buckets.iter_mut().for_each(|b| b.clear());
            }
            S {
                kc: CurCache::new(a_t),
                ks: crate::scan::scratch_fetch(),
                codes: vec![0; 8192],
                ca: aux.map(|(a, _)| ColState::fetch(a)),
                cd: ColState::fetch(a_d),
                ds: crate::scan::scratch_fetch(),
                dc: CurCache::new(a_d),
                surv: Vec::new(),
                buckets,
                intern: Intern::new(t),
            }
        },
        |s: &mut S, i| {
            let (pi, g, rows, _) = units[i];
            let rows = rows as usize;
            let amask = aux.map(|(_, m)| m).unwrap_or(0);
            let mut prev: Option<(u64, u64, u64)> = None;
            if pf2[pi].dh.is_some() {
                if s.codes.len() < rows {
                    s.codes.resize(rows, 0);
                }
                s.kc.get(bank, pi).decode_codes(g, &mut s.codes[..rows]).expect("codes");
                let empty = pf2[pi].empty_code;
                if drop_empty && aux.is_none() {
                    // Selective decode of the distinct column over the
                    // non-empty survivors (survivors << rows: the hot-shape sel
                    // law — the key lane is the ONLY full decode).
                    s.surv.clear();
                    for r in 0..rows {
                        if Some(s.codes[r]) != empty {
                            s.surv.push(r as u16);
                        }
                    }
                    if s.surv.is_empty() {
                        return;
                    }
                    let du = s.ds.decode_sel(s.dc.get(bank, pi), g, &s.surv);
                    for (idx, &r) in s.surv.iter().enumerate() {
                        let tr = (psk(pi, s.codes[r as usize]), 0u64, du[idx]);
                        if prev != Some(tr) {
                            s.buckets[radix_of(hash64(tr.2))].push(tr);
                            prev = Some(tr);
                        }
                    }
                    return;
                }
                let da: Option<&[u64]> = s.ca.as_mut().map(|c| {
                    let d = c.dec(bank, pi, g, rows);
                    unsafe { std::slice::from_raw_parts(d.as_ptr(), d.len()) }
                });
                let dd = s.cd.dec(bank, pi, g, rows);
                let dd: &[u64] = unsafe { std::slice::from_raw_parts(dd.as_ptr(), dd.len()) };
                for r in 0..rows {
                    let c = s.codes[r];
                    if drop_empty && Some(c) == empty {
                        continue;
                    }
                    let a = da.map(|d| d[r] & amask).unwrap_or(0);
                    let tr = (psk(pi, c), a, dd[r]);
                    if prev != Some(tr) {
                        s.buckets[radix_of(hash64(tr.2))].push(tr);
                        prev = Some(tr);
                    }
                }
            } else {
                let da: Option<&[u64]> = s.ca.as_mut().map(|c| {
                    let d = c.dec(bank, pi, g, rows);
                    unsafe { std::slice::from_raw_parts(d.as_ptr(), d.len()) }
                });
                let dd = s.cd.dec(bank, pi, g, rows);
                let dd: &[u64] = unsafe { std::slice::from_raw_parts(dd.as_ptr(), dd.len()) };
                let d = s.ks.decode_full(s.kc.get(bank, pi), g, rows);
                let d: &[u64] = unsafe { std::slice::from_raw_parts(d.as_ptr(), d.len()) };
                for r in 0..rows {
                    let p = unsafe { crate::scan::varlena_payload(d[r]) };
                    if drop_empty && p.is_empty() {
                        continue;
                    }
                    let a = da.map(|dv| dv[r] & amask).unwrap_or(0);
                    let tr = (s.intern.key(p), a, dd[r]);
                    if prev != Some(tr) {
                        s.buckets[radix_of(hash64(tr.2))].push(tr);
                        prev = Some(tr);
                    }
                }
            }
        },
        |s| {
            crate::scan::scratch_park(s.ks);
            crate::scan::scratch_park(s.ds);
            if let Some(c) = s.ca {
                c.park();
            }
            s.cd.park();
            SK { buckets: s.buckets, intern: s.intern }
        },
    );
    let kb = KeyBytes { pf: &pf, interns: interns_by_slot(pool.threads(), pass1.iter().map(|s| &s.intern)) };
    let kbr = &kb;
    let scattered: Vec<&Vec<Vec<(u64, u64, u64)>>> = pass1.iter().map(|s| &s.buckets).collect();
    let owned = pool.run(
        RADIX_P,
        |_| {
            (
                Cnt128::new(16),
                Vec::<(u64, u64, u64)>::new(), // (user, aux, key)
                Vec::<(u64, u64)>::new(),      // (key, aux) hits, one per distinct user
            )
        },
        |(seen, tris, hits), p| {
            let n: usize = scattered.iter().map(|b| b[p].len()).sum();
            seen.reset(n.max(16));
            tris.clear();
            for b in &scattered {
                for &(key, a, u) in &b[p] {
                    debug_assert!(a < (1 << 23), "aux lane must fit 23 bits");
                    let sk = ((key as u128) << 87) | ((a as u128) << 64) | u as u128;
                    if seen.add(sk, 1) {
                        tris.push((u, a, key));
                    }
                }
            }
            // Sort-based cross-part closure: runs of one (user, aux) hold
            // that user's distinct keys; single-entry and single-part runs
            // (the overwhelming majority) never touch bytes.
            tris.sort_unstable();
            let mut reps: Vec<u64> = Vec::new();
            let mut i = 0usize;
            while i < tris.len() {
                let (u, a, _) = tris[i];
                let mut j = i;
                while j < tris.len() && tris[j].0 == u && tris[j].1 == a {
                    j += 1;
                }
                let one_part = tris[i..j].iter().all(|t| t.2 & SIDE_BIT == 0)
                    && psk_part(tris[i].2) == psk_part(tris[j - 1].2);
                if j - i == 1 || one_part {
                    for t in &tris[i..j] {
                        hits.push((t.2, a));
                    }
                } else {
                    reps.clear();
                    for t in &tris[i..j] {
                        if !reps.iter().any(|&q| kbr.same(q, t.2)) {
                            reps.push(t.2);
                            hits.push((t.2, a));
                        }
                    }
                }
                i = j;
            }
            // `hits` ACCUMULATES across the partitions this worker owns;
            // the sort + run-length collapse happens once per worker below.
        },
    );
    let mut hit_lists: Vec<Vec<(u64, u64)>> = owned.into_iter().map(|(_, _, h)| h).collect();
    let hl = &mut hit_lists;
    let hl_ptr = std::sync::Mutex::new(hl.iter_mut().collect::<Vec<_>>());
    let collapsed = pool.run(
        hit_lists_len(&hl_ptr),
        |_| Vec::new(),
        |out: &mut Vec<Vec<(u64, u64, u64)>>, _i| {
            let hits = match hl_ptr.lock().unwrap().pop() {
                Some(h) => h,
                None => return,
            };
            hits.sort_unstable();
            let mut i = 0usize;
            let mut runs: Vec<(u64, u64, u64)> = Vec::new();
            while i < hits.len() {
                let (key, a) = hits[i];
                let mut j = i;
                while j < hits.len() && hits[j] == (key, a) {
                    j += 1;
                }
                runs.push((key, a, (j - i) as u64));
                i = j;
            }
            hits.clear();
            out.push(runs);
        },
    );
    // Side (raw-part) keys are few: fold them out serially, bytes-resolved.
    let mut side: HashMap<(Vec<u8>, u64), u64, Fx> = Default::default();
    let mut owner_runs: PairRuns = Vec::new();
    for runs in collapsed.into_iter().flatten() {
        let mut keep: Vec<(u64, u64, u64)> = Vec::with_capacity(runs.len());
        for (key, a, c) in runs {
            if key & SIDE_BIT != 0 {
                *side.entry((kb.bytes(key).to_vec(), a)).or_insert(0) += c;
            } else {
                keep.push((key, a, c));
            }
        }
        owner_runs.push(keep);
    }
    drop(scattered);
    {
        let mut v: Vec<Vec<Vec<(u64, u64, u64)>>> = parked.into_inner().unwrap();
        for st in pass1 {
            v.push(st.buckets);
        }
        for b in v {
            let bytes = crate::stencils::statepark::nested_bytes(&b);
            PARKPD.park(b, bytes);
        }
    }
    (owner_runs, side.into_iter().collect())
}

fn hit_lists_len(m: &std::sync::Mutex<Vec<&mut Vec<(u64, u64)>>>) -> usize {
    m.lock().unwrap().len()
}

// ---------------------------------------------------------------------------
// [fpcombine] Pair-distinct on the fingerprint plane (hot-shape shape, no aux):
// pass1 carries each pair as (group ENTRY fingerprint, user, rep key);
// owners dedupe on a 128-bit pair fingerprint (group fp XOR a two-lane
// user mix) and fold group distinct-counts on group-fp equality in the
// same pass. No cross-part closure, no per-user candidate sort, no hit
// accumulation/sort — cross-part identity is the fingerprint itself.
// ---------------------------------------------------------------------------

/// Fingerprint-carrying fragment: (group fp, rep key, cnt).
pub(crate) type FpFrag = (u128, u64, u64);

/// Returns the group column's dict faces as the third element so the
/// caller's combine/render leg reuses them — [p2-phase-widening] the
/// refetch was a second per-exec faces walk + prewarm pool fan-out
/// (q10's `merge_render` served widening).
pub(crate) fn pair_distinct_fp(
    ctx: &SqeCtx,
    q: u32,
    a_t: u32,
    a_d: u32,
    drop_empty: bool,
) -> (Vec<Vec<FpFrag>>, Vec<((Vec<u8>, u64), u64)>, Vec<Arc<DictFace>>) {
    use crate::grouped::{hash128, hash64, radix_of, Cnt128, RADIX_P};
    use crate::kernels_g::ColState;
    use crate::scan::CurCache;
    let (bank, pool) = (ctx.bank, ctx.pool);
    assert_psk_fits(bank);
    let pf = dict_faces(ctx, a_t);
    let t_fpb = std::time::Instant::now();
    let fps = build_fps_cached(ctx, &pf, a_t);
    crate::engine::ph(q, "pd_fpb", t_fpb);
    let units = ctx.faces.walk(bank, a_t);
    #[inline(always)]
    fn umix(u: u64) -> u128 {
        use crate::grouped::hash64;
        ((hash64(u) as u128) << 64) | hash64(u ^ 0xA5A3_1E8B_7F19_C6D3) as u128
    }
    struct S {
        kc: CurCache,
        ks: Scratch,
        codes: Vec<u32>,
        cd: ColState,
        ds: Scratch,
        dc: CurCache,
        surv: Vec<u16>,
        buckets: Vec<Vec<(u128, u64, u64)>>, // (group fp, user, rep key)
        intern: Intern,
    }
    struct SK {
        buckets: Vec<Vec<(u128, u64, u64)>>,
        intern: Intern,
    }
    let pf2 = &pf;
    let fpr: &[Vec<u128>] = &fps;
    // [spill-2, shrink law] formerly UNCAPPED and column-keyed (see
    // PARKPD): scatter-arena class cap, query-agnostic.
    static PARKPDF: crate::stencils::statepark::StatePark<Vec<Vec<(u128, u64, u64)>>> = crate::stencils::statepark::StatePark::new(256 << 20);
    let parked: std::sync::Mutex<Vec<Vec<Vec<(u128, u64, u64)>>>> =
        std::sync::Mutex::new(PARKPDF.fetch_up_to(pool.threads()));
    let t_p1 = std::time::Instant::now();
    let pass1 = pool.run_finish(
        units.len(),
        |t| {
            let mut buckets = parked.lock().unwrap().pop().unwrap_or_default();
            if buckets.len() != RADIX_P {
                buckets = (0..RADIX_P).map(|_| Vec::new()).collect();
            } else {
                buckets.iter_mut().for_each(|b| b.clear());
            }
            S {
                kc: CurCache::new(a_t),
                ks: crate::scan::scratch_fetch(),
                codes: vec![0; 8192],
                cd: ColState::fetch(a_d),
                ds: crate::scan::scratch_fetch(),
                dc: CurCache::new(a_d),
                surv: Vec::new(),
                buckets,
                intern: Intern::new(t),
            }
        },
        |s: &mut S, i| {
            let (pi, g, rows, _) = units[i];
            let rows = rows as usize;
            let mut prev: Option<(u64, u64)> = None; // (rep key, user)
            if pf2[pi].dh.is_some() {
                if s.codes.len() < rows {
                    s.codes.resize(rows, 0);
                }
                s.kc.get(bank, pi).decode_codes(g, &mut s.codes[..rows]).expect("codes");
                let empty = pf2[pi].empty_code;
                let fpp = &fpr[pi];
                if drop_empty {
                    // Selective decode of the distinct column over the
                    // non-empty survivors (the hot-shape sel law).
                    s.surv.clear();
                    for r in 0..rows {
                        if Some(s.codes[r]) != empty {
                            s.surv.push(r as u16);
                        }
                    }
                    if s.surv.is_empty() {
                        return;
                    }
                    let du = s.ds.decode_sel(s.dc.get(bank, pi), g, &s.surv);
                    for (idx, &r) in s.surv.iter().enumerate() {
                        let c = s.codes[r as usize];
                        let key = psk(pi, c);
                        let u = du[idx];
                        if prev != Some((key, u)) {
                            s.buckets[radix_of(hash64(u))].push((fpp[c as usize], u, key));
                            prev = Some((key, u));
                        }
                    }
                    return;
                }
                let dd = s.cd.dec(bank, pi, g, rows);
                let dd: &[u64] = unsafe { std::slice::from_raw_parts(dd.as_ptr(), dd.len()) };
                for r in 0..rows {
                    let c = s.codes[r];
                    let key = psk(pi, c);
                    let u = dd[r];
                    if prev != Some((key, u)) {
                        s.buckets[radix_of(hash64(u))].push((fpp[c as usize], u, key));
                        prev = Some((key, u));
                    }
                }
            } else {
                let dd = s.cd.dec(bank, pi, g, rows);
                let dd: &[u64] = unsafe { std::slice::from_raw_parts(dd.as_ptr(), dd.len()) };
                let d = s.ks.decode_full(s.kc.get(bank, pi), g, rows);
                let d: &[u64] = unsafe { std::slice::from_raw_parts(d.as_ptr(), d.len()) };
                let mut prev_fp: u128 = 0;
                for r in 0..rows {
                    let p = unsafe { crate::scan::varlena_payload(d[r]) };
                    if drop_empty && p.is_empty() {
                        continue;
                    }
                    let key = s.intern.key(p);
                    let u = dd[r];
                    if prev != Some((key, u)) {
                        if prev.map(|(k, _)| k) != Some(key) {
                            prev_fp = crate::fp::entry_fp128(p);
                        }
                        s.buckets[radix_of(hash64(u))].push((prev_fp, u, key));
                        prev = Some((key, u));
                    }
                }
            }
        },
        |s| {
            crate::scan::scratch_park(s.ks);
            crate::scan::scratch_park(s.ds);
            s.cd.park();
            SK { buckets: s.buckets, intern: s.intern }
        },
    );
    crate::engine::ph(q, "pd_p1", t_p1);
    let t_own = std::time::Instant::now();
    let kb = KeyBytes { pf: &pf, interns: interns_by_slot(pool.threads(), pass1.iter().map(|s| &s.intern)) };
    let kbr = &kb;
    let scattered: Vec<&Vec<Vec<(u128, u64, u64)>>> = pass1.iter().map(|s| &s.buckets).collect();
    // owner group slot: (group fp, rep key, distinct cnt).
    type FpGrpSlot = (u128, u64, u64);
    // [spill-2, shrink law] formerly UNCAPPED — table class cap.
    static PARKPDT: crate::stencils::statepark::StatePark<(Cnt128, Vec<FpGrpSlot>)> = crate::stencils::statepark::StatePark::new(64 << 20);
    let owned = pool.run(
        RADIX_P,
        |_| {
            let (seen, slots) =
                PARKPDT.fetch().unwrap_or_else(|| (Cnt128::new(16), Vec::new()));
            (Vec::<FpFrag>::new(), Vec::<((Vec<u8>, u64), u64)>::new(), seen, slots)
        },
        |(out, sideo, seen, slots): &mut (
            Vec<FpFrag>,
            Vec<((Vec<u8>, u64), u64)>,
            Cnt128,
            Vec<FpGrpSlot>,
        ),
         p| {
            let n: usize = scattered.iter().map(|b| b[p].len()).sum();
            if n == 0 {
                return;
            }
            seen.reset(n.max(16));
            let cap = (n * 2).next_power_of_two().max(16);
            let mask = cap - 1;
            if slots.len() < cap {
                slots.resize(cap, (0, 0, 0));
            }
            for e in slots[..cap].iter_mut() {
                e.2 = 0;
            }
            let tbl = &mut slots[..cap];
            for b in &scattered {
                for &(gfp, u, key) in &b[p] {
                    if seen.add(gfp ^ umix(u), 1) {
                        let mut i = (hash128(gfp) as usize) & mask;
                        loop {
                            let e = &mut tbl[i];
                            if e.2 == 0 {
                                *e = (gfp, key, 1);
                                break;
                            }
                            if e.0 == gfp {
                                e.2 += 1;
                                break;
                            }
                            i = (i + 1) & mask;
                        }
                    }
                }
            }
            for e in tbl.iter() {
                if e.2 != 0 {
                    if e.1 & SIDE_BIT != 0 {
                        sideo.push(((kbr.bytes(e.1).to_vec(), 0), e.2));
                    } else {
                        out.push(*e);
                    }
                }
            }
        },
    );
    // Side (raw-part) keys are few: fold them out serially, bytes-resolved.
    let mut side: HashMap<(Vec<u8>, u64), u64, Fx> = Default::default();
    let mut owner_runs: Vec<Vec<FpFrag>> = Vec::new();
    for (runs, sideo, seen, slots) in owned {
        for ((b, a), c) in sideo {
            *side.entry((b, a)).or_insert(0) += c;
        }
        owner_runs.push(runs);
        let bytes = seen.keys.capacity() * 16 + seen.cnt.capacity() * 4 + crate::stencils::statepark::vec_bytes(&slots);
        PARKPDT.park((seen, slots), bytes);
    }
    drop(scattered);
    {
        let mut v: Vec<Vec<Vec<(u128, u64, u64)>>> = parked.into_inner().unwrap();
        for st in pass1 {
            v.push(st.buckets);
        }
        for b in v {
            let bytes = crate::stencils::statepark::nested_bytes(&b);
            PARKPDF.park(b, bytes);
        }
    }
    park_fps(fps);
    crate::engine::ph(q, "pd_own", t_own);
    (owner_runs, side.into_iter().collect(), pf)
}

// ---------------------------------------------------------------------------
// hot-shape: GROUP BY one varlena key, COUNT(*), no predicate — per-part
// dense local counts folded inside the bit-unpack, then the k-way string
// merge. Replaces gid_count's registry translate + global dense add.
// ---------------------------------------------------------------------------

pub fn part_count(ctx: &SqeCtx, node: &PlanNode) -> AnswerSet {
    let (bank, pool) = (ctx.bank, ctx.pool);
    assert_psk_fits(bank);
    let a_k = node.params.group_cols[0];
    let drop_empty = node.params.flags & F_DROP_EMPTY_KEY != 0;
    let np = bank.parts.len();

    struct S1 {
        counts: Vec<(usize, Vec<u32>)>,
        side: FxBytesMap,
        scr: Scratch,
        codes: Vec<u32>,
    }
    let t_p1 = std::time::Instant::now();
    let states = pool.run_finish(
        np,
        |_| S1 {
            counts: Vec::new(),
            side: Default::default(),
            scr: crate::scan::scratch_fetch(),
            codes: vec![0; 8192],
        },
        |s: &mut S1, pi| {
            let df = ctx.faces.dict(bank, pi, a_k);
            if df.dh.is_some() {
                let mut local = vec![0u32; df.ncodes as usize];
                let fc = crate::fused::FusedCodes::open(bank, pi, a_k);
                let mut cur = crate::scan::open_cursor(bank, pi, a_k);
                for g in 0..cur.granule_count() {
                    let rows = cur.rows_in_granule(g) as usize;
                    match &fc {
                        Some(fc) => fc.count_into(g, rows, &mut local),
                        None => {
                            if s.codes.len() < rows {
                                s.codes.resize(rows, 0);
                            }
                            cur.decode_codes(g, &mut s.codes[..rows]).expect("codes");
                            for &c in &s.codes[..rows] {
                                local[c as usize] += 1;
                            }
                        }
                    }
                }
                s.counts.push((pi, local));
            } else {
                let mut cur = crate::scan::open_cursor(bank, pi, a_k);
                for g in 0..cur.granule_count() {
                    let rows = cur.rows_in_granule(g) as usize;
                    let d = s.scr.decode_full(&mut cur, g, rows);
                    for &x in d {
                        let p = unsafe { crate::scan::varlena_payload(x) };
                        *s.side.entry(p.to_vec()).or_insert(0) += 1;
                    }
                }
            }
        },
        |s| {
            crate::scan::scratch_park(s.scr);
            (s.counts, s.side)
        },
    );
    crate::engine::phn(node, "pass1", t_p1);

    let t_m = std::time::Instant::now();
    let mut parts: Vec<(usize, Vec<u32>, Arc<DictFace>)> = Vec::new();
    let mut side: FxBytesMap = Default::default();
    let mut covered: u64 = 0;
    for s in states {
        for (pi, counts) in s.0 {
            covered += counts.iter().map(|&c| c as u64).sum::<u64>();
            let df = ctx.faces.dict(bank, pi, a_k);
            parts.push((pi, counts, df));
        }
        for (b, c) in s.1 {
            covered += c;
            *side.entry(b).or_insert(0) += c;
        }
    }
    parts.sort_unstable_by_key(|p| p.0);
    assert_eq!(covered, bank.rows_total(), "q{}: part counts must cover the bank", node.q);
    let kw = node.params.emit_cap();
    let mut frags = dense_to_frags(pool, &parts, drop_empty);
    let mut side_intern = Intern::new(0);
    let mut sf: Vec<Frag> = Vec::new();
    for (b, &c) in &side {
        if drop_empty && b.is_empty() {
            continue;
        }
        sf.push((side_intern.key(b), 0, c));
    }
    frags.push(sf);
    let pf = dict_faces(ctx, a_k);
    let kb = KeyBytes { pf: &pf, interns: vec![side_intern.tab.as_slice()] };
    let (cands, _, _) = if ctx.faces.cfg.fpcombine {
        let fps = build_fps_cached(ctx, &pf, a_k);
        let kf = KeyFp { fps: &fps, kb: &kb };
        let r = fp_combine(pool, &frags, &kf, kw);
        park_fps(fps);
        r
    } else {
        hash_combine(pool, &frags, &kb, kw)
    };
    let mut top: Vec<(u64, &[u8])> = cands.iter().map(|&(c, key, _)| (c, kb.bytes(key))).collect();
    top.sort_unstable_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(b.1)));
    top.truncate(kw);
    crate::engine::phn(node, "merge_render", t_m);
    let key_ty = node.ty_of(a_k);
    let mut kbuild = BytesBuild::new();
    let mut cnts: Vec<i64> = Vec::new();
    for (c, b) in top.iter().skip(node.params.offset) {
        kbuild.push(b);
        cnts.push(*c as i64);
    }
    AnswerSet::from_cols(vec![
        kbuild.finish(key_ty),
        AnswerCol::i64s(TypMeta::INT8, cnts),
    ])
}

// ---------------------------------------------------------------------------
// [fpcombine] Per-part dict-entry fingerprint plane: fp[pi][code] =
// entry_fp128(entry bytes) — a pure function of ONE part's dict, computed
// per query run at ENTRY grain (sequential payload walk, part-parallel).
// Combine consumers fold on 128-bit fingerprint equality (the shipped
// text128/hot-shape identity convention: collision odds ~1e-20 at 100m NDV, and
// the in-process oracle assert is the standing witness) instead of hashing
// + byte-comparing each fragment's key bytes: the measured hot-shape cost
// was ~10M RANDOM dict-entry touches per rep at the combine. No cross-part
// or part-set-coupled state exists here.
// ---------------------------------------------------------------------------

// [spill-2, shrink law] formerly UNCAPPED — the fp plane is a whole-
// column entry-fingerprint buffer (16 B x dict entries per part);
// scatter-arena class cap.
static PARKFP: crate::stencils::statepark::StatePark<Vec<Vec<u128>>> = crate::stencils::statepark::StatePark::new(256 << 20);

pub(crate) fn build_fps(pool: &Pool, pf: &[Arc<DictFace>]) -> Vec<Vec<u128>> {
    build_fps_arm(pool, pf, crate::engine::bulk_entries_on())
}

/// [bulkentries] Forced-arm twin of [`build_fps`]: both decode arms are
/// bit-identical by contract (the identity test pins it); production
/// always routes through the gate.
pub fn build_fps_arm(pool: &Pool, pf: &[Arc<DictFace>], bulk: bool) -> Vec<Vec<u128>> {
    use crate::fp::entry_fp128;
    let t0 = std::time::Instant::now();
    // [arena law] fingerprint buffers parked across reps (grow-only
    // BUFFERS only — contents are recomputed every run; nothing derived
    // persists across queries).
    let mut out: Vec<Vec<u128>> = PARKFP.fetch().unwrap_or_default();
    out.resize_with(pf.len(), Vec::new);
    out.truncate(pf.len());
    // Entry-grain units (load balance: part dict sizes vary wildly), each
    // writing a disjoint range of its part's preallocated array.
    const FCHUNK: usize = 1 << 16;
    let mut units: Vec<(usize, u32, u32)> = Vec::new();
    for (pi, df) in pf.iter().enumerate() {
        let n = df.ncodes as usize;
        if df.dh.is_none() {
            out[pi].clear();
            continue;
        }
        if out[pi].len() != n {
            out[pi].resize(n, 0);
        }
        let mut c = 0usize;
        while c < n {
            units.push((pi, c as u32, ((c + FCHUNK).min(n)) as u32));
            c += FCHUNK;
        }
    }
    struct Ptrs(Vec<*mut u128>);
    unsafe impl Sync for Ptrs {}
    let ptrs = Ptrs(out.iter_mut().map(|v| v.as_mut_ptr()).collect());
    let ptrs = &ptrs;
    let unitsr = &units;
    pool.run(
        units.len(),
        |_| (),
        |_, i| {
            let (pi, c0, c1) = unitsr[i];
            let dh = pf[pi].dh.as_ref().expect("dict handle");
            let wp = ptrs.0[pi];
            if bulk {
                // [bulkentries] Bulk sequential decode: one frame-grain
                // refill instead of a per-entry `entry()` engagement
                // (bounds + ensure + arm dispatch on every touch).
                let mut cur = dh.entries(c0, c1).expect("dict cursor");
                while let Some((c, e)) = cur.next_entry().expect("dict entry") {
                    // SAFETY: this unit owns [c0, c1) of part pi exclusively.
                    unsafe { *wp.add(c as usize) = entry_fp128(e.bytes) };
                }
            } else {
                for c in c0..c1 {
                    let b = dh.entry(c).expect("dict entry").bytes;
                    // SAFETY: this unit owns [c0, c1) of part pi exclusively.
                    unsafe { *wp.add(c as usize) = entry_fp128(b) };
                }
            }
        },
    );
    crate::coldledger::note(
        "dict_fp",
        format!("parts={}", pf.len()),
        t0,
        pf.iter().map(|d| d.ncodes as u64 * 16).sum(),
        crate::coldledger::Reason::TouchedByQuery,
    );
    out
}

/// Park the fingerprint buffers for the next run (call after the combine).
/// A cached (shared) plane is left alone — only sole owners reclaim.
pub(crate) fn park_fps(fps: Arc<Vec<Vec<u128>>>) {
    if let Ok(v) = Arc::try_unwrap(fps) {
        let bytes = crate::stencils::statepark::nested_bytes(&v);
        PARKFP.park(v, bytes);
    }
}

/// [fpcombine] Fingerprint plane with OPTIONAL second-touch persistence
/// (cfg.fpcache; ruling measurement — see engine.rs): first execution
/// computes and does NOT store, recurrence stores, later runs replay.
/// Shape-verified against the current faces on every replay. The store
/// lives on the relation's Faces (per-relation, the crate reshape of the
/// PoC's process statics).
pub(crate) fn build_fps_cached(
    ctx: &SqeCtx,
    pf: &[Arc<DictFace>],
    attno: u32,
) -> Arc<Vec<Vec<u128>>> {
    if !ctx.faces.cfg.fpcache {
        return Arc::new(build_fps(ctx.pool, pf));
    }
    if let Some(v) = ctx.faces.fp_plane_get(attno) {
        let ok = v.len() == pf.len()
            && v.iter().zip(pf.iter()).all(|(f, d)| {
                f.len() == if d.dh.is_some() { d.ncodes as usize } else { 0 }
            });
        if ok {
            // [oracle, ruling Q4] The fp plane's KEY (attno) is structural
            // already; the hash-as-identity lives in the VALUES (entry
            // fingerprints of the full entry bytes — no encoder fields to
            // omit). Verify the replayed plane's PROVENANCE anyway: each
            // part's first entry must re-fingerprint to the cached value
            // (catches a stale plane served across dict-content or seed
            // drift that the ncodes shape check cannot see).
            #[cfg(feature = "oracle")]
            for (pi, d) in pf.iter().enumerate() {
                if let Some(dh) = d.dh.as_ref() {
                    if d.ncodes > 0 {
                        let b = dh.entry(0).expect("dict entry").bytes;
                        let want = crate::fp::entry_fp128(b);
                        assert!(
                            v[pi][0] == want,
                            "sqe oracle: fingerprint collision or encoder omission on the \
                             fp plane (attno {attno}, part {pi}): cached entry-0 fp \
                             {:#034x} != recomputed {want:#034x}",
                            v[pi][0]
                        );
                    }
                }
            }
            return v;
        }
    }
    let store = ctx.faces.fp_plane_touch(attno);
    let v = Arc::new(build_fps(ctx.pool, pf));
    if store {
        ctx.faces.fp_plane_put(attno, v.clone());
    }
    v
}

/// Fingerprint resolution over the mixed psk/side key space.
pub(crate) struct KeyFp<'a> {
    pub fps: &'a [Vec<u128>],
    pub kb: &'a KeyBytes<'a>,
}

impl<'a> KeyFp<'a> {
    #[inline(always)]
    pub fn fp(&self, k: u64) -> u128 {
        if k & SIDE_BIT != 0 {
            crate::fp::entry_fp128(self.kb.bytes(k))
        } else {
            self.fps[psk_part(k)][psk_code(k) as usize]
        }
    }
}

/// [fpcombine] STAGE-2 COMBINE on fingerprints: same contract and output
/// as `hash_combine` (per-bucket candidates under the tie-inclusive
/// count-only bar) but fragments fold on (fp128, aux) equality — no byte
/// hashing, no byte equality, no dict-entry touches.
pub(crate) fn fp_combine(
    pool: &Pool,
    frags: &[Vec<Frag>],
    kf: &KeyFp,
    k: usize,
) -> (Vec<(u64, u64, u64)>, u64, u64) {
    const CB: usize = 256;
    let scat = pool.run(
        frags.len(),
        |_| (0..CB).map(|_| Vec::<(u128, u64, u64, u64)>::new()).collect::<Vec<_>>(),
        |b: &mut Vec<Vec<(u128, u64, u64, u64)>>, i| {
            for &(key, a, c) in &frags[i] {
                let f = kf.fp(key);
                let h = crate::grouped::hash128(f) ^ a.wrapping_mul(0x9E37_79B9_7F4A_7C15);
                b[(h >> 56) as usize].push((f, key, a, c));
            }
        },
    );
    let scatr = &scat;
    type FpSlot = (u128, u64, u64, u64); // (fp, repkey, aux, cnt)
    let folded = pool.run(
        CB,
        |_| (Vec::<(u64, u64, u64)>::new(), 0u64, 0u64, Vec::<FpSlot>::new()),
        |(out, g, r, slots): &mut (Vec<(u64, u64, u64)>, u64, u64, Vec<FpSlot>), bk| {
            let n: usize = scatr.iter().map(|b| b[bk].len()).sum();
            if n == 0 {
                return;
            }
            let cap = (n * 2).next_power_of_two().max(16);
            let mask = cap - 1;
            if slots.len() < cap {
                slots.resize(cap, (0, 0, 0, 0));
            }
            for e in slots[..cap].iter_mut() {
                e.3 = 0;
            }
            let slots = &mut slots[..cap];
            for b in scatr.iter() {
                for &(f, key, a, c) in &b[bk] {
                    let mut i = (crate::grouped::hash128(f) as usize) & mask;
                    loop {
                        let e = &mut slots[i];
                        if e.3 == 0 {
                            *e = (f, key, a, c);
                            break;
                        }
                        if e.0 == f && e.2 == a {
                            e.3 += c;
                            break;
                        }
                        i = (i + 1) & mask;
                    }
                }
            }
            let mut all: Vec<(u64, u64, u64)> = Vec::new();
            for e in slots.iter() {
                if e.3 != 0 {
                    all.push((e.3, e.1, e.2));
                    *r += e.3;
                }
            }
            *g += all.len() as u64;
            if k > 0 && all.len() > k {
                let (_, nth, _) = all.select_nth_unstable_by(k - 1, |x, y| y.0.cmp(&x.0));
                let bar = nth.0;
                all.retain(|e| e.0 >= bar);
            }
            out.extend(all);
        },
    );
    let mut cands: Vec<(u64, u64, u64)> = Vec::new();
    let (mut groups, mut rows) = (0u64, 0u64);
    for (o, g, r, _) in folded {
        groups += g;
        rows += r;
        cands.extend(o);
    }
    (cands, groups, rows)
}

/// [fpcombine] Combine over fragments that ALREADY CARRY their group
/// fingerprint (no lookup, no byte touch): scatter by the fp's top byte,
/// fold on fp equality, per-bucket tie-inclusive count bar. Returns
/// (cnt, rep key, 0) candidates.
pub(crate) fn fp_combine_pre(
    pool: &Pool,
    frags: &[Vec<FpFrag>],
    k: usize,
) -> Vec<(u64, u64, u64)> {
    const CB: usize = 256;
    // [arena law] scatter buckets parked across reps (fresh-alloc scatter
    // measured ~10ms/rep on the hot-shape fragment volume).
    // [spill-2, shrink law] formerly UNCAPPED — scatter-arena class cap.
    static PARKFPS: crate::stencils::statepark::StatePark<Vec<Vec<FpFrag>>> = crate::stencils::statepark::StatePark::new(256 << 20);
    let parked: std::sync::Mutex<Vec<Vec<Vec<FpFrag>>>> =
        std::sync::Mutex::new(PARKFPS.fetch_up_to(pool.threads()));
    let scat = pool.run(
        frags.len(),
        |_| {
            let mut b = parked.lock().unwrap().pop().unwrap_or_default();
            if b.len() != CB {
                b = (0..CB).map(|_| Vec::new()).collect();
            } else {
                b.iter_mut().for_each(|v| v.clear());
            }
            b
        },
        |b: &mut Vec<Vec<FpFrag>>, i| {
            for &(f, key, c) in &frags[i] {
                b[(f >> 120) as usize].push((f, key, c));
            }
        },
    );
    let scatr = &scat;
    // [spill-2, shrink law] formerly UNCAPPED — table class cap.
    static PARKFPC: crate::stencils::statepark::StatePark<Vec<FpFrag>> = crate::stencils::statepark::StatePark::new(64 << 20);
    let folded = pool.run(
        CB,
        |_| (Vec::<(u64, u64, u64)>::new(), PARKFPC.fetch().unwrap_or_default()),
        |(out, slots): &mut (Vec<(u64, u64, u64)>, Vec<FpFrag>), bk| {
            let n: usize = scatr.iter().map(|b| b[bk].len()).sum();
            if n == 0 {
                return;
            }
            let cap = (n * 2).next_power_of_two().max(16);
            let mask = cap - 1;
            if slots.len() < cap {
                slots.resize(cap, (0, 0, 0));
            }
            for e in slots[..cap].iter_mut() {
                e.2 = 0;
            }
            let tbl = &mut slots[..cap];
            for b in scatr.iter() {
                for &(f, key, c) in &b[bk] {
                    let mut i = (crate::grouped::hash128(f) as usize) & mask;
                    loop {
                        let e = &mut tbl[i];
                        if e.2 == 0 {
                            *e = (f, key, c);
                            break;
                        }
                        if e.0 == f {
                            e.2 += c;
                            break;
                        }
                        i = (i + 1) & mask;
                    }
                }
            }
            let mut all: Vec<(u64, u64, u64)> = Vec::new();
            for e in tbl.iter() {
                if e.2 != 0 {
                    all.push((e.2, e.1, 0));
                }
            }
            if k > 0 && all.len() > k {
                let (_, nth, _) = all.select_nth_unstable_by(k - 1, |x, y| y.0.cmp(&x.0));
                let bar = nth.0;
                all.retain(|e| e.0 >= bar);
            }
            out.extend(all);
        },
    );
    let mut cands: Vec<(u64, u64, u64)> = Vec::new();
    for (o, slots) in folded {
        cands.extend(o);
        let b = crate::stencils::statepark::vec_bytes(&slots);
        PARKFPC.park(slots, b);
    }
    {
        let mut v = parked.into_inner().unwrap();
        v.extend(scat);
        for b in v {
            let bytes = crate::stencils::statepark::nested_bytes(&b);
            PARKFPS.park(b, bytes);
        }
    }
    cands
}

// ---------------------------------------------------------------------------
// STAGE-2 COMBINE: hash-partitioned fold at FRAGMENT grain. Consumers first
// fold exact (part-scoped key, aux) counts however they like (that fold
// never touches bytes); the fragments — one per (part, code, aux), i.e.
// GROUP grain per part — then get their key BYTES hashed once (read off
// the dict faces / side interns, no allocation), scatter into 256 hash
// buckets, and bucket owners fold fragments on (hash, aux) with byte
// equality checked ONLY on hash match. Output: per-bucket candidates
// under the count-only bar (tie-inclusive: any global top-k group's count
// >= the global k-th >= its bucket's k-th). At 511 parts this beats any
// k-way merge front (O(P) or O(log P) memcmps per group) — the measured
// 100m cliff on hot-shape.
// ---------------------------------------------------------------------------

/// (key, aux, cnt) — key is a psk or side key; aux any int lane (0 if none).
pub(crate) type Frag = (u64, u64, u64);

pub(crate) fn hash_combine(
    pool: &Pool,
    frags: &[Vec<Frag>],
    kb: &KeyBytes,
    k: usize,
) -> (Vec<(u64, u64, u64)>, u64, u64) {
    hash_combine_ord(pool, frags, kb, k, false)
}

/// `key_asc`: per-bucket exact top-k under (aux ASC, reg_ord ASC, bytes
/// ASC) — the registry-order KeyAsc shapes — instead of the count bar.
pub(crate) fn hash_combine_ord(
    pool: &Pool,
    frags: &[Vec<Frag>],
    kb: &KeyBytes,
    k: usize,
    key_asc: bool,
) -> (Vec<(u64, u64, u64)>, u64, u64) {
    const CB: usize = 256;
    let scat = pool.run(
        frags.len(),
        |_| (0..CB).map(|_| Vec::<(u64, u64, u64, u64)>::new()).collect::<Vec<_>>(),
        |b: &mut Vec<Vec<(u64, u64, u64, u64)>>, i| {
            crate::engine::census_entries(frags[i].len() as u64);
            for &(key, a, c) in &frags[i] {
                let h = crate::grouped::hash_bytes(kb.bytes(key)) ^ a.wrapping_mul(0x9E37_79B9_7F4A_7C15);
                b[(h >> 56) as usize].push((h, key, a, c));
            }
        },
    );
    let scatr = &scat;
    let folded = pool.run(
        CB,
        |_| (Vec::<(u64, u64, u64)>::new(), 0u64, 0u64, Vec::<(u64, u64, u64, u64)>::new()),
        |(out, g, r, slots): &mut (Vec<(u64, u64, u64)>, u64, u64, Vec<(u64, u64, u64, u64)>), bk| {
            let n: usize = scatr.iter().map(|b| b[bk].len()).sum();
            if n == 0 {
                return;
            }
            let cap = (n * 2).next_power_of_two().max(16);
            let mask = cap - 1;
            if slots.len() < cap {
                slots.resize(cap, (0, 0, 0, 0));
            }
            for e in slots[..cap].iter_mut() {
                e.3 = 0;
            }
            let slots = &mut slots[..cap];
            for b in scatr.iter() {
                for &(h, key, a, c) in &b[bk] {
                    let mut i = (h as usize) & mask;
                    loop {
                        let e = &mut slots[i];
                        if e.3 == 0 {
                            *e = (h, key, a, c);
                            break;
                        }
                        if e.0 == h && e.2 == a && kb.same(e.1, key) {
                            e.3 += c;
                            break;
                        }
                        i = (i + 1) & mask;
                    }
                }
            }
            let mut all: Vec<(u64, u64, u64)> = Vec::new();
            for e in slots.iter() {
                if e.3 != 0 {
                    all.push((e.3, e.1, e.2));
                    *r += e.3;
                }
            }
            *g += all.len() as u64;
            if k > 0 && all.len() > k {
                if key_asc {
                    // k smallest under (aux, reg_ord, bytes): select by the
                    // aux lane alone (tie-inclusive), THEN sort the tiny
                    // remainder with byte order — bytes touched only at
                    // the aux boundary.
                    let (_, nth, _) = all.select_nth_unstable_by(k - 1, |x, y| x.2.cmp(&y.2));
                    let bar = nth.2;
                    all.retain(|e| e.2 <= bar);
                    all.sort_unstable_by(|x, y| {
                        x.2.cmp(&y.2).then_with(|| {
                            let (bx, by) = (kb.bytes(x.1), kb.bytes(y.1));
                            reg_ord(bx).cmp(&reg_ord(by)).then_with(|| bx.cmp(by))
                        })
                    });
                    all.truncate(k);
                } else {
                    let (_, nth, _) = all.select_nth_unstable_by(k - 1, |x, y| y.0.cmp(&x.0));
                    let bar = nth.0;
                    all.retain(|e| e.0 >= bar);
                }
            }
            out.extend(all);
        },
    );
    let mut cands: Vec<(u64, u64, u64)> = Vec::new();
    let (mut groups, mut rows) = (0u64, 0u64);
    for (o, g, r, _) in folded {
        groups += g;
        rows += r;
        cands.extend(o);
    }
    (cands, groups, rows)
}

/// Dense per-part count arrays -> fragments (parallel over parts).
pub(crate) fn dense_to_frags(
    pool: &Pool,
    parts: &[(usize, Vec<u32>, Arc<DictFace>)],
    drop_empty: bool,
) -> Vec<Vec<Frag>> {
    let out = pool.run(
        parts.len(),
        |_| Vec::new(),
        |acc: &mut Vec<Vec<Frag>>, i| {
            let (pi, counts, df) = &parts[i];
            let mut v: Vec<Frag> = Vec::new();
            for (c, &n) in counts.iter().enumerate() {
                if n == 0 || (drop_empty && Some(c as u32) == df.empty_code) {
                    continue;
                }
                v.push((psk(*pi, c as u32), 0, n as u64));
            }
            acc.push(v);
        },
    );
    out.into_iter().flatten().collect()
}
