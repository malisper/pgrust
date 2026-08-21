//! Column-scan plumbing shared by the kernels: per-part cursors, granule
//! iteration, hydration scratch (datums + 8-aligned arena), null-freedom
//! assertion, dict faces.

use crate::bank::{binding, unwrappers, Bank};
use pgrc2_format::abi::{ByteArena, DecodeOut, Selection, ValidityVerdict};
use pgrc2_read::cursor::StreamCursor;
use pgrc2_read::dicthandle::DictHandle;
use pgrc2_read::ReadError;
use std::sync::Arc;

/// Reusable decode scratch. The arena is u64-backed for the 8-alignment
/// law; grows on ArenaExhausted exactly like the pgrc2_qa loop.
pub struct Scratch {
    pub datums: Vec<u64>,
    arena: Vec<u64>,
    n16: Vec<i16>,
    n32: Vec<i32>,
    n8: Vec<i8>,
    /// Validity bitmap words of the last `validity()` fetch (bit set =
    /// row valid). Untouched on the AllValid fast path.
    vwords: Vec<u64>,
}

/// Per-granule validity verdict of a decode face (P1-1 3VL threading,
/// currency-insertion.md §1). `AllValid` is THE fast path: NOT NULL
/// lanes take one metadata branch per granule (`has_validity_stream` is
/// a parsed-directory fact — no payload fault) and their row loops run
/// with `valid = true` hoisted out — byte-identical inner loops, zero
/// per-row cost (law 11).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GranValid {
    AllValid,
    /// Some rows NULL; the bitmap lives in `Scratch::vwords`
    /// (`Scratch::row_valid`).
    Mixed { nonnull: u32 },
}

impl GranValid {
    #[inline(always)]
    pub fn all_valid(self) -> bool {
        matches!(self, GranValid::AllValid)
    }
}

impl Scratch {
    pub fn new() -> Scratch {
        SCRATCH_FRESH.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Scratch {
            datums: vec![0u64; 8192],
            arena: vec![0u64; 512 * 1024], // 4 MiB
            n16: Vec::new(),
            n32: Vec::new(),
            n8: Vec::new(),
            vwords: Vec::new(),
        }
    }

    /// Reset for reuse across engagements (the depot law): validity and
    /// narrow-lane state clear, so nothing decoded under one statement can
    /// surface under the next — a stale `row_valid` read faults loud
    /// instead of answering stale. `datums`/`arena` keep capacity
    /// (grow-only); every window they expose is written by the decode
    /// that exposes it.
    pub fn reset(&mut self) {
        self.vwords.clear();
        self.n16.clear();
        self.n32.clear();
        self.n8.clear();
    }

    /// Held heap (capacity) — the depot's shrink-law currency.
    pub fn heap_bytes(&self) -> usize {
        self.datums.capacity() * 8
            + self.arena.capacity() * 8
            + self.n16.capacity() * 2
            + self.n32.capacity() * 4
            + self.n8.capacity()
            + self.vwords.capacity() * 8
    }

    /// Fetch granule `g`'s validity verdict (before or after decode —
    /// independent streams). AllValid short-circuits on the absent
    /// validity stream without touching the bitmap buffer.
    pub fn validity(&mut self, cur: &mut StreamCursor<'_>, g: u32, rows: usize) -> GranValid {
        if !cur.has_validity_stream() {
            return GranValid::AllValid;
        }
        let words = rows.div_ceil(64) + 1;
        if self.vwords.len() < words {
            self.vwords.resize(words, 0);
        }
        match cur.validity(g, &mut self.vwords).expect("validity stream") {
            ValidityVerdict::AllValid => GranValid::AllValid,
            ValidityVerdict::Mixed { nonnull } => GranValid::Mixed { nonnull },
        }
    }

    /// Row `i`'s validity per the LAST `validity()` fetch that answered
    /// Mixed. Callers on the AllValid path never call this.
    #[inline(always)]
    pub fn row_valid(&self, i: usize) -> bool {
        self.vwords[i >> 6] >> (i & 63) & 1 != 0
    }

    /// Decode granule `g` fully; returns (datums, arena_bounds).
    pub fn decode_full<'s>(
        &'s mut self,
        cur: &mut StreamCursor<'_>,
        g: u32,
        rows: usize,
    ) -> &'s [u64] {
        if self.datums.len() < rows {
            self.datums.resize(rows, 0);
        }
        loop {
            let arena_bytes = unsafe {
                std::slice::from_raw_parts_mut(
                    self.arena.as_mut_ptr() as *mut u8,
                    self.arena.len() * 8,
                )
            };
            let mut out = DecodeOut {
                datums: &mut self.datums[..rows],
                arena: ByteArena::new(arena_bytes),
            };
            match cur.decode_full(g, &mut out) {
                Ok(_) => break,
                Err(ReadError::Format(pgrc2_format::FormatError::ArenaExhausted { .. })) => {
                    let n = self.arena.len() * 2;
                    self.arena = vec![0u64; n];
                }
                Err(e) => panic!("decode_full: {e:?}"),
            }
        }
        &self.datums[..rows]
    }

    /// Native-width decode faces (phase-2A charter 1b). The reader ABI is
    /// u64-datums-only (`DecodeOut`), so these NARROW after the widened
    /// decode — they price the W2/W4/W1 lane penalty on the KERNEL side
    /// (8/16 lanes per 128b vector, 2-8× less state traffic), while the
    /// decode itself still pays the u64 currency. A real engine face would
    /// decode natively and save both; the delta measured through these is
    /// therefore a LOWER bound on the native-ABI win.
    pub fn decode_i16<'s>(
        &'s mut self,
        cur: &mut StreamCursor<'_>,
        g: u32,
        rows: usize,
    ) -> &'s [i16] {
        self.decode_full_internal(cur, g, rows);
        if self.n16.len() < rows {
            self.n16.resize(rows, 0);
        }
        for i in 0..rows {
            self.n16[i] = self.datums[i] as i16;
        }
        &self.n16[..rows]
    }

    pub fn decode_i32<'s>(
        &'s mut self,
        cur: &mut StreamCursor<'_>,
        g: u32,
        rows: usize,
    ) -> &'s [i32] {
        self.decode_full_internal(cur, g, rows);
        if self.n32.len() < rows {
            self.n32.resize(rows, 0);
        }
        for i in 0..rows {
            self.n32[i] = self.datums[i] as i32;
        }
        &self.n32[..rows]
    }

    pub fn decode_i8<'s>(
        &'s mut self,
        cur: &mut StreamCursor<'_>,
        g: u32,
        rows: usize,
    ) -> &'s [i8] {
        self.decode_full_internal(cur, g, rows);
        if self.n8.len() < rows {
            self.n8.resize(rows, 0);
        }
        for i in 0..rows {
            self.n8[i] = self.datums[i] as i8;
        }
        &self.n8[..rows]
    }

    fn decode_full_internal(&mut self, cur: &mut StreamCursor<'_>, g: u32, rows: usize) {
        if self.datums.len() < rows {
            self.datums.resize(rows, 0);
        }
        loop {
            let arena_bytes = unsafe {
                std::slice::from_raw_parts_mut(
                    self.arena.as_mut_ptr() as *mut u8,
                    self.arena.len() * 8,
                )
            };
            let mut out = DecodeOut {
                datums: &mut self.datums[..rows],
                arena: ByteArena::new(arena_bytes),
            };
            match cur.decode_full(g, &mut out) {
                Ok(_) => break,
                Err(ReadError::Format(pgrc2_format::FormatError::ArenaExhausted { .. })) => {
                    let n = self.arena.len() * 2;
                    self.arena = vec![0u64; n];
                }
                Err(e) => panic!("decode_full: {e:?}"),
            }
        }
    }

    /// decode_sel over explicit in-granule row ordinals.
    pub fn decode_sel<'s>(
        &'s mut self,
        cur: &mut StreamCursor<'_>,
        g: u32,
        sel: &[u16],
    ) -> &'s [u64] {
        let n = sel.len();
        if self.datums.len() < n {
            self.datums.resize(n, 0);
        }
        loop {
            let arena_bytes = unsafe {
                std::slice::from_raw_parts_mut(
                    self.arena.as_mut_ptr() as *mut u8,
                    self.arena.len() * 8,
                )
            };
            let mut out = DecodeOut {
                datums: &mut self.datums[..n],
                arena: ByteArena::new(arena_bytes),
            };
            match cur.decode_sel(g, &Selection { rows: sel }, &mut out) {
                Ok(_) => break,
                Err(ReadError::Format(pgrc2_format::FormatError::ArenaExhausted { .. })) => {
                    let n2 = self.arena.len() * 2;
                    self.arena = vec![0u64; n2];
                }
                Err(e) => panic!("decode_sel: {e:?}"),
            }
        }
        &self.datums[..n]
    }
}

// ---------------------------------------------------------------------------
// Worker scratch depot [scratch-init lane, width-ladder-cell.md finding 2]:
// pool workers are persistent, so their decode arenas persist with them.
// The warm engagement bill was NOT the wake (6µs/worker) but per-run
// Scratch init (a 4 MiB arena fault per decode column per worker per
// engagement — 29.4µs/worker on the CI cluster fold line). Stencil init
// closures `scratch_fetch()` instead of `Scratch::new()`, and
// `Pool::run_finish` parks them back ON THE WORKER THREAD at engagement
// end. Laws:
//   - shrink law: a thread parks at most DEPOT_CAP_BYTES (~8 default
//     arenas); a worker that once served a very wide column set does not
//     hold that high-water mark forever — overflow drops eagerly
//     (allocation law: never-free-HOT, bounded — the bound is the cap).
//   - no-leakage law: parked scratches are reset at BOTH park and fetch;
//     no statement's validity/length state is ever at rest in the depot.
//     A canceled generation parks only through the same reset gate
//     (cooperative break), and a panicking one drops its state on the
//     worker during unwind — freed, never parked.
//   - plain heap only: never CurCache/cursors — parking those pins bank
//     mmaps and can resurface another bank's (or column's) cursor.
//   - width currency: depots are worker-thread TLS, so the pool rebuild
//     on a width change retires them with the threads.
// ---------------------------------------------------------------------------

const DEPOT_CAP_BYTES: usize = 32 << 20;

static SCRATCH_FRESH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Process-wide count of fresh `Scratch` allocations (the zero-alloc
/// warm-engagement gate's instrument).
pub fn scratch_fresh_count() -> u64 {
    SCRATCH_FRESH.load(std::sync::atomic::Ordering::Relaxed)
}

thread_local! {
    // tls-dtor: plain-data — held type audited 2026-08-19: no Drop beyond plain collections/dealloc.
    static DEPOT: std::cell::RefCell<Vec<Scratch>> = const { std::cell::RefCell::new(Vec::new()) };
}

/// Kill switch (A/B + born-red hygiene): `SQE_SCRATCH_DEPOT=0` restores
/// the alloc-per-engagement behavior exactly (fetch allocates, park
/// drops).
fn depot_on() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var("SQE_SCRATCH_DEPOT").map_or(true, |v| v != "0"))
}

/// Fetch a reset scratch from this thread's depot (or allocate).
pub fn scratch_fetch() -> Scratch {
    if !depot_on() {
        return Scratch::new();
    }
    match DEPOT.with(|d| d.borrow_mut().pop()) {
        Some(mut s) => {
            s.reset();
            s
        }
        None => Scratch::new(),
    }
}

/// Park a scratch on this thread's depot, reset; dropped when the depot
/// is at its byte cap (the shrink law).
pub fn scratch_park(mut s: Scratch) {
    if !depot_on() {
        return;
    }
    s.reset();
    DEPOT.with(|d| {
        let mut d = d.borrow_mut();
        let held: usize = d.iter().map(Scratch::heap_bytes).sum();
        if held + s.heap_bytes() <= DEPOT_CAP_BYTES {
            d.push(s);
        }
    });
}

/// This thread's depot census: (parked count, parked bytes).
pub fn scratch_depot_stats() -> (usize, usize) {
    DEPOT.with(|d| {
        let d = d.borrow();
        (d.len(), d.iter().map(Scratch::heap_bytes).sum())
    })
}

/// Parse a varlena datum word into its payload bytes (4B-U image law; the
/// pointer targets the scratch arena or the part-resident dict region —
/// both live across the granule window we use them in).
#[inline(always)]
pub unsafe fn varlena_payload<'a>(d: u64) -> &'a [u8] {
    let hdr = std::ptr::read_unaligned(d as *const u32);
    let total = (hdr >> 2) as usize;
    std::slice::from_raw_parts((d + 4) as *const u8, total - 4)
}

/// [type-vocab] Parse a Fixed-face datum word into its image bytes: the
/// decoder's `alloc_fixed` pointer targets a headerless `len`-byte image
/// in the scratch arena (verbhot `vh_full_fixed`/`vh_sel_fixed`).
#[inline(always)]
pub unsafe fn fixed_payload<'a>(d: u64, len: u32) -> &'a [u8] {
    std::slice::from_raw_parts(d as *const u8, len as usize)
}

/// [type-vocab] The byte-face payload accessor: one dispatch for the two
/// byte faces (Varlena payload after the 4B-U header; Fixed raw image).
/// Callers gate word faces before this — a word datum is not a pointer.
#[inline(always)]
pub unsafe fn byte_payload<'a>(face: crate::bank::Face, d: u64) -> &'a [u8] {
    match face {
        crate::bank::Face::Varlena => varlena_payload(d),
        crate::bank::Face::Fixed(len) => fixed_payload(d, len),
        _ => panic!("byte_payload on a word face (lowering bug)"),
    }
}

/// Prove (once, cheap) that a column carries no validity stream in any
/// part — the NOT NULL exploitation. Consumed through the
/// cached `Bank::null_free`: the lowering's 3VL admission fact (null-
/// blind shapes refuse on `false`; the threaded shapes hoist the proof
/// out of their row loops). NOT NULL columns answer from the parsed
/// stream directory alone.
pub fn assert_null_free(bank: &Bank, attno: u32) -> bool {
    // [coldopen] The per-part validity walks are independent reads of a
    // sealed bank; at open width they run part-parallel (a disproof stops
    // the remaining workers early). Verdict identical to the serial walk.
    let n = bank.parts.len();
    let w = bank.open_width();
    if w > 1 && n > 1 {
        use std::sync::atomic::{AtomicBool, Ordering};
        let stop = AtomicBool::new(false);
        let per = crate::engine::par_parts_meta(w, n, |pi| {
            if stop.load(Ordering::Relaxed) {
                return true;
            }
            let ok = part_null_free(bank, pi, attno);
            if !ok {
                stop.store(true, Ordering::Relaxed);
            }
            ok
        });
        return per.into_iter().all(|b| b);
    }
    (0..n).all(|pi| part_null_free(bank, pi, attno))
}

fn part_null_free(bank: &Bank, pi: usize, attno: u32) -> bool {
    // [json-rung1] lane columns resolve to (parent attno, path_ord).
    let (sa, po) = bank.stream_key(pi, attno);
    let mut cur = StreamCursor::open(Arc::clone(&bank.parts[pi]), binding(), sa, po)
        .unwrap_or_else(|e| panic!("cursor attno {attno}: {e:?}"));
    if cur.has_validity_stream() {
        // Still fine if every granule answers AllValid.
        let mut vw = vec![0u64; 129];
        for g in 0..cur.granule_count() {
            match cur.validity(g, &mut vw) {
                Ok(ValidityVerdict::AllValid) => {}
                _ => return false,
            }
        }
    }
    true
}

/// Iterate (part_idx, granule, rows_in_granule, global_row_base).
pub fn granule_walk(bank: &Bank, attno: u32) -> Vec<(usize, u32, u32, u64)> {
    let mut out = Vec::new();
    let mut base = 0u64;
    for (pi, part) in bank.parts.iter().enumerate() {
        let (sa, po) = bank.stream_key(pi, attno);
        let cur = StreamCursor::open(Arc::clone(part), binding(), sa, po).expect("cursor");
        for g in 0..cur.granule_count() {
            let r = cur.rows_in_granule(g);
            out.push((pi, g, r, base));
            base += r as u64;
        }
    }
    out
}

/// [coldstart] Part-parallel granule walk: per-part granule row counts are
/// gathered by scoped workers (each part's cursor open is a cold pread on
/// a fresh page cache — 511 of them serially was the walk's first-touch
/// tax), then the global row base is prefix-summed serially. Output is
/// identical to `granule_walk`.
pub fn granule_walk_par(bank: &Bank, attno: u32, threads: usize) -> Vec<(usize, u32, u32, u64)> {
    let per: Vec<Vec<u32>> = crate::engine::par_parts_meta(threads, bank.parts.len(), |pi| {
        let (sa, po) = bank.stream_key(pi, attno);
        let cur = StreamCursor::open(Arc::clone(&bank.parts[pi]), binding(), sa, po)
            .expect("cursor");
        (0..cur.granule_count()).map(|g| cur.rows_in_granule(g)).collect()
    });
    let mut out = Vec::with_capacity(per.iter().map(|v| v.len()).sum());
    let mut base = 0u64;
    for (pi, rows) in per.iter().enumerate() {
        for (g, &r) in rows.iter().enumerate() {
            out.push((pi, g as u32, r, base));
            base += r as u64;
        }
    }
    out
}

pub fn open_cursor<'b>(bank: &Bank, part_idx: usize, attno: u32) -> StreamCursor<'static> {
    let (sa, po) = bank.stream_key(part_idx, attno);
    StreamCursor::open(Arc::clone(&bank.parts[part_idx]), binding(), sa, po)
        .unwrap_or_else(|e| panic!("cursor attno {attno} part {part_idx}: {e:?}"))
}

/// Whether the column's Values stream is dict-published in this part.
///
/// [cold2] The default arm answers from the STREAM DIRECTORY (presence of
/// the DictPayload stream for (attno, path 0)) — a metadata lookup, no
/// payload fault. The pre-fix arm (`PGRUST_SQE_DICTMETA=0`) answered via
/// `dict_payload_bounds`, whose `ensure_dict` ASSEMBLED THE WHOLE dict
/// payload region (every extent pread + CRC + memcpy) into a cursor that
/// was immediately dropped — one whole-payload fault per part per column
/// per face build, ahead of any entry touch (the hot-shape first-touch
/// ingredient). Equivalence: `dict_payload_bounds().is_some()` ⇔ the
/// cursor parsed a DictPayload stream ⇔ the directory lists one; asserted
/// transitively by the 43/43 identity gate on both arms.
pub fn is_dict(bank: &Bank, part_idx: usize, attno: u32) -> bool {
    use pgrc2_format::part::StreamRole;
    let (sa, po) = bank.stream_key(part_idx, attno);
    bank.parts[part_idx]
        .stream_directory()
        .map(|d| d.lookup(sa, po, StreamRole::DictPayload).is_some())
        .unwrap_or(false)
}

/// The §8.1 Stats SECTION body for (attno, path_ord 0) of one part:
/// granule StatsRecords, then band records, then ONE part record. `None`
/// when the part carries no stats section for the column.
pub fn stats_body(bank: &Bank, part_idx: usize, attno: u32) -> Option<Vec<u8>> {
    use pgrc2_format::part::{SectionKind, SECTIONF_META_ZSTD};
    // [fmt-layout] bank-grain stats plane first (the metadata stencil's
    // per-part §8.1 consult — F4 seq 1786958444 showed this unwired
    // reader paying 511 cold preads on hot-shape once the plane
    // stopped warming the adjacent per-part sections at plan time).
    // [json-rung1] the bank-grain plane predates shred lanes and is
    // keyed by root attno only — lane columns take the per-part section
    // path below (never wrong, only slower).
    let (sa, po) = bank.stream_key(part_idx, attno);
    if po == 0 {
        if let Some(pl) = bank.stats_plane.as_ref() {
            if let Some(body) = pl.try_stats_body(attno, part_idx) {
                return body;
            }
        }
    }
    let part = &bank.parts[part_idx];
    let idx = part.find_section(SectionKind::Stats, sa, po)?;
    let flags = part.sections()[idx].flags;
    let raw = part.section_bytes(idx).expect("stats section bytes");
    if flags & SECTIONF_META_ZSTD != 0 {
        Some(pgrc2_codec::wrapper::meta_unwrap_body(raw.bytes()).expect("meta unwrap"))
    } else {
        Some(raw.bytes().to_vec())
    }
}

pub fn stats_record(body: &[u8], idx: usize) -> Option<pgrc2_format::meta::StatsRecord> {
    use pgrc2_format::meta::{StatsRecord, STATS_RECORD_LEN};
    let rec = body.get(idx * STATS_RECORD_LEN..(idx + 1) * STATS_RECORD_LEN)?;
    StatsRecord::decode(&mut pgrc2_format::wire::Cur::new(rec)).ok()
}

// ---------------------------------------------------------------------------
// morsel-parallel driver (the engine's claim law in miniature): ONE shared
// atomic cursor over the unit list, dynamic claiming (never static slices —
// dynamic claiming is what absorbs skew), thread-local state merged by the
// caller. std::thread::scope only — zero external deps.
// ---------------------------------------------------------------------------

/// Claim indices 0..n dynamically across `threads` workers; return every
/// worker's final state (caller merges).
pub fn par_range<S, FI, FW>(n: usize, threads: usize, init: FI, work: FW) -> Vec<S>
where
    S: Send,
    FI: Fn(usize) -> S + Sync,
    FW: Fn(&mut S, usize) + Sync,
{
    use std::sync::atomic::{AtomicUsize, Ordering};
    let cursor = AtomicUsize::new(0);
    let cancel = crate::cancel::current();
    // Completion latch for the armed join: workers bump on EVERY exit
    // (drop guard — panic included) and notify, so the armer's poll wait
    // wakes the moment the last worker finishes instead of overshooting
    // into its backoff nap. [sqe-stmt-constant]: the old sleep-poll join
    // (50µs..5ms exponential naps) charged every armed par_range call a
    // mid-nap remainder — a 1-3ms per-statement server constant on the
    // varlena-filtered grouped shapes (rig arms nothing, so the native
    // arm never paid it; the §10b seven-violator band).
    struct Latch {
        m: std::sync::Mutex<usize>,
        cv: std::sync::Condvar,
    }
    struct Bump<'a>(&'a Latch);
    impl Drop for Bump<'_> {
        fn drop(&mut self) {
            *self.0.m.lock().unwrap() += 1;
            self.0.cv.notify_one();
        }
    }
    let latch = Latch { m: std::sync::Mutex::new(0), cv: std::sync::Condvar::new() };
    let (states, panicked) = std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for t in 0..threads {
            let cursor = &cursor;
            let init = &init;
            let work = &work;
            let cancel = &cancel;
            let latch = &latch;
            handles.push(scope.spawn(move || {
                let _bump = Bump(latch);
                let _inh = crate::cancel::inherit(cancel);
                let mut s = init(t);
                loop {
                    if crate::cancel::fired_of(cancel) {
                        break;
                    }
                    let i = cursor.fetch_add(1, Ordering::Relaxed);
                    if i >= n {
                        break;
                    }
                    work(&mut s, i);
                }
                s
            }));
        }
        // Armed join: event-driven wait on the completion latch, with a
        // bounded timeout so the interrupt poll keeps its sub-quantum
        // cadence (statement_timeout law: ~10ms). The latch bump is
        // drop-guarded, so a panicking worker still releases the wait.
        if crate::cancel::armed_here() {
            let mut done = latch.m.lock().unwrap();
            while *done < threads {
                let (g, _) = latch
                    .cv
                    .wait_timeout(done, std::time::Duration::from_millis(5))
                    .unwrap();
                done = g;
                if *done < threads {
                    drop(done);
                    crate::cancel::poll_now();
                    done = latch.m.lock().unwrap();
                }
            }
        }
        let mut states = Vec::with_capacity(handles.len());
        let mut panicked: Option<Box<dyn std::any::Any + Send>> = None;
        for h in handles {
            match h.join() {
                Ok(s) => states.push(s),
                Err(p) => {
                    let canceled =
                        p.is::<crate::cancel::Canceled>() && crate::cancel::fired_of(&cancel);
                    if !canceled && panicked.is_none() {
                        panicked = Some(p);
                    }
                }
            }
        }
        (states, panicked)
    });
    if let Some(p) = panicked {
        std::panic::resume_unwind(p);
    }
    crate::cancel::checkpoint();
    states
}

/// A thread-local cursor cache over one column: reopens only on part change
/// (claims are index-ordered, so per-thread part locality is high).
pub struct CurCache {
    attno: u32,
    cur: Option<(usize, StreamCursor<'static>)>,
}

impl CurCache {
    pub fn new(attno: u32) -> CurCache {
        CurCache { attno, cur: None }
    }
    pub fn get(&mut self, bank: &Bank, pi: usize) -> &mut StreamCursor<'static> {
        if self.cur.as_ref().map(|(p, _)| *p) != Some(pi) {
            self.cur = Some((pi, open_cursor(bank, pi, self.attno)));
        }
        &mut self.cur.as_mut().unwrap().1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The no-leakage law at the depot boundary: validity/narrow state
    /// parked dirty NEVER comes back — reset fires at park AND fetch, and
    /// a stale `row_valid` read faults (empty bitmap) instead of
    /// answering stale.
    #[test]
    fn depot_reset_clears_statement_state() {
        let mut s = scratch_fetch();
        s.vwords = vec![u64::MAX; 4];
        s.n16 = vec![7; 100];
        s.n32 = vec![7; 100];
        s.n8 = vec![7; 100];
        s.datums[0] = 0xDEAD_BEEF;
        let arena_cap = s.arena.capacity();
        scratch_park(s);
        let s2 = scratch_fetch();
        assert!(s2.vwords.is_empty(), "validity bitmap survived the depot");
        assert!(s2.n16.is_empty() && s2.n32.is_empty() && s2.n8.is_empty());
        // Persistence works: the arena allocation itself is the reused part.
        assert_eq!(s2.arena.capacity(), arena_cap, "arena was reallocated");
        scratch_park(s2);
    }

    /// The shrink law: a thread's depot never holds more than
    /// DEPOT_CAP_BYTES — overflow drops eagerly.
    #[test]
    fn depot_byte_cap_bounds_parked_memory() {
        std::thread::spawn(|| {
            let many: Vec<Scratch> = (0..20).map(|_| Scratch::new()).collect();
            for s in many {
                scratch_park(s);
            }
            let (n, bytes) = scratch_depot_stats();
            assert!(bytes <= DEPOT_CAP_BYTES, "depot over cap: {bytes}");
            assert!(n < 20, "depot kept every scratch (cap dead)");
            assert!(n > 0, "depot parked nothing");
        })
        .join()
        .unwrap();
    }
}

pub fn dict_handle(bank: &Bank, part_idx: usize, attno: u32) -> DictHandle {
    let (sa, po) = bank.stream_key(part_idx, attno);
    DictHandle::open(
        Arc::clone(&bank.parts[part_idx]),
        None,
        unwrappers(),
        sa,
        po,
    )
    .unwrap_or_else(|e| panic!("dict handle attno {attno}: {e:?}"))
}
