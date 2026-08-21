//! The framed lazy dict handle (spec §7; charter §6): per-(part, attno,
//! path_ord) access to the byte-rank-sorted global dictionary with lazy
//! section faulting, a GENERATION-STABLE payload region (the StrView §7b
//! zero-copy dependency), and varlena-shaped entry presentation.
//!
//! ## Generation stability (the pin `src/tests/dict.rs` enforces)
//!
//! Once a dict section is faulted, its bytes NEVER move for the handle's
//! life: the buffer is an `Arc<[u8]>` held in a write-once cell inside the
//! handle (and the handle holds the part). A pointer returned by
//! [`DictHandle::entry`] therefore stays valid and constant across part-cache
//! churn, registry eviction, and concurrent faulting — dict materialization
//! can hand out zero-copy views (`lanev3-strview.md` §4).
//!
//! ## The lx_vec seam (M3-G)
//!
//! `lx_vec::DictSpace` is implemented over this type by the lx_source
//! implementor: `ncodes` ← [`DictHandle::ncodes`], `entry_datum` ← the
//! [`DictHandle::entry`] image pointer (a valid PG datum by the §7b law),
//! byte/char length faces ← [`DictHandle::lengths`] (index-only — stored
//! lengths make `length()` a table lookup with NO payload fault), sorted
//! order ← the byte-rank contract (spec §7: sorted order is contractual).
//! The u64 epoch currency is [`DictHandle::epoch64`]; the structural
//! identity is [`DictHandle::epoch_key`] (Law A: codes are meaningless
//! across epochs).

use std::sync::Arc;

use pgrc2_format::dict::{dict_entry, DictEntryRef, DictSections, DICT_INDEX_ENTRY_LEN};
use pgrc2_format::enc::Wrapper;
use pgrc2_format::geom::DICT_FRAME_ENTRIES;
use pgrc2_format::part::{StreamRole, StreamSectionHdr, STREAMF_DICT_EXEC, STREAM_SECTION_HDR_LEN};
use pgrc2_format::FormatError;
use pgsync::atomic::{AtomicBool, AtomicPtr, AtomicU32, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use pgsync::Mutex;

use crate::cursor::{load_region, RegionState};
use crate::openpart::OpenPart;
use crate::registry::PartPin;
use crate::streams::ParsedStream;
use crate::{ReadError, ReadResult};

/// SB-7 payload fault mode. `FrameLazy` is the DESIGN DEFAULT (format-ledger
/// SB-7: frame-grain residency is format-default geometry); `WholeFault` is
/// the pre-L3 arm kept ONLY as the born-RED reproduction path — the
/// residency probe must show the difference (kill switch
/// `PGRUST_PGRC2_FRAME_LAZY=0`, the #802-class census discipline).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DictFaultMode {
    FrameLazy,
    WholeFault,
}

/// The frame-serve arm for frame-cut (SB-7 multi-extent, unwrapped)
/// payloads, stated explicitly — the env-independent probe face mirroring
/// [`DictFaultMode`]. `Direct` (production default) serves entries straight
/// from the part-cache `SegBuf`s; `Copy` is the frame-lazy memcpy arm that
/// preserves 8-aligned entry images (StrView §7b strict form; also the
/// `PGRUST_PGRC2_FRAME_DIRECT=0` kill-switch arm).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DictFrameServe {
    Direct,
    Copy,
}

impl DictFrameServe {
    fn from_env() -> DictFrameServe {
        if frame_direct() {
            DictFrameServe::Direct
        } else {
            DictFrameServe::Copy
        }
    }
}

/// [cold2→fd-arm] Frame-cut dict payloads serve DIRECT from the part cache
/// by default (no per-handle region, no frame memcpy) — armed default-ON
/// 2026-08-18 after the consumer audit found every production reader of
/// entry images byte-addressable (unaligned-safe header decodes; no
/// aligned casts over entry bytes — ADJUDICATION-20260818 §FRAME_DIRECT).
/// `PGRUST_PGRC2_FRAME_DIRECT=0` is the kill switch back to the frame-lazy
/// copy arm, which preserves 8-aligned entry images (StrView §7b strict
/// form). Read per open, like the fault mode.
fn frame_direct() -> bool {
    !matches!(std::env::var("PGRUST_PGRC2_FRAME_DIRECT").as_deref(), Ok("0"))
}

/// [stack] `PGRUST_PGRC2_DICT_BLOCK_LAZY=1` OPTS IN to block-lazy serving
/// of WRAPPED single-extent dict payloads (the banks-of-record geometry:
/// one zstd block per 1024-entry dict frame under the §6.4 wrapper —
/// RESULTS-FMTLAND §B). Default OFF after the 100m A/B (RESULTS-STACK
/// §4.2): the decompressed blocks are HANDLE-lifetime, and the per-query-
/// run ruling reopens dict handles every rep/query — the whole arm
/// re-serves from the part-cache's process-lifetime unwrapped map, while
/// block-lazy re-preads + re-decompresses per handle (hot 10-50x on the
/// dense dict families; only sparse consumers win). Re-pose = move block
/// residency to part grain (the `unwrapped`-map analogue at block grain).
/// [densedict] Block-lazy grain election. `1`/`on` = the HANDLE-grain arm
/// (the measured-negative §4.2 configuration, kept for the A/B);
/// `part`/`2` = the RE-POSE: block residency at PART grain — the
/// decompressed block image lives in the part cache (`OpenPart::
/// dict_block_payload`, the `unwrapped`-map analogue at block grain), so
/// handle reopens under the per-query-run ruling Arc-clone it instead of
/// re-preading + re-decompressing (one decompress per (part, block) per
/// part residency). Default OFF (whole-region assembly) either way.
/// [sqe8blk] `part` additionally carries the per-handle DENSE/SPARSE
/// election ([`DictTouch`]): dense demands stream the whole payload
/// (uncached CRC'd read — no raw-extent double cache), sparse demands
/// fetch blocks; undeclared handles elect from bytes touched. `=1`
/// (handle grain) keeps the measured §4.2 behavior for the A/B.
#[derive(Clone, Copy, PartialEq, Eq)]
enum BlockLazyGrain {
    Off,
    Handle,
    Part,
}

fn block_lazy() -> BlockLazyGrain {
    match std::env::var("PGRUST_PGRC2_DICT_BLOCK_LAZY").as_deref() {
        Ok("1") | Ok("on") => BlockLazyGrain::Handle,
        Ok("part") | Ok("2") => BlockLazyGrain::Part,
        _ => BlockLazyGrain::Off,
    }
}

/// [sqe8blk] The per-handle touch class a consumer MAY declare after open
/// (the RESULTS-DENSEDICT §3.1 banked dense/sparse election). On the
/// PART-grain block-lazy arm the handle elects, per demand, between
/// fetch-blocks (sub-extent preads at 1024-entry block grain — the sparse
/// floor economics: q25 336→81) and stream-whole (one sequential CRC'd
/// whole-extent read + decompress-all — the dense economics the gp2
/// measurement demands: q25 sqe-arm's 8,050 block preads / 171 MB LOST to
/// the orderly whole stream, +342 ms cold).
///
/// ELECTION INPUTS (per the house law — observable stats, never a
/// constant):
/// 1. The declared class, when the call site knows its touch pattern
///    (first declaration wins; byte-folding combines = Dense — those call
///    [`DictHandle::prewarm_payload`], which IS the dense election; a
///    zone-mask prewarm that PROVED sparsity declares Sparse so the
///    fallback below cannot un-decide it).
/// 2. Undeclared handles fall back to BYTES TOUCHED: the handle counts the
///    block faults its own demands caused and compares against the
///    payload's §6.4 block count (`nblocks`, from the block-offset table —
///    self-described geometry, not a tunable). Once this handle has
///    demand-faulted > nblocks/4 sparsely, the touch is dense by
///    observation and the REMAINDER is streamed whole (the same 1/4
///    crossover the masked-prewarm election measured; at that point ≥1/4
///    of the payload has already been paid for in random sub-extent
///    preads and one sequential stream of the rest is cheaper on the
///    measured volume class).
/// 3. A handle that witnesses the shared part-grain image fully resident
///    (at open or after any ensure) short-circuits every later ensure to
///    one flag load — the §3.1 "skip the bit probe once all-resident"
///    hot refinement (q33/34 +21 ms class).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DictTouch {
    /// Byte-folding / whole-domain consumer: first demand streams the
    /// whole payload (one CRC'd sequential read + decompress-all).
    Dense,
    /// Point-probe consumer (top-K, gathers, zone-masked prewarm): serve
    /// at block grain, NEVER escalate to the whole stream.
    Sparse,
}

/// `DictHandle::touch` encoding (write-once via compare_exchange).
const TOUCH_UNDECLARED: u8 = 0;
const TOUCH_DENSE: u8 = 1;
const TOUCH_SPARSE: u8 = 2;

/// [sqe8blk] Kill switch for the §3.1 refinements (uncached ensure_all
/// whole-read + per-handle election + all-resident short-circuit):
/// `PGRUST_PGRC2_BLOCKLAZY_REFINE=0` restores the densedict-measured
/// `=part` behavior — the part(old) A/B arm. Read per open/ensure_all
/// (rare, never hot).
fn blocklazy_refine() -> bool {
    !matches!(
        std::env::var("PGRUST_PGRC2_BLOCKLAZY_REFINE").as_deref(),
        Ok("0")
    )
}

impl DictFaultMode {
    /// Resolve from the environment (read per open — the harness flips the
    /// arm between probe legs; opens are rare, the read is never hot).
    fn from_env() -> DictFaultMode {
        match std::env::var("PGRUST_PGRC2_FRAME_LAZY") {
            Ok(v) if v == "0" || v.eq_ignore_ascii_case("off") => DictFaultMode::WholeFault,
            _ => DictFaultMode::FrameLazy,
        }
    }
}

// ---------------------------------------------------------------------------
// [cold2] Process-global dict payload fault census: bytes/frames the dict
// payload plane faulted (frame-lazy frame faults and whole-region
// assemblies), for the pgrcbench attribution witness (payload bytes
// faulted vs entries touched). Relaxed counters bumped only on FAULT
// events (rare — thousands per query), never on the entry() fast path.
// ---------------------------------------------------------------------------
pub static DICT_FRAME_FAULT_BYTES: core::sync::atomic::AtomicU64 =
    core::sync::atomic::AtomicU64::new(0);
pub static DICT_FRAME_FAULTS: core::sync::atomic::AtomicU64 =
    core::sync::atomic::AtomicU64::new(0);
pub static DICT_WHOLE_FAULT_BYTES: core::sync::atomic::AtomicU64 =
    core::sync::atomic::AtomicU64::new(0);
/// [stack] Block-lazy census: COMPRESSED bytes preaded at wrapper-block
/// grain, and the number of block faults (RESULTS-FMTLAND §B.3 serving).
pub static DICT_BLOCK_FAULT_BYTES: core::sync::atomic::AtomicU64 =
    core::sync::atomic::AtomicU64::new(0);
pub static DICT_BLOCK_FAULTS: core::sync::atomic::AtomicU64 =
    core::sync::atomic::AtomicU64::new(0);

/// Snapshot of the block-lazy census: (compressed_bytes_preaded, block_faults).
pub fn dict_block_census() -> (u64, u64) {
    use core::sync::atomic::Ordering::Relaxed;
    (
        DICT_BLOCK_FAULT_BYTES.load(Relaxed),
        DICT_BLOCK_FAULTS.load(Relaxed),
    )
}

/// Snapshot of the dict payload fault census:
/// (frame_fault_bytes, frame_faults, whole_region_bytes).
pub fn dict_fault_census() -> (u64, u64, u64) {
    use core::sync::atomic::Ordering::Relaxed;
    (
        DICT_FRAME_FAULT_BYTES.load(Relaxed),
        DICT_FRAME_FAULTS.load(Relaxed),
        DICT_WHOLE_FAULT_BYTES.load(Relaxed),
    )
}

/// The structural dict-epoch identity (spec §7): a dict epoch IS
/// (part identity, attno, path_ord). Layout-pinned (24 B). The definition
/// moved to the frozen ABI crate at M3-L3 (AB-2.2 names it the dict-code
/// lane's per-batch epoch tag identity); this re-export keeps the reader's
/// public face unchanged.
pub use pgrc2_batch::DictEpochKey;

/// One lazily-faulted region publication: the write-once cell owns the
/// bytes; (ptr, len) are published AFTER the cell is written and never
/// change — the generation-stability mechanism.
struct LazyRegion {
    cell: Mutex<Option<RegionState>>,
    ptr: AtomicPtr<u8>,
    len: AtomicUsize,
}

impl LazyRegion {
    fn new() -> LazyRegion {
        LazyRegion {
            cell: Mutex::new(None),
            ptr: AtomicPtr::new(core::ptr::null_mut()),
            len: AtomicUsize::new(0),
        }
    }

    /// Fast path: the published region, if faulted.
    fn get(&self) -> Option<(*const u8, usize)> {
        let p = self.ptr.load(Ordering::Acquire);
        if p.is_null() {
            return None;
        }
        Some((p as *const u8, self.len.load(Ordering::Acquire)))
    }

    /// Fault-and-publish (once). Racing callers serialize on the cell lock;
    /// exactly one stores, all see the same region afterwards.
    fn ensure(
        &self,
        part: &Arc<OpenPart>,
        unwrappers: &[&dyn crate::SectionUnwrapper],
        ps: &ParsedStream,
        what: &'static str,
    ) -> ReadResult<(*const u8, usize)> {
        let census_payload = what == "dict payload stream";
        if let Some(r) = self.get() {
            return Ok(r);
        }
        let mut cell = match self.cell.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        if cell.is_none() {
            let region = load_region(part, unwrappers, ps, what)?;
            let payload = region.payload();
            let ptr = payload.as_ptr() as *mut u8;
            let len = payload.len();
            *cell = Some(region);
            // Publish AFTER the owning cell is written (Release pairs with
            // the Acquire in `get`): a non-null ptr proves the Arc'd buffer
            // is rooted in the cell for this handle's lifetime.
            self.len.store(len, Ordering::Release);
            self.ptr.store(ptr, Ordering::Release);
            if census_payload {
                DICT_WHOLE_FAULT_BYTES
                    .fetch_add(len as u64, core::sync::atomic::Ordering::Relaxed);
            }
        }
        let region = cell.as_ref().expect("just stored");
        let payload = region.payload();
        Ok((payload.as_ptr(), payload.len()))
    }
}

/// Frame-lazy interior state: the write-once whole-region reservation the
/// per-frame faults fill in place.
struct FrameLazyState {
    /// The stable backing store, sized to the WHOLE assembled region and
    /// allocated ZEROED (`alloc_zeroed` ⇒ untouched pages stay uncommitted
    /// — reserving 3 GB costs address space, faulted frames cost RSS).
    /// NEVER reallocated: base pointer stability for the handle's life is
    /// the StrView §7b dependency, exactly as for the whole-fault arm.
    buf: Box<[u64]>,
    len: usize,
    /// Region-relative start offset of each extent (frame), from the
    /// contiguity-validated extent table.
    offsets: Vec<usize>,
}

/// SB-7 frame-lazy payload publication (M3-L3): the contractual dict fault
/// grain (the frame) made REAL on the v4 writer's frame-boundary extent
/// geometry. Extent i carries dict frame i (per-frame CRC; extent 0 also
/// carries the section header, the last extent also the frame-table tail).
/// `ensure_extent(f)` faults exactly extent f — a first entry touch no
/// longer faults the 3.01/2.62/2.27 GB payload class (the q23 memory
/// ingredient the ledger row records).
struct FrameLazyPayload {
    cell: Mutex<Option<FrameLazyState>>,
    /// Published whole-region base + len + payload bounds (Release after
    /// the cell owns the buffer; non-null ptr proves rooting — the same
    /// publication idiom as [`LazyRegion`]).
    ptr: AtomicPtr<u8>,
    len: AtomicUsize,
    payload_start: AtomicUsize,
    payload_end: AtomicUsize,
    /// Per-frame publication bits (Release on store; Acquire on the fast
    /// path) — bit i set ⇔ extent i's bytes are resident and immutable.
    frame_bits: Vec<AtomicU64>,
    /// Faulted payload bytes — the residency census term
    /// (`pgrc2_dict_frames_resident` class; the born-RED whole-fault arm
    /// shows the whole region here).
    resident: AtomicUsize,
    frames: u32,
}

impl FrameLazyPayload {
    fn new(nextents: usize) -> FrameLazyPayload {
        let words = nextents.div_ceil(64);
        FrameLazyPayload {
            cell: Mutex::new(None),
            ptr: AtomicPtr::new(core::ptr::null_mut()),
            len: AtomicUsize::new(0),
            payload_start: AtomicUsize::new(0),
            payload_end: AtomicUsize::new(0),
            frame_bits: (0..words).map(|_| AtomicU64::new(0)).collect(),
            resident: AtomicUsize::new(0),
            frames: nextents as u32,
        }
    }

    #[inline]
    fn frame_resident(&self, f: u32) -> bool {
        let w = (f / 64) as usize;
        self.frame_bits[w].load(Ordering::Acquire) & (1u64 << (f % 64)) != 0
    }

    /// The published region, if extent 0 (header) has been faulted.
    fn get(&self) -> Option<(*const u8, usize, usize, usize)> {
        let p = self.ptr.load(Ordering::Acquire);
        if p.is_null() {
            return None;
        }
        Some((
            p as *const u8,
            self.len.load(Ordering::Acquire),
            self.payload_start.load(Ordering::Acquire),
            self.payload_end.load(Ordering::Acquire),
        ))
    }

    /// Fault extent (= dict frame) `f` into the reserved region; always
    /// faults extent 0 first (header decode publishes the region). Racing
    /// callers serialize on the cell lock; frame bytes are written exactly
    /// once and published with Release — once a frame bit is set, its
    /// bytes never change (generation stability at frame grain).
    fn ensure_extent(
        &self,
        part: &Arc<OpenPart>,
        ps: &ParsedStream,
        f: u32,
        what: &'static str,
    ) -> ReadResult<()> {
        if f >= self.frames {
            return Err(ReadError::Format(FormatError::Bounds { at: "dict frame" }));
        }
        if self.frame_resident(f) && !self.ptr.load(Ordering::Acquire).is_null() {
            return Ok(());
        }
        let mut cell = match self.cell.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        if cell.is_none() {
            // First fault on this handle: validate the extent geometry the
            // whole-fault path validates (contiguity; the SB-7 cut), size
            // and reserve the region.
            for w in ps.extents.windows(2) {
                if w[0].file_off + w[0].len != w[1].file_off {
                    return Err(ReadError::Format(FormatError::Corrupt {
                        at: "byte-run extent contiguity",
                    }));
                }
            }
            let total: usize = ps.extents.iter().map(|e| e.len as usize).sum();
            let mut offsets = Vec::with_capacity(ps.extents.len());
            let mut off = 0usize;
            for e in &ps.extents {
                offsets.push(off);
                off += e.len as usize;
            }
            *cell = Some(FrameLazyState {
                buf: vec![0u64; total.div_ceil(8)].into_boxed_slice(),
                len: total,
                offsets,
            });
        }
        let state = cell.as_mut().expect("just stored");
        let base = state.buf.as_mut_ptr() as *mut u8;
        let state_len = state.len;
        let offsets = &state.offsets;
        let fault_one = |i: u32| -> ReadResult<()> {
            if self.frame_resident(i) {
                return Ok(());
            }
            let rec = &ps.extents[i as usize];
            let seg = part.extent_bytes(&ps.entry, rec, i)?;
            let off = offsets[i as usize];
            debug_assert!(off + seg.len() <= state_len, "extent beyond region");
            // SAFETY: raw-pointer write into the never-reallocated,
            // cell-owned buffer; the target frame range is UNPUBLISHED
            // (its bit is clear), no shared reference reads unpublished
            // bytes (readers gate every dereference on ensure_code →
            // frame bit, Acquire), and all writes serialize on the cell
            // lock. Never a `&mut` over the buffer — published frames are
            // concurrently read.
            unsafe {
                core::ptr::copy_nonoverlapping(seg.bytes().as_ptr(), base.add(off), seg.len());
            }
            self.resident.fetch_add(seg.len(), Ordering::Relaxed);
            DICT_FRAME_FAULT_BYTES
                .fetch_add(seg.len() as u64, core::sync::atomic::Ordering::Relaxed);
            DICT_FRAME_FAULTS.fetch_add(1, core::sync::atomic::Ordering::Relaxed);
            let w = (i / 64) as usize;
            self.frame_bits[w].fetch_or(1u64 << (i % 64), Ordering::Release);
            Ok(())
        };
        // Extent 0 first: the header lives there, and publishing the
        // region requires its decode.
        fault_one(0)?;
        if self.ptr.load(Ordering::Acquire).is_null() {
            let bytes = unsafe {
                core::slice::from_raw_parts(state.buf.as_ptr() as *const u8, state.len)
            };
            let hdr = StreamSectionHdr::decode(bytes)?;
            if hdr.encoding != ps.entry.encoding || hdr.wrapper != ps.entry.wrapper {
                return Err(ReadError::Format(FormatError::Corrupt { at: what }));
            }
            let (pstart, pend) = crate::cursor::payload_bounds(&hdr, state.len)?;
            self.payload_start.store(pstart as usize, Ordering::Release);
            self.payload_end.store(pend as usize, Ordering::Release);
            self.len.store(state.len, Ordering::Release);
            self.ptr
                .store(state.buf.as_ptr() as *mut u8, Ordering::Release);
        }
        fault_one(f)
    }
}

/// [cold2] FRAME-DIRECT payload serving: entries are resolved straight
/// from the part-cache `SegBuf`s (one per frame-cut extent) — no
/// per-handle whole-region reservation and NO second memcpy per frame.
/// A frame the claim-horizon prefetcher already landed is an `Arc` clone
/// here (zero copies past the pread). Generation stability holds: the
/// `SegBuf`s are write-once in the per-extent `OnceLock`s and live for
/// the handle's life. THE TRADE: entry images can lose the §7b 8-align
/// datum guarantee (frame cuts land at the UNPADDED end of the previous
/// frame, so a frame's first entry sits at a non-8 offset inside its
/// SegBuf — the 8-align pin fails on this arm by measurement). The
/// 2026-08-18 consumer audit verified every production reader of entry
/// images — sqe kernels (`entry().bytes` compares), the scan-side datum
/// publishers (`ScanDictSpace::entry_datum`, the checked-gather lane) and
/// their downstream varlena readers (`varsize_any` family, StrView cells,
/// hashfn, tuple materialization) — reads entry bytes through
/// unaligned-safe byte accesses, so the arm is the production DEFAULT;
/// `PGRUST_PGRC2_FRAME_DIRECT=0` (kill switch) restores the copy arm and
/// with it the strict 8-align form.
struct FrameDirectPayload {
    /// Per-extent resident frames (write-once).
    segs: Vec<pgsync::OnceLock<crate::openpart::SegBuf>>,
    /// Region-relative start offset of each extent; published with the
    /// geometry after extent 0's header decode.
    geom: pgsync::OnceLock<FrameDirectGeom>,
    resident: AtomicUsize,
    frames: u32,
}

struct FrameDirectGeom {
    offsets: Vec<usize>,
    payload_start: usize,
    payload_end: usize,
}

impl FrameDirectPayload {
    fn new(nextents: usize) -> FrameDirectPayload {
        FrameDirectPayload {
            segs: (0..nextents).map(|_| pgsync::OnceLock::new()).collect(),
            geom: pgsync::OnceLock::new(),
            resident: AtomicUsize::new(0),
            frames: nextents as u32,
        }
    }

    /// Fault extent `f` (and extent 0 for the header, once); publishes the
    /// geometry after extent 0. Same contiguity validation as frame-lazy.
    fn ensure_extent(
        &self,
        part: &Arc<OpenPart>,
        ps: &ParsedStream,
        f: u32,
        what: &'static str,
    ) -> ReadResult<()> {
        if f >= self.frames {
            return Err(ReadError::Format(FormatError::Bounds { at: "dict frame" }));
        }
        if self.geom.get().is_none() {
            for w in ps.extents.windows(2) {
                if w[0].file_off + w[0].len != w[1].file_off {
                    return Err(ReadError::Format(FormatError::Corrupt {
                        at: "byte-run extent contiguity",
                    }));
                }
            }
            let total: usize = ps.extents.iter().map(|e| e.len as usize).sum();
            let mut offsets = Vec::with_capacity(ps.extents.len());
            let mut off = 0usize;
            for e in &ps.extents {
                offsets.push(off);
                off += e.len as usize;
            }
            // Extent 0 carries the section header: decode it to bound the
            // payload window, then publish the geometry once.
            let seg0 = self.fault_seg(part, ps, 0)?;
            let hdr = StreamSectionHdr::decode(seg0.bytes())?;
            if hdr.encoding != ps.entry.encoding || hdr.wrapper != ps.entry.wrapper {
                return Err(ReadError::Format(FormatError::Corrupt { at: what }));
            }
            let (pstart, pend) = crate::cursor::payload_bounds(&hdr, total)?;
            let _ = self.geom.set(FrameDirectGeom {
                offsets,
                payload_start: pstart as usize,
                payload_end: pend as usize,
            });
        }
        self.fault_seg(part, ps, f)?;
        Ok(())
    }

    fn fault_seg(
        &self,
        part: &Arc<OpenPart>,
        ps: &ParsedStream,
        i: u32,
    ) -> ReadResult<&crate::openpart::SegBuf> {
        if let Some(seg) = self.segs[i as usize].get() {
            return Ok(seg);
        }
        let rec = &ps.extents[i as usize];
        let seg = part.extent_bytes(&ps.entry, rec, i)?;
        let len = seg.bytes().len();
        let cell = &self.segs[i as usize];
        if cell.set(seg).is_ok() {
            self.resident.fetch_add(len, Ordering::Relaxed);
            DICT_FRAME_FAULT_BYTES.fetch_add(len as u64, core::sync::atomic::Ordering::Relaxed);
            DICT_FRAME_FAULTS.fetch_add(1, core::sync::atomic::Ordering::Relaxed);
        }
        Ok(cell.get().expect("just set"))
    }

    /// Resolve entry bytes for `code` frame-locally (index rec already
    /// decoded by the caller): `(image, bytes)` out of frame
    /// `code / DICT_FRAME_ENTRIES`'s SegBuf.
    fn entry_slices(
        &self,
        code: u32,
        payload_off: usize,
    ) -> ReadResult<(&[u8], &[u8])> {
        let f = (code / DICT_FRAME_ENTRIES) as usize;
        let geom = self.geom.get().ok_or(ReadError::Format(FormatError::Corrupt {
            at: "dict payload geometry unpublished after ensure",
        }))?;
        let seg = self.segs[f]
            .get()
            .ok_or(ReadError::Format(FormatError::Corrupt {
                at: "dict frame unresident after ensure",
            }))?;
        let region_off = geom.payload_start + payload_off;
        if region_off >= geom.payload_end {
            return Err(ReadError::Format(FormatError::Bounds { at: "dict payload" }));
        }
        let lo = region_off
            .checked_sub(geom.offsets[f])
            .ok_or(ReadError::Format(FormatError::Bounds { at: "dict payload" }))?;
        pgrc2_format::wire::varlena_entry_at(seg.bytes(), lo, "dict payload")
            .map_err(Into::into)
    }
}

/// [stack] BLOCK-LAZY serving of WRAPPED single-extent dict payloads — the
/// reader-only partial unwrap RESULTS-FMTLAND §B proved viable on the banks
/// of record (every wrapped dict payload is block-cut at 1024-entry dict-
/// frame grain with a §6.4 block-offset table at the head of the payload
/// region and the frame table at the raw tail). Serving chain: entry code →
/// frame = code/1024 → block (1:1 when the geometry is valid) → sub-extent
/// pread of just that compressed block (~40-300 KB) → one-block decompress
/// into a write-once reserved region at the block's uncompressed span.
/// Generation stability and §7b alignment hold exactly as on the whole-
/// fault arm: the reserved buffer never moves, and the uncompressed image
/// bytes land at the same payload-relative offsets the whole unwrap would
/// put them at. Integrity: sub-extent preads cannot be checked against the
/// extent's whole-range CRC — per-block zstd decode failure is the typed
/// witness (the bulk `ensure_all` path reads the whole extent CRC-checked).
/// Any geometry the probe would skip (no frame table, block/frame count
/// skew, binding without block decode) falls back to the whole-region
/// unwrap — never wrong, only whole.
struct BlockLazyState {
    /// Reserved zeroed uncompressed-payload image (`alloc_zeroed` ⇒
    /// untouched blocks stay uncommitted). NEVER reallocated. Empty on the
    /// whole-fallback path (the region below roots the bytes instead).
    buf: Box<[u64]>,
    /// Uncompressed payload length (`hdr.uncompressed_len`).
    len: usize,
    /// Compressed block offsets, payload-relative, `nb + 1` entries
    /// (§6.4 block-offset table; entry 0 = the table's own length).
    btab: Vec<u32>,
    /// Uncompressed frame starts, payload-relative, `nb` entries.
    ftab: Vec<u32>,
    /// Whole-fallback rooting: the assembled region (wrapped geometry the
    /// block path refused), published instead of `buf`.
    whole: Option<RegionState>,
}

pub(crate) struct BlockLazyPayload {
    cell: Mutex<Option<BlockLazyState>>,
    /// Per-block publication bits (sized at first fault; Release on store,
    /// Acquire on the fast path) — bit b set ⇔ block b's uncompressed
    /// bytes are resident and immutable. Whole-fallback publishes ONE
    /// all-set bit.
    bits: pgsync::OnceLock<Vec<AtomicU64>>,
    nblocks: AtomicUsize,
    /// Published payload window (base, len): Release after the cell owns
    /// the backing store — the same idiom as [`LazyRegion`].
    ptr: AtomicPtr<u8>,
    len: AtomicUsize,
    /// Faulted UNCOMPRESSED payload bytes (the residency census term).
    resident: AtomicUsize,
    /// [sqe8blk] Published block count (Release AFTER each block's bit —
    /// under the cell lock, so `== nblocks` with Acquire proves every
    /// block's bytes are visible). Feeds [`BlockLazyPayload::
    /// fully_resident`], the handle's all-resident short-circuit.
    blocks_resident: AtomicUsize,
}

impl BlockLazyPayload {
    pub(crate) fn new() -> BlockLazyPayload {
        BlockLazyPayload {
            cell: Mutex::new(None),
            bits: pgsync::OnceLock::new(),
            nblocks: AtomicUsize::new(0),
            ptr: AtomicPtr::new(core::ptr::null_mut()),
            len: AtomicUsize::new(0),
            resident: AtomicUsize::new(0),
            blocks_resident: AtomicUsize::new(0),
        }
    }

    /// [sqe8blk] Every block published (whole-fallback counts as its one
    /// block). All publications serialize on the cell lock and bump
    /// `blocks_resident` with Release AFTER the bit + bytes, so a true
    /// answer here proves every block's bytes are visible to the caller.
    pub(crate) fn fully_resident(&self) -> bool {
        let nb = self.nblocks.load(Ordering::Acquire);
        nb > 0 && self.blocks_resident.load(Ordering::Acquire) == nb
    }

    fn get(&self) -> Option<(*const u8, usize)> {
        let p = self.ptr.load(Ordering::Acquire);
        if p.is_null() {
            return None;
        }
        Some((p as *const u8, self.len.load(Ordering::Acquire)))
    }

    #[inline]
    fn block_resident(&self, b: usize) -> bool {
        match self.bits.get() {
            Some(bits) => bits[b / 64].load(Ordering::Acquire) & (1u64 << (b % 64)) != 0,
            None => false,
        }
    }

    /// Map a dict frame to its block ordinal (1:1 on valid geometry; the
    /// whole-fallback path has one all-set block).
    #[inline]
    fn block_of_frame(&self, frame: u32) -> usize {
        let nb = self.nblocks.load(Ordering::Acquire);
        (frame as usize).min(nb.saturating_sub(1))
    }

    /// Initialize the state under the cell lock: head pread (header +
    /// block-offset table), tail pread (frame table), geometry validation,
    /// region reservation. Falls back to the whole-region unwrap on any
    /// geometry the block path cannot prove.
    fn init_state(
        &self,
        part: &Arc<OpenPart>,
        unwrappers: &[&dyn crate::SectionUnwrapper],
        ps: &ParsedStream,
        what: &'static str,
    ) -> ReadResult<BlockLazyState> {
        let rec = &ps.extents[0];
        let try_blocks = || -> ReadResult<Option<BlockLazyState>> {
            let head_len = rec.len.min(4096);
            if (head_len as usize) < STREAM_SECTION_HDR_LEN + 8 {
                return Ok(None);
            }
            let head = part.subrange_bytes(rec, 0, head_len, what)?;
            let hdr = StreamSectionHdr::decode(head.bytes())?;
            if hdr.encoding != ps.entry.encoding || hdr.wrapper != ps.entry.wrapper {
                return Err(ReadError::Format(FormatError::Corrupt { at: what }));
            }
            let unc_len = hdr.uncompressed_len as usize;
            if hdr.frame_table_off == 0 || unc_len == 0 {
                return Ok(None);
            }
            let payload_end = hdr.frame_table_off as usize;
            if payload_end < STREAM_SECTION_HDR_LEN + 8 || payload_end as u64 > rec.len {
                return Ok(None);
            }
            let first = u32::from_le_bytes(
                head.bytes()[STREAM_SECTION_HDR_LEN..STREAM_SECTION_HDR_LEN + 4]
                    .try_into()
                    .expect("len 4"),
            ) as usize;
            if first < 8 || first % 4 != 0 || first > payload_end - STREAM_SECTION_HDR_LEN {
                return Ok(None);
            }
            let nb = first / 4 - 1;
            if nb < 1 || hdr.frame_count as usize != nb {
                // Block/frame skew (or a single block): the partial win is
                // zero — whole assembly is strictly simpler.
                return Ok(None);
            }
            // Block-offset table: re-read if the fixed head missed it.
            let table = if STREAM_SECTION_HDR_LEN + first <= head.bytes().len() {
                head
            } else {
                part.subrange_bytes(
                    rec,
                    0,
                    (STREAM_SECTION_HDR_LEN + first) as u64,
                    what,
                )?
            };
            let tb = &table.bytes()[STREAM_SECTION_HDR_LEN..STREAM_SECTION_HDR_LEN + first];
            let btab: Vec<u32> = (0..=nb)
                .map(|i| u32::from_le_bytes(tb[i * 4..i * 4 + 4].try_into().expect("len 4")))
                .collect();
            let comp_len = (payload_end - STREAM_SECTION_HDR_LEN) as u32;
            if btab.windows(2).any(|w| w[0] > w[1]) || *btab.last().expect("nonempty") > comp_len
            {
                return Ok(None);
            }
            // Frame table at the raw tail: uncompressed frame starts.
            let tail = part.subrange_bytes(
                rec,
                hdr.frame_table_off as u64,
                (nb * 4) as u64,
                what,
            )?;
            let ftab: Vec<u32> = (0..nb)
                .map(|i| {
                    u32::from_le_bytes(
                        tail.bytes()[i * 4..i * 4 + 4].try_into().expect("len 4"),
                    )
                })
                .collect();
            if ftab[0] != 0
                || ftab.windows(2).any(|w| w[0] > w[1])
                || *ftab.last().expect("nonempty") as usize > unc_len
            {
                return Ok(None);
            }
            Ok(Some(BlockLazyState {
                buf: vec![0u64; unc_len.div_ceil(8)].into_boxed_slice(),
                len: unc_len,
                btab,
                ftab,
                whole: None,
            }))
        };
        if let Some(st) = try_blocks()? {
            let nb = st.ftab.len();
            let _ = self.bits.set((0..nb.div_ceil(64)).map(|_| AtomicU64::new(0)).collect());
            self.nblocks.store(nb, Ordering::Release);
            self.len.store(st.len, Ordering::Release);
            self.ptr.store(st.buf.as_ptr() as *mut u8, Ordering::Release);
            return Ok(st);
        }
        // Whole-region fallback: exactly today's assembly (CRC-validated
        // whole extent + whole unwrap), published as one all-set block.
        let region = load_region(part, unwrappers, ps, what)?;
        let payload = region.payload();
        let (rptr, rlen) = (payload.as_ptr() as *mut u8, payload.len());
        DICT_WHOLE_FAULT_BYTES.fetch_add(rlen as u64, core::sync::atomic::Ordering::Relaxed);
        self.resident.fetch_add(rlen, Ordering::Relaxed);
        let st = BlockLazyState {
            buf: Box::new([]),
            len: rlen,
            btab: Vec::new(),
            ftab: Vec::new(),
            whole: Some(region),
        };
        let _ = self.bits.set(vec![AtomicU64::new(1)]);
        self.nblocks.store(1, Ordering::Release);
        self.blocks_resident.store(1, Ordering::Release);
        self.len.store(rlen, Ordering::Release);
        self.ptr.store(rptr, Ordering::Release);
        Ok(st)
    }

    /// Ensure the block backing dict frame `frame` is resident: one
    /// sub-extent pread of the compressed block + a one-block decompress
    /// into the reserved region. Returns `true` iff THIS call faulted a
    /// block at block grain (a sub-extent pread happened) — the [sqe8blk]
    /// bytes-touched election input.
    fn ensure_frame_block(
        &self,
        part: &Arc<OpenPart>,
        unwrappers: &[&dyn crate::SectionUnwrapper],
        ps: &ParsedStream,
        frame: u32,
        what: &'static str,
    ) -> ReadResult<bool> {
        if !self.ptr.load(Ordering::Acquire).is_null()
            && self.block_resident(self.block_of_frame(frame))
        {
            return Ok(false);
        }
        let mut cell = match self.cell.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        if cell.is_none() {
            *cell = Some(self.init_state(part, unwrappers, ps, what)?);
        }
        let state = cell.as_mut().expect("just stored");
        if state.whole.is_some() {
            return Ok(false); // whole-fallback: everything is resident
        }
        let nb = state.ftab.len();
        let b = (frame as usize).min(nb - 1);
        if self.block_resident(b) {
            return Ok(false);
        }
        self.fault_block(part, unwrappers, ps, state, b, what)
    }

    /// Fault ONE block (cell lock held, bit b clear): sub-extent pread +
    /// one-block decompress into the unpublished span, then publish bit b.
    /// Returns `true` on a block-grain fault, `false` when the binding
    /// forced the whole-fallback (everything resident afterwards).
    fn fault_block(
        &self,
        part: &Arc<OpenPart>,
        unwrappers: &[&dyn crate::SectionUnwrapper],
        ps: &ParsedStream,
        state: &mut BlockLazyState,
        b: usize,
        what: &'static str,
    ) -> ReadResult<bool> {
        let nb = state.ftab.len();
        let (c0, c1) = (state.btab[b] as usize, state.btab[b + 1] as usize);
        let f0 = state.ftab[b] as usize;
        let f1 = if b + 1 < nb { state.ftab[b + 1] as usize } else { state.len };
        let rec = &ps.extents[0];
        let seg = part.subrange_bytes(
            rec,
            (STREAM_SECTION_HDR_LEN + c0) as u64,
            (c1 - c0) as u64,
            what,
        )?;
        let wrapper = pgrc2_format::enc::Wrapper::from_u8(ps.entry.wrapper)?;
        let u = unwrappers
            .iter()
            .find(|u| u.wrapper() == wrapper)
            .ok_or(ReadError::Format(FormatError::WrapperUnsupported {
                wrapper: wrapper.as_u8(),
            }))?;
        // SAFETY: raw-pointer slice into the never-reallocated, cell-owned
        // buffer; the target span is UNPUBLISHED (bit b clear), no shared
        // reference reads unpublished bytes (readers gate on the bit,
        // Acquire), and all writes serialize on the cell lock.
        let dst = unsafe {
            core::slice::from_raw_parts_mut(
                (state.buf.as_mut_ptr() as *mut u8).add(f0),
                f1 - f0,
            )
        };
        if !u.unwrap_block(seg.bytes(), dst)? {
            // Binding cannot serve block grain: whole-fallback ON TOP of
            // the reservation — decompress everything via the whole path
            // and copy the payload into the reserved buffer (bits all set).
            let region = load_region(part, unwrappers, ps, what)?;
            let payload = region.payload();
            if payload.len() != state.len {
                return Err(ReadError::Format(FormatError::Corrupt { at: what }));
            }
            let base = state.buf.as_mut_ptr() as *mut u8;
            // SAFETY: same discipline as above; unpublished spans only —
            // published block bytes are IDENTICAL by determinism (spec
            // §11), so overwriting them with equal bytes is benign, but we
            // still only touch unpublished spans to keep the law simple:
            // copy the whole image before any bit beyond b is published.
            for blk in 0..nb {
                if self.block_resident(blk) {
                    continue;
                }
                let s0 = state.ftab[blk] as usize;
                let s1 = if blk + 1 < nb { state.ftab[blk + 1] as usize } else { state.len };
                unsafe {
                    core::ptr::copy_nonoverlapping(
                        payload.as_ptr().add(s0),
                        base.add(s0),
                        s1 - s0,
                    );
                }
                self.resident.fetch_add(s1 - s0, Ordering::Relaxed);
            }
            DICT_WHOLE_FAULT_BYTES
                .fetch_add(payload.len() as u64, core::sync::atomic::Ordering::Relaxed);
            let bits = self.bits.get().expect("published at init");
            for w in bits {
                w.store(u64::MAX, Ordering::Release);
            }
            self.blocks_resident.store(nb, Ordering::Release);
            return Ok(false);
        }
        self.resident.fetch_add(f1 - f0, Ordering::Relaxed);
        DICT_BLOCK_FAULT_BYTES
            .fetch_add((c1 - c0) as u64, core::sync::atomic::Ordering::Relaxed);
        DICT_BLOCK_FAULTS.fetch_add(1, core::sync::atomic::Ordering::Relaxed);
        let bits = self.bits.get().expect("published at init");
        bits[b / 64].fetch_or(1u64 << (b % 64), Ordering::Release);
        self.blocks_resident.fetch_add(1, Ordering::Release);
        Ok(true)
    }

    /// Bulk publication (the C7 prewarm / dense-consumer path): ONE
    /// CRC-validated whole-extent read, then decompress every unresident
    /// block — the orderly whole-region economics of today's arm, into the
    /// same block-grain publication.
    fn ensure_all(
        &self,
        part: &Arc<OpenPart>,
        unwrappers: &[&dyn crate::SectionUnwrapper],
        ps: &ParsedStream,
        what: &'static str,
    ) -> ReadResult<()> {
        let mut cell = match self.cell.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        if cell.is_none() {
            *cell = Some(self.init_state(part, unwrappers, ps, what)?);
        }
        let state = cell.as_mut().expect("just stored");
        if state.whole.is_some() {
            return Ok(());
        }
        let nb = state.ftab.len();
        if (0..nb).all(|b| self.block_resident(b)) {
            return Ok(());
        }
        // Whole raw extent: CRC-validated, in-flight-registered (plays the
        // C6 game with the claim-horizon issuers) but NOT retained in the
        // part cache — the bytes immediately decompress into the block
        // image below, and caching the raw extent alongside doubled the
        // dense first-touch memory traffic (RESULTS-DENSEDICT §3.1 banked
        // refinement 1: the q14 +361 ms class). A raw image some other
        // consumer already cached (or has in flight) is still served from
        // the cache — hit costs nothing extra. PGRUST_PGRC2_BLOCKLAZY_
        // REFINE=0 = the densedict-measured cached arm (part-old A/B).
        let raw = if blocklazy_refine() {
            part.extent_bytes_uncached(&ps.entry, &ps.extents[0], 0)?
        } else {
            part.extent_bytes(&ps.entry, &ps.extents[0], 0)?
        };
        let wrapper = pgrc2_format::enc::Wrapper::from_u8(ps.entry.wrapper)?;
        let u = unwrappers
            .iter()
            .find(|u| u.wrapper() == wrapper)
            .ok_or(ReadError::Format(FormatError::WrapperUnsupported {
                wrapper: wrapper.as_u8(),
            }))?;
        let base = state.buf.as_mut_ptr() as *mut u8;
        let bits = self.bits.get().expect("published at init");
        let mut block_grain = true;
        for b in 0..nb {
            if self.block_resident(b) {
                continue;
            }
            let (c0, c1) = (state.btab[b] as usize, state.btab[b + 1] as usize);
            let f0 = state.ftab[b] as usize;
            let f1 = if b + 1 < nb { state.ftab[b + 1] as usize } else { state.len };
            let src = &raw.bytes()[STREAM_SECTION_HDR_LEN + c0..STREAM_SECTION_HDR_LEN + c1];
            // SAFETY: unpublished span under the cell lock (same law as
            // fault_block).
            let dst = unsafe { core::slice::from_raw_parts_mut(base.add(f0), f1 - f0) };
            if !u.unwrap_block(src, dst)? {
                block_grain = false;
                break;
            }
            self.resident.fetch_add(f1 - f0, Ordering::Relaxed);
            bits[b / 64].fetch_or(1u64 << (b % 64), Ordering::Release);
            self.blocks_resident.fetch_add(1, Ordering::Release);
        }
        if !block_grain {
            // Binding without block decode: whole unwrap + copy (bits set).
            let region = load_region(part, unwrappers, ps, what)?;
            let payload = region.payload();
            if payload.len() != state.len {
                return Err(ReadError::Format(FormatError::Corrupt { at: what }));
            }
            for b in 0..nb {
                if self.block_resident(b) {
                    continue;
                }
                let f0 = state.ftab[b] as usize;
                let f1 = if b + 1 < nb { state.ftab[b + 1] as usize } else { state.len };
                unsafe {
                    core::ptr::copy_nonoverlapping(
                        payload.as_ptr().add(f0),
                        base.add(f0),
                        f1 - f0,
                    );
                }
                self.resident.fetch_add(f1 - f0, Ordering::Relaxed);
                bits[b / 64].fetch_or(1u64 << (b % 64), Ordering::Release);
                self.blocks_resident.fetch_add(1, Ordering::Release);
            }
        }
        DICT_WHOLE_FAULT_BYTES
            .fetch_add(state.len as u64, core::sync::atomic::Ordering::Relaxed);
        Ok(())
    }
}

/// The payload region arm elected at open: frame-lazy on the SB-7
/// unwrapped multi-extent geometry, whole-fault otherwise (single-extent
/// legacy/small dicts, wrapped payloads — O-CMP-5(a) unwrap is
/// whole-section by design — and the kill-switch arm).
enum PayloadRegion {
    Whole(LazyRegion),
    FrameLazy(FrameLazyPayload),
    FrameDirect(FrameDirectPayload),
    /// Arc'd so the image can be PART-grain shared (the §4.2 re-pose);
    /// the handle-grain A/B arm holds a private Arc.
    BlockLazy(Arc<BlockLazyPayload>),
}

/// The reader-level lazy dict handle. `&self` faces throughout (dict lanes
/// are consumed concurrently under claim scope); internal publication is
/// write-once.
pub struct DictHandle {
    part: Arc<OpenPart>,
    /// Registry-mediated opens keep the part pinned for the handle's life
    /// (the lane that hands out codes must keep its payload region
    /// resident).
    _pin: Option<PartPin>,
    /// Section unwrappers for wrapped dict streams (CMP-B: the byte-run
    /// stream class takes the wrapper offer). `'static` like every
    /// production binding; adjudicated at open (refusal-before-fault).
    unwrappers: &'static [&'static dyn crate::SectionUnwrapper],
    attno: u32,
    path_ord: u32,
    entry_count: u32,
    exec_publishable: bool,
    column_all_valid: bool,
    /// The DictIndex `char_field` form (spec §6.3 flags; M5d char-len
    /// record): Absolute / Delta (reconstructed, typed corruption on
    /// underflow) / Absent (Option-C: recomputed — see `char_table`).
    /// Self-described per stream — never a process posture.
    charlen_form: pgrc2_format::dict::DictCharLenForm,
    /// Option-C (`STREAMF_CHARLEN_ABSENT`): the load-time one-pass
    /// per-code char-length table, built on the FIRST `lengths()` call
    /// (one UTF-8 lead-count walk over every entry — the index-only/
    /// no-payload-fault contract is deviated under this form BY DESIGN;
    /// the M5d length-lanes consumer shares this pass). Empty vec until
    /// built; write-once.
    char_table: pgsync::OnceLock<Vec<u32>>,
    epoch64: u64,
    index_stream: ParsedStream,
    payload_stream: ParsedStream,
    index: LazyRegion,
    payload: PayloadRegion,
    /// [sqe8blk] Dense/sparse election is LIVE on this handle — true only
    /// on the PART-grain block-lazy arm (`=part`). The `=handle` A/B arm
    /// and every non-block arm keep their measured behavior unchanged.
    elect: bool,
    /// [sqe8blk] Declared touch class (TOUCH_*; write-once, first wins).
    touch: AtomicU8,
    /// [sqe8blk] The all-resident short-circuit: once true, `ensure_frame`
    /// on the block-lazy arm is one Acquire load (set with the shared
    /// image's full publication proven — see `fully_resident`).
    payload_all_resident: AtomicBool,
    /// [sqe8blk] Block faults THIS handle's demands caused (the
    /// bytes-touched election input; shared-image hits don't count).
    demand_block_faults: AtomicU32,
}

impl DictHandle {
    /// Open the handle for (attno, path_ord). Faults at most the StreamDir
    /// section (directory lookup); dict sections stay unfaulted until
    /// [`DictHandle::ensure_frame`] / [`DictHandle::entry`] /
    /// [`DictHandle::lengths`] ask for them — the lazy contract the fault
    /// tests pin.
    pub fn open(
        part: Arc<OpenPart>,
        pin: Option<PartPin>,
        unwrappers: &'static [&'static dyn crate::SectionUnwrapper],
        attno: u32,
        path_ord: u32,
    ) -> ReadResult<DictHandle> {
        DictHandle::open_with_mode(part, pin, unwrappers, attno, path_ord, DictFaultMode::from_env())
    }

    /// [`DictHandle::open`] with the SB-7 fault mode stated explicitly —
    /// the probe face (unit tests and the harness born-RED arm select
    /// `WholeFault` without touching process env).
    pub fn open_with_mode(
        part: Arc<OpenPart>,
        pin: Option<PartPin>,
        unwrappers: &'static [&'static dyn crate::SectionUnwrapper],
        attno: u32,
        path_ord: u32,
        mode: DictFaultMode,
    ) -> ReadResult<DictHandle> {
        DictHandle::open_with_arms(part, pin, unwrappers, attno, path_ord, mode, DictFrameServe::from_env())
    }

    /// [`DictHandle::open_with_mode`] with the frame-serve arm ALSO stated
    /// explicitly (env-independent — the arm-specific pins in
    /// `tests/dict.rs` select `Copy`/`Direct` without touching process env).
    pub fn open_with_arms(
        part: Arc<OpenPart>,
        pin: Option<PartPin>,
        unwrappers: &'static [&'static dyn crate::SectionUnwrapper],
        attno: u32,
        path_ord: u32,
        mode: DictFaultMode,
        serve: DictFrameServe,
    ) -> ReadResult<DictHandle> {
        let dir = part.stream_directory()?;
        let index_stream = dir
            .lookup(attno, path_ord, StreamRole::DictIndex)
            .ok_or(ReadError::StreamMissing {
                attno,
                path_ord,
                role: StreamRole::DictIndex.as_u8(),
            })?
            .clone();
        let payload_stream = dir
            .lookup(attno, path_ord, StreamRole::DictPayload)
            .ok_or(ReadError::StreamMissing {
                attno,
                path_ord,
                role: StreamRole::DictPayload.as_u8(),
            })?
            .clone();
        // Refusal-before-fault (CMP-B, the cursor-open pin mirrored): a
        // wrapped dict stream whose arm this binding lacks refuses TYPED at
        // open — the lazy contract (StreamDir only) is preserved, and no
        // payload fault can precede the refusal.
        for ps in [&index_stream, &payload_stream] {
            let w = pgrc2_format::enc::Wrapper::from_u8(ps.entry.wrapper)?;
            if w != pgrc2_format::enc::Wrapper::None
                && !unwrappers.iter().any(|u| u.wrapper() == w)
            {
                return Err(ReadError::Format(FormatError::WrapperUnsupported {
                    wrapper: w.as_u8(),
                }));
            }
        }
        let entry_count = u32::try_from(index_stream.entry.values).map_err(|_| {
            ReadError::Format(FormatError::Corrupt {
                at: "dict entry count",
            })
        })?;
        // Publishability facts (spec §7): the lattice flag lives on the
        // values stream; the O-6 zero-null proof is "no Validity stream".
        let values = dir.lookup(attno, path_ord, StreamRole::Values);
        let exec_publishable = values
            .map(|v| v.entry.flags & STREAMF_DICT_EXEC != 0)
            .unwrap_or(false);
        let column_all_valid = dir.lookup(attno, path_ord, StreamRole::Validity).is_none();
        let epoch64 = part.dict_epoch(attno, path_ord);
        // SB-7 arm election (M3-L3): frame-lazy on the v4 writer's
        // unwrapped multi-extent frame geometry; whole-fault for
        // single-extent payloads (legacy grain / sub-frame dicts), wrapped
        // payloads (O-CMP-5(a): unwrap is whole-section by design), and
        // the PGRUST_PGRC2_FRAME_LAZY=0 kill-switch arm (the born-RED
        // reproduction path the residency probe consumes).
        let payload_wrapper = pgrc2_format::enc::Wrapper::from_u8(payload_stream.entry.wrapper)?;
        let payload = if payload_stream.extents.len() > 1
            && payload_wrapper == Wrapper::None
            && mode == DictFaultMode::FrameLazy
        {
            if serve == DictFrameServe::Direct {
                PayloadRegion::FrameDirect(FrameDirectPayload::new(payload_stream.extents.len()))
            } else {
                PayloadRegion::FrameLazy(FrameLazyPayload::new(payload_stream.extents.len()))
            }
        } else if payload_stream.extents.len() == 1
            && payload_wrapper != Wrapper::None
            && mode == DictFaultMode::FrameLazy
            && block_lazy() != BlockLazyGrain::Off
        {
            // [stack] Wrapped single-extent geometry (the banks of record):
            // block-lazy partial unwrap (RESULTS-FMTLAND §B.3). Invalid
            // block geometry falls back to whole assembly at first fault.
            // [densedict] Part grain shares the image through the part
            // cache (reopens re-serve, never re-decompress); handle grain
            // is the born-negative A/B arm.
            match block_lazy() {
                BlockLazyGrain::Part => {
                    let rec = &payload_stream.extents[0];
                    PayloadRegion::BlockLazy(
                        part.dict_block_payload((rec.file_off, rec.len)),
                    )
                }
                _ => PayloadRegion::BlockLazy(Arc::new(BlockLazyPayload::new())),
            }
        } else {
            PayloadRegion::Whole(LazyRegion::new())
        };
        // [sqe8blk] Election is live only on the part-grain arm; a reopen
        // that finds the shared image fully published starts on the
        // all-resident fast path (the §3.1 hot-probe refinement).
        let (elect, all_resident) = match &payload {
            PayloadRegion::BlockLazy(bl)
                if block_lazy() == BlockLazyGrain::Part && blocklazy_refine() =>
            {
                (true, bl.fully_resident())
            }
            _ => (false, false),
        };
        let charlen_form =
            pgrc2_format::dict::DictCharLenForm::from_flags(index_stream.entry.flags)?;
        Ok(DictHandle {
            part,
            _pin: pin,
            unwrappers,
            attno,
            path_ord,
            entry_count,
            exec_publishable,
            column_all_valid,
            charlen_form,
            char_table: pgsync::OnceLock::new(),
            epoch64,
            index_stream,
            payload_stream,
            index: LazyRegion::new(),
            payload,
            elect,
            touch: AtomicU8::new(TOUCH_UNDECLARED),
            payload_all_resident: AtomicBool::new(all_resident),
            demand_block_faults: AtomicU32::new(0),
        })
    }

    /// [sqe8blk] Declare this handle's touch class (RESULTS-DENSEDICT
    /// §3.1 refinement 2 — see [`DictTouch`] for the election inputs).
    /// First declaration wins; undeclared handles use the bytes-touched
    /// fallback. A no-op on every arm but part-grain block-lazy.
    pub fn declare_touch(&self, t: DictTouch) {
        let v = match t {
            DictTouch::Dense => TOUCH_DENSE,
            DictTouch::Sparse => TOUCH_SPARSE,
        };
        let _ = self.touch.compare_exchange(
            TOUCH_UNDECLARED,
            v,
            Ordering::Relaxed,
            Ordering::Relaxed,
        );
    }

    /// Number of global codes (dict entries).
    pub fn ncodes(&self) -> u32 {
        self.entry_count
    }

    /// The u64 epoch currency (minted per open-part instance — crate doc
    /// "Dict epochs"; equality certifies code-space identity).
    pub fn epoch64(&self) -> u64 {
        self.epoch64
    }

    /// The structural epoch identity (spec §7, Law A).
    pub fn epoch_key(&self) -> DictEpochKey {
        DictEpochKey {
            part_uuid: self.part.uuid(),
            attno: self.attno,
            path_ord: self.path_ord,
        }
    }

    /// `STREAMF_DICT_EXEC`: code-eq == value-eq holds; execution lanes may
    /// consume codes (spec §7 publishability lattice).
    pub fn exec_publishable(&self) -> bool {
        self.exec_publishable
    }

    /// The O-6 zero-null proof input: the column stores no Validity stream
    /// in this part (spec §6.1 — absent = all-valid).
    pub fn column_all_valid(&self) -> bool {
        self.column_all_valid
    }

    /// Byte-rank sorted order is contractual for every stored dictionary
    /// (spec §7): global code order == byte order of the stored images.
    pub fn byte_rank_sorted(&self) -> bool {
        true
    }

    /// The dict frame a code lives in (spec §7 lazy-fault grain).
    pub fn frame_of_code(&self, code: u32) -> u32 {
        code / DICT_FRAME_ENTRIES
    }

    /// Ensure the sections backing dict frame `frame` are resident. The
    /// contractual grain is the dict frame, and since M3-L3 the REALIZED
    /// grain matches it on SB-7 geometry: the v4 writer cuts the UNWRAPPED
    /// DictPayload extent table at frame boundaries (per-frame CRCs), and
    /// the frame-lazy region reservation faults EXACTLY extent `frame`
    /// (plus extent 0, the header, once). Single-extent and wrapped
    /// payloads — and the `PGRUST_PGRC2_FRAME_LAZY=0` born-RED arm — keep
    /// the v1 whole-region assembly. The index stream stays whole-resident
    /// by design (12-B stride, the small always-resident half).
    pub fn ensure_frame(&self, frame: u32) -> ReadResult<()> {
        self.index
            .ensure(&self.part, self.unwrappers, &self.index_stream, "dict index stream")?;
        match &self.payload {
            PayloadRegion::Whole(r) => {
                r.ensure(&self.part, self.unwrappers, &self.payload_stream, "dict payload stream")?;
            }
            PayloadRegion::FrameLazy(fl) => {
                // On frame-cut geometry the frame ordinal IS the extent
                // ordinal (seal.rs dict_frame_extents: one extent per
                // frame, `granule_start` carries the frame ordinal).
                fl.ensure_extent(&self.part, &self.payload_stream, frame, "dict payload stream")?;
            }
            PayloadRegion::FrameDirect(fd) => {
                fd.ensure_extent(&self.part, &self.payload_stream, frame, "dict payload stream")?;
            }
            PayloadRegion::BlockLazy(bl) => {
                // [sqe8blk] Part-grain dense/sparse election (see
                // [`DictTouch`] for the inputs). Handle-grain (`=1`) keeps
                // the measured A/B behavior: always block grain.
                if !self.elect {
                    bl.ensure_frame_block(
                        &self.part,
                        self.unwrappers,
                        &self.payload_stream,
                        frame,
                        "dict payload stream",
                    )?;
                    return Ok(());
                }
                if self.payload_all_resident.load(Ordering::Acquire) {
                    return Ok(());
                }
                let touch = self.touch.load(Ordering::Relaxed);
                let dense = touch == TOUCH_DENSE
                    || (touch == TOUCH_UNDECLARED && {
                        // Bytes-touched fallback: this handle's own demand
                        // faults vs the §6.4 block count (nblocks == 0
                        // before the first fault ⇒ stays block-grain).
                        let nb = bl.nblocks.load(Ordering::Acquire);
                        nb > 0 && self.demand_block_faults.load(Ordering::Relaxed) as usize * 4 > nb
                    });
                if dense {
                    bl.ensure_all(
                        &self.part,
                        self.unwrappers,
                        &self.payload_stream,
                        "dict payload stream",
                    )?;
                    self.payload_all_resident.store(true, Ordering::Release);
                    return Ok(());
                }
                let faulted = bl.ensure_frame_block(
                    &self.part,
                    self.unwrappers,
                    &self.payload_stream,
                    frame,
                    "dict payload stream",
                )?;
                if faulted {
                    self.demand_block_faults.fetch_add(1, Ordering::Relaxed);
                }
                if bl.fully_resident() {
                    self.payload_all_resident.store(true, Ordering::Release);
                }
            }
        }
        Ok(())
    }

    /// SB-7 residency witnesses (the census terms the M3 residency probe
    /// and the memwatchdog line consume): faulted payload bytes on this
    /// handle. Whole-fault arms report the full region once assembled.
    pub fn resident_payload_bytes(&self) -> u64 {
        match &self.payload {
            PayloadRegion::Whole(r) => r.get().map(|(_, len)| len as u64).unwrap_or(0),
            PayloadRegion::FrameLazy(fl) => fl.resident.load(Ordering::Relaxed) as u64,
            PayloadRegion::FrameDirect(fd) => fd.resident.load(Ordering::Relaxed) as u64,
            PayloadRegion::BlockLazy(bl) => bl.resident.load(Ordering::Relaxed) as u64,
        }
    }

    /// Payload frames this handle serves at frame grain (0 = whole-fault
    /// arm; the witness the probe uses to prove the arm is live).
    pub fn frame_lazy_frames(&self) -> u32 {
        match &self.payload {
            PayloadRegion::Whole(_) => 0,
            PayloadRegion::FrameLazy(fl) => fl.frames,
            PayloadRegion::FrameDirect(fd) => fd.frames,
            // Block-lazy: block count once known (0 before the first fault).
            PayloadRegion::BlockLazy(bl) => bl.nblocks.load(Ordering::Acquire) as u32,
        }
    }

    /// Ensure the sections backing `code` are resident.
    pub fn ensure_code(&self, code: u32) -> ReadResult<()> {
        if code >= self.entry_count {
            return Err(ReadError::Format(FormatError::Bounds { at: "dict code" }));
        }
        self.ensure_frame(self.frame_of_code(code))
    }

    /// [stack] Orderly WHOLE-payload publication — the C7 prewarm / dense-
    /// consumer face. On the block-lazy arm this is ONE CRC-validated
    /// whole-extent read + decompress-all (today's whole-region economics);
    /// on every other arm it is `ensure_code(0)` (whole-fault assembles the
    /// region; frame arms keep their lazy grain — the prefetcher owns their
    /// bulk story). Bulk byte-folding consumers call THIS, never a fake
    /// first-entry touch.
    pub fn prewarm_payload(&self) -> ReadResult<()> {
        if self.entry_count == 0 {
            return Ok(());
        }
        match &self.payload {
            PayloadRegion::BlockLazy(bl) => {
                self.index.ensure(
                    &self.part,
                    self.unwrappers,
                    &self.index_stream,
                    "dict index stream",
                )?;
                bl.ensure_all(
                    &self.part,
                    self.unwrappers,
                    &self.payload_stream,
                    "dict payload stream",
                )?;
                // [sqe8blk] prewarm IS the dense election: everything is
                // published now — arm the all-resident short-circuit.
                self.payload_all_resident.store(true, Ordering::Release);
                Ok(())
            }
            _ => self.ensure_code(0),
        }
    }

    /// Stored byte + char lengths of `code` — INDEX-ONLY (no payload fault):
    /// the stored-lengths law makes `length()` a table lookup (spec §7).
    pub fn lengths(&self, code: u32) -> ReadResult<(u32, u32)> {
        if code >= self.entry_count {
            return Err(ReadError::Format(FormatError::Bounds { at: "dict code" }));
        }
        let (iptr, ilen) = self
            .index
            .ensure(&self.part, self.unwrappers, &self.index_stream, "dict index stream")?;
        // SAFETY: (iptr, ilen) were published from a payload slice of an
        // `Arc<[u8]>` rooted in `self.index.cell` for `self`'s lifetime;
        // the cell is write-once, so the region is live and immutable.
        let index = unsafe { core::slice::from_raw_parts(iptr, ilen) };
        let off = code as usize * DICT_INDEX_ENTRY_LEN;
        let rec = index
            .get(off..off + DICT_INDEX_ENTRY_LEN)
            .ok_or(ReadError::Format(FormatError::Bounds { at: "dict index" }))?;
        let byte_len = u32::from_le_bytes(rec[4..8].try_into().expect("len 4"));
        let char_field = u32::from_le_bytes(rec[8..12].try_into().expect("len 4"));
        // M5d char-len record: absolute char_len under the stream's
        // declared form (typed corruption on a delta > byte_len — UTF-8
        // chars are never longer than their bytes).
        use pgrc2_format::dict::DictCharLenForm;
        let char_len = match self.charlen_form {
            DictCharLenForm::Absolute => char_field,
            DictCharLenForm::Delta => byte_len
                .checked_sub(char_field)
                .ok_or(ReadError::Format(FormatError::Corrupt {
                    at: "dict entry char_len delta",
                }))?,
            // Option-C: nothing stored — serve from the load-time
            // one-pass table (built on first call; the index-only/
            // no-payload-fault contract is DEVIATED under this form by
            // design — the build faults the payload once, then every
            // call is a table read).
            DictCharLenForm::Absent => {
                if self.char_table.get().is_none() {
                    let t = self.build_char_table()?;
                    let _ = self.char_table.set(t);
                }
                let t = self.char_table.get().expect("just set");
                *t.get(code as usize).ok_or(ReadError::Format(
                    FormatError::Bounds { at: "dict char table" },
                ))?
            }
        };
        Ok((byte_len, char_len))
    }

    /// Stored byte length of `code` — INDEX-ONLY under EVERY char-len
    /// form (`byte_len` lives in the index's second field regardless of
    /// what the third carries), so byte-only consumers (the per-row
    /// StrView flip's cell sizing — the hot caller) never touch the
    /// Option-C char table. The §9 guardrail pair caught the coupling:
    /// routing byte_len through `lengths()` made every dict-text query
    /// pay the load-time char walk it never asked for.
    pub fn byte_len_only(&self, code: u32) -> ReadResult<u32> {
        if code >= self.entry_count {
            return Err(ReadError::Format(FormatError::Bounds { at: "dict code" }));
        }
        let (iptr, ilen) = self
            .index
            .ensure(&self.part, self.unwrappers, &self.index_stream, "dict index stream")?;
        // SAFETY: write-once published region, live for `self`'s lifetime.
        let index = unsafe { core::slice::from_raw_parts(iptr, ilen) };
        let off = code as usize * DICT_INDEX_ENTRY_LEN;
        let rec = index
            .get(off..off + DICT_INDEX_ENTRY_LEN)
            .ok_or(ReadError::Format(FormatError::Bounds { at: "dict index" }))?;
        Ok(u32::from_le_bytes(rec[4..8].try_into().expect("len 4")))
    }

    /// Option-C (`STREAMF_CHARLEN_ABSENT`): the load-time one-pass —
    /// resolve every entry once and count UTF-8 lead bytes (the
    /// char-length fact's one definition). The M5d length-lanes consumer
    /// builds its per-code table from this same pass.
    fn build_char_table(&self) -> ReadResult<Vec<u32>> {
        let mut t = Vec::with_capacity(self.entry_count as usize);
        let mut cur = self.entries(0, self.entry_count)?;
        while let Some((_, e)) = cur.next_entry()? {
            // Under the Absent form the cursor's `char_len` IS the
            // `utf8_char_count` walk (the fact's one definition).
            t.push(e.char_len);
        }
        Ok(t)
    }

    /// Resolve entry `code`: the varlena-shaped image (a valid PG datum by
    /// the §7b law), its payload bytes, and the stored lengths. The returned
    /// references live as long as the handle and their addresses NEVER
    /// change (generation stability — module doc).
    pub fn entry(&self, code: u32) -> ReadResult<DictEntryRef<'_>> {
        self.ensure_code(code)?;
        let (iptr, ilen) = self
            .index
            .ensure(&self.part, self.unwrappers, &self.index_stream, "dict index stream")?;
        // [cold2] frame-direct: resolve out of the frame's own SegBuf —
        // no whole-payload slice exists on this arm.
        if let PayloadRegion::FrameDirect(fd) = &self.payload {
            // SAFETY: write-once published index region, live for `self`'s
            // lifetime (same justification as the other arms below).
            let index = unsafe { core::slice::from_raw_parts(iptr, ilen) };
            let off = code as usize * DICT_INDEX_ENTRY_LEN;
            let rec = index
                .get(off..off + DICT_INDEX_ENTRY_LEN)
                .ok_or(ReadError::Format(FormatError::Bounds { at: "dict index" }))?;
            let payload_off = u32::from_le_bytes(rec[0..4].try_into().expect("len 4"));
            let byte_len = u32::from_le_bytes(rec[4..8].try_into().expect("len 4"));
            let char_field = u32::from_le_bytes(rec[8..12].try_into().expect("len 4"));
            let (image, bytes) = fd.entry_slices(code, payload_off as usize)?;
            if bytes.len() != byte_len as usize {
                return Err(ReadError::Format(FormatError::Corrupt {
                    at: "dict entry byte_len",
                }));
            }
            use pgrc2_format::dict::DictCharLenForm;
            let char_len = match self.charlen_form {
                DictCharLenForm::Absolute => char_field,
                DictCharLenForm::Delta => byte_len.checked_sub(char_field).ok_or(
                    ReadError::Format(FormatError::Corrupt {
                        at: "dict entry char_len delta",
                    }),
                )?,
                DictCharLenForm::Absent => pgrc2_format::dict::utf8_char_count(bytes),
            };
            return Ok(DictEntryRef { image, bytes, byte_len, char_len });
        }
        let (pptr, plen) = match &self.payload {
            PayloadRegion::Whole(r) => r.ensure(
                &self.part,
                self.unwrappers,
                &self.payload_stream,
                "dict payload stream",
            )?,
            PayloadRegion::FrameDirect(_) => unreachable!("handled above"),
            PayloadRegion::BlockLazy(bl) => {
                // SAFETY: published write-once payload window (reserved
                // buffer or the whole-fallback region), live and immovable
                // for `self`'s lifetime; only bytes of PUBLISHED blocks are
                // dereferenced (`ensure_code` above published `code`'s
                // block; `dict_entry` reads only that entry's bytes).
                bl.get().ok_or(ReadError::Format(FormatError::Corrupt {
                    at: "dict payload region unpublished after ensure",
                }))?
            }
            PayloadRegion::FrameLazy(fl) => {
                let (base, _len, pstart, pend) =
                    fl.get().ok_or(ReadError::Format(FormatError::Corrupt {
                        at: "dict payload region unpublished after ensure",
                    }))?;
                // SAFETY: `base` is the published, never-moving region
                // reservation; the payload window is a fixed sub-range.
                // Only bytes of PUBLISHED frames are dereferenced through
                // this slice (`ensure_code` above published `code`'s
                // frame; `dict_entry` reads only that entry's bytes).
                (unsafe { base.add(pstart) }, pend - pstart)
            }
        };
        // SAFETY: both regions were published from payload slices of
        // `Arc<[u8]>` buffers rooted in this handle's write-once cells; they
        // are live for `self`'s lifetime and never move, so tying the
        // returned slices to `&self` is sound.
        let sections = DictSections {
            index: unsafe { core::slice::from_raw_parts(iptr, ilen) },
            payload: unsafe { core::slice::from_raw_parts(pptr, plen) },
            entry_count: self.entry_count,
            charlen_form: self.charlen_form,
        };
        dict_entry(&sections, code).map_err(Into::into)
    }

    /// The owning part.
    pub fn part(&self) -> &Arc<OpenPart> {
        &self.part
    }

    /// [sqe P5-0 lever 4] Bulk sequential entry access: a cursor over codes
    /// `[start, end)` that amortizes the per-entry `entry()` overhead
    /// (bounds + ensure + arm dispatch + region republication, ~44ns/entry
    /// measured on the q16/q17 fp build) down to one frame-grain refill per
    /// `DICT_FRAME_ENTRIES` codes. Yields the SAME `DictEntryRef`s as
    /// [`DictHandle::entry`] — byte-for-byte identical output is pinned by
    /// `tests/dict.rs` — with the same generation-stability law: the
    /// returned slices borrow the handle, never the cursor, and no
    /// per-entry allocation occurs.
    pub fn entries(&self, start: u32, end: u32) -> ReadResult<DictEntryCursor<'_>> {
        if start > end || end > self.entry_count {
            return Err(ReadError::Format(FormatError::Bounds { at: "dict code" }));
        }
        let index: &[u8] = if start == end {
            // Empty range: fault nothing (the lazy contract holds).
            &[]
        } else {
            let (iptr, ilen) = self.index.ensure(
                &self.part,
                self.unwrappers,
                &self.index_stream,
                "dict index stream",
            )?;
            // SAFETY: (iptr, ilen) were published from a payload slice of an
            // `Arc<[u8]>` rooted in `self.index.cell` for `self`'s lifetime;
            // the cell is write-once, so the region is live and immutable.
            unsafe { core::slice::from_raw_parts(iptr, ilen) }
        };
        Ok(DictEntryCursor {
            dh: self,
            index,
            win: BulkWin::Contig(&[]),
            code: start,
            end,
            // Forces the first `next_entry` to ensure + bind frame(start).
            frame_end: start,
        })
    }
}

/// The bulk cursor's per-frame payload binding — resolved ONCE per frame
/// at refill instead of once per entry.
#[derive(Clone, Copy)]
enum BulkWin<'a> {
    /// Whole-payload window (Whole / FrameLazy / BlockLazy arms): entry
    /// `payload_off`s index directly into this slice. Only bytes of
    /// ENSURED frames are dereferenced through it (the refill ensured the
    /// current frame; entries of frame f live wholly inside extent f).
    Contig(&'a [u8]),
    /// [cold2] frame-direct: the current frame's SegBuf; `payload_off` maps
    /// via `payload_start + payload_off - base` (the `entry_slices` law).
    Direct {
        seg: &'a [u8],
        base: usize,
        payload_start: usize,
        payload_end: usize,
    },
}

/// Sequential bulk entry cursor over a [`DictHandle`] — see
/// [`DictHandle::entries`]. Entry slices borrow the HANDLE (`'a`), so they
/// remain valid after the cursor advances or drops (generation stability).
pub struct DictEntryCursor<'a> {
    dh: &'a DictHandle,
    index: &'a [u8],
    win: BulkWin<'a>,
    code: u32,
    end: u32,
    /// First code NOT served by the currently-bound frame window.
    frame_end: u32,
}

impl<'a> DictEntryCursor<'a> {
    /// Bind the frame containing `self.code`: one `ensure_frame` + one arm
    /// dispatch per `DICT_FRAME_ENTRIES` codes.
    #[cold]
    fn refill(&mut self) -> ReadResult<()> {
        let dh = self.dh;
        let frame = dh.frame_of_code(self.code);
        dh.ensure_frame(frame)?;
        self.win = match &dh.payload {
            PayloadRegion::Whole(r) => {
                let (p, l) = r.get().ok_or(ReadError::Format(FormatError::Corrupt {
                    at: "dict payload region unpublished after ensure",
                }))?;
                // SAFETY: published from a payload slice of an `Arc<[u8]>`
                // rooted in the handle's write-once cell; live and immutable
                // for `'a` (the same justification as `entry()`).
                BulkWin::Contig(unsafe { core::slice::from_raw_parts(p, l) })
            }
            PayloadRegion::FrameLazy(fl) => {
                let (base, _len, pstart, pend) =
                    fl.get().ok_or(ReadError::Format(FormatError::Corrupt {
                        at: "dict payload region unpublished after ensure",
                    }))?;
                // SAFETY: `base` is the published, never-moving region
                // reservation; the payload window is a fixed sub-range.
                // Only bytes of PUBLISHED frames are dereferenced through
                // this slice (`ensure_frame` above published the frame this
                // window serves until the next refill).
                BulkWin::Contig(unsafe {
                    core::slice::from_raw_parts(base.add(pstart), pend - pstart)
                })
            }
            PayloadRegion::BlockLazy(bl) => {
                let (p, l) = bl.get().ok_or(ReadError::Format(FormatError::Corrupt {
                    at: "dict payload region unpublished after ensure",
                }))?;
                // SAFETY: published write-once payload window (reserved
                // buffer or the whole-fallback region), live and immovable
                // for `'a`; only bytes of PUBLISHED blocks are dereferenced
                // (`ensure_frame` above published this frame's block).
                BulkWin::Contig(unsafe { core::slice::from_raw_parts(p, l) })
            }
            PayloadRegion::FrameDirect(fd) => {
                let geom = fd
                    .geom
                    .get()
                    .ok_or(ReadError::Format(FormatError::Corrupt {
                        at: "dict payload geometry unpublished after ensure",
                    }))?;
                let seg = fd.segs[frame as usize].get().ok_or(ReadError::Format(
                    FormatError::Corrupt {
                        at: "dict frame unresident after ensure",
                    },
                ))?;
                BulkWin::Direct {
                    seg: seg.bytes(),
                    base: geom.offsets[frame as usize],
                    payload_start: geom.payload_start,
                    payload_end: geom.payload_end,
                }
            }
        };
        self.frame_end = (((frame as u64) + 1) * DICT_FRAME_ENTRIES as u64)
            .min(self.end as u64) as u32;
        Ok(())
    }

    /// The next `(code, entry)` in the range, or `Ok(None)` at the end.
    /// Identical validation + presentation to [`DictHandle::entry`].
    #[inline]
    pub fn next_entry(&mut self) -> ReadResult<Option<(u32, DictEntryRef<'a>)>> {
        if self.code >= self.frame_end {
            if self.code >= self.end {
                return Ok(None);
            }
            self.refill()?;
        }
        let code = self.code;
        let off = code as usize * DICT_INDEX_ENTRY_LEN;
        let rec = self
            .index
            .get(off..off + DICT_INDEX_ENTRY_LEN)
            .ok_or(ReadError::Format(FormatError::Bounds { at: "dict index" }))?;
        let payload_off = u32::from_le_bytes(rec[0..4].try_into().expect("len 4"));
        let byte_len = u32::from_le_bytes(rec[4..8].try_into().expect("len 4"));
        let char_field = u32::from_le_bytes(rec[8..12].try_into().expect("len 4"));
        let (image, bytes) = match self.win {
            BulkWin::Contig(p) => {
                pgrc2_format::wire::varlena_entry_at(p, payload_off as usize, "dict payload")?
            }
            BulkWin::Direct {
                seg,
                base,
                payload_start,
                payload_end,
            } => {
                let region_off = payload_start + payload_off as usize;
                if region_off >= payload_end {
                    return Err(ReadError::Format(FormatError::Bounds {
                        at: "dict payload",
                    }));
                }
                let lo = region_off
                    .checked_sub(base)
                    .ok_or(ReadError::Format(FormatError::Bounds { at: "dict payload" }))?;
                pgrc2_format::wire::varlena_entry_at(seg, lo, "dict payload")?
            }
        };
        if bytes.len() != byte_len as usize {
            return Err(ReadError::Format(FormatError::Corrupt {
                at: "dict entry byte_len",
            }));
        }
        use pgrc2_format::dict::DictCharLenForm;
        let char_len = match self.dh.charlen_form {
            DictCharLenForm::Absolute => char_field,
            DictCharLenForm::Delta => {
                byte_len
                    .checked_sub(char_field)
                    .ok_or(ReadError::Format(FormatError::Corrupt {
                        at: "dict entry char_len delta",
                    }))?
            }
            DictCharLenForm::Absent => pgrc2_format::dict::utf8_char_count(bytes),
        };
        self.code = code + 1;
        Ok(Some((
            code,
            DictEntryRef {
                image,
                bytes,
                byte_len,
                char_len,
            },
        )))
    }
}
