//! Part open + CRC-validated section faulting (spec §5–§6.2, §11).
//!
//! The open path faults exactly four fixed regions — tail, footer, section
//! table, header — then everything else is lazy at section grain through the
//! per-part segment cache. Every fault is recorded in the part's fault log:
//! the O(streams-touched) born-RED gate reads that log as its witness
//! (spec §6.2; the 2.9 ms/MiB cold-open law is the reason).
//!
//! A part is READABLE iff its tail magic + footer CRC validate AND it is
//! listed in an effective manifest generation (spec §5). This module owns
//! the first half; the manifest gate composes in [`crate::registry`] +
//! [`crate::manifest_walk`] via [`PartExpect`] validation facts.

use std::collections::BTreeMap;
use std::sync::Arc;

use pgrc2_format::ident::{part_uuid, PartIdent};
use pgrc2_format::manifest::PartRecord;
use pgrc2_format::part::{
    ExtentRecord, FooterFixed, PartHeader, SectionEntry, SectionKind, StreamEntry,
    FOOTER_FIXED_LEN, PART_HEADER_LEN, PART_TAIL_LEN, SECTION_ENTRY_LEN, TAIL_MAGIC,
};
use pgrc2_format::wire::{crc32c, Cur};
use pgrc2_format::FormatError;
use pgsync::atomic::{AtomicU32, AtomicU64, Ordering};
use pgsync::Mutex;

use crate::io::PartIo;
use crate::streams::StreamDirectory;
use crate::{ReadError, ReadResult};

pgsync::process_global! {
    /// Process-global dict-epoch clock (crate doc "Dict epochs"): u64 epochs
    /// are minted once per (open-part instance, dict stream), so equality
    /// certifies code-space identity structurally. Starts at 1 (0 is never a
    /// minted epoch).
    static EPOCH_CLOCK: pgsync::atomic::AtomicU64 = pgsync::atomic::AtomicU64::new(1);
}

// ---------------------------------------------------------------------------
// stream-fault observer (claim-horizon lane)
// ---------------------------------------------------------------------------

/// One demand access to a stream extent through the cursor faces
/// ([`OpenPart::extent_bytes`] / [`OpenPart::unwrapped_extent_bytes`]),
/// resident or not. `part` is an opaque identity key (`&OpenPart as usize`)
/// — observers correlate it against the `Arc<OpenPart>`s they already hold.
#[derive(Debug, Clone, Copy)]
pub struct StreamFaultEvent {
    pub part: usize,
    pub attno: u32,
    pub path_ord: u32,
    pub role: u8,
    pub extent_idx: u32,
    pub granule_start: u32,
    pub granule_count: u32,
    pub file_off: u64,
    pub len: u64,
    pub resident: bool,
    pub wrapped: bool,
}

/// Observer contract: called on the FAULTING thread, inside the measured
/// window, for every stream-extent access (hit or miss) while enabled —
/// it must be cheap and MUST NOT call back into the faulting faces
/// synchronously (hand work to another thread). Observation is a witness
/// face: it never changes what a read returns.
pub type StreamFaultObserver = dyn Fn(&StreamFaultEvent) + Send + Sync;

pgsync::process_global! {
    static STREAM_FAULT_OBS_ENABLED: pgsync::atomic::AtomicU32 =
        pgsync::atomic::AtomicU32::new(0);
    static STREAM_FAULT_OBS: pgsync::OnceLock<Box<StreamFaultObserver>> =
        pgsync::OnceLock::new();
}

/// Install the process-wide stream-fault observer (once; later calls are
/// refused with `false`). Enable/disable with
/// [`set_stream_fault_observer_enabled`] — installed-but-disabled costs one
/// relaxed atomic load per access.
pub fn set_stream_fault_observer(cb: Box<StreamFaultObserver>) -> bool {
    STREAM_FAULT_OBS.set(cb).is_ok()
}

pub fn set_stream_fault_observer_enabled(on: bool) {
    STREAM_FAULT_OBS_ENABLED.store(on as u32, Ordering::Relaxed);
}

#[inline]
fn observe_stream_fault(ev: &StreamFaultEvent) {
    if STREAM_FAULT_OBS_ENABLED.load(Ordering::Relaxed) != 0 {
        if let Some(cb) = STREAM_FAULT_OBS.get() {
            cb(ev);
        }
    }
}

// ---------------------------------------------------------------------------
// SegBuf — 8-aligned immutable section bytes
// ---------------------------------------------------------------------------

/// One resident section image. Backed by an `Arc<[u64]>` so the byte base is
/// ALWAYS 8-aligned: sections start 8-aligned in the file (spec §1), so
/// section-relative entry alignment (varlena slots, dict payload entries)
/// becomes absolute pointer alignment — the ≥8-align law and the StrView §7b
/// zero-copy dependency. Clones share the buffer; bytes never move.
#[derive(Clone)]
pub struct SegBuf {
    words: Arc<[u64]>,
    len: usize,
}

impl core::fmt::Debug for SegBuf {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("SegBuf").field("len", &self.len).finish()
    }
}

impl SegBuf {
    /// Allocate `len` zeroed bytes in an 8-aligned buffer and let `fill`
    /// populate them before the buffer is shared.
    fn build(
        len: usize,
        fill: impl FnOnce(&mut [u8]) -> ReadResult<()>,
    ) -> ReadResult<SegBuf> {
        // `len` is an on-file extent length (up to u32::MAX): admitted under
        // C's MaxAllocSize and reserved fallibly, so a hostile part is a
        // typed read error, never an allocator abort.
        const MAX_ALLOC_SIZE: usize = 0x3FFF_FFFF;
        let nwords = len.div_ceil(8);
        let mut words: Vec<u64> = Vec::new();
        if len > MAX_ALLOC_SIZE || words.try_reserve_exact(nwords).is_err() {
            return Err(ReadError::Io { at: "extent image allocation", errno: libc::ENOMEM });
        }
        words.resize(nwords, 0);
        // SAFETY: u64 → u8 reinterpret of an exclusively owned buffer;
        // alignment only loosens and `len <= words.len() * 8`.
        let bytes =
            unsafe { core::slice::from_raw_parts_mut(words.as_mut_ptr() as *mut u8, len) };
        fill(bytes)?;
        Ok(SegBuf {
            words: words.into(),
            len,
        })
    }

    /// Wrap a byte image (unwrapped-section rebuilds).
    pub fn from_bytes(v: &[u8]) -> SegBuf {
        SegBuf::build(v.len(), |b| {
            b.copy_from_slice(v);
            Ok(())
        })
        .expect("infallible fill")
    }

    /// Assemble an owned region from a fallible fill (the SB-7 multi-extent
    /// byte-run concatenation in `cursor::load_region`).
    pub(crate) fn assemble(
        len: usize,
        fill: impl FnOnce(&mut [u8]) -> ReadResult<()>,
    ) -> ReadResult<SegBuf> {
        SegBuf::build(len, fill)
    }

    pub fn bytes(&self) -> &[u8] {
        // SAFETY: the Arc'd u64 buffer is immutable and outlives the
        // returned slice; `len <= words.len() * 8`.
        unsafe { core::slice::from_raw_parts(self.words.as_ptr() as *const u8, self.len) }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

// ---------------------------------------------------------------------------
// fault witness
// ---------------------------------------------------------------------------

/// What a recorded fault touched.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultTag {
    /// PartTail (16 B, fixed open set).
    Tail,
    /// FooterFixed (96 B, fixed open set).
    Footer,
    /// The section table (fixed open set).
    SectionTable,
    /// PartHeader (64 B, fixed open set).
    Header,
    /// A footer-listed section faulted by table index.
    ListedSection {
        kind: u16,
        attno: u32,
        path_ord: u32,
    },
    /// A stream extent faulted through its ExtentRecord.
    StreamExtent {
        attno: u32,
        path_ord: u32,
        role: u8,
        extent: u32,
    },
}

/// One recorded fault: the region and why it was read. Faults are recorded
/// once per resident region — a cache hit records nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FaultEntry {
    pub tag: FaultTag,
    pub off: u64,
    pub len: u64,
}

// ---------------------------------------------------------------------------
// open-time validation facts
// ---------------------------------------------------------------------------

/// Validation facts the caller knows before open (manifest record + catalog
/// derivation). Every present fact is checked; a mismatch is a typed
/// [`ReadError::OpenMismatch`] (spec §13.1/§5.5).
#[derive(Debug, Clone, Copy, Default)]
pub struct PartExpect {
    pub part_no: Option<u32>,
    pub rows: Option<u64>,
    pub file_len: Option<u64>,
    pub footer_off: Option<u64>,
    pub schema_fingerprint: Option<u64>,
    pub relfilenumber: Option<u64>,
    pub spc_db: Option<(u32, u32)>,
}

impl PartExpect {
    /// No expectations (tests/tools). Product opens go through
    /// [`PartExpect::from_manifest`].
    pub fn none() -> PartExpect {
        PartExpect::default()
    }

    /// The full expectation set for a manifest-listed part.
    pub fn from_manifest(
        rec: &PartRecord,
        schema_fingerprint: u64,
        relfilenumber: u64,
        spc: u32,
        db: u32,
    ) -> PartExpect {
        PartExpect {
            part_no: Some(rec.part_no),
            rows: Some(rec.rows),
            file_len: Some(rec.file_len),
            footer_off: Some(rec.footer_off),
            schema_fingerprint: Some(schema_fingerprint),
            relfilenumber: Some(relfilenumber),
            spc_db: Some((spc, db)),
        }
    }
}

// ---------------------------------------------------------------------------
// OpenPart
// ---------------------------------------------------------------------------

struct PartState {
    /// The segment cache: (file_off, len) → CRC-validated bytes. Write-once
    /// per key; buffers never move after insert (Arc), which is what makes
    /// dict payload regions generation-stable (spec §7, StrView §7b).
    segs: BTreeMap<(u64, u64), SegBuf>,
    /// The UNWRAPPED-image cache for wrapped extents (O-CMP-5(a), ruled
    /// 2026-08-10): (file_off, len) of the WRAPPED extent → the rebuilt
    /// unwrapped (still-encoded) section image. Decompress once per part
    /// residency; scans hit encoded bytes. The wrapped file bytes are read,
    /// CRC-checked (the wrapper CRC law) and DROPPED — they never enter
    /// `segs` — so `resident` prices the image readers actually use.
    unwrapped: BTreeMap<(u64, u64), SegBuf>,
    /// Lazily parsed stream directory (spec §6.2).
    stream_dir: Option<Arc<StreamDirectory>>,
    /// Minted u64 dict epochs per (attno, path_ord) — one per open-part
    /// instance (crate doc "Dict epochs").
    dict_epochs: BTreeMap<(u32, u32), u64>,
    /// The fault log (witness; grows by one entry per resident region).
    faults: Vec<FaultEntry>,
    /// Extents a prefetch run is currently reading (claim-horizon lane):
    /// a demand fault that finds its key here WAITS on `inflight_cv`
    /// instead of re-issuing the read (duplicate reads are what helped
    /// kill the WILLNEED arm — RESULTS-OFFICIAL §6).
    inflight: std::collections::BTreeSet<(u64, u64)>,
    /// [densedict] PART-GRAIN block-lazy dict payload images (the
    /// RESULTS-STACK §4.2 re-pose): (file_off, len) of the WRAPPED
    /// single-extent dict payload → the shared block-image state. The
    /// `unwrapped`-map analogue at BLOCK grain — one pread + one-block
    /// decompress per (part, block) per part residency; handle reopens
    /// Arc-clone the image instead of re-decompressing (the +49% hot tax
    /// of the handle-grain arm). Only faulted blocks commit RSS
    /// (`alloc_zeroed` reservation), so cost ≤ the `unwrapped` image.
    dict_blocks: BTreeMap<(u64, u64), Arc<crate::dicthandle::BlockLazyPayload>>,
}

/// One open part: identity facts, validated fixed structures, and the lazy
/// segment cache. Shared process-wide as `Arc<OpenPart>` through
/// [`crate::registry::PartRegistry`]; all methods take `&self`.
pub struct OpenPart {
    io: Box<dyn PartIo>,
    ident: PartIdent,
    uuid: [u8; 16],
    header: PartHeader,
    footer: FooterFixed,
    /// The part's elected granule grain (SB-10) — validated at open from
    /// the footer; every granule/band closed form this crate computes keys
    /// off it.
    grain: pgrc2_format::geom::GranuleGrain,
    sections: Vec<SectionEntry>,
    /// Adjudicated kind per section: `None` = unknown-but-optional (skipped
    /// by readers, still CRC-faultable).
    known: Vec<Option<SectionKind>>,
    state: Mutex<PartState>,
    /// Signalled when a prefetch run retires in-flight keys.
    inflight_cv: pgsync::Condvar,
    /// Pin count (registry law: pinned parts are never evicted).
    pub(crate) pin_count: AtomicU32,
    /// Logical LRU stamp (registry clock — never wall time).
    pub(crate) last_used: AtomicU64,
    /// Bytes resident in the segment cache.
    resident: AtomicU64,
}

impl core::fmt::Debug for OpenPart {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("OpenPart")
            .field("ident", &self.ident)
            .field("uuid", &self.uuid)
            .finish_non_exhaustive()
    }
}

impl OpenPart {
    /// Open and validate a part (spec §5): tail → footer → section table →
    /// header, all CRC-checked, geometry echoes verified, expectation facts
    /// enforced. Exactly four reads; nothing else is faulted.
    pub fn open(io: Box<dyn PartIo>, expect: &PartExpect) -> ReadResult<OpenPart> {
        let file_len = io.len();
        let min = (PART_HEADER_LEN + FOOTER_FIXED_LEN + PART_TAIL_LEN) as u64;
        if file_len < min {
            return Err(ReadError::Format(FormatError::Truncated {
                at: "part file",
            }));
        }
        if let Some(want) = expect.file_len {
            if want != file_len {
                return Err(ReadError::OpenMismatch { field: "file_len" });
            }
        }
        let mut faults: Vec<FaultEntry> = Vec::with_capacity(4);

        // 1. Tail (spec §5.4).
        let tail_off = file_len - PART_TAIL_LEN as u64;
        let mut tail_buf = [0u8; PART_TAIL_LEN];
        io.pread_exact(tail_off, &mut tail_buf, "PartTail")?;
        faults.push(FaultEntry {
            tag: FaultTag::Tail,
            off: tail_off,
            len: PART_TAIL_LEN as u64,
        });
        let footer_off = decode_tail(&tail_buf, file_len)?;
        if let Some(want) = expect.footer_off {
            if want != footer_off {
                return Err(ReadError::OpenMismatch {
                    field: "footer_off",
                });
            }
        }
        let (dev, ino) = io.dev_ino();
        let ident = PartIdent {
            dev,
            ino,
            len: file_len,
            footer_off,
        };

        // 2. Footer (spec §5.3; CRC + geometry echoes inside decode).
        let mut footer_buf = [0u8; FOOTER_FIXED_LEN];
        io.pread_exact(footer_off, &mut footer_buf, "FooterFixed")?;
        faults.push(FaultEntry {
            tag: FaultTag::Footer,
            off: footer_off,
            len: FOOTER_FIXED_LEN as u64,
        });
        let footer = FooterFixed::decode(&footer_buf)?;
        // SB-10: the grain is ladder-validated inside decode; bind the
        // typed form once for every closed form downstream.
        let grain = footer.grain()?;
        if let Some(want) = expect.rows {
            if want != footer.rows {
                return Err(ReadError::OpenMismatch { field: "rows" });
            }
        }
        if let Some(want) = expect.part_no {
            if want != footer.part_no {
                return Err(ReadError::OpenMismatch { field: "part_no" });
            }
        }
        if let Some(want) = expect.schema_fingerprint {
            if want != footer.schema_fingerprint {
                return Err(ReadError::OpenMismatch {
                    field: "schema_fingerprint",
                });
            }
        }

        // 3. Section table (spec §5.2).
        let st_len = footer.section_count as u64 * SECTION_ENTRY_LEN as u64;
        let st_off = footer.section_table_off;
        if st_off
            .checked_add(st_len)
            .map(|end| end > file_len)
            .unwrap_or(true)
        {
            return Err(ReadError::Format(FormatError::Bounds {
                at: "section table",
            }));
        }
        let mut st_buf = vec![0u8; st_len as usize];
        io.pread_exact(st_off, &mut st_buf, "section table")?;
        faults.push(FaultEntry {
            tag: FaultTag::SectionTable,
            off: st_off,
            len: st_len,
        });
        if crc32c(&st_buf) != footer.section_table_crc {
            return Err(ReadError::Format(FormatError::CrcMismatch {
                at: "section table",
            }));
        }
        let mut sections = Vec::with_capacity(footer.section_count as usize);
        let mut known = Vec::with_capacity(footer.section_count as usize);
        let mut c = Cur::new(&st_buf);
        for _ in 0..footer.section_count {
            let e = SectionEntry::decode(&mut c)?;
            // Unknown non-optional kind: typed refusal at open (spec §5.2).
            known.push(e.known_kind()?);
            sections.push(e);
        }
        validate_section_layout(&sections, file_len, footer_off, st_off, st_len)?;

        // 4. Header (spec §5.1) + echo checks.
        let mut hdr_buf = [0u8; PART_HEADER_LEN];
        io.pread_exact(0, &mut hdr_buf, "PartHeader")?;
        faults.push(FaultEntry {
            tag: FaultTag::Header,
            off: 0,
            len: PART_HEADER_LEN as u64,
        });
        let header = PartHeader::decode(&hdr_buf)?;
        if header.part_no != footer.part_no {
            return Err(ReadError::Format(FormatError::Corrupt {
                at: "header/footer part_no echo",
            }));
        }
        if header.schema_fingerprint != footer.schema_fingerprint {
            return Err(ReadError::Format(FormatError::Corrupt {
                at: "header/footer fingerprint echo",
            }));
        }
        if let Some(want) = expect.relfilenumber {
            if want != header.relfilenumber {
                return Err(ReadError::OpenMismatch {
                    field: "relfilenumber",
                });
            }
        }
        if let Some((spc, db)) = expect.spc_db {
            if spc != header.spc || db != header.db {
                return Err(ReadError::OpenMismatch { field: "spc/db" });
            }
        }

        let uuid = part_uuid(&ident);
        Ok(OpenPart {
            io,
            ident,
            uuid,
            header,
            footer,
            grain,
            sections,
            known,
            state: Mutex::new(PartState {
                segs: BTreeMap::new(),
                unwrapped: BTreeMap::new(),
                stream_dir: None,
                dict_epochs: BTreeMap::new(),
                faults,
                inflight: std::collections::BTreeSet::new(),
                dict_blocks: BTreeMap::new(),
            }),
            pin_count: AtomicU32::new(0),
            last_used: AtomicU64::new(0),
            resident: AtomicU64::new(0),
            inflight_cv: pgsync::Condvar::new(),
        })
    }

    pub fn ident(&self) -> PartIdent {
        self.ident
    }

    pub fn uuid(&self) -> [u8; 16] {
        self.uuid
    }

    pub fn header(&self) -> &PartHeader {
        &self.header
    }

    pub fn footer(&self) -> &FooterFixed {
        &self.footer
    }

    pub fn rows(&self) -> u64 {
        self.footer.rows
    }

    /// The part's elected granule grain (SB-10; footer-validated at open).
    pub fn grain(&self) -> pgrc2_format::geom::GranuleGrain {
        self.grain
    }

    pub fn sections(&self) -> &[SectionEntry] {
        &self.sections
    }

    /// Adjudicated kind of section `idx` (`None` = unknown-but-optional).
    pub fn section_kind(&self, idx: usize) -> Option<SectionKind> {
        self.known.get(idx).copied().flatten()
    }

    /// Find the first section of `kind` for (attno, path_ord).
    pub fn find_section(&self, kind: SectionKind, attno: u32, path_ord: u32) -> Option<usize> {
        self.sections.iter().enumerate().position(|(i, e)| {
            self.known[i] == Some(kind) && e.attno == attno && e.path_ord == path_ord
        })
    }

    /// Bytes resident in the segment cache.
    pub fn resident(&self) -> u64 {
        self.resident.load(Ordering::Relaxed)
    }

    /// Snapshot of the fault log (the O(streams-touched) witness).
    pub fn faults(&self) -> Vec<FaultEntry> {
        lock(&self.state).faults.clone()
    }

    /// Fault one footer-listed section by table index: CRC-validated against
    /// the section entry, cached, logged once.
    pub fn section_bytes(&self, idx: usize) -> ReadResult<SegBuf> {
        let e = *self
            .sections
            .get(idx)
            .ok_or(ReadError::Format(FormatError::Bounds {
                at: "section index",
            }))?;
        self.fault_range(
            e.off,
            e.len,
            e.crc,
            "listed section",
            FaultTag::ListedSection {
                kind: e.kind,
                attno: e.attno,
                path_ord: e.path_ord,
            },
        )
    }

    /// Fault one stream extent (spec §6.4): CRC-validated against the
    /// ExtentRecord, cached, logged once. `entry` supplies the log identity.
    pub fn extent_bytes(
        &self,
        entry: &StreamEntry,
        rec: &ExtentRecord,
        extent_idx: u32,
    ) -> ReadResult<SegBuf> {
        if STREAM_FAULT_OBS_ENABLED.load(Ordering::Relaxed) != 0 {
            let resident = lock(&self.state).segs.contains_key(&(rec.file_off, rec.len));
            observe_stream_fault(&StreamFaultEvent {
                part: self as *const OpenPart as usize,
                attno: entry.attno,
                path_ord: entry.path_ord,
                role: entry.role,
                extent_idx,
                granule_start: rec.granule_start,
                granule_count: rec.granule_count,
                file_off: rec.file_off,
                len: rec.len,
                resident,
                wrapped: false,
            });
        }
        self.fault_range(
            rec.file_off,
            rec.len,
            rec.crc,
            "stream extent",
            FaultTag::StreamExtent {
                attno: entry.attno,
                path_ord: entry.path_ord,
                role: entry.role,
                extent: extent_idx,
            },
        )
    }

    /// [sqe8blk] One WHOLE stream extent, CRC-validated, WITHOUT part-cache
    /// retention — the block-lazy `ensure_all` whole-read (RESULTS-
    /// DENSEDICT §3.1 banked refinement 1). The caller immediately
    /// decompresses the wrapped payload into its own write-once block
    /// image; retaining the raw extent alongside (what `extent_bytes`
    /// does) doubled the dense first-touch memory traffic AND resident
    /// bytes. Cache hit: the cached image is served (already resident —
    /// costs nothing extra, is never evicted here). Miss: registers
    /// in-flight first (the C6 courtesy — a racing prefetch run drops the
    /// extent instead of double-reading; a racing demand faulter waits),
    /// preads + CRC-checks, retires the key WITHOUT inserting. A waiter
    /// that wakes to a still-missing image issues its own read (bounded;
    /// only across distinct handles racing the same extent).
    pub fn extent_bytes_uncached(
        &self,
        entry: &StreamEntry,
        rec: &ExtentRecord,
        extent_idx: u32,
    ) -> ReadResult<SegBuf> {
        let at: &'static str = "stream extent";
        let key = (rec.file_off, rec.len);
        if STREAM_FAULT_OBS_ENABLED.load(Ordering::Relaxed) != 0 {
            let resident = lock(&self.state).segs.contains_key(&key);
            observe_stream_fault(&StreamFaultEvent {
                part: self as *const OpenPart as usize,
                attno: entry.attno,
                path_ord: entry.path_ord,
                role: entry.role,
                extent_idx,
                granule_start: rec.granule_start,
                granule_count: rec.granule_count,
                file_off: rec.file_off,
                len: rec.len,
                resident,
                wrapped: false,
            });
        }
        loop {
            let mut st = lock(&self.state);
            if let Some(b) = st.segs.get(&key) {
                return Ok(b.clone());
            }
            if st.inflight.contains(&key) {
                while st.inflight.contains(&key) {
                    st = self.inflight_cv.wait(st).unwrap_or_else(|e| e.into_inner());
                }
                continue; // re-check the cache under the same lock
            }
            st.inflight.insert(key);
            break;
        }
        let retire = |this: &OpenPart| {
            let mut st = lock(&this.state);
            st.inflight.remove(&key);
            drop(st);
            this.inflight_cv.notify_all();
        };
        if rec
            .file_off
            .checked_add(rec.len)
            .map(|end| end > self.io.len())
            .unwrap_or(true)
        {
            retire(self);
            return Err(ReadError::Format(FormatError::Bounds { at }));
        }
        let buf = match SegBuf::build(rec.len as usize, |b| {
            self.io.pread_exact(rec.file_off, b, at)
        }) {
            Ok(b) => b,
            Err(e) => {
                retire(self);
                return Err(e);
            }
        };
        if crc32c(buf.bytes()) != rec.crc {
            retire(self);
            return Err(ReadError::Format(FormatError::CrcMismatch { at }));
        }
        retire(self);
        Ok(buf)
    }

    /// Fault one WRAPPED stream extent and return its UNWRAPPED (encoded)
    /// section image — the O-CMP-5(a) residency home. Cache hit: the
    /// rebuilt image, no I/O, no decompress (once per part residency). Miss:
    /// bounds-check, read, CRC-validate the WRAPPED bytes against the
    /// ExtentRecord (the wrapper CRC law — CRC over the wrapped bytes),
    /// hand them to `rebuild` (the cursor's unwrapper adjudication +
    /// header cross-witness + unwrap), cache ONLY the rebuilt image, and
    /// account `resident` at the image's real size. The transient wrapped
    /// buffer drops — RAM cost of a wrapped extent = its encoded image.
    /// Racing faulters both rebuild; the first insert wins (equal identity
    /// ⇒ identical bytes, spec §11) and is the one logged.
    pub fn unwrapped_extent_bytes(
        &self,
        entry: &StreamEntry,
        rec: &ExtentRecord,
        extent_idx: u32,
        rebuild: &mut dyn FnMut(&[u8]) -> ReadResult<Vec<u8>>,
    ) -> ReadResult<SegBuf> {
        let key = (rec.file_off, rec.len);
        let hit = lock(&self.state).unwrapped.get(&key).cloned();
        if STREAM_FAULT_OBS_ENABLED.load(Ordering::Relaxed) != 0 {
            observe_stream_fault(&StreamFaultEvent {
                part: self as *const OpenPart as usize,
                attno: entry.attno,
                path_ord: entry.path_ord,
                role: entry.role,
                extent_idx,
                granule_start: rec.granule_start,
                granule_count: rec.granule_count,
                file_off: rec.file_off,
                len: rec.len,
                resident: hit.is_some(),
                wrapped: true,
            });
        }
        if let Some(b) = hit {
            return Ok(b);
        }
        if self.wait_inflight(key) {
            if let Some(b) = lock(&self.state).unwrapped.get(&key) {
                return Ok(b.clone());
            }
        }
        let at: &'static str = "wrapped stream extent";
        if rec
            .file_off
            .checked_add(rec.len)
            .map(|end| end > self.io.len())
            .unwrap_or(true)
        {
            return Err(ReadError::Format(FormatError::Bounds { at }));
        }
        let raw = SegBuf::build(rec.len as usize, |b| {
            self.io.pread_exact(rec.file_off, b, at)
        })?;
        if crc32c(raw.bytes()) != rec.crc {
            return Err(ReadError::Format(FormatError::CrcMismatch { at }));
        }
        let rebuilt = rebuild(raw.bytes())?;
        drop(raw);
        let buf = SegBuf::from_bytes(&rebuilt);
        let mut st = lock(&self.state);
        if let Some(b) = st.unwrapped.get(&key) {
            return Ok(b.clone());
        }
        st.unwrapped.insert(key, buf.clone());
        st.faults.push(FaultEntry {
            tag: FaultTag::StreamExtent {
                attno: entry.attno,
                path_ord: entry.path_ord,
                role: entry.role,
                extent: extent_idx,
            },
            off: rec.file_off,
            len: rec.len,
        });
        self.resident
            .fetch_add(rebuilt.len() as u64, Ordering::Relaxed);
        Ok(buf)
    }

    /// [stack] Raw sub-range pread INSIDE a listed stream extent — the
    /// block-lazy wrapped-payload serving primitive (RESULTS-FMTLAND §B.3).
    /// Bounds-checked against the extent and the file; NOT extent-CRC
    /// validated (a strict sub-range cannot be checked against the extent's
    /// whole-range CRC — integrity rides the per-block codec decode); NOT
    /// cached (the bytes immediately decompress into the caller's
    /// write-once region and drop). When the WHOLE extent is already
    /// resident (a claim-horizon prefetch or an earlier whole fault), the
    /// cached image is sliced instead of re-read; an in-flight whole-extent
    /// read is waited on first (the C6 courtesy, not a second read).
    pub fn subrange_bytes(
        &self,
        rec: &ExtentRecord,
        off_in_extent: u64,
        len: u64,
        at: &'static str,
    ) -> ReadResult<SegBuf> {
        if off_in_extent
            .checked_add(len)
            .map(|end| end > rec.len)
            .unwrap_or(true)
        {
            return Err(ReadError::Format(FormatError::Bounds { at }));
        }
        let whole_key = (rec.file_off, rec.len);
        let slice_of = |seg: &SegBuf| -> SegBuf {
            SegBuf::from_bytes(
                &seg.bytes()[off_in_extent as usize..(off_in_extent + len) as usize],
            )
        };
        if let Some(b) = lock(&self.state).segs.get(&whole_key).cloned() {
            return Ok(slice_of(&b));
        }
        if self.wait_inflight(whole_key) {
            if let Some(b) = lock(&self.state).segs.get(&whole_key).cloned() {
                return Ok(slice_of(&b));
            }
        }
        let off = rec.file_off + off_in_extent;
        if off.checked_add(len).map(|end| end > self.io.len()).unwrap_or(true) {
            return Err(ReadError::Format(FormatError::Bounds { at }));
        }
        SegBuf::build(len as usize, |b| self.io.pread_exact(off, b, at))
    }

    /// [densedict] The PART-GRAIN shared block-lazy image for a wrapped
    /// dict payload extent (the RESULTS-STACK §4.2 re-pose). Get-or-create;
    /// the returned Arc keeps the image (and every published block pointer)
    /// alive for the caller's life even across part-cache churn — the same
    /// generation-stability rooting the handle-grain arm had, now shared by
    /// every handle that reopens the same extent in this part residency.
    pub(crate) fn dict_block_payload(
        &self,
        key: (u64, u64),
    ) -> Arc<crate::dicthandle::BlockLazyPayload> {
        let mut st = lock(&self.state);
        st.dict_blocks
            .entry(key)
            .or_insert_with(|| Arc::new(crate::dicthandle::BlockLazyPayload::new()))
            .clone()
    }

    /// The lazily-parsed stream directory (spec §6.2). First call faults the
    /// StreamDir section; later calls are cache hits.
    pub fn stream_directory(&self) -> ReadResult<Arc<StreamDirectory>> {
        if let Some(d) = lock(&self.state).stream_dir.clone() {
            return Ok(d);
        }
        let idx = self
            .find_section(SectionKind::StreamDir, 0, 0)
            .ok_or(ReadError::Format(FormatError::Corrupt {
                at: "missing StreamDir section",
            }))?;
        let bytes = self.section_bytes(idx)?;
        let dir = Arc::new(StreamDirectory::parse(bytes.bytes(), self.footer.stream_count)?);
        let mut st = lock(&self.state);
        // First parse wins (racing parses produce identical directories —
        // equal identity ⇒ identical bytes).
        if let Some(d) = st.stream_dir.clone() {
            return Ok(d);
        }
        st.stream_dir = Some(dir.clone());
        Ok(dir)
    }

    /// Eagerly fault EVERY footer-listed section (cache warming / integrity
    /// sweep — also the born-RED gate's "whole-part fault" tooth: the
    /// O(streams-touched) witness must detect this).
    pub fn prefault_all_sections(&self) -> ReadResult<u32> {
        let mut n = 0u32;
        for idx in 0..self.sections.len() {
            self.section_bytes(idx)?;
            n += 1;
        }
        Ok(n)
    }

    /// The minted u64 dict epoch for (attno, path_ord) — one per open-part
    /// instance, from the process-global epoch clock (crate doc).
    pub fn dict_epoch(&self, attno: u32, path_ord: u32) -> u64 {
        let mut st = lock(&self.state);
        if let Some(&e) = st.dict_epochs.get(&(attno, path_ord)) {
            return e;
        }
        let e = EPOCH_CLOCK.fetch_add(1, Ordering::Relaxed);
        st.dict_epochs.insert((attno, path_ord), e);
        e
    }

    /// Advisory WILLNEED over a run of stream extents (the cold-readahead
    /// claim-hook face, C3 inherit). Contract:
    ///
    /// - ADVISORY ONLY: never faults, never errors, never changes what a
    ///   later `extent_bytes` returns — a refused hint costs speed, not rows.
    /// - Residency-skipping: extents already in the segment cache (plain or
    ///   unwrapped) are skipped, so hot repetitions issue ~zero syscalls.
    /// - Run-merging: file-contiguous non-resident extents coalesce into one
    ///   hint (extents of one stream ascend by construction, so a granule
    ///   run usually costs one syscall per stream).
    /// - Bounds-clamped against the real file length (a lying record is
    ///   skipped here and reported typed by the real read).
    ///
    /// Returns `(hints_issued, bytes_hinted)` — the caller's witness input
    /// (the no-silent-no-op law).
    pub fn advise_extent_run(&self, recs: &[ExtentRecord]) -> (u64, u64) {
        let file_len = self.io.len();
        let mut hints = 0u64;
        let mut bytes = 0u64;
        let mut run_off = 0u64;
        let mut run_len = 0u64;
        for r in recs {
            let in_bounds = r.len > 0
                && r.file_off
                    .checked_add(r.len)
                    .map(|end| end <= file_len)
                    .unwrap_or(false);
            let resident = in_bounds && {
                let key = (r.file_off, r.len);
                let st = lock(&self.state);
                st.segs.contains_key(&key) || st.unwrapped.contains_key(&key)
            };
            if !in_bounds || resident {
                if run_len > 0 {
                    self.io.advise_willneed(run_off, run_len);
                    hints += 1;
                    bytes += run_len;
                    run_len = 0;
                }
                continue;
            }
            if run_len > 0 && run_off + run_len == r.file_off {
                run_len += r.len;
            } else {
                if run_len > 0 {
                    self.io.advise_willneed(run_off, run_len);
                    hints += 1;
                    bytes += run_len;
                }
                run_off = r.file_off;
                run_len = r.len;
            }
        }
        if run_len > 0 {
            self.io.advise_willneed(run_off, run_len);
            hints += 1;
            bytes += run_len;
        }
        (hints, bytes)
    }

    /// Prefetch a run of stream extents with REAL reads into the residency
    /// plane (claim-horizon lane — the advisory WILLNEED face above was
    /// measured NEGATIVE on gp2; this face reads). Contract:
    ///
    /// - Warms EXACTLY the caches the cursor faces consult: unwrapped
    ///   entries (`entry.wrapper != None`, via `rebuild` — the caller
    ///   supplies the same unwrapper adjudication `cursor::load_extent`
    ///   uses) land in the unwrapped-image cache; plain extents land in
    ///   `segs`. A later demand access is a cache hit — byte-identical by
    ///   the insert-if-absent law (equal identity ⇒ identical bytes).
    /// - Coalescing: file-contiguous non-resident extents are read with ONE
    ///   pread of up to `max_run` bytes, then split, CRC-validated and
    ///   inserted per extent.
    /// - NEVER fails the scan: bounds/CRC/IO/rebuild problems skip the
    ///   extent — the demand path surfaces them typed exactly as today.
    /// - Resident/fault accounting matches what the demand faults would
    ///   have recorded.
    ///
    /// Returns `(inserted_extents, inserted_bytes, already_resident,
    /// io_bytes, io_runs)`.
    pub fn prefetch_extent_run(
        &self,
        entry: &StreamEntry,
        recs: &[(u32, ExtentRecord)],
        max_run: u64,
        rebuild: Option<&mut dyn FnMut(&[u8]) -> ReadResult<Vec<u8>>>,
    ) -> (u64, u64, u64, u64, u64) {
        self.prefetch_extent_run_holes(entry, recs, max_run, 0, rebuild, None)
    }

    /// `prefetch_extent_run` with a HOLE LIMIT (the Arrow/DuckDB
    /// bandwidth×latency rule): extents separated by ≤ `max_hole` bytes of
    /// unneeded file are read in ONE pread (the hole bytes are discarded)
    /// when the whole run stays ≤ `max_run`. `max_hole = 0` = strictly
    /// contiguous. Extents in the run are marked IN-FLIGHT for the
    /// duration of the read; a demand fault on one of them waits instead
    /// of re-issuing. `inserted_log`, when given, receives the (file_off,
    /// len) key of every extent THIS call inserted — the caller's
    /// provenance witness ([claimh-cooling]: only prefetch-inserted images
    /// are probationary; already-resident drop-outs must never be evicted).
    pub fn prefetch_extent_run_holes(
        &self,
        entry: &StreamEntry,
        recs: &[(u32, ExtentRecord)],
        max_run: u64,
        max_hole: u64,
        mut rebuild: Option<&mut dyn FnMut(&[u8]) -> ReadResult<Vec<u8>>>,
        mut inserted_log: Option<&mut Vec<(u64, u64)>>,
    ) -> (u64, u64, u64, u64, u64) {
        let wrapped = entry.wrapper != 0;
        if wrapped && rebuild.is_none() {
            return (0, 0, 0, 0, 0);
        }
        let file_len = self.io.len();
        let (mut inserted, mut inserted_bytes, mut already, mut io_bytes, mut io_runs) =
            (0u64, 0u64, 0u64, 0u64, 0u64);
        let mut run: Vec<(u32, ExtentRecord)> = Vec::new();
        let mut run_end = 0u64;
        let flush = |run: &mut Vec<(u32, ExtentRecord)>,
                         inserted: &mut u64,
                         inserted_bytes: &mut u64,
                         io_bytes: &mut u64,
                         io_runs: &mut u64,
                         rebuild: &mut Option<&mut dyn FnMut(&[u8]) -> ReadResult<Vec<u8>>>,
                         inserted_log: &mut Option<&mut Vec<(u64, u64)>>| {
            if run.is_empty() {
                return;
            }
            let off0 = run[0].1.file_off;
            let last = &run[run.len() - 1].1;
            let total = (last.file_off + last.len - off0) as usize;
            // Register the run in flight (skip keys a demand fault or a
            // racing run already resident/in-flight — those drop out).
            {
                let mut st = lock(&self.state);
                run.retain(|(_, r)| {
                    let k = (r.file_off, r.len);
                    let present = if wrapped { st.unwrapped.contains_key(&k) } else { st.segs.contains_key(&k) };
                    !present && !st.inflight.contains(&k)
                });
                for (_, r) in run.iter() {
                    st.inflight.insert((r.file_off, r.len));
                }
            }
            if run.is_empty() {
                return;
            }
            let mut buf = vec![0u8; total];
            let ok = self.io.pread_exact(off0, &mut buf, "prefetch extent run").is_ok();
            let retire = |this: &OpenPart, run: &Vec<(u32, ExtentRecord)>| {
                let mut st = lock(&this.state);
                for (_, r) in run.iter() {
                    st.inflight.remove(&(r.file_off, r.len));
                }
                drop(st);
                this.inflight_cv.notify_all();
            };
            if ok {
                *io_bytes += total as u64;
                *io_runs += 1;
                for (idx, rec) in run.iter() {
                    let s = (rec.file_off - off0) as usize;
                    let raw = &buf[s..s + rec.len as usize];
                    if crc32c(raw) != rec.crc {
                        continue; // demand read reports this typed
                    }
                    let key = (rec.file_off, rec.len);
                    let tag = FaultTag::StreamExtent {
                        attno: entry.attno,
                        path_ord: entry.path_ord,
                        role: entry.role,
                        extent: *idx,
                    };
                    if wrapped {
                        let rebuilt = match rebuild.as_mut().unwrap()(raw) {
                            Ok(v) => v,
                            Err(_) => continue,
                        };
                        let img = SegBuf::from_bytes(&rebuilt);
                        let mut st = lock(&self.state);
                        if st.unwrapped.contains_key(&key) {
                            continue;
                        }
                        st.unwrapped.insert(key, img);
                        st.faults.push(FaultEntry { tag, off: rec.file_off, len: rec.len });
                        drop(st);
                        self.resident.fetch_add(rebuilt.len() as u64, Ordering::Relaxed);
                        if let Some(log) = inserted_log.as_mut() {
                            log.push(key);
                        }
                        *inserted += 1;
                        *inserted_bytes += rec.len;
                    } else {
                        let img = SegBuf::from_bytes(raw);
                        let mut st = lock(&self.state);
                        if st.segs.contains_key(&key) {
                            continue;
                        }
                        st.segs.insert(key, img);
                        st.faults.push(FaultEntry { tag, off: rec.file_off, len: rec.len });
                        drop(st);
                        self.resident.fetch_add(rec.len, Ordering::Relaxed);
                        if let Some(log) = inserted_log.as_mut() {
                            log.push(key);
                        }
                        *inserted += 1;
                        *inserted_bytes += rec.len;
                    }
                }
            }
            retire(self, run);
            run.clear();
        };
        for (idx, rec) in recs {
            let in_bounds = rec.len > 0
                && rec
                    .file_off
                    .checked_add(rec.len)
                    .map(|end| end <= file_len)
                    .unwrap_or(false);
            if !in_bounds {
                flush(&mut run, &mut inserted, &mut inserted_bytes, &mut io_bytes, &mut io_runs, &mut rebuild, &mut inserted_log);
                run_end = 0;
                continue;
            }
            let key = (rec.file_off, rec.len);
            let resident = {
                let st = lock(&self.state);
                if wrapped { st.unwrapped.contains_key(&key) } else { st.segs.contains_key(&key) }
            };
            if resident {
                already += 1;
                flush(&mut run, &mut inserted, &mut inserted_bytes, &mut io_bytes, &mut io_runs, &mut rebuild, &mut inserted_log);
                run_end = 0;
                continue;
            }
            let near = !run.is_empty()
                && rec.file_off >= run_end
                && rec.file_off - run_end <= max_hole;
            let fits = run.is_empty()
                || (near && rec.file_off + rec.len - run[0].1.file_off <= max_run);
            if !fits {
                flush(&mut run, &mut inserted, &mut inserted_bytes, &mut io_bytes, &mut io_runs, &mut rebuild, &mut inserted_log);
            }
            run_end = rec.file_off + rec.len;
            run.push((*idx, *rec));
        }
        flush(&mut run, &mut inserted, &mut inserted_bytes, &mut io_bytes, &mut io_runs, &mut rebuild, &mut inserted_log);
        (inserted, inserted_bytes, already, io_bytes, io_runs)
    }

    /// [claimh-cooling] Evict a set of extent images from the residency
    /// plane (both `segs` and `unwrapped`). Scan-resistant admission
    /// (LeanStore-style cooling mapped onto this plane): prefetch-admitted
    /// images that were never consumed by a demand access are probationary;
    /// the caller (the claim-horizon prefetcher's rep gate) evicts them at
    /// the cold→hot rep boundary so hot reps walk exactly the demand-built
    /// residency set. Eviction is cache-only and can never change what a
    /// read returns — a later demand access simply re-faults, CRC-checked,
    /// exactly as a first access would. Keys currently IN FLIGHT are
    /// skipped (a racing run's insert would resurrect them anyway).
    /// Returns `(evicted_extents, evicted_bytes)`; `resident` is debited
    /// by the evicted image sizes.
    pub fn evict_extent_images(&self, keys: &[(u64, u64)]) -> (u64, u64) {
        let mut n = 0u64;
        let mut bytes = 0u64;
        {
            let mut st = lock(&self.state);
            for k in keys {
                if st.inflight.contains(k) {
                    continue;
                }
                if let Some(b) = st.segs.remove(k) {
                    n += 1;
                    bytes += b.len() as u64;
                } else if let Some(b) = st.unwrapped.remove(k) {
                    n += 1;
                    bytes += b.len() as u64;
                }
            }
        }
        if bytes > 0 {
            self.resident.fetch_sub(bytes, Ordering::Relaxed);
        }
        (n, bytes)
    }

    /// [claim-horizon] Block while a prefetch run holds `key` in flight
    /// (returns immediately when it does not). Called by the demand
    /// faces before issuing their own read; the cache lookup that follows
    /// finds the prefetched image. Bounded: an in-flight run always
    /// retires its keys (success or skip).
    fn wait_inflight(&self, key: (u64, u64)) -> bool {
        let mut st = lock(&self.state);
        if !st.inflight.contains(&key) {
            return false;
        }
        while st.inflight.contains(&key) {
            st = self.inflight_cv.wait(st).unwrap_or_else(|e| e.into_inner());
        }
        true
    }

    /// The shared fault engine: cache lookup → (outside the lock) bounded
    /// read + CRC check → insert-if-absent. Racing faulters both read; the
    /// first insert wins and is the one logged — sound because equal
    /// identity ⇒ identical bytes (spec §11); the loser's buffer drops.
    fn fault_range(
        &self,
        off: u64,
        len: u64,
        crc: u32,
        at: &'static str,
        tag: FaultTag,
    ) -> ReadResult<SegBuf> {
        let key = (off, len);
        // [cold2] Demand faults REGISTER IN FLIGHT before reading (the
        // prefetch runs always did): a racing prefetch run now drops the
        // extent from its run (`retain(!inflight)`) instead of reading it a
        // second time, and a racing demand faulter waits instead of
        // re-issuing. Measured on the 100m bank: single-extent dict payload
        // regions consumed part-parallel (VerdictWords, hash combines) were
        // read TWICE — once by the worker, once by the chained issuer —
        // pf_wasted ≈ the payload size and R_MBps at half the ceiling.
        if !demand_inflight() {
            // Kill-switch arm (PGRUST_PGRC2_DEMAND_INFLIGHT=0): the pre-fix
            // race — read without registering; first insert wins.
            if let Some(b) = lock(&self.state).segs.get(&key) {
                return Ok(b.clone());
            }
            if self.wait_inflight(key) {
                if let Some(b) = lock(&self.state).segs.get(&key) {
                    return Ok(b.clone());
                }
            }
            if off.checked_add(len).map(|end| end > self.io.len()).unwrap_or(true) {
                return Err(ReadError::Format(FormatError::Bounds { at }));
            }
            let buf = SegBuf::build(len as usize, |b| self.io.pread_exact(off, b, at))?;
            if crc32c(buf.bytes()) != crc {
                return Err(ReadError::Format(FormatError::CrcMismatch { at }));
            }
            let mut st = lock(&self.state);
            if let Some(b) = st.segs.get(&key) {
                return Ok(b.clone());
            }
            st.segs.insert(key, buf.clone());
            st.faults.push(FaultEntry { tag, off, len });
            self.resident.fetch_add(len, Ordering::Relaxed);
            return Ok(buf);
        }
        loop {
            let mut st = lock(&self.state);
            if let Some(b) = st.segs.get(&key) {
                return Ok(b.clone());
            }
            if st.inflight.contains(&key) {
                while st.inflight.contains(&key) {
                    st = self.inflight_cv.wait(st).unwrap_or_else(|e| e.into_inner());
                }
                continue; // re-check the cache under the same lock
            }
            st.inflight.insert(key);
            break;
        }
        let retire = |this: &OpenPart| {
            let mut st = lock(&this.state);
            st.inflight.remove(&key);
            drop(st);
            this.inflight_cv.notify_all();
        };
        if off
            .checked_add(len)
            .map(|end| end > self.io.len())
            .unwrap_or(true)
        {
            retire(self);
            return Err(ReadError::Format(FormatError::Bounds { at }));
        }
        let buf = match SegBuf::build(len as usize, |b| self.io.pread_exact(off, b, at)) {
            Ok(b) => b,
            Err(e) => {
                retire(self);
                return Err(e);
            }
        };
        if crc32c(buf.bytes()) != crc {
            retire(self);
            return Err(ReadError::Format(FormatError::CrcMismatch { at }));
        }
        let mut st = lock(&self.state);
        if let Some(b) = st.segs.get(&key).cloned() {
            st.inflight.remove(&key);
            drop(st);
            self.inflight_cv.notify_all();
            return Ok(b);
        }
        st.segs.insert(key, buf.clone());
        st.faults.push(FaultEntry { tag, off, len });
        st.inflight.remove(&key);
        drop(st);
        self.inflight_cv.notify_all();
        self.resident.fetch_add(len, Ordering::Relaxed);
        Ok(buf)
    }
}

/// [cold2] kill switch for demand-fault in-flight registration
/// (`PGRUST_PGRC2_DEMAND_INFLIGHT=0` = the pre-fix racing arm). Read once.
fn demand_inflight() -> bool {
    static F: pgsync::OnceLock<bool> = pgsync::OnceLock::new();
    *F.get_or_init(|| !matches!(std::env::var("PGRUST_PGRC2_DEMAND_INFLIGHT").as_deref(), Ok("0")))
}

/// Poison recovery: a panicked holder cannot corrupt this state (buffers are
/// write-once CRC-validated bytes; maps are insert-only), so continue.
fn lock<T>(m: &Mutex<T>) -> pgsync::MutexGuard<'_, T> {
    match m.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    }
}

/// Decode the 16 tail bytes (spec §5.4) against the real file length —
/// mirrors `PartTail::decode_at_eof`, which wants the whole file image this
/// crate must never read.
fn decode_tail(buf: &[u8; PART_TAIL_LEN], file_len: u64) -> ReadResult<u64> {
    let mut c = Cur::new(buf);
    let footer_off = c.u64("PartTail")?;
    let footer_len = c.u32("PartTail")?;
    let magic = c.u32("PartTail")?;
    if magic != TAIL_MAGIC {
        return Err(ReadError::Format(FormatError::BadMagic { at: "PartTail" }));
    }
    if footer_len != FOOTER_FIXED_LEN as u32 {
        return Err(ReadError::Format(FormatError::Corrupt {
            at: "PartTail footer_len",
        }));
    }
    if footer_off
        .checked_add((FOOTER_FIXED_LEN + PART_TAIL_LEN) as u64)
        .map(|end| end > file_len)
        .unwrap_or(true)
    {
        return Err(ReadError::Format(FormatError::Bounds {
            at: "PartTail footer_off",
        }));
    }
    Ok(footer_off)
}

/// Structural layout validation of the section table (spec §5.2/§1): every
/// section inside the file, past the header, and no two sections (or the
/// footer/tail/table regions) overlap.
fn validate_section_layout(
    sections: &[SectionEntry],
    file_len: u64,
    footer_off: u64,
    st_off: u64,
    st_len: u64,
) -> ReadResult<()> {
    let mut ranges: Vec<(u64, u64)> = Vec::with_capacity(sections.len() + 2);
    for e in sections {
        let end = e
            .off
            .checked_add(e.len)
            .ok_or(ReadError::Format(FormatError::Bounds {
                at: "section range",
            }))?;
        if e.off < PART_HEADER_LEN as u64 || end > file_len {
            return Err(ReadError::Format(FormatError::Bounds {
                at: "section range",
            }));
        }
        ranges.push((e.off, end));
    }
    ranges.push((st_off, st_off + st_len));
    ranges.push((
        footer_off,
        footer_off + (FOOTER_FIXED_LEN + PART_TAIL_LEN) as u64,
    ));
    ranges.sort_unstable();
    for w in ranges.windows(2) {
        // Zero-length sections may share offsets; real ranges must not
        // overlap.
        if w[1].0 < w[0].1 {
            return Err(ReadError::Format(FormatError::Corrupt {
                at: "section overlap",
            }));
        }
    }
    Ok(())
}
