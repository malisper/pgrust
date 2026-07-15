//! Morsel-parallel COPY FROM (load-speed lane L2/L3 —
//! docs/design/load-speed-2026-07.md §5 lever 1, the measured 7.33x@k8
//! ceiling with flat CPU).
//!
//! Shape (the ClickHouse pipeline translated to this runtime):
//!  1. SEGMENTATOR (leader): stream the COPY input, find row boundaries
//!     cheaply (memchr terminator scan + backslash-run parity — never the
//!     full parse), publish whole-RG chunk descriptors (65,536 rows each —
//!     RG seams fall exactly where the serial writer's would) as claimable
//!     granules of a runtime [`runtime::StreamSource`].
//!  2. WORKERS (full-identity parallel helpers, the vacuum-morsels
//!     ceremony): claim chunks off the pinned RG, parse+convert through the
//!     UNCHANGED per-chunk COPY machinery (CopySrc::Chunk), run the serial
//!     path's exec_constraints, and encode whole RGs via
//!     [`cbstore::RgChunkEncoder`].
//!  3. ORDERED COMMITTER (leader): commit encoded RGs in INPUT ORDER into
//!     the one [`cbstore::CbWriter`] — the part is BYTE-IDENTICAL to a
//!     serial COPY of the same stream (the acceptance oracle).
//!
//! Error semantics: workers record chunk-indexed errors (context lines
//! attached with the worker's exact cur_lineno); chunks past the lowest
//! erroring index drain; the leader re-raises the minimum-index error after
//! completion — first-error-in-input-order, exactly like serial.
//!
//! Admission is FAIL-CLOSED (every refusal is today's serial COPY,
//! byte-identically): PGRUST_PARALLEL_COPY=1 + a live runtime; cbstore AM
//! only; text (non-CSV, non-binary) format; no triggers, no WHERE clause,
//! no defaults, no ON_ERROR ignore, no header, no transcoding, no
//! cluster_key, no indexes, no generated columns.

use std::collections::{BTreeMap, HashMap};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use elog::ereport;
use mcx::{vec_from_elem_in, Mcx, MemoryContext, PgVec};
use types_core::Oid;
use types_error::{PgError, PgResult, ERROR, WARNING};
use types_fmgr::FmgrInfo;
use types_rel::Relation;

use backend_progress::progress::{PROGRESS_COPY_BYTES_PROCESSED, PROGRESS_COPY_TUPLES_PROCESSED};
use backend_progress::pgstat_progress_update_param;

use crate::from::{copy_from_error_context, CopyFromState, CopySrc};
use crate::fromparse::EolType;
use crate::{CopyFormatOptions, CopyHeaderChoice, CopyOnErrorChoice};

// ---------------------------------------------------------------------------
// Knobs (env: new real GUCs are barred by pg_settings byte-identity — the
// runtime-lane precedent).
// ---------------------------------------------------------------------------

fn flag_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| std::env::var("PGRUST_PARALLEL_COPY").is_ok_and(|v| v.trim() == "1"))
}

/// Engagement/refusal trace (PGRUST_PARALLEL_COPY_TRACE=1): the e2e
/// battery's engagement oracle channel. Default-off, zero cost.
fn ptrace_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| std::env::var("PGRUST_PARALLEL_COPY_TRACE").is_ok_and(|v| v.trim() == "1"))
}

fn ptrace(msg: &str) {
    if ptrace_enabled() {
        eprintln!("parallel-copy: {msg}");
    }
}

macro_rules! refuse {
    ($why:expr) => {{
        ptrace(&format!("refused: {}", $why));
        return Ok(None);
    }};
}

/// load-r2 L3-1: PGRUST_PARALLEL_COPY_SORT=1 lets a PGRUST_COPY_PRESORT
/// load engage the PARALLEL sort pipeline (workers spill memcmp-key runs,
/// leader k-way merges into the plain writer). Default OFF: presort loads
/// refuse to the serial sort-on-ingest path verbatim.
fn sort_flag_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| std::env::var("PGRUST_PARALLEL_COPY_SORT").is_ok_and(|v| v.trim() == "1"))
}

/// Per-worker in-memory (key,row) batch budget before a run spill
/// (PGRUST_PARALLEL_COPY_SORT_MEM, MB; default 256, floor 1 — the floor
/// exists for the e2e battery's multi-run coverage, not for production).
fn sort_budget() -> usize {
    static N: OnceLock<usize> = OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("PGRUST_PARALLEL_COPY_SORT_MEM")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .unwrap_or(256)
            .max(1)
            * (1 << 20)
    })
}

/// Worker gang size: PGRUST_PARALLEL_COPY_DOP, default = the runtime pool's
/// execution width, clamped to the external-lane budget.
fn dop(rt: &runtime::Runtime) -> i32 {
    static N: OnceLock<Option<u64>> = OnceLock::new();
    let req = *N.get_or_init(|| {
        std::env::var("PGRUST_PARALLEL_COPY_DOP").ok().and_then(|v| v.trim().parse().ok())
    });
    let d = req.unwrap_or(rt.config().workers as u64);
    d.clamp(1, (runtime::MAX_EXTERNAL_LANES as u64).min(32)) as i32
}

/// In-flight chunk window (published − committed): bounds leader read-ahead
/// memory (a chunk is ~1 RG of raw input, ~50 MB on ClickBench rows).
fn window(k: i32) -> u64 {
    static N: OnceLock<Option<u64>> = OnceLock::new();
    let req = *N.get_or_init(|| {
        std::env::var("PGRUST_PARALLEL_COPY_WINDOW").ok().and_then(|v| v.trim().parse().ok())
    });
    req.unwrap_or((2 * k as u64) + 4).max(2)
}

/// Sort-merge encode threads (PGRUST_PARALLEL_COPY_SORT_ENCODERS,
/// default = the COPY dop).
fn sort_encoders(rt: &runtime::Runtime) -> usize {
    static N: OnceLock<Option<usize>> = OnceLock::new();
    let req = *N.get_or_init(|| {
        std::env::var("PGRUST_PARALLEL_COPY_SORT_ENCODERS")
            .ok()
            .and_then(|v| v.trim().parse().ok())
    });
    req.unwrap_or(dop(rt) as usize).clamp(1, 32)
}

/// Segmentator read-block bytes.
const READ_BLOCK: usize = 4 << 20;

/// File-source engagement floor (bytes): tiny loads keep the serial path
/// (frontend streams engage regardless — their size is unknowable).
fn file_floor() -> u64 {
    static N: OnceLock<u64> = OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("PGRUST_PARALLEL_COPY_MIN_BYTES")
            .ok()
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(4 << 20)
    })
}

// ---------------------------------------------------------------------------
// Chunk plumbing: descriptors over refcounted read buffers.
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub(crate) struct ChunkSeg {
    pub(crate) buf: Arc<Vec<u8>>,
    pub(crate) start: usize,
    pub(crate) end: usize,
}

/// Worker-side sequential reader over a chunk's segments (CopySrc::Chunk).
pub(crate) struct ChunkCursor {
    segs: Vec<ChunkSeg>,
    seg: usize,
    off: usize,
}

impl ChunkCursor {
    pub(crate) fn new(segs: Vec<ChunkSeg>) -> ChunkCursor {
        let off = segs.first().map(|s| s.start).unwrap_or(0);
        ChunkCursor { segs, seg: 0, off }
    }

    pub(crate) fn read(&mut self, dst: &mut [u8]) -> usize {
        let mut filled = 0usize;
        while filled < dst.len() && self.seg < self.segs.len() {
            let s = &self.segs[self.seg];
            let avail = s.end - self.off;
            if avail == 0 {
                self.seg += 1;
                self.off = self.segs.get(self.seg).map(|s| s.start).unwrap_or(0);
                continue;
            }
            let n = avail.min(dst.len() - filled);
            dst[filled..filled + n].copy_from_slice(&s.buf[self.off..self.off + n]);
            self.off += n;
            filled += n;
        }
        filled
    }
}

struct ChunkDesc {
    /// 1-based line number of the chunk's first row (workers preset
    /// cur_lineno so error contexts carry exact input line numbers).
    first_lineno: u64,
    segs: Vec<ChunkSeg>,
}

// ---------------------------------------------------------------------------
// The segmentator: cheap row-boundary scan (TEXT format).
// ---------------------------------------------------------------------------
//
// Rules (mirror copy_read_line_text, non-CSV):
//  * a terminator byte is a row boundary iff the run of consecutive
//    backslashes immediately before it has EVEN length (a backslash consumes
//    the next byte; consumption chains only inside contiguous runs);
//  * EOL style is decided by the FIRST unescaped terminator (Nl / Cr /
//    Crnl); later inconsistent terminators are NOT boundaries here — the
//    owning worker raises the exact serial error (literal newline/carriage
//    return) at the exact line;
//  * a line starting with `\.` ends the input: the marker LINE goes into
//    the final chunk (the worker replays every marker validation error);
//    bytes past it are never segmented (frontend streams drain protocol-
//    level, files stop early — serial behavior).

#[derive(Clone, Copy, PartialEq)]
enum SegEol {
    Unknown,
    Nl,
    Cr,
    Crnl,
}

struct Segmentator {
    eol: SegEol,
    rows_per_chunk: u32,
    // Current chunk accumulation.
    segs: Vec<ChunkSeg>,
    rows: u32,
    first_lineno: u64,
    rows_total: u64,
    // Cross-buffer carry state.
    /// Backslash run length ending at the previous buffer's last byte.
    trailing_bs: u32,
    /// First up-to-2 bytes of the in-progress line (for `\.` detection when
    /// the line started in an earlier buffer). len = bytes captured so far.
    line_head: [u8; 2],
    line_head_len: u8,
    /// Bytes seen in the in-progress line (caps line_head capture).
    line_len: u64,
    /// Previous buffer ended in '\r' with EOL Unknown pending the
    /// lookahead byte (Cr vs Crnl decision).
    pending_cr: bool,
    /// Decided-Crnl mode: previous buffer's last byte was an UNESCAPED
    /// '\r' — a '\n' at the next buffer's start pairs into a boundary.
    prev_ended_cr: bool,
    /// Escape state for the detect phase (odd backslash run in progress).
    detect_esc: bool,
    /// End-of-copy marker seen: stop segmenting.
    eoc: bool,
}

impl Segmentator {
    fn new(rows_per_chunk: u32) -> Segmentator {
        Segmentator {
            eol: SegEol::Unknown,
            rows_per_chunk,
            segs: Vec::new(),
            rows: 0,
            first_lineno: 1,
            rows_total: 0,
            trailing_bs: 0,
            line_head: [0; 2],
            line_head_len: 0,
            line_len: 0,
            pending_cr: false,
            prev_ended_cr: false,
            detect_esc: false,
            eoc: false,
        }
    }

    fn eol_type(&self) -> EolType {
        match self.eol {
            SegEol::Unknown => EolType::Unknown,
            SegEol::Nl => EolType::Nl,
            SegEol::Cr => EolType::Cr,
            SegEol::Crnl => EolType::Crnl,
        }
    }

    /// Backslash-run parity before `pos` (run may extend into the previous
    /// buffer iff it reaches offset `base`).
    fn bs_parity_even(&self, data: &[u8], base: usize, pos: usize) -> bool {
        let mut k = pos;
        while k > base && data[k - 1] == b'\\' {
            k -= 1;
        }
        let mut run = (pos - k) as u32;
        if k == base {
            run += self.trailing_bs;
        }
        run % 2 == 0
    }

    /// The in-progress line's first two bytes, given the line started at
    /// `start` in `data` (or earlier — then line_head carries them).
    fn line_first2(&self, data: &[u8], start: usize, upto: usize) -> [Option<u8>; 2] {
        let mut out = [None, None];
        let mut n = 0usize;
        for i in 0..self.line_head_len as usize {
            out[n] = Some(self.line_head[i]);
            n += 1;
        }
        let mut i = start;
        while n < 2 && i < upto {
            out[n] = Some(data[i]);
            n += 1;
            i += 1;
        }
        out
    }

    /// Feed one read buffer (`data[..len]` of `buf`). Emits completed chunk
    /// descriptors into `out`. Returns the number of bytes CONSUMED — less
    /// than `len` only when the end-of-copy marker line ended inside the
    /// buffer (the rest of the stream is not COPY data).
    fn feed(&mut self, buf: &Arc<Vec<u8>>, len: usize, out: &mut Vec<ChunkDesc>) -> usize {
        assert!(!self.eoc, "feed after the end-of-copy marker");
        let data = &buf[..len];
        // Start of the not-yet-chunked region of THIS buffer.
        let mut chunk_start = 0usize;
        // Start of the in-progress line within this buffer (line_head covers
        // bytes from earlier buffers).
        let mut line_start = 0usize;
        let mut i = 0usize;

        // Resolve a pending CR lookahead from the previous buffer (EOL was
        // Unknown; the \r at the edge decides Cr vs Crnl by this byte).
        if self.pending_cr {
            self.pending_cr = false;
            self.eol = if data.first() == Some(&b'\n') { SegEol::Crnl } else { SegEol::Cr };
            if self.eol == SegEol::Crnl {
                i = 1;
            }
            // The \r (+\n) terminated a row.
            if self.row_boundary(buf, data, &mut chunk_start, i, &mut line_start, out) {
                return i;
            }
        } else if self.prev_ended_cr && self.eol == SegEol::Crnl && data.first() == Some(&b'\n')
        {
            // Decided-Crnl mode, \r|\n split across the buffer edge.
            self.prev_ended_cr = false;
            i = 1;
            if self.row_boundary(buf, data, &mut chunk_start, i, &mut line_start, out) {
                return i;
            }
        }
        self.prev_ended_cr = false;

        while i < len {
            match self.eol {
                SegEol::Unknown => {
                    // Detect phase: scalar scan honoring escapes until the
                    // first unescaped terminator.
                    let b = data[i];
                    if self.detect_esc {
                        self.detect_esc = false;
                        i += 1;
                        continue;
                    }
                    match b {
                        b'\\' => {
                            self.detect_esc = true;
                            i += 1;
                        }
                        b'\n' => {
                            self.eol = SegEol::Nl;
                            i += 1;
                            if self.row_boundary(buf, data, &mut chunk_start, i, &mut line_start, out) {
                                return i;
                            }
                        }
                        b'\r' => {
                            if i + 1 < len {
                                self.eol =
                                    if data[i + 1] == b'\n' { SegEol::Crnl } else { SegEol::Cr };
                                i += if self.eol == SegEol::Crnl { 2 } else { 1 };
                                if self.row_boundary(buf, data, &mut chunk_start, i, &mut line_start, out) {
                                    return i;
                                }
                            } else {
                                // Buffer edge: defer the Cr/Crnl decision.
                                self.pending_cr = true;
                                i += 1;
                            }
                        }
                        _ => i += 1,
                    }
                }
                SegEol::Nl | SegEol::Crnl => {
                    let Some(j) = memchr::memchr(b'\n', &data[i..len]) else {
                        break;
                    };
                    let pos = i + j;
                    i = pos + 1;
                    let boundary = match self.eol {
                        SegEol::Nl => self.bs_parity_even(data, 0, pos),
                        SegEol::Crnl => {
                            // \r\n pair with even parity before the \r. A
                            // lone \n (or an escaped \r) is data here — the
                            // owning worker errors serial-exactly.
                            if pos == 0 {
                                // \n at buffer start: the \r (if any) was the
                                // previous buffer's last byte — handled by
                                // pending_cr above, so this \n is bare.
                                false
                            } else {
                                data[pos - 1] == b'\r' && self.bs_parity_even(data, 0, pos - 1)
                            }
                        }
                        _ => unreachable!(),
                    };
                    if boundary
                        && self.row_boundary(buf, data, &mut chunk_start, i, &mut line_start, out)
                    {
                        return i;
                    }
                }
                SegEol::Cr => {
                    let Some(j) = memchr::memchr(b'\r', &data[i..len]) else {
                        break;
                    };
                    let pos = i + j;
                    i = pos + 1;
                    if self.bs_parity_even(data, 0, pos)
                        && self.row_boundary(buf, data, &mut chunk_start, i, &mut line_start, out)
                    {
                        return i;
                    }
                }
            }
        }

        // Buffer exhausted: carry the tail into the current chunk + state.
        if chunk_start < len {
            self.segs.push(ChunkSeg { buf: Arc::clone(buf), start: chunk_start, end: len });
        }
        // Trailing backslash run (for parity across the edge). The detect
        // phase tracks escapes itself; boundary modes use run parity.
        let mut k = len;
        while k > 0 && data[k - 1] == b'\\' {
            k -= 1;
        }
        let run = (len - k) as u32;
        let carry_in = if k == 0 { self.trailing_bs } else { 0 };
        // Decided-Crnl mode: an unescaped \r as the buffer's last byte may
        // pair with a \n at the next buffer's start.
        self.prev_ended_cr = self.eol == SegEol::Crnl
            && len > 0
            && data[len - 1] == b'\r'
            && self.bs_parity_even(data, 0, len - 1);
        self.trailing_bs = carry_in + run;
        // Line-head capture for a line spilling past the buffer.
        let mut idx = line_start;
        while self.line_head_len < 2 && idx < len {
            self.line_head[self.line_head_len as usize] = data[idx];
            self.line_head_len += 1;
            idx += 1;
        }
        self.line_len += (len - line_start) as u64;
        len
    }

    /// A row boundary just closed at `end` (exclusive, includes its EOL
    /// bytes). Counts the row, checks the `\.` marker, cuts a chunk at
    /// rows_per_chunk. Returns true ⇔ the end-of-copy marker line closed
    /// (caller stops consuming).
    fn row_boundary(
        &mut self,
        buf: &Arc<Vec<u8>>,
        data: &[u8],
        chunk_start: &mut usize,
        end: usize,
        line_start: &mut usize,
        out: &mut Vec<ChunkDesc>,
    ) -> bool {
        let first2 = self.line_first2(data, *line_start, end);
        let is_eoc = first2[0] == Some(b'\\') && first2[1] == Some(b'.');
        self.rows += 1;
        self.rows_total += 1;
        self.line_head_len = 0;
        self.line_len = 0;
        *line_start = end;
        self.trailing_bs = 0;
        if is_eoc {
            // The marker line itself goes to the final chunk; the worker
            // replays serial marker validation (aloneness, EOL style).
            self.eoc = true;
            if *chunk_start < end {
                self.segs.push(ChunkSeg { buf: Arc::clone(buf), start: *chunk_start, end });
            }
            *chunk_start = end;
            self.cut_chunk(out);
            return true;
        }
        if self.rows >= self.rows_per_chunk {
            self.segs.push(ChunkSeg { buf: Arc::clone(buf), start: *chunk_start, end });
            *chunk_start = end;
            self.cut_chunk(out);
        }
        false
    }

    fn cut_chunk(&mut self, out: &mut Vec<ChunkDesc>) {
        if self.segs.is_empty() {
            self.rows = 0;
            self.first_lineno = self.rows_total + 1;
            return;
        }
        out.push(ChunkDesc {
            first_lineno: self.first_lineno,
            segs: std::mem::take(&mut self.segs),
        });
        self.rows = 0;
        self.first_lineno = self.rows_total + 1;
    }

    /// Stream EOF: cut whatever remains (a trailing unterminated line is a
    /// row — serial parses it too).
    fn finish(&mut self, out: &mut Vec<ChunkDesc>) {
        self.cut_chunk(out);
    }
}

// ---------------------------------------------------------------------------
// Shared statement state (the parallel context's private payload AND the
// task set's work body — the vacuum-morsels shape).
// ---------------------------------------------------------------------------

pub(crate) struct ParCopyShared {
    rt: &'static Arc<runtime::Runtime>,
    rg: OnceLock<runtime::WeakRgHandle>,
    source: Arc<runtime::StreamSource>,
    relid: Oid,
    relname: String,
    // Parse plan.
    delim: u8,
    null_print: String,
    freeze: bool,
    file_encoding: i32,
    /// EOL preset per chunk index: chunk 0 detects itself (Unknown), later
    /// chunks inherit the segmentator's decision. Encoded as the SegEol the
    /// leader publishes BEFORE the first chunk past the decision.
    eol: Mutex<EolPre>,
    attnumlist: Vec<i16>,
    // Encode plan.
    plan: Arc<cbstore::ParallelIngestPlan>,
    // Chunk registry: leader inserts BEFORE publishing the watermark past
    // the index; the claiming worker removes.
    chunks: Mutex<HashMap<u64, ChunkDesc>>,
    // Completed encodes, keyed by chunk index; the leader commits in order.
    done: Mutex<BTreeMap<u64, Option<cbstore::EncodedRg>>>,
    // First-error-in-input-order protocol: chunk-indexed error records;
    // claims for chunks ABOVE the floor drain (chunks below still parse, so
    // an earlier error can still surface and win).
    errors: Mutex<BTreeMap<u64, Box<PgError>>>,
    error_floor: AtomicU64,
    /// Hard failure (worker panic / non-data error): abort the RG now.
    failed_hard: AtomicBool,
    hard_error: Mutex<Option<Box<PgError>>>,
    refused: AtomicUsize,
    started: AtomicUsize,
    leader_proc: types_core::ProcNumber,
    /// load-r2 L3-1 sort mode (parallel load sort): Some = workers spill
    /// sorted (key,row) runs instead of encoding RGs; the leader merges
    /// after the RG completes. None = the landed encode pipeline verbatim.
    sort: Option<ParCopySort>,
    /// Registered run files (paths pushed BEFORE their spill starts so
    /// every file is cleanup-tracked); leader takes them for the merge.
    sort_runs: Mutex<Vec<std::path::PathBuf>>,
    sort_run_seq: AtomicU64,
}

/// Sort-mode plan: the presort key spec in memcmp-key terms.
struct ParCopySort {
    keys: Vec<(u16, cbstore::sortkey::CbSortKeyKind)>,
    key_w: usize,
    budget: usize,
    /// Statement-unique run-file name component.
    nonce: u64,
}

#[derive(Clone, Copy)]
struct EolPre {
    /// EolType for chunks >= 1 (chunk 0 always starts Unknown, exactly like
    /// serial's first line).
    later: EolType,
}

impl ParCopyShared {
    fn record_error(&self, chunk: u64, e: Box<PgError>) {
        self.errors.lock().unwrap_or_else(|p| p.into_inner()).insert(chunk, e);
        self.error_floor.fetch_min(chunk, Ordering::SeqCst);
        self.wake_leader();
    }

    fn fail_hard(&self, e: Box<PgError>) {
        {
            let mut g = self.hard_error.lock().unwrap_or_else(|p| p.into_inner());
            if g.is_none() {
                *g = Some(e);
            }
        }
        self.failed_hard.store(true, Ordering::SeqCst);
        if let Some(rg) = self.rg.get().and_then(|w| w.upgrade()) {
            rg.abort();
        }
        self.wake_leader();
    }

    fn wake_leader(&self) {
        latch::SetLatch(types_storage::latch::LatchHandle::proc(self.leader_proc));
    }

    fn take_min_error(&self) -> Option<Box<PgError>> {
        let mut g = self.errors.lock().unwrap_or_else(|p| p.into_inner());
        let k = *g.keys().next()?;
        g.remove(&k)
    }

    fn take_hard_error(&self) -> Option<Box<PgError>> {
        self.hard_error.lock().unwrap_or_else(|p| p.into_inner()).take()
    }
}

impl runtime::TaskSetWork for ParCopyShared {
    fn run_morsel(&self, worker: usize, range: runtime::MorselRange) {
        let r = catch_unwind(AssertUnwindSafe(|| self.morsel_body(worker, range)));
        match r {
            Ok(Ok(())) => {}
            Ok(Err(e)) => self.fail_hard(e),
            Err(_panic) => self.fail_hard(
                PgError::new(ERROR, "parallel COPY worker panicked in a chunk").into(),
            ),
        }
    }

    fn finalize(&self) {
        // Results live in the done/errors maps; the LEADER commits/raises.
    }
}

// ---------------------------------------------------------------------------
// Worker side.
// ---------------------------------------------------------------------------

/// Per-helper parse/encode context, on the entry-task frame around
/// drive_pinned; run_morsel reaches it through the thread-local pointer
/// (this thread is the only driver of its lane) — the vacuum-morsels shape.
struct ParCopyWorkerCx<'a, 'mcx> {
    mcx: Mcx<'mcx>,
    rel: &'a Relation<'mcx>,
    st: CopyFromState<'mcx, 'a>,
    slot: types_slot::SlotData<'mcx>,
    check_exprs: Option<PgVec<'mcx, nodemodifytable::CheckExpr<'mcx>>>,
    virtual_nn: Option<PgVec<'mcx, nodemodifytable::VirtualNnExpr<'mcx>>>,
    inserted_cols: types_nodes::Bitmapset<'mcx>,
    /// Per-row datum arena, reset after every appended row.
    row_cx: MemoryContext,
    /// load-r2 L3-1 sort mode: this worker's (key,row) batch + codec.
    sort_state: Option<WorkerSortState>,
}

struct WorkerSortState {
    batch: cbstore::loadsort::SortBatch,
    codec: cbstore::loadsort::RowCodec,
    keybuf: Vec<u8>,
    rowbuf: Vec<u8>,
}

thread_local! {
    static WORKER_CX: std::cell::Cell<*mut ParCopyWorkerCx<'static, 'static>> =
        const { std::cell::Cell::new(std::ptr::null_mut()) };
}

impl ParCopyShared {
    fn morsel_body(&self, _worker: usize, range: runtime::MorselRange) -> PgResult<()> {
        let p = WORKER_CX.with(|c| c.get());
        if p.is_null() {
            return Err(PgError::new(ERROR, "parallel COPY chunk without a bound worker").into());
        }
        // SAFETY: set by THIS thread's entry frame around drive_pinned; the
        // frame outlives the drive, and run_morsel only executes on the
        // claiming thread.
        let wcx: &mut ParCopyWorkerCx<'_, '_> = unsafe { &mut *p };
        for g in range {
            self.run_chunk(wcx, g)?;
        }
        Ok(())
    }

    fn run_chunk(&self, wcx: &mut ParCopyWorkerCx<'_, '_>, g: u64) -> PgResult<()> {
        let chunk = {
            let mut m = self.chunks.lock().unwrap_or_else(|p| p.into_inner());
            m.remove(&g)
        };
        let Some(chunk) = chunk else {
            return Err(PgError::new(ERROR, "parallel COPY chunk claimed before publish").into());
        };
        // Drain claims past the lowest erroring chunk (chunks BELOW it keep
        // parsing so the first error in input order wins).
        if g > self.error_floor.load(Ordering::SeqCst) {
            return Ok(());
        }
        let eol = if g == 0 {
            EolType::Unknown
        } else {
            self.eol.lock().unwrap_or_else(|p| p.into_inner()).later
        };
        match self.parse_encode_chunk(wcx, chunk, eol) {
            Ok(enc) => {
                self.done.lock().unwrap_or_else(|p| p.into_inner()).insert(g, enc);
                self.wake_leader();
            }
            Err(e) => {
                // Data-shaped error: context attached with the worker's
                // exact line/column; recorded for the leader's ordered
                // re-raise. NOT a hard failure — earlier chunks finish.
                self.record_error(g, copy_from_error_context(&wcx.st, e));
            }
        }
        Ok(())
    }

    fn parse_encode_chunk(
        &self,
        wcx: &mut ParCopyWorkerCx<'_, '_>,
        chunk: ChunkDesc,
        eol: EolType,
    ) -> PgResult<Option<cbstore::EncodedRg>> {
        {
            let st = &mut wcx.st;
            st.src = CopySrc::Chunk(ChunkCursor::new(chunk.segs));
            st.raw_buf_index = 0;
            st.raw_buf_len = 0;
            st.raw_reached_eof = false;
            st.input_reached_eof = false;
            st.input_reached_error = false;
            st.input_buf_index = 0;
            st.input_buf_len = 0;
            st.line_buf.clear();
            st.line_buf_valid = false;
            st.eol_type = eol;
            st.cur_lineno = chunk.first_lineno - 1;
            st.cur_attidx = None;
            st.cur_attval_off = None;
        }

        let mut enc = if self.sort.is_none() {
            Some(cbstore::RgChunkEncoder::new(Arc::clone(&self.plan)))
        } else {
            None
        };
        let mut since_cfi = 0u32;
        loop {
            since_cfi += 1;
            if since_cfi >= 4096 {
                since_cfi = 0;
                postgres_seams::check_for_interrupts::call()?;
            }
            wcx.row_cx.reset();
            exectuples::exec_clear_tuple(&mut wcx.slot, wcx.mcx);
            // SAFETY (lifetime erasure): per-row datums land in row_cx and
            // are COPIED into the chunk encoder before the next reset;
            // nothing retains them past the row (the serial path's
            // statement-mcx contract, tightened to row scope).
            let row_mcx: Mcx<'_> = unsafe { core::mem::transmute(wcx.row_cx.mcx()) };
            {
                let base = wcx.slot.base_mut();
                if !wcx.st.next_copy_from(row_mcx, &mut base.tts_values, &mut base.tts_isnull)? {
                    break;
                }
            }
            exectuples::exec_store_virtual_tuple(&mut wcx.slot);
            wcx.slot.base_mut().tts_tableOid = self.relid;
            // The serial path's ExecConstraints, worker-side (identical
            // errors by construction — same function, same slot shape).
            nodemodifytable::exec_constraints(
                wcx.mcx,
                &mut wcx.check_exprs,
                &mut wcx.virtual_nn,
                wcx.rel,
                &mut wcx.slot,
                None,
                Some(&wcx.inserted_cols),
            )?;
            let base = wcx.slot.base();
            if let Some(enc) = enc.as_mut() {
                enc.append_row(&base.tts_values, &base.tts_isnull)?;
            } else {
                // Sort mode: (memcmp key, row image) into this worker's
                // batch; spill a sorted run at the budget. NULLs refuse
                // HERE with the worker's exact line context (serial cites
                // its buffered-flush line — the recorded parallel-copy
                // divergence rule 2 class; message/sqlstate identical).
                let sort = self.sort.as_ref().unwrap();
                let st = wcx
                    .sort_state
                    .as_mut()
                    .expect("sort mode without a worker sort state");
                let ncols = self.plan.coltypes.len();
                if base.tts_isnull[..ncols].iter().any(|&n| n) {
                    return Err(Box::new(
                        PgError::error("cbstore does not support NULL values".to_string())
                            .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
                    ));
                }
                st.keybuf.clear();
                cbstore::sortkey::encode_sort_key(&sort.keys, &base.tts_values, &mut st.keybuf);
                st.rowbuf.clear();
                st.codec.serialize_row(&base.tts_values, &mut st.rowbuf)?;
                st.batch.push(&st.keybuf, &st.rowbuf);
                if st.batch.bytes() >= sort.budget {
                    self.spill_worker_batch(st)?;
                }
            }
        }
        let Some(enc) = enc else { return Ok(None) };
        if enc.rows() == 0 {
            // A final chunk holding only the end-of-copy marker line (or an
            // empty stream tail): nothing to encode.
            return Ok(None);
        }
        Ok(Some(enc.seal()))
    }

    /// Sort + spill the worker's current batch as one run file. The path is
    /// registered BEFORE the write so teardown can always unlink it.
    fn spill_worker_batch(&self, st: &mut WorkerSortState) -> PgResult<()> {
        if st.batch.is_empty() {
            return Ok(());
        }
        let sort = self.sort.as_ref().expect("spill without sort mode");
        let dir = std::path::Path::new("base/pgsql_tmp");
        std::fs::create_dir_all(dir).map_err(|e| {
            Box::new(PgError::error(format!("parallel load-sort temp dir: {e}")))
        })?;
        let seq = self.sort_run_seq.fetch_add(1, Ordering::SeqCst);
        let path = dir.join(format!(
            "pgsql_tmp{}.parcopysort.{:x}.{}.run",
            std::process::id(),
            sort.nonce,
            seq
        ));
        self.sort_runs.lock().unwrap_or_else(|p| p.into_inner()).push(path.clone());
        st.batch.sort();
        st.batch.spill_run(&path)?;
        ptrace(&format!("sort run spilled seq={seq}"));
        Ok(())
    }
}

/// The launched entry task (vacuum-morsels ceremony: the substrate already
/// connected the helper to the leader's database, restored leader state,
/// and entered parallel mode).
fn parallel_copy_worker_main(pshared: &parallel::ParallelShared) -> PgResult<()> {
    let Some(private) = pshared.private() else { return Ok(()) };
    let Ok(shared) = private.downcast::<ParCopyShared>() else { return Ok(()) };

    let r = catch_unwind(AssertUnwindSafe(|| worker_drive(&shared)));
    let outcome = match r {
        Ok(o) => o,
        Err(unwind) => {
            shared.fail_hard(PgError::new(ERROR, "parallel COPY helper panicked").into());
            if parallel::standing::is_exit_unwind(&*unwind) {
                latch::SetLatch(types_storage::latch::LatchHandle::proc(
                    pshared.parallel_leader_proc_number,
                ));
                std::panic::resume_unwind(unwind);
            }
            Err(Box::new(PgError::new(ERROR, "parallel COPY worker failed (see leader error)")))
        }
    };
    latch::SetLatch(types_storage::latch::LatchHandle::proc(
        pshared.parallel_leader_proc_number,
    ));
    outcome
}

fn worker_drive(shared: &Arc<ParCopyShared>) -> PgResult<()> {
    let Some(rg) = shared.rg.get().and_then(|w| w.upgrade()) else {
        shared.refused.fetch_add(1, Ordering::SeqCst);
        return Ok(());
    };
    let Some(lane) = shared.rt.acquire_external_lane() else {
        shared.refused.fetch_add(1, Ordering::SeqCst);
        return Ok(());
    };
    let mut lane_local = lane.local();

    let ctx = MemoryContext::new("parallel COPY worker");
    let mcx = ctx.mcx();
    let rel = match table::table_open(mcx, shared.relid, types_rel::lock::RowExclusiveLock) {
        Ok(rel) => rel,
        Err(e) => {
            shared.fail_hard(e);
            if rg.try_outcome().is_none() {
                rg.abort();
                let _ = shared.rt.drive_pinned(&mut lane_local, &rg);
            }
            return Ok(());
        }
    };

    let build = (|| -> PgResult<ParCopyWorkerCx<'_, '_>> {
        // Input-function resolution, BeginCopyFrom's loop verbatim.
        let mut in_functions: PgVec<'_, FmgrInfo> = PgVec::new_in(mcx);
        let mut typioparams: PgVec<'_, Oid> = PgVec::new_in(mcx);
        let mut atttypmods: PgVec<'_, i32> = PgVec::new_in(mcx);
        let mut attnames: PgVec<'_, types_tuple::NameData> = PgVec::new_in(mcx);
        let tup_desc = &rel.rd_att;
        let num_phys_attrs = tup_desc.natts as usize;
        let mut attnumlist: PgVec<'_, i16> = PgVec::new_in(mcx);
        for &a in &shared.attnumlist {
            attnumlist.push(a);
        }
        for &attnum in attnumlist.iter() {
            let att = tup_desc.attr(attnum as usize - 1);
            let (func_oid, typioparam) = lsyscache::typ::getTypeInputInfo(att.atttypid)?;
            in_functions.push(fmgr_core::fmgr_info(func_oid)?);
            typioparams.push(typioparam);
            atttypmods.push(att.atttypmod);
        }
        let mut defexprs: PgVec<'_, Option<mcx::PgBox<'_, execexpr::ExprState<'_>>>> =
            PgVec::new_in(mcx);
        for i in 0..num_phys_attrs {
            attnames.push(tup_desc.attr(i).attname);
            defexprs.push(None);
        }
        let inserted_cols = {
            const FLIHAN: i32 = types_tuple::htup::FirstLowInvalidHeapAttributeNumber;
            let mut b = types_nodes::Bitmapset::empty();
            for &a in attnumlist.iter() {
                b.add_member(mcx, a as i32 - FLIHAN)?;
            }
            b
        };
        let max_fields = attnumlist.len();
        let st = CopyFromState {
            opts: CopyFormatOptions {
                file_encoding: shared.file_encoding,
                binary: false,
                csv_mode: false,
                freeze: shared.freeze,
                delim: shared.delim,
                quote: b'"',
                escape: b'"',
                null_print: &shared.null_print,
                default_print: None,
                header_line: CopyHeaderChoice::False,
                force_quote: None,
                force_quote_all: false,
                force_notnull: None,
                force_notnull_all: false,
                force_null: None,
                force_null_all: false,
                convert_selectively: false,
                convert_select: None,
                on_error: CopyOnErrorChoice::Stop,
                log_verbosity: crate::CopyLogVerbosityChoice::Default,
                reject_limit: 0,
            },
            src: CopySrc::Chunk(ChunkCursor::new(Vec::new())),
            raw_buf: vec_from_elem_in(mcx, 0u8, crate::fromparse::RAW_BUF_SIZE + 1),
            raw_buf_index: 0,
            raw_buf_len: 0,
            raw_reached_eof: false,
            input_reached_eof: false,
            input_reached_error: false,
            input_buf: None,
            input_buf_index: 0,
            input_buf_len: 0,
            line_buf: PgVec::new_in(mcx),
            line_buf_valid: false,
            attribute_buf: PgVec::new_in(mcx),
            binary_attr_buf: stringinfo::StringInfo::new_in(mcx)?,
            raw_fields: PgVec::new_in(mcx),
            max_fields,
            eol_type: EolType::Unknown,
            cur_lineno: 0,
            cur_attidx: None,
            cur_attval_off: None,
            file_encoding: shared.file_encoding,
            need_transcoding: false,
            conversion_proc: 0,
            convertcx: MemoryContext::new("parallel COPY convert (unused)"),
            attnumlist,
            in_functions,
            typioparams,
            atttypmods,
            attnames,
            force_notnull_flags: vec_from_elem_in(mcx, false, num_phys_attrs),
            force_null_flags: vec_from_elem_in(mcx, false, num_phys_attrs),
            convert_select_flags: None,
            defexprs,
            defmap: PgVec::new_in(mcx),
            defaults: vec_from_elem_in(mcx, false, num_phys_attrs),
            where_clause: types_nodes::NodeList::nil(),
            relname: shared.relname.clone(),
            escontext: None,
            num_errors: 0,
            bytes_processed: 0,
            volatile_defexprs: false,
        };
        let slot = tableam::table_slot_create(mcx, &rel)?;
        let sort_state = shared.sort.as_ref().map(|sp| WorkerSortState {
            batch: cbstore::loadsort::SortBatch::new(sp.key_w),
            codec: cbstore::loadsort::RowCodec::new(shared.plan.coltypes.clone()),
            keybuf: Vec::with_capacity(sp.key_w),
            rowbuf: Vec::new(),
        });
        Ok(ParCopyWorkerCx {
            mcx,
            rel: &rel,
            st,
            slot,
            check_exprs: None,
            virtual_nn: None,
            inserted_cols,
            row_cx: MemoryContext::new_bump("ParallelCopyRowEval"),
            sort_state,
        })
    })();
    let mut wcx = match build {
        Ok(w) => w,
        Err(e) => {
            shared.fail_hard(e);
            if rg.try_outcome().is_none() {
                rg.abort();
                let _ = shared.rt.drive_pinned(&mut lane_local, &rg);
            }
            let _ = table::table_close(rel, types_rel::lock::RowExclusiveLock);
            return Ok(());
        }
    };
    shared.started.fetch_add(1, Ordering::SeqCst);

    // Publish the worker cx for run_morsel (this thread only), drive, clear.
    // SAFETY (lifetime erasure): wcx outlives the drive on this frame; the
    // pointer is cleared before wcx drops.
    WORKER_CX.with(|c| {
        c.set(unsafe {
            core::mem::transmute::<*mut ParCopyWorkerCx<'_, '_>, *mut ParCopyWorkerCx<'static, 'static>>(
                &mut wcx as *mut ParCopyWorkerCx<'_, '_>,
            )
        })
    });
    let _outcome = shared.rt.drive_pinned(&mut lane_local, &rg);
    WORKER_CX.with(|c| c.set(std::ptr::null_mut()));

    // Sort mode: flush this worker's final batch as its last run. Skipped
    // when the statement is already failing (hard error or any recorded
    // data error — the COPY raises regardless; the leader never merges).
    if let Some(st) = wcx.sort_state.as_mut() {
        if !shared.failed_hard.load(Ordering::SeqCst)
            && shared.error_floor.load(Ordering::SeqCst) == u64::MAX
        {
            if let Err(e) = shared.spill_worker_batch(st) {
                shared.fail_hard(e);
            }
        }
    }

    drop(wcx);
    table::table_close(rel, types_rel::lock::RowExclusiveLock)?;

    if shared.failed_hard.load(Ordering::SeqCst) {
        // A recorded hard error (possibly a sibling's): abort the worker
        // transaction so resowner releases residue; the leader rethrows the
        // recorded error, never this message.
        return Err(Box::new(PgError::new(
            ERROR,
            "parallel COPY worker failed (see leader error)",
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Leader: admission + the ceremony (segment/publish/commit loop).
// ---------------------------------------------------------------------------

/// Sort-mode phase 2: k-way merge every spilled run, encode in parallel,
/// commit in order (load-r2 L3-1 step d).
///
/// The merge thread streams rows in global memcmp-key order (== the serial
/// presort order — no key ties on admitted int-class keys) into exactly
/// RG_ROWS-row batches; a scoped encode pool (RgChunkEncoder — the landed
/// worker-encode machinery, byte-proven vs serial by its seam oracle)
/// seals them; this thread commits EncodedRgs in batch order through
/// commit_encoded_rg. Backpressure: the bounded work channel caps
/// in-flight batches at ~encoders+1. Returns the merged row count.
fn merge_sorted_runs(
    writer: &mut cbstore::CbWriter,
    shared: &Arc<ParCopyShared>,
) -> PgResult<u64> {
    let sort = shared.sort.as_ref().expect("merge without sort mode");
    let paths =
        std::mem::take(&mut *shared.sort_runs.lock().unwrap_or_else(|p| p.into_inner()));
    let mut merge = cbstore::loadsort::RunMerge::open(&paths, sort.key_w)?;
    // Eager unlink: the open fds keep the data; a crash from here leaves
    // no orphan files.
    for p in &paths {
        let _ = std::fs::remove_file(p);
    }
    let nenc = sort_encoders(shared.rt);
    ptrace(&format!("sort merge over {} runs encoders={nenc}", paths.len()));

    const RG: usize = cbstore::format::RG_ROWS;
    struct Batch {
        idx: u64,
        arena: Vec<u8>,
        lens: Vec<u32>,
    }
    let (work_tx, work_rx) = std::sync::mpsc::sync_channel::<Batch>(nenc + 1);
    let work_rx = Mutex::new(work_rx);
    let (done_tx, done_rx) =
        std::sync::mpsc::channel::<(u64, PgResult<cbstore::EncodedRg>)>();

    let mut n_rows = 0u64;
    let mut first_err: Option<Box<PgError>> = None;
    let mut committed = 0u64;

    std::thread::scope(|scope| {
        for _ in 0..nenc {
            let rx = &work_rx;
            let tx = done_tx.clone();
            let plan = Arc::clone(&shared.plan);
            scope.spawn(move || {
                let codec = cbstore::loadsort::RowCodec::new(plan.coltypes.clone());
                let ncols = plan.coltypes.len();
                let mut arena: Vec<u8> = Vec::new();
                let mut values = vec![::datum::Datum::null(); ncols];
                let isnull = vec![false; ncols];
                loop {
                    let b = {
                        let g = rx.lock().unwrap_or_else(|p| p.into_inner());
                        g.recv()
                    };
                    let Ok(b) = b else { break };
                    let r = catch_unwind(AssertUnwindSafe(
                        || -> PgResult<cbstore::EncodedRg> {
                            let mut enc =
                                cbstore::RgChunkEncoder::new(Arc::clone(&plan));
                            let mut off = 0usize;
                            for &l in &b.lens {
                                let l = l as usize;
                                arena.clear();
                                codec.deserialize_row(
                                    &b.arena[off..off + l],
                                    &mut arena,
                                    &mut values,
                                )?;
                                enc.append_row(&values, &isnull)?;
                                off += l;
                            }
                            Ok(enc.seal())
                        },
                    ));
                    let r = match r {
                        Ok(r) => r,
                        Err(_) => Err(Box::new(PgError::new(
                            ERROR,
                            "parallel load-sort encoder panicked",
                        ))),
                    };
                    let failed = r.is_err();
                    if tx.send((b.idx, r)).is_err() || failed {
                        break;
                    }
                }
            });
        }
        drop(done_tx); // encoder clones remain; done_rx ends when they exit

        let mut body = |first_err: &mut Option<Box<PgError>>| -> PgResult<()> {
            let mut key: Vec<u8> = Vec::with_capacity(sort.key_w);
            let mut row: Vec<u8> = Vec::new();
            let mut cur = Batch { idx: 0, arena: Vec::new(), lens: Vec::new() };
            let mut next_send = 0u64;
            let mut pending: BTreeMap<u64, cbstore::EncodedRg> = BTreeMap::new();
            let mut merge_done = false;
            let mut since_cfi = 0u32;
            loop {
                // 1. fill + send one RG-sized batch (blocking send = the
                //    backpressure bound; done_rx is unbounded so encoders
                //    never deadlock against a full work channel).
                if !merge_done && first_err.is_none() {
                    while cur.lens.len() < RG {
                        if !merge.next_entry(&mut key, &mut row)? {
                            merge_done = true;
                            break;
                        }
                        cur.arena.extend_from_slice(&row);
                        cur.lens.push(row.len() as u32);
                        n_rows += 1;
                        since_cfi += 1;
                        if since_cfi >= 4096 {
                            since_cfi = 0;
                            postgres_seams::check_for_interrupts::call()?;
                        }
                    }
                    if !cur.lens.is_empty() && (cur.lens.len() == RG || merge_done) {
                        let full = std::mem::replace(
                            &mut cur,
                            Batch { idx: next_send + 1, arena: Vec::new(), lens: Vec::new() },
                        );
                        if work_tx.send(full).is_err() {
                            return Err(Box::new(PgError::new(
                                ERROR,
                                "parallel load-sort encoder pool exited early",
                            )));
                        }
                        next_send += 1;
                    }
                }
                // 2. drain finished encodes; commit in batch order.
                loop {
                    match done_rx.try_recv() {
                        Ok((idx, Ok(enc))) => {
                            pending.insert(idx, enc);
                        }
                        Ok((_, Err(e))) => {
                            if first_err.is_none() {
                                *first_err = Some(e);
                            }
                        }
                        Err(_) => break,
                    }
                }
                while pending.keys().next() == Some(&committed) {
                    let enc = pending.remove(&committed).unwrap();
                    writer.commit_encoded_rg(enc)?;
                    committed += 1;
                    if committed % 16 == 0 {
                        pgstat_progress_update_param(
                            PROGRESS_COPY_TUPLES_PROCESSED,
                            (committed * RG as u64) as i64,
                        );
                    }
                }
                if first_err.is_some() {
                    return Ok(());
                }
                if merge_done && committed == next_send {
                    return Ok(());
                }
                // 3. everything sent, nothing committable: block on done.
                if merge_done {
                    match done_rx.recv() {
                        Ok((idx, Ok(enc))) => {
                            pending.insert(idx, enc);
                        }
                        Ok((_, Err(e))) => {
                            if first_err.is_none() {
                                *first_err = Some(e);
                            }
                        }
                        Err(_) => {
                            return Err(Box::new(PgError::new(
                                ERROR,
                                "parallel load-sort encoder pool exited with batches pending",
                            )));
                        }
                    }
                }
            }
        };
        let r = body(&mut first_err);
        drop(work_tx); // encoders drain + exit; scope joins them
        if let Err(e) = r {
            if first_err.is_none() {
                first_err = Some(e);
            }
        }
    });

    if let Some(e) = first_err {
        return Err(e);
    }
    pgstat_progress_update_param(PROGRESS_COPY_TUPLES_PROCESSED, n_rows as i64);
    Ok(n_rows)
}

fn vacuum_style_shutdown(private: &(dyn std::any::Any + Send + Sync)) {
    let Some(payload) = private.downcast_ref::<ParCopyShared>() else { return };
    payload.source.close();
    payload.rt.notify_source_progress();
    if let Some(rg) = payload.rg.get().and_then(|w| w.upgrade()) {
        if rg.try_outcome().is_none() {
            drain_rg(payload.rt, &rg);
        }
    }
}

fn ensure_hooks_registered() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        parallel::register_parallel_worker_entrypoint(
            "pgrust_parallel_copy_main",
            parallel_copy_worker_main,
        );
        parallel::register_parallel_private_shutdown(vacuum_style_shutdown);
    });
}

/// Abort + BOUNDED drain of a pinned RG no helper will drive (the vacuum
/// drain shape). False = leaked (dead participant).
fn drain_rg(rt: &'static Arc<runtime::Runtime>, rg: &runtime::RgHandle) -> bool {
    rg.abort();
    rt.notify_source_progress();
    let mut lane = None;
    for _ in 0..4000 {
        if let Some(l) = rt.acquire_external_lane() {
            lane = Some(l);
            break;
        }
        std::thread::sleep(std::time::Duration::from_micros(500));
    }
    let Some(lane) = lane else { return false };
    let mut local = lane.local();
    rt.try_drain_pinned(&mut local, rg, 4000).is_some()
}

/// Fail-closed admission. `Ok(None)` = serial COPY, byte-identically. All
/// checks are metadata-only: NO input is consumed before this passes.
fn admit<'mcx>(
    cstate: &CopyFromState<'mcx, '_>,
    rel: &Relation<'mcx>,
    has_triggers: bool,
) -> PgResult<Option<(&'static Arc<runtime::Runtime>, i32, Option<ParCopySort>)>> {
    // Macro-compatible shim: refuse! returns Ok(None) from THIS fn.
    if !flag_enabled() || !runtime::runtime_enabled() {
        return Ok(None);
    }
    let Some(rt) = runtime::global() else { refuse!("no runtime pool") };
    if parallel::IsParallelWorker() || !init_small::globals::IsUnderPostmaster() {
        refuse!("not a postmaster session leader");
    }
    if tableam_vocab::TableAm::of(rel) != Some(tableam_vocab::TableAm::Cbstore) {
        refuse!("not a cbstore relation");
    }
    if has_triggers {
        refuse!("relation has triggers");
    }
    if rel.rd_rel.relhasindex {
        refuse!("relation has indexes");
    }
    if rel
        .rd_att
        .constr
        .as_deref()
        .is_some_and(|c| c.has_generated_stored || c.has_generated_virtual)
    {
        refuse!("generated columns");
    }
    let o = &cstate.opts;
    if o.binary {
        refuse!("binary format");
    }
    if o.csv_mode {
        refuse!("csv format (phase-1 is text-only)");
    }
    if o.header_line != CopyHeaderChoice::False {
        refuse!("HEADER");
    }
    if o.on_error != CopyOnErrorChoice::Stop {
        refuse!("ON_ERROR ignore (row-dropping shifts RG seams)");
    }
    if o.default_print.is_some() || !cstate.defmap.is_empty() || cstate.volatile_defexprs {
        refuse!("column defaults");
    }
    if cstate.convert_select_flags.is_some() {
        refuse!("convert_select");
    }
    if !cstate.where_clause.is_nil() {
        refuse!("WHERE clause (row-dropping shifts RG seams)");
    }
    if cstate.need_transcoding {
        refuse!("encoding conversion");
    }
    if cstate.escontext.is_some() {
        refuse!("soft-error context");
    }
    // Every physical column must be COPY-listed (no defaults admitted, and
    // cbstore refuses NULLs anyway — but refuse here for the exact serial
    // error path).
    if cstate.attnumlist.len() != rel.rd_att.natts as usize {
        refuse!("partial column list");
    }
    // cbstore geometry: supported coltypes, no cluster key (sort-on-ingest
    // drains serially by construction).
    let coltypes = match cbstore::coltypes_of(rel) {
        Ok(t) => t,
        Err(_) => refuse!("unsupported cbstore column type (serial raises the error)"),
    };
    let sort = match cbstore::writer::writer_opts_of(rel, &coltypes) {
        Ok(opts) if opts.cluster_key.is_empty() && opts.presort_key.is_empty() => None,
        Ok(opts) if !opts.cluster_key.is_empty() => {
            refuse!("cluster_key (sort-on-ingest is serial)")
        }
        Ok(opts) => {
            // PGRUST_COPY_PRESORT: the parallel load-sort pipeline, behind
            // its own flag; int-class fixed-width keys only. Every refusal
            // = the serial sort-on-ingest path verbatim (L3-0, byte-proven).
            if !sort_flag_enabled() {
                refuse!("PGRUST_COPY_PRESORT (sort-on-ingest is serial; PGRUST_PARALLEL_COPY_SORT=1 engages the parallel sort)");
            }
            let Some(key_w) = cbstore::sortkey::fixed_key_width(&opts.presort_key) else {
                refuse!("PGRUST_COPY_PRESORT text key (parallel load-sort is int-class only)");
            };
            static SORT_NONCE: AtomicU64 = AtomicU64::new(1);
            Some(ParCopySort {
                keys: opts.presort_key,
                key_w,
                budget: sort_budget(),
                nonce: SORT_NONCE.fetch_add(1, Ordering::SeqCst),
            })
        }
        Err(_) => refuse!("cbstore reloption error (serial raises it)"),
    };
    // File-source size floor (frontend streams engage regardless).
    if let CopySrc::File { fd, .. } = &cstate.src {
        let size = fd::with_allocated_stdio(*fd, |f| f.metadata().map(|m| m.len()).unwrap_or(0))
            .unwrap_or(0);
        if size < file_floor() {
            refuse!(format!("file smaller than the {}B floor", file_floor()));
        }
    }
    let k = dop(rt);
    if k < 1 {
        refuse!("dop < 1");
    }
    Ok(Some((rt, k, sort)))
}

enum Ceremony {
    /// Pre-consumption refusal (zero workers launched/participating):
    /// nothing read; serial takes over.
    Refused,
    Done(u64),
}

/// Morsel-parallel COPY FROM. `Ok(None)` = refused, run the serial path
/// (cstate untouched). `Ok(Some(n))` = n rows loaded and published.
/// Errors are FULLY CONTEXTED (worker line contexts attached) — the caller
/// must NOT wrap them in copy_from_error_context again.
pub(crate) fn copy_from_parallel<'mcx>(
    _mcx: Mcx<'mcx>,
    cstate: &mut CopyFromState<'mcx, '_>,
    rel: &Relation<'mcx>,
    has_triggers: bool,
) -> PgResult<Option<u64>> {
    let Some((rt, k, sort)) = admit(cstate, rel, has_triggers)? else {
        return Ok(None);
    };

    // Writer open BEFORE EnterParallelMode (xid/cid assignment); identical
    // to the serial open (header init, freeze decision, append handling).
    // Sort mode opens PLAIN (the sort happens upstream in the workers; the
    // merged drain through append_row is the L3-0 byte-proven path).
    let mut writer = if sort.is_some() {
        cbstore::writer::begin_parallel_ingest_presorted(rel)?
    } else {
        cbstore::begin_parallel_ingest(rel)?
    };
    let Some(plan) = writer.parallel_ingest_plan() else {
        // Belt+braces: admission already refused cluster keys.
        return Ok(None);
    };

    let shared = Arc::new(ParCopyShared {
        rt,
        rg: OnceLock::new(),
        source: Arc::new(runtime::StreamSource::new()),
        relid: rel.rd_id,
        relname: cstate.relname.clone(),
        delim: cstate.opts.delim,
        null_print: cstate.opts.null_print.to_string(),
        freeze: cstate.opts.freeze,
        file_encoding: cstate.file_encoding,
        eol: Mutex::new(EolPre { later: EolType::Unknown }),
        attnumlist: cstate.attnumlist.iter().copied().collect(),
        plan: Arc::new(plan),
        chunks: Mutex::new(HashMap::new()),
        done: Mutex::new(BTreeMap::new()),
        errors: Mutex::new(BTreeMap::new()),
        error_floor: AtomicU64::new(u64::MAX),
        failed_hard: AtomicBool::new(false),
        hard_error: Mutex::new(None),
        refused: AtomicUsize::new(0),
        started: AtomicUsize::new(0),
        leader_proc: init_small::globals::MyProcNumber(),
        sort,
        sort_runs: Mutex::new(Vec::new()),
        sort_run_seq: AtomicU64::new(0),
    });
    ensure_hooks_registered();

    // Sort-run cleanup on EVERY exit path (error unwind included): any
    // registered, not-yet-consumed run file is unlinked (missing = fine —
    // the merge eagerly unlinks after open).
    struct RunCleanup(Arc<ParCopyShared>);
    impl Drop for RunCleanup {
        fn drop(&mut self) {
            let paths = std::mem::take(
                &mut *self.0.sort_runs.lock().unwrap_or_else(|p| p.into_inner()),
            );
            for p in paths {
                let _ = std::fs::remove_file(p);
            }
        }
    }
    let _run_cleanup = RunCleanup(Arc::clone(&shared));

    xact::EnterParallelMode();
    let r = ceremony(cstate, &mut writer, &shared, rt, k);
    xact::ExitParallelMode();

    match r? {
        Ceremony::Refused => Ok(None),
        Ceremony::Done(processed) => {
            // Publish (footer + header, durable) — the serial finish.
            writer.finish_parallel_ingest()?;
            pgstat_progress_update_param(PROGRESS_COPY_TUPLES_PROCESSED, processed as i64);
            ptrace(&format!("done rows={processed}"));
            Ok(Some(processed))
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn ceremony(
    cstate: &mut CopyFromState<'_, '_>,
    writer: &mut cbstore::CbWriter,
    shared: &Arc<ParCopyShared>,
    rt: &'static Arc<runtime::Runtime>,
    k: i32,
) -> PgResult<Ceremony> {
    let pcxt = parallel::CreateParallelContext("postgres", "pgrust_parallel_copy_main", k)?;
    let mut submitted: Option<runtime::RgHandle> = None;

    let body = (|submitted: &mut Option<runtime::RgHandle>| -> PgResult<Ceremony> {
        parallel::InitializeParallelDSM(pcxt)?;
        if parallel::nworkers(pcxt) <= 0 {
            return Ok(Ceremony::Refused);
        }
        parallel::set_private(pcxt, Arc::clone(shared) as _);

        static NEXT_QUERY_ID: AtomicU64 = AtomicU64::new(1);
        let work: Arc<dyn runtime::TaskSetWork> = Arc::clone(shared) as _;
        let source: Arc<dyn runtime::MorselSource> = Arc::clone(&shared.source) as _;
        let (rg, waiter) = rt.submit_pinned(runtime::QuerySpec {
            query_id: NEXT_QUERY_ID.fetch_add(1, Ordering::SeqCst),
            tasksets: vec![runtime::TaskSetSpec { source, work, deps: vec![] }],
        });
        shared
            .rg
            .set(rg.downgrade())
            .unwrap_or_else(|_| unreachable!("rg set once per statement payload"));
        *submitted = Some(rg.clone());

        let launched = parallel::LaunchParallelWorkers(pcxt)?;
        if launched <= 0 {
            drain_rg(rt, &rg);
            return Ok(Ceremony::Refused);
        }
        ptrace(&format!("engaged dop={launched} window={}", window(k)));

        // ---- the leader loop: segment/publish + ordered commit ----
        let mut seg = Segmentator::new(cbstore::format::RG_ROWS as u32);
        let mut published = 0u64;
        let mut next_commit = 0u64;
        let mut processed = 0u64;
        let mut input_done = false;
        let mut closed = false;
        let mut bytes_read = 0u64;
        let window = window(k);
        let mut ready: Vec<ChunkDesc> = Vec::new();
        let outcome = loop {
            // 1. Ordered commits of every ready RG.
            let mut committed_any = false;
            loop {
                let enc = {
                    let mut d = shared.done.lock().unwrap_or_else(|p| p.into_inner());
                    d.remove(&next_commit)
                };
                let Some(enc) = enc else { break };
                if let Some(enc) = enc {
                    processed += enc.nrows() as u64;
                    writer.commit_encoded_rg(enc)?;
                }
                next_commit += 1;
                committed_any = true;
            }
            if committed_any {
                pgstat_progress_update_param(PROGRESS_COPY_TUPLES_PROCESSED, processed as i64);
            }

            // 2. Read + segment + publish under the window (stop on error).
            let error_seen = shared.error_floor.load(Ordering::SeqCst) != u64::MAX;
            let mut read_any = false;
            if !input_done && !error_seen && published.saturating_sub(next_commit) < window {
                let mut buf = vec![0u8; READ_BLOCK];
                let n = cstate.copy_read_stream(&mut buf)?;
                bytes_read += n as u64;
                pgstat_progress_update_param(PROGRESS_COPY_BYTES_PROCESSED, bytes_read as i64);
                read_any = n > 0;
                if n > 0 {
                    buf.truncate(n);
                    let abuf = Arc::new(buf);
                    let consumed = seg.feed(&abuf, n, &mut ready);
                    if seg.eoc {
                        // End-of-copy marker: never segment past it. A
                        // frontend stream drains protocol-level (serial's
                        // copy_read_line drain); files just stop.
                        let _ = consumed;
                        if matches!(cstate.src, CopySrc::Frontend { .. }) {
                            let mut sink = vec![0u8; READ_BLOCK];
                            while cstate.copy_read_stream(&mut sink)? > 0 {}
                        }
                        input_done = true;
                    }
                }
                if n == 0 && !input_done {
                    input_done = true;
                }
                if input_done {
                    seg.finish(&mut ready);
                }
                if !ready.is_empty() {
                    // EOL decided by now (any cut chunk saw a terminator);
                    // chunks >= 1 inherit it.
                    shared.eol.lock().unwrap_or_else(|p| p.into_inner()).later = seg.eol_type();
                    let mut m = shared.chunks.lock().unwrap_or_else(|p| p.into_inner());
                    for c in ready.drain(..) {
                        m.insert(published, c);
                        published += 1;
                    }
                    drop(m);
                    shared.source.publish(published);
                    rt.notify_source_progress();
                }
                if input_done && !closed {
                    shared.source.close();
                    rt.notify_source_progress();
                    closed = true;
                    ptrace(&format!(
                        "input closed chunks={published} rows={} bytes={bytes_read}",
                        seg.rows_total
                    ));
                }
            } else if error_seen && !closed {
                // First error recorded: stop feeding; already-published
                // chunks above the floor drain in the workers.
                shared.source.close();
                rt.notify_source_progress();
                closed = true;
            }

            // 3. Completion / failure / cancel polling.
            if let Some(o) = waiter.try_wait() {
                break o;
            }
            if let Err(e) = postgres_seams::check_for_interrupts::call()
                .and_then(|()| parallel::ProcessParallelMessages())
            {
                drain_rg(rt, &rg);
                return Err(e);
            }
            if parallel::parallel_workers_all_stopped(pcxt) {
                if let Some(o) = waiter.try_wait() {
                    break o;
                }
                let claimed = rg.stats().tasks_claimed;
                let drained = drain_rg(rt, &rg);
                if claimed == 0 && drained && published == 0 && bytes_read == 0 {
                    return Ok(Ceremony::Refused);
                }
                if let Some(e) = shared.take_hard_error() {
                    return Err(e);
                }
                return Err(Box::new(PgError::new(
                    ERROR,
                    "parallel COPY helpers exited before completing the load",
                )));
            }
            let refused = shared.refused.load(Ordering::SeqCst);
            let started = shared.started.load(Ordering::SeqCst);
            if started == 0 && refused >= launched as usize {
                drain_rg(rt, &rg);
                if bytes_read == 0 {
                    return Ok(Ceremony::Refused);
                }
                return Err(Box::new(PgError::new(
                    ERROR,
                    "parallel COPY: every helper refused participation mid-load",
                )));
            }

            // 4. Idle wait only when there is nothing to do (window full or
            // input done, and nothing committed this pass).
            if !committed_any && !read_any {
                if let Err(e) = parallel::wait_parallel_finish_quantum() {
                    drain_rg(rt, &rg);
                    return Err(e);
                }
            }
        };

        // RG complete: drain remaining ordered commits.
        loop {
            let enc = {
                let mut d = shared.done.lock().unwrap_or_else(|p| p.into_inner());
                d.remove(&next_commit)
            };
            let Some(enc) = enc else { break };
            if let Some(enc) = enc {
                processed += enc.nrows() as u64;
                writer.commit_encoded_rg(enc)?;
            }
            next_commit += 1;
        }

        if let Some(e) = shared.take_hard_error() {
            return Err(e);
        }
        // First-error-in-input-order: the minimum-chunk error wins (every
        // chunk below it completed or recorded its own, earlier error).
        if let Some(e) = shared.take_min_error() {
            return Err(e);
        }
        if outcome == runtime::RgOutcome::Aborted {
            postgres_seams::check_for_interrupts::call()?;
            return Err(Box::new(PgError::new(ERROR, "parallel COPY aborted")));
        }
        if shared.started.load(Ordering::SeqCst) == 0 {
            if bytes_read == 0 {
                return Ok(Ceremony::Refused);
            }
            return Err(Box::new(PgError::new(
                ERROR,
                "parallel COPY completed with no participating workers",
            )));
        }
        debug_assert_eq!(next_commit, published, "ordered commit hole");

        // load-r2 L3-1 sort mode: every parsed row lives in the run files;
        // workers flush their FINAL run post-drive (after the RG outcome),
        // so wait for actual worker exit, then k-way merge the runs into
        // the plain writer — the serial presort drain byte-path.
        if shared.sort.is_some() {
            while !parallel::parallel_workers_all_stopped(pcxt) {
                postgres_seams::check_for_interrupts::call()?;
                parallel::ProcessParallelMessages()?;
                parallel::wait_parallel_finish_quantum()?;
            }
            parallel::ProcessParallelMessages()?;
            if let Some(e) = shared.take_hard_error() {
                return Err(e);
            }
            processed = merge_sorted_runs(writer, shared)?;
        }
        Ok(Ceremony::Done(processed))
    })(&mut submitted);

    // Teardown tail (every path): the RG must be COMPLETE before the
    // context is destroyed (helpers reference the payload until then).
    if let Some(rg) = &submitted {
        if rg.try_outcome().is_none() {
            shared.source.close();
            rt.notify_source_progress();
            if !drain_rg(rt, rg) {
                ereport(WARNING)
                    .errmsg("parallel COPY leaked a pinned resource group during teardown")
                    .finish(types_error::ErrorLocation::new("copyfrom.c", 0, "ceremony"))?;
            }
        }
    }
    let destroy = parallel::DestroyParallelContext(pcxt);
    let out = body?;
    destroy?;
    Ok(out)
}

#[cfg(test)]
mod segmentator_tests {
    use super::*;

    fn segment(input: &[u8], rows_per_chunk: u32, block: usize) -> (Vec<ChunkDesc>, Segmentator) {
        let mut seg = Segmentator::new(rows_per_chunk);
        let mut out = Vec::new();
        let mut off = 0usize;
        while off < input.len() && !seg.eoc {
            let hi = (off + block).min(input.len());
            let buf = Arc::new(input[off..hi].to_vec());
            let n = buf.len();
            seg.feed(&buf, n, &mut out);
            off = hi;
        }
        if !seg.eoc {
            seg.finish(&mut out);
        }
        (out, seg)
    }

    fn chunk_bytes(c: &ChunkDesc) -> Vec<u8> {
        let mut cur = ChunkCursor::new(c.segs.clone());
        let mut all = Vec::new();
        let mut buf = [0u8; 64];
        loop {
            let n = cur.read(&mut buf);
            if n == 0 {
                break;
            }
            all.extend_from_slice(&buf[..n]);
        }
        all
    }

    /// Chunks partition the input exactly, cut every rows_per_chunk rows,
    /// with 1-based first_lineno bookkeeping — at EVERY block size (buffer-
    /// edge carry states).
    #[test]
    fn partitions_lf_rows_exactly() {
        let mut input = Vec::new();
        for i in 0..25 {
            input.extend_from_slice(format!("row{i}\tv\n").as_bytes());
        }
        for block in [1, 2, 3, 7, 64, 4096] {
            let (chunks, seg) = segment(&input, 10, block);
            assert_eq!(seg.rows_total, 25, "block {block}");
            assert_eq!(chunks.len(), 3, "block {block}");
            assert_eq!(chunks[0].first_lineno, 1);
            assert_eq!(chunks[1].first_lineno, 11);
            assert_eq!(chunks[2].first_lineno, 21);
            let joined: Vec<u8> =
                chunks.iter().flat_map(|c| chunk_bytes(c)).collect();
            assert_eq!(joined, input, "block {block}");
        }
    }

    /// Escaped newlines are data: "a\<LF>b" is ONE row (odd backslash run),
    /// "a\\<LF>" ends a row (even run) — at every block size.
    #[test]
    fn backslash_parity_rules() {
        let input = b"a\\\nb\nc\\\\\nd\n".to_vec();
        // Rows: "a\<LF>b", "c\\", "d".
        for block in [1, 2, 3, 5, 64] {
            let (chunks, seg) = segment(&input, 1, block);
            assert_eq!(seg.rows_total, 3, "block {block}");
            assert_eq!(chunks.len(), 3, "block {block}");
            assert_eq!(chunk_bytes(&chunks[0]), b"a\\\nb\n".to_vec());
            assert_eq!(chunk_bytes(&chunks[1]), b"c\\\\\n".to_vec());
            assert_eq!(chunk_bytes(&chunks[2]), b"d\n".to_vec());
        }
    }

    /// CRLF detection + boundaries, including the \r|\n buffer-edge split.
    #[test]
    fn crlf_rows() {
        let input = b"a\r\nb\r\nc\r\n".to_vec();
        for block in [1, 2, 3, 4, 64] {
            let (chunks, seg) = segment(&input, 2, block);
            assert_eq!(seg.rows_total, 3, "block {block}");
            assert!(matches!(seg.eol, SegEol::Crnl));
            assert_eq!(chunks.len(), 2);
            assert_eq!(chunks[1].first_lineno, 3);
        }
    }

    /// Classic-Mac CR rows.
    #[test]
    fn cr_rows() {
        let input = b"a\rb\rc\r".to_vec();
        for block in [1, 2, 64] {
            let (chunks, seg) = segment(&input, 10, block);
            assert_eq!(seg.rows_total, 3, "block {block}");
            assert!(matches!(seg.eol, SegEol::Cr));
            assert_eq!(chunks.len(), 1);
        }
    }

    /// A trailing unterminated line is a row (serial parses it too).
    #[test]
    fn trailing_partial_line() {
        let input = b"a\nb\nc-no-newline".to_vec();
        let (chunks, seg) = segment(&input, 10, 4);
        assert_eq!(seg.rows_total, 2, "boundaries only");
        assert_eq!(chunks.len(), 1);
        let joined = chunk_bytes(&chunks[0]);
        assert_eq!(joined, input);
    }

    /// End-of-copy marker: the marker LINE lands in the final chunk; bytes
    /// past it are never segmented.
    #[test]
    fn end_of_copy_marker() {
        let input = b"a\nb\n\\.\nGARBAGE AFTER".to_vec();
        for block in [1, 2, 3, 64] {
            let (chunks, seg) = segment(&input, 10, block);
            assert!(seg.eoc, "block {block}");
            let joined: Vec<u8> = chunks.iter().flat_map(|c| chunk_bytes(c)).collect();
            assert_eq!(joined, b"a\nb\n\\.\n".to_vec(), "block {block}");
        }
    }

    /// "\\." at line start is an escaped backslash + dot — NOT the marker.
    #[test]
    fn escaped_backslash_dot_is_not_eoc() {
        let input = b"\\\\.\nb\n".to_vec();
        let (chunks, seg) = segment(&input, 10, 2);
        assert!(!seg.eoc);
        assert_eq!(seg.rows_total, 2);
        assert_eq!(chunks.len(), 1);
    }

    /// Exact-RG cut: no empty trailing chunk when input ends on a boundary.
    #[test]
    fn no_empty_final_chunk() {
        let input = b"a\nb\n".to_vec();
        let (chunks, seg) = segment(&input, 2, 64);
        assert_eq!(seg.rows_total, 2);
        assert_eq!(chunks.len(), 1);
    }

    /// Chunk cursor reassembles multi-seg chunks byte-exactly.
    #[test]
    fn cursor_reassembles() {
        let mut input = Vec::new();
        for i in 0..100 {
            input.extend_from_slice(format!("{i}\tabcdefghij\n").as_bytes());
        }
        let (chunks, _) = segment(&input, 40, 17);
        let joined: Vec<u8> = chunks.iter().flat_map(|c| chunk_bytes(c)).collect();
        assert_eq!(joined, input);
    }
}
