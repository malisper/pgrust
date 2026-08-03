//! instrument_diff: differential fuzz driver — shipped Rust
//! `backend/executor/instrument` vs vendored PostgreSQL 18.3 (Stamp-18.3,
//! upstream sha 62d6c7d3df) C (csrc/pg_instrbe_io.c, assembled by
//! csrc/gen/assemble_instrbe.sh). Crate under test:
//! crates/backend/executor/instrument. Census carve: family-grain.
//!
//! IN: InstrInit / InstrStartNode / InstrStopNode / InstrUpdateTupleCount /
//! InstrEndLoop / InstrAggNode + BufferUsageAdd / WalUsageAdd /
//! BufferUsageAccumDiff / WalUsageAccumDiff — pure struct arithmetic
//! (wal_bytes wrapping matches C uint64 semantics).
//!
//! OUT per census (fenced, not vendored — see the oracle header):
//! InstrStartParallelQuery / InstrEndParallelQuery / InstrAccumParallelQuery
//! and the save_pg* statics: the Rust WORKER_CONTRIB overlay
//! (instr_start/end/accum_parallel_query, pg_buffer_usage's overlay add) is
//! a DELIBERATE divergence from C's write-into-the-live-globals scheme, and
//! the global-counter readers (pg_buffer_usage / pg_wal_usage as live-counter
//! probes) are environment reads. InstrAlloc (palloc plumbing; no Rust twin)
//! is also OUT; its option fanout is InstrInit's, which is compared.
//!
//! GLOBALS PIN (census mandate, the exact wiring):
//!   - pgBufferUsage is pinned ZERO on both sides for the start/stop paths.
//!     Rust: the fuzz driver never bumps bufmgr::counters / fd::buffile and
//!     never calls instr_accum_parallel_query, so `pg_buffer_usage()` is
//!     zero by construction — WITNESSED by an assert at every arm-1 init.
//!     C: pg_instrbe_reset() zeroes pgBufferUsage and the oracle TU compiles
//!     no writer of it and exports no setter. Fuzz-fed buffer deltas enter
//!     through IDENTICAL struct-field injection on both sides (ops 4-6
//!     write bufusage_start / accumulators from one derivation).
//!   - pgWalUsage is SET FROM FUZZ INPUT identically on both sides (op 7):
//!     C via pg_instrbe_set_wal_global; Rust via the installed
//!     transam_xlog_seams::wal_usage seam, which reads a driver
//!     thread-local holding the same derived WalUsage. This drives the REAL
//!     global-read path (walusage_start snapshot + accum-diff) with fuzz
//!     values.
//!
//! stub:clock (census stub_req): every INSTR_TIME_SET_CURRENT read derives
//! from fuzz input as base + per-op increments and is pinned identically on
//! both sides before every paired call via `stubs::clock::pin_mono_ns`
//! (Rust: pg_clock's fuzz_mono_pin cell — `instr_time_current` now reads
//! `pg_clock::mono_ns`; C: pg_stub_get_mono_ns behind the oracle's
//! INSTR_TIME_SET_CURRENT shim). The clock SEQUENCE re-pins between ops and
//! is non-decreasing with cumulative bound < 2^42 ns (see bounds below).
//!
//! Comparison planes: EVERY Instrumentation/BufferUsage/WalUsage field
//! value-compared after EVERY op (f64 totals bit-exact via a packed
//! little-endian wire image both sides serialize to; counters exact) +
//! error-verdict + error message literal (the three instrument.c elogs are
//! single string literals; the Rust panics carry the same strings). No
//! tolerance anywhere except the NaN-payload relaxation on the f64 fields
//! (fuzz-domain bounds below).
//!
//! Input layout: [selector][payload]; selector % 4 picks the arm:
//!   0 init: fuzz-constructed garbage node loaded BOTH sides, then
//!     InstrInit(options) with options = full i32 from input; full-struct
//!     compare (witnesses memset == Instrumentation::default()).
//!   1 cycle sequence: [flags][clock base u32][options u8] then up to 24
//!     ops, op byte % 8: 0 StartNode, 1 StopNode(nTuples), 2
//!     InstrUpdateTupleCount(nTuples), 3 InstrEndLoop, 4 inject
//!     bufusage_start, 5 inject walusage_start, 6 inject accumulators, 7
//!     set the WAL globals (both sides). Start/Stop advance + re-pin the
//!     clock. Error shapes (start-twice, stop-without-start,
//!     end-loop-running) are REACHED by op sequences and compared on the
//!     verdict + message planes; both sides keep their pre-error partial
//!     mutations (C longjmp == Rust unwind, compared field-by-field after).
//!   2 EndLoop/AggNode over two fuzz-constructed Instrumentation states
//!     (all fields from input, one derivation, loaded both sides);
//!     optional EndLoop on each then InstrAggNode(dst, add); both structs
//!     compared after every step.
//!   3 pure struct arithmetic: BufferUsageAdd / BufferUsageAccumDiff /
//!     WalUsageAdd / WalUsageAccumDiff on fuzz-fed operands (fresh
//!     derivations each; wal_bytes FULL u64 domain incl. a near-u64::MAX
//!     mode for the wrapping semantics).
//!
//! Fuzz-domain bounds (documented; all on COMPARED-EQUAL derived inputs so
//! neither side sees a value the other doesn't):
//!   - int64 counter fields and instr_time ticks in constructed structs are
//!     i32-derived (sign-extended): |v| <= 2^31, and <= 40 add/sub ops per
//!     exec keep every sum within +/-2^37 — C signed-overflow UB and the
//!     Rust debug-overflow panic stay unreachable on BOTH sides for the
//!     int64 planes. This is a fuzz-domain bound, not a behavior carve
//!     (the executor's counters are far below 2^31 in any real period).
//!   - wal_bytes is the FULL u64 domain (C uint64 arithmetic is modular;
//!     Rust uses wrapping_add/wrapping_sub — the wrap IS a compared
//!     surface, seeded near u64::MAX).
//!   - f64 fields (nTuples, firsttuple, tuplecount, startup, total,
//!     ntuples..nfiltered2) are the FULL bit domain incl. NaN/inf/-0.0;
//!     compared bit-exact EXCEPT that any-NaN == any-NaN (the diff.rs /
//!     geo_io_diff.rs certified relaxation): WHICH NaN payload `a + b`
//!     propagates when an operand is NaN is IEEE-unspecified and
//!     compiler-dependent — LLVM freely commutes fadd, so C-vs-C under
//!     different compilers diverges the same way (witnessed 2026-08-01:
//!     tuplecount += sNaN onto a qNaN total returned the first-operand
//!     qNaN from clang C and the quietened sNaN from rustc on the same
//!     arm64 fadd hardware — seed nan_payload_carve_witness). NaN-ness
//!     itself (NaN vs non-NaN, and every non-NaN bit incl. -0.0 vs 0.0)
//!     stays bit-exact; no executor consumer reads NaN payloads.
//!   - pinned clock: base < 2^32 ns, per-op increments <= 2^36, <= 24 ops:
//!     cumulative < 2^42 — the int64 tick arithmetic (counter += end -
//!     start) stays in the defined region on both sides. A REAL monotonic
//!     clock never regresses either; zero-clock (base = 0) IS exercised
//!     (the INSTR_TIME_SET_CURRENT_LAZY zero-sentinel path, which both
//!     sides resolve identically).

use std::cell::Cell;
use std::ffi::CStr;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Once;

use instrument::{
    buffer_usage_accum_diff, buffer_usage_add, instr_agg_node, instr_end_loop, instr_init,
    instr_start_node, instr_stop_node, instr_update_tuple_count, wal_usage_accum_diff,
    wal_usage_add,
};
use types_core::instrument::{instr_time, BufferUsage, Instrumentation, WalUsage};

extern "C" {
    fn pg_instrbe_reset();
    fn pg_instrbe_set_wal_global(w: *const u8);
    fn pg_instrbe_set_async(on: i32);
    fn pg_instrbe_load_node(which: i32, wire: *const u8);
    fn pg_instrbe_store_node(which: i32, out: *mut u8);
    fn pg_instrbe_errmsg() -> *const std::os::raw::c_char;
    fn pg_instrbe_init(options: i32);
    fn pg_instrbe_start_node() -> i32;
    fn pg_instrbe_stop_node(ntuples_bits: u64) -> i32;
    fn pg_instrbe_update_tuple_count(ntuples_bits: u64);
    fn pg_instrbe_end_loop(which: i32) -> i32;
    fn pg_instrbe_agg_node();
    fn pg_instrbe_bufusage_add(dst: *mut u8, add: *const u8);
    fn pg_instrbe_bufusage_accum_diff(dst: *mut u8, add: *const u8, sub: *const u8);
    fn pg_instrbe_walusage_add(dst: *mut u8, add: *const u8);
    fn pg_instrbe_walusage_accum_diff(dst: *mut u8, add: *const u8, sub: *const u8);
    fn pg_instrbe_inject_bufusage_start(w: *const u8);
    fn pg_instrbe_inject_walusage_start(w: *const u8);
    fn pg_instrbe_inject_accums(buf: *const u8, wal: *const u8);
}

pub const BUF_WIRE: usize = 128;
pub const WAL_WIRE: usize = 32;
pub const INSTR_WIRE: usize = 413;

// ---------------------------------------------------------------------------
// The Rust wal_usage seam impl: the stub:wal-global pin (driver header).
// ---------------------------------------------------------------------------

std::thread_local! {
    static DRIVER_WAL: Cell<WalUsage> = const { Cell::new(WalUsage {
        wal_records: 0, wal_fpi: 0, wal_bytes: 0, wal_buffers_full: 0 }) };
}

fn driver_wal_usage() -> WalUsage {
    DRIVER_WAL.with(|c| c.get())
}

/// Pin the pgWalUsage analog on BOTH sides from one derivation.
fn pin_wal_global(w: &WalUsage) {
    DRIVER_WAL.with(|c| c.set(*w));
    let wire = wal_wire(w);
    unsafe { pg_instrbe_set_wal_global(wire.as_ptr()) };
}

// ---------------------------------------------------------------------------
// Panic plumbing: the shipped functions signal their three elog(ERROR) twins
// with panic!. Expected panics are caught (suppressed hook) and converted to
// verdicts; comparator asserts stay UNSUPPRESSED so a divergence still
// reaches the pre-existing hook (libFuzzer's abort in fuzz builds, the
// default printer under cargo test).
// ---------------------------------------------------------------------------

std::thread_local! {
    static SUPPRESS_PANIC_HOOK: Cell<bool> = const { Cell::new(false) };
}

static INIT: Once = Once::new();

fn init_once() {
    INIT.call_once(|| {
        transam_xlog_seams::wal_usage::set(driver_wal_usage);
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            if !SUPPRESS_PANIC_HOOK.with(|s| s.get()) {
                prev(info);
            }
        }));
    });
}

/// Run a shipped entry point, converting an (expected-shape) panic into the
/// error verdict + payload message.
fn shipped(f: impl FnOnce()) -> Result<(), String> {
    SUPPRESS_PANIC_HOOK.with(|s| s.set(true));
    let r = catch_unwind(AssertUnwindSafe(f));
    SUPPRESS_PANIC_HOOK.with(|s| s.set(false));
    r.map_err(|e| {
        e.downcast_ref::<&str>()
            .map(|s| s.to_string())
            .or_else(|| e.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "<non-string panic>".to_string())
    })
}

fn c_errmsg() -> String {
    unsafe { CStr::from_ptr(pg_instrbe_errmsg()) }
        .to_string_lossy()
        .into_owned()
}

/// Error-verdict + message planes.
fn check_verdict(cv: i32, r: &Result<(), String>, ctx: &str) {
    match r {
        Ok(()) => assert!(
            cv == 0,
            "{ctx}: verdict DIVERGENCE C=err({}) Rust=Ok",
            c_errmsg()
        ),
        Err(msg) => {
            assert!(cv == 1, "{ctx}: verdict DIVERGENCE C=Ok Rust=Err({msg})");
            let cm = c_errmsg();
            assert!(
                *msg == cm,
                "{ctx}: error message DIVERGENCE C={cm:?} Rust={msg:?}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Wire images (mirror of the C pack/unpack in pg_instrbe_io.c SECTION 10).
// ---------------------------------------------------------------------------

fn wal_wire(w: &WalUsage) -> [u8; WAL_WIRE] {
    let mut out = [0u8; WAL_WIRE];
    out[0..8].copy_from_slice(&w.wal_records.to_le_bytes());
    out[8..16].copy_from_slice(&w.wal_fpi.to_le_bytes());
    out[16..24].copy_from_slice(&w.wal_bytes.to_le_bytes());
    out[24..32].copy_from_slice(&w.wal_buffers_full.to_le_bytes());
    out
}

fn buf_wire(b: &BufferUsage) -> [u8; BUF_WIRE] {
    let mut out = [0u8; BUF_WIRE];
    let f: [i64; 16] = [
        b.shared_blks_hit,
        b.shared_blks_read,
        b.shared_blks_dirtied,
        b.shared_blks_written,
        b.local_blks_hit,
        b.local_blks_read,
        b.local_blks_dirtied,
        b.local_blks_written,
        b.temp_blks_read,
        b.temp_blks_written,
        b.shared_blk_read_time.ticks,
        b.shared_blk_write_time.ticks,
        b.local_blk_read_time.ticks,
        b.local_blk_write_time.ticks,
        b.temp_blk_read_time.ticks,
        b.temp_blk_write_time.ticks,
    ];
    for (i, v) in f.iter().enumerate() {
        out[i * 8..i * 8 + 8].copy_from_slice(&v.to_le_bytes());
    }
    out
}

fn instr_wire(n: &Instrumentation) -> [u8; INSTR_WIRE] {
    let mut out = [0u8; INSTR_WIRE];
    out[0] = n.need_timer as u8;
    out[1] = n.need_bufusage as u8;
    out[2] = n.need_walusage as u8;
    out[3] = n.async_mode as u8;
    out[4] = n.running as u8;
    out[5..13].copy_from_slice(&n.starttime.ticks.to_le_bytes());
    out[13..21].copy_from_slice(&n.counter.ticks.to_le_bytes());
    out[21..29].copy_from_slice(&n.firsttuple.to_bits().to_le_bytes());
    out[29..37].copy_from_slice(&n.tuplecount.to_bits().to_le_bytes());
    out[37..165].copy_from_slice(&buf_wire(&n.bufusage_start));
    out[165..197].copy_from_slice(&wal_wire(&n.walusage_start));
    for (i, v) in [
        n.startup,
        n.total,
        n.ntuples,
        n.ntuples2,
        n.nloops,
        n.nfiltered1,
        n.nfiltered2,
    ]
    .iter()
    .enumerate()
    {
        out[197 + i * 8..205 + i * 8].copy_from_slice(&v.to_bits().to_le_bytes());
    }
    out[253..381].copy_from_slice(&buf_wire(&n.bufusage));
    out[381..413].copy_from_slice(&wal_wire(&n.walusage));
    out
}

const BUF_FIELDS: [&str; 16] = [
    "shared_blks_hit",
    "shared_blks_read",
    "shared_blks_dirtied",
    "shared_blks_written",
    "local_blks_hit",
    "local_blks_read",
    "local_blks_dirtied",
    "local_blks_written",
    "temp_blks_read",
    "temp_blks_written",
    "shared_blk_read_time",
    "shared_blk_write_time",
    "local_blk_read_time",
    "local_blk_write_time",
    "temp_blk_read_time",
    "temp_blk_write_time",
];
const WAL_FIELDS: [&str; 4] = ["wal_records", "wal_fpi", "wal_bytes", "wal_buffers_full"];

/// Named per-field ranges of the Instrumentation wire — the value plane is
/// per-field so a divergence names the exact C struct member.
fn instr_fields() -> Vec<(String, std::ops::Range<usize>)> {
    let mut f: Vec<(String, std::ops::Range<usize>)> = vec![
        ("need_timer".into(), 0..1),
        ("need_bufusage".into(), 1..2),
        ("need_walusage".into(), 2..3),
        ("async_mode".into(), 3..4),
        ("running".into(), 4..5),
        ("starttime".into(), 5..13),
        ("counter".into(), 13..21),
        ("firsttuple".into(), 21..29),
        ("tuplecount".into(), 29..37),
    ];
    for (i, n) in BUF_FIELDS.iter().enumerate() {
        f.push((format!("bufusage_start.{n}"), 37 + i * 8..45 + i * 8));
    }
    for (i, n) in WAL_FIELDS.iter().enumerate() {
        f.push((format!("walusage_start.{n}"), 165 + i * 8..173 + i * 8));
    }
    for (i, n) in [
        "startup",
        "total",
        "ntuples",
        "ntuples2",
        "nloops",
        "nfiltered1",
        "nfiltered2",
    ]
    .iter()
    .enumerate()
    {
        f.push(((*n).into(), 197 + i * 8..205 + i * 8));
    }
    for (i, n) in BUF_FIELDS.iter().enumerate() {
        f.push((format!("bufusage.{n}"), 253 + i * 8..261 + i * 8));
    }
    for (i, n) in WAL_FIELDS.iter().enumerate() {
        f.push((format!("walusage.{n}"), 381 + i * 8..389 + i * 8));
    }
    f
}

/// f64-typed Instrumentation wire fields: the NaN-payload relaxation
/// (module header) applies to these and ONLY these.
const F64_FIELDS: [&str; 9] = [
    "firsttuple",
    "tuplecount",
    "startup",
    "total",
    "ntuples",
    "ntuples2",
    "nloops",
    "nfiltered1",
    "nfiltered2",
];

/// Per-field wire equality: bit-exact, except any-NaN == any-NaN on the
/// f64-typed fields (certified NaN-payload non-surface, module header).
fn wire_field_eq(name: &str, c: &[u8], r: &[u8]) -> bool {
    if c == r {
        return true;
    }
    if F64_FIELDS.contains(&name) && c.len() == 8 {
        let cv = f64::from_bits(u64::from_le_bytes(c.try_into().unwrap()));
        let rv = f64::from_bits(u64::from_le_bytes(r.try_into().unwrap()));
        return cv.is_nan() && rv.is_nan();
    }
    false
}

/// Full-struct value plane: C node `which` vs the Rust Instrumentation,
/// field by field, bit-exact (f64 via the wire image; the named field
/// ranges tile the whole 413-byte wire, so per-field compare is total).
fn compare_node(which: i32, r: &Instrumentation, ctx: &str) {
    let mut c = [0u8; INSTR_WIRE];
    unsafe { pg_instrbe_store_node(which, c.as_mut_ptr()) };
    let rw = instr_wire(r);
    if c == rw {
        return;
    }
    for (name, range) in instr_fields() {
        assert!(
            wire_field_eq(&name, &c[range.clone()], &rw[range.clone()]),
            "{ctx}: field {name} DIVERGENCE C={:02x?} Rust={:02x?}",
            &c[range.clone()],
            &rw[range]
        );
    }
}

fn compare_buf(c_wire: &[u8; BUF_WIRE], r: &BufferUsage, ctx: &str) {
    let rw = buf_wire(r);
    for (i, n) in BUF_FIELDS.iter().enumerate() {
        assert!(
            c_wire[i * 8..i * 8 + 8] == rw[i * 8..i * 8 + 8],
            "{ctx}: BufferUsage field {n} DIVERGENCE C={:02x?} Rust={:02x?}",
            &c_wire[i * 8..i * 8 + 8],
            &rw[i * 8..i * 8 + 8]
        );
    }
}

fn compare_wal(c_wire: &[u8; WAL_WIRE], r: &WalUsage, ctx: &str) {
    let rw = wal_wire(r);
    for (i, n) in WAL_FIELDS.iter().enumerate() {
        assert!(
            c_wire[i * 8..i * 8 + 8] == rw[i * 8..i * 8 + 8],
            "{ctx}: WalUsage field {n} DIVERGENCE C={:02x?} Rust={:02x?}",
            &c_wire[i * 8..i * 8 + 8],
            &rw[i * 8..i * 8 + 8]
        );
    }
}

// ---------------------------------------------------------------------------
// Little-endian field reader (zero-fills past the payload end).
// ---------------------------------------------------------------------------

struct Rd<'a>(&'a [u8]);

impl Rd<'_> {
    fn bytes<const N: usize>(&mut self) -> [u8; N] {
        let mut out = [0u8; N];
        let n = N.min(self.0.len());
        out[..n].copy_from_slice(&self.0[..n]);
        self.0 = &self.0[n..];
        out
    }
    fn u8(&mut self) -> u8 {
        u8::from_le_bytes(self.bytes::<1>())
    }
    fn u16(&mut self) -> u16 {
        u16::from_le_bytes(self.bytes::<2>())
    }
    fn u32(&mut self) -> u32 {
        u32::from_le_bytes(self.bytes::<4>())
    }
    fn u64(&mut self) -> u64 {
        u64::from_le_bytes(self.bytes::<8>())
    }
    /// i32-derived int64 (fuzz-domain bound, module header).
    fn i64b(&mut self) -> i64 {
        self.u32() as i32 as i64
    }
    fn f64(&mut self) -> f64 {
        f64::from_bits(self.u64())
    }
    fn empty(&self) -> bool {
        self.0.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Bounded constructors (ONE derivation; both sides consume the result).
// ---------------------------------------------------------------------------

fn derive_buf(rd: &mut Rd<'_>) -> BufferUsage {
    BufferUsage {
        shared_blks_hit: rd.i64b(),
        shared_blks_read: rd.i64b(),
        shared_blks_dirtied: rd.i64b(),
        shared_blks_written: rd.i64b(),
        local_blks_hit: rd.i64b(),
        local_blks_read: rd.i64b(),
        local_blks_dirtied: rd.i64b(),
        local_blks_written: rd.i64b(),
        temp_blks_read: rd.i64b(),
        temp_blks_written: rd.i64b(),
        shared_blk_read_time: instr_time { ticks: rd.i64b() },
        shared_blk_write_time: instr_time { ticks: rd.i64b() },
        local_blk_read_time: instr_time { ticks: rd.i64b() },
        local_blk_write_time: instr_time { ticks: rd.i64b() },
        temp_blk_read_time: instr_time { ticks: rd.i64b() },
        temp_blk_write_time: instr_time { ticks: rd.i64b() },
    }
}

/// wal_bytes: FULL u64 domain; mode bit 0 flips to a near-u64::MAX value so
/// the wrapping semantics are reachable without 8 exact 0xff bytes.
fn derive_wal(rd: &mut Rd<'_>) -> WalUsage {
    let mode = rd.u8();
    let raw = rd.u64();
    WalUsage {
        wal_records: rd.i64b(),
        wal_fpi: rd.i64b(),
        wal_bytes: if mode & 1 != 0 { u64::MAX - (raw & 0xFFFF) } else { raw },
        wal_buffers_full: rd.i64b(),
    }
}

fn derive_instr(rd: &mut Rd<'_>) -> Instrumentation {
    let flags = rd.u8();
    Instrumentation {
        need_timer: flags & 1 != 0,
        need_bufusage: flags & 2 != 0,
        need_walusage: flags & 4 != 0,
        async_mode: flags & 8 != 0,
        running: flags & 16 != 0,
        // bit 5 forces the zero starttime sentinel (EndLoop's non-error arm).
        starttime: instr_time { ticks: if flags & 32 != 0 { 0 } else { rd.i64b() } },
        counter: instr_time { ticks: rd.i64b() },
        firsttuple: rd.f64(),
        tuplecount: rd.f64(),
        bufusage_start: derive_buf(rd),
        walusage_start: derive_wal(rd),
        startup: rd.f64(),
        total: rd.f64(),
        ntuples: rd.f64(),
        ntuples2: rd.f64(),
        nloops: rd.f64(),
        nfiltered1: rd.f64(),
        nfiltered2: rd.f64(),
        bufusage: derive_buf(rd),
        walusage: derive_wal(rd),
    }
}

/// nTuples: mode bit 0 = small integral count, else FULL f64 bit domain.
fn derive_ntuples(rd: &mut Rd<'_>) -> f64 {
    let mode = rd.u8();
    if mode & 1 != 0 {
        rd.u16() as f64
    } else {
        rd.f64()
    }
}

/// Load one derived state into BOTH sides' node `which`.
fn load_both(which: i32, rd: &mut Rd<'_>) -> Instrumentation {
    let n = derive_instr(rd);
    let wire = instr_wire(&n);
    unsafe { pg_instrbe_load_node(which, wire.as_ptr()) };
    n
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

pub fn instrument_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    init_once();
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    match sel % 4 {
        0 => init_diff(payload),
        1 => cycle_diff(payload),
        2 => agg_diff(payload),
        _ => arith_diff(payload),
    }
}

// ---------------------------------------------------------------------------
// Arm 0: InstrInit over fuzz-constructed garbage (memset equivalence).
// ---------------------------------------------------------------------------

fn init_diff(payload: &[u8]) {
    let mut rd = Rd(payload);
    unsafe { pg_instrbe_reset() };
    let mut n = load_both(0, &mut rd);
    compare_node(0, &n, "init: pre-load");
    let options = rd.u32() as i32;
    unsafe { pg_instrbe_init(options) };
    instr_init(&mut n, options);
    compare_node(0, &n, &format!("InstrInit(options={options:#x})"));
}

// ---------------------------------------------------------------------------
// Arm 1: start/stop cycle sequences under the pinned clock + pinned globals.
// ---------------------------------------------------------------------------

fn cycle_diff(payload: &[u8]) {
    let mut rd = Rd(payload);
    let flags = rd.u8();
    // Clock base: bit 0 forces the zero-clock sentinel start.
    let mut clock_ns: u64 = if flags & 1 != 0 { 0 } else { rd.u32() as u64 };
    let options = rd.u8() as i32;

    unsafe { pg_instrbe_reset() };
    // Zero-pin WITNESS (census mandate): the Rust live buffer globals the
    // shipped start/stop paths read must be zero in this process.
    assert_eq!(
        instrument::pg_buffer_usage(),
        BufferUsage::default(),
        "pgBufferUsage zero-pin violated on the Rust side"
    );
    pin_wal_global(&WalUsage::default());
    crate::stubs::clock::pin_mono_ns(clock_ns);

    let mut n = Instrumentation::default();
    unsafe { pg_instrbe_init(options) };
    instr_init(&mut n, options);
    if flags & 2 != 0 {
        // async_mode is InstrAlloc's field (census OUT) — set directly,
        // identically, both sides.
        unsafe { pg_instrbe_set_async(1) };
        n.async_mode = true;
    }
    compare_node(0, &n, &format!("cycle init(options={options:#x})"));

    for step in 0..24 {
        if rd.empty() {
            break;
        }
        let op = rd.u8() % 8;
        let ctx = format!("cycle step {step} op {op} (options={options:#x})");
        match op {
            0 => {
                clock_ns += clock_step(&mut rd);
                crate::stubs::clock::pin_mono_ns(clock_ns);
                let cv = unsafe { pg_instrbe_start_node() };
                let r = shipped(|| instr_start_node(&mut n));
                check_verdict(cv, &r, &ctx);
            }
            1 => {
                clock_ns += clock_step(&mut rd);
                crate::stubs::clock::pin_mono_ns(clock_ns);
                let t = derive_ntuples(&mut rd);
                let cv = unsafe { pg_instrbe_stop_node(t.to_bits()) };
                let r = shipped(|| instr_stop_node(&mut n, t));
                check_verdict(cv, &r, &ctx);
            }
            2 => {
                let t = derive_ntuples(&mut rd);
                unsafe { pg_instrbe_update_tuple_count(t.to_bits()) };
                instr_update_tuple_count(&mut n, t);
            }
            3 => {
                let cv = unsafe { pg_instrbe_end_loop(0) };
                let r = shipped(|| instr_end_loop(&mut n));
                check_verdict(cv, &r, &ctx);
            }
            4 => {
                let b = derive_buf(&mut rd);
                unsafe { pg_instrbe_inject_bufusage_start(buf_wire(&b).as_ptr()) };
                n.bufusage_start = b;
            }
            5 => {
                let w = derive_wal(&mut rd);
                unsafe { pg_instrbe_inject_walusage_start(wal_wire(&w).as_ptr()) };
                n.walusage_start = w;
            }
            6 => {
                let b = derive_buf(&mut rd);
                let w = derive_wal(&mut rd);
                unsafe {
                    pg_instrbe_inject_accums(buf_wire(&b).as_ptr(), wal_wire(&w).as_ptr())
                };
                n.bufusage = b;
                n.walusage = w;
            }
            _ => {
                let w = derive_wal(&mut rd);
                pin_wal_global(&w);
            }
        }
        compare_node(0, &n, &ctx);
    }
}

/// Per-op clock increment: 0 / tiny / medium / large (<= 2^36).
fn clock_step(rd: &mut Rd<'_>) -> u64 {
    let sel = rd.u8();
    match sel & 3 {
        0 => 0,
        1 => rd.u8() as u64,
        2 => (rd.u16() as u64) << 8,
        _ => (rd.u16() as u64) << 20,
    }
}

// ---------------------------------------------------------------------------
// Arm 2: InstrEndLoop + InstrAggNode over constructed states.
// ---------------------------------------------------------------------------

fn agg_diff(payload: &[u8]) {
    let mut rd = Rd(payload);
    let ops = rd.u8();
    unsafe { pg_instrbe_reset() };
    let mut dst = load_both(0, &mut rd);
    let mut add = load_both(1, &mut rd);
    compare_node(0, &dst, "agg: dst pre");
    compare_node(1, &add, "agg: add pre");

    if ops & 1 != 0 {
        let cv = unsafe { pg_instrbe_end_loop(0) };
        let r = shipped(|| instr_end_loop(&mut dst));
        check_verdict(cv, &r, "agg: InstrEndLoop(dst)");
        compare_node(0, &dst, "agg: InstrEndLoop(dst)");
    }
    if ops & 2 != 0 {
        let cv = unsafe { pg_instrbe_end_loop(1) };
        let r = shipped(|| instr_end_loop(&mut add));
        check_verdict(cv, &r, "agg: InstrEndLoop(add)");
        compare_node(1, &add, "agg: InstrEndLoop(add)");
    }
    unsafe { pg_instrbe_agg_node() };
    instr_agg_node(&mut dst, &add);
    compare_node(0, &dst, "InstrAggNode: dst");
    compare_node(1, &add, "InstrAggNode: add (must be untouched)");
}

// ---------------------------------------------------------------------------
// Arm 3: pure BufferUsage/WalUsage arithmetic on fuzz-fed operands.
// ---------------------------------------------------------------------------

fn arith_diff(payload: &[u8]) {
    let mut rd = Rd(payload);

    // BufferUsageAdd
    let mut d = derive_buf(&mut rd);
    let a = derive_buf(&mut rd);
    let mut cw = buf_wire(&d);
    unsafe { pg_instrbe_bufusage_add(cw.as_mut_ptr(), buf_wire(&a).as_ptr()) };
    buffer_usage_add(&mut d, &a);
    compare_buf(&cw, &d, "BufferUsageAdd");

    // BufferUsageAccumDiff
    let mut d = derive_buf(&mut rd);
    let a = derive_buf(&mut rd);
    let s = derive_buf(&mut rd);
    let mut cw = buf_wire(&d);
    unsafe {
        pg_instrbe_bufusage_accum_diff(cw.as_mut_ptr(), buf_wire(&a).as_ptr(),
                                       buf_wire(&s).as_ptr())
    };
    buffer_usage_accum_diff(&mut d, &a, &s);
    compare_buf(&cw, &d, "BufferUsageAccumDiff");

    // WalUsageAdd (wal_bytes full u64 wrap domain)
    let mut d = derive_wal(&mut rd);
    let a = derive_wal(&mut rd);
    let mut cw = wal_wire(&d);
    unsafe { pg_instrbe_walusage_add(cw.as_mut_ptr(), wal_wire(&a).as_ptr()) };
    wal_usage_add(&mut d, &a);
    compare_wal(&cw, &d, "WalUsageAdd");

    // WalUsageAccumDiff (negative-delta shapes: sub > add is C-legal modular
    // arithmetic on wal_bytes and plain signed arithmetic elsewhere)
    let mut d = derive_wal(&mut rd);
    let a = derive_wal(&mut rd);
    let s = derive_wal(&mut rd);
    let mut cw = wal_wire(&d);
    unsafe {
        pg_instrbe_walusage_accum_diff(cw.as_mut_ptr(), wal_wire(&a).as_ptr(),
                                       wal_wire(&s).as_ptr())
    };
    wal_usage_accum_diff(&mut d, &a, &s);
    compare_wal(&cw, &d, "WalUsageAccumDiff");
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// The C oracle's pgBufferUsage/pgWalUsage are the VERBATIM plain
    /// globals (the nodes are _Thread_local, the globals are not — exactly
    /// as in instrument.c). cargo test runs tests on parallel threads, so
    /// every test touching the oracle serializes here; fuzz execs are
    /// single-threaded per process and never contend.
    static C_GLOBALS: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn lock_c_globals() -> std::sync::MutexGuard<'static, ()> {
        C_GLOBALS.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Replay every checked-in seed (catches shim/link errors before the
    /// nightly fuzz campaign).
    #[test]
    fn seed_corpus_replays_clean() {
        let _g = lock_c_globals();
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/instrument_diff");
        let mut n = 0;
        for e in std::fs::read_dir(dir).expect("corpus/instrument_diff missing") {
            let p = e.unwrap().path();
            if p.is_file() && p.file_name().is_some_and(|f| f != ".gitkeep") {
                instrument_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n >= 30, "expected >=30 seeds, found {n}");
    }

    /// Per-arm smoke: ok + error shapes per arm.
    #[test]
    fn arms_smoke() {
        let _g = lock_c_globals();
        // Arm 0: garbage + full option-bit matrix (TIMER/BUFFERS/ROWS/WAL).
        for opt in 0u32..16 {
            let mut v = vec![0u8];
            v.extend([0x5Au8; 240]); // constructed-garbage payload
            v.extend(opt.to_le_bytes());
            instrument_diff(&v);
        }
        // Arm 1: timer+buffers+wal cycle: start/stop x2 + endloop.
        let mut v = vec![1u8, 0]; // flags: nonzero clock base
        v.extend(1000u32.to_le_bytes()); // clock base
        v.push(0x0B); // options TIMER|BUFFERS|WAL
        for _ in 0..2 {
            v.extend([0u8, 1]); // start, dt tiny
            v.push(1);
            v.extend([1u8, 2]); // stop, dt medium
            v.extend(2u16.to_le_bytes());
            v.extend([1u8, 3u8]); // ntuples small = 3
            v.extend(3u16.to_le_bytes());
        }
        v.push(3); // endloop
        instrument_diff(&v);
        // Arm 1 error shapes: start-twice; stop-without-start; endloop-running.
        let mut v = vec![1u8, 0];
        v.extend(7u32.to_le_bytes());
        v.push(0x01); // TIMER
        v.extend([0u8, 0, 0u8, 0]); // start, start -> "called twice"
        instrument_diff(&v);
        let mut v = vec![1u8, 0];
        v.extend(7u32.to_le_bytes());
        v.push(0x01);
        v.extend([1u8, 0, 1, 0, 0]); // stop without start
        instrument_diff(&v);
        let mut v = vec![1u8, 0];
        v.extend(7u32.to_le_bytes());
        v.push(0x01);
        v.extend([0u8, 0]); // start
        v.push(3); // endloop on running node... (running=false -> noop)
        v.extend([1u8, 0, 1, 0, 5]); // stop (running := true)
        v.extend([0u8, 0]); // start again
        v.push(3); // endloop -> "called on running node"
        instrument_diff(&v);
        // Arm 1: zero-clock sentinel (flags bit 0) + async mode (bit 1).
        let mut v = vec![1u8, 3];
        v.push(0x01);
        v.extend([0u8, 0, 1, 0, 0]); // start at 0 (LAZY zero), stop
        instrument_diff(&v);
        // Arm 1: injections + wal-global pin ops.
        let mut v = vec![1u8, 0];
        v.extend(50u32.to_le_bytes());
        v.push(0x0A); // BUFFERS|WAL
        v.push(7); // set wal global
        v.extend([0x21u8; 40]);
        v.push(4); // inject bufusage_start
        v.extend([0x42u8; 64]);
        v.push(5); // inject walusage_start
        v.extend([0x17u8; 24]);
        v.extend([0u8, 1, 9]); // start
        v.push(6); // inject accumulators
        v.extend([0x33u8; 90]);
        v.extend([1u8, 2]); // stop
        v.extend(9u16.to_le_bytes());
        v.extend([1u8, 7u8]);
        v.extend(7u16.to_le_bytes());
        instrument_diff(&v);
        // Arm 2: agg over two constructed states, all op combos.
        for ops in 0u8..4 {
            let mut v = vec![2u8, ops];
            v.extend([0x71u8; 500]);
            instrument_diff(&v);
        }
        // Arm 3: arithmetic incl. near-MAX wal_bytes (mode bytes odd).
        let mut v = vec![3u8];
        v.extend([0xFFu8; 600]);
        instrument_diff(&v);
        let mut v = vec![3u8];
        v.extend([0x01u8; 600]);
        instrument_diff(&v);
    }

    /// Wire-LAYOUT PIN (injection R1 closer): a field-order defect applied
    /// symmetrically to the driver's encode AND the C load path fabricates
    /// agreement for the symmetric i64 counters (both sides consume the
    /// same swapped image — the stub:nodes N3 blind class). Pin every wire
    /// offset against hand-written literals so any drift in the Rust
    /// mirror is caught here; one-sided C-side drift is caught by the
    /// differential itself (injection C1).
    #[test]
    fn wire_layout_is_pinned() {
        let mut b = BufferUsage::default();
        b.shared_blks_hit = 0x01;
        b.shared_blks_read = 0x02;
        b.shared_blks_dirtied = 0x03;
        b.shared_blks_written = 0x04;
        b.local_blks_hit = 0x05;
        b.local_blks_read = 0x06;
        b.local_blks_dirtied = 0x07;
        b.local_blks_written = 0x08;
        b.temp_blks_read = 0x09;
        b.temp_blks_written = 0x0A;
        b.shared_blk_read_time = instr_time { ticks: 0x0B };
        b.shared_blk_write_time = instr_time { ticks: 0x0C };
        b.local_blk_read_time = instr_time { ticks: 0x0D };
        b.local_blk_write_time = instr_time { ticks: 0x0E };
        b.temp_blk_read_time = instr_time { ticks: 0x0F };
        b.temp_blk_write_time = instr_time { ticks: 0x10 };
        let w = buf_wire(&b);
        for i in 0..16 {
            assert_eq!(w[i * 8], (i + 1) as u8, "BufferUsage wire slot {i}");
            assert_eq!(&w[i * 8 + 1..i * 8 + 8], [0u8; 7], "slot {i} tail");
        }

        let w = wal_wire(&WalUsage {
            wal_records: 0x11,
            wal_fpi: 0x12,
            wal_bytes: 0x13,
            wal_buffers_full: 0x14,
        });
        assert_eq!((w[0], w[8], w[16], w[24]), (0x11, 0x12, 0x13, 0x14));

        let mut n = Instrumentation::default();
        n.need_timer = true;
        n.async_mode = true;
        n.starttime = instr_time { ticks: 0x21 };
        n.counter = instr_time { ticks: 0x22 };
        n.firsttuple = f64::from_bits(0x23);
        n.tuplecount = f64::from_bits(0x24);
        n.bufusage_start.shared_blks_hit = 0x25;
        n.walusage_start.wal_records = 0x26;
        n.startup = f64::from_bits(0x27);
        n.total = f64::from_bits(0x28);
        n.ntuples = f64::from_bits(0x29);
        n.ntuples2 = f64::from_bits(0x2A);
        n.nloops = f64::from_bits(0x2B);
        n.nfiltered1 = f64::from_bits(0x2C);
        n.nfiltered2 = f64::from_bits(0x2D);
        n.bufusage.temp_blk_write_time = instr_time { ticks: 0x2E };
        n.walusage.wal_buffers_full = 0x2F;
        let w = instr_wire(&n);
        assert_eq!(
            (w[0], w[1], w[2], w[3], w[4]),
            (1, 0, 0, 1, 0),
            "flag bytes"
        );
        for (off, v) in [
            (5usize, 0x21u8),
            (13, 0x22),
            (21, 0x23),
            (29, 0x24),
            (37, 0x25),
            (165, 0x26),
            (197, 0x27),
            (205, 0x28),
            (213, 0x29),
            (221, 0x2A),
            (229, 0x2B),
            (237, 0x2C),
            (245, 0x2D),
            (253 + 15 * 8, 0x2E),
            (381 + 24, 0x2F),
        ] {
            assert_eq!(w[off], v, "Instrumentation wire offset {off}");
        }
    }

    /// stub:clock monotonic-half MUST-FAIL CONTROL (STUBS.md law): (a)
    /// parity through the REAL shipped/vendored consumers under matched
    /// pins, then (b) a deliberate ONE-SIDED clock advance the counter
    /// field plane MUST see.
    #[test]
    fn control_clock_mono_pin() {
        // Lock order everywhere in this file: lock_c_globals() FIRST, then
        // the oracle guard (the driver takes oracle_serial() under the same
        // ordering when called with lock_c_globals held).
        let _g = lock_c_globals();
        let _oracle = crate::c_oracle_serial();
        init_once();
        // (a) matched pins through a full timed cycle.
        unsafe { pg_instrbe_reset() };
        pin_wal_global(&WalUsage::default());
        let mut n = Instrumentation::default();
        unsafe { pg_instrbe_init(1) }; // TIMER
        instr_init(&mut n, 1);
        crate::stubs::clock::pin_mono_ns(5_000);
        assert_eq!(unsafe { pg_instrbe_start_node() }, 0);
        instr_start_node(&mut n);
        crate::stubs::clock::pin_mono_ns(9_000);
        assert_eq!(unsafe { pg_instrbe_stop_node(1.0f64.to_bits()) }, 0);
        instr_stop_node(&mut n, 1.0);
        compare_node(0, &n, "control(a): matched pins");
        assert_eq!(n.counter.ticks, 4_000);

        // (b) one-sided pin: advance ONLY the Rust cell before the second
        // cycle's stop; the C side keeps the older reading => counter
        // fields diverge and the comparator MUST flag it.
        crate::stubs::clock::pin_mono_ns(20_000);
        assert_eq!(unsafe { pg_instrbe_start_node() }, 0);
        instr_start_node(&mut n);
        crate::stubs::clock::pin_mono_ns(21_000);
        pg_clock::fuzz_mono_pin::set(900_000); // Rust side ONLY
        let cv = unsafe { pg_instrbe_stop_node(1.0f64.to_bits()) };
        assert_eq!(cv, 0);
        instr_stop_node(&mut n, 1.0);
        let caught = catch_unwind(AssertUnwindSafe(|| {
            compare_node(0, &n, "control(b): one-sided clock pin");
        }));
        assert!(
            caught.is_err(),
            "mismatched mono pins MUST be visible (timer plane dead?)"
        );
        crate::stubs::clock::pin_mono_ns(0); // re-align for later tests
    }

    /// WAL-global pin MUST-FAIL CONTROL: (a) parity with matched pins
    /// through the REAL global-read path (walusage_start snapshot +
    /// WalUsageAccumDiff), then (b) a one-sided C-only global advance the
    /// walusage field plane MUST see.
    #[test]
    fn control_wal_global_pin() {
        let _g = lock_c_globals();
        let _oracle = crate::c_oracle_serial();
        init_once();
        unsafe { pg_instrbe_reset() };
        let mut n = Instrumentation::default();
        unsafe { pg_instrbe_init(8) }; // WAL
        instr_init(&mut n, 8);
        let w1 = WalUsage { wal_records: 3, wal_fpi: 1, wal_bytes: 100, wal_buffers_full: 0 };
        pin_wal_global(&w1);
        crate::stubs::clock::pin_mono_ns(0);
        assert_eq!(unsafe { pg_instrbe_start_node() }, 0);
        instr_start_node(&mut n);
        let w2 = WalUsage { wal_records: 7, wal_fpi: 1, wal_bytes: 400, wal_buffers_full: 2 };
        pin_wal_global(&w2);
        assert_eq!(unsafe { pg_instrbe_stop_node(1.0f64.to_bits()) }, 0);
        instr_stop_node(&mut n, 1.0);
        compare_node(0, &n, "control(a): matched wal pins");
        assert_eq!(n.walusage.wal_bytes, 300);

        // (b) advance ONLY the C global before another cycle.
        assert_eq!(unsafe { pg_instrbe_start_node() }, 0);
        instr_start_node(&mut n);
        let w3 = WalUsage { wal_records: 9, wal_fpi: 5, wal_bytes: 999, wal_buffers_full: 2 };
        unsafe { pg_instrbe_set_wal_global(wal_wire(&w3).as_ptr()) }; // C ONLY
        assert_eq!(unsafe { pg_instrbe_stop_node(1.0f64.to_bits()) }, 0);
        instr_stop_node(&mut n, 1.0);
        let caught = catch_unwind(AssertUnwindSafe(|| {
            compare_node(0, &n, "control(b): one-sided wal global");
        }));
        assert!(
            caught.is_err(),
            "mismatched wal-global pins MUST be visible (walusage plane dead?)"
        );
        pin_wal_global(&WalUsage::default());
    }

    /// C-branch-executes witness for the error plane: force the C-side
    /// elog on its own and observe the verdict + captured message literal
    /// (proves the setjmp/elog channel is live, not comparing Ok vs Ok).
    #[test]
    fn c_error_branch_executes() {
        let _g = lock_c_globals();
        let _oracle = crate::c_oracle_serial();
        init_once();
        unsafe { pg_instrbe_reset() };
        unsafe { pg_instrbe_init(1) }; // TIMER
        let cv = unsafe { pg_instrbe_stop_node(1.0f64.to_bits()) };
        assert_eq!(cv, 1, "C InstrStopNode without start must elog");
        assert_eq!(c_errmsg(), "InstrStopNode called without start");
        // And the twice-start arm.
        unsafe { pg_instrbe_reset() };
        unsafe { pg_instrbe_init(1) };
        crate::stubs::clock::pin_mono_ns(77);
        assert_eq!(unsafe { pg_instrbe_start_node() }, 0);
        assert_eq!(unsafe { pg_instrbe_start_node() }, 1);
        assert_eq!(c_errmsg(), "InstrStartNode called twice in a row");
        crate::stubs::clock::pin_mono_ns(0);
    }
}
