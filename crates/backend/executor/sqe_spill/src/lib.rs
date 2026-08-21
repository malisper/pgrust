//! # sqe_spill — buffer-managed spill pages (charter C5, chunk M2-B)
//!
//! **Copy provenance (the copy-first law):** this crate is the lanev4 carry
//! of `origin/lanev3` `crates/backend/executor/lx/lx_spill/` @
//! `dc3c67c56214` — mechanism bodies, laws, and the full acceptance battery
//! verbatim; the re-keys are the crate name (lx_spill → sqe_spill) and doc
//! references (lx_vec → pgrc2_batch, whose StrCell/StrViews carry the same
//! contract). Landed with the M5e breadth lane's L2 (the file-backed
//! BatchStore) as the v4 spill substrate; its first consumer is
//! `the join family (P4-2d)::fstore`. The v3 milestone/chunk citations in the docs below
//! are kept as historical provenance.
//!
//! The spill SUBSTRATE the M2 wave-3 consumers build against: M2-K (agg
//! partial-state spill at the lx_agg pressure witness), M2-L (grace-hash
//! `BatchStore` implementor), M2-H (sort run storage). One crate, three
//! coupled pieces:
//!
//! - **[`set`]** — file lifecycle: [`SpillSet`] (an `fd::FileSet` under
//!   `pgsql_tmp`, per O-M2-3), [`SpillFile`] plain-data descriptors,
//!   event-scoped [`SpillWriter`]/[`SpillReader`], and the extent
//!   directories ([`SpillExtent`]/[`EpochDir`]/[`StreamDir`]) that ride
//!   sink Locals as plain `Send` data.
//! - **[`page`]** — the row-layout page format (charter C5 law: fixed-width
//!   rows and var-len data on SEPARATE pages) and the [`VarRef`] pointer
//!   swizzle vocabulary.
//! - **[`pool`]** — the PRIVATE work_mem-accounted page pool (O-M2-4:
//!   shared_buffers integration DECLINED), with pin/unload/reload and the
//!   batch-scoped swizzle discipline.
//!
//! ## Ruling provenance (binding)
//!
//! - **O-M2-3** (spill residency): files are fd `FileSet` segments under
//!   `pgsql_tmp`. temp_file_limit parity is INHERITED, not rebuilt — every
//!   create goes through `PathNameCreateTemporaryFile`, which sets
//!   `FD_TEMP_FILE_LIMIT` (accounted on the creating thread = per
//!   participant under thread-per-worker) and registers the VFD with the
//!   current resource owner; crash cleanup rides the existing `pgsql_tmp`
//!   startup reaper (`fd::RemovePgTempFiles`). Pinned by test
//!   (`tests::limits`, `tests::teardown`).
//! - **O-M2-4** (memory ownership): the page pool is engagement-private and
//!   charged against a byte budget the CONSUMER derives from the C-parity
//!   work_mem formulas (the M2-D faces); this crate never reads a GUC for
//!   its budget and never touches the buffer manager.
//! - **State-pointer lifetime law** (`lanev3-jit-fusion.md` §4): pointers
//!   materialized by [`pool::SpillPool::swizzle`] are valid until the token
//!   is returned; swizzle/unswizzle/eviction happen only BETWEEN batches at
//!   staging points. The pool makes this structural: swizzled pages are
//!   pinned by their token, pinned pages never move or unload, and the
//!   token is `!Send` + `#[must_use]`.
//! - **m3.5-spill.md §2 (inc-1 amendment)**: NO file handle survives an
//!   I/O event. Writers open (create or open-by-name at committed EOF),
//!   write, flush-close within one event on the owning thread; readers
//!   open/read/close within one task. Between events every spill object is
//!   plain data.
//!
//! ## Standing-law compliance (§7.1, reviewed per-item on the PR)
//!
//! - **TLS ratchet**: this crate declares ZERO `thread_local!`s. Thread
//!   affinity exists only inside fd's own VFD cache (its census rows) and
//!   the runtime's already-classified facade registration.
//! - **Session context is a passed capability**: budgets, filesets, and
//!   metrics are constructor arguments / owned fields; nothing reads
//!   MyProc-adjacent TLS.
//! - **No per-work-item ceremony**: events are flush/epoch-grain, never
//!   per row; budget checks are the consumer's at morsel/flush cadence.
//! - **No new thread populations; claim-channel laws**: this crate spawns
//!   nothing and shares NO mutable state across threads — single-owner
//!   structures plus frozen-before-read files opened by name (the deps-DAG
//!   happens-before edge every sink already relies on). There is no new
//!   loom obligation; the blocking-section facade's model lives with the
//!   runtime (`runtime/tests/loom.rs`, facade_standby_absorption).
//!
//! ## The teardown contract (delete-all on EVERY path)
//!
//! Spill artifacts are engagement-scoped: the [`SpillSet`] rides the
//! engagement payload (an `Arc` whose last drop is the payload's), and
//! `FileSet::drop = delete_all()` removes the on-disk directory tree. The
//! five paths, enumerated:
//!
//! 1. **Normal completion** — payload drop → `FileSet::delete_all`. Pins
//!    and swizzle tokens are already returned (debug-asserted at pool
//!    drop); writer/reader handles never outlive their event.
//! 2. **ERROR (ereport unwind)** — drop glue closes any mid-event handle
//!    ([`SpillWriter`]/[`SpillReader`] Drop; an abandoned mid-epoch write
//!    COMMITS NOTHING — the committed watermark moves only at `finish`),
//!    the resowner belt (`FD_CLOSE_AT_EOXACT` registration) closes any VFD
//!    the unwind skipped, and the payload drop deletes the files.
//! 3. **Statement cancel** — identical to path 2 by construction (cancel
//!    is an ERROR-class unwind); enumerated separately because the
//!    acceptance bar names it.
//! 4. **FATAL / proc_exit** — Drop glue runs during thread teardown with
//!    `proc_exit_inprogress` set: handle Drops SKIP `FileClose` (the
//!    resowner release already freed every temp VFD; flushing through dead
//!    Files aborted a postmaster once — the buffile precedent), while
//!    `delete_all` still unlinks by PATH (no VFD dependency). The startup
//!    reaper is the belt behind it.
//! 5. **Crash-restart (no destructor ran)** — `fd::RemovePgTempFiles` at
//!    startup removes every `pgsql_tmp*` entry, including
//!    `pgsql_tmp<pid>.<set>.fileset` directories, recursively. Pinned by
//!    test.
//!
//! ## Byref state classes (the 9-incident class's new home turf)
//!
//! Every teardown/yield/migration contract in this crate enumerates the
//! byref state it strands ([`ByrefClass`], with the exhaustive-match belt
//! [`byref_teardown_home`] — a new state kind FAILS COMPILE until it
//! declares a home). The classes:
//!
//! - [`ByrefClass::PagePin`] — raw page addresses ([`pool::PagePin`]):
//!   task-scoped, `!Send`, returned by value to `unpin`; unload/eviction
//!   refuse pinned pages, so a live pin can never dangle. Stranding home:
//!   pool drop frees frames regardless; debug builds assert zero live pins.
//! - [`ByrefClass::SwizzledRefs`] — raw payload addresses written INTO row
//!   pages by `swizzle`: batch-scoped, reversed by `unswizzle(token)`; the
//!   token pins source and target pages, and on-disk images are unswizzled
//!   by construction (a swizzled page is pinned, and pinned pages never
//!   unload). Stranding home: token must be consumed before seal/unload;
//!   the pool errors on unloading a swizzled page.
//! - [`ByrefClass::OpenHandle`] — VFD handles inside an event
//!   ([`SpillWriter`]/[`SpillReader`]): thread-affine, event-scoped, closed
//!   by Drop (proc_exit-guarded) with the resowner registration as belt.
//!   Never crosses threads, never rides a Local.
//! - [`ByrefClass::OnDiskBytes`] — the files themselves plus the fileset
//!   directory: engagement-scoped, owned by the [`SpillSet`]; teardown per
//!   the five-path contract above.
//! - [`ByrefClass::PlainDirectory`] — extent directories and [`SpillFile`]
//!   descriptors riding Locals/Seals: plain `Send + Sync` data, no
//!   teardown action; a descriptor whose files were already deleted FAILS
//!   CLOSED at open (ENOENT is an error on the read path, never UB).
//! - [`ByrefClass::StrView`] — 16-byte string-view cells and the payload
//!   pointers inside them (M4-N, `lanev3-strview.md` §4 — "StrView is
//!   added to the byref-state enumeration vocabulary"): batch-claim-scoped
//!   overlays in `pgrc2_batch`, dropped by `Batch::begin` (R4); retention
//!   copies payload bytes at the consumer; spill NEVER sees a cell — pages
//!   serialize payloads, never pointers (the swizzle law), so a StrView on
//!   the spill plane is structurally unreachable and fails closed.
//!
//! ## Consumer map (the wave-3 API surface)
//!
//! - **M2-K agg spill**: accept-side epoch flush = `SpillWriter` +
//!   partition-contiguous extents recorded in an [`EpochDir`]; combine
//!   streams a partition's extents from every Local's file by name
//!   (`SpillReader`); combine-side working set and var-len state images
//!   ride the [`pool::SpillPool`] with swizzle at batch staging points.
//! - **M2-L grace join**: the `BatchStore` implementor appends the frozen
//!   join-batch record bytes ([u32 hash][u32 len][payload][pad 8] — the
//!   nodehashjoin contract, golden-tested here) through `SpillWriter` and
//!   streams batches back through `SpillReader`. Storage swap, never bytes.
//! - **M2-H sort**: a run = one extent of length-prefixed records written
//!   at seal; merge reads run slices via `read_at` (fence indexes are the
//!   consumer's, built from `SpillWriter::tell` samples).
//!
//! ## Telemetry vocabulary (M2-J binds this)
//!
//! [`SpillMetrics`] is the measured-only counter set. The frozen
//! `lx_stats::SpillCounters` triple maps as: `spill_events` ⇐
//! consumer-counted flush events, `spilled_bytes` ⇐ `bytes_written`,
//! `spilled_partitions` ⇐ consumer-counted from its extent directories.
//! M2-J appends fields there; this crate never depends on lx_stats.

pub mod page;
pub mod pool;
pub mod set;

pub(crate) mod io;

#[cfg(test)]
mod tests;

pub use page::{PageHdr, PageKind, RowLayout, VarRef, MAX_ROW_REFS, PAGE_SIZE};
pub use pool::{PagePin, SpillPool, SwizzleToken};
pub use set::{
    spill_file_name, EpochDir, SpillExtent, SpillFile, SpillReader, SpillSet, SpillWriter,
    StreamDir,
};

/// The byref state classes this crate can strand (crate docs, "Byref state
/// classes"). The enum is the EXHAUSTIVE-MATCH BELT (the PR #161 pattern):
/// adding a variant refuses to compile until [`byref_teardown_home`]
/// declares its teardown home, and the doc contract above enumerates it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ByrefClass {
    /// Raw page address held by a [`pool::PagePin`].
    PagePin,
    /// Raw payload addresses swizzled into row-page ref words.
    SwizzledRefs,
    /// An open VFD inside one I/O event (writer/reader).
    OpenHandle,
    /// Spill files + the fileset directory tree on disk.
    OnDiskBytes,
    /// Extent directories / file descriptors riding Locals as plain data.
    PlainDirectory,
    /// String-view cells (16-byte `pgrc2_batch::StrCell`) and their payload
    /// pointers (M4-N): batch-claim-scoped, never spillable.
    StrView,
}

/// The declared teardown home of each byref class — the exhaustive match
/// that forces every NEW state kind to declare where it dies. Consumers'
/// teardown contracts (M2-K/L/H) cite these strings verbatim.
pub fn byref_teardown_home(class: ByrefClass) -> &'static str {
    match class {
        ByrefClass::PagePin => {
            "task-scoped: returned by value to SpillPool::unpin before the staging point; \
             pool drop frees frames; debug-asserted zero at drop"
        }
        ByrefClass::SwizzledRefs => {
            "batch-scoped: reversed by SpillPool::unswizzle(token) at the staging point; \
             token pins its pages so unload/eviction cannot race it"
        }
        ByrefClass::OpenHandle => {
            "event-scoped: closed by writer/reader Drop (proc_exit-guarded); \
             resowner FD_CLOSE_AT_EOXACT registration is the unwind belt"
        }
        ByrefClass::OnDiskBytes => {
            "engagement-scoped: SpillSet/FileSet delete_all on drop (normal/error/cancel/FATAL); \
             pgsql_tmp startup reaper on crash-restart"
        }
        ByrefClass::PlainDirectory => {
            "plain Send data riding Locals: dies with its Local; \
             stale extents fail closed at open (ENOENT)"
        }
        ByrefClass::StrView => {
            "batch-claim-scoped: cells die at Batch::begin (R4) / arena reset; \
             retention copies payload bytes at the consumer (varlena discipline); \
             never serialized — pages carry payloads, never pointers, so a cell \
             on the spill plane fails closed"
        }
    }
}

/// Measured-only spill telemetry (crate docs, "Telemetry vocabulary").
/// Owned per engagement participant (pool and consumers each hold one and
/// [`SpillMetrics::merge`] folds them at seal) — a number we didn't measure
/// is a number we don't print.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SpillMetrics {
    /// Consumer-counted flush/epoch events (⇒ `SpillCounters::spill_events`).
    pub spill_events: u64,
    /// Bytes written to spill files (⇒ `SpillCounters::spilled_bytes`).
    pub bytes_written: u64,
    /// Bytes read back from spill files.
    pub bytes_read: u64,
    /// Extents committed through writers (partition-grain provenance).
    pub extents_written: u64,
    /// Pool pages written out (unload/eviction write-backs).
    pub pages_unloaded: u64,
    /// Pool pages read back in on pin.
    pub pages_reloaded: u64,
    /// Pool evictions forced by the budget (unload minus explicit calls).
    pub pool_evictions: u64,
    /// High-water resident bytes in the pool (budget witness).
    pub peak_resident_bytes: u64,
}

impl SpillMetrics {
    /// Fold `other` into `self` (peak = max, everything else additive).
    pub fn merge(&mut self, other: &SpillMetrics) {
        self.spill_events += other.spill_events;
        self.bytes_written += other.bytes_written;
        self.bytes_read += other.bytes_read;
        self.extents_written += other.extents_written;
        self.pages_unloaded += other.pages_unloaded;
        self.pages_reloaded += other.pages_reloaded;
        self.pool_evictions += other.pool_evictions;
        self.peak_resident_bytes = self.peak_resident_bytes.max(other.peak_resident_bytes);
    }
}
