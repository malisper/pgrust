//! The per-kind callback table — the owner-crate half of `PgStat_KindInfo`.
//!
//! C's `PgStat_KindInfo` (`utils/pgstat_internal.h`) bundles two things: scalar
//! metadata (sizes/offsets/flags/name) and a set of `*_cb` function pointers
//! into the per-kind implementation files. The metadata half lives in
//! `types_pgstat::pgstat_internal::PgStat_KindInfo` (callback-free, so it stays
//! in the types vocabulary without a `PgResult` / owner dependency). This file
//! holds the *function-pointer* half: [`PgStat_KindCallbacks`], plus the full
//! per-kind descriptor [`PgStat_KindInfoFull`] that pairs them, and the builtin
//! table `pgstat_kind_builtin_infos[]` that `pgstat.c` indexes by kind.
//!
//! This mirrors the established cross-layer pattern: `types-pgstat` never names
//! a callback; the owner crate assembles the real table, and each per-kind
//! owner crate registers its own `*_cb` functions into it.
//!
//! ## Callback model vs. C
//!
//! C erases every callback through `void *` (`init_shmem_cb(void *stats)`,
//! `flush_pending_cb(PgStat_EntryRef *, bool)`, `snapshot_cb(void)` reaching
//! `pgStatLocal` globals). Two faithful adaptations are needed for the typed,
//! `PgResult`-returning Rust ports:
//!
//! * **Failure surface.** The Rust per-kind ports return `PgResult<()>` /
//!   `PgResult<bool>` where the C callback is `void` / `bool` but can
//!   `ereport(ERROR)` (e.g. `LWLockAcquire`). The table stores the
//!   `PgResult`-returning shapes (per "seam signatures mirror the C failure
//!   surface").
//!
//! * **Fixed-kind shmem projection.** C's `init_shmem_cb` / `reset_all_cb` /
//!   `snapshot_cb` reach the kind's fixed region (`init_shmem_cb` is handed the
//!   `void *` region; the others reach `pgStatLocal.shmem`/`.snapshot`). The
//!   per-kind Rust ports take the *typed* region (`&mut PgStatShared_BgWriter`)
//!   or reach typed snapshot slots. The table therefore stores adapter shapes
//!   keyed on the owner's [`PgStat_ShmemControl`] / [`PgStat_Snapshot`]; each
//!   per-kind crate supplies a thin adapter that projects its field and calls
//!   its typed `*_cb`. The adapter is owner glue, exactly where C's static
//!   table lives.

use types_core::TimestampTz;
use types_error::PgResult;
use types_pgstat::activity_pgstat::{PgStat_Kind, PGSTAT_KIND_BUILTIN_MIN, PGSTAT_KIND_BUILTIN_SIZE};
use types_pgstat::pgstat_internal::{
    PgStat_HashKey, PgStat_KindInfo, PgStat_ShmemControl, PgStat_Snapshot, PgStatShared_Common,
};

use crate::entry_ref::PgStat_EntryRef;

/// The function-pointer half of C's `PgStat_KindInfo`. Every field is `None`
/// when the corresponding C member is `NULL` (callback not provided for that
/// kind).
///
/// Each closure-friendly `fn` pointer mirrors one C callback. `Box<dyn Fn ...>`
/// is used (rather than bare `fn`) so per-kind crates can register an adapter
/// that closes over the field-projection from the owner [`PgStat_ShmemControl`]
/// / [`PgStat_Snapshot`] — the type erasure that replaces C's `void *`.
#[derive(Default)]
pub struct PgStat_KindCallbacks {
    /// `void (*init_backend_cb)(void)` — per-backend init. Optional.
    pub init_backend_cb: Option<Box<dyn Fn() -> PgResult<()> + Send + Sync>>,

    /// `bool (*flush_pending_cb)(PgStat_EntryRef *sr, bool nowait)` — flush a
    /// variable-numbered entry's pending data. Returns `true` if it could not
    /// be flushed (lock contention). Required if the kind uses pending data.
    pub flush_pending_cb:
        Option<Box<dyn Fn(&mut PgStat_EntryRef, bool) -> PgResult<bool> + Send + Sync>>,

    /// `void (*delete_pending_cb)(PgStat_EntryRef *sr)` — drop pending data.
    /// Optional.
    pub delete_pending_cb: Option<Box<dyn Fn(&mut PgStat_EntryRef) + Send + Sync>>,

    /// `void (*reset_timestamp_cb)(PgStatShared_Common *header, TimestampTz ts)`
    /// — reset the reset timestamp of a variable-numbered entry. Optional.
    pub reset_timestamp_cb:
        Option<Box<dyn Fn(&mut PgStatShared_Common, TimestampTz) + Send + Sync>>,

    /// `void (*to_serialized_name)(const PgStat_HashKey *, const
    /// PgStatShared_Common *, NameData *)` — derive the on-disk serialized name.
    /// Optional. Modeled as returning the name string.
    pub to_serialized_name:
        Option<Box<dyn Fn(&PgStat_HashKey, &PgStatShared_Common) -> String + Send + Sync>>,

    /// `bool (*from_serialized_name)(const NameData *, PgStat_HashKey *)` —
    /// parse a serialized name back into a key. Optional. Returns `Some(key)`
    /// on success (C `true` + filled key), `None` on failure (C `false`).
    pub from_serialized_name: Option<Box<dyn Fn(&str) -> Option<PgStat_HashKey> + Send + Sync>>,

    /// `void (*init_shmem_cb)(void *stats)` — initialize a fixed kind's shared
    /// region. The adapter projects the right field of [`PgStat_ShmemControl`].
    pub init_shmem_cb: Option<Box<dyn Fn(&mut PgStat_ShmemControl) + Send + Sync>>,

    /// `bool (*flush_static_cb)(bool nowait)` — flush pending stats for kinds
    /// that do not use `PgStat_EntryRef->pending`. Returns `true` if some could
    /// not be flushed. Optional.
    pub flush_static_cb: Option<Box<dyn Fn(bool) -> PgResult<bool> + Send + Sync>>,

    /// `void (*reset_all_cb)(TimestampTz ts)` — reset a fixed kind's stats. The
    /// adapter projects the field of [`PgStat_ShmemControl`].
    pub reset_all_cb:
        Option<Box<dyn Fn(&mut PgStat_ShmemControl, TimestampTz) -> PgResult<()> + Send + Sync>>,

    /// `void (*snapshot_cb)(void)` — build a fixed kind's snapshot. The adapter
    /// reads `pgStatLocal.shmem` and writes `pgStatLocal.snapshot`; modeled as
    /// taking both projected owner structures.
    pub snapshot_cb: Option<
        Box<dyn Fn(&PgStat_ShmemControl, &mut PgStat_Snapshot) -> PgResult<()> + Send + Sync>,
    >,
}

impl core::fmt::Debug for PgStat_KindCallbacks {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        // Function pointers are not Debug; report which slots are populated.
        f.debug_struct("PgStat_KindCallbacks")
            .field("init_backend_cb", &self.init_backend_cb.is_some())
            .field("flush_pending_cb", &self.flush_pending_cb.is_some())
            .field("delete_pending_cb", &self.delete_pending_cb.is_some())
            .field("reset_timestamp_cb", &self.reset_timestamp_cb.is_some())
            .field("to_serialized_name", &self.to_serialized_name.is_some())
            .field("from_serialized_name", &self.from_serialized_name.is_some())
            .field("init_shmem_cb", &self.init_shmem_cb.is_some())
            .field("flush_static_cb", &self.flush_static_cb.is_some())
            .field("reset_all_cb", &self.reset_all_cb.is_some())
            .field("snapshot_cb", &self.snapshot_cb.is_some())
            .finish()
    }
}

/// One full per-kind descriptor: the scalar metadata
/// (`types_pgstat::PgStat_KindInfo`) paired with its callbacks. Together these
/// are exactly C's `PgStat_KindInfo` entry, split so the metadata can live in
/// the callback-free types crate.
#[derive(Debug)]
pub struct PgStat_KindInfoFull {
    /// The scalar metadata half (sizes/offsets/flags/name).
    pub info: PgStat_KindInfo,
    /// The function-pointer half.
    pub cb: PgStat_KindCallbacks,
}

/// `pgstat_kind_builtin_infos[PGSTAT_KIND_BUILTIN_SIZE]` (`pgstat.c`) — the
/// builtin per-kind table. Indexed by `PgStat_Kind` value (entries below
/// `PGSTAT_KIND_BUILTIN_MIN` are unused, matching C's sparse designated array).
///
/// Built once at startup by [`crate::registry::pgstat_kind_infos`]; per-kind
/// crates fill their slot through [`KindInfoBuilder`].
pub struct PgStat_KindInfoTable {
    slots: [Option<PgStat_KindInfoFull>; PGSTAT_KIND_BUILTIN_SIZE],
}

impl PgStat_KindInfoTable {
    /// An empty table (all slots `NULL`, as the C array is before its
    /// designated initializers run).
    pub fn new() -> Self {
        PgStat_KindInfoTable {
            slots: core::array::from_fn(|_| None),
        }
    }

    /// Install one builtin kind's full descriptor. Panics if the kind is out of
    /// the builtin range or already registered (a static table is built once).
    pub fn register(&mut self, kind: PgStat_Kind, full: PgStat_KindInfoFull) {
        assert!(
            kind.is_builtin(),
            "pgstat kind {:?} is not a builtin kind",
            kind
        );
        let idx = kind.0 as usize;
        assert!(
            self.slots[idx].is_none(),
            "pgstat builtin kind {:?} registered twice",
            kind
        );
        self.slots[idx] = Some(full);
    }

    /// `pgstat_get_kind_info(kind)` (`pgstat.c`), builtin half — the per-kind
    /// descriptor, or `None` if the kind is not a registered builtin.
    pub fn get(&self, kind: PgStat_Kind) -> Option<&PgStat_KindInfoFull> {
        if !kind.is_builtin() {
            return None;
        }
        self.slots[kind.0 as usize].as_ref()
    }

    /// Test-only: remove and return a registered kind's descriptor.
    #[cfg(test)]
    pub(crate) fn take_for_test(&mut self, kind: PgStat_Kind) -> Option<PgStat_KindInfoFull> {
        if !kind.is_builtin() {
            return None;
        }
        self.slots[kind.0 as usize].take()
    }

    /// Iterate every registered builtin kind, in ascending kind order — the
    /// loop shape `pgstat.c` uses (`for kind in PGSTAT_KIND_BUILTIN_MIN..`).
    pub fn iter(&self) -> impl Iterator<Item = (PgStat_Kind, &PgStat_KindInfoFull)> {
        self.slots
            .iter()
            .enumerate()
            .skip(PGSTAT_KIND_BUILTIN_MIN.0 as usize)
            .filter_map(|(idx, slot)| slot.as_ref().map(|f| (PgStat_Kind(idx as u32), f)))
    }
}

impl Default for PgStat_KindInfoTable {
    fn default() -> Self {
        Self::new()
    }
}

/// The registration API a per-kind owner crate uses to fill its builtin slot.
///
/// A per-kind crate constructs one of these (metadata + the callbacks it
/// provides) and hands it to [`crate::registry::register_builtin_kind`]. This is
/// the single entry point per-kind crates call; the F0 carrier wires the three
/// already-ported fixed kinds (bgwriter / archiver / checkpointer) through it as
/// proof of shape.
pub struct KindInfoBuilder {
    kind: PgStat_Kind,
    info: PgStat_KindInfo,
    cb: PgStat_KindCallbacks,
}

impl KindInfoBuilder {
    /// Begin describing builtin `kind` with its scalar metadata.
    pub fn new(kind: PgStat_Kind, info: PgStat_KindInfo) -> Self {
        KindInfoBuilder {
            kind,
            info,
            cb: PgStat_KindCallbacks::default(),
        }
    }

    pub fn init_backend_cb(
        mut self,
        f: impl Fn() -> PgResult<()> + Send + Sync + 'static,
    ) -> Self {
        self.cb.init_backend_cb = Some(Box::new(f));
        self
    }

    pub fn flush_pending_cb(
        mut self,
        f: impl Fn(&mut PgStat_EntryRef, bool) -> PgResult<bool> + Send + Sync + 'static,
    ) -> Self {
        self.cb.flush_pending_cb = Some(Box::new(f));
        self
    }

    pub fn delete_pending_cb(
        mut self,
        f: impl Fn(&mut PgStat_EntryRef) + Send + Sync + 'static,
    ) -> Self {
        self.cb.delete_pending_cb = Some(Box::new(f));
        self
    }

    pub fn reset_timestamp_cb(
        mut self,
        f: impl Fn(&mut PgStatShared_Common, TimestampTz) + Send + Sync + 'static,
    ) -> Self {
        self.cb.reset_timestamp_cb = Some(Box::new(f));
        self
    }

    pub fn to_serialized_name(
        mut self,
        f: impl Fn(&PgStat_HashKey, &PgStatShared_Common) -> String + Send + Sync + 'static,
    ) -> Self {
        self.cb.to_serialized_name = Some(Box::new(f));
        self
    }

    pub fn from_serialized_name(
        mut self,
        f: impl Fn(&str) -> Option<PgStat_HashKey> + Send + Sync + 'static,
    ) -> Self {
        self.cb.from_serialized_name = Some(Box::new(f));
        self
    }

    pub fn init_shmem_cb(
        mut self,
        f: impl Fn(&mut PgStat_ShmemControl) + Send + Sync + 'static,
    ) -> Self {
        self.cb.init_shmem_cb = Some(Box::new(f));
        self
    }

    pub fn flush_static_cb(
        mut self,
        f: impl Fn(bool) -> PgResult<bool> + Send + Sync + 'static,
    ) -> Self {
        self.cb.flush_static_cb = Some(Box::new(f));
        self
    }

    pub fn reset_all_cb(
        mut self,
        f: impl Fn(&mut PgStat_ShmemControl, TimestampTz) -> PgResult<()> + Send + Sync + 'static,
    ) -> Self {
        self.cb.reset_all_cb = Some(Box::new(f));
        self
    }

    pub fn snapshot_cb(
        mut self,
        f: impl Fn(&PgStat_ShmemControl, &mut PgStat_Snapshot) -> PgResult<()>
            + Send
            + Sync
            + 'static,
    ) -> Self {
        self.cb.snapshot_cb = Some(Box::new(f));
        self
    }

    /// Finalize into a [`PgStat_KindInfoFull`] and its target kind.
    pub fn build(self) -> (PgStat_Kind, PgStat_KindInfoFull) {
        (
            self.kind,
            PgStat_KindInfoFull {
                info: self.info,
                cb: self.cb,
            },
        )
    }
}
