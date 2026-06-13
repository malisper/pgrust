//! `commands/vacuum.h` + `access/heapam.h` command-layer vocabulary: the
//! VACUUM parameter struct, the tri-state option enum, the computed cutoffs,
//! and the per-page prune/freeze result.

use types_core::{bits32, MultiXactId, OffsetNumber, Oid, TransactionId};
use types_storage::bufpage::MaxHeapTuplesPerPage;

/// `typedef enum VacOptValue` (`commands/vacuum.h`) — tri-state for the
/// `index_cleanup` / `truncate` VACUUM options. Discriminants match the C enum.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(i32)]
pub enum VacOptValue {
    #[default]
    VACOPTVALUE_UNSPECIFIED = 0,
    VACOPTVALUE_AUTO,
    VACOPTVALUE_DISABLED,
    VACOPTVALUE_ENABLED,
}

/// `typedef struct VacuumParams` (`commands/vacuum.h`) — parameters customizing
/// VACUUM/ANALYZE behavior. Field order matches the C struct (PostgreSQL 18.3).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct VacuumParams {
    /// `bits32 options` — bitmask of `VACOPT_*`.
    pub options: bits32,
    /// `int freeze_min_age` — min freeze age, -1 to use default.
    pub freeze_min_age: i32,
    /// `int freeze_table_age` — age at which to scan whole table.
    pub freeze_table_age: i32,
    /// `int multixact_freeze_min_age` — min multixact freeze age, -1 default.
    pub multixact_freeze_min_age: i32,
    /// `int multixact_freeze_table_age` — multixact age to scan whole table.
    pub multixact_freeze_table_age: i32,
    /// `bool is_wraparound` — force a for-wraparound vacuum.
    pub is_wraparound: bool,
    /// `int log_min_duration` — min execution threshold (ms) for logging.
    pub log_min_duration: i32,
    /// `VacOptValue index_cleanup` — do index vacuum and cleanup.
    pub index_cleanup: VacOptValue,
    /// `VacOptValue truncate` — truncate empty pages at the end.
    pub truncate: VacOptValue,
    /// `Oid toast_parent` — for privilege checks when recursing.
    pub toast_parent: Oid,
    /// `double max_eager_freeze_failure_rate` — eager-scan fail fraction (0 off).
    pub max_eager_freeze_failure_rate: f64,
    /// `int nworkers` — number of parallel vacuum workers (0 auto, -1 disabled).
    pub nworkers: i32,
}

/// `struct VacuumCutoffs` (`access/heapam.h`) — the freeze/removal cutoffs
/// computed by `vacuum_get_cutoffs`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct VacuumCutoffs {
    /// Existing `pg_class.relfrozenxid` at VACUUM start.
    pub relfrozenxid: TransactionId,
    /// Existing `pg_class.relminmxid` at VACUUM start.
    pub relminmxid: MultiXactId,
    /// XID below which committed-deleted tuples are DEAD (not RECENTLY_DEAD).
    pub OldestXmin: TransactionId,
    /// MXID below which multis are invisible to all running transactions.
    pub OldestMxact: MultiXactId,
    /// XID below which all XIDs are definitely frozen/removed.
    pub FreezeLimit: TransactionId,
    /// MXID below which all multis are definitely removed from xmax.
    pub MultiXactCutoff: MultiXactId,
}

/// `reason` codes for `heap_page_prune_and_freeze()` (`access/heapam.h`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum PruneReason {
    /// on-access pruning
    PRUNE_ON_ACCESS = 0,
    /// VACUUM 1st heap pass
    PRUNE_VACUUM_SCAN,
    /// VACUUM 2nd heap pass
    PRUNE_VACUUM_CLEANUP,
}

/// `struct PruneFreezeResult` (`access/heapam.h`) — per-page state returned by
/// `heap_page_prune_and_freeze()`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PruneFreezeResult {
    /// Number of tuples deleted from the page.
    pub ndeleted: i32,
    /// Number of newly LP_DEAD items.
    pub nnewlpdead: i32,
    /// Number of tuples we froze.
    pub nfrozen: i32,
    /// Number of live tuples on the page, after pruning.
    pub live_tuples: i32,
    /// Number of recently-dead tuples on the page, after pruning.
    pub recently_dead_tuples: i32,
    /// Whether the all-visible bit can be set for this page after pruning.
    pub all_visible: bool,
    /// Whether the all-frozen bit can be set for this page after pruning.
    pub all_frozen: bool,
    /// Newest xmin of live tuples on the page (valid only when frozen).
    pub vm_conflict_horizon: TransactionId,
    /// Whether the page makes rel truncation unsafe.
    pub hastup: bool,
    /// LP_DEAD items on the page after pruning (includes pre-existing ones).
    pub lpdead_items: i32,
    /// `OffsetNumber deadoffsets[MaxHeapTuplesPerPage]` — the LP_DEAD offsets.
    pub deadoffsets: [OffsetNumber; MaxHeapTuplesPerPage],
}

impl Default for PruneFreezeResult {
    fn default() -> Self {
        PruneFreezeResult {
            ndeleted: 0,
            nnewlpdead: 0,
            nfrozen: 0,
            live_tuples: 0,
            recently_dead_tuples: 0,
            all_visible: false,
            all_frozen: false,
            vm_conflict_horizon: 0,
            hastup: false,
            lpdead_items: 0,
            deadoffsets: [0; MaxHeapTuplesPerPage],
        }
    }
}
