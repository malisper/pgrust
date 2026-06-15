//! Table-access-method vocabulary (`access/tableam.h`, `access/relscan.h`,
//! `access/skey.h`), trimmed to the items the table-AM dispatch layer
//! consumes.
//!
//! Uses `std` for the `Mutex` that models the parallel-scan descriptor's
//! spinlock-protected field (the descriptor lives in shared memory in C and
//! is mutated concurrently by parallel workers).

#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

pub mod amapi;
pub mod genam;
pub mod relscan;
pub mod scankey;
pub mod tableam;

pub use amapi::{IndexAmRoutine, IndexInfo, IndexUniqueCheck, TIDBitmap};
pub use genam::{
    IndexBulkDeleteResult, IndexOrderByDistance, IndexScanInstrumentation, IndexVacuumInfo,
    SharedIndexScanInstrumentation,
};
pub use relscan::{
    IndexScanDesc, IndexScanDescData, ParallelBlockTableScanExt, ParallelBlockTableScanWorkerData,
    ParallelIndexScanDescData, ParallelTableScanDescData, TableScanDesc, TableScanDescData,
    SO_ALLOW_PAGEMODE, SO_ALLOW_STRAT, SO_ALLOW_SYNC, SO_TEMP_SNAPSHOT, SO_TYPE_ANALYZE,
    SO_TYPE_BITMAPSCAN, SO_TYPE_SAMPLESCAN, SO_TYPE_SEQSCAN, SO_TYPE_TIDRANGESCAN, SO_TYPE_TIDSCAN,
};
pub use scankey::ScanKeyData;
pub use tableam::{
    BulkInsertStateData, IndexFetchTableData, LockTupleExclusive, LockTupleKeyShare, LockTupleMode,
    LockTupleNoKeyExclusive, LockTupleShare, Snapshot, TM_FailureData, TM_Result,
    TUPLE_LOCK_FLAG_FIND_LAST_VERSION, TUPLE_LOCK_FLAG_LOCK_UPDATE_IN_PROGRESS, TU_UpdateIndexes,
    TableAmRoutine,
};
