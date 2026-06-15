use core::ffi::{c_char, c_int, c_void};

use crate::{Datum, NodeTag, Oid, Relation, StrategyNumber, TupleDesc};

pub const T_TableAmRoutine: NodeTag = 439;
pub const T_IndexAmRoutine: NodeTag = 438;

pub const BTREE_AM_OID: Oid = 403;
pub const BTMaxStrategyNumber: StrategyNumber = 5;

pub type LOCKMODE = c_int;
pub type CompareType = u32;
pub type RegProcedure = Oid;

pub const NoLock: LOCKMODE = 0;
pub const AccessShareLock: LOCKMODE = 1;
pub const RowShareLock: LOCKMODE = 2;
pub const RowExclusiveLock: LOCKMODE = 3;
pub const ShareUpdateExclusiveLock: LOCKMODE = 4;
pub const ShareLock: LOCKMODE = 5;
pub const ShareRowExclusiveLock: LOCKMODE = 6;
pub const ExclusiveLock: LOCKMODE = 7;
pub const AccessExclusiveLock: LOCKMODE = 8;

pub const COMPARE_INVALID: CompareType = 0;
pub const COMPARE_LT: CompareType = 1;
pub const COMPARE_LE: CompareType = 2;
pub const COMPARE_EQ: CompareType = 3;
pub const COMPARE_GE: CompareType = 4;
pub const COMPARE_GT: CompareType = 5;
pub const COMPARE_NE: CompareType = 6;
pub const COMPARE_OVERLAP: CompareType = 7;
pub const COMPARE_CONTAINED_BY: CompareType = 8;
pub const COMPARE_CONTAINS: CompareType = 9;

pub const RELKIND_RELATION: c_char = b'r' as c_char;
pub const RELKIND_INDEX: c_char = b'i' as c_char;
pub const RELKIND_SEQUENCE: c_char = b'S' as c_char;
pub const RELKIND_TOASTVALUE: c_char = b't' as c_char;
pub const RELKIND_VIEW: c_char = b'v' as c_char;
pub const RELKIND_MATVIEW: c_char = b'm' as c_char;
pub const RELKIND_COMPOSITE_TYPE: c_char = b'c' as c_char;
pub const RELKIND_FOREIGN_TABLE: c_char = b'f' as c_char;
pub const RELKIND_PARTITIONED_TABLE: c_char = b'p' as c_char;
pub const RELKIND_PARTITIONED_INDEX: c_char = b'I' as c_char;

// pg_attribute.h: attgenerated codes.
/// generated column stored on disk
pub const ATTRIBUTE_GENERATED_STORED: c_char = b's' as c_char;
/// generated column computed on read
pub const ATTRIBUTE_GENERATED_VIRTUAL: c_char = b'v' as c_char;

pub type OidFunctionCall0 = fn(Oid) -> Datum;
pub type IndexAmTranslateStrategy = fn(StrategyNumber, Oid) -> CompareType;
pub type IndexAmTranslateCompareType = fn(CompareType, Oid) -> StrategyNumber;
pub type IndexAmValidate = fn(Oid) -> bool;

#[repr(C)]
#[derive(Debug)]
pub struct TableAmRoutine {
    pub type_: NodeTag,
    pub slot_callbacks: *const c_void,
}

impl TableAmRoutine {
    pub fn node_tag(&self) -> NodeTag {
        self.type_
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct IndexAmRoutine {
    pub type_: NodeTag,
    pub amstrategies: u16,
    pub amsupport: u16,
    pub amoptsprocnum: u16,
    pub amcanorder: bool,
    pub amcanorderbyop: bool,
    pub amcanhash: bool,
    pub amconsistentequality: bool,
    pub amconsistentordering: bool,
    pub amcanbackward: bool,
    pub amcanunique: bool,
    pub amcanmulticol: bool,
    pub amoptionalkey: bool,
    pub amsearcharray: bool,
    pub amsearchnulls: bool,
    pub amstorage: bool,
    pub amclusterable: bool,
    pub ampredlocks: bool,
    pub amcanparallel: bool,
    /// does AM support parallel build? (`amapi.h`)
    pub amcanbuildparallel: bool,
    pub amcaninclude: bool,
    pub amusemaintenanceworkmem: bool,
    /// does AM store tuple information only at block granularity? (`amapi.h`)
    pub amsummarizing: bool,
    pub amparallelvacuumoptions: u8,
    pub amkeytype: Oid,
    pub amtranslatestrategy: Option<IndexAmTranslateStrategy>,
    pub amtranslatecmptype: Option<IndexAmTranslateCompareType>,
    pub amvalidate: Option<IndexAmValidate>,
}

impl IndexAmRoutine {
    pub fn node_tag(&self) -> NodeTag {
        self.type_
    }
}

/// `IndexAMProperty` (`access/amapi.h`) -- the index-AM property codes passed to
/// an AM's `ampropertyname` callback (e.g. `btproperty`).
#[repr(u32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexAMProperty {
    AmpropUnknown = 0, // anything not known to core code
    AmpropAsc,         // column properties
    AmpropDesc,
    AmpropNullsFirst,
    AmpropNullsLast,
    AmpropOrderable,
    AmpropDistanceOrderable,
    AmpropReturnable,
    AmpropSearchArray,
    AmpropSearchNulls,
    AmpropClusterable, // index properties
    AmpropIndexScan,
    AmpropBitmapScan,
    AmpropBackwardScan,
    AmpropCanOrder, // AM properties
    AmpropCanUnique,
    AmpropCanMultiCol,
    AmpropCanExclude,
    AmpropCanInclude,
}

pub type RangeVar = c_void;
pub type Snapshot = *mut c_void;
pub type IndexScanDesc = *mut c_void;
pub type BufferAccessStrategy = *mut c_void;
// `SampleScanState` is the opaque per-node state handle defined in `execnodes`.
// `TableScanDesc`, `ParallelTableScanDesc`, and `ReadStream` are the typed
// scan-descriptor handles defined in `relscan`.

/// `typedef enum VacOptValue` (commands/vacuum.h) — tri-state for the
/// `index_cleanup` / `truncate` VACUUM options.  Discriminants match the C enum.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum VacOptValue {
    VACOPTVALUE_UNSPECIFIED = 0,
    VACOPTVALUE_AUTO,
    VACOPTVALUE_DISABLED,
    VACOPTVALUE_ENABLED,
}

/// `typedef struct VacuumParams` (commands/vacuum.h) — parameters customizing
/// the behavior of VACUUM and ANALYZE.  `#[repr(C)]`, field order matches the C
/// struct exactly (PostgreSQL 18.3).
#[derive(Clone, Copy)]
#[repr(C)]
pub struct VacuumParams {
    /// `bits32 options` — bitmask of `VACOPT_*`.
    pub options: crate::bits32,
    /// `int freeze_min_age` — min freeze age, -1 to use default.
    pub freeze_min_age: c_int,
    /// `int freeze_table_age` — age at which to scan whole table.
    pub freeze_table_age: c_int,
    /// `int multixact_freeze_min_age` — min multixact freeze age, -1 default.
    pub multixact_freeze_min_age: c_int,
    /// `int multixact_freeze_table_age` — multixact age to scan whole table.
    pub multixact_freeze_table_age: c_int,
    /// `bool is_wraparound` — force a for-wraparound vacuum.
    pub is_wraparound: bool,
    /// `int log_min_duration` — min execution threshold (ms) for logging.
    pub log_min_duration: c_int,
    /// `VacOptValue index_cleanup` — do index vacuum and cleanup.
    pub index_cleanup: VacOptValue,
    /// `VacOptValue truncate` — truncate empty pages at the end.
    pub truncate: VacOptValue,
    /// `Oid toast_parent` — for privilege checks when recursing.
    pub toast_parent: Oid,
    /// `double max_eager_freeze_failure_rate` — eager-scan fail fraction (0 off).
    pub max_eager_freeze_failure_rate: f64,
    /// `int nworkers` — number of parallel vacuum workers (0 auto, -1 disabled).
    pub nworkers: c_int,
}
pub type TM_IndexDeleteOp = c_void;
pub type TIDBitmap = c_void;
pub type IndexInfo = c_void;
pub type IndexBuildResult = c_void;
pub type ToastTupleContext = c_void;

pub type EphemeralNameRelationType = u32;
pub const ENR_NAMED_TUPLESTORE: EphemeralNameRelationType = 0;

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct EphemeralNamedRelationMetadataData {
    pub name: *mut c_char,
    pub reliddesc: Oid,
    pub tupdesc: TupleDesc,
    pub enrtype: EphemeralNameRelationType,
    pub enrtuples: f64,
}

pub type EphemeralNamedRelationMetadata = *mut EphemeralNamedRelationMetadataData;

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct EphemeralNamedRelationData {
    pub md: EphemeralNamedRelationMetadataData,
    pub reldata: *mut c_void,
}

pub type EphemeralNamedRelation = *mut EphemeralNamedRelationData;

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RelationKind {
    pub relkind: c_char,
    pub name: *const c_char,
}

pub fn relkind_has_table_am(relkind: c_char) -> bool {
    matches!(
        relkind,
        RELKIND_RELATION | RELKIND_TOASTVALUE | RELKIND_MATVIEW
    )
}

pub fn relkind_has_storage(relkind: c_char) -> bool {
    matches!(
        relkind,
        RELKIND_RELATION | RELKIND_INDEX | RELKIND_SEQUENCE | RELKIND_TOASTVALUE | RELKIND_MATVIEW
    )
}

#[allow(dead_code)]
pub fn relation_is_null(relation: Relation) -> bool {
    relation.is_null()
}
