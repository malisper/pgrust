//! Index access-method API vocabulary (`access/amapi.h`) and the `CompareType`
//! enum (`access/cmptype.h`).
//!
//! As of the index-AM tower (F2) there is ONE canonical `IndexAmRoutine` and
//! its companion vocabulary, living in [`::types_tableam::amapi`] (the layer the
//! relcache `rd_indam` vtable and the `indexam.c` dispatch layer share). This
//! crate is now a thin re-export of those items so existing consumers
//! (`pg-constraint`, `indexcmds-seams`, `nbtree`, `hash`) keep compiling
//! through `types_amapi::…`.

#![allow(non_upper_case_globals)]

pub use ::types_tableam::amapi::{
    AmCostEstimate, CompareType, Cost, IndexAMProperty, IndexAmRoutine,
    IndexAmTranslateCompareType, IndexAmTranslateStrategy, IndexAmValidate, IndexBuildResult,
    IndexPath, IndexUniqueCheck, OpFamilyMember, PlannerInfo, Selectivity, TIDBitmap,
    T_IndexAmRoutine, COMPARE_CONTAINED_BY, COMPARE_EQ, COMPARE_GE, COMPARE_GT, COMPARE_INVALID,
    COMPARE_LE, COMPARE_LT, COMPARE_NE, COMPARE_OVERLAP,
};
/// The `'mcx`-safe `IndexInfo *` carrier (and its trait machinery) for the
/// index-AM dispatch edge — re-exported so consumers keep the
/// `types_amapi::…` path.
pub use ::types_tableam::index_info_carrier::{
    IndexInfoCarrier, IndexInfoLive, IndexInfoTagged, INDEX_INFO_TAG,
};
