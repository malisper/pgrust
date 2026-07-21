#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]

pub mod amop;
pub mod attribute;
pub mod function;
pub mod misc;
pub mod operator;
pub mod relation;
pub(crate) mod scratch;
pub mod statistics;
pub mod typ;
// Per-planning-cycle catalog memos (NOT glob-re-exported: its names shadow
// the incumbent per-call accessors deliberately, callers opt in via the
// module path + a &PlannerRun).
pub mod run_memo;

#[cfg(test)]
mod tests;

pub use amop::*;
pub use attribute::*;
pub use function::*;
pub use misc::*;
pub use operator::*;
pub use relation::*;
pub use statistics::*;
pub use typ::*;

use types_core::Oid;
pub use types_pathnodes::{
    CompareType, COMPARE_CONTAINED_BY, COMPARE_EQ, COMPARE_GE, COMPARE_GT, COMPARE_INVALID,
    COMPARE_LE, COMPARE_LT, COMPARE_NE, COMPARE_OVERLAP,
};

pub type StrategyNumber = u16;
pub const InvalidStrategy: StrategyNumber = 0;

// lsyscache.h
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OpIndexInterpretation {
    pub opfamily_id: Oid,
    pub cmptype: CompareType,
    pub oplefttype: Oid,
    pub oprighttype: Oid,
}

// lsyscache.h
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IOFuncSelector {
    IOFunc_input,
    IOFunc_output,
    IOFunc_receive,
    IOFunc_send,
}

#[cold]
#[inline(never)]
pub(crate) fn cache_lookup_error(msg: String) -> Box<types_error::PgError> {
    Box::new(types_error::PgError::error(msg))
}

/// Install every declaration this unit owns in `lsyscache_seams`.
pub fn init_seams() {
    lsyscache_seams::get_type_output_info::set(typ::getTypeOutputInfo);
    lsyscache_seams::get_type_binary_output_info::set(typ::getTypeBinaryOutputInfo);
    lsyscache_seams::get_base_type_and_typmod::set(|typid, mut typmod| {
        let base = typ::getBaseTypeAndTypmod(typid, &mut typmod)?;
        Ok((base, typmod))
    });
}
