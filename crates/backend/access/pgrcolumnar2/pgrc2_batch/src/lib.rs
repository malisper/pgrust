//! # pgrc2_batch — the lanev4 execution batch/lane vocabulary (M3-L3)
//!
//! The in-code form of the FROZEN batch/kernel ABI
//! (`docs/design/lanev4-batch-abi.md`, authored from the SIGNED ledger rows
//! XC-6..XC-11). Every type here is CONTRACT TEXT: the module files are
//! copy-first from `origin/lanev3` lx_vec @ `dc3c67c56214` (the AB-clause
//! copy sources), with the v4 deltas the ABI fixes applied as separate,
//! cited commits on top — never silently.
//!
//! Clause map (module → AB clause):
//!
//! | module | clause | substance |
//! |---|---|---|
//! | [`batch`] | AB-2.1 | typed lanes over one row window; no per-row form |
//! | [`rep`] | AB-2.2/2.3 | dict-code lanes, epoch tag + key, escape codes |
//! | [`strview`] | AB-2.4 (P-7) | the 16-byte German-string cell, layout-pinned |
//! | [`validity`] | AB-3.1 | per-lane dense bitmask |
//! | [`selection`] | AB-3.2 | THE carried row currency (ascending positions) |
//! | [`mask`] | AB-3.3 | the normalized verdict mask (three-rep) |
//! | [`arena`] | AB-4.1 | batch-owned varlena copies (claim arena) |
//! | [`staging`] | AB-4.1 | varlena classification at the ownership boundary |
//! | [`rowid`] | AB-5.1 | the 32/19/13 columnar rowid word |
//! | [`guard`] | AB-7.3 | the per-batch guard-flag word (v4 delta, NEW) |
//!
//! Consumers: the M3-L3 reader kernels (`pgrc2_scan`) produce and consume
//! these shapes; the decode floors are measured THROUGH them; the M4
//! substrate compiles what M3 proved (AB-7.4). There is no per-row emit
//! face anywhere in this crate (PC-5.1 cross-ref).

pub mod arena;
pub mod batch;
pub mod guard;
pub mod mask;
pub mod rep;
pub mod rowid;
pub mod selection;
pub mod staging;
pub mod strview;
pub mod validity;

pub use arena::ClaimArena;
pub use batch::{Batch, Column};
pub use guard::{
    GuardWord, GUARD_CODE_BOUND, GUARD_COLLATION, GUARD_ENCODING, GUARD_EPOCH,
    GUARD_NUMERIC_DOMAIN, GUARD_OVERFLOW,
};
pub use mask::Mask;
pub use rep::{
    code_width_for, CodeWidth, ColRep, DictCodes, DictEpoch, DictEpochKey, DictHandle, DictSpace,
    ESCAPE_CODE_U16, ESCAPE_CODE_U32,
};
pub use rowid::RowId;
pub use selection::Selection;
pub use staging::{classify_varlena, StagingClass, VarlenaForm};
pub use strview::{StrCell, StrScratch, StrViews, TextSpan, STRVIEW_INLINE_MAX};
pub use validity::Validity;

/// AB-3.4 (v4 delta): the density crossover, a CONTRACT CONSTANT — at or
/// above `sel.len() * SEL_DENSITY_CROSSOVER_DIV >= nrows` a kernel MAY
/// evaluate in word-parallel masked form internally; below it kernels
/// evaluate in position-gather form. The carried representation never
/// varies (AB-3.2); this bounds only the internal strategy. A refit is an
/// ABI amendment citing a measured cell, never a per-kernel constant.
pub const SEL_DENSITY_CROSSOVER_DIV: u32 = 8;

#[cfg(test)]
mod tests;
