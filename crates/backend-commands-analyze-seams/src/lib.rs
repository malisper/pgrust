//! Seam declaration for `analyze.c`'s `analyze_rel` entry point.
//!
//! `vacuum()` (commands/vacuum.c) calls `analyze_rel()` for the ANALYZE leg of
//! a VACUUM/ANALYZE command. `analyze.c` is not yet ported into this workspace,
//! so the call crosses this seam. There is only ONE seam here and it is
//! **declared, not installed** — until the owning `analyze` unit lands, a call
//! panics loudly (there is no fabricated analyze result).

#![allow(non_snake_case)]

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;
use types_nodes::rawnodes::RangeVar;
use types_vacuum::vacuum::VacuumParams;
use types_vacuum::vacuumlazy::StrategyHandle;

seam_core::seam!(
    /// `analyze_rel(relid, relation, params, va_cols, in_outer_xact, bstrategy)`
    /// (commands/analyze.c): collect statistics for one relation. `relation`
    /// is the parse-tree `RangeVar` (or `None` for an OID-only target);
    /// `va_cols` is the optional column-name list. The `mcx` is threaded so the
    /// owner can allocate its own working node values.
    pub fn analyze_rel<'mcx>(
        mcx: Mcx<'mcx>,
        relid: Oid,
        relation: Option<RangeVar<'mcx>>,
        params: VacuumParams,
        va_cols: Vec<String>,
        in_outer_xact: bool,
        bstrategy: StrategyHandle,
    ) -> PgResult<()>
);
