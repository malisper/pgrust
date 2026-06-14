//! Seam declarations owned by `backend-executor-functions`
//! (`src/backend/executor/functions.c`), the SQL-language function call
//! handler.
//!
//! `executor/functions.c` is not yet ported. fmgr.c's `fmgr_info_cxt_security`
//! resolves a `prolang == SQLlanguageId` function to `fn_addr = fmgr_sql`
//! (fmgr.c:251); the body of `fmgr_sql` lives in this unit, not in fmgr.c. The
//! fmgr owner therefore dispatches the call across this seam. Until
//! `executor/functions.c` lands and installs it from its own `init_seams()`, a
//! call panics loudly ("seam not installed").

use mcx::Mcx;
use types_core::Oid;
// Datum-unification (Batch 3): this seam's only `Datum` use is the `fmgr_sql`
// result. It is contract-pinned to the bare-word `types_datum::Datum`, NOT the
// canonical `types_tuple::Datum<'mcx>`, by its sole consumer
// `backend-utils-fmgr-core::function_call_invoke_with_expr`, which returns this
// seam's result as one arm of a `match` whose other arms come from
// `invoke_pgfunction` — the audited bare-word `Datum` PGFunction-return edge
// site. Flipping this seam to the canonical enum would diverge from fmgr-core's
// contract (which the Batch-3 fmgr-core sibling deliberately keeps on the
// bare-word shim) and break the consumer's `cargo check`. The migration of this
// seam is therefore blocked on the PGFunction-return edge migration, which is
// out of scope for the datum-unification phase (it is one of the two sanctioned
// bare-word edge sites). See execExpr-datum-mig-contract-blocked for the
// identical pattern.
use types_datum::Datum;
use types_error::PgResult;
use types_fmgr::FunctionCallInfoBaseData;

seam_core::seam!(
    /// `fmgr_sql(PG_FUNCTION_ARGS)` (`executor/functions.c`): the call handler
    /// installed as `fn_addr` for SQL-language (`prolang == SQLlanguageId`)
    /// functions. fmgr.c's `function_call_invoke` dispatches the resolved call
    /// frame here exactly as it dispatches a security-definer function to
    /// `fmgr_security_definer`. `fn_oid` is the called function's OID (C:
    /// `fcinfo->flinfo->fn_oid`, which `fmgr_sql` reads to fetch the SQL body);
    /// `mcx` is the call's memory context; `fcinfo` is the populated call frame
    /// (args/isnull/collation/context). Returns the function's `Datum` result;
    /// `Err` carries whatever the SQL function body raises.
    pub fn fmgr_sql<'mcx>(
        mcx: Mcx<'mcx>,
        fn_oid: Oid,
        fcinfo: &mut FunctionCallInfoBaseData,
    ) -> PgResult<Datum>
);
