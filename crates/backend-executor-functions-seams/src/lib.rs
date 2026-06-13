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
