//! Seam declarations for `backend/utils/cache/funccache.c`.
//!
//! `funccache.c` computes the cache hash key from the call frame + `pg_proc`
//! row, resolves polymorphic / RECORD argument types, and drives a
//! backend-lifetime cache — all ported 1:1 in the owner crate, which calls
//! funcapi / tupdesc directly (no cycle). The two couplings funccache cannot
//! reach directly cross these seams: the `pg_proc` syscache projection
//! (a syscache catalog read, owned + installed by the syscache crate) and the
//! trigger-OID downcast (a trigger-subsystem node access). Each defaults to a
//! loud-panic stub until its owner installs it; there is no silent fallback.

use mcx::Mcx;
use seam_core::seam;

use types_core::Oid;
use types_funccache::ProcCompileInfo;
use ::nodes::fmgr::FunctionCallInfoBaseData;

/* ---- syscache.c (pg_proc projection) --------------------------------------- */

seam!(
    /// `SearchSysCache1(PROCOID, funcOid)` + `GETSTRUCT` — the funccache
    /// projection of the function's `pg_proc` row: the input-type signature
    /// (`pronargs`/`proargtypes`/`proname`) for the hash key plus the row's
    /// `xmin`/`ctid` for the up-to-dateness check (mirrors the
    /// `search_pg_proc_fastpath` / `ProcInfo` syscache projections). `Ok(None)`
    /// on a cache miss (the caller turns that into "cache lookup failed for
    /// function %u"). The installer (syscache) owns the `ReleaseSysCache`.
    pub fn search_proc_compile_info<'mcx>(
        mcx: Mcx<'mcx>,
        func_oid: Oid,
    ) -> types_error::PgResult<Option<ProcCompileInfo<'mcx>>>
);

/* ---- pl/plpgsql opaque-header accessor ------------------------------------- */

seam!(
    /// `cfunc->use_count` read through a procedural language's *opaque*
    /// `CachedFunction` header handle. PL/pgSQL's `PLpgSQL_function.cfunc` is
    /// modeled in `types-plpgsql` as an inherited-opacity handle (it forwards
    /// the header across this seam rather than embedding the real struct), so
    /// `plpgsql_free_function_memory`'s "Better not call this on an in-use
    /// function" assert reads the count back through funccache, the field's
    /// owner. The funccache owner installs this; until then it panics loudly.
    pub fn cfunc_use_count(cfunc: plpgsql::CachedFunction) -> u64
);

/* ---- commands/trigger.c (TriggerData downcast) ----------------------------- */

seam!(
    /// `((TriggerData *) fcinfo->context)->tg_trigger->tgoid` (`trigger.c`). The
    /// OID of the trigger that invoked the function, used in DML-trigger mode to
    /// give each trigger usage a distinct hash entry. A trigger-subsystem node
    /// access funccache cannot express in-crate (the context node's `TriggerData`
    /// payload lives in the trigger crate); installed by the trigger owner.
    pub fn trigger_context_oid<'mcx>(
        fcinfo: &FunctionCallInfoBaseData<'mcx>,
    ) -> types_error::PgResult<Oid>
);
