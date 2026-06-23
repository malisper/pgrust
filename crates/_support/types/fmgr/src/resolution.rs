//! Resolution / catalog-fact vocabulary for the function-manager core
//! (`fmgr.c`): the facts read out of `pg_proc` / `pg_language` and the
//! resolution `fmgr_info_cxt_security` computes.

use mcx::{PgString, PgVec};
use types_core::{Oid, TransactionId};
use ::types_tuple::heaptuple::ItemPointerData;

use crate::fmgr::{FmgrInfo, PGFunction};

/// The PL/language kind of a looked-up `pg_proc` row (C: `prolang` matched
/// against `INTERNALlanguageId`/`ClanguageId`/`SQLlanguageId`/else). The numeric
/// discriminants are preserved for parity with the C `switch`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Default)]
pub enum ProcLanguage {
    /// C: `INTERNALlanguageId` (12) — resolved through the built-in registry by
    /// `prosrc` name.
    Internal = 12,
    /// C: `ClanguageId` (13) — dynamically-loaded C function.
    C = 13,
    /// C: `SQLlanguageId` (14) — SQL-language function (`fmgr_sql`).
    Sql = 14,
    /// C: any other `prolang` — a procedural-language handler.
    #[default]
    Other = 0,
}

/// The catalog facts a `pg_proc` lookup returns for a function Oid (the fields
/// `fmgr_info_cxt_security` reads out of the `pg_proc` tuple). The string fields
/// are copied out of the catcache into the caller's `Mcx`, so this carries that
/// allocation lifetime.
#[derive(Debug)]
pub struct ProcInfo<'mcx> {
    /// C: `pronargs` -> `fn_nargs`.
    pub nargs: i16,
    /// C: `proisstrict` -> `fn_strict`.
    pub strict: bool,
    /// C: `proretset` -> `fn_retset`.
    pub retset: bool,
    /// C: `prolang` -> the resolution `switch`.
    pub language: ProcLanguage,
    /// C: `prosrc` (internal name / C link symbol). `None` when unused.
    pub prosrc: Option<PgString<'mcx>>,
    /// C: `probin` (library filename). Only for a C-language function.
    pub probin: Option<PgString<'mcx>>,
    /// C: the combined `prosecdef || !proconfig-is-null || needs_fmgr_hook(...)`
    /// routing predicate (routes through `fmgr_security_definer`).
    pub security_definer: bool,
    /// C: `prosecdef` in isolation — SOLE gate for the userid switch.
    pub prosecdef: bool,
    /// C: `prolang`, the raw language Oid (for the `Other` path's LANGOID lookup).
    pub prolang: Oid,
    /// C: `NameStr(proname)`. Used by `CheckFunctionValidatorAccess`.
    pub proname: Option<PgString<'mcx>>,
    /// C: `proowner` — the function's owner role. Read when `prosecdef` is set.
    pub proowner: Oid,
    /// C: the `proconfig` GUC names (already `TransformGUCArray`'d), paired with
    /// [`Self::proconfig_values`]. Empty (C: `NIL`) when `proconfig` is null.
    pub proconfig_names: PgVec<'mcx, PgString<'mcx>>,
    /// C: the `proconfig` values, paired with [`Self::proconfig_names`].
    pub proconfig_values: PgVec<'mcx, PgString<'mcx>>,
    /// C: `HeapTupleHeaderGetRawXmin(procedureTuple->t_data)` — the `pg_proc`
    /// tuple's raw xmin, the up-to-dateness key half for the C-function cache
    /// (`lookup_C_func` / `record_C_func`).
    pub xmin: TransactionId,
    /// C: `procedureTuple->t_self` — the `pg_proc` tuple's TID, the other half
    /// of the C-function cache up-to-dateness key.
    pub tid: ItemPointerData,
}

/// The `pg_proc` facts `internal_get_result_type` (funcapi.c) reads to classify
/// a function's result type: `prorettype`, `proretset`, `pronargs`, and the
/// declared `proargtypes` vector (the OID-vector the polymorphic-tupdesc
/// resolver substitutes against). Copied out of the catcache into the caller's
/// `Mcx`, so it carries that allocation lifetime.
#[derive(Debug)]
pub struct ProcResultInfo<'mcx> {
    /// C: `prorettype` — the function's declared result type OID.
    pub prorettype: Oid,
    /// C: `proretset` — whether the function returns a set.
    pub proretset: bool,
    /// C: `pronargs` — the number of declared (input) arguments.
    pub pronargs: i16,
    /// C: `proargtypes` (the `oidvector` of declared input-argument types).
    pub proargtypes: PgVec<'mcx, Oid>,
    /// C: `NameStr(proname)` — used in the "could not determine actual result
    /// type" error message.
    pub proname: PgString<'mcx>,
}

/// The catalog facts a `pg_language` lookup returns (read by
/// `fmgr_info_other_lang` / `CheckFunctionValidatorAccess`).
#[derive(Debug)]
pub struct LangInfo<'mcx> {
    /// C: `lanplcallfoid` — the language's call-handler function Oid.
    pub lanplcallfoid: Oid,
    /// C: `lanvalidator` — the language's validator function Oid.
    pub lanvalidator: Oid,
    /// C: `NameStr(lanname)`.
    pub lanname: PgString<'mcx>,
}

/// One row of the built-in function table (the calling-convention core's view of
/// `FmgrBuiltin`): a non-optional safe callable + owned `funcName`.
#[derive(Clone, Debug)]
pub struct BuiltinFunction {
    /// C: `FmgrBuiltin.foid`.
    pub foid: Oid,
    /// C: `FmgrBuiltin.funcName`.
    pub name: String,
    /// C: `FmgrBuiltin.nargs`.
    pub nargs: i16,
    /// C: `FmgrBuiltin.strict`.
    pub strict: bool,
    /// C: `FmgrBuiltin.retset`.
    pub retset: bool,
    /// C: `FmgrBuiltin.func`.
    pub func: PGFunction,
}

/// A loaded, ready-to-call C-language function (C: the `(user_fn, inforec)` pair
/// `fmgr_info_C_lang` caches for `api_version == 1`).
#[derive(Clone, Debug)]
pub struct LoadedCFunc {
    /// C: the resolved callable (`fn_addr = user_fn`), wrapped safely.
    pub func: BuiltinFunction,
}

/// The raw `(user_fn, inforec)` pair the dynamic loader produces for a
/// `(probin, prosrc)` — C: `load_external_function(...)` +
/// `fetch_finfo_record(libraryhandle, prosrc)`. `fmgr_info_C_lang` validates
/// `inforec.api_version` and caches this; the validation/caching is the function
/// manager's own logic (`fetch_finfo_record`'s post-load `api_version` switch is
/// the loader's; the `CFuncHash` is fmgr's).
#[derive(Clone, Debug)]
pub struct LoadedExternalFunc {
    /// C: `user_fn` — the loaded symbol, wrapped as a safe `PGFunction`.
    pub user_fn: PGFunction,
    /// C: `inforec->api_version` (validated by `fetch_finfo_record`).
    pub api_version: i32,
}

/// What `fmgr_info_cxt_security` resolved a function to (C stores this in
/// `FmgrInfo::fn_addr`; the secdef/SQL legs need extra captured facts the bare
/// callable cannot express, so the resolution is returned alongside `FmgrInfo`).
#[derive(Clone, Debug)]
pub enum FmgrResolution {
    /// C: `fmgr_isbuiltin` hit.
    Builtin(BuiltinFunction),
    /// C: `prolang == INTERNAL`; resolved via `fmgr_lookupByName`.
    InternalByName(BuiltinFunction),
    /// C: `prolang == ClanguageId`; `fmgr_info_C_lang` loaded the symbol.
    CLanguage(BuiltinFunction),
    /// C: security-definer / proconfig / fmgr-hook path; `fn_addr =
    /// fmgr_security_definer`. The target's `fn_oid` is captured (C reads it from
    /// `fcinfo->flinfo->fn_oid` inside `fmgr_security_definer`). C also captures
    /// `fn_mcxt`, but here the handler allocates its cache into a per-call
    /// context it builds locally, so no context is stored on the resolution.
    SecurityDefiner {
        /// C: `fcinfo->flinfo->fn_oid`.
        fn_oid: Oid,
    },
    /// C: `prolang == SQLlanguageId`; `fn_addr = fmgr_sql`. `fmgr_sql`'s body
    /// lives in `executor/functions.c` (a separate compilation unit); fmgr.c
    /// merely installs the function pointer here, exactly as the secdef leg
    /// installs `fmgr_security_definer`. The target's `fn_oid` is captured (C
    /// reads it from `fcinfo->flinfo->fn_oid` inside `fmgr_sql`). At call time
    /// the dispatch hands the frame to the `executor/functions.c` owner via its
    /// `fmgr_sql` seam.
    Sql {
        /// C: `fcinfo->flinfo->fn_oid`.
        fn_oid: Oid,
    },
}

impl FmgrResolution {
    /// Convenience constructor for the security-definer resolution.
    pub fn security_definer(fn_oid: Oid) -> Self {
        FmgrResolution::SecurityDefiner { fn_oid }
    }

    /// Convenience constructor for the SQL-language resolution.
    pub fn sql(fn_oid: Oid) -> Self {
        FmgrResolution::Sql { fn_oid }
    }
}

/// C: `FmgrHookEventType` (`fmgr.h`) — the event passed to `fmgr_hook` at the
/// three points in `fmgr_security_definer`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FmgrHookEventType {
    /// C: `FHET_START`.
    Start,
    /// C: `FHET_END`.
    End,
    /// C: `FHET_ABORT`.
    Abort,
}

/// C: `OBJECT_LANGUAGE` / `OBJECT_FUNCTION` — the object kind for
/// `aclcheck_error`'s message.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AclObjectType {
    /// C: `OBJECT_LANGUAGE`.
    Language,
    /// C: `OBJECT_FUNCTION`.
    Function,
}

/// A populated `FmgrInfo` paired with its resolution, returned by the
/// `fmgr_info` family (the stand-in for C writing `fn_addr` + returning the
/// populated `finfo` in the `out` parameter).
#[derive(Clone, Debug)]
pub struct ResolvedFmgrInfo {
    /// The populated lookup info (C: the `FmgrInfo *finfo` out-parameter).
    pub finfo: FmgrInfo,
    /// The resolution (the stand-in for the `fn_addr` callable + the
    /// secdef/SQL legs' extra facts).
    pub resolution: FmgrResolution,
}
