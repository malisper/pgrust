//! Seam-boundary value types for `backend/catalog/namespace.c`: the
//! decomposed catalog rows its `*IsVisibleExt` predicates and the
//! `FuncnameGetCandidates`/`MatchNamedCall`/operator-lookup helpers read out
//! of a syscache tuple / catlist member via `GETSTRUCT`.
//!
//! All row copies are made out of the catcache into the consumer's `Mcx`
//! (the C reads are in-place on the cache tuple; the owned model copies into
//! the caller's current-context equivalent and drop is the release).

use mcx::{PgString, PgVec};
use types_core::Oid;

/// The decomposed view of one catalog row that the `*IsVisibleExt` predicates
/// extract from a syscache tuple via `GETSTRUCT`: the object's namespace OID
/// and its (NUL-trimmed) name.
#[derive(Debug)]
pub struct CatalogObjectName<'mcx> {
    /// The relevant `*namespace` column of the row.
    pub namespace: Oid,
    /// The relevant `*name` column of the row.
    pub name: PgString<'mcx>,
}

/// The projection of a `Form_pg_am` row that amcmds.c's `get_am_type_oid`
/// reads via `GETSTRUCT` after a `SearchSysCache1(AMNAME, ...)` lookup: the
/// access method's OID, its single-character type discriminant, and its
/// (NUL-trimmed) name (`NameStr(amform->amname)`, used in the wrong-type error
/// message). Copied out of the catcache into the consumer's `Mcx`; the
/// installer owns the `ReleaseSysCache`.
#[derive(Debug)]
pub struct PgAmInfo<'mcx> {
    /// `amform->oid`.
    pub oid: Oid,
    /// `amform->amtype` (`AMTYPE_INDEX` / `AMTYPE_TABLE`).
    pub amtype: u8,
    /// `NameStr(amform->amname)`.
    pub amname: PgString<'mcx>,
}

/// The raw projection of a `pg_proc.proallargtypes` attribute value (an
/// `ArrayType` datum): the header fields the C call site validates plus the
/// element data read as Oids. The consumer (`FuncnameGetCandidates`) performs
/// the C call-site validity checks (`ARR_NDIM(arr) != 1 || dim0 < 0 ||
/// ARR_HASNULL(arr) || ARR_ELEMTYPE(arr) != OIDOID` =>
/// `elog(ERROR, "proallargtypes is not a 1-D Oid array or it contains
/// nulls")`); the seam only projects.
#[derive(Debug)]
pub struct OidArrayDatum<'mcx> {
    /// `ARR_NDIM(arr)`.
    pub ndim: i32,
    /// `ARR_DIMS(arr)[0]` (meaningful only when `ndim >= 1`; 0 otherwise).
    pub dim0: i32,
    /// `ARR_HASNULL(arr)`.
    pub hasnull: bool,
    /// `ARR_ELEMTYPE(arr)`.
    pub elemtype: Oid,
    /// `ARR_DATA_PTR(arr)` read as `dim0` Oids (empty if the shape checks
    /// above would fail — the consumer must check them before use).
    pub values: PgVec<'mcx, Oid>,
}

/// The raw projection of a `"char"[]` `ArrayType` datum (e.g.
/// `pg_proc.proargmodes`): the header fields the funcapi call sites validate
/// (`ARR_NDIM != 1 || dim0 != numargs || ARR_HASNULL || ARR_ELEMTYPE != CHAROID`
/// => `elog(ERROR, "... is not a 1-D char array ...")`) plus the element data
/// read as raw `char` bytes (C reads `ARR_DATA_PTR(arr)` directly as `char[]`).
/// The consumer performs the validity checks; the seam only projects.
#[derive(Debug)]
pub struct CharArrayDatum<'mcx> {
    /// `ARR_NDIM(arr)`.
    pub ndim: i32,
    /// `ARR_DIMS(arr)[0]` (meaningful only when `ndim >= 1`; 0 otherwise).
    pub dim0: i32,
    /// `ARR_HASNULL(arr)`.
    pub hasnull: bool,
    /// `ARR_ELEMTYPE(arr)`.
    pub elemtype: Oid,
    /// `ARR_DATA_PTR(arr)` read as `dim0` `char`s (empty if the shape checks
    /// above would fail — the consumer must check them before use).
    pub values: PgVec<'mcx, u8>,
}

/// The raw projection of a `text[]` `ArrayType` datum (e.g.
/// `pg_proc.proargnames`): the header fields the funcapi call sites validate
/// (`ARR_NDIM != 1 || dim0 != numargs || ARR_HASNULL || ARR_ELEMTYPE != TEXTOID`
/// => `elog(ERROR, "... is not a 1-D text array ...")`) plus the elements
/// already passed through `deconstruct_array_builtin(arr, TEXTOID, ...)` +
/// per-element `TextDatumGetCString`. The consumer performs the validity checks;
/// the seam only projects.
#[derive(Debug)]
pub struct TextArrayDatum<'mcx> {
    /// `ARR_NDIM(arr)`.
    pub ndim: i32,
    /// `ARR_DIMS(arr)[0]` (meaningful only when `ndim >= 1`; 0 otherwise).
    pub dim0: i32,
    /// `ARR_HASNULL(arr)`.
    pub hasnull: bool,
    /// `ARR_ELEMTYPE(arr)`.
    pub elemtype: Oid,
    /// The `nelems` element strings from `deconstruct_array_builtin` /
    /// `TextDatumGetCString` (empty if the shape checks above would fail — the
    /// consumer must check them before use).
    pub values: PgVec<'mcx, PgString<'mcx>>,
}

/// The `pg_proc`-row attribute projection the `funcapi.c` `pg_proc`-row helpers
/// (`get_func_arg_info`, `get_func_trftypes`, `get_func_result_name`,
/// `build_function_result_tupdesc_t`) read out of a `SearchSysCache1(PROCOID,
/// funcid)` tuple: the `Form_pg_proc` `GETSTRUCT` scalars plus the
/// `SysCacheGetAttr` array columns, each projected (and detoasted /
/// deconstructed) by the syscache seam. `None` array fields mirror a SQL-NULL
/// attribute (`isNull`). The funcapi consumers perform the C shape checks and
/// the business logic.
#[derive(Debug)]
pub struct FuncProcAttrs<'mcx> {
    /// `procStruct->prorettype`.
    pub prorettype: Oid,
    /// `procStruct->prokind` (raw `char`).
    pub prokind: u8,
    /// `procStruct->pronargs`.
    pub pronargs: i32,
    /// `procStruct->proargtypes.values` (length `pronargs`).
    pub proargtypes: PgVec<'mcx, Oid>,
    /// `SysCacheGetAttr(PROCOID, .., Anum_pg_proc_proallargtypes, &isNull)`.
    pub proallargtypes: Option<OidArrayDatum<'mcx>>,
    /// `SysCacheGetAttr(PROCOID, .., Anum_pg_proc_proargmodes, &isNull)`.
    pub proargmodes: Option<CharArrayDatum<'mcx>>,
    /// `SysCacheGetAttr(PROCOID, .., Anum_pg_proc_proargnames, &isNull)`.
    pub proargnames: Option<TextArrayDatum<'mcx>>,
    /// `SysCacheGetAttr(PROCOID, .., Anum_pg_proc_protrftypes, &isNull)`.
    pub protrftypes: Option<OidArrayDatum<'mcx>>,
}

/// The decomposed `pg_proc` row fields `FuncnameGetCandidates`/`MatchNamedCall`
/// read out of each catlist member: the `Form_pg_proc` GETSTRUCT reads plus
/// the `proallargtypes` `SysCacheGetAttr` extraction.
#[derive(Debug)]
pub struct ProcRow<'mcx> {
    pub oid: Oid,
    pub pronamespace: Oid,
    pub provariadic: Oid,
    pub pronargs: i32,
    pub pronargdefaults: i32,
    /// `procform->prorettype` — the function's return type (read by
    /// `assignProcTypes` in opclasscmds.c).
    pub prorettype: Oid,
    /// `procform->proargtypes.values` (length `pronargs`).
    pub proargtypes: PgVec<'mcx, Oid>,
    /// The `proallargtypes` attribute as a raw array projection if non-null;
    /// `None` => SQL null. Shape validation (and the corresponding
    /// `elog(ERROR)`) belongs to the consumer, mirroring the C call site in
    /// `FuncnameGetCandidates`.
    pub proallargtypes: Option<OidArrayDatum<'mcx>>,
    /// The `proname` (used only by `FunctionIsVisibleExt`).
    pub proname: PgString<'mcx>,
}

/// The `pg_proc` facts `fetch_fp_info` (`tcop/fastpath.c`) reads out of the
/// `SearchSysCache1(PROCOID, ...)` tuple via `GETSTRUCT` to load its
/// `struct fp_info`: `prokind` and `proretset` (the fastpath-safety gate),
/// `pronargs`, `pronamespace`, `prorettype`, the declared `proargtypes`
/// vector, and `NameStr(proname)` (for the error/log messages). Copied out of
/// the catcache into the caller's `Mcx`, so it carries that allocation
/// lifetime.
#[derive(Debug)]
pub struct FastpathProcRow<'mcx> {
    /// `procform->prokind` (`PROKIND_FUNCTION` is `'f'`).
    pub prokind: i8,
    /// `procform->proretset`.
    pub proretset: bool,
    /// `procform->pronargs`.
    pub pronargs: i16,
    /// `procform->pronamespace`.
    pub pronamespace: Oid,
    /// `procform->prorettype`.
    pub prorettype: Oid,
    /// `procform->proargtypes.values` (length `pronargs`).
    pub proargtypes: PgVec<'mcx, Oid>,
    /// `NameStr(procform->proname)` — the function name, for logging and the
    /// fastpath-rejection / too-many-arguments error messages.
    pub proname: PgString<'mcx>,
}

/// The decomposed `pg_operator` row fields `OpernameGet*` /
/// `OperatorIsVisibleExt` read out of each catlist member.
#[derive(Debug)]
pub struct OperRow<'mcx> {
    pub oid: Oid,
    pub oprnamespace: Oid,
    /// `oprkind` (`b`/`l`) as the raw C `char`.
    pub oprkind: u8,
    pub oprleft: Oid,
    pub oprright: Oid,
    /// `oprresult` — the operator's result type (read by `assignOperTypes`
    /// in opclasscmds.c).
    pub oprresult: Oid,
    pub oprname: PgString<'mcx>,
}

/// `get_func_arg_info(proctup, &p_argtypes, &p_argnames, &p_argmodes)` result
/// (`funcapi.c`), used by `MatchNamedCall`. Allocated in the caller's `Mcx`
/// (C: palloc'd arrays in the current context).
#[derive(Debug)]
pub struct FuncArgInfo<'mcx> {
    /// `p_argtypes` (length `pronallargs`).
    pub argtypes: PgVec<'mcx, Oid>,
    /// `p_argnames[i]` (`None` where the C array element is NULL).
    pub argnames: PgVec<'mcx, Option<PgString<'mcx>>>,
    /// `p_argmodes` (empty => the C `p_argmodes == NULL`); raw proargmode
    /// chars.
    pub argmodes: PgVec<'mcx, u8>,
}
