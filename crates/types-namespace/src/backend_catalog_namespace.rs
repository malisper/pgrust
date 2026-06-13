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
