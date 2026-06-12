//! Seam-boundary value types for `backend/catalog/namespace.c`: the
//! decomposed catalog rows its `*IsVisibleExt` predicates and the
//! `FuncnameGetCandidates`/`MatchNamedCall`/operator-lookup helpers read out
//! of a syscache tuple / catlist member via `GETSTRUCT`.

use alloc::string::String;
use alloc::vec::Vec;

use types_core::Oid;

/// The decomposed view of one catalog row that the `*IsVisibleExt` predicates
/// extract from a syscache tuple via `GETSTRUCT`: the object's namespace OID
/// and its (NUL-trimmed) name.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CatalogObjectName {
    /// The relevant `*namespace` column of the row.
    pub namespace: Oid,
    /// The relevant `*name` column of the row.
    pub name: String,
}

/// The decomposed `pg_proc` row fields `FuncnameGetCandidates`/`MatchNamedCall`
/// read out of each catlist member: the `Form_pg_proc` GETSTRUCT reads plus
/// the `proallargtypes` `SysCacheGetAttr` extraction.
#[derive(Clone, Debug, Default)]
pub struct ProcRow {
    pub oid: Oid,
    pub pronamespace: Oid,
    pub provariadic: Oid,
    pub pronargs: i32,
    pub pronargdefaults: i32,
    /// `procform->proargtypes.values` (length `pronargs`).
    pub proargtypes: Vec<Oid>,
    /// `proallargtypes` array if non-null (length = its dim). `None` => SQL
    /// null. The installer performs the C call-site validity checks
    /// (1-D Oid array, no nulls, element type OIDOID — else
    /// `elog(ERROR, "proallargtypes is not a 1-D Oid array or it contains
    /// nulls")`) when materializing this field.
    pub proallargtypes: Option<Vec<Oid>>,
    /// The `proname` (used only by `FunctionIsVisibleExt`).
    pub proname: String,
}

/// The decomposed `pg_operator` row fields `OpernameGet*` /
/// `OperatorIsVisibleExt` read out of each catlist member.
#[derive(Clone, Debug, Default)]
pub struct OperRow {
    pub oid: Oid,
    pub oprnamespace: Oid,
    /// `oprkind` (`b`/`l`) as the raw C `char`.
    pub oprkind: u8,
    pub oprleft: Oid,
    pub oprright: Oid,
    pub oprname: String,
}

/// `get_func_arg_info(proctup, ...)` result (`funcapi.c`), used by
/// `MatchNamedCall`.
#[derive(Clone, Debug, Default)]
pub struct FuncArgInfo {
    /// `p_argtypes` (length `pronallargs`).
    pub argtypes: Vec<Oid>,
    /// `p_argnames[i]` (`None` where the C array element is NULL).
    pub argnames: Vec<Option<String>>,
    /// `p_argmodes` (empty => the C `p_argmodes == NULL`); raw proargmode
    /// chars.
    pub argmodes: Vec<u8>,
}
