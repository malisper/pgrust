//! Seam declarations for `backend-commands-collationcmds` (collationcmds.c).
//!
//! These are the externals the CREATE/ALTER COLLATION drivers and the two SQL
//! functions (`pg_collation_actual_version` / `pg_import_system_collations`)
//! reach that have no canonical seam home yet (their owners are not ported) or
//! that would cycle back into this crate's owners. Each panics until its owner
//! lands.
//!
//! Canonical seams used directly by collationcmds and NOT redeclared here:
//!   * ACL: `backend-catalog-aclchk-seams::{object_aclcheck, object_ownercheck,
//!     aclcheck_error, error_conflicting_def_elem}`;
//!   * identity / flags: `backend-utils-init-miscinit-seams::{get_user_id,
//!     superuser, is_binary_upgrade}`;
//!   * encoding: `backend-utils-mb-mbutils-seams::{get_database_encoding,
//!     get_database_encoding_name}`;
//!   * transaction: `backend-access-transam-xact-seams::command_counter_increment`;
//!   * pg_locale: `backend-utils-adt-pg-locale-seams::{get_collation_actual_version,
//!     pg_newlocale_from_collation}`;
//!   * define.c value layer: `backend-commands-define-seams::{def_get_string,
//!     def_get_boolean}`.
//!
//! Reused directly (direct cargo dep, no cycle): `backend-catalog-namespace`'s
//! `get_collation_oid` / `QualifiedNameGetCreationNamespace` / `NameListToString`.

use mcx::{Mcx, PgString};
use types_core::Oid;
use types_error::PgResult;

/// The `pg_collation` form fields collationcmds reads back from the syscache
/// (`SearchSysCache1(COLLOID, ...)`). All text columns are owned `Option<String>`
/// (SQL NULL ⇒ `None`); `provider` is the `char` `collprovider`, `encoding` the
/// `int` `collencoding`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CollationRow {
    /// `collprovider` (`COLLPROVIDER_*` as `char`).
    pub provider: i8,
    /// `collisdeterministic`.
    pub is_deterministic: bool,
    /// `collencoding`.
    pub encoding: i32,
    /// `collcollate` (libc LC_COLLATE), text or NULL.
    pub collate: Option<String>,
    /// `collctype` (libc LC_CTYPE), text or NULL.
    pub ctype: Option<String>,
    /// `colllocale` (builtin/ICU locale), text or NULL.
    pub locale: Option<String>,
    /// `collicurules` (ICU tailoring rules), text or NULL.
    pub icurules: Option<String>,
    /// `collversion`, text or NULL.
    pub version: Option<String>,
}

seam_core::seam!(
    /// `parser_errposition(pstate, location)` (parse_node.c): map a parse
    /// `location` to the `errposition` cursor offset. collationcmds forwards
    /// `defel->location` here for the "attribute not recognized" syntax error.
    pub fn parser_errposition(location: i32) -> i32
);

seam_core::seam!(
    /// `get_namespace_name(nspid)` (lsyscache.c): the schema name for an OID, or
    /// `None` (SQL NULL) when the namespace no longer exists. Used in the
    /// schema-ACL error and the duplicate-name errors.
    pub fn get_namespace_name(nspid: Oid) -> PgResult<Option<String>>
);

seam_core::seam!(
    /// `defGetQualifiedName(def)` (define.c): the option value as a qualified
    /// name list (used for the `FROM` source collation). `Err(syntax error)` when
    /// the value isn't a name.
    pub fn def_get_qualified_name(defname: String, arg_text: Option<String>) -> PgResult<Vec<String>>
);

seam_core::seam!(
    /// `SearchSysCache1(COLLOID, ObjectIdGetDatum(collid))` (syscache.c): the
    /// `pg_collation` row for `collid`, or `None` when no such tuple exists.
    /// `Err` carries any catalog-read failure.
    pub fn collation_row_by_oid(collid: Oid) -> PgResult<Option<CollationRow>>
);

seam_core::seam!(
    /// `CollationCreate(...)` (pg_collation.c): create a `pg_collation` entry,
    /// returning the new OID, or `InvalidOid` when `if_not_exists`/quiet and the
    /// collation already exists. `Err` carries the duplicate-object /
    /// permission / catalog-insert `ereport(ERROR)` surface.
    pub fn collation_create(args: CollationCreateArgs) -> PgResult<Oid>
);

/// Argument bundle for `CollationCreate` — mirrors its 13-parameter C signature.
#[derive(Clone, Debug)]
pub struct CollationCreateArgs {
    pub collname: String,
    pub collnamespace: Oid,
    pub collowner: Oid,
    pub collprovider: i8,
    pub collisdeterministic: bool,
    pub collencoding: i32,
    pub collcollate: Option<String>,
    pub collctype: Option<String>,
    pub colllocale: Option<String>,
    pub collicurules: Option<String>,
    pub collversion: Option<String>,
    pub if_not_exists: bool,
    pub quiet: bool,
}

seam_core::seam!(
    /// The `IsThereCollationInNamespace` existence probe: true if a
    /// `pg_collation` entry named `collname` with encoding `enc` already exists in
    /// namespace `nspid` (`SearchSysCacheExists3(COLLNAMEENCNSP, ...)`).
    pub fn collation_name_enc_nsp_exists(collname: String, enc: i32, nspid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `object_ownercheck(CollationRelationId, collOid, roleid)` against pg_collation —
    /// true if `roleid` owns the collation.
    pub fn collation_ownercheck(coll_oid: Oid, roleid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_COLLATION, collname)`: always
    /// raises the must-be-owner error for the collation named `collname`.
    pub fn aclcheck_error_not_owner_collation(collname: String) -> PgResult<()>
);

seam_core::seam!(
    /// The ALTER COLLATION REFRESH VERSION catalog mutation: opens pg_collation
    /// `RowExclusiveLock`, writes `collversion = newversion` (NULL ⇒ `None`) for
    /// `coll_oid` via `CatalogTupleUpdate`, fires `InvokeObjectPostAlterHook`, and
    /// closes `NoLock`.
    pub fn update_collation_version(coll_oid: Oid, newversion: Option<String>) -> PgResult<()>
);

seam_core::seam!(
    /// `(void) pg_import_system_collations` superuser gate uses miscinit's
    /// `superuser`; this is `SearchSysCacheExists1(NAMESPACEOID, ObjectIdGetDatum(nspid))`
    /// — whether the target schema exists.
    pub fn namespace_exists(nspid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// For `collid == DEFAULT_COLLATION_OID`: read `datlocprovider` /
    /// `datlocale` from `pg_database` for `MyDatabaseId`
    /// (`SearchSysCache1(DATABASEOID, ...)`). `None` when the row is missing
    /// (the "database with OID ... does not exist" path).
    pub fn database_locale_for_default_collation() -> PgResult<Option<(i8, String)>>
);

seam_core::seam!(
    /// `MyDatabaseId` — the current database OID (for the error message above).
    pub fn my_database_id() -> Oid
);

seam_core::seam!(
    /// `builtin_validate_locale(encoding, locale)` (pg_locale_builtin.c):
    /// validate and canonicalize the builtin-provider locale, returning the
    /// canonical name. `Err` on an invalid locale.
    pub fn builtin_validate_locale<'mcx>(
        mcx: Mcx<'mcx>,
        encoding: i32,
        locale: &str,
    ) -> PgResult<PgString<'mcx>>
);

seam_core::seam!(
    /// `builtin_locale_encoding(locale)` (pg_locale_builtin.c): the fixed
    /// encoding the builtin locale implies.
    pub fn builtin_locale_encoding(locale: &str) -> PgResult<i32>
);

seam_core::seam!(
    /// `is_encoding_supported_by_icu(encoding)` (pg_locale_icu.c): whether ICU is
    /// built (`USE_ICU`) and supports `encoding`. Always `false` when `!USE_ICU`.
    pub fn is_encoding_supported_by_icu(encoding: i32) -> PgResult<bool>
);

seam_core::seam!(
    /// `icu_validation_level` GUC (pg_locale.c): the `ereport` level
    /// `icu_language_tag` uses for an imperfect match (a `WARNING`/`ERROR`-class
    /// integer).
    pub fn icu_validation_level() -> PgResult<i32>
);

seam_core::seam!(
    /// `icu_language_tag(loc_str, strength)` (pg_locale_icu.c): canonicalize an
    /// ICU locale to its BCP 47 language tag. `Ok(None)` when ICU is not built
    /// and the level allows it; `Err` on a hard failure.
    pub fn icu_language_tag<'mcx>(
        mcx: Mcx<'mcx>,
        loc_str: &str,
        strength: i32,
    ) -> PgResult<Option<PgString<'mcx>>>
);

seam_core::seam!(
    /// `icu_language_tag(name, ERROR)` for the import path: same as above but
    /// always at `ERROR` strength, returning the tag (never NULL on success).
    pub fn icu_language_tag_error<'mcx>(
        mcx: Mcx<'mcx>,
        name: &str,
    ) -> PgResult<PgString<'mcx>>
);

seam_core::seam!(
    /// `icu_validate_locale(loc_str)` (pg_locale_icu.c): validate the ICU locale,
    /// raising/warning as appropriate. `Err` on a hard failure.
    pub fn icu_validate_locale(loc_str: &str) -> PgResult<()>
);

seam_core::seam!(
    /// `check_encoding_locale_matches(encoding, collate, ctype)`
    /// (pg_locale.c): raise if the libc locale's implied encoding does not match
    /// the database encoding.
    pub fn check_encoding_locale_matches(encoding: i32, collate: &str, ctype: &str) -> PgResult<()>
);

seam_core::seam!(
    /// `pg_get_encoding_from_locale(locale, write_message=true)` (chklocale.c):
    /// the PostgreSQL encoding id implied by a libc locale name, or `< 0` when
    /// unrecognized.
    pub fn pg_get_encoding_from_locale(locale: &str) -> PgResult<i32>
);

seam_core::seam!(
    /// `elog(DEBUG1, ...)` for the skipped-locale diagnostics in
    /// `create_collation_from_locale`.
    pub fn elog_debug1(msg: &str) -> PgResult<()>
);

seam_core::seam!(
    /// `OpenPipeStream("locale -a", "r")` + line read (collationcmds.c): the libc
    /// locale names, each already newline-stripped. Empty on `!READ_LOCALE_A_OUTPUT`.
    /// `Err` carries `errcode_for_file_access` if the pipe cannot be opened.
    pub fn enumerate_libc_locales() -> PgResult<Vec<String>>
);

seam_core::seam!(
    /// `uloc_countAvailable()`/`uloc_getAvailable()` (collationcmds.c) with the
    /// ICU root locale ("") prepended. Empty on `!USE_ICU`.
    pub fn enumerate_icu_locales() -> PgResult<Vec<String>>
);

seam_core::seam!(
    /// `get_icu_locale_comment(localename)` (collationcmds.c): the ICU display
    /// name used as the collation comment, or `None`.
    pub fn get_icu_locale_comment(localename: &str) -> PgResult<Option<String>>
);

seam_core::seam!(
    /// `CreateComments(oid, CollationRelationId, 0, comment)` (comment.c): attach
    /// a comment to the new collation.
    pub fn create_comment(collid: Oid, comment: &str) -> PgResult<()>
);
