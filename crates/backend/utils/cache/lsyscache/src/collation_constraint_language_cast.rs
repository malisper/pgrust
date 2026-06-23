//! `collation-constraint-language-cast` family — `lsyscache.c` lookups keyed
//! on `pg_collation`, `pg_constraint`, `pg_language`, `pg_cast` and the
//! transform helpers.
//!
//! C entry points covered here: `get_collation_isdeterministic`,
//! `get_collation_name`, `get_constraint_name`, `get_constraint_index`,
//! `get_constraint_type`, `get_language_name`, `get_cast_oid`,
//! `get_transform_fromsql`, `get_transform_tosql`.
//!
//! These have no `backend-utils-cache-lsyscache-seams` declaration (their
//! fan-in consumers reach them by direct dependency rather than through this
//! unit's seam crate), so they are plain public functions. Each one's single
//! `SearchSysCache*` / `GetSysCacheOid*` probe is routed through the
//! `backend-utils-cache-syscache` owner's per-owner seam crate
//! (`backend-utils-cache-syscache-seams`) — panicking loudly until that
//! partner lands — exactly as the syscache.c `SearchSysCache` macro expands to
//! a catcache lookup the syscache unit owns. The `list_member_oid` probe in
//! the transform helpers and the `format_type_be` error-message rendering in
//! `get_cast_oid` likewise route through their owners' seam crates.

use ::mcx::{Mcx, PgString};
use ::types_core::{InvalidOid, Oid};
use ::types_error::{PgError, PgResult, ERRCODE_UNDEFINED_OBJECT, ERROR};

use copyfuncs_pc_seams as node_seams;
use format_type_seams as format_type;
use syscache_seams as syscache;

// `pg_constraint.contype` codes (catalog/pg_constraint.h). Verified
// field-by-field against the C header: 'c' check, 'f' foreign key, 'n' not
// null, 'p' primary key, 'u' unique, 't' constraint trigger, 'x' exclusion.
const CONSTRAINT_PRIMARY: u8 = b'p';
const CONSTRAINT_UNIQUE: u8 = b'u';
const CONSTRAINT_EXCLUSION: u8 = b'x';

/// `get_collation_isdeterministic(colloid)` (lsyscache.c): whether the
/// collation is deterministic (`pg_collation.collisdeterministic`). A missing
/// collation is the C `elog(ERROR, "cache lookup failed for collation %u")`.
pub fn get_collation_isdeterministic(colloid: Oid) -> PgResult<bool> {
    match syscache::collation_isdeterministic::call(colloid)? {
        Some(result) => Ok(result),
        None => Err(PgError::error(format!(
            "cache lookup failed for collation {colloid}"
        ))),
    }
}

/// `get_collation_name(colloid)` (lsyscache.c): the collation's name, copied
/// into `mcx` (C: `pstrdup`), or `None` (C: NULL) when there is no such
/// `pg_collation` row.
pub fn get_collation_name<'mcx>(
    mcx: Mcx<'mcx>,
    colloid: Oid,
) -> PgResult<Option<PgString<'mcx>>> {
    syscache::collation_name::call(mcx, colloid)
}

/// `get_constraint_name(conoid)` (lsyscache.c): the constraint's name, copied
/// into `mcx` (C: `pstrdup`), or `None` (C: NULL) when there is no such
/// `pg_constraint` row.
pub fn get_constraint_name<'mcx>(
    mcx: Mcx<'mcx>,
    conoid: Oid,
) -> PgResult<Option<PgString<'mcx>>> {
    syscache::constraint_name::call(mcx, conoid)
}

/// `get_constraint_index(conoid)` (lsyscache.c): for a UNIQUE / PRIMARY KEY /
/// EXCLUSION constraint, the OID of its supporting index (`conindid`); for any
/// other constraint type, or when there is no such `pg_constraint` row,
/// `InvalidOid`.
pub fn get_constraint_index(conoid: Oid) -> PgResult<Oid> {
    match syscache::constraint_type_index::call(conoid)? {
        Some((contype, conindid)) => {
            if contype == CONSTRAINT_UNIQUE
                || contype == CONSTRAINT_PRIMARY
                || contype == CONSTRAINT_EXCLUSION
            {
                Ok(conindid)
            } else {
                Ok(InvalidOid)
            }
        }
        None => Ok(InvalidOid),
    }
}

/// `get_constraint_type(conoid)` (lsyscache.c): the constraint's `contype`
/// char. A missing constraint is the C `elog(ERROR, "cache lookup failed for
/// constraint %u")`.
pub fn get_constraint_type(conoid: Oid) -> PgResult<u8> {
    match syscache::constraint_type_index::call(conoid)? {
        Some((contype, _conindid)) => Ok(contype),
        None => Err(PgError::error(format!(
            "cache lookup failed for constraint {conoid}"
        ))),
    }
}

/// `get_language_name(langoid, missing_ok)` (lsyscache.c): the language's
/// name, copied into `mcx` (C: `pstrdup`). With `missing_ok = false` a missing
/// language is the C `elog(ERROR, "cache lookup failed for language %u")`;
/// with `missing_ok = true` it is `Ok(None)`.
pub fn get_language_name<'mcx>(
    mcx: Mcx<'mcx>,
    langoid: Oid,
    missing_ok: bool,
) -> PgResult<Option<PgString<'mcx>>> {
    match syscache::language_name::call(mcx, langoid)? {
        Some(name) => Ok(Some(name)),
        None => {
            if !missing_ok {
                return Err(PgError::error(format!(
                    "cache lookup failed for language {langoid}"
                )));
            }
            Ok(None)
        }
    }
}

/// `get_cast_oid(sourcetypeid, targettypeid, missing_ok)` (lsyscache.c): the
/// OID of the cast between the two types, or `InvalidOid` when none and
/// `missing_ok`. With `!missing_ok` a missing cast is the C `ereport(ERROR,
/// errcode(ERRCODE_UNDEFINED_OBJECT), "cast from type %s to type %s does not
/// exist")`.
pub fn get_cast_oid(
    sourcetypeid: Oid,
    targettypeid: Oid,
    missing_ok: bool,
) -> PgResult<Oid> {
    let oid = syscache::cast_oid::call(sourcetypeid, targettypeid)?;
    if !::types_core::OidIsValid(oid) && !missing_ok {
        let src = format_type::format_type_be_str::call(sourcetypeid)?;
        let tgt = format_type::format_type_be_str::call(targettypeid)?;
        return Err(PgError::new(
            ERROR,
            format!("cast from type {src} to type {tgt} does not exist"),
        )
        .with_sqlstate(ERRCODE_UNDEFINED_OBJECT));
    }
    Ok(oid)
}

/// `get_transform_fromsql(typid, langid, trftypes)` (lsyscache.c): the
/// `trffromsql` function OID of the transform for the (type, language) pair,
/// but only when `typid` is a member of `trftypes`; `InvalidOid` otherwise or
/// when there is no such `pg_transform` row.
pub fn get_transform_fromsql(
    typid: Oid,
    langid: Oid,
    trftypes: &[Oid],
) -> PgResult<Oid> {
    if !node_seams::list_member_oid::call(trftypes, typid)? {
        return Ok(InvalidOid);
    }

    match syscache::transform_funcs::call(typid, langid)? {
        Some((trffromsql, _trftosql)) => Ok(trffromsql),
        None => Ok(InvalidOid),
    }
}

/// `get_transform_tosql(typid, langid, trftypes)` (lsyscache.c): the
/// `trftosql` function OID of the transform for the (type, language) pair, but
/// only when `typid` is a member of `trftypes`; `InvalidOid` otherwise or when
/// there is no such `pg_transform` row.
pub fn get_transform_tosql(
    typid: Oid,
    langid: Oid,
    trftypes: &[Oid],
) -> PgResult<Oid> {
    if !node_seams::list_member_oid::call(trftypes, typid)? {
        return Ok(InvalidOid);
    }

    match syscache::transform_funcs::call(typid, langid)? {
        Some((_trffromsql, trftosql)) => Ok(trftosql),
        None => Ok(InvalidOid),
    }
}
