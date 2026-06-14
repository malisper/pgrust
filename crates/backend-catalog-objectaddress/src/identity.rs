//! F3 — `getObjectIdentity[Parts]` and the per-class identity helpers
//! (objectaddress.c 4824-6130).
//!
//! Assembles the dotted-identity string and, for `getObjectIdentityParts`, the
//! C `List **objname` / `List **objargs` out-parameters (modeled as Rust
//! out-vectors). Per-owner identity sub-seams mirror-and-panic into owners
//! until landed. Builds on F0 + [`crate::resolve::get_catalog_object_by_oid`].
//! Bodies scaffolded as mirror-and-panic.

use mcx::{Mcx, PgString};
use types_core::Oid;
use types_catalog::catalog_dependency::ObjectAddress;
use types_error::PgResult;

/// The C `List **objname` / `List **objargs` out-parameters of
/// `getObjectIdentityParts`, modeled as owned out-vectors of strings.
#[derive(Debug, Default)]
pub struct ObjectIdentityParts {
    /// `*objname` — the qualified-name components.
    pub objname: Vec<String>,
    /// `*objargs` — the argument-type components (empty when the C passes NULL
    /// or the object has no args).
    pub objargs: Vec<String>,
}

/// `getObjectIdentity(const ObjectAddress *object, bool missing_ok)`
/// (objectaddress.c 4824): the canonical dotted identity string, ignoring the
/// name/args breakdown. `Ok(None)` mirrors the C NULL for a vanished object
/// under `missing_ok`.
pub fn get_object_identity<'mcx>(
    _mcx: Mcx<'mcx>,
    _object: &ObjectAddress,
    _missing_ok: bool,
) -> PgResult<Option<PgString<'mcx>>> {
    panic!("decomp: getObjectIdentity not yet filled")
}

/// `getObjectIdentityParts(const ObjectAddress *object, List **objname, List
/// **objargs, bool missing_ok)` (objectaddress.c 4839; ~41 arms): the identity
/// string plus the name/args breakdown. The C out-params are returned in
/// [`ObjectIdentityParts`] alongside the identity string. `Ok(None)` mirrors
/// the C NULL for a vanished object under `missing_ok`.
pub fn get_object_identity_parts<'mcx>(
    _mcx: Mcx<'mcx>,
    _object: &ObjectAddress,
    _missing_ok: bool,
) -> PgResult<Option<(PgString<'mcx>, ObjectIdentityParts)>> {
    panic!("decomp: getObjectIdentityParts not yet filled")
}

/// `getOpFamilyIdentity(StringInfo buffer, Oid opfid, List **object, bool
/// missing_ok)` (objectaddress.c 6053).
pub fn get_op_family_identity<'mcx>(
    _mcx: Mcx<'mcx>,
    _buffer: &mut String,
    _opfid: Oid,
    _object: &mut Vec<String>,
    _missing_ok: bool,
) -> PgResult<()> {
    panic!("decomp: getOpFamilyIdentity not yet filled")
}

/// `getRelationIdentity(StringInfo buffer, Oid relid, List **object, bool
/// missing_ok)` (objectaddress.c 6097).
pub fn get_relation_identity<'mcx>(
    _mcx: Mcx<'mcx>,
    _buffer: &mut String,
    _relid: Oid,
    _object: &mut Vec<String>,
    _missing_ok: bool,
) -> PgResult<()> {
    panic!("decomp: getRelationIdentity not yet filled")
}
