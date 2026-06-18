//! Port of `src/backend/access/sequence/sequence.c` — the generic
//! `sequence_*` routines that implement access to sequences, in contrast to
//! other relation types like indexes.
//!
//! `sequence_open` is essentially `relation_open` (reached through the
//! `access/common/relation.c` seams) plus `validate_relation_kind`, which
//! checks that the relation is a sequence. The C `Relation` crosses as a
//! [`types_rel::Relation`] handle (the trimmed relcache-entry copy, allocated
//! in the caller-supplied `mcx`, armed by the relation.c owner with its close
//! function).
//!
//! `sequence_close` is the by-OID close: the C `sequence_close(relation,
//! lockmode)` is `relation_close`, and the call sites in the commands layer
//! hold only the relation OID, so the seam mirrors the handle-keyed close by
//! re-deriving the relcache reference and lock from the OID.

#![allow(non_snake_case)]

use mcx::Mcx;
use types_core::primitive::Oid;
use types_error::{PgError, PgResult, ERRCODE_WRONG_OBJECT_TYPE};
use types_rel::Relation;
use types_storage::lock::{LOCKMODE, NoLock};
use types_tuple::access::RELKIND_SEQUENCE;

/// Install this crate's seam implementations.
pub fn init_seams() {
    backend_access_sequence_seams::sequence_open::set(sequence_open);
    backend_access_sequence_seams::sequence_close::set(sequence_close);
}

/// `sequence_open(relationId, lockmode)` — open a sequence relation by
/// relation OID.
///
/// This is essentially `relation_open` plus a check that the relation is a
/// sequence.
pub fn sequence_open<'mcx>(
    mcx: Mcx<'mcx>,
    relationId: Oid,
    lockmode: LOCKMODE,
) -> PgResult<Relation<'mcx>> {
    let r = backend_access_common_relation_seams::relation_open::call(mcx, relationId, lockmode)?;

    validate_relation_kind(&r)?;

    Ok(r)
}

/// `sequence_close(relation, lockmode)` — close a sequence.
///
/// If `lockmode` is not `NoLock`, the specified lock is then released. Note
/// that it is often sensible to hold a lock beyond `relation_close`; in that
/// case, the lock is released automatically at xact end.
///
/// The call sites hold only the relation OID, so this re-derives the relcache
/// reference and lock from the OID: `RelationClose` (relcache) then
/// `UnlockRelationId`, mirroring the handle-keyed C close.
pub fn sequence_close(relid: Oid, lockmode: LOCKMODE) -> PgResult<()> {
    // The relcache does the real work of releasing the reference...
    backend_utils_cache_relcache_seams::relation_close::call(relid)?;

    if lockmode != NoLock {
        backend_storage_lmgr_lmgr_seams::unlock_relation_oid::call(relid, lockmode)?;
    }

    Ok(())
}

/// `validate_relation_kind(r)` (static inline) — make sure relkind is from a
/// sequence.
fn validate_relation_kind(r: &Relation<'_>) -> PgResult<()> {
    let relkind = r.rd_rel.relkind;

    if relkind != RELKIND_SEQUENCE {
        let relname = r.name();
        let detail =
            backend_catalog_pg_class_seams::errdetail_relkind_not_supported::call(relkind)?;
        return Err(
            PgError::error(format!("cannot open relation \"{relname}\""))
                .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE)
                .with_detail(detail),
        );
    }

    Ok(())
}
