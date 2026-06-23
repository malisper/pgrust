//! Seam declarations for the `backend-access-sequence` unit
//! (`access/sequence.c`): `sequence_open` / `sequence_close`.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use ::mcx::Mcx;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::rel::Relation;
use ::types_storage::lock::LOCKMODE;

seam_core::seam!(
    /// `sequence_open(relationId, lockmode)` (access/sequence.c): open+lock a
    /// relation by OID and verify `relkind == RELKIND_SEQUENCE` (raising
    /// `ERRCODE_WRONG_OBJECT_TYPE` "\"%s\" is not a sequence" otherwise).
    /// `Err` carries the lookup / wrong-object `ereport(ERROR)`s.
    pub fn sequence_open<'mcx>(
        mcx: Mcx<'mcx>,
        relation_id: Oid,
        lockmode: LOCKMODE,
    ) -> PgResult<Relation<'mcx>>
);

seam_core::seam!(
    /// `sequence_close(relation, lockmode)` (access/sequence.c): close a
    /// sequence relation, releasing the lock if `lockmode != NoLock`.
    /// Identified by its OID; refcount + optional lock release. `Err` carries
    /// the lock-release `ereport(ERROR)`s.
    pub fn sequence_close(relid: Oid, lockmode: LOCKMODE) -> PgResult<()>
);
