//! Seam declarations for the `backend-storage-lmgr-lmgr` unit
//! (`storage/lmgr/lmgr.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. Open relations cross as their `Oid`.

seam_core::seam!(
    /// `CheckRelationLockedByMe(relation, lockmode, orstronger)` (lmgr.c):
    /// does this backend hold `lockmode` (or, with `orstronger`, any
    /// stronger lock) on the relation? Infallible (pure local lock-table
    /// lookup).
    pub fn check_relation_locked_by_me(
        relation: types_core::primitive::Oid,
        lockmode: types_storage::lock::LOCKMODE,
        orstronger: bool,
    ) -> bool
);
