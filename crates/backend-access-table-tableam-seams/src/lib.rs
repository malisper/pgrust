//! Seam declarations for the table-AM dispatch helpers in
//! `access/table/tableam.c` that dispatch through a relation's
//! `rd_tableam` vtable into its access method.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. (The heap AM implementations these dispatch to,
//! `heapam_relation_toast_am` / `heapam_relation_needs_toast_table`, are
//! `access/heap/heapam_handler.c` — also unported; the call panics until both
//! land.)

use types_core::primitive::Oid;
use types_rel::Relation;

seam_core::seam!(
    /// `table_relation_toast_am(rel)` (access/tableam.h, static inline):
    /// `rel->rd_tableam->relation_toast_am(rel)` — the OID of the AM that
    /// should implement the TOAST table for `rel`. Infallible.
    pub fn table_relation_toast_am(rel: &Relation<'_>) -> Oid
);

seam_core::seam!(
    /// `table_relation_needs_toast_table(rel)` (access/tableam.h, static
    /// inline): `rel->rd_tableam->relation_needs_toast_table(rel)` — does the
    /// relation need a TOAST table? Infallible.
    pub fn table_relation_needs_toast_table(rel: &Relation<'_>) -> bool
);
