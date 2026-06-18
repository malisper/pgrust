//! Seams for `backend/commands/comment.c` (`COMMENT ON`).
//!
//! comment.c's whole command-driver and catalog-upsert control flow is ported
//! in-crate (`backend-commands-comment`): the COMMENT-ON-DATABASE dump
//! work-around, the per-`ObjectType` integrity check, the shared-vs-local
//! catalog dispatch, the empty-string -> NULL reduction, the null-comment ->
//! delete branch, the found-vs-not-found upsert decision, the
//! `values`/`nulls`/`replaces` array setup, the scan-key values, and the real
//! `pg_description`/`pg_shdescription` catalog reads and writes (`table_open`,
//! the `systable` index scans, `CatalogTupleDelete` /
//! `heap_modify_tuple`+`CatalogTupleUpdate` / `heap_form_tuple`+
//! `CatalogTupleInsert`) over a real [`types_rel::Relation`].
//!
//! Only two genuine cross-subsystem boundaries cross outward, both the
//! project-wide Datum/varlena/fmgr deferral:
//!
//!  * [`cstring_get_text_datum`] (`CStringGetTextDatum`) — pack a comment into a
//!    `text` `Datum`;
//!  * [`text_datum_get_cstring`] (`TextDatumGetCString`) — detoast a `text`
//!    `Datum` back to a string (`GetComment`).
//!
//! Inward, [`DeleteComments`] is the boundary dependency.c crosses when an
//! object is dropped (it cleans up the object's `pg_description` rows).

#![allow(non_snake_case)]

use mcx::Mcx;
use seam_core::seam;
use types_core::Oid;
use types_error::PgResult;
use types_tuple::backend_access_common_heaptuple::Datum;

// --- fmgr / varlena (the project-wide Datum/fmgr deferral) -----------------

seam!(
    /// `CStringGetTextDatum(comment)` (builtins.h) — pack a C string into a
    /// `text` `Datum` (a varlena palloc), for the description column.
    pub fn cstring_get_text_datum<'mcx>(mcx: Mcx<'mcx>, comment: &str) -> PgResult<Datum<'mcx>>
);

seam!(
    /// `TextDatumGetCString(value)` (builtins.h) — detoast a `text` `Datum` back
    /// to an owned string (`GetComment`'s description-field read).
    pub fn text_datum_get_cstring<'mcx>(value: Datum<'mcx>) -> PgResult<String>
);

// --- inward boundary -------------------------------------------------------

seam!(
    /// `DeleteComments(oid, classoid, subid)` (commands/comment.c): remove all
    /// comment(s) on the object `{oid, classoid, subid}` (subid 0 = the whole
    /// object). dependency.c's `deleteObjectsInList` calls this to clean up
    /// `pg_description` rows for a dropped object. Can `ereport(ERROR)`, carried
    /// on `Err`.
    pub fn DeleteComments(oid: Oid, classoid: Oid, subid: i32) -> PgResult<()>
);
