//! Outward seams for `backend/commands/comment.c` (`COMMENT ON`).
//!
//! comment.c's command-driver and catalog-upsert control flow is ported
//! in-crate (`backend-commands-comment`): the COMMENT-ON-DATABASE dump
//! work-around, the per-`ObjectType` integrity check, the shared-vs-local
//! catalog dispatch, the empty-string -> NULL reduction, the null-comment ->
//! delete branch, the found-vs-not-found upsert decision, the
//! `values`/`nulls`/`replaces` array setup, and the scan-key values. Only the
//! genuine subsystem *primitives* cross these seams; each owning subsystem
//! installs its real implementation when it lands, so until then a call panics
//! loudly with the seam path (mirror-PG-and-panic).
//!
//! `get_object_address` / `check_object_ownership` (objectaddress.c) are called
//! through the canonical `backend-catalog-objectaddress-seams` (installed by the
//! merged owner). The relation `get_object_address` opens is a real
//! `types_rel::Relation`, so `strVal(stmt->object)`, the relation
//! `relkind`/name reads, and `relation_close` are done in-crate, not seamed.
//!
//! Boundaries, by owning subsystem:
//!
//!  * access/table.h, genam.c, heaptuple.c, indexing.c — the decomposed
//!    `pg_description` / `pg_shdescription` catalog primitives: `*_open` /
//!    `*_close` (`table_open`/`table_close`), `*_find_one` /
//!    `*_get_description` (the `systable` index scan that finds the one match),
//!    `*_delete_all` (the `CatalogTupleDelete`-every-match loop), and
//!    `*_delete` / `*_update` / `*_insert` (the per-tuple `CatalogTupleDelete` /
//!    `heap_modify_tuple`+`CatalogTupleUpdate` /
//!    `heap_form_tuple`+`CatalogTupleInsert` mutations). The
//!    found-vs-not-found / delete-vs-upsert *decisions* are made in-crate; only
//!    these per-tuple primitives cross because the `pg_description` tuple ABI is
//!    not yet ported;
//!  * builtins.h / fmgr — [`cstring_get_text_datum`] (`CStringGetTextDatum`) and
//!    [`text_datum_get_cstring`] (`TextDatumGetCString`), the project-wide
//!    Datum/varlena/fmgr deferral.

#![allow(non_snake_case)]

use mcx::Mcx;
use seam_core::seam;
use types_core::Oid;
use types_error::PgResult;
use types_tuple::backend_access_common_heaptuple::Datum;
use types_storage::lock::LOCKMODE;

/// The row identity (`oldtuple->t_self`, the tuple's `ItemPointerData`) of a
/// matched `pg_description` / `pg_shdescription` tuple, carried opaquely across
/// the catalog-primitive seams. Mirrors C handing `&oldtuple->t_self` back to
/// `CatalogTupleDelete` / `CatalogTupleUpdate` without the caller inspecting it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DescriptionTupleId(pub types_tuple::heaptuple::ItemPointerData);

/// The description-column read of [`description_get_description`]: the column
/// `Datum` plus its `isnull` flag (`heap_getattr(tuple, ..., &isnull)`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DescriptionColumn<'mcx> {
    pub value: Datum<'mcx>,
    pub isnull: bool,
}

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

// --- pg_description catalog primitives (genam.c / heaptuple.c / indexing.c) -

seam!(
    /// `table_open(DescriptionRelationId, lockmode)` (access/table.h). Returns
    /// the opened relation's `Oid`.
    pub fn description_open(lockmode: LOCKMODE) -> PgResult<Oid>
);

seam!(
    /// `table_close(description, lockmode)` (access/table.h).
    pub fn description_close(description: Oid, lockmode: LOCKMODE) -> PgResult<()>
);

seam!(
    /// `systable_beginscan(DescriptionObjIndexId, ..., 3, skey)` +
    /// `systable_getnext` (first match) + `systable_endscan` — the index scan
    /// that finds the single pg_description tuple for `{objoid, classoid,
    /// objsubid}`, returning its row identity or `None`.
    pub fn description_find_one(
        description: Oid,
        objoid: Oid,
        classoid: Oid,
        objsubid: i32,
    ) -> PgResult<Option<DescriptionTupleId>>
);

seam!(
    /// `CatalogTupleDelete(description, &oldtuple->t_self)` (indexing.c).
    pub fn description_delete(description: Oid, tuple: DescriptionTupleId) -> PgResult<()>
);

seam!(
    /// `heap_modify_tuple(oldtuple, RelationGetDescr(description), values, nulls,
    /// replaces)` + `CatalogTupleUpdate(description, &oldtuple->t_self,
    /// newtuple)` + `heap_freetuple` — replace the found pg_description tuple
    /// from the in-crate `values`/`nulls`/`replaces` arrays.
    pub fn description_update<'mcx>(
        description: Oid,
        tuple: DescriptionTupleId,
        values: &[Datum<'mcx>],
        nulls: &[bool],
        replaces: &[bool],
    ) -> PgResult<()>
);

seam!(
    /// `heap_form_tuple(RelationGetDescr(description), values, nulls)` +
    /// `CatalogTupleInsert(description, newtuple)` + `heap_freetuple` — insert a
    /// fresh pg_description tuple from the in-crate `values`/`nulls` arrays.
    pub fn description_insert<'mcx>(
        description: Oid,
        values: &[Datum<'mcx>],
        nulls: &[bool],
    ) -> PgResult<()>
);

seam!(
    /// `systable_beginscan(DescriptionObjIndexId, ..., nkeys, skey)` +
    /// `CatalogTupleDelete` of every match + `systable_endscan` —
    /// `DeleteComments`'s remove-all-matching loop. `objsubid` is `Some` only
    /// when the caller's `subid != 0` (the in-crate `nkeys` 3-vs-2 decision).
    pub fn description_delete_all(
        description: Oid,
        objoid: Oid,
        classoid: Oid,
        objsubid: Option<i32>,
    ) -> PgResult<()>
);

seam!(
    /// `systable_getnext` (first match) + `heap_getattr(tuple,
    /// Anum_pg_description_description, ..., &isnull)` — the `GetComment` scan
    /// returning the description-column `Datum` of the one match (with its null
    /// flag), or `None` if no row matched.
    pub fn description_get_description<'mcx>(
        mcx: Mcx<'mcx>,
        description: Oid,
        objoid: Oid,
        classoid: Oid,
        objsubid: i32,
    ) -> PgResult<Option<DescriptionColumn<'mcx>>>
);

// --- pg_shdescription catalog primitives -----------------------------------

seam!(
    /// `table_open(SharedDescriptionRelationId, lockmode)` (access/table.h).
    pub fn shdescription_open(lockmode: LOCKMODE) -> PgResult<Oid>
);

seam!(
    /// `table_close(shdescription, lockmode)` (access/table.h).
    pub fn shdescription_close(shdescription: Oid, lockmode: LOCKMODE) -> PgResult<()>
);

seam!(
    /// `systable_beginscan(SharedDescriptionObjIndexId, ..., 2, skey)` +
    /// `systable_getnext` (first match) + `systable_endscan` — finds the single
    /// pg_shdescription tuple for `{objoid, classoid}` (no objsubid column).
    pub fn shdescription_find_one(
        shdescription: Oid,
        objoid: Oid,
        classoid: Oid,
    ) -> PgResult<Option<DescriptionTupleId>>
);

seam!(
    /// `CatalogTupleDelete(shdescription, &oldtuple->t_self)` (indexing.c).
    pub fn shdescription_delete(shdescription: Oid, tuple: DescriptionTupleId) -> PgResult<()>
);

seam!(
    /// `heap_modify_tuple` + `CatalogTupleUpdate` + `heap_freetuple` for
    /// pg_shdescription (no objsubid column).
    pub fn shdescription_update<'mcx>(
        shdescription: Oid,
        tuple: DescriptionTupleId,
        values: &[Datum<'mcx>],
        nulls: &[bool],
        replaces: &[bool],
    ) -> PgResult<()>
);

seam!(
    /// `heap_form_tuple` + `CatalogTupleInsert` + `heap_freetuple` for
    /// pg_shdescription.
    pub fn shdescription_insert<'mcx>(
        shdescription: Oid,
        values: &[Datum<'mcx>],
        nulls: &[bool],
    ) -> PgResult<()>
);

seam!(
    /// `systable_beginscan(SharedDescriptionObjIndexId, ..., 2, skey)` +
    /// `CatalogTupleDelete` of every match + `systable_endscan` —
    /// `DeleteSharedComments`'s remove-all-matching loop (always 2 scan keys).
    pub fn shdescription_delete_all(
        shdescription: Oid,
        objoid: Oid,
        classoid: Oid,
    ) -> PgResult<()>
);

seam!(
    /// `DeleteComments(oid, classoid, subid)` (commands/comment.c): remove all
    /// comment(s) on the object `{oid, classoid, subid}` (subid 0 = the whole
    /// object). dependency.c's `deleteObjectsInList` calls this to clean up
    /// `pg_description`/`pg_shdescription` rows for a dropped object. Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn DeleteComments(oid: Oid, classoid: Oid, subid: i32) -> PgResult<()>
);
