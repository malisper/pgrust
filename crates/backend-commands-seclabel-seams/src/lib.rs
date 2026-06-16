//! Seam declarations for `commands/seclabel.c`'s cross-subsystem callees.
//!
//! seclabel.c is a thin SECURITY LABEL command driver: every heavyweight
//! operation it performs — the current-user lookup, the `text` Datum
//! conversions, and the `pg_seclabel` / `pg_shseclabel` catalog open / scan /
//! find / insert / update / delete primitives — crosses one of these seams.
//! The object-address resolution and ownership check live in the owner's
//! `backend-catalog-objectaddress-seams` crate; `IsSharedRelation`
//! (`backend-catalog-catalog`) and `errdetail_relkind_not_supported`
//! (`backend-catalog-pg-class`) are real ported functions called directly.
//!
//! Each owner installs its real implementation when it lands; until then a
//! call panics loudly with the seam path. Signatures mirror each C function's
//! failure surface (`PgResult<_>` where the C path can `ereport` at ERROR+ or
//! palloc out of memory). The catalog primitives operate on the real opened
//! `Relation` (`table_open`), and a found tuple is identified by its
//! `ItemPointerData` (`oldtup->t_self`).

#![allow(non_snake_case)]

use mcx::Mcx;
use seam_core::seam;
use types_catalog::catalog_dependency::ObjectAddress;
use types_core::primitive::Oid;
use types_error::PgResult;
use types_rel::Relation;
use types_storage::lock::LOCKMODE;
use types_tuple::backend_access_common_heaptuple::Datum;
use types_tuple::heaptuple::ItemPointerData;

/// The `label` column of a matched `pg_seclabel` / `pg_shseclabel` tuple:
/// `heap_getattr(tuple, Anum_*_label, ..., &isnull)`. `value` is meaningful
/// only when `!isnull` (C reads `seclabel = TextDatumGetCString(datum)` only
/// in the `!isnull` branch). The canonical [`Datum<'mcx>`] rides its by-value
/// arm for a scalar word and its by-reference arm for a detoasted `text` image.
pub struct SecLabelColumn<'mcx> {
    pub value: Datum<'mcx>,
    pub isnull: bool,
}

/* --- user-id substrate (miscinit.c) --- */

seam!(
    /// `GetUserId()` (miscinit.c) — the current effective user, for the
    /// ownership check.
    pub fn get_user_id() -> PgResult<Oid>
);

/* --- text Datum conversions (utils/adt/varlena.c, utils/builtins.h) --- */

seam!(
    /// `CStringGetTextDatum(s)` (builtins.h) — pack a C string into a `text`
    /// Datum (a varlena palloc in `mcx`).
    pub fn cstring_get_text_datum<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<Datum<'mcx>>
);

seam!(
    /// `TextDatumGetCString(datum)` (builtins.h) — detoast a `text` Datum back
    /// to an owned string.
    pub fn text_datum_get_cstring<'mcx>(value: Datum<'mcx>) -> PgResult<String>
);

/* --- pg_seclabel catalog primitives (access/table.h, catalog/indexing.h) --- */

seam!(
    /// `table_open(SecLabelRelationId, lockmode)` (access/table.h).
    pub fn seclabel_open<'mcx>(mcx: Mcx<'mcx>, lockmode: LOCKMODE) -> PgResult<Relation<'mcx>>
);

seam!(
    /// `GetSecurityLabel`'s scan over `{objoid, classoid, objsubid, provider}`,
    /// returning the label-column Datum of the one match (with its null flag),
    /// or `None` if no row matched.
    pub fn seclabel_get_label<'mcx>(
        pg_seclabel: &Relation<'mcx>,
        objoid: Oid,
        classoid: Oid,
        objsubid: i32,
        provider: &str,
    ) -> PgResult<Option<SecLabelColumn<'mcx>>>
);

seam!(
    /// The index scan that finds the single pg_seclabel tuple for `{objoid,
    /// classoid, objsubid, provider}`, returning its row identity or `None`.
    pub fn seclabel_find_one<'mcx>(
        pg_seclabel: &Relation<'mcx>,
        objoid: Oid,
        classoid: Oid,
        objsubid: i32,
        provider: &str,
    ) -> PgResult<Option<ItemPointerData>>
);

seam!(
    /// `CatalogTupleDelete(pg_seclabel, &oldtup->t_self)` (indexing.c).
    pub fn seclabel_delete<'mcx>(pg_seclabel: &Relation<'mcx>, tuple: ItemPointerData) -> PgResult<()>
);

seam!(
    /// `heap_modify_tuple` + `CatalogTupleUpdate` + `heap_freetuple` — replace
    /// the found pg_seclabel tuple from the in-crate `values`/`nulls`/`replaces`
    /// arrays.
    pub fn seclabel_update<'mcx>(
        pg_seclabel: &Relation<'mcx>,
        tuple: ItemPointerData,
        values: &[Datum<'mcx>],
        nulls: &[bool],
        replaces: &[bool],
    ) -> PgResult<()>
);

seam!(
    /// `heap_form_tuple` + `CatalogTupleInsert` + `heap_freetuple` — insert a
    /// fresh pg_seclabel tuple from the in-crate `values`/`nulls` arrays.
    pub fn seclabel_insert<'mcx>(
        pg_seclabel: &Relation<'mcx>,
        values: &[Datum<'mcx>],
        nulls: &[bool],
    ) -> PgResult<()>
);

seam!(
    /// `DeleteSecurityLabel`'s remove-all-matching loop. `objsubid` is `Some`
    /// only when the caller's `objectSubId != 0` (the in-crate `nkeys` decision
    /// of 3 vs 2 scan keys).
    pub fn seclabel_delete_all<'mcx>(
        pg_seclabel: &Relation<'mcx>,
        objoid: Oid,
        classoid: Oid,
        objsubid: Option<i32>,
    ) -> PgResult<()>
);

/* --- pg_shseclabel catalog primitives (shared-object labels) --- */

seam!(
    /// `table_open(SharedSecLabelRelationId, lockmode)` (access/table.h).
    pub fn shseclabel_open<'mcx>(mcx: Mcx<'mcx>, lockmode: LOCKMODE) -> PgResult<Relation<'mcx>>
);

seam!(
    /// `GetSharedSecurityLabel`'s scan over `{objoid, classoid, provider}` (no
    /// objsubid column), returning the label-column Datum of the one match
    /// (with its null flag), or `None`.
    pub fn shseclabel_get_label<'mcx>(
        pg_shseclabel: &Relation<'mcx>,
        objoid: Oid,
        classoid: Oid,
        provider: &str,
    ) -> PgResult<Option<SecLabelColumn<'mcx>>>
);

seam!(
    /// The index scan that finds the single pg_shseclabel tuple for `{objoid,
    /// classoid, provider}` (no objsubid column), returning its row identity or
    /// `None`.
    pub fn shseclabel_find_one<'mcx>(
        pg_shseclabel: &Relation<'mcx>,
        objoid: Oid,
        classoid: Oid,
        provider: &str,
    ) -> PgResult<Option<ItemPointerData>>
);

seam!(
    /// `CatalogTupleDelete(pg_shseclabel, &oldtup->t_self)` (indexing.c).
    pub fn shseclabel_delete<'mcx>(
        pg_shseclabel: &Relation<'mcx>,
        tuple: ItemPointerData,
    ) -> PgResult<()>
);

seam!(
    /// `heap_modify_tuple` + `CatalogTupleUpdate` + `heap_freetuple` for
    /// pg_shseclabel (no objsubid column; `values` has `Natts_pg_shseclabel`
    /// columns).
    pub fn shseclabel_update<'mcx>(
        pg_shseclabel: &Relation<'mcx>,
        tuple: ItemPointerData,
        values: &[Datum<'mcx>],
        nulls: &[bool],
        replaces: &[bool],
    ) -> PgResult<()>
);

seam!(
    /// `heap_form_tuple` + `CatalogTupleInsert` + `heap_freetuple` for
    /// pg_shseclabel.
    pub fn shseclabel_insert<'mcx>(
        pg_shseclabel: &Relation<'mcx>,
        values: &[Datum<'mcx>],
        nulls: &[bool],
    ) -> PgResult<()>
);

seam!(
    /// `DeleteSharedSecurityLabel`'s remove-all-matching loop (always 2 scan
    /// keys: `{objoid, classoid}`).
    pub fn shseclabel_delete_all<'mcx>(
        pg_shseclabel: &Relation<'mcx>,
        objoid: Oid,
        classoid: Oid,
    ) -> PgResult<()>
);

seam!(
    /// `DeleteSecurityLabel(object)` (commands/seclabel.c): remove all security
    /// labels (every provider) attached to `object`. dependency.c's
    /// `deleteObjectsInList` calls this to clean up `pg_seclabel`/`pg_shseclabel`
    /// rows for a dropped object. Allocations land in `mcx`. Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn DeleteSecurityLabel<'mcx>(mcx: Mcx<'mcx>, object: &ObjectAddress) -> PgResult<()>
);
