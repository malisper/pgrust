//! Seams for `backend/commands/seclabel.c` (`SECURITY LABEL`).
//!
//! seclabel.c's whole command-driver and catalog-upsert control flow is ported
//! in-crate (`backend-commands-seclabel`): the provider-default logic, the
//! per-`ObjectType` support whitelist, the `OBJECT_COLUMN` relkind check, the
//! provider relabel-hook invocation, the shared-vs-local catalog dispatch, the
//! `label == NULL` -> delete branch, the found-vs-not-found upsert decision, the
//! `values`/`nulls`/`replaces` array setup, the scan-key values, and the real
//! `pg_seclabel`/`pg_shseclabel` catalog reads and writes (`table_open`, the
//! `systable` index scans, `CatalogTupleDelete` /
//! `heap_modify_tuple`+`CatalogTupleUpdate` / `heap_form_tuple`+
//! `CatalogTupleInsert`) over a real [`rel::Relation`].
//!
//! `get_object_address` / `check_object_ownership` cross the owner's
//! `backend-catalog-objectaddress-seams`; `GetUserId` is the canonical miscinit
//! seam; `IsSharedRelation` and `errdetail_relkind_not_supported` are real
//! ported functions called directly.
//!
//! Only two genuine cross-subsystem boundaries cross outward, both the
//! project-wide Datum/varlena/fmgr deferral:
//!
//!  * [`cstring_get_text_datum`] (`CStringGetTextDatum`) — pack a provider /
//!    label / scan-key string into a `text` `Datum`;
//!  * [`text_datum_get_cstring`] (`TextDatumGetCString`) — detoast a `text`
//!    `Datum` back to a string (`GetSecurityLabel`).
//!
//! Inward, [`DeleteSecurityLabel`] is the boundary dependency.c crosses when an
//! object is dropped (it cleans up the object's `pg_seclabel`/`pg_shseclabel`
//! rows).

#![allow(non_snake_case)]

use mcx::Mcx;
use seam_core::seam;
use types_catalog::catalog_dependency::ObjectAddress;
use types_error::PgResult;
use types_tuple::heaptuple::Datum;

// --- fmgr / varlena (the project-wide Datum/fmgr deferral) -----------------

seam!(
    /// `CStringGetTextDatum(s)` (builtins.h) — pack a C string into a `text`
    /// `Datum` (a varlena palloc in `mcx`), for the provider / label columns and
    /// the `F_TEXTEQ` scan keys.
    pub fn cstring_get_text_datum<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<Datum<'mcx>>
);

seam!(
    /// `TextDatumGetCString(value)` (builtins.h) — detoast a `text` `Datum` back
    /// to an owned string (`GetSecurityLabel`'s label-field read).
    pub fn text_datum_get_cstring<'mcx>(value: Datum<'mcx>) -> PgResult<String>
);

// --- inward boundary -------------------------------------------------------

seam!(
    /// `DeleteSecurityLabel(object)` (commands/seclabel.c): remove all security
    /// labels (every provider) attached to `object`. dependency.c's
    /// `deleteObjectsInList` calls this to clean up `pg_seclabel`/`pg_shseclabel`
    /// rows for a dropped object. Allocations land in `mcx`. Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn DeleteSecurityLabel<'mcx>(mcx: Mcx<'mcx>, object: &ObjectAddress) -> PgResult<()>
);
