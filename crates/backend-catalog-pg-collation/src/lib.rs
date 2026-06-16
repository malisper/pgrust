//! `backend-catalog-pg-collation` — the `pg_collation` catalog-insert owner
//! (`backend/catalog/pg_collation.c`).
//!
//! [`CollationCreate`] is the single routine pg_collation.c exposes: it adds a
//! new `pg_collation` tuple. This crate forms that tuple with a real
//! `heap_form_tuple` over the relation descriptor and inserts it with the
//! `catalog/indexing.c` keystone `CatalogTupleInsert` (the
//! pg_database / pg_type / pg_operator carrier precedent), records the
//! namespace / owner / current-extension dependencies, and fires the
//! post-create object-access hook — control flow reproduced exactly as the C
//! does it:
//!   * the two duplicate probes in C order (the `COLLNAMEENCNSP` syscache OID
//!     probe, then — after taking `ShareRowExclusiveLock` — the any-encoding
//!     shadow probe) with the exact `quiet` / `if_not_exists` / error branches
//!     and their `ERRCODE_DUPLICATE_OBJECT` SQLSTATE + message text (and, on
//!     `if_not_exists`, `checkMembershipInCurrentExtension` before the
//!     skip-notice);
//!   * the `values[]` / `nulls[]` field-formation in the exact `Anum_*` order;
//!   * `GetNewOidWithIndex` → `heap_form_tuple` → `CatalogTupleInsert`;
//!   * `recordDependencyOn(namespace, DEPENDENCY_NORMAL)` →
//!     `recordDependencyOnOwner` → `recordDependencyOnCurrentExtension` →
//!     `InvokeObjectPostCreateHook`;
//!   * `heap_freetuple` (the formed tuple is dropped) + `table_close(NoLock)`.
//!
//! The inward seam this crate installs is `collation_create` (declared in
//! `backend-commands-collationcmds-seams`, consumed by collationcmds.c's CREATE
//! / ALTER drivers and `pg_import_system_collations`). The seam carries a
//! `CollationCreateArgs` bundle (mirroring the 13-parameter C signature) and
//! returns the new OID (or `InvalidOid` for the `quiet` / `if_not_exists`
//! skips). It threads no `Mcx`, so the handler runs its tuple-forming +
//! catalog-mutation work in a scratch `MemoryContext` it owns for the call
//! (the relfilenumbermap idiom), dropped on return — the new OID is a scalar
//! the caller keeps.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]
// PgResult's PgError variant is large; boxing it would diverge from the rest of
// the workspace's vocabulary and from the C (which throws by value).
#![allow(clippy::result_large_err)]

use mcx::{Mcx, MemoryContext};
use types_catalog::catalog::NAMESPACE_RELATION_ID;
use types_catalog::catalog_dependency::{ObjectAddress, DEPENDENCY_NORMAL};
use types_catalog::pg_collation as cat;
use types_core::primitive::{AttrNumber, InvalidOid, Oid, OidIsValid};
use types_storage::lock::{NoLock, ShareRowExclusiveLock};
use types_tuple::backend_access_common_heaptuple::Datum;

use backend_utils_error::ereport;
use types_error::pg_error::ErrorLocation;
use types_error::{PgResult, ERRCODE_DUPLICATE_OBJECT, ERROR, NOTICE};

use backend_access_common_heaptuple::heap_form_tuple;
use backend_catalog_catalog::GetNewOidWithIndex;
use backend_catalog_indexing::keystone::CatalogTupleInsert;
use backend_catalog_pg_depend::{
    checkMembershipInCurrentExtension, recordDependencyOn, recordDependencyOnCurrentExtension,
};
use backend_catalog_pg_shdepend::recordDependencyOnOwner;
use backend_utils_cache_syscache::{GetSysCacheOid, COLLNAMEENCNSP};
use types_cache::syscache::SysCacheKey;
use types_datum::Datum as ScalarWord;

use backend_access_table_table_seams as table_seams;
use backend_catalog_objectaccess_seams as objectaccess_seams;
use backend_commands_collationcmds_seams::{collation_create, CollationCreateArgs};
use backend_utils_adt_varlena_seams as varlena_seams;
use backend_utils_mb_mbutils_seams as mbutils_seams;
use common_encnames_seams as encnames_seams;

/// `ErrorLocation` for `ereport(...).finish(...)` in this module. The C line
/// number is not tracked (it is `0`).
fn here(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("../src/backend/catalog/pg_collation.c", 0, funcname)
}

/// `ObjectAddressSet(object, class_id, object_id)` (objectaddress.h): set
/// `classId`/`objectId` and zero `objectSubId`.
fn object_address_set(class_id: Oid, object_id: Oid) -> ObjectAddress {
    ObjectAddress {
        classId: class_id,
        objectId: object_id,
        objectSubId: 0,
    }
}

/// `GetSysCacheOid3(COLLNAMEENCNSP, Anum_pg_collation_oid,
/// PointerGetDatum(collname), Int32GetDatum(encoding),
/// ObjectIdGetDatum(nsp))` — the duplicate-name probe. `GetSysCacheOid3` is the
/// macro `GetSysCacheOid(cacheId, oidcol, k1, k2, k3, 0)`.
fn get_syscache_oid3_collnameencnsp(
    mcx: Mcx<'_>,
    collname: &str,
    encoding: i32,
    nsp: Oid,
) -> PgResult<Oid> {
    GetSysCacheOid(
        mcx,
        COLLNAMEENCNSP,
        cat::Anum_pg_collation_oid as AttrNumber,
        SysCacheKey::Str(collname),
        SysCacheKey::Value(ScalarWord::from_i32(encoding)),
        SysCacheKey::Value(ScalarWord::from_oid(nsp)),
        SysCacheKey::UNUSED,
    )
}

/// `CStringGetTextDatum(s)` — build a `text` varlena `Datum` (varlena.c).
fn text_datum<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<Datum<'mcx>> {
    varlena_seams::cstring_to_text_v::call(mcx, s)
}

/// `namestrcpy(&name_name, collname)` + `NameGetDatum(&name_name)` — a 64-byte
/// NUL-padded `NameData` by-reference Datum image.
fn name_datum<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<Datum<'mcx>> {
    const NAMEDATALEN: usize = 64;
    let mut image: mcx::PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, NAMEDATALEN)?;
    let src = s.as_bytes();
    let take = core::cmp::min(src.len(), NAMEDATALEN - 1);
    for &b in &src[..take] {
        image.push(b);
    }
    while image.len() < NAMEDATALEN {
        image.push(0);
    }
    Ok(Datum::ByRef(image))
}

/// CollationCreate (pg_collation.c)
///
/// Add a new tuple to pg_collation.
///
/// `if_not_exists`: if true, don't fail on duplicate name, just print a notice
/// and return `InvalidOid`.
/// `quiet`: if true, don't fail on duplicate name, just silently return
/// `InvalidOid` (overrides `if_not_exists`).
///
/// The variable-length text columns
/// (`collcollate`/`collctype`/`colllocale`/`collicurules`/`collversion`) are
/// `Option<&str>` (a `None` column is the C `NULL`, which sets `nulls[i] =
/// true`). `collprovider` is the `char` column value (`COLLPROVIDER_*`).
pub fn CollationCreate<'mcx>(
    mcx: Mcx<'mcx>,
    collname: &str,
    collnamespace: Oid,
    collowner: Oid,
    collprovider: i8,
    collisdeterministic: bool,
    collencoding: i32,
    collcollate: Option<&str>,
    collctype: Option<&str>,
    colllocale: Option<&str>,
    collicurules: Option<&str>,
    collversion: Option<&str>,
    if_not_exists: bool,
    quiet: bool,
) -> PgResult<Oid> {
    let mut oid: Oid;

    debug_assert!(!collname.is_empty());
    debug_assert!(OidIsValid(collnamespace));
    debug_assert!(OidIsValid(collowner));
    debug_assert!(
        (collprovider == cat::COLLPROVIDER_LIBC
            && collcollate.is_some()
            && collctype.is_some()
            && colllocale.is_none())
            || (collprovider != cat::COLLPROVIDER_LIBC
                && collcollate.is_none()
                && collctype.is_none()
                && colllocale.is_some())
    );

    /*
     * Make sure there is no existing collation of same name & encoding.
     *
     * This would be caught by the unique index anyway; we're just giving a
     * friendlier error message.  The unique index provides a backstop against
     * race conditions.
     */
    oid = get_syscache_oid3_collnameencnsp(mcx, collname, collencoding, collnamespace)?;
    if OidIsValid(oid) {
        if quiet {
            return Ok(InvalidOid);
        } else if if_not_exists {
            /*
             * If we are in an extension script, insist that the pre-existing
             * object be a member of the extension, to avoid security risks.
             */
            let myself = object_address_set(cat::CollationRelationId, oid);
            checkMembershipInCurrentExtension(mcx, &myself)?;

            /* OK to skip */
            let msg = if collencoding == -1 {
                format!("collation \"{collname}\" already exists, skipping")
            } else {
                format!(
                    "collation \"{collname}\" for encoding \"{}\" already exists, skipping",
                    encnames_seams::pg_encoding_to_char::call(collencoding)
                )
            };
            ereport(NOTICE)
                .errcode(ERRCODE_DUPLICATE_OBJECT)
                .errmsg(msg)
                .finish(here("CollationCreate"))?;
            return Ok(InvalidOid);
        } else {
            let msg = if collencoding == -1 {
                format!("collation \"{collname}\" already exists")
            } else {
                format!(
                    "collation \"{collname}\" for encoding \"{}\" already exists",
                    encnames_seams::pg_encoding_to_char::call(collencoding)
                )
            };
            return Err(ereport(ERROR)
                .errcode(ERRCODE_DUPLICATE_OBJECT)
                .errmsg(msg)
                .into_error());
        }
    }

    /* open pg_collation; see below about the lock level */
    let rel = table_seams::table_open::call(mcx, cat::CollationRelationId, ShareRowExclusiveLock)?;

    /*
     * Also forbid a specific-encoding collation shadowing an any-encoding
     * collation, or an any-encoding collation being shadowed (see
     * get_collation_name()).  This test is not backed up by the unique index,
     * so we take a ShareRowExclusiveLock earlier, to protect against
     * concurrent changes fooling this check.
     */
    if collencoding == -1 {
        oid = get_syscache_oid3_collnameencnsp(
            mcx,
            collname,
            mbutils_seams::get_database_encoding::call(),
            collnamespace,
        )?;
    } else {
        oid = get_syscache_oid3_collnameencnsp(mcx, collname, -1, collnamespace)?;
    }
    if OidIsValid(oid) {
        if quiet {
            rel.close(NoLock)?;
            return Ok(InvalidOid);
        } else if if_not_exists {
            /*
             * If we are in an extension script, insist that the pre-existing
             * object be a member of the extension, to avoid security risks.
             */
            let myself = object_address_set(cat::CollationRelationId, oid);
            checkMembershipInCurrentExtension(mcx, &myself)?;

            /* OK to skip */
            rel.close(NoLock)?;
            ereport(NOTICE)
                .errcode(ERRCODE_DUPLICATE_OBJECT)
                .errmsg(format!("collation \"{collname}\" already exists, skipping"))
                .finish(here("CollationCreate"))?;
            return Ok(InvalidOid);
        } else {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_DUPLICATE_OBJECT)
                .errmsg(format!("collation \"{collname}\" already exists"))
                .into_error());
        }
    }

    /* form a tuple */
    let tupdesc = rel.rd_att_clone_in(mcx)?;

    // memset(nulls, 0, sizeof(nulls));
    let mut nulls = [false; cat::Natts_pg_collation];
    let idx = |attno: i32| (attno - 1) as usize;
    let mut values: [Datum<'mcx>; cat::Natts_pg_collation] =
        core::array::from_fn(|_| Datum::null());

    // namestrcpy(&name_name, collname);
    oid = GetNewOidWithIndex(
        &rel,
        cat::CollationOidIndexId,
        cat::Anum_pg_collation_oid as AttrNumber,
    )?;
    values[idx(cat::Anum_pg_collation_oid)] = Datum::from_oid(oid);
    values[idx(cat::Anum_pg_collation_collname)] = name_datum(mcx, collname)?;
    values[idx(cat::Anum_pg_collation_collnamespace)] = Datum::from_oid(collnamespace);
    values[idx(cat::Anum_pg_collation_collowner)] = Datum::from_oid(collowner);
    values[idx(cat::Anum_pg_collation_collprovider)] = Datum::from_char(collprovider);
    values[idx(cat::Anum_pg_collation_collisdeterministic)] = Datum::from_bool(collisdeterministic);
    values[idx(cat::Anum_pg_collation_collencoding)] = Datum::from_i32(collencoding);

    match collcollate {
        Some(s) => values[idx(cat::Anum_pg_collation_collcollate)] = text_datum(mcx, s)?,
        None => nulls[idx(cat::Anum_pg_collation_collcollate)] = true,
    }
    match collctype {
        Some(s) => values[idx(cat::Anum_pg_collation_collctype)] = text_datum(mcx, s)?,
        None => nulls[idx(cat::Anum_pg_collation_collctype)] = true,
    }
    match colllocale {
        Some(s) => values[idx(cat::Anum_pg_collation_colllocale)] = text_datum(mcx, s)?,
        None => nulls[idx(cat::Anum_pg_collation_colllocale)] = true,
    }
    match collicurules {
        Some(s) => values[idx(cat::Anum_pg_collation_collicurules)] = text_datum(mcx, s)?,
        None => nulls[idx(cat::Anum_pg_collation_collicurules)] = true,
    }
    match collversion {
        Some(s) => values[idx(cat::Anum_pg_collation_collversion)] = text_datum(mcx, s)?,
        None => nulls[idx(cat::Anum_pg_collation_collversion)] = true,
    }

    let mut tup = heap_form_tuple(mcx, &tupdesc, &values, &nulls)?;

    /* insert a new tuple */
    CatalogTupleInsert(mcx, &rel, &mut tup)?;
    debug_assert!(OidIsValid(oid));

    /* set up dependencies for the new collation */
    let myself = ObjectAddress {
        classId: cat::CollationRelationId,
        objectId: oid,
        objectSubId: 0,
    };

    /* create dependency on namespace */
    let referenced = ObjectAddress {
        classId: NAMESPACE_RELATION_ID,
        objectId: collnamespace,
        objectSubId: 0,
    };
    recordDependencyOn(mcx, &myself, &referenced, DEPENDENCY_NORMAL)?;

    /* create dependency on owner */
    recordDependencyOnOwner(cat::CollationRelationId, oid, collowner)?;

    /* dependency on extension */
    recordDependencyOnCurrentExtension(mcx, &myself, false)?;

    /* Post creation hook for new collation */
    objectaccess_seams::invoke_object_post_create_hook::call(cat::CollationRelationId, oid, 0)?;

    /* heap_freetuple(tup): the formed tuple is dropped at end of scope. */
    drop(tup);
    rel.close(NoLock)?;

    Ok(oid)
}

/// The inward `collation_create` seam handler (declared in
/// `backend-commands-collationcmds-seams`). The seam threads no `Mcx`, so the
/// handler runs the whole `CollationCreate` (tuple forming + catalog mutation)
/// in a scratch `MemoryContext` it owns for the call, dropped on return — the
/// new OID is a scalar the caller keeps. The `CollationCreateArgs` text fields
/// are owned `String`s (SQL NULL ⇒ `None`); they are passed as `&str` to
/// `CollationCreate`.
fn collation_create_handler(args: CollationCreateArgs) -> PgResult<Oid> {
    let scratch = MemoryContext::new("CollationCreate");
    let mcx = scratch.mcx();
    CollationCreate(
        mcx,
        &args.collname,
        args.collnamespace,
        args.collowner,
        args.collprovider,
        args.collisdeterministic,
        args.collencoding,
        args.collcollate.as_deref(),
        args.collctype.as_deref(),
        args.colllocale.as_deref(),
        args.collicurules.as_deref(),
        args.collversion.as_deref(),
        args.if_not_exists,
        args.quiet,
    )
}

/// Install every inward seam this unit owns. Wired into `seams-init::init_all`.
pub fn init_seams() {
    collation_create::set(collation_create_handler);
}
