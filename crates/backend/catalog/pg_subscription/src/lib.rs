//! `backend-catalog-pg-subscription` — the `pg_subscription` /
//! `pg_subscription_rel` shared replication catalogs read + mutate owner
//! (`backend/catalog/pg_subscription.c`).
//!
//! Faithful in-crate port of every `pg_subscription.c` function:
//! [`GetSubscription`], [`FreeSubscription`], [`CountDBSubscriptions`],
//! [`DisableSubscription`], [`textarray_to_stringlist`],
//! [`AddSubscriptionRelState`], [`UpdateSubscriptionRelState`],
//! [`GetSubscriptionRelState`], [`RemoveSubscriptionRel`],
//! [`HasSubscriptionRelations`], [`GetSubscriptionRelations`]. The branching,
//! error conditions, lock levels, NULL-handling and the `SUBREL_STATE_UNKNOWN`
//! / `InvalidXLogRecPtr` sentinels are ported 1:1.
//!
//! The carrier model mirrors the landed `backend-catalog-pg-database` /
//! `backend-catalog-pg-publication` owners: real `SearchSysCache*` reads,
//! `heap_form_tuple` / `heap_modify_tuple` + `CatalogTupleInsert` / `Update` /
//! `Delete` (the `catalog/indexing.c` engine consumed as `pub` functions from
//! `backend-catalog-indexing`, no cycle), and `systable_beginscan` /
//! `systable_getnext` keyed scans. The decoded `pg_subscription` /
//! `pg_subscription_rel` rows cross the boundary as the owned
//! [`::types_catalog::pg_subscription::Subscription`] /
//! [`::types_catalog::pg_subscription::SubscriptionRelState`] carriers, so
//! consumers never touch the datum layout.
//!
//! `GetPublicationsStr` lives in `pg_subscription.c` too, but it is pure
//! `StringInfo` formatting over a `List *` and depends on `quote_literal_cstr`;
//! it is not part of this catalog read/mutate surface and is left to the
//! StringInfo-carrying owner (no consumer in tree).

#![allow(non_snake_case)]

extern crate alloc;

use mcx::{Mcx, PgString, PgVec};
use ::types_catalog::pg_subscription as cat;
use ::types_core::fmgr::{F_CHARNE, F_OIDEQ};
use ::types_core::primitive::{AttrNumber, Oid, XLogRecPtr};
use ::types_core::xact::InvalidXLogRecPtr;
use ::types_core::OidIsValid;
use types_error::{PgError, PgResult};
use ::types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use ::types_storage::lock::{AccessShareLock, NoLock, RowExclusiveLock};
use types_tuple::heaptuple::{Datum, DeformedColumn};

use heaptuple::{heap_deform_tuple, heap_form_tuple, heap_modify_tuple};
use ::scankey::ScanKeyInit;
use ::indexing::keystone::{
    CatalogTupleDelete, CatalogTupleInsert, CatalogTupleUpdate,
};

use genam_seams as genam_seams;
use table_seams as table_seams;
use transam_xact_seams as xact_seams;
use lmgr_seams as lmgr_seams;
use arrayfuncs_seams as array_seams;
use varlena_seams as varlena_seams;
use lsyscache_seams as lsyscache_seams;
use cache_syscache::{SearchSysCache1, SearchSysCache2};
use ::utils_error::ereport;
use superuser_seams as superuser_seams;

use ::cache::SysCacheKey;
use ::datum::Datum as KeyDatum;
use types_error::{ERRCODE_INVALID_PARAMETER_VALUE, ERROR};
use types_syscache::{SUBSCRIPTIONOID, SUBSCRIPTIONRELMAP};

// The launcher's trimmed `Subscription` summary (`replication/launcher.c`'s
// `get_subscription_list` local struct), distinct from the full
// `::types_catalog::pg_subscription::Subscription` re-exported below.
use ::replication_launcher::Subscription as LauncherSubscription;

pub use ::types_catalog::pg_subscription::{Subscription, SubscriptionRelState};

/* ==========================================================================
 * Scan-key builders.
 * ========================================================================== */

/// `ScanKeyInit(&key, attno, BTEqualStrategyNumber, F_OIDEQ,
/// ObjectIdGetDatum(value))`.
fn oid_key<'mcx>(attno: AttrNumber, value: Oid) -> PgResult<ScanKeyData<'mcx>> {
    let mut key = ScanKeyData::empty();
    ScanKeyInit(&mut key, attno, BTEqualStrategyNumber, F_OIDEQ, Datum::from_oid(value))?;
    Ok(key)
}

/// `ScanKeyInit(&key, attno, BTEqualStrategyNumber, F_CHARNE,
/// CharGetDatum(value))`.
fn char_ne_key<'mcx>(attno: AttrNumber, value: i8) -> PgResult<ScanKeyData<'mcx>> {
    let mut key = ScanKeyData::empty();
    ScanKeyInit(&mut key, attno, BTEqualStrategyNumber, F_CHARNE, Datum::from_char(value))?;
    Ok(key)
}

/// `ObjectIdGetDatum(value)` as a syscache key word.
fn oid_cache_key(value: Oid) -> SysCacheKey<'static> {
    SysCacheKey::Value(KeyDatum::from_oid(value))
}

/* ==========================================================================
 * Decode helpers (one cached/scanned tuple column).
 * ========================================================================== */

/// Read a `NameData` (64-byte, NUL-padded) by-value image out of a deformed
/// column as a `PgString` (the bytes up to the first NUL). The C reads it via
/// `NameStr(*DatumGetName(datum))` / `pstrdup(NameStr(subform->subname))`.
fn name_to_string<'mcx>(mcx: Mcx<'mcx>, col: &DeformedColumn<'mcx>) -> PgResult<PgString<'mcx>> {
    let bytes: &[u8] = match &col.0 {
        Datum::ByRef(b) => b,
        Datum::ByVal(_)
        | Datum::Cstring(_)
        | Datum::Composite(_)
        | Datum::Expanded(_)
        | Datum::Internal(_) => &[],
    };
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    let s = core::str::from_utf8(&bytes[..end])
        .map_err(|_| PgError::error("pg_subscription name column is not valid UTF-8"))?;
    PgString::from_str_in(s, mcx)
}

/// `TextDatumGetCString(datum)` for one (not-null) `text` column — detoast +
/// copy the payload out as a `PgString` (`text_to_cstring`, varlena.c).
fn text_to_string<'mcx>(mcx: Mcx<'mcx>, d: &Datum<'mcx>) -> PgResult<PgString<'mcx>> {
    varlena_seams::text_to_cstring_v::call(mcx, d)
}

/// Verbatim varlena bytes of a (not-null) by-reference column.
fn byref_bytes<'a, 'mcx>(d: &'a Datum<'mcx>) -> &'a [u8] {
    match d {
        Datum::ByRef(b) => b,
        Datum::ByVal(_)
        | Datum::Cstring(_)
        | Datum::Composite(_)
        | Datum::Expanded(_)
        | Datum::Internal(_) => &[],
    }
}

/// `DatumGetLSN(d)` — an `XLogRecPtr` carried as a pass-by-value 8-byte word.
fn datum_get_lsn(d: &Datum<'_>) -> XLogRecPtr {
    d.as_u64()
}

/// `LSNGetDatum(value)`.
fn lsn_datum<'mcx>(value: XLogRecPtr) -> Datum<'mcx> {
    Datum::from_u64(value)
}

/* ==========================================================================
 * textarray_to_stringlist (pg_subscription.c:240)
 * ========================================================================== */

/// Convert a `text[]` array varlena image to a list of strings.
///
/// `deconstruct_array_builtin(textarray, TEXTOID, ...)` then
/// `lappend(makeString(TextDatumGetCString(elems[i])))` for each element; an
/// empty array short-circuits to the empty list. The seam returns the decoded
/// element strings directly.
fn textarray_to_stringlist<'mcx>(
    mcx: Mcx<'mcx>,
    textarray: &[u8],
) -> PgResult<PgVec<'mcx, PgString<'mcx>>> {
    // deconstruct_array_builtin(textarray, TEXTOID, &elems, NULL, &nelems);
    let elems = array_seams::deconstruct_text_array::call(mcx, textarray)?;

    // if (nelems == 0) return NIL;
    if elems.is_empty() {
        return ::mcx::vec_with_capacity_in(mcx, 0);
    }

    // for (i = 0; i < nelems; i++)
    //     res = lappend(res, makeString(TextDatumGetCString(elems[i])));
    let mut res: PgVec<'mcx, PgString<'mcx>> = ::mcx::vec_with_capacity_in(mcx, elems.len())?;
    for s in elems.iter() {
        res.push(PgString::from_str_in(s.as_str(), mcx)?);
    }
    Ok(res)
}

/* ==========================================================================
 * get_subscription_list (launcher.c) — the logical-replication launcher's
 * private full scan of pg_subscription. Lives here (not launcher.c) because it
 * is a catalog/heapam/xact read; the launcher only consumes the resulting list.
 * ========================================================================== */

/// `get_subscription_list(void)` (launcher.c): inside a fresh transaction, do a
/// full sequential catalog scan of `pg_subscription` and build the list of
/// launcher-relevant [`LauncherSubscription`] summaries (`oid`, `subdbid`,
/// `subowner`, `subenabled`, `pstrdup(NameStr(subname))`). On a fresh database
/// with no subscriptions this returns the empty list, which is what lets the
/// launcher idle on its latch.
fn get_subscription_list<'mcx>(
    mcx: Mcx<'mcx>,
) -> PgResult<PgVec<'mcx, LauncherSubscription>> {
    // This is the context that we will allocate our output data in. In C the
    // caller's CurrentMemoryContext is the per-cycle sublist context; here it
    // is the `mcx` the seam was handed. The transaction below runs in its own
    // (transaction) context, and the list elements are owned (`String`), so
    // there is no leak across the StartTransaction/CommitTransaction boundary.
    let mut res: PgVec<'mcx, LauncherSubscription> = ::mcx::vec_with_capacity_in(mcx, 0)?;

    // Start a transaction so we can access pg_subscription.
    xact_seams::start_transaction_command::call()?;

    // rel = table_open(SubscriptionRelationId, AccessShareLock);
    let rel = table_seams::table_open::call(mcx, cat::SubscriptionRelationId, AccessShareLock)?;

    // scan = table_beginscan_catalog(rel, 0, NULL); — a keyless sequential
    // catalog scan (the systable_beginscan analog with index_ok = false, nkeys
    // = 0), driven with heap_getnext / ForwardScanDirection.
    let keys: [ScanKeyData<'_>; 0] = [];
    let mut scan = genam_seams::systable_beginscan::call(&rel, 0, false, None, &keys)?;

    let desc = rel.rd_att_clone_in(mcx)?;
    // while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))
    while let Some(tup) = genam_seams::systable_getnext::call(mcx, scan.desc_mut())? {
        // subform = (Form_pg_subscription) GETSTRUCT(tup);
        let cols = heap_deform_tuple(mcx, &tup.tuple, &desc, &tup.data)?;
        let col = |attno: i32| &cols[(attno - 1) as usize];

        // sub = palloc0(sizeof(Subscription));
        // sub->oid = subform->oid; sub->dbid = subform->subdbid;
        // sub->owner = subform->subowner; sub->enabled = subform->subenabled;
        // sub->name = pstrdup(NameStr(subform->subname));
        // (We don't fill fields we are not interested in.)
        let name = name_to_string(mcx, col(cat::Anum_pg_subscription_subname))?;
        let sub = LauncherSubscription {
            oid: col(cat::Anum_pg_subscription_oid).0.as_oid(),
            dbid: col(cat::Anum_pg_subscription_subdbid).0.as_oid(),
            owner: col(cat::Anum_pg_subscription_subowner).0.as_oid(),
            enabled: col(cat::Anum_pg_subscription_subenabled).0.as_bool(),
            name: alloc::string::String::from(name.as_str()),
        };

        // res = lappend(res, sub);
        res.push(sub);
    }

    // table_endscan(scan);
    scan.end()?;
    // table_close(rel, AccessShareLock);
    rel.close(AccessShareLock)?;

    // CommitTransactionCommand();
    xact_seams::commit_transaction_command::call()?;

    res.shrink_to_fit();
    Ok(res)
}

/* ==========================================================================
 * GetSubscription (pg_subscription.c:71)
 * ========================================================================== */

/// Fetch the subscription from the syscache.
fn GetSubscription<'mcx>(
    mcx: Mcx<'mcx>,
    subid: Oid,
    missing_ok: bool,
) -> PgResult<Option<Subscription<'mcx>>> {
    // tup = SearchSysCache1(SUBSCRIPTIONOID, ObjectIdGetDatum(subid));
    let tup = SearchSysCache1(mcx, SUBSCRIPTIONOID, oid_cache_key(subid))?;
    let Some(tup) = tup else {
        // if (!HeapTupleIsValid(tup))
        if missing_ok {
            return Ok(None);
        }
        // elog(ERROR, "cache lookup failed for subscription %u", subid);
        return Err(PgError::error(alloc_format(format_args!(
            "cache lookup failed for subscription {subid}"
        ))));
    };

    // subform = (Form_pg_subscription) GETSTRUCT(tup);
    let rel = table_seams::table_open::call(mcx, cat::SubscriptionRelationId, AccessShareLock)?;
    let desc = rel.rd_att_clone_in(mcx)?;
    let cols = heap_deform_tuple(mcx, &tup.tuple, &desc, &tup.data)?;
    rel.close(AccessShareLock)?;

    let col = |attno: i32| &cols[(attno - 1) as usize];

    // The fixed-width columns come straight off GETSTRUCT.
    let owner = col(cat::Anum_pg_subscription_subowner).0.as_oid();

    // sub = (Subscription *) palloc(sizeof(Subscription));
    let sub = Subscription {
        oid: subid,
        dbid: col(cat::Anum_pg_subscription_subdbid).0.as_oid(),
        skiplsn: col(cat::Anum_pg_subscription_subskiplsn).0.as_u64(),
        // sub->name = pstrdup(NameStr(subform->subname));
        name: name_to_string(mcx, col(cat::Anum_pg_subscription_subname))?,
        owner,
        enabled: col(cat::Anum_pg_subscription_subenabled).0.as_bool(),
        binary: col(cat::Anum_pg_subscription_subbinary).0.as_bool(),
        stream: col(cat::Anum_pg_subscription_substream).0.as_char(),
        twophasestate: col(cat::Anum_pg_subscription_subtwophasestate).0.as_char(),
        disableonerr: col(cat::Anum_pg_subscription_subdisableonerr).0.as_bool(),
        passwordrequired: col(cat::Anum_pg_subscription_subpasswordrequired).0.as_bool(),
        runasowner: col(cat::Anum_pg_subscription_subrunasowner).0.as_bool(),
        failover: col(cat::Anum_pg_subscription_subfailover).0.as_bool(),

        // Get conninfo — SysCacheGetAttrNotNull + TextDatumGetCString.
        conninfo: text_to_string(mcx, &col(cat::Anum_pg_subscription_subconninfo).0)?,

        // Get slotname — SysCacheGetAttr; NULL -> None.
        slotname: {
            let c = col(cat::Anum_pg_subscription_subslotname);
            if c.1 {
                None
            } else {
                Some(name_to_string(mcx, c)?)
            }
        },

        // Get synccommit — SysCacheGetAttrNotNull + TextDatumGetCString.
        synccommit: text_to_string(mcx, &col(cat::Anum_pg_subscription_subsynccommit).0)?,

        // Get publications — SysCacheGetAttrNotNull + textarray_to_stringlist.
        publications: textarray_to_stringlist(
            mcx,
            byref_bytes(&col(cat::Anum_pg_subscription_subpublications).0),
        )?,

        // Get origin — SysCacheGetAttrNotNull + TextDatumGetCString.
        origin: text_to_string(mcx, &col(cat::Anum_pg_subscription_suborigin).0)?,

        // Is the subscription owner a superuser?
        // sub->ownersuperuser = superuser_arg(sub->owner);
        ownersuperuser: superuser_seams::superuser_arg::call(owner)?,
    };

    // ReleaseSysCache(tup); — the FormedTuple is owned/dropped.
    Ok(Some(sub))
}

/* ==========================================================================
 * FreeSubscription (pg_subscription.c:185)
 * ========================================================================== */

/// Free memory allocated by subscription struct. The C `pfree`s each field and
/// the struct; here that is the `Drop` of the owned [`Subscription`] (consumed
/// by this call), preserving the lifetime contract 1:1.
pub fn FreeSubscription(sub: Subscription<'_>) {
    // pfree(sub->name); pfree(sub->conninfo); if (sub->slotname) pfree(...);
    // list_free_deep(sub->publications); pfree(sub);
    drop(sub);
}

/* ==========================================================================
 * CountDBSubscriptions (pg_subscription.c:153)
 * ========================================================================== */

/// Return number of subscriptions defined in given database.
fn CountDBSubscriptions(mcx: Mcx<'_>, dbid: Oid) -> PgResult<i32> {
    // rel = table_open(SubscriptionRelationId, RowExclusiveLock);
    let rel = table_seams::table_open::call(mcx, cat::SubscriptionRelationId, RowExclusiveLock)?;

    // ScanKeyInit(&scankey, Anum_pg_subscription_subdbid, ..., F_OIDEQ, dbid);
    let keys = [oid_key(cat::Anum_pg_subscription_subdbid as AttrNumber, dbid)?];

    // scan = systable_beginscan(rel, InvalidOid, false, NULL, 1, &scankey);
    let mut scan = genam_seams::systable_beginscan::call(&rel, 0, false, None, &keys)?;

    // while (HeapTupleIsValid(tup = systable_getnext(scan))) nsubs++;
    let mut nsubs: i32 = 0;
    while genam_seams::systable_getnext::call(mcx, scan.desc_mut())?.is_some() {
        nsubs += 1;
    }

    // systable_endscan(scan);
    scan.end()?;
    // table_close(rel, NoLock);
    rel.close(NoLock)?;

    Ok(nsubs)
}

/* ==========================================================================
 * DisableSubscription (pg_subscription.c:199)
 * ========================================================================== */

/// Disable the given subscription.
fn DisableSubscription(mcx: Mcx<'_>, subid: Oid) -> PgResult<()> {
    // rel = table_open(SubscriptionRelationId, RowExclusiveLock);
    let rel = table_seams::table_open::call(mcx, cat::SubscriptionRelationId, RowExclusiveLock)?;

    // tup = SearchSysCacheCopy1(SUBSCRIPTIONOID, ObjectIdGetDatum(subid));
    let tup = SearchSysCache1(mcx, SUBSCRIPTIONOID, oid_cache_key(subid))?;
    let Some(tup) = tup else {
        // if (!HeapTupleIsValid(tup))
        //     elog(ERROR, "cache lookup failed for subscription %u", subid);
        rel.close(NoLock)?;
        return Err(PgError::error(alloc_format(format_args!(
            "cache lookup failed for subscription {subid}"
        ))));
    };

    // LockSharedObject(SubscriptionRelationId, subid, 0, AccessShareLock);
    let lock = lmgr_seams::lock_shared_object::call(
        cat::SubscriptionRelationId,
        subid,
        0,
        AccessShareLock,
    )?;

    // memset(values/nulls/replaces); set subenabled = false; replaces = true.
    let mut values: [Datum<'_>; cat::Natts_pg_subscription] =
        core::array::from_fn(|_| Datum::null());
    let nulls = [false; cat::Natts_pg_subscription];
    let mut replaces = [false; cat::Natts_pg_subscription];
    let idx = |attno: i32| (attno - 1) as usize;

    values[idx(cat::Anum_pg_subscription_subenabled)] = Datum::from_bool(false);
    replaces[idx(cat::Anum_pg_subscription_subenabled)] = true;

    // tup = heap_modify_tuple(tup, RelationGetDescr(rel), values, nulls, replaces);
    let desc = rel.rd_att_clone_in(mcx)?;
    let mut newtup = heap_modify_tuple(mcx, &tup, &desc, &values, &nulls, &replaces)
        .map_err(|e| PgError::error(alloc_format(format_args!("heap_modify_tuple failed: {e:?}"))))?;

    // CatalogTupleUpdate(rel, &tup->t_self, tup);
    let otid = newtup.tuple.t_self;
    CatalogTupleUpdate(mcx, &rel, otid, &mut newtup)?;
    // heap_freetuple(tup); — owned/dropped.

    // The C holds the shared-object lock until transaction end.
    lock.keep();

    // table_close(rel, NoLock);
    rel.close(NoLock)?;
    Ok(())
}

/* ==========================================================================
 * AddSubscriptionRelState (pg_subscription.c:266)
 * ========================================================================== */

/// Add new state record for a subscription table.
fn AddSubscriptionRelState(
    mcx: Mcx<'_>,
    subid: Oid,
    relid: Oid,
    state: u8,
    sublsn: XLogRecPtr,
    retain_lock: bool,
) -> PgResult<()> {
    // LockSharedObject(SubscriptionRelationId, subid, 0, AccessShareLock);
    let lock = lmgr_seams::lock_shared_object::call(
        cat::SubscriptionRelationId,
        subid,
        0,
        AccessShareLock,
    )?;

    // rel = table_open(SubscriptionRelRelationId, RowExclusiveLock);
    let rel = table_seams::table_open::call(mcx, cat::SubscriptionRelRelationId, RowExclusiveLock)?;

    // tup = SearchSysCacheCopy2(SUBSCRIPTIONRELMAP, relid, subid);
    // if (HeapTupleIsValid(tup)) elog(ERROR, "... already exists");
    let existing = SearchSysCache2(
        mcx,
        SUBSCRIPTIONRELMAP,
        oid_cache_key(relid),
        oid_cache_key(subid),
    )?;
    if existing.is_some() {
        rel.close(RowExclusiveLock)?;
        // The shared-object lock is released on the abort path (LockGuard Drop).
        return Err(PgError::error(alloc_format(format_args!(
            "subscription table {relid} in subscription {subid} already exists"
        ))));
    }

    // Form the tuple.
    let mut values: [Datum<'_>; cat::Natts_pg_subscription_rel] =
        core::array::from_fn(|_| Datum::null());
    let mut nulls = [false; cat::Natts_pg_subscription_rel];
    let idx = |attno: i32| (attno - 1) as usize;

    values[idx(cat::Anum_pg_subscription_rel_srsubid)] = Datum::from_oid(subid);
    values[idx(cat::Anum_pg_subscription_rel_srrelid)] = Datum::from_oid(relid);
    values[idx(cat::Anum_pg_subscription_rel_srsubstate)] = Datum::from_char(state as i8);
    if sublsn != InvalidXLogRecPtr {
        values[idx(cat::Anum_pg_subscription_rel_srsublsn)] = lsn_datum(sublsn);
    } else {
        nulls[idx(cat::Anum_pg_subscription_rel_srsublsn)] = true;
    }

    // tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);
    let desc = rel.rd_att_clone_in(mcx)?;
    let mut tup = heap_form_tuple(mcx, &desc, &values, &nulls)
        .map_err(|e| PgError::error(alloc_format(format_args!("heap_form_tuple failed: {e:?}"))))?;

    // CatalogTupleInsert(rel, tup);
    CatalogTupleInsert(mcx, &rel, &mut tup)?;
    // heap_freetuple(tup); — owned/dropped.

    // Cleanup.
    if retain_lock {
        // table_close(rel, NoLock);
        rel.close(NoLock)?;
        // Locks retained until transaction end.
        lock.keep();
    } else {
        // table_close(rel, RowExclusiveLock);
        rel.close(RowExclusiveLock)?;
        // UnlockSharedObject(SubscriptionRelationId, subid, 0, AccessShareLock);
        lock.release()?;
    }
    Ok(())
}

/* ==========================================================================
 * UpdateSubscriptionRelState (pg_subscription.c:320)
 * ========================================================================== */

/// Update the state of a subscription table.
fn UpdateSubscriptionRelState(
    mcx: Mcx<'_>,
    subid: Oid,
    relid: Oid,
    state: u8,
    sublsn: XLogRecPtr,
    already_locked: bool,
) -> PgResult<()> {
    // The USE_ASSERT_CHECKING lock-held asserts of the C are debug-only and
    // require a lock-introspection facility this layer doesn't model; the
    // observable behaviour (open at NoLock vs. lock-then-open) is preserved.
    let (rel, lock) = if already_locked {
        // rel = table_open(SubscriptionRelRelationId, NoLock);
        let rel =
            table_seams::table_open::call(mcx, cat::SubscriptionRelRelationId, NoLock)?;
        (rel, None)
    } else {
        // LockSharedObject(SubscriptionRelationId, subid, 0, AccessShareLock);
        let lock = lmgr_seams::lock_shared_object::call(
            cat::SubscriptionRelationId,
            subid,
            0,
            AccessShareLock,
        )?;
        // rel = table_open(SubscriptionRelRelationId, RowExclusiveLock);
        let rel = table_seams::table_open::call(
            mcx,
            cat::SubscriptionRelRelationId,
            RowExclusiveLock,
        )?;
        (rel, Some(lock))
    };

    // tup = SearchSysCacheCopy2(SUBSCRIPTIONRELMAP, relid, subid);
    // if (!HeapTupleIsValid(tup)) elog(ERROR, "... does not exist");
    let tup = SearchSysCache2(
        mcx,
        SUBSCRIPTIONRELMAP,
        oid_cache_key(relid),
        oid_cache_key(subid),
    )?;
    let Some(tup) = tup else {
        rel.close(NoLock)?;
        if let Some(lock) = lock {
            lock.keep();
        }
        return Err(PgError::error(alloc_format(format_args!(
            "subscription table {relid} in subscription {subid} does not exist"
        ))));
    };

    // Update the tuple: replace srsubstate + srsublsn (NULL when invalid).
    let mut values: [Datum<'_>; cat::Natts_pg_subscription_rel] =
        core::array::from_fn(|_| Datum::null());
    let mut nulls = [false; cat::Natts_pg_subscription_rel];
    let mut replaces = [false; cat::Natts_pg_subscription_rel];
    let idx = |attno: i32| (attno - 1) as usize;

    replaces[idx(cat::Anum_pg_subscription_rel_srsubstate)] = true;
    values[idx(cat::Anum_pg_subscription_rel_srsubstate)] = Datum::from_char(state as i8);

    replaces[idx(cat::Anum_pg_subscription_rel_srsublsn)] = true;
    if sublsn != InvalidXLogRecPtr {
        values[idx(cat::Anum_pg_subscription_rel_srsublsn)] = lsn_datum(sublsn);
    } else {
        nulls[idx(cat::Anum_pg_subscription_rel_srsublsn)] = true;
    }

    // tup = heap_modify_tuple(tup, RelationGetDescr(rel), values, nulls, replaces);
    let desc = rel.rd_att_clone_in(mcx)?;
    let mut newtup = heap_modify_tuple(mcx, &tup, &desc, &values, &nulls, &replaces)
        .map_err(|e| PgError::error(alloc_format(format_args!("heap_modify_tuple failed: {e:?}"))))?;

    // CatalogTupleUpdate(rel, &tup->t_self, tup);
    let otid = newtup.tuple.t_self;
    CatalogTupleUpdate(mcx, &rel, otid, &mut newtup)?;

    if let Some(lock) = lock {
        lock.keep();
    }
    // table_close(rel, NoLock);
    rel.close(NoLock)?;
    Ok(())
}

/* ==========================================================================
 * GetSubscriptionRelState (pg_subscription.c:386)
 * ========================================================================== */

/// Get state of subscription table. Returns `(SUBREL_STATE_UNKNOWN,
/// InvalidXLogRecPtr)` when the table is not in the subscription.
fn GetSubscriptionRelState(
    mcx: Mcx<'_>,
    subid: Oid,
    relid: Oid,
) -> PgResult<(u8, XLogRecPtr)> {
    // rel = table_open(SubscriptionRelRelationId, AccessShareLock);
    // (race avoidance with AlterSubscription removing this relstate.)
    let rel =
        table_seams::table_open::call(mcx, cat::SubscriptionRelRelationId, AccessShareLock)?;

    // tup = SearchSysCache2(SUBSCRIPTIONRELMAP, relid, subid);
    let tup = SearchSysCache2(
        mcx,
        SUBSCRIPTIONRELMAP,
        oid_cache_key(relid),
        oid_cache_key(subid),
    )?;

    let Some(tup) = tup else {
        // table_close(rel, AccessShareLock);
        // *sublsn = InvalidXLogRecPtr; return SUBREL_STATE_UNKNOWN;
        rel.close(AccessShareLock)?;
        return Ok((cat::SUBREL_STATE_UNKNOWN as u8, InvalidXLogRecPtr));
    };

    // Deform once for both the GETSTRUCT srsubstate and the nullable srsublsn.
    let desc = rel.rd_att_clone_in(mcx)?;
    let cols = heap_deform_tuple(mcx, &tup.tuple, &desc, &tup.data)?;

    // substate = ((Form_pg_subscription_rel) GETSTRUCT(tup))->srsubstate;
    let substate = cols[(cat::Anum_pg_subscription_rel_srsubstate - 1) as usize]
        .0
        .as_char() as u8;

    // d = SysCacheGetAttr(... srsublsn, &isnull);
    // if (isnull) *sublsn = InvalidXLogRecPtr; else *sublsn = DatumGetLSN(d);
    let lsn_col = &cols[(cat::Anum_pg_subscription_rel_srsublsn - 1) as usize];
    let sublsn = if lsn_col.1 {
        InvalidXLogRecPtr
    } else {
        datum_get_lsn(&lsn_col.0)
    };

    // ReleaseSysCache(tup); table_close(rel, AccessShareLock);
    rel.close(AccessShareLock)?;

    Ok((substate, sublsn))
}

/* ==========================================================================
 * RemoveSubscriptionRel (pg_subscription.c:436)
 * ========================================================================== */

/// Drop subscription relation mapping. These can be for a particular
/// subscription, or for a particular relation, or both.
fn RemoveSubscriptionRel(mcx: Mcx<'_>, subid: Oid, relid: Oid) -> PgResult<()> {
    // rel = table_open(SubscriptionRelRelationId, RowExclusiveLock);
    let rel = table_seams::table_open::call(mcx, cat::SubscriptionRelRelationId, RowExclusiveLock)?;

    // Build only the OidIsValid keys (the C nkeys-counted skey[2]).
    let mut keys: PgVec<'_, ScanKeyData<'_>> = ::mcx::vec_with_capacity_in(mcx, 2)?;
    if OidIsValid(subid) {
        keys.push(oid_key(cat::Anum_pg_subscription_rel_srsubid as AttrNumber, subid)?);
    }
    if OidIsValid(relid) {
        keys.push(oid_key(cat::Anum_pg_subscription_rel_srrelid as AttrNumber, relid)?);
    }

    // scan = table_beginscan_catalog(rel, nkeys, skey); — a sequential catalog
    // scan (index_ok = false), the systable_beginscan analog.
    let mut scan = genam_seams::systable_beginscan::call(&rel, 0, false, None, &keys)?;

    // while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))
    let desc = rel.rd_att_clone_in(mcx)?;
    while let Some(tup) = genam_seams::systable_getnext::call(mcx, scan.desc_mut())? {
        // subrel = (Form_pg_subscription_rel) GETSTRUCT(tup);
        let cols = heap_deform_tuple(mcx, &tup.tuple, &desc, &tup.data)?;
        let srsubid = cols[(cat::Anum_pg_subscription_rel_srsubid - 1) as usize].0.as_oid();
        let srsubstate = cols[(cat::Anum_pg_subscription_rel_srsubstate - 1) as usize]
            .0
            .as_char();

        // We don't allow dropping the relation mapping when the table
        // synchronization is in progress unless the caller updates the
        // corresponding subscription as well.
        // if (!OidIsValid(subid) && subrel->srsubstate != SUBREL_STATE_READY)
        if !OidIsValid(subid) && srsubstate != cat::SUBREL_STATE_READY {
            // get_subscription_name(subrel->srsubid, false)
            let subname = lsyscache_seams::get_subscription_name::call(mcx, srsubid, false)?
                .map(|s| s.as_str().to_owned())
                .unwrap_or_default();
            // get_rel_name(relid) — NULL when the relation has been dropped;
            // the C `%s` of NULL renders "(null)".
            let relname = lsyscache_seams::get_rel_name::call(mcx, relid)?
                .map(|s| s.as_str().to_owned())
                .unwrap_or_else(|| "(null)".to_owned());
            let substate_char = srsubstate as u8 as char;

            // Clean up the open scan/relation before erroring.
            scan.end()?;
            rel.close(RowExclusiveLock)?;

            return Err(ereport_invalid_param(
                alloc_format(format_args!(
                    "could not drop relation mapping for subscription \"{subname}\""
                )),
                alloc_format(format_args!(
                    "Table synchronization for relation \"{relname}\" is in progress and is in state \"{substate_char}\"."
                )),
                alloc_format(format_args!(
                    "Use {} to enable subscription if not already enabled or use {} to drop the subscription.",
                    "ALTER SUBSCRIPTION ... ENABLE", "DROP SUBSCRIPTION ..."
                )),
            ));
        }

        // CatalogTupleDelete(rel, &tup->t_self);
        CatalogTupleDelete(mcx, &rel, tup.tuple.t_self)?;
    }
    // table_endscan(scan);
    scan.end()?;
    // table_close(rel, RowExclusiveLock);
    rel.close(RowExclusiveLock)?;

    Ok(())
}

/* ==========================================================================
 * HasSubscriptionRelations (pg_subscription.c:511)
 * ========================================================================== */

/// Does the subscription have any relations?
fn HasSubscriptionRelations(mcx: Mcx<'_>, subid: Oid) -> PgResult<bool> {
    // rel = table_open(SubscriptionRelRelationId, AccessShareLock);
    let rel = table_seams::table_open::call(mcx, cat::SubscriptionRelRelationId, AccessShareLock)?;

    // ScanKeyInit(&skey[0], Anum_pg_subscription_rel_srsubid, ..., F_OIDEQ, subid);
    let keys = [oid_key(cat::Anum_pg_subscription_rel_srsubid as AttrNumber, subid)?];

    // scan = systable_beginscan(rel, InvalidOid, false, NULL, 1, skey);
    let mut scan = genam_seams::systable_beginscan::call(&rel, 0, false, None, &keys)?;

    // has_subrels = HeapTupleIsValid(systable_getnext(scan));
    let has_subrels = genam_seams::systable_getnext::call(mcx, scan.desc_mut())?.is_some();

    // systable_endscan(scan); table_close(rel, AccessShareLock);
    scan.end()?;
    rel.close(AccessShareLock)?;

    Ok(has_subrels)
}

/* ==========================================================================
 * GetSubscriptionRelations (pg_subscription.c:546)
 * ========================================================================== */

/// Get the relations for the subscription. If `not_ready` is true, return only
/// the relations that are not in a ready state, otherwise return all.
fn GetSubscriptionRelations<'mcx>(
    mcx: Mcx<'mcx>,
    subid: Oid,
    not_ready: bool,
) -> PgResult<PgVec<'mcx, SubscriptionRelState>> {
    // List *res = NIL;
    let mut res: PgVec<'mcx, SubscriptionRelState> = ::mcx::vec_with_capacity_in(mcx, 0)?;

    // rel = table_open(SubscriptionRelRelationId, AccessShareLock);
    let rel = table_seams::table_open::call(mcx, cat::SubscriptionRelRelationId, AccessShareLock)?;

    // ScanKeyInit(... srsubid, F_OIDEQ, subid); if (not_ready) ScanKeyInit(...
    // srsubstate, F_CHARNE, SUBREL_STATE_READY);
    let mut keys: PgVec<'mcx, ScanKeyData<'mcx>> = ::mcx::vec_with_capacity_in(mcx, 2)?;
    keys.push(oid_key(cat::Anum_pg_subscription_rel_srsubid as AttrNumber, subid)?);
    if not_ready {
        keys.push(char_ne_key(
            cat::Anum_pg_subscription_rel_srsubstate as AttrNumber,
            cat::SUBREL_STATE_READY,
        )?);
    }

    // scan = systable_beginscan(rel, InvalidOid, false, NULL, nkeys, skey);
    let mut scan = genam_seams::systable_beginscan::call(&rel, 0, false, None, &keys)?;

    let desc = rel.rd_att_clone_in(mcx)?;
    // while (HeapTupleIsValid(tup = systable_getnext(scan)))
    while let Some(tup) = genam_seams::systable_getnext::call(mcx, scan.desc_mut())? {
        // subrel = (Form_pg_subscription_rel) GETSTRUCT(tup);
        let cols = heap_deform_tuple(mcx, &tup.tuple, &desc, &tup.data)?;

        // relstate->relid = subrel->srrelid; relstate->state = subrel->srsubstate;
        // d = SysCacheGetAttr(... srsublsn, &isnull);
        // if (isnull) relstate->lsn = InvalidXLogRecPtr; else lsn = DatumGetLSN(d);
        let lsn_col = &cols[(cat::Anum_pg_subscription_rel_srsublsn - 1) as usize];
        let relstate = SubscriptionRelState {
            relid: cols[(cat::Anum_pg_subscription_rel_srrelid - 1) as usize].0.as_oid(),
            state: cols[(cat::Anum_pg_subscription_rel_srsubstate - 1) as usize].0.as_char(),
            lsn: if lsn_col.1 {
                InvalidXLogRecPtr
            } else {
                datum_get_lsn(&lsn_col.0)
            },
        };

        // res = lappend(res, relstate);
        res.push(relstate);
    }

    // systable_endscan(scan); table_close(rel, AccessShareLock);
    scan.end()?;
    rel.close(AccessShareLock)?;

    Ok(res)
}

/* ==========================================================================
 * Small helpers.
 * ========================================================================== */

/// `psprintf`-style format into an owned String.
fn alloc_format(args: core::fmt::Arguments<'_>) -> alloc::string::String {
    use core::fmt::Write;
    let mut s = alloc::string::String::new();
    let _ = s.write_fmt(args);
    s
}

/// `ereport(ERROR, errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg, errdetail,
/// errhint)` — the RemoveSubscriptionRel "sync in progress" error.
fn ereport_invalid_param(
    msg: alloc::string::String,
    detail: alloc::string::String,
    hint: alloc::string::String,
) -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
        .errmsg(msg)
        .errdetail(detail)
        .errhint(hint)
        .into_error()
}

/* ==========================================================================
 * Seam installation.
 * ========================================================================== */

/// Run `f` against a fresh, self-contained scratch [`::mcx::MemoryContext`]
/// (the C `CurrentMemoryContext` for these short catalog operations), dropped
/// when the call returns. Used to adapt the no-`mcx` seam signatures (the
/// caller never observes a heap allocation: every such seam returns `Copy` /
/// unit values, exactly as the C functions return scalars / void). `f`'s
/// result borrows nothing from the context.
fn with_scratch<R>(f: impl FnOnce(Mcx<'_>) -> PgResult<R>) -> PgResult<R> {
    let ctx = ::mcx::MemoryContext::new("pg_subscription scratch");
    f(ctx.mcx())
}

/// Install every inward seam this unit owns. Wired into `seams-init::init_all`.
pub fn init_seams() {
    use pg_subscription_seams as s;

    s::get_subscription::set(GetSubscription);
    s::get_subscription_list::set(get_subscription_list);
    s::count_db_subscriptions::set(|dbid| with_scratch(|mcx| CountDBSubscriptions(mcx, dbid)));
    s::disable_subscription::set(|subid| with_scratch(|mcx| DisableSubscription(mcx, subid)));
    s::add_subscription_rel_state::set(|subid, relid, state, sublsn, retain_lock| {
        with_scratch(|mcx| AddSubscriptionRelState(mcx, subid, relid, state, sublsn, retain_lock))
    });
    s::update_subscription_rel_state::set(|subid, relid, state, sublsn, already_locked| {
        with_scratch(|mcx| {
            UpdateSubscriptionRelState(mcx, subid, relid, state, sublsn, already_locked)
        })
    });
    s::get_subscription_rel_state::set(|subid, relid| {
        with_scratch(|mcx| GetSubscriptionRelState(mcx, subid, relid))
    });
    s::remove_subscription_rel::set(|subid, relid| {
        with_scratch(|mcx| RemoveSubscriptionRel(mcx, subid, relid))
    });
    s::has_subscription_relations::set(|subid| {
        with_scratch(|mcx| HasSubscriptionRelations(mcx, subid))
    });
    s::get_subscription_relations::set(GetSubscriptionRelations);
}
