// pg_subscription.c: subscription catalog API + pg_subscription_rel state.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use datum::Datum;
use mcx::{Mcx, PgString, PgVec};
use types_core::fmgr::{F_CHARNE, F_OIDEQ};
use types_core::primitive::{RegProcedure, XLogRecPtr};
use types_core::{AttrNumber, InvalidOid, InvalidXLogRecPtr, Oid, TEXTOID};
use types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE};
use types_rel::{AccessShareLock, NoLock, RowExclusiveLock};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_tuple::{HeapTupleData, NameData, TupleDescData};

use cache_syscache::cacheinfo::{SUBSCRIPTIONNAME, SUBSCRIPTIONOID, SUBSCRIPTIONRELMAP};
use cache_syscache::{
    GetSysCacheOid, ReleaseSysCache, SearchSysCache1, SearchSysCache2, SearchSysCacheCopy,
    SearchSysCacheExists, SysCacheGetAttr, SysCacheGetAttrNotNull, SysCacheKey,
};

pub use catalog::{SubscriptionNameIndexId, SubscriptionObjectIndexId, SubscriptionRelationId};

pub const SubscriptionRelRelationId: Oid = 6102;
pub const SubscriptionRelSrrelidSrsubidIndexId: Oid = 6117;

pub const Anum_pg_subscription_oid: i32 = 1;
pub const Anum_pg_subscription_subdbid: i32 = 2;
pub const Anum_pg_subscription_subskiplsn: i32 = 3;
pub const Anum_pg_subscription_subname: i32 = 4;
pub const Anum_pg_subscription_subowner: i32 = 5;
pub const Anum_pg_subscription_subenabled: i32 = 6;
pub const Anum_pg_subscription_subbinary: i32 = 7;
pub const Anum_pg_subscription_substream: i32 = 8;
pub const Anum_pg_subscription_subtwophasestate: i32 = 9;
pub const Anum_pg_subscription_subdisableonerr: i32 = 10;
pub const Anum_pg_subscription_subpasswordrequired: i32 = 11;
pub const Anum_pg_subscription_subrunasowner: i32 = 12;
pub const Anum_pg_subscription_subfailover: i32 = 13;
pub const Anum_pg_subscription_subconninfo: i32 = 14;
pub const Anum_pg_subscription_subslotname: i32 = 15;
pub const Anum_pg_subscription_subsynccommit: i32 = 16;
pub const Anum_pg_subscription_subpublications: i32 = 17;
pub const Anum_pg_subscription_suborigin: i32 = 18;
pub const Natts_pg_subscription: usize = 18;

pub const Anum_pg_subscription_rel_srsubid: i32 = 1;
pub const Anum_pg_subscription_rel_srrelid: i32 = 2;
pub const Anum_pg_subscription_rel_srsubstate: i32 = 3;
pub const Anum_pg_subscription_rel_srsublsn: i32 = 4;
pub const Natts_pg_subscription_rel: usize = 4;

pub const LOGICALREP_TWOPHASE_STATE_DISABLED: u8 = b'd';
pub const LOGICALREP_TWOPHASE_STATE_PENDING: u8 = b'p';
pub const LOGICALREP_TWOPHASE_STATE_ENABLED: u8 = b'e';
pub const LOGICALREP_ORIGIN_NONE: &str = "none";
pub const LOGICALREP_ORIGIN_ANY: &str = "any";
pub const LOGICALREP_STREAM_OFF: u8 = b'f';
pub const LOGICALREP_STREAM_ON: u8 = b't';
pub const LOGICALREP_STREAM_PARALLEL: u8 = b'p';

pub const SUBREL_STATE_INIT: u8 = b'i';
pub const SUBREL_STATE_DATASYNC: u8 = b'd';
pub const SUBREL_STATE_FINISHEDCOPY: u8 = b'f';
pub const SUBREL_STATE_SYNCDONE: u8 = b's';
pub const SUBREL_STATE_READY: u8 = b'r';
pub const SUBREL_STATE_UNKNOWN: u8 = 0;
pub const SUBREL_STATE_SYNCWAIT: u8 = b'w';
pub const SUBREL_STATE_CATCHUP: u8 = b'c';

pub struct Subscription<'mcx> {
    pub oid: Oid,
    pub dbid: Oid,
    pub skiplsn: XLogRecPtr,
    pub name: PgString<'mcx>,
    pub owner: Oid,
    pub ownersuperuser: bool,
    pub enabled: bool,
    pub binary: bool,
    pub stream: u8,
    pub twophasestate: u8,
    pub disableonerr: bool,
    pub passwordrequired: bool,
    pub runasowner: bool,
    pub failover: bool,
    pub conninfo: PgString<'mcx>,
    pub slotname: Option<PgString<'mcx>>,
    pub synccommit: PgString<'mcx>,
    pub publications: PgVec<'mcx, &'mcx str>,
    pub origin: PgString<'mcx>,
}

pub struct SubscriptionRelState {
    pub relid: Oid,
    pub lsn: XLogRecPtr,
    pub state: u8,
}

fn eq_key(attno: AttrNumber, func: RegProcedure, arg: Datum) -> ScanKeyData {
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = types_core::C_COLLATION_OID;
    key.sk_func = fmgr_seams::fmgr_info::call(func)
        .unwrap_or_else(|e| panic!("fmgr_info({func}) failed: {e:?}"));
    key.sk_argument = arg;
    key
}

fn getattr(td: &TupleDescData<'_>, tup: &HeapTupleData<'_>, attno: i32) -> (Datum, bool) {
    let mut isnull = false;
    // SAFETY: tup is a catalog row read under its relation's descriptor.
    let d = unsafe { types_tuple::heap_getattr(tup, attno, td, &mut isnull) };
    (d, isnull)
}

fn name_from_datum(d: Datum) -> NameData {
    // SAFETY: a name attr datum addresses NAMEDATALEN in-tuple bytes.
    unsafe { core::ptr::read_unaligned(d.as_usize() as *const NameData) }
}

fn detoast_datum<'mcx>(mcx: Mcx<'mcx>, d: Datum) -> PgResult<PgVec<'mcx, u8>> {
    let p = d.as_usize() as *const u8;
    // SAFETY: a live varlena readable through its full VARSIZE_ANY.
    let raw = unsafe { core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) };
    detoast::detoast_attr(mcx, raw)
}

fn varlena_payload(image: &[u8]) -> &[u8] {
    if image[0] & 0x01 == 0x01 {
        &image[1..(image[0] >> 1) as usize]
    } else {
        &image[4..(u32::from_ne_bytes(image[..4].try_into().unwrap()) >> 2) as usize]
    }
}

fn text_pgstring<'mcx>(mcx: Mcx<'mcx>, d: Datum) -> PgResult<PgString<'mcx>> {
    let img = detoast_datum(mcx, d)?;
    PgString::from_str_in(
        core::str::from_utf8(varlena_payload(&img)).expect("catalog text attr is UTF-8"),
        mcx,
    )
}

pub fn GetPublicationsStr<'mcx>(
    mcx: Mcx<'mcx>,
    publications: &[&str],
    dest: &mut PgString<'mcx>,
    quote_literal: bool,
) -> PgResult<()> {
    debug_assert!(!publications.is_empty());
    let mut first = true;
    for pubname in publications {
        if first {
            first = false;
        } else {
            dest.try_push_str(", ")?;
        }
        if quote_literal {
            let quoted = adt_quote::quote_literal(mcx, pubname.as_bytes())?.into_image();
            dest.try_push_str(
                core::str::from_utf8(varlena_payload(&quoted)).expect("quoted name is UTF-8"),
            )?;
        } else {
            dest.try_push('"')?;
            dest.try_push_str(pubname)?;
            dest.try_push('"')?;
        }
    }
    Ok(())
}

pub fn GetSubscription<'mcx>(
    mcx: Mcx<'mcx>,
    subid: Oid,
    missing_ok: bool,
) -> PgResult<Option<Subscription<'mcx>>> {
    let Some(tup) = SearchSysCache1(SUBSCRIPTIONOID, SysCacheKey::Value(Datum::from_oid(subid)))?
    else {
        if missing_ok {
            return Ok(None);
        }
        return Err(Box::new(PgError::error(format!(
            "cache lookup failed for subscription {subid}"
        ))));
    };
    let attr = |anum: i32| -> PgResult<Datum> {
        Ok(SysCacheGetAttr(SUBSCRIPTIONOID, &tup, anum)?.0)
    };

    let name_data = name_from_datum(attr(Anum_pg_subscription_subname)?);
    let name = PgString::from_str_in(
        core::str::from_utf8(name_data.name_str()).expect("subname is UTF-8"),
        mcx,
    )?;
    let owner = attr(Anum_pg_subscription_subowner)?.as_oid();

    let conninfo = text_pgstring(
        mcx,
        SysCacheGetAttrNotNull(SUBSCRIPTIONOID, &tup, Anum_pg_subscription_subconninfo)?,
    )?;

    let (slot_d, slot_isnull) =
        SysCacheGetAttr(SUBSCRIPTIONOID, &tup, Anum_pg_subscription_subslotname)?;
    let slotname = if slot_isnull {
        None
    } else {
        let n = name_from_datum(slot_d);
        Some(PgString::from_str_in(
            core::str::from_utf8(n.name_str()).expect("subslotname is UTF-8"),
            mcx,
        )?)
    };

    let synccommit = text_pgstring(
        mcx,
        SysCacheGetAttrNotNull(SUBSCRIPTIONOID, &tup, Anum_pg_subscription_subsynccommit)?,
    )?;

    let pubs_d =
        SysCacheGetAttrNotNull(SUBSCRIPTIONOID, &tup, Anum_pg_subscription_subpublications)?;
    let pubs_img = detoast_datum(mcx, pubs_d)?;
    let publications = textarray_to_stringlist(mcx, &pubs_img)?;

    let origin = text_pgstring(
        mcx,
        SysCacheGetAttrNotNull(SUBSCRIPTIONOID, &tup, Anum_pg_subscription_suborigin)?,
    )?;

    let sub = Subscription {
        oid: subid,
        dbid: attr(Anum_pg_subscription_subdbid)?.as_oid(),
        skiplsn: attr(Anum_pg_subscription_subskiplsn)?.as_u64(),
        name,
        owner,
        ownersuperuser: superuser::superuser_arg(owner)?,
        enabled: attr(Anum_pg_subscription_subenabled)?.as_bool(),
        binary: attr(Anum_pg_subscription_subbinary)?.as_bool(),
        stream: attr(Anum_pg_subscription_substream)?.as_u8(),
        twophasestate: attr(Anum_pg_subscription_subtwophasestate)?.as_u8(),
        disableonerr: attr(Anum_pg_subscription_subdisableonerr)?.as_bool(),
        passwordrequired: attr(Anum_pg_subscription_subpasswordrequired)?.as_bool(),
        runasowner: attr(Anum_pg_subscription_subrunasowner)?.as_bool(),
        failover: attr(Anum_pg_subscription_subfailover)?.as_bool(),
        conninfo,
        slotname,
        synccommit,
        publications,
        origin,
    };
    ReleaseSysCache(tup);
    Ok(Some(sub))
}

pub fn CountDBSubscriptions<'mcx>(mcx: Mcx<'mcx>, dbid: Oid) -> PgResult<i32> {
    let mut nsubs = 0;
    let rel = table::table_open(mcx, SubscriptionRelationId, RowExclusiveLock)?;
    let keys = [eq_key(
        Anum_pg_subscription_subdbid as AttrNumber,
        F_OIDEQ,
        Datum::from_oid(dbid),
    )];
    let mut scan = genam::systable_beginscan(mcx, &rel, InvalidOid, false, None, &keys)?;
    while genam::systable_getnext(mcx, &mut scan)?.is_some() {
        nsubs += 1;
    }
    genam::systable_endscan(mcx, scan)?;
    rel.close(NoLock)?;
    Ok(nsubs)
}

pub fn DisableSubscription<'mcx>(mcx: Mcx<'mcx>, subid: Oid) -> PgResult<()> {
    let rel = table::table_open(mcx, SubscriptionRelationId, RowExclusiveLock)?;
    let Some(tup) = SearchSysCacheCopy(
        mcx,
        SUBSCRIPTIONOID,
        SysCacheKey::Value(Datum::from_oid(subid)),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )?
    else {
        return Err(Box::new(PgError::error(format!(
            "cache lookup failed for subscription {subid}"
        ))));
    };

    lmgr::LockSharedObject(SubscriptionRelationId, subid, 0, AccessShareLock)?;

    let mut values = [Datum::null(); Natts_pg_subscription];
    let nulls = [false; Natts_pg_subscription];
    let mut replaces = [false; Natts_pg_subscription];
    values[(Anum_pg_subscription_subenabled - 1) as usize] = Datum::from_bool(false);
    replaces[(Anum_pg_subscription_subenabled - 1) as usize] = true;

    let mut new_tup = heaptuple::heap_modify_tuple(
        mcx,
        tup.as_tuple(),
        rel.descr(),
        &values,
        &nulls,
        &replaces,
    )?;
    let otid = tup.as_tuple().t_self;
    catalog_indexing::CatalogTupleUpdate(mcx, &rel, &otid, &mut new_tup)?;

    rel.close(NoLock)
}

pub fn textarray_to_stringlist<'mcx>(
    mcx: Mcx<'mcx>,
    textarray: &[u8],
) -> PgResult<PgVec<'mcx, &'mcx str>> {
    let (elems, _nulls) = arrayfuncs::deconstruct_array_builtin(mcx, textarray, TEXTOID, false)?;
    let mut res: PgVec<'mcx, &'mcx str> = mcx::vec_with_capacity_in(mcx, elems.len())?;
    for &e in elems.iter() {
        let img = detoast_datum(mcx, e)?;
        let bytes = mcx::slice_borrow_in(mcx, varlena_payload(&img))?;
        res.push(core::str::from_utf8(bytes).expect("publication name is UTF-8"));
    }
    Ok(res)
}

pub fn AddSubscriptionRelState<'mcx>(
    mcx: Mcx<'mcx>,
    subid: Oid,
    relid: Oid,
    state: u8,
    sublsn: XLogRecPtr,
    retain_lock: bool,
) -> PgResult<()> {
    lmgr::LockSharedObject(SubscriptionRelationId, subid, 0, AccessShareLock)?;

    let rel = table::table_open(mcx, SubscriptionRelRelationId, RowExclusiveLock)?;

    if SearchSysCacheExists(
        SUBSCRIPTIONRELMAP,
        SysCacheKey::Value(Datum::from_oid(relid)),
        SysCacheKey::Value(Datum::from_oid(subid)),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )? {
        return Err(Box::new(PgError::error(format!(
            "subscription table {relid} in subscription {subid} already exists"
        ))));
    }

    let mut values = [Datum::null(); Natts_pg_subscription_rel];
    let mut nulls = [false; Natts_pg_subscription_rel];
    values[(Anum_pg_subscription_rel_srsubid - 1) as usize] = Datum::from_oid(subid);
    values[(Anum_pg_subscription_rel_srrelid - 1) as usize] = Datum::from_oid(relid);
    values[(Anum_pg_subscription_rel_srsubstate - 1) as usize] = Datum::from_char(state as i8);
    if sublsn != InvalidXLogRecPtr {
        values[(Anum_pg_subscription_rel_srsublsn - 1) as usize] = Datum::from_u64(sublsn);
    } else {
        nulls[(Anum_pg_subscription_rel_srsublsn - 1) as usize] = true;
    }

    let mut tup = heaptuple::heap_form_tuple(mcx, rel.descr(), &values, &nulls)?;
    catalog_indexing::CatalogTupleInsert(mcx, &rel, &mut tup)?;

    if retain_lock {
        rel.close(NoLock)
    } else {
        rel.close(RowExclusiveLock)?;
        lmgr::UnlockSharedObject(SubscriptionRelationId, subid, 0, AccessShareLock)
    }
}

pub fn UpdateSubscriptionRelState<'mcx>(
    mcx: Mcx<'mcx>,
    subid: Oid,
    relid: Oid,
    state: u8,
    sublsn: XLogRecPtr,
    already_locked: bool,
) -> PgResult<()> {
    let rel = if already_locked {
        table::table_open(mcx, SubscriptionRelRelationId, NoLock)?
    } else {
        lmgr::LockSharedObject(SubscriptionRelationId, subid, 0, AccessShareLock)?;
        table::table_open(mcx, SubscriptionRelRelationId, RowExclusiveLock)?
    };

    let Some(tup) = SearchSysCacheCopy(
        mcx,
        SUBSCRIPTIONRELMAP,
        SysCacheKey::Value(Datum::from_oid(relid)),
        SysCacheKey::Value(Datum::from_oid(subid)),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )?
    else {
        return Err(Box::new(PgError::error(format!(
            "subscription table {relid} in subscription {subid} does not exist"
        ))));
    };

    let mut values = [Datum::null(); Natts_pg_subscription_rel];
    let mut nulls = [false; Natts_pg_subscription_rel];
    let mut replaces = [false; Natts_pg_subscription_rel];

    replaces[(Anum_pg_subscription_rel_srsubstate - 1) as usize] = true;
    values[(Anum_pg_subscription_rel_srsubstate - 1) as usize] = Datum::from_char(state as i8);

    replaces[(Anum_pg_subscription_rel_srsublsn - 1) as usize] = true;
    if sublsn != InvalidXLogRecPtr {
        values[(Anum_pg_subscription_rel_srsublsn - 1) as usize] = Datum::from_u64(sublsn);
    } else {
        nulls[(Anum_pg_subscription_rel_srsublsn - 1) as usize] = true;
    }

    let mut new_tup = heaptuple::heap_modify_tuple(
        mcx,
        tup.as_tuple(),
        rel.descr(),
        &values,
        &nulls,
        &replaces,
    )?;
    let otid = tup.as_tuple().t_self;
    catalog_indexing::CatalogTupleUpdate(mcx, &rel, &otid, &mut new_tup)?;

    rel.close(NoLock)
}

// UpdateTwoPhaseState (subscriptioncmds.c): transition
// pg_subscription.subtwophasestate. C hosts it in subscriptioncmds.c and
// exports it for the apply worker; it lives beside the other pg_subscription
// catalog updaters here.
pub fn UpdateTwoPhaseState<'mcx>(mcx: Mcx<'mcx>, suboid: Oid, new_state: u8) -> PgResult<()> {
    debug_assert!(matches!(
        new_state,
        LOGICALREP_TWOPHASE_STATE_DISABLED
            | LOGICALREP_TWOPHASE_STATE_PENDING
            | LOGICALREP_TWOPHASE_STATE_ENABLED
    ));
    let rel = table::table_open(mcx, SubscriptionRelationId, RowExclusiveLock)?;
    let Some(tup) = SearchSysCacheCopy(
        mcx,
        SUBSCRIPTIONOID,
        SysCacheKey::Value(Datum::from_oid(suboid)),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )?
    else {
        return Err(Box::new(PgError::error(format!(
            "cache lookup failed for subscription oid {suboid}"
        ))));
    };

    let mut values = [Datum::null(); Natts_pg_subscription];
    let mut nulls = [false; Natts_pg_subscription];
    let mut replaces = [false; Natts_pg_subscription];
    values[(Anum_pg_subscription_subtwophasestate - 1) as usize] =
        Datum::from_char(new_state as i8);
    replaces[(Anum_pg_subscription_subtwophasestate - 1) as usize] = true;

    let mut new_tup = heaptuple::heap_modify_tuple(
        mcx,
        tup.as_tuple(),
        rel.descr(),
        &values,
        &nulls,
        &replaces,
    )?;
    let otid = tup.as_tuple().t_self;
    catalog_indexing::CatalogTupleUpdate(mcx, &rel, &otid, &mut new_tup)?;

    rel.close(RowExclusiveLock)
}

pub fn GetSubscriptionRelState<'mcx>(
    mcx: Mcx<'mcx>,
    subid: Oid,
    relid: Oid,
) -> PgResult<(u8, XLogRecPtr)> {
    let rel = table::table_open(mcx, SubscriptionRelRelationId, AccessShareLock)?;

    let Some(tup) = SearchSysCache2(
        SUBSCRIPTIONRELMAP,
        SysCacheKey::Value(Datum::from_oid(relid)),
        SysCacheKey::Value(Datum::from_oid(subid)),
    )?
    else {
        rel.close(AccessShareLock)?;
        return Ok((SUBREL_STATE_UNKNOWN, InvalidXLogRecPtr));
    };

    let substate =
        SysCacheGetAttr(SUBSCRIPTIONRELMAP, &tup, Anum_pg_subscription_rel_srsubstate)?.0.as_u8();
    let (d, isnull) =
        SysCacheGetAttr(SUBSCRIPTIONRELMAP, &tup, Anum_pg_subscription_rel_srsublsn)?;
    let sublsn = if isnull { InvalidXLogRecPtr } else { d.as_u64() };

    ReleaseSysCache(tup);
    rel.close(AccessShareLock)?;
    Ok((substate, sublsn))
}

pub fn RemoveSubscriptionRel<'mcx>(mcx: Mcx<'mcx>, subid: Oid, relid: Oid) -> PgResult<()> {
    let rel = table::table_open(mcx, SubscriptionRelRelationId, RowExclusiveLock)?;

    let mut keys: PgVec<'mcx, ScanKeyData> = PgVec::new_in(mcx);
    if subid != InvalidOid {
        keys.push(eq_key(
            Anum_pg_subscription_rel_srsubid as AttrNumber,
            F_OIDEQ,
            Datum::from_oid(subid),
        ));
    }
    if relid != InvalidOid {
        keys.push(eq_key(
            Anum_pg_subscription_rel_srrelid as AttrNumber,
            F_OIDEQ,
            Datum::from_oid(relid),
        ));
    }

    let mut scan = genam::systable_beginscan(mcx, &rel, InvalidOid, false, None, &keys)?;
    let td = rel.descr();
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let srsubid = getattr(td, tup, Anum_pg_subscription_rel_srsubid).0.as_oid();
        let srsubstate = getattr(td, tup, Anum_pg_subscription_rel_srsubstate).0.as_u8();
        if subid == InvalidOid && srsubstate != SUBREL_STATE_READY {
            let subname = lsyscache::get_subscription_name(mcx, srsubid, false)?
                .expect("missing_ok=false yields an error");
            let relname = lsyscache::get_rel_name(mcx, relid)?
                .map(|n| n.as_str().to_string())
                .unwrap_or_default();
            return Err(Box::new(
                PgError::error(format!(
                    "could not drop relation mapping for subscription \"{}\"",
                    subname.as_str()
                ))
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
                .with_detail(format!(
                    "Table synchronization for relation \"{relname}\" is in progress and is in state \"{}\".",
                    srsubstate as char
                ))
                .with_hint(
                    "Use ALTER SUBSCRIPTION ... ENABLE to enable subscription if not already \
                     enabled or use DROP SUBSCRIPTION ... to drop the subscription.",
                ),
            ));
        }
        let tid = tup.t_self;
        catalog_indexing::CatalogTupleDelete(&rel, &tid)?;
    }
    genam::systable_endscan(mcx, scan)?;

    rel.close(RowExclusiveLock)
}

pub fn HasSubscriptionRelations<'mcx>(mcx: Mcx<'mcx>, subid: Oid) -> PgResult<bool> {
    let rel = table::table_open(mcx, SubscriptionRelRelationId, AccessShareLock)?;
    let keys = [eq_key(
        Anum_pg_subscription_rel_srsubid as AttrNumber,
        F_OIDEQ,
        Datum::from_oid(subid),
    )];
    let mut scan = genam::systable_beginscan(mcx, &rel, InvalidOid, false, None, &keys)?;
    let has_subrels = genam::systable_getnext(mcx, &mut scan)?.is_some();
    genam::systable_endscan(mcx, scan)?;
    rel.close(AccessShareLock)?;
    Ok(has_subrels)
}

pub fn GetSubscriptionRelations<'mcx>(
    mcx: Mcx<'mcx>,
    subid: Oid,
    not_ready: bool,
) -> PgResult<PgVec<'mcx, SubscriptionRelState>> {
    let mut res: PgVec<'mcx, SubscriptionRelState> = PgVec::new_in(mcx);
    let rel = table::table_open(mcx, SubscriptionRelRelationId, AccessShareLock)?;

    let mut keys: PgVec<'mcx, ScanKeyData> = PgVec::new_in(mcx);
    keys.push(eq_key(
        Anum_pg_subscription_rel_srsubid as AttrNumber,
        F_OIDEQ,
        Datum::from_oid(subid),
    ));
    if not_ready {
        keys.push(eq_key(
            Anum_pg_subscription_rel_srsubstate as AttrNumber,
            F_CHARNE,
            Datum::from_char(SUBREL_STATE_READY as i8),
        ));
    }

    let mut scan = genam::systable_beginscan(mcx, &rel, InvalidOid, false, None, &keys)?;
    let td = rel.descr();
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let relid = getattr(td, tup, Anum_pg_subscription_rel_srrelid).0.as_oid();
        let state = getattr(td, tup, Anum_pg_subscription_rel_srsubstate).0.as_u8();
        let (d, isnull) = getattr(td, tup, Anum_pg_subscription_rel_srsublsn);
        let lsn = if isnull { InvalidXLogRecPtr } else { d.as_u64() };
        res.push(SubscriptionRelState { relid, lsn, state });
    }
    genam::systable_endscan(mcx, scan)?;
    rel.close(AccessShareLock)?;
    Ok(res)
}

// The launcher's get_subscription_list (launcher.c:111) body: the caller
// (launcher crate) wraps this in StartTransactionCommand/Commit. Only the
// worker-start fields are filled. Rendering divergence: a plain full
// systable scan of pg_subscription reading attrs off each tuple (C reads the
// fixed-layout Form struct directly).
pub struct SubscriptionListEntry {
    pub oid: Oid,
    pub dbid: Oid,
    pub owner: Oid,
    pub enabled: bool,
    pub name: String,
}

pub fn GetSubscriptionList<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Vec<SubscriptionListEntry>> {
    let mut res = Vec::new();
    let rel = table::table_open(mcx, SubscriptionRelationId, AccessShareLock)?;
    let mut scan = genam::systable_beginscan(mcx, &rel, InvalidOid, false, None, &[])?;
    let td = rel.descr();
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let name_d = getattr(td, tup, Anum_pg_subscription_subname).0;
        let name = name_from_datum(name_d);
        res.push(SubscriptionListEntry {
            oid: getattr(td, tup, Anum_pg_subscription_oid).0.as_oid(),
            dbid: getattr(td, tup, Anum_pg_subscription_subdbid).0.as_oid(),
            owner: getattr(td, tup, Anum_pg_subscription_subowner).0.as_oid(),
            enabled: getattr(td, tup, Anum_pg_subscription_subenabled).0.as_bool(),
            name: String::from_utf8_lossy(name.name_str()).into_owned(),
        });
    }
    genam::systable_endscan(mcx, scan)?;
    rel.close(AccessShareLock)?;
    Ok(res)
}

fn seam_lookup_pg_subscription_oid(dbid: Oid, subname: &str) -> PgResult<Oid> {
    GetSysCacheOid(
        SUBSCRIPTIONNAME,
        Anum_pg_subscription_oid,
        SysCacheKey::Value(Datum::from_oid(dbid)),
        SysCacheKey::Str(subname),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )
}

fn seam_pg_subscription_subname(subid: Oid) -> PgResult<Option<NameData>> {
    let Some(tup) = SearchSysCache1(SUBSCRIPTIONOID, SysCacheKey::Value(Datum::from_oid(subid)))?
    else {
        return Ok(None);
    };
    let (d, _) = SysCacheGetAttr(SUBSCRIPTIONOID, &tup, Anum_pg_subscription_subname)?;
    let name = name_from_datum(d);
    ReleaseSysCache(tup);
    Ok(Some(name))
}

pub fn init_seams() {
    syscache_seams::lookup_pg_subscription_oid::set(seam_lookup_pg_subscription_oid);
    syscache_seams::pg_subscription_subname::set(seam_pg_subscription_subname);
}
