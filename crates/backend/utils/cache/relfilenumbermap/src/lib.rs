//! `utils/cache/relfilenumbermap.c` — relfilenumber to oid mapping cache.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::cell::RefCell;

use ::heaptuple::heap_deform_tuple;
use genam_seams as genam_seams;
use table::{table_close, table_open};
use inval_seams as inval_seams;
use relmapper_seams as relmapper_seams;
use fmgr_seams as fmgr_seams;
use mcx::{McxOwned, Mcx, MemoryContext, PgHashMap};
use ::types_core::fmgr::F_OIDEQ;
use types_core::{InvalidOid, Oid, RelFileNumber};
use types_error::{PgError, PgResult};
use ::types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use ::types_storage::lock::AccessShareLock;
// The canonical value enum (`Datum<'mcx>`) is the migration target for the
// deformed-column reads below. The bare-word newtype (`::datum::Datum`)
// survives only at the audited ABI/storage edges where a plain machine word is
// stored: the relcache-callback function-pointer `arg` (its type is fixed by
// `RelcacheCallbackFunction`) and `ScanKeyData.sk_argument` (a bare word in the
// `types-scan` vocabulary). Those uses are spelled `ScalarWord` here.
use ::datum::Datum as ScalarWord;
use types_tuple::heaptuple::Datum;

/// `RelationRelationId` (`catalog/pg_class.h`) — pg_class.
const RelationRelationId: Oid = 1259;
/// `ClassTblspcRelfilenodeIndexId` (`catalog/pg_class.h`).
const ClassTblspcRelfilenodeIndexId: Oid = 3455;
/// `GLOBALTABLESPACE_OID` (`catalog/pg_tablespace.h`).
const GLOBALTABLESPACE_OID: Oid = 1664;
/// `Anum_pg_class_oid` / `_relfilenode` / `_reltablespace` /
/// `_relpersistence` (`catalog/pg_class.h`).
const Anum_pg_class_oid: i32 = 1;
const Anum_pg_class_relfilenode: i32 = 8;
const Anum_pg_class_reltablespace: i32 = 9;
const Anum_pg_class_relpersistence: i32 = 17;
/// `RELPERSISTENCE_TEMP` (`catalog/pg_class.h`).
const RELPERSISTENCE_TEMP: i8 = b't' as i8;

/// `RelfilenumberMapKey`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct RelfilenumberMapKey {
    reltablespace: Oid,
    relfilenumber: RelFileNumber,
}

struct RelfilenumberMap<'mcx> {
    mcx: Mcx<'mcx>,
    /// `static ScanKeyData relfilenumber_skey[2]` — built first time through
    /// in `InitializeRelfilenumberMap`; `sk_func` is resolved eagerly the way
    /// C does (`fmgr_info_cxt(F_OIDEQ, &sk_func, ..)`) through the fmgr seam.
    relfilenumber_skey: [ScanKeyData<'mcx>; 2],
    /// `static HTAB *RelfilenumberMapHash` — the entry value is `relid`
    /// (`pg_class.oid`; `InvalidOid` is a negative cache entry).
    hash: PgHashMap<'mcx, RelfilenumberMapKey, Oid>,
}

::mcx::bind!(RelfilenumberMapTy => RelfilenumberMap<'mcx>);

thread_local! {
    static RELFILENUMBER_MAP: RefCell<Option<McxOwned<RelfilenumberMapTy>>> =
        const { RefCell::new(None) };
}

/// `RelfilenumberMapInvalidateCallback` — flush mapping entries when pg_class
/// is updated in a relevant fashion.
fn RelfilenumberMapInvalidateCallback(_arg: ScalarWord, relid: Oid) {
    RELFILENUMBER_MAP.with(|cell| {
        let mut slot = cell.borrow_mut();
        // callback only gets registered after creating the hash
        let owned = slot.as_mut().expect("RelfilenumberMapHash != NULL");
        owned.with_mut(|map| {
            // If relid is InvalidOid, signaling a complete reset, we must
            // remove all entries, otherwise just remove the specific
            // relation's entry. Always remove negative cache entries.
            map.hash.retain(|_, entry_relid| {
                !(relid == InvalidOid          // complete reset
                    || *entry_relid == InvalidOid  // negative cache entry
                    || *entry_relid == relid)      // individual flushed relation
            });
        });
    });
}

/// `InitializeRelfilenumberMap` — initialize cache, either on first use or
/// after a reset.
fn InitializeRelfilenumberMap() -> PgResult<()> {
    // build skey — C fills the fields by hand (fmgr_info_cxt(F_OIDEQ, ..) +
    // BTEqualStrategyNumber, subtype/collation InvalidOid) rather than
    // calling ScanKeyInit; mirror that. The eager fmgr resolution crosses
    // the fmgr seam (panics until fmgr lands, exactly where C does the
    // lookup); the trimmed FmgrInfo records the resolved procedure OID.
    let mut skey = [ScanKeyData::empty(), ScanKeyData::empty()];
    for entry in &mut skey {
        fmgr_seams::fmgr_info_check::call(F_OIDEQ)?;
        entry.sk_func = ::types_core::fmgr::FmgrInfo { fn_oid: F_OIDEQ, ..Default::default() };
        entry.sk_strategy = BTEqualStrategyNumber;
        entry.sk_subtype = InvalidOid;
        entry.sk_collation = InvalidOid;
    }
    skey[0].sk_attno = Anum_pg_class_reltablespace as i16;
    skey[1].sk_attno = Anum_pg_class_relfilenode as i16;

    // Only create the RelfilenumberMapHash now, so we don't end up partially
    // initialized if building the scan keys errors out.
    let owned = McxOwned::<RelfilenumberMapTy>::try_new(
        MemoryContext::new("RelfilenumberMap cache"),
        |mcx| {
            Ok(RelfilenumberMap {
                mcx,
                relfilenumber_skey: skey,
                hash: PgHashMap::new_in(mcx),
            })
        },
    )?;
    RELFILENUMBER_MAP.with(|cell| {
        *cell.borrow_mut() = Some(owned);
    });

    // Watch for invalidation events.
    inval_seams::cache_register_relcache_callback::call(
        RelfilenumberMapInvalidateCallback,
        ScalarWord::null(),
    )
}

/// `RelidByRelfilenumber` — map a relation's (tablespace, relfilenumber) to
/// the relation's OID and cache the result.
///
/// A temporary relation may share its relfilenumber with a permanent relation
/// or temporary relations created in other backends. Being able to uniquely
/// identify a temporary relation would require a backend's proc number, which
/// we do not know about. Hence, this function ignores this case.
///
/// Returns `InvalidOid` if no relation matching the criteria could be found.
///
/// `my_database_tablespace` is C's `MyDatabaseTableSpace` (globals.c),
/// passed explicitly — no ambient-global seams.
pub fn RelidByRelfilenumber(
    mut reltablespace: Oid,
    relfilenumber: RelFileNumber,
    my_database_tablespace: Oid,
) -> PgResult<Oid> {
    let initialized = RELFILENUMBER_MAP.with(|cell| cell.borrow().is_some());
    if !initialized {
        InitializeRelfilenumberMap()?;
    }

    // pg_class will show 0 when the value is actually MyDatabaseTableSpace
    if reltablespace == my_database_tablespace {
        reltablespace = 0;
    }

    let key = RelfilenumberMapKey { reltablespace, relfilenumber };

    // Check cache and return entry if one is found. Even if no target
    // relation can be found later on we store the negative match and return
    // an InvalidOid from cache. That's not really necessary for performance
    // since querying invalid values isn't supposed to be a frequent thing,
    // but it's basically free.
    let cached = RELFILENUMBER_MAP
        .with(|cell| cell.borrow().as_ref().and_then(|owned| owned.with(|s| s.hash.get(&key).copied())));
    if let Some(relid) = cached {
        return Ok(relid);
    }

    // ok, no previous cache entry, do it the hard way

    // initialize empty/negative cache entry before doing the actual lookups
    let mut relid = InvalidOid;

    if reltablespace == GLOBALTABLESPACE_OID {
        // Ok, shared table, check relmapper.
        relid = relmapper_seams::relation_map_filenumber_to_oid::call(relfilenumber, true);
    } else {
        // Not a shared table, could either be a plain relation or a
        // non-shared, nailed one, like e.g. pg_class.

        // copy scankey to local copy and set scan arguments. C does a flat
        // `memcpy(skey, relfilenumber_skey, sizeof(skey))` then overwrites
        // `sk_argument`. The cached `sk_argument` is always the by-value
        // `Datum::null()` from `InitializeRelfilenumberMap` and is discarded
        // here anyway, so we copy out the plain key fields (which carry no
        // `'mcx` borrow) under the short `with` borrow and rebuild the local
        // `ScanKeyData<'mcx>` with the fresh by-value arguments — equivalent to
        // the C memcpy-then-overwrite without escaping the borrowed lifetime.
        let key_parts = RELFILENUMBER_MAP.with(|cell| {
            cell.borrow().as_ref().unwrap().with(|s| {
                let mut parts = [ScanKeyData::empty(), ScanKeyData::empty()];
                for (dst, src) in parts.iter_mut().zip(s.relfilenumber_skey.iter()) {
                    dst.sk_flags = src.sk_flags;
                    dst.sk_attno = src.sk_attno;
                    dst.sk_strategy = src.sk_strategy;
                    dst.sk_subtype = src.sk_subtype;
                    dst.sk_collation = src.sk_collation;
                    dst.sk_func = src.sk_func.clone();
                    // src.sk_argument is the cached by-value null; the local
                    // copy's sk_argument is set below, matching C.
                }
                parts
            })
        });
        let mut skey = key_parts;
        skey[0].sk_argument = Datum::from_oid(reltablespace);
        skey[1].sk_argument = Datum::from_oid(relfilenumber);

        // check for plain relations by looking in pg_class. The scan
        // temporaries land in a scratch context dropped below.
        let scratch = MemoryContext::new("RelidByRelfilenumber scan");
        let smcx = scratch.mcx();
        let relation = table_open(smcx, RelationRelationId, AccessShareLock)?;
        let mut scandesc = genam_seams::systable_beginscan::call(
            &relation,
            ClassTblspcRelfilenodeIndexId,
            true,
            None,
            &skey,
        )?;

        let mut found = false;
        while let Some(ntp) = genam_seams::systable_getnext::call(smcx, scandesc.desc_mut())? {
            // Form_pg_class classform = (Form_pg_class) GETSTRUCT(ntp);
            // field reads, via the deformed columns.
            let row = heap_deform_tuple(smcx, &ntp.tuple, &relation.rd_att, &ntp.data)?;
            let relpersistence = match &row[(Anum_pg_class_relpersistence - 1) as usize].0 {
                Datum::ByVal(d) => Datum::from_usize(*d).as_char(),
                Datum::ByRef(_) => {
                    return Err(PgError::error("relpersistence is not by-value"))
                }
                Datum::Cstring(_) | Datum::Composite(_) | Datum::Expanded(_) | Datum::Internal(_) => {
                    return Err(PgError::error("relpersistence is not by-value"))
                }
            };
            if relpersistence == RELPERSISTENCE_TEMP {
                continue;
            }

            if found {
                return Err(PgError::error(format!(
                    "unexpected duplicate for tablespace {reltablespace}, relfilenumber {relfilenumber}"
                )));
            }
            found = true;

            let classform_oid = match &row[(Anum_pg_class_oid - 1) as usize].0 {
                Datum::ByVal(d) => Datum::from_usize(*d).as_oid(),
                Datum::ByRef(_) => return Err(PgError::error("pg_class.oid is not by-value")),
                Datum::Cstring(_) | Datum::Composite(_) | Datum::Expanded(_) | Datum::Internal(_) => {
                    return Err(PgError::error("pg_class.oid is not by-value"))
                }
            };
            #[cfg(debug_assertions)]
            {
                let row_tblspc = match &row[(Anum_pg_class_reltablespace - 1) as usize].0 {
                    Datum::ByVal(d) => Datum::from_usize(*d).as_oid(),
                    Datum::ByRef(_) => InvalidOid,
                    Datum::Cstring(_) | Datum::Composite(_) | Datum::Expanded(_) | Datum::Internal(_) => InvalidOid,
                };
                let row_filenode = match &row[(Anum_pg_class_relfilenode - 1) as usize].0 {
                    Datum::ByVal(d) => Datum::from_usize(*d).as_oid(),
                    Datum::ByRef(_) => InvalidOid,
                    Datum::Cstring(_) | Datum::Composite(_) | Datum::Expanded(_) | Datum::Internal(_) => InvalidOid,
                };
                debug_assert_eq!(row_tblspc, reltablespace);
                debug_assert_eq!(row_filenode, relfilenumber);
            }
            relid = classform_oid;
        }

        scandesc.end()?;
        table_close(relation, AccessShareLock)?;
        drop(scratch);

        // check for tables that are mapped but not shared
        if !found {
            relid = relmapper_seams::relation_map_filenumber_to_oid::call(relfilenumber, false);
        }
    }

    // Only enter entry into cache now, our opening of pg_class could have
    // caused cache invalidations to be executed which would have deleted a
    // new entry if we had entered it above.
    RELFILENUMBER_MAP.with(|cell| -> PgResult<()> {
        let mut slot = cell.borrow_mut();
        let owned = slot
            .as_mut()
            .ok_or_else(|| PgError::error("RelidByRelfilenumber: map not initialized"))?;
        owned.with_mut(|map| -> PgResult<()> {
            map.hash
                .try_reserve(1)
                .map_err(|_| map.mcx.oom(core::mem::size_of::<(RelfilenumberMapKey, Oid)>()))?;
            if map.hash.insert(key, relid).is_some() {
                // C: hash_search(HASH_ENTER) reported the key already present.
                return Err(PgError::error("corrupted hashtable"));
            }
            Ok(())
        })
    })?;

    Ok(relid)
}

/// This crate declares no inward seams (callers depend on it directly).
pub fn init_seams() {}
