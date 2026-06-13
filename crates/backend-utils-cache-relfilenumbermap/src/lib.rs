//! `utils/cache/relfilenumbermap.c` — relfilenumber to oid mapping cache.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::cell::RefCell;

use backend_access_index_genam_seams as genam_seams;
use backend_utils_cache_inval_seams as inval_seams;
use backend_utils_cache_relmapper_seams as relmapper_seams;
use mcx::{McxOwned, Mcx, MemoryContext, PgHashMap};
use types_cache::{BTEqualStrategyNumber, ScanKeyInit, F_OIDEQ};
use types_core::{InvalidOid, Oid, RelFileNumber};
use types_datum::Datum;
use types_error::{PgError, PgResult};
use types_tuple::backend_access_common_heaptuple::TupleValue;

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
    /// in `InitializeRelfilenumberMap` (the comparison proc `F_OIDEQ` crosses
    /// the genam seam unresolved; the C `fmgr_info_cxt` resolution into
    /// `sk_func` happens owner-side at scan time).
    relfilenumber_skey: [ScanKeyInit; 2],
    /// `static HTAB *RelfilenumberMapHash` — the entry value is `relid`
    /// (`pg_class.oid`; `InvalidOid` is a negative cache entry).
    hash: PgHashMap<'mcx, RelfilenumberMapKey, Oid>,
}

mcx::bind!(RelfilenumberMapTy => RelfilenumberMap<'mcx>);

thread_local! {
    static RELFILENUMBER_MAP: RefCell<Option<McxOwned<RelfilenumberMapTy>>> =
        const { RefCell::new(None) };
}

/// `RelfilenumberMapInvalidateCallback` — flush mapping entries when pg_class
/// is updated in a relevant fashion.
fn RelfilenumberMapInvalidateCallback(_arg: Datum, relid: Oid) {
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
    // build skey
    let mut skey = [ScanKeyInit {
        sk_attno: 0,
        sk_strategy: BTEqualStrategyNumber,
        sk_procedure: F_OIDEQ,
        sk_argument: Datum::null(),
        sk_subtype: InvalidOid,
        sk_collation: InvalidOid,
    }; 2];
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
        Datum::null(),
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

        // copy scankey to local copy and set scan arguments
        let mut skey = RELFILENUMBER_MAP
            .with(|cell| cell.borrow().as_ref().unwrap().with(|s| s.relfilenumber_skey));
        skey[0].sk_argument = Datum::from_oid(reltablespace);
        skey[1].sk_argument = Datum::from_oid(relfilenumber);

        // check for plain relations by looking in pg_class
        // (table_open(RelationRelationId, AccessShareLock) +
        // systable_beginscan(.., ClassTblspcRelfilenodeIndexId, true, NULL,
        // 2, skey) + getnext loop + endscan + table_close, batched across
        // the genam seam; the rows land in a scratch context dropped below).
        let scratch = MemoryContext::new("RelidByRelfilenumber scan");
        let rows = genam_seams::systable_scan::call(
            scratch.mcx(),
            RelationRelationId,
            ClassTblspcRelfilenodeIndexId,
            true,
            &skey,
        )?;

        let mut found = false;
        for row in &rows {
            let relpersistence = match &row[(Anum_pg_class_relpersistence - 1) as usize].0 {
                TupleValue::ByVal(d) => d.as_char(),
                TupleValue::ByRef(_) => {
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
                TupleValue::ByVal(d) => d.as_oid(),
                TupleValue::ByRef(_) => return Err(PgError::error("pg_class.oid is not by-value")),
            };
            #[cfg(debug_assertions)]
            {
                let row_tblspc = match &row[(Anum_pg_class_reltablespace - 1) as usize].0 {
                    TupleValue::ByVal(d) => d.as_oid(),
                    TupleValue::ByRef(_) => InvalidOid,
                };
                let row_filenode = match &row[(Anum_pg_class_relfilenode - 1) as usize].0 {
                    TupleValue::ByVal(d) => d.as_oid(),
                    TupleValue::ByRef(_) => InvalidOid,
                };
                debug_assert_eq!(row_tblspc, reltablespace);
                debug_assert_eq!(row_filenode, relfilenumber);
            }
            relid = classform_oid;
        }
        drop(rows);
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
