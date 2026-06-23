//! `utils/cache/attoptcache.c` — attribute options cache management.
//!
//! Attribute options are cached separately from the fixed-size portion of
//! pg_attribute entries, which are handled by the relcache.
//!
//! The C `HTAB` lives in `CacheMemoryContext` and is keyed by the two-arg
//! system-cache hash (`relatt_cache_syshash`) so that
//! `hash_seq_init_with_hash_value` can flush selectively; here the map's own
//! hashing is internal and the syscache hash value is stored per entry, with
//! [`InvalidateAttoptCacheCallback`] filtering on it. The map and its owning
//! context (the per-table `CacheMemoryContext` analog) live in a
//! `thread_local!`, matching the per-backend C static.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::cell::RefCell;

use reloptions_seams as reloptions_seams;
use inval_seams as inval_seams;
use cache_syscache as syscache;
use mcx::{McxOwned, Mcx, MemoryContext, PgHashMap};
use cache::SysCacheKey;
use types_core::Oid;
// Bare-word machine-word `Datum` (`datum::Datum`), aliased `ScalarWord`.
// The system-cache search keys (`SysCacheKey::Value`) and the syscache
// invalidation callback's `arg` (`SyscacheCallbackFunction`) are audited bare
// words (C: `Datum key1..key4`, `Datum arg`); both contracts live in
// `types-cache`, so the word stays here at that edge.
use datum::Datum as ScalarWord;
use types_error::{PgError, PgResult};
use types_reloptions::AttributeOpts;
// The canonical owned `Datum<'mcx>` enum — the value carried by a deformed
// catalog column (`SysCacheGetAttr`).
use types_tuple::heaptuple::Datum;

/// `Anum_pg_attribute_attoptions` (`catalog/pg_attribute.h`).
const Anum_pg_attribute_attoptions: i32 = 23;

/// `AttoptCacheKey` — attrelid and attnum form the lookup key. (The C code
/// `memset`s the struct to zero padding bits for the by-bytes hash; a Rust
/// value hashes by field, so no equivalent is needed.)
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct AttoptCacheKey {
    attrelid: Oid,
    attnum: i32,
}

/// `AttoptCacheEntry` minus the embedded key: the parsed options (or `None`)
/// plus the entry's syscache hash value (in C the HTAB's hash function *is*
/// `relatt_cache_syshash`; stored here so the selective flush can filter).
#[derive(Clone, Copy, Debug)]
struct AttoptCacheEntry {
    /// options, or `None` if none.
    opts: Option<AttributeOpts>,
    hash_value: u32,
}

struct AttoptCache<'mcx> {
    mcx: Mcx<'mcx>,
    hash: PgHashMap<'mcx, AttoptCacheKey, AttoptCacheEntry>,
}

mcx::bind!(AttoptCacheTy => AttoptCache<'mcx>);

thread_local! {
    /// `static HTAB *AttoptCacheHash = NULL;` — `None` until the cache is
    /// initialized (the lazy-init sentinel of `get_attribute_options`).
    static ATTOPT_CACHE: RefCell<Option<McxOwned<AttoptCacheTy>>> = const { RefCell::new(None) };
}

/// `InvalidateAttoptCacheCallback` — flush cache entry (or entries) when
/// pg_attribute is updated.
///
/// When pg_attribute is updated, we must flush the cache entry at least for
/// that attribute.
fn InvalidateAttoptCacheCallback(_arg: ScalarWord, _cacheid: i32, hashvalue: u32) {
    ATTOPT_CACHE.with(|cell| {
        let mut slot = cell.borrow_mut();
        let Some(owned) = slot.as_mut() else { return };
        owned.with_mut(|cache| {
            // By convention, zero hash value is passed to the callback as a
            // sign that it's time to invalidate the whole cache. See
            // sinval.c, inval.c and InvalidateSystemCachesExtended().
            //
            // C walks hash_seq_init[_with_hash_value], pfree()s each entry's
            // opts and HASH_REMOVEs it; the entry payload here is by-value,
            // so removal is the whole job. (The C `elog(ERROR, "hash table
            // corrupted")` guards a HASH_REMOVE miss that cannot occur for a
            // map removal.)
            if hashvalue == 0 {
                cache.hash.clear();
            } else {
                cache.hash.retain(|_, entry| entry.hash_value != hashvalue);
            }
        });
    });
}

/// Hash function compatible with the two-arg system cache hash function
/// (`relatt_cache_syshash`): `GetSysCacheHashValue2(ATTNUM, attrelid, attnum)`.
fn relatt_cache_syshash(key: &AttoptCacheKey) -> PgResult<u32> {
    syscache::GetSysCacheHashValue2(
        syscache::ATTNUM,
        SysCacheKey::Value(ScalarWord::from_oid(key.attrelid)),
        SysCacheKey::Value(ScalarWord::from_i32(key.attnum)),
    )
}

/// `InitializeAttoptCache` — initialize the attribute options cache: create
/// the hash table and watch for invalidation events.
///
/// The C "make sure we've initialized CacheMemoryContext" step has no
/// counterpart: the map's owning context is created with it.
fn InitializeAttoptCache() -> PgResult<()> {
    let owned = McxOwned::<AttoptCacheTy>::try_new(MemoryContext::new("Attopt cache"), |mcx| {
        Ok(AttoptCache { mcx, hash: PgHashMap::new_in(mcx) })
    })?;
    ATTOPT_CACHE.with(|cell| {
        *cell.borrow_mut() = Some(owned);
    });

    // Watch for invalidation events.
    inval_seams::cache_register_syscache_callback::call(
        syscache::ATTNUM,
        InvalidateAttoptCacheCallback,
        ScalarWord::null(),
    )
}

/// `get_attribute_options` — fetch attribute options for a specified table
/// OID/attnum.
///
/// C returns a freshly palloc'd copy in the caller's context (or NULL when
/// there are no options); `AttributeOpts` is plain by-value data here, so the
/// copy is the returned value itself.
pub fn get_attribute_options(attrelid: Oid, attnum: i32) -> PgResult<Option<AttributeOpts>> {
    // Find existing cache entry, if any.
    let initialized = ATTOPT_CACHE.with(|cell| cell.borrow().is_some());
    if !initialized {
        InitializeAttoptCache()?;
    }
    let key = AttoptCacheKey { attrelid, attnum };

    let cached = ATTOPT_CACHE.with(|cell| {
        cell.borrow().as_ref().and_then(|owned| owned.with(|s| s.hash.get(&key).copied()))
    });
    if let Some(entry) = cached {
        // Return results in caller's memory context (a by-value copy).
        return Ok(entry.opts);
    }

    // Not found in Attopt cache. Construct new cache entry.
    //
    // The syscache read's transient tuple copy lives in a scratch context
    // dropped at the end of this block (C: the catcache's own entry, released
    // with ReleaseSysCache).
    let opts = {
        let scratch = MemoryContext::new("attopt cache lookup");
        let mcx = scratch.mcx();

        let tp = syscache::SearchSysCache2(
            mcx,
            syscache::ATTNUM,
            SysCacheKey::Value(ScalarWord::from_oid(attrelid)),
            SysCacheKey::Value(ScalarWord::from_i16(attnum as i16)),
        )?;

        // If we don't find a valid HeapTuple, it must mean someone has
        // managed to request attribute details for a non-existent attribute.
        // We treat that case as if no options were specified.
        match tp {
            None => None,
            Some(tup) => {
                let (datum, is_null) = syscache::SysCacheGetAttr(
                    mcx,
                    syscache::ATTNUM,
                    &tup,
                    Anum_pg_attribute_attoptions,
                )?;
                let opts = if is_null {
                    None
                } else {
                    let bytes = match &datum {
                        Datum::ByRef(b) => &b[..],
                        Datum::ByVal(_)
                        | Datum::Cstring(_)
                        | Datum::Composite(_)
                        | Datum::Expanded(_)
                        | Datum::Internal(_) => {
                            return Err(PgError::error("attoptions datum is not by-reference"))
                        }
                    };
                    // bytea_opts = attribute_reloptions(datum, false), then
                    // the C copies it into CacheMemoryContext; the parsed
                    // struct is by-value here, stored in the entry below.
                    Some(reloptions_seams::attribute_reloptions::call(bytes, false)?)
                };
                syscache::ReleaseSysCache(tup);
                opts
            }
        }
    };

    // It's important to create the actual cache entry only after reading
    // pg_attribute, since the read could cause a cache flush.
    let hash_value = relatt_cache_syshash(&key)?;
    ATTOPT_CACHE.with(|cell| -> PgResult<()> {
        let mut slot = cell.borrow_mut();
        let owned = slot
            .as_mut()
            .ok_or_else(|| PgError::error("get_attribute_options: AttoptCache not initialized"))?;
        owned.with_mut(|cache| -> PgResult<()> {
            cache
                .hash
                .try_reserve(1)
                .map_err(|_| cache.mcx.oom(core::mem::size_of::<AttoptCacheEntry>()))?;
            cache.hash.insert(key, AttoptCacheEntry { opts, hash_value });
            Ok(())
        })
    })?;

    Ok(opts)
}

/// This crate declares no inward seams (callers depend on it directly).
pub fn init_seams() {}
