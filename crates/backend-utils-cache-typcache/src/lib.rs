//! `backend/utils/cache/typcache.c` — the type metadata cache.
//!
//! Owned-value rewrite of PostgreSQL 18.3 `typcache.c`. The cache *logic* lives
//! here: the `TypeCacheHash` (keyed by type OID), the `RelIdToTypeId` map, the
//! domain-type chain, the `RecordCache` array + hash, the in-progress list, the
//! enum sort tables, the `DomainConstraintCache` refcounting, the flag state
//! machine driving eq/lt/gt/cmp/hash opclass resolution and the
//! composite/range/multirange/domain/enum sub-loads, plus invalidation
//! processing.
//!
//! Genuinely-external work — system-catalog reads (pg_type/pg_range/pg_enum/
//! pg_constraint), opclass/opfamily/operator/proc resolution, relcache
//! composite-tupdesc access, fmgr lookup, the owned-`TupleDesc` transfer
//! operations, domain-constraint expression planning/initialization, callback
//! registration, and the DSA shared record-typmod registry — crosses the
//! boundary through the per-owner seam crates and panics loudly until the
//! owner lands.
//!
//! # Idiomatic shape
//!
//! C threads a `TypeCacheEntry *` between functions; identity there is really
//! the type OID (the hash key). The whole cache state is a backend-lifetime
//! `McxOwned<TypCacheStateTy>` (the `CacheMemoryContext` analog) accessed only
//! through lifetime-universal closures, so a `'mcx`-bearing handle can never
//! escape. We therefore thread the **type OID** as the entry identity: the
//! cross-entry links (`nextDomain`, `rngelemtype`, `rngtype`) are OIDs, and
//! every entry access is a `with_state(|st| st.type_cache.get(&oid)...)`
//! re-lookup. This matches C's semantics (the pointer is stable for the
//! backend's life) without aliasing a `&mut` across the re-entrant lookups.
//!
//! PostgreSQL is single-threaded per backend, so the process-global mutable
//! state is a `thread_local!`, exactly mirroring the C globals.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use mcx::{Mcx, McxOwned, MemoryContext, PgBox, PgVec};
use std::collections::HashMap;

use types_cache::typcache::{
    DomainConstraintState, DomainCtxHandle, PgTypeRow,
    DOM_CONSTRAINT_CHECK, DOM_CONSTRAINT_NOTNULL,
};
use types_core::fmgr::FmgrInfo;
use types_core::primitive::{Oid, INVALID_OID};
use types_error::{
    PgError, PgResult, SqlState, ERRCODE_DATATYPE_MISMATCH, ERRCODE_OUT_OF_MEMORY,
    ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERRCODE_UNDEFINED_FUNCTION, ERRCODE_UNDEFINED_OBJECT,
    ERRCODE_WRONG_OBJECT_TYPE,
};
use types_tuple::heaptuple::{TupleDescData, RECORDOID};

// Bare-word machine-word `Datum` (`types_datum::Datum`), aliased `ScalarWord`.
// Kept only at the cache-callback registration ABI edge: the syscache/relcache
// callback `arg` is a plain machine word that C passes as `(Datum) 0`. The
// value-carrying canonical enum is `types_tuple::backend_access_common_heaptuple::Datum`,
// which typcache does not traffic in (it returns typed entries, not Datums).
use types_datum::Datum as ScalarWord;

use backend_access_common_session_seams as session_seams;
use backend_access_common_tupdesc_seams as tupdesc_seams;
use backend_catalog_pg_enum_seams as pg_enum_seams;
use backend_utils_adt_domains_seams as domains_seams;
use backend_utils_adt_format_type_seams as format_type_seams;
use backend_utils_cache_inval_seams as inval_seams;
use backend_utils_cache_lsyscache_seams as lsyscache_seams;
use backend_utils_cache_relcache_seams as relcache_seams;
use backend_utils_fmgr_fmgr_seams as fmgr_seams;

/* ==========================================================================
 * Public flag bits (utils/typcache.h) — the `flags` argument of
 * lookup_type_cache.
 * ======================================================================== */

pub const TYPECACHE_EQ_OPR: i32 = 0x00001;
pub const TYPECACHE_LT_OPR: i32 = 0x00002;
pub const TYPECACHE_GT_OPR: i32 = 0x00004;
pub const TYPECACHE_CMP_PROC: i32 = 0x00008;
pub const TYPECACHE_HASH_PROC: i32 = 0x00010;
pub const TYPECACHE_EQ_OPR_FINFO: i32 = 0x00020;
pub const TYPECACHE_CMP_PROC_FINFO: i32 = 0x00040;
pub const TYPECACHE_HASH_PROC_FINFO: i32 = 0x00080;
pub const TYPECACHE_TUPDESC: i32 = 0x00100;
pub const TYPECACHE_BTREE_OPFAMILY: i32 = 0x00200;
pub const TYPECACHE_HASH_OPFAMILY: i32 = 0x00400;
pub const TYPECACHE_RANGE_INFO: i32 = 0x00800;
pub const TYPECACHE_DOMAIN_BASE_INFO: i32 = 0x01000;
pub const TYPECACHE_DOMAIN_CONSTR_INFO: i32 = 0x02000;
pub const TYPECACHE_HASH_EXTENDED_PROC: i32 = 0x04000;
pub const TYPECACHE_HASH_EXTENDED_PROC_FINFO: i32 = 0x08000;
pub const TYPECACHE_MULTIRANGE_INFO: i32 = 0x10000;

/* --------------------------------------------------------------------------
 * Catalog/AM constants used by typcache.c (fixed PostgreSQL OID/strategy/proc
 * values).
 * ------------------------------------------------------------------------ */

const BTREE_AM_OID: Oid = 403;
const HASH_AM_OID: Oid = 405;

const BT_LESS_STRATEGY_NUMBER: i16 = 1;
const BT_EQUAL_STRATEGY_NUMBER: i16 = 3;
const BT_GREATER_STRATEGY_NUMBER: i16 = 5;
const HT_EQUAL_STRATEGY_NUMBER: i16 = 1;

const BTORDER_PROC: i16 = 1;
const HASHSTANDARD_PROC: i16 = 1;
const HASHEXTENDED_PROC: i16 = 2;

/* pg_type.typtype codes (pg_type.h) */
const TYPTYPE_COMPOSITE: i8 = b'c' as i8;
const TYPTYPE_DOMAIN: i8 = b'd' as i8;
const TYPTYPE_ENUM: i8 = b'e' as i8;
const TYPTYPE_RANGE: i8 = b'r' as i8;
const TYPTYPE_MULTIRANGE: i8 = b'm' as i8;

/* Well-known operator/function OIDs that typcache.c special-cases. */
const ARRAY_EQ_OP: Oid = 1070;
const ARRAY_LT_OP: Oid = 1072;
const ARRAY_GT_OP: Oid = 1073;
const RECORD_EQ_OP: Oid = 2988;
const RECORD_LT_OP: Oid = 2990;
const RECORD_GT_OP: Oid = 2991;
const F_BTARRAYCMP: Oid = 382;
const F_BTRECORDCMP: Oid = 2987;
const F_HASH_ARRAY: Oid = 626;
const F_HASH_ARRAY_EXTENDED: Oid = 782;
const F_HASH_RECORD: Oid = 6192;
const F_HASH_RECORD_EXTENDED: Oid = 6193;
const F_HASH_RANGE: Oid = 3902;
const F_HASH_RANGE_EXTENDED: Oid = 3417;
const F_HASH_MULTIRANGE: Oid = 4278;
const F_HASH_MULTIRANGE_EXTENDED: Oid = 4279;

/* syscache ids used for callback registration (syscache cacheinfo ordering). */
const TYPEOID: i32 = 82;
const CLAOID: i32 = 14;
const CONSTROID: i32 = 19;

/// `INVALID_TUPLEDESC_IDENTIFIER` (typcache.h) — the "no identifier yet" value.
const INVALID_TUPLEDESC_IDENTIFIER: u64 = 0;

/// `MaxAllocSize` (1 GB - 1) — the `AllocSizeIsValid` ceiling.
const MAX_ALLOC_SIZE: usize = 0x3fff_ffff;

/* --------------------------------------------------------------------------
 * Private flag bits in the TypeCacheEntry.flags field (typcache.c).
 * ------------------------------------------------------------------------ */

const TCFLAGS_HAVE_PG_TYPE_DATA: i32 = 0x000001;
const TCFLAGS_CHECKED_BTREE_OPCLASS: i32 = 0x000002;
const TCFLAGS_CHECKED_HASH_OPCLASS: i32 = 0x000004;
const TCFLAGS_CHECKED_EQ_OPR: i32 = 0x000008;
const TCFLAGS_CHECKED_LT_OPR: i32 = 0x000010;
const TCFLAGS_CHECKED_GT_OPR: i32 = 0x000020;
const TCFLAGS_CHECKED_CMP_PROC: i32 = 0x000040;
const TCFLAGS_CHECKED_HASH_PROC: i32 = 0x000080;
const TCFLAGS_CHECKED_HASH_EXTENDED_PROC: i32 = 0x000100;
const TCFLAGS_CHECKED_ELEM_PROPERTIES: i32 = 0x000200;
const TCFLAGS_HAVE_ELEM_EQUALITY: i32 = 0x000400;
const TCFLAGS_HAVE_ELEM_COMPARE: i32 = 0x000800;
const TCFLAGS_HAVE_ELEM_HASHING: i32 = 0x001000;
const TCFLAGS_HAVE_ELEM_EXTENDED_HASHING: i32 = 0x002000;
const TCFLAGS_CHECKED_FIELD_PROPERTIES: i32 = 0x004000;
const TCFLAGS_HAVE_FIELD_EQUALITY: i32 = 0x008000;
const TCFLAGS_HAVE_FIELD_COMPARE: i32 = 0x010000;
const TCFLAGS_HAVE_FIELD_HASHING: i32 = 0x020000;
const TCFLAGS_HAVE_FIELD_EXTENDED_HASHING: i32 = 0x040000;
const TCFLAGS_CHECKED_DOMAIN_CONSTRAINTS: i32 = 0x080000;
const TCFLAGS_DOMAIN_BASE_IS_COMPOSITE: i32 = 0x100000;

/// The flags associated with equality/comparison/hashing are all but these.
const TCFLAGS_OPERATOR_FLAGS: i32 = !(TCFLAGS_HAVE_PG_TYPE_DATA
    | TCFLAGS_CHECKED_DOMAIN_CONSTRAINTS
    | TCFLAGS_DOMAIN_BASE_IS_COMPOSITE);

/* ==========================================================================
 * Cache node types.
 * ======================================================================== */

/// `TypeCacheEntry` (`utils/typcache.h`), in-crate form.
///
/// Cross-entry links (`rngelemtype`, `rngtype`, `nextDomain`) are type OIDs
/// (re-looked-up in the cache), not handles — see the crate docs. The
/// composite `tup_desc` is owned plain data held INLINE on the entry in the
/// cache context (the C `tupDesc`); callers receive owned `clone_in` copies.
/// `domain_data` holds the shared, refcounted `DomainConstraintCache` directly
/// via `Rc` (the C `domainData` pointer); `enum_data` is the 1:1-owned enum
/// cache held inline (the C `enumData`).
struct TypeCacheEntry<'mcx> {
    type_id: Oid,
    type_id_hash: u32,
    typlen: i16,
    typbyval: bool,
    typalign: i8,
    typstorage: i8,
    typtype: i8,
    typrelid: Oid,
    typsubscript: Oid,
    typelem: Oid,
    typarray: Oid,
    typcollation: Oid,
    btree_opf: Oid,
    btree_opintype: Oid,
    hash_opf: Oid,
    hash_opintype: Oid,
    eq_opr: Oid,
    lt_opr: Oid,
    gt_opr: Oid,
    cmp_proc: Oid,
    hash_proc: Oid,
    hash_extended_proc: Oid,
    eq_opr_finfo: FmgrInfo,
    cmp_proc_finfo: FmgrInfo,
    hash_proc_finfo: FmgrInfo,
    hash_extended_proc_finfo: FmgrInfo,
    /// The composite type's owned descriptor (the C `tupDesc`), or `None`.
    tup_desc: Option<PgBox<'mcx, TupleDescData<'mcx>>>,
    tup_desc_identifier: u64,
    /// Range element type OID (the C `rngelemtype->type_id`), or `None`.
    rngelemtype: Option<Oid>,
    rng_opfamily: Oid,
    rng_collation: Oid,
    rng_cmp_proc_finfo: FmgrInfo,
    rng_canonical_finfo: FmgrInfo,
    rng_subdiff_finfo: FmgrInfo,
    /// Range type OID for a multirange (the C `rngtype->type_id`), or `None`.
    rngtype: Option<Oid>,
    domain_base_type: Oid,
    domain_base_typmod: i32,
    /// The shared, refcounted `DomainConstraintCache`, or `None` (the C
    /// `domainData` pointer). Shared with every live `DomainConstraintRef.dcc`
    /// via `Rc`; the explicit `dcc_refcount` cell mirrors the C `dccRefCount`.
    domain_data: Option<Rc<DomainConstraintCache>>,
    flags: i32,
    /// The entry's enum sort cache (the C `enumData` pointer, owned 1:1 by the
    /// entry). `None` until `load_enum_cache_data` populates it; freed/replaced
    /// in place on reload (C's `pfree(tcache->enumData); tcache->enumData = ..`).
    enum_data: Option<TypeCacheEnumData<'mcx>>,
    /// Next domain entry's OID (the C `nextDomain`), or `None`.
    next_domain: Option<Oid>,
}

impl<'mcx> TypeCacheEntry<'mcx> {
    fn zeroed(type_id: Oid) -> Self {
        TypeCacheEntry {
            type_id,
            type_id_hash: 0,
            typlen: 0,
            typbyval: false,
            typalign: 0,
            typstorage: 0,
            typtype: 0,
            typrelid: INVALID_OID,
            typsubscript: INVALID_OID,
            typelem: INVALID_OID,
            typarray: INVALID_OID,
            typcollation: INVALID_OID,
            btree_opf: INVALID_OID,
            btree_opintype: INVALID_OID,
            hash_opf: INVALID_OID,
            hash_opintype: INVALID_OID,
            eq_opr: INVALID_OID,
            lt_opr: INVALID_OID,
            gt_opr: INVALID_OID,
            cmp_proc: INVALID_OID,
            hash_proc: INVALID_OID,
            hash_extended_proc: INVALID_OID,
            eq_opr_finfo: FmgrInfo::empty(),
            cmp_proc_finfo: FmgrInfo::empty(),
            hash_proc_finfo: FmgrInfo::empty(),
            hash_extended_proc_finfo: FmgrInfo::empty(),
            tup_desc: None,
            tup_desc_identifier: 0,
            rngelemtype: None,
            rng_opfamily: INVALID_OID,
            rng_collation: INVALID_OID,
            rng_cmp_proc_finfo: FmgrInfo::empty(),
            rng_canonical_finfo: FmgrInfo::empty(),
            rng_subdiff_finfo: FmgrInfo::empty(),
            rngtype: None,
            domain_base_type: INVALID_OID,
            domain_base_typmod: 0,
            domain_data: None,
            flags: 0,
            enum_data: None,
            next_domain: None,
        }
    }
}

/// `DomainConstraintCache` (opaque outside typcache.c). The `constraints` list
/// is real in-crate data (built by `load_domaintype_info`); only the planned
/// `check_expr`/`check_exprstate` payloads inside each node are opaque
/// planner/executor handles. `dcc_context` is the external "Domain
/// constraints" memory context the nodes are allocated in.
#[derive(Debug)]
struct DomainConstraintCache {
    constraints: Vec<DomainConstraintState>,
    dcc_context: DomainCtxHandle,
    /// The C `dccRefCount`. Mirrored explicitly (rather than leaning on
    /// `Rc::strong_count`) because dropping the last reference must run the
    /// fallible `delete_domain_ctx` seam, which `Drop` cannot do. Wrapped in a
    /// `Cell` because the struct is shared through `Rc` (immutable borrows).
    dcc_refcount: Cell<i64>,
}

/// `EnumItem` — OID + its sort position (typcache.c).
#[derive(Clone, Copy, Debug)]
struct EnumItem {
    enum_oid: Oid,
    sort_order: f32,
}

/// `TypeCacheEnumData` — the enum sort cache (typcache.c). The `enum_values`
/// array and the `sorted_values` bitmap are copied into the cache context (the
/// C `load_enum_cache_data` "copy the data into CacheMemoryContext").
struct TypeCacheEnumData<'mcx> {
    bitmap_base: Oid,
    sorted_values: Bitmapset<'mcx>,
    #[allow(dead_code)]
    num_values: i32,
    enum_values: PgVec<'mcx, EnumItem>,
}

/// Minimal `Bitmapset` matching the membership semantics typcache.c needs.
/// Stores a dense bit array over non-negative indices, charged to the cache
/// context.
struct Bitmapset<'mcx> {
    words: PgVec<'mcx, u64>,
}

impl<'mcx> Bitmapset<'mcx> {
    fn new(mcx: Mcx<'mcx>) -> Self {
        Bitmapset { words: PgVec::new_in(mcx) }
    }
    fn make_singleton(mcx: Mcx<'mcx>, x: i32) -> PgResult<Self> {
        let mut bms = Bitmapset::new(mcx);
        bms.add_member(mcx, x)?;
        Ok(bms)
    }
    fn add_member(&mut self, mcx: Mcx<'mcx>, x: i32) -> PgResult<()> {
        debug_assert!(x >= 0);
        let x = x as usize;
        let word = x / 64;
        let bit = x % 64;
        if word >= self.words.len() {
            let needed = word + 1;
            if needed > MAX_ALLOC_SIZE / core::mem::size_of::<u64>() {
                return Err(mcx.oom(needed * core::mem::size_of::<u64>()));
            }
            self.words
                .try_reserve(needed - self.words.len())
                .map_err(|_| mcx.oom(needed * core::mem::size_of::<u64>()))?;
            while self.words.len() < needed {
                self.words.push(0);
            }
        }
        self.words[word] |= 1u64 << bit;
        Ok(())
    }
    fn is_member(&self, x: i32) -> bool {
        if x < 0 {
            return false;
        }
        let x = x as usize;
        let word = x / 64;
        let bit = x % 64;
        self.words.get(word).is_some_and(|w| (w >> bit) & 1 != 0)
    }
    fn num_members(&self) -> i32 {
        self.words.iter().map(|w| w.count_ones() as i32).sum()
    }
}

/// `RecordCacheArrayEntry` (typcache.c).
struct RecordCacheArrayEntry<'mcx> {
    id: u64,
    tupdesc: Option<PgBox<'mcx, TupleDescData<'mcx>>>,
}

impl Default for RecordCacheArrayEntry<'_> {
    fn default() -> Self {
        RecordCacheArrayEntry { id: 0, tupdesc: None }
    }
}

/// Process-wide typcache state (the C file-scope globals).
struct TypCacheState<'mcx> {
    mcx: Mcx<'mcx>,
    /// Whether the cache (and its callbacks) have been initialized.
    initialized: bool,
    /// `TypeCacheHash` — keyed by type OID; entries live for the backend's
    /// life.
    type_cache: HashMap<Oid, TypeCacheEntry<'mcx>>,
    /// `RelIdToTypeIdCacheHash` — relid → composite type OID.
    rel_id_to_type_id: HashMap<Oid, Oid>,
    /// `firstDomainTypeEntry` — head OID of the domain-entry chain threaded via
    /// `TypeCacheEntry.next_domain`.
    first_domain_type_entry: Option<Oid>,
    /// Monotonic allocator for `DomainConstraintRef` identity tokens (handed to
    /// the external reset-callback ABI, which can only carry a plain word).
    next_token: u64,
    /// `RecordCacheHash` — structural row type → stored descriptor ids.
    record_cache: HashMap<u32, PgVec<'mcx, u64>>,
    /// `RecordCacheArray` — indexed by assigned typmod.
    record_cache_array: PgVec<'mcx, RecordCacheArrayEntry<'mcx>>,
    /// `NextRecordTypmod` — number of entries used.
    next_record_typmod: i32,
    /// `tupledesc_id_counter`.
    tupledesc_id_counter: u64,
    /// `in_progress_list`.
    in_progress_list: PgVec<'mcx, Oid>,
    /// Live `DomainConstraintRef`s, keyed by token, so a reset callback can
    /// release the dcc refcount. The external memory-context reset machinery
    /// can only carry a plain word (`ref_token`) back to us, not the
    /// caller-owned `DomainConstraintRef` itself, so we keep the ref's `Rc`
    /// share here for the callback to release. Removing this table entirely
    /// would require the reset-callback ABI to hand back the real ref — a
    /// cross-crate carrier keystone outside this crate.
    refs: HashMap<u64, RefRecord>,
}

/// The recorded part of a `DomainConstraintRef` needed by the reset callback:
/// the shared `Rc` it holds, so releasing it decrements the dcc refcount.
#[derive(Debug)]
struct RefRecord {
    dcc: Option<Rc<DomainConstraintCache>>,
}

impl<'mcx> TypCacheState<'mcx> {
    fn new(mcx: Mcx<'mcx>) -> Self {
        TypCacheState {
            mcx,
            initialized: false,
            type_cache: HashMap::new(),
            rel_id_to_type_id: HashMap::new(),
            first_domain_type_entry: None,
            next_token: 1,
            record_cache: HashMap::new(),
            record_cache_array: PgVec::new_in(mcx),
            next_record_typmod: 0,
            tupledesc_id_counter: INVALID_TUPLEDESC_IDENTIFIER,
            in_progress_list: PgVec::new_in(mcx),
            refs: HashMap::new(),
        }
    }

    fn fresh_token(&mut self) -> u64 {
        let t = self.next_token;
        self.next_token += 1;
        t
    }

    fn entry(&self, type_id: Oid) -> &TypeCacheEntry<'mcx> {
        self.type_cache
            .get(&type_id)
            .expect("typcache: entry must exist for type_id")
    }
    fn entry_mut(&mut self, type_id: Oid) -> &mut TypeCacheEntry<'mcx> {
        self.type_cache
            .get_mut(&type_id)
            .expect("typcache: entry must exist for type_id")
    }
}

mcx::bind!(TypCacheStateTy => TypCacheState<'mcx>);

thread_local! {
    /// The crate's `CacheMemoryContext` analog co-owning the process-global
    /// typcache state. `None` until first use.
    static STATE: RefCell<Option<McxOwned<TypCacheStateTy>>> = const { RefCell::new(None) };
}

/// Run `f` over the backend-local typcache state, creating it on first use.
/// Catalog/seam reads happen OUTSIDE this borrow so an invalidation callback
/// fired mid-read can take the state.
fn with_state<R>(f: impl for<'mcx> FnOnce(&mut TypCacheState<'mcx>) -> R) -> R {
    STATE.with(|s| {
        {
            let mut slot = s.borrow_mut();
            if slot.is_none() {
                let owned =
                    McxOwned::<TypCacheStateTy>::try_new(MemoryContext::new("CacheMemoryContext"), |mcx| {
                        Ok(TypCacheState::new(mcx))
                    })
                    .expect("allocating the empty typcache state cannot fail");
                *slot = Some(owned);
            }
        }
        let mut slot = s.borrow_mut();
        slot.as_mut().unwrap().with_mut(f)
    })
}

/* --------------------------------------------------------------------------
 * Small helpers mirroring C macros/inlines.
 * ------------------------------------------------------------------------ */

#[inline]
fn oid_is_valid(oid: Oid) -> bool {
    oid != INVALID_OID
}

#[inline]
fn reg_procedure_is_valid(p: Oid) -> bool {
    p != INVALID_OID
}

fn ereport_error<T>(code: SqlState, msg: String) -> PgResult<T> {
    Err(PgError::error(msg).with_sqlstate(code))
}

fn elog_error<T>(msg: String) -> PgResult<T> {
    Err(PgError::error(msg))
}

/// `format_type_be(type_id)` for an error message, using a scratch context.
fn format_type(type_id: Oid) -> PgResult<String> {
    let scratch = MemoryContext::new("typcache format_type");
    let s = format_type_seams::format_type_be::call(scratch.mcx(), type_id)?;
    Ok(s.as_str().to_string())
}

/* ==========================================================================
 * lookup_type_cache
 * ======================================================================== */

/// `lookup_type_cache` — fetch/build the `TypeCacheEntry` for `type_id`,
/// ensuring the fields requested by `flags` are computed. Returns nothing
/// directly usable (the entry stays in the cache); callers read what they need
/// through the typed public accessors below.
pub fn lookup_type_cache(type_id: Oid, flags: i32) -> PgResult<()> {
    let mut flags = flags;

    // First time through: initialize the hash table + register callbacks.
    let need_init = with_state(|st| !st.initialized);
    if need_init {
        inval_seams::cache_register_relcache_callback::call(
            type_cache_rel_callback,
            ScalarWord::null(),
        )?;
        inval_seams::cache_register_syscache_callback::call(
            TYPEOID,
            type_cache_typ_callback,
            ScalarWord::null(),
        )?;
        inval_seams::cache_register_syscache_callback::call(
            CLAOID,
            type_cache_opc_callback,
            ScalarWord::null(),
        )?;
        inval_seams::cache_register_syscache_callback::call(
            CONSTROID,
            type_cache_constr_callback,
            ScalarWord::null(),
        )?;
        with_state(|st| -> PgResult<()> {
            st.initialized = true;
            // C reserves allocsize = 4 in_progress_list slots in
            // CacheMemoryContext.
            st.in_progress_list
                .try_reserve(4)
                .map_err(|_| st.mcx.oom(4 * core::mem::size_of::<Oid>()))?;
            Ok(())
        })?;
    }

    // Register to catch invalidation messages: push our type_id on the
    // in-progress list (record the offset for the final assert).
    let in_progress_offset = with_state(|st| -> PgResult<usize> {
        let off = st.in_progress_list.len();
        st.in_progress_list
            .try_reserve(1)
            .map_err(|_| st.mcx.oom(core::mem::size_of::<Oid>()))?;
        st.in_progress_list.push(type_id);
        Ok(off)
    })?;

    let exists = with_state(|st| st.type_cache.contains_key(&type_id));

    if !exists {
        // Look up the pg_type row first, so we don't make a cache entry for an
        // invalid type OID. On the early-error paths the C ereport(ERROR)s and
        // never reaches the in_progress_list_len-- at the end, so the slot is
        // left for finalize_in_progress_typentries(); we mirror that (return
        // Err leaving the pushed slot).
        let typtup = match lsyscache_seams::lookup_pg_type::call(type_id)? {
            None => {
                return ereport_error(
                    ERRCODE_UNDEFINED_OBJECT,
                    format!("type with OID {type_id} does not exist"),
                );
            }
            Some(t) => t,
        };
        if !typtup.typisdefined {
            return ereport_error(
                ERRCODE_UNDEFINED_OBJECT,
                format!("type \"{}\" is only a shell", typtup.typname),
            );
        }

        let type_id_hash = lsyscache_seams::syscache_hash_value_typeoid::call(type_id)?;

        with_state(|st| {
            let mut e = TypeCacheEntry::zeroed(type_id);
            e.type_id_hash = type_id_hash;
            copy_pg_type_fields(&mut e, &typtup);
            e.flags |= TCFLAGS_HAVE_PG_TYPE_DATA;
            let is_domain = typtup.typtype == TYPTYPE_DOMAIN;

            // If it's a domain, thread it into the domain list head.
            if is_domain {
                let prev = st.first_domain_type_entry.take();
                e.next_domain = prev;
                st.first_domain_type_entry = Some(type_id);
            }
            // C: hash_search(..., HASH_ENTER, &found); Assert(!found).
            let prev = st.type_cache.insert(type_id, e);
            debug_assert!(prev.is_none(), "it wasn't there a moment ago");
        });
    } else {
        // Have an entry; reload pg_type data if it was invalidated.
        let need_reload =
            with_state(|st| (st.entry(type_id).flags & TCFLAGS_HAVE_PG_TYPE_DATA) == 0);
        if need_reload {
            let typtup = match lsyscache_seams::lookup_pg_type::call(type_id)? {
                None => {
                    return ereport_error(
                        ERRCODE_UNDEFINED_OBJECT,
                        format!("type with OID {type_id} does not exist"),
                    );
                }
                Some(t) => t,
            };
            if !typtup.typisdefined {
                return ereport_error(
                    ERRCODE_UNDEFINED_OBJECT,
                    format!("type \"{}\" is only a shell", typtup.typname),
                );
            }
            with_state(|st| {
                let e = st.entry_mut(type_id);
                copy_pg_type_fields(e, &typtup);
                e.flags |= TCFLAGS_HAVE_PG_TYPE_DATA;
            });
        }
    }

    // The opclass-resolution + sub-load portion. Propagating Err mirrors the C
    // ereport(ERROR): the in_progress_list slot is left for
    // finalize_in_progress_typentries() (AtEOXact/AtEOSubXact).
    build_type_cache_entry(type_id, &mut flags)?;

    domains_seams::injection_point::call("typecache-before-rel-type-cache-insert");

    with_state(|st| {
        debug_assert_eq!(in_progress_offset + 1, st.in_progress_list.len());
        st.in_progress_list.pop();
    });

    insert_rel_type_cache_if_needed(type_id);

    Ok(())
}

/// Copy the subsidiary `Form_pg_type` fields into the entry.
fn copy_pg_type_fields(e: &mut TypeCacheEntry, t: &PgTypeRow) {
    e.typlen = t.typlen;
    e.typbyval = t.typbyval;
    e.typalign = t.typalign;
    e.typstorage = t.typstorage;
    e.typtype = t.typtype;
    e.typrelid = t.typrelid;
    e.typsubscript = t.typsubscript;
    e.typelem = t.typelem;
    e.typarray = t.typarray;
    e.typcollation = t.typcollation;
}

/// The opclass-resolution + sub-load portion of `lookup_type_cache`.
fn build_type_cache_entry(type_id: Oid, flags: &mut i32) -> PgResult<()> {
    // Look up opclasses if we haven't already and any dependent info requested.
    {
        let (need, ef) = with_state(|st| {
            let e = st.entry(type_id);
            (
                (*flags
                    & (TYPECACHE_EQ_OPR
                        | TYPECACHE_LT_OPR
                        | TYPECACHE_GT_OPR
                        | TYPECACHE_CMP_PROC
                        | TYPECACHE_EQ_OPR_FINFO
                        | TYPECACHE_CMP_PROC_FINFO
                        | TYPECACHE_BTREE_OPFAMILY))
                    != 0,
                e.flags,
            )
        });
        if need && (ef & TCFLAGS_CHECKED_BTREE_OPCLASS) == 0 {
            let opclass = lsyscache_seams::get_default_opclass::call(type_id, BTREE_AM_OID)?;
            let (opf, opintype) = if oid_is_valid(opclass) {
                (
                    lsyscache_seams::get_opclass_family::call(opclass)?,
                    lsyscache_seams::get_opclass_input_type::call(opclass)?,
                )
            } else {
                (INVALID_OID, INVALID_OID)
            };
            with_state(|st| {
                let e = st.entry_mut(type_id);
                e.btree_opf = opf;
                e.btree_opintype = opintype;
                e.flags &= !(TCFLAGS_CHECKED_EQ_OPR
                    | TCFLAGS_CHECKED_LT_OPR
                    | TCFLAGS_CHECKED_GT_OPR
                    | TCFLAGS_CHECKED_CMP_PROC);
                e.flags |= TCFLAGS_CHECKED_BTREE_OPCLASS;
            });
        }
    }

    // If we need eq and there's no btree opclass, force hash opclass lookup.
    {
        let force = with_state(|st| {
            let e = st.entry(type_id);
            (*flags & (TYPECACHE_EQ_OPR | TYPECACHE_EQ_OPR_FINFO)) != 0
                && (e.flags & TCFLAGS_CHECKED_EQ_OPR) == 0
                && e.btree_opf == INVALID_OID
        });
        if force {
            *flags |= TYPECACHE_HASH_OPFAMILY;
        }
    }

    {
        let (need, ef) = with_state(|st| {
            let e = st.entry(type_id);
            (
                (*flags
                    & (TYPECACHE_HASH_PROC
                        | TYPECACHE_HASH_PROC_FINFO
                        | TYPECACHE_HASH_EXTENDED_PROC
                        | TYPECACHE_HASH_EXTENDED_PROC_FINFO
                        | TYPECACHE_HASH_OPFAMILY))
                    != 0,
                e.flags,
            )
        });
        if need && (ef & TCFLAGS_CHECKED_HASH_OPCLASS) == 0 {
            let opclass = lsyscache_seams::get_default_opclass::call(type_id, HASH_AM_OID)?;
            let (opf, opintype) = if oid_is_valid(opclass) {
                (
                    lsyscache_seams::get_opclass_family::call(opclass)?,
                    lsyscache_seams::get_opclass_input_type::call(opclass)?,
                )
            } else {
                (INVALID_OID, INVALID_OID)
            };
            with_state(|st| {
                let e = st.entry_mut(type_id);
                e.hash_opf = opf;
                e.hash_opintype = opintype;
                e.flags &= !(TCFLAGS_CHECKED_HASH_PROC | TCFLAGS_CHECKED_HASH_EXTENDED_PROC);
                e.flags |= TCFLAGS_CHECKED_HASH_OPCLASS;
            });
        }
    }

    // Look for requested operators and functions, if we haven't already.
    if needs(type_id, *flags, TYPECACHE_EQ_OPR | TYPECACHE_EQ_OPR_FINFO, TCFLAGS_CHECKED_EQ_OPR) {
        let (btree_opf, btree_opintype, hash_opf, hash_opintype) = with_state(|st| {
            let e = st.entry(type_id);
            (e.btree_opf, e.btree_opintype, e.hash_opf, e.hash_opintype)
        });
        let mut eq_opr = INVALID_OID;
        if btree_opf != INVALID_OID {
            eq_opr = lsyscache_seams::get_opfamily_member::call(
                btree_opf,
                btree_opintype,
                btree_opintype,
                BT_EQUAL_STRATEGY_NUMBER,
            )?;
        }
        if eq_opr == INVALID_OID && hash_opf != INVALID_OID {
            eq_opr = lsyscache_seams::get_opfamily_member::call(
                hash_opf,
                hash_opintype,
                hash_opintype,
                HT_EQUAL_STRATEGY_NUMBER,
            )?;
        }
        if eq_opr == ARRAY_EQ_OP && !array_element_has_equality(type_id)? {
            eq_opr = INVALID_OID;
        } else if eq_opr == RECORD_EQ_OP && !record_fields_have_equality(type_id)? {
            eq_opr = INVALID_OID;
        }
        with_state(|st| {
            let e = st.entry_mut(type_id);
            if e.eq_opr != eq_opr {
                e.eq_opr_finfo.fn_oid = INVALID_OID;
            }
            e.eq_opr = eq_opr;
            e.flags &= !(TCFLAGS_CHECKED_HASH_PROC | TCFLAGS_CHECKED_HASH_EXTENDED_PROC);
            e.flags |= TCFLAGS_CHECKED_EQ_OPR;
        });
    }

    if needs(type_id, *flags, TYPECACHE_LT_OPR, TCFLAGS_CHECKED_LT_OPR) {
        let (btree_opf, btree_opintype) =
            with_state(|st| (st.entry(type_id).btree_opf, st.entry(type_id).btree_opintype));
        let mut lt_opr = INVALID_OID;
        if btree_opf != INVALID_OID {
            lt_opr = lsyscache_seams::get_opfamily_member::call(
                btree_opf,
                btree_opintype,
                btree_opintype,
                BT_LESS_STRATEGY_NUMBER,
            )?;
        }
        if lt_opr == ARRAY_LT_OP && !array_element_has_compare(type_id)? {
            lt_opr = INVALID_OID;
        } else if lt_opr == RECORD_LT_OP && !record_fields_have_compare(type_id)? {
            lt_opr = INVALID_OID;
        }
        with_state(|st| {
            let e = st.entry_mut(type_id);
            e.lt_opr = lt_opr;
            e.flags |= TCFLAGS_CHECKED_LT_OPR;
        });
    }

    if needs(type_id, *flags, TYPECACHE_GT_OPR, TCFLAGS_CHECKED_GT_OPR) {
        let (btree_opf, btree_opintype) =
            with_state(|st| (st.entry(type_id).btree_opf, st.entry(type_id).btree_opintype));
        let mut gt_opr = INVALID_OID;
        if btree_opf != INVALID_OID {
            gt_opr = lsyscache_seams::get_opfamily_member::call(
                btree_opf,
                btree_opintype,
                btree_opintype,
                BT_GREATER_STRATEGY_NUMBER,
            )?;
        }
        if gt_opr == ARRAY_GT_OP && !array_element_has_compare(type_id)? {
            gt_opr = INVALID_OID;
        } else if gt_opr == RECORD_GT_OP && !record_fields_have_compare(type_id)? {
            gt_opr = INVALID_OID;
        }
        with_state(|st| {
            let e = st.entry_mut(type_id);
            e.gt_opr = gt_opr;
            e.flags |= TCFLAGS_CHECKED_GT_OPR;
        });
    }

    if needs(type_id, *flags, TYPECACHE_CMP_PROC | TYPECACHE_CMP_PROC_FINFO, TCFLAGS_CHECKED_CMP_PROC)
    {
        let (btree_opf, btree_opintype) =
            with_state(|st| (st.entry(type_id).btree_opf, st.entry(type_id).btree_opintype));
        let mut cmp_proc = INVALID_OID;
        if btree_opf != INVALID_OID {
            cmp_proc = lsyscache_seams::get_opfamily_proc::call(
                btree_opf,
                btree_opintype,
                btree_opintype,
                BTORDER_PROC,
            )?;
        }
        if cmp_proc == F_BTARRAYCMP && !array_element_has_compare(type_id)? {
            cmp_proc = INVALID_OID;
        } else if cmp_proc == F_BTRECORDCMP && !record_fields_have_compare(type_id)? {
            cmp_proc = INVALID_OID;
        }
        with_state(|st| {
            let e = st.entry_mut(type_id);
            if e.cmp_proc != cmp_proc {
                e.cmp_proc_finfo.fn_oid = INVALID_OID;
            }
            e.cmp_proc = cmp_proc;
            e.flags |= TCFLAGS_CHECKED_CMP_PROC;
        });
    }

    if needs(type_id, *flags, TYPECACHE_HASH_PROC | TYPECACHE_HASH_PROC_FINFO, TCFLAGS_CHECKED_HASH_PROC)
    {
        let mut hash_proc = resolve_hash_proc(type_id, HASHSTANDARD_PROC)?;
        if hash_proc == F_HASH_ARRAY && !array_element_has_hashing(type_id)? {
            hash_proc = INVALID_OID;
        } else if hash_proc == F_HASH_RECORD && !record_fields_have_hashing(type_id)? {
            hash_proc = INVALID_OID;
        } else if hash_proc == F_HASH_RANGE && !range_element_has_hashing(type_id)? {
            hash_proc = INVALID_OID;
        }
        if hash_proc == F_HASH_MULTIRANGE && !multirange_element_has_hashing(type_id)? {
            hash_proc = INVALID_OID;
        }
        with_state(|st| {
            let e = st.entry_mut(type_id);
            if e.hash_proc != hash_proc {
                e.hash_proc_finfo.fn_oid = INVALID_OID;
            }
            e.hash_proc = hash_proc;
            e.flags |= TCFLAGS_CHECKED_HASH_PROC;
        });
    }

    if needs(
        type_id,
        *flags,
        TYPECACHE_HASH_EXTENDED_PROC | TYPECACHE_HASH_EXTENDED_PROC_FINFO,
        TCFLAGS_CHECKED_HASH_EXTENDED_PROC,
    ) {
        let mut hash_extended_proc = resolve_hash_proc(type_id, HASHEXTENDED_PROC)?;
        if hash_extended_proc == F_HASH_ARRAY_EXTENDED
            && !array_element_has_extended_hashing(type_id)?
        {
            hash_extended_proc = INVALID_OID;
        } else if hash_extended_proc == F_HASH_RECORD_EXTENDED
            && !record_fields_have_extended_hashing(type_id)?
        {
            hash_extended_proc = INVALID_OID;
        } else if hash_extended_proc == F_HASH_RANGE_EXTENDED
            && !range_element_has_extended_hashing(type_id)?
        {
            hash_extended_proc = INVALID_OID;
        }
        if hash_extended_proc == F_HASH_MULTIRANGE_EXTENDED
            && !multirange_element_has_extended_hashing(type_id)?
        {
            hash_extended_proc = INVALID_OID;
        }
        with_state(|st| {
            let e = st.entry_mut(type_id);
            if e.hash_extended_proc != hash_extended_proc {
                e.hash_extended_proc_finfo.fn_oid = INVALID_OID;
            }
            e.hash_extended_proc = hash_extended_proc;
            e.flags |= TCFLAGS_CHECKED_HASH_EXTENDED_PROC;
        });
    }

    // Set up fmgr lookup info as requested.
    {
        let (want, present, opr) = with_state(|st| {
            let e = st.entry(type_id);
            ((*flags & TYPECACHE_EQ_OPR_FINFO) != 0, e.eq_opr_finfo.fn_oid != INVALID_OID, e.eq_opr)
        });
        if want && !present && opr != INVALID_OID {
            let eq_opr_func = lsyscache_seams::get_opcode::call(opr)?;
            if eq_opr_func != INVALID_OID {
                let finfo = fmgr_info_cxt(eq_opr_func)?;
                with_state(|st| st.entry_mut(type_id).eq_opr_finfo = finfo);
            }
        }
    }
    fmgr_finfo_if_needed(
        type_id,
        *flags,
        TYPECACHE_CMP_PROC_FINFO,
        |e| (e.cmp_proc_finfo.fn_oid, e.cmp_proc),
        |e, finfo| e.cmp_proc_finfo = finfo,
    )?;
    fmgr_finfo_if_needed(
        type_id,
        *flags,
        TYPECACHE_HASH_PROC_FINFO,
        |e| (e.hash_proc_finfo.fn_oid, e.hash_proc),
        |e, finfo| e.hash_proc_finfo = finfo,
    )?;
    fmgr_finfo_if_needed(
        type_id,
        *flags,
        TYPECACHE_HASH_EXTENDED_PROC_FINFO,
        |e| (e.hash_extended_proc_finfo.fn_oid, e.hash_extended_proc),
        |e, finfo| e.hash_extended_proc_finfo = finfo,
    )?;

    // If it's a composite type, get tupdesc if requested.
    {
        let (want, has_tupdesc, is_composite) = with_state(|st| {
            let e = st.entry(type_id);
            ((*flags & TYPECACHE_TUPDESC) != 0, e.tup_desc.is_some(), e.typtype == TYPTYPE_COMPOSITE)
        });
        if want && !has_tupdesc && is_composite {
            load_typcache_tupdesc(type_id)?;
        }
    }

    // Range type info.
    {
        let (want, is_range, has_elem, elem_oid) = with_state(|st| {
            let e = st.entry(type_id);
            (
                (*flags & TYPECACHE_RANGE_INFO) != 0,
                e.typtype == TYPTYPE_RANGE,
                e.rngelemtype.is_some(),
                e.rngelemtype,
            )
        });
        if want && is_range {
            if !has_elem {
                load_rangetype_info(type_id)?;
            } else if let Some(elem) = elem_oid {
                let need =
                    with_state(|st| (st.entry(elem).flags & TCFLAGS_HAVE_PG_TYPE_DATA) == 0);
                if need {
                    lookup_type_cache(elem, 0)?;
                }
            }
        }
    }

    // Multirange type info.
    {
        let (want, has_rngtype, is_multirange) = with_state(|st| {
            let e = st.entry(type_id);
            (
                (*flags & TYPECACHE_MULTIRANGE_INFO) != 0,
                e.rngtype.is_some(),
                e.typtype == TYPTYPE_MULTIRANGE,
            )
        });
        if want && !has_rngtype && is_multirange {
            load_multirangetype_info(type_id)?;
        }
    }

    // Domain base info.
    {
        let (want, no_base, is_domain) = with_state(|st| {
            let e = st.entry(type_id);
            (
                (*flags & TYPECACHE_DOMAIN_BASE_INFO) != 0,
                e.domain_base_type == INVALID_OID,
                e.typtype == TYPTYPE_DOMAIN,
            )
        });
        if want && no_base && is_domain {
            with_state(|st| st.entry_mut(type_id).domain_base_typmod = -1);
            let (base, typmod) = lsyscache_seams::get_base_type_and_typmod::call(type_id)?;
            with_state(|st| {
                let e = st.entry_mut(type_id);
                e.domain_base_type = base;
                e.domain_base_typmod = typmod;
            });
        }
    }

    {
        let (want, checked, is_domain) = with_state(|st| {
            let e = st.entry(type_id);
            (
                (*flags & TYPECACHE_DOMAIN_CONSTR_INFO) != 0,
                (e.flags & TCFLAGS_CHECKED_DOMAIN_CONSTRAINTS) != 0,
                e.typtype == TYPTYPE_DOMAIN,
            )
        });
        if want && !checked && is_domain {
            load_domaintype_info(type_id)?;
        }
    }

    Ok(())
}

/// Predicate: the requested-bits intersect `flags` and the `checked` bit is
/// not yet set on the entry.
fn needs(type_id: Oid, flags: i32, want: i32, checked: i32) -> bool {
    (flags & want) != 0 && with_state(|st| (st.entry(type_id).flags & checked) == 0)
}

/// The shared `get_opfamily_proc(hash_opf, ...)` body, including the `eq_opr`
/// agreement check.
fn resolve_hash_proc(type_id: Oid, procnum: i16) -> PgResult<Oid> {
    let (hash_opf, hash_opintype, eq_opr) =
        with_state(|st| {
            let e = st.entry(type_id);
            (e.hash_opf, e.hash_opintype, e.eq_opr)
        });
    if hash_opf == INVALID_OID {
        return Ok(INVALID_OID);
    }
    let eq_ok = if !oid_is_valid(eq_opr) {
        true
    } else {
        eq_opr
            == lsyscache_seams::get_opfamily_member::call(
                hash_opf,
                hash_opintype,
                hash_opintype,
                HT_EQUAL_STRATEGY_NUMBER,
            )?
    };
    if eq_ok {
        lsyscache_seams::get_opfamily_proc::call(hash_opf, hash_opintype, hash_opintype, procnum)
    } else {
        Ok(INVALID_OID)
    }
}

/// Resolve and cache an `FmgrInfo` when requested, unset, and the proc OID is
/// valid.
fn fmgr_finfo_if_needed(
    type_id: Oid,
    flags: i32,
    want_flag: i32,
    read: impl Fn(&TypeCacheEntry) -> (Oid, Oid),
    write: impl Fn(&mut TypeCacheEntry, FmgrInfo),
) -> PgResult<()> {
    let (fn_oid, proc) = with_state(|st| read(st.entry(type_id)));
    if (flags & want_flag) != 0 && fn_oid == INVALID_OID && proc != INVALID_OID {
        let resolved = fmgr_info_cxt(proc)?;
        with_state(|st| write(st.entry_mut(type_id), resolved));
    }
    Ok(())
}

/// `fmgr_info_cxt(func_oid, finfo, CacheMemoryContext)` — resolve the proc OID
/// (the lookup-failure half is the fmgr seam) and stamp the `FmgrInfo`. The
/// trimmed `FmgrInfo` here carries only `fn_oid`, so there is no pointer to
/// store.
fn fmgr_info_cxt(func_oid: Oid) -> PgResult<FmgrInfo> {
    fmgr_seams::fmgr_info_check::call(func_oid)?;
    Ok(FmgrInfo { fn_oid: func_oid, ..Default::default() })
}

/* ==========================================================================
 * load_typcache_tupdesc / load_rangetype_info / load_multirangetype_info
 * ======================================================================== */

/// `load_typcache_tupdesc` — set up a composite type's `tupDesc`.
fn load_typcache_tupdesc(type_id: Oid) -> PgResult<()> {
    let typrelid = with_state(|st| st.entry(type_id).typrelid);
    if !oid_is_valid(typrelid) {
        return elog_error(format!("invalid typrelid for composite type {type_id}"));
    }
    with_state(|st| -> PgResult<()> {
        // relation_open(AccessShareLock) + assert reltype + RelationGetDescr +
        // relation_close, copied into the cache context. The C bumps
        // tdrefcount; the safe port owns the copy.
        let tupdesc = relcache_seams::relation_get_composite_tupdesc::call(st.mcx, typrelid, type_id)?;
        st.tupledesc_id_counter += 1;
        let next_id = st.tupledesc_id_counter;
        let e = st.entry_mut(type_id);
        e.tup_desc = Some(tupdesc);
        e.tup_desc_identifier = next_id;
        Ok(())
    })
}

/// `load_rangetype_info` — set up range type information.
fn load_rangetype_info(type_id: Oid) -> PgResult<()> {
    let pg_range = lsyscache_seams::lookup_pg_range::call(type_id)?
        .ok_or(())
        .or_else(|_| elog_error(format!("cache lookup failed for range type {type_id}")))?;

    let subtype_oid = pg_range.rngsubtype;
    let opclass_oid = pg_range.rngsubopc;
    let canonical_oid = pg_range.rngcanonical;
    let subdiff_oid = pg_range.rngsubdiff;
    with_state(|st| st.entry_mut(type_id).rng_collation = pg_range.rngcollation);

    // Get opclass properties and look up the comparison function.
    let opfamily_oid = lsyscache_seams::get_opclass_family::call(opclass_oid)?;
    let opcintype = lsyscache_seams::get_opclass_input_type::call(opclass_oid)?;
    with_state(|st| st.entry_mut(type_id).rng_opfamily = opfamily_oid);

    let cmp_fn_oid =
        lsyscache_seams::get_opfamily_proc::call(opfamily_oid, opcintype, opcintype, BTORDER_PROC)?;
    if !reg_procedure_is_valid(cmp_fn_oid) {
        return elog_error(format!(
            "missing support function {BTORDER_PROC}({opcintype},{opcintype}) in opfamily {opfamily_oid}"
        ));
    }

    // Set up cached fmgrinfo structs.
    let finfo = fmgr_info_cxt(cmp_fn_oid)?;
    with_state(|st| st.entry_mut(type_id).rng_cmp_proc_finfo = finfo);
    if oid_is_valid(canonical_oid) {
        let finfo = fmgr_info_cxt(canonical_oid)?;
        with_state(|st| st.entry_mut(type_id).rng_canonical_finfo = finfo);
    }
    if oid_is_valid(subdiff_oid) {
        let finfo = fmgr_info_cxt(subdiff_oid)?;
        with_state(|st| st.entry_mut(type_id).rng_subdiff_finfo = finfo);
    }

    // Lastly, set up link to the element type --- this marks data valid.
    lookup_type_cache(subtype_oid, 0)?;
    with_state(|st| st.entry_mut(type_id).rngelemtype = Some(subtype_oid));
    Ok(())
}

/// `load_multirangetype_info` — set up multirange type information.
fn load_multirangetype_info(type_id: Oid) -> PgResult<()> {
    let rangetype_oid = lsyscache_seams::get_multirange_range::call(type_id)?;
    if !oid_is_valid(rangetype_oid) {
        return elog_error(format!("cache lookup failed for multirange type {type_id}"));
    }
    lookup_type_cache(rangetype_oid, TYPECACHE_RANGE_INFO)?;
    with_state(|st| st.entry_mut(type_id).rngtype = Some(rangetype_oid));
    Ok(())
}

/* ==========================================================================
 * load_domaintype_info + DomainConstraintCache refcounting
 * ======================================================================== */

/// `load_domaintype_info` — compile a domain's constraints. The whole
/// orchestration (domain-stack crawl, per-domain CHECK collection, name sort,
/// parent-first `lcons` ordering, NOT NULL prepend, lazy DomainConstraintCache
/// creation) lives here; only the genuinely-external callees — the
/// `pg_constraint`/`pg_type` catalog reads, `stringToNode`/`expression_planner`,
/// and the "Domain constraints" memory-context lifecycle — cross the domains
/// seam.
fn load_domaintype_info(type_id: Oid) -> PgResult<()> {
    // If we're here, any existing constraint info is stale, so release it.
    // For safety, be sure to null the link before trying to delete the data.
    let stale = with_state(|st| st.entry_mut(type_id).domain_data.take());
    if let Some(dcc) = stale {
        decr_dcc_refcount(&dcc)?;
    }

    // We try to optimize the common case of no domain constraints, so don't
    // create the dcc object and context until we find a constraint. The
    // accumulated constraint nodes and the (lazily created) context handle:
    let mut dcc: Option<(DomainCtxHandle, Vec<DomainConstraintState>)> = None;
    let mut not_null = false;

    // Scan pg_constraint for relevant constraints. We want to find constraints
    // for not just this domain, but any ancestor domains, so the outer loop
    // crawls up the domain stack.
    let mut type_oid = type_id;
    loop {
        // SearchSysCache1(TYPEOID, typeOid); elog(ERROR) if missing.
        let level = domains_seams::lookup_domain_type_level::call(type_oid)?;

        // Not a domain, so done.
        if !level.is_domain {
            break;
        }

        // Test for NOT NULL constraint.
        if level.typnotnull {
            not_null = true;
        }

        // Look for CHECK constraints on this domain (catalog scan; plan each in
        // the dcc context, which we create lazily on first constraint).
        let rows = domains_seams::scan_domain_check_constraints::call(type_oid)?;
        if !rows.is_empty() {
            // Create the DomainConstraintCache object and context if needed.
            if dcc.is_none() {
                let cxt = domains_seams::create_domain_ctx::call()?;
                dcc = Some((cxt, Vec::new()));
            }
            let ctx = dcc.as_ref().unwrap().0;

            // Plan each CHECK's Expr into ctx and build the
            // DomainConstraintState nodes (constrainttype + name are this
            // crate's data; check_expr is the planned handle from ctx).
            let mut nccons: Vec<DomainConstraintState> = Vec::new();
            for row in rows {
                let check_expr = domains_seams::plan_check_expr::call(&row.conbin, ctx)?;
                nccons.push(DomainConstraintState {
                    constrainttype: DOM_CONSTRAINT_CHECK,
                    name: row.conname,
                    check_expr,
                    check_exprstate: types_cache::typcache::ExprStateHandle::NULL,
                });
            }

            if !nccons.is_empty() {
                // Sort the items for this domain, so that CHECKs are applied in
                // a deterministic order (dcs_cmp == strcmp on name).
                if nccons.len() > 1 {
                    nccons.sort_by(|a, b| a.name.cmp(&b.name));
                }
                // Attach them to the overall list. Use lcons() semantics here
                // because constraints of parent domains should be applied
                // earlier: while (nccons > 0) constraints = lcons(ccons[--n], ...)
                // i.e. prepend in reverse, leaving this domain's checks in
                // ascending-name order at the front of the list.
                let list = &mut dcc.as_mut().unwrap().1;
                for node in nccons.into_iter().rev() {
                    list.insert(0, node);
                }
            }
        }

        // Loop to next domain in stack.
        type_oid = level.typbasetype;
    }

    // Only need to add one NOT NULL check regardless of how many domains in the
    // stack request it.
    if not_null {
        // Create the DomainConstraintCache object and context if needed.
        if dcc.is_none() {
            let cxt = domains_seams::create_domain_ctx::call()?;
            dcc = Some((cxt, Vec::new()));
        }
        let node = DomainConstraintState {
            constrainttype: DOM_CONSTRAINT_NOTNULL,
            name: "NOT NULL".to_string(),
            check_expr: types_cache::typcache::ExprHandle::NULL,
            check_exprstate: types_cache::typcache::ExprStateHandle::NULL,
        };
        // lcons to apply the nullness check FIRST.
        dcc.as_mut().unwrap().1.insert(0, node);
    }

    // If we made a constraint object, move it into CacheMemoryContext and
    // attach it to the typcache entry.
    if let Some((ctx, constraints)) = dcc {
        domains_seams::set_parent_to_cache_context::call(ctx)?;

        let dcc = Rc::new(DomainConstraintCache {
            constraints,
            dcc_context: ctx,
            dcc_refcount: Cell::new(1), // count the typcache's reference
        });
        with_state(|st| st.entry_mut(type_id).domain_data = Some(dcc));
    }

    // Either way, the typcache entry's domain data is now valid.
    with_state(|st| st.entry_mut(type_id).flags |= TCFLAGS_CHECKED_DOMAIN_CONSTRAINTS);
    Ok(())
}

/// `decr_dcc_refcount` — decrement and free when no references remain. The
/// `dcc` is shared via `Rc`; we decrement the explicit `dccRefCount` cell and,
/// when it reaches zero, delete the external "Domain constraints" memory
/// context (the C `MemoryContextDelete(dcc->dccContext)`). The `Rc`'s own
/// allocation is reclaimed when the caller's clone drops.
fn decr_dcc_refcount(dcc: &Rc<DomainConstraintCache>) -> PgResult<()> {
    let count = dcc.dcc_refcount.get();
    debug_assert!(count > 0);
    let count = count - 1;
    dcc.dcc_refcount.set(count);
    if count <= 0 {
        domains_seams::delete_domain_ctx::call(dcc.dcc_context)?;
    }
    Ok(())
}

/// `dccref_deletion_callback` — release a `DomainConstraintRef`'s refcount,
/// invoked through the reset callback registered on the ref's context.
pub fn release_domain_constraint_ref(ref_token: u64) {
    let dcc = with_state(|st| st.refs.get_mut(&ref_token).and_then(|r| r.dcc.take()));
    if let Some(dcc) = dcc {
        let _ = decr_dcc_refcount(&dcc);
    }
    with_state(|st| {
        st.refs.remove(&ref_token);
    });
}

/* ==========================================================================
 * Element/field property caching helpers.
 * ======================================================================== */

fn elem_property(
    type_id: Oid,
    cache: impl Fn(Oid) -> PgResult<()>,
    have: i32,
) -> PgResult<bool> {
    if with_state(|st| (st.entry(type_id).flags & TCFLAGS_CHECKED_ELEM_PROPERTIES) == 0) {
        cache(type_id)?;
    }
    Ok(with_state(|st| (st.entry(type_id).flags & have) != 0))
}

fn array_element_has_equality(type_id: Oid) -> PgResult<bool> {
    elem_property(type_id, cache_array_element_properties, TCFLAGS_HAVE_ELEM_EQUALITY)
}
fn array_element_has_compare(type_id: Oid) -> PgResult<bool> {
    elem_property(type_id, cache_array_element_properties, TCFLAGS_HAVE_ELEM_COMPARE)
}
fn array_element_has_hashing(type_id: Oid) -> PgResult<bool> {
    elem_property(type_id, cache_array_element_properties, TCFLAGS_HAVE_ELEM_HASHING)
}
fn array_element_has_extended_hashing(type_id: Oid) -> PgResult<bool> {
    elem_property(type_id, cache_array_element_properties, TCFLAGS_HAVE_ELEM_EXTENDED_HASHING)
}

fn cache_array_element_properties(type_id: Oid) -> PgResult<()> {
    let elem_type = lsyscache_seams::get_base_element_type::call(type_id)?;
    if oid_is_valid(elem_type) {
        lookup_type_cache(
            elem_type,
            TYPECACHE_EQ_OPR | TYPECACHE_CMP_PROC | TYPECACHE_HASH_PROC | TYPECACHE_HASH_EXTENDED_PROC,
        )?;
        with_state(|st| {
            let (eq, cmp, h, he) = {
                let el = st.entry(elem_type);
                (el.eq_opr, el.cmp_proc, el.hash_proc, el.hash_extended_proc)
            };
            let e = st.entry_mut(type_id);
            if oid_is_valid(eq) {
                e.flags |= TCFLAGS_HAVE_ELEM_EQUALITY;
            }
            if oid_is_valid(cmp) {
                e.flags |= TCFLAGS_HAVE_ELEM_COMPARE;
            }
            if oid_is_valid(h) {
                e.flags |= TCFLAGS_HAVE_ELEM_HASHING;
            }
            if oid_is_valid(he) {
                e.flags |= TCFLAGS_HAVE_ELEM_EXTENDED_HASHING;
            }
        });
    }
    with_state(|st| st.entry_mut(type_id).flags |= TCFLAGS_CHECKED_ELEM_PROPERTIES);
    Ok(())
}

fn field_property(type_id: Oid, have: i32) -> PgResult<bool> {
    if with_state(|st| (st.entry(type_id).flags & TCFLAGS_CHECKED_FIELD_PROPERTIES) == 0) {
        cache_record_field_properties(type_id)?;
    }
    Ok(with_state(|st| (st.entry(type_id).flags & have) != 0))
}

fn record_fields_have_equality(type_id: Oid) -> PgResult<bool> {
    field_property(type_id, TCFLAGS_HAVE_FIELD_EQUALITY)
}
fn record_fields_have_compare(type_id: Oid) -> PgResult<bool> {
    field_property(type_id, TCFLAGS_HAVE_FIELD_COMPARE)
}
fn record_fields_have_hashing(type_id: Oid) -> PgResult<bool> {
    field_property(type_id, TCFLAGS_HAVE_FIELD_HASHING)
}
fn record_fields_have_extended_hashing(type_id: Oid) -> PgResult<bool> {
    field_property(type_id, TCFLAGS_HAVE_FIELD_EXTENDED_HASHING)
}

fn cache_record_field_properties(type_id: Oid) -> PgResult<()> {
    let (typtype, has_tupdesc) =
        with_state(|st| (st.entry(type_id).typtype, st.entry(type_id).tup_desc.is_some()));

    if type_id == RECORDOID {
        // Can't tell; assume equality + comparison work.
        with_state(|st| {
            st.entry_mut(type_id).flags |= TCFLAGS_HAVE_FIELD_EQUALITY | TCFLAGS_HAVE_FIELD_COMPARE
        });
    } else if typtype == TYPTYPE_COMPOSITE {
        // Fetch composite type's tupdesc if we don't have it already.
        if !has_tupdesc {
            load_typcache_tupdesc(type_id)?;
        }
        // Collect the field type OIDs from the descriptor under one borrow (the
        // owned `Rc`-clone of C is the cache ownership; here the descriptor is
        // pinned in the cache context for the backend's life).
        let field_types: Vec<Oid> = with_state(|st| {
            let td = st.entry(type_id).tup_desc.as_ref().expect("composite has tupdesc after load");
            let natts = td.natts;
            let mut v = Vec::new();
            for i in 0..natts {
                let attr = td.attr(i as usize);
                if attr.attisdropped {
                    continue;
                }
                v.push(attr.atttypid);
            }
            v
        });

        let mut newflags = TCFLAGS_HAVE_FIELD_EQUALITY
            | TCFLAGS_HAVE_FIELD_COMPARE
            | TCFLAGS_HAVE_FIELD_HASHING
            | TCFLAGS_HAVE_FIELD_EXTENDED_HASHING;
        for atttypid in field_types {
            lookup_type_cache(
                atttypid,
                TYPECACHE_EQ_OPR
                    | TYPECACHE_CMP_PROC
                    | TYPECACHE_HASH_PROC
                    | TYPECACHE_HASH_EXTENDED_PROC,
            )?;
            let (eq, cmp, h, he) = with_state(|st| {
                let f = st.entry(atttypid);
                (f.eq_opr, f.cmp_proc, f.hash_proc, f.hash_extended_proc)
            });
            if !oid_is_valid(eq) {
                newflags &= !TCFLAGS_HAVE_FIELD_EQUALITY;
            }
            if !oid_is_valid(cmp) {
                newflags &= !TCFLAGS_HAVE_FIELD_COMPARE;
            }
            if !oid_is_valid(h) {
                newflags &= !TCFLAGS_HAVE_FIELD_HASHING;
            }
            if !oid_is_valid(he) {
                newflags &= !TCFLAGS_HAVE_FIELD_EXTENDED_HASHING;
            }
            if newflags == 0 {
                break;
            }
        }
        with_state(|st| st.entry_mut(type_id).flags |= newflags);
    } else if typtype == TYPTYPE_DOMAIN {
        // Domain over composite: copy base type's properties.
        let no_base = with_state(|st| st.entry(type_id).domain_base_type == INVALID_OID);
        if no_base {
            with_state(|st| st.entry_mut(type_id).domain_base_typmod = -1);
            let (base, typmod) = lsyscache_seams::get_base_type_and_typmod::call(type_id)?;
            with_state(|st| {
                let e = st.entry_mut(type_id);
                e.domain_base_type = base;
                e.domain_base_typmod = typmod;
            });
        }
        let domain_base_type = with_state(|st| st.entry(type_id).domain_base_type);
        lookup_type_cache(
            domain_base_type,
            TYPECACHE_EQ_OPR | TYPECACHE_CMP_PROC | TYPECACHE_HASH_PROC | TYPECACHE_HASH_EXTENDED_PROC,
        )?;
        with_state(|st| {
            let (base_is_composite, base_flags) = {
                let base = st.entry(domain_base_type);
                (base.typtype == TYPTYPE_COMPOSITE, base.flags)
            };
            if base_is_composite {
                let e = st.entry_mut(type_id);
                e.flags |= TCFLAGS_DOMAIN_BASE_IS_COMPOSITE;
                e.flags |= base_flags
                    & (TCFLAGS_HAVE_FIELD_EQUALITY
                        | TCFLAGS_HAVE_FIELD_COMPARE
                        | TCFLAGS_HAVE_FIELD_HASHING
                        | TCFLAGS_HAVE_FIELD_EXTENDED_HASHING);
            }
        });
    }
    with_state(|st| st.entry_mut(type_id).flags |= TCFLAGS_CHECKED_FIELD_PROPERTIES);
    Ok(())
}

fn range_element_has_hashing(type_id: Oid) -> PgResult<bool> {
    elem_property(type_id, cache_range_element_properties, TCFLAGS_HAVE_ELEM_HASHING)
}
fn range_element_has_extended_hashing(type_id: Oid) -> PgResult<bool> {
    elem_property(type_id, cache_range_element_properties, TCFLAGS_HAVE_ELEM_EXTENDED_HASHING)
}

fn cache_range_element_properties(type_id: Oid) -> PgResult<()> {
    let (no_elem, is_range) =
        with_state(|st| (st.entry(type_id).rngelemtype.is_none(), st.entry(type_id).typtype == TYPTYPE_RANGE));
    if no_elem && is_range {
        load_rangetype_info(type_id)?;
    }
    let elem_oid = with_state(|st| st.entry(type_id).rngelemtype);
    if let Some(elem) = elem_oid {
        lookup_type_cache(elem, TYPECACHE_HASH_PROC | TYPECACHE_HASH_EXTENDED_PROC)?;
        with_state(|st| {
            let (h, he) = {
                let el = st.entry(elem);
                (el.hash_proc, el.hash_extended_proc)
            };
            let e = st.entry_mut(type_id);
            if oid_is_valid(h) {
                e.flags |= TCFLAGS_HAVE_ELEM_HASHING;
            }
            if oid_is_valid(he) {
                e.flags |= TCFLAGS_HAVE_ELEM_EXTENDED_HASHING;
            }
        });
    }
    with_state(|st| st.entry_mut(type_id).flags |= TCFLAGS_CHECKED_ELEM_PROPERTIES);
    Ok(())
}

fn multirange_element_has_hashing(type_id: Oid) -> PgResult<bool> {
    elem_property(type_id, cache_multirange_element_properties, TCFLAGS_HAVE_ELEM_HASHING)
}
fn multirange_element_has_extended_hashing(type_id: Oid) -> PgResult<bool> {
    elem_property(type_id, cache_multirange_element_properties, TCFLAGS_HAVE_ELEM_EXTENDED_HASHING)
}

fn cache_multirange_element_properties(type_id: Oid) -> PgResult<()> {
    let (no_rngtype, is_multirange) = with_state(|st| {
        (st.entry(type_id).rngtype.is_none(), st.entry(type_id).typtype == TYPTYPE_MULTIRANGE)
    });
    if no_rngtype && is_multirange {
        load_multirangetype_info(type_id)?;
    }
    let rngtype = with_state(|st| st.entry(type_id).rngtype);
    if let Some(rng) = rngtype {
        let rngelemtype = with_state(|st| st.entry(rng).rngelemtype);
        if let Some(elem) = rngelemtype {
            lookup_type_cache(elem, TYPECACHE_HASH_PROC | TYPECACHE_HASH_EXTENDED_PROC)?;
            with_state(|st| {
                let (h, he) = {
                    let el = st.entry(elem);
                    (el.hash_proc, el.hash_extended_proc)
                };
                let e = st.entry_mut(type_id);
                if oid_is_valid(h) {
                    e.flags |= TCFLAGS_HAVE_ELEM_HASHING;
                }
                if oid_is_valid(he) {
                    e.flags |= TCFLAGS_HAVE_ELEM_EXTENDED_HASHING;
                }
            });
        }
    }
    with_state(|st| st.entry_mut(type_id).flags |= TCFLAGS_CHECKED_ELEM_PROPERTIES);
    Ok(())
}

/* ==========================================================================
 * RecordCache array + lookups.
 * ======================================================================== */

/// `ensure_record_cache_typmod_slot_exists`.
fn ensure_record_cache_typmod_slot_exists(st: &mut TypCacheState<'_>, typmod: i32) -> PgResult<()> {
    if st.record_cache_array.is_empty() {
        grow_record_cache_array(st, 64)?;
    }
    if typmod >= st.record_cache_array.len() as i32 {
        debug_assert!(typmod >= 0);
        let newlen = pg_nextpower2_32((typmod as u32).wrapping_add(1)) as usize;
        grow_record_cache_array(st, newlen)?;
    }
    Ok(())
}

/// Grow `record_cache_array` to `newlen` slots (the `repalloc0_array` analog —
/// new slots zero-filled).
fn grow_record_cache_array(st: &mut TypCacheState<'_>, newlen: usize) -> PgResult<()> {
    if newlen <= st.record_cache_array.len() {
        return Ok(());
    }
    if newlen > MAX_ALLOC_SIZE / core::mem::size_of::<RecordCacheArrayEntry>() {
        return Err(PgError::error("record cache array too large")
            .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED));
    }
    let grow = newlen - st.record_cache_array.len();
    st.record_cache_array
        .try_reserve(grow)
        .map_err(|_| st.mcx.oom(newlen * core::mem::size_of::<RecordCacheArrayEntry>()))?;
    while st.record_cache_array.len() < newlen {
        st.record_cache_array.push(RecordCacheArrayEntry::default());
    }
    Ok(())
}

/// Copy a cache-resident descriptor into the caller's `mcx`.
fn copy_tupdesc_out<'mcx>(
    mcx: Mcx<'mcx>,
    src: &TupleDescData<'_>,
) -> PgResult<PgBox<'mcx, TupleDescData<'mcx>>> {
    mcx::alloc_in(mcx, src.clone_in(mcx)?)
}

/// `lookup_rowtype_tupdesc_internal` core, returning the descriptor copied into
/// `out_mcx`. The C "pin without bumping" discipline collapses under ownership:
/// the caller receives an owned copy (dropping it is the matching release).
fn lookup_rowtype_tupdesc_internal<'mcx>(
    out_mcx: Mcx<'mcx>,
    type_id: Oid,
    typmod: i32,
    no_error: bool,
) -> PgResult<Option<PgBox<'mcx, TupleDescData<'mcx>>>> {
    if type_id != RECORDOID {
        // Named composite type: use the regular typcache.
        lookup_type_cache(type_id, TYPECACHE_TUPDESC)?;
        let copied = with_state(|st| -> PgResult<Option<PgBox<'mcx, TupleDescData<'mcx>>>> {
            match st.entry(type_id).tup_desc.as_ref() {
                Some(td) => Ok(Some(copy_tupdesc_out(out_mcx, td)?)),
                None => Ok(None),
            }
        })?;
        if copied.is_none() && !no_error {
            return ereport_error(
                ERRCODE_WRONG_OBJECT_TYPE,
                format!("type {} is not composite", format_type(type_id)?),
            );
        }
        Ok(copied)
    } else {
        // Transient record type: look in our record-type table.
        if typmod >= 0 {
            // Already in our local cache?
            let cached = with_state(|st| -> PgResult<Option<PgBox<'mcx, TupleDescData<'mcx>>>> {
                if (typmod as usize) < st.record_cache_array.len() {
                    if let Some(td) = &st.record_cache_array[typmod as usize].tupdesc {
                        return Ok(Some(copy_tupdesc_out(out_mcx, td)?));
                    }
                }
                Ok(None)
            })?;
            if let Some(td) = cached {
                return Ok(Some(td));
            }

            // Attached to a shared record typmod registry?
            if session_seams::shared_registry_attached::call() {
                let scratch = MemoryContext::new("typcache shared tupdesc");
                let found = session_seams::shared_typmod_table_find::call(scratch.mcx(), typmod)?;
                if let Some(found) = found {
                    debug_assert_eq!(found.tdrefcount, -1);
                    // Store an owned copy in the cache, assign a local id,
                    // return a copy into out_mcx.
                    let out = with_state(|st| -> PgResult<PgBox<'mcx, TupleDescData<'mcx>>> {
                        ensure_record_cache_typmod_slot_exists(st, typmod)?;
                        let stored = copy_tupdesc_into_cache(st, &found)?;
                        let out = copy_tupdesc_out(out_mcx, &stored)?;
                        st.record_cache_array[typmod as usize].tupdesc = Some(stored);
                        st.tupledesc_id_counter += 1;
                        st.record_cache_array[typmod as usize].id = st.tupledesc_id_counter;
                        Ok(out)
                    })?;
                    return Ok(Some(out));
                }
            }
        }

        if !no_error {
            return ereport_error(
                ERRCODE_WRONG_OBJECT_TYPE,
                "record type has not been registered".to_string(),
            );
        }
        Ok(None)
    }
}

/// Copy `src` into the cache's own context.
fn copy_tupdesc_into_cache<'mcx>(
    st: &TypCacheState<'mcx>,
    src: &TupleDescData<'_>,
) -> PgResult<PgBox<'mcx, TupleDescData<'mcx>>> {
    mcx::alloc_in(st.mcx, src.clone_in(st.mcx)?)
}

/// `lookup_rowtype_tupdesc`. The returned descriptor is an owned copy in
/// `mcx`; dropping it is the matching `ReleaseTupleDesc`.
pub fn lookup_rowtype_tupdesc<'mcx>(
    mcx: Mcx<'mcx>,
    type_id: Oid,
    typmod: i32,
) -> PgResult<PgBox<'mcx, TupleDescData<'mcx>>> {
    let td = lookup_rowtype_tupdesc_internal(mcx, type_id, typmod, false)?;
    td.ok_or(())
        .or_else(|_| Err(PgError::error("lookup_rowtype_tupdesc: internal returned None on no_error=false")))
}

/// `lookup_rowtype_tupdesc_noerror`.
pub fn lookup_rowtype_tupdesc_noerror<'mcx>(
    mcx: Mcx<'mcx>,
    type_id: Oid,
    typmod: i32,
    no_error: bool,
) -> PgResult<Option<PgBox<'mcx, TupleDescData<'mcx>>>> {
    lookup_rowtype_tupdesc_internal(mcx, type_id, typmod, no_error)
}

/// `lookup_rowtype_tupdesc_copy` — a fully owned constraint-preserving copy
/// (`CreateTupleDescCopyConstr`: refcount `-1`).
pub fn lookup_rowtype_tupdesc_copy<'mcx>(
    mcx: Mcx<'mcx>,
    type_id: Oid,
    typmod: i32,
) -> PgResult<PgBox<'mcx, TupleDescData<'mcx>>> {
    let tmp = lookup_rowtype_tupdesc_internal(mcx, type_id, typmod, false)?
        .ok_or(())
        .or_else(|_| Err(PgError::error("lookup_rowtype_tupdesc_copy: internal returned None")))?;
    // CreateTupleDescCopyConstr is `clone_in` + reset refcount to -1.
    let mut copy = tmp;
    copy.tdrefcount = -1;
    Ok(copy)
}

/// `lookup_rowtype_tupdesc_domain`.
pub fn lookup_rowtype_tupdesc_domain<'mcx>(
    mcx: Mcx<'mcx>,
    type_id: Oid,
    typmod: i32,
    no_error: bool,
) -> PgResult<Option<PgBox<'mcx, TupleDescData<'mcx>>>> {
    if type_id != RECORDOID {
        lookup_type_cache(type_id, TYPECACHE_TUPDESC | TYPECACHE_DOMAIN_BASE_INFO)?;
        let (is_domain, base, base_typmod, has_td) = with_state(|st| {
            let e = st.entry(type_id);
            (e.typtype == TYPTYPE_DOMAIN, e.domain_base_type, e.domain_base_typmod, e.tup_desc.is_some())
        });
        if is_domain {
            return lookup_rowtype_tupdesc_noerror(mcx, base, base_typmod, no_error);
        }
        if !has_td {
            if !no_error {
                return ereport_error(
                    ERRCODE_WRONG_OBJECT_TYPE,
                    format!("type {} is not composite", format_type(type_id)?),
                );
            }
            return Ok(None);
        }
        let out = with_state(|st| -> PgResult<PgBox<'mcx, TupleDescData<'mcx>>> {
            let td = st.entry(type_id).tup_desc.as_ref().expect("has td");
            copy_tupdesc_out(mcx, td)
        })?;
        Ok(Some(out))
    } else {
        lookup_rowtype_tupdesc_internal(mcx, type_id, typmod, no_error)
    }
}

/// `assign_record_type_typmod` — assign/look up the typmod for a record
/// tupdesc, writing the assigned typmod into `tup_desc->tdtypmod`.
pub fn assign_record_type_typmod(tup_desc: &mut TupleDescData<'_>) -> PgResult<()> {
    debug_assert_eq!(tup_desc.tdtypeid, RECORDOID);

    // Find a hashtable entry for this tuple descriptor.
    if let Some(existing_typmod) = record_cache_find(tup_desc)? {
        tup_desc.tdtypmod = existing_typmod;
        return Ok(());
    }

    // Not present: manufacture an entry. Look in the SharedRecordTypmodRegistry
    // first, if attached.
    let scratch = MemoryContext::new("typcache assign typmod");
    let shared =
        session_seams::find_or_make_matching_shared_tupledesc::call(scratch.mcx(), tup_desc)?;

    let assigned_typmod = match shared {
        None => {
            // Reference-counted local cache only.
            let next = with_state(|st| -> PgResult<i32> {
                ensure_record_cache_typmod_slot_exists(st, st.next_record_typmod)?;
                Ok(st.next_record_typmod)
            })?;
            // CreateTupleDescCopy (no constraints) via the tupdesc owner.
            let mut ed = tupdesc_seams::create_tupledesc_copy::call(scratch.mcx(), tup_desc)?;
            ed.tdrefcount = 1;
            ed.tdtypmod = next;
            with_state(|st| -> PgResult<()> {
                let stored = copy_tupdesc_into_cache(st, &ed)?;
                st.record_cache_array[next as usize].tupdesc = Some(stored);
                st.tupledesc_id_counter += 1;
                st.record_cache_array[next as usize].id = st.tupledesc_id_counter;
                st.next_record_typmod += 1;
                Ok(())
            })?;
            next
        }
        Some(ed) => {
            let tm = ed.tdtypmod;
            with_state(|st| -> PgResult<()> {
                ensure_record_cache_typmod_slot_exists(st, tm)?;
                let stored = copy_tupdesc_into_cache(st, &ed)?;
                st.record_cache_array[tm as usize].tupdesc = Some(stored);
                st.tupledesc_id_counter += 1;
                st.record_cache_array[tm as usize].id = st.tupledesc_id_counter;
                Ok(())
            })?;
            tm
        }
    };

    // Fully initialized; create the hash table entry mapping the structural row
    // type to the stored descriptor at `assigned_typmod`.
    record_cache_insert(tup_desc, assigned_typmod)?;

    tup_desc.tdtypmod = assigned_typmod;
    Ok(())
}

/// Find the typmod whose stored descriptor structurally matches `tup_desc`.
fn record_cache_find(tup_desc: &TupleDescData<'_>) -> PgResult<Option<i32>> {
    let hash = tupdesc_seams::hash_row_type::call(tup_desc);
    // Snapshot the candidate descriptor ids for the bucket; compare each
    // outside the borrow (the seam may itself touch the cache).
    let ids: Vec<u64> = with_state(|st| st.record_cache.get(&hash).map(|b| b.to_vec()).unwrap_or_default());
    for id in ids {
        let matched = with_state(|st| {
            st.record_cache_array
                .iter()
                .find(|slot| slot.id == id)
                .map(|slot| {
                    slot.tupdesc
                        .as_ref()
                        .map(|td| (td.tdtypmod, tupdesc_seams::equal_row_types::call(tup_desc, td)))
                })
        });
        if let Some(Some((typmod, eq))) = matched {
            if eq {
                return Ok(Some(typmod));
            }
        }
    }
    Ok(None)
}

/// Insert (or overwrite) a `RecordCacheEntry` mapping the structural row type of
/// `key_desc` to the stored descriptor id at `typmod`.
fn record_cache_insert(key_desc: &TupleDescData<'_>, typmod: i32) -> PgResult<()> {
    let hash = tupdesc_seams::hash_row_type::call(key_desc);
    let new_id = with_state(|st| st.record_cache_array[typmod as usize].id);
    // Find an existing structural match in the bucket to overwrite.
    let ids: Vec<u64> = with_state(|st| st.record_cache.get(&hash).map(|b| b.to_vec()).unwrap_or_default());
    let mut replace_idx: Option<usize> = None;
    for (idx, id) in ids.iter().enumerate() {
        let eq = with_state(|st| {
            st.record_cache_array
                .iter()
                .find(|slot| slot.id == *id)
                .and_then(|slot| slot.tupdesc.as_ref().map(|td| tupdesc_seams::equal_row_types::call(key_desc, td)))
                .unwrap_or(false)
        });
        if eq {
            replace_idx = Some(idx);
            break;
        }
    }
    with_state(|st| -> PgResult<()> {
        let bucket = st.record_cache.entry(hash).or_insert_with(|| PgVec::new_in(st.mcx));
        match replace_idx {
            Some(idx) => bucket[idx] = new_id,
            None => {
                bucket
                    .try_reserve(1)
                    .map_err(|_| st.mcx.oom(core::mem::size_of::<u64>()))?;
                bucket.push(new_id);
            }
        }
        Ok(())
    })
}

/// `assign_record_type_identifier`.
pub fn assign_record_type_identifier(type_id: Oid, typmod: i32) -> PgResult<u64> {
    if type_id != RECORDOID {
        lookup_type_cache(type_id, TYPECACHE_TUPDESC)?;
        let (td_null, id) =
            with_state(|st| (st.entry(type_id).tup_desc.is_none(), st.entry(type_id).tup_desc_identifier));
        if td_null {
            return ereport_error(
                ERRCODE_WRONG_OBJECT_TYPE,
                format!("type {} is not composite", format_type(type_id)?),
            );
        }
        debug_assert_ne!(id, 0);
        Ok(id)
    } else {
        let cached = with_state(|st| {
            if typmod >= 0
                && (typmod as usize) < st.record_cache_array.len()
                && st.record_cache_array[typmod as usize].tupdesc.is_some()
            {
                debug_assert_ne!(st.record_cache_array[typmod as usize].id, 0);
                Some(st.record_cache_array[typmod as usize].id)
            } else {
                None
            }
        });
        if let Some(id) = cached {
            return Ok(id);
        }
        Ok(with_state(|st| {
            st.tupledesc_id_counter += 1;
            st.tupledesc_id_counter
        }))
    }
}

/// Read the composite-type view (`tupDesc` clone + `tupDesc_identifier`) of an
/// already-resolved cache entry. `tup_desc` is `None` when the entry has no
/// tupdesc (the type is not composite). Mirrors the C `typentry->tupDesc` /
/// `typentry->tupDesc_identifier` reads (cache descriptors are refcounted).
fn read_tupdesc_view<'mcx>(
    mcx: Mcx<'mcx>,
    type_id: Oid,
) -> PgResult<(Option<PgBox<'mcx, TupleDescData<'mcx>>>, u64)> {
    with_state(|st| -> PgResult<(Option<PgBox<'mcx, TupleDescData<'mcx>>>, u64)> {
        let e = st.entry(type_id);
        let id = e.tup_desc_identifier;
        match e.tup_desc.as_ref() {
            Some(td) => Ok((Some(copy_tupdesc_out(mcx, td)?), id)),
            None => Ok((None, id)),
        }
    })
}

/// `make_expanded_record_from_typeid`'s typcache resolution (expandedrecord.c:84):
/// `lookup_type_cache(type_id, TYPECACHE_TUPDESC | TYPECACHE_DOMAIN_BASE_INFO)`,
/// then if the result is a domain, the chained `lookup_type_cache(domainBaseType,
/// TYPECACHE_TUPDESC)`. Returns the typtype of the original type, the domain base
/// OID, and the composite descriptor / identifier of whichever entry carries the
/// tupdesc.
fn lookup_type_cache_expanded_record<'mcx>(
    mcx: Mcx<'mcx>,
    type_id: Oid,
) -> PgResult<backend_utils_cache_typcache_seams::ExpandedRecordTypeCacheView<'mcx>> {
    lookup_type_cache(type_id, TYPECACHE_TUPDESC | TYPECACHE_DOMAIN_BASE_INFO)?;
    let (typtype, domain_base_type) =
        with_state(|st| (st.entry(type_id).typtype, st.entry(type_id).domain_base_type));
    // The entry whose tupDesc we read: for a domain over composite it's the
    // domain's base type (re-looked-up with TYPECACHE_TUPDESC).
    let td_type_id = if typtype == TYPTYPE_DOMAIN {
        lookup_type_cache(domain_base_type, TYPECACHE_TUPDESC)?;
        domain_base_type
    } else {
        type_id
    };
    let (tup_desc, tup_desc_identifier) = read_tupdesc_view(mcx, td_type_id)?;
    Ok(backend_utils_cache_typcache_seams::ExpandedRecordTypeCacheView {
        typtype,
        domain_base_type,
        // typcache composite descriptors are refcounted in C (tdrefcount >= 0).
        tup_desc_refcounted: tup_desc.is_some(),
        tup_desc,
        tup_desc_identifier,
    })
}

/// `make_expanded_record_from_tupdesc`'s named-composite typcache resolution
/// (expandedrecord.c:226): `lookup_type_cache(type_id, TYPECACHE_TUPDESC)`, then
/// read `tupDesc` / `tupDesc_identifier`.
fn lookup_type_cache_tupdesc_view<'mcx>(
    mcx: Mcx<'mcx>,
    type_id: Oid,
) -> PgResult<backend_utils_cache_typcache_seams::ExpandedRecordTypeCacheView<'mcx>> {
    lookup_type_cache(type_id, TYPECACHE_TUPDESC)?;
    let typtype = with_state(|st| st.entry(type_id).typtype);
    let (tup_desc, tup_desc_identifier) = read_tupdesc_view(mcx, type_id)?;
    Ok(backend_utils_cache_typcache_seams::ExpandedRecordTypeCacheView {
        typtype,
        domain_base_type: INVALID_OID,
        tup_desc_refcounted: tup_desc.is_some(),
        tup_desc,
        tup_desc_identifier,
    })
}

/* ==========================================================================
 * Shared record typmod registry (DSA) — body via seam.
 * ======================================================================== */

/// `SharedRecordTypmodRegistryEstimate`.
pub fn shared_record_typmod_registry_estimate() -> usize {
    session_seams::shared_registry_estimate::call()
}

/// `SharedRecordTypmodRegistryInit`.
pub fn shared_record_typmod_registry_init() -> PgResult<()> {
    with_state(|st| -> PgResult<()> {
        let next = st.next_record_typmod;
        // Borrow each present descriptor; the owner copies into the DSA area
        // (share_tupledesc).
        let mut entries: Vec<(i32, &TupleDescData<'_>)> = Vec::new();
        for typmod in 0..next {
            if let Some(td) = &st.record_cache_array[typmod as usize].tupdesc {
                entries.push((typmod, td));
            }
        }
        session_seams::shared_registry_init::call(next, &entries)
    })
}

/// `SharedRecordTypmodRegistryAttach`.
pub fn shared_record_typmod_registry_attach() -> PgResult<()> {
    debug_assert_eq!(with_state(|st| st.next_record_typmod), 0);
    session_seams::shared_registry_attach::call()
}

/* ==========================================================================
 * Invalidation callbacks.
 * ======================================================================== */

/// `InvalidateCompositeTypeCacheEntry`.
fn invalidate_composite_type_cache_entry(type_id: Oid) {
    let (had_tupdesc, had_opclass) = with_state(|st| {
        let e = st.entry(type_id);
        debug_assert!(e.typtype == TYPTYPE_COMPOSITE && oid_is_valid(e.typrelid));
        (e.tup_desc.is_some(), (e.flags & TCFLAGS_OPERATOR_FLAGS) != 0)
    });

    let had_tupdesc_or_opclass = had_tupdesc || had_opclass;

    // Delete tupdesc if we have it (the C DecrTupleDescRefCount-and-maybe-free
    // is the owned drop — drop the cache-owned descriptor; any external copies
    // callers hold are independent owned copies).
    if had_tupdesc {
        with_state(|st| {
            let e = st.entry_mut(type_id);
            e.tup_desc = None;
            e.tup_desc_identifier = 0;
        });
    }

    // Reset equality/comparison/hashing validity information.
    with_state(|st| st.entry_mut(type_id).flags &= !TCFLAGS_OPERATOR_FLAGS);

    if had_tupdesc_or_opclass {
        delete_rel_type_cache_if_needed(type_id);
    }
}

/// `TypeCacheRelCallback` — relcache invalidation hook.
fn type_cache_rel_callback(_arg: ScalarWord, relid: Oid) {
    if oid_is_valid(relid) {
        // Find a RelIdToTypeIdCacheHash entry.
        let composite_typid = with_state(|st| st.rel_id_to_type_id.get(&relid).copied());
        if let Some(comp) = composite_typid {
            let present = with_state(|st| {
                st.type_cache.get(&comp).map(|e| {
                    debug_assert!(e.typtype == TYPTYPE_COMPOSITE);
                    debug_assert_eq!(relid, e.typrelid);
                })
            });
            if present.is_some() {
                invalidate_composite_type_cache_entry(comp);
            }
        }

        // Visit all domain types sequentially via the next_domain chain.
        let mut typentry = with_state(|st| st.first_domain_type_entry);
        while let Some(oid) = typentry {
            with_state(|st| {
                let e = st.entry_mut(oid);
                if (e.flags & TCFLAGS_DOMAIN_BASE_IS_COMPOSITE) != 0 {
                    e.flags &= !TCFLAGS_OPERATOR_FLAGS;
                }
            });
            typentry = with_state(|st| st.entry(oid).next_domain);
        }
    } else {
        // relid invalid: reset all composite types + domain flags.
        let all: Vec<Oid> = with_state(|st| st.type_cache.keys().copied().collect());
        for oid in all {
            let typtype = with_state(|st| st.entry(oid).typtype);
            if typtype == TYPTYPE_COMPOSITE {
                invalidate_composite_type_cache_entry(oid);
            } else if typtype == TYPTYPE_DOMAIN {
                with_state(|st| {
                    let e = st.entry_mut(oid);
                    if (e.flags & TCFLAGS_DOMAIN_BASE_IS_COMPOSITE) != 0 {
                        e.flags &= !TCFLAGS_OPERATOR_FLAGS;
                    }
                });
            }
        }
    }
}

/// `TypeCacheTypCallback` — pg_type syscache invalidation hook.
fn type_cache_typ_callback(_arg: ScalarWord, _cacheid: i32, hashvalue: u32) {
    let entries: Vec<Oid> = with_state(|st| {
        st.type_cache
            .values()
            .filter(|e| hashvalue == 0 || e.type_id_hash == hashvalue)
            .map(|e| e.type_id)
            .collect()
    });
    for oid in entries {
        let had_pg_type_data = with_state(|st| {
            let e = st.entry_mut(oid);
            debug_assert!(hashvalue == 0 || e.type_id_hash == hashvalue);
            let had = (e.flags & TCFLAGS_HAVE_PG_TYPE_DATA) != 0;
            e.flags &= !(TCFLAGS_HAVE_PG_TYPE_DATA | TCFLAGS_CHECKED_DOMAIN_CONSTRAINTS);
            had
        });
        if had_pg_type_data {
            delete_rel_type_cache_if_needed(oid);
        }
    }
}

/// `TypeCacheOpcCallback` — pg_opclass syscache invalidation hook.
fn type_cache_opc_callback(_arg: ScalarWord, _cacheid: i32, _hashvalue: u32) {
    let entries: Vec<Oid> = with_state(|st| st.type_cache.keys().copied().collect());
    for oid in entries {
        let had_opclass = with_state(|st| {
            let e = st.entry_mut(oid);
            let had = (e.flags & TCFLAGS_OPERATOR_FLAGS) != 0;
            e.flags &= !TCFLAGS_OPERATOR_FLAGS;
            had
        });
        if had_opclass {
            delete_rel_type_cache_if_needed(oid);
        }
    }
}

/// `TypeCacheConstrCallback` — pg_constraint syscache invalidation hook.
fn type_cache_constr_callback(_arg: ScalarWord, _cacheid: i32, _hashvalue: u32) {
    let mut typentry = with_state(|st| st.first_domain_type_entry);
    while let Some(oid) = typentry {
        with_state(|st| st.entry_mut(oid).flags &= !TCFLAGS_CHECKED_DOMAIN_CONSTRAINTS);
        typentry = with_state(|st| st.entry(oid).next_domain);
    }
}

/* ==========================================================================
 * Enum sort cache.
 * ======================================================================== */

/// `enum_known_sorted`.
fn enum_known_sorted(enumdata: &TypeCacheEnumData<'_>, arg: Oid) -> bool {
    if arg < enumdata.bitmap_base {
        return false;
    }
    let offset = arg - enumdata.bitmap_base;
    if offset > i32::MAX as Oid {
        return false;
    }
    enumdata.sorted_values.is_member(offset as i32)
}

/// `compare_values_of_enum`.
pub fn compare_values_of_enum(type_id: Oid, arg1: Oid, arg2: Oid) -> PgResult<i32> {
    if arg1 == arg2 {
        return Ok(0);
    }

    // Load up the cache if first time through. Entry must be in the cache (the
    // C caller passes a `TypeCacheEntry *`; here the caller passes its OID).
    if with_state(|st| st.entry(type_id).enum_data.is_none()) {
        load_enum_cache_data(type_id)?;
    }

    // Fast path: both known-sorted → compare OIDs directly.
    let known = with_enumdata(type_id, |ed| enum_known_sorted(ed, arg1) && enum_known_sorted(ed, arg2));
    if known {
        return Ok(if arg1 < arg2 { -1 } else { 1 });
    }

    // Slow path: identify actual sort-order positions.
    let (mut o1, mut o2) = with_enumdata(type_id, |ed| {
        (find_enumitem(ed, arg1).map(|i| i.sort_order), find_enumitem(ed, arg2).map(|i| i.sort_order))
    });

    if o1.is_none() || o2.is_none() {
        // Enum changed under us: reload and retry.
        load_enum_cache_data(type_id)?;
        let (n1, n2) = with_enumdata(type_id, |ed| {
            (find_enumitem(ed, arg1).map(|i| i.sort_order), find_enumitem(ed, arg2).map(|i| i.sort_order))
        });
        o1 = n1;
        o2 = n2;
        if o1.is_none() {
            return elog_error(format!(
                "enum value {arg1} not found in cache for enum {}",
                format_type(type_id)?
            ));
        }
        if o2.is_none() {
            return elog_error(format!(
                "enum value {arg2} not found in cache for enum {}",
                format_type(type_id)?
            ));
        }
    }

    let s1 = o1.expect("sort order for arg1 present");
    let s2 = o2.expect("sort order for arg2 present");
    Ok(if s1 < s2 {
        -1
    } else if s1 > s2 {
        1
    } else {
        0
    })
}

/// Run `f` with a borrow of the entry's `TypeCacheEnumData`.
fn with_enumdata<R>(type_id: Oid, f: impl FnOnce(&TypeCacheEnumData<'_>) -> R) -> R {
    with_state(|st| {
        let ed = st
            .entry(type_id)
            .enum_data
            .as_ref()
            .expect("enum data must exist after load");
        f(ed)
    })
}

/// `load_enum_cache_data` — build (or rebuild) the enum sort table.
fn load_enum_cache_data(type_id: Oid) -> PgResult<()> {
    let typtype = with_state(|st| st.entry(type_id).typtype);
    if typtype != TYPTYPE_ENUM {
        return ereport_error(ERRCODE_WRONG_OBJECT_TYPE, format!("{} is not an enum", format_type(type_id)?));
    }

    // Read all members of the enum type into a working vector (the
    // CurrentMemoryContext-resident build buffer in C; only the finished
    // enumdata is copied into the cache context).
    let mut items: Vec<EnumItem> = Vec::new();
    let mut push_err: Option<PgError> = None;
    pg_enum_seams::scan_enum_members::call(type_id, &mut |enum_oid, sort_order| {
        if push_err.is_some() {
            return;
        }
        if items.try_reserve(1).is_err() {
            push_err = Some(PgError::error("out of memory").with_sqlstate(ERRCODE_OUT_OF_MEMORY));
            return;
        }
        items.push(EnumItem { enum_oid, sort_order });
    })?;
    if let Some(e) = push_err {
        return Err(e);
    }
    let numitems = items.len() as i32;

    // Sort into OID order (enum_oid_cmp == pg_cmp_u32).
    items.sort_by(|a, b| a.enum_oid.cmp(&b.enum_oid));

    // Build the bitmap and the finished, cache-context-charged enumdata, then
    // link it in (replacing any prior enumdata in place).
    with_state(|st| -> PgResult<()> {
        let mcx = st.mcx;
        // Build a bitmap of a subset of OIDs known to be in order.
        let mut bitmap_base = INVALID_OID;
        let mut bitmap: Option<Bitmapset> = None;
        let mut bm_size = 1; // only save sets of at least 2 OIDs

        let n = numitems as usize;
        for start_pos in 0..n.saturating_sub(1) {
            let mut this_bitmap = Bitmapset::make_singleton(mcx, 0)?;
            let mut this_bm_size = 1;
            let start_oid = items[start_pos].enum_oid;
            let mut prev_order = items[start_pos].sort_order;

            for item in &items[(start_pos + 1)..n] {
                let offset = item.enum_oid.wrapping_sub(start_oid);
                if offset >= 8192 {
                    break;
                }
                if item.sort_order > prev_order {
                    prev_order = item.sort_order;
                    this_bitmap.add_member(mcx, offset as i32)?;
                    this_bm_size += 1;
                }
            }

            if this_bm_size > bm_size {
                bitmap_base = start_oid;
                bitmap = Some(this_bitmap);
                bm_size = this_bm_size;
            }
            // (non-winners just drop here — the bms_free of the discarded
            // search bitmap; the borrow-checked context reclaims it.)

            if bm_size >= (numitems - start_pos as i32 - 1) {
                break;
            }
        }
        debug_assert!(bitmap.as_ref().map(|b| b.num_members()).unwrap_or(0) <= numitems);

        // Copy the data into the cache context (charged).
        let enum_values = mcx::slice_in(mcx, &items)?;
        let enumdata = TypeCacheEnumData {
            bitmap_base,
            sorted_values: bitmap.unwrap_or_else(|| Bitmapset::new(mcx)),
            num_values: numitems,
            enum_values,
        };

        // Link the finished cache struct in, dropping the old's charged spines
        // (C's `pfree(tcache->enumData)`; the prior `Some` is replaced/dropped).
        st.entry_mut(type_id).enum_data = Some(enumdata);
        Ok(())
    })?;
    Ok(())
}

/// `find_enumitem` — locate the EnumItem with the given OID via binary search.
fn find_enumitem(enumdata: &TypeCacheEnumData<'_>, arg: Oid) -> Option<EnumItem> {
    if enumdata.enum_values.is_empty() {
        return None;
    }
    enumdata
        .enum_values
        .binary_search_by(|probe| probe.enum_oid.cmp(&arg))
        .ok()
        .map(|idx| enumdata.enum_values[idx])
}

/* ==========================================================================
 * RelIdToTypeId map maintenance + in-progress finalization.
 * ======================================================================== */

/// `insert_rel_type_cache_if_needed`.
fn insert_rel_type_cache_if_needed(type_id: Oid) {
    with_state(|st| {
        let e = st.entry(type_id);
        if e.typtype != TYPTYPE_COMPOSITE {
            return;
        }
        debug_assert!(oid_is_valid(e.typrelid));
        if (e.flags & TCFLAGS_HAVE_PG_TYPE_DATA) != 0
            || (e.flags & TCFLAGS_OPERATOR_FLAGS) != 0
            || e.tup_desc.is_some()
        {
            let relid = e.typrelid;
            let typid = e.type_id;
            st.rel_id_to_type_id.insert(relid, typid);
        }
    });
}

/// `delete_rel_type_cache_if_needed`.
fn delete_rel_type_cache_if_needed(type_id: Oid) {
    with_state(|st| {
        let (typtype, typrelid, flags, td_null) = {
            let e = st.entry(type_id);
            (e.typtype, e.typrelid, e.flags, e.tup_desc.is_none())
        };

        // C computes is_in_progress (USE_ASSERT_CHECKING only) by scanning the
        // in_progress_list for this entry's type_id.
        let is_in_progress = st.in_progress_list.contains(&type_id);

        if typtype != TYPTYPE_COMPOSITE {
            return;
        }
        debug_assert!(oid_is_valid(typrelid));
        if (flags & TCFLAGS_HAVE_PG_TYPE_DATA) == 0 && (flags & TCFLAGS_OPERATOR_FLAGS) == 0 && td_null
        {
            let found = st.rel_id_to_type_id.remove(&typrelid).is_some();
            debug_assert!(found || is_in_progress);
        } else if !is_in_progress {
            let found = st.rel_id_to_type_id.contains_key(&typrelid);
            debug_assert!(found);
        }
    });
}

/// `finalize_in_progress_typentries`.
fn finalize_in_progress_typentries() {
    let list: Vec<Oid> = with_state(|st| st.in_progress_list.to_vec());
    for type_id in list {
        let present = with_state(|st| st.type_cache.contains_key(&type_id));
        if present {
            insert_rel_type_cache_if_needed(type_id);
        }
    }
    // C sets in_progress_list_len = 0 (keeps the CacheMemoryContext
    // allocation); clear() drops the elements and keeps the spine.
    with_state(|st| st.in_progress_list.clear());
}

/// `AtEOXact_TypeCache`.
pub fn at_eoxact_type_cache() {
    finalize_in_progress_typentries();
}

/// `AtEOSubXact_TypeCache`.
pub fn at_eosubxact_type_cache() {
    finalize_in_progress_typentries();
}

/* ==========================================================================
 * Domain constraint refs (typcache.h).
 * ======================================================================== */

/// `DomainConstraintRef` (typcache.h) — long-lived domain constraint reference.
///
/// Holds its share of the `DomainConstraintCache` directly via `Rc` (the C
/// `ref->dcc` pointer); the same `Rc` is mirrored into `TypCacheState::refs` so
/// the reset callback can release it. Not `Clone`: like the C struct, a ref's
/// refcount share is unique to that ref (cloning would dup the share without a
/// matching `dccRefCount` bump).
#[derive(Debug)]
pub struct DomainConstraintRef {
    pub constraints: Vec<DomainConstraintState>,
    pub refctx: DomainCtxHandle,
    pub tcache: Oid,
    pub need_exprstate: bool,
    dcc: Option<Rc<DomainConstraintCache>>,
    /// Stable token identifying this ref to the reset callback.
    token: u64,
}

/// `prep_domain_constraints` — convert the cached constraint list into an
/// executable list, computing `check_exprstate` via `ExecInitExpr` in
/// `execctx`. The list copy + node construction is this crate's logic; only
/// `ExecInitExpr` crosses the domains seam.
fn prep_domain_constraints(
    constraints: &[DomainConstraintState],
    execctx: DomainCtxHandle,
) -> PgResult<Vec<DomainConstraintState>> {
    let mut result = Vec::new();
    for r in constraints {
        let check_exprstate = domains_seams::exec_init_expr::call(r.check_expr, execctx)?;
        result.push(DomainConstraintState {
            constrainttype: r.constrainttype,
            name: r.name.clone(),
            check_expr: r.check_expr,
            check_exprstate,
        });
    }
    Ok(result)
}

/// `InitDomainConstraintRef`.
pub fn init_domain_constraint_ref(
    type_id: Oid,
    refctx: DomainCtxHandle,
    need_exprstate: bool,
) -> PgResult<DomainConstraintRef> {
    // Look up the typcache entry --- assume it survives indefinitely.
    lookup_type_cache(type_id, TYPECACHE_DOMAIN_CONSTR_INFO)?;

    let token = with_state(|st| st.fresh_token());
    let mut r = DomainConstraintRef {
        constraints: Vec::new(),
        refctx,
        tcache: type_id,
        need_exprstate,
        dcc: None,
        token,
    };

    // Establish the callback before acquiring a refcount.
    domains_seams::register_ref_reset_callback::call(refctx, token)?;

    // Acquire refcount if there are constraints, and set up exported list.
    let domain_data = with_state(|st| st.entry(type_id).domain_data.clone());
    if let Some(dcc) = domain_data {
        dcc.dcc_refcount.set(dcc.dcc_refcount.get() + 1);
        let constraints = dcc.constraints.clone();
        r.dcc = Some(dcc);
        r.constraints = if r.need_exprstate {
            prep_domain_constraints(&constraints, r.refctx)?
        } else {
            constraints
        };
    } else {
        r.constraints = Vec::new();
    }

    // Record the ref so the reset callback can find it (mirror its Rc share).
    with_state(|st| {
        st.refs.insert(token, RefRecord { dcc: r.dcc.clone() });
    });
    Ok(r)
}

/// `UpdateDomainConstraintRef`.
pub fn update_domain_constraint_ref(r: &mut DomainConstraintRef) -> PgResult<()> {
    let type_id = r.tcache;

    // Make sure typcache entry's data is up to date.
    let (checked, is_domain) = with_state(|st| {
        let e = st.entry(type_id);
        ((e.flags & TCFLAGS_CHECKED_DOMAIN_CONSTRAINTS) != 0, e.typtype == TYPTYPE_DOMAIN)
    });
    if !checked && is_domain {
        load_domaintype_info(type_id)?;
    }

    let domain_data = with_state(|st| st.entry(type_id).domain_data.clone());
    // C compares the raw pointers `ref->dcc != typentry->domainData`; here the
    // identity comparison is `Rc::ptr_eq` (same allocation = same dcc).
    let same = match (&r.dcc, &domain_data) {
        (None, None) => true,
        (Some(a), Some(b)) => Rc::ptr_eq(a, b),
        _ => false,
    };
    if !same {
        // Release the previous dcc (leaking previous exec list, as in C).
        if let Some(old) = r.dcc.take() {
            r.constraints = Vec::new();
            decr_dcc_refcount(&old)?;
        }
        if let Some(dcc) = domain_data {
            dcc.dcc_refcount.set(dcc.dcc_refcount.get() + 1);
            let constraints = dcc.constraints.clone();
            r.dcc = Some(dcc);
            r.constraints = if r.need_exprstate {
                prep_domain_constraints(&constraints, r.refctx)?
            } else {
                constraints
            };
        }
    }

    // Keep the recorded copy (used by the reset callback) in sync.
    with_state(|st| {
        if let Some(slot) = st.refs.get_mut(&r.token) {
            slot.dcc = r.dcc.clone();
        }
    });
    Ok(())
}

/// `DomainHasConstraints`.
pub fn domain_has_constraints(type_id: Oid) -> PgResult<bool> {
    lookup_type_cache(type_id, TYPECACHE_DOMAIN_CONSTR_INFO)?;
    Ok(with_state(|st| st.entry(type_id).domain_data.is_some()))
}

/// `domain_state_setup`'s typcache half (utils/adt/domains.c). Mirrors the C:
/// `lookup_type_cache(domainType, TYPECACHE_DOMAIN_BASE_INFO)` (which throws a
/// clean user-facing error for a bad OID and caches the base-type info), the
/// `typtype != TYPTYPE_DOMAIN` guard raising
/// `errcode(ERRCODE_DATATYPE_MISMATCH), "type %s is not a domain"`, the read of
/// `domainBaseType`/`domainBaseTypmod`, and the base type's input-function
/// lookup (`getTypeBinaryInputInfo` when `binary`, else `getTypeInputInfo`).
/// The caller (domain_in/domain_recv) does the residual `fmgr_info_cxt` and
/// `InitDomainConstraintRef`.
fn domain_get_base_input_info(
    domain_type: Oid,
    binary: bool,
) -> PgResult<backend_utils_cache_typcache_seams::DomainBaseInputInfo> {
    lookup_type_cache(domain_type, TYPECACHE_DOMAIN_BASE_INFO)?;

    let (typtype, base_type, typtypmod) = with_state(|st| {
        let e = st.entry(domain_type);
        (e.typtype, e.domain_base_type, e.domain_base_typmod)
    });
    if typtype != TYPTYPE_DOMAIN {
        return ereport_error(
            ERRCODE_DATATYPE_MISMATCH,
            format!("type {} is not a domain", format_type(domain_type)?),
        );
    }

    let (typiofunc, typioparam) = if binary {
        lsyscache_seams::get_type_binary_input_info::call(base_type)?
    } else {
        lsyscache_seams::get_type_input_info::call(base_type)?
    };

    Ok(backend_utils_cache_typcache_seams::DomainBaseInputInfo {
        typiofunc,
        typioparam,
        typtypmod,
    })
}

/* ==========================================================================
 * Typed public accessors — read fields off a cache entry the caller has
 * already populated via lookup_type_cache (the C reads `typentry->field`).
 * ======================================================================== */

/// Read the resolved operator/proc OIDs and finfo `fn_oid`s for `type_id`
/// (the entry must already be in the cache).
pub fn type_cache_eq_opr(type_id: Oid) -> Oid {
    with_state(|st| st.entry(type_id).eq_opr)
}
pub fn type_cache_lt_opr(type_id: Oid) -> Oid {
    with_state(|st| st.entry(type_id).lt_opr)
}
pub fn type_cache_gt_opr(type_id: Oid) -> Oid {
    with_state(|st| st.entry(type_id).gt_opr)
}
pub fn type_cache_cmp_proc(type_id: Oid) -> Oid {
    with_state(|st| st.entry(type_id).cmp_proc)
}
pub fn type_cache_hash_proc(type_id: Oid) -> Oid {
    with_state(|st| st.entry(type_id).hash_proc)
}
pub fn type_cache_hash_extended_proc(type_id: Oid) -> Oid {
    with_state(|st| st.entry(type_id).hash_extended_proc)
}
pub fn type_cache_typtype(type_id: Oid) -> i8 {
    with_state(|st| st.entry(type_id).typtype)
}

/* --------------------------------------------------------------------------
 * Element-type support-function lookups (typcache.c surface used by array /
 * range ADTs). Each is the C idiom
 *   lookup_type_cache(elem, TYPECACHE_*_FINFO); read entry->*_finfo.fn_oid
 * — own typcache logic (lookup + a cached-field read), returning InvalidOid
 * when the type has no such support function (the caller raises the
 * ERRCODE_UNDEFINED_FUNCTION ereport, exactly as the C does at the call site).
 * ------------------------------------------------------------------------ */

/// `lookup_type_cache(type_id, flags)` copy-out (the seam shape): run the cache
/// lookup, then hand back the `pg_type` storage fields by value (the C returns a
/// long-lived cache pointer; the safe seam copies the small row out).
fn lookup_type_cache_copyout(
    type_id: Oid,
    flags: i32,
) -> PgResult<types_typcache::TypeCacheEntry> {
    lookup_type_cache(type_id, flags)?;
    Ok(with_state(|st| {
        let e = st.entry(type_id);
        types_typcache::TypeCacheEntry {
            type_id: e.type_id,
            typlen: e.typlen,
            typbyval: e.typbyval,
            typalign: e.typalign,
            typstorage: e.typstorage,
            typtype: e.typtype,
        }
    }))
}

/// Build the `types_cache::TypeCacheEntry` copy-out shape (the range/multirange
/// view: storage fields + rng_*/hash_* finfo + recursively-copied element/range
/// sub-entries). Pure read of the already-resolved cache entry.
fn build_types_cache_entry(st: &TypCacheState<'_>, type_id: Oid) -> types_cache::TypeCacheEntry {
    let e = st.entry(type_id);
    types_cache::TypeCacheEntry {
        type_id: e.type_id,
        typlen: e.typlen,
        typbyval: e.typbyval,
        typalign: e.typalign,
        typstorage: e.typstorage,
        rng_collation: e.rng_collation,
        rng_cmp_proc_finfo: e.rng_cmp_proc_finfo,
        rng_canonical_finfo: e.rng_canonical_finfo,
        rng_subdiff_finfo: e.rng_subdiff_finfo,
        hash_proc_finfo: e.hash_proc_finfo,
        hash_extended_proc_finfo: e.hash_extended_proc_finfo,
        rngelemtype: e
            .rngelemtype
            .map(|oid| Box::new(build_types_cache_entry(st, oid))),
        rngtype: e
            .rngtype
            .map(|oid| Box::new(build_types_cache_entry(st, oid))),
    }
}

/// `lookup_type_cache(type_id, flags)` range/multirange-ADT view: resolve the
/// entry, then hand back the `types_cache::TypeCacheEntry` shape (with the
/// rng_*/hash_* finfo support fields the range ports read).
fn lookup_type_cache_entry(type_id: Oid, flags: i32) -> PgResult<types_cache::TypeCacheEntry> {
    lookup_type_cache(type_id, flags)?;
    Ok(with_state(|st| build_types_cache_entry(st, type_id)))
}

/// `lookup_type_cache(element_type, TYPECACHE_EQ_OPR_FINFO)->eq_opr_finfo.fn_oid`.
fn lookup_element_eq_opr(element_type: Oid) -> PgResult<Oid> {
    lookup_type_cache(element_type, TYPECACHE_EQ_OPR_FINFO)?;
    Ok(with_state(|st| st.entry(element_type).eq_opr_finfo.fn_oid))
}

/// `lookup_type_cache(type_id, TYPECACHE_EQ_OPR)->eq_opr` — the equality
/// operator oid of a type. `analyzeCTE` (parse_cte.c) uses it to find the
/// cycle-mark column's `<>` operator (via its negator).
fn lookup_type_cache_eq_opr(type_id: Oid) -> PgResult<Oid> {
    lookup_type_cache(type_id, TYPECACHE_EQ_OPR)?;
    Ok(with_state(|st| st.entry(type_id).eq_opr))
}

/// `lookup_type_cache(element_type, TYPECACHE_CMP_PROC_FINFO)->cmp_proc_finfo.fn_oid`.
fn lookup_element_cmp_proc(element_type: Oid) -> PgResult<Oid> {
    lookup_type_cache(element_type, TYPECACHE_CMP_PROC_FINFO)?;
    Ok(with_state(|st| st.entry(element_type).cmp_proc_finfo.fn_oid))
}

/// `lookup_type_cache(element_type, TYPECACHE_HASH_PROC_FINFO)->hash_proc_finfo.fn_oid`.
fn lookup_element_hash_proc(element_type: Oid) -> PgResult<Oid> {
    lookup_type_cache(element_type, TYPECACHE_HASH_PROC_FINFO)?;
    Ok(with_state(|st| st.entry(element_type).hash_proc_finfo.fn_oid))
}

/// `lookup_type_cache(element_type, TYPECACHE_HASH_EXTENDED_PROC_FINFO)->hash_extended_proc_finfo.fn_oid`.
fn lookup_element_hash_extended_proc(element_type: Oid) -> PgResult<Oid> {
    lookup_type_cache(element_type, TYPECACHE_HASH_EXTENDED_PROC_FINFO)?;
    Ok(with_state(|st| st.entry(element_type).hash_extended_proc_finfo.fn_oid))
}

/// `lookup_range_elem_hash_proc` — the `hash_range`/`hash_multirange` element
/// fallback: resolve the (optionally extended) hash support function OID for
/// the element type. Mirrors the C `lookup_type_cache(elem, TYPECACHE_HASH_*…)`
/// + `OidIsValid(finfo.fn_oid)` check, raising `ERRCODE_UNDEFINED_FUNCTION`
/// ("could not identify a hash function for type %s") when none exists.
fn lookup_range_elem_hash_proc(elem_type_id: Oid, extended: bool) -> PgResult<Oid> {
    let oid = if extended {
        lookup_element_hash_extended_proc(elem_type_id)?
    } else {
        lookup_element_hash_proc(elem_type_id)?
    };
    if !oid_is_valid(oid) {
        return ereport_error(
            ERRCODE_UNDEFINED_FUNCTION,
            format!("could not identify a hash function for type {}", format_type(elem_type_id)?),
        );
    }
    Ok(oid)
}

/* ==========================================================================
 * Small numeric helpers.
 * ======================================================================== */

/// `pg_nextpower2_32` — smallest power of 2 >= num (num must be > 0).
fn pg_nextpower2_32(num: u32) -> u32 {
    debug_assert!(num > 0);
    if num == 1 {
        return 1;
    }
    1u32 << (32 - (num - 1).leading_zeros())
}

/* ==========================================================================
 * Seam installation.
 * ======================================================================== */

/// Install every seam this crate owns (the inward seams other crates call
/// across a cycle).
/// `get_sort_group_operators` (parse_oper.c) typcache leg: run
/// `lookup_type_cache(argtype, TYPECACHE_LT_OPR | TYPECACHE_EQ_OPR |
/// TYPECACHE_GT_OPR [| TYPECACHE_HASH_PROC])` and read back the resolved
/// default sort/group operators. The trimmed copy-out [`TypeCacheEntry`]
/// does not carry these fields, so the seam encapsulates the lookup + the
/// cached-field reads on the owner.
fn sort_group_operators(argtype: Oid, want_hashable: bool) -> PgResult<(Oid, Oid, Oid, bool)> {
    let cache_flags = if want_hashable {
        TYPECACHE_LT_OPR | TYPECACHE_EQ_OPR | TYPECACHE_GT_OPR | TYPECACHE_HASH_PROC
    } else {
        TYPECACHE_LT_OPR | TYPECACHE_EQ_OPR | TYPECACHE_GT_OPR
    };

    lookup_type_cache(argtype, cache_flags)?;

    let lt_opr = type_cache_lt_opr(argtype);
    let eq_opr = type_cache_eq_opr(argtype);
    let gt_opr = type_cache_gt_opr(argtype);
    let hashable = want_hashable && type_cache_hash_proc(argtype) != INVALID_OID;

    Ok((lt_opr, eq_opr, gt_opr, hashable))
}

pub fn init_seams() {
    backend_utils_cache_typcache_seams::compare_values_of_enum::set(compare_values_of_enum);
    backend_utils_cache_typcache_seams::sort_group_operators::set(sort_group_operators);
    backend_utils_cache_typcache_seams::lookup_rowtype_tupdesc::set(lookup_rowtype_tupdesc);
    backend_utils_cache_typcache_seams::assign_record_type_typmod::set(assign_record_type_typmod);
    backend_utils_cache_typcache_seams::at_eoxact_type_cache::set(at_eoxact_type_cache);
    backend_utils_cache_typcache_seams::at_eosubxact_type_cache::set(at_eosubxact_type_cache);
    // Pure-wiring install (assemble/seam-wiring-guard): owner body matches.
    backend_utils_cache_typcache_seams::lookup_rowtype_tupdesc_copy::set(lookup_rowtype_tupdesc_copy);
    // Element-type support-function lookups (own typcache logic: lookup + finfo
    // OID read). The array/range ADTs call these across the dep cycle.
    backend_utils_cache_typcache_seams::lookup_element_eq_opr::set(lookup_element_eq_opr);
    backend_utils_cache_typcache_seams::lookup_type_cache_eq_opr::set(lookup_type_cache_eq_opr);
    backend_utils_cache_typcache_seams::lookup_element_cmp_proc::set(lookup_element_cmp_proc);
    backend_utils_cache_typcache_seams::lookup_element_hash_proc::set(lookup_element_hash_proc);
    backend_utils_cache_typcache_seams::lookup_element_hash_extended_proc::set(
        lookup_element_hash_extended_proc,
    );
    backend_utils_cache_typcache_seams::lookup_range_elem_hash_proc::set(lookup_range_elem_hash_proc);
    // Copy-out entry lookups (own typcache logic + a by-value copy of the cache
    // row). The range/multirange ADTs call these across the dep cycle.
    backend_utils_cache_typcache_seams::lookup_type_cache::set(lookup_type_cache_copyout);
    backend_utils_cache_typcache_seams::lookup_type_cache_entry::set(lookup_type_cache_entry);
    // expandedrecord.c builder views (composite/domain-over-composite tupdesc +
    // identifier) and the RECORD-type identifier assignment. The expandedrecord
    // family (backend-utils-adt-misc2) calls these across the dep cycle.
    backend_utils_cache_typcache_seams::lookup_type_cache_expanded_record::set(
        lookup_type_cache_expanded_record,
    );
    backend_utils_cache_typcache_seams::lookup_type_cache_tupdesc_view::set(
        lookup_type_cache_tupdesc_view,
    );
    backend_utils_cache_typcache_seams::assign_record_type_identifier::set(
        assign_record_type_identifier,
    );
    // domain_state_setup's typcache half (domains.c): lookup_type_cache +
    // TYPTYPE_DOMAIN guard + base type I/O lookup. The domains ADT
    // (backend-utils-adt-misc2) calls this across the dep cycle.
    backend_utils_cache_typcache_seams::domain_get_base_input_info::set(
        domain_get_base_input_info,
    );
}
