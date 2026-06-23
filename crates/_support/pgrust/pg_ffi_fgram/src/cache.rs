//! Catalog/relation/type cache ABI vocabulary.
//!
//! These `#[repr(C)]` structs cross the boundary between the rewritten cache
//! crates (`backend-utils-cache-catcache`, `-syscache`, `-inval`, `-relcache`,
//! `-typcache`) and the rest of the backend.  They mirror the C definitions in
//!   * `src/include/utils/catcache.h`   (CatCache / CatCTup / CatCList / header)
//!   * `src/include/utils/rel.h`        (RelationData)
//!   * `src/backend/utils/cache/relcache.c` (RelIdCacheEnt)
//!   * `src/include/storage/sinval.h`   (SharedInvalidationMessage union)
//!   * `src/include/utils/typcache.h`   (TypeCacheEntry)
//!
//! Layout-critical fields keep their exact C order/width; catalog-side sub-objects
//! that this workspace has not yet modeled are held as pointer-width opaque
//! handles (`*mut c_void`), which is ABI-identical to the C pointers they stand
//! in for.  Compile-time size/align assertions pin the layout where it matters.

use core::ffi::c_void;
use core::mem::{align_of, size_of};

use crate::access::RegProcedure;
use crate::guc::{dlist_head, dlist_node, slist_head, slist_node};
use crate::heaptuple::{HeapTupleData, TupleDesc};
use crate::scankey::ScanKeyData;
use crate::wal::RelFileLocator;
use crate::{Datum, FmgrInfo, Oid, ProcNumber, SubTransactionId};

/* ---------------------------------------------------------------------------
 * catcache.h
 * ------------------------------------------------------------------------- */

/// `CATCACHE_MAXKEYS` — maximum number of keys in a catalog cache.
pub const CATCACHE_MAXKEYS: usize = 4;

/// `CT_MAGIC` — sentinel stored in `CatCTup::ct_magic`.
pub const CT_MAGIC: i32 = 0x5726_1502;
/// `CL_MAGIC` — sentinel stored in `CatCList::cl_magic`.
pub const CL_MAGIC: i32 = 0x5276_5103;

/// `CCHashFN` — hash function for a single catcache key datum.
pub type CCHashFN = unsafe extern "C" fn(Datum) -> u32;
/// `CCFastEqualFN` — equality function for two catcache key datums.
pub type CCFastEqualFN = unsafe extern "C" fn(Datum, Datum) -> bool;

/// `CatCache` (`struct catcache`) — per-cache control block.
///
/// Built without `CATCACHE_STATS`, matching the default backend build.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CatCache {
    pub id: i32,
    pub cc_nbuckets: i32,
    pub cc_tupdesc: TupleDesc,
    pub cc_bucket: *mut dlist_head,
    pub cc_hashfunc: [Option<CCHashFN>; CATCACHE_MAXKEYS],
    pub cc_fastequal: [Option<CCFastEqualFN>; CATCACHE_MAXKEYS],
    pub cc_keyno: [i32; CATCACHE_MAXKEYS],
    pub cc_nkeys: i32,
    pub cc_ntup: i32,
    pub cc_nlist: i32,
    pub cc_nlbuckets: i32,
    pub cc_lbucket: *mut dlist_head,
    pub cc_relname: *const core::ffi::c_char,
    pub cc_reloid: Oid,
    pub cc_indexoid: Oid,
    pub cc_relisshared: bool,
    pub cc_next: slist_node,
    pub cc_skey: [ScanKeyData; CATCACHE_MAXKEYS],
}

/// `CatCTup` (`struct catctup`) — one cached catalog tuple.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CatCTup {
    pub ct_magic: i32,
    pub hash_value: u32,
    pub keys: [Datum; CATCACHE_MAXKEYS],
    pub cache_elem: dlist_node,
    pub refcount: i32,
    pub dead: bool,
    pub negative: bool,
    pub tuple: HeapTupleData,
    pub c_list: *mut CatCList,
    pub my_cache: *mut CatCache,
}

/// `CatCList` (`struct catclist`) — result of a partial-key list search.
///
/// The trailing `members[FLEXIBLE_ARRAY_MEMBER]` is represented as a
/// zero-length array; the real allocation is over-sized by the cache code.
#[repr(C)]
pub struct CatCList {
    pub cl_magic: i32,
    pub hash_value: u32,
    pub cache_elem: dlist_node,
    pub keys: [Datum; CATCACHE_MAXKEYS],
    pub refcount: i32,
    pub dead: bool,
    pub ordered: bool,
    pub nkeys: i16,
    pub n_members: i32,
    pub my_cache: *mut CatCache,
    pub members: [*mut CatCTup; 0],
}

/// `CatCacheHeader` (`struct catcacheheader`).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CatCacheHeader {
    pub ch_caches: slist_head,
    pub ch_ntup: i32,
}

/* ---------------------------------------------------------------------------
 * rel.h: RelationData
 *
 * The many catalog-side sub-objects (Form_pg_class, RuleLock, TriggerDesc,
 * PartitionKey, ...) are pointer-width opaque handles here, ABI-identical to
 * the C pointers.  `LockInfoData` is embedded by value and modeled below.
 * ------------------------------------------------------------------------- */

/// `LockRelId` (`src/include/storage/lock.h`).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LockRelId {
    pub relId: Oid,
    pub dbId: Oid,
}

/// `LockInfoData` (`src/include/storage/lock.h`) — embedded in `RelationData`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LockInfoData {
    pub lockRelId: LockRelId,
}

/// `RelationData` (`src/include/utils/rel.h`) — a relcache entry.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RelationData {
    pub rd_locator: RelFileLocator,
    pub rd_smgr: *mut c_void,
    pub rd_refcnt: i32,
    pub rd_backend: ProcNumber,
    pub rd_islocaltemp: bool,
    pub rd_isnailed: bool,
    pub rd_isvalid: bool,
    pub rd_indexvalid: bool,
    pub rd_statvalid: bool,
    pub rd_createSubid: SubTransactionId,
    pub rd_newRelfilelocatorSubid: SubTransactionId,
    pub rd_firstRelfilelocatorSubid: SubTransactionId,
    pub rd_droppedSubid: SubTransactionId,
    pub rd_rel: *mut c_void,
    pub rd_att: TupleDesc,
    pub rd_id: Oid,
    pub rd_lockInfo: LockInfoData,
    pub rd_rules: *mut c_void,
    pub rd_rulescxt: *mut c_void,
    pub trigdesc: *mut c_void,
    pub rd_rsdesc: *mut c_void,
    pub rd_fkeylist: *mut c_void,
    pub rd_fkeyvalid: bool,
    pub rd_partkey: *mut c_void,
    pub rd_partkeycxt: *mut c_void,
    pub rd_partdesc: *mut c_void,
    pub rd_pdcxt: *mut c_void,
    pub rd_partdesc_nodetached: *mut c_void,
    pub rd_pddcxt: *mut c_void,
    pub rd_partdesc_nodetached_xmin: u32,
    pub rd_partcheck: *mut c_void,
    pub rd_partcheckvalid: bool,
    pub rd_partcheckcxt: *mut c_void,
    pub rd_indexlist: *mut c_void,
    pub rd_pkindex: Oid,
    pub rd_ispkdeferrable: bool,
    pub rd_replidindex: Oid,
    pub rd_statlist: *mut c_void,
    pub rd_attrsvalid: bool,
    pub rd_keyattr: *mut c_void,
    pub rd_pkattr: *mut c_void,
    pub rd_idattr: *mut c_void,
    pub rd_hotblockingattr: *mut c_void,
    pub rd_summarizedattr: *mut c_void,
    pub rd_pubdesc: *mut c_void,
    pub rd_options: *mut c_void,
    pub rd_amhandler: Oid,
    pub rd_tableam: *const c_void,
    pub rd_index: *mut c_void,
    pub rd_indextuple: *mut HeapTupleData,
    pub rd_indexcxt: *mut c_void,
    pub rd_indam: *mut c_void,
    pub rd_opfamily: *mut Oid,
    pub rd_opcintype: *mut Oid,
    pub rd_support: *mut RegProcedure,
    pub rd_supportinfo: *mut FmgrInfo,
    pub rd_indoption: *mut i16,
    pub rd_indexprs: *mut c_void,
    pub rd_indpred: *mut c_void,
    pub rd_exclops: *mut Oid,
    pub rd_exclprocs: *mut Oid,
    pub rd_exclstrats: *mut u16,
    pub rd_indcollation: *mut Oid,
    pub rd_opcoptions: *mut *mut c_void,
    pub rd_amcache: *mut c_void,
    pub rd_fdwroutine: *mut c_void,
    pub rd_toastoid: Oid,
    pub pgstat_enabled: bool,
    pub pgstat_info: *mut c_void,
}

// NOTE: the `Relation` type alias (`typedef struct RelationData *`) is provided
// at the crate root by `gist.rs` (as an opaque `*mut c_void` for the wide set of
// crates that only pass relations through).  The relcache crate uses
// `*mut RelationData` directly where it needs typed field access.

/// `RelIdCacheEnt` (relcache.c) — the `RelationIdCache` dynahash entry.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RelIdCacheEnt {
    pub reloid: Oid,
    pub reldesc: *mut RelationData,
}

/* ---------------------------------------------------------------------------
 * sinval.h: SharedInvalidationMessage union
 *
 * The message structs, union, and `SHAREDINVAL*_ID` discriminators are already
 * defined in `crate::storage` (they are shared with the SI queue / smgr).  We
 * re-export them through this module so the cache crates have a single import.
 * ------------------------------------------------------------------------- */

use crate::storage::{SharedInvalCatcacheMsg, SharedInvalSmgrMsg, SharedInvalidationMessage};

/* ---------------------------------------------------------------------------
 * typcache.h: TypeCacheEntry
 * ------------------------------------------------------------------------- */

/// `TypeCacheEntry` (`src/include/utils/typcache.h`).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TypeCacheEntry {
    pub type_id: Oid,
    pub type_id_hash: u32,
    pub typlen: i16,
    pub typbyval: bool,
    pub typalign: core::ffi::c_char,
    pub typstorage: core::ffi::c_char,
    pub typtype: core::ffi::c_char,
    pub typrelid: Oid,
    pub typsubscript: Oid,
    pub typelem: Oid,
    pub typarray: Oid,
    pub typcollation: Oid,
    pub btree_opf: Oid,
    pub btree_opintype: Oid,
    pub hash_opf: Oid,
    pub hash_opintype: Oid,
    pub eq_opr: Oid,
    pub lt_opr: Oid,
    pub gt_opr: Oid,
    pub cmp_proc: Oid,
    pub hash_proc: Oid,
    pub hash_extended_proc: Oid,
    pub eq_opr_finfo: FmgrInfo,
    pub cmp_proc_finfo: FmgrInfo,
    pub hash_proc_finfo: FmgrInfo,
    pub hash_extended_proc_finfo: FmgrInfo,
    pub tupDesc: TupleDesc,
    pub tupDesc_identifier: u64,
    pub rngelemtype: *mut TypeCacheEntry,
    pub rng_opfamily: Oid,
    pub rng_collation: Oid,
    pub rng_cmp_proc_finfo: FmgrInfo,
    pub rng_canonical_finfo: FmgrInfo,
    pub rng_subdiff_finfo: FmgrInfo,
    pub rngtype: *mut TypeCacheEntry,
    pub domainBaseType: Oid,
    pub domainBaseTypmod: i32,
    pub domainData: *mut c_void,
    pub flags: i32,
    pub enumData: *mut c_void,
    pub nextDomain: *mut TypeCacheEntry,
}

/* TypeCacheEntry::flags bit values (typcache.h) */
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

/// `INVALID_TUPLEDESC_IDENTIFIER` — a tupdesc id that never equals a real one.
pub const INVALID_TUPLEDESC_IDENTIFIER: u64 = 1;

/* ---------------------------------------------------------------------------
 * Layout assertions: pin the fields/widths the ABI depends on.
 * ------------------------------------------------------------------------- */

const _: () = assert!(CATCACHE_MAXKEYS == 4);
const _: () = assert!(align_of::<CatCache>() == align_of::<*mut c_void>());
const _: () = assert!(align_of::<CatCTup>() == align_of::<*mut c_void>());
const _: () = assert!(align_of::<CatCList>() == align_of::<*mut c_void>());
const _: () = assert!(align_of::<RelationData>() == align_of::<*mut c_void>());
const _: () = assert!(align_of::<TypeCacheEntry>() == align_of::<*mut c_void>());

// LockInfoData is two Oids wide.
const _: () = assert!(size_of::<LockInfoData>() == 2 * size_of::<Oid>());
// RelIdCacheEnt: Oid then pointer (with padding to pointer alignment).
const _: () = assert!(size_of::<RelIdCacheEnt>() == 2 * size_of::<*mut c_void>());
// Catcache message: i8 + Oid + u32, padded to 12 bytes.
const _: () = assert!(size_of::<SharedInvalCatcacheMsg>() == 12);
// The union is at least as large as its widest member (the smgr message,
// which embeds a RelFileLocator: 3 Oids = 12 bytes plus the 4-byte prefix).
const _: () = assert!(size_of::<SharedInvalidationMessage>() >= size_of::<SharedInvalSmgrMsg>());
const _: () = assert!(size_of::<SharedInvalSmgrMsg>() == 16);
// TypeCacheEntry begins with type_id (Oid) immediately followed by its hash.
const _: () = assert!(core::mem::offset_of!(TypeCacheEntry, type_id) == 0);
const _: () = assert!(core::mem::offset_of!(TypeCacheEntry, type_id_hash) == size_of::<Oid>());
