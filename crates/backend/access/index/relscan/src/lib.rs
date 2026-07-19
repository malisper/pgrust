//! relscan.h vocabulary shared by indexam (dispatch) and the index AMs —
//! split out so nbtree takes the descriptor without a cycle through indexam.
#![allow(non_snake_case)]

use std::rc::Rc;
use std::sync::Arc;

pub mod parallel;
pub use parallel::{
    BTParallelScanShared, BtParallelArrayElem, BtParallelScanState, BtParallelSkipArg, BtPsState,
    ParallelIndexAmShared, ParallelIndexScanDescShared,
};

use ::mcx::{Mcx, PgBox, PgVec};
use ::gin_vocab::GinScanOpaqueData;
use ::types_core::{Oid, BRIN_AM_OID, BTREE_AM_OID, GIN_AM_OID, GIST_AM_OID, HASH_AM_OID, SPGIST_AM_OID};
use ::types_hash::HashScanOpaqueData;
use ::types_error::PgResult;
use ::types_gist::state::GISTScanOpaqueData;
use ::types_nbtree::BTScanOpaqueData;
use ::types_rel::Relation;
use ::types_scan::scankey::ScanKeyData;
use ::types_snapshot::SnapshotData;
use ::types_tuple::itemptr::ItemPointerData;
use ::types_tuple::TupleDescData;

#[cfg(feature = "mock")]
pub const MOCK_AM_OID: Oid = 9999;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IndexAmKind {
    Btree,
    Hash,
    Gin,
    Gist,
    Spgist,
    Brin,
    Hnsw,
    Bloom,
    #[cfg(feature = "mock")]
    Mock,
}

impl IndexAmKind {
    pub fn from_relam(relam: Oid) -> Self {
        match relam {
            BTREE_AM_OID => IndexAmKind::Btree,
            HASH_AM_OID => IndexAmKind::Hash,
            GIN_AM_OID => IndexAmKind::Gin,
            GIST_AM_OID => IndexAmKind::Gist,
            SPGIST_AM_OID => IndexAmKind::Spgist,
            BRIN_AM_OID => IndexAmKind::Brin,
            #[cfg(feature = "mock")]
            MOCK_AM_OID => IndexAmKind::Mock,
            other => registered_index_am(other),
        }
    }

    pub const fn ampredlocks(self) -> bool {
        match self {
            IndexAmKind::Btree => true,
            IndexAmKind::Hash => true,
            IndexAmKind::Gin => true,
            IndexAmKind::Gist => true,
            IndexAmKind::Spgist => false,
            IndexAmKind::Brin => false,
            IndexAmKind::Hnsw => false,
            IndexAmKind::Bloom => false,
            #[cfg(feature = "mock")]
            IndexAmKind::Mock => true,
        }
    }

    pub const fn amsummarizing(self) -> bool {
        match self {
            IndexAmKind::Btree => false,
            IndexAmKind::Hash => false,
            IndexAmKind::Gin => false,
            IndexAmKind::Gist => false,
            IndexAmKind::Spgist => false,
            IndexAmKind::Brin => true,
            IndexAmKind::Hnsw => false,
            IndexAmKind::Bloom => false,
            #[cfg(feature = "mock")]
            IndexAmKind::Mock => false,
        }
    }

    pub const fn amsearcharray(self) -> bool {
        match self {
            IndexAmKind::Btree => true,
            IndexAmKind::Hash => false,
            IndexAmKind::Gin => true,
            IndexAmKind::Gist => false,
            IndexAmKind::Spgist => false,
            IndexAmKind::Brin => false,
            IndexAmKind::Hnsw => false,
            IndexAmKind::Bloom => false,
            #[cfg(feature = "mock")]
            IndexAmKind::Mock => false,
        }
    }

    pub const fn has_ammarkpos(self) -> bool {
        match self {
            IndexAmKind::Btree => true,
            IndexAmKind::Hash => false,
            IndexAmKind::Gin => false,
            IndexAmKind::Gist => false,
            IndexAmKind::Spgist => false,
            IndexAmKind::Brin => false,
            IndexAmKind::Hnsw => false,
            IndexAmKind::Bloom => false,
            #[cfg(feature = "mock")]
            IndexAmKind::Mock => true,
        }
    }

    pub const fn has_amrestrpos(self) -> bool {
        match self {
            IndexAmKind::Btree => true,
            IndexAmKind::Hash => false,
            IndexAmKind::Gin => false,
            IndexAmKind::Gist => false,
            IndexAmKind::Spgist => false,
            IndexAmKind::Brin => false,
            IndexAmKind::Hnsw => false,
            IndexAmKind::Bloom => false,
            #[cfg(feature = "mock")]
            IndexAmKind::Mock => false,
        }
    }

    pub const fn amstrategies(self) -> i32 {
        match self {
            IndexAmKind::Btree => 5,
            IndexAmKind::Hash => 1,
            IndexAmKind::Gin => 0,
            IndexAmKind::Gist => 0,
            IndexAmKind::Spgist => 0,
            IndexAmKind::Brin => 0,
            IndexAmKind::Hnsw => 0,
            IndexAmKind::Bloom => 1,
            #[cfg(feature = "mock")]
            IndexAmKind::Mock => 5,
        }
    }

    pub const fn amsupport(self) -> i32 {
        match self {
            IndexAmKind::Btree => 6,
            IndexAmKind::Hash => 3,
            IndexAmKind::Gin => 7,
            IndexAmKind::Gist => 12,
            IndexAmKind::Spgist => 7,
            IndexAmKind::Brin => 15,
            IndexAmKind::Hnsw => 3,
            IndexAmKind::Bloom => 2,
            #[cfg(feature = "mock")]
            IndexAmKind::Mock => 6,
        }
    }

    pub const fn amoptsprocnum(self) -> i32 {
        match self {
            IndexAmKind::Btree => 5,
            IndexAmKind::Hash => 3,
            IndexAmKind::Gin => 7,
            IndexAmKind::Gist => 10,
            IndexAmKind::Spgist => 7,
            IndexAmKind::Brin => 5,
            IndexAmKind::Hnsw => 0,
            IndexAmKind::Bloom => 2,
            #[cfg(feature = "mock")]
            IndexAmKind::Mock => 5,
        }
    }

    pub const fn amstorage(self) -> bool {
        match self {
            IndexAmKind::Btree => false,
            IndexAmKind::Hash => false,
            IndexAmKind::Gin => true,
            IndexAmKind::Gist => true,
            IndexAmKind::Spgist => true,
            IndexAmKind::Brin => true,
            IndexAmKind::Hnsw => false,
            IndexAmKind::Bloom => false,
            #[cfg(feature = "mock")]
            IndexAmKind::Mock => false,
        }
    }

    pub const fn amcanorder(self) -> bool {
        match self {
            IndexAmKind::Btree => true,
            IndexAmKind::Hash => false,
            IndexAmKind::Gin => false,
            IndexAmKind::Gist => false,
            IndexAmKind::Spgist => false,
            IndexAmKind::Brin => false,
            IndexAmKind::Hnsw => false,
            IndexAmKind::Bloom => false,
            #[cfg(feature = "mock")]
            IndexAmKind::Mock => true,
        }
    }

    pub const fn amcanhash(self) -> bool {
        match self {
            IndexAmKind::Btree => false,
            IndexAmKind::Hash => true,
            IndexAmKind::Gin => false,
            IndexAmKind::Gist => false,
            IndexAmKind::Spgist => false,
            IndexAmKind::Brin => false,
            IndexAmKind::Hnsw => false,
            IndexAmKind::Bloom => false,
            #[cfg(feature = "mock")]
            IndexAmKind::Mock => false,
        }
    }

    pub const fn amcanorderbyop(self) -> bool {
        match self {
            IndexAmKind::Btree => false,
            IndexAmKind::Hash => false,
            IndexAmKind::Gin => false,
            IndexAmKind::Gist => true,
            IndexAmKind::Spgist => true,
            IndexAmKind::Brin => false,
            IndexAmKind::Hnsw => true,
            IndexAmKind::Bloom => false,
            #[cfg(feature = "mock")]
            IndexAmKind::Mock => false,
        }
    }

    pub const fn has_aminsertcleanup(self) -> bool {
        match self {
            IndexAmKind::Btree => false,
            IndexAmKind::Hash => false,
            IndexAmKind::Gin => false,
            IndexAmKind::Gist => false,
            IndexAmKind::Spgist => false,
            IndexAmKind::Brin => true,
            IndexAmKind::Hnsw => false,
            IndexAmKind::Bloom => false,
            #[cfg(feature = "mock")]
            IndexAmKind::Mock => false,
        }
    }
}

// amapi.h OpFamilyMember: DDL carrier for opclass/opfamily member entries.
#[derive(Clone, Copy, Debug)]
pub struct OpFamilyMember {
    pub is_func: bool,
    pub object: Oid,
    pub number: i16,
    pub sortfamily: Oid,
    pub lefttype: Oid,
    pub righttype: Oid,
    pub ref_is_hard: bool,
    pub ref_is_family: bool,
    pub refobjid: Oid,
}

impl Default for OpFamilyMember {
    fn default() -> Self {
        OpFamilyMember {
            is_func: false,
            object: 0,
            number: 0,
            sortfamily: 0,
            lefttype: 0,
            righttype: 0,
            ref_is_hard: false,
            ref_is_family: false,
            refobjid: 0,
        }
    }
}

// C resolves any pg_am row through its amhandler; the closed IndexAmKind set
// gives non-builtin AMs (CREATE ACCESS METHOD over a builtin handler) a
// per-thread registry plus a lazy resolver (amapi's pg_am.amhandler lookup).
// INVARIANT (set-once): the resolver slot is written during single-threaded
// startup, before any session thread calls from_relam (seam_core's model).
thread_local! {
    static REGISTERED_INDEX_AMS: std::cell::RefCell<Vec<(Oid, IndexAmKind)>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

static INDEX_AM_RESOLVER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

pub fn register_index_am(relam: Oid, kind: IndexAmKind) {
    REGISTERED_INDEX_AMS.with(|v| {
        let mut v = v.borrow_mut();
        if !v.iter().any(|&(o, _)| o == relam) {
            v.push((relam, kind));
        }
    });
}

pub fn set_index_am_resolver(f: fn(Oid) -> Option<IndexAmKind>) {
    INDEX_AM_RESOLVER.store(f as usize, std::sync::atomic::Ordering::Relaxed);
}

#[cold]
#[inline(never)]
fn registered_index_am(relam: Oid) -> IndexAmKind {
    if let Some(k) = REGISTERED_INDEX_AMS
        .with(|v| v.borrow().iter().find(|&&(o, _)| o == relam).map(|&(_, k)| k))
    {
        return k;
    }
    let raw = INDEX_AM_RESOLVER.load(std::sync::atomic::Ordering::Relaxed);
    if raw != 0 {
        // SAFETY: set-once invariant above — always a valid resolver fn.
        let f: fn(Oid) -> Option<IndexAmKind> = unsafe { std::mem::transmute(raw) };
        if let Some(k) = f(relam) {
            register_index_am(relam, k);
            return k;
        }
    }
    unported_index_am(relam)
}

#[cold]
#[inline(never)]
fn unported_index_am(relam: Oid) -> ! {
    panic!("unported: index AM {relam} (IndexAmKind covers btree+hash+gin+gist+spgist+brin)")
}

pub enum IndexScanOpaque<'mcx> {
    Btree(PgBox<'mcx, BTScanOpaqueData<'mcx>>),
    Hash(PgBox<'mcx, HashScanOpaqueData<'mcx>>),
    Gin(PgBox<'mcx, GinScanOpaqueData>),
    Gist(PgBox<'mcx, GISTScanOpaqueData<'mcx>>),
    Spgist(PgBox<'mcx, ::types_spgist::state::SpGistScanOpaqueData<'mcx>>),
    Brin(PgBox<'mcx, ::types_brin::BrinOpaque<'mcx>>),
    Hnsw(PgBox<'mcx, ::types_hnsw::HnswScanOpaqueData<'mcx>>),
    Bloom(PgBox<'mcx, ::types_bloom::BloomScanOpaqueData>),
    #[cfg(feature = "mock")]
    Mock(MockOpaque),
}

impl IndexScanOpaque<'_> {
    #[inline]
    pub fn kind(&self) -> IndexAmKind {
        match self {
            IndexScanOpaque::Btree(_) => IndexAmKind::Btree,
            IndexScanOpaque::Hash(_) => IndexAmKind::Hash,
            IndexScanOpaque::Gin(_) => IndexAmKind::Gin,
            IndexScanOpaque::Gist(_) => IndexAmKind::Gist,
            IndexScanOpaque::Spgist(_) => IndexAmKind::Spgist,
            IndexScanOpaque::Brin(_) => IndexAmKind::Brin,
            IndexScanOpaque::Hnsw(_) => IndexAmKind::Hnsw,
            IndexScanOpaque::Bloom(_) => IndexAmKind::Bloom,
            #[cfg(feature = "mock")]
            IndexScanOpaque::Mock(_) => IndexAmKind::Mock,
        }
    }
}

#[cfg(feature = "mock")]
#[derive(Default)]
pub struct MockOpaque {
    pub tids: Vec<ItemPointerData>,
    pub next: usize,
    pub kill_seen: Vec<bool>,
    pub rescans: u32,
    pub markpos_calls: u32,
}

// C's IndexFetchTableData; wraps tableam's enum so tests can script fetches.
pub enum IndexFetchTableData<'mcx> {
    Table(::tableam::IndexFetchTableData<'mcx>),
    #[cfg(feature = "mock")]
    Mock(MockFetch<'mcx>),
}

#[cfg(feature = "mock")]
pub struct MockFetch<'mcx> {
    pub rel: Relation<'mcx>,
    pub mock_fetch: Vec<(bool, bool, bool)>,
    pub resets: u32,
}

#[cfg(feature = "mock")]
impl<'mcx> IndexFetchTableData<'mcx> {
    pub fn mock(&self) -> &MockFetch<'mcx> {
        match self {
            IndexFetchTableData::Mock(m) => m,
            IndexFetchTableData::Table(_) => panic!("not a mock fetch"),
        }
    }

    pub fn mock_mut(&mut self) -> &mut MockFetch<'mcx> {
        match self {
            IndexFetchTableData::Mock(m) => m,
            IndexFetchTableData::Table(_) => panic!("not a mock fetch"),
        }
    }
}

/// Heap-readahead state for plain index scans (upstream index-prefetching
/// series, CF 4351, adapted). Advisory only: issuance never changes what the
/// scan reads, only when the kernel sees the block coming. Dormant (`!active`)
/// until the scan proves it does real I/O — warm scans pay one block compare
/// per heap-block switch and nothing else.
#[derive(Clone, Copy, Debug)]
pub struct IndexPrefetchState {
    /// shared_blks_read snapshot at first block switch; delta > 0 arms issuance.
    pub read_base: u64,
    pub last_block: u32,
    pub last_issued: u32,
    /// Leaf page + item cursor the issuance walk has covered.
    pub leaf_page: u32,
    pub issued_idx: i32,
    pub distance: u16,
    pub inflight: u16,
    pub switches: u16,
    pub cached_streak: u16,
    pub active: bool,
}

impl IndexPrefetchState {
    pub const fn reset() -> Self {
        IndexPrefetchState {
            read_base: 0,
            last_block: u32::MAX,
            last_issued: u32::MAX,
            leaf_page: u32::MAX,
            issued_idx: 0,
            distance: 0,
            inflight: 0,
            switches: 0,
            cached_streak: 0,
            active: false,
        }
    }
}

const _: () = assert!(!core::mem::needs_drop::<IndexPrefetchState>());

// Trimmed to the amgettuple shape; a rule-3 by-value resource owner.
pub struct IndexScanDescData<'mcx> {
    pub heapRelation: Option<Relation<'mcx>>,
    // None only while parked in an executor skeleton (relations close per
    // execution — CheckTableNotInUse counts these aliases).
    pub indexRelation: Option<Relation<'mcx>>,
    pub xs_snapshot: Option<Rc<SnapshotData<'mcx>>>,
    pub numberOfKeys: i32,
    pub numberOfOrderBys: i32,
    pub keyData: PgVec<'mcx, ScanKeyData>,
    pub orderByData: PgVec<'mcx, ScanKeyData>,

    pub parallel_scan: Option<Arc<ParallelIndexScanDescShared>>,

    pub xs_want_itup: bool,
    // Points into the AM's page-copy buffer (nbtree currTuples); valid until
    // the next amgettuple/amrescan/amendscan on this descriptor.
    pub xs_itup: Option<core::ptr::NonNull<u8>>,
    pub xs_itupdesc: Option<Rc<TupleDescData<'mcx>>>,
    pub xs_temp_snap: bool,
    // 'mcx-erased xs_snapshot copy: the 'static Rc UnregisterSnapshot needs.
    pub xs_temp_snapshot: Option<Rc<SnapshotData<'static>>>,
    pub kill_prior_tuple: bool,
    pub ignore_killed_tuples: bool,
    pub xactStartedInRecovery: bool,

    pub opaque: IndexScanOpaque<'mcx>,

    pub xs_heaptid: ItemPointerData,
    pub xs_heap_continue: bool,
    pub xs_heapfetch: Option<IndexFetchTableData<'mcx>>,
    pub xs_recheck: bool,

    // amcanorderbyop (KNN) scans: numberOfOrderBys entries, filled by the AM
    // per returned tuple (C xs_orderbyvals/xs_orderbynulls/xs_recheckorderby).
    pub xs_orderbyvals: PgVec<'mcx, datum::Datum>,
    pub xs_orderbynulls: PgVec<'mcx, bool>,
    pub xs_recheckorderby: bool,
    pub xs_prefetch: IndexPrefetchState,

    pub xs_pgstat_index_tuples: u64,
    pub xs_pgstat_heap_fetches: u64,
    pub xs_pgstat_index_scans: u64,
    // C IndexScanInstrumentation.nsearches (pgstat-independent; EXPLAIN reads it).
    pub xs_nsearches: u64,
}

impl<'mcx> IndexScanDescData<'mcx> {
    #[inline]
    pub fn index_rel(&self) -> &Relation<'mcx> {
        self.indexRelation.as_ref().expect("index scan parked (skeleton)")
    }
}

// ScanKeyData is droppy (sk_func.fn_extra): plain reserve, not arena helpers.
fn skey_vec<'mcx>(mcx: Mcx<'mcx>, n: usize) -> PgResult<PgVec<'mcx, ScanKeyData>> {
    let mut v = PgVec::new_in(mcx);
    v.try_reserve_exact(n)
        .map_err(|_| Box::new(mcx.oom(n * core::mem::size_of::<ScanKeyData>())))?;
    v.resize(n, ScanKeyData::empty());
    Ok(v)
}

/// C RelationGetIndexScan; recovery state threaded in from above xact.
pub fn relation_get_index_scan<'mcx>(
    mcx: Mcx<'mcx>,
    indexRelation: &Relation<'mcx>,
    nkeys: i32,
    norderbys: i32,
    opaque: IndexScanOpaque<'mcx>,
    xactStartedInRecovery: bool,
) -> PgResult<IndexScanDescData<'mcx>> {
    Ok(IndexScanDescData {
        heapRelation: None,
        indexRelation: Some(indexRelation.alias()),
        xs_snapshot: None,
        numberOfKeys: nkeys,
        numberOfOrderBys: norderbys,
        keyData: skey_vec(mcx, nkeys.max(0) as usize)?,
        orderByData: skey_vec(mcx, norderbys.max(0) as usize)?,
        parallel_scan: None,
        xs_want_itup: false,
        xs_itup: None,
        xs_itupdesc: None,
        xs_temp_snap: false,
        xs_temp_snapshot: None,
        kill_prior_tuple: false,
        // In recovery killed-tuple hints are ignored (standby xmin skew).
        ignore_killed_tuples: !xactStartedInRecovery,
        xactStartedInRecovery,
        opaque,
        xs_heaptid: ItemPointerData::invalid(),
        xs_heap_continue: false,
        xs_heapfetch: None,
        xs_recheck: false,
        xs_orderbyvals: {
            let mut v = PgVec::new_in(mcx);
            v.resize(norderbys.max(0) as usize, datum::Datum::null());
            v
        },
        xs_orderbynulls: {
            let mut v = PgVec::new_in(mcx);
            v.resize(norderbys.max(0) as usize, true);
            v
        },
        xs_recheckorderby: false,
        xs_prefetch: IndexPrefetchState::reset(),
        xs_pgstat_index_tuples: 0,
        xs_pgstat_heap_fetches: 0,
        xs_pgstat_index_scans: 0,
        xs_nsearches: 0,
    })
}
