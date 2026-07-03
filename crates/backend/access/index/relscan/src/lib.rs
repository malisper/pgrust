//! relscan.h vocabulary shared by indexam (dispatch) and the index AMs —
//! split out so nbtree takes the descriptor without a cycle through indexam.
#![allow(non_snake_case)]

use std::rc::Rc;

use ::mcx::{Mcx, PgBox, PgVec};
use ::gin_vocab::GinScanOpaqueData;
use ::types_core::{Oid, BRIN_AM_OID, BTREE_AM_OID, GIN_AM_OID, GIST_AM_OID, HASH_AM_OID};
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
    Brin,
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
            BRIN_AM_OID => IndexAmKind::Brin,
            #[cfg(feature = "mock")]
            MOCK_AM_OID => IndexAmKind::Mock,
            other => unported_index_am(other),
        }
    }

    pub const fn ampredlocks(self) -> bool {
        match self {
            IndexAmKind::Btree => true,
            IndexAmKind::Hash => true,
            IndexAmKind::Gin => true,
            IndexAmKind::Gist => true,
            IndexAmKind::Brin => false,
            #[cfg(feature = "mock")]
            IndexAmKind::Mock => true,
        }
    }

    pub const fn has_ammarkpos(self) -> bool {
        match self {
            IndexAmKind::Btree => true,
            IndexAmKind::Hash => false,
            IndexAmKind::Gin => false,
            IndexAmKind::Gist => false,
            IndexAmKind::Brin => false,
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
            IndexAmKind::Brin => false,
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
            IndexAmKind::Brin => true,
            #[cfg(feature = "mock")]
            IndexAmKind::Mock => false,
        }
    }
}

#[cold]
#[inline(never)]
fn unported_index_am(relam: Oid) -> ! {
    panic!("unported: index AM {relam} (IndexAmKind covers btree+hash+gin+gist+brin)")
}

pub enum IndexScanOpaque<'mcx> {
    Btree(PgBox<'mcx, BTScanOpaqueData<'mcx>>),
    Hash(PgBox<'mcx, HashScanOpaqueData<'mcx>>),
    Gin(PgBox<'mcx, GinScanOpaqueData>),
    Gist(PgBox<'mcx, GISTScanOpaqueData<'mcx>>),
    Brin(PgBox<'mcx, ::types_brin::BrinOpaque<'mcx>>),
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
            IndexScanOpaque::Brin(_) => IndexAmKind::Brin,
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

// Trimmed to the amgettuple shape; a rule-3 by-value resource owner.
pub struct IndexScanDescData<'mcx> {
    pub heapRelation: Option<Relation<'mcx>>,
    pub indexRelation: Relation<'mcx>,
    pub xs_snapshot: Option<Rc<SnapshotData<'mcx>>>,
    pub numberOfKeys: i32,
    pub numberOfOrderBys: i32,
    pub keyData: PgVec<'mcx, ScanKeyData>,
    pub orderByData: PgVec<'mcx, ScanKeyData>,

    pub xs_want_itup: bool,
    // Points into the AM's page-copy buffer (nbtree currTuples); valid until
    // the next amgettuple/amrescan/amendscan on this descriptor.
    pub xs_itup: Option<core::ptr::NonNull<u8>>,
    pub xs_itupdesc: Option<Rc<TupleDescData<'mcx>>>,
    pub xs_temp_snap: bool,
    pub kill_prior_tuple: bool,
    pub ignore_killed_tuples: bool,
    pub xactStartedInRecovery: bool,

    pub opaque: IndexScanOpaque<'mcx>,

    pub xs_heaptid: ItemPointerData,
    pub xs_heap_continue: bool,
    pub xs_heapfetch: Option<IndexFetchTableData<'mcx>>,
    pub xs_recheck: bool,

    pub xs_pgstat_index_tuples: u64,
    pub xs_pgstat_heap_fetches: u64,
    pub xs_pgstat_index_scans: u64,
    // C IndexScanInstrumentation.nsearches (pgstat-independent; EXPLAIN reads it).
    pub xs_nsearches: u64,
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
        indexRelation: indexRelation.alias(),
        xs_snapshot: None,
        numberOfKeys: nkeys,
        numberOfOrderBys: norderbys,
        keyData: skey_vec(mcx, nkeys.max(0) as usize)?,
        orderByData: skey_vec(mcx, norderbys.max(0) as usize)?,
        xs_want_itup: false,
        xs_itup: None,
        xs_itupdesc: None,
        xs_temp_snap: false,
        kill_prior_tuple: false,
        // In recovery killed-tuple hints are ignored (standby xmin skew).
        ignore_killed_tuples: !xactStartedInRecovery,
        xactStartedInRecovery,
        opaque,
        xs_heaptid: ItemPointerData::invalid(),
        xs_heap_continue: false,
        xs_heapfetch: None,
        xs_recheck: false,
        xs_pgstat_index_tuples: 0,
        xs_pgstat_heap_fetches: 0,
        xs_pgstat_index_scans: 0,
        xs_nsearches: 0,
    })
}
