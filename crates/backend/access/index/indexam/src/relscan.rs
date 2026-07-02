use std::rc::Rc;

use ::mcx::{PgBox, PgVec};
use ::types_core::{Oid, BTREE_AM_OID};
use ::types_nbtree::BTScanOpaqueData;
use ::types_rel::Relation;
use ::types_scan::scankey::ScanKeyData;
use ::types_snapshot::SnapshotData;
use ::types_tuple::itemptr::ItemPointerData;

#[cfg(test)]
pub const MOCK_AM_OID: Oid = 9999;

// C's IndexAmRoutine vtable as rule-4 enum dispatch, resolved once from
// pg_class.relam; property flags mirror each handler's am* fields.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IndexAmKind {
    Btree,
    #[cfg(test)]
    Mock,
}

impl IndexAmKind {
    pub fn from_relam(relam: Oid) -> Self {
        match relam {
            BTREE_AM_OID => IndexAmKind::Btree,
            #[cfg(test)]
            MOCK_AM_OID => IndexAmKind::Mock,
            other => unported_index_am(other),
        }
    }

    pub const fn ampredlocks(self) -> bool {
        match self {
            IndexAmKind::Btree => true,
            #[cfg(test)]
            IndexAmKind::Mock => true,
        }
    }

    pub const fn has_ammarkpos(self) -> bool {
        match self {
            IndexAmKind::Btree => true,
            #[cfg(test)]
            IndexAmKind::Mock => true,
        }
    }

    pub const fn has_amrestrpos(self) -> bool {
        match self {
            IndexAmKind::Btree => true,
            #[cfg(test)]
            IndexAmKind::Mock => false,
        }
    }

    pub const fn has_aminsertcleanup(self) -> bool {
        match self {
            IndexAmKind::Btree => false,
            #[cfg(test)]
            IndexAmKind::Mock => false,
        }
    }
}

#[cold]
#[inline(never)]
fn unported_index_am(relam: Oid) -> ! {
    panic!("unported: index AM {relam} (IndexAmKind covers btree only)")
}

// C's void *opaque: the AM extension of the scan, tagged by AM kind.
pub enum IndexScanOpaque<'mcx> {
    Btree(PgBox<'mcx, BTScanOpaqueData<'mcx>>),
    #[cfg(test)]
    Mock(MockOpaque),
}

impl IndexScanOpaque<'_> {
    #[inline]
    pub fn kind(&self) -> IndexAmKind {
        match self {
            IndexScanOpaque::Btree(_) => IndexAmKind::Btree,
            #[cfg(test)]
            IndexScanOpaque::Mock(_) => IndexAmKind::Mock,
        }
    }
}

#[cfg(test)]
#[derive(Default)]
pub struct MockOpaque {
    pub tids: Vec<ItemPointerData>,
    pub next: usize,
    pub kill_seen: Vec<bool>,
    pub rescans: u32,
    pub markpos_calls: u32,
}

// tableam.h base; the heap AM's xs_cbuf extension arrives with the tableam port.
pub struct IndexFetchTableData<'mcx> {
    pub rel: Relation<'mcx>,
    #[cfg(test)]
    pub mock_fetch: Vec<(bool, bool, bool)>,
    #[cfg(test)]
    pub resets: u32,
}

// relscan.h trimmed to the amgettuple shape (parallel_scan/instrument/xs_itup
// land with their owners). A rule-3 resource owner: lives by value in the
// executor node, never in an arena; Rc on the snapshot is snapmgr's refcount.
pub struct IndexScanDescData<'mcx> {
    pub heapRelation: Option<Relation<'mcx>>,
    pub indexRelation: Relation<'mcx>,
    pub xs_snapshot: Option<Rc<SnapshotData<'mcx>>>,
    pub numberOfKeys: i32,
    pub numberOfOrderBys: i32,
    pub keyData: PgVec<'mcx, ScanKeyData>,
    pub orderByData: PgVec<'mcx, ScanKeyData>,

    pub xs_want_itup: bool,
    pub xs_temp_snap: bool,
    pub kill_prior_tuple: bool,
    pub ignore_killed_tuples: bool,
    pub xactStartedInRecovery: bool,

    pub opaque: IndexScanOpaque<'mcx>,

    pub xs_heaptid: ItemPointerData,
    pub xs_heap_continue: bool,
    pub xs_heapfetch: Option<IndexFetchTableData<'mcx>>,
    pub xs_recheck: bool,

    // pgstat_relation unported: the counts accrue here under C's probe shape.
    pub xs_pgstat_index_tuples: u64,
    pub xs_pgstat_heap_fetches: u64,
}
