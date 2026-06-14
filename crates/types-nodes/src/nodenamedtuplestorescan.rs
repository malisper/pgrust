//! NamedTuplestoreScan node vocabulary (nodes/plannodes.h /
//! executor/execnodes.h).
//!
//! A `NamedTuplestoreScan` scans an Ephemeral Named Relation (ENR) backed by a
//! tuplestore — e.g. the transition tables of `AFTER` triggers. The named
//! tuplestore is owned by the query environment (`get_ENR`); this node holds
//! its own read pointer into it.

use core::ptr::NonNull;

use mcx::{Mcx, PgString};
use types_error::PgResult;
use types_tuple::heaptuple::TupleDesc;

use crate::execnodes::{PlanStateData, ScanStateData};
use crate::funcapi::Tuplestorestate;
use crate::nodeindexscan::Scan;
use crate::nodes::NodeTag;

/// `T_NamedTuplestoreScanState` (nodes/nodetags.h) — the executor-state node
/// tag for a NamedTuplestoreScan node. Verified against PostgreSQL 18.3
/// (`T_NamedTuplestoreScanState`).
pub const T_NamedTuplestoreScanState: NodeTag = NodeTag(416);

/// `NamedTuplestoreScan` plan node (plannodes.h):
///
/// ```c
/// typedef struct NamedTuplestoreScan {
///     Scan        scan;
///     char       *enrname;    /* Name given to Ephemeral Named Relation */
/// } NamedTuplestoreScan;
/// ```
#[derive(Debug, Default)]
pub struct NamedTuplestoreScan<'mcx> {
    /// `Scan scan` — the abstract scan-plan base (embeds `Plan`).
    pub scan: Scan<'mcx>,
    /// `char *enrname` — name given to the Ephemeral Named Relation. `None` =
    /// the C `NULL`.
    pub enrname: Option<PgString<'mcx>>,
}

impl NamedTuplestoreScan<'_> {
    /// Deep copy into `mcx` (C: `copyObject` shape). Fallible: copying
    /// allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<NamedTuplestoreScan<'b>> {
        Ok(NamedTuplestoreScan {
            scan: self.scan.clone_in(mcx)?,
            enrname: match &self.enrname {
                Some(n) => Some(n.clone_in(mcx)?),
                None => None,
            },
        })
    }
}

/// `NamedTuplestoreScanState` (execnodes.h):
///
/// ```c
/// typedef struct NamedTuplestoreScanState {
///     ScanState   ss;             /* its first field is NodeTag */
///     int         readptr;        /* index of my tuplestore read pointer */
///     TupleDesc   tupdesc;        /* format of the tuples in the tuplestore */
///     Tuplestorestate *relation;  /* the rows */
/// } NamedTuplestoreScanState;
/// ```
///
/// The leading [`ScanStateData`] head's first member is a `NodeTag`, so a
/// `NamedTuplestoreScanState` is also a valid `Node` / `PlanState`. `relation`
/// mirrors the C `Tuplestorestate *` exactly: a non-owning pointer aliasing the
/// query-environment-owned ENR tuplestore (`enr->reldata`). This node never
/// frees it (see the `XXX` comment in `nodeNamedtuplestorescan.c`), so it is a
/// raw alias, not an owned `PgBox`. `None` is the pre-init C `NULL`.
#[derive(Debug)]
pub struct NamedTuplestoreScanState<'mcx> {
    /// `ScanState ss` — its first field is `NodeTag`.
    pub ss: ScanStateData<'mcx>,
    /// `int readptr` — index of my tuplestore read pointer.
    pub readptr: i32,
    /// `TupleDesc tupdesc` — format of the tuples in the tuplestore.
    pub tupdesc: TupleDesc<'mcx>,
    /// `Tuplestorestate *relation` — the rows, aliasing the ENR's tuplestore.
    pub relation: Option<NonNull<Tuplestorestate<'mcx>>>,
}

impl<'mcx> NamedTuplestoreScanState<'mcx> {
    /// `makeNode(NamedTuplestoreScanState)`-shaped construction: a palloc0
    /// state, allocated in `mcx`.
    pub fn new_in(_mcx: Mcx<'mcx>) -> Self {
        NamedTuplestoreScanState {
            ss: ScanStateData::default(),
            readptr: 0,
            tupdesc: None,
            relation: None,
        }
    }

    /// `&node->ss.ps` — the embedded `PlanState` head.
    #[inline]
    pub fn ps(&self) -> &PlanStateData<'mcx> {
        &self.ss.ps
    }

    /// `&mut node->ss.ps`.
    #[inline]
    pub fn ps_mut(&mut self) -> &mut PlanStateData<'mcx> {
        &mut self.ss.ps
    }

    /// `node->relation` as a `&mut Tuplestorestate` — the C dereference of the
    /// stored `Tuplestorestate *`. SAFETY: the pointer aliases the ENR's
    /// tuplestore, which the query environment keeps alive for the duration of
    /// the scan; the node is the only path that dereferences it during a
    /// `tuplestore_*` call, so no concurrent `&mut` exists (matching the C,
    /// where the executor drives one scan node at a time).
    ///
    /// # Safety
    /// The caller must ensure the aliased tuplestore is still live (the ENR has
    /// not been torn down) and that no other live `&mut` to it exists.
    #[inline]
    pub unsafe fn relation_mut(&mut self) -> Option<&mut Tuplestorestate<'mcx>> {
        self.relation.map(|mut p| unsafe { p.as_mut() })
    }
}
