//! Ephemeral-named-relation vocabulary (`utils/queryenvironment.h`).
//!
//! These types live here (not in `types-tuple`) because an ENR's
//! execution-time payload is a [`Tuplestorestate`]: C declares `reldata` as a
//! `void *` only to dodge a header dependency on `tuplestore.h` — SPI's
//! transition-table registration stores a `Tuplestorestate *` there and
//! `nodeNamedtuplestorescan.c` casts it back. Per docs/types.md rule 6 the
//! layering moves up to where the payload lives instead of encoding the C
//! header workaround as a fake opaque type.

use ::mcx::{alloc_in, Mcx, PgBox, PgString, PgVec};
use ::types_core::primitive::Oid;
use ::types_error::PgResult;
use ::types_tuple::heaptuple::TupleDesc;

use crate::funcapi::Tuplestorestate;

/// `EphemeralNameRelationType` (`utils/queryenvironment.h`).
#[repr(u32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EphemeralNameRelationType {
    /// `ENR_NAMED_TUPLESTORE` — named tuplestore relation; e.g., deltas.
    NamedTuplestore = 0,
}

/// `ENR_NAMED_TUPLESTORE` (`utils/queryenvironment.h`).
pub const ENR_NAMED_TUPLESTORE: EphemeralNameRelationType =
    EphemeralNameRelationType::NamedTuplestore;

/// `EphemeralNamedRelationMetadataData` (`utils/queryenvironment.h`) —
/// metadata for an ephemeral named relation. Exactly one of `reliddesc` /
/// `tupdesc` is filled (a relation OID whose descriptor is read from the
/// catalogs, or an inline tuple descriptor). The owned `name`/`tupdesc` are
/// context-allocated (C pallocs them in the registering caller's context).
#[derive(Debug)]
pub struct EphemeralNamedRelationMetadataData<'mcx> {
    /// name used to identify the relation
    pub name: Option<PgString<'mcx>>,
    /// OID of relation to get the TupleDesc from
    pub reliddesc: Oid,
    /// inline TupleDesc, if relid not used
    pub tupdesc: TupleDesc<'mcx>,
    pub enrtype: EphemeralNameRelationType,
    /// estimated number of tuples
    pub enrtuples: f64,
}

impl EphemeralNamedRelationMetadataData<'_> {
    /// Deep copy into `mcx`.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<EphemeralNamedRelationMetadataData<'b>> {
        Ok(EphemeralNamedRelationMetadataData {
            name: match &self.name {
                Some(n) => Some(n.clone_in(mcx)?),
                None => None,
            },
            reliddesc: self.reliddesc,
            tupdesc: match &self.tupdesc {
                Some(td) => Some(alloc_in(mcx, td.clone_in(mcx)?)?),
                None => None,
            },
            enrtype: self.enrtype,
            enrtuples: self.enrtuples,
        })
    }
}

pub type EphemeralNamedRelationMetadata<'mcx> =
    Option<PgBox<'mcx, EphemeralNamedRelationMetadataData<'mcx>>>;

/// `EphemeralNamedRelationData` (`utils/queryenvironment.h`).
#[derive(Debug)]
pub struct EphemeralNamedRelationData<'mcx> {
    pub md: EphemeralNamedRelationMetadataData<'mcx>,
    /// `void *reldata` — structure for execution-time access to data: in
    /// practice a [`Tuplestorestate`] (SPI registers transition tables with
    /// one; `nodeNamedtuplestorescan.c` reads it back). Can be left `None`
    /// (the C NULL) if the ENR is intended exclusively for planning purposes.
    pub reldata: Option<PgBox<'mcx, Tuplestorestate<'mcx>>>,
}

pub type EphemeralNamedRelation<'mcx> = Option<PgBox<'mcx, EphemeralNamedRelationData<'mcx>>>;

/// `QueryEnvironment` (`utils/queryenvironment.h`) — private state of a query
/// environment; the struct is forward-declared in the header and defined in
/// `backend/utils/misc/queryenvironment.c`.
///
/// C allocates it with `palloc0` in the caller's current context; here the
/// ENR list is context-allocated through the `Mcx` handle the constructor
/// receives, so the environment cannot outlive that context and its bytes
/// show up in the context's accounting.
#[derive(Debug)]
pub struct QueryEnvironment<'mcx> {
    /// list of `EphemeralNamedRelation`s registered in this environment
    pub namedRelList: PgVec<'mcx, EphemeralNamedRelationData<'mcx>>,
}

impl<'mcx> QueryEnvironment<'mcx> {
    pub fn new_in(mcx: Mcx<'mcx>) -> Self {
        QueryEnvironment {
            namedRelList: PgVec::new_in(mcx),
        }
    }

    /// Deep-copy the environment's ENR list into `mcx` for use by a child
    /// `ParseState` (C aliases the parent's `QueryEnvironment *`; the owned model
    /// holds it by value, so a `make_parsestate(parent)` child copies it).
    ///
    /// Only the per-ENR **metadata** (`name` / `reliddesc` / `tupdesc` / type /
    /// tuple estimate) is copied: that is the entirety of what parse analysis
    /// reads (ENRs are looked up by name in `parse_enr`, and the RTE is built
    /// from the metadata's reliddesc/tupdesc). The `reldata` (a live
    /// `Tuplestorestate`) is an *execution*-time resource — never touched during
    /// parse analysis — so the child carries `reldata: None`, observationally
    /// identical for analysis. A child `ParseState` drops at the end of analysis,
    /// well before any executor would read `reldata`.
    pub fn clone_for_child<'b>(&self, mcx: Mcx<'b>) -> PgResult<QueryEnvironment<'b>> {
        let mut namedRelList = PgVec::new_in(mcx);
        namedRelList
            .try_reserve(self.namedRelList.len())
            .map_err(|_| mcx.oom(self.namedRelList.len()))?;
        for enr in self.namedRelList.iter() {
            namedRelList.push(EphemeralNamedRelationData {
                md: enr.md.clone_in(mcx)?,
                reldata: None,
            });
        }
        Ok(QueryEnvironment { namedRelList })
    }
}
