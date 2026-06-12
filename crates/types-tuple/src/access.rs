//! Access-layer vocabulary consumed by the queryenvironment port: lock-mode
//! scalars (`storage/lockdefs.h`) and the ephemeral-named-relation types
//! (`utils/queryenvironment.h`).

use mcx::{alloc_in, Mcx, PgBox, PgString};
use types_core::primitive::Oid;
use types_error::PgResult;

use crate::heaptuple::TupleDesc;

/// `LOCKMODE` (`storage/lockdefs.h`).
pub type LOCKMODE = i32;

/// `NoLock` (`storage/lockdefs.h`) — open a relation without taking a lock
/// (the caller relies on a lock already being held).
pub const NoLock: LOCKMODE = 0;

/// `AccessShareLock` (`storage/lockdefs.h`) — SELECT.
pub const AccessShareLock: LOCKMODE = 1;

/// `RowExclusiveLock` (`storage/lockdefs.h`) — INSERT, UPDATE, DELETE.
pub const RowExclusiveLock: LOCKMODE = 3;

/// `EphemeralNameRelationType` (`utils/queryenvironment.h`).
pub type EphemeralNameRelationType = u32;
/// `ENR_NAMED_TUPLESTORE` — named tuplestore relation (e.g. a trigger
/// transition table).
pub const ENR_NAMED_TUPLESTORE: EphemeralNameRelationType = 0;

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

/// `void *reldata` (`utils/queryenvironment.h`) — the execution-time backing
/// payload for a named relation. PostgreSQL declares it as an untyped
/// `void *`, so it stays a minimal constructible handle.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EphemeralRelationData {}

/// `EphemeralNamedRelationData` (`utils/queryenvironment.h`).
#[derive(Debug)]
pub struct EphemeralNamedRelationData<'mcx> {
    pub md: EphemeralNamedRelationMetadataData<'mcx>,
    /// structure for execution-time access to data; can be left `None` if the
    /// ENR is intended exclusively for planning purposes
    pub reldata: Option<PgBox<'mcx, EphemeralRelationData>>,
}

pub type EphemeralNamedRelation<'mcx> = Option<PgBox<'mcx, EphemeralNamedRelationData<'mcx>>>;
