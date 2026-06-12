//! Access-layer vocabulary consumed by the queryenvironment port: lock-mode
//! scalars (`storage/lockdefs.h`) and the ephemeral-named-relation types
//! (`utils/queryenvironment.h`).

use alloc::boxed::Box;
use alloc::string::String;

use types_core::primitive::Oid;

use crate::heaptuple::TupleDesc;

/// `LOCKMODE` (`storage/lockdefs.h`).
pub type LOCKMODE = i32;

/// `NoLock` (`storage/lockdefs.h`) — open a relation without taking a lock
/// (the caller relies on a lock already being held).
pub const NoLock: LOCKMODE = 0;

/// `EphemeralNameRelationType` (`utils/queryenvironment.h`).
pub type EphemeralNameRelationType = u32;
/// `ENR_NAMED_TUPLESTORE` — named tuplestore relation (e.g. a trigger
/// transition table).
pub const ENR_NAMED_TUPLESTORE: EphemeralNameRelationType = 0;

/// `EphemeralNamedRelationMetadataData` (`utils/queryenvironment.h`) —
/// metadata for an ephemeral named relation. Exactly one of `reliddesc` /
/// `tupdesc` is filled (a relation OID whose descriptor is read from the
/// catalogs, or an inline tuple descriptor).
#[derive(Clone, Debug)]
pub struct EphemeralNamedRelationMetadataData {
    /// name used to identify the relation
    pub name: Option<String>,
    /// OID of relation to get the TupleDesc from
    pub reliddesc: Oid,
    /// inline TupleDesc, if relid not used
    pub tupdesc: TupleDesc,
    pub enrtype: EphemeralNameRelationType,
    /// estimated number of tuples
    pub enrtuples: f64,
}

pub type EphemeralNamedRelationMetadata = Option<Box<EphemeralNamedRelationMetadataData>>;

/// `void *reldata` (`utils/queryenvironment.h`) — the execution-time backing
/// payload for a named relation. PostgreSQL declares it as an untyped
/// `void *`, so it stays a minimal constructible handle.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EphemeralRelationData {}

/// `EphemeralNamedRelationData` (`utils/queryenvironment.h`).
#[derive(Clone, Debug)]
pub struct EphemeralNamedRelationData {
    pub md: EphemeralNamedRelationMetadataData,
    /// structure for execution-time access to data; can be left `None` if the
    /// ENR is intended exclusively for planning purposes
    pub reldata: Option<Box<EphemeralRelationData>>,
}

pub type EphemeralNamedRelation = Option<Box<EphemeralNamedRelationData>>;
