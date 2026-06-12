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

/* ----------------------------------------------------------------
 * catalog/pg_class.h: relkind / relpersistence vocabulary
 * ---------------------------------------------------------------- */

pub const RELKIND_RELATION: u8 = b'r';
pub const RELKIND_INDEX: u8 = b'i';
pub const RELKIND_SEQUENCE: u8 = b'S';
pub const RELKIND_TOASTVALUE: u8 = b't';
pub const RELKIND_VIEW: u8 = b'v';
pub const RELKIND_MATVIEW: u8 = b'm';
pub const RELKIND_COMPOSITE_TYPE: u8 = b'c';
pub const RELKIND_FOREIGN_TABLE: u8 = b'f';
pub const RELKIND_PARTITIONED_TABLE: u8 = b'p';
pub const RELKIND_PARTITIONED_INDEX: u8 = b'I';

pub const RELPERSISTENCE_PERMANENT: u8 = b'p';
pub const RELPERSISTENCE_UNLOGGED: u8 = b'u';
pub const RELPERSISTENCE_TEMP: u8 = b't';

/* ----------------------------------------------------------------
 * nodes/primnodes.h: RangeVar
 * ---------------------------------------------------------------- */

/// `RangeVar` (`nodes/primnodes.h`) — a qualified relation name as written in
/// a query, trimmed to the fields the relation-open paths consume. The C
/// `char *` name fields become owned strings (`relname` is never NULL in a
/// well-formed parse node; `catalogname`/`schemaname` may be).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RangeVar {
    /// the catalog (database) name, or `None`
    pub catalogname: Option<alloc::string::String>,
    /// the schema name, or `None`
    pub schemaname: Option<alloc::string::String>,
    /// the relation/sequence name
    pub relname: alloc::string::String,
    /// expand rel by inheritance? recursively act on children?
    pub inh: bool,
    /// see `RELPERSISTENCE_*`
    pub relpersistence: u8,
    /// token location, or -1 if unknown
    pub location: i32,
}
