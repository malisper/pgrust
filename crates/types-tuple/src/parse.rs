//! Parser-facing vocabulary: currently only `QueryEnvironment`.

use alloc::vec::Vec;

use crate::access::EphemeralNamedRelationData;

/// `QueryEnvironment` (`utils/queryenvironment.h`) — private state of a query
/// environment; the struct is forward-declared in the header and defined in
/// `backend/utils/misc/queryenvironment.c`. Reached via
/// `Option<Box<QueryEnvironment>>`.
#[derive(Clone, Debug, Default)]
pub struct QueryEnvironment {
    /// list of `EphemeralNamedRelation`s registered in this environment
    pub namedRelList: Vec<EphemeralNamedRelationData>,
}
