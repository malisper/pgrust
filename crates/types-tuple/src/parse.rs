//! Parser-facing vocabulary: currently only `QueryEnvironment`.

use mcx::{Mcx, PgVec};

use crate::access::EphemeralNamedRelationData;

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
        QueryEnvironment { namedRelList: PgVec::new_in(mcx) }
    }
}
