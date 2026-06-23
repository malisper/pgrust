//! Port of PostgreSQL's query environment
//! (`src/backend/utils/misc/queryenvironment.c`).
//!
//! Query environment, to store context-specific values like ephemeral named
//! relations. Initial use is for named tuplestores for delta information from
//! "normal" relations.
//!
//! The initial implementation uses a list because the number of such relations
//! in any one context is expected to be very small. If that becomes a
//! performance problem, the implementation can be changed with no other impact
//! on callers, since this is an opaque structure. This is the reason to
//! require a create function.
//!
//! The C struct wraps a `List *namedRelList` of heap pointers; here the
//! environment directly owns its ENRs in a `Vec<EphemeralNamedRelationData>`,
//! so `register_ENR` takes the ENR by value, the lookup functions borrow the
//! match, and the C NULL returns become `None`.

#![no_std]
#![allow(non_snake_case)]

extern crate alloc;

use mcx::{Mcx, PgBox};
use ::types_core::primitive::InvalidOid;
use ::types_error::PgResult;
use ::nodes::queryenvironment::{
    EphemeralNamedRelationData, EphemeralNamedRelationMetadataData, QueryEnvironment,
};
use ::types_storage::lock::NoLock;
use ::types_tuple::heaptuple::TupleDescData;

/// Install this crate's seam implementations. This unit owns no seams.
pub fn init_seams() {}

/// `create_queryEnv(void)` — allocate a fresh, empty query environment.
///
/// C `palloc0(sizeof(QueryEnvironment))` in `CurrentMemoryContext`; per the
/// mcx translation rule the caller passes the context handle instead, and the
/// environment's allocations are tied to (and accounted in) that context.
pub fn create_queryEnv(mcx: Mcx<'_>) -> QueryEnvironment<'_> {
    QueryEnvironment::new_in(mcx)
}

/// `get_visible_ENR_metadata(queryEnv, refname)` — return the metadata of the
/// ENR registered under `refname`, or `None`.
///
/// C returns NULL when `queryEnv == NULL` (here `None`) and otherwise the
/// borrowed `&(enr->md)` with zero allocation; same here.
pub fn get_visible_ENR_metadata<'e, 'mcx>(
    query_env: Option<&'e QueryEnvironment<'mcx>>,
    refname: &str,
) -> Option<&'e EphemeralNamedRelationMetadataData<'mcx>> {
    // Assert(refname != NULL) — `&str` is non-null by construction.
    let query_env = query_env?;
    get_ENR(query_env, refname).map(|enr| &enr.md)
}

/// `register_ENR(queryEnv, enr)` — register a named relation for use in the
/// given environment.
///
/// If this is intended exclusively for planning purposes, the `reldata` field
/// can be left `None` (C: NULL `tstate`).
///
/// Fallible because C's `lappend` pallocs, which can `ereport(ERROR)` on OOM.
pub fn register_ENR<'mcx>(
    query_env: &mut QueryEnvironment<'mcx>,
    enr: EphemeralNamedRelationData<'mcx>,
) -> PgResult<()> {
    // Assert(enr != NULL) — `enr` is owned, never null.
    // Assert(get_ENR(queryEnv, enr->md.name) == NULL)
    debug_assert!(
        enr.md
            .name
            .as_deref()
            .map(|name| get_ENR(query_env, name).is_none())
            .unwrap_or(true),
        "register_ENR: duplicate ephemeral named relation"
    );

    let mcx = *query_env.namedRelList.allocator();
    query_env
        .namedRelList
        .try_reserve(1)
        .map_err(|_| mcx.oom(core::mem::size_of::<EphemeralNamedRelationData>()))?;
    query_env.namedRelList.push(enr);
    Ok(())
}

/// `unregister_ENR(queryEnv, name)` — unregister an ephemeral relation by
/// name. This will probably be a rarely used function, but seems like it
/// should be provided "just in case".
pub fn unregister_ENR(query_env: &mut QueryEnvironment<'_>, name: &str) {
    if let Some(idx) = enr_index(query_env, name) {
        query_env.namedRelList.remove(idx);
    }
}

/// `get_ENR(queryEnv, name)` — return an ENR if there is a name match in the
/// given collection. It must quietly return `None` if no match is found.
///
/// C also returns NULL when `queryEnv == NULL`; here the caller passes a
/// borrow, so non-null is implied.
pub fn get_ENR<'e, 'mcx>(
    query_env: &'e QueryEnvironment<'mcx>,
    name: &str,
) -> Option<&'e EphemeralNamedRelationData<'mcx>> {
    // Assert(name != NULL) — `&str` is non-null.
    enr_index(query_env, name).map(|idx| &query_env.namedRelList[idx])
}

/// Shared name-match walk used by `get_ENR` / `unregister_ENR`. Mirrors C's
/// `foreach` + `strcmp(enr->md.name, name) == 0`.
fn enr_index(query_env: &QueryEnvironment<'_>, name: &str) -> Option<usize> {
    query_env
        .namedRelList
        .iter()
        .position(|enr| enr.md.name.as_deref() == Some(name))
}

/// The `TupleDesc` return of [`ENRMetadataGetTupDesc`]: the inline-tupdesc
/// path borrows the stored descriptor (C returns `enrmd->tupdesc` without
/// allocating); the catalog path owns the copy the relcache seam allocates in
/// `mcx`.
#[derive(Debug)]
pub enum EnrTupleDesc<'e, 'mcx> {
    Borrowed(&'e TupleDescData<'mcx>),
    Owned(PgBox<'mcx, TupleDescData<'mcx>>),
}

impl<'mcx> core::ops::Deref for EnrTupleDesc<'_, 'mcx> {
    type Target = TupleDescData<'mcx>;

    fn deref(&self) -> &Self::Target {
        match self {
            EnrTupleDesc::Borrowed(td) => td,
            EnrTupleDesc::Owned(td) => td,
        }
    }
}

/// `ENRMetadataGetTupDesc(enrmd)` — gets the `TupleDesc` for an Ephemeral
/// Named Relation, based on which field was filled.
///
/// When the `TupleDesc` is based on a relation from the catalogs, we count on
/// that relation being used at the same time, so that appropriate locks will
/// already be held. Locking here would be too late anyway.
pub fn ENRMetadataGetTupDesc<'e, 'mcx>(
    mcx: Mcx<'mcx>,
    enrmd: &'e EphemeralNamedRelationMetadataData<'mcx>,
) -> PgResult<Option<EnrTupleDesc<'e, 'mcx>>> {
    // One, and only one, of these fields must be filled.
    debug_assert!(
        (enrmd.reliddesc == InvalidOid) != enrmd.tupdesc.is_none(),
        "ENRMetadataGetTupDesc: exactly one of reliddesc/tupdesc must be set"
    );

    if let Some(tupdesc) = &enrmd.tupdesc {
        // tupdesc = enrmd->tupdesc; — the stored descriptor, no copy.
        Ok(Some(EnrTupleDesc::Borrowed(tupdesc)))
    } else {
        // relation = table_open(enrmd->reliddesc, NoLock);
        // tupdesc = relation->rd_att;
        // table_close(relation, NoLock);
        let relation = table::table_open(mcx, enrmd.reliddesc, NoLock)?;
        let tupdesc = ::mcx::alloc_in(mcx, relation.rd_att.clone_in(mcx)?)?;
        table::table_close(relation, NoLock)?;
        Ok(Some(EnrTupleDesc::Owned(tupdesc)))
    }
}

#[cfg(test)]
mod tests;
