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

use mcx::{alloc_in, Mcx};
use types_core::primitive::InvalidOid;
use types_error::PgResult;
use types_tuple::access::{
    EphemeralNamedRelationData, EphemeralNamedRelationMetadata,
    EphemeralNamedRelationMetadataData, NoLock,
};
use types_tuple::heaptuple::TupleDesc;
use types_tuple::parse::QueryEnvironment;

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
/// C returns NULL when `queryEnv == NULL` (here `None`). On a hit C returns
/// `&(enr->md)`; the owned signature hands back an owned
/// `EphemeralNamedRelationMetadata`, so the matched `md` is cloned into `mcx`
/// (fallible: the copy allocates).
pub fn get_visible_ENR_metadata<'mcx>(
    mcx: Mcx<'mcx>,
    query_env: Option<&QueryEnvironment<'_>>,
    refname: &str,
) -> PgResult<EphemeralNamedRelationMetadata<'mcx>> {
    // Assert(refname != NULL) — `&str` is non-null by construction.

    let Some(query_env) = query_env else {
        return Ok(None);
    };

    match get_ENR(query_env, refname) {
        Some(enr) => Ok(Some(alloc_in(mcx, enr.md.clone_in(mcx)?)?)),
        None => Ok(None),
    }
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

/// `ENRMetadataGetTupDesc(enrmd)` — gets the `TupleDesc` for an Ephemeral
/// Named Relation, based on which field was filled.
///
/// When the `TupleDesc` is based on a relation from the catalogs, we count on
/// that relation being used at the same time, so that appropriate locks will
/// already be held. Locking here would be too late anyway.
pub fn ENRMetadataGetTupDesc<'mcx>(
    mcx: Mcx<'mcx>,
    enrmd: &EphemeralNamedRelationMetadataData<'_>,
) -> types_error::PgResult<TupleDesc<'mcx>> {
    // One, and only one, of these fields must be filled.
    debug_assert!(
        (enrmd.reliddesc == InvalidOid) != enrmd.tupdesc.is_none(),
        "ENRMetadataGetTupDesc: exactly one of reliddesc/tupdesc must be set"
    );

    if let Some(tupdesc) = &enrmd.tupdesc {
        Ok(Some(alloc_in(mcx, tupdesc.clone_in(mcx)?)?))
    } else {
        // relation = table_open(enrmd->reliddesc, NoLock);
        // tupdesc = relation->rd_att;
        // table_close(relation, NoLock);
        let relation = backend_access_table_table_seams::table_open::call(enrmd.reliddesc, NoLock)?;
        let tupdesc = backend_utils_cache_relcache_seams::relation_rd_att::call(mcx, &relation)?;
        relation.close(NoLock)?;
        Ok(tupdesc)
    }
}

#[cfg(test)]
mod tests;
