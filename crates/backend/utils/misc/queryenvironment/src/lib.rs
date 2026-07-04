#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]

use std::rc::Rc;

use mcx::{Mcx, PgString, PgVec};
use types_core::{InvalidOid, Oid};
use types_error::PgResult;
use types_portal::TuplestoreHandle;
use types_rel::NoLock;
use types_tuple::TupleDescData;

pub mod hold;

#[cfg(test)]
mod tests;

pub fn init_seams() {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum EphemeralNameRelationType {
    ENR_NAMED_TUPLESTORE = 0,
}

pub use EphemeralNameRelationType::ENR_NAMED_TUPLESTORE;

// Exactly one of reliddesc/tupdesc is set: a catalog relation OID whose
// descriptor is read via the relcache, or an inline descriptor.
#[derive(Debug)]
pub struct EphemeralNamedRelationMetadataData<'mcx> {
    pub name: PgString<'mcx>,
    pub reliddesc: Oid,
    pub tupdesc: Option<Rc<TupleDescData<'mcx>>>,
    pub enrtype: EphemeralNameRelationType,
    pub enrtuples: f64,
}

#[derive(Debug)]
pub struct EphemeralNamedRelationData<'mcx> {
    pub md: EphemeralNamedRelationMetadataData<'mcx>,
    // C `void *reldata`: identity token for the not-yet-ported tuplestore
    // owner (cf. types_portal); NULL when registered for planning only.
    pub reldata: TuplestoreHandle,
}

#[derive(Debug)]
pub struct QueryEnvironment<'mcx> {
    pub namedRelList: PgVec<'mcx, EphemeralNamedRelationData<'mcx>>,
}

pub fn create_queryEnv(mcx: Mcx<'_>) -> QueryEnvironment<'_> {
    QueryEnvironment {
        namedRelList: PgVec::new_in(mcx),
    }
}

pub fn get_visible_ENR_metadata<'e, 'mcx>(
    queryEnv: Option<&'e QueryEnvironment<'mcx>>,
    refname: &str,
) -> Option<&'e EphemeralNamedRelationMetadataData<'mcx>> {
    get_ENR(queryEnv?, refname).map(|enr| &enr.md)
}

pub fn register_ENR<'mcx>(
    queryEnv: &mut QueryEnvironment<'mcx>,
    enr: EphemeralNamedRelationData<'mcx>,
) -> PgResult<()> {
    debug_assert!(
        get_ENR(queryEnv, &enr.md.name).is_none(),
        "register_ENR: duplicate ephemeral named relation"
    );
    let mcx = *queryEnv.namedRelList.allocator();
    queryEnv
        .namedRelList
        .try_reserve(1)
        .map_err(|_| mcx.oom(core::mem::size_of::<EphemeralNamedRelationData>()))?;
    queryEnv.namedRelList.push(enr);
    Ok(())
}

pub fn unregister_ENR(queryEnv: &mut QueryEnvironment<'_>, name: &str) {
    if let Some(idx) = enr_index(queryEnv, name) {
        queryEnv.namedRelList.remove(idx);
    }
}

pub fn get_ENR<'e, 'mcx>(
    queryEnv: &'e QueryEnvironment<'mcx>,
    name: &str,
) -> Option<&'e EphemeralNamedRelationData<'mcx>> {
    enr_index(queryEnv, name).map(|idx| &queryEnv.namedRelList[idx])
}

fn enr_index(queryEnv: &QueryEnvironment<'_>, name: &str) -> Option<usize> {
    queryEnv
        .namedRelList
        .iter()
        .position(|enr| enr.md.name == name)
}

// Caller already holds locks on the reliddesc relation (locking here would be
// too late anyway); the Rc shares mirror C's borrowed TupleDesc returns.
pub fn ENRMetadataGetTupDesc<'mcx>(
    mcx: Mcx<'mcx>,
    enrmd: &EphemeralNamedRelationMetadataData<'mcx>,
) -> PgResult<Rc<TupleDescData<'mcx>>> {
    debug_assert!(
        (enrmd.reliddesc == InvalidOid) != enrmd.tupdesc.is_none(),
        "ENRMetadataGetTupDesc: exactly one of reliddesc/tupdesc must be set"
    );
    match &enrmd.tupdesc {
        Some(tupdesc) => Ok(Rc::clone(tupdesc)),
        None => {
            let relation = table::table_open(mcx, enrmd.reliddesc, NoLock)?;
            let tupdesc = Rc::clone(&relation.rd_att);
            table::table_close(relation, NoLock)?;
            Ok(tupdesc)
        }
    }
}
