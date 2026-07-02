use mcx::{Mcx, PgVec};
use types_core::Oid;
use types_error::PgResult;
use types_rel::RelationData;
use types_storage::PrepareToInvalidateCacheTuple as InvalRequest;
use types_tuple::HeapTupleData;

use crate::compute::CCFastKind;
use crate::graph::compute_tuple_hash_value;
use crate::{with_state, CATCACHE_MAXKEYS};

struct CacheProbe {
    id: i32,
    cc_nkeys: i32,
    cc_keyno: [i32; CATCACHE_MAXKEYS],
    cc_kind: [CCFastKind; CATCACHE_MAXKEYS],
    cc_relisshared: bool,
}

/// `PrepareToInvalidateCacheTuple`, the per-catcache callback inverted into
/// returned requests.
pub fn PrepareToInvalidateCacheTuple<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &RelationData<'_>,
    tuple: &HeapTupleData<'_>,
    newtuple: Option<&HeapTupleData<'_>>,
) -> PgResult<PgVec<'mcx, InvalRequest>> {
    let reloid = relation.rd_id;

    let mut matching = [0i32; crate::graph::MAX_CACHES];
    let n = with_state(|st| {
        let mut n = 0;
        for c in st.caches.iter().flatten() {
            if c.cc_reloid == reloid {
                matching[n] = c.id;
                n += 1;
            }
        }
        n
    });

    /* just in case cache hasn't finished initialization yet */
    for &id in &matching[..n] {
        if !with_state(|st| st.cache(id).initialized) {
            crate::init::catalog_cache_initialize_cache(id)?;
        }
    }

    let mut requests: PgVec<'mcx, InvalRequest> = mcx::vec_with_capacity_in(mcx, n * 2)?;
    for &id in &matching[..n] {
        let probe = with_state(|st| {
            let c = st.cache(id);
            CacheProbe {
                id: c.id,
                cc_nkeys: c.cc_nkeys,
                cc_keyno: c.cc_keyno,
                cc_kind: c.cc_kind,
                cc_relisshared: c.cc_relisshared,
            }
        });
        let tupdesc = crate::init::cache_tupdesc(id).expect("initialized above");
        let hashvalue =
            compute_tuple_hash_value(&probe.cc_kind, probe.cc_nkeys, &probe.cc_keyno, tupdesc, tuple);
        let dbid: Oid = if probe.cc_relisshared {
            0
        } else {
            init_small::globals::MyDatabaseId()
        };
        requests.push(InvalRequest { cache_id: probe.id, hash_value: hashvalue, db_id: dbid });

        if let Some(newtuple) = newtuple {
            let newhash = compute_tuple_hash_value(
                &probe.cc_kind,
                probe.cc_nkeys,
                &probe.cc_keyno,
                tupdesc,
                newtuple,
            );
            if newhash != hashvalue {
                requests.push(InvalRequest { cache_id: probe.id, hash_value: newhash, db_id: dbid });
            }
        }
    }
    Ok(requests)
}
