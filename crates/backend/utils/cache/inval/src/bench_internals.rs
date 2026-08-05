// Bench-only hooks at C's static-fn boundaries (inval.c:604/682, 566): the
// crate-internal registration path has no public single-message entry, and
// the paired C reference must start at the same boundary.
use types_core::Oid;
use types_error::PgResult;

use crate::registration::{prepare_invalidation_state, register_catcache_invalidation};
use crate::with_state;

pub fn bench_register_catcache_invalidation(
    cache_id: i32,
    hash_value: u32,
    db_id: Oid,
) -> PgResult<()> {
    with_state(|state| {
        let mcx = state.mcx;
        let info = prepare_invalidation_state(state)?;
        register_catcache_invalidation(mcx, state, info, cache_id, hash_value, db_id)
    })
}

pub fn bench_process_current_locally() -> PgResult<()> {
    crate::eoxact::process_group_locally(|state| {
        Some(state.trans_stack.last()?.ii.current_cmd_invalid_msgs)
    })
}

pub fn bench_current_group_len() -> usize {
    with_state(|state| {
        state
            .trans_stack
            .last()
            .map(|top| top.ii.current_cmd_invalid_msgs.num_in_group())
            .unwrap_or(0)
    })
}
