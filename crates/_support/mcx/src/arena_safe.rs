/// # Safety
/// Dropping `T` must reclaim nothing beyond the arena bytes: transitively no
/// global-heap allocation, no `Rc`/`Arc`/`Weak`, no OS handle, no arena
/// collection backed outside the arena being reset.
pub unsafe trait ArenaSafe {}

// SAFETY: Copy forbids Drop and non-Copy fields — no heap buffer, refcount, handle.
unsafe impl<T: Copy> ArenaSafe for T {}

#[cfg(test)]
mod tests {
    use super::*;

    #[allow(dead_code)]
    struct PodNode {
        oid: u32,
        cost: f64,
        flags: u8,
    }
    // SAFETY: all fields are Copy primitives; no heap/Rc/handle; no manual Drop.
    unsafe impl ArenaSafe for PodNode {}

    fn assert_is_arena_safe<T: ArenaSafe>() {}

    #[test]
    fn copy_leaves_are_arena_safe() {
        assert_is_arena_safe::<u8>();
        assert_is_arena_safe::<i32>();
        assert_is_arena_safe::<u64>();
        assert_is_arena_safe::<f64>();
        assert_is_arena_safe::<bool>();
        assert_is_arena_safe::<*const u8>();
        assert_is_arena_safe::<Option<u32>>();
        assert_is_arena_safe::<[u64; 8]>();
        assert_is_arena_safe::<(u32, i16, bool)>();
    }

    #[test]
    fn audited_pod_aggregate_is_arena_safe() {
        assert_is_arena_safe::<PodNode>();
    }
}
