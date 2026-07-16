/// # Safety
/// Dropping `T` must reclaim nothing beyond the arena bytes: transitively no
/// global-heap allocation, no `Rc`/`Arc`/`Weak`, no OS handle, no arena
/// collection backed outside the arena being reset.
pub unsafe trait ArenaSafe {}

// SAFETY: Copy forbids Drop and non-Copy fields — no heap buffer, refcount, handle.
unsafe impl<T: Copy> ArenaSafe for T {}

/// # Safety
/// Forgetting `Self` (drop glue never runs) must leak nothing beyond arena
/// bytes: transitively no global-heap ownership, no `Rc`/`Arc`/`Weak`, no OS
/// handle, no registry entry that outlives the arena reset.
/// No `Copy` blanket (it would block the container impls below); no-drop leaf
/// types get impls via [`forget_safe_nodrop!`], which const-proves them.
pub unsafe trait ForgetSafe {}

// SAFETY (each): container whose drop reclaims only arena bytes plus its
// elements' drops; elements are recursively ForgetSafe.
unsafe impl<T: ForgetSafe> ForgetSafe for Option<T> {}
unsafe impl<T: ForgetSafe> ForgetSafe for core::cell::Cell<T> {}
unsafe impl<T: ForgetSafe> ForgetSafe for core::cell::RefCell<T> {}
unsafe impl<T: ForgetSafe> ForgetSafe for core::cell::OnceCell<T> {}
unsafe impl<T: ForgetSafe, const N: usize> ForgetSafe for [T; N] {}
unsafe impl<A: ForgetSafe, B: ForgetSafe> ForgetSafe for (A, B) {}
unsafe impl<A: ForgetSafe, B: ForgetSafe, C: ForgetSafe> ForgetSafe for (A, B, C) {}
unsafe impl<A: ForgetSafe, B: ForgetSafe, C: ForgetSafe, D: ForgetSafe> ForgetSafe
    for (A, B, C, D)
{
}
unsafe impl<'mcx, T: ForgetSafe> ForgetSafe for crate::PgVec<'mcx, T> {}
unsafe impl<'mcx, T: ForgetSafe + ?Sized> ForgetSafe for crate::PgBox<'mcx, T> {}
unsafe impl<'mcx, K: ForgetSafe, V: ForgetSafe> ForgetSafe
    for hashbrown::HashMap<K, V, rustc_hash::FxBuildHasher, crate::Mcx<'mcx>>
{
}
// SAFETY (each): no drop glue at all.
unsafe impl<'a, T: ?Sized> ForgetSafe for &'a T {}
unsafe impl<'a, T: ?Sized> ForgetSafe for &'a mut T {}
unsafe impl<T: ?Sized> ForgetSafe for *const T {}
unsafe impl<T: ?Sized> ForgetSafe for *mut T {}
unsafe impl<T: ?Sized> ForgetSafe for core::ptr::NonNull<T> {}
// SAFETY: PgString's only drop is its arena-backed byte buffer's.
unsafe impl ForgetSafe for crate::PgString<'_> {}
// SAFETY: Copy handle.
unsafe impl ForgetSafe for crate::Mcx<'_> {}

/// No-drop leaf: the const assert is the proof, the impl is free.
#[macro_export]
macro_rules! forget_safe_nodrop {
    ($($ty:ty),+ $(,)?) => {$(
        const _: () = assert!(!core::mem::needs_drop::<$ty>());
        // SAFETY: !needs_drop — forgetting reclaims nothing.
        unsafe impl $crate::ForgetSafe for $ty {}
    )+};
}

/// Struct impl with a compile-checked field census: the destructure is
/// exhaustive (a new field breaks the build) and every field must be
/// ForgetSafe.
#[macro_export]
macro_rules! forget_safe_struct {
    ($($ty:ident $(<$lt:lifetime>)? { $($f:ident),+ $(,)? $(; $($x:ident),+ $(,)?)? }),+ $(,)?) => {$(
        // SAFETY: every field is ForgetSafe (exhaustive census below); fields
        // after `;` are exempt — each needs a justifying comment at the call.
        unsafe impl $crate::ForgetSafe for $ty $(<$lt>)? {}
        const _: fn(&$ty $(<$lt>)?) = |v| {
            fn ok<T: $crate::ForgetSafe + ?Sized>(_: &T) {}
            let $ty { $($f),+ $($(, $x: _)+)? } = v;
            $( ok($f); )+
        };
    )+};
}

/// Enum form: exhaustive variant match, each payload checked ForgetSafe.
#[macro_export]
macro_rules! forget_safe_enum {
    ($($ty:ident $(<$lt:lifetime>)? { $($v:ident $(($p:ident))?),+ $(,)? }),+ $(,)?) => {$(
        // SAFETY: every variant payload is ForgetSafe (exhaustive match below).
        unsafe impl $crate::ForgetSafe for $ty $(<$lt>)? {}
        const _: fn(&$ty $(<$lt>)?) = |v| {
            fn ok<T: $crate::ForgetSafe + ?Sized>(_: &T) {}
            match v { $( $ty::$v $(($p))? => { $( ok($p); )? } )+ }
        };
    )+};
}

/// Tuple-struct form.
#[macro_export]
macro_rules! forget_safe_tuple {
    ($($ty:ident $(<$lt:lifetime>)? ( $($f:ident),+ $(,)? )),+ $(,)?) => {$(
        // SAFETY: every field is ForgetSafe (exhaustive census below).
        unsafe impl $crate::ForgetSafe for $ty $(<$lt>)? {}
        const _: fn(&$ty $(<$lt>)?) = |v| {
            fn ok<T: $crate::ForgetSafe + ?Sized>(_: &T) {}
            let $ty($($f),+) = v;
            $( ok($f); )+
        };
    )+};
}

forget_safe_nodrop!(
    u8, u16, u32, u64, u128, usize, i8, i16, i32, i64, i128, isize, f32, f64,
    bool, char, ()
);

/// Stack owner whose destructor intentionally never runs (C model: the
/// value's allocations die with the memory context, zero per-object teardown).
pub struct ArenaForget<T: ForgetSafe>(core::mem::ManuallyDrop<T>);

impl<T: ForgetSafe> ArenaForget<T> {
    #[inline]
    pub fn new(value: T) -> Self {
        ArenaForget(core::mem::ManuallyDrop::new(value))
    }
}

impl<T: ForgetSafe> core::ops::Deref for ArenaForget<T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &T {
        &self.0
    }
}

impl<T: ForgetSafe> core::ops::DerefMut for ArenaForget<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut T {
        &mut self.0
    }
}

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
