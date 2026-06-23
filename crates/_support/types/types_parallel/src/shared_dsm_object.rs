//! `shared_dsm_object` — the typed, concurrently-mutated shared-DSM-object
//! primitive.
//!
//! This module is the SOLE sanctioned raw-pointer / cross-process aliasing
//! surface for the *per-node* shared state objects the parallel executor
//! places in a DSM segment (`ParallelTableScanDescData`, `ParallelHashJoinState`,
//! `ParallelBitmapHeapState`, `SharedAggInfo`, `SharedHashInfo`,
//! `SharedExecutorInstrumentation`, …). It generalizes the single-writer
//! `store_fixed_state`/`store_instrumentation_header` pattern in
//! `backend-access-transam-parallel`'s `lib.rs` to cover the case where
//! multiple OS processes mutate the SAME physical bytes concurrently after the
//! launch barrier.
//!
//! It mirrors `shm_toc_estimate_chunk` / `shm_toc_allocate` / `shm_toc_insert`
//! / `shm_toc_lookup` exactly:
//!
//! * the leader [`estimate`]s a chunk size, `ShmToc::allocate`s a real
//!   in-segment chunk, [`place_and_init`]s the `repr(C)` `T` in place (running
//!   the C field initializers through the [`SharedView`] accessors so atomics
//!   and spinlocks are constructed correctly), and `shm_toc_insert`s its key;
//! * a worker `shm_toc_lookup`s the same key — which returns the SAME real
//!   in-segment address in the worker's own mapping — and [`attach`]es to it,
//!   reinterpreting the bytes as a shared `&T`.
//!
//! # SAFETY contract (the soundness core)
//!
//! Reuses the `backend-access-transam-parallel` `lib.rs` module SAFETY
//! wording: the `chunk`
//! [`SerializeCursor`] is a chunk previously handed out by the real
//! `shm_toc_allocate` (or recovered via `shm_toc_lookup`), so it points at
//! `>= size_of::<T>()` (or `>= nbytes` for a flexible-array tail) writable,
//! suitably-aligned bytes inside the mapped DSM (or private-memory) segment,
//! live for as long as the owning `ParallelContext` holds the segment. The
//! execParallel contract never resurrects a handle past
//! `DestroyParallelContext`. Chunks come from `BUFFERALIGN`ed
//! `shm_toc_allocate`, which over-aligns relative to these structs' natural
//! alignment.
//!
//! The deeper invariant — multiple OS processes map the SAME physical bytes —
//! makes a plain `&mut T` Undefined Behaviour, and even a `&T` is sound ONLY if
//! every field C mutates concurrently across processes is interior-mutable:
//!
//! * `pg_atomic_uint32` / `pg_atomic_uint64` — the `repr(C)` atomics already
//!   used by `shm-mq`/`shm-toc`/`dsa` (a `core::sync::atomic` cell over the
//!   shared bytes); accessed with the C's exact Acquire/Release/barrier pairings.
//! * `slock_t` — the real in-segment [`Spinlock`] with the RAII-guard acquire/
//!   release discipline `shm_toc` itself uses.
//! * `Barrier` — the real barrier type via the `backend-storage-ipc-barrier`
//!   seam.
//! * `ConditionVariable` — the real CV.
//! * Plain scalar fields written ONCE by the leader before workers launch and
//!   only READ afterward (`phs_relid`, `phs_syncscan`, the launch-once
//!   `Instrumentation` slots) stay plain fields accessed by copy, exactly like
//!   `store_fixed_state`: the launch barrier (the bgworker fork +
//!   `WaitForParallelWorkersToAttach` happens-before) supplies the ordering, so
//!   no atomic is needed.
//!
//! This interior-mutability requirement is encoded as the [`SharedDsmObject`]
//! marker trait, implemented ONLY on the audited per-node `repr(C)` structs.
//! [`SharedRef`] hands callers a plain shared `&T`; per-node code stays 100%
//! safe — it constructs nothing and dereferences nothing raw, it only calls the
//! `T`'s own interior-mutable accessor methods (atomic load/store, spinlock
//! guard, copy-getter for launch-once scalars).
//!
//! The borrow's lifetime `'seg` is carried via the [`DsmSegmentHandle`] /
//! `ParallelContext` handle, so the borrow cannot outlive the mapping. There is
//! never a `&'static mut` (consistent with `seam-signatures-mirror-C-failure-
//! surface`).

use core::marker::PhantomData;
use core::mem::size_of;

use ::types_core::Size;
// The execParallel-visible `dsm_segment *` handle — the one per-node crates
// receive from the `pcxt_seg` / `pwcxt_seg` seams. It is value-identical to the
// parallel-internal `types_parallel::DsmSegmentHandle` (both wrap the same real
// `DsmSegmentId`); the primitive only uses it to tie the borrow's lifetime to
// the segment mapping, never to dereference it.
use execparallel::{DsmSegmentHandle, SerializeCursor};

/// The [`SharedDsmObject`](crate::SharedDsmObject) marker trait, re-exported
/// at the historical `shared_dsm_object::SharedDsmObject` path. It is defined
/// in this crate's root (the lowest crate that owns [`DsmSegmentHandle`]) so
/// the audited per-node `repr(C)` structs (which live in `types-nodes` /
/// `types-execparallel`) can implement it next to their definition without
/// tripping the orphan rule. Callers keep using
/// `shared_dsm_object::SharedDsmObject`.
pub use crate::SharedDsmObject;

/// The size, in bytes, a chunk for a fixed-size `T` must be requested at for
/// `ShmToc::allocate` — `shm_toc_estimate_chunk(e, sizeof(T))`.
///
/// `BUFFERALIGN` is applied by `shm_toc_estimate_chunk`/`shm_toc_allocate` (as
/// it is today for `store_fixed_state`), so this returns the natural
/// `size_of::<T>()`; the allocator over-aligns.
#[inline]
pub fn estimate<T: SharedDsmObject>() -> Size {
    size_of::<T>()
}

/// The size, in bytes, a chunk for a flexible-array-tail `T` must be requested
/// at — the C `offsetof(T, tail) + n * sizeof(elem)` idiom. The caller computes
/// the exact tail byte count; this helper exists to keep the call-site shape
/// uniform with [`estimate`] and to document that the same `BUFFERALIGN`
/// over-alignment applies (`SharedExecutorInstrumentation`, `SharedAggInfo`,
/// `SharedHashInfo`).
#[inline]
pub fn estimate_flex(nbytes: Size) -> Size {
    nbytes
}

/// A view onto an in-segment `T` used *during placement-initialization*: the
/// leader has a pointer to freshly-allocated (un-zeroed) chunk bytes and must
/// run `T`'s C field initializers through it so atomics/spinlocks/barriers are
/// constructed correctly in place. It exposes the raw chunk pointer to the
/// init closure, which is the audited per-struct initializer (it lives in the
/// owning per-node crate's `repr(C)` impl, calling only that crate's own
/// constructors — still no raw deref outside this primitive, because the
/// closure receives `&SharedView` and uses [`SharedView::as_ptr`] solely to
/// hand to `T`'s own `init_in_place`-style associated fn, which this primitive
/// requires to be written against a `*mut T` it never aliases concurrently —
/// the leader is the sole writer pre-launch).
pub struct SharedView<'seg, T: SharedDsmObject> {
    ptr: *mut T,
    _seg: PhantomData<&'seg ()>,
}

impl<'seg, T: SharedDsmObject> SharedView<'seg, T> {
    /// The raw chunk pointer, for `T`'s own audited placement initializer.
    ///
    /// # Safety
    ///
    /// Valid ONLY during `place_and_init` before any worker has attached: the
    /// leader is the sole writer (the launch barrier has not yet released).
    /// The pointer addresses `>= size_of::<T>()` writable suitably-aligned
    /// in-segment bytes (see the module SAFETY contract).
    #[inline]
    pub unsafe fn as_ptr(&self) -> *mut T {
        self.ptr
    }
}

/// A shared borrow of an in-segment `T`, tied to the DSM segment's lifetime
/// `'seg`. This is what per-node code holds; all field access goes through
/// `T`'s own interior-mutable accessor methods (`&self`). There is no `&mut T`
/// path and no `&'static` escape.
#[derive(Clone, Copy)]
pub struct SharedRef<'seg, T: SharedDsmObject> {
    ptr: *const T,
    _seg: PhantomData<&'seg T>,
}

// SAFETY: `SharedRef`/`SharedView` are borrows of a shared segment whose
// cross-process synchronization is the embedded interior-mutable fields'
// responsibility (mirrors `ShmToc: Send`).
unsafe impl<T: SharedDsmObject> Send for SharedRef<'_, T> {}
unsafe impl<T: SharedDsmObject> Sync for SharedRef<'_, T> {}

impl<'seg, T: SharedDsmObject> SharedRef<'seg, T> {
    /// The shared `&T`. All concurrent mutation happens through `T`'s
    /// interior-mutable fields, so this shared reference is sound even while
    /// other processes hold their own `&T` to the same bytes.
    #[inline]
    pub fn get(&self) -> &'seg T {
        // SAFETY: see the module SAFETY contract: `ptr` is a real in-segment
        // address of an initialized `T` live for `'seg`; `T: SharedDsmObject`
        // guarantees every concurrently-mutated field is interior-mutable, so a
        // shared `&T` aliasing another process's shared `&T` is sound.
        unsafe { &*self.ptr }
    }
}

impl<'seg, T: SharedDsmObject> core::ops::Deref for SharedRef<'seg, T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &T {
        self.get()
    }
}

/// Leader side: placement-init a `T` at a real `shm_toc_allocate`'d chunk and
/// return a [`SharedRef`] tied to the segment.
///
/// Mirrors the leader's `obj = shm_toc_allocate(toc, sizeof(*obj)); *obj =
/// (T){ … }` followed by the explicit atomic/spinlock/barrier initializers
/// (e.g. `SpinLockInit(&obj->mutex); pg_atomic_init_u64(&obj->nallocated, 0);
/// BarrierInit(&obj->build_barrier, 0)`). The `init` closure is the per-node
/// crate's audited initializer; it runs through [`SharedView`] so the in-place
/// constructors (which construct the atomics/spinlocks/barriers correctly) are
/// the only writers, and the leader is the sole writer until the launch barrier
/// releases.
///
/// `_seg` ties the returned borrow to the segment mapping so it cannot outlive
/// it.
pub fn place_and_init<'seg, T: SharedDsmObject>(
    _seg: DsmSegmentHandle,
    chunk: SerializeCursor,
    init: impl FnOnce(&SharedView<'seg, T>),
) -> SharedRef<'seg, T> {
    let view = SharedView::<'seg, T> {
        ptr: chunk.0 as *mut T,
        _seg: PhantomData,
    };
    // Run the per-node crate's audited placement initializer. It writes every
    // field of the freshly-allocated (un-zeroed) chunk via the in-place
    // constructors; the leader is the sole writer pre-launch.
    init(&view);
    SharedRef {
        ptr: view.ptr as *const T,
        _seg: PhantomData,
    }
}

/// Leader side, mutable-reference variant: placement-init a `T` at a real
/// `shm_toc_allocate`'d chunk, handing the per-node crate a plain `&mut T` to
/// run its field initializers (`*p.field = ...`, `BarrierInit(&mut p.barrier,
/// 0)`, `LWLockInitialize(&mut p.lock, ...)`, `pg_atomic_init_u32(&mut
/// p.atomic, 0)`, …) with ZERO `unsafe` in the per-node crate.
///
/// This is sound for exactly the same reason `place_and_init`'s `SharedView`
/// path is: pre-launch the leader is the SOLE writer (no worker has attached
/// and the launch barrier — bgworker fork + `WaitForParallelWorkersToAttach` —
/// has not released), so a unique `&mut T` over the freshly-`shm_toc_allocate`'d
/// (un-aliased) chunk bytes is valid. The returned [`SharedRef`] downgrades to a
/// shared `&T` for the cross-process phase, which is sound because
/// `T: SharedDsmObject` guarantees every concurrently-mutated field is
/// interior-mutable. There is never a `&'static mut`.
///
/// Mirrors the leader's `pstate = shm_toc_allocate(toc, sizeof(*pstate));
/// pstate->nbatch = 0; ...; LWLockInitialize(&pstate->lock, ...);
/// BarrierInit(&pstate->build_barrier, 0); ...`.
pub fn place_and_init_mut<'seg, T: SharedDsmObject>(
    _seg: DsmSegmentHandle,
    chunk: SerializeCursor,
    init: impl FnOnce(&mut T),
) -> SharedRef<'seg, T> {
    let ptr = chunk.0 as *mut T;
    // SAFETY: `chunk` is a real `shm_toc_allocate`'d (or looked-up) chunk of
    // `>= size_of::<T>()` writable suitably-aligned in-segment bytes (module
    // SAFETY contract). Pre-launch the leader is the sole writer, so this
    // `&mut T` is unique. The closure fully initializes every field (audited via
    // `T: SharedDsmObject` clause 3) before any worker attaches.
    let m: &mut T = unsafe { &mut *ptr };
    init(m);
    SharedRef {
        ptr: ptr as *const T,
        _seg: PhantomData,
    }
}

/// Leader side: placement-init a `T` from a fully-constructed value, returning
/// a [`SharedRef`]. This is the 100%-safe variant of [`place_and_init`]: the
/// caller hands a `T` it built with safe constructors (atomics/spinlock/CV all
/// constructed by their own `new`/`Default`), and the primitive performs the
/// single placement write into the chunk. The caller writes NO raw pointer.
///
/// Mirrors the leader's `*obj = (T){ … }` followed by the explicit
/// atomic/spinlock/CV initializers — here folded into one move of a value whose
/// fields are already correctly constructed. The leader is the sole writer
/// until the launch barrier releases, so moving the constructed `T` into the
/// shared bytes is sound (no other process aliases it yet).
pub fn place_value<'seg, T: SharedDsmObject>(
    seg: DsmSegmentHandle,
    chunk: SerializeCursor,
    value: T,
) -> SharedRef<'seg, T> {
    place_and_init::<T>(seg, chunk, |view: &SharedView<'seg, T>| {
        // SAFETY: pre-launch the leader is the sole writer; `view.as_ptr()`
        // addresses `>= size_of::<T>()` writable suitably-aligned in-segment
        // bytes (the `place_and_init` contract). The single raw write is the
        // placement move of an already-constructed value.
        let p = unsafe { view.as_ptr() };
        unsafe { core::ptr::write(p, value) };
    })
}

/// A shared borrow of a `[E]` flexible-array tail living in-segment immediately
/// after a header object, tied to the DSM segment's lifetime `'seg`. Each `E`
/// is accessed through its own interior-mutable accessor methods (`&self`);
/// there is no `&mut E` path.
#[derive(Clone, Copy)]
pub struct SharedSlice<'seg, E: SharedDsmObject> {
    ptr: *const E,
    len: usize,
    _seg: PhantomData<&'seg E>,
}

// SAFETY: same reasoning as `SharedRef` — a borrow of a shared segment whose
// cross-process synchronization is the element type's interior-mutable fields'
// responsibility.
unsafe impl<E: SharedDsmObject> Send for SharedSlice<'_, E> {}
unsafe impl<E: SharedDsmObject> Sync for SharedSlice<'_, E> {}

impl<'seg, E: SharedDsmObject> SharedSlice<'seg, E> {
    /// Number of elements in the tail.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the tail is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The shared slice `&[E]`. Each element's concurrent mutation happens
    /// through its interior-mutable fields, so a shared slice aliasing another
    /// process's shared slice over the same bytes is sound.
    #[inline]
    pub fn get(&self) -> &'seg [E] {
        // SAFETY: see the module SAFETY contract: `ptr` addresses `len`
        // initialized `E`s laid out contiguously in-segment, live for `'seg`;
        // `E: SharedDsmObject` guarantees every concurrently-mutated field is
        // interior-mutable, so the shared `&[E]` is sound.
        unsafe { core::slice::from_raw_parts(self.ptr, self.len) }
    }
}

/// `offsetof(H, tail)` for a `#[repr(C)]` header `H` whose last C field is a
/// `FLEXIBLE_ARRAY_MEMBER` of `E`: the header size rounded up to `E`'s
/// alignment (where the C array begins). Mirrors the `offsetof` the C uses to
/// size the chunk and to locate the array.
#[inline]
fn flex_tail_offset<H, E>() -> usize {
    let h = size_of::<H>();
    let a = core::mem::align_of::<E>();
    (h + a - 1) & !(a - 1)
}

/// Leader side: placement-init a flexible-array object — a `#[repr(C)]` header
/// `H` immediately followed by `len` elements of `E` — at a real chunk sized
/// via [`estimate_flex`]. Returns the header [`SharedRef`] and the tail
/// [`SharedSlice`]. `init_header` builds the header value and `init_elem(i)`
/// builds each element value (the C `sinstrument->num_workers = …; memset(…)`).
/// The leader is the sole writer pre-launch.
pub fn place_flex<'seg, H, E>(
    _seg: DsmSegmentHandle,
    chunk: SerializeCursor,
    len: usize,
    header: H,
    mut elem: impl FnMut(usize) -> E,
) -> (SharedRef<'seg, H>, SharedSlice<'seg, E>)
where
    H: SharedDsmObject,
    E: SharedDsmObject,
{
    let base = chunk.0;
    let hdr_ptr = base as *mut H;
    let tail_ptr = (base + flex_tail_offset::<H, E>()) as *mut E;
    // SAFETY: pre-launch the leader is the sole writer; `chunk` addresses
    // `>= estimate_flex(offsetof(H, tail) + len * size_of::<E>())` writable
    // suitably-aligned in-segment bytes (the `estimate_flex`/`shm_toc_allocate`
    // contract). Placement-move each already-constructed value into place.
    unsafe {
        core::ptr::write(hdr_ptr, header);
        for i in 0..len {
            core::ptr::write(tail_ptr.add(i), elem(i));
        }
    }
    (
        SharedRef {
            ptr: hdr_ptr as *const H,
            _seg: PhantomData,
        },
        SharedSlice {
            ptr: tail_ptr as *const E,
            len,
            _seg: PhantomData,
        },
    )
}

/// Worker side: attach to a flexible-array object that `shm_toc_lookup` already
/// located. Reinterprets the header bytes as `&H` and the `len`-element tail as
/// `&[E]` (no init — the leader already placement-initialized them). `len` is
/// recovered from the header (e.g. `header.num_workers`), exactly as the C
/// computes `offsetof(H, tail) + num_workers * sizeof(E)`.
pub fn attach_flex<'seg, H, E>(
    _seg: DsmSegmentHandle,
    chunk: SerializeCursor,
    len: usize,
) -> (SharedRef<'seg, H>, SharedSlice<'seg, E>)
where
    H: SharedDsmObject,
    E: SharedDsmObject,
{
    let base = chunk.0;
    (
        SharedRef {
            ptr: base as *const H,
            _seg: PhantomData,
        },
        SharedSlice {
            ptr: (base + flex_tail_offset::<H, E>()) as *const E,
            len,
            _seg: PhantomData,
        },
    )
}

/// Worker side: attach to a `T` that `shm_toc_lookup` already located at its
/// real in-segment address in this process's mapping. Reinterprets the bytes as
/// a shared `&T` (no init — the leader already placement-initialized it before
/// the launch barrier released).
///
/// Mirrors the worker's `obj = shm_toc_lookup(toc, KEY, false)`.
///
/// `_seg` ties the returned borrow to the segment mapping.
pub fn attach<'seg, T: SharedDsmObject>(
    _seg: DsmSegmentHandle,
    chunk: SerializeCursor,
) -> SharedRef<'seg, T> {
    SharedRef {
        ptr: chunk.0 as *const T,
        _seg: PhantomData,
    }
}

/// Run a closure with a unique `&mut T` over a `shm_toc_lookup`'d in-segment
/// object, for the phases the C code mutates the object through a plain
/// `Obj *` while NO other participant is concurrently touching it — namely:
///
/// * a worker attaching pre-launch (before the launch barrier releases), and
/// * the leader resetting shared state between scans (`ReInitializeDSM`), after
///   all participants have detached from the previous generation.
///
/// In both windows the caller is the sole accessor, so a unique `&mut T` over
/// the in-segment bytes is valid — the same reasoning as
/// [`place_and_init_mut`], applied to an already-initialized object. This lets
/// per-node code pass `&mut obj.field` to owner seams whose C prototype is
/// `Foo(SubObj *sub, ...)` (e.g. `SharedFileSetAttach(&pstate->fileset, seg)`,
/// `BarrierInit(&pstate->build_barrier, 0)`) with ZERO `unsafe` in the per-node
/// crate.
///
/// # Caller obligation
///
/// The caller asserts (mirroring the C call-site's invariant) that no other
/// process is concurrently accessing `*chunk` for the duration of `f`. This is
/// guaranteed by the parallel-hash protocol at the two call sites above.
pub fn with_mut<T: SharedDsmObject, R>(
    _seg: DsmSegmentHandle,
    chunk: SerializeCursor,
    f: impl FnOnce(&mut T) -> R,
) -> R {
    // SAFETY: `chunk` is a real in-segment address of an initialized `T`
    // (module SAFETY contract). The caller guarantees it is the sole accessor
    // for the duration of `f` (worker pre-launch attach, or leader rescan reset
    // after all participants detached), so this `&mut T` is unique.
    let m: &mut T = unsafe { &mut *(chunk.0 as *mut T) };
    f(m)
}
