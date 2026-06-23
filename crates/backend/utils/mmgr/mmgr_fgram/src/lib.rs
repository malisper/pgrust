#![allow(non_snake_case)]
//!
//! PostgreSQL memory contexts own palloc allocations. Safe Rust code uses
//! lifetime-bound handles (`PgMemory<'ctx>`, `PgArray<'ctx, T>`,
//! `PgCString<'ctx>`, `PgBox<'ctx, T>`) to access those allocations:
//!
//! 1. The memory context owns the allocation.
//! 2. Rust code passes around a handle tied to the context lifetime.
//! 3. The handle cannot outlive the context because of `'ctx`.
//! 4. Dropping the handle does not free memory.
//! 5. Explicit `pfree(self)` consumes the handle and frees the allocation.
//! 6. Because `self` is consumed, safe Rust prevents a second free through the
//!    same handle.
//!
//! Raw pointer frees remain unsafe; safe callers should prefer consuming
//! handle methods.

mod aligned;
mod aset;
mod bump;
mod generation;
mod raw;
mod slab;

use std::ffi::{CStr, CString};
use std::marker::PhantomData;
use std::mem::{align_of, size_of};
use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;
use std::rc::Rc;

use error_fgram::{PgError, PgResult};
use pg_ffi_fgram::{
    instr_time, BufferUsage, FmgrInfo, Instrumentation, Size, TupleHashEntryData, WalUsage,
    WorkerInstrumentation,
};
pub use pg_ffi_fgram::{
    MemoryContext, MemoryContextCallback, MemoryContextCounters, MemoryContextData,
    MemoryContextMethodID, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE,
    ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_SEPARATE_THRESHOLD, ALLOCSET_SMALL_INITSIZE,
    ALLOCSET_SMALL_MAXSIZE, ALLOCSET_SMALL_MINSIZE, MAX_ALLOC_HUGE_SIZE, MAX_ALLOC_SIZE,
    MCTX_ALIGNED_REDIRECT_ID, MCTX_ASET_ID, MCTX_BUMP_ID, MCTX_GENERATION_ID, MCTX_SLAB_ID,
    MCXT_ALLOC_HUGE, MCXT_ALLOC_NO_OOM, MCXT_ALLOC_ZERO, SLAB_DEFAULT_BLOCK_SIZE,
    SLAB_LARGE_BLOCK_SIZE,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MemoryContextKind {
    AllocSet,
    Generation,
    Slab { chunk_size: Size },
    Bump,
}

#[derive(Debug)]
pub struct PgMemoryContext {
    raw: NonNull<MemoryContextData>,
    _marker: PhantomData<Rc<MemoryContextData>>,
}

impl Clone for PgMemoryContext {
    fn clone(&self) -> Self {
        *self
    }
}

impl Copy for PgMemoryContext {}

impl PgMemoryContext {
    pub fn top() -> PgResult<Self> {
        MemoryContextInit()?;
        context_from_raw(raw::top_memory_context())
    }

    pub fn error() -> PgResult<Self> {
        MemoryContextInit()?;
        context_from_raw(raw::error_context())
    }

    pub fn current() -> PgResult<Self> {
        MemoryContextInit()?;
        context_from_raw(raw::current_memory_context())
    }

    pub fn from_raw(raw: MemoryContext) -> PgResult<Self> {
        context_from_raw(raw)
    }

    pub fn as_ptr(self) -> MemoryContext {
        self.raw.as_ptr()
    }

    pub fn switch_to(self) -> PgResult<MemoryContextSwitchGuard> {
        let old = MemoryContextSwitchTo(self)?;
        Ok(MemoryContextSwitchGuard {
            old,
            _marker: PhantomData,
        })
    }

    pub fn alloc_owned(self, size: Size) -> PgResult<PgOwnedMemory> {
        MemoryContextAllocOwned(self, size)
    }

    pub fn alloc_zero_owned(self, size: Size) -> PgResult<PgOwnedMemory> {
        MemoryContextAllocZeroOwned(self, size)
    }

    pub unsafe fn reset(self) -> PgResult<()> {
        unsafe { MemoryContextReset(self) }
    }

    pub unsafe fn delete(self) -> PgResult<()> {
        unsafe { MemoryContextDelete(self) }
    }

    pub fn is_empty(self) -> PgResult<bool> {
        MemoryContextIsEmpty(self)
    }

    pub fn mem_allocated(self, recurse: bool) -> PgResult<Size> {
        MemoryContextMemAllocated(self, recurse)
    }

    pub fn stats(self) -> PgResult<String> {
        MemoryContextStats(self)
    }

    pub fn set_identifier(self, ident: impl Into<String>) -> PgResult<()> {
        MemoryContextSetIdentifier(self, ident)
    }
}

pub struct MemoryContextSwitchGuard {
    old: PgMemoryContext,
    _marker: PhantomData<Rc<()>>,
}

impl Drop for MemoryContextSwitchGuard {
    fn drop(&mut self) {
        let _ = raw::set_current_memory_context(self.old.as_ptr());
    }
}

#[derive(Clone, Copy, Debug)]
pub struct MemoryContextScope<'ctx> {
    context: PgMemoryContext,
    _marker: PhantomData<&'ctx OwnedMemoryContext>,
}

impl<'ctx> MemoryContextScope<'ctx> {
    /// Attach a Rust lifetime to an existing PostgreSQL memory context.
    ///
    /// # Safety
    ///
    /// The caller must ensure `context` remains live for the chosen `'ctx`
    /// lifetime, and that objects allocated through the returned scope do not
    /// outlive the PostgreSQL memory context that owns them.
    pub unsafe fn from_context_unchecked(context: PgMemoryContext) -> Self {
        Self {
            context,
            _marker: PhantomData,
        }
    }

    pub fn as_context(self) -> PgMemoryContext {
        self.context
    }

    pub fn alloc_bytes(self, size: Size) -> PgResult<PgMemory<'ctx>> {
        unsafe { MemoryContextAlloc(self.context, size) }
    }

    pub fn alloc_zeroed_bytes(self, size: Size) -> PgResult<PgMemory<'ctx>> {
        unsafe { MemoryContextAllocZero(self.context, size) }
    }

    pub fn alloc_extended(self, size: Size, flags: i32) -> PgResult<Option<PgMemory<'ctx>>> {
        unsafe { MemoryContextAllocExtended(self.context, size, flags) }
    }

    pub fn alloc_aligned(
        self,
        size: Size,
        alignto: Size,
        flags: i32,
    ) -> PgResult<Option<PgMemory<'ctx>>> {
        unsafe { MemoryContextAllocAligned(self.context, size, alignto, flags) }
    }

    pub fn alloc_object<T>(self, value: T) -> PgResult<PgBox<'ctx, T>> {
        let memory = unsafe {
            MemoryContextAllocAligned(
                self.context,
                std::mem::size_of::<T>(),
                std::mem::align_of::<T>(),
                0,
            )
        }
        .and_then(required_allocation)?;
        let ptr = memory.as_ptr().cast::<T>();
        unsafe {
            ptr.write(value);
        }
        Ok(PgBox {
            _memory: memory,
            ptr: NonNull::new(ptr)
                .ok_or_else(|| PgError::error("alloc_object: palloc returned a null pointer"))?,
            _marker: PhantomData,
        })
    }

    pub fn alloc_zeroed_array<T: PgZeroable>(self, len: usize) -> PgResult<PgArray<'ctx, T>> {
        pg_array_from_memory(self.alloc_zeroed_bytes(array_size::<T>(len)?)?, len)
    }
}

#[derive(Debug)]
pub struct PgOwnedMemory {
    ptr: NonNull<u8>,
    size: Size,
    _marker: PhantomData<Rc<()>>,
}

#[derive(Debug)]
pub struct PgMemory<'ctx> {
    ptr: NonNull<u8>,
    size: Size,
    _marker: PhantomData<&'ctx ()>,
}

#[derive(Debug)]
pub struct PgArray<'ctx, T> {
    memory: PgMemory<'ctx>,
    len: usize,
    _marker: PhantomData<T>,
}

#[derive(Debug)]
pub struct PgOwnedArray<T> {
    memory: PgOwnedMemory,
    len: usize,
    _marker: PhantomData<T>,
}

/// Types whose all-zero byte pattern is a valid initialized value.
///
/// # Safety
///
/// Implementors must be plain data types where `0u8` in every byte is a valid
/// value. `palloc0_array` relies on this to expose zeroed memory as `&mut [T]`.
pub unsafe trait PgZeroable: Copy {}

macro_rules! impl_pg_zeroable {
    ($($ty:ty),+ $(,)?) => {
        $(unsafe impl PgZeroable for $ty {})+
    };
}

impl_pg_zeroable!(u8, i8, u16, i16, u32, i32, u64, i64, usize, isize, f32, f64, bool);
impl_pg_zeroable!(
    instr_time,
    BufferUsage,
    WalUsage,
    Instrumentation,
    WorkerInstrumentation,
    TupleHashEntryData,
    FmgrInfo,
);

#[derive(Debug)]
pub struct OwnedMemoryContext {
    context: PgMemoryContext,
    deleted: bool,
    _marker: PhantomData<Rc<()>>,
}

impl OwnedMemoryContext {
    pub fn alloc_set(
        parent: Option<PgMemoryContext>,
        name: impl Into<String>,
        min_context_size: Size,
        init_block_size: Size,
        max_block_size: Size,
    ) -> PgResult<Self> {
        Ok(Self::new(AllocSetContextCreateInternal(
            parent,
            name,
            min_context_size,
            init_block_size,
            max_block_size,
        )?))
    }

    pub fn generation(
        parent: Option<PgMemoryContext>,
        name: impl Into<String>,
        min_context_size: Size,
        init_block_size: Size,
        max_block_size: Size,
    ) -> PgResult<Self> {
        Ok(Self::new(GenerationContextCreate(
            parent,
            name,
            min_context_size,
            init_block_size,
            max_block_size,
        )?))
    }

    pub fn slab(
        parent: Option<PgMemoryContext>,
        name: impl Into<String>,
        block_size: Size,
        chunk_size: Size,
    ) -> PgResult<Self> {
        Ok(Self::new(SlabContextCreate(
            parent, name, block_size, chunk_size,
        )?))
    }

    pub fn bump(
        parent: Option<PgMemoryContext>,
        name: impl Into<String>,
        min_context_size: Size,
        init_block_size: Size,
        max_block_size: Size,
    ) -> PgResult<Self> {
        Ok(Self::new(BumpContextCreate(
            parent,
            name,
            min_context_size,
            init_block_size,
            max_block_size,
        )?))
    }

    pub fn as_context(&self) -> PgMemoryContext {
        self.context
    }

    pub fn into_context(mut self) -> PgMemoryContext {
        self.deleted = true;
        self.context
    }

    pub fn into_raw(mut self) -> MemoryContext {
        self.deleted = true;
        self.context.as_ptr()
    }

    /// Reconstruct an owned memory-context handle from a raw C ABI
    /// `MemoryContext` field.
    ///
    /// # Safety
    ///
    /// `raw` must be a live context whose ownership is being transferred into
    /// Rust exactly once, and no other Rust owner may delete it.
    pub unsafe fn from_raw_owned(raw: MemoryContext) -> PgResult<Self> {
        Ok(Self::new(PgMemoryContext::from_raw(raw)?))
    }

    pub fn scope(&self) -> MemoryContextScope<'_> {
        MemoryContextScope {
            context: self.context,
            _marker: PhantomData,
        }
    }

    pub fn alloc_bytes(&self, size: Size) -> PgResult<PgMemory<'_>> {
        self.scope().alloc_bytes(size)
    }

    pub fn alloc_zeroed_bytes(&self, size: Size) -> PgResult<PgMemory<'_>> {
        self.scope().alloc_zeroed_bytes(size)
    }

    pub fn alloc_object<T>(&self, value: T) -> PgResult<PgBox<'_, T>> {
        self.scope().alloc_object(value)
    }

    pub fn reset(&mut self) -> PgResult<()> {
        unsafe { MemoryContextReset(self.context) }
    }

    pub fn reset_only(&mut self) -> PgResult<()> {
        unsafe { MemoryContextResetOnly(self.context) }
    }

    pub fn delete(mut self) -> PgResult<()> {
        unsafe { MemoryContextDelete(self.context)? };
        self.deleted = true;
        Ok(())
    }

    fn new(context: PgMemoryContext) -> Self {
        Self {
            context,
            deleted: false,
            _marker: PhantomData,
        }
    }
}

impl Drop for OwnedMemoryContext {
    fn drop(&mut self) {
        if !self.deleted {
            let _ = unsafe { MemoryContextDelete(self.context) };
            self.deleted = true;
        }
    }
}

pub type ScopedPgMemory<'ctx> = PgMemory<'ctx>;

#[derive(Debug)]
pub struct PgBox<'ctx, T> {
    _memory: PgMemory<'ctx>,
    ptr: NonNull<T>,
    _marker: PhantomData<&'ctx OwnedMemoryContext>,
}

impl<T> Deref for PgBox<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { self.ptr.as_ref() }
    }
}

impl<T> DerefMut for PgBox<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.ptr.as_mut() }
    }
}

impl<T> Drop for PgBox<'_, T> {
    fn drop(&mut self) {
        unsafe {
            std::ptr::drop_in_place(self.ptr.as_ptr());
        }
    }
}

impl<T> PgBox<'_, T> {
    pub fn pfree(self) -> PgResult<()> {
        let value_ptr = self.ptr.as_ptr();
        let memory_ptr = self._memory.as_ptr();
        std::mem::forget(self);
        unsafe {
            std::ptr::drop_in_place(value_ptr);
            pfree(memory_ptr)
        }
    }
}

impl PgOwnedMemory {
    pub fn as_ptr(&self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    pub fn len(&self) -> Size {
        self.size
    }

    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.size) }
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.size) }
    }

    pub fn resize(&mut self, size: Size) -> PgResult<()> {
        let (ptr, size) = raw::repalloc_raw(self.ptr.as_ptr().cast(), size, 0)?;
        self.ptr = ptr.cast();
        self.size = size;
        Ok(())
    }

    pub fn into_raw(self) -> *mut u8 {
        let ptr = self.ptr.as_ptr();
        std::mem::forget(self);
        ptr
    }
}

impl Drop for PgOwnedMemory {
    fn drop(&mut self) {
        let _ = raw::pfree_raw(self.ptr.as_ptr().cast());
    }
}

impl<'ctx> PgMemory<'ctx> {
    pub fn as_ptr(&self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    pub fn len(&self) -> Size {
        self.size
    }

    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.size) }
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.size) }
    }

    pub fn resize(&mut self, size: Size) -> PgResult<()> {
        let (ptr, size) = raw::repalloc_raw(self.ptr.as_ptr().cast(), size, 0)?;
        self.ptr = ptr.cast();
        self.size = size;
        Ok(())
    }

    pub fn into_raw(self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    pub fn pfree(self) -> PgResult<()> {
        let ptr = self.into_raw();
        unsafe { pfree(ptr) }
    }
}

impl<'ctx, T> PgArray<'ctx, T> {
    pub fn as_ptr(&self) -> *const T {
        self.memory.as_ptr().cast::<T>()
    }

    pub fn as_mut_ptr(&mut self) -> *mut T {
        self.memory.as_ptr().cast::<T>()
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn as_slice(&self) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.as_ptr(), self.len) }
    }

    pub fn as_mut_slice(&mut self) -> &mut [T] {
        unsafe { std::slice::from_raw_parts_mut(self.as_mut_ptr(), self.len) }
    }

    pub fn into_memory(self) -> PgMemory<'ctx> {
        self.memory
    }

    pub fn pfree(self) -> PgResult<()> {
        self.into_memory().pfree()
    }
}

impl<T> PgOwnedArray<T> {
    pub fn as_ptr(&self) -> *const T {
        self.memory.as_ptr().cast::<T>()
    }

    pub fn as_mut_ptr(&mut self) -> *mut T {
        self.memory.as_ptr().cast::<T>()
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn as_slice(&self) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.as_ptr(), self.len) }
    }

    pub fn as_mut_slice(&mut self) -> &mut [T] {
        unsafe { std::slice::from_raw_parts_mut(self.as_mut_ptr(), self.len) }
    }

    pub fn into_memory(self) -> PgOwnedMemory {
        self.memory
    }
}

pub fn MemoryContextInit() -> PgResult<()> {
    raw::memory_context_init()
}

pub fn AllocSetContextCreateInternal(
    parent: Option<PgMemoryContext>,
    name: impl Into<String>,
    min_context_size: Size,
    init_block_size: Size,
    max_block_size: Size,
) -> PgResult<PgMemoryContext> {
    raw::create_context(
        parent.map(PgMemoryContext::as_ptr),
        name.into(),
        MemoryContextKind::AllocSet,
        min_context_size,
        init_block_size,
        max_block_size,
    )
    .and_then(context_from_raw)
}

pub fn GenerationContextCreate(
    parent: Option<PgMemoryContext>,
    name: impl Into<String>,
    min_context_size: Size,
    init_block_size: Size,
    max_block_size: Size,
) -> PgResult<PgMemoryContext> {
    raw::create_context(
        parent.map(PgMemoryContext::as_ptr),
        name.into(),
        MemoryContextKind::Generation,
        min_context_size,
        init_block_size,
        max_block_size,
    )
    .and_then(context_from_raw)
}

pub fn SlabContextCreate(
    parent: Option<PgMemoryContext>,
    name: impl Into<String>,
    block_size: Size,
    chunk_size: Size,
) -> PgResult<PgMemoryContext> {
    raw::create_context(
        parent.map(PgMemoryContext::as_ptr),
        name.into(),
        MemoryContextKind::Slab { chunk_size },
        0,
        block_size,
        block_size,
    )
    .and_then(context_from_raw)
}

pub fn BumpContextCreate(
    parent: Option<PgMemoryContext>,
    name: impl Into<String>,
    min_context_size: Size,
    init_block_size: Size,
    max_block_size: Size,
) -> PgResult<PgMemoryContext> {
    raw::create_context(
        parent.map(PgMemoryContext::as_ptr),
        name.into(),
        MemoryContextKind::Bump,
        min_context_size,
        init_block_size,
        max_block_size,
    )
    .and_then(context_from_raw)
}

pub fn MemoryContextSwitchTo(context: PgMemoryContext) -> PgResult<PgMemoryContext> {
    context_from_raw(raw::set_current_memory_context(context.as_ptr())?)
}

/// Allocate memory in `context` and tie the returned pointer to a caller-chosen
/// context lifetime.
///
/// # Safety
///
/// The caller must ensure the returned `PgMemory` does not outlive `context`
/// and is not used after the context is reset or deleted.
pub unsafe fn MemoryContextAlloc<'ctx>(
    context: PgMemoryContext,
    size: Size,
) -> PgResult<PgMemory<'ctx>> {
    unsafe { MemoryContextAllocExtended(context, size, 0) }.and_then(required_allocation)
}

/// # Safety
///
/// The caller must ensure the returned `PgMemory` does not outlive `context`
/// and is not used after the context is reset or deleted.
pub unsafe fn MemoryContextAllocZero<'ctx>(
    context: PgMemoryContext,
    size: Size,
) -> PgResult<PgMemory<'ctx>> {
    unsafe { MemoryContextAllocExtended(context, size, MCXT_ALLOC_ZERO) }
        .and_then(required_allocation)
}

/// # Safety
///
/// The caller must ensure any returned `PgMemory` does not outlive `context`
/// and is not used after the context is reset or deleted.
pub unsafe fn MemoryContextAllocExtended<'ctx>(
    context: PgMemoryContext,
    size: Size,
    flags: i32,
) -> PgResult<Option<PgMemory<'ctx>>> {
    let ptr = raw::alloc_raw(context.as_ptr(), size, flags)?;
    Ok(ptr.map(scoped_memory))
}

/// # Safety
///
/// The caller must ensure any returned `PgMemory` does not outlive `context`
/// and is not used after the context is reset or deleted.
pub unsafe fn MemoryContextAllocAligned<'ctx>(
    context: PgMemoryContext,
    size: Size,
    alignto: Size,
    flags: i32,
) -> PgResult<Option<PgMemory<'ctx>>> {
    let ptr = raw::alloc_aligned_raw(context.as_ptr(), size, alignto, flags)?;
    Ok(ptr.map(scoped_memory))
}

/// # Safety
///
/// The caller must ensure the returned `PgMemory` does not outlive `context`
/// and is not used after the context is reset or deleted.
pub unsafe fn MemoryContextAllocHuge<'ctx>(
    context: PgMemoryContext,
    size: Size,
) -> PgResult<PgMemory<'ctx>> {
    unsafe { MemoryContextAllocExtended(context, size, MCXT_ALLOC_HUGE) }
        .and_then(required_allocation)
}

pub fn MemoryContextAllocOwned(context: PgMemoryContext, size: Size) -> PgResult<PgOwnedMemory> {
    MemoryContextAllocExtendedOwned(context, size, 0).and_then(required_allocation)
}

pub fn MemoryContextAllocZeroOwned(
    context: PgMemoryContext,
    size: Size,
) -> PgResult<PgOwnedMemory> {
    MemoryContextAllocExtendedOwned(context, size, MCXT_ALLOC_ZERO).and_then(required_allocation)
}

pub fn MemoryContextAllocExtendedOwned(
    context: PgMemoryContext,
    size: Size,
    flags: i32,
) -> PgResult<Option<PgOwnedMemory>> {
    let ptr = raw::alloc_raw(context.as_ptr(), size, flags)?;
    Ok(ptr.map(owned_memory))
}

pub fn MemoryContextAllocAlignedOwned(
    context: PgMemoryContext,
    size: Size,
    alignto: Size,
    flags: i32,
) -> PgResult<Option<PgOwnedMemory>> {
    let ptr = raw::alloc_aligned_raw(context.as_ptr(), size, alignto, flags)?;
    Ok(ptr.map(owned_memory))
}

pub fn MemoryContextAllocHugeOwned(
    context: PgMemoryContext,
    size: Size,
) -> PgResult<PgOwnedMemory> {
    MemoryContextAllocExtendedOwned(context, size, MCXT_ALLOC_HUGE).and_then(required_allocation)
}

pub fn palloc<'ctx>(scope: &MemoryContextScope<'ctx>, size: Size) -> PgResult<PgMemory<'ctx>> {
    scope.alloc_bytes(size)
}

pub fn palloc0<'ctx>(scope: &MemoryContextScope<'ctx>, size: Size) -> PgResult<PgMemory<'ctx>> {
    scope.alloc_zeroed_bytes(size)
}

pub fn palloc0_array<'ctx, T: PgZeroable>(
    scope: &MemoryContextScope<'ctx>,
    len: usize,
) -> PgResult<PgArray<'ctx, T>> {
    scope.alloc_zeroed_array(len)
}

pub fn palloc_extended<'ctx>(
    scope: &MemoryContextScope<'ctx>,
    size: Size,
    flags: i32,
) -> PgResult<Option<PgMemory<'ctx>>> {
    scope.alloc_extended(size, flags)
}

pub fn palloc_aligned<'ctx>(
    scope: &MemoryContextScope<'ctx>,
    size: Size,
    alignto: Size,
    flags: i32,
) -> PgResult<Option<PgMemory<'ctx>>> {
    scope.alloc_aligned(size, alignto, flags)
}

pub fn palloc_owned(size: Size) -> PgResult<PgOwnedMemory> {
    MemoryContextAllocOwned(PgMemoryContext::current()?, size)
}

pub fn palloc0_owned(size: Size) -> PgResult<PgOwnedMemory> {
    MemoryContextAllocZeroOwned(PgMemoryContext::current()?, size)
}

pub fn palloc0_array_owned<T: PgZeroable>(len: usize) -> PgResult<PgOwnedArray<T>> {
    pg_owned_array_from_memory(palloc0_owned(array_size::<T>(len)?)?, len)
}

pub fn palloc_extended_owned(size: Size, flags: i32) -> PgResult<Option<PgOwnedMemory>> {
    MemoryContextAllocExtendedOwned(PgMemoryContext::current()?, size, flags)
}

pub fn palloc_aligned_owned(
    size: Size,
    alignto: Size,
    flags: i32,
) -> PgResult<Option<PgOwnedMemory>> {
    MemoryContextAllocAlignedOwned(PgMemoryContext::current()?, size, alignto, flags)
}

pub unsafe fn pfree_raw(pointer: *mut u8) -> PgResult<()> {
    raw::pfree_raw(pointer.cast())
}

pub unsafe fn pfree(pointer: *mut u8) -> PgResult<()> {
    unsafe { pfree_raw(pointer) }
}

/// Allocate a `T` in the current memory context, move `value` into it, and
/// return a raw `*mut T` whose memory is owned by the context (the Rust
/// ownership wrapper is leaked via `into_raw`). This is the escapee allocator:
/// the object is stored in a long-lived struct and freed at a C-dictated point
/// (or by context reset), NOT at Rust scope exit — mirroring C's palloc. Use
/// [`crate::PgBox`] / `alloc_object` instead for scope-local temporaries.
pub fn palloc_struct<T>(value: T) -> PgResult<*mut T> {
    let memory = palloc_owned(std::mem::size_of::<T>())?;
    let ptr = memory.as_ptr().cast::<T>();
    unsafe {
        ptr.write(value);
    }
    let _ = memory.into_raw();
    Ok(ptr)
}

/// Free a `*mut T` produced by [`palloc_struct`], running `T`'s destructor
/// first. The `drop_in_place` is load-bearing for non-POD `T` (e.g. structs
/// owning `Vec`s) — plain [`pfree`] would leak the inner allocations.
///
/// # Safety
/// `ptr` must be null or a live allocation from [`palloc_struct`] that has not
/// already been freed.
pub unsafe fn pfree_struct<T>(ptr: *mut T) -> PgResult<()> {
    if ptr.is_null() {
        return Ok(());
    }
    unsafe {
        std::ptr::drop_in_place(ptr);
        pfree(ptr.cast())
    }
}

/// `repalloc(pointer, size)` on a raw pointer, returning the (possibly moved)
/// pointer.  This is the C-style entry point used by ports that manage their
/// own raw `palloc`/`repalloc`/`pfree` arrays.
///
/// # Safety
/// `pointer` must be a live allocation produced by the palloc family.
pub unsafe fn repalloc_raw(pointer: *mut u8, size: Size) -> PgResult<*mut u8> {
    let (ptr, _size) = raw::repalloc_raw(pointer.cast(), size, 0)?;
    Ok(ptr.as_ptr().cast())
}

/// Like [`repalloc_raw`] but permits allocations larger than `MaxAllocSize`
/// (the `MCXT_ALLOC_HUGE` flag). Mirrors C `repalloc_huge(pointer, size)`.
///
/// # Safety
///
/// Same requirements as [`repalloc_raw`].
pub unsafe fn repalloc_huge_raw(pointer: *mut u8, size: Size) -> PgResult<*mut u8> {
    let (ptr, _size) = raw::repalloc_raw(pointer.cast(), size, MCXT_ALLOC_HUGE)?;
    Ok(ptr.as_ptr().cast())
}

/// `repalloc(pointer, size)` operating directly on a raw chunk pointer, the way
/// the C API does.  Returns the (possibly moved) new pointer.
///
/// # Safety
/// `pointer` must be a live chunk previously returned by one of the palloc-family
/// allocators.
pub unsafe fn repalloc_raw_ptr(pointer: *mut u8, size: Size, flags: i32) -> PgResult<*mut u8> {
    let (ptr, _size) = raw::repalloc_raw(pointer.cast(), size, flags)?;
    Ok(ptr.as_ptr().cast())
}

/// `repalloc_huge(pointer, size)` operating directly on a raw chunk pointer.
///
/// # Safety
/// See [`repalloc_raw_ptr`].
pub unsafe fn repalloc_huge_raw_ptr(pointer: *mut u8, size: Size) -> PgResult<*mut u8> {
    unsafe { repalloc_raw_ptr(pointer, size, MCXT_ALLOC_HUGE) }
}

pub fn repalloc<'ctx>(memory: PgMemory<'ctx>, size: Size) -> PgResult<PgMemory<'ctx>> {
    let ptr = memory.ptr;
    let (ptr, size) = raw::repalloc_raw(ptr.as_ptr().cast(), size, 0)?;
    Ok(PgMemory {
        ptr: ptr.cast(),
        size,
        _marker: PhantomData,
    })
}

pub fn repalloc_extended<'ctx>(
    memory: PgMemory<'ctx>,
    size: Size,
    flags: i32,
) -> PgResult<Option<PgMemory<'ctx>>> {
    let ptr = memory.ptr;
    match raw::repalloc_extended_raw(ptr.as_ptr().cast(), size, flags)? {
        Some((ptr, size)) => Ok(Some(PgMemory {
            ptr: ptr.cast(),
            size,
            _marker: PhantomData,
        })),
        None => Ok(None),
    }
}

pub fn repalloc0<'ctx>(
    mut memory: PgMemory<'ctx>,
    old_size: Size,
    size: Size,
) -> PgResult<PgMemory<'ctx>> {
    if old_size > size {
        return Err(PgError::error(format!(
            "invalid repalloc0 call: oldsize {old_size}, new size {size}"
        )));
    }
    let old_len = memory.len();
    memory = repalloc(memory, size)?;
    let zero_from = old_size.min(old_len);
    if size > zero_from {
        memory.as_mut_slice()[zero_from..].fill(0);
    }
    Ok(memory)
}

pub fn repalloc_huge<'ctx>(memory: PgMemory<'ctx>, size: Size) -> PgResult<PgMemory<'ctx>> {
    repalloc_extended(memory, size, MCXT_ALLOC_HUGE).and_then(required_allocation)
}

pub fn repalloc_owned(memory: PgOwnedMemory, size: Size) -> PgResult<PgOwnedMemory> {
    let ptr = memory.ptr;
    let (ptr, size) = raw::repalloc_raw(ptr.as_ptr().cast(), size, 0)?;
    std::mem::forget(memory);
    Ok(PgOwnedMemory {
        ptr: ptr.cast(),
        size,
        _marker: PhantomData,
    })
}

pub fn repalloc0_owned(
    mut memory: PgOwnedMemory,
    old_size: Size,
    size: Size,
) -> PgResult<PgOwnedMemory> {
    if old_size > size {
        return Err(PgError::error(format!(
            "invalid repalloc0 call: oldsize {old_size}, new size {size}"
        )));
    }
    let old_len = memory.len();
    memory = repalloc_owned(memory, size)?;
    let zero_from = old_size.min(old_len);
    if size > zero_from {
        memory.as_mut_slice()[zero_from..].fill(0);
    }
    Ok(memory)
}

pub fn repalloc_huge_owned(memory: PgOwnedMemory, size: Size) -> PgResult<PgOwnedMemory> {
    let ptr = memory.ptr;
    let (ptr, size) = raw::repalloc_raw(ptr.as_ptr().cast(), size, MCXT_ALLOC_HUGE)?;
    std::mem::forget(memory);
    Ok(PgOwnedMemory {
        ptr: ptr.cast(),
        size,
        _marker: PhantomData,
    })
}

pub fn MemoryContextStrdup<'ctx>(
    scope: &MemoryContextScope<'ctx>,
    input: &CStr,
) -> PgResult<PgCString<'ctx>> {
    let bytes = input.to_bytes_with_nul();
    let mut memory = palloc(scope, bytes.len())?;
    memory.as_mut_slice().copy_from_slice(bytes);
    Ok(PgCString { memory })
}

pub fn pstrdup<'ctx>(scope: &MemoryContextScope<'ctx>, input: &CStr) -> PgResult<PgCString<'ctx>> {
    MemoryContextStrdup(scope, input)
}

pub fn pnstrdup<'ctx>(
    scope: &MemoryContextScope<'ctx>,
    input: &[u8],
    len: Size,
) -> PgResult<PgCString<'ctx>> {
    let end = input
        .iter()
        .take(len)
        .position(|byte| *byte == 0)
        .unwrap_or_else(|| input.len().min(len));
    let cstring = CString::new(&input[..end]).map_err(|error| PgError::error(error.to_string()))?;
    pstrdup(scope, cstring.as_c_str())
}

pub fn pchomp<'ctx>(scope: &MemoryContextScope<'ctx>, input: &CStr) -> PgResult<PgCString<'ctx>> {
    let bytes = input.to_bytes();
    let end = bytes
        .strip_suffix(b"\n")
        .or_else(|| bytes.strip_suffix(b"\r"))
        .unwrap_or(bytes)
        .len();
    pnstrdup(scope, bytes, end)
}

#[derive(Debug)]
pub struct PgCString<'ctx> {
    memory: PgMemory<'ctx>,
}

impl<'ctx> PgCString<'ctx> {
    pub fn as_c_str(&self) -> &CStr {
        unsafe { CStr::from_ptr(self.memory.as_ptr().cast()) }
    }

    pub fn into_memory(self) -> PgMemory<'ctx> {
        self.memory
    }

    pub fn pfree(self) -> PgResult<()> {
        self.into_memory().pfree()
    }
}

pub unsafe fn MemoryContextReset(context: PgMemoryContext) -> PgResult<()> {
    raw::reset_context(context.as_ptr())
}

pub unsafe fn MemoryContextResetOnly(context: PgMemoryContext) -> PgResult<()> {
    raw::reset_context_only(context.as_ptr())
}

pub unsafe fn MemoryContextResetChildren(context: PgMemoryContext) -> PgResult<()> {
    raw::reset_children(context.as_ptr())
}

pub unsafe fn MemoryContextDelete(context: PgMemoryContext) -> PgResult<()> {
    raw::delete_context(context.as_ptr())
}

pub unsafe fn MemoryContextDeleteChildren(context: PgMemoryContext) -> PgResult<()> {
    raw::delete_children(context.as_ptr())
}

pub fn MemoryContextSetIdentifier(
    context: PgMemoryContext,
    ident: impl Into<String>,
) -> PgResult<()> {
    raw::set_identifier(context.as_ptr(), ident.into())
}

pub fn MemoryContextSetParent(
    context: PgMemoryContext,
    new_parent: Option<PgMemoryContext>,
) -> PgResult<()> {
    raw::set_parent(context.as_ptr(), new_parent.map(PgMemoryContext::as_ptr))
}

pub fn MemoryContextGetParent(context: PgMemoryContext) -> PgResult<Option<PgMemoryContext>> {
    unsafe {
        raw::validate_context_public(context.as_ptr())?;
        let parent = (*context.as_ptr()).parent;
        if parent.is_null() {
            Ok(None)
        } else {
            context_from_raw(parent).map(Some)
        }
    }
}

pub fn MemoryContextIsEmpty(context: PgMemoryContext) -> PgResult<bool> {
    raw::is_empty(context.as_ptr())
}

pub fn MemoryContextMemAllocated(context: PgMemoryContext, recurse: bool) -> PgResult<Size> {
    raw::mem_allocated(context.as_ptr(), recurse)
}

pub fn MemoryContextMemConsumed(context: PgMemoryContext) -> PgResult<MemoryContextCounters> {
    raw::mem_consumed(context.as_ptr())
}

pub fn MemoryContextStats(context: PgMemoryContext) -> PgResult<String> {
    MemoryContextStatsDetail(context, usize::MAX, usize::MAX, false)
}

pub fn MemoryContextStatsDetail(
    context: PgMemoryContext,
    max_level: usize,
    max_children: usize,
    _print_to_stderr: bool,
) -> PgResult<String> {
    raw::stats_detail(context.as_ptr(), max_level, max_children)
}

pub fn MemoryContextCheck(context: PgMemoryContext) -> PgResult<()> {
    raw::check_context(context.as_ptr())
}

pub fn GetMemoryChunkContext(pointer: *mut u8) -> PgResult<PgMemoryContext> {
    let context = raw::get_chunk_context(pointer.cast())?;
    context_from_raw(context)
}

pub fn GetMemoryChunkSpace(pointer: *mut u8) -> PgResult<Size> {
    raw::get_chunk_space(pointer.cast())
}

pub fn MemoryContextAllowInCriticalSection(context: PgMemoryContext, allow: bool) -> PgResult<()> {
    raw::allow_in_critical_section(context.as_ptr(), allow)
}

pub fn MemoryContextRegisterResetCallback(
    context: PgMemoryContext,
    cb: &mut MemoryContextCallback,
) -> PgResult<()> {
    raw::register_reset_callback(context.as_ptr(), cb)
}

fn context_from_raw(raw: MemoryContext) -> PgResult<PgMemoryContext> {
    NonNull::new(raw)
        .map(|raw| PgMemoryContext {
            raw,
            _marker: PhantomData,
        })
        .ok_or_else(|| PgError::error("memory context is null"))
}

fn required_allocation<T>(allocation: Option<T>) -> PgResult<T> {
    allocation.ok_or_else(|| PgError::error("out of memory"))
}

fn array_size<T>(len: usize) -> PgResult<Size> {
    len.checked_mul(size_of::<T>())
        .ok_or_else(|| PgError::error("out of memory"))
}

fn scoped_memory<'ctx>((ptr, size): (NonNull<std::ffi::c_void>, Size)) -> PgMemory<'ctx> {
    PgMemory {
        ptr: ptr.cast(),
        size,
        _marker: PhantomData,
    }
}

fn owned_memory((ptr, size): (NonNull<std::ffi::c_void>, Size)) -> PgOwnedMemory {
    PgOwnedMemory {
        ptr: ptr.cast(),
        size,
        _marker: PhantomData,
    }
}

fn pg_array_from_memory<'ctx, T: PgZeroable>(
    memory: PgMemory<'ctx>,
    len: usize,
) -> PgResult<PgArray<'ctx, T>> {
    if size_of::<T>() != 0 && (memory.as_ptr() as usize) % align_of::<T>() != 0 {
        return Err(PgError::error("memory allocation is not properly aligned"));
    }
    Ok(PgArray {
        memory,
        len,
        _marker: PhantomData,
    })
}

fn pg_owned_array_from_memory<T: PgZeroable>(
    memory: PgOwnedMemory,
    len: usize,
) -> PgResult<PgOwnedArray<T>> {
    if size_of::<T>() != 0 && (memory.as_ptr() as usize) % align_of::<T>() != 0 {
        return Err(PgError::error("memory allocation is not properly aligned"));
    }
    Ok(PgOwnedArray {
        memory,
        len,
        _marker: PhantomData,
    })
}

#[cfg(test)]
mod tests {
    use std::ffi::c_void;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    static CALLBACKS: AtomicUsize = AtomicUsize::new(0);

    unsafe extern "C" fn count_callback(_arg: *mut c_void) {
        CALLBACKS.fetch_add(1, Ordering::SeqCst);
    }

    #[test]
    fn memory_context_init_sets_current_top_and_error() {
        MemoryContextInit().unwrap();

        assert!(!raw::top_memory_context().is_null());
        assert!(!raw::error_context().is_null());
        assert_eq!(raw::current_memory_context(), raw::top_memory_context());
    }

    #[test]
    fn palloc_uses_current_context_and_realloc_preserves_prefix() {
        let top = PgMemoryContext::top().unwrap();
        let child = AllocSetContextCreateInternal(
            Some(top),
            "test child",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE,
        )
        .unwrap();
        let _guard = child.switch_to().unwrap();
        let mut memory = palloc0_owned(4).unwrap();

        memory.as_mut_slice().copy_from_slice(b"test");
        let memory = repalloc_owned(memory, 6).unwrap();

        assert_eq!(&memory.as_slice()[..4], b"test");
        assert_eq!(
            GetMemoryChunkContext(memory.as_ptr()).unwrap().as_ptr(),
            child.as_ptr()
        );
        MemoryContextCheck(child).unwrap();
    }

    #[test]
    fn failed_repalloc_owned_does_not_leak_original_allocation() {
        let context = OwnedMemoryContext::alloc_set(
            Some(PgMemoryContext::top().unwrap()),
            "repalloc error cleanup",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE,
        )
        .unwrap();
        let memory = MemoryContextAllocOwned(context.as_context(), 16).unwrap();
        let ptr = memory.as_ptr();

        assert!(repalloc_owned(memory, usize::MAX).is_err());
        let next = MemoryContextAllocOwned(context.as_context(), 16).unwrap();
        assert_eq!(next.as_ptr(), ptr);
    }

    #[test]
    fn reset_frees_allocations_and_callbacks_fire_once() {
        CALLBACKS.store(0, Ordering::SeqCst);
        let top = PgMemoryContext::top().unwrap();
        let child = GenerationContextCreate(Some(top), "generation", 0, 1024, 8192).unwrap();
        let mut cb = MemoryContextCallback {
            func: Some(count_callback),
            arg: std::ptr::null_mut(),
            next: std::ptr::null_mut(),
        };
        MemoryContextRegisterResetCallback(child, &mut cb).unwrap();
        let memory = child.alloc_owned(8).unwrap();
        std::mem::forget(memory);

        assert!(!child.is_empty().unwrap());
        MemoryContextCheck(child).unwrap();
        unsafe { child.reset() }.unwrap();

        assert!(child.is_empty().unwrap());
        assert_eq!(CALLBACKS.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn delete_child_unlinks_from_parent() {
        let top = PgMemoryContext::top().unwrap();
        let child = SlabContextCreate(Some(top), "slab", SLAB_DEFAULT_BLOCK_SIZE, 32).unwrap();

        assert!(top.mem_allocated(true).unwrap() >= top.mem_allocated(false).unwrap());
        unsafe { child.delete() }.unwrap();
        assert!(MemoryContextGetParent(child).is_err());
    }

    #[test]
    fn stats_detail_reports_context_tree_and_totals() {
        let top = PgMemoryContext::top().unwrap();
        let child = AllocSetContextCreateInternal(
            Some(top),
            "stats child",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE,
        )
        .unwrap();
        let _memory = child.alloc_owned(24).unwrap();

        let stats = MemoryContextStatsDetail(top, 1, usize::MAX, false).unwrap();

        assert!(stats.contains("TopMemoryContext"));
        assert!(stats.contains("stats child"));
        assert!(stats.contains("Grand total:"));
    }

    #[test]
    fn bump_rejects_free_and_realloc() {
        let top = PgMemoryContext::top().unwrap();
        let bump = BumpContextCreate(Some(top), "bump", 0, 1024, 8192).unwrap();
        let memory = bump.alloc_owned(8).unwrap();
        let ptr = memory.into_raw();

        assert!(unsafe { pfree(ptr) }.is_err());
        assert!(raw::repalloc_raw(ptr.cast(), 16, 0).is_err());
        assert!(GetMemoryChunkContext(ptr).is_err());
        assert!(!bump.is_empty().unwrap());
        MemoryContextCheck(bump).unwrap();
        unsafe { bump.reset() }.unwrap();
        assert!(bump.is_empty().unwrap());
    }

    #[test]
    fn aligned_allocation_returns_requested_alignment() {
        let top = PgMemoryContext::top().unwrap();
        let mut memory = MemoryContextAllocAlignedOwned(top, 16, 64, 0)
            .unwrap()
            .unwrap();

        assert_eq!((memory.as_ptr() as usize) % 64, 0);
        assert_eq!(
            GetMemoryChunkContext(memory.as_ptr()).unwrap().as_ptr(),
            top.as_ptr()
        );
        assert!(GetMemoryChunkSpace(memory.as_ptr()).unwrap() > memory.len());

        memory.as_mut_slice().copy_from_slice(b"abcdefghijklmnop");
        let memory = repalloc_owned(memory, 32).unwrap();

        assert_eq!((memory.as_ptr() as usize) % 64, 0);
        assert_eq!(&memory.as_slice()[..16], b"abcdefghijklmnop");
    }

    #[test]
    fn no_oom_flag_returns_none_for_invalid_size() {
        let top = PgMemoryContext::top().unwrap();
        let memory = MemoryContextAllocExtendedOwned(top, usize::MAX, MCXT_ALLOC_NO_OOM).unwrap();

        assert!(memory.is_none());
    }

    #[test]
    fn c_string_helpers_allocate_in_scope_context() {
        let top = PgMemoryContext::top().unwrap();
        let context = OwnedMemoryContext::alloc_set(
            Some(top),
            "cstring helpers",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE,
        )
        .unwrap();
        let scope = context.scope();
        let cstring = pstrdup(&scope, c"hello").unwrap();
        assert_eq!(cstring.as_c_str(), c"hello");

        let chomped = pchomp(&scope, c"hello\n").unwrap();
        assert_eq!(chomped.as_c_str(), c"hello");
    }

    #[test]
    fn owned_context_allocates_lifetime_bound_object() {
        let top = PgMemoryContext::top().unwrap();
        let context = OwnedMemoryContext::alloc_set(
            Some(top),
            "owned context",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE,
        )
        .unwrap();

        let value = context.alloc_object(String::from("hello")).unwrap();

        assert_eq!(&**value, "hello");
    }

    #[test]
    fn owned_context_allocates_lifetime_bound_bytes() {
        let top = PgMemoryContext::top().unwrap();
        let context = OwnedMemoryContext::alloc_set(
            Some(top),
            "owned bytes",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE,
        )
        .unwrap();
        let mut memory = context.alloc_zeroed_bytes(4).unwrap();

        memory.as_mut_slice().copy_from_slice(b"test");

        assert_eq!(memory.as_slice(), b"test");
    }

    #[test]
    fn palloc0_array_returns_zeroed_typed_slice() {
        let top = PgMemoryContext::top().unwrap();
        let context = OwnedMemoryContext::alloc_set(
            Some(top),
            "typed array",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE,
        )
        .unwrap();
        let scope = context.scope();
        let mut array = palloc0_array::<u64>(&scope, 4).unwrap();

        assert_eq!(array.len(), 4);
        assert_eq!(array.as_slice(), &[0, 0, 0, 0]);
        array.as_mut_slice()[2] = 42;
        assert_eq!(array.as_slice()[2], 42);
        assert_eq!((array.as_ptr() as usize) % align_of::<u64>(), 0);
    }

    #[test]
    fn scoped_memory_pfree_releases_chunk() {
        let top = PgMemoryContext::top().unwrap();
        let context = OwnedMemoryContext::alloc_set(
            Some(top),
            "scoped pfree",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE,
        )
        .unwrap();
        let scope = context.scope();
        let memory = palloc(&scope, 16).unwrap();
        let ptr = memory.as_ptr();

        memory.pfree().unwrap();
        let next = palloc(&scope, 16).unwrap();

        assert_eq!(next.as_ptr(), ptr);
    }

    #[test]
    fn palloc0_array_detects_size_overflow() {
        let top = PgMemoryContext::top().unwrap();
        let context = OwnedMemoryContext::alloc_set(
            Some(top),
            "typed array overflow",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE,
        )
        .unwrap();
        let scope = context.scope();
        assert!(palloc0_array::<u64>(&scope, usize::MAX).is_err());
    }

    #[test]
    fn allocset_reports_bucket_chunk_space() {
        let top = PgMemoryContext::top().unwrap();
        let context = AllocSetContextCreateInternal(
            Some(top),
            "allocset chunk space",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE,
        )
        .unwrap();

        let memory = MemoryContextAllocOwned(context, 4).unwrap();

        assert!(GetMemoryChunkSpace(memory.as_ptr()).unwrap() > memory.len());
    }

    #[test]
    fn allocset_reuses_freed_small_chunks() {
        let top = PgMemoryContext::top().unwrap();
        let context = AllocSetContextCreateInternal(
            Some(top),
            "allocset reuse",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE,
        )
        .unwrap();

        let first = MemoryContextAllocOwned(context, 16).unwrap();
        let first_ptr = first.into_raw();
        unsafe { pfree(first_ptr) }.unwrap();

        let second = MemoryContextAllocOwned(context, 16).unwrap();

        assert_eq!(second.as_ptr(), first_ptr);
    }

    #[test]
    fn slab_enforces_fixed_chunk_size_and_reuses_chunks() {
        let top = PgMemoryContext::top().unwrap();
        let context =
            SlabContextCreate(Some(top), "slab fixed", SLAB_DEFAULT_BLOCK_SIZE, 32).unwrap();

        let first = MemoryContextAllocOwned(context, 32).unwrap();
        assert_eq!(GetMemoryChunkSpace(first.as_ptr()).unwrap(), 40);
        let first_ptr = first.into_raw();
        unsafe { pfree(first_ptr) }.unwrap();

        let second = MemoryContextAllocOwned(context, 32).unwrap();
        assert_eq!(second.as_ptr(), first_ptr);
        assert!(MemoryContextAllocOwned(context, 16).is_err());
        MemoryContextCheck(context).unwrap();
    }

    #[test]
    fn top_context_is_thread_local() {
        let main_top = PgMemoryContext::top().unwrap().as_ptr() as usize;
        let worker_top = std::thread::spawn(|| PgMemoryContext::top().unwrap().as_ptr() as usize)
            .join()
            .unwrap();

        assert_ne!(main_top, worker_top);
    }
}
