use core::ffi::{c_char, c_int, c_void};

use crate::types::{uint64, NodeTag, Size};

pub const MAX_ALLOC_SIZE: Size = 0x3fff_ffff;
pub const MAX_ALLOC_HUGE_SIZE: Size = Size::MAX / 2;
pub const INVALID_ALLOC_SIZE: Size = Size::MAX;

pub const MCXT_ALLOC_HUGE: c_int = 0x01;
pub const MCXT_ALLOC_NO_OOM: c_int = 0x02;
pub const MCXT_ALLOC_ZERO: c_int = 0x04;

pub const ALLOCSET_DEFAULT_MINSIZE: Size = 0;
pub const ALLOCSET_DEFAULT_INITSIZE: Size = 8 * 1024;
pub const ALLOCSET_DEFAULT_MAXSIZE: Size = 8 * 1024 * 1024;
pub const ALLOCSET_SMALL_MINSIZE: Size = 0;
pub const ALLOCSET_SMALL_INITSIZE: Size = 1024;
pub const ALLOCSET_SMALL_MAXSIZE: Size = 8 * 1024;
pub const ALLOCSET_SEPARATE_THRESHOLD: Size = 8192;
pub const SLAB_DEFAULT_BLOCK_SIZE: Size = 8 * 1024;
pub const SLAB_LARGE_BLOCK_SIZE: Size = 8 * 1024 * 1024;

pub const T_ALLOC_SET_CONTEXT: NodeTag = 474;
pub const T_GENERATION_CONTEXT: NodeTag = 475;
pub const T_SLAB_CONTEXT: NodeTag = 476;
pub const T_BUMP_CONTEXT: NodeTag = 477;

pub type MemoryContextMethodID = u32;
pub const MCTX_0_RESERVED_UNUSEDMEM_ID: MemoryContextMethodID = 0;
pub const MCTX_1_RESERVED_GLIBC_ID: MemoryContextMethodID = 1;
pub const MCTX_2_RESERVED_GLIBC_ID: MemoryContextMethodID = 2;
pub const MCTX_ASET_ID: MemoryContextMethodID = 3;
pub const MCTX_GENERATION_ID: MemoryContextMethodID = 4;
pub const MCTX_SLAB_ID: MemoryContextMethodID = 5;
pub const MCTX_ALIGNED_REDIRECT_ID: MemoryContextMethodID = 6;
pub const MCTX_BUMP_ID: MemoryContextMethodID = 7;
pub const MCTX_8_UNUSED_ID: MemoryContextMethodID = 8;
pub const MCTX_9_UNUSED_ID: MemoryContextMethodID = 9;
pub const MCTX_10_UNUSED_ID: MemoryContextMethodID = 10;
pub const MCTX_11_UNUSED_ID: MemoryContextMethodID = 11;
pub const MCTX_12_UNUSED_ID: MemoryContextMethodID = 12;
pub const MCTX_13_UNUSED_ID: MemoryContextMethodID = 13;
pub const MCTX_14_UNUSED_ID: MemoryContextMethodID = 14;
pub const MCTX_15_RESERVED_WIPEDMEM_ID: MemoryContextMethodID = 15;

pub const MEMORY_CONTEXT_METHODID_BITS: u32 = 4;
pub const MEMORY_CONTEXT_METHODID_MASK: uint64 = (1 << MEMORY_CONTEXT_METHODID_BITS) - 1;
pub const MEMORYCHUNK_MAX_VALUE: uint64 = 0x3fff_ffff;
pub const MEMORYCHUNK_MAX_BLOCKOFFSET: uint64 = 0x3fff_ffff;

#[repr(C)]
pub struct MemoryContextData {
    pub type_: NodeTag,
    pub isReset: bool,
    pub allowInCritSection: bool,
    pub mem_allocated: Size,
    pub methods: *const MemoryContextMethods,
    pub parent: MemoryContext,
    pub firstchild: MemoryContext,
    pub prevchild: MemoryContext,
    pub nextchild: MemoryContext,
    pub name: *const c_char,
    pub ident: *const c_char,
    pub reset_cbs: *mut MemoryContextCallback,
}

pub type MemoryContext = *mut MemoryContextData;
pub type MemoryContextCallbackFunction = Option<unsafe extern "C" fn(*mut c_void)>;
pub type MemoryStatsPrintFunc =
    Option<unsafe extern "C" fn(MemoryContext, *mut c_void, *const c_char, bool)>;

pub type MemoryAllocFn = Option<unsafe extern "C" fn(MemoryContext, Size, c_int) -> *mut c_void>;
pub type MemoryFreeFn = Option<unsafe extern "C" fn(*mut c_void)>;
pub type MemoryReallocFn = Option<unsafe extern "C" fn(*mut c_void, Size, c_int) -> *mut c_void>;
pub type MemoryResetFn = Option<unsafe extern "C" fn(MemoryContext)>;
pub type MemoryDeleteFn = Option<unsafe extern "C" fn(MemoryContext)>;
pub type MemoryGetChunkContextFn = Option<unsafe extern "C" fn(*mut c_void) -> MemoryContext>;
pub type MemoryGetChunkSpaceFn = Option<unsafe extern "C" fn(*mut c_void) -> Size>;
pub type MemoryIsEmptyFn = Option<unsafe extern "C" fn(MemoryContext) -> bool>;
pub type MemoryStatsFn = Option<
    unsafe extern "C" fn(
        MemoryContext,
        MemoryStatsPrintFunc,
        *mut c_void,
        *mut MemoryContextCounters,
        bool,
    ),
>;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct MemoryContextMethods {
    pub alloc: MemoryAllocFn,
    pub free_p: MemoryFreeFn,
    pub realloc: MemoryReallocFn,
    pub reset: MemoryResetFn,
    pub delete_context: MemoryDeleteFn,
    pub get_chunk_context: MemoryGetChunkContextFn,
    pub get_chunk_space: MemoryGetChunkSpaceFn,
    pub is_empty: MemoryIsEmptyFn,
    pub stats: MemoryStatsFn,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MemoryContextCounters {
    pub nblocks: Size,
    pub freechunks: Size,
    pub totalspace: Size,
    pub freespace: Size,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct MemoryContextCallback {
    pub func: MemoryContextCallbackFunction,
    pub arg: *mut c_void,
    pub next: *mut MemoryContextCallback,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MemoryChunk {
    pub hdrmask: uint64,
}

impl MemoryChunk {
    const EXTERNAL_BASEBIT: u32 = MEMORY_CONTEXT_METHODID_BITS;
    const VALUE_BASEBIT: u32 = Self::EXTERNAL_BASEBIT + 1;
    const BLOCKOFFSET_BASEBIT: u32 = Self::VALUE_BASEBIT + 29;
    const BLOCKOFFSET_MASK: uint64 = 0x3fff_fffe;
    const MAGIC: uint64 = (0xb1a8_db85_8eb6_efba_u64 >> Self::VALUE_BASEBIT) << Self::VALUE_BASEBIT;

    pub fn set_hdrmask(
        &mut self,
        block_offset: Size,
        value: Size,
        method_id: MemoryContextMethodID,
    ) {
        self.hdrmask = ((block_offset as uint64) << Self::BLOCKOFFSET_BASEBIT)
            | ((value as uint64) << Self::VALUE_BASEBIT)
            | method_id as uint64;
    }

    pub fn set_external(&mut self, method_id: MemoryContextMethodID) {
        self.hdrmask = Self::MAGIC | (1 << Self::EXTERNAL_BASEBIT) | method_id as uint64;
    }

    pub fn method_id(&self) -> MemoryContextMethodID {
        (self.hdrmask & MEMORY_CONTEXT_METHODID_MASK) as MemoryContextMethodID
    }

    pub fn is_external(&self) -> bool {
        self.hdrmask & (1 << Self::EXTERNAL_BASEBIT) != 0
    }

    pub fn value(&self) -> Size {
        ((self.hdrmask >> Self::VALUE_BASEBIT) & MEMORYCHUNK_MAX_VALUE) as Size
    }

    pub fn block_offset(&self) -> Size {
        ((self.hdrmask >> Self::BLOCKOFFSET_BASEBIT) & Self::BLOCKOFFSET_MASK) as Size
    }

    pub fn external_magic_ok(&self) -> bool {
        !self.is_external()
            || Self::MAGIC == (self.hdrmask >> Self::VALUE_BASEBIT) << Self::VALUE_BASEBIT
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_chunk_header_round_trips() {
        let mut chunk = MemoryChunk::default();
        chunk.set_hdrmask(128, 64, MCTX_ASET_ID);

        assert_eq!(chunk.method_id(), MCTX_ASET_ID);
        assert!(!chunk.is_external());
        assert_eq!(chunk.value(), 64);
        assert_eq!(chunk.block_offset(), 128);
    }

    #[test]
    fn external_memory_chunk_records_method_and_magic() {
        let mut chunk = MemoryChunk::default();
        chunk.set_external(MCTX_BUMP_ID);

        assert_eq!(chunk.method_id(), MCTX_BUMP_ID);
        assert!(chunk.is_external());
        assert!(chunk.external_magic_ok());
    }

    #[test]
    fn memory_context_layout_starts_with_node_tag() {
        assert_eq!(core::mem::offset_of!(MemoryContextData, type_), 0);
        assert_eq!(core::mem::size_of::<MemoryChunk>(), 8);
    }
}
