//! Carrier types for the block-reference table (`common/blkreftable.c`).
//!
//! `BlockRefTable` and `BlockRefTableReader` are incomplete types in
//! `common/blkreftable.h` (their definitions are private to `blkreftable.c`):
//! callers only ever hold opaque `BlockRefTable *` / `BlockRefTableReader *`
//! pointers. The owning unit (`common-blkreftable`) is ported, so these are the
//! genuine owned structs the owner constructs and mutates and the consumers
//! thread by `&` / `&mut`. They live in this carrier crate (the moral
//! equivalent of `blkreftable.h`) so the owner's seam declarations
//! (`common-blkreftable-seams`, which cannot depend on the owner without a
//! cycle) can name them. The fields are `pub` because the owner — a separate
//! crate — manipulates them directly; consumers only ever hold references and
//! drive the owner's seam/`pub fn` API, never the fields.

#![no_std]

extern crate alloc;

use alloc::boxed::Box;
use alloc::string::String;
use alloc::vec::Vec;

use hashbrown::HashMap;
use types_core::{uint16, uint32, BlockNumber, ForkNumber};
use types_storage::RelFileLocator;

// ---------------------------------------------------------------------------
// Key / entry (`BlockRefTableKey`, `struct BlockRefTableEntry`).
// ---------------------------------------------------------------------------

/// `typedef struct BlockRefTableKey { RelFileLocator rlocator; ForkNumber forknum; }`.
///
/// The C `SH_HASH_KEY` hashes `sizeof(BlockRefTableKey)` raw bytes and
/// `SH_EQUAL` is a `memcmp`; the owner reproduces that via a canonical 16-byte
/// serialization so the hash matches the C layout.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct BlockRefTableKey {
    pub rlocator: RelFileLocator,
    pub forknum: ForkNumber,
}

/// `struct BlockRefTableEntry` (blkreftable.c).
///
/// The three parallel arrays are kept exactly as in C. `chunk_data[c]` is the
/// `uint16 *` chunk body; its allocated length is `chunk_size[c]` and the number
/// of meaningful slots is `chunk_usage[c]` (`== MAX_ENTRIES_PER_CHUNK` means the
/// chunk is a bitmap). `nchunks` is the common allocated length of the three
/// arrays.
#[derive(Clone, Debug)]
pub struct BlockRefTableEntry {
    pub key: BlockRefTableKey,
    pub limit_block: BlockNumber,
    pub nchunks: uint32,
    pub chunk_size: Vec<uint16>,
    pub chunk_usage: Vec<uint16>,
    pub chunk_data: Vec<Vec<uint16>>,
}

// ---------------------------------------------------------------------------
// The in-memory table (`struct BlockRefTable`).
// ---------------------------------------------------------------------------

/// `struct BlockRefTable { blockreftable_hash *hash; MemoryContext mcxt; }`.
///
/// The simplehash over `BlockRefTableEntry` becomes a [`HashMap`] keyed by the
/// raw 16 key bytes (matching the C `SH_HASH_KEY` / `SH_EQUAL` memcmp over
/// `sizeof(BlockRefTableKey)`) carrying the entry. The C iterate order is
/// arbitrary and the writer sorts before emitting, so iteration order does not
/// affect output. The owner manipulates `hash` directly.
pub struct BlockRefTable {
    /// Entries keyed by the canonical 16 key bytes (the C `memcmp` key).
    pub hash: HashMap<[u8; 16], BlockRefTableEntry>,
}

impl BlockRefTable {
    pub fn new() -> Self {
        BlockRefTable {
            hash: HashMap::new(),
        }
    }
}

impl Default for BlockRefTable {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Reader (`struct BlockRefTableReader` + read-side `struct BlockRefTableBuffer`).
// ---------------------------------------------------------------------------

/// `io_callback_fn` on the read path: `fn(callback_arg, data, length) -> bytes_read`.
/// The boxed closure reads up to `data.len()` bytes into `data`, returning the
/// number actually read (0 = EOF), mirroring the C `read_callback`.
pub type ReadCallback = Box<dyn FnMut(&mut [u8]) -> usize>;

/// Read-side buffer (`struct BlockRefTableBuffer`).
pub struct ReadBuffer {
    pub io_callback: ReadCallback,
    pub data: Vec<u8>,
    pub used: usize,
    pub cursor: usize,
    pub crc: u32,
}

/// `struct BlockRefTableReader`.
pub struct BlockRefTableReader {
    pub buffer: ReadBuffer,
    pub error_filename: String,
    pub total_chunks: uint32,
    pub consumed_chunks: uint32,
    pub chunk_size: Vec<uint16>,
    pub chunk_data: Vec<uint16>,
    pub chunk_position: uint32,
}
