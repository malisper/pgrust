//! Block reference tables (`src/common/blkreftable.c`).
//!
//! A block reference table tracks which relation-fork blocks have been modified
//! by WAL records within an LSN range, plus a per-fork "limit block" (the
//! shortest known relation length over that range). It backs incremental backup
//! (the WAL summarizer writes these files; `pg_wal_summary_contents` reads
//! them).
//!
//! # Faithful model
//!
//! * The C `struct BlockRefTableEntry` stores, per relation fork, a `limit_block`
//!   and three parallel arrays indexed by chunk number: `chunk_size` (allocated
//!   capacity in `uint16` slots), `chunk_usage` (number of entries used, or
//!   `MAX_ENTRIES_PER_CHUNK` when the chunk is a bitmap), and `chunk_data` (the
//!   per-chunk `uint16 *`). We mirror this exactly with [`Vec`]s so the
//!   array<->bitmap conversion and the offset/bitmap encodings are bit-for-bit
//!   identical to C, which the on-disk format requires.
//! * `BLOCKS_PER_CHUNK = 2^16`. A chunk used as an array stores 2-byte offsets;
//!   used as a bitmap it stores `MAX_ENTRIES_PER_CHUNK` `uint16` words. The
//!   `chunk_usage == MAX_ENTRIES_PER_CHUNK` test distinguishes the two.
//! * Serialization writes a magic number, then sorted `BlockRefTableSerializedEntry`
//!   records each followed by the truncated `chunk_usage` array and the non-empty
//!   chunk bodies, terminated by an all-zero sentinel entry and a CRC-32C of
//!   everything before the CRC.
//!
//! # Repo reconciliation
//!
//! * The C `BlockRefTable *` and `BlockRefTableReader *` are opaque pointers.
//!   Here the in-memory table and the reader live in process-global registries
//!   (`thread_local!` like the other registry-token owners), named by the
//!   [`BlockRefTableHandle`] / [`BlockRefTableReaderHandle`] tokens the consumers
//!   already hold. This mirrors the C `palloc` into a long-lived memory context.
//! * The C buffered I/O over `io_callback_fn` is preserved: the writer buffers
//!   into a [`BlockRefTableBuffer`] and the consumer's [`write_block_ref_table`]
//!   seam returns the fully serialized bytes (the backend would instead stream
//!   them through its `WriteWalSummary` callback). The reader drives a boxed
//!   `read_callback` closure installed by the walsummary owner via
//!   [`create_block_ref_table_reader`].
//! * `hash_bytes` and `pg_comp_crc32c` are the primitives behind the C
//!   `SH_HASH_KEY` and `COMP_CRC32C`, reached through their seams.

extern crate alloc;

use alloc::boxed::Box;
use alloc::format;
use alloc::vec;
use alloc::vec::Vec;
use core::cell::RefCell;
use std::collections::HashMap;

use mcx::{Mcx, PgVec};
use types_blkreftable::{BlockRefTableHandle, BlockRefTableReaderHandle};
use types_core::{
    uint16, uint32, BlockNumber, ForkNumber, InvalidBlockNumber, BITS_PER_BYTE,
};
use types_error::{PgError, PgResult};
use types_storage::RelFileLocator;

// ---------------------------------------------------------------------------
// Constants (blkreftable.c).
// ---------------------------------------------------------------------------

/// `#define BLOCKS_PER_CHUNK (1 << 16)`.
const BLOCKS_PER_CHUNK: u32 = 1 << 16;
/// `#define BLOCKS_PER_ENTRY (BITS_PER_BYTE * sizeof(uint16))` = 16.
const BLOCKS_PER_ENTRY: u32 = (BITS_PER_BYTE as u32) * (core::mem::size_of::<uint16>() as u32);
/// `#define MAX_ENTRIES_PER_CHUNK (BLOCKS_PER_CHUNK / BLOCKS_PER_ENTRY)` = 4096.
const MAX_ENTRIES_PER_CHUNK: u32 = BLOCKS_PER_CHUNK / BLOCKS_PER_ENTRY;
/// `#define INITIAL_ENTRIES_PER_CHUNK 16`.
const INITIAL_ENTRIES_PER_CHUNK: u32 = 16;
/// `#define BUFSIZE 65536`.
const BUFSIZE: usize = 65536;
/// `#define BLOCKREFTABLE_MAGIC 0x652b137b` (`common/blkreftable.h`).
const BLOCKREFTABLE_MAGIC: u32 = 0x652b_137b;

/// On-disk size of `BlockRefTableSerializedEntry`: `RelFileLocator` (3 × `Oid`)
/// + `ForkNumber` (int) + `BlockNumber` (uint32) + `nchunks` (uint32), with the
/// C struct having no trailing padding (all 4-byte fields). 6 × 4 = 24 bytes.
const SERIALIZED_ENTRY_LEN: usize = 24;

// ---------------------------------------------------------------------------
// Key / entry (blkreftable.c `BlockRefTableKey`, `struct BlockRefTableEntry`).
// ---------------------------------------------------------------------------

/// `typedef struct BlockRefTableKey { RelFileLocator rlocator; ForkNumber forknum; }`.
///
/// The C `SH_HASH_KEY` hashes `sizeof(BlockRefTableKey)` raw bytes and
/// `SH_EQUAL` is a `memcmp`; we reproduce that exactly via the canonical
/// 16-byte serialization below so the hash matches the C layout.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
struct BlockRefTableKey {
    rlocator: RelFileLocator,
    forknum: ForkNumber,
}

impl BlockRefTableKey {
    /// The 16 raw key bytes the C `SH_HASH_KEY` hashes: `spcOid`, `dbOid`,
    /// `relNumber` (each a 4-byte `Oid`) then `forknum` (a 4-byte int), in C
    /// struct order with zero padding (`= {0}` in C ensures padding is zero;
    /// the layout has none).
    fn raw_bytes(&self) -> [u8; 16] {
        let mut b = [0u8; 16];
        b[0..4].copy_from_slice(&self.rlocator.spcOid.to_ne_bytes());
        b[4..8].copy_from_slice(&self.rlocator.dbOid.to_ne_bytes());
        b[8..12].copy_from_slice(&self.rlocator.relNumber.to_ne_bytes());
        b[12..16].copy_from_slice(&(self.forknum as i32).to_ne_bytes());
        b
    }
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
    key: BlockRefTableKey,
    limit_block: BlockNumber,
    nchunks: uint32,
    chunk_size: Vec<uint16>,
    chunk_usage: Vec<uint16>,
    chunk_data: Vec<Vec<uint16>>,
}

impl BlockRefTableEntry {
    /// A freshly inserted entry with no chunks (the `!found` arm of the C
    /// insert helpers initializes exactly these fields).
    fn empty(key: BlockRefTableKey, limit_block: BlockNumber) -> Self {
        BlockRefTableEntry {
            key,
            limit_block,
            nchunks: 0,
            chunk_size: Vec::new(),
            chunk_usage: Vec::new(),
            chunk_data: Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// The in-memory table (`struct BlockRefTable`).
// ---------------------------------------------------------------------------

/// `struct BlockRefTable { blockreftable_hash *hash; MemoryContext mcxt; }`.
///
/// The simplehash over `BlockRefTableEntry` becomes a `HashMap` keyed by the
/// raw key bytes (matching the C `memcmp` equality) carrying the entry. We keep
/// insertion-time tagging too: the C iterate order is arbitrary, and the writer
/// sorts before emitting, so iteration order does not affect output.
pub struct BlockRefTable {
    hash: HashMap<[u8; 16], BlockRefTableEntry>,
}

impl BlockRefTable {
    fn new() -> Self {
        BlockRefTable {
            hash: HashMap::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// On-disk serialized entry (`BlockRefTableSerializedEntry`).
// ---------------------------------------------------------------------------

/// `typedef struct BlockRefTableSerializedEntry { RelFileLocator rlocator;
/// ForkNumber forknum; BlockNumber limit_block; uint32 nchunks; }`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct BlockRefTableSerializedEntry {
    rlocator: RelFileLocator,
    forknum: ForkNumber,
    limit_block: BlockNumber,
    nchunks: uint32,
}

impl BlockRefTableSerializedEntry {
    /// The C `BlockRefTableComparator`: sort by tablespace, then database, then
    /// relfilenumber, then fork number.
    fn cmp_key(&self) -> (u32, u32, u32, i32) {
        (
            self.rlocator.spcOid,
            self.rlocator.dbOid,
            self.rlocator.relNumber,
            self.forknum as i32,
        )
    }

    /// Serialize to the 24 on-disk bytes in C struct order.
    fn to_bytes(&self) -> [u8; SERIALIZED_ENTRY_LEN] {
        let mut b = [0u8; SERIALIZED_ENTRY_LEN];
        b[0..4].copy_from_slice(&self.rlocator.spcOid.to_ne_bytes());
        b[4..8].copy_from_slice(&self.rlocator.dbOid.to_ne_bytes());
        b[8..12].copy_from_slice(&self.rlocator.relNumber.to_ne_bytes());
        b[12..16].copy_from_slice(&(self.forknum as i32).to_ne_bytes());
        b[16..20].copy_from_slice(&self.limit_block.to_ne_bytes());
        b[20..24].copy_from_slice(&self.nchunks.to_ne_bytes());
        b
    }

    /// Deserialize the 24 on-disk bytes.
    fn from_bytes(b: &[u8; SERIALIZED_ENTRY_LEN]) -> Self {
        let spc = u32::from_ne_bytes(b[0..4].try_into().unwrap());
        let db = u32::from_ne_bytes(b[4..8].try_into().unwrap());
        let rel = u32::from_ne_bytes(b[8..12].try_into().unwrap());
        let fork_i = i32::from_ne_bytes(b[12..16].try_into().unwrap());
        let limit_block = u32::from_ne_bytes(b[16..20].try_into().unwrap());
        let nchunks = u32::from_ne_bytes(b[20..24].try_into().unwrap());
        BlockRefTableSerializedEntry {
            rlocator: RelFileLocator {
                spcOid: spc,
                dbOid: db,
                relNumber: rel,
            },
            // ForkNumber decode: the on-disk value is whatever the writer
            // stored. We carry it through as i32 in the public seam, but
            // need a ForkNumber for the seam return; map known values and
            // preserve the raw int otherwise via from_i32 fallback.
            forknum: ForkNumber::from_i32(fork_i).unwrap_or(ForkNumber::InvalidForkNumber),
            limit_block,
            nchunks,
        }
    }
}

// ---------------------------------------------------------------------------
// Buffered I/O over a callback (`struct BlockRefTableBuffer`).
// ---------------------------------------------------------------------------

/// `INIT_CRC32C(crc)` — `crc = 0xFFFFFFFF`.
const fn init_crc32c() -> u32 {
    0xFFFF_FFFF
}

/// `FIN_CRC32C(crc)` — `crc ^= 0xFFFFFFFF`.
const fn fin_crc32c(crc: u32) -> u32 {
    crc ^ 0xFFFF_FFFF
}

/// `COMP_CRC32C(crc, data, len)` via the crc32c seam.
fn comp_crc32c(crc: u32, data: &[u8]) -> u32 {
    port_pg_crc32c_seams::pg_comp_crc32c::call(crc, data)
}

/// Write-side buffer (`struct BlockRefTableBuffer` on the write path): collects
/// bytes, computing the running CRC. The C version flushes to a file via
/// `io_callback`; we accumulate into `out` (the seam returns these bytes), which
/// is behaviorally identical to flushing into an in-memory sink.
struct WriteBuffer {
    data: Vec<u8>,
    crc: u32,
    out: Vec<u8>,
}

impl WriteBuffer {
    fn new() -> Self {
        WriteBuffer {
            data: Vec::with_capacity(BUFSIZE),
            crc: init_crc32c(),
            out: Vec::new(),
        }
    }

    /// `BlockRefTableWrite(buffer, data, length)`.
    fn write(&mut self, data: &[u8]) {
        // Update running CRC calculation.
        self.crc = comp_crc32c(self.crc, data);

        // If the new data can't fit into the buffer, flush the buffer.
        if self.data.len() + data.len() > BUFSIZE {
            self.out.extend_from_slice(&self.data);
            self.data.clear();
        }

        // If the new data would fill the buffer, or more, write it directly.
        if data.len() >= BUFSIZE {
            self.out.extend_from_slice(data);
            return;
        }

        // Otherwise, copy the new data into the buffer.
        self.data.extend_from_slice(data);
        debug_assert!(self.data.len() <= BUFSIZE);
    }

    /// `BlockRefTableFlush(buffer)`.
    fn flush(&mut self) {
        self.out.extend_from_slice(&self.data);
        self.data.clear();
    }

    /// `BlockRefTableFileTerminate(buffer)`: write the all-zero sentinel entry,
    /// then the finalized CRC, then flush.
    fn terminate(&mut self) {
        let zentry = [0u8; SERIALIZED_ENTRY_LEN];
        self.write(&zentry);

        // Copy the CRC state before perturbing it, then finalize.
        let crc = fin_crc32c(self.crc);
        self.write(&crc.to_ne_bytes());

        self.flush();
    }
}

// ---------------------------------------------------------------------------
// Reader (`struct BlockRefTableReader` + `struct BlockRefTableBuffer` read side).
// ---------------------------------------------------------------------------

/// `io_callback_fn` on the read path: `fn(callback_arg, data, length) -> bytes_read`.
/// The boxed closure reads up to `data.len()` bytes into `data`, returning the
/// number actually read (0 = EOF), mirroring the C `read_callback`.
pub type ReadCallback = Box<dyn FnMut(&mut [u8]) -> usize>;

/// Read-side buffer (`struct BlockRefTableBuffer`).
struct ReadBuffer {
    io_callback: ReadCallback,
    data: Vec<u8>,
    used: usize,
    cursor: usize,
    crc: u32,
}

/// `struct BlockRefTableReader`.
pub struct BlockRefTableReader {
    buffer: ReadBuffer,
    error_filename: alloc::string::String,
    total_chunks: uint32,
    consumed_chunks: uint32,
    chunk_size: Vec<uint16>,
    chunk_data: Vec<uint16>,
    chunk_position: uint32,
}

impl ReadBuffer {
    /// `BlockRefTableRead(reader, data, length)`: read exactly `out.len()` bytes,
    /// updating the running CRC over the returned data.
    fn read(&mut self, out: &mut [u8], error_filename: &str) -> PgResult<()> {
        let mut written = 0usize;
        let total = out.len();

        // Loop until read is fully satisfied.
        while written < total {
            let mut length = total - written;
            if self.cursor < self.used {
                // Satisfy as much of the request as possible from the buffer.
                let bytes_to_copy = core::cmp::min(length, self.used - self.cursor);
                out[written..written + bytes_to_copy]
                    .copy_from_slice(&self.data[self.cursor..self.cursor + bytes_to_copy]);
                self.crc = comp_crc32c(
                    self.crc,
                    &self.data[self.cursor..self.cursor + bytes_to_copy],
                );
                self.cursor += bytes_to_copy;
                written += bytes_to_copy;
            } else if length >= BUFSIZE {
                // Long request: read directly into the caller's buffer.
                let dst = &mut out[written..written + length];
                let bytes_read = (self.io_callback)(dst);
                self.crc = comp_crc32c(self.crc, &dst[..bytes_read]);
                written += bytes_read;
                length -= bytes_read;
                let _ = length;

                // If we didn't get anything, that's bad.
                if bytes_read == 0 {
                    return Err(report_error(error_filename, "ends unexpectedly", &[]));
                }
            } else {
                // Refill our buffer.
                self.data.resize(BUFSIZE, 0);
                let n = (self.io_callback)(&mut self.data[..BUFSIZE]);
                self.used = n;
                self.cursor = 0;

                // If we didn't get anything, that's bad.
                if self.used == 0 {
                    return Err(report_error(error_filename, "ends unexpectedly", &[]));
                }
            }
        }
        Ok(())
    }
}

/// Build the `report_error_fn` `ereport(ERROR)` message. The C callback formats
/// `"file \"%s\" ..."`; we relay it through `PgError::error`.
fn report_error(filename: &str, detail: &str, extra: &[(&str, alloc::string::String)]) -> PgError {
    let mut msg = format!("file \"{filename}\" {detail}");
    for (k, v) in extra {
        msg.push_str(&format!(" {k}={v}"));
    }
    PgError::error(msg)
}

// ---------------------------------------------------------------------------
// Entry manipulation (no registry; pure C logic over BlockRefTableEntry).
// ---------------------------------------------------------------------------

/// `BlockRefTableEntrySetLimitBlock(entry, limit_block)`.
fn entry_set_limit_block(entry: &mut BlockRefTableEntry, limit_block: BlockNumber) {
    // If we already have an equal or lower limit block, do nothing.
    if limit_block >= entry.limit_block {
        return;
    }

    // Record the new limit block value.
    entry.limit_block = limit_block;

    // Which chunk would store the new limit block, and which offset.
    let limit_chunkno = limit_block / BLOCKS_PER_CHUNK;
    let limit_chunkoffset = limit_block % BLOCKS_PER_CHUNK;

    // If no equal-or-higher blocks can exist, nothing further to do.
    if limit_chunkno >= entry.nchunks {
        return;
    }
    let limit_chunkno = limit_chunkno as usize;

    // Discard entire contents of any higher-numbered chunks.
    for chunkno in (limit_chunkno + 1)..(entry.nchunks as usize) {
        entry.chunk_usage[chunkno] = 0;
    }

    // Discard offsets within the chunk that would contain limit_block.
    if entry.chunk_usage[limit_chunkno] as u32 == MAX_ENTRIES_PER_CHUNK {
        // It's a bitmap. Unset bits.
        let chunk = &mut entry.chunk_data[limit_chunkno];
        for chunkoffset in limit_chunkoffset..BLOCKS_PER_CHUNK {
            chunk[(chunkoffset / BLOCKS_PER_ENTRY) as usize] &=
                !(1u16 << (chunkoffset % BLOCKS_PER_ENTRY));
        }
    } else {
        // It's an offset array. Filter out large offsets.
        let usage = entry.chunk_usage[limit_chunkno] as usize;
        let chunk = &mut entry.chunk_data[limit_chunkno];
        let mut j = 0usize;
        for i in 0..usage {
            if (chunk[i] as u32) < limit_chunkoffset {
                chunk[j] = chunk[i];
                j += 1;
            }
        }
        debug_assert!(j <= usage);
        entry.chunk_usage[limit_chunkno] = j as uint16;
    }
}

/// `BlockRefTableEntryMarkBlockModified(entry, forknum, blknum)`.
fn entry_mark_block_modified(entry: &mut BlockRefTableEntry, blknum: BlockNumber) {
    // Which chunk and which offset within the chunk.
    let chunkno = (blknum / BLOCKS_PER_CHUNK) as usize;
    let chunkoffset = blknum % BLOCKS_PER_CHUNK;

    // Enlarge arrays if nchunks isn't big enough to represent this block.
    if chunkno >= entry.nchunks as usize {
        // New array size: power of 2, >= 16, big enough to index chunkno.
        let mut max_chunks = core::cmp::max(16u32, entry.nchunks);
        while (max_chunks as usize) < chunkno + 1 {
            max_chunks *= 2;
        }
        let max_chunks = max_chunks as usize;

        entry.chunk_size.resize(max_chunks, 0);
        entry.chunk_usage.resize(max_chunks, 0);
        entry.chunk_data.resize(max_chunks, Vec::new());
        entry.nchunks = max_chunks as uint32;
    }

    // If the chunk that covers this block doesn't exist yet, create it as a
    // small array with the single offset.
    if entry.chunk_size[chunkno] == 0 {
        let mut newchunk = vec![0u16; INITIAL_ENTRIES_PER_CHUNK as usize];
        entry.chunk_size[chunkno] = INITIAL_ENTRIES_PER_CHUNK as uint16;
        newchunk[0] = chunkoffset as uint16;
        entry.chunk_data[chunkno] = newchunk;
        entry.chunk_usage[chunkno] = 1;
        return;
    }

    // If usage is already maximum, it's a bitmap; just set the bit.
    if entry.chunk_usage[chunkno] as u32 == MAX_ENTRIES_PER_CHUNK {
        let chunk = &mut entry.chunk_data[chunkno];
        chunk[(chunkoffset / BLOCKS_PER_ENTRY) as usize] |=
            1u16 << (chunkoffset % BLOCKS_PER_ENTRY);
        return;
    }

    // Existing array chunk: does it already have an entry for this block?
    let usage = entry.chunk_usage[chunkno] as usize;
    for i in 0..usage {
        if entry.chunk_data[chunkno][i] as u32 == chunkoffset {
            return;
        }
    }

    // If usage is one less than the maximum, convert to bitmap format.
    if entry.chunk_usage[chunkno] as u32 == MAX_ENTRIES_PER_CHUNK - 1 {
        let mut newchunk = vec![0u16; MAX_ENTRIES_PER_CHUNK as usize];

        // Set the bit for each existing entry.
        for j in 0..usage {
            let coff = entry.chunk_data[chunkno][j] as u32;
            newchunk[(coff / BLOCKS_PER_ENTRY) as usize] |= 1u16 << (coff % BLOCKS_PER_ENTRY);
        }

        // Set the bit for the new entry.
        newchunk[(chunkoffset / BLOCKS_PER_ENTRY) as usize] |=
            1u16 << (chunkoffset % BLOCKS_PER_ENTRY);

        // Swap the new chunk into place and update metadata.
        entry.chunk_data[chunkno] = newchunk;
        entry.chunk_size[chunkno] = MAX_ENTRIES_PER_CHUNK as uint16;
        entry.chunk_usage[chunkno] = MAX_ENTRIES_PER_CHUNK as uint16;
        return;
    }

    // Array, no conversion needed, but add a new element; grow if full.
    if entry.chunk_usage[chunkno] == entry.chunk_size[chunkno] {
        let newsize = (entry.chunk_size[chunkno] as u32) * 2;
        debug_assert!(newsize <= MAX_ENTRIES_PER_CHUNK);
        entry.chunk_data[chunkno].resize(newsize as usize, 0);
        entry.chunk_size[chunkno] = newsize as uint16;
    }

    // Now add the new entry.
    let u = entry.chunk_usage[chunkno] as usize;
    entry.chunk_data[chunkno][u] = chunkoffset as uint16;
    entry.chunk_usage[chunkno] += 1;
}

/// `BlockRefTableEntryGetBlocks(entry, start_blkno, stop_blkno, blocks, nblocks)`.
fn entry_get_blocks(
    entry: &BlockRefTableEntry,
    start_blkno: BlockNumber,
    stop_blkno: BlockNumber,
    blocks: &mut Vec<BlockNumber>,
    nblocks: usize,
) -> usize {
    let mut nresults = 0usize;

    // Which chunks could potentially contain blocks of interest. Be careful
    // about overflow: stop_blkno could be InvalidBlockNumber.
    let start_chunkno = start_blkno / BLOCKS_PER_CHUNK;
    let mut stop_chunkno = stop_blkno / BLOCKS_PER_CHUNK;
    if (stop_blkno % BLOCKS_PER_CHUNK) != 0 {
        stop_chunkno += 1;
    }
    if stop_chunkno > entry.nchunks {
        stop_chunkno = entry.nchunks;
    }

    // Loop over chunks.
    let mut chunkno = start_chunkno;
    while chunkno < stop_chunkno {
        let chunk_usage = entry.chunk_usage[chunkno as usize];
        let chunk_data = &entry.chunk_data[chunkno as usize];
        let mut start_offset = 0u32;
        let mut stop_offset = BLOCKS_PER_CHUNK;

        if chunkno == start_chunkno {
            start_offset = start_blkno % BLOCKS_PER_CHUNK;
        }
        if chunkno == stop_chunkno - 1 {
            debug_assert!(stop_blkno > chunkno * BLOCKS_PER_CHUNK);
            stop_offset = stop_blkno - (chunkno * BLOCKS_PER_CHUNK);
            debug_assert!(stop_offset <= BLOCKS_PER_CHUNK);
        }

        if chunk_usage as u32 == MAX_ENTRIES_PER_CHUNK {
            // Bitmap: test every relevant bit.
            for i in start_offset..stop_offset {
                let w = chunk_data[(i / BLOCKS_PER_ENTRY) as usize];
                if (w & (1u16 << (i % BLOCKS_PER_ENTRY))) != 0 {
                    let blkno = chunkno * BLOCKS_PER_CHUNK + i;
                    blocks.push(blkno);
                    nresults += 1;
                    if nresults == nblocks {
                        return nresults;
                    }
                }
            }
        } else {
            // Array of offsets: check each one.
            for i in 0..(chunk_usage as usize) {
                let offset = chunk_data[i] as u32;
                if offset >= start_offset && offset < stop_offset {
                    let blkno = chunkno * BLOCKS_PER_CHUNK + offset;
                    blocks.push(blkno);
                    nresults += 1;
                    if nresults == nblocks {
                        return nresults;
                    }
                }
            }
        }

        chunkno += 1;
    }

    nresults
}

/// Serialize one entry into a [`WriteBuffer`] (shared by `WriteBlockRefTable`
/// and `BlockRefTableWriteEntry`).
fn write_entry(buffer: &mut WriteBuffer, entry: &BlockRefTableEntry) {
    let mut sentry = BlockRefTableSerializedEntry {
        rlocator: entry.key.rlocator,
        forknum: entry.key.forknum,
        limit_block: entry.limit_block,
        nchunks: entry.nchunks,
    };

    // Trim trailing zero entries.
    while sentry.nchunks > 0 && entry.chunk_usage[(sentry.nchunks - 1) as usize] == 0 {
        sentry.nchunks -= 1;
    }

    // Write the serialized entry itself.
    buffer.write(&sentry.to_bytes());

    // Write the untruncated portion of the chunk length array (chunk_usage).
    if sentry.nchunks != 0 {
        let mut usage_bytes = Vec::with_capacity(sentry.nchunks as usize * 2);
        for j in 0..(sentry.nchunks as usize) {
            usage_bytes.extend_from_slice(&entry.chunk_usage[j].to_ne_bytes());
        }
        buffer.write(&usage_bytes);
    }

    // Write the contents of each chunk.
    for j in 0..(entry.nchunks as usize) {
        if entry.chunk_usage[j] == 0 {
            continue;
        }
        let used = entry.chunk_usage[j] as usize;
        let mut chunk_bytes = Vec::with_capacity(used * 2);
        for k in 0..used {
            chunk_bytes.extend_from_slice(&entry.chunk_data[j][k].to_ne_bytes());
        }
        buffer.write(&chunk_bytes);
    }
}

// ---------------------------------------------------------------------------
// Registries (process-global, mirroring the C palloc'd opaque pointers).
// ---------------------------------------------------------------------------

thread_local! {
    /// Live in-memory `BlockRefTable`s keyed by handle id.
    static TABLES: RefCell<HashMap<u64, BlockRefTable>> = RefCell::new(HashMap::new());
    /// Live `BlockRefTableReader`s keyed by handle id.
    static READERS: RefCell<HashMap<u64, BlockRefTableReader>> = RefCell::new(HashMap::new());
    /// Monotonic id allocator (shared so table and reader ids never collide,
    /// keeping handles globally distinguishable for debugging).
    static NEXT_ID: RefCell<u64> = RefCell::new(1);
}

fn alloc_id() -> u64 {
    NEXT_ID.with(|n| {
        let mut n = n.borrow_mut();
        let id = *n;
        *n += 1;
        id
    })
}

// ---------------------------------------------------------------------------
// Public API + seam bodies — in-memory table side.
// ---------------------------------------------------------------------------

/// `CreateEmptyBlockRefTable()` (seam `create_empty_block_ref_table`).
fn create_empty_block_ref_table(_mcx: Mcx<'_>) -> PgResult<BlockRefTableHandle> {
    let id = alloc_id();
    TABLES.with(|t| t.borrow_mut().insert(id, BlockRefTable::new()));
    Ok(BlockRefTableHandle(id))
}

/// `BlockRefTableSetLimitBlock(brtab, rlocator, forknum, limit_block)`.
fn block_ref_table_set_limit_block(
    brtab: BlockRefTableHandle,
    rlocator: RelFileLocator,
    forknum: ForkNumber,
    limit_block: BlockNumber,
) -> PgResult<()> {
    let key = BlockRefTableKey { rlocator, forknum };
    let raw = key.raw_bytes();
    TABLES.with(|t| {
        let mut tables = t.borrow_mut();
        let table = tables
            .get_mut(&brtab.0)
            .expect("BlockRefTableSetLimitBlock: unknown BlockRefTableHandle");
        match table.hash.get_mut(&raw) {
            None => {
                // !found: record the limit block in a fresh entry.
                table
                    .hash
                    .insert(raw, BlockRefTableEntry::empty(key, limit_block));
            }
            Some(entry) => {
                entry_set_limit_block(entry, limit_block);
            }
        }
    });
    Ok(())
}

/// `BlockRefTableMarkBlockModified(brtab, rlocator, forknum, blknum)`.
fn block_ref_table_mark_block_modified(
    brtab: BlockRefTableHandle,
    rlocator: RelFileLocator,
    forknum: ForkNumber,
    blknum: BlockNumber,
) -> PgResult<()> {
    let key = BlockRefTableKey { rlocator, forknum };
    let raw = key.raw_bytes();
    TABLES.with(|t| {
        let mut tables = t.borrow_mut();
        let table = tables
            .get_mut(&brtab.0)
            .expect("BlockRefTableMarkBlockModified: unknown BlockRefTableHandle");
        let entry = table
            .hash
            .entry(raw)
            // !found: initialize limit_block to InvalidBlockNumber (higher than
            // any legal block number).
            .or_insert_with(|| BlockRefTableEntry::empty(key, InvalidBlockNumber));
        entry_mark_block_modified(entry, blknum);
    });
    Ok(())
}

/// `BlockRefTableGetEntry(brtab, rlocator, forknum, &limit_block)`
/// (seam `block_ref_table_get_entry`): look up the entry; return its
/// `limit_block` if present, else `None`.
fn block_ref_table_get_entry(
    brtab: BlockRefTableHandle,
    rlocator: RelFileLocator,
    forknum: ForkNumber,
) -> Option<BlockNumber> {
    let key = BlockRefTableKey { rlocator, forknum };
    let raw = key.raw_bytes();
    TABLES.with(|t| {
        let tables = t.borrow();
        let table = tables
            .get(&brtab.0)
            .expect("BlockRefTableGetEntry: unknown BlockRefTableHandle");
        table.hash.get(&raw).map(|entry| entry.limit_block)
    })
}

/// `BlockRefTableGetEntry(...)` + `BlockRefTableEntryGetBlocks(...)`
/// (seam `block_ref_table_get_entry_blocks`): look up the entry and, if it
/// exists, extract the modified block numbers in `[start_blkno, stop_blkno)`
/// (at most `nblocks`).
fn block_ref_table_get_entry_blocks<'mcx>(
    mcx: Mcx<'mcx>,
    brtab: BlockRefTableHandle,
    rlocator: RelFileLocator,
    forknum: ForkNumber,
    start_blkno: BlockNumber,
    stop_blkno: BlockNumber,
    nblocks: usize,
) -> PgResult<Option<(BlockNumber, PgVec<'mcx, BlockNumber>)>> {
    let key = BlockRefTableKey { rlocator, forknum };
    let raw = key.raw_bytes();
    let result = TABLES.with(|t| {
        let tables = t.borrow();
        let table = tables
            .get(&brtab.0)
            .expect("BlockRefTableGetEntryBlocks: unknown BlockRefTableHandle");
        table.hash.get(&raw).map(|entry| {
            let limit_block = entry.limit_block;
            let mut blocks: Vec<BlockNumber> = Vec::new();
            entry_get_blocks(entry, start_blkno, stop_blkno, &mut blocks, nblocks);
            (limit_block, blocks)
        })
    });
    match result {
        None => Ok(None),
        Some((limit_block, blocks)) => {
            let mut out = mcx::vec_with_capacity_in(mcx, blocks.len())?;
            out.extend_from_slice(&blocks);
            Ok(Some((limit_block, out)))
        }
    }
}

/// `WriteBlockRefTable(brtab, write_callback, write_callback_arg)`
/// (seam `write_block_ref_table`): serialize the whole table and return the
/// bytes (the backend would stream them through its write callback).
fn write_block_ref_table<'mcx>(
    mcx: Mcx<'mcx>,
    brtab: BlockRefTableHandle,
) -> PgResult<PgVec<'mcx, u8>> {
    let mut buffer = WriteBuffer::new();

    // Write magic number.
    buffer.write(&BLOCKREFTABLE_MAGIC.to_ne_bytes());

    TABLES.with(|t| {
        let tables = t.borrow();
        let table = tables
            .get(&brtab.0)
            .expect("WriteBlockRefTable: unknown BlockRefTableHandle");

        if !table.hash.is_empty() {
            // Extract entries into serializable form and sort them.
            let mut sdata: Vec<&BlockRefTableEntry> = table.hash.values().collect();
            sdata.sort_by_key(|e| {
                let sentry = BlockRefTableSerializedEntry {
                    rlocator: e.key.rlocator,
                    forknum: e.key.forknum,
                    limit_block: e.limit_block,
                    nchunks: e.nchunks,
                };
                sentry.cmp_key()
            });

            // Loop over entries in sorted order and serialize each one.
            for entry in sdata {
                write_entry(&mut buffer, entry);
            }
        }
    });

    // Write out terminator and CRC and flush buffer.
    buffer.terminate();

    // Hand the serialized bytes back, allocated in mcx.
    let mut out = mcx::vec_with_capacity_in(mcx, buffer.out.len())?;
    out.extend_from_slice(&buffer.out);
    Ok(out)
}

// ---------------------------------------------------------------------------
// Public API — reader side construction (driven by the walsummary owner).
// ---------------------------------------------------------------------------

/// `CreateBlockRefTableReader(read_callback, read_callback_arg, error_filename,
/// error_callback, error_callback_arg)` (blkreftable.c).
///
/// The C `io_callback_fn`/`report_error_fn` are owned by the walsummary unit
/// (`ReadWalSummary` / `ReportWalSummaryError`); that owner calls this with a
/// boxed `read_callback`. The error callback is folded into the returned
/// [`PgError`] (`error_filename` is captured for the messages). Registers the
/// reader and returns its handle.
///
/// Returns `Err` if the magic number is wrong, matching the C
/// `error_callback(...)` (which `ereport(ERROR)`s).
pub fn create_block_ref_table_reader(
    read_callback: ReadCallback,
    error_filename: alloc::string::String,
) -> PgResult<BlockRefTableReaderHandle> {
    let mut reader = BlockRefTableReader {
        buffer: ReadBuffer {
            io_callback: read_callback,
            data: Vec::new(),
            used: 0,
            cursor: 0,
            crc: init_crc32c(),
        },
        error_filename,
        total_chunks: 0,
        consumed_chunks: 0,
        chunk_size: Vec::new(),
        chunk_data: vec![0u16; MAX_ENTRIES_PER_CHUNK as usize],
        chunk_position: 0,
    };

    // Verify magic number.
    let mut magic_bytes = [0u8; 4];
    let fname = reader.error_filename.clone();
    reader.buffer.read(&mut magic_bytes, &fname)?;
    let magic = u32::from_ne_bytes(magic_bytes);
    if magic != BLOCKREFTABLE_MAGIC {
        return Err(report_error(
            &fname,
            "has wrong magic number",
            &[
                ("expected", format!("{BLOCKREFTABLE_MAGIC}")),
                ("found", format!("{magic}")),
            ],
        ));
    }

    let id = alloc_id();
    READERS.with(|r| r.borrow_mut().insert(id, reader));
    Ok(BlockRefTableReaderHandle(id))
}

// ---------------------------------------------------------------------------
// Seam bodies — reader side iteration (driven by walsummaryfuncs).
// ---------------------------------------------------------------------------

/// `BlockRefTableReaderNextRelation(reader, &rlocator, &forknum, &limit_block)`.
fn block_ref_table_reader_next_relation(
    reader: BlockRefTableReaderHandle,
) -> PgResult<Option<(RelFileLocator, ForkNumber, BlockNumber)>> {
    READERS.with(|r| {
        let mut readers = r.borrow_mut();
        let reader = readers
            .get_mut(&reader.0)
            .expect("BlockRefTableReaderNextRelation: unknown BlockRefTableReaderHandle");
        let fname = reader.error_filename.clone();

        // Sanity check: all chunks must have been consumed.
        debug_assert!(reader.total_chunks == reader.consumed_chunks);

        // Read serialized entry.
        let mut sbytes = [0u8; SERIALIZED_ENTRY_LEN];
        reader.buffer.read(&mut sbytes, &fname)?;

        // If we read the all-zero sentinel, read and check the CRC.
        let zentry = [0u8; SERIALIZED_ENTRY_LEN];
        if sbytes == zentry {
            // CRC of the file excluding the 4-byte CRC: snapshot the accumulator
            // before reading those bytes, finalize the copy.
            let expected_crc = fin_crc32c(reader.buffer.crc);

            let mut actual_bytes = [0u8; 4];
            reader.buffer.read(&mut actual_bytes, &fname)?;
            let actual_crc = u32::from_ne_bytes(actual_bytes);

            if expected_crc != actual_crc {
                return Err(report_error(
                    &fname,
                    "has wrong checksum",
                    &[
                        ("expected", format!("{expected_crc:08X}")),
                        ("found", format!("{actual_crc:08X}")),
                    ],
                ));
            }

            return Ok(None);
        }

        let sentry = BlockRefTableSerializedEntry::from_bytes(&sbytes);

        // Read chunk size array.
        reader.chunk_size = vec![0u16; sentry.nchunks as usize];
        let mut size_bytes = vec![0u8; sentry.nchunks as usize * 2];
        reader.buffer.read(&mut size_bytes, &fname)?;
        for j in 0..(sentry.nchunks as usize) {
            reader.chunk_size[j] =
                u16::from_ne_bytes(size_bytes[j * 2..j * 2 + 2].try_into().unwrap());
        }

        // Set up for chunk scan.
        reader.total_chunks = sentry.nchunks;
        reader.consumed_chunks = 0;

        Ok(Some((sentry.rlocator, sentry.forknum, sentry.limit_block)))
    })
}

/// `BlockRefTableReaderGetBlocks(reader, blocks, nblocks)` — fetch up to
/// `nblocks` modified block numbers of the current relation fork. The seam
/// returns them as a vector; empty means the current fork is exhausted.
fn block_ref_table_reader_get_blocks<'mcx>(
    mcx: Mcx<'mcx>,
    reader: BlockRefTableReaderHandle,
    nblocks: usize,
) -> PgResult<PgVec<'mcx, BlockNumber>> {
    // Must provide space for at least one block number to be returned.
    debug_assert!(nblocks > 0);

    let result = READERS.with(|r| -> PgResult<Vec<BlockNumber>> {
        let mut readers = r.borrow_mut();
        let reader = readers
            .get_mut(&reader.0)
            .expect("BlockRefTableReaderGetBlocks: unknown BlockRefTableReaderHandle");
        let fname = reader.error_filename.clone();

        let mut blocks: Vec<BlockNumber> = Vec::new();
        let mut blocks_found = 0usize;

        // Loop collecting blocks to return to caller.
        loop {
            // If we've read at least one chunk, maybe it has blocks of interest.
            if reader.consumed_chunks > 0 {
                let chunkno = reader.consumed_chunks - 1;
                let chunk_size = reader.chunk_size[chunkno as usize];

                if chunk_size as u32 == MAX_ENTRIES_PER_CHUNK {
                    // Bitmap format: search for set bits.
                    while reader.chunk_position < BLOCKS_PER_CHUNK && blocks_found < nblocks {
                        let chunkoffset = reader.chunk_position;
                        let w = reader.chunk_data[(chunkoffset / BLOCKS_PER_ENTRY) as usize];
                        if (w & (1u16 << (chunkoffset % BLOCKS_PER_ENTRY))) != 0 {
                            blocks.push(chunkno * BLOCKS_PER_CHUNK + chunkoffset);
                            blocks_found += 1;
                        }
                        reader.chunk_position += 1;
                    }
                } else {
                    // Array format: each entry is a 2-byte offset.
                    while reader.chunk_position < chunk_size as u32 && blocks_found < nblocks {
                        blocks.push(
                            chunkno * BLOCKS_PER_CHUNK
                                + reader.chunk_data[reader.chunk_position as usize] as u32,
                        );
                        blocks_found += 1;
                        reader.chunk_position += 1;
                    }
                }
            }

            // We found enough blocks, so we're done.
            if blocks_found >= nblocks {
                break;
            }

            // Need the next chunk; if there are none left, we're done.
            if reader.consumed_chunks == reader.total_chunks {
                break;
            }

            // Read data for next chunk and reset scan position. The next chunk
            // might be empty, consuming no bytes from the underlying file.
            let next_chunk_size = reader.chunk_size[reader.consumed_chunks as usize];
            if next_chunk_size > 0 {
                let mut chunk_bytes = vec![0u8; next_chunk_size as usize * 2];
                reader.buffer.read(&mut chunk_bytes, &fname)?;
                for j in 0..(next_chunk_size as usize) {
                    reader.chunk_data[j] =
                        u16::from_ne_bytes(chunk_bytes[j * 2..j * 2 + 2].try_into().unwrap());
                }
            }
            reader.consumed_chunks += 1;
            reader.chunk_position = 0;
        }

        Ok(blocks)
    })?;

    let mut out = mcx::vec_with_capacity_in(mcx, result.len())?;
    out.extend_from_slice(&result);
    Ok(out)
}

/// `DestroyBlockRefTableReader(reader)`.
fn destroy_block_ref_table_reader(reader: BlockRefTableReaderHandle) {
    READERS.with(|r| {
        r.borrow_mut().remove(&reader.0);
    });
}

// ---------------------------------------------------------------------------
// Standalone-entry API (CreateBlockRefTableEntry/.../WriteEntry/FreeEntry +
// CreateBlockRefTableWriter/...): the incremental writer over a callback.
// ---------------------------------------------------------------------------

/// `CreateBlockRefTableEntry(rlocator, forknum)` — a standalone entry, not part
/// of any in-memory table. Used by callers that stream a table to disk without
/// holding the whole thing in memory.
pub fn create_block_ref_table_entry(
    rlocator: RelFileLocator,
    forknum: ForkNumber,
) -> BlockRefTableEntry {
    BlockRefTableEntry::empty(BlockRefTableKey { rlocator, forknum }, InvalidBlockNumber)
}

/// `BlockRefTableEntrySetLimitBlock(entry, limit_block)`.
pub fn block_ref_table_entry_set_limit_block(
    entry: &mut BlockRefTableEntry,
    limit_block: BlockNumber,
) {
    entry_set_limit_block(entry, limit_block);
}

/// `BlockRefTableEntryMarkBlockModified(entry, forknum, blknum)`.
pub fn block_ref_table_entry_mark_block_modified(
    entry: &mut BlockRefTableEntry,
    blknum: BlockNumber,
) {
    entry_mark_block_modified(entry, blknum);
}

/// `BlockRefTableEntryGetBlocks(entry, start_blkno, stop_blkno, blocks, nblocks)`.
pub fn block_ref_table_entry_get_blocks(
    entry: &BlockRefTableEntry,
    start_blkno: BlockNumber,
    stop_blkno: BlockNumber,
    blocks: &mut Vec<BlockNumber>,
    nblocks: usize,
) -> usize {
    entry_get_blocks(entry, start_blkno, stop_blkno, blocks, nblocks)
}

/// The incremental on-disk writer (`struct BlockRefTableWriter`). The C version
/// flushes through a write callback; we accumulate the bytes and the caller
/// retrieves them via [`BlockRefTableWriter::finish`].
pub struct BlockRefTableWriter {
    buffer: WriteBuffer,
}

impl BlockRefTableWriter {
    /// `CreateBlockRefTableWriter(write_callback, write_callback_arg)`: prepare
    /// the buffer + CRC and write the magic number.
    pub fn new() -> Self {
        let mut buffer = WriteBuffer::new();
        buffer.write(&BLOCKREFTABLE_MAGIC.to_ne_bytes());
        BlockRefTableWriter { buffer }
    }

    /// `BlockRefTableWriteEntry(writer, entry)`: append one entry. Entries must
    /// be supplied in sorted order (caller's responsibility, as in C).
    pub fn write_entry(&mut self, entry: &BlockRefTableEntry) {
        write_entry(&mut self.buffer, entry);
    }

    /// `DestroyBlockRefTableWriter(writer)`: terminate (sentinel + CRC + flush)
    /// and return the serialized bytes.
    pub fn finish(mut self) -> Vec<u8> {
        self.buffer.terminate();
        self.buffer.out
    }
}

impl Default for BlockRefTableWriter {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Seam installation.
// ---------------------------------------------------------------------------

/// Install this crate's seams. Called once at startup by `seams-init`.
pub fn init_seams() {
    common_blkreftable_seams::create_empty_block_ref_table::set(create_empty_block_ref_table);
    common_blkreftable_seams::block_ref_table_set_limit_block::set(block_ref_table_set_limit_block);
    common_blkreftable_seams::block_ref_table_mark_block_modified::set(
        block_ref_table_mark_block_modified,
    );
    common_blkreftable_seams::block_ref_table_get_entry::set(block_ref_table_get_entry);
    common_blkreftable_seams::block_ref_table_get_entry_blocks::set(
        block_ref_table_get_entry_blocks,
    );
    common_blkreftable_seams::write_block_ref_table::set(write_block_ref_table);
    common_blkreftable_seams::block_ref_table_reader_next_relation::set(
        block_ref_table_reader_next_relation,
    );
    common_blkreftable_seams::block_ref_table_reader_get_blocks::set(
        block_ref_table_reader_get_blocks,
    );
    common_blkreftable_seams::destroy_block_ref_table_reader::set(destroy_block_ref_table_reader);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::rc::Rc;

    // Install the CRC seam (port-crc32c owner) for the roundtrip test.
    fn ensure_crc() {
        // pg_comp_crc32c is installed by port-crc32c::init_seams; call it once.
        // is_installed avoids double-install panic across tests.
        if !port_pg_crc32c_seams::pg_comp_crc32c::is_installed() {
            port_crc32c::init_seams();
        }
    }

    fn rl(spc: u32, db: u32, rel: u32) -> RelFileLocator {
        RelFileLocator { spcOid: spc, dbOid: db, relNumber: rel }
    }

    /// Build an entry, mark a spread of blocks (forcing both array and bitmap
    /// chunks), serialize via the writer, read back via the reader, and assert
    /// the exact set of blocks survives (chunk encoding + CRC roundtrip).
    #[test]
    fn writer_reader_roundtrip() {
        ensure_crc();

        let loc = rl(1663, 5, 16384);
        let mut entry = create_block_ref_table_entry(loc, ForkNumber::MAIN_FORKNUM);

        // Sparse blocks in chunk 0 (stays an array) + dense blocks in chunk 1
        // (forces the array->bitmap conversion: > MAX_ENTRIES_PER_CHUNK
        // distinct offsets) + a block far out (chunk 2, sparse array).
        let mut expected: Vec<BlockNumber> = Vec::new();
        for b in [0u32, 5, 100, 65535] {
            block_ref_table_entry_mark_block_modified(&mut entry, b);
            expected.push(b);
        }
        // Force chunk 1 to bitmap: mark MAX_ENTRIES_PER_CHUNK distinct offsets.
        for off in 0..MAX_ENTRIES_PER_CHUNK {
            let b = BLOCKS_PER_CHUNK + off;
            block_ref_table_entry_mark_block_modified(&mut entry, b);
            expected.push(b);
        }
        // chunk 2 sparse.
        for b in [2 * BLOCKS_PER_CHUNK + 7, 2 * BLOCKS_PER_CHUNK + 4095] {
            block_ref_table_entry_mark_block_modified(&mut entry, b);
            expected.push(b);
        }
        expected.sort_unstable();

        // Serialize with the incremental writer.
        let mut writer = BlockRefTableWriter::new();
        writer.write_entry(&entry);
        let bytes = writer.finish();

        // Read it back.
        let cursor = Rc::new(RefCell::new(0usize));
        let data = Rc::new(bytes);
        let cur2 = cursor.clone();
        let data2 = data.clone();
        let cb: ReadCallback = Box::new(move |out: &mut [u8]| {
            let mut pos = cur2.borrow_mut();
            let avail = data2.len() - *pos;
            let n = core::cmp::min(avail, out.len());
            out[..n].copy_from_slice(&data2[*pos..*pos + n]);
            *pos += n;
            n
        });

        let h = create_block_ref_table_reader(cb, "test".into()).expect("reader create");

        let next = block_ref_table_reader_next_relation(h).expect("next");
        let (got_rl, got_fork, _limit) = next.expect("one relation");
        assert_eq!(got_rl, loc);
        assert_eq!(got_fork, ForkNumber::MAIN_FORKNUM);

        // Drain blocks in small batches to exercise chunk_position resumption.
        let mcx_ctx = mcx::MemoryContext::new("t");
        let mut got: Vec<BlockNumber> = Vec::new();
        loop {
            let batch = block_ref_table_reader_get_blocks(mcx_ctx.mcx(), h, 3).expect("get");
            if batch.is_empty() {
                break;
            }
            got.extend_from_slice(&batch);
        }
        got.sort_unstable();
        assert_eq!(got, expected);

        // End-of-table sentinel + CRC must validate.
        let end = block_ref_table_reader_next_relation(h).expect("crc ok");
        assert!(end.is_none());

        destroy_block_ref_table_reader(h);
    }

    /// A corrupted CRC must be detected (silent-corruption guard).
    #[test]
    fn bad_crc_detected() {
        ensure_crc();
        let mut writer = BlockRefTableWriter::new();
        let entry = create_block_ref_table_entry(rl(1, 2, 3), ForkNumber::MAIN_FORKNUM);
        writer.write_entry(&entry);
        let mut bytes = writer.finish();
        // Flip a bit in the final CRC byte.
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;

        let cursor = Rc::new(RefCell::new(0usize));
        let data = Rc::new(bytes);
        let cur2 = cursor.clone();
        let data2 = data.clone();
        let cb: ReadCallback = Box::new(move |out: &mut [u8]| {
            let mut pos = cur2.borrow_mut();
            let avail = data2.len() - *pos;
            let n = core::cmp::min(avail, out.len());
            out[..n].copy_from_slice(&data2[*pos..*pos + n]);
            *pos += n;
            n
        });
        let h = create_block_ref_table_reader(cb, "bad".into()).expect("reader");
        // First next returns the entry (limit_block default), then end checks CRC.
        let _ = block_ref_table_reader_next_relation(h).expect("entry");
        let err = block_ref_table_reader_next_relation(h);
        assert!(err.is_err(), "corrupted CRC must be rejected");
        destroy_block_ref_table_reader(h);
    }

    /// SetLimitBlock must forget equal-or-higher blocks.
    #[test]
    fn set_limit_block_forgets_higher() {
        let loc = rl(1, 1, 1);
        let mut entry = create_block_ref_table_entry(loc, ForkNumber::MAIN_FORKNUM);
        for b in [10u32, 20, 30, 40] {
            block_ref_table_entry_mark_block_modified(&mut entry, b);
        }
        block_ref_table_entry_set_limit_block(&mut entry, 25);
        let mut blocks = Vec::new();
        let n = block_ref_table_entry_get_blocks(&entry, 0, 1000, &mut blocks, 100);
        blocks.sort_unstable();
        assert_eq!(n, 2);
        assert_eq!(blocks, vec![10, 20]);
    }
}
