//! Block reference tables: modified-block tracking per relation fork over an
//! LSN range, with the incremental-backup on-disk serialization format.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

// On-disk format is native-endian in C; hand-pinned test vectors assume LE.
#[cfg(target_endian = "big")]
compile_error!("only the little-endian blkreftable layout is implemented");

use crc32c::{fin_crc32c, pg_comp_crc32c, CRC32C_INIT};
use mcx::{vec_append_bytes, vec_new_in, Mcx, PgFxHashMap, PgVec};
use types_core::{BlockNumber, ForkNumber, InvalidBlockNumber};
use types_error::{PgError, PgResult};
use types_storage::RelFileLocator;

const BLOCKS_PER_CHUNK: u32 = 1 << 16;
const BLOCKS_PER_ENTRY: u32 = 16;
const MAX_ENTRIES_PER_CHUNK: u32 = BLOCKS_PER_CHUNK / BLOCKS_PER_ENTRY;
const INITIAL_ENTRIES_PER_CHUNK: u32 = 16;
const BUFSIZE: usize = 65536;

pub const BLOCKREFTABLE_MAGIC: u32 = 0x652b137b;

// RelFileLocator (3 x u32) + ForkNumber (i32) + BlockNumber + nchunks, no padding.
const SERIALIZED_ENTRY_LEN: usize = 24;

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlockRefTableKey {
    pub rlocator: RelFileLocator,
    pub forknum: ForkNumber,
}

pub struct BlockRefTableEntry<'mcx> {
    key: BlockRefTableKey,
    limit_block: BlockNumber,
    // chunk_usage[i] == MAX_ENTRIES_PER_CHUNK marks a bitmap chunk; less marks
    // an offset array. chunk_data[i].len() is C's chunk_size (0 = absent).
    chunk_usage: PgVec<'mcx, u16>,
    chunk_data: PgVec<'mcx, PgVec<'mcx, u16>>,
}

pub struct BlockRefTable<'mcx> {
    mcx: Mcx<'mcx>,
    hash: PgFxHashMap<'mcx, BlockRefTableKey, BlockRefTableEntry<'mcx>>,
}

struct SerializedEntry {
    rlocator: RelFileLocator,
    forknum: i32,
    limit_block: BlockNumber,
    nchunks: u32,
}

impl SerializedEntry {
    fn to_bytes(&self) -> [u8; SERIALIZED_ENTRY_LEN] {
        let mut b = [0u8; SERIALIZED_ENTRY_LEN];
        b[0..4].copy_from_slice(&self.rlocator.spcOid.to_ne_bytes());
        b[4..8].copy_from_slice(&self.rlocator.dbOid.to_ne_bytes());
        b[8..12].copy_from_slice(&self.rlocator.relNumber.to_ne_bytes());
        b[12..16].copy_from_slice(&self.forknum.to_ne_bytes());
        b[16..20].copy_from_slice(&self.limit_block.to_ne_bytes());
        b[20..24].copy_from_slice(&self.nchunks.to_ne_bytes());
        b
    }

    fn from_bytes(b: &[u8; SERIALIZED_ENTRY_LEN]) -> Self {
        let word = |off: usize| u32::from_ne_bytes(b[off..off + 4].try_into().unwrap());
        SerializedEntry {
            rlocator: RelFileLocator::new(word(0), word(4), word(8)),
            forknum: word(12) as i32,
            limit_block: word(16),
            nchunks: word(20),
        }
    }
}

fn u16s_as_bytes(v: &[u16]) -> &[u8] {
    // SAFETY: u16 has no padding or invalid values; length covers exactly v.
    unsafe { core::slice::from_raw_parts(v.as_ptr().cast::<u8>(), v.len() * 2) }
}

fn u16s_as_bytes_mut(v: &mut [u16]) -> &mut [u8] {
    // SAFETY: as above, and any byte pattern is a valid u16.
    unsafe { core::slice::from_raw_parts_mut(v.as_mut_ptr().cast::<u8>(), v.len() * 2) }
}

impl<'mcx> BlockRefTableEntry<'mcx> {
    pub fn new(mcx: Mcx<'mcx>, rlocator: RelFileLocator, forknum: ForkNumber) -> Self {
        BlockRefTableEntry {
            key: BlockRefTableKey { rlocator, forknum },
            limit_block: InvalidBlockNumber,
            chunk_usage: vec_new_in(mcx),
            chunk_data: PgVec::new_in(mcx),
        }
    }

    pub fn key(&self) -> &BlockRefTableKey {
        &self.key
    }

    pub fn limit_block(&self) -> BlockNumber {
        self.limit_block
    }

    fn nchunks(&self) -> u32 {
        self.chunk_usage.len() as u32
    }

    pub fn set_limit_block(&mut self, limit_block: BlockNumber) {
        if limit_block >= self.limit_block {
            return;
        }
        self.limit_block = limit_block;

        let limit_chunkno = limit_block / BLOCKS_PER_CHUNK;
        let limit_chunkoffset = limit_block % BLOCKS_PER_CHUNK;
        if limit_chunkno >= self.nchunks() {
            return;
        }
        let limit_chunkno = limit_chunkno as usize;

        for chunkno in (limit_chunkno + 1)..self.chunk_usage.len() {
            self.chunk_usage[chunkno] = 0;
        }

        let usage = self.chunk_usage[limit_chunkno] as u32;
        let chunk = &mut self.chunk_data[limit_chunkno];
        if usage == MAX_ENTRIES_PER_CHUNK {
            for chunkoffset in limit_chunkoffset..BLOCKS_PER_CHUNK {
                chunk[(chunkoffset / BLOCKS_PER_ENTRY) as usize] &=
                    !(1u16 << (chunkoffset % BLOCKS_PER_ENTRY));
            }
        } else {
            let mut j = 0usize;
            for i in 0..usage as usize {
                if (chunk[i] as u32) < limit_chunkoffset {
                    chunk[j] = chunk[i];
                    j += 1;
                }
            }
            self.chunk_usage[limit_chunkno] = j as u16;
        }
    }

    pub fn mark_block_modified(&mut self, mcx: Mcx<'mcx>, blknum: BlockNumber) -> PgResult<()> {
        let chunkno = (blknum / BLOCKS_PER_CHUNK) as usize;
        let chunkoffset = blknum % BLOCKS_PER_CHUNK;

        if chunkno >= self.chunk_usage.len() {
            let mut max_chunks = core::cmp::max(16, self.chunk_usage.len());
            while max_chunks < chunkno + 1 {
                max_chunks *= 2;
            }
            let grow = max_chunks - self.chunk_usage.len();
            self.chunk_usage
                .try_reserve(grow)
                .map_err(|_| mcx.oom(grow * 2))?;
            self.chunk_usage.resize(max_chunks, 0);
            self.chunk_data
                .try_reserve(grow)
                .map_err(|_| mcx.oom(grow * 8))?;
            self.chunk_data.resize_with(max_chunks, || PgVec::new_in(mcx));
        }

        if self.chunk_data[chunkno].is_empty() {
            let chunk = &mut self.chunk_data[chunkno];
            chunk
                .try_reserve(INITIAL_ENTRIES_PER_CHUNK as usize)
                .map_err(|_| mcx.oom(INITIAL_ENTRIES_PER_CHUNK as usize * 2))?;
            chunk.resize(INITIAL_ENTRIES_PER_CHUNK as usize, 0);
            chunk[0] = chunkoffset as u16;
            self.chunk_usage[chunkno] = 1;
            return Ok(());
        }

        let usage = self.chunk_usage[chunkno] as u32;
        if usage == MAX_ENTRIES_PER_CHUNK {
            self.chunk_data[chunkno][(chunkoffset / BLOCKS_PER_ENTRY) as usize] |=
                1u16 << (chunkoffset % BLOCKS_PER_ENTRY);
            return Ok(());
        }

        for i in 0..usage as usize {
            if self.chunk_data[chunkno][i] as u32 == chunkoffset {
                return Ok(());
            }
        }

        if usage == MAX_ENTRIES_PER_CHUNK - 1 {
            let mut newchunk: PgVec<'mcx, u16> = vec_new_in(mcx);
            newchunk
                .try_reserve(MAX_ENTRIES_PER_CHUNK as usize)
                .map_err(|_| mcx.oom(MAX_ENTRIES_PER_CHUNK as usize * 2))?;
            newchunk.resize(MAX_ENTRIES_PER_CHUNK as usize, 0);
            for j in 0..usage as usize {
                let coff = self.chunk_data[chunkno][j] as u32;
                newchunk[(coff / BLOCKS_PER_ENTRY) as usize] |= 1u16 << (coff % BLOCKS_PER_ENTRY);
            }
            newchunk[(chunkoffset / BLOCKS_PER_ENTRY) as usize] |=
                1u16 << (chunkoffset % BLOCKS_PER_ENTRY);
            self.chunk_data[chunkno] = newchunk;
            self.chunk_usage[chunkno] = MAX_ENTRIES_PER_CHUNK as u16;
            return Ok(());
        }

        if usage as usize == self.chunk_data[chunkno].len() {
            let newsize = self.chunk_data[chunkno].len() * 2;
            debug_assert!(newsize as u32 <= MAX_ENTRIES_PER_CHUNK);
            let chunk = &mut self.chunk_data[chunkno];
            chunk
                .try_reserve(newsize - chunk.len())
                .map_err(|_| mcx.oom(newsize * 2))?;
            chunk.resize(newsize, 0);
        }

        self.chunk_data[chunkno][usage as usize] = chunkoffset as u16;
        self.chunk_usage[chunkno] += 1;
        Ok(())
    }

    pub fn get_blocks(
        &self,
        start_blkno: BlockNumber,
        stop_blkno: BlockNumber,
        blocks: &mut [BlockNumber],
    ) -> usize {
        let nblocks = blocks.len();
        let mut nresults = 0usize;

        let start_chunkno = start_blkno / BLOCKS_PER_CHUNK;
        let mut stop_chunkno = stop_blkno / BLOCKS_PER_CHUNK;
        if (stop_blkno % BLOCKS_PER_CHUNK) != 0 {
            stop_chunkno += 1;
        }
        if stop_chunkno > self.nchunks() {
            stop_chunkno = self.nchunks();
        }

        for chunkno in start_chunkno..stop_chunkno {
            let chunk_usage = self.chunk_usage[chunkno as usize] as u32;
            let chunk_data = &self.chunk_data[chunkno as usize];
            let mut start_offset = 0u32;
            let mut stop_offset = BLOCKS_PER_CHUNK;

            if chunkno == start_chunkno {
                start_offset = start_blkno % BLOCKS_PER_CHUNK;
            }
            if chunkno == stop_chunkno - 1 {
                // C asserts stop_offset <= BLOCKS_PER_CHUNK; when stop_chunkno
                // was clamped it can exceed it, so enforce the invariant.
                stop_offset = (stop_blkno - (chunkno * BLOCKS_PER_CHUNK)).min(BLOCKS_PER_CHUNK);
            }

            if chunk_usage == MAX_ENTRIES_PER_CHUNK {
                for i in start_offset..stop_offset {
                    let w = chunk_data[(i / BLOCKS_PER_ENTRY) as usize];
                    if (w & (1u16 << (i % BLOCKS_PER_ENTRY))) != 0 {
                        blocks[nresults] = chunkno * BLOCKS_PER_CHUNK + i;
                        nresults += 1;
                        if nresults == nblocks {
                            return nresults;
                        }
                    }
                }
            } else {
                for i in 0..chunk_usage as usize {
                    let offset = chunk_data[i] as u32;
                    if offset >= start_offset && offset < stop_offset {
                        blocks[nresults] = chunkno * BLOCKS_PER_CHUNK + offset;
                        nresults += 1;
                        if nresults == nblocks {
                            return nresults;
                        }
                    }
                }
            }
        }

        nresults
    }

    fn trimmed_nchunks(&self) -> u32 {
        let mut n = self.nchunks();
        while n > 0 && self.chunk_usage[(n - 1) as usize] == 0 {
            n -= 1;
        }
        n
    }
}

impl<'mcx> BlockRefTable<'mcx> {
    pub fn new(mcx: Mcx<'mcx>) -> Self {
        // C sizes the hash for a few thousand relation forks up front.
        BlockRefTable {
            mcx,
            hash: PgFxHashMap::with_capacity_and_hasher_in(4096, Default::default(), mcx),
        }
    }

    pub fn set_limit_block(
        &mut self,
        rlocator: RelFileLocator,
        forknum: ForkNumber,
        limit_block: BlockNumber,
    ) {
        let key = BlockRefTableKey { rlocator, forknum };
        let mcx = self.mcx;
        match self.hash.entry(key) {
            hashbrown::hash_map::Entry::Vacant(v) => {
                let mut e = BlockRefTableEntry::new(mcx, rlocator, forknum);
                e.limit_block = limit_block;
                v.insert(e);
            }
            hashbrown::hash_map::Entry::Occupied(mut o) => {
                o.get_mut().set_limit_block(limit_block);
            }
        }
    }

    pub fn mark_block_modified(
        &mut self,
        rlocator: RelFileLocator,
        forknum: ForkNumber,
        blknum: BlockNumber,
    ) -> PgResult<()> {
        let key = BlockRefTableKey { rlocator, forknum };
        let mcx = self.mcx;
        let entry = self
            .hash
            .entry(key)
            .or_insert_with(|| BlockRefTableEntry::new(mcx, rlocator, forknum));
        entry.mark_block_modified(mcx, blknum)
    }

    pub fn get_entry(
        &self,
        rlocator: RelFileLocator,
        forknum: ForkNumber,
    ) -> Option<&BlockRefTableEntry<'mcx>> {
        self.hash.get(&BlockRefTableKey { rlocator, forknum })
    }

    pub fn write<W>(&self, write_callback: W) -> PgResult<()>
    where
        W: FnMut(&[u8]) -> PgResult<()>,
    {
        let mut writer = BlockRefTableWriter::new(self.mcx, write_callback)?;

        let mut sorted: PgVec<'_, &BlockRefTableEntry<'mcx>> = PgVec::new_in(self.mcx);
        sorted
            .try_reserve(self.hash.len())
            .map_err(|_| self.mcx.oom(self.hash.len() * 8))?;
        for entry in self.hash.values() {
            sorted.push(entry);
        }
        sorted.sort_unstable_by_key(|e| {
            (
                e.key.rlocator.spcOid,
                e.key.rlocator.dbOid,
                e.key.rlocator.relNumber,
                e.key.forknum as i32,
            )
        });
        for entry in &sorted {
            writer.write_entry(entry)?;
        }
        writer.close()
    }
}

struct WriteBuffer<'mcx, W: FnMut(&[u8]) -> PgResult<()>> {
    write_callback: W,
    data: PgVec<'mcx, u8>,
    crc: u32,
}

impl<'mcx, W: FnMut(&[u8]) -> PgResult<()>> WriteBuffer<'mcx, W> {
    fn new(mcx: Mcx<'mcx>, write_callback: W) -> PgResult<Self> {
        let mut data = vec_new_in(mcx);
        data.try_reserve(BUFSIZE).map_err(|_| mcx.oom(BUFSIZE))?;
        Ok(WriteBuffer {
            write_callback,
            data,
            crc: CRC32C_INIT,
        })
    }

    fn write(&mut self, data: &[u8]) -> PgResult<()> {
        self.crc = pg_comp_crc32c(self.crc, data);

        if self.data.len() + data.len() > BUFSIZE {
            (self.write_callback)(&self.data)?;
            self.data.clear();
        }
        if data.len() >= BUFSIZE {
            return (self.write_callback)(data);
        }
        vec_append_bytes(&mut self.data, data)
    }

    fn terminate(&mut self) -> PgResult<()> {
        self.write(&[0u8; SERIALIZED_ENTRY_LEN])?;
        // Snapshot the running CRC before the CRC bytes themselves perturb it.
        let crc = fin_crc32c(self.crc);
        self.write(&crc.to_ne_bytes())?;
        (self.write_callback)(&self.data)?;
        self.data.clear();
        Ok(())
    }
}

pub struct BlockRefTableWriter<'mcx, W: FnMut(&[u8]) -> PgResult<()>> {
    buffer: WriteBuffer<'mcx, W>,
}

impl<'mcx, W: FnMut(&[u8]) -> PgResult<()>> BlockRefTableWriter<'mcx, W> {
    pub fn new(mcx: Mcx<'mcx>, write_callback: W) -> PgResult<Self> {
        let mut buffer = WriteBuffer::new(mcx, write_callback)?;
        buffer.write(&BLOCKREFTABLE_MAGIC.to_ne_bytes())?;
        Ok(BlockRefTableWriter { buffer })
    }

    // Entries must arrive sorted by tablespace, database, relfilenumber, fork.
    pub fn write_entry(&mut self, entry: &BlockRefTableEntry<'_>) -> PgResult<()> {
        let sentry = SerializedEntry {
            rlocator: entry.key.rlocator,
            forknum: entry.key.forknum as i32,
            limit_block: entry.limit_block,
            nchunks: entry.trimmed_nchunks(),
        };
        self.buffer.write(&sentry.to_bytes())?;

        if sentry.nchunks != 0 {
            self.buffer
                .write(u16s_as_bytes(&entry.chunk_usage[..sentry.nchunks as usize]))?;
        }
        for j in 0..entry.chunk_usage.len() {
            let used = entry.chunk_usage[j] as usize;
            if used == 0 {
                continue;
            }
            self.buffer
                .write(u16s_as_bytes(&entry.chunk_data[j][..used]))?;
        }
        Ok(())
    }

    pub fn close(mut self) -> PgResult<()> {
        self.buffer.terminate()
    }
}

pub struct BlockRefTableReader<'mcx, 'f, R: FnMut(&mut [u8]) -> PgResult<usize>> {
    read_callback: R,
    error_filename: &'f str,
    data: PgVec<'mcx, u8>,
    used: usize,
    cursor: usize,
    crc: u32,
    total_chunks: u32,
    consumed_chunks: u32,
    chunk_size: PgVec<'mcx, u16>,
    chunk_data: PgVec<'mcx, u16>,
    chunk_position: u32,
}

impl<'mcx, 'f, R: FnMut(&mut [u8]) -> PgResult<usize>> BlockRefTableReader<'mcx, 'f, R> {
    pub fn new(mcx: Mcx<'mcx>, read_callback: R, error_filename: &'f str) -> PgResult<Self> {
        let mut data = vec_new_in(mcx);
        data.try_reserve(BUFSIZE).map_err(|_| mcx.oom(BUFSIZE))?;
        data.resize(BUFSIZE, 0);
        let mut chunk_data = vec_new_in(mcx);
        chunk_data
            .try_reserve(MAX_ENTRIES_PER_CHUNK as usize)
            .map_err(|_| mcx.oom(MAX_ENTRIES_PER_CHUNK as usize * 2))?;
        chunk_data.resize(MAX_ENTRIES_PER_CHUNK as usize, 0);

        let mut reader = BlockRefTableReader {
            read_callback,
            error_filename,
            data,
            used: 0,
            cursor: 0,
            crc: CRC32C_INIT,
            total_chunks: 0,
            consumed_chunks: 0,
            chunk_size: vec_new_in(mcx),
            chunk_data,
            chunk_position: 0,
        };

        let mut magic_bytes = [0u8; 4];
        reader.read(&mut magic_bytes)?;
        let magic = u32::from_ne_bytes(magic_bytes);
        if magic != BLOCKREFTABLE_MAGIC {
            return Err(PgError::error(format!(
                "file \"{}\" has wrong magic number: expected {}, found {}",
                reader.error_filename, BLOCKREFTABLE_MAGIC, magic
            ))
            .into());
        }
        Ok(reader)
    }

    fn read(&mut self, out: &mut [u8]) -> PgResult<()> {
        let mut written = 0usize;
        while written < out.len() {
            let length = out.len() - written;
            if self.cursor < self.used {
                let n = core::cmp::min(length, self.used - self.cursor);
                out[written..written + n]
                    .copy_from_slice(&self.data[self.cursor..self.cursor + n]);
                self.crc = pg_comp_crc32c(self.crc, &self.data[self.cursor..self.cursor + n]);
                self.cursor += n;
                written += n;
            } else if length >= BUFSIZE {
                let dst = &mut out[written..];
                let n = (self.read_callback)(dst)?;
                self.crc = pg_comp_crc32c(self.crc, &dst[..n]);
                written += n;
                if n == 0 {
                    return Err(self.ends_unexpectedly());
                }
            } else {
                self.used = (self.read_callback)(&mut self.data[..])?;
                self.cursor = 0;
                if self.used == 0 {
                    return Err(self.ends_unexpectedly());
                }
            }
        }
        Ok(())
    }

    #[track_caller]
    #[cold]
    fn ends_unexpectedly(&self) -> Box<PgError> {
        PgError::error(format!(
            "file \"{}\" ends unexpectedly",
            self.error_filename
        ))
        .into()
    }

    pub fn next_relation(
        &mut self,
    ) -> PgResult<Option<(RelFileLocator, ForkNumber, BlockNumber)>> {
        debug_assert_eq!(self.total_chunks, self.consumed_chunks);

        let mut sbytes = [0u8; SERIALIZED_ENTRY_LEN];
        self.read(&mut sbytes)?;

        if sbytes == [0u8; SERIALIZED_ENTRY_LEN] {
            // File CRC excludes the CRC bytes: finalize a snapshot first.
            let expected_crc = fin_crc32c(self.crc);
            let mut actual_bytes = [0u8; 4];
            self.read(&mut actual_bytes)?;
            let actual_crc = u32::from_ne_bytes(actual_bytes);
            if expected_crc != actual_crc {
                return Err(PgError::error(format!(
                    "file \"{}\" has wrong checksum: expected {:08X}, found {:08X}",
                    self.error_filename, expected_crc, actual_crc
                ))
                .into());
            }
            return Ok(None);
        }

        let sentry = SerializedEntry::from_bytes(&sbytes);

        self.chunk_size.clear();
        let n = sentry.nchunks as usize;
        let alloc = *self.chunk_size.allocator();
        self.chunk_size.try_reserve(n).map_err(|_| alloc.oom(n * 2))?;
        self.chunk_size.resize(n, 0);
        let mut size_words = core::mem::replace(&mut self.chunk_size, PgVec::new_in(alloc));
        let res = self.read(u16s_as_bytes_mut(&mut size_words));
        self.chunk_size = size_words;
        res?;

        self.total_chunks = sentry.nchunks;
        self.consumed_chunks = 0;

        // C carries the raw int through; unknown fork values only occur in
        // corrupt files and collapse to InvalidForkNumber here.
        let forknum =
            ForkNumber::from_i32(sentry.forknum).unwrap_or(ForkNumber::InvalidForkNumber);
        Ok(Some((sentry.rlocator, forknum, sentry.limit_block)))
    }

    pub fn get_blocks(&mut self, blocks: &mut [BlockNumber]) -> PgResult<usize> {
        let nblocks = blocks.len();
        debug_assert!(nblocks > 0);
        let mut blocks_found = 0usize;

        loop {
            if self.consumed_chunks > 0 {
                let chunkno = self.consumed_chunks - 1;
                let chunk_size = self.chunk_size[chunkno as usize] as u32;

                if chunk_size == MAX_ENTRIES_PER_CHUNK {
                    while self.chunk_position < BLOCKS_PER_CHUNK && blocks_found < nblocks {
                        let off = self.chunk_position;
                        let w = self.chunk_data[(off / BLOCKS_PER_ENTRY) as usize];
                        if (w & (1u16 << (off % BLOCKS_PER_ENTRY))) != 0 {
                            blocks[blocks_found] = chunkno * BLOCKS_PER_CHUNK + off;
                            blocks_found += 1;
                        }
                        self.chunk_position += 1;
                    }
                } else {
                    while self.chunk_position < chunk_size && blocks_found < nblocks {
                        blocks[blocks_found] = chunkno * BLOCKS_PER_CHUNK
                            + self.chunk_data[self.chunk_position as usize] as u32;
                        blocks_found += 1;
                        self.chunk_position += 1;
                    }
                }
            }

            if blocks_found >= nblocks {
                break;
            }
            if self.consumed_chunks == self.total_chunks {
                break;
            }

            let next_chunk_size = self.chunk_size[self.consumed_chunks as usize] as usize;
            if next_chunk_size > 0 {
                let alloc = *self.chunk_data.allocator();
                let mut chunk = core::mem::replace(&mut self.chunk_data, PgVec::new_in(alloc));
                let res = self.read(u16s_as_bytes_mut(&mut chunk[..next_chunk_size]));
                self.chunk_data = chunk;
                res?;
            }
            self.consumed_chunks += 1;
            self.chunk_position = 0;
        }

        Ok(blocks_found)
    }
}

#[cfg(test)]
mod tests;
