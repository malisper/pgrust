use super::*;
use mcx::MemoryContext;
use std::cell::Cell;
use std::vec::Vec;
use types_core::MAX_FORKNUM;

fn rl(spc: u32, db: u32, rel: u32) -> RelFileLocator {
    RelFileLocator::new(spc, db, rel)
}

fn serialize(brtab: &BlockRefTable<'_>) -> Vec<u8> {
    let mut out = Vec::new();
    brtab
        .write(|bytes: &[u8]| {
            out.extend_from_slice(bytes);
            Ok(())
        })
        .expect("write");
    out
}

fn reader_over<'a>(
    mcx: Mcx<'a>,
    bytes: &'a [u8],
    filename: &'a str,
) -> PgResult<BlockRefTableReader<'a, 'a, impl FnMut(&mut [u8]) -> PgResult<usize> + 'a>> {
    let pos = Cell::new(0usize);
    BlockRefTableReader::new(
        mcx,
        move |out: &mut [u8]| {
            let avail = bytes.len() - pos.get();
            let n = core::cmp::min(avail, out.len());
            out[..n].copy_from_slice(&bytes[pos.get()..pos.get() + n]);
            pos.set(pos.get() + n);
            Ok(n)
        },
        filename,
    )
}

fn drain_all(reader: &mut BlockRefTableReader<'_, '_, impl FnMut(&mut [u8]) -> PgResult<usize>>) -> Vec<BlockNumber> {
    let mut got = Vec::new();
    let mut buf = [0u32; 3];
    loop {
        let n = reader.get_blocks(&mut buf).expect("get_blocks");
        if n == 0 {
            break;
        }
        got.extend_from_slice(&buf[..n]);
    }
    got
}

#[test]
fn serialized_bytes_match_c_format() {
    let cx = MemoryContext::new("brt-bytes");
    let mut brtab = BlockRefTable::new(cx.mcx());
    brtab
        .mark_block_modified(rl(1663, 5, 16384), ForkNumber::MAIN_FORKNUM, 1)
        .unwrap();
    brtab
        .mark_block_modified(rl(1663, 5, 16384), ForkNumber::MAIN_FORKNUM, 5)
        .unwrap();
    let bytes = serialize(&brtab);

    let mut expected: Vec<u8> = Vec::new();
    expected.extend_from_slice(&0x652b137bu32.to_le_bytes());
    for w in [1663u32, 5, 16384, 0, 0xFFFF_FFFF, 1] {
        expected.extend_from_slice(&w.to_le_bytes());
    }
    expected.extend_from_slice(&2u16.to_le_bytes());
    expected.extend_from_slice(&1u16.to_le_bytes());
    expected.extend_from_slice(&5u16.to_le_bytes());
    expected.extend_from_slice(&[0u8; 24]);
    let crc = fin_crc32c(pg_comp_crc32c(CRC32C_INIT, &expected));
    expected.extend_from_slice(&crc.to_le_bytes());

    assert_eq!(bytes, expected);
}

#[test]
fn empty_table_serializes_magic_sentinel_crc() {
    let cx = MemoryContext::new("brt-empty");
    let brtab = BlockRefTable::new(cx.mcx());
    let bytes = serialize(&brtab);
    assert_eq!(bytes.len(), 4 + 24 + 4);

    let mut expected: Vec<u8> = Vec::new();
    expected.extend_from_slice(&0x652b137bu32.to_le_bytes());
    expected.extend_from_slice(&[0u8; 24]);
    let crc = fin_crc32c(pg_comp_crc32c(CRC32C_INIT, &expected));
    expected.extend_from_slice(&crc.to_le_bytes());
    assert_eq!(bytes, expected);
}

#[test]
fn roundtrip_array_bitmap_multichunk_and_sort_order() {
    let cx = MemoryContext::new("brt-rt");
    let mut brtab = BlockRefTable::new(cx.mcx());

    let loc_b = rl(1664, 5, 200);
    let loc_a = rl(1663, 5, 100);
    let mut expected_a: Vec<BlockNumber> = vec![0, 5, 100, 65535];
    for &b in &expected_a {
        brtab
            .mark_block_modified(loc_a, ForkNumber::MAIN_FORKNUM, b)
            .unwrap();
    }
    // Chunk 1 goes dense enough to convert to a bitmap.
    for off in 0..MAX_ENTRIES_PER_CHUNK {
        let b = BLOCKS_PER_CHUNK + off;
        brtab
            .mark_block_modified(loc_a, ForkNumber::MAIN_FORKNUM, b)
            .unwrap();
        expected_a.push(b);
    }
    for b in [2 * BLOCKS_PER_CHUNK + 7, 2 * BLOCKS_PER_CHUNK + 4095] {
        brtab
            .mark_block_modified(loc_a, ForkNumber::MAIN_FORKNUM, b)
            .unwrap();
        expected_a.push(b);
    }
    expected_a.sort_unstable();

    brtab
        .mark_block_modified(loc_b, ForkNumber::VISIBILITYMAP_FORKNUM, 42)
        .unwrap();
    brtab.set_limit_block(loc_b, ForkNumber::VISIBILITYMAP_FORKNUM, 100);

    let bytes = serialize(&brtab);
    let mut reader = reader_over(cx.mcx(), &bytes, "t").expect("reader");

    // Sorted by tablespace first: loc_a (1663) before loc_b (1664).
    let (got_rl, got_fork, limit) = reader.next_relation().expect("next").expect("rel a");
    assert_eq!(got_rl, loc_a);
    assert_eq!(got_fork, ForkNumber::MAIN_FORKNUM);
    assert_eq!(limit, InvalidBlockNumber);
    assert_eq!(drain_all(&mut reader), expected_a);

    let (got_rl, got_fork, limit) = reader.next_relation().expect("next").expect("rel b");
    assert_eq!(got_rl, loc_b);
    assert_eq!(got_fork, ForkNumber::VISIBILITYMAP_FORKNUM);
    assert_eq!(limit, 100);
    assert_eq!(drain_all(&mut reader), vec![42]);

    assert!(reader.next_relation().expect("crc ok").is_none());
}

#[test]
fn bitmap_conversion_at_max_minus_one() {
    let cx = MemoryContext::new("brt-conv");
    let mut entry = BlockRefTableEntry::new(cx.mcx(), rl(1, 1, 1), ForkNumber::MAIN_FORKNUM);
    // Distinct even offsets so the array fills to MAX-1 without duplicates.
    for i in 0..(MAX_ENTRIES_PER_CHUNK - 1) {
        entry.mark_block_modified(cx.mcx(), i * 2).unwrap();
    }
    let mut buf = vec![0u32; BLOCKS_PER_CHUNK as usize];
    assert_eq!(
        entry.get_blocks(0, BLOCKS_PER_CHUNK, &mut buf),
        (MAX_ENTRIES_PER_CHUNK - 1) as usize
    );
    // One more forces the bitmap; contents must survive the conversion.
    entry.mark_block_modified(cx.mcx(), 1).unwrap();
    let n = entry.get_blocks(0, BLOCKS_PER_CHUNK, &mut buf);
    assert_eq!(n, MAX_ENTRIES_PER_CHUNK as usize);
    let mut expected: Vec<u32> = (0..(MAX_ENTRIES_PER_CHUNK - 1)).map(|i| i * 2).collect();
    expected.push(1);
    expected.sort_unstable();
    assert_eq!(&buf[..n], &expected[..]);
}

#[test]
fn set_limit_block_forgets_higher_blocks() {
    let cx = MemoryContext::new("brt-limit");
    let mut entry = BlockRefTableEntry::new(cx.mcx(), rl(1, 1, 1), ForkNumber::MAIN_FORKNUM);
    for b in [10u32, 20, 30, 40] {
        entry.mark_block_modified(cx.mcx(), b).unwrap();
    }
    entry.set_limit_block(25);
    assert_eq!(entry.limit_block(), 25);
    let mut buf = [0u32; 100];
    let n = entry.get_blocks(0, 1000, &mut buf);
    let mut got = buf[..n].to_vec();
    got.sort_unstable();
    assert_eq!(got, vec![10, 20]);

    // Raising the limit afterwards is a no-op.
    entry.set_limit_block(1000);
    assert_eq!(entry.limit_block(), 25);
}

#[test]
fn set_limit_block_clears_bitmap_and_higher_chunks() {
    let cx = MemoryContext::new("brt-limit2");
    let mut entry = BlockRefTableEntry::new(cx.mcx(), rl(1, 1, 1), ForkNumber::MAIN_FORKNUM);
    for off in 0..MAX_ENTRIES_PER_CHUNK {
        entry.mark_block_modified(cx.mcx(), off).unwrap();
    }
    entry.mark_block_modified(cx.mcx(), BLOCKS_PER_CHUNK + 3).unwrap();
    entry.set_limit_block(100);
    let mut buf = [0u32; 8192];
    let n = entry.get_blocks(0, InvalidBlockNumber, &mut buf);
    let got = &buf[..n];
    assert!(got.iter().all(|&b| b < 100));
    assert_eq!(n, 100.min(MAX_ENTRIES_PER_CHUNK as usize));
}

#[test]
fn get_blocks_window_and_capacity() {
    let cx = MemoryContext::new("brt-window");
    let mut entry = BlockRefTableEntry::new(cx.mcx(), rl(1, 1, 1), ForkNumber::MAIN_FORKNUM);
    for b in [1u32, 3, 5, 7, 9] {
        entry.mark_block_modified(cx.mcx(), b).unwrap();
    }
    let mut buf = [0u32; 10];
    let n = entry.get_blocks(3, 8, &mut buf);
    let mut got = buf[..n].to_vec();
    got.sort_unstable();
    assert_eq!(got, vec![3, 5, 7]);

    // Output-capacity early exit.
    let mut small = [0u32; 2];
    assert_eq!(entry.get_blocks(0, InvalidBlockNumber, &mut small), 2);
}

#[test]
fn table_limit_block_on_missing_and_existing_entries() {
    let cx = MemoryContext::new("brt-table");
    let mut brtab = BlockRefTable::new(cx.mcx());
    let loc = rl(1, 2, 3);

    brtab.set_limit_block(loc, ForkNumber::MAIN_FORKNUM, 7);
    let e = brtab.get_entry(loc, ForkNumber::MAIN_FORKNUM).expect("entry");
    assert_eq!(e.limit_block(), 7);

    brtab
        .mark_block_modified(loc, ForkNumber::MAIN_FORKNUM, 3)
        .unwrap();
    brtab
        .mark_block_modified(loc, ForkNumber::MAIN_FORKNUM, 9)
        .unwrap();
    brtab.set_limit_block(loc, ForkNumber::MAIN_FORKNUM, 5);
    let e = brtab.get_entry(loc, ForkNumber::MAIN_FORKNUM).expect("entry");
    let mut buf = [0u32; 10];
    let n = e.get_blocks(0, InvalidBlockNumber, &mut buf);
    assert_eq!(&buf[..n], &[3]);
    assert!(brtab.get_entry(rl(9, 9, 9), ForkNumber::MAIN_FORKNUM).is_none());
}

#[test]
fn trailing_zero_chunks_are_trimmed() {
    let cx = MemoryContext::new("brt-trim");
    let mut brtab = BlockRefTable::new(cx.mcx());
    let loc = rl(1, 2, 3);
    brtab
        .mark_block_modified(loc, ForkNumber::MAIN_FORKNUM, 5)
        .unwrap();
    brtab.set_limit_block(loc, ForkNumber::MAIN_FORKNUM, 0);
    let bytes = serialize(&brtab);
    // nchunks trims to 0: entry occupies exactly 24 bytes, no chunk arrays.
    assert_eq!(bytes.len(), 4 + 24 + 24 + 4);
    let nchunks = u32::from_le_bytes(bytes[24..28].try_into().unwrap());
    assert_eq!(nchunks, 0);
}

#[test]
fn wrong_magic_is_rejected() {
    let cx = MemoryContext::new("brt-magic");
    let bytes = 0xdeadbeefu32.to_le_bytes().to_vec();
    let err = match reader_over(cx.mcx(), &bytes, "boom") {
        Err(e) => e,
        Ok(_) => panic!("wrong magic accepted"),
    };
    assert!(err.message().contains("has wrong magic number"));
    assert!(err.message().contains("boom"));
    assert_eq!(err.sqlstate(), ERRCODE_DATA_CORRUPTED);
}

#[test]
fn corrupt_crc_is_rejected() {
    let cx = MemoryContext::new("brt-crc");
    let mut brtab = BlockRefTable::new(cx.mcx());
    brtab
        .mark_block_modified(rl(1, 2, 3), ForkNumber::MAIN_FORKNUM, 5)
        .unwrap();
    let mut bytes = serialize(&brtab);
    let last = bytes.len() - 1;
    bytes[last] ^= 0xFF;

    let mut reader = reader_over(cx.mcx(), &bytes, "bad").expect("reader");
    let _ = reader.next_relation().expect("entry").expect("one rel");
    let _ = drain_all(&mut reader);
    let err = reader.next_relation().expect_err("bad crc");
    assert!(err.message().contains("has wrong checksum"));
    assert_eq!(err.sqlstate(), ERRCODE_DATA_CORRUPTED);
}

#[test]
fn truncated_file_ends_unexpectedly() {
    let cx = MemoryContext::new("brt-trunc");
    let mut brtab = BlockRefTable::new(cx.mcx());
    brtab
        .mark_block_modified(rl(1, 2, 3), ForkNumber::MAIN_FORKNUM, 5)
        .unwrap();
    let bytes = serialize(&brtab);
    let cut = &bytes[..bytes.len() - 10];

    let mut reader = reader_over(cx.mcx(), cut, "cut").expect("reader");
    let mut hit_err = false;
    for _ in 0..4 {
        match reader.next_relation() {
            Err(e) => {
                assert!(e.message().contains("ends unexpectedly"));
                assert_eq!(e.sqlstate(), ERRCODE_DATA_CORRUPTED);
                hit_err = true;
                break;
            }
            Ok(None) => panic!("truncated file validated"),
            Ok(Some(_)) => {
                let _ = drain_all(&mut reader);
            }
        }
    }
    assert!(hit_err);
}

#[test]
fn incremental_writer_matches_table_writer() {
    let cx = MemoryContext::new("brt-incr");
    let mut brtab = BlockRefTable::new(cx.mcx());
    let loc = rl(1663, 5, 42);
    for b in [3u32, 1, 70000] {
        brtab
            .mark_block_modified(loc, ForkNumber::MAIN_FORKNUM, b)
            .unwrap();
    }
    let table_bytes = serialize(&brtab);

    let mut entry = BlockRefTableEntry::new(cx.mcx(), loc, ForkNumber::MAIN_FORKNUM);
    for b in [3u32, 1, 70000] {
        entry.mark_block_modified(cx.mcx(), b).unwrap();
    }
    let mut incr_bytes = Vec::new();
    let mut writer = BlockRefTableWriter::new(cx.mcx(), |bytes: &[u8]| {
        incr_bytes.extend_from_slice(bytes);
        Ok(())
    })
    .expect("writer");
    writer.write_entry(&entry).expect("entry");
    writer.close().expect("close");

    assert_eq!(incr_bytes, table_bytes);
}

// upstream 01e568b8c11b (18.4): an entry whose nchunks would size the chunk
// array past MaxAllocSize is refused before anything is allocated.
#[test]
fn oversized_chunk_size_array_is_rejected() {
    let cx = MemoryContext::new("brt-nchunks");
    let mut bytes = BLOCKREFTABLE_MAGIC.to_ne_bytes().to_vec();
    let entry = SerializedEntry {
        rlocator: rl(1, 2, 3),
        forknum: 0,
        limit_block: InvalidBlockNumber,
        nchunks: u32::MAX,
    };
    bytes.extend_from_slice(&entry.to_bytes());
    let mut reader = reader_over(cx.mcx(), &bytes, "huge").expect("reader");
    let err = reader.next_relation().expect_err("oversized nchunks must be rejected");
    assert_eq!(err.message(), "file \"huge\" has oversized chunk size array");
    assert_eq!(err.sqlstate(), ERRCODE_DATA_CORRUPTED);
}

// upstream bcc428a23a69 (18.6): a corrupt fork number or chunk size is refused
// with C's message as it is read, instead of indexing past the fork or chunk
// arrays downstream.
fn two_chunk_entry_bytes(mcx: Mcx<'_>) -> Vec<u8> {
    let mut brtab = BlockRefTable::new(mcx);
    for b in [5u32, BLOCKS_PER_CHUNK + 7] {
        brtab
            .mark_block_modified(rl(1, 2, 3), ForkNumber::MAIN_FORKNUM, b)
            .unwrap();
    }
    serialize(&brtab)
}

// Image layout: 4-byte magic, 24-byte entry (forknum word at +12), then the
// u16 chunk_size array.
const FORKNUM_OFF: usize = 4 + 12;
const CHUNK_SIZE_OFF: usize = 4 + SERIALIZED_ENTRY_LEN;

#[test]
fn invalid_fork_number_is_rejected() {
    let cx = MemoryContext::new("brt-fork");
    for bad in [MAX_FORKNUM as i32 + 1, -1, 7, i32::MIN] {
        let mut bytes = two_chunk_entry_bytes(cx.mcx());
        bytes[FORKNUM_OFF..FORKNUM_OFF + 4].copy_from_slice(&bad.to_ne_bytes());
        let mut reader = reader_over(cx.mcx(), &bytes, "fork").expect("reader");
        let err = reader.next_relation().expect_err("corrupt fork number accepted");
        assert_eq!(err.message(), format!("file \"fork\" has invalid fork number {bad}"));
        assert_eq!(err.sqlstate(), ERRCODE_DATA_CORRUPTED);
    }

    // MAX_FORKNUM itself is the last valid value.
    let mut bytes = two_chunk_entry_bytes(cx.mcx());
    bytes[FORKNUM_OFF..FORKNUM_OFF + 4].copy_from_slice(&(MAX_FORKNUM as i32).to_ne_bytes());
    let mut reader = reader_over(cx.mcx(), &bytes, "fork").expect("reader");
    let (_, fork, _) = reader.next_relation().expect("next").expect("entry");
    assert_eq!(fork, MAX_FORKNUM);

    // The fork check comes before the nchunks check, as in C.
    let mut bytes = BLOCKREFTABLE_MAGIC.to_ne_bytes().to_vec();
    let entry = SerializedEntry {
        rlocator: rl(1, 2, 3),
        forknum: 7,
        limit_block: InvalidBlockNumber,
        nchunks: u32::MAX,
    };
    bytes.extend_from_slice(&entry.to_bytes());
    let mut reader = reader_over(cx.mcx(), &bytes, "fork").expect("reader");
    let err = reader.next_relation().expect_err("corrupt fork number accepted");
    assert_eq!(err.message(), "file \"fork\" has invalid fork number 7");
}

#[test]
fn oversized_chunk_is_rejected() {
    let cx = MemoryContext::new("brt-chunk");
    // Two chunks, so the reported index is the corrupt chunk's, not always 0.
    for (chunkno, bad) in [(1usize, MAX_ENTRIES_PER_CHUNK as u16 + 1), (0, 5000), (1, u16::MAX)] {
        let mut bytes = two_chunk_entry_bytes(cx.mcx());
        let off = CHUNK_SIZE_OFF + 2 * chunkno;
        bytes[off..off + 2].copy_from_slice(&bad.to_ne_bytes());
        let mut reader = reader_over(cx.mcx(), &bytes, "chunk").expect("reader");
        let err = reader.next_relation().expect_err("corrupt chunk size accepted");
        assert_eq!(
            err.message(),
            format!("file \"chunk\" chunk {chunkno} has invalid size {bad}")
        );
        assert_eq!(err.sqlstate(), ERRCODE_DATA_CORRUPTED);
    }

    // MAX_ENTRIES_PER_CHUNK itself (a bitmap chunk) is the last valid size.
    let mut bytes = two_chunk_entry_bytes(cx.mcx());
    bytes[CHUNK_SIZE_OFF..CHUNK_SIZE_OFF + 2]
        .copy_from_slice(&(MAX_ENTRIES_PER_CHUNK as u16).to_ne_bytes());
    let mut reader = reader_over(cx.mcx(), &bytes, "chunk").expect("reader");
    assert!(reader.next_relation().expect("max size is valid").is_some());
}
