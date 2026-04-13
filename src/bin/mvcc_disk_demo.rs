//! mvcc_disk_demo — end-to-end MVCC insert/update/delete demo with on-disk layout dumps.
//!
//! This exercises the full path through:
//! - typed tuple layout
//! - heap insert/update/delete with MVCC headers
//! - buffer manager flush
//! - storage manager persistence
//! - raw on-disk page inspection after each operation
//! - visibility-aware scans through snapshots
//!
//! Run with:
//!   cargo run --bin mvcc_disk_demo
//!   cargo run --bin mvcc_disk_demo -- --show-file-state

use pgrust::backend::access::heap::heapam::{
    heap_delete, heap_flush, heap_insert_mvcc, heap_scan_begin_visible, heap_scan_next_visible,
    heap_update,
};
use pgrust::backend::access::transam::xact::{
    INVALID_TRANSACTION_ID, Snapshot, TransactionManager,
};
use pgrust::backend::storage::page::bufpage::{
    ItemIdFlags, page_get_item_id, page_get_max_offset_number, page_header,
};
use pgrust::backend::storage::smgr::{
    ForkNumber, MdStorageManager, RelFileLocator, StorageManager,
};
use pgrust::include::access::htup::{
    AttributeAlign, AttributeCompression, AttributeDesc, AttributeStorage, HeapTuple, TupleValue,
};
use pgrust::{BufferPool, SmgrStorageBackend};
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

const FILE_DUMP_BYTES_PER_BLOCK: usize = 96;

fn rel() -> RelFileLocator {
    RelFileLocator {
        spc_oid: 0,
        db_oid: 1,
        rel_number: 13000,
    }
}

fn tuple_desc() -> Vec<AttributeDesc> {
    vec![
        AttributeDesc {
            name: "id".into(),
            attlen: 4,
            attalign: AttributeAlign::Int,
            attstorage: AttributeStorage::Plain,
            attcompression: AttributeCompression::Default,
            nullable: false,
        },
        AttributeDesc {
            name: "name".into(),
            attlen: -1,
            attalign: AttributeAlign::Int,
            attstorage: AttributeStorage::Extended,
            attcompression: AttributeCompression::Default,
            nullable: false,
        },
        AttributeDesc {
            name: "status".into(),
            attlen: -1,
            attalign: AttributeAlign::Int,
            attstorage: AttributeStorage::Extended,
            attcompression: AttributeCompression::Default,
            nullable: true,
        },
    ]
}

fn int4(v: i32) -> Vec<u8> {
    v.to_le_bytes().to_vec()
}

fn text(v: &str) -> Vec<u8> {
    v.as_bytes().to_vec()
}

fn tuple(desc: &[AttributeDesc], id: i32, name: &str, status: Option<&str>) -> HeapTuple {
    HeapTuple::from_values(
        desc,
        &[
            TupleValue::Bytes(int4(id)),
            TupleValue::Bytes(text(name)),
            match status {
                Some(v) => TupleValue::Bytes(text(v)),
                None => TupleValue::Null,
            },
        ],
    )
    .unwrap()
}

fn header(title: &str) {
    println!();
    println!("=== {} ===", title);
}

fn info(msg: &str) {
    println!("  {}", msg);
}

fn values_to_string(desc: &[AttributeDesc], tuple: &HeapTuple) -> String {
    let values = tuple.deform(desc).unwrap();
    let id = i32::from_le_bytes(values[0].unwrap().try_into().unwrap());
    let name = std::str::from_utf8(values[1].unwrap()).unwrap().to_owned();
    let status = values[2]
        .map(|v| std::str::from_utf8(v).unwrap().to_owned())
        .unwrap_or_else(|| "NULL".into());
    format!("id={} name={:?} status={:?}", id, name, status)
}

fn print_visible_rows(
    base_dir: &Path,
    desc: &[AttributeDesc],
    txns: &TransactionManager,
    snapshot: Snapshot,
    label: &str,
) {
    let smgr = MdStorageManager::new(base_dir);
    let pool = std::sync::Arc::new(BufferPool::new(SmgrStorageBackend::new(smgr), 8));
    let mut scan = heap_scan_begin_visible(&pool, 99, rel(), snapshot).unwrap();

    println!("  visible rows for {}:", label);
    let mut saw_any = false;
    while let Some((tid, tuple)) = heap_scan_next_visible(&*pool, 99, txns, &mut scan).unwrap() {
        saw_any = true;
        println!(
            "    ({},{}) {}",
            tid.block_number,
            tid.offset_number,
            values_to_string(desc, &tuple)
        );
    }
    if !saw_any {
        println!("    <none>");
    }
}

fn print_file_state(base_dir: &Path) {
    let path = base_dir.join("1").join("13000");
    let bytes = fs::read(&path).unwrap();
    println!("  raw file state:");
    println!(
        "    path={:?} size={} bytes blocks={}",
        path,
        bytes.len(),
        bytes.len().div_ceil(pgrust::BLCKSZ)
    );

    for (block, chunk) in bytes.chunks(pgrust::BLCKSZ).enumerate() {
        println!("    block {} raw bytes:", block);
        let dump_len = chunk.len().min(FILE_DUMP_BYTES_PER_BLOCK);
        for (line_no, line) in chunk[..dump_len].chunks(16).enumerate() {
            let base = line_no * 16;
            let hex = line
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect::<Vec<_>>()
                .join(" ");
            println!("      {:04x}: {}", base, hex);
        }
        if chunk.len() > dump_len {
            println!(
                "      ... {} more bytes omitted for this block",
                chunk.len() - dump_len
            );
        }
    }
}

fn inspect_relation(base_dir: &Path, desc: &[AttributeDesc], title: &str, show_file_state: bool) {
    header(title);

    let mut smgr = MdStorageManager::new(base_dir);
    smgr.open(rel()).unwrap();
    let nblocks = smgr.nblocks(rel(), ForkNumber::Main).unwrap();
    info(&format!(
        "relation file: {:?}",
        base_dir.join("1").join("13000")
    ));
    info(&format!("main fork blocks on disk: {}", nblocks));

    for block in 0..nblocks {
        let mut page = [0u8; pgrust::BLCKSZ];
        smgr.read_block(rel(), ForkNumber::Main, block, &mut page)
            .unwrap();

        let page_hdr = page_header(&page).unwrap();
        let max_offset = page_get_max_offset_number(&page).unwrap();
        println!(
            "  block {}  pd_lower={} pd_upper={} free={} max_offset={}",
            block,
            page_hdr.pd_lower,
            page_hdr.pd_upper,
            page_hdr.free_space(),
            max_offset
        );

        for off in 1..=max_offset {
            let item_id = page_get_item_id(&page, off).unwrap();
            println!(
                "    lp {} -> off={} len={} flags={:?}",
                off, item_id.lp_off, item_id.lp_len, item_id.lp_flags
            );

            if item_id.lp_flags != ItemIdFlags::Normal || !item_id.has_storage() {
                continue;
            }

            let tuple = pgrust::include::access::htup::heap_page_get_tuple(&page, off).unwrap();
            println!(
                "      tuple xmin={} xmax={} ctid=({},{}) hoff={} natts={}",
                tuple.header.xmin,
                tuple.header.xmax,
                tuple.header.ctid.block_number,
                tuple.header.ctid.offset_number,
                tuple.header.hoff,
                tuple.header.infomask2 & 0x07ff
            );
            println!("      values {}", values_to_string(desc, &tuple));
        }
    }

    if show_file_state {
        print_file_state(base_dir);
    }
}

fn flush_blocks(pool: &BufferPool<SmgrStorageBackend>, blocks: impl IntoIterator<Item = u32>) {
    let mut seen = BTreeSet::new();
    for block in blocks {
        if seen.insert(block) {
            heap_flush(pool, 1, rel(), block).unwrap();
        }
    }
}

fn main() {
    let show_file_state = std::env::args()
        .skip(1)
        .any(|arg| arg == "--show-file-state");
    let base_dir = PathBuf::from(std::env::temp_dir()).join("pgrust_mvcc_disk_demo");
    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&base_dir).unwrap();

    let desc = tuple_desc();
    let mut txns = TransactionManager::new_durable(&base_dir).unwrap();

    header("Setup");
    info(&format!("base directory: {:?}", base_dir));
    info(&format!("show raw file state: {}", show_file_state));

    let smgr = MdStorageManager::new(&base_dir);
    let pool = BufferPool::new(SmgrStorageBackend::new(smgr), 8);

    let insert_xid = txns.begin();
    let insert_tid = heap_insert_mvcc(
        &pool,
        1,
        rel(),
        insert_xid,
        &tuple(&desc, 1, "alice", Some("new")),
    )
    .unwrap();
    flush_blocks(&pool, [insert_tid.block_number]);
    inspect_relation(
        &base_dir,
        &desc,
        "After insert, before commit",
        show_file_state,
    );
    print_visible_rows(
        &base_dir,
        &desc,
        &txns,
        txns.snapshot(INVALID_TRANSACTION_ID).unwrap(),
        "outside snapshot before insert commit",
    );

    txns.commit(insert_xid).unwrap();
    header("Insert commit");
    info(&format!(
        "committed xid {} for tid ({},{})",
        insert_xid, insert_tid.block_number, insert_tid.offset_number
    ));
    print_visible_rows(
        &base_dir,
        &desc,
        &txns,
        txns.snapshot(INVALID_TRANSACTION_ID).unwrap(),
        "outside snapshot after insert commit",
    );

    let update_xid = txns.begin();
    let update_tid = heap_update(
        &pool,
        1,
        rel(),
        &txns,
        update_xid,
        insert_tid,
        &tuple(&desc, 1, "alice", Some("updated")),
    )
    .unwrap();
    flush_blocks(&pool, [insert_tid.block_number, update_tid.block_number]);
    inspect_relation(
        &base_dir,
        &desc,
        "After update, before commit",
        show_file_state,
    );

    let concurrent_reader = txns.begin();
    print_visible_rows(
        &base_dir,
        &desc,
        &txns,
        txns.snapshot(concurrent_reader).unwrap(),
        "concurrent snapshot before update commit",
    );

    txns.commit(update_xid).unwrap();
    header("Update commit");
    info(&format!(
        "committed xid {} old=({},{}) new=({},{})",
        update_xid,
        insert_tid.block_number,
        insert_tid.offset_number,
        update_tid.block_number,
        update_tid.offset_number
    ));
    print_visible_rows(
        &base_dir,
        &desc,
        &txns,
        txns.snapshot(INVALID_TRANSACTION_ID).unwrap(),
        "outside snapshot after update commit",
    );

    let delete_xid = txns.begin();
    heap_delete(&pool, 1, rel(), &txns, delete_xid, update_tid).unwrap();
    flush_blocks(&pool, [update_tid.block_number]);
    inspect_relation(
        &base_dir,
        &desc,
        "After delete, before commit",
        show_file_state,
    );

    print_visible_rows(
        &base_dir,
        &desc,
        &txns,
        txns.snapshot(INVALID_TRANSACTION_ID).unwrap(),
        "outside snapshot before delete commit",
    );

    txns.commit(delete_xid).unwrap();
    header("Delete commit");
    info(&format!(
        "committed xid {} deleted tid ({},{})",
        delete_xid, update_tid.block_number, update_tid.offset_number
    ));
    print_visible_rows(
        &base_dir,
        &desc,
        &txns,
        txns.snapshot(INVALID_TRANSACTION_ID).unwrap(),
        "outside snapshot after delete commit",
    );

    println!();
    println!("Files remain at {:?} for inspection.", base_dir.join("1"));
}
