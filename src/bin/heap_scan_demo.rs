//! heap_scan_demo — end-to-end heap insert + scan demo.
//!
//! This exercises the full path through:
//! - typed tuple layout
//! - heap insert
//! - buffer manager flush
//! - storage manager persistence
//! - sequential heap scan
//!
//! Run with: cargo run --bin heap_scan_demo

use pgrust::backend::access::heap::heapam::{
    heap_flush, heap_insert, heap_scan_begin, heap_scan_next,
};
use pgrust::backend::storage::smgr::{MdStorageManager, RelFileLocator};
use pgrust::include::access::htup::{AttributeAlign, AttributeDesc, HeapTuple, TupleValue};
use pgrust::{BufferPool, SmgrStorageBackend};
use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;

fn header(title: &str) {
    println!();
    println!("=== {} ===", title);
}

fn ok(msg: &str) {
    println!("  [ok] {}", msg);
}

fn info(msg: &str) {
    println!("       {}", msg);
}

fn rel() -> RelFileLocator {
    RelFileLocator {
        spc_oid: 0,
        db_oid: 1,
        rel_number: 12000,
    }
}

fn tuple_desc() -> Vec<AttributeDesc> {
    vec![
        AttributeDesc {
            name: "id".into(),
            attlen: 4,
            attalign: AttributeAlign::Int,
            nullable: false,
        },
        AttributeDesc {
            name: "name".into(),
            attlen: -1,
            attalign: AttributeAlign::Int,
            nullable: false,
        },
        AttributeDesc {
            name: "note".into(),
            attlen: -1,
            attalign: AttributeAlign::Int,
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

fn main() {
    let base_dir = PathBuf::from(std::env::temp_dir()).join("pgrust_heap_scan_demo");
    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&base_dir).unwrap();

    let desc = tuple_desc();
    let rows = vec![
        vec![
            TupleValue::Bytes(int4(1)),
            TupleValue::Bytes(text("alice")),
            TupleValue::Bytes(text("engineer")),
        ],
        vec![
            TupleValue::Bytes(int4(2)),
            TupleValue::Bytes(text("bob")),
            TupleValue::Null,
        ],
        vec![
            TupleValue::Bytes(int4(3)),
            TupleValue::Bytes(text("carol")),
            TupleValue::Bytes(text("storage")),
        ],
    ];

    header("Insert");
    info(&format!("base directory: {:?}", base_dir));

    let touched_blocks = {
        let smgr = MdStorageManager::new(&base_dir);
        let pool = BufferPool::new(SmgrStorageBackend::new(smgr), 8);
        let mut touched = BTreeSet::new();

        for row in &rows {
            let tuple = HeapTuple::from_values(&desc, row).unwrap();
            let tid = heap_insert(&pool, 1, rel(), &tuple).unwrap();
            touched.insert(tid.block_number);
            info(&format!(
                "inserted row at ({},{})",
                tid.block_number, tid.offset_number
            ));
        }

        for block in &touched {
            heap_flush(&pool, 1, rel(), *block).unwrap();
            info(&format!("flushed block {}", block));
        }

        ok(&format!("inserted {} row(s)", rows.len()));
        touched
    };

    header("Scan");
    let smgr = MdStorageManager::new(&base_dir);
    let pool = BufferPool::new(SmgrStorageBackend::new(smgr), 8);
    let mut scan = heap_scan_begin(&pool, rel()).unwrap();
    let mut scanned = Vec::new();

    while let Some((tid, tuple)) = heap_scan_next(&pool, 2, &mut scan).unwrap() {
        let vals = tuple.deform(&desc).unwrap();
        let id = i32::from_le_bytes(vals[0].unwrap().try_into().unwrap());
        let name = std::str::from_utf8(vals[1].unwrap()).unwrap().to_owned();
        let note = vals[2].map(|v| std::str::from_utf8(v).unwrap().to_owned());

        println!(
            "  tid=({},{}) id={} name={:?} note={:?}",
            tid.block_number, tid.offset_number, id, name, note
        );
        scanned.push((id, name, note));
    }

    assert_eq!(
        scanned,
        vec![
            (1, "alice".to_string(), Some("engineer".to_string())),
            (2, "bob".to_string(), None),
            (3, "carol".to_string(), Some("storage".to_string())),
        ]
    );
    ok(&format!(
        "scanned {} row(s) back successfully across {} block(s)",
        scanned.len(),
        touched_blocks.len()
    ));

    println!();
    println!("Files remain at {:?} for inspection.", base_dir.join("1"));
}
