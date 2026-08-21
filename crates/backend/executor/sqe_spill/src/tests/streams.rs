//! Stream substrate: append events, the commit watermark, extent
//! directories, segment splits, and the charter's cross-thread
//! write→open-by-name→read bar.

use std::sync::Arc;

use crate::page::PAGE_SIZE;
use crate::set::{spill_file_name, EpochDir, SpillExtent, SpillFile, StreamDir};

fn pattern(len: usize, seed: u8) -> Vec<u8> {
    (0..len).map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed)).collect()
}

#[test]
fn naming_law() {
    assert_eq!(spill_file_name(7, 3, "agg", 12), "spill-7-3-agg-w12");
}

#[test]
fn epoch_extents_roundtrip() {
    let (set, _dir, _cwd) = super::rig("streams-epoch");
    let mut file = SpillFile::new(Arc::clone(&set), spill_file_name(1, 0, "agg", 0));

    // Two epochs × three partitions, partition-contiguous (the m3.5 flush
    // shape). Partition 1 is empty in epoch 1 (len-0 extents are legal).
    let nparts = 3u32;
    let mut dir = StreamDir::new(nparts);
    let mut payloads: Vec<Vec<Vec<u8>>> = vec![Vec::new(); nparts as usize];
    for epoch in 0..2u32 {
        let mut w = file.append().unwrap();
        let mut ed = EpochDir::new(nparts);
        for p in 0..nparts {
            w.begin_extent();
            if !(epoch == 1 && p == 1) {
                let data = pattern(500 + (epoch * 1000 + p * 37) as usize, (epoch * 3 + p) as u8);
                w.write(&data).unwrap();
                payloads[p as usize].push(data);
            }
            ed.set_part(p, w.end_extent());
        }
        w.finish().unwrap();
        dir.push_epoch(ed);
    }
    assert_eq!(dir.nepochs(), 2);
    assert_eq!(file.committed(), dir.total_bytes());

    // Combine-side replay: partition extents in epoch order.
    let mut r = file.open_read();
    let mut buf = Vec::new();
    for p in 0..nparts {
        let extents: Vec<SpillExtent> = dir.part_extents(p).collect();
        assert_eq!(extents.len(), payloads[p as usize].len());
        for (ext, want) in extents.iter().zip(&payloads[p as usize]) {
            r.read_extent(*ext, &mut buf).unwrap();
            assert_eq!(&buf, want, "partition {p}");
        }
    }
    r.close().unwrap();
}

#[test]
fn abandoned_event_commits_nothing() {
    let (set, _dir, _cwd) = super::rig("streams-abandon");
    let mut file = SpillFile::new(Arc::clone(&set), "abandon".to_string());

    // Committed epoch.
    let first = pattern(PAGE_SIZE + 100, 1);
    let mut w = file.append().unwrap();
    w.write(&first).unwrap();
    let committed = w.finish().unwrap();
    assert_eq!(committed, first.len() as u64);

    // Abandoned event: bytes flushed past the watermark, then dropped.
    {
        let mut w = file.append().unwrap();
        w.write(&pattern(2 * PAGE_SIZE, 2)).unwrap(); // forces real flushes
        drop(w); // ERROR-path shape: commits nothing
    }
    assert_eq!(file.committed(), first.len() as u64);

    // A reader cannot see past the watermark.
    let mut r = file.open_read();
    let mut buf = vec![0u8; first.len() + 1];
    assert!(r.read_at(0, &mut buf).is_err(), "read past committed must fail closed");

    // The next event overwrites the torn tail; only its finish commits.
    let second = pattern(3000, 3);
    let mut w = file.append().unwrap();
    w.write(&second).unwrap();
    w.finish().unwrap();
    assert_eq!(file.committed(), (first.len() + second.len()) as u64);

    let mut r = file.open_read();
    let mut buf = vec![0u8; second.len()];
    r.read_at(first.len() as u64, &mut buf).unwrap();
    assert_eq!(buf, second);
}

#[test]
fn segment_splits_write_and_read() {
    let (set, _dir, _cwd) = super::rig("streams-segs");
    // Test geometry: one page per segment — every page boundary is a
    // segment boundary, extents straddle segments constantly.
    let seg = PAGE_SIZE as u64;
    let mut file =
        SpillFile::new_with_seg_bytes(Arc::clone(&set), "segs".to_string(), seg);

    let data = pattern(3 * PAGE_SIZE + 12345, 9);
    let mut w = file.append().unwrap();
    w.begin_extent();
    w.write(&data).unwrap();
    let ext = w.end_extent();
    w.finish().unwrap();
    assert_eq!(ext, SpillExtent { off: 0, len: data.len() as u64 });

    // Four segment files exist with the split sizes.
    let dir = find_fileset_dir();
    for (i, want) in [seg, seg, seg, 12345].iter().enumerate() {
        let md = std::fs::metadata(format!("{dir}/segs.{i}")).unwrap();
        assert_eq!(md.len(), *want, "segment {i}");
    }

    // Whole-extent read (crosses every boundary) + straddling window reads.
    let mut r = file.open_read();
    let mut buf = Vec::new();
    r.read_extent(ext, &mut buf).unwrap();
    assert_eq!(buf, data);
    for start in [seg - 7, 2 * seg - 1, 3 * seg - 100] {
        let mut win = vec![0u8; 200];
        r.read_at(start, &mut win).unwrap();
        assert_eq!(win, data[start as usize..start as usize + 200]);
    }
    r.close().unwrap();
}

#[test]
fn cross_thread_write_open_by_name_read() {
    let (set, _dir, _cwd) = super::rig("streams-xthread");

    // WRITER on its own thread with its OWN fd substrate (VFD cache is
    // thread-local; the file is created and written entirely over there).
    let wset = Arc::clone(&set);
    let (file, data, ext) = std::thread::spawn(move || {
        super::setup_thread();
        let mut file = SpillFile::new(wset, spill_file_name(2, 1, "xt", 5));
        let data = pattern(2 * PAGE_SIZE + 999, 4);
        let mut w = file.append().unwrap();
        w.begin_extent();
        w.write(&data).unwrap();
        let ext = w.end_extent();
        w.finish().unwrap();
        (file, data, ext)
    })
    .join()
    .expect("writer thread");

    // READER on this thread: open BY NAME through the shared set, chase
    // the descriptor's committed snapshot. (Frozen-before-read: the join
    // above is the test's stand-in for the deps-DAG edge.)
    assert_eq!(file.committed(), data.len() as u64);
    let mut r = file.open_read();
    let mut buf = Vec::new();
    r.read_extent(ext, &mut buf).unwrap();
    assert_eq!(buf, data);
    r.close().unwrap();
}

#[test]
fn writer_metrics_feed() {
    let (set, _dir, _cwd) = super::rig("streams-metrics");
    let mut file = SpillFile::new(Arc::clone(&set), "metrics".to_string());
    let mut w = file.append().unwrap();
    w.begin_extent();
    w.write(&pattern(PAGE_SIZE * 2 + 10, 5)).unwrap();
    let _ = w.end_extent();
    assert_eq!(w.extents_written(), 1);
    assert!(w.bytes_written() >= 2 * PAGE_SIZE as u64, "buffered tail not yet flushed");
    w.finish().unwrap();

    let mut r = file.open_read();
    let mut buf = vec![0u8; 100];
    r.read_at(0, &mut buf).unwrap();
    assert_eq!(r.bytes_read(), 100);
}

fn find_fileset_dir() -> String {
    for e in super::tmp_entries() {
        if e.ends_with(".fileset") {
            return format!("{}/{e}", super::TMP_DIR);
        }
    }
    panic!("no fileset dir under {}", super::TMP_DIR);
}
