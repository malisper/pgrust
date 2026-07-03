use super::*;
use mcx::MemoryContext;

fn tid(blk: BlockNumber, off: OffsetNumber) -> ItemPointerData {
    ItemPointerData::new(blk, off)
}

fn drain(tbm: &mut TIDBitmap<'_>) -> alloc::vec::Vec<(BlockNumber, bool, alloc::vec::Vec<OffsetNumber>)> {
    let mut iter = tbm.begin_private_iterate().unwrap();
    let mut out = alloc::vec::Vec::new();
    while let Some(res) = iter.next(tbm) {
        let mut offs = alloc::vec::Vec::new();
        if !res.lossy {
            let mut buf = [0 as OffsetNumber; TBM_MAX_TUPLES_PER_PAGE];
            let n = res.extract_page_tuples(&mut buf);
            assert!(n <= TBM_MAX_TUPLES_PER_PAGE);
            offs.extend_from_slice(&buf[..n]);
        }
        out.push((res.blockno, res.lossy, offs));
    }
    out
}

#[test]
fn empty_and_one_page() {
    let ctx = MemoryContext::new("t");
    let mut tbm = TIDBitmap::new(ctx.mcx(), 1024 * 1024);
    assert!(tbm.is_empty());
    tbm.add_tuples(&[tid(7, 3), tid(7, 1), tid(7, 291)], false).unwrap();
    assert!(!tbm.is_empty());
    let pages = drain(&mut tbm);
    assert_eq!(pages, alloc::vec![(7, false, alloc::vec![1, 3, 291])]);
}

#[test]
fn multi_page_sorted_iteration() {
    let ctx = MemoryContext::new("t");
    let mut tbm = TIDBitmap::new(ctx.mcx(), 1024 * 1024);
    tbm.add_tuples(&[tid(50, 2), tid(3, 9), tid(3, 10), tid(1000, 65)], true).unwrap();
    let pages = drain(&mut tbm);
    assert_eq!(
        pages,
        alloc::vec![
            (3, false, alloc::vec![9, 10]),
            (50, false, alloc::vec![2]),
            (1000, false, alloc::vec![65]),
        ]
    );
    // recheck flag propagates on exact pages
    let mut tbm2 = TIDBitmap::new(ctx.mcx(), 1024 * 1024);
    tbm2.add_tuples(&[tid(1, 1)], true).unwrap();
    let mut it = tbm2.begin_private_iterate().unwrap();
    assert!(it.next(&tbm2).unwrap().recheck);
}

#[test]
fn offset_out_of_range_errors() {
    let ctx = MemoryContext::new("t");
    let mut tbm = TIDBitmap::new(ctx.mcx(), 1024 * 1024);
    let err = tbm.add_tuples(&[tid(1, (TBM_MAX_TUPLES_PER_PAGE + 1) as OffsetNumber)], false);
    assert!(err.is_err());
}

#[test]
fn add_page_lossy_and_mixed_order() {
    let ctx = MemoryContext::new("t");
    let mut tbm = TIDBitmap::new(ctx.mcx(), 1024 * 1024);
    tbm.add_tuples(&[tid(700, 4)], false).unwrap();
    tbm.add_page(5).unwrap();
    tbm.add_page(300).unwrap();
    // tuples added to an already-lossy page vanish into the chunk bit
    tbm.add_tuples(&[tid(5, 9)], false).unwrap();
    let pages = drain(&mut tbm);
    assert_eq!(
        pages,
        alloc::vec![
            (5, true, alloc::vec![]),
            (300, true, alloc::vec![]),
            (700, false, alloc::vec![4]),
        ]
    );
}

#[test]
fn union_exact_and_lossy() {
    let ctx = MemoryContext::new("t");
    let mut a = TIDBitmap::new(ctx.mcx(), 1024 * 1024);
    let mut b = TIDBitmap::new(ctx.mcx(), 1024 * 1024);
    a.add_tuples(&[tid(1, 1), tid(2, 2)], false).unwrap();
    b.add_tuples(&[tid(2, 5), tid(9, 1)], true).unwrap();
    b.add_page(600).unwrap();
    a.union(&b).unwrap();
    let pages = drain(&mut a);
    assert_eq!(
        pages,
        alloc::vec![
            (1, false, alloc::vec![1]),
            (2, false, alloc::vec![2, 5]),
            (9, false, alloc::vec![1]),
            (600, true, alloc::vec![]),
        ]
    );
}

#[test]
fn intersect_exact() {
    let ctx = MemoryContext::new("t");
    let mut a = TIDBitmap::new(ctx.mcx(), 1024 * 1024);
    let mut b = TIDBitmap::new(ctx.mcx(), 1024 * 1024);
    a.add_tuples(&[tid(1, 1), tid(1, 2), tid(2, 3), tid(3, 4)], false).unwrap();
    b.add_tuples(&[tid(1, 2), tid(3, 4), tid(3, 5)], false).unwrap();
    a.intersect(&b);
    let pages = drain(&mut a);
    assert_eq!(pages, alloc::vec![(1, false, alloc::vec![2]), (3, false, alloc::vec![4])]);
}

#[test]
fn intersect_with_lossy_b_sets_recheck() {
    let ctx = MemoryContext::new("t");
    let mut a = TIDBitmap::new(ctx.mcx(), 1024 * 1024);
    let mut b = TIDBitmap::new(ctx.mcx(), 1024 * 1024);
    a.add_tuples(&[tid(5, 1), tid(6, 1)], false).unwrap();
    b.add_page(5).unwrap();
    a.intersect(&b);
    let mut iter = a.begin_private_iterate().unwrap();
    let res = iter.next(&a).unwrap();
    assert_eq!((res.blockno, res.lossy, res.recheck), (5, false, true));
    assert!(iter.next(&a).is_none());
}

#[test]
fn intersect_to_empty() {
    let ctx = MemoryContext::new("t");
    let mut a = TIDBitmap::new(ctx.mcx(), 1024 * 1024);
    let mut b = TIDBitmap::new(ctx.mcx(), 1024 * 1024);
    a.add_tuples(&[tid(1, 1)], false).unwrap();
    a.intersect(&b);
    assert!(a.is_empty());
    b.add_tuples(&[tid(2, 1)], false).unwrap();
    let mut c = TIDBitmap::new(ctx.mcx(), 1024 * 1024);
    c.add_tuples(&[tid(1, 1)], false).unwrap();
    c.intersect(&b);
    assert!(c.is_empty());
}

#[test]
fn lossify_under_memory_pressure() {
    let ctx = MemoryContext::new("t");
    // 16 entries max (sanity floor of tbm_calculate_entries)
    let mut tbm = TIDBitmap::new(ctx.mcx(), 1);
    assert_eq!(tbm_calculate_entries(1), 16);
    let mut expected = alloc::vec::Vec::new();
    for blk in 0..200u32 {
        tbm.add_tuples(&[tid(blk, 1)], false).unwrap();
        expected.push(blk);
    }
    let pages = drain(&mut tbm);
    // every input page still comes out exactly once, some lossy
    let mut seen: alloc::vec::Vec<BlockNumber> = pages.iter().map(|p| p.0).collect();
    seen.sort_unstable();
    assert_eq!(seen, expected);
    assert!(pages.iter().any(|p| p.1), "memory pressure should have lossified pages");
    for (blockno, lossy, offs) in pages {
        if !lossy {
            assert_eq!(offs, alloc::vec![1], "block {blockno}");
        }
    }
}

#[test]
fn chunk_header_page_roundtrip() {
    let ctx = MemoryContext::new("t");
    let mut tbm = TIDBitmap::new(ctx.mcx(), 1);
    // force chunk creation covering block 256..511, then add exact tuples on
    // the chunk-header block 256 (bit 0 of the chunk = the page itself)
    for blk in 256..300u32 {
        tbm.add_page(blk).unwrap();
    }
    tbm.add_tuples(&[tid(256, 3)], false).unwrap();
    let pages = drain(&mut tbm);
    assert_eq!(pages.len(), 44);
    assert!(pages.iter().all(|p| p.1));
    assert_eq!(pages[0].0, 256);
}

#[test]
fn tbm_iterator_wrapper() {
    let ctx = MemoryContext::new("t");
    let mut tbm = TIDBitmap::new(ctx.mcx(), 1024 * 1024);
    tbm.add_tuples(&[tid(4, 2)], false).unwrap();
    let mut it = TbmIterator::empty();
    assert!(it.exhausted());
    it = TbmIterator::private(tbm.begin_private_iterate().unwrap());
    assert!(!it.exhausted());
    assert_eq!(it.next(&tbm).unwrap().blockno, 4);
    assert!(it.next(&tbm).is_none());
    it.end_iterate();
    assert!(it.exhausted());
}
