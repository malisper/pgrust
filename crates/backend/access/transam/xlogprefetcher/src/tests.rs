use super::*;
use mcx::MemoryContext;

fn reader() -> XLogReaderState<'static> {
    XLogReaderState {
        ReadRecPtr: 0,
        EndRecPtr: 0,
        record: None,
        ..Default::default()
    }
}

fn prefetcher<'mcx, 'r, 'rdr>(
    mcx: Mcx<'mcx>,
    reader: &'r mut XLogReaderState<'rdr>,
) -> XLogPrefetcher<'mcx, 'r, 'rdr> {
    XLogPrefetcher {
        mcx,
        reader,
        record: None,
        next_block_id: 0,
        next_stats_shm_lsn: 0,
        filter_table: PgHashMap::new_in(mcx),
        filter_queue: PgVec::new_in(mcx),
        recent_rlocator: [RelFileLocator::default(); XLOGPREFETCHER_SEQ_WINDOW_SIZE],
        recent_block: [0; XLOGPREFETCHER_SEQ_WINDOW_SIZE],
        recent_idx: 0,
        no_readahead_until: 0,
        streaming_read: None,
        begin_ptr: 0,
        reconfigure_count: 0,
    }
}

#[test]
fn lrq_admission_and_completion() {
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();

    // max_distance 4, max_inflight 2 => size 5; admission stops at
    // inflight == max_inflight.
    let mut lrq = lrq_alloc(mcx, 4, 2).unwrap();
    let mut next_lsn = 0u64;
    lrq_prefetch(&mut lrq, |lsn| {
        next_lsn += 10;
        *lsn = next_lsn;
        Ok(LsnReadQueueNextStatus::Io)
    })
    .unwrap();
    assert_eq!(lrq.inflight, 2);
    assert_eq!(lrq.completed, 0);
    assert_eq!(lrq.head, 2);

    // Completing past the first IO's LSN releases one slot and (enabled)
    // refills it.
    let mut feed = vec![
        (LsnReadQueueNextStatus::NoIo, 40u64),
        (LsnReadQueueNextStatus::Io, 30u64),
    ];
    lrq_complete_lsn(&mut lrq, 15, true, |lsn| {
        let (status, l) = feed.pop().unwrap();
        *lsn = l;
        Ok(status)
    })
    .unwrap();
    assert_eq!(lrq.tail, 1);
    // One slot released, one IO refilled; admission then stops at
    // max_inflight with one feed entry unconsumed.
    assert_eq!(lrq.inflight, 2);
    assert_eq!(lrq.completed, 0);
    assert_eq!(feed.len(), 1);

    // AGAIN stops the loop immediately.
    lrq_complete_lsn(&mut lrq, 1000, true, |_| Ok(LsnReadQueueNextStatus::Again)).unwrap();
    assert_eq!(lrq.inflight, 0);
    assert_eq!(lrq.completed, 0);
    assert_eq!(lrq.tail, lrq.head);
}

#[test]
fn lrq_ring_wraps() {
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();

    // size 2 ring (max_distance 1): one usable slot, head/tail wrap.
    let mut lrq = lrq_alloc(mcx, 1, 1).unwrap();
    for round in 1..=5u64 {
        lrq_prefetch(&mut lrq, |lsn| {
            *lsn = round * 10;
            Ok(LsnReadQueueNextStatus::Io)
        })
        .unwrap();
        assert_eq!(lrq.inflight, 1);
        lrq_complete_lsn(&mut lrq, round * 10 + 1, false, |_| {
            unreachable!("disabled => no refill")
        })
        .unwrap();
        assert_eq!(lrq.inflight, 0);
        assert_eq!(lrq.head, lrq.tail);
    }
}

#[test]
fn filters_block_ranges_and_whole_database() {
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let mut rdr = reader();
    let mut p = prefetcher(mcx, &mut rdr);

    let rel = RelFileLocator::new(1663, 5, 1000);
    p.XLogPrefetcherAddFilter(rel, 10, 100).unwrap();

    assert!(p.XLogPrefetcherIsFiltered(rel, 10));
    assert!(p.XLogPrefetcherIsFiltered(rel, 11));
    assert!(!p.XLogPrefetcherIsFiltered(rel, 9));

    // Re-adding with a lower block extends the LSN and keeps the lower block.
    p.XLogPrefetcherAddFilter(rel, 3, 200).unwrap();
    assert!(p.XLogPrefetcherIsFiltered(rel, 5));

    // Whole-database filter: {InvalidOid, dbOid, InvalidRelFileNumber}.
    let db = RelFileLocator::new(InvalidOid, 5, InvalidRelFileNumber);
    p.XLogPrefetcherAddFilter(db, 0, 150).unwrap();
    let other_rel_same_db = RelFileLocator::new(1663, 5, 2000);
    assert!(p.XLogPrefetcherIsFiltered(other_rel_same_db, 0));
    let other_db = RelFileLocator::new(1663, 6, 2000);
    assert!(!p.XLogPrefetcherIsFiltered(other_db, 0));

    // CompleteFilters drains from the tail (least recently updated) and
    // stops at the first not-yet-replayed filter: the tail is `rel`
    // (updated at lsn 200), so nothing is dropped at 151 even though the
    // db filter's lsn (150) is already replayed past.
    p.XLogPrefetcherCompleteFilters(151);
    assert!(p.XLogPrefetcherIsFiltered(other_rel_same_db, 0));
    assert!(p.XLogPrefetcherIsFiltered(rel, 5));

    // filter_until_replayed >= replaying_lsn is kept (>= boundary).
    p.XLogPrefetcherCompleteFilters(200);
    assert!(p.XLogPrefetcherIsFiltered(rel, 5));
    // Past both LSNs everything drains.
    p.XLogPrefetcherCompleteFilters(201);
    assert!(!p.XLogPrefetcherIsFiltered(rel, 5));
    assert!(!p.XLogPrefetcherIsFiltered(other_rel_same_db, 0));
    assert!(p.filter_queue.is_empty());
    assert!(p.filter_table.is_empty());
}

#[test]
fn complete_filters_drain_order_is_least_recently_updated_first() {
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let mut rdr = reader();
    let mut p = prefetcher(mcx, &mut rdr);

    let a = RelFileLocator::new(1, 1, 1);
    let b = RelFileLocator::new(1, 1, 2);
    p.XLogPrefetcherAddFilter(a, 0, 100).unwrap();
    p.XLogPrefetcherAddFilter(b, 0, 300).unwrap();
    // Touch `a` again: it moves to the head, leaving `b` at the tail even
    // though b's LSN is older than a's new one.
    p.XLogPrefetcherAddFilter(a, 0, 500).unwrap();
    assert_eq!(*p.filter_queue.first().unwrap(), a);
    assert_eq!(*p.filter_queue.last().unwrap(), b);

    // Replaying past b's LSN drops b; a stays (the drain stops there).
    p.XLogPrefetcherCompleteFilters(301);
    assert!(!p.XLogPrefetcherIsFiltered(b, 0));
    assert!(p.XLogPrefetcherIsFiltered(a, 0));
}

#[test]
fn reconfigure_bumps_count_and_begin_read_resets() {
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let mut rdr = reader();
    let mut p = prefetcher(mcx, &mut rdr);
    p.no_readahead_until = 42;

    let before = XLOG_PREFETCH_RECONFIGURE_COUNT.with(Cell::get);
    XLogPrefetchReconfigure();
    assert_eq!(XLOG_PREFETCH_RECONFIGURE_COUNT.with(Cell::get), before + 1);

    // BeginRead needs the xlog_begin_read seam; install a no-op once.
    static INSTALL: std::sync::Once = std::sync::Once::new();
    INSTALL.call_once(|| xlogreader::xlog_begin_read::set(|_, _| {}));

    let rc = p.reconfigure_count;
    p.XLogPrefetcherBeginRead(7);
    assert_eq!(p.reconfigure_count, rc - 1);
    assert_eq!(p.begin_ptr, 7);
    assert_eq!(p.no_readahead_until, 0);
}

#[test]
fn recovery_prefetch_guc_constants() {
    assert_eq!(RECOVERY_PREFETCH_OFF, 0);
    assert_eq!(RECOVERY_PREFETCH_ON, 1);
    assert_eq!(RECOVERY_PREFETCH_TRY, 2);
    assert_eq!(recovery_prefetch(), RECOVERY_PREFETCH_TRY);
}
