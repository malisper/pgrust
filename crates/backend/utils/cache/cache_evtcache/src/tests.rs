use super::*;
use std::sync::Mutex;

use ::mcx::{vec_with_capacity_in, PgString};

// The seams and the cache are process-/thread-global state, so tests that
// drive them run serially under one lock.
static TEST_LOCK: Mutex<()> = Mutex::new(());

thread_local! {
    /// Each command tag string `decode_text_array_to_strings` should hand back,
    /// keyed nowhere — the fixture for the array seam.
    static ARRAY_FIXTURE: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
    /// Each command-tag number `bms_add_member` was asked to add, for asserting
    /// the accumulation faithfully (the real Bitmapset words are private).
    static ADDED_TAGS: RefCell<Vec<i32>> = const { RefCell::new(Vec::new()) };
}

/// Install the array + bms seams exactly once (the seam registry is set-once
/// per process). The behaviour is fixture-driven through the thread-locals.
fn install_seams_once() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        array_seams::decode_text_array_to_strings::set(|mcx, _bytes| {
            ARRAY_FIXTURE.with(|f| {
                let f = f.borrow();
                let mut out = vec_with_capacity_in::<PgString<'_>>(mcx, f.len())?;
                for s in f.iter() {
                    out.push(PgString::from_str_in(s, mcx)?);
                }
                Ok(out)
            })
        });
        bms_seams::bms_add_member::set(|mcx, a, x| {
            ADDED_TAGS.with(|t| t.borrow_mut().push(x));
            match a {
                Some(b) => Ok(b),
                None => ::mcx::alloc_in(
                    mcx,
                    nodes::Bitmapset {
                        words: PgVec::new_in(mcx),
                    },
                ),
            }
        });
    });
}

#[test]
fn constants_match_c() {
    assert_eq!(TRIGGER_DISABLED, b'D' as i8);
    assert_eq!(EventTriggerRelationId, 3466);
    assert_eq!(EventTriggerNameIndexId, 3467);
    assert_eq!(Anum_pg_event_trigger_evtevent, 3);
    assert_eq!(Anum_pg_event_trigger_evtfoid, 5);
    assert_eq!(Anum_pg_event_trigger_evtenabled, 6);
    assert_eq!(Anum_pg_event_trigger_evttags, 7);
    assert_eq!(EVENTTRIGGEROID, 26);
}

#[test]
fn decode_accumulates_each_tag_via_bms_add_member() {
    let _g = TEST_LOCK.lock().unwrap();
    install_seams_once();
    ADDED_TAGS.with(|t| t.borrow_mut().clear());

    let ctx = MemoryContext::new("decode-test");
    // Three known command tags decode to their numbers via get_command_tag_enum.
    ARRAY_FIXTURE.with(|f| {
        *f.borrow_mut() = vec!["SELECT".into(), "INSERT".into(), "DELETE".into()]
    });
    let bms = DecodeTextArrayToBitmapset(ctx.mcx(), b"ignored-bytes").unwrap();
    assert!(bms.is_some());
    // SELECT=179, INSERT=158, DELETE=103 (cmdtaglist positions).
    ADDED_TAGS.with(|t| assert_eq!(*t.borrow(), vec![179, 158, 103]));

    // Empty array -> NULL bitmapset (C: bms stays NULL).
    ADDED_TAGS.with(|t| t.borrow_mut().clear());
    ARRAY_FIXTURE.with(|f| f.borrow_mut().clear());
    let empty = DecodeTextArrayToBitmapset(ctx.mcx(), b"ignored").unwrap();
    assert!(empty.is_none());
    ADDED_TAGS.with(|t| assert!(t.borrow().is_empty()));
}

#[test]
fn unknown_command_tag_decodes_to_unknown() {
    let _g = TEST_LOCK.lock().unwrap();
    install_seams_once();
    ADDED_TAGS.with(|t| t.borrow_mut().clear());

    let ctx = MemoryContext::new("decode-unknown");
    ARRAY_FIXTURE.with(|f| *f.borrow_mut() = vec!["NOT A COMMAND".into()]);
    let _ = DecodeTextArrayToBitmapset(ctx.mcx(), b"x").unwrap();
    // GetCommandTagEnum returns CMDTAG_UNKNOWN (0) for unrecognized names.
    ADDED_TAGS.with(|t| assert_eq!(*t.borrow(), vec![0]));
}

#[test]
fn invalidate_when_valid_clears_cache_and_marks_rebuild() {
    let _g = TEST_LOCK.lock().unwrap();
    // Install a non-NULL cache directly.
    let owned = McxOwned::<CacheTy>::try_new(MemoryContext::new("evt-test"), |mcx| {
        Ok(CacheState {
            mcx,
            cache: PgHashMap::new_in(mcx),
        })
    })
    .unwrap();
    EVENT_TRIGGER_CACHE.with(|cell| *cell.borrow_mut() = Some(owned));
    EVENT_TRIGGER_CACHE_STATE.with(|s| *s.borrow_mut() = Valid);

    InvalidateEventCacheCallback(datum::datum::Datum::null(), 0, 0);

    EVENT_TRIGGER_CACHE.with(|cell| assert!(cell.borrow().is_none()));
    EVENT_TRIGGER_CACHE_STATE.with(|s| assert_eq!(*s.borrow(), NeedsRebuild));
}

#[test]
fn invalidate_when_not_valid_keeps_cache_but_marks_rebuild() {
    let _g = TEST_LOCK.lock().unwrap();
    let owned = McxOwned::<CacheTy>::try_new(MemoryContext::new("evt-test2"), |mcx| {
        Ok(CacheState {
            mcx,
            cache: PgHashMap::new_in(mcx),
        })
    })
    .unwrap();
    EVENT_TRIGGER_CACHE.with(|cell| *cell.borrow_mut() = Some(owned));
    EVENT_TRIGGER_CACHE_STATE.with(|s| *s.borrow_mut() = RebuildStarted);

    InvalidateEventCacheCallback(datum::datum::Datum::null(), 0, 0);

    // Rebuild in progress: cache is NOT blown away...
    EVENT_TRIGGER_CACHE.with(|cell| assert!(cell.borrow().is_some()));
    // ...but it is marked for rebuild.
    EVENT_TRIGGER_CACHE_STATE.with(|s| assert_eq!(*s.borrow(), NeedsRebuild));
    // Clean up.
    EVENT_TRIGGER_CACHE.with(|cell| *cell.borrow_mut() = None);
}
