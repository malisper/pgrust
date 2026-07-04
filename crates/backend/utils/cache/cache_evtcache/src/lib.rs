#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use core::cell::{Cell, RefCell};

use datum::Datum;
use mcx::{Mcx, PgVec};
use types_core::{AttrNumber, Oid, TEXTOID};
use types_error::PgResult;
use types_rel::AccessShareLock;

pub const EVENT_TRIGGER_RELATION_ID: Oid = 3466;
pub const EVENT_TRIGGER_NAME_INDEX_ID: Oid = 3467;
pub const EVENT_TRIGGER_OID_INDEX_ID: Oid = 3468;

pub const Anum_pg_event_trigger_oid: AttrNumber = 1;
pub const Anum_pg_event_trigger_evtname: AttrNumber = 2;
pub const Anum_pg_event_trigger_evtevent: AttrNumber = 3;
pub const Anum_pg_event_trigger_evtowner: AttrNumber = 4;
pub const Anum_pg_event_trigger_evtfoid: AttrNumber = 5;
pub const Anum_pg_event_trigger_evtenabled: AttrNumber = 6;
pub const Anum_pg_event_trigger_evttags: AttrNumber = 7;
pub const Natts_pg_event_trigger: usize = 7;

const TRIGGER_DISABLED: i8 = b'D' as i8;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
pub enum EventTriggerEvent {
    DdlCommandStart = 0,
    DdlCommandEnd = 1,
    SqlDrop = 2,
    TableRewrite = 3,
    Login = 4,
}
const NEVENTS: usize = 5;

// CommandTag values are dense indexes < 193 (cmdtag::TAG_BEHAVIOR).
#[derive(Clone, Copy, Default)]
pub struct TagSet([u64; 4]);

impl TagSet {
    pub fn add(&mut self, tag: i32) {
        debug_assert!((0..256).contains(&tag), "CommandTag {tag} out of TagSet range");
        self.0[(tag as usize) / 64] |= 1u64 << (tag as usize % 64);
    }
    pub fn is_member(&self, tag: i32) -> bool {
        (0..256).contains(&tag) && self.0[(tag as usize) / 64] & (1u64 << (tag as usize % 64)) != 0
    }
}

#[derive(Clone, Copy)]
pub struct EventTriggerCacheItem {
    pub fnoid: Oid,
    pub enabled: i8,
    pub tagset: Option<TagSet>,
}

const _: () = assert!(!core::mem::needs_drop::<EventTriggerCacheItem>());

#[derive(Clone, Copy, PartialEq, Eq)]
enum CacheState {
    NeedsRebuild,
    RebuildStarted,
    Valid,
}

thread_local! {
    static STATE: Cell<CacheState> = const { Cell::new(CacheState::NeedsRebuild) };
    static CALLBACK_REGISTERED: Cell<bool> = const { Cell::new(false) };
    // Backend-lifetime cache (C's EventTriggerCacheContext hash); Copy items,
    // no drop glue beyond the Vec headers.
    static CACHE: RefCell<[Vec<EventTriggerCacheItem>; NEVENTS]> =
        const { RefCell::new([Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new()]) };
}

pub fn EventCacheLookup<'mcx>(
    mcx: Mcx<'mcx>,
    event: EventTriggerEvent,
) -> PgResult<PgVec<'mcx, EventTriggerCacheItem>> {
    if STATE.with(|s| s.get()) != CacheState::Valid {
        BuildEventTriggerCache(mcx)?;
    }
    CACHE.with(|c| {
        let cache = c.borrow();
        let list = &cache[event as usize];
        let mut out: PgVec<'mcx, EventTriggerCacheItem> = PgVec::new_in(mcx);
        out.try_reserve_exact(list.len()).map_err(|_| mcx.oom(list.len()))?;
        for item in list.iter() {
            out.push(*item);
        }
        Ok(out)
    })
}

fn BuildEventTriggerCache(mcx: Mcx<'_>) -> PgResult<()> {
    if !CALLBACK_REGISTERED.with(|c| c.get()) {
        inval::invalidate::CacheRegisterSyscacheCallback(
            cache_syscache::cacheinfo::EVENTTRIGGEROID,
            InvalidateEventCacheCallback,
            Datum::null(),
        )?;
        CALLBACK_REGISTERED.with(|c| c.set(true));
    }

    // Prevent an invalidation arriving mid-scan from marking the cache we are
    // about to install as valid (C's ETCS_REBUILD_STARTED dance).
    STATE.with(|s| s.set(CacheState::RebuildStarted));

    let mut fresh: [Vec<EventTriggerCacheItem>; NEVENTS] = Default::default();

    let scratch = mcx::MemoryContext::new("EventTriggerCache build");
    let smcx = scratch.mcx();
    let rel = table::table_open(smcx, EVENT_TRIGGER_RELATION_ID, AccessShareLock)?;
    let irel = indexam::index_open(smcx, EVENT_TRIGGER_NAME_INDEX_ID, AccessShareLock)?;
    let mut scan = genam::systable_beginscan_ordered(smcx, &rel, &irel, None, &[])?;
    loop {
        let Some(tup) = genam::systable_getnext_ordered(
            smcx,
            &mut scan,
            types_scan::sdir::ScanDirection::ForwardScanDirection,
        )?
        else {
            break;
        };
        let descr = rel.descr();
        let mut isnull = false;
        // SAFETY (each): fixed NOT NULL pg_event_trigger columns of the
        // declared types (pg_event_trigger.h).
        let get = |attno: AttrNumber, isnull: &mut bool| unsafe {
            types_tuple::heap_getattr(tup, attno as i32, descr, isnull)
        };
        let enabled = get(Anum_pg_event_trigger_evtenabled, &mut isnull).as_i8();
        if enabled == TRIGGER_DISABLED {
            continue;
        }
        let evtevent_ptr = get(Anum_pg_event_trigger_evtevent, &mut isnull).as_usize() as *const u8;
        // SAFETY: name-column datum points at NAMEDATALEN bytes in the tuple.
        let evtevent = unsafe { name_str(evtevent_ptr) };
        let event = match evtevent {
            "ddl_command_start" => EventTriggerEvent::DdlCommandStart,
            "ddl_command_end" => EventTriggerEvent::DdlCommandEnd,
            "sql_drop" => EventTriggerEvent::SqlDrop,
            "table_rewrite" => EventTriggerEvent::TableRewrite,
            "login" => EventTriggerEvent::Login,
            _ => continue,
        };
        let fnoid = get(Anum_pg_event_trigger_evtfoid, &mut isnull).as_oid();
        let evttags = get(Anum_pg_event_trigger_evttags, &mut isnull);
        let tagset =
            if isnull { None } else { Some(DecodeTextArrayToTagSet(smcx, evttags)?) };
        fresh[event as usize].push(EventTriggerCacheItem { fnoid, enabled, tagset });
    }
    genam::systable_endscan_ordered(smcx, scan)?;
    irel.close(AccessShareLock)?;
    rel.close(AccessShareLock)?;
    let _ = mcx;

    CACHE.with(|c| *c.borrow_mut() = fresh);
    STATE.with(|s| {
        if s.get() == CacheState::RebuildStarted {
            s.set(CacheState::Valid);
        }
    });
    Ok(())
}

fn DecodeTextArrayToTagSet(mcx: Mcx<'_>, array: Datum) -> PgResult<TagSet> {
    let img = varlena_bytes(mcx, array)?;
    let (elems, nulls) = arrayfuncs::deconstruct_array_builtin(mcx, &img, TEXTOID, false)?;
    debug_assert!(nulls.iter().all(|n| !n));
    let mut set = TagSet::default();
    for &e in elems.iter() {
        let text = varlena_bytes(mcx, e)?;
        set.add(cmdtag::GetCommandTagEnum(&text[4..]).0);
    }
    Ok(set)
}

fn varlena_bytes<'mcx>(mcx: Mcx<'mcx>, d: Datum) -> PgResult<PgVec<'mcx, u8>> {
    let p = d.as_usize() as *const u8;
    // SAFETY: varlena image spans its header-declared size.
    let src = unsafe {
        let b0 = *p;
        let len = if b0 == 0x01 {
            2 + types_tuple::varatt::vartag_size(*p.add(1))
        } else if b0 & 0x01 != 0 {
            (b0 as usize >> 1) & 0x7F
        } else {
            (u32::from_ne_bytes(*(p as *const [u8; 4])) >> 2) as usize
        };
        core::slice::from_raw_parts(p, len)
    };
    detoast::detoast_attr(mcx, src)
}

// SAFETY: caller guarantees `p` points at a NAMEDATALEN name column.
unsafe fn name_str<'a>(p: *const u8) -> &'a str {
    let bytes = unsafe { core::slice::from_raw_parts(p, types_core::NAMEDATALEN as usize) };
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    core::str::from_utf8(&bytes[..end]).unwrap_or("")
}

fn InvalidateEventCacheCallback(_arg: Datum, _cacheid: i32, _hashvalue: u32) {
    if STATE.with(|s| s.get()) == CacheState::Valid {
        CACHE.with(|c| {
            for v in c.borrow_mut().iter_mut() {
                v.clear();
            }
        });
    }
    STATE.with(|s| s.set(CacheState::NeedsRebuild));
}
