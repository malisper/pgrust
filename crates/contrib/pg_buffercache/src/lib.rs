//! `contrib/pg_buffercache/pg_buffercache_pages.c` — the shared-buffer-cache
//! inspection view plus the summary/usage-count rollups and the PG17+
//! superuser eviction functions.
//!
//! Locking discipline mirrors C exactly: `pg_buffercache_pages` snapshots each
//! buffer header under its own header lock (self-consistent per buffer, no
//! global lock, no cross-buffer snapshot), while `summary`/`usage_counts` read
//! the state word with a plain atomic load and no lock at all (C's comment:
//! locking would not improve the result and noticeably raises the cost). Both
//! shapes hold no lock for longer than one header inspection, so the threaded
//! server never stalls behind the scan.
//!
//! `pg_buffercache_numa_pages` errors like C on a NUMA-less build; core
//! `pg_numa_available()` returns false here, so the C regress corpus takes the
//! same skip path as C on macOS.

#![allow(non_snake_case)]

use core::sync::atomic::Ordering;

use datum::Datum;
use types_core::{InvalidBlockNumber, Oid, RELPERSISTENCE_TEMP};
use types_error::{
    PgError, PgResult, ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INVALID_PARAMETER_VALUE,
};
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};
use types_storage::buf::{
    BM_DIRTY, BM_MAX_USAGE_COUNT, BM_TAG_VALID, BM_VALID, BUF_REFCOUNT_MASK, BUF_USAGECOUNT_MASK,
};

const LIBRARY: &str = "pg_buffercache";

const NUM_BUFFERCACHE_PAGES_MIN_ELEM: i32 = 8;
const NUM_BUFFERCACHE_PAGES_ELEM: i32 = 9;

// BUF_STATE_GET_USAGECOUNT / BUF_STATE_GET_REFCOUNT (buf_internals.h) over
// this port's public bit layout (types_storage::buf; bufmgr's own copies are
// pub(crate)).
#[inline]
fn state_usagecount(state: u32) -> u32 {
    (state & BUF_USAGECOUNT_MASK) >> 18
}

#[inline]
fn state_refcount(state: u32) -> u32 {
    state & BUF_REFCOUNT_MASK
}

fn resolved_flinfo<'a>(flinfo: Option<&'a mut FmgrInfo>, what: &str) -> &'a mut FmgrInfo {
    flinfo.unwrap_or_else(|| panic!("{what}: resolved FmgrInfo required"))
}

/// `pg_buffercache_superuser_check`.
fn superuser_check(func_name: &str) -> PgResult<()> {
    if !superuser::superuser()? {
        return Err(Box::new(
            PgError::error(format!("must be superuser to use {func_name}()"))
                .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE),
        ));
    }
    Ok(())
}

fn composite_result(
    fcinfo: &Fcinfo,
    flinfo: &mut FmgrInfo,
    values: &[Datum],
    nulls: &[bool],
) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let resolved = funcapi::get_call_result_type(mcx, flinfo, None)?;
    if resolved.class != funcapi::TypeFuncClass::Composite {
        return Err(Box::new(PgError::error("return type must be a row type")));
    }
    let tupdesc = resolved.result_tuple_desc.expect("composite result carries a tupdesc");
    let tup = heaptuple::heap_form_tuple(mcx, &tupdesc, values, nulls)?;
    let d = Datum::from_usize(tup.header_ptr() as usize);
    core::mem::forget(tup);
    Ok(d)
}

/// `pg_buffercache_pages`: one row per shared buffer. Each header is
/// inspected under its own header lock, exactly C's discipline (no partition
/// locks, so no cross-buffer consistency; each row is self-consistent).
fn fc_pg_buffercache_pages(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = resolved_flinfo(flinfo, "pg_buffercache_pages");

    // SAFETY: executor arms es_query_cxt pre-call; it outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut srf = funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;

    // The v1.0-compat natts window: 8 columns (no pinning_backends) or 9.
    let natts = srf.tupdesc.natts;
    if !(NUM_BUFFERCACHE_PAGES_MIN_ELEM..=NUM_BUFFERCACHE_PAGES_ELEM).contains(&natts) {
        return Err(Box::new(PgError::error("incorrect number of output arguments")));
    }

    for id in 0..bufmgr::NBuffersInited() {
        postgres_seams::check_for_interrupts::call()?;

        let desc = bufmgr::GetBufferDescriptor(id);
        // Lock each buffer header before inspecting.
        let state = bufmgr::LockBufHdr(desc);
        let tag = desc.tag();
        let bufferid = bufmgr::BufferDescriptorGetBuffer(desc);
        bufmgr::UnlockBufHdr(desc, state);

        let isvalid = state & BM_VALID != 0 && state & BM_TAG_VALID != 0;

        let mut values = [Datum::null(); NUM_BUFFERCACHE_PAGES_ELEM as usize];
        let mut nulls = [true; NUM_BUFFERCACHE_PAGES_ELEM as usize];
        values[0] = Datum::from_i32(bufferid);
        nulls[0] = false;

        // All fields except bufferid are null if the buffer is unused or not
        // valid.
        if tag.blockNum != InvalidBlockNumber && isvalid {
            values[1] = Datum::from_oid(tag.relNumber);
            values[2] = Datum::from_oid(tag.spcOid);
            values[3] = Datum::from_oid(tag.dbOid);
            values[4] = Datum::from_i16(tag.forkNum as i16);
            values[5] = Datum::from_i64(i64::from(tag.blockNum));
            values[6] = Datum::from_bool(state & BM_DIRTY != 0);
            values[7] = Datum::from_i16(state_usagecount(state) as i16);
            values[8] = Datum::from_i32(state_refcount(state) as i32);
            for n in nulls.iter_mut().take(natts as usize).skip(1) {
                *n = false;
            }
        }

        srf.putvalues(&values[..natts as usize], &nulls[..natts as usize])?;
    }

    Ok(srf.finish(fcinfo))
}

/// `pg_buffercache_numa_pages`: this build has no libnuma; C raises the same
/// error when `pg_numa_init()` fails.
fn fc_pg_buffercache_numa_pages(
    _flinfo: Option<&mut FmgrInfo>,
    _fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    Err(Box::new(PgError::error(
        "libnuma initialization failed or NUMA is not supported on this platform",
    )))
}

/// `pg_buffercache_summary`: unlocked state reads, C's exact rationale — the
/// state can change the instant the lock drops, so locking buys nothing and
/// costs a lot on a big pool.
fn fc_pg_buffercache_summary(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = resolved_flinfo(flinfo, "pg_buffercache_summary");

    let mut buffers_used: i32 = 0;
    let mut buffers_unused: i32 = 0;
    let mut buffers_dirty: i32 = 0;
    let mut buffers_pinned: i32 = 0;
    let mut usagecount_total: i64 = 0;

    for id in 0..bufmgr::NBuffersInited() {
        postgres_seams::check_for_interrupts::call()?;

        let desc = bufmgr::GetBufferDescriptor(id);
        let state = desc.state.load(Ordering::Relaxed);

        if state & BM_VALID != 0 {
            buffers_used += 1;
            usagecount_total += i64::from(state_usagecount(state));
            if state & BM_DIRTY != 0 {
                buffers_dirty += 1;
            }
        } else {
            buffers_unused += 1;
        }

        if state_refcount(state) > 0 {
            buffers_pinned += 1;
        }
    }

    let mut values = [
        Datum::from_i32(buffers_used),
        Datum::from_i32(buffers_unused),
        Datum::from_i32(buffers_dirty),
        Datum::from_i32(buffers_pinned),
        Datum::null(),
    ];
    let mut nulls = [false, false, false, false, true];
    if buffers_used != 0 {
        values[4] = Datum::from_f64(usagecount_total as f64 / f64::from(buffers_used));
        nulls[4] = false;
    }

    composite_result(fcinfo, flinfo, &values, &nulls)
}

/// `pg_buffercache_usage_counts`: histogram over usage counts 0..=5,
/// unlocked state reads like summary.
fn fc_pg_buffercache_usage_counts(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = resolved_flinfo(flinfo, "pg_buffercache_usage_counts");

    const NSLOT: usize = BM_MAX_USAGE_COUNT as usize + 1;
    let mut usage_counts = [0i32; NSLOT];
    let mut dirty = [0i32; NSLOT];
    let mut pinned = [0i32; NSLOT];

    // SAFETY: executor arms es_query_cxt pre-call; it outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut srf = funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;

    for id in 0..bufmgr::NBuffersInited() {
        postgres_seams::check_for_interrupts::call()?;

        let desc = bufmgr::GetBufferDescriptor(id);
        let state = desc.state.load(Ordering::Relaxed);
        let usage_count = state_usagecount(state) as usize;

        usage_counts[usage_count] += 1;
        if state & BM_DIRTY != 0 {
            dirty[usage_count] += 1;
        }
        if state_refcount(state) > 0 {
            pinned[usage_count] += 1;
        }
    }

    for i in 0..NSLOT {
        let values = [
            Datum::from_i32(i as i32),
            Datum::from_i32(usage_counts[i]),
            Datum::from_i32(dirty[i]),
            Datum::from_i32(pinned[i]),
        ];
        srf.putvalues(&values, &[false; 4])?;
    }

    Ok(srf.finish(fcinfo))
}

/// `pg_buffercache_evict` (STRICT in SQL; fmgr handles the NULL arg).
fn fc_pg_buffercache_evict(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = resolved_flinfo(flinfo, "pg_buffercache_evict");
    superuser_check("pg_buffercache_evict")?;

    let buf = fcinfo.arg_i32(0);
    if buf < 1 || buf > bufmgr::NBuffersInited() {
        return Err(Box::new(PgError::error(format!("bad buffer ID: {buf}"))));
    }

    let (evicted, flushed) = bufmgr::EvictUnpinnedBuffer(buf)?;
    composite_result(
        fcinfo,
        flinfo,
        &[Datum::from_bool(evicted), Datum::from_bool(flushed)],
        &[false, false],
    )
}

/// `pg_buffercache_evict_relation` (STRICT; regclass argument).
fn fc_pg_buffercache_evict_relation(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = resolved_flinfo(flinfo, "pg_buffercache_evict_relation");
    superuser_check("pg_buffercache_evict_relation")?;

    let rel_oid: Oid = fcinfo.arg_oid(0);
    let mcx = fcinfo.result_mcx();
    let rel = relation::relation_open(mcx, rel_oid, types_rel::AccessShareLock)?;

    // RelationUsesLocalBuffers(rel).
    if rel.rd_rel.relpersistence == RELPERSISTENCE_TEMP {
        return Err(Box::new(
            PgError::error(
                "relation uses local buffers, pg_buffercache_evict_relation() is \
                 intended to be used for shared buffers only",
            )
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        ));
    }

    let locator = rel.rd_locator.get();
    let counts = bufmgr::EvictRelUnpinnedBuffers(&locator)?;

    rel.close(types_rel::AccessShareLock)?;

    composite_result(
        fcinfo,
        flinfo,
        &[
            Datum::from_i32(counts.evicted),
            Datum::from_i32(counts.flushed),
            Datum::from_i32(counts.skipped),
        ],
        &[false, false, false],
    )
}

/// `pg_buffercache_evict_all`.
fn fc_pg_buffercache_evict_all(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = resolved_flinfo(flinfo, "pg_buffercache_evict_all");
    superuser_check("pg_buffercache_evict_all")?;

    let counts = bufmgr::EvictAllUnpinnedBuffers()?;
    composite_result(
        fcinfo,
        flinfo,
        &[
            Datum::from_i32(counts.evicted),
            Datum::from_i32(counts.flushed),
            Datum::from_i32(counts.skipped),
        ],
        &[false, false, false],
    )
}

fn lookup(function: &str) -> Option<PGFunction> {
    Some(match function {
        "pg_buffercache_pages" => fc_pg_buffercache_pages,
        "pg_buffercache_numa_pages" => fc_pg_buffercache_numa_pages,
        "pg_buffercache_summary" => fc_pg_buffercache_summary,
        "pg_buffercache_usage_counts" => fc_pg_buffercache_usage_counts,
        "pg_buffercache_evict" => fc_pg_buffercache_evict,
        "pg_buffercache_evict_relation" => fc_pg_buffercache_evict_relation,
        "pg_buffercache_evict_all" => fc_pg_buffercache_evict_all,
        _ => return None,
    })
}

pub fn init_seams() {
    dfmgr::register_builtin_library(dfmgr::BuiltinLibraryEntry {
        name: LIBRARY,
        lookup,
        // pg_buffercache_pages.c's PG_MODULE_MAGIC_EXT has no _PG_init.
        pg_init: None,
    });
}
