//! Shared on-disk md harness for the audit-18.6 b095 witnesses (the
//! `smgr_io.rs` setup, parameterised by scratch dir). Every witness lives in
//! its own test binary: the harness moves the process cwd and the md size
//! cache / io_direct flag are process-global.
#![allow(dead_code)]

use std::sync::atomic::{AtomicU32, Ordering};

use types_core::primitive::ForkNumber;

pub static SYNC_REQUESTS: AtomicU32 = AtomicU32::new(0);

fn fork_suffix(forknum: ForkNumber) -> &'static str {
    match forknum {
        ForkNumber::MAIN_FORKNUM => "",
        ForkNumber::FSM_FORKNUM => "_fsm",
        ForkNumber::VISIBILITYMAP_FORKNUM => "_vm",
        ForkNumber::INIT_FORKNUM => "_init",
        ForkNumber::InvalidForkNumber => panic!("invalid fork"),
    }
}

/// Installs the seams and moves the process into a fresh scratch datadir
/// (returned so the caller can remove it).
pub fn setup(tag: &str) -> std::path::PathBuf {
    guc_tables::init_seams();
    elog::init_seams();
    fd::init_seams();
    smgr::init_seams();

    xact_seams::get_current_sub_transaction_id::set(|| 1);
    aio_seams::pgaio_closing_fd::set(|_| {});
    aio_seams::pgaio_io_start_readv::set(|_, _, _| Ok(()));
    waitevent_seams::pgstat_report_wait_start::set(|_| {});
    waitevent_seams::pgstat_report_wait_end::set(|| {});
    pgstat_seams::pgstat_report_tempfile::set(|_| {});

    relpath_seams::relpathbackend::set(|rlocator, _backend, forknum| {
        format!(
            "base/{}/{}{}",
            rlocator.dbOid,
            rlocator.relNumber,
            fork_suffix(forknum)
        )
    });
    sync_seams::register_sync_request::set(|_tag, _ty, _retry| {
        SYNC_REQUESTS.fetch_add(1, Ordering::Relaxed);
        Ok(true)
    });
    tablespace_seams::tablespace_create_dbspace::set(|_, _, _| Ok(()));

    let dir = std::env::temp_dir().join(format!("pgrust_smgr_{tag}_{}", std::process::id()));
    std::fs::create_dir_all(dir.join("base/5")).unwrap();
    std::env::set_current_dir(&dir).unwrap();
    fd::InitFileAccess();
    dir
}
