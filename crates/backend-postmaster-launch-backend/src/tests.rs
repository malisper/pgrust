//! Tests for the pure decision logic: the `child_process_kinds` table
//! (length, names, `shmem_attach` / NULL-`main_fn` columns) and the
//! `IsExternalConnectionBackend` macro. `postmaster_child_launch` itself is
//! not unit-tested: every call-out is an owner seam that loud-panics until
//! the owner lands, and the Main dispatch is `-> !`.

use super::*;
use types_core::init::{
    B_ARCHIVER, B_AUTOVAC_LAUNCHER, B_AUTOVAC_WORKER, B_BG_WORKER, B_BG_WRITER, B_CHECKPOINTER,
    B_DEAD_END_BACKEND, B_INVALID, B_IO_WORKER, B_SLOTSYNC_WORKER, B_STANDALONE_BACKEND,
    B_STARTUP, B_WAL_RECEIVER, B_WAL_SUMMARIZER, B_WAL_WRITER,
};

#[test]
fn child_kind_table_length_matches_backend_num_types() {
    assert_eq!(CHILD_PROCESS_KINDS.len(), BACKEND_NUM_TYPES);
    assert_eq!(BACKEND_NUM_TYPES, (B_LOGGER + 1) as usize);
}

#[test]
fn postmaster_child_name_matches_table() {
    assert_eq!(postmaster_child_name(B_INVALID), "invalid");
    assert_eq!(postmaster_child_name(B_BACKEND), "backend");
    assert_eq!(postmaster_child_name(B_DEAD_END_BACKEND), "dead-end backend");
    assert_eq!(postmaster_child_name(B_AUTOVAC_LAUNCHER), "autovacuum launcher");
    assert_eq!(postmaster_child_name(B_AUTOVAC_WORKER), "autovacuum worker");
    assert_eq!(postmaster_child_name(B_BG_WORKER), "bgworker");
    assert_eq!(postmaster_child_name(B_WAL_SENDER), "wal sender");
    assert_eq!(postmaster_child_name(B_SLOTSYNC_WORKER), "slot sync worker");
    assert_eq!(postmaster_child_name(B_STANDALONE_BACKEND), "standalone backend");
    assert_eq!(postmaster_child_name(B_ARCHIVER), "archiver");
    assert_eq!(postmaster_child_name(B_BG_WRITER), "bgwriter");
    assert_eq!(postmaster_child_name(B_CHECKPOINTER), "checkpointer");
    assert_eq!(postmaster_child_name(B_IO_WORKER), "io_worker");
    assert_eq!(postmaster_child_name(B_STARTUP), "startup");
    assert_eq!(postmaster_child_name(B_WAL_RECEIVER), "wal_receiver");
    assert_eq!(postmaster_child_name(B_WAL_SUMMARIZER), "wal_summarizer");
    assert_eq!(postmaster_child_name(B_WAL_WRITER), "wal_writer");
    assert_eq!(postmaster_child_name(B_LOGGER), "syslogger");
}

#[test]
fn shmem_attach_matches_table() {
    // Only B_INVALID, B_STANDALONE_BACKEND and B_LOGGER have shmem_attach == false.
    for ty in 0..=B_LOGGER {
        let expected = ty != B_INVALID && ty != B_STANDALONE_BACKEND && ty != B_LOGGER;
        assert_eq!(
            CHILD_PROCESS_KINDS[ty as usize].shmem_attach, expected,
            "shmem_attach mismatch for backend type {ty}"
        );
    }
}

#[test]
fn null_main_fn_slots_match_c() {
    // B_INVALID, B_WAL_SENDER and B_STANDALONE_BACKEND have NULL main_fn.
    for ty in 0..=B_LOGGER {
        let expected = ty != B_INVALID && ty != B_WAL_SENDER && ty != B_STANDALONE_BACKEND;
        assert_eq!(
            CHILD_PROCESS_KINDS[ty as usize].main_fn.is_some(),
            expected,
            "main_fn presence mismatch for backend type {ty}"
        );
    }
}

#[test]
fn is_external_connection_backend_matches_macro() {
    for ty in 0..=B_LOGGER {
        let expected = ty == B_BACKEND || ty == B_WAL_SENDER;
        assert_eq!(is_external_connection_backend(ty), expected);
    }
}
