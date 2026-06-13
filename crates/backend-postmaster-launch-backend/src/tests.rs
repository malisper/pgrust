//! Tests for the pure decision logic: the `child_process_kinds` table
//! (length, names, `shmem_attach` / NULL-`main_fn` columns) and the
//! `IsExternalConnectionBackend` macro. `postmaster_child_launch` itself is
//! not unit-tested: every call-out is an owner seam that loud-panics until
//! the owner lands, and the Main dispatch is `-> !`.

use super::*;

#[test]
fn child_kind_table_length_matches_backend_num_types() {
    assert_eq!(CHILD_PROCESS_KINDS.len(), BACKEND_NUM_TYPES);
    assert_eq!(BACKEND_NUM_TYPES, BackendType::Logger as usize + 1);
}

#[test]
fn postmaster_child_name_matches_table() {
    assert_eq!(postmaster_child_name(BackendType::Invalid), "invalid");
    assert_eq!(postmaster_child_name(BackendType::Backend), "backend");
    assert_eq!(postmaster_child_name(BackendType::DeadEndBackend), "dead-end backend");
    assert_eq!(postmaster_child_name(BackendType::AutoVacLauncher), "autovacuum launcher");
    assert_eq!(postmaster_child_name(BackendType::AutoVacWorker), "autovacuum worker");
    assert_eq!(postmaster_child_name(BackendType::BgWorker), "bgworker");
    assert_eq!(postmaster_child_name(BackendType::WalSender), "wal sender");
    assert_eq!(postmaster_child_name(BackendType::SlotSyncWorker), "slot sync worker");
    assert_eq!(postmaster_child_name(BackendType::StandaloneBackend), "standalone backend");
    assert_eq!(postmaster_child_name(BackendType::Archiver), "archiver");
    assert_eq!(postmaster_child_name(BackendType::BgWriter), "bgwriter");
    assert_eq!(postmaster_child_name(BackendType::Checkpointer), "checkpointer");
    assert_eq!(postmaster_child_name(BackendType::IoWorker), "io_worker");
    assert_eq!(postmaster_child_name(BackendType::Startup), "startup");
    assert_eq!(postmaster_child_name(BackendType::WalReceiver), "wal_receiver");
    assert_eq!(postmaster_child_name(BackendType::WalSummarizer), "wal_summarizer");
    assert_eq!(postmaster_child_name(BackendType::WalWriter), "wal_writer");
    assert_eq!(postmaster_child_name(BackendType::Logger), "syslogger");
}

#[test]
fn backend_type_all_is_in_discriminant_order() {
    for (i, ty) in BackendType::ALL.iter().enumerate() {
        assert_eq!(*ty as usize, i);
    }
}

#[test]
fn shmem_attach_matches_table() {
    // Only B_INVALID, B_STANDALONE_BACKEND and B_LOGGER have shmem_attach == false.
    for ty in BackendType::ALL {
        let expected = ty != BackendType::Invalid
            && ty != BackendType::StandaloneBackend
            && ty != BackendType::Logger;
        assert_eq!(
            CHILD_PROCESS_KINDS[ty as usize].shmem_attach, expected,
            "shmem_attach mismatch for backend type {ty:?}"
        );
    }
}

#[test]
fn null_main_fn_slots_match_c() {
    // B_INVALID, B_WAL_SENDER and B_STANDALONE_BACKEND have NULL main_fn.
    for ty in BackendType::ALL {
        let expected = ty != BackendType::Invalid
            && ty != BackendType::WalSender
            && ty != BackendType::StandaloneBackend;
        assert_eq!(
            CHILD_PROCESS_KINDS[ty as usize].main_fn.is_some(),
            expected,
            "main_fn presence mismatch for backend type {ty:?}"
        );
    }
}

#[test]
fn is_external_connection_backend_matches_macro() {
    for ty in BackendType::ALL {
        let expected = ty == BackendType::Backend || ty == BackendType::WalSender;
        assert_eq!(is_external_connection_backend(ty), expected);
    }
}
