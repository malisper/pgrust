//! Tests for the pure (non-seamed) logic: processing-mode/backend-type
//! globals, the user-id / security-restriction state machine, the system-user
//! string, and `ClientConnectionInfo` serialization. Each test runs on its own
//! thread so the `thread_local!` backend state starts fresh.

use super::*;
use types_core::{uaMD5, uaSCRAM, uaTrust};

fn on_fresh_backend<R: Send + 'static>(f: impl FnOnce() -> R + Send + 'static) -> R {
    std::thread::spawn(f).join().unwrap()
}

#[test]
fn backend_type_descriptions_match_postgres() {
    assert_eq!(GetBackendTypeDesc(BackendType::Backend), "client backend");
    assert_eq!(GetBackendTypeDesc(BackendType::WalWriter), "walwriter");
    assert_eq!(GetBackendTypeDesc(BackendType::Invalid), "not initialized");
    assert_eq!(
        GetBackendTypeDesc(BackendType::DeadEndBackend),
        "dead-end client backend"
    );
}

#[test]
fn processing_mode_round_trip() {
    on_fresh_backend(|| {
        assert!(IsInitProcessingMode());
        assert!(!IsBootstrapProcessingMode());
        assert!(!IsNormalProcessingMode());

        SetProcessingMode(ProcessingMode::NormalProcessing);
        assert!(IsNormalProcessingMode());
        assert_eq!(GetProcessingMode(), ProcessingMode::NormalProcessing);
    });
}

#[test]
fn user_and_security_context_round_trip() {
    on_fresh_backend(|| {
        SetUserIdAndSecContext(42, SECURITY_LOCAL_USERID_CHANGE);
        assert_eq!(GetUserId(), 42);
        assert_eq!(GetUserIdAndSecContext(), (42, SECURITY_LOCAL_USERID_CHANGE));
        assert!(InLocalUserIdChange());
        assert!(!InSecurityRestrictedOperation());
        assert!(!InNoForceRLSOperation());
        assert_eq!(GetUserIdAndContext(), (42, true));
    });
}

#[test]
fn set_user_id_and_context_rejects_security_restricted_operation() {
    on_fresh_backend(|| {
        SetUserIdAndSecContext(42, SECURITY_RESTRICTED_OPERATION);
        let error = SetUserIdAndContext(43, false).unwrap_err();
        assert_eq!(error.level(), ERROR);
        assert_eq!(error.sqlstate(), ERRCODE_INSUFFICIENT_PRIVILEGE);
        assert_eq!(
            error.message(),
            "cannot set parameter \"role\" within security-restricted operation"
        );
    });
}

#[test]
fn system_user_built_as_method_colon_id() {
    on_fresh_backend(|| {
        assert_eq!(system_user(), None);
        InitializeSystemUser("alice", "scram-sha-256");
        assert_eq!(GetSystemUser(), Some("scram-sha-256:alice".to_owned()));
        assert_eq!(system_user(), Some("scram-sha-256:alice".to_owned()));
    });
}

#[test]
fn client_connection_info_serializes_and_restores() {
    on_fresh_backend(|| {
        set_client_connection_info(Some("alice".to_owned()), uaSCRAM);
        let mut buffer = vec![0u8; EstimateClientConnectionInfoSpace()];
        // Header (8) + "alice" (5) + NUL (1) = 14.
        assert_eq!(buffer.len(), 14);
        SerializeClientConnectionInfo(&mut buffer).unwrap();

        set_client_connection_info(Some("bob".to_owned()), uaMD5);
        RestoreClientConnectionInfo(&buffer).unwrap();
        let info = client_connection_info();
        assert_eq!(info.auth_method, uaSCRAM);
        assert_eq!(info.authn_id.as_deref(), Some("alice"));
    });
}

#[test]
fn client_connection_info_serializes_null_authn_id() {
    on_fresh_backend(|| {
        set_client_connection_info(None, uaTrust);
        let size = EstimateClientConnectionInfoSpace();
        assert_eq!(size, SERIALIZED_HEADER_LEN);
        let mut buffer = vec![0u8; size];
        SerializeClientConnectionInfo(&mut buffer).unwrap();

        set_client_connection_info(Some("x".to_owned()), uaMD5);
        RestoreClientConnectionInfo(&buffer).unwrap();
        let info = client_connection_info();
        assert_eq!(info.authn_id, None);
        assert_eq!(info.auth_method, uaTrust);
    });
}

#[test]
fn serialize_rejects_undersized_buffer() {
    on_fresh_backend(|| {
        set_client_connection_info(Some("alice".to_owned()), uaSCRAM);
        let mut buffer = [0u8; 4];
        assert!(SerializeClientConnectionInfo(&mut buffer).is_err());
    });
}

#[test]
fn restore_rejects_truncated_header() {
    on_fresh_backend(|| {
        let buffer = [0u8; 3];
        assert!(RestoreClientConnectionInfo(&buffer).is_err());
    });
}

#[test]
fn current_role_none_reverts_to_session_authorization() {
    // SetCurrentRoleId with SessionUserId unset is a no-op beyond clearing the
    // flag (does not touch the GUC seam), so this is testable without externals.
    on_fresh_backend(|| {
        SetCurrentRoleId(InvalidOid, false).unwrap();
        assert_eq!(GetCurrentRoleId(), InvalidOid);
    });
}
