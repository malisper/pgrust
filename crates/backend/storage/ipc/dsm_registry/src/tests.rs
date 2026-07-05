use super::*;

#[test]
fn name_and_size_validation_matches_c() {
    let e = GetNamedDSMSegment("", 8, None).unwrap_err();
    assert!(e.message().contains("DSM segment name cannot be empty"), "{e:?}");

    let long = "x".repeat(DSM_REGISTRY_NAME_LEN);
    let e = GetNamedDSMSegment(&long, 8, None).unwrap_err();
    assert!(e.message().contains("DSM segment name too long"), "{e:?}");

    let ok_len = "x".repeat(DSM_REGISTRY_NAME_LEN - 1);
    let e = GetNamedDSMSegment(&ok_len, 0, None).unwrap_err();
    assert!(e.message().contains("DSM segment size must be nonzero"), "{e:?}");
}

#[test]
fn shmem_size_matches_c_ctx_struct() {
    assert_eq!(DSMRegistryShmemSize(), 16);
}
