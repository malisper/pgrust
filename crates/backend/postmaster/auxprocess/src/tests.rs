#[test]
fn shutdown_callback_signature_matches_before_shmem_exit() {
    let f: fn(i32, datum::Datum) -> types_error::PgResult<()> = super::ShutdownAuxiliaryProcess;
    let _ = f;
}
