#[test]
fn main_fn_matches_child_main_shape() {
    let f: fn(&types_startup::StartupData) -> ! = super::StartupProcessMain;
    let _ = f;
}

#[test]
fn promote_flag_roundtrip() {
    assert!(!super::IsPromoteSignaled());
    super::PROMOTE_SIGNALED.store(true, std::sync::atomic::Ordering::Relaxed);
    assert!(super::IsPromoteSignaled());
    super::ResetPromoteSignaled();
    assert!(!super::IsPromoteSignaled());
}
