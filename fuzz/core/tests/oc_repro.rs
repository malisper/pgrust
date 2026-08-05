// Regression: the text_left(n>=0) verified-text carve holds (smoke artifact
// crash-e204081f818843456f758f94708a149c14f48d83; see oraclefam_diff.rs
// header carves). Skipped input must stay skipped, not diverge.
#[test]
fn tleft_carve_regression() {
    decoder_fuzz::oraclefam_diff(&[9, 1, 3, 0, 0, 0, 97, 98, 195, 169, 232, 101]);
}
