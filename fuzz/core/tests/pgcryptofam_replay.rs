//! CI regression rail for pgcryptofam_diff (task #145, the crypt lossy-image
//! mask deletion): every committed corpus input replays through the driver on
//! `cargo test` — stable rail, no nightly/libFuzzer needed (same pattern as
//! ltree_replay.rs / tupaccess_replay.rs). Landed together with the removal
//! of `crypt_value_matches` so the BYTE-EXACT crypt value plane (D21) runs
//! over the whole bank on every test run; a D21 regression fails this rail.
use std::path::Path;

#[test]
fn replay_committed_corpus() {
    let corpus =
        Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/pgcryptofam_diff"));
    let mut n = 0usize;
    let rd = std::fs::read_dir(corpus).expect(
        "fuzz/corpus/pgcryptofam_diff missing — corpus-excluding sparse checkout? \
         whole-lib gates need the committed corpus (see whole-lib-gate law)",
    );
    for e in rd {
        let p = e.unwrap().path();
        if p.is_file() && p.file_name().is_some_and(|f| f != ".gitkeep") {
            decoder_fuzz::pgcryptofam_diff(&std::fs::read(&p).unwrap());
            n += 1;
        }
    }
    assert!(
        n > 2000,
        "pgcryptofam_diff corpus rail replayed only {n} inputs — bank missing?"
    );
    // Non-vacuity: the counted-skip planes must be alive, not the whole arm.
    let (cost, fc) = (
        decoder_fuzz::pgcryptofam_diff::cost_skips(),
        decoder_fuzz::pgcryptofam_diff::fc_skips(),
    );
    assert_eq!(fc, 0, "arm 0/1 execs skipped for want of a GUC store");
    eprintln!(
        "pgcryptofam_diff: replayed {n} committed corpus inputs, 0 divergences \
         (cost_skips={cost}, fc_skips={fc})"
    );
}
