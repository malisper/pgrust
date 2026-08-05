//! Replay rail for tupaccess_diff divergence artifacts: any file left under
//! fuzz/artifacts-triage/tupaccess/ replays through the driver on `cargo
//! test` (stable rail; no nightly needed to reproduce a libFuzzer artifact).
#[test]
fn replay_triage_artifacts() {
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../artifacts-triage/tupaccess");
    let Ok(rd) = std::fs::read_dir(dir) else { return };
    for e in rd {
        let p = e.unwrap().path();
        if p.is_file() {
            eprintln!("REPLAY {}", p.display());
            decoder_fuzz::tupaccess_diff(&std::fs::read(&p).unwrap());
        }
    }
}
