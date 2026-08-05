// Stage every contrib crate's extension/ files (*.control, *.sql) into
// target/<profile>/share/extension/ beside the postgres binary — no install
// step exists, and CREATE EXTENSION resolves this staged dir first (see
// commands/extension control.rs staged_extension_dir). Dist binaries fetched
// from S3 travel without the staged dir; PGRUST_PGSHAREDIR-resolved system
// files cover that path.
use std::path::{Path, PathBuf};

fn main() {
    let manifest_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    let contrib_dir = manifest_dir.join("../../../contrib").canonicalize().unwrap();
    println!("cargo:rerun-if-changed={}", contrib_dir.display());

    // OUT_DIR = target/<profile>/build/main_main-<hash>/out
    let out_dir = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    let profile_dir = out_dir
        .ancestors()
        .nth(3)
        .expect("OUT_DIR has the standard cargo build layout");
    let staged = profile_dir.join("share/extension");
    std::fs::create_dir_all(&staged).unwrap();

    // Contrib text search data files (unaccent.rules, ...) stage the same way
    // into share/tsearch_data/; ts_locale's get_tsearch_config_filename
    // resolves the staged file first.
    let staged_tsdata = profile_dir.join("share/tsearch_data");
    std::fs::create_dir_all(&staged_tsdata).unwrap();

    for crate_entry in std::fs::read_dir(&contrib_dir).unwrap() {
        let crate_path = crate_entry.unwrap().path();
        for (sub, staged) in [("extension", &staged), ("tsearch_data", &staged_tsdata)] {
            let src_dir = crate_path.join(sub);
            if !src_dir.is_dir() {
                continue;
            }
            for f in std::fs::read_dir(&src_dir).unwrap() {
                let src = f.unwrap().path();
                if !src.is_file() {
                    continue;
                }
                copy_if_changed(&src, &staged.join(src.file_name().unwrap()));
            }
        }
    }
}

// Unconditional copies would dirty mtimes and cascade rebuilds.
fn copy_if_changed(src: &Path, dst: &Path) {
    let same = match (std::fs::read(src), std::fs::read(dst)) {
        (Ok(a), Ok(b)) => a == b,
        _ => false,
    };
    if !same {
        std::fs::copy(src, dst).unwrap();
    }
}
