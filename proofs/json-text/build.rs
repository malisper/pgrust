fn main() {
    // Link the vendored C only for native tests/bins; kani uses --c-lib.
    if std::env::var("CARGO_CFG_KANI").is_err() {
        cc::Build::new()
            .file("c/pg_json_text.c")
            .compile("pg_json_text");
    }
    println!("cargo:rerun-if-changed=c/pg_json_text.c");
}
