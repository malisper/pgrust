fn main() {
    // Link the vendored C only for native bins/tests; kani uses --c-lib instead.
    if std::env::var("CARGO_CFG_KANI").is_err() {
        cc::Build::new().file("c/pg_escape.c").compile("pg_escape");
    }
    println!("cargo:rerun-if-changed=c/pg_escape.c");
}
