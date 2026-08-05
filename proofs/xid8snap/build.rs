fn main() {
    // Link the vendored C only for native tests/bins; kani uses --c-lib.
    if std::env::var("CARGO_CFG_KANI").is_err() {
        cc::Build::new()
            .file("c/pg_xid8snap.c")
            .compile("pg_xid8snap");
    }
    println!("cargo:rerun-if-changed=c/pg_xid8snap.c");
}
