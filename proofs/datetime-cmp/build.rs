fn main() {
    // Link the vendored C only for native bins/tests (the wave-6 divider
    // rows' tested(differential) tier); kani uses --c-lib instead.
    if std::env::var("CARGO_CFG_KANI").is_err() {
        cc::Build::new()
            .file("c/pg_datetime_cmp.c")
            .flag("-fwrapv")
            .compile("pg_datetime_cmp");
    }
    println!("cargo:rerun-if-changed=c/pg_datetime_cmp.c");
}
