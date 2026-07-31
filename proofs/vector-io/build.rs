fn main() {
    // Link the vendored C only for native tests; kani uses --c-lib.
    if std::env::var("CARGO_CFG_KANI").is_err() {
        cc::Build::new()
            .file("c/pg_vector_io.c")
            .file("../intout/c/pg_intout.c")
            .compile("pg_vector_io_native");
    }
    println!("cargo:rerun-if-changed=c/pg_vector_io.c");
    println!("cargo:rerun-if-changed=../intout/c/pg_intout.c");
}
