fn main() {
    // Link the vendored C only for native bins/tests (native differential
    // replay of the sqrt-bearing grid FAILEDs — CBMC sqrt-model artifact
    // adjudication); kani uses --c-lib instead (datetime-cmp precedent).
    if std::env::var("CARGO_CFG_KANI").is_err() {
        cc::Build::new()
            .file("c/pg_float_agg.c")
            .flag("-fwrapv")
            .define("PG_PROOF_NATIVE", None)
            .compile("pg_float_agg");
    }
    println!("cargo:rerun-if-changed=c/pg_float_agg.c");
}
