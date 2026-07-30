fn main() {
    // Link the vendored C only for native bins (native differential replay
    // of the dist_time/dist_timetz wrap plane — Kani's debug overflow
    // checks make the out-of-contract plane unprovable through shipped
    // code; C -fwrapv vs shipped RELEASE wrap is adjudicated natively).
    // kani uses --c-lib instead (float-agg/datetime-cmp precedent).
    if std::env::var("CARGO_CFG_KANI").is_err() {
        cc::Build::new()
            .file("c/pg_brin_multi_dist.c")
            .flag("-fwrapv")
            .compile("pg_brin_multi_dist");
    }
    println!("cargo:rerun-if-changed=c/pg_brin_multi_dist.c");
}
