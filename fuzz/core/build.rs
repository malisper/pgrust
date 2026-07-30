// Compile the vendored PostgreSQL C oracles for the differential fuzz
// targets (csrc/README-style provenance headers in each file). Same cc
// pattern as proofs/brin-minmax/build.rs — plain native compile; there is
// no Kani arm here (the fuzz workspace never builds under cargo-kani).
fn main() {
    cc::Build::new()
        .file("csrc/pg_float_io.c")
        .file("csrc/pg_geo_io.c")
        .file("csrc/ryu/d2s.c")
        .file("csrc/ryu/f2s.c")
        .include("csrc/shim")
        .include("csrc/ryu")
        .flag_if_supported("-fno-strict-aliasing")
        .flag_if_supported("-fwrapv")
        .compile("pg_difffuzz_oracle");
    println!("cargo:rerun-if-changed=csrc");
}
