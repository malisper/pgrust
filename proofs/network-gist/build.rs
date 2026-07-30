// Native builds (tests / concrete playback) link the vendored C so extern
// symbols resolve; Kani links via --c-lib instead (build scripts are not
// run under cbmc codegen for the C side).
fn main() {
    println!("cargo:rerun-if-changed=c/pg_network_gist.c");
    println!("cargo:rerun-if-changed=c/pg_net_bits.c");
    cc::Build::new()
        .file("c/pg_network_gist.c")
        .file("c/pg_net_bits.c")
        .flag_if_supported("-fwrapv")
        .flag_if_supported("-U_FORTIFY_SOURCE")
        .flag_if_supported("-D_FORTIFY_SOURCE=0")
        .compile("pg_network_gist_proof");
}
