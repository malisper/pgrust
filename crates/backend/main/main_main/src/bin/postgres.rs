// mimalloc is a C allocator that does not build for wasm32; Rust's
// wasm32-wasip1 std defaults to dlmalloc, so the wasm arm simply takes the
// std default allocator and the release hook stays unset (mcx release is a
// no-op without a hook). Native arm unchanged.
#[cfg(not(target_family = "wasm"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() {
    // Transport provider resolution (§2.4 seam): one argv peek, once, before
    // any seam install — set-once at boot is the house pattern. Everything
    // but `--stdio-wire` (all the C dispatch options included) boots the
    // socket provider, i.e. the unchanged native byte path.
    let transport = if std::env::args().nth(1).as_deref() == Some("--stdio-wire") {
        seams_init::Transport::StdioWire
    } else {
        seams_init::Transport::Socket
    };
    seams_init::init_all_with_transport(transport);
    // mi_collect(force) releases mimalloc's freed-but-retained segments;
    // called only at alloc-churn boundaries (hashagg spill batch resets)
    // where retention would otherwise hold batch-sized RSS for the whole
    // spill pass.
    #[cfg(not(target_family = "wasm"))]
    mcx::set_allocator_release(|| unsafe { libmimalloc_sys::mi_collect(true) });
    let argv: Vec<String> = std::env::args().collect();
    if let Err(e) = main_main::pg_main(&argv) {
        elog::write_stderr(&format!("FATAL:  {}\n", e.message));
        std::process::exit(1);
    }
}
