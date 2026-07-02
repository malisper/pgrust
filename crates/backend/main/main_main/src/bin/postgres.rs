#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() {
    seams_init::init_all();
    let argv: Vec<String> = std::env::args().collect();
    if let Err(e) = main_main::pg_main(&argv) {
        elog::write_stderr(&format!("FATAL:  {}\n", e.message));
        std::process::exit(1);
    }
}
