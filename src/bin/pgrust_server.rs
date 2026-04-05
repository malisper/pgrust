use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use std::path::PathBuf;

use pgrust::database::Database;
use pgrust::server::serve;

fn main() -> Result<(), String> {
    let base_dir = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("pgrust_server"));
    std::fs::create_dir_all(&base_dir).map_err(|e| e.to_string())?;

    let port = std::env::args()
        .nth(2)
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(5433);

    let pool_size = std::env::args()
        .nth(3)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(128);

    eprintln!("pgrust: data directory: {}", base_dir.display());
    eprintln!("pgrust: buffer pool size: {pool_size}");

    let db = Database::open(&base_dir, pool_size).map_err(|e| format!("{e:?}"))?;

    serve(&format!("0.0.0.0:{port}"), db).map_err(|e| e.to_string())
}
