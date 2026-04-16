use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use std::path::PathBuf;

use pgrust::pgrust::database::Database;
use pgrust::pgrust::server::serve;

#[derive(Debug, Clone, PartialEq, Eq)]
struct Config {
    base_dir: PathBuf,
    port: u16,
    pool_size: usize,
}

fn raise_fd_limit() {
    unsafe {
        let mut rlim = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut rlim) == 0 {
            let target = 10240u64.min(rlim.rlim_max);
            if rlim.rlim_cur < target {
                rlim.rlim_cur = target;
                libc::setrlimit(libc::RLIMIT_NOFILE, &rlim);
            }
        }
    }
}

fn usage() -> String {
    format!(
        "Usage: pgrust_server [--dir PATH] [--port PORT] [--pool-size PAGES]\n       pgrust_server [PATH [PORT [POOL_SIZE]]]\n\nDefaults:\n  PATH = {}\n  PORT = 5433\n  POOL_SIZE = 16384",
        std::env::temp_dir().join("pgrust_server").display()
    )
}

fn take_value(raw: &[String], i: &mut usize, flag: &str) -> Result<String, String> {
    *i += 1;
    let value = raw
        .get(*i)
        .cloned()
        .ok_or_else(|| format!("missing value for {flag}\n\n{}", usage()))?;
    *i += 1;
    Ok(value)
}

fn parse_port(value: &str, label: &str) -> Result<u16, String> {
    value
        .parse::<u16>()
        .map_err(|_| format!("invalid {label} value: {value}\n\n{}", usage()))
}

fn parse_pool_size(value: &str, label: &str) -> Result<usize, String> {
    value
        .parse::<usize>()
        .map_err(|_| format!("invalid {label} value: {value}\n\n{}", usage()))
}

fn parse_args_from<I>(args: I) -> Result<Config, String>
where
    I: IntoIterator<Item = String>,
{
    let mut config = Config {
        base_dir: std::env::temp_dir().join("pgrust_server"),
        port: 5433,
        pool_size: 16384,
    };
    let mut saw_dir = false;
    let mut saw_port = false;
    let mut saw_pool_size = false;
    let mut positional = Vec::new();

    let raw = args.into_iter().collect::<Vec<_>>();
    let mut i = 0;
    while i < raw.len() {
        match raw[i].as_str() {
            "--dir" => {
                config.base_dir = PathBuf::from(take_value(&raw, &mut i, "--dir")?);
                saw_dir = true;
            }
            "--port" => {
                config.port = parse_port(&take_value(&raw, &mut i, "--port")?, "--port")?;
                saw_port = true;
            }
            "--pool-size" => {
                config.pool_size =
                    parse_pool_size(&take_value(&raw, &mut i, "--pool-size")?, "--pool-size")?;
                saw_pool_size = true;
            }
            "-h" | "--help" => return Err(usage()),
            arg if arg.starts_with('-') => {
                return Err(format!("unknown option: {arg}\n\n{}", usage()));
            }
            _ => {
                positional.push(raw[i].clone());
                i += 1;
            }
        }
    }

    let mut positional = positional.into_iter();
    if !saw_dir && let Some(value) = positional.next() {
        config.base_dir = PathBuf::from(value);
    }
    if !saw_port && let Some(value) = positional.next() {
        config.port = parse_port(&value, "PORT")?;
    }
    if !saw_pool_size && let Some(value) = positional.next() {
        config.pool_size = parse_pool_size(&value, "POOL_SIZE")?;
    }
    if let Some(extra) = positional.next() {
        return Err(format!(
            "unexpected extra positional argument: {extra}\n\n{}",
            usage()
        ));
    }

    Ok(config)
}

fn parse_args() -> Result<Config, String> {
    parse_args_from(std::env::args().skip(1))
}

fn main() -> Result<(), String> {
    raise_fd_limit();
    let config = parse_args()?;
    std::fs::create_dir_all(&config.base_dir).map_err(|e| e.to_string())?;

    eprintln!("pgrust: data directory: {}", config.base_dir.display());
    eprintln!("pgrust: buffer pool size: {}", config.pool_size);

    let db = Database::open(&config.base_dir, config.pool_size).map_err(|e| format!("{e:?}"))?;

    serve(&format!("0.0.0.0:{}", config.port), db).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_args_supports_named_flags() {
        let config = parse_args_from([
            "--dir".to_string(),
            "/tmp/pgrust_named".to_string(),
            "--port".to_string(),
            "5544".to_string(),
            "--pool-size".to_string(),
            "2048".to_string(),
        ])
        .unwrap();
        assert_eq!(config.base_dir, PathBuf::from("/tmp/pgrust_named"));
        assert_eq!(config.port, 5544);
        assert_eq!(config.pool_size, 2048);
    }

    #[test]
    fn parse_args_keeps_legacy_positionals() {
        let config = parse_args_from([
            "/tmp/pgrust_positional".to_string(),
            "5545".to_string(),
            "4096".to_string(),
        ])
        .unwrap();
        assert_eq!(config.base_dir, PathBuf::from("/tmp/pgrust_positional"));
        assert_eq!(config.port, 5545);
        assert_eq!(config.pool_size, 4096);
    }

    #[test]
    fn parse_args_allows_mixed_flags_and_positionals() {
        let config = parse_args_from([
            "--port".to_string(),
            "5546".to_string(),
            "/tmp/pgrust_mix".into(),
        ])
        .unwrap();
        assert_eq!(config.base_dir, PathBuf::from("/tmp/pgrust_mix"));
        assert_eq!(config.port, 5546);
        assert_eq!(config.pool_size, 16384);
    }
}
