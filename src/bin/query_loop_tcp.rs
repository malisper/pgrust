use std::env;
use std::io::{self, Read, Write};
use std::net::TcpStream;

const PROTOCOL_VERSION_3_0: i32 = 196608;

struct Config {
    host: String,
    port: u16,
    user: String,
    dbname: String,
    query: String,
    count: usize,
}

fn usage() -> ! {
    eprintln!(
        "Usage: cargo run --bin query_loop_tcp -- --query SQL --count N [options]

Options:
  --host HOST         PostgreSQL host (default: 127.0.0.1)
  --port PORT         PostgreSQL port (default: 5432)
  --user USER         PostgreSQL user (default: postgres)
  --db DBNAME         Database name (default: postgres)
  --query SQL         SQL to run
  --count N           Number of times to run the query

Example:
  cargo run --bin query_loop_tcp -- --port 5545 --query 'select * from bench_select;' --count 500"
    );
    std::process::exit(2);
}

fn parse_args() -> Config {
    let mut host = "127.0.0.1".to_string();
    let mut port = 5432u16;
    let mut user = "postgres".to_string();
    let mut dbname = "postgres".to_string();
    let mut query = None;
    let mut count = None;

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--host" => host = args.next().unwrap_or_else(|| usage()),
            "--port" => {
                let value = args.next().unwrap_or_else(|| usage());
                port = value.parse().unwrap_or_else(|_| usage());
            }
            "--user" => user = args.next().unwrap_or_else(|| usage()),
            "--db" | "--dbname" => dbname = args.next().unwrap_or_else(|| usage()),
            "--query" => query = Some(args.next().unwrap_or_else(|| usage())),
            "--count" => {
                let value = args.next().unwrap_or_else(|| usage());
                count = Some(value.parse().unwrap_or_else(|_| usage()));
            }
            "-h" | "--help" => usage(),
            _ => {
                eprintln!("Unknown argument: {arg}");
                usage();
            }
        }
    }

    Config {
        host,
        port,
        user,
        dbname,
        query: query.unwrap_or_else(|| {
            eprintln!("--query is required");
            usage();
        }),
        count: count.unwrap_or_else(|| {
            eprintln!("--count is required");
            usage();
        }),
    }
}

fn write_startup(stream: &mut TcpStream, cfg: &Config) -> io::Result<()> {
    let mut body = Vec::new();
    body.extend_from_slice(&PROTOCOL_VERSION_3_0.to_be_bytes());
    for (k, v) in [
        ("user", cfg.user.as_str()),
        ("database", cfg.dbname.as_str()),
        ("client_encoding", "UTF8"),
    ] {
        body.extend_from_slice(k.as_bytes());
        body.push(0);
        body.extend_from_slice(v.as_bytes());
        body.push(0);
    }
    body.push(0);

    let len = (body.len() + 4) as i32;
    stream.write_all(&len.to_be_bytes())?;
    stream.write_all(&body)?;
    stream.flush()
}

fn read_i32(stream: &mut TcpStream) -> io::Result<i32> {
    let mut buf = [0u8; 4];
    stream.read_exact(&mut buf)?;
    Ok(i32::from_be_bytes(buf))
}

fn read_cstr_fields(mut body: &[u8]) -> Vec<(String, String)> {
    let mut out = Vec::new();
    while !body.is_empty() && body[0] != 0 {
        let Some(pos1) = body.iter().position(|b| *b == 0) else {
            break;
        };
        let key = String::from_utf8_lossy(&body[..pos1]).to_string();
        body = &body[pos1 + 1..];
        let Some(pos2) = body.iter().position(|b| *b == 0) else {
            break;
        };
        let val = String::from_utf8_lossy(&body[..pos2]).to_string();
        body = &body[pos2 + 1..];
        out.push((key, val));
    }
    out
}

fn drain_until_ready(stream: &mut TcpStream) -> io::Result<()> {
    loop {
        let mut ty = [0u8; 1];
        stream.read_exact(&mut ty)?;
        let len = read_i32(stream)? as usize;
        if len < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "message too short",
            ));
        }
        let mut body = vec![0u8; len - 4];
        stream.read_exact(&mut body)?;
        match ty[0] {
            b'Z' => return Ok(()),
            b'E' => {
                let fields = read_cstr_fields(&body);
                let msg = fields
                    .iter()
                    .find(|(k, _)| k == "M")
                    .map(|(_, v)| v.clone())
                    .unwrap_or_else(|| "server error".to_string());
                return Err(io::Error::other(msg));
            }
            b'R' | b'S' | b'K' | b'T' | b'D' | b'C' | b'N' | b'I' => {}
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unexpected message type {}", other as char),
                ));
            }
        }
    }
}

fn send_query(stream: &mut TcpStream, query: &str) -> io::Result<()> {
    let mut body = Vec::with_capacity(query.len() + 1);
    body.extend_from_slice(query.as_bytes());
    body.push(0);
    let len = (body.len() + 4) as i32;
    stream.write_all(b"Q")?;
    stream.write_all(&len.to_be_bytes())?;
    stream.write_all(&body)?;
    stream.flush()
}

fn send_terminate(stream: &mut TcpStream) -> io::Result<()> {
    stream.write_all(b"X")?;
    stream.write_all(&4_i32.to_be_bytes())?;
    stream.flush()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = parse_args();
    let addr = format!("{}:{}", cfg.host, cfg.port);
    let mut stream = TcpStream::connect(addr)?;
    stream.set_nodelay(true)?;

    write_startup(&mut stream, &cfg)?;
    drain_until_ready(&mut stream)?;

    for _ in 0..cfg.count {
        send_query(&mut stream, &cfg.query)?;
        drain_until_ready(&mut stream)?;
    }

    let _ = send_terminate(&mut stream);
    Ok(())
}
