// Single-request HTTP/1.1 client over TCP or TLS (OpenSSL, hostname
// verification on). No redirects, no keep-alive: every request is
// `Connection: close`, and the body is capped at MAX_BODY.

use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use openssl::ssl::{SslConnector, SslMethod, SslStream};

const MAX_BODY: usize = 1 << 20;
const MAX_HEADER: usize = 64 << 10;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Url {
    pub https: bool,
    pub host: String,
    pub port: u16,
    pub path: String,
}

impl Url {
    pub fn origin(&self) -> String {
        let scheme = if self.https { "https" } else { "http" };
        let default = if self.https { 443 } else { 80 };
        if self.port == default {
            format!("{scheme}://{}", self.host)
        } else {
            format!("{scheme}://{}:{}", self.host, self.port)
        }
    }
}

pub fn parse_url(s: &str) -> Result<Url, String> {
    let (https, rest) = if let Some(r) = s.strip_prefix("https://") {
        (true, r)
    } else if let Some(r) = s.strip_prefix("http://") {
        (false, r)
    } else {
        return Err(format!("unsupported URL scheme in \"{s}\""));
    };
    let (authority, path) = match rest.find(['/', '?', '#']) {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };
    if authority.contains('@') {
        return Err(format!("userinfo is not allowed in URL \"{s}\""));
    }
    let (host, port) = if let Some(r) = authority.strip_prefix('[') {
        let end = r.find(']').ok_or_else(|| format!("bad IPv6 host in URL \"{s}\""))?;
        (&r[..end], r[end + 1..].strip_prefix(':'))
    } else {
        match authority.rsplit_once(':') {
            Some((h, p)) => (h, Some(p)),
            None => (authority, None),
        }
    };
    if host.is_empty() {
        return Err(format!("missing host in URL \"{s}\""));
    }
    let port = match port {
        Some(p) => p.parse::<u16>().map_err(|_| format!("bad port in URL \"{s}\""))?,
        None if https => 443,
        None => 80,
    };
    let path = match path.find('#') {
        Some(i) => &path[..i],
        None => path,
    };
    let path = if path.is_empty() || path.starts_with('?') {
        format!("/{path}")
    } else {
        path.to_string()
    };
    Ok(Url { https, host: host.to_string(), port, path })
}

#[derive(Clone, Debug, Default)]
pub struct HttpOptions {
    pub allow_insecure_http: bool,
    pub ca_file: Option<String>,
    pub timeout: Duration,
}

#[derive(Debug)]
pub struct Response {
    pub status: u16,
    pub body: Vec<u8>,
}

enum Stream {
    Plain(TcpStream),
    Tls(SslStream<TcpStream>),
}

impl Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Stream::Plain(s) => s.read(buf),
            Stream::Tls(s) => s.read(buf),
        }
    }
}

impl Write for Stream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Stream::Plain(s) => s.write(buf),
            Stream::Tls(s) => s.write(buf),
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Stream::Plain(s) => s.flush(),
            Stream::Tls(s) => s.flush(),
        }
    }
}

fn connect(url: &Url, opts: &HttpOptions) -> Result<Stream, String> {
    if !url.https && !opts.allow_insecure_http {
        return Err(format!(
            "refusing plain-HTTP endpoint \"{}\" (set jwt_validator.allow_insecure_http to permit it)",
            url.origin()
        ));
    }
    let addrs = (url.host.as_str(), url.port)
        .to_socket_addrs()
        .map_err(|e| format!("could not resolve \"{}\": {e}", url.host))?;
    let mut last = None;
    let mut tcp = None;
    for addr in addrs {
        match TcpStream::connect_timeout(&addr, opts.timeout) {
            Ok(s) => {
                tcp = Some(s);
                break;
            }
            Err(e) => last = Some(e),
        }
    }
    let tcp = tcp.ok_or_else(|| match last {
        Some(e) => format!("could not connect to \"{}\": {e}", url.origin()),
        None => format!("could not resolve \"{}\": no addresses", url.host),
    })?;
    tcp.set_read_timeout(Some(opts.timeout)).map_err(|e| e.to_string())?;
    tcp.set_write_timeout(Some(opts.timeout)).map_err(|e| e.to_string())?;
    if !url.https {
        return Ok(Stream::Plain(tcp));
    }
    let mut b = SslConnector::builder(SslMethod::tls_client()).map_err(|e| e.to_string())?;
    if let Some(ca) = opts.ca_file.as_deref().filter(|s| !s.is_empty()) {
        b.set_ca_file(ca)
            .map_err(|e| format!("could not load jwt_validator.ca_file \"{ca}\": {e}"))?;
    }
    let tls = b
        .build()
        .connect(&url.host, tcp)
        .map_err(|e| format!("TLS handshake with \"{}\" failed: {e}", url.origin()))?;
    Ok(Stream::Tls(tls))
}

pub fn get(url: &Url, opts: &HttpOptions) -> Result<Response, String> {
    request(url, "GET", &[("Accept", "application/json")], None, opts)
}

pub fn post_form(
    url: &Url,
    body: &str,
    basic_auth: Option<&str>,
    opts: &HttpOptions,
) -> Result<Response, String> {
    let mut headers = vec![
        ("Accept", "application/json".to_string()),
        ("Content-Type", "application/x-www-form-urlencoded".to_string()),
    ];
    if let Some(auth) = basic_auth {
        headers.push(("Authorization", format!("Basic {auth}")));
    }
    let hdrs: Vec<(&str, &str)> = headers.iter().map(|(k, v)| (*k, v.as_str())).collect();
    request(url, "POST", &hdrs, Some(body.as_bytes()), opts)
}

fn request(
    url: &Url,
    method: &str,
    headers: &[(&str, &str)],
    body: Option<&[u8]>,
    opts: &HttpOptions,
) -> Result<Response, String> {
    let mut stream = connect(url, opts)?;
    let mut req = format!("{method} {} HTTP/1.1\r\nHost: {}\r\nUser-Agent: pgrust-jwt-validator\r\nConnection: close\r\n", url.path, host_header(url));
    for (k, v) in headers {
        req.push_str(k);
        req.push_str(": ");
        req.push_str(v);
        req.push_str("\r\n");
    }
    if let Some(b) = body {
        req.push_str(&format!("Content-Length: {}\r\n", b.len()));
    }
    req.push_str("\r\n");
    stream
        .write_all(req.as_bytes())
        .and_then(|()| body.map_or(Ok(()), |b| stream.write_all(b)))
        .and_then(|()| stream.flush())
        .map_err(|e| format!("write to \"{}\" failed: {e}", url.origin()))?;

    let mut raw = Vec::new();
    let mut buf = [0u8; 8192];
    let header_end = loop {
        if let Some(i) = find(&raw, b"\r\n\r\n") {
            break i;
        }
        if raw.len() > MAX_HEADER {
            return Err("response headers too large".into());
        }
        let n = stream
            .read(&mut buf)
            .map_err(|e| format!("read from \"{}\" failed: {e}", url.origin()))?;
        if n == 0 {
            return Err("connection closed before response headers".into());
        }
        raw.extend_from_slice(&buf[..n]);
    };
    let head = std::str::from_utf8(&raw[..header_end]).map_err(|_| "non-UTF-8 response headers")?;
    let mut lines = head.split("\r\n");
    let status_line = lines.next().unwrap_or("");
    let status = status_line
        .strip_prefix("HTTP/1.")
        .and_then(|s| s.get(2..5))
        .and_then(|s| s.parse::<u16>().ok())
        .ok_or_else(|| format!("malformed status line \"{status_line}\""))?;
    let mut content_length: Option<usize> = None;
    let mut chunked = false;
    for line in lines {
        let Some((k, v)) = line.split_once(':') else { continue };
        let v = v.trim();
        if k.eq_ignore_ascii_case("content-length") {
            content_length = Some(v.parse().map_err(|_| "bad Content-Length")?);
        } else if k.eq_ignore_ascii_case("transfer-encoding") && v.to_ascii_lowercase().contains("chunked") {
            chunked = true;
        }
    }
    let mut rest = raw[header_end + 4..].to_vec();
    let want = if chunked { None } else { content_length };
    loop {
        if let Some(w) = want {
            if rest.len() >= w {
                rest.truncate(w);
                break;
            }
        }
        if rest.len() > MAX_BODY {
            return Err("response body too large".into());
        }
        let n = stream
            .read(&mut buf)
            .map_err(|e| format!("read from \"{}\" failed: {e}", url.origin()))?;
        if n == 0 {
            if want.is_some_and(|w| rest.len() < w) {
                return Err("connection closed before the full response body".into());
            }
            break;
        }
        rest.extend_from_slice(&buf[..n]);
    }
    let body = if chunked { dechunk(&rest)? } else { rest };
    Ok(Response { status, body })
}

fn host_header(url: &Url) -> String {
    let host = if url.host.contains(':') { format!("[{}]", url.host) } else { url.host.clone() };
    let default = if url.https { 443 } else { 80 };
    if url.port == default {
        host
    } else {
        format!("{host}:{}", url.port)
    }
}

fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

fn dechunk(data: &[u8]) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    let mut i = 0;
    loop {
        let eol = find(&data[i..], b"\r\n").ok_or("truncated chunked body")? + i;
        let size_text = std::str::from_utf8(&data[i..eol]).map_err(|_| "bad chunk size")?;
        let size_text = size_text.split(';').next().unwrap_or("").trim();
        let size = usize::from_str_radix(size_text, 16).map_err(|_| "bad chunk size")?;
        i = eol + 2;
        if size == 0 {
            return Ok(out);
        }
        let end = i + size;
        if end > data.len() {
            return Err("truncated chunk".into());
        }
        out.extend_from_slice(&data[i..end]);
        if out.len() > MAX_BODY {
            return Err("response body too large".into());
        }
        i = end + 2;
    }
}
