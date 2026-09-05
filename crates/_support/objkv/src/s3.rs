//! S3 client: `ureq` for transport (blocking, TLS, pooled connections),
//! `rusty-s3` for request construction and SigV4 presigning -- synchronous, so
//! it suits a thread-per-backend server, and it supplies the S3 semantics
//! (ListObjectsV2 parsing especially) this used to reimplement badly.
//!
//! Signing is by presigned URL, so bodies are never hashed: the payload hash
//! is `UNSIGNED-PAYLOAD`.

use std::io::{self, Read};
use std::time::Duration;

use rusty_s3::actions::{ListObjectsV2, S3Action};
use rusty_s3::{Bucket, Credentials, UrlStyle};

/// Socket deadline. Without one a network black hole becomes an unbounded
/// stall, which is the failure mode that matters most here.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// How long a presigned URL stays valid; it only has to cover clock skew.
const SIGN_EXPIRY: Duration = Duration::from_secs(300);

/// Attempts per request, including the first. Transport failures and 5xx are
/// retried; every 4xx except the conditional-write conflict is permanent.
const MAX_ATTEMPTS: u32 = 4;
const RETRY_BASE_DELAY: Duration = Duration::from_millis(50);

include!("s3_types.rs");

pub struct Client {
    agent: ureq::Agent,
    bucket: Bucket,
    creds: Credentials,
}

impl Client {
    /// Path-style addressing: virtual-host style needs per-bucket DNS.
    pub fn new(
        endpoint: &str,
        bucket: &str,
        region: &str,
        access_key: &str,
        secret_key: &str,
    ) -> io::Result<Client> {
        let url = endpoint
            .parse()
            .map_err(|e| io::Error::other(format!("bad S3 endpoint {endpoint:?}: {e}")))?;
        let bucket = Bucket::new(url, UrlStyle::Path, bucket.to_string(), region.to_string())
            .map_err(|e| io::Error::other(format!("bad S3 bucket: {e}")))?;
        Ok(Client {
            agent: ureq::AgentBuilder::new()
                .timeout_read(REQUEST_TIMEOUT)
                .timeout_write(REQUEST_TIMEOUT)
                .timeout_connect(REQUEST_TIMEOUT)
                // Never follow a redirect. S3 answers 301/307 for a bucket in
                // another region or one just created; ureq would re-send a
                // PUT as a GET, or hand back the 3xx as success, and either
                // way a commit that never landed would be acknowledged. A
                // redirect is a misconfiguration and is reported as one.
                .redirects(0)
                .build(),
            bucket,
            creds: Credentials::new(access_key, secret_key),
        })
    }

    pub fn new_with_token(
        endpoint: &str,
        bucket: &str,
        region: &str,
        access_key: &str,
        secret_key: &str,
        token: &str,
    ) -> io::Result<Client> {
        let mut c = Client::new(endpoint, bucket, region, access_key, secret_key)?;
        c.creds = Credentials::new_with_token(access_key, secret_key, token);
        Ok(c)
    }

    /// `PUT` with `If-None-Match: *` — the primitive the whole design rests on.
    /// Only a 2xx is `Written`; `execute` turns everything else into an error.
    pub fn put_if_absent(&self, key: &str, body: &[u8]) -> io::Result<PutOutcome> {
        let mut action = self.bucket.put_object(Some(&self.creds), key);
        action.headers_mut().insert("if-none-match", "*");
        let url = action.sign(SIGN_EXPIRY);
        let hdrs = [("if-none-match", "*")];

        match self.execute("PUT", url.as_str(), &hdrs, Some(body)) {
            Ok(_) => Ok(PutOutcome::Written),
            // 412 is the conditional-write refusal: the key exists, we lost. 409 is
            // deliberately not folded in -- S3 returns it when a concurrent request for
            // the same key is in flight and documents it as retryable, so the outcome is
            // unknown rather than lost. `execute` retries it.
            Err(Fail::Status(412, _)) => Ok(PutOutcome::AlreadyExists),
            Err(e) => Err(e.into()),
        }
    }

    pub fn get(&self, key: &str) -> io::Result<Option<Vec<u8>>> {
        let url = self.bucket.get_object(Some(&self.creds), key).sign(SIGN_EXPIRY);
        match self.execute("GET", url.as_str(), &[], None) {
            Ok(r) => Ok(Some(r.body)),
            Err(Fail::Status(404, _)) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Ranged GET — what makes a cold point-read one round trip instead of
    /// downloading a whole sorted run.
    pub fn get_range(&self, key: &str, offset: u64, len: u64) -> io::Result<Option<Vec<u8>>> {
        // `offset + len - 1` underflows on a zero-length range.
        if len == 0 {
            return Ok(Some(Vec::new()));
        }
        let range = format!("bytes={offset}-{}", offset + len - 1);
        let mut action = self.bucket.get_object(Some(&self.creds), key);
        action.headers_mut().insert("range", range.clone());
        let url = action.sign(SIGN_EXPIRY);
        let hdrs = [("range", range.as_str())];

        match self.execute("GET", url.as_str(), &hdrs, None) {
            Ok(r) => {
                // A store that ignores `Range` answers 200 with the whole object, handing
                // the run reader a file where it expected a block. The status is the only
                // thing that distinguishes them.
                if r.status != 206 {
                    return Err(io::Error::other(format!(
                        "s3: ranged GET of {key} returned status {}, not 206; \
                         the store ignored the Range header",
                        r.status
                    )));
                }
                Ok(Some(r.body))
            }
            Err(Fail::Status(404, _)) => Ok(None),
            // A range past the end of the object.
            Err(Fail::Status(416, _)) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// A 404 is success: a retried delete must not fail the second time.
    pub fn delete(&self, key: &str) -> io::Result<()> {
        let url = self.bucket.delete_object(Some(&self.creds), key).sign(SIGN_EXPIRY);
        match self.execute("DELETE", url.as_str(), &[], None) {
            Ok(_) | Err(Fail::Status(404, _)) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// `ListObjectsV2`, following continuation tokens to completion. Pagination is
    /// not optional: 1000 keys per call, and ignoring the token passes every small
    /// test and then truncates recovery at scale.
    pub fn list(&self, prefix: &str) -> io::Result<Vec<ObjectInfo>> {
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut action: ListObjectsV2 = self.bucket.list_objects_v2(Some(&self.creds));
            action.with_prefix(prefix.to_string());
            action.with_max_keys(1000);
            if let Some(t) = &token {
                action.with_continuation_token(t.clone());
            }
            let url = action.sign(SIGN_EXPIRY);
            let body = self
                .execute("GET", url.as_str(), &[], None)
                .map_err(io::Error::from)?
                .body;
            // Real XML parsing: a key containing `&` arrives as `&amp;`, and the
            // substring scanner this replaced handed it back verbatim.
            let text = String::from_utf8(body)
                .map_err(|e| io::Error::other(format!("s3 list: non-UTF-8 response: {e}")))?;
            let parsed = ListObjectsV2::parse_response(&text)
                .map_err(|e| io::Error::other(format!("s3 list: malformed XML: {e}")))?;
            out.extend(parsed.contents.into_iter().map(|c| ObjectInfo {
                key: c.key,
                size: c.size,
            }));
            match parsed.next_continuation_token {
                Some(t) => token = Some(t),
                None => return Ok(out),
            }
        }
    }

    /// One HTTP exchange, retried on failures that are actually transient.
    fn execute(
        &self,
        method: &str,
        url: &str,
        headers: &[(&str, &str)],
        body: Option<&[u8]>,
    ) -> Result<Response, Fail> {
        let mut attempt = 0;
        loop {
            attempt += 1;
            match self.send_once(method, url, headers, body) {
                Ok(r) => return Ok(r),
                Err(e) if e.retryable() && attempt < MAX_ATTEMPTS => {
                    std::thread::sleep(RETRY_BASE_DELAY * (1 << (attempt - 1)));
                }
                Err(e) => return Err(e),
            }
        }
    }

    fn send_once(
        &self,
        method: &str,
        url: &str,
        headers: &[(&str, &str)],
        body: Option<&[u8]>,
    ) -> Result<Response, Fail> {
        let mut req = self.agent.request(method, url);
        for (k, v) in headers {
            req = req.set(k, v);
        }
        let resp = match body {
            Some(b) => req.send_bytes(b),
            None => req.call(),
        };
        match resp {
            Ok(r) => {
                let status = r.status();
                let mut buf = Vec::new();
                r.into_reader()
                    .read_to_end(&mut buf)
                    .map_err(|e| Fail::Transport(e.to_string()))?;
                // ureq reports only 4xx and 5xx as errors; a 3xx (or anything
                // else outside 2xx) comes back here as `Ok`, and would pass for
                // a stored object. Success is 2xx and nothing else.
                if !(200..300).contains(&status) {
                    return Err(Fail::Status(status, String::from_utf8_lossy(&buf).into_owned()));
                }
                Ok(Response { status, body: buf })
            }
            Err(ureq::Error::Status(code, r)) => {
                Err(Fail::Status(code, r.into_string().unwrap_or_default()))
            }
            Err(e) => Err(Fail::Transport(e.to_string())),
        }
    }
}

struct Response {
    status: u16,
    body: Vec<u8>,
}

/// Distinguishes a refused request from a broken connection, which is what the
/// retry loop in `execute` needs.
#[derive(Debug)]
enum Fail {
    Status(u16, String),
    Transport(String),
}

impl Fail {
    fn retryable(&self) -> bool {
        match self {
            // A dropped connection says nothing about whether the request applied, but
            // every operation here is idempotent or guarded by put-if-absent.
            Fail::Transport(_) => true,
            Fail::Status(409, _) => true,
            Fail::Status(429, _) => true,
            Fail::Status(c, _) => *c >= 500,
        }
    }
}

impl From<Fail> for io::Error {
    fn from(f: Fail) -> io::Error {
        match f {
            // char_indices, not a byte slice: keys are arbitrary UTF-8 and S3 echoes
            // them into error bodies, so a byte cut can split a codepoint and panic.
            Fail::Status(code, body) => {
                let cut = body
                    .char_indices()
                    .map(|(i, _)| i)
                    .chain([body.len()])
                    .take_while(|&i| i <= 512)
                    .last()
                    .unwrap_or(0);
                io::Error::other(format!("s3 status {code}: {}", &body[..cut]))
            }
            Fail::Transport(e) => io::Error::other(format!("s3 transport: {e}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client(endpoint: &str) -> Client {
        Client::new(endpoint, "b", "us-east-1", "k", "s").unwrap()
    }

    #[test]
    fn path_style_urls_name_the_bucket_in_the_path() {
        let c = client("http://127.0.0.1:9000");
        let url = c.bucket.get_object(Some(&c.creds), "run/000a").sign(SIGN_EXPIRY);
        assert_eq!(url.host_str(), Some("127.0.0.1"));
        assert_eq!(url.port(), Some(9000));
        assert_eq!(url.path(), "/b/run/000a");
        assert!(url.query().unwrap().contains("X-Amz-Signature="));
    }

    #[test]
    fn object_keys_are_encoded_but_keep_separators() {
        let c = client("http://h");
        let url = c.bucket.get_object(Some(&c.creds), "a b").sign(SIGN_EXPIRY);
        assert_eq!(url.path(), "/b/a%20b");
    }

    #[test]
    fn conditional_header_is_signed_into_the_url() {
        let c = client("http://h");
        let mut a = c.bucket.put_object(Some(&c.creds), "k");
        a.headers_mut().insert("if-none-match", "*");
        let signed = a.sign(SIGN_EXPIRY);
        let q = signed.query().unwrap();
        assert!(
            q.contains("if-none-match"),
            "if-none-match must appear in X-Amz-SignedHeaders, got {q}"
        );
    }

    #[test]
    fn list_responses_decode_xml_entities() {
        let xml = "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
                   <Name>b</Name><KeyCount>1</KeyCount><MaxKeys>1000</MaxKeys>\
                   <IsTruncated>false</IsTruncated>\
                   <Contents><Key>a&amp;b</Key><Size>10</Size>\
                   <LastModified>2024-01-01T00:00:00.000Z</LastModified>\
                   <ETag>\"x\"</ETag></Contents></ListBucketResult>";
        let r = ListObjectsV2::parse_response(xml).unwrap();
        assert_eq!(r.contents.len(), 1);
        assert_eq!(r.contents[0].key, "a&b");
        assert_eq!(r.contents[0].size, 10);
    }

    #[test]
    fn retry_classification() {
        assert!(Fail::Transport("reset".into()).retryable());
        assert!(Fail::Status(409, String::new()).retryable());
        assert!(Fail::Status(503, String::new()).retryable());
        assert!(!Fail::Status(412, String::new()).retryable());
        assert!(!Fail::Status(404, String::new()).retryable());
        assert!(!Fail::Status(403, String::new()).retryable());
        assert!(!Fail::Status(301, String::new()).retryable(), "a redirect is a misconfiguration");
        assert!(!Fail::Status(307, String::new()).retryable());
    }

    /// One HTTP exchange served by hand on a local socket, answering with
    /// the given status line and no body, and returning the request line.
    fn serve_once(status_line: &'static str) -> (String, std::thread::JoinHandle<String>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let h = std::thread::spawn(move || {
            use std::io::{BufRead, BufReader, Write};
            let (mut sock, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(sock.try_clone().unwrap());
            let mut request = String::new();
            reader.read_line(&mut request).unwrap();
            // Drain the headers so the client's body write does not fail.
            let mut line = String::new();
            let mut content_length = 0usize;
            loop {
                line.clear();
                reader.read_line(&mut line).unwrap();
                if let Some(v) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                    content_length = v.trim().parse().unwrap_or(0);
                }
                if line == "\r\n" || line.is_empty() {
                    break;
                }
            }
            let mut body = vec![0u8; content_length];
            std::io::Read::read_exact(&mut reader, &mut body).unwrap();
            let reply = format!(
                "HTTP/1.1 {status_line}\r\nLocation: http://127.0.0.1:1/elsewhere\r\n\
                 Content-Length: 0\r\nConnection: close\r\n\r\n"
            );
            sock.write_all(reply.as_bytes()).unwrap();
            request
        });
        (format!("http://{addr}"), h)
    }

    #[test]
    fn a_redirected_put_is_an_error_not_a_write() {
        for status in ["307 Temporary Redirect", "301 Moved Permanently"] {
            let (endpoint, server) = serve_once(status);
            let c = client(&endpoint);
            let err = c.put_if_absent("commit/1", b"x").unwrap_err().to_string();
            let code = &status[..3];
            assert!(err.contains(&format!("s3 status {code}")), "got: {err}");
            let request = server.join().unwrap();
            assert!(request.starts_with("PUT "), "the redirect was not followed: {request}");
        }
    }

    #[test]
    fn a_redirected_get_or_delete_is_an_error_too() {
        let (endpoint, server) = serve_once("302 Found");
        let err = client(&endpoint).get("k").unwrap_err().to_string();
        assert!(err.contains("s3 status 302"), "got: {err}");
        server.join().unwrap();

        let (endpoint, server) = serve_once("307 Temporary Redirect");
        let err = client(&endpoint).delete("k").unwrap_err().to_string();
        assert!(err.contains("s3 status 307"), "got: {err}");
        server.join().unwrap();
    }
}
