#!/usr/bin/env python3
"""Counts, lists, creates and empties the test bucket.

The shell tests need to see the bucket from outside the server -- how many
objects there are, and whether collection removed any. MinIO refuses
unauthenticated listing, so this signs the request itself rather than pulling
in a dependency for six lines of HMAC.

  bucket.py mk                 create the bucket if it does not exist
  bucket.py count [prefix]     how many objects
  bucket.py bytes [prefix]     how many bytes
  bucket.py ls    [prefix]     the keys
  bucket.py rm    [prefix]     delete them
"""
import datetime, hashlib, hmac, os, sys, urllib.error, urllib.parse, urllib.request
import xml.etree.ElementTree as ET

ENDPOINT = os.environ.get("OBJKV_S3_ENDPOINT", "http://127.0.0.1:9000")
_url = urllib.parse.urlparse(ENDPOINT)
HOST = _url.netloc

# `rm` issues signed deletes against whatever the listing returns, so plaintext
# to anywhere but this machine lets whoever is in the middle pick the keys.
if _url.scheme != "https" and (_url.hostname or "") not in ("127.0.0.1", "::1", "localhost"):
    if not os.environ.get("OBJKV_S3_INSECURE"):
        sys.exit(f"bucket.py: refusing plaintext to {HOST}; use https:// or set OBJKV_S3_INSECURE=1")
BUCKET = os.environ.get("OBJKV_S3_BUCKET", "objkv")
KEY = os.environ.get("OBJKV_S3_KEY", "minioadmin")
SECRET = os.environ.get("OBJKV_S3_SECRET", "minioadmin")
REGION = os.environ.get("OBJKV_S3_REGION", "us-east-1")


def signed(method, path, query):
    now = datetime.datetime.now(datetime.timezone.utc)
    amz, day = now.strftime("%Y%m%dT%H%M%SZ"), now.strftime("%Y%m%d")
    empty = hashlib.sha256(b"").hexdigest()
    canonical = (
        f"{method}\n{path}\n{query}\nhost:{HOST}\nx-amz-content-sha256:{empty}\n"
        f"x-amz-date:{amz}\n\nhost;x-amz-content-sha256;x-amz-date\n{empty}"
    )
    scope = f"{day}/{REGION}/s3/aws4_request"
    to_sign = f"AWS4-HMAC-SHA256\n{amz}\n{scope}\n{hashlib.sha256(canonical.encode()).hexdigest()}"
    k = ("AWS4" + SECRET).encode()
    for part in (day, REGION, "s3", "aws4_request"):
        k = hmac.new(k, part.encode(), hashlib.sha256).digest()
    sig = hmac.new(k, to_sign.encode(), hashlib.sha256).hexdigest()
    url = f"{ENDPOINT}{path}" + (f"?{query}" if query else "")
    req = urllib.request.Request(
        url,
        method=method,
        headers={
            "Authorization": (
                f"AWS4-HMAC-SHA256 Credential={KEY}/{scope}, "
                "SignedHeaders=host;x-amz-content-sha256;x-amz-date, "
                f"Signature={sig}"
            ),
            "x-amz-content-sha256": empty,
            "x-amz-date": amz,
        },
    )
    return NO_REDIRECT.open(req, timeout=30).read()


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    # A signed request must not be replayed at another origin: the default
    # opener would forward the Authorization header along with it.
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise urllib.error.HTTPError(req.full_url, code, "redirect refused", headers, fp)


NO_REDIRECT = urllib.request.build_opener(_NoRedirect)


def objects(prefix):
    out, token = [], None
    while True:
        parts = {"list-type": "2"}
        if prefix:
            parts["prefix"] = prefix
        if token:
            parts["continuation-token"] = token
        query = "&".join(
            f"{k}={urllib.parse.quote(v, safe='')}" for k, v in sorted(parts.items())
        )
        root = ET.fromstring(signed("GET", f"/{BUCKET}", query))
        ns = {"s": root.tag.split("}")[0].strip("{")}
        for c in root.findall("s:Contents", ns):
            out.append((c.find("s:Key", ns).text, int(c.find("s:Size", ns).text)))
        nxt = root.find("s:NextContinuationToken", ns)
        if nxt is None or not nxt.text:
            return out
        token = nxt.text


def make_bucket():
    try:
        signed("PUT", f"/{BUCKET}", "")
    except urllib.error.HTTPError as e:
        # 409: BucketAlreadyOwnedByYou / BucketAlreadyExists.
        if e.code != 409:
            raise
    print(BUCKET)


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "count"
    if cmd == "mk":
        make_bucket()
        sys.exit(0)
    found = objects(sys.argv[2] if len(sys.argv) > 2 else "")
    if cmd == "count":
        print(len(found))
    elif cmd == "bytes":
        print(sum(size for _, size in found))
    elif cmd == "rm":
        for key, _ in found:
            signed("DELETE", f"/{BUCKET}/{urllib.parse.quote(key)}", "")
        print(len(found))
    else:
        for key, size in found:
            print(f"{size:>10}  {key}")
