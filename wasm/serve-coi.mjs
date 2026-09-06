#!/usr/bin/env node
// serve-coi.mjs — serve.mjs plus CROSS-ORIGIN ISOLATION.
//
// Verbatim copy of wasm/serve.mjs with one addition: every response carries
//   Cross-Origin-Opener-Policy: same-origin
//   Cross-Origin-Embedder-Policy: require-corp
// which is what makes `self.crossOriginIsolated === true`, which is what makes
// SharedArrayBuffer (and therefore the whole wasm32-wasip1-threads story:
// shared linear memory + Atomics.wait) available to the page and its workers.
// The shipped demo (serve.mjs) deliberately does NOT set these — the
// single-threaded JSPI build must keep working without isolation — so this is
// a separate file rather than a flag on that one.
//
// Usage: node serve-coi.mjs [port]  (default 8081), then open
//        http://localhost:8081/threads.html
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const port = Number(process.argv[2]) || 8081;

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.mjs': 'text/javascript; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.wasm': 'application/wasm',
  '.img': 'application/octet-stream',
};

function accepts(req, encoding) {
  return (req.headers['accept-encoding'] || '')
    .split(',')
    .map((v) => v.trim().split(';')[0])
    .includes(encoding);
}

function encodedVariant(req, filePath, forcedEncoding) {
  if (req.headers.range) return null;
  if (forcedEncoding === 'raw') return null;
  if (forcedEncoding === 'br' && fs.existsSync(filePath + '.br')) return { path: filePath + '.br', encoding: 'br' };
  if (forcedEncoding === 'gzip' && fs.existsSync(filePath + '.gz')) return { path: filePath + '.gz', encoding: 'gzip' };
  if (accepts(req, 'br') && fs.existsSync(filePath + '.br')) return { path: filePath + '.br', encoding: 'br' };
  if (accepts(req, 'gzip') && fs.existsSync(filePath + '.gz')) return { path: filePath + '.gz', encoding: 'gzip' };
  return null;
}

function cacheHeaders(st) {
  return {
    'ETag': `"${st.size.toString(16)}-${Math.floor(st.mtimeMs).toString(16)}"`,
    'Last-Modified': st.mtime.toUTCString(),
  };
}

function isNotModified(req, etag) {
  return (req.headers['if-none-match'] || '').split(',').map((v) => v.trim()).includes(etag);
}

http.createServer((req, res) => {
  let urlPath = decodeURIComponent(req.url.split('?')[0]);
  if (urlPath === '/') urlPath = '/index.html';
  let forcedEncoding = null;
  const forced = /^\/(raw|gzip|br)\//.exec(urlPath);
  if (forced) {
    forcedEncoding = forced[1];
    urlPath = urlPath.slice(forced[0].length - 1);
  }
  const filePath = path.join(here, urlPath);
  if (!filePath.startsWith(here)) { res.writeHead(403); return res.end('forbidden'); }
  fs.stat(filePath, (err, st) => {
    if (err || !st.isFile()) {
      res.writeHead(404, {
        'Cross-Origin-Opener-Policy': 'same-origin',
        'Cross-Origin-Embedder-Policy': 'require-corp',
      });
      return res.end('not found');
    }
    const ext = path.extname(filePath).toLowerCase();
    const headers = {
      'Content-Type': MIME[ext] || 'application/octet-stream',
      'Cache-Control': forcedEncoding === 'raw' ? 'no-cache, no-transform' : 'no-cache',
      'Vary': 'Accept-Encoding',
      // The cross-origin-isolation pair — the whole point of this file.
      'Cross-Origin-Opener-Policy': 'same-origin',
      'Cross-Origin-Embedder-Policy': 'require-corp',
      'Cross-Origin-Resource-Policy': 'same-origin',
    };
    const variant = encodedVariant(req, filePath, forcedEncoding);
    if (variant) {
      const encSt = fs.statSync(variant.path);
      headers['Content-Encoding'] = variant.encoding;
      headers['Content-Length'] = encSt.size;
      Object.assign(headers, cacheHeaders(encSt));
      if (isNotModified(req, headers['ETag'])) {
        delete headers['Content-Length'];
        res.writeHead(304, headers);
        return res.end();
      }
      res.writeHead(200, headers);
      if (req.method === 'HEAD') return res.end();
      fs.createReadStream(variant.path).pipe(res);
      return;
    }
    const range = req.headers.range;
    if (range) {
      const m = /bytes=(\d*)-(\d*)/.exec(range);
      let start = m[1] ? parseInt(m[1], 10) : 0;
      let end = m[2] ? parseInt(m[2], 10) : st.size - 1;
      if (start > end || end >= st.size) end = st.size - 1;
      headers['Content-Range'] = `bytes ${start}-${end}/${st.size}`;
      headers['Accept-Ranges'] = 'bytes';
      headers['Content-Length'] = end - start + 1;
      Object.assign(headers, cacheHeaders(st));
      res.writeHead(206, headers);
      if (req.method === 'HEAD') return res.end();
      fs.createReadStream(filePath, { start, end }).pipe(res);
    } else {
      headers['Content-Length'] = st.size;
      headers['Accept-Ranges'] = 'bytes';
      Object.assign(headers, cacheHeaders(st));
      if (isNotModified(req, headers['ETag'])) {
        delete headers['Content-Length'];
        res.writeHead(304, headers);
        return res.end();
      }
      res.writeHead(200, headers);
      if (req.method === 'HEAD') return res.end();
      fs.createReadStream(filePath).pipe(res);
    }
  });
}).listen(port, () => {
  console.error(`pgrust wasm webapp (cross-origin isolated): http://localhost:${port}/threads.html`);
});
