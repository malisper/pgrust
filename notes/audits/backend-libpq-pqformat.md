# Audit: backend-libpq-pqformat

- Catalog unit: `backend-libpq-pqformat` — `src/backend/libpq/pqformat.c`, plus
  the `pqformat.h` static-inline send helpers (bundled into this unit per the
  catalog notes).
- C sources: postgres-18.3 `src/backend/libpq/pqformat.c` (641 lines, 26
  function definitions), `src/include/libpq/pqformat.h` (11 static inlines).
  Supporting logic carried into this crate: `enlargeStringInfo` /
  `appendBinaryStringInfo[NT]` / `initStringInfo` semantics from
  `src/common/stringinfo.c` (the pieces pqformat depends on; the catalog's
  stringinfo unit holds the rest), `SET_VARSIZE`/`VARHDRSZ` from `varatt.h`/
  `c.h`, `MaxAllocSize` from `utils/memutils.h`.
- c2rust reference: `c2rust-runs/backend-libpq-pqformat/src/pqformat.rs`
  (26 `pq_*` externs + the four header inlines the file itself instantiates:
  `pq_writeint32`, `pq_writeint64`, `pq_sendint32`, `pq_sendint64` — matches
  the C inventory exactly; no extra statics or build-config branches).
- Ports: `crates/backend-libpq-pqformat/src/lib.rs` (+ `src/tests.rs`),
  `crates/types-stringinfo/src/lib.rs` (type only),
  `crates/backend-libpq-pqformat-seams/src/lib.rs` (inward seam decls),
  `crates/backend-utils-mb-mbutils-seams/src/lib.rs` (outward seam decls,
  owned here until mbutils lands).
- Audit date: 2026-06-12. One fix round (see Findings); verdicts below are for
  the post-fix code, re-derived from scratch.

## Representation notes (apply to many rows)

- `StringInfoData {data,len,maxlen,cursor}` → `StringInfo { data: PgVec<u8>,
  cursor: usize }`; `len` = `data.len()`, `maxlen` = capacity. C's guaranteed
  trailing-NUL sentinel is not stored; the only readers of the sentinel
  (`pq_getmsgstring`/`pq_getmsgrawstring` via `strlen`) are reproduced by an
  explicit NUL scan whose no-NUL case lands on exactly the same
  `cursor + slen >= len` failure as C's strlen-to-the-sentinel (verified
  below). NT vs non-NT appends differ only in sentinel maintenance, so both
  collapse to `append_binary` — observably identical.
- `CurrentMemoryContext` is replaced by an explicit `Mcx` parameter on the
  allocating entry points (`pq_beginmessage`, `pq_begintypsend`,
  `pq_getmsgtext`, `pq_getmsgstring`, `pq_puttextmessage`); growth of an
  existing buffer charges that buffer's own context, which mirrors C's
  repalloc-stays-in-original-context note in stringinfo.c.
- `ereport(ERROR)`/`elog(ERROR)` → `Err(PgError)`. `elog` default sqlstate:
  `PgError::error` defaults to `ERRCODE_INTERNAL_ERROR` (XX000), matching
  elog. palloc failure → `Mcx::oom` = "out of memory" / 53200 / "Failed on
  request of size %zu in memory context \"%s\"." matching mcxt.c.
- Pointer-identity test `p != str` after `pg_server_to_client` /
  `pg_client_to_server` cannot cross a seam; the seam returns
  `Ok(None)` (= same pointer, no conversion) / `Ok(Some(vec))` (= fresh
  palloc'd conversion, length = C's `strlen(p)`). Branch-for-branch identical.

## Function inventory and verdicts

### pqformat.c (26/26)

| C definition (pqformat.c) | Port (lib.rs) | Verdict | Notes |
|---|---|---|---|
| `pq_beginmessage` (:87) | `pq_beginmessage` | MATCH | `initStringInfo` (1024-byte prealloc in caller's context, len 0) then `buf->cursor = msgtype`. Port: `init_string_info(mcx)` reserves 1024 fallibly, `cursor = msgtype`. |
| `pq_beginmessage_reuse` (:108) | `pq_beginmessage_reuse` | MATCH | `resetStringInfo` (clear, cursor=0) then stash msgtype in cursor. |
| `pq_sendbytes` (:125) | `pq_sendbytes` | MATCH | `appendBinaryStringInfo` → `append_binary` (enlarge-with-cap + extend). |
| `pq_sendcountedtext` (:141) | `pq_sendcountedtext` | MATCH | Converted: `slen = strlen(p)`; int32 count then body, `pfree(p)` (drop). Unconverted: count = input `slen`, body verbatim. Count excludes itself; no NUL sent. Both branches verified against c2rust. |
| `pq_sendtext` (:171) | `pq_sendtext` | MATCH | Same two branches, no count, no NUL. |
| `pq_sendstring` (:194) | `pq_sendstring` | MATCH | C appends `slen + 1` bytes (string + its NUL). Port appends bytes then `[0]` — identical output; two appends vs one is unobservable (same cap check semantics: needed=k then 1 vs k+1 — cap fires identically except a 1-byte window at exactly the 1GB boundary where C errors one byte earlier on the combined request; both are the same ereport on the same logical operation, and the boundary state is unreachable through this module's own appends since reaching len = MaxAllocSize−1 is itself rejected. Confirmed equivalent: C fails iff `slen+1 >= Max−len`; port fails iff `slen >= Max−len` or `1 >= Max−(len+slen)` ⇔ `slen+1 >= Max−len`. Exactly equal.) |
| `pq_send_ascii_string` (:226) | `pq_send_ascii_string` | MATCH | `IS_HIGHBIT_SET` (`ch & 0x80`) → `'?'`; per-char append; trailing NUL. |
| `pq_sendfloat4` (:251) | `pq_sendfloat4` | MATCH | Union pun float4→uint32 = `f32::to_bits` (same object representation), then `pq_sendint32`. |
| `pq_sendfloat8` (:275) | `pq_sendfloat8` | MATCH | Union pun = `f64::to_bits`, then `pq_sendint64`. |
| `pq_endmessage` (:295) | `pq_endmessage` | MATCH | `(void) pq_putmessage(cursor, data, len)` — return value ignored (pqcomm already reported COMMERROR); buffer freed (consumed by value, drop = pfree). |
| `pq_endmessage_reuse` (:313) | `pq_endmessage_reuse` | MATCH | Same send, buffer not freed (`&StringInfo`). |
| `pq_begintypsend` (:325) | `pq_begintypsend` | MATCH | initStringInfo + four `'\0'` bytes reserved for the varlena length word. |
| `pq_endtypsend` (:345) | `pq_endtypsend` | MATCH | `Assert(len >= VARHDRSZ)` (port: `assert!`, strictly stronger than C's debug-only Assert; the state is unreachable via `pq_begintypsend`); `SET_VARSIZE(result, buf->len)` = native-endian store of `len << 2` (`varatt.h` non-WORDS_BIGENDIAN `SET_VARSIZE_4B`) → `to_le_bytes` on this LE target; returns `buf->data` as the bytea. |
| `pq_puttextmessage` (:366) | `pq_puttextmessage` | MATCH | Converted: send `strlen(p)+1` bytes (conversion + NUL), pfree, return. Unconverted: send `slen+1` bytes (caller's NUL materialized, since a Rust slice doesn't carry it). Send result discarded in both. |
| `pq_putemptymessage` (:387) | `pq_putemptymessage` | MATCH | `pq_putmessage(msgtype, NULL, 0)` → `&[]`. |
| `pq_getmsgbyte` (:398) | `pq_getmsgbyte` | MATCH | `cursor >= len` → 08P01 "no data left in message"; returns `(unsigned char)data[cursor++]` as int (0..=255). |
| `pq_getmsgint` (:414) | `pq_getmsgint` | MATCH | b=1: raw byte; b=2: `pg_ntoh16` = `u16::from_be_bytes`; b=4: `pg_ntoh32`; widths via `pq_copymsgbytes`; default: `elog(ERROR, "unsupported integer size %d")` → XX000, exact message. |
| `pq_getmsgint64` (:452) | `pq_getmsgint64` | MATCH | 8 bytes via copymsgbytes, `pg_ntoh64` → `i64::from_be_bytes`. |
| `pq_getmsgfloat4` (:468) | `pq_getmsgfloat4` | MATCH | `swap.i = pq_getmsgint(msg,4)` → `f32::from_bits`. |
| `pq_getmsgfloat8` (:487) | `pq_getmsgfloat8` | MATCH | `swap.i = pq_getmsgint64(msg)` → `f64::from_bits(.. as u64)` (same bits). |
| `pq_getmsgbytes` (:507) | `pq_getmsgbytes` | MATCH | `datalen < 0 \|\| datalen > len - cursor` → 08P01 "insufficient data left in message". `usize` makes `< 0` unrepresentable; `saturating_sub` reproduces C's signed `len - cursor` for the (caller-corrupted) `cursor > len` state (fixed in round 1). Returns borrow into the buffer; cursor += datalen. |
| `pq_copymsgbytes` (:527) | `pq_copymsgbytes` | MATCH | Same check (same fix); memcpy into caller buffer of length `datalen` (= `buf.len()`; every C call site passes `sizeof(dest)`); cursor += datalen. |
| `pq_getmsgtext` (:545) | `pq_getmsgtext` | MATCH | Same bounds check (same fix); cursor advances *before* conversion (so a conversion error leaves the cursor advanced, as in C); converted: result = conversion, `*nbytes = strlen(p)` = vec len; unconverted: fresh `palloc(rawbytes+1)` copy + NUL, `*nbytes = rawbytes` → `slice_in` copy of len rawbytes (NUL is sentinel-only, dropped per representation). |
| `pq_getmsgstring` (:578) | `pq_getmsgstring` | MATCH | `scan_cstring_len`: NUL found at offset k ⇒ slen=k, and `cursor+k >= len` impossible (NUL is inside data), so success — identical to C; no NUL ⇒ slen=len−cursor ⇒ `cursor+slen >= len` ⇒ 08P01 "invalid string in message" — exactly C's strlen-hits-the-sentinel case. cursor += slen+1; then `pg_client_to_server` (after advancing, as in C); returns buffer borrow (`p == str`) or conversion (`p != str`) via `PqString`. |
| `pq_getmsgrawstring` (:607) | `pq_getmsgrawstring` | MATCH | Same scan/check/advance, no conversion, borrow into buffer. |
| `pq_getmsgend` (:634) | `pq_getmsgend` | MATCH | `cursor != len` → 08P01 "invalid message format". |

### pqformat.h static inlines (11/11)

| C definition (pqformat.h) | Port (lib.rs) | Verdict | Notes |
|---|---|---|---|
| `pq_writeint8` (:45) | `pq_writeint8` | MATCH | C: Assert-space + memcpy + len++. Over `PgVec` the prealloc/write split is a C micro-optimization with no observable difference; port appends fallibly. |
| `pq_writeint16` (:59) | `pq_writeint16` | MATCH | `pg_hton16` = `to_be_bytes`. |
| `pq_writeint32` (:73) | `pq_writeint32` | MATCH | `pg_hton32` = `to_be_bytes` (cross-checked against c2rust `swap_bytes` on LE). |
| `pq_writeint64` (:87) | `pq_writeint64` | MATCH | `pg_hton64` = `to_be_bytes`. |
| `pq_writestring` (:107) | `pq_writestring` | MATCH | Same conversion branch + `slen+1` copy + conditional pfree as `pq_sendstring`; delegates to it (Assert-space again unobservable). |
| `pq_sendint8` (:127) | `pq_sendint8` | MATCH | enlarge + write = fallible append. |
| `pq_sendint16` (:135) | `pq_sendint16` | MATCH | ditto. |
| `pq_sendint32` (:143) | `pq_sendint32` | MATCH | ditto. |
| `pq_sendint64` (:151) | `pq_sendint64` | MATCH | ditto. |
| `pq_sendbyte` (:159) | `pq_sendbyte` | MATCH | = `pq_sendint8`. |
| `pq_sendint` (:170) | `pq_sendint` | MATCH | b ∈ {1,2,4} dispatch with truncating casts (`(uint8) i`, `(uint16) i` = `as u8`/`as u16`); default `elog(ERROR, "unsupported integer size %d")` → XX000, exact message. |

### Carried stringinfo.c pieces

| C definition | Port | Verdict | Notes |
|---|---|---|---|
| `enlargeStringInfo` (stringinfo.c:337) | `enlarge_string_info` | MATCH (after fix) | `needed < 0` unrepresentable (usize). Cap: `needed >= MaxAllocSize − len` (MaxAllocSize verified against memutils.h = `0x3fffffff`) → 54000, errmsg "string buffer exceeds maximum allowed length (1073741823 bytes)" (`%zu` of MaxAllocSize), errdetail "Cannot enlarge string buffer containing %d bytes by %d more bytes." — all three now exact for PG 18.3. Doubling/repalloc growth strategy is allocation policy, not observable; OOM → mcxt.c-shaped error via `Mcx::oom`. |
| `appendBinaryStringInfo` / `appendBinaryStringInfoNT` (stringinfo.c) | `append_binary` | MATCH | enlarge(datalen) + memcpy + len update; NT distinction is sentinel-only (not stored). |
| `initStringInfo` / `resetStringInfo` (stringinfo.c) | `init_string_info` / `StringInfo::reset` | MATCH | 1024-byte initial alloc (fallible), len 0, cursor 0 / clear keeping allocation, cursor 0. |

### types-stringinfo

Type-only crate (struct + trivial accessors/reset/from_vec/into_vec); no unit
logic. Field mapping verified against `lib/stringinfo.h` (`data`/`len`/
`maxlen`/`cursor`); sentinel difference documented and compensated as audited
above. No findings.

## Seam audit

Inward (`crates/backend-libpq-pqformat-seams`): 4 declarations —
`pq_beginmessage`, `pq_sendint32`, `pq_sendint64`, `pq_endmessage` (consumers:
backend-utils-activity-small per its catalog row). All 4 — and nothing else —
are installed by `backend_libpq_pqformat::init_seams()`, which contains only
`set()` calls; `seams-init::init_all()` calls it (seams-init/src/lib.rs:13).
No `set()` of these seams anywhere outside the owner (fixture installs in the
crate's own `#[cfg(test)]` module only). Seam signatures are C-faithful
StringInfo-passing shapes; implementations are the crate's real functions, no
marshalling logic in the seam path.

Outward:
- `backend-libpq-pqcomm-seams::pq_putmessage` — pqcomm.c is not yet merged;
  direct dependency impossible. Call sites (`pq_endmessage`,
  `pq_endmessage_reuse`, `pq_puttextmessage`, `pq_putemptymessage`) are one
  call with the return value discarded, exactly as C's `(void)` casts. Thin.
- `backend-utils-mb-mbutils-seams::{pg_server_to_client, pg_client_to_server}`
  — mbutils.c not ported; declarations live in their owner-named crate and
  will be installed by that unit's `init_seams()` when it lands (uninstalled
  calls panic loudly, which is the accepted unported-callee behavior). The
  `Option` encoding of C's pointer-identity protocol is pure marshalling; all
  branching on the result lives in this crate, mirroring the C `if (p != str)`
  branches. Thin.

No seam findings.

## Findings and fix round

Round 1 (commit on this branch):

1. **DIVERGES (fixed)** — `enlarge_string_info` raised errmsg "out of memory"
   for the MaxAllocSize cap. That is the pre-PG17 wording; PostgreSQL 18.3's
   `enlargeStringInfo` raises `errmsg("string buffer exceeds maximum allowed
   length (%zu bytes)", MaxAllocSize)` (stringinfo.c:362). Message corrected
   (sqlstate 54000 and errdetail were already exact); test updated.
2. **Minor divergence (fixed)** — `pq_getmsgbytes`, `pq_copymsgbytes`,
   `pq_getmsgtext` computed `len - cursor` with unsigned arithmetic, which
   panics (debug) instead of raising 08P01 if a caller ever sets
   `cursor > len`; C's signed `msg->len - msg->cursor` goes negative and
   raises the protocol error. Replaced with `saturating_sub`, restoring C's
   behavior on that degenerate state.

Both fixes re-audited from scratch against stringinfo.c/pqformat.c above;
full workspace test suite green (pqformat: 18/18).

## Verdict

**PASS** — 26/26 pqformat.c functions, 11/11 pqformat.h inlines, and the
carried stringinfo helpers all MATCH; seam wiring clean.
