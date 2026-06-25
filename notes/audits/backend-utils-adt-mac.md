# Audit: backend-utils-adt-mac (`src/backend/utils/adt/mac.c`)

Function-by-function comparison against postgres-18.3 `mac.c` and the
src-idiomatic port. Crate carves `mac.c` out of the combined
`backend-utils-adt-more-scalars*` catalog units as a standalone owner;
`mac8.c` stays a separate unit (untouched).

## Model

- `macaddr` value: `types_network::macaddr` (six `u8` fields a..f), matching
  `utils/inet.h`.
- fmgr/Datum/varlena/StringInfo envelope is the project-wide systemic deferral
  (same as the sibling `backend-utils-adt-network`): `macaddr_in` takes input
  text `&[u8]`, `macaddr_out` returns cstring `Vec<u8>`, `macaddr_recv` takes
  the raw external binary body `&[u8]`, `macaddr_send` returns the `bytea`
  payload bytes.
- Errors → `types_error::PgError` with exact SQLSTATE; soft-error path via
  `ereturn(escontext, None, error)`.
- Hashing calls in-repo `common_hashfn::hash_bytes` / `hash_bytes_extended`
  directly (owner ported, no cycle).

## Enumeration (every C function)

| C function | mac.c | Port | Status |
|---|---|---|---|
| `macaddr_in` | 55 | `macaddr_in` | Ported. Seven `sscanf` notations tried in C order via a faithful `Scanner` (`%x`/`%2x` glibc grammar: leading-ws skip, optional sign, optional `0x`/`0X` prefix with rollback, width-bounded hex run; trailing `%1s` junk rejection). Range check 0..=255 per octet. Both ereports preserved: 22P02 `invalid input syntax for type macaddr: "..."`, 22003 `invalid octet value in "macaddr" value: "..."`. Soft-error → `Ok(None)`. |
| `macaddr_out` | 121 | `macaddr_out` | Ported. `"%02x:%02x:%02x:%02x:%02x:%02x"`, lowercase zero-padded. |
| `macaddr_recv` | 140 | `macaddr_recv` | Ported. Six MSB-first bytes via a forward `MsgCursor` mirroring `pq_getmsgbyte`; past-end → `insufficient data left in message` (22P03), matching the C `pq_getmsgbyte` failure surface. |
| `macaddr_send` | 161 | `macaddr_send` | Ported. Body = the six address bytes in order (the `pq_begintypsend`/`pq_sendbyte`×6/`pq_endtypsend` framing yields exactly these bytes). |
| `macaddr_cmp_internal` | 181 | `macaddr_cmp_internal` | Ported. `hibits`/`lobits` three-way compare, identical branch order, returns -1/0/1. |
| `macaddr_cmp` | 197 | `macaddr_cmp` | Ported. |
| `macaddr_lt`/`le`/`eq`/`ge`/`gt`/`ne` | 210/219/228/237/246/255 | same | Ported, each `cmp_internal <op> 0`. |
| `hashmacaddr` | 267 | `hashmacaddr` | Ported. `hash_bytes` over the 6-byte image (== C `hash_any(key, sizeof(macaddr))`). `sizeof(macaddr)`=6 (six `u8`, no padding). |
| `hashmacaddrextended` | 275 | `hashmacaddrextended` | Ported. `hash_bytes_extended(image, seed)`. |
| `macaddr_not` | 287 | `macaddr_not` | Ported, bitwise `~` each field. |
| `macaddr_and` | 303 | `macaddr_and` | Ported, `&` each field. |
| `macaddr_or` | 320 | `macaddr_or` | Ported, `|` each field. |
| `macaddr_trunc` | 341 | `macaddr_trunc` | Ported, a/b/c kept, d/e/f zeroed. |
| `macaddr_sortsupport` | 363 | `macaddr_sortsupport` | Node mutation (install comparators, alloc `macaddr_sortsupport_state` in `ssup_cxt`, `initHyperLogLog(&abbr_card, 10)`, install `ssup_datum_unsigned_cmp`/abbrev hooks) is delegated to the outward `backend_utils_adt_mac_seams::sortsupport::register` seam — owned by the unported tuplesort / `lib/hyperloglog` substrate. Identical model to `network_sortsupport`. Default uninstalled = faithful no-op (btree falls back to `macaddr_cmp`). |
| `macaddr_fast_cmp` (static) | 399 | `macaddr_fast_cmp` | Ported (pure): `macaddr_cmp_internal` on the unpacked args. |
| `macaddr_abbrev_abort` (static) | 415 | (behind register seam) | The cost model + `estimateHyperLogLog` + `trace_sort` LOG lines live in the abbreviation substrate; delegated through `register`, same as `network_abbrev_abort`. Constants in the C body (10000 memtup/input floor, 100000.0 cardinality ceiling, `input_count/2000.0 + 0.5` threshold) belong to the owner's install. |
| `macaddr_abbrev_convert` (static) | 477 | `macaddr_abbrev_convert_bits` | Pure key-packing ported exactly: zero an 8-byte datum (or copy `SIZEOF_DATUM` bytes on 32-bit), `memcpy` the 6 bytes low, `DatumBigEndianToNative` (byteswap on little-endian). The HLL `addHyperLogLog` + `input_count`/`estimating` bookkeeping is the side effect owned by the register seam (same split as network). |

## Constants / parity

- SQLSTATEs: 22P02 (`ERRCODE_INVALID_TEXT_REPRESENTATION`), 22003
  (`ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE`), 22P03
  (`ERRCODE_INVALID_BINARY_REPRESENTATION`) — verified against
  `types-error/src/error.rs`.
- Message text matches mac.c verbatim (incl. the `"macaddr"` literal arg).
- `SIZEOF_DATUM` = `size_of::<usize>()`; abbrev `tmp` XOR-fold and byteswap
  branch on it exactly as the C `#if SIZEOF_DATUM == 8`.

## Seams

- Outward: `backend-utils-adt-mac-seams::sortsupport::register() -> bool`
  (installed by the unported tuplesort/hyperloglog owner; uninstalled =
  loud-panic-free no-op, like network's `register`).
- Inward: none. `init_seams()` is empty (the crate installs nothing); not added
  to `seams-init` (same as `backend-utils-adt-network`).

## Tests

18 unit tests: all seven input notations, trailing-junk / too-few / bad-syntax
rejections, out-of-range octet, trailing-whitespace acceptance, soft-error
escontext, glibc `0x` prefix accept + rollback-reject, direct `scan_hex`
grammar coverage, comparison ordering across hi/lo halves, hashing == raw-byte
`hash_bytes`, bitwise and/or/not, trunc, fast_cmp == cmp_internal,
abbrev_convert_bits pack+byteswap, recv (full + short-message error), send.

## Verdict

PASS. Every `mac.c` function accounted for; the abbreviation
abort/convert-side-effect/sortsupport-node-mutation split is the same faithful
owner-seam model the audited sibling `backend-utils-adt-network` uses.
