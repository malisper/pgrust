# Audit: backend-utils-adt-uuid (uuid.c)

C source: `../pgrust/postgres-18.3/src/backend/utils/adt/uuid.c` (784 lines).
Crate: `crates/backend-utils-adt-uuid`. New vocab crate `crates/types-uuid`
(`pg_uuid_t`, `UUID_LEN`, `uuid_sortsupport_state`).

## Function-by-function vs C

| C function (line) | Port | Verdict |
|---|---|---|
| `uuid_in` (77) | `uuid_in(&[u8], Option<&mut SoftErrorContext>)` | OK — palloc'd value returned owned; escontext threaded to `string_to_uuid`. |
| `uuid_out` (88) | `uuid_out(&pg_uuid_t) -> Vec<u8>` | OK — fixed `2*UUID_LEN+5` buffer, hyphens before bytes 4/6/8/10, lowercase hex, trailing NUL retained (matches C cstring). |
| `string_to_uuid` (130) | `string_to_uuid` (private) | OK — `{}` braces, optional dash after each group of 4 (`(i%2)==1 && i<UUID_LEN-1`), isxdigit checks, NUL-as-cstring-terminator via `byte_at`. `ereturn` -> `types_error::ereturn`; message `invalid input syntax for type uuid: "%s"`, SQLSTATE 22P02. Verified soft path saves, hard path errs (tests). |
| `uuid_recv` (180) | `uuid_recv(&mut StringInfo)` | OK — `pq_getmsgbytes(buffer, UUID_LEN)` (direct dep), copy 16 bytes. |
| `uuid_send` (191) | `uuid_send(Mcx, &pg_uuid_t) -> Bytea` | OK — `pq_begintypsend`/`pq_sendbytes`/`pq_endtypsend` (direct dep); returns the bytea body. Takes Mcx (C allocates the buffer in the current context). |
| `uuid_internal_cmp` (203) | `uuid_internal_cmp` | OK — memcmp returning signed byte difference (NOT normalized), verified `0xff` case in tests. |
| `uuid_lt/le/eq/ge/gt/ne` (209-261) | each | OK — `cmp < / <= / == / >= / > / != 0`. |
| `uuid_cmp` (264) | `uuid_cmp` | OK — raw internal cmp (int32). |
| `uuid_sortsupport` (276) | `uuid_sortsupport(Mcx, abbreviate) -> UuidSortSupport` | OK — returns decision struct (varlena precedent); when abbreviating builds `uuid_sortsupport_state{input_count:0, estimating:true, abbr_card: initHyperLogLog(_,10)}`. The function-pointer install into the trimmed node is the substrate's; comparator = uuid_fast_cmp (non-abbrev) / ssup_datum_unsigned_cmp + hooks (abbrev) documented on the struct. |
| `uuid_fast_cmp` (312) | `uuid_fast_cmp` | OK — `uuid_internal_cmp`. |
| `uuid_abbrev_abort` (327) | `uuid_abbrev_abort(i32, &mut state) -> bool` | OK — `<10000 \|\| input_count<10000 \|\| !estimating` -> false; estimate>100k -> estimating=false,false; estimate < input_count/2000.0+0.5 -> true; else false. `trace_sort` LOG elogs ELIDED (pure diagnostic; identical posture to merged `numeric_abbrev_abort`). |
| `uuid_abbrev_convert` (387) | `uuid_abbrev_convert(&pg_uuid_t, &mut state) -> usize` | OK — memcpy first sizeof(Datum) bytes; input_count+=1; estimating: tmp = lo^hi (SIZEOF_DATUM==8) fed to `hash_bytes_uint32` (=hash_uint32) into HLL; `DatumBigEndianToNative` = byteswap on little-endian. |
| `uuid_decrement` (423) | `uuid_decrement(&mut [u8;16]) -> bool` | OK — big-endian -1 from byte 15; underflow -> all 0xFF + true (C: discards copy, returns 0 + *underflow). In-place is the C returned-copy bytes. |
| `uuid_increment` (448) | `uuid_increment(&mut [u8;16]) -> bool` | OK — symmetric; overflow -> all 0x00 + true. |
| `uuid_skipsupport` (473) | `uuid_skipsupport() -> UuidSkipSupport` | OK — low_elem all 0x00, high_elem all 0xFF; increment/decrement kernels named on the struct. Substrate installs (token model). |
| `uuid_hash` (492) | `uuid_hash` | OK — `hash_bytes(&data)` (=hash_any). |
| `uuid_hash_extended` (500) | `uuid_hash_extended` | OK — `hash_bytes_extended(&data, seed)`. |
| `uuid_set_version` (511) | private | OK — byte6 = (b6&0x0f)\|(v<<4); byte8 = (b8&0x3f)\|0x80. Tested. |
| `gen_random_uuid` (527) | `gen_random_uuid` | OK — `pg_strong_random` (seam) over all 16; ERRCODE_INTERNAL_ERROR "could not generate random values"; set_version(4). Also serves `uuidv4` (same prosrc). |
| `get_real_time_ns_ascending` (551) | private | OK — clock read via `clock_realtime_ns` seam (combined ns); `static int64 previous_ns` -> thread_local Cell; minimal-step monotonic advance. |
| `generate_uuidv7` (604) | private | OK — 48-bit BE ms in bytes 0..6; `increased_clock_precision = (sub_ms*(1<<SUBMS_BITS))/NS_PER_MS` (wrapping mul, i64 div); bytes 6/7; rand bytes 8..16 via seam; SUBMS_MINIMAL_STEP_BITS==10 -> `data[7] ^= data[8]>>6`; set_version(7). |
| `uuidv7` (658) | `uuidv7` | OK — ns/NS_PER_MS, ns%NS_PER_MS. |
| `uuidv7_interval` (670) | `uuidv7_interval(&Interval)` | OK — ns->TimestampTz (Postgres-epoch us), `timestamptz_pl_interval` seam, back to UNIX us, generate_uuidv7(us/US_PER_MS, (us%US_PER_MS)*NS_PER_US + ns%NS_PER_US). |
| `uuid_extract_timestamp` (714) | `uuid_extract_timestamp -> Option<TimestampTz>` | OK — variant check `(d8&0xc0)!=0x80` -> None; v1: 60-bit tms (byte6 masked 0xf), /10 - (PG_EPOCH-GREGORIAN)*SECS_PER_DAY*USECS_PER_SEC; v7: 48-bit tms, *US_PER_MS - (PG_EPOCH-UNIX)*...; else None. Wrapping adds match C uint64; signedness note documented. |
| `uuid_extract_version` (770) | `uuid_extract_version -> Option<i16>` | OK — variant check -> None; version nibble as uint16 (returned i16 carries the bit pattern; PG_RETURN_UINT16). |

## Notes / divergences
- `uuid` typmod / array I/O are not in uuid.c.
- Bare-word PGFunction registry (`uuidv4`/`uuid_in`/... fmgr builtins) deferred
  project-wide; no fmgr_boundary in this port.
- SortSupport/SkipSupport node-mutation: trimmed `SortSupportData`/`SkipSupportData`
  carry opaque substrate tokens (not C function pointers), so strategy routines
  return decision structs for the substrate to install — the established
  `varstr_sortsupport`/`numeric_sortsupport` model. Wiring uuid into the substrate's
  `run_sortsupport` OID dispatch + per-type install seams is a separate substrate
  task; the kernels here are complete and pure.
- Outward seams (owners unported): `port_pg_strong_random_seams::{pg_strong_random,
  clock_realtime_ns}`, `backend_utils_adt_timestamp_seams::timestamptz_pl_interval`.
- `trace_sort` LOG elogs elided (diagnostic; numeric precedent).

## Tests
12 unit tests: in/out roundtrip, relaxed forms, hard + soft syntax errors,
comparisons (incl. non-normalized memcmp), hash parity, v7 extract, variant/v4
None paths, increment/decrement inverse+wrap, skipsupport boundaries, set_version.
All pass. no-todo-guard + seams-init guards green; `cargo check --workspace` green.
