# Audit: backend-utils-adt-pseudotypes

C source: `src/backend/utils/adt/pseudotypes.c` (PostgreSQL 18.3).
c2rust reference: `../pgrust/c2rust-runs` (pseudotypes unit).
Verdict: PASS.

## Scope

The I/O functions for the system pseudo-types. Most are macro-generated
`ereport(ERROR)` stubs (`PSEUDOTYPE_DUMMY_*`); those error returns ARE the
implementation and are ported as real `PgError` returns with
`ERRCODE_FEATURE_NOT_SUPPORTED` and the exact C message text. `cstring`/`void`
carry full working I/O; the `any*_out`/`*_send`/`pg_node_tree_*` delegators
forward to real type I/O (`return target(fcinfo)` one-liners).

## Function-by-function

Macro expansions: `PSEUDOTYPE_DUMMY_INPUT_FUNC(X)` -> `X_in`;
`PSEUDOTYPE_DUMMY_RECEIVE_FUNC(X)` -> `X_recv`;
`PSEUDOTYPE_DUMMY_IO_FUNCS(X)` -> `X_in` + `X_out`;
`PSEUDOTYPE_DUMMY_BINARY_IO_FUNCS(X)` -> `X_recv` + `X_send`.

| C function | message / behavior | ported |
|---|---|---|
| cstring_in | pstrdup(str) -> cstring | `cstring_in` echoes via PgString::from_str_in | ✓ |
| cstring_out | pstrdup(str) | `cstring_out` | ✓ |
| cstring_recv | pq_getmsgtext(remaining) | `cstring_recv` (buf.len-buf.cursor) | ✓ |
| cstring_send | pq_begintypsend/sendtext/endtypsend | `cstring_send` | ✓ |
| anyarray_in | "cannot accept a value of type anyarray" | ✓ |
| anyarray_recv | "cannot accept ... anyarray" | ✓ |
| anyarray_out | return array_out(fcinfo) | direct dep arrayfuncs::io::array_out | ✓ |
| anyarray_send | return array_send(fcinfo) | arrayfuncs::io::array_send | ✓ |
| anycompatiblearray_{in,recv,out,send} | as anyarray | ✓ |
| anyenum_in | "... anyenum" | ✓ |
| anyenum_out | return enum_out(fcinfo) | direct dep backend-utils-adt-enum::enum_out | ✓ |
| anyrange_in | "... anyrange" | ✓ |
| anyrange_out | return range_out(fcinfo) | rangetypes-seams range_out | ✓ |
| anycompatiblerange_{in,out} | as anyrange | ✓ |
| anymultirange_in | "... anymultirange" | ✓ |
| anymultirange_out | return multirange_out(fcinfo) | multirangetypes::typcache_io::multirange_out | ✓ |
| anycompatiblemultirange_{in,out} | as anymultirange | ✓ |
| void_in | PG_RETURN_VOID -> null Datum | ✓ |
| void_out | pstrdup("") -> empty cstring | ✓ |
| void_recv | PG_RETURN_VOID | ✓ |
| void_send | empty typsend -> empty bytea | ✓ |
| shell_in | "cannot accept a value of a shell type" | ✓ |
| shell_out | "cannot display a value of a shell type" | ✓ |
| pg_node_tree_in | "... pg_node_tree" (accept) | ✓ |
| pg_node_tree_recv | "... pg_node_tree" (accept) | ✓ |
| pg_node_tree_out | return textout(fcinfo) | varlena::wire_io::textout | ✓ |
| pg_node_tree_send | return textsend(fcinfo) | varlena::wire_io::textsend | ✓ |
| pg_ddl_command_{in,recv} | "cannot accept ... pg_ddl_command" | ✓ |
| pg_ddl_command_{out,send} | "cannot display ... pg_ddl_command" | ✓ |
| any/trigger/event_trigger/language_handler/fdw_handler/table_am_handler/index_am_handler/tsm_handler/internal/anyelement/anynonarray/anycompatible/anycompatiblenonarray _in/_out | DUMMY_IO_FUNCS: in="cannot accept", out="cannot display" | `dummy_io!` macro, one per typname | ✓ |

## Parity notes

- SQLSTATE: every dummy carries `ERRCODE_FEATURE_NOT_SUPPORTED` (`0A000`),
  matching C.
- Message text byte-for-byte: "cannot accept a value of type %s",
  "cannot display a value of type %s", "cannot {accept,display} a value of a
  shell type".
- `anyenum_out`: the value crosses as the enum's OID (C's by-value `Datum`)
  to the real `enum.c` port `backend-utils-adt-enum::enum_out` via a direct,
  acyclic cargo dep (preferred over a seam). `enum_out` returns an owned
  `String`, forwarded verbatim.
- Delegating outputs that DON'T cross a cycle (array/text/multirange) are
  direct cargo deps per the "direct dep by default" rule; `range_out` crosses
  the rangetypes cycle and stays a seam (already declared).
- No `todo!`/`unimplemented!`; no CONTRACT_RECONCILE_PENDING entries.

## Tests

11 unit tests: input/recv/output/send dummy message+SQLSTATE sweeps, shell
specials, cstring in/out/recv/send (ASCII, with identity mbutils mocks), void
in/recv/out/send. All pass.
