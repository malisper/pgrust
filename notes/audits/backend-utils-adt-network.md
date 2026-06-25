# Audit: backend-utils-adt-network

Scope: `network.c` (inet/cidr datatype) + the address codecs `inet_net_pton.c`,
`src/port/inet_net_ntop.c`, `inet_cidr_ntop.c`. The opclass/selfuncs files
(`network_gist.c`, `network_spgist.c`, `network_selfuncs.c`) are explicitly out
of scope (separate `backend-utils-adt-network-opclass` unit).

Independent function-by-function comparison of the port against the C and the
c2rust rendering. **Verdict: PASS.** Every function MATCH or SEAMED (per the
documented scope decomposition); no DIVERGES / MISSING / PARTIAL; all SQLSTATEs
and error message strings byte-exact; all bit arithmetic verified branch-for-
branch including integer signedness/width.

## network.c

All MATCH except the documented decomposed/seamed set:

- `network_in/out`, `inet_in/cidr_in/inet_out/cidr_out`, `network_recv/send` +
  `inet_recv/cidr_recv/inet_send/cidr_send`: MATCH (value-layer over `&[u8]` /
  `Vec<u8>` / `inet_struct`; the varlena/StringInfo envelope is the project-wide
  fmgr deferral). All 4 recv error messages + `ERRCODE_INVALID_BINARY_REPRESENTATION`
  + the "Value has bits set to right of mask." detail exact.
- `inet_to_cidr` (elog→`ERRCODE_INTERNAL_ERROR`), `inet_set_masklen`,
  `cidr_set_masklen`, `cidr_set_masklen_internal`: MATCH.
- `network_cmp_internal` + cmp/lt/le/eq/ge/gt/ne/smaller/larger: MATCH.
- `hashinet`/`hashinetextended`: MATCH (returns the `addrsize+2` byte view;
  `hash_any` is the deferred fmgr boundary).
- `network_sub/subeq/sup/supeq/overlap`, `inet_same_family`, `inet_merge`: MATCH.
- `network_host/show`, `inet_abbrev`, `cidr_abbrev`, `network_masklen/family`,
  `network_broadcast/network/netmask/hostmask`: MATCH.
- `convert_network_to_scalar` (inet/cidr), `convert_macaddr_to_scalar`,
  `convert_macaddr8_to_scalar`: MATCH — the signed-`i32` shift in the macaddr8
  branch faithfully reproduces C's negative-double behaviour on a high top byte.
- `bitncmp`, `bitncommon`, `addressOK`: MATCH.
- `inetnot`, `inetand`, `inetor`, `internal_inetpl`, `inetpl`, `inetmi_int8`
  (`wrapping_neg`), `inetmi`: MATCH — the int64 overflow + sign-extension logic
  in `inetmi`/`internal_inetpl` is exact.
- `clean_ipv6_addr`: MATCH.
- `inet_client_addr/server_addr/client_port/server_port`: SEAMED — MyProcPort /
  `pg_getnameinfo_all` via the outward `session::resolve` seam.
- `network_sortsupport`, `network_fast_cmp`, `network_abbrev_abort`,
  `network_abbrev_convert`: pure bit-packing (`network_abbrev_convert_bits`) and
  comparator (`network_fast_cmp`) ported; registration + HyperLogLog cross the
  outward `sortsupport::register` seam.
- `network_subset_support`, `match_network_function`, `match_network_subset`,
  `network_scan_first/last`: dispatch (`classify_network_function`) + scan
  helpers ported; index-condition node construction crosses the outward
  `planner::network_subset_support` seam.

## Codec files

`inet_net_pton.c`, `inet_net_ntop.c`, `inet_cidr_ntop.c`: every function MATCH
(family dispatch + errno, IPv4/IPv6 parsers, `getbits`/`getv4`, class inference,
the by-hand `::` overlap shift, longest-zero-run + encapsulated-IPv4 rendering,
all `sizeof "…"` capacity checks). The `bits.saturating_mul/add` over-long-CIDR
guard is a documented non-divergence: accepted values are unchanged and an
over-long run still trips the same EMSGSIZE, avoiding C's signed-overflow UB.

## Seam / wiring audit

The unit owns NO inward seam crate (no C function here is called across a cycle
from a consumer). The three slots in `backend-utils-adt-network-seams`
(`session::resolve`, `sortsupport::register`, `planner::network_subset_support`)
are OUTWARD seams the crate CALLS; their real owners (libpq-be / tuplesort /
planner) are unported, so they stay uninstalled and panic loudly — acceptable
mirror-PG-and-panic. `init_seams()` is correctly empty (no inward contract). The
recurrence guard's outward-seam exclusion confirms this (`seams-init` green).

Verdict: **PASS** — eligible to merge.
