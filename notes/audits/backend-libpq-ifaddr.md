# Audit: backend-libpq-ifaddr

- C source: `src/backend/libpq/ifaddr.c` (postgres-18.3, 455 lines)
- c2rust rendering: `c2rust-runs/backend-libpq-ifaddr/src/ifaddr.rs` (macOS build
  config: `HAVE_GETIFADDRS`, not `WIN32`)
- Port: `crates/backend-libpq-ifaddr/src/lib.rs`
- Auditor: independent re-derivation from C + c2rust; fix round 1 applied
  (see Findings), fixed function re-audited from scratch.

## Function inventory and verdicts

The C file defines 4 functions plus 4 mutually exclusive `#if` variants of
`pg_foreach_ifaddr` (WIN32 / HAVE_GETIFADDRS / SIOCGIFCONF / loopback
fallback). The c2rust rendering (post-preprocessor) contains exactly the 4
built functions plus the getifaddrs variant, confirming the inventory.

| C function (ifaddr.c) | Port location (lib.rs) | Verdict | Notes |
|---|---|---|---|
| `pg_range_sockaddr` (48) | `pg_range_sockaddr` (49) | MATCH | Family dispatch via `IpAddr` variants. C dispatches on `addr->ss_family` only and documents that the caller must have verified all three families match; the port's match on all three tuples is identical on every contract-respecting input, and unsupported/mixed families return 0/false in both. |
| `range_sockaddr_AF_INET` (65, static) | `range_sockaddr_af_inet` (61) | MATCH | `((addr ^ netaddr) & netmask) == 0`. C operates on network-byte-order `s_addr`, port on `to_bits()` (big-endian octets); XOR/AND/compare-to-zero is byte-order invariant. |
| `range_sockaddr_AF_INET6` (77, static) | `range_sockaddr_af_inet6` (65) | MATCH | Per-octet `(a ^ n) & m != 0 -> return 0` over all 16 octets; `all()` short-circuits identically. |
| `pg_sockaddr_cidr_mask` (104) | `pg_sockaddr_cidr_mask` (84) + `ipv4_cidr_mask` (103) + `ipv6_cidr_mask` (118) + `parse_strtol_base10` (147) | MATCH (after fix) | `numbits == NULL` => 32 (INET) / 128 (otherwise), matching `(family == AF_INET) ? 32 : 128`. strtol semantics re-derived: leading C-locale whitespace (incl. `\v` after fix), one optional sign, base-10 digits, whole-string consumption; whitespace-only / empty / no-digit / trailing-garbage inputs are rejected exactly as `*numbits == '\0' || *endptr != '\0'` does (strtol leaves endptr at the string start when no digits are consumed). Overflow: C clamps to LONG_MAX/LONG_MIN, then fails the 0..=32 / 0..=128 range check; port returns the same error directly. IPv4: `bits > 0 ? (0xffffffffUL << (32-bits)) & 0xffffffff : 0`, stored network-order (`pg_hton32` vs `Ipv4Addr::from_bits`) — identical for bits 0..=32. IPv6: 16-iteration loop with `<=0 -> 0`, `>=8 -> 0xff`, else `(0xff << (8-bits)) & 0xff`, `bits -= 8` — transcribed exactly. Unknown family => C `-1` / port `UnsupportedFamily` (C collapses both failure modes into `-1`; the port's two-variant error is a strict refinement). |
| `run_ifaddr_callback` (180, static) | `run_ifaddr_callback` (199) + `mask_is_valid_for_addr` (218) + `address_family` (229) | MATCH | Null-addr early return moved to the caller's `if let Some(addr)` (addr is non-optional `IpAddr`). Mask invalidation: family mismatch, IPv4 `INADDR_ANY` (0), IPv6 unspecified (all 16 bytes zero, matching `IN6_IS_ADDR_UNSPECIFIED`'s four-u32 test in the c2rust rendering) — all three reproduced. Invalid/missing mask replaced by `pg_sockaddr_cidr_mask(NULL, family)` full mask, which cannot fail for INET/INET6 (the only families representable as `IpAddr`); the C ignores the return code in the same way. |
| `pg_foreach_ifaddr` — WIN32 variant (229) | — | not in build config | `#ifdef WIN32`; absent from c2rust. Non-unix targets get the C file's loopback fallback instead (see below), per the catalog note. |
| `pg_foreach_ifaddr` — getifaddrs variant (294) | `pg_foreach_ifaddr` `#[cfg(unix)]` (275) + `sockaddr_to_ipaddr` (249) | MATCH | This is the built variant (c2rust lines 330-347). `getifaddrs` failure => C `-1` / port `Err(last_os_error)`. Linked-list walk, `run_ifaddr_callback(addr, netmask)` per entry, `freeifaddrs` at the end. `sockaddr_to_ipaddr` decodes AF_INET (`u32::from_be(s_addr)` -> correct octet order) and AF_INET6 (`s6_addr` bytes). Null `ifa_addr` skip = the C callee's `if (!addr) return`. Non-IP families (AF_LINK/AF_PACKET) are filtered at decode: in C such entries reach the callback, but every callback in the tree dispatches on AF_INET/AF_INET6 and ignores others (and the C even passes an *uninitialized* fullmask for them, since `pg_sockaddr_cidr_mask` fails for non-IP families); with the `IpAddr` vocabulary the filter is the only faithful realization and is behaviorally identical for all observable (IP) entries. |
| `pg_foreach_ifaddr` — SIOCGIFCONF variant (348) | — | not in build config | `!HAVE_GETIFADDRS && defined(SIOCGIFCONF)`; absent from c2rust. |
| `pg_foreach_ifaddr` — loopback fallback (424) | `pg_foreach_ifaddr` `#[cfg(not(unix))]` (305) | MATCH | Audited against C lines 424-452: reports 127.0.0.1 with `pg_sockaddr_cidr_mask("8", AF_INET)` = 255.0.0.0 and ::1 with the /128 mask, both routed through `run_ifaddr_callback` (masks are valid, so they pass through unchanged); returns success. `pg_ntoh32(0x7f000001)` stores network-order 7f.00.00.01 = `Ipv4Addr::new(127,0,0,1)`; `s6_addr[15] = 1` = `Ipv6Addr::LOCALHOST`. |

Spot-check pass: `range_sockaddr_AF_INET6`, the IPv6 mask loop, and the
`run_ifaddr_callback` mask-substitution path were re-derived a second time
against the c2rust rendering before sign-off; byte-order reasoning for
`s_addr` round-trips (`pg_hton32` / `from_bits` / `u32::from_be`) verified by
hand.

## Seam audit

- `CATALOG.tsv` declares the unit a leaf with no seams. Confirmed: no
  `backend-libpq-ifaddr-seams` crate exists, the crate's only dependency is
  `libc` (cfg(unix), for `getifaddrs`/`freeifaddrs`/sockaddr decoding — direct
  OS calls, exactly as the C makes them), and `seams-init` has no entry for
  this unit. No outward seam calls, no `set()` anywhere. No findings.

## Findings and fixes

1. **DIVERGES (fixed, round 1):** `parse_strtol_base10` skipped leading
   whitespace with `is_ascii_whitespace()`, which excludes vertical tab
   (0x0B); C-locale `isspace()` used by `strtol` includes it, so e.g.
   `"\v24"` was accepted by C but rejected by the port. Fixed by adding
   `|| bytes[i] == 0x0b` to the skip loop and a regression test
   (`cidr_mask_accepts_strtol_style_whitespace_and_sign`). Re-audited the
   function from scratch post-fix: whitespace set, sign handling, endptr
   semantics, and overflow behavior all match.

## Verdict

**PASS.** All built functions MATCH (the two `#if` variants outside the build
config are not part of the unit; the non-unix fallback that the port does ship
matches its C counterpart), zero seam findings. `cargo test -p
backend-libpq-ifaddr` (13 tests) and `cargo clippy --all-targets` are clean.
