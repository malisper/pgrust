# Audit: backend-utils-adt-network-spgist

C source: `src/backend/utils/adt/network_spgist.c` (PostgreSQL 18.3, 488 lines).
Port: `crates/backend-utils-adt-network-spgist/src/lib.rs`.
Vocabulary types (`inet_struct` codec, `PGSQL_AF_INET`/`PGSQL_AF_INET6`):
`crates/types-network/src/lib.rs`.
SP-GiST In/Out vocabulary (`spgConfigIn`/`spgConfigOut`/`spgChooseIn`/
`spgChooseOut`/`spgPickSplitIn`/`spgPickSplitOut`/`spgInnerConsistentIn`/
`spgInnerConsistentOut`/`spgLeafConsistentIn`/`spgLeafConsistentOut`):
`crates/types-spgist/src/lib.rs`.
By-OID dispatch wiring: `crates/backend-access-spg-quadtree/src/lib.rs`
(the single installer of the `backend-access-spg-core-seams` SP-GiST opclass
dispatch; routes the inet OIDs to the bodies here, exactly as it routes the
quad/kd/text opclasses).

No c2rust run exists for this unit; audited against the C directly plus the
src-idiomatic base (`pgrust/src-idiomatic/crates/backend-utils-adt-network-spgist`,
which uses the old central-types In/Out model — reconciled to the repo's real
`types_spgist` carriers and the quadtree dispatcher, the landed SP-GiST infra).

## Function inventory and verdicts

| # | C function (line) | Port location | Verdict | Notes |
|---|---|---|---|---|
| 1 | `inet_spg_config` (54) | `inet_spg_config` | MATCH | `prefixType=CIDROID(650)`, `labelType=VOIDOID(2278)`, `canReturnData=true`, `longValuesOK=false`. (`leafType` left default — C does not set it.) |
| 2 | `inet_spg_choose` (73) | `inet_spg_choose` | MATCH | `!hasPrefix` → MatchNode by family (IPv4→0/IPv6→1, `Assert !allTheSame && nNodes==2`); `Assert nNodes==4 \|\| allTheSame`; family-mismatch → 2-node SplitTuple (`prefixHasPrefix=false`, `childNodeN` by prefix family, `postfixPrefixDatum=prefix`); prefix-mismatch (`ip_bits(val)<commonbits \|\| bitncmp!=0`) → recompute `commonbits=bitncommon(...,Min(ip_bits(val),commonbits))`, 4-node SplitTuple (`prefixPrefixDatum=cidr_set_masklen_internal(val,commonbits)`, `childNodeN=node_number(prefix,commonbits)`); else MatchNode `node_number(val,commonbits)`. `restDatum/postfix/prefix` datums via `InetPGetDatum`. |
| 3 | `inet_spg_picksplit` (172) | `inet_spg_picksplit` | MATCH | Init prefix from `datums[0]`; scan `1..nTuples` reducing `commonbits` (family break → differentFamilies; `ip_bits(tmp)<commonbits` clamp; `bitncommon`; break on 0). `nodeLabels=NULL`. differentFamilies → 2-node, map by family; else 4-node, `prefixDatum=cidr_set_masklen_internal(prefix,commonbits)`, map by `node_number(tmp,commonbits)`. `leafTupleDatums[i]=InetPGetDatum(tmp)`. Output `palloc` arrays → owned `Vec`. |
| 4 | `inet_spg_inner_consistent` (251) | `inet_spg_inner_consistent` | MATCH | `!hasPrefix` (`Assert !allTheSame && nNodes==2`): `which=1\|(1<<1)`, per-key LT/LE narrows to family-IPv4, GE/GT to IPv6, NE no-op, default by family. `!allTheSame` (`Assert nNodes==4`): `which=consistent_bitmap(prefix,nkeys,scankeys,false)`. else `which=~0`. Emit `nodeNumbers[]` for set bits, counting `nNodes`. `~0` == `!0i32` (all bits set, mirrors C `~0`). |
| 5 | `inet_spg_leaf_consistent` (339) | `inet_spg_leaf_consistent` | MATCH | `recheck=false`; `leafValue=InetPGetDatum(leaf)`; returns `consistent_bitmap(leaf,nkeys,scankeys,true) != 0` (C `PG_RETURN_BOOL`). |
| 6 | `inet_spg_node_number` (366, static) | `inet_spg_node_number` | MATCH | `commonbits<ip_maxbits && (addr[commonbits/8] & (1<<(7-commonbits%8)))` → `\|=1`; `commonbits<ip_bits` → `\|=2`. |
| 7 | `inet_spg_consistent_bitmap` (391, static) | `inet_spg_consistent_bitmap` | MATCH | Checks 0–6 ported branch-for-branch: family mismatch (0), network-bit-count by op (1: SUB/SUBEQ/SUP/SUPEQ/EQ), common-bits `bitncmp(Min(commonbits,ip_bits(arg)))` (2), next-network-bit (3), basic-strategy gate (`strategy<EQ \|\| >GE` continue), netmask-width (4), `commonbits!=ip_bits(arg)` continue, next-host-bit (`!leaf` only) (5), whole-address `bitncmp(ip_maxbits(prefix))` (`leaf` only) (6). All `if (!bitmap) break;` and `continue` placements preserved. |

## Constant / OID verification

- Support-proc OIDs (pg_proc.dat): `inet_spg_config`=3795, `inet_spg_choose`=3796,
  `inet_spg_picksplit`=3797, `inet_spg_inner_consistent`=3798,
  `inet_spg_leaf_consistent`=3799. Confirmed against `src/include/catalog/pg_proc.dat`
  and the amprocnum 1..5 entries for `inet`/`inet` (spgist AM) in `pg_amproc.dat`.
- Type OIDs: `CIDROID`=650, `VOIDOID`=2278 (pg_type.dat).
- Strategy numbers (access/stratnum.h): EQ 18, NE 19, LT 20, LE 21, GT 22, GE 23,
  SUB 24, SUBEQ 25, SUP 26, SUPEQ 27. All matched.
- `PGSQL_AF_INET`=2, `PGSQL_AF_INET6`=3 (types-network, mirrors `utils/inet.h`).

## Macros / fmgr boundary

- `DatumGetInetPP(d)` → `inet_struct::from_datum_bytes(d.as_ref_bytes())`.
- `InetPGetDatum(p)` → `Datum::ByRef(slice_in(mcx, &p.to_datum_bytes()))`. `inet`
  and `cidr` share the same on-disk `inet_struct` image, so the `cidr` prefix
  datums use the same codec (matches C, where both are `inet *`).
- `ip_family`/`ip_bits`/`ip_addr`/`ip_maxbits` → inline helpers over `inet_struct`.
- `Min` → inline. `bitncmp`/`bitncommon`/`cidr_set_masklen_internal` reuse the
  ported `backend_utils_adt_network` bodies (no re-implementation).
- C `Assert(...)` → `debug_assert!(...)`.

## Seam / dispatch audit

This unit owns no inward seams and has no `-seams` crate (it is fmgr-dispatched,
exactly like `backend-access-spg-text` / `-quadtree` / `-kdtree`). It exports the
five opclass bodies as plain `pub fn` taking the typed `types_spgist` In/Out
structs; `backend-access-spg-quadtree` — the single installer of the SP-GiST
`backend-access-spg-core-seams` by-OID dispatch — was extended with five inet
arms (`F_INET_SPG_CONFIG`/`CHOOSE`/`PICKSPLIT`/`INNER_CONSISTENT`/
`LEAF_CONSISTENT`) routing to these bodies. `seams-init` already wires
`backend_access_spg_quadtree::init_seams()`; no new init wiring required.

## Divergences

None. No stubs, no `todo!`/`unimplemented!`, no introduced opacity. No
seam-and-panic legs (every dep — bit helpers, cidr masklen, inet codec — is
already ported). `residual_own_todos = 0`.

## Gate

- `cargo check --workspace`: green.
- `cargo test -p backend-utils-adt-network-spgist`: 5 unit tests pass
  (config, node_number branches, choose family-split + family-mismatch split,
  leaf-consistent EQ match/non-match).
- `no-todo-guard`: pass. `seams-init` recurrence guards (both): pass.
