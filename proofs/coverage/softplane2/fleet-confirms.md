# Soft plane wave 2 — fleet CONFIRM record

Lane proofs/softerror-plane-2, submitted 2026-07-31 at lane sha
2c56ed5f0c18a7b507a655ccc42cb20b769bedac (verified on origin via ls-remote
before submit). One job per target (phase-1 ruling), 10M-exec floor each.
Verdicts fetched from S3 (scripts/fetch-fuzz-results.sh); every job rc=0,
one full stats row per target, 10,000,000/10,000,000 execs, ZERO
divergences, ZERO sanitizer artifacts, zero crashed-early. Corpus
snapshots committed on the lane branch.

| target           | job                                        | corpus in->out | cov_lines | wall |
|------------------|--------------------------------------------|----------------|-----------|------|
| geo_io_diff      | pgrust-fuzz-campaign-1785556378-1601-44835 | 6743->7535     | 2162      | 632s |
| numeric_io_diff  | pgrust-fuzz-campaign-1785556385-7b93-45072 | 2956->3178     | 2028      | 655s |
| cash_diff        | pgrust-fuzz-campaign-1785556391-6f58-45316 | 719->745       | 1351      | 534s |
| mac_diff         | pgrust-fuzz-campaign-1785556398-6a35-45560 | 1509->1592     | 1379      | 569s |
| float_in_diff    | pgrust-fuzz-campaign-1785556404-4874-45833 | 1561->1949     | 562       | 562s |
| bool_diff        | pgrust-fuzz-campaign-1785556411-3193-46072 | 1096->1207     | 1064      | 522s |
| datetime_io_diff | pgrust-fuzz-campaign-1785556417-117b-46362 | 17778->18517   | 4405      | 664s |
| timestamp_diff   | pgrust-fuzz-campaign-1785556423-5355-46622 | 24676->25171   | 9036      | 703s |
| arrayfuncs_diff  | pgrust-fuzz-campaign-1785556430-1c0f-46930 | 18365->18896   | 3782      | 993s |

Fleet-lcov re-confirmation of every retired ledger line (glibc host, not
just the local macOS export):
- numeric io.rs:88 DA=76, io.rs:139 DA=172, builtins.rs:114 DA=1214
- float builtins.rs 60/61/63/64 DA=1142, 67/68/70/71 DA=792

Note: the fleet runs glibc hosts, so the geo macOS-only `nan(`-prefix
oracle carve (see geo.md) was dark there — those inputs were COMPARED on
fleet and produced zero divergences, which is the cross-platform
confirmation the carve's classification predicted.
