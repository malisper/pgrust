# NEXT-200: ranked proof waves (surveyed 2026-07-28)

Survey method: every nominated family's Rust source was opened and sampled
against the TRIAGE cost model (circuit shape / symbolic length / table length /
allocation / error paths). Census at survey time: 1282 untriaged, 25 candidate,
971 excluded (state 573, engine 220, typcache 178), 664 proved, 10 wall,
15 in-progress. In-progress rows owned by agents `text-slice` (strpos/substr/
textcat/byteacat family) and `strings-misc` (quote_ident/quote_literal/ascii/chr)
are NOT re-nominated here.

Classes used below: **fast** (<10s expected), **ladder** (10–30s+, needs
escalation flags: exact unwind, kissat-vs-default per harness,
`--no-assertion-reach-checks` + shared cover harness, magnitude case-splits),
**seam** (named stub pattern), **needs-refactor** (allocation/monolith in core),
**skip** (reason given).

Standing traps that apply everywhere: Box<PgError> drop glue (`mem::forget`
after verdict-matching, never `.unwrap()` on PgResult); exact-fit `unwind`
(slack = RSS under the 6GB watchdog); `kani::cover!` hoisted into one shared
regime harness; GROUND-TRUTH law before recording any divergence (docker
postgres:18 + macOS PG when libc-flavored).

---

## Wave 1 — mbconv local recodes (finish-in-flight) — 48 rows, FAST

**Lane:** `proofs/mbconv` (EXISTS: all C vendored in `proofs/mbconv/c/`
— pg_conv_cyrillic_mic.c, pg_conv_latin_mic.c, pg_conv_latin2_win1250.c,
pg_conv_euc_*_mic.c, pg_conv_euc_jp_sjis.c, pg_conv_euc_tw_big5.c,
pg_conv_euc2004_sjis2004.c — and **86 harnesses already written**
(`proofs/mbconv/harnesses.txt`). This wave is run-verify-record, the cheapest
~50 rows on the board.)

Rows (oid name): 4302 koi8r_to_mic, 4303 mic_to_koi8r, 4304 iso_to_mic,
4305 mic_to_iso, 4306 win1251_to_mic, 4307 mic_to_win1251, 4308 win866_to_mic,
4309 mic_to_win866, 4310 koi8r_to_win1251, 4311 win1251_to_koi8r,
4312 koi8r_to_win866, 4313 win866_to_koi8r, 4314 win866_to_win1251,
4315 win1251_to_win866, 4316 iso_to_koi8r, 4317 koi8r_to_iso,
4318 iso_to_win1251, 4319 win1251_to_iso, 4320 iso_to_win866,
4321 win866_to_iso, 4322 euc_cn_to_mic, 4323 mic_to_euc_cn,
4324 euc_jp_to_sjis, 4325 sjis_to_euc_jp, 4326 euc_jp_to_mic,
4327 sjis_to_mic, 4328 mic_to_euc_jp, 4329 mic_to_sjis, 4330 euc_kr_to_mic,
4331 mic_to_euc_kr, 4332 euc_tw_to_big5, 4333 big5_to_euc_tw,
4334 euc_tw_to_mic, 4335 big5_to_mic, 4336 mic_to_euc_tw, 4337 mic_to_big5,
4338 latin2_to_mic, 4339 mic_to_latin2, 4340 win1250_to_mic,
4341 mic_to_win1250, 4342 latin2_to_win1250, 4343 win1250_to_latin2,
4344 latin1_to_mic, 4345 mic_to_latin1, 4346 latin3_to_mic,
4347 mic_to_latin3, 4348 latin4_to_mic, 4349 mic_to_latin4.

Screened: cores are raw-pointer writer loops (`latin2mic`-style `Dst(*mut u8)`,
conv/src/lib.rs) — caller-provided dest buffer, **no Vec-push trap, no Mcx in
core**; per-byte table lookups + highbit branches. Class fast at len≤8
(single-byte recodes) / ladder for the multi-byte euc/sjis/big5 pairs
(2–3-byte sequences, still table+branch shape). Traps: exact unwind to the
length cap; NUL-terminator write at end (include in theorem); `no_error` soft
path needs both-arms cover.

## Wave 2 — pseudotypes stub sweep — 51 rows, FAST

**Lane:** new-ish `proofs/pseudotypes` (dir EXISTS with `c/pg_pseudotypes.c`
vendored). Crate: `crates/backend/utils/adt/pseudotypes`.

Rows: 86–88, 90 pg_ddl_command_in/out/recv/send; 195–198 pg_node_tree_*;
267/268 table_am_handler_in/out; 326/327 index_am_handler_in/out;
2292/2293 cstring_in/out; 2294/2295 any_in/out; 2296 anyarray_in;
2298/2299 void_in/out; 2300/2301 trigger_in/out; 2302/2303
language_handler_in/out; 2304/2305 internal_in/out; 2312/2313
anyelement_in/out; 2398/2399 shell_in/out; 2502 anyarray_recv; 2777/2778
anynonarray_in/out; 3116/3117 fdw_handler_in/out; 3120/3121 void_recv/send;
3311/3312 tsm_handler_in/out; 3504 anyenum_in; 3594/3595 event_trigger_in/out;
3832 anyrange_in; 4226 anycompatiblemultirange_in; 4229 anymultirange_in;
5086/5087 anycompatible_in/out; 5088/5090 anycompatiblearray_in/recv;
5092/5093 anycompatiblenonarray_in/out; 5094 anycompatiblerange_in.

Screened: builtins.rs is a `fc_stub!` macro over ereport-only cores + real
logic only in void_* (constants), cstring_in/out (copy via fn_extra scratch),
pg_node_tree_out/send (delegate to varlena textout/textsend — prove as
delegation identity). Theorem for stubs = error parity (sqlstate + message).
Class fast (sub-second each). Trap: this wave is 100% Err-arm — the
Box<PgError> drop-glue trap dominates; use forget + field asserts. **skip**
within wave: none needed; the `*_out`/`*_send` delegates whose target unit is
unported are unregistrable — record `excluded(unregistered)` if hit.

## Wave 3 — int/int8 remainder — ~34 rows, FAST (2 release-gate)

**Lane:** extend `proofs/int-arith` + `proofs/int-cmp` (C vendored:
pg_int_arith.c, pg_intcmp.c; add int.c/int8.c bitop/in_range sections).

Fast rows: 1892–1897 int2and/or/xor/not/shl/shr; 1898–1903
int4and/or/xor/not/shl/shr; 1904–1909 int8and/or/xor/not/shl/shr;
4126–4132 in_range_int8_int8, in_range_int4_int8/int4/int2,
in_range_int2_int8/int4/int2 (screened lib.rs:374-455 — checked add +
compares, overflow short-circuit branch); 313 i2toi4, 314 i4toi2, 480 int84,
481 int48, 714 int82, 754 int28, 1287 i8tooid, 1288 oidtoi8, 766 int4inc,
2804 int8inc_any, 3547 int8dec_any (checked ops); 2404–2409
int2/int4 recv+send, 2408/2409 int8recv/send.

Ladder: 38 int2in, 460 int8in (wrapper over the proved pg_strtoint core —
len≤8 per-commit, len 9+ stays wall per strtoint result; int8in 19-digit =
release-gate); 40/41 int2vectorin/out (dim≤4 per-length, oidvector-fence
precedent from hashoidvector).

**Parity flag (high value):** int4shl/shr comment claims hardware masking
where C `<<` past 31 is UB (`int/src/lib.rs:773` "C arg1 << arg2 is UB past
31; hardware (and this port) masks the count") — a shipped comment-claim to
machine-check; adjudicate count∈[0,31] as the contract, spot-witness the
out-of-range behavior against real PG per GROUND-TRUTH law before recording
any divergence.

Skip: 5044/5046 int4gcd/lcm, 5045/5047 int8gcd/lcm (data-dependent Euclid —
TRIAGE-excluded shape); 1066–1069 + 3994/3995 generate_series* (SRF
machinery); 6102? n/a.

## Wave 4 — cash family exhaustion — 23 rows, FAST/ladder mix

**Lane:** extend `proofs/cash` (C vendored: pg_cash.c; harness crate exists —
sibling-family marginal cost is minutes).

Fast (i64 checked ops; int-arith multiply refutation applies — forget on Err):
862 int4_mul_cash, 863 int2_mul_cash, 864 cash_mul_int4, 866 cash_mul_int2,
3344 cash_mul_int8, 3399 int8_mul_cash; 3811 int4_cash, 3812 int8_cash
(int8mul against scale_factor — **locale seam**: pin lconv fpoint identically
both sides; scale loop unwind exact).

Ladder (division): 865 cash_div_int4, 867 cash_div_int2, 3345 cash_div_int8
(symbolic÷symbolic 64-bit dividend = wall per division rule → danger-set spot
proofs x/0, INT64_MIN/-1, /-1 + 16-bit-dividend band); 3822 cash_div_cash
(i64→f64 casts proved fast, then f64÷ = 53-bit wall → zero-arm + grid).

Ladder (53-bit float): 846 cash_mul_flt4, 847 cash_div_flt4, 848
flt4_mul_cash, 896 cash_mul_flt8, 897 cash_div_flt8, 919 flt8_mul_cash —
float-arith law: special-grid + zero-arm + fenced-plane; `--no-overflow-checks`;
cover! on BOTH Result arms (retrofit owed to proofs/cash already noted).

Ladder (parse/format): 886 cash_in (multi-branch parse, locale seam, cap
len≤8; sscanf-cascade cost law — per-length, no reach-checks), 887 cash_out
(digit emission /10 sloped wall → magnitude case-split), 935 cash_words
(same + word tables; Mcx result → mcx-stubs), 2492/2493 cash_recv/send.

Needs-refactor/skip: 3823 cash_numeric, 3824 numeric_cash — numeric division
by scale runs the allocating digit-loop arithmetic (DigitBuf::realloc wall);
park until the numeric fixed-buffer core refactor.

## Wave 5 — scalar ids + uuid + pg_lsn + xid8 — ~38 rows, FAST

**Lane:** extend `proofs/scalar-misc`, `proofs/uuid` (c/pg_uuid.c),
`proofs/pg_lsn` (c/pg_pg_lsn.c); new xid8 harnesses beside them (vendor
xid8funcs.c snapshot-parse/visibility sections).

scalar (crates/utils/adt/scalar — screened: pure u32/u64/Tid compare/format):
52 cidin, 53 cidout, 69 cideq, 51 xidout (u32 digit emission — sloped, band
[0,1e7)+spots precedent from intout), 54/55 oidvectorin/out (dim≤4 per-length
+ valid-oidvector fence), 2418/2419 oidrecv/send, 2438–2443 tid/xid/cid
recv+send, 5082/5083 xid8recv/send, 2233/2234 hashtid[extended], 6419–6424
hashxid/hashxid8/hashcid [+extended] (hash-rows rig, proofs/hash-rows).
Seam: 1181 xid_age, 3939 mxid_age (ReadNextTransactionId → state-seam, nextval
precedent, symbolic next-xid). Skip: 1294 currtid_byrelname (catalog/heap).

uuid: 2961 uuid_recv, 2962 uuid_send, 2963 uuid_hash, 3412 uuid_hash_extended,
6343 uuid_extract_version (bit extract), 6342 uuid_extract_timestamp (constant
mul/shift on 48-bit ms — screen: i64 constant-mul of one symbolic contributor
= fast per i128 rule; the /-by-1000 variants ladder). Core-only: 77
generate_uuidv7(unix_ts_ms, sub_ms) pure assembly — prove the core, seam the
clock. Skip: 3432/6428 gen_random_uuid (PRNG state), 6429 uuidv7 / 6430
uuidv7_interval wrappers (clock; core covered above — wrapper = seam later).

pg_lsn: 3252 pg_lsn_hash, 3413 pg_lsn_hash_extended, 3238/3239
pg_lsn_recv/send (fast); 3237 pg_lsn_mi, 5022 pg_lsn_pli, 5024 pg_lsn_mii,
6103 numeric_pg_lsn — u64↔numeric conversion = /10000 digit emission, sloped
→ band case-split (int64_to_numeric precedent viable; numeric comparator rig
shows packed-header decode is cheap).

xid8funcs (screened lib.rs — SnapView is a borrowed byte image, no Mcx):
2945/5062 pg_snapshot_xmin, 2946/5063 pg_snapshot_xmax, 2948/5065
pg_visible_in_snapshot (is_visible_fxid: bounded xip search — cap nxip≤4,
exact unwind), 81 full_xid_from_allowable_at core (epoch arithmetic, fast);
2939/5055 pg_snapshot_in (strtou64 parse — per-length ladder), 2940/5056
pg_snapshot_out (digit emission ladder), 2941/5057 pg_snapshot_recv
(validating reader, fast), 2942/5058 pg_snapshot_send. Seam: 3360/5066
pg_xact_status (clog seam = state-seam pattern). Skip: 2943/5059
pg_current_xact_id, 2944/5061 pg_current_snapshot, 3348/5060 (live xact
state), 2947/5064 pg_snapshot_xip (SRF), 3809 pg_export_snapshot (file I/O).

## Wave 6 — date/time/interval arithmetic + hashes + wire — ~36 rows

**Lane:** extend `proofs/datetime-cmp` + `proofs/interval-cmp` (C vendored:
pg_datetime_cmp.c, pg_interval_cmp.c; add date.c/timestamp.c arithmetic
sections). Crates: adt_date, adt_timestamp (screened bodies cited).

Fast: 1140 date_mi (i32 sub), 1373 date_finite, 1390 interval_finite,
1168 interval_um, 1169 interval_pl, 1170 interval_mi (interval.rs:333-420 —
checked i64/i32 adds + NOBEGIN/NOEND lattice; forget-on-Err), 1690
time_mi_time (sub), 1308 overlaps_time, 1271 overlaps_timetz (pure comparator
branch logic — wrapper-level with null flags, datetime-cmp precedent),
1370 time_interval, 1419 interval_time, 2047 time_timetz (tz-seam for the
session offset), 2046 timetz_time, 3847 make_time (constant mults),
1688 time_hash, 1696 timetz_hash, 1697 interval_hash, 3409/3410/3418
*_hash_extended, 6415 hashdate, 6416 hashdateextended (hash-rows rig).
Wire/typmod fast: 2468/2469 date_recv/send, 2470/2471 time_recv/send,
2472/2473 timetz_recv/send, 2478/2479 interval_recv/send (range checks +
typmod scale — anytime_typmod_check in theorem), 2909–2912
time/timetz typmodin+out.

Ladder: 1175 interval_justify_hours (time/USECS_PER_DAY — 64-bit dividend by
large constant: band case-split on |time|<1e7·USECS grid + spot proofs; exact
unwind), 1295 interval_justify_days (/30 small constant — sloped, fine),
2711 interval_justify_interval, 1968 time_scale / 1969 timetz_scale
(power-of-10 rounding table — magnitude case-split), 1747–1750
time/timetz ± interval (wrapping add then one % USECS_PER_DAY — same band
treatment as justify_hours; if it walls, native-differential fallback,
record tested(differential)), 4133/4137/4138 in_range_date/time/timetz_interval.

Seam: 2037 timetz_zone, 2038 timetz_izone, 6336 timetz_at_local (tz-seam:
symbolic offset ±86400 + skew control).

Skip (this pass): 1084/1085 date_in/out, 1143/1144 time_in/out, 1350/1351
timetz_in/out, 1160/1161 interval_in/out (datetime multi-format parser =
heaviest-circuit cascade + j2date wall), 3846 make_date (date2j divider
chain), 2071/2072 date_pl/mi_interval and 1174/1178/2024/2029/1272/2025
date↔timestamp[tz] (month path runs j2date/date2j), extract_* 6199–6201
(formatting), 1326 interval_div / 1618 interval_mul / 1624 mul_d_interval
(two-symbolic-contributor multiply wall — revisit with literal-zeroing
case-split as a stretch goal), 1200 interval_scale (numeric-style rounding),
3944 time_support (planner support), 6177/6178 timestamp[tz]_bin (large-
constant modulo — screen again after justify_hours verdict).

## Wave 7 — timestamp siblings + float rounding — ~30 rows

**Lane:** same crates/proof dirs as Wave 6 + extend `proofs/float-arith`
(c/pg_float_arith.c) and `proofs/casts`.

timestamp fast: 1188/2031 timestamp_mi (checked sub + lattice, screened),
1389/2048 timestamp_finite, 1304/2041 overlaps_timestamp, 2039 timestamp_hash,
3411 timestamp_hash_extended, 6425/6426 timestamptz_hash[_extended],
2474–2477 timestamp/timestamptz recv+send, 2903–2908 interval/timestamp/
timestamptz typmodin+out, 3464 make_interval (constant mults + checked adds).
Seam: 2027 timestamptz_timestamp / 2028 timestamp_timestamptz (tz-seam — the
dt-minmax pattern exactly). Ladder: 1961 timestamp_scale / 1967
timestamptz_scale (pow-10 case-split), 4134/4135/4136
in_range_timestamp/timestamptz/interval_interval (pl_interval time-only plane
fast; month plane walls → fence months==0 + spot the month arm natively).
Skip: trunc/part/age/zone family (timestamp2tm divider chains), now/
statement_timestamp/clock_timestamp (clock state, low value), aggregates
(interval_avg_*, accum — transfn state arrays), generate_series_*,
timeofday, pg_timezone_* (SRF/tzdata).

float rows: fast — 228/1342 dround, 229/1343 dtrunc, 2308/2320 dceil,
2309 dfloor, 2310 dsign (screened funcs.rs:144-180 — round_ties_even/floor/
ceil/branch; rint≡round_ties_even already machine-checked in casts),
4139 in_range_float8_float8 / 4140 in_range_float4_float8 (funcs.rs:895 —
NaN lattice + f64 add/sub + compares = green per float law;
`--no-overflow-checks`), 2424–2427 float4/float8 recv+send.
Probe: 230/1344 dsqrt (IEEE sqrt is CBMC-native; overflow/underflow branch
checks cheap — one probe harness, class on the result). Skip: 231/1345 dcbrt
+ all transcendentals (dexp/dlog/dsin/… libm), dpow, float4in/8in (strtod),
accum/regr/avg family (53-bit mul chains + array state), width_bucket_float8
(division by symbolic span), 6219/6220/6383/6384 (erf/gamma libm).

## Wave 8 — varlena byte kernels — ~35 rows, FAST

**Lane:** extend `proofs/bytea-cmp` / `proofs/bytea-varbit` / `proofs/hash-rows`
(C vendored: pg_bytea_cmp.c, pg_bytea_varbit.c, pg_hash_rows.c; add
varlena.c to_hex/int-cast sections). Crate: utils/adt/varlena.
Coordinate with agent `text-slice` (owns strpos/substr/cat rows — not here).

Fast: 2089 to_hex32, 2090 to_hex64, 6330/6331 to_bin32/64, 6332/6333
to_oct32/64 (pow-2 base digit emission — shift/mask, NOT the /10 wall),
6367–6369 int2/int4/int8_bytea, 6370–6372 bytea_int2/int4/int8 (byte
pack/unpack + length checks), 720/2010 byteaoctetlen, 1374 textoctetlen,
3696 text_starts_with (memcmp shape, varlena (ptr,len) fence — bytea-cmp
pattern, symbolic lens cap 8), 6393/6394 bytea_larger/smaller (comparator on
proved bytea_cmp core), 6413/6414 hashbytea[extended], 456/772
hashvarlena[extended] (hash rig), 6163 bytea_bit_count (popcount rig — scalar
path only, NEON cfg'd out), 6382 bytea_reverse (byte loop), 849 textpos /
2014 byteapos (bounded nested search, caps 8/4), 46/47 textin/textout,
109/110 unknownin/unknownout (passthrough copies — mcx-stubs for result
alloc), 2412–2417 bytea/text/unknown recv+send (recv validates encoding →
pin encoding seam per hstore_recv lesson — parity of the validating path).

Ladder: 1257/1317/1369/1381 textlen (pg_mbstrlen loop — per-encoding split:
single-byte arm trivial, UTF8 arm uses proved mblen kernel; encoding pin
seam), 749/752 byteaoverlay[_no_len] (splice writes — slice core, reserve+
set_len shape per STD-VEC trap). Skip: pg_column_size/compression/
toast_chunk_id (toast state), text_format (engine), string_agg (state),
unicode_* (ICU/normalization tables — separate campaign), unistr (parse +
surrogate pairs — screen later), text_to_array/table (SRF).

## Wave 9 — re-opened exclusions: ACL privilege seam lane — ~32 rows, SEAM

**Lane:** extend `proofs/state-seam-probe` (aclmask membership-oracle seam
ALREADY PROVED there — this wave is its reach). Vendor acl.c object-aclcheck
sections. These rows count toward the 200 as excluded(state) re-opens.

Rows (six object classes × name/id argument forms — the object aclcheck layer
TRIAGE names as in-reach): 1922–1927 has_table_privilege_*, 2181–2186
has_sequence_privilege_*, 2250–2255 has_database_privilege_*, 2256–2261
has_function_privilege_*, 2262–2267 has_language_privilege_*, 2268–2273
has_schema_privilege_*. Plus re-opens: 1031 aclitemin / 1032 aclitemout
(role-name lookup → membership-oracle seam over get_role_oid; the priv-string
parse core is pure and partially covered by the proved makeaclitem row), and
the already-candidate rows 1035 aclinsert, 1036 aclremove (error stubs),
3943 acldefault_sql (pure objtype+owner mapping, candidate class fast).

Pattern: first-match membership-oracle seam (fully symbolic answers over the
reachable query set) + name→oid lookup seam; skew control load-bearing.
Traps: per-assert reach-checks × kissat fake-wall on deep loops →
`--no-assertion-reach-checks` + one cover harness; UNSAT direction expensive.
Expected release-gate tier (~460-490s halves per state-seam calibration) —
budget as release-gate, not per-commit. Remaining ~58 has_*_privilege
siblings (fdw/server/tablespace/type/role/param/largeobject) are the follow-on
once the rig lands.

---

## Reserve waves (post-200 bench, pre-screened)

- **W10 varbit rows (~25, fast/ladder)** — extend `proofs/varbit-rows`
  (c/pg_varbit_rows.c). bitgetbit/bitsetbit (screened: shift/mask + index
  check), bit_bit_count, bitoctetlength, bitfromint4/8, bit/varbit
  typmodin+out, bit_in/out ('0'/'1' loop, cap≤16), bitcat/bitsubstr/
  bitoverlay/bitposition (byte-shift loops — acyclic split lesson from
  varbit-rows applies), recv/send. Skip: varbit_support.
- **W11 network (~20)** — extend `proofs/network`. inetpl/inetmi/inetmi_int8
  (screened internal_inetpl: bounded 16-byte carry loop, /0x100 pow2 = fast),
  inet_set_masklen/cidr_set_masklen/inet_to_cidr (mask loops), hashinet[ext],
  recv/send (family/bits validation), network_host/show/abbrev + inet_out
  (octet formatting ≤3 digits — sloped, fine), inet_in/cidr_in (parse cascade
  — per-length ladder, macaddr lesson). Skip: client/server_addr (session),
  subset_support (planner).
- **W12 geo fast subset (~35)** — extend `proofs/geo-cmp` (c/pg_geo_cmp.c).
  Screened: lseg_vertical/horizontal/eq/ne (FPeq compares), point_add/sub,
  box_add/sub (f64 add/sub green), points_box, box_diagonal, construct_point,
  lseg_construct, path_npoints/isclosed/isopen, poly_npoints, circle_same/
  eq/ne (radius FPeq), poly_left/right/overleft/overright/above/below/
  overabove/overbelow (bound-box compares), circle_left/right/above/below/
  over* (radius±center compares), recv/send ×14. Grid-tier (53-bit):
  point_mul/div, box_mul/div, circle_lt/le/gt/ge (area), all dist_*/close_*
  (hypot). Spot-grid rule: ONE symbolic index into a concrete table.
- **W13 mbconv utf8 arm (~36, release-gate)** — harnesses exist. Big table
  binary search costs ~n^1.5 domain-independent (unicode-cat law): win/koi8/
  iso8859 maps small = fine; big5/gbk/uhc/johab moderate; gb18030 huge table +
  range arithmetic — expect wall, keep native-differential fallback.
- **W14 oracle_compat + varchar (~30, mcx-stubs)** — trims (btrim/ltrim/
  rtrim/1-arg forms, byteatrim/bytealtrim/byteartrim: set-membership loops,
  slice cores), text_left/right/reverse (mb caps), translate, repeat (count
  guard), lpad/rpad; bpcharlen/bpcharoctetlen (encoding pin), bpchar/varchar
  truncation cores, name_bpchar/bpchar_name (name-cmp 64-byte precedent),
  bpchar/varchar typmodin+out, recv/send. lower/upper/initcap/casefold =
  per-encoding split; ICU arm excluded.
- **W15 typcache re-opens (~25, seam)** — extend `proofs/typcache-inst`.
  Range accessors (lower/upper/isempty/lower_inc/upper_inc/lower_inf/
  upper_inf, elem_contained_by_range, range_overleft/overright per concrete
  int4range — rig proved 14/15), text[] comparators (screened-feasible per
  TRIAGE), array_larger/smaller over int4[] (comparator rig proved). Records
  stay blocked pending fc_record_eq refactor; range_adjacent stays wall
  (serialize refactor named in ledger).
- **Candidates cleanup (free)** — the 25 `candidate` rows: arrayfuncs header
  arithmetic (747/748/2091/2092/2176/3179), dbsize formatting
  (2288/3334/3166), adt_misc (89/438/440/2918/3165/6292), acl stubs (with
  W9), sequence seam rows (1575/1576/1765/2559/3078/4032/6427 — state-seam
  rig from nextval applies directly).

---

## Summary table

| # | Lane | Rows | Difficulty | Prerequisite |
|---|------|------|-----------|--------------|
| 1 | mbconv local recodes | 48 | fast (harnesses EXIST) | none |
| 2 | pseudotypes stubs | 51 | fast (err-parity; forget-glue) | none |
| 3 | int/int8 remainder | ~34 | fast + 2 release-gate | none |
| 4 | cash exhaustion | 23 | mixed fast/ladder | locale seam (trivial) |
| 5 | scalar/uuid/pg_lsn/xid8 | ~38 | fast + digit-band ladders | none |
| 6 | date/time/interval arith | ~36 | fast core + USECS band ladder | tz-seam (proven) |
| 7 | timestamp + float rounding | ~30 | fast | tz-seam (proven) |
| 8 | varlena byte kernels | ~35 | fast (slice cores) | mcx-stubs (proven recipe) |
| 9 | ACL privilege seam (re-opens) | ~32 | release-gate seam | membership-oracle seam (proven) |
| 10 | varbit rows | ~25 | fast/ladder | none |
| 11 | network | ~20 | fast/ladder | none |
| 12 | geo fast subset | ~35 | fast + grid tier | none |
| 13 | mbconv utf8 arm | ~36 | release-gate (table n^1.5) | idle-box for big tables |
| 14 | oracle_compat + varchar | ~30 | ladder | mcx-stubs + encoding pin |
| 15 | typcache re-opens | ~25 | release-gate seam | typcache-seam (proven) |

Core plan = waves 1–9 ≈ 235 nominated rows (expected ~200 green after
in-wave skips/walls); waves 10–15 are the pre-screened bench (~170 more).
Waves 1–3 alone are ~130 rows at near-zero marginal setup (C vendored or
stub-shaped) — start there and gate-as-you-go.
