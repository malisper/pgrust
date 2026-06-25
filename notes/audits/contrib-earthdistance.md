# Audit: contrib-earthdistance

C source: `contrib/earthdistance/earthdistance.c` (postgres-18.3)
c2rust: `c2rust-runs/contrib-earthdistance/src/earthdistance.rs`
Port: `crates/contrib-earthdistance/src/lib.rs`

## Function inventory

| C function | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|
| `degtorad` | earthdistance.c:33 | lib.rs `degtorad` | MATCH | `(degrees / 360.0) * TWO_PI`; identical. |
| `geo_distance_internal` | earthdistance.c:56 | lib.rs `geo_distance_internal` | MATCH | See detailed check below. |
| `geo_distance` (Datum entry) | earthdistance.c:97 | lib.rs `geo_distance` / `geo_distance_datum` | MATCH | fmgr arg extraction (`PG_GETARG_POINT_P`) is boundary glue; port takes `&Point` directly. `geo_distance_datum` mirrors `PG_RETURN_FLOAT8` via `Datum::from_f64`. |
| `Pg_magic_func` | macro `PG_MODULE_MAGIC_EXT` | — | N/A | Module-magic ABI boilerplate; not logic, intentionally not reproduced. |
| `pg_finfo_geo_distance` | macro `PG_FUNCTION_INFO_V1` | — | N/A | fmgr finfo boilerplate; not logic. |

## Constants verified

- `EARTH_RADIUS = 3958.747716` — port `pub const EARTH_RADIUS: f64 = 3958.747716` — MATCH.
- `TWO_PI = 2.0 * M_PI` — port `2.0 * std::f64::consts::PI` — MATCH. `M_PI` literal `3.14159265358979323846` and `std::f64::consts::PI` round to the same `f64`.
- `M_PI` comparison in `longdiff > M_PI` — port uses `std::f64::consts::PI` — MATCH.

## geo_distance_internal detailed check

- `long1/lat1/long2/lat2 = degtorad(pt.x/pt.y)` — MATCH.
- `longdiff = fabs(long1 - long2)` -> `(long1 - long2).abs()` — MATCH.
- `if (longdiff > M_PI) longdiff = TWO_PI - longdiff` — MATCH (predicate and assignment identical).
- haversine: C computes `sin(fabs(lat1-lat2)/2.)` twice; port binds `latdiff_half = (lat1-lat2).abs()/2.0` and squares `latdiff_half.sin()`. Pure deterministic functions, identical inputs => bit-identical result. MATCH.
- `cos(lat1)*cos(lat2)*sin(longdiff/2.)*sin(longdiff/2.)` -> `lat1.cos()*lat2.cos()*(longdiff/2.0).sin()*(longdiff/2.0).sin()` — same operand order/associativity. MATCH.
- `if (sino > 1.) sino = 1.` — MATCH.
- `return 2. * EARTH_RADIUS * asin(sino)` -> `2.0 * EARTH_RADIUS * sino.asin()` — MATCH.

## Datum boundary

`Float8GetDatum` reinterprets the IEEE-754 bits (`*(int64*)&X`); port `Datum::from_f64` uses `value.to_bits()`. Bit-for-bit identical. MATCH.

## Seam audit

Pure leaf module — no outward seam calls, no seam crate, none required. No findings.

## Verdict

PASS. Every function MATCH (or N/A fmgr boilerplate). No seams, no findings. Crate tests pass (4/4, including PostgreSQL reference distance values).
