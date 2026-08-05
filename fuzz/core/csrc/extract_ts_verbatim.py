#!/usr/bin/env python3
"""Extract verbatim C function/table definitions from the vendored
PostgreSQL 18.3 checkout (../pgrust-fabled/vendor/postgres-src @
62d6c7d3df6287f1bd83199c1a746e50d31571a0) for the timestamp_diff
differential-fuzz oracle (crate adt/adt_timestamp, lane p1-laney).

Companion to extract_verbatim.py (adt_date/adt_datetime family, lane
p1-lanel): that script's output pg_datetime_verbatim.inc carries the whole
datetime.c parse/encode core + timestamp2tm + the interval engine; THIS one
adds the timestamp.c SQL-entry bodies the adt_timestamp crate ships.
Bodies are copied BYTE-FOR-BYTE. Output committed; this script is
provenance — re-run to refresh.
"""
import sys

sys.path.insert(0, __file__.rsplit("/", 1)[0])
from extract_verbatim import extract_fn, extract_var, load  # noqa: E402


def main(out):
    ts = load("src/backend/utils/adt/timestamp.c")

    w = open(out, "w")

    def sec(title, text):
        w.write(f"\n/* ==== VERBATIM: {title} ==== */\n\n")
        w.write(text)

    # ---- text/binary I/O + typmod ----
    for f in [
        "anytimestamp_typmod_check",
        "timestamp_in", "timestamp_out", "timestamp_recv", "timestamp_send",
        "timestamptz_in", "timestamptz_out", "timestamptz_recv",
        "timestamptz_send",
        "AdjustTimestampForTypmod", "timestamp_scale",
        "EncodeSpecialTimestamp",
        "interval_in", "interval_out", "interval_recv", "interval_send",
        "interval_scale", "AdjustIntervalForTypmod",
        "EncodeSpecialInterval",
    ]:
        sec(f"src/backend/utils/adt/timestamp.c {f}", extract_fn(ts, f))

    # ---- calendar/civil kernels (timestamp2tm/interval2itm/dt2time ride
    # pg_datetime_verbatim.inc; these are the timestamp.c-only remainder) ----
    for f in [
        "time2t",
        "SetEpochTimestamp",
        "tm2timestamp", "dt2local", "itm2interval", "itmin2interval",
        "timestamp2timestamptz_opt_overflow", "timestamp2timestamptz",
        "timestamptz2timestamp",
        "parse_sane_timezone", "make_timestamp_internal",
        "make_timestamp", "make_timestamptz", "make_timestamptz_at_timezone",
        "make_interval",
        "lookup_timezone",
    ]:
        sec(f"src/backend/utils/adt/timestamp.c {f}", extract_fn(ts, f))

    # DecodeTimezoneName/DecodeTimezoneNameToTz live in datetime.c (not in
    # pg_datetime_verbatim.inc — lanel's targets never reach them).
    dt = load("src/backend/utils/adt/datetime.c")
    sec("src/backend/utils/adt/datetime.c DecodeTimezoneName",
        extract_fn(dt, "DecodeTimezoneName"))
    # DecodeTimezoneNameToTz returns pg_tz * — a return-type line the shared
    # extractor's prefix list does not recognize; back up one line by hand.
    i = next(i for i, ln in enumerate(dt)
             if ln.startswith("DecodeTimezoneNameToTz("))
    depth = 0
    seen = False
    for k in range(i, len(dt)):
        depth += dt[k].count("{") - dt[k].count("}")
        if "{" in dt[k]:
            seen = True
        if seen and depth == 0:
            sec("src/backend/utils/adt/datetime.c DecodeTimezoneNameToTz",
                "".join(dt[i - 1:k + 1]))
            break

    # ---- comparison / min-max (band-proved; fuzz supplement) ----
    for f in [
        "timestamp_cmp_internal", "timestamp_smaller", "timestamp_larger",
        "interval_cmp_value", "interval_cmp_internal", "interval_sign",
        "interval_smaller", "interval_larger",
    ]:
        sec(f"src/backend/utils/adt/timestamp.c {f}", extract_fn(ts, f))

    # ---- arithmetic ----
    for f in [
        "timestamp_mi", "timestamp_pl_interval", "timestamp_mi_interval",
        "timestamptz_pl_interval_internal", "timestamptz_mi_interval_internal",
        "timestamptz_pl_interval", "timestamptz_mi_interval",
        "finite_interval_pl", "finite_interval_mi",
        "interval_um_internal", "interval_um",
        "interval_pl", "interval_mi", "interval_mul", "mul_d_interval",
        "interval_div",
        "interval_justify_interval", "interval_justify_hours",
        "interval_justify_days",
        "timestamp_age", "timestamptz_age",
        "timestamp_bin", "timestamptz_bin",
        "timestamp_izone", "timestamptz_izone",
    ]:
        sec(f"src/backend/utils/adt/timestamp.c {f}", extract_fn(ts, f))

    # ---- pure difference helpers (backend instrument/timeout callers; the
    # adt_timestamp crate ships them as pure arithmetic — excreview-flagfix
    # 2026-07-31 measures them differentially instead of carving) ----
    for f in [
        "TimestampDifference", "TimestampDifferenceMilliseconds",
        "TimestampDifferenceExceeds", "TimestampDifferenceExceedsSeconds",
    ]:
        sec(f"src/backend/utils/adt/timestamp.c {f}", extract_fn(ts, f))

    # ---- trunc / part / extract ----
    for f in [
        "timestamp_trunc", "timestamptz_trunc_internal", "timestamptz_trunc",
        "timestamptz_trunc_zone", "interval_trunc",
        "NonFiniteTimestampTzPart", "timestamp_part_common", "timestamp_part",
        "extract_timestamp", "timestamptz_part_common", "timestamptz_part",
        "extract_timestamptz", "NonFiniteIntervalPart", "interval_part_common",
        "interval_part", "extract_interval",
    ]:
        sec(f"src/backend/utils/adt/timestamp.c {f}", extract_fn(ts, f))

    # ---- interval aggregate core (state struct + statics + finals) ----
    # typedef struct (extract_var doesn't match typedefs): byte-copy the
    # block from "typedef struct IntervalAggState" through "} IntervalAggState;".
    start = next(i for i, ln in enumerate(ts)
                 if ln.startswith("typedef struct IntervalAggState"))
    end = next(i for i, ln in enumerate(ts[start:], start)
               if ln.startswith("} IntervalAggState;"))
    sec("src/backend/utils/adt/timestamp.c IntervalAggState typedef",
        "".join(ts[start:end + 1]))
    for f in [
        "makeIntervalAggState",
        "do_interval_accum", "do_interval_discard",
        "interval_avg_combine", "interval_avg_serialize",
        "interval_avg_deserialize", "interval_avg", "interval_sum",
    ]:
        sec(f"src/backend/utils/adt/timestamp.c {f}", extract_fn(ts, f))

    w.close()


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "pg_timestamp_verbatim.inc")
