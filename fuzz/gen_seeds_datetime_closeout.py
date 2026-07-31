#!/usr/bin/env python3
"""Seed generator for the datetime_closeout_diff fuzz target (p1-lanel2).

Mechanical families matching the target's eight arms (selector % 8, see
core/src/datetime_closeout_diff.rs):

  0 extract_date + date skip-support:  every unit token x a date grid that
    includes both infinities (the oscillating-NULL and monotonic-Infinity
    arms), BC dates (the year<=0 adjustment arms), and the epoch.
  1/2 time/timetz part-extract:        every unit token x times with and
    without fractional seconds (the int64_div_fast_to_numeric fractional
    plane) x zones spanning the +-16h invariant.
  3 recv wrappers:                     valid wire frames for each width,
    range-violating frames (the error arms), short frames.
  4 in wrappers (hard+soft):           regress-style spellings per input
    kind, non-finite spellings, error shapes (the soft-error plane), the
    time-only input that drives date_in's non-DATE dtype arm.
  5 out/conversion wrappers:           non-finite sentinels (the
    PG_RETURN_NULL faces), out-of-range shapes, div-by-zero factor.
  6 cmp families:                      equal / adjacent / extreme pairs,
    plus SINGLE-FIELD-DIFFERENCE witness pairs for the two struct packings
    (TimeTzADT and Interval) — each field perturbed alone, both orders.
  7 typmod + in_range:                 typmod values across the
    negative-error / valid / reduce-warning arms, n=2 arrays, negative
    offsets (error arm) and +overflow saturation shapes.
"""
import os
import struct
import sys

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                   "corpus", "datetime_closeout_diff")

USECS_PER_DAY = 86_400_000_000

UNITS = [
    "microseconds", "milliseconds", "second", "minute", "hour", "day",
    "month", "quarter", "week", "year", "decade", "century", "millennium",
    "julian", "isoyear", "isodow", "dow", "doy", "epoch", "timezone",
    "timezone_hour", "timezone_minute", "junk",
]

DATES = [
    0, 1, -1,
    -(2**31), 2**31 - 1,                # -infinity / +infinity sentinels
    -2_451_545,                         # MIN date (4714-11-24 BC)
    2_932_896,                          # MAX finite output date
    -730_120,                           # 0001-01-01
    -730_121,                           # 1 BC (year<=0 arms)
    10_957,                             # 2030-01-01
    -365, 365,
]

TIMES = [
    0, 1, USECS_PER_DAY, USECS_PER_DAY - 1,
    43_200_000_000,                     # noon
    45_296_789_000,                     # 12:34:56.789 — fractional numeric
    45_296_000_001,                     # 1-usec fraction
    123_456,
]

ZONES = [0, 1, -1, 3600, -3600, 57_599, -57_599, 12 * 3600, -12 * 3600]

IN_TEXTS = {
    0: [b"2024-01-05", b"epoch", b"infinity", b"-infinity", b"now", b"today",
        b"5874897-12-31", b"0000-01-01", b"zz", b"04:05:06", b"J2451545",
        b"allballs",
        b"1999-01-08 04:05:06", b"January 8, 1999"],
    1: [b"12:34:56.789", b"04:05 PM", b"allballs", b"24:00:00", b"25:00:00",
        b"04:05:06.789+08", b"zz", b"123456", b"04:05"],
    2: [b"04:05:06+08", b"04:05:06-08:15", b"04:05:06", b"04:05:06 GMT",
        b"12:34:56.789-15:59", b"04:05:06+16", b"zz", b"24:00:00+00"],
    3: [b"2024-01-05 12:34:56", b"epoch", b"infinity", b"-infinity",
        b"2024-01-05T12:34:56.789", b"294276-12-31 23:59:59.999999",
        b"1999-01-08 04:05:06 GMT", b"zz", b"now", b"yesterday"],
    4: [b"2024-01-05 12:34:56+08", b"epoch", b"infinity", b"-infinity",
        b"2024-01-05 12:34:56 GMT", b"zz", b"2024-06-15 00:00:00-15:59"],
    5: [b"1 year 2 mons 3 days 04:05:06", b"@ 2 hours ago", b"P1Y2M3DT4H5M6S",
        b"infinity", b"-infinity", b"178956970 years", b"-178956970 years",
        b"1.5 hours", b"zz", b"PT0S", b"3 weeks", b"-2147483648 months"],
}

INTERVALS = [
    (0, 0, 0), (1, 0, 0), (-1, 0, 0),
    (3_600_000_000, 0, 0), (-3_600_000_000, 0, 0),
    (USECS_PER_DAY, 1, 1),
    (2**63 - 1, 0, 0), (-(2**63), 0, 0),
    (2**63 - 1, 2**31 - 1, 2**31 - 1),
    (-(2**63), -(2**31), -(2**31)),
]


def sel(a, n=0):
    return bytes([a + 8 * (n % 32)])


def main():
    os.makedirs(OUT, exist_ok=True)
    seeds = {}

    def emit(a, payload, tag):
        seeds[f"seed-a{a}-{tag}"] = sel(a, len(seeds)) + payload

    # ---- arm 0: extract_date + skip-support ----
    for d in DATES:
        for u in UNITS:
            emit(0, struct.pack("<i", d) + u.encode(), f"d{d}-{u}")

    # ---- arm 1: time part/extract ----
    for t in TIMES:
        for u in UNITS:
            emit(1, struct.pack("<q", t) + u.encode(), f"t{t}-{u}")

    # ---- arm 2: timetz part/extract ----
    for t in TIMES[:6]:
        for z in ZONES:
            for u in ["second", "epoch", "timezone", "timezone_hour",
                      "timezone_minute", "milliseconds", "microseconds",
                      "minute", "hour", "day", "junk"]:
                emit(2, struct.pack("<qi", t, z) + u.encode(),
                     f"t{t}-z{z}-{u}")

    # ---- arm 3: recv wire frames ----
    WIRES = [
        struct.pack(">i", 0), struct.pack(">i", 10957),
        struct.pack(">i", 2**31 - 1), struct.pack(">i", -(2**31)),
        struct.pack(">i", 3_000_000),           # out-of-range date
        struct.pack(">q", 0), struct.pack(">q", USECS_PER_DAY),
        struct.pack(">q", USECS_PER_DAY + 1),   # time out of range
        struct.pack(">q", -1),
        struct.pack(">qi", 43_200_000_000, 3600),
        struct.pack(">qi", 0, 16 * 3600),       # zone displacement error
        struct.pack(">qi", 0, -57599),
        struct.pack(">qii", 3_600_000_000, 3, 2),   # interval frame
        struct.pack(">qii", -(2**63), -(2**31), -(2**31)),
        b"", b"\x01", b"\xff" * 20, b"\x00" * 16,
    ]
    for i, w in enumerate(WIRES):
        for tb in (0, 3, 0x85):
            emit(3, bytes([tb, tb ^ 0x5A]) + w, f"wire{i}-t{tb}")

    # ---- arm 4: in wrappers (hard + soft) ----
    for which, texts in IN_TEXTS.items():
        for tx in texts:
            for style in (1, 7, 12):
                for tb in (0, 3, 0x80):
                    emit(4, bytes([which, style, tb, tb]) + tx,
                         f"w{which}-s{style}-t{tb}-{tx[:12].decode('latin-1').replace(' ', '_').replace('/', '_')}")

    # ---- arm 5: out + conversions ----
    def a5(date, ts, origin, iv, factor):
        return struct.pack("<iqqqiid", date, ts, origin, *iv, factor)

    for ts in [0, 1, -1, 2**63 - 1, -(2**63), 9_662 * USECS_PER_DAY,
               -211_813_488_000_000_000, 9_223_371_331_200_000_000 - 1]:
        emit(5, a5(0, ts, 0, (3_600_000_000, 1, 2), 2.5), f"ts{ts}")
    emit(5, a5(0, 0, USECS_PER_DAY, (USECS_PER_DAY, 0, 0), 0.0), "divzero")
    emit(5, a5(2_932_896, 0, 0, (1, 0, 0), float("nan")), "nan")
    emit(5, a5(-(2**31), 0, 0, (2**63 - 1, 0, 0), float("inf")), "inf")
    emit(5, a5(2**31 - 1, 0, 0, (-(2**63), -(2**31), -(2**31)), -1.5), "ninf")
    for iv in INTERVALS:
        emit(5, a5(10957, 86_400_000_123, 456, iv, 1.25), f"iv{iv[0]}_{iv[1]}_{iv[2]}")

    # ---- arm 6: cmp families ----
    def a6(d1, d2, t1, t2, z1, z2, iv1, iv2):
        return struct.pack("<iiqqiiqiiqii", d1, d2, t1, t2, z1, z2,
                           iv1[0], iv1[1], iv1[2], iv2[0], iv2[1], iv2[2])

    PAIRS = [(0, 0), (1, 2), (2, 1), (-1, 1), (2**31 - 1, -(2**31)),
             (10957, 10957), (10957, 10958)]
    for (x, y) in PAIRS:
        emit(6, a6(x, y, x * 1_000_000, y * 1_000_000, 3600, -3600,
                   (x, 0, 0), (y, 0, 0)), f"pair{x}_{y}")
    # SINGLE-FIELD-DIFFERENCE witness pairs (campaign obligation): TimeTzADT
    # (time, zone) and Interval (time, day, month), each field perturbed
    # alone, both orders, against a fixed baseline.
    bt, bz, biv = 43_200_000_000, 3600, (1_000_000, 5, 7)
    emit(6, a6(100, 100, bt, bt, bz, bz, biv, biv), "witness-base")
    for fi, delta in [(0, 1), (0, -1), (1, 1), (1, -1), (2, 1), (2, -1)]:
        iv2 = list(biv)
        iv2[fi] += delta
        emit(6, a6(100, 100, bt, bt, bz, bz, biv, tuple(iv2)),
             f"witness-iv-f{fi}{'p' if delta > 0 else 'm'}")
        emit(6, a6(100, 100, bt, bt, bz, bz, tuple(iv2), biv),
             f"witness-iv-r{fi}{'p' if delta > 0 else 'm'}")
    for (dt, dz) in [(1, 0), (-1, 0), (0, 1), (0, -1)]:
        emit(6, a6(100, 100, bt, bt + dt, bz, bz + dz, biv, biv),
             f"witness-tz-{dt}_{dz}")
        emit(6, a6(100, 100, bt + dt, bt, bz + dz, bz, biv, biv),
             f"witness-tz-r{dt}_{dz}")
    for (d1, d2) in [(100, 101), (101, 100)]:
        emit(6, a6(d1, d2, bt, bt, bz, bz, biv, biv), f"witness-d{d1}_{d2}")

    # ---- arm 7: typmod + in_range ----
    def a7(b0, b1, v0, tmout, val, base, iv, flags):
        return (bytes([b0, b1]) + struct.pack("<iiqqqii", v0, tmout, val,
                                              base, *iv) + bytes([flags]))

    for b0 in (0, 1, 2, 3, 4, 5, 6, 7):
        for flags in range(4):
            for v in (-3, -1, 0, 3, 6, 7, 9):
                emit(7, a7(b0, flags, v, v, 12, 34,
                           (3_600_000_000, 0, 0), flags),
                     f"tm{b0}-{flags}-v{v}")
    # in_range shapes: negative offset (error), +overflow saturation, normal,
    # date conversion-error shapes (>= TIMESTAMP_END_JULIAN date)
    for flags in range(4):
        emit(7, a7(0, flags, 3, -1, 0, 2**63 - 1, (-5, 0, 0), flags),
             f"ir-neg-{flags}")
        emit(7, a7(0, flags, 3, -1, 0, 2**63 - 1, (2**63 - 1, 0, 0), flags),
             f"ir-ovf-{flags}")
        emit(7, a7(0, flags, 3, -1, 43_200_000_000, 43_100_000_000,
                   (3_600_000_000, 0, 0), flags), f"ir-norm-{flags}")
        emit(7, a7(0, flags, 3, 2**31 - 1, 106_752_000, 106_752_001,
                   (1, 0, 0), flags), f"ir-dateovf-{flags}")

    for name, blob in seeds.items():
        with open(os.path.join(OUT, name), "wb") as f:
            f.write(blob)
    print(f"wrote {len(seeds)} seeds to {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
