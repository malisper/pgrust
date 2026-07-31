#!/usr/bin/env python3
"""Seed generator for the datetime_engine_diff fuzz target.

Two seed families, both mechanical:

  * EncodeDateTime (selector arm 0): the pg_tm field grid, taken from the
    tm values reachable through timestamp2tm (Julian range endpoints, BC/AD
    boundary, month/day/hour/minute/second edges) crossed with all 5 DateStyles
    x 3 DateOrders, both print_tz states, and a zone-name set that straddles
    MAXTZLEN (10) so the truncating copy is witnessed. Plus the
    SINGLE-FIELD-DIFFERENCE witness pairs the campaign requires for the
    seven-field tm packing: each field, small deltas, both orders.
  * ISO week/year (arms 1-3): year x month x mday and year x week x wday grids
    over the same endpoints plus the i32 extremes, which is where the wrapping
    arithmetic C leaves to -fwrapv actually differs from checked arithmetic.

Text spellings are NOT this target's input language (it takes packed binary
fields, not text), so there is nothing to harvest from the regress SQL here —
that harvest belongs to datetime_io_diff, which owns the parse surface.
"""
import os
import struct
import sys

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                   "corpus", "datetime_engine_diff")

# (year, mon, mday, hour, min, sec, isdst)
BASE = (2026, 6, 15, 12, 30, 45, 0)
TMS = [
    BASE,
    (1, 1, 1, 0, 0, 0, 0),                  # AD epoch edge
    (0, 12, 31, 23, 59, 59, 0),             # year zero (BC boundary)
    (-1, 2, 29, 0, 0, 0, -1),               # BC + leap-day + "no zone" isdst
    (-4713, 11, 24, 0, 0, 0, 1),            # Julian day 0
    (294276, 12, 31, 23, 59, 59, 0),        # timestamp max
    (1999, 12, 31, 23, 59, 60, 0),          # leap-second-shaped second
    (1970, 1, 1, 0, 0, 0, 0),               # unix epoch
    (2000, 1, 1, 0, 0, 0, 0),               # postgres epoch
    (1, 1, 1, 24, 60, 61, 0),               # out-of-range time fields
    (-100, 3, 1, 1, 1, 1, -1),              # BC, post-leap
]
FSECS = [0, 1, -1, 999_999, -999_999, 1_000_000, 123_456]
TZS = [0, -28800, 28800, 3600, -3600, 1, -1, 86399, -86399]
TZNS = [b"", b"GMT", b"UTC", b"PST", b"PDT", b"CEST",
        b"America/Los_Angeles",          # > MAXTZLEN: truncation witness
        b"ABCDEFGHIJ",                   # exactly MAXTZLEN
        b"ABCDEFGHIJK",                  # MAXTZLEN + 1
        b"+05", b"-1030"]

YEARS = [0, 1, -1, 2, -2, 1970, 2000, 2026, -4713, 294276, 100, -100,
         2**31 - 1, -2**31, 2**31 - 2, -2**31 + 1]
MONS = [-1, 0, 1, 2, 3, 6, 11, 12, 13, 2**31 - 1, -2**31]
MDAYS = [-1, 0, 1, 4, 5, 27, 28, 29, 30, 31, 32, 2**31 - 1, -2**31]
WEEKS = [-5, -1, 0, 1, 2, 51, 52, 53, 54, 2**31 - 1, -2**31]
WDAYS = [-4, -1, 0, 1, 2, 3, 7, 8, 11, 2**31 - 1, -2**31]


def i32(v):
    return struct.pack("<i", v & 0xFFFFFFFF if v >= 0 else v)


def enc_arm(so, flags, tm, fsec, tz, tzn):
    b = bytes([0, so & 0xFF, flags])
    for f in tm:
        b += i32(f)
    return b + i32(fsec) + i32(tz) + tzn


def tri_arm(sel, a, b_, c):
    return bytes([sel]) + i32(a) + i32(b_) + i32(c)


def main():
    os.makedirs(OUT, exist_ok=True)
    seeds = []

    # --- EncodeDateTime: style/order x tm x zone grid -----------------------
    for so in range(15):                      # 5 styles x 3 orders
        for tm in TMS:
            for flags in (0, 1, 3):           # no tz / numeric tz / named tz
                tzn = b"PST" if flags == 3 else b""
                seeds.append(enc_arm(so, flags, tm, 500_000, -28800, tzn))
    for fsec in FSECS:
        for tz in TZS:
            for tzn in TZNS:
                seeds.append(enc_arm(0, 3, BASE, fsec, tz, tzn))
                seeds.append(enc_arm(6, 3, BASE, fsec, tz, tzn))

    # --- single-field-difference witness pairs (campaign obligation) --------
    # Each pair differs in EXACTLY one tm field so the contribution of that
    # field to the image is individually witnessed; without these, a
    # field-swap or shift mutant survives at full line coverage.
    for so in range(15):
        for field in range(7):
            for delta in (1, -1, 2, -2, 10, -10):
                tm = list(BASE)
                tm[field] = tm[field] + delta
                seeds.append(enc_arm(so, 3, BASE, 500_000, -28800, b"PST"))
                seeds.append(enc_arm(so, 3, tuple(tm), 500_000, -28800, b"PST"))
                seeds.append(enc_arm(so, 1, BASE, 500_000, -28800, b""))
                seeds.append(enc_arm(so, 1, tuple(tm), 500_000, -28800, b""))
        for d in (1, -1, 10, -10, 100_000, -100_000):
            seeds.append(enc_arm(so, 3, BASE, 500_000 + d, -28800, b"PST"))
            seeds.append(enc_arm(so, 3, BASE, 500_000, -28800 + d, b"PST"))
        for tzn in (b"P", b"PS", b"PST", b"PSTX", b"PACIFICSTAND",
                    b"PACIFICSTANDARD"):
            seeds.append(enc_arm(so, 3, BASE, 500_000, -28800, tzn))

    # --- ISO week/year grids ----------------------------------------------
    for y in YEARS:
        for m in MONS:
            for d in MDAYS:
                seeds.append(tri_arm(1, y, m, d))
        for w in WEEKS:
            seeds.append(tri_arm(2, y, w, 0))
            for wd in WDAYS:
                seeds.append(tri_arm(3, y, w, wd))

    n = 0
    for s in seeds:
        import hashlib
        p = os.path.join(OUT, "seed-" + hashlib.sha1(s).hexdigest()[:24])
        if not os.path.exists(p):
            with open(p, "wb") as f:
                f.write(s)
            n += 1
    print(f"wrote {n} new seeds ({len(seeds)} generated) to {OUT}")


if __name__ == "__main__":
    sys.exit(main())
