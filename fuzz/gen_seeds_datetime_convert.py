#!/usr/bin/env python3
"""Seed generator for the datetime_convert_diff fuzz target.

Three mechanical families, matching the target's five arms:

  * timestamp / timestamptz datums (arms 0-1): the Julian-range endpoints, the
    two not-finite sentinels, the POSIX/Postgres epochs, local-midnight and
    day-boundary values (where timestamp2tm's TMODULO borrow arm fires), and
    the pg_time_t representability edge that selects its "treat as GMT" arm.
  * date datums (arm 2): the IS_VALID_DATE endpoints, both infinities, and the
    DATE_END_JULIAN boundary that selects date2timestamptz's overflow arm.
  * time/timetz x Interval (arms 3-4): time on its 0..USECS_PER_DAY invariant
    endpoints crossed with intervals spanning the i64 usec extremes, the
    USECS_PER_DAY multiples that make the `result / USECS_PER_DAY *
    USECS_PER_DAY` fold-back branch flip, negative intervals (the `< 0` wrap
    arm), and the not-finite interval sentinels.

Plus the SINGLE-FIELD-DIFFERENCE witness pairs the campaign requires for both
struct packings here — Interval (time, day, month) and TimeTzADT (time, zone):
each field perturbed alone, small deltas, both orders. Line coverage cannot
detect their absence, and without them a byte-shift mutant in the field staging
survives an arbitrarily long campaign.

Text spellings are not this target's input language (it takes packed binary
datums), so there is nothing to harvest from the regress SQL here — that
harvest belongs to datetime_io_diff, which owns the parse surface.
"""
import os
import struct
import sys

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                   "corpus", "datetime_convert_diff")

USECS_PER_DAY = 86_400_000_000
MIN_TIMESTAMP = -211_813_488_000_000_000
END_TIMESTAMP = 9_223_371_331_200_000_000
# DATETIME_MIN_JULIAN - POSTGRES_EPOCH_JDATE .. DATE_END_JULIAN - epoch
MIN_DATE = -2_451_545
END_DATE = 2_147_483_494 - 2_451_545 + 1

TIMESTAMPS = [
    0,                                  # 2000-01-01 00:00:00 (postgres epoch)
    -1, 1,
    MIN_TIMESTAMP, MIN_TIMESTAMP + 1,
    END_TIMESTAMP - 1,
    -(2**63), 2**63 - 1,                # -infinity / +infinity sentinels
    -946_684_800_000_000,               # unix epoch as a postgres timestamp
    USECS_PER_DAY, -USECS_PER_DAY,      # exact day boundaries
    USECS_PER_DAY - 1, -USECS_PER_DAY + 1,
    -1_000_000, 1_000_000,              # +-1s straddling the TMODULO borrow
    2_147_483_647_000_000,              # near the pg_time_t/int32 seam
    -2_147_483_648_000_000,
    123_456,                            # sub-second only
    -123_456,
]

DATES = [
    0, 1, -1,
    MIN_DATE, MIN_DATE + 1,
    END_DATE - 1, END_DATE - 2,
    -(2**31), 2**31 - 1,                # -infinity / +infinity sentinels
    -730_120,                           # 0001-01-01
    -2_451_545 + 2_451_545,             # julian day 0 offset check
    10_957,                             # 2030-01-01
]

TIMES = [
    0, 1, USECS_PER_DAY, USECS_PER_DAY - 1,
    43_200_000_000,                     # noon
    123_456, USECS_PER_DAY // 2 + 1,
]

# (time, day, month) interval field grid
INTERVALS = [
    (0, 0, 0),
    (1, 0, 0), (-1, 0, 0),
    (USECS_PER_DAY, 0, 0), (-USECS_PER_DAY, 0, 0),
    (USECS_PER_DAY - 1, 0, 0), (-USECS_PER_DAY + 1, 0, 0),
    (2 * USECS_PER_DAY, 0, 0), (-2 * USECS_PER_DAY, 0, 0),
    (2**63 - 1, 0, 0), (-(2**63), 0, 0),      # i64 extremes: the -fwrapv face
    (2**63 - 1, 2**31 - 1, 2**31 - 1),
    (-(2**63), -(2**31), -(2**31)),           # INTERVAL_NOT_FINITE shapes
    (3_600_000_000, 0, 0), (-3_600_000_000, 0, 0),   # +-1 hour
    (0, 1, 0), (0, -1, 0), (0, 0, 1), (0, 0, -1),
]

ZONES = [0, 1, -1, 3600, -3600, 57_599, -57_599, 15 * 3600, -15 * 3600]


def sel(arm, n=0):
    """A selector byte landing on `arm` (sel % 7), varied by n for corpus
    diversity without changing the arm."""
    return bytes([arm + 7 * (n % 36)])


def emit(seeds, arm, payload, tag):
    seeds[f"seed-a{arm}-{tag}"] = sel(arm, len(seeds)) + payload


def main():
    os.makedirs(OUT, exist_ok=True)
    seeds = {}

    # ---- arms 0/1: timestamp / timestamptz datums ----
    for arm in (0, 1):
        for ts in TIMESTAMPS:
            emit(seeds, arm, struct.pack("<q", ts), f"ts{ts}")

    # ---- arm 2: date datums ----
    for d in DATES:
        emit(seeds, 2, struct.pack("<i", d), f"date{d}")

    # ---- arm 3: time x interval ----
    for t in TIMES:
        for (it, idy, imo) in INTERVALS:
            emit(seeds, 3, struct.pack("<qqii", t, it, idy, imo),
                 f"t{t}-i{it}_{idy}_{imo}")

    # ---- arm 4: timetz x interval ----
    for t in TIMES[:4]:
        for z in ZONES:
            for (it, idy, imo) in INTERVALS[:11]:
                emit(seeds, 4, struct.pack("<qiqii", t, z, it, idy, imo),
                     f"t{t}-z{z}-i{it}_{idy}_{imo}")

    # ---- SINGLE-FIELD-DIFFERENCE witness pairs ----
    # Interval (time, day, month): perturb exactly one field, both directions,
    # so a mutant that swaps or shifts the staging is separated by SOME seed.
    base_iv = (1_000_000, 5, 7)
    base_t = 43_200_000_000
    for fi, delta in [(0, 1), (0, -1), (1, 1), (1, -1), (2, 1), (2, -1)]:
        fields = list(base_iv)
        fields[fi] += delta
        emit(seeds, 3,
             struct.pack("<qqii", base_t, fields[0], fields[1], fields[2]),
             f"witness-iv-f{fi}{'+' if delta > 0 else '-'}")
    # the unperturbed baseline both sides of every pair differ from
    emit(seeds, 3, struct.pack("<qqii", base_t, *base_iv), "witness-iv-base")

    # TimeTzADT (time, zone) x Interval: same discipline on the 5-field packing.
    base_z = 3600
    for fi, delta in [(0, 1), (0, -1), (1, 1), (1, -1)]:
        t, z = base_t, base_z
        if fi == 0:
            t += delta
        else:
            z += delta
        emit(seeds, 4, struct.pack("<qiqii", t, z, *base_iv),
             f"witness-tz-f{fi}{'+' if delta > 0 else '-'}")
    emit(seeds, 4, struct.pack("<qiqii", base_t, base_z, *base_iv),
         "witness-tz-base")
    # and one pair differing ONLY in the interval, zone held fixed, to separate
    # a mutant that reads the interval from the timetz offsets
    for fi, delta in [(0, 1), (1, 1), (2, 1)]:
        fields = list(base_iv)
        fields[fi] += delta
        emit(seeds, 4, struct.pack("<qiqii", base_t, base_z, *fields),
             f"witness-tz-iv-f{fi}+")

    # ---- arms 5/6: tz abbreviations over the PINNED table ----
    # Every pinned abbrev (exact, case-varied, truncated-at-TOKMAXLEN), the
    # DYNTZ token, near-misses that must MISS, and prefix strings whose
    # longest-prefix match is a pinned abbrev followed by trailing text.
    PINNED = ["aaa", "bbb", "ccc", "dddddddddd", "eee", "gmtdyn", "zzz"]
    NEARMISS = ["aa", "aaaa", "aab", "ab", "gmt", "gmtdy", "gmtdynx", "zz",
                "zzzz", "ddddddddd", "ddddddddddd", "", "a",
                "AAA", "GmtDyn", "ZZZ"]
    for tok in PINNED + NEARMISS:
        if tok:
            emit(seeds, 5, tok.encode(), f"abbrev-{tok}")
    for tok in PINNED:
        emit(seeds, 6, tok.encode(), f"prefix-exact-{tok}")
        for tail in ["+05", "-1", "x", "0", " rest", "aaa"]:
            emit(seeds, 6, (tok + tail).encode(),
                 f"prefix-{tok}-tail{tail.strip() or 'sp'}")
    for tok in NEARMISS:
        if tok:
            emit(seeds, 6, tok.encode(), f"prefix-miss-{tok}")
    # non-alphabetic leads: the prefix scanner stops immediately
    for tok in ["1aaa", "+aaa", "-aaa", ".aaa", "\x7faaa"]:
        emit(seeds, 6, tok.encode("latin-1"), f"prefix-nonalpha-{tok!r}")

    for name, blob in seeds.items():
        with open(os.path.join(OUT, name), "wb") as f:
            f.write(blob)
    print(f"wrote {len(seeds)} seeds to {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
