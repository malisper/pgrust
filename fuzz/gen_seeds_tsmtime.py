#!/usr/bin/env python3
"""gen_seeds_tsmtime.py — directed seed corpus for tsm_system_time_diff.

Emits, per arm: boundary seeds plus SINGLE-FIELD-DIFFERENCE WITNESS PAIRS
(each derived field — millis, seed, nblocks, clock base, clock step,
maxoffset, spc, pages, tuples — varied by a small delta in both directions
off a base seed, so every field's individual contribution to the verdict is
witnessed — the Lane-0B seeding obligation). Deterministic; output
committed. Sibling of gen_seeds_tsmrows.py.
"""
import struct
from pathlib import Path

OUT = Path(__file__).resolve().parent / "corpus" / "tsm_system_time_diff"
OUT.mkdir(parents=True, exist_ok=True)

seeds = {}


def put(name: str, data: bytes):
    assert name not in seeds, name
    seeds[name] = data


def arm0(millis: float, seed: int, nblocks: int, clockbase: int) -> bytes:
    return bytes([0]) + struct.pack("<dIHI", millis, seed, nblocks, clockbase)


def draws(*trips) -> bytes:
    """Per-draw bytes: (delta_u16, nbdelta_u8, maxoffset_u8) triples."""
    out = b""
    for dt, nbd, mo in trips:
        out += struct.pack("<HBB", dt, nbd, mo)
    return out


def arm12(sel: int, seed: int, millis_q: int, nblocks0: int, clockbase: int,
          d: bytes, tail: bytes = b"") -> bytes:
    return (bytes([sel]) + struct.pack("<IIHI", seed, millis_q, nblocks0,
                                       clockbase) + d + tail)


def arm3(flags: int, limit: float, spc: float, pages: int,
         tuples: float) -> bytes:
    return bytes([3, flags]) + struct.pack("<ddId", limit, spc, pages, tuples)


def arm4(n: int, seed: int) -> bytes:
    return bytes([4]) + struct.pack("<IQ", n, seed)


# ---- arm 0: begin (fields: millis, seed, nblocks, clockbase) ----
B = (5.0, 1000, 64, 100)
put("a0_base", arm0(*B))
for fld, deltas in ((0, (-1.0, 1.0)), (1, (-1, 1)), (2, (-1, 1)),
                    (3, (-1, 1))):
    for d in deltas:
        v = list(B)
        v[fld] += d
        put(f"a0_w_f{fld}_{'p' if d > 0 else 'm'}1", arm0(*v))
put("a0_neg_millis", arm0(-3.0, 1000, 64, 0))
put("a0_nan_millis", arm0(float("nan"), 1000, 64, 0))
put("a0_negzero_millis", arm0(-0.0, 1000, 64, 0))
put("a0_inf_millis", arm0(float("inf"), 1000, 64, 0))
put("a0_zero_millis", arm0(0.0, 1000, 64, 0))  # immediate expiry
put("a0_tiny_millis", arm0(5e-324, 1000, 64, 0))
put("a0_empty_rel", arm0(10.0, 42, 0, 0))

# ---- arm 1: single-scan walk ----
# base: generous budget (1000ms), steady 1ms steps (dt 16 -> ~1.05ms).
D = draws(*[(16, 8, 20)] * 48)  # nbdelta 8 => delta 0 (centered)
W = (42, 16000, 13, 5)
put("a1_base", arm12(1, *W, D))
for fld in range(4):
    for d in (-1, 1):
        v = list(W)
        v[fld] += d
        put(f"a1_w_f{fld}_{'p' if d > 0 else 'm'}1", arm12(1, *v, D))
# clock-step witness pair: first draw's dt one tick either way
for tag, dt0 in (("m1", 15), ("p1", 17)):
    put(f"a1_w_dt_{tag}",
        arm12(1, *W, draws((dt0, 8, 20)) + D[4:]))
# maxoffset witness pair on the first block
for tag, mo0 in (("m1", 19), ("p1", 21)):
    put(f"a1_w_mo_{tag}",
        arm12(1, *W, draws((16, 8, mo0)) + D[4:]))
# nbdelta witness pair on the second draw (first is the pinning draw)
for tag, nbd in (("m1", 7), ("p1", 9)):
    put(f"a1_w_nbd_{tag}",
        arm12(1, *W, draws((16, 8, 20), (16, nbd, 20)) + D[8:]))
put("a1_empty_rel", arm12(1, 42, 16000, 0, 5, D))
put("a1_one_block", arm12(1, 42, 16000, 1, 5, D))
put("a1_exhaust_blocks", arm12(1, 3, 4 * 10**6, 4, 5, D))  # budget outlasts rel
put("a1_zero_budget", arm12(1, 42, 0, 13, 5, D))           # expires on draw 1
# budget expires mid-scan: 5ms budget, ~4.3ms per step (dt 65535)
put("a1_expire_midscan",
    arm12(1, 42, 80, 13, 5, draws(*[(65535, 8, 20)] * 48)))
# budget/elapsed near-tie: dt 16000 steps = 1048.576ms; budget 1048.5625ms
# (q 16777) expires exactly at the second read; +/-1q witnesses the compare
for tag, q in (("tie", 16777), ("tie_m1", 16776), ("tie_p1", 16778)):
    put(f"a1_w_budget_{tag}",
        arm12(1, 42, q, 13, 5, draws(*[(16000, 8, 20)] * 48)))
put("a1_zero_maxoffset", arm12(1, 9, 16000, 8, 5, draws(*[(16, 8, 0)] * 24)))
put("a1_bigrel", arm12(1, 7, 16000, 65535, 5, D))

# ---- arm 2: rescan walk (adds seed2, millis2_q) ----
T = struct.pack("<II", 999, 8000)
W2 = (7, 16000, 11, 9)
D2 = draws(*[(16, 8, 16)] * 16)
put("a2_base", arm12(2, *W2, D2, T + D2))
for fld in range(4):
    for d in (-1, 1):
        v = list(W2)
        v[fld] += d
        put(f"a2_w_f{fld}_{'p' if d > 0 else 'm'}1",
            arm12(2, *v, D2, T + D2))
for tag, (s2, q2) in (("seed2_p1", (1000, 8000)), ("seed2_m1", (998, 8000)),
                      ("q2_p1", (999, 8001)), ("q2_m1", (999, 7999))):
    put(f"a2_w_{tag}",
        arm12(2, *W2, D2, struct.pack("<II", s2, q2) + D2))
# shrink-heavy rescan (nbdelta biased low -> nblocks below first scan's)
put("a2_shrink",
    arm12(2, 7, 16000, 40, 9, draws(*[(16, 8, 16)] * 16),
          T + draws(*[(16, 0, 16)] * 16)))
# rescan with zero second budget (start_time reinit + immediate expiry)
put("a2_zero_budget2",
    arm12(2, 7, 16000, 11, 9, D2, struct.pack("<II", 999, 0) + D2))

# ---- arm 3: getsamplesize (fields: flags, limit, spc, pages, tuples) ----
G = (1, 500.0, 4.0, 64, 1000.0)
put("a3_base", arm3(*G))
for fld, deltas in ((1, (-1.0, 1.0)), (2, (-1.0, 1.0)), (3, (-1, 1)),
                    (4, (-1.0, 1.0))):
    for d in deltas:
        v = list(G)
        v[fld] += d
        put(f"a3_w_f{fld}_{'p' if d > 0 else 'm'}1", arm3(*v))
put("a3_nonconst", arm3(0, 500.0, 4.0, 64, 1000.0))
put("a3_nullconst", arm3(3, 500.0, 4.0, 64, 1000.0))
put("a3_neg_limit", arm3(1, -1.0, 4.0, 64, 1000.0))
put("a3_nan_limit", arm3(1, float("nan"), 4.0, 64, 1000.0))
put("a3_inf_limit", arm3(1, float("inf"), 4.0, 64, 1000.0))
put("a3_zero_spc", arm3(1, 500.0, 0.0, 64, 1000.0))  # npages = millis arm
put("a3_zero_pages", arm3(1, 500.0, 4.0, 0, 1000.0))
put("a3_zero_tuples", arm3(1, 500.0, 4.0, 64, 0.0))
put("a3_neg_tuples", arm3(1, 500.0, 4.0, 64, -5.0))
put("a3_nan_tuples", arm3(1, 500.0, 4.0, 64, float("nan")))
put("a3_inf_tuples", arm3(1, 500.0, 4.0, 64, float("inf")))
put("a3_dense", arm3(1, 500.0, 4.0, 4, 1e6))
put("a3_halfround", arm3(1, 10.0, 4.0, 7, 2.5))  # rint ties-to-even path
# default-1000 path with a LARGE relation (rows-unit P3 lesson: small
# relations mask the default via the pages clamp)
put("a3_nonconst_bigrel", arm3(0, 500.0, 4.0, 4096, 1e6))
put("a3_nullconst_bigrel", arm3(3, 500.0, 4.0, 4096, 1e6))
put("a3_neg_limit_bigrel", arm3(1, -7.0, 4.0, 4096, 1e6))
put("a3_nan_limit_bigrel", arm3(1, float("nan"), 4.0, 4096, 1e6))
# spc = 1.0 default-path seeds: npages == millis exactly, so the default
# 1000 constant is WITNESSED un-rounded (injection P4 lesson: spc = 4.0
# divides the 1000-vs-1001 delta down to 0.25 and rint absorbs it)
put("a3_nonconst_spc1", arm3(0, 500.0, 1.0, 4096, 1e6))
put("a3_nullconst_spc1", arm3(3, 500.0, 1.0, 4096, 1e6))
put("a3_neg_limit_spc1", arm3(1, -7.0, 1.0, 4096, 1e6))
put("a3_nan_limit_spc1", arm3(1, float("nan"), 1.0, 4096, 1e6))

# ---- arm 4: random_relative_prime (fields: n, seed) ----
R = (97, 0xDEADBEEF)
put("a4_base", arm4(*R))
for fld in range(2):
    for d in (-1, 1):
        v = list(R)
        v[fld] += d
        put(f"a4_w_f{fld}_{'p' if d > 0 else 'm'}1", arm4(*v))
put("a4_n0", arm4(0, 5))
put("a4_n1", arm4(1, 5))
put("a4_n2", arm4(2, 5))
put("a4_nmax", arm4(0xFFFFFFFF, 5))
put("a4_pow2", arm4(4096, 5))
put("a4_seed0", arm4(97, 0))

# ---- arm 5: registry plumbing ----
put("a5_registry", bytes([5]))

for name, data in seeds.items():
    (OUT / name).write_bytes(data)

witness = sum(1 for n in seeds if "_w_" in n)
print(f"wrote {len(seeds)} seeds ({witness} single-field witness variants) to {OUT}")
