#!/usr/bin/env python3
"""gen_seeds_tsmrows.py — directed seed corpus for tsm_system_rows_diff.

Emits, per arm: boundary seeds plus SINGLE-FIELD-DIFFERENCE WITNESS PAIRS
(each derived field varied by a small delta in both directions off a base
seed, so every field's individual contribution to the verdict is witnessed —
the Lane-0B seeding obligation). Deterministic; output committed.
"""
import struct
from pathlib import Path

OUT = Path(__file__).resolve().parent / "corpus" / "tsm_system_rows_diff"
OUT.mkdir(parents=True, exist_ok=True)

seeds = {}


def put(name: str, data: bytes):
    assert name not in seeds, name
    seeds[name] = data


def arm0(ntuples: int, seed: int, nblocks: int) -> bytes:
    return bytes([0]) + struct.pack("<qIH", ntuples, seed, nblocks)


def arm12(sel: int, seed: int, ntuples: int, nblocks0: int, draws: bytes,
          tail: bytes = b"") -> bytes:
    return bytes([sel]) + struct.pack("<IHH", seed, ntuples, nblocks0) + draws + tail


def arm3(flags: int, limit: int, pages: int, tuples: float) -> bytes:
    return bytes([3, flags]) + struct.pack("<qId", limit, pages, tuples)


def arm4(n: int, seed: int) -> bytes:
    return bytes([4]) + struct.pack("<IQ", n, seed)


# ---- arm 0: begin (fields: ntuples, seed, nblocks) ----
B = (5, 1000, 64)
put("a0_base", arm0(*B))
for fld, deltas in ((0, (-1, 1)), (1, (-1, 1)), (2, (-1, 1))):
    for d in deltas:
        v = list(B)
        v[fld] += d
        put(f"a0_w_f{fld}_{'p' if d > 0 else 'm'}{abs(d)}", arm0(*v))
put("a0_neg1", arm0(-1, 1000, 64))
put("a0_mini64", arm0(-(2**63), 7, 3))
put("a0_zero_tuples", arm0(0, 1000, 64))
put("a0_empty_rel", arm0(10, 42, 0))

# ---- arm 1: single-scan walk (fields: seed, ntuples, nblocks0, draw bytes) ----
D = bytes([0x55] * 48)
W = (42, 100, 13)
put("a1_base", arm12(1, *W, D))
for fld in range(3):
    for d in (-1, 1):
        v = list(W)
        v[fld] += d
        put(f"a1_w_f{fld}_{'p' if d > 0 else 'm'}{abs(d)}", arm12(1, *v, D))
# draw-byte witness pair: first draw byte differs by one either way
for tag, b0 in (("m1", 0x54), ("p1", 0x56)):
    put(f"a1_w_draw_{tag}", arm12(1, *W, bytes([b0]) + D[1:]))
put("a1_empty_rel", arm12(1, 42, 100, 0, D))
put("a1_one_block", arm12(1, 42, 100, 1, D))
put("a1_exhaust_blocks", arm12(1, 3, 65535, 4, D))   # ntuples-limited
put("a1_exhaust_tuples", arm12(1, 65535, 2, 4, D))   # block-limited
put("a1_zero_maxoffset", arm12(1, 9, 50, 8, bytes([8, 0] * 24)))

# ---- arm 2: rescan walk (adds seed2, ntuples2) ----
T = struct.pack("<IH", 999, 50)
W2 = (7, 50, 11)
put("a2_base", arm12(2, *W2, bytes([0x20] * 32), T + bytes([0x10] * 32)))
for fld in range(3):
    for d in (-1, 1):
        v = list(W2)
        v[fld] += d
        put(f"a2_w_f{fld}_{'p' if d > 0 else 'm'}{abs(d)}",
            arm12(2, *v, bytes([0x20] * 32), T + bytes([0x10] * 32)))
for tag, (s2, n2) in (("seed2_p1", (1000, 50)), ("seed2_m1", (998, 50)),
                      ("nt2_p1", (999, 51)), ("nt2_m1", (999, 49))):
    put(f"a2_w_{tag}",
        arm12(2, *W2, bytes([0x20] * 32),
              struct.pack("<IH", s2, n2) + bytes([0x10] * 32)))
# shrink-heavy rescan (delta bytes biased low -> nblocks below first scan's)
put("a2_shrink", arm12(2, 7, 60, 40, bytes([0x08] * 32), T + bytes([0x00] * 32)))

# ---- arm 3: getsamplesize (fields: flags, limit, pages, tuples) ----
G = (1, 500, 64, 1000.0)
put("a3_base", arm3(*G))
for fld, deltas in ((1, (-1, 1)), (2, (-1, 1)), (3, (-1.0, 1.0))):
    for d in deltas:
        v = list(G)
        v[fld] += d
        put(f"a3_w_f{fld}_{'p' if d > 0 else 'm'}1", arm3(*v))
put("a3_nonconst", arm3(0, 500, 64, 1000.0))
put("a3_nullconst", arm3(3, 500, 64, 1000.0))
put("a3_neg_limit", arm3(1, -1, 64, 1000.0))
put("a3_zero_pages", arm3(1, 500, 0, 1000.0))
put("a3_zero_tuples", arm3(1, 500, 64, 0.0))
put("a3_neg_tuples", arm3(1, 500, 64, -5.0))
put("a3_dense", arm3(1, 500, 4, 1e6))
put("a3_halfround", arm3(1, 500, 7, 2.5))   # rint ties-to-even path
# default-1000 path with a LARGE relation (unmasked by the tuples clamp —
# the 1000-vs-1001 injection survived the small-relation seeds)
put("a3_nonconst_bigrel", arm3(0, 500, 4096, 1e6))
put("a3_nullconst_bigrel", arm3(3, 500, 4096, 1e6))
put("a3_neg_limit_bigrel", arm3(1, -7, 4096, 1e6))

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
