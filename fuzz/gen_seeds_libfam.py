#!/usr/bin/env python3
"""Seed generator for the libfam_diff differential fuzz target.

Op encodings mirror fuzz/core/src/libfam_diff.rs (the driver header is the
protocol of record). Seeds deliberately cover:
  - hll: both live bwidths (5, 10), add bursts + estimate ops, extreme
    hashes (0, 1, MSB-only, all-ones) — the rho() corner set.
  - binaryheap: add/add_unordered/build mixes, comparator TIES (duplicate
    values), remove_node positions, replace_first, reset-reuse.
  - pairingheap: ties, interior removes, singular checks, reset-reuse.
  - bloomfilter: single-byte-delta WITNESS PAIRS (each position, both
    directions), probes of previously added elements, k spread via
    total_elems, work_mem legs {0, 1024, 2048}.
  - integerset: 240+ consecutive runs (simple8b mode-0/1 full codewords),
    every 2^k gap (all selector bands), u64::MAX region (wrap-to-error),
    out-of-order absolute adds (error plane), add-during-iterate (error
    plane), interleaved iteration.
"""
import os
import struct

OUT = os.path.join(os.path.dirname(__file__), "corpus", "libfam_diff")
os.makedirs(OUT, exist_ok=True)

def w(name, sel, body):
    with open(os.path.join(OUT, name), "wb") as f:
        f.write(bytes([sel]) + bytes(body))

def u16(v): return struct.pack("<H", v)
def u32(v): return struct.pack("<I", v)
def u64(v): return struct.pack("<Q", v & (2**64 - 1))

# ---- arm 0: hyperloglog (sel byte, then width byte: &1 -> bwidth5) ----
for wb, tag in [(0, "b10"), (1, "b5")]:
    body = bytearray([wb])
    for h in [0, 1, 0x80000000, 0xFFFFFFFF, 0x7FFFFFFF, 0x00010000, 0xDEADBEEF]:
        body += bytes([0]) + u32(h)          # add
    body += bytes([6])                        # estimate
    for i in range(64):
        body += bytes([1]) + u32((i * 0x9E3779B9) & 0xFFFFFFFF)
    body += bytes([7])                        # estimate
    w(f"hll_{tag}_corners", 0, body)
    # dense: many adds hitting every register index band
    body = bytearray([wb])
    for i in range(256):
        body += bytes([2]) + u32((i << 22) | (i * 2654435761 & 0x3FFFFF))
    body += bytes([6])
    w(f"hll_{tag}_dense", 0, body)

# ---- arm 1: binaryheap (cap byte, then ops mod 9) ----
def bh_add(v): return bytes([0]) + u64(v)
def bh_addu(v): return bytes([2]) + u64(v)
BH_BUILD = bytes([3]); BH_POP = bytes([4]); BH_REPL = lambda v: bytes([6]) + u64(v)
BH_RMN = lambda n: bytes([5, n]); BH_CMP = bytes([7]); BH_RESET = bytes([8])

body = bytearray([31])                        # cap 32
for v in [5, 3, 9, 1, 7, 7, 7, 0, 2**63 - 1, 2**63, 100]:  # ties + extremes
    body += bh_addu(v)
body += BH_BUILD + BH_CMP + BH_POP + BH_POP + BH_CMP
for v in [42, 42, 41]:
    body += bh_add(v)
body += BH_RMN(3) + BH_REPL(6) + BH_CMP + BH_RESET + bh_add(1) + BH_CMP
w("bh_mixed", 1, body)

body = bytearray([7])                         # cap 8: exercise the full fence
for v in range(12):                           # driver skips adds past cap
    body += bh_add(v * 3 % 7)                 # ties
body += BH_CMP + BH_POP + BH_POP + BH_POP + BH_CMP
w("bh_full_ties", 1, body)

# ---- arm 2: pairingheap (ops mod 8) ----
def ph_add(v): return bytes([0]) + u64(v)
PH_POP = bytes([3]); PH_RM = lambda k: bytes([4, k]); PH_CHK = bytes([5])
PH_GET = lambda k: bytes([6, k]); PH_RESET = bytes([7])

body = bytearray()
for v in [3, 1, 4, 1, 5, 9, 2, 6, 5, 5]:      # ties
    body += ph_add(v)
body += PH_CHK + PH_RM(4) + PH_RM(0) + PH_GET(2) + PH_POP + PH_CHK
body += PH_RESET + ph_add(7) + PH_CHK + PH_POP + PH_CHK
w("ph_mixed_ties", 2, body)

body = bytearray()
for i in range(40):
    body += ph_add((i * 7919) % 13)           # heavy ties
for k in [0, 5, 9, 3, 1]:
    body += PH_RM(k)
body += PH_CHK
w("ph_interior_removes", 2, body)

# ---- arm 3: bloomfilter ([u24 total-1][wm byte][u64 seed], ops mod 8) ----
def bloom_hdr(total, wmidx, seed):
    return struct.pack("<I", total - 1)[:3] + bytes([wmidx]) + u64(seed)
def bl_add(elem): return bytes([0, len(elem)]) + elem
def bl_lacks_fresh(elem): return bytes([5, len(elem)]) + elem
def bl_lacks_logged(k): return bytes([4, k])
BL_PROP = bytes([6]); BL_EQ = bytes([7])

for total, wmidx, tag in [(1000, 1, "k10"), (1 << 22, 1, "k2"), (16_000_000, 2, "k1"), (300_000, 0, "wm0")]:
    body = bytearray(bloom_hdr(total, wmidx, 0x1234_5678_9ABC_DEF0))
    base = b"witness-elem"
    body += bl_add(base)
    for pos in range(len(base)):
        for delta in (1, 0x80):
            m = bytearray(base); m[pos] ^= delta
            body += bl_lacks_fresh(bytes(m))
    body += bl_lacks_logged(0) + BL_PROP + BL_EQ
    body += bl_add(b"") + bl_lacks_fresh(b"")  # len-0 element
    w(f"bloom_{tag}_witness", 3, body)

# ---- arm 4: integerset (ops mod 8) ----
def is_run(n): return bytes([0, n - 1])
def is_gap(g): return bytes([1]) + u16(g - 1)
def is_pow(k): return bytes([2, k])
def is_abs(x): return bytes([3]) + u64(x)
def is_probe_rel(m): return bytes([4, m])      # 0 last,1 last+1,2 last-1
def is_probe_abs(x): return bytes([4, 3]) + u64(x)
IS_NUM = bytes([5]); IS_ITER = bytes([6]); IS_NEXT = lambda k: bytes([7, k])

# full mode-0 codeword: 241+ consecutive values, then flush pressure
body = bytearray()
body += is_run(255) + is_run(255) + is_run(255)  # >482 forces a flush
body += IS_NUM + is_probe_rel(0) + is_probe_rel(1) + is_probe_rel(2)
# absolute probes INTO packed mode-0 codewords (simple8b_contains bits==0 arm)
body += is_probe_abs(100) + is_probe_abs(101) + is_probe_abs(241) + is_probe_abs(300)
body += IS_ITER + IS_NEXT(255) + IS_NEXT(255)
w("intset_mode0_runs", 4, body)

# every simple8b selector band: 2^k gaps k=0..63
body = bytearray()
for k in range(64):
    body += is_pow(k)
body += IS_NUM + IS_ITER + IS_NEXT(70)
for k in [0, 1, 5, 20, 59, 63]:
    body += is_probe_rel(2) + is_probe_rel(0)
w("intset_pow2_bands", 4, body)

# u64::MAX region + wrap error + out-of-order error + during-iterate error
body = bytearray()
body += is_abs(2**64 - 3) + is_run(2)          # reaches u64::MAX
body += is_pow(1)                              # wraps -> out-of-order error
body += is_abs(5)                              # error (backwards)
body += is_probe_abs(2**64 - 1) + is_probe_abs(2**64 - 2) + is_probe_abs(0)
body += IS_ITER + IS_NEXT(10)
body += is_abs(2**64 - 1)                      # add during (finished) iterate
w("intset_max_region", 4, body)

# mixed gaps stressing leaf splits and update_upper
body = bytearray()
for i in range(30):
    body += is_run(240) + is_gap(1 + i * 37)
body += IS_NUM + IS_ITER + IS_NEXT(255)
w("intset_leaf_splits", 4, body)

# empty-set probes + iterate-on-empty
body = bytearray()
body += is_probe_abs(0) + is_probe_abs(2**63) + IS_NUM + IS_ITER + IS_NEXT(3)
body += is_abs(0) + is_probe_abs(0)            # 0 as the very first member
w("intset_empty_and_zero", 4, body)

# widths 4 and 6 (alpha-table 16/64-register arms; valid generic
# instantiations on both sides)
for wb, tag in [(2, "b4"), (3, "b6")]:
    body = bytearray([wb])
    for i in range(48):
        body += bytes([0]) + u32((i * 0x9E3779B9) & 0xFFFFFFFF)
    body += bytes([6])
    w(f"hll_{tag}_dense", 0, body)

# 3-level tree: ~8450 adds of gap 2^31 -> mode-15 items (2 values each)
# -> >4096 leaf items -> 65+ leaves -> internal-root split (update_upper
# recursion + internal downlink_key arm), then below-min + spot probes.
def is_burst(n, k): return bytes([8, n - 1, k])
body = bytearray()
body += is_abs(1 << 20)                       # min member well above 0
for _ in range(41):
    body += is_burst(256, 31)
body += IS_NUM + is_probe_abs(5) + is_probe_abs(0) + is_probe_rel(0) + is_probe_rel(2)
body += IS_ITER + IS_NEXT(50)
w("intset_three_levels", 4, body)

# hll estimate branch seeds (small-range-with-zeros is everywhere above;
# these force the other three estimate() arms):
#   b5 saturated rho=28 -> large-range correction
body = bytearray([1])
for idx in range(32):
    body += bytes([0]) + u32(idx << 27)        # low 27 bits zero -> rho 28
body += bytes([6])
w("hll_b5_large_range", 0, body)
#   b5 all registers rho=1 -> small-range branch with zero_count == 0
body = bytearray([1])
for idx in range(32):
    body += bytes([0]) + u32((idx << 27) | (1 << 26))  # x<<5 MSB set -> rho 1
body += bytes([6])
w("hll_b5_smallrange_nozeros", 0, body)
#   b5 all registers rho=10 -> mid branch (no correction)
body = bytearray([1])
for idx in range(32):
    body += bytes([0]) + u32((idx << 27) | (1 << 17))  # 9 leading zeros after <<5
body += bytes([6])
w("hll_b5_mid_branch", 0, body)
#   b10 saturated -> large-range at the 1024-register width
body = bytearray([0])
for idx in range(1024):
    body += bytes([0]) + u32(idx << 22)        # low 22 bits zero -> rho 23
body += bytes([6])
w("hll_b10_large_range", 0, body)

# selector coverage: a couple of tiny/degenerate inputs per arm
for sel in range(5):
    w(f"tiny_arm{sel}", sel, b"")
    w(f"short_arm{sel}", sel, bytes(range(16)))

print(f"seeds written to {OUT}: {len(os.listdir(OUT))} files")
