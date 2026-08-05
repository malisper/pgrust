#!/usr/bin/env python3
"""Deterministic witness seeds for the varlena campaign targets (p1-lanes).

Each seed targets a specific in-scope line/branch that random mutation
reaches rarely or never; the file names are content sha1s so a re-run is
idempotent. Input layouts are transcribed from the driver decoders — keep
them in sync with fuzz/core/src/vl*_diff.rs when a layout changes.

  vltext_diff   [sel][flag][split u16 LE][t1][t2]        (sel % 36)
  vlbytea_diff  [sel][arm-specific]                      (sel % 37)
  vlmisc_diff   [sel][arm-specific]                      (sel % 16)
"""
import hashlib
import os
import struct

HERE = os.path.dirname(os.path.abspath(__file__))


def write(target, blob):
    d = os.path.join(HERE, "corpus", target)
    os.makedirs(d, exist_ok=True)
    p = os.path.join(d, hashlib.sha1(blob).hexdigest())
    with open(p, "wb") as f:
        f.write(blob)
    return p


def vltext(arm, flag, t1, t2):
    """sel picks the arm (sel % 36); split_two takes a u16 LE index."""
    rest = t1 + t2
    return bytes([arm, flag]) + struct.pack("<H", len(t1) % (len(rest) + 1)) + rest


n = 0

# --- 1a. textpos big haystack: the >=4096 B-M-H skip-table stride arm
# (varlena/src/lib.rs:607-610), unreachable under the 2 KiB shared cap.
NEEDLE = b"zqx"
BIG = (b"abcdefgh" * 640)[:5120]          # 5 KiB, needle absent
n += bool(write("vltext_diff", vltext(7, 0, BIG, NEEDLE)))
n += bool(write("vltext_diff", vltext(7, 0, BIG[:-3] + NEEDLE, NEEDLE)))  # near end
n += bool(write("vltext_diff", vltext(7, 0, NEEDLE + BIG[3:], NEEDLE)))   # at start

# --- 1c. text_larger / text_smaller: both which-argument arms
# (builtins.rs:183 / :195), each in both argument orders.
for arm in (15, 16):
    n += bool(write("vltext_diff", vltext(arm, 0, b"bbb", b"aaa")))
    n += bool(write("vltext_diff", vltext(arm, 0, b"aaa", b"bbb")))
    n += bool(write("vltext_diff", vltext(arm, 0, b"aaa", b"aaa")))

# --- 2a. hex_encode_into (arm 36 = sel % 37).
n += bool(write("vlbytea_diff", bytes([36]) + b""))
n += bool(write("vlbytea_diff", bytes([36]) + bytes(range(256))))
n += bool(write("vlbytea_diff", bytes([36]) + b"\x00\xff\x0f\xf0"))

# --- 2b. byteaSetBit clear arm (bytea.rs:462).
# [mb][n u64 LE][bm][data]; mb % 8 == 2 -> n = 0, bm % 6 == 0 -> new_bit = 0.
for data in (b"\xff\x00\xaa", b"\xfe\x01", b"\x01"):
    n += bool(write("vlbytea_diff", bytes([18, 2]) + struct.pack("<Q", 0) + b"\x00" + data))
# ... and the set arm (new_bit = 1) over the same data for the witness pair.
for data in (b"\xfe\x00", b"\xff\x00"):
    n += bool(write("vlbytea_diff", bytes([18, 2]) + struct.pack("<Q", 0) + b"\x01" + data))

# --- 3a. textToQualifiedNameList (arm 15 = sel % 16): the wrapper's 42602
# plane, including the trailing-separator reject fixed in this lane.
for s in (b"a.b", b"a.", b".", b'"Q".b', b"", b"   ", b"a.b.c", b'""', b"a..b"):
    n += bool(write("vlmisc_diff", bytes([15]) + s))

# --- 3b. levenshtein_less_equal stop_column clamp (levenshtein.rs:90):
# needs max_d < max_theo_d with a large slack, so stop_column > m0.
# hdr = [ins|del<<4][sub|trusted<<4][split u16 LE][max_d idx]
def lev_le(ins_i, del_i, sub_i, trusted, maxd_i, src, tgt):
    rest = src + tgt
    hdr = bytes([ins_i | (del_i << 4), sub_i | (trusted << 4)])
    hdr += struct.pack("<H", len(src) % (len(rest) + 1))
    hdr += bytes([maxd_i])
    return bytes([12]) + hdr + rest


# COSTS[1]=1 (ins/del), COSTS[7]=20000 (sub -> huge max_theo_d),
# MAXDS[6]=255 (big slack over ins_c+del_c=2 -> stop_column ~128 > m0).
n += bool(write("vlmisc_diff", lev_le(1, 1, 7, 0, 6, b"abcdef", b"xyz")))
n += bool(write("vlmisc_diff", lev_le(1, 1, 7, 0, 6, b"xyz", b"abcdef")))
n += bool(write("vlmisc_diff", lev_le(1, 1, 7, 0, 5, b"abc", b"abd")))   # MAXDS[5]=100
n += bool(write("vlmisc_diff", lev_le(1, 1, 7, 1, 6, b"abcdef", b"xyz")))  # trusted
n += bool(write("vlmisc_diff", lev_le(1, 1, 6, 0, 6, b"kitten", b"sitting")))

# --- 3c. unistr invalid surrogate pair (lib.rs:1140 and siblings): a lone
# high surrogate followed by a literal backslash escape / end / a non-low
# escape, in each accepted escape form.
for s in (
    rb"\D800\\",          # high surrogate then literal backslash  -> :1140
    rb"\uD800\\",
    rb"\+00D800\\",
    rb"\D800",            # high surrogate at end of string
    rb"\D800\0041",       # high surrogate then a non-low-surrogate escape
    rb"\D800\DC00",       # a VALID pair (the witness pair's other side)
    rb"\DC00",            # lone low surrogate
    rb"\UD800DC00",
):
    n += bool(write("vlmisc_diff", bytes([6]) + s))

print(f"{n} seeds written")
