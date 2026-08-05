#!/usr/bin/env python3
"""Seed generator for the oraclefam_diff corpus (gen_seeds.sh pattern).

Layout (core/src/oraclefam_diff.rs): [sel][enc_sel][payload]; sel % 12:
  0 case      [which][text]
  1 lpad      [len4][mode][l2][s2][s1]
  2 rpad      [len4][mode][l2][s2][s1]
  3 trim      [flags][setlen][set][string]
  4 byteatrim [flags][setlen][set][string]
  5 translate [fl][from][tl][to][string]
  6 ascii     [text]
  7 chr       [mode][arg4]
  8 repeat    [mode][count4][text]
  9 text_left [n4][text]
 10 text_right[n4][text]
 11 text_reverse [text]

Hand seeds per the lane charter: multi-char pads into width 7, trim sets
with repeated/multibyte chars, translate delete arm, UTF8 2/3/4-byte chars
at every boundary position, embedded NUL, empty fields, chr boundary bands,
and single-field-difference WITNESS PAIRS for dobyteatrim (set / string
differing by one byte at each end — a seeding OBLIGATION, lane-0B lesson).
"""
import os
import struct

OUT = os.path.join(os.path.dirname(__file__), "corpus", "oraclefam_diff")
os.makedirs(OUT, exist_ok=True)

n = 0


def seed(name: str, data: bytes) -> None:
    global n
    with open(os.path.join(OUT, f"oc-{name}"), "wb") as f:
        f.write(data)
    n += 1


def i32(v: int) -> bytes:
    return struct.pack("<i", v)


E2 = "é".encode()          # 2-byte UTF8
E3 = "€".encode()          # 3-byte UTF8 (euro)
E4 = "\U00010348".encode()      # 4-byte UTF8

for enc in range(3):
    # --- case (arm 0): kernels + NUL truncation plane -------------------
    for which in range(4):
        seed(f"case-{which}-e{enc}", bytes([0, enc, which]) + b"Hello, World 42!")
        seed(f"case-nul-{which}-e{enc}",
             bytes([0, enc, which]) + b"miXed\x00DROPPED tail")
    seed(f"case-mb-e{enc}", bytes([0, enc, 2]) + E2 + b"lan " + E4 + b"word")
    # invalid-collid arm (which | 0x80): 42P22 verdict, entry + fc wrapper
    for which in range(4):
        seed(f"case-invcoll-{which}-e{enc}",
             bytes([0, enc, which | 0x80]) + b"any text")

    # --- lpad/rpad (arms 1/2) -------------------------------------------
    for sel, tag in ((1, "lpad"), (2, "rpad")):
        # multi-char pad "xyz" into width 7
        seed(f"{tag}-xyz7-e{enc}",
             bytes([sel, enc]) + i32(7) + bytes([0, 3]) + b"xyz" + b"hi")
        # truncation (len < s1 chars)
        seed(f"{tag}-trunc-e{enc}",
             bytes([sel, enc]) + i32(2) + bytes([0, 3]) + b"xyz" + b"hello")
        # negative len band
        seed(f"{tag}-neg-e{enc}",
             bytes([sel, enc]) + i32(5) + bytes([1, 1]) + b"x" + b"hi")
        # >MaxAllocSize band (54000 plane)
        seed(f"{tag}-huge-e{enc}",
             bytes([sel, enc]) + i32(0x12345) + bytes([2, 1]) + b"x" + b"hi")
        # empty pad string (len collapses to s1len)
        seed(f"{tag}-emptypad-e{enc}",
             bytes([sel, enc]) + i32(9) + bytes([0, 0]) + b"hi")
        # multibyte pad chars wrapping mid-sequence
        seed(f"{tag}-mbpad-e{enc}",
             bytes([sel, enc]) + i32(9) + bytes([0, len(E2 + E3)]) + E2 + E3 + b"ab")

    # --- text trims (arm 3) ----------------------------------------------
    for flags in range(3):
        seed(f"trim-{flags}-e{enc}",
             bytes([3, enc, flags, 4]) + b"xy x" + b"xxhello worldyy  ")
        seed(f"trim-mb-{flags}-e{enc}",
             bytes([3, enc, flags, len(E2 + b"a")]) + E2 + b"a" + E2 + E2 + b"abc" + E2)
        seed(f"trim-all-{flags}-e{enc}",
             bytes([3, enc, flags, 2]) + b"ab" + b"abbaabab")
    seed(f"trim-emptyset-e{enc}", bytes([3, enc, 0, 0]) + b"  spaced  ")
    seed(f"trim-nul-e{enc}", bytes([3, enc, 0, 1]) + b"\x00" + b"\x00mid\x00")

    # --- translate (arm 5) ------------------------------------------------
    # delete arm: from longer than to
    seed(f"translate-del-e{enc}",
         bytes([5, enc, 3]) + b"abc" + bytes([1]) + b"X" + b"cabbage patch")
    # empty to: pure deletion
    seed(f"translate-delall-e{enc}",
         bytes([5, enc, 2]) + b"ab" + bytes([0]) + b"banana")
    # to longer than from (extra ignored)
    seed(f"translate-long-to-e{enc}",
         bytes([5, enc, 1]) + b"a" + bytes([3]) + b"xyz" + b"banana")
    # multibyte from/to of unequal char length
    seed(f"translate-mb-e{enc}",
         bytes([5, enc, len(E2 + b"e")]) + E2 + b"e" + bytes([len(E3)]) + E3 +
         b"cr" + E2 + b"me brulee")
    # empty string fast path
    seed(f"translate-empty-e{enc}", bytes([5, enc, 1]) + b"a" + bytes([1]) + b"b")

    # --- ascii (arm 6) -----------------------------------------------------
    for name, t in (("plain", b"A"), ("empty", b""), ("mb2", E2), ("mb3", E3),
                    ("mb4", E4), ("hi", b"\x80rest")):
        seed(f"ascii-{name}-e{enc}", bytes([6, enc]) + t)

    # --- chr (arm 7): boundary bands --------------------------------------
    for arg in (0, 1, 127, 128, 255, 256, 0x7FF, 0x800, 0xD7FF, 0xD800,
                0xDFFF, 0xE000, 0xFFFF, 0x10000, 0x10FFFF, 0x110000, -1):
        seed(f"chr-{arg & 0xffffffff:x}-e{enc}",
             bytes([7, enc, 0]) + i32(arg))

    # --- repeat (arm 8) ----------------------------------------------------
    seed(f"repeat-5ab-e{enc}", bytes([8, enc, 0]) + i32(5) + b"ab")
    seed(f"repeat-zero-e{enc}", bytes([8, enc, 0]) + i32(0) + b"ab")
    seed(f"repeat-neg-e{enc}", bytes([8, enc, 2]) + i32(7) + b"ab")
    seed(f"repeat-huge-e{enc}", bytes([8, enc, 1]) + i32(0x7FFFFFFF) + b"ab")
    seed(f"repeat-empty-e{enc}", bytes([8, enc, 0]) + i32(5))
    seed(f"repeat-mb-e{enc}", bytes([8, enc, 0]) + i32(3) + E4)

    # --- text_left/right (arms 9/10): INT32_MIN wrap arm ------------------
    for sel, tag in ((9, "left"), (10, "right")):
        for name, nval in (("intmin", -0x80000000), ("intmax", 0x7FFFFFFF),
                           ("neg1", -1), ("zero", 0), ("three", 3),
                           ("past", 100)):
            seed(f"t{tag}-{name}-e{enc}",
                 bytes([sel, enc]) + i32(nval) + b"ab" + E2 + b"de")
        seed(f"t{tag}-mb-e{enc}",
             bytes([sel, enc]) + i32(2) + E2 + E3 + E4 + b"x")

    # --- text_reverse (arm 11) ---------------------------------------------
    seed(f"reverse-e{enc}", bytes([11, enc]) + b"ab" + E2 + E3 + E4 + b"yz")
    seed(f"reverse-nul-e{enc}", bytes([11, enc]) + b"a\x00b")
    seed(f"reverse-bad-e{enc}", bytes([11, enc]) + b"a\xc3")

# --- byteatrim (arm 4): witness pairs (single-field difference) -----------
BASE_SET = b"\x10\x20"
BASE_STR = b"\x10\x20abc\x20\x10"
for flags in range(3):
    seed(f"bytetrim-base-{flags}", bytes([4, 0, flags, len(BASE_SET)]) + BASE_SET + BASE_STR)
    # set differing by one byte (each byte, small delta, both orders)
    for i in range(len(BASE_SET)):
        for d in (1, 0xFF):
            s = bytearray(BASE_SET)
            s[i] = (s[i] + d) & 0xFF
            seed(f"bytetrim-set{i}d{d}-{flags}",
                 bytes([4, 0, flags, len(s)]) + bytes(s) + BASE_STR)
    # string differing by one byte at each end
    for pos, name in ((0, "head"), (len(BASE_STR) - 1, "tail")):
        for d in (1, 0xFF):
            s = bytearray(BASE_STR)
            s[pos] = (s[pos] + d) & 0xFF
            seed(f"bytetrim-str{name}d{d}-{flags}",
                 bytes([4, 0, flags, len(BASE_SET)]) + BASE_SET + bytes(s))
seed("bytetrim-emptyset", bytes([4, 0, 0, 0]) + BASE_STR)
seed("bytetrim-emptystr", bytes([4, 0, 0, 2]) + BASE_SET)
seed("bytetrim-ff", bytes([4, 0, 0, 1]) + b"\xff" + b"\xff\xffmid\xff")

# --- EUC_JP arm (ascii/chr only; enc_sel % 4 == 3): multibyte-non-UTF8
# 54000 reject arms + accept arm -----------------------------------------
for name, t in (("plain", b"A"), ("hi", b"\x80rest"), ("ff", b"\xff"),
                ("empty", b"")):
    seed(f"ascii-eucjp-{name}", bytes([6, 3]) + t)
for arg in (1, 127, 128, 255, 0x7FFFFFFF):
    seed(f"chr-eucjp-{arg:x}", bytes([7, 3, 0]) + i32(arg))

# --- UTF8 boundary-position seeds: 2/3/4-byte char at head/mid/tail -------
for name, ch in (("b2", E2), ("b3", E3), ("b4", E4)):
    for arm in (0, 3, 5, 9, 10, 11):
        prefix = {0: bytes([2]), 3: bytes([0, 1]) + b" ",
                  5: bytes([1]) + b"x" + bytes([1]) + b"y",
                  9: i32(2), 10: i32(2), 11: b""}[arm]
        seed(f"utf8-{name}-arm{arm}",
             bytes([arm, 1]) + prefix + ch + b"mid" + ch + b"end" + ch)

print(f"wrote {n} seeds to {OUT}")
