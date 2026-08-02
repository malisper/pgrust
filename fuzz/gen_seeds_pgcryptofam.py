#!/usr/bin/env python3
"""Seed generator for the pgcryptofam_diff corpus (gen_seeds.sh pattern).

Layout (core/src/pgcryptofam_diff.rs is the protocol of record):
  [sel][mode][payload], sel % 6:

    0 crypt        [pwlen u8][pw][setting-suffix...]
                   mode>>1 % 12 picks the SETTING PREFIX from
                   ["", "$1$", "$5$", "$6$", "$2a$", "$2x$", "$2$",
                    "$2b$", "$2y$", "_", "$5$rounds=", "$7$"]
    1 gen_salt     mode&1 = 0 -> algo from (mode>>4) % 10, else
                                 [algolen u8][algo]
                   mode&2 = 0 -> rounds from ROUNDS_CORNERS[(mode>>2) % 16],
                                 else i32 LE
                   remaining payload = injected C entropy (padded to 32)
    2 armor        mode&1 = 0 -> no headers, payload = data
                   mode&1 = 1 -> [n u8 %4]{[klen u8][vlen u8][k][v]}*n [data]
    3 dearmor      mode&1 = 0 -> payload IS the text
                   mode&1 = 1 -> envelope builder:
                                 [n u8 %3]{[klen][vlen][k][v]}*n
                                 [bodylen u8][body][mutkind u8][mut args]
    4 digest       mode&1 = 0 -> hash name from HASH_NAMES[(mode>>2) % 16],
                   else [namelen u8][name]; rest = data
    5 hmac         same name selector; then [keylen u8][key][data]

Seeded per the lane charter (exec floors never witness boundaries):
  - zero-length and one-byte settings
  - every px_crypt_list prefix at length prefix-1 / prefix / prefix+1
  - xdes settings at count 0, 1, even, odd, 0xFFFFFF, 0xFFFFFF+1
  - sha settings with rounds empty / 0 / 1000, and `$` at every position
  - salt bytes on and off itoa64: 0x00 0x2E 0x2F 0x7A 0x7F 0x80 0xFF
  - armor bodies at base64 length %4 == 0/1/2/3, all-padding, `=` mid-stream,
    a missing CRC line, a CRC line short by one char
  - armor header keys/values containing "\n", ": " and non-ASCII (D8 shapes)
  - every px_find_digest name + case variants + misses, hmac keys straddling
    both HMAC block sizes (64 and 128) and the key-longer-than-B branch
  - the ground-truthed divergence inputs from
    docs/verification/evidence/p1-pgcrypto/GROUND-TRUTH-18.3.md (D1..D19)
  - SINGLE-FIELD-DIFFERENCE WITNESS PAIRS for every packing/shift/OR-merge
    helper whose output feeds a comparison (to64, bf_encode, the xdes count
    encode): pairs differing in EXACTLY ONE field, each field, small deltas,
    BOTH ORDERS. Line coverage and exec volume cannot detect their absence —
    a sibling lane's byte-shift mutants survived a 27.3M-exec corpus at 100%
    line coverage because no corpus pair differed in exactly one byte.
"""
import os
import struct

OUT = os.path.join(os.path.dirname(__file__), "corpus", "pgcryptofam_diff")
os.makedirs(OUT, exist_ok=True)

n = 0


def seed(name: str, data: bytes) -> None:
    global n
    with open(os.path.join(OUT, f"pc-{name}"), "wb") as f:
        f.write(data)
    n += 1


def i32(v: int) -> bytes:
    return struct.pack("<i", v)


# Driver-side tables, mirrored (the driver header is the protocol of record).
PREFIXES = ["", "$1$", "$5$", "$6$", "$2a$", "$2x$", "$2$",
            "$2b$", "$2y$", "_", "$5$rounds=", "$7$"]
ROUNDS_CORNERS = [0, 1, 2, 3, 4, 5, 25, 31, 32, 725, 1000, 5000,
                  999999999, 0xFFFFFF, 0x1000000, -1]
ALGOS = ["des", "md5", "xdes", "bf", "sha256crypt", "sha512crypt",
         "XDES", "Bf", "", "nosuchalgo"]
ITOA64 = b"./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"


def crypt_seed(name, pw: bytes, prefix_idx: int, suffix: bytes) -> None:
    mode = (prefix_idx << 1) & 0xFF
    seed(name, bytes([0, mode, len(pw)]) + pw + suffix)


def gensalt_seed(name, algo_idx: int, rounds_idx: int, entropy: bytes = b"",
                 one_arg: bool = False) -> None:
    mode = ((algo_idx & 0xF) << 4) | ((rounds_idx & 0xF) << 2)
    if not one_arg:
        mode |= 4  # force the two-argument pg_gen_salt_rounds wrapper
    seed(name, bytes([1, mode]) + entropy)


def gensalt_raw_seed(name, algo: bytes, rounds: int, entropy: bytes = b"") -> None:
    # mode&1 -> free-form algo, mode&2 -> raw i32 rounds, mode&4 -> _rounds fc
    seed(name, bytes([1, 0b111]) + bytes([len(algo)]) + algo + i32(rounds) + entropy)


def armor_seed(name, headers, data: bytes) -> None:
    if not headers:
        seed(name, bytes([2, 0]) + data)
        return
    body = bytes([len(headers)])
    for k, v in headers:
        body += bytes([len(k), len(v)]) + k + v
    seed(name, bytes([2, 1]) + body + data)


def raw_text_seed(name, sel: int, text: bytes) -> None:
    seed(name, bytes([sel, 0]) + text)


def env_seed(name, sel: int, headers, body: bytes, mutkind: int,
             mutargs: bytes = b"\x00\x00") -> None:
    payload = bytes([len(headers)])
    for k, v in headers:
        payload += bytes([len(k), len(v)]) + k + v
    payload += bytes([len(body)]) + body + bytes([mutkind]) + mutargs
    seed(name, bytes([sel, 1]) + payload)


# ===========================================================================
# arm 0 — crypt(): the px_crypt_list dispatch surface
# ===========================================================================

PW = b"foox"

# zero-length and one-byte settings, at the bare (no-prefix) row.
crypt_seed("crypt-setting-empty", PW, 0, b"")
for b in (b"x", b"$", b"_", b".", b"/", b"z", b"\x7f"):
    crypt_seed(f"crypt-setting-1byte-{b.hex()}", PW, 0, b)

# Every px_crypt_list prefix at length prefix-1 / prefix / prefix+1. The
# driver PREPENDS the prefix, so "prefix-1" is built through the bare row.
for i, p in enumerate(PREFIXES):
    pb = p.encode()
    if pb:
        crypt_seed(f"crypt-prefix{i}-minus1", PW, 0, pb[:-1])
    crypt_seed(f"crypt-prefix{i}-exact", PW, i, b"")
    crypt_seed(f"crypt-prefix{i}-plus1", PW, i, b"a")

# D1: "$2$" and "$2$06$..." -> C raises 39000 "crypt(3) returned NULL".
crypt_seed("crypt-d1-dollar2-bare", PW, 6, b"")
crypt_seed("crypt-d1-dollar2-bcryptish", PW, 6, b"04$......................")
# D2: "$2b$" has NO row -> traditional DES with the 2-char salt "$2".
crypt_seed("crypt-d2-dollar2b", PW, 7, b"04$......................")
crypt_seed("crypt-d2-dollar2y", PW, 8, b"04$......................")
# D11: "$2x$" sign-extension bug-compat with a >= 0x80 password byte.
crypt_seed("crypt-d11-2x-highbyte", "éabc".encode(),
           5, b"04$......................")
crypt_seed("crypt-d11-2a-highbyte", "éabc".encode(),
           4, b"04$......................")

# xdes settings at count 0, 1, even, odd, 0xFFFFFF, 0xFFFFFF+1. The count is
# 4 itoa64 chars, little-endian 6-bit groups, at setting[1..5].
def itoa64_4(v: int) -> bytes:
    return bytes(ITOA64[(v >> s) & 0x3F] for s in (0, 6, 12, 18))


# The driver pins the executed xdes count to <= 255; the larger values are
# seeded anyway because the cost probe's own parse runs on every one of them.
for count in (0, 1, 2, 3, 63, 64, 65, 127, 128, 254, 255, 256,
              724, 725, 726, 4094, 4095, 0xFFFFFF):
    crypt_seed(f"crypt-xdes-count{count}", b"password", 9,
               itoa64_4(count) + b"abcd")
# 0xFFFFFF+1 cannot be encoded in 4 chars: the wrap is the seed.
crypt_seed("crypt-xdes-count-wrap", b"password", 9, itoa64_4(0x1000000) + b"abcd")
# short xdes settings (< 9 chars): px_crypt_des errors before do_des
for k in range(0, 9):
    crypt_seed(f"crypt-xdes-short{k}", b"password", 9, (b"J9..abcd")[:k])

# sha settings: rounds empty / 0 / 1000, and '$' at every position.
for r in (b"", b"0", b"1", b"999", b"1000", b"1001", b"5000", b"+5000",
          b" 5000", b"-1", b"-5", b"0005000", b"abc", b"1000abc"):
    crypt_seed(f"crypt-sha5-rounds-{r.decode('latin1').strip() or 'empty'}",
               PW, 10, r + b"$abcdefgh")
    crypt_seed(f"crypt-sha6-rounds-{r.decode('latin1').strip() or 'empty'}",
               PW, 3, b"rounds=" + r + b"$abcdefgh")
BASE_SHA = b"rounds=1000$abcdefgh"
for pos in range(len(BASE_SHA) + 1):
    crypt_seed(f"crypt-sha5-dollar-at{pos}", PW, 2,
               BASE_SHA[:pos] + b"$" + BASE_SHA[pos:])
# D13/D14/D16: bare magic, leading '$', whole-string strstr guards.
crypt_seed("crypt-d13-sha5-bare", PW, 2, b"")
crypt_seed("crypt-d13-sha6-bare", PW, 3, b"")
crypt_seed("crypt-d14-sha5-leading-dollar", PW, 2, b"$abc")
crypt_seed("crypt-d14-sha5-plain", PW, 2, b"abc")
crypt_seed("crypt-d16-sha5-embedded-rounds", PW, 2, b"abc$rounds=1000")
crypt_seed("crypt-d16-sha5-embedded-magic", PW, 2, b"abc$$5$")
crypt_seed("crypt-d7-sha5-badchar", PW, 2, b"ab*cd")
crypt_seed("crypt-d18-sha5-sub0x20", PW, 2, b"ab\ncd")
crypt_seed("crypt-sha5-salt-28", PW, 2, b"a" * 28)

# Salt bytes on and off the itoa64 alphabet, at each of the two DES salt
# positions (SINGLE-FIELD-DIFFERENCE PAIRS: one position varies, the other
# is pinned; both orders).
EDGE = (0x00, 0x2E, 0x2F, 0x39, 0x3A, 0x41, 0x5A, 0x61, 0x7A, 0x7F, 0x80, 0xFF)
for b in EDGE:
    crypt_seed(f"crypt-des-salt0-{b:02x}", PW, 0, bytes([b]) + b".")
    crypt_seed(f"crypt-des-salt1-{b:02x}", PW, 0, b"." + bytes([b]))
# ...and inside the xdes salt (positions 5..8), one position at a time.
for pos in range(4):
    for b in EDGE:
        s = bytearray(b"J9..abcd")
        s[4 + pos] = b
        crypt_seed(f"crypt-xdes-salt{pos}-{b:02x}", b"password", 9, bytes(s))

# md5 salt: length 0..9 (C stops at 8 or the first '$').
for k in range(0, 10):
    crypt_seed(f"crypt-md5-saltlen{k}", PW, 1, b"Szzz0yzzq"[:k])
crypt_seed("crypt-md5-salt-dollar", PW, 1, b"ab$cd")

# bcrypt settings at the cost-parse boundaries. The driver PINS the cost to
# 04, so only "04" executes; every other spelling is a counted cost-probe
# refusal, which is itself a seeded case (the probe's own parse still runs).
for cost in ("00", "03", "04", "05", "06", "07", "31", "32", "3a", "0a"):
    crypt_seed(f"crypt-bf-cost{cost}", PW, 4,
               cost.encode() + b"$......................")
crypt_seed("crypt-bf-short", PW, 4, b"04$")
crypt_seed("crypt-bf-nodollar", PW, 4, b"04X......................")
# bf salt chars on/off the BF64 alphabet, one position at a time (witness
# pairs for bf_decode's per-char alphabet test).
for pos in (0, 10, 21):
    for b in (0x2E, 0x2F, 0x30, 0x39, 0x41, 0x5A, 0x61, 0x7A, 0x7F, 0x80, 0xFF):
        s = bytearray(b"." * 22)
        s[pos] = b
        crypt_seed(f"crypt-bf-salt{pos}-{b:02x}", PW, 4, b"04$" + bytes(s))

# Empty password / long password / password with a NUL-adjacent byte.
for pw, tag in ((b"", "empty"), (b"a", "1b"), (b"a" * 72, "72b"),
                (b"a" * 73, "73b"), (b"\x01\x02\x7f", "ctl")):
    crypt_seed(f"crypt-pw-{tag}", pw, 1, b"Szzz0yzz")

# ===========================================================================
# arm 0 — SINGLE-FIELD-DIFFERENCE WITNESS PAIRS for the count/salt packers
# ===========================================================================
# to64 / the xdes count encode merge four 6-bit fields with distinct shifts.
# A mutant that swaps two shifts, drops an OR, or masks with the wrong width
# survives every corpus in which no two inputs differ in EXACTLY ONE field.
# Below: for each of the four 6-bit fields, the base value and base +- delta,
# in BOTH orders, with all other fields pinned.
BASE_COUNTS = (0, 1, 725, 0x155555, 0xFFFFFF)
for base in BASE_COUNTS:
    for field in range(4):
        shift = 6 * field
        for delta in (1, 2, 4, 8, 16, 32):
            for sign in (+1, -1):
                cur = (base >> shift) & 0x3F
                new = (cur + sign * delta) & 0x3F
                if new == cur:
                    continue
                v = (base & ~(0x3F << shift)) | (new << shift)
                crypt_seed(
                    f"wp-xdes-b{base:06x}-f{field}-d{sign * delta}",
                    b"password", 9, itoa64_4(v & 0xFFFFFF) + b"abcd")
        # the pinned base itself, so the PAIR exists in the corpus
        crypt_seed(f"wp-xdes-b{base:06x}-f{field}-base", b"password", 9,
                   itoa64_4(base) + b"abcd")

# bf_encode/bf_decode pack 3 bytes into 4 chars across three shift lanes.
# One-char deltas at every position of a 22-char bcrypt salt, both directions.
BF64 = b"./ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
for pos in range(22):
    for idx in (0, 1, 2, 31, 32, 62, 63):
        s = bytearray(b"." * 22)
        s[pos] = BF64[idx]
        crypt_seed(f"wp-bf-p{pos:02d}-i{idx:02d}", PW, 4, b"04$" + bytes(s))
# neighbouring-index pairs at one position (delta = +-1 in exactly one field)
for pos in (0, 1, 7, 14, 21):
    for idx in range(0, 64, 7):
        for d in (-1, 1):
            j = (idx + d) % 64
            s = bytearray(b"." * 22)
            s[pos] = BF64[j]
            crypt_seed(f"wp-bf-nb-p{pos:02d}-i{idx:02d}{'m' if d < 0 else 'p'}",
                       PW, 4, b"04$" + bytes(s))

# md5's to64 packs (d[a]<<16)|(d[b]<<8)|d[c] then emits 4 chars. The only
# fuzz-reachable single-field lever is the salt, so vary one salt byte at a
# time across the itoa64 boundary values, both orders.
for pos in range(8):
    for b in (0x2E, 0x2F, 0x30, 0x39, 0x3A, 0x5A, 0x5B, 0x60, 0x61, 0x7A, 0x7B):
        s = bytearray(b"Szzz0yzz")
        s[pos] = b
        crypt_seed(f"wp-md5-p{pos}-{b:02x}", PW, 1, bytes(s))

# ===========================================================================
# arm 1 — gen_salt(): every gen_list row x every rounds corner
# ===========================================================================
for ai in range(len(ALGOS)):
    for ri in range(len(ROUNDS_CORNERS)):
        gensalt_seed(f"gensalt-a{ai}-r{ri}", ai, ri)
    # the ONE-ARGUMENT form (pg_gen_salt), rounds corner 0
    gensalt_seed(f"gensalt-a{ai}-1arg", ai, 0, one_arg=True)

# D3/D4/D5: xdes rounds validation — 725 default, even refusal, range ends.
for r in (0, 1, 2, 100, 101, 724, 725, 726, 0xFFFFFE, 0xFFFFFF, 0x1000000, -5):
    gensalt_raw_seed(f"gensalt-xdes-r{r}", b"xdes", r)
# bf [4,31] and sha [1000, 999999999] ends, one past each.
for r in (3, 4, 5, 30, 31, 32):
    gensalt_raw_seed(f"gensalt-bf-r{r}", b"bf", r)
for r in (999, 1000, 1001, 999999998, 999999999, 1000000000, -1):
    gensalt_raw_seed(f"gensalt-sha256-r{r}", b"sha256crypt", r)
    gensalt_raw_seed(f"gensalt-sha512-r{r}", b"sha512crypt", r)
# pg_strcasecmp: every case spelling must resolve to the same row.
for a in (b"DES", b"Des", b"MD5", b"Md5", b"XDES", b"xDeS", b"BF", b"bF",
          b"SHA256CRYPT", b"Sha512Crypt", b"", b" des", b"des ", b"nope"):
    gensalt_raw_seed(f"gensalt-case-{a.decode('latin1').strip() or 'empty'}",
                     a, 0)
# Entropy shapes: all-zero, all-ones, alternating, and the itoa64 edges — the
# generators mask with 0x3f, so the boundary bytes are the interesting ones.
for tag, e in (("zero", b"\x00" * 32), ("ff", b"\xff" * 32),
               ("alt", b"\xaa\x55" * 16), ("inc", bytes(range(32))),
               ("short3", b"\x01\x02\x03")):
    gensalt_raw_seed(f"gensalt-entropy-{tag}", b"xdes", 725, e)
    gensalt_raw_seed(f"gensalt-entropy-bf-{tag}", b"bf", 6, e)

# ===========================================================================
# arm 2 — armor(): base64 length classes + D8 header shapes
# ===========================================================================
for k in range(0, 13):
    armor_seed(f"armor-len{k}", [], bytes(range(k)))
armor_seed("armor-empty", [], b"")
armor_seed("armor-57", [], b"a" * 57)     # exactly one wrapped line
armor_seed("armor-58", [], b"a" * 58)
armor_seed("armor-114", [], b"a" * 114)
armor_seed("armor-long", [], bytes(range(256)))
armor_seed("armor-hdr-ok", [(b"Version", b"1.0"), (b"Comment", b"hi")], b"x")
# D8 shapes (all five checks + the allowed ": " in a VALUE)
armor_seed("armor-d8-value-newline", [(b"k", b"v\nForged: h")], b"a")
armor_seed("armor-d8-key-newline", [(b"k\nx", b"v")], b"a")
armor_seed("armor-d8-key-colonspace", [(b"k: x", b"v")], b"a")
armor_seed("armor-d8-key-nonascii", [(b"k\xe9", b"v")], b"a")
armor_seed("armor-d8-value-nonascii", [(b"k", b"v\xe9")], b"a")
armor_seed("armor-d8-value-colonspace-ok", [(b"k", b"v: v")], b"a")
armor_seed("armor-d8-key-colon-only", [(b"k:x", b"v")], b"a")
armor_seed("armor-d8-key-space-colon", [(b"k :x", b"v")], b"a")
armor_seed("armor-d8-value-cr", [(b"k", b"v\rx")], b"a")
armor_seed("armor-d8-key-0x7f", [(b"k\x7f", b"v")], b"a")
armor_seed("armor-d8-key-0x80", [(b"k\x80", b"v")], b"a")
armor_seed("armor-hdr-empty-key", [(b"", b"v")], b"a")
armor_seed("armor-hdr-empty-value", [(b"k", b"")], b"a")
armor_seed("armor-hdr-both-empty", [(b"", b"")], b"a")
armor_seed("armor-hdr-three", [(b"a", b"1"), (b"b", b"2"), (b"c", b"3")], b"z")

# ===========================================================================
# arms 3/4 — dearmor / pgp_armor_headers
# ===========================================================================
GOOD = (b"-----BEGIN PGP MESSAGE-----\n\nYWJj\n=TfTH\n"
        b"-----END PGP MESSAGE-----\n")
GOOD_HDR = (b"-----BEGIN PGP MESSAGE-----\nVersion: 1.0\nComment: hi\n\n"
            b"YWJj\n=TfTH\n-----END PGP MESSAGE-----\n")

for sel in (3,):
    tag = "dearmor"
    raw_text_seed(f"{tag}-good", sel, GOOD)
    raw_text_seed(f"{tag}-good-hdr", sel, GOOD_HDR)
    raw_text_seed(f"{tag}-empty", sel, b"")
    raw_text_seed(f"{tag}-junk", sel, b"not armor at all")
    raw_text_seed(f"{tag}-begin-only", sel, b"-----BEGIN PGP MESSAGE-----\n")
    raw_text_seed(f"{tag}-end-only", sel, b"-----END PGP MESSAGE-----\n")
    raw_text_seed(f"{tag}-no-blank", sel,
                  GOOD.replace(b"-----\n\n", b"-----\n"))
    # base64 body length % 4 == 0/1/2/3
    for body in (b"", b"Y", b"YW", b"YWJ", b"YWJj", b"YWJjZA", b"YWJjZGU"):
        raw_text_seed(f"{tag}-b64len{len(body)}", sel,
                      GOOD.replace(b"YWJj", body))
    # all-padding, '=' mid-stream
    raw_text_seed(f"{tag}-allpad", sel, GOOD.replace(b"YWJj", b"===="))
    raw_text_seed(f"{tag}-midpad", sel, GOOD.replace(b"YWJj", b"YW=j"))
    raw_text_seed(f"{tag}-onepad", sel, GOOD.replace(b"YWJj", b"YWJ="))
    raw_text_seed(f"{tag}-twopad", sel, GOOD.replace(b"YWJj", b"YW=="))
    # missing CRC line
    raw_text_seed(f"{tag}-no-crc", sel, GOOD.replace(b"=TfTH\n", b""))
    # CRC line short by one char / long by one / wrong value
    raw_text_seed(f"{tag}-crc-short", sel, GOOD.replace(b"=TfTH", b"=TfT"))
    raw_text_seed(f"{tag}-crc-long", sel, GOOD.replace(b"=TfTH", b"=TfTHX"))
    raw_text_seed(f"{tag}-crc-wrong", sel, GOOD.replace(b"=TfTH", b"=AAAA"))
    raw_text_seed(f"{tag}-crc-bare-eq", sel, GOOD.replace(b"=TfTH", b"="))
    # CRLF and \r-only line endings
    raw_text_seed(f"{tag}-crlf", sel, GOOD.replace(b"\n", b"\r\n"))
    raw_text_seed(f"{tag}-cr", sel, GOOD.replace(b"\n", b"\r"))
    # leading junk before BEGIN, trailing junk after END
    raw_text_seed(f"{tag}-lead-junk", sel, b"junk\n" + GOOD)
    raw_text_seed(f"{tag}-lead-nonl", sel, b"junk" + GOOD)
    raw_text_seed(f"{tag}-trail-junk", sel, GOOD + b"junk\n")
    # header lines with no ": " / with a bare colon / duplicated
    raw_text_seed(f"{tag}-hdr-nocolon", sel,
                  GOOD_HDR.replace(b"Version: 1.0", b"Version1.0"))
    raw_text_seed(f"{tag}-hdr-barecolon", sel,
                  GOOD_HDR.replace(b"Version: 1.0", b"Version:1.0"))
    raw_text_seed(f"{tag}-hdr-empty-value", sel,
                  GOOD_HDR.replace(b"Version: 1.0", b"Version: "))
    raw_text_seed(f"{tag}-hdr-empty-key", sel,
                  GOOD_HDR.replace(b"Version: 1.0", b": 1.0"))
    raw_text_seed(f"{tag}-hdr-nonascii", sel,
                  GOOD_HDR.replace(b"1.0", b"1.\xe9"))
    raw_text_seed(f"{tag}-hdr-only-crlf", sel,
                  GOOD_HDR.replace(b"Version: 1.0\n", b"Version: 1.0\r\n"))
    # every single-byte mutation of the CRC quad (single-field witness pairs
    # for the 6-bit CRC packing)
    for pos in range(4):
        for ch in (b"A", b"B", b"/", b"+", b"0", b"9", b"z"):
            crc = bytearray(b"TfTH")
            crc[pos] = ch[0]
            raw_text_seed(f"{tag}-wp-crc-p{pos}-{ch[0]:02x}", sel,
                          GOOD.replace(b"TfTH", bytes(crc)))
    # envelope builder: every mutation kind, with and without headers
    for kind in range(6):
        env_seed(f"{tag}-env-k{kind}-nohdr", sel, [], b"abcdef", kind)
        env_seed(f"{tag}-env-k{kind}-hdr", sel,
                 [(b"Key", b"val")], b"abcdef", kind)
        env_seed(f"{tag}-env-k{kind}-2hdr", sel,
                 [(b"A", b"1"), (b"B", b"2")], b"", kind)
    # envelope bodies at each base64 length class
    for blen in range(0, 9):
        env_seed(f"{tag}-env-body{blen}", sel, [], bytes(range(blen)), 0)


# ===========================================================================
# arms 4/5 — digest() and hmac()
# ===========================================================================
HASH_NAMES = ["md5", "sha1", "sha224", "sha256", "sha384", "sha512", "MD5",
              "SHA256", "Sha512", "crc32", "", "sha", "md", "sha2", "sha1 ",
              " md5"]


def digest_seed(name, name_idx: int, data: bytes) -> None:
    seed(name, bytes([4, (name_idx & 0x3F) << 2]) + data)


def digest_raw_seed(name, algo: bytes, data: bytes) -> None:
    seed(name, bytes([4, 1, len(algo)]) + algo + data)


def hmac_seed(name, name_idx: int, key: bytes, data: bytes) -> None:
    seed(name, bytes([5, (name_idx & 0x3F) << 2, len(key)]) + key + data)


def hmac_raw_seed(name, algo: bytes, key: bytes, data: bytes) -> None:
    seed(name, bytes([5, 1, len(algo)]) + algo + bytes([len(key)]) + key + data)


# every resolvable name and every miss, over data-length classes that cross
# each hash's block boundary (55/56/63/64 for the 64-byte block family,
# 111/112/127/128 for the 128-byte one) — the padding corner of every hash.
for i in range(len(HASH_NAMES)):
    for dl in (0, 1, 3, 55, 56, 63, 64, 65, 111, 112, 119, 120, 127, 128, 129):
        digest_seed(f"digest-n{i:02d}-d{dl:03d}", i, bytes(range(dl % 256)))
    digest_seed(f"digest-n{i:02d}-abc", i, b"abc")

# free-form names: case mixtures, whitespace, the NAMEDATALEN-1 = 63
# truncation boundary C applies via downcase_truncate_identifier
for nm in (b"MD5", b"Md5", b"mD5", b"SHA1", b"sha256", b"SHA512", b"sha-1",
           b"md5\x00x"[:3], b"md5 ", b" sha1", b"MD" + b"5" * 62,
           b"m" * 62 + b"d5", b"s" * 63, b"s" * 64, b"", b"x"):
    digest_raw_seed(f"digest-name-{nm.hex()[:24] or 'empty'}", nm, b"abc")

# hmac: keys shorter than / equal to / longer than each block size, plus the
# empty key and a key long enough to force the hash-the-key branch
for i in (0, 1, 3, 5):  # md5, sha1, sha256, sha512
    for kl in (0, 1, 63, 64, 65, 127, 128, 129, 159):
        hmac_seed(f"hmac-n{i}-k{kl:03d}", i, bytes(range(kl % 256)), b"data")
    for dl in (0, 1, 55, 56, 64, 128):
        hmac_seed(f"hmac-n{i}-d{dl:03d}", i, b"key", bytes(range(dl % 256)))
# RFC 2202 shapes
hmac_raw_seed("hmac-rfc2202-1", b"md5", b"\x0b" * 16, b"Hi There")
hmac_raw_seed("hmac-rfc2202-2", b"md5", b"Jefe", b"what do ya want for nothing?")
hmac_raw_seed("hmac-rfc2202-3", b"md5", b"\xaa" * 16, b"\xdd" * 50)
hmac_raw_seed("hmac-sha256-nist", b"sha256", b"\x0b" * 20, b"Hi There")
hmac_raw_seed("hmac-miss", b"crc32", b"k", b"d")
hmac_raw_seed("hmac-empty-name", b"", b"k", b"d")

print(f"wrote {n} seeds to {OUT}")
