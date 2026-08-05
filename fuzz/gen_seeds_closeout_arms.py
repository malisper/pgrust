#!/usr/bin/env python3
"""Directed closeout seeds for adt_date + adt_datetime coverage (p1-lanel2).

Writes raw seed files named closeout-* into corpus/<target>/ for:
  - datetime_io_diff   (arms 0/2/4: date_in / time_in / timetz_in text)
  - timestamp_diff     (arm 0 timestamp[tz]_in text, arm 2 interval_in text)
  - interval_engine_diff (arm 1: DecodeISO8601Interval text)

Target lines: decode.rs error/overflow arms + the strtod_model /
hex_subnormal_exact / token_true_value_below_dblmin block (reachable only
via ParseISO8601Number, i.e. interval text starting with 'P').

PLATFORM NOTE: tokens whose ROUNDED value is +-DBL_MIN (tiny-boundary and
k>500 shapes) are glibc-vs-macOS strtod ERANGE divergent; timestamp_diff's
interval arm carves them (dblmin_boundary) AFTER the Rust side runs, so
they are seeded ONLY there, never into interval_engine_diff (which has no
carve and would assert on macOS replay).
"""
import os

HERE = os.path.dirname(os.path.abspath(__file__))


def put(target: str, name: str, data: bytes):
    d = os.path.join(HERE, "corpus", target)
    assert os.path.isdir(d), d
    with open(os.path.join(d, f"closeout-{name}"), "wb") as f:
        f.write(data)


def io_date_in(name, text, style=0):
    # sel%9==0 -> date_in; payload [style][text]
    put("datetime_io_diff", f"datein-{name}", bytes([0, style]) + text.encode())


def io_time_in(name, text, style=0, typmod=0):
    # sel%9==2 -> time_in; payload [style][typmod][text]; typmod byte 0 -> -1
    put("datetime_io_diff", f"timein-{name}", bytes([2, style, typmod]) + text.encode())


def io_timetz_in(name, text, style=0, typmod=0):
    # sel%9==4 -> timetz_in; payload [style][typmod][text]
    put("datetime_io_diff", f"timetzin-{name}", bytes([4, style, typmod]) + text.encode())


def ts_in(name, text, style=0, tz=1):
    # timestamp_diff sel%26==0; payload [style][ts_typmod byte][tz byte][text]
    put("timestamp_diff", f"tsin-{name}", bytes([0, style, 0, tz]) + text.encode())


def ts_interval_in(name, text):
    # timestamp_diff sel%26==2; payload [istyle][tb][tb2][text];
    # tb2=0x80 -> typmod -1 (unconstrained)
    put("timestamp_diff", f"ivin-{name}", bytes([2, 0, 0, 0x80]) + text.encode())


def iso_engine(name, text):
    # interval_engine_diff sel%3==1 -> DecodeISO8601Interval; payload = text
    put("interval_engine_diff", f"iso-{name}", bytes([1]) + text.encode())


# ---------------------------------------------------------------------------
# FILE 2: strtod_model / hex_subnormal_exact / token_true_value_below_dblmin
# ---------------------------------------------------------------------------

# SAFE on both platforms (exact subnormals: no ERANGE anywhere; inexact
# subnormals / underflow-to-zero: ERANGE on both glibc and macOS).
SAFE_ISO = {
    "hex-exact-1p1073": "P0x1p-1073Y",
    "hex-exact-1p1074": "P0x1p-1074Y",
    "hex-exact-18p1073": "P0x1.8p-1073Y",
    "hex-exact-fracform": "P0x0.0000000000001p-1022Y",
    "hex-exact-2p1075": "P0x2p-1075Y",
    "hex-exact-neg": "P-0x1p-1074Y",
    "hex-exact-dblmin": "P0x1p-1022Y",          # == DBL_MIN exactly (normal)
    "hex-exact-negdblmin": "P-0x1p-1022Y",      # sign path of token_true hex
    "hex-inexact-sub": "P0x1234.56p-1080Y",     # inexact subnormal: ERANGE both
    "hex-inexact-sub2": "P0x1.fp-1073Y",        # inexact subnormal
    "hex-longdigits": "P0x00000000000000000000000000000000000000001p-1074Y",
    "hex-zero": "P0x0p-1074YT0S",               # zero mantissa hex, value 0
    "hex-plus": "P+0x1p-1074Y",
    "dec-inexact-sub": "P1e-309Y",
    "dec-inexact-sub2": "P4.9e-324Y",
    "dec-under-500": "P1e-500Y",
    "dec-under-600": "P1e-600Y",
    "dec-under-15e321": "P15e-321Y",
    "dec-sub-15e320": "P1.5e-320Y",
    "dec-eplus": "P1.5e+2Y",
    "over-inf": "P1e400Y",
    "neg-inf-special": "P-infY",
    # decimal token that parses to exactly DBL_MIN, true value ABOVE the
    # boundary: token_true_value_below_dblmin runs and returns false; no
    # ERANGE on either platform.
    "dec-dblmin-above": "P2.2250738585072014e-308Y",
    "dec-dblmin-above-neg": "P-2.2250738585072014e-308Y",
    "dec-dblmin-above-plus": "P+2.2250738585072014e-308Y",
}

for n, t in SAFE_ISO.items():
    iso_engine(n, t)
    ts_interval_in(n, t)

# BOUNDARY shapes (rounded value == +-DBL_MIN, true value below): glibc
# ERANGE / macOS errno=0 -> timestamp_diff ONLY (dblmin_boundary carve).
BOUNDARY = {
    "hex-tinybound": "P0x1.fffffffffffffp-1023Y",
    "hex-tinybound-neg": "P-0x1.fffffffffffffp-1023Y",
    "dec-tinybound": "P2.2250738585072011e-308Y",
    "dec-tinybound-neg": "P-2.2250738585072011e-308Y",
    "dec-tinybound-nodot": "P22250738585072011e-324Y",
}
for n, t in BOUNDARY.items():
    ts_interval_in(n, t)

# k-spread of below-boundary decimal DBL_MIN tokens: exercises the bignum
# compare (limb-length differ + digit walk) and, at k=501, the k>500 arm.
# D_k = floor(2^-1022 * 10^k): true value strictly below DBL_MIN, rounds up
# to it (error < 10^-k << half-ulp for k >= 324). timestamp_diff ONLY.
for k in (324, 331, 340, 350, 365, 380, 400, 428, 450, 460, 470, 480, 490, 500, 501):
    d = (10**k) // (2**1022)
    tok = f"{d}e-{k}"
    if k == 501:
        # 194 digits + "e-501" = 199 chars; "P"+199 = 200 bytes: no unit
        # letter so the whole text fits the 200-byte payload cap. The
        # reversed DecodeInterval walk hits the 'e' STRING field first and
        # fails DTERR_BAD_FORMAT, so interval_in falls back to ISO-8601.
        text = "P" + tok
    else:
        text = "P" + tok + "Y"
    assert len(text) <= 200, (k, len(text))
    ts_interval_in(f"dec-kspread-{k}", text)

# carry-ripple token: a DBL_MIN-rounding decimal whose digit accumulation
# overflows limb0 during token_true_value_below_dblmin's add_small, forcing
# the carry-propagation (no-break) path (decode.rs:2765). Constructed so the
# 152-digit prefix P satisfies P*10 == 2^64-8 (mod 2^64) and the final digit
# 9 pushes the low limb past 2^64. timestamp_diff ONLY (boundary shape).
def _carry_ripple_token():
    k = 460
    s = str((10**k) // (2**1022))          # 153 digits of DBL_MIN*10^460
    fixed = int(s[:-21])
    r = 2**64 - 8
    c = ((r // 2) * pow(5, -1, 2**63)) % (2**63)
    t = (c - fixed * 10**20) % (2**63)
    digits = s[:-21] + str(t).rjust(20, "0") + "9"
    assert (int(digits[:-1]) * 10) % (2**64) == r
    return f"P{digits}e-{k}Y"


ts_interval_in("dec-carry-ripple", _carry_ripple_token())

# ceil variants (true value just ABOVE DBL_MIN, still rounds to it):
# token_true_value_below_dblmin walks the bignum compare and returns false.
# Safe on both platforms (no ERANGE anywhere) but keep them in
# timestamp_diff too (carved there regardless -- Rust runs first).
for k in (324, 350, 400, 460, 500):
    d = (10**k) // (2**1022) + 1
    text = f"P{d}e-{k}Y" if k < 501 else f"P{d}e-{k}"
    assert len(text) <= 200
    ts_interval_in(f"dec-kceil-{k}", text)

# ---------------------------------------------------------------------------
# NOTE: a ~370-seed shotgun battery for the decode.rs / adt_date lib.rs
# error-arm closeout list (DecodeDate dup-mask, DecodeNumber cp>2 dot,
# ptype julian/time twins, DecodeInterval force-negative/i64::MIN arms,
# ISO8601 yyyymmdd/hhmmss overflow arms, adt_date timestamp2tm-err ctors)
# was generated, replayed under coverage on all three text-driving targets,
# and turned NONE of those lines green -- consistent with the static
# analysis that they are defensively unreachable through the driven entry
# points (ParseDateTime field typing + the targets' domain fences). The
# battery was therefore pruned from the corpus; see the lane report for
# the per-line unreachability reasons.
