#!/usr/bin/env python3
"""gen_seeds_gr_fmt.py — gate-remediation witness seeds for fmt_dch_diff /
fmt_num_diff (p1-lanek remediation, proofs/gr-fmt 2026-07-31).

Targets the residual corpus-gap lines recorded in
proofs/coverage/lanek/residual-lanek.tsv plus the 5 dch.rs regression lines
(731/749/759/777/809: ISO-year IYY/IY/I % arms, interval MONTHS_PER_YEAR-1
arm, DCH_FX no-op arm) found by the cov-resweep audit. Deterministic hand
battery: every DCH/NUM keyword x suffix (FM/TH/th/TM) x edge value.

Writes fuzz/corpus/<target>/gr-<name> files. Idempotent.
"""
import os, struct

HERE = os.path.dirname(os.path.abspath(__file__))
DCH_DIR = os.path.join(HERE, "corpus", "fmt_dch_diff")
NUM_DIR = os.path.join(HERE, "corpus", "fmt_num_diff")

wrote = [0]

def seed(dirp, name, data):
    p = os.path.join(dirp, "gr-" + name)
    with open(p, "wb") as f:
        f.write(data)
    wrote[0] += 1

def dch(name, sel, payload):
    seed(DCH_DIR, name, bytes([sel]) + payload)

def num(name, sel, payload):
    seed(NUM_DIR, name, bytes([sel]) + payload)

# ---------------------------------------------------------------- DCH kws
DCH_KWS = [
    "A.D.", "A.M.", "AD", "AM", "B.C.", "BC", "CC", "DAY", "DDD", "DD",
    "DY", "Day", "Dy", "D", "FF1", "FF2", "FF3", "FF4", "FF5", "FF6",
    "FX", "HH24", "HH12", "HH", "IDDD", "ID", "IW", "IYYY", "IYY", "IY",
    "I", "J", "MI", "MM", "MONTH", "MON", "MS", "Month", "Mon", "OF",
    "P.M.", "PM", "Q", "RM", "SSSS", "SSSSS", "SS", "TZH", "TZM", "TZ",
    "US", "WW", "W", "Y,YYY", "YYYY", "YYY", "YY", "Y",
    "a.d.", "a.m.", "ad", "am", "b.c.", "bc", "day", "dy", "j", "month",
    "mon", "p.m.", "pm", "rm", "tz", "y,yyy",
]

def ts_payload(ts, fmt):
    return struct.pack("<q", ts) + fmt.encode()

# timestamps: epoch-2000 microseconds
TS_VALUES = {
    "zero": 0,
    "midday": 45_296_789_123,                    # 2000-01-01 12:34:56.789123
    "y1997": -94_665_600_000_000,                # 1997-01-02
    "bc31": -64_092_211_200_000_000,             # ~year -31 (BC arms)
    "bc4714": -211_810_204_800_000_000,          # near Julian floor
    "y294246": 9_223_371_331_200_000_000,        # near timestamp end
    "neg1s": -1_000_000,
    "imax": 2**63 - 1,                           # infinity image
    "imin": -(2**63),                            # -infinity image
}

for kw_i, kw in enumerate(DCH_KWS):
    tag = f"kw{kw_i:03d}"
    for arm, aname in ((0, "ts"), (1, "tstz")):
        dch(f"{aname}-{tag}-plain", arm, ts_payload(TS_VALUES["midday"], kw))
        dch(f"{aname}-{tag}-fm", arm, ts_payload(TS_VALUES["midday"], "FM" + kw))
        dch(f"{aname}-{tag}-th", arm, ts_payload(TS_VALUES["midday"], kw + "TH"))
        dch(f"{aname}-{tag}-thl", arm, ts_payload(TS_VALUES["midday"], kw + "th"))
        dch(f"{aname}-{tag}-tm", arm, ts_payload(TS_VALUES["midday"], "TM" + kw))
    # BC year + extreme values on the plain picture (arm 0 suffices)
    dch(f"ts-{tag}-bc", 0, ts_payload(TS_VALUES["bc31"], kw))
    dch(f"ts-{tag}-big", 0, ts_payload(TS_VALUES["y294246"], kw))

# composite pictures incl. regression lines (IYY/IY/I; FX no-op; SSSS)
COMPOSITES = [
    "IYYY-IW-ID", "IYY IY I", "FMIYY FMIY FMI", "IYYTH IYth Ith",
    "FXYYYY-MM-DD HH24:MI:SS", "FXDD Mon YYYY",
    "YYYY-MM-DD\"T\"HH24:MI:SS.FF6TZH:TZM",
    "Day, DDth Month y,yyy", "\"quoted literal\"YYYY\\\"esc",
    "HH12 A.M. am P.M. pm", "CCth ccth", "Jth", "Q WW W D ID",
    "SSSS SSSSS MS US FF1FF6", "RMrm", "B.C. b.c. AD ad",
    "TMDay TMMonth TMDy TMMon TMday TMmonth",
    "OF TZH:TZM TZ tz", "Y,YYYth",
]
for i, fmt in enumerate(COMPOSITES):
    for vname, ts in TS_VALUES.items():
        dch(f"comp{i:02d}-{vname}", 0, ts_payload(ts, fmt))
        dch(f"comp{i:02d}-{vname}-tz", 1, ts_payload(ts, fmt))

# ------------------------------------------------------------ interval arm
def iv_payload(time, day, month, fmt):
    return struct.pack("<qii", time, day, month) + fmt.encode()

IV_VALUES = [
    ("zero", 0, 0, 0),
    ("plain", 3_723_456_789, 5, 3),
    ("neg", -3_723_456_789, -5, -3),
    ("mon12", 0, 0, 12),          # tm_mon == 0 arms (MONTH continue)
    ("mon13", 0, 0, 13),
    ("monneg12", 0, 0, -12),
    ("monneg13", 0, 0, -13),      # MONTHS_PER_YEAR-1 arm (dch.rs:777)
    ("monneg1", 0, 0, -1),
    ("imindy", 0, -(2**31), 0),
    ("imaxdy", 0, 2**31 - 1, 0),
    ("imaxmon", 0, 0, 2**31 - 1),
    ("iminmon", 0, 0, -(2**31)),
    ("timemax", 2**63 - 1, 0, 0),
    ("timemin", -(2**63), 0, 0),
    ("hh25", 90_000_000_000, 0, 0),  # 25h: HH12/HH24 wrap arms
]
IV_FMTS = [
    "MM", "MONTH", "Month", "month", "MON", "Mon", "mon", "RM", "rm",
    "YYYY", "Y,YYY", "Y", "CC", "DDD", "DD", "D", "WW", "W", "Q", "J",
    "HH", "HH12", "HH24", "MI", "SS", "SSSS", "MS", "US", "AM", "OF",
    "TZ", "IYYY", "IW", "FMMM FMMON", "MMth", "DDth", "FXMM", "FF3",
    "HH12MI", "Dy", "DAY",
]
for vname, t, d, m in IV_VALUES:
    for j, fmt in enumerate(IV_FMTS):
        dch(f"iv-{vname}-f{j:02d}", 2, iv_payload(t, d, m, fmt))

# --------------------------------------------------------- from_char arms
def fc_payload(inp, fmt):
    b = inp.encode()
    return struct.pack("<H", len(b)) + b + fmt.encode()

FC_PAIRS = [
    ("2024-01-15", "YYYY-MM-DD"),
    ("15 JANUARY 2024", "DD MONTH YYYY"),
    ("15 january 2024", "DD month YYYY"),
    ("15 Jan 2024", "DD Mon YYYY"),
    ("MONDAY 2024 5", "DAY YYYY D"),
    ("friday", "day"),
    ("Fri", "Dy"),
    ("IV 2024", "RM YYYY"),
    ("iv 2024", "rm YYYY"),
    ("XII", "RM"),
    ("15th 2024", "DDth YYYY"),
    ("15TH 2024", "DDTH YYYY"),
    ("11 PM", "HH12 PM"),
    ("11 P.M.", "HH12 P.M."),
    ("11 a.m.", "HH12 a.m."),
    ("4713 BC", "YYYY BC"),
    ("4713 B.C.", "YYYY B.C."),
    ("2024 AD", "YYYY AD"),
    ("2024 a.d.", "YYYY a.d."),
    ("2,024", "Y,YYY"),
    ("21 24", "CC YY"),
    ("-21 24", "CC YY"),
    ("2451545", "J"),
    ("2024-03-1", "IYYY-IW-ID"),
    ("2024-053", "IYYY-IDDD"),
    ("2024-153", "YYYY-DDD"),
    ("86399", "SSSS"),
    ("86399", "SSSSS"),
    ("123", "MS"),
    ("123456", "US"),
    ("1 5", "FF1 FF5"),
    ("GMT", "TZ"),
    ("UTC 2024", "TZ YYYY"),
    ("gmt", "tz"),
    ("+05:30", "OF"),
    ("-08", "OF"),
    ("+14:00 2024", "OF YYYY"),
    ("+05 30", "TZH TZM"),
    ("-05 30", "TZH TZM"),
    (" 05 30", "TZH TZM"),
    ("2024  01  01", "FXYYYY MM DD"),
    ("2024 01 01", "FXYYYY MM DD"),
    ("foo2024", "\"foo\"YYYY"),
    ("x2024", "\\YYYY"),
    ("2024", "  YYYY"),
    ("..2024", "..YYYY"),
    ("99999999999999999999", "YYYY"),
    ("2147483648", "YYYY"),
    ("-0", "YYYY"),
    ("0", "YYYY"),
    ("0 0", "YYYY MM"),
    ("2024 13", "YYYY MM"),
    ("2024 32", "YYYY-MM-DD"),
    ("25:00", "HH24:MI"),
    ("13 PM", "HH12 PM"),
    ("60", "MI"),
    ("61", "SS"),
    ("366 2023", "DDD YYYY"),
    ("366 2024", "DDD YYYY"),
    ("9 9", "Y M"),
    ("5", "Y"),
    ("45", "YY"),
    ("345", "YYY"),
    ("5", "I"),
    ("45", "IY"),
    ("345", "IYY"),
    ("2024", "IYYY"),
    ("7", "ID"),
    ("53", "IW"),
    ("5", "Q"),
    ("4", "W"),
    ("52", "WW"),
    ("", "YYYY"),
    ("2024", ""),
    ("2024-01-15", "FXYYYY-MM-DD  "),
    ("2024-01-15 ", "FXYYYY-MM-DD"),
    ("A", "\"lit\""),
    ("2024+*~", "YYYY+*~"),
    ("2e4", "YYYY"),
    ("٢٠٢٤", "YYYY"),          # non-ASCII digits (multibyte walk)
    ("2024 01", "YYYY MM"),
]
for i, (inp, fmt) in enumerate(FC_PAIRS):
    dch(f"tots-{i:03d}", 3, fc_payload(inp, fmt))
    dch(f"todt-{i:03d}", 4, fc_payload(inp, fmt))

# parse_datetime (arm 5): [u16 ilen][input][strict byte][fmt]
def pdt_payload(inp, fmt, strict):
    b = inp.encode()
    return struct.pack("<H", len(b)) + b + bytes([strict]) + fmt.encode()

PDT = [
    ("2024-01-15", "YYYY-MM-DD", 0),
    ("2024-01-15", "YYYY-MM-DD", 1),
    ("2024-01-15 10:30 +05", "YYYY-MM-DD HH24:MI TZH", 0),
    ("10:30:45.123", "HH24:MI:SS.MS", 0),
    ("10:30", "HH24:MI", 1),
    ("2024-01-15 junk", "YYYY-MM-DD", 1),
    ("2024-01-15 junk", "YYYY-MM-DD", 0),
    ("2024 +05:30", "YYYY OF", 0),
    ("2024 GMT", "YYYY TZ", 0),
    ("2024", "YYYY TZH", 1),
    ("2024-01-15 10:30", "YYYY-MM-DD HH24:MI TZM", 0),
    ("53 7 2024", "IW ID IYYY", 0),
]
for i, (inp, fmt, s) in enumerate(PDT):
    dch(f"pdt-{i:02d}", 5, pdt_payload(inp, fmt, s))

# has_tz (arm 6)
for i, fmt in enumerate(["TZ", "tz", "TZH", "TZM", "OF", "YYYY", "FXTZH", "\"TZ\"", ""]):
    dch(f"htz-{i}", 6, fmt.encode())

# ---------------------------------------------------------------- NUM half
NUM_FMTS = [
    "9", "0", "99999", "00000", "9,999", "0,000", "9G999", "9.9", "0.00",
    "9D9", "FM9990.099", "S9999", "9999S", "9999MI", "MI9999", "PL9999",
    "SG9999", "9999PR", "L9999", "9999L", "9999TH", "9999th", "RN",
    "FMRN", "rn", "FMrn", "B9999", "9 9", "V99", "99V99", "999V999",
    "9.99EEEE", "FM9.99EEEE", "S9.99EEEE", "9999999999999999999999",
    "099999999999999999999", "9999V99999999999999999999",
    "FM99999999999999999999.099999999999999999999",
    "L9G999D99PR", "\"lit\"9999", "9999\"lit\"",
]
NUM_STRS = [
    "0", "1", "-1", "0.5", "-0.5", "123.456", "-123.456", "1234567",
    "-1234567", "NaN", "Infinity", "-Infinity", "1e100", "-1e-100",
    "99999999999999999999.9999", "0.00001", "3999", "4000", "-3999",
]

def n2c_payload(numstr, fmt):
    b = numstr.encode()
    return bytes([len(b)]) + b + fmt.encode()

for i, fmt in enumerate(NUM_FMTS):
    for j, s in enumerate(NUM_STRS):
        num(f"n2c-f{i:02d}-v{j:02d}", 0, n2c_payload(s, fmt))

INTS = [0, 1, -1, 42, 3999, 4000, -3999, 2**31 - 1, -(2**31)]
INTS64 = INTS + [2**63 - 1, -(2**63)]
for i, fmt in enumerate(NUM_FMTS):
    for j, v in enumerate(INTS):
        num(f"i4-f{i:02d}-v{j:02d}", 1, struct.pack("<i", v) + fmt.encode())
    for j, v in enumerate(INTS64):
        num(f"i8-f{i:02d}-v{j:02d}", 2, struct.pack("<q", v) + fmt.encode())

F32S = [0.0, -0.0, 1.5, -1.5, float("nan"), float("inf"), float("-inf"),
        1e-45, 3.4e38, 123456.789, 0.000123]
F64S = F32S + [1e300, -1e-300, 1.7976931348623157e308]
for i, fmt in enumerate(NUM_FMTS):
    for j, v in enumerate(F32S):
        num(f"f4-f{i:02d}-v{j:02d}", 3, struct.pack("<f", v) + fmt.encode())
    for j, v in enumerate(F64S):
        num(f"f8-f{i:02d}-v{j:02d}", 4, struct.pack("<d", v) + fmt.encode())

def t2n_payload(inp, fmt):
    b = inp.encode()
    return struct.pack("<H", len(b)) + b + fmt.encode()

T2N = [
    ("1234", "9999"), ("1,234", "9,999"), ("1,234.56", "9G999D99"),
    ("1234.56", "9999.99"), ("-1234", "S9999"), ("+1234", "S9999"),
    ("1234-", "9999MI"), ("<1234>", "9999PR"), ("$1234", "L9999"),
    ("1234", "L9999"), ("0.001", "FM9.999"), (".5", "99.99"),
    ("  12", "99999"), ("12  ", "99999"), ("", "9999"), ("1234", ""),
    ("abc", "9999"), ("12a4", "9999"), ("999999999999999999999", "999999999999999999999"),
    ("123", "9V99"), ("12345", "99V999"), ("1234", "9999TH"),
    ("1234th", "9999th"), ("12,34", "99,99"), ("1.2.3", "9.9.9"),
    ("-", "9MI"), ("<>", "9PR"), ("12", "B99"), ("1 2", "9 9"),
    ("1234", "FMRN"), ("MCMXC", "RN"),
]
for i, (inp, fmt) in enumerate(T2N):
    num(f"t2n-{i:02d}", 5, t2n_payload(inp, fmt))

print(f"wrote {wrote[0]} seeds")
