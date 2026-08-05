#!/usr/bin/env bash
# Generate a starter seed corpus for the fuzz targets. Hand-built well-formed
# inputs so libFuzzer starts from valid structure (coverage-guided fuzzing pays
# compounding returns on an accumulated corpus — persist corpus/ to S3 between
# runs: s3://${PGRUST_FLEET_BUCKET}/fuzz-corpora/).
#
# For a richer WAL seed corpus, harvest real records in-pod after a workload:
#   split pg_wal segment bytes into per-record slices (post-page-header,
#   post-record-header TLV bodies) and drop them into corpus/wal_record/.
set -euo pipefail
cd "$(dirname "$0")"

mkdir -p corpus/wal_record corpus/wire_pqformat

# --- wal_record: TLV record bodies (block headers + main-data markers) -------
# empty body
printf '' > corpus/wal_record/empty
# main-data-short: id 0xff, len, payload
printf '\xff\x05hello' > corpus/wal_record/main_short
# main-data-long: id 0xfe, u32 len (LE 260), payload
printf '\xfe\x04\x01\x00\x00' > corpus/wal_record/main_long_hdr
python3 - <<'PY'
open("corpus/wal_record/main_long_full","wb").write(b"\xfe" + (260).to_bytes(4,"little") + b"A"*260)
# one block ref (block_id 0), fork_flags HAS_DATA(0x20), data_len u16=4, rloc+blk, then data + main-short
blk = b"\x00" + b"\x20" + (4).to_bytes(2,"little")
rloc = (1).to_bytes(4,"little")+(2).to_bytes(4,"little")+(3).to_bytes(4,"little")+(0).to_bytes(4,"little")
main = b"\xff\x02hi"
open("corpus/wal_record/one_block","wb").write(blk + rloc + main + b"data")
PY

# --- wire_pqformat: [enc_sel][opcode-driven message] -------------------------
# enc=SQL_ASCII(0), then getmsgstring op(7) over a cstring, then end(10)
printf '\x00\x07hello\x00\x0a' > corpus/wire_pqformat/string_utf
# enc=UTF8(6), getmsgint4(2) + getmsgint64(3)
printf '\x06\x02\x00\x00\x00\x2a\x03\x00\x00\x00\x00\x00\x00\x00\x01' > corpus/wire_pqformat/ints
# enc=UTF8(6), getmsgbytes op(6) len(4) + 4 bytes
printf '\x06\x06\x04ABCD' > corpus/wire_pqformat/bytes
# multibyte edges under UTF8: truncated 2-byte lead
printf '\x06\x08\xc3' > corpus/wire_pqformat/utf8_trunc

# --- differential targets (C-oracle vs shipped Rust) -------------------------
mkdir -p corpus/float_in_diff corpus/float_out_diff corpus/geo_diff
python3 - <<'PY'
import struct, os

# float_in_diff: [sel][text]; sel bit0: 0=f8, 1=f4
strs = ["0","-0","1.5"," 1.5 ","1e-45","1e309","1e-323","5e-324","2.5e-324",
        "1.7976931348623157e308","2.2250738585072011e-308","3.4028236e38",
        "7.038531e-26","0.1","1e","..5","NaN","nan(1234)","-Infinity","inf",
        "0x1p3","0x1p-1074","9007199254740993","1.5junk","+","1,5"]
for i, s in enumerate(strs):
    for sel in (0, 1):
        open(f"corpus/float_in_diff/s{sel}_{i:02d}", "wb").write(bytes([sel]) + s.encode())

# float_out_diff: [sel][le bits]; sel bit0: 0=f8 (8 bytes), 1=f4 (4 bytes)
f64s = [0.0, -0.0, 1.0, 0.1, 2**53, 1e23, 5e-324, 1.7976931348623157e308,
        float("inf"), float("-inf"), float("nan"), 2.2250738585072014e-308]
for i, v in enumerate(f64s):
    open(f"corpus/float_out_diff/d{i:02d}", "wb").write(b"\x00" + struct.pack("<d", v))
    open(f"corpus/float_out_diff/f{i:02d}", "wb").write(b"\x01" + struct.pack("<f", struct.unpack("<f", struct.pack("<f", min(max(v, -3e38), 3e38) if v == v and abs(v) != float("inf") else v))[0]))

# geo_diff: sel 0 = point_out [x][y]; sel 1 = on_ppath [closed][pt][pts...]
pts = [(0.0, 0.0), (1.5, -2.5), (1e300, 1e-300), (float("nan"), 1.0),
       (float("inf"), float("-inf"))]
for i, (x, y) in enumerate(pts):
    open(f"corpus/geo_diff/po{i:02d}", "wb").write(b"\x00" + struct.pack("<dd", x, y))
tri = struct.pack("<dd", 0.5, 0.5) + struct.pack("<dd", 0.0, 0.0) + \
      struct.pack("<dd", 1.0, 0.0) + struct.pack("<dd", 0.0, 1.0)
open("corpus/geo_diff/path_open", "wb").write(b"\x01\x00" + tri)
open("corpus/geo_diff/path_closed", "wb").write(b"\x01\x01" + tri)
PY

# --- float math differentials (libm family) ---------------------------------
mkdir -p corpus/float_math_diff corpus/float_math2_diff
python3 - <<'PY'
import struct

# Deliberate boundary values — keep in sync with diff.rs FLOAT_MATH_VAL_CORPUS
# (domain edges, exp/gamma overflow-underflow edges, denormals, degree wrap
# points, near-pi/2, +-0/+-Inf/NaN).
vals = [0.0, -0.0, 1.0, -1.0, 0.5, -0.5, 0.9999999999999999, 1.0000000000000002,
        1.5, -1.5, -2.0, -3.0, 0.1, 30.0, 45.0, 60.0, 90.0, 180.0, 270.0, 360.0,
        -90.0, -360.0, 720.5, 90.00000000000001, 1.5707963267948966,
        3.141592653589793, 709.782712893384, -745.1332191019413, 710.0,
        171.62437695630272, -171.5, 5e-324, -5e-324, 2.2250738585072014e-308,
        1e308, -1e308, 1e22, float("inf"), float("-inf"), float("nan")]
# unary: [id][8 bytes]; every function id gets a spread of boundary values
for fid in range(28):
    for i, v in enumerate(vals):
        open(f"corpus/float_math_diff/f{fid:02d}_{i:02d}", "wb").write(
            bytes([fid]) + struct.pack("<d", v))
# two-arg: [id][16 bytes]; POSIX pow lattice + atan2 quadrant/inf corners
pairs = [(0.0, -1.0), (-0.0, -3.0), (0.0, 0.0), (1.0, float("nan")),
         (float("nan"), 0.0), (float("nan"), float("nan")), (-1.0, 0.5),
         (-2.0, 3.0), (-2.0, 2.0), (-1.0, float("inf")), (0.5, float("-inf")),
         (2.0, float("inf")), (float("-inf"), 3.0), (float("-inf"), -3.0),
         (float("-inf"), 2.0), (float("inf"), -1.0), (2.0, 1e18), (2.0, 9.9e15),
         (10.0, 309.0), (10.0, -324.0), (-0.0, float("inf")), (1e308, 2.0)]
for fid in range(3):
    for i, (a, b) in enumerate(pairs):
        open(f"corpus/float_math2_diff/g{fid}_{i:02d}", "wb").write(
            bytes([fid]) + struct.pack("<dd", a, b))
PY

# --- char/bool differentials (phase-1 100%-coverage campaign) ----------------
# Literals harvested mechanically from the vendored 18.3 regress SQL, crossed
# with every selector; plus hand seeds for the non-text selectors.
mkdir -p corpus/char_diff corpus/bool_diff
python3 - <<'PY'
import re, hashlib, pathlib

# repo root may be the main clone or a .wt-* worktree one level deeper.
_cands = [pathlib.Path(p) for p in
          ("../../pgrust-fabled/vendor/postgres-src/src/test/regress/sql",
           "../../../pgrust-fabled/vendor/postgres-src/src/test/regress/sql")]
REGRESS = next(p for p in _cands if p.is_dir())

def harvest(sqlfile, maxlen=24):
    txt = (REGRESS / sqlfile).read_text(errors="replace")
    return sorted(set(m[1:-1] for m in re.findall(r"'[^']*'", txt) if len(m) - 2 <= maxlen))

def put(tdir, tag, data):
    h = hashlib.sha1(data).hexdigest()[:12]
    (pathlib.Path(tdir) / f"{tag}_{h}").write_bytes(data)

# bool_diff: regress boolean.sql literals across selectors 0 (boolin),
# 1 (parse_bool_with_len), plus fixed out/cmp/agg shapes.
for lit in harvest("boolean.sql"):
    b = lit.encode()
    put("corpus/bool_diff", "in", bytes([0]) + b)
    put("corpus/bool_diff", "pb", bytes([1]) + b)
for b0 in (0, 1, 2, 255):
    put("corpus/bool_diff", "out", bytes([2, b0, 1]))
    put("corpus/bool_diff", "cmp", bytes([3, b0, b0 ^ 1]))
for ops in (b"", b"\x00\x04\x04\x01\x05", b"\x04\x04\x04", b"\x02\x02", b"\x01",
            b"\x05\x04", b"\x04\x05\x01\x00\x02\x06\x07\x03"):
    put("corpus/bool_diff", "agg", bytes([4]) + ops)

# char_diff: regress char.sql literals (octal escapes included) across
# selectors 0 (charin) and 4 (text_char), plus fixed shapes for out/cmp/
# int/recv.
for lit in harvest("char.sql"):
    b = lit.encode()
    put("corpus/char_diff", "in", bytes([0]) + b)
    put("corpus/char_diff", "tc", bytes([4]) + b)
for b0 in (0, 1, 0x41, 0x7F, 0x80, 0xFF):
    put("corpus/char_diff", "out", bytes([1, b0]))
    put("corpus/char_diff", "cmp", bytes([2, b0, (b0 + 1) & 0xFF]))
    put("corpus/char_diff", "rcv", bytes([5, b0]))
for arg in (-129, -128, -1, 0, 127, 128, 2**31 - 1, -2**31):
    put("corpus/char_diff", "i4",
        bytes([3]) + (arg & 0xFFFFFFFF).to_bytes(4, "little"))
PY

echo "seed corpus written under $(pwd)/corpus/"

# ---- pg_lsn_diff (Lane-0A p1 campaign): selector-stamped seeds ----
# Layout: [sel%8][payload]; regress literals harvested from
# vendor/postgres-src/src/test/regress/sql/pg_lsn.sql (Stamp 18.3).
mkdir -p corpus/pg_lsn_diff
python3 - <<'PYEOF'
import os, re, struct
out = "corpus/pg_lsn_diff"
def w(name, data):
    open(os.path.join(out, name), "wb").write(data)

lsn_texts = ["0/0", "0/12345678", "ABCD1234/beef0001", "FFFFFFFF/FFFFFFFF",
             "16/B374D848", "0/16B3748", "1/2", "0/FF", "", "/", "0/", "/0",
             "123456789/0", "0/123456789", " 0/0", "0/0 ", "xyz/0"]
# harvest every 'X/X'-shaped literal from the vendored regress pg_lsn.sql
reg = "../../pgrust-fabled/vendor/postgres-src/src/test/regress/sql/pg_lsn.sql"
try:
    sql = open(reg).read()
    lsn_texts += re.findall(r"'([0-9A-Fa-f]{1,9}/[0-9A-Fa-f]{1,9})'", sql)[:64]
except OSError:
    pass
for i, t in enumerate(dict.fromkeys(lsn_texts)):
    w(f"in_{i:03}", b"\x00" + t.encode())

vals = [0, 1, 0xFF, 0xFFFFFFFF, 0x100000000, 0xABCD1234BEEF0001,
        2**64 - 1, 2**63, 2**63 - 1]
for i, v in enumerate(vals):
    w(f"out_{i:02}", b"\x01" + struct.pack("<Q", v))
    w(f"send_{i:02}", b"\x03" + struct.pack(">Q", v))
for i, (a, b) in enumerate([(a, b) for a in vals[:5] for b in vals[:5]]):
    w(f"cmp_{i:02}", b"\x02" + struct.pack("<QQ", a, b))
    w(f"mi_{i:02}", b"\x04" + struct.pack("<QQ", a, b))

nums = ["0", "1", "-1", "10", "0.5", "-0.5", "2.5", "18446744073709551615",
        "18446744073709551616", "-18446744073709551615", "NaN", "Infinity",
        "-Infinity", "1e10", "1e-10", "1e1000000", " 42 ", "1_000"]
for i, n in enumerate(nums):
    w(f"pli_{i:02}", b"\x05" + struct.pack("<Q", 0xFF) + n.encode())
    w(f"mii_{i:02}", b"\x06" + struct.pack("<Q", 2**32) + n.encode())
    w(f"npl_{i:02}", b"\x07" + n.encode())
PYEOF
