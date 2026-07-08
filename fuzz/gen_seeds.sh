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

echo "seed corpus written under $(pwd)/corpus/"
