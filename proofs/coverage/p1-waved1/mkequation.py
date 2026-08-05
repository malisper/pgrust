#!/usr/bin/env python3
"""Coverage equation for the backend/access/transam/xlogreader FAMILY-GRAIN
carve (lane p1-waved1). The census row carves IN the pure WAL-decode surface
and OUT the callback-IO page assembly, so the denominator is the union of the
in-scope function spans below, not the whole file (carve_files=src/lib.rs,
family-grain; see phase1-carve-census.tsv row and the target header in
fuzz/core/src/xlogreader_diff.rs).

usage: mkequation.py <lcov-file>
"""
import re
import sys

FILE = "crates/backend/access/transam/xlogreader/src/lib.rs"

# In-scope spans: (lo, hi) inclusive, with the census surface each belongs to.
SPANS = [
    (82, 84, "XLByteToSeg (segment math)"),
    (86, 88, "XLogSegmentOffset (segment math)"),
    (100, 102, "lsn_fmt (errormsg LSN rendering)"),
    (104, 112, "XLogFileName (segment math)"),
    (114, 122, "crc_init/crc_comp/crc_fin (record CRC32C)"),
    (126, 132, "XLogPageHeaderSize (page-header validation)"),
    (146, 163, "read_u16/u32/u64 (decode primitives)"),
    (165, 180, "parse_page_header (page-header validation)"),
    (192, 201, "parse_xlog_record (record-header decode)"),
    (203, 209, "parse_rel_file_locator (block-ref decode)"),
    (211, 217, "record_crc (ValidXLogRecord CRC)"),
    (273, 280, "DecodeXLogRecordRequiredSpace"),
    (1238, 1296, "ValidXLogRecordHeader"),
    (1325, 1349, "XLogReaderValidatePageHeader (entry)"),
    (1519, 1562, "RestoreBlockImage (entry)"),
    (1636, 1754, "validate_page_header (core)"),
    (1780, 1799, "PayloadSink::put (decode_record support)"),
    (1802, 2122, "decode_record (C DecodeXLogRecord)"),
    (2134, 2158, "restore_err_msg"),
    (2160, 2205, "restore_image_core"),
]
# Out-of-scope (census OUT): ReadPageInternal/XLogDecodeNextRecord page
# assembly + callbacks, WALRead + seams, read-ahead queue, reader-state glue,
# XLByteInSeg/XRecOffIsValid (WALRead/assembly support), cfg-gated errno
# shims (platform pairs), marshal/view accessors.


def main(path):
    lcov = open(path).read()
    blk = next(b for b in lcov.split("end_of_record") if FILE in b)
    da = {int(a): int(b) for a, b in re.findall(r"DA:(\d+),(\d+)", blk)}
    tot = cov = 0
    residual = []
    print("span\tinstrumented\tcovered\tuncovered_lines")
    for lo, hi, name in SPANS:
        lines = [l for l in da if lo <= l <= hi]
        c = [l for l in lines if da[l] > 0]
        u = sorted(l for l in lines if da[l] == 0)
        tot += len(lines)
        cov += len(c)
        residual += u
        print(f"{name} [{lo}-{hi}]\t{len(lines)}\t{len(c)}\t{u if u else '-'}")
    print(f"\nTOTAL in-scope instrumented lines: {tot}")
    print(f"covered by fuzz: {cov}")
    print(f"residual (must equal recorded exceptions): {len(residual)}: {residual}")


if __name__ == "__main__":
    main(sys.argv[1])
