#!/usr/bin/env python3
"""Extract verbatim C function/table definitions from the vendored
PostgreSQL 18.3 checkout (../pgrust-reference/vendor/postgres-src @
62d6c7d3df6287f1bd83199c1a746e50d31571a0) for a differential-fuzz oracle.

Bodies are copied BYTE-FOR-BYTE from the definition line (function name at
column 0, return type on the line(s) above) through the closing brace.
Used by pg_datetime_io_io.c's generation recipe (see that file's header);
re-run to refresh: the output is committed, this script is provenance.
"""
import re
import sys

VENDOR = "/home/dev/dev/pgrust-reference/vendor/postgres-src"


def load(rel):
    with open(f"{VENDOR}/{rel}") as f:
        return f.readlines()


def extract_fn(lines, name):
    """Function whose NAME starts a line at col 0 ('type\nname(args)\n{')."""
    pat = re.compile(rf"^{re.escape(name)}\(")
    for i, ln in enumerate(lines):
        if pat.match(ln):
            # back up over return-type/qualifier lines (1-2 lines, col 0,
            # no braces, not a comment end)
            start = i
            j = i - 1
            while j >= 0 and re.match(
                r"^(static |const |inline |unsigned |struct |size_t|long|double|int|char|bool|void|float8|Datum|pg_time_t|fsec_t|TimeADT|DateADT|Timestamp|datetkn)",
                lines[j],
            ) and "{" not in lines[j] and ";" not in lines[j] and "*/" not in lines[j]:
                start = j
                j -= 1
            depth = 0
            seen = False
            for k in range(i, len(lines)):
                depth += lines[k].count("{") - lines[k].count("}")
                if "{" in lines[k]:
                    seen = True
                if seen and depth == 0:
                    return "".join(lines[start : k + 1])
            break
    raise SystemExit(f"extract_fn: {name} not found")


def extract_var(lines, name):
    """Table/variable definition 'name[...] = {...};' or '... name = ...;'."""
    pat = re.compile(rf"^\s*(static\s+)?const?.*\b{re.escape(name)}\s*[\[=]")
    for i, ln in enumerate(lines):
        if pat.match(ln):
            if ";" in ln:
                return ln
            depth = 0
            for k in range(i, len(lines)):
                depth += lines[k].count("{") - lines[k].count("}")
                if depth == 0 and ";" in lines[k]:
                    return "".join(lines[i : k + 1])
    raise SystemExit(f"extract_var: {name} not found")


def main(out):
    dt = load("src/backend/utils/adt/datetime.c")
    date = load("src/backend/utils/adt/date.c")
    ts = load("src/backend/utils/adt/timestamp.c")
    nu = load("src/backend/utils/adt/numutils.c")
    cs = load("src/common/string.c")
    sc = load("src/backend/parser/scansup.c")
    pc = load("src/port/pgstrcasecmp.c")
    sl = load("src/port/strlcpy.c")

    w = open(out, "w")

    def sec(title, text):
        w.write(f"\n/* ==== VERBATIM: {title} ==== */\n\n")
        w.write(text)

    sec("src/port/pgstrcasecmp.c pg_tolower", extract_fn(pc, "pg_tolower"))
    sec("src/port/pgstrcasecmp.c pg_toupper", extract_fn(pc, "pg_toupper"))
    sec("src/port/strlcpy.c strlcpy (renamed pg_dt_strlcpy via #define)",
        extract_fn(sl, "strlcpy"))
    sec("src/common/string.c strtoint", extract_fn(cs, "strtoint"))
    sec("src/backend/utils/adt/numutils.c DIGIT_TABLE", extract_var(nu, "DIGIT_TABLE"))
    sec("src/backend/utils/adt/numutils.c decimalLength32", extract_fn(nu, "decimalLength32"))
    sec("src/backend/utils/adt/numutils.c pg_ultoa_n", extract_fn(nu, "pg_ultoa_n"))
    sec("src/backend/utils/adt/numutils.c pg_ultostr_zeropad",
        extract_fn(nu, "pg_ultostr_zeropad"))
    sec("src/backend/utils/adt/numutils.c pg_ultostr", extract_fn(nu, "pg_ultostr"))
    sec("src/backend/parser/scansup.c downcase_truncate_identifier",
        extract_fn(sc, "downcase_truncate_identifier"))
    sec("src/backend/parser/scansup.c downcase_identifier",
        extract_fn(sc, "downcase_identifier"))
    sec("src/backend/utils/adt/timestamp.c dt2time", extract_fn(ts, "dt2time"))
    sec("src/backend/utils/adt/timestamp.c GetEpochTime", extract_fn(ts, "GetEpochTime"))

    # datetime.c tables
    for v in ["day_tab", "months", "days", "datetktbl", "szdatetktbl",
              "deltatktbl", "szdeltatktbl"]:
        sec(f"src/backend/utils/adt/datetime.c {v}", extract_var(dt, v))

    # datetime.c functions
    for f in ["date2j", "j2date", "j2day", "AppendSeconds", "ParseFraction",
              "ParseFractionalSecond", "ParseDateTime", "DecodeDateTime",
              "DetermineTimeZoneOffset", "DetermineTimeZoneOffsetInternal",
              "DetermineTimeZoneAbbrevOffset",
              "DetermineTimeZoneAbbrevOffsetInternal", "TimeZoneAbbrevIsKnown",
              "DecodeTimeOnly", "DecodeDate", "ValidateDate",
              "DecodeTimeCommon", "DecodeTime", "DecodeNumber",
              "DecodeNumberField", "DecodeTimezone", "DecodeTimezoneAbbrev",
              "ClearTimeZoneAbbrevCache", "DecodeSpecial", "DecodeUnits",
              "DateTimeParseError", "datebsearch", "EncodeTimezone",
              "EncodeDateOnly", "EncodeTimeOnly"]:
        sec(f"src/backend/utils/adt/datetime.c {f}", extract_fn(dt, f))

    # datetime.c interval engine (interval_engine_diff target)
    for f in ["ClearPgItmIn", "int64_multiply_add", "AdjustFractMicroseconds",
              "AdjustFractDays", "AdjustFractYears", "AdjustMicroseconds",
              "AdjustDays", "AdjustMonths", "AdjustYears",
              "DecodeTimeForInterval", "DecodeInterval",
              "ParseISO8601Number", "ISO8601IntegerWidth",
              "DecodeISO8601Interval", "AddISO8601IntPart",
              "AddPostgresIntPart", "AddVerboseIntPart", "EncodeInterval"]:
        sec(f"src/backend/utils/adt/datetime.c {f}", extract_fn(dt, f))
    # timestamp.c interval2itm (itm input preparation for EncodeInterval)
    sec("src/backend/utils/adt/timestamp.c interval2itm", extract_fn(ts, "interval2itm"))

    # date.c functions (fmgr wrappers stay verbatim over the shim fmgr.h)
    for f in ["anytime_typmod_check", "date_in", "date_out", "EncodeSpecialDate",
              "make_date", "time_in", "tm2time", "time_overflows",
              "float_time_overflows", "time2tm", "time_out", "make_time",
              "AdjustTimeForTypmod", "time_part_common", "time_part",
              "tm2timetz", "timetz_in", "timetz2tm", "timetz_out"]:
        sec(f"src/backend/utils/adt/date.c {f}", extract_fn(date, f))

    w.close()


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "pg_datetime_verbatim.inc")
