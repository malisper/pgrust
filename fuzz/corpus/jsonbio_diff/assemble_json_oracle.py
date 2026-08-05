#!/usr/bin/env python3
"""Assemble fuzz/core/csrc/pg_json_io.c: verbatim extraction from vendored
PG 18.3 json.c / jsonfuncs.c / string.c between hand-written shim prologue
and pg_diff driver epilogue. Extraction = byte-exact function bodies."""
import re, sys, os

V = os.path.expanduser("~/dev/pgrust-fabled/vendor/postgres-src")
JSON_C = open(f"{V}/src/backend/utils/adt/json.c").read().split("\n")
JFUN_C = open(f"{V}/src/backend/utils/adt/jsonfuncs.c").read().split("\n")
STR_C = open(f"{V}/src/common/string.c").read().split("\n")

def extract_fn(lines, name):
    """Extract a C function verbatim: the `name(` line at col 0, plus the
    return-type line(s) above it (and `static` line), through the closing
    `}` at col 0."""
    for i, l in enumerate(lines):
        if re.match(rf"^{re.escape(name)}\(", l):
            start = i
            # include preceding return-type line(s): walk up while previous
            # line is nonempty and not a comment end / brace
            j = i - 1
            while j >= 0 and lines[j].strip() and not lines[j].strip().endswith("*/") and not lines[j].startswith("}"):
                j -= 1
            start = j + 1
            k = i
            while lines[k] != "}":
                k += 1
            return "\n".join(lines[start:k+1])
    raise SystemExit(f"function {name} not found")

def extract_range(lines, a, b):
    return "\n".join(lines[a-1:b])

OUT = []
A = OUT.append

A(open(sys.argv[1]).read())  # prologue (hand-written)

A("\n/* =====================================================================")
A(" * SECTION V1: VERBATIM from src/common/string.c (strtoint)")
A(" * ===================================================================== */\n")
A(extract_fn(STR_C, "strtoint"))

A("\n/* =====================================================================")
A(" * SECTION V2: VERBATIM from src/backend/utils/adt/json.c @ 62d6c7d3df")
A(" * (uniqueness typedefs + machinery, validate/typeof/in, json_object,")
A(" * json_object_two_arg, escape_json family)")
A(" * ===================================================================== */\n")
A(extract_range(JSON_C, 40, 66))   # JsonUnique* typedefs (up to ParsingState end)
for f in ["json_unique_hash", "json_unique_hash_match", "json_unique_check_init",
          "json_unique_check_key",
          "escape_json_char", "escape_json"]:
    A("")
    A(extract_fn(JSON_C, f))
A("\n#define ESCAPE_JSON_FLUSH_AFTER 512\n")
for f in ["escape_json_with_len", "escape_json_text",
          "json_unique_object_start", "json_unique_object_end",
          "json_unique_object_field_start",
          "json_validate", "json_typeof",
          "json_in", "json_object", "json_object_two_arg"]:
    A("")
    A(extract_fn(JSON_C, f))

A("\n/* =====================================================================")
A(" * SECTION V3: VERBATIM from src/backend/utils/adt/jsonfuncs.c @ 62d6c7d3df")
A(" * (errsave plumbing, state typedefs, getters, array_length, strip_nulls)")
A(" * ===================================================================== */\n")
A(extract_range(JFUN_C, 85, 106))   # GetState + AlenState typedefs
A("")
A(extract_range(JFUN_C, 283, 291))  # StripnullState typedef
A("")
for f in ["pg_parse_json_or_errsave", "makeJsonLexContext",
          "json_errsave_error", "report_json_context",
          "get_worker",
          "get_object_start", "get_object_end",
          "get_object_field_start", "get_object_field_end",
          "get_array_start", "get_array_end",
          "get_array_element_start", "get_array_element_end", "get_scalar",
          "get_path_all",
          "json_object_field", "json_object_field_text",
          "json_array_element", "json_array_element_text",
          "json_extract_path", "json_extract_path_text",
          "json_array_length",
          "alen_object_start", "alen_scalar", "alen_array_element_start",
          "json_strip_nulls",
          "sn_object_start", "sn_object_end", "sn_array_start", "sn_array_end",
          "sn_object_field_start", "sn_array_element_start", "sn_scalar"]:
    A("")
    A(extract_fn(JFUN_C, f))

A(open(sys.argv[2]).read())  # epilogue (hand-written drivers)

open(sys.argv[3], "w").write("\n".join(OUT) + "\n")
print(f"wrote {sys.argv[3]}: {sum(len(x)+1 for x in OUT)} bytes")
