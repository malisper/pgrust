#!/usr/bin/env python3
"""Mechanical verbatim extractor for oracle TUs (jsonpath_diff lane).

Given a C source file and a list of items, emit each item VERBATIM with a
provenance marker line recording the source file and 1-based line range.
Items:
  lines:A-B          -- raw line range
  fn:NAME            -- function definition: walks back from the line where
                        NAME( starts at column 0 to include the return-type
                        line and the contiguous comment block above it; ends
                        at the first '}' at column 0.
Usage: extract_verbatim.py <src> <item> [<item>...]
Output on stdout.
"""
import sys, re

def main():
    src = sys.argv[1]
    items = sys.argv[2:]
    lines = open(src).read().split("\n")
    n = len(lines)
    out = []
    for item in items:
        if item.startswith("lines:"):
            a, b = item[6:].split("-")
            a, b = int(a), int(b)
            out.append(f"/* ---- {src.split('/')[-1]}:{a}-{b} VERBATIM ---- */")
            out.extend(lines[a-1:b])
            out.append("")
            continue
        assert item.startswith("fn:")
        name = item[3:]
        # find definition: NAME( at column 0
        defline = None
        pat = re.compile(r"^" + re.escape(name) + r"\(")
        for i, l in enumerate(lines):
            if pat.match(l):
                defline = i
                break
        if defline is None:
            sys.exit(f"function {name} not found in {src}")
        # walk back over return-type line(s): lines until a blank or '*/' end
        start = defline
        while start > 0 and lines[start-1].strip() != "" and not lines[start-1].rstrip().endswith("*/"):
            start -= 1
        # include contiguous comment block above
        cstart = start
        if cstart > 0 and lines[cstart-1].rstrip().endswith("*/"):
            j = cstart - 1
            while j >= 0:
                if lines[j].lstrip().startswith("/*"):
                    cstart = j
                    break
                j -= 1
        # find end: first '}' at column 0 after defline
        end = None
        for i in range(defline, n):
            if lines[i] == "}":
                end = i
                break
        assert end is not None, name
        out.append(f"/* ---- {src.split('/')[-1]}:{cstart+1}-{end+1} VERBATIM ({name}) ---- */")
        out.extend(lines[cstart:end+1])
        out.append("")
    sys.stdout.write("\n".join(out))

main()
