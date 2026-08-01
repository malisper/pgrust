#!/usr/bin/env python3
"""gen_nodesfam_seeds.py — directed seed corpus for nodesfam_diff.

One seed per Rust-port read label (the 80-label dispatch set), built
mechanically from the GENERATED C readfuncs bodies (field order + macro
kinds from core/csrc/nodesfam/gen/readfuncs.funcs.c and the hand-written
_read* in core/csrc/nodesfam/src/readfuncs.c), so field order can never be
hand-typed wrong. Every emitted seed is VALIDATED against the compiled C
oracle (read -> out -> copy -> equal -> re-read) before it is written;
labels whose skeleton the C oracle rejects are reported for directed
hand-seeding (tests assert census completeness, so a gap is loud).

Usage: gen_nodesfam_seeds.py <c-oracle-binary> [--labels label,...]
Seeds land in corpus/nodesfam_diff/seed-<label> with the arm-0 selector
byte prefixed.
"""
import re
import subprocess
import sys
import pathlib

HERE = pathlib.Path(__file__).resolve().parent
GEN = HERE / "core/csrc/nodesfam/gen"
SRC = HERE / "core/csrc/nodesfam/src"
CORPUS = HERE / "corpus/nodesfam_diff"

# port read labels: parse the Rust dispatch (same regex as the census test)
RUST_READ = (HERE / "../crates/backend/nodes/readfuncs/src/lib.rs").resolve()


def port_labels():
    labs = set()
    for line in RUST_READ.read_text().splitlines():
        m = re.match(r'\s*b"([A-Z_0-9]+)" => self\.read_', line)
        if m:
            labs.add(m.group(1))
    assert len(labs) >= 70, labs
    return labs


def c_read_map():
    """label -> _read fn name from the generated switch."""
    out = {}
    txt = (GEN / "readfuncs.switch.c").read_text()
    for m in re.finditer(r'MATCH\("([A-Z_0-9]+)", \d+\)\)\s*\n\s*return \(Node \*\) (_read\w+)\(\);', txt):
        out[m.group(1)] = m.group(2)
    return out


def read_bodies():
    """fn name -> body text, from generated + hand-written readfuncs."""
    bodies = {}
    for f in (GEN / "readfuncs.funcs.c", SRC / "readfuncs.c"):
        txt = f.read_text()
        for m in re.finditer(r"\n(_read\w+)\(void\)\n\{(.*?)\n\}\n", txt, re.S):
            bodies[m.group(1)] = m.group(2)
    return bodies


DEFAULT = {
    "READ_INT_FIELD": "0",
    "READ_UINT_FIELD": "0",
    "READ_UINT64_FIELD": "0",
    "READ_LONG_FIELD": "0",
    "READ_OID_FIELD": "0",
    "READ_ENUM_FIELD": "0",
    "READ_FLOAT_FIELD": "0",
    "READ_BOOL_FIELD": "false",
    "READ_CHAR_FIELD": "a",
    "READ_STRING_FIELD": "<>",
    "READ_NODE_FIELD": "<>",
    "READ_BITMAPSET_FIELD": "(b)",
    "READ_LOCATION_FIELD": "-1",
}
ARRAY_MACROS = {
    "READ_ATTRNUMBER_ARRAY",
    "READ_OID_ARRAY",
    "READ_INT_ARRAY",
    "READ_BOOL_ARRAY",
}


def skeleton(label, body):
    parts = ["{" + label]
    for m in re.finditer(r"(READ_[A-Z0-9_]+)\((\w+)(?:\s*,\s*(\w+))?\)", body):
        macro, field = m.group(1), m.group(2)
        if macro in ("READ_TEMP_LOCALS", "READ_LOCALS"):
            continue
        if macro in DEFAULT:
            parts.append(f":{field} {DEFAULT[macro]}")
        elif macro in ARRAY_MACROS:
            # count field defaulted to 0 above -> zero items follow
            parts.append(f":{field}")
        else:
            return None  # custom macro: needs a hand seed
    parts.append("}")
    return " ".join(parts)


def validate(oracle, text):
    r = subprocess.run([oracle, text], capture_output=True, text=True)
    return r.returncode == 0 and "EQ 1 REREAD 1 SAME 1" in r.stdout


def main():
    oracle = sys.argv[1]
    only = None
    if len(sys.argv) > 3 and sys.argv[2] == "--labels":
        only = set(sys.argv[3].split(","))
    CORPUS.mkdir(parents=True, exist_ok=True)
    labels = port_labels()
    cmap = c_read_map()
    bodies = read_bodies()
    missing, invalid, ok = [], [], []
    for label in sorted(labels):
        if only and label not in only:
            continue
        fn = cmap.get(label)
        body = bodies.get(fn) if fn else None
        text = skeleton(label, body) if body else None
        if text is None:
            missing.append(label)
            continue
        if not validate(oracle, text):
            invalid.append((label, text))
            continue
        (CORPUS / f"seed-{label.lower()}").write_bytes(b"\x00" + text.encode())
        ok.append(label)
    print(f"ok {len(ok)}; custom-macro (hand seed needed): {missing}")
    for label, text in invalid:
        print(f"C-REJECTED {label}: {text}")


if __name__ == "__main__":
    main()
