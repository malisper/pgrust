#!/usr/bin/env python3
"""fill-overrides.py — statically resolve module qualification for unqualified
SUITE harness names into joblist column 3, so the capture does not depend on
`cargo kani list` preflight (which can time out on a contended host and leave
"Failed to match" failures with no candidates).

Resolution: scan proofs/<family>/src/**/*.rs tracking `mod NAME {` brace
nesting; when `fn <harness>(` with a #[kani::proof]-style attribute context is
found, emit the module path. Only fills column 3 when exactly one declaration
site resolves; leaves it empty otherwise (runner default / preflight handles).
"""
import os, re, sys

HERE = os.path.dirname(os.path.abspath(__file__))
PROOFS = os.path.normpath(os.path.join(HERE, "..", ".."))

def harness_paths(fam):
    """{name: set(modpaths)} for fn definitions in the family crate."""
    out = {}
    src = os.path.join(PROOFS, fam, "src")
    for root, _, names in os.walk(src):
        for nm in names:
            if not nm.endswith(".rs"):
                continue
            # file-module prefix relative to src/
            rel = os.path.relpath(os.path.join(root, nm), src)
            parts = rel[:-3].split(os.sep)
            if parts[-1] in ("lib", "main", "mod"):
                parts = parts[:-1]
            stack = []  # (modname, depth_at_open)
            depth = 0
            for line in open(os.path.join(root, nm), errors="replace"):
                m = re.match(r"\s*(?:pub\s+)?mod\s+([A-Za-z_][A-Za-z0-9_]*)\s*\{", line)
                if m:
                    stack.append((m.group(1), depth))
                fnm = re.match(r"\s*(?:pub\s+)?fn\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(", line)
                if fnm:
                    mods = parts + [s[0] for s in stack]
                    out.setdefault(fnm.group(1), set()).add("::".join(mods))
                depth += line.count("{") - line.count("}")
                while stack and depth <= stack[-1][1]:
                    stack.pop()
            # note: brace counting is heuristic; strings/comments may skew it,
            # acceptable because ambiguity => leave blank
    return out

def main():
    done = set()
    dn = os.path.join(HERE, "logs", "families.done")
    if os.path.exists(dn):
        done = {l.strip() for l in open(dn) if l.strip()}
    for jf in sorted(os.listdir(os.path.join(HERE, "joblists"))):
        fam = jf[:-4]
        if fam in done or not jf.endswith(".tsv"):
            continue
        rows = [l.rstrip("\n").split("\t") for l in open(os.path.join(HERE, "joblists", jf))]
        if all(("::" in r[1]) or (len(r) > 2 and r[2]) for r in rows if r and r[0]):
            continue
        hp = harness_paths(fam)
        changed = 0
        for r in rows:
            if not r or not r[0] or "::" in r[1] or (len(r) > 2 and r[2]):
                continue
            sites = hp.get(r[1], set())
            if len(sites) == 1:
                mod = next(iter(sites))
                while len(r) < 3:
                    r.append("")
                r[2] = (mod + "::" + r[1]) if mod else r[1]
                changed += 1
        if changed:
            with open(os.path.join(HERE, "joblists", jf), "w") as f:
                for r in rows:
                    f.write("\t".join(r) + "\n")
            print(f"{fam}: {changed} overrides filled")

if __name__ == "__main__":
    main()
