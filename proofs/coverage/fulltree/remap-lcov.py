#!/usr/bin/env python3
"""remap-lcov.py OLDSHA NEWSHA IN.lcov OUT.lcov [--repo R]

Remap an lcov file measured at OLDSHA's tree onto NEWSHA's tree, so a
three-axis join can be published at ONE sha without lying at line grain.

Method: per SF file that differs between the shas, build a line map from
`git diff OLDSHA NEWSHA -- file` hunks. A DA line outside every old-side hunk
shifts by the cumulative offset of hunks above it; a DA line INSIDE an
old-side hunk is DROPPED (unmappable — the measured line no longer exists as
written). Unchanged files pass through untouched. Drops are reported per file
on stderr and totalled; the caller must publish the totals.

BRDA/FN records are passed through only for unchanged files; for drifted
files they are dropped along with their mapping ambiguity (the coverage
merge consumes DA only).
"""
import subprocess, sys, re

def build_map(repo, old, new, path):
    """Return (offset_map, dropped_pred). offset at old line L = sum of
    (new_len - old_len) for hunks entirely above L; dropped if L falls inside
    any old hunk span."""
    d = subprocess.run(["git", "-C", repo, "diff", "-U0", old, new, "--", path],
                       capture_output=True, text=True).stdout
    hunks = []  # (old_start, old_len, new_len)
    for m in re.finditer(r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@",
                         d, re.M):
        os_, ol, ns_, nl = (int(m.group(1)), int(m.group(2) or 1),
                            int(m.group(3)), int(m.group(4) or 1))
        hunks.append((os_, ol, nl))
    def remap(line):
        off = 0
        for os_, ol, nl in hunks:
            if ol == 0:
                # pure insertion at os_: lines > os_ shift
                if line > os_:
                    off += nl
                continue
            if os_ <= line < os_ + ol:
                return None  # inside a changed/deleted span
            if line >= os_ + ol:
                off += nl - ol
        return line + off
    return remap

def main():
    old, new, inp, outp = sys.argv[1:5]
    repo = sys.argv[6] if len(sys.argv) > 6 and sys.argv[5] == "--repo" else "."
    changed = set(subprocess.run(
        ["git", "-C", repo, "diff", "--name-only", old, new],
        capture_output=True, text=True).stdout.split())
    out, cur, curmap = [], None, None
    dropped = {}
    kept = {}
    import os as _os
    repo_abs = _os.path.realpath(repo)
    for line in open(inp):
        line = line.rstrip("\n")
        if line.startswith("SF:"):
            f = _os.path.realpath(line[3:])
            # the lcov was captured in ANOTHER worktree of this repo: the
            # repo-relative identity is the part starting at "crates/"
            rel = None
            i = f.find("/crates/")
            if i != -1:
                rel = f[i + 1:]
            cur = rel
            curmap = build_map(repo, old, new, rel) if (rel in changed) else None
            # rewrite SF onto the pinned tree so the consumer maps it
            out.append("SF:" + (_os.path.join(repo_abs, rel) if rel else f))
        elif line.startswith(("DA:", "BRDA:", "FN:", "FNDA:")) and curmap is not None:
            if line.startswith("DA:"):
                ln, rest = line[3:].split(",", 1)
                nl = curmap(int(ln))
                if nl is None:
                    dropped[cur] = dropped.get(cur, 0) + 1
                else:
                    kept[cur] = kept.get(cur, 0) + 1
                    out.append(f"DA:{nl},{rest}")
            # non-DA records for drifted files are dropped silently (unused)
        else:
            out.append(line)
    open(outp, "w").write("\n".join(out) + "\n")
    td = sum(dropped.values())
    print(f"remap {inp}: {len(dropped)} drifted files with drops, "
          f"{td} DA lines dropped, {sum(kept.values())} DA lines remapped",
          file=sys.stderr)
    for f, n in sorted(dropped.items(), key=lambda x: -x[1]):
        print(f"  dropped {n:5d}  {f}", file=sys.stderr)

if __name__ == "__main__":
    main()
