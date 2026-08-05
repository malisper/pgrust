#!/usr/bin/env python3
"""Falsification pass over the residual classification (lane p1-pgcryptofam-resid).

The 29 adversarial probes exist to REFUTE the zero-SEED-GAP claim: each is
the strongest input the driver encoding can express toward a residual line
classified unreachable/defensive/carved. This script replays their coverage
(probes.lcov, exported from the cargo-fuzz coverage profdata) against the
classification and reports:

  REFUTED       — a residual line a probe covered => misclassified, it is a
                  real seed gap; the probe must be promoted into the corpus
                  and the ledger row corrected.
  guard-witness — the DA counts of the guard/neighbor lines around each
                  probed residual line, proving the probe actually executed
                  the surrounding code rather than dying early.

Exit nonzero if anything is REFUTED.
"""
import os, sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
sys.path.insert(0, os.path.join(REPO, "proofs", "coverage"))
import sloc_rules, test_scope  # noqa: E402

RECORD_LCOV = sys.argv[1]  # lcov of record (fleet floor)
PROBE_LCOV = os.path.join(HERE, "probes.lcov")
CRATE = "crates/contrib/pgcrypto"
IN_SCOPE = ["src/crypt.rs", "src/crypt/bcrypt.rs", "src/crypt/cryptdes.rs",
            "src/crypt/desc.rs", "src/crypt/shacrypt.rs", "src/hashing.rs",
            "src/lib.rs", "src/pgp/armor.rs"]

# guard/neighbor lines to witness per probed residual line: resid -> guards
GUARDS = {
    ("src/crypt.rs", 76): [73, 77, 78],       # via gensalt-des-rounds25
    ("src/crypt.rs", 92): [88, 91, 94],       # via gensalt-xdes-rounds0
    ("src/crypt.rs", 120): [119, 122],        # via gensalt-bf-rounds0
    ("src/crypt.rs", 123): [122, 125],
    ("src/crypt.rs", 138): [137, 140],        # via gensalt-sha256-rounds0
    ("src/crypt.rs", 208): [206, 207, 212],
    ("src/crypt/bcrypt.rs", 40): [38, 39, 42],   # via crypt-bf-cost04
    ("src/crypt/cryptdes.rs", 206): [204, 205, 208],
    ("src/crypt/cryptdes.rs", 388): [385, 386],
    ("src/crypt/cryptdes.rs", 579): [577, 578, 581],  # via crypt-xdes-count255
    ("src/crypt/cryptdes.rs", 593): [588, 590, 591],  # via crypt-trad-2char-salt
    ("src/crypt/shacrypt.rs", 116): [111, 112, 118],  # via crypt-sha-multibyte-salt
    ("src/crypt/shacrypt.rs", 138): [136, 137, 144],  # rounds-1e9 is cost-skipped
    ("src/crypt/shacrypt.rs", 183): [175, 181, 185],  # via crypt-sha-rounds-1000
    ("src/crypt/shacrypt.rs", 361): [357, 360, 362],
    ("src/crypt/shacrypt.rs", 462): [459, 460],       # sha-no-rounds is cost-skipped
    ("src/lib.rs", 325): [318, 323, 327],     # via armor-no-headers/two-headers
    ("src/lib.rs", 410): [408, 409, 415],
    ("src/lib.rs", 425): [424, 431],
    ("src/lib.rs", 437): [436, 442],
    ("src/lib.rs", 465): [464, 470],
    ("src/pgp/armor.rs", 159): [144, 145, 152],   # via dearmor-no-marker/-dash-tail
    ("src/pgp/armor.rs", 209): [207, 208, 211],   # via dearmor-valid etc.
    ("src/pgp/armor.rs", 215): [213, 214, 217],
    ("src/pgp/armor.rs", 221): [219, 220, 222],
    ("src/pgp/armor.rs", 243): [242, 245],        # via dearmor-late-crc
}


def parse(path, strip_prefix=None):
    da, cur = {}, None
    for raw in open(path, encoding="utf-8", errors="replace"):
        raw = raw.strip()
        if raw.startswith("SF:"):
            p = raw[3:]
            if strip_prefix and p.startswith(strip_prefix):
                p = p[len(strip_prefix):]
            cur = p if p.startswith(CRATE) else None
            if cur is not None:
                da.setdefault(cur, {})
        elif cur and raw.startswith("DA:"):
            ln, cnt = raw[3:].split(",")[:2]
            d = da[cur]
            d[int(ln)] = max(d.get(int(ln), 0), int(cnt))
        elif raw == "end_of_record":
            cur = None
    return da


def main():
    record = parse(RECORD_LCOV)
    probes = parse(PROBE_LCOV, strip_prefix=REPO + "/")

    test_scope.set_repo_root(REPO)
    ts = test_scope.scope_for_crate(CRATE)

    refuted = []
    print("=== residual lines vs 29-probe coverage ===")
    for f in IN_SCOPE:
        rel = f"{CRATE}/{f}"
        assert not ts.is_test_file(rel)
        lines = sloc_rules.sloc_lines(os.path.join(REPO, rel))
        drec = record.get(rel, {})
        dpro = probes.get(rel, {})
        resid = sorted(ln for ln in lines if drec.get(ln, 0) == 0)
        for ln in resid:
            hits = dpro.get(ln, 0)
            if hits > 0:
                refuted.append((f, ln, hits))
                print(f"  REFUTED {f}:{ln}  probe DA={hits}  "
                      f"(classified exception, but a probe reached it)")
    if not refuted:
        print("  (none) — no probe covered any of the 248 residual lines")

    print("\n=== guard witnesses (probe DA on the lines AROUND each probed "
          "residual line; residual line itself shown as [x]) ===")
    for (f, ln), guards in sorted(GUARDS.items()):
        rel = f"{CRATE}/{f}"
        dpro = probes.get(rel, {})
        gtxt = " ".join(f"{g}:{dpro.get(g, 'noDA')}" for g in guards)
        print(f"  {f}:{ln} [{dpro.get(ln, 'noDA')}]  guards {gtxt}")

    print(f"\nREFUTED lines: {len(refuted)}")
    sys.exit(1 if refuted else 0)


if __name__ == "__main__":
    main()
