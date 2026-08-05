#!/usr/bin/env python3
"""check-suite-names.py — validate SUITE.tsv harness names against ground truth.

Ground truth is `cargo kani list` run in each family's harness crate: every
SUITE.tsv row's harness name must resolve against the real harnesses the
crate exposes, so manifest names cannot rot silently when harnesses are
renamed in source (the 2026-07-30 repair fixed 104 rows that had).

Row rules (in order):
  1. Non-kani rows — flags without a `-Z` token (e.g. `cargo run --release
     --bin X`, `(run-cmp.sh)`): if the flags name a `--bin X`, the file
     src/bin/X.rs must exist in the family crate; otherwise the row is
     skipped with a note. These rows never appear in `cargo kani list`.
  2. expected == "missing" — the row documents a harness that was never
     landed or was superseded (adjudicated 2026-07-30). Its name must NOT
     resolve; if it starts resolving, that is an ERROR ("marked missing but
     harness exists — re-adjudicate the row").
  3. Rows whose flags contain `--exact`: kani's --exact matches only the
     EXACT FULLY QUALIFIED harness name (e.g. `proofs::eq_x`, not `eq_x`),
     so the name must appear verbatim in the crate's kani list output.
  4. All other rows: kani --harness substring-matches, so the name must be
     a substring of at least one listed harness. A name matching MORE than
     one harness is a WARNING (the row runs several harnesses in one
     invocation and run-suite.sh scores only the last verdict).

Known toolchain wart handled here: crates that carry kani flags in
[package.metadata.kani.flags] (misc-ops, strtoint) misroute the `list`
subcommand into a full verification run on cargo-kani 0.67. For those, the
metadata flags section is stripped from a temporary copy of Cargo.toml for
the duration of the list call and always restored (flags are irrelevant to
enumeration — `list` only scans for #[kani::proof]).

Census discipline (2026-07-30 hardening): every considered row gets exactly
one disposition — checked, skipped (with a printed reason), or errored — and
the run FAILS if `checked + skipped + errored != considered rows`. A family
whose `cargo kani list` fails no longer silently removes its rows from the
denominator (that hole made the same sha report "2090 rows OK" and "2184 rows
OK" on different runs, a 94-row gap = one family's whole row set); the list
call is retried with backoff first, and any family still unlisted triggers a
prominent INCOMPLETE CENSUS banner. Never print a bare "N rows OK".

This checker validates harness NAMES only. Row SHAPE (column count, expected/
tier vocabulary, unexpanded `<...>` placeholders, prose in the flags column,
packed/shuffled columns) is validated offline in seconds by the companion
lint-suite-rows.py — run that first; it needs no cargo and no solver.

Usage:
  ./check-suite-names.py                 # all families (compiles each crate)
  ./check-suite-names.py mbconv geo-cmp  # only these families
  ./check-suite-names.py --cache DIR     # reuse/populate DIR/<family>.json

Exit nonzero on any ERROR (unresolvable name, missing-marked name that now
resolves, kani list failure, missing native bin) or on a census mismatch.
"""

import atexit
import collections
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import time

PROOFS_DIR = os.path.dirname(os.path.abspath(__file__))
SUITE_TSV = os.path.join(PROOFS_DIR, "SUITE.tsv")


# Manifests currently stripped of [package.metadata.kani.flags] as (path,
# backup) pairs. Restored by the normal `finally`, and by an atexit/signal
# handler if the run is interrupted — an abandoned stripped manifest silently
# changes a family's proof recipe.
PENDING_RESTORES = set()


def restore_pending(*_args):
    for manifest, backup in list(PENDING_RESTORES):
        if os.path.exists(backup):
            shutil.move(backup, manifest)
            print(f"restored {manifest} (interrupted)", file=sys.stderr)
    PENDING_RESTORES.clear()


atexit.register(restore_pending)
for _sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
    signal.signal(_sig, lambda s, f: sys.exit(128 + s))


LIST_ATTEMPTS = 3
LIST_BACKOFF_S = 20  # doubles per retry


def kani_list_retrying(family, cache_dir=None, attempts=LIST_ATTEMPTS):
    """kani_list with retries: `cargo kani list` failures are usually a
    transient build-lock/contention artifact (two families' `cargo` invocations
    racing the same target dir), not a real name defect. An unretried failure
    dropped a whole family's rows out of the census, which is how the same sha
    reported 2090 and 2184 "rows OK" on different runs (2026-07-30)."""
    delay = LIST_BACKOFF_S
    last = None
    for attempt in range(1, attempts + 1):
        try:
            return kani_list(family, cache_dir)
        except Exception as e:
            last = e
            if attempt < attempts:
                print(f"RETRY {family}: kani list attempt {attempt}/{attempts} "
                      f"failed ({str(e).splitlines()[0][:120]}); "
                      f"retrying in {delay}s", flush=True)
                time.sleep(delay)
                delay *= 2
    raise RuntimeError(f"kani list failed {attempts}/{attempts} attempts: "
                       f"{last}")


def kani_list(family, cache_dir=None):
    """Return the set of fully qualified harness names for a family crate."""
    if cache_dir:
        cached = os.path.join(cache_dir, family + ".json")
        if os.path.exists(cached):
            return load_names(cached)

    crate = os.path.join(PROOFS_DIR, family)
    manifest = os.path.join(crate, "Cargo.toml")
    out_json = os.path.join(crate, "kani-list.json")
    backup = None

    def restore():
        # The stripped manifest must never outlive this call: a leftover
        # stripped Cargo.toml silently drops the family's c-lib flags, which
        # is the same manifest-rot class this checker exists to catch. The
        # finally block covers exceptions; PENDING_RESTORES covers signals.
        if backup and os.path.exists(backup):
            shutil.move(backup, manifest)
        PENDING_RESTORES.discard(token)

    token = (manifest, manifest + ".check-names.bak")

    with open(manifest) as f:
        toml = f.read()
    if "[package.metadata.kani.flags]" in toml:
        # cargo-kani 0.67: metadata flags misroute `list` into verification.
        backup = manifest + ".check-names.bak"
        shutil.copy2(manifest, backup)
        PENDING_RESTORES.add(token)
        stripped = re.sub(
            r"\[package\.metadata\.kani\.flags\][^\[]*", "", toml
        )
        with open(manifest, "w") as f:
            f.write(stripped)

    try:
        if os.path.exists(out_json):
            os.remove(out_json)
        proc = subprocess.run(
            ["cargo", "kani", "list", "-Z", "stubbing", "-Z", "c-ffi",
             "--format", "json", "-q"],
            cwd=crate, capture_output=True, text=True, timeout=900,
        )
        if proc.returncode != 0 or not os.path.exists(out_json):
            raise RuntimeError(
                f"cargo kani list failed for {family} "
                f"(rc={proc.returncode}):\n{proc.stderr[-2000:]}"
            )
        if cache_dir:
            os.makedirs(cache_dir, exist_ok=True)
            shutil.copy2(out_json, os.path.join(cache_dir, family + ".json"))
        names = load_names(out_json)
        os.remove(out_json)  # don't leave build artifacts in the crate
        return names
    finally:
        restore()


def load_names(path):
    with open(path) as f:
        d = json.load(f)
    names = set()
    for sec in ("standard-harnesses", "contract-harnesses"):
        for _file, harnesses in d.get(sec, {}).items():
            names.update(harnesses)
    return names


def main():
    args = sys.argv[1:]
    cache_dir = None
    if "--cache" in args:
        i = args.index("--cache")
        cache_dir = args[i + 1]
        del args[i:i + 2]
    only = set(args)

    rows = []
    with open(SUITE_TSV) as f:
        header = f.readline()
        assert header.startswith("family\tharness\t"), "SUITE.tsv header moved"
        for lineno, line in enumerate(f, 2):
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 5 or not parts[0] or parts[0].startswith("#"):
                continue
            rows.append((lineno, parts))

    families = sorted({p[0] for _, p in rows})
    if only:
        unknown = only - set(families)
        if unknown:
            sys.exit(f"unknown families: {sorted(unknown)}")
        families = sorted(only)

    errors, warnings = [], []
    # Census: every considered row gets EXACTLY ONE disposition — checked,
    # skipped (with a reason), or errored. checked+skipped+errored must equal
    # the number of considered rows, or the printed count is not a census.
    checked = 0
    skipped = 0
    skip_reasons = collections.Counter()
    errored_rows = 0

    def skip(reason):
        nonlocal skipped
        skipped += 1
        skip_reasons[reason] += 1

    listed = {}
    unlisted_families = []
    for fam in families:
        try:
            listed[fam] = kani_list_retrying(fam, cache_dir)
        except Exception as e:  # compile break, kani missing, etc.
            errors.append(f"{fam}: {e}")
            listed[fam] = None
            unlisted_families.append(fam)

    considered = [(lineno, parts) for lineno, parts in rows
                  if not only or parts[0] in only]

    for lineno, parts in considered:
        fam, harness, flags, expected = parts[0], parts[1], parts[2], parts[3]
        names = listed.get(fam)

        # Rule 1: non-kani rows (native differential bins, family scripts).
        if "-Z" not in flags:
            m = re.search(r"--bin\s+(\S+)", flags)
            if m:
                bin_rs = os.path.join(PROOFS_DIR, fam, "src", "bin",
                                      m.group(1) + ".rs")
                if os.path.exists(bin_rs):
                    checked += 1
                else:
                    errored_rows += 1
                    errors.append(
                        f"line {lineno} {fam}/{harness}: native bin "
                        f"src/bin/{m.group(1)}.rs not found")
            else:
                skip("non-kani row with no --bin to verify "
                     "(family script recipe)")
            continue

        if names is None:
            # Family list failed (already an ERROR): this row is NOT checked.
            # Counting it as skipped keeps the census honest — a shrinking
            # denominator must be visible, never silent.
            skip(f"family `{fam}` could not be listed — row UNVERIFIED")
            continue

        # Rule 2: adjudicated-missing rows must stay missing, judged under
        # the row's own matching mode (--exact = exact FQN; else substring).
        if expected == "missing":
            if "--exact" in flags:
                resolves = harness in names
            else:
                resolves = harness in names or any(harness in n for n in names)
            if resolves:
                errored_rows += 1
                errors.append(
                    f"line {lineno} {fam}/{harness}: marked missing but the "
                    f"harness now exists — re-adjudicate the row")
            else:
                checked += 1
            continue

        # Rule 3: --exact rows need the exact fully qualified name.
        if "--exact" in flags:
            if harness in names:
                checked += 1
            else:
                cands = [n for n in names if n.split("::")[-1] == harness]
                hint = f" (did you mean {cands[0]}?)" if len(cands) == 1 else ""
                errored_rows += 1
                errors.append(
                    f"line {lineno} {fam}/{harness}: --exact row but this is "
                    f"not an exact fully qualified harness name{hint}")
            continue

        # Rule 4: substring rows must match at least one harness.
        matches = [n for n in names if harness in n]
        if not matches:
            errored_rows += 1
            errors.append(
                f"line {lineno} {fam}/{harness}: matches no harness in "
                f"cargo kani list")
        else:
            checked += 1
            if len(matches) > 1:
                warnings.append(
                    f"line {lineno} {fam}/{harness}: substring-matches "
                    f"{len(matches)} harnesses {sorted(matches)[:5]} — one "
                    f"invocation runs them all; consider FQN + --exact")

    for w in warnings:
        print(f"WARN  {w}")
    for e in errors:
        print(f"ERROR {e}")

    # ---- census -----------------------------------------------------------
    # The count is only meaningful if every considered row is accounted for.
    total = len(considered)
    census_ok = (checked + skipped + errored_rows) == total
    print(f"\n== check-suite-names census "
          f"({len(families)} families{' (filtered)' if only else ''}) ==")
    print(f"  SUITE.tsv data rows considered: {total}")
    print(f"  checked (name resolved):        {checked}")
    print(f"  skipped (UNVERIFIED):          {skipped}")
    for reason, n in skip_reasons.most_common():
        print(f"      {n:5d}  {reason}")
    print(f"  rows with errors:              {errored_rows}")
    print(f"  errors (incl. per-family):     {len(errors)}")
    print(f"  warnings:                      {len(warnings)}")

    if not census_ok:
        print(f"\nCENSUS FAIL: checked({checked}) + skipped({skipped}) + "
              f"errored({errored_rows}) = "
              f"{checked + skipped + errored_rows} != {total} considered "
              f"rows — the checker lost rows; the count above is not a "
              f"census and this run certifies nothing.")
    if unlisted_families:
        pct = 100.0 * checked / total if total else 0.0
        print(f"\nINCOMPLETE CENSUS: {len(unlisted_families)} of "
              f"{len(families)} families could not be listed "
              f"({', '.join(unlisted_families)}). Their rows are UNVERIFIED, "
              f"not OK. Only {checked}/{total} rows ({pct:.1f}%) were "
              f"actually checked — do NOT read this run as a green.")

    verdict_fail = bool(errors) or not census_ok
    print(f"\ncheck-suite-names: {'FAIL' if verdict_fail else 'PASS'} "
          f"({checked}/{total} rows verified"
          f"{f', {skipped} UNVERIFIED' if skipped else ''})")
    sys.exit(1 if verdict_fail else 0)


if __name__ == "__main__":
    main()
