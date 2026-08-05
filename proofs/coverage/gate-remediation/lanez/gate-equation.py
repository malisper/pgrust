#!/usr/bin/env python3
"""Re-derive the mb/conv gate equation at current origin/main basis.

Inputs: lcov files (named legs). Scope: crates/backend/utils/mb/conv/src minus
maps/ minus tests.rs, v2 SLOC + exclude-const-tables (tree-sloc strict basis,
denominator 1852). Macro attribution applied via proofs/coverage/macro_attrib
so conv_pair!/similar macro-invocation decl lines are credited from FNDA.
"""
import sys, os, glob, json, collections
REPO = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../../..'))
sys.path.insert(0, os.path.join(REPO, 'proofs/coverage'))
from sloc_rules import sloc_lines
import importlib
macro_attrib = importlib.import_module('macro_attrib')

CRATE = 'crates/backend/utils/mb/conv'

def scope_sets():
    scope = {}
    for f in sorted(glob.glob(os.path.join(REPO, CRATE, 'src/**/*.rs'), recursive=True)):
        rel = os.path.relpath(f, REPO)
        if '/maps/' in rel or rel.endswith('tests.rs'):
            continue
        scope[rel] = sloc_lines(f, rule='v2', exclude_const_tables=True)
    return scope

def parse_lcov(path):
    """returns {relfile: set(covered lines)}, {relfile: set(fnda-hit function names)}"""
    da = collections.defaultdict(set)
    fnda = collections.defaultdict(set)
    cur = None
    for line in open(path, errors='replace'):
        line = line.strip()
        if line.startswith('SF:'):
            p = line[3:]
            i = p.find(CRATE)
            cur = p[i:] if i >= 0 else None
        elif cur and line.startswith('DA:'):
            ln, cnt = line[3:].split(',')[:2]
            if int(cnt) > 0:
                da[cur].add(int(ln))
        elif cur and line.startswith('FNDA:'):
            cnt, name = line[5:].split(',', 1)
            if int(cnt) > 0:
                fnda[cur].add(name)
        elif line == 'end_of_record':
            cur = None
    return da, fnda

def macro_credit(scope, fnda_by_file):
    """credit macro-invocation decl lines for generated functions hit (FNDA>0),
    using the repo's macro_attrib index (same rule merge-coverage.py applies)."""
    try:
        idx = macro_attrib.MacroIndex.build(REPO, [CRATE])
    except Exception:
        try:
            idx = macro_attrib.build_index(REPO, [CRATE])
        except Exception as e:
            print('macro_attrib API mismatch:', e); return {}
    credited = collections.defaultdict(set)
    for f, names in fnda_by_file.items():
        for n in names:
            try:
                hits = idx.decl_lines_for(n)
            except Exception:
                hits = []
            for (df, dl) in hits or []:
                rel = os.path.relpath(df, REPO) if os.path.isabs(df) else df
                if rel in scope and dl in scope[rel]:
                    credited[rel].add(dl)
    return credited

def main(argv):
    legs = argv[1:]
    scope = scope_sets()
    denom = sum(len(v) for v in scope.values())
    union = collections.defaultdict(set)
    all_fnda = collections.defaultdict(set)
    for leg in legs:
        da, fnda = parse_lcov(leg)
        n = 0
        for f, lines in da.items():
            if f in scope:
                add = lines & scope[f]
                union[f] |= add
        for f, names in fnda.items():
            all_fnda[f] |= names
        cov = sum(len(da.get(f, set()) & scope[f]) for f in scope)
        print(f'leg {leg}: in-scope covered alone = {cov}')
    mc = macro_credit(scope, all_fnda)
    mc_added = 0
    for f, lines in mc.items():
        new = lines - union[f]
        mc_added += len(new)
        union[f] |= new
    measured = sum(len(union[f]) for f in scope)
    print(f'denominator (v2, no maps/tests, strict): {denom}')
    print(f'macro-attribution credited lines: {mc_added}')
    print(f'MEASURED UNION: {measured} / {denom}')
    out = {'denominator': denom, 'measured': measured, 'macro_credited': mc_added,
           'per_file': {}, 'uncovered': {}}
    for f in sorted(scope):
        out['per_file'][f] = {'sloc': len(scope[f]), 'covered': len(union[f])}
        unc = sorted(scope[f] - union[f])
        if unc:
            out['uncovered'][f] = unc
        print(f'{f}: {len(union[f])} / {len(scope[f])}')
    json.dump(out, open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'union.json'), 'w'), indent=1)
    print('wrote union.json')

if __name__ == '__main__':
    main(sys.argv)
