#!/usr/bin/env python3
"""mutsweep.py — bulk local re-sweep of cargo-mutants survivors against the
mbconv differential rails (p1-lanez; lanef precedent: rails live in
fuzz/core, invisible to cargo-mutants' crate-local test run).

For each MissedMutant in the fleet audit's outcomes.json: apply the exact
span replacement, run the fast mbconv_diff differential test set (smoke +
exhaustive k1/k2 full domains + bad-args + quoted lattice + utf-engine
invalid-encoding — sub-second runtime; the k1/k2 sweep alone is total over
every 1-2 byte per-char path), record KILLED/SURVIVED, revert. Survivors go
to the escalation list (k3/k4 replay or arid triage).
"""
import json, subprocess, sys, os, time

REPO = subprocess.check_output(['git','rev-parse','--show-toplevel'],text=True).strip()
os.chdir(REPO)
OUT = json.load(open('proofs/coverage/lanez/mutants/outcomes.json'))
missed = [o['scenario']['Mutant'] for o in OUT['outcomes'] if o['summary']=='MissedMutant']
start = int(sys.argv[1]) if len(sys.argv)>1 else 0
end = int(sys.argv[2]) if len(sys.argv)>2 else len(missed)
log = open('proofs/coverage/lanez/mutsweep-results.log','a')

def apply(m):
    f = m['file']
    s, e = m['span']['start'], m['span']['end']
    src = open(f).read().split('\n')
    # span columns are 1-based, end-exclusive per cargo-mutants (proc-macro2 spans)
    sl, sc, el, ec = s['line']-1, s['column']-1, e['line']-1, e['column']-1
    before = '\n'.join(src[:sl]) + ('\n' if sl else '') + src[sl][:sc]
    after = src[el][ec:] + ('\n' if el < len(src)-1 else '') + '\n'.join(src[el+1:])
    open(f,'w').write(before + m['replacement'] + after)

for i, m in enumerate(missed[start:end], start):
    name = m['name']
    apply(m)
    r = subprocess.run(['cargo','test','--release','-q','mbconv_diff::tests','--','--include-ignored','--skip','exhaustive_k3','--skip','exhaustive_k4','--skip','exhaustive_combined'],
                       cwd='fuzz/core', capture_output=True, timeout=1800)
    verdict = 'SURVIVED' if r.returncode == 0 else 'KILLED'
    subprocess.run(['git','checkout','--',m['file']], capture_output=True)
    line = f'{verdict}\t{i}\t{name}'
    print(line, flush=True)
    log.write(line+'\n'); log.flush()
