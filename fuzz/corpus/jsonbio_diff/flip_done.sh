#!/bin/bash
set -e
WT=/tmp/claims-laneab-done
git -C /Users/malisper/dev/pgrust-fast worktree remove $WT --force 2>/dev/null || true
git -C /Users/malisper/dev/pgrust-fast fetch origin main -q
git -C /Users/malisper/dev/pgrust-fast worktree add $WT origin/main --detach -q
cd $WT
NOTE='p1-laneab DONE 2026-07-31. GATE: 2173/2173 in-scope v2-SLOC ACCOUNTED (1176 fuzz-measured green + 997 recorded exception rows in proofs/coverage/phase1-exceptions.tsv: 802 excluded-state carves [aggs/srfs/tojson catalog+fmgr], 101 cross-crate-entry [json_recv wire, json_encode_datetime dispatch — RE-OPEN jsondt_diff when a datetime lane vendors the C encode chain], 26 proof-covered-unmeasured [oids 322/324/3199/3201 join at fulltree kani capture], 25 const-eval-only [JSON_BUILTINS table], 21 unreachable-arm, 10 instrument-unmappable [NO-DA verified], 7 encoding-carve [UTF8 pin], 5 defensive-c-parity). 2 fleet campaigns x 10M execs 0 divergences 0 sanitizer artifacts (jobs pgrust-fuzz-campaign-1785508521-0065-90755 @ ff70d8f172, CONFIRM pgrust-fuzz-campaign-1785509922-6e9e-70975 @ c4f6a5c323); a0 EXHAUSTIVE-DIFF 67,174,400-case unicode-escape domain PASS (83s laptop release, banked fuzz/campaigns/2026-07-31-laneab-exhaustive-unicode.md). Target json_diff: 14 arms, planes value+verdict+sqlstate+SQL-NULL+fc-wrapper+soft-error(ErrorSaveNode); oracle = whole-TU verbatim 18.3 common/jsonapi.c+stringinfo.c (csrc/jsonfam, pg_jsonfam_ prefix) + json.c/jsonfuncs.c extraction (pg_json_io.c); UTF8 pin; SQL-reachability gate for json-typed args (lone-surrogate/u0000 de-escape errors kept in-domain). Replay rail GREEN over 5683 committed inputs. 0 divergences to triage. Work on proofs/p1-laneab. mutants-audit pending job pgrust-mutants-audit-1785509900-0d7b-67211'
for i in 1 2 3 4 5; do
  git fetch origin main -q && git reset --hard origin/main -q
  python3 - "$NOTE" <<'PY'
import sys
note=sys.argv[1]
out=[]
for line in open('docs/verification/phase1-claims.tsv'):
    p=line.rstrip('\n').split('\t')
    if p[0]=='adt/json' and len(p)>=5 and p[1]=='p1-laneab':
        p[3]=note
        p[4]='done'
    out.append('\t'.join(p))
open('docs/verification/phase1-claims.tsv','w').write('\n'.join(out)+'\n')
PY
  git add docs/verification/phase1-claims.tsv
  git commit -q -m "claims: p1-laneab adt/json -> done (2173/2173 accounted; 20M fleet execs + 67.2M exhaustive, 0 divergences)"
  if git push origin HEAD:main 2>&1 | grep -q "HEAD -> main"; then echo DONE-PUSHED; break; fi
  echo retrying
done
cd /
git -C /Users/malisper/dev/pgrust-fast worktree remove $WT --force
