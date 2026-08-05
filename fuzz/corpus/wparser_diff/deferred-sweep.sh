#!/bin/zsh
# Poll deferred candidates every 15 min for up to 4h; act when idle>=3h with proof.
LOG=/private/tmp/claude-501/-Users-malisper-dev-pgrust-fast/2cd0edb9-da0d-4c8a-b94c-d03c38bec772/scratchpad/deferred-sweep.log
ARC=/private/tmp/claude-501/-Users-malisper-dev-pgrust-fast/2cd0edb9-da0d-4c8a-b94c-d03c38bec772/scratchpad/pass3-archive
REMOVE_CAND=(.wt-p1-localtime .wt-p1-refresh4 .wt-evidence-train .wt-claims-evtrain .wt-land-define .wt-symfix)
PURGE_CAND=(.wt-p1-new1 .wt-pgqsort .wt-stubfac2)
deadline=$(( $(date +%s) + 14400 ))
while [ $(date +%s) -lt $deadline ]; do
  remaining=0
  for t in $REMOVE_CAND; do
    wt=/Users/malisper/dev/pgrust-fast/$t
    [ -d "$wt" ] || continue
    cd "$wt" || continue
    u=$(git rev-list HEAD --not --remotes=origin --count 2>/dev/null)
    recent=$(find . -path ./target -prune -o -path ./fuzz/target -prune -o -path ./.git -prune -o -type f -mmin -180 -print 2>/dev/null | head -1)
    # untracked corpus content = hard skip
    corp=$(git status --porcelain 2>/dev/null | grep '^??' | grep 'fuzz/corpus/' | head -1)
    modtracked=$(git status --porcelain 2>/dev/null | grep -v '^ D\|^D \|^??' | head -1)
    untracked=$(git status --porcelain 2>/dev/null | grep '^??' | head -1)
    if [ -n "$corp" ]; then echo "$(date +%H:%M) HARDSKIP-corpus $t $corp" >> $LOG; continue; fi
    if [ -n "$modtracked" ]; then echo "$(date +%H:%M) HARDSKIP-modified $t $modtracked" >> $LOG; continue; fi
    if [ "$u" = "0" ] && [ -z "$recent" ]; then
      if [ -n "$untracked" ]; then
        mkdir -p $ARC/$t
        git status --porcelain | grep '^??' | sed 's|^?? ||' | while read f; do rsync -a --relative "./$f" $ARC/$t/ 2>/dev/null; done
      fi
      cd /Users/malisper/dev/pgrust-fast
      git worktree remove --force "$wt" >/dev/null 2>&1 && echo "$(date +%H:%M) REMOVED $t" >> $LOG || echo "$(date +%H:%M) RMFAIL $t" >> $LOG
    else
      remaining=1
    fi
  done
  for t in $PURGE_CAND; do
    wt=/Users/malisper/dev/pgrust-fast/$t
    [ -d "$wt/target" ] || [ -d "$wt/fuzz/target" ] || continue
    recent=$(find "$wt" -path "$wt/target" -prune -o -path "$wt/fuzz/target" -prune -o -path "$wt/.git" -prune -o -type f -mmin -180 -print 2>/dev/null | head -1)
    if [ -z "$recent" ]; then
      rm -rf "$wt/target" "$wt/fuzz/target" && echo "$(date +%H:%M) PURGED $t" >> $LOG
    else
      remaining=1
    fi
  done
  [ $remaining -eq 0 ] && break
  sleep 900
done
echo "$(date +%H:%M) LOOP-DONE" >> $LOG
df -h / | tail -1 >> $LOG
