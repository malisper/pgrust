# print "<rss_kb> <pid> <pid>..." for the full descendant tree of argv[1]
import subprocess, sys
root = sys.argv[1]
ps = subprocess.run(['ps','ax','-o','pid=,ppid=,rss='],capture_output=True,text=True).stdout
kids, rss = {}, {}
for l in ps.strip().splitlines():
    f = l.split()
    if len(f) < 3: continue
    pid, ppid, r = f[0], f[1], f[2]
    kids.setdefault(ppid, []).append(pid); rss[pid] = int(r)
seen, stack = set(), [root]
while stack:
    p = stack.pop()
    if p in seen: continue
    seen.add(p); stack += kids.get(p, [])
print(sum(rss.get(p,0) for p in seen), ' '.join(seen))
