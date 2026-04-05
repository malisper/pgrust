#!/bin/bash
# Show inclusive and exclusive (self) time per pgrust function from a dtrace profile.
# Usage: bench/analyze_inclusive.sh [profile_file]
# Default: /tmp/pgrust_server_profile.out

FILE="${1:-/tmp/pgrust_server_profile.out}"

python3 -c "
import re

with open('$FILE') as f:
    text = f.read()

stacks = text.strip().split('\n\n')

inclusive = {}
exclusive = {}
total_samples = 0
for stack in stacks:
    lines = [l.strip() for l in stack.strip().split('\n') if l.strip()]
    if not lines:
        continue
    try:
        count = int(lines[-1])
    except ValueError:
        continue
    total_samples += count

    # Inclusive: every pgrust frame in the stack
    seen = set()
    for line in lines[:-1]:
        if 'pgrust::' in line:
            frame = re.sub(r'\+0x[0-9a-f]+', '', line)
            frame = frame.replace('pgrust_server\`', '')
            if frame not in seen:
                seen.add(frame)
                inclusive[frame] = inclusive.get(frame, 0) + count

    # Exclusive: deepest (first) pgrust frame only
    for line in lines[:-1]:
        if 'pgrust::' in line:
            frame = re.sub(r'\+0x[0-9a-f]+', '', line)
            frame = frame.replace('pgrust_server\`', '')
            exclusive[frame] = exclusive.get(frame, 0) + count
            break

print('INCLUSIVE       SELF  FUNCTION')
print('-' * 80)
for func, inc in sorted(inclusive.items(), key=lambda x: -x[1])[:30]:
    exc = exclusive.get(func, 0)
    print(f'{inc*100/total_samples:9.1f}%  {exc*100/total_samples:9.1f}%  {func}')
"
