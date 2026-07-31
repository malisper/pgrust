import sys
def extract(path, names):
    lines = open(path).read().split('\n')
    out = []
    for name in names:
        # find "^name(" line
        idx = None
        for i, l in enumerate(lines):
            if l.startswith(name + '(') :
                idx = i; break
        if idx is None:
            print(f'MISSING {name}', file=sys.stderr); continue
        # walk back over return type + comment block
        start = idx - 1  # return type line
        j = start - 1
        # include preceding comment block
        if lines[j].strip() == '*/' or lines[j].strip().startswith('*/'):
            while j >= 0 and not lines[j].lstrip().startswith('/*'):
                j -= 1
            start_c = j
        else:
            start_c = start
        # find end: first line that is exactly "}"
        k = idx
        while lines[k] != '}':
            k += 1
        out.append('\n'.join(lines[start_c:k+1]))
    return '\n\n'.join(out)

path = sys.argv[1]
names = sys.argv[2].split(',')
print(extract(path, names))
