# Canonicalizer + set-difference for pgo/lint-training-overlap.sh.
# Driven with -v denylist="<newline-separated paths>" -v exemptfile="<path>".
#
# Canonical form: string literals, numbers and every parameter placeholder
# collapse to "@"; harness scaffolding (leading RESET ALL; / SET ...;) is
# stripped; case is folded; punctuation is spaced out and whitespace collapsed.
# What survives is the identifier-and-operator skeleton, so a denied statement
# cannot be smuggled in by changing a search term, a bound, or a binding.

function canon(s,   t) {
  t = s
  gsub(/'([^']|'')*'/, "@", t)            # string literals (before comment cut)
  sub(/--.*$/, "", t)                     # line comment
  t = tolower(t)
  while (match(t, /^[ \t]*reset[ \t]+all[ \t]*;/) || match(t, /^[ \t]*set[ \t][^;]*;/))
    t = substr(t, RSTART + RLENGTH)
  gsub(/\$[0-9]+/, "@", t)                # $1 placeholders
  gsub(/:[a-z_][a-z0-9_]*/, "@", t)       # :name placeholders
  gsub(/\?/, "@", t)                      # ? placeholders
  gsub(/[0-9]+/, "@", t)                  # numeric literals
  gsub(/[(),=<>+*\/;.|-]/, " & ", t)      # space out punctuation
  gsub(/[ \t]+/, " ", t)
  gsub(/^ +| +$/, "", t)
  gsub(/( @)+$/, "", t)                   # trailing normalized ";"
  gsub(/^ +| +$/, "", t)
  return t
}

function isstmt(s) { return (s ~ /[a-z]/) }

BEGIN {
  nd = split(ENVIRON["PGO_DENY_FILES"], df, "\n")
  for (i = 1; i <= nd; i++) {
    if (df[i] == "") continue
    while ((getline line < df[i]) > 0) {
      if (line ~ /^[ \t]*--/ || line ~ /^[ \t]*$/) continue
      c = canon(line)
      if (!isstmt(c)) continue
      if (!(c in DENY)) ndeny++
      DENY[c] = df[i]
    }
    close(df[i])
  }
  ne = 0
  exemptfile = ENVIRON["PGO_EXEMPT_FILE"]
  if (exemptfile != "") {
    while ((getline line < exemptfile) > 0) {
      if (line ~ /^[ \t]*#/ || line ~ /^[ \t]*$/) continue
      c = canon(line)
      if (!isstmt(c)) continue
      EX[c] = 1; ne++
    }
    close(exemptfile)
  }
  if (ndeny == 0) {
    print "pgo-lint: FATAL denylist parsed to 0 statements" > "/dev/stderr"
    exit 2
  }
  hits = 0; ntrain = 0; nexempted = 0; nfiles = 0
}

FNR == 1 { nfiles++ }

{
  if ($0 ~ /^[ \t]*--/ || $0 ~ /^[ \t]*$/ || $0 ~ /^[ \t]*\\/ || $0 ~ /^[ \t]*#/) next
  c = canon($0)
  if (!isstmt(c)) next
  ntrain++
  SEEN[c] = 1
  if (c in DENY) {
    if (c in EX) {
      nexempted++
      printf "pgo-lint: EXEMPT  %s:%d  %s\n", FILENAME, FNR, substr($0, 1, 110)
      next
    }
    hits++
    printf "pgo-lint: OVERLAP %s:%d\n", FILENAME, FNR
    printf "          training : %s\n", substr($0, 1, 180)
    printf "          denied by: %s\n", DENY[c]
    printf "          canonical: %s\n", substr(c, 1, 180)
  }
}

END {
  if (hits > 0) {
    printf "pgo-lint: FAIL - %d training statement(s) are published measurement statements (%d training stmts over %d file(s) vs %d denied forms)\n", hits, ntrain, nfiles, ndeny > "/dev/stderr"
    exit 1
  }
  ndistinct = 0
  for (k in SEEN) ndistinct++
  printf "pgo-lint: PROOF disjoint - %d training statements (%d distinct canonical forms) over %d file(s); 0 of %d denied canonical forms present", ntrain, ndistinct, nfiles, ndeny
  if (nexempted > 0) printf "; %d documented exemption(s) applied", nexempted
  printf "\n"
  exit 0
}
