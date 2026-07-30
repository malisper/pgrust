# usage: awk -v fn=NAME -f extract_fn.awk FILE
# prints return-type line (static/inline stripped) + body verbatim
{ lines[NR] = $0 }
END {
  for (i = 1; i <= NR; i++) {
    if (lines[i] ~ ("^" fn "\\(")) {
      rt = lines[i-1]
      sub(/^static[ \t]+/, "", rt)
      sub(/^inline[ \t]+/, "", rt)
      print rt
      for (j = i; j <= NR; j++) {
        print lines[j]
        if (lines[j] ~ /^}/) { print ""; exit }
      }
    }
  }
}
