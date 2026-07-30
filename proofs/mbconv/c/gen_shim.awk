# usage: awk -v fn=NAME -f gen_shim.awk FILE
# Emits a plain-C shim for conversion proc NAME: fmgr unwrapping replaced by
# scalar args (allowed shim), CHECK_ENCODING_CONVERSION_ARGS dropped (checked
# by a dedicated harness; harnesses pin the expected encodings), body's
# "converted = ...;" statement copied VERBATIM.
{ lines[NR] = $0 }
END {
  for (i = 1; i <= NR; i++) {
    if (lines[i] ~ ("^" fn "\\(PG_FUNCTION_ARGS\\)")) {
      print "int"
      print "pg_" fn "(const unsigned char *src, unsigned char *dest, int len, bool noError)"
      print "{"
      print "	int			converted;"
      print ""
      print "	pg_mbconv_err = 0;"
      for (j = i; j <= NR; j++) {
        if (lines[j] ~ /converted = /) {
          for (k = j; k <= NR; k++) { print lines[k]; if (lines[k] ~ /;[ \t]*$/) break }
          break
        }
        if (lines[j] ~ /^}/) break
      }
      print "	return converted;"
      print "}"
      print ""
      break
    }
  }
}
