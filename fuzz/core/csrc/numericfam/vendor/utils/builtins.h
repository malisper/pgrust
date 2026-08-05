/* shim: fcinfo-style externs the formatting TU calls through
 * DirectFunctionCall; numeric_* resolve in the vendored numeric TU,
 * int4out/int8out/int8mul/dtoi8 are verbatim lifts in fmt_bench.c */
#ifndef FMTV_BUILTINS_H
#define FMTV_BUILTINS_H
extern Datum numeric_round(PG_FUNCTION_ARGS);
extern Datum numeric_power(PG_FUNCTION_ARGS);
extern Datum numeric_mul(PG_FUNCTION_ARGS);
extern Datum numeric_in(PG_FUNCTION_ARGS);
extern Datum int4out(PG_FUNCTION_ARGS);
extern Datum int8out(PG_FUNCTION_ARGS);
extern Datum int8mul(PG_FUNCTION_ARGS);
extern Datum dtoi8(PG_FUNCTION_ARGS);
extern char *text_to_cstring(const text *t);
extern text *cstring_to_text(const char *s);
extern text *cstring_to_text_with_len(const char *s, int len);
extern char *psprintf(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
extern char *pnstrdup(const char *in, Size len);
#ifndef strlcpy
extern size_t strlcpy(char *dst, const char *src, size_t siz);
#endif
extern unsigned char pg_toupper(unsigned char ch);
extern unsigned char pg_tolower(unsigned char ch);
extern unsigned char pg_ascii_toupper(unsigned char ch);
extern unsigned char pg_ascii_tolower(unsigned char ch);
#define MemSet(start, val, len) memset(start, val, len)
#endif
