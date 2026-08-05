/* SHIM mb/pg_wchar.h (tsvec oracle) — NOT PostgreSQL code.
 * Encoding pinned to UTF-8 (see ../postgres.h header comment);
 * implementations in pg_tsvector_core_io.c wrap the VERBATIM
 * pg_utf_mblen from src/common/wchar.c. */
#ifndef PG_DIFFFUZZ_TSVEC_PG_WCHAR_H
#define PG_DIFFFUZZ_TSVEC_PG_WCHAR_H
extern int	pg_mblen_cstr(const char *mbstr);
extern int	pg_mblen_range(const char *mbstr, const char *end);
extern int	pg_database_encoding_max_length(void);
extern int	pg_utf_mblen(const unsigned char *s);
#endif
