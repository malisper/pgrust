/* shim: mblen surface — parse-time only in the fmt lanes (UTF8 rule),
 * never inside a timed render loop */
#ifndef FMTV_PG_WCHAR_H
#define FMTV_PG_WCHAR_H
#define PG_UTF8 6
#define MAX_MULTIBYTE_CHAR_LEN 4
extern int	GetDatabaseEncoding(void);
extern int	pg_mblen_cstr(const char *mbstr);
extern int	pg_mblen_range(const char *mbstr, const char *end);
extern int	pg_mbstrlen_range(const char *mbstr, const char *end);
extern int	pg_mbstrlen(const char *mbstr);
#endif
