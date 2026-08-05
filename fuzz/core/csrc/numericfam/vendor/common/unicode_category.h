/* shim: alnum classification (unreachable in the fmt lanes) */
#ifndef FMTV_UNICODE_CATEGORY_H
#define FMTV_UNICODE_CATEGORY_H
typedef uint32 pg_wchar;
extern bool pg_u_isalnum(pg_wchar c, bool posix);
#endif
