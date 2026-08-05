/* shim: str_casefold support (unreachable in the fmt lanes; link stubs abort) */
#ifndef FMTV_UNICODE_CASE_H
#define FMTV_UNICODE_CASE_H
#include <sys/types.h>
typedef size_t (*WordBoundaryNext) (void *wbstate);
extern size_t unicode_strlower(char *dst, size_t dstsize, const char *src,
							   ssize_t srclen, bool full);
extern size_t unicode_strupper(char *dst, size_t dstsize, const char *src,
							   ssize_t srclen, bool full);
extern size_t unicode_strtitle(char *dst, size_t dstsize, const char *src,
							   ssize_t srclen, bool full,
							   WordBoundaryNext wbnext, void *wbstate);
extern size_t unicode_strfold(char *dst, size_t dstsize, const char *src,
							  ssize_t srclen, bool full);
#endif
