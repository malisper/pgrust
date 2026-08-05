/* trgmfam shim postgres.h: minimal c.h surface for the VERBATIM
 * unicode_case.c whole-file vendoring (types + Assert only; no logic). */
#ifndef TRGMFAM_POSTGRES_H
#define TRGMFAM_POSTGRES_H
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <sys/types.h>
typedef int8_t int8;
typedef int16_t int16;
typedef int32_t int32;
typedef int64_t int64;
typedef uint8_t uint8;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;
typedef size_t Size;
#define Assert(x) ((void) 0)
#define pg_attribute_unused()
#define lengthof(array) (sizeof (array) / sizeof ((array)[0]))
/* TU isolation: the one extern this family exports is renamed (linked from
 * pg_trgm_io.c as trgmf_unicode_strlower); the siblings get the prefix too
 * so they cannot collide with any future unicode_case vendoring. */
#define unicode_strlower trgmf_unicode_strlower
#define unicode_strtitle trgmf_unicode_strtitle
#define unicode_strupper trgmf_unicode_strupper
#define unicode_strfold trgmf_unicode_strfold
#define unicode_lowercase_simple trgmf_unicode_lowercase_simple
#define unicode_titlecase_simple trgmf_unicode_titlecase_simple
#define unicode_uppercase_simple trgmf_unicode_uppercase_simple
#define unicode_casefold_simple trgmf_unicode_casefold_simple
#endif
