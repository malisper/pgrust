/* SHIM header — NOT PostgreSQL code. See common/unicode_category.h: the
 * BUILTIN strategy is unreachable under the pinned C-ctype default
 * collation; loud unreachable-arm stubs. */
#ifndef UNICODE_CASE_H
#define UNICODE_CASE_H
#include "postgres.h"
static inline pg_wchar unicode_uppercase_simple(pg_wchar code) { abort(); }
static inline pg_wchar unicode_lowercase_simple(pg_wchar code) { abort(); }
#endif
