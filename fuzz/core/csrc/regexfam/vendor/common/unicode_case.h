/* BUILTIN-strategy case mapping: unreachable under C collation; stub aborts. */
#ifndef CREF_REGEX_UNICODE_CASE_H
#define CREF_REGEX_UNICODE_CASE_H

static inline pg_wchar unicode_lowercase_simple(pg_wchar code) { abort(); }
static inline pg_wchar unicode_uppercase_simple(pg_wchar code) { abort(); }

#endif
