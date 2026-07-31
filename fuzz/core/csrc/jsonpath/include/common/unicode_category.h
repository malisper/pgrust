/* SHIM header — NOT PostgreSQL code. The jsonpath oracle pins the default
 * collation to ctype_is_c (mirroring the harness's Rust-side
 * set_default_locale_c_for_tests pin), so regc_pg_locale.c's BUILTIN
 * strategy arms are unreachable; these are loud unreachable-arm stubs, not
 * vendored logic (static inline: no external symbols, so the tablesfam
 * lane's real verbatim unicode_category.c is untouched at link time). */
#ifndef UNICODE_CATEGORY_H
#define UNICODE_CATEGORY_H
#include "postgres.h"
static inline bool pg_u_isdigit(pg_wchar c, bool posix) { abort(); }
static inline bool pg_u_isalpha(pg_wchar c) { abort(); }
static inline bool pg_u_isalnum(pg_wchar c, bool posix) { abort(); }
static inline bool pg_u_isupper(pg_wchar c) { abort(); }
static inline bool pg_u_islower(pg_wchar c) { abort(); }
static inline bool pg_u_isgraph(pg_wchar c) { abort(); }
static inline bool pg_u_isprint(pg_wchar c) { abort(); }
static inline bool pg_u_ispunct(pg_wchar c, bool posix) { abort(); }
static inline bool pg_u_isspace(pg_wchar c) { abort(); }
#endif
