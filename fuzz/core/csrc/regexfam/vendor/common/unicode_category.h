/* BUILTIN-strategy classifiers: unreachable under C collation; stubs abort. */
#ifndef CREF_REGEX_UNICODE_CATEGORY_H
#define CREF_REGEX_UNICODE_CATEGORY_H

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
