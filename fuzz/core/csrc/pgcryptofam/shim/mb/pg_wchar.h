/*
 * SHIM mb/pg_wchar.h — NOT PostgreSQL code.
 *
 * crypt-sha.c includes this only for pg_mblen_cstr(), used to size the
 * "%.*s" excerpt in the invalid-salt-character errmsg. The mock (in
 * pgcryptofam_shim.c) returns 1, which is exactly pg_wchar_table[
 * PG_SQL_ASCII].mblen under the campaign's pinned single-byte encoding;
 * message TEXT is a triage plane, not a compared plane.
 */
#ifndef PGCRYPTOFAM_SHIM_PG_WCHAR_H
#define PGCRYPTOFAM_SHIM_PG_WCHAR_H

extern int	pg_mblen_cstr(const char *mbstr);

#endif
