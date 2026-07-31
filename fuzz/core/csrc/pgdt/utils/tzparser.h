/*
 * BYTE-COPY from PostgreSQL 18.3 src/include/utils/tzparser.h @ 62d6c7d3df
 * (only the tzEntry definition the vendored ConvertTimeZoneAbbrevs needs; the
 * parser entry points themselves are not vendored — the driver hands
 * ConvertTimeZoneAbbrevs a pinned, already-parsed array).
 */
#ifndef PG_DIFFFUZZ_PGDT_TZPARSER_H
#define PG_DIFFFUZZ_PGDT_TZPARSER_H

typedef struct tzEntry
{
	/* the actual data */
	char	   *abbrev;			/* TZ abbreviation (downcased) */
	char	   *zone;			/* zone name if dynamic abbrev, else NULL */
	/* for a dynamic abbreviation, offset/is_dst are not used */
	int			offset;			/* offset in seconds from UTC */
	bool		is_dst;			/* true if a DST abbreviation */
	/* source information (for error messages) */
	int			lineno;
	const char *filename;
} tzEntry;

#endif
