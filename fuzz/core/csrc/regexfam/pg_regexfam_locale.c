/* regex_diff locale probe (p1-lanew): standalone compile of the verbatim
 * REL_18_3 regc_pg_locale.c with REAL builtin-provider tables (the same
 * generated unicode_category.c the tablesfam oracle vendors, recompiled
 * here under the lprobe_ symbol prefix, plus unicode_case.c fetched
 * verbatim at the pinned sha) so the exhaustive pg_wc_* class/case sweeps
 * can diff the shipped Rust regex_locale.rs against the true C behavior
 * for the C and BUILTIN strategies. LIBC/ICU strategies stay carved
 * (platform-locale FFI; see phase1-routes.tsv regex/regex_core).
 *
 * pg_set_regex_collation here is renamed lprobe_set_regex_collation by a
 * build.rs define — the engine oracle (regcomp.c TU) owns the unprefixed
 * symbol. Strategy state is this TU's own static, so the engine oracle's
 * C-collation pin is untouched by sweep calls. */

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/* EXPLICIT vendor path: since the regexp_diff family landed its own
 * shim postgres.h as this file's directory neighbor, a bare
 * "postgres.h" quoted include would resolve to THAT tree first
 * (quoted-include search starts in the includer's directory) and
 * clash with the vendor headers this TU compiles against. */
#include "vendor/postgres.h"
#include "regex/regguts.h"

/* localereal/regc_pg_locale.c is a byte-identical copy of
 * vendor/regc_pg_locale.c (cmp-verified at vendoring time): quoted
 * includes resolve file-relative first, so the copy must sit next to the
 * probe's REAL common/ and utils/ headers or the vendor tree's aborting
 * stubs win regardless of -I order. */
#include "localereal/regc_pg_locale.c"

/* Synthetic builtin collation rows (see localereal/utils/pg_locale.h). */
static struct pg_locale_struct lprobe_builtin_posix = {
	.provider = COLLPROVIDER_BUILTIN,
	.deterministic = true,
	.collate_is_c = true,
	.ctype_is_c = false,
	.is_default = false,
	.info.builtin = {.locale = "C.UTF-8", .casemap_full = false},
};
static struct pg_locale_struct lprobe_builtin_full = {
	.provider = COLLPROVIDER_BUILTIN,
	.deterministic = true,
	.collate_is_c = true,
	.ctype_is_c = false,
	.is_default = false,
	.info.builtin = {.locale = "PG_UNICODE_FAST", .casemap_full = true},
};

#define LPROBE_BUILTIN_POSIX_OID 61001
#define LPROBE_BUILTIN_FULL_OID 61002

pg_locale_t
pg_newlocale_from_collation(Oid collid)
{
	switch (collid)
	{
		case LPROBE_BUILTIN_POSIX_OID:
			return &lprobe_builtin_posix;
		case LPROBE_BUILTIN_FULL_OID:
			return &lprobe_builtin_full;
		default:
			abort();			/* probe drives only the two builtin oids */
	}
}

/* driver entry: 950 (C), 61001 (builtin posix), 61002 (builtin full) */
void
pg_diff_locale_set(Oid collid)
{
	pg_set_regex_collation(collid);	/* renamed lprobe_... by build.rs */
}

/* All 11 class predicates packed into one mask, LSB order:
 * digit alpha alnum word upper lower graph print punct space isword-dup?
 * (10 predicates + pg_wc_isword = 11 bits as listed below). */
uint32_t
pg_diff_wc_class_mask(uint32_t c)
{
	uint32_t	m = 0;

	m |= (uint32_t) (pg_wc_isdigit(c) != 0) << 0;
	m |= (uint32_t) (pg_wc_isalpha(c) != 0) << 1;
	m |= (uint32_t) (pg_wc_isalnum(c) != 0) << 2;
	m |= (uint32_t) (pg_wc_isword(c) != 0) << 3;
	m |= (uint32_t) (pg_wc_isupper(c) != 0) << 4;
	m |= (uint32_t) (pg_wc_islower(c) != 0) << 5;
	m |= (uint32_t) (pg_wc_isgraph(c) != 0) << 6;
	m |= (uint32_t) (pg_wc_isprint(c) != 0) << 7;
	m |= (uint32_t) (pg_wc_ispunct(c) != 0) << 8;
	m |= (uint32_t) (pg_wc_isspace(c) != 0) << 9;
	return m;
}

uint32_t
pg_diff_wc_toupper(uint32_t c)
{
	return pg_wc_toupper(c);
}

uint32_t
pg_diff_wc_tolower(uint32_t c)
{
	return pg_wc_tolower(c);
}
