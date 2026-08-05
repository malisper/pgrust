/*
 * pg_portfam_io.c: vendored PostgreSQL C oracle glue for the portfam_diff
 * differential fuzz target (100%-coverage campaign, p1-microbatch PORTFAM:
 * crates/port/pg_bitutils, crates/port/crc32c, crates/port/pgstrcasecmp,
 * crates/port/pg_path, crates/backend/access/common/bufmask).
 *
 * COMPILED IN ITS OWN cc::Build (pg_difffuzz_portfam) with the
 * csrc/portfam/{shim,include} include tree — see core/build.rs.
 *
 * Provenance: every PostgreSQL body is a VERBATIM whole-file copy under
 * csrc/portfam/ from postgres-src @ 62d6c7d3df6287f1bd83199c1a746e50d31571a0
 * (Stamp-18.3):
 *   - portfam/pg_bitutils.c            = src/port/pg_bitutils.c
 *   - portfam/pg_popcount_aarch64.c    = src/port/pg_popcount_aarch64.c
 *   - portfam/pg_crc32c_sb8.c          = src/port/pg_crc32c_sb8.c
 *   - portfam/pg_crc.c                 = src/backend/utils/hash/pg_crc.c
 *   - portfam/pgstrcasecmp.c           = src/port/pgstrcasecmp.c
 *   - portfam/path.c                   = src/port/path.c (compiled -DFRONTEND)
 *   - portfam/strlcpy.c                = src/port/strlcpy.c
 *   - portfam/bufmask.c                = src/backend/access/common/bufmask.c
 *   - portfam/include/                 = verbatim headers: port/pg_bitutils.h
 *     (ALL static inlines vendored, never re-written), port/pg_crc32c.h,
 *     port/pg_bswap.h, utils/pg_crc.h, storage/{bufpage,itemid,off,block,
 *     item}.h, access/{bufmask,xlogdefs}.h, varatt.h.
 * Shims (portfam/shim, plumbing only, never logic): c.h/postgres.h/
 * postgres_fe.h typedef+macro environment, elog->pg_portfam_errcode capture
 * (bufmask.c invalid-page arm), pg_config_paths.h with the Rust crate's
 * default --prefix dirs, storage/bufmgr.h -> bufpage.h passthrough.
 *
 * Symbol isolation: every extern the vendored TUs export is renamed
 * portfam_* via -D in core/build.rs (several oracle families vendor the
 * same TUs — the wave-1 duplicate-definition hazard). This glue TU is
 * compiled with the same -D set, so the plain names below bind to the
 * portfam copies.
 *
 * ORACLE ARMS OF RECORD:
 *   - crc32c: the portable slicing-by-8 arm (pg_comp_crc32c_sb8) — hardware
 *     CRC arms (armv8/sse42 dispatch) are value-identical by algorithm and
 *     are exercised on the RUST side against this oracle.
 *   - pg_bitutils: pg_popcount32/64 etc. resolve exactly as a real build on
 *     this platform (Neon TU on aarch64, portable slow arm elsewhere).
 *   - pgstrcasecmp: isupper/tolower are the PROCESS LOCALE on both sides
 *     (the fuzz process runs in the C locale); locale is environment, not
 *     computation.
 *
 * DOMAIN CARVES (C caller-contract / UB fences — each fences C UB, never
 * pgrust behavior; the Rust side stays total):
 *   - pg_rotate_right32/pg_rotate_left32: C shifts by (32 - n) — n == 0 or
 *     n >= 32 is shift-by->=32 UB. Driver folds n into 1..=31. (Rust
 *     rotate_right/left mask n mod 32 — deliberate, documented divergence.)
 *   - pg_leftmost/rightmost_one_pos*, pg_prevpower2_*: word != 0 (C
 *     __builtin_clz/ctz UB at 0). pg_nextpower2_*: 0 < num <= 2^31/2^63.
 *   - pg_popcount buffers: bytes is a C int; driver lens are payload-sized.
 *   - path strings: C operates on NUL-terminated MAXPGPATH stack buffers;
 *     the driver caps inputs at MAXPGPATH-1 bytes and remaps interior NUL
 *     bytes (a NUL terminates the C string but is an ordinary byte to the
 *     Rust &str API — representation difference, not a behavior surface).
 *   - bufmask mask_lp_flags: pd_lower is clamped into the page (C callers
 *     only pass real WAL-consistency pages; an unclamped uint16 pd_lower
 *     walks line pointers past BLCKSZ — OOB on both sides). Same clamp
 *     applied to both sides by the driver.
 *
 * Driver entries (below) are fuzz plumbing, NOT Postgres code.
 */

#include "c.h"
#include "port/pg_bitutils.h"
#include "port/pg_crc32c.h"
#include "utils/pg_crc.h"

_Thread_local int pg_portfam_errcode;

/* vendored path.c externs (port.h decls; renamed portfam_* by build.rs) */
extern char *first_dir_separator(const char *filename);
extern char *last_dir_separator(const char *filename);
extern char *first_path_var_separator(const char *pathlist);
extern void join_path_components(char *ret_path, const char *head, const char *tail);
extern void canonicalize_path(char *path);
extern bool path_contains_parent_reference(const char *path);
extern bool path_is_relative_and_below_cwd(const char *path);
extern bool path_is_prefix_of_path(const char *path1, const char *path2);
extern void get_share_path(const char *my_exec_path, char *ret_path);
extern void get_etc_path(const char *my_exec_path, char *ret_path);
extern void get_include_path(const char *my_exec_path, char *ret_path);
extern void get_pkginclude_path(const char *my_exec_path, char *ret_path);
extern void get_includeserver_path(const char *my_exec_path, char *ret_path);
extern void get_lib_path(const char *my_exec_path, char *ret_path);
extern void get_pkglib_path(const char *my_exec_path, char *ret_path);
extern void get_locale_path(const char *my_exec_path, char *ret_path);
extern void get_doc_path(const char *my_exec_path, char *ret_path);
extern void get_html_path(const char *my_exec_path, char *ret_path);
extern void get_man_path(const char *my_exec_path, char *ret_path);
extern void get_parent_directory(char *path);

/* vendored bufmask.c externs (renamed portfam_* by build.rs) */
extern void mask_page_lsn_and_checksum(char *page);
extern void mask_page_hint_bits(char *page);
extern void mask_unused_space(char *page);
extern void mask_lp_flags(char *page);
extern void mask_page_content(char *page);

/* ==================== pg_bitutils driver entries ==================== */

/*
 * Word-op battery. out layout (a slot is written only when its domain guard
 * passed; the Rust driver applies the same guards):
 *   out[0] = pg_popcount32/64
 *   out[1] = pg_ceil_log2_32/64
 *   out[2] = pg_leftmost_one_pos   (word != 0)
 *   out[3] = pg_rightmost_one_pos  (word != 0)
 *   out[4] = pg_prevpower2         (word != 0)
 *   out[5] = pg_nextpower2         (0 < word <= 2^31 / 2^63)
 */
void
pg_diff_pf_bitutils32(uint32 word, uint64 out[6])
{
	out[0] = (uint64) (int64) pg_popcount32(word);
	out[1] = pg_ceil_log2_32(word);
	if (word != 0)
	{
		out[2] = (uint64) (int64) pg_leftmost_one_pos32(word);
		out[3] = (uint64) (int64) pg_rightmost_one_pos32(word);
		out[4] = pg_prevpower2_32(word);
	}
	if (word > 0 && word <= PG_UINT32_MAX / 2 + 1)
		out[5] = pg_nextpower2_32(word);
}

void
pg_diff_pf_bitutils64(uint64 word, uint64 out[6])
{
	out[0] = (uint64) (int64) pg_popcount64(word);
	out[1] = pg_ceil_log2_64(word);
	if (word != 0)
	{
		out[2] = (uint64) (int64) pg_leftmost_one_pos64(word);
		out[3] = (uint64) (int64) pg_rightmost_one_pos64(word);
		out[4] = pg_prevpower2_64(word);
	}
	if (word > 0 && word <= PG_UINT64_MAX / 2 + 1)
		out[5] = pg_nextpower2_64(word);
}

/* n must be 1..=31 (see DOMAIN CARVES). */
uint32
pg_diff_pf_rotate_right32(uint32 word, int n)
{
	return pg_rotate_right32(word, n);
}

uint32
pg_diff_pf_rotate_left32(uint32 word, int n)
{
	return pg_rotate_left32(word, n);
}

uint64
pg_diff_pf_popcount(const char *buf, int bytes)
{
	return pg_popcount(buf, bytes);
}

uint64
pg_diff_pf_popcount_masked(const char *buf, int bytes, uint8 mask)
{
	return pg_popcount_masked(buf, bytes, (bits8) mask);
}

/* ============== crc32c / legacy crc driver entries ============== */

uint32
pg_diff_pf_crc32c_sb8(uint32 crc, const void *data, size_t len)
{
	return pg_comp_crc32c_sb8(crc, data, len);
}

/*
 * Full INIT/COMP/FIN pipelines — the macro bodies expand VERBATIM from the
 * vendored utils/pg_crc.h (the only form these algorithms exist in).
 */
uint32
pg_diff_pf_crc32_traditional(const void *data, size_t len)
{
	pg_crc32	crc;

	INIT_TRADITIONAL_CRC32(crc);
	COMP_TRADITIONAL_CRC32(crc, data, len);
	FIN_TRADITIONAL_CRC32(crc);
	return crc;
}

uint32
pg_diff_pf_crc32_legacy(const void *data, size_t len)
{
	pg_crc32	crc;

	INIT_LEGACY_CRC32(crc);
	COMP_LEGACY_CRC32(crc, data, len);
	FIN_LEGACY_CRC32(crc);
	return crc;
}

/* ==================== pgstrcasecmp driver entries ==================== */

int
pg_diff_pf_strcasecmp(const char *s1, const char *s2)
{
	return pg_strcasecmp(s1, s2);
}

int
pg_diff_pf_strncasecmp(const char *s1, const char *s2, size_t n)
{
	return pg_strncasecmp(s1, s2, n);
}

int
pg_diff_pf_toupper(int ch)
{
	return pg_toupper((unsigned char) ch);
}

int
pg_diff_pf_tolower(int ch)
{
	return pg_tolower((unsigned char) ch);
}

int
pg_diff_pf_ascii_toupper(int ch)
{
	return pg_ascii_toupper((unsigned char) ch);
}

int
pg_diff_pf_ascii_tolower(int ch)
{
	return pg_ascii_tolower((unsigned char) ch);
}

/* ==================== path.c driver entries ==================== */

/* buf must hold at least MAXPGPATH bytes; modified in place. */
void
pg_diff_pf_canonicalize(char *buf)
{
	canonicalize_path(buf);
}

void
pg_diff_pf_join(const char *head, const char *tail, char *ret /* MAXPGPATH */)
{
	join_path_components(ret, head, tail);
}

void
pg_diff_pf_parent_dir(char *buf)
{
	get_parent_directory(buf);
}

long
pg_diff_pf_first_dir_sep(const char *s)
{
	char	   *p = first_dir_separator(s);

	return p ? (long) (p - s) : -1;
}

long
pg_diff_pf_last_dir_sep(const char *s)
{
	char	   *p = last_dir_separator(s);

	return p ? (long) (p - s) : -1;
}

long
pg_diff_pf_first_path_var_sep(const char *s)
{
	char	   *p = first_path_var_separator(s);

	return p ? (long) (p - s) : -1;
}

int
pg_diff_pf_contains_parent_ref(const char *s)
{
	return path_contains_parent_reference(s) ? 1 : 0;
}

int
pg_diff_pf_rel_below_cwd(const char *s)
{
	return path_is_relative_and_below_cwd(s) ? 1 : 0;
}

int
pg_diff_pf_prefix_of(const char *p1, const char *p2)
{
	return path_is_prefix_of_path(p1, p2) ? 1 : 0;
}

/* which selects the get_*_path flavor, 0..=10; ret must hold MAXPGPATH. */
void
pg_diff_pf_get_rel_path(int which, const char *my_exec_path, char *ret)
{
	switch (which)
	{
		case 0:
			get_share_path(my_exec_path, ret);
			break;
		case 1:
			get_etc_path(my_exec_path, ret);
			break;
		case 2:
			get_include_path(my_exec_path, ret);
			break;
		case 3:
			get_pkginclude_path(my_exec_path, ret);
			break;
		case 4:
			get_includeserver_path(my_exec_path, ret);
			break;
		case 5:
			get_lib_path(my_exec_path, ret);
			break;
		case 6:
			get_pkglib_path(my_exec_path, ret);
			break;
		case 7:
			get_locale_path(my_exec_path, ret);
			break;
		case 8:
			get_doc_path(my_exec_path, ret);
			break;
		case 9:
			get_html_path(my_exec_path, ret);
			break;
		default:
			get_man_path(my_exec_path, ret);
			break;
	}
}

/* ==================== bufmask driver entries ==================== */

void
pg_diff_pf_mask_page_lsn_and_checksum(char *page)
{
	mask_page_lsn_and_checksum(page);
}

void
pg_diff_pf_mask_page_hint_bits(char *page)
{
	mask_page_hint_bits(page);
}

/* Returns 1 when the vendored elog(ERROR) arm fired (invalid page). */
int
pg_diff_pf_mask_unused_space(char *page)
{
	pg_portfam_errcode = 0;
	mask_unused_space(page);
	return pg_portfam_errcode != 0;
}

void
pg_diff_pf_mask_lp_flags(char *page)
{
	mask_lp_flags(page);
}

void
pg_diff_pf_mask_page_content(char *page)
{
	mask_page_content(page);
}
