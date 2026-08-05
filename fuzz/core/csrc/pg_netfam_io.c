/*
 * pg_netfam_io.c: vendored PostgreSQL C oracle for the netfam_diff
 * differential fuzz target (100%-coverage campaign, lane p1-mb-netfam).
 * Crates under test (see fuzz/core/src/netfam_diff.rs):
 *   crates/backend/libpq/ifaddr, crates/backend/libpq/pqformat.
 *
 * Provenance (all Postgres bodies VERBATIM sed-extracted from the vendor
 * tree at ~/dev/pgrust-fabled/vendor/postgres-src, Stamp-18.3, upstream
 * sha 62d6c7d3df6287f1bd83199c1a746e50d31571a0 — assembled by
 * scratchpad/assemble_netfam.sh, never hand-typed):
 *   - src/include/port/pg_bswap.h lines 31-128 (pg_bswap16/32/64 +
 *     pg_hton/pg_ntoh; HAVE__BUILTIN_BSWAP* defined => builtin macros).
 *   - src/include/lib/stringinfo.h lines 46-54 (StringInfoData), 112
 *     (STRINGINFO_DEFAULT_SIZE), 231-234 (appendStringInfoCharMacro).
 *   - src/include/c.h lines 655-659 (struct varlena), 661 (VARHDRSZ),
 *     668 (bytea typedef).
 *   - src/include/varatt.h lines 111-139 (varattrib_4b/1b/1b_e), 176-242
 *     (both endian macro arms; WORDS_BIGENDIAN undefined on x86-64 and
 *     aarch64 => the little-endian arm compiles, as on the fleet), 305
 *     (SET_VARSIZE).
 *   - src/include/libpq/ifaddr.h lines 17-19 (PgIfAddrCallback).
 *   - src/backend/libpq/ifaddr.c lines 33-216 (pg_range_sockaddr +
 *     range_sockaddr_AF_INET/6 + pg_sockaddr_cidr_mask +
 *     run_ifaddr_callback) and 294-309 (the HAVE_GETIFADDRS
 *     pg_foreach_ifaddr — the arm both macOS and Linux build).
 *   - src/common/stringinfo.c lines 32-48 (initStringInfoInternal),
 *     90-100 (initStringInfo), 116-134 (resetStringInfo), 235-252
 *     (appendStringInfoChar), 274-298 (appendBinaryStringInfo), 300-317
 *     (appendBinaryStringInfoNT), 319-400 (enlargeStringInfo).
 *     Sibling-lane precedent: p1-mb-miscfam vendored the same functions in
 *     csrc/pg_miscfam_io.c; this TU keeps its OWN verbatim copy behind
 *     nf_-renames (drift-detection property of per-lane duplication; no
 *     symbol collision: every extern here is nf_-prefixed).
 *   - src/include/libpq/pqformat.h lines 33-188 (pq_writeint8/16/32/64,
 *     pq_writestring, pq_sendint8/16/32/64, pq_sendbyte, pq_sendint —
 *     header static inlines, vendored verbatim per the
 *     never-fabricate-header-inlines hazard).
 *   - src/backend/libpq/pqformat.c lines 83-641 (every function:
 *     pq_beginmessage .. pq_getmsgend).
 *
 * Shims (plumbing only, never logic):
 *   - fixed-width typedefs matching c.h on LP64; Size = size_t; Assert
 *     noop (release parity); pg_restrict -> C99 restrict; _( ) NLS-off.
 *   - ereport/elog(ERROR) -> errcode-class capture + longjmp to the armed
 *     driver entry. Classes: 0 ok, 1 = ERRCODE_PROTOCOL_VIOLATION (08P01),
 *     2 = internal/elog (XX000), 3 = ERRCODE_CHARACTER_NOT_IN_REPERTOIRE
 *     (22021), 4 = ERRCODE_PROGRAM_LIMIT_EXCEEDED (54000).
 *   - palloc/repalloc/pfree -> malloc/realloc/free (MaxAllocSize guard
 *     fires first, as in C).
 *   - pg_client_to_server: models the boot-default encoding environment
 *     (client == server == SQL_ASCII): len<=0 and NUL-free inputs pass
 *     through IDENTITY (pg_any_to_server mbutils.c:610-624 returns the
 *     caller's pointer after pg_verify_mbstr); an embedded NUL raises
 *     22021 exactly as pg_verify_mbstr_len does for 1-byte encodings
 *     (wchar.c report_invalid_encoding). This seam is the excluded-state
 *     carve of record for the crate (client-encoding conversion).
 *   - pg_server_to_client: same environment; additionally, when the
 *     driver arms the pg_nf_convert flag (modeling an installed identity
 *     conversion, NUL-free inputs only) it returns a malloc'd copy so the
 *     p != str converted arms execute on both sides. The Rust driver
 *     installs the mirror-image seam impl (netfam_diff.rs).
 *   - pq_putmessage -> capture (msgtype, body bytes) in TLS; the Rust
 *     driver installs the mirror pqcomm_seams::pq_putmessage collector.
 *     This is the socket carve: bytes are compared at the seam.
 *   - every vendored extern is renamed nf_* via #define (symbol isolation
 *     vs pg_rowtypes_io.c / sibling-lane TUs vendoring the same files).
 *
 * Driver entries (SECTION D, pg_nf_* prefix) are fuzz plumbing, NOT
 * Postgres code. Every entry that can reach ereport/elog arms the jmp_buf.
 */

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <setjmp.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <ifaddrs.h>
#include "pg_oracle_guard.h"	/* oracle-serialization holder check */

typedef int8_t int8;
typedef int16_t int16;
typedef int32_t int32;
typedef int64_t int64;
typedef uint8_t uint8;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;
typedef float float4;
typedef double float8;
typedef size_t Size;

#define Assert(x) ((void) 0)
#define AssertMacro(x) ((void) 0)
#define FLEXIBLE_ARRAY_MEMBER	/* empty */
#define pg_restrict restrict
#define _(x) (x)
#define HIGHBIT (0x80)
#define IS_HIGHBIT_SET(ch) ((unsigned char)(ch) & HIGHBIT)
/* memutils.h verbatim value */
#define MaxAllocSize	((Size) 0x3fffffff) /* 1 gigabyte - 1 */
/* both fleet (aarch64/x86-64 Linux) and macOS hosts have the builtins */
#define HAVE__BUILTIN_BSWAP16 1
#define HAVE__BUILTIN_BSWAP32 1
#define HAVE__BUILTIN_BSWAP64 1
/* HAVE_GETIFADDRS arm is the vendored one (Linux + macOS both) */

/* ---- SHIM: TLS error channel + longjmp (armed by driver entries) ---- */

static _Thread_local int pg_nf_errcode;	/* 0 ok / classes above */
static _Thread_local jmp_buf pg_nf_jmp;
static _Thread_local int pg_nf_pending_code;

#define PG_NF_ERR_PROTOCOL 1	/* 08P01 */
#define PG_NF_ERR_INTERNAL 2	/* XX000 (elog ERROR default) */
#define PG_NF_ERR_CHAR_REPERTOIRE 3 /* 22021 */
#define PG_NF_ERR_PROGRAM_LIMIT 4	/* 54000 */

static void
pg_nf_raise(int code)
{
	pg_nf_errcode = code;
	longjmp(pg_nf_jmp, 1);
}

static int
pg_nf_errcode_set(int code)
{
	pg_nf_pending_code = code;
	return 0;
}

static int
pg_nf_errmsg(const char *fmt,...)
{
	(void) fmt;
	return 0;
}

#define errcode(c) pg_nf_errcode_set(c)
#define errmsg pg_nf_errmsg
#define errdetail pg_nf_errmsg
#define ereport(level, rest) do { pg_nf_pending_code = PG_NF_ERR_INTERNAL; ((void) (rest)); pg_nf_raise(pg_nf_pending_code); } while (0)
#define elog(level, ...) pg_nf_raise(PG_NF_ERR_INTERNAL)
#define ERRCODE_PROTOCOL_VIOLATION PG_NF_ERR_PROTOCOL
#define ERRCODE_PROGRAM_LIMIT_EXCEEDED PG_NF_ERR_PROGRAM_LIMIT
#define ERROR 21

#define palloc(n) malloc(n)
#define repalloc(p, n) realloc((p), (n))
#define pfree(p) free(p)

/* ---- SHIM: encoding-conversion seam (see header) ---- */
static _Thread_local int pg_nf_convert;	/* fake installed s2c conversion */
static char *pg_nf_client_to_server(const char *s, int len);
static char *pg_nf_server_to_client(const char *s, int len);
#define pg_client_to_server pg_nf_client_to_server
#define pg_server_to_client pg_nf_server_to_client

/* ---- SHIM: pq_putmessage capture (socket seam) ---- */
static int pg_nf_putmessage(char msgtype, const char *s, size_t len);
#define pq_putmessage pg_nf_putmessage

/* ---- symbol isolation: rename every vendored extern nf_* ---- */
#define initStringInfo nf_initStringInfo
#define resetStringInfo nf_resetStringInfo
#define appendStringInfoChar nf_appendStringInfoChar
#define appendBinaryStringInfo nf_appendBinaryStringInfo
#define appendBinaryStringInfoNT nf_appendBinaryStringInfoNT
#define enlargeStringInfo nf_enlargeStringInfo
#define pq_beginmessage nf_pq_beginmessage
#define pq_beginmessage_reuse nf_pq_beginmessage_reuse
#define pq_sendbytes nf_pq_sendbytes
#define pq_sendcountedtext nf_pq_sendcountedtext
#define pq_sendtext nf_pq_sendtext
#define pq_sendstring nf_pq_sendstring
#define pq_send_ascii_string nf_pq_send_ascii_string
#define pq_sendfloat4 nf_pq_sendfloat4
#define pq_sendfloat8 nf_pq_sendfloat8
#define pq_endmessage nf_pq_endmessage
#define pq_endmessage_reuse nf_pq_endmessage_reuse
#define pq_begintypsend nf_pq_begintypsend
#define pq_endtypsend nf_pq_endtypsend
#define pq_puttextmessage nf_pq_puttextmessage
#define pq_putemptymessage nf_pq_putemptymessage
#define pq_getmsgbyte nf_pq_getmsgbyte
#define pq_getmsgint nf_pq_getmsgint
#define pq_getmsgint64 nf_pq_getmsgint64
#define pq_getmsgfloat4 nf_pq_getmsgfloat4
#define pq_getmsgfloat8 nf_pq_getmsgfloat8
#define pq_getmsgbytes nf_pq_getmsgbytes
#define pq_copymsgbytes nf_pq_copymsgbytes
#define pq_getmsgtext nf_pq_getmsgtext
#define pq_getmsgstring nf_pq_getmsgstring
#define pq_getmsgrawstring nf_pq_getmsgrawstring
#define pq_getmsgend nf_pq_getmsgend
#define pg_range_sockaddr nf_pg_range_sockaddr
#define pg_sockaddr_cidr_mask nf_pg_sockaddr_cidr_mask
#define pg_foreach_ifaddr nf_pg_foreach_ifaddr

/* ---- VERBATIM src/include/port/pg_bswap.h lines 31-128 ---- */
#if defined(HAVE__BUILTIN_BSWAP16)

#define pg_bswap16(x) __builtin_bswap16(x)

#elif defined(_MSC_VER)

#define pg_bswap16(x) _byteswap_ushort(x)

#else

static inline uint16
pg_bswap16(uint16 x)
{
	return
		((x << 8) & 0xff00) |
		((x >> 8) & 0x00ff);
}

#endif							/* HAVE__BUILTIN_BSWAP16 */


/* implementation of uint32 pg_bswap32(uint32) */
#if defined(HAVE__BUILTIN_BSWAP32)

#define pg_bswap32(x) __builtin_bswap32(x)

#elif defined(_MSC_VER)

#define pg_bswap32(x) _byteswap_ulong(x)

#else

static inline uint32
pg_bswap32(uint32 x)
{
	return
		((x << 24) & 0xff000000) |
		((x << 8) & 0x00ff0000) |
		((x >> 8) & 0x0000ff00) |
		((x >> 24) & 0x000000ff);
}

#endif							/* HAVE__BUILTIN_BSWAP32 */


/* implementation of uint64 pg_bswap64(uint64) */
#if defined(HAVE__BUILTIN_BSWAP64)

#define pg_bswap64(x) __builtin_bswap64(x)


#elif defined(_MSC_VER)

#define pg_bswap64(x) _byteswap_uint64(x)

#else

static inline uint64
pg_bswap64(uint64 x)
{
	return
		((x << 56) & UINT64CONST(0xff00000000000000)) |
		((x << 40) & UINT64CONST(0x00ff000000000000)) |
		((x << 24) & UINT64CONST(0x0000ff0000000000)) |
		((x << 8) & UINT64CONST(0x000000ff00000000)) |
		((x >> 8) & UINT64CONST(0x00000000ff000000)) |
		((x >> 24) & UINT64CONST(0x0000000000ff0000)) |
		((x >> 40) & UINT64CONST(0x000000000000ff00)) |
		((x >> 56) & UINT64CONST(0x00000000000000ff));
}
#endif							/* HAVE__BUILTIN_BSWAP64 */


/*
 * Portable and fast equivalents for ntohs, ntohl, htons, htonl,
 * additionally extended to 64 bits.
 */
#ifdef WORDS_BIGENDIAN

#define pg_hton16(x)		(x)
#define pg_hton32(x)		(x)
#define pg_hton64(x)		(x)

#define pg_ntoh16(x)		(x)
#define pg_ntoh32(x)		(x)
#define pg_ntoh64(x)		(x)

#else

#define pg_hton16(x)		pg_bswap16(x)
#define pg_hton32(x)		pg_bswap32(x)
#define pg_hton64(x)		pg_bswap64(x)

#define pg_ntoh16(x)		pg_bswap16(x)
#define pg_ntoh32(x)		pg_bswap32(x)
#define pg_ntoh64(x)		pg_bswap64(x)

#endif							/* WORDS_BIGENDIAN */

/* ---- VERBATIM src/include/lib/stringinfo.h lines 46-54, 112 ---- */
typedef struct StringInfoData
{
	char	   *data;
	int			len;
	int			maxlen;
	int			cursor;
} StringInfoData;

typedef StringInfoData *StringInfo;
#define STRINGINFO_DEFAULT_SIZE 1024	/* default initial allocation size */

/* prototypes for the renamed vendored stringinfo externs (stringinfo.h
 * carried these in C; the #define renames above apply) */
extern void initStringInfo(StringInfo str);
extern void resetStringInfo(StringInfo str);
extern void appendStringInfoChar(StringInfo str, char ch);
extern void appendBinaryStringInfo(StringInfo str, const void *data, int datalen);
extern void appendBinaryStringInfoNT(StringInfo str, const void *data, int datalen);
extern void enlargeStringInfo(StringInfo str, int needed);

/* ---- VERBATIM src/include/lib/stringinfo.h lines 231-234 ---- */
#define appendStringInfoCharMacro(str,ch) \
	(((str)->len + 1 >= (str)->maxlen) ? \
	 appendStringInfoChar(str, ch) : \
	 (void)((str)->data[(str)->len] = (ch), (str)->data[++(str)->len] = '\0'))

/* ---- VERBATIM src/include/c.h lines 655-659, 661, 668 ---- */
struct varlena
{
	char		vl_len_[4];		/* Do not touch this field directly! */
	char		vl_dat[FLEXIBLE_ARRAY_MEMBER];	/* Data content is here */
};
#define VARHDRSZ		((int32) sizeof(int32))
typedef struct varlena bytea;

/* ---- VERBATIM src/include/varatt.h lines 111-139, 176-242, 305 ---- */
typedef union
{
	struct						/* Normal varlena (4-byte length) */
	{
		uint32		va_header;
		char		va_data[FLEXIBLE_ARRAY_MEMBER];
	}			va_4byte;
	struct						/* Compressed-in-line format */
	{
		uint32		va_header;
		uint32		va_tcinfo;	/* Original data size (excludes header) and
								 * compression method; see va_extinfo */
		char		va_data[FLEXIBLE_ARRAY_MEMBER]; /* Compressed data */
	}			va_compressed;
} varattrib_4b;

typedef struct
{
	uint8		va_header;
	char		va_data[FLEXIBLE_ARRAY_MEMBER]; /* Data begins here */
} varattrib_1b;

/* TOAST pointers are a subset of varattrib_1b with an identifying tag byte */
typedef struct
{
	uint8		va_header;		/* Always 0x80 or 0x01 */
	uint8		va_tag;			/* Type of datum */
	char		va_data[FLEXIBLE_ARRAY_MEMBER]; /* Type-specific data */
} varattrib_1b_e;
#ifdef WORDS_BIGENDIAN

#define VARATT_IS_4B(PTR) \
	((((varattrib_1b *) (PTR))->va_header & 0x80) == 0x00)
#define VARATT_IS_4B_U(PTR) \
	((((varattrib_1b *) (PTR))->va_header & 0xC0) == 0x00)
#define VARATT_IS_4B_C(PTR) \
	((((varattrib_1b *) (PTR))->va_header & 0xC0) == 0x40)
#define VARATT_IS_1B(PTR) \
	((((varattrib_1b *) (PTR))->va_header & 0x80) == 0x80)
#define VARATT_IS_1B_E(PTR) \
	((((varattrib_1b *) (PTR))->va_header) == 0x80)
#define VARATT_NOT_PAD_BYTE(PTR) \
	(*((uint8 *) (PTR)) != 0)

/* VARSIZE_4B() should only be used on known-aligned data */
#define VARSIZE_4B(PTR) \
	(((varattrib_4b *) (PTR))->va_4byte.va_header & 0x3FFFFFFF)
#define VARSIZE_1B(PTR) \
	(((varattrib_1b *) (PTR))->va_header & 0x7F)
#define VARTAG_1B_E(PTR) \
	(((varattrib_1b_e *) (PTR))->va_tag)

#define SET_VARSIZE_4B(PTR,len) \
	(((varattrib_4b *) (PTR))->va_4byte.va_header = (len) & 0x3FFFFFFF)
#define SET_VARSIZE_4B_C(PTR,len) \
	(((varattrib_4b *) (PTR))->va_4byte.va_header = ((len) & 0x3FFFFFFF) | 0x40000000)
#define SET_VARSIZE_1B(PTR,len) \
	(((varattrib_1b *) (PTR))->va_header = (len) | 0x80)
#define SET_VARTAG_1B_E(PTR,tag) \
	(((varattrib_1b_e *) (PTR))->va_header = 0x80, \
	 ((varattrib_1b_e *) (PTR))->va_tag = (tag))

#else							/* !WORDS_BIGENDIAN */

#define VARATT_IS_4B(PTR) \
	((((varattrib_1b *) (PTR))->va_header & 0x01) == 0x00)
#define VARATT_IS_4B_U(PTR) \
	((((varattrib_1b *) (PTR))->va_header & 0x03) == 0x00)
#define VARATT_IS_4B_C(PTR) \
	((((varattrib_1b *) (PTR))->va_header & 0x03) == 0x02)
#define VARATT_IS_1B(PTR) \
	((((varattrib_1b *) (PTR))->va_header & 0x01) == 0x01)
#define VARATT_IS_1B_E(PTR) \
	((((varattrib_1b *) (PTR))->va_header) == 0x01)
#define VARATT_NOT_PAD_BYTE(PTR) \
	(*((uint8 *) (PTR)) != 0)

/* VARSIZE_4B() should only be used on known-aligned data */
#define VARSIZE_4B(PTR) \
	((((varattrib_4b *) (PTR))->va_4byte.va_header >> 2) & 0x3FFFFFFF)
#define VARSIZE_1B(PTR) \
	((((varattrib_1b *) (PTR))->va_header >> 1) & 0x7F)
#define VARTAG_1B_E(PTR) \
	(((varattrib_1b_e *) (PTR))->va_tag)

#define SET_VARSIZE_4B(PTR,len) \
	(((varattrib_4b *) (PTR))->va_4byte.va_header = (((uint32) (len)) << 2))
#define SET_VARSIZE_4B_C(PTR,len) \
	(((varattrib_4b *) (PTR))->va_4byte.va_header = (((uint32) (len)) << 2) | 0x02)
#define SET_VARSIZE_1B(PTR,len) \
	(((varattrib_1b *) (PTR))->va_header = (((uint8) (len)) << 1) | 0x01)
#define SET_VARTAG_1B_E(PTR,tag) \
	(((varattrib_1b_e *) (PTR))->va_header = 0x01, \
	 ((varattrib_1b_e *) (PTR))->va_tag = (tag))

#endif							/* WORDS_BIGENDIAN */
#define SET_VARSIZE(PTR, len)				SET_VARSIZE_4B(PTR, len)

/* ---- VERBATIM src/include/libpq/ifaddr.h lines 17-19 ---- */
typedef void (*PgIfAddrCallback) (struct sockaddr *addr,
								  struct sockaddr *netmask,
								  void *cb_data);

/* prototypes for the renamed vendored pqformat externs (pqformat.h carried
 * these in C; the #define renames above apply) */
extern void pq_beginmessage(StringInfo buf, char msgtype);
extern void pq_beginmessage_reuse(StringInfo buf, char msgtype);
extern void pq_endmessage(StringInfo buf);
extern void pq_endmessage_reuse(StringInfo buf);
extern void pq_sendbytes(StringInfo buf, const void *data, int datalen);
extern void pq_sendcountedtext(StringInfo buf, const char *str, int slen);
extern void pq_sendtext(StringInfo buf, const char *str, int slen);
extern void pq_sendstring(StringInfo buf, const char *str);
extern void pq_send_ascii_string(StringInfo buf, const char *str);
extern void pq_sendfloat4(StringInfo buf, float4 f);
extern void pq_sendfloat8(StringInfo buf, float8 f);
extern void pq_begintypsend(StringInfo buf);
extern bytea *pq_endtypsend(StringInfo buf);
extern void pq_puttextmessage(char msgtype, const char *str);
extern void pq_putemptymessage(char msgtype);
extern int	pq_getmsgbyte(StringInfo msg);
extern unsigned int pq_getmsgint(StringInfo msg, int b);
extern int64 pq_getmsgint64(StringInfo msg);
extern float4 pq_getmsgfloat4(StringInfo msg);
extern float8 pq_getmsgfloat8(StringInfo msg);
extern const char *pq_getmsgbytes(StringInfo msg, int datalen);
extern void pq_copymsgbytes(StringInfo msg, void *buf, int datalen);
extern char *pq_getmsgtext(StringInfo msg, int rawbytes, int *nbytes);
extern const char *pq_getmsgstring(StringInfo msg);
extern const char *pq_getmsgrawstring(StringInfo msg);
extern void pq_getmsgend(StringInfo msg);

/* ---- VERBATIM src/backend/libpq/ifaddr.c lines 33-216 ---- */
static int	range_sockaddr_AF_INET(const struct sockaddr_in *addr,
								   const struct sockaddr_in *netaddr,
								   const struct sockaddr_in *netmask);

static int	range_sockaddr_AF_INET6(const struct sockaddr_in6 *addr,
									const struct sockaddr_in6 *netaddr,
									const struct sockaddr_in6 *netmask);


/*
 * pg_range_sockaddr - is addr within the subnet specified by netaddr/netmask ?
 *
 * Note: caller must already have verified that all three addresses are
 * in the same address family; and AF_UNIX addresses are not supported.
 */
int
pg_range_sockaddr(const struct sockaddr_storage *addr,
				  const struct sockaddr_storage *netaddr,
				  const struct sockaddr_storage *netmask)
{
	if (addr->ss_family == AF_INET)
		return range_sockaddr_AF_INET((const struct sockaddr_in *) addr,
									  (const struct sockaddr_in *) netaddr,
									  (const struct sockaddr_in *) netmask);
	else if (addr->ss_family == AF_INET6)
		return range_sockaddr_AF_INET6((const struct sockaddr_in6 *) addr,
									   (const struct sockaddr_in6 *) netaddr,
									   (const struct sockaddr_in6 *) netmask);
	else
		return 0;
}

static int
range_sockaddr_AF_INET(const struct sockaddr_in *addr,
					   const struct sockaddr_in *netaddr,
					   const struct sockaddr_in *netmask)
{
	if (((addr->sin_addr.s_addr ^ netaddr->sin_addr.s_addr) &
		 netmask->sin_addr.s_addr) == 0)
		return 1;
	else
		return 0;
}

static int
range_sockaddr_AF_INET6(const struct sockaddr_in6 *addr,
						const struct sockaddr_in6 *netaddr,
						const struct sockaddr_in6 *netmask)
{
	int			i;

	for (i = 0; i < 16; i++)
	{
		if (((addr->sin6_addr.s6_addr[i] ^ netaddr->sin6_addr.s6_addr[i]) &
			 netmask->sin6_addr.s6_addr[i]) != 0)
			return 0;
	}

	return 1;
}

/*
 *	pg_sockaddr_cidr_mask - make a network mask of the appropriate family
 *	  and required number of significant bits
 *
 * numbits can be null, in which case the mask is fully set.
 *
 * The resulting mask is placed in *mask, which had better be big enough.
 *
 * Return value is 0 if okay, -1 if not.
 */
int
pg_sockaddr_cidr_mask(struct sockaddr_storage *mask, char *numbits, int family)
{
	long		bits;
	char	   *endptr;

	if (numbits == NULL)
	{
		bits = (family == AF_INET) ? 32 : 128;
	}
	else
	{
		bits = strtol(numbits, &endptr, 10);
		if (*numbits == '\0' || *endptr != '\0')
			return -1;
	}

	switch (family)
	{
		case AF_INET:
			{
				struct sockaddr_in mask4;
				long		maskl;

				if (bits < 0 || bits > 32)
					return -1;
				memset(&mask4, 0, sizeof(mask4));
				/* avoid "x << 32", which is not portable */
				if (bits > 0)
					maskl = (0xffffffffUL << (32 - (int) bits))
						& 0xffffffffUL;
				else
					maskl = 0;
				mask4.sin_addr.s_addr = pg_hton32(maskl);
				memcpy(mask, &mask4, sizeof(mask4));
				break;
			}

		case AF_INET6:
			{
				struct sockaddr_in6 mask6;
				int			i;

				if (bits < 0 || bits > 128)
					return -1;
				memset(&mask6, 0, sizeof(mask6));
				for (i = 0; i < 16; i++)
				{
					if (bits <= 0)
						mask6.sin6_addr.s6_addr[i] = 0;
					else if (bits >= 8)
						mask6.sin6_addr.s6_addr[i] = 0xff;
					else
					{
						mask6.sin6_addr.s6_addr[i] =
							(0xff << (8 - (int) bits)) & 0xff;
					}
					bits -= 8;
				}
				memcpy(mask, &mask6, sizeof(mask6));
				break;
			}

		default:
			return -1;
	}

	mask->ss_family = family;
	return 0;
}


/*
 * Run the callback function for the addr/mask, after making sure the
 * mask is sane for the addr.
 */
static void
run_ifaddr_callback(PgIfAddrCallback callback, void *cb_data,
					struct sockaddr *addr, struct sockaddr *mask)
{
	struct sockaddr_storage fullmask;

	if (!addr)
		return;

	/* Check that the mask is valid */
	if (mask)
	{
		if (mask->sa_family != addr->sa_family)
		{
			mask = NULL;
		}
		else if (mask->sa_family == AF_INET)
		{
			if (((struct sockaddr_in *) mask)->sin_addr.s_addr == INADDR_ANY)
				mask = NULL;
		}
		else if (mask->sa_family == AF_INET6)
		{
			if (IN6_IS_ADDR_UNSPECIFIED(&((struct sockaddr_in6 *) mask)->sin6_addr))
				mask = NULL;
		}
	}

	/* If mask is invalid, generate our own fully-set mask */
	if (!mask)
	{
		pg_sockaddr_cidr_mask(&fullmask, NULL, addr->sa_family);
		mask = (struct sockaddr *) &fullmask;
	}

	(*callback) (addr, mask, cb_data);
}

/* ---- VERBATIM src/backend/libpq/ifaddr.c lines 294-309 (the
 * HAVE_GETIFADDRS pg_foreach_ifaddr; both macOS and Linux take it) ---- */
int
pg_foreach_ifaddr(PgIfAddrCallback callback, void *cb_data)
{
	struct ifaddrs *ifa,
			   *l;

	if (getifaddrs(&ifa) < 0)
		return -1;

	for (l = ifa; l; l = l->ifa_next)
		run_ifaddr_callback(callback, cb_data,
							l->ifa_addr, l->ifa_netmask);

	freeifaddrs(ifa);
	return 0;
}

/* ---- VERBATIM src/common/stringinfo.c blocks (see header) ---- */
/*
 * initStringInfoInternal
 *
 * Initialize a StringInfoData struct (with previously undefined contents)
 * to describe an empty string.
 * The initial memory allocation size is specified by 'initsize'.
 * The valid range for 'initsize' is 1 to MaxAllocSize.
 */
static inline void
initStringInfoInternal(StringInfo str, int initsize)
{
	Assert(initsize >= 1 && initsize <= MaxAllocSize);

	str->data = (char *) palloc(initsize);
	str->maxlen = initsize;
	resetStringInfo(str);
}
/*
 * initStringInfo
 *
 * Initialize a StringInfoData struct (with previously undefined contents)
 * to describe an empty string.
 */
void
initStringInfo(StringInfo str)
{
	initStringInfoInternal(str, STRINGINFO_DEFAULT_SIZE);
}
/*
 * resetStringInfo
 *
 * Reset the StringInfo: the data buffer remains valid, but its
 * previous content, if any, is cleared.
 *
 * Read-only StringInfos as initialized by initReadOnlyStringInfo cannot be
 * reset.
 */
void
resetStringInfo(StringInfo str)
{
	/* don't allow resets of read-only StringInfos */
	Assert(str->maxlen != 0);

	str->data[0] = '\0';
	str->len = 0;
	str->cursor = 0;
}
/*
 * appendStringInfoChar
 *
 * Append a single byte to str.
 * Like appendStringInfo(str, "%c", ch) but much faster.
 */
void
appendStringInfoChar(StringInfo str, char ch)
{
	/* Make more room if needed */
	if (str->len + 1 >= str->maxlen)
		enlargeStringInfo(str, 1);

	/* OK, append the character */
	str->data[str->len] = ch;
	str->len++;
	str->data[str->len] = '\0';
}
/*
 * appendBinaryStringInfo
 *
 * Append arbitrary binary data to a StringInfo, allocating more space
 * if necessary. Ensures that a trailing null byte is present.
 */
void
appendBinaryStringInfo(StringInfo str, const void *data, int datalen)
{
	Assert(str != NULL);

	/* Make more room if needed */
	enlargeStringInfo(str, datalen);

	/* OK, append the data */
	memcpy(str->data + str->len, data, datalen);
	str->len += datalen;

	/*
	 * Keep a trailing null in place, even though it's probably useless for
	 * binary data.  (Some callers are dealing with text but call this because
	 * their input isn't null-terminated.)
	 */
	str->data[str->len] = '\0';
}
/*
 * appendBinaryStringInfoNT
 *
 * Append arbitrary binary data to a StringInfo, allocating more space
 * if necessary. Does not ensure a trailing null-byte exists.
 */
void
appendBinaryStringInfoNT(StringInfo str, const void *data, int datalen)
{
	Assert(str != NULL);

	/* Make more room if needed */
	enlargeStringInfo(str, datalen);

	/* OK, append the data */
	memcpy(str->data + str->len, data, datalen);
	str->len += datalen;
}
/*
 * enlargeStringInfo
 *
 * Make sure there is enough space for 'needed' more bytes
 * ('needed' does not include the terminating null).
 *
 * External callers usually need not concern themselves with this, since
 * all stringinfo.c routines do it automatically.  However, if a caller
 * knows that a StringInfo will eventually become X bytes large, it
 * can save some palloc overhead by enlarging the buffer before starting
 * to store data in it.
 *
 * NB: In the backend, because we use repalloc() to enlarge the buffer, the
 * string buffer will remain allocated in the same memory context that was
 * current when initStringInfo was called, even if another context is now
 * current.  This is the desired and indeed critical behavior!
 */
void
enlargeStringInfo(StringInfo str, int needed)
{
	int			newlen;

	/* validate this is not a read-only StringInfo */
	Assert(str->maxlen != 0);

	/*
	 * Guard against out-of-range "needed" values.  Without this, we can get
	 * an overflow or infinite loop in the following.
	 */
	if (needed < 0)				/* should not happen */
	{
#ifndef FRONTEND
		elog(ERROR, "invalid string enlargement request size: %d", needed);
#else
		fprintf(stderr, "invalid string enlargement request size: %d\n", needed);
		exit(EXIT_FAILURE);
#endif
	}
	if (((Size) needed) >= (MaxAllocSize - (Size) str->len))
	{
#ifndef FRONTEND
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("string buffer exceeds maximum allowed length (%zu bytes)", MaxAllocSize),
				 errdetail("Cannot enlarge string buffer containing %d bytes by %d more bytes.",
						   str->len, needed)));
#else
		fprintf(stderr,
				_("string buffer exceeds maximum allowed length (%zu bytes)\n\nCannot enlarge string buffer containing %d bytes by %d more bytes.\n"),
				MaxAllocSize, str->len, needed);
		exit(EXIT_FAILURE);
#endif
	}

	needed += str->len + 1;		/* total space required now */

	/* Because of the above test, we now have needed <= MaxAllocSize */

	if (needed <= str->maxlen)
		return;					/* got enough space already */

	/*
	 * We don't want to allocate just a little more space with each append;
	 * for efficiency, double the buffer size each time it overflows.
	 * Actually, we might need to more than double it if 'needed' is big...
	 */
	newlen = 2 * str->maxlen;
	while (needed > newlen)
		newlen = 2 * newlen;

	/*
	 * Clamp to MaxAllocSize in case we went past it.  Note we are assuming
	 * here that MaxAllocSize <= INT_MAX/2, else the above loop could
	 * overflow.  We will still have newlen >= needed.
	 */
	if (newlen > (int) MaxAllocSize)
		newlen = (int) MaxAllocSize;

	str->data = (char *) repalloc(str->data, newlen);

	str->maxlen = newlen;
}

/* ---- VERBATIM src/include/libpq/pqformat.h lines 33-188 ---- */
/*
 * Append a [u]int8 to a StringInfo buffer, which already has enough space
 * preallocated.
 *
 * The use of pg_restrict allows the compiler to optimize the code based on
 * the assumption that buf, buf->len, buf->data and *buf->data don't
 * overlap. Without the annotation buf->len etc cannot be kept in a register
 * over subsequent pq_writeintN calls.
 *
 * The use of StringInfoData * rather than StringInfo is due to MSVC being
 * overly picky and demanding a * before a restrict.
 */
static inline void
pq_writeint8(StringInfoData *pg_restrict buf, uint8 i)
{
	uint8		ni = i;

	Assert(buf->len + (int) sizeof(uint8) <= buf->maxlen);
	memcpy((char *pg_restrict) (buf->data + buf->len), &ni, sizeof(uint8));
	buf->len += sizeof(uint8);
}

/*
 * Append a [u]int16 to a StringInfo buffer, which already has enough space
 * preallocated.
 */
static inline void
pq_writeint16(StringInfoData *pg_restrict buf, uint16 i)
{
	uint16		ni = pg_hton16(i);

	Assert(buf->len + (int) sizeof(uint16) <= buf->maxlen);
	memcpy((char *pg_restrict) (buf->data + buf->len), &ni, sizeof(uint16));
	buf->len += sizeof(uint16);
}

/*
 * Append a [u]int32 to a StringInfo buffer, which already has enough space
 * preallocated.
 */
static inline void
pq_writeint32(StringInfoData *pg_restrict buf, uint32 i)
{
	uint32		ni = pg_hton32(i);

	Assert(buf->len + (int) sizeof(uint32) <= buf->maxlen);
	memcpy((char *pg_restrict) (buf->data + buf->len), &ni, sizeof(uint32));
	buf->len += sizeof(uint32);
}

/*
 * Append a [u]int64 to a StringInfo buffer, which already has enough space
 * preallocated.
 */
static inline void
pq_writeint64(StringInfoData *pg_restrict buf, uint64 i)
{
	uint64		ni = pg_hton64(i);

	Assert(buf->len + (int) sizeof(uint64) <= buf->maxlen);
	memcpy((char *pg_restrict) (buf->data + buf->len), &ni, sizeof(uint64));
	buf->len += sizeof(uint64);
}

/*
 * Append a null-terminated text string (with conversion) to a buffer with
 * preallocated space.
 *
 * NB: The pre-allocated space needs to be sufficient for the string after
 * converting to client encoding.
 *
 * NB: passed text string must be null-terminated, and so is the data
 * sent to the frontend.
 */
static inline void
pq_writestring(StringInfoData *pg_restrict buf, const char *pg_restrict str)
{
	int			slen = strlen(str);
	char	   *p;

	p = pg_server_to_client(str, slen);
	if (p != str)				/* actual conversion has been done? */
		slen = strlen(p);

	Assert(buf->len + slen + 1 <= buf->maxlen);

	memcpy(((char *pg_restrict) buf->data + buf->len), p, slen + 1);
	buf->len += slen + 1;

	if (p != str)
		pfree(p);
}

/* append a binary [u]int8 to a StringInfo buffer */
static inline void
pq_sendint8(StringInfo buf, uint8 i)
{
	enlargeStringInfo(buf, sizeof(uint8));
	pq_writeint8(buf, i);
}

/* append a binary [u]int16 to a StringInfo buffer */
static inline void
pq_sendint16(StringInfo buf, uint16 i)
{
	enlargeStringInfo(buf, sizeof(uint16));
	pq_writeint16(buf, i);
}

/* append a binary [u]int32 to a StringInfo buffer */
static inline void
pq_sendint32(StringInfo buf, uint32 i)
{
	enlargeStringInfo(buf, sizeof(uint32));
	pq_writeint32(buf, i);
}

/* append a binary [u]int64 to a StringInfo buffer */
static inline void
pq_sendint64(StringInfo buf, uint64 i)
{
	enlargeStringInfo(buf, sizeof(uint64));
	pq_writeint64(buf, i);
}

/* append a binary byte to a StringInfo buffer */
static inline void
pq_sendbyte(StringInfo buf, uint8 byt)
{
	pq_sendint8(buf, byt);
}

/*
 * Append a binary integer to a StringInfo buffer
 *
 * This function is deprecated; prefer use of the functions above.
 */
static inline void
pq_sendint(StringInfo buf, uint32 i, int b)
{
	switch (b)
	{
		case 1:
			pq_sendint8(buf, (uint8) i);
			break;
		case 2:
			pq_sendint16(buf, (uint16) i);
			break;
		case 4:
			pq_sendint32(buf, (uint32) i);
			break;
		default:
			elog(ERROR, "unsupported integer size %d", b);
			break;
	}
}

/* ---- VERBATIM src/backend/libpq/pqformat.c lines 83-641 ---- */
/* --------------------------------
 *		pq_beginmessage		- initialize for sending a message
 * --------------------------------
 */
void
pq_beginmessage(StringInfo buf, char msgtype)
{
	initStringInfo(buf);

	/*
	 * We stash the message type into the buffer's cursor field, expecting
	 * that the pq_sendXXX routines won't touch it.  We could alternatively
	 * make it the first byte of the buffer contents, but this seems easier.
	 */
	buf->cursor = msgtype;
}

/* --------------------------------

 *		pq_beginmessage_reuse - initialize for sending a message, reuse buffer
 *
 * This requires the buffer to be allocated in a sufficiently long-lived
 * memory context.
 * --------------------------------
 */
void
pq_beginmessage_reuse(StringInfo buf, char msgtype)
{
	resetStringInfo(buf);

	/*
	 * We stash the message type into the buffer's cursor field, expecting
	 * that the pq_sendXXX routines won't touch it.  We could alternatively
	 * make it the first byte of the buffer contents, but this seems easier.
	 */
	buf->cursor = msgtype;
}

/* --------------------------------
 *		pq_sendbytes	- append raw data to a StringInfo buffer
 * --------------------------------
 */
void
pq_sendbytes(StringInfo buf, const void *data, int datalen)
{
	/* use variant that maintains a trailing null-byte, out of caution */
	appendBinaryStringInfo(buf, data, datalen);
}

/* --------------------------------
 *		pq_sendcountedtext - append a counted text string (with character set conversion)
 *
 * The data sent to the frontend by this routine is a 4-byte count field
 * followed by the string.  The count does not include itself, as required by
 * protocol version 3.0.  The passed text string need not be null-terminated,
 * and the data sent to the frontend isn't either.
 * --------------------------------
 */
void
pq_sendcountedtext(StringInfo buf, const char *str, int slen)
{
	char	   *p;

	p = pg_server_to_client(str, slen);
	if (p != str)				/* actual conversion has been done? */
	{
		slen = strlen(p);
		pq_sendint32(buf, slen);
		appendBinaryStringInfoNT(buf, p, slen);
		pfree(p);
	}
	else
	{
		pq_sendint32(buf, slen);
		appendBinaryStringInfoNT(buf, str, slen);
	}
}

/* --------------------------------
 *		pq_sendtext		- append a text string (with conversion)
 *
 * The passed text string need not be null-terminated, and the data sent
 * to the frontend isn't either.  Note that this is not actually useful
 * for direct frontend transmissions, since there'd be no way for the
 * frontend to determine the string length.  But it is useful for binary
 * format conversions.
 * --------------------------------
 */
void
pq_sendtext(StringInfo buf, const char *str, int slen)
{
	char	   *p;

	p = pg_server_to_client(str, slen);
	if (p != str)				/* actual conversion has been done? */
	{
		slen = strlen(p);
		appendBinaryStringInfo(buf, p, slen);
		pfree(p);
	}
	else
		appendBinaryStringInfo(buf, str, slen);
}

/* --------------------------------
 *		pq_sendstring	- append a null-terminated text string (with conversion)
 *
 * NB: passed text string must be null-terminated, and so is the data
 * sent to the frontend.
 * --------------------------------
 */
void
pq_sendstring(StringInfo buf, const char *str)
{
	int			slen = strlen(str);
	char	   *p;

	p = pg_server_to_client(str, slen);
	if (p != str)				/* actual conversion has been done? */
	{
		slen = strlen(p);
		appendBinaryStringInfoNT(buf, p, slen + 1);
		pfree(p);
	}
	else
		appendBinaryStringInfoNT(buf, str, slen + 1);
}

/* --------------------------------
 *		pq_send_ascii_string	- append a null-terminated text string (without conversion)
 *
 * This function intentionally bypasses encoding conversion, instead just
 * silently replacing any non-7-bit-ASCII characters with question marks.
 * It is used only when we are having trouble sending an error message to
 * the client with normal localization and encoding conversion.  The caller
 * should already have taken measures to ensure the string is just ASCII;
 * the extra work here is just to make certain we don't send a badly encoded
 * string to the client (which might or might not be robust about that).
 *
 * NB: passed text string must be null-terminated, and so is the data
 * sent to the frontend.
 * --------------------------------
 */
void
pq_send_ascii_string(StringInfo buf, const char *str)
{
	while (*str)
	{
		char		ch = *str++;

		if (IS_HIGHBIT_SET(ch))
			ch = '?';
		appendStringInfoCharMacro(buf, ch);
	}
	appendStringInfoChar(buf, '\0');
}

/* --------------------------------
 *		pq_sendfloat4	- append a float4 to a StringInfo buffer
 *
 * The point of this routine is to localize knowledge of the external binary
 * representation of float4, which is a component of several datatypes.
 *
 * We currently assume that float4 should be byte-swapped in the same way
 * as int4.  This rule is not perfect but it gives us portability across
 * most IEEE-float-using architectures.
 * --------------------------------
 */
void
pq_sendfloat4(StringInfo buf, float4 f)
{
	union
	{
		float4		f;
		uint32		i;
	}			swap;

	swap.f = f;
	pq_sendint32(buf, swap.i);
}

/* --------------------------------
 *		pq_sendfloat8	- append a float8 to a StringInfo buffer
 *
 * The point of this routine is to localize knowledge of the external binary
 * representation of float8, which is a component of several datatypes.
 *
 * We currently assume that float8 should be byte-swapped in the same way
 * as int8.  This rule is not perfect but it gives us portability across
 * most IEEE-float-using architectures.
 * --------------------------------
 */
void
pq_sendfloat8(StringInfo buf, float8 f)
{
	union
	{
		float8		f;
		int64		i;
	}			swap;

	swap.f = f;
	pq_sendint64(buf, swap.i);
}

/* --------------------------------
 *		pq_endmessage	- send the completed message to the frontend
 *
 * The data buffer is pfree()d, but if the StringInfo was allocated with
 * makeStringInfo then the caller must still pfree it.
 * --------------------------------
 */
void
pq_endmessage(StringInfo buf)
{
	/* msgtype was saved in cursor field */
	(void) pq_putmessage(buf->cursor, buf->data, buf->len);
	/* no need to complain about any failure, since pqcomm.c already did */
	pfree(buf->data);
	buf->data = NULL;
}

/* --------------------------------
 *		pq_endmessage_reuse	- send the completed message to the frontend
 *
 * The data buffer is *not* freed, allowing to reuse the buffer with
 * pq_beginmessage_reuse.
 --------------------------------
 */

void
pq_endmessage_reuse(StringInfo buf)
{
	/* msgtype was saved in cursor field */
	(void) pq_putmessage(buf->cursor, buf->data, buf->len);
}


/* --------------------------------
 *		pq_begintypsend		- initialize for constructing a bytea result
 * --------------------------------
 */
void
pq_begintypsend(StringInfo buf)
{
	initStringInfo(buf);
	/* Reserve four bytes for the bytea length word */
	appendStringInfoCharMacro(buf, '\0');
	appendStringInfoCharMacro(buf, '\0');
	appendStringInfoCharMacro(buf, '\0');
	appendStringInfoCharMacro(buf, '\0');
}

/* --------------------------------
 *		pq_endtypsend	- finish constructing a bytea result
 *
 * The data buffer is returned as the palloc'd bytea value.  (We expect
 * that it will be suitably aligned for this because it has been palloc'd.)
 * We assume the StringInfoData is just a local variable in the caller and
 * need not be pfree'd.
 * --------------------------------
 */
bytea *
pq_endtypsend(StringInfo buf)
{
	bytea	   *result = (bytea *) buf->data;

	/* Insert correct length into bytea length word */
	Assert(buf->len >= VARHDRSZ);
	SET_VARSIZE(result, buf->len);

	return result;
}


/* --------------------------------
 *		pq_puttextmessage - generate a character set-converted message in one step
 *
 *		This is the same as the pqcomm.c routine pq_putmessage, except that
 *		the message body is a null-terminated string to which encoding
 *		conversion applies.
 * --------------------------------
 */
void
pq_puttextmessage(char msgtype, const char *str)
{
	int			slen = strlen(str);
	char	   *p;

	p = pg_server_to_client(str, slen);
	if (p != str)				/* actual conversion has been done? */
	{
		(void) pq_putmessage(msgtype, p, strlen(p) + 1);
		pfree(p);
		return;
	}
	(void) pq_putmessage(msgtype, str, slen + 1);
}


/* --------------------------------
 *		pq_putemptymessage - convenience routine for message with empty body
 * --------------------------------
 */
void
pq_putemptymessage(char msgtype)
{
	(void) pq_putmessage(msgtype, NULL, 0);
}


/* --------------------------------
 *		pq_getmsgbyte	- get a raw byte from a message buffer
 * --------------------------------
 */
int
pq_getmsgbyte(StringInfo msg)
{
	if (msg->cursor >= msg->len)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("no data left in message")));
	return (unsigned char) msg->data[msg->cursor++];
}

/* --------------------------------
 *		pq_getmsgint	- get a binary integer from a message buffer
 *
 *		Values are treated as unsigned.
 * --------------------------------
 */
unsigned int
pq_getmsgint(StringInfo msg, int b)
{
	unsigned int result;
	unsigned char n8;
	uint16		n16;
	uint32		n32;

	switch (b)
	{
		case 1:
			pq_copymsgbytes(msg, &n8, 1);
			result = n8;
			break;
		case 2:
			pq_copymsgbytes(msg, &n16, 2);
			result = pg_ntoh16(n16);
			break;
		case 4:
			pq_copymsgbytes(msg, &n32, 4);
			result = pg_ntoh32(n32);
			break;
		default:
			elog(ERROR, "unsupported integer size %d", b);
			result = 0;			/* keep compiler quiet */
			break;
	}
	return result;
}

/* --------------------------------
 *		pq_getmsgint64	- get a binary 8-byte int from a message buffer
 *
 * It is tempting to merge this with pq_getmsgint, but we'd have to make the
 * result int64 for all data widths --- that could be a big performance
 * hit on machines where int64 isn't efficient.
 * --------------------------------
 */
int64
pq_getmsgint64(StringInfo msg)
{
	uint64		n64;

	pq_copymsgbytes(msg, &n64, sizeof(n64));

	return pg_ntoh64(n64);
}

/* --------------------------------
 *		pq_getmsgfloat4 - get a float4 from a message buffer
 *
 * See notes for pq_sendfloat4.
 * --------------------------------
 */
float4
pq_getmsgfloat4(StringInfo msg)
{
	union
	{
		float4		f;
		uint32		i;
	}			swap;

	swap.i = pq_getmsgint(msg, 4);
	return swap.f;
}

/* --------------------------------
 *		pq_getmsgfloat8 - get a float8 from a message buffer
 *
 * See notes for pq_sendfloat8.
 * --------------------------------
 */
float8
pq_getmsgfloat8(StringInfo msg)
{
	union
	{
		float8		f;
		int64		i;
	}			swap;

	swap.i = pq_getmsgint64(msg);
	return swap.f;
}

/* --------------------------------
 *		pq_getmsgbytes	- get raw data from a message buffer
 *
 *		Returns a pointer directly into the message buffer; note this
 *		may not have any particular alignment.
 * --------------------------------
 */
const char *
pq_getmsgbytes(StringInfo msg, int datalen)
{
	const char *result;

	if (datalen < 0 || datalen > (msg->len - msg->cursor))
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("insufficient data left in message")));
	result = &msg->data[msg->cursor];
	msg->cursor += datalen;
	return result;
}

/* --------------------------------
 *		pq_copymsgbytes - copy raw data from a message buffer
 *
 *		Same as above, except data is copied to caller's buffer.
 * --------------------------------
 */
void
pq_copymsgbytes(StringInfo msg, void *buf, int datalen)
{
	if (datalen < 0 || datalen > (msg->len - msg->cursor))
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("insufficient data left in message")));
	memcpy(buf, &msg->data[msg->cursor], datalen);
	msg->cursor += datalen;
}

/* --------------------------------
 *		pq_getmsgtext	- get a counted text string (with conversion)
 *
 *		Always returns a pointer to a freshly palloc'd result.
 *		The result has a trailing null, *and* we return its strlen in *nbytes.
 * --------------------------------
 */
char *
pq_getmsgtext(StringInfo msg, int rawbytes, int *nbytes)
{
	char	   *str;
	char	   *p;

	if (rawbytes < 0 || rawbytes > (msg->len - msg->cursor))
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("insufficient data left in message")));
	str = &msg->data[msg->cursor];
	msg->cursor += rawbytes;

	p = pg_client_to_server(str, rawbytes);
	if (p != str)				/* actual conversion has been done? */
		*nbytes = strlen(p);
	else
	{
		p = (char *) palloc(rawbytes + 1);
		memcpy(p, str, rawbytes);
		p[rawbytes] = '\0';
		*nbytes = rawbytes;
	}
	return p;
}

/* --------------------------------
 *		pq_getmsgstring - get a null-terminated text string (with conversion)
 *
 *		May return a pointer directly into the message buffer, or a pointer
 *		to a palloc'd conversion result.
 * --------------------------------
 */
const char *
pq_getmsgstring(StringInfo msg)
{
	char	   *str;
	int			slen;

	str = &msg->data[msg->cursor];

	/*
	 * It's safe to use strlen() here because a StringInfo is guaranteed to
	 * have a trailing null byte.  But check we found a null inside the
	 * message.
	 */
	slen = strlen(str);
	if (msg->cursor + slen >= msg->len)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("invalid string in message")));
	msg->cursor += slen + 1;

	return pg_client_to_server(str, slen);
}

/* --------------------------------
 *		pq_getmsgrawstring - get a null-terminated text string - NO conversion
 *
 *		Returns a pointer directly into the message buffer.
 * --------------------------------
 */
const char *
pq_getmsgrawstring(StringInfo msg)
{
	char	   *str;
	int			slen;

	str = &msg->data[msg->cursor];

	/*
	 * It's safe to use strlen() here because a StringInfo is guaranteed to
	 * have a trailing null byte.  But check we found a null inside the
	 * message.
	 */
	slen = strlen(str);
	if (msg->cursor + slen >= msg->len)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("invalid string in message")));
	msg->cursor += slen + 1;

	return str;
}

/* --------------------------------
 *		pq_getmsgend	- verify message fully consumed
 * --------------------------------
 */
void
pq_getmsgend(StringInfo msg)
{
	if (msg->cursor != msg->len)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("invalid message format")));
}

/* ========== shim function bodies (see header comment) ========== */

static char *
pg_nf_client_to_server(const char *s, int len)
{
	int			i;

	if (len <= 0)
		return (char *) s;
	/* pg_verify_mbstr(SQL_ASCII): only an embedded NUL is invalid */
	for (i = 0; i < len; i++)
	{
		if (s[i] == '\0')
			pg_nf_raise(PG_NF_ERR_CHAR_REPERTOIRE);
	}
	return (char *) s;
}

static char *
pg_nf_server_to_client(const char *s, int len)
{
	int			i;
	char	   *p;

	if (len <= 0)
		return (char *) s;
	if (!pg_nf_convert)
		return (char *) s;
	for (i = 0; i < len; i++)
	{
		if (s[i] == '\0')
			return (char *) s;	/* driver arms convert for NUL-free only */
	}
	/* identity conversion: fresh NUL-terminated copy (p != s) */
	p = malloc(len + 1);
	memcpy(p, s, len);
	p[len] = '\0';
	return p;
}

/* last captured pq_putmessage frame */
static _Thread_local char pg_nf_put_type;
static _Thread_local unsigned char *pg_nf_put_body;
static _Thread_local size_t pg_nf_put_len;
static _Thread_local int pg_nf_put_seen;

static int
pg_nf_putmessage(char msgtype, const char *s, size_t len)
{
	free(pg_nf_put_body);
	pg_nf_put_body = malloc(len ? len : 1);
	if (len > 0)
		memcpy(pg_nf_put_body, s, len);
	pg_nf_put_type = msgtype;
	pg_nf_put_len = len;
	pg_nf_put_seen = 1;
	return 0;
}

/* ========== SECTION D: fuzz-facing driver entries (NOT Postgres code) ==== */

#define PG_NF_OP(body) \
	do { \
		pg_nf_errcode = 0; \
		if (setjmp(pg_nf_jmp) != 0) \
			return pg_nf_errcode; \
		body; \
		return 0; \
	} while (0)

void
pg_nf_set_convert(int flag)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	pg_nf_convert = flag;
}

/* ---- ifaddr ---- */

/* family: 0 = AF_INET (4 addr bytes), 1 = AF_INET6 (16 addr bytes) */
static void
pg_nf_mk_sockaddr(struct sockaddr_storage *ss, int family, const uint8_t *addr)
{
	memset(ss, 0, sizeof(*ss));
	if (family == 0)
	{
		struct sockaddr_in sa4;

		memset(&sa4, 0, sizeof(sa4));
		sa4.sin_family = AF_INET;
		memcpy(&sa4.sin_addr, addr, 4);
		memcpy(ss, &sa4, sizeof(sa4));
	}
	else
	{
		struct sockaddr_in6 sa6;

		memset(&sa6, 0, sizeof(sa6));
		sa6.sin6_family = AF_INET6;
		memcpy(&sa6.sin6_addr, addr, 16);
		memcpy(ss, &sa6, sizeof(sa6));
	}
}

int
pg_nf_range(int family, const uint8_t *addr, const uint8_t *netaddr,
			const uint8_t *netmask)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	struct sockaddr_storage a, n, m;

	pg_nf_mk_sockaddr(&a, family, addr);
	pg_nf_mk_sockaddr(&n, family, netaddr);
	pg_nf_mk_sockaddr(&m, family, netmask);
	return nf_pg_range_sockaddr(&a, &n, &m);
}

/* the vendored else-arm: an ss_family that is neither INET nor INET6 */
int
pg_nf_range_other(void)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	struct sockaddr_storage a, n, m;

	memset(&a, 0, sizeof(a));
	memset(&n, 0, sizeof(n));
	memset(&m, 0, sizeof(m));
	a.ss_family = AF_UNSPEC;
	return nf_pg_range_sockaddr(&a, &n, &m);
}

/*
 * family_sel: 0 = AF_INET, 1 = AF_INET6, 2 = other (AF_UNSPEC).
 * numbits NULL models C's NULL. On success (returns 0) the mask address
 * bytes are written to out (4 or 16 bytes by family).
 */
int
pg_nf_cidr_mask(const char *numbits, int family_sel, uint8_t *out)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	struct sockaddr_storage mask;
	int			family;
	int			rc;

	family = family_sel == 0 ? AF_INET : family_sel == 1 ? AF_INET6 : AF_UNSPEC;
	memset(&mask, 0, sizeof(mask));
	rc = nf_pg_sockaddr_cidr_mask(&mask, (char *) numbits, family);
	if (rc != 0)
		return rc;
	if (family == AF_INET)
	{
		struct sockaddr_in sa4;

		memcpy(&sa4, &mask, sizeof(sa4));
		memcpy(out, &sa4.sin_addr, 4);
	}
	else
	{
		struct sockaddr_in6 sa6;

		memcpy(&sa6, &mask, sizeof(sa6));
		memcpy(out, &sa6.sin6_addr, 16);
	}
	return 0;
}

/* foreach: collect (family, addr, mask) for IP entries, in list order.
 * Filtering non-IP families at the collector mirrors the Rust decode
 * filter (real PG callbacks inspect sa_family the same way). */
typedef struct
{
	uint8_t		fam;			/* 4 or 6 */
	uint8_t		addr[16];
	uint8_t		mask[16];
} pg_nf_if_entry;

typedef struct
{
	pg_nf_if_entry *ents;
	int			cap;
	int			count;
} pg_nf_if_acc;

static void
pg_nf_if_cb(struct sockaddr *addr, struct sockaddr *mask, void *cb_data)
{
	pg_nf_if_acc *acc = (pg_nf_if_acc *) cb_data;
	pg_nf_if_entry *e;

	if (addr == NULL)
		return;
	if (acc->count >= acc->cap)
		return;
	e = &acc->ents[acc->count];
	memset(e, 0, sizeof(*e));
	if (addr->sa_family == AF_INET)
	{
		e->fam = 4;
		memcpy(e->addr, &((struct sockaddr_in *) addr)->sin_addr, 4);
		memcpy(e->mask, &((struct sockaddr_in *) mask)->sin_addr, 4);
	}
	else if (addr->sa_family == AF_INET6)
	{
		e->fam = 6;
		memcpy(e->addr, &((struct sockaddr_in6 *) addr)->sin6_addr, 16);
		memcpy(e->mask, &((struct sockaddr_in6 *) mask)->sin6_addr, 16);
	}
	else
		return;
	acc->count++;
}

/*
 * Drive the vendored run_ifaddr_callback directly (the Rust fuzz conduit
 * run_ifaddr_callback_for_fuzz mirrors this). addr_family: 0 v4 / 1 v6;
 * mask_kind: 0 = NULL mask, 1 = v4 mask, 2 = v6 mask, 3 = AF_UNSPEC mask
 * (the family-mismatch arm). Returns the single collected entry.
 */
int
pg_nf_run_cb(int addr_family, const uint8_t *addr, int mask_kind,
			 const uint8_t *mask, uint8_t *out_fam, uint8_t *out_addr,
			 uint8_t *out_mask)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	struct sockaddr_storage a, m;
	pg_nf_if_entry ent;
	pg_nf_if_acc acc;

	pg_nf_mk_sockaddr(&a, addr_family, addr);
	acc.ents = &ent;
	acc.cap = 1;
	acc.count = 0;
	if (mask_kind == 0)
		run_ifaddr_callback(pg_nf_if_cb, &acc, (struct sockaddr *) &a, NULL);
	else if (mask_kind == 3)
	{
		memset(&m, 0, sizeof(m));
		m.ss_family = AF_UNSPEC;
		run_ifaddr_callback(pg_nf_if_cb, &acc, (struct sockaddr *) &a,
							(struct sockaddr *) &m);
	}
	else
	{
		pg_nf_mk_sockaddr(&m, mask_kind == 1 ? 0 : 1, mask);
		run_ifaddr_callback(pg_nf_if_cb, &acc, (struct sockaddr *) &a,
							(struct sockaddr *) &m);
	}
	if (acc.count != 1)
		return -1;
	*out_fam = ent.fam;
	memcpy(out_addr, ent.addr, 16);
	memcpy(out_mask, ent.mask, 16);
	return 0;
}

int
pg_nf_foreach(pg_nf_if_entry *out, int cap)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	pg_nf_if_acc acc;

	acc.ents = out;
	acc.cap = cap;
	acc.count = 0;
	if (nf_pg_foreach_ifaddr(pg_nf_if_cb, &acc) != 0)
		return -1;
	return acc.count;
}

/* ---- pqformat send/out buffer ---- */

static _Thread_local StringInfoData pg_nf_out;
static _Thread_local int pg_nf_out_live;

/* kind: 0 pq_beginmessage, 1 pq_begintypsend, 2 pq_beginmessage_reuse
 * (reuse requires a live buffer; the driver guarantees it) */
int
pg_nf_out_begin(int kind, uint8_t msgtype)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	if (kind != 2 && pg_nf_out_live)
	{
		free(pg_nf_out.data);
		pg_nf_out_live = 0;
	}
	pg_nf_errcode = 0;
	if (setjmp(pg_nf_jmp) != 0)
		return pg_nf_errcode;
	if (kind == 0)
		nf_pq_beginmessage(&pg_nf_out, (char) msgtype);
	else if (kind == 1)
		nf_pq_begintypsend(&pg_nf_out);
	else
		nf_pq_beginmessage_reuse(&pg_nf_out, (char) msgtype);
	pg_nf_out_live = 1;
	return 0;
}

const char *
pg_nf_out_get(int *len, int *maxlen, int *cursor)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	*len = pg_nf_out.len;
	*maxlen = pg_nf_out.maxlen;
	*cursor = pg_nf_out.cursor;
	return pg_nf_out.data;
}

int
pg_nf_out_enlarge(int needed)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	PG_NF_OP(nf_enlargeStringInfo(&pg_nf_out, needed));
}

int
pg_nf_sendbyte(uint8_t b)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	PG_NF_OP(pq_sendbyte(&pg_nf_out, b));
}

int
pg_nf_sendint(uint32_t i, int b)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	PG_NF_OP(pq_sendint(&pg_nf_out, i, b));
}

int
pg_nf_sendint8(uint8_t i)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	PG_NF_OP(pq_sendint8(&pg_nf_out, i));
}

int
pg_nf_sendint16(uint16_t i)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	PG_NF_OP(pq_sendint16(&pg_nf_out, i));
}

int
pg_nf_sendint32(uint32_t i)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	PG_NF_OP(pq_sendint32(&pg_nf_out, i));
}

int
pg_nf_sendint64(uint64_t i)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	PG_NF_OP(pq_sendint64(&pg_nf_out, i));
}

int
pg_nf_sendfloat4(uint32_t bits)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	float4		f;

	memcpy(&f, &bits, 4);
	PG_NF_OP(nf_pq_sendfloat4(&pg_nf_out, f));
}

int
pg_nf_sendfloat8(uint64_t bits)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	float8		f;

	memcpy(&f, &bits, 8);
	PG_NF_OP(nf_pq_sendfloat8(&pg_nf_out, f));
}

int
pg_nf_sendbytes(const uint8_t *data, int datalen)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	PG_NF_OP(nf_pq_sendbytes(&pg_nf_out, data, datalen));
}

int
pg_nf_sendtext(const uint8_t *data, int datalen)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	PG_NF_OP(nf_pq_sendtext(&pg_nf_out, (const char *) data, datalen));
}

int
pg_nf_sendcountedtext(const uint8_t *data, int datalen)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	PG_NF_OP(nf_pq_sendcountedtext(&pg_nf_out, (const char *) data, datalen));
}

int
pg_nf_sendstring(const char *s)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	PG_NF_OP(nf_pq_sendstring(&pg_nf_out, s));
}

int
pg_nf_send_ascii_string(const char *s)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	PG_NF_OP(nf_pq_send_ascii_string(&pg_nf_out, s));
}

/* width: 1/2/4/8; caller pre-enlarged (pq_writeintN contract) */
int
pg_nf_writeint(int width, uint64_t v)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	pg_nf_errcode = 0;
	if (setjmp(pg_nf_jmp) != 0)
		return pg_nf_errcode;
	switch (width)
	{
		case 1:
			pq_writeint8(&pg_nf_out, (uint8) v);
			break;
		case 2:
			pq_writeint16(&pg_nf_out, (uint16) v);
			break;
		case 4:
			pq_writeint32(&pg_nf_out, (uint32) v);
			break;
		default:
			pq_writeint64(&pg_nf_out, (uint64) v);
			break;
	}
	return 0;
}

int
pg_nf_writestring(const char *s)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	PG_NF_OP(pq_writestring(&pg_nf_out, s));
}

int
pg_nf_endmessage(int reuse)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	pg_nf_errcode = 0;
	if (setjmp(pg_nf_jmp) != 0)
		return pg_nf_errcode;
	if (reuse)
		nf_pq_endmessage_reuse(&pg_nf_out);
	else
	{
		nf_pq_endmessage(&pg_nf_out);
		/* buf->data was pfree'd and NULLed by pq_endmessage */
	}
	return 0;
}

/* returns the varlena image (bytea *) and its total length */
const uint8_t *
pg_nf_endtypsend(int *lenout)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	bytea	   *result;

	result = nf_pq_endtypsend(&pg_nf_out);
	*lenout = pg_nf_out.len;
	return (const uint8_t *) result;
}

int
pg_nf_puttextmessage(uint8_t msgtype, const char *s)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	PG_NF_OP(nf_pq_puttextmessage((char) msgtype, s));
}

int
pg_nf_putemptymessage(uint8_t msgtype)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	PG_NF_OP(nf_pq_putemptymessage((char) msgtype));
}

const uint8_t *
pg_nf_put_get(int *msgtype, size_t *len)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	if (!pg_nf_put_seen)
	{
		*msgtype = -1;
		*len = 0;
		return NULL;
	}
	*msgtype = (int) (unsigned char) pg_nf_put_type;
	*len = pg_nf_put_len;
	return pg_nf_put_body;
}

void
pg_nf_put_reset(void)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	pg_nf_put_seen = 0;
	pg_nf_put_len = 0;
}

/* ---- pqformat getmsg buffer ---- */

static _Thread_local StringInfoData pg_nf_msg;
static _Thread_local int pg_nf_msg_live;

int
pg_nf_msg_set(const uint8_t *bytes, int len)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	if (pg_nf_msg_live)
	{
		free(pg_nf_msg.data);
		pg_nf_msg_live = 0;
	}
	pg_nf_errcode = 0;
	if (setjmp(pg_nf_jmp) != 0)
		return pg_nf_errcode;
	nf_initStringInfo(&pg_nf_msg);
	nf_appendBinaryStringInfo(&pg_nf_msg, bytes, len);
	pg_nf_msg_live = 1;
	return 0;
}

int
pg_nf_msg_cursor(void)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return pg_nf_msg.cursor;
}

int
pg_nf_getmsgbyte(int *out)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	PG_NF_OP(*out = nf_pq_getmsgbyte(&pg_nf_msg));
}

int
pg_nf_getmsgint(int b, uint32_t *out)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	PG_NF_OP(*out = nf_pq_getmsgint(&pg_nf_msg, b));
}

int
pg_nf_getmsgint64(int64_t *out)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	PG_NF_OP(*out = nf_pq_getmsgint64(&pg_nf_msg));
}

int
pg_nf_getmsgfloat4(uint32_t *bits)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	pg_nf_errcode = 0;
	if (setjmp(pg_nf_jmp) != 0)
		return pg_nf_errcode;
	{
		float4		f = nf_pq_getmsgfloat4(&pg_nf_msg);

		memcpy(bits, &f, 4);
	}
	return 0;
}

int
pg_nf_getmsgfloat8(uint64_t *bits)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	pg_nf_errcode = 0;
	if (setjmp(pg_nf_jmp) != 0)
		return pg_nf_errcode;
	{
		float8		f = nf_pq_getmsgfloat8(&pg_nf_msg);

		memcpy(bits, &f, 8);
	}
	return 0;
}

int
pg_nf_getmsgbytes(int datalen, const char **out)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	PG_NF_OP(*out = nf_pq_getmsgbytes(&pg_nf_msg, datalen));
}

int
pg_nf_copymsgbytes(int datalen, uint8_t *out)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	PG_NF_OP(nf_pq_copymsgbytes(&pg_nf_msg, out, datalen));
}

/* *out is malloc'd (palloc shim or the conversion copy); caller frees */
int
pg_nf_getmsgtext(int rawbytes, char **out, int *nbytes)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	PG_NF_OP(*out = nf_pq_getmsgtext(&pg_nf_msg, rawbytes, nbytes));
}

int
pg_nf_getmsgstring(const char **out)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	PG_NF_OP(*out = nf_pq_getmsgstring(&pg_nf_msg));
}

int
pg_nf_getmsgrawstring(const char **out)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	PG_NF_OP(*out = nf_pq_getmsgrawstring(&pg_nf_msg));
}

int
pg_nf_getmsgend(void)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	PG_NF_OP(nf_pq_getmsgend(&pg_nf_msg));
}
