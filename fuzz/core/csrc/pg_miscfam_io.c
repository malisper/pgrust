/*
 * pg_miscfam_io.c: vendored PostgreSQL C oracle for the miscfam_diff
 * differential fuzz target (100%-coverage campaign, lane p1-mb-miscfam).
 * Crates under test (one selector arm each; see fuzz/core/src/miscfam_diff.rs):
 *   crates/backend/tcop/cmdtag, crates/backend/catalog/pg_class,
 *   crates/contrib/earthdistance, crates/backend/utils/misc/pg_rusage,
 *   crates/backend/access/transam/xlogstats, crates/_support/types/stringinfo.
 *
 * Provenance (all bodies VERBATIM sed-extracted from the vendor tree at
 * ~/dev/pgrust-fabled/vendor/postgres-src, Stamp-18.3, upstream sha
 * 62d6c7d3df6287f1bd83199c1a746e50d31571a0 — assembled by
 * scratchpad/assemble_miscfam.sh, never hand-typed):
 *   - src/include/tcop/cmdtag.h lines 17-33 (COMPLETION_TAG_BUFSIZE, the
 *     CommandTag enum over cmdtaglist.h, QueryCompletion).
 *   - src/include/tcop/cmdtaglist.h: copied whole to miscfam/tcop/ (data).
 *   - src/backend/tcop/cmdtag.c lines 19-163 (everything after the
 *     #includes: CommandTagBehavior, tag_behavior table, all 8 functions).
 *   - src/include/catalog/pg_class.h lines 167-176 (RELKIND_* defines).
 *   - src/backend/catalog/pg_class.c lines 19-52
 *     (errdetail_relkind_not_supported).
 *   - contrib/earthdistance/earthdistance.c lines 10-13 (M_PI guard) and
 *     19-81 (EARTH_RADIUS, TWO_PI, degtorad, geo_distance_internal).
 *     geo_decls.h Point reduced to the {float8 x, y} pair (only fields).
 *   - src/include/utils/pg_rusage.h lines 22-26 (PGRUsage).
 *   - src/backend/utils/misc/pg_rusage.c lines 33-73 (pg_rusage_show,
 *     static-prefixed). pg_rusage_init is a SHIM here (fixture copy; see
 *     below) - that is the excluded-state carve of record for this crate.
 *   - src/include/access/xlogrecord.h lines 41-53 (XLogRecord).
 *   - src/include/access/xlogreader.h lines 119-173 (DecodedBkpBlock,
 *     DecodedXLogRecord) and 404-426 (XLogRec* accessor macros).
 *   - src/include/access/xlogstats.h lines 19-37 (MAX_XLINFO_TYPES,
 *     XLogRecStats, XLogStats; FRONTEND startptr/endptr arm compiled out
 *     exactly as a backend build would).
 *   - src/backend/access/transam/xlogstats.c lines 18-96 (XLogRecGetLen,
 *     XLogRecStoreStats).
 *   - src/include/lib/stringinfo.h lines 46-54 + 112 (StringInfoData,
 *     StringInfo, STRINGINFO_DEFAULT_SIZE).
 *   - src/common/stringinfo.c: initStringInfoInternal (31-48),
 *     initStringInfo (88-99), initStringInfoExt (101-113), resetStringInfo
 *     (115-135), appendStringInfoString (222-232), appendStringInfoChar
 *     (234-252), appendStringInfoSpaces (254-272), appendBinaryStringInfo
 *     (274-298), appendBinaryStringInfoNT (300-317), enlargeStringInfo
 *     (318-400). Deliberately NOT vendored (unused by the shipped Rust
 *     crate, would need a pvsnprintf shim): makeStringInfo{,Ext,Internal},
 *     appendStringInfo, appendStringInfoVA, destroyStringInfo.
 *
 * Shims (plumbing only, never logic):
 *   - fixed-width typedefs matching c.h on LP64; Size = size_t; lengthof;
 *     Assert(noop) (release parity); likely/unlikely passthrough.
 *   - ereport/elog(ERROR) -> record an errcode class in the TLS
 *     pg_mf_errcode channel and longjmp to the armed driver entry (models
 *     PG's error longjmp). errmsg/errdetail argument strings never cross
 *     the comparison seam except pg_class's errdetail VALUE, captured into
 *     pg_mf_detail (that function's return value IS the detail string).
 *     Classes: 0 ok, 1 = ERRCODE_INTERNAL_ERROR (XX000, elog ERROR),
 *     2 = ERRCODE_PROGRAM_LIMIT_EXCEEDED (54000).
 *   - palloc/repalloc -> malloc/realloc (never fail at the sizes driven;
 *     MaxAllocSize guard fires first, exactly as in C).
 *   - pg_strcasecmp / pg_ulltoa_n: extern, resolved against the verbatim
 *     vendored copies in pg_arrayfuncs_io.c / pg_numutils.c (one verbatim
 *     definition per symbol across the whole fuzz oracle build).
 *   - MAXINT8LEN 20 (src/include/utils/builtins.h line 22, same value).
 *   - pg_rusage_init: FIXTURE COPY from pg_mf_ru1_fixture instead of
 *     getrusage/gettimeofday (environment mock; the OS clock read is the
 *     carved-out arm on the Rust side too). pg_rusage_show body verbatim.
 *   - _( ) gettext passthrough (backend NLS-off parity).
 *   - XLogReaderState reduced to { DecodedXLogRecord *record; }: every
 *     vendored macro/function here reaches only ->record (grep the macro
 *     block); the remaining ~40 reader fields are decode-machinery state
 *     with no bearing on the stats math.
 *   - Oid/BlockNumber/Buffer/ForkNumber/RelFileLocator/TimeLineID etc.:
 *     minimal layout typedefs for the DecodedBkpBlock fields the fixtures
 *     set by name (in_use, has_image, bimg_len) plus the untouched rest.
 *   - RM_MAX_ID UINT8_MAX (rmgr.h line 33 verbatim); RM_XACT_ID 1
 *     (rmgrlist.h order: RM_XLOG_ID=0, RM_XACT_ID=1).
 *   - FLEXIBLE_ARRAY_MEMBER -> empty (flexible array; the driver mallocs
 *     sizeof + (XLR_MAX_BLOCK_ID+1)*sizeof(DecodedBkpBlock)).
 *   - cmdtag/pg_class/earthdistance/rusage/xlogstats/stringinfo function
 *     names stay at file scope unprefixed (this is the only TU in the fuzz
 *     oracle build defining them; checked against csrc).
 *
 * Driver entries (SECTION D, pg_mf_* prefix) are fuzz plumbing, NOT
 * Postgres code. Every entry that can reach ereport/elog arms the jmp_buf.
 */

#include <stddef.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <setjmp.h>
#include <math.h>
#include <sys/time.h>
#include <sys/resource.h>

typedef int8_t int8;
typedef int16_t int16;
typedef int32_t int32;
typedef int64_t int64;
typedef uint8_t uint8;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;
typedef double float8;
typedef size_t Size;
typedef uint32 Oid;
typedef uint32 BlockNumber;
typedef int Buffer;
typedef uint32 TransactionId;
typedef uint64 XLogRecPtr;
typedef uint16 RepOriginId;
typedef uint8 RmgrId;
typedef uint32 pg_crc32c;
typedef int ForkNumber;			/* enum in relpath.h; int-backed */
typedef struct RelFileLocator
{
	Oid			spcOid;
	Oid			dbOid;
	Oid			relNumber;
} RelFileLocator;

#define lengthof(array) (sizeof (array) / sizeof ((array)[0]))
#define Assert(x) ((void) 0)
#define unlikely(x) (x)
#define likely(x) (x)
#define _(x) (x)
#define FLEXIBLE_ARRAY_MEMBER	/* empty */

/* memutils.h, verbatim value */
#define MaxAllocSize	((Size) 0x3fffffff) /* 1 gigabyte - 1 */

/* utils/builtins.h line 22, same value */
#define MAXINT8LEN		20

/* one verbatim definition in pg_numutils.c (global there) */
extern int	pg_ulltoa_n(uint64 value, char *a);

/* c.h verbatim values + ctype, for pg_strcasecmp (vendored below: the
 * copies elsewhere in csrc are pg_afx_-prefixed or static) */
#include <ctype.h>
#include "pg_oracle_guard.h"	/* oracle-serialization holder check */
#define HIGHBIT					(0x80)
#define IS_HIGHBIT_SET(ch)		((unsigned char)(ch) & HIGHBIT)

/* ---- SHIM: TLS error channel + longjmp (armed by driver entries) ---- */

static _Thread_local int pg_mf_errcode;	/* 0 ok / 1 XX000 / 2 54000 */
static _Thread_local jmp_buf pg_mf_jmp;
static _Thread_local char pg_mf_detail[256];

int
pg_mf_errcode_get(void)
{
	return pg_mf_errcode;
}

const char *
pg_mf_detail_get(void)
{
	return pg_mf_detail;
}

#define PG_MF_ERR_INTERNAL 1	/* XX000: elog(ERROR) default */
#define PG_MF_ERR_PROGRAM_LIMIT 2	/* 54000 */

static void
pg_mf_raise(int code)
{
	pg_mf_errcode = code;
	longjmp(pg_mf_jmp, 1);
}

/* ereport machinery: evaluate the aux-call list, then longjmp. */
static _Thread_local int pg_mf_pending_code;

static int
pg_mf_errcode_set(int code)
{
	pg_mf_pending_code = code;
	return 0;
}

static int
pg_mf_errmsg(const char *fmt,...)
{
	(void) fmt;
	return 0;
}

static int
pg_mf_errdetail(const char *fmt,...)
{
	va_list		args;

	va_start(args, fmt);
	vsnprintf(pg_mf_detail, sizeof(pg_mf_detail), fmt, args);
	va_end(args);
	return 0;
}

#define errcode(c) pg_mf_errcode_set(c)
#define errmsg pg_mf_errmsg
#define errdetail pg_mf_errdetail
#define ereport(level, rest) do { pg_mf_pending_code = PG_MF_ERR_INTERNAL; ((void) (rest)); pg_mf_raise(pg_mf_pending_code); } while (0)
#define elog(level, ...) do { pg_mf_errdetail(__VA_ARGS__); pg_mf_raise(PG_MF_ERR_INTERNAL); } while (0)
#define ERRCODE_PROGRAM_LIMIT_EXCEEDED PG_MF_ERR_PROGRAM_LIMIT
#define ERROR 21

#define palloc(n) malloc(n)
#define repalloc(p, n) realloc((p), (n))

/* ================= SECTION 1: cmdtag ================= */

/* ---- VERBATIM src/include/tcop/cmdtag.h lines 17-33 ---- */

/* ---- VERBATIM src/port/pgstrcasecmp.c lines 32-62 ---- */
/*
 * Case-independent comparison of two null-terminated strings.
 */
int
pg_strcasecmp(const char *s1, const char *s2)
{
	for (;;)
	{
		unsigned char ch1 = (unsigned char) *s1++;
		unsigned char ch2 = (unsigned char) *s2++;

		if (ch1 != ch2)
		{
			if (ch1 >= 'A' && ch1 <= 'Z')
				ch1 += 'a' - 'A';
			else if (IS_HIGHBIT_SET(ch1) && isupper(ch1))
				ch1 = tolower(ch1);

			if (ch2 >= 'A' && ch2 <= 'Z')
				ch2 += 'a' - 'A';
			else if (IS_HIGHBIT_SET(ch2) && isupper(ch2))
				ch2 = tolower(ch2);

			if (ch1 != ch2)
				return (int) ch1 - (int) ch2;
		}
		if (ch1 == 0)
			break;
	}
	return 0;
}
#define COMPLETION_TAG_BUFSIZE	64

#define PG_CMDTAG(tag, name, evtrgok, rwrok, rowcnt) \
	tag,

typedef enum CommandTag
{
#include "tcop/cmdtaglist.h"
} CommandTag;

#undef PG_CMDTAG

typedef struct QueryCompletion
{
	CommandTag	commandTag;
	uint64		nprocessed;
} QueryCompletion;

/* ---- VERBATIM src/backend/tcop/cmdtag.c lines 19-163 (post-#include) ---- */

typedef struct CommandTagBehavior
{
	const char *name;			/* tag name, e.g. "SELECT" */
	const uint8 namelen;		/* set to strlen(name) */
	const bool	event_trigger_ok;
	const bool	table_rewrite_ok;
	const bool	display_rowcount;	/* should the number of rows affected be
									 * shown in the command completion string */
} CommandTagBehavior;

#define PG_CMDTAG(tag, name, evtrgok, rwrok, rowcnt) \
	{ name, (uint8) (sizeof(name) - 1), evtrgok, rwrok, rowcnt },

static const CommandTagBehavior tag_behavior[] = {
#include "tcop/cmdtaglist.h"
};

#undef PG_CMDTAG

void
InitializeQueryCompletion(QueryCompletion *qc)
{
	qc->commandTag = CMDTAG_UNKNOWN;
	qc->nprocessed = 0;
}

const char *
GetCommandTagName(CommandTag commandTag)
{
	return tag_behavior[commandTag].name;
}

const char *
GetCommandTagNameAndLen(CommandTag commandTag, Size *len)
{
	*len = (Size) tag_behavior[commandTag].namelen;
	return tag_behavior[commandTag].name;
}

bool
command_tag_display_rowcount(CommandTag commandTag)
{
	return tag_behavior[commandTag].display_rowcount;
}

bool
command_tag_event_trigger_ok(CommandTag commandTag)
{
	return tag_behavior[commandTag].event_trigger_ok;
}

bool
command_tag_table_rewrite_ok(CommandTag commandTag)
{
	return tag_behavior[commandTag].table_rewrite_ok;
}

/*
 * Search CommandTag by name
 *
 * Returns CommandTag, or CMDTAG_UNKNOWN if not recognized
 */
CommandTag
GetCommandTagEnum(const char *commandname)
{
	const CommandTagBehavior *base,
			   *last,
			   *position;
	int			result;

	if (commandname == NULL || *commandname == '\0')
		return CMDTAG_UNKNOWN;

	base = tag_behavior;
	last = tag_behavior + lengthof(tag_behavior) - 1;
	while (last >= base)
	{
		position = base + ((last - base) >> 1);
		result = pg_strcasecmp(commandname, position->name);
		if (result == 0)
			return (CommandTag) (position - tag_behavior);
		else if (result < 0)
			last = position - 1;
		else
			base = position + 1;
	}
	return CMDTAG_UNKNOWN;
}

/*
 * BuildQueryCompletionString
 *		Build a string containing the command tag name with the
 *		QueryCompletion's nprocessed for command tags with display_rowcount
 *		set.  Returns the strlen of the constructed string.
 *
 * The caller must ensure that buff is at least COMPLETION_TAG_BUFSIZE bytes.
 *
 * If nameonly is true, then the constructed string will contain only the tag
 * name.
 */
Size
BuildQueryCompletionString(char *buff, const QueryCompletion *qc,
						   bool nameonly)
{
	CommandTag	tag = qc->commandTag;
	Size		taglen;
	const char *tagname = GetCommandTagNameAndLen(tag, &taglen);
	char	   *bufp;

	/*
	 * We assume the tagname is plain ASCII and therefore requires no encoding
	 * conversion.
	 */
	memcpy(buff, tagname, taglen);
	bufp = buff + taglen;

	/* ensure that the tagname isn't long enough to overrun the buffer */
	Assert(taglen <= COMPLETION_TAG_BUFSIZE - MAXINT8LEN - 4);

	/*
	 * In PostgreSQL versions 11 and earlier, it was possible to create a
	 * table WITH OIDS.  When inserting into such a table, INSERT used to
	 * include the Oid of the inserted record in the completion tag.  To
	 * maintain compatibility in the wire protocol, we now write a "0" (for
	 * InvalidOid) in the location where we once wrote the new record's Oid.
	 */
	if (command_tag_display_rowcount(tag) && !nameonly)
	{
		if (tag == CMDTAG_INSERT)
		{
			*bufp++ = ' ';
			*bufp++ = '0';
		}
		*bufp++ = ' ';
		bufp += pg_ulltoa_n(qc->nprocessed, bufp);
	}

	/* and finally, NUL terminate the string */
	*bufp = '\0';

	Assert((bufp - buff) == strlen(buff));

	return bufp - buff;
}

/* ================= SECTION 2: pg_class ================= */

/* ---- VERBATIM src/include/catalog/pg_class.h lines 167-176 ---- */
#define		  RELKIND_RELATION		  'r'	/* ordinary table */
#define		  RELKIND_INDEX			  'i'	/* secondary index */
#define		  RELKIND_SEQUENCE		  'S'	/* sequence object */
#define		  RELKIND_TOASTVALUE	  't'	/* for out-of-line values */
#define		  RELKIND_VIEW			  'v'	/* view */
#define		  RELKIND_MATVIEW		  'm'	/* materialized view */
#define		  RELKIND_COMPOSITE_TYPE  'c'	/* composite type */
#define		  RELKIND_FOREIGN_TABLE   'f'	/* foreign table */
#define		  RELKIND_PARTITIONED_TABLE 'p' /* partitioned table */
#define		  RELKIND_PARTITIONED_INDEX 'I' /* partitioned index */

/* ---- VERBATIM src/backend/catalog/pg_class.c lines 19-52 ---- */
/*
 * Issue an errdetail() informing that the relkind is not supported for this
 * operation.
 */
int
errdetail_relkind_not_supported(char relkind)
{
	switch (relkind)
	{
		case RELKIND_RELATION:
			return errdetail("This operation is not supported for tables.");
		case RELKIND_INDEX:
			return errdetail("This operation is not supported for indexes.");
		case RELKIND_SEQUENCE:
			return errdetail("This operation is not supported for sequences.");
		case RELKIND_TOASTVALUE:
			return errdetail("This operation is not supported for TOAST tables.");
		case RELKIND_VIEW:
			return errdetail("This operation is not supported for views.");
		case RELKIND_MATVIEW:
			return errdetail("This operation is not supported for materialized views.");
		case RELKIND_COMPOSITE_TYPE:
			return errdetail("This operation is not supported for composite types.");
		case RELKIND_FOREIGN_TABLE:
			return errdetail("This operation is not supported for foreign tables.");
		case RELKIND_PARTITIONED_TABLE:
			return errdetail("This operation is not supported for partitioned tables.");
		case RELKIND_PARTITIONED_INDEX:
			return errdetail("This operation is not supported for partitioned indexes.");
		default:
			elog(ERROR, "unrecognized relkind: '%c'", relkind);
			return 0;
	}
}

/* ================= SECTION 3: earthdistance ================= */

/* geo_decls.h Point, reduced to its two (only) fields */
typedef struct
{
	float8		x,
				y;
} Point;

/* ---- VERBATIM contrib/earthdistance/earthdistance.c lines 10-13, 19-81 ---- */
#ifndef M_PI
#define M_PI 3.14159265358979323846
#endif

/* Earth's radius is in statute miles. */
static const double EARTH_RADIUS = 3958.747716;
static const double TWO_PI = 2.0 * M_PI;


/******************************************************
 *
 * degtorad - convert degrees to radians
 *
 * arg: double, angle in degrees
 *
 * returns: double, same angle in radians
 ******************************************************/

static double
degtorad(double degrees)
{
	return (degrees / 360.0) * TWO_PI;
}

/******************************************************
 *
 * geo_distance_internal - distance between points
 *
 * args:
 *	 a pair of points - for each point,
 *	   x-coordinate is longitude in degrees west of Greenwich
 *	   y-coordinate is latitude in degrees above equator
 *
 * returns: double
 *	 distance between the points in miles on earth's surface
 ******************************************************/

static double
geo_distance_internal(Point *pt1, Point *pt2)
{
	double		long1,
				lat1,
				long2,
				lat2;
	double		longdiff;
	double		sino;

	/* convert degrees to radians */

	long1 = degtorad(pt1->x);
	lat1 = degtorad(pt1->y);

	long2 = degtorad(pt2->x);
	lat2 = degtorad(pt2->y);

	/* compute difference in longitudes - want < 180 degrees */
	longdiff = fabs(long1 - long2);
	if (longdiff > M_PI)
		longdiff = TWO_PI - longdiff;

	sino = sqrt(sin(fabs(lat1 - lat2) / 2.) * sin(fabs(lat1 - lat2) / 2.) +
				cos(lat1) * cos(lat2) * sin(longdiff / 2.) * sin(longdiff / 2.));
	if (sino > 1.)
		sino = 1.;

	return 2. * EARTH_RADIUS * asin(sino);
}

/* ================= SECTION 4: pg_rusage ================= */

/* ---- VERBATIM src/include/utils/pg_rusage.h lines 22-26 ---- */
typedef struct PGRUsage
{
	struct timeval tv;
	struct rusage ru;
} PGRUsage;

/*
 * SHIM pg_rusage_init: copy the driver-provided fixture snapshot instead of
 * reading getrusage/gettimeofday (environment mock — the OS clock read is
 * exactly the excluded-state carve on the Rust side). static-prefixed.
 */
static _Thread_local PGRUsage pg_mf_ru1_fixture;

static void
pg_rusage_init(PGRUsage *ru0)
{
	*ru0 = pg_mf_ru1_fixture;
}

/* ---- VERBATIM src/backend/utils/misc/pg_rusage.c lines 33-73
 * [static-prefixed] ---- */
static
/*
 * Compute elapsed time since ru0 usage snapshot, and format into
 * a displayable string.  Result is in a static string, which is
 * tacky, but no one ever claimed that the Postgres backend is
 * threadable...
 */
const char *
pg_rusage_show(const PGRUsage *ru0)
{
	static char result[100];
	PGRUsage	ru1;

	pg_rusage_init(&ru1);

	if (ru1.tv.tv_usec < ru0->tv.tv_usec)
	{
		ru1.tv.tv_sec--;
		ru1.tv.tv_usec += 1000000;
	}
	if (ru1.ru.ru_stime.tv_usec < ru0->ru.ru_stime.tv_usec)
	{
		ru1.ru.ru_stime.tv_sec--;
		ru1.ru.ru_stime.tv_usec += 1000000;
	}
	if (ru1.ru.ru_utime.tv_usec < ru0->ru.ru_utime.tv_usec)
	{
		ru1.ru.ru_utime.tv_sec--;
		ru1.ru.ru_utime.tv_usec += 1000000;
	}

	snprintf(result, sizeof(result),
			 _("CPU: user: %d.%02d s, system: %d.%02d s, elapsed: %d.%02d s"),
			 (int) (ru1.ru.ru_utime.tv_sec - ru0->ru.ru_utime.tv_sec),
			 (int) (ru1.ru.ru_utime.tv_usec - ru0->ru.ru_utime.tv_usec) / 10000,
			 (int) (ru1.ru.ru_stime.tv_sec - ru0->ru.ru_stime.tv_sec),
			 (int) (ru1.ru.ru_stime.tv_usec - ru0->ru.ru_stime.tv_usec) / 10000,
			 (int) (ru1.tv.tv_sec - ru0->tv.tv_sec),
			 (int) (ru1.tv.tv_usec - ru0->tv.tv_usec) / 10000);

	return result;
}

/* ================= SECTION 5: xlogstats ================= */

/* rmgr.h line 33 verbatim; rmgrlist.h order: RM_XLOG_ID=0, RM_XACT_ID=1 */
#define RM_MAX_ID			UINT8_MAX
#define RM_XACT_ID			1
#define XLR_MAX_BLOCK_ID	32

/* ---- VERBATIM src/include/access/xlogrecord.h lines 41-53 ---- */
typedef struct XLogRecord
{
	uint32		xl_tot_len;		/* total len of entire record */
	TransactionId xl_xid;		/* xact id */
	XLogRecPtr	xl_prev;		/* ptr to previous record in log */
	uint8		xl_info;		/* flag bits, see below */
	RmgrId		xl_rmid;		/* resource manager for this record */
	/* 2 bytes of padding here, initialize to zero */
	pg_crc32c	xl_crc;			/* CRC for this record */

	/* XLogRecordBlockHeaders and XLogRecordDataHeader follow, no padding */

} XLogRecord;

/* ---- VERBATIM src/include/access/xlogreader.h lines 119-173 ---- */
typedef struct
{
	/* Is this block ref in use? */
	bool		in_use;

	/* Identify the block this refers to */
	RelFileLocator rlocator;
	ForkNumber	forknum;
	BlockNumber blkno;

	/* Prefetching workspace. */
	Buffer		prefetch_buffer;

	/* copy of the fork_flags field from the XLogRecordBlockHeader */
	uint8		flags;

	/* Information on full-page image, if any */
	bool		has_image;		/* has image, even for consistency checking */
	bool		apply_image;	/* has image that should be restored */
	char	   *bkp_image;
	uint16		hole_offset;
	uint16		hole_length;
	uint16		bimg_len;
	uint8		bimg_info;

	/* Buffer holding the rmgr-specific data associated with this block */
	bool		has_data;
	char	   *data;
	uint16		data_len;
	uint16		data_bufsz;
} DecodedBkpBlock;

/*
 * The decoded contents of a record.  This occupies a contiguous region of
 * memory, with main_data and blocks[n].data pointing to memory after the
 * members declared here.
 */
typedef struct DecodedXLogRecord
{
	/* Private member used for resource management. */
	size_t		size;			/* total size of decoded record */
	bool		oversized;		/* outside the regular decode buffer? */
	struct DecodedXLogRecord *next; /* decoded record queue link */

	/* Public members. */
	XLogRecPtr	lsn;			/* location */
	XLogRecPtr	next_lsn;		/* location of next record */
	XLogRecord	header;			/* header */
	RepOriginId record_origin;
	TransactionId toplevel_xid; /* XID of top-level transaction */
	char	   *main_data;		/* record's main data portion */
	uint32		main_data_len;	/* main data portion's length */
	int			max_block_id;	/* highest block_id in use (-1 if none) */
	DecodedBkpBlock blocks[FLEXIBLE_ARRAY_MEMBER];
} DecodedXLogRecord;

/*
 * SHIM XLogReaderState: reduced to the single member the vendored macros
 * and xlogstats.c bodies reach (->record). See header comment.
 */
typedef struct XLogReaderState
{
	DecodedXLogRecord *record;
} XLogReaderState;

/* ---- VERBATIM src/include/access/xlogreader.h lines 404-426 ---- */
/*
 * Macros that provide access to parts of the record most recently returned by
 * XLogReadRecord() or XLogNextRecord().
 */
#define XLogRecGetTotalLen(decoder) ((decoder)->record->header.xl_tot_len)
#define XLogRecGetPrev(decoder) ((decoder)->record->header.xl_prev)
#define XLogRecGetInfo(decoder) ((decoder)->record->header.xl_info)
#define XLogRecGetRmid(decoder) ((decoder)->record->header.xl_rmid)
#define XLogRecGetXid(decoder) ((decoder)->record->header.xl_xid)
#define XLogRecGetOrigin(decoder) ((decoder)->record->record_origin)
#define XLogRecGetTopXid(decoder) ((decoder)->record->toplevel_xid)
#define XLogRecGetData(decoder) ((decoder)->record->main_data)
#define XLogRecGetDataLen(decoder) ((decoder)->record->main_data_len)
#define XLogRecHasAnyBlockRefs(decoder) ((decoder)->record->max_block_id >= 0)
#define XLogRecMaxBlockId(decoder) ((decoder)->record->max_block_id)
#define XLogRecGetBlock(decoder, i) (&(decoder)->record->blocks[(i)])
#define XLogRecHasBlockRef(decoder, block_id)			\
	(((decoder)->record->max_block_id >= (block_id)) &&	\
	 ((decoder)->record->blocks[block_id].in_use))
#define XLogRecHasBlockImage(decoder, block_id)		\
	((decoder)->record->blocks[block_id].has_image)
#define XLogRecBlockImageApply(decoder, block_id)		\
	((decoder)->record->blocks[block_id].apply_image)

/* ---- VERBATIM src/include/access/xlogstats.h lines 19-37 ---- */
#define MAX_XLINFO_TYPES 16

typedef struct XLogRecStats
{
	uint64		count;
	uint64		rec_len;
	uint64		fpi_len;
} XLogRecStats;

typedef struct XLogStats
{
	uint64		count;
#ifdef FRONTEND
	XLogRecPtr	startptr;
	XLogRecPtr	endptr;
#endif
	XLogRecStats rmgr_stats[RM_MAX_ID + 1];
	XLogRecStats record_stats[RM_MAX_ID + 1][MAX_XLINFO_TYPES];
} XLogStats;

/* ---- VERBATIM src/backend/access/transam/xlogstats.c lines 18-96 ---- */
/*
 * Calculate the size of a record, split into !FPI and FPI parts.
 */
void
XLogRecGetLen(XLogReaderState *record, uint32 *rec_len,
			  uint32 *fpi_len)
{
	int			block_id;

	/*
	 * Calculate the amount of FPI data in the record.
	 *
	 * XXX: We peek into xlogreader's private decoded backup blocks for the
	 * bimg_len indicating the length of FPI data.
	 */
	*fpi_len = 0;
	for (block_id = 0; block_id <= XLogRecMaxBlockId(record); block_id++)
	{
		if (!XLogRecHasBlockRef(record, block_id))
			continue;

		if (XLogRecHasBlockImage(record, block_id))
			*fpi_len += XLogRecGetBlock(record, block_id)->bimg_len;
	}

	/*
	 * Calculate the length of the record as the total length - the length of
	 * all the block images.
	 */
	*rec_len = XLogRecGetTotalLen(record) - *fpi_len;
}

/*
 * Store per-rmgr and per-record statistics for a given record.
 */
void
XLogRecStoreStats(XLogStats *stats, XLogReaderState *record)
{
	RmgrId		rmid;
	uint8		recid;
	uint32		rec_len;
	uint32		fpi_len;

	Assert(stats != NULL && record != NULL);

	stats->count++;

	rmid = XLogRecGetRmid(record);

	XLogRecGetLen(record, &rec_len, &fpi_len);

	/* Update per-rmgr statistics */

	stats->rmgr_stats[rmid].count++;
	stats->rmgr_stats[rmid].rec_len += rec_len;
	stats->rmgr_stats[rmid].fpi_len += fpi_len;

	/*
	 * Update per-record statistics, where the record is identified by a
	 * combination of the RmgrId and the four bits of the xl_info field that
	 * are the rmgr's domain (resulting in sixteen possible entries per
	 * RmgrId).
	 */

	recid = XLogRecGetInfo(record) >> 4;

	/*
	 * XACT records need to be handled differently. Those records use the
	 * first bit of those four bits for an optional flag variable and the
	 * following three bits for the opcode. We filter opcode out of xl_info
	 * and use it as the identifier of the record.
	 */
	if (rmid == RM_XACT_ID)
		recid &= 0x07;

	stats->record_stats[rmid][recid].count++;
	stats->record_stats[rmid][recid].rec_len += rec_len;
	stats->record_stats[rmid][recid].fpi_len += fpi_len;
}

/* ================= SECTION 6: stringinfo ================= */

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

/* forward decls (stringinfo.h provided these in C) */
static void resetStringInfo(StringInfo str);
static void enlargeStringInfo(StringInfo str, int needed);
static void appendBinaryStringInfo(StringInfo str, const void *data, int datalen);

/* ---- VERBATIM src/common/stringinfo.c blocks (see header); each function
 * [static-prefixed] via the marker lines below ---- */

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
static
void
initStringInfo(StringInfo str)
{
	initStringInfoInternal(str, STRINGINFO_DEFAULT_SIZE);
}
/*
 * initStringInfoExt
 *
 * Initialize a StringInfoData struct (with previously undefined contents)
 * to describe an empty string.
 * The initial memory allocation size is specified by 'initsize'.
 * The valid range for 'initsize' is 1 to MaxAllocSize.
 */
static
void
initStringInfoExt(StringInfo str, int initsize)
{
	initStringInfoInternal(str, initsize);
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
static
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
 * appendStringInfoString
 *
 * Append a null-terminated string to str.
 * Like appendStringInfo(str, "%s", s) but faster.
 */
static
void
appendStringInfoString(StringInfo str, const char *s)
{
	appendBinaryStringInfo(str, s, strlen(s));
}
/*
 * appendStringInfoChar
 *
 * Append a single byte to str.
 * Like appendStringInfo(str, "%c", ch) but much faster.
 */
static
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
 * appendStringInfoSpaces
 *
 * Append the specified number of spaces to a buffer.
 */
static
void
appendStringInfoSpaces(StringInfo str, int count)
{
	if (count > 0)
	{
		/* Make more room if needed */
		enlargeStringInfo(str, count);

		/* OK, append the spaces */
		memset(&str->data[str->len], ' ', count);
		str->len += count;
		str->data[str->len] = '\0';
	}
}
/*
 * appendBinaryStringInfo
 *
 * Append arbitrary binary data to a StringInfo, allocating more space
 * if necessary. Ensures that a trailing null byte is present.
 */
static
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
static
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
static
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

/* ========== SECTION D: fuzz-facing driver entries (NOT Postgres code) ===== */

/* Every entry arms the jmp_buf; on longjmp it reports the errcode class. */

/* ---- cmdtag ---- */

const char *
pg_mf_cmdtag_props(int tag, uint64 *namelen, int *evtrgok, int *rwrok,
				   int *rowcnt)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	Size		len = 0;
	const char *name = GetCommandTagNameAndLen((CommandTag) tag, &len);

	*namelen = (uint64) len;
	*evtrgok = command_tag_event_trigger_ok((CommandTag) tag) ? 1 : 0;
	*rwrok = command_tag_table_rewrite_ok((CommandTag) tag) ? 1 : 0;
	*rowcnt = command_tag_display_rowcount((CommandTag) tag) ? 1 : 0;
	/* GetCommandTagName is the same row load; assert table coherence */
	if (GetCommandTagName((CommandTag) tag) != name)
		abort();
	return name;
}

int
pg_mf_cmdtag_enum(const char *commandname)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return (int) GetCommandTagEnum(commandname);
}

void
pg_mf_init_qc(int *tag, uint64 *nprocessed)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	QueryCompletion qc;

	qc.commandTag = (CommandTag) 42;
	qc.nprocessed = 42;
	InitializeQueryCompletion(&qc);
	*tag = (int) qc.commandTag;
	*nprocessed = qc.nprocessed;
}

uint64
pg_mf_build_qc(int tag, uint64 nprocessed, int nameonly, char *buff)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	QueryCompletion qc;

	qc.commandTag = (CommandTag) tag;
	qc.nprocessed = nprocessed;
	return (uint64) BuildQueryCompletionString(buff, &qc, nameonly != 0);
}

/* ---- pg_class ---- */

int
pg_mf_relkind_detail(uint8 relkind, char *out, int outsz)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	pg_mf_errcode = 0;
	pg_mf_detail[0] = '\0';
	if (setjmp(pg_mf_jmp) != 0)
		return pg_mf_errcode;
	(void) errdetail_relkind_not_supported((char) relkind);
	/* the captured errdetail VALUE is this function's result */
	snprintf(out, outsz, "%s", pg_mf_detail);
	return 0;
}

/* ---- earthdistance ---- */

double
pg_mf_geo_distance(double x1, double y1, double x2, double y2)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	Point		pt1 = {x1, y1};
	Point		pt2 = {x2, y2};

	return geo_distance_internal(&pt1, &pt2);
}

/* ---- pg_rusage ---- */

void
pg_mf_rusage_show(const int64 *ru0_fields, const int64 *ru1_fields, char *out)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	PGRUsage	ru0;

	memset(&ru0, 0, sizeof(ru0));
	memset(&pg_mf_ru1_fixture, 0, sizeof(pg_mf_ru1_fixture));
	ru0.tv.tv_sec = (time_t) ru0_fields[0];
	ru0.tv.tv_usec = (suseconds_t) ru0_fields[1];
	ru0.ru.ru_utime.tv_sec = (time_t) ru0_fields[2];
	ru0.ru.ru_utime.tv_usec = (suseconds_t) ru0_fields[3];
	ru0.ru.ru_stime.tv_sec = (time_t) ru0_fields[4];
	ru0.ru.ru_stime.tv_usec = (suseconds_t) ru0_fields[5];
	pg_mf_ru1_fixture.tv.tv_sec = (time_t) ru1_fields[0];
	pg_mf_ru1_fixture.tv.tv_usec = (suseconds_t) ru1_fields[1];
	pg_mf_ru1_fixture.ru.ru_utime.tv_sec = (time_t) ru1_fields[2];
	pg_mf_ru1_fixture.ru.ru_utime.tv_usec = (suseconds_t) ru1_fields[3];
	pg_mf_ru1_fixture.ru.ru_stime.tv_sec = (time_t) ru1_fields[4];
	pg_mf_ru1_fixture.ru.ru_stime.tv_usec = (suseconds_t) ru1_fields[5];
	snprintf(out, 100, "%s", pg_rusage_show(&ru0));
}

/* ---- xlogstats ---- */

static XLogStats pg_mf_stats;

void
pg_mf_xlog_reset(void)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	memset(&pg_mf_stats, 0, sizeof(pg_mf_stats));
}

/*
 * Store one decoded-record fixture. in_use/has_image are byte flags,
 * bimg_len the u16 image lengths; arrays have max_block_id+1 entries
 * (max_block_id may be -1 for a blockless record).
 */
void
pg_mf_xlog_store(uint8 rmid, uint8 info, uint32 tot_len, int max_block_id,
				 const uint8 *in_use, const uint8 *has_image,
				 const uint16 *bimg_len,
				 uint32 *out_rec_len, uint32 *out_fpi_len)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	DecodedXLogRecord *rec;
	XLogReaderState reader;
	int			i;

	rec = (DecodedXLogRecord *) malloc(sizeof(DecodedXLogRecord) +
									   (XLR_MAX_BLOCK_ID + 1) * sizeof(DecodedBkpBlock));
	memset(rec, 0, sizeof(DecodedXLogRecord) +
		   (XLR_MAX_BLOCK_ID + 1) * sizeof(DecodedBkpBlock));
	rec->header.xl_rmid = rmid;
	rec->header.xl_info = info;
	rec->header.xl_tot_len = tot_len;
	rec->max_block_id = max_block_id;
	for (i = 0; i <= max_block_id; i++)
	{
		rec->blocks[i].in_use = in_use[i] != 0;
		rec->blocks[i].has_image = has_image[i] != 0;
		rec->blocks[i].bimg_len = bimg_len[i];
	}
	reader.record = rec;
	XLogRecGetLen(&reader, out_rec_len, out_fpi_len);
	XLogRecStoreStats(&pg_mf_stats, &reader);
	free(rec);
}

uint64
pg_mf_xlog_count(void)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	return pg_mf_stats.count;
}

void
pg_mf_xlog_cell(int rmid, int recid, uint64 *rmgr_out, uint64 *rec_out)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	rmgr_out[0] = pg_mf_stats.rmgr_stats[rmid].count;
	rmgr_out[1] = pg_mf_stats.rmgr_stats[rmid].rec_len;
	rmgr_out[2] = pg_mf_stats.rmgr_stats[rmid].fpi_len;
	rec_out[0] = pg_mf_stats.record_stats[rmid][recid].count;
	rec_out[1] = pg_mf_stats.record_stats[rmid][recid].rec_len;
	rec_out[2] = pg_mf_stats.record_stats[rmid][recid].fpi_len;
}

/* ---- stringinfo ---- */

static StringInfoData pg_mf_si;
static int	pg_mf_si_live = 0;

int
pg_mf_si_init(int initsize)
{
	if (pg_mf_si_live)
	{
		free(pg_mf_si.data);
		pg_mf_si_live = 0;
	}
	pg_mf_errcode = 0;
	if (setjmp(pg_mf_jmp) != 0)
		return pg_mf_errcode;
	initStringInfoInternal(&pg_mf_si, initsize);
	pg_mf_si_live = 1;
	return 0;
}

int
pg_mf_si_init_default(void)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	if (pg_mf_si_live)
	{
		free(pg_mf_si.data);
		pg_mf_si_live = 0;
	}
	pg_mf_errcode = 0;
	if (setjmp(pg_mf_jmp) != 0)
		return pg_mf_errcode;
	initStringInfo(&pg_mf_si);
	pg_mf_si_live = 1;
	return 0;
}

int
pg_mf_si_init_ext(int initsize)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	if (pg_mf_si_live)
	{
		free(pg_mf_si.data);
		pg_mf_si_live = 0;
	}
	pg_mf_errcode = 0;
	if (setjmp(pg_mf_jmp) != 0)
		return pg_mf_errcode;
	initStringInfoExt(&pg_mf_si, initsize);
	pg_mf_si_live = 1;
	return 0;
}

#define PG_MF_SI_OP(body) \
	do { \
		pg_mf_errcode = 0; \
		if (setjmp(pg_mf_jmp) != 0) \
			return pg_mf_errcode; \
		body; \
		return 0; \
	} while (0)

int
pg_mf_si_reset(void)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	PG_MF_SI_OP(resetStringInfo(&pg_mf_si));
}

int
pg_mf_si_append_bin(const uint8 *data, int datalen)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	PG_MF_SI_OP(appendBinaryStringInfo(&pg_mf_si, data, datalen));
}

int
pg_mf_si_append_bin_nt(const uint8 *data, int datalen)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	PG_MF_SI_OP(appendBinaryStringInfoNT(&pg_mf_si, data, datalen));
}

int
pg_mf_si_append_char(uint8 ch)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	PG_MF_SI_OP(appendStringInfoChar(&pg_mf_si, (char) ch));
}

int
pg_mf_si_append_spaces(int count)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	PG_MF_SI_OP(appendStringInfoSpaces(&pg_mf_si, count));
}

int
pg_mf_si_append_string(const char *s)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	PG_MF_SI_OP(appendStringInfoString(&pg_mf_si, s));
}

int
pg_mf_si_enlarge(int needed)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	PG_MF_SI_OP(enlargeStringInfo(&pg_mf_si, needed));
}

/*
 * pq_writeintN shape (pqformat.h): caller pre-enlarged; memcpy + len bump,
 * no NUL maintenance (driver plumbing mirroring the Rust write_fixed).
 */
int
pg_mf_si_write_fixed(const uint8 *data, int n)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	if (pg_mf_si.maxlen - pg_mf_si.len < n)
		return 99;				/* driver precondition violated */
	memcpy(pg_mf_si.data + pg_mf_si.len, data, n);
	pg_mf_si.len += n;
	return 0;
}

/*
 * The rmgrdesc roll-back idiom the Rust truncate() mirrors
 * (`buf->len = n; buf->data[n] = '\0';` — driver plumbing, the idiom is
 * two field stores, not a stringinfo.c function).
 */
void
pg_mf_si_truncate(int newlen)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	if (newlen < pg_mf_si.len)
	{
		pg_mf_si.len = newlen;
		pg_mf_si.data[newlen] = '\0';
	}
}

const char *
pg_mf_si_get(int *len, int *maxlen)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	*len = pg_mf_si.len;
	*maxlen = pg_mf_si.maxlen;
	return pg_mf_si.data;
}
