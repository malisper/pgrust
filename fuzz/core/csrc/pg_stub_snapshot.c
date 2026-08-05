/*
 * stub:snapshot — C-oracle side of the shared constructed-state snapshot
 * builder (fuzz/core/src/stub_snapshot.rs is the Rust side; the wire format
 * documented there is the contract, and the two decoders are transcriptions
 * of each other — asymmetry is a harness bug, never a divergence).
 *
 * Provenance: SnapshotType and SnapshotData vendored VERBATIM (modulo the
 * pointer-only environment stubs listed below) from postgres-src
 * 62d6c7d3df6287f1bd83199c1a746e50d31571a0 (REL_18 "Stamp 18.3",
 * ../pgrust-fabled/vendor/postgres-src) src/include/utils/snapshot.h.
 *
 * Shims (plumbing only, never logic):
 *   - TransactionId/CommandId/uint32/int32/uint64/bool via <stdint.h>
 *     fixed-width typedefs (LP64, same as the other csrc shims).
 *   - struct GlobalVisState is opaque in snapshot.h already (pointer only);
 *     declared but never defined here. pairingheap_node vendored as its
 *     three-pointer shape (lib/pairingheap.h) so the struct layout is real;
 *     both stay zeroed and are NOT in the compared plane.
 *
 * SECTION-S writer: serializes the CONSTRUCTED struct's fields (never the
 * wire bytes) in the exact order of the Rust ser_snapshot_plane, so a
 * construction difference on either side is a caught divergence.
 */

#include <stdint.h>
#include <string.h>
#include <stdbool.h>
#include "pg_oracle_guard.h"	/* oracle-serialization holder check */

typedef uint32_t TransactionId;
typedef uint32_t CommandId;
typedef uint32_t uint32;
typedef int32_t int32;
typedef uint64_t uint64;

/* lib/pairingheap.h pairingheap_node — three-pointer shape, zeroed here */
typedef struct pairingheap_node
{
	struct pairingheap_node *first_child;
	struct pairingheap_node *next_sibling;
	struct pairingheap_node *prev_or_parent;
} pairingheap_node;

struct GlobalVisState;			/* opaque, exactly as in snapshot.h */

/* src/include/utils/snapshot.h — VERBATIM (comments elided) */
typedef enum SnapshotType
{
	SNAPSHOT_MVCC = 0,
	SNAPSHOT_SELF,
	SNAPSHOT_ANY,
	SNAPSHOT_TOAST,
	SNAPSHOT_DIRTY,
	SNAPSHOT_HISTORIC_MVCC,
	SNAPSHOT_NON_VACUUMABLE,
} SnapshotType;

typedef struct SnapshotData
{
	SnapshotType snapshot_type; /* type of snapshot */
	TransactionId xmin;			/* all XID < xmin are visible to me */
	TransactionId xmax;			/* all XID >= xmax are invisible to me */
	TransactionId *xip;
	uint32		xcnt;			/* # of xact ids in xip[] */
	TransactionId *subxip;
	int32		subxcnt;		/* # of xact ids in subxip[] */
	bool		suboverflowed;	/* has the subxip array overflowed? */
	bool		takenDuringRecovery;	/* recovery-shaped snapshot? */
	bool		copied;			/* false if it's a static snapshot */
	CommandId	curcid;			/* in my xact, CID < curcid are visible */
	uint32		speculativeToken;
	struct GlobalVisState *vistest;
	uint32		active_count;	/* refcount on ActiveSnapshot stack */
	uint32		regd_count;		/* refcount on RegisteredSnapshots */
	pairingheap_node ph_node;	/* link in the RegisteredSnapshots heap */
	uint64		snapXactCompletionCount;
} SnapshotData;

/* xip/subxip ceiling — MUST equal stub_snapshot.rs MAX_XIP */
#define PG_STUB_MAX_XIP 64

/* little-endian readers over the wire (missing bytes read as 0 is the RUST
 * cursor's job: the wire is always complete; a short wire here is a harness
 * internal error, reported as a negative status, never silently padded) */

typedef struct
{
	const uint8_t *b;
	int			len;
	int			i;
	int			short_read;
} StubRd;

static uint8_t
rd_u8(StubRd *r)
{
	if (r->i >= r->len)
	{
		r->short_read = 1;
		return 0;
	}
	return r->b[r->i++];
}

static uint32_t
rd_u32(StubRd *r)
{
	uint32_t	v = 0;
	for (int k = 0; k < 4; k++)
		v |= ((uint32_t) rd_u8(r)) << (8 * k);
	return v;
}

static uint64_t
rd_u64(StubRd *r)
{
	uint64_t	v = 0;
	for (int k = 0; k < 8; k++)
		v |= ((uint64_t) rd_u8(r)) << (8 * k);
	return v;
}

typedef struct
{
	uint8_t    *b;
	int			cap;
	int			i;
	int			overflow;
} StubWr;

static void
wr_u8(StubWr *w, uint8_t v)
{
	if (w->i >= w->cap)
	{
		w->overflow = 1;
		return;
	}
	w->b[w->i++] = v;
}

static void
wr_u32(StubWr *w, uint32_t v)
{
	for (int k = 0; k < 4; k++)
		wr_u8(w, (uint8_t) (v >> (8 * k)));
}

static void
wr_u64(StubWr *w, uint64_t v)
{
	for (int k = 0; k < 8; k++)
		wr_u8(w, (uint8_t) (v >> (8 * k)));
}

/*
 * Decode the wire, CONSTRUCT a SnapshotData, then serialize the constructed
 * struct (SECTION-S).  Returns 0 ok, -1 truncated wire, -2 output overflow,
 * -3 clamp violation (xcnt/subxcnt beyond PG_STUB_MAX_XIP: the Rust encoder
 * can never produce it, so it is a harness bug, not fuzz surface).
 */
int
pg_stub_snapshot_build(const uint8_t *wire, int wirelen,
					   uint8_t *out, int outcap, int *outlen)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	StubRd		rd = {wire, wirelen, 0, 0};
	StubWr		wr = {out, outcap, 0, 0};
	TransactionId xip[PG_STUB_MAX_XIP];
	TransactionId subxip[PG_STUB_MAX_XIP];
	SnapshotData snap;

	memset(&snap, 0, sizeof(snap));

	snap.snapshot_type = (SnapshotType) rd_u8(&rd);
	snap.xmin = rd_u32(&rd);
	snap.xmax = rd_u32(&rd);
	{
		uint8_t		xcnt = rd_u8(&rd);

		if (xcnt > PG_STUB_MAX_XIP)
			return -3;
		for (int k = 0; k < xcnt; k++)
			xip[k] = rd_u32(&rd);
		snap.xip = xip;
		snap.xcnt = xcnt;
	}
	{
		uint8_t		subxcnt = rd_u8(&rd);

		if (subxcnt > PG_STUB_MAX_XIP)
			return -3;
		for (int k = 0; k < subxcnt; k++)
			subxip[k] = rd_u32(&rd);
		snap.subxip = subxip;
		snap.subxcnt = subxcnt;
	}
	{
		uint8_t		flags = rd_u8(&rd);

		snap.suboverflowed = (flags & 0x01) != 0;
		snap.takenDuringRecovery = (flags & 0x02) != 0;
		snap.copied = (flags & 0x04) != 0;
	}
	snap.curcid = rd_u32(&rd);
	snap.speculativeToken = rd_u32(&rd);
	snap.snapXactCompletionCount = rd_u64(&rd);

	if (rd.short_read)
		return -1;

	/* SECTION-S: serialize the CONSTRUCTED struct (mirror of Rust
	 * ser_snapshot_plane; keep in lockstep) */
	wr_u32(&wr, (uint32_t) snap.snapshot_type);
	wr_u32(&wr, snap.xmin);
	wr_u32(&wr, snap.xmax);
	wr_u32(&wr, snap.xcnt);
	for (uint32 k = 0; k < snap.xcnt; k++)
		wr_u32(&wr, snap.xip[k]);
	wr_u32(&wr, (uint32_t) snap.subxcnt);
	for (int32 k = 0; k < snap.subxcnt; k++)
		wr_u32(&wr, snap.subxip[k]);
	wr_u8(&wr, snap.suboverflowed ? 1 : 0);
	wr_u8(&wr, snap.takenDuringRecovery ? 1 : 0);
	wr_u8(&wr, snap.copied ? 1 : 0);
	wr_u32(&wr, snap.curcid);
	wr_u32(&wr, snap.speculativeToken);
	wr_u64(&wr, snap.snapXactCompletionCount);

	if (wr.overflow)
		return -2;
	*outlen = wr.i;
	return 0;
}
