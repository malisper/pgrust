/*
 * Vendored PostgreSQL C for the state-seam-probe proof family — sequence
 * nextval arithmetic.
 *
 * Provenance:
 *   - src/backend/commands/sequence.c, nextval_internal()
 *   ref: postgres/postgres REL_18_STABLE
 *   fetched: 2026-07-28
 *
 * STATE-SEAM EXTRACTION: pg_nextval_advance is the arithmetic section of
 * nextval_internal — everything between the state reads (seq tuple +
 * pg_sequence catalog form) and the local-cache writeback. The state
 * machinery around it is the seam, shimmed to parameters that the harness
 * makes fully symbolic (both sides read the SAME symbolic values):
 *
 *   seq->last_value / seq->log_cnt / seq->is_called  -> last_value, log_cnt,
 *                                                       is_called params
 *   pgsform->seqincrement/seqmax/seqmin/seqcache/seqcycle
 *                                                    -> incby, maxv, minv,
 *                                                       cache, cycle params
 *   PageGetLSN(page) <= GetRedoRecPtr()              -> lsn_le_redo param
 *                       (evaluated at the same program point: only in the
 *                        else-arm of the pre-log decision)
 *
 * Function-local shims (plumbing only, never logic):
 *   - `pg_` prefix; plain scalar signature instead of (Oid relid, bool).
 *   - ereport(ERROR, ERRCODE_SEQUENCE_GENERATOR_LIMIT_EXCEEDED "maximum")
 *       -> `return 1;` at the exact program point (message text out of
 *       proof; the max-vs-min verdict stays in via distinct sentinels).
 *     ereport(... "minimum") -> `return 2;`
 *   - outputs (result, last, next, log, logit) -> out-params; returns 0.
 *     `result` is what nextval returns; `last` is elm->cached / the value
 *     written back as last_value; `next` is the value stored in the WAL
 *     image; `log` the new log_cnt; `logit` whether WAL must be emitted.
 *   - Assert(log >= 0) compiled out via shim header (release parity).
 *
 * The body between the parameter reads and the out-param stores is verbatim
 * (comments included). Postgres compiles with -fwrapv; CBMC's default
 * two's-complement wrap matches.
 */

#include "../../support/c/pg_proof_shim.h"

#define SEQ_LOG_VALS	32

int
pg_nextval_advance(int64 last_value, int64 log_cnt, int is_called,
				   int64 incby, int64 maxv, int64 minv, int64 cache,
				   int cycle, int lsn_le_redo,
				   int64 *out_result, int64 *out_last, int64 *out_next,
				   int64 *out_log, int *out_logit)
{
	int64		log,
				fetch,
				last;
	int64		result,
				next,
				rescnt = 0;
	bool		logit = false;

	last = next = result = last_value;
	fetch = cache;
	log = log_cnt;

	if (!is_called)
	{
		rescnt++;				/* return last_value if not is_called */
		fetch--;
	}

	/*
	 * Decide whether we should emit a WAL log record.  If so, force up the
	 * fetch count to grab SEQ_LOG_VALS more values than we actually need to
	 * cache.  (These will then be usable without logging.)
	 *
	 * If this is the first nextval after a checkpoint, we must force a new
	 * WAL record to be written anyway, else replay starting from the
	 * checkpoint would fail to advance the sequence past the logged values.
	 * In this case we may as well fetch extra values.
	 */
	if (log < fetch || !is_called)
	{
		/* forced log to satisfy local demand for values */
		fetch = log = fetch + SEQ_LOG_VALS;
		logit = true;
	}
	else
	{
		/* seam shim: PageGetLSN(page) <= GetRedoRecPtr() -> lsn_le_redo */
		if (lsn_le_redo)
		{
			/* last update of seq was before checkpoint */
			fetch = log = fetch + SEQ_LOG_VALS;
			logit = true;
		}
	}

	while (fetch)				/* try to fetch cache [+ log ] numbers */
	{
		/*
		 * Check MAXVALUE for ascending sequences and MINVALUE for descending
		 * sequences
		 */
		if (incby > 0)
		{
			/* ascending sequence */
			if ((maxv >= 0 && next > maxv - incby) ||
				(maxv < 0 && next + incby > maxv))
			{
				if (rescnt > 0)
					break;		/* stop fetching */
				if (!cycle)
					return 1;	/* shim: ereport(ERROR, ...) "nextval:
								 * reached maximum value of sequence" */
				next = minv;
			}
			else
				next += incby;
		}
		else
		{
			/* descending sequence */
			if ((minv < 0 && next < minv - incby) ||
				(minv >= 0 && next + incby < minv))
			{
				if (rescnt > 0)
					break;		/* stop fetching */
				if (!cycle)
					return 2;	/* shim: ereport(ERROR, ...) "nextval:
								 * reached minimum value of sequence" */
				next = maxv;
			}
			else
				next += incby;
		}
		fetch--;
		if (rescnt < cache)
		{
			log--;
			rescnt++;
			last = next;
			if (rescnt == 1)	/* if it's first result - */
				result = next;	/* it's what to return */
		}
	}

	log -= fetch;				/* adjust for any unfetched numbers */
	Assert(log >= 0);

	*out_result = result;
	*out_last = last;
	*out_next = next;
	*out_log = log;
	*out_logit = logit;
	return 0;
}
