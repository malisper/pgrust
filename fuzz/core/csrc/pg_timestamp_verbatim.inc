
/* ==== VERBATIM: src/backend/utils/adt/timestamp.c anytimestamp_typmod_check ==== */

int32
anytimestamp_typmod_check(bool istz, int32 typmod)
{
	if (typmod < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("TIMESTAMP(%d)%s precision must not be negative",
						typmod, (istz ? " WITH TIME ZONE" : ""))));
	if (typmod > MAX_TIMESTAMP_PRECISION)
	{
		ereport(WARNING,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("TIMESTAMP(%d)%s precision reduced to maximum allowed, %d",
						typmod, (istz ? " WITH TIME ZONE" : ""),
						MAX_TIMESTAMP_PRECISION)));
		typmod = MAX_TIMESTAMP_PRECISION;
	}

	return typmod;
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c timestamp_in ==== */

Datum
timestamp_in(PG_FUNCTION_ARGS)
{
	char	   *str = PG_GETARG_CSTRING(0);
#ifdef NOT_USED
	Oid			typelem = PG_GETARG_OID(1);
#endif
	int32		typmod = PG_GETARG_INT32(2);
	Node	   *escontext = fcinfo->context;
	Timestamp	result;
	fsec_t		fsec;
	struct pg_tm tt,
			   *tm = &tt;
	int			tz;
	int			dtype;
	int			nf;
	int			dterr;
	char	   *field[MAXDATEFIELDS];
	int			ftype[MAXDATEFIELDS];
	char		workbuf[MAXDATELEN + MAXDATEFIELDS];
	DateTimeErrorExtra extra;

	dterr = ParseDateTime(str, workbuf, sizeof(workbuf),
						  field, ftype, MAXDATEFIELDS, &nf);
	if (dterr == 0)
		dterr = DecodeDateTime(field, ftype, nf,
							   &dtype, tm, &fsec, &tz, &extra);
	if (dterr != 0)
	{
		DateTimeParseError(dterr, &extra, str, "timestamp", escontext);
		PG_RETURN_NULL();
	}

	switch (dtype)
	{
		case DTK_DATE:
			if (tm2timestamp(tm, fsec, NULL, &result) != 0)
				ereturn(escontext, (Datum) 0,
						(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
						 errmsg("timestamp out of range: \"%s\"", str)));
			break;

		case DTK_EPOCH:
			result = SetEpochTimestamp();
			break;

		case DTK_LATE:
			TIMESTAMP_NOEND(result);
			break;

		case DTK_EARLY:
			TIMESTAMP_NOBEGIN(result);
			break;

		default:
			elog(ERROR, "unexpected dtype %d while parsing timestamp \"%s\"",
				 dtype, str);
			TIMESTAMP_NOEND(result);
	}

	AdjustTimestampForTypmod(&result, typmod, escontext);

	PG_RETURN_TIMESTAMP(result);
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c timestamp_out ==== */

Datum
timestamp_out(PG_FUNCTION_ARGS)
{
	Timestamp	timestamp = PG_GETARG_TIMESTAMP(0);
	char	   *result;
	struct pg_tm tt,
			   *tm = &tt;
	fsec_t		fsec;
	char		buf[MAXDATELEN + 1];

	if (TIMESTAMP_NOT_FINITE(timestamp))
		EncodeSpecialTimestamp(timestamp, buf);
	else if (timestamp2tm(timestamp, NULL, tm, &fsec, NULL, NULL) == 0)
		EncodeDateTime(tm, fsec, false, 0, NULL, DateStyle, buf);
	else
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	result = pstrdup(buf);
	PG_RETURN_CSTRING(result);
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c timestamp_recv ==== */

Datum
timestamp_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);

#ifdef NOT_USED
	Oid			typelem = PG_GETARG_OID(1);
#endif
	int32		typmod = PG_GETARG_INT32(2);
	Timestamp	timestamp;
	struct pg_tm tt,
			   *tm = &tt;
	fsec_t		fsec;

	timestamp = (Timestamp) pq_getmsgint64(buf);

	/* range check: see if timestamp_out would like it */
	if (TIMESTAMP_NOT_FINITE(timestamp))
		 /* ok */ ;
	else if (timestamp2tm(timestamp, NULL, tm, &fsec, NULL, NULL) != 0 ||
			 !IS_VALID_TIMESTAMP(timestamp))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	AdjustTimestampForTypmod(&timestamp, typmod, NULL);

	PG_RETURN_TIMESTAMP(timestamp);
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c timestamp_send ==== */

Datum
timestamp_send(PG_FUNCTION_ARGS)
{
	Timestamp	timestamp = PG_GETARG_TIMESTAMP(0);
	StringInfoData buf;

	pq_begintypsend(&buf);
	pq_sendint64(&buf, timestamp);
	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c timestamptz_in ==== */

Datum
timestamptz_in(PG_FUNCTION_ARGS)
{
	char	   *str = PG_GETARG_CSTRING(0);
#ifdef NOT_USED
	Oid			typelem = PG_GETARG_OID(1);
#endif
	int32		typmod = PG_GETARG_INT32(2);
	Node	   *escontext = fcinfo->context;
	TimestampTz result;
	fsec_t		fsec;
	struct pg_tm tt,
			   *tm = &tt;
	int			tz;
	int			dtype;
	int			nf;
	int			dterr;
	char	   *field[MAXDATEFIELDS];
	int			ftype[MAXDATEFIELDS];
	char		workbuf[MAXDATELEN + MAXDATEFIELDS];
	DateTimeErrorExtra extra;

	dterr = ParseDateTime(str, workbuf, sizeof(workbuf),
						  field, ftype, MAXDATEFIELDS, &nf);
	if (dterr == 0)
		dterr = DecodeDateTime(field, ftype, nf,
							   &dtype, tm, &fsec, &tz, &extra);
	if (dterr != 0)
	{
		DateTimeParseError(dterr, &extra, str, "timestamp with time zone",
						   escontext);
		PG_RETURN_NULL();
	}

	switch (dtype)
	{
		case DTK_DATE:
			if (tm2timestamp(tm, fsec, &tz, &result) != 0)
				ereturn(escontext, (Datum) 0,
						(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
						 errmsg("timestamp out of range: \"%s\"", str)));
			break;

		case DTK_EPOCH:
			result = SetEpochTimestamp();
			break;

		case DTK_LATE:
			TIMESTAMP_NOEND(result);
			break;

		case DTK_EARLY:
			TIMESTAMP_NOBEGIN(result);
			break;

		default:
			elog(ERROR, "unexpected dtype %d while parsing timestamptz \"%s\"",
				 dtype, str);
			TIMESTAMP_NOEND(result);
	}

	AdjustTimestampForTypmod(&result, typmod, escontext);

	PG_RETURN_TIMESTAMPTZ(result);
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c timestamptz_out ==== */

Datum
timestamptz_out(PG_FUNCTION_ARGS)
{
	TimestampTz dt = PG_GETARG_TIMESTAMPTZ(0);
	char	   *result;
	int			tz;
	struct pg_tm tt,
			   *tm = &tt;
	fsec_t		fsec;
	const char *tzn;
	char		buf[MAXDATELEN + 1];

	if (TIMESTAMP_NOT_FINITE(dt))
		EncodeSpecialTimestamp(dt, buf);
	else if (timestamp2tm(dt, &tz, tm, &fsec, &tzn, NULL) == 0)
		EncodeDateTime(tm, fsec, true, tz, tzn, DateStyle, buf);
	else
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	result = pstrdup(buf);
	PG_RETURN_CSTRING(result);
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c timestamptz_recv ==== */

Datum
timestamptz_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);

#ifdef NOT_USED
	Oid			typelem = PG_GETARG_OID(1);
#endif
	int32		typmod = PG_GETARG_INT32(2);
	TimestampTz timestamp;
	int			tz;
	struct pg_tm tt,
			   *tm = &tt;
	fsec_t		fsec;

	timestamp = (TimestampTz) pq_getmsgint64(buf);

	/* range check: see if timestamptz_out would like it */
	if (TIMESTAMP_NOT_FINITE(timestamp))
		 /* ok */ ;
	else if (timestamp2tm(timestamp, &tz, tm, &fsec, NULL, NULL) != 0 ||
			 !IS_VALID_TIMESTAMP(timestamp))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	AdjustTimestampForTypmod(&timestamp, typmod, NULL);

	PG_RETURN_TIMESTAMPTZ(timestamp);
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c timestamptz_send ==== */

Datum
timestamptz_send(PG_FUNCTION_ARGS)
{
	TimestampTz timestamp = PG_GETARG_TIMESTAMPTZ(0);
	StringInfoData buf;

	pq_begintypsend(&buf);
	pq_sendint64(&buf, timestamp);
	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c AdjustTimestampForTypmod ==== */

bool
AdjustTimestampForTypmod(Timestamp *time, int32 typmod, Node *escontext)
{
	static const int64 TimestampScales[MAX_TIMESTAMP_PRECISION + 1] = {
		INT64CONST(1000000),
		INT64CONST(100000),
		INT64CONST(10000),
		INT64CONST(1000),
		INT64CONST(100),
		INT64CONST(10),
		INT64CONST(1)
	};

	static const int64 TimestampOffsets[MAX_TIMESTAMP_PRECISION + 1] = {
		INT64CONST(500000),
		INT64CONST(50000),
		INT64CONST(5000),
		INT64CONST(500),
		INT64CONST(50),
		INT64CONST(5),
		INT64CONST(0)
	};

	if (!TIMESTAMP_NOT_FINITE(*time)
		&& (typmod != -1) && (typmod != MAX_TIMESTAMP_PRECISION))
	{
		if (typmod < 0 || typmod > MAX_TIMESTAMP_PRECISION)
			ereturn(escontext, false,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("timestamp(%d) precision must be between %d and %d",
							typmod, 0, MAX_TIMESTAMP_PRECISION)));

		if (*time >= INT64CONST(0))
		{
			*time = ((*time + TimestampOffsets[typmod]) / TimestampScales[typmod]) *
				TimestampScales[typmod];
		}
		else
		{
			*time = -((((-*time) + TimestampOffsets[typmod]) / TimestampScales[typmod])
					  * TimestampScales[typmod]);
		}
	}

	return true;
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c timestamp_scale ==== */

Datum
timestamp_scale(PG_FUNCTION_ARGS)
{
	Timestamp	timestamp = PG_GETARG_TIMESTAMP(0);
	int32		typmod = PG_GETARG_INT32(1);
	Timestamp	result;

	result = timestamp;

	AdjustTimestampForTypmod(&result, typmod, NULL);

	PG_RETURN_TIMESTAMP(result);
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c EncodeSpecialTimestamp ==== */

void
EncodeSpecialTimestamp(Timestamp dt, char *str)
{
	if (TIMESTAMP_IS_NOBEGIN(dt))
		strcpy(str, EARLY);
	else if (TIMESTAMP_IS_NOEND(dt))
		strcpy(str, LATE);
	else						/* shouldn't happen */
		elog(ERROR, "invalid argument for EncodeSpecialTimestamp");
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c interval_in ==== */

Datum
interval_in(PG_FUNCTION_ARGS)
{
	char	   *str = PG_GETARG_CSTRING(0);
#ifdef NOT_USED
	Oid			typelem = PG_GETARG_OID(1);
#endif
	int32		typmod = PG_GETARG_INT32(2);
	Node	   *escontext = fcinfo->context;
	Interval   *result;
	struct pg_itm_in tt,
			   *itm_in = &tt;
	int			dtype;
	int			nf;
	int			range;
	int			dterr;
	char	   *field[MAXDATEFIELDS];
	int			ftype[MAXDATEFIELDS];
	char		workbuf[256];
	DateTimeErrorExtra extra;

	itm_in->tm_year = 0;
	itm_in->tm_mon = 0;
	itm_in->tm_mday = 0;
	itm_in->tm_usec = 0;

	if (typmod >= 0)
		range = INTERVAL_RANGE(typmod);
	else
		range = INTERVAL_FULL_RANGE;

	dterr = ParseDateTime(str, workbuf, sizeof(workbuf), field,
						  ftype, MAXDATEFIELDS, &nf);
	if (dterr == 0)
		dterr = DecodeInterval(field, ftype, nf, range,
							   &dtype, itm_in);

	/* if those functions think it's a bad format, try ISO8601 style */
	if (dterr == DTERR_BAD_FORMAT)
		dterr = DecodeISO8601Interval(str,
									  &dtype, itm_in);

	if (dterr != 0)
	{
		if (dterr == DTERR_FIELD_OVERFLOW)
			dterr = DTERR_INTERVAL_OVERFLOW;
		DateTimeParseError(dterr, &extra, str, "interval", escontext);
		PG_RETURN_NULL();
	}

	result = (Interval *) palloc(sizeof(Interval));

	switch (dtype)
	{
		case DTK_DELTA:
			if (itmin2interval(itm_in, result) != 0)
				ereturn(escontext, (Datum) 0,
						(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
						 errmsg("interval out of range")));
			break;

		case DTK_LATE:
			INTERVAL_NOEND(result);
			break;

		case DTK_EARLY:
			INTERVAL_NOBEGIN(result);
			break;

		default:
			elog(ERROR, "unexpected dtype %d while parsing interval \"%s\"",
				 dtype, str);
	}

	AdjustIntervalForTypmod(result, typmod, escontext);

	PG_RETURN_INTERVAL_P(result);
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c interval_out ==== */

Datum
interval_out(PG_FUNCTION_ARGS)
{
	Interval   *span = PG_GETARG_INTERVAL_P(0);
	char	   *result;
	struct pg_itm tt,
			   *itm = &tt;
	char		buf[MAXDATELEN + 1];

	if (INTERVAL_NOT_FINITE(span))
		EncodeSpecialInterval(span, buf);
	else
	{
		interval2itm(*span, itm);
		EncodeInterval(itm, IntervalStyle, buf);
	}

	result = pstrdup(buf);
	PG_RETURN_CSTRING(result);
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c interval_recv ==== */

Datum
interval_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);

#ifdef NOT_USED
	Oid			typelem = PG_GETARG_OID(1);
#endif
	int32		typmod = PG_GETARG_INT32(2);
	Interval   *interval;

	interval = (Interval *) palloc(sizeof(Interval));

	interval->time = pq_getmsgint64(buf);
	interval->day = pq_getmsgint(buf, sizeof(interval->day));
	interval->month = pq_getmsgint(buf, sizeof(interval->month));

	AdjustIntervalForTypmod(interval, typmod, NULL);

	PG_RETURN_INTERVAL_P(interval);
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c interval_send ==== */

Datum
interval_send(PG_FUNCTION_ARGS)
{
	Interval   *interval = PG_GETARG_INTERVAL_P(0);
	StringInfoData buf;

	pq_begintypsend(&buf);
	pq_sendint64(&buf, interval->time);
	pq_sendint32(&buf, interval->day);
	pq_sendint32(&buf, interval->month);
	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c interval_scale ==== */

Datum
interval_scale(PG_FUNCTION_ARGS)
{
	Interval   *interval = PG_GETARG_INTERVAL_P(0);
	int32		typmod = PG_GETARG_INT32(1);
	Interval   *result;

	result = palloc(sizeof(Interval));
	*result = *interval;

	AdjustIntervalForTypmod(result, typmod, NULL);

	PG_RETURN_INTERVAL_P(result);
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c AdjustIntervalForTypmod ==== */

static bool
AdjustIntervalForTypmod(Interval *interval, int32 typmod,
						Node *escontext)
{
	static const int64 IntervalScales[MAX_INTERVAL_PRECISION + 1] = {
		INT64CONST(1000000),
		INT64CONST(100000),
		INT64CONST(10000),
		INT64CONST(1000),
		INT64CONST(100),
		INT64CONST(10),
		INT64CONST(1)
	};

	static const int64 IntervalOffsets[MAX_INTERVAL_PRECISION + 1] = {
		INT64CONST(500000),
		INT64CONST(50000),
		INT64CONST(5000),
		INT64CONST(500),
		INT64CONST(50),
		INT64CONST(5),
		INT64CONST(0)
	};

	/* Typmod has no effect on infinite intervals */
	if (INTERVAL_NOT_FINITE(interval))
		return true;

	/*
	 * Unspecified range and precision? Then not necessary to adjust. Setting
	 * typmod to -1 is the convention for all data types.
	 */
	if (typmod >= 0)
	{
		int			range = INTERVAL_RANGE(typmod);
		int			precision = INTERVAL_PRECISION(typmod);

		/*
		 * Our interpretation of intervals with a limited set of fields is
		 * that fields to the right of the last one specified are zeroed out,
		 * but those to the left of it remain valid.  Thus for example there
		 * is no operational difference between INTERVAL YEAR TO MONTH and
		 * INTERVAL MONTH.  In some cases we could meaningfully enforce that
		 * higher-order fields are zero; for example INTERVAL DAY could reject
		 * nonzero "month" field.  However that seems a bit pointless when we
		 * can't do it consistently.  (We cannot enforce a range limit on the
		 * highest expected field, since we do not have any equivalent of
		 * SQL's <interval leading field precision>.)  If we ever decide to
		 * revisit this, interval_support will likely require adjusting.
		 *
		 * Note: before PG 8.4 we interpreted a limited set of fields as
		 * actually causing a "modulo" operation on a given value, potentially
		 * losing high-order as well as low-order information.  But there is
		 * no support for such behavior in the standard, and it seems fairly
		 * undesirable on data consistency grounds anyway.  Now we only
		 * perform truncation or rounding of low-order fields.
		 */
		if (range == INTERVAL_FULL_RANGE)
		{
			/* Do nothing... */
		}
		else if (range == INTERVAL_MASK(YEAR))
		{
			interval->month = (interval->month / MONTHS_PER_YEAR) * MONTHS_PER_YEAR;
			interval->day = 0;
			interval->time = 0;
		}
		else if (range == INTERVAL_MASK(MONTH))
		{
			interval->day = 0;
			interval->time = 0;
		}
		/* YEAR TO MONTH */
		else if (range == (INTERVAL_MASK(YEAR) | INTERVAL_MASK(MONTH)))
		{
			interval->day = 0;
			interval->time = 0;
		}
		else if (range == INTERVAL_MASK(DAY))
		{
			interval->time = 0;
		}
		else if (range == INTERVAL_MASK(HOUR))
		{
			interval->time = (interval->time / USECS_PER_HOUR) *
				USECS_PER_HOUR;
		}
		else if (range == INTERVAL_MASK(MINUTE))
		{
			interval->time = (interval->time / USECS_PER_MINUTE) *
				USECS_PER_MINUTE;
		}
		else if (range == INTERVAL_MASK(SECOND))
		{
			/* fractional-second rounding will be dealt with below */
		}
		/* DAY TO HOUR */
		else if (range == (INTERVAL_MASK(DAY) |
						   INTERVAL_MASK(HOUR)))
		{
			interval->time = (interval->time / USECS_PER_HOUR) *
				USECS_PER_HOUR;
		}
		/* DAY TO MINUTE */
		else if (range == (INTERVAL_MASK(DAY) |
						   INTERVAL_MASK(HOUR) |
						   INTERVAL_MASK(MINUTE)))
		{
			interval->time = (interval->time / USECS_PER_MINUTE) *
				USECS_PER_MINUTE;
		}
		/* DAY TO SECOND */
		else if (range == (INTERVAL_MASK(DAY) |
						   INTERVAL_MASK(HOUR) |
						   INTERVAL_MASK(MINUTE) |
						   INTERVAL_MASK(SECOND)))
		{
			/* fractional-second rounding will be dealt with below */
		}
		/* HOUR TO MINUTE */
		else if (range == (INTERVAL_MASK(HOUR) |
						   INTERVAL_MASK(MINUTE)))
		{
			interval->time = (interval->time / USECS_PER_MINUTE) *
				USECS_PER_MINUTE;
		}
		/* HOUR TO SECOND */
		else if (range == (INTERVAL_MASK(HOUR) |
						   INTERVAL_MASK(MINUTE) |
						   INTERVAL_MASK(SECOND)))
		{
			/* fractional-second rounding will be dealt with below */
		}
		/* MINUTE TO SECOND */
		else if (range == (INTERVAL_MASK(MINUTE) |
						   INTERVAL_MASK(SECOND)))
		{
			/* fractional-second rounding will be dealt with below */
		}
		else
			elog(ERROR, "unrecognized interval typmod: %d", typmod);

		/* Need to adjust sub-second precision? */
		if (precision != INTERVAL_FULL_PRECISION)
		{
			if (precision < 0 || precision > MAX_INTERVAL_PRECISION)
				ereturn(escontext, false,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("interval(%d) precision must be between %d and %d",
								precision, 0, MAX_INTERVAL_PRECISION)));

			if (interval->time >= INT64CONST(0))
			{
				if (pg_add_s64_overflow(interval->time,
										IntervalOffsets[precision],
										&interval->time))
					ereturn(escontext, false,
							(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
							 errmsg("interval out of range")));
				interval->time -= interval->time % IntervalScales[precision];
			}
			else
			{
				if (pg_sub_s64_overflow(interval->time,
										IntervalOffsets[precision],
										&interval->time))
					ereturn(escontext, false,
							(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
							 errmsg("interval out of range")));
				interval->time -= interval->time % IntervalScales[precision];
			}
		}
	}

	return true;
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c EncodeSpecialInterval ==== */

static void
EncodeSpecialInterval(const Interval *interval, char *str)
{
	if (INTERVAL_IS_NOBEGIN(interval))
		strcpy(str, EARLY);
	else if (INTERVAL_IS_NOEND(interval))
		strcpy(str, LATE);
	else						/* shouldn't happen */
		elog(ERROR, "invalid argument for EncodeSpecialInterval");
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c time2t ==== */

static TimeOffset
time2t(const int hour, const int min, const int sec, const fsec_t fsec)
{
	return (((((hour * MINS_PER_HOUR) + min) * SECS_PER_MINUTE) + sec) * USECS_PER_SEC) + fsec;
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c SetEpochTimestamp ==== */

Timestamp
SetEpochTimestamp(void)
{
	Timestamp	dt;
	struct pg_tm tt,
			   *tm = &tt;

	GetEpochTime(tm);
	/* we don't bother to test for failure ... */
	tm2timestamp(tm, 0, NULL, &dt);

	return dt;
}								/* SetEpochTimestamp() */

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c tm2timestamp ==== */

int
tm2timestamp(struct pg_tm *tm, fsec_t fsec, int *tzp, Timestamp *result)
{
	TimeOffset	date;
	TimeOffset	time;

	/* Prevent overflow in Julian-day routines */
	if (!IS_VALID_JULIAN(tm->tm_year, tm->tm_mon, tm->tm_mday))
	{
		*result = 0;			/* keep compiler quiet */
		return -1;
	}

	date = date2j(tm->tm_year, tm->tm_mon, tm->tm_mday) - POSTGRES_EPOCH_JDATE;
	time = time2t(tm->tm_hour, tm->tm_min, tm->tm_sec, fsec);

	if (unlikely(pg_mul_s64_overflow(date, USECS_PER_DAY, result) ||
				 pg_add_s64_overflow(*result, time, result)))
	{
		*result = 0;			/* keep compiler quiet */
		return -1;
	}
	if (tzp != NULL)
		*result = dt2local(*result, -(*tzp));

	/* final range check catches just-out-of-range timestamps */
	if (!IS_VALID_TIMESTAMP(*result))
	{
		*result = 0;			/* keep compiler quiet */
		return -1;
	}

	return 0;
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c dt2local ==== */

static Timestamp
dt2local(Timestamp dt, int timezone)
{
	dt -= (timezone * USECS_PER_SEC);
	return dt;
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c itm2interval ==== */

int
itm2interval(struct pg_itm *itm, Interval *span)
{
	int64		total_months = (int64) itm->tm_year * MONTHS_PER_YEAR + itm->tm_mon;

	if (total_months > INT_MAX || total_months < INT_MIN)
		return -1;
	span->month = (int32) total_months;
	span->day = itm->tm_mday;
	if (pg_mul_s64_overflow(itm->tm_hour, USECS_PER_HOUR,
							&span->time))
		return -1;
	/* tm_min, tm_sec are 32 bits, so intermediate products can't overflow */
	if (pg_add_s64_overflow(span->time, itm->tm_min * USECS_PER_MINUTE,
							&span->time))
		return -1;
	if (pg_add_s64_overflow(span->time, itm->tm_sec * USECS_PER_SEC,
							&span->time))
		return -1;
	if (pg_add_s64_overflow(span->time, itm->tm_usec,
							&span->time))
		return -1;
	if (INTERVAL_NOT_FINITE(span))
		return -1;
	return 0;
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c itmin2interval ==== */

int
itmin2interval(struct pg_itm_in *itm_in, Interval *span)
{
	int64		total_months = (int64) itm_in->tm_year * MONTHS_PER_YEAR + itm_in->tm_mon;

	if (total_months > INT_MAX || total_months < INT_MIN)
		return -1;
	span->month = (int32) total_months;
	span->day = itm_in->tm_mday;
	span->time = itm_in->tm_usec;
	return 0;
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c timestamp2timestamptz_opt_overflow ==== */

TimestampTz
timestamp2timestamptz_opt_overflow(Timestamp timestamp, int *overflow)
{
	TimestampTz result;
	struct pg_tm tt,
			   *tm = &tt;
	fsec_t		fsec;
	int			tz;

	if (overflow)
		*overflow = 0;

	if (TIMESTAMP_NOT_FINITE(timestamp))
		return timestamp;

	/* We don't expect this to fail, but check it pro forma */
	if (timestamp2tm(timestamp, NULL, tm, &fsec, NULL, NULL) == 0)
	{
		tz = DetermineTimeZoneOffset(tm, session_timezone);

		result = dt2local(timestamp, -tz);

		if (IS_VALID_TIMESTAMP(result))
		{
			return result;
		}
		else if (overflow)
		{
			if (result < MIN_TIMESTAMP)
			{
				*overflow = -1;
				TIMESTAMP_NOBEGIN(result);
			}
			else
			{
				*overflow = 1;
				TIMESTAMP_NOEND(result);
			}
			return result;
		}
	}

	ereport(ERROR,
			(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
			 errmsg("timestamp out of range")));

	return 0;
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c timestamp2timestamptz ==== */

static TimestampTz
timestamp2timestamptz(Timestamp timestamp)
{
	return timestamp2timestamptz_opt_overflow(timestamp, NULL);
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c timestamptz2timestamp ==== */

static Timestamp
timestamptz2timestamp(TimestampTz timestamp)
{
	Timestamp	result;
	struct pg_tm tt,
			   *tm = &tt;
	fsec_t		fsec;
	int			tz;

	if (TIMESTAMP_NOT_FINITE(timestamp))
		result = timestamp;
	else
	{
		if (timestamp2tm(timestamp, &tz, tm, &fsec, NULL, NULL) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("timestamp out of range")));
		if (tm2timestamp(tm, fsec, NULL, &result) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("timestamp out of range")));
	}
	return result;
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c parse_sane_timezone ==== */

static int
parse_sane_timezone(struct pg_tm *tm, text *zone)
{
	char		tzname[TZ_STRLEN_MAX + 1];
	int			dterr;
	int			tz;

	text_to_cstring_buffer(zone, tzname, sizeof(tzname));

	/*
	 * Look up the requested timezone.  First we try to interpret it as a
	 * numeric timezone specification; if DecodeTimezone decides it doesn't
	 * like the format, we try timezone abbreviations and names.
	 *
	 * Note pg_tzset happily parses numeric input that DecodeTimezone would
	 * reject.  To avoid having it accept input that would otherwise be seen
	 * as invalid, it's enough to disallow having a digit in the first
	 * position of our input string.
	 */
	if (isdigit((unsigned char) *tzname))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid input syntax for type %s: \"%s\"",
						"numeric time zone", tzname),
				 errhint("Numeric time zones must have \"-\" or \"+\" as first character.")));

	dterr = DecodeTimezone(tzname, &tz);
	if (dterr != 0)
	{
		int			type,
					val;
		pg_tz	   *tzp;

		if (dterr == DTERR_TZDISP_OVERFLOW)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("numeric time zone \"%s\" out of range", tzname)));
		else if (dterr != DTERR_BAD_FORMAT)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("time zone \"%s\" not recognized", tzname)));

		type = DecodeTimezoneName(tzname, &val, &tzp);

		if (type == TZNAME_FIXED_OFFSET)
		{
			/* fixed-offset abbreviation */
			tz = -val;
		}
		else if (type == TZNAME_DYNTZ)
		{
			/* dynamic-offset abbreviation, resolve using specified time */
			tz = DetermineTimeZoneAbbrevOffset(tm, tzname, tzp);
		}
		else
		{
			/* full zone name */
			tz = DetermineTimeZoneOffset(tm, tzp);
		}
	}

	return tz;
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c make_timestamp_internal ==== */

static Timestamp
make_timestamp_internal(int year, int month, int day,
						int hour, int min, double sec)
{
	struct pg_tm tm;
	TimeOffset	date;
	TimeOffset	time;
	int			dterr;
	bool		bc = false;
	Timestamp	result;

	tm.tm_year = year;
	tm.tm_mon = month;
	tm.tm_mday = day;

	/* Handle negative years as BC */
	if (tm.tm_year < 0)
	{
		bc = true;
		tm.tm_year = -tm.tm_year;
	}

	dterr = ValidateDate(DTK_DATE_M, false, false, bc, &tm);

	if (dterr != 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_FIELD_OVERFLOW),
				 errmsg("date field value out of range: %d-%02d-%02d",
						year, month, day)));

	if (!IS_VALID_JULIAN(tm.tm_year, tm.tm_mon, tm.tm_mday))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("date out of range: %d-%02d-%02d",
						year, month, day)));

	date = date2j(tm.tm_year, tm.tm_mon, tm.tm_mday) - POSTGRES_EPOCH_JDATE;

	/* Check for time overflow */
	if (float_time_overflows(hour, min, sec))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_FIELD_OVERFLOW),
				 errmsg("time field value out of range: %d:%02d:%02g",
						hour, min, sec)));

	/* This should match tm2time */
	time = (((hour * MINS_PER_HOUR + min) * SECS_PER_MINUTE)
			* USECS_PER_SEC) + (int64) rint(sec * USECS_PER_SEC);

	if (unlikely(pg_mul_s64_overflow(date, USECS_PER_DAY, &result) ||
				 pg_add_s64_overflow(result, time, &result)))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range: %d-%02d-%02d %d:%02d:%02g",
						year, month, day,
						hour, min, sec)));

	/* final range check catches just-out-of-range timestamps */
	if (!IS_VALID_TIMESTAMP(result))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range: %d-%02d-%02d %d:%02d:%02g",
						year, month, day,
						hour, min, sec)));

	return result;
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c make_timestamp ==== */

Datum
make_timestamp(PG_FUNCTION_ARGS)
{
	int32		year = PG_GETARG_INT32(0);
	int32		month = PG_GETARG_INT32(1);
	int32		mday = PG_GETARG_INT32(2);
	int32		hour = PG_GETARG_INT32(3);
	int32		min = PG_GETARG_INT32(4);
	float8		sec = PG_GETARG_FLOAT8(5);
	Timestamp	result;

	result = make_timestamp_internal(year, month, mday,
									 hour, min, sec);

	PG_RETURN_TIMESTAMP(result);
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c make_timestamptz ==== */

Datum
make_timestamptz(PG_FUNCTION_ARGS)
{
	int32		year = PG_GETARG_INT32(0);
	int32		month = PG_GETARG_INT32(1);
	int32		mday = PG_GETARG_INT32(2);
	int32		hour = PG_GETARG_INT32(3);
	int32		min = PG_GETARG_INT32(4);
	float8		sec = PG_GETARG_FLOAT8(5);
	Timestamp	result;

	result = make_timestamp_internal(year, month, mday,
									 hour, min, sec);

	PG_RETURN_TIMESTAMPTZ(timestamp2timestamptz(result));
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c make_timestamptz_at_timezone ==== */

Datum
make_timestamptz_at_timezone(PG_FUNCTION_ARGS)
{
	int32		year = PG_GETARG_INT32(0);
	int32		month = PG_GETARG_INT32(1);
	int32		mday = PG_GETARG_INT32(2);
	int32		hour = PG_GETARG_INT32(3);
	int32		min = PG_GETARG_INT32(4);
	float8		sec = PG_GETARG_FLOAT8(5);
	text	   *zone = PG_GETARG_TEXT_PP(6);
	TimestampTz result;
	Timestamp	timestamp;
	struct pg_tm tt;
	int			tz;
	fsec_t		fsec;

	timestamp = make_timestamp_internal(year, month, mday,
										hour, min, sec);

	if (timestamp2tm(timestamp, NULL, &tt, &fsec, NULL, NULL) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	tz = parse_sane_timezone(&tt, zone);

	result = dt2local(timestamp, -tz);

	if (!IS_VALID_TIMESTAMP(result))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	PG_RETURN_TIMESTAMPTZ(result);
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c make_interval ==== */

Datum
make_interval(PG_FUNCTION_ARGS)
{
	int32		years = PG_GETARG_INT32(0);
	int32		months = PG_GETARG_INT32(1);
	int32		weeks = PG_GETARG_INT32(2);
	int32		days = PG_GETARG_INT32(3);
	int32		hours = PG_GETARG_INT32(4);
	int32		mins = PG_GETARG_INT32(5);
	double		secs = PG_GETARG_FLOAT8(6);
	Interval   *result;

	/*
	 * Reject out-of-range inputs.  We reject any input values that cause
	 * integer overflow of the corresponding interval fields.
	 */
	if (isinf(secs) || isnan(secs))
		goto out_of_range;

	result = (Interval *) palloc(sizeof(Interval));

	/* years and months -> months */
	if (pg_mul_s32_overflow(years, MONTHS_PER_YEAR, &result->month) ||
		pg_add_s32_overflow(result->month, months, &result->month))
		goto out_of_range;

	/* weeks and days -> days */
	if (pg_mul_s32_overflow(weeks, DAYS_PER_WEEK, &result->day) ||
		pg_add_s32_overflow(result->day, days, &result->day))
		goto out_of_range;

	/* hours and mins -> usecs (cannot overflow 64-bit) */
	result->time = hours * USECS_PER_HOUR + mins * USECS_PER_MINUTE;

	/* secs -> usecs */
	secs = rint(float8_mul(secs, USECS_PER_SEC));
	if (!FLOAT8_FITS_IN_INT64(secs) ||
		pg_add_s64_overflow(result->time, (int64) secs, &result->time))
		goto out_of_range;

	/* make sure that the result is finite */
	if (INTERVAL_NOT_FINITE(result))
		goto out_of_range;

	PG_RETURN_INTERVAL_P(result);

out_of_range:
	ereport(ERROR,
			errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
			errmsg("interval out of range"));

	PG_RETURN_NULL();			/* keep compiler quiet */
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c lookup_timezone ==== */

static pg_tz *
lookup_timezone(text *zone)
{
	char		tzname[TZ_STRLEN_MAX + 1];

	text_to_cstring_buffer(zone, tzname, sizeof(tzname));

	return DecodeTimezoneNameToTz(tzname);
}

/* ==== VERBATIM: src/backend/utils/adt/datetime.c DecodeTimezoneName ==== */

int
DecodeTimezoneName(const char *tzname, int *offset, pg_tz **tz)
{
	char	   *lowzone;
	int			dterr,
				type;
	DateTimeErrorExtra extra;

	/*
	 * First we look in the timezone abbreviation table (to handle cases like
	 * "EST"), and if that fails, we look in the timezone database (to handle
	 * cases like "America/New_York").  This matches the order in which
	 * timestamp input checks the cases; it's important because the timezone
	 * database unwisely uses a few zone names that are identical to offset
	 * abbreviations.
	 */

	/* DecodeTimezoneAbbrev requires lowercase input */
	lowzone = downcase_truncate_identifier(tzname,
										   strlen(tzname),
										   false);

	dterr = DecodeTimezoneAbbrev(0, lowzone, &type, offset, tz, &extra);
	if (dterr)
		DateTimeParseError(dterr, &extra, NULL, NULL, NULL);

	if (type == TZ || type == DTZ)
	{
		/* fixed-offset abbreviation, return the offset */
		return TZNAME_FIXED_OFFSET;
	}
	else if (type == DYNTZ)
	{
		/* dynamic-offset abbreviation, return its referenced timezone */
		return TZNAME_DYNTZ;
	}
	else
	{
		/* try it as a full zone name */
		*tz = pg_tzset(tzname);
		if (*tz == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("time zone \"%s\" not recognized", tzname)));
		return TZNAME_ZONE;
	}
}

/* ==== VERBATIM: src/backend/utils/adt/datetime.c DecodeTimezoneNameToTz ==== */

pg_tz *
DecodeTimezoneNameToTz(const char *tzname)
{
	pg_tz	   *result;
	int			offset;

	if (DecodeTimezoneName(tzname, &offset, &result) == TZNAME_FIXED_OFFSET)
	{
		/* fixed-offset abbreviation, get a pg_tz descriptor for that */
		result = pg_tzset_offset(-offset);	/* flip to POSIX sign convention */
	}
	return result;
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c timestamp_cmp_internal ==== */

int
timestamp_cmp_internal(Timestamp dt1, Timestamp dt2)
{
	return (dt1 < dt2) ? -1 : ((dt1 > dt2) ? 1 : 0);
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c timestamp_smaller ==== */

Datum
timestamp_smaller(PG_FUNCTION_ARGS)
{
	Timestamp	dt1 = PG_GETARG_TIMESTAMP(0);
	Timestamp	dt2 = PG_GETARG_TIMESTAMP(1);
	Timestamp	result;

	/* use timestamp_cmp_internal to be sure this agrees with comparisons */
	if (timestamp_cmp_internal(dt1, dt2) < 0)
		result = dt1;
	else
		result = dt2;
	PG_RETURN_TIMESTAMP(result);
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c timestamp_larger ==== */

Datum
timestamp_larger(PG_FUNCTION_ARGS)
{
	Timestamp	dt1 = PG_GETARG_TIMESTAMP(0);
	Timestamp	dt2 = PG_GETARG_TIMESTAMP(1);
	Timestamp	result;

	if (timestamp_cmp_internal(dt1, dt2) > 0)
		result = dt1;
	else
		result = dt2;
	PG_RETURN_TIMESTAMP(result);
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c interval_cmp_value ==== */

static inline INT128
interval_cmp_value(const Interval *interval)
{
	INT128		span;
	int64		days;

	/*
	 * Combine the month and day fields into an integral number of days.
	 * Because the inputs are int32, int64 arithmetic suffices here.
	 */
	days = interval->month * INT64CONST(30);
	days += interval->day;

	/* Widen time field to 128 bits */
	span = int64_to_int128(interval->time);

	/* Scale up days to microseconds, forming a 128-bit product */
	int128_add_int64_mul_int64(&span, days, USECS_PER_DAY);

	return span;
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c interval_cmp_internal ==== */

static int
interval_cmp_internal(const Interval *interval1, const Interval *interval2)
{
	INT128		span1 = interval_cmp_value(interval1);
	INT128		span2 = interval_cmp_value(interval2);

	return int128_compare(span1, span2);
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c interval_sign ==== */

static int
interval_sign(const Interval *interval)
{
	INT128		span = interval_cmp_value(interval);
	INT128		zero = int64_to_int128(0);

	return int128_compare(span, zero);
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c interval_smaller ==== */

Datum
interval_smaller(PG_FUNCTION_ARGS)
{
	Interval   *interval1 = PG_GETARG_INTERVAL_P(0);
	Interval   *interval2 = PG_GETARG_INTERVAL_P(1);
	Interval   *result;

	/* use interval_cmp_internal to be sure this agrees with comparisons */
	if (interval_cmp_internal(interval1, interval2) < 0)
		result = interval1;
	else
		result = interval2;
	PG_RETURN_INTERVAL_P(result);
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c interval_larger ==== */

Datum
interval_larger(PG_FUNCTION_ARGS)
{
	Interval   *interval1 = PG_GETARG_INTERVAL_P(0);
	Interval   *interval2 = PG_GETARG_INTERVAL_P(1);
	Interval   *result;

	if (interval_cmp_internal(interval1, interval2) > 0)
		result = interval1;
	else
		result = interval2;
	PG_RETURN_INTERVAL_P(result);
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c timestamp_mi ==== */

Datum
timestamp_mi(PG_FUNCTION_ARGS)
{
	Timestamp	dt1 = PG_GETARG_TIMESTAMP(0);
	Timestamp	dt2 = PG_GETARG_TIMESTAMP(1);
	Interval   *result;

	result = (Interval *) palloc(sizeof(Interval));

	/*
	 * Handle infinities.
	 *
	 * We treat anything that amounts to "infinity - infinity" as an error,
	 * since the interval type has nothing equivalent to NaN.
	 */
	if (TIMESTAMP_NOT_FINITE(dt1) || TIMESTAMP_NOT_FINITE(dt2))
	{
		if (TIMESTAMP_IS_NOBEGIN(dt1))
		{
			if (TIMESTAMP_IS_NOBEGIN(dt2))
				ereport(ERROR,
						(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
						 errmsg("interval out of range")));
			else
				INTERVAL_NOBEGIN(result);
		}
		else if (TIMESTAMP_IS_NOEND(dt1))
		{
			if (TIMESTAMP_IS_NOEND(dt2))
				ereport(ERROR,
						(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
						 errmsg("interval out of range")));
			else
				INTERVAL_NOEND(result);
		}
		else if (TIMESTAMP_IS_NOBEGIN(dt2))
			INTERVAL_NOEND(result);
		else					/* TIMESTAMP_IS_NOEND(dt2) */
			INTERVAL_NOBEGIN(result);

		PG_RETURN_INTERVAL_P(result);
	}

	if (unlikely(pg_sub_s64_overflow(dt1, dt2, &result->time)))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("interval out of range")));

	result->month = 0;
	result->day = 0;

	/*----------
	 *	This is wrong, but removing it breaks a lot of regression tests.
	 *	For example:
	 *
	 *	test=> SET timezone = 'EST5EDT';
	 *	test=> SELECT
	 *	test-> ('2005-10-30 13:22:00-05'::timestamptz -
	 *	test(>	'2005-10-29 13:22:00-04'::timestamptz);
	 *	?column?
	 *	----------------
	 *	 1 day 01:00:00
	 *	 (1 row)
	 *
	 *	so adding that to the first timestamp gets:
	 *
	 *	 test=> SELECT
	 *	 test-> ('2005-10-29 13:22:00-04'::timestamptz +
	 *	 test(> ('2005-10-30 13:22:00-05'::timestamptz -
	 *	 test(>  '2005-10-29 13:22:00-04'::timestamptz)) at time zone 'EST';
	 *		timezone
	 *	--------------------
	 *	2005-10-30 14:22:00
	 *	(1 row)
	 *----------
	 */
	result = DatumGetIntervalP(DirectFunctionCall1(interval_justify_hours,
												   IntervalPGetDatum(result)));

	PG_RETURN_INTERVAL_P(result);
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c timestamp_pl_interval ==== */

Datum
timestamp_pl_interval(PG_FUNCTION_ARGS)
{
	Timestamp	timestamp = PG_GETARG_TIMESTAMP(0);
	Interval   *span = PG_GETARG_INTERVAL_P(1);
	Timestamp	result;

	/*
	 * Handle infinities.
	 *
	 * We treat anything that amounts to "infinity - infinity" as an error,
	 * since the timestamp type has nothing equivalent to NaN.
	 */
	if (INTERVAL_IS_NOBEGIN(span))
	{
		if (TIMESTAMP_IS_NOEND(timestamp))
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("timestamp out of range")));
		else
			TIMESTAMP_NOBEGIN(result);
	}
	else if (INTERVAL_IS_NOEND(span))
	{
		if (TIMESTAMP_IS_NOBEGIN(timestamp))
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("timestamp out of range")));
		else
			TIMESTAMP_NOEND(result);
	}
	else if (TIMESTAMP_NOT_FINITE(timestamp))
		result = timestamp;
	else
	{
		if (span->month != 0)
		{
			struct pg_tm tt,
					   *tm = &tt;
			fsec_t		fsec;

			if (timestamp2tm(timestamp, NULL, tm, &fsec, NULL, NULL) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
						 errmsg("timestamp out of range")));

			if (pg_add_s32_overflow(tm->tm_mon, span->month, &tm->tm_mon))
				ereport(ERROR,
						(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
						 errmsg("timestamp out of range")));
			if (tm->tm_mon > MONTHS_PER_YEAR)
			{
				tm->tm_year += (tm->tm_mon - 1) / MONTHS_PER_YEAR;
				tm->tm_mon = ((tm->tm_mon - 1) % MONTHS_PER_YEAR) + 1;
			}
			else if (tm->tm_mon < 1)
			{
				tm->tm_year += tm->tm_mon / MONTHS_PER_YEAR - 1;
				tm->tm_mon = tm->tm_mon % MONTHS_PER_YEAR + MONTHS_PER_YEAR;
			}

			/* adjust for end of month boundary problems... */
			if (tm->tm_mday > day_tab[isleap(tm->tm_year)][tm->tm_mon - 1])
				tm->tm_mday = (day_tab[isleap(tm->tm_year)][tm->tm_mon - 1]);

			if (tm2timestamp(tm, fsec, NULL, &timestamp) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
						 errmsg("timestamp out of range")));
		}

		if (span->day != 0)
		{
			struct pg_tm tt,
					   *tm = &tt;
			fsec_t		fsec;
			int			julian;

			if (timestamp2tm(timestamp, NULL, tm, &fsec, NULL, NULL) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
						 errmsg("timestamp out of range")));

			/*
			 * Add days by converting to and from Julian.  We need an overflow
			 * check here since j2date expects a non-negative integer input.
			 */
			julian = date2j(tm->tm_year, tm->tm_mon, tm->tm_mday);
			if (pg_add_s32_overflow(julian, span->day, &julian) ||
				julian < 0)
				ereport(ERROR,
						(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
						 errmsg("timestamp out of range")));
			j2date(julian, &tm->tm_year, &tm->tm_mon, &tm->tm_mday);

			if (tm2timestamp(tm, fsec, NULL, &timestamp) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
						 errmsg("timestamp out of range")));
		}

		if (pg_add_s64_overflow(timestamp, span->time, &timestamp))
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("timestamp out of range")));

		if (!IS_VALID_TIMESTAMP(timestamp))
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("timestamp out of range")));

		result = timestamp;
	}

	PG_RETURN_TIMESTAMP(result);
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c timestamp_mi_interval ==== */

Datum
timestamp_mi_interval(PG_FUNCTION_ARGS)
{
	Timestamp	timestamp = PG_GETARG_TIMESTAMP(0);
	Interval   *span = PG_GETARG_INTERVAL_P(1);
	Interval	tspan;

	interval_um_internal(span, &tspan);

	return DirectFunctionCall2(timestamp_pl_interval,
							   TimestampGetDatum(timestamp),
							   PointerGetDatum(&tspan));
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c timestamptz_pl_interval_internal ==== */

static TimestampTz
timestamptz_pl_interval_internal(TimestampTz timestamp,
								 Interval *span,
								 pg_tz *attimezone)
{
	TimestampTz result;
	int			tz;

	/*
	 * Handle infinities.
	 *
	 * We treat anything that amounts to "infinity - infinity" as an error,
	 * since the timestamptz type has nothing equivalent to NaN.
	 */
	if (INTERVAL_IS_NOBEGIN(span))
	{
		if (TIMESTAMP_IS_NOEND(timestamp))
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("timestamp out of range")));
		else
			TIMESTAMP_NOBEGIN(result);
	}
	else if (INTERVAL_IS_NOEND(span))
	{
		if (TIMESTAMP_IS_NOBEGIN(timestamp))
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("timestamp out of range")));
		else
			TIMESTAMP_NOEND(result);
	}
	else if (TIMESTAMP_NOT_FINITE(timestamp))
		result = timestamp;
	else
	{
		/* Use session timezone if caller asks for default */
		if (attimezone == NULL)
			attimezone = session_timezone;

		if (span->month != 0)
		{
			struct pg_tm tt,
					   *tm = &tt;
			fsec_t		fsec;

			if (timestamp2tm(timestamp, &tz, tm, &fsec, NULL, attimezone) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
						 errmsg("timestamp out of range")));

			if (pg_add_s32_overflow(tm->tm_mon, span->month, &tm->tm_mon))
				ereport(ERROR,
						(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
						 errmsg("timestamp out of range")));
			if (tm->tm_mon > MONTHS_PER_YEAR)
			{
				tm->tm_year += (tm->tm_mon - 1) / MONTHS_PER_YEAR;
				tm->tm_mon = ((tm->tm_mon - 1) % MONTHS_PER_YEAR) + 1;
			}
			else if (tm->tm_mon < 1)
			{
				tm->tm_year += tm->tm_mon / MONTHS_PER_YEAR - 1;
				tm->tm_mon = tm->tm_mon % MONTHS_PER_YEAR + MONTHS_PER_YEAR;
			}

			/* adjust for end of month boundary problems... */
			if (tm->tm_mday > day_tab[isleap(tm->tm_year)][tm->tm_mon - 1])
				tm->tm_mday = (day_tab[isleap(tm->tm_year)][tm->tm_mon - 1]);

			tz = DetermineTimeZoneOffset(tm, attimezone);

			if (tm2timestamp(tm, fsec, &tz, &timestamp) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
						 errmsg("timestamp out of range")));
		}

		if (span->day != 0)
		{
			struct pg_tm tt,
					   *tm = &tt;
			fsec_t		fsec;
			int			julian;

			if (timestamp2tm(timestamp, &tz, tm, &fsec, NULL, attimezone) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
						 errmsg("timestamp out of range")));

			/*
			 * Add days by converting to and from Julian.  We need an overflow
			 * check here since j2date expects a non-negative integer input.
			 * In practice though, it will give correct answers for small
			 * negative Julian dates; we should allow -1 to avoid
			 * timezone-dependent failures, as discussed in timestamp.h.
			 */
			julian = date2j(tm->tm_year, tm->tm_mon, tm->tm_mday);
			if (pg_add_s32_overflow(julian, span->day, &julian) ||
				julian < -1)
				ereport(ERROR,
						(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
						 errmsg("timestamp out of range")));
			j2date(julian, &tm->tm_year, &tm->tm_mon, &tm->tm_mday);

			tz = DetermineTimeZoneOffset(tm, attimezone);

			if (tm2timestamp(tm, fsec, &tz, &timestamp) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
						 errmsg("timestamp out of range")));
		}

		if (pg_add_s64_overflow(timestamp, span->time, &timestamp))
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("timestamp out of range")));

		if (!IS_VALID_TIMESTAMP(timestamp))
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("timestamp out of range")));

		result = timestamp;
	}

	return result;
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c timestamptz_mi_interval_internal ==== */

static TimestampTz
timestamptz_mi_interval_internal(TimestampTz timestamp,
								 Interval *span,
								 pg_tz *attimezone)
{
	Interval	tspan;

	interval_um_internal(span, &tspan);

	return timestamptz_pl_interval_internal(timestamp, &tspan, attimezone);
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c timestamptz_pl_interval ==== */

Datum
timestamptz_pl_interval(PG_FUNCTION_ARGS)
{
	TimestampTz timestamp = PG_GETARG_TIMESTAMPTZ(0);
	Interval   *span = PG_GETARG_INTERVAL_P(1);

	PG_RETURN_TIMESTAMP(timestamptz_pl_interval_internal(timestamp, span, NULL));
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c timestamptz_mi_interval ==== */

Datum
timestamptz_mi_interval(PG_FUNCTION_ARGS)
{
	TimestampTz timestamp = PG_GETARG_TIMESTAMPTZ(0);
	Interval   *span = PG_GETARG_INTERVAL_P(1);

	PG_RETURN_TIMESTAMP(timestamptz_mi_interval_internal(timestamp, span, NULL));
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c finite_interval_pl ==== */

static void
finite_interval_pl(const Interval *span1, const Interval *span2, Interval *result)
{
	Assert(!INTERVAL_NOT_FINITE(span1));
	Assert(!INTERVAL_NOT_FINITE(span2));

	if (pg_add_s32_overflow(span1->month, span2->month, &result->month) ||
		pg_add_s32_overflow(span1->day, span2->day, &result->day) ||
		pg_add_s64_overflow(span1->time, span2->time, &result->time) ||
		INTERVAL_NOT_FINITE(result))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("interval out of range")));
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c finite_interval_mi ==== */

static void
finite_interval_mi(const Interval *span1, const Interval *span2, Interval *result)
{
	Assert(!INTERVAL_NOT_FINITE(span1));
	Assert(!INTERVAL_NOT_FINITE(span2));

	if (pg_sub_s32_overflow(span1->month, span2->month, &result->month) ||
		pg_sub_s32_overflow(span1->day, span2->day, &result->day) ||
		pg_sub_s64_overflow(span1->time, span2->time, &result->time) ||
		INTERVAL_NOT_FINITE(result))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("interval out of range")));
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c interval_um_internal ==== */

static void
interval_um_internal(const Interval *interval, Interval *result)
{
	if (INTERVAL_IS_NOBEGIN(interval))
		INTERVAL_NOEND(result);
	else if (INTERVAL_IS_NOEND(interval))
		INTERVAL_NOBEGIN(result);
	else
	{
		/* Negate each field, guarding against overflow */
		if (pg_sub_s64_overflow(INT64CONST(0), interval->time, &result->time) ||
			pg_sub_s32_overflow(0, interval->day, &result->day) ||
			pg_sub_s32_overflow(0, interval->month, &result->month) ||
			INTERVAL_NOT_FINITE(result))
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("interval out of range")));
	}
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c interval_um ==== */

Datum
interval_um(PG_FUNCTION_ARGS)
{
	Interval   *interval = PG_GETARG_INTERVAL_P(0);
	Interval   *result;

	result = (Interval *) palloc(sizeof(Interval));
	interval_um_internal(interval, result);

	PG_RETURN_INTERVAL_P(result);
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c interval_pl ==== */

Datum
interval_pl(PG_FUNCTION_ARGS)
{
	Interval   *span1 = PG_GETARG_INTERVAL_P(0);
	Interval   *span2 = PG_GETARG_INTERVAL_P(1);
	Interval   *result;

	result = (Interval *) palloc(sizeof(Interval));

	/*
	 * Handle infinities.
	 *
	 * We treat anything that amounts to "infinity - infinity" as an error,
	 * since the interval type has nothing equivalent to NaN.
	 */
	if (INTERVAL_IS_NOBEGIN(span1))
	{
		if (INTERVAL_IS_NOEND(span2))
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("interval out of range")));
		else
			INTERVAL_NOBEGIN(result);
	}
	else if (INTERVAL_IS_NOEND(span1))
	{
		if (INTERVAL_IS_NOBEGIN(span2))
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("interval out of range")));
		else
			INTERVAL_NOEND(result);
	}
	else if (INTERVAL_NOT_FINITE(span2))
		memcpy(result, span2, sizeof(Interval));
	else
		finite_interval_pl(span1, span2, result);

	PG_RETURN_INTERVAL_P(result);
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c interval_mi ==== */

Datum
interval_mi(PG_FUNCTION_ARGS)
{
	Interval   *span1 = PG_GETARG_INTERVAL_P(0);
	Interval   *span2 = PG_GETARG_INTERVAL_P(1);
	Interval   *result;

	result = (Interval *) palloc(sizeof(Interval));

	/*
	 * Handle infinities.
	 *
	 * We treat anything that amounts to "infinity - infinity" as an error,
	 * since the interval type has nothing equivalent to NaN.
	 */
	if (INTERVAL_IS_NOBEGIN(span1))
	{
		if (INTERVAL_IS_NOBEGIN(span2))
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("interval out of range")));
		else
			INTERVAL_NOBEGIN(result);
	}
	else if (INTERVAL_IS_NOEND(span1))
	{
		if (INTERVAL_IS_NOEND(span2))
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("interval out of range")));
		else
			INTERVAL_NOEND(result);
	}
	else if (INTERVAL_IS_NOBEGIN(span2))
		INTERVAL_NOEND(result);
	else if (INTERVAL_IS_NOEND(span2))
		INTERVAL_NOBEGIN(result);
	else
		finite_interval_mi(span1, span2, result);

	PG_RETURN_INTERVAL_P(result);
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c interval_mul ==== */

Datum
interval_mul(PG_FUNCTION_ARGS)
{
	Interval   *span = PG_GETARG_INTERVAL_P(0);
	float8		factor = PG_GETARG_FLOAT8(1);
	double		month_remainder_days,
				sec_remainder,
				result_double;
	int32		orig_month = span->month,
				orig_day = span->day;
	Interval   *result;

	result = (Interval *) palloc(sizeof(Interval));

	/*
	 * Handle NaN and infinities.
	 *
	 * We treat "0 * infinity" and "infinity * 0" as errors, since the
	 * interval type has nothing equivalent to NaN.
	 */
	if (isnan(factor))
		goto out_of_range;

	if (INTERVAL_NOT_FINITE(span))
	{
		if (factor == 0.0)
			goto out_of_range;

		if (factor < 0.0)
			interval_um_internal(span, result);
		else
			memcpy(result, span, sizeof(Interval));

		PG_RETURN_INTERVAL_P(result);
	}
	if (isinf(factor))
	{
		int			isign = interval_sign(span);

		if (isign == 0)
			goto out_of_range;

		if (factor * isign < 0)
			INTERVAL_NOBEGIN(result);
		else
			INTERVAL_NOEND(result);

		PG_RETURN_INTERVAL_P(result);
	}

	result_double = span->month * factor;
	if (isnan(result_double) || !FLOAT8_FITS_IN_INT32(result_double))
		goto out_of_range;
	result->month = (int32) result_double;

	result_double = span->day * factor;
	if (isnan(result_double) || !FLOAT8_FITS_IN_INT32(result_double))
		goto out_of_range;
	result->day = (int32) result_double;

	/*
	 * The above correctly handles the whole-number part of the month and day
	 * products, but we have to do something with any fractional part
	 * resulting when the factor is non-integral.  We cascade the fractions
	 * down to lower units using the conversion factors DAYS_PER_MONTH and
	 * SECS_PER_DAY.  Note we do NOT cascade up, since we are not forced to do
	 * so by the representation.  The user can choose to cascade up later,
	 * using justify_hours and/or justify_days.
	 */

	/*
	 * Fractional months full days into days.
	 *
	 * Floating point calculation are inherently imprecise, so these
	 * calculations are crafted to produce the most reliable result possible.
	 * TSROUND() is needed to more accurately produce whole numbers where
	 * appropriate.
	 */
	month_remainder_days = (orig_month * factor - result->month) * DAYS_PER_MONTH;
	month_remainder_days = TSROUND(month_remainder_days);
	sec_remainder = (orig_day * factor - result->day +
					 month_remainder_days - (int) month_remainder_days) * SECS_PER_DAY;
	sec_remainder = TSROUND(sec_remainder);

	/*
	 * Might have 24:00:00 hours due to rounding, or >24 hours because of time
	 * cascade from months and days.  It might still be >24 if the combination
	 * of cascade and the seconds factor operation itself.
	 */
	if (fabs(sec_remainder) >= SECS_PER_DAY)
	{
		if (pg_add_s32_overflow(result->day,
								(int) (sec_remainder / SECS_PER_DAY),
								&result->day))
			goto out_of_range;
		sec_remainder -= (int) (sec_remainder / SECS_PER_DAY) * SECS_PER_DAY;
	}

	/* cascade units down */
	if (pg_add_s32_overflow(result->day, (int32) month_remainder_days,
							&result->day))
		goto out_of_range;
	result_double = rint(span->time * factor + sec_remainder * USECS_PER_SEC);
	if (isnan(result_double) || !FLOAT8_FITS_IN_INT64(result_double))
		goto out_of_range;
	result->time = (int64) result_double;

	if (INTERVAL_NOT_FINITE(result))
		goto out_of_range;

	PG_RETURN_INTERVAL_P(result);

out_of_range:
	ereport(ERROR,
			errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
			errmsg("interval out of range"));

	PG_RETURN_NULL();			/* keep compiler quiet */
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c mul_d_interval ==== */

Datum
mul_d_interval(PG_FUNCTION_ARGS)
{
	/* Args are float8 and Interval *, but leave them as generic Datum */
	Datum		factor = PG_GETARG_DATUM(0);
	Datum		span = PG_GETARG_DATUM(1);

	return DirectFunctionCall2(interval_mul, span, factor);
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c interval_div ==== */

Datum
interval_div(PG_FUNCTION_ARGS)
{
	Interval   *span = PG_GETARG_INTERVAL_P(0);
	float8		factor = PG_GETARG_FLOAT8(1);
	double		month_remainder_days,
				sec_remainder,
				result_double;
	int32		orig_month = span->month,
				orig_day = span->day;
	Interval   *result;

	result = (Interval *) palloc(sizeof(Interval));

	if (factor == 0.0)
		ereport(ERROR,
				(errcode(ERRCODE_DIVISION_BY_ZERO),
				 errmsg("division by zero")));

	/*
	 * Handle NaN and infinities.
	 *
	 * We treat "infinity / infinity" as an error, since the interval type has
	 * nothing equivalent to NaN.  Otherwise, dividing by infinity is handled
	 * by the regular division code, causing all fields to be set to zero.
	 */
	if (isnan(factor))
		goto out_of_range;

	if (INTERVAL_NOT_FINITE(span))
	{
		if (isinf(factor))
			goto out_of_range;

		if (factor < 0.0)
			interval_um_internal(span, result);
		else
			memcpy(result, span, sizeof(Interval));

		PG_RETURN_INTERVAL_P(result);
	}

	result_double = span->month / factor;
	if (isnan(result_double) || !FLOAT8_FITS_IN_INT32(result_double))
		goto out_of_range;
	result->month = (int32) result_double;

	result_double = span->day / factor;
	if (isnan(result_double) || !FLOAT8_FITS_IN_INT32(result_double))
		goto out_of_range;
	result->day = (int32) result_double;

	/*
	 * Fractional months full days into days.  See comment in interval_mul().
	 */
	month_remainder_days = (orig_month / factor - result->month) * DAYS_PER_MONTH;
	month_remainder_days = TSROUND(month_remainder_days);
	sec_remainder = (orig_day / factor - result->day +
					 month_remainder_days - (int) month_remainder_days) * SECS_PER_DAY;
	sec_remainder = TSROUND(sec_remainder);
	if (fabs(sec_remainder) >= SECS_PER_DAY)
	{
		if (pg_add_s32_overflow(result->day,
								(int) (sec_remainder / SECS_PER_DAY),
								&result->day))
			goto out_of_range;
		sec_remainder -= (int) (sec_remainder / SECS_PER_DAY) * SECS_PER_DAY;
	}

	/* cascade units down */
	if (pg_add_s32_overflow(result->day, (int32) month_remainder_days,
							&result->day))
		goto out_of_range;
	result_double = rint(span->time / factor + sec_remainder * USECS_PER_SEC);
	if (isnan(result_double) || !FLOAT8_FITS_IN_INT64(result_double))
		goto out_of_range;
	result->time = (int64) result_double;

	if (INTERVAL_NOT_FINITE(result))
		goto out_of_range;

	PG_RETURN_INTERVAL_P(result);

out_of_range:
	ereport(ERROR,
			errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
			errmsg("interval out of range"));

	PG_RETURN_NULL();			/* keep compiler quiet */
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c interval_justify_interval ==== */

Datum
interval_justify_interval(PG_FUNCTION_ARGS)
{
	Interval   *span = PG_GETARG_INTERVAL_P(0);
	Interval   *result;
	TimeOffset	wholeday;
	int32		wholemonth;

	result = (Interval *) palloc(sizeof(Interval));
	result->month = span->month;
	result->day = span->day;
	result->time = span->time;

	/* do nothing for infinite intervals */
	if (INTERVAL_NOT_FINITE(result))
		PG_RETURN_INTERVAL_P(result);

	/* pre-justify days if it might prevent overflow */
	if ((result->day > 0 && result->time > 0) ||
		(result->day < 0 && result->time < 0))
	{
		wholemonth = result->day / DAYS_PER_MONTH;
		result->day -= wholemonth * DAYS_PER_MONTH;
		if (pg_add_s32_overflow(result->month, wholemonth, &result->month))
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("interval out of range")));
	}

	/*
	 * Since TimeOffset is int64, abs(wholeday) can't exceed about 1.07e8.  If
	 * we pre-justified then abs(result->day) is less than DAYS_PER_MONTH, so
	 * this addition can't overflow.  If we didn't pre-justify, then day and
	 * time are of different signs, so it still can't overflow.
	 */
	TMODULO(result->time, wholeday, USECS_PER_DAY);
	result->day += wholeday;

	wholemonth = result->day / DAYS_PER_MONTH;
	result->day -= wholemonth * DAYS_PER_MONTH;
	if (pg_add_s32_overflow(result->month, wholemonth, &result->month))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("interval out of range")));

	if (result->month > 0 &&
		(result->day < 0 || (result->day == 0 && result->time < 0)))
	{
		result->day += DAYS_PER_MONTH;
		result->month--;
	}
	else if (result->month < 0 &&
			 (result->day > 0 || (result->day == 0 && result->time > 0)))
	{
		result->day -= DAYS_PER_MONTH;
		result->month++;
	}

	if (result->day > 0 && result->time < 0)
	{
		result->time += USECS_PER_DAY;
		result->day--;
	}
	else if (result->day < 0 && result->time > 0)
	{
		result->time -= USECS_PER_DAY;
		result->day++;
	}

	PG_RETURN_INTERVAL_P(result);
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c interval_justify_hours ==== */

Datum
interval_justify_hours(PG_FUNCTION_ARGS)
{
	Interval   *span = PG_GETARG_INTERVAL_P(0);
	Interval   *result;
	TimeOffset	wholeday;

	result = (Interval *) palloc(sizeof(Interval));
	result->month = span->month;
	result->day = span->day;
	result->time = span->time;

	/* do nothing for infinite intervals */
	if (INTERVAL_NOT_FINITE(result))
		PG_RETURN_INTERVAL_P(result);

	TMODULO(result->time, wholeday, USECS_PER_DAY);
	if (pg_add_s32_overflow(result->day, wholeday, &result->day))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("interval out of range")));

	if (result->day > 0 && result->time < 0)
	{
		result->time += USECS_PER_DAY;
		result->day--;
	}
	else if (result->day < 0 && result->time > 0)
	{
		result->time -= USECS_PER_DAY;
		result->day++;
	}

	PG_RETURN_INTERVAL_P(result);
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c interval_justify_days ==== */

Datum
interval_justify_days(PG_FUNCTION_ARGS)
{
	Interval   *span = PG_GETARG_INTERVAL_P(0);
	Interval   *result;
	int32		wholemonth;

	result = (Interval *) palloc(sizeof(Interval));
	result->month = span->month;
	result->day = span->day;
	result->time = span->time;

	/* do nothing for infinite intervals */
	if (INTERVAL_NOT_FINITE(result))
		PG_RETURN_INTERVAL_P(result);

	wholemonth = result->day / DAYS_PER_MONTH;
	result->day -= wholemonth * DAYS_PER_MONTH;
	if (pg_add_s32_overflow(result->month, wholemonth, &result->month))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("interval out of range")));

	if (result->month > 0 && result->day < 0)
	{
		result->day += DAYS_PER_MONTH;
		result->month--;
	}
	else if (result->month < 0 && result->day > 0)
	{
		result->day -= DAYS_PER_MONTH;
		result->month++;
	}

	PG_RETURN_INTERVAL_P(result);
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c timestamp_age ==== */

Datum
timestamp_age(PG_FUNCTION_ARGS)
{
	Timestamp	dt1 = PG_GETARG_TIMESTAMP(0);
	Timestamp	dt2 = PG_GETARG_TIMESTAMP(1);
	Interval   *result;
	fsec_t		fsec1,
				fsec2;
	struct pg_itm tt,
			   *tm = &tt;
	struct pg_tm tt1,
			   *tm1 = &tt1;
	struct pg_tm tt2,
			   *tm2 = &tt2;

	result = (Interval *) palloc(sizeof(Interval));

	/*
	 * Handle infinities.
	 *
	 * We treat anything that amounts to "infinity - infinity" as an error,
	 * since the interval type has nothing equivalent to NaN.
	 */
	if (TIMESTAMP_IS_NOBEGIN(dt1))
	{
		if (TIMESTAMP_IS_NOBEGIN(dt2))
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("interval out of range")));
		else
			INTERVAL_NOBEGIN(result);
	}
	else if (TIMESTAMP_IS_NOEND(dt1))
	{
		if (TIMESTAMP_IS_NOEND(dt2))
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("interval out of range")));
		else
			INTERVAL_NOEND(result);
	}
	else if (TIMESTAMP_IS_NOBEGIN(dt2))
		INTERVAL_NOEND(result);
	else if (TIMESTAMP_IS_NOEND(dt2))
		INTERVAL_NOBEGIN(result);
	else if (timestamp2tm(dt1, NULL, tm1, &fsec1, NULL, NULL) == 0 &&
			 timestamp2tm(dt2, NULL, tm2, &fsec2, NULL, NULL) == 0)
	{
		/* form the symbolic difference */
		tm->tm_usec = fsec1 - fsec2;
		tm->tm_sec = tm1->tm_sec - tm2->tm_sec;
		tm->tm_min = tm1->tm_min - tm2->tm_min;
		tm->tm_hour = tm1->tm_hour - tm2->tm_hour;
		tm->tm_mday = tm1->tm_mday - tm2->tm_mday;
		tm->tm_mon = tm1->tm_mon - tm2->tm_mon;
		tm->tm_year = tm1->tm_year - tm2->tm_year;

		/* flip sign if necessary... */
		if (dt1 < dt2)
		{
			tm->tm_usec = -tm->tm_usec;
			tm->tm_sec = -tm->tm_sec;
			tm->tm_min = -tm->tm_min;
			tm->tm_hour = -tm->tm_hour;
			tm->tm_mday = -tm->tm_mday;
			tm->tm_mon = -tm->tm_mon;
			tm->tm_year = -tm->tm_year;
		}

		/* propagate any negative fields into the next higher field */
		while (tm->tm_usec < 0)
		{
			tm->tm_usec += USECS_PER_SEC;
			tm->tm_sec--;
		}

		while (tm->tm_sec < 0)
		{
			tm->tm_sec += SECS_PER_MINUTE;
			tm->tm_min--;
		}

		while (tm->tm_min < 0)
		{
			tm->tm_min += MINS_PER_HOUR;
			tm->tm_hour--;
		}

		while (tm->tm_hour < 0)
		{
			tm->tm_hour += HOURS_PER_DAY;
			tm->tm_mday--;
		}

		while (tm->tm_mday < 0)
		{
			if (dt1 < dt2)
			{
				tm->tm_mday += day_tab[isleap(tm1->tm_year)][tm1->tm_mon - 1];
				tm->tm_mon--;
			}
			else
			{
				tm->tm_mday += day_tab[isleap(tm2->tm_year)][tm2->tm_mon - 1];
				tm->tm_mon--;
			}
		}

		while (tm->tm_mon < 0)
		{
			tm->tm_mon += MONTHS_PER_YEAR;
			tm->tm_year--;
		}

		/* recover sign if necessary... */
		if (dt1 < dt2)
		{
			tm->tm_usec = -tm->tm_usec;
			tm->tm_sec = -tm->tm_sec;
			tm->tm_min = -tm->tm_min;
			tm->tm_hour = -tm->tm_hour;
			tm->tm_mday = -tm->tm_mday;
			tm->tm_mon = -tm->tm_mon;
			tm->tm_year = -tm->tm_year;
		}

		if (itm2interval(tm, result) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("interval out of range")));
	}
	else
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	PG_RETURN_INTERVAL_P(result);
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c timestamptz_age ==== */

Datum
timestamptz_age(PG_FUNCTION_ARGS)
{
	TimestampTz dt1 = PG_GETARG_TIMESTAMPTZ(0);
	TimestampTz dt2 = PG_GETARG_TIMESTAMPTZ(1);
	Interval   *result;
	fsec_t		fsec1,
				fsec2;
	struct pg_itm tt,
			   *tm = &tt;
	struct pg_tm tt1,
			   *tm1 = &tt1;
	struct pg_tm tt2,
			   *tm2 = &tt2;
	int			tz1;
	int			tz2;

	result = (Interval *) palloc(sizeof(Interval));

	/*
	 * Handle infinities.
	 *
	 * We treat anything that amounts to "infinity - infinity" as an error,
	 * since the interval type has nothing equivalent to NaN.
	 */
	if (TIMESTAMP_IS_NOBEGIN(dt1))
	{
		if (TIMESTAMP_IS_NOBEGIN(dt2))
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("interval out of range")));
		else
			INTERVAL_NOBEGIN(result);
	}
	else if (TIMESTAMP_IS_NOEND(dt1))
	{
		if (TIMESTAMP_IS_NOEND(dt2))
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("interval out of range")));
		else
			INTERVAL_NOEND(result);
	}
	else if (TIMESTAMP_IS_NOBEGIN(dt2))
		INTERVAL_NOEND(result);
	else if (TIMESTAMP_IS_NOEND(dt2))
		INTERVAL_NOBEGIN(result);
	else if (timestamp2tm(dt1, &tz1, tm1, &fsec1, NULL, NULL) == 0 &&
			 timestamp2tm(dt2, &tz2, tm2, &fsec2, NULL, NULL) == 0)
	{
		/* form the symbolic difference */
		tm->tm_usec = fsec1 - fsec2;
		tm->tm_sec = tm1->tm_sec - tm2->tm_sec;
		tm->tm_min = tm1->tm_min - tm2->tm_min;
		tm->tm_hour = tm1->tm_hour - tm2->tm_hour;
		tm->tm_mday = tm1->tm_mday - tm2->tm_mday;
		tm->tm_mon = tm1->tm_mon - tm2->tm_mon;
		tm->tm_year = tm1->tm_year - tm2->tm_year;

		/* flip sign if necessary... */
		if (dt1 < dt2)
		{
			tm->tm_usec = -tm->tm_usec;
			tm->tm_sec = -tm->tm_sec;
			tm->tm_min = -tm->tm_min;
			tm->tm_hour = -tm->tm_hour;
			tm->tm_mday = -tm->tm_mday;
			tm->tm_mon = -tm->tm_mon;
			tm->tm_year = -tm->tm_year;
		}

		/* propagate any negative fields into the next higher field */
		while (tm->tm_usec < 0)
		{
			tm->tm_usec += USECS_PER_SEC;
			tm->tm_sec--;
		}

		while (tm->tm_sec < 0)
		{
			tm->tm_sec += SECS_PER_MINUTE;
			tm->tm_min--;
		}

		while (tm->tm_min < 0)
		{
			tm->tm_min += MINS_PER_HOUR;
			tm->tm_hour--;
		}

		while (tm->tm_hour < 0)
		{
			tm->tm_hour += HOURS_PER_DAY;
			tm->tm_mday--;
		}

		while (tm->tm_mday < 0)
		{
			if (dt1 < dt2)
			{
				tm->tm_mday += day_tab[isleap(tm1->tm_year)][tm1->tm_mon - 1];
				tm->tm_mon--;
			}
			else
			{
				tm->tm_mday += day_tab[isleap(tm2->tm_year)][tm2->tm_mon - 1];
				tm->tm_mon--;
			}
		}

		while (tm->tm_mon < 0)
		{
			tm->tm_mon += MONTHS_PER_YEAR;
			tm->tm_year--;
		}

		/*
		 * Note: we deliberately ignore any difference between tz1 and tz2.
		 */

		/* recover sign if necessary... */
		if (dt1 < dt2)
		{
			tm->tm_usec = -tm->tm_usec;
			tm->tm_sec = -tm->tm_sec;
			tm->tm_min = -tm->tm_min;
			tm->tm_hour = -tm->tm_hour;
			tm->tm_mday = -tm->tm_mday;
			tm->tm_mon = -tm->tm_mon;
			tm->tm_year = -tm->tm_year;
		}

		if (itm2interval(tm, result) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("interval out of range")));
	}
	else
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	PG_RETURN_INTERVAL_P(result);
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c timestamp_bin ==== */

Datum
timestamp_bin(PG_FUNCTION_ARGS)
{
	Interval   *stride = PG_GETARG_INTERVAL_P(0);
	Timestamp	timestamp = PG_GETARG_TIMESTAMP(1);
	Timestamp	origin = PG_GETARG_TIMESTAMP(2);
	Timestamp	result,
				stride_usecs,
				tm_diff,
				tm_modulo,
				tm_delta;

	if (TIMESTAMP_NOT_FINITE(timestamp))
		PG_RETURN_TIMESTAMP(timestamp);

	if (TIMESTAMP_NOT_FINITE(origin))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("origin out of range")));

	if (INTERVAL_NOT_FINITE(stride))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamps cannot be binned into infinite intervals")));

	if (stride->month != 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("timestamps cannot be binned into intervals containing months or years")));

	if (unlikely(pg_mul_s64_overflow(stride->day, USECS_PER_DAY, &stride_usecs)) ||
		unlikely(pg_add_s64_overflow(stride_usecs, stride->time, &stride_usecs)))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("interval out of range")));

	if (stride_usecs <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("stride must be greater than zero")));

	if (unlikely(pg_sub_s64_overflow(timestamp, origin, &tm_diff)))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("interval out of range")));

	/* These calculations cannot overflow */
	tm_modulo = tm_diff % stride_usecs;
	tm_delta = tm_diff - tm_modulo;
	result = origin + tm_delta;

	/*
	 * We want to round towards -infinity, not 0, when tm_diff is negative and
	 * not a multiple of stride_usecs.  This adjustment *can* cause overflow,
	 * since the result might now be out of the range origin .. timestamp.
	 */
	if (tm_modulo < 0)
	{
		if (unlikely(pg_sub_s64_overflow(result, stride_usecs, &result)) ||
			!IS_VALID_TIMESTAMP(result))
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("timestamp out of range")));
	}

	PG_RETURN_TIMESTAMP(result);
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c timestamptz_bin ==== */

Datum
timestamptz_bin(PG_FUNCTION_ARGS)
{
	Interval   *stride = PG_GETARG_INTERVAL_P(0);
	TimestampTz timestamp = PG_GETARG_TIMESTAMPTZ(1);
	TimestampTz origin = PG_GETARG_TIMESTAMPTZ(2);
	TimestampTz result,
				stride_usecs,
				tm_diff,
				tm_modulo,
				tm_delta;

	if (TIMESTAMP_NOT_FINITE(timestamp))
		PG_RETURN_TIMESTAMPTZ(timestamp);

	if (TIMESTAMP_NOT_FINITE(origin))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("origin out of range")));

	if (INTERVAL_NOT_FINITE(stride))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamps cannot be binned into infinite intervals")));

	if (stride->month != 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("timestamps cannot be binned into intervals containing months or years")));

	if (unlikely(pg_mul_s64_overflow(stride->day, USECS_PER_DAY, &stride_usecs)) ||
		unlikely(pg_add_s64_overflow(stride_usecs, stride->time, &stride_usecs)))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("interval out of range")));

	if (stride_usecs <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("stride must be greater than zero")));

	if (unlikely(pg_sub_s64_overflow(timestamp, origin, &tm_diff)))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("interval out of range")));

	/* These calculations cannot overflow */
	tm_modulo = tm_diff % stride_usecs;
	tm_delta = tm_diff - tm_modulo;
	result = origin + tm_delta;

	/*
	 * We want to round towards -infinity, not 0, when tm_diff is negative and
	 * not a multiple of stride_usecs.  This adjustment *can* cause overflow,
	 * since the result might now be out of the range origin .. timestamp.
	 */
	if (tm_modulo < 0)
	{
		if (unlikely(pg_sub_s64_overflow(result, stride_usecs, &result)) ||
			!IS_VALID_TIMESTAMP(result))
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("timestamp out of range")));
	}

	PG_RETURN_TIMESTAMPTZ(result);
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c timestamp_izone ==== */

Datum
timestamp_izone(PG_FUNCTION_ARGS)
{
	Interval   *zone = PG_GETARG_INTERVAL_P(0);
	Timestamp	timestamp = PG_GETARG_TIMESTAMP(1);
	TimestampTz result;
	int			tz;

	if (TIMESTAMP_NOT_FINITE(timestamp))
		PG_RETURN_TIMESTAMPTZ(timestamp);

	if (INTERVAL_NOT_FINITE(zone))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("interval time zone \"%s\" must be finite",
						DatumGetCString(DirectFunctionCall1(interval_out,
															PointerGetDatum(zone))))));

	if (zone->month != 0 || zone->day != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("interval time zone \"%s\" must not include months or days",
						DatumGetCString(DirectFunctionCall1(interval_out,
															PointerGetDatum(zone))))));

	tz = zone->time / USECS_PER_SEC;

	result = dt2local(timestamp, tz);

	if (!IS_VALID_TIMESTAMP(result))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	PG_RETURN_TIMESTAMPTZ(result);
}								/* timestamp_izone() */

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c timestamptz_izone ==== */

Datum
timestamptz_izone(PG_FUNCTION_ARGS)
{
	Interval   *zone = PG_GETARG_INTERVAL_P(0);
	TimestampTz timestamp = PG_GETARG_TIMESTAMPTZ(1);
	Timestamp	result;
	int			tz;

	if (TIMESTAMP_NOT_FINITE(timestamp))
		PG_RETURN_TIMESTAMP(timestamp);

	if (INTERVAL_NOT_FINITE(zone))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("interval time zone \"%s\" must be finite",
						DatumGetCString(DirectFunctionCall1(interval_out,
															PointerGetDatum(zone))))));

	if (zone->month != 0 || zone->day != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("interval time zone \"%s\" must not include months or days",
						DatumGetCString(DirectFunctionCall1(interval_out,
															PointerGetDatum(zone))))));

	tz = -(zone->time / USECS_PER_SEC);

	result = dt2local(timestamp, tz);

	if (!IS_VALID_TIMESTAMP(result))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	PG_RETURN_TIMESTAMP(result);
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c TimestampDifference ==== */

void
TimestampDifference(TimestampTz start_time, TimestampTz stop_time,
					long *secs, int *microsecs)
{
	TimestampTz diff = stop_time - start_time;

	if (diff <= 0)
	{
		*secs = 0;
		*microsecs = 0;
	}
	else
	{
		*secs = (long) (diff / USECS_PER_SEC);
		*microsecs = (int) (diff % USECS_PER_SEC);
	}
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c TimestampDifferenceMilliseconds ==== */

long
TimestampDifferenceMilliseconds(TimestampTz start_time, TimestampTz stop_time)
{
	TimestampTz diff;

	/* Deal with zero or negative elapsed time quickly. */
	if (start_time >= stop_time)
		return 0;
	/* To not fail with timestamp infinities, we must detect overflow. */
	if (pg_sub_s64_overflow(stop_time, start_time, &diff))
		return (long) INT_MAX;
	if (diff >= (INT_MAX * INT64CONST(1000) - 999))
		return (long) INT_MAX;
	else
		return (long) ((diff + 999) / 1000);
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c TimestampDifferenceExceeds ==== */

bool
TimestampDifferenceExceeds(TimestampTz start_time,
						   TimestampTz stop_time,
						   int msec)
{
	TimestampTz diff = stop_time - start_time;

	return (diff >= msec * INT64CONST(1000));
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c TimestampDifferenceExceedsSeconds ==== */

bool
TimestampDifferenceExceedsSeconds(TimestampTz start_time,
								  TimestampTz stop_time,
								  int threshold_sec)
{
	long		secs;
	int			usecs;

	/* Calculate the difference in seconds */
	TimestampDifference(start_time, stop_time, &secs, &usecs);

	return (secs >= threshold_sec);
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c timestamp_trunc ==== */

Datum
timestamp_trunc(PG_FUNCTION_ARGS)
{
	text	   *units = PG_GETARG_TEXT_PP(0);
	Timestamp	timestamp = PG_GETARG_TIMESTAMP(1);
	Timestamp	result;
	int			type,
				val;
	char	   *lowunits;
	fsec_t		fsec;
	struct pg_tm tt,
			   *tm = &tt;

	lowunits = downcase_truncate_identifier(VARDATA_ANY(units),
											VARSIZE_ANY_EXHDR(units),
											false);

	type = DecodeUnits(0, lowunits, &val);

	if (type == UNITS)
	{
		if (TIMESTAMP_NOT_FINITE(timestamp))
		{
			/*
			 * Errors thrown here for invalid units should exactly match those
			 * below, else there will be unexpected discrepancies between
			 * finite- and infinite-input cases.
			 */
			switch (val)
			{
				case DTK_WEEK:
				case DTK_MILLENNIUM:
				case DTK_CENTURY:
				case DTK_DECADE:
				case DTK_YEAR:
				case DTK_QUARTER:
				case DTK_MONTH:
				case DTK_DAY:
				case DTK_HOUR:
				case DTK_MINUTE:
				case DTK_SECOND:
				case DTK_MILLISEC:
				case DTK_MICROSEC:
					PG_RETURN_TIMESTAMP(timestamp);
					break;
				default:
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("unit \"%s\" not supported for type %s",
									lowunits, format_type_be(TIMESTAMPOID))));
					result = 0;
			}
		}

		if (timestamp2tm(timestamp, NULL, tm, &fsec, NULL, NULL) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("timestamp out of range")));

		switch (val)
		{
			case DTK_WEEK:
				{
					int			woy;

					woy = date2isoweek(tm->tm_year, tm->tm_mon, tm->tm_mday);

					/*
					 * If it is week 52/53 and the month is January, then the
					 * week must belong to the previous year. Also, some
					 * December dates belong to the next year.
					 */
					if (woy >= 52 && tm->tm_mon == 1)
						--tm->tm_year;
					if (woy <= 1 && tm->tm_mon == MONTHS_PER_YEAR)
						++tm->tm_year;
					isoweek2date(woy, &(tm->tm_year), &(tm->tm_mon), &(tm->tm_mday));
					tm->tm_hour = 0;
					tm->tm_min = 0;
					tm->tm_sec = 0;
					fsec = 0;
					break;
				}
			case DTK_MILLENNIUM:
				/* see comments in timestamptz_trunc */
				if (tm->tm_year > 0)
					tm->tm_year = ((tm->tm_year + 999) / 1000) * 1000 - 999;
				else
					tm->tm_year = -((999 - (tm->tm_year - 1)) / 1000) * 1000 + 1;
				/* FALL THRU */
			case DTK_CENTURY:
				/* see comments in timestamptz_trunc */
				if (tm->tm_year > 0)
					tm->tm_year = ((tm->tm_year + 99) / 100) * 100 - 99;
				else
					tm->tm_year = -((99 - (tm->tm_year - 1)) / 100) * 100 + 1;
				/* FALL THRU */
			case DTK_DECADE:
				/* see comments in timestamptz_trunc */
				if (val != DTK_MILLENNIUM && val != DTK_CENTURY)
				{
					if (tm->tm_year > 0)
						tm->tm_year = (tm->tm_year / 10) * 10;
					else
						tm->tm_year = -((8 - (tm->tm_year - 1)) / 10) * 10;
				}
				/* FALL THRU */
			case DTK_YEAR:
				tm->tm_mon = 1;
				/* FALL THRU */
			case DTK_QUARTER:
				tm->tm_mon = (3 * ((tm->tm_mon - 1) / 3)) + 1;
				/* FALL THRU */
			case DTK_MONTH:
				tm->tm_mday = 1;
				/* FALL THRU */
			case DTK_DAY:
				tm->tm_hour = 0;
				/* FALL THRU */
			case DTK_HOUR:
				tm->tm_min = 0;
				/* FALL THRU */
			case DTK_MINUTE:
				tm->tm_sec = 0;
				/* FALL THRU */
			case DTK_SECOND:
				fsec = 0;
				break;

			case DTK_MILLISEC:
				fsec = (fsec / 1000) * 1000;
				break;

			case DTK_MICROSEC:
				break;

			default:
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("unit \"%s\" not supported for type %s",
								lowunits, format_type_be(TIMESTAMPOID))));
				result = 0;
		}

		if (tm2timestamp(tm, fsec, NULL, &result) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("timestamp out of range")));
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unit \"%s\" not recognized for type %s",
						lowunits, format_type_be(TIMESTAMPOID))));
		result = 0;
	}

	PG_RETURN_TIMESTAMP(result);
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c timestamptz_trunc_internal ==== */

static TimestampTz
timestamptz_trunc_internal(text *units, TimestampTz timestamp, pg_tz *tzp)
{
	TimestampTz result;
	int			tz;
	int			type,
				val;
	bool		redotz = false;
	char	   *lowunits;
	fsec_t		fsec;
	struct pg_tm tt,
			   *tm = &tt;

	lowunits = downcase_truncate_identifier(VARDATA_ANY(units),
											VARSIZE_ANY_EXHDR(units),
											false);

	type = DecodeUnits(0, lowunits, &val);

	if (type == UNITS)
	{
		if (TIMESTAMP_NOT_FINITE(timestamp))
		{
			/*
			 * Errors thrown here for invalid units should exactly match those
			 * below, else there will be unexpected discrepancies between
			 * finite- and infinite-input cases.
			 */
			switch (val)
			{
				case DTK_WEEK:
				case DTK_MILLENNIUM:
				case DTK_CENTURY:
				case DTK_DECADE:
				case DTK_YEAR:
				case DTK_QUARTER:
				case DTK_MONTH:
				case DTK_DAY:
				case DTK_HOUR:
				case DTK_MINUTE:
				case DTK_SECOND:
				case DTK_MILLISEC:
				case DTK_MICROSEC:
					return timestamp;
					break;

				default:
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("unit \"%s\" not supported for type %s",
									lowunits, format_type_be(TIMESTAMPTZOID))));
					result = 0;
			}
		}

		if (timestamp2tm(timestamp, &tz, tm, &fsec, NULL, tzp) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("timestamp out of range")));

		switch (val)
		{
			case DTK_WEEK:
				{
					int			woy;

					woy = date2isoweek(tm->tm_year, tm->tm_mon, tm->tm_mday);

					/*
					 * If it is week 52/53 and the month is January, then the
					 * week must belong to the previous year. Also, some
					 * December dates belong to the next year.
					 */
					if (woy >= 52 && tm->tm_mon == 1)
						--tm->tm_year;
					if (woy <= 1 && tm->tm_mon == MONTHS_PER_YEAR)
						++tm->tm_year;
					isoweek2date(woy, &(tm->tm_year), &(tm->tm_mon), &(tm->tm_mday));
					tm->tm_hour = 0;
					tm->tm_min = 0;
					tm->tm_sec = 0;
					fsec = 0;
					redotz = true;
					break;
				}
				/* one may consider DTK_THOUSAND and DTK_HUNDRED... */
			case DTK_MILLENNIUM:

				/*
				 * truncating to the millennium? what is this supposed to
				 * mean? let us put the first year of the millennium... i.e.
				 * -1000, 1, 1001, 2001...
				 */
				if (tm->tm_year > 0)
					tm->tm_year = ((tm->tm_year + 999) / 1000) * 1000 - 999;
				else
					tm->tm_year = -((999 - (tm->tm_year - 1)) / 1000) * 1000 + 1;
				/* FALL THRU */
			case DTK_CENTURY:
				/* truncating to the century? as above: -100, 1, 101... */
				if (tm->tm_year > 0)
					tm->tm_year = ((tm->tm_year + 99) / 100) * 100 - 99;
				else
					tm->tm_year = -((99 - (tm->tm_year - 1)) / 100) * 100 + 1;
				/* FALL THRU */
			case DTK_DECADE:

				/*
				 * truncating to the decade? first year of the decade. must
				 * not be applied if year was truncated before!
				 */
				if (val != DTK_MILLENNIUM && val != DTK_CENTURY)
				{
					if (tm->tm_year > 0)
						tm->tm_year = (tm->tm_year / 10) * 10;
					else
						tm->tm_year = -((8 - (tm->tm_year - 1)) / 10) * 10;
				}
				/* FALL THRU */
			case DTK_YEAR:
				tm->tm_mon = 1;
				/* FALL THRU */
			case DTK_QUARTER:
				tm->tm_mon = (3 * ((tm->tm_mon - 1) / 3)) + 1;
				/* FALL THRU */
			case DTK_MONTH:
				tm->tm_mday = 1;
				/* FALL THRU */
			case DTK_DAY:
				tm->tm_hour = 0;
				redotz = true;	/* for all cases >= DAY */
				/* FALL THRU */
			case DTK_HOUR:
				tm->tm_min = 0;
				/* FALL THRU */
			case DTK_MINUTE:
				tm->tm_sec = 0;
				/* FALL THRU */
			case DTK_SECOND:
				fsec = 0;
				break;
			case DTK_MILLISEC:
				fsec = (fsec / 1000) * 1000;
				break;
			case DTK_MICROSEC:
				break;

			default:
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("unit \"%s\" not supported for type %s",
								lowunits, format_type_be(TIMESTAMPTZOID))));
				result = 0;
		}

		if (redotz)
			tz = DetermineTimeZoneOffset(tm, tzp);

		if (tm2timestamp(tm, fsec, &tz, &result) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("timestamp out of range")));
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unit \"%s\" not recognized for type %s",
						lowunits, format_type_be(TIMESTAMPTZOID))));
		result = 0;
	}

	return result;
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c timestamptz_trunc ==== */

Datum
timestamptz_trunc(PG_FUNCTION_ARGS)
{
	text	   *units = PG_GETARG_TEXT_PP(0);
	TimestampTz timestamp = PG_GETARG_TIMESTAMPTZ(1);
	TimestampTz result;

	result = timestamptz_trunc_internal(units, timestamp, session_timezone);

	PG_RETURN_TIMESTAMPTZ(result);
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c timestamptz_trunc_zone ==== */

Datum
timestamptz_trunc_zone(PG_FUNCTION_ARGS)
{
	text	   *units = PG_GETARG_TEXT_PP(0);
	TimestampTz timestamp = PG_GETARG_TIMESTAMPTZ(1);
	text	   *zone = PG_GETARG_TEXT_PP(2);
	TimestampTz result;
	pg_tz	   *tzp;

	/*
	 * Look up the requested timezone.
	 */
	tzp = lookup_timezone(zone);

	result = timestamptz_trunc_internal(units, timestamp, tzp);

	PG_RETURN_TIMESTAMPTZ(result);
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c interval_trunc ==== */

Datum
interval_trunc(PG_FUNCTION_ARGS)
{
	text	   *units = PG_GETARG_TEXT_PP(0);
	Interval   *interval = PG_GETARG_INTERVAL_P(1);
	Interval   *result;
	int			type,
				val;
	char	   *lowunits;
	struct pg_itm tt,
			   *tm = &tt;

	result = (Interval *) palloc(sizeof(Interval));

	lowunits = downcase_truncate_identifier(VARDATA_ANY(units),
											VARSIZE_ANY_EXHDR(units),
											false);

	type = DecodeUnits(0, lowunits, &val);

	if (type == UNITS)
	{
		if (INTERVAL_NOT_FINITE(interval))
		{
			/*
			 * Errors thrown here for invalid units should exactly match those
			 * below, else there will be unexpected discrepancies between
			 * finite- and infinite-input cases.
			 */
			switch (val)
			{
				case DTK_MILLENNIUM:
				case DTK_CENTURY:
				case DTK_DECADE:
				case DTK_YEAR:
				case DTK_QUARTER:
				case DTK_MONTH:
				case DTK_DAY:
				case DTK_HOUR:
				case DTK_MINUTE:
				case DTK_SECOND:
				case DTK_MILLISEC:
				case DTK_MICROSEC:
					memcpy(result, interval, sizeof(Interval));
					PG_RETURN_INTERVAL_P(result);
					break;

				default:
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("unit \"%s\" not supported for type %s",
									lowunits, format_type_be(INTERVALOID)),
							 (val == DTK_WEEK) ? errdetail("Months usually have fractional weeks.") : 0));
					result = 0;
			}
		}

		interval2itm(*interval, tm);
		switch (val)
		{
			case DTK_MILLENNIUM:
				/* caution: C division may have negative remainder */
				tm->tm_year = (tm->tm_year / 1000) * 1000;
				/* FALL THRU */
			case DTK_CENTURY:
				/* caution: C division may have negative remainder */
				tm->tm_year = (tm->tm_year / 100) * 100;
				/* FALL THRU */
			case DTK_DECADE:
				/* caution: C division may have negative remainder */
				tm->tm_year = (tm->tm_year / 10) * 10;
				/* FALL THRU */
			case DTK_YEAR:
				tm->tm_mon = 0;
				/* FALL THRU */
			case DTK_QUARTER:
				tm->tm_mon = 3 * (tm->tm_mon / 3);
				/* FALL THRU */
			case DTK_MONTH:
				tm->tm_mday = 0;
				/* FALL THRU */
			case DTK_DAY:
				tm->tm_hour = 0;
				/* FALL THRU */
			case DTK_HOUR:
				tm->tm_min = 0;
				/* FALL THRU */
			case DTK_MINUTE:
				tm->tm_sec = 0;
				/* FALL THRU */
			case DTK_SECOND:
				tm->tm_usec = 0;
				break;
			case DTK_MILLISEC:
				tm->tm_usec = (tm->tm_usec / 1000) * 1000;
				break;
			case DTK_MICROSEC:
				break;

			default:
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("unit \"%s\" not supported for type %s",
								lowunits, format_type_be(INTERVALOID)),
						 (val == DTK_WEEK) ? errdetail("Months usually have fractional weeks.") : 0));
		}

		if (itm2interval(tm, result) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("interval out of range")));
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unit \"%s\" not recognized for type %s",
						lowunits, format_type_be(INTERVALOID))));
	}

	PG_RETURN_INTERVAL_P(result);
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c NonFiniteTimestampTzPart ==== */

static float8
NonFiniteTimestampTzPart(int type, int unit, char *lowunits,
						 bool isNegative, bool isTz)
{
	if ((type != UNITS) && (type != RESERV))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unit \"%s\" not recognized for type %s",
						lowunits,
						format_type_be(isTz ? TIMESTAMPTZOID : TIMESTAMPOID))));

	switch (unit)
	{
			/* Oscillating units */
		case DTK_MICROSEC:
		case DTK_MILLISEC:
		case DTK_SECOND:
		case DTK_MINUTE:
		case DTK_HOUR:
		case DTK_DAY:
		case DTK_MONTH:
		case DTK_QUARTER:
		case DTK_WEEK:
		case DTK_DOW:
		case DTK_ISODOW:
		case DTK_DOY:
		case DTK_TZ:
		case DTK_TZ_MINUTE:
		case DTK_TZ_HOUR:
			return 0.0;

			/* Monotonically-increasing units */
		case DTK_YEAR:
		case DTK_DECADE:
		case DTK_CENTURY:
		case DTK_MILLENNIUM:
		case DTK_JULIAN:
		case DTK_ISOYEAR:
		case DTK_EPOCH:
			if (isNegative)
				return -get_float8_infinity();
			else
				return get_float8_infinity();

		default:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("unit \"%s\" not supported for type %s",
							lowunits,
							format_type_be(isTz ? TIMESTAMPTZOID : TIMESTAMPOID))));
			return 0.0;			/* keep compiler quiet */
	}
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c timestamp_part_common ==== */

static Datum
timestamp_part_common(PG_FUNCTION_ARGS, bool retnumeric)
{
	text	   *units = PG_GETARG_TEXT_PP(0);
	Timestamp	timestamp = PG_GETARG_TIMESTAMP(1);
	int64		intresult;
	Timestamp	epoch;
	int			type,
				val;
	char	   *lowunits;
	fsec_t		fsec;
	struct pg_tm tt,
			   *tm = &tt;

	lowunits = downcase_truncate_identifier(VARDATA_ANY(units),
											VARSIZE_ANY_EXHDR(units),
											false);

	type = DecodeUnits(0, lowunits, &val);
	if (type == UNKNOWN_FIELD)
		type = DecodeSpecial(0, lowunits, &val);

	if (TIMESTAMP_NOT_FINITE(timestamp))
	{
		double		r = NonFiniteTimestampTzPart(type, val, lowunits,
												 TIMESTAMP_IS_NOBEGIN(timestamp),
												 false);

		if (r != 0.0)
		{
			if (retnumeric)
			{
				if (r < 0)
					return DirectFunctionCall3(numeric_in,
											   CStringGetDatum("-Infinity"),
											   ObjectIdGetDatum(InvalidOid),
											   Int32GetDatum(-1));
				else if (r > 0)
					return DirectFunctionCall3(numeric_in,
											   CStringGetDatum("Infinity"),
											   ObjectIdGetDatum(InvalidOid),
											   Int32GetDatum(-1));
			}
			else
				PG_RETURN_FLOAT8(r);
		}
		else
			PG_RETURN_NULL();
	}

	if (type == UNITS)
	{
		if (timestamp2tm(timestamp, NULL, tm, &fsec, NULL, NULL) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("timestamp out of range")));

		switch (val)
		{
			case DTK_MICROSEC:
				intresult = tm->tm_sec * INT64CONST(1000000) + fsec;
				break;

			case DTK_MILLISEC:
				if (retnumeric)
					/*---
					 * tm->tm_sec * 1000 + fsec / 1000
					 * = (tm->tm_sec * 1'000'000 + fsec) / 1000
					 */
					PG_RETURN_NUMERIC(int64_div_fast_to_numeric(tm->tm_sec * INT64CONST(1000000) + fsec, 3));
				else
					PG_RETURN_FLOAT8(tm->tm_sec * 1000.0 + fsec / 1000.0);
				break;

			case DTK_SECOND:
				if (retnumeric)
					/*---
					 * tm->tm_sec + fsec / 1'000'000
					 * = (tm->tm_sec * 1'000'000 + fsec) / 1'000'000
					 */
					PG_RETURN_NUMERIC(int64_div_fast_to_numeric(tm->tm_sec * INT64CONST(1000000) + fsec, 6));
				else
					PG_RETURN_FLOAT8(tm->tm_sec + fsec / 1000000.0);
				break;

			case DTK_MINUTE:
				intresult = tm->tm_min;
				break;

			case DTK_HOUR:
				intresult = tm->tm_hour;
				break;

			case DTK_DAY:
				intresult = tm->tm_mday;
				break;

			case DTK_MONTH:
				intresult = tm->tm_mon;
				break;

			case DTK_QUARTER:
				intresult = (tm->tm_mon - 1) / 3 + 1;
				break;

			case DTK_WEEK:
				intresult = date2isoweek(tm->tm_year, tm->tm_mon, tm->tm_mday);
				break;

			case DTK_YEAR:
				if (tm->tm_year > 0)
					intresult = tm->tm_year;
				else
					/* there is no year 0, just 1 BC and 1 AD */
					intresult = tm->tm_year - 1;
				break;

			case DTK_DECADE:

				/*
				 * what is a decade wrt dates? let us assume that decade 199
				 * is 1990 thru 1999... decade 0 starts on year 1 BC, and -1
				 * is 11 BC thru 2 BC...
				 */
				if (tm->tm_year >= 0)
					intresult = tm->tm_year / 10;
				else
					intresult = -((8 - (tm->tm_year - 1)) / 10);
				break;

			case DTK_CENTURY:

				/* ----
				 * centuries AD, c>0: year in [ (c-1)* 100 + 1 : c*100 ]
				 * centuries BC, c<0: year in [ c*100 : (c+1) * 100 - 1]
				 * there is no number 0 century.
				 * ----
				 */
				if (tm->tm_year > 0)
					intresult = (tm->tm_year + 99) / 100;
				else
					/* caution: C division may have negative remainder */
					intresult = -((99 - (tm->tm_year - 1)) / 100);
				break;

			case DTK_MILLENNIUM:
				/* see comments above. */
				if (tm->tm_year > 0)
					intresult = (tm->tm_year + 999) / 1000;
				else
					intresult = -((999 - (tm->tm_year - 1)) / 1000);
				break;

			case DTK_JULIAN:
				if (retnumeric)
					PG_RETURN_NUMERIC(numeric_add_opt_error(int64_to_numeric(date2j(tm->tm_year, tm->tm_mon, tm->tm_mday)),
															numeric_div_opt_error(int64_to_numeric(((((tm->tm_hour * MINS_PER_HOUR) + tm->tm_min) * SECS_PER_MINUTE) + tm->tm_sec) * INT64CONST(1000000) + fsec),
																				  int64_to_numeric(SECS_PER_DAY * INT64CONST(1000000)),
																				  NULL),
															NULL));
				else
					PG_RETURN_FLOAT8(date2j(tm->tm_year, tm->tm_mon, tm->tm_mday) +
									 ((((tm->tm_hour * MINS_PER_HOUR) + tm->tm_min) * SECS_PER_MINUTE) +
									  tm->tm_sec + (fsec / 1000000.0)) / (double) SECS_PER_DAY);
				break;

			case DTK_ISOYEAR:
				intresult = date2isoyear(tm->tm_year, tm->tm_mon, tm->tm_mday);
				/* Adjust BC years */
				if (intresult <= 0)
					intresult -= 1;
				break;

			case DTK_DOW:
			case DTK_ISODOW:
				intresult = j2day(date2j(tm->tm_year, tm->tm_mon, tm->tm_mday));
				if (val == DTK_ISODOW && intresult == 0)
					intresult = 7;
				break;

			case DTK_DOY:
				intresult = (date2j(tm->tm_year, tm->tm_mon, tm->tm_mday)
							 - date2j(tm->tm_year, 1, 1) + 1);
				break;

			case DTK_TZ:
			case DTK_TZ_MINUTE:
			case DTK_TZ_HOUR:
			default:
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("unit \"%s\" not supported for type %s",
								lowunits, format_type_be(TIMESTAMPOID))));
				intresult = 0;
		}
	}
	else if (type == RESERV)
	{
		switch (val)
		{
			case DTK_EPOCH:
				epoch = SetEpochTimestamp();
				/* (timestamp - epoch) / 1000000 */
				if (retnumeric)
				{
					Numeric		result;

					if (timestamp < (PG_INT64_MAX + epoch))
						result = int64_div_fast_to_numeric(timestamp - epoch, 6);
					else
					{
						result = numeric_div_opt_error(numeric_sub_opt_error(int64_to_numeric(timestamp),
																			 int64_to_numeric(epoch),
																			 NULL),
													   int64_to_numeric(1000000),
													   NULL);
						result = DatumGetNumeric(DirectFunctionCall2(numeric_round,
																	 NumericGetDatum(result),
																	 Int32GetDatum(6)));
					}
					PG_RETURN_NUMERIC(result);
				}
				else
				{
					float8		result;

					/* try to avoid precision loss in subtraction */
					if (timestamp < (PG_INT64_MAX + epoch))
						result = (timestamp - epoch) / 1000000.0;
					else
						result = ((float8) timestamp - epoch) / 1000000.0;
					PG_RETURN_FLOAT8(result);
				}
				break;

			default:
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("unit \"%s\" not supported for type %s",
								lowunits, format_type_be(TIMESTAMPOID))));
				intresult = 0;
		}
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unit \"%s\" not recognized for type %s",
						lowunits, format_type_be(TIMESTAMPOID))));
		intresult = 0;
	}

	if (retnumeric)
		PG_RETURN_NUMERIC(int64_to_numeric(intresult));
	else
		PG_RETURN_FLOAT8(intresult);
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c timestamp_part ==== */

Datum
timestamp_part(PG_FUNCTION_ARGS)
{
	return timestamp_part_common(fcinfo, false);
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c extract_timestamp ==== */

Datum
extract_timestamp(PG_FUNCTION_ARGS)
{
	return timestamp_part_common(fcinfo, true);
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c timestamptz_part_common ==== */

static Datum
timestamptz_part_common(PG_FUNCTION_ARGS, bool retnumeric)
{
	text	   *units = PG_GETARG_TEXT_PP(0);
	TimestampTz timestamp = PG_GETARG_TIMESTAMPTZ(1);
	int64		intresult;
	Timestamp	epoch;
	int			tz;
	int			type,
				val;
	char	   *lowunits;
	fsec_t		fsec;
	struct pg_tm tt,
			   *tm = &tt;

	lowunits = downcase_truncate_identifier(VARDATA_ANY(units),
											VARSIZE_ANY_EXHDR(units),
											false);

	type = DecodeUnits(0, lowunits, &val);
	if (type == UNKNOWN_FIELD)
		type = DecodeSpecial(0, lowunits, &val);

	if (TIMESTAMP_NOT_FINITE(timestamp))
	{
		double		r = NonFiniteTimestampTzPart(type, val, lowunits,
												 TIMESTAMP_IS_NOBEGIN(timestamp),
												 true);

		if (r != 0.0)
		{
			if (retnumeric)
			{
				if (r < 0)
					return DirectFunctionCall3(numeric_in,
											   CStringGetDatum("-Infinity"),
											   ObjectIdGetDatum(InvalidOid),
											   Int32GetDatum(-1));
				else if (r > 0)
					return DirectFunctionCall3(numeric_in,
											   CStringGetDatum("Infinity"),
											   ObjectIdGetDatum(InvalidOid),
											   Int32GetDatum(-1));
			}
			else
				PG_RETURN_FLOAT8(r);
		}
		else
			PG_RETURN_NULL();
	}

	if (type == UNITS)
	{
		if (timestamp2tm(timestamp, &tz, tm, &fsec, NULL, NULL) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("timestamp out of range")));

		switch (val)
		{
			case DTK_TZ:
				intresult = -tz;
				break;

			case DTK_TZ_MINUTE:
				intresult = (-tz / SECS_PER_MINUTE) % MINS_PER_HOUR;
				break;

			case DTK_TZ_HOUR:
				intresult = -tz / SECS_PER_HOUR;
				break;

			case DTK_MICROSEC:
				intresult = tm->tm_sec * INT64CONST(1000000) + fsec;
				break;

			case DTK_MILLISEC:
				if (retnumeric)
					/*---
					 * tm->tm_sec * 1000 + fsec / 1000
					 * = (tm->tm_sec * 1'000'000 + fsec) / 1000
					 */
					PG_RETURN_NUMERIC(int64_div_fast_to_numeric(tm->tm_sec * INT64CONST(1000000) + fsec, 3));
				else
					PG_RETURN_FLOAT8(tm->tm_sec * 1000.0 + fsec / 1000.0);
				break;

			case DTK_SECOND:
				if (retnumeric)
					/*---
					 * tm->tm_sec + fsec / 1'000'000
					 * = (tm->tm_sec * 1'000'000 + fsec) / 1'000'000
					 */
					PG_RETURN_NUMERIC(int64_div_fast_to_numeric(tm->tm_sec * INT64CONST(1000000) + fsec, 6));
				else
					PG_RETURN_FLOAT8(tm->tm_sec + fsec / 1000000.0);
				break;

			case DTK_MINUTE:
				intresult = tm->tm_min;
				break;

			case DTK_HOUR:
				intresult = tm->tm_hour;
				break;

			case DTK_DAY:
				intresult = tm->tm_mday;
				break;

			case DTK_MONTH:
				intresult = tm->tm_mon;
				break;

			case DTK_QUARTER:
				intresult = (tm->tm_mon - 1) / 3 + 1;
				break;

			case DTK_WEEK:
				intresult = date2isoweek(tm->tm_year, tm->tm_mon, tm->tm_mday);
				break;

			case DTK_YEAR:
				if (tm->tm_year > 0)
					intresult = tm->tm_year;
				else
					/* there is no year 0, just 1 BC and 1 AD */
					intresult = tm->tm_year - 1;
				break;

			case DTK_DECADE:
				/* see comments in timestamp_part */
				if (tm->tm_year > 0)
					intresult = tm->tm_year / 10;
				else
					intresult = -((8 - (tm->tm_year - 1)) / 10);
				break;

			case DTK_CENTURY:
				/* see comments in timestamp_part */
				if (tm->tm_year > 0)
					intresult = (tm->tm_year + 99) / 100;
				else
					intresult = -((99 - (tm->tm_year - 1)) / 100);
				break;

			case DTK_MILLENNIUM:
				/* see comments in timestamp_part */
				if (tm->tm_year > 0)
					intresult = (tm->tm_year + 999) / 1000;
				else
					intresult = -((999 - (tm->tm_year - 1)) / 1000);
				break;

			case DTK_JULIAN:
				if (retnumeric)
					PG_RETURN_NUMERIC(numeric_add_opt_error(int64_to_numeric(date2j(tm->tm_year, tm->tm_mon, tm->tm_mday)),
															numeric_div_opt_error(int64_to_numeric(((((tm->tm_hour * MINS_PER_HOUR) + tm->tm_min) * SECS_PER_MINUTE) + tm->tm_sec) * INT64CONST(1000000) + fsec),
																				  int64_to_numeric(SECS_PER_DAY * INT64CONST(1000000)),
																				  NULL),
															NULL));
				else
					PG_RETURN_FLOAT8(date2j(tm->tm_year, tm->tm_mon, tm->tm_mday) +
									 ((((tm->tm_hour * MINS_PER_HOUR) + tm->tm_min) * SECS_PER_MINUTE) +
									  tm->tm_sec + (fsec / 1000000.0)) / (double) SECS_PER_DAY);
				break;

			case DTK_ISOYEAR:
				intresult = date2isoyear(tm->tm_year, tm->tm_mon, tm->tm_mday);
				/* Adjust BC years */
				if (intresult <= 0)
					intresult -= 1;
				break;

			case DTK_DOW:
			case DTK_ISODOW:
				intresult = j2day(date2j(tm->tm_year, tm->tm_mon, tm->tm_mday));
				if (val == DTK_ISODOW && intresult == 0)
					intresult = 7;
				break;

			case DTK_DOY:
				intresult = (date2j(tm->tm_year, tm->tm_mon, tm->tm_mday)
							 - date2j(tm->tm_year, 1, 1) + 1);
				break;

			default:
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("unit \"%s\" not supported for type %s",
								lowunits, format_type_be(TIMESTAMPTZOID))));
				intresult = 0;
		}
	}
	else if (type == RESERV)
	{
		switch (val)
		{
			case DTK_EPOCH:
				epoch = SetEpochTimestamp();
				/* (timestamp - epoch) / 1000000 */
				if (retnumeric)
				{
					Numeric		result;

					if (timestamp < (PG_INT64_MAX + epoch))
						result = int64_div_fast_to_numeric(timestamp - epoch, 6);
					else
					{
						result = numeric_div_opt_error(numeric_sub_opt_error(int64_to_numeric(timestamp),
																			 int64_to_numeric(epoch),
																			 NULL),
													   int64_to_numeric(1000000),
													   NULL);
						result = DatumGetNumeric(DirectFunctionCall2(numeric_round,
																	 NumericGetDatum(result),
																	 Int32GetDatum(6)));
					}
					PG_RETURN_NUMERIC(result);
				}
				else
				{
					float8		result;

					/* try to avoid precision loss in subtraction */
					if (timestamp < (PG_INT64_MAX + epoch))
						result = (timestamp - epoch) / 1000000.0;
					else
						result = ((float8) timestamp - epoch) / 1000000.0;
					PG_RETURN_FLOAT8(result);
				}
				break;

			default:
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("unit \"%s\" not supported for type %s",
								lowunits, format_type_be(TIMESTAMPTZOID))));
				intresult = 0;
		}
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unit \"%s\" not recognized for type %s",
						lowunits, format_type_be(TIMESTAMPTZOID))));

		intresult = 0;
	}

	if (retnumeric)
		PG_RETURN_NUMERIC(int64_to_numeric(intresult));
	else
		PG_RETURN_FLOAT8(intresult);
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c timestamptz_part ==== */

Datum
timestamptz_part(PG_FUNCTION_ARGS)
{
	return timestamptz_part_common(fcinfo, false);
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c extract_timestamptz ==== */

Datum
extract_timestamptz(PG_FUNCTION_ARGS)
{
	return timestamptz_part_common(fcinfo, true);
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c NonFiniteIntervalPart ==== */

static float8
NonFiniteIntervalPart(int type, int unit, char *lowunits, bool isNegative)
{
	if ((type != UNITS) && (type != RESERV))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unit \"%s\" not recognized for type %s",
						lowunits, format_type_be(INTERVALOID))));

	switch (unit)
	{
			/* Oscillating units */
		case DTK_MICROSEC:
		case DTK_MILLISEC:
		case DTK_SECOND:
		case DTK_MINUTE:
		case DTK_WEEK:
		case DTK_MONTH:
		case DTK_QUARTER:
			return 0.0;

			/* Monotonically-increasing units */
		case DTK_HOUR:
		case DTK_DAY:
		case DTK_YEAR:
		case DTK_DECADE:
		case DTK_CENTURY:
		case DTK_MILLENNIUM:
		case DTK_EPOCH:
			if (isNegative)
				return -get_float8_infinity();
			else
				return get_float8_infinity();

		default:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("unit \"%s\" not supported for type %s",
							lowunits, format_type_be(INTERVALOID))));
			return 0.0;			/* keep compiler quiet */
	}
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c interval_part_common ==== */

static Datum
interval_part_common(PG_FUNCTION_ARGS, bool retnumeric)
{
	text	   *units = PG_GETARG_TEXT_PP(0);
	Interval   *interval = PG_GETARG_INTERVAL_P(1);
	int64		intresult;
	int			type,
				val;
	char	   *lowunits;
	struct pg_itm tt,
			   *tm = &tt;

	lowunits = downcase_truncate_identifier(VARDATA_ANY(units),
											VARSIZE_ANY_EXHDR(units),
											false);

	type = DecodeUnits(0, lowunits, &val);
	if (type == UNKNOWN_FIELD)
		type = DecodeSpecial(0, lowunits, &val);

	if (INTERVAL_NOT_FINITE(interval))
	{
		double		r = NonFiniteIntervalPart(type, val, lowunits,
											  INTERVAL_IS_NOBEGIN(interval));

		if (r != 0.0)
		{
			if (retnumeric)
			{
				if (r < 0)
					return DirectFunctionCall3(numeric_in,
											   CStringGetDatum("-Infinity"),
											   ObjectIdGetDatum(InvalidOid),
											   Int32GetDatum(-1));
				else if (r > 0)
					return DirectFunctionCall3(numeric_in,
											   CStringGetDatum("Infinity"),
											   ObjectIdGetDatum(InvalidOid),
											   Int32GetDatum(-1));
			}
			else
				PG_RETURN_FLOAT8(r);
		}
		else
			PG_RETURN_NULL();
	}

	if (type == UNITS)
	{
		interval2itm(*interval, tm);
		switch (val)
		{
			case DTK_MICROSEC:
				intresult = tm->tm_sec * INT64CONST(1000000) + tm->tm_usec;
				break;

			case DTK_MILLISEC:
				if (retnumeric)
					/*---
					 * tm->tm_sec * 1000 + fsec / 1000
					 * = (tm->tm_sec * 1'000'000 + fsec) / 1000
					 */
					PG_RETURN_NUMERIC(int64_div_fast_to_numeric(tm->tm_sec * INT64CONST(1000000) + tm->tm_usec, 3));
				else
					PG_RETURN_FLOAT8(tm->tm_sec * 1000.0 + tm->tm_usec / 1000.0);
				break;

			case DTK_SECOND:
				if (retnumeric)
					/*---
					 * tm->tm_sec + fsec / 1'000'000
					 * = (tm->tm_sec * 1'000'000 + fsec) / 1'000'000
					 */
					PG_RETURN_NUMERIC(int64_div_fast_to_numeric(tm->tm_sec * INT64CONST(1000000) + tm->tm_usec, 6));
				else
					PG_RETURN_FLOAT8(tm->tm_sec + tm->tm_usec / 1000000.0);
				break;

			case DTK_MINUTE:
				intresult = tm->tm_min;
				break;

			case DTK_HOUR:
				intresult = tm->tm_hour;
				break;

			case DTK_DAY:
				intresult = tm->tm_mday;
				break;

			case DTK_WEEK:
				intresult = tm->tm_mday / 7;
				break;

			case DTK_MONTH:
				intresult = tm->tm_mon;
				break;

			case DTK_QUARTER:

				/*
				 * We want to maintain the rule that a field extracted from a
				 * negative interval is the negative of the field's value for
				 * the sign-reversed interval.  The broken-down tm_year and
				 * tm_mon aren't very helpful for that, so work from
				 * interval->month.
				 */
				if (interval->month >= 0)
					intresult = (tm->tm_mon / 3) + 1;
				else
					intresult = -(((-interval->month % MONTHS_PER_YEAR) / 3) + 1);
				break;

			case DTK_YEAR:
				intresult = tm->tm_year;
				break;

			case DTK_DECADE:
				/* caution: C division may have negative remainder */
				intresult = tm->tm_year / 10;
				break;

			case DTK_CENTURY:
				/* caution: C division may have negative remainder */
				intresult = tm->tm_year / 100;
				break;

			case DTK_MILLENNIUM:
				/* caution: C division may have negative remainder */
				intresult = tm->tm_year / 1000;
				break;

			default:
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("unit \"%s\" not supported for type %s",
								lowunits, format_type_be(INTERVALOID))));
				intresult = 0;
		}
	}
	else if (type == RESERV && val == DTK_EPOCH)
	{
		if (retnumeric)
		{
			Numeric		result;
			int64		secs_from_day_month;
			int64		val;

			/*
			 * To do this calculation in integer arithmetic even though
			 * DAYS_PER_YEAR is fractional, multiply everything by 4 and then
			 * divide by 4 again at the end.  This relies on DAYS_PER_YEAR
			 * being a multiple of 0.25 and on SECS_PER_DAY being a multiple
			 * of 4.
			 */
			secs_from_day_month = ((int64) (4 * DAYS_PER_YEAR) * (interval->month / MONTHS_PER_YEAR) +
								   (int64) (4 * DAYS_PER_MONTH) * (interval->month % MONTHS_PER_YEAR) +
								   (int64) 4 * interval->day) * (SECS_PER_DAY / 4);

			/*---
			 * result = secs_from_day_month + interval->time / 1'000'000
			 * = (secs_from_day_month * 1'000'000 + interval->time) / 1'000'000
			 */

			/*
			 * Try the computation inside int64; if it overflows, do it in
			 * numeric (slower).  This overflow happens around 10^9 days, so
			 * not common in practice.
			 */
			if (!pg_mul_s64_overflow(secs_from_day_month, 1000000, &val) &&
				!pg_add_s64_overflow(val, interval->time, &val))
				result = int64_div_fast_to_numeric(val, 6);
			else
				result =
					numeric_add_opt_error(int64_div_fast_to_numeric(interval->time, 6),
										  int64_to_numeric(secs_from_day_month),
										  NULL);

			PG_RETURN_NUMERIC(result);
		}
		else
		{
			float8		result;

			result = interval->time / 1000000.0;
			result += ((double) DAYS_PER_YEAR * SECS_PER_DAY) * (interval->month / MONTHS_PER_YEAR);
			result += ((double) DAYS_PER_MONTH * SECS_PER_DAY) * (interval->month % MONTHS_PER_YEAR);
			result += ((double) SECS_PER_DAY) * interval->day;

			PG_RETURN_FLOAT8(result);
		}
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unit \"%s\" not recognized for type %s",
						lowunits, format_type_be(INTERVALOID))));
		intresult = 0;
	}

	if (retnumeric)
		PG_RETURN_NUMERIC(int64_to_numeric(intresult));
	else
		PG_RETURN_FLOAT8(intresult);
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c interval_part ==== */

Datum
interval_part(PG_FUNCTION_ARGS)
{
	return interval_part_common(fcinfo, false);
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c extract_interval ==== */

Datum
extract_interval(PG_FUNCTION_ARGS)
{
	return interval_part_common(fcinfo, true);
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c IntervalAggState typedef ==== */

typedef struct IntervalAggState
{
	int64		N;				/* count of finite intervals processed */
	Interval	sumX;			/* sum of finite intervals processed */
	/* These counts are *not* included in N!  Use IA_TOTAL_COUNT() as needed */
	int64		pInfcount;		/* count of +infinity intervals */
	int64		nInfcount;		/* count of -infinity intervals */
} IntervalAggState;

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c makeIntervalAggState ==== */

static IntervalAggState *
makeIntervalAggState(FunctionCallInfo fcinfo)
{
	IntervalAggState *state;
	MemoryContext agg_context;
	MemoryContext old_context;

	if (!AggCheckCallContext(fcinfo, &agg_context))
		elog(ERROR, "aggregate function called in non-aggregate context");

	old_context = MemoryContextSwitchTo(agg_context);

	state = (IntervalAggState *) palloc0(sizeof(IntervalAggState));

	MemoryContextSwitchTo(old_context);

	return state;
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c do_interval_accum ==== */

static void
do_interval_accum(IntervalAggState *state, Interval *newval)
{
	/* Infinite inputs are counted separately, and do not affect "N" */
	if (INTERVAL_IS_NOBEGIN(newval))
	{
		state->nInfcount++;
		return;
	}

	if (INTERVAL_IS_NOEND(newval))
	{
		state->pInfcount++;
		return;
	}

	finite_interval_pl(&state->sumX, newval, &state->sumX);
	state->N++;
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c do_interval_discard ==== */

static void
do_interval_discard(IntervalAggState *state, Interval *newval)
{
	/* Infinite inputs are counted separately, and do not affect "N" */
	if (INTERVAL_IS_NOBEGIN(newval))
	{
		state->nInfcount--;
		return;
	}

	if (INTERVAL_IS_NOEND(newval))
	{
		state->pInfcount--;
		return;
	}

	/* Handle the to-be-discarded finite value. */
	state->N--;
	if (state->N > 0)
		finite_interval_mi(&state->sumX, newval, &state->sumX);
	else
	{
		/* All values discarded, reset the state */
		Assert(state->N == 0);
		memset(&state->sumX, 0, sizeof(state->sumX));
	}
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c interval_avg_combine ==== */

Datum
interval_avg_combine(PG_FUNCTION_ARGS)
{
	IntervalAggState *state1;
	IntervalAggState *state2;

	state1 = PG_ARGISNULL(0) ? NULL : (IntervalAggState *) PG_GETARG_POINTER(0);
	state2 = PG_ARGISNULL(1) ? NULL : (IntervalAggState *) PG_GETARG_POINTER(1);

	if (state2 == NULL)
		PG_RETURN_POINTER(state1);

	if (state1 == NULL)
	{
		/* manually copy all fields from state2 to state1 */
		state1 = makeIntervalAggState(fcinfo);

		state1->N = state2->N;
		state1->pInfcount = state2->pInfcount;
		state1->nInfcount = state2->nInfcount;

		state1->sumX.day = state2->sumX.day;
		state1->sumX.month = state2->sumX.month;
		state1->sumX.time = state2->sumX.time;

		PG_RETURN_POINTER(state1);
	}

	state1->N += state2->N;
	state1->pInfcount += state2->pInfcount;
	state1->nInfcount += state2->nInfcount;

	/* Accumulate finite interval values, if any. */
	if (state2->N > 0)
		finite_interval_pl(&state1->sumX, &state2->sumX, &state1->sumX);

	PG_RETURN_POINTER(state1);
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c interval_avg_serialize ==== */

Datum
interval_avg_serialize(PG_FUNCTION_ARGS)
{
	IntervalAggState *state;
	StringInfoData buf;
	bytea	   *result;

	/* Ensure we disallow calling when not in aggregate context */
	if (!AggCheckCallContext(fcinfo, NULL))
		elog(ERROR, "aggregate function called in non-aggregate context");

	state = (IntervalAggState *) PG_GETARG_POINTER(0);

	pq_begintypsend(&buf);

	/* N */
	pq_sendint64(&buf, state->N);

	/* sumX */
	pq_sendint64(&buf, state->sumX.time);
	pq_sendint32(&buf, state->sumX.day);
	pq_sendint32(&buf, state->sumX.month);

	/* pInfcount */
	pq_sendint64(&buf, state->pInfcount);

	/* nInfcount */
	pq_sendint64(&buf, state->nInfcount);

	result = pq_endtypsend(&buf);

	PG_RETURN_BYTEA_P(result);
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c interval_avg_deserialize ==== */

Datum
interval_avg_deserialize(PG_FUNCTION_ARGS)
{
	bytea	   *sstate;
	IntervalAggState *result;
	StringInfoData buf;

	if (!AggCheckCallContext(fcinfo, NULL))
		elog(ERROR, "aggregate function called in non-aggregate context");

	sstate = PG_GETARG_BYTEA_PP(0);

	/*
	 * Initialize a StringInfo so that we can "receive" it using the standard
	 * recv-function infrastructure.
	 */
	initReadOnlyStringInfo(&buf, VARDATA_ANY(sstate),
						   VARSIZE_ANY_EXHDR(sstate));

	result = (IntervalAggState *) palloc0(sizeof(IntervalAggState));

	/* N */
	result->N = pq_getmsgint64(&buf);

	/* sumX */
	result->sumX.time = pq_getmsgint64(&buf);
	result->sumX.day = pq_getmsgint(&buf, 4);
	result->sumX.month = pq_getmsgint(&buf, 4);

	/* pInfcount */
	result->pInfcount = pq_getmsgint64(&buf);

	/* nInfcount */
	result->nInfcount = pq_getmsgint64(&buf);

	pq_getmsgend(&buf);

	PG_RETURN_POINTER(result);
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c interval_avg ==== */

Datum
interval_avg(PG_FUNCTION_ARGS)
{
	IntervalAggState *state;

	state = PG_ARGISNULL(0) ? NULL : (IntervalAggState *) PG_GETARG_POINTER(0);

	/* If there were no non-null inputs, return NULL */
	if (state == NULL || IA_TOTAL_COUNT(state) == 0)
		PG_RETURN_NULL();

	/*
	 * Aggregating infinities that all have the same sign produces infinity
	 * with that sign.  Aggregating infinities with different signs results in
	 * an error.
	 */
	if (state->pInfcount > 0 || state->nInfcount > 0)
	{
		Interval   *result;

		if (state->pInfcount > 0 && state->nInfcount > 0)
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("interval out of range")));

		result = (Interval *) palloc(sizeof(Interval));
		if (state->pInfcount > 0)
			INTERVAL_NOEND(result);
		else
			INTERVAL_NOBEGIN(result);

		PG_RETURN_INTERVAL_P(result);
	}

	return DirectFunctionCall2(interval_div,
							   IntervalPGetDatum(&state->sumX),
							   Float8GetDatum((double) state->N));
}

/* ==== VERBATIM: src/backend/utils/adt/timestamp.c interval_sum ==== */

Datum
interval_sum(PG_FUNCTION_ARGS)
{
	IntervalAggState *state;
	Interval   *result;

	state = PG_ARGISNULL(0) ? NULL : (IntervalAggState *) PG_GETARG_POINTER(0);

	/* If there were no non-null inputs, return NULL */
	if (state == NULL || IA_TOTAL_COUNT(state) == 0)
		PG_RETURN_NULL();

	/*
	 * Aggregating infinities that all have the same sign produces infinity
	 * with that sign.  Aggregating infinities with different signs results in
	 * an error.
	 */
	if (state->pInfcount > 0 && state->nInfcount > 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("interval out of range")));

	result = (Interval *) palloc(sizeof(Interval));

	if (state->pInfcount > 0)
		INTERVAL_NOEND(result);
	else if (state->nInfcount > 0)
		INTERVAL_NOBEGIN(result);
	else
		memcpy(result, &state->sumX, sizeof(Interval));

	PG_RETURN_INTERVAL_P(result);
}
