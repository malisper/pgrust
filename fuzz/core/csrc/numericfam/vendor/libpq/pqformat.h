#ifndef NV_PQFORMAT_H
#define NV_PQFORMAT_H
typedef struct StringInfoData
{
	char	   *data;
	int			len;
	int			maxlen;
	int			cursor;
} StringInfoData;
typedef StringInfoData *StringInfo;

extern void pq_begintypsend(StringInfo buf);
extern bytea *pq_endtypsend(StringInfo buf);
extern void pq_sendint16(StringInfo buf, uint16 i);
extern void pq_sendint32(StringInfo buf, uint32 i);
extern void pq_sendint64(StringInfo buf, uint64 i);
extern unsigned int pq_getmsgint(StringInfo msg, int b);
extern int64 pq_getmsgint64(StringInfo msg);
extern void pq_getmsgend(StringInfo msg);
extern void initReadOnlyStringInfo(StringInfo str, char *data, int len);
#endif
