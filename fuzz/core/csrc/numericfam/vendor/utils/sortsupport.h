#ifndef NV_SORTSUPPORT_H
#define NV_SORTSUPPORT_H
typedef struct SortSupportData *SortSupport;
typedef struct SortSupportData
{
	MemoryContext ssup_cxt;
	Oid			ssup_collation;
	bool		ssup_reverse;
	bool		ssup_nulls_first;
	void	   *ssup_extra;
	int			(*comparator) (Datum x, Datum y, SortSupport ssup);
	bool		abbreviate;
	Datum		(*abbrev_converter) (Datum original, SortSupport ssup);
	bool		(*abbrev_abort) (int memtupcount, SortSupport ssup);
	int			(*abbrev_full_comparator) (Datum x, Datum y, SortSupport ssup);
} SortSupportData;

static inline int
ApplySortComparator(Datum d1, bool n1, Datum d2, bool n2, SortSupport ssup)
{
	return ssup->comparator(d1, d2, ssup);
}
#endif
