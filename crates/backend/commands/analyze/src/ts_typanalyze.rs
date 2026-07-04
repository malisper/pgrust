//! ts_typanalyze.c: compute_tsvector_stats — most-common-lexemes MCELEM via
//! Lossy Counting.

use datum::Datum;
use mcx::{Mcx, MemoryContext, PgFxHashMap, PgVec};
use types_error::PgResult;

use crate::{ComputeStats, FetchSource, VacAttrStats};

const TEXT_EQUAL_OPERATOR: types_core::Oid = 98;
const DEFAULT_COLLATION_OID: types_core::Oid = 100;
const TEXTOID: types_core::Oid = 25;

pub(crate) fn setup(stats: &mut VacAttrStats<'_>) -> PgResult<bool> {
    if stats.attstattarget < 0 {
        stats.attstattarget = guc_tables::vars::default_statistics_target.read();
    }
    stats.compute = ComputeStats::TsVector;
    stats.minrows = 300 * stats.attstattarget;
    Ok(true)
}

#[derive(Clone, Copy)]
struct TrackItem {
    frequency: i32,
    delta: i32,
}

pub(crate) fn compute_tsvector_stats<'mcx>(
    anl_mcx: Mcx<'mcx>,
    col_mcx: Mcx<'_>,
    stats: &mut VacAttrStats<'mcx>,
    src: &FetchSource<'_, '_>,
    samplerows: i32,
) -> PgResult<()> {
    let num_mcelem_target = stats.attstattarget * 10;
    let bucket_width = (num_mcelem_target + 10) * 1000 / 7;

    let mut lexemes_tab: PgFxHashMap<'_, &[u8], TrackItem> =
        PgFxHashMap::with_hasher_in(Default::default(), col_mcx);

    let mut b_current = 1i32;
    let mut lexeme_no = 0i64;
    let mut null_cnt = 0i32;
    let mut total_width = 0.0f64;

    let mut row_scratch = MemoryContext::new_bump("compute_tsvector_stats row scratch");
    for vector_no in 0..samplerows {
        let (value, isnull) = src.fetch(vector_no as usize, stats.tupattnum);
        if isnull {
            null_cnt += 1;
            continue;
        }
        {
            let row_mcx = row_scratch.mcx();
            let p = value.as_usize() as *const u8;
            // SAFETY: non-null varlena datum readable through its header.
            let raw =
                unsafe { core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) };
            // C uses the toasted width (VARSIZE_ANY of the stored datum).
            total_width += raw.len() as f64;

            let img: &[u8] = if raw[0] & 0x03 == 0 {
                raw
            } else {
                let v = detoast_seams::detoast_attr::call(row_mcx, raw)?;
                adt_multirangetypes::leak_image(v)
            };
            let vector = adt_tsvector_core::layout::TsVec { payload: &img[4..] };
            for j in 0..vector.size() {
                let lexeme = vector.lexeme(vector.entry(j));
                match lexemes_tab.get_mut(lexeme) {
                    Some(item) => item.frequency += 1,
                    None => {
                        let key: &[u8] = mcx::slice_borrow_in(col_mcx, lexeme)?;
                        lexemes_tab
                            .insert(key, TrackItem { frequency: 1, delta: b_current - 1 });
                    }
                }
                lexeme_no += 1;
                if lexeme_no % bucket_width as i64 == 0 {
                    // Pruned keys stay in the column bump until its reset
                    // (C pfrees per prune); the retained set is C's.
                    lexemes_tab.retain(|_, it| it.frequency + it.delta > b_current);
                    b_current += 1;
                }
            }
        }
        row_scratch.reset();
    }

    if null_cnt < samplerows {
        let nonnull_cnt = samplerows - null_cnt;
        stats.stats_valid = true;
        stats.stanullfrac = (null_cnt as f64 / samplerows as f64) as f32;
        stats.stawidth = (total_width / nonnull_cnt as f64) as i32;
        stats.stadistinct = (-1.0 * (1.0 - stats.stanullfrac as f64)) as f32;

        let cutoff_freq = 9 * lexeme_no / bucket_width as i64;

        let mut sort_table: PgVec<'_, (&[u8], i32)> =
            mcx::vec_with_capacity_in(col_mcx, lexemes_tab.len())?;
        let mut minfreq = lexeme_no;
        let mut maxfreq = 0i64;
        for (&lexeme, it) in lexemes_tab.iter() {
            if it.frequency as i64 > cutoff_freq {
                sort_table.push((lexeme, it.frequency));
                minfreq = minfreq.min(it.frequency as i64);
                maxfreq = maxfreq.max(it.frequency as i64);
            }
        }
        let track_len = sort_table.len() as i32;

        let mut num_mcelem = num_mcelem_target;
        if num_mcelem < track_len {
            // C's qsort tie order is hash-iteration-dependent; ties at the
            // truncation boundary can keep a different (equal-frequency) set.
            sort_table.sort_unstable_by(|a, b| b.1.cmp(&a.1));
            minfreq = sort_table[num_mcelem as usize - 1].1 as i64;
        } else {
            num_mcelem = track_len;
        }

        if num_mcelem > 0 {
            let prefix = &mut sort_table[..num_mcelem as usize];
            prefix.sort_unstable_by(|a, b| {
                a.0.len().cmp(&b.0.len()).then_with(|| a.0.cmp(b.0))
            });

            let mut mcelem_values: PgVec<'mcx, Datum> =
                mcx::vec_with_capacity_in(anl_mcx, num_mcelem as usize)?;
            let mut mcelem_freqs: PgVec<'mcx, f32> =
                mcx::vec_with_capacity_in(anl_mcx, num_mcelem as usize + 2)?;
            for &(lexeme, frequency) in prefix.iter() {
                mcelem_values.push(text_datum_in(anl_mcx, lexeme)?);
                mcelem_freqs.push((frequency as f64 / nonnull_cnt as f64) as f32);
            }
            mcelem_freqs.push((minfreq as f64 / nonnull_cnt as f64) as f32);
            mcelem_freqs.push((maxfreq as f64 / nonnull_cnt as f64) as f32);

            stats.stakind[0] = crate::array_typanalyze::STATISTIC_KIND_MCELEM;
            stats.staop[0] = TEXT_EQUAL_OPERATOR;
            stats.stacoll[0] = DEFAULT_COLLATION_OID;
            stats.stanumbers[0] = mcelem_freqs;
            stats.stavalues[0] = mcelem_values;
            stats.stavalues_set[0] = true;
            stats.statypid[0] = TEXTOID;
            stats.statyplen[0] = -1;
            stats.statypbyval[0] = false;
            stats.statypalign[0] = b'i';
        }
    } else {
        stats.stats_valid = true;
        stats.stanullfrac = 1.0;
        stats.stawidth = 0;
        stats.stadistinct = 0.0;
    }
    Ok(())
}

fn text_datum_in<'m>(mcx: Mcx<'m>, s: &[u8]) -> PgResult<Datum> {
    let mut img: PgVec<'m, u8> = mcx::vec_with_capacity_in(mcx, s.len() + datum::VARHDRSZ)?;
    mcx::vec_append_bytes(&mut img, &datum::set_varsize_4b(s.len() + datum::VARHDRSZ))?;
    mcx::vec_append_bytes(&mut img, s)?;
    Ok(Datum::from_usize(adt_multirangetypes::leak_image(img).as_ptr() as usize))
}
