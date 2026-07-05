use crate::{appendf, rec_data, rec_info, Rec, XLR_INFO_MASK};
use stringinfo::StringInfo;
use types_error::PgResult;
use types_hash::hashpage::{
    XLH_SPLIT_META_UPDATE_MASKS, XLH_SPLIT_META_UPDATE_SPLITPOINT, XLOG_HASH_ADD_OVFL_PAGE,
    XLOG_HASH_DELETE, XLOG_HASH_INIT_BITMAP_PAGE, XLOG_HASH_INIT_META_PAGE, XLOG_HASH_INSERT,
    XLOG_HASH_MOVE_PAGE_CONTENTS, XLOG_HASH_SPLIT_ALLOCATE_PAGE, XLOG_HASH_SPLIT_CLEANUP,
    XLOG_HASH_SPLIT_COMPLETE, XLOG_HASH_SPLIT_PAGE, XLOG_HASH_SQUEEZE_PAGE,
    XLOG_HASH_UPDATE_META_PAGE, XLOG_HASH_VACUUM_ONE_PAGE,
};
use xlogreader_seams::XLogReaderState;

fn trim_trailing_zeros(s: &str) -> &str {
    if !s.contains('.') {
        return s;
    }
    s.trim_end_matches('0').trim_end_matches('.')
}

// printf("%g", v) with the C default precision (6 significant digits): %e
// style when the exponent is < -4 or >= 6, else %f style; trailing zeros
// trimmed either way. hashm_ntuples is always a whole-number double, but the
// %e branch is real once a table exceeds ~1e6 tuples.
fn append_g6(buf: &mut StringInfo<'_>, v: f64) -> PgResult<()> {
    if v == 0.0 {
        return buf.append_str("0");
    }
    let neg = v.is_sign_negative();
    let av = v.abs();
    let sci = format!("{av:.5e}");
    let epos = sci.find('e').expect("Rust {:e} always emits 'e'");
    let exp: i32 = sci[epos + 1..].parse().expect("Rust {:e} exponent is a plain integer");

    if !(-4..6).contains(&exp) {
        let mantissa = trim_trailing_zeros(&sci[..epos]);
        appendf!(
            buf,
            "{}{mantissa}e{}{:02}",
            if neg { "-" } else { "" },
            if exp < 0 { '-' } else { '+' },
            exp.abs()
        )
    } else {
        let decimals = (5 - exp).max(0) as usize;
        let fixed = format!("{av:.decimals$}");
        let trimmed = trim_trailing_zeros(&fixed);
        appendf!(buf, "{}{trimmed}", if neg { "-" } else { "" })
    }
}

pub fn hash_desc(buf: &mut StringInfo<'_>, record: &XLogReaderState) -> PgResult<()> {
    let rec = Rec(rec_data(record));
    let info = rec_info(record) & !XLR_INFO_MASK;

    match info {
        XLOG_HASH_INIT_META_PAGE => {
            // xl_hash_init_meta_page: num_tuples 0 (f64), procid 8 (u32), ffactor 12.
            buf.append_str("num_tuples ")?;
            append_g6(buf, f64::from_ne_bytes(rec.arr::<8>(0, "xl_hash_init_meta_page")?))?;
            appendf!(buf, ", fillfactor {}", rec.u16(12, "xl_hash_init_meta_page")?)?;
        }
        XLOG_HASH_INIT_BITMAP_PAGE => {
            appendf!(buf, "bmsize {}", rec.u16(0, "xl_hash_init_bitmap_page")?)?;
        }
        XLOG_HASH_INSERT => {
            appendf!(buf, "off {}", rec.u16(0, "xl_hash_insert")?)?;
        }
        XLOG_HASH_ADD_OVFL_PAGE => {
            appendf!(
                buf,
                "bmsize {}, bmpage_found {}",
                rec.u16(0, "xl_hash_add_ovfl_page")?,
                if rec.u8(2, "xl_hash_add_ovfl_page")? != 0 { 'T' } else { 'F' }
            )?;
        }
        XLOG_HASH_SPLIT_ALLOCATE_PAGE => {
            let flags = rec.u8(8, "xl_hash_split_allocate_page")?;
            appendf!(
                buf,
                "new_bucket {}, meta_page_masks_updated {}, issplitpoint_changed {}",
                rec.u32(0, "xl_hash_split_allocate_page")?,
                if flags & XLH_SPLIT_META_UPDATE_MASKS != 0 { 'T' } else { 'F' },
                if flags & XLH_SPLIT_META_UPDATE_SPLITPOINT != 0 { 'T' } else { 'F' }
            )?;
        }
        XLOG_HASH_SPLIT_COMPLETE => {
            appendf!(
                buf,
                "old_bucket_flag {}, new_bucket_flag {}",
                rec.u16(0, "xl_hash_split_complete")?,
                rec.u16(2, "xl_hash_split_complete")?
            )?;
        }
        XLOG_HASH_MOVE_PAGE_CONTENTS => {
            appendf!(
                buf,
                "ntups {}, is_primary {}",
                rec.u16(0, "xl_hash_move_page_contents")?,
                if rec.u8(2, "xl_hash_move_page_contents")? != 0 { 'T' } else { 'F' }
            )?;
        }
        XLOG_HASH_SQUEEZE_PAGE => {
            appendf!(
                buf,
                "prevblkno {}, nextblkno {}, ntups {}, is_primary {}",
                rec.u32(0, "xl_hash_squeeze_page")?,
                rec.u32(4, "xl_hash_squeeze_page")?,
                rec.u16(8, "xl_hash_squeeze_page")?,
                if rec.u8(10, "xl_hash_squeeze_page")? != 0 { 'T' } else { 'F' }
            )?;
        }
        XLOG_HASH_DELETE => {
            appendf!(
                buf,
                "clear_dead_marking {}, is_primary {}",
                if rec.u8(0, "xl_hash_delete")? != 0 { 'T' } else { 'F' },
                if rec.u8(1, "xl_hash_delete")? != 0 { 'T' } else { 'F' }
            )?;
        }
        XLOG_HASH_UPDATE_META_PAGE => {
            buf.append_str("ntuples ")?;
            append_g6(buf, f64::from_ne_bytes(rec.arr::<8>(0, "xl_hash_update_meta_page")?))?;
        }
        XLOG_HASH_VACUUM_ONE_PAGE => {
            appendf!(
                buf,
                "ntuples {}, snapshotConflictHorizon {}, isCatalogRel {}",
                rec.u16(4, "xl_hash_vacuum_one_page")?,
                rec.u32(0, "xl_hash_vacuum_one_page")?,
                if rec.u8(6, "xl_hash_vacuum_one_page")? != 0 { 'T' } else { 'F' }
            )?;
        }
        _ => {}
    }
    Ok(())
}

pub fn hash_identify(info: u8) -> Option<&'static str> {
    match info & !XLR_INFO_MASK {
        XLOG_HASH_INIT_META_PAGE => Some("INIT_META_PAGE"),
        XLOG_HASH_INIT_BITMAP_PAGE => Some("INIT_BITMAP_PAGE"),
        XLOG_HASH_INSERT => Some("INSERT"),
        XLOG_HASH_ADD_OVFL_PAGE => Some("ADD_OVFL_PAGE"),
        XLOG_HASH_SPLIT_ALLOCATE_PAGE => Some("SPLIT_ALLOCATE_PAGE"),
        XLOG_HASH_SPLIT_PAGE => Some("SPLIT_PAGE"),
        XLOG_HASH_SPLIT_COMPLETE => Some("SPLIT_COMPLETE"),
        XLOG_HASH_MOVE_PAGE_CONTENTS => Some("MOVE_PAGE_CONTENTS"),
        XLOG_HASH_SQUEEZE_PAGE => Some("SQUEEZE_PAGE"),
        XLOG_HASH_DELETE => Some("DELETE"),
        XLOG_HASH_SPLIT_CLEANUP => Some("SPLIT_CLEANUP"),
        XLOG_HASH_UPDATE_META_PAGE => Some("UPDATE_META_PAGE"),
        XLOG_HASH_VACUUM_ONE_PAGE => Some("VACUUM_ONE_PAGE"),
        _ => None,
    }
}
