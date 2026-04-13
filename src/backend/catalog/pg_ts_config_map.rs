use crate::include::catalog::PgTsConfigMapRow;

pub fn sort_pg_ts_config_map_rows(rows: &mut [PgTsConfigMapRow]) {
    rows.sort_by(|left, right| {
        left.mapcfg
            .cmp(&right.mapcfg)
            .then_with(|| left.maptokentype.cmp(&right.maptokentype))
            .then_with(|| left.mapseqno.cmp(&right.mapseqno))
            .then_with(|| left.mapdict.cmp(&right.mapdict))
    });
}
