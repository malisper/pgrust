use pgrust_catalog_data::PgEventTriggerRow;

pub fn sort_pg_event_trigger_rows(rows: &mut [PgEventTriggerRow]) {
    rows.sort_by_key(|row| (row.evtname.clone(), row.oid))
}
