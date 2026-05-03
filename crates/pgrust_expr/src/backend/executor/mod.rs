pub mod exec_expr;
pub mod expr_bit;
pub mod expr_bool;
pub mod expr_casts;
pub mod expr_date;
pub mod expr_datetime;
pub mod expr_format;
pub mod expr_geometry;
pub mod expr_json;
pub mod expr_mac;
pub mod expr_math;
pub mod expr_money;
pub mod expr_multirange;
pub mod expr_network;
pub mod expr_numeric;
pub mod expr_ops;
pub mod expr_range;
pub mod expr_reg;
pub mod expr_string;
pub mod expr_txid;
pub mod expr_xml;
pub mod jsonb;
pub mod jsonpath;
pub mod node_types;
pub mod pg_regex;
pub mod tsearch;
pub mod value_io;

pub use crate::error::{ExprError as ExecError, RegexError};
pub use expr_bit::render_bit_text;
pub use expr_casts::{
    cast_value, cast_value_with_config, cast_value_with_source_type_catalog_and_config,
    parse_bytea_text, parse_interval_text_value, parse_text_array_literal_with_catalog_and_op,
    parse_text_array_literal_with_catalog_op_and_explicit, parse_text_array_literal_with_op,
    render_internal_char_text, render_interval_text, render_interval_text_with_config,
    render_pg_lsn_text,
};
pub use expr_datetime::{
    current_timestamp_value, render_datetime_value_text, render_datetime_value_text_with_config,
};
pub use expr_geometry::{
    eval_geometry_function, geometry_input_error_message, render_geometry_text,
};
pub use expr_mac::{
    eval_macaddr_function, macaddr_to_macaddr8, macaddr8_to_macaddr, parse_macaddr_bytes,
    parse_macaddr_text, parse_macaddr8_bytes, parse_macaddr8_text, render_macaddr_text,
    render_macaddr8_text,
};
pub use expr_money::{money_format_text, money_parse_text};
pub use expr_multirange::{
    compare_multirange_values, decode_multirange_bytes, encode_multirange_bytes,
    eval_multirange_function, multirange_intersection_agg_transition, parse_multirange_text,
    range_agg_transition, render_multirange_text, render_multirange_text_with_config,
};
pub use expr_network::{
    compare_network_values, encode_network_bytes, eval_network_function, network_btree_upper_bound,
    network_contains, network_merge, network_prefix, parse_cidr_bytes, parse_cidr_text,
    parse_inet_bytes, parse_inet_text, render_network_text,
};
pub use expr_numeric::eval_power_function;
pub use expr_ops::*;
pub use expr_range::{
    compare_range_values, decode_range_bytes, encode_range_bytes, eval_range_function,
    parse_range_text, render_range_text, render_range_text_with_config,
};
pub use expr_string::eval_to_char_function;
pub use expr_txid::{
    cast_text_to_txid_snapshot, eval_txid_snapshot_xip_values, is_txid_snapshot_type_oid,
};
pub use expr_xml::{render_xml_output_text, strip_xml_declaration, validate_xml_input};
pub use jsonb::*;
pub use pg_regex::*;
pub use pgrust_nodes::datum::Value;
pub use tsearch::*;
pub use value_io::{
    format_array_value_text, format_record_text, format_record_text_with_config,
    indirect_varlena_to_value, render_tid_text, render_uuid_text,
};

#[derive(Debug, Clone, PartialEq)]
pub struct TupleSlot {
    pub values: Vec<Value>,
}

impl TupleSlot {
    pub fn virtual_row(values: Vec<Value>) -> Self {
        Self { values }
    }
}

#[derive(Debug, Clone)]
pub struct ExecutorContext {
    pub datetime_config: crate::expr_backend::utils::misc::guc_datetime::DateTimeConfig,
}

impl Default for ExecutorContext {
    fn default() -> Self {
        Self {
            datetime_config: Default::default(),
        }
    }
}

impl ExecutorContext {
    pub fn bytea_output(&self) -> crate::compat::pgrust::session::ByteaOutputFormat {
        crate::compat::pgrust::session::ByteaOutputFormat::Hex
    }

    pub fn catalog(&self) -> Option<&dyn crate::services::ExprCatalogLookup> {
        None
    }
}
