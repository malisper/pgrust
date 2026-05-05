use crate::desc::column_desc;
use crate::*;
#[cfg(test)]
use pgrust_catalog_ids::{AggFunc, BuiltinWindowFunction, HypotheticalAggFunc};
use pgrust_catalog_ids::{BuiltinScalarFunction, HashFunctionKind};
pub use pgrust_catalog_ids::{
    builtin_aggregate_function_for_proc_oid, builtin_hypothetical_aggregate_function_for_proc_oid,
    builtin_ordered_set_aggregate_function_for_proc_oid, builtin_scalar_function_for_proc_oid,
    builtin_window_function_for_proc_oid, proc_oid_for_builtin_aggregate_function,
    proc_oid_for_builtin_hypothetical_aggregate_function,
    proc_oid_for_builtin_ordered_set_aggregate_function, proc_oid_for_builtin_scalar_function,
    proc_oid_for_builtin_window_function,
};
use pgrust_nodes::parsenodes::{SqlType, SqlTypeKind};
use pgrust_nodes::primnodes::RelationDesc;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::{OnceLock, RwLock};

#[cfg(test)]
const VOID_TYPE_OID: u32 = 2278;
pub const ORDERED_SET_TRANSITION_PROC_OID: u32 = 3970;
pub const PERCENTILE_DISC_AGG_PROC_OID: u32 = 3972;
pub const PERCENTILE_DISC_FINAL_PROC_OID: u32 = 3973;
pub const PERCENTILE_CONT_FLOAT8_AGG_PROC_OID: u32 = 3974;
pub const PERCENTILE_CONT_FLOAT8_FINAL_PROC_OID: u32 = 3975;
pub const PERCENTILE_CONT_INTERVAL_AGG_PROC_OID: u32 = 3976;
pub const PERCENTILE_CONT_INTERVAL_FINAL_PROC_OID: u32 = 3977;
pub const PERCENTILE_DISC_MULTI_AGG_PROC_OID: u32 = 3978;
pub const PERCENTILE_DISC_MULTI_FINAL_PROC_OID: u32 = 3979;
pub const PERCENTILE_CONT_FLOAT8_MULTI_AGG_PROC_OID: u32 = 3980;
pub const PERCENTILE_CONT_FLOAT8_MULTI_FINAL_PROC_OID: u32 = 3981;
pub const PERCENTILE_CONT_INTERVAL_MULTI_AGG_PROC_OID: u32 = 3982;
pub const PERCENTILE_CONT_INTERVAL_MULTI_FINAL_PROC_OID: u32 = 3983;
pub const MODE_AGG_PROC_OID: u32 = 3984;
pub const MODE_FINAL_PROC_OID: u32 = 3985;
pub const HYPOTHETICAL_RANK_FINAL_PROC_OID: u32 = 3987;

pub const CAST_PROC_INT4_INT2_OID: u32 = 313;
pub const CAST_PROC_INT8_INT2_OID: u32 = 754;
pub const CAST_PROC_NUMERIC_INT2_OID: u32 = 1782;
pub const CAST_PROC_INT2_INT4_OID: u32 = 6231;
pub const CAST_PROC_INT8_INT4_OID: u32 = 481;
pub const CAST_PROC_NUMERIC_INT4_OID: u32 = 1740;
pub const CAST_PROC_INT2_INT8_OID: u32 = 6234;
pub const CAST_PROC_INT4_INT8_OID: u32 = 6235;
pub const CAST_PROC_NUMERIC_INT8_OID: u32 = 1781;
pub const CAST_PROC_TEXT_BPCHAR_OID: u32 = 6237;
pub const BOOL_CMP_LT_PROC_OID: u32 = 56;
pub const BOOL_CMP_GT_PROC_OID: u32 = 57;
pub const BOOL_CMP_EQ_PROC_OID: u32 = 60;
pub const INT4_CMP_EQ_PROC_OID: u32 = 65;
pub const FLOAT8_CBRT_PROC_OID: u32 = 231;
pub const INT4_CMP_LT_PROC_OID: u32 = 66;
pub const TEXT_CMP_EQ_PROC_OID: u32 = 67;
pub const BOOL_CMP_NE_PROC_OID: u32 = 84;
pub const INT4_CMP_NE_PROC_OID: u32 = 144;
pub const INT4_CMP_GT_PROC_OID: u32 = 147;
pub const INT4_CMP_LE_PROC_OID: u32 = 149;
pub const INT4_CMP_GE_PROC_OID: u32 = 150;
pub const TEXT_CMP_NE_PROC_OID: u32 = 157;
pub const TEXT_CMP_LT_PROC_OID: u32 = 740;
pub const TEXT_CMP_LE_PROC_OID: u32 = 741;
pub const TEXT_CMP_GT_PROC_OID: u32 = 742;
pub const TEXT_CMP_GE_PROC_OID: u32 = 743;
pub const TID_CMP_NE_PROC_OID: u32 = 1265;
pub const TID_CMP_EQ_PROC_OID: u32 = 1292;
pub const CURRTID2_PROC_OID: u32 = 1294;
pub const BOOL_CMP_LE_PROC_OID: u32 = 1691;
pub const BOOL_CMP_GE_PROC_OID: u32 = 1692;
pub const RI_FKEY_CHECK_INS_PROC_OID: u32 = 1644;
pub const RI_FKEY_CHECK_UPD_PROC_OID: u32 = 1645;
pub const RI_FKEY_CASCADE_DEL_PROC_OID: u32 = 1646;
pub const RI_FKEY_CASCADE_UPD_PROC_OID: u32 = 1647;
pub const RI_FKEY_RESTRICT_DEL_PROC_OID: u32 = 1648;
pub const RI_FKEY_RESTRICT_UPD_PROC_OID: u32 = 1649;
pub const RI_FKEY_SETNULL_DEL_PROC_OID: u32 = 1650;
pub const RI_FKEY_SETNULL_UPD_PROC_OID: u32 = 1651;
pub const RI_FKEY_SETDEFAULT_DEL_PROC_OID: u32 = 1652;
pub const RI_FKEY_SETDEFAULT_UPD_PROC_OID: u32 = 1653;
pub const RI_FKEY_NOACTION_DEL_PROC_OID: u32 = 1654;
pub const RI_FKEY_NOACTION_UPD_PROC_OID: u32 = 1655;
pub const TID_CMP_GT_PROC_OID: u32 = 2790;
pub const TID_CMP_LT_PROC_OID: u32 = 2791;
pub const TID_CMP_GE_PROC_OID: u32 = 2792;
pub const TID_CMP_LE_PROC_OID: u32 = 2793;
pub const TEXT_STARTS_WITH_PROC_OID: u32 = 3696;
pub const BIT_CMP_EQ_PROC_OID: u32 = 1581;
pub const BIT_CMP_NE_PROC_OID: u32 = 1582;
pub const BIT_CMP_GE_PROC_OID: u32 = 1592;
pub const BIT_CMP_GT_PROC_OID: u32 = 1593;
pub const BIT_CMP_LE_PROC_OID: u32 = 1594;
pub const BIT_CMP_LT_PROC_OID: u32 = 1595;
pub const VARBIT_CMP_EQ_PROC_OID: u32 = 1666;
pub const VARBIT_CMP_NE_PROC_OID: u32 = 1667;
pub const VARBIT_CMP_GE_PROC_OID: u32 = 1668;
pub const VARBIT_CMP_GT_PROC_OID: u32 = 1669;
pub const VARBIT_CMP_LE_PROC_OID: u32 = 1670;
pub const VARBIT_CMP_LT_PROC_OID: u32 = 1671;
pub const BYTEA_CMP_EQ_PROC_OID: u32 = 1948;
pub const BYTEA_CMP_LT_PROC_OID: u32 = 1949;
pub const BYTEA_CMP_LE_PROC_OID: u32 = 1950;
pub const BYTEA_CMP_GT_PROC_OID: u32 = 1951;
pub const BYTEA_CMP_GE_PROC_OID: u32 = 1952;
pub const BYTEA_CMP_NE_PROC_OID: u32 = 1953;
pub const JSONB_CMP_NE_PROC_OID: u32 = 4038;
pub const JSONB_CMP_LT_PROC_OID: u32 = 4039;
pub const JSONB_CMP_GT_PROC_OID: u32 = 4040;
pub const JSONB_CMP_LE_PROC_OID: u32 = 4041;
pub const JSONB_CMP_GE_PROC_OID: u32 = 4042;
pub const JSONB_CMP_EQ_PROC_OID: u32 = 4043;
pub const INTERVAL_CMP_EQ_PROC_OID: u32 = 1162;
pub const INTERVAL_CMP_NE_PROC_OID: u32 = 1163;
pub const INTERVAL_CMP_LT_PROC_OID: u32 = 1164;
pub const INTERVAL_CMP_LE_PROC_OID: u32 = 1165;
pub const INTERVAL_CMP_GE_PROC_OID: u32 = 1166;
pub const INTERVAL_CMP_GT_PROC_OID: u32 = 1167;
pub const JSONB_CONTAINS_PROC_OID: u32 = 4046;
pub const JSONB_EXISTS_PROC_OID: u32 = 4047;
pub const JSONB_EXISTS_ANY_PROC_OID: u32 = 4048;
pub const JSONB_EXISTS_ALL_PROC_OID: u32 = 4049;
pub const JSONB_CONTAINED_PROC_OID: u32 = 4050;
pub const JSONB_PATH_EXISTS_PROC_OID: u32 = 4010;
pub const JSONB_PATH_MATCH_PROC_OID: u32 = 4011;
pub const JSONB_CONCAT_PROC_OID: u32 = 3301;
pub const GIN_COMPARE_JSONB_PROC_OID: u32 = 3480;
pub const GIN_EXTRACT_JSONB_PROC_OID: u32 = 3482;
pub const GIN_EXTRACT_JSONB_QUERY_PROC_OID: u32 = 3483;
pub const GIN_CONSISTENT_JSONB_PROC_OID: u32 = 3484;
pub const GIN_TRICONSISTENT_JSONB_PROC_OID: u32 = 3488;
pub const GIST_BOX_CONSISTENT_PROC_OID: u32 = 2578;
pub const GIST_BOX_PENALTY_PROC_OID: u32 = 2581;
pub const GIST_BOX_PICKSPLIT_PROC_OID: u32 = 2582;
pub const GIST_BOX_UNION_PROC_OID: u32 = 2583;
pub const GIST_BOX_SAME_PROC_OID: u32 = 2584;
pub const GIST_POINT_CONSISTENT_PROC_OID: u32 = 76030;
pub const GIST_POINT_UNION_PROC_OID: u32 = 76031;
pub const GIST_POINT_PENALTY_PROC_OID: u32 = 76032;
pub const GIST_POINT_PICKSPLIT_PROC_OID: u32 = 76033;
pub const GIST_POINT_SAME_PROC_OID: u32 = 76034;
pub const GIST_POINT_SORTSUPPORT_PROC_OID: u32 = 76035;
pub const GIST_POINT_DISTANCE_PROC_OID: u32 = 76036;
pub const GIST_POLY_CONSISTENT_PROC_OID: u32 = 76630;
pub const GIST_POLY_UNION_PROC_OID: u32 = 76631;
pub const GIST_POLY_PENALTY_PROC_OID: u32 = 76632;
pub const GIST_POLY_PICKSPLIT_PROC_OID: u32 = 76633;
pub const GIST_POLY_SAME_PROC_OID: u32 = 76634;
pub const GIST_POLY_DISTANCE_PROC_OID: u32 = 76635;
pub const GIST_CIRCLE_CONSISTENT_PROC_OID: u32 = 76636;
pub const GIST_CIRCLE_UNION_PROC_OID: u32 = 76637;
pub const GIST_CIRCLE_PENALTY_PROC_OID: u32 = 76638;
pub const GIST_CIRCLE_PICKSPLIT_PROC_OID: u32 = 76639;
pub const GIST_CIRCLE_SAME_PROC_OID: u32 = 76640;
pub const GIST_CIRCLE_DISTANCE_PROC_OID: u32 = 76641;
pub const RANGE_GIST_CONSISTENT_PROC_OID: u32 = 3875;
pub const RANGE_GIST_UNION_PROC_OID: u32 = 3876;
pub const RANGE_GIST_PENALTY_PROC_OID: u32 = 3879;
pub const RANGE_GIST_PICKSPLIT_PROC_OID: u32 = 3880;
pub const RANGE_GIST_SAME_PROC_OID: u32 = 3881;
pub const MULTIRANGE_GIST_CONSISTENT_PROC_OID: u32 = 6154;
pub const GIST_BOX_DISTANCE_PROC_OID: u32 = 3998;
pub const GIST_NETWORK_CONSISTENT_PROC_OID: u32 = 76610;
pub const GIST_NETWORK_UNION_PROC_OID: u32 = 76611;
pub const GIST_NETWORK_PENALTY_PROC_OID: u32 = 76612;
pub const GIST_NETWORK_PICKSPLIT_PROC_OID: u32 = 76613;
pub const GIST_NETWORK_SAME_PROC_OID: u32 = 76614;
pub const GIST_TSVECTOR_CONSISTENT_PROC_OID: u32 = 76730;
pub const GIST_TSVECTOR_UNION_PROC_OID: u32 = 76731;
pub const GIST_TSVECTOR_PENALTY_PROC_OID: u32 = 76732;
pub const GIST_TSVECTOR_PICKSPLIT_PROC_OID: u32 = 76733;
pub const GIST_TSVECTOR_SAME_PROC_OID: u32 = 76734;
pub const GIST_TSQUERY_CONSISTENT_PROC_OID: u32 = 76735;
pub const GIST_TSQUERY_UNION_PROC_OID: u32 = 76736;
pub const GIST_TSQUERY_PENALTY_PROC_OID: u32 = 76737;
pub const GIST_TSQUERY_PICKSPLIT_PROC_OID: u32 = 76738;
pub const GIST_TSQUERY_SAME_PROC_OID: u32 = 76739;
pub const SPG_QUAD_CONFIG_PROC_OID: u32 = 4018;
pub const SPG_QUAD_CHOOSE_PROC_OID: u32 = 4019;
pub const SPG_QUAD_PICKSPLIT_PROC_OID: u32 = 4020;
pub const SPG_QUAD_INNER_CONSISTENT_PROC_OID: u32 = 4021;
pub const SPG_QUAD_LEAF_CONSISTENT_PROC_OID: u32 = 4022;
pub const SPG_KD_CONFIG_PROC_OID: u32 = 4023;
pub const SPG_KD_CHOOSE_PROC_OID: u32 = 4024;
pub const SPG_KD_PICKSPLIT_PROC_OID: u32 = 4025;
pub const SPG_KD_INNER_CONSISTENT_PROC_OID: u32 = 4026;
pub const SPG_TEXT_CONFIG_PROC_OID: u32 = 4027;
pub const SPG_TEXT_CHOOSE_PROC_OID: u32 = 4028;
pub const SPG_TEXT_PICKSPLIT_PROC_OID: u32 = 4029;
pub const SPG_TEXT_INNER_CONSISTENT_PROC_OID: u32 = 4030;
pub const SPG_TEXT_LEAF_CONSISTENT_PROC_OID: u32 = 4031;
pub const SPG_BOX_QUAD_CONFIG_PROC_OID: u32 = 5012;
pub const SPG_BOX_QUAD_CHOOSE_PROC_OID: u32 = 5013;
pub const SPG_BOX_QUAD_PICKSPLIT_PROC_OID: u32 = 5014;
pub const SPG_BOX_QUAD_INNER_CONSISTENT_PROC_OID: u32 = 5015;
pub const SPG_BOX_QUAD_LEAF_CONSISTENT_PROC_OID: u32 = 5016;
pub const SPG_NETWORK_CONFIG_PROC_OID: u32 = 76615;
pub const SPG_NETWORK_CHOOSE_PROC_OID: u32 = 76616;
pub const SPG_NETWORK_PICKSPLIT_PROC_OID: u32 = 76617;
pub const SPG_NETWORK_INNER_CONSISTENT_PROC_OID: u32 = 76618;
pub const SPG_NETWORK_LEAF_CONSISTENT_PROC_OID: u32 = 76619;
pub const SPG_RANGE_CONFIG_PROC_OID: u32 = 5022;
pub const SPG_RANGE_CHOOSE_PROC_OID: u32 = 5023;
pub const SPG_RANGE_PICKSPLIT_PROC_OID: u32 = 5024;
pub const SPG_RANGE_INNER_CONSISTENT_PROC_OID: u32 = 5025;
pub const SPG_RANGE_LEAF_CONSISTENT_PROC_OID: u32 = 5026;
pub const GIST_TRANSLATE_CMPTYPE_COMMON_PROC_OID: u32 = 6347;
pub const RANGE_SORTSUPPORT_PROC_OID: u32 = 6391;
pub const BRIN_MINMAX_OPCINFO_PROC_OID: u32 = 3383;
pub const BRIN_MINMAX_ADD_VALUE_PROC_OID: u32 = 3384;
pub const BRIN_MINMAX_CONSISTENT_PROC_OID: u32 = 3385;
pub const BRIN_MINMAX_UNION_PROC_OID: u32 = 3386;
pub const HASH_BOOL_PROC_OID: u32 = 6417;
pub const HASH_BOOL_EXTENDED_PROC_OID: u32 = 6418;
pub const HASH_INT2_PROC_OID: u32 = 449;
pub const HASH_INT2_EXTENDED_PROC_OID: u32 = 441;
pub const HASH_INT4_PROC_OID: u32 = 450;
pub const HASH_INT4_EXTENDED_PROC_OID: u32 = 425;
pub const HASH_INT8_PROC_OID: u32 = 949;
pub const HASH_INT8_EXTENDED_PROC_OID: u32 = 442;
pub const HASH_OID_PROC_OID: u32 = 453;
pub const HASH_OID_EXTENDED_PROC_OID: u32 = 445;
pub const HASH_CHAR_PROC_OID: u32 = 454;
pub const HASH_CHAR_EXTENDED_PROC_OID: u32 = 446;
pub const HASH_NAME_PROC_OID: u32 = 455;
pub const HASH_NAME_EXTENDED_PROC_OID: u32 = 447;
pub const HASH_TEXT_PROC_OID: u32 = 400;
pub const HASH_TEXT_EXTENDED_PROC_OID: u32 = 448;
pub const HASH_VARCHAR_PROC_OID: u32 = 76508;
pub const HASH_BPCHAR_PROC_OID: u32 = 1080;
pub const HASH_BPCHAR_EXTENDED_PROC_OID: u32 = 972;
pub const HASH_FLOAT4_PROC_OID: u32 = 451;
pub const HASH_FLOAT4_EXTENDED_PROC_OID: u32 = 443;
pub const HASH_FLOAT8_PROC_OID: u32 = 452;
pub const HASH_FLOAT8_EXTENDED_PROC_OID: u32 = 444;
pub const HASH_NUMERIC_PROC_OID: u32 = 432;
pub const HASH_NUMERIC_EXTENDED_PROC_OID: u32 = 780;
pub const HASH_TIMESTAMP_PROC_OID: u32 = 2039;
pub const HASH_TIMESTAMP_EXTENDED_PROC_OID: u32 = 3411;
pub const HASH_TIMESTAMPTZ_PROC_OID: u32 = 6425;
pub const HASH_TIMESTAMPTZ_EXTENDED_PROC_OID: u32 = 6426;
pub const HASH_DATE_PROC_OID: u32 = 6415;
pub const HASH_DATE_EXTENDED_PROC_OID: u32 = 6416;
pub const HASH_TIME_PROC_OID: u32 = 1688;
pub const HASH_TIME_EXTENDED_PROC_OID: u32 = 3409;
pub const HASH_TIMETZ_PROC_OID: u32 = 1696;
pub const HASH_TIMETZ_EXTENDED_PROC_OID: u32 = 3410;
pub const HASH_BYTEA_PROC_OID: u32 = 6413;
pub const HASH_BYTEA_EXTENDED_PROC_OID: u32 = 6414;
pub const HASH_OIDVECTOR_PROC_OID: u32 = 457;
pub const HASH_OIDVECTOR_EXTENDED_PROC_OID: u32 = 776;
pub const HASH_ACLITEM_PROC_OID: u32 = 329;
pub const HASH_ACLITEM_EXTENDED_PROC_OID: u32 = 777;
pub const HASH_INET_PROC_OID: u32 = 422;
pub const HASH_INET_EXTENDED_PROC_OID: u32 = 779;
pub const HASH_ARRAY_PROC_OID: u32 = 626;
pub const HASH_ARRAY_EXTENDED_PROC_OID: u32 = 782;
pub const HASH_MULTIRANGE_PROC_OID: u32 = 4278;
pub const HASH_MULTIRANGE_EXTENDED_PROC_OID: u32 = 4279;
pub const HASH_UUID_PROC_OID: u32 = 2963;
pub const HASH_UUID_EXTENDED_PROC_OID: u32 = 3412;
pub const HASH_RANGE_PROC_OID: u32 = 3902;
pub const HASH_RANGE_EXTENDED_PROC_OID: u32 = 3417;
pub const HASH_INTERVAL_PROC_OID: u32 = 1697;
pub const HASH_INTERVAL_EXTENDED_PROC_OID: u32 = 3418;
pub const HASH_PG_LSN_PROC_OID: u32 = 3252;
pub const HASH_PG_LSN_EXTENDED_PROC_OID: u32 = 3413;
pub const ENUM_EQ_PROC_OID: u32 = 3508;
pub const ENUM_NE_PROC_OID: u32 = 3509;
pub const ENUM_LT_PROC_OID: u32 = 3510;
pub const ENUM_GT_PROC_OID: u32 = 3511;
pub const ENUM_LE_PROC_OID: u32 = 3512;
pub const ENUM_GE_PROC_OID: u32 = 3513;
pub const ENUM_CMP_PROC_OID: u32 = 3514;
pub const HASH_ENUM_PROC_OID: u32 = 3515;
pub const HASH_ENUM_EXTENDED_PROC_OID: u32 = 3414;
pub const HASH_JSONB_PROC_OID: u32 = 4045;
pub const HASH_JSONB_EXTENDED_PROC_OID: u32 = 3416;
pub const HASH_RECORD_PROC_OID: u32 = 6192;
pub const HASH_RECORD_EXTENDED_PROC_OID: u32 = 6193;
pub const MACADDR_EQ_PROC_OID: u32 = 830;
pub const MACADDR_LT_PROC_OID: u32 = 831;
pub const MACADDR_LE_PROC_OID: u32 = 832;
pub const MACADDR_GT_PROC_OID: u32 = 833;
pub const MACADDR_GE_PROC_OID: u32 = 834;
pub const MACADDR_NE_PROC_OID: u32 = 835;
pub const MACADDR_CMP_PROC_OID: u32 = 836;
pub const MACADDR_TRUNC_PROC_OID: u32 = 753;
pub const HASH_MACADDR_PROC_OID: u32 = 399;
pub const HASH_MACADDR_EXTENDED_PROC_OID: u32 = 778;
pub const MACADDR_NOT_PROC_OID: u32 = 3144;
pub const MACADDR_AND_PROC_OID: u32 = 3145;
pub const MACADDR_OR_PROC_OID: u32 = 3146;
pub const HASH_MACADDR8_PROC_OID: u32 = 328;
pub const HASH_MACADDR8_EXTENDED_PROC_OID: u32 = 781;
pub const MACADDR8_EQ_PROC_OID: u32 = 4113;
pub const MACADDR8_LT_PROC_OID: u32 = 4114;
pub const MACADDR8_LE_PROC_OID: u32 = 4115;
pub const MACADDR8_GT_PROC_OID: u32 = 4116;
pub const MACADDR8_GE_PROC_OID: u32 = 4117;
pub const MACADDR8_NE_PROC_OID: u32 = 4118;
pub const MACADDR8_CMP_PROC_OID: u32 = 4119;
pub const MACADDR8_TRUNC_PROC_OID: u32 = 4112;
pub const MACADDR8_NOT_PROC_OID: u32 = 4120;
pub const MACADDR8_AND_PROC_OID: u32 = 4121;
pub const MACADDR8_OR_PROC_OID: u32 = 4122;
pub const MACADDR_TO_MACADDR8_PROC_OID: u32 = 4123;
pub const MACADDR8_TO_MACADDR_PROC_OID: u32 = 4124;
pub const MACADDR8_SET7BIT_PROC_OID: u32 = 4125;
pub const NAME_CMP_EQ_PROC_OID: u32 = 62;
pub const NAME_CMP_LT_PROC_OID: u32 = 655;
pub const NAME_CMP_LE_PROC_OID: u32 = 656;
pub const NAME_CMP_GT_PROC_OID: u32 = 657;
pub const NAME_CMP_GE_PROC_OID: u32 = 658;
pub const NAME_CMP_NE_PROC_OID: u32 = 659;
pub const BTNAMECMP_PROC_OID: u32 = 359;
pub const NAME_EQ_TEXT_PROC_OID: u32 = 240;
pub const NAME_LT_TEXT_PROC_OID: u32 = 241;
pub const NAME_LE_TEXT_PROC_OID: u32 = 242;
pub const NAME_GE_TEXT_PROC_OID: u32 = 243;
pub const NAME_GT_TEXT_PROC_OID: u32 = 244;
pub const NAME_NE_TEXT_PROC_OID: u32 = 245;
pub const BT_NAME_TEXT_CMP_PROC_OID: u32 = 246;
pub const TEXT_EQ_NAME_PROC_OID: u32 = 247;
pub const TEXT_LT_NAME_PROC_OID: u32 = 248;
pub const TEXT_LE_NAME_PROC_OID: u32 = 249;
pub const TEXT_GE_NAME_PROC_OID: u32 = 250;
pub const TEXT_GT_NAME_PROC_OID: u32 = 251;
pub const TEXT_NE_NAME_PROC_OID: u32 = 252;
pub const BT_TEXT_NAME_CMP_PROC_OID: u32 = 253;
pub const VARCHAR_CMP_EQ_PROC_OID: u32 = 76551;
pub const NUMERIC_CMP_EQ_PROC_OID: u32 = 1718;
pub const NUMERIC_CMP_NE_PROC_OID: u32 = 1719;
pub const NUMERIC_CMP_GT_PROC_OID: u32 = 1720;
pub const NUMERIC_CMP_GE_PROC_OID: u32 = 1721;
pub const NUMERIC_CMP_LT_PROC_OID: u32 = 1722;
pub const NUMERIC_CMP_LE_PROC_OID: u32 = 1723;
pub const ARRAY_CMP_EQ_PROC_OID: u32 = 744;
pub const ARRAY_CMP_NE_PROC_OID: u32 = 390;
pub const ARRAY_CMP_LT_PROC_OID: u32 = 391;
pub const ARRAY_CMP_GT_PROC_OID: u32 = 392;
pub const ARRAY_CMP_LE_PROC_OID: u32 = 393;
pub const ARRAY_CMP_GE_PROC_OID: u32 = 396;
pub const MULTIRANGE_CMP_EQ_PROC_OID: u32 = 4244;
pub const MULTIRANGE_CMP_NE_PROC_OID: u32 = 4245;
pub const MULTIRANGE_CMP_PROC_OID: u32 = 4273;
pub const MULTIRANGE_CMP_LT_PROC_OID: u32 = 4274;
pub const MULTIRANGE_CMP_LE_PROC_OID: u32 = 4275;
pub const MULTIRANGE_CMP_GE_PROC_OID: u32 = 4276;
pub const MULTIRANGE_CMP_GT_PROC_OID: u32 = 4277;
pub const UUID_CMP_LT_PROC_OID: u32 = 2954;
pub const UUID_CMP_LE_PROC_OID: u32 = 2955;
pub const UUID_CMP_EQ_PROC_OID: u32 = 2956;
pub const UUID_CMP_GE_PROC_OID: u32 = 2957;
pub const UUID_CMP_GT_PROC_OID: u32 = 2958;
pub const UUID_CMP_NE_PROC_OID: u32 = 2959;
pub const UUID_CMP_PROC_OID: u32 = 2960;
pub const MULTIRANGE_GIST_UNION_PROC_OID: u32 = 76620;
pub const MULTIRANGE_GIST_PENALTY_PROC_OID: u32 = 76621;
pub const MULTIRANGE_GIST_PICKSPLIT_PROC_OID: u32 = 76622;
pub const MULTIRANGE_GIST_SAME_PROC_OID: u32 = 76623;
pub const MULTIRANGE_SORTSUPPORT_PROC_OID: u32 = 76624;
pub const BTOIDVECTORCMP_PROC_OID: u32 = 404;
pub const OIDVECTOR_CMP_NE_PROC_OID: u32 = 619;
pub const OIDVECTOR_CMP_LT_PROC_OID: u32 = 677;
pub const OIDVECTOR_CMP_LE_PROC_OID: u32 = 678;
pub const OIDVECTOR_CMP_EQ_PROC_OID: u32 = 679;
pub const OIDVECTOR_CMP_GE_PROC_OID: u32 = 680;
pub const OIDVECTOR_CMP_GT_PROC_OID: u32 = 681;
pub const INT4_MINUS_PROC_OID: u32 = 181;
pub const INT4_PLUS_PROC_OID: u32 = 177;
pub const INT4_UMINUS_PROC_OID: u32 = 212;
pub const INFORMATION_SCHEMA_EXPANDARRAY_PROC_OID: u32 = 78220;
pub const INFORMATION_SCHEMA_INDEX_POSITION_PROC_OID: u32 = 78221;
pub const EQSEL_PROC_OID: u32 = 101;
pub const SCALARLTSEL_PROC_OID: u32 = 102;
pub const EQJOINSEL_PROC_OID: u32 = 105;
pub const AGG_TRANSITION_PROC_OID_BASE: u32 = 880_000;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PgProcRow {
    pub oid: u32,
    pub proname: String,
    pub pronamespace: u32,
    pub proowner: u32,
    pub proacl: Option<Vec<String>>,
    pub prolang: u32,
    pub procost: f64,
    pub prorows: f64,
    pub provariadic: u32,
    pub prosupport: u32,
    pub prokind: char,
    pub prosecdef: bool,
    pub proleakproof: bool,
    pub proisstrict: bool,
    pub proretset: bool,
    pub provolatile: char,
    pub proparallel: char,
    pub pronargs: i16,
    pub pronargdefaults: i16,
    pub prorettype: u32,
    pub proargtypes: String,
    pub proallargtypes: Option<Vec<u32>>,
    pub proargmodes: Option<Vec<u8>>,
    pub proargnames: Option<Vec<String>>,
    pub proargdefaults: Option<String>,
    pub prosrc: String,
    pub probin: Option<String>,
    pub prosqlbody: Option<String>,
    pub proconfig: Option<Vec<String>>,
}

impl Eq for PgProcRow {}

pub fn pg_proc_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("oid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("proname", SqlType::new(SqlTypeKind::Name), false),
            column_desc("pronamespace", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("proowner", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("prolang", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("procost", SqlType::new(SqlTypeKind::Float4), false),
            column_desc("prorows", SqlType::new(SqlTypeKind::Float4), false),
            column_desc("provariadic", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("prosupport", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("prokind", SqlType::new(SqlTypeKind::InternalChar), false),
            column_desc("prosecdef", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("proleakproof", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("proisstrict", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("proretset", SqlType::new(SqlTypeKind::Bool), false),
            column_desc(
                "provolatile",
                SqlType::new(SqlTypeKind::InternalChar),
                false,
            ),
            column_desc(
                "proparallel",
                SqlType::new(SqlTypeKind::InternalChar),
                false,
            ),
            column_desc("pronargs", SqlType::new(SqlTypeKind::Int2), false),
            column_desc("pronargdefaults", SqlType::new(SqlTypeKind::Int2), false),
            column_desc("prorettype", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("proargtypes", SqlType::new(SqlTypeKind::OidVector), false),
            column_desc(
                "proallargtypes",
                SqlType::array_of(SqlType::new(SqlTypeKind::Oid)),
                true,
            ),
            column_desc(
                "proargmodes",
                SqlType::array_of(SqlType::new(SqlTypeKind::InternalChar)),
                true,
            ),
            column_desc(
                "proargnames",
                SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
                true,
            ),
            column_desc(
                "proargdefaults",
                SqlType::new(SqlTypeKind::PgNodeTree),
                true,
            ),
            column_desc("prosrc", SqlType::new(SqlTypeKind::Text), false),
            column_desc("probin", SqlType::new(SqlTypeKind::Text), true),
            column_desc("prosqlbody", SqlType::new(SqlTypeKind::PgNodeTree), true),
            column_desc(
                "proconfig",
                SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
                true,
            ),
            column_desc(
                "proacl",
                SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
                true,
            ),
        ],
    }
}

pub fn bootstrap_pg_proc_rows_ref() -> &'static [PgProcRow] {
    static ROWS: OnceLock<Vec<PgProcRow>> = OnceLock::new();
    ROWS.get_or_init(build_bootstrap_pg_proc_rows).as_slice()
}

pub fn bootstrap_pg_proc_rows() -> Vec<PgProcRow> {
    bootstrap_pg_proc_rows_ref().to_vec()
}

fn bootstrap_pg_proc_by_oid() -> &'static BTreeMap<u32, &'static PgProcRow> {
    static ROWS_BY_OID: OnceLock<BTreeMap<u32, &'static PgProcRow>> = OnceLock::new();
    ROWS_BY_OID.get_or_init(|| {
        bootstrap_pg_proc_rows_ref()
            .iter()
            .map(|row| (row.oid, row))
            .collect()
    })
}

pub fn bootstrap_pg_proc_row_by_oid(oid: u32) -> Option<PgProcRow> {
    bootstrap_pg_proc_by_oid()
        .get(&oid)
        .map(|row| (**row).clone())
}

fn bootstrap_pg_proc_by_name() -> &'static BTreeMap<String, Vec<&'static PgProcRow>> {
    static ROWS_BY_NAME: OnceLock<BTreeMap<String, Vec<&'static PgProcRow>>> = OnceLock::new();
    ROWS_BY_NAME.get_or_init(|| {
        let mut rows_by_name: BTreeMap<String, Vec<&'static PgProcRow>> = BTreeMap::new();
        for row in bootstrap_pg_proc_rows_ref() {
            rows_by_name
                .entry(row.proname.to_ascii_lowercase())
                .or_default()
                .push(row);
        }
        rows_by_name
    })
}

pub fn bootstrap_pg_proc_rows_by_name(name: &str) -> Vec<PgProcRow> {
    let normalized = name
        .strip_prefix("pg_catalog.")
        .unwrap_or(name)
        .to_ascii_lowercase();
    bootstrap_pg_proc_by_name()
        .get(&normalized)
        .map(|rows| rows.iter().map(|row| (**row).clone()).collect())
        .unwrap_or_default()
}

pub fn is_bootstrap_proc_oid(oid: u32) -> bool {
    bootstrap_pg_proc_by_oid().contains_key(&oid)
}

fn bootstrap_proc_acl_overrides() -> &'static RwLock<BTreeMap<u32, Vec<String>>> {
    static ACLS: OnceLock<RwLock<BTreeMap<u32, Vec<String>>>> = OnceLock::new();
    ACLS.get_or_init(|| RwLock::new(BTreeMap::new()))
}

pub fn set_bootstrap_proc_acl_override(oid: u32, acl: Option<Vec<String>>) {
    let mut overrides = bootstrap_proc_acl_overrides()
        .write()
        .expect("bootstrap proc ACL override lock poisoned");
    if let Some(acl) = acl {
        overrides.insert(oid, acl);
    } else {
        overrides.remove(&oid);
    }
}

pub fn bootstrap_proc_acl_override(oid: u32) -> Option<Vec<String>> {
    bootstrap_proc_acl_overrides()
        .read()
        .expect("bootstrap proc ACL override lock poisoned")
        .get(&oid)
        .cloned()
}

fn build_bootstrap_pg_proc_rows() -> Vec<PgProcRow> {
    serde_json::from_slice(include_bytes!("../data/pg_proc.json"))
        .expect("decode embedded pg_proc catalog data")
}

pub fn aggregate_transition_proc_oid(aggfnoid: u32) -> u32 {
    AGG_TRANSITION_PROC_OID_BASE + aggfnoid
}

pub fn builtin_scalar_function_for_proc_row(row: &PgProcRow) -> Option<BuiltinScalarFunction> {
    let builtin_by_src = builtin_scalar_function_for_proc_src(&row.prosrc);
    if row.prosrc.eq_ignore_ascii_case("test_atomic_ops") {
        return Some(BuiltinScalarFunction::PgRustTestAtomicOps);
    }
    if row.pronamespace != PG_CATALOG_NAMESPACE_OID {
        // :HACK: PostgreSQL regressions define a small number of C helper
        // functions in public with symbols that pgrust implements natively.
        // Keep this whitelist explicit so arbitrary user C symbols still fail
        // as unsupported internal functions.
        if matches!(
            row.prosrc.to_ascii_lowercase().as_str(),
            "interpt_pp" | "test_canonicalize_path" | "test_relpath" | "reverse_name" | "overpaid"
        ) {
            return builtin_by_src;
        }
        return builtin_by_src.filter(|func| is_dynamic_range_scalar_function(*func));
    }
    if row.proname.eq_ignore_ascii_case("timestamptz")
        && matches!(row.proargtypes.trim(), "1082 1083" | "1082 1266")
    {
        return Some(BuiltinScalarFunction::TimestampTzConstructor);
    }
    builtin_by_src.or_else(|| builtin_scalar_function_for_proc_src(&row.proname))
}

fn is_dynamic_range_scalar_function(func: BuiltinScalarFunction) -> bool {
    matches!(
        func,
        BuiltinScalarFunction::RangeConstructor
            | BuiltinScalarFunction::RangeIsEmpty
            | BuiltinScalarFunction::RangeLower
            | BuiltinScalarFunction::RangeUpper
            | BuiltinScalarFunction::RangeLowerInc
            | BuiltinScalarFunction::RangeUpperInc
            | BuiltinScalarFunction::RangeLowerInf
            | BuiltinScalarFunction::RangeUpperInf
            | BuiltinScalarFunction::RangeContains
            | BuiltinScalarFunction::RangeContainedBy
            | BuiltinScalarFunction::RangeOverlap
            | BuiltinScalarFunction::RangeStrictLeft
            | BuiltinScalarFunction::RangeStrictRight
            | BuiltinScalarFunction::RangeOverLeft
            | BuiltinScalarFunction::RangeOverRight
            | BuiltinScalarFunction::RangeAdjacent
            | BuiltinScalarFunction::RangeUnion
            | BuiltinScalarFunction::RangeIntersect
            | BuiltinScalarFunction::RangeDifference
            | BuiltinScalarFunction::RangeMerge
            | BuiltinScalarFunction::PgRustInternalBinaryCoercible
            | BuiltinScalarFunction::PgRustTestAtomicOps
            | BuiltinScalarFunction::MakeTupleIndirect
            | BuiltinScalarFunction::PgRustIsCatalogTextUniqueIndexOid
    )
}

fn builtin_scalar_function_for_proc_src(proc_src: &str) -> Option<BuiltinScalarFunction> {
    hash_scalar_function_for_proc_src(proc_src).or_else(|| {
        legacy_scalar_function_entries()
            .iter()
            .find_map(|(name, func)| proc_src.eq_ignore_ascii_case(name).then_some(*func))
            .or_else(|| {
                range_prefixed_proc_src(proc_src).and_then(builtin_scalar_function_for_proc_src)
            })
            .or_else(|| {
                proc_src
                    .rsplit_once('_')
                    .filter(|(_, suffix)| suffix.chars().all(|ch| ch.is_ascii_digit()))
                    .and_then(|(base, _)| builtin_scalar_function_for_proc_src(base))
            })
    })
}

fn hash_scalar_function_for_proc_src(proc_src: &str) -> Option<BuiltinScalarFunction> {
    let normalized = proc_src.to_ascii_lowercase();
    let (base, extended) = normalized
        .strip_suffix("_extended")
        .map(|base| (base, true))
        .or_else(|| normalized.strip_suffix("extended").map(|base| (base, true)))
        .unwrap_or((normalized.as_str(), false));
    let kind = match base {
        "hashbool" => HashFunctionKind::Bool,
        "hashint2" => HashFunctionKind::Int2,
        "hashint4" => HashFunctionKind::Int4,
        "hashint8" => HashFunctionKind::Int8,
        "hashoid" => HashFunctionKind::Oid,
        "hashchar" => HashFunctionKind::InternalChar,
        "hashname" => HashFunctionKind::Name,
        "hashtext" => HashFunctionKind::Text,
        "hashvarchar" => HashFunctionKind::Varchar,
        "hashbpchar" => HashFunctionKind::BpChar,
        "hashfloat4" => HashFunctionKind::Float4,
        "hashfloat8" => HashFunctionKind::Float8,
        "hash_numeric" => HashFunctionKind::Numeric,
        "hashtimestamp" | "timestamp_hash" => HashFunctionKind::Timestamp,
        "hashtimestamptz" | "timestamptz_hash" => HashFunctionKind::TimestampTz,
        "hashdate" => HashFunctionKind::Date,
        "hashtime" | "time_hash" => HashFunctionKind::Time,
        "hashtimetz" | "timetz_hash" => HashFunctionKind::TimeTz,
        "hashbytea" => HashFunctionKind::Bytea,
        "hashoidvector" => HashFunctionKind::OidVector,
        "hash_aclitem" => HashFunctionKind::AclItem,
        "hashinet" => HashFunctionKind::Inet,
        "hashmacaddr" => HashFunctionKind::MacAddr,
        "hashmacaddr8" => HashFunctionKind::MacAddr8,
        "hash_array" => HashFunctionKind::Array,
        "interval_hash" => HashFunctionKind::Interval,
        "uuid_hash" => HashFunctionKind::Uuid,
        "pg_lsn_hash" => HashFunctionKind::PgLsn,
        "hashenum" => HashFunctionKind::Enum,
        "jsonb_hash" => HashFunctionKind::Jsonb,
        "hash_range" => HashFunctionKind::Range,
        "hash_multirange" => HashFunctionKind::Multirange,
        "hash_record" => HashFunctionKind::Record,
        _ => return None,
    };
    Some(if extended {
        BuiltinScalarFunction::HashValueExtended(kind)
    } else {
        BuiltinScalarFunction::HashValue(kind)
    })
}

fn range_prefixed_proc_src(proc_src: &str) -> Option<&str> {
    let stripped = [
        "int4range_",
        "int8range_",
        "numrange_",
        "daterange_",
        "tsrange_",
        "tstzrange_",
        "arrayrange_",
        "varbitrange_",
    ]
    .into_iter()
    .find_map(|prefix| proc_src.strip_prefix(prefix))?;

    [
        "range_constructor2",
        "range_constructor3",
        "range_isempty",
        "range_lower_inc",
        "range_upper_inc",
        "range_lower_inf",
        "range_upper_inf",
        "range_lower",
        "range_upper",
        "range_merge",
        "range_adjacent",
        "range_difference",
        "range_contains",
        "range_contained_by",
        "range_strict_left",
        "range_over_left",
        "range_strict_right",
        "range_over_right",
        "range_overlap",
        "range_union",
        "range_intersect",
    ]
    .into_iter()
    .find(|base| stripped == *base || stripped.starts_with(&format!("{base}_")))
}

#[cfg(test)]
fn aggregate_func_for_proname(name: &str) -> Option<AggFunc> {
    match name.to_ascii_lowercase().as_str() {
        "count" => Some(AggFunc::Count),
        "any_value" => Some(AggFunc::AnyValue),
        "sum" => Some(AggFunc::Sum),
        "avg" => Some(AggFunc::Avg),
        "variance" | "var_samp" => Some(AggFunc::VarSamp),
        "var_pop" => Some(AggFunc::VarPop),
        "stddev" | "stddev_samp" => Some(AggFunc::StddevSamp),
        "stddev_pop" => Some(AggFunc::StddevPop),
        "regr_count" => Some(AggFunc::RegrCount),
        "regr_sxx" => Some(AggFunc::RegrSxx),
        "regr_syy" => Some(AggFunc::RegrSyy),
        "regr_sxy" => Some(AggFunc::RegrSxy),
        "regr_avgx" => Some(AggFunc::RegrAvgX),
        "regr_avgy" => Some(AggFunc::RegrAvgY),
        "regr_r2" => Some(AggFunc::RegrR2),
        "regr_slope" => Some(AggFunc::RegrSlope),
        "regr_intercept" => Some(AggFunc::RegrIntercept),
        "covar_pop" => Some(AggFunc::CovarPop),
        "covar_samp" => Some(AggFunc::CovarSamp),
        "corr" => Some(AggFunc::Corr),
        "bool_and" | "every" => Some(AggFunc::BoolAnd),
        "bool_or" => Some(AggFunc::BoolOr),
        "bit_and" => Some(AggFunc::BitAnd),
        "bit_or" => Some(AggFunc::BitOr),
        "bit_xor" => Some(AggFunc::BitXor),
        "min" => Some(AggFunc::Min),
        "max" => Some(AggFunc::Max),
        "string_agg" => Some(AggFunc::StringAgg),
        "array_agg" => Some(AggFunc::ArrayAgg),
        "json_agg" => Some(AggFunc::JsonAgg),
        "jsonb_agg" => Some(AggFunc::JsonbAgg),
        "json_object_agg" => Some(AggFunc::JsonObjectAgg),
        "json_object_agg_unique" => Some(AggFunc::JsonObjectAggUnique),
        "json_object_agg_unique_strict" => Some(AggFunc::JsonObjectAggUniqueStrict),
        "jsonb_object_agg" => Some(AggFunc::JsonbObjectAgg),
        "jsonb_object_agg_unique" => Some(AggFunc::JsonbObjectAggUnique),
        "jsonb_object_agg_unique_strict" => Some(AggFunc::JsonbObjectAggUniqueStrict),
        "xmlagg" => Some(AggFunc::XmlAgg),
        "range_agg" => Some(AggFunc::RangeAgg),
        "range_intersect_agg" => Some(AggFunc::RangeIntersectAgg),
        _ => None,
    }
}

#[cfg(test)]
fn window_func_for_proname(name: &str) -> Option<BuiltinWindowFunction> {
    match name.to_ascii_lowercase().as_str() {
        "row_number" => Some(BuiltinWindowFunction::RowNumber),
        "rank" => Some(BuiltinWindowFunction::Rank),
        "dense_rank" => Some(BuiltinWindowFunction::DenseRank),
        "percent_rank" => Some(BuiltinWindowFunction::PercentRank),
        "cume_dist" => Some(BuiltinWindowFunction::CumeDist),
        "ntile" => Some(BuiltinWindowFunction::Ntile),
        "lag" => Some(BuiltinWindowFunction::Lag),
        "lead" => Some(BuiltinWindowFunction::Lead),
        "first_value" => Some(BuiltinWindowFunction::FirstValue),
        "last_value" => Some(BuiltinWindowFunction::LastValue),
        "nth_value" => Some(BuiltinWindowFunction::NthValue),
        _ => None,
    }
}

#[cfg(test)]
fn hypothetical_aggregate_func_for_proname(name: &str) -> Option<HypotheticalAggFunc> {
    match name.to_ascii_lowercase().as_str() {
        "rank" => Some(HypotheticalAggFunc::Rank),
        "dense_rank" => Some(HypotheticalAggFunc::DenseRank),
        "percent_rank" => Some(HypotheticalAggFunc::PercentRank),
        "cume_dist" => Some(HypotheticalAggFunc::CumeDist),
        _ => None,
    }
}

fn legacy_scalar_function_entries() -> &'static [(&'static str, BuiltinScalarFunction)] {
    &[
        ("random", BuiltinScalarFunction::Random),
        ("drandom", BuiltinScalarFunction::Random),
        ("int4random", BuiltinScalarFunction::Random),
        ("int8random", BuiltinScalarFunction::Random),
        ("numeric_random", BuiltinScalarFunction::Random),
        ("random_normal", BuiltinScalarFunction::RandomNormal),
        ("drandom_normal", BuiltinScalarFunction::RandomNormal),
        ("drandom_normal_noargs", BuiltinScalarFunction::RandomNormal),
        ("setseed", BuiltinScalarFunction::SetSeed),
        ("current_database", BuiltinScalarFunction::CurrentDatabase),
        (
            "make_tuple_indirect",
            BuiltinScalarFunction::MakeTupleIndirect,
        ),
        ("reverse_name", BuiltinScalarFunction::TestReverseName),
        ("overpaid", BuiltinScalarFunction::TestOverpaid),
        ("current_schemas", BuiltinScalarFunction::CurrentSchemas),
        ("pg_backend_pid", BuiltinScalarFunction::PgBackendPid),
        ("pg_cancel_backend", BuiltinScalarFunction::PgCancelBackend),
        (
            "pg_terminate_backend",
            BuiltinScalarFunction::PgTerminateBackend,
        ),
        ("pg_blocking_pids", BuiltinScalarFunction::PgBlockingPids),
        (
            "pg_isolation_test_session_is_blocked",
            BuiltinScalarFunction::PgIsolationTestSessionIsBlocked,
        ),
        (
            "pg_settings_get_flags",
            BuiltinScalarFunction::PgSettingsGetFlags,
        ),
        ("pg_get_partkeydef", BuiltinScalarFunction::PgGetPartKeyDef),
        (
            "pg_table_is_visible",
            BuiltinScalarFunction::PgTableIsVisible,
        ),
        ("pg_type_is_visible", BuiltinScalarFunction::PgTypeIsVisible),
        (
            "pg_operator_is_visible",
            BuiltinScalarFunction::PgOperatorIsVisible,
        ),
        (
            "pg_opclass_is_visible",
            BuiltinScalarFunction::PgOpclassIsVisible,
        ),
        (
            "pg_opfamily_is_visible",
            BuiltinScalarFunction::PgOpfamilyIsVisible,
        ),
        (
            "pg_conversion_is_visible",
            BuiltinScalarFunction::PgConversionIsVisible,
        ),
        (
            "pg_collation_is_visible",
            BuiltinScalarFunction::PgCollationIsVisible,
        ),
        (
            "pg_ts_parser_is_visible",
            BuiltinScalarFunction::PgTsParserIsVisible,
        ),
        (
            "pg_ts_dict_is_visible",
            BuiltinScalarFunction::PgTsDictIsVisible,
        ),
        (
            "pg_ts_template_is_visible",
            BuiltinScalarFunction::PgTsTemplateIsVisible,
        ),
        (
            "pg_ts_config_is_visible",
            BuiltinScalarFunction::PgTsConfigIsVisible,
        ),
        ("cashlarger", BuiltinScalarFunction::CashLarger),
        ("cashsmaller", BuiltinScalarFunction::CashSmaller),
        ("cash_words", BuiltinScalarFunction::CashWords),
        ("table_to_xml", BuiltinScalarFunction::UnsupportedXmlFeature),
        (
            "table_to_xmlschema",
            BuiltinScalarFunction::UnsupportedXmlFeature,
        ),
        (
            "table_to_xml_and_xmlschema",
            BuiltinScalarFunction::UnsupportedXmlFeature,
        ),
        ("query_to_xml", BuiltinScalarFunction::UnsupportedXmlFeature),
        (
            "query_to_xmlschema",
            BuiltinScalarFunction::UnsupportedXmlFeature,
        ),
        (
            "query_to_xml_and_xmlschema",
            BuiltinScalarFunction::UnsupportedXmlFeature,
        ),
        (
            "cursor_to_xml",
            BuiltinScalarFunction::UnsupportedXmlFeature,
        ),
        (
            "cursor_to_xmlschema",
            BuiltinScalarFunction::UnsupportedXmlFeature,
        ),
        (
            "schema_to_xml",
            BuiltinScalarFunction::UnsupportedXmlFeature,
        ),
        (
            "schema_to_xmlschema",
            BuiltinScalarFunction::UnsupportedXmlFeature,
        ),
        (
            "schema_to_xml_and_xmlschema",
            BuiltinScalarFunction::UnsupportedXmlFeature,
        ),
        (
            "pg_get_constraintdef",
            BuiltinScalarFunction::PgGetConstraintDef,
        ),
        (
            "pg_get_constraintdef_ext",
            BuiltinScalarFunction::PgGetConstraintDef,
        ),
        ("pg_get_partkeydef", BuiltinScalarFunction::PgGetPartKeyDef),
        ("pg_get_indexdef", BuiltinScalarFunction::PgGetIndexDef),
        ("pg_get_indexdef_ext", BuiltinScalarFunction::PgGetIndexDef),
        ("pg_get_partkeydef", BuiltinScalarFunction::PgGetPartKeyDef),
        (
            "pg_get_partition_constraintdef",
            BuiltinScalarFunction::PgGetPartitionConstraintDef,
        ),
        ("xmlcomment", BuiltinScalarFunction::XmlComment),
        ("xmltext", BuiltinScalarFunction::XmlText),
        ("xml_is_well_formed", BuiltinScalarFunction::XmlIsWellFormed),
        (
            "xml_is_well_formed_document",
            BuiltinScalarFunction::XmlIsWellFormedDocument,
        ),
        (
            "xml_is_well_formed_content",
            BuiltinScalarFunction::XmlIsWellFormedContent,
        ),
        ("xpath", BuiltinScalarFunction::XPath),
        ("xpath_exists", BuiltinScalarFunction::XPathExists),
        ("xmlexists", BuiltinScalarFunction::XPathExists),
        ("pg_get_triggerdef", BuiltinScalarFunction::PgGetTriggerDef),
        ("pg_trigger_depth", BuiltinScalarFunction::PgTriggerDepth),
        ("now", BuiltinScalarFunction::Now),
        (
            "transaction_timestamp",
            BuiltinScalarFunction::TransactionTimestamp,
        ),
        (
            "statement_timestamp",
            BuiltinScalarFunction::StatementTimestamp,
        ),
        ("clock_timestamp", BuiltinScalarFunction::ClockTimestamp),
        ("timeofday", BuiltinScalarFunction::TimeOfDay),
        ("pg_sleep", BuiltinScalarFunction::PgSleep),
        ("pg_sleep_for", BuiltinScalarFunction::PgSleep),
        ("timezone", BuiltinScalarFunction::Timezone),
        ("date_part", BuiltinScalarFunction::DatePart),
        ("extract", BuiltinScalarFunction::Extract),
        ("date_trunc", BuiltinScalarFunction::DateTrunc),
        ("date_bin", BuiltinScalarFunction::DateBin),
        ("date_add", BuiltinScalarFunction::DateAdd),
        ("date_subtract", BuiltinScalarFunction::DateSubtract),
        ("age", BuiltinScalarFunction::Age),
        ("justify_days", BuiltinScalarFunction::JustifyDays),
        ("justify_hours", BuiltinScalarFunction::JustifyHours),
        ("justify_interval", BuiltinScalarFunction::JustifyInterval),
        ("isfinite", BuiltinScalarFunction::IsFinite),
        ("make_interval", BuiltinScalarFunction::MakeInterval),
        ("make_date", BuiltinScalarFunction::MakeDate),
        ("make_time", BuiltinScalarFunction::MakeTime),
        ("make_timestamp", BuiltinScalarFunction::MakeTimestamp),
        ("make_timestamptz", BuiltinScalarFunction::MakeTimestampTz),
        ("interval_hash", BuiltinScalarFunction::IntervalHash),
        ("uuid_in", BuiltinScalarFunction::UuidIn),
        ("uuid_out", BuiltinScalarFunction::UuidOut),
        ("uuid_recv", BuiltinScalarFunction::UuidRecv),
        ("uuid_send", BuiltinScalarFunction::UuidSend),
        ("uuid_eq", BuiltinScalarFunction::UuidEq),
        ("uuid_ne", BuiltinScalarFunction::UuidNe),
        ("uuid_lt", BuiltinScalarFunction::UuidLt),
        ("uuid_le", BuiltinScalarFunction::UuidLe),
        ("uuid_gt", BuiltinScalarFunction::UuidGt),
        ("uuid_ge", BuiltinScalarFunction::UuidGe),
        ("uuid_cmp", BuiltinScalarFunction::UuidCmp),
        ("uuid_hash", BuiltinScalarFunction::UuidHash),
        (
            "uuid_hash_extended",
            BuiltinScalarFunction::UuidHashExtended,
        ),
        ("gen_random_uuid", BuiltinScalarFunction::GenRandomUuid),
        ("uuidv7", BuiltinScalarFunction::UuidV7),
        ("uuidv7_interval", BuiltinScalarFunction::UuidV7),
        (
            "uuid_extract_version",
            BuiltinScalarFunction::UuidExtractVersion,
        ),
        (
            "uuid_extract_timestamp",
            BuiltinScalarFunction::UuidExtractTimestamp,
        ),
        (
            "getdatabaseencoding",
            BuiltinScalarFunction::GetDatabaseEncoding,
        ),
        ("unicode_version", BuiltinScalarFunction::UnicodeVersion),
        ("unicode_assigned", BuiltinScalarFunction::UnicodeAssigned),
        ("normalize", BuiltinScalarFunction::Normalize),
        ("unicode_normalize_func", BuiltinScalarFunction::Normalize),
        ("is_normalized", BuiltinScalarFunction::IsNormalized),
        ("unicode_is_normalized", BuiltinScalarFunction::IsNormalized),
        (
            "pg_char_to_encoding",
            BuiltinScalarFunction::PgCharToEncoding,
        ),
        (
            "pg_encoding_to_char",
            BuiltinScalarFunction::PgEncodingToChar,
        ),
        ("pg_partition_root", BuiltinScalarFunction::PgPartitionRoot),
        (
            "satisfies_hash_partition",
            BuiltinScalarFunction::SatisfiesHashPartition,
        ),
        (
            "pg_relation_filenode",
            BuiltinScalarFunction::PgRelationFilenode,
        ),
        (
            "pg_relation_is_updatable",
            BuiltinScalarFunction::PgRelationIsUpdatable,
        ),
        (
            "pg_column_is_updatable",
            BuiltinScalarFunction::PgColumnIsUpdatable,
        ),
        (
            "pg_filenode_relation",
            BuiltinScalarFunction::PgFilenodeRelation,
        ),
        (
            "pg_tablespace_location",
            BuiltinScalarFunction::PgTablespaceLocation,
        ),
        ("pg_my_temp_schema", BuiltinScalarFunction::PgMyTempSchema),
        (
            "pg_rust_internal_binary_coercible",
            BuiltinScalarFunction::PgRustInternalBinaryCoercible,
        ),
        (
            "binary_coercible",
            BuiltinScalarFunction::PgRustInternalBinaryCoercible,
        ),
        (
            "pg_rust_test_fdw_handler",
            BuiltinScalarFunction::PgRustTestFdwHandler,
        ),
        (
            "test_fdw_handler",
            BuiltinScalarFunction::PgRustTestFdwHandler,
        ),
        (
            "pg_rust_test_enc_setup",
            BuiltinScalarFunction::PgRustTestEncSetup,
        ),
        (
            "pg_rust_test_enc_conversion",
            BuiltinScalarFunction::PgRustTestEncConversion,
        ),
        (
            "test_atomic_ops",
            BuiltinScalarFunction::PgRustTestAtomicOps,
        ),
        (
            "pg_rust_is_catalog_text_unique_index_oid",
            BuiltinScalarFunction::PgRustIsCatalogTextUniqueIndexOid,
        ),
        ("amvalidate", BuiltinScalarFunction::AmValidate),
        ("btequalimage", BuiltinScalarFunction::BtEqualImage),
        ("current_setting", BuiltinScalarFunction::CurrentSetting),
        ("set_config", BuiltinScalarFunction::SetConfig),
        ("nextval", BuiltinScalarFunction::NextVal),
        ("currval", BuiltinScalarFunction::CurrVal),
        ("lastval", BuiltinScalarFunction::LastVal),
        ("currtid2", BuiltinScalarFunction::CurrTid2),
        ("currtid_byrelname", BuiltinScalarFunction::CurrTid2),
        ("setval", BuiltinScalarFunction::SetVal),
        ("setval_oid", BuiltinScalarFunction::SetVal),
        ("setval_text", BuiltinScalarFunction::SetVal),
        ("setval3_oid", BuiltinScalarFunction::SetVal),
        ("setval3_text", BuiltinScalarFunction::SetVal),
        (
            "pg_get_serial_sequence",
            BuiltinScalarFunction::PgGetSerialSequence,
        ),
        (
            "pg_sequence_parameters",
            BuiltinScalarFunction::PgSequenceParameters,
        ),
        (
            "pg_sequence_last_value",
            BuiltinScalarFunction::PgSequenceLastValue,
        ),
        (
            "pg_get_sequence_data",
            BuiltinScalarFunction::PgGetSequenceData,
        ),
        ("pg_get_acl", BuiltinScalarFunction::PgGetAcl),
        ("makeaclitem", BuiltinScalarFunction::MakeAclItem),
        ("txid_current", BuiltinScalarFunction::TxidCurrent),
        ("pg_current_xact_id", BuiltinScalarFunction::TxidCurrent),
        (
            "txid_current_if_assigned",
            BuiltinScalarFunction::TxidCurrentIfAssigned,
        ),
        (
            "pg_current_xact_id_if_assigned",
            BuiltinScalarFunction::TxidCurrentIfAssigned,
        ),
        (
            "txid_current_snapshot",
            BuiltinScalarFunction::TxidCurrentSnapshot,
        ),
        (
            "pg_current_snapshot",
            BuiltinScalarFunction::TxidCurrentSnapshot,
        ),
        (
            "txid_snapshot_xmin",
            BuiltinScalarFunction::TxidSnapshotXmin,
        ),
        ("pg_snapshot_xmin", BuiltinScalarFunction::TxidSnapshotXmin),
        (
            "txid_snapshot_xmax",
            BuiltinScalarFunction::TxidSnapshotXmax,
        ),
        ("pg_snapshot_xmax", BuiltinScalarFunction::TxidSnapshotXmax),
        (
            "txid_visible_in_snapshot",
            BuiltinScalarFunction::TxidVisibleInSnapshot,
        ),
        (
            "pg_visible_in_snapshot",
            BuiltinScalarFunction::TxidVisibleInSnapshot,
        ),
        ("txid_status", BuiltinScalarFunction::TxidStatus),
        ("pg_xact_status", BuiltinScalarFunction::TxidStatus),
        ("pg_size_pretty", BuiltinScalarFunction::PgSizePretty),
        (
            "pg_size_pretty_numeric",
            BuiltinScalarFunction::PgSizePretty,
        ),
        ("pg_size_bytes", BuiltinScalarFunction::PgSizeBytes),
        ("parse_ident", BuiltinScalarFunction::ParseIdent),
        ("parse_ident_text", BuiltinScalarFunction::ParseIdent),
        ("pg_advisory_lock", BuiltinScalarFunction::PgAdvisoryLock),
        (
            "pg_advisory_lock_int8",
            BuiltinScalarFunction::PgAdvisoryLock,
        ),
        (
            "pg_advisory_lock_int4",
            BuiltinScalarFunction::PgAdvisoryLock,
        ),
        (
            "pg_advisory_xact_lock",
            BuiltinScalarFunction::PgAdvisoryXactLock,
        ),
        (
            "pg_advisory_xact_lock_int8",
            BuiltinScalarFunction::PgAdvisoryXactLock,
        ),
        (
            "pg_advisory_xact_lock_int4",
            BuiltinScalarFunction::PgAdvisoryXactLock,
        ),
        (
            "pg_advisory_lock_shared",
            BuiltinScalarFunction::PgAdvisoryLockShared,
        ),
        (
            "pg_advisory_lock_shared_int8",
            BuiltinScalarFunction::PgAdvisoryLockShared,
        ),
        (
            "pg_advisory_lock_shared_int4",
            BuiltinScalarFunction::PgAdvisoryLockShared,
        ),
        (
            "pg_advisory_xact_lock_shared",
            BuiltinScalarFunction::PgAdvisoryXactLockShared,
        ),
        (
            "pg_advisory_xact_lock_shared_int8",
            BuiltinScalarFunction::PgAdvisoryXactLockShared,
        ),
        (
            "pg_advisory_xact_lock_shared_int4",
            BuiltinScalarFunction::PgAdvisoryXactLockShared,
        ),
        (
            "pg_try_advisory_lock",
            BuiltinScalarFunction::PgTryAdvisoryLock,
        ),
        (
            "pg_try_advisory_lock_int8",
            BuiltinScalarFunction::PgTryAdvisoryLock,
        ),
        (
            "pg_try_advisory_lock_int4",
            BuiltinScalarFunction::PgTryAdvisoryLock,
        ),
        (
            "pg_try_advisory_xact_lock",
            BuiltinScalarFunction::PgTryAdvisoryXactLock,
        ),
        (
            "pg_try_advisory_xact_lock_int8",
            BuiltinScalarFunction::PgTryAdvisoryXactLock,
        ),
        (
            "pg_try_advisory_xact_lock_int4",
            BuiltinScalarFunction::PgTryAdvisoryXactLock,
        ),
        (
            "pg_try_advisory_lock_shared",
            BuiltinScalarFunction::PgTryAdvisoryLockShared,
        ),
        (
            "pg_try_advisory_lock_shared_int8",
            BuiltinScalarFunction::PgTryAdvisoryLockShared,
        ),
        (
            "pg_try_advisory_lock_shared_int4",
            BuiltinScalarFunction::PgTryAdvisoryLockShared,
        ),
        (
            "pg_try_advisory_xact_lock_shared",
            BuiltinScalarFunction::PgTryAdvisoryXactLockShared,
        ),
        (
            "pg_try_advisory_xact_lock_shared_int8",
            BuiltinScalarFunction::PgTryAdvisoryXactLockShared,
        ),
        (
            "pg_try_advisory_xact_lock_shared_int4",
            BuiltinScalarFunction::PgTryAdvisoryXactLockShared,
        ),
        (
            "pg_advisory_unlock",
            BuiltinScalarFunction::PgAdvisoryUnlock,
        ),
        (
            "pg_advisory_unlock_int8",
            BuiltinScalarFunction::PgAdvisoryUnlock,
        ),
        (
            "pg_advisory_unlock_int4",
            BuiltinScalarFunction::PgAdvisoryUnlock,
        ),
        (
            "pg_advisory_unlock_shared",
            BuiltinScalarFunction::PgAdvisoryUnlockShared,
        ),
        (
            "pg_advisory_unlock_shared_int8",
            BuiltinScalarFunction::PgAdvisoryUnlockShared,
        ),
        (
            "pg_advisory_unlock_shared_int4",
            BuiltinScalarFunction::PgAdvisoryUnlockShared,
        ),
        (
            "pg_advisory_unlock_all",
            BuiltinScalarFunction::PgAdvisoryUnlockAll,
        ),
        ("lo_create", BuiltinScalarFunction::LoCreate),
        ("lo_unlink", BuiltinScalarFunction::LoUnlink),
        ("lo_open", BuiltinScalarFunction::LoOpen),
        ("lo_close", BuiltinScalarFunction::LoClose),
        ("loread", BuiltinScalarFunction::LoRead),
        ("lowrite", BuiltinScalarFunction::LoWrite),
        ("lo_lseek", BuiltinScalarFunction::LoLseek),
        ("lo_lseek64", BuiltinScalarFunction::LoLseek64),
        ("lo_tell", BuiltinScalarFunction::LoTell),
        ("lo_tell64", BuiltinScalarFunction::LoTell64),
        ("lo_truncate", BuiltinScalarFunction::LoTruncate),
        ("lo_truncate64", BuiltinScalarFunction::LoTruncate64),
        ("lo_creat", BuiltinScalarFunction::LoCreat),
        ("lo_from_bytea", BuiltinScalarFunction::LoFromBytea),
        ("lo_get", BuiltinScalarFunction::LoGet),
        ("lo_put", BuiltinScalarFunction::LoPut),
        ("lo_import", BuiltinScalarFunction::LoImport),
        ("lo_export", BuiltinScalarFunction::LoExport),
        ("pg_typeof", BuiltinScalarFunction::PgTypeof),
        ("pg_basetype", BuiltinScalarFunction::PgBaseType),
        (
            "pg_column_compression",
            BuiltinScalarFunction::PgColumnCompression,
        ),
        (
            "pg_column_toast_chunk_id",
            BuiltinScalarFunction::PgColumnToastChunkId,
        ),
        ("pg_column_size", BuiltinScalarFunction::PgColumnSize),
        ("pg_relation_size", BuiltinScalarFunction::PgRelationSize),
        ("pg_numa_available", BuiltinScalarFunction::PgNumaAvailable),
        (
            "gin_clean_pending_list",
            BuiltinScalarFunction::GinCleanPendingList,
        ),
        (
            "brin_summarize_new_values",
            BuiltinScalarFunction::BrinSummarizeNewValues,
        ),
        (
            "brin_summarize_range",
            BuiltinScalarFunction::BrinSummarizeRange,
        ),
        (
            "brin_desummarize_range",
            BuiltinScalarFunction::BrinDesummarizeRange,
        ),
        ("pg_table_size", BuiltinScalarFunction::PgTableSize),
        ("pg_num_nulls", BuiltinScalarFunction::NumNulls),
        ("num_nulls", BuiltinScalarFunction::NumNulls),
        ("pg_num_nonnulls", BuiltinScalarFunction::NumNonNulls),
        ("num_nonnulls", BuiltinScalarFunction::NumNonNulls),
        (
            "pg_log_backend_memory_contexts",
            BuiltinScalarFunction::PgLogBackendMemoryContexts,
        ),
        (
            "has_function_privilege",
            BuiltinScalarFunction::HasFunctionPrivilege,
        ),
        (
            "has_function_privilege_name_name",
            BuiltinScalarFunction::HasFunctionPrivilege,
        ),
        (
            "has_function_privilege_name_id",
            BuiltinScalarFunction::HasFunctionPrivilege,
        ),
        (
            "has_function_privilege_id_name",
            BuiltinScalarFunction::HasFunctionPrivilege,
        ),
        (
            "has_function_privilege_id_id",
            BuiltinScalarFunction::HasFunctionPrivilege,
        ),
        (
            "has_function_privilege_name",
            BuiltinScalarFunction::HasFunctionPrivilege,
        ),
        (
            "has_function_privilege_id",
            BuiltinScalarFunction::HasFunctionPrivilege,
        ),
        (
            "has_schema_privilege",
            BuiltinScalarFunction::HasSchemaPrivilege,
        ),
        (
            "has_schema_privilege_name_name",
            BuiltinScalarFunction::HasSchemaPrivilege,
        ),
        (
            "has_schema_privilege_name_id",
            BuiltinScalarFunction::HasSchemaPrivilege,
        ),
        (
            "has_schema_privilege_id_name",
            BuiltinScalarFunction::HasSchemaPrivilege,
        ),
        (
            "has_schema_privilege_id_id",
            BuiltinScalarFunction::HasSchemaPrivilege,
        ),
        (
            "has_schema_privilege_name",
            BuiltinScalarFunction::HasSchemaPrivilege,
        ),
        (
            "has_schema_privilege_id",
            BuiltinScalarFunction::HasSchemaPrivilege,
        ),
        (
            "has_type_privilege",
            BuiltinScalarFunction::HasTypePrivilege,
        ),
        (
            "has_type_privilege_name_name",
            BuiltinScalarFunction::HasTypePrivilege,
        ),
        (
            "has_type_privilege_name_id",
            BuiltinScalarFunction::HasTypePrivilege,
        ),
        (
            "has_type_privilege_id_name",
            BuiltinScalarFunction::HasTypePrivilege,
        ),
        (
            "has_type_privilege_id_id",
            BuiltinScalarFunction::HasTypePrivilege,
        ),
        (
            "has_type_privilege_name",
            BuiltinScalarFunction::HasTypePrivilege,
        ),
        (
            "has_type_privilege_id",
            BuiltinScalarFunction::HasTypePrivilege,
        ),
        (
            "has_table_privilege",
            BuiltinScalarFunction::HasTablePrivilege,
        ),
        (
            "has_table_privilege_name_name",
            BuiltinScalarFunction::HasTablePrivilege,
        ),
        (
            "has_table_privilege_name_id",
            BuiltinScalarFunction::HasTablePrivilege,
        ),
        (
            "has_table_privilege_id_name",
            BuiltinScalarFunction::HasTablePrivilege,
        ),
        (
            "has_table_privilege_id_id",
            BuiltinScalarFunction::HasTablePrivilege,
        ),
        (
            "has_table_privilege_name",
            BuiltinScalarFunction::HasTablePrivilege,
        ),
        (
            "has_table_privilege_id",
            BuiltinScalarFunction::HasTablePrivilege,
        ),
        (
            "row_security_active",
            BuiltinScalarFunction::RowSecurityActive,
        ),
        (
            "has_sequence_privilege",
            BuiltinScalarFunction::HasSequencePrivilege,
        ),
        (
            "has_sequence_privilege_name_name",
            BuiltinScalarFunction::HasSequencePrivilege,
        ),
        (
            "has_sequence_privilege_name_id",
            BuiltinScalarFunction::HasSequencePrivilege,
        ),
        (
            "has_sequence_privilege_id_name",
            BuiltinScalarFunction::HasSequencePrivilege,
        ),
        (
            "has_sequence_privilege_id_id",
            BuiltinScalarFunction::HasSequencePrivilege,
        ),
        (
            "has_sequence_privilege_name",
            BuiltinScalarFunction::HasSequencePrivilege,
        ),
        (
            "has_sequence_privilege_id",
            BuiltinScalarFunction::HasSequencePrivilege,
        ),
        ("pg_has_role", BuiltinScalarFunction::PgHasRole),
        ("pg_has_role_name_name", BuiltinScalarFunction::PgHasRole),
        ("pg_has_role_name_id", BuiltinScalarFunction::PgHasRole),
        ("pg_has_role_id_name", BuiltinScalarFunction::PgHasRole),
        ("pg_has_role_id_id", BuiltinScalarFunction::PgHasRole),
        ("pg_has_role_name", BuiltinScalarFunction::PgHasRole),
        ("pg_has_role_id", BuiltinScalarFunction::PgHasRole),
        (
            "has_column_privilege",
            BuiltinScalarFunction::HasColumnPrivilege,
        ),
        (
            "has_column_privilege_name_name_name",
            BuiltinScalarFunction::HasColumnPrivilege,
        ),
        (
            "has_column_privilege_name_name_attnum",
            BuiltinScalarFunction::HasColumnPrivilege,
        ),
        (
            "has_column_privilege_name_id_name",
            BuiltinScalarFunction::HasColumnPrivilege,
        ),
        (
            "has_column_privilege_name_id_attnum",
            BuiltinScalarFunction::HasColumnPrivilege,
        ),
        (
            "has_column_privilege_id_name_name",
            BuiltinScalarFunction::HasColumnPrivilege,
        ),
        (
            "has_column_privilege_id_name_attnum",
            BuiltinScalarFunction::HasColumnPrivilege,
        ),
        (
            "has_column_privilege_id_id_name",
            BuiltinScalarFunction::HasColumnPrivilege,
        ),
        (
            "has_column_privilege_id_id_attnum",
            BuiltinScalarFunction::HasColumnPrivilege,
        ),
        (
            "has_column_privilege_name_name",
            BuiltinScalarFunction::HasColumnPrivilege,
        ),
        (
            "has_column_privilege_name_attnum",
            BuiltinScalarFunction::HasColumnPrivilege,
        ),
        (
            "has_column_privilege_id_name",
            BuiltinScalarFunction::HasColumnPrivilege,
        ),
        (
            "has_column_privilege_id_attnum",
            BuiltinScalarFunction::HasColumnPrivilege,
        ),
        (
            "has_any_column_privilege",
            BuiltinScalarFunction::HasAnyColumnPrivilege,
        ),
        (
            "has_any_column_privilege_name_name",
            BuiltinScalarFunction::HasAnyColumnPrivilege,
        ),
        (
            "has_any_column_privilege_name_id",
            BuiltinScalarFunction::HasAnyColumnPrivilege,
        ),
        (
            "has_any_column_privilege_id_name",
            BuiltinScalarFunction::HasAnyColumnPrivilege,
        ),
        (
            "has_any_column_privilege_id_id",
            BuiltinScalarFunction::HasAnyColumnPrivilege,
        ),
        (
            "has_any_column_privilege_name",
            BuiltinScalarFunction::HasAnyColumnPrivilege,
        ),
        (
            "has_any_column_privilege_id",
            BuiltinScalarFunction::HasAnyColumnPrivilege,
        ),
        (
            "has_largeobject_privilege",
            BuiltinScalarFunction::HasLargeObjectPrivilege,
        ),
        (
            "has_largeobject_privilege_name_id",
            BuiltinScalarFunction::HasLargeObjectPrivilege,
        ),
        (
            "has_largeobject_privilege_id",
            BuiltinScalarFunction::HasLargeObjectPrivilege,
        ),
        (
            "has_largeobject_privilege_id_id",
            BuiltinScalarFunction::HasLargeObjectPrivilege,
        ),
        (
            "pg_current_logfile",
            BuiltinScalarFunction::PgCurrentLogfile,
        ),
        (
            "pg_current_logfile_1arg",
            BuiltinScalarFunction::PgCurrentLogfile,
        ),
        ("pg_read_file_off_len", BuiltinScalarFunction::PgReadFile),
        (
            "pg_read_file_off_len_missing",
            BuiltinScalarFunction::PgReadFile,
        ),
        ("pg_read_file_all", BuiltinScalarFunction::PgReadFile),
        (
            "pg_read_file_all_missing",
            BuiltinScalarFunction::PgReadFile,
        ),
        (
            "pg_read_binary_file_off_len",
            BuiltinScalarFunction::PgReadBinaryFile,
        ),
        (
            "pg_read_binary_file_off_len_missing",
            BuiltinScalarFunction::PgReadBinaryFile,
        ),
        (
            "pg_read_binary_file_all",
            BuiltinScalarFunction::PgReadBinaryFile,
        ),
        (
            "pg_read_binary_file_all_missing",
            BuiltinScalarFunction::PgReadBinaryFile,
        ),
        ("pg_stat_file", BuiltinScalarFunction::PgStatFile),
        ("pg_stat_file_1arg", BuiltinScalarFunction::PgStatFile),
        ("pg_walfile_name", BuiltinScalarFunction::PgWalfileName),
        (
            "pg_walfile_name_offset",
            BuiltinScalarFunction::PgWalfileNameOffset,
        ),
        (
            "pg_split_walfile_name",
            BuiltinScalarFunction::PgSplitWalfileName,
        ),
        ("pg_control_system", BuiltinScalarFunction::PgControlSystem),
        (
            "pg_control_checkpoint",
            BuiltinScalarFunction::PgControlCheckpoint,
        ),
        (
            "pg_control_recovery",
            BuiltinScalarFunction::PgControlRecovery,
        ),
        ("pg_control_init", BuiltinScalarFunction::PgControlInit),
        (
            "pg_replication_origin_create",
            BuiltinScalarFunction::PgReplicationOriginCreate,
        ),
        (
            "gist_translate_cmptype_common",
            BuiltinScalarFunction::GistTranslateCmpTypeCommon,
        ),
        (
            "test_canonicalize_path",
            BuiltinScalarFunction::TestCanonicalizePath,
        ),
        ("test_relpath", BuiltinScalarFunction::TestRelpath),
        (
            "pg_stat_get_checkpointer_num_timed",
            BuiltinScalarFunction::PgStatGetCheckpointerNumTimed,
        ),
        (
            "pg_stat_get_checkpointer_num_requested",
            BuiltinScalarFunction::PgStatGetCheckpointerNumRequested,
        ),
        (
            "pg_stat_get_checkpointer_num_performed",
            BuiltinScalarFunction::PgStatGetCheckpointerNumPerformed,
        ),
        (
            "pg_stat_get_checkpointer_buffers_written",
            BuiltinScalarFunction::PgStatGetCheckpointerBuffersWritten,
        ),
        (
            "pg_stat_get_checkpointer_slru_written",
            BuiltinScalarFunction::PgStatGetCheckpointerSlruWritten,
        ),
        (
            "pg_stat_get_checkpointer_write_time",
            BuiltinScalarFunction::PgStatGetCheckpointerWriteTime,
        ),
        (
            "pg_stat_get_checkpointer_sync_time",
            BuiltinScalarFunction::PgStatGetCheckpointerSyncTime,
        ),
        (
            "pg_stat_get_checkpointer_stat_reset_time",
            BuiltinScalarFunction::PgStatGetCheckpointerStatResetTime,
        ),
        (
            "pg_stat_force_next_flush",
            BuiltinScalarFunction::PgStatForceNextFlush,
        ),
        (
            "pg_stat_get_snapshot_timestamp",
            BuiltinScalarFunction::PgStatGetSnapshotTimestamp,
        ),
        (
            "pg_stat_clear_snapshot",
            BuiltinScalarFunction::PgStatClearSnapshot,
        ),
        (
            "pg_stat_get_backend_pid",
            BuiltinScalarFunction::PgStatGetBackendPid,
        ),
        (
            "pg_stat_get_backend_wal",
            BuiltinScalarFunction::PgStatGetBackendWal,
        ),
        ("pg_stat_reset", BuiltinScalarFunction::PgStatReset),
        (
            "pg_stat_reset_shared",
            BuiltinScalarFunction::PgStatResetShared,
        ),
        (
            "pg_stat_reset_single_table_counters",
            BuiltinScalarFunction::PgStatResetSingleTableCounters,
        ),
        (
            "pg_stat_reset_single_function_counters",
            BuiltinScalarFunction::PgStatResetSingleFunctionCounters,
        ),
        (
            "pg_stat_reset_backend_stats",
            BuiltinScalarFunction::PgStatResetBackendStats,
        ),
        ("pg_stat_reset_slru", BuiltinScalarFunction::PgStatResetSlru),
        (
            "pg_stat_reset_replication_slot",
            BuiltinScalarFunction::PgStatResetReplicationSlot,
        ),
        (
            "pg_stat_reset_subscription_stats",
            BuiltinScalarFunction::PgStatResetSubscriptionStats,
        ),
        (
            "pg_stat_get_replication_slot",
            BuiltinScalarFunction::PgStatGetReplicationSlot,
        ),
        (
            "pg_stat_get_subscription_stats",
            BuiltinScalarFunction::PgStatGetSubscriptionStats,
        ),
        ("shobj_description", BuiltinScalarFunction::ShobjDescription),
        ("pg_stat_have_stats", BuiltinScalarFunction::PgStatHaveStats),
        (
            "pg_stat_get_numscans",
            BuiltinScalarFunction::PgStatGetNumscans,
        ),
        (
            "pg_stat_get_lastscan",
            BuiltinScalarFunction::PgStatGetLastscan,
        ),
        (
            "pg_stat_get_tuples_returned",
            BuiltinScalarFunction::PgStatGetTuplesReturned,
        ),
        (
            "pg_stat_get_tuples_fetched",
            BuiltinScalarFunction::PgStatGetTuplesFetched,
        ),
        (
            "pg_stat_get_tuples_inserted",
            BuiltinScalarFunction::PgStatGetTuplesInserted,
        ),
        (
            "pg_stat_get_tuples_updated",
            BuiltinScalarFunction::PgStatGetTuplesUpdated,
        ),
        (
            "pg_stat_get_tuples_hot_updated",
            BuiltinScalarFunction::PgStatGetTuplesHotUpdated,
        ),
        (
            "pg_stat_get_tuples_deleted",
            BuiltinScalarFunction::PgStatGetTuplesDeleted,
        ),
        (
            "pg_stat_get_live_tuples",
            BuiltinScalarFunction::PgStatGetLiveTuples,
        ),
        (
            "pg_stat_get_dead_tuples",
            BuiltinScalarFunction::PgStatGetDeadTuples,
        ),
        (
            "pg_stat_get_blocks_fetched",
            BuiltinScalarFunction::PgStatGetBlocksFetched,
        ),
        (
            "pg_stat_get_blocks_hit",
            BuiltinScalarFunction::PgStatGetBlocksHit,
        ),
        (
            "pg_stat_get_xact_numscans",
            BuiltinScalarFunction::PgStatGetXactNumscans,
        ),
        (
            "pg_stat_get_xact_tuples_returned",
            BuiltinScalarFunction::PgStatGetXactTuplesReturned,
        ),
        (
            "pg_stat_get_xact_tuples_fetched",
            BuiltinScalarFunction::PgStatGetXactTuplesFetched,
        ),
        (
            "pg_stat_get_xact_tuples_inserted",
            BuiltinScalarFunction::PgStatGetXactTuplesInserted,
        ),
        (
            "pg_stat_get_xact_tuples_updated",
            BuiltinScalarFunction::PgStatGetXactTuplesUpdated,
        ),
        (
            "pg_stat_get_xact_tuples_deleted",
            BuiltinScalarFunction::PgStatGetXactTuplesDeleted,
        ),
        (
            "pg_stat_get_function_calls",
            BuiltinScalarFunction::PgStatGetFunctionCalls,
        ),
        (
            "pg_stat_get_function_total_time",
            BuiltinScalarFunction::PgStatGetFunctionTotalTime,
        ),
        (
            "pg_stat_get_function_self_time",
            BuiltinScalarFunction::PgStatGetFunctionSelfTime,
        ),
        (
            "pg_stat_get_xact_function_calls",
            BuiltinScalarFunction::PgStatGetXactFunctionCalls,
        ),
        (
            "pg_stat_get_xact_function_total_time",
            BuiltinScalarFunction::PgStatGetXactFunctionTotalTime,
        ),
        (
            "pg_stat_get_xact_function_self_time",
            BuiltinScalarFunction::PgStatGetXactFunctionSelfTime,
        ),
        (
            "pg_restore_relation_stats",
            BuiltinScalarFunction::PgRestoreRelationStats,
        ),
        (
            "pg_clear_relation_stats",
            BuiltinScalarFunction::PgClearRelationStats,
        ),
        (
            "pg_restore_attribute_stats",
            BuiltinScalarFunction::PgRestoreAttributeStats,
        ),
        (
            "pg_clear_attribute_stats",
            BuiltinScalarFunction::PgClearAttributeStats,
        ),
        ("to_json", BuiltinScalarFunction::ToJson),
        ("to_jsonb", BuiltinScalarFunction::ToJsonb),
        ("to_tsvector", BuiltinScalarFunction::ToTsVector),
        ("to_tsvector_byid", BuiltinScalarFunction::ToTsVector),
        ("json_to_tsvector", BuiltinScalarFunction::JsonToTsVector),
        (
            "json_to_tsvector_byid",
            BuiltinScalarFunction::JsonToTsVector,
        ),
        (
            "jsonb_string_to_tsvector",
            BuiltinScalarFunction::ToTsVector,
        ),
        (
            "jsonb_string_to_tsvector_byid",
            BuiltinScalarFunction::ToTsVector,
        ),
        ("to_tsquery", BuiltinScalarFunction::ToTsQuery),
        ("to_tsquery_byid", BuiltinScalarFunction::ToTsQuery),
        ("plainto_tsquery", BuiltinScalarFunction::PlainToTsQuery),
        (
            "plainto_tsquery_byid",
            BuiltinScalarFunction::PlainToTsQuery,
        ),
        ("phraseto_tsquery", BuiltinScalarFunction::PhraseToTsQuery),
        (
            "phraseto_tsquery_byid",
            BuiltinScalarFunction::PhraseToTsQuery,
        ),
        (
            "websearch_to_tsquery",
            BuiltinScalarFunction::WebSearchToTsQuery,
        ),
        (
            "websearch_to_tsquery_byid",
            BuiltinScalarFunction::WebSearchToTsQuery,
        ),
        ("ts_lexize", BuiltinScalarFunction::TsLexize),
        ("ts_headline", BuiltinScalarFunction::TsHeadline),
        ("tsvectorin", BuiltinScalarFunction::TsVectorIn),
        ("tsvectorout", BuiltinScalarFunction::TsVectorOut),
        ("tsqueryin", BuiltinScalarFunction::TsQueryIn),
        ("tsqueryout", BuiltinScalarFunction::TsQueryOut),
        ("tsquery_phrase", BuiltinScalarFunction::TsQueryPhrase),
        (
            "tsquery_phrase_distance",
            BuiltinScalarFunction::TsQueryPhrase,
        ),
        ("ts_rewrite", BuiltinScalarFunction::TsRewrite),
        ("tsquery_numnode", BuiltinScalarFunction::TsQueryNumnode),
        ("numnode", BuiltinScalarFunction::TsQueryNumnode),
        ("tsq_mcontains", BuiltinScalarFunction::TsQueryContains),
        ("tsq_mcontained", BuiltinScalarFunction::TsQueryContainedBy),
        ("tsvector_strip", BuiltinScalarFunction::TsVectorStrip),
        ("strip", BuiltinScalarFunction::TsVectorStrip),
        ("tsvector_delete_str", BuiltinScalarFunction::TsVectorDelete),
        ("tsvector_delete_arr", BuiltinScalarFunction::TsVectorDelete),
        ("ts_delete", BuiltinScalarFunction::TsVectorDelete),
        ("tsvector_to_array", BuiltinScalarFunction::TsVectorToArray),
        ("array_to_tsvector", BuiltinScalarFunction::ArrayToTsVector),
        (
            "tsvector_setweight",
            BuiltinScalarFunction::TsVectorSetWeight,
        ),
        (
            "tsvector_setweight_by_filter",
            BuiltinScalarFunction::TsVectorSetWeight,
        ),
        ("setweight", BuiltinScalarFunction::TsVectorSetWeight),
        ("tsvector_filter", BuiltinScalarFunction::TsVectorFilter),
        ("ts_filter", BuiltinScalarFunction::TsVectorFilter),
        ("ts_rank", BuiltinScalarFunction::TsRank),
        ("ts_rank_wttf", BuiltinScalarFunction::TsRank),
        ("ts_rank_wtt", BuiltinScalarFunction::TsRank),
        ("ts_rank_ttf", BuiltinScalarFunction::TsRank),
        ("ts_rank_tt", BuiltinScalarFunction::TsRank),
        ("ts_rank_cd", BuiltinScalarFunction::TsRankCd),
        ("ts_rankcd_wttf", BuiltinScalarFunction::TsRankCd),
        ("ts_rankcd_wtt", BuiltinScalarFunction::TsRankCd),
        ("ts_rankcd_ttf", BuiltinScalarFunction::TsRankCd),
        ("ts_rankcd_tt", BuiltinScalarFunction::TsRankCd),
        ("tsq_mcontains", BuiltinScalarFunction::TsQueryContains),
        ("tsq_mcontained", BuiltinScalarFunction::TsQueryContains),
        ("ts_rewrite", BuiltinScalarFunction::TsRewrite),
        ("tsquery_rewrite", BuiltinScalarFunction::TsRewrite),
        ("tsquery_rewrite_query", BuiltinScalarFunction::TsRewrite),
        ("ts_headline", BuiltinScalarFunction::TsHeadline),
        ("ts_headline_byid", BuiltinScalarFunction::TsHeadline),
        ("ts_headline_opt", BuiltinScalarFunction::TsHeadline),
        ("ts_headline_byid_opt", BuiltinScalarFunction::TsHeadline),
        ("ts_headline_jsonb", BuiltinScalarFunction::TsHeadline),
        ("ts_headline_jsonb_byid", BuiltinScalarFunction::TsHeadline),
        ("ts_headline_jsonb_opt", BuiltinScalarFunction::TsHeadline),
        (
            "ts_headline_jsonb_byid_opt",
            BuiltinScalarFunction::TsHeadline,
        ),
        ("ts_headline_json", BuiltinScalarFunction::TsHeadline),
        ("ts_headline_json_byid", BuiltinScalarFunction::TsHeadline),
        ("ts_headline_json_opt", BuiltinScalarFunction::TsHeadline),
        (
            "ts_headline_json_byid_opt",
            BuiltinScalarFunction::TsHeadline,
        ),
        ("array_to_json", BuiltinScalarFunction::ArrayToJson),
        ("row_to_json", BuiltinScalarFunction::RowToJson),
        ("row_to_json_pretty", BuiltinScalarFunction::RowToJson),
        ("json_build_array", BuiltinScalarFunction::JsonBuildArray),
        ("json_build_object", BuiltinScalarFunction::JsonBuildObject),
        ("json_object", BuiltinScalarFunction::JsonObject),
        (
            "json_populate_record",
            BuiltinScalarFunction::JsonPopulateRecord,
        ),
        (
            "json_populate_record_valid",
            BuiltinScalarFunction::JsonPopulateRecordValid,
        ),
        ("json_to_record", BuiltinScalarFunction::JsonToRecord),
        ("json_strip_nulls", BuiltinScalarFunction::JsonStripNulls),
        ("json_typeof", BuiltinScalarFunction::JsonTypeof),
        ("json_array_length", BuiltinScalarFunction::JsonArrayLength),
        ("json_extract_path", BuiltinScalarFunction::JsonExtractPath),
        (
            "json_extract_path_text",
            BuiltinScalarFunction::JsonExtractPathText,
        ),
        // :HACK: Keep synthetic OID mapping for plain SQL/JSON query function
        // spellings while they are lowered through legacy scalar builtins.
        ("json_exists", BuiltinScalarFunction::JsonExists),
        ("json_value", BuiltinScalarFunction::JsonValue),
        ("json_query", BuiltinScalarFunction::JsonQuery),
        ("jsonb_typeof", BuiltinScalarFunction::JsonbTypeof),
        (
            "jsonb_array_length",
            BuiltinScalarFunction::JsonbArrayLength,
        ),
        (
            "jsonb_extract_path",
            BuiltinScalarFunction::JsonbExtractPath,
        ),
        (
            "jsonb_extract_path_text",
            BuiltinScalarFunction::JsonbExtractPathText,
        ),
        ("jsonb_object", BuiltinScalarFunction::JsonbObject),
        ("jsonb_object_two_arg", BuiltinScalarFunction::JsonbObject),
        ("jsonb_to_tsvector", BuiltinScalarFunction::JsonbToTsVector),
        (
            "jsonb_to_tsvector_byid",
            BuiltinScalarFunction::JsonbToTsVector,
        ),
        (
            "jsonb_populate_record",
            BuiltinScalarFunction::JsonbPopulateRecord,
        ),
        (
            "jsonb_populate_record_valid",
            BuiltinScalarFunction::JsonbPopulateRecordValid,
        ),
        ("jsonb_to_record", BuiltinScalarFunction::JsonbToRecord),
        ("jsonb_strip_nulls", BuiltinScalarFunction::JsonbStripNulls),
        ("jsonb_pretty", BuiltinScalarFunction::JsonbPretty),
        ("jsonb_build_array", BuiltinScalarFunction::JsonbBuildArray),
        (
            "jsonb_build_object",
            BuiltinScalarFunction::JsonbBuildObject,
        ),
        ("jsonb_concat", BuiltinScalarFunction::JsonbConcat),
        ("jsonb_contains", BuiltinScalarFunction::JsonbContains),
        ("jsonb_contained", BuiltinScalarFunction::JsonbContained),
        ("jsonb_delete", BuiltinScalarFunction::JsonbDelete),
        ("jsonb_delete_path", BuiltinScalarFunction::JsonbDeletePath),
        ("jsonb_exists", BuiltinScalarFunction::JsonbExists),
        ("jsonb_exists_any", BuiltinScalarFunction::JsonbExistsAny),
        ("jsonb_exists_all", BuiltinScalarFunction::JsonbExistsAll),
        ("jsonb_set", BuiltinScalarFunction::JsonbSet),
        ("jsonb_set_lax", BuiltinScalarFunction::JsonbSetLax),
        ("jsonb_insert", BuiltinScalarFunction::JsonbInsert),
        ("jsonb_path_exists", BuiltinScalarFunction::JsonbPathExists),
        ("jsonb_path_match", BuiltinScalarFunction::JsonbPathMatch),
        (
            "jsonb_path_query_array",
            BuiltinScalarFunction::JsonbPathQueryArray,
        ),
        (
            "jsonb_path_query_first",
            BuiltinScalarFunction::JsonbPathQueryFirst,
        ),
        ("btrim", BuiltinScalarFunction::BTrim),
        ("ltrim", BuiltinScalarFunction::LTrim),
        ("rtrim", BuiltinScalarFunction::RTrim),
        ("regexp_match", BuiltinScalarFunction::RegexpMatch),
        ("regexp_like", BuiltinScalarFunction::RegexpLike),
        ("regexp_replace", BuiltinScalarFunction::RegexpReplace),
        ("regexp_count", BuiltinScalarFunction::RegexpCount),
        ("regexp_instr", BuiltinScalarFunction::RegexpInstr),
        ("regexp_substr", BuiltinScalarFunction::RegexpSubstr),
        (
            "regexp_split_to_array",
            BuiltinScalarFunction::RegexpSplitToArray,
        ),
        ("substring_similar", BuiltinScalarFunction::SimilarSubstring),
        ("initcap", BuiltinScalarFunction::Initcap),
        ("casefold", BuiltinScalarFunction::Casefold),
        ("textcat", BuiltinScalarFunction::TextCat),
        ("concat", BuiltinScalarFunction::Concat),
        ("concat_ws", BuiltinScalarFunction::ConcatWs),
        ("format", BuiltinScalarFunction::Format),
        ("left", BuiltinScalarFunction::Left),
        ("right", BuiltinScalarFunction::Right),
        ("lpad", BuiltinScalarFunction::LPad),
        ("rpad", BuiltinScalarFunction::RPad),
        ("repeat", BuiltinScalarFunction::Repeat),
        ("length", BuiltinScalarFunction::Length),
        ("octet_length", BuiltinScalarFunction::OctetLength),
        ("bit_length", BuiltinScalarFunction::BitLength),
        ("array_ndims", BuiltinScalarFunction::ArrayNdims),
        ("array_dims", BuiltinScalarFunction::ArrayDims),
        ("array_lower", BuiltinScalarFunction::ArrayLower),
        ("array_upper", BuiltinScalarFunction::ArrayUpper),
        ("array_fill", BuiltinScalarFunction::ArrayFill),
        ("array_in", BuiltinScalarFunction::ArrayIn),
        ("anyrange_in", BuiltinScalarFunction::AnyRangeIn),
        ("array_larger", BuiltinScalarFunction::ArrayLarger),
        ("string_to_array", BuiltinScalarFunction::StringToArray),
        ("array_to_string", BuiltinScalarFunction::ArrayToString),
        ("array_length", BuiltinScalarFunction::ArrayLength),
        ("cardinality", BuiltinScalarFunction::Cardinality),
        ("array_append", BuiltinScalarFunction::ArrayAppend),
        ("array_prepend", BuiltinScalarFunction::ArrayPrepend),
        ("array_cat", BuiltinScalarFunction::ArrayCat),
        ("array_position", BuiltinScalarFunction::ArrayPosition),
        ("array_positions", BuiltinScalarFunction::ArrayPositions),
        ("array_remove", BuiltinScalarFunction::ArrayRemove),
        ("array_replace", BuiltinScalarFunction::ArrayReplace),
        ("trim_array", BuiltinScalarFunction::TrimArray),
        ("array_shuffle", BuiltinScalarFunction::ArrayShuffle),
        ("array_sample", BuiltinScalarFunction::ArraySample),
        ("array_reverse", BuiltinScalarFunction::ArrayReverse),
        ("array_sort", BuiltinScalarFunction::ArraySort),
        ("enum_first", BuiltinScalarFunction::EnumFirst),
        ("enum_last", BuiltinScalarFunction::EnumLast),
        ("enum_range", BuiltinScalarFunction::EnumRange),
        ("enum_range_bounds", BuiltinScalarFunction::EnumRange),
        ("lower", BuiltinScalarFunction::Lower),
        ("upper", BuiltinScalarFunction::Upper),
        ("casefold", BuiltinScalarFunction::Casefold),
        ("unistr", BuiltinScalarFunction::Unistr),
        ("strpos", BuiltinScalarFunction::Strpos),
        ("position", BuiltinScalarFunction::Position),
        ("substring", BuiltinScalarFunction::Substring),
        ("substr", BuiltinScalarFunction::Substring),
        ("similar_substring", BuiltinScalarFunction::SimilarSubstring),
        ("overlay", BuiltinScalarFunction::Overlay),
        ("replace", BuiltinScalarFunction::Replace),
        ("split_part", BuiltinScalarFunction::SplitPart),
        ("translate", BuiltinScalarFunction::Translate),
        ("host", BuiltinScalarFunction::NetworkHost),
        ("abbrev", BuiltinScalarFunction::NetworkAbbrev),
        ("broadcast", BuiltinScalarFunction::NetworkBroadcast),
        ("network", BuiltinScalarFunction::NetworkNetwork),
        ("masklen", BuiltinScalarFunction::NetworkMasklen),
        ("family", BuiltinScalarFunction::NetworkFamily),
        ("netmask", BuiltinScalarFunction::NetworkNetmask),
        ("hostmask", BuiltinScalarFunction::NetworkHostmask),
        ("set_masklen", BuiltinScalarFunction::NetworkSetMasklen),
        ("inet_same_family", BuiltinScalarFunction::NetworkSameFamily),
        ("inet_merge", BuiltinScalarFunction::NetworkMerge),
        ("network_sub", BuiltinScalarFunction::NetworkSubnet),
        ("network_subeq", BuiltinScalarFunction::NetworkSubnetEq),
        ("network_sup", BuiltinScalarFunction::NetworkSupernet),
        ("network_supeq", BuiltinScalarFunction::NetworkSupernetEq),
        ("network_overlap", BuiltinScalarFunction::NetworkOverlap),
        ("text_to_regclass", BuiltinScalarFunction::TextToRegClass),
        ("to_regproc", BuiltinScalarFunction::ToRegProc),
        ("to_regprocedure", BuiltinScalarFunction::ToRegProcedure),
        ("to_regoper", BuiltinScalarFunction::ToRegOper),
        ("to_regoperator", BuiltinScalarFunction::ToRegOperator),
        ("to_regclass", BuiltinScalarFunction::ToRegClass),
        ("to_regtype", BuiltinScalarFunction::ToRegType),
        ("to_regtypemod", BuiltinScalarFunction::ToRegTypeMod),
        ("to_regrole", BuiltinScalarFunction::ToRegRole),
        ("to_regnamespace", BuiltinScalarFunction::ToRegNamespace),
        ("to_regcollation", BuiltinScalarFunction::ToRegCollation),
        ("format_type", BuiltinScalarFunction::FormatType),
        (
            "has_foreign_data_wrapper_privilege",
            BuiltinScalarFunction::HasForeignDataWrapperPrivilege,
        ),
        (
            "has_server_privilege",
            BuiltinScalarFunction::HasServerPrivilege,
        ),
        ("regproc_to_text", BuiltinScalarFunction::RegProcToText),
        ("regprocout", BuiltinScalarFunction::RegProcToText),
        ("regclass_to_text", BuiltinScalarFunction::RegClassToText),
        ("regclassout", BuiltinScalarFunction::RegClassToText),
        ("regtype_to_text", BuiltinScalarFunction::RegTypeToText),
        ("regtypeout", BuiltinScalarFunction::RegTypeToText),
        ("regoper_to_text", BuiltinScalarFunction::RegOperToText),
        ("regoperout", BuiltinScalarFunction::RegOperToText),
        (
            "regoperator_to_text",
            BuiltinScalarFunction::RegOperatorToText,
        ),
        ("regoperatorout", BuiltinScalarFunction::RegOperatorToText),
        (
            "regprocedure_to_text",
            BuiltinScalarFunction::RegProcedureToText,
        ),
        ("regprocedureout", BuiltinScalarFunction::RegProcedureToText),
        (
            "regcollation_to_text",
            BuiltinScalarFunction::RegCollationToText,
        ),
        ("regcollationout", BuiltinScalarFunction::RegCollationToText),
        ("regrole_to_text", BuiltinScalarFunction::RegRoleToText),
        ("regroleout", BuiltinScalarFunction::RegRoleToText),
        ("ascii", BuiltinScalarFunction::Ascii),
        ("chr", BuiltinScalarFunction::Chr),
        ("quote_ident", BuiltinScalarFunction::QuoteIdent),
        ("quote_literal", BuiltinScalarFunction::QuoteLiteral),
        ("quote_nullable", BuiltinScalarFunction::QuoteNullable),
        ("bpchar_to_text", BuiltinScalarFunction::BpcharToText),
        ("bpchartotext", BuiltinScalarFunction::BpcharToText),
        ("trim_scale", BuiltinScalarFunction::TrimScale),
        ("scale", BuiltinScalarFunction::Scale),
        ("min_scale", BuiltinScalarFunction::MinScale),
        ("numeric_inc", BuiltinScalarFunction::NumericInc),
        ("int4smaller", BuiltinScalarFunction::Int4Smaller),
        ("int4mi", BuiltinScalarFunction::Int4Mi),
        ("int4mul", BuiltinScalarFunction::Int4Mul),
        ("int4pl", BuiltinScalarFunction::Int4Pl),
        ("int4_sum", BuiltinScalarFunction::Int4Sum),
        ("int8inc", BuiltinScalarFunction::Int8Inc),
        ("int8inc_any", BuiltinScalarFunction::Int8IncAny),
        ("int4_avg_accum", BuiltinScalarFunction::Int4AvgAccum),
        ("int8_avg", BuiltinScalarFunction::Int8Avg),
        ("factorial", BuiltinScalarFunction::Factorial),
        ("pg_lsn", BuiltinScalarFunction::PgLsn),
        ("div", BuiltinScalarFunction::Div),
        ("mod", BuiltinScalarFunction::Mod),
        ("width_bucket", BuiltinScalarFunction::WidthBucket),
        ("get_bit", BuiltinScalarFunction::GetBit),
        ("set_bit", BuiltinScalarFunction::SetBit),
        ("bit_count", BuiltinScalarFunction::BitCount),
        ("get_byte", BuiltinScalarFunction::GetByte),
        ("set_byte", BuiltinScalarFunction::SetByte),
        ("convert", BuiltinScalarFunction::Convert),
        ("pg_convert", BuiltinScalarFunction::Convert),
        ("convert_from", BuiltinScalarFunction::ConvertFrom),
        ("pg_convert_from", BuiltinScalarFunction::ConvertFrom),
        ("convert_to", BuiltinScalarFunction::ConvertTo),
        ("pg_convert_to", BuiltinScalarFunction::ConvertTo),
        ("md5", BuiltinScalarFunction::Md5),
        ("reverse", BuiltinScalarFunction::Reverse),
        ("starts_with", BuiltinScalarFunction::TextStartsWith),
        ("encode", BuiltinScalarFunction::Encode),
        ("decode", BuiltinScalarFunction::Decode),
        ("sha224", BuiltinScalarFunction::Sha224),
        ("sha256", BuiltinScalarFunction::Sha256),
        ("sha384", BuiltinScalarFunction::Sha384),
        ("sha512", BuiltinScalarFunction::Sha512),
        ("crc32", BuiltinScalarFunction::Crc32),
        ("crc32c", BuiltinScalarFunction::Crc32c),
        ("to_bin", BuiltinScalarFunction::ToBin),
        ("to_oct", BuiltinScalarFunction::ToOct),
        ("to_hex", BuiltinScalarFunction::ToHex),
        ("to_char", BuiltinScalarFunction::ToChar),
        ("to_date", BuiltinScalarFunction::ToDate),
        ("to_number", BuiltinScalarFunction::ToNumber),
        ("to_timestamp", BuiltinScalarFunction::ToTimestamp),
        ("abs", BuiltinScalarFunction::Abs),
        ("log", BuiltinScalarFunction::Log),
        ("dlog10", BuiltinScalarFunction::Log),
        ("numeric_log", BuiltinScalarFunction::Log),
        ("numeric_log10", BuiltinScalarFunction::Log),
        ("log10", BuiltinScalarFunction::Log10),
        ("gcd", BuiltinScalarFunction::Gcd),
        ("lcm", BuiltinScalarFunction::Lcm),
        ("greatest", BuiltinScalarFunction::Greatest),
        ("least", BuiltinScalarFunction::Least),
        ("trunc", BuiltinScalarFunction::Trunc),
        ("macaddr_eq", BuiltinScalarFunction::MacAddrEq),
        ("macaddr_ne", BuiltinScalarFunction::MacAddrNe),
        ("macaddr_lt", BuiltinScalarFunction::MacAddrLt),
        ("macaddr_le", BuiltinScalarFunction::MacAddrLe),
        ("macaddr_gt", BuiltinScalarFunction::MacAddrGt),
        ("macaddr_ge", BuiltinScalarFunction::MacAddrGe),
        ("macaddr_cmp", BuiltinScalarFunction::MacAddrCmp),
        ("macaddr_not", BuiltinScalarFunction::MacAddrNot),
        ("macaddr_and", BuiltinScalarFunction::MacAddrAnd),
        ("macaddr_or", BuiltinScalarFunction::MacAddrOr),
        ("macaddr_trunc", BuiltinScalarFunction::MacAddrTrunc),
        (
            "macaddrtomacaddr8",
            BuiltinScalarFunction::MacAddrToMacAddr8,
        ),
        ("macaddr8_eq", BuiltinScalarFunction::MacAddr8Eq),
        ("macaddr8_ne", BuiltinScalarFunction::MacAddr8Ne),
        ("macaddr8_lt", BuiltinScalarFunction::MacAddr8Lt),
        ("macaddr8_le", BuiltinScalarFunction::MacAddr8Le),
        ("macaddr8_gt", BuiltinScalarFunction::MacAddr8Gt),
        ("macaddr8_ge", BuiltinScalarFunction::MacAddr8Ge),
        ("macaddr8_cmp", BuiltinScalarFunction::MacAddr8Cmp),
        ("macaddr8_not", BuiltinScalarFunction::MacAddr8Not),
        ("macaddr8_and", BuiltinScalarFunction::MacAddr8And),
        ("macaddr8_or", BuiltinScalarFunction::MacAddr8Or),
        ("macaddr8_trunc", BuiltinScalarFunction::MacAddr8Trunc),
        (
            "macaddr8tomacaddr",
            BuiltinScalarFunction::MacAddr8ToMacAddr,
        ),
        ("macaddr8_set7bit", BuiltinScalarFunction::MacAddr8Set7Bit),
        ("hashmacaddr", BuiltinScalarFunction::HashMacAddr),
        (
            "hashmacaddrextended",
            BuiltinScalarFunction::HashMacAddrExtended,
        ),
        ("hashmacaddr8", BuiltinScalarFunction::HashMacAddr8),
        (
            "hashmacaddr8extended",
            BuiltinScalarFunction::HashMacAddr8Extended,
        ),
        ("round", BuiltinScalarFunction::Round),
        ("numeric_round", BuiltinScalarFunction::Round),
        ("ceil", BuiltinScalarFunction::Ceil),
        ("ceiling", BuiltinScalarFunction::Ceiling),
        ("floor", BuiltinScalarFunction::Floor),
        ("sign", BuiltinScalarFunction::Sign),
        ("sqrt", BuiltinScalarFunction::Sqrt),
        ("dsqrt", BuiltinScalarFunction::Sqrt),
        ("numeric_sqrt", BuiltinScalarFunction::Sqrt),
        ("pi", BuiltinScalarFunction::Pi),
        ("dpi", BuiltinScalarFunction::Pi),
        ("sin", BuiltinScalarFunction::Sin),
        ("dsin", BuiltinScalarFunction::Sin),
        ("cos", BuiltinScalarFunction::Cos),
        ("dcos", BuiltinScalarFunction::Cos),
        ("cbrt", BuiltinScalarFunction::Cbrt),
        ("dcbrt", BuiltinScalarFunction::Cbrt),
        ("power", BuiltinScalarFunction::Power),
        ("dpow", BuiltinScalarFunction::Power),
        ("numeric_power", BuiltinScalarFunction::Power),
        ("exp", BuiltinScalarFunction::Exp),
        ("dexp", BuiltinScalarFunction::Exp),
        ("numeric_exp", BuiltinScalarFunction::Exp),
        ("ln", BuiltinScalarFunction::Ln),
        ("numeric_ln", BuiltinScalarFunction::Ln),
        ("sinh", BuiltinScalarFunction::Sinh),
        ("cosh", BuiltinScalarFunction::Cosh),
        ("tanh", BuiltinScalarFunction::Tanh),
        ("asinh", BuiltinScalarFunction::Asinh),
        ("acosh", BuiltinScalarFunction::Acosh),
        ("atanh", BuiltinScalarFunction::Atanh),
        ("sind", BuiltinScalarFunction::Sind),
        ("cosd", BuiltinScalarFunction::Cosd),
        ("tand", BuiltinScalarFunction::Tand),
        ("cotd", BuiltinScalarFunction::Cotd),
        ("asind", BuiltinScalarFunction::Asind),
        ("acosd", BuiltinScalarFunction::Acosd),
        ("atand", BuiltinScalarFunction::Atand),
        ("atan2d", BuiltinScalarFunction::Atan2d),
        ("float4send", BuiltinScalarFunction::Float4Send),
        ("float8send", BuiltinScalarFunction::Float8Send),
        ("float8_accum", BuiltinScalarFunction::Float8Accum),
        ("float8_combine", BuiltinScalarFunction::Float8Combine),
        ("float8_regr_accum", BuiltinScalarFunction::Float8RegrAccum),
        (
            "float8_regr_combine",
            BuiltinScalarFunction::Float8RegrCombine,
        ),
        ("erf", BuiltinScalarFunction::Erf),
        ("erfc", BuiltinScalarFunction::Erfc),
        ("gamma", BuiltinScalarFunction::Gamma),
        ("lgamma", BuiltinScalarFunction::Lgamma),
        ("array_fill", BuiltinScalarFunction::ArrayFill),
        ("array_length", BuiltinScalarFunction::ArrayLength),
        ("array_lower", BuiltinScalarFunction::ArrayLower),
        ("array_upper", BuiltinScalarFunction::ArrayUpper),
        ("cardinality", BuiltinScalarFunction::Cardinality),
        ("array_ndims", BuiltinScalarFunction::ArrayNdims),
        ("array_dims", BuiltinScalarFunction::ArrayDims),
        ("array_append", BuiltinScalarFunction::ArrayAppend),
        ("array_prepend", BuiltinScalarFunction::ArrayPrepend),
        ("array_cat", BuiltinScalarFunction::ArrayCat),
        ("array_position", BuiltinScalarFunction::ArrayPosition),
        ("array_positions", BuiltinScalarFunction::ArrayPositions),
        ("array_remove", BuiltinScalarFunction::ArrayRemove),
        ("array_replace", BuiltinScalarFunction::ArrayReplace),
        ("trim_array", BuiltinScalarFunction::TrimArray),
        ("array_shuffle", BuiltinScalarFunction::ArrayShuffle),
        ("array_sample", BuiltinScalarFunction::ArraySample),
        ("array_reverse", BuiltinScalarFunction::ArrayReverse),
        ("array_sort", BuiltinScalarFunction::ArraySort),
        ("string_to_array", BuiltinScalarFunction::StringToArray),
        ("array_to_string", BuiltinScalarFunction::ArrayToString),
        ("booleq", BuiltinScalarFunction::BoolEq),
        ("boolne", BuiltinScalarFunction::BoolNe),
        ("booland_statefunc", BuiltinScalarFunction::BoolAndStateFunc),
        ("boolor_statefunc", BuiltinScalarFunction::BoolOrStateFunc),
        ("ts_match_vq", BuiltinScalarFunction::TsMatch),
        ("ts_match_qv", BuiltinScalarFunction::TsMatch),
        ("tsmatch", BuiltinScalarFunction::TsMatch),
        ("tsquery_and", BuiltinScalarFunction::TsQueryAnd),
        ("tsquery_or", BuiltinScalarFunction::TsQueryOr),
        ("tsquery_not", BuiltinScalarFunction::TsQueryNot),
        ("tsq_mcontains", BuiltinScalarFunction::TsQueryContains),
        ("tsq_mcontained", BuiltinScalarFunction::TsQueryContainedBy),
        ("tsvector_concat", BuiltinScalarFunction::TsVectorConcat),
        ("point", BuiltinScalarFunction::GeoPoint),
        ("construct_point", BuiltinScalarFunction::GeoPoint),
        ("circle_center", BuiltinScalarFunction::GeoPoint),
        ("lseg_center", BuiltinScalarFunction::GeoPoint),
        ("box_center", BuiltinScalarFunction::GeoPoint),
        ("poly_center", BuiltinScalarFunction::GeoPoint),
        ("box", BuiltinScalarFunction::GeoBox),
        ("points_box", BuiltinScalarFunction::GeoBox),
        ("point_box", BuiltinScalarFunction::GeoBox),
        ("poly_box", BuiltinScalarFunction::GeoBox),
        ("circle_box", BuiltinScalarFunction::GeoBox),
        ("line", BuiltinScalarFunction::GeoLine),
        ("lseg", BuiltinScalarFunction::GeoLseg),
        ("lseg_construct", BuiltinScalarFunction::GeoLseg),
        ("box_diagonal", BuiltinScalarFunction::GeoLseg),
        ("path", BuiltinScalarFunction::GeoPath),
        ("polygon", BuiltinScalarFunction::GeoPolygon),
        ("box_poly", BuiltinScalarFunction::GeoPolygon),
        ("path_poly", BuiltinScalarFunction::GeoPolygon),
        ("circle_poly", BuiltinScalarFunction::GeoPolygon),
        ("circle_poly_12", BuiltinScalarFunction::GeoPolygon),
        ("circle", BuiltinScalarFunction::GeoCircle),
        ("cr_circle", BuiltinScalarFunction::GeoCircle),
        ("poly_circle", BuiltinScalarFunction::GeoCircle),
        ("box_circle", BuiltinScalarFunction::GeoCircle),
        ("area", BuiltinScalarFunction::GeoArea),
        ("box_area", BuiltinScalarFunction::GeoArea),
        ("path_area", BuiltinScalarFunction::GeoArea),
        ("circle_area", BuiltinScalarFunction::GeoArea),
        ("center", BuiltinScalarFunction::GeoCenter),
        ("poly_center", BuiltinScalarFunction::GeoPolyCenter),
        ("poly_path", BuiltinScalarFunction::GeoPath),
        ("bound_box", BuiltinScalarFunction::GeoBoundBox),
        ("diagonal", BuiltinScalarFunction::GeoDiagonal),
        ("path_length", BuiltinScalarFunction::GeoLength),
        ("lseg_length", BuiltinScalarFunction::GeoLength),
        ("radius", BuiltinScalarFunction::GeoRadius),
        ("diameter", BuiltinScalarFunction::GeoDiameter),
        ("npoints", BuiltinScalarFunction::GeoNpoints),
        ("path_npoints", BuiltinScalarFunction::GeoNpoints),
        ("poly_npoints", BuiltinScalarFunction::GeoNpoints),
        ("pclose", BuiltinScalarFunction::GeoPclose),
        ("popen", BuiltinScalarFunction::GeoPopen),
        ("isopen", BuiltinScalarFunction::GeoIsOpen),
        ("isclosed", BuiltinScalarFunction::GeoIsClosed),
        ("slope", BuiltinScalarFunction::GeoSlope),
        ("isvertical", BuiltinScalarFunction::GeoIsVertical),
        ("point_vert", BuiltinScalarFunction::GeoIsVertical),
        ("lseg_vertical", BuiltinScalarFunction::GeoIsVertical),
        ("line_vertical", BuiltinScalarFunction::GeoIsVertical),
        ("ishorizontal", BuiltinScalarFunction::GeoIsHorizontal),
        ("point_horiz", BuiltinScalarFunction::GeoIsHorizontal),
        ("lseg_horizontal", BuiltinScalarFunction::GeoIsHorizontal),
        ("line_horizontal", BuiltinScalarFunction::GeoIsHorizontal),
        ("height", BuiltinScalarFunction::GeoHeight),
        ("width", BuiltinScalarFunction::GeoWidth),
        ("geoeq", BuiltinScalarFunction::GeoEq),
        ("geone", BuiltinScalarFunction::GeoNe),
        ("geolt", BuiltinScalarFunction::GeoLt),
        ("geole", BuiltinScalarFunction::GeoLe),
        ("geogt", BuiltinScalarFunction::GeoGt),
        ("geoge", BuiltinScalarFunction::GeoGe),
        ("box_same", BuiltinScalarFunction::GeoSame),
        ("same", BuiltinScalarFunction::GeoSame),
        ("dist_pb", BuiltinScalarFunction::GeoDistance),
        ("box_distance", BuiltinScalarFunction::GeoDistance),
        ("distance", BuiltinScalarFunction::GeoDistance),
        ("close_pt", BuiltinScalarFunction::GeoClosestPoint),
        ("interpt", BuiltinScalarFunction::GeoIntersection),
        ("interpt_pp", BuiltinScalarFunction::GeoIntersection),
        ("path_inter", BuiltinScalarFunction::GeoIntersects),
        ("lseg_intersect", BuiltinScalarFunction::GeoIntersects),
        ("line_intersect", BuiltinScalarFunction::GeoIntersects),
        ("inter_sl", BuiltinScalarFunction::GeoIntersects),
        ("inter_lb", BuiltinScalarFunction::GeoIntersects),
        ("inter_sb", BuiltinScalarFunction::GeoIntersects),
        ("intersects", BuiltinScalarFunction::GeoIntersects),
        ("parallel", BuiltinScalarFunction::GeoParallel),
        ("perpendicular", BuiltinScalarFunction::GeoPerpendicular),
        ("box_contain", BuiltinScalarFunction::GeoContains),
        ("box_contain_pt", BuiltinScalarFunction::GeoContains),
        ("contains", BuiltinScalarFunction::GeoContains),
        ("box_contained", BuiltinScalarFunction::GeoContainedBy),
        ("contained", BuiltinScalarFunction::GeoContainedBy),
        ("box_overlap", BuiltinScalarFunction::GeoOverlap),
        ("circle_overlap", BuiltinScalarFunction::GeoOverlap),
        ("overlap", BuiltinScalarFunction::GeoOverlap),
        ("box_left", BuiltinScalarFunction::GeoLeft),
        ("left", BuiltinScalarFunction::GeoLeft),
        ("box_overleft", BuiltinScalarFunction::GeoOverLeft),
        ("overleft", BuiltinScalarFunction::GeoOverLeft),
        ("box_right", BuiltinScalarFunction::GeoRight),
        ("right", BuiltinScalarFunction::GeoRight),
        ("box_overright", BuiltinScalarFunction::GeoOverRight),
        ("overright", BuiltinScalarFunction::GeoOverRight),
        ("box_below", BuiltinScalarFunction::GeoBelow),
        ("below", BuiltinScalarFunction::GeoBelow),
        ("box_overbelow", BuiltinScalarFunction::GeoOverBelow),
        ("overbelow", BuiltinScalarFunction::GeoOverBelow),
        ("box_above", BuiltinScalarFunction::GeoAbove),
        ("above", BuiltinScalarFunction::GeoAbove),
        ("box_overabove", BuiltinScalarFunction::GeoOverAbove),
        ("overabove", BuiltinScalarFunction::GeoOverAbove),
        ("geo_add", BuiltinScalarFunction::GeoAdd),
        ("geo_sub", BuiltinScalarFunction::GeoSub),
        ("geo_mul", BuiltinScalarFunction::GeoMul),
        ("geo_div", BuiltinScalarFunction::GeoDiv),
        ("range_constructor", BuiltinScalarFunction::RangeConstructor),
        (
            "range_constructor2",
            BuiltinScalarFunction::RangeConstructor,
        ),
        (
            "range_constructor3",
            BuiltinScalarFunction::RangeConstructor,
        ),
        ("range_isempty", BuiltinScalarFunction::RangeIsEmpty),
        ("range_lower", BuiltinScalarFunction::RangeLower),
        ("range_upper", BuiltinScalarFunction::RangeUpper),
        ("range_lower_inc", BuiltinScalarFunction::RangeLowerInc),
        ("range_upper_inc", BuiltinScalarFunction::RangeUpperInc),
        ("range_lower_inf", BuiltinScalarFunction::RangeLowerInf),
        ("range_upper_inf", BuiltinScalarFunction::RangeUpperInf),
        ("range_contains", BuiltinScalarFunction::RangeContains),
        ("range_contains_elem", BuiltinScalarFunction::RangeContains),
        (
            "range_contained_by",
            BuiltinScalarFunction::RangeContainedBy,
        ),
        (
            "elem_contained_by_range",
            BuiltinScalarFunction::RangeContainedBy,
        ),
        ("range_overlaps", BuiltinScalarFunction::RangeOverlap),
        ("range_overlap", BuiltinScalarFunction::RangeOverlap),
        ("range_before", BuiltinScalarFunction::RangeStrictLeft),
        ("range_strict_left", BuiltinScalarFunction::RangeStrictLeft),
        ("range_after", BuiltinScalarFunction::RangeStrictRight),
        (
            "range_strict_right",
            BuiltinScalarFunction::RangeStrictRight,
        ),
        ("range_overleft", BuiltinScalarFunction::RangeOverLeft),
        ("range_over_left", BuiltinScalarFunction::RangeOverLeft),
        ("range_overright", BuiltinScalarFunction::RangeOverRight),
        ("range_over_right", BuiltinScalarFunction::RangeOverRight),
        ("range_adjacent", BuiltinScalarFunction::RangeAdjacent),
        ("range_union", BuiltinScalarFunction::RangeUnion),
        ("range_intersect", BuiltinScalarFunction::RangeIntersect),
        ("range_difference", BuiltinScalarFunction::RangeDifference),
        ("range_merge", BuiltinScalarFunction::RangeMerge),
        ("box_high", BuiltinScalarFunction::GeoBoxHigh),
        ("box_low", BuiltinScalarFunction::GeoBoxLow),
        ("pointx", BuiltinScalarFunction::GeoPointX),
        ("pointy", BuiltinScalarFunction::GeoPointY),
        (
            "bitcast_integer_to_float4",
            BuiltinScalarFunction::BitcastIntegerToFloat4,
        ),
        (
            "bitcast_bigint_to_float8",
            BuiltinScalarFunction::BitcastBigintToFloat8,
        ),
        ("pg_input_is_valid", BuiltinScalarFunction::PgInputIsValid),
        (
            "pg_input_error_message",
            BuiltinScalarFunction::PgInputErrorMessage,
        ),
        (
            "pg_input_error_detail",
            BuiltinScalarFunction::PgInputErrorDetail,
        ),
        (
            "pg_input_error_hint",
            BuiltinScalarFunction::PgInputErrorHint,
        ),
        (
            "pg_input_error_sqlstate",
            BuiltinScalarFunction::PgInputErrorSqlState,
        ),
        ("pg_get_acl", BuiltinScalarFunction::PgGetAcl),
        ("pg_get_userbyid", BuiltinScalarFunction::PgGetUserById),
        ("obj_description", BuiltinScalarFunction::ObjDescription),
        (
            "pg_describe_object",
            BuiltinScalarFunction::PgDescribeObject,
        ),
        (
            "pg_identify_object",
            BuiltinScalarFunction::PgIdentifyObject,
        ),
        (
            "pg_identify_object_as_address",
            BuiltinScalarFunction::PgIdentifyObjectAsAddress,
        ),
        (
            "pg_get_object_address",
            BuiltinScalarFunction::PgGetObjectAddress,
        ),
        (
            "pg_event_trigger_table_rewrite_oid",
            BuiltinScalarFunction::PgEventTriggerTableRewriteOid,
        ),
        (
            "pg_event_trigger_table_rewrite_reason",
            BuiltinScalarFunction::PgEventTriggerTableRewriteReason,
        ),
        (
            "pg_get_function_arguments",
            BuiltinScalarFunction::PgGetFunctionArguments,
        ),
        (
            "pg_get_function_identity_arguments",
            BuiltinScalarFunction::PgGetFunctionIdentityArguments,
        ),
        (
            "pg_get_function_arg_default",
            BuiltinScalarFunction::PgGetFunctionArgDefault,
        ),
        (
            "pg_get_functiondef",
            BuiltinScalarFunction::PgGetFunctionDef,
        ),
        (
            "pg_get_function_result",
            BuiltinScalarFunction::PgGetFunctionResult,
        ),
        (
            "pg_function_is_visible",
            BuiltinScalarFunction::PgFunctionIsVisible,
        ),
        (
            "pg_get_constraintdef",
            BuiltinScalarFunction::PgGetConstraintDef,
        ),
        (
            "pg_get_constraintdef_ext",
            BuiltinScalarFunction::PgGetConstraintDef,
        ),
        ("pg_get_indexdef", BuiltinScalarFunction::PgGetIndexDef),
        ("pg_get_indexdef_ext", BuiltinScalarFunction::PgGetIndexDef),
        ("pg_get_partkeydef", BuiltinScalarFunction::PgGetPartKeyDef),
        ("pg_get_ruledef", BuiltinScalarFunction::PgGetRuleDef),
        ("pg_get_ruledef_ext", BuiltinScalarFunction::PgGetRuleDef),
        ("pg_get_viewdef", BuiltinScalarFunction::PgGetViewDef),
        ("pg_get_viewdef_name", BuiltinScalarFunction::PgGetViewDef),
        ("pg_get_viewdef_ext", BuiltinScalarFunction::PgGetViewDef),
        (
            "pg_get_viewdef_name_ext",
            BuiltinScalarFunction::PgGetViewDef,
        ),
        ("pg_get_viewdef_wrap", BuiltinScalarFunction::PgGetViewDef),
        ("pg_get_ruledef", BuiltinScalarFunction::PgGetRuleDef),
        ("pg_get_ruledef_ext", BuiltinScalarFunction::PgGetRuleDef),
        (
            "pg_get_statisticsobjdef",
            BuiltinScalarFunction::PgGetStatisticsObjDef,
        ),
        (
            "pg_get_statisticsobjdef_columns",
            BuiltinScalarFunction::PgGetStatisticsObjDefColumns,
        ),
        (
            "pg_get_statisticsobjdef_expressions",
            BuiltinScalarFunction::PgGetStatisticsObjDefExpressions,
        ),
        (
            "pg_statistics_obj_is_visible",
            BuiltinScalarFunction::PgStatisticsObjIsVisible,
        ),
        ("pg_notify", BuiltinScalarFunction::PgNotify),
        (
            "pg_notification_queue_usage",
            BuiltinScalarFunction::PgNotificationQueueUsage,
        ),
        (
            "pg_indexam_has_property",
            BuiltinScalarFunction::PgIndexAmHasProperty,
        ),
        (
            "pg_index_has_property",
            BuiltinScalarFunction::PgIndexHasProperty,
        ),
        (
            "pg_index_column_has_property",
            BuiltinScalarFunction::PgIndexColumnHasProperty,
        ),
        ("pg_get_expr", BuiltinScalarFunction::PgGetExpr),
        ("pg_get_expr_ext", BuiltinScalarFunction::PgGetExpr),
        ("pg_get_partkeydef", BuiltinScalarFunction::PgGetPartKeyDef),
        (
            "pg_get_partition_constraintdef",
            BuiltinScalarFunction::PgGetPartitionConstraintDef,
        ),
        (
            "pg_relation_is_publishable",
            BuiltinScalarFunction::PgRelationIsPublishable,
        ),
        (
            "pg_relation_is_updatable",
            BuiltinScalarFunction::PgRelationIsUpdatable,
        ),
        (
            "pg_column_is_updatable",
            BuiltinScalarFunction::PgColumnIsUpdatable,
        ),
    ]
}

#[cfg(test)]
fn oid_argtypes(arg_oids: &[u32]) -> String {
    arg_oids
        .iter()
        .map(u32::to_string)
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_proc_desc_matches_expected_columns() {
        let desc = pg_proc_desc();
        let names: Vec<_> = desc
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect();
        assert_eq!(
            names,
            vec![
                "oid",
                "proname",
                "pronamespace",
                "proowner",
                "prolang",
                "procost",
                "prorows",
                "provariadic",
                "prosupport",
                "prokind",
                "prosecdef",
                "proleakproof",
                "proisstrict",
                "proretset",
                "provolatile",
                "proparallel",
                "pronargs",
                "pronargdefaults",
                "prorettype",
                "proargtypes",
                "proallargtypes",
                "proargmodes",
                "proargnames",
                "proargdefaults",
                "prosrc",
                "probin",
                "prosqlbody",
                "proconfig",
                "proacl",
            ]
        );
    }

    #[test]
    fn bootstrap_record_returning_rows_expose_out_metadata() {
        let row = bootstrap_pg_proc_rows()
            .into_iter()
            .find(|row| {
                row.proname == "json_each" && row.proargtypes == oid_argtypes(&[JSON_TYPE_OID])
            })
            .expect("json_each row");
        assert_eq!(row.prorettype, RECORD_TYPE_OID);
        assert_eq!(
            row.proallargtypes,
            Some(vec![JSON_TYPE_OID, TEXT_TYPE_OID, JSON_TYPE_OID])
        );
        assert_eq!(row.proargmodes, Some(vec![b'i', b'o', b'o']));
        assert_eq!(
            row.proargnames,
            Some(vec![String::new(), "key".into(), "value".into()])
        );
    }

    #[test]
    fn bootstrap_variadic_rows_mark_last_arg_mode() {
        let rows = bootstrap_pg_proc_rows();
        for proname in ["concat", "concat_ws", "format", "json_build_array"] {
            let row = rows
                .iter()
                .find(|row| row.proname == proname)
                .unwrap_or_else(|| panic!("missing variadic row {proname}"));
            assert_ne!(row.provariadic, 0);
            assert_eq!(
                row.proargmodes
                    .as_ref()
                    .and_then(|modes| modes.last())
                    .copied(),
                Some(b'v')
            );
        }
    }

    #[test]
    fn scalar_proc_oid_helpers_cover_real_and_synthetic_builtins() {
        assert_eq!(
            builtin_scalar_function_for_proc_oid(6202),
            Some(BuiltinScalarFunction::Lower)
        );
        assert_eq!(
            proc_oid_for_builtin_scalar_function(BuiltinScalarFunction::Lower),
            Some(6202)
        );
        assert_eq!(
            builtin_scalar_function_for_proc_oid(6204),
            Some(BuiltinScalarFunction::Upper)
        );
        assert_eq!(
            proc_oid_for_builtin_scalar_function(BuiltinScalarFunction::Upper),
            Some(6204)
        );
        assert_eq!(
            builtin_scalar_function_for_proc_oid(
                proc_oid_for_builtin_scalar_function(BuiltinScalarFunction::ArrayToJson)
                    .expect("synthetic oid")
            ),
            Some(BuiltinScalarFunction::ArrayToJson)
        );
        assert_eq!(
            builtin_scalar_function_for_proc_oid(
                proc_oid_for_builtin_scalar_function(BuiltinScalarFunction::RegRoleToText)
                    .expect("synthetic oid")
            ),
            Some(BuiltinScalarFunction::RegRoleToText)
        );
    }

    #[test]
    fn bootstrap_left_proc_row_matches_postgres_volatility() {
        let row = bootstrap_pg_proc_rows()
            .into_iter()
            .find(|row| row.oid == 3060)
            .expect("left(text, int4) row");
        assert_eq!(row.proname, "left");
        assert_eq!(
            row.proargtypes,
            oid_argtypes(&[TEXT_TYPE_OID, INT4_TYPE_OID])
        );
        assert_eq!(row.provolatile, 'i');
        assert_eq!(
            builtin_scalar_function_for_proc_oid(row.oid),
            Some(BuiltinScalarFunction::Left)
        );
    }

    #[test]
    fn bootstrap_rows_include_macaddr_builtin_procs() {
        let rows = bootstrap_pg_proc_rows();
        let macaddr_cmp = rows
            .iter()
            .find(|row| row.oid == MACADDR_CMP_PROC_OID)
            .expect("macaddr_cmp row");
        assert_eq!(macaddr_cmp.proname, "macaddr_cmp");
        assert_eq!(
            macaddr_cmp.proargtypes,
            oid_argtypes(&[MACADDR_TYPE_OID, MACADDR_TYPE_OID])
        );
        assert_eq!(macaddr_cmp.prorettype, INT4_TYPE_OID);

        let macaddr8_cast = rows
            .iter()
            .find(|row| row.oid == MACADDR_TO_MACADDR8_PROC_OID)
            .expect("macaddr to macaddr8 cast row");
        assert_eq!(macaddr8_cast.prorettype, MACADDR8_TYPE_OID);
        assert_eq!(
            builtin_scalar_function_for_proc_oid(macaddr8_cast.oid),
            Some(BuiltinScalarFunction::MacAddrToMacAddr8)
        );

        let hash_extended = rows
            .iter()
            .find(|row| row.oid == HASH_MACADDR8_EXTENDED_PROC_OID)
            .expect("hashmacaddr8extended row");
        assert_eq!(hash_extended.proname, "hashmacaddr8extended");
        assert_eq!(
            hash_extended.proargtypes,
            oid_argtypes(&[MACADDR8_TYPE_OID, INT8_TYPE_OID])
        );
        assert_eq!(hash_extended.prorettype, INT8_TYPE_OID);
    }

    #[test]
    fn bootstrap_rows_include_pg_rust_test_fdw_handler() {
        let row = bootstrap_pg_proc_rows()
            .into_iter()
            .find(|row| row.proname == "pg_rust_test_fdw_handler")
            .expect("pg_rust_test_fdw_handler row");
        assert_eq!(row.prorettype, FDW_HANDLER_TYPE_OID);
        assert_eq!(row.proargtypes, "");
        assert_eq!(
            builtin_scalar_function_for_proc_oid(row.oid),
            Some(BuiltinScalarFunction::PgRustTestFdwHandler)
        );
    }

    #[test]
    fn bootstrap_rows_have_unique_oids() {
        use std::collections::HashSet;

        let mut seen = HashSet::new();
        for row in bootstrap_pg_proc_rows() {
            assert!(
                seen.insert(row.oid),
                "duplicate pg_proc oid {} for {}",
                row.oid,
                row.proname
            );
        }
    }

    #[test]
    fn indexed_builtin_proc_helpers_match_bootstrap_rows() {
        let lower_rows = bootstrap_pg_proc_rows_ref()
            .iter()
            .filter(|row| row.proname == "lower")
            .map(|row| (*row).clone())
            .collect::<Vec<_>>();
        assert_eq!(bootstrap_pg_proc_rows_by_name("lower"), lower_rows);
        assert_eq!(
            bootstrap_pg_proc_rows_by_name("pg_catalog.lower"),
            lower_rows
        );

        for row in bootstrap_pg_proc_rows_ref() {
            if row.prokind == 'f' && !row.proretset {
                let expected = builtin_scalar_function_for_proc_row(row);
                if expected.is_some() {
                    assert_eq!(builtin_scalar_function_for_proc_oid(row.oid), expected);
                }
            }
            if row.prokind == 'a' {
                let expected = aggregate_func_for_proname(&row.proname);
                if expected.is_some() {
                    assert_eq!(builtin_aggregate_function_for_proc_oid(row.oid), expected);
                }
                let expected = hypothetical_aggregate_func_for_proname(&row.proname);
                if expected.is_some() {
                    assert_eq!(
                        builtin_hypothetical_aggregate_function_for_proc_oid(row.oid),
                        expected
                    );
                }
            }
            if row.prokind == 'w' {
                let expected = window_func_for_proname(&row.proname);
                if expected.is_some() {
                    assert_eq!(builtin_window_function_for_proc_oid(row.oid), expected);
                }
            }
        }
    }

    #[test]
    fn builtin_scalar_helpers_have_proc_oid_mappings() {
        for func in [
            BuiltinScalarFunction::CurrentDatabase,
            BuiltinScalarFunction::PgBackendPid,
            BuiltinScalarFunction::PgBlockingPids,
            BuiltinScalarFunction::PgIsolationTestSessionIsBlocked,
            BuiltinScalarFunction::CurrentSetting,
            BuiltinScalarFunction::RegProcedureToText,
            BuiltinScalarFunction::RegRoleToText,
            BuiltinScalarFunction::PgGetUserById,
            BuiltinScalarFunction::PgDescribeObject,
            BuiltinScalarFunction::PgIdentifyObject,
            BuiltinScalarFunction::PgIdentifyObjectAsAddress,
            BuiltinScalarFunction::PgGetObjectAddress,
            BuiltinScalarFunction::PgGetRuleDef,
            BuiltinScalarFunction::PgGetViewDef,
            BuiltinScalarFunction::PgGetPartKeyDef,
            BuiltinScalarFunction::PgRelationFilenode,
            BuiltinScalarFunction::PgFilenodeRelation,
            BuiltinScalarFunction::PgNotify,
            BuiltinScalarFunction::PgNotificationQueueUsage,
            BuiltinScalarFunction::PgIndexAmHasProperty,
            BuiltinScalarFunction::PgIndexHasProperty,
            BuiltinScalarFunction::PgIndexColumnHasProperty,
            BuiltinScalarFunction::Float8Accum,
            BuiltinScalarFunction::Float8Combine,
            BuiltinScalarFunction::Float8RegrAccum,
            BuiltinScalarFunction::Float8RegrCombine,
            BuiltinScalarFunction::MacAddrEq,
            BuiltinScalarFunction::MacAddrCmp,
            BuiltinScalarFunction::MacAddrTrunc,
            BuiltinScalarFunction::MacAddrToMacAddr8,
            BuiltinScalarFunction::MacAddr8Eq,
            BuiltinScalarFunction::MacAddr8Cmp,
            BuiltinScalarFunction::MacAddr8ToMacAddr,
            BuiltinScalarFunction::MacAddr8Set7Bit,
            BuiltinScalarFunction::HashMacAddr,
            BuiltinScalarFunction::HashMacAddrExtended,
            BuiltinScalarFunction::HashMacAddr8,
            BuiltinScalarFunction::HashMacAddr8Extended,
            BuiltinScalarFunction::XmlComment,
            BuiltinScalarFunction::XmlText,
            BuiltinScalarFunction::XmlIsWellFormed,
            BuiltinScalarFunction::XmlIsWellFormedDocument,
            BuiltinScalarFunction::XmlIsWellFormedContent,
        ] {
            let oid = proc_oid_for_builtin_scalar_function(func)
                .unwrap_or_else(|| panic!("missing pg_proc oid mapping for {:?}", func));
            assert_eq!(builtin_scalar_function_for_proc_oid(oid), Some(func));
        }
    }

    #[test]
    fn advisory_lock_rows_have_expected_oids_and_parallel_safety() {
        let cases = [
            (
                "pg_advisory_lock",
                oid_argtypes(&[INT8_TYPE_OID]),
                2880,
                VOID_TYPE_OID,
                BuiltinScalarFunction::PgAdvisoryLock,
            ),
            (
                "pg_advisory_lock",
                oid_argtypes(&[INT4_TYPE_OID, INT4_TYPE_OID]),
                3089,
                VOID_TYPE_OID,
                BuiltinScalarFunction::PgAdvisoryLock,
            ),
            (
                "pg_try_advisory_lock_shared",
                oid_argtypes(&[INT8_TYPE_OID]),
                2887,
                BOOL_TYPE_OID,
                BuiltinScalarFunction::PgTryAdvisoryLockShared,
            ),
            (
                "pg_advisory_unlock_all",
                String::new(),
                2892,
                VOID_TYPE_OID,
                BuiltinScalarFunction::PgAdvisoryUnlockAll,
            ),
        ];

        for (proname, proargtypes, oid, prorettype, func) in cases {
            let row = bootstrap_pg_proc_rows()
                .into_iter()
                .find(|row| row.proname == proname && row.proargtypes == proargtypes)
                .unwrap_or_else(|| panic!("missing pg_proc row for {proname}({proargtypes})"));
            assert_eq!(row.oid, oid);
            assert_eq!(row.prorettype, prorettype);
            assert_eq!(row.provolatile, 'v');
            assert_eq!(row.proparallel, 'r');
            assert_eq!(builtin_scalar_function_for_proc_oid(row.oid), Some(func));
        }
    }

    #[test]
    fn bootstrap_rows_include_pg_backend_pid() {
        let row = bootstrap_pg_proc_rows()
            .into_iter()
            .find(|row| row.proname == "pg_backend_pid")
            .expect("pg_backend_pid row");
        assert_eq!(row.oid, 2026);
        assert_eq!(row.prorettype, INT4_TYPE_OID);
        assert_eq!(row.proargtypes, "");
        assert_eq!(row.provolatile, 's');
        assert_eq!(row.proparallel, 'r');
        assert_eq!(
            builtin_scalar_function_for_proc_oid(row.oid),
            Some(BuiltinScalarFunction::PgBackendPid)
        );
    }

    #[test]
    fn bootstrap_rows_include_factorial_int8() {
        let row = bootstrap_pg_proc_rows()
            .into_iter()
            .find(|row| {
                row.proname == "factorial" && row.proargtypes == oid_argtypes(&[INT8_TYPE_OID])
            })
            .expect("factorial(int8) row");
        assert_eq!(row.oid, 1376);
        assert_eq!(row.prorettype, NUMERIC_TYPE_OID);
        assert_eq!(
            builtin_scalar_function_for_proc_oid(row.oid),
            Some(BuiltinScalarFunction::Factorial)
        );
    }

    #[test]
    fn any_value_aggregate_has_proc_oid_mapping() {
        let oid =
            proc_oid_for_builtin_aggregate_function(AggFunc::AnyValue).expect("any_value oid");
        assert_eq!(
            builtin_aggregate_function_for_proc_oid(oid),
            Some(AggFunc::AnyValue)
        );
    }
}
