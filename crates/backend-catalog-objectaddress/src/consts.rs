//! Catalog-header constants objectaddress.c reasons over: the fixed catalog
//! relation OIDs (`*RelationId`), their OID-column unique index OIDs
//! (`*OidIndexId`), the per-catalog attribute numbers used in the
//! `ObjectProperty[]` table, and the small `char` enums (`DEFACLOBJ_*`,
//! `PROKIND_*`, `TYPTYPE_DOMAIN`).
//!
//! These are pinned `#define`s from `src/include/catalog/pg_*_d.h` (the genbki
//! bootstrap OIDs are fixed across the build), so they are reproduced here as
//! plain constants rather than crossing the seam.  (Syscache resolves its
//! `cacheinfo[]` reloid/indoid pairs at init because those vary with the index
//! definition; objectaddress.c only ever *compares* `classId` against these
//! fixed values, so constants are the faithful translation.)

#![allow(dead_code)]
#![allow(non_upper_case_globals)]

use types_core::Oid;

/* ---------------------------------------------------------------------------
 * Catalog relation OIDs (pg_*_d.h *RelationId)
 * ------------------------------------------------------------------------- */

pub const AccessMethodRelationId: Oid = 2601;
pub const AccessMethodOperatorRelationId: Oid = 2602;
pub const AccessMethodProcedureRelationId: Oid = 2603;
pub const AttrDefaultRelationId: Oid = 2604;
pub const CastRelationId: Oid = 2605;
pub const CollationRelationId: Oid = 3456;
pub const ConstraintRelationId: Oid = 2606;
pub const ConversionRelationId: Oid = 2607;
pub const DatabaseRelationId: Oid = 1262;
pub const DefaultAclRelationId: Oid = 826;
pub const ExtensionRelationId: Oid = 3079;
pub const ForeignDataWrapperRelationId: Oid = 2328;
pub const ForeignServerRelationId: Oid = 1417;
pub const LanguageRelationId: Oid = 2612;
pub const LargeObjectRelationId: Oid = 2613;
pub const LargeObjectMetadataRelationId: Oid = 2995;
pub const OperatorClassRelationId: Oid = 2616;
pub const OperatorRelationId: Oid = 2617;
pub const OperatorFamilyRelationId: Oid = 2753;
pub const AuthIdRelationId: Oid = 1260;
pub const AuthMemRelationId: Oid = 1261;
pub const RewriteRelationId: Oid = 2618;
pub const NamespaceRelationId: Oid = 2615;
pub const RelationRelationId: Oid = 1259;
pub const TableSpaceRelationId: Oid = 1213;
pub const TransformRelationId: Oid = 3576;
pub const TriggerRelationId: Oid = 2620;
pub const PolicyRelationId: Oid = 3256;
pub const EventTriggerRelationId: Oid = 3466;
pub const TSConfigRelationId: Oid = 3602;
pub const TSDictionaryRelationId: Oid = 3600;
pub const TSParserRelationId: Oid = 3601;
pub const TSTemplateRelationId: Oid = 3764;
pub const TypeRelationId: Oid = 1247;
pub const PublicationRelationId: Oid = 6104;
pub const PublicationNamespaceRelationId: Oid = 6237;
pub const PublicationRelRelationId: Oid = 6106;
pub const SubscriptionRelationId: Oid = 6100;
pub const StatisticExtRelationId: Oid = 3381;
pub const UserMappingRelationId: Oid = 1418;
pub const ProcedureRelationId: Oid = 1255;
pub const ParameterAclRelationId: Oid = 6243;

/* ---------------------------------------------------------------------------
 * OID-column unique index OIDs (pg_*_d.h *OidIndexId / *ObjectIndexId)
 * ------------------------------------------------------------------------- */

pub const AmOidIndexId: Oid = 2652;
pub const AccessMethodOperatorOidIndexId: Oid = 2756;
pub const AccessMethodProcedureOidIndexId: Oid = 2757;
pub const CastOidIndexId: Oid = 2660;
pub const CollationOidIndexId: Oid = 3085;
pub const ConstraintOidIndexId: Oid = 2667;
pub const ConversionOidIndexId: Oid = 2670;
pub const DatabaseOidIndexId: Oid = 2672;
pub const DefaultAclOidIndexId: Oid = 828;
pub const ExtensionOidIndexId: Oid = 3080;
pub const ForeignDataWrapperOidIndexId: Oid = 112;
pub const ForeignServerOidIndexId: Oid = 113;
pub const LanguageOidIndexId: Oid = 2682;
pub const LargeObjectMetadataOidIndexId: Oid = 2996;
pub const OpclassOidIndexId: Oid = 2687;
pub const OperatorOidIndexId: Oid = 2688;
pub const OpfamilyOidIndexId: Oid = 2755;
pub const AuthIdOidIndexId: Oid = 2677;
pub const AuthMemOidIndexId: Oid = 6303;
pub const RewriteOidIndexId: Oid = 2692;
pub const NamespaceOidIndexId: Oid = 2685;
pub const ClassOidIndexId: Oid = 2662;
pub const TablespaceOidIndexId: Oid = 2697;
pub const TransformOidIndexId: Oid = 3574;
pub const TriggerOidIndexId: Oid = 2702;
pub const PolicyOidIndexId: Oid = 3257;
pub const EventTriggerOidIndexId: Oid = 3468;
pub const TSConfigOidIndexId: Oid = 3712;
pub const TSDictionaryOidIndexId: Oid = 3605;
pub const TSParserOidIndexId: Oid = 3607;
pub const TSTemplateOidIndexId: Oid = 3767;
pub const TypeOidIndexId: Oid = 2703;
pub const PublicationObjectIndexId: Oid = 6110;
pub const PublicationNamespaceObjectIndexId: Oid = 6238;
pub const PublicationRelObjectIndexId: Oid = 6112;
pub const SubscriptionObjectIndexId: Oid = 6114;
pub const StatisticExtOidIndexId: Oid = 3380;
pub const UserMappingOidIndexId: Oid = 174;
pub const ProcedureOidIndexId: Oid = 2690;
pub const ParameterAclOidIndexId: Oid = 6247;
pub const AttrDefaultOidIndexId: Oid = 2657;

/* ---------------------------------------------------------------------------
 * defaclobjtype (pg_default_acl.h)
 * ------------------------------------------------------------------------- */

pub const DEFACLOBJ_RELATION: i8 = b'r' as i8;
pub const DEFACLOBJ_SEQUENCE: i8 = b'S' as i8;
pub const DEFACLOBJ_FUNCTION: i8 = b'f' as i8;
pub const DEFACLOBJ_TYPE: i8 = b'T' as i8;
pub const DEFACLOBJ_NAMESPACE: i8 = b'n' as i8;
pub const DEFACLOBJ_LARGEOBJECT: i8 = b'L' as i8;

/* ---------------------------------------------------------------------------
 * prokind (pg_proc.h) / typtype (pg_type.h)
 * ------------------------------------------------------------------------- */

pub const PROKIND_FUNCTION: i8 = b'f' as i8;
pub const PROKIND_AGGREGATE: i8 = b'a' as i8;
pub const PROKIND_WINDOW: i8 = b'w' as i8;
pub const PROKIND_PROCEDURE: i8 = b'p' as i8;

pub const TYPTYPE_DOMAIN: i8 = b'd' as i8;

/* ---------------------------------------------------------------------------
 * format_type/format_procedure/format_operator flag bits (utils headers)
 * ------------------------------------------------------------------------- */

pub const FORMAT_TYPE_TYPEMOD_GIVEN: u16 = 0x01;
pub const FORMAT_TYPE_ALLOW_INVALID: u16 = 0x02;
pub const FORMAT_TYPE_FORCE_QUALIFY: u16 = 0x04;
pub const FORMAT_TYPE_INVALID_AS_NULL: u16 = 0x08;

pub const FORMAT_PROC_INVALID_AS_NULL: u16 = 0x01;
pub const FORMAT_PROC_FORCE_QUALIFY: u16 = 0x02;

pub const FORMAT_OPERATOR_INVALID_AS_NULL: u16 = 0x01;
pub const FORMAT_OPERATOR_FORCE_QUALIFY: u16 = 0x02;

/* ---------------------------------------------------------------------------
 * Attribute numbers used in ObjectProperty[] (pg_*_d.h Anum_*).
 *
 * These are the catalog struct field positions; reproduced here verbatim so
 * the ObjectProperty rows match the C source one-to-one.
 * ------------------------------------------------------------------------- */

pub const InvalidAttrNumber: i16 = 0;

// pg_am
pub const Anum_pg_am_oid: i16 = 1;
pub const Anum_pg_am_amname: i16 = 2;
// pg_amop / pg_amproc
pub const Anum_pg_amop_oid: i16 = 1;
pub const Anum_pg_amop_amopfamily: i16 = 2;
pub const Anum_pg_amop_amoplefttype: i16 = 3;
pub const Anum_pg_amop_amoprighttype: i16 = 4;
pub const Anum_pg_amop_amopstrategy: i16 = 5;
pub const Anum_pg_amop_amopopr: i16 = 7;
pub const Anum_pg_amproc_oid: i16 = 1;
pub const Anum_pg_amproc_amprocfamily: i16 = 2;
pub const Anum_pg_amproc_amproclefttype: i16 = 3;
pub const Anum_pg_amproc_amprocrighttype: i16 = 4;
pub const Anum_pg_amproc_amprocnum: i16 = 5;
pub const Anum_pg_amproc_amproc: i16 = 6;
// pg_cast
pub const Anum_pg_cast_oid: i16 = 1;
pub const Anum_pg_cast_castsource: i16 = 2;
pub const Anum_pg_cast_casttarget: i16 = 3;
// pg_collation
pub const Anum_pg_collation_oid: i16 = 1;
pub const Anum_pg_collation_collname: i16 = 2;
pub const Anum_pg_collation_collnamespace: i16 = 3;
pub const Anum_pg_collation_collowner: i16 = 4;
// pg_constraint
pub const Anum_pg_constraint_oid: i16 = 1;
pub const Anum_pg_constraint_conname: i16 = 2;
pub const Anum_pg_constraint_connamespace: i16 = 3;
// pg_conversion
pub const Anum_pg_conversion_oid: i16 = 1;
pub const Anum_pg_conversion_conname: i16 = 2;
pub const Anum_pg_conversion_connamespace: i16 = 3;
pub const Anum_pg_conversion_conowner: i16 = 4;
// pg_database
pub const Anum_pg_database_oid: i16 = 1;
pub const Anum_pg_database_datname: i16 = 2;
pub const Anum_pg_database_datdba: i16 = 3;
pub const Anum_pg_database_datacl: i16 = 18;
// pg_default_acl
pub const Anum_pg_default_acl_oid: i16 = 1;
pub const Anum_pg_default_acl_defaclrole: i16 = 2;
pub const Anum_pg_default_acl_defaclnamespace: i16 = 3;
pub const Anum_pg_default_acl_defaclobjtype: i16 = 4;
pub const Anum_pg_default_acl_defaclacl: i16 = 5;
// pg_extension
pub const Anum_pg_extension_oid: i16 = 1;
pub const Anum_pg_extension_extname: i16 = 2;
pub const Anum_pg_extension_extowner: i16 = 3;
// pg_foreign_data_wrapper
pub const Anum_pg_foreign_data_wrapper_oid: i16 = 1;
pub const Anum_pg_foreign_data_wrapper_fdwname: i16 = 2;
pub const Anum_pg_foreign_data_wrapper_fdwowner: i16 = 3;
pub const Anum_pg_foreign_data_wrapper_fdwacl: i16 = 6;
// pg_foreign_server
pub const Anum_pg_foreign_server_oid: i16 = 1;
pub const Anum_pg_foreign_server_srvname: i16 = 2;
pub const Anum_pg_foreign_server_srvowner: i16 = 3;
pub const Anum_pg_foreign_server_srvacl: i16 = 7;
// pg_proc
pub const Anum_pg_proc_oid: i16 = 1;
pub const Anum_pg_proc_proname: i16 = 2;
pub const Anum_pg_proc_pronamespace: i16 = 3;
pub const Anum_pg_proc_proowner: i16 = 4;
pub const Anum_pg_proc_proacl: i16 = 30;
// pg_language
pub const Anum_pg_language_oid: i16 = 1;
pub const Anum_pg_language_lanname: i16 = 2;
pub const Anum_pg_language_lanowner: i16 = 3;
pub const Anum_pg_language_lanacl: i16 = 9;
// pg_largeobject_metadata
pub const Anum_pg_largeobject_metadata_oid: i16 = 1;
pub const Anum_pg_largeobject_metadata_lomowner: i16 = 2;
pub const Anum_pg_largeobject_metadata_lomacl: i16 = 3;
// pg_opclass
pub const Anum_pg_opclass_oid: i16 = 1;
pub const Anum_pg_opclass_opcname: i16 = 3;
pub const Anum_pg_opclass_opcnamespace: i16 = 4;
pub const Anum_pg_opclass_opcowner: i16 = 5;
// pg_operator
pub const Anum_pg_operator_oid: i16 = 1;
pub const Anum_pg_operator_oprname: i16 = 2;
pub const Anum_pg_operator_oprnamespace: i16 = 3;
pub const Anum_pg_operator_oprowner: i16 = 4;
// pg_opfamily
pub const Anum_pg_opfamily_oid: i16 = 1;
pub const Anum_pg_opfamily_opfname: i16 = 3;
pub const Anum_pg_opfamily_opfnamespace: i16 = 4;
pub const Anum_pg_opfamily_opfowner: i16 = 5;
// pg_authid
pub const Anum_pg_authid_oid: i16 = 1;
pub const Anum_pg_authid_rolname: i16 = 2;
// pg_auth_members
pub const Anum_pg_auth_members_oid: i16 = 1;
pub const Anum_pg_auth_members_roleid: i16 = 2;
pub const Anum_pg_auth_members_member: i16 = 3;
pub const Anum_pg_auth_members_grantor: i16 = 4;
// pg_rewrite
pub const Anum_pg_rewrite_oid: i16 = 1;
pub const Anum_pg_rewrite_rulename: i16 = 2;
// pg_namespace
pub const Anum_pg_namespace_oid: i16 = 1;
pub const Anum_pg_namespace_nspname: i16 = 2;
pub const Anum_pg_namespace_nspowner: i16 = 3;
pub const Anum_pg_namespace_nspacl: i16 = 4;
// pg_class
pub const Anum_pg_class_oid: i16 = 1;
pub const Anum_pg_class_relname: i16 = 2;
pub const Anum_pg_class_relnamespace: i16 = 3;
pub const Anum_pg_class_relowner: i16 = 6;
pub const Anum_pg_class_relacl: i16 = 32;
// pg_tablespace
pub const Anum_pg_tablespace_oid: i16 = 1;
pub const Anum_pg_tablespace_spcname: i16 = 2;
pub const Anum_pg_tablespace_spcowner: i16 = 3;
pub const Anum_pg_tablespace_spcacl: i16 = 4;
// pg_transform
pub const Anum_pg_transform_oid: i16 = 1;
// pg_trigger
pub const Anum_pg_trigger_oid: i16 = 1;
pub const Anum_pg_trigger_tgrelid: i16 = 2;
pub const Anum_pg_trigger_tgname: i16 = 4;
// pg_policy
pub const Anum_pg_policy_oid: i16 = 1;
pub const Anum_pg_policy_polname: i16 = 2;
pub const Anum_pg_policy_polrelid: i16 = 3;
// pg_event_trigger
pub const Anum_pg_event_trigger_oid: i16 = 1;
pub const Anum_pg_event_trigger_evtname: i16 = 2;
pub const Anum_pg_event_trigger_evtowner: i16 = 4;
// pg_ts_config
pub const Anum_pg_ts_config_oid: i16 = 1;
pub const Anum_pg_ts_config_cfgname: i16 = 2;
pub const Anum_pg_ts_config_cfgnamespace: i16 = 3;
pub const Anum_pg_ts_config_cfgowner: i16 = 4;
// pg_ts_dict
pub const Anum_pg_ts_dict_oid: i16 = 1;
pub const Anum_pg_ts_dict_dictname: i16 = 2;
pub const Anum_pg_ts_dict_dictnamespace: i16 = 3;
pub const Anum_pg_ts_dict_dictowner: i16 = 4;
// pg_ts_parser
pub const Anum_pg_ts_parser_oid: i16 = 1;
pub const Anum_pg_ts_parser_prsname: i16 = 2;
pub const Anum_pg_ts_parser_prsnamespace: i16 = 3;
// pg_ts_template
pub const Anum_pg_ts_template_oid: i16 = 1;
pub const Anum_pg_ts_template_tmplname: i16 = 2;
pub const Anum_pg_ts_template_tmplnamespace: i16 = 3;
// pg_type
pub const Anum_pg_type_oid: i16 = 1;
pub const Anum_pg_type_typname: i16 = 2;
pub const Anum_pg_type_typnamespace: i16 = 3;
pub const Anum_pg_type_typowner: i16 = 4;
pub const Anum_pg_type_typacl: i16 = 32;
// pg_publication
pub const Anum_pg_publication_oid: i16 = 1;
pub const Anum_pg_publication_pubname: i16 = 2;
pub const Anum_pg_publication_pubowner: i16 = 3;
// pg_subscription
pub const Anum_pg_subscription_oid: i16 = 1;
pub const Anum_pg_subscription_subname: i16 = 4;
pub const Anum_pg_subscription_subowner: i16 = 5;
// pg_statistic_ext
pub const Anum_pg_statistic_ext_oid: i16 = 1;
pub const Anum_pg_statistic_ext_stxname: i16 = 3;
pub const Anum_pg_statistic_ext_stxnamespace: i16 = 4;
pub const Anum_pg_statistic_ext_stxowner: i16 = 5;
// pg_user_mapping
pub const Anum_pg_user_mapping_oid: i16 = 1;
// pg_publication_rel / pg_publication_namespace
pub const Anum_pg_publication_rel_oid: i16 = 1;
pub const Anum_pg_publication_namespace_oid: i16 = 1;
// pg_parameter_acl
pub const Anum_pg_parameter_acl_parname: i16 = 2;
// pg_attribute
pub const Anum_pg_attribute_attacl: i16 = 22;
