ALTER TABLE pg_proc ADD PRIMARY KEY USING INDEX pg_proc_oid_index;

ALTER TABLE pg_proc ADD UNIQUE USING INDEX pg_proc_proname_args_nsp_index;

ALTER TABLE pg_type ADD PRIMARY KEY USING INDEX pg_type_oid_index;

ALTER TABLE pg_type ADD UNIQUE USING INDEX pg_type_typname_nsp_index;

ALTER TABLE pg_attribute ADD UNIQUE USING INDEX pg_attribute_relid_attnam_index;

ALTER TABLE pg_attribute ADD PRIMARY KEY USING INDEX pg_attribute_relid_attnum_index;

ALTER TABLE pg_class ADD PRIMARY KEY USING INDEX pg_class_oid_index;

ALTER TABLE pg_class ADD UNIQUE USING INDEX pg_class_relname_nsp_index;

ALTER TABLE pg_attrdef ADD UNIQUE USING INDEX pg_attrdef_adrelid_adnum_index;

ALTER TABLE pg_attrdef ADD PRIMARY KEY USING INDEX pg_attrdef_oid_index;

ALTER TABLE pg_constraint ADD UNIQUE USING INDEX pg_constraint_conrelid_contypid_conname_index;

ALTER TABLE pg_constraint ADD PRIMARY KEY USING INDEX pg_constraint_oid_index;

ALTER TABLE pg_inherits ADD PRIMARY KEY USING INDEX pg_inherits_relid_seqno_index;

ALTER TABLE pg_index ADD PRIMARY KEY USING INDEX pg_index_indexrelid_index;

ALTER TABLE pg_operator ADD PRIMARY KEY USING INDEX pg_operator_oid_index;

ALTER TABLE pg_operator ADD UNIQUE USING INDEX pg_operator_oprname_l_r_n_index;

ALTER TABLE pg_opfamily ADD UNIQUE USING INDEX pg_opfamily_am_name_nsp_index;

ALTER TABLE pg_opfamily ADD PRIMARY KEY USING INDEX pg_opfamily_oid_index;

ALTER TABLE pg_opclass ADD UNIQUE USING INDEX pg_opclass_am_name_nsp_index;

ALTER TABLE pg_opclass ADD PRIMARY KEY USING INDEX pg_opclass_oid_index;

ALTER TABLE pg_am ADD UNIQUE USING INDEX pg_am_name_index;

ALTER TABLE pg_am ADD PRIMARY KEY USING INDEX pg_am_oid_index;

ALTER TABLE pg_amop ADD UNIQUE USING INDEX pg_amop_fam_strat_index;

ALTER TABLE pg_amop ADD UNIQUE USING INDEX pg_amop_opr_fam_index;

ALTER TABLE pg_amop ADD PRIMARY KEY USING INDEX pg_amop_oid_index;

ALTER TABLE pg_amproc ADD UNIQUE USING INDEX pg_amproc_fam_proc_index;

ALTER TABLE pg_amproc ADD PRIMARY KEY USING INDEX pg_amproc_oid_index;

ALTER TABLE pg_language ADD UNIQUE USING INDEX pg_language_name_index;

ALTER TABLE pg_language ADD PRIMARY KEY USING INDEX pg_language_oid_index;

ALTER TABLE pg_largeobject_metadata ADD PRIMARY KEY USING INDEX pg_largeobject_metadata_oid_index;

ALTER TABLE pg_largeobject ADD PRIMARY KEY USING INDEX pg_largeobject_loid_pn_index;

ALTER TABLE pg_aggregate ADD PRIMARY KEY USING INDEX pg_aggregate_fnoid_index;

ALTER TABLE pg_statistic ADD PRIMARY KEY USING INDEX pg_statistic_relid_att_inh_index;

ALTER TABLE pg_statistic_ext ADD PRIMARY KEY USING INDEX pg_statistic_ext_oid_index;

ALTER TABLE pg_statistic_ext ADD UNIQUE USING INDEX pg_statistic_ext_name_index;

ALTER TABLE pg_statistic_ext_data ADD PRIMARY KEY USING INDEX pg_statistic_ext_data_stxoid_inh_index;

ALTER TABLE pg_rewrite ADD PRIMARY KEY USING INDEX pg_rewrite_oid_index;

ALTER TABLE pg_rewrite ADD UNIQUE USING INDEX pg_rewrite_rel_rulename_index;

ALTER TABLE pg_trigger ADD UNIQUE USING INDEX pg_trigger_tgrelid_tgname_index;

ALTER TABLE pg_trigger ADD PRIMARY KEY USING INDEX pg_trigger_oid_index;

ALTER TABLE pg_event_trigger ADD UNIQUE USING INDEX pg_event_trigger_evtname_index;

ALTER TABLE pg_event_trigger ADD PRIMARY KEY USING INDEX pg_event_trigger_oid_index;

ALTER TABLE pg_description ADD PRIMARY KEY USING INDEX pg_description_o_c_o_index;

ALTER TABLE pg_cast ADD PRIMARY KEY USING INDEX pg_cast_oid_index;

ALTER TABLE pg_cast ADD UNIQUE USING INDEX pg_cast_source_target_index;

ALTER TABLE pg_enum ADD PRIMARY KEY USING INDEX pg_enum_oid_index;

ALTER TABLE pg_enum ADD UNIQUE USING INDEX pg_enum_typid_label_index;

ALTER TABLE pg_enum ADD UNIQUE USING INDEX pg_enum_typid_sortorder_index;

ALTER TABLE pg_namespace ADD UNIQUE USING INDEX pg_namespace_nspname_index;

ALTER TABLE pg_namespace ADD PRIMARY KEY USING INDEX pg_namespace_oid_index;

ALTER TABLE pg_conversion ADD UNIQUE USING INDEX pg_conversion_default_index;

ALTER TABLE pg_conversion ADD UNIQUE USING INDEX pg_conversion_name_nsp_index;

ALTER TABLE pg_conversion ADD PRIMARY KEY USING INDEX pg_conversion_oid_index;

ALTER TABLE pg_database ADD UNIQUE USING INDEX pg_database_datname_index;

ALTER TABLE pg_database ADD PRIMARY KEY USING INDEX pg_database_oid_index;

ALTER TABLE pg_db_role_setting ADD PRIMARY KEY USING INDEX pg_db_role_setting_databaseid_rol_index;

ALTER TABLE pg_tablespace ADD PRIMARY KEY USING INDEX pg_tablespace_oid_index;

ALTER TABLE pg_tablespace ADD UNIQUE USING INDEX pg_tablespace_spcname_index;

ALTER TABLE pg_authid ADD UNIQUE USING INDEX pg_authid_rolname_index;

ALTER TABLE pg_authid ADD PRIMARY KEY USING INDEX pg_authid_oid_index;

ALTER TABLE pg_auth_members ADD PRIMARY KEY USING INDEX pg_auth_members_oid_index;

ALTER TABLE pg_auth_members ADD UNIQUE USING INDEX pg_auth_members_role_member_index;

ALTER TABLE pg_auth_members ADD UNIQUE USING INDEX pg_auth_members_member_role_index;

ALTER TABLE pg_shdescription ADD PRIMARY KEY USING INDEX pg_shdescription_o_c_index;

ALTER TABLE pg_ts_config ADD UNIQUE USING INDEX pg_ts_config_cfgname_index;

ALTER TABLE pg_ts_config ADD PRIMARY KEY USING INDEX pg_ts_config_oid_index;

ALTER TABLE pg_ts_config_map ADD PRIMARY KEY USING INDEX pg_ts_config_map_index;

ALTER TABLE pg_ts_dict ADD UNIQUE USING INDEX pg_ts_dict_dictname_index;

ALTER TABLE pg_ts_dict ADD PRIMARY KEY USING INDEX pg_ts_dict_oid_index;

ALTER TABLE pg_ts_parser ADD UNIQUE USING INDEX pg_ts_parser_prsname_index;

ALTER TABLE pg_ts_parser ADD PRIMARY KEY USING INDEX pg_ts_parser_oid_index;

ALTER TABLE pg_ts_template ADD UNIQUE USING INDEX pg_ts_template_tmplname_index;

ALTER TABLE pg_ts_template ADD PRIMARY KEY USING INDEX pg_ts_template_oid_index;

ALTER TABLE pg_extension ADD PRIMARY KEY USING INDEX pg_extension_oid_index;

ALTER TABLE pg_extension ADD UNIQUE USING INDEX pg_extension_name_index;

ALTER TABLE pg_foreign_data_wrapper ADD PRIMARY KEY USING INDEX pg_foreign_data_wrapper_oid_index;

ALTER TABLE pg_foreign_data_wrapper ADD UNIQUE USING INDEX pg_foreign_data_wrapper_name_index;

ALTER TABLE pg_foreign_server ADD PRIMARY KEY USING INDEX pg_foreign_server_oid_index;

ALTER TABLE pg_foreign_server ADD UNIQUE USING INDEX pg_foreign_server_name_index;

ALTER TABLE pg_user_mapping ADD PRIMARY KEY USING INDEX pg_user_mapping_oid_index;

ALTER TABLE pg_user_mapping ADD UNIQUE USING INDEX pg_user_mapping_user_server_index;

ALTER TABLE pg_foreign_table ADD PRIMARY KEY USING INDEX pg_foreign_table_relid_index;

ALTER TABLE pg_policy ADD PRIMARY KEY USING INDEX pg_policy_oid_index;

ALTER TABLE pg_policy ADD UNIQUE USING INDEX pg_policy_polrelid_polname_index;

ALTER TABLE pg_replication_origin ADD PRIMARY KEY USING INDEX pg_replication_origin_roiident_index;

ALTER TABLE pg_replication_origin ADD UNIQUE USING INDEX pg_replication_origin_roname_index;

ALTER TABLE pg_default_acl ADD UNIQUE USING INDEX pg_default_acl_role_nsp_obj_index;

ALTER TABLE pg_default_acl ADD PRIMARY KEY USING INDEX pg_default_acl_oid_index;

ALTER TABLE pg_init_privs ADD PRIMARY KEY USING INDEX pg_init_privs_o_c_o_index;

ALTER TABLE pg_seclabel ADD PRIMARY KEY USING INDEX pg_seclabel_object_index;

ALTER TABLE pg_shseclabel ADD PRIMARY KEY USING INDEX pg_shseclabel_object_index;

ALTER TABLE pg_collation ADD UNIQUE USING INDEX pg_collation_name_enc_nsp_index;

ALTER TABLE pg_collation ADD PRIMARY KEY USING INDEX pg_collation_oid_index;

ALTER TABLE pg_parameter_acl ADD UNIQUE USING INDEX pg_parameter_acl_parname_index;

ALTER TABLE pg_parameter_acl ADD PRIMARY KEY USING INDEX pg_parameter_acl_oid_index;

ALTER TABLE pg_partitioned_table ADD PRIMARY KEY USING INDEX pg_partitioned_table_partrelid_index;

ALTER TABLE pg_range ADD PRIMARY KEY USING INDEX pg_range_rngtypid_index;

ALTER TABLE pg_range ADD UNIQUE USING INDEX pg_range_rngmultitypid_index;

ALTER TABLE pg_transform ADD PRIMARY KEY USING INDEX pg_transform_oid_index;

ALTER TABLE pg_transform ADD UNIQUE USING INDEX pg_transform_type_lang_index;

ALTER TABLE pg_sequence ADD PRIMARY KEY USING INDEX pg_sequence_seqrelid_index;

ALTER TABLE pg_publication ADD PRIMARY KEY USING INDEX pg_publication_oid_index;

ALTER TABLE pg_publication ADD UNIQUE USING INDEX pg_publication_pubname_index;

ALTER TABLE pg_publication_namespace ADD PRIMARY KEY USING INDEX pg_publication_namespace_oid_index;

ALTER TABLE pg_publication_namespace ADD UNIQUE USING INDEX pg_publication_namespace_pnnspid_pnpubid_index;

ALTER TABLE pg_publication_rel ADD PRIMARY KEY USING INDEX pg_publication_rel_oid_index;

ALTER TABLE pg_publication_rel ADD UNIQUE USING INDEX pg_publication_rel_prrelid_prpubid_index;

ALTER TABLE pg_subscription ADD PRIMARY KEY USING INDEX pg_subscription_oid_index;

ALTER TABLE pg_subscription ADD UNIQUE USING INDEX pg_subscription_subname_index;

ALTER TABLE pg_subscription_rel ADD PRIMARY KEY USING INDEX pg_subscription_rel_srrelid_srsubid_index;

