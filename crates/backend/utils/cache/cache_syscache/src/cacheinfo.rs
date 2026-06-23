//! The `cacheinfo[]` table and `SysCacheIdentifier` ids (syscache.c +
//! genbki-generated `catalog/syscache_ids.h` / `catalog/syscache_info.h`).
//!
//! Every value here — relation OID, index OID, key attribute numbers, bucket
//! count — is a compile-time catalog constant transcribed from the generated
//! header (cross-checked against the proven c2rust rendering of this unit).

use types_core::Oid;

/// `SysCacheSize` (`catalog/syscache_ids.h`).
pub const SysCacheSize: usize = 85;

/// `struct cachedesc` (syscache.c): information defining a single syscache.
#[derive(Clone, Copy, Debug)]
pub struct CacheDesc {
    /// OID of the relation being cached.
    pub reloid: Oid,
    /// OID of index relation for this cache.
    pub indoid: Oid,
    /// # of keys needed for cache lookup.
    pub nkeys: i32,
    /// attribute numbers of key attrs.
    pub key: [i32; 4],
    /// number of hash buckets for this cache.
    pub nbuckets: i32,
}

// `enum SysCacheIdentifier` (`catalog/syscache_ids.h`), one per cache.
pub const AGGFNOID: i32 = 0;
pub const AMNAME: i32 = 1;
pub const AMOID: i32 = 2;
pub const AMOPOPID: i32 = 3;
pub const AMOPSTRATEGY: i32 = 4;
pub const AMPROCNUM: i32 = 5;
pub const ATTNAME: i32 = 6;
pub const ATTNUM: i32 = 7;
pub const AUTHMEMMEMROLE: i32 = 8;
pub const AUTHMEMROLEMEM: i32 = 9;
pub const AUTHNAME: i32 = 10;
pub const AUTHOID: i32 = 11;
pub const CASTSOURCETARGET: i32 = 12;
pub const CLAAMNAMENSP: i32 = 13;
pub const CLAOID: i32 = 14;
pub const COLLNAMEENCNSP: i32 = 15;
pub const COLLOID: i32 = 16;
pub const CONDEFAULT: i32 = 17;
pub const CONNAMENSP: i32 = 18;
pub const CONSTROID: i32 = 19;
pub const CONVOID: i32 = 20;
pub const DATABASEOID: i32 = 21;
pub const DEFACLROLENSPOBJ: i32 = 22;
pub const ENUMOID: i32 = 23;
pub const ENUMTYPOIDNAME: i32 = 24;
pub const EVENTTRIGGERNAME: i32 = 25;
pub const EVENTTRIGGEROID: i32 = 26;
pub const EXTENSIONNAME: i32 = 27;
pub const EXTENSIONOID: i32 = 28;
pub const FOREIGNDATAWRAPPERNAME: i32 = 29;
pub const FOREIGNDATAWRAPPEROID: i32 = 30;
pub const FOREIGNSERVERNAME: i32 = 31;
pub const FOREIGNSERVEROID: i32 = 32;
pub const FOREIGNTABLEREL: i32 = 33;
pub const INDEXRELID: i32 = 34;
pub const LANGNAME: i32 = 35;
pub const LANGOID: i32 = 36;
pub const NAMESPACENAME: i32 = 37;
pub const NAMESPACEOID: i32 = 38;
pub const OPERNAMENSP: i32 = 39;
pub const OPEROID: i32 = 40;
pub const OPFAMILYAMNAMENSP: i32 = 41;
pub const OPFAMILYOID: i32 = 42;
pub const PARAMETERACLNAME: i32 = 43;
pub const PARAMETERACLOID: i32 = 44;
pub const PARTRELID: i32 = 45;
pub const PROCNAMEARGSNSP: i32 = 46;
pub const PROCOID: i32 = 47;
pub const PUBLICATIONNAME: i32 = 48;
pub const PUBLICATIONNAMESPACE: i32 = 49;
pub const PUBLICATIONNAMESPACEMAP: i32 = 50;
pub const PUBLICATIONOID: i32 = 51;
pub const PUBLICATIONREL: i32 = 52;
pub const PUBLICATIONRELMAP: i32 = 53;
pub const RANGEMULTIRANGE: i32 = 54;
pub const RANGETYPE: i32 = 55;
pub const RELNAMENSP: i32 = 56;
pub const RELOID: i32 = 57;
pub const REPLORIGIDENT: i32 = 58;
pub const REPLORIGNAME: i32 = 59;
pub const RULERELNAME: i32 = 60;
pub const SEQRELID: i32 = 61;
pub const STATEXTDATASTXOID: i32 = 62;
pub const STATEXTNAMENSP: i32 = 63;
pub const STATEXTOID: i32 = 64;
pub const STATRELATTINH: i32 = 65;
pub const SUBSCRIPTIONNAME: i32 = 66;
pub const SUBSCRIPTIONOID: i32 = 67;
pub const SUBSCRIPTIONRELMAP: i32 = 68;
pub const TABLESPACEOID: i32 = 69;
pub const TRFOID: i32 = 70;
pub const TRFTYPELANG: i32 = 71;
pub const TSCONFIGMAP: i32 = 72;
pub const TSCONFIGNAMENSP: i32 = 73;
pub const TSCONFIGOID: i32 = 74;
pub const TSDICTNAMENSP: i32 = 75;
pub const TSDICTOID: i32 = 76;
pub const TSPARSERNAMENSP: i32 = 77;
pub const TSPARSEROID: i32 = 78;
pub const TSTEMPLATENAMENSP: i32 = 79;
pub const TSTEMPLATEOID: i32 = 80;
pub const TYPENAMENSP: i32 = 81;
pub const TYPEOID: i32 = 82;
pub const USERMAPPINGOID: i32 = 83;
pub const USERMAPPINGUSERSERVER: i32 = 84;

/// `cacheinfo[]` (syscache.c via `catalog/syscache_info.h`), indexed by
/// `SysCacheIdentifier`.
pub const cacheinfo: [CacheDesc; SysCacheSize] = [
    // AGGFNOID: AggregateRelationId / AggregateFnoidIndexId
    CacheDesc { reloid: 2600, indoid: 2650, nkeys: 1, key: [1, 0, 0, 0], nbuckets: 16 },
    // AMNAME: AccessMethodRelationId / AmNameIndexId
    CacheDesc { reloid: 2601, indoid: 2651, nkeys: 1, key: [2, 0, 0, 0], nbuckets: 4 },
    // AMOID: AccessMethodRelationId / AmOidIndexId
    CacheDesc { reloid: 2601, indoid: 2652, nkeys: 1, key: [1, 0, 0, 0], nbuckets: 4 },
    // AMOPOPID: AccessMethodOperatorRelationId / AccessMethodOperatorIndexId
    CacheDesc { reloid: 2602, indoid: 2654, nkeys: 3, key: [7, 6, 2, 0], nbuckets: 64 },
    // AMOPSTRATEGY: AccessMethodOperatorRelationId / AccessMethodStrategyIndexId
    CacheDesc { reloid: 2602, indoid: 2653, nkeys: 4, key: [2, 3, 4, 5], nbuckets: 64 },
    // AMPROCNUM: AccessMethodProcedureRelationId / AccessMethodProcedureIndexId
    CacheDesc { reloid: 2603, indoid: 2655, nkeys: 4, key: [2, 3, 4, 5], nbuckets: 16 },
    // ATTNAME: AttributeRelationId / AttributeRelidNameIndexId
    CacheDesc { reloid: 1249, indoid: 2658, nkeys: 2, key: [1, 2, 0, 0], nbuckets: 32 },
    // ATTNUM: AttributeRelationId / AttributeRelidNumIndexId
    CacheDesc { reloid: 1249, indoid: 2659, nkeys: 2, key: [1, 5, 0, 0], nbuckets: 128 },
    // AUTHMEMMEMROLE: AuthMemRelationId / AuthMemMemRoleIndexId
    CacheDesc { reloid: 1261, indoid: 2695, nkeys: 3, key: [3, 2, 4, 0], nbuckets: 8 },
    // AUTHMEMROLEMEM: AuthMemRelationId / AuthMemRoleMemIndexId
    CacheDesc { reloid: 1261, indoid: 2694, nkeys: 3, key: [2, 3, 4, 0], nbuckets: 8 },
    // AUTHNAME: AuthIdRelationId / AuthIdRolnameIndexId
    CacheDesc { reloid: 1260, indoid: 2676, nkeys: 1, key: [2, 0, 0, 0], nbuckets: 8 },
    // AUTHOID: AuthIdRelationId / AuthIdOidIndexId
    CacheDesc { reloid: 1260, indoid: 2677, nkeys: 1, key: [1, 0, 0, 0], nbuckets: 8 },
    // CASTSOURCETARGET: CastRelationId / CastSourceTargetIndexId
    CacheDesc { reloid: 2605, indoid: 2661, nkeys: 2, key: [2, 3, 0, 0], nbuckets: 256 },
    // CLAAMNAMENSP: OperatorClassRelationId / OpclassAmNameNspIndexId
    CacheDesc { reloid: 2616, indoid: 2686, nkeys: 3, key: [2, 3, 4, 0], nbuckets: 8 },
    // CLAOID: OperatorClassRelationId / OpclassOidIndexId
    CacheDesc { reloid: 2616, indoid: 2687, nkeys: 1, key: [1, 0, 0, 0], nbuckets: 8 },
    // COLLNAMEENCNSP: CollationRelationId / CollationNameEncNspIndexId
    CacheDesc { reloid: 3456, indoid: 3164, nkeys: 3, key: [2, 7, 3, 0], nbuckets: 8 },
    // COLLOID: CollationRelationId / CollationOidIndexId
    CacheDesc { reloid: 3456, indoid: 3085, nkeys: 1, key: [1, 0, 0, 0], nbuckets: 8 },
    // CONDEFAULT: ConversionRelationId / ConversionDefaultIndexId
    CacheDesc { reloid: 2607, indoid: 2668, nkeys: 4, key: [3, 5, 6, 1], nbuckets: 8 },
    // CONNAMENSP: ConversionRelationId / ConversionNameNspIndexId
    CacheDesc { reloid: 2607, indoid: 2669, nkeys: 2, key: [2, 3, 0, 0], nbuckets: 8 },
    // CONSTROID: ConstraintRelationId / ConstraintOidIndexId
    CacheDesc { reloid: 2606, indoid: 2667, nkeys: 1, key: [1, 0, 0, 0], nbuckets: 16 },
    // CONVOID: ConversionRelationId / ConversionOidIndexId
    CacheDesc { reloid: 2607, indoid: 2670, nkeys: 1, key: [1, 0, 0, 0], nbuckets: 8 },
    // DATABASEOID: DatabaseRelationId / DatabaseOidIndexId
    CacheDesc { reloid: 1262, indoid: 2672, nkeys: 1, key: [1, 0, 0, 0], nbuckets: 4 },
    // DEFACLROLENSPOBJ: DefaultAclRelationId / DefaultAclRoleNspObjIndexId
    CacheDesc { reloid: 826, indoid: 827, nkeys: 3, key: [2, 3, 4, 0], nbuckets: 8 },
    // ENUMOID: EnumRelationId / EnumOidIndexId
    CacheDesc { reloid: 3501, indoid: 3502, nkeys: 1, key: [1, 0, 0, 0], nbuckets: 8 },
    // ENUMTYPOIDNAME: EnumRelationId / EnumTypIdLabelIndexId
    CacheDesc { reloid: 3501, indoid: 3503, nkeys: 2, key: [2, 4, 0, 0], nbuckets: 8 },
    // EVENTTRIGGERNAME: EventTriggerRelationId / EventTriggerNameIndexId
    CacheDesc { reloid: 3466, indoid: 3467, nkeys: 1, key: [2, 0, 0, 0], nbuckets: 8 },
    // EVENTTRIGGEROID: EventTriggerRelationId / EventTriggerOidIndexId
    CacheDesc { reloid: 3466, indoid: 3468, nkeys: 1, key: [1, 0, 0, 0], nbuckets: 8 },
    // EXTENSIONNAME: ExtensionRelationId / ExtensionNameIndexId
    CacheDesc { reloid: 3079, indoid: 3081, nkeys: 1, key: [2, 0, 0, 0], nbuckets: 2 },
    // EXTENSIONOID: ExtensionRelationId / ExtensionOidIndexId
    CacheDesc { reloid: 3079, indoid: 3080, nkeys: 1, key: [1, 0, 0, 0], nbuckets: 2 },
    // FOREIGNDATAWRAPPERNAME: ForeignDataWrapperRelationId / ForeignDataWrapperNameIndexId
    CacheDesc { reloid: 2328, indoid: 548, nkeys: 1, key: [2, 0, 0, 0], nbuckets: 2 },
    // FOREIGNDATAWRAPPEROID: ForeignDataWrapperRelationId / ForeignDataWrapperOidIndexId
    CacheDesc { reloid: 2328, indoid: 112, nkeys: 1, key: [1, 0, 0, 0], nbuckets: 2 },
    // FOREIGNSERVERNAME: ForeignServerRelationId / ForeignServerNameIndexId
    CacheDesc { reloid: 1417, indoid: 549, nkeys: 1, key: [2, 0, 0, 0], nbuckets: 2 },
    // FOREIGNSERVEROID: ForeignServerRelationId / ForeignServerOidIndexId
    CacheDesc { reloid: 1417, indoid: 113, nkeys: 1, key: [1, 0, 0, 0], nbuckets: 2 },
    // FOREIGNTABLEREL: ForeignTableRelationId / ForeignTableRelidIndexId
    CacheDesc { reloid: 3118, indoid: 3119, nkeys: 1, key: [1, 0, 0, 0], nbuckets: 4 },
    // INDEXRELID: IndexRelationId / IndexRelidIndexId
    CacheDesc { reloid: 2610, indoid: 2679, nkeys: 1, key: [1, 0, 0, 0], nbuckets: 64 },
    // LANGNAME: LanguageRelationId / LanguageNameIndexId
    CacheDesc { reloid: 2612, indoid: 2681, nkeys: 1, key: [2, 0, 0, 0], nbuckets: 4 },
    // LANGOID: LanguageRelationId / LanguageOidIndexId
    CacheDesc { reloid: 2612, indoid: 2682, nkeys: 1, key: [1, 0, 0, 0], nbuckets: 4 },
    // NAMESPACENAME: NamespaceRelationId / NamespaceNameIndexId
    CacheDesc { reloid: 2615, indoid: 2684, nkeys: 1, key: [2, 0, 0, 0], nbuckets: 4 },
    // NAMESPACEOID: NamespaceRelationId / NamespaceOidIndexId
    CacheDesc { reloid: 2615, indoid: 2685, nkeys: 1, key: [1, 0, 0, 0], nbuckets: 16 },
    // OPERNAMENSP: OperatorRelationId / OperatorNameNspIndexId
    CacheDesc { reloid: 2617, indoid: 2689, nkeys: 4, key: [2, 8, 9, 3], nbuckets: 256 },
    // OPEROID: OperatorRelationId / OperatorOidIndexId
    CacheDesc { reloid: 2617, indoid: 2688, nkeys: 1, key: [1, 0, 0, 0], nbuckets: 32 },
    // OPFAMILYAMNAMENSP: OperatorFamilyRelationId / OpfamilyAmNameNspIndexId
    CacheDesc { reloid: 2753, indoid: 2754, nkeys: 3, key: [2, 3, 4, 0], nbuckets: 8 },
    // OPFAMILYOID: OperatorFamilyRelationId / OpfamilyOidIndexId
    CacheDesc { reloid: 2753, indoid: 2755, nkeys: 1, key: [1, 0, 0, 0], nbuckets: 8 },
    // PARAMETERACLNAME: ParameterAclRelationId / ParameterAclParnameIndexId
    CacheDesc { reloid: 6243, indoid: 6246, nkeys: 1, key: [2, 0, 0, 0], nbuckets: 4 },
    // PARAMETERACLOID: ParameterAclRelationId / ParameterAclOidIndexId
    CacheDesc { reloid: 6243, indoid: 6247, nkeys: 1, key: [1, 0, 0, 0], nbuckets: 4 },
    // PARTRELID: PartitionedRelationId / PartitionedRelidIndexId
    CacheDesc { reloid: 3350, indoid: 3351, nkeys: 1, key: [1, 0, 0, 0], nbuckets: 32 },
    // PROCNAMEARGSNSP: ProcedureRelationId / ProcedureNameArgsNspIndexId
    CacheDesc { reloid: 1255, indoid: 2691, nkeys: 3, key: [2, 20, 3, 0], nbuckets: 128 },
    // PROCOID: ProcedureRelationId / ProcedureOidIndexId
    CacheDesc { reloid: 1255, indoid: 2690, nkeys: 1, key: [1, 0, 0, 0], nbuckets: 128 },
    // PUBLICATIONNAME: PublicationRelationId / PublicationNameIndexId
    CacheDesc { reloid: 6104, indoid: 6111, nkeys: 1, key: [2, 0, 0, 0], nbuckets: 8 },
    // PUBLICATIONNAMESPACE: PublicationNamespaceRelationId / PublicationNamespaceObjectIndexId
    CacheDesc { reloid: 6237, indoid: 6238, nkeys: 1, key: [1, 0, 0, 0], nbuckets: 64 },
    // PUBLICATIONNAMESPACEMAP: PublicationNamespaceRelationId / PublicationNamespacePnnspidPnpubidIndexId
    CacheDesc { reloid: 6237, indoid: 6239, nkeys: 2, key: [3, 2, 0, 0], nbuckets: 64 },
    // PUBLICATIONOID: PublicationRelationId / PublicationObjectIndexId
    CacheDesc { reloid: 6104, indoid: 6110, nkeys: 1, key: [1, 0, 0, 0], nbuckets: 8 },
    // PUBLICATIONREL: PublicationRelRelationId / PublicationRelObjectIndexId
    CacheDesc { reloid: 6106, indoid: 6112, nkeys: 1, key: [1, 0, 0, 0], nbuckets: 64 },
    // PUBLICATIONRELMAP: PublicationRelRelationId / PublicationRelPrrelidPrpubidIndexId
    CacheDesc { reloid: 6106, indoid: 6113, nkeys: 2, key: [3, 2, 0, 0], nbuckets: 64 },
    // RANGEMULTIRANGE: RangeRelationId / RangeMultirangeTypidIndexId
    CacheDesc { reloid: 3541, indoid: 2228, nkeys: 1, key: [3, 0, 0, 0], nbuckets: 4 },
    // RANGETYPE: RangeRelationId / RangeTypidIndexId
    CacheDesc { reloid: 3541, indoid: 3542, nkeys: 1, key: [1, 0, 0, 0], nbuckets: 4 },
    // RELNAMENSP: RelationRelationId / ClassNameNspIndexId
    CacheDesc { reloid: 1259, indoid: 2663, nkeys: 2, key: [2, 3, 0, 0], nbuckets: 128 },
    // RELOID: RelationRelationId / ClassOidIndexId
    CacheDesc { reloid: 1259, indoid: 2662, nkeys: 1, key: [1, 0, 0, 0], nbuckets: 128 },
    // REPLORIGIDENT: ReplicationOriginRelationId / ReplicationOriginIdentIndex
    CacheDesc { reloid: 6000, indoid: 6001, nkeys: 1, key: [1, 0, 0, 0], nbuckets: 16 },
    // REPLORIGNAME: ReplicationOriginRelationId / ReplicationOriginNameIndex
    CacheDesc { reloid: 6000, indoid: 6002, nkeys: 1, key: [2, 0, 0, 0], nbuckets: 16 },
    // RULERELNAME: RewriteRelationId / RewriteRelRulenameIndexId
    CacheDesc { reloid: 2618, indoid: 2693, nkeys: 2, key: [3, 2, 0, 0], nbuckets: 8 },
    // SEQRELID: SequenceRelationId / SequenceRelidIndexId
    CacheDesc { reloid: 2224, indoid: 5002, nkeys: 1, key: [1, 0, 0, 0], nbuckets: 32 },
    // STATEXTDATASTXOID: StatisticExtDataRelationId / StatisticExtDataStxoidInhIndexId
    CacheDesc { reloid: 3429, indoid: 3433, nkeys: 2, key: [1, 2, 0, 0], nbuckets: 4 },
    // STATEXTNAMENSP: StatisticExtRelationId / StatisticExtNameIndexId
    CacheDesc { reloid: 3381, indoid: 3997, nkeys: 2, key: [3, 4, 0, 0], nbuckets: 4 },
    // STATEXTOID: StatisticExtRelationId / StatisticExtOidIndexId
    CacheDesc { reloid: 3381, indoid: 3380, nkeys: 1, key: [1, 0, 0, 0], nbuckets: 4 },
    // STATRELATTINH: StatisticRelationId / StatisticRelidAttnumInhIndexId
    CacheDesc { reloid: 2619, indoid: 2696, nkeys: 3, key: [1, 2, 3, 0], nbuckets: 128 },
    // SUBSCRIPTIONNAME: SubscriptionRelationId / SubscriptionNameIndexId
    CacheDesc { reloid: 6100, indoid: 6115, nkeys: 2, key: [2, 4, 0, 0], nbuckets: 4 },
    // SUBSCRIPTIONOID: SubscriptionRelationId / SubscriptionObjectIndexId
    CacheDesc { reloid: 6100, indoid: 6114, nkeys: 1, key: [1, 0, 0, 0], nbuckets: 4 },
    // SUBSCRIPTIONRELMAP: SubscriptionRelRelationId / SubscriptionRelSrrelidSrsubidIndexId
    CacheDesc { reloid: 6102, indoid: 6117, nkeys: 2, key: [2, 1, 0, 0], nbuckets: 64 },
    // TABLESPACEOID: TableSpaceRelationId / TablespaceOidIndexId
    CacheDesc { reloid: 1213, indoid: 2697, nkeys: 1, key: [1, 0, 0, 0], nbuckets: 4 },
    // TRFOID: TransformRelationId / TransformOidIndexId
    CacheDesc { reloid: 3576, indoid: 3574, nkeys: 1, key: [1, 0, 0, 0], nbuckets: 16 },
    // TRFTYPELANG: TransformRelationId / TransformTypeLangIndexId
    CacheDesc { reloid: 3576, indoid: 3575, nkeys: 2, key: [2, 3, 0, 0], nbuckets: 16 },
    // TSCONFIGMAP: TSConfigMapRelationId / TSConfigMapIndexId
    CacheDesc { reloid: 3603, indoid: 3609, nkeys: 3, key: [1, 2, 3, 0], nbuckets: 2 },
    // TSCONFIGNAMENSP: TSConfigRelationId / TSConfigNameNspIndexId
    CacheDesc { reloid: 3602, indoid: 3608, nkeys: 2, key: [2, 3, 0, 0], nbuckets: 2 },
    // TSCONFIGOID: TSConfigRelationId / TSConfigOidIndexId
    CacheDesc { reloid: 3602, indoid: 3712, nkeys: 1, key: [1, 0, 0, 0], nbuckets: 2 },
    // TSDICTNAMENSP: TSDictionaryRelationId / TSDictionaryNameNspIndexId
    CacheDesc { reloid: 3600, indoid: 3604, nkeys: 2, key: [2, 3, 0, 0], nbuckets: 2 },
    // TSDICTOID: TSDictionaryRelationId / TSDictionaryOidIndexId
    CacheDesc { reloid: 3600, indoid: 3605, nkeys: 1, key: [1, 0, 0, 0], nbuckets: 2 },
    // TSPARSERNAMENSP: TSParserRelationId / TSParserNameNspIndexId
    CacheDesc { reloid: 3601, indoid: 3606, nkeys: 2, key: [2, 3, 0, 0], nbuckets: 2 },
    // TSPARSEROID: TSParserRelationId / TSParserOidIndexId
    CacheDesc { reloid: 3601, indoid: 3607, nkeys: 1, key: [1, 0, 0, 0], nbuckets: 2 },
    // TSTEMPLATENAMENSP: TSTemplateRelationId / TSTemplateNameNspIndexId
    CacheDesc { reloid: 3764, indoid: 3766, nkeys: 2, key: [2, 3, 0, 0], nbuckets: 2 },
    // TSTEMPLATEOID: TSTemplateRelationId / TSTemplateOidIndexId
    CacheDesc { reloid: 3764, indoid: 3767, nkeys: 1, key: [1, 0, 0, 0], nbuckets: 2 },
    // TYPENAMENSP: TypeRelationId / TypeNameNspIndexId
    CacheDesc { reloid: 1247, indoid: 2704, nkeys: 2, key: [2, 3, 0, 0], nbuckets: 64 },
    // TYPEOID: TypeRelationId / TypeOidIndexId
    CacheDesc { reloid: 1247, indoid: 2703, nkeys: 1, key: [1, 0, 0, 0], nbuckets: 64 },
    // USERMAPPINGOID: UserMappingRelationId / UserMappingOidIndexId
    CacheDesc { reloid: 1418, indoid: 174, nkeys: 1, key: [1, 0, 0, 0], nbuckets: 2 },
    // USERMAPPINGUSERSERVER: UserMappingRelationId / UserMappingUserServerIndexId
    CacheDesc { reloid: 1418, indoid: 175, nkeys: 2, key: [2, 3, 0, 0], nbuckets: 2 },
];
