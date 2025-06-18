
MATCH (t:Temp)<--(s:FileUpload)-->(n:FileUpload)-->(l:ListIssue)

OPTIONAL MATCH (l)-[r:TEMP_INDUSTRY_RANKS]->(rankTemp:Temp)
WITH t, l, COLLECT(DISTINCT {relationship: r {.category, .categoryRank, .fuelId}}) AS allIndustryRanks



OPTIONAL MATCH (l)-[:USES]->(lt:ListTableSchema)
OPTIONAL MATCH (lt)-[a:HAS_ATTRIBUTE]->(attr)
OPTIONAL MATCH (lt)-[:HAS_GROUP]->(ag:SchemaAttributeGroup)




WITH 
    t, l, lt, allIndustryRanks, a, attr, ag,
    COLLECT(DISTINCT CASE WHEN a.isCategoryRankValue = true THEN attr.attributeName ELSE NULL END) AS categoryRankAttributes


WITH 
    t, l, lt, a, attr, ag, categoryRankAttributes,
    CASE 
        WHEN lt IS NULL OR size(categoryRankAttributes) = 0 
        THEN allIndustryRanks
        ELSE [rank IN allIndustryRanks 
              WHERE rank.relationship.category IN categoryRankAttributes]
    END AS industryRanks



WITH 
    t, l, industryRanks, lt,
    COLLECT(DISTINCT {
        fuelId: lt.fuelId, 
        internalName: lt.internalName
    }) AS listTable,
    
    COLLECT(DISTINCT {
        fuelId: attr.fuelId,
        attributeName: attr.attributeName,
        attributeType: attr.attributeType,
        groupOrder: a.groupOrder,
        isFilterable: a.isFilterable,
        groupIsDefault: a.groupIsDefault,
        groupId: a.groupId,
        isUnique: a.isUnique,
        isSortable: a.isSortable,
        isAutoIncrement: a.isAutoIncrement,
        isReadOnly: a.isReadOnly,
        isImportable: a.isImportable,
        isSearchable: a.isSearchable,
        minimumRoleEdit: a.minimumRoleEdit,
        order: a.order,
        groupIsHidden: a.groupIsHidden,
        displayLabel: a.displayLabel,
        isRequired: a.isRequired,
        minimumRoleImport: a.minimumRoleImport,
        groupLabel: a.groupLabel,
        isPrimaryKey: a.isPrimaryKey,
        groupIsCollapsed: a.groupIsCollapsed,
        isCategoryRankSlug: a.isCategoryRankSlug,
        minimumRoleExport: a.minimumRoleExport,
        isHidden: a.isHidden,
        minimumRoleView: a.minimumRoleView,
        isExportable: a.isExportable,
        isCategoryRankValue: a.isCategoryRankValue,
        groupDescription: a.groupDescription,
        isNullable: a.isNullable,
        isSecondaryKey: a.isSecondaryKey,
        isCategoryRankLabel: a.isCategoryRankLabel
    }) AS attributes,
    
    COLLECT(DISTINCT ag {
        .isDerived,
        .validationRules,
        .schemaGroupId,
        .schemaGroupName,
        .schemaGroupFormat
    }) AS attributeGroups



RETURN COLLECT(
    apoc.map.mergeList([
        { listId: l.fuelId },
        t,
        { industryRanks: industryRanks },
        { listTableSchema: listTable },
        { schemaAttributes: attributes },
        { attributeGroups: attributeGroups }
    ])
) AS listIssueNodes
