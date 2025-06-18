MATCH (t:Temp)<--
(s:FileUpload)-->
(n:FileUpload)-->
(l:ListIssue)-[r:TEMP_INDUSTRY_RANKS]->(rankTemp:Temp)


WITH t, l, COLLECT(DISTINCT {relationship: r {.category, .categoryRank, .fuelId}, tempNode: rankTemp}) AS allIndustryRanks


OPTIONAL MATCH (l)-[:USES]->(lt:ListTableSchema)

WITH t, l, allIndustryRanks, lt

OPTIONAL MATCH (lt)-[a:HAS_ATTRIBUTE]->(attr)

WHERE a.isCategoryRankValue = true

WITH t, l, allIndustryRanks, lt, COLLECT(DISTINCT attr.attributeName) AS categoryRankAttributes


WITH t, l, lt,
     CASE WHEN lt IS NULL OR size(categoryRankAttributes) = 0 
          THEN allIndustryRanks
          ELSE [rank IN allIndustryRanks 
                WHERE rank.relationship.category IN categoryRankAttributes]
     END AS industryRanks

OPTIONAL MATCH (lt)-[a:HAS_ATTRIBUTE]->(attr)

OPTIONAL MATCH (lt)-[:HAS_GROUP]->(ag:SchemaAttributeGroup)

WITH t, l, industryRanks, lt,
     COLLECT(DISTINCT {fuelId: lt.fuelId, internalName: lt.internalName})[0] AS listTable,
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
        {listId: l.fuelId},
        t,
        {industryRanks: industryRanks},
        CASE WHEN lt IS NOT NULL THEN { 
            listTableSchema: [listTable WHERE listTable.fuelId IS NOT NULL],
            schemaAttributes: [attr IN attributes WHERE attr.attributeName IS NOT NULL],
            attributeGroups: attributeGroups
        } ELSE {} END
    ])
) AS listIssueNodes


-- ////////////////////////
-- ////////////////////////
-- ////////////////////////
-- ////////////////////////
-- ////////////////////////
-- ////////////////////////
-- ////////////////////////
-- ////////////////////////
-- ////////////////////////
-- ////////////////////////
-- ////////////////////////
-- ////////////////////////



MATCH (t:Temp)
  <-- (s:FileUpload)
  --> (n:FileUpload )
  --> (l:ListIssue)
  limit 100
  RETURN l.fuelId, l.year, l.naturalId, t.nodeType

-- ////////////////////////
-- ////////////////////////
-- ////////////////////////
-- ////////////////////////
-- ////////////////////////
-- ////////////////////////
-- ////////////////////////
-- ////////////////////////
-- ////////////////////////
-- ////////////////////////
-- ////////////////////////
-- ////////////////////////

MATCH (t:Temp)
  <-- (s:FileUpload)
  --> (n:FileUpload )
  --> (l:ListIssue)
  RETURN collect(distinct {fuelId: l.fuelId, year: l.year, naturalId: l.naturalId, nodeType: t.nodeType}) as res

-- ////////////////////////
-- ////////////////////////
-- ////////////////////////
-- ////////////////////////
-- ////////////////////////
-- ////////////////////////
-- ////////////////////////
-- ////////////////////////
-- ////////////////////////
-- ////////////////////////
-- ////////////////////////
-- ////////////////////////



MATCH (t:Temp)<--
(s:FileUpload)-->
(n:FileUpload)-->
(l:ListIssue)
OPTIONAL MATCH (l)->[r:TEMP_INDUSTRY_RANKS]->(rankTemp:Temp)


WITH t, l, COLLECT(DISTINCT {relationship: r {.category, .categoryRank, .fuelId}}) AS allIndustryRanks


OPTIONAL MATCH (l)-[:USES]->(lt:ListTableSchema)
OPTIONAL MATCH (lt)-[a:HAS_ATTRIBUTE]->(attr)
OPTIONAL MATCH (lt)-[:HAS_GROUP]->(ag:SchemaAttributeGroup)


WITH t, l, allIndustryRanks, lt, COLLECT(DISTINCT case when a.isCategoryRankValue = true then attr.attributeName else null end) AS categoryRankAttributes


WITH t, l, lt,
     CASE WHEN lt IS NULL OR size(categoryRankAttributes) = 0 
          THEN allIndustryRanks
          ELSE [rank IN allIndustryRanks 
                WHERE rank.relationship.category IN categoryRankAttributes]
     END AS industryRanks



WITH t, l, industryRanks, lt,
     COLLECT(DISTINCT {fuelId: lt.fuelId, internalName: lt.internalName}) AS listTable,
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
        {listId: l.fuelId},
        t,
        {industryRanks: industryRanks},
        {listTableSchema: listTable},
        {schemaAttributes: attributes},
        {attributeGroups: attributeGroups}
    ])
) AS listIssueNodes



-- ////////////////////////
-- ////////////////////////
-- ////////////////////////
-- ////////////////////////
-- ////////////////////////
-- ////////////////////////
-- ////////////////////////
-- ////////////////////////
-- ////////////////////////
-- ////////////////////////
-- ////////////////////////
-- ////////////////////////


MATCH (t:Temp)<--
(s:FileUpload)-->
(n:FileUpload)-->
(l:ListIssue)-[r:TEMP_INDUSTRY_RANKS]->(rankTemp:Temp)


WITH t, l, COLLECT(DISTINCT {relationship: r {.category, .categoryRank, .fuelId}}) AS allIndustryRanks


OPTIONAL MATCH (l)-[:USES]->(lt:ListTableSchema)

WITH t, l, allIndustryRanks, lt

OPTIONAL MATCH (lt)-[a:HAS_ATTRIBUTE]->(attr)

WHERE a.isCategoryRankValue = true

WITH t, l, allIndustryRanks, lt, COLLECT(DISTINCT attr.attributeName) AS categoryRankAttributes


WITH t, l, lt,
     CASE WHEN lt IS NULL OR size(categoryRankAttributes) = 0 
          THEN allIndustryRanks
          ELSE [rank IN allIndustryRanks 
                WHERE rank.relationship.category IN categoryRankAttributes]
     END AS industryRanks

OPTIONAL MATCH (lt)-[a:HAS_ATTRIBUTE]->(attr)

OPTIONAL MATCH (lt)-[:HAS_GROUP]->(ag:SchemaAttributeGroup)

WITH t, l, industryRanks, lt,
     COLLECT(DISTINCT {fuelId: lt.fuelId, internalName: lt.internalName}) AS listTable,
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
        {listId: l.fuelId},
        t,
        {industryRanks: industryRanks},
        {listTableSchema: listTable},
        {schemaAttributes: attributes},
        {attributeGroups: attributeGroups}
    ])
) AS listIssueNodes



-- ////////////////////////
-- ////////////////////////
-- ////////////////////////
-- ////////////////////////
-- ////////////////////////
-- ////////////////////////
-- ////////////////////////
-- ////////////////////////
-- ////////////////////////
-- ////////////////////////
-- ////////////////////////
-- ////////////////////////




MATCH (t:Temp)<--
(s:FileUpload)-->
(n:FileUpload)-->
(l:ListIssue)

OPTIONAL MATCH (l)-[r:TEMP_INDUSTRY_RANKS]->(rankTemp:Temp)


WITH t, l, COLLECT(DISTINCT {relationship: r {.category, .categoryRank, .fuelId}}) AS allIndustryRanks


OPTIONAL MATCH (l)-[:USES]->(lt:ListTableSchema)
OPTIONAL MATCH (lt)-[a:HAS_ATTRIBUTE]->(attr)
OPTIONAL MATCH (lt)-[:HAS_GROUP]->(ag:SchemaAttributeGroup)


WITH t, l, allIndustryRanks, lt, COLLECT(DISTINCT case when a.isCategoryRankValue = true then attr.attributeName else null end) AS categoryRankAttributes


WITH t, l, lt,
     CASE WHEN lt IS NULL OR size(categoryRankAttributes) = 0 
          THEN allIndustryRanks
          ELSE [rank IN allIndustryRanks 
                WHERE rank.relationship.category IN categoryRankAttributes]
     END AS industryRanks



WITH t, l, industryRanks, lt,
     COLLECT(DISTINCT {fuelId: lt.fuelId, internalName: lt.internalName}) AS listTable,
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
        {listId: l.fuelId},
        t,
        {industryRanks: industryRanks},
        {listTableSchema: listTable},
        {schemaAttributes: attributes},
        {attributeGroups: attributeGroups}
    ])
) AS listIssueNodes