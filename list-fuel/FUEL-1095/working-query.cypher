
MATCH (t:Temp )
<-- (s:FileUpload)
--> (n:FileUpload {fuelId: 'e457ef52-346a-4e40-8789-61dbc5d2e257'})
--> (l:ListIssue {naturalId: '..canada-best-employers-diversity2026', year: 2026})
OPTIONAL MATCh (l)-[r:TEMP_INDUSTRY_RANKS]->(temp:Temp)

WITH t, s, n, l, COLLECT(DISTINCT r {.category, .categoryRank, .fuelId}) AS industryRanks

OPTIONAL MATCH (l)-[:USES]->(lt:ListTableSchema)
OPTIONAL MATCH (lt)-[a:HAS_ATTRIBUTE]->(attr)
OPTIONAL MATCH (lt)-[:HAS_GROUP]->(ag:SchemaAttributeGroup)

with t, s, n,l,industryRanks,
[item IN COLLECT(DISTINCT {
      fuelId: lt.fuelId,
      internalName: lt.internalName
}) WHERE item.fuelId IS NOT NULL] AS listTable,

[item IN COLLECT(DISTINCT {
fuelId: attr.fuelId,
attributeName: attr.attributeName,
attributeType: attr.attributeType,
isRequired: attr.isRequired,
fuelId: attr.fuelId,
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
}) WHERE item.attributeName IS NOT NULL] AS attributes,

COLLECT(DISTINCT ag {
      .isDerived,
      .validationRules,
      .schemaGroupId,
      .schemaGroupName,
      .schemaGroupFormat
}) AS attributeGroups

with t,l,industryRanks,
CASE WHEN size(listTable) > 0 THEN { listTableSchema: listTable } ELSE {} END AS listTableSchemaPart,
{ schemaAttributes: attributes } AS schemaAttributesPart,
CASE WHEN size(attributeGroups) > 0 THEN { attributeGroups: attributeGroups } ELSE {} END AS attributeGroupsPart
limit 50
RETURN COLLECT(
apoc.map.mergeList([
      {listId: l.fuelId},
      t,
      {industryRanks: industryRanks},
      listTableSchemaPart,
      schemaAttributesPart,
      attributeGroupsPart
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

MATCH (t:Temp )
<-- (s:FileUpload)
--> (n:FileUpload {fuelId: 'e457ef52-346a-4e40-8789-61dbc5d2e257'})
--> (l:ListIssue {naturalId: 'diab2025', year: 2025})

OPTIONAL MATCh (l)-[r:TEMP_INDUSTRY_RANKS]->(temp:Temp)

WITH t, l, COLLECT(DISTINCT r {.category, .categoryRank, .fuelId}) AS industryRanks


Match (l:ListIssue {listUri:"diab", year: 2025})-[:USES]->(lt:ListTableSchema)
match (lt)-[r]->(g)

with t,l,industryRanks, 
[item IN COLLECT(DISTINCT {
      fuelId: lt.fuelId,
      internalName: lt.internalName,
      attributeGroups: [item IN COLLECT(DISTINCT {
                        groupOrder: r.groupOrder,
                        isFilterable: r.isFilterable,
                        groupIsDefault: r.groupIsDefault,
                        groupId: r.groupId,
                        isUnique: r.isUnique,
                        isSortable: r.isSortable,
                        isAutoIncrement: r.isAutoIncrement,
                        isReadOnly: r.isReadOnly,
                        isImportable: r.isImportable,
                        isSearchable: r.isSearchable,
                        minimumRoleEdit: r.minimumRoleEdit,
                        order: r.order,
                        groupIsHidden: r.groupIsHidden,
                        displayLabel: r.displayLabel,
                        isRequired: r.isRequired,
                        minimumRoleImport: r.minimumRoleImport,
                        groupLabel: r.groupLabel,
                        isPrimaryKey: r.isPrimaryKey,
                        groupIsCollapsed: r.groupIsCollapsed,
                        isCategoryRankSlug: r.isCategoryRankSlug,
                        minimumRoleExport: r.minimumRoleExport,
                        isHidden: r.isHidden,
                        minimumRoleView: r.minimumRoleView,
                        isExportable: r.isExportable,
                        isCategoryRankValue: r.isCategoryRankValue,
                        groupDescription: r.groupDescription,
                        isNullable: r.isNullable,
                        isSecondaryKey: r.isSecondaryKey,
                        isCategoryRankLabel: r.isCategoryRankLabel
                        }) WHERE item.attributeName IS NOT NULL]
}) WHERE item.fuelId IS NOT NULL] AS listTable,

[item IN COLLECT(DISTINCT {
   fuelId: g.fuelId,
   attributeName: g.attributeName,
   attributeType: g.attributeType,
   isRequired: g.isRequired,
   fuelId: g.fuelId})] as schemaAttribute,

COLLECT(DISTINCT g {
      .isDerived,
      .validationRules,
      .schemaGroupId,
      .schemaGroupName,
      .schemaGroupFormat
}) AS attributeGroups

limit 50
RETURN COLLECT(
apoc.map.mergeList([
      {listId: l.fuelId},
      t,
      {industryRanks: industryRanks},
      listTable,
      schemaAttribute,
      attributeGroups
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







MATCH (t:Temp)
<-- (s:FileUpload)
--> (n:FileUpload {fuelId: 'e457ef52-346a-4e40-8789-61dbc5d2e257'})
--> (l:ListIssue {naturalId: '..canada-best-employers-diversity2026', year: 2026})

OPTIONAL MATCH (l)-[r:TEMP_INDUSTRY_RANKS]->(temp:Temp)

WITH t, l, COLLECT(DISTINCT r {.category, .categoryRank, .fuelId}) AS industryRanks

OPTIONAL MATCH (l)-[:USES]->(lt:ListTableSchema)
OPTIONAL MATCH (lt)-[r]->(g)

WITH t, l, industryRanks, lt, r, g
WITH t, l, industryRanks,
     COLLECT(DISTINCT {
       listTableInfo: {
         fuelId: lt.fuelId,
         internalName: lt.internalName
       },
       attributeGroupsInfo: {
         groupOrder: r.groupOrder,
         isFilterable: r.isFilterable,
         groupIsDefault: r.groupIsDefault,
         groupId: r.groupId,
         isUnique: r.isUnique,
         isSortable: r.isSortable,
         isAutoIncrement: r.isAutoIncrement,
         isReadOnly: r.isReadOnly,
         isImportable: r.isImportable,
         isSearchable: r.isSearchable,
         minimumRoleEdit: r.minimumRoleEdit,
         order: r.order,
         groupIsHidden: r.groupIsHidden,
         displayLabel: r.displayLabel,
         isRequired: r.isRequired,
         minimumRoleImport: r.minimumRoleImport,
         groupLabel: r.groupLabel,
         isPrimaryKey: r.isPrimaryKey,
         groupIsCollapsed: r.groupIsCollapsed,
         isCategoryRankSlug: r.isCategoryRankSlug,
         minimumRoleExport: r.minimumRoleExport,
         isHidden: r.isHidden,
         minimumRoleView: r.minimumRoleView,
         isExportable: r.isExportable,
         isCategoryRankValue: r.isCategoryRankValue,
         groupDescription: r.groupDescription,
         isNullable: r.isNullable,
         isSecondaryKey: r.isSecondaryKey,
         isCategoryRankLabel: r.isCategoryRankLabel
       },
       schemaAttributeInfo: {
         fuelId: g.fuelId,
         attributeName: g.attributeName
      attributeType: g.attributeType,
      isRequired: g.isRequired,
       }
     }) AS combinedData

UNWIND combinedData AS data
RETURN COLLECT(apoc.map.mergeList([
      {listId: l.fuelId},
      t,
      {industryRanks: industryRanks},
      data.listTableInfo,
      data.schemaAttributeInfo,
      data.attributeGroupsInfo
])) AS listIssueNodes
LIMIT 50