MATCH (n:ListIssue) 
WHERE toLower(n.name) CONTAINS toLower("women") and n.year=2024 
WITH n, n.listType as listTypes

OPTIONAL MATCH (n)-[:INDUSTRY_RANKS]->(ir:IndustryRank)
OPTIONAL MATCH (n)-[:USES]->(lt:ListTableSchema)

OPTIONAL MATCH (lt)-[a:HAS_ATTRIBUTE]->(attr)
OPTIONAL MATCH (lt)-[:HAS_GROUP]->(group:SchemaAttributeGroup)

WITH n, listTypes, 
     CASE WHEN count(ir) > 0 THEN true ELSE false END as hasIndustryRanks,
    COLLECT(DISTINCT {  // ListTable schema
        fuelId: lt.fuelId,
        internalName: lt.internalName
     }) AS listTable,
     COLLECT(DISTINCT {
        attrFuelId: attr.fuelId,
        attrName: attr.internalName,
        attrType: attr.attributeType,
        groupOrder: a.groupOrder,
        isFilterable: a.isFilterable,
        groupIsDefault: a.groupIsDefault,
        groupId: a.groupId,
        isUnique: a.isUnique,
        isSortable: a.isSortable,
        isAutoIncrement: a.isAutoIncrement,
        isReadOnly: a.isReadOnly,
        isImportable: a.isImportable,
        attributeType: a.attributeType,
        isSearchable: a.isSearchable,
        minimumRoleEdit: a.minimumRoleEdit,
        fuelId: a.fuelId,
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
     group{.isDerived,.validationRules, .schemaGroupId, .schemaGroupName, .schemaGroupFormat}

ORDER BY n.date DESC
SKIP 0 LIMIT 100

RETURN COLLECT({
    allowPremiumProfiles: n.allowPremiumProfiles,
    date: n.date,
    embargo: n.embargo,
    fuelId: n.fuelId,
    hasIndustryRanks: hasIndustryRanks,
    issueDate: n.issueDate,
    lastModifiedInputId: n.lastModifiedInputId,
    lastModifiedTimestamp: n.lastModifiedTimestamp,
    lastModifiedUser: n.lastModifiedUser,
    listType: n.listType,
    listUri: n.listUri,
    month: n.month,
    name: n.name,
    naturalId: n.naturalId,
    pageTitle: n.pageTitle,
    status: n.status,
    templateFields: n.templateFields,
    year: n.year,
    listTableSchema: listTable,
    schemaAttributes: attributes,
    attributeGroups: group
}) AS listIssueNodes





-- /////////////////////////////////////////////////////////////
-- /////////////////////////////////////////////////////////////
-- /////////////////////////////////////////////////////////////
-- /////////////////////////////////////////////////////////////
-- /////////////////////////////////////////////////////////////
-- /////////////////////////////////////////////////////////////
MATCH (n:ListIssue) 
WHERE toLower(n.name) CONTAINS toLower("women") and n.year=2024 
WITH n, n.listType as listTypes

OPTIONAL MATCH (n)-[:INDUSTRY_RANKS]->(ir:IndustryRank)
OPTIONAL MATCH (n)-[:USES]->(lt:ListTableSchema)

OPTIONAL MATCH (lt)-[a:HAS_ATTRIBUTE]->(attr)
OPTIONAL MATCH (lt)-[:HAS_GROUP]->(group:SchemaAttributeGroup)

WITH n, listTypes, 
     CASE WHEN count(ir) > 0 THEN true ELSE false END as hasIndustryRanks,
    COLLECT(DISTINCT {  // ListTable schema
        fuelId: lt.fuelId,
        internalName: lt.internalName
     }) AS listTable,
     COLLECT(DISTINCT {
        attrFuelId: attr.fuelId,
        attrName: attr.internalName,
        attrType: attr.attributeType,
        groupOrder: a.groupOrder,
        isFilterable: a.isFilterable,
        groupIsDefault: a.groupIsDefault,
        groupId: a.groupId,
        isUnique: a.isUnique,
        isSortable: a.isSortable,
        isAutoIncrement: a.isAutoIncrement,
        isReadOnly: a.isReadOnly,
        isImportable: a.isImportable,
        attributeType: a.attributeType,
        isSearchable: a.isSearchable,
        minimumRoleEdit: a.minimumRoleEdit,
        fuelId: a.fuelId,
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
     group{.isDerived,.validationRules, .schemaGroupId, .schemaGroupName, .schemaGroupFormat}

ORDER BY n.date DESC
SKIP 0 LIMIT 100

RETURN COLLECT({
    allowPremiumProfiles: n.allowPremiumProfiles,
    date: n.date,
    embargo: n.embargo,
    fuelId: n.fuelId,
    hasIndustryRanks: hasIndustryRanks,
    issueDate: n.issueDate,
    lastModifiedInputId: n.lastModifiedInputId,
    lastModifiedTimestamp: n.lastModifiedTimestamp,
    lastModifiedUser: n.lastModifiedUser,
    listType: n.listType,
    listUri: n.listUri,
    month: n.month,
    name: n.name,
    naturalId: n.naturalId,
    pageTitle: n.pageTitle,
    status: n.status,
    templateFields: n.templateFields,
    year: n.year,
    listTableSchema: listTable,
    schemaAttributes: attributes,
    attributeGroups: group
}) AS listIssueNodes

-- ////////////////////////////
-- ////////////////////////////
-- ////////////////////////////
-- ////////////////////////////
-- ////////////////////////////
-- ////////////////////////////
-- ////////////////////////////

MATCH (n:ListIssue) 
WHERE toLower(n.name) CONTAINS toLower("net")  and n.date >=1741381200000  and n.date <= 1749416399000
WITH n, n.listType as listTypes

OPTIONAL MATCH (n)-[:INDUSTRY_RANKS]->(ir:IndustryRank)
OPTIONAL MATCH (n)-[:USES]->(lt:ListTableSchema)

OPTIONAL MATCH (lt)-[a:HAS_ATTRIBUTE]->(attr)
OPTIONAL MATCH (lt)-[:HAS_GROUP]->(attributeGroups:SchemaAttributeGroup)

WITH n, listTypes, 
     CASE WHEN count(ir) > 0 THEN true ELSE false END as hasIndustryRanks,
    COLLECT(DISTINCT {  // ListTable schema
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
        attributeType: a.attributeType,
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
     attributeGroups{.isDerived,.validationRules, .schemaGroupId, .schemaGroupName, .schemaGroupFormat}

ORDER BY n.date DESC
SKIP 0 LIMIT 100

RETURN COLLECT({
    allowPremiumProfiles: n.allowPremiumProfiles,
    date: n.date,
    embargo: n.embargo,
    fuelId: n.fuelId,
    hasIndustryRanks: hasIndustryRanks,
    issueDate: n.issueDate,
    lastModifiedInputId: n.lastModifiedInputId,
    lastModifiedTimestamp: n.lastModifiedTimestamp,
    lastModifiedUser: n.lastModifiedUser,
    listType: n.listType,
    listUri: n.listUri,
    month: n.month,
    name: n.name,
    naturalId: n.naturalId,
    pageTitle: n.pageTitle,
    status: n.status,
    templateFields: n.templateFields,
    year: n.year,
    listTableSchema: listTable,
    schemaAttributes: attributes,
    attributeGroups: attributeGroups
}) AS listIssueNodes
-- //////////////////
-- //////////////////
-- //////////////////
-- //////////////////
-- //////////////////
-- //////////////////
-- //////////////////
-- //////////////////
-- //////////////////
-- Last Working Query
-- //////////////////
-- //////////////////
-- //////////////////
-- //////////////////

MATCH (n:ListIssue) 
WHERE toLower(n.name) CONTAINS toLower("women") and n.year=2024 
WITH n, n.listType as listTypes

OPTIONAL MATCH (n)-[:INDUSTRY_RANKS]->(ir:IndustryRank)
OPTIONAL MATCH (n)-[:USES]->(lt:ListTableSchema)

OPTIONAL MATCH (lt)-[h:HAS_ATTRIBUTE]->(attr)
OPTIONAL MATCH (lt)-[:HAS_GROUP]->(attributeGroups:SchemaAttributeGroup)

WITH n, listTypes, 
     CASE WHEN count(ir) > 0 THEN true ELSE false END as hasIndustryRanks,
    [item IN COLLECT(DISTINCT {
        fuelId: lt.fuelId,
        internalName: lt.internalName
     }) WHERE item.fuelId IS NOT NULL] AS listTable,
     [item IN COLLECT(DISTINCT {
        fuelId: attr.fuelId,
        attributeName: attr.attributeName,
        attributeType: attr.attributeType,
        groupOrder: h.groupOrder,
        isFilterable: h.isFilterable,
        groupIsDefault: h.groupIsDefault,
        groupId: h.groupId,
        isUnique: h.isUnique,
        isSortable: h.isSortable,
        isAutoIncrement: h.isAutoIncrement,
        isReadOnly: h.isReadOnly,
        isImportable: h.isImportable,
        attributeType: h.attributeType,
        isSearchable: h.isSearchable,
        minimumRoleEdit: h.minimumRoleEdit,
        order: h.order,
        groupIsHidden: h.groupIsHidden,
        displayLabel: h.displayLabel,
        isRequired: h.isRequired,
        minimumRoleImport: h.minimumRoleImport,
        groupLabel: h.groupLabel,
        isPrimaryKey: h.isPrimaryKey,
        groupIsCollapsed: h.groupIsCollapsed,
        isCategoryRankSlug: h.isCategoryRankSlug,
        minimumRoleExport: h.minimumRoleExport,
        isHidden: h.isHidden,
        minimumRoleView: h.minimumRoleView,
        isExportable: h.isExportable,
        isCategoryRankValue: h.isCategoryRankValue,
        groupDescription: h.groupDescription,
        isNullable: h.isNullable,
        isSecondaryKey: h.isSecondaryKey,
        isCategoryRankLabel: h.isCategoryRankLabel
     }) WHERE item.attributeName IS NOT NULL] AS attributes,
     attributeGroups{.isDerived,.validationRules, .schemaGroupId, .schemaGroupName, .schemaGroupFormat}

ORDER BY n.date DESC
SKIP 0 LIMIT 100

WITH n, hasIndustryRanks, listTypes,
     CASE WHEN size(listTable) > 0 THEN {listTableSchema: listTable} ELSE {} END AS listTablePart,
     CASE WHEN size(attributes) > 0 THEN {schemaAttributes: attributes} ELSE {} END AS attributesPart,
     CASE WHEN attributeGroups IS NOT NULL AND 
               (attributeGroups.schemaGroupId IS NOT NULL OR 
                attributeGroups.schemaGroupName IS NOT NULL) 
          THEN {attributeGroups: attributeGroups} 
          ELSE {} END AS groupsPart

RETURN COLLECT(
  apoc.map.mergeList([
    {
      allowPremiumProfiles: n.allowPremiumProfiles,
      date: n.date,
      embargo: n.embargo,
      fuelId: n.fuelId,
      hasIndustryRanks: hasIndustryRanks,
      issueDate: n.issueDate,
      lastModifiedInputId: n.lastModifiedInputId,
      lastModifiedTimestamp: n.lastModifiedTimestamp,
      lastModifiedUser: n.lastModifiedUser,
      listType: n.listType,
      listUri: n.listUri,
      month: n.month,
      name: n.name,
      naturalId: n.naturalId,
      pageTitle: n.pageTitle,
      status: n.status,
      templateFields: n.templateFields,
      year: n.year
    },
    listTablePart,
    attributesPart,
    groupsPart
  ])
) AS listIssueNodes









-- //////////////////
-- //////////////////
-- //////////////////
-- //////////////////
-- //////////////////
-- //////////////////
-- //////////////////
-- //////////////////
-- //////////////////
-- Last Working Query 2
-- //////////////////
-- //////////////////
-- //////////////////
-- //////////////////

MATCH (n:ListIssue) 
WHERE toLower(n.name) CONTAINS toLower("women") AND n.year = 2024 
WITH n, n.listType AS listTypes

OPTIONAL MATCH (n)-[:INDUSTRY_RANKS]->(ir:IndustryRank)

OPTIONAL MATCH (n)-[:USES]->(lt:ListTableSchema)
OPTIONAL MATCH (lt)-[a:HAS_ATTRIBUTE]->(attr)
OPTIONAL MATCH (lt)-[:HAS_GROUP]->(ag:SchemaAttributeGroup)

WITH 
  n, 
  listTypes, 
  CASE WHEN count(ir) > 0 THEN true ELSE false END AS hasIndustryRanks,

  // List Table Schemas
  [item IN COLLECT(DISTINCT {
    fuelId: lt.fuelId,
    internalName: lt.internalName
  }) WHERE item.fuelId IS NOT NULL] AS listTable,

  // Schema Attributes
  [item IN COLLECT(DISTINCT {
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
  }) WHERE item.attributeName IS NOT NULL] AS attributes,

  // Attribute Groups
  COLLECT(DISTINCT ag {
    .isDerived,
    .validationRules,
    .schemaGroupId,
    .schemaGroupName,
    .schemaGroupFormat
  }) AS attributeGroups

ORDER BY n.date DESC
SKIP 0 LIMIT 100

WITH 
  n, 
  hasIndustryRanks, 
  listTypes,
  CASE WHEN size(listTable) > 0 THEN { listTableSchema: listTable } ELSE {} END AS listTablePart,
  { schemaAttributes: attributes } AS attributesPart,
  CASE WHEN size(attributeGroups) > 0 THEN { attributeGroups: attributeGroups } ELSE {} END AS groupsPart

RETURN COLLECT(
  apoc.map.mergeList([
    {
      allowPremiumProfiles: n.allowPremiumProfiles,
      date: n.date,
      embargo: n.embargo,
      fuelId: n.fuelId,
      hasIndustryRanks: hasIndustryRanks,
      issueDate: n.issueDate,
      lastModifiedInputId: n.lastModifiedInputId,
      lastModifiedTimestamp: n.lastModifiedTimestamp,
      lastModifiedUser: n.lastModifiedUser,
      listType: n.listType,
      listUri: n.listUri,
      month: n.month,
      name: n.name,
      naturalId: n.naturalId,
      pageTitle: n.pageTitle,
      status: n.status,
      templateFields: n.templateFields,
      year: n.year
    },
    listTablePart,
    attributesPart,
    groupsPart
  ])
) AS listIssueNodes
