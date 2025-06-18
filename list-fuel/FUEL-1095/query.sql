MATCH (t:Temp {nodeType: 'company'})
        <-- (s:FileUpload)
        --> (n:FileUpload {fuelId: $uploadId})
        --> (l:ListIssue {naturalId: $listNaturalId, year: $targetListYear})
    
MATCH (t:Temp)
return distinct t.nodeType


match (schema:SchemaConfig {fuelId: '7dedb820-9fa4-41d9-8c0b-3ba3badbbb23'})-[r:HAS_ATTRIBUTE]->(attr:SchemaConfigAttribute)
WITH schema, r, attr
OPTIONAL MATCH (schema)-[g:HAS_GROUP]->(attrGroup:SchemaAttributeGroup)
WITH schema, r, attr, g, attrGroup
OPTIONAL MATCH (schema)-[g]-(attrGroup)-[r2:INDUSTRY_RANKS]-(attrList:ListSchemaAttribute)
return *



MATCH (t:Temp {nodeType: 'company'})
        <-- (s:FileUpload)
        --> (n:FileUpload {fuelId: 'cb3914af-f9cf-419c-a9df-cbe9053a9f6e'})
        --> (issue:ListIssue {fuelId :'0da9c4de-78dc-4eb2-a2fe-1d28d1394914', year: 2026})
with t, s, n, issue
OPTIONAL MATCH (issue)-[:USES]->(lt:ListTableSchema)
OPTIONAL MATCH (lt)-[a:HAS_ATTRIBUTE]->(attr)
OPTIONAL MATCH (lt)-[:HAS_GROUP]->(ag:SchemaAttributeGroup)
with t, s, n, issue,
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
      WITH
        t, s, n, issue,
        CASE WHEN size(listTable) > 0 THEN { listTableSchema: listTable } ELSE {} END AS listTablePart,
        { schemaAttributes: attributes } AS attributesPart,
        CASE WHEN size(attributeGroups) > 0 THEN { attributeGroups: attributeGroups } ELSE {} END AS groupsPart
return collect(
        apoc.map.mergeList([
                t, 
                s,
                n,
                issue,
                listTablePart,
                attributesPart,
                groupsPart
        ])
)



MATCH (issue:ListIssue {fuelId: '0da9c4de-78dc-4eb2-a2fe-1d28d1394914', year: 2026})
MATCH (n:FileUpload {fuelId: 'cb3914af-f9cf-419c-a9df-cbe9053a9f6e'})-->()-->(t:Temp {nodeType: 'company'})<--(s:FileUpload)
WHERE (n)-->(issue) AND (s)-->(issue)

// Get schema base information
OPTIONAL MATCH (issue)-[:USES]->(lt:ListTableSchema)
OPTIONAL MATCH (lt)-[:HAS_ATTRIBUTE]->(attr:SchemaAttribute)
WITH t, s, n, issue, lt, COLLECT(DISTINCT attr { .* }) AS allAttributes

// Process groups separately
OPTIONAL MATCH (lt)-[:HAS_GROUP]->(ag:SchemaAttributeGroup)
WITH t, s, n, issue, lt, allAttributes, COLLECT(DISTINCT ag { .* }) AS groups

// Get attributes for each group
UNWIND groups AS group
OPTIONAL MATCH (group)-[:CONTAINS_ATTRIBUTE]->(groupAttr:SchemaAttribute)
WITH t, s, n, issue, lt, allAttributes, group, COLLECT(DISTINCT groupAttr { .* }) AS groupAttrs
WITH t, s, n, issue, lt, allAttributes, 
     COLLECT({group: group, attributes: groupAttrs}) AS groupedAttributes

// Finally get category ranks
OPTIONAL MATCH (t)-[r:TEMP_INDUSTRY_RANKS]->(cr:CategoryRank)
WITH t, s, n, issue, lt, allAttributes, groupedAttributes,
     COLLECT(DISTINCT {category: cr.category, rank: cr.rank}) AS categoryRanks

RETURN COLLECT({
  temp: t { .* },
  fileUpload: s { .* },
  relatedFileUpload: n { .* },
  issue: issue { .* },
  schema: CASE WHEN lt IS NOT NULL THEN {
    listTableSchema: lt { .* },
    schemaAttributes: allAttributes,
    attributeGroups: groupedAttributes
  } ELSE null END,
  categoryRanks: categoryRanks
}) AS results




MATCH (issue:ListIssue )
MATCH (n:FileUpload )-->()-->(t:Temp {nodeType: 'company'})<--(s:FileUpload)
WHERE (n)-->(issue) AND (s)-->(issue)

OPTIONAL MATCH (issue)-[:USES]->(lt:ListTableSchema)
OPTIONAL MATCH (lt)-[:HAS_ATTRIBUTE]->(attr:SchemaAttribute)
WITH t, s, n, issue, lt, COLLECT(DISTINCT attr { .* }) AS allAttributes
where lt is not null
limit 100
return *







MATCH (t:Temp)
  <-- (s:FileUpload)
  --> (n:FileUpload )
  --> (l:ListIssue)-[r:TEMP_INDUSTRY_RANKS]->(temp:Temp)
with t, s, n,l,r,temp
OPTIONAL MATCH (l)-[:USES]->(lt:ListTableSchema)
OPTIONAL MATCH (lt)-[a:HAS_ATTRIBUTE]->(attr)
OPTIONAL MATCH (lt)-[:HAS_GROUP]->(ag:SchemaAttributeGroup)
with t, s, n,l,r,temp,
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

with t,l,r,temp,
CASE WHEN size(listTable) > 0 THEN { listTableSchema: listTable } ELSE {} END AS listTablePart,
{ schemaAttributes: attributes } AS attributesPart,
CASE WHEN size(attributeGroups) > 0 THEN { attributeGroups: attributeGroups } ELSE {} END AS groupsPart

RETURN COLLECT(
apoc.map.mergeList([
        t,
        r,
        listId: l.fuelId,
        listTablePart,
        attributesPart,
        groupsPart
])
) AS listIssueNodes










MATCH (t:Temp)
  <-- (s:FileUpload)
  --> (n:FileUpload )
  --> (l:ListIssue)-[r:TEMP_INDUSTRY_RANKS]->(temp:Temp)
WITH t, s, n, l, COLLECT(DISTINCT {relationship: r {.category, .categoryRank, .fuelId}}) AS industryRanks
OPTIONAL MATCH (l)-[:USES]->(lt:ListTableSchema)
OPTIONAL MATCH (lt)-[a:HAS_ATTRIBUTE]->(attr)
OPTIONAL MATCH (lt)-[:HAS_GROUP]->(ag:SchemaAttributeGroup)
with t, s, n,l,industryRanks,
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

with t,l,industryRanks,
CASE WHEN size(listTable) > 0 THEN { listTableSchema: listTable } ELSE {} END AS listTablePart,
{ schemaAttributes: attributes } AS attributesPart,
CASE WHEN size(attributeGroups) > 0 THEN { attributeGroups: attributeGroups } ELSE {} END AS groupsPart
limit 10
RETURN COLLECT(
apoc.map.mergeList([
        {listId: l.fuelId},
        t,
        {industryRanks: industryRanks},
        listTablePart,
        attributesPart,
        groupsPart
])
) AS listIssueNodes













-- last version working 
MATCH (t:Temp)
  <-- (s:FileUpload)
  --> (n:FileUpload )
  --> (l:ListIssue)-[r:TEMP_INDUSTRY_RANKS]->(temp:Temp)
WITH t, s, n, l, COLLECT(DISTINCT {
    relationship: r {.category, .categoryRank, .fuelId},
    isCategoryRank: a.isCategoryRankValue
}) WHERE item.isCategoryRank = true AS industryRanks
OPTIONAL MATCH (l)-[:USES]->(lt:ListTableSchema)
OPTIONAL MATCH (lt)-[a:HAS_ATTRIBUTE]->(attr)
OPTIONAL MATCH (lt)-[:HAS_GROUP]->(ag:SchemaAttributeGroup)
with t, s, n,l,industryRanks,
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

with t,l,industryRanks,
CASE WHEN size(listTable) > 0 THEN { listTableSchema: listTable } ELSE {} END AS listTablePart,
{ schemaAttributes: attributes } AS attributesPart,
CASE WHEN size(attributeGroups) > 0 THEN { attributeGroups: attributeGroups } ELSE {} END AS groupsPart
RETURN COLLECT(
apoc.map.mergeList([
        {listId: l.fuelId},
        t,
        {industryRanks: industryRanks},
        listTablePart,
        attributesPart,
        groupsPart
])
) AS listIssueNodes




-- modified version 
MATCH (t:Temp)
  <-- (s:FileUpload)
  --> (n:FileUpload)
  --> (l:ListIssue)-[r:TEMP_INDUSTRY_RANKS]->(temp:Temp)
// First collect all industry ranks (unfiltered)
WITH t, s, n, l, COLLECT(DISTINCT {
    relationship: r {.category, .categoryRank, .fuelId},
    tempNode: temp
}) AS allIndustryRanks

// Get schema information to identify category rank attributes
OPTIONAL MATCH (l)-[:USES]->(lt:ListTableSchema)
OPTIONAL MATCH (lt)-[a:HAS_ATTRIBUTE]->(attr)
WHERE a.isCategoryRankValue = true
WITH t, s, n, l, allIndustryRanks,
     COLLECT(DISTINCT attr.attributeName) AS categoryRankAttributes

// Filter industry ranks to only include schema-defined category ranks
WITH t, s, n, l,
     CASE WHEN size(categoryRankAttributes) > 0
          THEN [rank IN allIndustryRanks 
                WHERE rank.relationship.category IN categoryRankAttributes]
          ELSE allIndustryRanks
     END AS industryRanks

// Get complete schema information for output
OPTIONAL MATCH (l)-[:USES]->(lt:ListTableSchema)
OPTIONAL MATCH (lt)-[a:HAS_ATTRIBUTE]->(attr)
OPTIONAL MATCH (lt)-[:HAS_GROUP]->(ag:SchemaAttributeGroup)
WITH t, l, industryRanks,
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

WITH t, l, industryRanks,
     CASE WHEN size(listTable) > 0 THEN { listTableSchema: listTable } ELSE {} END AS listTablePart,
     { schemaAttributes: attributes } AS attributesPart,
     CASE WHEN size(attributeGroups) > 0 THEN { attributeGroups: attributeGroups } ELSE {} END AS groupsPart

RETURN COLLECT(
    apoc.map.mergeList([
        {listId: l.fuelId},
        t,
        {industryRanks: industryRanks},
        listTablePart,
        attributesPart,
        groupsPart
    ])
) AS listIssueNodes