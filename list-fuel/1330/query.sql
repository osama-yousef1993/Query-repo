MATCH (issue:ListIssue { fuelId: $issueFuelId })
MERGE (schema:SchemaConfig:ListUploadSchema { fuelId: $schemaId })
WITH issue,schema
OPTIONAL MATCH (issue)-[existing:USES]->(:SchemaConfig:ListUploadSchema)
WITH issue, schema, existing
WHERE existing IS NULL
MERGE (issue)-[u:USES]->(schema)
  ON CREATE SET u.fuelId = randomUUID()
  ON MATCH SET u.fuelId = COALESCE(u.fuelId, randomUUID())
WITH schema
OPTIONAL MATCH (schema)-[existingRel:HAS_ATTRIBUTE]->(:SchemaConfigAttribute)
      WITH schema, collect(existingRel) as rels
      FOREACH (rel IN rels | DELETE rel)
      WITH schema
          MERGE (attr${1}:SchemaConfigAttribute { attributeName: $attributes[${1}].attributeName })
          ON CREATE SET
          attr${1}.fuelId = randomUUID(),
          attr${1}.attributeType =  $attributes[${1}].attributeType,
          ${type},
          ${inputType}
          // Create a new HAS_ATTRIBUTE relationship with all properties
          MERGE (schema)-[rel${1}:HAS_ATTRIBUTE]->(attr${1})
          ON CREATE SET rel${1} += {
            order: COALESCE($attributes[${1}].order, ${1 + 1}),
            attributeType: COALESCE($attributes[${1}].attributeType, 'string'),
            format: COALESCE($attributes[${1}].format, ''),
            displayLabel: COALESCE($attributes[${1}].displayLabel, ''),
            isSearchable: COALESCE($attributes[${1}].isSearchable, false),
            isFilterable: COALESCE($attributes[${1}].isFilterable, false),
            isRequired: COALESCE($attributes[${1}].isRequired, false),
            isHidden: COALESCE($attributes[${1}].isHidden, false),
            isUnique: COALESCE($attributes[${1}].isUnique, false),
            isReadOnly: COALESCE($attributes[${1}].isReadOnly, false),
            isSortable: COALESCE($attributes[${1}].isSortable, true),
            isExportable: COALESCE($attributes[${1}].isExportable, true),
            isImportable: COALESCE($attributes[${1}].isImportable, true),
            isNullable: COALESCE($attributes[${1}].isNullable, false),
            isAutoIncrement: COALESCE($attributes[${1}].isAutoIncrement, false),
            isPrimaryKey: COALESCE($attributes[${1}].isPrimaryKey, false),
            isSecondaryKey: COALESCE($attributes[${1}].isSecondaryKey, false),
            groupId: COALESCE($attributes[${1}].groupId, ''),
            groupOrder: COALESCE($attributes[${1}].groupOrder, 0),
            groupLabel: COALESCE($attributes[${1}].groupLabel, ''),
            groupIsCollapsed: COALESCE($attributes[${1}].groupIsCollapsed, false),
            groupIsHidden: COALESCE($attributes[${1}].groupIsHidden, false),
            groupIsDefault: COALESCE($attributes[${1}].groupIsDefault, false),
            groupDescription: COALESCE($attributes[${1}].groupDescription, ''),
            minimumRoleEdit: COALESCE($attributes[${1}].minimumRoleEdit, 'NONE'),
            minimumRoleView: COALESCE($attributes[${1}].minimumRoleView, 'NONE'),
            minimumRoleExport: COALESCE($attributes[${1}].minimumRoleExport, 'NONE'),
            minimumRoleImport: COALESCE($attributes[${1}].minimumRoleImport, 'NONE'),
            isCategoryRankLabel: COALESCE($attributes[${1}].isCategoryRankLabel, false),
            isCategoryRankSlug: COALESCE($attributes[${1}].isCategoryRankSlug, false),
            isCategoryRankValue: COALESCE($attributes[${1}].isCategoryRankValue, false),
            fuelId: randomUUID()
          }

WITH schema
RETURN 




Startups_US_19515

Startups_US_19596