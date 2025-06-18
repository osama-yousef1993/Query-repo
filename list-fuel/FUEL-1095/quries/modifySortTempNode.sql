          MATCH (t:Temp {nodeType: $tempNodeType})
            <-- (s:FileUpload)
            --> (n:FileUpload {fuelId: $uploadId})
            --> (l:ListIssue {naturalId: $listNaturalId, year: $targetListYear})
            OPTIONAL MATCh (l)-[r:TEMP_INDUSTRY_RANKS]->(temp:Temp)
            WITH t, l, COLLECT(DISTINCT r {.category, .categoryRank, .fuelId}) AS industryRanks

            OPTIONAL MATCH (l)-[:USES]->(lt:ListTableSchema)
            OPTIONAL MATCH (lt)-[r]->(g)
            with t, l, industryRanks,
            [item IN COLLECT(DISTINCT {
                  fuelId: lt.fuelId,
                  internalName: lt.internalName
            }) WHERE item.fuelId IS NOT NULL] AS listTable,

            [item IN COLLECT(DISTINCT {
              fuelId: r.fuelId,
              attributeName: r.attributeName,
              attributeType: r.attributeType,
              isRequired: r.isRequired,
              fuelId: r.fuelId,
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
              }) WHERE item.attributeName IS NOT NULL] AS attributes,

            COLLECT(DISTINCT g {
                    .*
              }) AS schemaAttributes

              With t, l, industryRanks,
              CASE WHEN size(listTable) > 0 Then listTable else [] end as listTableSchema,
              CASE WHEN size(attributes) > 0 Then attributes else [] end as attributeGroups,
              CASE WHEN size(schemaAttributes) > 0 Then schemaAttributes else [] end as schemaAttributes

              RETURN apoc.map.merge(t {.*}, {
                  industryRanks: industryRanks,
                  listTableSchema: listTableSchema,
                  attributeGroups: attributeGroups,
                  schemaAttributes: schemaAttributes
              }) AS modifiedTemp, l.fuelId as listId