MATCH (n:SchemaConfigAttribute)
 WITH collect(n.attributeName) AS propertyKeys
 MATCH (n)
 WHERE ANY(label IN labels(n) WHERE label IN ['College','Company','Organization','Person','Team'])
 AND n.fuelId = 'a4af69c4-85f2-4316-84b0-b7a9a0aeb432'
 LIMIT 100
  WITH n, propertyKeys

 WITH n, propertyKeys, [] AS listIssues
     OPTIONAL MATCH (n)-[r]->(m)
     WHERE ANY(label IN labels(m) WHERE label IN ['College','Company','Organization','Person','Team'])
    WITH n, propertyKeys, listIssues, collect({
      relationshipData: r,
      entity: CASE
        WHEN m IS NOT NULL THEN apoc.map.merge(
          apoc.map.fromPairs([key IN propertyKeys WHERE m[key] IS NOT NULL | [key, m[key]]]),
          {
            labels: labels(m),
            mostRelevantLabel: labels(m)[-1],
            __typename: 'EntityTypesAggregateOut'
          }
        )
        ELSE null
      END
    }) AS relationships
      // RETURN apoc.map.merge(
      //   apoc.map.fromPairs([key IN propertyKeys WHERE n[key] IS NOT NULL | [key, n[key]]]),
      //   {
      //     labels: labels(n),
      //     mostRelevantLabel: labels(n)[-1],
      //     relationships: relationships,
      //     listIssues: listIssues
      //   }
      // ) AS ndata
      return relationships, n, propertyKeys, listIssues
    





/////////////////////////
/////////////////////////
/////////////////////////
/////////////////////////
/////////////////////////
/////////////////////////
/////////////////////////
/////////////////////////
/////////////////////////
/////////////////////////
/////////////////////////
MATCH (n:SchemaConfigAttribute)
 WITH collect(n.attributeName) AS propertyKeys
 MATCH (n)
 WHERE ANY(label IN labels(n) WHERE label IN ['College','Company','Organization','Person','Team'])
 AND n.fuelId = 'a4af69c4-85f2-4316-84b0-b7a9a0aeb432'
 LIMIT 100
 WITH n, propertyKeys
return n.attributeName






// Now handle the group and INCLUDES_ATTRIBUTE relationship
WITH schema, attr${index}, $attributes[${index}] as attribute
// First find or create the group if specified
FOREACH (ignore IN CASE WHEN attribute.groupId IS NOT NULL AND attribute.groupId <> '' THEN [1] ELSE [] END |
  MERGE (group:SchemaConfigGroup { fuelId: attribute.groupId })
  ON CREATE SET
    group.fuelId = attribute.groupId,
    group.groupLabel = COALESCE(attribute.groupLabel, ''),
    group.groupOrder = COALESCE(attribute.groupOrder, 0),
    group.groupIsCollapsed = COALESCE(attribute.groupIsCollapsed, false),
    group.groupIsHidden = COALESCE(attribute.groupIsHidden, false)
  
  // Create INCLUDES_ATTRIBUTE relationship with primaryRank/secondaryRank
  MERGE (group)-[ia:INCLUDES_ATTRIBUTE]->(attr${index})
  ON CREATE SET ia += {
    fuelId: randomUUID(),
    // Set primaryRank if this attribute is in primaryRankAttributes array
    primaryRank: CASE 
      WHEN $primaryRankAttributes IS NOT NULL AND attr${index}.attributeName IN $primaryRankAttributes 
      THEN apoc.coll.indexOf($primaryRankAttributes, attr${index}.attributeName) + 1
      ELSE null
    END,
    // Set secondaryRank if this attribute is in secondaryRankAttributes array
    secondaryRank: CASE
      WHEN $secondaryRankAttributes IS NOT NULL AND attr${index}.attributeName IN $secondaryRankAttributes
      THEN apoc.coll.indexOf($secondaryRankAttributes, attr${index}.attributeName) + 1
      ELSE null
    END
  }
  ON MATCH SET ia += {
    // Update primaryRank/secondaryRank if they exist in the arrays
    primaryRank: CASE 
      WHEN $primaryRankAttributes IS NOT NULL AND attr${index}.attributeName IN $primaryRankAttributes 
      THEN apoc.coll.indexOf($primaryRankAttributes, attr${index}.attributeName) + 1
      ELSE null
    END,
    secondaryRank: CASE
      WHEN $secondaryRankAttributes IS NOT NULL AND attr${index}.attributeName IN $secondaryRankAttributes
      THEN apoc.coll.indexOf($secondaryRankAttributes, attr${index}.attributeName) + 1
      ELSE null
    END
  }
  
  // Ensure the schema has the group
  MERGE (schema)-[:HAS_GROUP]->(group)
)