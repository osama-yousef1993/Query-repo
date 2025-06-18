match (o:Organization {name: 'acceldata'}) 
set o.visible = false
return o.visible

MATCH (n {fuelId : "c11a8b0d-1b92-4d1f-aa97-94a5fc508506"}) return n

MATCH (n) WHERE n.naturalId CONTAINS "124555" return n

MATCH (l:ListIssue)-[r:LISTS]->(n:Organization {naturalId: "fred/company/124555"}) 
RETURN l.listUri, l.year, n



MATCH (n:ListIssue {listUri:'americas-best-startup-employers', year:2023 })-[r:LISTS {visible: true }]->(o:Organization {visible: true })
 WITH count(o) AS org_count
  MATCH (n:ListIssue {listUri:'americas-best-startup-employers', year:2023 })-[r:LISTS {visible: true }]->(o:Organization { visible: true })
   WITH n, r, o, org_count, toInteger(r.rank) as rank, toInteger(r.position) as position
 ORDER BY position ASC, rank ASC
  RETURN collect({

uris: o.uris


  }) AS listIssueNodes, org_count as count

  MATCH (n:Organization {naturalId: "fred/company/124555"}) 
SET n.visible = true
RETURN n

match(n:ListSchemaAttribute ) where n.attributeName in ['state','city'] return n



MATCH (n:ListIssue {listUri:'americas-best-startup-employers', year:2023 })-[r:LISTS {visible: true }]->(o:Organization {visible: true })
 WITH count(o) AS org_count
  MATCH (n:ListIssue {listUri:'americas-best-startup-employers', year:2023 })-[r:LISTS {visible: true }]->(o:Organization { visible: true })
   WITH n, r, o, org_count, toInteger(r.rank) as rank, toInteger(r.position) as position
 ORDER BY position ASC, rank ASC
  RETURN collect({

siteHandler: n.siteHandle,

socialNetworks: n.socialNetworks,
       orgSiteHandler: o.siteHandle,

orgSocialNetworks: o.socialNetworks


  }) AS listIssueNodes, org_count as count



MATCH (n:Organization)
WITH collect(n.socialNetworks) AS keys, collect(n.siteHandle) AS values
RETURN apoc.map.fromLists(keys, values) AS social




 
MATCH (issue:ListIssue { fuelId: '49c1eb2a-c0ec-4877-a82d-6bf2b19947dc' })
MERGE (schema:SchemaConfig:ListUploadSchema { fuelId: '2fa90010-0fa6-4ec7-9eb3-eb5b9469bfec' })
MERGE (issue)-[u:USES]->(schema)
ON CREATE SET u.fuelId = randomUUID()
ON MATCH SET u.fuelId = COALESCE(u.fuelId, randomUUID())
WITH schema  
    OPTIONAL MATCH (schema)-[existingRel:HAS_ATTRIBUTE]->(:SchemaConfigAttribute) 
    WITH schema, collect(existingRel) as rels 
    FOREACH (rel IN rels | DELETE rel)      
    WITH schema
  MERGE (attr0:SchemaConfigAttribute { attributeName: $attributes[0].attributeName })
  ON CREATE SET
  attr0.fuelId = randomUUID(),
  attr0.attributeType =  $attributes[0].attributeType
  Merge (schema)-[rel0:HAS_ATTRIBUTE]->(attr0)
  ON CREATE SET rel0 = {
    order: COALESCE($attributes[0].order, 1),
    attributeType: COALESCE($attributes[0].attributeType, 'string'),
    format: COALESCE($attributes[0].format, ''),
    displayLabel: COALESCE($attributes[0].displayLabel, ''),
    isSearchable: COALESCE($attributes[0].isSearchable, false),
    isFilterable: COALESCE($attributes[0].isFilterable, false),
    isRequired: COALESCE($attributes[0].isRequired, false),
    isHidden: COALESCE($attributes[0].isHidden, false),
    isUnique: COALESCE($attributes[0].isUnique, false),
    isReadOnly: COALESCE($attributes[0].isReadOnly, false),
    isSortable: COALESCE($attributes[0].isSortable, true),
    isExportable: COALESCE($attributes[0].isExportable, true),
    isImportable: COALESCE($attributes[0].isImportable, true),
    isNullable: COALESCE($attributes[0].isNullable, false),
    isAutoIncrement: COALESCE($attributes[0].isAutoIncrement, false),
    isPrimaryKey: COALESCE($attributes[0].isPrimaryKey, false),
    isSecondaryKey: COALESCE($attributes[0].isSecondaryKey, false),
    groupId: COALESCE($attributes[0].groupId, ''),
    groupOrder: COALESCE($attributes[0].groupOrder, 0),
    groupLabel: COALESCE($attributes[0].groupLabel, ''),
    groupIsCollapsed: COALESCE($attributes[0].groupIsCollapsed, false),
    groupIsHidden: COALESCE($attributes[0].groupIsHidden, false),
    groupIsDefault: COALESCE($attributes[0].groupIsDefault, false),
    groupDescription: COALESCE($attributes[0].groupDescription, ''),
    minimumRoleEdit: COALESCE($attributes[0].minimumRoleEdit, 'NONE'),
    minimumRoleView: COALESCE($attributes[0].minimumRoleView, 'NONE'),
    minimumRoleExport: COALESCE($attributes[0].minimumRoleExport, 'NONE'),
    minimumRoleImport: COALESCE($attributes[0].minimumRoleImport, 'NONE'),
    isCategoryRankLabel: COALESCE($attributes[0].isCategoryRankLabel, false),
    isCategoryRankSlug: COALESCE($attributes[0].isCategoryRankSlug, false),
    isCategoryRankValue: COALESCE($attributes[0].isCategoryRankValue, false),
    fuelId: randomUUID()
  }
  WITH schema
RETURN schema








match (n:ListIssue {fuelId: '49c1eb2a-c0ec-4877-a82d-6bf2b19947dc'}) -[:USES]-> (schema:SchemaConfig) return *


MATCH (n:SchemaConfig)-[r:HAS_ATTRIBUTE]->(attr:ListSchemaAttribute)

RETURN *




MATCH (issue:ListIssue { fuelId: '49c1eb2a-c0ec-4877-a82d-6bf2b19947dc' })
MERGE (schema:SchemaConfig:ListUploadSchema { fuelId: '2fa90010-0fa6-4ec7-9eb3-eb5b9469bfec' })
MERGE (schem:SchemaConfig:ListTableSchema { fuelId: 'a12a1e15-763e-4665-918f-b5f64d73be34' })
MERGE (issue)-[u:USES]->(schema)
MERGE (issue)-[u1:USES]->(schem)
WITH schema , schem
return *

MATCH (issue:ListIssue { fuelId: '49c1eb2a-c0ec-4877-a82d-6bf2b19947dc' })
MERGE (schema:SchemaConfig:ListUploadSchema { fuelId: '89a7a770-7a43-4196-80e4-daed3f6a7628' })
MERGE (schem:SchemaConfig:ListTableSchema { fuelId: 'a12a1e15-763e-4665-918f-b5f64d73be34' })
MERGE (issue)-[u:USES]->(schema)
MERGE (issue)-[u1:USES]->(schem)
WITH schema , schem
return *
MATCH (issue:ListIssue { fuelId: '49c1eb2a-c0ec-4877-a82d-6bf2b19947dc' })
MERGE (schema:SchemaConfig:ListUploadSchema { fuelId: '5e141a8c-a04f-4584-a06d-11582a40c0eb' })
MERGE (schem:SchemaConfig:ListTableSchema { fuelId: 'a12a1e15-763e-4665-918f-b5f64d73be34' })
MERGE (issue)-[u:USES]->(schema)
MERGE (issue)-[u1:USES]->(schem)
WITH schema , schem
return *
MATCH (issue:ListIssue { fuelId: '49c1eb2a-c0ec-4877-a82d-6bf2b19947dc' })
MERGE (schema:SchemaConfig:ListUploadSchema { fuelId: '12c0ff05-26a7-4713-8e61-53314b23bf6b' })
MERGE (schem:SchemaConfig:ListTableSchema { fuelId: 'a12a1e15-763e-4665-918f-b5f64d73be34' })
MERGE (issue)-[u:USES]->(schema)
MERGE (issue)-[u1:USES]->(schem)
WITH schema , schem
return *


match (n:SchemaConfig) -[r:HAS_ATTRIBUTE]-> (attr:ListSchemaAttribute) 
WITH n, collect(r) AS rels
// WITH n, rels[1..] AS toDelete
// UNWIND toDelete AS r

RETURN *