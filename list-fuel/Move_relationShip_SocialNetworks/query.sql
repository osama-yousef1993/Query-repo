MATCH (c:Organization:Company {fuelId: 'a4af69c4-85f2-4316-84b0-b7a9a0aeb432'})-[:HAS_SOCIAL_NETWORK]->(attr)
WITH c, collect(attr) as att
return c, att



MATCH (org:Organization:Company {fuelId: 'a4af69c4-85f2-4316-84b0-b7a9a0aeb432'})-[:HAS_SOCIAL_NETWORK]->(sn:SocialNetwork)
WITH org, 
     COLLECT({platform: sn.socialNetworks, handle: sn.siteHandle}) AS socials
SET org.socialNetworks = apoc.convert.toJson(socials)
return *



MATCH (org:Organization:Company {fuelId: 'a4af69c4-85f2-4316-84b0-b7a9a0aeb432'})-[:HAS_SOCIAL_NETWORK]->(sn:SocialNetwork)
WITH org, 
     COLLECT([sn.socialNetworks, sn.siteHandle]) AS socialPairs
SET org.socialPlatforms = apoc.convert.toJson(
  apoc.map.fromPairs(socialPairs)
)
REMOVE org.socialNetworks
return org


MATCH (company:Organization:Company {fuelId: 'a4af69c4-85f2-4316-84b0-b7a9a0aeb432'})
OPTIONAL MATCH (company)-[:HAS_SOCIAL_NETWORK]->(existing:SocialNetwork)
WITH company, 
     COLLECT(DISTINCT {socialNetwork: existing.socialNetworks, siteHandle: existing.siteHandle}) AS existingNetworks
// SET company.socialPlatforms = apoc.convert.toJson(
//    apoc.map.fromPairs(existingNetworks)
// )
// WITH company
return *




MATCH (company:Organization:Company {fuelId: 'a4af69c4-85f2-4316-84b0-b7a9a0aeb432'})
OPTIONAL MATCH (company)-[:HAS_SOCIAL_NETWORK]->(existing:SocialNetwork)
WITH company, 
     apoc.convert.toJson(COLLECT(DISTINCT {
       socialNetwork: COALESCE(existing.socialNetworks, ""), 
       siteHandle: COALESCE(existing.siteHandle, "")
     })) AS socialNetworksJson
SET company.socialNetworksJson = socialNetworksJson
RETURN company



-- working 
MATCH (company:Organization:Company {fuelId: 'a4af69c4-85f2-4316-84b0-b7a9a0aeb432'})
OPTIONAL MATCH (company)-[:HAS_SOCIAL_NETWORK]->(existing:SocialNetwork)
WITH company, COLLECT([existing.socialNetworks, existing.siteHandle]) AS socialPairs
with company,
    apoc.convert.toJson(apoc.map.fromPairs(socialPairs)) AS socialNetworksJson

SET company.socialNetworks = socialNetworksJson
RETURN company


MATCH (company:Organization:Company {fuelId: 'a4af69c4-85f2-4316-84b0-b7a9a0aeb432'})
RETURN  collect({
     {
    country: company.country,
    city: company.city,
    postalCode: company.postalCode,
    state: company.state,
    description: company.description,
    industry: company.industry,
    employees: company.employees,
    name: company.name,
    nameSort: company.nameSort,
    nameTiny: company.nameTiny,
    fuelId: company.fuelId,
    socialNetworks: apoc.convert.fromJsonMap(company.socialNetworks),
    geoLocationLatitude: company.geoLocationLatitude,
    geoLocationLongitude: company.geoLocationLongitude,
    displayName: company.displayName,
    changeBy: company.changeBy,
    latitude: company.latitude,
    webSite: company.webSite,
    changeDate: company.changeDate,
    ceoName: company.ceoName,
    image: company.image,
    visible: company.visible,
    organizationName: company.organizationName,
    descriptionAlt: company.descriptionAlt,
    squareImage: company.squareImage,
    yearFounded: company.yearFounded,
    uri: company.uri,
    ceoTitle: company.ceoTitle,
    uris: company.uris,
    allowAccolades: company.allowAccolades,
    premiumProfile: company.premiumProfile,
    name: company.name,
    subType: company.subType,
    embargo: company.embargo,
    employees: company.employees,
    nameSort: company.nameSort,
    altDescription: company.altDescription
})


-- old query
CreateSocialNetworks: `
MATCH (company:Organization:Company {naturalId: $companyId}) // Match company
UNWIND $socialNetworks AS network                           // Unwind social networks array
MERGE (s:SocialNetwork {                                    // Match or create social network node
     socialNetwork: network.socialNetwork,
     siteHandle: network.siteHandle
})
ON CREATE SET
     s.fuelId = coalesce(randomUUID(), s.fuelId),            // Assign fuelId
     s.createdAt = timestamp()                              // Set creation timestamp
ON MATCH SET
     s.updatedAt = timestamp()                              // Update last modified timestamp
MERGE (company)-[r:HAS_SOCIAL_NETWORK]->(s)                 // Create company-social network relationship
ON CREATE SET
     r.fuelId = coalesce(randomUUID(), r.fuelId),            // Assign relationship fuelId
     r.createdAt = timestamp()                              // Set relationship creation timestamp
ON MATCH SET
     r.updatedAt = timestamp()                              // Update relationship timestamp
`,


MATCH (company:Organization:Company {fuelId: 'a4af69c4-85f2-4316-84b0-b7a9a0aeb432'})
WITH company, 
  CASE 
    WHEN company.socialNetworks IS NULL OR company.socialNetworks = "" 
    THEN {} 
    ELSE apoc.convert.fromJsonMap(company.socialNetworks) 
  END as existingSocialNetworks

UNWIND [
    {socialNetwork: "LinkedIn", siteHandle: "https://linkedin.com/company/new-linkedin"},
    {socialNetwork: "youtube", siteHandle: "https://youtube.com/new-tube-0"},
    {socialNetwork: "Facebook", siteHandle: "https://facebook.com/new-fb"},
    {socialNetwork: "Twitter", siteHandle: "https://Twitter.com/new-x"}
] AS network

WITH company, existingSocialNetworks, network,
  CASE
    WHEN size(keys(existingSocialNetworks)) = 0 THEN true
    WHEN NONE(k IN keys(existingSocialNetworks) 
         WHERE toLower(k) = toLower(network.socialNetwork))
    THEN true
    WHEN ANY(k IN keys(existingSocialNetworks) 
         WHERE toLower(k) = toLower(network.socialNetwork) 
         AND existingSocialNetworks[k] <> network.siteHandle)
    THEN true
    ELSE false
  END AS isNew

WITH company, existingSocialNetworks,
  COLLECT(CASE WHEN isNew THEN {
    socialNetwork: network.socialNetwork,
    siteHandle: network.siteHandle
  } END) AS newNetworks

WITH company, existingSocialNetworks, 
     [n IN newNetworks WHERE n IS NOT NULL] AS filteredNewNetworks

// Handle empty existing case
WITH company, 
  CASE WHEN size(keys(existingSocialNetworks)) = 0
  THEN apoc.map.fromLists(
    [n IN filteredNewNetworks | n.socialNetwork],
    [n IN filteredNewNetworks | n.siteHandle]
  )
  ELSE apoc.map.merge(
    existingSocialNetworks,
    apoc.map.fromLists(
      [n IN filteredNewNetworks | n.socialNetwork],
      [n IN filteredNewNetworks | n.siteHandle]
    )
  ) END AS updatedNetworks

SET company.socialNetworks = apoc.convert.toJson(updatedNetworks)
RETURN company, updatedNetworks




MATCH (c:Organization:Company)-[:HAS_SOCIAL_NETWORK]->(attr)
WITH c, collect(distinct attr.socialNetworks) as att
return  att




MATCH (company:Organization:Company {fuelId: 'a4af69c4-85f2-4316-84b0-b7a9a0aeb432'})

UNWIND [
    {socialNetwork: "LinkedIn", siteHandle: "www.linkedin.com/company/charlestonareamedicalcenter/"},
    {socialNetwork: "Facebook", siteHandle: "www.facebook.com/CharlestonAreaMedicalCenter"},
    {socialNetwork: "Twitter", siteHandle: "twitter.com/camchealth"},
    {socialNetwork: "Instagram", siteHandle: "www.instagram.com/camchealth/"},
] AS network

SET company[toLower(network.socialNetwork)] = network.siteHandle

RETURN company, updatedNetworks





MATCH (template:EntityTypesAggregate)
SET template.LinkedIn = "string"
SET template.Instagram = "string"
SET template.Facebook = "string"
SET template.Twitter = "string"
SET template.YouTube = "string"
RETURN template




MATCH (template:EntityTypesAggregate)
WITH keys(template) AS propertyKeys
MATCH (n)
WHERE ANY(label IN labels(n) WHERE label IN ['College', 'Company', 'Organization', 'Person', 'Team'])
LIMIT 1
WITH n, propertyKeys
OPTIONAL MATCH (n {fuelId: "a4af69c4-85f2-4316-84b0-b7a9a0aeb432"})-[r]->(m)
WHERE ANY(label IN labels(m) WHERE label IN ['College', 'Company', 'Organization', 'Person', 'Team'])
        WITH n, propertyKeys, collect({
            relationshipData: r,
            entity: CASE
                WHEN m IS NOT NULL THEN apoc.map.merge(
                    apoc.map.fromPairs([key IN propertyKeys WHERE m[key] IS NOT NULL | [key, m[key]]]),
                    {
                        labels: labels(m),
                        mostRelevantLabel: labels(m)[-1]
                    }
                )
                ELSE null
            END
        }) AS relationships

        // Step 4: Aggregate relationships and construct the final map
        RETURN apoc.map.merge(
            apoc.map.fromPairs([key IN propertyKeys WHERE n[key] IS NOT NULL | [key, n[key]]]),
            {
                labels: labels(n),
                mostRelevantLabel: labels(n)[-1],
                relationships: relationships
            }
        ) AS ndata


MATCH (company:Organization:Company {fuelId: 'a4af69c4-85f2-4316-84b0-b7a9a0aeb432'})

UNWIND [
    {socialNetwork: "LinkedIn", siteHandle: "www.linkedin.com/company/charlestonareamedicalcenter/"},
    {socialNetwork: "Facebook", siteHandle: "www.facebook.com/CharlestonAreaMedicalCenter"},
    {socialNetwork: "Twitter", siteHandle: "twitter.com/camchealth"},
    {socialNetwork: "Instagram", siteHandle: "www.instagram.com/camchealth/"}
] AS network

SET company[network.socialNetwork] = network.siteHandle

RETURN company