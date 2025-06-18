MATCH (n:ListIssue {listUri:'best-employers-for-veterans', year:2020 })-[r:LISTS]->(o:Organization)
WITH count(o) AS org_count
MATCH (n:ListIssue {listUri:'best-employers-for-veterans', year:2020 })-[r:LISTS]->(o:Organization)
WITH n, r, o, org_count, toInteger(r.rank) as rank, toInteger(r.position) as position
ORDER BY position ASC, rank ASC
RETURN collect({
    position: position,
    rank: rank,
    country: o.country,
    naturalId: o.naturalId,
    displayName: o.displayName,
    changeBy: o.changeBy,
    latitude: o.latitude,
    description: o.description,
    industry: o.industry,
    webSite: o.webSite,
    youTubePlayList: o.youTubePlayList,
    changeDate: o.changeDate,
    ceoName: o.ceoName,
    state: o.state,
    nameTiny: o.nameTiny,
    fuelId: o.fuelId,
    longitude: o.longitude,
    _profileComplete: o._profileComplete,
    image: o.image,
    visible: o.visible,
    organizationName: o.organizationName,
    descriptionAlt: o.descriptionAlt,
    squareImage: o.squareImage,
    yearFounded: o.yearFounded,
    uri: o.uri,
    ceoTitle: o.ceoTitle,
    uris: o.uris,
    phoneNumber: o.phoneNumber,
    allowAccolades: o.allowAccolades,
    phone: o.phone,
    premiumProfile: o.premiumProfile,
    name: o.name,
    embargo: o.embargo,
    employees: o.employees,
    nameSort: o.nameSort,
    altDescription: o.altDescription
}) AS listIssueNodes, org_count as count




-- /////////////////////////////////
-- /////////////////////////////////
-- /////////////////////////////////
-- /////////////////////////////////
-- /////////////////////////////////
-- /////////////////////////////////
-- /////////////////////////////////
-- /////////////////////////////////
MATCH (n:ListIssue {listUri:'best-employers-for-veterans', year:2020 })-[r:LISTS]->(o:Organization)
WITH count(o) AS org_count
MATCH (n:ListIssue {listUri:'best-employers-for-veterans', year:2020 })-[r:LISTS]->(o:Organization)
WITH n, r, o, org_count, toInteger(r.rank) as rank, toInteger(r.position) as position
ORDER BY position DESC, rank DESC
RETURN collect({
    position: position,
    rank: rank,
    country: o.country,
    naturalId: o.naturalId,
    displayName: o.displayName,
    changeBy: o.changeBy,
    latitude: o.latitude,
    description: o.description,
    industry: o.industry,
    webSite: o.webSite,
    youTubePlayList: o.youTubePlayList,
    changeDate: o.changeDate,
    ceoName: o.ceoName,
    state: o.state,
    nameTiny: o.nameTiny,
    fuelId: o.fuelId,
    longitude: o.longitude,
    _profileComplete: o._profileComplete,
    image: o.image,
    visible: o.visible,
    organizationName: o.organizationName,
    descriptionAlt: o.descriptionAlt,
    squareImage: o.squareImage,
    yearFounded: o.yearFounded,
    uri: o.uri,
    ceoTitle: o.ceoTitle,
    uris: o.uris,
    phoneNumber: o.phoneNumber,
    allowAccolades: o.allowAccolades,
    phone: o.phone,
    premiumProfile: o.premiumProfile,
    name: o.name,
    embargo: o.embargo,
    employees: o.employees,
    nameSort: o.nameSort,
    altDescription: o.altDescription
}) AS listIssueNodes, org_count as count




-- /////////////////////////////////////
-- /////////////////////////////////////
-- /////////////////////////////////////
-- /////////////////////////////////////
-- /////////////////////////////////////
-- /////////////////////////////////////
-- /////////////////////////////////////
queryTemporaryNodesFromUploadId (company, ACCTX2024, 2024, 4ff31ff3-5e9d-4c75-9be3-bc7f95ee6d21):
 MATCH (t:Temp {nodeType: 'company'})<--(s:FileUpload)--> (n:FileUpload {fuelId: '4ff31ff3-5e9d-4c75-9be3-bc7f95ee6d21'})--> (l:ListIssue {naturalId: 'ACCTX2024', year: 2024})
  RETURN t,l.fuelId as listId


match (o:Organization {visible: false, industry: 'Business Products & Software Services', ceoName: "Rohit Choudhary"})
return o

MATCH (l:ListIssue)-[r:LISTS {visible: true}]-> (n:Organization {visible: false}) RETURN l, r, n






MATCH (n:ListIssue {listUri:'americas-best-startup-employers', year:2023 })-[r:LISTS {visible: true }]->(o:Organization {visible: true })
 WITH count(o) AS org_count
  MATCH (n:ListIssue {listUri:'americas-best-startup-employers', year:2023 })-[r:LISTS {visible: true }]->(o:Organization { visible: true })
   WITH n, r, o, org_count, toInteger(r.rank) as rank, toInteger(r.position) as position
 ORDER BY position ASC, rank ASC
  RETURN collect({\n…nAlt,
    squareImage: o.squareImage,
    yearFounded: o.yearFounded,
    uri: o.uri,
    ceoTitle: o.ceoTitle,
    uris: o.uris,
    phoneNumber: o.phoneNumber,
    allowAccolades: o.allowAccolades,
    phone: o.phone,
    premiumProfile: o.premiumProfile,
    name: o.name,
    embargo: o.embargo,
    employees: o.employees,
    nameSort: o.nameSort,
    altDescription: o.altDescription
  }) AS listIssueNodes, org_count as count