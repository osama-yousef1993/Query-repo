MATCH (n {name: "Apple"})-[r]->(m {name:'IBM'})
return n, m, r.relationshipType