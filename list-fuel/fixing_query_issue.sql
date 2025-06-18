MATCH (listsTemplate :LISTSTypesAggregate) WITH listsTemplate MATCH (
    l :ListIssue { fuelId: '39a5761f-07e2-4481-81c8-155bdf442849' }
) - [r:LISTS] ->(
    e { fuelId: 'a1966857-cf70-4996-a448-0f6a3bc74bae' }
) WITH l,
e,
r,
listsTemplate MATCH (t :EntityTypesAggregate) WITH l,
e,
r,
CASE
    WHEN listsTemplate IS NOT NULL
    AND apoc.meta.cypher.type(listsTemplate.results) = apoc.meta.cypher.type(
        { companyid: null,
        statistaId: null,
        name: null,
        namePrint: null,
        webSite: "http://www.google.com",
        ceoFirstName: null,
        ceoJobTitle: null,
        employees: 183320,
        ticker: null,
        SEDOL: null }
    ) THEN "lists_pass"
    ELSE "failed to update list. expected attribute of type " + apoc.meta.cypher.type(listsTemplate.results)
END AS lists_result0,
CASE
    WHEN apoc.meta.cypher.type(listsTemplate.fuelId) = apoc.meta.cypher.type("a1966857-cf70-4996-a448-0f6a3bc74bae") THEN "lists_pass"
    ELSE "failed to update list. expected attribute of type " + apoc.meta.cypher.type(listsTemplate.fuelId)
END AS lists_result1
SET
    r.results = CASE
        WHEN lists_result0 = "lists_pass" THEN { companyid: null,
        statistaId: null,
        name: null,
        namePrint: null,
        webSite: "http://www.google.com",
        ceoFirstName: null,
        ceoJobTitle: null,
        employees: 183320,
        ticker: null,
        SEDOL: null }
        ELSE r.results
    END,
    r.fuelId = CASE
        WHEN lists_result1 = "lists_pass" THEN "a1966857-cf70-4996-a448-0f6a3bc74bae"
        ELSE r.fuelId
    END RETURN { results: CASE
        WHEN lists_result0 <> "lists_pass" THEN lists_result0
        ELSE "success"
    END },
    { fuelId: CASE
        WHEN lists_result1 <> "lists_pass" THEN lists_result1
        ELSE "success"
    END } -- ////////////////////////////////////////////////////
    -- ////////////////////////////////////////////////////
    -- ////////////////////////////////////////////////////
    -- ////////////////////////////////////////////////////
    -- ////////////////////////////////////////////////////
    -- ////////////////////////////////////////////////////
    -- ////////////////////////////////////////////////////
    -- ////////////////////////////////////////////////////
    -- ////////////////////////////////////////////////////
    MATCH (listsTemplate :LISTSTypesAggregate) WITH listsTemplate MATCH (
        l :ListIssue { fuelId: '39a5761f-07e2-4481-81c8-155bdf442849' }
    ) - [r:LISTS] ->(
        e { fuelId: 'a1966857-cf70-4996-a448-0f6a3bc74bae' }
    ) WITH l,
    e,
    r,
    listsTemplate MATCH (t :EntityTypesAggregate) WITH l,
    e,
    r,
    listsTemplate,
    CASE
        WHEN listsTemplate IS NOT NULL
        AND apoc.meta.cypher.type(listsTemplate.results) = apoc.meta.cypher.type(
            { "companyid" :null,
            "statistaId" :null,
            "name" :null,
            "namePrint" :null,
            "webSite" :"http://www.google.com",
            "ceoFirstName" :null,
            "ceoJobTitle" :null,
            "employees" :183320,
            "ticker" :null,
            "SEDOL" :null }
        ) THEN "lists_pass"
        ELSE "failed to update list. expected attribute of type " + apoc.meta.cypher.type(listsTemplate.results)
    END as lists_result0,
    CASE
        WHEN listsTemplate IS NOT NULL
        AND apoc.meta.cypher.type(listsTemplate.fuelId) = apoc.meta.cypher.type("a1966857-cf70-4996-a448-0f6a3bc74bae") THEN "lists_pass"
        ELSE "failed to update list. expected attribute of type " + apoc.meta.cypher.type(listsTemplate.fuelId)
    END as lists_result1
SET
    r.results = CASE
        WHEN lists_result0 = "lists_pass" THEN { "companyid" :null,
        "statistaId" :null,
        "name" :null,
        "namePrint" :null,
        "webSite" :"http://www.google.com",
        "ceoFirstName" :null,
        "ceoJobTitle" :null,
        "employees" :183320,
        "ticker" :null,
        "SEDOL" :null }
        ELSE r.results
    END,
    r.fuelId = CASE
        WHEN lists_result1 = "lists_pass" THEN "a1966857-cf70-4996-a448-0f6a3bc74bae"
        ELSE r.fuelId
    END RETURN { results: CASE
        WHEN lists_result0 <> "lists_pass" THEN lists_result0
        ELSE "success"
    END },
    { fuelId: CASE
        WHEN lists_result1 <> "lists_pass" THEN lists_result1
        ELSE "success"
    END } 



[
{
  "attributeName": "fuelId",
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "createdAt",
  "inputType": "text",
  "type": "Default"
}
, 
{
  "attributeName": "embargo",
  "inputType": "toggle",
  "type": false
}
, 
{
  "attributeName": "city",
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "country",
  "inputType": "dropdown",
  "type": ""
}
, 
{
  "attributeName": "state",
  "inputType": "dropdown",
  "type": ""
}
, 
{
  "attributeName": "employees",
  "inputType": "number",
  "type": 0
}
, 
{
  "attributeName": "industry",
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "ceoName",
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "ceoTitle",
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "premiumProfile",
  "inputType": "toggle",
  "type": false
}
, 
{
  "attributeName": "description",
  "inputType": "textarea",
  "type": ""
}
, 
{
  "attributeName": "ticker",
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "exchange",
  "inputType": "dropdown",
  "type": ""
}
, 
{
  "attributeName": "organizationName",
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "yearFounded",
  "inputType": "number",
  "type": 0
}
, 
{
  "attributeName": "rank",
  "inputType": "number",
  "type": 0
}
, 
{
  "attributeName": "position",
  "inputType": "number",
  "type": 0
}
, 
{
  "attributeName": "webSite",
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "visible",
  "inputType": "toggle",
  "type": false
}
, 
{
  "attributeName": "squareImage",
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "landscapeImage",
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "portraitImage",
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "phoneNumber",
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "altDescription",
  "inputType": "textarea",
  "type": ""
}
, 
{
  "attributeName": "street",
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "zipCode",
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "cfoName",
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "youTubePlayList",
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "email",
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "relatedVisible",
  "inputType": "toggle",
  "type": false
}
, 
{
  "attributeName": "value",
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "recentContentCount",
  "inputType": "number",
  "type": 0
}
, 
{
  "attributeName": "uri",
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "naturalId",
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "name",
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "createdAt",
  "inputType": "datePicker",
  "type": "2025-06-17T23:12:21.920000000Z"
}
, 
{
  "attributeName": "uris",
  "inputType": "text",
  "type": "Default"
}
, 
{
  "attributeName": "allowAccolades",
  "inputType": "toggle",
  "type": false
}
, 
{
  "attributeName": "quantalyticsMomentumVolatility",
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "quantalyticsBasket",
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "quantalyticsTicker",
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "quantalyticsTechnical",
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "quantalyticsQualityValue",
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "quantalyticsExchange",
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "quantalyticsGrowth",
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "quantalyticsFactorScore",
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "companyId",
  "inputType": "number",
  "type": 0
}
, 
{
  "attributeName": "updatedAt",
  "inputType": "datePicker",
  "type": "2025-06-17T23:12:21.920000000Z"
}
, 
{
  "attributeName": "firstName",
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "lastName",
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "phone",
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "geoLocationLongitude",
  "inputType": "number",
  "type": 0.0
}
, 
{
  "attributeName": "geoLocationLatitude",
  "inputType": "number",
  "type": 0.0
}
, 
{
  "attributeName": "quantalyticsTimestamp",
  "inputType": "datePicker",
  "type": "2025-06-17T23:12:21.920000000Z"
}
, 
{
  "attributeName": "Facebook",
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "Instagram",
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "image",
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "Twitter",
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "LinkedIn",
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "YouTube",
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "ceoJobTitle",
  "isRequired": true,
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "companyid",
  "isRequired": true,
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "ceoFirstName",
  "isRequired": true,
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "namePrint",
  "isRequired": true,
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "statistaId",
  "isRequired": true,
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "SEDOL",
  "isRequired": true,
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "industries",
  "isRequired": true,
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "cEOLastName",
  "isRequired": true,
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "street3",
  "fuelId": "62c2e267-2b93-49ab-b6f9-fe46e627f21f",
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "changeDate",
  "fuelId": "97b3d4c1-bef3-40de-af6f-280a6b06de27",
  "inputType": "number",
  "isReadOnly": true,
  "type": 0.0
}
, 
{
  "attributeName": "changeBy",
  "fuelId": "01e7378b-0db2-45de-b898-28cd68fef79b",
  "inputType": "text",
  "isReadOnly": true,
  "type": ""
}
, 
{
  "attributeName": "secondTicker",
  "fuelId": "0f985f89-5b3c-4b4e-b860-bc65d8a9468d",
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "adrSEDOL",
  "fuelId": "8fb5e8db-d467-4c53-aedf-ae4e3d9bbea1",
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "adrExchange",
  "fuelId": "97f634f6-8226-4208-aa3a-f76c7284eda8",
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "youTubePlayList",
  "fuelId": "a29f7f9d-705a-46e3-8d31-993901d67cad",
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "zipCode",
  "fuelId": "e7e5825f-1c7f-48fa-a0a2-3ea23a4ac0ff",
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "nameTiny",
  "fuelId": "512adfcb-4241-4b74-8295-8f034a05e806",
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "street2",
  "fuelId": "262c4fb5-fcba-4e4e-b794-71555a1231b4",
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "adrTicker",
  "fuelId": "4a384584-b8d6-4280-a74f-222070f59a30",
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "displayName",
  "fuelId": "4c6613da-b73a-48c3-a122-3c1c6bb7795b",
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "nameSort",
  "fuelId": "09ad62ac-d89a-4ccf-8d42-b8446608e634",
  "inputType": "text",
  "type": ""
}
, 
{
  "attributeName": "descriptionLong",
  "fuelId": "11815045-96c1-4872-abb0-b65fc342b5fe",
  "inputType": "textarea",
  "type": ""
}
, 
{
  "attributeName": "altDescription",
  "fuelId": "a501620c-e76d-4229-a569-838e0251474b",
  "inputType": "textarea",
  "type": ""
}
, 
{
  "attributeName": "descriptionShort",
  "fuelId": "3744be12-3c7d-4fad-84c8-b9bcba755eb6",
  "inputType": "textarea",
  "type": ""
}
, 
{
  "attributeName": "descriptionAlt",
  "fuelId": "396e31b4-eb53-4d69-8d32-4e49d44bdca5",
  "inputType": "textarea",
  "type": ""
}
]


{
  "attributes": [
    {
      "attributeName": "fuelId",
      "attributeValue": "a1966857-cf70-4996-a448-0f6a3bc74bae",
      "updateRootEntity": false
  }
  ],
  "entityId": "48ef4523-ed46-4e27-a9f0-d42a58c39b1a",
  "listId": "e828045a-1499-45cf-a3be-bf33e94bfa74",
}