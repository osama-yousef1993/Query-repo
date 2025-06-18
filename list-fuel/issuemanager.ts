async updateListAndEntity(listId: string, entityId: string, attributes: any[]): Promise<any> {
    // Validate inputs
    if (!attributes || !Array.isArray(attributes)) {
        throw new Error("Attributes must be an array");
    }

    // Return early if no attributes to update
    if (attributes.length === 0) {
        return { results: "No attributes provided for update" };
    }

    let caseStatements = '';
    let setStatements = '';
    let results = '';

    attributes.forEach((attribute, index) => {
        if (!attribute || typeof attribute !== 'object') {
            throw new Error(`Invalid attribute at index ${index}`);
        }

        if (attribute.removeAttribute === true) {
            // Handle attribute removal
            setStatements += `r.${attribute.attributeName} = NULL,`;
            if (attribute.updateRootEntity) {
                setStatements += `e.${attribute.attributeName} = NULL,`;
            }
            results += `{${attribute.attributeName}: "success"},";
            return;
        }

        // Format the attribute value for Cypher
        let cypherValue;
        if (typeof attribute.attributeValue === 'object' && attribute.attributeValue !== null) {
            // Convert object to Cypher map syntax
            const entries = Object.entries(attribute.attributeValue)
                .map(([k, v]) => `${k}: ${typeof v === 'string' ? `"${v.replace(/"/g, '\\"')}"` : v}`)
                .join(', ');
            cypherValue = `{${entries}}`;
        } else if (typeof attribute.attributeValue === 'string') {
            cypherValue = `"${attribute.attributeValue.replace(/"/g, '\\"')}"`;
        } else if (attribute.attributeValue === null || attribute.attributeValue === undefined) {
            cypherValue = 'null';
        } else {
            cypherValue = attribute.attributeValue;
        }

        // Check incoming type against the LISTS Types template
        caseStatements += `
            CASE
                WHEN any(attr IN attributes WHERE attr.attributeName = "${attribute.attributeName}") THEN
                    CASE
                        // Skip date validation for objects/maps
                        WHEN any(attr IN attributes WHERE attr.attributeName = "${attribute.attributeName}" AND attr.inputType = "datePicker") AND 
                             typeof(${cypherValue}) = "STRING" AND
                             ${cypherValue} =~ "^\\\\d{4}-\\\\d{2}-\\\\d{2}T\\\\d{2}:\\\\d{2}:\\\\d{2}\\\\.\\\\d+Z$"
                        THEN "lists_pass"
                        // Number validation (both integer and float)
                        WHEN any(attr IN attributes WHERE attr.attributeName = "${attribute.attributeName}" AND attr.inputType = "number") AND 
                             typeof(${cypherValue}) IN ["INTEGER", "FLOAT", "NUMBER"]
                        THEN "lists_pass"
                        // Boolean validation
                        WHEN any(attr IN attributes WHERE attr.attributeName = "${attribute.attributeName}" AND attr.inputType = "toggle") AND 
                             typeof(${cypherValue}) = "BOOLEAN"
                        THEN "lists_pass"
                        // Handle objects/maps
                        WHEN typeof(${cypherValue}) = "MAP" OR typeof(${cypherValue}) = "OBJECT"
                        THEN "lists_pass"
                        // Default string validation (text, textarea, dropdown)
                        WHEN typeof(${cypherValue}) = "STRING"
                        THEN "lists_pass"
                        ELSE "failed to update list. invalid type for ${attribute.attributeName}"
                    END
                ELSE "Attribute ${attribute.attributeName} not in schema"
            END as lists_result${index},`;

        if (attribute.updateRootEntity) {
            // Check incoming type against the entity types
            caseStatements += `
                CASE
                    WHEN any(attr IN attributes WHERE attr.attributeName = "${attribute.attributeName}") THEN
                        CASE
                            WHEN any(attr IN attributes WHERE attr.attributeName = "${attribute.attributeName}" AND attr.inputType = "datePicker") AND 
                                 typeof(${cypherValue}) = "STRING" AND
                                 ${cypherValue} =~ "^\\\\d{4}-\\\\d{2}-\\\\d{2}T\\\\d{2}:\\\\d{2}:\\\\d{2}\\\\.\\\\d+Z$"
                            THEN "t_pass"
                            WHEN any(attr IN attributes WHERE attr.attributeName = "${attribute.attributeName}" AND attr.inputType = "number") AND 
                                 typeof(${cypherValue}) IN ["INTEGER", "FLOAT", "NUMBER"]
                            THEN "t_pass"
                            WHEN any(attr IN attributes WHERE attr.attributeName = "${attribute.attributeName}" AND attr.inputType = "toggle") AND 
                                 typeof(${cypherValue}) = "BOOLEAN"
                            THEN "t_pass"
                            WHEN typeof(${cypherValue}) = "MAP" OR typeof(${cypherValue}) = "OBJECT"
                            THEN "t_pass"
                            WHEN typeof(${cypherValue}) = "STRING"
                            THEN "t_pass"
                            ELSE "Entity type mismatch for ${attribute.attributeName}"
                        END
                    ELSE "Attribute ${attribute.attributeName} not in schema"
                END as t_result${index},`;

            // Set entity value only if both type checks pass
            setStatements += `
                e.${attribute.attributeName} = CASE
                    WHEN t_result${index} = "t_pass" AND lists_result${index} = "lists_pass"
                    THEN ${cypherValue}
                    ELSE e.${attribute.attributeName}
                END,
                r.${attribute.attributeName} = CASE
                    WHEN t_result${index} = "t_pass" AND lists_result${index} = "lists_pass"
                    THEN ${cypherValue}
                    ELSE r.${attribute.attributeName}
                END,`;
        } else {
            // Set LISTS value only if type check passes
            setStatements += `
                r.${attribute.attributeName} = CASE
                    WHEN lists_result${index} = "lists_pass"
                    THEN ${cypherValue}
                    ELSE r.${attribute.attributeName}
                END,`;
        }

        // Add result message
        results += `
            {${attribute.attributeName}: CASE
                ${attribute.updateRootEntity ? `WHEN t_result${index} <> "t_pass" THEN t_result${index}` : ''}
                WHEN lists_result${index} <> "lists_pass" THEN lists_result${index}
                ELSE "success"
            END},`;
    });

    // Construct the Cypher query
    const query = `
        MATCH (n:ListSchemaAttribute)
        WITH collect(distinct n {.*}) as attributes
        MATCH (l:ListIssue {fuelId: $listId})-[r:LISTS]->(e {fuelId: $entityId})
        WITH l, e, r, attributes, ${caseStatements.trim().slice(0, -1)}
        SET ${setStatements.trim().slice(0, -1)}
        RETURN ${results.trim().slice(0, -1)}
    `;

    console.log('DEBUG Cypher Query:\n', query);

    // Execute the query
    const setListIssue = executeQueryAndReturnObservable(
        query,
        { listId, entityId },
        true,
        'Successfully updated ListIssue status',
        'Error updating ListIssue status'
    );

    // Wait for the observable to complete and return the result
    const setUnfoldListIssue = firstValueFrom(setListIssue);

    return setUnfoldListIssue;
}