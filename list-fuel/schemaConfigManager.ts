/**
 * @file src/schemaConfigManager.ts
 * @description Manages ListTableSchema and ListUploadSchema in Neo4j.
 */
import { inject } from 'inversify';
import { ILogger, LOGGER } from '@forbes/lists-fuel-logger';
import {
  executeCypherRead,
  executeQueryAndReturnObservable,
} from './databaseManager';
import { Observable, of } from 'rxjs';
import { map, tap, toArray, switchMap } from 'rxjs/operators';
import { Config } from '@forbes/lists-fuel-config';
import { SchemaAttributeGroup, SchemaConfigAttribute } from './types';

/**
 * The SchemaConfigManager class manages ListTableSchema and ListUploadSchema in Neo4j.
 * It provides methods for creating, updating, and retrieving schema definitions that are
 * used for ListIssues and People, Organization and Place nodes when not in a list context.
 *
 * This is different than SchemaManager which manages the schema of the entire database.
 */
export class SchemaConfigManager {
  private _logger: ILogger | null;
  private verbose: boolean = false;
  private _mockAttributes: SchemaConfigAttribute[] = [];
  private mockForTests: boolean = false;

  /**
   * Constructs a SchemaConfigManager instance.
   * @param _logger Optional logger for logging information and errors.
   */
  constructor(@inject(LOGGER.Logger) _logger?: ILogger) {
    // Assign provided logger or default to null
    this._logger = _logger ?? null;
    // Determine verbose logging from configuration (FUEL_DEBUG flag)
    this.verbose = 'false' !== Config.getOrDefault('FUEL_DEBUG', 'false');
    // testing
    this.verbose = true;
  }

  /**
   * Creates/Updates a ListTableSchema from Neo4j using the fuelId of the ListIssue.
   * Builds a dynamic Cypher query based on the provided attributes.
   * @param attributes Array of attribute objects for the schema.
   * @param issueFuelId The fuelId of the ListIssue.
   * @param schemaId The unique identifier for the schema.
   * @returns A string representing the complete Cypher query.
   */
  getAttributesQuery(
    attributes: any[],
    issueFuelId: string,
    schemaId: string,
    schemaType: 'ListTableSchema' | 'ListUploadSchema' = 'ListTableSchema'
  ): string {
    // Log the operation with the number of attributes being processed.
    this._logger.info(`[FUEL][INFO] Creating or updating schema with ${attributes.length} attributes: schema ${schemaId} for issue ${issueFuelId}`);

    // Process the attributes array to ensure default values are set for each attribute.
    if (attributes && attributes.length > 0) {
      attributes = attributes.map((attr, index) => ({
        ...attr,
        // Assign default order if not provided
        order: attr.order || index + 1,
        attributeType: attr.attributeType || 'string',
        displayLabel: attr.displayLabel || '',
        isSearchable: attr.isSearchable || false,
        isFilterable: attr.isFilterable || false,
        isRequired: attr.isRequired || false,
        isHidden: attr.isHidden || false,
        isUnique: attr.isUnique || false,
        isReadOnly: attr.isReadOnly || false,
        isSortable: attr.isSortable !== undefined ? attr.isSortable : true,
        isExportable: attr.isExportable !== undefined ? attr.isExportable : true,
        isImportable: attr.isImportable !== undefined ? attr.isImportable : true,
        isNullable: attr.isNullable || false,
        isAutoIncrement: attr.isAutoIncrement || false,
        isPrimaryKey: attr.isPrimaryKey || false,
        isSecondaryKey: attr.isSecondaryKey || false,
        groupId: attr.groupId || '',
        groupOrder: attr.groupOrder || 0,
        groupLabel: attr.groupLabel || '',
        groupIsCollapsed: attr.groupIsCollapsed || false,
        groupIsHidden: attr.groupIsHidden || false,
        groupIsDefault: attr.groupIsDefault || false,
        groupDescription: attr.groupDescription || '',
        minimumRoleEdit: attr.minimumRoleEdit || 'NONE',
        minimumRoleView: attr.minimumRoleView || 'NONE',
        minimumRoleExport: attr.minimumRoleExport || 'NONE',
        minimumRoleImport: attr.minimumRoleImport || 'NONE',
        isCategoryRankLabel: attr.isCategoryRankLabel || false,
        isCategoryRankSlug: attr.isCategoryRankSlug || false,
        isCategoryRankValue: attr.isCategoryRankValue || false
      }));

      // Log that attributes have been processed
      this._logger.info(`Processing ${attributes.length} attributes`);

      // If verbose logging is enabled, output the full details of attributes
      if (this.verbose) {
        this._logger.info(`Attribute details:`, JSON.stringify(attributes, null, 2));
      }
    }

    let query = '';

    // Begin building the query: if issueFuelId is provided, link the schema to the ListIssue node.
    if (issueFuelId && '' !== issueFuelId) {
      query += `
        // Match the ListIssue node by its fuelId.
        MATCH (issue:ListIssue { fuelId: $issueFuelId })

        // Merge the ListTableSchema node (if it doesn't already exist).
        MERGE (schema:SchemaConfig:${schemaType} { fuelId: $schemaId })

        // Create the new USES relationship with the updated properties.
        MERGE (issue)-[u:USES]->(schema)

        // Set or update the relationship fuelId
        ON CREATE SET u.fuelId = randomUUID()
        ON MATCH SET u.fuelId = COALESCE(u.fuelId, randomUUID())

        WITH schema
      `;
    } else {
      // If no issue fuelId is provided, only merge the schema node.
      query += `
        // Merge the ListTableSchema node (if it doesn't already exist).
        MERGE (schema:SchemaConfig:${schemaType} { fuelId: $schemaId })
        WITH schema
      `;
    }

    // CRITICAL: Delete any existing HAS_ATTRIBUTE relationships to start with a clean slate.
    query += `
      // First delete any existing relationships to clean the slate
      OPTIONAL MATCH (schema)-[existingRel:HAS_ATTRIBUTE]->(:SchemaConfigAttribute)
      WITH schema, collect(existingRel) as rels
      FOREACH (rel IN rels | DELETE rel)
      WITH schema
    `;

    if (this.verbose) {
      this._logger.info(`[FUEL][DEBUG] getAttributesQuery attributes count: ${attributes.length}`);
    }
    // For each attribute, merge the attribute node and create a new HAS_ATTRIBUTE relationship with its properties.
    if (attributes && attributes.length > 0) {
      attributes.forEach((attr, index) => {
        query += `
          // Process attribute ${index}: ${attr.attributeName}
          MERGE (attr${index}:SchemaConfigAttribute { attributeName: $attributes[${index}].attributeName })
          ON CREATE SET
          attr${index}.fuelId = randomUUID(),
          attr${index}.attributeType =  $attributes[${index}].attributeType

          // Create a new HAS_ATTRIBUTE relationship with all properties
          Merge (schema)-[rel${index}:HAS_ATTRIBUTE]->(attr${index})
          ON CREATE SET rel${index} += {
            order: COALESCE($attributes[${index}].order, ${index + 1}),
            attributeType: COALESCE($attributes[${index}].attributeType, 'string'),
            format: COALESCE($attributes[${index}].format, ''),
            displayLabel: COALESCE($attributes[${index}].displayLabel, ''),
            isSearchable: COALESCE($attributes[${index}].isSearchable, false),
            isFilterable: COALESCE($attributes[${index}].isFilterable, false),
            isRequired: COALESCE($attributes[${index}].isRequired, false),
            isHidden: COALESCE($attributes[${index}].isHidden, false),
            isUnique: COALESCE($attributes[${index}].isUnique, false),
            isReadOnly: COALESCE($attributes[${index}].isReadOnly, false),
            isSortable: COALESCE($attributes[${index}].isSortable, true),
            isExportable: COALESCE($attributes[${index}].isExportable, true),
            isImportable: COALESCE($attributes[${index}].isImportable, true),
            isNullable: COALESCE($attributes[${index}].isNullable, false),
            isAutoIncrement: COALESCE($attributes[${index}].isAutoIncrement, false),
            isPrimaryKey: COALESCE($attributes[${index}].isPrimaryKey, false),
            isSecondaryKey: COALESCE($attributes[${index}].isSecondaryKey, false),
            groupId: COALESCE($attributes[${index}].groupId, ''),
            groupOrder: COALESCE($attributes[${index}].groupOrder, 0),
            groupLabel: COALESCE($attributes[${index}].groupLabel, ''),
            groupIsCollapsed: COALESCE($attributes[${index}].groupIsCollapsed, false),
            groupIsHidden: COALESCE($attributes[${index}].groupIsHidden, false),
            groupIsDefault: COALESCE($attributes[${index}].groupIsDefault, false),
            groupDescription: COALESCE($attributes[${index}].groupDescription, ''),
            minimumRoleEdit: COALESCE($attributes[${index}].minimumRoleEdit, 'NONE'),
            minimumRoleView: COALESCE($attributes[${index}].minimumRoleView, 'NONE'),
            minimumRoleExport: COALESCE($attributes[${index}].minimumRoleExport, 'NONE'),
            minimumRoleImport: COALESCE($attributes[${index}].minimumRoleImport, 'NONE'),
            isCategoryRankLabel: COALESCE($attributes[${index}].isCategoryRankLabel, false),
            isCategoryRankSlug: COALESCE($attributes[${index}].isCategoryRankSlug, false),
            isCategoryRankValue: COALESCE($attributes[${index}].isCategoryRankValue, false),
            fuelId: randomUUID()
          }

          WITH schema
        `;
      });
    }

    // Append the final RETURN clause to output the schema node.
    query += `
    RETURN schema`;

    if (this.verbose) {
      this._logger.info(`[FUEL][DEBUG] getAttributesQuery query: ${query}`);
    }

    return query;
  }

  /**
   * Retrieves all schemas linked to a ListIssue.
   * @param issueFuelId The fuelId of the ListIssue.
   * @returns An observable containing an array of normalized schema data.
   */
  getAllListSchema(issueFuelId: string): Observable<any[]> {
    const query = `
      MATCH (issue:ListIssue { fuelId: $issueFuelId })-[:USES]->(schema)
      RETURN collect(schema) as schemas
    `;

    return executeCypherRead(query, { issueFuelId }).pipe(
      tap(result => {
        if (this.verbose) {
          this._logger.info(`[FUEL][DEBUG] getAllListSchema raw response for issueFuelId: ${issueFuelId}`,
            JSON.stringify(result, null, 2));
        }
      }),
      map(result => {
        if (!result || !result._fields || result._fields.length === 0 || !result._fields[0]) {
          this._logger.warn(`[FUEL][WARN] No schemas found for ListIssue with fuelId: ${issueFuelId}`);
          return [];
        }

        // Extract all schemas from the collected result
        const schemaNodes = result._fields[0];

        if (!Array.isArray(schemaNodes)) {
          this._logger.warn(`[FUEL][WARN] Unexpected result format for getAllListSchema`);
          return [];
        }

        // Map each schema node to its properties
        const schemas = schemaNodes.map(node => {
          if (!node || !node.properties) return null;
          return {
            ...node.properties,
            schemaType: node.labels[0]
          };
        }).filter(schema => schema !== null);

        this._logger.info(`[FUEL][INFO] Found ${schemas.length} schemas for ListIssue: ${issueFuelId}`, schemas);

        return schemas;
      })
    );
  }

  /**
   * Retrieves a ListTableSchema from Neo4j using the fuelId of the ListIssue.
   * Executes a Cypher query that finds the schema linked to a specific ListIssue.
   * @param issueFuelId The fuelId of the ListIssue.
   * @returns An observable containing the normalized schema data.
   */
  getListTableSchema(issueFuelId: string): Observable<any> {
    // Define the Cypher query to match the ListIssue and its related ListTableSchema
    const query = `
      MATCH (issue:ListIssue { fuelId: $issueFuelId })-[:USES]->(schema:SchemaConfig:ListTableSchema)
      RETURN schema
    `;

    // Execute the read query and normalize the result before returning.
    return executeCypherRead(query, { issueFuelId }).pipe(
      map(result => this.normalizeSchemaResponse(result))
    );
  }

  /**
   * A helper function for testing.
   * Sets the mock attributes array to the provided array.
   * @param attributes
   **/
  public setMockAttributes(attributes: SchemaConfigAttribute[]): void {
    this._mockAttributes = attributes;
    this.mockForTests = true;
  }

  /**
   * A helper function for testing.
   * Resets the mock attributes array to an empty array.
   */
  public resetMockAttributes(): void {
    this._mockAttributes = [];
    this.mockForTests = false;
  }

  /**
   * Retrieves a ListTableSchema from Neo4j using the fuelId of the ListIssue.
   * @param schemaId The fuelId of the schema.
   * @returns An observable containing the normalized schema data.
   */
  public getSchemaAttributesFlat(schemaId: string): Observable<SchemaConfigAttribute[]> {
    if (this.verbose) {
      this._logger.info(`[FUEL][INFO] Fetching attributes for schema: ${schemaId}`);
    }

    // For tests - return mock data directly
    if (this.mockForTests) {
      return of(this._mockAttributes);
    }

    // First check if the schema exists
    return this.checkSchemaExists(schemaId).pipe(
      tap(exists => {
        if (this.verbose) {
          this._logger.info(`[FUEL][DEBUG] Schema existence check: ${exists ? 'Schema exists' : 'Schema does not exist'}`);
        }
        if (!exists) {
          this._logger.warn(`[FUEL][WARN] No schema found with fuelId: ${schemaId}`);
        }
      }),
      switchMap(exists => {
        if (!exists) {
          return of([]);
        }

        // Now check if schema has attributes
        return this.checkSchemaHasAttributes(schemaId).pipe(
          tap(hasAttributes => {
            if (this.verbose) {
              this._logger.info(`[FUEL][DEBUG] Schema has attributes check: ${hasAttributes ? 'Has attributes' : 'No attributes'}`);
            }
            if (!hasAttributes) {
              this._logger.warn(`[FUEL][WARN] Schema with fuelId: ${schemaId} has no attributes`);
            }
          }),
          switchMap(hasAttributes => {
            if (!hasAttributes) {
              return of([]);
            }

            // Main query: uses SchemaConfig generically (works for both TableSchema and UploadSchema)
            const query = `
              MATCH (schema:SchemaConfig { fuelId: $schemaId })-[r:HAS_ATTRIBUTE]->(attr:SchemaConfigAttribute)
              RETURN
                attr.attributeName           AS attributeName,
                r.attributeType              AS attributeType,
                r.displayLabel               AS displayLabel,
                r.order                      AS order,
                r.isSearchable               AS isSearchable,
                r.isFilterable               AS isFilterable,
                r.isRequired                 AS isRequired,
                r.isHidden                   AS isHidden,
                r.isUnique                   AS isUnique,
                r.isReadOnly                 AS isReadOnly,
                r.isSortable                 AS isSortable,
                r.isExportable               AS isExportable,
                r.isImportable               AS isImportable,
                r.isNullable                 AS isNullable,
                r.isAutoIncrement            AS isAutoIncrement,
                r.isPrimaryKey               AS isPrimaryKey,
                r.isSecondaryKey             AS isSecondaryKey,
                r.groupId                    AS groupId,
                r.groupOrder                 AS groupOrder,
                r.groupLabel                 AS groupLabel,
                r.groupIsCollapsed           AS groupIsCollapsed,
                r.groupIsHidden              AS groupIsHidden,
                r.groupIsDefault             AS groupIsDefault,
                r.groupDescription           AS groupDescription,
                r.minimumRoleEdit            AS minimumRoleEdit,
                r.minimumRoleView            AS minimumRoleView,
                r.minimumRoleExport          AS minimumRoleExport,
                r.minimumRoleImport          AS minimumRoleImport,
                r.isCategoryRankLabel        AS isCategoryRankLabel,
                r.isCategoryRankSlug         AS isCategoryRankSlug,
                r.isCategoryRankValue        AS isCategoryRankValue,
                r.validationRules            AS validationRules,
                r.validOptions               AS validOptions,
                r.validValues                AS validValues,
                r.defaultValue               AS defaultValue,
                r.delimiter                  AS delimiter
            `;

            if (this.verbose) {
              this._logger.info(`[FUEL][DEBUG] getSchemaAttributesFlat query: ${query}`);
            }

            return executeCypherRead(query, { schemaId }).pipe(
              toArray(), // Collects all record emissions into a single array
              map(records => {
                if (!records || records.length === 0) {
                  this._logger.warn(`[FUEL][WARN] No attributes found for schemaId: ${schemaId}`);
                  return [];
                }

                const attributes = records
                  .filter(record => record !== null && record !== undefined)
                  .map(record => {
                    if (record && typeof record.get === 'function') {
                      try {
                        return {
                          attributeName: record.get('attributeName') || '',
                          attributeType: record.get('attributeType') || 'string',
                          displayLabel: record.get('displayLabel') || '',
                          order: record.get('order') || 0,
                          isSearchable: record.get('isSearchable') ?? false,
                          isFilterable: record.get('isFilterable') ?? false,
                          isRequired: record.get('isRequired') ?? false,
                          isHidden: record.get('isHidden') ?? false,
                          isUnique: record.get('isUnique') ?? false,
                          isReadOnly: record.get('isReadOnly') ?? false,
                          isSortable: record.get('isSortable') ?? true,
                          isExportable: record.get('isExportable') ?? true,
                          isImportable: record.get('isImportable') ?? true,
                          isNullable: record.get('isNullable') ?? false,
                          isAutoIncrement: record.get('isAutoIncrement') ?? false,
                          isPrimaryKey: record.get('isPrimaryKey') ?? false,
                          isSecondaryKey: record.get('isSecondaryKey') ?? false,
                          groupId: record.get('groupId') ?? '',
                          groupOrder: record.get('groupOrder') ?? 0,
                          groupLabel: record.get('groupLabel') ?? '',
                          groupIsCollapsed: record.get('groupIsCollapsed') ?? false,
                          groupIsHidden: record.get('groupIsHidden') ?? false,
                          groupIsDefault: record.get('groupIsDefault') ?? false,
                          groupDescription: record.get('groupDescription') ?? '',
                          minimumRoleEdit: record.get('minimumRoleEdit') ?? 'NONE',
                          minimumRoleView: record.get('minimumRoleView') ?? 'NONE',
                          minimumRoleExport: record.get('minimumRoleExport') ?? 'NONE',
                          minimumRoleImport: record.get('minimumRoleImport') ?? 'NONE',
                          isCategoryRankLabel: record.get('isCategoryRankLabel') ?? false,
                          isCategoryRankSlug: record.get('isCategoryRankSlug') ?? false,
                          isCategoryRankValue: record.get('isCategoryRankValue') ?? false,
                          validValues: record.get('validValues') || [],
                          defaultValue: record.get('defaultValue'),
                          delimiter: record.get('delimiter')
                        };
                      } catch (error) {
                        this._logger.error(`[FUEL][ERROR] Error processing record: ${error.message}`);
                        return null;
                      }
                    } else {
                      this._logger.error(`[FUEL][ERROR] Invalid record structure: ${JSON.stringify(record, null, 2)}`);
                      return null;
                    }
                  })
                  .filter(attr => attr !== null);

                if (this.verbose) {
                  this._logger.info(`[FUEL][DEBUG] getSchemaAttributesFlat attributes: ${JSON.stringify(attributes, null, 2)}`);
                  this._logger.info(`[FUEL][INFO] Returning ${attributes.length} attributes for schema: ${schemaId}`);
                }

                return attributes;
              })
            );
          })
        );
      })
    );
  }

  /**
   * Check if a schema exists in the database.
   * @param schemaId The fuelId of the schema.
   * @returns An observable containing a boolean indicating if the schema exists.
   */
  checkSchemaExists(schemaId: string): Observable<boolean> {
    const query = `
      MATCH (schema:SchemaConfig { fuelId: $schemaId })
      RETURN count(schema) > 0 as exists
    `;

    return executeCypherRead(query, { schemaId }).pipe(
      map(result => {
        if (!result || !result._fields || result._fields.length === 0) {
          return false;
        }
        return result._fields[0]; // This is a boolean value
      })
    );
  }

  /**
   * Check if a schema has attributes.
   * Checks if the schema has any HAS_ATTRIBUTE relationships.
   * @param schemaId The fuelId of the schema.
   * @returns An observable containing a boolean indicating if attributes exist.
   */
  checkSchemaHasAttributes(schemaId: string): Observable<boolean> {
    const query = `
      MATCH (schema:SchemaConfig { fuelId: $schemaId })-[r:HAS_ATTRIBUTE]->()
      RETURN count(r) > 0 as hasAttributes
    `;

    return executeCypherRead(query, { schemaId }).pipe(
      map(result => {
        if (!result || !result._fields || result._fields.length === 0) {
          return false;
        }
        return result._fields[0];
      })
    );
  }

  /**
   * Creates or updates a ListTableSchema in Neo4j.
   * Ensures that every node has a unique fuelId and links it to the corresponding ListIssue.
   * @param schemaData The schema JSON object.
   * @param issueId The ListIssue fuelId to link the schema.
   * @returns An observable of the execution results.
   */
  createOrUpdateListTableSchema(schemaData: any, issueId: string): Observable<any> {
    // Validate the input schema data; if invalid, return an error observable.
    if (!this.validateSchemaInput(schemaData)) {
      return of({ error: 'Invalid schema data format' });
    }

    // Ensure the attributes array exists and contains elements.
    if (!schemaData.attributes || !Array.isArray(schemaData.attributes) || schemaData.attributes.length === 0) {
      this._logger.warn('[FUEL][WARN] No attributes provided for schema update');
      return of({ error: 'No attributes provided' });
    }

    // Log the start of the create/update process with key details.
    this._logger.info(`[FUEL][INFO] Creating/updating TableSchema schemaId (fuelId) ${schemaData.schemaId} with ${schemaData.attributes.length} attributes for issue: ${issueId}`);

    // Build the Cypher query for creating/updating the schema using the helper function.
    const query = this.getAttributesQuery(schemaData.attributes, issueId, schemaData.schemaId, 'ListTableSchema');

    // Log detailed debug information if verbose mode is enabled.
    if (this.verbose) {
      this._logger.info(`[FUEL][DEBUG] createOrUpdateListTableSchema query: ${query}`);
      this._logger.info(`[FUEL][DEBUG] Query params:`, {
        schemaId: schemaData.schemaId,
        issueId,
        attributesCount: schemaData.attributes.length,
      });
    }

    // Execute the query and return the observable; also use tap to log post-update actions.
    return executeQueryAndReturnObservable(
      query,
      {
        schemaId: schemaData.schemaId,
        attributes: schemaData.attributes,
        issueFuelId: issueId
      },
      true,
      'Successfully created or updated ListTableSchema',
      'Error creating or updating ListTableSchema'
    ).pipe(
      tap(result => {
        // Log the successful schema update.
        this._logger.info(`[FUEL][INFO] Schema update complete: ` + query, result);

        // Add a delay before verification to ensure Neo4j has processed the transaction.
        setTimeout(() => {
          // Verify the updated schema by fetching the attributes.
          this.getSchemaAttributesFlat(schemaData.schemaId).subscribe(
            attributes => {
              this._logger.info(`[FUEL][INFO] Verified schema attributes after update: ${attributes.length} attributes found`);
              if (this.verbose) {
                this._logger.info(`[FUEL][DEBUG] Retrieved attributes:`, JSON.stringify(attributes, null, 2));
              }
            },
            error => {
              // Log an error if verification fails.
              this._logger.error(`[FUEL][ERROR] Failed to verify schema attributes: ${error.message}`);
            }
          );
        }, 1000); // Add a 1-second delay
      })
    );
  }

  /**
   * Retrieves a ListUploadSchema from Neo4j using the fuelId of the ListIssue.
   * Executes a Cypher query to find the ListUploadSchema linked to a specific ListIssue.
   * @param issueFuelId The fuelId of the ListIssue.
   * @returns An observable containing the normalized schema data.
   */
  getListUploadSchema(issueFuelId: string): Observable<any> {
    // Define the query to match the ListIssue and its ListUploadSchema relationship.
    const query = `
      MATCH (issue:ListIssue { fuelId: $issueFuelId })-[:USES]->(schema:SchemaConfig:ListUploadSchema)
      RETURN schema
    `;

    // Execute the read query and normalize the response.
    return executeCypherRead(query, { issueFuelId }).pipe(
      map(result => this.normalizeSchemaResponse(result))
    );
  }

  /**
   * Creates or updates a ListUploadSchema in Neo4j.
   * Ensures that every node has a unique fuelId and links it to the corresponding ListIssue.
   * @param schemaData The schema JSON object.
   * @param issueFuelId The ListIssue fuelId to link the schema.
   * @returns An observable of the execution results.
   */
  createOrUpdateListUploadSchema(schemaData: any, issueFuelId: string): Observable<any> {
    // Validate the input schema data.
    if (!this.validateSchemaInput(schemaData)) {
      return of({ error: 'Invalid schema data format' });
    }

    // Check for empty attributes array and warn if none are provided.
    if (!schemaData.attributes || !Array.isArray(schemaData.attributes) || schemaData.attributes.length === 0) {
      this._logger.warn('[FUEL][WARN] No attributes provided for ListUploadSchema update');
      return of({ error: 'No attributes provided' });
    }

    // Log the start of the create/update process with key details.
    this._logger.info(`[FUEL][INFO] Creating/updating UploadSchema schemaId (fuelId) ${schemaData.schemaId} with ${schemaData.attributes.length} attributes for issue: ${issueFuelId}`);

    // Build the Cypher query for creating/updating the schema using the helper function.
    const query = this.getAttributesQuery(schemaData.attributes, issueFuelId, schemaData.schemaId, 'ListUploadSchema');

    // Log detailed debug information if verbose mode is enabled.
    if (this.verbose) {
      this._logger.info(`[FUEL][DEBUG] createOrUpdateListUploadSchema query: ${query}`);
      this._logger.info(`[FUEL][DEBUG] Query params:`, {
        schemaId: schemaData.schemaId,
        issueFuelId,
        attributesCount: schemaData.attributes.length,
      });
    }

    // Execute the main query to create/update the schema
    return executeQueryAndReturnObservable(
      query,
      {
        schemaId: schemaData.schemaId,
        attributes: schemaData.attributes,
        issueFuelId
      },
      true,
      'Successfully created or updated ListUploadSchema',
      'Error creating or updated ListUploadSchema'
    ).pipe(
      // Use switchMap to chain the second query properly
      switchMap(result => {
        // Set additional schema properties query
        const setPropertiesQuery = `
          MATCH (schema:SchemaConfig:ListUploadSchema { fuelId: $schemaId })
          SET schema.internalName = $internalName
          RETURN schema
        `;

        // Log successful update
        this._logger.info(`[FUEL][INFO] Schema update complete for UploadSchema`);

        // After a delay, verify schema attributes
        setTimeout(() => {
          this.getSchemaAttributesFlat(schemaData.schemaId).subscribe(
            attributes => {
              this._logger.info(`[FUEL][INFO] Verified schema attributes after update: ${attributes.length} attributes found`);
              if (this.verbose) {
                this._logger.info(`[FUEL][DEBUG] Retrieved attributes:`, JSON.stringify(attributes, null, 2));
              }
            },
            error => {
              this._logger.error(`[FUEL][ERROR] Failed to verify schema attributes: ${error.message}`);
            }
          );
        }, 1000);

        // Return the original result, we don't need to wait for the second query
        return of(result);
      })
    );
  }

  /**
   * Retrieves a ListTableSchema from Neo4j using fuelId.
   * Executes a query to find the schema node based on its unique fuelId.
   * @param schemaId The unique fuelId of the schema.
   * @returns An observable containing the normalized schema data.
   */
  getListTableSchemaByFuelId(schemaId: string): Observable<any> {
    // Define the query to fetch the schema by its fuelId.
    const query = `
      MATCH (s:SchemaConfig:ListTableSchema { fuelId: $schemaId })
      RETURN s
    `;

    // Log the operation if verbose mode is enabled.
    if (this.verbose) {
      this._logger.info(`Fetched getListTableSchemaByFuelId (schemaId): ${schemaId}`, query);
    }

    // Execute the query and normalize the result.
    return executeCypherRead(query, { schemaId }).pipe(
      map(result => this.normalizeSchemaResponse(result))
    );
  }

  /**
   * Processes a format string using a data object to generate a formatted output.
   * Replaces placeholders (e.g. {key}) in the string with corresponding values from data.
   * @param formatString The format string with {placeholders}
   * @param data Object containing values for the placeholders
   * @returns The formatted string with placeholders replaced by values
   */
  processFormatString(formatString: string, data: any): string {
    // Use a regular expression to find placeholders and replace them with actual data values.
    return formatString.replace(/\{(\w+)\}/g, (match, key) => {
      return data[key] !== undefined ? data[key] : match;
    });
  }

  /**
   * Evaluates all attribute groups for a given data object.
   * Retrieves the schema attribute groups and processes any derived groups based on a format.
   * @param schemaFuelId The schema to use for group definitions.
   * @param data The data object to evaluate.
   * @returns An Observable with the original data enhanced with derived group fields.
   */
  evaluateAttributeGroups(schemaFuelId: string, data: any): Observable<any> {
    // Get attribute groups for the schema and process each group.
    return this.getSchemaAttributeGroups(schemaFuelId).pipe(
      map(groups => {
        const result = { ...data };

        // For each group, if it is marked as derived, process its format string.
        groups.forEach(group => {
          if (group.isDerived) {
            // Replace placeholders with actual values from data.
            result[group.schemaGroupId] = this.processFormatString(group.schemaGroupFormat, data);
          }
        });

        return result;
      })
    );
  }

  /**
   * Validates the schema input structure.
   * Checks that schemaData is an object, contains a schemaId, and has a valid attributes array.
   * @param schemaData The schema JSON object.
   * @returns Boolean indicating whether the schema is valid.
   */
  private validateSchemaInput(schemaData: any): boolean {
    // Ensure the input is a valid object.
    if (!schemaData || typeof schemaData !== 'object') {
      this._logger.error('[FUEL][ERROR] Invalid schema data: not an object');
      return false;
    }

    // Check for presence of schemaId.
    if (!schemaData.schemaId) {
      this._logger.error('[FUEL][ERROR] Invalid schema data: missing fuelId');
      return false;
    }

    // Ensure attributes exist and are an array.
    if (!schemaData.attributes || !Array.isArray(schemaData.attributes)) {
      this._logger.error('[FUEL][ERROR] Invalid schema data: missing or invalid attributes');
      return false;
    }

    return true;
  }

  /**
   * Creates a new SchemaAttributeGroup and links it to a schema.
   * Merges the group node, sets its properties, and creates relationships to associated attributes.
   * @param schemaFuelId The fuelId of the schema (ListUploadSchema or ListTableSchema).
   * @param group The SchemaAttributeGroup object with configuration.
   * @returns An observable confirming the group creation.
   */
  createAttributeGroup(schemaFuelId: string, group: SchemaAttributeGroup): Observable<any> {
    // Validate required fields for the group.
    if (!group.schemaGroupId || !group.schemaGroupName || !group.schemaGroupFormat) {
      return of({ error: 'Invalid group data. Required fields: schemaGroupId, schemaGroupName, schemaGroupFormat' });
    }

    // Convert validation rules to JSON string for storage if they exist.
    const validationRulesJson = group.validationRules ? JSON.stringify(group.validationRules) : null;

    // Build the query to merge the group node and link it to the schema.
    const query = `
      MATCH (schema { fuelId: $schemaFuelId })
      MERGE (g:SchemaAttributeGroup { schemaGroupId: $group.schemaGroupId })
      SET g.schemaGroupName = $group.schemaGroupName,
          g.schemaGroupFormat = $group.schemaGroupFormat,
          g.isDerived = $group.isDerived,
          g.validationRules = $validationRulesJson
      MERGE (schema)-[:HAS_GROUP]->(g)
      WITH g

      // Remove all existing attribute relationships to reset group associations.
      OPTIONAL MATCH (g)-[r:INCLUDES_ATTRIBUTE]-()
      DELETE r

      // Create new relationships for all attributes specified in the group.
      WITH g
      UNWIND $group.attributes AS attrName
      MATCH (attr:SchemaConfigAttribute { attributeName: attrName })
      MERGE (g)-[:INCLUDES_ATTRIBUTE]->(attr)

      RETURN g
    `;

    // Execute the query and return an observable with the result.
    return executeQueryAndReturnObservable(
      query,
      { schemaFuelId, group, validationRulesJson },
      true,
      'Successfully created attribute group',
      'Error creating attribute group'
    );
  }

  /**
   * Updates an existing SchemaAttributeGroup.
   * Builds a dynamic query to update provided fields and resets attribute relationships if needed.
   * @param groupId The schemaGroupId of the group to update.
   * @param groupData The updated SchemaAttributeGroup data.
   * @returns An observable confirming the update.
   */
  updateAttributeGroup(groupId: string, groupData: Partial<SchemaAttributeGroup>): Observable<any> {
    // Validate that a groupId is provided.
    if (!groupId) {
      return of({ error: 'Invalid groupId' });
    }

    let setClause = '';
    const params: any = { groupId };

    // Dynamically build the SET clause for fields that are provided.
    if (groupData.schemaGroupName) {
      setClause += 'g.schemaGroupName = $schemaGroupName, ';
      params.schemaGroupName = groupData.schemaGroupName;
    }

    if (groupData.schemaGroupFormat) {
      setClause += 'g.schemaGroupFormat = $schemaGroupFormat, ';
      params.schemaGroupFormat = groupData.schemaGroupFormat;
    }

    if (groupData.isDerived !== undefined) {
      setClause += 'g.isDerived = $isDerived, ';
      params.isDerived = groupData.isDerived;
    }

    if (groupData.validationRules) {
      setClause += 'g.validationRules = $validationRules, ';
      params.validationRules = JSON.stringify(groupData.validationRules);
    }

    // Remove trailing comma and space if setClause is not empty.
    if (setClause) {
      setClause = 'SET ' + setClause.slice(0, -2) + ' ';
    } else {
      // If no properties to update, just return the group.
      setClause = '';
    }

    let query = `
      MATCH (g:SchemaAttributeGroup { schemaGroupId: $groupId })
      ${setClause}
    `;

    // If attributes are provided in the update, handle the attribute relationships.
    if (groupData.attributes && Array.isArray(groupData.attributes)) {
      params.attributes = groupData.attributes;
      query += `
        WITH g
        // Remove all existing attribute relationships.
        OPTIONAL MATCH (g)-[r:INCLUDES_ATTRIBUTE]-()
        DELETE r

        // Create new relationships for each attribute specified.
        WITH g
        UNWIND $attributes AS attrName
        MATCH (attr:SchemaConfigAttribute { attributeName: attrName })
        MERGE (g)-[:INCLUDES_ATTRIBUTE]->(attr)
      `;
    }

    // Return the updated group node.
    query += `RETURN g`;

    return executeQueryAndReturnObservable(
      query,
      params,
      true,
      'Successfully updated attribute group',
      'Error updating attribute group'
    );
  }

  /**
   * Deletes a SchemaAttributeGroup.
   * Removes the group node and all its relationships.
   * @param groupId The schemaGroupId of the group to delete.
   * @returns An observable confirming the deletion.
   */
  deleteAttributeGroup(groupId: string): Observable<any> {
    // Build a query to match the group and delete it along with its relationships.
    const query = `
      MATCH (g:SchemaAttributeGroup { schemaGroupId: $groupId })
      OPTIONAL MATCH (g)-[r]-()
      DELETE r, g
    `;

    return executeQueryAndReturnObservable(
      query,
      { groupId },
      true,
      'Successfully deleted attribute group',
      'Error deleting attribute group'
    );
  }

  /**
   * Gets all attribute groups for a schema.
   * Retrieves groups along with the list of attribute names included in each group.
   * @param schemaFuelId The fuelId of the schema.
   * @returns An observable with all attribute groups and their attributes.
   */
  getSchemaAttributeGroups(schemaFuelId: string): Observable<SchemaAttributeGroup[]> {
    // Define the query to match schema and its related attribute groups.
    const query = `
      MATCH (schema { fuelId: $schemaFuelId })-[:HAS_GROUP]->(g:SchemaAttributeGroup)
      OPTIONAL MATCH (g)-[:INCLUDES_ATTRIBUTE]->(attr:SchemaConfigAttribute)
      RETURN g.schemaGroupId AS schemaGroupId,
             g.schemaGroupName AS schemaGroupName,
             g.schemaGroupFormat AS schemaGroupFormat,
             g.isDerived AS isDerived,
             g.validationRules AS validationRules,
             collect(attr.attributeName) AS attributes
    `;

    return executeCypherRead(query, { schemaFuelId }).pipe(
      map(result => {
        // If no records are found, return an empty array.
        if (!result || !result.records || result.records.length === 0) {
          return [];
        }

        // Map each record into a SchemaAttributeGroup object.
        return result.records.map(record => {
          const group: SchemaAttributeGroup = {
            schemaGroupId: record.get('schemaGroupId'),
            schemaGroupName: record.get('schemaGroupName'),
            schemaGroupFormat: record.get('schemaGroupFormat'),
            attributes: record.get('attributes').filter(a => a), // Filter out nulls
            isDerived: record.get('isDerived')
          };

          // Attempt to parse the validation rules JSON string if it exists.
          const validationRules = record.get('validationRules');
          if (validationRules) {
            try {
              group.validationRules = JSON.parse(validationRules);
            } catch (e) {
              this._logger.error(`[FUEL][ERROR] Error parsing validation rules for group ${group.schemaGroupId}`, e);
            }
          }

          return group;
        });
      })
    );
  }

  /**
   * Gets a specific attribute group by ID.
   * Retrieves the group details and its associated attribute names.
   * @param groupId The schemaGroupId to look up.
   * @returns An observable with the attribute group's details, or null if not found.
   */
  getAttributeGroupById(groupId: string): Observable<SchemaAttributeGroup | null> {
    // Build the query similar to getSchemaAttributeGroups, but filter by the groupId.
    const query = `
      MATCH (g:SchemaAttributeGroup { schemaGroupId: $groupId })
      OPTIONAL MATCH (g)-[:INCLUDES_ATTRIBUTE]->(attr:SchemaConfigAttribute)
      RETURN g.schemaGroupId AS schemaGroupId,
             g.schemaGroupName AS schemaGroupName,
             g.schemaGroupFormat AS schemaGroupFormat,
             g.isDerived AS isDerived,
             g.validationRules AS validationRules,
             collect(attr.attributeName) AS attributes
    `;

    return executeCypherRead(query, { groupId }).pipe(
      map(result => {
        // If no records are found, return null.
        if (!result || !result.records || result.records.length === 0) {
          return null;
        }

        // Process the first record found.
        const record = result.records[0];
        const group: SchemaAttributeGroup = {
          schemaGroupId: record.get('schemaGroupId'),
          schemaGroupName: record.get('schemaGroupName'),
          schemaGroupFormat: record.get('schemaGroupFormat'),
          attributes: record.get('attributes').filter(a => a), // Filter out nulls
          isDerived: record.get('isDerived')
        };

        // Parse the validation rules if provided.
        const validationRules = record.get('validationRules');
        if (validationRules) {
          try {
            group.validationRules = JSON.parse(validationRules);
          } catch (e) {
            this._logger.error(`[FUEL][ERROR] Error parsing validation rules for group ${group.schemaGroupId}`, e);
          }
        }

        return group;
      })
    );
  }

  /**
   * Normalizes a Neo4j response into a structured JSON object.
   * Extracts the properties of the schema node from the raw query result.
   * @param result The raw Neo4j query result.
   * @returns A formatted schema object or null if not found.
   */
  private normalizeSchemaResponse(result: any): any {
    if (this.verbose) {
      this._logger.info('[FUEL][DEBUG] Normalizing schema response', result);
    }

    // Check if the result is null/undefined
    if (!result) {
      this._logger.error('[FUEL][ERROR] normalizeSchemaResponse: Result is null or undefined');
      return null;
    }

    // Check if the fields array exists and has elements
    if (!result._fields || !Array.isArray(result._fields) || result._fields.length === 0) {
      this._logger.error('[FUEL][ERROR] normalizeSchemaResponse: No fields found in result');
      return null;
    }

    // Get the first field (schema node)
    const schemaNode = result._fields[0];

    // If no schema node was found
    if (!schemaNode) {
      this._logger.error('[FUEL][ERROR] normalizeSchemaResponse: Schema node is null or undefined');
      return null;
    }

    // If schema node doesn't have properties
    if (!schemaNode.properties) {
      this._logger.error('[FUEL][ERROR] normalizeSchemaResponse: Schema node has no properties', schemaNode);
      return null;
    }

    if (this.verbose) {
      this._logger.info('[FUEL][INFO] Normalized schema response:', schemaNode.properties);
    }

    return schemaNode.properties;
  }

  /**
   * Retrieves all schema attributes with their properties, including validation details.
   * Executes a query and collects attribute details into a structured format.
   * @param fuelId The fuelId of the schema.
   * @returns An observable with detailed attribute information.
   */
  getSchemaAttributesWithDetails(fuelId: string): Observable<any> {
    // Define the query to collect detailed attribute information.
    const query = `
      MATCH (schema:SchemaConfig { fuelId: $fuelId })-[r:HAS_ATTRIBUTE]->(attr:SchemaConfigAttribute)
      WITH schema, collect({
        attributeName: attr.attributeName,
        displayLabel: COALESCE(r.displayLabel, ""),
        attributeType: COALESCE(r.attributeType, "string"),
        order: COALESCE(r.order, 0),
        isPrimaryKey: COALESCE(r.isPrimaryKey, false),
        isRequired: COALESCE(r.isRequired, false),
        isSortable: COALESCE(r.isSortable, false),
        isUnique: COALESCE(r.isUnique, false),
        isSearchable: COALESCE(r.isSearchable, false),
        isFilterable: COALESCE(r.isFilterable, false),
        isExportable: COALESCE(r.isExportable, false),
        isImportable: COALESCE(r.isImportable, false),
        isHidden: COALESCE(r.isHidden, false),
        isReadOnly: COALESCE(r.isReadOnly, false),
        isNullable: COALESCE(r.isNullable, false),
        isAutoIncrement: COALESCE(r.isAutoIncrement, false),
        isSecondaryKey: COALESCE(r.isSecondaryKey, false),
        groupId: COALESCE(r.groupId, ""),
        groupOrder: COALESCE(r.groupOrder, 0),
        groupLabel: COALESCE(r.groupLabel, ""),
        groupIsCollapsed: COALESCE(r.groupIsCollapsed, false),
        groupIsHidden: COALESCE(r.groupIsHidden, false),
        groupIsDefault: COALESCE(r.groupIsDefault, false),
        groupDescription: COALESCE(r.groupDescription, ""),
        minimumRoleEdit: COALESCE(r.minimumRoleEdit, "NONE"),
        minimumRoleView: COALESCE(r.minimumRoleView, "NONE"),
        minimumRoleExport: COALESCE(r.minimumRoleExport, "NONE"),
        minimumRoleImport: COALESCE(r.minimumRoleImport, "NONE"),
        isCategoryRankLabel: COALESCE(r.isCategoryRankLabel, false),
        isCategoryRankSlug: COALESCE(r.isCategoryRankSlug, false),
        isCategoryRankValue: COALESCE(r.isCategoryRankValue, false),
        validationRules: COALESCE(r.validationRules, ""),
        validOptions: COALESCE(r.validOptions, ""),
        defaultValue: COALESCE(r.defaultValue, ""),
        delimiter: COALESCE(r.delimiter, "")
      }) AS rawAttributes
      RETURN [x IN rawAttributes WHERE x.attributeName IS NOT NULL] AS attributes
    `;

    return executeCypherRead(query, { fuelId }).pipe(
      map(result => {
        // Check if any attributes were returned.
        if (!result || !result._fields || result._fields.length === 0) {
          return [];
        }

        const attributes = result._fields[0] || [];

        // Process each attribute: parse validValues if stored as a JSON string.
        return attributes.map(attr => {
          if (attr.validValues && typeof attr.validValues === 'string') {
            try {
              attr.validValues = JSON.parse(attr.validValues);
            } catch (e) {
              // Log a warning if validValues cannot be parsed.
              this._logger.warn(`[FUEL][WARN] Could not parse validValues for ${attr.attributeName}: ${e}`);
            }
          }
          return attr;
        });
      })
    );
  }

  /**
   * Retrieves all schemas with their attributes in a structured format.
   * Queries for both ListTableSchema and ListUploadSchema nodes and aggregates attribute details.
   * @returns An observable containing an array of schemas with attributes.
   */
  getAllSchemasWithAttributes(): Observable<any> {
    // Define the query to fetch all schemas (both table and upload types) and their attributes.
    const query = `
      MATCH (s:SchemaConfig)
      WHERE s:ListTableSchema OR s:ListUploadSchema
      OPTIONAL MATCH (s)-[r:HAS_ATTRIBUTE]->(attr:SchemaConfigAttribute)
      RETURN s.fuelId AS schemaId,
        s.internalName AS schemaName,
        labels(s) AS schemaLabels,
        attr.attributeName AS attributeName,
        r.displayLabel AS displayLabel,
        r.order AS order,
        r.attributeType AS attributeType,
        r.isSearchable AS isSearchable,
        r.isFilterable AS isFilterable,
        r.isRequired AS isRequired,
        r.isHidden AS isHidden,
        r.isUnique AS isUnique,
        r.isReadOnly AS isReadOnly,
        r.isSortable AS isSortable,
        r.isExportable AS isExportable,
        r.isImportable AS isImportable,
        r.isNullable AS isNullable,
        r.isAutoIncrement AS isAutoIncrement,
        r.isPrimaryKey AS isPrimaryKey,
        r.isSecondaryKey AS isSecondaryKey,
        r.groupId AS groupId,
        r.groupOrder AS groupOrder,
        r.groupLabel AS groupLabel,
        r.groupIsCollapsed AS groupIsCollapsed,
        r.groupIsHidden AS groupIsHidden,
        r.groupIsDefault AS groupIsDefault,
        r.groupDescription AS groupDescription,
        r.minimumRoleEdit AS minimumRoleEdit,
        r.minimumRoleView AS minimumRoleView,
        r.minimumRoleExport AS minimumRoleExport,
        r.minimumRoleImport AS minimumRoleImport,
        r.isCategoryRankLabel AS isCategoryRankLabel,
        r.isCategoryRankSlug AS isCategoryRankSlug,
        r.isCategoryRankValue AS isCategoryRankValue,
        r.fuelId AS attributeRelFuelId
      ORDER BY s.fuelId, r.order
    `;

    return executeCypherRead(query, {}).pipe(
      map(result => {
        // Create a dictionary to map schemas by their fuelId.
        const schemas = {};

        if (!result || !result.records || result.records.length === 0) {
          return [];
        }

        result.records.forEach(record => {
          const schemaId = record.get('schemaId');
          if (!schemaId) return; // Skip if no schema ID

          // Initialize a new schema entry if not already present.
          if (!schemas[schemaId]) {
            const schemaLabels = record.get('schemaLabels') || [];
            const schemaType = schemaLabels.find(label =>
              label === 'ListTableSchema' || label === 'ListUploadSchema'
            ) || 'UnknownSchema';

            schemas[schemaId] = {
              schemaId,
              schemaType,
              internalName: record.get('schemaName'),
              attributes: []
            };
          }

          // Append attribute details if present.
          if (record.get('attributeName')) {
            schemas[schemaId].attributes.push({
              attributeType: record.get('attributeType') || 'string',
              attributeName: record.get('attributeName'),
              displayLabel: record.get('displayLabel') || '',
              order: record.get('order') || 0,
              isSearchable: record.get('isSearchable') || false,
              isFilterable: record.get('isFilterable') || false,
              isRequired: record.get('isRequired') || false,
              isHidden: record.get('isHidden') || false,
              isUnique: record.get('isUnique') || false,
              isReadOnly: record.get('isReadOnly') || false,
              isSortable: record.get('isSortable') !== false, // default to true
              isExportable: record.get('isExportable') !== false, // default to true
              isImportable: record.get('isImportable') !== false, // default to true
              isNullable: record.get('isNullable') || false,
              isAutoIncrement: record.get('isAutoIncrement') || false,
              isPrimaryKey: record.get('isPrimaryKey') || false,
              isSecondaryKey: record.get('isSecondaryKey') || false,
              groupId: record.get('groupId') || '',
              groupOrder: record.get('groupOrder') || 0,
              groupLabel: record.get('groupLabel') || '',
              groupIsCollapsed: record.get('groupIsCollapsed') || false,
              groupIsHidden: record.get('groupIsHidden') || false,
              groupIsDefault: record.get('groupIsDefault') || false,
              groupDescription: record.get('groupDescription') || '',
              minimumRoleEdit: record.get('minimumRoleEdit') || 'NONE',
              minimumRoleView: record.get('minimumRoleView') || 'NONE',
              minimumRoleExport: record.get('minimumRoleExport') || 'NONE',
              minimumRoleImport: record.get('minimumRoleImport') || 'NONE',
              isCategoryRankLabel: record.get('isCategoryRankLabel') || false,
              isCategoryRankSlug: record.get('isCategoryRankSlug') || false,
              isCategoryRankValue: record.get('isCategoryRankValue') || false,
              fuelId: record.get('attributeRelFuelId')
            });
          }
        });

        // Convert the dictionary to an array for the observable return.
        return Object.values(schemas);
      })
    );
  }

  /**
   * Retrieves a ListUploadSchema from Neo4j using fuelId.
   * Executes a query to find the schema node based on its unique fuelId.
   * @param schemaId The unique fuelId of the schema.
   * @returns An observable containing the normalized schema data.
   */
  getListUploadSchemaByFuelId(schemaId: string): Observable<any> {
    // Define the query to fetch the schema by its fuelId.
    const query = `
      MATCH (s:SchemaConfig:ListUploadSchema { fuelId: $schemaId })
      RETURN s
    `;

    // Log the operation if verbose mode is enabled.
    if (this.verbose) {
      this._logger.info(`Fetched getListUploadSchemaByFuelId (schemaId): ${schemaId}`, query);
    }

    // Execute the query and normalize the result.
    return executeCypherRead(query, { schemaId }).pipe(
      map(result => this.normalizeSchemaResponse(result))
    );
  }

  /**
   * Retrieves a specific schema of a given type linked to a ListIssue.
   * Executes a Cypher query to find the schema node based on the issue's fuelId and schema type.
   * @param issueId The fuelId of the ListIssue.
   * @param schemaType The type of schema to retrieve (e.g., 'ListTableSchema', 'ListUploadSchema').
   * @returns An observable containing the normalized schema data.
   */
  getSchemaByIssueId(issueId: string, schemaType: string): Observable<any> {
    // Define the Cypher query to match the ListIssue and its associated schema of the specified type.
    const query = `
      MATCH (issue:ListIssue { fuelId: $issueId })-[:USES]->(schema:SchemaConfig:${schemaType})
      RETURN schema
    `;

    // Execute the query and normalize the result before returning.
    return executeCypherRead(query, { issueId }).pipe(
      // Map the raw query result to a normalized schema object.
      map(result => this.normalizeSchemaResponse(result))
    );
  }

  /**
   * Retrieves all schemas linked to a ListIssue.
   * Queries for all schema nodes associated with a specific ListIssue.
   * @param issueId The fuelId of the ListIssue.
   * @returns An observable containing an array of normalized schema data.
   */
  getAllSchemaConfig(issueId: string): Observable<any[]> {
    // Define the query to match all schemas linked via the USES relationship.
    const query = `
      MATCH (issue:ListIssue { fuelId: $issueId })-[:USES]->(schema:SchemaConfig)
      RETURN collect(schema) as schemas
    `;

    return executeCypherRead(query, { issueId }).pipe(
      // Log the raw response for debugging purposes.
      tap(result => {
        if (this.verbose) {
          this._logger.info(`[FUEL][DEBUG] getAllSchemaConfig raw response for issueId: ${issueId}`,
            JSON.stringify(result, null, 2));
        }
      }),
      map(result => {
        // Validate the result format before processing.
        if (!result || !result._fields || result._fields.length === 0 || !result._fields[0]) {
          this._logger.warn(`[FUEL][WARN] No schemas found for ListIssue with issueId: ${issueId}`);
          return [];
        }

        // Extract the array of schema nodes.
        const schemaNodes = result._fields[0];

        // Check if the returned result is an array.
        if (!Array.isArray(schemaNodes)) {
          this._logger.warn(`[FUEL][WARN] Unexpected result format for getAllSchemaConfig`);
          return [];
        }

        // Map each node to its properties and include the schema type based on its label.
        const schemas = schemaNodes.map(node => {
          if (!node || !node.properties) return null;
          return {
            ...node.properties,
            schemaType: node.labels.find(label =>
              label === 'ListTableSchema' || label === 'ListUploadSchema'
            ) || node.labels[0]
          };
        }).filter(schema => schema !== null);

        this._logger.info(`[FUEL][INFO] Found ${schemas.length} schemas for ListIssue: ${issueId}`, schemas);

        return schemas;
      })
    );
  }

}
