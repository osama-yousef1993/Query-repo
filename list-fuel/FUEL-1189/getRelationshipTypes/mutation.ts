/**
 * @file src/graphql/queries/getSchemaRequirements/query.ts
 * @description Query resolver for retrieving schema requirements
 */

import { ILogger, getLoggerContainer, LOGGER } from '@forbes/lists-fuel-logger';
import {
  EntityManager
} from '@forbes/lists-fuel-graph-manager';
import { firstValueFrom } from 'rxjs';
import { RelationshipTypesResponse } from './types';

// Retrieve the logger instance
const logger = getLoggerContainer().get<ILogger>(LOGGER.Logger);

// Create instances of schema-related classes
const entityManager = new EntityManager(logger);

/**
 * Helper function to get schema ID for a list issue
 */
async function getRelationshipTypes(): Promise<string> {
  // Get the schema by list issue natural ID
  const relationTypesResponse: any[] = await firstValueFrom(
    entityManager.getAllRelationshipTypes()
  );

  if (!relationTypesResponse || relationTypesResponse.length === 0) {
    logger.warn(`[SCHEMA] No relationship Types found`);
    throw new Error(`No relationship Types found`);
  }
    try {
        const relationshipTypes = await entityManager.getAllRelationshipTypes().pipe(toArray()).toPromise();
        if (!relationshipTypes) {
            throw new GraphQLError('Failed to fetch GetRelationshipTypes: nodeTypes is undefined');
        }
        const result = nodeTypes.map((nodeType) => ({
            field: nodeType._fields?.[0],
            type: nodeType._fields?.[1],
        }));

        return {
            result,
        };
    } catch (error) {
        logger.error(
          `Failed to fetch GetNodeTypesFields: ${
            error instanceof Error ? error.message : error
          }`,
        );
        throw new GraphQLError(
          `Failed to fetch GetNodeTypesFields: ${
            error instanceof Error ? error.message : error
          }`,
        );
      }

};

export default getSchemaRequirements;
