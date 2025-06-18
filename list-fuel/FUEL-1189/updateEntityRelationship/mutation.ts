/**
 * @file src/graphql/mutations/updateEntityRelationship/mutation.ts
 *
 * @description This resolver is used to update attributes on entities, such as the legal name and print name, using the provided input.
 *
 */

import { EntityManager } from '@forbes/lists-fuel-graph-manager';
import { GraphQLError } from 'graphql';
import { ILogger, getLoggerContainer, LOGGER } from '@forbes/lists-fuel-logger';
import {AttributeUpdateResult, UpdateEntityRelationshipResponse,  UpdateEntityRelationshipInput} from './types';


const logger = getLoggerContainer().get<ILogger>(LOGGER.Logger);
const entityManager = new EntityManager(logger);


const updateEntityRelationshipResolver = async (
  _parent: unknown,
  args: UpdateEntityRelationshipInput
): Promise<UpdateEntityRelationshipResponse> => {
  // todo add option properties that will be added to relationship
  try {
    if (!args.sourceFuelId || !args.targetFuelId) {
      throw new Error('sourceFuelId and targetFuelId are required');
    }
    const res = await entityManager.updateEntities(args);
    const result: AttributeUpdateResult[] = [];
  } catch (error: unknown) {
    if (
      typeof error === 'object' &&
      error !== null &&
      'message' in error &&
      typeof (error as { message: unknown }).message === 'string'
    ) {
      throw new GraphQLError(
        `Failed to update Entity: ${(error as { message: string }).message}`
      );
    } else {
      throw new GraphQLError('Failed to update Entity');
    }
  }
};

export default updateEntityRelationshipResolver;
