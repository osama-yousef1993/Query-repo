/**
 * @file src/graphql/mutation/getRelationshipTypes/types.ts
 * @description Type definitions for getRelationshipTypes query
 */

/**
 * Response for getRelationshipTypes query
 */
export interface RelationshipTypesResponse {
  /**
   * Whether the operation was successful
   */
  success: boolean;

  /**
   * List of relationship Types
   */
  requirements: string[];
}
