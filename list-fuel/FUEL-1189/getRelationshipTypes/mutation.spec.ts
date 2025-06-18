/**
 * @file src/graphql/queries/getSchemaRequirements/query.spec.ts
 * @description Test suite for getSchemaRequirements query
 */

// Import necessary modules and dependencies
import express, { Express } from 'express';
import request from 'supertest'; // for testing Express routes
import { Driver } from 'neo4j-driver'; // Neo4j driver class
import http from 'http'; // Node.js core module for creating HTTP server
// Import the TestServerUtils class
import TestServerUtils from '../../testServerUtils';

// Declare variables for Express app, Neo4j driver, Apollo server, and HTTP server
const app: Express = express();
let driver: Driver;
let apolloServer: any;
let httpServer: http.Server;
const testUtilsServer = new TestServerUtils();


// mock SchemaConfigManager in GraphManager
jest.mock('@forbes/lists-fuel-graph-manager', () => {
  return {
    SchemaConfigManager: jest.fn().mockImplementation(() => ({
      getAllSchemaConfig: jest.fn(() => Promise.resolve([
        {
          fuelId: 'schemaId',
          schemaType: 'ListUploadSchema'
        }
      ]))
    }))
  };
});

// Define the test suite for getSchemaRequirements
describe('getSchemaRequirements Test Suite', () => {
  // Set up before running the test suite
  beforeAll(async () => {
    [apolloServer, httpServer, driver] =
      await testUtilsServer.beforeProcess(app);
  }, 20000);

  // Test getSchemaRequirements query
  test('getSchemaRequirements GraphQL Query', async () => {
    // Define the GraphQL query
    const GET_SCHEMA_REQUIREMENTS_QUERY = `
      query GetSchemaRequirements(
        $listIssueNaturalId: String!,
        $listIssueYear: Int!
      ) {
        getSchemaRequirements(
          listIssueNaturalId: $listIssueNaturalId,
          listIssueYear: $listIssueYear
        ) {
          success
          requirements
        }
      }
    `;

    const queryVariables = {
      listIssueNaturalId: 'Company',
      listIssueYear: 2023
    };

    // Make a POST request to the GraphQL endpoint
    const res = await request(httpServer)
      .post('/graphql')
      .send({
        query: GET_SCHEMA_REQUIREMENTS_QUERY,
        variables: queryVariables,
      })
      .expect('Content-Type', /json/)
      .expect(200);

    // Check that the response contains the expected structure
    expect(res.body.data.getSchemaRequirements).toHaveProperty('success');
    expect(res.body.data.getSchemaRequirements).toHaveProperty('requirements');

    // Log the result for debugging
    console.log('Schema requirements result:', {
      success: res.body.data.getSchemaRequirements.success,
      requirementsLength: res.body.data.getSchemaRequirements.requirements.length,
    });
  }, 60000);

  // Test getSchemaRequirements query with nonexistent list issue
  test('getSchemaRequirements GraphQL Query - Nonexistent List Issue', async () => {
    // Define the GraphQL query
    const GET_SCHEMA_REQUIREMENTS_QUERY = `
      query GetSchemaRequirements(
        $listIssueNaturalId: String!,
        $listIssueYear: Int!
      ) {
        getSchemaRequirements(
          listIssueNaturalId: $listIssueNaturalId,
          listIssueYear: $listIssueYear
        ) {
          success
          requirements
        }
      }
    `;

    const queryVariables = {
      listIssueNaturalId: 'NonexistentList',
      listIssueYear: 2099
    };

    // Make a POST request to the GraphQL endpoint
    const res = await request(httpServer)
      .post('/graphql')
      .send({
        query: GET_SCHEMA_REQUIREMENTS_QUERY,
        variables: queryVariables,
      })
      .expect('Content-Type', /json/)
      .expect(200);

    // Check that the response contains the expected structure
    expect(res.body.data.getSchemaRequirements).toHaveProperty('success');
    expect(res.body.data.getSchemaRequirements).toHaveProperty('requirements');

    // Should be unsuccessful for a nonexistent list
    expect(res.body.data.getSchemaRequirements.success).toBe(false);
    expect(res.body.data.getSchemaRequirements.requirements).toContain('Error');

    // Log the result for debugging
    console.log('Schema requirements error result:', {
      success: res.body.data.getSchemaRequirements.success,
      requirements: res.body.data.getSchemaRequirements.requirements,
    });
  }, 60000);

  // Tear down after running the test suite
  afterAll(async () => {
    await testUtilsServer.afterProcess();
  }, 5000);
});
