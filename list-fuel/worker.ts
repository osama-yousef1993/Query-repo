/**
 * @file worker.ts
 * @description The WorkerProcessor class handles data processing tasks.
 * It is spawned by the supervisor container to process data from a CSV file slice.
 */
import { readFileSync } from 'fs';
import * as Papa from 'papaparse';
import { getLoggerContainer, ILogger, LOGGER } from '@forbes/lists-fuel-logger';
import { fetchEnvVar } from '@forbes/lists-fuel-config';
import {
  ValidationData,
  TempNode,
  SchemaManager,
  TempManager,
  EntityManager,
  ListData,
} from '@forbes/lists-fuel-graph-manager';
import {
  Observable,
  firstValueFrom,
  mergeMap,
  from,
  throwError,
  finalize,
  EMPTY,
} from 'rxjs';
import { take, tap } from 'rxjs/operators';
import { PubSubManager } from '@forbes/lists-fuel-pubsub-manager';
import { Config } from '@forbes/lists-fuel-config';
import { lowerCaseColumns } from '@forbes/lists-fuel-data-utils';
// The public IP of the worker host.
const ownHost: string = PubSubManager.getPublicIP();

type workerTimingsObject = {
  load: number;
  end: number;
  init: number;
  beginProcessUpload: number;
  endProcessUpload: number;
  beginProcessRow: number;
  endProcessRow: number;
  processRowEnd: number[];
  processRowStart: number[];
};

const internalTimings: workerTimingsObject = {
  load: Date.now(),
  end: 0,
  init: 0,
  beginProcessUpload: 0,
  endProcessUpload: 0,
  beginProcessRow: 0,
  endProcessRow: 0,
  processRowStart: [],
  processRowEnd: [],
};

/**
 * WorkerProcessor class.
 */
export class WorkerProcessor {
  private logger: ILogger;
  private schemaManager: SchemaManager;
  private tempManager: TempManager;
  private entityManager: EntityManager;
  private topic: any;
  private debug: boolean;
  private verbose: boolean;

  /**
   * Initializes a new instance of the WorkerProcessor class.
   * @param schemaManager - The SchemaManager instance.
   * @param tempManager - The TempManager instance.
   * @param topic - The PubSub topic.
   * @param logger - The ILogger instance.
   * @returns A new instance of the WorkerProcessor class.
   */
  constructor(
    schemaManager: SchemaManager,
    tempManager: TempManager,
    entityManager: EntityManager,
    topic: any,
    logger?: ILogger,
    debug?: boolean,
    verbose?: boolean,
  ) {
    this.logger = logger || getLoggerContainer().get<ILogger>(LOGGER.Logger);
    this.schemaManager = schemaManager;
    this.tempManager = tempManager;
    this.entityManager = entityManager;
    this.topic = topic;
    internalTimings.init = Date.now();
    if (debug) {
      this.debug = true;
    } else {
      // Get the debug flag from the environment
      const debugVar: string = Config.getOrDefault('FUEL_DEBUG', 'false');
      this.debug = 'false' !== debugVar.toLowerCase() && '0' !== debugVar;
    }
    if (verbose) {
      this.verbose = true;
    } else {
      // Get the verbose flag from the environment
      this.verbose =
        'false' !== Config.getOrDefault('FUEL_VERBOSE_PROCESS', 'false')
          ? true
          : false;
    }
  }

  /**
   * Parses CSV data using PapaParse library.
   * @param data - The CSV data to parse.
   * @returns The parsed CSV data.
   */
  parseCSV(data: string): Papa.ParseResult<object> {
    return Papa.parse(data, {
      header: true,
      skipEmptyLines: true,
    });
  }

  /**
   * Processes a single data row and returns an Observable.
   * @param row - The data row to process.
   * @param schemaData - The schema data.
   * @param config - The configuration data.
   * @param validationData - The validation data.
   * @param listIssueData - The list issue data.
   * @returns An Observable containing the processing result.
   */
  processRow(
    row: any,
    cleanedRow: any,
    schemaData: any,
    config: any,
    validationData: ValidationData,
    listIssueData: ListData,
    columnNameWarning?: any,
    columnNameWarningMessages?: any,
  ): Observable<any> {
    const tempNodeProperties: TempNode = {
      nodeType: config.FUEL_LIST_ISSUE_TYPE,
      userDataJSON: JSON.stringify(row),
      mitigationsJSON: JSON.stringify(cleanedRow),
      schemaModelJSON: JSON.stringify(schemaData),
      schemaOkAttributes: validationData.schemaOkAttributes,
      invalidEntityID: validationData.invalidEntityID,
      duplicateID: validationData.duplicateID,
      missingAttributes: validationData.missingAttributes,
      missingAttributeMessages: validationData.missingAttributeMessages,
      extraAttributes: validationData.extraAttributes,
      extraAttributeMessages: validationData.extraAttributeMessages,
      badAttributes: validationData.badAttributes,
      badAttributeMessages: validationData.badAttributeMessages,
      columnNameWarning,
      columnNameWarningMessages,
      discrepencies: validationData.discrepencies,
      idDetectedButMissingError: validationData.idDetectedButMissingError,
      idDuplicateError: validationData.idDuplicateError,
      idNotDetectedError: validationData.idNotDetectedError,
      organizationNotMatchWarning: validationData.organizationNotMatchWarning,
      nameMismatchWarning: validationData.nameMismatchWarning,
      inputId: config.FUEL_INPUT_ID,
      sliceId: config.FUEL_SLICE_ID,
      modifiedTime: 0,
      useForListOnly: false,
      row: cleanedRow.rowNumber,
      totalRow: cleanedRow.totalRow,
      targetListIssueType: listIssueData.listIssueType,
      targetListIssueNaturalId: listIssueData.listIssueNaturalId,
      targetListIssueYear: listIssueData.listIssueYear,
      targetListIssueURI: listIssueData.listIssueURI,
      targetListIssueName: listIssueData.listIssueName,
      invalidLocation: validationData.invalidLocation,
      templateFields: Object.keys(row).join(', '),
    };
    internalTimings.processRowStart.push(Date.now());
    return new Observable<any>((subscriber) => {
      if (this.debug) {
        this.logger.info(
          'addTemporaryNode: ' + JSON.stringify(tempNodeProperties),
        );
      }
      this.tempManager
        .addTemporaryNode(tempNodeProperties)
        .pipe(
          finalize(() => {
            subscriber.complete();
            subscriber.unsubscribe();
          }),
        )
        .subscribe({
          next: (tempNode: any) => {
            internalTimings.processRowEnd.push(Date.now());
            subscriber.next(tempNode);
          },
          error: (err) => {
            subscriber.error(err);
          },
        });
    });
  }

  /**
   * Processes an array of data rows and returns an Observable.
   * @param parsedData - The parsed data rows.
   * @param schemaData - The schema data.
   * @param config - The configuration data.
   * @param listIssueData - The list issue data.
   * @returns An Observable containing the processing result.
   */
  processRows(
    parsedData: any[],
    schemaData: any,
    config: any,
    listIssueData: ListData,
    idColumn: any,
    idDuplicateInformation: any,
    displayNameColumn: string,
    needToMatchAndGenerateDisplayName: any,
    currentRow: any,
    typeKey: any,
  ): Observable<any> {
    let rowNumber: number = 1;
    const totalRows = parsedData.length; // Total number of rows
    let processedRows = 0; // Counter for processed rows

    if (this.debug) {
      this.logger.info('Worker schema data: ', schemaData);
    }

    const columnNameWarningMap = this.schemaManager.getColumnNameWarningMap(
      parsedData[0],
      schemaData,
    );
    const columnNameWarningMessages: string[] =
      this.schemaManager.getColumnNameWarningMessages(columnNameWarningMap);

    const lowerCasedColumn = Object.keys(parsedData[0]);

    return from(parsedData).pipe(
      mergeMap((row) => {
        // Using from() to create an Observable from an array of data rows.
        // let cleanedRow = performDataCleanup([row]); FUEL-441 asks that we only perform cleanup IF the user asks us to do so.
        const cleanedRow = JSON.parse(JSON.stringify(row)); // Deep copy the dictionary

        // this logic is incorrect when naturalId does not contain 'fred/company/'
        const idPayload = this.schemaManager.getNeo4jNaturalId(
          cleanedRow,
          typeKey,
          idColumn,
        );
        if (idPayload.naturalId) {
          this.entityManager
            .getNode('Organization:Company', { naturalId: idPayload.naturalId })
            .pipe(take(1))
            .subscribe((data) => {
              const uri = data._fields?.[0]?.properties?.uri;
              if (uri) {
                cleanedRow.uri = uri;
              }
            });
        }

        let originalName: string = '';
        if (lowerCasedColumn.includes('name')) {
          originalName =
            Object.keys(cleanedRow)[lowerCasedColumn.indexOf('name')];
          
          if (!cleanedRow[displayNameColumn]) {
            cleanedRow[displayNameColumn] = cleanedRow[originalName];
          }
        }

        if (!idPayload.naturalId && !cleanedRow[originalName]) {
          // Definition of empty row: There is no Id and name in that row
          rowNumber++;
          processedRows++;
          return EMPTY;
        } else {
          cleanedRow.rowNumber = rowNumber;
          cleanedRow.totalRow = currentRow + rowNumber;

          if (this.debug) {
            this.logger.info('Processing row', cleanedRow.rowNumber);
          }
          rowNumber++;
          return new Observable<any>((subscriber) => {
            // Creating a new Observable for processing each row asynchronously.
            let idDuplicateInformationArray = [];
            if (idDuplicateInformation) {
              idDuplicateInformationArray = idDuplicateInformation.split(';');
            }

            this.schemaManager
              .validateAttributesAgainstSchema(
                cleanedRow,
                schemaData,
                idPayload.idColumnArray,
                idDuplicateInformationArray,
                columnNameWarningMessages,
                columnNameWarningMap,
              )
              .pipe(
                tap(() => {
                  if (this.debug) {
                    this.logger.info(
                      'Validated attributes against schema of rowNumber:',
                      cleanedRow.rowNumber,
                    );
                  }
                }),
              )
              .subscribe({
                next: async (validationData: ValidationData) => {
                  // Subscription to validate attributes against the schema.
                  const nameResult =
                    await this.schemaManager.validateOrganizationName(
                      cleanedRow,
                      typeKey,
                      idColumn,
                      displayNameColumn,
                      needToMatchAndGenerateDisplayName,
                    );

                  if (!nameResult.organizationName) {
                    if (this.verbose) {
                      await this.topic.publishMessage(
                        PubSubManager.createMessageSchema(
                          'ORGANIZATION_NAME_MISSING_IN_DATABASE',
                          {
                            eventMessage: `ORGANIZATION NAME IS MISSING FOR ID ${idPayload.numericId}`,
                            listIssue: config.FUEL_LIST_ISSUE_NAME,
                            fileName: config.FUEL_SLICE_FILE,
                            originalFileName: config.FUEL_ORIGINAL_FILE,
                            eventName: 'ORGANIZATION_NAME_MISSING_IN_DATABASE',
                            serverHostname: await PubSubManager.getPublicIP(),
                            internalTimings: JSON.stringify(internalTimings),
                          },
                        ),
                      );
                    }
                  }

                  // Handle displayName
                  if (needToMatchAndGenerateDisplayName) {
                    const displayNameCandidate =
                      await this.schemaManager.getOrganizationDisplayName(
                        cleanedRow,
                        typeKey,
                        idColumn,
                      );
                    cleanedRow.displayName = displayNameCandidate;
                  }

                  validationData.invalidEntityID =
                    nameResult.invalidEntityIDOrganizationNotMatch;
                  validationData.organizationNotMatchWarning =
                    nameResult.organizationNotMatchWarning;
                  validationData.nameMismatchWarning =
                    nameResult.nameMismatchWarning;

                  const result = await firstValueFrom(
                    this.processRow(
                      row,
                      cleanedRow,
                      schemaData,
                      config,
                      validationData,
                      listIssueData,
                      Object.keys(columnNameWarningMap),
                      columnNameWarningMessages,
                    ),
                  );

                  // Unfolds the mitigationJSON
                  let parsedMitigationsJSON: {} = {};

                  try {
                    parsedMitigationsJSON = JSON.parse(
                      result._fields[0].properties.mitigationsJSON,
                    );
                  } catch (error) {
                    this.logger?.warn('Error parsing JSON:', error);
                    await this.topic.publishMessage(
                      PubSubManager.createMessageSchema('FILE_UPLOAD_ERROR', {
                        eventMessage: `Error parsing JSON during worker process: ${JSON.stringify(
                          error,
                        )}`,
                        listIssue: config.FUEL_LIST_ISSUE_NAME,
                        fileName: config.FUEL_SLICE_FILE,
                        originalFileName: config.FUEL_ORIGINAL_FILE,
                        eventName: 'FILE_UPLOAD_ERROR',
                        serverHostname: ownHost,
                        internalTimings: JSON.stringify(internalTimings),
                      }),
                    );
                  }

                  await this.tempManager.updateTempNode(
                    result._fields[0].properties.fuelId,
                    parsedMitigationsJSON,
                  );

                  processedRows++; // Increment processed rows count

                  // Check if all rows are processed
                  if (processedRows === totalRows) {
                    subscriber.next('All rows processed');
                    subscriber.complete();
                  }
                },
                error: (err) => {
                  subscriber.error(err);
                },
              });
          });
        }
      }),
    );
  }

  /**
   * Imports a CSV file and processes its data.
   * @param schemaData - The schema data.
   * @param config - The configuration data.
   * @param listIssueData - The list issue data.
   * @returns An Observable containing the processing result.
   */
  importFile(
    schemaData: any,
    config: any,
    listIssueData: ListData,
    idColumn: any,
    idDuplicateInformation: any,
    displayNameColumn: string,
    needToMatchAndGenerateDisplayName: any,
    currentRow: any,
    typeKey: any,
  ): Observable<any> {
    const filePath: string = config.FUEL_SLICE_FILE;
    if ('' === filePath) {
      return throwError(
        () => 'FUEL_SLICE_FILE environment variable should not be blank.',
      );
    }
    if (!filePath) {
      return throwError(
        () => 'FUEL_SLICE_FILE environment variable should be set.',
      );
    }
    try {
      if (this.debug) {
        this.logger.info('ImportFile: ' + filePath);
      }
      const csvData = readFileSync(filePath, 'utf-8');
      const parsedData = this.parseCSV(csvData);

      // Check for errors in the CSV file.

      if (parsedData.errors.length > 0) {
        return throwError(
          () => `Parse errors: ${JSON.stringify(parsedData.errors)}`,
        );
      }

      if (parsedData.data.length === 0) {
        return throwError(() => 'CSV file must include a header and 1+ row.');
      }

      if (this.debug) {
        this.logger.info('insertData meta: ' + JSON.stringify(parsedData.meta));
        this.logger.info('insertData data: ' + JSON.stringify(parsedData.data));
      }

      internalTimings.beginProcessRow = Date.now();
      const rows = this.processRows(
        parsedData.data,
        schemaData,
        config,
        listIssueData,
        idColumn,
        idDuplicateInformation,
        displayNameColumn,
        needToMatchAndGenerateDisplayName,
        currentRow,
        typeKey,
      );
      internalTimings.endProcessRow = Date.now();
      return rows;
    } catch (error: any) {
      return throwError(() => error);
    }
  }

  /**
   * Processes an upload task.
   */
  async processUpload(): Promise<void> {
    let now = Date.now();
    internalTimings.beginProcessUpload = now;

    const config = await fetchEnvVar([
      'FUEL_SLICE_FILE',
      'FUEL_INPUT_ID',
      'FUEL_SLICE_ID',
      'FUEL_LIST_ISSUE_URI',
      'FUEL_LIST_ISSUE_TYPE',
      'FUEL_LIST_ISSUE_NAME',
      'FUEL_LIST_ISSUE_YEAR',
      'FUEL_LIST_ISSUE_NATURAL_ID',
      'FUEL_ORIGINAL_FILE',
      'PROTECTS_DATABASE_FOR_TESTING',
    ]);

    try {
      if (this.verbose) {
        await this.topic.publishMessage(
          PubSubManager.createMessageSchema('FILE_UPLOAD', {
            eventMessage: `Worker Started ${JSON.stringify(
              config.FUEL_SLICE_ID,
            )} fileName: ${config.FUEL_SLICE_FILE} and listIssue: ${
              config.FUEL_LIST_ISSUE_NAME
            }`,
            listIssue: config.FUEL_LIST_ISSUE_NAME,
            fileName: config.FUEL_SLICE_FILE,
            originalFileName: config.FUEL_ORIGINAL_FILE,
            eventName: 'FILE_UPLOAD_STARTING',
            serverHostname: ownHost,
            internalTimings: JSON.stringify(internalTimings),
          }),
        );
      }
      // By default, the Env variables passes string
      // We need to manually transform the type
      // So that the type are the same for the following passing
      config.FUEL_LIST_ISSUE_YEAR = parseInt(config.FUEL_LIST_ISSUE_YEAR, 10);

      const typeKey: string = config.FUEL_LIST_ISSUE_TYPE;
      const rawSchema = await this.fetchSchema(typeKey);
      const schemaModels = {};
      schemaModels[typeKey] = rawSchema;
      if (this.debug) {
        this.logger.info('Schema fetched: ' + JSON.stringify(schemaModels));
      }

      const listIssueData: ListData = {
        listIssueNaturalId: config.FUEL_LIST_ISSUE_NATURAL_ID,
        listIssueType: config.FUEL_LIST_ISSUE_TYPE,
        listIssueName: config.FUEL_LIST_ISSUE_NAME,
        listIssueURI: config.FUEL_LIST_ISSUE_URI,
        listIssueYear: config.FUEL_LIST_ISSUE_YEAR,
        originalFileName: config.FUEL_ORIGINAL_FILE,
      };

      const schema = schemaModels[typeKey];

      const fileUploadNodeData: any = await firstValueFrom(
        this.schemaManager.getFileUploadNode({ fuelId: config.FUEL_SLICE_ID })
      );

      const firstFieldProperties = fileUploadNodeData?._fields?.[0]?.properties || {};

      const idColumn = firstFieldProperties.idColumn || null;
      const idDuplicateInformation = firstFieldProperties.idDuplicateInformation || null;
      const displayNameColumn = firstFieldProperties.displayNameColumn || 'displayName';
      // @to-do remove revert this change before merge
      const needToMatchAndGenerateDisplayName = true;
      const currentRow = firstFieldProperties.currentRow || null;

      if (this.debug) {
        this.logger.info('currentRow', currentRow);
      }

      const data = await firstValueFrom(
        this.importFile(
          schema,
          config,
          listIssueData,
          idColumn,
          idDuplicateInformation,
          displayNameColumn,
          needToMatchAndGenerateDisplayName,
          currentRow,
          typeKey,
        ),
      );

      if (this.debug) {
        this.logger.info('importFile: ' + JSON.stringify(data));
      }

      internalTimings.endProcessUpload = Date.now();

      if (this.verbose) {
        await this.topic.publishMessage(
          PubSubManager.createMessageSchema('FILE_UPLOAD', {
            eventMessage: `Worker FINISHING ${JSON.stringify(
              config.FUEL_SLICE_ID,
            )} fileName: ${config.FUEL_SLICE_FILE} and listIssue: ${
              config.FUEL_LIST_ISSUE_NAME
            }`,
            listIssue: config.FUEL_LIST_ISSUE_NAME,
            fileName: config.FUEL_SLICE_FILE,
            originalFileName: config.FUEL_ORIGINAL_FILE,
            eventName: 'FILE_UPLOAD_FINISHING',
            serverHostname: ownHost,
            internalTimings: JSON.stringify(internalTimings),
          }),
        );
      }
    } catch (error: any) {
      internalTimings.end = Date.now();
      this.logger.error('[Worker] Error in main: ' + error.toString());

      await this.topic.publishMessage(
        PubSubManager.createMessageSchema('FILE_UPLOAD_ERROR', {
          eventMessage: `WORKER FAILING ${JSON.stringify(error)} fileName: ${
            config.FUEL_SLICE_FILE
          } and listIssue: ${config.FUEL_LIST_ISSUE_NAME}`,
          listIssue: config.FUEL_LIST_ISSUE_NAME,
          fileName: config.FUEL_SLICE_FILE,
          originalFileName: config.FUEL_ORIGINAL_FILE,
          eventName: 'FILE_UPLOAD_ERROR',
          serverHostname: ownHost,
          internalTimings: JSON.stringify(internalTimings),
        }),
      );
      if (this.verbose) {
        await this.topic.publishMessage(
          PubSubManager.createMessageSchema('WORKER_ERROR', {
            eventMessage: `WORKER FAILING ${JSON.stringify(error)} fileName: ${
              config.FUEL_SLICE_FILE
            } and listIssue: ${config.FUEL_LIST_ISSUE_NAME}`,
            listIssue: config.FUEL_LIST_ISSUE_NAME,
            fileName: config.FUEL_SLICE_FILE,
            originalFileName: config.FUEL_ORIGINAL_FILE,
            eventName: 'WORKER_ERROR',
            serverHostname: ownHost,
            internalTimings: JSON.stringify(internalTimings),
          }),
        );
      }
      process.exit(1);
    }

    if (this.debug) {
      this.logger.info('Script execution complete');
    }

    internalTimings.end = Date.now();

    await this.topic.publishMessage(
      PubSubManager.createMessageSchema('THREAD_COMPLETE', {
        eventMessage: `WORKER Execution Complete`,
        listIssue: config.FUEL_LIST_ISSUE_NAME,
        fileName: config.FUEL_SLICE_FILE,
        originalFileName: config.FUEL_ORIGINAL_FILE,
        eventName: 'THREAD_COMPLETE',
        serverHostname: ownHost,
        internalTimings: JSON.stringify(internalTimings),
      }),
    );
  }

  /**
   * Fetches the schema data for a given schema type.
   * @param schemaType - The type of the schema.
   * @returns A Promise containing the schema data.
   */
  private async fetchSchema(schemaType: string): Promise<any> {
    return firstValueFrom(
      this.schemaManager.getOrganizationSchema({
        typeSlug: schemaType,
      }),
    );
  }
}

export default WorkerProcessor;
