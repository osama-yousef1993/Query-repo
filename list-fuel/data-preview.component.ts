import {
  Component,
  Inject,
  LOCALE_ID,
  OnDestroy,
  OnInit,
  Renderer2,
  ViewChild,
  ViewContainerRef,
} from '@angular/core';
import { formatDate } from '@angular/common';
import { ActivatedRoute, Router } from '@angular/router';

import { BehaviorSubject, Subject, takeUntil } from 'rxjs';
import { SelectedListService } from '../shared/services/selected-list.service';
import { DataTableComponent } from '../shared/data-table/data-table.component';
import {
  CleanTempsFromUploadIdArgs,
  CleanTempsFromUploadIdGQL,
  TemporaryNodesSortGQL,
  DownloadTempGQL,
  DownloadTempArgs,
  FileUploadsGQL,
  GetNodeCountGQL,
  MergeTempsGQL,
  MergeTempsArgs,
  PurgeTempsFromUploadIdGQL,
  PurgeTempsFromUploadIdArgs,
  FinalizeTempsIdGQL,
  FinalizeTempsIdArgs,
  RemoveTempNodeGQL,
  RemoveTempNodeMutationVariables,
  GetNodeCountArgs,
  RunGlobalSchemaValidationArgs,
  RunGlobalSchemaValidationGQL,
  ClearInboundIdAndFinalizeTempIdGQL,
  ClearInboundIdAndFinalizeTempIdArgs,
  GetTempNodeGQL,
  SetUseForListOnlyArgs,
  SetUseForListOnlyGQL,
  UpdateNameGQL,
  RunLocalSchemaValidationGQL,
  UpdateNameArgs,
  SaveTempGQL,
  RunLocalSchemaValidationArgs,
  GetNodeCountResponse,
  SyncDataToFredArgs,
  SyncDataToFredGQL,
} from '../shared/__generated__/generated';
import { BrtButtonsService } from '../shared/services';
import { ModalDialogService } from '../shared/modal/modal-dialog/modal-dialog.service';

import {
  IFileData,
  IListIssue,
  ITempNode,
  IConflictConfig,
  ITabConfig,
  ConflictType,
} from '../shared/models';
import { saveAs } from 'file-saver';
import { ModalResultService } from '../shared/modal/modal-result/modal-result.service';
import { MatTabChangeEvent } from '@angular/material/tabs';
import { EditRowComponent } from '../edit-row/edit-row.component';
import { MatDialog } from '@angular/material/dialog';
import { ListLanderService } from '../list-lander/services/list-lander.service';
import { EStatus, IListOfListIssue } from '../list-lander/types';
import { constructConflict } from './utils/util';
import { DataPreviewService } from './services/data-preview.service';

@Component({
  selector: 'workspace-data-preview',
  templateUrl: './data-preview.component.html',
  styleUrls: ['./data-preview.component.scss'],
})
export class DataPreviewComponent implements OnInit, OnDestroy {
  // Labels
  entriesLabel = 'Entries: ';
  cardDefaultSubtitle = 'Download original template';
  cardButtonLabel = 'download';
  cardButtonTooltipLabel =
    'Keep a copy of your progress and any conflicts found.';
  okayLabel = 'Okay';
  successLabel = 'Success!';
  mergeTempErrorLabel =
    'There was a problem completing your import. Please try again.';
  mergeTempSuccessLabel = ' entries were updated in the main system for ';
  noConflictLabel = 'No Conflicts Found';

  listType = 'company';
  fileData: IFileData;
  selectedListIssue: IListIssue;
  baseOperationVars;
  tabConfig: ITabConfig[] = [
    { label: 'All Entries', rowStyle: 'default' },
    { label: 'Conflicts', rowStyle: 'none' },
  ];
  rawTempNodes: any[]; // A pure record of temp nodes to reference for data lookups
  rawConflictTempNodes: any[]; // A pure record of conflict temp nodes to reference for data lookups
  listLanderService: ListLanderService;
  dataPreviewService: DataPreviewService;

  // Pagination variables
  private pageSize = 50;
  private offset = 0;

  // Observables
  loadingNext$ = new BehaviorSubject(false); // Handles the status of next batch of temp nodes loading
  cardMessage$ = new BehaviorSubject(
    'Please press NEXT if the data shown below matches your file. If not, please CANCEL and upload a new file.',
  ); // Message on the fuel card
  prefetchBuffer$ = new BehaviorSubject<ITempNode[]>([]); // Stores the next 'n' temp nodes batch
  cleanNodeCompleted$ = new BehaviorSubject<boolean>(false); // Used to trigger UI changes after clean temp nodes is called
  hasErrorMessages$ = new BehaviorSubject<boolean>(false); // Looking at when totalNumberOfConflicts > 0 to control ui elements for conflict state
  totalNumberOfConflicts$ = new BehaviorSubject(0); // Total number of conflicts for temp nodes on a file upload
  totalEntries$ = new BehaviorSubject(0); // Total entries of temp nodes for a file upload
  destroy$: Subject<boolean> = new Subject<boolean>(); // Handles destroying open subscriptions
  columnDefinitions$ = new BehaviorSubject<string[]>([]); // The columns sent to data-table for rendering
  dataRows$ = new BehaviorSubject<any>([]); // tempNodes observable sent to data table component
  dataRowsWithConflicts$ = new BehaviorSubject<any>([]); // tempNodes observable sent to data table component
  listName$ = new BehaviorSubject<string>(''); // i.e 'Best Employer For Veterans' set from import list data on list selection
  currentTab$ = new BehaviorSubject<ITabConfig>(this.tabConfig[0] ?? ''); // Currently selected tab above the table
  finalizedData$ = new BehaviorSubject<boolean>(false); // Tracking if the list data has been finalized successfully

  // Sort variables
  sortedColumns: {
    [key: string]: { ascending: boolean; direction: string };
  } = {
    rank: { ascending: true, direction: 'ASC' },
  };
  sortKeys = [];

  @ViewChild(DataTableComponent) dataTable: DataTableComponent;

  constructor(
    private getNodeCountGQL: GetNodeCountGQL,
    private temporaryNodesGQL: TemporaryNodesSortGQL,
    private selectedListService: SelectedListService,
    private renderer: Renderer2,
    private modalDialogService: ModalDialogService,
    private modalResultService: ModalResultService,
    private router: Router,
    private cleanTempsGQL: CleanTempsFromUploadIdGQL,
    private downloadTempGQL: DownloadTempGQL,
    private fileUploadsGQL: FileUploadsGQL,
    private purgeTempsFromUploadIdGQL: PurgeTempsFromUploadIdGQL,
    private route: ActivatedRoute,
    private mergeTempsGQL: MergeTempsGQL,
    private finalizeTempsIdGQL: FinalizeTempsIdGQL,
    private removeTempNodeGQL: RemoveTempNodeGQL,
    private clearIdGQL: ClearInboundIdAndFinalizeTempIdGQL,
    private globalSchemaValidationGQL: RunGlobalSchemaValidationGQL,
    private setUseForListOnlyGQL: SetUseForListOnlyGQL,
    private getTempNodeGQL: GetTempNodeGQL,
    @Inject(LOCALE_ID) private locale: string,
    private dialog: MatDialog,
    private updateNameGQL: UpdateNameGQL,
    private runLocalSchemaValidationGQL: RunLocalSchemaValidationGQL,
    private saveTempGQL: SaveTempGQL,
    private viewContainerRef: ViewContainerRef, // allows you to keep child components like a regular component that will get its dependencies injected automatically
    private syncDataToFredGQL: SyncDataToFredGQL,
    listLanderService: ListLanderService,
    dataPreviewService: DataPreviewService,
  ) {
    this.temporaryNodesGQL = temporaryNodesGQL;
    this.cleanTempsGQL = cleanTempsGQL;
    this.listLanderService = listLanderService;
    this.dataPreviewService = dataPreviewService;
  }

  ngOnInit(): void {
    this.getFileUploadMetaData(); // Meta data from the file uploaded like fileName
    this.setSelectedIssueFromState(); // The issue selected by user on /fuel route
    this.dataPreviewService.setBaseOperationVars({
      targetNodeType: 'company',
      targetListIssueNaturalId: 'best-tax-accounting-firms02025',
      targetListIssueYear: 2025,
      targetUploadId: 'c11a8b0d-1b92-4d1f-aa97-94a5fc508506',
      targetAllowPremiumProfiles: false,
    }); // Persisted query/mutation request params
    this.baseOperationVars = this.dataPreviewService.getBaseOpaerationVars();
    this.fetchNodeCount(); // Controls value shown on fuel-card and conflict tab-badge
    this.fetchColumnNames(); // Column names for the table
    this.fetchTempNodesFromGraphQL(); // All Data rows for the table
    this.fetchConflictTempNodesFromGraphQL(); // Conflict tabs data rows
    this.setBertieHeaderButtons(); // Set the bertie header buttons

    // create subscriptions on behavior subjects to destroy them all on component destroy
    // these were being fired even after component destroy since we weren't using | async
    //  directly in the html file for cleanup.
    // never call destroy on a subject directly
    this.prefetchBuffer$.pipe(takeUntil(this.destroy$)).subscribe({});
    this.finalizedData$.pipe(takeUntil(this.destroy$)).subscribe({});
  }

  /**
   * Set the header buttons in bertie
   * initially after upload buttons should be
   * cancel and next
   * If none passed in then default provided
   */
  setBertieHeaderButtons(buttonConfig?: any) {
    const defaults = [
      {
        buttonType: 'secondary',
        label: 'Cancel',
        disabled: false,
        action: () => this.handleCancelButton(),
      },
      {
        buttonType: 'primary',
        label: 'Next',
        disabled: false,
        action: () => this.handleNextButton(),
      },
    ];
    BrtButtonsService.setButtons(buttonConfig ?? defaults);
  }

  /**
   * Consolidate all the calls being made repeatedly to the GraphQL
   * server to get the current state of the nodes
   */
  getCurrentStateOfNodes() {
    this.fetchTempNodesFromGraphQL(); // All Data rows for the table
    this.fetchConflictTempNodesFromGraphQL(); // Conflict tabs data rows
    this.fetchNodeCount(); // Controls value shown on fuel-card and conflict tab-badge
  }

  /**
   * Fetch the number of conflicts from the GraphQL server
   */
  fetchNodeCount() {
    const nodeCountArgs: GetNodeCountArgs = {
      ...this.baseOperationVars,
    };
    this.getNodeCountGQL
      .watch({ ...nodeCountArgs })
      .valueChanges.pipe(takeUntil(this.destroy$))
      .subscribe({
        next: (results) => this.handleNewNodeCount(results.data.getNodeCount),
        error: (err) => console.error('Observable emitted an error: ' + err),
        complete: () =>
          console.info('Observable emitted the complete notification'),
      });
  }

  /**
   * Handle the node count results from the getNodeCountGQL call
   * @param results getNodeCount Results TO DO: get actual type if possible
   */
  handleNewNodeCount(results: GetNodeCountResponse) {
    if (results) {
      const conflictCount = results.conflictCount;
      const totalEntriesCount = results.totalTempCount;
      this.totalNumberOfConflicts$.next(conflictCount);
      this.totalEntries$.next(totalEntriesCount);
      this.hasErrorMessages$.next(conflictCount > 0);
      if (this.cleanNodeCompleted$.getValue() === true) {
        this.getErrorMessageByConflicts(); // Set the fuel card error message
      }
    }
  }

  /**
   * Get the file upload data from state
   */
  getFileUploadMetaData() {
    this.selectedListService
      .getUploadedFileMetaData()
      .pipe(takeUntil(this.destroy$))
      .subscribe((fileUploadData) => {
        if (fileUploadData) {
          this.fileData = fileUploadData;
        }
      });
  }

  /**
   * Based on the selected index then find our tab in the tab config to return
   * to child components. Data-table uses the rowStyle on the tabConfig to control row highlights
   * based on current tab.
   * @param event tab meta data from angular that contains the index
   */
  handleTabChange(event: MatTabChangeEvent) {
    const matchedTabRow: ITabConfig = this.tabConfig.find(
      (tab, index) => index === event.index,
    ) as ITabConfig;
    // Reset loadingNext so scroll isn't prevented
    this.loadingNext$.next(false);
    this.currentTab$.next(matchedTabRow);
  }

  /**
   * Open a modal that allows user to
   * cancel import: navigate to fuel and will auto select list issue stored in state and clear temp nodes
   * nevermind: closes modal and takes no action
   */
  handleCancelButton(): void {
    const dialogRef = this.modalDialogService.openDialog({
      content:
        'If you cancel now, you will erase the imported data. Are you sure? You might want to download the data first.',
      actionLabel: 'Cancel this import',
      cancelLabel: 'Nevermind',
    });
    dialogRef
      .afterClosed()
      .pipe(takeUntil(this.destroy$))
      .subscribe({
        next: (results: boolean) => {
          if (results === true) {
            this.purgeNodesByFileId().then((results) => {
              // true result means that the action label was clicked
              if (results) {
                console.info('completed purging temp nodes');
                // clear the buttons from the header
                this.setBertieHeaderButtons([]);
                this.router.navigate(['/fuel']);
              }
            });
          }
        },
        error: (e: any) => {
          console.error('Error purging nodes', e);
        },
        complete: () => {
          console.info('completed and setting open to false');
          this.modalDialogService.setDialogOpen(false);
        },
      });
  }

  /**
   * Open a modal that allows user to
   * edit row data.
   */
  handleEditOption(event: any, rowData: any): void {
    const rawTempNode = this.getRawTempNode(rowData);
    const modalData = {
      content: {
        rowData,
        useForListOnly: rawTempNode.useForListOnly,
        rawTempNodeFuelId: rawTempNode.fuelId,
        baseOperationsVars: this.baseOperationVars,
      },
      title: 'Edit Row',
      actionLabel: 'Save Changes',
      listType: this.listType,
    };
    const dialogRef = this.dialog.open(EditRowComponent, {
      data: modalData,
      height: '542px',
      width: '520px',
      autoFocus: false,
      viewContainerRef: this.viewContainerRef, // extends the DI'd items to the modal
    });
    dialogRef
      .afterClosed()
      .pipe(takeUntil(this.destroy$))
      .subscribe({
        next: async (results) => {
          if (results) {
            const updatedMitigationsJSON = JSON.stringify(
              this.mapControlToMitigations(
                results.newMitigations,
                JSON.parse(rawTempNode.mitigationsJSON as string),
              ),
            );
            await this.saveTempNode(updatedMitigationsJSON, rawTempNode.fuelId);
            if (results.useForListOnly !== rawTempNode.useForListOnly) {
              await this.handleUseForListOnly(null, true, rawTempNode.fuelId);
            }
            await this.runLocalValidation(rawTempNode.fuelId);
            await this.runGlobalValidation();
            this.fetchNodeCount();
          }
        },
        error: (e) => {
          console.error('Error on edit modal', e);
        },
        complete: () => {
          console.info('completed edit modal');
        },
      });
  }

  /**
   * Map the values of the form controls back to the mitigations json
   * to prep the data for mutation
   * @param incomingNodeChanges the form controls with values from the modal
   * @param currentNodeValues the current raw temp node
   */
  mapControlToMitigations(incomingNodeChanges: any, currentNodeValues: any) {
    Object.keys(incomingNodeChanges).forEach((key) => {
      if (Object.prototype.hasOwnProperty.call(currentNodeValues, key)) {
        currentNodeValues[key] = incomingNodeChanges[key].value;
      }
    });
    return currentNodeValues;
  }

  /**
   * Save the the changes made to the temp node
   * @returns Promise from observable
   */
  saveTempNode(mitigationsJson: string, targetTempId: string): Promise<any> {
    const saveTempVar = {
      mitigationsJson,
      targetTempId,
    };
    return new Promise((resolve) => {
      this.saveTempGQL
        .mutate(saveTempVar)
        .pipe(takeUntil(this.destroy$))
        .subscribe({
          next: (results) => {
            if (results) {
              resolve(results);
            } else {
              resolve(results);
            }
          },
          error: (e) => {
            resolve(e);
          },
          complete: () =>
            console.info('completed save temp for node: ' + targetTempId),
        });
    });
  }

  /**
   * Purge data by file upload id on cancel button
   * @returns Promise from observable
   */
  purgeNodesByFileId(): Promise<any> {
    const purgeTempsVar: PurgeTempsFromUploadIdArgs = {
      ...this.baseOperationVars,
    };
    return new Promise((resolve) => {
      this.purgeTempsFromUploadIdGQL
        .mutate(purgeTempsVar)
        .pipe(takeUntil(this.destroy$))
        .subscribe({
          next: (results) => {
            if (results) {
              resolve(results);
            } else {
              resolve(results);
            }
          },
          error: (e) => {
            resolve(e);
          },
          complete: () =>
            console.info(
              'completed purge temp for file upload id: ' +
                this.fileData.fuelId,
            ),
        });
    });
  }

  /**
   * Syncs data to FRED after a successful merge of temporary nodes.
   * @param {SyncDataToFredArgs} syncDataToFredArgs - The arguments required for syncing data to FRED.
   * @param {string} syncDataToFredArgs.targetListIssueNaturalId - The natural ID of the list issue that needs syncing.
   * @param {number} syncDataToFredArgs.targetListIssueYear - The year associated with the list issue.
   * @param {string} syncDataToFredArgs.targetNodeType - The type of node being synced, such as 'Company' or other types.
   */
  private syncDataToFred(): void {
    const syncDataToFredArgs: SyncDataToFredArgs = {
      targetListIssueNaturalId: this.baseOperationVars.targetListIssueNaturalId,
      targetListIssueYear: this.baseOperationVars.targetListIssueYear,
      targetNodeType: this.baseOperationVars.targetNodeType,
    };

    console.info(`Syncing data to fred.... args: ${syncDataToFredArgs}`);

    this.syncDataToFredGQL
      .mutate(syncDataToFredArgs)
      .pipe(takeUntil(this.destroy$))
      .subscribe({
        next: (syncResults) => {
          if (syncResults?.data?.syncDataToFred?.success) {
            console.info('Data synced to Fred successfully');
          } else {
            console.error('Failed to sync data to Fred');
          }
        },
        error: (error) => {
          console.error('Error syncing data to Fred: ', error);
        },
        complete: () => {
          console.info('Completed syncing data to Fred');
        },
      });
  }

  /* On click of the Finalize data button from the brt header then
   * call the mergeTemps mutation to merge clean, conflict-free
   * temp nodes into 'main'.
   * Open success modal on success response from mutation
   * else open error modal
   */
  handleFinalizeButton(): void {
    const mergeTempsArgs: MergeTempsArgs = {
      ...this.baseOperationVars,
      updateNode: true, // Controls entity node update or list-issue rltnship TO DO: will be tied to the toggle in the modal
    };
    this.mergeTempsGQL
      .mutate(mergeTempsArgs)
      .pipe(takeUntil(this.destroy$))
      .subscribe({
        next: (results) => {
          this.setBertieHeaderButtons([]);
          if (results?.data?.mergeTemps?.success) {
            this.finalizedData$.next(true);
            this.setBertieHeaderButtons([
              {
                buttonType: 'secondary',
                label: 'Cancel',
                disabled: false,
                action: () => this.handleCancelButton(),
              },
            ]);
            // Call success modal
            const customSuccessLabel =
              this.totalEntries$.getValue() + this.mergeTempSuccessLabel;
            const optionalContentLabel = this.listName$.getValue();
            this.modalResultService.openDialog({
              success: true,
              buttonText: this.okayLabel,
              title: this.successLabel,
              subtitle: customSuccessLabel,
              optionalContent: optionalContentLabel,
            }); // Open error dialog
            this.modalResultService.setDialogOpen(true);

            // Sync data to Fred after a successful mergeTemps
            this.syncDataToFred();
          } else {
            this.finalizedData$.next(false);
            this.modalResultService.openDialog({
              success: false,
              buttonText: this.okayLabel,
              subtitle: this.mergeTempErrorLabel,
            }); // Open error dialog
            this.modalResultService.setDialogOpen(true);
          }
        },
        error: (e) => {
          this.finalizedData$.next(false);
          console.error('Error merging temp nodes: ', e);
        },
        complete: () => {
          console.info('completed merging temp nodes subscription');
        },
      });
  }

  /**
   * Utility function that changes behavior within the html based
   * on the presence of n conflicts in the data.
   */
  getErrorMessageByConflicts() {
    this.hasErrorMessages$.next(this.totalNumberOfConflicts$.getValue() > 0);
    this.cardMessage$.next(
      this.hasErrorMessages$.getValue()
        ? `${this.totalNumberOfConflicts$.getValue()} conflicts have been found. Please resolve to finalize this import.`
        : `Your list looks great and is ready to finalize.`,
    );
    // if there are no conflicts then enable finalize button
    if (
      this.hasErrorMessages$.getValue() === false &&
      this.finalizedData$.getValue() === false
    ) {
      this.setBertieHeaderButtons([
        {
          buttonType: 'secondary',
          label: 'Cancel',
          disabled: false,
          action: () => this.handleCancelButton(),
        },
        {
          buttonType: 'primary',
          label: 'finalize data',
          disabled: false,
          className: 'finalize-button',
          action: () => this.handleFinalizeButton(),
        },
      ]);
    }
    // if there are conflicts then disable finalize button
    if (
      this.hasErrorMessages$.getValue() === true &&
      this.finalizedData$.getValue() === false
    ) {
      this.setBertieHeaderButtons([
        {
          buttonType: 'secondary',
          label: 'Cancel',
          disabled: false,
          action: () => this.handleCancelButton(),
        },
        {
          buttonType: 'primary',
          label: 'finalize data',
          disabled: true,
          className: 'finalize-button',
          action: () => this.handleFinalizeButton(),
        },
      ]);
    }
  }

  /**
   * Next button will call clean temp mutation
   * to do preliminary clean up of data to show errors or
   * perfect state of temp nodes
   */
  handleNextButton(): void {
    const cleanTempVars: CleanTempsFromUploadIdArgs = {
      ...this.baseOperationVars,
    };

    this.cleanTempsGQL
      .mutate(cleanTempVars)
      .pipe(takeUntil(this.destroy$))
      .subscribe({
        next: (results) => {
          if (results) {
            // TO DO: use switchMap to handle nested observable but have to resolve RxJS mod fed issues
            // https://github.com/Forbes-Media/lists-fuel/issues/361
            this.finalizeIDsForTempNodes().then(() => {
              this.cleanNodeCompleted$.next(true);
              this.fetchColumnNames(); // refetch the columns since we now have an edit config column for the ellipsis
              this.getCurrentStateOfNodes(); // Fetch the current state of the nodes
              // Scroll back to top of the screen after user presses next
              this.scrollTo(0);
              this.setBertieHeaderButtons([
                {
                  buttonType: 'secondary',
                  label: 'Cancel',
                  disabled: false,
                  action: () => this.handleCancelButton(),
                },
                {
                  buttonType: 'primary',
                  label: 'finalize data',
                  disabled: this.hasErrorMessages$.getValue(),
                  className: 'finalize-button',
                  action: () => this.handleFinalizeButton(),
                  tooltipMessage:
                    'Please resolve your conflicts to finalize your imported data.',
                },
              ]);
              // Reset the offset and prefetched buffer when the user presses next
              // to load a batch of temp nodes that will go through the clean process
              this.offset = 0;
              this.prefetchBuffer$.next([]);
            });
          } else {
            this.cleanNodeCompleted$.next(false);
          }
        },
        error: (e) => {
          this.cleanNodeCompleted$.next(false);
          console.error('Error cleaning temp nodes: ', e);
        },
        complete: () => {
          console.info('completed cleaning temp nodes');
          this.loadingNext$.next(false);
        },
      });
  }

  /**
   * When the 'Use Existing Name" option
   * is chosen to resolve a name mismatch warning
   * then run
   */
  useExistingName(tempNodeFuelId: any) {
    const updateNameArgs: UpdateNameArgs = {
      targetTempId: tempNodeFuelId,
      targetUploadId: this.baseOperationVars.targetUploadId,
      targetNodeType: this.baseOperationVars.targetNodeType,
      companyNewName: '',
    };
    return new Promise((resolve, reject) => {
      this.updateNameGQL
        .mutate(updateNameArgs)
        .pipe(takeUntil(this.destroy$))
        .subscribe({
          next: (results) => {
            if (results) {
              resolve(results);
            } else {
              resolve(null);
            }
          },
          error: (e) => {
            reject(e);
            console.error('Error on use existing name: ', e);
          },
          complete: () => {
            this.fetchNodeCount(); // Update the tab counts;
            console.info('completed use existing name observable');
          },
        });
    });
  }

  /**
   * After cleaning temp nodes then check for
   * any rows with blank IDs and handle them
   * @returns promise
   */
  finalizeIDsForTempNodes(): Promise<any> {
    const finalizeTempsVars: FinalizeTempsIdArgs = {
      ...this.baseOperationVars,
    };
    return new Promise((resolve, reject) => {
      this.finalizeTempsIdGQL
        .mutate(finalizeTempsVars)
        .pipe(takeUntil(this.destroy$))
        .subscribe({
          next: (results) => {
            if (results) {
              resolve(results);
            } else {
              resolve(results);
            }
          },
          error: (e) => {
            reject(e);
            console.error('Error finalizing temp nodes: ', e);
          },
          complete: () => {
            console.info('completed finalizing temp nodes observable');
          },
        });
    });
  }

  /**
   * Destroy any subscriptions using the takeUntil method
   * on subscribe to auto unsubscribe.
   */
  ngOnDestroy(): void {
    this.destroy$.next(true);
    this.setBertieHeaderButtons([]);
    this.destroy$.unsubscribe();
  }

  /**
   * Fetch the columns for the table using
   * file upload id
   */
  fetchColumnNames() {
    const where: any = {
      fuelId:
        this.route.snapshot?.queryParamMap?.get('fuelId') ??
        this.fileData?.fuelId,
    };
    this.fileUploadsGQL
      .watch({ where })
      .valueChanges.pipe(takeUntil(this.destroy$))
      .subscribe((results: any) => {
        if (results.data?.fileUploads[0]) {
          // Column names HAVE to align with the user data json
          const columns = results.data?.fileUploads[0]?.userColumn.map(
            (colName: string) => colName.replace(/.*_/, ''),
          );
          if (this.cleanNodeCompleted$.getValue()) {
            columns.push('editConfig');
          }
          this.columnDefinitions$.next(columns);
        }
      });
  }

  /**
   * Get the selected list issue and list type from
   * service. Values were set in import-list-data at /fuel.
   */
  setSelectedIssueFromState() {
    // Get the selected list issue
    this.selectedListService
      .getSelectedListIssue()
      .pipe(takeUntil(this.destroy$))
      .subscribe((listIssue) => {
        this.selectedListIssue = listIssue as IListIssue;
        const listName =
          this.selectedListIssue?.name + ', ' + this.selectedListIssue?.year;
        this.listName$.next(listName);
      });

    // Get the list type. Example: 'company'
    this.selectedListService
      .getListType()
      .pipe(takeUntil(this.destroy$))
      .subscribe((listType) => {
        this.listType = listType as string;
      });
  }

  /**
   * Handle click event on the subtitle
   * Downloads the original template for the chosen list issue
   * template and filename is fetched from shared service
   */
  public subTitleAction(): void {
    this.selectedListService.downloadSchema();
  }

  /**
   * Fuel-Card button action on data-preview is to 'Download'
   * the state of the temp nodes
   */
  public cardButtonAction() {
    // if the data has been finalized then the download will
    // get the finalized version of temp nodes
    if (this.finalizedData$.getValue() === true) {
      // the only values that need to be accurate are name, year and naturalId
      const convertToListLanderType: IListOfListIssue = {
        listName: this.selectedListIssue.name,
        labels: [],
        launchDate: this.selectedListIssue.launchDate,
        lastEdited: '',
        lastEditedBy: '',
        status: EStatus.Published,
        listId: this.selectedListIssue.naturalId,
        year: this.selectedListIssue.year,
      };
      this.listLanderService.downloadFinalizedData(convertToListLanderType);
    } else {
      this.downloadPreFinalizedNodes();
    }
  }

  /**
   * Download the pre finalized temp nodes
   */
  public downloadPreFinalizedNodes() {
    const downloadTempGQLVars: DownloadTempArgs = {
      ...this.baseOperationVars,
      includeConflicts: true,
      includeOtherConflicts: false,
    };
    const dateString = formatDate(Date.now(), 'yyyy-MM-dd_HHmmss', this.locale);
    this.downloadTempGQL
      .watch(downloadTempGQLVars)
      .valueChanges.pipe(takeUntil(this.destroy$))
      .pipe(takeUntil(this.destroy$))
      .subscribe({
        next: (results) => {
          if (results) {
            const data: Blob = new Blob([...results.data.downloadTemp.result], {
              type: 'text/csv;charset=utf-8',
            });
            saveAs(
              data,
              `${this.selectedListIssue.listUri}-${this.selectedListIssue.naturalId}-${dateString}`,
            );
            // Set the cancel button to enabled state
            BrtButtonsService.setButton(0, {
              buttonType: 'secondary',
              label: 'Cancel',
              disabled: false,
              action: () => this.handleCancelButton(),
            });
          } else {
            console.info('no results after download');
          }
        },
        error: (e) => {
          console.error('Error downloading temp nodes: ', e);
        },
        complete: () => console.info('completed temp node download'),
      });
  }

  /**
   * Initial fetch of temp nodes from GraphQL
   * @returns void
   */
  public fetchTempNodesFromGraphQL(): void {
    const options: any = {
      ...this.baseOperationVars,
      customSort: this.sortKeys,
      includeInfoMessages: true,
      includeWarnings: true,
      limit: this.pageSize, // Limit specifies the number of rows to retain from the result set
    };

    this.temporaryNodesGQL
      .fetch({
        ...options,
        cleanedTempNodes: this.cleanNodeCompleted$.getValue(),
        conflictOnlyNodes: false,
      })
      .pipe(takeUntil(this.destroy$))
      .subscribe((results: any) => {
        const tempNodes: ITempNode[] = results.data.sortTemps.result;
        if (tempNodes) {
          this.rawTempNodes = tempNodes;
          const nextNodes = this.checkIfNodesCleanedForRowUI(tempNodes);

          if (nextNodes.length) {
            this.dataRows$.next(nextNodes);
            this.offset = this.offset + tempNodes.length;
          }
          if (
            this.dataRows$.getValue().length < this.totalEntries$.getValue()
          ) {
            this.loadMoreTempNodes();
          }
        }
      });
  }

  /**
   * Initial fetch of temp nodes from GraphQL
   * @returns void
   */
  public fetchConflictTempNodesFromGraphQL(): void {
    const options: any = {
      ...this.baseOperationVars,
      customSort: this.sortKeys,
      includeInfoMessages: true,
      includeWarnings: true,
      // limit: this.pageSize, // set no limit and load all conflicts
    };
    this.temporaryNodesGQL
      .fetch({
        ...options,
        cleanedTempNodes: this.cleanNodeCompleted$.getValue(),
        conflictOnlyNodes: true,
      })
      .pipe(takeUntil(this.destroy$))
      .subscribe((results: any) => {
        const conflictNodes: ITempNode[] = results.data.sortTemps.result;

        const validConflictNodes = conflictNodes?.filter((node) => node);
        this.rawConflictTempNodes = validConflictNodes;

        if (
          this.cleanNodeCompleted$.getValue() === true &&
          validConflictNodes?.length > 0
        ) {
          this.dataRowsWithConflicts$.next(
            this.assembleMetaTempNode(validConflictNodes),
          );
        }
      });
  }

  /**
   * After cleanTemps mutation has run
   * then configure conflictConfig for row rendering
   * then configure editConfig for ellipsis menu rendering
   * spread conflict config into node and previous temp node values
   * and add the edit config as nested object
   *
   * @param tempNodes base temp node array
   * @returns ITempNode[] merged with conflict config object
   */
  assembleMetaTempNode(tempNodes: any): any {
    const nextNodes = tempNodes.map((node: ITempNode) => {
      const conflictConfig = this.checkForTempNodeConflicts(node);
      let newObj: any = {
        isFuelOrganization: node.isFuelOrganization ?? false,
        ...(JSON.parse(node.mitigationsJSON) as ITempNode),
        ...conflictConfig,
      };
      // Get editConfig made after the conflict config is on the new object
      // to get the conflict enum for evaluation
      const editConfig = this.getEditConfig(newObj);
      newObj = {
        ...newObj,
        editConfig,
      };
      return newObj;
    });
    return nextNodes;
  }

  /**
   * Each node will have default items in the edit config but
   * check here for specific conflict type
   * 'Use for list only' or 'clear id' or 'use existing name'
   * @returns editConfig to be added to the row data
   * example config from bertie:
   *  { item: 'option a', displayName: 'Option A' },
   *  { item: 'option b' },
   *   { item: 'option c', disabled: true },
   *   { item: 'option d (hidden)', hidden: true },
   */
  getEditConfig(transformedTempNode: {
    conflictType: string;
    subConflicts: string | string[];
  }) {
    const editConfig = [
      {
        item: 'listOnly',
        displayName: 'Use For This List Only',
        info: 'Imported data will appear on the list and will not affect Profile.',
        action: ($event: any, row: any) =>
          this.handleUseForListOnly(row, false, null),
      },
      {
        item: 'edit',
        displayName: 'Edit Row',
        info: 'Edit all fields and/or find a better match.',
        action: ($event: any, row: any) => this.handleEditOption($event, row),
      },
      {
        item: 'delete',
        displayName: 'Remove From List',
        action: ($event: any, row: any) => this.handleRemoveOption($event, row),
      },
    ];
    /**
     *  Ellipsis order to be preserved:
     *  0 - USE EXISTING NAME - ONLY IF NAME mismatch was detected (warning) - hidden otherwise
     *  1 - CLEAR INBOUND ID - ONLY IF ID error OR NAME mismatch were detected (error or warning)
     *  2 - USE FOR LIST ONLY - ONLY IF NO ID error
     *  3 - EDIT
     *  4 - REMOVE ENTRY
     */
    if (
      transformedTempNode.conflictType ===
      ConflictType[ConflictType.NameMismatch]
    ) {
      editConfig.splice(0, 0, {
        item: 'useExisting',
        displayName: 'Use Existing Profile',
        info: `Replace your imported name with the existing profile name.`,
        action: ($event, row) => this.handleUseExistingOption($event, row),
      });
      editConfig.splice(1, 0, {
        item: 'clearId',
        displayName: 'Clear ID',
        info: 'Auto-match to an existing profile or create a new one if none found.',
        action: ($event, row) => this.handleClearIdOption($event, row),
      });
    } else if (
      transformedTempNode.conflictType ===
      ConflictType[ConflictType.InvalidEntityId]
    ) {
      // Insert Clear ID at index 0 and remove Use for list only from index 0
      editConfig.splice(0, 1, {
        item: 'clearId',
        displayName: 'Clear ID',
        info: 'Auto-match to an existing profile or create a new one if none found.',
        action: ($event, row) => this.handleClearIdOption($event, row),
      });
      //When the conflict type is multiple then additional checks must occur
    } else if (
      transformedTempNode.conflictType === ConflictType[ConflictType.Multiple]
    ) {
      // Remove "Use for List Only" from the ellipsis menu
      // USE FOR LIST ONLY - ONLY IF NO Invalid ID error was detected
      if (
        transformedTempNode.subConflicts.includes(
          ConflictType[ConflictType.InvalidEntityId],
        )
      ) {
        editConfig.splice(0, 1);
      }
      if (
        transformedTempNode.subConflicts.includes(
          ConflictType[ConflictType.NameMismatch],
        )
      ) {
        // USE EXISTING NAME - ONLY IF NAME mismatch was detected (warning) - hidden otherwise.
        // This option should also be present if MULTIPLE errors were detected and at least one of them was NAME mismatch
        editConfig.splice(0, 0, {
          item: 'useExisting',
          displayName: 'Use Existing Profile',
          info: `Replace your imported name with the existing profile name.`,
          action: ($event, row) => this.handleUseExistingOption($event, row),
        });
      }
      if (
        transformedTempNode.subConflicts.includes(
          ConflictType[ConflictType.InvalidEntityId],
        ) ||
        transformedTempNode.subConflicts.includes(
          ConflictType[ConflictType.NameMismatch],
        )
      ) {
        //CLEAR INBOUND ID - ONLY IF ID error or NAME mismatch were detected (error or warning).
        // This option should also be present if MULTIPLE errors were detected
        // and at least one of them was either Invalid ID error OR NAME mismatch
        // Insert Clear ID at index 0 and remove Use for list only from index 0
        editConfig.splice(0, 0, {
          item: 'clearId',
          displayName: 'Clear ID',
          info: 'Auto-match to an existing profile or create a new one if none found.',
          action: ($event, row) => this.handleClearIdOption($event, row),
        });
      }
    }
    return editConfig;
  }

  /**
   * Return the entire node from the raw temp nodes list by naturalId
   * Check if the node is a conflict type and change the node data source if so.
   * The Conflict tab fetches only nodes with conflict type so we won't be able to
   * reference a node that hasn't been saved in the regular rawTempNodes list
   * @param row data row from the table
   * @returns
   */
  getRawTempNode(row) {
    let foundNode;
    if (row.hasConflict) {
      foundNode = this.rawConflictTempNodes.find(
        (obj: { mitigationsJSON: string }) =>
          JSON.parse(obj.mitigationsJSON as string).naturalId === row.naturalId,
      );
    } else {
      foundNode = this.rawTempNodes.find(
        (obj: { mitigationsJSON: string }) =>
          JSON.parse(obj.mitigationsJSON as string).naturalId === row.naturalId,
      );
    }
    return foundNode;
  }

  /**
   * When a user has clicked 'Use for List Only"
   * from the row ellipsis menu then run the mutation to apply
   * data at the list level only.
   * When a user has toggled "Use for List Only" from the modal
   * then we already have the fuelID so call the mutation but
   * do not update on the table yet.
   * @param $event
   * @param row
   */
  handleUseForListOnly(row: any = null, fromModal = false, nodeFuelId = null) {
    const rawTempNode = row ? this.getRawTempNode(row) : null;
    const targetTempId = nodeFuelId ?? rawTempNode.fuelId;
    const setUseForListOnlyArgs: SetUseForListOnlyArgs = {
      targetTempId,
    };
    if (!fromModal) {
      const dialogRef = this.modalDialogService.openDialog({
        content: `Are you sure that you want to skip Profile updates from the imported data for ${row.name} and only use it for the List display?`,
        actionLabel: 'Yes',
        cancelLabel: 'Nevermind',
        buttonColor: 'yellow',
      });

      dialogRef
        .afterClosed()
        .pipe(takeUntil(this.destroy$))
        .subscribe({
          next: async (results: boolean) => {
            if (results === true) {
              // Set the boolean for a row in the table to Use For List Only
              // then call method that fetches node and updates tables
              this.setUseForListOnly(setUseForListOnlyArgs);
            }
          },
          error: (e: any) => {
            console.error(
              'Error on modal dialog on use for list only for: ' +
                rawTempNode.fuelId +
                ': ' +
                e.message,
            );
          },
          complete: () => {
            this.modalDialogService.setDialogOpen(false);
          },
        });
    } else {
      this.setUseForListOnly(setUseForListOnlyArgs);
    }
  }

  /**
   * Run the mutation to set the user for list only flag for a node
   * @param setUseForListOnlyArgs boolean to set use for list only
   */
  setUseForListOnly(setUseForListOnlyArgs) {
    this.setUseForListOnlyGQL
      .mutate(setUseForListOnlyArgs)
      .pipe(takeUntil(this.destroy$))
      .subscribe({
        next: (results: any) => {
          if (results.data?.setUseForListOnly?.success) {
            this.getCurrentStateOfNodes();
          }
        },
        error: (e) => {
          console.error('Error seting use for list only on row: ', e);
        },
        complete: () => {
          console.info('Complete use for list only observable');
        },
      });
  }

  /**
   * When a use has clicked 'Use Existing Name"
   * from the row ellipsis menu then open a modal to warn
   * user and on confirmation perform mutation for use existing name
   * @param event
   * @param row
   */
  handleRemoveOption(
    event: any,
    row: { naturalId: any; totalRow: any; name: any },
  ) {
    // Lookup the 'raw' temp node to find the fuelId to delete it from the list
    const rawTempNode = this.getRawTempNode(row);

    const dialogRef = this.modalDialogService.openDialog({
      content: `Are you sure that you want to remove Row ${row.totalRow}: ${row.name} from your list?`,
      actionLabel: 'Yes',
      cancelLabel: 'Nevermind',
      buttonColor: 'yellow',
    });
    const removeTempNodeParams: RemoveTempNodeMutationVariables = {
      fuelId: rawTempNode.fuelId,
    };

    dialogRef
      .afterClosed()
      .pipe(takeUntil(this.destroy$))
      .subscribe({
        next: (results: boolean) => {
          // If modal close was triggered by confirmation the results are true
          if (results === true) {
            // Remove the node from the list
            this.removeTempNodeGQL
              .mutate(removeTempNodeParams)
              .pipe(takeUntil(this.destroy$))
              .subscribe({
                next: async (results: any) => {
                  if (results.data?.removeTempNode?.success) {
                    await this.runGlobalValidation();
                    this.fetchNodeCount(); // Update the tab counts
                  }
                },
                error: (e) => {
                  console.error('Error removing temp node: ', e);
                },
                complete: () => {
                  console.info('completed remove temp node observable');
                },
              });
          }
        },
        error: (e: { message: string }) => {
          console.error(
            'Error on modal dialog for remove temp node' + ': ' + e.message,
          );
        },
        complete: () => {
          this.modalDialogService.setDialogOpen(false);
        },
      });
  }

  /**
   * When a use has clicked 'Use Existing Name"
   * from the row ellipsis menu then open a modal to warn
   * user and on confirmation perform mutation for use existing name
   * @param event
   * @param row
   */
  handleUseExistingOption(
    event: any,
    row: { naturalId: any; displayName: any; namePrint: any; totalRow: any },
  ) {
    // Lookup the 'raw' temp node to find the fuelId
    const rawTempNodeFuelId = this.getRawTempNode(row).fuelId;
    const dialogRef = this.modalDialogService.openDialog({
      content: `Are you sure that you want to ignore this warning and agree for the imported name ${
        row.displayName ?? row.namePrint
      } on Row ${row.totalRow} to be replaced by the existing system name?`,
      actionLabel: 'Yes',
      cancelLabel: 'Nevermind',
      buttonColor: 'yellow',
    });

    dialogRef
      .afterClosed()
      .pipe(takeUntil(this.destroy$))
      .subscribe({
        next: async (results: boolean) => {
          if (results === true) {
            // First wait for the clear id mutation
            await this.useExistingName(rawTempNodeFuelId);
            this.getCurrentStateOfNodes();
          }
        },
        error: (e: any) => {
          console.error(
            'Error on modal dialog for use existing name' + ': ' + e.message,
          );
        },
        complete: () => {
          this.modalDialogService.setDialogOpen(false);
        },
      });
  }

  /**
   * When a use has clicked 'Clear ID"
   * from the row ellipsis menu then Open a modal to warn user and on confirmation
   * perform mutation for clear id
   * @param event
   * @param row
   */
  handleClearIdOption(
    event: any,
    row: { naturalId: any; name: any; totalRow: any },
  ) {
    // Lookup the 'raw' temp node to find the fuelId
    const rawTempNodeFuelId = this.getRawTempNode(row).fuelId;
    const dialogRef = this.modalDialogService.openDialog({
      content: `Are you sure that you want to clear the provided ID ${row.naturalId} for ${row.name} and have this profile auto-matched or created as a new one?`,
      actionLabel: 'Yes',
      cancelLabel: 'Nevermind',
      buttonColor: 'yellow',
    });

    dialogRef
      .afterClosed()
      .pipe(takeUntil(this.destroy$))
      .subscribe({
        next: async (results: boolean) => {
          if (results === true) {
            // First wait for the clear id mutation
            await this.clearIdOnTempNode(rawTempNodeFuelId);
            // Then wait for global validation
            await this.runGlobalValidation();
          }
        },
        error: (e: any) => {
          console.error(
            'Error on modal dialog on clearID for: ' +
              rawTempNodeFuelId +
              ': ' +
              e.message,
          );
        },
        complete: () => {
          this.modalDialogService.setDialogOpen(false);
        },
      });
  }

  /**
   * Call the mutation RunGlobalSchemaValidation
   * to re validate the temp nodes after a change
   * like Clear ID is chosen to check for new conflicts
   * @returns Promise
   */
  runGlobalValidation(): Promise<any> {
    const globalValidationArgs: RunGlobalSchemaValidationArgs =
      this.baseOperationVars;
    return new Promise((resolve, reject) => {
      this.globalSchemaValidationGQL
        .mutate(globalValidationArgs)
        .pipe(takeUntil(this.destroy$))
        .subscribe({
          next: (results) => {
            if (results) {
              this.offset = 0; // resetting to 0 so scroll is not restricted if user has already loaded in all items
              this.fetchTempNodesFromGraphQL();
              this.fetchConflictTempNodesFromGraphQL();
              resolve(results);
            } else {
              resolve(null);
            }
          },
          error: (e) => {
            reject(e);
          },
          complete: () => {
            console.info('completed global schema validation');
          },
        });
    });
  }

  /**
   * Call the mutation LocalSchemaValidation
   * to re validate the temp nodes after a change
   * from the modal.
   * @returns Promise
   */
  runLocalValidation(tempNodeFuelId: any): Promise<any> {
    const localValidationArgs: RunLocalSchemaValidationArgs = {
      targetTempId: tempNodeFuelId,
      targetUploadId: this.baseOperationVars.targetUploadId,
      targetNodeType: this.baseOperationVars.targetNodeType,
    };

    return new Promise((resolve, reject) => {
      this.runLocalSchemaValidationGQL
        .mutate(localValidationArgs)
        .pipe(takeUntil(this.destroy$))
        .subscribe({
          next: (results) => {
            if (results) {
              resolve(results);
            } else {
              resolve(null);
            }
          },
          error: (e) => {
            reject(e);
          },
          complete: () => {
            console.info('completed local schema validation');
          },
        });
    });
  }

  /**
   * Call the mutation ClearInboundIdAndFinalizeTempId
   * when user selects Clear ID from the ellipsis menu
   * for an Invalid ID Conflict
   * @param tempNodeFuelId the fuel id of the node
   * @returns
   */
  clearIdOnTempNode(tempNodeFuelId: any): Promise<any> {
    const clearIdArgs: ClearInboundIdAndFinalizeTempIdArgs = {
      targetTempId: tempNodeFuelId,
      targetUploadId: this.baseOperationVars.targetUploadId,
      targetNodeType: this.baseOperationVars.targetNodeType,
    };
    return new Promise((resolve, reject) => {
      this.clearIdGQL
        .mutate(clearIdArgs)
        .pipe(takeUntil(this.destroy$))
        .subscribe({
          next: (results) => {
            if (results) {
              resolve(results);
            } else {
              resolve(null);
            }
          },
          error: (e) => {
            reject(e);
          },
          complete: () => {
            this.fetchNodeCount();
            console.info(
              'completed clear id for temp node id: ' + tempNodeFuelId,
            );
          },
        });
    });
  }

  /**
   * If there are any errors with a warning
   *  then return error state of row
   * If there are only warnings
   *  then return warning state of row
   * @param tempNode
   * @returns IConflictConfig
   * error dictionary: https://docs.google.com/spreadsheets/d/1hcVSE1SnAnGmWiRT-c3_CYf-KnO_ED6LfaNj0tFv_nw/edit#gid=1054111230
   */
  checkForTempNodeConflicts(tempNode: ITempNode): IConflictConfig {
    return constructConflict(tempNode);
  }

  /**
   * If the user clicked next then construct the edit confifg and
   * conflict config on the node
   * @param tempNodes will either be userDataJSON or ITemp
   * @returns
   */
  checkIfNodesCleanedForRowUI(tempNodes: ITempNode[]) {
    let nextNodes;

    if (this.cleanNodeCompleted$.getValue() === false) {
      // Unclean temp nodes reference the userDataJSON
      try {
        nextNodes = tempNodes?.map((slice) =>
          JSON.parse(slice?.userDataJSON as string),
        );
      } catch (e) {
        return console.error(e);
      }
    } else {
      // Build object for rows to have data and conflict config
      nextNodes = this.assembleMetaTempNode(tempNodes);
    }
    return nextNodes;
  }

  /**
   * Load more temp nodes using .fetchMore instead of just .watch() from GQL class
   * Method is called when it reaches the scroll threshold set in onTableScroll(e)
   */
  loadMoreTempNodes() {
    const currentOffset = this.offset === 0 ? 50 : this.offset;
    const options: any = {
      ...this.baseOperationVars,
      customSort: this.sortKeys,
      includeInfoMessages: true,
      includeWarnings: true,
      limit: this.pageSize, // Limit specifies the number of rows to retain from the result set
      offset: currentOffset,
      conflictOnlyNodes: false,
    };

    // If there are no items in our buffer then get the next limit amount
    if (this.prefetchBuffer$.getValue().length <= 0) {
      this.loadingNext$.next(true);
      // fetchMore returns a Promise so using .then.catch
      this.temporaryNodesGQL
        .watch({
          ...options,
          cleanedTempNodes: this.cleanNodeCompleted$.getValue(),
        })
        .fetchMore({
          query: this.temporaryNodesGQL.document,
          variables: {
            ...options,
            cleanedTempNodes: this.cleanNodeCompleted$.getValue(),
          },
        })
        .then((results) => {
          const tempNodes = results?.data?.sortTemps?.result;
          if (tempNodes) {
            this.rawTempNodes = this.rawTempNodes.concat(tempNodes);
            const nextNodes = this.checkIfNodesCleanedForRowUI(
              tempNodes as ITempNode[],
            );

            if (tempNodes?.length) {
              if (nextNodes) {
                this.prefetchBuffer$.next(nextNodes);
              }
              this.offset = tempNodes.length + this.offset;
              this.loadingNext$.next(false);
            }
          }
        })
        .catch((error) => {
          console.error('error on fetch more rows', error);
          this.loadingNext$.next(false);
        });
    } else {
      // Load from buffer and combine already shown temp nodes
      const previousAndNextTempNodes = this.dataRows$
        .getValue()
        .concat(this.prefetchBuffer$.getValue());
      // Two different observables for data rows for tab
      this.dataRows$.next(previousAndNextTempNodes); // All entries tab data
      this.offset = previousAndNextTempNodes.length;
      this.prefetchBuffer$.next([]); // Clear the buffer so it knows to load more next time
    }
  }

  /**
   * Called from the html from data-table event emitter callback
   * @param sortedColumns
   */
  sortTable(sortedColumns: {
    [x: string]: { ascending: boolean; direction: string } | { direction: any };
  }) {
    this.sortKeys = [];
    this.offset = 0;
    this.prefetchBuffer$.next([]);

    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore: Unreachable code errorthis.sortKeys = Object.keys(this.sortedColumns).map((key) => {
    this.sortKeys = Object.keys(sortedColumns).map((key) => {
      return {
        field: key,
        order: sortedColumns[key].direction,
      };
    });
    this.sortedColumns = Object.keys(sortedColumns).reduce(
      (acc, key) => {
        const column = sortedColumns[key];
        acc[key] = {
          ascending: 'ascending' in column ? column.ascending : true,
          direction: column.direction,
        };
        return acc;
      },
      {} as { [key: string]: { ascending: boolean; direction: string } },
    );
    this.fetchTempNodesFromGraphQL();
    this.fetchConflictTempNodesFromGraphQL();
  }

  /**
   * Keeps track of where the user is scrolling
   * @param e
   */
  onTableScroll(e: {
    target: { clientHeight: any; scrollHeight: any; scrollTop: any };
  }) {
    // only trigger to load more on the All Entries tab
    if (
      this.currentTab$.getValue().label !== 'Conflicts' &&
      this.dataRows$.getValue().length < this.totalEntries$.getValue()
    ) {
      const tableViewHeight = e.target.clientHeight; // Client height excludes the horizontal scroll bar in calculation vs. offsetHeight
      const tableScrollHeight = e.target.scrollHeight; // length of all table
      const scrollLocation = e.target.scrollTop; // how far user scrolled

      // If the user has scrolled within 700px of the bottom, add more data
      const scrollThreshold = 700;

      const scrollDownLimit =
        tableScrollHeight - tableViewHeight - scrollThreshold;
      // Only load more when scrollDownLimit reached and we haven't loaded all temp nodes
      if (scrollLocation > scrollDownLimit) {
        this.loadMoreTempNodes();
        this.scrollTo(tableScrollHeight + tableViewHeight);
      }
    }
  }

  // Will scroll to top of newly loaded data
  private scrollTo(position: number): void {
    this.renderer.setProperty(
      this.dataTable.scrollContainer.nativeElement,
      'scrollTop',
      position,
    );
  }
}
