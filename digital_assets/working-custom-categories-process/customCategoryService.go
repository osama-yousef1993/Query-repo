package services

import (
	"context"
	"errors"
	"strings"

	"github.com/Forbes-Media/forbes-digital-assets/refactored/common"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/datastruct"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/repository"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/codes"
)

type CustomCategoryService interface {
	// Builds custom category data from FS based on the provided request parameters.
	BuildCustomCategoriesData(ctx context.Context, customCategoryRequest datastruct.CustomCategoryRequest) error

	// Retrieves custom category data from PostgreSQL based on the specified category type (e.g., FT, NFT, CATEGORY).
	GetCustomCategories(ctx context.Context, customCategoryType string) ([]datastruct.CustomCategory, error)

	// Update all custom categories data from FS based on the provided request parameters.
	BuildCustomCategoriesDataFS(ctx context.Context) error
}

type customCategoryService struct {
	dao repository.DAO
}

func NewCustomCategoryService(dao repository.DAO) CustomCategoryService {
	return &customCategoryService{dao: dao}
}

// BuildCustomCategoriesDataFS constructs and inserts custom category data into a PostgreSQL table.
// This function will read all FS data and rebuild it to make ensure that the markets data updated with latest values.
// Parameters:
//   - ctx: Context for managing request lifecycle and tracing.
//
// Returns:
//   - An error if any part of the process (validation, querying, struct construction, insertion) fails.
func (c *customCategoryService) BuildCustomCategoriesDataFS(ctx context.Context) error {
	span, labels := common.GenerateSpan("V2 CustomCategoryService.BuildCustomCategoriesDataFS", ctx)
	defer span.End()
	span.AddEvent("Starting V2 CustomCategoryService.BuildCustomCategoriesDataFS")
	startTime := log.StartTimeL(labels, "Starting V2 CustomCategoryService.BuildCustomCategoriesDataFS")

	queryManager := c.dao.NewCustomCategoryQuery()

	customCategoryRequests, err := queryManager.GetCustomCategoriesDataFS(ctx)

	if err != nil {
		log.Error("Validation failed in BuildCustomCategoriesDataFS: %s", err)
		span.SetStatus(codes.Error, "Validation error in BuildCustomCategoriesDataFS")
		log.EndTimeL(labels, "V2 CustomCategoryService.BuildCustomCategoriesData Finished with Error", startTime, err)
		return err
	}

	for _, customCategoryRequest := range customCategoryRequests {
		err := c.BuildCustomCategoriesData(ctx, customCategoryRequest)
		if err != nil {
			log.Error("Validation failed in BuildCustomCategoriesDataFS: %s", err)
			span.SetStatus(codes.Error, "Validation error in BuildCustomCategoriesDataFS")
			log.EndTimeL(labels, "V2 CustomCategoryService.BuildCustomCategoriesData Finished with Error", startTime, err)
			return err
		}
	}

	// Successful completion
	log.EndTimeL(labels, "V2 CustomCategoryService.BuildCustomCategoriesDataFS Finished", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return nil
}

// BuildCustomCategoriesData constructs and inserts custom category data into a PostgreSQL table.
//
// Parameters:
//   - ctx: Context for managing request lifecycle and tracing.
//   - customCategoryRequest: The CustomCategoryRequest struct containing input values for constructing the query and category data.
//
// Returns:
//   - An error if any part of the process (validation, querying, struct construction, insertion) fails.
func (c *customCategoryService) BuildCustomCategoriesData(ctx context.Context, customCategoryRequest datastruct.CustomCategoryRequest) error {
	span, labels := common.GenerateSpan("V2 CustomCategoryService.BuildCustomCategoriesData", ctx)
	defer span.End()
	span.AddEvent("Starting V2 CustomCategoryService.BuildCustomCategoriesData")
	startTime := log.StartTimeL(labels, "Starting V2 CustomCategoryService.BuildCustomCategoriesData")

	// Step 1: Validate and map the custom category request data
	customCategoryFields, err := c.CheckCustomCategoryRequestData(ctx, customCategoryRequest)
	if err != nil {
		log.Error("Validation failed in CheckCustomCategoryRequestData: %s", err)
		span.SetStatus(codes.Error, "Validation error in CheckCustomCategoryRequestData")
		log.EndTimeL(labels, "V2 CustomCategoryService.BuildCustomCategoriesData Finished with Error", startTime, err)
		return err
	}

	// Step 2: Build a dynamic query based on validated custom category fields
	queryManager := c.dao.NewCustomCategoryQuery()
	query := queryManager.BuildDynamicQuery(ctx, customCategoryFields)

	// Step 3: Fetch data using the generated dynamic query
	result, err := queryManager.FetchDataByTableName(ctx, query, *customCategoryFields)
	if err != nil {
		log.Error("Data fetching failed in FetchDataByTableName: %s", err)
		span.SetStatus(codes.Error, "Data fetching error in FetchDataByTableName")
		log.EndTimeL(labels, "V2 CustomCategoryService.BuildCustomCategoriesData Finished with Error", startTime, err)
		return err
	}

	// Step 4: Build a CustomCategory struct with the fetched data
	data := BuildCustomCategoriesStruct(ctx, customCategoryFields, result)

	// Step 5: Insert the structured custom category data into the PostgreSQL table
	err = queryManager.InsertCustomCategories(ctx, data)
	if err != nil {
		log.Error("Data insertion failed in InsertCustomCategories: %s", err)
		span.SetStatus(codes.Error, "Data insertion error in InsertCustomCategories")
		log.EndTimeL(labels, "V2 CustomCategoryService.BuildCustomCategoriesData Finished with Error", startTime, err)
		return err
	}

	// Successful completion
	log.EndTimeL(labels, "V2 CustomCategoryService.BuildCustomCategoriesData Finished", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return nil
}

// CheckCustomCategoryRequestData validates and maps fields in a CustomCategoryRequest struct.
//
// Parameters:
//   - ctx: Context to manage request lifecycle and logging.
//   - customCategoryRequest: The CustomCategoryRequest struct containing input values for validation and mapping.
//
// Returns:
//   - A pointer to an updated CustomCategoryRequest struct with mapped fields, or an error if validation fails.
func (c *customCategoryService) CheckCustomCategoryRequestData(ctx context.Context, customCategoryRequest datastruct.CustomCategoryRequest) (*datastruct.CustomCategoryRequest, error) {
	span, labels := common.GenerateSpan("V2 CustomCategoryService.CheckCustomCategoryRequestData", ctx)
	defer span.End()
	span.AddEvent("Starting V2 CustomCategoryService.CheckCustomCategoryRequestData")
	startTime := log.StartTimeL(labels, "Starting V2 CustomCategoryService.CheckCustomCategoryRequestData")

	// Validate and map fields based on CategoryName or PlatformId
	var customCategoryFields datastruct.CustomCategoryRequest
	if customCategoryRequest.CategoryName != "" {
		customCategoryFields = BuildCustomCategoryRequestData(ctx, customCategoryRequest.CategoryName, customCategoryRequest)
	} else if customCategoryRequest.PlatformId != "" {
		customCategoryFields = BuildCustomCategoryRequestData(ctx, customCategoryRequest.PlatformId, customCategoryRequest)
	} else {
		// Log the error and set span status before returning
		err := errors.New("missing CategoryName or PlatformId in CheckCustomCategoryRequestData")
		log.EndTimeL(labels, "V2 CustomCategoryService.CheckCustomCategoryRequestData Finished with Error", startTime, err)
		span.SetStatus(codes.Error, "validation error: missing CategoryName or PlatformId")
		return nil, err
	}

	// Successful completion logging
	log.EndTimeL(labels, "V2 CustomCategoryService.CheckCustomCategoryRequestData Finished", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return &customCategoryFields, nil
}

// BuildCustomCategoryRequestData constructs a CustomCategoryRequest struct with mapped fields
// based on the provided categoryName and customCategoryRequest parameters.
//
// Parameters:
//   - ctx: Context to manage the request's lifecycle and logging.
//   - categoryName: Name of the category, which will be trimmed and assigned to CategoryName in the new request.
//   - customCategoryRequest: Existing CustomCategoryRequest struct providing initial values.
//
// Returns:
//   - A populated CustomCategoryRequest struct with mapped fields and table names based on the input parameters.
func BuildCustomCategoryRequestData(ctx context.Context, categoryName string, customCategoryRequest datastruct.CustomCategoryRequest) datastruct.CustomCategoryRequest {
	span, labels := common.GenerateSpan("V2 CustomCategoryService.BuildCustomCategoryRequestData", ctx)
	defer span.End()
	span.AddEvent("Starting V2 CustomCategoryService.BuildCustomCategoryRequestData")
	startTime := log.StartTimeL(labels, "Starting V2 CustomCategoryService.BuildCustomCategoryRequestData")

	// Initialize and populate the new CustomCategoryRequest
	customCategoryFields := datastruct.CustomCategoryRequest{
		CategoryName: strings.TrimSpace(categoryName),
	}

	// Set table name and assets based on customCategoryRequest values
	if len(customCategoryRequest.Assets) > 0 {
		customCategoryFields.Assets = customCategoryRequest.Assets
		customCategoryFields.TableName = "fundamentalslatest"
	} else if customCategoryRequest.TableName != "" {
		// Map the table name based on provided category request values
		switch strings.TrimSpace(customCategoryRequest.TableName) {
		case "Assets":
			customCategoryFields.TableName = "fundamentalslatest"
		case "NFTs":
			customCategoryFields.TableName = "nftdatalatest"
		case "Category":
			customCategoryFields.TableName = "categories_fundamentals"
		}

		// Assign other request fields for sorting, limiting, and ordering
		customCategoryFields.Sort = customCategoryRequest.Sort
		customCategoryFields.Limit = customCategoryRequest.Limit
		customCategoryFields.OrderColumn = customCategoryRequest.OrderColumn

		// Apply condition fields if provided in customCategoryRequest
		if customCategoryRequest.ConditionColumn != "" && customCategoryRequest.ConditionSymbol != "" && customCategoryRequest.ConditionValue != "" {
			customCategoryFields.ConditionColumn = customCategoryRequest.ConditionColumn
			customCategoryFields.ConditionSymbol = mapConditionSymbol(customCategoryRequest.ConditionSymbol)
			customCategoryFields.ConditionValue = customCategoryRequest.ConditionValue
		}
	}

	// Log the end of the function and mark success in tracing
	log.EndTimeL(labels, "V2 CustomCategoryService.BuildCustomCategoryRequestData Finished", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return customCategoryFields
}

// mapConditionSymbol maps human-readable condition symbols to SQL operators.
func mapConditionSymbol(symbol string) string {
	switch symbol {
	case "equal":
		return "="
	case "not equal":
		return "!="
	case "less than":
		return "<"
	case "more than":
		return ">"
	case "less or equal":
		return "<="
	case "more or equal":
		return ">="
	default:
		return "" // Return an empty string for unsupported symbols
	}
}

// BuildCustomCategoriesStruct constructs a CustomCategory struct using the provided CustomCategoryRequest and result data.
// Parameters:
//   - ctx: Context to manage the request's lifecycle and logging.
//   - customCategoryFields: Pointer to a CustomCategoryRequest containing category request details.
//   - result: Interface containing the result data to be mapped to CategoryFields.
//
// Returns:
//   - Pointer to the constructed CustomCategory struct with populated fields based on the request data and table name.
func BuildCustomCategoriesStruct(ctx context.Context, customCategoryFields *datastruct.CustomCategoryRequest, result interface{}) *datastruct.CustomCategory {
	// Start tracing and logging
	span, labels := common.GenerateSpan("V2 CustomCategoryService.BuildCustomCategoriesStruct", ctx)
	defer span.End()
	span.AddEvent("Starting V2 CustomCategoryService.BuildCustomCategoriesStruct")
	startTime := log.StartTimeL(labels, "Starting V2 CustomCategoryService.BuildCustomCategoriesStruct")

	// Initialize and populate the new CustomCategory struct
	customCategory := &datastruct.CustomCategory{
		CategoryName:   customCategoryFields.CategoryName,
		CategoryFields: result,
	}

	// Determine the category type based on the table name specified in the CustomCategoryRequest
	switch customCategoryFields.TableName {
	case "fundamentalslatest":
		customCategory.CategoryType = "FT"
	case "nftdatalatest":
		customCategory.CategoryType = "NFT"
	case "categories_fundamentals":
		customCategory.CategoryType = "CATEGORY"
	default:
		customCategory.CategoryType = "UNKNOWN" // Fallback for unrecognized table names
	}

	// Log successful struct creation
	log.EndTimeL(labels, "V2 CustomCategoryService.BuildCustomCategoriesStruct Finished", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return customCategory
}

// GetCustomCategories retrieves all CustomCategory records that match the specified customCategoryType.
// Parameters:
//   - ctx: Context to manage request lifecycle and logging.
//   - customCategoryType: String indicating the category type to filter by (e.g., FT, NFT, CATEGORY).
//
// Returns:
//   - A slice of CustomCategory structs or an error if retrieval fails.
func (c *customCategoryService) GetCustomCategories(ctx context.Context, customCategoryType string) ([]datastruct.CustomCategory, error) {
	// Start tracing and logging
	span, labels := common.GenerateSpan("V2 CustomCategoryService.GetCustomCategories", ctx)
	defer span.End()
	span.AddEvent("Starting V2 CustomCategoryService.GetCustomCategories")
	startTime := log.StartTimeL(labels, "Starting V2 CustomCategoryService.GetCustomCategories")

	// Retrieve custom categories from the DAO layer based on the provided type
	customCategories, err := c.dao.NewCustomCategoryQuery().GetCustomCategories(ctx, customCategoryType)
	if err != nil {
		// Log the error and update the span status accordingly
		span.SetStatus(codes.Error, "Error retrieving custom categories")
		log.EndTime("V2 CustomCategoryService.GetCustomCategories", startTime, err)
		return nil, err
	}

	// Log successful completion
	log.EndTimeL(labels, "V2 CustomCategoryService.GetCustomCategories Finished", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return customCategories, nil
}
