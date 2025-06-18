package services

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/Forbes-Media/forbes-digital-assets/refactored/common"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/datastruct"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/dto"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/repository"
	"github.com/Forbes-Media/forbes-digital-assets/store"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/codes"
)

type CustomCategoryService interface {
	// Builds custom category data from FS based on the provided request parameters.
	BuildCustomCategoriesData(ctx context.Context, customCategoryRequest datastruct.CustomCategoryRequest) error

	// Retrieves custom category data from PostgreSQL based on the specified category type (e.g., FT, NFT, CATEGORY).
	GetCustomCategories(ctx context.Context, customCategoryType string) ([]datastruct.CustomCategory, error)

	// Update all custom categories data from FS based on the provided request parameters.
	BuildCustomCategoriesDataFS(ctx context.Context) (map[string]store.CategoriesData, error)
}

type customCategoryService struct {
	dao repository.DAO
}

func NewCustomCategoryService(dao repository.DAO) CustomCategoryService {
	return &customCategoryService{dao: dao}
}

// BuildCustomCategoriesDataFS builds a map of custom category data with all necessary fields
// to be added to the Categories Fundamentals table.
// Parameters:
//   - ctx: Context to manage the request's lifecycle and logging.
//
// Returns:
//   - map[string]store.CategoriesData: A map containing the constructed categories data keyed by category slug.
//   - error: Returns an error if any step in the process fails; otherwise, nil.
//
// Details:
//   - Retrieves active and inactive custom categories from the database.
//   - Processes each active category to build the category data.
//   - Deletes records of inactive categories from relevant database tables if any exist.
//   - Retrieves custom categories marked as "ft" and maps them to `CategoriesData` structs.
//   - Logs and traces the operation's success or failure.
func (c *customCategoryService) BuildCustomCategoriesDataFS(ctx context.Context) (map[string]store.CategoriesData, error) {
	span, labels := common.GenerateSpan("V2 CustomCategoryService.BuildCustomCategoriesDataFS", ctx)
	defer span.End()
	span.AddEvent("Starting V2 CustomCategoryService.BuildCustomCategoriesDataFS")

	// Start a log timer to measure the duration of the operation.
	startTime := log.StartTimeL(labels, "Starting V2 CustomCategoryService.BuildCustomCategoriesDataFS")

	// Create a query manager instance for database operations.
	queryManager := c.dao.NewCustomCategoryQuery()

	// Fetch active custom categories from the FS.
	customCategoryRequests, err := queryManager.GetCustomCategoriesDataFS(ctx)
	if err != nil {
		// Log and return error if fetching active custom categories fails.
		log.Error("Validation failed in BuildCustomCategoriesDataFS: %s", err)
		span.SetStatus(codes.Error, "Validation error in BuildCustomCategoriesDataFS")
		log.EndTimeL(labels, "V2 CustomCategoryService.BuildCustomCategoriesDataFS Finished with Error", startTime, err)
		return nil, err
	}

	// Fetch inactive custom categories from the FS.
	inactiveCustomCategoryRequests, err := queryManager.GetInActiveCustomCategoriesDataFS(ctx)
	if err != nil {
		// Log and return error if fetching inactive custom categories fails.
		log.Error("Validation failed in BuildCustomCategoriesDataFS: %s", err)
		span.SetStatus(codes.Error, "Validation error in BuildCustomCategoriesDataFS")
		log.EndTimeL(labels, "V2 CustomCategoryService.BuildCustomCategoriesDataFS Finished with Error", startTime, err)
		return nil, err
	}

	// Initialize a map to hold the constructed category data.
	customCategories := make(map[string]store.CategoriesData)

	// Process each active custom category to build the category data.
	for _, customCategoryRequest := range customCategoryRequests {
		err := c.BuildCustomCategoriesData(ctx, customCategoryRequest)
		if err != nil {
			// Log and return error if category data construction fails.
			log.Error("Validation failed in BuildCustomCategoriesDataFS: %s", err)
			span.SetStatus(codes.Error, "Validation error in BuildCustomCategoriesDataFS")
			log.EndTimeL(labels, "V2 CustomCategoryService.BuildCustomCategoriesDataFS Finished with Error", startTime, err)
			return nil, err
		}
	}

	// If inactive categories exist, delete them from relevant database tables (Custom Categories, Categories Fundamentals).
	if len(inactiveCustomCategoryRequests) > 0 {
		err = c.DeleteCustomCategories(ctx, inactiveCustomCategoryRequests)
		if err != nil {
			// Log and handle deletion errors but continue the process.
			log.Error("Deletion failed in DeleteCustomCategories: %s", err)
			span.SetStatus(codes.Error, "Deletion error in DeleteCustomCategories")
			log.EndTimeL(labels, "V2 CustomCategoryService.BuildCustomCategoriesDataFS Finished with Error", startTime, err)
		}
	}

	// Fetch custom categories marked as "ft" from the database.
	categories, err := c.GetCustomCategories(ctx, "ft")
	if err != nil {
		// Log and return error if fetching "ft" categories fails.
		log.Error("Validation failed in BuildCustomCategoriesDataFS: %s", err)
		span.SetStatus(codes.Error, "Validation error in BuildCustomCategoriesDataFS")
		log.EndTimeL(labels, "V2 CustomCategoryService.BuildCustomCategoriesDataFS Finished with Error", startTime, err)
		return nil, err
	}

	// Map custom categories to CategoriesData structs for use with Categories Fundamentals.
	for _, category := range categories {
		CategoryData := store.CategoriesData{
			ID:            category.CategorySlug,
			Name:          category.CategoryName,
			Inactive:      category.InActive,
			IsHighlighted: category.IsHighlighted,
		}

		// Populate market details for each category.
		for _, asset := range category.Markets {
			CategoryData.Markets = append(CategoryData.Markets, store.CoinsMarketResult{
				ID:   asset.Symbol,
				Name: asset.Name,
			})
		}

		// Add the constructed data to the category map.
		customCategories[category.CategorySlug] = CategoryData
	}

	// Log the successful completion of the operation.
	log.EndTimeL(labels, "V2 CustomCategoryService.BuildCustomCategoriesDataFS Finished", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	var allFundamentals []store.CategoryFundamental
	var categoryMap = make(map[string]store.CategoriesData)

	rowyCategories, err := store.GetFeaturedCategoriesMap(ctx)
	if err != nil {
		log.ErrorL(labels, "Error BuildCategoriesFundamentals %s", err)
		span.SetStatus(codes.Error, "Category Fundamentals Building failed due to rowy category lookup")
	}

	for name, marketsData := range customCategories {
		categoryMap[name] = marketsData
	}

	//Build Newest category fundamentals
	newestAssets, err := store.GetNewestFundamentals(context.Background())
	if err != nil {
		fmt.Println("something went wrong")
	}

	newTokens := store.CategoriesData{ID: "new-cryptocurrencies", Name: "New Crypto Currencies", Inactive: false}

	for _, asset := range *newestAssets {
		newTokens.Markets = append(newTokens.Markets, store.CoinsMarketResult{
			ID:   asset.Symbol,
			Name: asset.Name,
		})
	}

	categoryMap["new-cryptocurrencies"] = newTokens

	for _, category := range categoryMap {
		var categoryFundamental store.CategoryFundamental
		categoryFundamental.ID = category.ID
		categoryFundamental.Name = category.Name
		categoryFundamental.Inactive = category.Inactive

		rowyCategory, exists := rowyCategories[category.ID]
		//Check to see if the category exists in rowy
		if exists {
			//if it does pull user input columns and assign them to the category fundamental
			categoryFundamental.ForbesID = rowyCategory.ForbesId
			categoryFundamental.ForbesName = rowyCategory.ForbesName
			if category.IsHighlighted {
				categoryFundamental.IsHighlighted = category.IsHighlighted
				rowyCategory.Link = fmt.Sprintf("/highlights/%s/", categoryFundamental.ID)
			} else {
				categoryFundamental.IsHighlighted = rowyCategory.IsHighlighted
				rowyCategory.Link = fmt.Sprintf("/categories/%s/", categoryFundamental.ID)
			}
			//assign the rowy link column based on the id if there is a forbes id build a link using that instead ()
			// if rowyCategory.IsHighlighted {
			// 	rowyCategory.Link = fmt.Sprintf("/highlights/%s/", categoryFundamental.ID)
			// } else {
			// 	rowyCategory.Link = fmt.Sprintf("/categories/%s/", categoryFundamental.ID)
			// }
			if rowyCategory.ForbesId != "" {
				rowyCategory.Link = fmt.Sprintf("/categories/%s/", categoryFundamental.ForbesID)
				categoryFundamental.Slug = rowyCategory.ForbesId
				rowyCategories[categoryFundamental.ID] = rowyCategory
			}
		} else if !categoryFundamental.Inactive {
			//if the category does not exist create a new entry in rowy. This way updates can be made by the seo team
			var newCat store.FeaturedCategory
			if categoryFundamental.IsHighlighted {
				newCat = store.FeaturedCategory{ID: categoryFundamental.ID, Name: categoryFundamental.Name, Link: fmt.Sprintf("/highlights/%s/", categoryFundamental.ID), IsHighlighted: categoryFundamental.IsHighlighted}
			} else {
				newCat = store.FeaturedCategory{ID: categoryFundamental.ID, Name: categoryFundamental.Name, Link: fmt.Sprintf("/categories/%s/", categoryFundamental.ID), IsHighlighted: categoryFundamental.IsHighlighted}

			}
			rowyCategories[categoryFundamental.ID] = newCat
		}
		allFundamentals = append(allFundamentals, categoryFundamental)
	}
	file, _ := json.MarshalIndent(allFundamentals, "", " ")
	_ = os.WriteFile("fundamentals.json", file, 0644)
	return customCategories, nil
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
		log.Error("V2 CustomCategoryService.BuildCustomCategoriesData Validation failed in CheckCustomCategoryRequestData: %s", err)
		span.SetStatus(codes.Error, "V2 CustomCategoryService.BuildCustomCategoriesData Validation error in CheckCustomCategoryRequestData")
		log.EndTimeL(labels, "V2 CustomCategoryService.BuildCustomCategoriesData Finished with Error", startTime, err)
		return err
	}

	// Step 2: Build a dynamic query based on validated custom category fields
	queryManager := c.dao.NewCustomCategoryQuery()
	query := queryManager.BuildDynamicQuery(ctx, customCategoryFields)

	// Step 3: Fetch data using the generated dynamic query
	result, err := queryManager.FetchDataByTableName(ctx, query, *customCategoryFields)
	if err != nil {
		log.Error("V2 CustomCategoryService.BuildCustomCategoriesData Data fetching failed in FetchDataByTableName: %s", err)
		span.SetStatus(codes.Error, "V2 CustomCategoryService.BuildCustomCategoriesData Data fetching error in FetchDataByTableName")
		log.EndTimeL(labels, "V2 CustomCategoryService.BuildCustomCategoriesData Finished with Error", startTime, err)
		return err
	}

	// Step 4: Build a CustomCategory struct with the fetched data
	data := BuildCustomCategoriesStruct(ctx, customCategoryFields, result)

	// Step 5: Insert the structured custom category data into the PostgreSQL table
	err = queryManager.InsertCustomCategories(ctx, data)
	if err != nil {
		log.Error("V2 CustomCategoryService.BuildCustomCategoriesData Data insertion failed in InsertCustomCategories: %s", err)
		span.SetStatus(codes.Error, "V2 CustomCategoryService.BuildCustomCategoriesData Data insertion error in InsertCustomCategories")
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
	if !customCategoryRequest.IsActive {
		// Log the error and set span status before returning
		err := errors.New("missing Custom Category Is not active to be build CheckCustomCategoryRequestData")
		log.EndTimeL(labels, "V2 CustomCategoryService.CheckCustomCategoryRequestData Finished with Error", startTime, err)
		span.SetStatus(codes.Error, "validation error: Not active Category")
		return nil, err
	} else if customCategoryRequest.CategoryName != "" {
		customCategoryFields = BuildCustomCategoryRequestData(ctx, customCategoryRequest.CategoryName, customCategoryRequest)
	} else if customCategoryRequest.PlatformId[0].Name != "" {
		customCategoryFields = BuildCustomCategoryRequestData(ctx, customCategoryRequest.PlatformId[0].Name, customCategoryRequest)
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
		CategoryName:  strings.TrimSpace(categoryName),
		Path:          customCategoryRequest.Path,
		IsHighlighted: customCategoryRequest.IsHighlighted,
		IsActive:      customCategoryRequest.IsActive,
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
//   - result: byte containing the result data to be mapped to CategoryFields.
//
// Returns:
//   - Pointer to the constructed CustomCategory struct with populated fields based on the request data and table name.
func BuildCustomCategoriesStruct(ctx context.Context, customCategoryFields *datastruct.CustomCategoryRequest, result []byte) *datastruct.CustomCategory {
	// Start tracing and logging
	span, labels := common.GenerateSpan("V2 CustomCategoryService.BuildCustomCategoriesStruct", ctx)
	defer span.End()
	span.AddEvent("Starting V2 CustomCategoryService.BuildCustomCategoriesStruct")
	startTime := log.StartTimeL(labels, "Starting V2 CustomCategoryService.BuildCustomCategoriesStruct")
	slug := strings.ReplaceAll(strings.ToLower(customCategoryFields.CategoryName), " ", "-")
	// Initialize and populate the new CustomCategory struct
	customCategory := &datastruct.CustomCategory{
		CategoryName:   customCategoryFields.CategoryName,
		CategorySlug:   slug,
		CategoryPath:   fmt.Sprintf("/%s/%s/", strings.ToLower(customCategoryFields.Path), slug),
		CategoryFields: result,
		IsHighlighted:  customCategoryFields.IsHighlighted,
		InActive:       customCategoryFields.IsActive,
	}

	// Determine the category type based on the table name specified in the CustomCategoryRequest
	switch customCategoryFields.TableName {
	case "fundamentalslatest":
		customCategory.CategoryType = string(dto.Ft)
	case "nftdatalatest":
		customCategory.CategoryType = string(dto.Nft)
	case "categories_fundamentals":
		customCategory.CategoryType = string(dto.Category)
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

	var customCategories []datastruct.CustomCategory
	// Retrieve custom categories from the DAO layer based on the provided type
	result, err := c.dao.NewCustomCategoryQuery().GetCustomCategories(ctx, customCategoryType)
	if err != nil {
		// Log the error and update the span status accordingly
		span.SetStatus(codes.Error, "Error retrieving custom categories")
		log.EndTime("V2 CustomCategoryService.GetCustomCategories", startTime, err)
		return nil, err
	}

	for _, category := range result {
		switch dto.DictionaryDataSet(category.CategoryType) {
		case dto.Ft:
			err := json.Unmarshal(category.CategoryFields, &category.Markets)
			if err != nil {
				// Log and return error if JSON unmarshaling fails
				span.SetStatus(codes.Error, "Error unmarshaling Markets JSON in GetCustomCategories")
				log.EndTime("GetCustomCategories", startTime, err)
				return nil, err
			}
		case dto.Nft:
			err := json.Unmarshal(category.CategoryFields, &category.NFTs)
			if err != nil {
				// Log and return error if JSON unmarshaling fails
				span.SetStatus(codes.Error, "Error unmarshaling NFTs JSON in GetCustomCategories")
				log.EndTime("GetCustomCategories", startTime, err)
				return nil, err
			}
		case dto.Category:
			err := json.Unmarshal(category.CategoryFields, &category.Categories)
			if err != nil {
				// Log and return error if JSON unmarshaling fails
				span.SetStatus(codes.Error, "Error unmarshaling Categories JSON in GetCustomCategories")
				log.EndTime("GetCustomCategories", startTime, err)
				return nil, err
			}
		}
		category.CategoryFields = nil
		customCategories = append(customCategories, category)
	}

	// Log successful completion
	log.EndTimeL(labels, "V2 CustomCategoryService.GetCustomCategories Finished", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return customCategories, nil
}

// DeleteCustomCategories deletes inactive custom categories based on the provided requests.
// Parameters:
//   - ctx: Context to manage the request's lifecycle and logging.
//   - inactiveCustomCategoryRequests: Slice of InactiveCustomCategoryRequest containing details of the categories to be deleted.
//
// Returns:
//   - error: Returns an error if the deletion fails; otherwise, nil.
//
// Details:
//   - It processes the input requests to extract category names or platform names for deletion.
//   - It then calls the DeleteInactiveCustomCategories to perform the deletion of inactive custom categories.
func (c *customCategoryService) DeleteCustomCategories(ctx context.Context, inactiveCustomCategoryRequests []datastruct.CustomCategoryRequest) error {
	// Start tracing and logging
	span, labels := common.GenerateSpan("V2 CustomCategoryService.DeleteCustomCategories", ctx)
	defer span.End()
	span.AddEvent("Starting V2 CustomCategoryService.DeleteCustomCategories")
	startTime := log.StartTimeL(labels, "Starting V2 CustomCategoryService.DeleteCustomCategories")

	var inactiveCategory []string
	for _, category := range inactiveCustomCategoryRequests {
		if category.CategoryName != "" {
			inactiveCategory = append(inactiveCategory, fmt.Sprintf(`'%s'`, category.CategoryName))
		} else if category.PlatformId[0].Name != "" {
			inactiveCategory = append(inactiveCategory, fmt.Sprintf(`'%s'`, category.PlatformId[0].Name))
		}
	}

	// Retrieve custom categories from the DAO layer based on the provided type
	err := c.dao.NewCustomCategoryQuery().DeleteInactiveCustomCategories(ctx, inactiveCategory)
	if err != nil {
		// Log the error and update the span status accordingly
		span.SetStatus(codes.Error, "Error deleting custom categories")
		log.EndTime("V2 CustomCategoryService.DeleteCustomCategories", startTime, err)
		return err
	}

	// Log successful completion
	log.EndTimeL(labels, "V2 CustomCategoryService.DeleteCustomCategories Finished", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return nil
}
