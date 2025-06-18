package services

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/Forbes-Media/forbes-digital-assets/refactored/common"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/datastruct"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/repository"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/codes"
)

type CustomCategoryService interface {
	GetCustomFieldFromFS(ctx context.Context, customCategoryRequest datastruct.CustomCategoryRequest) (interface{}, error)
	GetCustomCategories(ctx context.Context, customCategoryType string) ([]datastruct.CustomCategory, error)
}

type customCategoryService struct {
	dao repository.DAO
}

func NewCustomCategoryService(dao repository.DAO) CustomCategoryService {
	return &customCategoryService{dao: dao}
}

func (c *customCategoryService) GetCustomFieldFromFS(ctx context.Context, customCategoryRequest datastruct.CustomCategoryRequest) (interface{}, error) {
	customCategoryFields, err := c.CheckCustomCategoryRequestData(ctx, customCategoryRequest)
	if err != nil {
		log.Error("%s", err)
		return "", err
	}
	queryMGR := c.dao.NewCustomCategoryQuery()

	query := queryMGR.BuildDynamicQuery(ctx, customCategoryFields)

	// todo build function to execute the query and return the result
	result, _ := queryMGR.FetchDataByTableName(ctx, query, *customCategoryFields)

	// todo build function to build the data we will insert it to PG

	data := BuildCustomCategoriesStruct(ctx, customCategoryFields, result)

	// todo build insert function to add the result to PG

	err = queryMGR.InsertCustomCategories(ctx, data)

	if err != nil {
		log.Error("%s", err)
		return nil, err
	}

	return nil, nil

}

func (c *customCategoryService) CheckCustomCategoryRequestData(ctx context.Context, customCategoryRequest datastruct.CustomCategoryRequest) (*datastruct.CustomCategoryRequest, error) {
	span, labels := common.GenerateSpan("V2 CustomCategoryService.CheckCustomCategoryRequestData", ctx)
	defer span.End()

	span.AddEvent(fmt.Sprintf("Starting %s", "V2 CustomCategoryService.CheckCustomCategoryRequestData"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 CustomCategoryService.CheckCustomCategoryRequestData"))

	var customCategoryFields datastruct.CustomCategoryRequest

	if customCategoryRequest.CategoryName != "" {
		customCategoryFields.CategoryName = strings.Trim(customCategoryRequest.CategoryName, "")
		if len(customCategoryRequest.Assets) > 0 {
			customCategoryFields.Assets = customCategoryRequest.Assets
			customCategoryFields.TableName = "fundamentalslatest"
			return &customCategoryFields, nil
		} else if customCategoryRequest.TableName != "" {
			customCategoryFields.TableName = strings.Trim(customCategoryRequest.TableName, "")
			switch customCategoryRequest.TableName {
			case "Assets":
				customCategoryFields.TableName = "fundamentalslatest"
			case "NFTs":
				customCategoryFields.TableName = "nftdatalatest"
			case "Category":
				customCategoryFields.TableName = "categories_fundamentals"
			}
			customCategoryFields.Sort = customCategoryRequest.Sort
			customCategoryFields.Limit = customCategoryRequest.Limit
			customCategoryFields.Column = customCategoryRequest.Column
			if customCategoryRequest.Condition != "" && customCategoryRequest.ConditionValue != "" {
				switch customCategoryRequest.Condition {
				case "equal":
					customCategoryFields.Condition = "="
				case "less than":
					customCategoryFields.Condition = "<"
				case "more than":
					customCategoryFields.Condition = ">"
				case "less or equal":
					customCategoryFields.Condition = "<="
				case "more or equal":
					customCategoryFields.Condition = ">="
				}
				customCategoryFields.ConditionValue = customCategoryRequest.ConditionValue
			}

		}
	} else if customCategoryRequest.PlatformId != "" {
		customCategoryFields.PlatformId = customCategoryRequest.PlatformId
		if len(customCategoryRequest.Assets) > 0 {
			customCategoryFields.Assets = customCategoryRequest.Assets
			customCategoryFields.TableName = "fundamentalslatest"
			return &customCategoryFields, nil
		} else if customCategoryRequest.TableName != "" {
			customCategoryFields.TableName = strings.Trim(customCategoryRequest.TableName, "")
			switch customCategoryRequest.TableName {
			case "Assets":
				customCategoryFields.TableName = "fundamentalslatest"
			case "NFTs":
				customCategoryFields.TableName = "nftdatalatest"
			case "Category":
				customCategoryFields.TableName = "categories_fundamentals"
			}
			customCategoryFields.Sort = customCategoryRequest.Sort
			customCategoryFields.Limit = customCategoryRequest.Limit
			customCategoryFields.Column = customCategoryRequest.Column
			if customCategoryRequest.Condition != "" && customCategoryRequest.ConditionValue != "" {
				switch customCategoryRequest.Condition {
				case "equal":
					customCategoryFields.Condition = "="
				case "less than":
					customCategoryFields.Condition = "<"
				case "more than":
					customCategoryFields.Condition = ">"
				case "less or equal":
					customCategoryFields.Condition = "<="
				case "more or equal":
					customCategoryFields.Condition = ">="
				}
				customCategoryFields.ConditionValue = customCategoryRequest.ConditionValue
			}

		}
	} else {
		return nil, errors.New("V2 CustomCategoryService.CheckCustomCategoryRequestData Request Data mismatch error")
	}

	log.EndTimeL(labels, "V2 CustomCategoryService.CheckCustomCategoryRequestData Finished", startTime, nil)
	span.SetStatus(codes.Ok, "V2 CustomCategoryService.CheckCustomCategoryRequestData Finished")
	return &customCategoryFields, nil

}

func BuildCustomCategoriesStruct(ctx context.Context, customCategoryFields *datastruct.CustomCategoryRequest, result interface{}) *datastruct.CustomCategory {
	span, labels := common.GenerateSpan("V2 CustomCategoryService.CheckCustomCategoryRequestData", ctx)
	defer span.End()

	span.AddEvent(fmt.Sprintf("Starting %s", "V2 CustomCategoryService.CheckCustomCategoryRequestData"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 CustomCategoryService.CheckCustomCategoryRequestData"))
	var customCategoryResult datastruct.CustomCategory
	if customCategoryFields.CategoryName != "" {
		customCategoryResult.CategoryName = customCategoryFields.CategoryName
	} else {
		customCategoryResult.CategoryName = customCategoryFields.PlatformId
	}
	customCategoryResult.CategoryFields = result
	switch customCategoryFields.TableName {
	case "fundamentalslatest":
		customCategoryResult.CategoryType = "FT"
	case "nftdatalatest":
		customCategoryResult.CategoryType = "NFT"
	case "categories_fundamentals":
		customCategoryResult.CategoryType = "CATEGORY"
	}

	log.EndTimeL(labels, "V2 CustomCategoryService.CheckCustomCategoryRequestData Finished", startTime, nil)
	span.SetStatus(codes.Ok, "V2 CustomCategoryService.CheckCustomCategoryRequestData Finished")
	return &customCategoryResult
}

func (c *customCategoryService) GetCustomCategories(ctx context.Context, customCategoryType string) ([]datastruct.CustomCategory, error) {
	span, labels := common.GenerateSpan("GetCustomCategories ", ctx)
	defer span.End()

	span.AddEvent(fmt.Sprintf("Starting %s", "GetCustomCategories"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "GetCustomCategories"))

	customCategories, err := c.dao.NewCustomCategoryQuery().GetCustomCategories(ctx, customCategoryType)
	if err != nil {
		span.SetStatus(codes.Error, "GetCustomCategories()")
		log.EndTime("GetCustomCategories", startTime, err)
		return nil, err
	}

	log.EndTimeL(labels, fmt.Sprintf("finished %s", "GetCustomCategories"), startTime, nil)
	span.SetStatus(codes.Ok, fmt.Sprintf("Starting %s", "GetCustomCategories"))
	return customCategories, nil

}
