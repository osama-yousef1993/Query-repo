func (c *categoriesTableCacheService) BuildCategorySearchTermResponse(ctx context.Context, searchTerm string, paginate dto.Paginate) *dto.SearchResponse {
	span, labels := common.GenerateSpan("V2 categoriesCache.SearchTerm", ctx)
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 categoriesCache.SearchTerm"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 categoriesCache.SearchTerm"))
	defer span.End()
	var returnData = dto.SearchResponse{}
	searchTerm = strings.ToLower(searchTerm)
	var assets []datastruct.CategoryFundamental
	exactMatchAssets, isID := c.CategoriesById[searchTerm] // Check the categoriesid map to see if the search term passed in from the user is is the unique id of eligible categories.
	if isID {                                              // if the searchterm is a match we extract the value of the key and continue
		assets = append(exactMatchAssets, assets...)
	} else {
		//if we dont have a match by id we check to see if there is an exat match by asset name ex: if searchterm= "layer 1 (l1)" returns the category info layer-1
		exactMatchAssets, isExact := c.CategoriesTable[searchTerm]
		if isExact { //if we have an exact match return the matched data
			assets = append(exactMatchAssets, assets...)
		} else if searchTerm == "" { //if empty string passed while searching, we return all the assets.
			for _, assetList := range c.CategoriesTable {
				assets = append(assets, assetList...)
			}
		} else if c.SearchType == dto.Fuzzy { // If we dont have an exact match do a fuzzy search
			assets = fuzzySearch[datastruct.CategoryFundamental](ctx, searchTerm, &c.Words, &c.CategoriesTable, c.DefaultFuzzySearchLimit)
		}
	}

	totalAssets := len(assets)
	//if no data was found return
	if totalAssets <= 0 {
		err := errors.New("V2 categoriesCache.SearchTerm Failed to Assert Type []datastruct.CategoryFundamental ")
		span.SetStatus(codes.Error, err.Error())
		log.ErrorL(labels, err.Error())
		log.EndTimeL(labels, "V2 categoriesCache.SearchTerm", startTime, err)
		return &returnData
	}
	sortedtResults := c.PaginateSortAssets(ctx, &assets, paginate, 0)

	categoryFundamentals, ok := sortedtResults.([]datastruct.CategoryFundamental)
	if !ok {
		err := errors.New("V2 categoriesCache.SearchTerm Failed to Assert Type []datastruct.CategoryFundamental ")
		span.SetStatus(codes.Error, err.Error())
		log.ErrorL(labels, err.Error())
		log.EndTimeL(labels, "V2 categoriesCache.SearchTerm", startTime, err)
		return &returnData
	}

	/*
		We need the response from search, exactly the same response from Traded Assets endpoint.
		- Source : for the data (Coingecko)
		- Total : the number of Assets that return after the search term
		- Assets : the data to be displayED on the page.
	*/
	returnData = dto.SearchResponse{Source: c.Datasource, Total: totalAssets, Categories: &categoryFundamentals}
	log.EndTimeL(labels, "V2 AssetsService.SearchTerm", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return &returnData
}