// cacheService.go

// Fuzzy search for the given searchTerm in the given words. Returns the assets related to the search term.
func (c *categoriesTableCacheService) fuzzySearch(ctx0 context.Context, searchTerm string, words *[]string, assets *map[string][]datastruct.CategoryFundamental) []datastruct.CategoryFundamental {
	_, span := tracer.Start(ctx0, "fuzzySearch")
	defer span.End()

	var result []datastruct.CategoryFundamental
	ranks := fuzzy.RankFindNormalized(searchTerm, *words) // case-insensitive & unicode-normalized fuzzy search.
	sort.Sort(ranks)                                      // sorts by the Levenshtein distance
	for rankIdx, rank := range ranks {
		if rankIdx >= c.DefaultFuzzySearchLimit {
			break
		}
		result = append(result, (*assets)[rank.Target]...)
	}

	span.SetStatus(codes.Ok, "success")
	return result
}


// the sort function will sort the data after we filter it with regex
// it will sort data by ASC order for all data
func cryptoSortFunctionality(assets []datastruct.CategoryFundamental) {
	sort.Slice(assets, func(i, j int) bool {
		var res = j > i
		res = strings.ToLower(assets[i].Name) < strings.ToLower(assets[j].Name)
		return res
	})
}


// Special Sort Function we will use it only if we need to sort by Crypto Names
func paginateSortCryptoByNames(ctx context.Context, paginate dto.Paginate, assets []datastruct.CategoryFundamental) []datastruct.CategoryFundamental {
	span, labels := common.GenerateSpan("V2 categoriesCache.paginateSortCryptoByNames", ctx)
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 categoriesCache.paginateSortCryptoByNames"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 categoriesCache.paginateSortCryptoByNames"))
	defer span.End()

	// build array of all cases that exist in Crypto name
	specialNames, numberNames, normalNames := buildCryptoDataUsingRegexFilter(assets)

	// Sort Names Start with Special Characters
	cryptoSortFunctionality(specialNames)
	// Sort Names start with numeric Characters
	cryptoSortFunctionality(numberNames)
	// Sort Names start with Alphabetic Characters
	cryptoSortFunctionality(normalNames)

	// Build Crypto response Sorted by name
	// If the sort ASC the result will return in this order --> normalName --> numericName --> specialName
	// If the sort DESC the result will return in this order --> numericName --> normalName --> specialName
	if paginate.Direction == "asc" {
		// It will sort data using ASC like: a-z --> 0-9 --> special
		assets = buildOrderedCryptoPrices(normalNames, numberNames, specialNames)
	} else if paginate.Direction == "desc" {
		// It will sort data using DESC like: 0-9 --> a-z --> special
		assets = buildOrderedCryptoPrices(numberNames, normalNames, specialNames)
	}
	log.EndTimeL(labels, "V2 categoriesCache.PaginateSortAssets", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return assets
}

// Use this function to build the Crypto for all names after the filter and the sort functions
// it will return an array of Crypto that ordered as we need
// Asc order will return ==> normalName --> numericName --> specialName
// Desc order will return ==> numericName --> normalName --> specialName
func buildOrderedCryptoPrices(normalNames []datastruct.CategoryFundamental, numericNames []datastruct.CategoryFundamental, specialNames []datastruct.CategoryFundamental) []datastruct.CategoryFundamental {
	var combinedPrices []datastruct.CategoryFundamental
	combinedPrices = append(combinedPrices, normalNames...)
	combinedPrices = append(combinedPrices, numericNames...)
	combinedPrices = append(combinedPrices, specialNames...)
	return combinedPrices
}

// Build the array of names that match the regex
// use regex to build the data array that match each filter.
func buildCryptoDataUsingRegexFilter(assets []datastruct.CategoryFundamental) ([]datastruct.CategoryFundamental, []datastruct.CategoryFundamental, []datastruct.CategoryFundamental) {
	var specialNames []datastruct.CategoryFundamental
	var numberNames []datastruct.CategoryFundamental
	var normalNames []datastruct.CategoryFundamental
	specialPattern := `/[^\s\w ]/`
	numberPattern := `^[0-9]`
	charPattern := `^[a-zA-Z]`

	// Create a regular expression object
	specialRegex, err := regexp.Compile(specialPattern)
	if err != nil {
		fmt.Println("Error compiling regex for Special Characters:", err)
		return nil, nil, nil
	}
	numberRegex, err := regexp.Compile(numberPattern)
	if err != nil {
		fmt.Println("Error compiling regex for numeric Characters:", err)
		return nil, nil, nil
	}
	charRegex, err := regexp.Compile(charPattern)
	if err != nil {
		fmt.Println("Error compiling regex for Normal Characters:", err)
		return nil, nil, nil
	}

	// check if the NFT name match the regex condition then append it to Crypto Price Array.
	for _, asset := range assets {
		name := asset.Name
		if specialRegex.MatchString(name) {
			specialNames = append(specialNames, asset)
		} else if numberRegex.MatchString(name) {
			numberNames = append(numberNames, asset)
		} else if charRegex.MatchString(name) {
			normalNames = append(normalNames, asset)
		} else {
			specialNames = append(specialNames, asset)
		}
	}

	return specialNames, numberNames, normalNames
}




