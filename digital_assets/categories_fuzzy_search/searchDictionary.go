	var exactMatchAssets []TradedAssetsTable
	var isExact bool
	// If there's exact match for the category ID in the dictionary, we will use fuzzy search for the search term in this category, and return the assets.
	if exactMatchFound {
		if searchTerm == "" {
			assetsResult = categoryAssets
		} else {
			exactMatchAssets, isExact = assets[searchTerm] //Assets that directly match the search term.
			exactMatchAssets = RemoveDuplicateInactiveAssets(ctx0, exactMatchAssets)
			if isExact {
				assetsResult = append(exactMatchAssets, assetsResult...) // append the direct match assets to the assets array in the front.
			} else {
				assetsResult = fuzzySearch(ctx0, searchTerm, &dictionary.words, &assets)
			}
		}
	}
