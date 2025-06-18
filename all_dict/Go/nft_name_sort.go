// line 112
if paginate.SortBy == "name" {
	nfts = PaginateSortNFTsByNames(ctx0, &nfts, paginate, len(exactMatchNFTs))
} else {
	nfts = PaginateSortNFTs(ctx0, &nfts, paginate, len(exactMatchNFTs))
}





/*
	asc 0-9 then a-z then the CHARACTER
	desc a-z then 0-9 then the CHARACTER
	desc z-a then 9-0 then the CHARACTER
	OR
	dec CHARACTER a-z then 0-9
*/

// Special Sort Function we will use it only if we need to sort by  NFTs Names
func PaginateSortNFTsByNames(ctx0 context.Context, allNFTs *[]NFTPrices, paginate Paginate, ignoreInitialNFTs int) []NFTPrices {
	_, span := tracer.Start(ctx0, "PaginateSortNFTs")
	defer span.End()

	validatePaginate(ctx0, &paginate) // validate the paginate object
	if len(*allNFTs) == 0 {
		return *allNFTs
	}
	initialNFTs := (*allNFTs)[:ignoreInitialNFTs]
	nfts := (*allNFTs)[ignoreInitialNFTs:]

	// build array of all cases that exist in nfts name
	specialNames, numberNames, charNames := BuildRegexFilter(nfts)

	// Sort Names Start with Special Characters
	SortFunctionality(specialNames, paginate)
	// Sort Names start with numeric Characters
	SortFunctionality(numberNames, paginate)
	// Sort Names start with Alphabetic Characters
	SortFunctionality(charNames, paginate)

	// Build NFTs response Sorted by name
	// If the sort ASC the result will return in this order --> numericChar --> normalChar --> specialChar
	// If the sort DESC the result will return in this order --> normalChar --> numericChar --> specialChar
	if paginate.Direction == "asc" {
		// case 1
		// asc: a-z --> 0-9 --> special
		// desc: 9-0 --> z-a --> special
		// nfts = AppendNFTs(initialNFTs, charNames, numberNames, specialNames)

		// case 2
		// 0-9 --> a-z --> special
		nfts = AppendNFTs(initialNFTs, numberNames, charNames, specialNames)
	} else {
		// case 1
		// 9-0 --> z-a --> special
		// nfts = AppendNFTs(initialNFTs, numberNames, charNames, specialNames)

		// case 2
		// a-z --> 0-9 --> special
		nfts = AppendNFTs(initialNFTs, charNames, numberNames, specialNames)
	}

	start := (paginate.PageNum - 1) * paginate.Limit // paginate the nfts
	end := start + paginate.Limit
	if start > len(nfts) {
		return []NFTPrices{}
	}
	if end > len(nfts) {
		end = len(nfts)
	}
	span.SetStatus(codes.Ok, "success")
	return nfts[start:end]
}

// the sort function will sort the data after we filter it with regex
func SortFunctionality(nfts []NFTPrices, paginate Paginate) {
	sort.Slice(nfts, func(i, j int) bool {
		var res = j > i
		// if paginate.Direction == "asc" {
		res = strings.ToLower(nfts[i].Name) < strings.ToLower(nfts[j].Name)
		// } else {
		// 	res = strings.ToLower(nfts[i].Name) > strings.ToLower(nfts[j].Name)
		// }
		return res
	})
}

// Use this function to build the nfts for all names after the filter and the sort functions
// it will return an array of nfts that ordered as we need
// Asc order will return ==> numericChar --> normalChar --> specialChar
// Desc order will return ==> normalChar --> numericChar --> specialChar
func AppendNFTs(initialNFTs []NFTPrices, normalChar []NFTPrices, numericChar []NFTPrices, specialChar []NFTPrices) []NFTPrices {
	var nfts []NFTPrices
	nfts = append(initialNFTs, normalChar...)
	nfts = append(nfts, numericChar...)
	nfts = append(nfts, specialChar...)
	return nfts
}

// Build the array of names that match the regex
func BuildRegexFilter(nfts []NFTPrices) ([]NFTPrices, []NFTPrices, []NFTPrices) {
	var specialNames []NFTPrices
	var numberNames []NFTPrices
	var charNames []NFTPrices
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

	for _, nft := range nfts {
		name := nft.Name
		if specialRegex.MatchString(name) {
			specialNames = append(specialNames, nft)
		} else if numberRegex.MatchString(name) {
			numberNames = append(numberNames, nft)
		} else if charRegex.MatchString(name) {
			charNames = append(charNames, nft)
		} else {
			specialNames = append(specialNames, nft)
		}
	}

	return specialNames, numberNames, charNames
}
