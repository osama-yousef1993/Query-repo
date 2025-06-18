func PaginateSortNFTsByNames(ctx0 context.Context, allNFTs *[]NFTPrices, paginate Paginate, ignoreInitialNFTs int) []NFTPrices {
	_, span := tracer.Start(ctx0, "PaginateSortNFTs")
	defer span.End()

	validatePaginate(ctx0, &paginate) // validate the paginate object
	if len(*allNFTs) == 0 {
		return *allNFTs
	}
	initialNFTs := (*allNFTs)[:ignoreInitialNFTs]
	nfts := (*allNFTs)[ignoreInitialNFTs:]

	// var specialNames []NFTPrices
	// var numberNames []NFTPrices
	// var charNames []NFTPrices
	// // specialPattern := `^[!@#$%^&*()_+{}\[\]:;<>,.?~\\/\|\-✖𝚃\- ]`
	// specialPattern := `/[^\s\w ]/`
	// numberPattern := `^[0-9]`
	// charPattern := `^[a-zA-Z]`

	// // Create a regular expression object
	// specialRegex, err := regexp.Compile(specialPattern)
	// if err != nil {
	// 	fmt.Println("Error compiling regex for Special Characters:", err)
	// 	return nil
	// }
	// numberRegex, err := regexp.Compile(numberPattern)
	// if err != nil {
	// 	fmt.Println("Error compiling regex for numeric Characters:", err)
	// 	return nil
	// }
	// charRegex, err := regexp.Compile(charPattern)
	// if err != nil {
	// 	fmt.Println("Error compiling regex for Normal Characters:", err)
	// 	return nil
	// }

	// for _, nft := range nfts {
	// 	name := nft.Name
	// 	if specialRegex.MatchString(name) {
	// 		specialNames = append(specialNames, nft)
	// 	} else if numberRegex.MatchString(name) {
	// 		numberNames = append(numberNames, nft)
	// 	} else if charRegex.MatchString(name) {
	// 		charNames = append(charNames, nft)
	// 	} else {
	// 		specialNames = append(specialNames, nft)
	// 	}
	// }

	specialNames, numberNames, charNames := BuildRegexFilter(nfts)

	SortFunctionality(specialNames, paginate)
	SortFunctionality(numberNames, paginate)
	SortFunctionality(charNames, paginate)

	// sort.Slice(specialNames, func(i, j int) bool {
	// 	var res = j > i
	// 	if paginate.Direction == "asc" {
	// 		res = strings.ToLower(specialNames[i].Name) < strings.ToLower(specialNames[j].Name)
	// 	} else {
	// 		res = strings.ToLower(specialNames[i].Name) > strings.ToLower(specialNames[j].Name)
	// 	}
	// 	return res
	// })
	// sort.Slice(numberNames, func(i, j int) bool {
	// 	var res = j > i
	// 	if paginate.Direction == "asc" {
	// 		res = strings.ToLower(numberNames[i].Name) < strings.ToLower(numberNames[j].Name)
	// 	} else {
	// 		res = strings.ToLower(numberNames[i].Name) > strings.ToLower(numberNames[j].Name)
	// 	}
	// 	return res
	// })
	// sort.Slice(charNames, func(i, j int) bool {
	// 	var res = j > i
	// 	if paginate.Direction == "asc" {
	// 		res = strings.ToLower(charNames[i].Name) < strings.ToLower(charNames[j].Name)
	// 	} else {
	// 		res = strings.ToLower(charNames[i].Name) > strings.ToLower(charNames[j].Name)
	// 	}
	// 	return res
	// })

	// file, _ := json.MarshalIndent(specialNames, " ", "")
	// _ = os.WriteFile("specialNames.json", file, 0644)
	// file1, _ := json.MarshalIndent(numberNames, " ", "")
	// _ = os.WriteFile("numberNames.json", file1, 0644)
	// file2, _ := json.MarshalIndent(charNames, " ", "")
	// _ = os.WriteFile("charNames.json", file2, 0644)

	if paginate.Direction == "asc" {
		nfts = AppendNFTs(initialNFTs, charNames, numberNames, specialNames)
	} else {
		nfts = AppendNFTs(initialNFTs, numberNames, charNames, specialNames)
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
	file2, _ := json.MarshalIndent(nfts[start:end], " ", "")
	_ = os.WriteFile("nfts.json", file2, 0644)
	return nfts[start:end]
}

// the sort function will sort the data after we filter it with regex
func SortFunctionality(nfts []NFTPrices, paginate Paginate) {
	sort.Slice(nfts, func(i, j int) bool {
		var res = j > i
		if paginate.Direction == "asc" {
			res = strings.ToLower(nfts[i].Name) < strings.ToLower(nfts[j].Name)
		} else {
			res = strings.ToLower(nfts[i].Name) > strings.ToLower(nfts[j].Name)
		}
		return res
	})
}

// Use this function to build the nfts for all names after the filter and the sort functions
// it wil return array of nfts that ordered as we need
// asc order will return ==> numericChar --> normalChar --> specialChar
// desc order will return ==> normalChar --> numericChar --> specialChar
func AppendNFTs(initialNFTs []NFTPrices, normalChar []NFTPrices, numericChar []NFTPrices, specialChar []NFTPrices) []NFTPrices {
	var nfts []NFTPrices
	nfts = append(initialNFTs, normalChar...)
	nfts = append(nfts, numericChar...)
	nfts = append(nfts, specialChar...)
	return nfts
}

func BuildRegexFilter(nfts []NFTPrices) ([]NFTPrices, []NFTPrices, []NFTPrices) {
	var specialNames []NFTPrices
	var numberNames []NFTPrices
	var charNames []NFTPrices
	// specialPattern := `^[!@#$%^&*()_+{}\[\]:;<>,.?~\\/\|\-✖𝚃\- ]`
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