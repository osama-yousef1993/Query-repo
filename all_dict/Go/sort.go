package store

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"unicode"
)

func customLess(a, b string) bool {
	// Check if a and b are both numbers or both letters
	isANumber := unicode.IsDigit(rune(a[0]))
	isBNumber := unicode.IsDigit(rune(b[0]))

	if isANumber && isBNumber {
		return a < b // Compare numbers as strings
	} else if !isANumber && !isBNumber {
		// Compare lowercase versions of the strings
		aLower := strings.ToLower(a)
		bLower := strings.ToLower(b)
		return aLower < bLower
	} else {
		return isANumber // Numbers come before letters
	}
}

func SortNamesAscending(names []string) {
	sort.SliceStable(names, func(i, j int) bool {
		return customLess(names[i], names[j])
	})
}

func SortNamesDescending(names []string) {
	sort.SliceStable(names, func(i, j int) bool {
		return customLess(names[j], names[i])
	})
}

func MainLess() {
	names := []string{"ZUUDTigerWarriors", "𝚃𝙷𝙴 𝙸𝙽𝚂𝙸𝙳𝙴𝚁𝚂", "100 Web Characters", "Doodleverse", "10KTF", "序数プロトコル", "9Name", "0N1 Force", "✖"}

	fmt.Println("Original names:", names)

	SortNamesAscending(names)
	fmt.Println("Ascending order:", names)

	SortNamesDescending(names)
	fmt.Println("Descending order:", names)
}

func SortSpecialChart() {
	specialPattern := `[!@#$%^&*()_+{}\[\]:;<>,.?~\\/\|\-✖𝚃]`
	numberPattern := `[0-9]`
	charPattern := `[a-z A-Z]`
	// Create a regular expression object
	specialRegex, err := regexp.Compile(specialPattern)
	if err != nil {
		fmt.Println("Error compiling regex:", err)
		return
	}
	numberRegex, err := regexp.Compile(numberPattern)
	if err != nil {
		fmt.Println("Error compiling regex:", err)
		return
	}
	charRegex, err := regexp.Compile(charPattern)
	if err != nil {
		fmt.Println("Error compiling regex:", err)
		return
	}
	names := []string{"ZUUDTigerWarriors", "𝚃𝙷𝙴 𝙸𝙽𝚂𝙸𝙳𝙴𝚁𝚂", "100 Web Characters", "Doodleverse", "10KTF", "序数プロトコル", "9Name", "0N1 Force", "✖"}
	var specialNames []string
	var numberNames []string
	var charNames []string
	var result []string
	for _, str := range names {
		if specialRegex.MatchString(str) {
			specialNames = append(specialNames, str)
		} else if numberRegex.MatchString(str) {
			numberNames = append(numberNames, str)
		} else if charRegex.MatchString(str) {
			charNames = append(charNames, str)
		}
	}
	Direction := "asc"
	sort.Slice(specialNames, func(i, j int) bool {
		var res = j > i
		if Direction == "asc" {
			res = strings.ToLower(specialNames[i]) < strings.ToLower(specialNames[j])
		} else {
			res = strings.ToLower(specialNames[i]) > strings.ToLower(specialNames[j])
		}
		return res
	})
	sort.Slice(numberNames, func(i, j int) bool {
		var res = j > i
		if Direction == "asc" {
			res = strings.ToLower(numberNames[i]) < strings.ToLower(numberNames[j])
		} else {
			res = strings.ToLower(numberNames[i]) > strings.ToLower(numberNames[j])
		}
		return res
	})
	sort.Slice(charNames, func(i, j int) bool {
		var res = j > i
		if Direction == "asc" {
			res = strings.ToLower(charNames[i]) < strings.ToLower(charNames[j])
		} else {
			res = strings.ToLower(charNames[i]) > strings.ToLower(charNames[j])
		}
		return res
	})

	if Direction == "asc" {

		result = append(result, charNames...)
		result = append(result, numberNames...)
		result = append(result, specialNames...)
	} else {
		result = append(result, numberNames...)
		result = append(result, charNames...)
		result = append(result, specialNames...)
	}

	fmt.Println("Error compiling regex:", result)
	// [9Name 10KTF 100 Web Characters 0N1 Force ZUUDTigerWarriors Doodleverse 𝚃𝙷𝙴 𝙸𝙽𝚂𝙸𝙳𝙴𝚁𝚂 ✖]
	// [Doodleverse ZUUDTigerWarriors 0N1 Force 100 Web Characters 10KTF 9Name ✖ 𝚃𝙷𝙴 𝙸𝙽𝚂𝙸𝙳𝙴𝚁𝚂]
}
