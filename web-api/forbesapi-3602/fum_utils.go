package utils

import (
	"strings"
	"time"

	"github.com/Forbes-Media/Systems-golang-common-libraries/futils"
	"github.com/Forbes-Media/fum"
	"github.com/mitchellh/mapstructure"
)

func UnsetEmailsFromAuthor(author *fum.Author) {
	if author == nil {
		return
	}
	author.Email = ""
	author.AltEmail = ""
}

func UnsetEmailsFromPublication(pub *fum.Publication) {
	if pub == nil {
		return
	}
	UnsetEmailsFromAuthor(pub.PrimaryContributorData)
	UnsetEmailsFromPublication(pub.PreviousPublicationData)
	for i := range pub.Contributors {
		UnsetEmailsFromAuthor(&pub.Contributors[i])
	}
	for i := range pub.SubBlogs {
		UnsetEmailsFromPublication(&pub.SubBlogs[i])
	}

}

func FilterItemsByDate(items []fum.Item, limit int) []fum.Item {
	var templates []fum.Item
	if len(items) != 0 && items != nil {
		for i := 0; i < len(items) && len(templates) < limit; i++ {
			item := items[i]
			if item.Date != nil && item.Date.Before(time.Now()) {
				templates = append(templates, item)
			}
		}
	}
	return templates
}

func ConvertListToItemList[T fum.IListEntity](lists []T) []fum.Item {
	var items []fum.Item
	if len(lists) > 0 {
		lastName := ""
		for _, list := range lists {
			var item fum.Item
			item.Id = list.GetNaturalId()
			item.ListName = list.GetName()
			item.ListUri = list.GetListUri()
			item.ListRank = list.GetRank()
			item.ListYear = list.GetYear()
			if !strings.EqualFold(lastName, list.GetName()) {
				items = append(items, item)
			}
			lastName = list.GetName()
		}
	}
	return items
}
func ConvertPersonListToItemList(personLists []fum.PersonList) []fum.Item {
	var items []fum.Item
	if len(personLists) > 0 {
		lastName := ""
		for _, personList := range personLists {
			var item fum.Item
			item.Id = personList.GetNaturalId()
			item.ListName = personList.GetName()
			item.ListUri = personList.GetListUri()
			item.ListRank = personList.GetRank()
			item.ListYear = personList.GetYear()
			if !strings.EqualFold(lastName, personList.GetName()) {
				items = append(items, item)
			}
			lastName = personList.GetName()
		}
	}
	return items
}
func ConvertPlaceListToItemList(placeLists []fum.PlaceList) []fum.Item {
	var items []fum.Item
	if len(placeLists) > 0 {
		lastName := ""
		for _, placeList := range placeLists {
			var item fum.Item
			item.Id = placeList.NaturalId
			item.ListName = placeList.Name
			item.ListUri = placeList.Uri
			item.ListRank = placeList.Rank
			item.ListYear = placeList.Year
			if !strings.EqualFold(lastName, placeList.Name) {
				items = append(items, item)
			}
			lastName = placeList.Name
		}
	}
	return items
}
func ConvertOrganizationListToItemList(organizationLists []fum.OrganizationList) []fum.Item {
	var items []fum.Item
	if len(organizationLists) > 0 {
		lastName := ""
		for _, organizationList := range organizationLists {
			var item fum.Item
			item.Id = organizationList.GetNaturalId()
			item.ListName = organizationList.GetName()
			item.ListUri = organizationList.GetListUri()
			item.ListRank = organizationList.GetRank()
			item.ListYear = organizationList.GetYear()
			if !strings.EqualFold(lastName, organizationList.GetName()) {
				items = append(items, item)
			}
			lastName = organizationList.GetName()
		}
	}
	return items
}

func ConvertContentVidToItem(vid fum.Content) fum.Item {
	item := futils.MapStructureDecodeTo[fum.Item](vid)
	item.Id = vid.NaturalId
	item.Type = "video"
	item.Keywords = vid.NewsKeywords
	item.ContentId = vid.Id.Hex()
	item.ModifiedDate = vid.UpdatedDate
	if vid.Video == nil {
		return *item
	}
	vd := vid.Video
	_ = mapstructure.Decode(vd, &item)
	item.Image = vd.StillImage
	item.Thumbnail = vd.ThumbImage
	return *item
}
