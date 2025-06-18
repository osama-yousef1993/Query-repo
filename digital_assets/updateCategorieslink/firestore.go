
// Get All Featured Categories from FS
func GetFeaturedCategoriesTest(ctx0 context.Context) ([]FeaturedCategory, error) {
	fs := GetFirestoreClient()
	ctx, span := tracer.Start(ctx0, "GetFeaturedCategories")
	defer span.End()

	var featuresCategories []FeaturedCategory

	collectionName := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "categories")
	// Get Featured Categories and order it by category order
	iter := fs.Collection(collectionName).Documents(ctx)
	span.AddEvent("Start Getting Featured Categories Data from FS")

	for {
		var featuresCategory FeaturedCategory

		doc, err := iter.Next()

		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Error("Error Getting Featured Categories Data from FS: %s", err)
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, err
		}
		err = doc.DataTo(&featuresCategory)
		if err != nil {
			log.Error("Error Getting Featured Categories Data from FS: %s", err)
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, err
		}
		featuresCategory.Link = fmt.Sprintf("https://www.forbes.com/digital-assets/categories/%s/", featuresCategory.ID)
		featuresCategories = append(featuresCategories, featuresCategory)

	}
	return featuresCategories, nil
}


// Update trending tags for topics from 24 hours.
func UpdateIsCategoriesLink(ctx0 context.Context, categories []FeaturedCategory) {

	fs := GetFirestoreClient()
	ctx, span := tracer.Start(ctx0, "SaveNewsTopics")
	defer span.End()

	collectionName := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "categories")
	span.AddEvent("Start Update old Trending as not Trending")
	for _, category := range categories {
		fs.Collection(collectionName).Doc(category.ID).Set(ctx, map[string]interface{}{
			"categoryLink": category.Link,
		}, firestore.MergeAll)
		_, err := fs.Collection(collectionName).Doc(category.ID).Update(ctx, []firestore.Update{
			{
				Path:  "category",
				Value: firestore.Delete,
			},
			{
				Path:  "link",
				Value: firestore.Delete,
			},
		})
		if err != nil {
			log.Error("Error Community Page Announcements Data from FS: %s", err)
			span.AddEvent(fmt.Sprintf("Error Community Page Announcements Data from FS: %s", err))
			span.SetStatus(otelCodes.Error, err.Error())
		}

	}

	span.SetStatus(otelCodes.Ok, "Success")

}
