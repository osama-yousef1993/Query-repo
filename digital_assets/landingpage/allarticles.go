// articles from newsfeed, editerpick and latest section

newsFeed, err := services.GetCachedNewsFeed()
if err != nil {
	log.Error("%s", err)
}

data, err := services.GetEditorsPick()