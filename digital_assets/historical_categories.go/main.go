r.Handle("/build-historical-categories-Data", oidcAuthMiddleware.Middleware(http.HandlerFunc(BuildCategoriesHistoricalData))).Methods(http.MethodPost)


// build fundamentals from coingecko Data
func BuildCategoriesFundamentals(ctx0 context.Context, labels map[string]string, bqs *store.BQStore, categoryList *[]store.CategoriesData, assets *[]store.Fundamentals, categoriesHistoricalList map[string]store.HistoricalCategoriesBQ) error {

	ctx, span := tracer.Start(ctx0, "PGGetCategories")
	defer span.End()

	labels["subfunction"] = "BuildCategoriesFundamentals"
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()
	span.SetAttributes(attribute.String("UUID", labels["UUID"]))

	startTime := log.StartTimeL(labels, "Build Categories Fundamentals Data")
	var allFundamentals []store.CategoryFundamental
	for _, category := range *categoryList {
		categoryHistoricalData := categoriesHistoricalList[category.ID]
		categoryFundamental := store.MapCategoryFundamental(ctx, category, assets, categoryHistoricalData)
		allFundamentals = append(allFundamentals, categoryFundamental)
	}
	errUpsert := store.UpsertCategoryFundamentalsPG(ctx, &allFundamentals, labels)
	if errUpsert != nil {
		log.ErrorL(labels, "Error UpsertCategoryFundamentalsPG %s", errUpsert)
		span.SetStatus(codes.Error, "Category Fundamentals Building failed due to insert in BQ failed")
	}

	errInsertBQ := bqs.InsertCategoryFundamentalsBQ(ctx, labels["UUID"], &allFundamentals)
	if errInsertBQ != nil {
		log.ErrorL(labels, "Error InsertCategoryFundamentalsBQ %s", errInsertBQ)
		span.SetStatus(codes.Error, "Category Fundamentals Building failed due to insert in BQ failed")
	}

	log.EndTimeL(labels, "Category Fundamentals Build ", startTime, nil)
	if errUpsert == nil && errInsertBQ == nil {
		span.SetStatus(codes.Ok, "Category Fundamentals Built")
	}
	return nil
}


	// // Results from Go Routine 10
	// // List of categoryFundamentals24h to generate their fundamentals
	// var categoryFundamental24hList []store.CategoryFundamental

	// // Go Routine 10
	// // Get all categories 24h old fundamentals from BQ
	// g.Go(func() error {

	// 	e, err := bqs.GetCategoryFundamental24h(ctx, labels["UUID"])
	// 	if err != nil {
	// 		log.ErrorL(labels, "Error getting list of categories old Fundamentals from BQ: %s", err.Error())
	// 		return nil
	// 	}

	// 	log.DebugL(labels, "Received %d results from BQ Categories FUndamentals", len(e))

	// 	categoryFundamental24hList = e
	// 	fmt.Println(len(categoryFundamental24hList))

	// 	return nil
	// })

	// Results from Go Routine 10
	// List of categories historical Data to generate their fundamentals
	categoriesHistoricalList := make(map[string]store.HistoricalCategoriesBQ)
	// Go Routine 10
	// Get all categories historical data from BQ
	g.Go(func() error {

		e, err := bqs.GetCategoriesHistoricalData(ctx)
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return errors.New("Error getting list of categories historical from PG: " + err.Error())
		}

		log.DebugL(labels, "Received %d results from BQ Categories Historical", len(e))

		categoriesHistoricalList = e
		fmt.Println(len(categoriesHistoricalList))

		return nil
	})


	BuildCategoriesFundamentals(r.Context(), labels, bqs, &categoryList, &allFundamentals, categoriesHistoricalList)


func BuildCategoriesHistoricalData(w http.ResponseWriter, r *http.Request) {
	setResponseHeaders(w, 100)
	labels := make(map[string]string)
	ctx, span := tracer.Start(r.Context(), "BuildCategoriesHistoricalData")

	defer span.End()

	labels["UUID"] = uuid.New().String()
	labels["function"] = "BuildCategoriesHistoricalData"
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	span.SetAttributes(attribute.String("UUID", labels["UUID"]))

	startTime := log.StartTimeL(labels, "BuildCategoriesHistoricalData")

	bqs, err := store.NewBQStore()
	if err != nil {
		// If an error occurs, the full context is canceled. Which causes all the first two Go Routines to cancel.
		log.ErrorL(labels, "BuildCategoriesHistoricalData: Error connecting BigQuery %s", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	g, ctx := errgroup.WithContext(r.Context())
	var categoriesList []store.Categories
	g.Go(func() error {
		c, err := store.GetCategories(ctx)
		if err != nil {
			log.ErrorL(labels, "BuildCategoriesHistoricalData: Error Getting Categories Data from PG %s", err)
			span.SetStatus(codes.Error, err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			return err
		}
		categoriesList = c
		return nil
	})
	var assetsMetaData map[string]store.AssetMetaData
	g.Go(func() error {
		a, err := store.GetCoinGeckoMetaData(ctx)
		if err != nil {
			log.ErrorL(labels, "BuildCategoriesHistoricalData: Error Getting Assets Metadata from PG %s", err)
			span.SetStatus(codes.Error, err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			return err
		}
		assetsMetaData = a
		return nil
	})
	// var assets map[string]store.TopAsset
	// g.Go(func() error {
	// 	a, err := store.GetTopAssets(ctx)
	// 	if err != nil {
	// 		log.ErrorL(labels, "BuildCategoriesHistoricalData: Error Getting Assets Metadata from PG %s", err)
	// 		span.SetStatus(codes.Error, err.Error())
	// 		w.WriteHeader(http.StatusInternalServerError)
	// 		return err
	// 	}
	// 	assets = a
	// 	return nil
	// })

	err = g.Wait()  // Blocks till all go routines are done
	if err != nil { // If any of the go routines return an error
		log.ErrorL(labels, "BuildCategoriesHistoricalData: in go routines  %s", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Internal Server Error"))
		span.SetStatus(codes.Error, err.Error())
		return
	}
	var (
		wg           sync.WaitGroup
		throttleChan = make(chan bool, 20)
		lock         sync.Mutex
		qErr         error
	)
	for index, category := range categoriesList {
		throttleChan <- true
		wg.Add(1)
		go func(category store.Categories, index int) {
			log.Debug("BuildCategoriesHistoricalData: start build historical data for %d ->>>:  %s", index, category.ID)
			categoriesPrices, err := bqs.BuildCategoriesHistoricalData(context.Background(), category, assetsMetaData)
			if err != nil {
				log.ErrorL(labels, "BuildCategoriesHistoricalData: Writing file error for %s :-->  %s", category.ID, err)
				span.SetStatus(codes.Error, err.Error())
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			file, err := json.MarshalIndent(categoriesPrices, " ", "")
			if err != nil {
				log.ErrorL(labels, "BuildCategoriesHistoricalData: Writing file error for %s :-->  %s", category.ID, err)
				span.SetStatus(codes.Error, err.Error())
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			fileName := fmt.Sprintf("categoriesData/%s.json", category.ID)
			_ = os.WriteFile(fileName, file, 0644)

			if err != nil {
				log.ErrorL(labels, "BuildCategoriesHistoricalData: Error Building Categories historical Data from BQ %s", err)
				span.SetStatus(codes.Error, err.Error())
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			lock.Lock()
			if categoriesPrices != nil {
				log.Debug("BuildCategoriesHistoricalData: start inserting historical data for %d ->>>:  %s", index, category.ID)
				// bqs.InsertCategoriesHistoricalDataBQ(context.Background(), labels["UUID"], categoriesPrices)
			}
			lock.Unlock()
			<-throttleChan // Remove from the throttle channel - allowing another go routine to start
			wg.Done()

		}(category, index)

	}

	wg.Wait()
	if qErr != nil {
		log.Error("%s", qErr)
		w.WriteHeader(http.StatusInternalServerError)
	}

	log.EndTimeL(labels, "BuildCategoriesHistoricalData", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}
