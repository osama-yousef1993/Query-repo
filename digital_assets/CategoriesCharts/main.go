
	// Go Routine 11
	// Get All 24 Hour Categories Chart Data for all CAtegories from BQ
	var chartCategoriesData24hrResults []store.TimeSeriesResultPG

	g.Go(func() error {

		chartCategoriesData24hrResults, err = bqs.QueryChartByInterval("24 hour", "900", labels["UUID"], store.Category, ctx)
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return errors.New("error getting 24hr chart Data  " + err.Error())
		}
		log.DebugL(labels, "Received Exchanges %d results from BQ", len(chartCategoriesData24hrResults))

		return nil
	})