
func GetTop75Assets(ctx0 context.Context) ([]services.TopAsset, error) {
	pg := PGConnect()

	ctx, span := tracer.Start(ctx0, "Get Top75 Assets")

	defer span.End()
	startTime := log.StartTime("Get Top75 Assets Query")

	var topAssets []services.TopAsset
	span.AddEvent("Start Getting Top75 Assets")
	queryResult, err := pg.QueryContext(ctx, `
	SELECT 
		symbol, 
		name, 
		slug, 
		market_cap
	FROM 
		public.fundamentalslatest
	where 
		market_cap != 0
		and 
		market_cap is not null
	order by 
		market_cap desc
	limit 75
	`)

	if err != nil {
		span.SetStatus(codes.Error, "PGGetTop75Assets")
		log.EndTime("Get Top75 Assets Query", startTime, err)
		return nil, err
	}
	defer queryResult.Close()

	for queryResult.Next() {
		var topAsset services.TopAsset
		err := queryResult.Scan(&topAsset.Symbol, &topAsset.Name, &topAsset.Slug, &topAsset.MarketCap)
		if err != nil {
			span.SetStatus(codes.Error, "PG GetTop75Assets scan error")
			log.EndTime("Get Top75 Assets Query", startTime, err)
			return nil, err
		}
		topAssets = append(topAssets, topAsset)
	}
	log.EndTime("Get Top75 Assets Query", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	// sort.Slice(topAssets, func(i, j int) bool {
	// 	return topAssets[i].Name < topAssets[j].Name
	// })
	return topAssets, nil
}
