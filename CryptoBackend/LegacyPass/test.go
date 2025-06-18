
// BQInsertLegacyPassInfo insert all members info that fetched from MySql to BQ
// Takes (ctx context.Context, memberInfo []datastruct.BQLegacyPassInfo)
// Returns (error)
//
// Returns  error if the insert process to BQ failed and no error if successful
func (m *memberReportQuery) BQInsertLegacyPassInfo(ctx context.Context, memberInfo []datastruct.BQLegacyPassInfo) error {
	span, labels := common.GenerateSpan("memberReportQuery.BQInsertLegacyPassInfo", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "memberReportQuery.BQInsertLegacyPassInfo"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "memberReportQuery.BQInsertLegacyPassInfo"))
	var (
		wg           = sync.WaitGroup{}
		throttleChan = make(chan bool, 200)
		mu           = sync.Mutex{}
	)

	batchSize := 1000
	for i := 0; i < len(memberInfo); i += batchSize {
		end := i + batchSize
		if end > len(memberInfo) {
			end = len(memberInfo)
		}
		throttleChan <- true
		wg.Add(1)
		go func(i int, end int) error {

			mu.Lock()
			err := m.InsertLegacyPass(ctx, memberInfo[i:end])
			if err != nil {
				log.EndTimeL(labels, "memberReportQuery.BQInsertLegacyPassInfo Error Upserting Member Info ", startTime, err)
				return err
			}
			mu.Unlock()
			<-throttleChan
			wg.Done()
			return nil
		}(i, end)
	}
	wg.Wait()
	log.EndTimeL(labels, "memberReportQuery.BQInsertLegacyPassInfo Finished Successfully ", startTime, nil)
	span.SetStatus(codes.Ok, "memberReportQuery.BQInsertLegacyPassInfo Finished Successfully ")

	return nil
}

func (m *memberReportQuery) InsertLegacyPass(ctx context.Context, memberInfo []datastruct.BQLegacyPassInfo) error {
	span, labels := common.GenerateSpan("memberReportQuery.BQInsertLegacyPassInfo", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "memberReportQuery.BQInsertLegacyPassInfo"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "memberReportQuery.BQInsertLegacyPassInfo"))

	var initialRecord string
	var subsequentRecords string
	legacyPassTable := common.GetTableName("Legacy_Pass_Info")
	client, err := bqUtils.GetBigQueryClient()

	if err != nil {
		return err
	}
	for index, member := range memberInfo {
		if index == 0 {
			initialRecord = BuildSelectStatementForLegacyPassInfo(member)
		} else {
			subsequentRecords += fmt.Sprintf(" UNION ALL %s", BuildSelectStatementForLegacyPassInfo(member))
		}
	}
	queryString := `MERGE INTO api-project-901373404215.digital_assets.` + legacyPassTable + ` T
	USING (
	  ` + initialRecord + subsequentRecords + `
	) AS S
	ON T.id = S.id
	WHEN MATCHED THEN
	  UPDATE SET
		id = S.id,
		email = S.email,
		row_last_updated = S.row_last_updated
	WHEN NOT MATCHED THEN
	  INSERT (id, email, row_last_updated)
	  VALUES (
		S.id,
		S.email,
		S.row_last_updated
	  );`

	query := client.Query(queryString)

	job, err := query.Run(ctx)

	var retryError error
	if err != nil {
		// We need to check the error if it contains 400
		// If it contains 400 we need to divide the Query so the BigQuery can handle it.
		if strings.Contains(err.Error(), "400") || strings.Contains(err.Error(), "413") {
			l := len(memberInfo)
			var memInfo []datastruct.BQLegacyPassInfo
			memInfo = append(memInfo, memberInfo...)
			for y := (l / 3); y < l; y += (l / 3) {
				a := memInfo[y-(l/3) : y]
				er := m.BQInsertLegacyPassInfo(ctx, a)
				if er != nil {
					retryError = er
				}
			}
			log.EndTimeL(labels, "memberReportQuery.BQInsertLegacyPassInfo Error Sub Upserting Member Info for recursive", startTime, retryError)
			return retryError
		}
		log.EndTimeL(labels, "memberReportQuery.BQInsertLegacyPassInfo Error Upserting Member Info ", startTime, err)
		return err
	}
	log.Info("memberReportQuery.BQInsertLegacyPassInfo BigQuery Job ID : %s", job.ID())
	_, err = job.Wait(ctx)
	if err != nil {
		log.EndTimeL(labels, "memberReportQuery.BQInsertLegacyPassInfo Error Upserting Member Info ", startTime, err)
		return err
	}
	log.EndTimeL(labels, "memberReportQuery.BQInsertLegacyPassInfo Finished Successfully ", startTime, nil)
	span.SetStatus(codes.Ok, "memberReportQuery.BQInsertLegacyPassInfo Finished Successfully ")
	return nil
}
