// BuildCustomCategoriesDataFS
// It will build the Custom Categories Data with dynamic query from FS.
func (m *Microservices) BuildCustomCategoriesDataFSTest(w http.ResponseWriter, r *http.Request) {
	span, labels := common.GenerateSpan("V2 BuildCustomCategoriesDataFS", r.Context())
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 BuildCustomCategoriesDataFS"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 BuildCustomCategoriesDataFS"))
	var (
		categories map[string]store.CategoriesData
		err        error
		res        []byte
	)

	categories, err = m.customCategoryService.BuildCustomCategoriesDataFS(r.Context())

	if err != nil {
		goto ERR
	}
	res, err = json.Marshal(categories)

	log.EndTimeL(labels, "V2 BuildCustomCategoriesDataFS", startTime, nil)
	span.SetStatus(codes.Ok, "V2 BuildCustomCategoriesDataFS")
	w.Write(res)
	return
ERR:
	log.ErrorL(labels, "%s", err)
	span.SetStatus(codes.Error, err.Error())
	w.WriteHeader(http.StatusInternalServerError)
	return
}
