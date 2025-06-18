r.HandleFunc("/UpdateUserInfo",	microservices.UpdateMemberInfo).Methods(http.MethodGet, http.MethodOptions)

CORDIAL_CONTACTS_URL=
// memberInfoServices
func (m *memberInfoService) UpdateMemberInfo(ctx context.Context, memberInfo *dto.MemberInfo) error {

	span, labels := common.GenerateSpan("memberInfoQuery.UpdateMemberInfo", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "memberInfoQuery.UpdateMemberInfo"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "memberInfoQuery.UpdateMemberInfo"))

	var userInfo *datastruct.MemberInfo

	// Get member Information from Database
	memInfo, err := m.GetMemberInfo(ctx, memberInfo)
	if err != nil {
		log.Info("User With this Email Not Exist")
		goto ERR
	}
	// Convert dto.MemberInfo to datastruct.MemberInfo
	userInfo = memberInfo_ConvertDTOToDatastruct(ctx, memInfo)

	// Check User Profile Data from Cordial
	err = common.CheckCordialUser(ctx, userInfo)
	if err != nil {
		log.Info("User Not created in Cordial")
		goto ERR
	}
	// Check user Grand if it exist 	
	err = common.CheckGrandAuthorization(ctx, userInfo)

	log.EndTime("memberInfoQuery.UpdateMemberInfo", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return nil

ERR:
	log.EndTime("memberInfoQuery.UpdateMemberInfo", startTime, nil)
	span.SetStatus(codes.Error, err.Error())
	return nil

}

// http getway
/*
CheckZephr  Makes a request to Zephr services and returns an object of the desired generic type.
The Generic type should be passed in a the object you are expecting back from the response.
Returns object of type T (use classes from Zephr.go)
*/

func CallCordial[T interface{}](ctx context.Context, host string, reqbody string, httpMethod string, header http.Header) (*T, error) {

	labels := make(map[string]string)
	span := trace.SpanFromContext(ctx)
	defer span.End()

	labels["function"] = "CheckZephr"
	labels["UUID"] = uuid.New().String()
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	span.SetAttributes(attribute.String("UUID", labels["UUID"]))
	span.AddEvent("start CheckZephr")
	var data T
	req, _ := http.NewRequest(httpMethod, host, strings.NewReader(reqbody))
	req.Header = header

	resp := Process(req)

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, errors.New(resp.Status)
	}
	data, err = ConvertResponseToObj[T](body, resp.Header["Content-Type"][0])

	resp.Body.Close()

	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return nil, err

	}
	span.SetStatus(codes.Ok, "CheckZephr")
	return &data, nil

}


// getmemberinfo
func (m *Microservices) UpdateMemberInfo(w http.ResponseWriter, r *http.Request) {
	var memberInfo *dto.MemberInfo

	span, labels := common.GenerateSpan("UpdateMemberInfo", r.Context())

	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "UpdateMemberInfo"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "UpdateMemberInfo"))

	body, err := io.ReadAll(r.Body)

	if err != nil {
		goto ERR
	}

	if err := json.Unmarshal(body, &memberInfo); err != nil {
		goto ERR
	}

	err = m.memberInfoService.UpdateMemberInfo(r.Context(), memberInfo)
	if err != nil {
		goto ERR
	}

	log.EndTimeL(labels, "UpdateMemberInfo", startTime, nil)
	w.Header().Add("Content-Type", "application/json")
	span.SetStatus(codes.Ok, "UpdateMemberInfo")
	w.Write([]byte("ok"))
	return

ERR:
	log.ErrorL(labels, "%s", err)
	span.SetStatus(codes.Error, err.Error())
	w.WriteHeader(http.StatusInternalServerError)
	return
}