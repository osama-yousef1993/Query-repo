memberReport.Handle("/GetLegacyPassHolderProfiles", http.HandlerFunc(microservices.GetLegacyPassHolderProfiles)).Methods(http.MethodGet, http.MethodOptions)
