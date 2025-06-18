nftsService = rfServices.NewNFTsService(db)
nftsService

// nfts page endpoints
nfts = v2.PathPrefix("/nfts").Subrouter()
nfts.HandleFunc("/chains", microservices.GetNFTChains).Methods(http.MethodGet, http.MethodOptions)
nfts.HandleFunc("/prices/{dataset}", microservices.Search).Methods(http.MethodGet, http.MethodOptions)