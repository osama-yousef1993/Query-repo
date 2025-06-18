
nftsService          = rfServices.NewNFTsService(db)


nftsService

nfts = v2.PathPrefix("/nfts").Subrouter()
nfts.HandleFunc("/chains", microservices.GetNFTChains).Methods(http.MethodGet, http.MethodOptions)
nfts.HandleFunc("/prices", GetNFTPrices).Methods(http.MethodGet, http.MethodOptions)
