r.HandleFunc("/validateToken", microservices.ValidateToken).Methods(http.MethodGet, http.MethodOptions)
