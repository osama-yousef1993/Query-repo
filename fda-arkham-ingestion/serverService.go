package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Forbes-Media/go-tools/log"
	"github.com/gorilla/mux"
)

func initServer(r *mux.Router) {
	port := os.Getenv("port")
	if port == "" {
		port = "3000"
	}

	// Server Setting
	svr := &http.Server{
		Handler:      r,
		Addr:         ":" + port,
		WriteTimeout: 3599 * time.Second,
		ReadTimeout:  3599 * time.Second,
	}

	go func() {
		// start the web server on port and accept requests
		log.Info("Server Listening on port %s", port)
		if err := svr.ListenAndServe(); err != nil {
			log.Error("%s", err)
		}
	}()

	var wait time.Duration
	gracefulStop := make(chan os.Signal, 1)

	// We'll accept graceful shutdowns when quit via SIGINT, SIGTERM, SIGKILL, or Interrupt
	signal.Notify(gracefulStop, syscall.SIGTERM, syscall.SIGINT, os.Interrupt)

	// Block until we receive our signal.
	<-gracefulStop

	ctx, cancel := context.WithTimeout(context.Background(), wait)

	defer cancel()

	svr.Shutdown(ctx)

	log.Info("Web Server ShutDown")

	close(gracefulStop)

}
