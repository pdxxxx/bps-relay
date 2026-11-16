package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pdxxxx/bps-relay/internal/relay"
)

var version = "development"

func main() {
	if len(os.Args) == 2 && os.Args[1] == "--version" {
		log.Printf("bps-relay %s protocol=1", version)
		return
	}
	service, err := relay.New(os.Getenv("BPS_RELAY_TOKEN"))
	if err != nil {
		log.Fatal(err)
	}
	addr := os.Getenv("BPS_RELAY_LISTEN")
	if addr == "" {
		addr = "127.0.0.1:19361"
	}
	server := &http.Server{Addr: addr, Handler: service, ReadHeaderTimeout: 60 * time.Second, IdleTimeout: 180 * time.Second, MaxHeaderBytes: 32768}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		service.Close()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	log.Printf("bps-relay %s listening; protocol=1 no server concurrency limit", version)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal("relay listener failed")
	}
}
