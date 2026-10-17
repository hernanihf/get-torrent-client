package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/hernan/get-torrent/pkg/rpc"
)

func main() {
	rpcAddr := flag.String("rpc-addr", "127.0.0.1:9091", "Address for Transmission RPC server")
	flag.Parse()

	log.Println("==================================================")
	log.Println("⚡ get-torrent Daemon (Transmission BitTorrent Clone)")
	log.Println("==================================================")

	server := rpc.NewServer(*rpcAddr)

	go func() {
		if err := server.ListenAndServe(); err != nil {
			log.Fatalf("RPC server error: %v", err)
		}
	}()

	log.Printf("Ready. Compatible with Transmission Remote GUI, transmission-remote, and WebUI.")
	log.Println("Press Ctrl+C to terminate.")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down get-torrent daemon gracefully...")
}
