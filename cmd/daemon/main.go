package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/hernan/get-torrent/pkg/engine"
	"github.com/hernan/get-torrent/pkg/rpc"
)

func main() {
	rpcAddr := flag.String("rpc-addr", "127.0.0.1:9091", "Address for Transmission RPC server")
	downloadDir := flag.String("download-dir", "./downloads", "Directory to save completed torrents")
	port := flag.Uint("port", 6881, "BitTorrent peer listen port")
	flag.Parse()

	log.Println("==================================================")
	log.Println("⚡ get-torrent Daemon (Transmission BitTorrent Clone)")
	log.Println("==================================================")

	eng := engine.NewEngine(*downloadDir, uint16(*port))
	log.Printf("[Daemon] Initialized BitTorrent Engine (PeerID: %s, Port: %d)", string(eng.PeerID[:8]), *port)

	server := rpc.NewServer(*rpcAddr, eng)

	go func() {
		if err := server.ListenAndServe(); err != nil {
			log.Fatalf("RPC server error: %v", err)
		}
	}()

	log.Printf("[Daemon] Ready. Compatible with Transmission Remote GUI, transmission-remote, and WebUI.")
	log.Println("[Daemon] Press Ctrl+C to terminate.")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down get-torrent daemon gracefully...")
}
