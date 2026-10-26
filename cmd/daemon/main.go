package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/hernan/get-torrent/pkg/dht"
	"github.com/hernan/get-torrent/pkg/engine"
	"github.com/hernan/get-torrent/pkg/rpc"
)

func main() {
	rpcAddr := flag.String("rpc-addr", "127.0.0.1:9091", "Address for Transmission RPC server")
	downloadDir := flag.String("download-dir", "./downloads", "Directory to save completed torrents")
	configDir := flag.String("config-dir", "./config", "Directory to store session state and resume data")
	port := flag.Uint("port", 6881, "BitTorrent peer listen port (TCP; the DHT uses the same UDP port)")
	dhtEnabled := flag.Bool("dht", true, "Enable the Mainline DHT (BEP 0005) for trackerless peer discovery")
	dhtBootstrap := flag.String("dht-bootstrap", strings.Join(dht.DefaultBootstrap, ","), "Comma-separated DHT bootstrap routers (host:port)")
	flag.Parse()

	log.Println("==================================================")
	log.Println("⚡ get-torrent Daemon (Transmission BitTorrent Clone)")
	log.Println("==================================================")

	eng := engine.NewEngine(*downloadDir, uint16(*port))
	eng.ConfigDir = *configDir
	log.Printf("[Daemon] Initialized BitTorrent Engine (PeerID: %s, Port: %d)", string(eng.PeerID[:8]), *port)

	if err := eng.StartListener(); err != nil {
		log.Fatalf("[Daemon] Failed to start peer listener: %v", err)
	}

	if *dhtEnabled {
		var routers []string
		for _, r := range strings.Split(*dhtBootstrap, ",") {
			if r = strings.TrimSpace(r); r != "" {
				routers = append(routers, r)
			}
		}
		if err := eng.EnableDHT(routers); err != nil {
			log.Printf("[Daemon] DHT disabled: %v", err)
		}
	}

	// Restore prior session state and resume torrents. This runs after the listener and DHT are
	// up so resumed torrents announce the right port and can use the DHT from their first round.
	if err := eng.LoadState(); err != nil {
		log.Printf("[Daemon] Notice: Could not load prior session: %v", err)
	}

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
	// Save before Close: Close stops every torrent, which would persist them all as "stopped"
	// and prevent active downloads from resuming on the next start.
	if err := eng.SaveState(); err != nil {
		log.Printf("[Daemon] Error saving session state: %v", err)
	} else {
		log.Println("[Daemon] Session state saved successfully.")
	}
	_ = eng.Close()
}
