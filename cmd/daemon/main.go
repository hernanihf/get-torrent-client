package main

import (
	"flag"
	"log"
	"net"
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
	rpcUser := flag.String("rpc-username", "", "Require HTTP Basic authentication for RPC with this username")
	rpcPass := flag.String("rpc-password", "", "RPC password (prefer the GET_TORRENT_RPC_PASSWORD environment variable, which is not visible in process listings)")
	rpcHosts := flag.String("rpc-host-whitelist", "", "Comma-separated extra hostnames allowed in the RPC Host header (IPs and localhost are always allowed; \"*.\" prefixes match subdomains)")
	flag.Parse()

	password := *rpcPass
	if env := os.Getenv("GET_TORRENT_RPC_PASSWORD"); env != "" {
		password = env
	}
	if *rpcUser != "" && password == "" {
		log.Fatalf("[Daemon] --rpc-username requires a password (--rpc-password or GET_TORRENT_RPC_PASSWORD)")
	}

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
		if err := eng.EnableDHT(splitList(*dhtBootstrap)); err != nil {
			log.Printf("[Daemon] DHT disabled: %v", err)
		}
	}

	// Restore prior session state and resume torrents. This runs after the listener and DHT are
	// up so resumed torrents announce the right port and can use the DHT from their first round.
	if err := eng.LoadState(); err != nil {
		log.Printf("[Daemon] Notice: Could not load prior session: %v", err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	server := rpc.NewServerWithOptions(*rpcAddr, eng, rpc.Options{
		Username:      *rpcUser,
		Password:      password,
		HostWhitelist: splitList(*rpcHosts),
		// "session-close" shuts the daemon down like a signal would.
		OnSessionClose: func() { sigChan <- syscall.SIGTERM },
	})
	if *rpcUser == "" && !isLoopback(*rpcAddr) {
		log.Printf("[Daemon] WARNING: RPC is reachable on %s without authentication; set --rpc-username", *rpcAddr)
	}

	go func() {
		if err := server.ListenAndServe(); err != nil {
			log.Fatalf("RPC server error: %v", err)
		}
	}()

	log.Printf("[Daemon] Ready. Compatible with Transmission Remote GUI, transmission-remote, and WebUI.")
	log.Println("[Daemon] Press Ctrl+C to terminate.")

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

// splitList parses a comma-separated flag value, dropping empty entries.
func splitList(v string) []string {
	var out []string
	for _, item := range strings.Split(v, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}

// isLoopback reports whether a listen address only accepts local connections.
func isLoopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
