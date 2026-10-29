package engine

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"

	"github.com/hernan/get-torrent/pkg/dht"
)

// dhtState is the persisted DHT identity and routing table, so restarts rejoin quickly.
type dhtState struct {
	ID    string   `json:"id"`
	Nodes []string `json:"nodes"`
}

// dhtStateFile is where a node's state is kept: dht.json for IPv4, dht6.json for IPv6.
func (e *Engine) dhtStateFile(ipv6 bool) string {
	if e.ConfigDir == "" {
		return ""
	}
	if ipv6 {
		return filepath.Join(e.ConfigDir, "dht6.json")
	}
	return filepath.Join(e.ConfigDir, "dht.json")
}

// loadDHTState reads a saved node ID and routing table into cfg, if present.
func loadDHTState(file string, cfg *dht.Config) {
	if file == "" {
		return
	}
	data, err := os.ReadFile(file)
	if err != nil {
		return
	}
	var st dhtState
	if json.Unmarshal(data, &st) != nil {
		return
	}
	if raw, err := hex.DecodeString(st.ID); err == nil && len(raw) == len(cfg.ID) {
		copy(cfg.ID[:], raw)
	}
	cfg.Nodes = st.Nodes
}

// EnableDHT starts a DHT node on the peer port (UDP) and bootstraps it in the background, plus
// an IPv6 node (BEP 0032) on the same port when the host has IPv6; failing to start that one is
// not an error. Call after StartListener so the port is known. Saved node IDs and routing
// tables are reused, and both nodes share the IPv4 node's ID.
func (e *Engine) EnableDHT(bootstrap []string) error {
	e.mu.Lock()
	if e.DHT != nil {
		e.mu.Unlock()
		return nil
	}
	port := e.Port
	file4, file6 := e.dhtStateFile(false), e.dhtStateFile(true)
	e.mu.Unlock()

	cfg := dht.Config{Addr: fmt.Sprintf(":%d", port), Bootstrap: bootstrap}
	loadDHTState(file4, &cfg)
	server, err := dht.New(cfg)
	if err != nil {
		return err
	}

	cfg6 := dht.Config{Addr: fmt.Sprintf("[::]:%d", port), Bootstrap: bootstrap, ID: server.ID(), IPv6: true}
	loadDHTState(file6, &cfg6)
	cfg6.ID = server.ID()
	server6, err := dht.New(cfg6)
	if err != nil {
		log.Printf("[Engine] IPv6 DHT unavailable: %v", err)
		server6 = nil
	}

	e.mu.Lock()
	e.DHT, e.DHT6 = server, server6
	e.mu.Unlock()

	for _, s := range e.dhtServers() {
		log.Printf("[Engine] DHT node %s listening on UDP %s", s.ID(), s.Addr())
		go func(s *dht.Server) {
			if err := s.Bootstrap(e.ctx); err != nil {
				log.Printf("[Engine] DHT bootstrap on %s: %v (will retry)", s.Addr(), err)
			}
		}(s)
	}
	return nil
}

// DHTEnabled reports whether a DHT node is running.
func (e *Engine) DHTEnabled() bool { return e.dhtServer() != nil }

// dhtServers returns the running DHT nodes: IPv4 first, then IPv6.
func (e *Engine) dhtServers() []*dht.Server {
	e.mu.RLock()
	defer e.mu.RUnlock()
	var out []*dht.Server
	for _, s := range []*dht.Server{e.DHT, e.DHT6} {
		if s != nil {
			out = append(out, s)
		}
	}
	return out
}

// dhtServer returns the DHT node, or nil when DHT is disabled.
func (e *Engine) dhtServer() *dht.Server {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.DHT
}

// saveDHTState persists each DHT node's ID and routing table. Caller must not hold e.mu.
func (e *Engine) saveDHTState() error {
	e.mu.RLock()
	nodes := map[string]*dht.Server{e.dhtStateFile(false): e.DHT, e.dhtStateFile(true): e.DHT6}
	e.mu.RUnlock()
	for file, server := range nodes {
		if err := saveDHTNode(server, file); err != nil {
			return err
		}
	}
	return nil
}

func saveDHTNode(server *dht.Server, stateFile string) error {
	if server == nil || stateFile == "" {
		return nil
	}
	id := server.ID()
	data, err := json.MarshalIndent(dhtState{ID: hex.EncodeToString(id[:]), Nodes: server.Nodes()}, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(stateFile), 0755); err != nil {
		return fmt.Errorf("creating config dir: %w", err)
	}
	tmp := stateFile + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return fmt.Errorf("writing dht state: %w", err)
	}
	return os.Rename(tmp, stateFile)
}

// addDHTNode handles a BEP 0005 PORT message: the peer runs a DHT node on that UDP port. Each
// node ignores addresses of the other family, so it is offered to both.
func (e *Engine) addDHTNode(peerIP net.IP, port uint16) {
	if peerIP == nil {
		return
	}
	for _, server := range e.dhtServers() {
		server.AddNode(&net.UDPAddr{IP: peerIP, Port: int(port)})
	}
}
