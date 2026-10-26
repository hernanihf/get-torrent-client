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

func (e *Engine) dhtStateFile() string {
	if e.ConfigDir == "" {
		return ""
	}
	return filepath.Join(e.ConfigDir, "dht.json")
}

// EnableDHT starts a DHT node on the peer port (UDP) and bootstraps it in the background.
// Call after StartListener so the port is known. A saved node ID and routing table are reused.
func (e *Engine) EnableDHT(bootstrap []string) error {
	e.mu.Lock()
	if e.DHT != nil {
		e.mu.Unlock()
		return nil
	}
	port := e.Port
	stateFile := e.dhtStateFile()
	e.mu.Unlock()

	cfg := dht.Config{Addr: fmt.Sprintf(":%d", port), Bootstrap: bootstrap}
	if stateFile != "" {
		if data, err := os.ReadFile(stateFile); err == nil {
			var st dhtState
			if json.Unmarshal(data, &st) == nil {
				if raw, err := hex.DecodeString(st.ID); err == nil && len(raw) == len(cfg.ID) {
					copy(cfg.ID[:], raw)
				}
				cfg.Nodes = st.Nodes
			}
		}
	}

	server, err := dht.New(cfg)
	if err != nil {
		return err
	}

	e.mu.Lock()
	e.DHT = server
	e.mu.Unlock()

	log.Printf("[Engine] DHT node %s listening on UDP %s", server.ID(), server.Addr())
	go func() {
		if err := server.Bootstrap(e.ctx); err != nil {
			log.Printf("[Engine] DHT bootstrap: %v (will retry)", err)
		}
	}()
	return nil
}

// DHTEnabled reports whether a DHT node is running.
func (e *Engine) DHTEnabled() bool { return e.dhtServer() != nil }

// dhtServer returns the DHT node, or nil when DHT is disabled.
func (e *Engine) dhtServer() *dht.Server {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.DHT
}

// saveDHTState persists the DHT node ID and routing table. Caller must not hold e.mu.
func (e *Engine) saveDHTState() error {
	server := e.dhtServer()
	e.mu.RLock()
	stateFile := e.dhtStateFile()
	e.mu.RUnlock()
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

// addDHTNode handles a BEP 0005 PORT message: the peer runs a DHT node on that UDP port.
func (e *Engine) addDHTNode(peerIP net.IP, port uint16) {
	if server := e.dhtServer(); server != nil && peerIP != nil {
		server.AddNode(&net.UDPAddr{IP: peerIP, Port: int(port)})
	}
}
