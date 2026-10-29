package dht

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha1"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/hernan/get-torrent/pkg/tracker"
)

var (
	ErrClosed  = errors.New("dht: server closed")
	ErrTimeout = errors.New("dht: query timed out")
	ErrNoNodes = errors.New("dht: no reachable nodes")
)

const (
	// DefaultQueryTimeout bounds the wait for a single KRPC response.
	DefaultQueryTimeout = 4 * time.Second

	// tokenRotation is how often the announce token secret changes; the previous one stays valid.
	tokenRotation = 5 * time.Minute

	// maintenanceInterval drives token rotation, peer expiry and re-bootstrapping.
	maintenanceInterval = time.Minute

	maxPendingQueries = 4096
	maxPacketSize     = 8192
)

// DefaultBootstrap lists well-known public DHT routers.
var DefaultBootstrap = []string{
	"router.bittorrent.com:6881",
	"dht.transmissionbt.com:6881",
	"router.utorrent.com:6881",
	"dht.libtorrent.org:25401",
}

// Config configures a DHT Server.
type Config struct {
	// Addr is the UDP listen address, e.g. ":6881".
	Addr string
	// ID is the local node ID; a random one is generated when zero.
	ID NodeID
	// Bootstrap lists "host:port" routers contacted when the routing table is empty.
	Bootstrap []string
	// Nodes lists "ip:port" addresses of previously known nodes (e.g. from a saved session).
	Nodes []string
	// QueryTimeout overrides DefaultQueryTimeout when non-zero.
	QueryTimeout time.Duration
	// IPv6 runs the node on the separate IPv6 DHT (BEP 0032) instead of the IPv4 one.
	IPv6 bool
}

// Server is a DHT node: it answers KRPC queries and performs lookups. Safe for concurrent use.
type Server struct {
	id           NodeID
	fam          family
	conn         *net.UDPConn
	table        *Table
	peers        *peerStore
	bootstrap    []string
	queryTimeout time.Duration

	mu      sync.Mutex
	pending map[string]*pendingQuery
	seeds   []string
	secret  [2][16]byte // current, previous
	rotated time.Time

	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once
	wg        sync.WaitGroup
}

type pendingQuery struct {
	addr *net.UDPAddr
	ch   chan *message
}

// New starts a DHT node listening on cfg.Addr. Call Bootstrap to join the network.
func New(cfg Config) (*Server, error) {
	fam := ipv4
	if cfg.IPv6 {
		fam = ipv6
	}
	laddr, err := net.ResolveUDPAddr(fam.network, cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("dht: resolving listen address: %w", err)
	}
	conn, err := net.ListenUDP(fam.network, laddr)
	if err != nil {
		return nil, fmt.Errorf("dht: listening on %s: %w", cfg.Addr, err)
	}

	id := cfg.ID
	if id == (NodeID{}) {
		id = RandomID()
	}
	timeout := cfg.QueryTimeout
	if timeout == 0 {
		timeout = DefaultQueryTimeout
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{
		id:           id,
		fam:          fam,
		conn:         conn,
		table:        NewTable(id),
		peers:        newPeerStore(),
		bootstrap:    cfg.Bootstrap,
		queryTimeout: timeout,
		pending:      make(map[string]*pendingQuery),
		seeds:        cfg.Nodes,
		rotated:      time.Now(),
		ctx:          ctx,
		cancel:       cancel,
	}
	_, _ = rand.Read(s.secret[0][:])
	s.secret[1] = s.secret[0]

	s.wg.Add(2)
	go s.readLoop()
	go s.maintain()
	return s, nil
}

// ID returns the local node ID.
func (s *Server) ID() NodeID { return s.id }

// Addr returns the local UDP address.
func (s *Server) Addr() *net.UDPAddr { return s.conn.LocalAddr().(*net.UDPAddr) }

// NumNodes returns the size of the routing table.
func (s *Server) NumNodes() int { return s.table.Len() }

// Nodes returns the addresses of all nodes in the routing table, for persisting across restarts.
func (s *Server) Nodes() []string {
	var out []string
	for _, n := range s.table.All() {
		out = append(out, n.Addr.String())
	}
	return out
}

// LocalPeers returns peers that other nodes announced to us for infoHash.
func (s *Server) LocalPeers(infoHash [20]byte) []tracker.Peer {
	return s.peers.get(infoHash, maxValues, time.Now())
}

// Close shuts the node down and waits for its goroutines to exit.
func (s *Server) Close() error {
	var err error
	s.closeOnce.Do(func() {
		s.cancel()
		err = s.conn.Close()
		s.wg.Wait()
	})
	return err
}

// AddNode pings addr asynchronously; if it answers it joins the routing table.
// Used for nodes learnt from peers' BEP 0005 PORT messages.
func (s *Server) AddNode(addr *net.UDPAddr) {
	if s.fam.ip(addr.IP) == nil {
		return // the other address family's DHT
	}
	go func() { _, _ = s.query(s.ctx, addr, "ping", map[string]any{}) }()
}

func (s *Server) readLoop() {
	defer s.wg.Done()
	buf := make([]byte, maxPacketSize)
	for {
		n, addr, err := s.conn.ReadFromUDP(buf)
		if err != nil {
			if s.ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			continue
		}
		msg, err := parseMessage(buf[:n])
		if err != nil {
			continue // Unparseable datagrams are silently dropped
		}
		switch msg.Y {
		case "q":
			s.handleQuery(msg, addr)
		case "r", "e":
			s.handleResponse(msg, addr)
		}
	}
}

func (s *Server) maintain() {
	defer s.wg.Done()
	ticker := time.NewTicker(maintenanceInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case now := <-ticker.C:
			s.mu.Lock()
			if now.Sub(s.rotated) >= tokenRotation {
				s.secret[1] = s.secret[0]
				_, _ = rand.Read(s.secret[0][:])
				s.rotated = now
			}
			s.mu.Unlock()
			s.peers.expire(now)

			if s.table.Len() < K {
				ctx, cancel := context.WithTimeout(s.ctx, 30*time.Second)
				_ = s.Bootstrap(ctx)
				cancel()
			}
		}
	}
}

func (s *Server) send(m *message, addr *net.UDPAddr) error {
	_, err := s.conn.WriteToUDP(m.encode(), addr)
	return err
}

// token computes the announce token for an IP under the given secret.
func token(secret [16]byte, ip net.IP) string {
	h := sha1.New()
	h.Write(secret[:])
	if ip4 := ip.To4(); ip4 != nil {
		ip = ip4
	}
	h.Write(ip)
	return string(h.Sum(nil))
}

func (s *Server) tokenFor(ip net.IP) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return token(s.secret[0], ip)
}

func (s *Server) validToken(tok string, ip net.IP) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sec := range s.secret {
		if hmac.Equal([]byte(tok), []byte(token(sec, ip))) {
			return true
		}
	}
	return false
}

func (s *Server) handleQuery(msg *message, addr *net.UDPAddr) {
	reply := func(r map[string]any) {
		r["id"] = string(s.id[:])
		_ = s.send(&message{T: msg.T, Y: "r", R: r}, addr)
	}
	fail := func(code int, text string) {
		_ = s.send(&message{T: msg.T, Y: "e", E: &KRPCError{Code: code, Msg: text}}, addr)
	}

	senderID, ok := getID(msg.A, "id")
	if !ok {
		fail(ErrCodeProtocol, "invalid id")
		return
	}

	switch msg.Q {
	case "ping":
		reply(map[string]any{})

	case "find_node":
		target, ok := getID(msg.A, "target")
		if !ok {
			fail(ErrCodeProtocol, "invalid target")
			return
		}
		reply(map[string]any{s.fam.nodesKey: s.fam.encodeNodes(s.table.Closest(target, K))})

	case "get_peers":
		ih, ok := getID(msg.A, "info_hash")
		if !ok {
			fail(ErrCodeProtocol, "invalid info_hash")
			return
		}
		r := map[string]any{"token": s.tokenFor(addr.IP)}
		if peers := s.peers.get(ih, maxValues, time.Now()); len(peers) > 0 {
			values := make([]any, 0, len(peers))
			for _, p := range peers {
				if c := s.fam.encodePeer(p); c != "" {
					values = append(values, c)
				}
			}
			r["values"] = values
		} else {
			r[s.fam.nodesKey] = s.fam.encodeNodes(s.table.Closest(ih, K))
		}
		reply(r)

	case "announce_peer":
		ih, ok := getID(msg.A, "info_hash")
		tok, _ := msg.A["token"].(string)
		if !ok {
			fail(ErrCodeProtocol, "invalid info_hash")
			return
		}
		if !s.validToken(tok, addr.IP) {
			fail(ErrCodeProtocol, "bad token")
			return
		}
		port := addr.Port
		if implied, _ := msg.A["implied_port"].(int64); implied == 0 {
			p, _ := msg.A["port"].(int64)
			if p <= 0 || p > 65535 {
				fail(ErrCodeProtocol, "invalid port")
				return
			}
			port = int(p)
		}
		s.peers.add(ih, tracker.Peer{IP: addr.IP, Port: uint16(port)}, time.Now())
		reply(map[string]any{})

	default:
		fail(ErrCodeMethod, "method unknown")
		return
	}

	// Querying nodes are live; add them unless they declare themselves read-only (BEP 0043).
	if ro, _ := msg.A["ro"].(int64); ro != 1 && senderID != s.id {
		s.table.Seen(NodeInfo{ID: senderID, Addr: addr})
	}
}

func (s *Server) handleResponse(msg *message, addr *net.UDPAddr) {
	s.mu.Lock()
	pq := s.pending[msg.T]
	// Only the node we queried may answer: a mismatched source is spoofed or stale.
	if pq == nil || !sameAddr(pq.addr, addr) {
		s.mu.Unlock()
		return
	}
	delete(s.pending, msg.T)
	s.mu.Unlock()

	if msg.Y == "r" {
		if id, ok := getID(msg.R, "id"); ok && id != s.id {
			s.table.Seen(NodeInfo{ID: id, Addr: addr})
		}
	}
	pq.ch <- msg // buffered; never blocks
}

// query sends a KRPC query and waits for its response values.
func (s *Server) query(ctx context.Context, addr *net.UDPAddr, method string, args map[string]any) (map[string]any, error) {
	args["id"] = string(s.id[:])
	pq := &pendingQuery{addr: addr, ch: make(chan *message, 1)}

	s.mu.Lock()
	if len(s.pending) >= maxPendingQueries {
		s.mu.Unlock()
		return nil, errors.New("dht: too many pending queries")
	}
	var tid string
	for {
		var b [2]byte
		_, _ = rand.Read(b[:])
		if tid = string(b[:]); s.pending[tid] == nil {
			break
		}
	}
	s.pending[tid] = pq
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		if s.pending[tid] == pq {
			delete(s.pending, tid)
		}
		s.mu.Unlock()
	}()

	if err := s.send(&message{T: tid, Y: "q", Q: method, A: args}, addr); err != nil {
		return nil, fmt.Errorf("dht: sending %s to %s: %w", method, addr, err)
	}

	timer := time.NewTimer(s.queryTimeout)
	defer timer.Stop()
	select {
	case resp := <-pq.ch:
		if resp.Y == "e" {
			return nil, resp.E
		}
		if _, ok := getID(resp.R, "id"); !ok {
			return nil, fmt.Errorf("%w: response without id", ErrMalformed)
		}
		return resp.R, nil
	case <-timer.C:
		s.table.Failed(addr)
		return nil, ErrTimeout
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.ctx.Done():
		return nil, ErrClosed
	}
}

// Ping checks whether the node at addr is alive, adding it to the routing table if so.
func (s *Server) Ping(ctx context.Context, addr *net.UDPAddr) error {
	_, err := s.query(ctx, addr, "ping", map[string]any{})
	return err
}

// Bootstrap joins the network: it contacts the configured routers and saved nodes, then looks up
// our own ID to populate the routing table with nearby nodes.
func (s *Server) Bootstrap(ctx context.Context) error {
	s.mu.Lock()
	hosts := append(append([]string(nil), s.bootstrap...), s.seeds...)
	s.mu.Unlock()

	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		found []NodeInfo
	)
	for _, h := range hosts {
		wg.Add(1)
		go func(h string) {
			defer wg.Done()
			addr, err := net.ResolveUDPAddr(s.fam.network, h)
			if err != nil {
				return
			}
			r, err := s.query(ctx, addr, "find_node", map[string]any{"target": string(s.id[:])})
			if err != nil {
				return
			}
			nodes, _ := r[s.fam.nodesKey].(string)
			mu.Lock()
			found = append(found, s.fam.decodeNodes(nodes)...)
			mu.Unlock()
		}(h)
	}
	wg.Wait()

	s.lookup(ctx, s.id, "find_node", found)
	if s.table.Len() == 0 {
		return ErrNoNodes
	}
	log.Printf("[DHT] Bootstrapped: %d nodes in routing table", s.table.Len())
	return nil
}
