package node

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	pb "mand4-consensus/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type NodeState int

const (
	RELEASED NodeState = iota 
	WANTED                    
	HELD                      
)

type Node struct {
	pb.UnimplementedMutualExclusionServer

	id          int32
	port        int
	lamport     int64
	state       NodeState
	peers       map[int32]string 
	peerClients map[int32]pb.MutualExclusionClient

	repliesReceived int
	requestTime     int64

	mu sync.Mutex 

	deferredReplies []int32
}

func NewNode(id int32, port int, peers map[int32]string) *Node {
	return &Node{
		id:              id,
		port:            port,
		lamport:         0,
		state:           RELEASED,
		peers:           peers,
		peerClients:     make(map[int32]pb.MutualExclusionClient),
		deferredReplies: make([]int32, 0),
	}
}

func (n *Node) Start() error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", n.port))
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterMutualExclusionServer(grpcServer, n)

	go func() {
		log.Printf("[Node %d] Starting server on port %d", n.id, n.port)
		if err := grpcServer.Serve(listener); err != nil {
			log.Fatalf("[Node %d] Server failed: %v", n.id, err)
		}
	}()

	time.Sleep(500 * time.Millisecond)

	return n.connectToPeers()
}

func (n *Node) connectToPeers() error {
	for peerID, address := range n.peers {
		log.Printf("[Node %d] Connecting to peer %d at %s", n.id, peerID, address)

		conn, err := grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("failed to connect to peer %d: %v", peerID, err)
		}

		n.peerClients[peerID] = pb.NewMutualExclusionClient(conn)
		log.Printf("[Node %d] Connected to peer %d", n.id, peerID)
	}

	return nil
}

func (n *Node) incrementLamport() {
	n.lamport++
}

func (n *Node) updateLamport(receivedTime int64) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if receivedTime > n.lamport {
		n.lamport = receivedTime
	}
	n.lamport++
}

func (n *Node) Request(ctx context.Context, req *pb.RequestMessage) (*pb.ReplyMessage, error) {
	n.updateLamport(req.Timestamp)

	n.mu.Lock()
	currentLamport := n.lamport
	currentState := n.state
	myRequestTime := n.requestTime
	myID := n.id
	n.mu.Unlock()

	log.Printf("[Node %d][Lamport: %d] Received REQUEST from Node %d with timestamp %d",
		myID, currentLamport, req.NodeId, req.Timestamp)

	shouldReply := false

	if currentState == RELEASED {
		shouldReply = true
		log.Printf("[Node %d][Lamport: %d] State is RELEASED, replying immediately to Node %d",
			myID, currentLamport, req.NodeId)
	} else if currentState == WANTED {
		if req.Timestamp < myRequestTime || (req.Timestamp == myRequestTime && req.NodeId < myID) {
			shouldReply = true
			log.Printf("[Node %d][Lamport: %d] Other request has priority, replying immediately to Node %d",
				myID, currentLamport, req.NodeId)
		} else {
			shouldReply = false
			log.Printf("[Node %d][Lamport: %d] Our request has priority, deferring reply to Node %d",
				myID, currentLamport, req.NodeId)

			n.mu.Lock()
			n.deferredReplies = append(n.deferredReplies, req.NodeId)
			n.mu.Unlock()
		}
	} else if currentState == HELD {
		shouldReply = false
		log.Printf("[Node %d][Lamport: %d] In critical section, deferring reply to Node %d",
			myID, currentLamport, req.NodeId)

		n.mu.Lock()
		n.deferredReplies = append(n.deferredReplies, req.NodeId)
		n.mu.Unlock()
	}

	if shouldReply {
		n.mu.Lock()
		n.incrementLamport()
		replyTime := n.lamport
		n.mu.Unlock()

		log.Printf("[Node %d][Lamport: %d] Sending REPLY to Node %d",
			myID, replyTime, req.NodeId)

		return &pb.ReplyMessage{
			NodeId:    myID,
			Timestamp: replyTime,
			Granted:   true,
		}, nil
	}

	// For deferred replies, we'll send them later when exiting CS
	// For now, just return an empty reply, need to handle this differently in full implementation
	return &pb.ReplyMessage{
		NodeId:    myID,
		Timestamp: currentLamport,
		Granted:   false,
	}, nil
}

func (n *Node) RequestCriticalSection() {
	n.mu.Lock()
	n.state = WANTED
	n.incrementLamport()
	n.requestTime = n.lamport
	n.repliesReceived = 0
	requestTime := n.requestTime
	n.mu.Unlock()

	log.Printf("[Node %d][Lamport: %d] Requesting access to critical section",
		n.id, requestTime)

	// Send REQUEST to all peers
	for peerID, client := range n.peerClients {
		log.Printf("[Node %d][Lamport: %d] Sending REQUEST to Node %d",
			n.id, requestTime, peerID)

		go func(pid int32, c pb.MutualExclusionClient) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			reply, err := c.Request(ctx, &pb.RequestMessage{
				NodeId:    n.id,
				Timestamp: requestTime,
			})

			if err != nil {
				log.Printf("[Node %d] Error sending request to Node %d: %v",
					n.id, pid, err)
				return
			}

			if reply.Granted {
				n.mu.Lock()
				n.updateLamport(reply.Timestamp)
				n.repliesReceived++
				currentReplies := n.repliesReceived
				n.mu.Unlock()

				log.Printf("[Node %d][Lamport: %d] Received REPLY from Node %d (%d/%d)",
					n.id, reply.Timestamp, pid, currentReplies, len(n.peers))
			}
		}(peerID, client)
	}

	n.waitForAllReplies()

	n.mu.Lock()
	n.state = HELD
	n.mu.Unlock()

	n.enterCriticalSection()

	n.exitCriticalSection()
}

func (n *Node) waitForAllReplies() {
	for {
		n.mu.Lock()
		received := n.repliesReceived
		needed := len(n.peers)
		n.mu.Unlock()

		if received >= needed {
			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	log.Printf("[Node %d] Received all replies, entering critical section", n.id)
}

func (n *Node) enterCriticalSection() {
	n.mu.Lock()
	currentLamport := n.lamport
	n.mu.Unlock()

	log.Printf("[Node %d][Lamport: %d] ====== ENTERING CRITICAL SECTION ======", n.id, currentLamport)
	fmt.Printf(">>> Node %d is in CRITICAL SECTION <<<\n", n.id)

	time.Sleep(2 * time.Second)
}

func (n *Node) exitCriticalSection() {
	n.mu.Lock()
	n.state = RELEASED
	n.incrementLamport()
	currentLamport := n.lamport
	deferredList := n.deferredReplies
	n.deferredReplies = make([]int32, 0) 
	n.mu.Unlock()

	log.Printf("[Node %d][Lamport: %d] ====== EXITING CRITICAL SECTION ======", n.id, currentLamport)

	for _, peerID := range deferredList {
		log.Printf("[Node %d][Lamport: %d] Sending deferred REPLY to Node %d",
			n.id, currentLamport, peerID)

	}
}
