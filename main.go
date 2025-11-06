package main

import (
	"flag"
	"fmt"
	"log"
	"mand4-consensus/node"
	"os"
	"time"
)

func main() {
	nodeID := flag.Int("id", 1, "Node ID")
	port := flag.Int("port", 5001, "Port to listen on")
	flag.Parse()

	logFile, err := os.Create(fmt.Sprintf("node%d.log", *nodeID))
	if err != nil {
		log.Fatalf("Failed to create log file: %v", err)
	}
	defer logFile.Close()
	log.SetOutput(logFile)

	peers := make(map[int32]string)
	if *nodeID == 1 {
		peers[2] = "localhost:5002"
	} else if *nodeID == 2 {
		peers[1] = "localhost:5001"
	}

	n := node.NewNode(int32(*nodeID), *port, peers)

	fmt.Printf("Node %d starting on port %d...\n", *nodeID, *port)

	if err := n.Start(); err != nil {
		log.Fatalf("Failed to start node: %v", err)
	}

	fmt.Printf("Node %d ready. Connected to %d peer(s)\n", *nodeID, len(peers))

	// Wait 2 seconds for the nodes
	time.Sleep(2 * time.Second)

	if *nodeID == 1 {
		time.Sleep(1 * time.Second)
		fmt.Printf("\n[Node %d] Attempting to enter critical section...\n", *nodeID)
		n.RequestCriticalSection()
		fmt.Printf("[Node %d] Finished with critical section\n\n", *nodeID)
	}

	if *nodeID == 2 {
		time.Sleep(3 * time.Second)
		fmt.Printf("\n[Node %d] Attempting to enter critical section...\n", *nodeID)
		n.RequestCriticalSection()
		fmt.Printf("[Node %d] Finished with critical section\n\n", *nodeID)
	}

	fmt.Printf("Node %d completed its critical section access\n", *nodeID)
	select {}
}
