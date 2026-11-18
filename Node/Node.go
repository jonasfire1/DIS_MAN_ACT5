package main

import (
	"context"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	pb "handin5/gRPC"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	numberOfNodes             = 4
	firstNodePort             = 5050
	rpcTimeout                = 1 * time.Second
	leaderAnnouncementTimeout = 2 * time.Second
)

type Node struct {
	pb.UnimplementedAuctionServer
	Mutex sync.Mutex
	Clock int32

	Clients     [numberOfNodes]pb.AuctionClient
	Connections [numberOfNodes]*grpc.ClientConn

	id              int
	currentLeader   int
	electionOngoing bool
}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Run the node with: go run ./Node <node_id>")
	}

	nodeID, err := strconv.Atoi(os.Args[1])
	if err != nil || nodeID < 0 || nodeID >= numberOfNodes {
		log.Fatal("The node identifier must be an integer from 0 through 3")
	}

	node := &Node{
		id:            nodeID,
		Clock:         0,
		currentLeader: -1,
	}

	nodeAddress := ":" + strconv.Itoa(firstNodePort+node.id)

	connectToPeers(node)
	defer closeConnections(node)

	listener, err := net.Listen("tcp", nodeAddress)
	if err != nil {
		log.Fatalf("Could not listen on %s: %v", nodeAddress, err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterAuctionServer(grpcServer, node)

	node.Mutex.Lock()
	node.Clock++
	log.Printf(
		"logical time %d: node %d started on port %d",
		node.Clock,
		node.id,
		firstNodePort+node.id,
	)
	node.Mutex.Unlock()

	go func() {
		time.Sleep(500 * time.Millisecond)
		node.StartElection()
	}()

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatal(err)
	}
}

func connectToPeers(node *Node) {
	for peerID := 0; peerID < numberOfNodes; peerID++ {
		if peerID == node.id {
			continue
		}

		address := "localhost:" + strconv.Itoa(firstNodePort+peerID)

		connection, err := grpc.NewClient(
			address,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			log.Fatalf(
				"Could not create connection from node %d to node %d: %v",
				node.id,
				peerID,
				err,
			)
		}

		node.Connections[peerID] = connection
		node.Clients[peerID] = pb.NewAuctionClient(connection)
	}
}

func closeConnections(node *Node) {
	for peerID, connection := range node.Connections {
		if connection == nil {
			continue
		}

		if err := connection.Close(); err != nil {
			log.Printf(
				"Could not close connection to node %d: %v",
				peerID,
				err,
			)
		}
	}
}

// StartElection implements the Bully election algorithm.
//
// The node sends election messages to all nodes with higher IDs.
// If none respond, this node declares itself the leader.
// If a higher node responds, this node waits for that node or another
// higher node to announce the new leader.
func (n *Node) StartElection() {
	n.Mutex.Lock()

	if n.electionOngoing {
		n.Mutex.Unlock()
		return
	}

	n.electionOngoing = true
	n.currentLeader = -1
	n.Clock++

	log.Printf(
		"logical time %d: node %d started an election",
		n.Clock,
		n.id,
	)

	n.Mutex.Unlock()

	numberOfHigherNodes := numberOfNodes - n.id - 1

	if numberOfHigherNodes == 0 {
		n.AnnounceLeadership()
		return
	}

	responses := make(chan bool, numberOfHigherNodes)

	for peerID := n.id + 1; peerID < numberOfNodes; peerID++ {
		go n.sendElectionRequest(peerID, responses)
	}

	higherNodeResponded := false

	for checked := 0; checked < numberOfHigherNodes; checked++ {
		if <-responses {
			higherNodeResponded = true
		}
	}

	if !higherNodeResponded {
		n.AnnounceLeadership()
		return
	}

	n.Mutex.Lock()
	n.Clock++

	log.Printf(
		"logical time %d: node %d received a response from a higher node and is waiting for a leader announcement",
		n.Clock,
		n.id,
	)

	n.Mutex.Unlock()

	go n.waitForLeaderAnnouncement()
}

func (n *Node) sendElectionRequest(peerID int, responses chan<- bool) {
	n.Mutex.Lock()
	n.Clock++

	request := &pb.IdentifierMSG{
		Id:    int32(n.id),
		Clock: n.Clock,
	}

	log.Printf(
		"logical time %d: node %d sent an election request to node %d",
		n.Clock,
		n.id,
		peerID,
	)

	n.Mutex.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()

	response, err := n.Clients[peerID].Election(ctx, request)
	if err != nil {
		n.Mutex.Lock()
		n.Clock++

		log.Printf(
			"logical time %d: election request from node %d to node %d timed out",
			n.Clock,
			n.id,
			peerID,
		)

		n.Mutex.Unlock()

		responses <- false
		return
	}

	n.Mutex.Lock()
	n.Clock = max(n.Clock, response.GetClock()) + 1

	log.Printf(
		"logical time %d: node %d received an election response from node %d",
		n.Clock,
		n.id,
		peerID,
	)

	n.Mutex.Unlock()

	responses <- true
}

func (n *Node) waitForLeaderAnnouncement() {
	timer := time.NewTimer(leaderAnnouncementTimeout)
	defer timer.Stop()

	<-timer.C

	n.Mutex.Lock()

	if n.currentLeader != -1 {
		n.electionOngoing = false
		n.Mutex.Unlock()
		return
	}

	n.electionOngoing = false
	n.Clock++

	log.Printf(
		"logical time %d: node %d did not receive a leader announcement and will start a new election",
		n.Clock,
		n.id,
	)

	n.Mutex.Unlock()

	n.StartElection()
}

func (n *Node) AnnounceLeadership() {
	n.Mutex.Lock()

	n.electionOngoing = false
	n.currentLeader = n.id
	n.Clock++

	log.Printf(
		"logical time %d: node %d declared itself the leader",
		n.Clock,
		n.id,
	)

	n.Mutex.Unlock()

	for peerID := 0; peerID < numberOfNodes; peerID++ {
		if peerID == n.id {
			continue
		}

		go n.sendLeaderAnnouncement(peerID)
	}
}

func (n *Node) sendLeaderAnnouncement(peerID int) {
	n.Mutex.Lock()
	n.Clock++

	request := &pb.IdentifierMSG{
		Id:    int32(n.id),
		Clock: n.Clock,
	}

	log.Printf(
		"logical time %d: node %d sent a leader announcement to node %d",
		n.Clock,
		n.id,
		peerID,
	)

	n.Mutex.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()

	response, err := n.Clients[peerID].SetnewLeader(ctx, request)
	if err != nil {
		n.Mutex.Lock()
		n.Clock++

		log.Printf(
			"logical time %d: node %d could not announce leadership to node %d",
			n.Clock,
			n.id,
			peerID,
		)

		n.Mutex.Unlock()
		return
	}

	n.Mutex.Lock()
	n.Clock = max(n.Clock, response.GetClock()) + 1

	log.Printf(
		"logical time %d: node %d received a leader acknowledgement from node %d",
		n.Clock,
		n.id,
		peerID,
	)

	n.Mutex.Unlock()
}

func (n *Node) Election(
	ctx context.Context,
	msg *pb.IdentifierMSG,
) (*pb.AcknowledgementMSG, error) {
	n.Mutex.Lock()

	n.Clock = max(n.Clock, msg.GetClock()) + 1

	log.Printf(
		"logical time %d: node %d received an election request from node %d",
		n.Clock,
		n.id,
		msg.GetId(),
	)

	shouldStartElection := !n.electionOngoing

	n.Clock++
	responseClock := n.Clock

	log.Printf(
		"logical time %d: node %d sent an election response to node %d",
		n.Clock,
		n.id,
		msg.GetId(),
	)

	n.Mutex.Unlock()

	if shouldStartElection {
		go n.StartElection()
	}

	return &pb.AcknowledgementMSG{
		Clock: responseClock,
	}, nil
}

func (n *Node) SetnewLeader(
	ctx context.Context,
	msg *pb.IdentifierMSG,
) (*pb.AcknowledgementMSG, error) {
	n.Mutex.Lock()

	n.Clock = max(n.Clock, msg.GetClock()) + 1
	n.currentLeader = int(msg.GetId())
	n.electionOngoing = false

	log.Printf(
		"logical time %d: node %d accepted node %d as the new leader",
		n.Clock,
		n.id,
		msg.GetId(),
	)

	n.Clock++
	responseClock := n.Clock

	n.Mutex.Unlock()

	return &pb.AcknowledgementMSG{
		Clock: responseClock,
	}, nil
}
