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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	numberOfNodes             = 4
	firstNodePort             = 5050
	rpcTimeout                = 1 * time.Second
	leaderAnnouncementTimeout = 2 * time.Second
	heartbeatInterval         = 1 * time.Second
	heartbeatFailureThreshold = 3
	auctionDuration           = 100 * time.Second
)

type Node struct {
	pb.UnimplementedAuctionServer
	Mutex sync.Mutex
	Clock int32

	Clients     [numberOfNodes]pb.AuctionClient
	Connections [numberOfNodes]*grpc.ClientConn

	id              int
	currentLeader   int
	electionOngoing bool // yes keeping track of system state this way is silly but we're too lazy to change it
	synchOngoing    bool
	leaderTerm      int64

	highestBid            int32
	highestBidder         string
	auctionDeadlineUnixMs int64
	auctionOver           bool
	stateVersion          int64

	processedRequests map[string]*pb.AcknowledgementMSG
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
		id:                    nodeID,
		Clock:                 0,
		currentLeader:         -1,
		auctionDeadlineUnixMs: time.Now().Add(auctionDuration).UnixMilli(),
		processedRequests:     make(map[string]*pb.AcknowledgementMSG),
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

	go node.MonitorLeader()

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
		n.Synchronize()
		n.UpdateReplicas()
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
		n.Synchronize()
		n.UpdateReplicas()
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
	n.synchOngoing = true
	n.currentLeader = n.id
	n.leaderTerm++

	n.Clock++

	log.Printf(
		"logical time %d: node %d declared itself the leader for term %d",
		n.Clock,
		n.id,
		n.leaderTerm,
	)

	n.Mutex.Unlock()
	var wg sync.WaitGroup
	wg.Add(numberOfNodes - 1)

	for peerID := 0; peerID < numberOfNodes; peerID++ {
		if peerID == n.id {
			continue
		}

		go n.sendLeaderAnnouncement(peerID, &wg)
	}
	wg.Wait()

}

func (n *Node) sendLeaderAnnouncement(peerID int, wg *sync.WaitGroup) {
	defer wg.Done()
	n.Mutex.Lock()
	n.Clock++

	request := &pb.IdentifierMSG{
		Id:         int32(n.id),
		Clock:      n.Clock,
		LeaderTerm: n.leaderTerm,
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

	isCurrentLeader := n.currentLeader == n.id
	shouldStartElection := !n.electionOngoing && n.currentLeader != n.id
	requestingNodeID := int(msg.GetId())
	n.Clock++
	responseClock := n.Clock

	log.Printf(
		"logical time %d: node %d sent an election response to node %d",
		n.Clock,
		n.id,
		msg.GetId(),
	)

	n.Mutex.Unlock()

	if isCurrentLeader {
		go func() {
			var wg sync.WaitGroup
			wg.Add(1)
			n.sendLeaderAnnouncement(requestingNodeID, &wg)
			wg.Wait()
		}()
	} else if shouldStartElection {
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
	defer n.Mutex.Unlock()

	n.Clock = max(n.Clock, msg.GetClock()) + 1
	if msg.GetLeaderTerm() < n.leaderTerm || (msg.GetLeaderTerm() == n.leaderTerm && n.currentLeader >= 0 && int(msg.GetId()) < n.currentLeader) {
		n.Clock++
		return &pb.AcknowledgementMSG{
			Clock:        n.Clock,
			Outcome:      pb.BidOutcome_BID_EXCEPTION,
			Message:      "Rejected stale leader announcement",
			LeaderId:     int32(n.currentLeader),
			StateVersion: n.stateVersion,
		}, nil
	}

	n.currentLeader = int(msg.GetId())
	n.leaderTerm = msg.GetLeaderTerm()
	n.electionOngoing = false
	n.synchOngoing = false

	log.Printf(
		"logical time %d: node %d accepted node %d as the new leader for term %d",
		n.Clock,
		n.id,
		msg.GetId(),
		n.leaderTerm,
	)

	n.Clock++

	return &pb.AcknowledgementMSG{
		Clock:        n.Clock,
		Outcome:      pb.BidOutcome_BID_SUCCESS,
		Message:      "Leader announcement accepted",
		LeaderId:     int32(n.currentLeader),
		StateVersion: n.stateVersion,
	}, nil
}

func (n *Node) Bid(ctx context.Context, msg *pb.BidMSG) (*pb.AcknowledgementMSG, error) {
	n.Mutex.Lock()
	defer n.Mutex.Unlock()

	n.Clock = max(n.Clock, msg.GetClock()) + 1

	log.Printf(
		"logical time %d: node %d received bid %d from bidder %q",
		n.Clock,
		n.id,
		msg.GetAmount(),
		msg.GetBidderId(),
	)

	if n.currentLeader != n.id {
		n.Clock++

		return &pb.AcknowledgementMSG{
			Clock:        n.Clock,
			Outcome:      pb.BidOutcome_BID_EXCEPTION,
			Message:      "This node is not the leader",
			LeaderId:     int32(n.currentLeader),
			StateVersion: n.stateVersion,
		}, nil
	}

	if n.synchOngoing {
		n.Clock++

		return &pb.AcknowledgementMSG{
			Clock:        n.Clock,
			Outcome:      pb.BidOutcome_BID_EXCEPTION,
			Message:      "The leader is synchronizing with other nodes. Please try again later.",
			LeaderId:     int32(n.currentLeader),
			StateVersion: n.stateVersion,
		}, nil
	}

	if msg.GetBidderId() == "" {
		n.Clock++

		return &pb.AcknowledgementMSG{
			Clock:        n.Clock,
			Outcome:      pb.BidOutcome_BID_FAIL,
			Message:      "A bidder ID is required",
			LeaderId:     int32(n.currentLeader),
			StateVersion: n.stateVersion,
		}, nil
	}

	if msg.GetRequestId() == "" {
		n.Clock++

		return &pb.AcknowledgementMSG{
			Clock:        n.Clock,
			Outcome:      pb.BidOutcome_BID_FAIL,
			Message:      "A request ID is required",
			LeaderId:     int32(n.currentLeader),
			StateVersion: n.stateVersion,
		}, nil
	}

	if previousResponse, found := n.processedRequests[msg.GetRequestId()]; found {
		n.Clock++

		return &pb.AcknowledgementMSG{
			Clock:        n.Clock,
			Outcome:      previousResponse.GetOutcome(),
			Message:      previousResponse.GetMessage(),
			LeaderId:     previousResponse.GetLeaderId(),
			StateVersion: previousResponse.GetStateVersion(),
		}, nil
	}

	n.updateAuctionStatusLocked()

	if n.auctionOver {
		n.Clock++

		response := &pb.AcknowledgementMSG{
			Clock:        n.Clock,
			Outcome:      pb.BidOutcome_BID_FAIL,
			Message:      "The auction is over",
			LeaderId:     int32(n.currentLeader),
			StateVersion: n.stateVersion,
		}

		n.processedRequests[msg.GetRequestId()] = response
		return response, nil
	}

	if msg.GetAmount() <= n.highestBid {
		n.Clock++

		response := &pb.AcknowledgementMSG{
			Clock:        n.Clock,
			Outcome:      pb.BidOutcome_BID_FAIL,
			Message:      "The bid must be higher than the current highest bid",
			LeaderId:     int32(n.currentLeader),
			StateVersion: n.stateVersion,
		}

		n.processedRequests[msg.GetRequestId()] = response
		return response, nil
	}

	n.highestBid = msg.GetAmount()
	n.highestBidder = msg.GetBidderId()
	n.stateVersion++
	n.Clock++

	response := &pb.AcknowledgementMSG{
		Clock:        n.Clock,
		Outcome:      pb.BidOutcome_BID_SUCCESS,
		Message:      "The bid was accepted",
		LeaderId:     int32(n.currentLeader),
		StateVersion: n.stateVersion,
	}

	n.processedRequests[msg.GetRequestId()] = response

	acceptedClock := n.Clock
	acceptedBid := n.highestBid
	acceptedBidder := n.highestBidder
	acceptedVersion := n.stateVersion

	n.Mutex.Unlock()

	n.UpdateReplicas()
	n.Mutex.Lock()
	log.Printf(
		"logical time %d: node %d accepted bid %d from bidder %q as state version %d",
		acceptedClock,
		n.id,
		acceptedBid,
		acceptedBidder,
		acceptedVersion,
	)

	return response, nil
}

func (n *Node) Result(
	ctx context.Context,
	msg *pb.AcknowledgementMSG,
) (*pb.ResultMSG, error) {
	n.Mutex.Lock()
	defer n.Mutex.Unlock()

	n.Clock = max(n.Clock, msg.GetClock()) + 1

	if n.currentLeader != n.id {
		return nil, status.Errorf(
			codes.FailedPrecondition,
			"This node is not the leader",
		)
	}

	n.updateAuctionStatusLocked()

	log.Printf(
		"logical time %d: returned auction state version %d",
		n.Clock,
		n.stateVersion,
	)

	n.Clock++

	return &pb.ResultMSG{
		Clock:        n.Clock,
		Amount:       n.highestBid,
		BidderId:     n.highestBidder,
		AuctionOver:  n.auctionOver,
		LeaderId:     int32(n.currentLeader),
		StateVersion: n.stateVersion,
	}, nil
}

func (n *Node) RequestLeader(
	ctx context.Context,
	msg *pb.AcknowledgementMSG,
) (*pb.IdentifierMSG, error) {
	n.Mutex.Lock()
	defer n.Mutex.Unlock()

	n.Clock = max(n.Clock, msg.GetClock()) + 1

	log.Printf(
		"logical time %d: node %d received a leader information request",
		n.Clock,
		n.id,
	)
	leaderID := n.currentLeader

	if n.electionOngoing || n.synchOngoing {
		leaderID = -1
	}

	n.Clock++

	log.Printf(
		"logical time %d: node %d responded to leader information request with leader %d for term %d",
		n.Clock,
		n.id,
		leaderID,
		n.leaderTerm,
	)

	return &pb.IdentifierMSG{
		Clock:      n.Clock,
		Id:         int32(leaderID),
		LeaderTerm: n.leaderTerm,
	}, nil
}

func (n *Node) Ping(
	ctx context.Context,
	msg *pb.AcknowledgementMSG,
) (*pb.AcknowledgementMSG, error) {
	n.Mutex.Lock()
	defer n.Mutex.Unlock()

	n.Clock = max(n.Clock, msg.GetClock()) + 1
	n.Clock++

	return &pb.AcknowledgementMSG{
		Clock:        n.Clock,
		Outcome:      pb.BidOutcome_BID_SUCCESS,
		Message:      "Node is alive",
		LeaderId:     int32(n.currentLeader),
		StateVersion: n.stateVersion,
	}, nil
}

func (n *Node) updateAuctionStatusLocked() {
	if n.auctionDeadlineUnixMs == 0 {
		return
	}

	if !n.auctionOver &&
		time.Now().UnixMilli() >= n.auctionDeadlineUnixMs {
		n.auctionOver = true

		log.Printf(
			"logical time %d: auction ended with bidder %q winning with bid %d",
			n.Clock,
			n.highestBidder,
			n.highestBid,
		)
	}
}
func (n *Node) MonitorLeader() {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	consecutiveFailures := 0
	monitoredLeader := -1

	for range ticker.C {
		n.Mutex.Lock()

		leaderID := n.currentLeader

		if leaderID == n.id {
			consecutiveFailures = 0
			monitoredLeader = leaderID
			n.Mutex.Unlock()
			continue
		}

		if leaderID < 0 || leaderID >= numberOfNodes {
			consecutiveFailures = 0
			monitoredLeader = leaderID
			n.Mutex.Unlock()
			continue
		}

		if leaderID != monitoredLeader {
			consecutiveFailures = 0
			monitoredLeader = leaderID
		}

		request := &pb.AcknowledgementMSG{
			Clock:        n.Clock,
			LeaderId:     int32(leaderID),
			StateVersion: n.stateVersion,
		}

		n.Mutex.Unlock()

		ctx, cancel := context.WithTimeout(
			context.Background(),
			rpcTimeout,
		)

		response, err := n.Clients[leaderID].Ping(ctx, request)
		cancel()

		if err != nil {
			consecutiveFailures++

			n.Mutex.Lock()
			n.Clock++

			log.Printf(
				"logical time %d: node %d failed to ping leader %d, failure %d of %d",
				n.Clock,
				n.id,
				leaderID,
				consecutiveFailures,
				heartbeatFailureThreshold,
			)

			leaderIsStillCurrent := n.currentLeader == leaderID
			electionAlreadyRunning := n.electionOngoing

			n.Mutex.Unlock()

			if consecutiveFailures >= heartbeatFailureThreshold &&
				leaderIsStillCurrent &&
				!electionAlreadyRunning {
				n.handleLeaderFailure(leaderID)
				consecutiveFailures = 0
				monitoredLeader = -1
			}

			continue
		}

		n.Mutex.Lock()

		if n.currentLeader == leaderID {
			n.Clock = max(n.Clock, response.GetClock()) + 1
		}

		n.Mutex.Unlock()

		consecutiveFailures = 0
	}
}

func (n *Node) handleLeaderFailure(failedLeaderID int) {
	n.Mutex.Lock()

	if n.currentLeader != failedLeaderID || n.electionOngoing {
		n.Mutex.Unlock()
		return
	}

	n.currentLeader = -1
	n.Clock++

	log.Printf(
		"logical time %d: node %d detected that leader %d failed",
		n.Clock,
		n.id,
		failedLeaderID,
	)

	n.Mutex.Unlock()

	n.StartElection()
}

func (n *Node) Update(
	ctx context.Context,
	msg *pb.AuctionStateMSG,
) (*pb.AcknowledgementMSG, error) {
	n.Mutex.Lock()
	defer n.Mutex.Unlock()
	n.Clock = max(n.Clock, msg.GetClock()) + 1
	if msg.GetLeaderId() != int32(n.currentLeader) || msg.GetLeaderTerm() != n.leaderTerm {
		n.Clock++
		return &pb.AcknowledgementMSG{
			Clock:   n.Clock,
			Outcome: pb.BidOutcome_BID_EXCEPTION,
			Message: "Rejected update from an invalid or stale leader",
		}, nil
	}
	if msg.GetStateVersion() < n.stateVersion {
		n.Clock++
		return &pb.AcknowledgementMSG{
			Clock:        n.Clock,
			Outcome:      pb.BidOutcome_BID_FAIL,
			Message:      "Rejected stale auction state",
			LeaderId:     int32(n.currentLeader),
			StateVersion: n.stateVersion,
		}, nil
	}
	log.Printf("logical time %d: recieved auctionstate update from %d", n.Clock, msg.GetLeaderId())
	n.highestBid = msg.GetHighestBid()
	n.highestBidder = msg.GetHighestBidder()
	n.auctionDeadlineUnixMs = msg.GetAuctionDeadlineUnixMs()
	n.auctionOver = msg.GetAuctionOver()
	n.stateVersion = msg.GetStateVersion()
	n.updateAuctionStatusLocked()
	n.Clock++

	log.Printf("logical time %d: sends auctionstate update acknowledgement to node %d", n.Clock, msg.GetLeaderId())
	return &pb.AcknowledgementMSG{
		Clock:        n.Clock,
		Outcome:      pb.BidOutcome_BID_SUCCESS,
		Message:      "Auction state update",
		LeaderId:     int32(n.currentLeader),
		StateVersion: n.stateVersion,
	}, nil
}

func (n *Node) UpdateReplicas() {
	var wg sync.WaitGroup
	wg.Add(numberOfNodes - 1)
	for peerID := 0; peerID < numberOfNodes; peerID++ {
		if peerID == n.id {
			continue
		}
		go n.sendUpdate(peerID, &wg)
	}
	wg.Wait()
}

func (n *Node) sendUpdate(peerID int, wg *sync.WaitGroup) {
	defer wg.Done()
	n.Mutex.Lock()
	n.Clock++

	AuctionStateMSG := &pb.AuctionStateMSG{
		Clock:                 n.Clock,
		HighestBid:            n.highestBid,
		HighestBidder:         n.highestBidder,
		AuctionDeadlineUnixMs: n.auctionDeadlineUnixMs,
		AuctionOver:           n.auctionOver,
		LeaderId:              int32(n.id),
		LeaderTerm:            n.leaderTerm,
		StateVersion:          n.stateVersion,
	}

	log.Printf(
		"logical time %d: sent update to node %d",
		n.Clock,
		peerID,
	)
	n.Mutex.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()

	response, err := n.Clients[peerID].Update(ctx, AuctionStateMSG)
	if err != nil {
		n.Mutex.Lock()
		n.Clock++

		log.Printf(
			"logical time %d: could not update node %d",
			n.Clock,
			peerID,
		)
		n.Mutex.Unlock()
		return
	}

	n.Mutex.Lock()
	n.Clock = max(n.Clock, response.GetClock()) + 1

	if response.GetOutcome() != pb.BidOutcome_BID_SUCCESS {
		log.Printf(
			"logical time %d: node %d rejected update: %s",
			n.Clock,
			peerID,
			response.GetMessage(),
		)
		n.Mutex.Unlock()
		return
	}

	log.Printf(
		"logical time %d: received update acknowledgement from node %d",
		n.Clock,
		peerID,
	)
	n.Mutex.Unlock()
}
func (n *Node) Synchronize() {

	responses := make(chan *pb.AuctionStateMSG, n.id)
	var wg sync.WaitGroup
	wg.Add(n.id) // number of nodes with smaller ids
	for peerID := 0; peerID < n.id; peerID++ {
		go func() {
			defer wg.Done()
			n.Mutex.Lock()
			n.Clock++
			log.Printf(
				"logical time %d: sent Auctionstate synch request to node %d",
				n.Clock,
				peerID,
			)
			msg := &pb.AcknowledgementMSG{Clock: n.Clock}
			n.Mutex.Unlock()

			ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
			defer cancel()
			response, err := n.Clients[peerID].GetState(ctx, msg)
			if err != nil {
				n.Mutex.Lock()
				n.Clock++

				log.Printf(
					"logical time %d: Failed to get state from node %d",
					n.Clock,
					peerID,
				)
				n.Mutex.Unlock()
				responses <- nil
				return
			}

			n.Mutex.Lock()
			n.Clock = max(n.Clock, response.GetClock()) + 1
			log.Printf(
				"logical time %d: recieved state version %d from node %d",
				n.Clock,
				response.StateVersion,
				peerID,
			)
			n.Mutex.Unlock()
			responses <- response
		}()
	}
	wg.Wait()
	var latest *pb.AuctionStateMSG

	n.Mutex.Lock()
	earliestDeadline := n.auctionDeadlineUnixMs
	n.Mutex.Unlock()

	for i := 0; i < n.id; i++ {
		response := <-responses
		if response == nil {
			continue
		}
		if latest == nil || response.GetStateVersion() > latest.GetStateVersion() {
			latest = response
		}
		responseDeadline := response.GetAuctionDeadlineUnixMs()
		if responseDeadline > 0 && (earliestDeadline == 0 || responseDeadline < earliestDeadline) {
			earliestDeadline = responseDeadline
		}
	}
	if latest != nil {

		n.Mutex.Lock()
		n.Clock++
		n.highestBid = latest.GetHighestBid()
		n.highestBidder = latest.GetHighestBidder()
		n.auctionOver = latest.GetAuctionOver()
		n.stateVersion = latest.GetStateVersion()
		if earliestDeadline > 0 {
			n.auctionDeadlineUnixMs = earliestDeadline
		}
		n.updateAuctionStatusLocked()
		log.Printf(
			"logical time %d: Updated auction state version to %d",
			n.Clock,
			n.stateVersion,
		)
		n.Mutex.Unlock()
	}
	n.Mutex.Lock()
	n.synchOngoing = false
	n.Mutex.Unlock()
}

func (n *Node) GetState(
	ctx context.Context,
	msg *pb.AcknowledgementMSG,
) (*pb.AuctionStateMSG, error) {

	n.Mutex.Lock()
	n.Clock = max(n.Clock, msg.GetClock()) + 1
	log.Printf("logical time %d: recieved get state request", n.Clock)
	AuctionStateMSG := &pb.AuctionStateMSG{
		Clock:                 n.Clock,
		HighestBid:            n.highestBid,
		HighestBidder:         n.highestBidder,
		AuctionDeadlineUnixMs: n.auctionDeadlineUnixMs,
		AuctionOver:           n.auctionOver,
		LeaderId:              int32(n.id),
		LeaderTerm:            n.leaderTerm,
		StateVersion:          n.stateVersion,
	}
	n.Mutex.Unlock()
	return AuctionStateMSG, nil
}
