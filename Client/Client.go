package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	pb "handin5/gRPC"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	numberOfNodes        = 4
	firstNodePort        = 5050
	rpcTimeout           = 1 * time.Second
	retryDelay           = 1 * time.Second
	maxLeaderDiscoveries = 8
)

type AuctionClient struct {
	bidderID       string
	clock          int32
	lastKnowLeader int
	clients        [numberOfNodes]pb.AuctionClient
	connections    [numberOfNodes]*grpc.ClientConn
}

func main() {
	if len(os.Args) < 3 {
		printUsage()
	}

	bidderID := os.Args[1]

	operation := strings.ToLower(os.Args[2])

	client := &AuctionClient{
		bidderID: bidderID,
	}

	client.connectToNodes()
	defer client.closeConnections()

	switch operation {
	case "bid":
		if len(os.Args) != 4 {
			log.Fatal("Bid usage: go run ./Client <bidder_id> bid <amount>")
		}

		amount, err := strconv.Atoi(os.Args[3])
		if err != nil {
			log.Fatal("The bid amount must be an integer")
		}

		if amount <= 0 {
			log.Fatal("The bid amount must be greater than zero")
		}

		client.submitBid(int32(amount))

	case "result":
		if len(os.Args) != 3 {
			log.Fatal("Result usage: go run ./Client <bidder_id> result")
		}

		client.requestResult()

	default:
		printUsage()
	}
}

func printUsage() {
	fmt.Println("Usage:")
	fmt.Println("  go run ./Client <bidder_id> <known_node_id> bid <amount>")
	fmt.Println("  go run ./Client <bidder_id> <known_node_id> result")
	fmt.Println()
	fmt.Println("Examples:")
	fmt.Println("  go run ./Client alice 0 bid 500")
	fmt.Println("  go run ./Client alice 1 result")
	os.Exit(1)
}

func (c *AuctionClient) connectToNodes() {
	for nodeID := 0; nodeID < numberOfNodes; nodeID++ {
		address := "localhost:" + strconv.Itoa(firstNodePort+nodeID)

		connection, err := grpc.NewClient(
			address,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			log.Fatalf(
				"Could not create connection to node %d: %v",
				nodeID,
				err,
			)
		}

		c.connections[nodeID] = connection
		c.clients[nodeID] = pb.NewAuctionClient(connection)
	}
}

func (c *AuctionClient) closeConnections() {
	for nodeID, connection := range c.connections {
		if connection == nil {
			continue
		}

		if err := connection.Close(); err != nil {
			log.Printf(
				"Could not close connection to node %d: %v",
				nodeID,
				err,
			)
		}
	}
}

func (c *AuctionClient) discoverLeader() (int, error) {
	nodeOrder := c.nodeSearchOrder(int(c.lastKnowLeader))

	for attempt := 1; attempt <= maxLeaderDiscoveries; attempt++ {
		for _, nodeID := range nodeOrder {

			ctx, cancel := context.WithTimeout(
				context.Background(),
				rpcTimeout,
			)

			response, err := c.clients[nodeID].RequestLeader(
				ctx,
				&pb.AcknowledgementMSG{},
			)

			cancel()

			if err != nil {
				continue
			}

			leaderID := int(response.GetId())
			c.lastKnowLeader = leaderID

			if leaderID >= 0 && leaderID < numberOfNodes {
				return leaderID, nil
			}
		}

		if attempt < maxLeaderDiscoveries {
			fmt.Printf(
				"No leader available yet. Waiting for election, attempt %d of %d...\n",
				attempt,
				maxLeaderDiscoveries,
			)

			time.Sleep(retryDelay)
		}
	}

	return -1, fmt.Errorf(
		"could not discover a leader after %d attempts",
		maxLeaderDiscoveries,
	)
}

func (c *AuctionClient) nodeSearchOrder(knownNodeID int) []int {
	nodeOrder := []int{knownNodeID}

	for nodeID := 0; nodeID < numberOfNodes; nodeID++ {
		if nodeID != knownNodeID {
			nodeOrder = append(nodeOrder, nodeID)
		}
	}

	return nodeOrder
}

func (c *AuctionClient) submitBid(amount int32) {
	requestID := fmt.Sprintf(
		"%s-%d",
		c.bidderID,
		time.Now().UnixNano(),
	)

	for attempt := 1; attempt <= maxLeaderDiscoveries; attempt++ {
		leaderID, err := c.discoverLeader()
		if err != nil {
			log.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(
			context.Background(),
			rpcTimeout,
		)

		response, err := c.clients[leaderID].Bid(
			ctx,
			&pb.BidMSG{
				Clock:     c.clock,
				Amount:    amount,
				BidderId:  c.bidderID,
				RequestId: requestID,
			},
		)

		cancel()

		if err != nil {
			fmt.Printf(
				"Leader node %d did not respond. Waiting for a new leader...\n",
				leaderID,
			)

			time.Sleep(retryDelay)
			continue
		}

		c.clock = max(c.clock, response.GetClock()) + 1

		if response.GetOutcome() == pb.BidOutcome_BID_EXCEPTION {
			fmt.Printf(
				"Node %d could not process the bid: %s\n",
				leaderID,
				response.GetMessage(),
			)

			time.Sleep(retryDelay)
			/*
				if response.GetLeaderId() >= 0 && response.GetLeaderId() < numberOfNodes {
					knownNodeID = int(response.GetLeaderId())
				} else {
					knownNodeID = nextNodeID(leaderID)
				}*/

			continue
		}

		printBidResponse(response, amount, c.bidderID)
		return
	}

	log.Fatal("The bid could not be completed after repeated attempts")
}

func (c *AuctionClient) requestResult() {
	for attempt := 1; attempt <= maxLeaderDiscoveries; attempt++ {
		leaderID, err := c.discoverLeader()
		if err != nil {
			log.Fatal(err)
		}

		c.clock++

		ctx, cancel := context.WithTimeout(
			context.Background(),
			rpcTimeout,
		)

		response, err := c.clients[leaderID].Result(
			ctx,
			&pb.AcknowledgementMSG{
				Clock:    c.clock,
				LeaderId: int32(leaderID),
			},
		)

		cancel()

		if err != nil {
			fmt.Printf(
				"Leader node %d did not respond. Waiting for a new leader...\n",
				leaderID,
			)

			time.Sleep(retryDelay)
			continue
		}

		c.clock = max(c.clock, response.GetClock()) + 1
		printResult(response)
		return
	}

	log.Fatal("The auction result could not be retrieved after repeated attempts")
}

func printBidResponse(
	response *pb.AcknowledgementMSG,
	amount int32,
	bidderID string,
) {
	switch response.GetOutcome() {
	case pb.BidOutcome_BID_SUCCESS:
		fmt.Println("Bid accepted")
	case pb.BidOutcome_BID_FAIL:
		fmt.Println("Bid rejected")
	default:
		fmt.Println("Bid outcome unknown")
	}

	fmt.Printf("Bidder: %s\n", bidderID)
	fmt.Printf("Amount: %d\n", amount)
	fmt.Printf("Message: %s\n", response.GetMessage())
}

func printResult(response *pb.ResultMSG) {
	if response.GetBidderId() == "" {
		fmt.Println("No bids have been accepted yet")
	} else if response.GetAuctionOver() {
		fmt.Println("Auction over")
		fmt.Printf("Winner: %s\n", response.GetBidderId())
		fmt.Printf("Winning bid: %d\n", response.GetAmount())
	} else {
		fmt.Println("Auction still running")
		fmt.Printf("Highest bidder: %s\n", response.GetBidderId())
		fmt.Printf("Highest bid: %d\n", response.GetAmount())
	}

	fmt.Printf("Leader: node %d\n", response.GetLeaderId())
	fmt.Printf("State version: %d\n", response.GetStateVersion())
}

func nextNodeID(nodeID int) int {
	return (nodeID + 1) % numberOfNodes
}
