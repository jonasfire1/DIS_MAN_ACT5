package main

import (
	"context"
	"net"
	"os"
	"strconv"

	pb "handin5/gRPC"
	"log"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Node struct {
	pb.UnimplementedAuctionServer
	Mutex      sync.Mutex
	Clients    []pb.AuctionClient
	id         int
	currentPRM int
}

func main() {

	if len(os.Args) < 2 {
		log.Fatal("Run the client with: go run ./client <node_id>")
	}

	node := &Node{
		// initialize array code goes here
	}
	var node_address string

	val, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatal("Run the client with: go run ./client <node_id> (an integer 1 through 4)")
	}

	switch val {
	case 0:
		node.id = 0
		node_address = ":5050"

		conn1, err := grpc.NewClient(
			"localhost:5051",
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			log.Fatal(err)
		}
		node.Clients[1] = pb.NewAuctionClient(conn1)

		conn2, err := grpc.NewClient(
			"localhost:5052",
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			log.Fatal(err)
		}
		node.Clients[2] = pb.NewAuctionClient(conn2)

		conn3, err := grpc.NewClient(
			"localhost:5053",
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			log.Fatal(err)
		}
		node.Clients[3] = pb.NewAuctionClient(conn3)

	case 1:
		node.id = 1
		node_address = ":5051"

		conn0, err := grpc.NewClient(
			"localhost:5050",
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			log.Fatal(err)
		}
		node.Clients[0] = pb.NewAuctionClient(conn0)

		conn2, err := grpc.NewClient(
			"localhost:5052",
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			log.Fatal(err)
		}
		node.Clients[2] = pb.NewAuctionClient(conn2)

		conn3, err := grpc.NewClient(
			"localhost:5053",
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			log.Fatal(err)
		}
		node.Clients[3] = pb.NewAuctionClient(conn3)
	case 2:
		node.id = 2
		node_address = ":5052"

		conn0, err := grpc.NewClient(
			"localhost:5050",
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			log.Fatal(err)
		}
		node.Clients[0] = pb.NewAuctionClient(conn0)

		conn1, err := grpc.NewClient(
			"localhost:5051",
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			log.Fatal(err)
		}
		node.Clients[1] = pb.NewAuctionClient(conn1)

		conn3, err := grpc.NewClient(
			"localhost:5053",
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			log.Fatal(err)
		}
		node.Clients[3] = pb.NewAuctionClient(conn3)
	case 3:
		node.id = 3
		node_address = ":5053"

		conn0, err := grpc.NewClient(
			"localhost:5050",
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			log.Fatal(err)
		}
		node.Clients[0] = pb.NewAuctionClient(conn0)

		conn1, err := grpc.NewClient(
			"localhost:5051",
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			log.Fatal(err)
		}
		node.Clients[1] = pb.NewAuctionClient(conn1)

		conn2, err := grpc.NewClient(
			"localhost:5052",
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			log.Fatal(err)
		}
		node.Clients[2] = pb.NewAuctionClient(conn2)

	default:
		log.Fatal("the provided node identifier is invalid")
	}

	listener, err := net.Listen("tcp", node_address)
	if err != nil {
		log.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterAuctionServer(grpcServer, node)

	go func() {
		log.Println("Server SERVER_STARTED Port=" + node_address)

		if err := grpcServer.Serve(listener); err != nil {
			log.Println(err)
		}
	}()
}

// call all clients with greater id than self, and if no responses are recieved, declare self new leader
func (n *Node) StartElection() {
	var peersChecked sync.WaitGroup
	peersChecked.Add(len(n.Clients) - n.id - 1) // the amount of ids higher
	higherFound := false
	for i := n.id; i < len(n.Clients); i++ {
		go func() {
			defer peersChecked.Done()
			response, err := n.Clients[0].Election(context.Background(), nil)
			if err == nil && response != nil {
				higherFound = true
			}
		}()
	}
	peersChecked.Wait()
	if higherFound == false {
		// code for winning election goes here
	}
}
