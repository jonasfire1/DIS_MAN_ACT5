package main

import (
	"context"
	"net"
	"os"
	"strconv"
	"time"

	pb "handin5/gRPC"
	"log"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Node struct {
	pb.UnimplementedAuctionServer
	Mutex      sync.Mutex
	Clients    [4]pb.AuctionClient
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
			log.Fatal(err)
		}
	}()
	node.StartElection()
	for {

	}
}

// call all clients with greater id than self, and if no responses are recieved, declare self new leader
func (n *Node) StartElection() {
	log.Println("starting election")
	var peersChecked sync.WaitGroup
	peersChecked.Add(len(n.Clients) - n.id - 1) // the amount of ids higher
	higherFound := false
	identifier := pb.IdentifierMSG{Id: int32(n.id)}
	for i := n.id + 1; i < len(n.Clients); i++ {
		go func() {
			defer peersChecked.Done()

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			response, err := n.Clients[i].Election(ctx, &identifier)
			if err == nil && response != nil {
				higherFound = true
			}
		}()
	}
	log.Println("waiting for response")
	peersChecked.Wait() // wait until all calls either respond or timeout.
	if higherFound == false {
		n.AnounceLeadership()
	}
}

func (n *Node) AnounceLeadership() {

	log.Println("Won leadership election")
	n.Mutex.Lock()
	n.currentPRM = n.id
	n.Mutex.Unlock()
	identifier := pb.IdentifierMSG{Id: int32(n.id)}
	for i := 0; i < n.id; i++ {
		go n.Clients[i].SetnewLeader(context.Background(), &identifier)
	}

}

func (n *Node) Election(ctx context.Context, msg *pb.IdentifierMSG) (*pb.EmptyMSG, error) {

	go n.StartElection()       // start election in seperate go rountine
	return &pb.EmptyMSG{}, nil // return to tell the calling node, that it is still alive and prevent a timeout.
}

func (n *Node) SetnewLeader(ctx context.Context, msg *pb.IdentifierMSG) (*pb.EmptyMSG, error) {

	n.currentPRM = int(msg.Id)
	log.Printf("PRM set to %d", msg.Id)
	return &pb.EmptyMSG{}, nil // return acknowledgement
}
