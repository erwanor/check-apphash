package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strconv"

	logging "cloud.google.com/go/logging/apiv2"
	loggingpb "google.golang.org/genproto/googleapis/logging/v2"
)

type LogData struct {
	Height  int
	Hash    string
	Root    string
	NumTxs  int
	PodName string
}

type RootHashRecord struct {
	PodName string
	Root    string
}

func parseLogEntry(logEntry string) (*LogData, error) {
	re := regexp.MustCompile(`finalizing commit of block\s+module=consensus height=(\d+) hash=([0-9a-fA-F]+) root=([0-9a-fA-F]+) num_txs=(\d+)`)
	match := re.FindStringSubmatch(logEntry)

	if len(match) == 0 {
		return nil, fmt.Errorf("no match")
	}

	height, err := strconv.Atoi(match[1])
	if err != nil {
		return nil, fmt.Errorf("parsing height: %v", err)
	}

	hash := match[2]
	root := match[3]

	numTxs, err := strconv.Atoi(match[4])
	if err != nil {
		return nil, fmt.Errorf("parsing num_txs: %v", err)
	}

	return &LogData{
		Height: height,
		Hash:   hash,
		Root:   root,
		NumTxs: numTxs,
	}, nil
}

func tailLogs(ctx context.Context, projectID string, out chan<- *LogData) error {
	client, err := logging.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("NewClient error: %v", err)
	}

	stream, err := client.TailLogEntries(ctx)
	if err != nil {
		client.Close()
		return fmt.Errorf("TailLogEntries error: %v", err)
	}

	filter := fmt.Sprintf(`resource.labels.container_name="tm" AND resource.labels.cluster_name="testnet" AND resource.labels.pod_name:"penumbra-testnet-fn"`)

	req := &loggingpb.TailLogEntriesRequest{
		ResourceNames: []string{
			"projects/" + projectID,
		},
		Filter: filter,
	}

	if err := stream.Send(req); err != nil {
		stream.CloseSend()
		client.Close()
		return fmt.Errorf("stream.Send error: %v", err)
	}

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println("stream.Recv error:", err)
			break
		}

		for _, entry := range resp.Entries {
			logData, err := parseLogEntry(entry.GetTextPayload())
			if err != nil {
				continue
			}

			podName := entry.GetResource().GetLabels()["pod_name"]
			logData.PodName = podName

			out <- logData
		}
	}

	close(out)
	stream.CloseSend()
	client.Close()
	return nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run main.go PROJECT_ID")
		os.Exit(1)
	}

	projectID := os.Args[1]
	ctx := context.Background()
	dataChan := make(chan *LogData)

	// Map the blockheight to a list of `RootHashRecord` that store the pod name
	// and reported root hash.
	rootCache := make(map[int][]RootHashRecord)

	go func() {
		if err := tailLogs(ctx, projectID, dataChan); err != nil {
			fmt.Println("tailLogs error:", err)
		}
	}()

	for data := range dataChan {
		record := RootHashRecord{
			PodName: data.PodName,
			Root:    data.Root,
		}

		log.Print(data.PodName, " has root ", data.Root, " at height ", data.Height)

		if prev, exists := rootCache[data.Height]; exists {
			if prev[0].Root != record.Root {
				err_str := fmt.Sprintf("root mismatch detected at height %d, between:\n%s: %s\n%s: %s\n", data.Height, prev[0].PodName, prev[0].Root, record.PodName, record.Root)
				log.Fatalf(err_str)
			}

			rootCache[data.Height] = append(rootCache[data.Height], record)
		} else {
			rootCache[data.Height] = []RootHashRecord{record}
		}

	}
}
