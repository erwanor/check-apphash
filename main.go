package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"sync"

	logging "cloud.google.com/go/logging/apiv2"
	"cloud.google.com/go/logging/apiv2/loggingpb"
)

type LogEntry struct {
	metadata map[string]string
	payload  string
}

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

func parseCommitLog(podName, logEntry string) (*LogData, error) {
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

func streamLogsWithFilter(ctx context.Context, projectID string, filter string, out chan<- LogEntry) error {
	client, err := logging.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("NewClient error: %v", err)
	}

	stream, err := client.TailLogEntries(ctx)
	if err != nil {
		client.Close()
		return fmt.Errorf("TailLogEntries error: %v", err)
	}

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

			metadata := entry.GetResource().GetLabels()
			payload := entry.GetTextPayload()

			out <- LogEntry{
				metadata: metadata,
				payload:  payload,
			}
		}
	}

	close(out)
	stream.CloseSend()
	client.Close()
	return nil
}

func postToDiscord(msg string) {
	webhookUrl := os.Getenv("DISCORD_WEBHOOK")

	payload := map[string]interface{}{
		"content": msg,
	}

	payloadBytes, _ := json.Marshal(payload)

	http.Post(webhookUrl, "application/json", bytes.NewBuffer(payloadBytes))
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run main.go PROJECT_ID")
		os.Exit(1)
	}

	projectID := os.Args[1]
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		// Map the block height to a list of `RootHashRecord` that store the pod name
		// and reported root hash.
		rootCache := make(map[int][]RootHashRecord)
		ctx := context.Background()
		commitLogs := make(chan LogEntry)

		filter := `resource.labels.container_name="tm" AND resource.labels.cluster_name="testnet" AND resource.labels.pod_name:"penumbra-testnet-fn"`
		if err := streamLogsWithFilter(ctx, projectID, filter, commitLogs); err != nil {
			fmt.Println(" error:", err)
		}

		for logEntry := range commitLogs {
			podName, exists := logEntry.metadata["pod_name"]
			if !exists {
				continue
			}

			commitLog, err := parseCommitLog(podName, logEntry.payload)
			if err != nil {
				continue
			}

			record := RootHashRecord{
				PodName: commitLog.PodName,
				Root:    commitLog.Root,
			}

			log_msg := fmt.Sprintf("%s, at height %d, has apphash %s", commitLog.PodName, commitLog.Height, commitLog.Root)
			log.Print(log_msg)

			if commitLog.Height%4320 == 0 {
				postToDiscord(log_msg)
			}

			if prev, exists := rootCache[commitLog.Height]; exists {
				if prev[0].Root != record.Root {
					err_str := fmt.Sprintf("root mismatch detected at height %d, between:\n%s: %s\n%s: %s\n", commitLog.Height, prev[0].PodName, prev[0].Root, record.PodName, record.Root)
					disc_msg := fmt.Sprintf("@erwanor : %s", err_str)
					postToDiscord(disc_msg)
					log.Fatal(err_str)
				}

				rootCache[commitLog.Height] = append(rootCache[commitLog.Height], record)
			} else {
				rootCache[commitLog.Height] = []RootHashRecord{record}
			}

		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		ctx := context.Background()
		errorLogs := make(chan LogEntry)

		filter := `resource.labels.container_name="pd" AND resource.labels.cluster_name="testnet" AND resource.labels.pod_name:"penumbra-testnet-fn" AND severity>=ERROR`
		if err := streamLogsWithFilter(ctx, projectID, filter, errorLogs); err != nil {
			fmt.Println(" error:", err)
		}

		for logEntry := range errorLogs {
			podName, exists := logEntry.metadata["pod_name"]
			if !exists {
				continue
			}

			postToDiscord(fmt.Sprintf("%s: %s", podName, logEntry.payload))
		}
	}()

	wg.Wait()
}
