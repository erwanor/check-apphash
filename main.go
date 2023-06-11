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
	"google.golang.org/api/option"
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
		Height:  height,
		Hash:    hash,
		Root:    root,
		NumTxs:  numTxs,
		PodName: podName,
	}, nil
}

func streamLogsWithFilter(ctx context.Context, projectID string, filter string, out chan<- LogEntry) error {
	client, err := logging.NewClient(ctx, option.WithCredentialsJSON([]byte(os.Getenv("GCP_CREDENTIALS"))))
	if err != nil {
		return fmt.Errorf("NewClient error: %v", err)
	}

	log.Print("connected to GCP")

	stream, err := client.TailLogEntries(ctx)
	if err != nil {
		client.Close()
		return fmt.Errorf("TailLogEntries error: %v", err)
	}

	log.Print("established stream")

	req := &loggingpb.TailLogEntriesRequest{
		ResourceNames: []string{
			"projects/" + projectID,
		},
		Filter: filter,
	}

	if err := stream.Send(req); err != nil {
		stream.CloseSend()
		client.Close()
		log.Fatal("stream.Send error: %v", err)
	}

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			log.Print("stream EOF")
			break
		}
		if err != nil {
			log.Print("stream.Recv error:", err)
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
	log.Print("terminating routine")
	return nil
}

func postToDiscord(msg string) {
	webhookUrl := os.Getenv("DISCORD_WEBHOOK_URL")

	payload := map[string]interface{}{
		"content": msg,
	}

	payloadBytes, _ := json.Marshal(payload)

	http.Post(webhookUrl, "application/json", bytes.NewBuffer(payloadBytes))
}

func main() {
	projectID := os.Getenv("GCP_PROJECT_ID")
	if projectID == "" {
		fmt.Println("GCP PROJECT_ID is not set or empty")
		os.Exit(1)
	} else if os.Getenv("DISCORD_WEBHOOK_URL") == "" {
		fmt.Println("DISCORD_WEBHOOK_URL is unset or empty")
		os.Exit(1)
	} else if os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") == "" {
		fmt.Println("GOOGLE_APPLICATION_CREDENTIALS is unset or empty")
		os.Exit(1)
	} else if os.Getenv("GCP_CREDENTIALS") == "" {
		fmt.Println("GCP_CREDENTIALS is unset or empty")
		os.Exit(1)
	} else {
		log.Print("log relayer starting up!")
	}

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Print("started tm worker")
		// Map the block height to a list of `RootHashRecord` that store the pod name
		// and reported root hash.
		rootCache := make(map[int][]RootHashRecord)
		ctx := context.Background()
		commitLogs := make(chan LogEntry)

		confirmedHeight := 0

		filter := `resource.labels.container_name="tm" AND resource.labels.cluster_name="testnet" AND resource.labels.pod_name:"penumbra-testnet-fn"`
		go streamLogsWithFilter(ctx, projectID, filter, commitLogs)

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
				discord_msg := fmt.Sprintf("**%s**, at height **%d**, has apphash _%s_", commitLog.PodName, commitLog.Height, commitLog.Root)
				postToDiscord(discord_msg)
			}

			if prev, exists := rootCache[commitLog.Height]; exists {
				if commitLog.Height < confirmedHeight {
					msg := fmt.Sprintf("detected chain restart, current height=%d, previous tip: height=%d, %s:%s and %s:%s", commitLog.Height, confirmedHeight, prev[0].PodName, prev[0].Root, prev[1].PodName, prev[1].Root)
					postToDiscord(msg)
					rootCache = map[int][]RootHashRecord{
						commitLog.Height: {record},
					}
					continue
				} else if prev[0].Root != record.Root {
					err_str := fmt.Sprintf("root mismatch detected at height %d, between:\n%s: %s\n%s: %s\n", commitLog.Height, prev[0].PodName, prev[0].Root, record.PodName, record.Root)
					disc_msg := fmt.Sprintf("@erwanor : %s", err_str)
					postToDiscord(disc_msg)
					log.Fatal(err_str)
				} else {
				}

				rootCache[commitLog.Height] = append(rootCache[commitLog.Height], record)
				confirmedHeight = commitLog.Height
			} else {
				rootCache[commitLog.Height] = []RootHashRecord{record}
			}

		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Print("started pd worker")
		ctx := context.Background()
		errorLogs := make(chan LogEntry)

		filter := `resource.labels.container_name="pd" AND resource.labels.cluster_name="testnet" AND resource.labels.pod_name:"penumbra-testnet-fn" AND severity>=ERROR`
		go streamLogsWithFilter(ctx, projectID, filter, errorLogs)

		for logEntry := range errorLogs {
			podName, exists := logEntry.metadata["pod_name"]
			if !exists {
				log.Print("pod name not found!")
				continue
			}

			msg := fmt.Sprintf("%s: %s", podName, logEntry.payload)
			postToDiscord(msg)
		}
	}()

	wg.Wait()
}
