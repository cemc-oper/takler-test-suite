// Public Go query APIs exposed as bounded JSON lines for scenario orchestration.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/cemc-oper/takler-client/common"
	"google.golang.org/grpc/metadata"
)

type request struct {
	Operation string          `json:"operation"`
	Scope     string          `json:"scope"`
	Path      string          `json:"path"`
	Depth     *int            `json:"depth"`
	Groups    []string        `json:"groups"`
	Count     int             `json:"count"`
	Document  json.RawMessage `json:"document"`
}
type queryTransport interface {
	common.Transport
	QueryDocument(context.Context, string, []byte) ([]byte, error)
}
type item struct {
	Node       map[string]any
	Completion *common.FullSnapshotComplete
	Error      error
}
type stream struct {
	Items chan item
	Stop  chan struct{}
	Done  chan struct{}
}

func snapshot(view *common.QuerySnapshot) any {
	nodes := map[string]common.QueryNode{}
	for _, path := range view.Paths() {
		nodes[path], _ = view.Node(path)
	}
	return map[string]any{"session_id": view.SessionID, "query_id": view.QueryID, "snapshot_id": view.SnapshotID, "as_of": view.AsOf, "expires_at": view.ExpiresAt, "revision": view.Revision, "stale_scopes": view.StaleScopes(), "paths": view.Paths(), "nodes": nodes}
}
func stopStream(current *stream) error {
	if current == nil {
		return nil
	}
	close(current.Stop)
	select {
	case <-current.Done:
		return nil
	case <-time.After(8 * time.Second):
		return errors.New("stream cancellation exceeded deadline")
	}
}

func main() {
	host := flag.String("host", "127.0.0.1", "host")
	port := flag.String("port", "33083", "port")
	transportName := flag.String("transport", "grpc", "transport")
	flag.Parse()
	client, err := common.NewTaklerServiceClient(*host, *port, *transportName, common.SecurityLevels{})
	if err != nil {
		panic(err)
	}
	var wire queryTransport
	if *transportName == "grpc" {
		wire = common.NewGrpcTransport(*host, *port, common.SecurityLevels{})
	} else {
		wire = common.NewHttpTransport(*host, *port, common.SecurityLevels{})
	}
	if err := wire.Open(); err != nil {
		panic(err)
	}
	defer wire.Close()
	var view *common.QuerySnapshot
	var current *stream
	defer func() { _ = stopStream(current) }()
	input := bufio.NewScanner(os.Stdin)
	input.Buffer(make([]byte, 4096), 2*1024*1024)
	output := json.NewEncoder(os.Stdout)
	output.SetEscapeHTML(false)
	for input.Scan() {
		var r request
		if err := json.Unmarshal(input.Bytes(), &r); err != nil {
			panic(err)
		}
		var value any
		var err error
		switch r.Operation {
		case "summary":
			view, err = client.ReadQuerySnapshot(common.QuerySelection{ScopePath: r.Scope, Depth: r.Depth})
			if err == nil {
				value = snapshot(view)
			}
		case "since":
			var next *common.QuerySnapshot
			next, err = client.ReadQuerySince(view)
			if err == nil {
				view = next
				value = snapshot(view)
			}
		case "detail":
			value, err = client.ReadQueryDetail(view, r.Path, r.Groups)
		case "raw":
			var document map[string]any
			err = json.Unmarshal(r.Document, &document)
			if err == nil {
				var raw []byte
				ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
				var md metadata.MD
				md, err = client.Credentials().BuildMetadata(common.KindQuery)
				if err == nil {
					raw, err = wire.QueryDocument(metadata.NewOutgoingContext(ctx, md), document["kind"].(string), r.Document)
				}
				cancel()
				if err == nil {
					err = json.Unmarshal(raw, &value)
				}
			}
		case "reconnect":
			wire.Close()
			err = wire.Open()
			value = map[string]any{"reconnected": err == nil}
		case "full_start":
			err = stopStream(current)
			if err == nil {
				current = &stream{make(chan item), make(chan struct{}), make(chan struct{})}
				s := current
				selection := common.FullSnapshotSelection{ScopePath: r.Scope, FieldGroups: r.Groups}
				go func() {
					defer close(s.Done)
					defer close(s.Items)
					complete, failure := client.IterFullSnapshot(selection, func(node map[string]any) error {
						select {
						case s.Items <- item{Node: node}:
							return nil
						case <-s.Stop:
							return errors.New("scenario cancelled stream")
						}
					})
					select {
					case s.Items <- item{Completion: complete, Error: failure}:
					case <-s.Stop:
					}
				}()
				first := <-s.Items
				err = first.Error
				value = map[string]any{"node": first.Node}
			}
		case "full_next":
			if r.Count <= 0 {
				r.Count = 50
			}
			nodes := []map[string]any{}
			var complete *common.FullSnapshotComplete
			for i := 0; i < r.Count; i++ {
				next, ok := <-current.Items
				if !ok {
					err = errors.New("stream closed without a completion record")
					break
				}
				if next.Error != nil {
					err = next.Error
					break
				}
				if next.Completion != nil {
					complete = next.Completion
					break
				}
				nodes = append(nodes, next.Node)
			}
			value = map[string]any{"nodes": nodes, "completion": complete}
		case "full_cancel":
			err = stopStream(current)
			current = nil
			value = map[string]any{"cancelled": err == nil}
		case "close":
			err = stopStream(current)
			current = nil
			value = map[string]any{"closed": err == nil}
		default:
			err = fmt.Errorf("unknown operation %q", r.Operation)
		}
		response := map[string]any{"ok": err == nil, "value": value}
		if err != nil {
			code := "client_error"
			var queryError *common.QueryFailureError
			if errors.As(err, &queryError) {
				code = queryError.Code
			}
			response["code"], response["error"] = code, err.Error()
		}
		if err := output.Encode(response); err != nil {
			panic(err)
		}
		if r.Operation == "close" {
			break
		}
	}
}
