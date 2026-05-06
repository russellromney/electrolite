// Tiny test-only HTTP server exposing the Go engine over Electrolite's
// HTTP protocol. Used by the cross-language client × engine matrix.
//
// Usage:
//
//	go run ./engines/go/server --port 5102 --db /tmp/x/app.db
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"

	electrolite "github.com/russellromney/electrolite/engines/go"
)

func main() {
	if os.Getenv("ELECTROLITE_TEST_SERVER") != "1" {
		fmt.Fprintln(os.Stderr,
			"engines/go/server is a test-only HTTP server with an "+
				"unauthenticated /_test/exec endpoint. Set "+
				"ELECTROLITE_TEST_SERVER=1 to launch it.")
		os.Exit(1)
	}
	port := flag.Int("port", 0, "listen port")
	dbPath := flag.String("db", "", "sqlite database path")
	flag.Parse()
	if *port == 0 || *dbPath == "" {
		log.Fatal("--port and --db are required")
	}

	app, err := electrolite.Open(*dbPath)
	if err != nil {
		log.Fatalf("open: %v", err)
	}
	app.LiveTimeout = 2_000_000_000 // 2s
	if err := app.ExecBatch(`
		CREATE TABLE IF NOT EXISTS todos (
		  id INTEGER PRIMARY KEY,
		  project_id TEXT NOT NULL,
		  title TEXT NOT NULL,
		  done INTEGER NOT NULL DEFAULT 0
		);
		CREATE TABLE IF NOT EXISTS feature_flags (
		  id INTEGER PRIMARY KEY,
		  enabled BOOLEAN NOT NULL DEFAULT 0
		);
		CREATE TABLE IF NOT EXISTS memberships (
		  org TEXT NOT NULL,
		  "user" TEXT NOT NULL,
		  role TEXT NOT NULL,
		  PRIMARY KEY (org, "user")
		);
	`); err != nil {
		log.Fatalf("create: %v", err)
	}
	if err := app.InstallTriggers("todos"); err != nil {
		log.Fatalf("triggers: %v", err)
	}
	if err := app.InstallTriggers("feature_flags"); err != nil {
		log.Fatalf("triggers: %v", err)
	}
	if err := app.InstallTriggers("memberships"); err != nil {
		log.Fatalf("triggers: %v", err)
	}

	app.AddShape("projectTodos", electrolite.ShapeDef{
		Table:   "todos",
		Columns: []string{"id", "project_id", "title", "done"},
		Params:  []string{"project_id"},
		Where: func(c electrolite.BuildContext) electrolite.Predicate {
			return electrolite.Eq("project_id", c.Params["project_id"])
		},
		Scope: func(c electrolite.BuildContext) string { return "project:" + c.Params["project_id"] },
		Authorize: func(c electrolite.AuthContext) bool {
			ctx, ok := c.Context.(map[string]bool)
			return ok && ctx[c.Params["project_id"]]
		},
	})
	app.AddShape("highIds", electrolite.ShapeDef{
		Table:   "todos",
		Columns: []string{"id", "project_id", "title", "done"},
		Where:   func(electrolite.BuildContext) electrolite.Predicate { return electrolite.Gt("id", 1) },
	})
	// Boolean coercion proof: BOOLEAN column + true predicate.
	app.AddShape("enabledFlags", electrolite.ShapeDef{
		Table:   "feature_flags",
		Columns: []string{"id", "enabled"},
		Where:   func(electrolite.BuildContext) electrolite.Predicate { return electrolite.Eq("enabled", true) },
	})
	// Range-null proof: every engine must reject with 400.
	app.AddShape("bogusGt", electrolite.ShapeDef{
		Table:   "todos",
		Columns: []string{"id", "project_id", "title", "done"},
		Where:   func(electrolite.BuildContext) electrolite.Predicate { return electrolite.Gt("id", nil) },
	})
	// Composite-PK proof.
	app.AddShape("memberships", electrolite.ShapeDef{
		Table:   "memberships",
		Columns: []string{"org", "user", "role"},
	})
	// IN-predicate parity through SQL.
	app.AddShape("multiProject", electrolite.ShapeDef{
		Table:   "todos",
		Columns: []string{"id", "project_id", "title", "done"},
		Where: func(electrolite.BuildContext) electrolite.Predicate {
			return electrolite.In("project_id", "p1", "p2")
		},
	})
	// AND-predicate parity through SQL.
	app.AddShape("p1HighIds", electrolite.ShapeDef{
		Table:   "todos",
		Columns: []string{"id", "project_id", "title", "done"},
		Where: func(electrolite.BuildContext) electrolite.Predicate {
			return electrolite.And(electrolite.Eq("project_id", "p1"), electrolite.Gt("id", 1))
		},
	})
	// OR + NOT predicate parity through SQL.
	app.AddShape("activeP1OrP2", electrolite.ShapeDef{
		Table:   "todos",
		Columns: []string{"id", "project_id", "title", "done"},
		Where: func(electrolite.BuildContext) electrolite.Predicate {
			return electrolite.And(
				electrolite.Or(electrolite.Eq("project_id", "p1"), electrolite.Eq("project_id", "p2")),
				electrolite.Not(electrolite.Eq("done", 1)),
			)
		},
	})

	mux := http.NewServeMux()
	mux.HandleFunc("/electrolite/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("access-control-allow-origin", "*")
		query := ""
		if r.URL.RawQuery != "" {
			query = r.URL.RawQuery
		}

		ctx := map[string]bool{"p1": true, "p2": true}
		if strings.Contains(r.Header.Get("Accept"), "text/event-stream") {
			streamSSE(w, r, app, query, ctx)
			return
		}

		resp := app.Handle(r.URL.Path, query, ctx)

		// Compute CDN-friendly cache headers.
		etag := ""
		cacheControl := ""
		if resp.Status == 200 {
			if body, ok := resp.Body.(map[string]interface{}); ok {
				shapeHandle, _ := body["shape_handle"].(string)
				offsetOut := body["offset"]
				etag = fmt.Sprintf(`"%s-%v"`, shapeHandle, offsetOut)
				values, _ := url.ParseQuery(query)
				offsetIn := int64(-1)
				if v := values.Get("offset"); v != "" {
					if n, err := strconv.ParseInt(v, 10, 64); err == nil {
						offsetIn = n
					}
				}
				live := values.Get("live") == "true"
				switch {
				case live:
					cacheControl = "no-store"
				case offsetIn >= 0:
					cacheControl = "public, max-age=31536000, immutable"
				default:
					cacheControl = "public, max-age=5"
				}
			}
		}
		w.Header().Set("vary", "authorization")
		if etag != "" {
			if r.Header.Get("If-None-Match") == etag {
				w.Header().Set("etag", etag)
				if cacheControl != "" {
					w.Header().Set("cache-control", cacheControl)
				}
				w.WriteHeader(304)
				return
			}
			w.Header().Set("etag", etag)
		}
		if cacheControl != "" {
			w.Header().Set("cache-control", cacheControl)
		}
		w.Header().Set("content-type", "application/json")
		w.WriteHeader(resp.Status)
		_ = json.NewEncoder(w).Encode(resp.Body)
	})

	mux.HandleFunc("/_test/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("access-control-allow-origin", "*")
		switch r.URL.Path {
		case "/_test/exec":
			var p struct {
				SQL  string        `json:"sql"`
				Args []interface{} `json:"args"`
			}
			if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			if _, err := app.Exec(p.SQL, p.Args...); err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			_, _ = w.Write([]byte(`{"ok":true}`))
		case "/_test/write_batch":
			var p struct {
				Statements [][]json.RawMessage `json:"statements"`
			}
			if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			stmts := make([]electrolite.Stmt, 0, len(p.Statements))
			for _, s := range p.Statements {
				if len(s) != 2 {
					http.Error(w, "bad statement", 400)
					return
				}
				var sql string
				if err := json.Unmarshal(s[0], &sql); err != nil {
					http.Error(w, err.Error(), 400)
					return
				}
				var args []interface{}
				if err := json.Unmarshal(s[1], &args); err != nil {
					http.Error(w, err.Error(), 400)
					return
				}
				stmts = append(stmts, electrolite.Stmt{SQL: sql, Args: args})
			}
			if err := app.WriteBatch(stmts); err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			_, _ = w.Write([]byte(`{"ok":true}`))
		case "/_test/seed":
			var p struct {
				SQL string `json:"sql"`
			}
			if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			if err := app.ExecBatch(p.SQL); err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			_, _ = w.Write([]byte(`{"ok":true}`))
		case "/_test/match-predicate":
			var p struct {
				Predicate map[string]interface{}   `json:"predicate"`
				Rows      []map[string]interface{} `json:"rows"`
			}
			if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			pred, err := electrolite.PredicateFromJSON(p.Predicate)
			if err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			matched := []interface{}{}
			for _, row := range p.Rows {
				if electrolite.PredicateMatches(pred, row) {
					matched = append(matched, row["id"])
				}
			}
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"matched_ids": matched})
		default:
			http.NotFound(w, r)
		}
	})

	addr := fmt.Sprintf("127.0.0.1:%d", *port)
	fmt.Printf("electrolite-server listening on %d\n", *port)
	_ = os.Stdout.Sync()
	if err := http.ListenAndServe(addr, mux); err != nil && !strings.Contains(err.Error(), "use of closed") {
		log.Fatalf("serve: %v", err)
	}
}

func streamSSE(w http.ResponseWriter, r *http.Request, app *electrolite.Electrolite, query string, ctx map[string]bool) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", 500)
		return
	}
	w.Header().Set("content-type", "text/event-stream")
	w.Header().Set("cache-control", "no-cache")
	w.Header().Set("connection", "keep-alive")
	w.WriteHeader(200)

	// Pull initial snapshot or replay.
	resp := app.Handle(r.URL.Path, query, ctx)
	if resp.Status != 200 {
		writeSSE(w, "error", resp.Body)
		flusher.Flush()
		return
	}
	body, _ := resp.Body.(map[string]interface{})

	kind := "replay"
	values, _ := url.ParseQuery(query)
	if v := values.Get("offset"); v == "" || v == "-1" {
		kind = "snapshot"
	}
	writeSSE(w, kind, body)
	flusher.Flush()

	offset := body["offset"]
	logID, _ := body["log_id"].(string)
	shapeHandle, _ := body["shape_handle"].(string)

	notify := r.Context().Done()
	for {
		select {
		case <-notify:
			return
		default:
		}
		nextQuery := fmt.Sprintf(
			"offset=%v&log_id=%s&shape_handle=%s&live=true",
			offset, logID, shapeHandle,
		)
		resp := app.Handle(r.URL.Path, nextQuery, ctx)
		if resp.Status != 200 {
			writeSSE(w, "error", resp.Body)
			flusher.Flush()
			return
		}
		b, _ := resp.Body.(map[string]interface{})
		msgs, _ := b["messages"].([]electrolite.Message)
		if len(msgs) > 0 {
			writeSSE(w, "replay", b)
			flusher.Flush()
			offset = b["offset"]
		}
		// Heartbeat to detect client disconnect.
		if _, err := w.Write([]byte(": ping\n\n")); err != nil {
			return
		}
		flusher.Flush()
	}
}

func writeSSE(w http.ResponseWriter, event string, data interface{}) {
	payload, _ := json.Marshal(data)
	_, _ = w.Write([]byte("event: " + event + "\ndata: "))
	_, _ = w.Write(payload)
	_, _ = w.Write([]byte("\n\n"))
}
