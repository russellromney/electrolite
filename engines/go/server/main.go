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
	"os"
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
	`); err != nil {
		log.Fatalf("create: %v", err)
	}
	if err := app.InstallTriggers("todos"); err != nil {
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

	mux := http.NewServeMux()
	mux.HandleFunc("/electrolite/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("access-control-allow-origin", "*")
		query := ""
		if r.URL.RawQuery != "" {
			query = r.URL.RawQuery
		}
		resp := app.Handle(r.URL.Path, query, map[string]bool{"p1": true, "p2": true})
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
