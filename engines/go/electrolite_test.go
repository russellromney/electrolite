package electrolite

import (
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// ---------- helpers ----------

func openApp(t *testing.T) *Electrolite {
	t.Helper()
	dir := t.TempDir()
	app, err := Open(filepath.Join(dir, "app.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = app.Close() })
	if err := app.ExecBatch(`
		CREATE TABLE todos (
		  id INTEGER PRIMARY KEY,
		  project_id TEXT NOT NULL,
		  title TEXT NOT NULL,
		  done INTEGER NOT NULL DEFAULT 0
		);
		INSERT INTO todos (id, project_id, title, done) VALUES
		  (1, 'p1', 'ship electrolite', 0),
		  (2, 'p2', 'other project', 0),
		  (3, 'p1', 'second p1', 0);
	`); err != nil {
		t.Fatal(err)
	}
	if err := app.InstallTriggers("todos"); err != nil {
		t.Fatal(err)
	}
	return app
}

func projectTodosApp(t *testing.T) *Electrolite {
	app := openApp(t)
	app.AddShape("projectTodos", ShapeDef{
		Table:   "todos",
		Columns: []string{"id", "project_id", "title", "done"},
		Params:  []string{"project_id"},
		Where:   func(c BuildContext) Predicate { return Eq("project_id", c.Params["project_id"]) },
		Scope:   func(c BuildContext) string { return "project:" + c.Params["project_id"] },
		Authorize: func(c AuthContext) bool {
			ctx, ok := c.Context.(map[string]bool)
			return ok && ctx[c.Params["project_id"]]
		},
	})
	return app
}

func bodyMap(r HandleResponse) map[string]interface{} {
	m, _ := r.Body.(map[string]interface{})
	return m
}

// ---------- conformance ----------

func TestSnapshotReturnsMatchingRowsOrderedByKey(t *testing.T) {
	app := projectTodosApp(t)
	r := app.Handle("/electrolite/v1/projectTodos/p1", "offset=-1", map[string]bool{"p1": true})
	if r.Status != 200 {
		t.Fatalf("status: %d body: %v", r.Status, r.Body)
	}
	b := bodyMap(r)
	rows := b["rows"].([]map[string]interface{})
	if len(rows) != 2 || rows[0]["id"].(float64) != 1 || rows[1]["id"].(float64) != 3 {
		t.Fatalf("unexpected rows: %v", rows)
	}
	keys := b["key_columns"].([]string)
	if len(keys) != 1 || keys[0] != "id" {
		t.Fatalf("bad key_columns: %v", keys)
	}
}

func TestEqPredicateFiltersSnapshot(t *testing.T) {
	app := projectTodosApp(t)
	r := app.Handle("/electrolite/v1/projectTodos/p2", "offset=-1", map[string]bool{"p2": true})
	if r.Status != 200 {
		t.Fatalf("status: %d", r.Status)
	}
	rows := bodyMap(r)["rows"].([]map[string]interface{})
	if len(rows) != 1 || rows[0]["id"].(float64) != 2 {
		t.Fatalf("unexpected rows: %v", rows)
	}
}

func TestInPredicateFiltersSnapshot(t *testing.T) {
	app := openApp(t)
	app.AddShape("todoSet", ShapeDef{
		Table:   "todos",
		Columns: []string{"id", "project_id"},
		Where:   func(BuildContext) Predicate { return In("project_id", "p1", "p2") },
	})
	r := app.Handle("/electrolite/v1/todoSet", "offset=-1", nil)
	rows := bodyMap(r)["rows"].([]map[string]interface{})
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
}

func TestRangePredicateFiltersSnapshot(t *testing.T) {
	app := openApp(t)
	app.AddShape("highIds", ShapeDef{
		Table:   "todos",
		Columns: []string{"id", "project_id"},
		Where:   func(BuildContext) Predicate { return Gt("id", 1) },
	})
	r := app.Handle("/electrolite/v1/highIds", "offset=-1", nil)
	rows := bodyMap(r)["rows"].([]map[string]interface{})
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

func TestAndPredicateCombinesConditions(t *testing.T) {
	app := openApp(t)
	app.AddShape("p1HighIds", ShapeDef{
		Table:   "todos",
		Columns: []string{"id", "project_id"},
		Where:   func(BuildContext) Predicate { return And(Eq("project_id", "p1"), Gt("id", 1)) },
	})
	r := app.Handle("/electrolite/v1/p1HighIds", "offset=-1", nil)
	rows := bodyMap(r)["rows"].([]map[string]interface{})
	if len(rows) != 1 || rows[0]["id"].(float64) != 3 {
		t.Fatalf("unexpected rows: %v", rows)
	}
}

func TestReplayEmitsInsertUpdateDelete(t *testing.T) {
	app := projectTodosApp(t)
	snap := bodyMap(app.Handle("/electrolite/v1/projectTodos/p1", "offset=-1", map[string]bool{"p1": true}))
	if _, err := app.Exec("INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)", 10, "p1", "new"); err != nil {
		t.Fatal(err)
	}
	if _, err := app.Exec("UPDATE todos SET title = ? WHERE id = ?", "renamed", 10); err != nil {
		t.Fatal(err)
	}
	if _, err := app.Exec("DELETE FROM todos WHERE id = ?", 10); err != nil {
		t.Fatal(err)
	}
	q := buildQ(snap, false)
	body := bodyMap(app.Handle("/electrolite/v1/projectTodos/p1", q, map[string]bool{"p1": true}))
	msgs := body["messages"].([]Message)
	if len(msgs) != 3 || msgs[0].Type != "insert" || msgs[1].Type != "update" || msgs[2].Type != "delete" {
		t.Fatalf("unexpected messages: %+v", msgs)
	}
}

func TestWriteBatchSharesBatchID(t *testing.T) {
	app := projectTodosApp(t)
	snap := bodyMap(app.Handle("/electrolite/v1/projectTodos/p1", "offset=-1", map[string]bool{"p1": true}))
	if err := app.WriteBatch([]Stmt{
		{SQL: "INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)", Args: []interface{}{11, "p1", "a"}},
		{SQL: "INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)", Args: []interface{}{12, "p1", "b"}},
	}); err != nil {
		t.Fatal(err)
	}
	q := buildQ(snap, false)
	body := bodyMap(app.Handle("/electrolite/v1/projectTodos/p1", q, map[string]bool{"p1": true}))
	msgs := body["messages"].([]Message)
	if len(msgs) != 2 || msgs[0].BatchID != msgs[1].BatchID {
		t.Fatalf("unexpected messages: %+v", msgs)
	}
}

func TestReplayExtendsPastLimitToFinishBatch(t *testing.T) {
	app := projectTodosApp(t)
	shape := Shape{
		Table:     "todos",
		Columns:   []string{"id", "project_id"},
		Predicate: Eq("project_id", "p1"),
	}
	snap, err := app.Snapshot(shape)
	if err != nil {
		t.Fatal(err)
	}
	if err := app.WriteBatch([]Stmt{
		{SQL: "INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)", Args: []interface{}{21, "p1", "a"}},
		{SQL: "INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)", Args: []interface{}{22, "p1", "b"}},
		{SQL: "INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)", Args: []interface{}{23, "p1", "c"}},
	}); err != nil {
		t.Fatal(err)
	}
	r, err := app.Replay(shape, snap.Offset, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Messages) != 3 {
		t.Fatalf("expected 3 messages (batch fully extended), got %d", len(r.Messages))
	}
}

func TestPKChangeReplaysAsDeleteThenInsert(t *testing.T) {
	dir := t.TempDir()
	app, err := Open(filepath.Join(dir, "app.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = app.Close() })
	if err := app.ExecBatch(`
		CREATE TABLE things (thing_id TEXT PRIMARY KEY, owner TEXT NOT NULL);
		INSERT INTO things (thing_id, owner) VALUES ('a', 'alice');
	`); err != nil {
		t.Fatal(err)
	}
	if err := app.InstallTriggers("things"); err != nil {
		t.Fatal(err)
	}
	app.AddShape("thingsByOwner", ShapeDef{
		Table:   "things",
		Columns: []string{"thing_id", "owner"},
		Params:  []string{"owner"},
		Where:   func(c BuildContext) Predicate { return Eq("owner", c.Params["owner"]) },
	})
	snap := bodyMap(app.Handle("/electrolite/v1/thingsByOwner/alice", "offset=-1", nil))
	if _, err := app.Exec("UPDATE things SET thing_id = ? WHERE thing_id = ?", "b", "a"); err != nil {
		t.Fatal(err)
	}
	q := buildQ(snap, false)
	body := bodyMap(app.Handle("/electrolite/v1/thingsByOwner/alice", q, nil))
	msgs := body["messages"].([]Message)
	if msgs[0].Type != "delete" || msgs[0].Key["thing_id"] != "a" {
		t.Fatalf("expected delete of a, got %+v", msgs[0])
	}
	if msgs[1].Type != "insert" || msgs[1].Key["thing_id"] != "b" {
		t.Fatalf("expected insert of b, got %+v", msgs[1])
	}
}

func TestUnauthorizedShapeReturns404(t *testing.T) {
	app := projectTodosApp(t)
	r := app.Handle("/electrolite/v1/projectTodos/p1", "offset=-1", map[string]bool{"p2": true})
	if r.Status != 404 {
		t.Fatalf("status: %d", r.Status)
	}
}

func TestLogIDMismatchReturns409(t *testing.T) {
	app := projectTodosApp(t)
	snap := bodyMap(app.Handle("/electrolite/v1/projectTodos/p1", "offset=-1", map[string]bool{"p1": true}))
	q := buildQ(map[string]interface{}{
		"offset":       snap["offset"],
		"log_id":       "00000000000000000000000000000000",
		"shape_handle": snap["shape_handle"],
	}, false)
	r := app.Handle("/electrolite/v1/projectTodos/p1", q, map[string]bool{"p1": true})
	if r.Status != 409 {
		t.Fatalf("status: %d", r.Status)
	}
}

func TestCompactMakesOldOffsetsResync(t *testing.T) {
	app := projectTodosApp(t)
	snap := bodyMap(app.Handle("/electrolite/v1/projectTodos/p1", "offset=-1", map[string]bool{"p1": true}))
	if _, err := app.Exec("INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)", 50, "p1", "x"); err != nil {
		t.Fatal(err)
	}
	stats, err := app.Compact("todos", 0)
	if err != nil {
		t.Fatal(err)
	}
	if stats.RetainedOffset < snap["offset"].(int64) {
		t.Fatalf("retained_offset %d < snap.offset %d", stats.RetainedOffset, snap["offset"])
	}
	q := buildQ(snap, false)
	r := app.Handle("/electrolite/v1/projectTodos/p1", q, map[string]bool{"p1": true})
	if r.Status != 409 {
		t.Fatalf("status: %d body: %v", r.Status, r.Body)
	}
}

func TestInstallTriggersRequiresPrimaryKey(t *testing.T) {
	dir := t.TempDir()
	app, err := Open(filepath.Join(dir, "app.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = app.Close() })
	if err := app.ExecBatch(`CREATE TABLE no_pk (a INTEGER, b INTEGER);`); err != nil {
		t.Fatal(err)
	}
	if err := app.InstallTriggers("no_pk"); err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestCompositePrimaryKeysExposedAndUsedInKeys(t *testing.T) {
	dir := t.TempDir()
	app, err := Open(filepath.Join(dir, "app.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = app.Close() })
	if err := app.ExecBatch(`
		CREATE TABLE memberships (
		  org TEXT NOT NULL,
		  user TEXT NOT NULL,
		  role TEXT NOT NULL,
		  PRIMARY KEY (org, user)
		);
		INSERT INTO memberships (org, user, role) VALUES ('o1', 'u1', 'admin');
	`); err != nil {
		t.Fatal(err)
	}
	if err := app.InstallTriggers("memberships"); err != nil {
		t.Fatal(err)
	}
	app.AddShape("memberships", ShapeDef{
		Table:   "memberships",
		Columns: []string{"org", "user", "role"},
	})

	snap := bodyMap(app.Handle("/electrolite/v1/memberships", "offset=-1", nil))
	keys := snap["key_columns"].([]string)
	if len(keys) != 2 || keys[0] != "org" || keys[1] != "user" {
		t.Fatalf("bad key_columns: %v", keys)
	}

	if _, err := app.Exec("DELETE FROM memberships WHERE org = ? AND user = ?", "o1", "u1"); err != nil {
		t.Fatal(err)
	}
	q := buildQ(snap, false)
	body := bodyMap(app.Handle("/electrolite/v1/memberships", q, nil))
	msgs := body["messages"].([]Message)
	if msgs[0].Type != "delete" || msgs[0].Key["org"] != "o1" || msgs[0].Key["user"] != "u1" {
		t.Fatalf("unexpected delete: %+v", msgs[0])
	}
}

func TestLiveWaitWakesWhenWriteCommits(t *testing.T) {
	app := projectTodosApp(t)
	app.LiveTimeout = 1 * time.Second
	snap := bodyMap(app.Handle("/electrolite/v1/projectTodos/p1", "offset=-1", map[string]bool{"p1": true}))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(50 * time.Millisecond)
		_, _ = app.Exec("INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)", 99, "p1", "live")
	}()

	q := buildQ(snap, true)
	r := app.Handle("/electrolite/v1/projectTodos/p1", q, map[string]bool{"p1": true})
	wg.Wait()
	if r.Status != 200 {
		t.Fatalf("status: %d body: %v", r.Status, r.Body)
	}
	msgs := bodyMap(r)["messages"].([]Message)
	if len(msgs) == 0 || msgs[0].Type != "insert" {
		t.Fatalf("expected an insert, got: %+v", msgs)
	}
}

// buildQ assembles the query string for a replay/live request from the
// snapshot/replay body.
func buildQ(body map[string]interface{}, live bool) string {
	q := ""
	if v, ok := body["offset"]; ok {
		q = "offset=" + asString(v)
	}
	if live {
		q += "&live=true"
	}
	if v, ok := body["log_id"]; ok {
		q += "&log_id=" + asString(v)
	}
	if v, ok := body["shape_handle"]; ok {
		q += "&shape_handle=" + asString(v)
	}
	return q
}

func asString(v interface{}) string {
	switch x := v.(type) {
	case string:
		return x
	case int64:
		return formatInt(x)
	case int:
		return formatInt(int64(x))
	case float64:
		return formatInt(int64(x))
	}
	return ""
}

func formatInt(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	digits := []byte{}
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	if neg {
		return "-" + string(digits)
	}
	return string(digits)
}
