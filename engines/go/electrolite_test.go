package electrolite

import (
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func setup(t *testing.T) *Electrolite {
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
		  (2, 'p2', 'other project', 0);
	`); err != nil {
		t.Fatal(err)
	}
	if err := app.InstallTriggers("todos"); err != nil {
		t.Fatal(err)
	}
	return app
}

func p1Shape() Shape {
	return Shape{
		Table:     "todos",
		Columns:   []string{"id", "project_id", "title", "done"},
		Predicate: Eq("project_id", "p1"),
	}
}

func TestSnapshotFiltersByPredicate(t *testing.T) {
	app := setup(t)
	snap, err := app.Snapshot(p1Shape())
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(snap.Rows))
	}
	if snap.Rows[0]["title"] != "ship electrolite" {
		t.Fatalf("unexpected title: %v", snap.Rows[0])
	}
	if len(snap.ShapeHandle) != 64 {
		t.Fatalf("bad shape handle: %s", snap.ShapeHandle)
	}
	if got := snap.KeyColumns; len(got) != 1 || got[0] != "id" {
		t.Fatalf("bad key columns: %v", got)
	}
}

func TestReplayInsertUpdateDeleteAndBatch(t *testing.T) {
	app := setup(t)
	snap, err := app.Snapshot(p1Shape())
	if err != nil {
		t.Fatal(err)
	}

	if _, err := app.Exec(
		"INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)",
		3, "p1", "from go",
	); err != nil {
		t.Fatal(err)
	}
	r, err := app.Replay(p1Shape(), snap.Offset, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Messages) != 1 || r.Messages[0].Type != "insert" || r.Messages[0].Value["title"] != "from go" {
		t.Fatalf("bad insert replay: %+v", r.Messages)
	}

	if _, err := app.Exec("UPDATE todos SET title = ? WHERE id = ?", "renamed", 3); err != nil {
		t.Fatal(err)
	}
	r2, err := app.Replay(p1Shape(), r.Offset, 100)
	if err != nil {
		t.Fatal(err)
	}
	if r2.Messages[0].Type != "update" || r2.Messages[0].Value["title"] != "renamed" {
		t.Fatalf("bad update replay: %+v", r2.Messages)
	}

	if _, err := app.Exec("DELETE FROM todos WHERE id = ?", 3); err != nil {
		t.Fatal(err)
	}
	r3, err := app.Replay(p1Shape(), r2.Offset, 100)
	if err != nil {
		t.Fatal(err)
	}
	if r3.Messages[0].Type != "delete" {
		t.Fatalf("bad delete replay: %+v", r3.Messages)
	}

	if err := app.WriteBatch([]Stmt{
		{SQL: "INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)", Args: []interface{}{4, "p1", "a"}},
		{SQL: "INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)", Args: []interface{}{5, "p1", "b"}},
	}); err != nil {
		t.Fatal(err)
	}
	r4, err := app.Replay(p1Shape(), r3.Offset, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(r4.Messages) != 2 {
		t.Fatalf("expected 2 batched messages, got %d", len(r4.Messages))
	}
	if r4.Messages[0].BatchID != r4.Messages[1].BatchID {
		t.Fatalf("batch ids differ: %s vs %s", r4.Messages[0].BatchID, r4.Messages[1].BatchID)
	}
}

func TestLiveWaitWakesOnChange(t *testing.T) {
	app := setup(t)
	app.LiveTimeout = 1 * time.Second

	snap, err := app.Snapshot(p1Shape())
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(50 * time.Millisecond)
		_, _ = app.Exec(
			"INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)",
			9, "p1", "live go",
		)
	}()

	app.WaitForChange()
	wg.Wait()

	r, err := app.Replay(p1Shape(), snap.Offset, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Messages) == 0 || r.Messages[0].Type != "insert" || r.Messages[0].Value["title"] != "live go" {
		t.Fatalf("expected one live insert; got %+v", r.Messages)
	}
}
