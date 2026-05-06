// Package electrolite implements the Electrolite engine in Go. See
// engines/PROTOCOL.md for the contract every engine satisfies.
//
// Uses modernc.org/sqlite (pure Go, no cgo).
package electrolite

import (
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite"
)

// ---------- predicates ----------

// Predicate is the sealed sum type covering `all`, `eq`, range
// (gt/lt/gte/lte), `in`, and `and`. Variants are constructed via the
// All/Eq/Gt/Lt/Gte/Lte/In/And helpers and consumed via type switch.
type Predicate interface {
	predicateKind() string
}

// AllPredicate matches every row.
type AllPredicate struct{}

func (AllPredicate) predicateKind() string { return "all" }

// EqPredicate is an equality test against a column.
type EqPredicate struct {
	Column string
	Value  interface{}
}

func (EqPredicate) predicateKind() string { return "eq" }

// RangePredicate is a `>`, `<`, `>=`, or `<=` test against a column.
// Op is one of "gt", "lt", "gte", "lte".
type RangePredicate struct {
	Op     string
	Column string
	Value  interface{}
}

func (RangePredicate) predicateKind() string { return "range" }

// InPredicate matches rows where the column equals any of the values.
type InPredicate struct {
	Column string
	Values []interface{}
}

func (InPredicate) predicateKind() string { return "in" }

// AndPredicate is the conjunction of its children.
type AndPredicate struct {
	Predicates []Predicate
}

func (AndPredicate) predicateKind() string { return "and" }

// OrPredicate is the disjunction of its children.
type OrPredicate struct {
	Predicates []Predicate
}

func (OrPredicate) predicateKind() string { return "or" }

// NotPredicate is the negation of its child.
type NotPredicate struct {
	Predicate Predicate
}

func (NotPredicate) predicateKind() string { return "not" }

// All / Eq / Gt / Lt / Gte / Lte / In / And construct predicates.
func All() Predicate                              { return AllPredicate{} }
func Eq(col string, v interface{}) Predicate     { return EqPredicate{Column: col, Value: v} }
func Gt(col string, v interface{}) Predicate     { return RangePredicate{Op: "gt", Column: col, Value: v} }
func Lt(col string, v interface{}) Predicate     { return RangePredicate{Op: "lt", Column: col, Value: v} }
func Gte(col string, v interface{}) Predicate    { return RangePredicate{Op: "gte", Column: col, Value: v} }
func Lte(col string, v interface{}) Predicate    { return RangePredicate{Op: "lte", Column: col, Value: v} }
func In(col string, vs ...interface{}) Predicate { return InPredicate{Column: col, Values: vs} }
func And(children ...Predicate) Predicate        { return AndPredicate{Predicates: children} }
func Or(children ...Predicate) Predicate         { return OrPredicate{Predicates: children} }
func Not(child Predicate) Predicate              { return NotPredicate{Predicate: child} }

var rangeOps = map[string]string{"gt": ">", "lt": "<", "gte": ">=", "lte": "<="}

// ---------- shapes ----------

// Shape is the row subset a client subscribes to. AuthScope and
// SchemaVersion are part of the shape identity.
type Shape struct {
	Table         string
	Columns       []string
	Predicate     Predicate
	AuthScope     string
	SchemaVersion int
}

// BuildContext is passed to a Shape's where/scope callbacks.
type BuildContext struct {
	Params  map[string]string
	Context interface{}
}

// AuthContext is passed to a Shape's authorize callback.
type AuthContext struct {
	Params  map[string]string
	Context interface{}
	Scope   string
}

// ShapeDef is the registered shape with optional callbacks.
type ShapeDef struct {
	Table         string
	Columns       []string
	Params        []string
	Where         func(BuildContext) Predicate
	Scope         func(BuildContext) string
	Authorize     func(AuthContext) bool
	SchemaVersion int
}

// ---------- engine ----------

// Electrolite holds the SQLite connection, registered shapes, and the
// change-notify primitive.
type Electrolite struct {
	db          *sql.DB
	mu          sync.Mutex
	cond        *sync.Cond
	stopped     atomic.Bool
	shapes      map[string]ShapeDef
	prefix      string
	replayLimit int64
	LiveTimeout time.Duration
}

// Stmt is one SQL statement plus arguments inside a WriteBatch.
type Stmt struct {
	SQL  string
	Args []interface{}
}

// Snapshot is the current matching rows pinned to a log offset.
type Snapshot struct {
	LogID       string                   `json:"log_id"`
	ShapeHandle string                   `json:"shape_handle"`
	KeyColumns  []string                 `json:"key_columns"`
	Rows        []map[string]interface{} `json:"rows"`
	Offset      int64                    `json:"offset"`
}

// Message is one logical change emitted by Replay.
type Message struct {
	Type    string                 `json:"type"`
	BatchID string                 `json:"batch_id"`
	Key     map[string]interface{} `json:"key"`
	Offset  int64                  `json:"offset"`
	Value   map[string]interface{} `json:"value,omitempty"`
}

// Replay is a page of messages plus where the cursor advanced to.
type Replay struct {
	LogID       string    `json:"log_id"`
	ShapeHandle string    `json:"shape_handle"`
	Messages    []Message `json:"messages"`
	Offset      int64     `json:"offset"`
	UpToDate    bool      `json:"up_to_date"`
	Replica     string    `json:"replica,omitempty"`
}

// CompactStats is what compact() returns.
type CompactStats struct {
	RetainedOffset int64 `json:"retained_offset"`
	DeletedRows    int64 `json:"deleted_rows"`
}

// errResync is returned internally to surface a 409 to handle().
type errResync struct{}

func (errResync) Error() string { return "resync_required" }

// errBadInput is returned for predicate / argument validation failures
// (e.g., null in a range predicate, boolean against a non-BOOLEAN
// column). handle() maps it to 400.
type errBadInput struct{ msg string }

func (e errBadInput) Error() string { return e.msg }

// Open opens or creates a SQLite database and bootstraps Electrolite tables.
func Open(path string) (*Electrolite, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	e := &Electrolite{
		db:          db,
		shapes:      map[string]ShapeDef{},
		prefix:      "/electrolite/v1",
		replayLimit: 1000,
		LiveTimeout: 20 * time.Second,
	}
	e.cond = sync.NewCond(&e.mu)
	if err := e.bootstrap(); err != nil {
		return nil, err
	}
	return e, nil
}

// Close releases the SQLite connection.
func (e *Electrolite) Close() error { return e.db.Close() }

// Shutdown marks the engine stopped and wakes every live waiter.
// In-flight live waits return a clean
// `200 {messages: [], up_to_date: true, shutdown: true}` response
// instead of touching the closed database. Call Close() when ready
// to release the SQLite connection.
func (e *Electrolite) Shutdown() {
	e.stopped.Store(true)
	e.notify()
}

// IsStopped reports whether Shutdown has been called.
func (e *Electrolite) IsStopped() bool {
	return e.stopped.Load()
}

// AddShape registers a named shape.
func (e *Electrolite) AddShape(name string, def ShapeDef) {
	if def.SchemaVersion == 0 {
		def.SchemaVersion = 1
	}
	e.shapes[name] = def
}

// SetPrefix overrides the default "/electrolite/v1".
func (e *Electrolite) SetPrefix(prefix string) { e.prefix = prefix }

// SetReplayLimit overrides the default replay page size.
func (e *Electrolite) SetReplayLimit(limit int64) {
	if limit < 1 {
		limit = 1
	}
	e.replayLimit = limit
}

func (e *Electrolite) bootstrap() error {
	if _, err := e.db.Exec(`
		CREATE TABLE IF NOT EXISTS _electrolite_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
		CREATE TABLE IF NOT EXISTS _electrolite_watched_tables (table_name TEXT PRIMARY KEY, pk_columns TEXT NOT NULL);
		CREATE TABLE IF NOT EXISTS _electrolite_log (
		  seq INTEGER PRIMARY KEY AUTOINCREMENT,
		  batch_id TEXT NOT NULL,
		  table_name TEXT NOT NULL,
		  op TEXT NOT NULL,
		  pk_json TEXT NOT NULL,
		  old_pk_json TEXT, new_pk_json TEXT,
		  old_json TEXT, new_json TEXT,
		  created_at INTEGER NOT NULL DEFAULT (unixepoch())
		);
		CREATE INDEX IF NOT EXISTS _electrolite_log_table_seq_idx ON _electrolite_log (table_name, seq);
	`); err != nil {
		return err
	}
	var existing string
	err := e.db.QueryRow("SELECT value FROM _electrolite_meta WHERE key = 'log_id'").Scan(&existing)
	if err == sql.ErrNoRows {
		if _, err := e.db.Exec("INSERT INTO _electrolite_meta (key, value) VALUES ('log_id', ?)", randomHex(16)); err != nil {
			return err
		}
		err = nil
	}
	if err != nil {
		return err
	}
	// A crashed write_batch may have left current_batch_id behind;
	// clear it so the next unrelated write does not inherit a dead
	// batch_id.
	_, err = e.db.Exec("DELETE FROM _electrolite_meta WHERE key = 'current_batch_id'")
	return err
}

// Exec runs a single statement and notifies live waiters.
func (e *Electrolite) Exec(query string, args ...interface{}) (int64, error) {
	res, err := e.db.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	e.notify()
	return n, nil
}

// ExecBatch runs multiple statements (DDL friendly).
func (e *Electrolite) ExecBatch(script string) error {
	if _, err := e.db.Exec(script); err != nil {
		return err
	}
	e.notify()
	return nil
}

// InstallTriggers attaches the insert/update/delete log triggers to a table.
func (e *Electrolite) InstallTriggers(table string) error {
	info, err := e.inspectTable(table)
	if err != nil {
		return err
	}
	if len(info.PK) == 0 {
		return fmt.Errorf("table %s must have a primary key", table)
	}
	pk, _ := json.Marshal(info.PK)
	if _, err := e.db.Exec(
		`INSERT INTO _electrolite_watched_tables (table_name, pk_columns) VALUES (?, ?)
		 ON CONFLICT(table_name) DO UPDATE SET pk_columns = excluded.pk_columns`,
		table, string(pk),
	); err != nil {
		return err
	}

	newRow := rowJSON("NEW", info.Columns)
	oldRow := rowJSON("OLD", info.Columns)
	newPK := rowJSON("NEW", info.PK)
	oldPK := rowJSON("OLD", info.PK)
	batchID := `COALESCE((SELECT value FROM _electrolite_meta WHERE key = 'current_batch_id'), lower(hex(randomblob(16))))`
	lit := quoteString(table)
	tbl := quoteIdent(table)
	script := fmt.Sprintf(`
		DROP TRIGGER IF EXISTS "_electrolite_%s_ai";
		DROP TRIGGER IF EXISTS "_electrolite_%s_au";
		DROP TRIGGER IF EXISTS "_electrolite_%s_ad";
		CREATE TRIGGER "_electrolite_%s_ai" AFTER INSERT ON %s BEGIN
		  INSERT INTO _electrolite_log (batch_id, table_name, op, pk_json, old_pk_json, new_pk_json, old_json, new_json)
		  VALUES (%s, %s, 'insert', %s, NULL, %s, NULL, %s);
		END;
		CREATE TRIGGER "_electrolite_%s_au" AFTER UPDATE ON %s BEGIN
		  INSERT INTO _electrolite_log (batch_id, table_name, op, pk_json, old_pk_json, new_pk_json, old_json, new_json)
		  VALUES (%s, %s, 'update', %s, %s, %s, %s, %s);
		END;
		CREATE TRIGGER "_electrolite_%s_ad" AFTER DELETE ON %s BEGIN
		  INSERT INTO _electrolite_log (batch_id, table_name, op, pk_json, old_pk_json, new_pk_json, old_json, new_json)
		  VALUES (%s, %s, 'delete', %s, %s, NULL, %s, NULL);
		END;
	`,
		table, table, table,
		table, tbl, batchID, lit, newPK, newPK, newRow,
		table, tbl, batchID, lit, newPK, oldPK, newPK, oldRow, newRow,
		table, tbl, batchID, lit, oldPK, oldPK, oldRow,
	)
	_, err = e.db.Exec(script)
	return err
}

// WriteBatch runs multiple writes inside a transaction sharing one batch_id.
func (e *Electrolite) WriteBatch(stmts []Stmt) error {
	batchID := randomHex(16)
	tx, err := e.db.Begin()
	if err != nil {
		return err
	}
	commit := false
	defer func() {
		if !commit {
			_ = tx.Rollback()
		}
	}()
	if _, err := tx.Exec(
		`INSERT INTO _electrolite_meta (key, value) VALUES ('current_batch_id', ?)
		 ON CONFLICT(key) DO UPDATE SET value = excluded.value`, batchID,
	); err != nil {
		return err
	}
	for _, s := range stmts {
		if _, err := tx.Exec(s.SQL, s.Args...); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(`DELETE FROM _electrolite_meta WHERE key = 'current_batch_id'`); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	commit = true
	e.notify()
	return nil
}

// Compact deletes log rows older than keepLast and writes the watermark
// to retained_offset:<table>.
func (e *Electrolite) Compact(table string, keepLast int) (CompactStats, error) {
	var watermark int64
	err := e.db.QueryRow(
		"SELECT seq FROM _electrolite_log WHERE table_name = ? ORDER BY seq DESC LIMIT 1 OFFSET ?",
		table, keepLast,
	).Scan(&watermark)
	if err == sql.ErrNoRows {
		watermark, err = e.highWater()
		if err != nil {
			return CompactStats{}, err
		}
	} else if err != nil {
		return CompactStats{}, err
	}
	res, err := e.db.Exec("DELETE FROM _electrolite_log WHERE table_name = ? AND seq <= ?", table, watermark)
	if err != nil {
		return CompactStats{}, err
	}
	deleted, _ := res.RowsAffected()
	if _, err := e.db.Exec(
		`INSERT INTO _electrolite_meta (key, value) VALUES (?, ?)
		 ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
		"retained_offset:"+table, strconv.FormatInt(watermark, 10),
	); err != nil {
		return CompactStats{}, err
	}
	return CompactStats{RetainedOffset: watermark, DeletedRows: deleted}, nil
}

// Snapshot returns the current matching rows pinned to a log offset.
func (e *Electrolite) Snapshot(s Shape) (*Snapshot, error) {
	if s.SchemaVersion == 0 {
		s.SchemaVersion = 1
	}
	info, err := e.watchedInfo(s.Table)
	if err != nil {
		return nil, err
	}
	normalized, err := normalizePredicate(info, s.Predicate)
	if err != nil {
		return nil, err
	}
	s.Predicate = normalized
	whereSQL, args := compilePredicate(normalized)
	q := fmt.Sprintf("SELECT %s FROM %s", rowJSON("", s.Columns), quoteIdent(s.Table))
	if whereSQL != "" {
		q += " WHERE " + whereSQL
	}
	q += " ORDER BY " + strings.Join(quoteAll(info.PK), ",")
	rows, err := e.db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []map[string]interface{}
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		var v map[string]interface{}
		_ = json.Unmarshal([]byte(s), &v)
		out = append(out, v)
	}
	if out == nil {
		out = []map[string]interface{}{}
	}
	offset, err := e.highWater()
	if err != nil {
		return nil, err
	}
	logID, err := e.LogID()
	if err != nil {
		return nil, err
	}
	return &Snapshot{
		LogID:       logID,
		ShapeHandle: shapeHandle(s),
		KeyColumns:  info.PK,
		Rows:        out,
		Offset:      offset,
	}, nil
}

// ReplicaMode controls whether UPDATE messages carry the full new
// row or only the changed columns.
type ReplicaMode string

const (
	ReplicaFull ReplicaMode = "full"
	ReplicaDiff ReplicaMode = "diff"
)

// Replay returns logical changes after offset, snapping batch
// boundaries. Defaults to ReplicaFull; use ReplayWithReplica to
// request diff-mode UPDATE messages.
func (e *Electrolite) Replay(s Shape, offset, limit int64) (*Replay, error) {
	return e.ReplayWithReplica(s, offset, limit, ReplicaFull)
}

func (e *Electrolite) ReplayWithReplica(s Shape, offset, limit int64, replica ReplicaMode) (*Replay, error) {
	if s.SchemaVersion == 0 {
		s.SchemaVersion = 1
	}
	info, err := e.watchedInfo(s.Table)
	if err != nil {
		return nil, err
	}
	normalized, err := normalizePredicate(info, s.Predicate)
	if err != nil {
		return nil, err
	}
	s.Predicate = normalized
	retained, err := e.retainedOffset(s.Table)
	if err != nil {
		return nil, err
	}
	if offset < retained {
		return nil, errResync{}
	}
	if limit < 1 {
		limit = 1
	}
	rows, err := e.readLogPage(s.Table, offset, limit)
	if err != nil {
		return nil, err
	}
	latest := offset
	var msgs []Message
	for _, r := range rows {
		if r.Seq > latest {
			latest = r.Seq
		}
		msgs = append(msgs, messagesFor(s.Predicate, r, replica)...)
	}
	if msgs == nil {
		msgs = []Message{}
	}
	var newer int64
	err = e.db.QueryRow(
		"SELECT 1 FROM _electrolite_log WHERE table_name = ? AND seq > ? LIMIT 1",
		s.Table, latest,
	).Scan(&newer)
	upToDate := err == sql.ErrNoRows
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	logID, err := e.LogID()
	if err != nil {
		return nil, err
	}
	r := &Replay{
		LogID:       logID,
		ShapeHandle: shapeHandle(s),
		Messages:    msgs,
		Offset:      latest,
		UpToDate:    upToDate,
	}
	if replica == ReplicaDiff {
		r.Replica = "diff"
	}
	return r, nil
}

// WaitForChange blocks until Exec/ExecBatch/WriteBatch fires or LiveTimeout.
func (e *Electrolite) WaitForChange() {
	e.mu.Lock()
	defer e.mu.Unlock()
	done := make(chan struct{})
	go func() {
		select {
		case <-time.After(e.LiveTimeout):
			e.mu.Lock()
			e.cond.Broadcast()
			e.mu.Unlock()
		case <-done:
		}
	}()
	e.cond.Wait()
	close(done)
}

func (e *Electrolite) notify() {
	e.mu.Lock()
	e.cond.Broadcast()
	e.mu.Unlock()
}

// HandleResponse is the tuple returned by Handle.
type HandleResponse struct {
	Status int
	Body   interface{}
}

// Handle parses an Electrolite request and serves snapshot/replay/live
// from registered shapes. Returns a status code and a body suitable for
// JSON-encoding to the response. context is opaque to the engine and
// passed to the shape's where/scope/authorize callbacks.
func (e *Electrolite) Handle(path, query string, context interface{}) HandleResponse {
	route, ok, err := e.parseRoute(path, query)
	if err != nil {
		return HandleResponse{Status: 400, Body: map[string]string{"error": "bad_request", "detail": err.Error()}}
	}
	if !ok {
		return HandleResponse{Status: 404, Body: errBody("shape_not_found")}
	}
	def, ok := e.shapes[route.Name]
	if !ok {
		return HandleResponse{Status: 404, Body: errBody("shape_not_found")}
	}
	if len(route.Params) != len(def.Params) {
		return HandleResponse{Status: 404, Body: errBody("shape_not_found")}
	}
	params := map[string]string{}
	for i, name := range def.Params {
		params[name] = route.Params[i]
	}

	build := BuildContext{Params: params, Context: context}
	scope := ""
	if def.Scope != nil {
		scope = def.Scope(build)
	}
	if def.Authorize != nil && !def.Authorize(AuthContext{Params: params, Context: context, Scope: scope}) {
		return HandleResponse{Status: 404, Body: errBody("shape_not_found")}
	}

	predicate := All()
	if def.Where != nil {
		predicate = def.Where(build)
	}
	shape := Shape{
		Table:         def.Table,
		Columns:       def.Columns,
		Predicate:     predicate,
		AuthScope:     scope,
		SchemaVersion: def.SchemaVersion,
	}
	if shape.SchemaVersion == 0 {
		shape.SchemaVersion = 1
	}
	currentHandle := shapeHandle(shape)
	currentLogID, err := e.LogID()
	if err != nil {
		return HandleResponse{Status: 500, Body: errBody("internal")}
	}

	if route.Offset >= 0 {
		if route.LogID != "" && route.LogID != currentLogID {
			return HandleResponse{Status: 409, Body: errBody("resync_required")}
		}
		if route.ShapeHandle != "" && route.ShapeHandle != currentHandle {
			return HandleResponse{Status: 409, Body: errBody("resync_required")}
		}
	}

	if route.Offset < 0 {
		snap, err := e.Snapshot(shape)
		if err != nil {
			if _, ok := err.(errResync); ok {
				return HandleResponse{Status: 409, Body: errBody("resync_required")}
			}
			if be, ok := err.(errBadInput); ok {
				return HandleResponse{Status: 400, Body: map[string]string{"error": "bad_request", "detail": be.msg}}
			}
			return HandleResponse{Status: 500, Body: errBody("internal")}
		}
		return HandleResponse{Status: 200, Body: map[string]interface{}{
			"type":         "snapshot",
			"log_id":       snap.LogID,
			"shape_handle": snap.ShapeHandle,
			"key_columns":  snap.KeyColumns,
			"rows":         snap.Rows,
			"offset":       snap.Offset,
			"up_to_date":   true,
		}}
	}

	body, err := e.ReplayWithReplica(shape, route.Offset, e.replayLimit, route.Replica)
	if err != nil {
		if _, ok := err.(errResync); ok {
			return HandleResponse{Status: 409, Body: errBody("resync_required")}
		}
		if be, ok := err.(errBadInput); ok {
			return HandleResponse{Status: 400, Body: map[string]string{"error": "bad_request", "detail": be.msg}}
		}
		return HandleResponse{Status: 500, Body: errBody("internal")}
	}
	if route.Live && len(body.Messages) == 0 && body.UpToDate {
		e.waitUntil(time.Now().Add(e.LiveTimeout))
		if e.stopped.Load() {
			return HandleResponse{Status: 200, Body: map[string]interface{}{
				"type":         "replay",
				"log_id":       currentLogID,
				"shape_handle": currentHandle,
				"messages":     []Message{},
				"offset":       route.Offset,
				"up_to_date":   true,
				"shutdown":     true,
			}}
		}
		body, err = e.ReplayWithReplica(shape, route.Offset, e.replayLimit, route.Replica)
		if err != nil {
			if _, ok := err.(errResync); ok {
				return HandleResponse{Status: 409, Body: errBody("resync_required")}
			}
			if be, ok := err.(errBadInput); ok {
				return HandleResponse{Status: 400, Body: map[string]string{"error": "bad_request", "detail": be.msg}}
			}
			return HandleResponse{Status: 500, Body: errBody("internal")}
		}
	}
	out := map[string]interface{}{
		"type":         "replay",
		"log_id":       body.LogID,
		"shape_handle": body.ShapeHandle,
		"messages":     body.Messages,
		"offset":       body.Offset,
		"up_to_date":   body.UpToDate,
	}
	if body.Replica != "" {
		out["replica"] = body.Replica
	}
	return HandleResponse{Status: 200, Body: out}
}

func (e *Electrolite) waitUntil(deadline time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	done := make(chan struct{})
	go func() {
		select {
		case <-time.After(time.Until(deadline)):
			e.mu.Lock()
			e.cond.Broadcast()
			e.mu.Unlock()
		case <-done:
		}
	}()
	e.cond.Wait()
	close(done)
}

func errBody(code string) map[string]string {
	return map[string]string{"error": code}
}

type route struct {
	Name        string
	Params      []string
	Offset      int64
	Live        bool
	LogID       string
	ShapeHandle string
	Replica     ReplicaMode
}

// parseRoute returns (route, ok=true) on success, (zero, ok=false)
// when path is outside the prefix, and (zero, ok=false) with a
// non-empty err when an inner validation (e.g. offset) failed.
func (e *Electrolite) parseRoute(path, query string) (route, bool, error) {
	prefix := e.prefix + "/"
	if !strings.HasPrefix(path, prefix) {
		return route{}, false, nil
	}
	rest := path[len(prefix):]
	parts := []string{}
	for _, p := range strings.Split(rest, "/") {
		if p != "" {
			parts = append(parts, p)
		}
	}
	if len(parts) == 0 {
		return route{}, false, nil
	}
	values, _ := url.ParseQuery(strings.TrimPrefix(query, "?"))
	off := int64(-1)
	if v := values.Get("offset"); v != "" {
		x, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return route{}, false, fmt.Errorf("offset must be an integer, got %q", v)
		}
		off = x
	}
	replica := ReplicaFull
	if values.Get("replica") == "diff" {
		replica = ReplicaDiff
	}
	return route{
		Name:        parts[0],
		Params:      parts[1:],
		Offset:      off,
		Live:        values.Get("live") == "true",
		LogID:       values.Get("log_id"),
		ShapeHandle: values.Get("shape_handle"),
		Replica:     replica,
	}, true, nil
}

// LogID returns the current SQLite log identity.
func (e *Electrolite) LogID() (string, error) {
	var id string
	err := e.db.QueryRow("SELECT value FROM _electrolite_meta WHERE key = 'log_id'").Scan(&id)
	return id, err
}

func (e *Electrolite) highWater() (int64, error) {
	var n int64
	err := e.db.QueryRow("SELECT COALESCE(MAX(seq), 0) FROM _electrolite_log").Scan(&n)
	return n, err
}

func (e *Electrolite) retainedOffset(table string) (int64, error) {
	var v string
	err := e.db.QueryRow("SELECT value FROM _electrolite_meta WHERE key = ?", "retained_offset:"+table).Scan(&v)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	n, _ := strconv.ParseInt(v, 10, 64)
	return n, nil
}

type tableInfo struct {
	Columns     []string
	PK          []string
	ColumnTypes map[string]string
}

func isBooleanish(decl string) bool {
	return strings.Contains(strings.ToUpper(decl), "BOOL")
}

// normalizeValue coerces a single predicate value against a column's
// declared type. Booleans against BOOLEAN-affinity columns become 0/1.
// Booleans against any other column are an error.
func normalizeValue(info *tableInfo, column string, value interface{}) (interface{}, error) {
	hasColumn := false
	for _, c := range info.Columns {
		if c == column {
			hasColumn = true
			break
		}
	}
	if !hasColumn {
		return nil, errBadInput{msg: fmt.Sprintf("predicate column %s does not exist", column)}
	}
	colType := info.ColumnTypes[column]
	if b, ok := value.(bool); ok {
		if isBooleanish(colType) {
			if b {
				return 1, nil
			}
			return 0, nil
		}
		return nil, errBadInput{msg: "boolean predicates require BOOLEAN columns"}
	}
	return value, nil
}

// normalizePredicate walks a predicate tree and coerces values against
// the table info. Single normalization site; downstream code uses the
// already-normalized predicate.
func normalizePredicate(info *tableInfo, p Predicate) (Predicate, error) {
	switch x := p.(type) {
	case nil, AllPredicate:
		return AllPredicate{}, nil
	case EqPredicate:
		v, err := normalizeValue(info, x.Column, x.Value)
		if err != nil {
			return nil, err
		}
		return EqPredicate{Column: x.Column, Value: v}, nil
	case RangePredicate:
		if x.Value == nil {
			return nil, errBadInput{msg: fmt.Sprintf("range predicate %s requires a non-null value", x.Op)}
		}
		v, err := normalizeValue(info, x.Column, x.Value)
		if err != nil {
			return nil, err
		}
		return RangePredicate{Op: x.Op, Column: x.Column, Value: v}, nil
	case InPredicate:
		out := make([]interface{}, 0, len(x.Values))
		for _, v := range x.Values {
			nv, err := normalizeValue(info, x.Column, v)
			if err != nil {
				return nil, err
			}
			out = append(out, nv)
		}
		return InPredicate{Column: x.Column, Values: out}, nil
	case AndPredicate:
		children := make([]Predicate, 0, len(x.Predicates))
		for _, c := range x.Predicates {
			nc, err := normalizePredicate(info, c)
			if err != nil {
				return nil, err
			}
			children = append(children, nc)
		}
		return AndPredicate{Predicates: children}, nil
	case OrPredicate:
		children := make([]Predicate, 0, len(x.Predicates))
		for _, c := range x.Predicates {
			nc, err := normalizePredicate(info, c)
			if err != nil {
				return nil, err
			}
			children = append(children, nc)
		}
		return OrPredicate{Predicates: children}, nil
	case NotPredicate:
		nc, err := normalizePredicate(info, x.Predicate)
		if err != nil {
			return nil, err
		}
		return NotPredicate{Predicate: nc}, nil
	}
	return nil, fmt.Errorf("unsupported predicate type %T", p)
}

func (e *Electrolite) inspectTable(name string) (*tableInfo, error) {
	rows, err := e.db.Query(fmt.Sprintf("PRAGMA table_info(%s)", quoteString(name)))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	type col struct {
		cid     int
		name    string
		colType string
		pk      int
	}
	var cs []col
	for rows.Next() {
		var c col
		var typeName, dflt sql.NullString
		var notnull int
		if err := rows.Scan(&c.cid, &c.name, &typeName, &notnull, &dflt, &c.pk); err != nil {
			return nil, err
		}
		c.colType = typeName.String
		cs = append(cs, c)
	}
	if len(cs) == 0 {
		return nil, fmt.Errorf("table %s does not exist", name)
	}
	sort.Slice(cs, func(i, j int) bool { return cs[i].cid < cs[j].cid })
	info := &tableInfo{ColumnTypes: map[string]string{}}
	for _, c := range cs {
		info.Columns = append(info.Columns, c.name)
		info.ColumnTypes[c.name] = c.colType
	}
	pks := make([]col, 0)
	for _, c := range cs {
		if c.pk > 0 {
			pks = append(pks, c)
		}
	}
	sort.Slice(pks, func(i, j int) bool { return pks[i].pk < pks[j].pk })
	for _, c := range pks {
		info.PK = append(info.PK, c.name)
	}
	return info, nil
}

func (e *Electrolite) watchedInfo(name string) (*tableInfo, error) {
	info, err := e.inspectTable(name)
	if err != nil {
		return nil, err
	}
	var pkJSON string
	err = e.db.QueryRow("SELECT pk_columns FROM _electrolite_watched_tables WHERE table_name = ?", name).Scan(&pkJSON)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("table %s is not watched by Electrolite", name)
	}
	if err != nil {
		return nil, err
	}
	var pk []string
	_ = json.Unmarshal([]byte(pkJSON), &pk)
	info.PK = pk
	return info, nil
}

type logRow struct {
	Seq     int64
	BatchID string
	Op      string
	PK      map[string]interface{}
	OldPK   map[string]interface{}
	NewPK   map[string]interface{}
	OldRow  map[string]interface{}
	NewRow  map[string]interface{}
}

func (e *Electrolite) readLogPage(table string, offset, limit int64) ([]logRow, error) {
	rows, err := e.db.Query(
		`SELECT seq, batch_id, op, pk_json, old_pk_json, new_pk_json, old_json, new_json
		 FROM _electrolite_log WHERE table_name = ? AND seq > ? ORDER BY seq LIMIT ?`,
		table, offset, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []logRow
	for rows.Next() {
		r, err := scanLogRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	if len(out) > 0 {
		last := out[len(out)-1]
		// Extend until the trailing batch finishes or until we hit
		// the safety cap. Without the cap, a 10M-row batch would
		// force replay to load every row in one response.
		extensionCap := limit * 10
		if extensionCap < limit {
			extensionCap = limit
		}
		more, err := e.db.Query(
			`SELECT seq, batch_id, op, pk_json, old_pk_json, new_pk_json, old_json, new_json
			 FROM _electrolite_log WHERE table_name = ? AND seq > ? AND batch_id = ? ORDER BY seq LIMIT ?`,
			table, last.Seq, last.BatchID, extensionCap,
		)
		if err != nil {
			return nil, err
		}
		defer more.Close()
		for more.Next() {
			r, err := scanLogRow(more)
			if err != nil {
				return nil, err
			}
			out = append(out, r)
		}
	}
	return out, nil
}

func scanLogRow(rows *sql.Rows) (logRow, error) {
	var r logRow
	var pk, oldPK, newPK, oldJSON, newJSON sql.NullString
	if err := rows.Scan(&r.Seq, &r.BatchID, &r.Op, &pk, &oldPK, &newPK, &oldJSON, &newJSON); err != nil {
		return r, err
	}
	if pk.Valid {
		_ = json.Unmarshal([]byte(pk.String), &r.PK)
	}
	if oldPK.Valid {
		_ = json.Unmarshal([]byte(oldPK.String), &r.OldPK)
	}
	if newPK.Valid {
		_ = json.Unmarshal([]byte(newPK.String), &r.NewPK)
	}
	if oldJSON.Valid {
		_ = json.Unmarshal([]byte(oldJSON.String), &r.OldRow)
	}
	if newJSON.Valid {
		_ = json.Unmarshal([]byte(newJSON.String), &r.NewRow)
	}
	return r, nil
}

func messagesFor(p Predicate, r logRow, replica ReplicaMode) []Message {
	oldMatch := predicateMatches(p, r.OldRow)
	newMatch := predicateMatches(p, r.NewRow)
	oldKey := r.OldPK
	if oldKey == nil {
		oldKey = r.PK
	}
	newKey := r.NewPK
	if newKey == nil {
		newKey = r.PK
	}
	if !oldMatch && newMatch && r.NewRow != nil {
		// Predicate-transition INSERT always carries full row.
		return []Message{newMsg("insert", r, newKey, r.NewRow)}
	}
	if oldMatch && newMatch && r.NewRow != nil {
		if mapEq(oldKey, newKey) {
			value := r.NewRow
			if replica == ReplicaDiff && r.OldRow != nil {
				value = diffRow(r.OldRow, r.NewRow)
				if len(value) == 0 {
					return nil
				}
			}
			return []Message{newMsg("update", r, newKey, value)}
		}
		return []Message{newMsg("delete", r, oldKey, nil), newMsg("insert", r, newKey, r.NewRow)}
	}
	if oldMatch && !newMatch {
		return []Message{newMsg("delete", r, oldKey, nil)}
	}
	return nil
}

func diffRow(old, new map[string]interface{}) map[string]interface{} {
	out := map[string]interface{}{}
	for k, v := range new {
		if !jsonEqual(old[k], v) {
			out[k] = v
		}
	}
	return out
}

func newMsg(kind string, r logRow, key, value map[string]interface{}) Message {
	return Message{Type: kind, BatchID: r.BatchID, Key: key, Offset: r.Seq, Value: value}
}

// PredicateMatches evaluates a predicate against a row map. Used by
// the conformance harness to compare in-process matching against SQL
// `WHERE` results.
func PredicateMatches(p Predicate, row map[string]interface{}) bool {
	return predicateMatches(p, row)
}

// PredicateFromJSON parses a JSON-decoded predicate object (per the
// wire format) into a Predicate variant.
func PredicateFromJSON(raw map[string]interface{}) (Predicate, error) {
	kind, _ := raw["type"].(string)
	switch kind {
	case "all", "":
		return AllPredicate{}, nil
	case "eq":
		col, _ := raw["column"].(string)
		return EqPredicate{Column: col, Value: raw["value"]}, nil
	case "gt", "lt", "gte", "lte":
		col, _ := raw["column"].(string)
		return RangePredicate{Op: kind, Column: col, Value: raw["value"]}, nil
	case "in":
		col, _ := raw["column"].(string)
		var values []interface{}
		if vs, ok := raw["values"].([]interface{}); ok {
			values = vs
		}
		return InPredicate{Column: col, Values: values}, nil
	case "and":
		var children []Predicate
		if cs, ok := raw["predicates"].([]interface{}); ok {
			for _, c := range cs {
				cm, ok := c.(map[string]interface{})
				if !ok {
					return nil, fmt.Errorf("and child not an object")
				}
				child, err := PredicateFromJSON(cm)
				if err != nil {
					return nil, err
				}
				children = append(children, child)
			}
		}
		return AndPredicate{Predicates: children}, nil
	case "or":
		var children []Predicate
		if cs, ok := raw["predicates"].([]interface{}); ok {
			for _, c := range cs {
				cm, ok := c.(map[string]interface{})
				if !ok {
					return nil, fmt.Errorf("or child not an object")
				}
				child, err := PredicateFromJSON(cm)
				if err != nil {
					return nil, err
				}
				children = append(children, child)
			}
		}
		return OrPredicate{Predicates: children}, nil
	case "not":
		inner, ok := raw["predicate"].(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("not missing predicate")
		}
		child, err := PredicateFromJSON(inner)
		if err != nil {
			return nil, err
		}
		return NotPredicate{Predicate: child}, nil
	}
	return nil, fmt.Errorf("unknown predicate type: %s", kind)
}

func predicateMatches(p Predicate, row map[string]interface{}) bool {
	if row == nil {
		return false
	}
	switch x := p.(type) {
	case nil, AllPredicate:
		return true
	case EqPredicate:
		return jsonEqual(row[x.Column], x.Value)
	case RangePredicate:
		return compareScalar(row[x.Column], x.Value, x.Op)
	case InPredicate:
		for _, v := range x.Values {
			if jsonEqual(row[x.Column], v) {
				return true
			}
		}
		return false
	case AndPredicate:
		for _, c := range x.Predicates {
			if !predicateMatches(c, row) {
				return false
			}
		}
		return true
	case OrPredicate:
		for _, c := range x.Predicates {
			if predicateMatches(c, row) {
				return true
			}
		}
		return false
	case NotPredicate:
		return !predicateMatches(x.Predicate, row)
	}
	return false
}

// inPlaceholders returns a comma-joined list of `?` placeholders.
func inPlaceholders(n int) string {
	if n == 0 {
		return ""
	}
	parts := make([]string, n)
	for i := range parts {
		parts[i] = "?"
	}
	return strings.Join(parts, ",")
}

func compilePredicate(p Predicate) (string, []interface{}) {
	switch x := p.(type) {
	case nil, AllPredicate:
		return "", nil
	case EqPredicate:
		if x.Value == nil {
			return fmt.Sprintf("%s IS NULL", quoteIdent(x.Column)), nil
		}
		return fmt.Sprintf("%s = ?", quoteIdent(x.Column)), []interface{}{x.Value}
	case RangePredicate:
		if x.Value == nil {
			return "0", nil
		}
		return fmt.Sprintf("%s %s ?", quoteIdent(x.Column), rangeOps[x.Op]), []interface{}{x.Value}
	case InPredicate:
		if len(x.Values) == 0 {
			return "0", nil
		}
		var nonNull []interface{}
		hasNull := false
		for _, v := range x.Values {
			if v == nil {
				hasNull = true
			} else {
				nonNull = append(nonNull, v)
			}
		}
		var parts []string
		var args []interface{}
		if len(nonNull) > 0 {
			parts = append(parts, fmt.Sprintf("%s IN (%s)", quoteIdent(x.Column), inPlaceholders(len(nonNull))))
			args = append(args, nonNull...)
		}
		if hasNull {
			parts = append(parts, fmt.Sprintf("%s IS NULL", quoteIdent(x.Column)))
		}
		joined := []string{}
		for _, p := range parts {
			joined = append(joined, "("+p+")")
		}
		return strings.Join(joined, " OR "), args
	case AndPredicate:
		var parts []string
		var args []interface{}
		for _, c := range x.Predicates {
			where, a := compilePredicate(c)
			if where != "" {
				parts = append(parts, "("+where+")")
				args = append(args, a...)
			}
		}
		return strings.Join(parts, " AND "), args
	case OrPredicate:
		var parts []string
		var args []interface{}
		for _, c := range x.Predicates {
			where, a := compilePredicate(c)
			if where != "" {
				parts = append(parts, "("+where+")")
				args = append(args, a...)
			}
		}
		if len(parts) == 0 {
			return "", nil
		}
		return strings.Join(parts, " OR "), args
	case NotPredicate:
		// Special case: NOT (col IS NULL) → col IS NOT NULL.
		if eq, ok := x.Predicate.(EqPredicate); ok && eq.Value == nil {
			return fmt.Sprintf("%s IS NOT NULL", quoteIdent(eq.Column)), nil
		}
		sql, args := compilePredicate(x.Predicate)
		if sql == "" {
			return "0", nil
		}
		return "NOT (" + sql + ")", args
	}
	return "", nil
}

func compareScalar(left, right interface{}, op string) bool {
	if left == nil || right == nil {
		return false
	}
	if l, ok := left.(string); ok {
		r, ok := right.(string)
		if !ok {
			return false
		}
		return cmpStr(l, r, op)
	}
	if l, ok := toFloat(left); ok {
		if r, ok := toFloat(right); ok {
			return cmp(l, r, op)
		}
	}
	return false
}

func toFloat(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case int32:
		return float64(n), true
	}
	return 0, false
}

func cmp(a, b float64, op string) bool {
	switch op {
	case "gt":
		return a > b
	case "lt":
		return a < b
	case "gte":
		return a >= b
	case "lte":
		return a <= b
	}
	return false
}

func cmpStr(a, b, op string) bool {
	switch op {
	case "gt":
		return a > b
	case "lt":
		return a < b
	case "gte":
		return a >= b
	case "lte":
		return a <= b
	}
	return false
}

func rowJSON(prefix string, columns []string) string {
	q := ""
	if prefix != "" {
		q = prefix + "."
	}
	parts := make([]string, len(columns))
	for i, c := range columns {
		parts[i] = fmt.Sprintf("%s, %s%s", quoteString(c), q, quoteIdent(c))
	}
	return "json_object(" + strings.Join(parts, ", ") + ")"
}

func shapeHandle(s Shape) string {
	cols := append([]string{}, s.Columns...)
	sort.Strings(cols)
	if s.SchemaVersion == 0 {
		s.SchemaVersion = 1
	}
	body := map[string]interface{}{
		"auth_scope":     s.AuthScope,
		"columns":        cols,
		"predicate":      predicateToJSON(s.Predicate),
		"schema_version": s.SchemaVersion,
		"table":          s.Table,
	}
	b, _ := jsonCanonical(body)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func predicateToJSON(p Predicate) interface{} {
	switch x := p.(type) {
	case nil, AllPredicate:
		return map[string]interface{}{"type": "all"}
	case EqPredicate:
		return map[string]interface{}{"type": "eq", "column": x.Column, "value": x.Value}
	case RangePredicate:
		return map[string]interface{}{"type": x.Op, "column": x.Column, "value": x.Value}
	case InPredicate:
		// dedupe + sort by JSON encoding for deterministic handle
		seen := map[string]interface{}{}
		for _, v := range x.Values {
			b, _ := json.Marshal(v)
			seen[string(b)] = v
		}
		keys := make([]string, 0, len(seen))
		for k := range seen {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out := make([]interface{}, 0, len(keys))
		for _, k := range keys {
			out = append(out, seen[k])
		}
		return map[string]interface{}{"type": "in", "column": x.Column, "values": out}
	case AndPredicate:
		children := make([]interface{}, 0, len(x.Predicates))
		for _, c := range x.Predicates {
			children = append(children, predicateToJSON(c))
		}
		sort.Slice(children, func(i, j int) bool {
			a, _ := json.Marshal(children[i])
			b, _ := json.Marshal(children[j])
			return string(a) < string(b)
		})
		return map[string]interface{}{"type": "and", "predicates": children}
	case OrPredicate:
		children := make([]interface{}, 0, len(x.Predicates))
		for _, c := range x.Predicates {
			children = append(children, predicateToJSON(c))
		}
		sort.Slice(children, func(i, j int) bool {
			a, _ := json.Marshal(children[i])
			b, _ := json.Marshal(children[j])
			return string(a) < string(b)
		})
		return map[string]interface{}{"type": "or", "predicates": children}
	case NotPredicate:
		return map[string]interface{}{"type": "not", "predicate": predicateToJSON(x.Predicate)}
	}
	return map[string]interface{}{"type": "all"}
}

func jsonCanonical(v interface{}) ([]byte, error) {
	switch t := v.(type) {
	case map[string]interface{}:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var sb strings.Builder
		sb.WriteString("{")
		for i, k := range keys {
			if i > 0 {
				sb.WriteString(",")
			}
			kb, _ := json.Marshal(k)
			sb.Write(kb)
			sb.WriteString(":")
			vb, err := jsonCanonical(t[k])
			if err != nil {
				return nil, err
			}
			sb.Write(vb)
		}
		sb.WriteString("}")
		return []byte(sb.String()), nil
	case []interface{}:
		var sb strings.Builder
		sb.WriteString("[")
		for i, v := range t {
			if i > 0 {
				sb.WriteString(",")
			}
			b, err := jsonCanonical(v)
			if err != nil {
				return nil, err
			}
			sb.Write(b)
		}
		sb.WriteString("]")
		return []byte(sb.String()), nil
	case []string:
		var sb strings.Builder
		sb.WriteString("[")
		for i, s := range t {
			if i > 0 {
				sb.WriteString(",")
			}
			b, _ := json.Marshal(s)
			sb.Write(b)
		}
		sb.WriteString("]")
		return []byte(sb.String()), nil
	default:
		return json.Marshal(v)
	}
}

func mapEq(a, b map[string]interface{}) bool {
	ab, _ := json.Marshal(a)
	bb, _ := json.Marshal(b)
	return string(ab) == string(bb)
}

func jsonEqual(a, b interface{}) bool {
	ab, _ := json.Marshal(a)
	bb, _ := json.Marshal(b)
	return string(ab) == string(bb)
}

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func quoteString(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

func quoteAll(cols []string) []string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = quoteIdent(c)
	}
	return out
}

func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t := time.Now().UnixNano()
		for i := range b {
			b[i] = byte(t >> (i * 8))
		}
	}
	return hex.EncodeToString(b)
}
