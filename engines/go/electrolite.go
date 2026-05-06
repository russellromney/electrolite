// Package electrolite is a tiny experimental Electrolite engine for Go.
//
// It mirrors the Python and Node engines: install SQLite triggers, take
// a snapshot, replay logical changes, and run explicit write batches
// with a shared batch_id. It uses modernc.org/sqlite (pure Go, no cgo).
package electrolite

import (
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// Predicate is "all" or "eq".
type Predicate struct {
	Type   string      `json:"type"`
	Column string      `json:"column,omitempty"`
	Value  interface{} `json:"value,omitempty"`
}

func All() Predicate                           { return Predicate{Type: "all"} }
func Eq(col string, val interface{}) Predicate  { return Predicate{Type: "eq", Column: col, Value: val} }
func Gt(col string, val interface{}) Predicate  { return Predicate{Type: "gt", Column: col, Value: val} }
func Lt(col string, val interface{}) Predicate  { return Predicate{Type: "lt", Column: col, Value: val} }
func Gte(col string, val interface{}) Predicate { return Predicate{Type: "gte", Column: col, Value: val} }
func Lte(col string, val interface{}) Predicate { return Predicate{Type: "lte", Column: col, Value: val} }

var rangeOps = map[string]string{"gt": ">", "lt": "<", "gte": ">=", "lte": "<="}

// Shape is the row subset a client subscribes to.
type Shape struct {
	Table     string
	Columns   []string
	Predicate Predicate
}

// Electrolite holds a SQLite connection and the change-notify primitive.
type Electrolite struct {
	db          *sql.DB
	mu          sync.Mutex
	cond        *sync.Cond
	LiveTimeout time.Duration
}

// Snapshot is the current matching rows plus the log offset they pin to.
type Snapshot struct {
	LogID       string
	ShapeHandle string
	KeyColumns  []string
	Rows        []map[string]interface{}
	Offset      int64
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
	LogID       string
	ShapeHandle string
	Messages    []Message
	Offset      int64
	UpToDate    bool
}

// Open opens or creates a SQLite database and bootstraps Electrolite tables.
func Open(path string) (*Electrolite, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	e := &Electrolite{db: db, LiveTimeout: 20 * time.Second}
	e.cond = sync.NewCond(&e.mu)
	if err := e.bootstrap(); err != nil {
		return nil, err
	}
	return e, nil
}

func (e *Electrolite) Close() error { return e.db.Close() }

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
		_, err = e.db.Exec("INSERT INTO _electrolite_meta (key, value) VALUES ('log_id', ?)", randomHex(16))
	}
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

// Stmt is one SQL statement plus arguments inside a WriteBatch.
type Stmt struct {
	SQL  string
	Args []interface{}
}

// Snapshot returns the current matching rows pinned to a log offset.
func (e *Electrolite) Snapshot(s Shape) (*Snapshot, error) {
	info, err := e.watchedInfo(s.Table)
	if err != nil {
		return nil, err
	}
	whereSQL, args := compilePredicate(s.Predicate)
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

// Replay returns logical changes after offset, snapping batch boundaries.
func (e *Electrolite) Replay(s Shape, offset, limit int64) (*Replay, error) {
	if _, err := e.watchedInfo(s.Table); err != nil {
		return nil, err
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
		msgs = append(msgs, messagesFor(s.Predicate, r)...)
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
	return &Replay{
		LogID:       logID,
		ShapeHandle: shapeHandle(s),
		Messages:    msgs,
		Offset:      latest,
		UpToDate:    upToDate,
	}, nil
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

type tableInfo struct {
	Columns []string
	PK      []string
}

func (e *Electrolite) inspectTable(name string) (*tableInfo, error) {
	rows, err := e.db.Query(fmt.Sprintf("PRAGMA table_info(%s)", quoteString(name)))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	type col struct {
		cid  int
		name string
		pk   int
	}
	var cs []col
	for rows.Next() {
		var c col
		var typeName, dflt sql.NullString
		var notnull int
		if err := rows.Scan(&c.cid, &c.name, &typeName, &notnull, &dflt, &c.pk); err != nil {
			return nil, err
		}
		cs = append(cs, c)
	}
	if len(cs) == 0 {
		return nil, fmt.Errorf("table %s does not exist", name)
	}
	sort.Slice(cs, func(i, j int) bool { return cs[i].cid < cs[j].cid })
	info := &tableInfo{}
	for _, c := range cs {
		info.Columns = append(info.Columns, c.name)
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
		more, err := e.db.Query(
			`SELECT seq, batch_id, op, pk_json, old_pk_json, new_pk_json, old_json, new_json
			 FROM _electrolite_log WHERE table_name = ? AND seq > ? AND batch_id = ? ORDER BY seq`,
			table, last.Seq, last.BatchID,
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

func messagesFor(p Predicate, r logRow) []Message {
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
		return []Message{newMsg("insert", r, newKey, r.NewRow)}
	}
	if oldMatch && newMatch && r.NewRow != nil {
		if mapEq(oldKey, newKey) {
			return []Message{newMsg("update", r, newKey, r.NewRow)}
		}
		return []Message{newMsg("delete", r, oldKey, nil), newMsg("insert", r, newKey, r.NewRow)}
	}
	if oldMatch && !newMatch {
		return []Message{newMsg("delete", r, oldKey, nil)}
	}
	return nil
}

func newMsg(kind string, r logRow, key, value map[string]interface{}) Message {
	return Message{Type: kind, BatchID: r.BatchID, Key: key, Offset: r.Seq, Value: value}
}

func predicateMatches(p Predicate, row map[string]interface{}) bool {
	if row == nil {
		return false
	}
	switch p.Type {
	case "all", "":
		return true
	case "eq":
		return jsonEqual(row[p.Column], p.Value)
	case "gt", "lt", "gte", "lte":
		return compareScalar(row[p.Column], p.Value, p.Type)
	}
	return false
}

func compilePredicate(p Predicate) (string, []interface{}) {
	switch p.Type {
	case "all", "":
		return "", nil
	case "eq":
		if p.Value == nil {
			return fmt.Sprintf("%s IS NULL", quoteIdent(p.Column)), nil
		}
		return fmt.Sprintf("%s = ?", quoteIdent(p.Column)), []interface{}{p.Value}
	case "gt", "lt", "gte", "lte":
		if p.Value == nil {
			return "0", nil
		}
		return fmt.Sprintf("%s %s ?", quoteIdent(p.Column), rangeOps[p.Type]), []interface{}{p.Value}
	}
	return "", nil
}

func compareScalar(left, right interface{}, op string) bool {
	if left == nil || right == nil {
		return false
	}
	switch l := left.(type) {
	case float64:
		r, ok := toFloat(right)
		if !ok {
			return false
		}
		return cmp(l, r, op)
	case string:
		r, ok := right.(string)
		if !ok {
			return false
		}
		return cmpStr(l, r, op)
	case bool:
		// JSON unmarshals booleans rarely show up in range comparisons; skip.
		return false
	}
	if r, ok := toFloat(left); ok {
		if rr, ok := toFloat(right); ok {
			return cmp(r, rr, op)
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
	body := map[string]interface{}{
		"table":          s.Table,
		"columns":        cols,
		"predicate":      s.Predicate,
		"auth_scope":     "",
		"schema_version": 1,
	}
	b, _ := jsonCanonical(body)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// jsonCanonical writes a canonical (sorted-keys) JSON encoding.
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
		// Fall back to time-based seed if /dev/urandom is unavailable.
		t := time.Now().UnixNano()
		for i := range b {
			b[i] = byte(t >> (i * 8))
		}
	}
	return hex.EncodeToString(b)
}
