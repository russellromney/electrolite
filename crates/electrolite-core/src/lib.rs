use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, HashMap, HashSet};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Shape {
    pub name: String,
    pub table: String,
    pub columns: Vec<String>,
    pub predicate: Predicate,
    pub auth_scope: String,
    pub schema_version: u64,
}

impl Shape {
    pub fn handle(&self) -> String {
        let bytes = serde_json::to_vec(&self.canonical_handle_input())
            .expect("shape handle serialization is infallible");
        let digest = Sha256::digest(bytes);
        format!("{digest:x}")
    }

    fn canonical_handle_input(&self) -> CanonicalShape<'_> {
        let mut columns = self.columns.clone();
        columns.sort();
        columns.dedup();
        CanonicalShape {
            table: &self.table,
            columns,
            predicate: self.predicate.canonical(),
            auth_scope: &self.auth_scope,
            schema_version: self.schema_version,
        }
    }
}

#[derive(Debug, Serialize)]
struct CanonicalShape<'a> {
    table: &'a str,
    columns: Vec<String>,
    predicate: Predicate,
    auth_scope: &'a str,
    schema_version: u64,
}

#[derive(Debug, Clone, Default)]
pub struct ShapeRegistry {
    shapes: HashMap<String, Shape>,
}

impl ShapeRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, shape: Shape) -> Option<Shape> {
        self.shapes.insert(shape.name.clone(), shape)
    }

    pub fn get(&self, name: &str) -> Option<&Shape> {
        self.shapes.get(name)
    }

    pub fn contains(&self, name: &str) -> bool {
        self.shapes.contains_key(name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Predicate {
    All,
    Eq { column: String, value: Value },
    In { column: String, values: Vec<Value> },
    And { predicates: Vec<Predicate> },
}

impl Predicate {
    pub fn matches(&self, row: Option<&Value>) -> bool {
        let Some(Value::Object(row)) = row else {
            return false;
        };
        self.matches_object(row)
    }

    pub fn eq_terms(&self) -> Vec<PredicateEqTerm> {
        let mut terms = Vec::new();
        self.collect_eq_terms(&mut terms);
        terms
    }

    fn matches_object(&self, row: &serde_json::Map<String, Value>) -> bool {
        match self {
            Predicate::All => true,
            Predicate::Eq { column, value } => row.get(column) == Some(value),
            Predicate::In { column, values } => row
                .get(column)
                .map(|value| values.iter().any(|candidate| candidate == value))
                .unwrap_or(false),
            Predicate::And { predicates } => predicates.iter().all(|p| p.matches_object(row)),
        }
    }

    fn collect_eq_terms(&self, terms: &mut Vec<PredicateEqTerm>) {
        match self {
            Predicate::All => {}
            Predicate::Eq { column, value } => terms.push(PredicateEqTerm {
                column: column.clone(),
                value: value.clone(),
            }),
            Predicate::In { column, values } => {
                for value in values {
                    terms.push(PredicateEqTerm {
                        column: column.clone(),
                        value: value.clone(),
                    });
                }
            }
            Predicate::And { predicates } => {
                for predicate in predicates {
                    predicate.collect_eq_terms(terms);
                }
            }
        }
    }

    pub fn canonical(&self) -> Predicate {
        match self {
            Predicate::All => Predicate::All,
            Predicate::Eq { column, value } => Predicate::Eq {
                column: column.clone(),
                value: value.clone(),
            },
            Predicate::In { column, values } => {
                let mut seen = BTreeSet::new();
                let mut values = values
                    .iter()
                    .filter(|value| {
                        seen.insert(
                            serde_json::to_string(value)
                                .expect("predicate values serialize to canonical JSON"),
                        )
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                values.sort_by_key(|value| {
                    serde_json::to_string(value)
                        .expect("predicate values serialize to canonical JSON")
                });
                if let [value] = values.as_slice() {
                    Predicate::Eq {
                        column: column.clone(),
                        value: value.clone(),
                    }
                } else {
                    Predicate::In {
                        column: column.clone(),
                        values,
                    }
                }
            }
            Predicate::And { predicates } => {
                let mut predicates = predicates
                    .iter()
                    .flat_map(|predicate| match predicate.canonical() {
                        Predicate::And { predicates } => predicates,
                        predicate => vec![predicate],
                    })
                    .collect::<Vec<_>>();
                predicates.sort_by_key(|predicate| {
                    serde_json::to_string(predicate)
                        .expect("predicates serialize to canonical JSON")
                });
                predicates.dedup();
                match predicates.as_slice() {
                    [] => Predicate::All,
                    [predicate] => predicate.clone(),
                    _ => Predicate::And { predicates },
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PredicateEqTerm {
    pub column: String,
    pub value: Value,
}

#[derive(Debug, Clone, Default)]
pub struct ShapeIndex {
    shapes: HashMap<String, Shape>,
    handles_by_name: HashMap<String, String>,
    table_scans: HashMap<String, Vec<String>>,
    equality: HashMap<EqIndexKey, Vec<String>>,
}

impl ShapeIndex {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, shape: Shape) -> Option<Shape> {
        let handle = shape.handle();
        let previous_handle = self
            .handles_by_name
            .insert(shape.name.clone(), handle.clone());
        let previous = previous_handle
            .as_ref()
            .and_then(|handle| self.shapes.get(handle))
            .cloned();
        if let Some(previous_handle) = previous_handle.as_ref() {
            if previous_handle != &handle && !self.handle_has_name(previous_handle) {
                self.shapes.remove(previous_handle);
                self.remove_from_indexes(previous_handle);
            }
        }
        self.remove_from_indexes(&handle);

        self.shapes.insert(handle.clone(), shape.clone());
        let terms = shape.predicate.eq_terms();
        if terms.is_empty() {
            self.table_scans
                .entry(shape.table.clone())
                .or_default()
                .push(handle);
            return previous;
        }

        let mut seen_terms = HashSet::new();
        for term in terms {
            let key = EqIndexKey::new(&shape.table, &term.column, &term.value);
            if seen_terms.insert(key.clone()) {
                self.equality.entry(key).or_default().push(handle.clone());
            }
        }

        previous
    }

    pub fn candidates_for_log(&self, row: &LogRow) -> Vec<&Shape> {
        let mut handles = HashSet::new();
        if let Some(table_scan_handles) = self.table_scans.get(&row.table_name) {
            handles.extend(table_scan_handles.iter().cloned());
        }

        for key in row_equality_keys(&row.table_name, row.old_json.as_ref())
            .into_iter()
            .chain(row_equality_keys(&row.table_name, row.new_json.as_ref()))
        {
            if let Some(indexed_handles) = self.equality.get(&key) {
                handles.extend(indexed_handles.iter().cloned());
            }
        }

        let mut shapes = handles
            .iter()
            .filter_map(|handle| self.shapes.get(handle))
            .collect::<Vec<_>>();
        shapes.sort_by(|a, b| a.handle().cmp(&b.handle()));
        shapes
    }

    pub fn len(&self) -> usize {
        self.shapes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.shapes.is_empty()
    }

    fn remove_from_indexes(&mut self, handle: &str) {
        for handles in self.table_scans.values_mut() {
            handles.retain(|candidate| candidate != handle);
        }
        for handles in self.equality.values_mut() {
            handles.retain(|candidate| candidate != handle);
        }
    }

    fn handle_has_name(&self, handle: &str) -> bool {
        self.handles_by_name
            .values()
            .any(|candidate| candidate == handle)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct EqIndexKey {
    table: String,
    column: String,
    value_json: String,
}

impl EqIndexKey {
    fn new(table: &str, column: &str, value: &Value) -> Self {
        Self {
            table: table.to_string(),
            column: column.to_string(),
            value_json: serde_json::to_string(value)
                .expect("predicate values serialize to canonical JSON"),
        }
    }
}

fn row_equality_keys(table: &str, row: Option<&Value>) -> Vec<EqIndexKey> {
    let Some(Value::Object(row)) = row else {
        return Vec::new();
    };
    row.iter()
        .map(|(column, value)| EqIndexKey::new(table, column, value))
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogOp {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogRow {
    pub seq: i64,
    pub batch_id: String,
    pub table_name: String,
    pub op: LogOp,
    pub pk_json: Value,
    pub old_pk_json: Option<Value>,
    pub new_pk_json: Option<Value>,
    pub old_json: Option<Value>,
    pub new_json: Option<Value>,
    pub created_at: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ShapeMessage {
    Insert {
        key: Value,
        value: Value,
        offset: i64,
    },
    Update {
        key: Value,
        value: Value,
        offset: i64,
    },
    Delete {
        key: Value,
        offset: i64,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Snapshot {
    pub key_columns: Vec<String>,
    pub rows: Vec<Value>,
    pub offset: i64,
    pub up_to_date: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Replay {
    pub messages: Vec<ShapeMessage>,
    pub offset: i64,
    pub up_to_date: bool,
}

pub fn message_for_log(shape: &Shape, row: &LogRow) -> Option<ShapeMessage> {
    messages_for_log(shape, row).into_iter().next()
}

pub fn messages_for_log(shape: &Shape, row: &LogRow) -> Vec<ShapeMessage> {
    if row.table_name != shape.table {
        return Vec::new();
    }

    let old_matches = shape.predicate.matches(row.old_json.as_ref());
    let new_matches = shape.predicate.matches(row.new_json.as_ref());
    let old_key = row
        .old_pk_json
        .clone()
        .unwrap_or_else(|| row.pk_json.clone());
    let new_key = row
        .new_pk_json
        .clone()
        .unwrap_or_else(|| row.pk_json.clone());

    match (old_matches, new_matches) {
        (false, true) => row
            .new_json
            .clone()
            .map(|value| {
                vec![ShapeMessage::Insert {
                    key: new_key,
                    value,
                    offset: row.seq,
                }]
            })
            .unwrap_or_default(),
        (true, true) => row
            .new_json
            .clone()
            .map(|value| {
                if old_key == new_key {
                    vec![ShapeMessage::Update {
                        key: new_key,
                        value,
                        offset: row.seq,
                    }]
                } else {
                    vec![
                        ShapeMessage::Delete {
                            key: old_key,
                            offset: row.seq,
                        },
                        ShapeMessage::Insert {
                            key: new_key,
                            value,
                            offset: row.seq,
                        },
                    ]
                }
            })
            .unwrap_or_default(),
        (true, false) => vec![ShapeMessage::Delete {
            key: old_key,
            offset: row.seq,
        }],
        (false, false) => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn shape() -> Shape {
        Shape {
            name: "activeUsers".to_string(),
            table: "users".to_string(),
            columns: vec!["id".to_string(), "name".to_string(), "active".to_string()],
            predicate: Predicate::Eq {
                column: "active".to_string(),
                value: json!(1),
            },
            auth_scope: "public".to_string(),
            schema_version: 1,
        }
    }

    #[test]
    fn handles_are_stable() {
        assert_eq!(shape().handle(), shape().handle());
    }

    #[test]
    fn handles_ignore_name_and_canonicalize_columns_and_predicates() {
        let shape_a = Shape {
            name: "projectTodos/p1-p2".to_string(),
            table: "todos".to_string(),
            columns: vec![
                "title".to_string(),
                "id".to_string(),
                "project_id".to_string(),
                "id".to_string(),
            ],
            predicate: Predicate::And {
                predicates: vec![
                    Predicate::Eq {
                        column: "done".to_string(),
                        value: json!(0),
                    },
                    Predicate::In {
                        column: "project_id".to_string(),
                        values: vec![json!("p2"), json!("p1"), json!("p1")],
                    },
                ],
            },
            auth_scope: "projects:p1,p2".to_string(),
            schema_version: 1,
        };
        let shape_b = Shape {
            name: "anotherName".to_string(),
            table: "todos".to_string(),
            columns: vec![
                "project_id".to_string(),
                "id".to_string(),
                "title".to_string(),
            ],
            predicate: Predicate::And {
                predicates: vec![
                    Predicate::In {
                        column: "project_id".to_string(),
                        values: vec![json!("p1"), json!("p2")],
                    },
                    Predicate::Eq {
                        column: "done".to_string(),
                        value: json!(0),
                    },
                ],
            },
            auth_scope: "projects:p1,p2".to_string(),
            schema_version: 1,
        };

        assert_eq!(shape_a.handle(), shape_b.handle());
    }

    #[test]
    fn registry_stores_shapes_by_name() {
        let shape = shape();
        let mut registry = ShapeRegistry::new();
        assert!(!registry.contains("activeUsers"));

        registry.add(shape.clone());

        assert!(registry.contains("activeUsers"));
        assert_eq!(registry.get("activeUsers"), Some(&shape));
    }

    #[test]
    fn membership_transition_emits_insert_update_delete() {
        let shape = shape();

        let inserted = LogRow {
            seq: 1,
            batch_id: "batch".to_string(),
            table_name: "users".to_string(),
            op: LogOp::Insert,
            pk_json: json!({"id": 7}),
            old_pk_json: None,
            new_pk_json: Some(json!({"id": 7})),
            old_json: None,
            new_json: Some(json!({"id": 7, "name": "Ada", "active": 1})),
            created_at: 0,
        };
        assert!(matches!(
            message_for_log(&shape, &inserted),
            Some(ShapeMessage::Insert { .. })
        ));

        let updated = LogRow {
            seq: 2,
            batch_id: "batch".to_string(),
            table_name: "users".to_string(),
            op: LogOp::Update,
            pk_json: json!({"id": 7}),
            old_pk_json: Some(json!({"id": 7})),
            new_pk_json: Some(json!({"id": 7})),
            old_json: Some(json!({"id": 7, "name": "Ada", "active": 1})),
            new_json: Some(json!({"id": 7, "name": "Ada Lovelace", "active": 1})),
            created_at: 0,
        };
        assert!(matches!(
            message_for_log(&shape, &updated),
            Some(ShapeMessage::Update { .. })
        ));

        let removed = LogRow {
            seq: 3,
            batch_id: "batch".to_string(),
            table_name: "users".to_string(),
            op: LogOp::Update,
            pk_json: json!({"id": 7}),
            old_pk_json: Some(json!({"id": 7})),
            new_pk_json: Some(json!({"id": 7})),
            old_json: Some(json!({"id": 7, "name": "Ada", "active": 1})),
            new_json: Some(json!({"id": 7, "name": "Ada", "active": 0})),
            created_at: 0,
        };
        assert!(matches!(
            message_for_log(&shape, &removed),
            Some(ShapeMessage::Delete { .. })
        ));
    }

    #[test]
    fn primary_key_change_in_shape_expands_to_delete_insert() {
        let shape = shape();
        let row = LogRow {
            seq: 4,
            batch_id: "batch".to_string(),
            table_name: "users".to_string(),
            op: LogOp::Update,
            pk_json: json!({"id": 8}),
            old_pk_json: Some(json!({"id": 7})),
            new_pk_json: Some(json!({"id": 8})),
            old_json: Some(json!({"id": 7, "name": "Ada", "active": 1})),
            new_json: Some(json!({"id": 8, "name": "Ada", "active": 1})),
            created_at: 0,
        };

        assert_eq!(
            messages_for_log(&shape, &row),
            vec![
                ShapeMessage::Delete {
                    key: json!({"id": 7}),
                    offset: 4,
                },
                ShapeMessage::Insert {
                    key: json!({"id": 8}),
                    value: json!({"id": 8, "name": "Ada", "active": 1}),
                    offset: 4,
                },
            ]
        );
    }

    #[test]
    fn predicate_eq_terms_collect_nested_and_terms() {
        let predicate = Predicate::And {
            predicates: vec![
                Predicate::Eq {
                    column: "project_id".to_string(),
                    value: json!("p1"),
                },
                Predicate::Eq {
                    column: "done".to_string(),
                    value: json!(0),
                },
            ],
        };

        assert_eq!(
            predicate.eq_terms(),
            vec![
                PredicateEqTerm {
                    column: "project_id".to_string(),
                    value: json!("p1"),
                },
                PredicateEqTerm {
                    column: "done".to_string(),
                    value: json!(0),
                },
            ]
        );
    }

    #[test]
    fn in_predicate_matches_any_value_and_indexes_each_term() {
        let predicate = Predicate::In {
            column: "project_id".to_string(),
            values: vec![json!("p1"), json!("p2")],
        };
        assert!(predicate.matches(Some(&json!({"project_id": "p1"}))));
        assert!(predicate.matches(Some(&json!({"project_id": "p2"}))));
        assert!(!predicate.matches(Some(&json!({"project_id": "p3"}))));
        assert_eq!(
            predicate.eq_terms(),
            vec![
                PredicateEqTerm {
                    column: "project_id".to_string(),
                    value: json!("p1"),
                },
                PredicateEqTerm {
                    column: "project_id".to_string(),
                    value: json!("p2"),
                },
            ]
        );
    }

    #[test]
    fn shape_index_finds_equality_candidates_from_old_and_new_rows() {
        let mut index = ShapeIndex::new();
        index.add(shape());

        let inactive_insert = LogRow {
            seq: 1,
            batch_id: "batch".to_string(),
            table_name: "users".to_string(),
            op: LogOp::Insert,
            pk_json: json!({"id": 1}),
            old_pk_json: None,
            new_pk_json: Some(json!({"id": 1})),
            old_json: None,
            new_json: Some(json!({"id": 1, "name": "Ada", "active": 0})),
            created_at: 0,
        };
        assert!(index.candidates_for_log(&inactive_insert).is_empty());

        let active_insert = LogRow {
            new_json: Some(json!({"id": 1, "name": "Ada", "active": 1})),
            ..inactive_insert.clone()
        };
        assert_eq!(
            index
                .candidates_for_log(&active_insert)
                .into_iter()
                .map(|shape| shape.name.as_str())
                .collect::<Vec<_>>(),
            vec!["activeUsers"]
        );

        let leaving_shape = LogRow {
            seq: 2,
            batch_id: "batch".to_string(),
            table_name: "users".to_string(),
            op: LogOp::Update,
            pk_json: json!({"id": 1}),
            old_pk_json: Some(json!({"id": 1})),
            new_pk_json: Some(json!({"id": 1})),
            old_json: Some(json!({"id": 1, "name": "Ada", "active": 1})),
            new_json: Some(json!({"id": 1, "name": "Ada", "active": 0})),
            created_at: 0,
        };
        assert_eq!(
            index
                .candidates_for_log(&leaving_shape)
                .into_iter()
                .map(|shape| shape.name.as_str())
                .collect::<Vec<_>>(),
            vec!["activeUsers"]
        );
    }

    #[test]
    fn shape_index_includes_table_scan_shapes_and_filters_other_tables() {
        let mut all_users = shape();
        all_users.name = "allUsers".to_string();
        all_users.predicate = Predicate::All;

        let mut index = ShapeIndex::new();
        index.add(all_users);

        let users_row = LogRow {
            seq: 1,
            batch_id: "batch".to_string(),
            table_name: "users".to_string(),
            op: LogOp::Insert,
            pk_json: json!({"id": 1}),
            old_pk_json: None,
            new_pk_json: Some(json!({"id": 1})),
            old_json: None,
            new_json: Some(json!({"id": 1, "name": "Ada", "active": 0})),
            created_at: 0,
        };
        assert_eq!(
            index
                .candidates_for_log(&users_row)
                .into_iter()
                .map(|shape| shape.name.as_str())
                .collect::<Vec<_>>(),
            vec!["allUsers"]
        );

        let posts_row = LogRow {
            table_name: "posts".to_string(),
            ..users_row
        };
        assert!(index.candidates_for_log(&posts_row).is_empty());
    }

    #[test]
    fn shape_index_uses_any_equality_term_as_a_candidate_gate() {
        let mut project_shape = shape();
        project_shape.name = "openProjectTodos".to_string();
        project_shape.table = "todos".to_string();
        project_shape.predicate = Predicate::And {
            predicates: vec![
                Predicate::Eq {
                    column: "project_id".to_string(),
                    value: json!("p1"),
                },
                Predicate::Eq {
                    column: "done".to_string(),
                    value: json!(0),
                },
            ],
        };

        let mut index = ShapeIndex::new();
        index.add(project_shape);

        let row = LogRow {
            seq: 1,
            batch_id: "batch".to_string(),
            table_name: "todos".to_string(),
            op: LogOp::Update,
            pk_json: json!({"id": 1}),
            old_pk_json: Some(json!({"id": 1})),
            new_pk_json: Some(json!({"id": 1})),
            old_json: Some(json!({"id": 1, "project_id": "p1", "done": 1})),
            new_json: Some(json!({"id": 1, "project_id": "p1", "done": 0})),
            created_at: 0,
        };

        assert_eq!(
            index
                .candidates_for_log(&row)
                .into_iter()
                .map(|shape| shape.name.as_str())
                .collect::<Vec<_>>(),
            vec!["openProjectTodos"]
        );
    }

    #[test]
    fn shape_index_replaces_shapes_by_name_without_stale_candidates() {
        let mut index = ShapeIndex::new();
        let mut indexed_shape = shape();
        indexed_shape.name = "usersByStatus".to_string();
        index.add(indexed_shape.clone());

        indexed_shape.predicate = Predicate::Eq {
            column: "active".to_string(),
            value: json!(0),
        };
        assert_eq!(
            index.add(indexed_shape),
            Some(shape_with_name("usersByStatus"))
        );
        assert_eq!(index.len(), 1);

        let active_row = LogRow {
            seq: 1,
            batch_id: "batch".to_string(),
            table_name: "users".to_string(),
            op: LogOp::Insert,
            pk_json: json!({"id": 1}),
            old_pk_json: None,
            new_pk_json: Some(json!({"id": 1})),
            old_json: None,
            new_json: Some(json!({"id": 1, "name": "Ada", "active": 1})),
            created_at: 0,
        };
        assert!(index.candidates_for_log(&active_row).is_empty());

        let inactive_row = LogRow {
            new_json: Some(json!({"id": 1, "name": "Ada", "active": 0})),
            ..active_row
        };
        assert_eq!(
            index
                .candidates_for_log(&inactive_row)
                .into_iter()
                .map(|shape| shape.name.as_str())
                .collect::<Vec<_>>(),
            vec!["usersByStatus"]
        );
    }

    #[test]
    fn shape_index_allows_multiple_names_for_same_canonical_handle() {
        let mut index = ShapeIndex::new();
        let mut alias = shape();
        alias.name = "alias".to_string();

        index.add(shape());
        index.add(alias.clone());

        assert_eq!(index.len(), 1);
        let row = LogRow {
            seq: 1,
            batch_id: "batch".to_string(),
            table_name: "users".to_string(),
            op: LogOp::Insert,
            pk_json: json!({"id": 1}),
            old_pk_json: None,
            new_pk_json: Some(json!({"id": 1})),
            old_json: None,
            new_json: Some(json!({"id": 1, "name": "Ada", "active": 1})),
            created_at: 0,
        };
        let candidates = index.candidates_for_log(&row);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].handle(), alias.handle());

        let mut alias_private = alias;
        alias_private.auth_scope = "private".to_string();
        index.add(alias_private);
        assert_eq!(index.len(), 2);
        assert_eq!(index.candidates_for_log(&row).len(), 2);
    }

    fn shape_with_name(name: &str) -> Shape {
        let mut out = shape();
        out.name = name.to_string();
        out
    }
}
