use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::HashMap;

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
        let bytes = serde_json::to_vec(self).expect("shape handle serialization is infallible");
        let digest = Sha256::digest(bytes);
        format!("{digest:x}")
    }
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
    And { predicates: Vec<Predicate> },
}

impl Predicate {
    pub fn matches(&self, row: Option<&Value>) -> bool {
        let Some(Value::Object(row)) = row else {
            return false;
        };
        self.matches_object(row)
    }

    fn matches_object(&self, row: &serde_json::Map<String, Value>) -> bool {
        match self {
            Predicate::All => true,
            Predicate::Eq { column, value } => row.get(column) == Some(value),
            Predicate::And { predicates } => predicates.iter().all(|p| p.matches_object(row)),
        }
    }
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
    pub rows: Vec<Value>,
    pub offset: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Replay {
    pub messages: Vec<ShapeMessage>,
    pub offset: i64,
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
}
