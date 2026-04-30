// LumeDB Query Engine
// JSON-based query DSL supporting comparison, logical, and array operators

use crate::document::Document;
use crate::error::{LumeError, LumeResult};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A query filter that can be evaluated against documents
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueryFilter {
    /// Match all documents
    All,
    /// Equality: { field: value }
    Eq { field: String, value: Value },
    /// Not equal: { field: { $ne: value } }
    Ne { field: String, value: Value },
    /// Greater than: { field: { $gt: value } }
    Gt { field: String, value: Value },
    /// Greater than or equal: { field: { $gte: value } }
    Gte { field: String, value: Value },
    /// Less than: { field: { $lt: value } }
    Lt { field: String, value: Value },
    /// Less than or equal: { field: { $lte: value } }
    Lte { field: String, value: Value },
    /// In array: { field: { $in: [values] } }
    In { field: String, values: Vec<Value> },
    /// Not in array: { field: { $nin: [values] } }
    Nin { field: String, values: Vec<Value> },
    /// Exists: { field: { $exists: true/false } }
    Exists { field: String, exists: bool },
    /// Regex match: { field: { $regex: "pattern" } }
    Regex { field: String, pattern: String },
    /// Logical AND: { $and: [filters] }
    And(Vec<QueryFilter>),
    /// Logical OR: { $or: [filters] }
    Or(Vec<QueryFilter>),
    /// Logical NOT: { $not: filter }
    Not(Box<QueryFilter>),
}

impl QueryFilter {
    /// Parse a JSON query into a QueryFilter
    pub fn from_json(query: &Value) -> LumeResult<Self> {
        match query {
            Value::Object(map) if map.is_empty() => Ok(QueryFilter::All),
            Value::Object(map) => {
                let mut filters = Vec::new();

                for (key, value) in map {
                    match key.as_str() {
                        "$and" => {
                            if let Value::Array(arr) = value {
                                let sub_filters: LumeResult<Vec<QueryFilter>> =
                                    arr.iter().map(QueryFilter::from_json).collect();
                                filters.push(QueryFilter::And(sub_filters?));
                            } else {
                                return Err(LumeError::InvalidQuery(
                                    "$and must be an array".to_string(),
                                ));
                            }
                        }
                        "$or" => {
                            if let Value::Array(arr) = value {
                                let sub_filters: LumeResult<Vec<QueryFilter>> =
                                    arr.iter().map(QueryFilter::from_json).collect();
                                filters.push(QueryFilter::Or(sub_filters?));
                            } else {
                                return Err(LumeError::InvalidQuery(
                                    "$or must be an array".to_string(),
                                ));
                            }
                        }
                        "$not" => {
                            let sub_filter = QueryFilter::from_json(value)?;
                            filters.push(QueryFilter::Not(Box::new(sub_filter)));
                        }
                        field => {
                            let field_filters = Self::parse_field_filter(field, value)?;
                            filters.extend(field_filters);
                        }
                    }
                }

                if filters.len() == 1 {
                    Ok(filters.into_iter().next().unwrap())
                } else {
                    Ok(QueryFilter::And(filters))
                }
            }
            _ => Err(LumeError::InvalidQuery(
                "Query must be a JSON object".to_string(),
            )),
        }
    }

    /// Parse a single field's filter conditions
    fn parse_field_filter(field: &str, value: &Value) -> LumeResult<Vec<QueryFilter>> {
        match value {
            Value::Object(ops) => {
                let mut filters = Vec::new();
                for (op, val) in ops {
                    match op.as_str() {
                        "$eq" => filters.push(QueryFilter::Eq {
                            field: field.to_string(),
                            value: val.clone(),
                        }),
                        "$ne" => filters.push(QueryFilter::Ne {
                            field: field.to_string(),
                            value: val.clone(),
                        }),
                        "$gt" => filters.push(QueryFilter::Gt {
                            field: field.to_string(),
                            value: val.clone(),
                        }),
                        "$gte" => filters.push(QueryFilter::Gte {
                            field: field.to_string(),
                            value: val.clone(),
                        }),
                        "$lt" => filters.push(QueryFilter::Lt {
                            field: field.to_string(),
                            value: val.clone(),
                        }),
                        "$lte" => filters.push(QueryFilter::Lte {
                            field: field.to_string(),
                            value: val.clone(),
                        }),
                        "$in" => {
                            if let Value::Array(arr) = val {
                                filters.push(QueryFilter::In {
                                    field: field.to_string(),
                                    values: arr.clone(),
                                });
                            } else {
                                return Err(LumeError::InvalidQuery(
                                    "$in must be an array".to_string(),
                                ));
                            }
                        }
                        "$nin" => {
                            if let Value::Array(arr) = val {
                                filters.push(QueryFilter::Nin {
                                    field: field.to_string(),
                                    values: arr.clone(),
                                });
                            } else {
                                return Err(LumeError::InvalidQuery(
                                    "$nin must be an array".to_string(),
                                ));
                            }
                        }
                        "$exists" => {
                            if let Value::Bool(b) = val {
                                filters.push(QueryFilter::Exists {
                                    field: field.to_string(),
                                    exists: *b,
                                });
                            }
                        }
                        "$regex" => {
                            if let Value::String(pattern) = val {
                                filters.push(QueryFilter::Regex {
                                    field: field.to_string(),
                                    pattern: pattern.clone(),
                                });
                            }
                        }
                        _ => {
                            return Err(LumeError::InvalidQuery(format!(
                                "Unknown operator: {}",
                                op
                            )));
                        }
                    }
                }
                Ok(filters)
            }
            // Direct value equality: { field: "value" }
            _ => Ok(vec![QueryFilter::Eq {
                field: field.to_string(),
                value: value.clone(),
            }]),
        }
    }

    /// Evaluate the filter against a document
    pub fn matches(&self, doc: &Document) -> bool {
        match self {
            QueryFilter::All => true,

            QueryFilter::Eq { field, value } => {
                doc.get_field_value(field)
                    .map(|v| values_equal(&v, value))
                    .unwrap_or(false)
            }

            QueryFilter::Ne { field, value } => {
                doc.get_field_value(field)
                    .map(|v| !values_equal(&v, value))
                    .unwrap_or(true)
            }

            QueryFilter::Gt { field, value } => {
                doc.get_field_value(field)
                    .map(|v| compare_values(&v, value) == Some(std::cmp::Ordering::Greater))
                    .unwrap_or(false)
            }

            QueryFilter::Gte { field, value } => {
                doc.get_field_value(field)
                    .map(|v| {
                        matches!(
                            compare_values(&v, value),
                            Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                        )
                    })
                    .unwrap_or(false)
            }

            QueryFilter::Lt { field, value } => {
                doc.get_field_value(field)
                    .map(|v| compare_values(&v, value) == Some(std::cmp::Ordering::Less))
                    .unwrap_or(false)
            }

            QueryFilter::Lte { field, value } => {
                doc.get_field_value(field)
                    .map(|v| {
                        matches!(
                            compare_values(&v, value),
                            Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                        )
                    })
                    .unwrap_or(false)
            }

            QueryFilter::In { field, values } => {
                doc.get_field_value(field)
                    .map(|v| values.iter().any(|val| values_equal(&v, val)))
                    .unwrap_or(false)
            }

            QueryFilter::Nin { field, values } => {
                doc.get_field_value(field)
                    .map(|v| !values.iter().any(|val| values_equal(&v, val)))
                    .unwrap_or(true)
            }

            QueryFilter::Exists { field, exists } => {
                let has_field = doc.get_field_value(field).is_some();
                has_field == *exists
            }

            QueryFilter::Regex { field, pattern } => {
                doc.get_field_value(field)
                    .and_then(|v| v.as_str().map(|s| s.to_string()))
                    .map(|s| s.contains(pattern.as_str()))
                    .unwrap_or(false)
            }

            QueryFilter::And(filters) => filters.iter().all(|f| f.matches(doc)),
            QueryFilter::Or(filters) => filters.iter().any(|f| f.matches(doc)),
            QueryFilter::Not(filter) => !filter.matches(doc),
        }
    }
}

/// Compare two JSON values for equality
fn values_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(a), Value::Number(b)) => {
            a.as_f64().unwrap_or(0.0) == b.as_f64().unwrap_or(0.0)
        }
        _ => a == b,
    }
}

/// Compare two JSON values for ordering
fn compare_values(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (Value::Number(a), Value::Number(b)) => {
            let a = a.as_f64()?;
            let b = b.as_f64()?;
            a.partial_cmp(&b)
        }
        (Value::String(a), Value::String(b)) => Some(a.cmp(b)),
        (Value::Bool(a), Value::Bool(b)) => Some(a.cmp(b)),
        _ => None,
    }
}

/// Query options for find operations
#[derive(Debug, Clone, Default)]
pub struct QueryOptions {
    /// Fields to include (projection)
    pub projection: Option<Vec<String>>,
    /// Sort order: (field, ascending)
    pub sort: Option<Vec<(String, bool)>>,
    /// Skip N results
    pub skip: Option<usize>,
    /// Limit results to N
    pub limit: Option<usize>,
}

/// Apply query options to a list of documents
pub fn apply_options(mut docs: Vec<Document>, options: &QueryOptions) -> Vec<Document> {
    // Sort
    if let Some(ref sort_fields) = options.sort {
        docs.sort_by(|a, b| {
            for (field, ascending) in sort_fields {
                let val_a = a.get_field_value(field);
                let val_b = b.get_field_value(field);

                let ordering = match (val_a, val_b) {
                    (Some(a), Some(b)) => compare_values(&a, &b).unwrap_or(std::cmp::Ordering::Equal),
                    (Some(_), None) => std::cmp::Ordering::Less,
                    (None, Some(_)) => std::cmp::Ordering::Greater,
                    (None, None) => std::cmp::Ordering::Equal,
                };

                let ordering = if *ascending {
                    ordering
                } else {
                    ordering.reverse()
                };

                if ordering != std::cmp::Ordering::Equal {
                    return ordering;
                }
            }
            std::cmp::Ordering::Equal
        });
    }

    // Skip
    if let Some(skip) = options.skip {
        docs = docs.into_iter().skip(skip).collect();
    }

    // Limit
    if let Some(limit) = options.limit {
        docs = docs.into_iter().take(limit).collect();
    }

    docs
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_doc(data: Value) -> Document {
        Document::new(data)
    }

    #[test]
    fn test_eq_filter() {
        let filter = QueryFilter::from_json(&json!({"name": "Alice"})).unwrap();
        let doc = make_doc(json!({"name": "Alice", "age": 30}));
        assert!(filter.matches(&doc));

        let doc2 = make_doc(json!({"name": "Bob", "age": 25}));
        assert!(!filter.matches(&doc2));
    }

    #[test]
    fn test_comparison_operators() {
        let filter = QueryFilter::from_json(&json!({
            "age": { "$gte": 18, "$lt": 65 }
        }))
        .unwrap();

        let doc_valid = make_doc(json!({"age": 30}));
        let doc_young = make_doc(json!({"age": 10}));
        let doc_old = make_doc(json!({"age": 70}));

        assert!(filter.matches(&doc_valid));
        assert!(!filter.matches(&doc_young));
        assert!(!filter.matches(&doc_old));
    }

    #[test]
    fn test_in_operator() {
        let filter = QueryFilter::from_json(&json!({
            "city": { "$in": ["NYC", "LA", "SF"] }
        }))
        .unwrap();

        let doc_nyc = make_doc(json!({"city": "NYC"}));
        let doc_chi = make_doc(json!({"city": "Chicago"}));

        assert!(filter.matches(&doc_nyc));
        assert!(!filter.matches(&doc_chi));
    }

    #[test]
    fn test_and_or() {
        let filter = QueryFilter::from_json(&json!({
            "$or": [
                { "age": { "$lt": 18 } },
                { "age": { "$gte": 65 } }
            ]
        }))
        .unwrap();

        let doc_young = make_doc(json!({"age": 10}));
        let doc_mid = make_doc(json!({"age": 30}));
        let doc_old = make_doc(json!({"age": 70}));

        assert!(filter.matches(&doc_young));
        assert!(!filter.matches(&doc_mid));
        assert!(filter.matches(&doc_old));
    }

    #[test]
    fn test_sort_and_limit() {
        let docs = vec![
            make_doc(json!({"name": "Charlie", "age": 35})),
            make_doc(json!({"name": "Alice", "age": 25})),
            make_doc(json!({"name": "Bob", "age": 30})),
        ];

        let options = QueryOptions {
            sort: Some(vec![("age".to_string(), true)]),
            limit: Some(2),
            ..Default::default()
        };

        let result = apply_options(docs, &options);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].data.get("name").unwrap(), &json!("Alice"));
        assert_eq!(result[1].data.get("name").unwrap(), &json!("Bob"));
    }
}
