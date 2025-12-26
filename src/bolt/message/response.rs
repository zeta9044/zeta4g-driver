//! Bolt protocol response messages.
//!
//! Response messages are sent from the server to the client.

use std::collections::HashMap;

use super::tag;
use crate::bolt::packstream::{
    PackStreamError, PackStreamStructure, PackStreamValue,
};

/// All Bolt response messages.
#[derive(Debug, Clone)]
pub enum BoltResponse {
    /// SUCCESS - Operation completed successfully
    Success(SuccessMessage),
    /// RECORD - Query result record
    Record(RecordMessage),
    /// FAILURE - Operation failed
    Failure(FailureMessage),
    /// IGNORED - Message was ignored (connection in FAILED state)
    Ignored,
}

impl BoltResponse {
    /// Get the message tag.
    pub fn tag(&self) -> u8 {
        match self {
            BoltResponse::Success(_) => tag::SUCCESS,
            BoltResponse::Record(_) => tag::RECORD,
            BoltResponse::Failure(_) => tag::FAILURE,
            BoltResponse::Ignored => tag::IGNORED,
        }
    }

    /// Get message name for logging.
    pub fn name(&self) -> &'static str {
        match self {
            BoltResponse::Success(_) => "SUCCESS",
            BoltResponse::Record(_) => "RECORD",
            BoltResponse::Failure(_) => "FAILURE",
            BoltResponse::Ignored => "IGNORED",
        }
    }

    /// Check if this is a success response.
    pub fn is_success(&self) -> bool {
        matches!(self, BoltResponse::Success(_))
    }

    /// Check if this is a failure response.
    pub fn is_failure(&self) -> bool {
        matches!(self, BoltResponse::Failure(_))
    }

    /// Check if this is a record response.
    pub fn is_record(&self) -> bool {
        matches!(self, BoltResponse::Record(_))
    }

    /// Convert to PackStream structure.
    pub fn to_structure(&self) -> PackStreamStructure {
        match self {
            BoltResponse::Success(msg) => msg.to_structure(),
            BoltResponse::Record(msg) => msg.to_structure(),
            BoltResponse::Failure(msg) => msg.to_structure(),
            BoltResponse::Ignored => PackStreamStructure::new(tag::IGNORED, vec![]),
        }
    }

    /// Parse from PackStream structure.
    pub fn from_structure(s: &PackStreamStructure) -> Result<Self, PackStreamError> {
        match s.tag {
            tag::SUCCESS => Ok(BoltResponse::Success(SuccessMessage::from_structure(s)?)),
            tag::RECORD => Ok(BoltResponse::Record(RecordMessage::from_structure(s)?)),
            tag::FAILURE => Ok(BoltResponse::Failure(FailureMessage::from_structure(s)?)),
            tag::IGNORED => Ok(BoltResponse::Ignored),
            _ => Err(PackStreamError::InvalidStructure(format!(
                "Unknown response message tag: 0x{:02X}",
                s.tag
            ))),
        }
    }
}

/// SUCCESS message - Operation completed successfully.
#[derive(Debug, Clone)]
pub struct SuccessMessage {
    /// Response metadata
    pub metadata: HashMap<String, PackStreamValue>,
}

impl SuccessMessage {
    /// Create a new SUCCESS message with empty metadata.
    pub fn new() -> Self {
        Self {
            metadata: HashMap::new(),
        }
    }

    /// Create a SUCCESS message with metadata.
    pub fn with_metadata(metadata: HashMap<String, PackStreamValue>) -> Self {
        Self { metadata }
    }

    /// Add metadata entry.
    pub fn add(&mut self, key: &str, value: PackStreamValue) {
        self.metadata.insert(key.to_string(), value);
    }

    /// Get metadata entry.
    pub fn get(&self, key: &str) -> Option<&PackStreamValue> {
        self.metadata.get(key)
    }

    /// Get server name.
    pub fn server(&self) -> Option<&str> {
        self.metadata.get("server").and_then(|v| v.as_str())
    }

    /// Get result available after.
    pub fn result_available_after(&self) -> Option<i64> {
        self.metadata.get("t_first").and_then(|v| v.as_int())
    }

    /// Get result consumed after.
    pub fn result_consumed_after(&self) -> Option<i64> {
        self.metadata.get("t_last").and_then(|v| v.as_int())
    }

    /// Get field names from RUN success.
    pub fn fields(&self) -> Option<Vec<String>> {
        self.metadata.get("fields").and_then(|v| {
            if let PackStreamValue::List(list) = v {
                Some(
                    list.iter()
                        .filter_map(|item| item.as_str().map(|s| s.to_string()))
                        .collect(),
                )
            } else {
                None
            }
        })
    }

    /// Get query statistics.
    pub fn stats(&self) -> Option<&HashMap<String, PackStreamValue>> {
        self.metadata.get("stats").and_then(|v| v.as_map())
    }

    /// Check if there are more results.
    pub fn has_more(&self) -> bool {
        self.metadata.get("has_more")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
    }

    /// Get bookmark.
    pub fn bookmark(&self) -> Option<&str> {
        self.metadata.get("bookmark").and_then(|v| v.as_str())
    }

    /// Get database name.
    pub fn db(&self) -> Option<&str> {
        self.metadata.get("db").and_then(|v| v.as_str())
    }

    /// Get query ID.
    pub fn qid(&self) -> Option<i64> {
        self.metadata.get("qid").and_then(|v| v.as_int())
    }

    /// Get connection ID.
    pub fn connection_id(&self) -> Option<&str> {
        self.metadata.get("connection_id").and_then(|v| v.as_str())
    }

    /// Create a HELLO success response.
    pub fn hello_success(server: &str, connection_id: &str) -> Self {
        let mut msg = Self::new();
        msg.add("server", PackStreamValue::String(server.to_string()));
        msg.add("connection_id", PackStreamValue::String(connection_id.to_string()));
        msg
    }

    /// Create a RUN success response.
    pub fn run_success(fields: Vec<String>, qid: Option<i64>) -> Self {
        let mut msg = Self::new();
        let field_list: Vec<PackStreamValue> = fields
            .into_iter()
            .map(PackStreamValue::String)
            .collect();
        msg.add("fields", PackStreamValue::List(field_list));
        if let Some(id) = qid {
            msg.add("qid", PackStreamValue::Integer(id));
        }
        msg
    }

    /// Create a PULL/DISCARD success response.
    pub fn streaming_success(has_more: bool, bookmark: Option<String>) -> Self {
        let mut msg = Self::new();
        if has_more {
            msg.add("has_more", PackStreamValue::Boolean(true));
        }
        if let Some(bm) = bookmark {
            msg.add("bookmark", PackStreamValue::String(bm));
        }
        msg
    }

    /// Create a BEGIN success response.
    pub fn begin_success() -> Self {
        Self::new()
    }

    /// Create a COMMIT success response.
    pub fn commit_success(bookmark: String) -> Self {
        let mut msg = Self::new();
        msg.add("bookmark", PackStreamValue::String(bookmark));
        msg
    }

    /// Create a ROLLBACK success response.
    pub fn rollback_success() -> Self {
        Self::new()
    }

    /// Convert to PackStream structure.
    pub fn to_structure(&self) -> PackStreamStructure {
        PackStreamStructure::new(
            tag::SUCCESS,
            vec![PackStreamValue::Map(self.metadata.clone())],
        )
    }

    /// Parse from PackStream structure.
    pub fn from_structure(s: &PackStreamStructure) -> Result<Self, PackStreamError> {
        if s.tag != tag::SUCCESS {
            return Err(PackStreamError::InvalidStructure("Expected SUCCESS tag".to_string()));
        }

        let metadata = if !s.fields.is_empty() {
            s.fields[0].as_map().cloned().unwrap_or_default()
        } else {
            HashMap::new()
        };

        Ok(Self { metadata })
    }
}

impl Default for SuccessMessage {
    fn default() -> Self {
        Self::new()
    }
}

/// RECORD message - Query result record.
#[derive(Debug, Clone)]
pub struct RecordMessage {
    /// Field values
    pub fields: Vec<PackStreamValue>,
}

impl RecordMessage {
    /// Create a new RECORD message.
    pub fn new(fields: Vec<PackStreamValue>) -> Self {
        Self { fields }
    }

    /// Create an empty record.
    pub fn empty() -> Self {
        Self { fields: Vec::new() }
    }

    /// Get field count.
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    /// Check if record is empty.
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// Get field at index.
    pub fn get(&self, index: usize) -> Option<&PackStreamValue> {
        self.fields.get(index)
    }

    /// Convert to PackStream structure.
    pub fn to_structure(&self) -> PackStreamStructure {
        PackStreamStructure::new(
            tag::RECORD,
            vec![PackStreamValue::List(self.fields.clone())],
        )
    }

    /// Parse from PackStream structure.
    pub fn from_structure(s: &PackStreamStructure) -> Result<Self, PackStreamError> {
        if s.tag != tag::RECORD {
            return Err(PackStreamError::InvalidStructure("Expected RECORD tag".to_string()));
        }

        let fields = if !s.fields.is_empty() {
            if let PackStreamValue::List(list) = &s.fields[0] {
                list.clone()
            } else {
                return Err(PackStreamError::InvalidStructure("RECORD fields must be list".to_string()));
            }
        } else {
            Vec::new()
        };

        Ok(Self { fields })
    }
}

/// FAILURE message - Operation failed.
#[derive(Debug, Clone)]
pub struct FailureMessage {
    /// Neo4j error code
    pub code: String,
    /// Error message
    pub message: String,
}

impl FailureMessage {
    /// Create a new FAILURE message.
    pub fn new(code: &str, message: &str) -> Self {
        Self {
            code: code.to_string(),
            message: message.to_string(),
        }
    }

    /// Create a syntax error.
    pub fn syntax_error(message: &str) -> Self {
        Self::new("Neo.ClientError.Statement.SyntaxError", message)
    }

    /// Create a semantic error.
    pub fn semantic_error(message: &str) -> Self {
        Self::new("Neo.ClientError.Statement.SemanticError", message)
    }

    /// Create an authentication error.
    pub fn auth_error(message: &str) -> Self {
        Self::new("Neo.ClientError.Security.Unauthorized", message)
    }

    /// Create a database not found error.
    pub fn database_not_found(db_name: &str) -> Self {
        Self::new(
            "Neo.ClientError.Database.DatabaseNotFound",
            &format!("Database '{}' not found", db_name),
        )
    }

    /// Create a constraint violation error.
    pub fn constraint_violation(message: &str) -> Self {
        Self::new("Neo.ClientError.Schema.ConstraintValidationFailed", message)
    }

    /// Create a transaction error.
    pub fn transaction_error(message: &str) -> Self {
        Self::new("Neo.TransientError.Transaction.TransactionTimedOut", message)
    }

    /// Create a general error.
    pub fn general_error(message: &str) -> Self {
        Self::new("Neo.ClientError.General.UnknownError", message)
    }

    /// Create an internal error.
    pub fn internal_error(message: &str) -> Self {
        Self::new("Neo.DatabaseError.General.UnknownError", message)
    }

    /// Get error category from code.
    pub fn category(&self) -> &str {
        self.code.split('.').nth(1).unwrap_or("Unknown")
    }

    /// Get error classification from code.
    pub fn classification(&self) -> &str {
        self.code.split('.').next().unwrap_or("Neo")
    }

    /// Check if this is a client error.
    pub fn is_client_error(&self) -> bool {
        self.code.contains("ClientError")
    }

    /// Check if this is a transient error.
    pub fn is_transient(&self) -> bool {
        self.code.contains("TransientError")
    }

    /// Check if this is a database error.
    pub fn is_database_error(&self) -> bool {
        self.code.contains("DatabaseError")
    }

    /// Convert to PackStream structure.
    pub fn to_structure(&self) -> PackStreamStructure {
        let mut metadata = HashMap::new();
        metadata.insert("code".to_string(), PackStreamValue::String(self.code.clone()));
        metadata.insert("message".to_string(), PackStreamValue::String(self.message.clone()));

        PackStreamStructure::new(tag::FAILURE, vec![PackStreamValue::Map(metadata)])
    }

    /// Parse from PackStream structure.
    pub fn from_structure(s: &PackStreamStructure) -> Result<Self, PackStreamError> {
        if s.tag != tag::FAILURE {
            return Err(PackStreamError::InvalidStructure("Expected FAILURE tag".to_string()));
        }

        let metadata = s.fields.get(0)
            .and_then(|v| v.as_map())
            .ok_or_else(|| PackStreamError::InvalidStructure("FAILURE requires metadata map".to_string()))?;

        let code = metadata.get("code")
            .and_then(|v| v.as_str())
            .ok_or_else(|| PackStreamError::InvalidStructure("FAILURE requires code".to_string()))?
            .to_string();

        let message = metadata.get("message")
            .and_then(|v| v.as_str())
            .ok_or_else(|| PackStreamError::InvalidStructure("FAILURE requires message".to_string()))?
            .to_string();

        Ok(Self { code, message })
    }
}

impl std::fmt::Display for FailureMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_success_message() {
        let mut msg = SuccessMessage::new();
        msg.add("server", PackStreamValue::String("Neo4j/5.0".to_string()));
        msg.add("connection_id", PackStreamValue::String("bolt-123".to_string()));

        let structure = msg.to_structure();
        assert_eq!(structure.tag, tag::SUCCESS);

        let parsed = SuccessMessage::from_structure(&structure).unwrap();
        assert_eq!(parsed.server(), Some("Neo4j/5.0"));
        assert_eq!(parsed.connection_id(), Some("bolt-123"));
    }

    #[test]
    fn test_success_hello() {
        let msg = SuccessMessage::hello_success("Zeta4G/1.0", "conn-001");
        assert_eq!(msg.server(), Some("Zeta4G/1.0"));
        assert_eq!(msg.connection_id(), Some("conn-001"));
    }

    #[test]
    fn test_success_run() {
        let msg = SuccessMessage::run_success(
            vec!["name".to_string(), "age".to_string()],
            Some(1),
        );
        let fields = msg.fields().unwrap();
        assert_eq!(fields, vec!["name", "age"]);
        assert_eq!(msg.qid(), Some(1));
    }

    #[test]
    fn test_success_streaming() {
        let msg = SuccessMessage::streaming_success(true, Some("bm:1234".to_string()));
        assert!(msg.has_more());
        assert_eq!(msg.bookmark(), Some("bm:1234"));
    }

    #[test]
    fn test_record_message() {
        let fields = vec![
            PackStreamValue::String("Alice".to_string()),
            PackStreamValue::Integer(30),
        ];
        let msg = RecordMessage::new(fields);

        assert_eq!(msg.len(), 2);
        assert!(!msg.is_empty());

        let structure = msg.to_structure();
        assert_eq!(structure.tag, tag::RECORD);

        let parsed = RecordMessage::from_structure(&structure).unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed.get(0).unwrap().as_str().unwrap(), "Alice");
        assert_eq!(parsed.get(1).unwrap().as_int().unwrap(), 30);
    }

    #[test]
    fn test_failure_message() {
        let msg = FailureMessage::syntax_error("Invalid query");
        assert_eq!(msg.code, "Neo.ClientError.Statement.SyntaxError");
        assert_eq!(msg.message, "Invalid query");
        assert!(msg.is_client_error());
        assert!(!msg.is_transient());

        let structure = msg.to_structure();
        assert_eq!(structure.tag, tag::FAILURE);

        let parsed = FailureMessage::from_structure(&structure).unwrap();
        assert_eq!(parsed.code, msg.code);
        assert_eq!(parsed.message, msg.message);
    }

    #[test]
    fn test_failure_types() {
        assert!(FailureMessage::auth_error("").is_client_error());
        assert!(FailureMessage::transaction_error("").is_transient());
        assert!(FailureMessage::internal_error("").is_database_error());
    }

    #[test]
    fn test_failure_display() {
        let msg = FailureMessage::syntax_error("Unexpected token");
        let display = format!("{}", msg);
        assert!(display.contains("SyntaxError"));
        assert!(display.contains("Unexpected token"));
    }

    #[test]
    fn test_bolt_response_helpers() {
        let success = BoltResponse::Success(SuccessMessage::new());
        assert!(success.is_success());
        assert!(!success.is_failure());

        let failure = BoltResponse::Failure(FailureMessage::general_error("test"));
        assert!(failure.is_failure());
        assert!(!failure.is_success());

        let record = BoltResponse::Record(RecordMessage::empty());
        assert!(record.is_record());

        let ignored = BoltResponse::Ignored;
        assert!(!ignored.is_success());
    }

    #[test]
    fn test_bolt_response_tags() {
        assert_eq!(BoltResponse::Success(SuccessMessage::new()).tag(), tag::SUCCESS);
        assert_eq!(BoltResponse::Record(RecordMessage::empty()).tag(), tag::RECORD);
        assert_eq!(BoltResponse::Failure(FailureMessage::general_error("")).tag(), tag::FAILURE);
        assert_eq!(BoltResponse::Ignored.tag(), tag::IGNORED);
    }

    #[test]
    fn test_bolt_response_names() {
        assert_eq!(BoltResponse::Success(SuccessMessage::new()).name(), "SUCCESS");
        assert_eq!(BoltResponse::Record(RecordMessage::empty()).name(), "RECORD");
        assert_eq!(BoltResponse::Failure(FailureMessage::general_error("")).name(), "FAILURE");
        assert_eq!(BoltResponse::Ignored.name(), "IGNORED");
    }

    #[test]
    fn test_success_commit() {
        let msg = SuccessMessage::commit_success("bm:commit:1234".to_string());
        assert_eq!(msg.bookmark(), Some("bm:commit:1234"));
    }

    #[test]
    fn test_failure_category() {
        let msg = FailureMessage::syntax_error("");
        assert_eq!(msg.category(), "ClientError");
        assert_eq!(msg.classification(), "Neo");
    }

    #[test]
    fn test_empty_record() {
        let record = RecordMessage::empty();
        assert!(record.is_empty());
        assert_eq!(record.len(), 0);
        assert!(record.get(0).is_none());
    }
}
