//! Custom assertions for CDC event testing
//!
//! Provides fluent, readable assertions for CDC events with detailed error messages.

use pretty_assertions::assert_eq;
use rivven_cdc::{CdcEvent, CdcOp};
use serde_json::Value;

/// Fluent assertion builder for CDC events
pub struct CdcEventAssertions<'a> {
    events: &'a [CdcEvent],
}

impl<'a> CdcEventAssertions<'a> {
    pub fn new(events: &'a [CdcEvent]) -> Self {
        Self { events }
    }

    /// Assert the number of events
    pub fn has_count(self, expected: usize) -> Self {
        assert_eq!(
            self.events.len(),
            expected,
            "Expected {} events, got {}. Events: {:#?}",
            expected,
            self.events.len(),
            self.events
        );
        self
    }

    /// Assert at least N events
    pub fn has_at_least(self, min: usize) -> Self {
        assert!(
            self.events.len() >= min,
            "Expected at least {} events, got {}",
            min,
            self.events.len()
        );
        self
    }

    /// Get event at index for further assertions
    pub fn event_at(&self, index: usize) -> CdcEventAssertion<'_> {
        assert!(
            index < self.events.len(),
            "Event index {} out of bounds (len: {})",
            index,
            self.events.len()
        );
        CdcEventAssertion::new(&self.events[index])
    }

    /// Filter events by operation
    pub fn with_op(&self, op: CdcOp) -> Vec<&CdcEvent> {
        self.events
            .iter()
            .filter(|e| std::mem::discriminant(&e.op) == std::mem::discriminant(&op))
            .collect()
    }

    /// Assert count of specific operation type
    pub fn has_op_count(self, op: CdcOp, expected: usize) -> Self {
        let count = self.with_op(op.clone()).len();
        assert_eq!(
            count, expected,
            "Expected {} {:?} events, got {}",
            expected, op, count
        );
        self
    }

    /// Get all events for a specific table
    pub fn for_table(&self, table: &str) -> Vec<&CdcEvent> {
        self.events.iter().filter(|e| e.table == table).collect()
    }

    /// Assert events are in timestamp order
    pub fn is_ordered_by_timestamp(self) -> Self {
        for window in self.events.windows(2) {
            assert!(
                window[0].timestamp <= window[1].timestamp,
                "Events not in timestamp order: {} > {}",
                window[0].timestamp,
                window[1].timestamp
            );
        }
        self
    }

    /// Get the underlying events
    pub fn get_events(&self) -> &[CdcEvent] {
        self.events
    }
}

/// Fluent assertion for a single CDC event
pub struct CdcEventAssertion<'a> {
    event: &'a CdcEvent,
}

impl<'a> CdcEventAssertion<'a> {
    pub fn new(event: &'a CdcEvent) -> Self {
        Self { event }
    }

    /// Assert the operation type
    pub fn has_op(self, expected: CdcOp) -> Self {
        assert_eq!(
            std::mem::discriminant(&self.event.op),
            std::mem::discriminant(&expected),
            "Expected operation {:?}, got {:?}",
            expected,
            self.event.op
        );
        self
    }

    /// Assert the table name
    pub fn has_table(self, expected: &str) -> Self {
        assert_eq!(
            self.event.table, expected,
            "Expected table {}, got {}",
            expected, self.event.table
        );
        self
    }

    /// Assert the schema name
    pub fn has_schema(self, expected: &str) -> Self {
        assert_eq!(
            self.event.schema, expected,
            "Expected schema {}, got {}",
            expected, self.event.schema
        );
        self
    }

    /// Assert a field value in `after`
    pub fn after_has_field(self, field: &str, expected: &Value) -> Self {
        let after = self
            .event
            .after
            .as_ref()
            .expect("Event has no 'after' data");

        let actual = after
            .get(field)
            .unwrap_or_else(|| panic!("Field '{}' not found in after data", field));

        assert_eq!(
            actual, expected,
            "Field '{}': expected {:?}, got {:?}",
            field, expected, actual
        );
        self
    }

    /// Assert a field exists in `after`
    pub fn after_contains_field(self, field: &str) -> Self {
        let after = self
            .event
            .after
            .as_ref()
            .expect("Event has no 'after' data");

        assert!(
            after.get(field).is_some(),
            "Field '{}' not found in after data",
            field
        );
        self
    }

    /// Assert `before` is present (for updates/deletes)
    pub fn has_before(self) -> Self {
        assert!(
            self.event.before.is_some(),
            "Expected 'before' data but it was None"
        );
        self
    }

    /// Assert `after` is present
    pub fn has_after(self) -> Self {
        assert!(
            self.event.after.is_some(),
            "Expected 'after' data but it was None"
        );
        self
    }

    /// Assert `after` is None (for deletes)
    pub fn has_no_after(self) -> Self {
        assert!(
            self.event.after.is_none(),
            "Expected no 'after' data but got {:?}",
            self.event.after
        );
        self
    }

    /// Get the underlying event
    pub fn get_event(&self) -> &CdcEvent {
        self.event
    }
}

/// Extension trait for Vec<CdcEvent>
pub trait CdcEventVecExt {
    fn assert(&self) -> CdcEventAssertions<'_>;
}

impl CdcEventVecExt for Vec<CdcEvent> {
    fn assert(&self) -> CdcEventAssertions<'_> {
        CdcEventAssertions::new(self)
    }
}

impl CdcEventVecExt for [CdcEvent] {
    fn assert(&self) -> CdcEventAssertions<'_> {
        CdcEventAssertions::new(self)
    }
}

/// Helper to assert JSON field equality
pub fn assert_json_field(event: &CdcEvent, field: &str, expected: impl Into<Value>) {
    let expected = expected.into();
    let after = event.after.as_ref().expect("Event has no 'after' data");
    let actual = after
        .get(field)
        .expect(&format!("Field '{}' not found", field));
    assert_eq!(*actual, expected, "Field '{}' mismatch", field);
}

/// Helper to extract field value from event
pub fn get_event_field(event: &CdcEvent, field: &str) -> Option<Value> {
    event.after.as_ref()?.get(field).cloned()
}

/// Helper to extract string field from event
pub fn get_string_field(event: &CdcEvent, field: &str) -> Option<String> {
    get_event_field(event, field)?
        .as_str()
        .map(|s| s.to_string())
}

/// Helper to extract integer field from event
pub fn get_int_field(event: &CdcEvent, field: &str) -> Option<i64> {
    // Try both string and number representations
    let value = get_event_field(event, field)?;
    value
        .as_i64()
        .or_else(|| value.as_str().and_then(|s| s.parse().ok()))
}
