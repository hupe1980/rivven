//! # CDC Event Router
//!
//! Content-based routing for CDC events with dead letter handling.
//!
//! ## Features
//!
//! - Rule-based event routing
//! - Priority-based rule matching
//! - Dead letter queue for unroutable events
//! - Conditional routing (predicates)
//! - Multi-destination fan-out
//!
//! ## Example
//!
//! ```rust,ignore
//! use rivven_cdc::common::router::{EventRouter, RouteRule};
//!
//! let router = EventRouter::builder()
//!     .route(RouteRule::table_match("users", "user-events"))
//!     .route(RouteRule::table_match("orders", "order-events"))
//!     .default_destination("default-events")
//!     .dead_letter("dlq")
//!     .build();
//!
//! let destinations = router.route(&event);
//! ```

use crate::common::{CdcEvent, CdcOp};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Routing decision for an event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteDecision {
    /// Route to specific destination(s)
    Route(Vec<String>),
    /// Drop the event
    Drop,
    /// Send to dead letter queue
    DeadLetter(String),
}

impl RouteDecision {
    /// Create a route to a single destination.
    pub fn to(destination: impl Into<String>) -> Self {
        Self::Route(vec![destination.into()])
    }

    /// Create a route to multiple destinations.
    pub fn to_many(destinations: Vec<String>) -> Self {
        Self::Route(destinations)
    }

    /// Check if event should be dropped.
    pub fn is_drop(&self) -> bool {
        matches!(self, Self::Drop)
    }

    /// Check if event goes to DLQ.
    pub fn is_dead_letter(&self) -> bool {
        matches!(self, Self::DeadLetter(_))
    }

    /// Get destinations (empty if dropped or DLQ).
    pub fn destinations(&self) -> &[String] {
        match self {
            Self::Route(dests) => dests,
            _ => &[],
        }
    }
}

/// Condition for route matching.
#[derive(Clone)]
pub enum RouteCondition {
    /// Always matches
    Always,
    /// Match specific table
    Table(String),
    /// Match table pattern (glob)
    TablePattern(String),
    /// Match schema
    Schema(String),
    /// Match operation type
    Operation(CdcOp),
    /// Match operations in list
    Operations(Vec<CdcOp>),
    /// Custom predicate
    Predicate(Arc<dyn Fn(&CdcEvent) -> bool + Send + Sync>),
    /// All conditions must match
    All(Vec<RouteCondition>),
    /// Any condition must match
    Any(Vec<RouteCondition>),
    /// Negation
    Not(Box<RouteCondition>),
    /// Field equals value
    FieldEquals(String, serde_json::Value),
    /// Field exists
    FieldExists(String),
}

impl std::fmt::Debug for RouteCondition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Always => write!(f, "Always"),
            Self::Table(t) => write!(f, "Table({})", t),
            Self::TablePattern(p) => write!(f, "TablePattern({})", p),
            Self::Schema(s) => write!(f, "Schema({})", s),
            Self::Operation(op) => write!(f, "Operation({:?})", op),
            Self::Operations(ops) => write!(f, "Operations({:?})", ops),
            Self::Predicate(_) => write!(f, "Predicate(<fn>)"),
            Self::All(conds) => write!(f, "All({:?})", conds),
            Self::Any(conds) => write!(f, "Any({:?})", conds),
            Self::Not(cond) => write!(f, "Not({:?})", cond),
            Self::FieldEquals(field, val) => write!(f, "FieldEquals({}, {:?})", field, val),
            Self::FieldExists(field) => write!(f, "FieldExists({})", field),
        }
    }
}

impl RouteCondition {
    /// Check if condition matches an event.
    pub fn matches(&self, event: &CdcEvent) -> bool {
        match self {
            Self::Always => true,
            Self::Table(table) => event.table == *table,
            Self::TablePattern(pattern) => {
                if pattern == "*" {
                    return true;
                }
                if let Some(prefix) = pattern.strip_suffix('*') {
                    return event.table.starts_with(prefix);
                }
                if let Some(suffix) = pattern.strip_prefix('*') {
                    return event.table.ends_with(suffix);
                }
                event.table == *pattern
            }
            Self::Schema(schema) => event.schema == *schema,
            Self::Operation(op) => event.op == *op,
            Self::Operations(ops) => ops.contains(&event.op),
            Self::Predicate(f) => f(event),
            Self::All(conditions) => conditions.iter().all(|c| c.matches(event)),
            Self::Any(conditions) => conditions.iter().any(|c| c.matches(event)),
            Self::Not(condition) => !condition.matches(event),
            Self::FieldEquals(field, value) => event
                .after
                .as_ref()
                .or(event.before.as_ref())
                .and_then(|obj| obj.get(field))
                .map(|v| v == value)
                .unwrap_or(false),
            Self::FieldExists(field) => event
                .after
                .as_ref()
                .or(event.before.as_ref())
                .and_then(|obj| obj.get(field))
                .is_some(),
        }
    }

    /// Create table match condition.
    pub fn table(table: impl Into<String>) -> Self {
        Self::Table(table.into())
    }

    /// Create table pattern condition.
    pub fn table_pattern(pattern: impl Into<String>) -> Self {
        Self::TablePattern(pattern.into())
    }

    /// Create schema match condition.
    pub fn schema(schema: impl Into<String>) -> Self {
        Self::Schema(schema.into())
    }

    /// Create operation match condition.
    pub fn operation(op: CdcOp) -> Self {
        Self::Operation(op)
    }

    /// Create multi-operation match condition.
    pub fn operations(ops: Vec<CdcOp>) -> Self {
        Self::Operations(ops)
    }

    /// Create field equals condition.
    pub fn field_equals(field: impl Into<String>, value: serde_json::Value) -> Self {
        Self::FieldEquals(field.into(), value)
    }

    /// Create field exists condition.
    pub fn field_exists(field: impl Into<String>) -> Self {
        Self::FieldExists(field.into())
    }

    /// Combine with AND.
    pub fn and(self, other: RouteCondition) -> Self {
        match self {
            Self::All(mut conds) => {
                conds.push(other);
                Self::All(conds)
            }
            _ => Self::All(vec![self, other]),
        }
    }

    /// Combine with OR.
    pub fn or(self, other: RouteCondition) -> Self {
        match self {
            Self::Any(mut conds) => {
                conds.push(other);
                Self::Any(conds)
            }
            _ => Self::Any(vec![self, other]),
        }
    }

    /// Negate condition.
    pub fn negate(self) -> Self {
        Self::Not(Box::new(self))
    }
}

/// A routing rule.
#[derive(Clone)]
pub struct RouteRule {
    /// Rule name (for debugging)
    pub name: String,
    /// Priority (higher = evaluated first)
    pub priority: i32,
    /// Condition for matching
    pub condition: RouteCondition,
    /// Destination(s) if matched
    pub destinations: Vec<String>,
    /// Whether to continue evaluating rules after match
    pub continue_matching: bool,
}

impl std::fmt::Debug for RouteRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RouteRule")
            .field("name", &self.name)
            .field("priority", &self.priority)
            .field("condition", &self.condition)
            .field("destinations", &self.destinations)
            .field("continue_matching", &self.continue_matching)
            .finish()
    }
}

impl RouteRule {
    /// Create a new route rule.
    pub fn new(
        name: impl Into<String>,
        condition: RouteCondition,
        destination: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            priority: 0,
            condition,
            destinations: vec![destination.into()],
            continue_matching: false,
        }
    }

    /// Create a table match rule.
    pub fn table_match(table: impl Into<String>, destination: impl Into<String>) -> Self {
        let table_str = table.into();
        Self::new(
            format!("table:{}", table_str),
            RouteCondition::Table(table_str),
            destination,
        )
    }

    /// Create an operation match rule.
    pub fn operation_match(op: CdcOp, destination: impl Into<String>) -> Self {
        Self::new(
            format!("op:{:?}", op),
            RouteCondition::Operation(op),
            destination,
        )
    }

    /// Set priority.
    pub fn with_priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    /// Set multiple destinations.
    pub fn with_destinations(mut self, destinations: Vec<String>) -> Self {
        self.destinations = destinations;
        self
    }

    /// Continue matching after this rule.
    pub fn and_continue(mut self) -> Self {
        self.continue_matching = true;
        self
    }

    /// Check if rule matches.
    pub fn matches(&self, event: &CdcEvent) -> bool {
        self.condition.matches(event)
    }
}

/// Router configuration.
#[derive(Debug, Clone, Default)]
pub struct RouterConfig {
    /// Default destination if no rules match
    pub default_destination: Option<String>,
    /// Dead letter queue
    pub dead_letter_queue: Option<String>,
    /// Drop unroutable events instead of DLQ
    pub drop_unroutable: bool,
    /// Enable statistics
    pub enable_stats: bool,
}

/// Event router.
pub struct EventRouter {
    rules: Vec<RouteRule>,
    config: RouterConfig,
    stats: RouterStats,
}

impl EventRouter {
    /// Create a new router builder.
    pub fn builder() -> EventRouterBuilder {
        EventRouterBuilder::default()
    }

    /// Create a router with rules.
    pub fn new(rules: Vec<RouteRule>, config: RouterConfig) -> Self {
        // Sort rules by priority (descending)
        let mut sorted_rules = rules;
        sorted_rules.sort_by(|a, b| b.priority.cmp(&a.priority));

        Self {
            rules: sorted_rules,
            config,
            stats: RouterStats::new(),
        }
    }

    /// Route an event.
    pub fn route(&self, event: &CdcEvent) -> RouteDecision {
        self.stats.record_event();

        let mut destinations = Vec::new();
        let mut matched = false;

        for rule in &self.rules {
            if rule.matches(event) {
                matched = true;
                self.stats.record_rule_match(&rule.name);
                destinations.extend(rule.destinations.clone());

                if !rule.continue_matching {
                    break;
                }
            }
        }

        if !destinations.is_empty() {
            // Deduplicate destinations
            destinations.sort();
            destinations.dedup();
            self.stats.record_routed(destinations.len() as u64);
            return RouteDecision::Route(destinations);
        }

        if !matched {
            // No rules matched, use default or DLQ
            if let Some(default) = &self.config.default_destination {
                self.stats.record_default();
                return RouteDecision::to(default.clone());
            }

            if self.config.drop_unroutable {
                self.stats.record_dropped();
                return RouteDecision::Drop;
            }

            if let Some(dlq) = &self.config.dead_letter_queue {
                self.stats.record_dead_letter();
                return RouteDecision::DeadLetter(dlq.clone());
            }
        }

        // No destination found
        self.stats.record_dropped();
        RouteDecision::Drop
    }

    /// Route multiple events.
    pub fn route_batch<'a>(&self, events: &'a [CdcEvent]) -> Vec<(RouteDecision, &'a CdcEvent)> {
        events.iter().map(|e| (self.route(e), e)).collect()
    }

    /// Route and group by destination.
    pub fn route_and_group<'a>(
        &self,
        events: &'a [CdcEvent],
    ) -> HashMap<String, Vec<&'a CdcEvent>> {
        let mut groups: HashMap<String, Vec<&'a CdcEvent>> = HashMap::new();

        for event in events {
            match self.route(event) {
                RouteDecision::Route(destinations) => {
                    for dest in destinations {
                        groups.entry(dest).or_default().push(event);
                    }
                }
                RouteDecision::DeadLetter(dlq) => {
                    groups.entry(dlq).or_default().push(event);
                }
                RouteDecision::Drop => {}
            }
        }

        groups
    }

    /// Get statistics.
    pub fn stats(&self) -> RouterStatsSnapshot {
        self.stats.snapshot()
    }

    /// Get number of rules.
    pub fn num_rules(&self) -> usize {
        self.rules.len()
    }
}

/// Builder for EventRouter.
#[derive(Default)]
pub struct EventRouterBuilder {
    rules: Vec<RouteRule>,
    config: RouterConfig,
}

impl EventRouterBuilder {
    /// Add a route rule.
    pub fn route(mut self, rule: RouteRule) -> Self {
        self.rules.push(rule);
        self
    }

    /// Add multiple route rules.
    pub fn routes(mut self, rules: Vec<RouteRule>) -> Self {
        self.rules.extend(rules);
        self
    }

    /// Set default destination.
    pub fn default_destination(mut self, destination: impl Into<String>) -> Self {
        self.config.default_destination = Some(destination.into());
        self
    }

    /// Set dead letter queue.
    pub fn dead_letter(mut self, queue: impl Into<String>) -> Self {
        self.config.dead_letter_queue = Some(queue.into());
        self
    }

    /// Drop unroutable events.
    pub fn drop_unroutable(mut self) -> Self {
        self.config.drop_unroutable = true;
        self
    }

    /// Enable statistics.
    pub fn with_stats(mut self) -> Self {
        self.config.enable_stats = true;
        self
    }

    /// Build the router.
    pub fn build(self) -> EventRouter {
        EventRouter::new(self.rules, self.config)
    }
}

/// Router statistics.
#[derive(Debug, Default)]
pub struct RouterStats {
    events_processed: AtomicU64,
    events_routed: AtomicU64,
    events_dropped: AtomicU64,
    events_dead_letter: AtomicU64,
    events_default: AtomicU64,
    destinations_total: AtomicU64,
    rule_matches: parking_lot::RwLock<HashMap<String, u64>>,
}

impl RouterStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_event(&self) {
        self.events_processed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_routed(&self, destinations: u64) {
        self.events_routed.fetch_add(1, Ordering::Relaxed);
        self.destinations_total
            .fetch_add(destinations, Ordering::Relaxed);
    }

    pub fn record_dropped(&self) {
        self.events_dropped.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_dead_letter(&self) {
        self.events_dead_letter.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_default(&self) {
        self.events_default.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_rule_match(&self, rule_name: &str) {
        let mut matches = self.rule_matches.write();
        *matches.entry(rule_name.to_string()).or_insert(0) += 1;
    }

    pub fn snapshot(&self) -> RouterStatsSnapshot {
        RouterStatsSnapshot {
            events_processed: self.events_processed.load(Ordering::Relaxed),
            events_routed: self.events_routed.load(Ordering::Relaxed),
            events_dropped: self.events_dropped.load(Ordering::Relaxed),
            events_dead_letter: self.events_dead_letter.load(Ordering::Relaxed),
            events_default: self.events_default.load(Ordering::Relaxed),
            destinations_total: self.destinations_total.load(Ordering::Relaxed),
            rule_matches: self.rule_matches.read().clone(),
        }
    }
}

/// Snapshot of router statistics.
#[derive(Debug, Clone)]
pub struct RouterStatsSnapshot {
    pub events_processed: u64,
    pub events_routed: u64,
    pub events_dropped: u64,
    pub events_dead_letter: u64,
    pub events_default: u64,
    pub destinations_total: u64,
    pub rule_matches: HashMap<String, u64>,
}

impl RouterStatsSnapshot {
    /// Get routing success rate.
    pub fn success_rate(&self) -> f64 {
        if self.events_processed == 0 {
            return 100.0;
        }
        (self.events_routed as f64 / self.events_processed as f64) * 100.0
    }

    /// Get average destinations per event.
    pub fn avg_destinations_per_event(&self) -> f64 {
        if self.events_routed == 0 {
            return 0.0;
        }
        self.destinations_total as f64 / self.events_routed as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_event(table: &str, op: CdcOp) -> CdcEvent {
        CdcEvent {
            source_type: "test".to_string(),
            database: "test".to_string(),
            schema: "public".to_string(),
            table: table.to_string(),
            op,
            before: None,
            after: Some(serde_json::json!({"id": 1, "status": "active"})),
            timestamp: chrono::Utc::now().timestamp(),
            transaction: None,
        }
    }

    #[test]
    fn test_route_decision_to() {
        let decision = RouteDecision::to("topic1");
        assert_eq!(decision.destinations(), &["topic1".to_string()]);
        assert!(!decision.is_drop());
        assert!(!decision.is_dead_letter());
    }

    #[test]
    fn test_route_decision_to_many() {
        let decision = RouteDecision::to_many(vec!["t1".to_string(), "t2".to_string()]);
        assert_eq!(decision.destinations().len(), 2);
    }

    #[test]
    fn test_route_decision_drop() {
        let decision = RouteDecision::Drop;
        assert!(decision.is_drop());
        assert!(decision.destinations().is_empty());
    }

    #[test]
    fn test_route_decision_dead_letter() {
        let decision = RouteDecision::DeadLetter("dlq".to_string());
        assert!(decision.is_dead_letter());
    }

    #[test]
    fn test_condition_always() {
        let cond = RouteCondition::Always;
        let event = make_event("users", CdcOp::Insert);
        assert!(cond.matches(&event));
    }

    #[test]
    fn test_condition_table() {
        let cond = RouteCondition::table("users");
        let event1 = make_event("users", CdcOp::Insert);
        let event2 = make_event("orders", CdcOp::Insert);

        assert!(cond.matches(&event1));
        assert!(!cond.matches(&event2));
    }

    #[test]
    fn test_condition_table_pattern() {
        let cond = RouteCondition::table_pattern("user*");
        let event1 = make_event("users", CdcOp::Insert);
        let event2 = make_event("user_profiles", CdcOp::Insert);
        let event3 = make_event("orders", CdcOp::Insert);

        assert!(cond.matches(&event1));
        assert!(cond.matches(&event2));
        assert!(!cond.matches(&event3));
    }

    #[test]
    fn test_condition_schema() {
        let cond = RouteCondition::schema("public");
        let event = make_event("users", CdcOp::Insert);
        assert!(cond.matches(&event));
    }

    #[test]
    fn test_condition_operation() {
        let cond = RouteCondition::operation(CdcOp::Insert);
        let event1 = make_event("users", CdcOp::Insert);
        let event2 = make_event("users", CdcOp::Update);

        assert!(cond.matches(&event1));
        assert!(!cond.matches(&event2));
    }

    #[test]
    fn test_condition_operations() {
        let cond = RouteCondition::operations(vec![CdcOp::Insert, CdcOp::Update]);
        let event1 = make_event("users", CdcOp::Insert);
        let event2 = make_event("users", CdcOp::Update);
        let event3 = make_event("users", CdcOp::Delete);

        assert!(cond.matches(&event1));
        assert!(cond.matches(&event2));
        assert!(!cond.matches(&event3));
    }

    #[test]
    fn test_condition_field_equals() {
        let cond = RouteCondition::field_equals("status", serde_json::json!("active"));
        let event = make_event("users", CdcOp::Insert);

        assert!(cond.matches(&event));
    }

    #[test]
    fn test_condition_field_exists() {
        let cond = RouteCondition::field_exists("id");
        let event = make_event("users", CdcOp::Insert);

        assert!(cond.matches(&event));
    }

    #[test]
    fn test_condition_and() {
        let cond = RouteCondition::table("users").and(RouteCondition::operation(CdcOp::Insert));

        let event1 = make_event("users", CdcOp::Insert);
        let event2 = make_event("users", CdcOp::Update);
        let event3 = make_event("orders", CdcOp::Insert);

        assert!(cond.matches(&event1));
        assert!(!cond.matches(&event2));
        assert!(!cond.matches(&event3));
    }

    #[test]
    fn test_condition_or() {
        let cond = RouteCondition::table("users").or(RouteCondition::table("orders"));

        let event1 = make_event("users", CdcOp::Insert);
        let event2 = make_event("orders", CdcOp::Insert);
        let event3 = make_event("products", CdcOp::Insert);

        assert!(cond.matches(&event1));
        assert!(cond.matches(&event2));
        assert!(!cond.matches(&event3));
    }

    #[test]
    fn test_condition_not() {
        let cond = RouteCondition::table("users").negate();

        let event1 = make_event("users", CdcOp::Insert);
        let event2 = make_event("orders", CdcOp::Insert);

        assert!(!cond.matches(&event1));
        assert!(cond.matches(&event2));
    }

    #[test]
    fn test_route_rule_table_match() {
        let rule = RouteRule::table_match("users", "user-topic");
        let event = make_event("users", CdcOp::Insert);

        assert!(rule.matches(&event));
        assert_eq!(rule.destinations, vec!["user-topic"]);
    }

    #[test]
    fn test_route_rule_operation_match() {
        let rule = RouteRule::operation_match(CdcOp::Delete, "delete-topic");

        let event1 = make_event("users", CdcOp::Delete);
        let event2 = make_event("users", CdcOp::Insert);

        assert!(rule.matches(&event1));
        assert!(!rule.matches(&event2));
    }

    #[test]
    fn test_route_rule_priority() {
        let rule = RouteRule::table_match("users", "topic").with_priority(10);

        assert_eq!(rule.priority, 10);
    }

    #[test]
    fn test_route_rule_multiple_destinations() {
        let rule = RouteRule::table_match("users", "topic1")
            .with_destinations(vec!["topic1".to_string(), "topic2".to_string()]);

        assert_eq!(rule.destinations.len(), 2);
    }

    #[test]
    fn test_router_basic() {
        let router = EventRouter::builder()
            .route(RouteRule::table_match("users", "user-events"))
            .route(RouteRule::table_match("orders", "order-events"))
            .build();

        let user_event = make_event("users", CdcOp::Insert);
        let order_event = make_event("orders", CdcOp::Insert);

        match router.route(&user_event) {
            RouteDecision::Route(dests) => assert_eq!(dests, vec!["user-events"]),
            _ => panic!("Expected Route decision"),
        }

        match router.route(&order_event) {
            RouteDecision::Route(dests) => assert_eq!(dests, vec!["order-events"]),
            _ => panic!("Expected Route decision"),
        }
    }

    #[test]
    fn test_router_default_destination() {
        let router = EventRouter::builder()
            .route(RouteRule::table_match("users", "user-events"))
            .default_destination("default-topic")
            .build();

        let unknown_event = make_event("products", CdcOp::Insert);

        match router.route(&unknown_event) {
            RouteDecision::Route(dests) => assert_eq!(dests, vec!["default-topic"]),
            _ => panic!("Expected Route to default"),
        }
    }

    #[test]
    fn test_router_dead_letter() {
        let router = EventRouter::builder()
            .route(RouteRule::table_match("users", "user-events"))
            .dead_letter("dlq")
            .build();

        let unknown_event = make_event("products", CdcOp::Insert);

        match router.route(&unknown_event) {
            RouteDecision::DeadLetter(dlq) => assert_eq!(dlq, "dlq"),
            _ => panic!("Expected DeadLetter decision"),
        }
    }

    #[test]
    fn test_router_drop_unroutable() {
        let router = EventRouter::builder()
            .route(RouteRule::table_match("users", "user-events"))
            .drop_unroutable()
            .build();

        let unknown_event = make_event("products", CdcOp::Insert);

        match router.route(&unknown_event) {
            RouteDecision::Drop => {}
            _ => panic!("Expected Drop decision"),
        }
    }

    #[test]
    fn test_router_priority() {
        let router = EventRouter::builder()
            .route(RouteRule::table_match("users", "low-priority").with_priority(1))
            .route(RouteRule::table_match("users", "high-priority").with_priority(10))
            .build();

        let event = make_event("users", CdcOp::Insert);

        // High priority rule should match first
        match router.route(&event) {
            RouteDecision::Route(dests) => assert_eq!(dests, vec!["high-priority"]),
            _ => panic!("Expected high-priority route"),
        }
    }

    #[test]
    fn test_router_continue_matching() {
        let router = EventRouter::builder()
            .route(
                RouteRule::table_match("users", "audit")
                    .with_priority(10)
                    .and_continue(),
            )
            .route(RouteRule::table_match("users", "main").with_priority(5))
            .build();

        let event = make_event("users", CdcOp::Insert);

        match router.route(&event) {
            RouteDecision::Route(mut dests) => {
                dests.sort();
                assert_eq!(dests, vec!["audit", "main"]);
            }
            _ => panic!("Expected multi-destination route"),
        }
    }

    #[test]
    fn test_router_batch() {
        let router = EventRouter::builder()
            .route(RouteRule::table_match("users", "user-events"))
            .route(RouteRule::table_match("orders", "order-events"))
            .build();

        let events = vec![
            make_event("users", CdcOp::Insert),
            make_event("orders", CdcOp::Insert),
            make_event("users", CdcOp::Update),
        ];

        let results = router.route_batch(&events);
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_router_group() {
        let router = EventRouter::builder()
            .route(RouteRule::table_match("users", "user-events"))
            .route(RouteRule::table_match("orders", "order-events"))
            .build();

        let events = vec![
            make_event("users", CdcOp::Insert),
            make_event("orders", CdcOp::Insert),
            make_event("users", CdcOp::Update),
        ];

        let groups = router.route_and_group(&events);

        assert_eq!(groups.get("user-events").map(|v| v.len()), Some(2));
        assert_eq!(groups.get("order-events").map(|v| v.len()), Some(1));
    }

    #[test]
    fn test_router_stats() {
        let router = EventRouter::builder()
            .route(RouteRule::table_match("users", "user-events"))
            .with_stats()
            .build();

        let event = make_event("users", CdcOp::Insert);
        router.route(&event);
        router.route(&event);

        let stats = router.stats();
        assert_eq!(stats.events_processed, 2);
        assert_eq!(stats.events_routed, 2);
    }

    #[test]
    fn test_router_stats_success_rate() {
        let router = EventRouter::builder()
            .route(RouteRule::table_match("users", "user-events"))
            .drop_unroutable()
            .build();

        let user_event = make_event("users", CdcOp::Insert);
        let other_event = make_event("products", CdcOp::Insert);

        router.route(&user_event);
        router.route(&other_event);

        let stats = router.stats();
        assert_eq!(stats.success_rate(), 50.0);
    }

    #[test]
    fn test_router_stats_avg_destinations() {
        let router = EventRouter::builder()
            .route(
                RouteRule::table_match("users", "t1")
                    .with_destinations(vec!["t1".to_string(), "t2".to_string()]),
            )
            .build();

        let event = make_event("users", CdcOp::Insert);
        router.route(&event);

        let stats = router.stats();
        assert_eq!(stats.avg_destinations_per_event(), 2.0);
    }

    #[test]
    fn test_router_num_rules() {
        let router = EventRouter::builder()
            .route(RouteRule::table_match("users", "t1"))
            .route(RouteRule::table_match("orders", "t2"))
            .build();

        assert_eq!(router.num_rules(), 2);
    }

    #[test]
    fn test_condition_predicate() {
        let cond = RouteCondition::Predicate(Arc::new(|e: &CdcEvent| e.table.starts_with("user")));

        let event1 = make_event("users", CdcOp::Insert);
        let event2 = make_event("orders", CdcOp::Insert);

        assert!(cond.matches(&event1));
        assert!(!cond.matches(&event2));
    }

    #[test]
    fn test_route_condition_debug() {
        let cond = RouteCondition::table("users");
        let debug_str = format!("{:?}", cond);
        assert!(debug_str.contains("Table"));
        assert!(debug_str.contains("users"));
    }

    #[test]
    fn test_route_rule_debug() {
        let rule = RouteRule::table_match("users", "topic");
        let debug_str = format!("{:?}", rule);
        assert!(debug_str.contains("RouteRule"));
    }
}
