//! # Pattern Matching for CDC Filtering
//!
//! High-performance pattern matching using compiled regular expressions.
//! Supports both regex and glob syntax for table/column filtering.
//!
//! ## Features
//!
//! - **Compiled patterns**: Regex compiled once, matched many times
//! - **Glob syntax**: User-friendly `*` and `?` wildcards (converted to regex)
//! - **Case-insensitive**: Default for SQL identifiers
//! - **Qualified name matching**: Matches `schema.table` or just `table`
//!
//! ## Example
//!
//! ```rust
//! use rivven_cdc::common::pattern::{PatternMatcher, PatternSet};
//!
//! // Single pattern matching
//! let matcher = PatternMatcher::new("public.*").unwrap();
//! assert!(matcher.matches("public.users"));
//! assert!(!matcher.matches("private.users"));
//!
//! // Pattern set for include/exclude logic
//! let mut set = PatternSet::new();
//! set.add("public.*").unwrap();
//! set.add("audit.*").unwrap();
//! assert!(set.matches("public.users"));
//! assert!(set.matches("audit.log"));
//! assert!(!set.matches("private.secrets"));
//!
//! // Qualified name matching (schema + table)
//! let matcher = PatternMatcher::new("*.users").unwrap();
//! assert!(matcher.matches_qualified("public", "users"));
//! assert!(matcher.matches_qualified("private", "users"));
//! ```

use regex::Regex;
use std::sync::Arc;
use std::sync::LazyLock;

/// Pattern syntax type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PatternSyntax {
    /// Glob syntax: `*` matches any chars, `?` matches single char
    #[default]
    Glob,
    /// Regular expression syntax
    Regex,
}

/// Error type for pattern operations
#[derive(Debug, thiserror::Error)]
pub enum PatternError {
    #[error("Invalid regex pattern: {0}")]
    InvalidRegex(#[from] regex::Error),
    #[error("Empty pattern")]
    EmptyPattern,
}

/// A compiled pattern matcher
///
/// Pre-compiles the pattern to regex for efficient repeated matching.
/// Supports both glob and regex syntax.
#[derive(Debug, Clone)]
pub struct PatternMatcher {
    /// Original pattern string (for display/debugging)
    pattern: String,
    /// Compiled regex (case-insensitive)
    regex: Regex,
    /// Whether this is a wildcard-only pattern (matches everything)
    is_wildcard: bool,
}

impl PatternMatcher {
    /// Create a new pattern matcher from a glob pattern
    ///
    /// Glob syntax:
    /// - `*` matches zero or more characters
    /// - `?` matches exactly one character
    ///
    /// # Example
    ///
    /// ```rust
    /// use rivven_cdc::common::pattern::PatternMatcher;
    ///
    /// let matcher = PatternMatcher::new("public.*").unwrap();
    /// assert!(matcher.matches("public.users"));
    /// ```
    pub fn new(pattern: &str) -> Result<Self, PatternError> {
        Self::with_syntax(pattern, PatternSyntax::Glob)
    }

    /// Create a new pattern matcher with explicit syntax
    pub fn with_syntax(pattern: &str, syntax: PatternSyntax) -> Result<Self, PatternError> {
        if pattern.is_empty() {
            return Err(PatternError::EmptyPattern);
        }

        let is_wildcard = pattern == "*" || pattern == ".*" || pattern == "^.*$";

        let regex_pattern = match syntax {
            PatternSyntax::Glob => glob_to_regex(pattern),
            PatternSyntax::Regex => pattern.to_string(),
        };

        // Compile case-insensitive regex
        let regex = regex::RegexBuilder::new(&regex_pattern)
            .case_insensitive(true)
            .build()?;

        Ok(Self {
            pattern: pattern.to_string(),
            regex,
            is_wildcard,
        })
    }

    /// Check if text matches the pattern
    #[inline]
    pub fn matches(&self, text: &str) -> bool {
        if self.is_wildcard {
            return true;
        }
        self.regex.is_match(text)
    }

    /// Check if a qualified name (schema.table) matches the pattern
    ///
    /// Matches against both the full qualified name and the table name alone.
    pub fn matches_qualified(&self, schema: &str, table: &str) -> bool {
        if self.is_wildcard {
            return true;
        }

        // Try full qualified name first
        let full_name = format!("{}.{}", schema, table);
        if self.regex.is_match(&full_name) {
            return true;
        }

        // Also try just the table name (for unqualified patterns)
        self.regex.is_match(table)
    }

    /// Get the original pattern string
    pub fn pattern(&self) -> &str {
        &self.pattern
    }

    /// Check if this is a wildcard pattern (matches everything)
    pub fn is_wildcard(&self) -> bool {
        self.is_wildcard
    }
}

/// A set of patterns for batch matching
///
/// Efficiently checks if text matches any pattern in the set.
/// Optimized for the common case of include/exclude filtering.
#[derive(Debug, Clone, Default)]
pub struct PatternSet {
    patterns: Vec<PatternMatcher>,
    has_wildcard: bool,
}

impl PatternSet {
    /// Create an empty pattern set
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a pattern set from a list of glob patterns
    pub fn from_patterns(patterns: &[String]) -> Result<Self, PatternError> {
        Self::from_patterns_with_syntax(patterns, PatternSyntax::Glob)
    }

    /// Create a pattern set from a list of patterns with explicit syntax
    pub fn from_patterns_with_syntax(
        patterns: &[String],
        syntax: PatternSyntax,
    ) -> Result<Self, PatternError> {
        let mut set = Self::new();
        for pattern in patterns {
            set.add_with_syntax(pattern, syntax)?;
        }
        Ok(set)
    }

    /// Add a glob pattern to the set
    pub fn add(&mut self, pattern: &str) -> Result<(), PatternError> {
        self.add_with_syntax(pattern, PatternSyntax::Glob)
    }

    /// Add a pattern with explicit syntax
    pub fn add_with_syntax(
        &mut self,
        pattern: &str,
        syntax: PatternSyntax,
    ) -> Result<(), PatternError> {
        let matcher = PatternMatcher::with_syntax(pattern, syntax)?;
        if matcher.is_wildcard {
            self.has_wildcard = true;
        }
        self.patterns.push(matcher);
        Ok(())
    }

    /// Check if text matches any pattern in the set
    #[inline]
    pub fn matches(&self, text: &str) -> bool {
        if self.has_wildcard {
            return true;
        }
        self.patterns.iter().any(|p| p.matches(text))
    }

    /// Check if a qualified name matches any pattern in the set
    pub fn matches_qualified(&self, schema: &str, table: &str) -> bool {
        if self.has_wildcard {
            return true;
        }
        self.patterns
            .iter()
            .any(|p| p.matches_qualified(schema, table))
    }

    /// Check if the set is empty
    pub fn is_empty(&self) -> bool {
        self.patterns.is_empty()
    }

    /// Get the number of patterns in the set
    pub fn len(&self) -> usize {
        self.patterns.len()
    }

    /// Iterate over the patterns
    pub fn iter(&self) -> impl Iterator<Item = &PatternMatcher> {
        self.patterns.iter()
    }
}

/// Thread-safe shared pattern set
///
/// For use in concurrent CDC pipelines where patterns are shared across tasks.
pub type SharedPatternSet = Arc<PatternSet>;

/// Convert a glob pattern to regex
///
/// Escapes special regex characters and converts:
/// - `*` → `.*`
/// - `?` → `.`
fn glob_to_regex(pattern: &str) -> String {
    let escaped = regex::escape(pattern);
    let regex_pattern = escaped.replace(r"\*", ".*").replace(r"\?", ".");
    format!("^{}$", regex_pattern)
}

// ============================================================================
// Legacy API compatibility
// ============================================================================

/// Global pattern cache for frequently used patterns
/// F-097: `parking_lot` — O(1) pattern cache lookup, never held across `.await`.
static PATTERN_CACHE: LazyLock<
    parking_lot::RwLock<std::collections::HashMap<String, PatternMatcher>>,
> = LazyLock::new(|| parking_lot::RwLock::new(std::collections::HashMap::new()));

/// Match a text against a glob pattern (case-insensitive)
///
/// **Note**: For repeated matching, prefer [`PatternMatcher`] to avoid
/// repeated pattern compilation.
///
/// # Example
///
/// ```rust
/// use rivven_cdc::common::pattern_match;
///
/// assert!(pattern_match("public.*", "public.users"));
/// assert!(pattern_match("*.users", "schema.users"));
/// ```
#[inline]
pub fn pattern_match(pattern: &str, text: &str) -> bool {
    // Fast path for exact match
    if !pattern.contains('*') && !pattern.contains('?') {
        return pattern.eq_ignore_ascii_case(text);
    }

    // Fast path for wildcard
    if pattern == "*" {
        return true;
    }

    // Use cached pattern if available
    {
        let cache = PATTERN_CACHE.read();
        if let Some(matcher) = cache.get(pattern) {
            return matcher.matches(text);
        }
    }

    // Compile and cache the pattern
    if let Ok(matcher) = PatternMatcher::new(pattern) {
        let result = matcher.matches(text);
        // Only cache if the cache isn't too large
        let mut cache = PATTERN_CACHE.write();
        if cache.len() < 1000 {
            cache.insert(pattern.to_string(), matcher);
        }
        result
    } else {
        false
    }
}

/// Match a text against a glob pattern (case-sensitive)
///
/// **Note**: For repeated matching, prefer [`PatternMatcher`].
pub fn pattern_match_case_sensitive(pattern: &str, text: &str) -> bool {
    // Fast path for exact match
    if !pattern.contains('*') && !pattern.contains('?') {
        return pattern == text;
    }

    // Fast path for wildcard
    if pattern == "*" {
        return true;
    }

    // For case-sensitive, we don't use the cache (different behavior)
    let regex_pattern = glob_to_regex(pattern);
    if let Ok(regex) = Regex::new(&regex_pattern) {
        regex.is_match(text)
    } else {
        false
    }
}

/// Check if a qualified name (schema.table) matches a pattern
///
/// Matches against both the full qualified name and the table name alone.
///
/// # Example
///
/// ```rust
/// use rivven_cdc::common::matches_qualified_name;
///
/// assert!(matches_qualified_name("public.*", "public", "users"));
/// assert!(matches_qualified_name("users", "public", "users")); // table-only match
/// ```
#[inline]
pub fn matches_qualified_name(pattern: &str, schema: &str, table: &str) -> bool {
    let full_name = format!("{}.{}", schema, table);
    pattern_match(pattern, &full_name) || pattern_match(pattern, table)
}

/// Check if a qualified name matches any pattern in a list
pub fn matches_any_pattern(patterns: &[String], schema: &str, table: &str) -> bool {
    patterns
        .iter()
        .any(|p| matches_qualified_name(p, schema, table))
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pattern_matcher_exact() {
        let matcher = PatternMatcher::new("exact").unwrap();
        assert!(matcher.matches("exact"));
        assert!(matcher.matches("EXACT")); // Case insensitive
        assert!(!matcher.matches("different"));
    }

    #[test]
    fn test_pattern_matcher_wildcard() {
        let matcher = PatternMatcher::new("*").unwrap();
        assert!(matcher.matches("anything"));
        assert!(matcher.matches(""));
        assert!(matcher.is_wildcard());
    }

    #[test]
    fn test_pattern_matcher_star() {
        let matcher = PatternMatcher::new("prefix*").unwrap();
        assert!(matcher.matches("prefix_suffix"));
        assert!(matcher.matches("prefix"));
        assert!(!matcher.matches("other"));

        let matcher = PatternMatcher::new("*suffix").unwrap();
        assert!(matcher.matches("prefix_suffix"));
        assert!(matcher.matches("suffix"));
        assert!(!matcher.matches("other"));

        let matcher = PatternMatcher::new("pre*fix").unwrap();
        assert!(matcher.matches("prefix"));
        assert!(matcher.matches("pre_fix"));
        assert!(matcher.matches("pre123fix"));
    }

    #[test]
    fn test_pattern_matcher_question() {
        let matcher = PatternMatcher::new("user?").unwrap();
        assert!(matcher.matches("users"));
        assert!(matcher.matches("userA"));
        assert!(!matcher.matches("user"));
        assert!(!matcher.matches("username"));
    }

    #[test]
    fn test_pattern_matcher_qualified() {
        let matcher = PatternMatcher::new("public.*").unwrap();
        assert!(matcher.matches_qualified("public", "users"));
        assert!(matcher.matches_qualified("public", "orders"));
        assert!(!matcher.matches_qualified("private", "users"));

        let matcher = PatternMatcher::new("*.users").unwrap();
        assert!(matcher.matches_qualified("public", "users"));
        assert!(matcher.matches_qualified("private", "users"));
        assert!(!matcher.matches_qualified("public", "orders"));

        // Table-only pattern
        let matcher = PatternMatcher::new("users").unwrap();
        assert!(matcher.matches_qualified("public", "users"));
        assert!(matcher.matches_qualified("private", "users"));
    }

    #[test]
    fn test_pattern_set() {
        let mut set = PatternSet::new();
        set.add("public.*").unwrap();
        set.add("audit.*").unwrap();

        assert!(set.matches("public.users"));
        assert!(set.matches("audit.log"));
        assert!(!set.matches("private.secrets"));
    }

    #[test]
    fn test_pattern_set_qualified() {
        let set =
            PatternSet::from_patterns(&["temp_*".to_string(), "audit.*".to_string()]).unwrap();

        assert!(set.matches_qualified("audit", "log"));
        assert!(set.matches_qualified("public", "temp_data"));
        assert!(!set.matches_qualified("public", "users"));
    }

    #[test]
    fn test_pattern_set_wildcard() {
        let mut set = PatternSet::new();
        set.add("*").unwrap();
        assert!(set.has_wildcard);
        assert!(set.matches("anything"));
    }

    #[test]
    fn test_legacy_pattern_match() {
        assert!(pattern_match("exact", "exact"));
        assert!(pattern_match("exact", "EXACT"));
        assert!(!pattern_match("exact", "different"));

        assert!(pattern_match("*", "anything"));
        assert!(pattern_match("public.*", "public.users"));
        assert!(pattern_match("user?", "users"));
    }

    #[test]
    fn test_legacy_qualified_name() {
        assert!(matches_qualified_name("public.*", "public", "users"));
        assert!(matches_qualified_name("*.users", "schema", "users"));
        assert!(matches_qualified_name("users", "public", "users"));
        assert!(!matches_qualified_name("private.*", "public", "users"));
    }

    #[test]
    fn test_legacy_matches_any_pattern() {
        let patterns = vec!["temp_*".to_string(), "audit.*".to_string()];

        assert!(matches_any_pattern(&patterns, "audit", "log"));
        assert!(matches_any_pattern(&patterns, "public", "temp_data"));
        assert!(!matches_any_pattern(&patterns, "public", "users"));
    }

    #[test]
    fn test_pattern_syntax_regex() {
        let matcher =
            PatternMatcher::with_syntax(r"^public\.(users|orders)$", PatternSyntax::Regex).unwrap();
        assert!(matcher.matches("public.users"));
        assert!(matcher.matches("public.orders"));
        assert!(!matcher.matches("public.products"));
    }

    #[test]
    fn test_edge_cases() {
        // Empty text with wildcard
        let matcher = PatternMatcher::new("*").unwrap();
        assert!(matcher.matches(""));

        // Special regex chars in pattern are escaped
        let matcher = PatternMatcher::new("table.name").unwrap();
        assert!(matcher.matches("table.name"));
        assert!(!matcher.matches("tablexname")); // . is escaped, not regex wildcard

        // Multiple stars
        let matcher = PatternMatcher::new("*.*").unwrap();
        assert!(matcher.matches("schema.table"));

        // Pattern with brackets (should be escaped)
        let matcher = PatternMatcher::new("table[1]").unwrap();
        assert!(matcher.matches("table[1]"));
        assert!(!matcher.matches("table1"));
    }

    #[test]
    fn test_error_cases() {
        assert!(PatternMatcher::new("").is_err());
        assert!(PatternMatcher::with_syntax("[invalid", PatternSyntax::Regex).is_err());
    }
}
