//! Topic Resolver for CDC connectors
//!
//! Resolves topic names from patterns using CDC event metadata.
//! This enables dynamic topic routing based on database, schema, and table names,
//! following industry-standard topic naming conventions.
//!
//! ## Supported Placeholders
//!
//! | Placeholder | Description | Example Value |
//! |-------------|-------------|---------------|
//! | `{database}` | Database name | `mydb` |
//! | `{schema}` | Schema name | `public` |
//! | `{table}` | Table name | `users` |
//!
//! ## Example
//!
//! ```rust
//! use rivven_connect::topic_resolver::{TopicResolver, TopicMetadata};
//!
//! let resolver = TopicResolver::new("cdc.{schema}.{table}").unwrap();
//! let metadata = TopicMetadata::new("mydb", "public", "users");
//!
//! // Resolves to "cdc.public.users"
//! assert_eq!(resolver.resolve(&metadata), "cdc.public.users");
//! ```
//!
//! ## Topic Name Normalization
//!
//! Database identifiers (schema/table names) often contain characters that are
//! invalid in topic names. The resolver can automatically normalize these:
//!
//! | Original | Normalized | Reason |
//! |----------|------------|--------|
//! | `user@data` | `user_data` | Special characters replaced |
//! | `My Table` | `my_table` | Spaces replaced, lowercase (optional) |
//! | `MyTable` | `my_table` | snake_case conversion (optional) |
//! | `my-table` | `my_table` | kebab-case to snake_case (optional) |
//!
//! ## Case Conversion Strategies
//!
//! | Strategy | Input | Output | Use Case |
//! |----------|-------|--------|----------|
//! | `None` (default) | `MyTable` | `MyTable` | Preserve original case |
//! | `Lower` | `MyTable` | `mytable` | Simple lowercase |
//! | `Upper` | `MyTable` | `MYTABLE` | Uppercase convention |
//! | `SnakeCase` | `MyTable` | `my_table` | Topic-friendly snake_case |
//! | `KebabCase` | `MyTable` | `my-table` | URL-friendly kebab-case |
//!
//! ## Avro Schema Compatibility
//!
//! When using Schema Registry with Avro, topic names must be valid Avro names:
//! - Must start with a letter or underscore
//! - Can only contain letters, digits, and underscores
//!
//! Enable Avro-compatible mode for strict compliance:
//!
//! ```rust
//! use rivven_connect::topic_resolver::{TopicResolverBuilder};
//!
//! let resolver = TopicResolverBuilder::new("cdc.{schema}.{table}")
//!     .avro_compatible()
//!     .build()
//!     .unwrap();
//! ```
//!
//! ## Architecture: TopicResolver vs EventRouter
//!
//! Rivven provides two complementary routing mechanisms for CDC events:
//!
//! | Aspect | TopicResolver | EventRouter |
//! |--------|---------------|-------------|
//! | **Location** | rivven-connect | rivven-cdc |
//! | **Purpose** | Topic name resolution | Content-based routing |
//! | **Input** | CDC metadata (db/schema/table) | CDC event content (fields, op type) |
//! | **Output** | Broker topic name | Abstract destination(s) |
//! | **Config** | `topic_routing` pattern | Programmatic rules |
//! | **Scope** | CDC connectors only | Any CDC pipeline |
//!
//! ### When to use TopicResolver
//!
//! - Route events from different tables to different topics
//! - Follow standard `prefix.schema.table` naming
//! - Simple, config-driven routing based on source metadata
//! - Automatic topic name normalization for database identifiers
//!
//! ### When to use EventRouter (rivven-cdc)
//!
//! - Route based on event content (field values, operation type)
//! - Complex conditional routing with priorities
//! - Dead letter queue handling for unroutable events
//! - Multi-destination fan-out
//!
//! ### Composing Both
//!
//! TopicResolver and EventRouter can be composed: EventRouter routes to
//! logical destinations, which can be patterns that TopicResolver resolves
//! to actual broker topic names.
//!
//! ```text
//! CDC Event
//!     │
//!     ▼
//! ┌──────────────────┐
//! │   EventRouter    │ ─── Content-based routing (rivven-cdc)
//! │  (rivven-cdc)    │     "orders with priority=high → priority-orders"
//! └────────┬─────────┘
//!          │ logical destination
//!          ▼
//! ┌──────────────────┐
//! │  TopicResolver   │ ─── Metadata-based naming (rivven-connect)
//! │ (rivven-connect) │     "priority-orders.{schema}.{table}"
//! └────────┬─────────┘
//!          │ actual topic name
//!          ▼
//!   "priority-orders.public.orders"
//! ```

use std::collections::HashSet;
use std::sync::LazyLock;

/// Pre-compiled regex for placeholder extraction and validation
static PLACEHOLDER_REGEX: LazyLock<regex::Regex> = LazyLock::new(|| {
    regex::Regex::new(r"\{([a-z_][a-z0-9_]*)\}")
        .expect("placeholder regex pattern is invalid - this is a bug")
});

/// Valid placeholder names for topic routing
const VALID_PLACEHOLDERS: &[&str] = &["database", "schema", "table"];

/// Maximum length for topic names (industry standard)
const MAX_TOPIC_LENGTH: usize = 249;

/// Case conversion strategies for topic name normalization.
///
/// Different systems have different naming conventions. This enum allows
/// converting identifiers to match your preferred style.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CaseConversion {
    /// Preserve original case (default)
    #[default]
    None,
    /// Convert to lowercase: `MyTable` → `mytable`
    Lower,
    /// Convert to UPPERCASE: `MyTable` → `MYTABLE`
    Upper,
    /// Convert to snake_case: `MyTable` → `my_table`, `myTable` → `my_table`
    SnakeCase,
    /// Convert to kebab-case: `MyTable` → `my-table`, `myTable` → `my-table`
    KebabCase,
}

impl CaseConversion {
    /// Apply case conversion to a string.
    pub fn apply(&self, s: &str) -> String {
        match self {
            Self::None => s.to_string(),
            Self::Lower => s.to_lowercase(),
            Self::Upper => s.to_uppercase(),
            Self::SnakeCase => Self::to_snake_case(s),
            Self::KebabCase => Self::to_kebab_case(s),
        }
    }

    /// Convert string to snake_case.
    ///
    /// Handles PascalCase, camelCase, and existing delimiters.
    fn to_snake_case(s: &str) -> String {
        let mut result = String::with_capacity(s.len() + 10);
        let mut prev_was_upper = false;
        let mut prev_was_delimiter = true; // Start as true to handle leading chars

        for c in s.chars() {
            if c == '-' || c == '_' || c == ' ' {
                if !result.is_empty() && !result.ends_with('_') {
                    result.push('_');
                }
                prev_was_delimiter = true;
                prev_was_upper = false;
            } else if c.is_uppercase() {
                // Insert underscore before uppercase if:
                // - Not at start
                // - Previous was lowercase
                // - Not already preceded by delimiter or underscore
                if !result.is_empty() && !prev_was_delimiter && !prev_was_upper {
                    result.push('_');
                }
                result.push(c.to_ascii_lowercase());
                prev_was_upper = true;
                prev_was_delimiter = false;
            } else {
                result.push(c.to_ascii_lowercase());
                prev_was_upper = false;
                prev_was_delimiter = false;
            }
        }

        result
    }

    /// Convert string to kebab-case.
    fn to_kebab_case(s: &str) -> String {
        let mut result = String::with_capacity(s.len() + 10);
        let mut prev_was_upper = false;
        let mut prev_was_delimiter = true;

        for c in s.chars() {
            if c == '-' || c == '_' || c == ' ' {
                if !result.is_empty() && !result.ends_with('-') {
                    result.push('-');
                }
                prev_was_delimiter = true;
                prev_was_upper = false;
            } else if c.is_uppercase() {
                if !result.is_empty() && !prev_was_delimiter && !prev_was_upper {
                    result.push('-');
                }
                result.push(c.to_ascii_lowercase());
                prev_was_upper = true;
                prev_was_delimiter = false;
            } else {
                result.push(c.to_ascii_lowercase());
                prev_was_upper = false;
                prev_was_delimiter = false;
            }
        }

        result
    }
}

/// Configuration for topic name normalization.
///
/// Database identifiers (schema/table names) often contain characters that are
/// invalid in topic names. This configuration controls how these are normalized.
///
/// ## Features
///
/// - **Character replacement**: Replace invalid characters with configurable replacement
/// - **Case conversion**: Convert to lowercase, UPPERCASE, snake_case, or kebab-case
/// - **Avro compatibility**: Ensure names are valid Avro identifiers for Schema Registry
/// - **Prefix/suffix stripping**: Remove common prefixes like `dbo_`, `public_`
/// - **Length management**: Truncate long names with optional hash for uniqueness
///
/// ## Example
///
/// ```rust
/// use rivven_connect::topic_resolver::{NormalizationConfig, CaseConversion};
///
/// let config = NormalizationConfig::new()
///     .with_case_conversion(CaseConversion::SnakeCase)
///     .with_avro_compatible()
///     .with_strip_prefixes(vec!["dbo_".to_string(), "public_".to_string()]);
///
/// assert_eq!(config.normalize_identifier("dbo_MyTable"), "my_table");
/// ```
#[derive(Debug, Clone)]
pub struct NormalizationConfig {
    /// Whether to normalize placeholder values (default: true)
    pub enabled: bool,
    /// Case conversion strategy (default: None - preserve original case)
    pub case_conversion: CaseConversion,
    /// Character to replace invalid characters with (default: '_')
    pub replacement_char: char,
    /// Truncate long topic names to max length (default: true)
    pub truncate_long_names: bool,
    /// Add hash suffix when truncating to ensure uniqueness (default: true)
    pub hash_on_truncate: bool,
    /// Enforce Avro-compatible naming (letters, digits, underscores only, must start with letter/underscore)
    pub avro_compatible: bool,
    /// Prefixes to strip from identifiers
    pub strip_prefixes: Vec<String>,
    /// Suffixes to strip from identifiers
    pub strip_suffixes: Vec<String>,
}

impl Default for NormalizationConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            case_conversion: CaseConversion::None,
            replacement_char: '_',
            truncate_long_names: true,
            hash_on_truncate: true,
            avro_compatible: false,
            strip_prefixes: Vec::new(),
            strip_suffixes: Vec::new(),
        }
    }
}

impl NormalizationConfig {
    /// Create a new normalization config with defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Disable normalization entirely.
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            ..Default::default()
        }
    }

    /// Enable lowercase normalization (convenience method).
    ///
    /// Equivalent to `with_case_conversion(CaseConversion::Lower)`.
    pub fn with_lowercase(mut self) -> Self {
        self.case_conversion = CaseConversion::Lower;
        self
    }

    /// Set case conversion strategy.
    pub fn with_case_conversion(mut self, conversion: CaseConversion) -> Self {
        self.case_conversion = conversion;
        self
    }

    /// Set the replacement character for invalid characters.
    pub fn with_replacement_char(mut self, c: char) -> Self {
        self.replacement_char = c;
        self
    }

    /// Disable truncation of long names.
    pub fn without_truncation(mut self) -> Self {
        self.truncate_long_names = false;
        self
    }

    /// Disable hash suffix when truncating.
    pub fn without_hash(mut self) -> Self {
        self.hash_on_truncate = false;
        self
    }

    /// Enable Avro-compatible naming.
    ///
    /// Avro names must:
    /// - Start with a letter (a-zA-Z) or underscore
    /// - Contain only letters, digits, and underscores
    ///
    /// When enabled:
    /// - Hyphens and dots are replaced with underscores
    /// - Names starting with digits are prefixed with underscore
    pub fn with_avro_compatible(mut self) -> Self {
        self.avro_compatible = true;
        // Avro only allows underscores, not hyphens
        if self.replacement_char == '-' {
            self.replacement_char = '_';
        }
        self
    }

    /// Set prefixes to strip from identifiers.
    ///
    /// Common prefixes like `dbo_`, `public_`, `schema_` can be removed.
    pub fn with_strip_prefixes(mut self, prefixes: Vec<String>) -> Self {
        self.strip_prefixes = prefixes;
        self
    }

    /// Set suffixes to strip from identifiers.
    pub fn with_strip_suffixes(mut self, suffixes: Vec<String>) -> Self {
        self.strip_suffixes = suffixes;
        self
    }

    /// Normalize a single identifier (database/schema/table name).
    ///
    /// Topic names allow: `[a-zA-Z0-9._-]`
    /// Database identifiers might contain: spaces, @, #, etc.
    ///
    /// Normalization steps (in order):
    /// 1. Strip configured prefixes/suffixes
    /// 2. Apply case conversion
    /// 3. Replace invalid characters
    /// 4. Ensure Avro compatibility (if enabled)
    /// 5. Ensure non-empty result
    pub fn normalize_identifier(&self, identifier: &str) -> String {
        if !self.enabled || identifier.is_empty() {
            return identifier.to_string();
        }

        let mut result = identifier.to_string();

        // Step 1: Strip prefixes and suffixes (case-insensitive matching)
        for prefix in &self.strip_prefixes {
            if result.to_lowercase().starts_with(&prefix.to_lowercase()) {
                result = result[prefix.len()..].to_string();
            }
        }
        for suffix in &self.strip_suffixes {
            if result.to_lowercase().ends_with(&suffix.to_lowercase()) {
                result = result[..result.len() - suffix.len()].to_string();
            }
        }

        // Step 2: Apply case conversion
        result = self.case_conversion.apply(&result);

        // Step 3: Replace invalid characters
        result = self.replace_invalid_chars(&result);

        // Step 4: Ensure Avro compatibility
        if self.avro_compatible {
            result = self.ensure_avro_compatible(&result);
        }

        // Step 5: Ensure non-empty result
        if result.is_empty() {
            result.push(self.replacement_char);
        }

        result
    }

    /// Replace characters invalid for topic names.
    fn replace_invalid_chars(&self, s: &str) -> String {
        let mut result = String::with_capacity(s.len());
        let valid_special_chars = if self.avro_compatible {
            // Avro only allows underscores
            &['_'][..]
        } else {
            // Standard topic names allow underscore, hyphen, and dot
            &['_', '-'][..]
        };

        for c in s.chars() {
            if c.is_ascii_alphanumeric() || valid_special_chars.contains(&c) {
                result.push(c);
            } else if c == '.' && !self.avro_compatible {
                // Dots in identifiers become underscores to avoid creating extra topic segments
                result.push(self.replacement_char);
            } else {
                result.push(self.replacement_char);
            }
        }

        result
    }

    /// Ensure the identifier is a valid Avro name.
    fn ensure_avro_compatible(&self, s: &str) -> String {
        if s.is_empty() {
            return "_".to_string();
        }

        let mut result = s.to_string();

        // Avro names must start with letter or underscore
        if let Some(first_char) = result.chars().next() {
            if first_char.is_ascii_digit() {
                result = format!("_{}", result);
            }
        }

        // Replace any remaining invalid chars (hyphens, dots, etc.) with underscores
        result = result
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect();

        result
    }

    /// Normalize a complete topic name (after placeholder substitution).
    ///
    /// This handles the final topic name, ensuring it's valid.
    pub fn normalize_topic_name(&self, topic: &str) -> String {
        if !self.enabled {
            return topic.to_string();
        }

        let mut result = topic.to_string();

        // Apply case conversion to the entire topic name
        result = self.case_conversion.apply(&result);

        // Handle truncation if needed
        if self.truncate_long_names && result.len() > MAX_TOPIC_LENGTH {
            result = self.truncate_with_hash(&result);
        }

        result
    }

    /// Truncate a topic name with optional hash suffix for uniqueness.
    fn truncate_with_hash(&self, topic: &str) -> String {
        if self.hash_on_truncate {
            // Use a simple hash of the full name
            let hash = Self::simple_hash(topic);
            let hash_suffix = format!("_{:08x}", hash);
            let max_prefix_len = MAX_TOPIC_LENGTH - hash_suffix.len();
            format!("{}{}", &topic[..max_prefix_len], hash_suffix)
        } else {
            topic[..MAX_TOPIC_LENGTH].to_string()
        }
    }

    /// Simple hash function for truncation uniqueness.
    fn simple_hash(s: &str) -> u32 {
        let mut hash: u32 = 0;
        for byte in s.bytes() {
            hash = hash.wrapping_mul(31).wrapping_add(byte as u32);
        }
        hash
    }

    /// Check if the current configuration is Avro-compatible.
    pub fn is_avro_compatible(&self) -> bool {
        self.avro_compatible
    }
}

/// Topic resolver for CDC events.
///
/// Resolves dynamic topic names from patterns using CDC event metadata.
/// Supports automatic normalization of database identifiers to valid topic names.
#[derive(Debug, Clone)]
pub struct TopicResolver {
    /// The pattern with placeholders
    pattern: String,
    /// Extracted placeholders from the pattern
    placeholders: HashSet<String>,
    /// Whether the pattern contains any placeholders
    has_placeholders: bool,
    /// Normalization configuration
    normalization: NormalizationConfig,
}

/// Metadata required for topic resolution
#[derive(Debug, Clone)]
pub struct TopicMetadata<'a> {
    /// Database name
    pub database: &'a str,
    /// Schema name
    pub schema: &'a str,
    /// Table name
    pub table: &'a str,
}

impl<'a> TopicMetadata<'a> {
    /// Create new topic metadata
    pub fn new(database: &'a str, schema: &'a str, table: &'a str) -> Self {
        Self {
            database,
            schema,
            table,
        }
    }
}

/// Validation error for topic patterns
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopicPatternError {
    /// Pattern is empty
    EmptyPattern,
    /// Pattern contains invalid placeholder
    InvalidPlaceholder(String),
    /// Pattern contains unclosed brace
    UnclosedBrace,
    /// Pattern contains invalid characters
    InvalidCharacters(String),
}

impl std::fmt::Display for TopicPatternError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyPattern => write!(f, "topic pattern cannot be empty"),
            Self::InvalidPlaceholder(p) => write!(
                f,
                "invalid placeholder '{{{}}}', valid placeholders are: {}",
                p,
                VALID_PLACEHOLDERS.join(", ")
            ),
            Self::UnclosedBrace => write!(f, "topic pattern contains unclosed brace"),
            Self::InvalidCharacters(chars) => {
                write!(f, "topic pattern contains invalid characters: {}", chars)
            }
        }
    }
}

impl std::error::Error for TopicPatternError {}

impl TopicResolver {
    /// Create a new topic resolver from a pattern.
    ///
    /// Returns an error if the pattern is invalid.
    /// Uses default normalization settings.
    ///
    /// # Example
    ///
    /// ```rust
    /// use rivven_connect::topic_resolver::TopicResolver;
    ///
    /// let resolver = TopicResolver::new("cdc.{schema}.{table}").unwrap();
    /// ```
    pub fn new(pattern: impl Into<String>) -> Result<Self, TopicPatternError> {
        Self::with_normalization(pattern, NormalizationConfig::default())
    }

    /// Create a topic resolver with custom normalization config.
    ///
    /// # Example
    ///
    /// ```rust
    /// use rivven_connect::topic_resolver::{TopicResolver, NormalizationConfig};
    ///
    /// let resolver = TopicResolver::with_normalization(
    ///     "cdc.{schema}.{table}",
    ///     NormalizationConfig::new().with_lowercase()
    /// ).unwrap();
    /// ```
    pub fn with_normalization(
        pattern: impl Into<String>,
        normalization: NormalizationConfig,
    ) -> Result<Self, TopicPatternError> {
        let pattern = pattern.into();

        // Validate pattern
        Self::validate_pattern(&pattern)?;

        // Extract placeholders
        let placeholders: HashSet<String> = PLACEHOLDER_REGEX
            .captures_iter(&pattern)
            .map(|cap| cap[1].to_string())
            .collect();

        let has_placeholders = !placeholders.is_empty();

        Ok(Self {
            pattern,
            placeholders,
            has_placeholders,
            normalization,
        })
    }

    /// Create a builder for configuring the topic resolver.
    pub fn builder(pattern: impl Into<String>) -> TopicResolverBuilder {
        TopicResolverBuilder::new(pattern)
    }

    /// Validate a topic pattern.
    fn validate_pattern(pattern: &str) -> Result<(), TopicPatternError> {
        if pattern.is_empty() {
            return Err(TopicPatternError::EmptyPattern);
        }

        // Check for unclosed braces
        let open_count = pattern.chars().filter(|c| *c == '{').count();
        let close_count = pattern.chars().filter(|c| *c == '}').count();
        if open_count != close_count {
            return Err(TopicPatternError::UnclosedBrace);
        }

        // Extract and validate placeholders
        for cap in PLACEHOLDER_REGEX.captures_iter(pattern) {
            let placeholder = &cap[1];
            if !VALID_PLACEHOLDERS.contains(&placeholder) {
                return Err(TopicPatternError::InvalidPlaceholder(
                    placeholder.to_string(),
                ));
            }
        }

        // Check for invalid characters in the non-placeholder parts
        let mut temp = pattern.to_string();
        for cap in PLACEHOLDER_REGEX.captures_iter(pattern) {
            temp = temp.replace(&cap[0], "");
        }

        // Valid topic characters: alphanumeric, dot, underscore, hyphen
        let invalid_chars: String = temp
            .chars()
            .filter(|c| !c.is_alphanumeric() && *c != '.' && *c != '_' && *c != '-')
            .collect();

        if !invalid_chars.is_empty() {
            return Err(TopicPatternError::InvalidCharacters(invalid_chars));
        }

        Ok(())
    }

    /// Resolve a topic name from the pattern using metadata.
    ///
    /// Applies normalization to database identifiers if configured.
    ///
    /// # Example
    ///
    /// ```rust
    /// use rivven_connect::topic_resolver::{TopicResolver, TopicMetadata};
    ///
    /// let resolver = TopicResolver::new("cdc.{schema}.{table}").unwrap();
    /// let metadata = TopicMetadata::new("mydb", "public", "users");
    ///
    /// assert_eq!(resolver.resolve(&metadata), "cdc.public.users");
    /// ```
    pub fn resolve(&self, metadata: &TopicMetadata<'_>) -> String {
        if !self.has_placeholders {
            return self.normalization.normalize_topic_name(&self.pattern);
        }

        let mut result = self.pattern.clone();

        // Use "_unknown_" instead of empty strings for
        // unresolvable placeholders to avoid malformed topic names
        // like "cdc..users".
        let resolve_value = |value: &str| -> String {
            if value.is_empty() {
                tracing::warn!(
                    "Topic placeholder resolved to empty string, using '_unknown_' fallback"
                );
                "_unknown_".to_string()
            } else {
                self.normalization.normalize_identifier(value)
            }
        };

        // Normalize and substitute placeholders
        if self.placeholders.contains("database") {
            let normalized = resolve_value(metadata.database);
            result = result.replace("{database}", &normalized);
        }
        if self.placeholders.contains("schema") {
            let normalized = resolve_value(metadata.schema);
            result = result.replace("{schema}", &normalized);
        }
        if self.placeholders.contains("table") {
            let normalized = resolve_value(metadata.table);
            result = result.replace("{table}", &normalized);
        }

        // Apply final topic name normalization (length truncation, etc.)
        self.normalization.normalize_topic_name(&result)
    }

    /// Resolve using individual parameters (convenience method).
    pub fn resolve_parts(&self, database: &str, schema: &str, table: &str) -> String {
        self.resolve(&TopicMetadata::new(database, schema, table))
    }

    /// Get the original pattern.
    pub fn pattern(&self) -> &str {
        &self.pattern
    }

    /// Check if the pattern contains placeholders.
    pub fn has_placeholders(&self) -> bool {
        self.has_placeholders
    }

    /// Get the placeholders used in the pattern.
    pub fn placeholders(&self) -> &HashSet<String> {
        &self.placeholders
    }

    /// Check if a specific placeholder is used.
    pub fn uses_placeholder(&self, name: &str) -> bool {
        self.placeholders.contains(name)
    }

    /// Get the normalization configuration.
    pub fn normalization(&self) -> &NormalizationConfig {
        &self.normalization
    }

    /// Check if normalization is enabled.
    pub fn is_normalization_enabled(&self) -> bool {
        self.normalization.enabled
    }
}

/// Builder for TopicResolver with fluent configuration.
///
/// Provides a convenient API for configuring topic resolution with
/// normalization options.
///
/// ## Example
///
/// ```rust
/// use rivven_connect::topic_resolver::{TopicResolverBuilder, CaseConversion};
///
/// let resolver = TopicResolverBuilder::new("cdc.{schema}.{table}")
///     .case_conversion(CaseConversion::SnakeCase)
///     .avro_compatible()
///     .strip_prefixes(vec!["dbo_", "public_"])
///     .build()
///     .unwrap();
/// ```
#[derive(Debug, Clone)]
pub struct TopicResolverBuilder {
    pattern: String,
    normalization: NormalizationConfig,
}

impl TopicResolverBuilder {
    /// Create a new builder with a pattern.
    pub fn new(pattern: impl Into<String>) -> Self {
        Self {
            pattern: pattern.into(),
            normalization: NormalizationConfig::default(),
        }
    }

    /// Enable lowercase normalization (convenience method).
    ///
    /// Equivalent to `case_conversion(CaseConversion::Lower)`.
    pub fn lowercase(mut self) -> Self {
        self.normalization.case_conversion = CaseConversion::Lower;
        self
    }

    /// Set case conversion strategy.
    pub fn case_conversion(mut self, conversion: CaseConversion) -> Self {
        self.normalization.case_conversion = conversion;
        self
    }

    /// Enable snake_case conversion (convenience method).
    pub fn snake_case(mut self) -> Self {
        self.normalization.case_conversion = CaseConversion::SnakeCase;
        self
    }

    /// Enable kebab-case conversion (convenience method).
    pub fn kebab_case(mut self) -> Self {
        self.normalization.case_conversion = CaseConversion::KebabCase;
        self
    }

    /// Disable normalization entirely.
    pub fn no_normalization(mut self) -> Self {
        self.normalization.enabled = false;
        self
    }

    /// Set custom replacement character.
    pub fn replacement_char(mut self, c: char) -> Self {
        self.normalization.replacement_char = c;
        self
    }

    /// Disable truncation of long topic names.
    pub fn no_truncation(mut self) -> Self {
        self.normalization.truncate_long_names = false;
        self
    }

    /// Enable Avro-compatible naming.
    ///
    /// Ensures topic names are valid Avro identifiers for use with Schema Registry.
    pub fn avro_compatible(mut self) -> Self {
        self.normalization = self.normalization.with_avro_compatible();
        self
    }

    /// Set prefixes to strip from identifiers.
    ///
    /// Accepts string slices for convenience.
    pub fn strip_prefixes<S: AsRef<str>>(mut self, prefixes: Vec<S>) -> Self {
        self.normalization.strip_prefixes =
            prefixes.iter().map(|s| s.as_ref().to_string()).collect();
        self
    }

    /// Set suffixes to strip from identifiers.
    pub fn strip_suffixes<S: AsRef<str>>(mut self, suffixes: Vec<S>) -> Self {
        self.normalization.strip_suffixes =
            suffixes.iter().map(|s| s.as_ref().to_string()).collect();
        self
    }

    /// Set the complete normalization config.
    pub fn normalization(mut self, config: NormalizationConfig) -> Self {
        self.normalization = config;
        self
    }

    /// Build the TopicResolver.
    pub fn build(self) -> Result<TopicResolver, TopicPatternError> {
        TopicResolver::with_normalization(self.pattern, self.normalization)
    }
}

/// Helper function to validate a topic routing pattern without creating a resolver.
pub fn validate_topic_routing(pattern: &str) -> Result<(), TopicPatternError> {
    TopicResolver::validate_pattern(pattern)
}

/// Get list of valid placeholder names.
pub fn valid_placeholders() -> &'static [&'static str] {
    VALID_PLACEHOLDERS
}

/// Get the maximum topic name length.
pub fn max_topic_length() -> usize {
    MAX_TOPIC_LENGTH
}

#[cfg(test)]
mod tests {
    use super::*;

    // ============================================================================
    // TopicResolver Creation Tests
    // ============================================================================

    #[test]
    fn test_resolver_simple_pattern() {
        let resolver = TopicResolver::new("cdc.events").unwrap();
        assert_eq!(resolver.pattern(), "cdc.events");
        assert!(!resolver.has_placeholders());
    }

    #[test]
    fn test_resolver_with_table_placeholder() {
        let resolver = TopicResolver::new("cdc.{table}").unwrap();
        assert!(resolver.has_placeholders());
        assert!(resolver.uses_placeholder("table"));
        assert!(!resolver.uses_placeholder("schema"));
    }

    #[test]
    fn test_resolver_with_all_placeholders() {
        let resolver = TopicResolver::new("{database}.{schema}.{table}").unwrap();
        assert!(resolver.uses_placeholder("database"));
        assert!(resolver.uses_placeholder("schema"));
        assert!(resolver.uses_placeholder("table"));
        assert_eq!(resolver.placeholders().len(), 3);
    }

    #[test]
    fn test_resolver_mixed_pattern() {
        let resolver = TopicResolver::new("cdc-{schema}-{table}-events").unwrap();
        assert!(resolver.has_placeholders());
        assert!(resolver.uses_placeholder("schema"));
        assert!(resolver.uses_placeholder("table"));
        assert!(!resolver.uses_placeholder("database"));
    }

    // ============================================================================
    // Pattern Validation Tests
    // ============================================================================

    #[test]
    fn test_validate_empty_pattern() {
        let result = TopicResolver::new("");
        assert!(matches!(result, Err(TopicPatternError::EmptyPattern)));
    }

    #[test]
    fn test_validate_invalid_placeholder() {
        let result = TopicResolver::new("cdc.{invalid}");
        assert!(matches!(
            result,
            Err(TopicPatternError::InvalidPlaceholder(ref p)) if p == "invalid"
        ));
    }

    #[test]
    fn test_validate_unclosed_brace() {
        let result = TopicResolver::new("cdc.{table");
        assert!(matches!(result, Err(TopicPatternError::UnclosedBrace)));
    }

    #[test]
    fn test_validate_invalid_characters() {
        let result = TopicResolver::new("cdc/{table}");
        assert!(matches!(
            result,
            Err(TopicPatternError::InvalidCharacters(_))
        ));
    }

    #[test]
    fn test_validate_space_in_pattern() {
        let result = TopicResolver::new("cdc {table}");
        assert!(matches!(
            result,
            Err(TopicPatternError::InvalidCharacters(_))
        ));
    }

    #[test]
    fn test_validate_valid_characters() {
        // All these should be valid
        assert!(TopicResolver::new("cdc.events").is_ok());
        assert!(TopicResolver::new("cdc-events").is_ok());
        assert!(TopicResolver::new("cdc_events").is_ok());
        assert!(TopicResolver::new("CDC.Events").is_ok());
        assert!(TopicResolver::new("topic123").is_ok());
    }

    // ============================================================================
    // Topic Resolution Tests
    // ============================================================================

    #[test]
    fn test_resolve_simple_pattern() {
        let resolver = TopicResolver::new("cdc.events").unwrap();
        let metadata = TopicMetadata::new("mydb", "public", "users");
        assert_eq!(resolver.resolve(&metadata), "cdc.events");
    }

    #[test]
    fn test_resolve_table_only() {
        let resolver = TopicResolver::new("cdc.{table}").unwrap();
        let metadata = TopicMetadata::new("mydb", "public", "users");
        assert_eq!(resolver.resolve(&metadata), "cdc.users");
    }

    #[test]
    fn test_resolve_schema_and_table() {
        let resolver = TopicResolver::new("cdc.{schema}.{table}").unwrap();
        let metadata = TopicMetadata::new("mydb", "public", "users");
        assert_eq!(resolver.resolve(&metadata), "cdc.public.users");
    }

    #[test]
    fn test_resolve_all_placeholders() {
        let resolver = TopicResolver::new("{database}.{schema}.{table}").unwrap();
        let metadata = TopicMetadata::new("mydb", "public", "users");
        assert_eq!(resolver.resolve(&metadata), "mydb.public.users");
    }

    #[test]
    fn test_resolve_standard_cdc_style() {
        // Standard CDC style: database.schema.table
        let resolver = TopicResolver::new("{database}.{schema}.{table}").unwrap();
        let metadata = TopicMetadata::new("inventory", "public", "orders");
        assert_eq!(resolver.resolve(&metadata), "inventory.public.orders");
    }

    #[test]
    fn test_resolve_with_prefix_suffix() {
        let resolver = TopicResolver::new("cdc-{schema}-{table}-events").unwrap();
        let metadata = TopicMetadata::new("db", "sales", "orders");
        assert_eq!(resolver.resolve(&metadata), "cdc-sales-orders-events");
    }

    #[test]
    fn test_resolve_multiple_same_placeholder() {
        let resolver = TopicResolver::new("{table}.{table}").unwrap();
        let metadata = TopicMetadata::new("db", "schema", "users");
        assert_eq!(resolver.resolve(&metadata), "users.users");
    }

    #[test]
    fn test_resolve_parts_convenience() {
        let resolver = TopicResolver::new("cdc.{schema}.{table}").unwrap();
        assert_eq!(
            resolver.resolve_parts("mydb", "public", "users"),
            "cdc.public.users"
        );
    }

    // ============================================================================
    // Real-World Scenario Tests
    // ============================================================================

    #[test]
    fn test_resolve_postgres_typical() {
        // Typical PostgreSQL CDC setup
        let resolver = TopicResolver::new("cdc.{schema}.{table}").unwrap();

        // public schema tables
        let metadata = TopicMetadata::new("inventory", "public", "products");
        assert_eq!(resolver.resolve(&metadata), "cdc.public.products");

        // non-public schema
        let metadata = TopicMetadata::new("inventory", "sales", "orders");
        assert_eq!(resolver.resolve(&metadata), "cdc.sales.orders");
    }

    #[test]
    fn test_resolve_mysql_typical() {
        // MySQL doesn't have schema like PostgreSQL, uses database name in schema field
        let resolver = TopicResolver::new("cdc.{schema}.{table}").unwrap();

        let metadata = TopicMetadata::new("mysql_server", "inventory", "customers");
        assert_eq!(resolver.resolve(&metadata), "cdc.inventory.customers");
    }

    #[test]
    fn test_resolve_multi_tenant() {
        // Multi-tenant setup where database is part of topic
        let resolver = TopicResolver::new("tenant-{database}.{table}").unwrap();

        let metadata = TopicMetadata::new("tenant_acme", "public", "users");
        assert_eq!(resolver.resolve(&metadata), "tenant-tenant_acme.users");
    }

    #[test]
    fn test_resolve_with_special_names() {
        // Tables with underscores and numbers
        let resolver = TopicResolver::new("cdc.{schema}.{table}").unwrap();

        let metadata = TopicMetadata::new("db", "public", "user_profiles_v2");
        assert_eq!(resolver.resolve(&metadata), "cdc.public.user_profiles_v2");
    }

    // ============================================================================
    // Helper Function Tests
    // ============================================================================

    #[test]
    fn test_validate_topic_routing_function() {
        assert!(validate_topic_routing("cdc.{table}").is_ok());
        assert!(validate_topic_routing("").is_err());
        assert!(validate_topic_routing("cdc.{invalid}").is_err());
    }

    #[test]
    fn test_valid_placeholders() {
        let placeholders = valid_placeholders();
        assert!(placeholders.contains(&"database"));
        assert!(placeholders.contains(&"schema"));
        assert!(placeholders.contains(&"table"));
        assert_eq!(placeholders.len(), 3);
    }

    // ============================================================================
    // Error Display Tests
    // ============================================================================

    #[test]
    fn test_error_display_empty() {
        let err = TopicPatternError::EmptyPattern;
        assert!(err.to_string().contains("empty"));
    }

    #[test]
    fn test_error_display_invalid_placeholder() {
        let err = TopicPatternError::InvalidPlaceholder("foo".to_string());
        let msg = err.to_string();
        assert!(msg.contains("foo"));
        assert!(msg.contains("database"));
        assert!(msg.contains("schema"));
        assert!(msg.contains("table"));
    }

    #[test]
    fn test_error_display_unclosed() {
        let err = TopicPatternError::UnclosedBrace;
        assert!(err.to_string().contains("unclosed"));
    }

    #[test]
    fn test_error_display_invalid_chars() {
        let err = TopicPatternError::InvalidCharacters("/ @".to_string());
        assert!(err.to_string().contains("/ @"));
    }

    // ============================================================================
    // Clone and Debug Tests
    // ============================================================================

    #[test]
    fn test_resolver_clone() {
        let resolver = TopicResolver::new("cdc.{table}").unwrap();
        let cloned = resolver.clone();
        assert_eq!(resolver.pattern(), cloned.pattern());
    }

    #[test]
    fn test_resolver_debug() {
        let resolver = TopicResolver::new("cdc.{table}").unwrap();
        let debug_str = format!("{:?}", resolver);
        assert!(debug_str.contains("TopicResolver"));
        assert!(debug_str.contains("cdc.{table}"));
    }

    #[test]
    fn test_metadata_debug() {
        let metadata = TopicMetadata::new("db", "schema", "table");
        let debug_str = format!("{:?}", metadata);
        assert!(debug_str.contains("TopicMetadata"));
    }

    // ============================================================================
    // Normalization Configuration Tests
    // ============================================================================

    #[test]
    fn test_normalization_config_default() {
        let config = NormalizationConfig::default();
        assert!(config.enabled);
        assert_eq!(config.case_conversion, CaseConversion::None);
        assert_eq!(config.replacement_char, '_');
        assert!(config.truncate_long_names);
        assert!(config.hash_on_truncate);
        assert!(!config.avro_compatible);
        assert!(config.strip_prefixes.is_empty());
        assert!(config.strip_suffixes.is_empty());
    }

    #[test]
    fn test_normalization_config_disabled() {
        let config = NormalizationConfig::disabled();
        assert!(!config.enabled);
    }

    #[test]
    fn test_normalization_config_builder() {
        let config = NormalizationConfig::new()
            .with_lowercase()
            .with_replacement_char('-')
            .without_truncation();

        assert!(config.enabled);
        assert_eq!(config.case_conversion, CaseConversion::Lower);
        assert_eq!(config.replacement_char, '-');
        assert!(!config.truncate_long_names);
    }

    #[test]
    fn test_normalization_config_avro_compatible() {
        let config = NormalizationConfig::new().with_avro_compatible();
        assert!(config.avro_compatible);
        assert_eq!(config.replacement_char, '_'); // Avro doesn't allow hyphens
    }

    #[test]
    fn test_normalization_config_avro_overrides_hyphen_replacement() {
        // If user set hyphen as replacement, Avro mode should override it
        let config = NormalizationConfig::new()
            .with_replacement_char('-')
            .with_avro_compatible();
        assert_eq!(config.replacement_char, '_');
    }

    #[test]
    fn test_normalization_config_strip_prefixes() {
        let config = NormalizationConfig::new()
            .with_strip_prefixes(vec!["dbo_".to_string(), "public_".to_string()]);
        assert_eq!(config.strip_prefixes.len(), 2);
    }

    // ============================================================================
    // Case Conversion Tests
    // ============================================================================

    #[test]
    fn test_case_conversion_none() {
        let conv = CaseConversion::None;
        assert_eq!(conv.apply("MyTable"), "MyTable");
        assert_eq!(conv.apply("my_table"), "my_table");
        assert_eq!(conv.apply("MY-TABLE"), "MY-TABLE");
    }

    #[test]
    fn test_case_conversion_lower() {
        let conv = CaseConversion::Lower;
        assert_eq!(conv.apply("MyTable"), "mytable");
        assert_eq!(conv.apply("MY_TABLE"), "my_table");
        assert_eq!(conv.apply("already_lower"), "already_lower");
    }

    #[test]
    fn test_case_conversion_upper() {
        let conv = CaseConversion::Upper;
        assert_eq!(conv.apply("MyTable"), "MYTABLE");
        assert_eq!(conv.apply("my_table"), "MY_TABLE");
        assert_eq!(conv.apply("ALREADY_UPPER"), "ALREADY_UPPER");
    }

    #[test]
    fn test_case_conversion_snake_case_from_pascal() {
        let conv = CaseConversion::SnakeCase;
        assert_eq!(conv.apply("MyTable"), "my_table");
        assert_eq!(conv.apply("UserProfiles"), "user_profiles");
        assert_eq!(conv.apply("HTTPRequest"), "httprequest"); // Consecutive caps
        assert_eq!(conv.apply("XMLParser"), "xmlparser");
    }

    #[test]
    fn test_case_conversion_snake_case_from_camel() {
        let conv = CaseConversion::SnakeCase;
        assert_eq!(conv.apply("myTable"), "my_table");
        assert_eq!(conv.apply("userProfiles"), "user_profiles");
        assert_eq!(conv.apply("getHTTPResponse"), "get_httpresponse");
    }

    #[test]
    fn test_case_conversion_snake_case_from_kebab() {
        let conv = CaseConversion::SnakeCase;
        assert_eq!(conv.apply("my-table"), "my_table");
        assert_eq!(conv.apply("user-profiles"), "user_profiles");
    }

    #[test]
    fn test_case_conversion_snake_case_from_spaces() {
        let conv = CaseConversion::SnakeCase;
        assert_eq!(conv.apply("my table"), "my_table");
        assert_eq!(conv.apply("user profiles"), "user_profiles");
    }

    #[test]
    fn test_case_conversion_snake_case_mixed() {
        let conv = CaseConversion::SnakeCase;
        assert_eq!(conv.apply("My-Table_Name"), "my_table_name");
        assert_eq!(conv.apply("User Profile Data"), "user_profile_data");
    }

    #[test]
    fn test_case_conversion_kebab_case() {
        let conv = CaseConversion::KebabCase;
        assert_eq!(conv.apply("MyTable"), "my-table");
        assert_eq!(conv.apply("UserProfiles"), "user-profiles");
        assert_eq!(conv.apply("my_table"), "my-table");
        assert_eq!(conv.apply("my table"), "my-table");
    }

    #[test]
    fn test_case_conversion_default() {
        let conv = CaseConversion::default();
        assert_eq!(conv, CaseConversion::None);
    }

    // ============================================================================
    // Identifier Normalization Tests
    // ============================================================================

    #[test]
    fn test_normalize_identifier_no_change_needed() {
        let config = NormalizationConfig::default();

        // Already valid identifiers should not change
        assert_eq!(config.normalize_identifier("users"), "users");
        assert_eq!(
            config.normalize_identifier("user_profiles"),
            "user_profiles"
        );
        assert_eq!(config.normalize_identifier("orders123"), "orders123");
    }

    #[test]
    fn test_normalize_identifier_special_chars() {
        let config = NormalizationConfig::default();

        // Special characters should be replaced with underscore
        assert_eq!(config.normalize_identifier("user@data"), "user_data");
        assert_eq!(config.normalize_identifier("order#items"), "order_items");
        assert_eq!(config.normalize_identifier("table name"), "table_name");
        assert_eq!(config.normalize_identifier("my.schema"), "my_schema");
    }

    #[test]
    fn test_normalize_identifier_dots() {
        let config = NormalizationConfig::default();

        // Dots in identifiers should be replaced (to avoid creating extra topic segments)
        assert_eq!(
            config.normalize_identifier("schema.with.dots"),
            "schema_with_dots"
        );
    }

    #[test]
    fn test_normalize_identifier_hyphen_preserved() {
        let config = NormalizationConfig::default();

        // Hyphens are valid in topic names, so preserve them (unless Avro mode)
        assert_eq!(config.normalize_identifier("my-table"), "my-table");
    }

    #[test]
    fn test_normalize_identifier_lowercase() {
        let config = NormalizationConfig::new().with_lowercase();

        assert_eq!(config.normalize_identifier("MyTable"), "mytable");
        assert_eq!(config.normalize_identifier("USER_DATA"), "user_data");
        assert_eq!(config.normalize_identifier("Orders123"), "orders123");
    }

    #[test]
    fn test_normalize_identifier_snake_case() {
        let config = NormalizationConfig::new().with_case_conversion(CaseConversion::SnakeCase);

        assert_eq!(config.normalize_identifier("MyTable"), "my_table");
        assert_eq!(config.normalize_identifier("UserProfiles"), "user_profiles");
        assert_eq!(config.normalize_identifier("myTable"), "my_table");
    }

    #[test]
    fn test_normalize_identifier_custom_replacement() {
        let config = NormalizationConfig::new().with_replacement_char('-');

        assert_eq!(config.normalize_identifier("user@data"), "user-data");
        assert_eq!(config.normalize_identifier("my.schema"), "my-schema");
    }

    #[test]
    fn test_normalize_identifier_empty() {
        let config = NormalizationConfig::default();

        // Empty string should return empty (normalization is no-op for empty)
        assert_eq!(config.normalize_identifier(""), "");
    }

    #[test]
    fn test_normalize_identifier_disabled() {
        let config = NormalizationConfig::disabled();

        // Should return original string when disabled
        assert_eq!(config.normalize_identifier("user@data"), "user@data");
        assert_eq!(config.normalize_identifier("My Table"), "My Table");
    }

    // ============================================================================
    // Avro Compatibility Tests
    // ============================================================================

    #[test]
    fn test_normalize_identifier_avro_basic() {
        let config = NormalizationConfig::new().with_avro_compatible();

        // Basic valid names should pass through
        assert_eq!(config.normalize_identifier("users"), "users");
        assert_eq!(
            config.normalize_identifier("user_profiles"),
            "user_profiles"
        );
    }

    #[test]
    fn test_normalize_identifier_avro_hyphens() {
        let config = NormalizationConfig::new().with_avro_compatible();

        // Hyphens are not valid in Avro names
        assert_eq!(config.normalize_identifier("my-table"), "my_table");
        assert_eq!(config.normalize_identifier("user-data"), "user_data");
    }

    #[test]
    fn test_normalize_identifier_avro_leading_digit() {
        let config = NormalizationConfig::new().with_avro_compatible();

        // Names starting with digits need underscore prefix
        assert_eq!(config.normalize_identifier("123table"), "_123table");
        assert_eq!(config.normalize_identifier("1users"), "_1users");
    }

    #[test]
    fn test_normalize_identifier_avro_special_chars() {
        let config = NormalizationConfig::new().with_avro_compatible();

        // All special chars become underscores
        assert_eq!(config.normalize_identifier("user@data"), "user_data");
        assert_eq!(config.normalize_identifier("my.table"), "my_table");
        assert_eq!(config.normalize_identifier("user data"), "user_data");
    }

    // ============================================================================
    // Prefix/Suffix Stripping Tests
    // ============================================================================

    #[test]
    fn test_normalize_identifier_strip_prefix() {
        let config = NormalizationConfig::new().with_strip_prefixes(vec!["dbo_".to_string()]);

        assert_eq!(config.normalize_identifier("dbo_users"), "users");
        assert_eq!(
            config.normalize_identifier("dbo_user_profiles"),
            "user_profiles"
        );
        // Should not strip if not matching
        assert_eq!(config.normalize_identifier("users"), "users");
    }

    #[test]
    fn test_normalize_identifier_strip_prefix_case_insensitive() {
        let config = NormalizationConfig::new().with_strip_prefixes(vec!["DBO_".to_string()]);

        // Prefix matching should be case-insensitive
        assert_eq!(config.normalize_identifier("dbo_users"), "users");
        assert_eq!(config.normalize_identifier("DBO_USERS"), "USERS");
    }

    #[test]
    fn test_normalize_identifier_strip_suffix() {
        let config = NormalizationConfig::new().with_strip_suffixes(vec!["_table".to_string()]);

        assert_eq!(config.normalize_identifier("users_table"), "users");
        assert_eq!(config.normalize_identifier("orders_table"), "orders");
    }

    #[test]
    fn test_normalize_identifier_strip_multiple_prefixes() {
        let config = NormalizationConfig::new()
            .with_strip_prefixes(vec!["dbo_".to_string(), "public_".to_string()]);

        assert_eq!(config.normalize_identifier("dbo_users"), "users");
        assert_eq!(config.normalize_identifier("public_orders"), "orders");
    }

    #[test]
    fn test_normalize_identifier_strip_with_case_conversion() {
        let config = NormalizationConfig::new()
            .with_strip_prefixes(vec!["dbo_".to_string()])
            .with_case_conversion(CaseConversion::SnakeCase);

        assert_eq!(config.normalize_identifier("dbo_MyTable"), "my_table");
        assert_eq!(
            config.normalize_identifier("dbo_UserProfiles"),
            "user_profiles"
        );
    }

    // ============================================================================
    // Builder Tests
    // ============================================================================

    #[test]
    fn test_builder_basic() {
        let resolver = TopicResolver::builder("cdc.{table}").build().unwrap();

        assert_eq!(resolver.pattern(), "cdc.{table}");
        assert!(resolver.is_normalization_enabled());
    }

    #[test]
    fn test_builder_with_all_options() {
        let resolver = TopicResolver::builder("cdc.{table}")
            .lowercase()
            .replacement_char('-')
            .no_truncation()
            .build()
            .unwrap();

        assert_eq!(
            resolver.normalization().case_conversion,
            CaseConversion::Lower
        );
        assert_eq!(resolver.normalization().replacement_char, '-');
        assert!(!resolver.normalization().truncate_long_names);
    }

    #[test]
    fn test_builder_snake_case() {
        let resolver = TopicResolver::builder("cdc.{table}")
            .snake_case()
            .build()
            .unwrap();

        assert_eq!(
            resolver.normalization().case_conversion,
            CaseConversion::SnakeCase
        );
    }

    #[test]
    fn test_builder_kebab_case() {
        let resolver = TopicResolver::builder("cdc.{table}")
            .kebab_case()
            .build()
            .unwrap();

        assert_eq!(
            resolver.normalization().case_conversion,
            CaseConversion::KebabCase
        );
    }

    #[test]
    fn test_builder_avro_compatible() {
        let resolver = TopicResolver::builder("cdc.{table}")
            .avro_compatible()
            .build()
            .unwrap();

        assert!(resolver.normalization().avro_compatible);
    }

    #[test]
    fn test_builder_strip_prefixes() {
        let resolver = TopicResolver::builder("cdc.{table}")
            .strip_prefixes(vec!["dbo_", "public_"])
            .build()
            .unwrap();

        assert_eq!(resolver.normalization().strip_prefixes.len(), 2);
    }

    #[test]
    fn test_builder_invalid_pattern() {
        let result = TopicResolver::builder("cdc.{invalid}").build();

        assert!(result.is_err());
    }

    // ============================================================================
    // Edge Cases and Real-World Scenarios
    // ============================================================================

    #[test]
    fn test_postgres_quoted_identifiers() {
        // PostgreSQL allows quoted identifiers with spaces and special chars
        let resolver = TopicResolver::new("cdc.{schema}.{table}").unwrap();

        let metadata = TopicMetadata::new("mydb", "my schema", "user data");
        assert_eq!(resolver.resolve(&metadata), "cdc.my_schema.user_data");
    }

    #[test]
    fn test_mysql_backtick_escaped_names() {
        // MySQL uses backticks for identifiers with special chars
        let resolver = TopicResolver::new("cdc.{database}.{table}").unwrap();

        let metadata = TopicMetadata::new("my-database", "schema", "order items");
        assert_eq!(resolver.resolve(&metadata), "cdc.my-database.order_items");
    }

    #[test]
    fn test_unicode_characters() {
        let resolver = TopicResolver::new("cdc.{schema}.{table}").unwrap();

        // Unicode characters should be replaced
        let metadata = TopicMetadata::new("db", "schéma", "tàblé");
        let result = resolver.resolve(&metadata);
        // Non-ASCII chars replaced with underscore
        assert!(!result.contains('é'));
        assert!(!result.contains('à'));
    }

    #[test]
    fn test_very_long_identifiers() {
        let resolver = TopicResolver::new("{database}.{schema}.{table}").unwrap();

        // Very long identifiers that would exceed topic length
        let long_db = "a".repeat(100);
        let long_schema = "b".repeat(100);
        let long_table = "c".repeat(100);

        let metadata = TopicMetadata::new(&long_db, &long_schema, &long_table);
        let result = resolver.resolve(&metadata);

        // Should be truncated to max length
        assert!(result.len() <= MAX_TOPIC_LENGTH);
    }

    #[test]
    fn test_hash_uniqueness_on_truncation() {
        let resolver = TopicResolver::new("{database}.{schema}.{table}").unwrap();

        // Two different long names should produce different truncated results
        let db1 = "a".repeat(100);
        let schema1 = "b".repeat(100);
        let table1 = "c".repeat(100);
        let metadata1 = TopicMetadata::new(&db1, &schema1, &table1);

        let db2 = "x".repeat(100);
        let schema2 = "y".repeat(100);
        let table2 = "z".repeat(100);
        let metadata2 = TopicMetadata::new(&db2, &schema2, &table2);

        let result1 = resolver.resolve(&metadata1);
        let result2 = resolver.resolve(&metadata2);

        // Both should be max length but different due to hash
        assert_eq!(result1.len(), MAX_TOPIC_LENGTH);
        assert_eq!(result2.len(), MAX_TOPIC_LENGTH);
        assert_ne!(result1, result2);
    }

    #[test]
    fn test_max_topic_length_constant() {
        assert_eq!(max_topic_length(), 249);
    }
}
