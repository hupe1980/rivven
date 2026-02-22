//! Security utilities for SQL injection prevention in rivven-rdbc.
//!
//! Provides:
//! - Identifier validation for savepoints, table names, schema names
//! - String literal escaping for SQL string contexts
//!
//! These functions are used by all RDBC backends (PostgreSQL, MySQL, SQL Server)
//! and the dialect abstraction layer to prevent SQL injection attacks.

use crate::error::Error;

/// Validate a SQL identifier (savepoint, table, schema names).
///
/// Prevents SQL injection by enforcing strict character rules:
/// - Must not be empty
/// - Maximum 255 characters
/// - Must start with ASCII letter or underscore
/// - May only contain ASCII alphanumeric characters and underscores
///
/// This matches the `IDENTIFIER_REGEX` pattern from `rivven-core::validation`
/// (`^[a-zA-Z_][a-zA-Z0-9_]{0,254}$`) but uses char-iteration instead
/// of regex for zero-dependency, zero-allocation validation on the hot path.
///
/// # Examples
///
/// ```
/// use rivven_rdbc::security::validate_sql_identifier;
///
/// assert!(validate_sql_identifier("users").is_ok());
/// assert!(validate_sql_identifier("my_table_123").is_ok());
/// assert!(validate_sql_identifier("_private").is_ok());
///
/// // Rejects injection attempts
/// assert!(validate_sql_identifier("x; DROP TABLE users--").is_err());
/// assert!(validate_sql_identifier("").is_err());
/// assert!(validate_sql_identifier("123abc").is_err());
/// ```
pub fn validate_sql_identifier(name: &str) -> crate::Result<()> {
    if name.is_empty() {
        return Err(Error::config("SQL identifier cannot be empty"));
    }

    if name.len() > 255 {
        return Err(Error::config(format!(
            "SQL identifier too long: {} chars (max 255)",
            name.len()
        )));
    }

    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => {
            return Err(Error::config(format!(
                "Invalid SQL identifier '{}': must start with a letter or underscore",
                name
            )));
        }
    }

    for c in chars {
        if !c.is_ascii_alphanumeric() && c != '_' {
            return Err(Error::config(format!(
                "Invalid SQL identifier '{}': contains invalid character '{}'",
                name, c
            )));
        }
    }

    Ok(())
}

/// Escape a string value for safe interpolation into a SQL string literal context.
///
/// Replaces `'` with `''` (standard SQL escaping for single-quoted string literals).
/// This is used for `information_schema` queries where parameterized queries
/// are not practical (e.g., `table_exists_sql`, `list_columns_sql`) because
/// the SQL must be returned as a pre-built string.
///
/// **Prefer parameterized queries whenever possible.** This function is a fallback
/// for cases where the SQL generation API requires a complete SQL string.
///
/// # Examples
///
/// ```
/// use rivven_rdbc::security::escape_string_literal;
///
/// assert_eq!(escape_string_literal("users"), "users");
/// assert_eq!(escape_string_literal("don't"), "don''t");
/// assert_eq!(escape_string_literal("x'; DROP TABLE users--"), "x''; DROP TABLE users--");
/// ```
pub fn escape_string_literal(value: &str) -> String {
    // Fast path: no escaping needed (common case)
    if !value.contains('\'') {
        return value.to_string();
    }
    value.replace('\'', "''")
}

/// Validate a SQL type name for safe interpolation into DDL statements.
///
/// Prevents SQL injection via `ColumnMetadata::type_name` by allowing only
/// characters that appear in legitimate SQL type specifications:
/// - ASCII letters, digits, underscores: `VARCHAR`, `INT`, `BIGINT`
/// - Parentheses and commas: `DECIMAL(10,2)`, `ENUM('a','b')`
/// - Spaces: `INT UNSIGNED`, `DOUBLE PRECISION`
/// - Single quotes: `ENUM('x','y')` (MySQL set/enum value lists)
/// - Periods: `NUMERIC(10.2)` (some dialects)
///
/// Rejects semicolons, double-dashes, newlines, backticks, and other
/// metacharacters that could escape the DDL context.
///
/// # Examples
///
/// ```
/// use rivven_rdbc::security::validate_sql_type_name;
///
/// assert!(validate_sql_type_name("INT").is_ok());
/// assert!(validate_sql_type_name("VARCHAR(255)").is_ok());
/// assert!(validate_sql_type_name("DECIMAL(10,2)").is_ok());
/// assert!(validate_sql_type_name("INT UNSIGNED").is_ok());
/// assert!(validate_sql_type_name("ENUM('a','b')").is_ok());
///
/// // Rejects injection attempts
/// assert!(validate_sql_type_name("INT; DROP TABLE users--").is_err());
/// assert!(validate_sql_type_name("").is_err());
/// ```
pub fn validate_sql_type_name(type_name: &str) -> crate::Result<()> {
    if type_name.is_empty() {
        return Err(Error::config("SQL type name cannot be empty"));
    }

    if type_name.len() > 255 {
        return Err(Error::config(format!(
            "SQL type name too long: {} chars (max 255)",
            type_name.len()
        )));
    }

    for c in type_name.chars() {
        if !(c.is_ascii_alphanumeric()
            || c == '_'
            || c == '('
            || c == ')'
            || c == ','
            || c == ' '
            || c == '\''
            || c == '.')
        {
            return Err(Error::config(format!(
                "Invalid SQL type name '{}': contains invalid character '{}'",
                type_name, c
            )));
        }
    }

    Ok(())
}

/// Validate a user-supplied WHERE clause for safe interpolation.
///
/// The clause is injected as a raw SQL fragment (via `Expr::cust()`), so it
/// **cannot** be parameterized. This function applies basic deny-list checks
/// to reject the most common SQL injection patterns:
///
/// - Semicolons (statement terminators / stacking)
/// - Double-dash `--` line comments
/// - C-style `/* */` block comments
/// - Backslash escapes (MySQL-specific injection vector)
///
/// # Security note
///
/// This is a **best-effort** safeguard, **not** a guarantee. A determined
/// attacker can craft payloads that bypass simple deny-list checks. Prefer
/// parameterized queries wherever possible. This clause should only come
/// from **trusted connector configuration**, never from end-user input.
///
/// # Examples
///
/// ```
/// use rivven_rdbc::security::validate_where_clause;
///
/// assert!(validate_where_clause("status = 'active'").is_ok());
/// assert!(validate_where_clause("id > 0 AND deleted = false").is_ok());
///
/// assert!(validate_where_clause("1=1; DROP TABLE users").is_err());
/// assert!(validate_where_clause("1=1 -- bypass").is_err());
/// assert!(validate_where_clause("1=1 /* comment */").is_err());
/// ```
pub fn validate_where_clause(clause: &str) -> crate::Result<()> {
    if clause.is_empty() {
        return Err(Error::config("WHERE clause cannot be empty"));
    }

    if clause.len() > 4096 {
        return Err(Error::config(format!(
            "WHERE clause too long: {} chars (max 4096)",
            clause.len()
        )));
    }

    if clause.contains(';') {
        return Err(Error::config(format!(
            "WHERE clause contains prohibited character ';': {}",
            clause
        )));
    }

    if clause.contains("--") {
        return Err(Error::config(format!(
            "WHERE clause contains prohibited pattern '--': {}",
            clause
        )));
    }

    if clause.contains("/*") || clause.contains("*/") {
        return Err(Error::config(format!(
            "WHERE clause contains prohibited comment syntax: {}",
            clause
        )));
    }

    if clause.contains('\\') {
        return Err(Error::config(format!(
            "WHERE clause contains prohibited backslash escape: {}",
            clause
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // validate_sql_identifier
    // -----------------------------------------------------------------------

    #[test]
    fn test_valid_identifiers() {
        assert!(validate_sql_identifier("users").is_ok());
        assert!(validate_sql_identifier("my_table").is_ok());
        assert!(validate_sql_identifier("_private").is_ok());
        assert!(validate_sql_identifier("a").is_ok());
        assert!(validate_sql_identifier("TABLE_123").is_ok());
        assert!(validate_sql_identifier("sp1").is_ok());
    }

    #[test]
    fn test_empty_identifier() {
        assert!(validate_sql_identifier("").is_err());
    }

    #[test]
    fn test_too_long_identifier() {
        let long = "a".repeat(256);
        assert!(validate_sql_identifier(&long).is_err());

        let max = "a".repeat(255);
        assert!(validate_sql_identifier(&max).is_ok());
    }

    #[test]
    fn test_starts_with_digit() {
        assert!(validate_sql_identifier("123abc").is_err());
        assert!(validate_sql_identifier("0").is_err());
    }

    #[test]
    fn test_injection_attempts() {
        // SQL injection via semicolon
        assert!(validate_sql_identifier("x; DROP TABLE users--").is_err());
        // SQL injection via quote
        assert!(validate_sql_identifier("x' OR '1'='1").is_err());
        // SQL injection via comment
        assert!(validate_sql_identifier("x--").is_err());
        // SQL injection via parentheses
        assert!(validate_sql_identifier("x()").is_err());
        // Unicode smuggling
        assert!(validate_sql_identifier("tabl\u{0435}").is_err()); // Cyrillic е
                                                                   // Whitespace
        assert!(validate_sql_identifier("user name").is_err());
        // Newlines
        assert!(validate_sql_identifier("x\nDROP TABLE").is_err());
        // Null bytes
        assert!(validate_sql_identifier("x\0").is_err());
        // Dots (schema.table injection)
        assert!(validate_sql_identifier("schema.table").is_err());
    }

    #[test]
    fn test_special_chars_rejected() {
        for ch in &[
            '.', '-', '@', '#', '$', '!', '%', '&', '*', '[', ']', '"', '`',
        ] {
            let name = format!("a{}", ch);
            assert!(
                validate_sql_identifier(&name).is_err(),
                "Should reject '{}'",
                name
            );
        }
    }

    // -----------------------------------------------------------------------
    // escape_string_literal
    // -----------------------------------------------------------------------

    #[test]
    fn test_escape_no_quotes() {
        assert_eq!(escape_string_literal("users"), "users");
        assert_eq!(escape_string_literal("my_table"), "my_table");
    }

    #[test]
    fn test_escape_single_quotes() {
        assert_eq!(escape_string_literal("don't"), "don''t");
        assert_eq!(escape_string_literal("'hello'"), "''hello''");
    }

    #[test]
    fn test_escape_injection_attempt() {
        assert_eq!(
            escape_string_literal("x'; DROP TABLE users--"),
            "x''; DROP TABLE users--"
        );
        assert_eq!(escape_string_literal("' OR '1'='1"), "'' OR ''1''=''1");
    }

    #[test]
    fn test_escape_empty_string() {
        assert_eq!(escape_string_literal(""), "");
    }

    // -----------------------------------------------------------------------
    // validate_sql_type_name
    // -----------------------------------------------------------------------

    #[test]
    fn test_valid_type_names() {
        assert!(validate_sql_type_name("INT").is_ok());
        assert!(validate_sql_type_name("BIGINT").is_ok());
        assert!(validate_sql_type_name("VARCHAR(255)").is_ok());
        assert!(validate_sql_type_name("DECIMAL(10,2)").is_ok());
        assert!(validate_sql_type_name("INT UNSIGNED").is_ok());
        assert!(validate_sql_type_name("DOUBLE PRECISION").is_ok());
        assert!(validate_sql_type_name("ENUM('a','b','c')").is_ok());
        assert!(validate_sql_type_name("SET('x','y')").is_ok());
        assert!(validate_sql_type_name("NUMERIC(10.2)").is_ok());
        assert!(validate_sql_type_name("timestamp").is_ok());
        assert!(validate_sql_type_name("TINYINT(1)").is_ok());
    }

    #[test]
    fn test_empty_type_name() {
        assert!(validate_sql_type_name("").is_err());
    }

    #[test]
    fn test_type_name_injection_attempts() {
        assert!(validate_sql_type_name("INT; DROP TABLE users--").is_err());
        assert!(validate_sql_type_name("INT`; DROP TABLE").is_err());
        assert!(validate_sql_type_name("INT\nDROP TABLE").is_err());
        assert!(validate_sql_type_name("INT\0").is_err());
        assert!(validate_sql_type_name("INT--comment").is_err());
    }

    #[test]
    fn test_type_name_too_long() {
        let long = "A".repeat(256);
        assert!(validate_sql_type_name(&long).is_err());
    }
}
