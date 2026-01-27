//! Test data generators for CDC integration tests
//!
//! Provides realistic test data using fake-rs for comprehensive testing.

#![allow(dead_code)] // Some generators are available for future tests

use fake::faker::internet::en::*;
use fake::faker::lorem::en::*;
use fake::faker::name::en::*;
use fake::{Fake, Faker};
use serde_json::{json, Value};
use std::collections::HashMap;

/// Generate a batch of user insert SQL statements for a specific table
pub fn generate_user_inserts_for_table(table: &str, count: usize) -> Vec<String> {
    (0..count)
        .map(|_| {
            let name: String = Name().fake();
            let email: String = SafeEmail().fake();
            let age: u8 = (18..80).fake();
            format!(
                "INSERT INTO {} (name, email, age) VALUES ('{}', '{}', {})",
                table,
                escape_sql(&name),
                escape_sql(&email),
                age
            )
        })
        .collect()
}

/// Generate a batch of user insert SQL statements (for "users" table)
pub fn generate_user_inserts(count: usize) -> Vec<String> {
    generate_user_inserts_for_table("users", count)
}

/// Generate a single user insert with known values
pub fn generate_user_insert(name: &str, email: &str, age: i32) -> String {
    format!(
        "INSERT INTO users (name, email, age) VALUES ('{}', '{}', {})",
        escape_sql(name),
        escape_sql(email),
        age
    )
}

/// Generate user with all fields
pub fn generate_full_user_insert() -> (String, HashMap<String, Value>) {
    let name: String = Name().fake();
    let email: String = SafeEmail().fake();
    let age: i32 = (18..80).fake();
    let balance: f64 = (0.0..100000.0).fake();
    let is_active: bool = Faker.fake();
    let tags: Vec<String> = Words(1..5).fake();
    let metadata = json!({"source": "test", "version": 1});

    let sql = format!(
        r#"INSERT INTO users (name, email, age, balance, is_active, metadata, tags) 
           VALUES ('{}', '{}', {}, {:.2}, {}, '{}', ARRAY[{}])"#,
        escape_sql(&name),
        escape_sql(&email),
        age,
        balance,
        is_active,
        metadata.to_string().replace('\'', "''"),
        tags.iter()
            .map(|t| format!("'{}'", escape_sql(t)))
            .collect::<Vec<_>>()
            .join(", ")
    );

    let mut expected = HashMap::new();
    expected.insert("name".to_string(), json!(name));
    expected.insert("email".to_string(), json!(email));
    expected.insert("age".to_string(), json!(age.to_string()));
    expected.insert("balance".to_string(), json!(format!("{:.2}", balance)));
    expected.insert("is_active".to_string(), json!(is_active.to_string()));

    (sql, expected)
}

/// Generate an update SQL statement
pub fn generate_user_update(id: i32, new_age: i32) -> String {
    format!(
        "UPDATE users SET age = {}, updated_at = NOW() WHERE id = {}",
        new_age, id
    )
}

/// Generate a delete SQL statement
pub fn generate_user_delete(id: i32) -> String {
    format!("DELETE FROM users WHERE id = {}", id)
}

/// Generate a batch of operations for transaction testing
pub fn generate_transaction_batch(table: &str, count: usize) -> Vec<String> {
    let mut statements = vec!["BEGIN".to_string()];

    for _ in 0..count {
        let name: String = Name().fake();
        let email: String = SafeEmail().fake();
        let age: u8 = (18..80).fake();
        statements.push(format!(
            "INSERT INTO {} (name, email, age) VALUES ('{}', '{}', {})",
            table,
            escape_sql(&name),
            escape_sql(&email),
            age
        ));
    }

    statements.push("COMMIT".to_string());
    statements
}

/// Generate test data for all PostgreSQL types
pub fn generate_all_types_insert(table: &str) -> String {
    let uuid = uuid::Uuid::new_v4();

    format!(
        r#"INSERT INTO {} (
            col_smallint, col_integer, col_bigint,
            col_real, col_double, col_numeric,
            col_text, col_varchar, col_char,
            col_bytea, col_boolean,
            col_date, col_time, col_timestamp, col_timestamptz,
            col_json, col_jsonb, col_uuid,
            col_int_array, col_text_array
        ) VALUES (
            32767, 2147483647, 9223372036854775807,
            3.14, 3.141592653589793, 12345.678901,
            'Hello, World!', 'varchar_value', 'char_val  ',
            '\x48656c6c6f', true,
            '2024-01-15', '14:30:00', '2024-01-15 14:30:00', '2024-01-15 14:30:00+00',
            '{{"key": "value"}}', '{{"nested": {{"data": 123}}}}', '{}',
            ARRAY[1, 2, 3], ARRAY['a', 'b', 'c']
        )"#,
        table, uuid
    )
}

/// Generate bulk insert for stress testing
pub fn generate_bulk_inserts(table: &str, count: usize, batch_size: usize) -> Vec<String> {
    let mut batches = Vec::new();

    for batch_start in (0..count).step_by(batch_size) {
        let batch_end = (batch_start + batch_size).min(count);
        let values: Vec<String> = (batch_start..batch_end)
            .map(|_| {
                let name: String = Name().fake();
                let email: String = SafeEmail().fake();
                let age: u8 = (18..80).fake();
                format!(
                    "('{}', '{}', {})",
                    escape_sql(&name),
                    escape_sql(&email),
                    age
                )
            })
            .collect();

        batches.push(format!(
            "INSERT INTO {} (name, email, age) VALUES {}",
            table,
            values.join(", ")
        ));
    }

    batches
}

/// Generate mixed operations for realistic workload testing
pub fn generate_mixed_workload(table: &str, count: usize) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current_id = 1;

    for i in 0..count {
        match i % 10 {
            0..=6 => {
                // 70% inserts
                let name: String = Name().fake();
                let email: String = SafeEmail().fake();
                let age: u8 = (18..80).fake();
                statements.push(format!(
                    "INSERT INTO {} (name, email, age) VALUES ('{}', '{}', {})",
                    table,
                    escape_sql(&name),
                    escape_sql(&email),
                    age
                ));
                current_id += 1;
            }
            7..=8 => {
                // 20% updates
                if current_id > 1 {
                    let id = (1..current_id).fake::<i32>();
                    let new_age: u8 = (18..80).fake();
                    statements.push(format!(
                        "UPDATE {} SET age = {} WHERE id = {}",
                        table, new_age, id
                    ));
                }
            }
            _ => {
                // 10% deletes
                if current_id > 1 {
                    let id = (1..current_id).fake::<i32>();
                    statements.push(format!("DELETE FROM {} WHERE id = {}", table, id));
                }
            }
        }
    }

    statements
}

/// Escape SQL string to prevent injection in test data
fn escape_sql(s: &str) -> String {
    s.replace('\'', "''").replace('\\', "\\\\")
}

/// Test data for specific scenarios
pub mod scenarios {
    use super::*;

    /// Data for testing NULL handling
    pub fn null_handling_inserts(table: &str) -> Vec<String> {
        vec![
            format!("INSERT INTO {} (name) VALUES ('Only Name')", table),
            format!(
                "INSERT INTO {} (name, email) VALUES ('With Email', NULL)",
                table
            ),
            format!("INSERT INTO {} (name, age) VALUES ('With Age', 25)", table),
            format!(
                "INSERT INTO {} (name, email, age) VALUES ('Full Record', 'full@test.com', 30)",
                table
            ),
        ]
    }

    /// Data for testing special characters
    pub fn special_characters_inserts(table: &str) -> Vec<String> {
        vec![
            format!(
                "INSERT INTO {} (name, email) VALUES ('O''Brien', 'obrien@test.com')",
                table
            ),
            format!(
                "INSERT INTO {} (name, email) VALUES ('José García', 'jose@test.com')",
                table
            ),
            format!(
                "INSERT INTO {} (name, email) VALUES ('山田太郎', 'yamada@test.com')",
                table
            ),
            format!(
                "INSERT INTO {} (name, email) VALUES ('Test\\nNewline', 'newline@test.com')",
                table
            ),
            format!(
                "INSERT INTO {} (name, email) VALUES ('Tab\\tCharacter', 'tab@test.com')",
                table
            ),
        ]
    }

    /// Data for testing large values
    pub fn large_values_insert(table: &str) -> String {
        let large_text: String = Paragraphs(10..20).fake::<Vec<String>>().join("\n");
        format!(
            "INSERT INTO {} (name, email) VALUES ('{}', 'large@test.com')",
            table,
            escape_sql(&large_text[..large_text.len().min(1000)])
        )
    }

    /// Generate cascading update scenario
    pub fn cascading_updates(table: &str, id: i32, iterations: usize) -> Vec<String> {
        (0..iterations)
            .map(|i| {
                format!(
                    "UPDATE {} SET age = {}, updated_at = NOW() WHERE id = {}",
                    table,
                    20 + i as i32,
                    id
                )
            })
            .collect()
    }
}
