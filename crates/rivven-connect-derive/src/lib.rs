//! Derive macros for Rivven connector development
//!
//! This crate provides procedural macros that reduce boilerplate when implementing
//! Rivven connectors.
//!
//! # Example
//!
//! ```rust,ignore
//! use rivven_connect_derive::{SourceConfig, SinkConfig, connector_spec};
//!
//! #[derive(Debug, Deserialize, Validate, JsonSchema, SourceConfig)]
//! #[source(
//!     name = "my-source",
//!     version = "1.0.0",
//!     description = "Custom data source",
//!     author = "Rivven Team",
//!     license = "Apache-2.0",
//!     documentation_url = "https://rivven.dev/docs/connectors/my-source"
//! )]
//! pub struct MySourceConfig {
//!     #[validate(url)]
//!     pub endpoint: String,
//!     #[validate(range(min = 1, max = 100))]
//!     pub batch_size: usize,
//! }
//!
//! #[derive(Debug, Deserialize, Validate, JsonSchema, SinkConfig)]
//! #[sink(
//!     name = "my-sink",
//!     version = "1.0.0",
//!     batching,
//!     batch_size = 1000
//! )]
//! pub struct MySinkConfig {
//!     pub bucket: String,
//! }
//! ```

// Allow unused fields in darling attribute structs - they're parsed but not all are used yet
#![allow(dead_code)]

use darling::{FromDeriveInput, FromMeta};
use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

/// Attributes for the Source derive macro
#[derive(Debug, FromDeriveInput)]
#[darling(attributes(source), supports(struct_named))]
struct SourceAttrs {
    ident: syn::Ident,
    /// Connector name (e.g., "postgres-cdc")
    #[darling(default)]
    name: Option<String>,
    /// Connector version (e.g., "1.0.0")
    #[darling(default)]
    version: Option<String>,
    /// Description of the connector
    #[darling(default)]
    description: Option<String>,
    /// Author or maintainer
    #[darling(default)]
    author: Option<String>,
    /// License identifier (e.g., "Apache-2.0")
    #[darling(default)]
    license: Option<String>,
    /// Documentation URL
    #[darling(default)]
    documentation_url: Option<String>,
    /// Whether the source supports incremental sync
    #[darling(default)]
    incremental: bool,
    /// Supported source types (e.g., full_refresh, incremental)
    #[darling(default)]
    source_types: Option<SourceTypesAttr>,
}

#[derive(Debug, Default, FromMeta)]
struct SourceTypesAttr {
    full_refresh: bool,
    incremental: bool,
}

/// Attributes for the Sink derive macro
#[derive(Debug, FromDeriveInput)]
#[darling(attributes(sink), supports(struct_named))]
struct SinkAttrs {
    ident: syn::Ident,
    /// Connector name (e.g., "s3-sink")
    #[darling(default)]
    name: Option<String>,
    /// Connector version (e.g., "1.0.0")
    #[darling(default)]
    version: Option<String>,
    /// Description of the connector
    #[darling(default)]
    description: Option<String>,
    /// Author or maintainer
    #[darling(default)]
    author: Option<String>,
    /// License identifier (e.g., "Apache-2.0")
    #[darling(default)]
    license: Option<String>,
    /// Documentation URL
    #[darling(default)]
    documentation_url: Option<String>,
    /// Whether the sink supports batching
    #[darling(default)]
    batching: bool,
    /// Default batch size
    #[darling(default)]
    batch_size: Option<usize>,
}

/// Attributes for the Transform derive macro
#[derive(Debug, FromDeriveInput)]
#[darling(attributes(transform), supports(struct_named))]
struct TransformAttrs {
    ident: syn::Ident,
    /// Transform name (e.g., "filter-transform")
    #[darling(default)]
    name: Option<String>,
    /// Transform version (e.g., "1.0.0")
    #[darling(default)]
    version: Option<String>,
    /// Description of the transform
    #[darling(default)]
    description: Option<String>,
}

/// Derive macro for implementing SourceConfig boilerplate
///
/// This macro generates a `*Spec` struct with `spec()`, `name()`, and `version()` methods.
/// The generated spec includes config schema derived from the struct's JsonSchema implementation.
///
/// # Attributes
///
/// - `#[source(name = "...")]` - Connector name (defaults to struct name without "Config")
/// - `#[source(version = "...")]` - Connector version (default: env!("CARGO_PKG_VERSION") or "0.0.1")
/// - `#[source(description = "...")]` - Connector description
/// - `#[source(author = "...")]` - Author or maintainer
/// - `#[source(license = "...")]` - License identifier (e.g., "Apache-2.0")
/// - `#[source(documentation_url = "...")]` - Documentation URL
/// - `#[source(incremental)]` - Enable incremental sync support
///
/// # Example
///
/// ```rust,ignore
/// #[derive(Debug, Deserialize, Validate, JsonSchema, SourceConfig)]
/// #[source(
///     name = "postgres-cdc",
///     version = "1.0.0",
///     description = "PostgreSQL CDC connector",
///     author = "Rivven Team",
///     license = "Apache-2.0",
///     incremental
/// )]
/// pub struct PostgresCdcConfig {
///     pub connection_string: String,
///     pub slot_name: String,
/// }
/// ```
#[proc_macro_derive(SourceConfig, attributes(source))]
pub fn derive_source_config(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let attrs = match SourceAttrs::from_derive_input(&input) {
        Ok(v) => v,
        Err(e) => return TokenStream::from(e.write_errors()),
    };

    let struct_name = &attrs.ident;
    let spec_struct_name = quote::format_ident!("{}Spec", struct_name);

    let name = attrs.name.unwrap_or_else(|| {
        let name = struct_name.to_string();
        name.strip_suffix("Config").unwrap_or(&name).to_lowercase()
    });
    let version = attrs.version.unwrap_or_else(|| "0.0.1".to_string());

    let description_code = match &attrs.description {
        Some(desc) => quote! { .description(#desc) },
        None => quote! {},
    };

    let author_code = match &attrs.author {
        Some(author) => quote! { .author(#author) },
        None => quote! {},
    };

    let license_code = match &attrs.license {
        Some(license) => quote! { .license(#license) },
        None => quote! {},
    };

    let doc_url_code = match &attrs.documentation_url {
        Some(url) => quote! { .documentation_url(#url) },
        None => quote! {},
    };

    let incremental_code = if attrs.incremental {
        quote! { .incremental(true) }
    } else {
        quote! {}
    };

    let expanded = quote! {
        /// Auto-generated spec holder for this source configuration
        pub struct #spec_struct_name;

        impl #spec_struct_name {
            /// Returns the connector specification with config schema
            pub fn spec() -> rivven_connect::ConnectorSpec {
                rivven_connect::ConnectorSpec::builder(#name, #version)
                    #description_code
                    #author_code
                    #license_code
                    #doc_url_code
                    #incremental_code
                    .config_schema::<#struct_name>()
                    .build()
            }

            /// Returns the connector name
            pub const fn name() -> &'static str {
                #name
            }

            /// Returns the connector version
            pub const fn version() -> &'static str {
                #version
            }
        }
    };

    TokenStream::from(expanded)
}

/// Derive macro for implementing SinkConfig boilerplate
///
/// This macro generates a `*Spec` struct with `spec()`, `name()`, `version()`, and optionally
/// `batch_config()` methods. The generated spec includes config schema derived from the struct's
/// JsonSchema implementation.
///
/// # Attributes
///
/// - `#[sink(name = "...")]` - Connector name (defaults to struct name without "Config")
/// - `#[sink(version = "...")]` - Connector version (default: "0.0.1")
/// - `#[sink(description = "...")]` - Connector description
/// - `#[sink(author = "...")]` - Author or maintainer
/// - `#[sink(license = "...")]` - License identifier (e.g., "Apache-2.0")
/// - `#[sink(documentation_url = "...")]` - Documentation URL
/// - `#[sink(batching)]` - Enable batching support (generates batch_config() method)
/// - `#[sink(batch_size = N)]` - Default batch size (default: 10000)
///
/// # Example
///
/// ```rust,ignore
/// #[derive(Debug, Deserialize, Validate, JsonSchema, SinkConfig)]
/// #[sink(
///     name = "s3-sink",
///     version = "1.0.0",
///     description = "Amazon S3 storage sink",
///     author = "Rivven Team",
///     license = "Apache-2.0",
///     batching,
///     batch_size = 1000
/// )]
/// pub struct S3SinkConfig {
///     pub bucket: String,
///     pub prefix: Option<String>,
/// }
/// ```
#[proc_macro_derive(SinkConfig, attributes(sink))]
pub fn derive_sink_config(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let attrs = match SinkAttrs::from_derive_input(&input) {
        Ok(v) => v,
        Err(e) => return TokenStream::from(e.write_errors()),
    };

    let struct_name = &attrs.ident;
    let spec_struct_name = quote::format_ident!("{}Spec", struct_name);

    let name = attrs.name.unwrap_or_else(|| {
        let name = struct_name.to_string();
        name.strip_suffix("Config").unwrap_or(&name).to_lowercase()
    });
    let version = attrs.version.unwrap_or_else(|| "0.0.1".to_string());

    let description_code = match &attrs.description {
        Some(desc) => quote! { .description(#desc) },
        None => quote! {},
    };

    let author_code = match &attrs.author {
        Some(author) => quote! { .author(#author) },
        None => quote! {},
    };

    let license_code = match &attrs.license {
        Some(license) => quote! { .license(#license) },
        None => quote! {},
    };

    let doc_url_code = match &attrs.documentation_url {
        Some(url) => quote! { .documentation_url(#url) },
        None => quote! {},
    };

    let batch_config_code = if attrs.batching {
        let batch_size = attrs.batch_size.unwrap_or(10_000);
        quote! {
            /// Returns the default batch configuration
            pub fn batch_config() -> rivven_connect::BatchConfig {
                rivven_connect::BatchConfig {
                    max_records: #batch_size,
                    ..Default::default()
                }
            }
        }
    } else {
        quote! {}
    };

    let expanded = quote! {
        /// Auto-generated spec holder for this sink configuration
        pub struct #spec_struct_name;

        impl #spec_struct_name {
            /// Returns the connector specification with config schema
            pub fn spec() -> rivven_connect::ConnectorSpec {
                rivven_connect::ConnectorSpec::builder(#name, #version)
                    #description_code
                    #author_code
                    #license_code
                    #doc_url_code
                    .config_schema::<#struct_name>()
                    .build()
            }

            /// Returns the connector name
            pub const fn name() -> &'static str {
                #name
            }

            /// Returns the connector version
            pub const fn version() -> &'static str {
                #version
            }

            #batch_config_code
        }
    };

    TokenStream::from(expanded)
}

/// Derive macro for implementing TransformConfig boilerplate
///
/// # Attributes
///
/// - `#[transform(name = "...")]` - Transform name (required)
/// - `#[transform(version = "...")]` - Transform version (default: "0.0.1")
/// - `#[transform(description = "...")]` - Transform description
///
/// # Example
///
/// ```rust,ignore
/// #[derive(Debug, Deserialize, Validate, JsonSchema, TransformConfig)]
/// #[transform(name = "json-filter", version = "1.0.0")]
/// pub struct JsonFilterConfig {
///     pub field: String,
///     pub pattern: String,
/// }
/// ```
#[proc_macro_derive(TransformConfig, attributes(transform))]
pub fn derive_transform_config(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let attrs = match TransformAttrs::from_derive_input(&input) {
        Ok(v) => v,
        Err(e) => return TokenStream::from(e.write_errors()),
    };

    let struct_name = &attrs.ident;
    let spec_struct_name = quote::format_ident!("{}Spec", struct_name);

    let name = attrs.name.unwrap_or_else(|| {
        let name = struct_name.to_string();
        name.strip_suffix("Config").unwrap_or(&name).to_lowercase()
    });
    let version = attrs.version.unwrap_or_else(|| "0.0.1".to_string());

    // Use `.description()` consistent with SourceConfig/SinkConfig
    let description_code = match attrs.description {
        Some(desc) => quote! { .description(#desc) },
        None => quote! {},
    };

    let expanded = quote! {
        /// Auto-generated spec holder for this transform configuration
        pub struct #spec_struct_name;

        impl #spec_struct_name {
            /// Returns the connector specification
            pub fn spec() -> rivven_connect::ConnectorSpec {
                rivven_connect::ConnectorSpec::builder(#name, #version)
                    #description_code
                    .build()
            }

            /// Returns the transform name
            pub const fn name() -> &'static str {
                #name
            }

            /// Returns the transform version
            pub const fn version() -> &'static str {
                #version
            }
        }
    };

    TokenStream::from(expanded)
}

/// Attributes for connector_spec macro
#[derive(Debug, Default, FromMeta)]
struct ConnectorSpecAttrs {
    /// Connector name
    #[darling(default)]
    name: Option<String>,
    /// Connector version
    #[darling(default)]
    version: Option<String>,
    /// Description of the connector
    #[darling(default)]
    description: Option<String>,
    /// Documentation URL
    #[darling(default)]
    documentation_url: Option<String>,
}

/// Attribute macro for defining connector specifications inline
///
/// This macro can be applied to a module or struct to generate
/// a `ConnectorSpec` with the given attributes.
///
/// # Example
///
/// ```rust,ignore
/// #[connector_spec(
///     name = "my-connector",
///     version = "1.0.0",
///     description = "A custom connector",
///     documentation_url = "https://docs.example.com"
/// )]
/// pub mod my_connector {
///     // Connector implementation
/// }
/// ```
#[proc_macro_attribute]
pub fn connector_spec(attr: TokenStream, item: TokenStream) -> TokenStream {
    // Parse attributes using darling
    let attr_args = match darling::ast::NestedMeta::parse_meta_list(attr.into()) {
        Ok(v) => v,
        Err(e) => return TokenStream::from(darling::Error::from(e).write_errors()),
    };

    let attrs = match ConnectorSpecAttrs::from_list(&attr_args) {
        Ok(v) => v,
        Err(e) => return TokenStream::from(e.write_errors()),
    };

    let name = attrs.name.unwrap_or_else(|| "unknown".to_string());
    let version = attrs.version.unwrap_or_else(|| "0.0.1".to_string());

    // Use `.description()` and `.documentation_url()` consistent with builder API
    let description_code = match attrs.description {
        Some(desc) => quote! { .description(#desc) },
        None => quote! {},
    };

    let doc_url_code = match attrs.documentation_url {
        Some(url) => quote! { .documentation_url(#url) },
        None => quote! {},
    };

    let item: proc_macro2::TokenStream = item.into();

    let expanded = quote! {
        #item

        /// Auto-generated connector specification
        pub fn connector_spec() -> rivven_connect::ConnectorSpec {
            rivven_connect::ConnectorSpec::builder(#name, #version)
                #description_code
                #doc_url_code
                .build()
        }
    };

    TokenStream::from(expanded)
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_derives_compile() {
        // These tests just verify the macros compile
        // Actual behavior is tested in integration tests
    }
}
