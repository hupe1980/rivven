//! Dashboard UI components
//!
//! Modular Leptos components for the Rivven dashboard.
//!
//! ## Architecture
//!
//! The component hierarchy follows a layered design:
//!
//! 1. **Primitives** (`primitives.rs`) - Low-level reusable building blocks
//!    - Loading indicators: `LoadingSpinner`, `Skeleton`, `SkeletonRow`
//!    - Error handling: `ErrorBoundary`, `ErrorState`, `ConnectionError`
//!    - Data display: `StatCard`, `TableCard`, `Badge`, `StatusDot`
//!    - Forms: `SearchInput`
//!
//! 2. **Icons** (`icons.rs`) - SVG icon components for air-gapped deployments
//!    - Navigation: `HomeIcon`, `TopicIcon`, `GroupIcon`, `ClusterIcon`, `MetricsIcon`
//!    - Actions: `RefreshIcon`, `SearchIcon`, `CopyIcon`, `CheckIcon`
//!    - Status: `SuccessIcon`, `WarningIcon`, `ErrorIcon`, `InfoIcon`
//!
//! 3. **Layout** (`header.rs`, `sidebar.rs`) - Structural components
//!
//! 4. **Views** (`overview.rs`, `topics.rs`, etc.) - Page-level components
//!
//! ## Accessibility
//!
//! All components include proper ARIA attributes for screen reader support.

pub mod cluster;
pub mod consumer_groups;
pub mod header;
pub mod icons;
pub mod metrics;
pub mod overview;
pub mod primitives;
pub mod sidebar;
pub mod topics;

// Re-export layout components
pub use header::Header;
pub use sidebar::Sidebar;

// Re-export primitives for convenient access
pub use primitives::{
    Badge, BadgeVariant, ConnectionError, EmptyState, ErrorBoundary, ErrorState, InfoRow,
    LagIndicator, LoadingSpinner, SearchInput, Skeleton, SkeletonRow, StatCard, StatusDot,
    TableCard,
};

// Re-export commonly used icons
pub use icons::{
    ClusterIcon, ErrorIcon, GroupIcon, HomeIcon, InfoIcon, LeaderIcon, MessageIcon, MetricsIcon,
    PartitionIcon, RefreshIcon, SearchIcon, SuccessIcon, TopicIcon, WarningIcon,
};
