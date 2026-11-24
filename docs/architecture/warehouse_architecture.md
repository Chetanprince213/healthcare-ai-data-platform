# Healthcare Analytical Warehouse

## Overview

A configuration-driven analytical warehouse implemented
using Python, DuckDB and advanced SQL for synthetic
healthcare datasets.

## Architecture

    Synthetic Patient CSV
            |
            v
    Transactional Ingestion
            |
            v
    Data Quality Validation
            |
            v
    Persistent Analytical Warehouse
            |
            +--> Patient Analytics View
            |
            +--> Regional Healthcare Metrics
            |
            +--> Patient Utilization Ranking
            |
            +--> Synthetic Benchmark Workload
                        |
                        v
                Compute Benchmarking
                        |
                        v
                Monitoring and Evidence

## Database Organization

- Catalog: healthcare_analytics
- Schema: healthcare
- Source table: patients
- Analytical view: patient_analytics
- Benchmark table: benchmark_patients

The catalog and schema names are intentionally distinct
to prevent ambiguous DuckDB object resolution.

## Snowflake Three-Layer Architecture

### Database Storage Layer

Snowflake stores compressed columnar data in automatically
managed micro-partitions.

Storage is separated from virtual warehouse compute.

Micro-partition metadata supports efficient pruning
and analytical data access.

### Query Processing Layer

Virtual warehouses provide compute resources for SQL,
data loading and transformation workloads.

Independent warehouses can access shared data while
providing compute workload isolation.

### Cloud Services Layer

Coordinates authentication, authorization, metadata,
query compilation, optimization and transaction management.

## Virtual Warehouses

Virtual warehouses provide configurable compute capacity.

Common standard warehouse sizes include X-Small,
Small, Medium, Large, X-Large and larger sizes.

### Scale-Up

Increasing warehouse size provides additional resources
for computationally intensive workloads.

### Scale-Out

Multi-cluster warehouses add compute clusters to address
concurrent workloads and query queuing.

Availability depends on Snowflake edition and configuration.

### Auto-Suspend

Suspends idle warehouse compute after a configured
period to reduce unnecessary compute consumption.

### Auto-Resume

Automatically resumes suspended compute when a
qualifying workload requires warehouse resources.

### Workload Isolation

Separate warehouses can support ingestion, transformation,
business intelligence and data science workloads.

Local DuckDB resource profiles do not provide equivalent
independent distributed compute isolation.

## Snowflake Table Types

### Permanent Tables

Persistent tables with standard data protection features,
subject to configured retention and edition.

### Transient Tables

Persistent tables without Fail-safe and with reduced
Time Travel retention options.

### Temporary Tables

Session-scoped tables for intermediate processing.

### Views

Stored SQL definitions that provide logical access
to underlying data.

### Materialized Views

Persisted, maintained query results with platform
restrictions and additional maintenance costs.

### Dynamic Tables

Declaratively maintained query results with configurable
target freshness and refresh behavior.

Dynamic tables are covered conceptually here and
will receive separate treatment in later modules.

## Micro-Partitions and Pruning

Snowflake automatically organizes table data into
micro-partitions.

Metadata allows the query engine to skip partitions
that cannot satisfy query predicates.

Clustering can improve data locality for appropriate
large-table access patterns.

Clustering maintenance introduces additional cost.

The local benchmark demonstrates full-scan and
filtered-scan query plans.

DuckDB storage statistics and zone-map behavior are
not equivalent to Snowflake micro-partitions.

## Caching

### Persisted Query Results

Eligible previously computed query results may be
reused without executing the complete query again.

### Warehouse Data Cache

Running warehouses can retain locally cached data.

Suspending a warehouse can remove this local cache.

### Metadata-Based Optimization

Stored metadata can support certain query optimizations
without scanning all underlying data.

Repeated local DuckDB query execution does not
establish Snowflake result-cache behavior.

## Query Performance Optimization

- Select only required columns.
- Apply selective predicates.
- Avoid unnecessary joins and transformations.
- Inspect query execution plans.
- Review partition pruning and bytes scanned.
- Investigate memory pressure and disk spilling.
- Monitor query queuing and concurrency.
- Select compute resources using measured workloads.

Snowflake Query Profile provides execution operator
and resource consumption information.

The local implementation uses EXPLAIN ANALYZE.

## Advanced SQL

Implemented SQL capabilities:

- Common Table Expressions (CTEs).
- GROUP BY and aggregate functions.
- CASE expressions.
- NULLIF and null-safe division.
- DENSE_RANK.
- ROW_NUMBER.
- PARTITION BY.
- Windowed AVG and SUM.
- Views.
- Metadata inspection.
- Query execution plans.

## Security and Access Control

Snowflake implements Role-Based Access Control (RBAC).

Privileges are assigned to roles and roles are assigned
to users or other roles.

Common privileges include USAGE, SELECT, INSERT,
UPDATE, DELETE and CREATE TABLE.

Service identities and analysts should receive only
the permissions required for their responsibilities.

Local benchmarking uses read-only database connections.

Read-only DuckDB connections are not equivalent
to Snowflake cloud RBAC.

## Credit Consumption and Cost Optimization

Snowflake compute consumption is measured in credits.

Compute consumption depends on warehouse type,
size, running duration and applicable pricing.

Standard warehouse compute is generally billed per
second with a minimum charge when compute starts.

Storage and serverless services may incur separate charges.

Cost optimization techniques include:

- Appropriate warehouse sizing.
- Auto-suspend and auto-resume.
- Workload isolation.
- Query optimization.
- Monitoring concurrency and idle time.
- Resource monitors and budgets where applicable.

Resource monitors do not cover all serverless charges.

No actual Snowflake credits are consumed by this project.

## Data Quality and Reliability

- Transactional ingestion.
- Rollback on validation failure.
- Unique patient identifiers.
- Mandatory field validation.
- Valid patient age ranges.
- Non-negative prior visit counts.
- Binary appointment no-show indicators.
- Source-to-target reconciliation.
- Aggregate reconciliation.
- Repeatable full-refresh processing.
- Consistent results across compute profiles.

## Benchmark Methodology

The synthetic benchmark table expands the source
dataset by a configurable multiplier.

Expanded records are repeated synthetic records,
not additional independent patients.

| Profile | Threads | Memory Limit |
|---|---:|---:|
| Small | 1 | 512 MB |
| Medium | 2 | 1 GB |

Each profile executes an initial query followed by
five measured executions.

Metrics include minimum, maximum, mean and median
execution duration.

The first execution is not guaranteed to be cold-cache.

Repeated execution does not prove result-cache usage.

Benchmark results do not establish production scalability.

## Implementation Scope

LOCAL IMPLEMENTATION - NOT DEPLOYED TO SNOWFLAKE.

The analytical warehouse, SQL, transactions, data quality,
query profiling and automated tests are implemented directly.

Snowflake-specific distributed architecture, warehouse
management, caching, micro-partitions, cloud RBAC and
credit consumption are documented conceptually.

All healthcare data is synthetic and intended for
engineering demonstrations only.
