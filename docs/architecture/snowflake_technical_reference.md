# Snowflake Architecture and Warehouse Engineering

## Architecture

Snowflake separates database storage, query processing
and cloud services into distinct architectural layers.

This allows compute resources to be managed separately
from stored data.

## Virtual Warehouse

A virtual warehouse is a compute resource used for
queries, transformations and supported loading operations.

## Scaling

Scale-up increases warehouse size.

Scale-out adds clusters to address workload concurrency.

## Auto-Suspend and Auto-Resume

Auto-suspend reduces idle compute consumption.

Auto-resume starts suspended compute when required.

## Micro-Partitions

Automatically managed columnar storage units with
metadata used for pruning.

## Partition Pruning

The query engine skips storage partitions that cannot
contain matching records.

## Clustering

Improves data organization for suitable large-table
access patterns, subject to maintenance costs.

## Query Caching

Persisted result reuse, warehouse-local data caching
and metadata-based optimization serve different purposes.

## Query Optimization

Inspect query profiles, pruning, scanned data,
execution operators, spilling and queuing.

## RBAC

Role-Based Access Control assigns privileges through
roles and supports least-privilege access.

## Resource Monitoring

Resource monitors can track and control applicable
warehouse credit consumption.

Budgets provide broader supported cost monitoring.

## Data Reliability

ACID means Atomicity, Consistency, Isolation and
Durability.

Transactions protect data changes through commit
and rollback behavior.

Idempotent ingestion produces the intended target
state when the same input is processed repeatedly.

## Project Experience Boundary

The implemented project uses DuckDB for local
analytical processing.

Snowflake-specific cloud operations are reference
examples and conceptual learning, not production
Snowflake deployment experience.
