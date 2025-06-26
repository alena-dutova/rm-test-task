# Test task for RenMoney

## Overview

This repository contains a two-part solution to a test assignment focused on data pipeline design, cleaning, and analysis using SQL in PostgreSQL.

---

## Repository Structure

| File | Description |
|------|-------------|
| `Task_1_import_data.sql` | Solution for task 1 |
| `Task_2_query.sql` | Solution for task 2 |
| `Task_2_query_B_screen.sql` | Screen data for query B task 2 |
| `README.md` | Current documentation (you are here) |

---

## Task 1: Data Ingestion & Validation

### Goals

- Normalize data from 3 raw CSVs: `customers`, `customer_balance`, `customer_orders`.
- Clean invalid or inconsistent records.
- Ensure referential integrity.
- Log invalid rows for further QA.

### Key Techniques

- **Temporary staging tables** to separate raw from validated data.
- **ENUM types** to restrict categorical values like traffic sources and transaction types.
- **Cleansing operations** like `TRIM()`, `LOWER()`, and `CAST`.
- **Dynamic validation** using subqueries from `pg_enum` for flexibility.
- **Audit logs** for rejected values.

### Execution Flow

1. Drop existing objects for reproducibility.
2. Start a transaction to ensure atomicity.
3. Create staging tables and load raw data.
4. Clean and insert validated data into final tables.
5. Log any rejected rows.
6. Commit transaction.

---
## Task 2: SQL queries

**Key Points for B**:
- Implemented via `RANK()` to support N-top flexibility.
- Symbol analysis done via `ROW_NUMBER()` with partitioning.
