# Kimball-Style Job Postings Data Warehouse (PostgreSQL)

## Overview

This project is a dimensional **data warehouse** built in PostgreSQL using job postings data.

It is inspired by the warehouse build project from [Luke Barousse's SQL Data Engineering Course](https://github.com/lukebarousse/SQL_Data_Engineering_Course), but redesigned to be fully aligned with **Kimball’s dimensional modeling methodology**.

The objective of this project was not only to follow along and replicate the functionality of the original project, but to:

- ☑️ Apply Kimball architecture principles
- ☑️ Design a clean star schema
- ☑️ Introduce surrogate keys
- ☑️ Define a precise fact table grain
- ☑️ Handle multivalued dimensions correctly (bridge table pattern)
- ☑️ Build a structured ETL flow in pure SQL

This project was developed as a learning exercise and a showcase of the practical use of DDL and DML in SQL as well as the fundamentals of dimensional data engineering.

![ERD_Data_Warehouse](/ERD-DW.png)
*The Entity-Relationship Diagram of the data warehouse created in this project (diagram created with https://dbdiagram.io).*

The source code and original data can be found on [Luke's GitHub](https://github.com/lukebarousse/SQL_Project_Data_Job_Analysis/tree/main/sql_load).

---

# Architecture

## Dimensional Model (Star Schema)

The warehouse follows a **Kimball-style star schema** with:

### Fact Table
- `job_postings_fact`

### Dimensions
- `date_posted_dim` <-- date dimension 🗓️
- `company_dim`
- `job_title_dim`
- `location_dim`
- `job_profile_dim` <-- 'junk' dimension 🗑️
- `job_via_dim`
- `skills_dim`

### Bridge Table
- `skills_job_dim`
(resolves many-to-many relationship between job postings and skills)

---

## Fact Table Grain

The grain of `job_postings_fact` is:

> **One row per job posting**

All measures and foreign keys are defined at this level. This ensures no mixed granularity, no double counting and clean dimensional joins.

---

# Key Design Decisions (Kimball Alignment)

## 1️⃣ Surrogate Keys

All dimensions use **integer surrogate keys** instead of natural keys.

Benefits:
- Decouples warehouse from source system volatility
- Enables Slowly Changing Dimensions (possible future extension)
- Improves join and query performance
- Maintains referential integrity

The fact table uses `job_key` as its primary key, replacing the original source identifier (`job_id`) which was preserved as degenerate dimension. 

---

## 2️⃣ Multivalued Dimension – Bridge Table

Skills represent a **multivalued dimension** (a job can require many skills).

Instead of denormalizing skills into the fact table, the model implements a bridge table `skills_job_dim`.

This bridge table:
- Connects `job_postings_fact` and `skills_dim`
- Preserves many-to-many cardinality
- Prevents fact row explosion

---

## 3️⃣ Date Dimension

A proper `date_posted_dim` was created instead of relying on raw timestamps.

Advantages:
- Enables time intelligence
- Supports calendar attributes
- Decouples reporting logic from raw datetime fields
- Aligns with dimensional modeling best practices

---

## 4️⃣ Clear ETL Separation

The project is organized into three logical steps:

### 1. Data Exploration
`01_data_exploration.sql`
- Understand raw tables
- Inspect distributions
- Validate assumptions

### 2. Schema Creation
`02_create_schema.sql`
- Create fact and dimension tables
- Define primary & foreign keys
- Implement star schema structure

### 3. Data Population (ETL)
`03_populate_tables.sql`
- Populate dimensions first
- Generate surrogate keys
- Load fact table
- Populate bridge table
- Ensure referential consistency

This sequencing follows Kimball’s recommended load order:

> Dimensions → Fact → Bridge

---

# Technologies and Skills Used

- PostgreSQL
- SQL (DDL + DML)
- Dimensional modeling (Kimball methodology)

Concepts applied:
- Star schema
- Surrogate keys
- Fact table grain definition
- Bridge table design
- Referential integrity
- Structured ETL sequencing

---

# How This Differs From the Original Project

Compared to the original implementation from Luke Barousse, this version introduces:

- Strict dimensional modeling discipline as per Ralph Kimball
- Dedicated date dimension
- Warehouse-oriented schema (all attributes moved from fact table to dedicated dimensions)
- Entire data quality validation layer

The goal was to move from SQL transformations for analytics to architected dimensional data warehouse.

---

# What This Project Demonstrates

This project demonstrates the ability to:

- Translate transactional-style data into dimensional form
- Apply Kimball principles in practice
- Design star schemas intentionally
- Handle complex relationships (many-to-many)
- Structure SQL-based ETL
- Data quality awareness
- Think beyond analysis toward data engineering architecture

---

# Possible Extensions

Future improvements could include:

- Slowly Changing Dimensions
- Enterprise bus matrix for scaling (multiple fact tables connected via conformed dimensions)
- Indexing strategy optimization
- Partitioning of fact table, incremental loading logic
- Views for BI consumption
- ~~Data quality validation layer (**done**)~~

---

# Final Notes

This project was built as a learning initiative to deepen understanding of:

- Data warehouse architecture
- SQL-based ETL design
- Dimensional modeling

It bridges the gap between data analysis and data engineering, with an emphasis on correct modeling discipline rather than just query results.

![](https://komarev.com/ghpvc/?username=MarcinCzerkas&style=flat-square&label=VIEWS&style=pixel)
