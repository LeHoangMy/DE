# Project 03 – Python in Data

## Overview

This project focuses on building a **Synthetic Data Generation** system for an E-commerce OLTP environment using Python. The primary goal is to generate relational data that is logically connected and load it into a **PostgreSQL** database while maintaining high data integrity.

**Core Technologies:**

**Python:** Main programming language.

**Faker:** Library for generating realistic synthetic data.

**Psycopg2:** PostgreSQL adapter for Python to handle database operations.

**Poetry:** Dependency management and virtual environment control.



---

## Project Objectives

* Define a relational schema for an E-commerce OLTP system
* Generate realistic synthetic data using **Python + Faker**
* Ensure relational and business consistency across all tables
* Insert generated data into a SQL database
* Organize the codebase in a clean, scalable, and testable structure

---

## Project Workflow

1. Define database schemas and relationships
2. Generate synthetic data for each table using Faker
3. Convert data types to match database schema requirements
4. Insert data into the target SQL database
5. Run validation tests to ensure data integrity

---

## Directory Structure

```
LE_HOANG_MY_LV1_PROJECT_03/
│
├── src/le_hoang_my_lv1_project_03/
│   ├── config/
│   │   ├── config.py          # Global configuration
│   │   ├── database.ini       # Database connection settings
│   │   └── __init__.py
│   │
│   ├── db/
│   │   ├── generators/        # Data generation logic using Faker
│   │   │   ├── base.py
│   │   │   ├── brand.py
│   │   │   ├── category.py
│   │   │   ├── seller.py
│   │   │   ├── product.py
│   │   │   ├── order.py
│   │   │   ├── order_item.py
│   │   │   ├── promotion.py
│   │   │   ├── promotion_product.py
│   │   │   └── __init__.py
│   │   │
│   │   ├── inserters/         # Database insertion logic
│   │   │   ├── inserter.py
│   │   │   └── __init__.py
│   │   │
│   │   ├── connection.py      # Database connection handling
│   │   ├── pipeline.py        # End-to-end data generation & load pipeline
│   │   ├── schemas.py         # Table schemas and metadata
│   │   └── __init__.py
│   │
│   ├── tests/
│   │   ├── quick_test.py      # Quick sanity checks
│   │   └── __init__.py
│   │
│   ├── main.py                # Project entry point
│   └── __init__.py
│
├── .gitignore
├── poetry.lock
└── README.md
```

---

## Key Components

### 1. Data Generators (`db/generators`)

Each generator is responsible for creating synthetic data for a specific table.

Examples:

* `product.py`: generates product information
* `order.py`: generates orders linked to customers
* `order_item.py`: generates order line items linked to orders and products
* `promotion.py`: generates promotions

All generators:

* Use **Faker** for realistic data
* Respect foreign key dependencies
* Inherit common logic from `base.py`

---

### 2. Database Layer

* `connection.py`: Handles database connections
* `schemas.py`: Centralized schema definitions
* `inserters/inserter.py`: Handles batch inserts into database tables
* `pipeline.py`: Orchestrates generation and insertion order to preserve referential integrity

---

### 3. Configuration

* `database.ini`: Stores database credentials and connection info
* `config.py`: Global constants and runtime settings

---

### 4. Testing

* `tests/quick_test.py`: Lightweight tests to validate:

  * Record counts
  * Foreign key relationships
  * Basic data sanity

---

## Installation

### 1. Clone the Repository

```bash
git clone <repository-url>
cd LE_HOANG_MY_LV1_PROJECT_03
```

### 2. Install Dependencies (Poetry)

```bash
poetry install
```

Activate the virtual environment:

```bash
poetry shell
```

---

## Database Setup

1. Create an empty database in PostgreSQL / MySQL / SQL Server
2. Update `database.ini` with your database credentials
3. Ensure the database schema (tables & constraints) is created before running the pipeline

---

## Running the Project

Execute the full data generation and loading pipeline:

```bash
poetry run python main.py
```

Use the provided testing utility to verify specific modules:

```bash
poetry run python tests/quick_test.py
```

---

## Notes & Assumptions

* The project focuses on **OLTP-style normalized data**, not analytics
* Data volumes can be configured in `config.py`
* Faker locale and randomness can be customized if needed

---


