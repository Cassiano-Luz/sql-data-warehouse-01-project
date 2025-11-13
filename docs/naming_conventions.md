# **Naming Conventions**

This document defines the naming standards applied to schemas, tables, views, columns, and other data warehouse objects

## **Table of Contents**

1. [General Principles](#general-principles)
2. [Table Naming Conventions](#table-naming-conventions)
   - [Bronze Rules](#bronze-rules)
   - [Silver Rules](#silver-rules)
   - [Gold Rules](#gold-rules)
3. [Column Naming Conventions](#column-naming-conventions)
   - [Surrogate Keys](#surrogate-keys)
   - [Technical Columns](#technical-columns)
4. [Stored Procedure](#stored-procedure)
5. [Scripts psql](#scripts-psql)
---


## **General Principles**

**Naming Conventions:** Use snake_case, with lowercase letter and underscores ( _ ) to separate word.
**Language:** Use English for all names.
**Avoid Reserved Words:** Do not use SQL reserved words as object names.


## **Table Naming Conventions**

### **Bronze Rules**
- All names must start with source system name, and table names must match their original names without renaming.
- sourcesystem_entity: Name of the source system (crm, erp).
- entity: Exact table name from the source system.
- Example: `crm_customer_info` -> Customer information from the CRM system.

### **Silver Rules**
- All names must start with source system name, and table names must match their original names without renaming.
- sourcesystem_entity: Name of the source system (crm, erp).
- entity: Exact table name from the source system.
- Example: `crm_customer_info` -> Customer information from the CRM system.

### **Gold Rules**
- All names must use meaningful, business-aligned names for tables, starting with the category prefix.
- category: Describes the role of the table, such as dim(dimension) or fact (fact table).
- entity: Descriptive name of the table, aligned with the business domain (customers, products, sales).
- Example: `dim_customers` (Dimension table for customer data).
- Example: `fact_sales` (Fact table containing sales transactions).


#### **Glossary of Category Patterns**

| Pattern | Meaning          | Examples                             |
| ------- | ---------------- | ------------------------------------ |
| dim_    | Dimension table  | `dim_customer`, `dim_product`        |
| fact_   | Fact table       | `fact_sales`                         |
| agg_    | Aggregated table | `agg_customers`, `agg_sales_monthly` |

## **Column Naming Conventions**

### **Surrogate Keys**  

- All primary keys in dimension tables must use the suffix `_key`.
- **table_name_key**  
  - `<table_name>`: Refers to the name of the table or entity the key belongs to.  
  - `_key`: A suffix indicating that this column is a surrogate key.  
  - Example: `customer_key` → Surrogate key in the `dim_customers` table.

### **Technical Columns**

- All technical columns must start with the prefix `dwh_`, followed by a descriptive name indicating the column's purpose.
- **`dwh_<column_name>`**  
  - `dwh`: Prefix exclusively for system-generated metadata.  
  - `<column_name>`: Descriptive name indicating the column's purpose.  

  - Example: `dwh_load_date` → System-generated column used to store the date when the record was loaded.


## **Stored Procedure**

- All stored procedures used for loading data must follow the naming pattern:
- **`layer>_load`**.
  - `<layer>`: Represents the layer being loaded, such as `bronze`, `silver`, or `gold`.
  - Example:
    - `bronze_load` → Stored procedure for loading data into the Bronze layer.
    - `silver_load` → Stored procedure for loading data into the Silver layer.


## **Scripts psql**

- All scripts psql used for loading data must follow the naming pattern:
- **`<layer>_load`**.
  - `<layer>`: Represents the layer being loaded, such as `bronze`, `silver`, or `gold`.
  - Example:
    - `bronze_load` → Script for loading data into the Bronze layer.
    - `silver_load` → Script for loading data into the Silver layer.
