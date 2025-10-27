# Data Catalog — Gold Layer

## Overview
The **Gold Layer** represents business-ready, curated data modeled to support analytical and reporting use cases.  
It integrates cleaned and conformed dimension tables with fact tables optimized for performance and usability in BI tools such as Power BI and Tableau.

---

## 1. `gold.dim_customers`
- **Purpose:** Provides enriched customer information including demographic and geographic details.
- **Columns**

| Column Name       | Data Type     | Description                                                                 |
|-------------------|---------------|------------------------------------------------------------------------------|
| `customer_key`    | INT           | Surrogate key uniquely identifying each customer record in the dimension table.|
| `customer_id`     | INT           | Unique numerical identifier assigned to each customer.                      |
| `customer_number` | NVARCHAR(50)  | Alphanumeric identifier used for tracking and referencing.                  |
| `first_name`      | NVARCHAR(50)  | Customer’s first name.                                                      |
| `last_name`       | NVARCHAR(50)  | Customer’s last name or family name.                                        |
| `country`         | NVARCHAR(50)  | Customer’s country of residence (e.g., 'Australia').                        |
| `marital_status`  | NVARCHAR(50)  | Marital status (e.g., 'Married', 'Single').                                 |
| `gender`          | NVARCHAR(50)  | Gender (e.g., 'Male', 'Female', 'n/a').                                     |
| `birthdate`       | DATE          | Customer’s date of birth `YYYY-MM-DD` (e.g., 1971-10-06).                   |
| `create_date`     | DATE          | The date and time when the customer record was created in the system.       |


---

## 2. `gold.dim_products`
- **Purpose:** Provides detailed product information, including classification, cost, and maintenance requirements.
- **Columns**

| Column Name           | Data Type     | Description                                                                 |
|-----------------------|---------------|------------------------------------------------------------------------------|
| `product_key`         | INT           | Surrogate key uniquely identifying each product record in the product dimension table.|
| `product_id`          | INT           | A unique identifier assigned to the product for internal tracking and referencing.|
| `product_number`      | NVARCHAR(50)  | A structured alphanumeric code representing the product, often used for categorization or inventory.|
| `product_name`        | NVARCHAR(50)  | Descriptive name of the product.                                             |
| `category_id`         | NVARCHAR(50)  | A unique identifier for the product's category, linking to its high-level classification.|
| `category`            | NVARCHAR(50)  | Product’s main classification (e.g., 'Bikes', 'Components').                 |
| `subcategory`         | NVARCHAR(50)  | Detailed classification within the main category.                            |
| `maintenance_required`| NVARCHAR(50)  | Indicates if the product requires maintenance ('Yes' / 'No').                |
| `cost`                | INT           | The cost or base price of the product, measured in monetary units.           |
| `product_line`        | NVARCHAR(50)  | Product line or series (e.g., 'Road', 'Mountain').                           |
| `start_date`          | DATE          | The date when the product became available for sale or use, stored in.       |


---

## 3. `gold.fact_sales`
- **Purpose:** Stores transactional sales data used for revenue and trend analysis.
- **Columns**

| Column Name     | Data Type     | Description                                                                 |
|-----------------|---------------|------------------------------------------------------------------------------|
| `order_number`  | NVARCHAR(50)  | A unique alphanumeric identifier for each sales order (e.g., 'SO54496').    |
| `product_key`   | INT           | Foreign key referencing `dim_products`.                                     |
| `customer_key`  | INT           | Foreign key referencing `dim_customers`.                                    |
| `order_date`    | DATE          | Date when the order was placed.                                             |
| `shipping_date` | DATE          | Date when the order was shipped.                                            |
| `due_date`      | DATE          | Date when the order payment was due.                                        |
| `sales_amount`  | INT           | The total monetary value of the sale for the line item, in whole currency units (e.g., 25).|
| `quantity`      | INT           | The number of units of the product ordered for the line item (e.g., 1).     |
| `price`         | INT           | The price per unit of the product for the line item, in whole currency units (e.g., 25).|


---
