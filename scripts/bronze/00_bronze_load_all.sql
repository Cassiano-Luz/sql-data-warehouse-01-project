/*
=============================================================
Load Bronze Layer (All Tables)
=============================================================

File: scripts/bronze/bronze_load_all.psql

Script Purpose:

    - Load all Bronze tables from CSV sources (CRM and ERP).
    - Truncate each target table before loading to ensure consistency.
    - Use client-side '\copy' for portability across environments.
    - Use relative paths so the script runs on any machine when executed from the project root.

Run Context:
    - Execute this script using psql, connected to the 'DataWarehouse' database.
    - Example:
        psql -h localhost -U postgres -d DataWarehouse -f scripts/bronze/bronze_load_all.psql

Notes:
    - When running the script, psql will prompt for the PostgreSQL user password (default user: 'postgres').
      Ensure you have the correct password before execution, or configure a .pgpass file.
    - '\copy' must be written on a single physical line (psql limitation).
    - CSV files are expected under:
        ./datasets/source_crm/  →  cust_info.csv, prd_info.csv, sales_details.csv
        ./datasets/source_erp/  →  CUST_AZ12.csv, LOC_A101.csv, PX_CAT_G1V2.csv
    - This script stops immediately if any error occurs.
    - Timing is enabled to display execution duration for each step.
*/


-- Stop execution immediately if any error occurs
\set ON_ERROR_STOP on

-- Enable timing for each executed statement
\timing on


/* ==========================================================
   CRM TABLES
   ========================================================== */

\echo ==========================================
\echo Loading bronze.crm_cust_info
\echo ==========================================
TRUNCATE TABLE bronze.crm_cust_info;
\copy bronze.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date) FROM './datasets/source_crm/cust_info.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8', QUOTE '"', ESCAPE '"');
\echo --- crm_cust_info loaded ---

\echo ==========================================
\echo Loading bronze.crm_prd_info
\echo ==========================================
TRUNCATE TABLE bronze.crm_prd_info;
\copy bronze.crm_prd_info (prd_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt) FROM './datasets/source_crm/prd_info.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8', QUOTE '"', ESCAPE '"');
\echo --- crm_prd_info loaded ---

\echo ==========================================
\echo Loading bronze.crm_sales_details
\echo ==========================================
TRUNCATE TABLE bronze.crm_sales_details;
\copy bronze.crm_sales_details (sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price) FROM './datasets/source_crm/sales_details.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8', QUOTE '"', ESCAPE '"');
\echo --- crm_sales_details loaded ---


/* ==========================================================
   ERP TABLES
   ========================================================== */

\echo ==========================================
\echo Loading bronze.erp_cust_az12
\echo ==========================================
TRUNCATE TABLE bronze.erp_cust_az12;
\copy bronze.erp_cust_az12 (cid, bdate, gen) FROM './datasets/source_erp/CUST_AZ12.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8', QUOTE '"', ESCAPE '"');
\echo --- erp_cust_az12 loaded ---

\echo ==========================================
\echo Loading bronze.erp_loc_a101
\echo ==========================================
TRUNCATE TABLE bronze.erp_loc_a101;
\copy bronze.erp_loc_a101 (cid, cntry) FROM './datasets/source_erp/LOC_A101.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8', QUOTE '"', ESCAPE '"');
\echo --- erp_loc_a101 loaded ---

\echo ==========================================
\echo Loading bronze.erp_px_cat_g1v2
\echo ==========================================
TRUNCATE TABLE bronze.erp_px_cat_g1v2;
\copy bronze.erp_px_cat_g1v2 (id, cat, subcat, maintenance) FROM './datasets/source_erp/PX_CAT_G1V2.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8', QUOTE '"', ESCAPE '"');
\echo --- erp_px_cat_g1v2 loaded ---


-- Final message
\echo ==========================================
\echo Bronze Layer Load Completed Successfully
\echo ==========================================
\timing off
