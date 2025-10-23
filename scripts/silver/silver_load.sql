/*
=============================================================
Loading data into Silver Tables
=============================================================

File: scripts/silver/silver_load.sql

Description: Creates a stored procedure in the 'silver' schema to load cleaned data from the 'bronze' layer into the Silver tables.

Script Purpose:

	- Standardize data transformation and normalization steps.
  - Ensure data freshness and consistency across Silver tables.
  - Provide logging and performance monitoring for each load step.

Run Context:
	- Connected to database 'datawarehouse'.
	- Requires read access on schema 'bronze' and write access on schema 'silver'.
*/

CREATE OR REPLACE PROCEDURE silver.silver_load()
LANGUAGE plpgsql
AS $$
DECLARE
	v_start_time TIMESTAMP;
	v_end_time TIMESTAMP;
	v_rows_affected INTEGER;
	v_step_name VARCHAR(200);
	v_total_start TIMESTAMP;
BEGIN
	-- Log procedure start time
    v_total_start := CLOCK_TIMESTAMP();
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Initiating procedure silver.silver_load()';
    RAISE NOTICE 'Timestamp: %', v_total_start;
    RAISE NOTICE '========================================';
	
	/*
	==================================================
	Loading cleaned data into silver.crm_cust_info
	==================================================
	*/
	BEGIN
		-- Performance monitoring variables
        v_step_name := 'silver.crm_cust_info';
        v_start_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '';
        RAISE NOTICE '[%] Starting data load...', v_step_name;

		TRUNCATE TABLE silver.crm_cust_info;
		INSERT INTO silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date)
		SELECT
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				ELSE 'n/a'
			END AS cst_marital_status, -- Normalize marital status values to readable format
			CASE
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				ELSE 'n/a'
			END AS cst_gndr, -- Normalize gender values to readable format
			cst_create_date
		FROM
				(SELECT
					*,
					ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
				FROM bronze.crm_cust_info
				)t
		WHERE 
			flag_last = 1 AND
			cst_id IS NOT NULL;

		-- Log step completion summary with row count and execution time.
		-- In case of error, capture and raise an explicit message with the step name.
		GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
        v_end_time := CLOCK_TIMESTAMP();
        
        RAISE NOTICE '[%] ✓ Completed | Rows: % | Duration: %s', 
            v_step_name, 
            v_rows_affected, 
            ROUND(EXTRACT(EPOCH FROM (v_end_time - v_start_time))::numeric, 3);
        
	    EXCEPTION WHEN OTHERS THEN
	        RAISE NOTICE '[%] ✗ ERROR: %', v_step_name, SQLERRM;
	        RAISE;
    END;
		
		/*
		==================================================
		Loading cleaned data into silver.crm_prd_info
		==================================================
		*/
	BEGIN
		-- Performance monitoring variables
        v_step_name := 'silver.crm_prd_info';
        v_start_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '';
        RAISE NOTICE '[%] Starting data load...', v_step_name;
		
		TRUNCATE TABLE silver.crm_prd_info;
		INSERT INTO silver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt)
		SELECT
			prd_id,
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
			SUBSTRING(prd_key, 7) AS prd_key,
			prd_nm,
			CASE
				WHEN prd_cost IS NULL THEN 0
				ELSE prd_cost
			END AS prd_cost,
			CASE UPPER(TRIM(prd_line))
				WHEN 'M' THEN 'Mountain'
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'Other Sales'
				WHEN 'T' THEN 'Touring'
				ELSE 'n/a'
			END AS prd_line,
			CAST(prd_start_dt AS DATE),
			CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL '1 day' AS DATE) AS prd_end_dt
		FROM bronze.crm_prd_info
		ORDER BY prd_id;

		-- Log step completion summary with row count and execution time.
		-- In case of error, capture and raise an explicit message with the step name.
		GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
        v_end_time := CLOCK_TIMESTAMP();
        
        RAISE NOTICE '[%] ✓ Completed | Rows: % | Duration: %s', 
            v_step_name, 
            v_rows_affected, 
            ROUND(EXTRACT(EPOCH FROM (v_end_time - v_start_time))::numeric, 3);
        
	    EXCEPTION WHEN OTHERS THEN
	        RAISE NOTICE '[%] ✗ ERROR: %', v_step_name, SQLERRM;
	        RAISE;
    END;
		
		/*
		==================================================
		Loading cleaned data into silver.crm_sales_details
		==================================================
		*/
	BEGIN
		-- Performance monitoring variables
        v_step_name := 'silver.crm_sales_details';
        v_start_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '';
        RAISE NOTICE '[%] Starting data load...', v_step_name;
		
		TRUNCATE TABLE silver.crm_sales_details;
		INSERT INTO silver.crm_sales_details (
		    sls_ord_num,
		    sls_prd_key,
		    sls_cust_id,
		    sls_order_dt,
		    sls_ship_dt,
		    sls_due_dt,
		    sls_sales,
		    sls_quantity,
		    sls_price)
		SELECT
		    sls_ord_num,
		    sls_prd_key,
		    sls_cust_id,
			CASE
				WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::text) <> 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt,
			CASE
				WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::text) <> 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,
			CASE
				WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::text) <> 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,
			CASE
				WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales <> (sls_quantity * ABS(sls_price)) THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales,
		    sls_quantity,
			CASE
				WHEN sls_price <= 0 OR sls_price IS NULL THEN sls_sales / NULLIF(sls_quantity, 0)
				ELSE sls_price
			END AS sls_price
		FROM bronze.crm_sales_details;

		-- Log step completion summary with row count and execution time.
		-- In case of error, capture and raise an explicit message with the step name.
		GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
        v_end_time := CLOCK_TIMESTAMP();
        
        RAISE NOTICE '[%] ✓ Completed | Rows: % | Duration: %s', 
            v_step_name, 
            v_rows_affected, 
            ROUND(EXTRACT(EPOCH FROM (v_end_time - v_start_time))::numeric, 3);
        
	    EXCEPTION WHEN OTHERS THEN
	        RAISE NOTICE '[%] ✗ ERROR: %', v_step_name, SQLERRM;
	        RAISE;
    END;
		
		/*
		==================================================
		Loading cleaned data into silver.erp_cust_az12
		==================================================
		*/
	BEGIN
		-- Performance monitoring variables
        v_step_name := 'silver.erp_cust_az12';
        v_start_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '';
        RAISE NOTICE '[%] Starting data load...', v_step_name;
		
		TRUNCATE TABLE silver.erp_cust_az12;
		INSERT INTO silver.erp_cust_az12 (
		    cid,
		    bdate,
		    gen)
		SELECT
		    CASE
				WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4)
				ELSE cid
			END AS cid,
		    CASE
				WHEN bdate > CURRENT_DATE THEN NULL
				ELSE bdate
			END AS bdate,
			CASE
				WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
				WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
				ELSE 'n/a'
			END AS gen -- Normalize gender values to readable format
		FROM bronze.erp_cust_az12;

		-- Log step completion summary with row count and execution time.
		-- In case of error, capture and raise an explicit message with the step name.
		GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
        v_end_time := CLOCK_TIMESTAMP();
        
        RAISE NOTICE '[%] ✓ Completed | Rows: % | Duration: %s', 
            v_step_name, 
            v_rows_affected, 
            ROUND(EXTRACT(EPOCH FROM (v_end_time - v_start_time))::numeric, 3);
        
	    EXCEPTION WHEN OTHERS THEN
	        RAISE NOTICE '[%] ✗ ERROR: %', v_step_name, SQLERRM;
	        RAISE;
    END;
		
		/*
		==================================================
		Loading cleaned data into silver.erp_loca_a101
		==================================================
		*/
	BEGIN
		-- Performance monitoring variables
        v_step_name := 'silver.erp_loca_a101';
        v_start_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '';
        RAISE NOTICE '[%] Starting data load...', v_step_name;
		
		TRUNCATE TABLE silver.erp_loc_a101;
		INSERT INTO silver.erp_loc_a101 (
		    cid,
			cntry)
		SELECT
			REPLACE(cid, '-', '') AS cid,
			CASE
				WHEN UPPER(TRIM(cntry)) IN ('US', 'USA', 'UNITED STATES') THEN 'United States'
				WHEN UPPER(TRIM(cntry)) IN ('DE', 'GERMANY') THEN 'Germany'
				WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
				ELSE TRIM(cntry) -- Normalize and Handle missing or blank country codes
			END AS cntry
		FROM bronze.erp_loc_a101;

		-- Log step completion summary with row count and execution time.
		-- In case of error, capture and raise an explicit message with the step name.
		GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
        v_end_time := CLOCK_TIMESTAMP();
        
        RAISE NOTICE '[%] ✓ Completed | Rows: % | Duration: %s', 
            v_step_name, 
            v_rows_affected, 
            ROUND(EXTRACT(EPOCH FROM (v_end_time - v_start_time))::numeric, 3);
        
	    EXCEPTION WHEN OTHERS THEN
	        RAISE NOTICE '[%] ✗ ERROR: %', v_step_name, SQLERRM;
	        RAISE;
    END;
		
		/*
		==================================================
		Loading cleaned data into silver.erp_px_cat_g1v2
		==================================================
		*/
	BEGIN
		-- Performance monitoring variables
        v_step_name := 'silver.erp_px_cat_g1v2';
        v_start_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '';
        RAISE NOTICE '[%] Starting data load...', v_step_name;
		
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		INSERT INTO silver.erp_px_cat_g1v2 (
		    id,
			cat,
			subcat,
			maintenance)
		SELECT
		    id,
			cat,
			subcat,
			maintenance
		FROM bronze.erp_px_cat_g1v2;

		-- Log step completion summary with row count and execution time.
		-- In case of error, capture and raise an explicit message with the step name.
		GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
        v_end_time := CLOCK_TIMESTAMP();

        RAISE NOTICE '[%] ✓ Completed | Rows: % | Duration: %s', 
            v_step_name, 
            v_rows_affected, 
            ROUND(EXTRACT(EPOCH FROM (v_end_time - v_start_time))::numeric, 3);
        
	    EXCEPTION WHEN OTHERS THEN
	        RAISE NOTICE '[%] ✗ ERROR: %', v_step_name, SQLERRM;
	        RAISE;
    END;

    -- Final summary
    RAISE NOTICE '';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Procedure completed successfully!';
    RAISE NOTICE 'Total Duration: %s', 
        ROUND(EXTRACT(EPOCH FROM (CLOCK_TIMESTAMP() - v_total_start))::numeric, 3);
    RAISE NOTICE '========================================';
    
	EXCEPTION WHEN OTHERS THEN
	    RAISE NOTICE '';
	    RAISE NOTICE '========================================';
	    RAISE NOTICE 'CRITICAL ERROR DURING EXECUTION';
	    RAISE NOTICE 'Message: %', SQLERRM;
	    RAISE NOTICE '========================================';
    RAISE;
END;
$$;
-- Run the silver_load procedure
CALL silver.silver_load();
