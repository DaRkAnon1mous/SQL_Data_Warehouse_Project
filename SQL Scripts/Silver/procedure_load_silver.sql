/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    call silver.load_silver;
===============================================================================
*/



DROP PROCEDURE IF EXISTS silver.load_silver;

CREATE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time      timestamp;
    v_end_time        timestamp;
    v_batch_start_time timestamp;
    v_batch_end_time   timestamp;
BEGIN
    v_batch_start_time := now();
    RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading Silver Layer';
    RAISE NOTICE '================================================';

    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '------------------------------------------------';

    -- CRM Customer Info
    v_start_time := now();
    RAISE NOTICE '>> Truncating Table: silver.crm_cust_info';
    TRUNCATE TABLE silver.crm_cust_info;
    RAISE NOTICE '>> Inserting Data Into: silver.crm_cust_info';
    INSERT INTO silver.crm_cust_info (
        cst_id, 
        cst_key, 
        cst_firstname, 
        cst_lastname, 
        cst_marital_status, 
        cst_gndr,
        cst_create_date
    )
    SELECT 
        cst_id,
        cst_key,
        TRIM(cst_firstname),
        TRIM(cst_lastname),
        CASE WHEN upper(trim(cst_marital_status)) = 'S' THEN 'Single'
             WHEN upper(trim(cst_marital_status)) = 'M' THEN 'Married'
             ELSE 'n/a' 
        END,
        CASE WHEN upper(trim(cst_gndr)) = 'F' THEN 'Female'
             WHEN upper(trim(cst_gndr)) = 'M' THEN 'Male'
             ELSE 'n/a' 
        END,
        cst_create_date
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
        FROM bronze.crm_cust_info 
        WHERE cst_id IS NOT NULL
    ) t
    WHERE flag_last = 1;
    v_end_time := now();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (v_end_time - v_start_time))::int;
    RAISE NOTICE '>> -------------';

    -- CRM Product Info
    v_start_time := now();
    RAISE NOTICE '>> Truncating Table: silver.crm_prd_info';
    TRUNCATE TABLE silver.crm_prd_info;
    RAISE NOTICE '>> Inserting Data Into: silver.crm_prd_info';
    INSERT INTO silver.crm_prd_info (
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    )
    SELECT
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_'),
        SUBSTRING(prd_key, 7, LENGTH(prd_key)),
        prd_nm,
        COALESCE(prd_cost, 0),
        CASE UPPER(TRIM(prd_line)) 
            WHEN 'M' THEN 'Mountain'
            WHEN 'R' THEN 'Road'
            WHEN 'S' THEN 'Other Sales'
            WHEN 'T' THEN 'Touring'
            ELSE 'n/a'
        END,
        CAST(prd_start_dt AS DATE),
        CAST(
            LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL '1 day'
            AS DATE
        )
    FROM bronze.crm_prd_info;
    v_end_time := now();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (v_end_time - v_start_time))::int;
    RAISE NOTICE '>> -------------';

    -- CRM Sales Details
    v_start_time := now();
    RAISE NOTICE '>> Truncating Table: silver.crm_sales_details';
    TRUNCATE TABLE silver.crm_sales_details;
    RAISE NOTICE '>> Inserting Data Into: silver.crm_sales_details';
    INSERT INTO silver.crm_sales_details (
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
    )
    SELECT 
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE
            WHEN sls_order_dt = 0 OR char_length(CAST(sls_order_dt AS TEXT)) != 8 THEN NULL
            ELSE TO_DATE(CAST(sls_order_dt AS TEXT), 'YYYYMMDD')
        END,
        CASE
            WHEN sls_ship_dt = 0 OR char_length(CAST(sls_ship_dt AS TEXT)) != 8 THEN NULL
            ELSE TO_DATE(CAST(sls_ship_dt AS TEXT), 'YYYYMMDD')
        END,
        CASE
            WHEN sls_due_dt = 0 OR char_length(CAST(sls_due_dt AS TEXT)) != 8 THEN NULL
            ELSE TO_DATE(CAST(sls_due_dt AS TEXT), 'YYYYMMDD')
        END,
        CASE 
            WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
                THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END,
        sls_quantity,
        CASE 
            WHEN sls_price IS NULL OR sls_price <= 0 
                THEN sls_sales / NULLIF(sls_quantity, 0)
            ELSE sls_price
        END
    FROM bronze.crm_sales_details;
    v_end_time := now();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (v_end_time - v_start_time))::int;
    RAISE NOTICE '>> -------------';

    -- ERP Customer Table
    v_start_time := now();
    RAISE NOTICE '>> Truncating Table: silver.erp_cust_az12';
    TRUNCATE TABLE silver.erp_cust_az12;
    RAISE NOTICE '>> Inserting Data Into: silver.erp_cust_az12';
    INSERT INTO silver.erp_cust_az12 (
        cid,
        bdate,
        gen
    )
    SELECT
        CASE
            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
            ELSE cid
        END,
        CASE
            WHEN bdate > CURRENT_DATE THEN NULL
            ELSE bdate
        END,
        CASE
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            ELSE 'n/a'
        END
    FROM bronze.erp_cust_az12;
    v_end_time := now();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (v_end_time - v_start_time))::int;
    RAISE NOTICE '>> -------------';

    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '------------------------------------------------';

    -- ERP Location Table
    v_start_time := now();
    RAISE NOTICE '>> Truncating Table: silver.erp_loc_a101';
    TRUNCATE TABLE silver.erp_loc_a101;
    RAISE NOTICE '>> Inserting Data Into: silver.erp_loc_a101';
    INSERT INTO silver.erp_loc_a101 (
        cid,
        cntry
    )
    SELECT
        REPLACE(cid, '-', ''),
        CASE
            WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
            WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
            ELSE TRIM(cntry)
        END
    FROM bronze.erp_loc_a101;
    v_end_time := now();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (v_end_time - v_start_time))::int;
    RAISE NOTICE '>> -------------';

    -- ERP PX CAT Table
    v_start_time := now();
    RAISE NOTICE '>> Truncating Table: silver.erp_px_cat_g1v2';
    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    RAISE NOTICE '>> Inserting Data Into: silver.erp_px_cat_g1v2';
    INSERT INTO silver.erp_px_cat_g1v2 (
        id,
        cat,
        subcat,
        maintenance
    )
    SELECT
        id,
        cat,
        subcat,
        maintenance
    FROM bronze.erp_px_cat_g1v2;
    v_end_time := now();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (v_end_time - v_start_time))::int;
    RAISE NOTICE '>> -------------';

    v_batch_end_time := now();
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Loading Silver Layer is Completed';
    RAISE NOTICE '   - Total Load Duration: % seconds', EXTRACT(EPOCH FROM (v_batch_end_time - v_batch_start_time))::int;
    RAISE NOTICE '==========================================';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '==========================================';
        RAISE NOTICE 'ERROR OCCURRED DURING LOADING SILVER LAYER';
        RAISE NOTICE 'Error SQLSTATE: %', SQLSTATE;
        RAISE NOTICE 'Error Message: %', SQLERRM;
        RAISE NOTICE '==========================================';
        -- Optionally: RAISE; -- uncomment to fail the procedure hard
END;
$$;
