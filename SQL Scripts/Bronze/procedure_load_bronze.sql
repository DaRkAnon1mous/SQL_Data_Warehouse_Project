/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `COPY` command to copy data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL bronze.load_bronze();
===============================================================================
*/
DROP PROCEDURE IF EXISTS bronze.load_bronze();

CREATE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
  v_start_time       timestamp;
  v_end_time         timestamp;
  v_batch_start_time timestamp;
  v_batch_end_time   timestamp;
  v_rowcount         bigint;
BEGIN
  v_batch_start_time := now();
  RAISE NOTICE '============';
  RAISE NOTICE 'Loading Bronze Layer';
  RAISE NOTICE '============';

  RAISE NOTICE '------------------------------------------------';
  RAISE NOTICE 'Loading CRM Tables';
  RAISE NOTICE '------------------------------------------------';

  -- 1) bronze.crm_cust_info
  v_start_time := now();
  RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
  TRUNCATE TABLE bronze.crm_cust_info;

  RAISE NOTICE '>> Inserting Data Into: bronze.crm_cust_info';
  COPY bronze.crm_cust_info
  FROM 'D:\Coding Workspaces\Data_Warehouse_Scratch_Project\datasets\source_crm\cust_info.csv'
  WITH (FORMAT csv, HEADER true, DELIMITER ',');

  SELECT COUNT(*) INTO v_rowcount FROM bronze.crm_cust_info;
  v_end_time := now();
  RAISE NOTICE '>> Rows loaded: %', v_rowcount;
  RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (v_end_time - v_start_time))::bigint;
  RAISE NOTICE '>> -------------';

  -- 2) bronze.crm_prd_info
  v_start_time := now();
  RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
  TRUNCATE TABLE bronze.crm_prd_info;

  RAISE NOTICE '>> Inserting Data Into: bronze.crm_prd_info';
  COPY bronze.crm_prd_info
  FROM 'D:\Coding Workspaces\Data_Warehouse_Scratch_Project\datasets\source_crm\prd_info.csv'
  WITH (FORMAT csv, HEADER true, DELIMITER ',');

  SELECT COUNT(*) INTO v_rowcount FROM bronze.crm_prd_info;
  v_end_time := now();
  RAISE NOTICE '>> Rows loaded: %', v_rowcount;
  RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (v_end_time - v_start_time))::bigint;
  RAISE NOTICE '>> -------------';

  -- 3) bronze.crm_sales_details
  v_start_time := now();
  RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
  TRUNCATE TABLE bronze.crm_sales_details;

  RAISE NOTICE '>> Inserting Data Into: bronze.crm_sales_details';
  COPY bronze.crm_sales_details
  FROM 'D:\Coding Workspaces\Data_Warehouse_Scratch_Project\datasets\source_crm\sales_details.csv'
  WITH (FORMAT csv, HEADER true, DELIMITER ',');

  SELECT COUNT(*) INTO v_rowcount FROM bronze.crm_sales_details;
  v_end_time := now();
  RAISE NOTICE '>> Rows loaded: %', v_rowcount;
  RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (v_end_time - v_start_time))::bigint;
  RAISE NOTICE '>> -------------';

  RAISE NOTICE '------------------------------------------------';
  RAISE NOTICE 'Loading ERP Tables';
  RAISE NOTICE '------------------------------------------------';

  -- 4) bronze.erp_loc_a101
  v_start_time := now();
  RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
  TRUNCATE TABLE bronze.erp_loc_a101;

  RAISE NOTICE '>> Inserting Data Into: bronze.erp_loc_a101';
  COPY bronze.erp_loc_a101
  FROM 'D:\Coding Workspaces\Data_Warehouse_Scratch_Project\datasets\source_erp\LOC_A101.csv'
  WITH (FORMAT csv, HEADER true, DELIMITER ',');

  SELECT COUNT(*) INTO v_rowcount FROM bronze.erp_loc_a101;
  v_end_time := now();
  RAISE NOTICE '>> Rows loaded: %', v_rowcount;
  RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (v_end_time - v_start_time))::bigint;
  RAISE NOTICE '>> -------------';

  -- 5) bronze.erp_cust_az12
  v_start_time := now();
  RAISE NOTICE '>> Truncating Table: bronze.erp_cust_az12';
  TRUNCATE TABLE bronze.erp_cust_az12;

  RAISE NOTICE '>> Inserting Data Into: bronze.erp_cust_az12';
  COPY bronze.erp_cust_az12
  FROM 'D:\Coding Workspaces\Data_Warehouse_Scratch_Project\datasets\source_erp\CUST_AZ12.csv'
  WITH (FORMAT csv, HEADER true, DELIMITER ',');

  SELECT COUNT(*) INTO v_rowcount FROM bronze.erp_cust_az12;
  v_end_time := now();
  RAISE NOTICE '>> Rows loaded: %', v_rowcount;
  RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (v_end_time - v_start_time))::bigint;
  RAISE NOTICE '>> -------------';

  -- 6) bronze.erp_px_cat_g1v2
  v_start_time := now();
  RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
  TRUNCATE TABLE bronze.erp_px_cat_g1v2;

  RAISE NOTICE '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
  COPY bronze.erp_px_cat_g1v2
  FROM 'D:\Coding Workspaces\Data_Warehouse_Scratch_Project\datasets\source_erp\PX_CAT_G1V2.csv'
  WITH (FORMAT csv, HEADER true, DELIMITER ',');

  SELECT COUNT(*) INTO v_rowcount FROM bronze.erp_px_cat_g1v2;
  v_end_time := now();
  RAISE NOTICE '>> Rows loaded: %', v_rowcount;
  RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (v_end_time - v_start_time))::bigint;
  RAISE NOTICE '>> -------------';

  v_batch_end_time := now();
  RAISE NOTICE '==========';
  RAISE NOTICE 'Loading Bronze Layer is Completed';
  RAISE NOTICE '   - Total Load Duration: % seconds',
               EXTRACT(EPOCH FROM (v_batch_end_time - v_batch_start_time))::bigint;
  RAISE NOTICE '==========';

EXCEPTION
  WHEN OTHERS THEN
    RAISE NOTICE '==========';
    RAISE NOTICE 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
    RAISE NOTICE 'Error SQLSTATE: %', SQLSTATE;
    RAISE NOTICE 'Error Message: %', SQLERRM;
    RAISE NOTICE '==========';
    -- Optionally re-raise to fail the call:
    -- RAISE;
END;
$$;
