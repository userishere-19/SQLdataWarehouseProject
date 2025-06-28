/*
==============================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
==============================================================

Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;

==============================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN 
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME,@batch_end_time DATETIME;
	BEGIN TRY
			
			SET @batch_start_time = GETDATE();
			PRINT '=========================================='
			PRINT 'LOADING BRONZE LAYER'
			PRINT '=========================================='

			PRINT '=========================================='
			PRINT 'Loading CRM Table'
			PRINT '=========================================='

			SET @start_time = GETDATE();
			PRINT '>> TRUNCATING Table: bronze.crm_cust_info'
			TRUNCATE TABLE bronze.crm_cust_info;
			PRINT '>> INSERTING Table: bronze.crm_cust_info'
			BULK INSERT bronze.crm_cust_info
			FROM 'C:\Users\abhis\OneDrive\Desktop\DW Project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
			WITH (
				FIRSTROW = 2, 
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			SET @end_time = GETDATE();
			PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
			PRINT '------------------------------------------'

			SET @start_time = GETDATE();
			PRINT '>> TRUNCATING Table: bronze.crm_prd_info'
			TRUNCATE TABLE bronze.crm_prd_info;
			PRINT '>> INSERTING Table: bronze.crm_prd_info'
			BULK INSERT bronze.crm_prd_info
			FROM 'C:\Users\abhis\OneDrive\Desktop\DW Project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
			WITH (
				FIRSTROW = 2, 
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			SET @end_time = GETDATE();
			PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
			PRINT '------------------------------------------'

			SET @start_time = GETDATE();
			PRINT '>> TRUNCATING Table: bronze.crm_sales_details'
			TRUNCATE TABLE bronze.crm_sales_details;
			PRINT '>> INSERTING Table: bronze.crm_sales_details'
			BULK INSERT bronze.crm_sales_details
			FROM 'C:\Users\abhis\OneDrive\Desktop\DW Project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
			WITH (
				FIRSTROW = 2, 
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			SET @end_time = GETDATE();
			PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
			PRINT '------------------------------------------'
			
			PRINT '=========================================='
			PRINT 'Loading ERP Table'
			PRINT '=========================================='

			SET @start_time = GETDATE();
			PRINT '>> TRUNCATING Table: bronze.erp_loc_a101'	
			TRUNCATE TABLE bronze.erp_loc_a101;
			PRINT '>> INSERTING Table: bronze.erp_loc_a101'
			BULK INSERT bronze.erp_loc_a101
			FROM 'C:\Users\abhis\OneDrive\Desktop\DW Project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
			WITH (
				FIRSTROW = 2, 
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			SET @end_time = GETDATE();
			PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
			PRINT '------------------------------------------'

			SET @start_time = GETDATE();
			PRINT '>> TRUNCATING Table: bronze.erp_cust_az12'	
			TRUNCATE TABLE bronze.erp_cust_az12;
			PRINT '>> INSERTING Table: bronze.erp_cust_az12'
			BULK INSERT bronze.erp_cust_az12
			FROM 'C:\Users\abhis\OneDrive\Desktop\DW Project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
			WITH (
				FIRSTROW = 2, 
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			SET @end_time = GETDATE();
			PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
			PRINT '------------------------------------------'

			SET @start_time = GETDATE();
			PRINT '>> TRUNCATING Table: bronze.erp_px_cat_glv2'	
			TRUNCATE TABLE bronze.erp_px_cat_glv2;
			PRINT '>> INSERTING Table: bronze.erp_px_cat_glv2'
			BULK INSERT bronze.erp_px_cat_glv2
			FROM 'C:\Users\abhis\OneDrive\Desktop\DW Project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
			WITH (
				FIRSTROW = 2, 
				FIELDTERMINATOR = ',',
				TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
		PRINT '------------------------------------------'

		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'LOADING BATCH LAYER COMPLETED';
		PRINT '>> TOTAL DURATION:' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' Seconds';
		PRINT '=========================================='

	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING'
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT 'ERROR MESSAGE' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR MESSAGE' + CAST (ERROR_STATE() AS NVARCHAR);

		PRINT '=========================================='
	END CATCH
END
