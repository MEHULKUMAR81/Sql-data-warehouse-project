/* =========================================================
   SILVER LAYER QUALITY CHECKS + TRANSFORMATION LOAD SCRIPT
   Project   : sql_project
   Purpose   : 
     1. Run table-wise data quality checks on bronze tables
     2. Apply cleaning and standardization logic
     3. Load cleaned records into silver layer tables

   ========================================================= */



/* =========================================================
   SECTION 1: CUSTOMER DATA
   Source Table : crm_cust_info
   Target Table : silver_crm_cust_info
   ========================================================= */

-- ---------------------------------------------------------
-- Quality Check 1: Check for nulls or duplicates in primary key
-- ---------------------------------------------------------
SELECT cst_id, COUNT(*)
FROM sql_project.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;


-- ---------------------------------------------------------
-- Quality Check 2: Review latest customer record using ROW_NUMBER
-- ---------------------------------------------------------
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY cst_id
               ORDER BY cst_create_date DESC
           ) AS rn
    FROM sql_project.crm_cust_info
) t
WHERE rn = 1;


-- ---------------------------------------------------------
-- Quality Check 3: Check for unwanted spaces in first name
-- ---------------------------------------------------------
SELECT cst_firstname
FROM crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);


-- ---------------------------------------------------------
-- Quality Check 4: Check for unwanted spaces in last name
-- ---------------------------------------------------------
SELECT cst_lastname
FROM crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);


-- ---------------------------------------------------------
-- Load Cleaned Customer Data into Silver
-- ---------------------------------------------------------
TRUNCATE TABLE silver_crm_cust_info;

INSERT INTO silver_crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_maritial_status,
    cst_gender,
    cst_create_date
)
SELECT 
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    CASE 
        WHEN UPPER(TRIM(cst_maritial_status)) = 'S' THEN 'Single' 
        WHEN UPPER(TRIM(cst_maritial_status)) = 'M' THEN 'Married'
        ELSE 'n/a'
    END cst_maritial_status,
    CASE 
        WHEN UPPER(TRIM(cst_gender)) = 'F' THEN 'Female' 
        WHEN UPPER(TRIM(cst_gender)) = 'M' THEN 'Male'
        ELSE 'n/a'
    END cst_gender,
    cst_create_date
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY cst_id
               ORDER BY cst_create_date DESC
           ) AS rn
    FROM sql_project.crm_cust_info
) t
WHERE rn = 1
  AND cst_id IS NOT NULL;



/* =========================================================
   SECTION 2: PRODUCT DATA
   Source Table : crm_prd_info
   Target Table : silver_crm_prd_info
   ========================================================= */

-- ---------------------------------------------------------
-- Quality Check 1: Check duplicate product IDs
-- ---------------------------------------------------------
SELECT prd_id, COUNT(*)
FROM crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1;


-- ---------------------------------------------------------
-- Quality Check 2: Same prd_key has multiple prd_id values
-- ---------------------------------------------------------
SELECT 
    prd_id,
    prd_key,
    ROW_NUMBER() OVER (PARTITION BY prd_key) AS a
FROM crm_prd_info;


-- ---------------------------------------------------------
-- Quality Check 3: Check negative or null product cost values
-- ---------------------------------------------------------
SELECT 
    prd_cost,
    prd_id,
    prd_key
FROM crm_prd_info
WHERE prd_cost < 0
   OR prd_cost IS NULL;


-- ---------------------------------------------------------
-- Quality Check 4: Date validation
-- ---------------------------------------------------------
SELECT *
FROM crm_prd_info
WHERE prd_end_date < prd_start_date;


-- ---------------------------------------------------------
-- Correction thought:
-- maybe editing the end dates as starting dates from subsequent rows
-- and subtract 1
-- ---------------------------------------------------------



-- ---------------------------------------------------------
-- Load Cleaned Product Data into Silver
-- ---------------------------------------------------------
TRUNCATE TABLE silver_crm_prd_info;

INSERT INTO silver_crm_prd_info (
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_date,
    prd_end_date
)
SELECT 
    prd_id,
    REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
    SUBSTRING(prd_key,7,LENGTH(prd_key)) AS prd_key,
    TRIM(prd_nm) AS prd_nm,
    COALESCE(prd_cost,0) AS prd_cost,
    CASE UPPER(TRIM(prd_line))
        WHEN 'M' THEN 'Mountain'
        WHEN 'R' THEN 'Road'
        WHEN 'S' THEN 'other sales'
        WHEN 'T' THEN 'touring'
        ELSE 'n/a'
    END AS prd_line,
    prd_start_date,
    DATE_SUB(
        LEAD(prd_start_date) OVER (
            PARTITION BY prd_key
            ORDER BY prd_start_date ASC
        ),
        INTERVAL 1 DAY
    ) AS prd_end_date
FROM crm_prd_info;



/* =========================================================
   SECTION 3: SALES DATA
   Source Table : crm_sales_details
   Target Table : silver_crm_sales_details
   ========================================================= */

-- ---------------------------------------------------------
-- Quality / Cleaning / Transformation Load
-- Check date boundaries, malformed dates, and sales-price logic
-- ---------------------------------------------------------
TRUNCATE TABLE silver_crm_sales_details;

INSERT INTO silver_crm_sales_details (
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quant,
    sls_price
)
SELECT 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    
    CASE 
        WHEN sls_order_dt = 0 
             OR LENGTH(CAST(sls_order_dt AS CHAR)) != 8 
        THEN NULL
        ELSE STR_TO_DATE(sls_order_dt, '%Y%m%d')
    END AS sls_order_dt,

    CASE 
        WHEN sls_ship_dt = 0 
             OR LENGTH(CAST(sls_ship_dt AS CHAR)) != 8 
        THEN NULL
        ELSE STR_TO_DATE(sls_ship_dt, '%Y%m%d')
    END AS sls_ship_dt,
	  
    CASE 
        WHEN sls_due_dt = 0 
             OR LENGTH(CAST(sls_due_dt AS CHAR)) != 8 
        THEN NULL
        ELSE STR_TO_DATE(sls_due_dt, '%Y%m%d')
    END AS sls_due_dt,

    CASE 
        WHEN sls_price <= 0 
             OR sls_price IS NULL 
             OR sls_sales != sls_quant * ABS(sls_price)
        THEN ABS(sls_price) * sls_quant
        ELSE sls_sales
    END AS sls_sales,
   
    NULLIF(sls_quant, 0) AS sls_quant,
   
    CASE  
        WHEN sls_price < 0 OR sls_price IS NULL 
        THEN sls_sales / NULLIF(sls_quant, 0)
        ELSE sls_price
    END AS sls_price
FROM crm_sales_details;



/* =========================================================
   SECTION 4: ERP CUSTOMER MASTER DATA
   Source Table : erp_cust_az12
   Target Table : silver_erp_cust_az12
   ========================================================= */

-- ---------------------------------------------------------
-- Quality Check 1: Duplicate customer IDs
-- ---------------------------------------------------------
SELECT cid, COUNT(*)
FROM erp_cust_az12
GROUP BY cid
HAVING COUNT(*) > 1;


-- ---------------------------------------------------------
-- Load Cleaned ERP Customer Data into Silver
-- ---------------------------------------------------------
TRUNCATE TABLE silver_erp_cust_az12;

INSERT INTO silver_erp_cust_az12 (
    cid,
    bdate,
    gen
)
SELECT 
    CASE 
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4)
        ELSE cid
    END AS cid,

    CASE 
        WHEN bdate > CURDATE() THEN NULL 
        ELSE bdate 
    END AS bdate,

    CASE 
        WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male' 
        WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
        ELSE 'N/A'
    END AS gen
FROM erp_cust_az12;


-- ---------------------------------------------------------
-- Quality Check 2: Distinct raw vs cleaned gender values
-- ---------------------------------------------------------
SELECT DISTINCT 
    gen,
    CASE 
        WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male' 
        WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
        ELSE 'N/A'
    END AS gen
FROM erp_cust_az12;



/* =========================================================
   SECTION 5: ERP LOCATION MASTER DATA
   Source Table : erp_loca101
   Target Table : silver_erp_local101
   ========================================================= */

-- ---------------------------------------------------------
-- Load Cleaned ERP Location Data into Silver
-- ---------------------------------------------------------
TRUNCATE TABLE silver_erp_local101;

INSERT INTO silver_erp_local101 (
    cid,
    cntry
)
SELECT 
    REPLACE(cid, '-', '') AS cid,
    CASE
        WHEN TRIM(cntry) = 'usa' THEN 'United states'
        WHEN TRIM(cntry) = 'US' THEN 'United states'
        WHEN TRIM(cntry) = 'DE' THEN 'Germany'
        WHEN TRIM(cntry) = '' THEN 'n/a'
        WHEN cntry IS NULL THEN 'n/a'
        ELSE TRIM(cntry)
    END AS cntry
FROM erp_loca101 e;



/* =========================================================
   SECTION 6: ERP PRODUCT CATEGORY MASTER DATA
   Source Table : erp_px_cat_g1v2
   Target Table : silver_erp_px_cat_g1v2
   ========================================================= */

-- ---------------------------------------------------------
-- Load ERP Product Category Data into Silver
-- ---------------------------------------------------------
TRUNCATE TABLE silver_erp_px_cat_g1v2;

INSERT INTO silver_erp_px_cat_g1v2 (
    id,
    cat,
    subcat,
    maintenace
)
SELECT
    id,
    cat,
    subcat,
    maintenace
FROM erp_px_cat_g1v2 e;



