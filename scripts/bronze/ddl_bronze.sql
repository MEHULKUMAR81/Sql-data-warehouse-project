
/* ============================================================
   BRONZE LAYER - DATA INGESTION SCRIPT
   Project   : sql_project
   Purpose   :
     1. Truncate existing raw tables
     2. Load CSV data using LOAD DATA INFILE
     3. Apply basic ingestion cleaning (TRIM, NULLIF, STR_TO_DATE)
   ============================================================ */



/* ============================================================
   SECTION 1: CRM CUSTOMER DATA LOAD
   Source File : cust_info.csv
   Target Table: crm_cust_info
   ============================================================ */

-- Clear existing data
TRUNCATE TABLE crm_cust_info;

-- Load and clean customer data
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 9.6/Uploads/cust_info.csv'
INTO TABLE crm_cust_info
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(@cst_id, @cst_key, @cst_firstname, @cst_lastname, cst_maritial_status, @cst_gender, @cst_create_date)
SET 
    cst_id           = NULLIF(TRIM(@cst_id), ''),
    cst_key          = NULLIF(TRIM(@cst_key), ''),
    cst_firstname    = TRIM(@cst_firstname),
    cst_lastname     = TRIM(@cst_lastname),
    cst_gender       = NULLIF((@cst_gender), ''),
    cst_create_date  = STR_TO_DATE(NULLIF(TRIM(@cst_create_date), ''), '%d-%m-%Y');



/* ============================================================
   SECTION 2: CRM PRODUCT DATA LOAD
   Source File : prd_info.csv
   Target Table: crm_prd_info
   ============================================================ */

-- Clear existing data
TRUNCATE TABLE crm_prd_info;

-- Load and clean product data
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 9.6/Uploads/prd_info.csv'
INTO TABLE crm_prd_info
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(@prd_id, @prd_key, @prd_nm, @prd_cost, @prd_line, @prd_start_date, @prd_end_date)
SET
    prd_id          = NULLIF(TRIM(@prd_id), ''),
    prd_key         = NULLIF(TRIM(@prd_key), ''),
    prd_nm          = NULLIF(TRIM(@prd_nm), ''),
    prd_cost        = NULLIF(TRIM(@prd_cost), ''),
    prd_line        = NULLIF(TRIM(@prd_line), ''),
    prd_start_date  = NULLIF(TRIM(@prd_start_date), ''),
    prd_end_date    = NULLIF(TRIM(@prd_end_date), '');



/* ============================================================
   SECTION 3: CRM SALES DATA LOAD
   Source File : sales_details.csv
   Target Table: crm_sales_details
   ============================================================ */

-- Clear existing data
TRUNCATE TABLE crm_sales_details;

-- Load and clean sales data
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 9.6/Uploads/sales_details.csv'
INTO TABLE crm_sales_details
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(@sls_ord_num, @sls_prd_key, @sls_cust_id, @sls_order_dt, @sls_ship_dt, @sls_due_dt, @sls_sales, @sls_quant, @sls_price)
SET
    sls_ord_num   = NULLIF(TRIM(@sls_ord_num), ''),
    sls_prd_key   = NULLIF(TRIM(@sls_prd_key), ''),
    sls_cust_id   = NULLIF(TRIM(@sls_cust_id), ''),
    sls_order_dt  = NULLIF(TRIM(@sls_order_dt), ''),
    sls_ship_dt   = NULLIF(TRIM(@sls_ship_dt), ''),
    sls_due_dt    = NULLIF(TRIM(@sls_due_dt), ''),
    sls_sales     = NULLIF(TRIM(@sls_sales), ''),
    sls_quant     = NULLIF(TRIM(@sls_quant), ''),
    sls_price     = NULLIF(TRIM(@sls_price), '');



/* ============================================================
   SECTION 4: ERP CUSTOMER MASTER DATA LOAD
   Source File : CUST_AZ12.csv
   Target Table: erp_cust_az12
   ============================================================ */

-- Clear existing data
TRUNCATE TABLE erp_cust_az12;

-- Load and clean ERP customer data
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 9.6/Uploads/CUST_AZ12.csv'
INTO TABLE erp_cust_az12
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(@CID, @BDATE, @GEN)
SET
    cid   = NULLIF(TRIM(@cid), ''),
    bdate = NULLIF(TRIM(@bdate), ''),
    gen   = NULLIF(TRIM(@gen), '');


/* ============================================================
   NOTE: REPEATED ERP LOAD BLOCK (AS PROVIDED)
   - Logic unchanged
   ============================================================ */

TRUNCATE TABLE erp_cust_az12;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 9.6/Uploads/CUST_AZ12.csv'
INTO TABLE erp_cust_az12
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(@CID, @BDATE, @GEN)
SET
    cid   = NULLIF(TRIM(@cid), ''),
    bdate = NULLIF(TRIM(@bdate), ''),
    gen   = NULLIF(TRIM(@gen), '');



/* ============================================================
   SECTION 5: ERP PRODUCT CATEGORY DATA LOAD
   Source File : PX_CAT_G1V2.csv
   Target Table: erp_px_cat_g1v2
   ============================================================ */

-- Clear existing data
TRUNCATE TABLE erp_px_cat_g1v2;

-- Load and clean ERP product category data
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 9.6/Uploads/PX_CAT_G1V2.csv'
INTO TABLE erp_px_cat_g1v2
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(@ID, @CAT, @SUBCAT, @MAINTENACE)
SET
    id           = NULLIF(TRIM(@id), ''),
    cat          = NULLIF(TRIM(@cat), ''),
    subcat       = NULLIF(TRIM(@subcat), ''),
    MAINTENACE   = NULLIF(TRIM(@MAINTENACE), '');
