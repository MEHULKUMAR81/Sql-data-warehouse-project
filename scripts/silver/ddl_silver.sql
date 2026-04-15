CREATE TABLE silver_crm_cust_info (
    cst_id INT,
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_maritial_status VARCHAR(50),
    cst_gender VARCHAR(50),
    cst_create_date DATE,
    dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);
drop table if exists silver_crm_prd_info;
CREATE TABLE silver_crm_prd_info (
    prd_id INT,
    cat_id varchar(50),
    prd_key VARCHAR(50),
    prd_nm VARCHAR(50),
    prd_cost int ,
    prd_line VARCHAR(50),
	prd_start_date VARCHAR(50),
   prd_end_date DATE,
   dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);
drop table if exists silver_crm_sales_details;
CREATE TABLE silver_crm_sales_details (
    sls_ord_num varchar (50),
    sls_prd_key  VARCHAR(50),
    sls_cust_id int,
    sls_order_dt date,
    sls_ship_dt date ,
    sls_due_dt date ,
    sls_sales int,
    sls_quant int ,
    sls_price int ,
    dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE silver_erp_cust_az12 (
    cid varchar (50),
    bdate date ,
    gen varchar (50),
    dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE silver_erp_local101(
     cid varchar (50),
     cntry varchar (50),
      dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE silver_erp_px_cat_g1v2 (
    id varchar (50),
    cat varchar (50),
    subcat varchar (50),
    maintenace varchar(50),
    dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);



