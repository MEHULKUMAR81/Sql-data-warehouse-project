
/* ============================================================
   GOLD LAYER - DIMENSION & FACT MODELING
   ============================================================ */


/* ============================================================
   CUSTOMER DIMENSION (gold_dim_customers)
   - Generates surrogate key
   - Integrates CRM + ERP customer + location data
   - Applies gender reconciliation logic
   ============================================================ */

CREATE VIEW gold_dim_customers AS 
SELECT 
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,   -- surrogate key

    ci.cst_id        AS customer_id,
    ci.cst_key       AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname  AS last_name,

    el.cntry         AS country,

    ci.cst_maritial_status AS marital_status,

    /* Gender integration logic:
       - Prefer CRM value if valid
       - Else fallback to ERP value
       - Default to 'n/a'
    */
    CASE 
        WHEN ci.cst_gender != 'n/a' THEN ci.cst_gender 
        ELSE COALESCE(ec.gen, 'n/a')
    END AS gender,

    ec.bdate              AS birthdate,
    ci.cst_create_date    AS create_date

FROM silver_crm_cust_info ci

JOIN silver_erp_cust_az12 ec 
    ON ci.cst_key = ec.cid

JOIN silver_erp_local101 el 
    ON ec.cid = el.cid;



/* ============================================================
   VALIDATION CHECK - GENDER CONSISTENCY
   - Compare CRM vs ERP gender values
   - Validate final integrated gender logic
   ============================================================ */

SELECT DISTINCT 
    ci.cst_gender,
    ec.gen,

    CASE 
        WHEN ci.cst_gender != 'n/a' THEN ci.cst_gender 
        ELSE COALESCE(ec.gen, 'n/a')
    END AS new_gen

FROM silver_crm_cust_info ci

JOIN silver_erp_cust_az12 ec 
    ON ci.cst_key = ec.cid

JOIN silver_erp_local101 el 
    ON ec.cid = el.cid;



/* ============================================================
   PRODUCT DIMENSION (gold_dim_products)
   - Generates surrogate key
   - Joins CRM product data with ERP category hierarchy
   - Filters only current (active) products
   ============================================================ */

CREATE VIEW gold_dim_products AS 

SELECT
    ROW_NUMBER() OVER (ORDER BY pi.prd_key, pi.prd_start_date) AS product_key,  -- surrogate key

    pi.prd_id        AS product_id,
    pi.cat_id        AS category_id,
    pi.prd_key       AS product_number,
    pi.prd_nm        AS product_name,
    pi.prd_line      AS product_line,
    pi.prd_start_date AS product_start_date,
    pi.prd_cost      AS product_cost,

    ep.maintenace    AS product_maintenance,
    ep.subcat        AS subproduct_category

FROM silver_crm_prd_info pi

JOIN silver_erp_px_cat_g1v2 ep
    ON pi.cat_id = ep.id

WHERE pi.prd_end_date IS NULL;   -- filtering out historical records



/* ============================================================
   FACT TABLE (gold_fact_sales)
   - Transaction-level sales data
   - Links to dimension tables via surrogate keys
   ============================================================ */

CREATE VIEW gold_fact_sales AS  

SELECT 
    cs.sls_ord_num,

    pr.product_key,   -- surrogate key from product dimension
    gc.customer_key,  -- surrogate key from customer dimension

    cs.sls_order_dt,
    cs.sls_ship_dt,
    cs.sls_due_dt,

    cs.sls_sales,
    cs.sls_quant,
    cs.sls_price

FROM silver_crm_sales_details cs

JOIN gold_dim_products pr
    ON cs.sls_prd_key = pr.product_number 

JOIN gold_dim_customers gc
    ON cs.sls_cust_id = gc.customer_id;



/* ============================================================
   VALIDATION CHECK - FACT ↔ DIMENSION INTEGRITY
   - Ensures no missing customer_key after join
   ============================================================ */

-- check 

SELECT *
FROM gold_fact_sales f 

JOIN gold_dim_customers c
    ON f.customer_key = c.customer_key

WHERE c.customer_key IS NULL;
