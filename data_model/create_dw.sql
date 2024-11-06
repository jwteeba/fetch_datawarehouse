/**
Created by: Jah-Wilson Teeba
Date: 2024-11-06
Designed for Amazon Redshift Data Warehouse

DIM_USER is related to FACT_RECEIPT through the user_id field.
DIM_CPG is related to DIM_BRAND through the cpg_id field.
DIM_BRAND is related to FACT_RECEIPT_ITEM through the brand_id field.
DIM_DATE is related to FACT_RECEIPT through various date_id fields (purchase_date_id, scan_date_id, finish_date_id, points_awarded_date_id).
FACT_RECEIPT is related to FACT_RECEIPT_ITEM through the receipt_id field.
The FACT_RECEIPT_ITEM table serves as a bridge table between FACT_RECEIPT and DIM_BRAND. It contains the detailed information about each item in a receipt, allowing for a many-to-many relationship between receipts and brands.
This bridge table design allows for:
Multiple items from the same brand on a single receipt.
Multiple brands on a single receipt.
Detailed item-level information to be stored without repeating it in the main FACT_RECEIPT table.
The FACT_RECEIPT_ITEM table uses DISTSTYLE KEY with receipt_id as the DISTKEY to keep all items from the same receipt on the same node, optimizing for queries that analyze all items in a receipt.

DIM_USER
|
|---- 0..* FACT_RECEIPT (One user can have zero to many receipts)
|
DIM_DATE
|
|---- 0..* FACT_RECEIPT (purchase_date) (One date can be associated with zero to many receipts as purchase date)
|---- 0..* FACT_RECEIPT (scan_date) (One date can be associated with zero to many receipts as scan date)
|---- 0..* FACT_RECEIPT (finish_date) (One date can be associated with zero to many receipts as finish date)
|---- 0..* FACT_RECEIPT (points_awarded_date) (One date can be associated with zero to many receipts as points awarded date)
|
DIM_CPG
|
|---- 1..* DIM_BRAND (One CPG can have one to many brands)
     |
     |---- 0..* FACT_RECEIPT_ITEM (One brand can be associated with zero to many receipt items)
           |
           |---- 1 FACT_RECEIPT (Each receipt item belongs to exactly one receipt)


**/



-- Dimension Tables - Added DISTSTYLE ALL for dimension tables as it replicates the entire table to all nodes, which can speed up joins.

-- DIM_USER
CREATE TABLE DIM_USER (
    user_id VARCHAR(45) PRIMARY KEY,
    active BOOLEAN,
    created_date TIMESTAMP,
    last_login TIMESTAMP,
    role VARCHAR(50),
    sign_up_source VARCHAR(50),
    state VARCHAR(2)
)
DISTSTYLE ALL
SORTKEY (user_id);

-- DIM_CPG (Consumer Packaged Goods)
CREATE TABLE DIM_CPG (
    cpg_id VARCHAR(24) PRIMARY KEY,
    name VARCHAR(255)
)
DISTSTYLE ALL
SORTKEY (cpg_id);

-- DIM_BRAND
CREATE TABLE DIM_BRAND (
    brand_id VARCHAR(45) PRIMARY KEY,
    cpg_id VARCHAR(45) REFERENCES DIM_CPG(cpg_id),
    barcode VARCHAR(50),
    brand_code VARCHAR(50),
    category VARCHAR(100),
    category_code VARCHAR(50),
    name VARCHAR(255),
    top_brand BOOLEAN
)
DISTSTYLE ALL
SORTKEY (brand_id);

-- DIM_DATE
CREATE TABLE DIM_DATE (
    date_id INT PRIMARY KEY,
    full_date DATE,
    year INT,
    month INT,
    day INT,
    quarter INT,
    day_of_week INT,
    is_weekend BOOLEAN
)
DISTSTYLE ALL
SORTKEY (date_id);

-- Fact Table - Foreign key references are included, for query planning and are not enforced in Redshift

-- FACT_RECEIPT
CREATE TABLE FACT_RECEIPT (
    receipt_id VARCHAR(45) PRIMARY KEY,
    user_id VARCHAR(45) REFERENCES DIM_USER(user_id),
    purchase_date_id INT REFERENCES DIM_DATE(date_id),
    scan_date_id INT REFERENCES DIM_DATE(date_id),
    finish_date_id INT REFERENCES DIM_DATE(date_id),
    points_awarded_date_id INT REFERENCES DIM_DATE(date_id),
    bonus_points_earned INT,
    bonus_points_earned_reason VARCHAR(255),
    points_earned DECIMAL(10,2),
    purchased_item_count INT,
    total_spent DECIMAL(10,2),
    receipt_status VARCHAR(50)
)
DISTSTYLE KEY
DISTKEY (user_id)
COMPOUND SORTKEY (purchase_date_id, scan_date_id);

-- Bridge Table for Receipt Items
-- FACT_RECEIPT_ITEM
CREATE TABLE FACT_RECEIPT_ITEM (
    receipt_id VARCHAR(45) REFERENCES FACT_RECEIPT(receipt_id),
    brand_id VARCHAR(45) REFERENCES DIM_BRAND(brand_id),
    barcode VARCHAR(50),
    description VARCHAR(255),
    final_price DECIMAL(10,2),
    item_price DECIMAL(10,2),
    quantity_purchased INT,
    needs_fetch_review BOOLEAN,
    partner_item_id VARCHAR(50),
    prevent_target_gap_points BOOLEAN,
    user_flagged_barcode VARCHAR(50),
    user_flagged_new_item BOOLEAN,
    user_flagged_price DECIMAL(10,2),
    user_flagged_quantity INT,
    PRIMARY KEY (receipt_id, brand_id, barcode)
)
DISTSTYLE KEY
DISTKEY (receipt_id)
COMPOUND SORTKEY (receipt_id, brand_id);

