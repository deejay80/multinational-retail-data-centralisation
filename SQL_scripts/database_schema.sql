-- task 1
ALTER TABLE orders_table
    ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid,
    ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
    ALTER COLUMN card_number TYPE VARCHAR(19),
    ALTER COLUMN store_code TYPE VARCHAR(12),
    ALTER COLUMN product_code TYPE VARCHAR(11),
    ALTER COLUMN product_quantity TYPE SMALLINT;

-- task 2

ALTER TABLE dim_users
    ALTER COLUMN first_name TYPE VARCHAR(255),
    ALTER COLUMN last_name TYPE VARCHAR(255),
    ALTER COLUMN date_of_birth TYPE DATE,
    ALTER COLUMN country_code TYPE VARCHAR(2),
    ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
    ALTER COLUMN join_date TYPE DATE;
-- task 3
-- update NULL values to 'N/A'
UPDATE dim_store_details
SET
  address = COALESCE(address, 'N/A'),
  longitude = COALESCE(NULLIF(CAST(longitude AS VARCHAR), 'N/A'), 'N/A'),
  locality = COALESCE(locality, 'N/A'),
  latitude = COALESCE(NULLIF(CAST(latitude AS VARCHAR), 'N/A'), 'N/A')
WHERE address IS NULL OR longitude IS NULL OR locality IS NULL OR latitude IS NULL;

-- alter column types
ALTER TABLE dim_store_details
  ALTER COLUMN longitude TYPE FLOAT USING (CASE WHEN longitude <> 'N/A' THEN longitude::REAL END),
  ALTER COLUMN locality TYPE VARCHAR(255),
  ALTER COLUMN store_code TYPE VARCHAR(12),
  ALTER COLUMN staff_numbers TYPE INTEGER USING (trim(staff_numbers)::INTEGER),
  ALTER COLUMN opening_date TYPE DATE,
  ALTER COLUMN store_type TYPE VARCHAR(255),
  ALTER COLUMN latitude TYPE FLOAT USING (CASE WHEN latitude <> 'N/A' THEN latitude::REAL END),
  ALTER COLUMN country_code TYPE VARCHAR(2),
  ALTER COLUMN continent TYPE VARCHAR(255);


--task 4

ALTER TABLE dim_products
ADD COLUMN weight_class TEXT,
ALTER COLUMN weight TYPE FLOAT USING (weight::REAL);

UPDATE dim_products
SET weight_class = 
    CASE
        WHEN weight < 2 THEN 'Light'
        WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
        WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
        ELSE 'Truck_Required'
    END;

--task 5
ALTER TABLE dim_products
RENAME COLUMN removed TO still_available;

UPDATE dim_products
SET still_available = 
    CASE
        WHEN still_available = 'Still_available' THEN TRUE
        WHEN still_available = 'Removed' THEN FALSE       
    END;

ALTER TABLE dim_products
    ALTER COLUMN product_price TYPE FLOAT USING product_price::REAL,
    ALTER COLUMN "EAN" TYPE VARCHAR(17),
    ALTER COLUMN product_code TYPE VARCHAR(11),
    ALTER COLUMN date_added TYPE DATE,
    ALTER COLUMN uuid TYPE UUID USING uuid::uuid,
    ALTER COLUMN still_available TYPE BOOLEAN USING still_available::BOOLEAN,
    ALTER COLUMN weight_class TYPE VARCHAR(14);


-- task 6

ALTER TABLE dim_date_times
    ALTER COLUMN month TYPE VARCHAR(2),
    ALTER COLUMN year TYPE VARCHAR(4),
    ALTER COLUMN day TYPE VARCHAR(2),
    ALTER COLUMN time_period TYPE VARCHAR(10),
    ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid;


-- task 7

ALTER TABLE dim_card_details
    ALTER COLUMN card_number TYPE VARCHAR(19),
    ALTER COLUMN expiry_date TYPE VARCHAR(5),
    ALTER COLUMN date_payment_confirmed TYPE DATE;

-- task 8

ALTER TABLE dim_date_times
    ADD PRIMARY KEY (date_uuid);

ALTER TABLE dim_products
    ADD PRIMARY KEY (product_code);

ALTER TABLE dim_store_details
    ADD PRIMARY KEY (store_code);

ALTER TABLE dim_users
    ADD PRIMARY KEY (user_uuid);

ALTER TABLE dim_card_details
    ADD PRIMARY KEY (card_number);

-- task 9

ALTER TABLE orders_table
    ADD CONSTRAINT fk_date_uuid
    FOREIGN KEY (date_uuid) REFERENCES dim_date_times (date_uuid);

ALTER TABLE orders_table
    ADD CONSTRAINT fk_store_code
    FOREIGN KEY (store_code) REFERENCES dim_store_details (store_code);

ALTER TABLE orders_table
    ADD CONSTRAINT fk_user_uuid
    FOREIGN KEY (user_uuid) REFERENCES dim_users (user_uuid);

ALTER TABLE orders_table
    ADD CONSTRAINT fk_product_code
    FOREIGN KEY (product_code) REFERENCES dim_products (product_code);

ALTER TABLE orders_table
    ADD CONSTRAINT fk_card_number
    FOREIGN KEY (card_number) REFERENCES dim_card_details (card_number);