-- Table: processed_data.bid_data_cleaned

-- DROP TABLE processed_data.bid_data_cleaned;

CREATE TABLE processed_data.bid_data_cleaned
(
    fiscal integer NOT NULL,
    bid_no integer NOT NULL,
    destination_no integer NOT NULL,
    bidder_id integer,
    bidder_name text COLLATE pg_catalog."default",
    region text COLLATE pg_catalog."default",
    bid_name text COLLATE pg_catalog."default",
    bid_due_date date,
    bid_type text COLLATE pg_catalog."default",
    customer_id integer,
    customer_name text COLLATE pg_catalog."default",
    customer_type text COLLATE pg_catalog."default",
    destination_name text COLLATE pg_catalog."default",
    destination_county text COLLATE pg_catalog."default",
    destination_state text COLLATE pg_catalog."default",
    destination_zip integer,
    depot_no integer,
    depot_name text COLLATE pg_catalog."default",
    producer_id integer,
    producer_name text COLLATE pg_catalog."default",
    product_id integer,
    product_name text COLLATE pg_catalog."default",
    award_price double precision,
    total_qty double precision,
    total_ef_qty double precision,
    min_purchase_qty double precision,
    max_supply_qty double precision,
    distance double precision,
    stockpile_depot_cost double precision,
    direct_trsfr_depot_cost double precision,
    freight_cost double precision,
    fuel_srchrg_cost double precision,
    equip_cost double precision,
    committed_cost double precision,
    committed_price double precision,
    award_mr double precision,
    award_margin double precision,
    shipment_qty double precision,
    bidder_price double precision,
    request_method text COLLATE pg_catalog."default",
    country text COLLATE pg_catalog."default",
    county_state text COLLATE pg_catalog."default",
    effective_date date,
    expiry_date date,
    opening_date date,
    multi_year boolean,
    multi_year_end_fiscal integer,
    rollover boolean,
    CONSTRAINT bid_data_cleaned_pkey PRIMARY KEY (fiscal, bid_no, destination_no)
)

TABLESPACE pg_default;

ALTER TABLE processed_data.bid_data_cleaned
    OWNER to master_admin;


--------------------------------------------------------------

-- Table: raw_data.bid_data

-- DROP TABLE raw_data.bid_data;

CREATE TABLE raw_data.bid_data
(
    fiscal integer,
    bid_no integer,
    destination_no integer,
    bidder_id integer,
    bidder_name text COLLATE pg_catalog."default",
    region text COLLATE pg_catalog."default",
    bid_name text COLLATE pg_catalog."default",
    bid_due_date date,
    bid_type text COLLATE pg_catalog."default",
    customer_id integer,
    customer_name text COLLATE pg_catalog."default",
    customer_type text COLLATE pg_catalog."default",
    destination_name text COLLATE pg_catalog."default",
    destination_county text COLLATE pg_catalog."default",
    destination_state text COLLATE pg_catalog."default",
    destination_zip integer,
    depot_no integer,
    depot_name text COLLATE pg_catalog."default",
    producer_id integer,
    producer_name text COLLATE pg_catalog."default",
    product_id integer,
    product_name text COLLATE pg_catalog."default",
    award_price double precision,
    total_qty double precision,
    total_ef_qty double precision,
    min_purchase_qty double precision,
    max_supply_qty double precision,
    distance double precision,
    stockpile_depot_cost double precision,
    direct_trsfr_depot_cost double precision,
    freight_cost double precision,
    fuel_srchrg_cost double precision,
    equip_cost double precision,
    committed_cost double precision,
    committed_price double precision,
    award_mr double precision,
    award_margin double precision,
    shipment_qty double precision,
    bidder_price double precision,
    request_method text COLLATE pg_catalog."default",
    country text COLLATE pg_catalog."default",
    county_state text COLLATE pg_catalog."default",
    effective_date date,
    expiry_date date,
    opening_date date,
    multi_year boolean,
    multi_year_end_fiscal integer,
    rollover boolean
)

TABLESPACE pg_default;

ALTER TABLE raw_data.bid_data
    OWNER to master_admin;


--------------------------------------------------------------
-- Table: intermediate_data.bid_data

-- DROP TABLE intermediate_data.bid_data;

CREATE TABLE intermediate_data.bid_data_int
(
    fiscal integer,
    bid_no integer,
    destination_no integer,
    bidder_id integer,
    bidder_name text COLLATE pg_catalog."default",
    region text COLLATE pg_catalog."default",
    bid_name text COLLATE pg_catalog."default",
    bid_due_date date,
    bid_type text COLLATE pg_catalog."default",
    customer_id integer,
    customer_name text COLLATE pg_catalog."default",
    customer_type text COLLATE pg_catalog."default",
    destination_name text COLLATE pg_catalog."default",
    destination_county text COLLATE pg_catalog."default",
    destination_state text COLLATE pg_catalog."default",
    destination_zip integer,
    depot_no integer,
    depot_name text COLLATE pg_catalog."default",
    producer_id integer,
    producer_name text COLLATE pg_catalog."default",
    product_id integer,
    product_name text COLLATE pg_catalog."default",
    award_price double precision,
    total_qty double precision,
    total_ef_qty double precision,
    min_purchase_qty double precision,
    max_supply_qty double precision,
    distance double precision,
    stockpile_depot_cost double precision,
    direct_trsfr_depot_cost double precision,
    freight_cost double precision,
    fuel_srchrg_cost double precision,
    equip_cost double precision,
    committed_cost double precision,
    committed_price double precision,
    award_mr double precision,
    award_margin double precision,
    shipment_qty double precision,
    bidder_price double precision,
    request_method text COLLATE pg_catalog."default",
    country text COLLATE pg_catalog."default",
    county_state text COLLATE pg_catalog."default",
    effective_date date,
    expiry_date date,
    opening_date date,
    multi_year boolean,
    multi_year_end_fiscal integer,
    rollover boolean
)

TABLESPACE pg_default;

ALTER TABLE intermediate_data.bid_data_int
    OWNER to master_admin;

 