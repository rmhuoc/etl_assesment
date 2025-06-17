-- DROP SCHEMA loads;

CREATE SCHEMA loads AUTHORIZATION postgres;

-- DROP SCHEMA etl_assesment_data;

CREATE SCHEMA etl_assesment_data AUTHORIZATION postgres;




-- etl_assesment_data.sales definition

-- Drop table

-- DROP TABLE etl_assesment_data.sales;

CREATE TABLE etl_assesment_data.sales (
	id serial4 NOT NULL,
	transaction_id varchar(100) NULL,
	customer_id varchar(100) NULL,
	product_id varchar(100) NULL,
	quantity int4 NULL,
	"timestamp" timestamp NULL,
	process_id int4 NULL,
	CONSTRAINT sales_pkey PRIMARY KEY (id),
	CONSTRAINT sales_quantity_check CHECK ((quantity > 0))
);

-- etl_assesment_data.sales_tmp definition

-- Drop table

-- DROP TABLE etl_assesment_data.sales_tmp;

CREATE TABLE etl_assesment_data.sales_tmp (
	id int4 NOT NULL DEFAULT nextval('etl_assesment_data.sales_id_seq'::regclass),
	transaction_id varchar(100) NULL,
	customer_id varchar(100) NULL,
	product_id varchar(100) NULL,
	quantity int4 NULL,
	"timestamp" timestamp NULL,
	process_id int4 NULL,
	CONSTRAINT sales_pkey_tmp PRIMARY KEY (id),
	CONSTRAINT sales_quantity_check_tmp CHECK ((quantity > 0))
);

-- etl_assesment_data.sales_id_seq definition

-- DROP SEQUENCE etl_assesment_data.sales_id_seq;

CREATE SEQUENCE etl_assesment_data.sales_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	CACHE 1
	NO CYCLE;
	
	-- loads.etl_load_log definition

-- Drop table

-- DROP TABLE loads.etl_load_log;

CREATE TABLE loads.etl_load_log (
	process_id int8 NOT NULL,
	start_time timestamp NOT NULL,
	end_time timestamp NULL,
	num_records_loaded int4 NULL,
	status varchar(20) NULL,
	error_message text NULL,
	CONSTRAINT etl_load_log_pkey PRIMARY KEY (process_id)
);