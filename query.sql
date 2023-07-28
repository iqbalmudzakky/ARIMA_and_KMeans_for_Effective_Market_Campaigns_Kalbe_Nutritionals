---------- INPUT DATA ----------
--table customer--
create table Customer(
	Customer_ID integer,
	Age integer,
	Gender integer,
	Marital_Status varchar,
	Income double precision
);
COPY Customer
FROM 'D:\Data Science Boot Camp\VIX Program\VIX - Kalbe Nutritionals\Project\dataset\Case Study - Customer.csv'
DELIMITER ';' 
CSV HEADER;
--table store--
create table Store(
	Store_ID integer,
	Store_Name varchar,
	Group_Store varchar,
	Type varchar,
	Latitude numeric,
	Longitude numeric
);
COPY Store
FROM 'D:\Data Science Boot Camp\VIX Program\VIX - Kalbe Nutritionals\Project\dataset\Case Study - Store.csv'
DELIMITER ';' 
CSV HEADER;
--table product--
create table Product(
	Product_ID varchar,
	Product_Name varchar,
	Price integer
);
COPY Product
FROM 'D:\Data Science Boot Camp\VIX Program\VIX - Kalbe Nutritionals\Project\dataset\Case Study - Product.csv'
DELIMITER ';' 
CSV HEADER;
--table transaction--
create table Transaction(
	Transaction_ID varchar,
	Customer_ID integer,
	Date date,
	Product_ID varchar,
	Price integer,
	Qty integer,
	Total_Amount integer,
	Store_ID integer
);
COPY Transaction
FROM 'D:\Data Science Boot Camp\VIX Program\VIX - Kalbe Nutritionals\Project\dataset\transaction.csv'
DELIMITER ';' 
CSV HEADER;

---------- JOIN TABLE ----------
--check data transaction for duplicate data, because transaction_id has duplicate id--  
with a as (
	select distinct *
	from transaction
)
select
	count (*) as unique,
	(select count (*) from transaction) as total
from a;
--there is no duplicated for transaction data-- 
--make new table with 4 dataset--
CREATE TABLE all_data (
    transaction_id VARCHAR,
    customer_id INTEGER,
    date DATE,
    product_id VARCHAR,
    price INTEGER,
    qty INTEGER,
    total_amount INTEGER,
    store_id INTEGER,
    age INTEGER,
    gender INTEGER,
    marital_status VARCHAR,
    income DOUBLE PRECISION,
    product_name VARCHAR,
    store_name VARCHAR,
    group_store VARCHAR,
    type VARCHAR,
    latitude NUMERIC,
    longitude NUMERIC
);
with i as (
	select
		t.transaction_id,
		t.customer_id,
		t.date,
		t.product_id,
		t.price,
		t.qty,
		t.total_amount,
		t.store_id,
		c.age,
		c.gender,
		c.marital_status,
		c.income,
		p.product_name,
		s.store_name,
		s.group_store,
		s.type,
		s.latitude,
		s.longitude
	from transaction as t
	left join customer as c
		on t.customer_id = c.customer_id
	left join product as p
		on t.product_id = p.product_id
	left join store as s
		on t.store_id = s.store_id
)
insert into all_data
select * from i
;
---------- TABLE AGGREGATE FOR EDA ----------
------avg age by marital status------
with a as (
	select *
	from all_data
	where marital_status is null
)
select
	count (*) as total_null,
	(select count (*) from all_data) as total_data
from a;
--karena jumlah data null <1%, maka data dibuang (tidak terlalu mengandung informasi berguna untuk proses EDA)--
--create table--
create table table_agg_avg_age_by_marital_status as (
	select
		marital_status,
		avg (age) as avg_age_cust
	from all_data
	group by 1
	having marital_status is not null
);
----------avg age by gender----------
create table table_agg_avg_age_by_gender as (
	select
		case
			when gender = 0 then 'wanita'
			when gender = 1 then 'pria'
		end as gender,
		avg (age) as avg_age_cust   
	from all_data
	group by 1
);
------rank name store by total qty------
create table table_agg_rank_name_store_by_total_qty as (
	select
		store_name,
		sum (qty) as total_qty
	from all_data
	group by 1
	order by 2 desc
);
------best selling product by total amount------
create table table_agg_best_selling_product_by_total_amount as (
	select
		product_name,
		sum (total_amount) as total_amount
	from all_data
	group by 1
	order by 2 desc
);
---------- TABLE AGGREGATE FOR DASHBOARD ----------
--total qty per month--
create table table_agg_total_qty_per_month(
	bulan varchar,
	bulan_ke numeric,
	total_qty bigint
);
with a as (	
	select
		to_char (date::date, 'Month') as bulan,
		extract (month from date) as bulan_ke,
		qty
	from all_data
	order by 2
)
insert into table_agg_total_qty_per_month
select
	bulan,
	bulan_ke,
	sum (qty) as total_qty
from a
group by 1,2
order by 2;
--daily total amount--
create table table_agg_daily_total_amount as(
	select
		date,
		sum (total_amount) as total_amount	
	from all_data
	group by 1
	order by 1
);
--total qty by product--
create table table_agg_total_qty_by_product	as (
	select
		product_name,
		sum (qty) as total_qty
	from all_data
	group by 1
	order by 2 desc
);
--total amount by store name--
create table table_agg_total_amount_by_store_name	as (
	select
		store_name,
		sum (total_amount) as total_amount
	from all_data
	group by 1
	order by 2 desc
);