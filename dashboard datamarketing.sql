-- Databricks notebook source
-- MAGIC %md # Principaux KPIs de notre base clients Français

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from datalake_cds_parquet.d_customers
-- MAGIC limit 10

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC 
-- MAGIC SHOW PARTITIONS
-- MAGIC datalake_cds_parquet.f_transaction_header

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW customer_transaction_header
AS
SELECT * FROM 
    datalake_cds_parquet.f_transaction_header
WHERE
    year in (2021,2022)

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE OR REPLACE TABLE preprod_kycp.customer_informations
-- MAGIC AS (
-- MAGIC SELECT
-- MAGIC             (CASE WHEN b.reg_lib_region is not null then b.reg_lib_region else 'DECATHLON.FR' END) as region,
-- MAGIC             (CASE WHEN b.but_name_business_unit is not null then b.but_name_business_unit ELSE 'Non déterminé' END) as magasin,
-- MAGIC             cust.usual_business_unit_num as code_magasin,
-- MAGIC             loyalty_card_num,
-- MAGIC             date_trunc('DD',person_creation_date) as creation_compte,
-- MAGIC             email_is_valid,
-- MAGIC             mobile_is_valid,
-- MAGIC             optin_sport,
-- MAGIC             optin_review,
-- MAGIC             optin_event,
-- MAGIC             count(distinct ftd.the_transaction_id) as nb_achats
-- MAGIC 
-- MAGIC from datalake_cds_parquet.d_customers cust
-- MAGIC left join customer_transaction_header ftd on cust.loyalty_card_num = ftd.ctm_customer_id and date_trunc('DD',ftd.the_date_transaction) >= DATEADD(month, -12, current_date()) and ftd.cnt_idr_country = 66
-- MAGIC left join datalake_cds_parquet.d_business_unit b on b.but_idr_business_unit = cust.but_idr_business_unit_usual
-- MAGIC 
-- MAGIC where cnt_idr_country_usual = 66
-- MAGIC and cust.cnt_country_code_usual = 'FR'
-- MAGIC and cust.loyalty_card_num <> '' and cust.is_deleted is false
-- MAGIC 
-- MAGIC group by 1,2,3,4,5,6,7,8,9,10
-- MAGIC );
-- MAGIC 
-- MAGIC select * from preprod_kycp.customer_informations
-- MAGIC limit 10

-- COMMAND ----------

select nb_achats, count(distinct loyalty_card_num) from preprod_kycp.customer_informations
group by nb_achats

-- COMMAND ----------


