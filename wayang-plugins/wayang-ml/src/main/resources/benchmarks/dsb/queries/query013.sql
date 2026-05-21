
select avg(ss_quantity)
,avg(ss_ext_sales_price)
,avg(ss_ext_wholesale_cost)
,sum(ss_ext_wholesale_cost)
from postgres.store_sales
   ,postgres.store
   ,postgres.customer_demographics
   ,postgres.household_demographics
   ,postgres.customer_address
   ,postgres.date_dim
where s_store_sk = ss_store_sk
and  ss_sold_date_sk = d_date_sk and d_year = 2001
and((ss_hdemo_sk=hd_demo_sk
and cd_demo_sk = ss_cdemo_sk
and cd_marital_status = 'U'
and cd_education_status = 'College'
and ss_sales_price between 100.00 and 150.00
and hd_dep_count = 3
   )or
   (ss_hdemo_sk=hd_demo_sk
and cd_demo_sk = ss_cdemo_sk
and cd_marital_status = 'W'
and cd_education_status = '2 yr Degree'
and ss_sales_price between 50.00 and 100.00
and hd_dep_count = 1
   ) or
   (ss_hdemo_sk=hd_demo_sk
and cd_demo_sk = ss_cdemo_sk
and cd_marital_status = 'S'
and cd_education_status = 'College'
and ss_sales_price between 150.00 and 200.00
and hd_dep_count = 1
   ))
and((ss_addr_sk = ca_address_sk
and ca_country = 'United States'
and ca_state in ('IN', 'NM', 'VA')
and ss_net_profit between 100 and 200
   ) or
   (ss_addr_sk = ca_address_sk
and ca_country = 'United States'
and ca_state in ('MT', 'OH', 'OR')
and ss_net_profit between 150 and 300
   ) or
   (ss_addr_sk = ca_address_sk
and ca_country = 'United States'
and ca_state in ('GA', 'IL', 'TX')
and ss_net_profit between 50 and 250
   ))
;


