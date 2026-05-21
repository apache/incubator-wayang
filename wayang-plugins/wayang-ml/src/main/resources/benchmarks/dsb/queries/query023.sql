
with frequent_ss_items as
 (select substring(i_item_desc,1,30) itemdesc,i_item_sk item_sk,d_date solddate,count(*) cnt
  from store_sales
      ,date_dim
      ,item
  where ss_sold_date_sk = d_date_sk
    and ss_item_sk = i_item_sk
    and d_year = 2001
    and i_manager_id BETWEEN 77 and 96
     AND i_category IN ('Books', 'Jewelry', 'Sports')
  group by substring(i_item_desc,1,30),i_item_sk,d_date
  having count(*) >4),
 max_store_sales as
 (select max(csales) tpcds_cmax
  from (select c_customer_sk,sum(ss_quantity*ss_sales_price) csales
        from store_sales
            ,customer
            ,date_dim
        where ss_customer_sk = c_customer_sk
         and ss_sold_date_sk = d_date_sk
         and d_year = 2001
         and ss_wholesale_cost BETWEEN 2 AND 12
        group by c_customer_sk) tmp1),
 best_ss_customer as
 (select c_customer_sk,sum(ss_quantity*ss_sales_price) ssales
  from store_sales
      ,customer
  where ss_customer_sk = c_customer_sk
  and c_birth_year BETWEEN 1977 AND 1983
  group by c_customer_sk
  having sum(ss_quantity*ss_sales_price) > (95/100.0) * (select
  *
from
 max_store_sales))
  select  sum(sales)
 from (select cs_quantity*cs_list_price sales
       from catalog_sales
           ,date_dim
       where d_year = 2001
         and d_moy = 10
         and cs_sold_date_sk = d_date_sk
         and cs_item_sk in (select item_sk from frequent_ss_items)
         and cs_bill_customer_sk in (select c_customer_sk from best_ss_customer)
         and cs_wholesale_cost BETWEEN 2 AND 12
      union all
      select ws_quantity*ws_list_price sales
       from web_sales
           ,date_dim
       where d_year = 2001
         and d_moy = 10
         and ws_sold_date_sk = d_date_sk
         and ws_item_sk in (select item_sk from frequent_ss_items)
         and ws_bill_customer_sk in (select c_customer_sk from best_ss_customer)
         and ws_wholesale_cost BETWEEN 2 AND 12) tmp2
 limit 100;


