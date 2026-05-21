
with ss_items as
 (select i_item_id item_id
       ,c_birth_year birth_year
        ,sum(ss_ext_sales_price) ss_item_rev
 from store_sales
     ,item
     ,date_dim
     ,customer
 where ss_item_sk = i_item_sk
   and d_date in (select d_date
                  from date_dim
                  where d_month_seq = (select d_month_seq
                                      from date_dim
                                      where d_date = '1999-02-11'))
   and ss_sold_date_sk   = d_date_sk
   and ss_list_price between 269 and 298
   and i_manager_id BETWEEN 28 and 57
   and ss_customer_sk = c_customer_sk
   and c_birth_year BETWEEN 1938 AND 1944
group by i_item_id, c_birth_year),
 cs_items as
 (select i_item_id item_id
        ,c_birth_year birth_year
        ,sum(cs_ext_sales_price) cs_item_rev
  from catalog_sales
      ,item
      ,date_dim
      ,customer
 where cs_item_sk = i_item_sk
  and  d_date in (select d_date
                  from date_dim
                  where d_month_seq = (select d_month_seq
                                      from date_dim
                                      where d_date = '1999-02-11'))
  and  cs_sold_date_sk = d_date_sk
  and  cs_list_price between 269 and 298
  and i_manager_id BETWEEN 28 and 57
  and cs_bill_customer_sk = c_customer_sk
  and c_birth_year BETWEEN 1938 AND 1944
group by i_item_id, c_birth_year),
 ws_items as
 (select i_item_id item_id
      ,c_birth_year birth_year
        ,sum(ws_ext_sales_price) ws_item_rev
  from web_sales
      ,item
      ,date_dim
      ,customer
 where ws_item_sk = i_item_sk
  and  d_date in (select d_date
                  from date_dim
                  where d_month_seq = (select d_month_seq
                                     from date_dim
                                     where d_date = '1999-02-11'))
  and ws_sold_date_sk   = d_date_sk
  and ws_list_price between 269 and 298
  and i_manager_id BETWEEN 28 and 57
  and ws_bill_customer_sk = c_customer_sk
  and c_birth_year BETWEEN 1938 AND 1944
group by i_item_id, c_birth_year)
  select  ss_items.item_id, ss_items.birth_year
       ,ss_item_rev
       ,ss_item_rev/((ss_item_rev+cs_item_rev+ws_item_rev)/3) * 100 ss_dev
       ,cs_item_rev
       ,cs_item_rev/((ss_item_rev+cs_item_rev+ws_item_rev)/3) * 100 cs_dev
       ,ws_item_rev
       ,ws_item_rev/((ss_item_rev+cs_item_rev+ws_item_rev)/3) * 100 ws_dev
       ,(ss_item_rev+cs_item_rev+ws_item_rev)/3 average
 from ss_items,cs_items,ws_items
 where ss_items.item_id=cs_items.item_id
   and ss_items.item_id=ws_items.item_id
   and ss_items.birth_year = cs_items.birth_year
   and ss_items.birth_year = ws_items.birth_year
   and ss_item_rev between 0.9 * cs_item_rev and 1.1 * cs_item_rev
   and ss_item_rev between 0.9 * ws_item_rev and 1.1 * ws_item_rev
   and cs_item_rev between 0.9 * ss_item_rev and 1.1 * ss_item_rev
   and cs_item_rev between 0.9 * ws_item_rev and 1.1 * ws_item_rev
   and ws_item_rev between 0.9 * ss_item_rev and 1.1 * ss_item_rev
   and ws_item_rev between 0.9 * cs_item_rev and 1.1 * cs_item_rev
 order by item_id, birth_year
         ,ss_item_rev
 limit 100;


