
with customer_total_return as
 (select wr_returning_customer_sk as ctr_customer_sk
 ,ca_state as ctr_state
,wr_reason_sk as ctr_reason_sk
,sum(wr_return_amt) as ctr_total_return
 from web_returns
     ,date_dim
     ,customer_address
     ,item
 where wr_returned_date_sk = d_date_sk
   and d_year =2001
   and wr_returning_addr_sk = ca_address_sk
   and wr_item_sk = i_item_sk
   and i_manager_id BETWEEN 69 and 78
   and wr_return_amt / wr_return_quantity between 28 and 57
 group by wr_returning_customer_sk
         ,ca_state, wr_reason_sk)
  select  c_customer_id,c_salutation,c_first_name,c_last_name,c_preferred_cust_flag
       ,c_birth_day,c_birth_month,c_birth_year,c_birth_country,c_login,c_email_address
       ,c_last_review_date_sk,ctr_total_return
 from customer_total_return ctr1
     ,customer_address
     ,customer
 where ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2
 			  from customer_total_return ctr2
                  	  where ctr1.ctr_state = ctr2.ctr_state)
       and ca_address_sk = c_current_addr_sk
       and ca_state in ('IA', 'MT', 'NE', 'TX')
       and ctr1.ctr_customer_sk = c_customer_sk
       and ctr1.ctr_reason_sk in (3, 31)
      and c_birth_year BETWEEN 1938 AND 1944
 order by c_customer_id,c_salutation,c_first_name,c_last_name,c_preferred_cust_flag
                  ,c_birth_day,c_birth_month,c_birth_year,c_birth_country,c_login,c_email_address
                  ,c_last_review_date_sk,ctr_total_return
limit 100;


