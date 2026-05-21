
with customer_total_return as (
	select sr_customer_sk as ctr_customer_sk, 
	       sr_store_sk as ctr_store_sk,
               sr_reason_sk as ctr_reason_sk,
               sum(sr_refunded_cash) as ctr_total_return
	from postgres.store_returns, postgres.date_dim
	where sr_returned_date_sk = d_date_sk
		and d_year =2001
		and sr_return_amt / sr_return_quantity between 236 and 295
	group by sr_customer_sk, sr_store_sk, sr_reason_sk)
select c_customer_id
from customer_total_return AS ctr1,
     postgres.store,
     postgres.customer,
     postgres.customer_demographics
where ctr1.ctr_total_return > (
	select avg(ctr_total_return)*1.2
	from customer_total_return AS ctr2
	where ctr1.ctr_store_sk = ctr2.ctr_store_sk)
and ctr1.ctr_reason_sk BETWEEN 28 AND 31
and s_store_sk = ctr1.ctr_store_sk
and s_state IN ('MI', 'NC', 'WI')
and ctr1.ctr_customer_sk = c_customer_sk
and c_current_cdemo_sk = cd_demo_sk
and cd_marital_status IN ('W', 'W')
and cd_education_status IN ('4 yr Degree', 'College')
and cd_gender = 'M'
and c_birth_month = 5
and c_birth_year BETWEEN 1950 AND 1956
order by c_customer_id
limit 100;


