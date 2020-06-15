
----------------------- Create table having columns with telco data and Update Subscriber Activation & refill & Usage table ---------------
create table subscriber_activation_202001 (MSISDN text ,ACCOUNT_ACTIVATED_DATE text,LAST_USAGE_DATE text);

create table subscriber_refill_201912 (YEAR_MONTH text, MSISDN text ,NUMBER_TIME_REFILLED text,TOTAL_VALUE_OF_MONTH text);

create table subscriber_usage_201912 (YEAR_MONTH text, MSISDN text ,TOTAL_VALUE_OF_MONTH text);


---copy subscriber_activation_202001 from ftp - the path will be shared by Telco;
copy subscriber_activation_202001 from '/data2/ftp_dumps/SUBSCRIBER_ACTIVATION_202001.csv' with delimiter','csv;

copy subscriber_refill_201912 from '/data2/ftp_dumps/SUBSCRIBER_REFILL_201912.csv' with delimiter','csv;

copy subscriber_usage_201912 from '/data2/ftp_dumps/subscriber_revenue_201912.csv' with delimiter','csv;


---remove record having unwanted record/data from activation/refill/usage table
--------clean activation files
delete from subscriber_activation_202001 where MSISDN='231||MSISDN'

--------clean refill files
delete from subscriber_refill_201908 where MSISDN='MSISDN';

--------clean usage files
delete from subscriber_usage_201912 where MSISDN='MSISDN';

---explore data into table
select min(TOTAL_VALUE_OF_MONTH::numeric), max(TOTAL_VALUE_OF_MONTH::numeric) from subscriber_refill_201912 where TOTAL_VALUE_OF_MONTH::numeric>0; 

select min(TOTAL_VALUE_OF_MONTH::numeric), max(TOTAL_VALUE_OF_MONTH::numeric) from subscriber_usage_201912 where TOTAL_VALUE_OF_MONTH::numeric>0; 

------check the length of msisdn from refill/usgae/activation tables and remove country codes
select length(msisdn::text), count(*) from subscriber_refill_201912 group by 1 order by 1;

select length(msisdn::text), count(*) from subscriber_activation_202001 group by 1 order by 1   

update subscriber_activation_202001 set msisdn= substring(msisdn::text,4,12)::bigint;

update subscriber_refill_201912 set msisdn= substring(msisdn::text,4,12)::bigint ;

update subscriber_usage_201912 set msisdn= substring(msisdn::text,4,12)::bigint ;

----rename the account_activated_date to activation_date;

alter table subscriber_activation_202001 rename column account_activated_date to activation_date;

---check activation trend from activation table
select extract(year from activation_date::date) as year, extract(month from activation_date::date) as month,  count(*) from subscriber_activation_202001 group by 1,2 order by 1,2;

---------------------------------------------------------------------------------------------------------------------------
|			---No Need to update this if we have latest activation table 					   |
---------------------------------------------------------------------------------------------------------------------------
---take backup of previous activation table and update the same into latest table
alter table tbl_subscriber_activations_tmp rename to tbl_subscriber_activations_tmp_old_Dec2019

create table tbl_subscriber_activations_tmp as select * from subscriber_activation_202001

--returns New Subscribers
select count(*) from 
(select subscriber::bigint from tbl_subscriber_activations_tmp_old_Dec2019 
except
select msisdn::bigint from subscriber_activation_202001) b;

---create table 
Drop table new_additions;

create table new_additions as
select subscriber::bigint from tbl_subscriber_activations_tmp_old_Dec2019 
except
select msisdn::bigint from subscriber_activation_201903;


--append their Activations dates as well
drop table new_additions_final;

create table new_additions_final as
select a.msisdn,activation_date from 
(select subscriber as msisdn from new_additions) a
left outer join
(select subscriber,max(activation_date::date) as activation_date from tbl_subscriber_activations_tmp_old_Dec2019 group by 1) b
on a.msisdn::bigint=b.subscriber::bigint;


alter table subscriber_activation_202001 rename to tbl_subscriber_activations_tmp;

insert into tbl_subscriber_activations_tmp(msisdn,activation_date) select * from new_additions_final;


---------------------------------------------------------------------------------------------------------------------------
|				-- continue from here									   |
---------------------------------------------------------------------------------------------------------------------------
alter table subscriber_activation_202001 rename to tbl_subscriber_activations_tmp;


--****************************************************************************************************************--
--|					--Create table for profiling						  |--
--****************************************************************************************************************--

select t1.msisdn, t1.activation_date::date,refill_count_201910, refill_amount_201910,refill_count_201911, refill_amount_201911, 
refill_count_201912, refill_amount_201912, (null)::integer three_month_wavg,(null)::double precision amount_loanable,(current_date - activation_date::date) age,
(null)::integer age_in_month, (null)::numeric default_probability,(null)::integer segment 
	into tbl_decisioning_20191231
	from (select msisdn, max(activation_date::date) activation_date 
		from tbl_subscriber_activations_tmp group by msisdn, activation_date) t1
	left join (select msisdn, sum(number_time_refilled::numeric) as refill_count_201910, sum(total_value_of_month::numeric) as refill_amount_201910
			from subscriber_refill_201910
			group by msisdn) t2
		on t1.msisdn::bigint= t2.msisdn::bigint
	left join (select msisdn, sum(number_time_refilled::numeric) as refill_count_201911, sum(total_value_of_month::numeric) as refill_amount_201911
			from subscriber_refill_201911
			group by msisdn) t3
		on t1.msisdn::bigint = t3.msisdn::bigint
	left join (select msisdn, sum(number_time_refilled::numeric) as refill_count_201912, sum(total_value_of_month::numeric) as refill_amount_201912
			from subscriber_refill_201912
			group by msisdn) t4
		on t1.msisdn::bigint= t4.msisdn::bigint;


-----update age in month
update tbl_decisioning_20191231 t1 set age_in_month=age/30

----update three month weightage average
update tbl_decisioning_20191231 t1
  set three_month_wavg = t2.wavg_three_months 
  from (select msisdn, 
      (((coalesce(refill_amount_201910,0)::numeric * 1) + 
      (coalesce(refill_amount_201911,0)::numeric * 2) + 
      (coalesce(refill_amount_201912,0)::numeric * 3))/6)::bigint  as wavg_three_months
    from tbl_decisioning_20191231) t2
  where t1.msisdn = t2.msisdn;


---update amount loanable
 update tbl_decisioning_20191231 t1
        set amount_loanable = (case 
        when t1.age >= 90 and three_month_wavg >= 60 then 1000
        when t1.age >= 90 and three_month_wavg >= 30 then 500
        when t1.age >= 90 and three_month_wavg >= 12 then 200
        when t1.age >= 90 and three_month_wavg >= 5 then 100   
        when t1.age >= 90 and three_month_wavg >= 2.5 then 50 
        when t1.age >= 90 and three_month_wavg >= 1.5 then 30    
        when t1.age >= 90 and three_month_wavg >= 0.5 then 10
        when refill_amount_201903 > 0 then 10
        else 0 
    end);



-----update credit scoring and segment
update tbl_decisioning_20191231 t1	
 set default_probability = t2.pd 	
 from (select msisdn,(1/(1+ 2.71828281828 ^ (-2.271256-(0.018098*coalesce(age_in_month,0)::numeric)-(0.013754*coalesce(refill_amount_201912,0)::bigint)))) as pd	
  from tbl_decisioning_20191231) as t2	
 where t1.msisdn=t2.msisdn;	

---update segment
update tbl_decisioning_20191231 t1	
 set segment= (case when t1.default_probability >= 0.993670475714178 then 1	
       when t1.default_probability >= 0.982670475714178 then 2	
       when t1.default_probability >= 0.971670475714178 then 3	
       when t1.default_probability >= 0.960670475714178 then 4	
       when t1.default_probability >= 0.949670475714178 then 5	
       when t1.default_probability >= 0.938670475714178 then 6	
       when t1.default_probability < 0.938670475714178 then 7                	
    end);	


-----update last usage and dormant days

alter table tbl_decisioning_20191231 add column last_usage_date date, add column dormant_days integer, add column dormant_days_flag integer;


update tbl_decisioning_20191231 t1 set last_usage_date=t2.last_usage_date from (select msisdn, max(last_usage_date::date) as last_usage_date from tbl_subscriber_activations_tmp group by 1) t2
where t1.msisdn::bigint=t2.msisdn::bigint


update tbl_decisioning_20191231 t1 set dormant_days=current_date::date-last_usage_date::date;


update tbl_decisioning_20191231 t1 set dormant_days_flag= (case when t1.dormant_days>=90 then 4
when t1.dormant_days>=60 then 3 when t1.dormant_days>=30 then 2 when t1.dormant_days>0 then 1
else 0 end);

---downgrade based upon dormant_days

update tbl_decisioning_20191231 set amount_loanable=0 where amount_loanable>0 and dormant_days_flag>3 ;
update tbl_decisioning_20191231 set amount_loanable=10 where amount_loanable>=30 and amount_loanable<=50 and dormant_days_flag=3 ;
update tbl_decisioning_20191231 set amount_loanable=30 where amount_loanable=100 and dormant_days_flag=3;
update tbl_decisioning_20191231 set amount_loanable=50 where amount_loanable=200 and dormant_days_flag=3 ;
update tbl_decisioning_20191231 set amount_loanable=100 where amount_loanable=500 and dormant_days_flag=3 ;

update tbl_decisioning_12Apr2019 set amount_loanable=200 where amount_loanable=500 and dormant_days_flag=2 ;
update tbl_decisioning_12Apr2019 set amount_loanable=500 where amount_loanable=1000 and dormant_days_flag=2 ;


----update the base into tbl_decisioning_20191231_new table;
select distinct(msisdn)::bigint as subscriber_fk, max(amount_loanable) as cents_loanable into tbl_decisioning_20191231_new_base 
from tbl_decisioning_20191231 where amount_loanable <> 0 group by 1 order by 1;

----------------------
---New Adds
select count(*) from (select msisdn::bigint as subcriber_fk from tbl_decisioning_20191231_new_base except select subscriber_fk from tbl_decisioning2_new)b; 

--Dropout
select count(t2.subscriber_fk) as dropouts from tbl_decisioning_20191231_new_base t1 right join tbl_decisioning2_new t2 
on t1.subscriber_fk=t2.subscriber_fk where t1.subscriber_fk is null; 

--Check Upgraders
select count(t1.subscriber_fk) as upgarder from tbl_decisioning_20191231_new_base t1 left join tbl_decisioning2_new t2 
on t1.subscriber_fk=t2.subscriber_fk where t1.cents_loanable>t2.cents_loanable;

--Downgraders
select count(t1.subscriber_fk) as downgrade from tbl_decisioning_20191231_new_base t1 left join tbl_decisioning2_new t2 
on t1.subscriber_fk=t2.subscriber_fk where t1.cents_loanable<t2.cents_loanable;

--No Change
select count(t1.subscriber_fk) as nochange from tbl_decisioning_20191231_new_base t1 left join tbl_decisioning2_new t2 
on t1.subscriber_fk=t2.subscriber_fk where t1.cents_loanable=t2.cents_loanable;


----Creating backup of tbl_decisioning2_new and Updating the base into tbl_decisioning2_new

create table tbl_decisioning2_new_31Jan2020_old_base as select * from tbl_decisioning2_new;

truncate tbl_decisioning2_new;

insert into tbl_decisioning2_new select subscriber_fk,cents_loanable,null,null,'regular' from tbl_decisioning_20191231_new_base where cents_loanable > 0;


