
----------------------- Create table having columns with telco data and import data into Update Subscriber Activation  tmp table ---------------
create table subscriber_activation_file_Nov19_Dec19 (Transaction_date text, MSISDN text, last_events_date text, rgs_position text, cell_id text, activation_date text, site_name text, sector_name text, region_name text);

---if dates are in dd-mm-yyyy format, change date from dd-mm-yyyy format to yyyy-mm-dd format--

ALTER TABLE subscriber_activation_file_Nov19_Dec19 ALTER COLUMN last_events_date TYPE DATE using to_date(last_events_date, 'DD/MM/YYYY');
ALTER TABLE subscriber_activation_file_Nov19_Dec19 ALTER COLUMN transaction_date TYPE DATE using to_date(transaction_date, 'DD/MM/YYYY');
ALTER TABLE subscriber_activation_file_Nov19_Dec19 ALTER COLUMN activation_date TYPE DATE using to_date(activation_date, 'DD/MM/YYYY');

---Check the lenght of msisdn & remove country code from msisdn

select distinct(length (msisdn::text)), count(*) from subscriber_activation_file_Nov19_Dec19 group by 1 order by 1; -----12;58699

update subscriber_activation_file_Nov19_Dec19 set msisdn=substring(msisdn::text,4,12)::bigint;

------------------------------------Update subscriber activation tmp table with new subs ------------------------------------------
--rename previous subcriber activation table end with latest date for reference; 
alter table tbl_subscriber_activation_tmp rename to tbl_subscriber_activation_tmp_20191031
--create new activation table;
Create table tbl_subscriber_activation_tmp as select * from tbl_subscriber_activation_tmp_20191031 limit 0;
--Update the activaiton table with latest active subs;
insert into tbl_subscriber_activation_tmp (MSISDN,activation_date) select msisdn::bigint as msisdn,activation_date::date as activation_date from subscriber_activation_file_Nov19_Dec19;

--Check the previous table and Update the active subs not available in current table 
select count(*) from (select msisdn::bigint from tbl_subscriber_activation_tmp_20191031
except
select msisdn::bigint from tbl_subscriber_activation_tmp) b; ----1267833

---create & update new adds from previous activation table
drop table new_adds;
create table new_adds as select msisdn::bigint from tbl_subscriber_activation_tmp_20191031
except
select msisdn::bigint from tbl_subscriber_activation_tmp;

drop table new_adds_final;

create table new_adds_final as select a.msisdn::bigint,b.activation_date from (select msisdn::bigint from new_adds) a left outer join
(select msisdn::bigint,max(activation_date) activation_date from tbl_subscriber_activation_tmp_20191031 group by msisdn) b
on a.msisdn::bigint=b.msisdn::bigint;

---insert into  new Additions into the master Base
insert into tbl_subscriber_activation_tmp(msisdn,activation_date) select msisdn, activation_date::date from new_adds_final;


------------------- Create table with variable provided by telco ------------------
------------------- Import data into refill table from PGAdmin by right clicking the table & select import option ------------

create table tbl_refill_Nov2019_Dec2019
(MSISDN bigint, TOTAL_REFILL_COUNT_201911 numeric,TOTAL_REFILL_201911 numeric, VOUCHER_REFILL_201911 numeric,EVD_REFILL_201911 numeric,
MOMO_REFILL_201911 numeric, OTHERS_REFILL_201911 numeric, TOTAL_REFILL_COUNT_201912 numeric, TOTAL_REFILL_201912 numeric, VOUCHER_REFILL_201912 numeric,
EVD_REFILL_201912 numeric, MOMO_REFILL_201912 numeric, OTHERS_REFILL_201912 numeric);


---------	creat monthly refill tables	& update them from above table ---------
create table refill_201911 (MSISDN bigint, refill_count integer, refill_amount numeric);
create table refill_201912 (MSISDN bigint, refill_count integer, refill_amount numeric);

insert into refill_201911 (MSISDN,refill_count, refill_amount) select msisdn::bigint as msisdn, total_refill_count_201911::integer as refill_count,
total_refill_201911::numeric as  refill_amount from tbl_refill_Nov2019_Dec2019;


insert into refill_201912 (MSISDN,refill_count, refill_amount) select msisdn::bigint as msisdn,total_refill_count_201912::integer as refill_count,
total_refill_201912::numeric as  refill_amount from tbl_refill_Nov2019_Dec2019;


----Check the msisdn lenght and remove the country code from msisdn
select distinct(length (msisdn::text)), count(*) from refill_201911 group by 1 order by 1;---12;687004
select distinct(length (msisdn::text)), count(*) from refill_201912 group by 1 order by 1;---12;687004

update refill_201911 set msisdn=substring(msisdn::text,4,12)::bigint;
update refill_201912 set msisdn=substring(msisdn::text,4,12)::bigint;

----------------------------------------------------------------------------------------------------------------
-----Check the data of last three month refill table  
select count(*), sum(refill_count),sum(refill_amount) from refill_201912 where refill_amount>0; --- 599134;4106772;1232554748
select count(*), sum(refill_count),sum(refill_amount) from refill_201911 where refill_amount>0; --- 561906;3624242;1064299527
select count(*), sum(refill_count),sum(refill_amount) from refill_201910 where refill_amount>0; --- 558948;3701217;1106116120.0

------------------------------ Prepare decisioning table --------------------------------------------------------
---- drop table tbl_decisioning2_20191231_new
select t1.msisdn, t1.activation_date::date, refill_count_201910, refill_amount_201910, refill_count_201911, refill_amount_201911, 
refill_count_201912, refill_amount_201912, (null)::integer three_month_wavg,(null)::double precision amount_loanable, (current_date - activation_date::date) age, 
(null)::integer age_in_month,(null)::numeric default_probability,(null)::integer segment, (null)::integer dormant_days_flag
	into tbl_decisioning2_20191231_new
	from (select msisdn, max(activation_date::date) activation_date 
		from tbl_subscriber_activation_tmp
		group by msisdn, activation_date) t1
	left join (select msisdn, sum(refill_count::numeric) as refill_count_201910, sum(refill_amount::numeric) as refill_amount_201910
			from refill_201910
			group by msisdn) t2
		on t1.msisdn::bigint= t2.msisdn::bigint
	left join (select msisdn, sum(refill_count::numeric) as refill_count_201911, sum(refill_amount::numeric) as refill_amount_201911
			from refill_201911
			group by msisdn) t3
		on t1.msisdn::bigint = t3.msisdn::bigint
	left join (select msisdn, sum(refill_count::numeric) as refill_count_201912, sum(refill_amount::numeric) as refill_amount_201912
			from refill_201912
			group by msisdn) t4
on t1.msisdn::bigint= t4.msisdn::bigint;


----update age in month
update tbl_decisioning2_20191231_new t1 set age_in_month=age/30;

--update refill_count and amount as 0 where the value is null;
update tbl_decisioning2_20191231_new set refill_count_201910=0 where refill_count_201910 is null;
update tbl_decisioning2_20191231_new set refill_amount_201910=0 where refill_amount_201910 is null;
update tbl_decisioning2_20191231_new set refill_count_201911=0 where refill_count_201911 is null;
update tbl_decisioning2_20191231_new set refill_amount_201911=0 where refill_amount_201911 is null;
update tbl_decisioning2_20191231_new set refill_count_201912=0 where refill_count_201912 is null;
update tbl_decisioning2_20191231_new set refill_amount_201912=0 where refill_amount_201912 is null;

------------------------------------**************************************------------------------------------------------------------
-----Update three month wavg
update tbl_decisioning2_20191231_new t1
  set three_month_wavg = t2.wavg_three_months 
  from (select msisdn, 
      (((coalesce(refill_amount_201910,0)::numeric * 1) + 
      (coalesce(refill_amount_201911,0)::numeric * 2) + 
      (coalesce(refill_amount_201912,0)::numeric * 3))/6)::bigint  as wavg_three_months
    from tbl_decisioning2_20191231_new) t2
  where t1.msisdn = t2.msisdn;


-----update amount_loanable

 update tbl_decisioning2_20191231_new t1
  set amount_loanable = (case when age >= 90 and three_month_wavg >= 11000 then 200000
  when age >= 90 and three_month_wavg>=5500 and three_month_wavg<11000 then 100000
  when age >= 90 and three_month_wavg>=2200 and three_month_wavg<5500 then 50000
  when age >= 90 and three_month_wavg>=880 and three_month_wavg<2200 then 20000
  when age >= 90 and three_month_wavg>=330 and three_month_wavg<880 then 10000
  when three_month_wavg>0 then 10000
  else 0
    end);

----update credit scoring
update tbl_decisioning2_20191231_new t1
 set default_probability = t2.pd 
 from (select msisdn,(1/(1+ 2.71828281828 ^ (-0.9787968+(-0.0070193*coalesce(age_in_month,0)::numeric)+(-0.0102179*coalesce(refill_count_201912,0)::bigint)))) as pd
  from tbl_decisioning2_20191231_new) as t2
   where t1.msisdn::bigint=t2.msisdn::bigint;


update tbl_decisioning2_20191231_new t1
 set segment= (case when t1.default_probability >= 0.858070475714178 then 1
       when t1.default_probability >= 0.8080015704757141 then 2
       when t1.default_probability >= 0.7777957047571417 then 3
       when t1.default_probability >= 0.7551570475714178 then 4
       when t1.default_probability >= 0.7445570475714178 then 5
       when t1.default_probability >= 0.7345570475714178 then 6
       when t1.default_probability <  0.7345570475714178 then 7                
    end);

  

------------------------------------------------------------------------------
---alter table tbl_decisioning2_20191231_new add column dormant_days_flag integer

update tbl_decisioning2_20191231_new t1
 set dormant_days_flag = (case 
       when refill_amount_201912 > 0 then 1
       when refill_amount_201912+refill_amount_201911 > 0 then 2
       else 3 
    end);

----------Upgrade & downgrade Subs based on behavior
-----downgrade subs
update tbl_decisioning2_20191231_new set amount_loanable=10000 where amount_loanable=20000 and segment >= 6; 
update tbl_decisioning2_20191231_new set amount_loanable=10000 where amount_loanable=50000 and segment >= 6; 
update tbl_decisioning2_20191231_new set amount_loanable=20000 where amount_loanable=100000 and segment >= 6; 
update tbl_decisioning2_20191231_new set amount_loanable=50000 where amount_loanable=200000 and segment >= 6; 

update tbl_decisioning2_20191231_new set amount_loanable=20000 where amount_loanable=50000 and segment = 5; 
update tbl_decisioning2_20191231_new set amount_loanable=50000 where amount_loanable=100000 and segment = 5; 
update tbl_decisioning2_20191231_new set amount_loanable=100000 where amount_loanable=200000 and segment = 5; 

update tbl_decisioning2_20191231_new set amount_loanable=100000 where amount_loanable=200000 and segment = 4; 


---upgrade subs

select count(*) from tbl_decisioning2_20191231_new where amount_loanable=100000 and segment = 1 and dormant_days_flag<3; 

select count(*) from tbl_decisioning2_20191231_new where amount_loanable=50000 and segment = 1 and dormant_days_flag<3;  

update tbl_decisioning2_20191231_new set amount_loanable=120000 where amount_loanable=100000 and segment = 1 and dormant_days_flag<3; 

update tbl_decisioning2_20191231_new set amount_loanable=60000 where amount_loanable=50000 and segment = 1 and dormant_days_flag<3; 

select segment,amount_loanable, count(*) from tbl_decisioning2_20191231_new where amount_loanable>0 group by 1,2 order by 1,2;

--------------------**************----------- downgrade subs based upon repaymnet ---------**************--------------

---update loan_count & avg_repay_time into decisioning table;

alter table tbl_decisioning2_20191231_new add column loan_count integer, add column avg_repay_time numeric;

-----import data for updating loan_count & avg_repay time;
create table am_tbl_loans as select * 
	from  
   dblink('dbname=mode_gb_acs user=amishra host=10.195.2.194 password=@amishra1 port=7799', 
						'select subscriber_fk,loan_time,repay_time,loan_id,cents_loaned,cents_serviceq,cents_paid
						from tbl_loans') 
as t2 (subscriber_fk bigint,loan_time timestamp,repay_time timestamp, loan_id bigint,cents_loaned float, cents_serviceq float,cents_paid float) where loan_time between 
'2019-11-01 00:00:00.00000' and '2020-01-31 23:59:59.9999999';


create table am_tbl_loans_count_avg_rpay_time_Jan2020 as select distinct subscriber_fk as subscriber_fk, count(loan_id) as loan_count,
avg(repay_time::date-loan_time::date) as avg_repay_time from am_tbl_loans where loan_time>='1-Nov-2019' group by subscriber_fk;


update tbl_decisioning2_20191231_new t1 set avg_repay_time=t2.avg_repay_time
 from (select distinct(subscriber_fk) as subscriber_fk, max(avg_repay_time) as avg_repay_time from am_tbl_loans_count_avg_rpay_time_Jan2020 group by 1) t2 
where t1.msisdn::bigint=t2.subscriber_fk::bigint;

update tbl_decisioning2_20191231_new t1 set loan_count=t2.loan_count
 from (select distinct(subscriber_fk) as subscriber_fk, max(loan_count) as loan_count from am_tbl_loans_count_avg_rpay_time_Jan2020 group by 1) t2 
where t1.msisdn::bigint=t2.subscriber_fk::bigint;


update tbl_decisioning2_20191231_new set amount_loanable=10000 where amount_loanable>10000 and avg_repay_time>=15 and segment >=4; ---8523


--------------------**************--------------------**************--------------------**************
select amount_loanable, count(*) from tbl_decisioning2_20191231_new group by 1 order by 1

select * from tbl_decisioning2_new limit 5
------------------------------------------------------------------------------------------------------------
select distinct(msisdn)::bigint as subscriber_fk, max(amount_loanable)::integer as cents_loanable, max(segment)::integer as segment
into tbl_decisioning2_20191231_new_base
from tbl_decisioning2_20191231_new
where amount_loanable <> 0
group by 1
order by 1


------Check the base

---new adds
select count(t1.subscriber_fk) as NewAddition from tbl_decisioning2_20191231_new_base t1 left join tbl_decisioning2_new t2 
on t1.subscriber_fk=t2.subscriber_fk where t2.subscriber_fk is null; --133645

---Dropout
select count(t2.subscriber_fk) as dropouts from tbl_decisioning2_20191231_new_base t1 right join tbl_decisioning2_new t2 
on t1.subscriber_fk=t2.subscriber_fk where t1.subscriber_fk is null; --165229

--Check Upgraders
select count(t1.subscriber_fk) as upgarder from tbl_decisioning2_20191231_new_base t1 left join tbl_decisioning2_new t2 
on t1.subscriber_fk=t2.subscriber_fk where t1.cents_loanable>t2.cents_loanable; --86514

--Downgraders
select count(t1.subscriber_fk) as downgrade from tbl_decisioning2_20191231_new_base t1 left join tbl_decisioning2_new t2 
on t1.subscriber_fk=t2.subscriber_fk where t1.cents_loanable<t2.cents_loanable; --176640

--No Change
select count(t1.subscriber_fk) as nochange from tbl_decisioning2_20191231_new_base t1 left join tbl_decisioning2_new t2 
on t1.subscriber_fk=t2.subscriber_fk where t1.cents_loanable=t2.cents_loanable --325621


---Creat backup of current tbl_decisioning2_new table and update new base into tbl_decisioning2_new

create table tbl_decisioning2_new_backup_05Dec2019 as select * from tbl_decisioning2_new

truncate table tbl_decisioning2_new

insert into tbl_decisioning2_new select subscriber_fk::bigint as subscriber_fk, cents_loanable::bigint as cents_loanable 
from tbl_decisioning2_20191231_new_base;


UPDATE tbl_decisioning2_new SET allowed_services = '{"regular"}' WHERE allowed_services is null;


----Check the data of tbl_decisioning2_new
select * from tbl_decisioning2_new limit 5 

select cents_loanable, count(*) from tbl_decisioning2_new group by 1 order by 1;

