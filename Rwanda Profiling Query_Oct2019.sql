--------create table for data uploading
--alter table tbl_rwanda_data_temp_201902_new alter column msisdn set data type text;

create table tbl_rwanda_data_temp_201906_new as select * from tbl_rwanda_data_temp_201904_new limit 0;
create table tbl_rwanda_data_temp_201907_new as select * from tbl_rwanda_data_temp_201906_new limit 0;
create table tbl_rwanda_data_temp_201908_new as select * from tbl_rwanda_data_temp_201906_new limit 0;

select * from tbl_rwanda_data_temp_201907_new limit 5;
select * from tbl_rwanda_data_temp_201908_new limit 5;

----select * from tbl_cdr_refills limit 5;
select distinct(length(msisdn::text)),count(*) from tbl_rwanda_data_temp_201906_new group by 1;  -- 9;3990569
select distinct(length(msisdn::text)),count(*) from tbl_rwanda_data_temp_201907_new group by 1;  -- 12;4118691
select distinct(length(msisdn::text)),count(*) from tbl_rwanda_data_temp_201908_new group by 1;  -- 12;4184127

update tbl_rwanda_data_temp_201907_new set msisdn= substring(msisdn::text,4,12)::bigint;  --50714ms
update tbl_rwanda_data_temp_201908_new set msisdn= substring(msisdn::text,4,12)::bigint;  --51700 

select distinct(length(msisdn::text)),count(*) from tbl_rwanda_data_temp_201906_new group by 1  -- 9;3990569
select distinct(length(msisdn::text)),count(*) from tbl_rwanda_data_temp_201907_new group by 1  -- 9;4118691
select distinct(length(msisdn::text)),count(*) from tbl_rwanda_data_temp_201908_new group by 1  -- 9;4184127

---Check the data;
select min(activation_date), max(activation_date) from tbl_rwanda_data_temp_201907_new; --"1999-03-09";"2019-09-15"
select min(activation_date), max(activation_date) from tbl_rwanda_data_temp_201908_new; --"1999-03-09";"2019-09-13"

select * from tbl_rwanda_data_temp_201907_new where refill_amount>0 and usage_amount>0 limit 10;
select * from tbl_rwanda_data_temp_201908_new where msisdn in (
785742578,
784585068,
782436910,
783368959,
785742590,
784439317,
785516281,
783320622,
784412351,
782322405
)

----check refill_count, refill_amount;
select count(*), sum(refill_count), sum(refill_amount) from tbl_rwanda_data_temp_201908_new; --4184127;38381124;11074660752
select count(*), sum(refill_count), sum(refill_amount) from tbl_rwanda_data_temp_201907_new; --4118691;38075321;10928597566

select * from tbl_subscriber_activations_tmp limit 5;
alter table tbl_subscriber_activations_tmp rename to tbl_subscriber_activations_tmp_new

select min(activation_date::date), max(activation_date::date) from tbl_subscriber_activations_tmp where activation_date is not null; --"1998-09-01";"2019-09-13"
select min(activation_date::date), max(activation_date::date) from tbl_subscriber_activations_tmp_new where activation_date is not null; ---"1998-09-01";"2019-09-11"

------create and update activation table 
alter table tbl_subscriber_activations_tmp_new rename to tbl_subscriber_activations_tmp_old_11sep2019;
create table tbl_subscriber_activations_tmp as select * from tbl_rwanda_data_temp_201908_new;
select * from tbl_subscriber_activations_tmp limit 5;
alter table tbl_subscriber_activations_tmp drop column usage_amount, drop column usage_count, drop column refill_amount, drop column refill_count;
select count(*) from tbl_subscriber_activations_tmp;  --4,184,127

select count(*) from tbl_rwanda_data_temp_201907_new where msisdn not in (select msisdn from tbl_subscriber_activations_tmp); ---2789797
-- select count(*) from tbl_rwanda_data_temp_201906_new where msisdn not in (select msisdn from tbl_subscriber_activations_tmp); ---368449

select count(*) from tbl_subscriber_activations_tmp --4487161
-- select count(distinct msisdn) from tbl_subscriber_activations_tmp --5,683,629

--------returns count of New Subscribers-- Don't use if Telco share the latest full active base.
select count(*) from (select msisdn::bigint from tbl_rwanda_data_temp_201811_new
except
select msisdn::bigint from tbl_subscriber_activations_tmp)b; --136994

----returns New Subscribers
drop table new_additions;

create table new_additions as
select msisdn::bigint from tbl_rwanda_data_temp_201811_new
except
select msisdn::bigint from tbl_subscriber_activations_tmp;
---Query returned successfully: 375099 rows affected, 5750 ms execution time.

select * from new_additions limit 5;

--append their Activations dates as well
drop table new_additions_final;

create table new_additions_final as select a.msisdn::bigint,service_class, activation_date, kyc_status from 
(select msisdn::bigint from new_additions) a
left outer join
(select msisdn::bigint,max(service_class) as service_class, max(activation_date)::date activation_date,kyc_status from tbl_rwanda_data_temp_201811_new group by msisdn,kyc_status) b
on a.msisdn::bigint=b.msisdn::bigint;
---Query returned successfully: 375099 rows affected, 13705 ms execution time.

-- select count(*) from tbl_subscriber_activations_tmp_old where activation_date::date is null;
select min(activation_date)::date, max(activation_date)::date from new_additions_final; --"1998-09-01";"2019-09-11"
select min(activation_date)::date, max(activation_date)::date from new_additions_final where activation_date!='NULL'; --"19980901";"NULL"

select count(*) from new_additions_final where activation_date='NULL'; --3208 ---2311500 
delete from new_additions_final where activation_date ='NULL';

select * from new_additions_final limit 5;
---insert into  new Additions into the master Base
select * from tbl_subscriber_activations_tmp limit 5;

insert into tbl_subscriber_activations_tmp(msisdn,service_class,activation_date,kyc_status) select msisdn::bigint,service_class,activation_date::date,kyc_status from new_additions_final;
---Query returned successfully: 375099 rows affected, 1094 ms execution time.

select count(*) from tbl_subscriber_activations_tmp;--6,046,331
select count(distinct msisdn) from tbl_subscriber_activations_tmp; --6,046,314

select distinct(length(msisdn::text)) from tbl_subscriber_activations_tmp --9

--remove duplicate
select msisdn, count(1) from 
(select t1.*,ROW_NUMBER() OVER(partition by msisdn ORDER BY msisdn asc)r1 from tbl_subscriber_activations_tmp t1) t2
WHERE r1=2 group by 1 order by 1; ---10907ms

select * from tbl_subscriber_activations_tmp where msisdn in (
780867367,
782261425,
785450031,
788200337,
788350358,
788475528,
789968280
)

delete from tbl_subscriber_activations_tmp where msisdn in(780065661, 780192179,780825936,781180873,781701957,783347715,
784409859, 784514372, 787560333, 788329753) and service_class=46;

delete from tbl_subscriber_activations_tmp where msisdn::bigint=785450031 and activation_date='03-07-2018' ---and service_class=46

--delete from tbl_subscriber_activations_tmp where msisdn=787011351 and activation_date ='NULL'

select distinct(length(msisdn::text)) from tbl_subscriber_activations_tmp -- 9
select distinct(length(subscriber_fk::text)) from tbl_decisioning2_new  -- 9

select distinct kyc_status as ks, count(*) from tbl_subscriber_activations_tmp group by ks 
--"AC";6046314

select * from tbl_subscriber_activations_tmp limit 5;

----Import data from BI ftp server
--copy tbl_rwanda_data_temp_2019021 from '/data1/bi/incoming/MTN_RW_PROFILING_STONE/Ihereze_Scoring_2019021.csv' with csv header ---123343ms
--copy tbl_rwanda_data_temp_new from '/data1/bi/incoming/MTN_RW_PROFILING_STONE/IHEREZE_Scoring June 2018.csv' with csv header --3342409---3388036

select * from tbl_rwanda_data_temp_201908_new limit 5
select kyc_status,count(*) from tbl_rwanda_data_temp_201908_new group by 1 order by 1
---"AC";4184127

select min(activation_date), max(activation_date) from tbl_subscriber_activations_tmp -- where activation_date != 'NULL' --"1998-09-01";"2019-09-15"
select min(activation_date), max(activation_date) from tbl_rwanda_data_temp_201908_new -- where activation_date != "NULL"; --"1999-03-09";"2019-09-13"
select min(activation_date), max(activation_date) from tbl_rwanda_data_temp_201907_new -- where activation_date != "NULL"; --"1999-03-09";"2019-09-15"

--------returns count of New Subscribers
select extract (year from activation_date) active_year, extract(month from activation_date) active_month, count(*) from tbl_subscriber_activations_tmp group by 1,2 order by 1,2
---------------------------------------------------------------------------------------------------
select * from tbl_subscriber_usage_201907 limit 5;
drop table tbl_subscriber_usage_201908;
select * from tbl_subscriber_topups_201908 limit 5;
drop table tbl_subscriber_topups_201908;
select msisdn msisdn,usage_amount usage_amount into tbl_subscriber_usage_201908 from tbl_rwanda_data_temp_201908_new;  
---Query returned successfully: 4184127 rows affected, 4547 ms execution time.
select msisdn msisdn,usage_amount usage_amount into tbl_subscriber_usage_201907 from tbl_rwanda_data_temp_201907_new;  ---26750ms
---Query returned successfully: 4118691 rows affected, 5781 ms execution time.

select msisdn msisdn, refill_amount topup_value into tbl_subscriber_topups_201908 from  tbl_rwanda_data_temp_201908_new;
--Query returned successfully: 4184127 rows affected, 4735 ms execution time.
select msisdn msisdn, refill_amount topup_value into tbl_subscriber_topups_201907 from  tbl_rwanda_data_temp_201907_new;
---Query returned successfully: 4118691 rows affected, 4688 ms execution time.

select distinct(length(msisdn::text)),count(*) from tbl_subscriber_topups_201907 group by 1 order by 1 ---9;4118691
select distinct(length(msisdn::text)),count(*) from tbl_subscriber_topups_201908 group by 1 order by 1 ---9;4184127
select distinct(length(msisdn::text)),count(*) from tbl_subscriber_usage_201907 group by 1 order by 1 ---9;4118691
select distinct(length(msisdn::text)),count(*) from tbl_subscriber_usage_201908 group by 1 order by 1 ---9;4184127

select count(*) count, sum(usage_amount) usage from tbl_subscriber_usage_201906 where usage_amount > 0; --3571451;8451385200.3766
select count(*) count, sum(usage_amount) usage from tbl_subscriber_usage_201907 where usage_amount > 0; --3707059;9170761240.9814
select count(*) count, sum(usage_amount) usage from tbl_subscriber_usage_201908 where usage_amount > 0; --3782928;9149108422.0680

select count(*) count, sum(usage_amount) usage from tbl_rwanda_data_temp_201906_new where usage_amount > 0; --3571451;8451385200.3766
select count(*) count, sum(usage_amount) usage from tbl_rwanda_data_temp_201907_new where usage_amount > 0; --3707059;9170761240.9814
select count(*) count, sum(usage_amount) usage from tbl_rwanda_data_temp_201908_new where usage_amount > 0; --3782928;9149108422.0680

select count(distinct msisdn), sum(refill_count) r_count, sum(refill_amount) as r_amount from tbl_rwanda_data_temp_201906_new; --3990560;34382442;9954469583
select count(distinct msisdn), sum(refill_count) r_count, sum(refill_amount) as r_amount from tbl_rwanda_data_temp_201907_new; --4118673;38075321;10928597566
select count(distinct msisdn), sum(refill_count) r_count, sum(refill_amount) as r_amount from tbl_rwanda_data_temp_201908_new; --4184110;38381124;11074660752

select * from tbl_subscriber_usage_201908 limit 5;
--alter table tbl_subscriber_usage_201902 alter column msisdn set data type bigint;
--ALTER TABLE tbl_subscriber_usage_201902 ALTER COLUMN msisdn type bigint USING msisdn::bigint;
---------------------------------------------------------------------------------------------------

CREATE INDEX tbl_subscriber_activations_tmp_msisdn_idx
  ON tbl_subscriber_activations_tmp
  USING btree
  (msisdn);

  CREATE INDEX tbl_subscriber_activations_tmp_activation_date_idx
  ON tbl_subscriber_activations_tmp
  USING btree
  (activation_date);
  
CREATE INDEX tbl_subscriber_usage_201902_msisdn_idx
  ON tbl_subscriber_usage_201902
  USING btree
  (msisdn);

  CREATE INDEX tbl_subscriber_usage_201903_msisdn_idx
  ON tbl_subscriber_usage_201903
  USING btree
  (msisdn);

  CREATE INDEX tbl_subscriber_usage_201906_msisdn_idx
  ON tbl_subscriber_usage_201906
  USING btree
  (msisdn);

  CREATE INDEX tbl_subscriber_usage_201907_msisdn_idx
  ON tbl_subscriber_usage_201907
  USING btree
  (msisdn);

  CREATE INDEX tbl_subscriber_usage_201908_msisdn_idx
  ON tbl_subscriber_usage_201908
  USING btree
  (msisdn);

select * from tbl_subscriber_usage_201908 limit 5
select * from tbl_subscriber_activations_tmp limit 5

--- take backup of tables into another DB split having space
--- copy (select * from tbl_subscriber_usage_201902) to '/data2/db_backup/tbl_subscriber_usage_201902.csv' delimiter ',' csv header;
--- copy  tbl_subscriber_usage_201902 from '/data2/db_backup/tbl_subscriber_usage_201902.csv' delimiter ',' csv header;
---------------------------------------------------------------------------------------------------
select t1.msisdn, t1.activation_date, t1.kyc_status,
usage_201906,usage_201907,usage_201908,(null)::integer three_month_wavg,(null)::double precision amount_loanable, (current_date::date - activation_date::date) age, 
(null)::numeric default_probability,(null)::integer segment,(null)::integer age_in_months,(null)::integer dormant_days
into tbl_decisioning_20190831
  from (select msisdn,max(activation_date::date) activation_date, kyc_status
    from tbl_subscriber_activations_tmp ---where activation_date != 'NULL'
    group by msisdn,activation_date,kyc_status) t1
  left join (select msisdn,max(usage_amount) usage_201906 
      from tbl_subscriber_usage_201906
      group by msisdn) t2
    on t1.msisdn::bigint = t2.msisdn::bigint
  left join (select msisdn,max(usage_amount) usage_201907 
      from tbl_subscriber_usage_201907
      group by msisdn) t3
    on t1.msisdn::bigint = t3.msisdn::bigint
  left join (select msisdn,max(usage_amount) usage_201908
      from tbl_subscriber_usage_201908
      group by msisdn) t4
    on t1.msisdn::bigint = t4.msisdn::bigint;

---Query returned successfully: 6973907 rows affected, 101107 ms execution time.
select * from tbl_decisioning_20190831 limit 5;
---------------------------------------------------------------------------------------------------
update tbl_decisioning_20190831 set usage_201906=0 where usage_201906 is null; ---55063ms
update tbl_decisioning_20190831 set usage_201907=0 where usage_201907 is null; ---56414ms
update tbl_decisioning_20190831 set usage_201908=0 where usage_201908 is null; ---54850 ms

select sum(usage_201906), sum(usage_201907), sum(usage_201908) from tbl_decisioning_20190831;

update tbl_decisioning_20190831 t1
  set three_month_wavg = t2.wavg_three_months 
  from (select msisdn, 
      (((coalesce(usage_201906,0)::numeric * 1) + 
      (coalesce(usage_201907,0)::numeric * 2) + 
      (coalesce(usage_201908,0)::numeric * 3))/6)::bigint  as wavg_three_months
    from tbl_decisioning_20190831) t2
  where t1.msisdn = t2.msisdn;
  
--Query returned successfully: 6046314 rows affected, 213637 ms execution time.
---- select * from tbl_decisioning_20190630 limit 5
---------------------------------------------------------------------------------------------------
-------------------------------NEW PROFILING-------------------------------------------------------

update tbl_decisioning_20190831 set age_in_months = (age/30) where age_in_months is null; ---123365ms

---update denom
update tbl_decisioning_20190831 t1
 set amount_loanable = (case when t1.age >= '90' and three_month_wavg >= 40000 then 10000
       when t1.age >= '90' and three_month_wavg >= 20000 then 5000
       when t1.age >= '90' and three_month_wavg >= 8000 then 2000
       when t1.age >= '90' and three_month_wavg >= 4000 then 1000
       when t1.age >= '90' and three_month_wavg >= 1500 then 500
       when t1.age >= '90' and three_month_wavg >= 750 then 250
       when t1.age >= '90' and three_month_wavg >= 300 then 100
       when t1.age >= '90' and three_month_wavg >= 150 then 50
       when usage_201908 > 0 then 50
                else 0 
    end);
---Query returned successfully: 6046314 rows affected, 87460 ms execution time.

----Create new table with denom >0
--alter table tbl_decisioning_20190831_new rename to tbl_decisioning_20190831_Old;

select * from tbl_decisioning_20190831 limit 5;
Create table tbl_decisioning_20190831_new as select * from tbl_decisioning_20190831 where amount_loanable>0;
---Query returned successfully: 3968182 rows affected, 28428 ms execution time.
select * from tbl_decisioning_20190831_new limit 5
select amount_loanable, count(*) from tbl_decisioning_20190831_new group by 1 order by 1; -- where age_in_months is null; --0
---Query returned successfully: 6597146 rows affected, 75134 ms execution time.
select * from tbl_decisioning_20190831_new limit 5;

---update credit scoring
update tbl_decisioning_20190831_new t1
 set default_probability = t2.pd 
 from (select msisdn,(1/(1+ 2.71828281828 ^ (-2.271256-(0.018098*coalesce(age_in_months,0)::numeric)-(0.013754*coalesce(usage_201908/100,0)::bigint))))  as pd
  from tbl_decisioning_20190831_new) as t2
 where t1.msisdn=t2.msisdn;
---Query returned successfully: 3968181 rows affected, 369544 ms execution time.

----update segment 
update tbl_decisioning_20190831_new t1
 set segment= (case when t1.default_probability >= 0.993670475714178 then 1
       when t1.default_probability >= 0.982670475714178 then 2
       when t1.default_probability >= 0.971670475714178 then 3
       when t1.default_probability >= 0.960670475714178 then 4
       when t1.default_probability >= 0.949670475714178 then 5
       when t1.default_probability >= 0.938670475714178 then 6
       when t1.default_probability < 0.938670475714178 then 7                
    end);
---Query returned successfully: 3968181 rows affected, 369544 ms execution time.

---update dormant days flag
update tbl_decisioning_20190831_new t1
 set dormant_days = (case 
       when usage_201908 > 0 then 1
       when usage_201908+usage_201907 > 0 then 2
       else 3 
    end);
---Query returned successfully: 3968181 rows affected, 73234 ms execution time.

select segment, count(*) from tbl_decisioning_20190831_new group by 1 order by 1;

select count(*) from tbl_decisioning_20190831_new where amount_loanable>0; ---3968181
select segment, amount_loanable, count(*) from tbl_decisioning_20190831_new where amount_loanable>0 group by 1,2 order by 1,2; ---3968181

--Query returned successfully: 3472776 rows affected, 9266 ms execution time.

select segment, dormant_days, amount_loanable, count(*) from tbl_decisioning_20190831_new where amount_loanable>0 group by 1,2,3 order by 1,2,3;

select segment, amount_loanable, count(*) from tbl_decisioning_20190831_new group by 1,2 order by 1,2; --3760231

----calculate loan_count & Avg_repay_time
create table am_tbl_loans_26Jul_26Aug_2018 as select * 
 from  
   dblink('host=10.122.40.6 port=37821 user=amishra password=@amishra1 dbname=live_ecs_rw', 
      'select subscriber_fk, loan_time,repay_time,loan_id,cents_loaned,cents_serviceq,cents_paid from tbl_loans') 
as t2 (subscriber_fk text,loan_time timestamp ,repay_time timestamp, loan_id bigint,cents_loaned float, cents_serviceq float,cents_paid float) 
where loan_time between '2018-07-26 00:00:00.00000' and '2018-08-25 23:59:59.9999999'
-----Query returned successfully: 5832639 rows affected, 73885 ms execution time.

select * from am_tbl_loans_26Jul_26Aug_2018 limit 5;

select * from tbl_decisioning_20190831_new limit 5;

select segment, count(*) from tbl_decisioning_20190831_new where amount_loanable>0 group by 1 order by 1

update tbl_decisioning_20190831_new set amount_loanable = 50 where amount_loanable = 100 and segment = 7; --349540  
update tbl_decisioning_20190831_new set amount_loanable = 50 where amount_loanable = 250 and segment = 7; --238476   
update tbl_decisioning_20190831_new set amount_loanable = 100 where amount_loanable = 500 and segment = 7; --107765  
update tbl_decisioning_20190831_new set amount_loanable = 100 where amount_loanable = 1000 and segment = 7; --3449  
update tbl_decisioning_20190831_new set amount_loanable = 250 where amount_loanable = 2000 and segment = 7; --629  
update tbl_decisioning_20190831_new set amount_loanable = 500 where amount_loanable = 5000 and segment = 7; --37
update tbl_decisioning_20190831_new set amount_loanable = 500 where amount_loanable = 10000 and segment = 7; --6

update tbl_decisioning_20190831_new set amount_loanable = 100 where amount_loanable = 250 and segment = 6; --107514   
update tbl_decisioning_20190831_new set amount_loanable = 100 where amount_loanable = 500 and segment = 6; --151187   
update tbl_decisioning_20190831_new set amount_loanable = 250 where amount_loanable = 1000 and segment = 6; --6951  
update tbl_decisioning_20190831_new set amount_loanable = 500 where amount_loanable = 2000 and segment = 6; --263 
update tbl_decisioning_20190831_new set amount_loanable = 500 where amount_loanable = 5000 and segment = 6; --14
update tbl_decisioning_20190831_new set amount_loanable = 1000 where amount_loanable = 10000 and segment = 6; --1

update tbl_decisioning_20190831_new set amount_loanable = 250 where amount_loanable = 500 and segment = 5; --151855  
update tbl_decisioning_20190831_new set amount_loanable = 250 where amount_loanable = 1000 and segment = 5; --33790  
update tbl_decisioning_20190831_new set amount_loanable = 500 where amount_loanable = 2000 and segment = 5; --640 
update tbl_decisioning_20190831_new set amount_loanable = 1000 where amount_loanable = 5000 and segment = 5; --10
update tbl_decisioning_20190831_new set amount_loanable = 1000 where amount_loanable = 10000 and segment = 5; --1

update tbl_decisioning_20190831_new set amount_loanable = 250 where amount_loanable = 500 and segment = 4; --105952  
update tbl_decisioning_20190831_new set amount_loanable = 500 where amount_loanable = 1000 and segment = 4; --70370  
update tbl_decisioning_20190831_new set amount_loanable = 500 where amount_loanable = 2000 and segment = 4; --2737  
update tbl_decisioning_20190831_new set amount_loanable = 1000 where amount_loanable = 5000 and segment = 4; --24
update tbl_decisioning_20190831_new set amount_loanable = 2000 where amount_loanable = 10000 and segment = 4; --1

update tbl_decisioning_20190831_new set amount_loanable = 500 where amount_loanable = 1000 and segment = 3; --77048  
update tbl_decisioning_20190831_new set amount_loanable = 1000 where amount_loanable = 2000 and segment = 3; --17454  
update tbl_decisioning_20190831_new set amount_loanable = 2000 where amount_loanable = 5000 and segment = 3; --51
update tbl_decisioning_20190831_new set amount_loanable = 2000 where amount_loanable = 10000 and segment = 3; --1

update tbl_decisioning_20190831_new set amount_loanable = 5000 where amount_loanable = 10000 and segment = 2; --10

select segment, amount_loanable, count(*) from tbl_decisioning_20190831_new group by 1,2 order by 1,2;

select segment, dormant_days, amount_loanable, count(*) from tbl_decisioning_20190831_new group by 1,2,3 order by 1,2,3;

select * from tbl_decisioning_20190831_new limit 5;
three_month_wavg>0, usage_201908>0 segment 1;
-----upgrade subs
select count(*) from tbl_decisioning_20190831_new where amount_loanable=5000 and age>=90 and three_month_wavg>=35000 and three_month_wavg<40000 and segment=1 and usage_201908>=35000; 
--1568
select count(*) from tbl_decisioning_20190831_new where amount_loanable=2000 and age>=90 and three_month_wavg>=17500 and three_month_wavg<20000 and segment=1 and usage_201908>=17500;
--7706
select count(*) from tbl_decisioning_20190831_new where amount_loanable=1000 and age>=90 and three_month_wavg>=7000 and three_month_wavg<8000 and segment=1 and usage_201908>=7000;
--18396
select count(*) from tbl_decisioning_20190831_new where amount_loanable=500 and age>=90 and three_month_wavg>=3000 and three_month_wavg<4000 and segment=1 and usage_201908>=3000;
--38924
select count(*) from tbl_decisioning_20190831_new where amount_loanable=250 and age>=90 and three_month_wavg>=1000 and three_month_wavg<1500 and segment=1 and usage_201908>=1000;
--12545
select count(*) from tbl_decisioning_20190831_new where amount_loanable=100 and age>=90 and three_month_wavg>=500 and three_month_wavg<750 and segment<=2 and usage_201908>=500;
--53854
select count(*) from tbl_decisioning_20190831_new where amount_loanable=50 and age>=90 and three_month_wavg>=200 and three_month_wavg<300 and segment<=2 and usage_201908>=200;
--26403

update tbl_decisioning_20190831_new set amount_loanable = 10000 where amount_loanable=5000 and age>=90 and three_month_wavg>=35000 and three_month_wavg<40000 and segment=1 and usage_201908>=35000 ;
--1568 
update tbl_decisioning_20190831_new set amount_loanable = 5000 where amount_loanable=2000 and age>=90 and three_month_wavg>=17500 and three_month_wavg<20000 and segment=1 and usage_201908>=17500;
--7706 
update tbl_decisioning_20190831_new set amount_loanable = 2000 where amount_loanable=1000 and age>=90 and three_month_wavg>=7000 and three_month_wavg<8000 and segment=1 and usage_201908>=7000;
--18396 
update tbl_decisioning_20190831_new set amount_loanable = 1000 where amount_loanable=500 and age>=90 and three_month_wavg>=3000 and three_month_wavg<4000 and segment=1 and usage_201908>=3000;
--38924 
update tbl_decisioning_20190831_new set amount_loanable = 500 where amount_loanable=250 and age>=90 and three_month_wavg>=1000 and three_month_wavg<1500 and segment=1 and usage_201908>=1000;
--12545 
update tbl_decisioning_20190831_new set amount_loanable = 250 where amount_loanable=100 and age>=90 and three_month_wavg>=500 and three_month_wavg<750 and segment<=2 and usage_201908>=500;
--53854 
update tbl_decisioning_20190831_new set amount_loanable = 100 where amount_loanable=50 and age>=90 and three_month_wavg>=200 and three_month_wavg<300 and segment<=2 and usage_201908>=200;
--26403 

select segment, amount_loanable, count(*) from tbl_decisioning_20190831_new group by 1,2 order by 1,2;

select max(activation_date) from tbl_subscriber_activations_tmp; --"2019-09-11"

----update missing activation_date into master activation_table
select count(*) from tbl_subscriber_activations_tmp where activation_date is null and msisdn in (select msisdn from tbl_rwanda_data_temp_201905_new where activation_date is not null); --161 --97

update tbl_subscriber_activations_tmp t1 set activation_date=t2.activation_date from (select msisdn::bigint, max(activation_date) as activation_date from tbl_rwanda_data_temp_201905_new group by 1 order by 1) t2
where t1.msisdn::bigint=t2.msisdn::bigint and t1.activation_date is null;

select * from tbl_rwanda_data_temp_201902_new limit 5;

---update missing subscriber into decisioning_table
select count(*) from tbl_decisioning_20190630 where activation_date is null and msisdn in (select msisdn from tbl_subscriber_activations_tmp where activation_date is not null); --161 --97

select count(*) from tbl_decisioning_20190630 where activation_date is null and msisdn in (select msisdn from tbl_subscriber_activations_tmp where activation_date is not null); --161 --97

select count(*) from tbl_decisioning_20190630 where three_month_wavg>0 and age>0 and amount_loanable=0; ---407002
select count(*) from tbl_decisioning_20190630 where three_month_wavg>0 and age>90 and amount_loanable=0; ---374105
select * from tbl_decisioning_20190630 where three_month_wavg>0 and age>90 and amount_loanable=0 limit 100; ---374105
select count(*) from tbl_decisioning_20190630 where three_month_wavg>0 and age<90 and amount_loanable=0; ---32153

select count(*) from tbl_decisioning_20190630 where three_month_wavg>0 and age>0 and age<90 and msisdn not in (select msisdn from tbl_decisioning_20190831_new); --407002 --32153

select * from tbl_decisioning_20190630 where three_month_wavg>0 and age>0 and msisdn not in (select msisdn from tbl_decisioning_20190831_new) limit 100; --407002

update tbl_subscriber_activations_tmp t1 set activation_date=t2.activation_date from (select msisdn::bigint, max(activation_date) as activation_date from tbl_rwanda_data_temp_201905_new group by 1 order by 1) t2
where t1.msisdn::bigint=t2.msisdn::bigint and t1.activation_date is null;

--Query returned successfully: 3472776 rows affected, 57486 ms execution time.
select segment, amount_loanable, count(*) from tbl_decisioning_20190831_new where dormant_days=3 group by 1,2 order by 1,2

select segment, count(*) from tbl_decisioning_20190831_new group by 1 order by 1

select * from tbl_decisioning_20190831_new limit 5

-- select kyc_status, count(distinct msisdn),segment from tbl_decisioning_20190220_new where amount_loanable > 0 and kyc_status = 'AC' group by 2 order by 2
-- select kyc_status, count(*) from tbl_decisioning_20190220_new group by 1 order by 1

select amount_loanable, count(*) from tbl_decisioning_20190831_new group by 1 order by 1

select * from tbl_decisioning2_new limit 5

select * from tbl_decisioning_20190831_new_base limit 5
drop table tbl_decisioning_20190831_new_base

select distinct(msisdn::bigint) subscriber_fk,max(amount_loanable::bigint) cents_loanable,(null)::text loan_type,max(segment) segment, (null)::bigint loan_cap
into tbl_decisioning_20190831_new_base from tbl_decisioning_20190831_new where amount_loanable <> 0 group by 1 order by 1;
---Query returned successfully: 3968181 rows affected, 25064 ms execution time.

select cents_loanable, count (*) count from tbl_decisioning_20190831_new_base group by 1 order by 1;

select * from tbl_decisioning_20190831_new_base limit 5;
----update loan_type
update tbl_decisioning_20190831_new_base set loan_type = 'airtime'
----Query returned successfully: 3472776 rows affected, 45861 ms execution time.

select * from tbl_decisioning2_new limit 5;
select count(*) from tbl_decisioning2_new;

select distinct (segment), min(loan_cap), max(loan_cap) from tbl_decisioning2_new group by 1 order by 1;

----update loan_cap
update tbl_decisioning_20190831_new_base t1 
set loan_cap = (case
 when t1.segment > 5 then 1
 when t1.segment > 2 then 3
 else 9
end);
----Query returned successfully: 3968181 rows affected, 53986 ms execution time.

select distinct (segment), min(loan_cap), max(loan_cap) from tbl_decisioning_20190831_new_base group by 1 order by 1;

select * from tbl_decisioning_20190831_new_base limit 5;
select cents_loanable, count(*) from tbl_decisioning_20190831_new_base group by 1 order by 1;
select cents_loanable, count(*) from tbl_decisioning2_new group by 1 order by 1;

update tbl_decisioning_20190831_new_base set cents_loanable = cents_loanable*100;
----Query returned successfully: 3757182 rows affected, 28959 ms execution time.

select cents_loanable, count(*) from tbl_decisioning_20190831_new_base group by 1 order by 1;
select cents_loanable, count(*) from tbl_decisioning2_new group by 1 order by 1;
-----------************-----------************-----------************-----------************

create table tbl_decisioning2_new_backup_old_20190831 as select * from tbl_decisioning2_new;
select * from tbl_decisioning2_new_backup_old_20190831 limit 5;
select cents_loanable, count(*) from tbl_decisioning2_new_backup_old_20190831 group by 1 order by 1;
select count(*) from tbl_decisioning2_new_backup_old_20190831; --3,472,776
------------------------------------------------------------

truncate table tbl_decisioning2_new;

-------------------------------------------------------------------------
select * from tbl_decisioning2_new limit 5;
select * from tbl_decisioning_20190831_new_base limit 5;
select segment, count(*) from tbl_decisioning_20190831_new_base group by 1 order by 1

----------------------------------------------------------------------------------------------------------------------------------------------------
INSERT INTO tbl_decisioning2_new SELECT subscriber_fk, cents_loanable,loan_type,segment,loan_cap from tbl_decisioning_20190831_new_base;


select * from tbl_decisioning2_new limit 5;

select distinct cents_loanable as cl, count(*)  from tbl_decisioning2_new group by cl order by cl;

------------------------------------------------------------------------------------------------------------
select distinct loan_type, count(*)  from tbl_decisioning2_new group by loan_type ---"airtime";3472776
select count(*) from tbl_decisioning2_new; --3968181
select loan_type, count(*)  from tbl_decisioning2_new group by loan_type;

-------****************************************************************-------
--------*********  copy the base into rawanda test sub db  *********--------
-------****************************************************************-------


---New Adds
select count(*) from
(select subscriber_fk from tbl_decisioning_20190831_new_base 
except
select subscriber_fk from tbl_decisioning2_new)b; --597416

---Upgraders
select count(t1.subscriber_fk) as upgarder from tbl_decisioning_20190831_new_base t1 left join tbl_decisioning2_new t2 
on t1.subscriber_fk=t2.subscriber_fk where t1.cents_loanable>t2.cents_loanable; --772550

---Downgraders
select count(t1.subscriber_fk) as downgrder from tbl_decisioning_20190831_new_base t1 left join tbl_decisioning2_new t2 
on t1.subscriber_fk=t2.subscriber_fk where t1.cents_loanable<t2.cents_loanable; --463152

---Dropouts
select count(t2.subscriber_fk) as dropouts from tbl_decisioning_20190831_new_base t1 right join tbl_decisioning2_new t2 
on t1.subscriber_fk=t2.subscriber_fk where t1.subscriber_fk is null; --386417

select segment,cents_loanable, count(*) from tbl_decisioning2_new group by 1,2 order by 1,2
