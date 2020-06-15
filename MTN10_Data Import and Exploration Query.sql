---create a new table or a duplicate table from previous table am_tbl_irancell_mode_usage_96_12_new ;
create table tbl_refill_Mar2019 as select * from tbl_irancell_mode_usage_72m_97_11_new limit 0;

----or

create table tbl_irancell_mode_usage_72m_97_11_new 
(dw_msisdn text,Fake_id text,registration_date text,activation_date text,service_class text,Province text,City text,pre_to_post_flag text,
dormant_days_Status text,MNP_Flag text,Voice_revenue text,voice_second text,PAYG_DATA_revenue text,PAYG_usage_KB text,bolton_rev text,
bolton_volum text,total_data_revenue text,total_data_usage_KB text,sms_revenue text,sms_count text,total_recharge_count text,
total_recharge_amount text,normal_Recharg_count text,normal_Recharge_amount text);

-------update the table from data availble on FTP path will be provided by the Telco;
copy tbl_irancell_mode_usage_72m_97_11_new from '/data/dumps/BIdumps/tbl_irancell_mode_usage_72m_97_11_new.csv' with delimiter';'csv header encoding 'windows-1251';

----------------------------Add and Update avg_total_topup_amount; avg_normal_topup_amount; and total_revenue_201912----------------------------------------------------
alter table tbl_irancell_mode_usage_72m_97_11_new  add column avg_total_topup_amount numeric, add column avg_normal_topup_amount numeric,
add column total_revenue_201805 numeric

UPDATE tbl_irancell_mode_usage_72m_97_11_new SET avg_total_topup_amount = total_recharge_amount::numeric/total_recharge_count::numeric 
where total_recharge_amount::numeric>0;

UPDATE tbl_irancell_mode_usage_72m_97_11_new SET avg_normal_topup_amount = normal_recharge_amount::numeric/normal_recharge_count::numeric 
where normal_recharge_amount::numeric>0;

UPDATE tbl_irancell_mode_usage_72m_97_11_new SET total_revenue_201805 = voice_revenue::numeric + payg_data_revenue::numeric + sms_revenue::numeric;

-------------**********--------------*******Create & update Subscriber Activation table********----------------***************----------------

---Create & update Subscriber Activation table;
---take backup of previous table & create new table having with latest data;
alter table tbl_subscriber_activations_tmp rename to tbl_subscriber_activations_tmp_20191231_Old;

create table tbl_subscriber_activations_tmp (fake_id text, registration_date date, activation_date date);

insert into tbl_subscriber_activations_tmp_new (fake_id, registration_date, activation_date) 
select distinct fake_id as fake_id, max(registration_date::date) as registration_date,
max(activation_date::date) as activation_date from tbl_irancell_mode_usage_72m_97_11_new group by fake_id;

----------**********-----Explore the data in table tbl_irancell_mode_usage_72m_97_11_new---------***************-----

select 
max(registration_date) as max_registration_date, min(registration_date) as min_registration_date,
max(activation_date) as max_activation_date, min(activation_date) as min_activation_date,
max(service_class) as max_service_class, min(service_class) as min_service_class,
max(voice_revenue) as max_voice_revenue, min(voice_revenue) as min_voice_revenue,
sum(voice_revenue::numeric) as sum_voice_revenue, count(voice_revenue::numeric) as count_voice_revenue,
max(voice_second) as max_voice_second, min(voice_second) as min_voice_second,
sum(voice_second::numeric) as sum, count(voice_second::numeric) as count_voice_second,
max(payg_data_revenue) as max_payg_data_revenue, min(payg_data_revenue) as min_payg_data_revenue,
sum(payg_data_revenue::numeric) as sum_payg_data_revenue, count(payg_data_revenue::numeric) as count_payg_data_revenue,
max(payg_usage_kb) as max_payg_usage_kb, min(payg_usage_kb) as min_payg_usage_kb,
sum(payg_usage_kb::numeric) as sum_payg_usage_kb, count(payg_usage_kb::numeric) as count_payg_usage_kb,
max(bolton_rev) as max_bolton_rev, min(bolton_rev) as min_bolton_rev,
sum(bolton_rev::numeric) as sum_bolton_rev, count(bolton_rev::numeric) as count_bolton_rev,
max(bolton_volum) as max_bolton_volum, min(bolton_volum) as min_bolton_volum,
sum(bolton_volum::numeric) as sum_bolton_volum, count(bolton_volum::numeric) as count_bolton_volum,
max(total_data_revenue) as max_total_data_revenue, min(total_data_revenue) as min_total_data_revenue,
sum(total_data_revenue::numeric) as sum_total_data_revenue, count(total_data_revenue::numeric) as count_total_data_revenue,
max(total_data_usage_kb) as max_total_data_usage_kb, min(total_data_usage_kb) as min_total_data_usage_kb,
sum(total_data_usage_kb::numeric) as sum_total_data_usage_kb, count(total_data_usage_kb::numeric) as count_total_data_usage_kb,
max(sms_revenue) as max_sms_revenue, min(sms_revenue) as min_sms_revenue,
sum(sms_revenue::numeric) as sum_sms_revenue, count(sms_revenue::numeric) as count_sms_revenue,
max(sms_count) as max_sms_count, min(sms_count) as min_sms_count,
sum(sms_count::numeric) as sum_sms_count, count(sms_count::numeric) as count_sms_count,
max(total_recharge_count) as max_total_recharge_count, min(total_recharge_count::numeric) as min_total_recharge_count,
sum(total_recharge_count::numeric) as sum_total_recharge_count, count(total_recharge_count::numeric) as count_total_recharge_count,
max(total_recharge_amount) as max_total_recharge_amount, min(total_recharge_amount) as min_total_recharge_amount,
sum(total_recharge_amount::numeric) as sum_total_recharge_amount, count(total_recharge_amount::numeric) as count_total_recharge_amount,
max(normal_recharge_count) as max_normal_recharge_count, min(normal_recharge_count) as min_normal_recharge_count,
sum(normal_recharge_count::numeric) as sum_normal_recharge_count, count(normal_recharge_count::numeric) as count_normal_recharge_count,
max(normal_recharge_amount) as max_normal_recharge_amount, min(normal_recharge_amount) as min_normal_recharge_amount,
sum(normal_recharge_amount::numeric) as sum_normal_recharge_amount, count(normal_recharge_amount::numeric) as count_normal_recharge_amount
from tbl_irancell_mode_usage_72m_97_11_new;





