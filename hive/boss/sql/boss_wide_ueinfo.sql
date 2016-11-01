use fm;
dfs -mkdir -p ${location};
create external table if not exists ${table_name} (IMSI_NO string, IMEI_TIMES int, MACHINE_USE_TYPE int, 
IS_CONTRACT_MACHINE string, LAST_CHANGE string, IS_ZD_END string,IMEI_NO int,
MACHINE_BRAND string,MACHINE_TYPE string,MACHINE_STANDARD string,OS_FLAG string,TD_SMART_FLAG string,ZD_END_TIME string)
COMMENT 'This is a temperature table for UE information in boss monthly widetable'
row format delimited
  fields terminated by '\001'
  COLLECTION ITEMS TERMINATED BY '\002'
  MAP KEYS TERMINATED BY '\003'
 STORED AS TEXTFILE
 location '${location}';
 
insert overwrite table ${table_name}
select 
t1.imsi_no,
cast(datediff(cast(from_unixtime(unix_timestamp(), "yyyy-MM-dd") as string), cast(from_unixtime(unix_timestamp(cast(t1.IMEI_CHG_YM as string), "yyyyMM"), "yyyy-MM-dd")as string))/30 as int) , 
cast (t1.smart_flag as int), 
case when t1.zd_sale_name is not null and t1.zd_sale_name!='' then 1 else 0 end,
case when t1.imei_no is not null and t1.imei_no!='' and t2.imei_no is not null and t2.imei_no != '' and t1.imei_no != t2.imei_no then 1 else 0 end,
t1.ZD_END_TIME, t1.imei_no,t1.machine_brand, t1.machine_type,t1.machine_standard,t1.os_flag,'',t1.zd_end_time 
from ${original} t1 , ${original_lastm} t2 where t1.imsi_no is not null and t1.imsi_no != '' and  t1.imsi_no =t2.imsi_no;