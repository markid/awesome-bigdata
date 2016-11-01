CREATE EXTERNAL TABLE IF NOT EXISTS ${hbase_hive_name}(key string, month map<string,string>, day map<string,string>) 
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key, month:, day:") 
TBLPROPERTIES ("hbase.table.name" = '${hbase_table_name}');

insert overwrite table ${hbase_hive_name}
select IMSI_NO as key,
map(
	'IMEI_TIMES',case
    when IMEI_TIMES <= 6 then 'A'
	when IMEI_TIMES > 6 and IMEI_TIMES <=12 then 'B'
	when IMEI_TIMES > 12 and IMEI_TIMES <=24 then 'C'
	when IMEI_TIMES > 24 and IMEI_TIMES <=30 then 'D'
	when IMEI_TIMES > 30 and IMEI_TIMES <=36 then 'E'
	when IMEI_TIMES > 36 then 'F'
    else 'Z' end,
	'MACHINE_USE_TYPE',case
	when MACHINE_USE_TYPE = 1 then 'A'
	when MACHINE_USE_TYPE = 0 then 'B'
	else 'Z' end,
	'IS_CONTRACT_MACHINE',case
	when IS_CONTRACT_MACHINE = 1 then 'A'
	when IS_CONTRACT_MACHINE = 0 then 'B'
	else 'Z' end,
	'LAST_CHANGE',case
	when LAST_CHANGE = 1 then 'A'
	when LAST_CHANGE = 0 then 'B'
	else 'Z' end,
	'IS_ZD_END',case
	when year(IS_ZD_END)=year(date) and month(IS_ZD_END)= month(date+1) then 'A'
	when IS_ZD_END is not null and IS_ZD_END != '' then 'B'
	else 'Z' end,
	--'IMEI_NO'
	--'MACHINE_BRAND'
	--'MACHINE_TYPE'
	
	'MACHINE_STANDARD',case
	when charindex('2G',MACHINE_STANDARD)>0 then 'A'
	when charindex('3G',MACHINE_STANDARD)>0 then 'B'
	when charindex('4G',MACHINE_STANDARD)>0 then 'C'
	else 'Z' end,
	'OS_FLAG',case
	when charindex('ios',OS_FLAG)>0 then 'A'
	when charindex('android',OS_FLAG)>0 then 'B'
	when charindex('windows',OS_FLAG)>0 then 'C'
	else 'Z' end,
	--'TD_SMART_FLAG'
	
	'ZD_END_TIME',case
	when datediff(from_unixtime(unix_timestamp(), "yyyy-MM-dd"), from_unixtime(unix_timestamp(ZD_END_TIME, "yyyyMMdd"), "yyyy-MM-dd")) <= 30 then 'A'
	when datediff(from_unixtime(unix_timestamp(), "yyyy-MM-dd"), from_unixtime(unix_timestamp(ZD_END_TIME, "yyyyMMdd"), "yyyy-MM-dd")) > 30 and when datediff(from_unixtime(unix_timestamp(), "yyyy-MM-dd"), from_unixtime(unix_timestamp(ZD_END_TIME, "yyyyMMdd"), "yyyy-MM-dd")) <= 60 then 'B'
	when datediff(from_unixtime(unix_timestamp(), "yyyy-MM-dd"), from_unixtime(unix_timestamp(ZD_END_TIME, "yyyyMMdd"), "yyyy-MM-dd")) > 60 and when datediff(from_unixtime(unix_timestamp(), "yyyy-MM-dd"), from_unixtime(unix_timestamp(ZD_END_TIME, "yyyyMMdd"), "yyyy-MM-dd")) <= 90 then 'C'
	when datediff(from_unixtime(unix_timestamp(), "yyyy-MM-dd"), from_unixtime(unix_timestamp(ZD_END_TIME, "yyyyMMdd"), "yyyy-MM-dd")) > 90 then 'D'
	else 'Z' end,
	'CALL_BUNDLE_FLAG',case
	when CALL_BUNDLE_FLAG = 1 then 'A'
	when CALL_BUNDLE_FLAG = 0 then 'B'
	else 'Z' end,
	'MACHINE_BUNDLE_FLAG'
	when MACHINE_BUNDLE_FLAG = 1 then 'A'
	when MACHINE_BUNDLE_FLAG = 0 then 'B'
	else 'Z' end,
	'COMMODITY_BUNDLE_FLAG'
	when COMMODITY_BUNDLE_FLAG = 1 then 'A'
	when COMMODITY_BUNDLE_FLAG = 0 then 'B'
	else 'Z' end,
	'COMMODITY_LOW'
	when COMMODITY_BUNDLE_FLAG = 1 then 'A'
	when COMMODITY_BUNDLE_FLAG = 0 then 'B'
	else 'Z' end

	--'LOWFEE_END_TIME'
	
	--'NET_ACTIVE_FLAG'

) as month,
map('default', 'null') as day from ${widetable_month};


