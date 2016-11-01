CREATE EXTERNAL TABLE IF NOT EXISTS hbase_hive_client_identity(key string, month map<string,string>, day map<string,string>) 
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key, month:, day:") 
TBLPROPERTIES ("hbase.table.name" = "flowmarketing:label");

insert overwrite table hbase_hive_client_identity
select IMSI_NO as key,
map(

  'OPEN_TIME', case
    when OPEN_TIME is null or OPEN_TIME == '' then 'Z'
    when datediff(from_unixtime(unix_timestamp(), "yyyy-MM-dd"), from_unixtime(unix_timestamp(OPEN_TIME, "yyyyMMdd"), "yyyy-MM-dd")) < 365 then 'A'
    when datediff(from_unixtime(unix_timestamp(), "yyyy-MM-dd"), from_unixtime(unix_timestamp(OPEN_TIME, "yyyyMMdd"), "yyyy-MM-dd")) >= 365 and datediff(from_unixtime(unix_timestamp(), "yyyy-MM-dd"), from_unixtime(unix_timestamp(OPEN_TIME, "yyyyMMdd"), "yyyy-MM-dd")) < 1095 then 'B'
    when datediff(from_unixtime(unix_timestamp(), "yyyy-MM-dd"), from_unixtime(unix_timestamp(OPEN_TIME, "yyyyMMdd"), "yyyy-MM-dd")) >= 1095 and datediff(from_unixtime(unix_timestamp(), "yyyy-MM-dd"), from_unixtime(unix_timestamp(OPEN_TIME, "yyyyMMdd"), "yyyy-MM-dd")) < 1825 then 'C' 
    else 'D' end,
  'IS_GROUP_UNITS', case
    when GROUP_UNITS is null or GROUP_UNITS == '' then 'B'
    else 'A' end,
  'IS_VPMN_UNITS', case
    when VPMN_CODE is null or VPMN_CODE == '' then 'B'
    else 'A' end,
   --'DYNAMIC_TYPE', case
   --'IS_VIP',case
   --'PAY_TYPE',case
   --'HURRY_MODE',case
   'SM_NAME',case
    when SM_NAME = '全球通' then 'A'
    when SM_NAME = '动感地带'then 'B'
    when SM_NAME = '神州行' then 'C'
    else 'Z' end,
    'CREDIT_LEVEL',case
    when charindex('准星'，CREDIT_LEVEL)>0 then 'A'
    when charindex('1星'，CREDIT_LEVEL)>0 then 'B'
    when charindex('2星'，CREDIT_LEVEL)>0 then 'C'
    when charindex('3星'，CREDIT_LEVEL)>0 then 'D'
    when charindex('4星'，CREDIT_LEVEL)>0 then 'E'
    when charindex('5星'，CREDIT_LEVEL)>0 then 'F'
    else 'Z' end,
     ) as month,
map('default', 'null') as day
from fm.widetable_201606;