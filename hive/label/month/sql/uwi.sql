CREATE EXTERNAL TABLE IF NOT EXISTS ${hbase_hive_name}(key string, month map<string,string>, day map<string,string>) 
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key, month:, day:") 
TBLPROPERTIES ("hbase.table.name" = '${hbase_table_name}');

insert overwrite table ${hbase_hive_name}
select IMSI_NO as key,
map(
    'CALLING_NUMBER', case
    when CALLING_NUMBER = 0 then 'A'
    when CALLING_NUMBER > 0 and CALLING_NUMBER<=5 then 'B'
    when CALLING_NUMBER > 5 and CALLING_NUMBER<=10 then 'C'
    when CALLING_NUMBER > 10 and CALLING_NUMBER<=15 then 'D'
    when CALLING_NUMBER > 15 and CALLING_NUMBER<=20 then 'E'
    when CALLING_NUMBER > 20 then 'F'
    else 'Z' end,
    'MIGU',case
    when MIGU is null or MIGU = '' then 'B'
    else 'A' end,
    'SJB', case
    when SJB is null or SJB = '' then 'B'
    else 'A' end,
    'SJYD', case
    when SJYD is null or SJYD = '' then 'B'
    else 'A' end,
    'SJYX',case
    when SJYX is null or SJYX = '' then 'B'
    else 'A' end,
    'SJSP',case
    when SJSP is null or SJSP = '' then 'B'
    else 'A' end
    --'UNIVERSAL_FLOW'
    --'SUB_NETWORK_PACKAGE_FLOW'
    --'SUB_NETWORK_PACKAGE_PROP'
    --'First_Half_Flow'
    --'Second_Half_Flow'
    'G4_USER_FLAG',case
    when G4_USER_FLAG is null or G4_USER_FLAG = '' then 'B'
    else 'A' end,
    'V_BASIC_FEE',case
    when V_BASIC_FEE is null or V_BASIC_FEE = '' then 'B'
    else 'A' end,
    'IS_ONLINE',case
    when IS_ONLINE is null or IS_ONLINE = '' then 'B'
    else 'A' end

    ) as month,
map('default', 'null') as day from ${widetable_month};
    


