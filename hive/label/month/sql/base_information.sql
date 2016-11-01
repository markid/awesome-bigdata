CREATE EXTERNAL TABLE IF NOT EXISTS ${hbase_hive_name}(key string, month map<string,string>, day map<string,string>) 
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key, month:, day:") 
TBLPROPERTIES ("hbase.table.name" = '${hbase_table_name}');

insert overwrite table ${hbase_hive_name}
select IMSI_NO as key,
map(
  'PHONE_NO', PHONE_NO,
  'GENDER', case
    when GENDER == '男' or GENDER == 'male' then 'A'
    when GENDER == '女' or GENDER == 'female' then 'B'
    else 'Z' end,
  'BIRTHDAY', case 
    when unix_timestamp(BIRTHDAY, "yyyy/MM/dd") >= unix_timestamp("1999/1/1", "yyyy/MM/dd") and unix_timestamp(BIRTHDAY, "yyyy/MM/dd") <= unix_timestamp("2009/1/1", "yyyy/MM/dd") then 'A'
    when unix_timestamp(BIRTHDAY, "yyyy/MM/dd") >= unix_timestamp("1976/1/1", "yyyy/MM/dd") and unix_timestamp(BIRTHDAY, "yyyy/MM/dd") <= unix_timestamp("1998/12/31", "yyyy/MM/dd") then 'B'
    when unix_timestamp(BIRTHDAY, "yyyy/MM/dd") >= unix_timestamp("1951/1/1", "yyyy/MM/dd") and unix_timestamp(BIRTHDAY, "yyyy/MM/dd") <= unix_timestamp("1975/12/31", "yyyy/MM/dd") then 'C'
    when unix_timestamp(BIRTHDAY, "yyyy/MM/dd") <= unix_timestamp("1950/12/31", "yyyy/MM/dd") then 'D'
    else 'Z' end,
  'AGE', case
    when AGE >= 7 and AGE <= 17 then 'A'
    when AGE >= 18 and AGE <= 30 then 'B'
    when AGE >= 31 and AGE <= 45 then 'C'
    when AGE >= 46 and AGE <= 60 then 'D'
    when AGE >= 61 then 'E'
    else 'Z' end,
  'COUNTY_ID', case
    when COUNTY_ID == '西湖区' then 'A'
    when COUNTY_ID == '余杭区' then 'B'
    else 'Z' end,
  'DISTRICT', case
    when DISTRICT == '农村' then 'A'
    when DISTRICT == '城市' then 'B'
    when DISTRICT == '校园' then 'C'
    else 'Z' end,
  'GROUP_CODE', case
    when GROUP_CODE is null or trim(GROUP_CODE) == '' then 'B'
    else 'A' end,
  'CORP_NAME', case 
    when CORP_NAME is null or trim(CORP_NAME) == '' then 'Z'
    when upper(substr(trim(CORP_NAME), 0, 1)) == 'A' then 'A'
    when upper(substr(trim(CORP_NAME), 0, 1)) == 'B' then 'B'
    when upper(substr(trim(CORP_NAME), 0, 1)) == 'C' then 'C'
    else 'Z' end,
   'INNET_MONTHS', case
    when cast(INNET_MONTHS as int) <= 3 and cast(INNET_MONTHS as int) >= 0 then 'A'
    when cast(INNET_MONTHS as int) > 3 and cast(INNET_MONTHS as int) <= 6 then 'B'
    when cast(INNET_MONTHS as int) > 6 and cast(INNET_MONTHS as int) <= 12 then 'C'
    when cast(INNET_MONTHS as int) > 12 and cast(INNET_MONTHS as int) <= 36 then 'D'
    when cast(INNET_MONTHS as int) > 36 and cast(INNET_MONTHS as int) <= 60 then 'E'
    when cast(INNET_MONTHS as int) > 60 and cast(INNET_MONTHS as int) <= 120 then 'F'
    when cast(INNET_MONTHS as int) > 120 and cast(INNET_MONTHS as int) <= 240 then 'G'
    when cast(INNET_MONTHS as int) > 240 then 'H'
    else 'Z' end
    ) as month,
map('default', 'null') as day from ${widetable_month};