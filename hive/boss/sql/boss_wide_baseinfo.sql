use fm;
dfs -mkdir -p ${location};
create external table if not exists ${table_name}(imsi_no string, phone_no string, gender string, birthday string, age int, county_id string, district string, group_code string, corp_name string, innet_months string) 
 COMMENT 'This is a temperature table for base_info in boss monthly widetable'
 row format delimited
 fields terminated by '\001'
 COLLECTION ITEMS TERMINATED BY '\002'
 MAP KEYS TERMINATED BY '\003'
 STORED AS TEXTFILE
 location '${location}'; 
	
insert overwrite table ${table_name} select IMSI_NO, PHONE_NO, "", "", "", COUNTY_ID, SEGMENT_TYPE, GROUP_CODE, DYNAMIC_TYPE, INNET_MONTHS from ${original};
