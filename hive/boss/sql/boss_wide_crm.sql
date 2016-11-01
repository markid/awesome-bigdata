use fm;
dfs -mkdir -p ${location};
create external table if not exists ${table_name} (imsi_no string, pay_type string, hurry_mode string,sm_name string,credit_level string) 
row format delimited
  fields terminated by '\001'
  COLLECTION ITEMS TERMINATED BY '\002'
  MAP KEYS TERMINATED BY '\003'
 STORED AS TEXTFILE
 location '${location}';

insert overwrite table ${table_name}
select imsi_no,'','',
case when credit_level is not null then credit_level else '' end,
case when sm_name is not null then sm_name else '' end
from ${original} where imsi_no is not null and imsi_no != ''; 

