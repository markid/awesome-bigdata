use fm;
dfs -mkdir -p ${location};
create external table if not exists ${table_name} (IMSI_NO string, OPEN_TIME string, IS_GROUP_UNITS string, 
                                                        IS_VPMN_UNITS string, DYNAMIC_TYPE string, IS_VIP string)
COMMENT 'This is a temperature table for client identity in boss monthly widetable'
row format delimited
  fields terminated by '\001'
  COLLECTION ITEMS TERMINATED BY '\002'
  MAP KEYS TERMINATED BY '\003'
 STORED AS TEXTFILE
 location '${location}';
 
insert overwrite table ${table_name}
 select imsi_no, open_time, group_code, vpmn_code, '', '' from ${original} where imsi_no!='' and imsi_no is not null;
