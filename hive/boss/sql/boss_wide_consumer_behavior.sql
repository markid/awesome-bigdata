use fm;
dfs -mkdir -p ${location};
create external table if not exists ${table_name} (IMSI_NO string, CALL_BUNDLE_FLAG string, MACHINE_BUNDLE_FLAG string, COMMODITY_BUNDLE_FLAG string, 
                                                        COMMODITY_LOW string, LOWFEE_END_TIME string, NET_ACTIVE_FLAG string)
COMMENT 'This is a temperature table for Consumer behavior in boss monthly widetable'
row format delimited
  fields terminated by '\001'
  COLLECTION ITEMS TERMINATED BY '\002'
  MAP KEYS TERMINATED BY '\003'
 STORED AS TEXTFILE
 location '${location}';
 
insert overwrite table ${table_name}
 select imsi_no, 
 case when CS_SALE_NAME is not null and CS_SALE_NAME != '' then 1 else 0 end, 
 case when ZD_SALE_NAME is not null and ZD_SALE_NAME !='' then 1 else 0 end,
 case when ZD_SALE_NAME is not null and ZD_SALE_NAME !='' then 1 else 0 end,
 case when LOW_FEE is not null and LOW_FEE !='' then 1 else 0 end,
 LOWFEE_END_TIME ,'' from ${original} where imsi_no!='' and imsi_no is not null;

