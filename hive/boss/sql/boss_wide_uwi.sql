use fm;
dfs -mkdir -p ${location};
create external table if not exists ${table_name} (imsi_no string, calling_number int, migu string,sjb string, sjyd string, sjyx string,sjsp string,g4_user_flag string, v_basic_fee string, is_online string) 
row format delimited
  fields terminated by '\001'
  COLLECTION ITEMS TERMINATED BY '\002'
  MAP KEYS TERMINATED BY '\003'
 STORED AS TEXTFILE
 location '${location}';

insert overwrite table ${table_name} 
select imsi_no, 0, case when migu is not null then migu else '' end, case when sjb is not null then sjb else '' end, case when sjyd is not null then sjyd else '' end, case when sjyx is not null then sjyx else '' end, '', '', '',case when is_online is not null then is_online else '' end 
from ${original} where imsi_no is not null and imsi_no != '';





	
