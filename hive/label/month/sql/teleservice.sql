CREATE EXTERNAL TABLE IF NOT EXISTS hbase_hive_teleservice(key string, month map<string,string>, day map<string,string>) 
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key, month:, day:") 
TBLPROPERTIES ("hbase.table.name" = "flowmarketing:label");

insert overwrite table hbase_hive_teleservice
select IMSI_NO as key,
map(
  --'XIANSHI_GPRS_NAME', XIANSHI_GPRS_NAME,
  'ADD_GPRS_NAME', case
    when cast(regexp_extract(ADD_GPRS_NAME, ".*\\D(\\d+)元.*",1) as int) = 5 then 'A'
    when cast(regexp_extract(ADD_GPRS_NAME, ".*\\D(\\d+)元.*",1) as int) = 10 then 'B'
    when cast(regexp_extract(ADD_GPRS_NAME, ".*\\D(\\d+)元.*",1) as int) = 20 then 'C'
    when cast(regexp_extract(ADD_GPRS_NAME, ".*\\D(\\d+)元.*",1) as int) = 30 then 'D'
    when cast(regexp_extract(ADD_GPRS_NAME, ".*\\D(\\d+)元.*",1) as int) = 40 then 'E'
    when cast(regexp_extract(ADD_GPRS_NAME, ".*\\D(\\d+)元.*",1) as int) = 50 then 'F'
    when cast(regexp_extract(ADD_GPRS_NAME, ".*\\D(\\d+)元.*",1) as int) = 70 then 'G'
    when cast(regexp_extract(ADD_GPRS_NAME, ".*\\D(\\d+)元.*",1) as int) = 100 then 'H'
    when cast(regexp_extract(ADD_GPRS_NAME, ".*\\D(\\d+)元.*",1) as int) = 130 then 'I'
    when cast(regexp_extract(ADD_GPRS_NAME, ".*\\D(\\d+)元.*",1) as int) = 280 then 'J'
    else 'Z' end,
   'OVERFLOW', case
    when cast(OVERFLOW as int)>0 then 'A'
    else 'B' end,
   'IS_GPRS',case
    when cast(IS_GPRS as int)=0 then 'A'
    when cast(IS_GPRS as int)=1 then 'B'
    else 'Z' end,
   'NATIONAL_ROAM_FEE', case
    when NATIONAL_ROAM_FEE >=0 and NATIONAL_ROAM_FEE<=100 then 'A'
    when NATIONAL_ROAM_FEE >100 and NATIONAL_ROAM_FEE<=200 then 'B'
    when NATIONAL_ROAM_FEE >200 and NATIONAL_ROAM_FEE<=300 then 'C'
    when NATIONAL_ROAM_FEE >300 and NATIONAL_ROAM_FEE<=400 then 'D'
    when NATIONAL_ROAM_FEE >400 then 'E'
    else 'Z' end,
   'NATIONAL_ROAM_FEE1', case
    when NATIONAL_ROAM_FEE1 >=0 and NATIONAL_ROAM_FEE1<=100 then 'A'
    when NATIONAL_ROAM_FEE1 >100 and NATIONAL_ROAM_FEE1<=200 then 'B'
    when NATIONAL_ROAM_FEE1 >200 and NATIONAL_ROAM_FEE1<=300 then 'C'
    when NATIONAL_ROAM_FEE1 >300 and NATIONAL_ROAM_FEE1<=400 then 'D'
    when NATIONAL_ROAM_FEE1 >400 then 'E'
    else 'Z' end,
   'NATIONAL_ROAM_FEE2', case
    when NATIONAL_ROAM_FEE2 >=0 and NATIONAL_ROAM_FEE2<=100 then 'A'
    when NATIONAL_ROAM_FEE2 >100 and NATIONAL_ROAM_FEE2<=200 then 'B'
    when NATIONAL_ROAM_FEE2 >200 and NATIONAL_ROAM_FEE2<=300 then 'C'
    when NATIONAL_ROAM_FEE2 >300 and NATIONAL_ROAM_FEE2<=400 then 'D'
    when NATIONAL_ROAM_FEE2 >400 then 'E'
    else 'Z' end,
   'SMS_FEE',case
    when SMS_FEE >=0 and SMS_FEE<=10 then 'A'
    when SMS_FEE >10 and SMS_FEE<=20 then 'B'
    when SMS_FEE >20 and SMS_FEE<=30 then 'C'
    when SMS_FEE >30 and SMS_FEE<=40 then 'D'
    when SMS_FEE >40 then 'E'
    else 'Z' end,
   'MMS_FEE',case
    when MMS_FEE >=0 and MMS_FEE<=100 then 'A'
    when MMS_FEE >100 and MMS_FEE<=200 then 'B'
    when MMS_FEE >200 and MMS_FEE<=300 then 'C'
    when MMS_FEE >300 and MMS_FEE<=400 then 'D'
    when MMS_FEE >400 then 'E'
    else 'Z' end,
   'TARIFF_FEE',case
    when cast(regexp_extract(TARIFF_FEE, "[\u4e00-\u9fa5a-zA-Z]*(\\d+)元[\u4e00-\u9fa5a-zA-Z]*", 1) as int) >0 
        and cast(regexp_extract(TARIFF_FEE, "[\u4e00-\u9fa5a-zA-Z]*(\\d+)元[\u4e00-\u9fa5a-zA-Z]*", 1) as int) <=10 then 'A'
    when cast(regexp_extract(TARIFF_FEE, "[\u4e00-\u9fa5a-zA-Z]*(\\d+)元[\u4e00-\u9fa5a-zA-Z]*", 1) as int) >10 
        and cast(regexp_extract(TARIFF_FEE, "[\u4e00-\u9fa5a-zA-Z]*(\\d+)元[\u4e00-\u9fa5a-zA-Z]*", 1) as int)<=20 then 'B'
    when cast(regexp_extract(TARIFF_FEE, "[\u4e00-\u9fa5a-zA-Z]*(\\d+)元[\u4e00-\u9fa5a-zA-Z]*", 1) as int) >20 
        and cast(regexp_extract(TARIFF_FEE, "[\u4e00-\u9fa5a-zA-Z]*(\\d+)元[\u4e00-\u9fa5a-zA-Z]*", 1) as int)<=30 then 'C'
    when cast(regexp_extract(TARIFF_FEE, "[\u4e00-\u9fa5a-zA-Z]*(\\d+)元[\u4e00-\u9fa5a-zA-Z]*", 1) as int) >30 
        and cast(regexp_extract(TARIFF_FEE, "[\u4e00-\u9fa5a-zA-Z]*(\\d+)元[\u4e00-\u9fa5a-zA-Z]*", 1) as int)<=40 then 'D'
    when cast(regexp_extract(TARIFF_FEE, "[\u4e00-\u9fa5a-zA-Z]*(\\d+)元[\u4e00-\u9fa5a-zA-Z]*", 1) as int) >40 then 'E'
    else 'Z' end,
   'SHUJU_FEE',case
    when SHUJU_FEE >=0 and SHUJU_FEE<=20 then 'A'
    when SHUJU_FEE >20 and SHUJU_FEE<=50 then 'B'
    when SHUJU_FEE >50 and SHUJU_FEE<=100 then 'C'
    when SHUJU_FEE >100 then 'D'
    else 'Z' end,
   'LOCAL_FEE',case
    when LOCAL_FEE >=0 and LOCAL_FEE<=100 then 'A'
    when LOCAL_FEE >100 and LOCAL_FEE<=200 then 'B'
    when LOCAL_FEE >200 and LOCAL_FEE<=300 then 'C'
    when LOCAL_FEE >300 and LOCAL_FEE<=400 then 'D'
    when LOCAL_FEE >400 then 'E'
    else 'Z' end,
    'INTER_LONG_FEE',case
    when INTER_LONG_FEE >=0 and INTER_LONG_FEE<=100 then 'A'
    when INTER_LONG_FEE >100 and INTER_LONG_FEE<=200 then 'B'
    when INTER_LONG_FEE >200 and INTER_LONG_FEE<=300 then 'C'
    when INTER_LONG_FEE >300 and INTER_LONG_FEE<=400 then 'D'
    when INTER_LONG_FEE >400 then 'E'
    else 'Z' end,
    'SMS_PACKAGE' ,case
    when SMS_PACKAGE >=0 and SMS_PACKAGE<=10 then 'A'
    when SMS_PACKAGE >10 and SMS_PACKAGE<=20 then 'B'
    when SMS_PACKAGE >20 and SMS_PACKAGE<=30 then 'C'
    when SMS_PACKAGE >30 and SMS_PACKAGE<=40 then 'D'
    when SMS_PACKAGE >40 then 'E'
    else 'Z' end,
    'LDTX', case
    when LDTX is not NULL and LDTX != '' then 'A'
    else 'B' end,
    'LONG_FEE', case
    when LONG_FEE >=0 and LONG_FEE<=100 then 'A'
    when LONG_FEE >100 and LONG_FEE<=200 then 'B'
    when LONG_FEE >200 and LONG_FEE<=300 then 'C'
    when LONG_FEE >300 and LONG_FEE<=400 then 'D'
    when LONG_FEE >400 then 'E'
    else 'Z' end,
    'NATIONAL_LONG_FEE', case
    when NATIONAL_LONG_FEE >=0 and NATIONAL_LONG_FEE<=100 then 'A'
    when NATIONAL_LONG_FEE >100 and NATIONAL_LONG_FEE<=200 then 'B'
    when NATIONAL_LONG_FEE >200 and NATIONAL_LONG_FEE<=300 then 'C'
    when NATIONAL_LONG_FEE >300 and NATIONAL_LONG_FEE<=400 then 'D'
    when NATIONAL_LONG_FEE >400 then 'E'
    else 'Z' end,
    'NATIONAL_LONG_FEE1', case
    when NATIONAL_LONG_FEE1 >=0 and NATIONAL_LONG_FEE1<=100 then 'A'
    when NATIONAL_LONG_FEE1 >100 and NATIONAL_LONG_FEE1<=200 then 'B'
    when NATIONAL_LONG_FEE1 >200 and NATIONAL_LONG_FEE1<=300 then 'C'
    when NATIONAL_LONG_FEE1 >300 and NATIONAL_LONG_FEE1<=400 then 'D'
    when NATIONAL_LONG_FEE1 >400 then 'E'
    else 'Z' end,
    'FLOW2', case
    when FLOW2 >=0 and FLOW2<=100 then 'A'
    when FLOW2 >100 and FLOW2<=200 then 'B'
    when FLOW2 >200 and FLOW2<=300 then 'C'
    when FLOW2 >300 and FLOW2<=400 then 'D'
    when FLOW2 >400 and FLOW2<=500 then 'E'
    when FLOW2 >500 then 'F'
    else 'Z' end,
    'FLOW3', case
    when FLOW3 >=0 and FLOW3<=300 then 'A'
    when FLOW3 >300 and FLOW3<=500 then 'B'
    when FLOW3 >500 and FLOW3<=800 then 'C'
    when FLOW3 >800 and FLOW3<=1024 then 'D'
    when FLOW3 >1024 and FLOW3<=2048 then 'E'
    when FLOW3 >2048 then 'F'
    else 'Z' end,
    'FLOW4', case
    when FLOW4 >=0 and FLOW4<=500 then 'A'
    when FLOW4 >500 and FLOW4<=700 then 'B'
    when FLOW4 >700 and FLOW4<=1024 then 'C'
    when FLOW4 >1024 and FLOW4<=2048 then 'D'
    when FLOW4 >2048 and FLOW4<=3072 then 'E'
    when FLOW4 >3072 and FLOW4<=4096 then 'F'
    when FLOW4 >4096 and FLOW4<=6144 then 'G'
    when FLOW4 >6144 and FLOW4<=11264 then 'H'
    when FLOW4 >11264 then 'I'
    else 'Z' end,
    'FLOW', case
    when FLOW >=0 and FLOW<=500 then 'A'
    when FLOW >500 and FLOW<=700 then 'B'
    when FLOW >700 and FLOW<=1024 then 'C'
    when FLOW >1024 and FLOW<=2048 then 'D'
    when FLOW >2048 and FLOW<=3072 then 'E'
    when FLOW >3072 and FLOW<=4096 then 'F'
    when FLOW >4096 and FLOW<=6144 then 'G'
    when FLOW >6144 and FLOW<=11264 then 'H'
    when FLOW >11264 then 'I'
    else 'Z' end,
    'FLOW_WIFI', case
    when FLOW_WIFI >=0 and FLOW_WIFI<=500 then 'A'
    when FLOW_WIFI >500 and FLOW_WIFI<=700 then 'B'
    when FLOW_WIFI >700 and FLOW_WIFI<=1024 then 'C'
    when FLOW_WIFI >1024 and FLOW_WIFI<=2048 then 'D'
    when FLOW_WIFI >2048 and FLOW_WIFI<=3072 then 'E'
    when FLOW_WIFI >3072 and FLOW_WIFI<=4096 then 'F'
    when FLOW_WIFI >4096 and FLOW_WIFI<=6144 then 'G'
    when FLOW_WIFI >6144 and FLOW_WIFI<=11264 then 'H'
    when FLOW_WIFI >11264 then 'I'
    else 'Z' end,
    'IS_SMS_PACKAGE', case
    when IS_SMS_PACKAGE is not null and IS_SMS_PACKAGE!='' then 'A'
    else 'B' end,
    'ARPU3_FEE', case 
    when ARPU3_FEE >=0 and ARPU3_FEE<=10 then 'A'
    when ARPU3_FEE >10 and ARPU3_FEE<=20 then 'B'
    when ARPU3_FEE >20 and ARPU3_FEE<=30 then 'C'
    when ARPU3_FEE >30 and ARPU3_FEE<=50 then 'D'
    when ARPU3_FEE >50 and ARPU3_FEE<=80 then 'E'
    when ARPU3_FEE >80 and ARPU3_FEE<=100 then 'F'
    when ARPU3_FEE >100 and ARPU3_FEE<=150 then 'G'
    when ARPU3_FEE >150 and ARPU3_FEE<=200 then 'H'
    when ARPU3_FEE >200 then 'I'
    else 'Z' end,
    'ARPU3_CALLING_DURATIONS', case
    when ARPU3_CALLING_DURATIONS >=0 and ARPU3_CALLING_DURATIONS<=20 then 'A'
    when ARPU3_CALLING_DURATIONS >20 and ARPU3_CALLING_DURATIONS<=50 then 'B'
    when ARPU3_CALLING_DURATIONS >50 and ARPU3_CALLING_DURATIONS<=80 then 'C'
    when ARPU3_CALLING_DURATIONS >80 and ARPU3_CALLING_DURATIONS<=100 then 'D'
    when ARPU3_CALLING_DURATIONS >100 and ARPU3_CALLING_DURATIONS<=120 then 'E'
    when ARPU3_CALLING_DURATIONS >120 and ARPU3_CALLING_DURATIONS<=140 then 'F'
    when ARPU3_CALLING_DURATIONS >140 and ARPU3_CALLING_DURATIONS<=160 then 'G'
    when ARPU3_CALLING_DURATIONS >160 and ARPU3_CALLING_DURATIONS<=180 then 'H'
    when ARPU3_CALLING_DURATIONS >180 and ARPU3_CALLING_DURATIONS<=200 then 'I'
    when ARPU3_CALLING_DURATIONS >200 then 'J'
    else 'Z' end,
    'ARPU3_LOCAL_DURATIONS', case
    when ARPU3_LOCAL_DURATIONS >=0 and ARPU3_LOCAL_DURATIONS<=20 then 'A'
    when ARPU3_LOCAL_DURATIONS >20 and ARPU3_LOCAL_DURATIONS<=50 then 'B'
    when ARPU3_LOCAL_DURATIONS >50 and ARPU3_LOCAL_DURATIONS<=80 then 'C'
    when ARPU3_LOCAL_DURATIONS >80 and ARPU3_LOCAL_DURATIONS<=100 then 'D'
    when ARPU3_LOCAL_DURATIONS >100 and ARPU3_LOCAL_DURATIONS<=120 then 'E'
    when ARPU3_LOCAL_DURATIONS >120 and ARPU3_LOCAL_DURATIONS<=140 then 'F'
    when ARPU3_LOCAL_DURATIONS >140 and ARPU3_LOCAL_DURATIONS<=160 then 'G'
    when ARPU3_LOCAL_DURATIONS >160 and ARPU3_LOCAL_DURATIONS<=180 then 'H'
    when ARPU3_LOCAL_DURATIONS >180 and ARPU3_LOCAL_DURATIONS<=200 then 'I'
    when ARPU3_LOCAL_DURATIONS >200 then 'J'
    else 'Z' end,
    'ARPU3_ROAM_DURATIONS', case
    when ARPU3_ROAM_DURATIONS >=0 and ARPU3_ROAM_DURATIONS<=20 then 'A'
    when ARPU3_ROAM_DURATIONS >20 and ARPU3_ROAM_DURATIONS<=50 then 'B'
    when ARPU3_ROAM_DURATIONS >50 and ARPU3_ROAM_DURATIONS<=80 then 'C'
    when ARPU3_ROAM_DURATIONS >80 and ARPU3_ROAM_DURATIONS<=100 then 'D'
    when ARPU3_ROAM_DURATIONS >100 and ARPU3_ROAM_DURATIONS<=120 then 'E'
    when ARPU3_ROAM_DURATIONS >120 and ARPU3_ROAM_DURATIONS<=140 then 'F'
    when ARPU3_ROAM_DURATIONS >140 and ARPU3_ROAM_DURATIONS<=160 then 'G'
    when ARPU3_ROAM_DURATIONS >160 and ARPU3_ROAM_DURATIONS<=180 then 'H'
    when ARPU3_ROAM_DURATIONS >180 and ARPU3_ROAM_DURATIONS<=200 then 'I'
    when ARPU3_ROAM_DURATIONS >200 then 'J'
    else 'Z' end,
    'ARPU3_LONG_DURATIONS', case
    when ARPU3_LONG_DURATIONS >=0 and ARPU3_LONG_DURATIONS<=20 then 'A'
    when ARPU3_LONG_DURATIONS >20 and ARPU3_LONG_DURATIONS<=50 then 'B'
    when ARPU3_LONG_DURATIONS >50 and ARPU3_LONG_DURATIONS<=80 then 'C'
    when ARPU3_LONG_DURATIONS >80 and ARPU3_LONG_DURATIONS<=100 then 'D'
    when ARPU3_LONG_DURATIONS >100 and ARPU3_LONG_DURATIONS<=120 then 'E'
    when ARPU3_LONG_DURATIONS >120 and ARPU3_LONG_DURATIONS<=140 then 'F'
    when ARPU3_LONG_DURATIONS >140 and ARPU3_LONG_DURATIONS<=160 then 'G'
    when ARPU3_LONG_DURATIONS >160 and ARPU3_LONG_DURATIONS<=180 then 'H'
    when ARPU3_LONG_DURATIONS >180 and ARPU3_LONG_DURATIONS<=200 then 'I'
    when ARPU3_LONG_DURATIONS >200 then 'J'
    else 'Z' end,
    'ARPU3_SMS_CNT' ,case
    when ARPU3_SMS_CNT >=0 and ARPU3_SMS_CNT<=10 then 'A'
    when ARPU3_SMS_CNT >10 and ARPU3_SMS_CNT<=20 then 'B'
    when ARPU3_SMS_CNT >20 and ARPU3_SMS_CNT<=30 then 'C'
    when ARPU3_SMS_CNT >30 and ARPU3_SMS_CNT<=40 then 'D'
    when ARPU3_SMS_CNT >40 then 'E'
    else 'Z' end,
    'ARPU3_MMS_CNT' ,case
    when ARPU3_MMS_CNT >=0 and ARPU3_MMS_CNT<=10 then 'A'
    when ARPU3_MMS_CNT >10 and ARPU3_MMS_CNT<=20 then 'B'
    when ARPU3_MMS_CNT >20 and ARPU3_MMS_CNT<=30 then 'C'
    when ARPU3_MMS_CNT >30 and ARPU3_MMS_CNT<=40 then 'D'
    when ARPU3_MMS_CNT >40 then 'E'
    else 'Z' end,
    'ARPU3_FLOW_FEE', case
    when ARPU3_FLOW_FEE >=0 and ARPU3_FLOW_FEE<=10 then 'A'
    when ARPU3_FLOW_FEE >10 and ARPU3_FLOW_FEE<=20 then 'B'
    when ARPU3_FLOW_FEE >20 and ARPU3_FLOW_FEE<=30 then 'C'
    when ARPU3_FLOW_FEE >30 and ARPU3_FLOW_FEE<=50 then 'D'
    when ARPU3_FLOW_FEE >50 and ARPU3_FLOW_FEE<=80 then 'E'
    when ARPU3_FLOW_FEE >80 and ARPU3_FLOW_FEE<=100 then 'F'
    when ARPU3_FLOW_FEE >100 and ARPU3_FLOW_FEE<=150 then 'G'
    when ARPU3_FLOW_FEE >150 and ARPU3_FLOW_FEE<=200 then 'H'
    when ARPU3_FLOW_FEE >200 then 'I'
    else 'Z' end,
    'ARPU3_LOCAL_FEE',case
    when ARPU3_LOCAL_FEE >=0 and ARPU3_LOCAL_FEE<=10 then 'A'
    when ARPU3_LOCAL_FEE >10 and ARPU3_LOCAL_FEE<=20 then 'B'
    when ARPU3_LOCAL_FEE >20 and ARPU3_LOCAL_FEE<=30 then 'C'
    when ARPU3_LOCAL_FEE >30 and ARPU3_LOCAL_FEE<=50 then 'D'
    when ARPU3_LOCAL_FEE >50 and ARPU3_LOCAL_FEE<=80 then 'E'
    when ARPU3_LOCAL_FEE >80 and ARPU3_LOCAL_FEE<=100 then 'F'
    when ARPU3_LOCAL_FEE >100 and ARPU3_LOCAL_FEE<=150 then 'G'
    when ARPU3_LOCAL_FEE >150 and ARPU3_LOCAL_FEE<=200 then 'H'
    when ARPU3_LOCAL_FEE >200 then 'I'
    else 'Z' end,
    'APPU3_ROAM_FEE', case
    when APPU3_ROAM_FEE >=0 and APPU3_ROAM_FEE<=10 then 'A'
    when APPU3_ROAM_FEE >10 and APPU3_ROAM_FEE<=20 then 'B'
    when APPU3_ROAM_FEE >20 and APPU3_ROAM_FEE<=30 then 'C'
    when APPU3_ROAM_FEE >30 and APPU3_ROAM_FEE<=50 then 'D'
    when APPU3_ROAM_FEE >50 and APPU3_ROAM_FEE<=80 then 'E'
    when APPU3_ROAM_FEE >80 and APPU3_ROAM_FEE<=100 then 'F'
    when APPU3_ROAM_FEE >100 and APPU3_ROAM_FEE<=150 then 'G'
    when APPU3_ROAM_FEE >150 and APPU3_ROAM_FEE<=200 then 'H'
    when APPU3_ROAM_FEE >200 then 'I'
    else 'Z' end,
    'ARPU3_LONG_FEE', case
    when ARPU3_LONG_FEE >=0 and ARPU3_LONG_FEE<=10 then 'A'
    when ARPU3_LONG_FEE >10 and ARPU3_LONG_FEE<=20 then 'B'
    when ARPU3_LONG_FEE >20 and ARPU3_LONG_FEE<=30 then 'C'
    when ARPU3_LONG_FEE >30 and ARPU3_LONG_FEE<=50 then 'D'
    when ARPU3_LONG_FEE >50 and ARPU3_LONG_FEE<=80 then 'E'
    when ARPU3_LONG_FEE >80 and ARPU3_LONG_FEE<=100 then 'F'
    when ARPU3_LONG_FEE >100 and ARPU3_LONG_FEE<=150 then 'G'
    when ARPU3_LONG_FEE >150 and ARPU3_LONG_FEE<=200 then 'H'
    when ARPU3_LONG_FEE >200 then 'I'
    else 'Z' end,
    'ARPU3_SMS_FEE',case
    when ARPU3_SMS_FEE >=0 and ARPU3_SMS_FEE<=10 then 'A'
    when ARPU3_SMS_FEE >10 and ARPU3_SMS_FEE<=20 then 'B'
    when ARPU3_SMS_FEE >20 and ARPU3_SMS_FEE<=30 then 'C'
    when ARPU3_SMS_FEE >30 and ARPU3_SMS_FEE<=40 then 'D'
    when ARPU3_SMS_FEE >40 then 'E'
    else 'Z' end,
    'ARPU3_MMS_FEE',case
    when ARPU3_MMS_FEE >=0 and ARPU3_MMS_FEE<=10 then 'A'
    when ARPU3_MMS_FEE >10 and ARPU3_MMS_FEE<=20 then 'B'
    when ARPU3_MMS_FEE >20 and ARPU3_MMS_FEE<=30 then 'C'
    when ARPU3_MMS_FEE >30 and ARPU3_MMS_FEE<=40 then 'D'
    when ARPU3_MMS_FEE >40 then 'E'
    else 'Z' end,
    'ARPU3_INTER_LONG_DURATIONS',case
    when ARPU3_INTER_LONG_DURATIONS >=0 and ARPU3_INTER_LONG_DURATIONS<=20 then 'A'
    when ARPU3_INTER_LONG_DURATIONS >20 and ARPU3_INTER_LONG_DURATIONS<=50 then 'B'
    when ARPU3_INTER_LONG_DURATIONS >50 and ARPU3_INTER_LONG_DURATIONS<=80 then 'C'
    when ARPU3_INTER_LONG_DURATIONS >80 and ARPU3_INTER_LONG_DURATIONS<=100 then 'D'
    when ARPU3_INTER_LONG_DURATIONS >100 and ARPU3_INTER_LONG_DURATIONS<=120 then 'E'
    when ARPU3_INTER_LONG_DURATIONS >120 and ARPU3_INTER_LONG_DURATIONS<=140 then 'F'
    when ARPU3_INTER_LONG_DURATIONS >140 and ARPU3_INTER_LONG_DURATIONS<=160 then 'G'
    when ARPU3_INTER_LONG_DURATIONS >160 and ARPU3_INTER_LONG_DURATIONS<=180 then 'H'
    when ARPU3_INTER_LONG_DURATIONS >180 and ARPU3_INTER_LONG_DURATIONS<=200 then 'I'
    when ARPU3_INTER_LONG_DURATIONS >200 then 'J'
    else 'Z' end,
    'ARPU3_INTER_ROAM_DURATIONS', case
    when ARPU3_INTER_ROAM_DURATIONS >=0 and ARPU3_INTER_ROAM_DURATIONS<=20 then 'A'
    when ARPU3_INTER_ROAM_DURATIONS >20 and ARPU3_INTER_ROAM_DURATIONS<=50 then 'B'
    when ARPU3_INTER_ROAM_DURATIONS >50 and ARPU3_INTER_ROAM_DURATIONS<=80 then 'C'
    when ARPU3_INTER_ROAM_DURATIONS >80 and ARPU3_INTER_ROAM_DURATIONS<=100 then 'D'
    when ARPU3_INTER_ROAM_DURATIONS >100 and ARPU3_INTER_ROAM_DURATIONS<=120 then 'E'
    when ARPU3_INTER_ROAM_DURATIONS >120 and ARPU3_INTER_ROAM_DURATIONS<=140 then 'F'
    when ARPU3_INTER_ROAM_DURATIONS >140 and ARPU3_INTER_ROAM_DURATIONS<=160 then 'G'
    when ARPU3_INTER_ROAM_DURATIONS >160 and ARPU3_INTER_ROAM_DURATIONS<=180 then 'H'
    when ARPU3_INTER_ROAM_DURATIONS >180 and ARPU3_INTER_ROAM_DURATIONS<=200 then 'I'
    when ARPU3_INTER_ROAM_DURATIONS >200 then 'J'
    else 'Z' end,
    'ARPU3_INTER_LONG_FEE',case
    when ARPU3_INTER_LONG_FEE >=0 and ARPU3_INTER_LONG_FEE<=10 then 'A'
    when ARPU3_INTER_LONG_FEE >10 and ARPU3_INTER_LONG_FEE<=20 then 'B'
    when ARPU3_INTER_LONG_FEE >20 and ARPU3_INTER_LONG_FEE<=30 then 'C'
    when ARPU3_INTER_LONG_FEE >30 and ARPU3_INTER_LONG_FEE<=50 then 'D'
    when ARPU3_INTER_LONG_FEE >50 and ARPU3_INTER_LONG_FEE<=80 then 'E'
    when ARPU3_INTER_LONG_FEE >80 and ARPU3_INTER_LONG_FEE<=100 then 'F'
    when ARPU3_INTER_LONG_FEE >100 and ARPU3_INTER_LONG_FEE<=150 then 'G'
    when ARPU3_INTER_LONG_FEE >150 and ARPU3_INTER_LONG_FEE<=200 then 'H'
    when ARPU3_INTER_LONG_FEE >200 then 'I'
    else 'Z' end,
    'ARPU3_INTER_ROAM_FEE',case
    when ARPU3_INTER_ROAM_FEE >=0 and ARPU3_INTER_ROAM_FEE<=10 then 'A'
    when ARPU3_INTER_ROAM_FEE >10 and ARPU3_INTER_ROAM_FEE<=20 then 'B'
    when ARPU3_INTER_ROAM_FEE >20 and ARPU3_INTER_ROAM_FEE<=30 then 'C'
    when ARPU3_INTER_ROAM_FEE >30 and ARPU3_INTER_ROAM_FEE<=50 then 'D'
    when ARPU3_INTER_ROAM_FEE >50 and ARPU3_INTER_ROAM_FEE<=80 then 'E'
    when ARPU3_INTER_ROAM_FEE >80 and ARPU3_INTER_ROAM_FEE<=100 then 'F'
    when ARPU3_INTER_ROAM_FEE >100 and ARPU3_INTER_ROAM_FEE<=150 then 'G'
    when ARPU3_INTER_ROAM_FEE >150 and ARPU3_INTER_ROAM_FEE<=200 then 'H'
    when ARPU3_INTER_ROAM_FEE >200 then 'I'
    else 'Z' end,
    'ONLINE_DAYS',case
    when ARPU3_MMS_FEE >=0 and ARPU3_MMS_FEE<=7 then 'A'
    when ARPU3_MMS_FEE >7 and ARPU3_MMS_FEE<=14 then 'B'
    when ARPU3_MMS_FEE >14 and ARPU3_MMS_FEE<=21 then 'C'
    when ARPU3_MMS_FEE >21 and ARPU3_MMS_FEE<=28 then 'D'
    when ARPU3_MMS_FEE >28 and ARPU3_MMS_FEE<=31 then 'E'
    else 'Z' end,
    'LAST_MOU_LEVEL', case
    when LAST_MOU_LEVEL >=0 and LAST_MOU_LEVEL<=50 then 'A'
    when LAST_MOU_LEVEL >50 and LAST_MOU_LEVEL<=80 then 'B'
    when LAST_MOU_LEVEL >80 and LAST_MOU_LEVEL<=100 then 'C'
    when LAST_MOU_LEVEL >100 and LAST_MOU_LEVEL<=150 then 'D'
    when LAST_MOU_LEVEL >150 and LAST_MOU_LEVEL<=200 then 'E'
    when LAST_MOU_LEVEL >200 and LAST_MOU_LEVEL<=250 then 'F'
    when LAST_MOU_LEVEL >250 and LAST_MOU_LEVEL<=300 then 'G'
    when LAST_MOU_LEVEL >300 and LAST_MOU_LEVEL<=400 then 'H'
    when LAST_MOU_LEVEL >400 and LAST_MOU_LEVEL<=500 then 'I'
    when LAST_MOU_LEVEL >500 then 'J'
    else 'Z' end,
    'LAST_GPRS_FLOW', case
    when LAST_GPRS_FLOW >=0 and LAST_GPRS_FLOW<=500 then 'A'
    when LAST_GPRS_FLOW >500 and LAST_GPRS_FLOW<=700 then 'B'
    when LAST_GPRS_FLOW >700 and LAST_GPRS_FLOW<=1024 then 'C'
    when LAST_GPRS_FLOW >1024 and LAST_GPRS_FLOW<=2048 then 'D'
    when LAST_GPRS_FLOW >2048 and LAST_GPRS_FLOW<=3072 then 'E'
    when LAST_GPRS_FLOW >3072 and LAST_GPRS_FLOW<=4096 then 'F'
    when LAST_GPRS_FLOW >4096 and LAST_GPRS_FLOW<=6144 then 'G'
    when LAST_GPRS_FLOW >6144 and LAST_GPRS_FLOW<=11264 then 'H'
    when LAST_GPRS_FLOW >11264 then 'I'
    else 'Z' end,
    'LAST_FLOW_EXC_CONTENT', case
    when LAST_FLOW_EXC_CONTENT >=0 and LAST_FLOW_EXC_CONTENT<=300 then 'A'
    when LAST_FLOW_EXC_CONTENT >300 and LAST_FLOW_EXC_CONTENT<=500 then 'B'
    when LAST_FLOW_EXC_CONTENT >500 and LAST_FLOW_EXC_CONTENT<=800 then 'C'
    when LAST_FLOW_EXC_CONTENT >800 and LAST_FLOW_EXC_CONTENT<=1024 then 'D'
    when LAST_FLOW_EXC_CONTENT >1024 and LAST_FLOW_EXC_CONTENT<=2048 then 'E'
    when LAST_FLOW_EXC_CONTENT >2048 then 'F'
    else 'Z' end,
    'SMS_CNT', case
    when SMS_CNT >=0 and SMS_CNT<=10 then 'A'
    when SMS_CNT >10 and SMS_CNT<=20 then 'B'
    when SMS_CNT >20 and SMS_CNT<=30 then 'C'
    when SMS_CNT >30 and SMS_CNT<=40 then 'D'
    when SMS_CNT >40 then 'E'
    else 'Z' end,
    'MMS_CNT', case
    when SMS_CNT >=0 and SMS_CNT<=10 then 'A'
    when SMS_CNT >10 and SMS_CNT<=20 then 'B'
    when SMS_CNT >20 and SMS_CNT<=30 then 'C'
    when SMS_CNT >30 and SMS_CNT<=40 then 'D'
    when SMS_CNT >40 then 'E'
    else 'Z' end,
    'SHORT_NO', case
    when SHORT_NO = '是' then 'A'
    when SHORT_NO = '否' then 'B'
    else 'Z' end,
    'PRODUCT_NAME', case
    when cast(regexp_extract(PRODUCT_NAME, ".*\\D(\\d+)元.*",1) as int) = 58 then 'A'
    when cast(regexp_extract(PRODUCT_NAME, ".*\\D(\\d+)元.*",1) as int) = 88 then 'B'
    when cast(regexp_extract(PRODUCT_NAME, ".*\\D(\\d+)元.*",1) as int) = 128 then 'C'
    when cast(regexp_extract(PRODUCT_NAME, ".*\\D(\\d+)元.*",1) as int) = 158 then 'D'
    when cast(regexp_extract(PRODUCT_NAME, ".*\\D(\\d+)元.*",1) as int) = 188 then 'E'
    when cast(regexp_extract(PRODUCT_NAME, ".*\\D(\\d+)元.*",1) as int) = 288 then 'F'
    when cast(regexp_extract(PRODUCT_NAME, ".*\\D(\\d+)元.*",1) as int) = 388 then 'G'
    when cast(regexp_extract(PRODUCT_NAME, ".*\\D(\\d+)元.*",1) as int) = 588 then 'H'
    when cast(regexp_extract(PRODUCT_NAME, ".*\\D(\\d+)元.*",1) as int) = 888 then 'I'
    else 'Z' end,
    'BILL_CODE',case
    when BILL_CODE = 'ACAZ22253' or BILL_CODE = 'ACAZ22254' or BILL_CODE = 'ACAZ22255' or BILL_CODE = 'ACAZ21855' or BILL_CODE = 'ACAZ21856'or BILL_CODE = 'ACAZ21857'or BILL_CODE = 'ACAZ21858'
    or BILL_CODE = 'ACAZ21859'or BILL_CODE = 'ACAZ21860'then 'A'
    when BILL_CODE = 'ACAZ19908' or BILL_CODE = 'ACAZ19912'or BILL_CODE = 'ACAZ19913'or BILL_CODE = 'ACAZ19914'or BILL_CODE = 'ACAZ19915'or BILL_CODE = 'ACAZ19921'or BILL_CODE = 'ACAZ19925'or BILL_CODE = 'ACAZ19926'
    or BILL_CODE = 'ACAZ19927'or BILL_CODE = 'ACAZ19929'or BILL_CODE = 'ACAZ19931'or BILL_CODE = 'ACAZ19933'or BILL_CODE = 'ACAZ20126'or BILL_CODE = 'ACAZ20141'or BILL_CODE = 'ACAZ20147'or BILL_CODE = 'ACAZ22306'
    or BILL_CODE = 'ACAZ22307'or BILL_CODE = 'ACAZ22308'or BILL_CODE = 'ACAZ22309'or BILL_CODE = 'ACAZ22310'or BILL_CODE = 'ACAZ22311'or BILL_CODE = 'ACAZ22312'or BILL_CODE = 'ACAZ22313'or BILL_CODE = 'ACAZ24222'
    or BILL_CODE = 'ACAZ24223'or BILL_CODE = 'ACAZ25128'or BILL_CODE = 'ACAZ21610'or BILL_CODE = 'ACAZ21617'or BILL_CODE = 'ACAZ24221'then 'B'
    when BILL_CODE = 'ACAZ22711'or BILL_CODE = 'ACAZ22712'or BILL_CODE = 'ACAZ22713'or BILL_CODE = 'ACAZ22714'then 'C'
    when BILL_CODE = 'ACAZ00301' or BILL_CODE = 'ACAZ00302' or BILL_CODE = 'ACAZ00303'or BILL_CODE = 'ACAZ11861'or BILL_CODE = 'ACAZ11943'or BILL_CODE = 'ACAZ15378'then 'D'
    when BILL_CODE = 'ACAZ11272' or BILL_CODE = 'ACAZ10867' or BILL_CODE = 'ACAZ11290' or BILL_CODE = 'ACAZ11299'then 'E'
    else 'Z' end,
      
    --'FLOW_PROP',case
    
    --'FLOW_PROP1',case
  
    ) as month,
map('default', 'null') as day
from fm.widetable_201606;