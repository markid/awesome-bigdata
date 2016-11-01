use fm;
dfs -mkdir -p ${location};
CREATE external TABLE if not exists ${table_name}(ID_NO string,IMSI_NO string,PHONE_NO string,CUST_STATUS string,OPEN_TIME string,
                            INNET_MONTHS string,COUNTY_ID string,GROUP_ID string,CHANNEL_NAME string,SEGMENT_TYPE string,
                            DONGTAI_DISTRICT string,DONGTAI_PIANQU string,DONGTAI_TOWN string,CELL_ID string,CELL_NAME string,
                            SM_CODE string,SM_NAME string,BILL_CODE string,PRODUCT_NAME string,CREDIT_LEVEL string,GPRSFUNC_FLAG string,
                            GROUP_CODE string,DYNAMIC_TYPE string,GROUPMANAGER_CODE string,GROUPMANAGER_NAME string,VPMN_CODE string,
                            SHORT_NO string,MONTH_NAME string,INGRP_DATE string,IMEI_NO string,MACHINE_BRAND string,MACHINE_TYPE string,
                            MACHINE_STANDARD string,OS_FLAG string,SMART_FLAG string,IMEI_CHG_YM string,GPRS_CODE string,GPRS_NAME string,
                            GPRS_NAME_END string,ADD_GPRS_NAME string,XIANSHI_GPRS_NAME string,LONG_FLAG string,ROAM_FLAG string,HJH_FLAG string,
                            HJH_MAIN_PHONE string,KD_FAMILY_NAME string,KD_MAIN_PHONE string,PYQ_FLAG string,MIGU string,LDTX string,SJB string,
                            SJYD string,SJYX string,TARIFF string,CS_SALE_NAME string,CS_END_TIME string,ZD_SALE_NAME string,ZD_END_TIME string,
                            LOW_FEE string,LOWFEE_END_TIME string,TOTAL_FEE string,ARPU3_FEE string,YUEZU_FEE string,LOCAL_FEE string,LONG_FEE string,
                            ROAM_FEE string,SHUJU_FEE string,SMS_FEE string,MMS_FEE string,V_BASIC_FEE string,GPRS_TAOCAN_FEE string,GPRS_FLOW_FEE string,
                            IS_ONLINE string,ONLINE_DAYS string,TIMES string,DURATIONS string,TD_DURATIONS string,V_DURATIONS string,HJH_DURATIONS string,
                            PYQ_DURATIONS string,CALLING_DURATIONS string,CALLED_DURATIONS string,LOCAL_DURATIONS string,LONG_DURATIONS string,ROAM_DURATIONS string,
                            SMS_CNT string,FLOW_WIFI string,FLOW string,FLOW3 string,FLOW4 string,ARPU3_FLOW string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = '"',
   "escapeChar"    = "\\"
)
STORED AS TEXTFILE
location '${location}'
tblproperties("skip.header.line.count"="1");
