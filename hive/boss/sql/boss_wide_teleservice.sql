use fm;
dfs -mkdir -p ${location};
create external table if not exists ${table_name} (IMSI_NO string, XIANSHI_GPRS_NAME string,ADD_GPRS_NAME string,OVERFLOW string,IS_GPRS string,
                                                NATIONAL_ROAM_FEE double,NATIONAL_ROAM_FEE1 double,NATIONAL_ROAM_FEE2 double,
                                                SMS_FEE double,MMS_FEE double,TARIFF_FEE string,SHUJU_FEE double,LOCAL_FEE double,
                                                INTER_LONG_FEE double,SMS_PACKAGE double,LDTX string,LONG_FEE double,
                                                NATIONAL_LONG_FEE double,NATIONAL_LONG_FEE1 double,FLOW2 double,FLOW3 double,FLOW4 double,FLOW double,FLOW_WIFI double,
                                                IS_SMS_PACKAGE string,ARPU3_FEE double,ARPU3_CALLING_DURATIONS int,ARPU3_LOCAL_DURATIONS int,ARPU3_ROAM_DURATIONS int,
                                                ARPU3_LONG_DURATIONS int,ARPU3_SMS_CNT int,ARPU3_MMS_CNT int,ARPU3_FLOW_FEE double ,ARPU3_LOCAL_FEE double ,APPU3_ROAM_FEE double ,
                                                ARPU3_LONG_FEE double ,ARPU3_SMS_FEE double ,ARPU3_MMS_FEE double ,ARPU3_INTER_LONG_DURATIONS int,ARPU3_INTER_ROAM_DURATIONS int,
                                                ARPU3_INTER_LONG_FEE double,ARPU3_INTER_ROAM_FEE double,ONLINE_DAYS int,LAST_MOU_LEVEL int,LAST_GPRS_FLOW double,
                                                LAST_FLOW_EXC_CONTENT double,SMS_CNT int,MMS_CNT int,SHORT_NO string,PRODUCT_NAME string,BILL_CODE string,
                                                FLOW_PROP double,FLOW_PROP1 double) 
    COMMENT 'This is a temperature table for teleservice in boss monthly widetable'
    row format delimited
    fields terminated by '\001'
    COLLECTION ITEMS TERMINATED BY '\002'
    MAP KEYS TERMINATED BY '\003'
    STORED AS TEXTFILE
    location '${location}'; 
    
insert overwrite table ${table_name} select t1.IMSI_NO IMSI_NO, t1.XIANSHI_GPRS_NAME XIANSHI_GPRS_NAME, t1.ADD_GPRS_NAME ADD_GPRS_NAME,t1.GPRS_FLOW_FEE OVERFLOW,
    case 
    when t1.GPRS_CODE is not null and t1.GPRS_CODE!='' and t1.ADD_GPRS_NAME is not null and t1.ADD_GPRS_NAME != '' then 0
    else 1
    end IS_GPRS,
    cast(t1.ROAM_FEE as double) NATIONAL_ROAM_FEE,"" NATIONAL_ROAM_FEE1,"" NATIONAL_ROAM_FEE2,cast(t1.SMS_FEE as double) SMS_FEE,cast(t1.MMS_FEE as double) MMS_FEE,
    t1.TARIFF TARIFF_FEE,cast(t1.SHUJU_FEE as double) SHUJU_FEE,cast(t1.LOCAL_FEE as double) LOCAL_FEE,"" INTER_LONG_FEE,"" SMS_PACKAGE,t1.LDTX LDTX,
    cast(t1.LONG_FEE as double) LONG_FEE,"" NATIONAL_LONG_FEE,"" NATIONAL_LONG_FEE1,
    cast(t1.FLOW as double)-cast(t1.FLOW3 as double)-cast(t1.FLOW4 as double) FLOW2,
    cast(t1.FLOW3 as double) FLOW3,cast(t1.FLOW4 as double) FLOW4,cast(t1.FLOW as double) FLOW,cast(t1.FLOW_WIFI as double) FLOW_WIFI,"" IS_SMS_PACKAGE,cast(t1.ARPU3_FEE as double) ARPU3_FEE,
    case
    when t2.CALLING_DURATIONS is null or t2.CALLING_DURATIONS=''or t3.CALLING_DURATIONS is null or t3.CALLING_DURATIONS='' then ""
    else (cast(t3.CALLING_DURATIONS as int)+cast(t2.CALLING_DURATIONS as int)+cast(t1.CALLING_DURATIONS as int))/3
    end ARPU3_CALLING_DURATIONS,
    case
    when t2.LOCAL_DURATIONS is null or t2.LOCAL_DURATIONS=''or t3.LOCAL_DURATIONS is null or t3.LOCAL_DURATIONS='' then ""
    else (cast(t3.LOCAL_DURATIONS as int)+cast(t2.LOCAL_DURATIONS as int)+cast(t1.LOCAL_DURATIONS as int))/3 
    end ARPU3_LOCAL_DURATIONS,
    case
    when t2.ROAM_DURATIONS is null or t2.ROAM_DURATIONS=''or t3.ROAM_DURATIONS is null or t3.ROAM_DURATIONS='' then ""
    else (cast(t3.ROAM_DURATIONS as int)+cast(t2.ROAM_DURATIONS as int)+cast(t1.ROAM_DURATIONS as int))/3 
    end ARPU3_ROAM_DURATIONS,
    case
    when t2.LONG_DURATIONS is null or t2.LONG_DURATIONS=''or t3.LONG_DURATIONS is null or t3.LONG_DURATIONS='' then ""
    else (cast(t3.LONG_DURATIONS as int)+cast(t2.LONG_DURATIONS as int)+cast(t1.LONG_DURATIONS as int))/3 
    end ARPU3_LONG_DURATIONS,
    case
    when t2.SMS_CNT is null or t2.SMS_CNT=''or t3.SMS_CNT is null or t3.SMS_CNT='' then ""
    else (cast(t3.SMS_CNT as int)+cast(t2.SMS_CNT as int)+cast(t1.SMS_CNT as int))/3 
    end ARPU3_SMS_CNT,
    "" ARPU3_MMS_CNT,
    case
    when t2.SHUJU_FEE is null or t2.SHUJU_FEE=''or t3.SHUJU_FEE is null or t3.SHUJU_FEE='' then ""
    else (cast(t3.SHUJU_FEE as double)+cast(t2.SHUJU_FEE as double)+cast(t1.SHUJU_FEE as double))/3 
    end ARPU3_FLOW_FEE,
    case
    when t2.LOCAL_FEE is null or t2.LOCAL_FEE=''or t3.LOCAL_FEE is null or t3.LOCAL_FEE='' then ""
    else (cast(t3.LOCAL_FEE as double)+cast(t2.LOCAL_FEE as double)+cast(t1.LOCAL_FEE as double))/3 
    end ARPU3_LOCAL_FEE,
    case
    when t2.ROAM_FEE is null or t2.ROAM_FEE=''or t3.ROAM_FEE is null or t3.ROAM_FEE='' then ""
    else (cast(t3.ROAM_FEE as double)+cast(t2.ROAM_FEE as double)+cast(t1.ROAM_FEE as double))/3 
    end APPU3_ROAM_FEE, 
    case
    when t2.LONG_FEE is null or t2.LONG_FEE=''or t3.LONG_FEE is null or t3.LONG_FEE='' then ""
    else (cast(t3.LONG_FEE as double)+cast(t2.LONG_FEE as double)+cast(t1.LONG_FEE as double))/3 
    end ARPU3_LONG_FEE,
    case
    when t2.SMS_FEE is null or t2.SMS_FEE=''or t3.SMS_FEE is null or t3.SMS_FEE='' then ""
    else (cast(t3.SMS_FEE as double)+cast(t2.SMS_FEE as double)+cast(t1.SMS_FEE as double))/3 
    end ARPU3_SMS_FEE,
    case
    when t2.MMS_FEE is null or t2.MMS_FEE=''or t3.MMS_FEE is null or t3.MMS_FEE='' then ""
    else (cast(t3.MMS_FEE as double)+cast(t2.MMS_FEE as double)+cast(t1.MMS_FEE as double))/3 
    end ARPU3_MMS_FEE,
    "" ARPU3_INTER_LONG_DURATIONS,"" ARPU3_INTER_ROAM_DURATIONS,"" ARPU3_INTER_LONG_FEE,"" ARPU3_INTER_ROAM_FEE,cast(t1.ONLINE_DAYS as int) ONLINE_DAYS,
    "" LAST_MOU_LEVEL,"" LAST_GPRS_FLOW,"" LAST_FLOW_EXC_CONTENT,cast(t1.SMS_CNT as int) SMS_CNT,"" MMS_CNT,"" SHORT_NO,t1.PRODUCT_NAME PRODUCT_NAME,
    t1.BILL_CODE BILL_CODE,cast(t1.FLOW as double) FLOW_PROP,"" FLOW_PROP1 from (select * from ${original} where imsi_no != ''and imsi_no is not null) t1 
    left outer join (select * from ${original_lastm} where imsi_no != ''and imsi_no is not null)t2 on t1.imsi_no =t2.imsi_no
    left outer join (select * from ${original_last2m} where imsi_no != ''and imsi_no is not null)t3 on t1.imsi_no = t3.imsi_no;
