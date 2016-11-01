#!/bin/bash
#--------------
#功能描述：boss多表数据整合到hive宽表
#创建人：
#创建日期：
#--------------
username="***"
userpwd="******"
mysql_host="192.168.0.1"
loadprodir=$1
start_time=`date +%s`
##需要创建的表（临时表，中间表等等）

echo $loadprodir
if [ x"$loadprodir" == x ]; then
    echo "[ `date +%Y-%m-%d' '%T` ]***请传参数********************";
    exit 1;
fi
while read line
do
  ##读取配置文件
  filename=`echo $line | cut -d" " -f1`
echo $filename
  if [ $filename == "boss" ]; then

    ##需要计算日期
    dateconfig=`echo $line | cut -d" " -f2`
    ##标识按月或天分区，1：天，2：月
    ifdate=`echo $line | cut -d" " -f3`
    ##判断程序是否完成，没完成继续跑当前配置文件日期，1：完成，0：未完成
    ifflag=`echo $line | cut -d" " -f4`
    else
    exit 0;
  fi
done <${loadprodir}/etc/config.ini

##判断是否正常完成,正常完成，日期或月份+1，否则为配置文件日期

if [ $ifflag == "1" ]; then
        ##判断分区，获取分区时间格式
        if [[ $ifdate == "1" ]]; then
           datetime=`date -d "${dateconfig} +1 day" +%Y%m%d""`
           dateconfig=${datetime}
           else
           datetime=`date -d "${dateconfig} +1 month" +%Y%m""`
           dateconfig=`date -d "${dateconfig} +1 month" +%Y%m%d""`
        fi
  else
         ##判断分区，获取分区时间格式
        if [[ $ifdate == "1" ]]; then
           datetime=`date -d "${dateconfig}" +%Y%m%d""`
           dateconfig=${datetime}
           else
           datetime=`date -d "${dateconfig}" +%Y%m""`
           dateconfig=`date -d "${dateconfig}" +%Y%m%d""`
        fi
fi
logfile="${loadprodir}/log/${filename}.log"
boss_current="original_boss_"${datetime}
if [[ $ifdate == "1" ]]; then
    datetime_last=`date -d "${datetime} -1 day" +%Y%m%d""`
    datetime_last2=`date -d "${datetime} -2 day" +%Y%m%d""`
    widetable="/user/flowmarketing/widetable/day/"
    else
    datetime_last=`date -d "${datetime}01 -1 month" +%Y%m""`
    datetime_last2=`date -d "${datetime}01 -2 month" +%Y%m""`
    widetable="/user/flowmarketing/widetable/month/"
fi
boss_last="original_boss_"${datetime_last}
boss_last2="original_boss_"${datetime_last2}

tmp="/user/flowmarketing/tmp/boss/"
table_bi="base_info_tmp"
table_ci="client_identity_tmp"
table_cb="consumer_behavior_tmp"
table_crm="crm_tmp"
table_uwi="uwi_tmp"
table_teleservice="teleservice_tmp"
table_ueinfo="ueinfo_tmp"
tmp_base_info=${tmp}${table_bi}
tmp_client_identity=${tmp}${table_ci}
tmp_consumer_behavior=${tmp}${table_cb}
tmp_crm=${tmp}${table_crm}
tmp_uwi=${tmp}${table_uwi}
tmp_teleservice=${tmp}${table_teleservice}
tmp_ueinfo=${tmp}${table_ueinfo}

data="/user/flowmarketing/data/boss/"
data_boss_current=${data}${datetime}

widetable_boss=${widetable}"boss/"${datetime}
widetable_name="widetable_boss_"${datetime}

function errFun() {
if [ $? -ne 0 ]; then
    sed -i "s/${filename}.*/${filename} ${dateconfig} ${ifdate} 0/g" ${loadprodir}"/etc/config.ini"
    echo "[ `date +%Y-%m-%d' '%T` ]***hive执行失败******************** " >> ${logfile};
    exit 1;
 else
    sed -i "s/${filename}.*/${filename} ${dateconfig} ${ifdate} 1/g" ${loadprodir}"/etc/config.ini"
    echo "[ `date +%Y-%m-%d' '%T` ]***hive执行完成******************** " >> ${logfile};
fi
}


##判断表是否已经创建
tblname=`echo "select TBL_NAME from hive.TBLS where TBL_NAME='${boss_current}'" |mysql -u${username} -p${userpwd} -h${mysql_host} -P3306 -N`
if [ x"$tblname" == x ]; then
/usr/bin/hive -f "${loadprodir}/sql/boss.sql" -d location=${data_boss_current} -d table_name=${boss_current} -v -S
fi
tblname=`echo "select TBL_NAME from hive.TBLS where TBL_NAME='${boss_last}'" |mysql -u${username} -p${userpwd} -h${mysql_host} -P3306 -N`
if [ x"$tblname" == x ]; then
boss_last=${boss_current}
fi
tblname=`echo "select TBL_NAME from hive.TBLS where TBL_NAME='${boss_last2}'" |mysql -u${username} -p${userpwd} -h${mysql_host} -P3306 -N`
if [ x"$tblname" == x ]; then
boss_last2=${boss_last}
fi
##-------实现hiveSQL部分-----------
/usr/bin/hive -f "${loadprodir}/sql/boss_wide_baseinfo.sql" -d location=${tmp_base_info} -d original=${boss_current} -d table_name=${table_bi} -S
errFun
/usr/bin/hive -f "${loadprodir}/sql/boss_wide_client_identity.sql" -d location=${tmp_client_identity} -d original=${boss_current} -d table_name=${table_ci} -S
errFun
/usr/bin/hive -f "${loadprodir}/sql/boss_wide_crm.sql" -d location=${tmp_crm} -d original=${boss_current} -d table_name=${table_crm} -S
errFun
/usr/bin/hive -f "${loadprodir}/sql/boss_wide_teleservice.sql" -d location=${tmp_teleservice} -d original=${boss_current} -d original_lastm=${boss_last} -d original_last2m=${boss_last2} -d table_name=${table_teleservice} -S
errFun
/usr/bin/hive -f "${loadprodir}/sql/boss_wide_uwi.sql" -d location=${tmp_uwi} -d original=${boss_current} -d table_name=${table_uwi} -S
errFun
/usr/bin/hive -f "${loadprodir}/sql/boss_wide_ueinfo.sql" -d location=${tmp_ueinfo} -d original=${boss_current} -d original_lastm=${boss_last} -d table_name=${table_ueinfo} -S
errFun
/usr/bin/hive -f "${loadprodir}/sql/boss_wide_consumer_behavior.sql" -d location=${tmp_consumer_behavior} -d original=${boss_current} -d table_name=${table_cb} -S
errFun
/usr/bin/hive -f "${loadprodir}/sql/boss_wide_merge.sql" -d location=${widetable_boss} -d table_name=${widetable_name} -d base_info_tmp=${table_bi} -d client_identity_tmp=${table_ci} -d crm_tmp=${table_crm} -d teleservice_tmp=${table_teleservice} -d uwi_tmp=${table_uwi} -d consumer_behavior_tmp=${table_cb} -d ueinfo_tmp=${table_ueinfo} -v
errFun
##-------实现hiveSQL部分-----------
########################################################################################################
    end_time=`date +%s`
    echo "程序结束时间：[ `date +%Y-%m-%d' '%T` ]" >> ${logfile}
    echo "总运行时长：$((end_time-start_time))s" >> ${logfile}
########################################################################################################
exit 0

