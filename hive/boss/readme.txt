
BOSS sql代码列表：
1、boss.sql boss宽表建表；
2、boss_wide_baseinfo.sql  基础信息类宽表字段
3、boss_wide_client_identity.sql 客户身份特征字段
4、boss_wide_consumer_behavior.sql 消费行为信息字段
5、boss_wide_crm.sql             客户业务关系
6、boss_wide_teleservice.sql 	电信业务关系
7、boss_wide_ueinfo.sql      终端信息
8、boss_wide_uwi.sql		互联网行为、用量类属性
9、boss_wide_merge.sql   宽表整合代码


boss.sh 生成BOSS宽表的shell文件；

输入数据源：本月boss数据源  "original_boss_"${datetime}
上个月boss数据源：boss_last="original_boss_"${datetime_last}
上上个月boss数据源：boss_last2="original_boss_"${datetime_last2}

