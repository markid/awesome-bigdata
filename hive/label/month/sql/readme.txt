hive和hbase整合代码，整合之后可以实现：
1. 在hive中创建的表能直接创建保存到hbase中。
2. hive中的表插入数据，插入的数据会同步更新到hbase对应的表中。
3. hbase对应的列簇值变更，也会在Hive中对应的表中变更。
4. 实现了多列，多列簇的转化。
