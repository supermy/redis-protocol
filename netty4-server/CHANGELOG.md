### 2018-10-31
    完成Set 数据类型的支持，测试完成。

### 2018-10-28
    数据监控 0-9 a-z A-Z 数据统计图表；todo

### 2018-10-23
    list 数据类型支撑完成；
    set 数据类型支撑；（后续 zset/hyperlog/快速list-支持pop&push）

### 2018-10-14
    List 数据类型 
    val编码 = ttl|type|count|第一个元素|最后一个元素|当前元素编码
    
### 2018-10-12
    完成hashmeta and hashnode 代码重构，并且通过测试。
    完成 hashmeta stringmeta 线程安全优化，并且通过测试。
    

### 2018-10-09 
    Hash:val编码 = ttl|type|size|value
    meta 与 element value 都采用上述规则 

### 2018-09-24
    StringMeta 重构完成，除setbit & getbit 都已经完成；计划SortSet 的机制完成。
    HashMeta 开始重构；


### 2018-09-18
    链式使用；

    String: set key value
    
    创建覆盖（不知道是否存在否则多一次查询）或者删除的时候秒删方案，使用范围删除。先删除meta,再异步线程删除eledments,rocksDb.deleteRange.
      

### 2018-09-07
    key 方案；KV存储的n次读写(N>=1)。在符合Redis命令语义的情况下，编码曾设计应当尽量的减少n的次数。
        综合考虑crud，所有数据类型key 唯一；
        创建(stirng n=1,hash/list/set/sortset n=2)次，如果存在，抛出异常（覆盖创建，有hash/set/sortset/list类型，删除队列？）；如果不存在，创建；
        修改n=2，如果存在，修改；如果不存在，抛出异常；
        删除n=1，如果存在，不存在删除，删除；
        查询n=1，按Key查询；
        
    value 方案：
        set命令后，crud不同类型数据之后，废弃数据清理，删除meta,自动清理object；？快速删除，实际是在后台进行队列删除；
        
        
### 2018-02-19
    
    适配现有的数据类型
    支持 table;

### 2017-12-21
    代码进行二次重构
    
    Meta and Data 使用一个 DB;
    MetaObject TTL+TYPE+Value-size+Value
    
    数据类型        Key                                 Value
    String      [<ns>] <key> KEY_META                 KEY_STRING <MetaObject>
    Hash        [<ns>] <key> KEY_META                 KEY_HASH <MetaObject>
                [<ns>] <key> KEY_HASH_FIELD <field>   KEY_HASH_FIELD <field-value>
    Set         [<ns>] <key> KEY_META                 KEY_SET <MetaObject>
                [<ns>] <key> KEY_SET_MEMBER <member>  KEY_SET_MEMBER
    List        [<ns>] <key> KEY_META                 KEY_LIST <MetaObject>
                [<ns>] <key> KEY_LIST_ELEMENT <index> KEY_LIST_ELEMENT <element-value>
    Sorted Set  [<ns>] <key> KEY_META                 KEY_ZSET <MetaObject>
                [<ns>] <key> KEY_ZSET_SCORE <member>  KEY_ZSET_SCORE <score>
                [<ns>] <key> KEY_ZSET_SORT <score> <member> KEY_ZSET_SORT
                
    TTL 数据独立现成处理，扫描这个key，过期则取出key 删除。
    KEY_TTL_SORT [TTL_DB] "" KEY_TTL_SORT（具体为ttl+key,）             value为空
                
    
   
 