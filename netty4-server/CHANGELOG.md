### 2019-01-15
    HyperLogLog类型支撑完成，测试完成。

### 2019-01-11
    HyperLogLog类型支撑
    kyro 与 ByteBuf的效率与大小的对比；是选择效率，还是选则压缩空间。

### 2019-01-10
    零拷贝：ZerocopyFile、ZerocopyServer
    轮询：RoundRobin2Test
    
    地理位置：GeoHashTest/GeohashUtil
    基数统计：HyperLogLogTest
    是否重复：BloomFilterTest
    
    Json查询；JsonPathTest 
    Json遍历：GsonTest  
    
    二叉树-链式存储：BinaryTree
    二叉树：BTree 
    
    位计算：SimpleSet、BitSetTest、BitSetDemo、BitMapTest、BitMap
    Bitmap：RoaringBitmapTest
    

### 2019-01-06
    Guava LoadingCache 完成 RoaringBitmap/Hyperloglog/BloomFilter定时同步到RocksDb;
    高效的多维空间点索引算法 — Geohash 和 Google S2
    


### 2019-01-03
    实现位图索引：数据清洗时完成数据字典业务整理。
        1.索引存储：数据Key Hash,表名+indexBitMap+rowNumber=dataId;数据字典Hash,表名+编码字段+字段枚举值=枚举编码；索引数据Hash,表名+索引字段+字段字典枚举值=索引bitmap
        2.索引检索：表名+字段+数据字典枚举值获取索引bitmap,通过索引bitmap的 and or 运算，获取为true的位置即为rowNumber;
        3.索引返回：Hash获取，表名+rowNumber 返回dataId,从数据源通过dataId获取数据；
        注意采用kryo+RoaringBitmap进行序列化及bit计算。
        
    BloomFilter & Hyperloglog 去重 & 统计

### 2018-12-19
    Json 类型支撑采用JsonPath 支持，内置GsonJsonProvider与GsonMappingProvider提高效率，Json 字符串落盘RocksDb;

### 2018-11-24
    Resid List类型采用RocksDb 原生支撑完成代码测试；
    Todo 扩充指令，支持json 数据；lua 解析json 为基本数据???；java 解析为json ->tree node

### 2018-11-15

    multi / exec  /watch
    单个 Redis 命令的执行是原子性的，但 Redis 没有在事务上增加任何维持原子性的机制，所以 Redis 事务的执行并不是原子性的。
    事务可以理解为一个打包的批量执行脚本，但批量指令并非原子化的操作，中间某条指令的失败不会导致前面已做指令的回滚，也不会造成后续的指令不做。

    Redis 发布订阅命令可以用rabbitma or kafka 更快，不在版本中实现。


    Redis中hyperloglog是用来做基数统计的，其优点是：在输入元素的数量或者体积非常非常大的时候，计算基数所需的空间总是固定的，并且是很小的。
    在Redis里面，每个Hyperloglog键只需要12Kb的大小就能计算接近2^64个不同元素的基数，但是hyperloglog只会根据输入元素来计算基数，而不会
    存储元素本身，所以不能像集合那样返回各个元素本身。
    Redis的HyperLogLog内存占用仅仅4.32MB，足够应对绝大部分场景。不在redis硬盘版本中实现。
    
    HyperLogLog 实现独立 IP 计算功能，访问者的实时估算，足够大的样本误差在2%左右
    独立IP数量 一天  一个月  一年  一年（使用集合）
    
    一百万  12KB 360KB 4.32MB 5.4 GB
    
    一千万 12KB 360KB 4.32MB 54 GB
    
    一亿  12KB 360KB 4.32MB 540 GB
    

### 2018-11-5
    Redis ZSet 数据类型支撑完成，代码需优化重构；
    
### 2018-11-01
    Redis ZSet 数据类型的支撑；

### 2018-10-31
    完成Set 数据类型的支持，测试完成。
    QList 不支持 LINSERT命令可采用原生的RocksDb 效率更高 todo

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
    