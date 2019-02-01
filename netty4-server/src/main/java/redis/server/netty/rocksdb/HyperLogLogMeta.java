package redis.server.netty.rocksdb;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.google.common.base.Throwables;
import com.google.common.cache.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.supermy.util.MyUtils;
import redis.netty4.*;
import redis.server.netty.RedisException;
import redis.server.netty.utis.DataType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static redis.netty4.BulkReply.NIL_REPLY;
import static redis.netty4.IntegerReply.integer;
import static redis.netty4.StatusReply.OK;
import static redis.server.netty.rocksdb.RedisBase.invalidValue;
import static redis.server.netty.rocksdb.RocksdbRedis._toposint;
import static redis.util.Encoding.bytesToNum;
import static redis.util.Encoding.numToBytes;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

/**
 *
 * Redis HyperLogLog 是用来做基数统计的算法，HyperLogLog 的优点是，在输入元素的数量或者体积非常非常大时，计算基数所需的空间总是固定 的、并且是很小的。
 *
 * 在 Redis 里面，每个 HyperLogLog 键只需要花费 12 KB 内存，就可以计算接近 2^64 个不同元素的基 数。这和计算基数时，元素越多耗费内存就越多的集合形成鲜明对比。
 *
 * 但是，因为 HyperLogLog 只会根据输入元素来计算基数，而不会储存输入元素本身，所以 HyperLogLog 不能像集合那样，返回输入的各个元素。
 *
 * <p>
 * HyperLogLog
 * <p>
 * HyperLogLog      [<ns>] <key> KEY_META                 KEY_HyperLogLog <MetaObject>
 * <p>
 * <p>
 * key 都采用 | 分隔符号
 * value 使用Kyro 进行序列化；
 *      业务数据转化：hll.toBytes()
 *      业务数据恢复：HLL.fromBytes(bytes);
 *
 * * getKey 一般是包含组合键；
 * * getkey0 是纯粹的业务主键；
 *
 * <p>
 * Created by moyong on 2019/01/08.
 * Update by moyong 2019/01/08.
 * method: PFADD key element [element ...] ；PFCOUNT key [key ...] ；PFMERGE destkey sourcekey [sourcekey ...]
 * <p>
 * not support method: getbit setbit
 */
public class HyperLogLogMeta {

    private static Logger log = Logger.getLogger(HyperLogLogMeta.class);


    ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();

    private static RocksDB db;

    private byte[] NS;
    private static byte[] TYPE = DataType.KEY_META;

    private ByteBuf metaKey;
    private ByteBuf metaVal;


    private HyperLogLogMeta() {
    }

    private static HyperLogLogMeta instance = new HyperLogLogMeta();

    /**
     * 使用入口
     * <p>
     * db0 数据库
     * ns0 namespace
     *
     * @param db0
     * @param ns0
     * @return
     */
    public static HyperLogLogMeta getInstance(RocksDB db0, byte[] ns0) {
        instance.db = db0;
        instance.NS = ns0;
        return instance;
    }

    /**
     * 构造 MetaKey
     *
     * @param key0
     * @return
     * @throws RedisException
     */
    public HyperLogLogMeta genMetaKey(byte[] key0) throws RedisException {
        if (key0 == null) {
            throw new RedisException(String.format("主键不能为空"));
        }
        instance.metaKey = Unpooled.wrappedBuffer(instance.NS, DataType.SPLIT, key0, DataType.SPLIT, TYPE);
        return instance;
    }


    private byte[] getKey0() {
        metaKey.resetReaderIndex();
        ByteBuf bb = metaKey.slice(NS.length + DataType.SPLIT.length, metaKey.readableBytes() - 8);
        return bb.readBytes(bb.readableBytes()).array();
    }

    private String getKey0Str() {
        return new String(getKey0());
    }

    private byte[] getKey() {
        metaKey.resetReaderIndex();
        return metaKey.readBytes(metaKey.readableBytes()).array();
    }

//    public void setKey0(byte[] key0)  {
//        metaKey.resetReaderIndex();
//        this.metaKey = Unpooled.wrappedBuffer(NS, key0, TYPE);
//    }


    private byte[] getVal() {
        if (metaVal == null) {
            return null;
        } else {
            metaVal.resetReaderIndex();
            return metaVal.readBytes(metaVal.readableBytes()).array();
        }
    }

    private byte[] getVal0() throws RedisException {
        //test fixme
        metaVal.resetReaderIndex();
        ByteBuf valueBuf = metaVal.slice(8 + 4 + 4 + 3, metaVal.readableBytes() - 8 - 4 - 4 - 3);
        //数据过期处理
        if (getTtl() < now() && getTtl() != -1) {
            try {
                db.delete(getKey());

                metaVal = null;
                metaKey = null;

                return null;
            } catch (RocksDBException e) {
                e.printStackTrace();
                throw new RedisException(e.getMessage());
            }
        }
        return valueBuf.readBytes(valueBuf.readableBytes()).array();
    }

    private String getVal0Str() throws RedisException {
        return new String(getVal0());
    }

    protected HyperLogLogMeta genVal(byte[] value, long expiration) {

        ByteBuf ttlBuf = Unpooled.buffer(12);
        ttlBuf.writeLong(expiration); //ttl 无限期 -1
        ttlBuf.writeBytes(DataType.SPLIT);

        ttlBuf.writeInt(DataType.KEY_HYPERLOGLOG); //value type
        ttlBuf.writeBytes(DataType.SPLIT);

        ttlBuf.writeInt(value.length); //value size
        ttlBuf.writeBytes(DataType.SPLIT);


        ByteBuf valueBuf = Unpooled.wrappedBuffer(value); //零拷贝
        this.metaVal = Unpooled.wrappedBuffer(ttlBuf, valueBuf);//零拷贝

//      return valbuf.readBytes(valbuf.readableBytes()).array();

//        return getVal();
        return this;
    }

    /**
     * 分解 Value,获取业务数据
     *
     * @param key0
     * @param values
     * @return
     * @throws RedisException
     */
    protected byte[] parseValue(byte[] key0, byte[] values) throws RedisException {
        if (values != null) {

            ByteBuf vvBuf = Unpooled.wrappedBuffer(values);

            vvBuf.resetReaderIndex();

            ByteBuf ttlBuf = vvBuf.readSlice(8);
            ByteBuf typeBuf = vvBuf.slice(8 + 1, 4);
            ByteBuf sizeBuf = vvBuf.slice(8 + 1 + 4 + 1, 4);

            ByteBuf valueBuf = vvBuf.slice(8 + 4 + 4 + 3, values.length - 8 - 4 - 4 - 3);

            long ttl = ttlBuf.readLong();//ttl
            long size = sizeBuf.readInt();//长度数据

            //数据过期处理,对应的数据返回null；
            if (ttl < now() && ttl != -1) {
                try {
                    db.delete(key0);
                } catch (RocksDBException e) {
                    e.printStackTrace();
                    throw new RedisException(e.getMessage());
                }
                return null;
            }

            return valueBuf.readBytes(valueBuf.readableBytes()).array();

        } else return null; //数据不存在 ？ 测试验证
    }


    public long getTtl() {
        return metaVal.getLong(0);
    }

    public int getSize() {
        return metaVal.getInt(8 + 4 + 2);
    }


    private long now() {
        return System.currentTimeMillis();
    }


    public String info() throws RedisException {

        StringBuilder sb = new StringBuilder(getKey0Str());

        sb.append(":");
        sb.append("  size=");
        sb.append(getSize());
        sb.append("  str:");
        sb.append(getVal0Str());

        log.debug(sb.toString());

        return sb.toString();
    }



    /**
     * 主键索赔员类型
     *
     * @return
     */
    private byte[] genKeyPartten() {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(NS, DataType.SPLIT, getKey0(), DataType.SPLIT);
        return byteBuf.readBytes(byteBuf.readableBytes()).array();
    }

    /**
     * 指定元素子类型
     *
     * @param filedType
     * @return
     * @throws RedisException
     */
    public byte[] genKeyPartten(byte[] filedType) throws RedisException {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(NS, DataType.SPLIT, getKey0(), DataType.SPLIT, filedType, DataType.SPLIT);
        return byteBuf.readBytes(byteBuf.readableBytes()).array();
    }

    /**
     * 清除所有key
     *
     * @param key0
     * @throws RedisException
     */
    public void cleanBy(byte[] key0) throws RedisException {

        cleanBy(db,key0);
    }

    private void cleanBy(RocksDB db9,byte[] key0) throws RedisException {
        ByteBuf byteBufBegin = Unpooled.wrappedBuffer(key0);
        ByteBuf byteBufEnd = Unpooled.wrappedBuffer(key0, "z".getBytes());

        byte[] begin = byteBufBegin.readBytes(byteBufBegin.readableBytes()).array();
        byte[] end = byteBufEnd.readBytes(byteBufEnd.readableBytes()).array();

        try {
            db9.deleteRange(begin, end);
        } catch (RocksDBException e) {
            e.printStackTrace();
            throw new RedisException(e.getMessage());
        }

    }

    /**
     * 如果字段是哈希表中的一个新建字段，并且值设置成功，返回 1 。 如果哈希表中域字段已经存在且旧值已被新值覆盖，返回 0 。
     *
     * @param key0
     * @param val1
     * @return
     * @throws RedisException
     */
    public StatusReply set(byte[] key0, byte[] val1, byte[] seconds2) throws RedisException {
        //todo  ???数据是否存在

        genMetaKey(key0);

        long ttl = 0;
        if (seconds2 == null) {
            ttl = -1; //ttl 无限期 -1

        } else {
            ttl = RocksdbRedis.bytesToLong(seconds2);//fixme 重构
        }

        genVal(val1, ttl);

        try {

            db.put(getKey(), getVal());

        } catch (RocksDBException e) {
            throw new RedisException(e.getMessage());
        }



//        singleThreadExecutor.execute(new MetaCleanCaller(db,
//                genKeyPartten(DataType.KEY_HASH_FIELD),
//                genKeyPartten(DataType.KEY_LIST_ELEMENT),
//                genKeyPartten(DataType.KEY_SET_MEMBER),
//                genKeyPartten(DataType.KEY_ZSET_SCORE),
//                genKeyPartten(DataType.KEY_ZSET_SORT)));


        return OK;
    }


    class MetaCleanCaller implements Runnable {

        private RocksDB db0;
        private byte[] keyPartten1;
        private byte[] keyPartten2;
        private byte[] keyPartten3;
        private byte[] keyPartten4;
        private byte[] keyPartten5;

        public MetaCleanCaller(RocksDB db0, byte[] key1, byte[] key2, byte[] key3, byte[] key4, byte[] key5) {
            this.db0 = db0;
            this.keyPartten1 = key1;
            this.keyPartten2 = key2;
            this.keyPartten3 = key3;
            this.keyPartten4 = key4;
            this.keyPartten5 = key5;
        }

        @Override
        public void run() {
            try {
                cleanBy(db0,keyPartten1);
                cleanBy(db0,keyPartten2);
                cleanBy(db0,keyPartten3);
                cleanBy(db0,keyPartten4);
                cleanBy(db0,keyPartten5);
            } catch (RedisException e) {
                e.printStackTrace();
//                throw new RedisException(e.getMessage());
            }

        }
    }


    /**
     * 通过key 获取string 的数据
     *
     * @param key0
     * @return
     * @throws RedisException
     */
    public BulkReply get(byte[] key0) throws RedisException {
        genMetaKey(key0);

        try {

            if (metaKey == null) {
                throw new RedisException(String.format("主键不能为空"));
            }

            byte[] values = db.get(getKey());

            if (values == null) {
                return NIL_REPLY;
            }

            this.metaVal = Unpooled.wrappedBuffer(values);

            return new BulkReply(getVal0());

        } catch (RocksDBException e) {
            e.printStackTrace();
            throw new RedisException(e.getMessage());
        }

    }





//////////////////////
    public int getType() throws RedisException {
        if (metaVal == null) return -1;
        return this.metaVal.getInt(8 + 1);
    }

    /**
     * 获取meta 数据
     *
     * @return
     * @throws RedisException
     */
    protected HyperLogLogMeta getMeta() throws RedisException {

        try {
            byte[] value = db.get(getKey());
            if (value == null) this.metaVal = null;
            else
                this.metaVal = Unpooled.wrappedBuffer(value);
        } catch (RocksDBException e) {
            e.printStackTrace();
            throw new RedisException(e.getMessage());
        }

        return this;
    }


    /**
     * Pfadd 命令将所有元素参数添加到 HyperLogLog 数据结构中。
     *
     * 整型，如果至少有个元素被添加返回 1， 否则返回 0。
     *
     * @param args
     * @return
     * @throws RedisException
     */
    public IntegerReply pfadd(byte[]... members) throws RedisException{
        if (members.length == 0) {
            throw new RedisException("wrong number of arguments for PFADD");
        }

        //判断类型，非hash 类型返回异常信息； fixme 改走缓存
        int type = getMeta().getType();

        if (type != -1 && type != DataType.KEY_HYPERLOGLOG) {
            //抛出异常 类型不匹配
            throw invalidValue();
        }

        log.debug(getKey0Str());

        //初始化HLL
        HyperLogLog hll= null;//尽量放在循环体外，可以节约一半的时间；
        try {
            hll = hyperLogLogCache.get(Unpooled.wrappedBuffer(getKey0()));
        } catch (ExecutionException e) {
            e.printStackTrace();
            Throwables.propagateIfPossible(e,RedisException.class);
        }

//        BulkReply bulkReply = get(getKey0());
//        if (bulkReply.isEmpty()){
//             hll = new HyperLogLog(12);
//        } else {
//            try {
//                hll=HyperLogLog.Builder.build(getVal0());
//            } catch (IOException e) {
//                e.printStackTrace();
//                Throwables.propagateIfPossible(e,RedisException.class);
//            }
//        }

        //赋值HLL
        for (int i = 0; i < members.length; i++) {
            hll.offer(members[i]);
        }

        log.debug(hll.cardinality());

//        //持久化HLL
//        try {
//            set(getKey0(),hll.getBytes(),null);
//        } catch (IOException e) {
//            e.printStackTrace();
//            Throwables.propagateIfPossible(e,RedisException.class);
//        }

        return integer(members.length);
    }


    /**
     * Pfcount 命令返回给定 HyperLogLog 的基数估算值。
     * @param key0
     * @return
     * @throws RedisException
     */
    public IntegerReply pfcount(byte[]... key0) throws RedisException {
        long count=0l;
        for (byte[] key:key0
             ) {
//            get(key);

            HyperLogLog hll= null;//尽量放在循环体外，可以节约一半的时间；
            try {
                hll = hyperLogLogCache.get(Unpooled.wrappedBuffer(key));
            } catch (ExecutionException e) {
                e.printStackTrace();
                Throwables.propagateIfPossible(e,RedisException.class);
            }

//            HyperLogLog hll= null;
//            try {
//                hll = HyperLogLog.Builder.build(getVal0());
//            } catch (IOException e) {
//                e.printStackTrace();
//                Throwables.propagateIfPossible(e,RedisException.class);
//            }
            count=count+hll.cardinality();
        }
        return integer(count);
    }


    public IntegerReply pfmerge(byte[]... key0) throws RedisException {
        long count=0l;

        //合并
        try {
        HyperLogLog hll0=null;
        for (int i = 0; i <key0.length ; i++) {
            if (i==0){

                hll0=hyperLogLogCache.get(Unpooled.wrappedBuffer(key0[i]));//尽量放在循环体外，可以节约一半的时间；


//                get(key0[i]);
//                try {
//                    hll0=HyperLogLog.Builder.build(getVal0());
//                } catch (IOException e) {
//                    e.printStackTrace();
//                    Throwables.propagateIfPossible(e,RedisException.class);
//                }
            }else{
                HyperLogLog hll=hyperLogLogCache.get(Unpooled.wrappedBuffer(key0[i]));//尽量放在循环体外，可以节约一半的时间；

                log.debug(hll0.cardinality());
                log.debug(hll.cardinality());

                hll0.addAll(hll);

                log.debug(hll0.cardinality());


//                get(key0[i]);
//                HyperLogLog hll= null;
//                try {
//                    hll = HyperLogLog.Builder.build(getVal0());
//                    hll0.addAll(hll);
//                } catch (IOException e) {
//                    e.printStackTrace();
//                    Throwables.propagateIfPossible(e,RedisException.class);
//                } catch (CardinalityMergeException e) {
//                    e.printStackTrace();
//                    Throwables.propagateIfPossible(e,RedisException.class);
//                }

            }
        }
            //提取数据
            count=hll0.cardinality();

        } catch (ExecutionException | CardinalityMergeException e) {
            e.printStackTrace();
            Throwables.propagateIfPossible(e,RedisException.class);
        }

        hyperLogLogCache.invalidate(MyUtils.ByteBuf2String(getMetaKey1(key0[0])));
//
//        try {
//            set(getKey0(), hll0.getBytes(),null);
//        } catch (IOException e) {
//            e.printStackTrace();
//            Throwables.propagateIfPossible(e,RedisException.class);
//        }

        return integer(count);
    }

    /**
     * 批量删除主键(0-9.A-Z,a-z)；
     * 根据genkey 特征，增加风格符号，避免误删除数据；
     *
     * @param key0
     * @throws RedisException
     */
    public void deleteRange(byte[] key0) throws RedisException {



        ByteBuf byteBufBegin = Unpooled.wrappedBuffer(NS, DataType.SPLIT, key0, DataType.SPLIT);
        ByteBuf byteBufEnd = Unpooled.wrappedBuffer(NS, DataType.SPLIT, key0, DataType.SPLIT, "z".getBytes());

        byte[] begin = byteBufBegin.readBytes(byteBufBegin.readableBytes()).array();
        byte[] end = byteBufEnd.readBytes(byteBufEnd.readableBytes()).array();

        log.debug(String.format("begin %s -> end %s", new String(begin),new String(end)));

        try {
            db.deleteRange(begin, end);
        } catch (RocksDBException e) {
            e.printStackTrace();
            throw new RedisException(e.getMessage());
        }
    }


    public static byte[] getMetaKey(byte[] key0) {
        ByteBuf metaKey = MyUtils.concat(instance.NS, DataType.SPLIT, key0, DataType.SPLIT, TYPE);
        return MyUtils.toByteArray(metaKey);
    }

    public static ByteBuf getMetaKey1(byte[] key0) {
        ByteBuf metaKey = MyUtils.concat(instance.NS, DataType.SPLIT, key0, DataType.SPLIT, TYPE);
        return metaKey;
    }

    public static byte[] getMetaVal(byte[] value, long expiration) {

        ByteBuf buf = Unpooled.buffer(12);
        buf.writeLong(expiration); //ttl 无限期 -1
        buf.writeBytes(DataType.SPLIT);

        buf.writeInt(DataType.KEY_BLOOMFILTER); //value type
        buf.writeBytes(DataType.SPLIT);

        buf.writeInt(value.length); //value size
        buf.writeBytes(DataType.SPLIT);

        //业务数据
        buf.writeBytes(value);

        return MyUtils.toByteArray(buf);

    }

    public static byte[] getMetaVal0(ByteBuf metaVal) throws RedisException {
        metaVal.resetReaderIndex();
        ByteBuf valueBuf = metaVal.slice(8 + 4 + 4 + 3, metaVal.readableBytes() - 8 - 4 - 4 - 3);
        return MyUtils.toByteArray(valueBuf);
    }

    /**
     * 在内存中进行计算，定期持久化到HyperLogLog;
     */
    static LoadingCache<ByteBuf, HyperLogLog> hyperLogLogCache
            // CacheBuilder的构造函数是私有的，只能通过其静态方法newBuilder()来获得CacheBuilder的实例
            = CacheBuilder.newBuilder()
            // 设置并发级别为8，并发级别是指可以同时写缓存的线程数
            .concurrencyLevel(8)
            // 设置写缓存后8秒钟过期
            .expireAfterWrite(60, TimeUnit.SECONDS)
            // 设置缓存容器的初始容量为10
            .initialCapacity(10)
//            .maximumWeight(10*1024*1024)
            // 设置缓存最大容量为100，超过100之后就会按照LRU最近虽少使用算法来移除缓存项
            .maximumSize(100)
            // 设置要统计缓存的命中率
            .recordStats()
            // 设置缓存的移除通知 ,将数据持久化到RocksDb
            .removalListener(new RemovalListener<ByteBuf, HyperLogLog>() {
                public void onRemoval(RemovalNotification<ByteBuf, HyperLogLog> notification) {
                    log.debug(String.format("因为%s的原因，%s=%s已经删除",
                            notification.getCause(),
                            MyUtils.ByteBuf2String(notification.getKey()),
                            notification.getValue().cardinality()));

                    try {
                        {
                            HyperLogLog hll = notification.getValue();

                            db.put(getMetaKey(notification.getKey().array()), getMetaVal(hll.getBytes(), -1));


                            log.debug(String.format("获取HyperLogLog(基数=%s) 列表，持久化到 RocksDb。\n key:%s 初始验证：%s  修改验证：%s",
                                    hll.cardinality(),
                                    MyUtils.ByteBuf2String(notification.getKey()),
                                    hll.offer("test_one".getBytes()),
                                    hll.offer("test_modify".getBytes())
                                    )
                            );
                        }
                    } catch (RocksDBException | IOException e) {
                        e.printStackTrace();
//                        Throwables.propagateIfPossible(e, RedisException.class);
                    }

                }
            })

            // build方法中可以指定CacheLoader，在缓存不存在时通过CacheLoader的实现自动加载缓存
            // 从RocksDb中加载数据
            .build(new CacheLoader<ByteBuf, HyperLogLog>() {
                @Override
                public HyperLogLog load(ByteBuf key0) throws Exception {
                    key0.resetReaderIndex();
//                    byte[] value = db.get(key.array());
                    byte[] value = db.get(getMetaKey(key0.array()));
                    HyperLogLog hll=null;

                    if (value==null){
                        hll = new HyperLogLog(12);
                    }else {
                        byte[] metaVal0 = getMetaVal0(MyUtils.concat(value));
                        hll =HyperLogLog.Builder.build(metaVal0);
                    }


                    log.debug(String.format("加载HyperLogLog从RocksDb: key %s  数量%s  初始验证：%s ",
                            MyUtils.ByteBuf2String(key0),
                            hll.cardinality(),
                            hll.offer("test_one".getBytes()))
                    );
                    return hll;
                }
            });

    public static void main(String[] args) throws Exception {
        hyperLogLogTest();

    }

    /**
     * Hash数据集测试
     *
     * @throws RedisException
     */
    private static void hyperLogLogTest() throws RedisException, IOException {


    }

}
