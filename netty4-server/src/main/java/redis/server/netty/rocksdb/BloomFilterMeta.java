package redis.server.netty.rocksdb;

import com.google.common.base.Throwables;
import com.google.common.cache.*;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.log4j.Logger;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.supermy.util.MyUtils;
import redis.netty4.BulkReply;
import redis.netty4.IntegerReply;
import redis.netty4.StatusReply;
import redis.server.netty.RedisException;
import redis.server.netty.utis.DataType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static redis.netty4.BulkReply.NIL_REPLY;
import static redis.netty4.IntegerReply.integer;
import static redis.netty4.StatusReply.OK;
import static redis.server.netty.rocksdb.RedisBase.invalidValue;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

/**
 * Bloom Filter是一个占用空间很小、效率很高的随机数据结构，它由一个bit数组和一组Hash算法构成。可用于判断一个元素是否在一个集合中，查询效率很高（1-N，最优能逼近于1）。
 * <p>
 * 在很多场景下，我们都需要一个能迅速判断一个元素是否在一个集合中。譬如：
 * <p>
 * 网页爬虫对URL的去重，避免爬取相同的URL地址；
 * <p>
 * 反垃圾邮件，从数十亿个垃圾邮件列表中判断某邮箱是否垃圾邮箱（同理，垃圾短信）；
 * <p>
 * 缓存击穿，将已存在的缓存放到布隆中，当黑客访问不存在的缓存时迅速返回避免缓存及DB挂掉。
 *
 *
 * <p>
 * BloomFilter
 * <p>
 * BloomFilter      [<ns>] <key> KEY_META                 KEY_BloomFilter <MetaObject>
 * <p>
 * <p>
 * key 都采用 | 分隔符号
 * value 使用Kyro 进行序列化；
 * 业务数据转化：filter.writeTo(out)
 * 业务数据合并：filter.putAll(filter);
 * 业务数据恢复：BloomFilter.readFrom(in,Funnels.stringFunnel(Charsets.UTF_8));
 * <p>
 * * getKey 一般是包含组合键；
 * * getkey0 是纯粹的业务主键；
 *
 * <p>
 * Created by moyong on 2019/01/16.
 * Update by moyong 2019/01/16.
 * method: BFADD key element [element ...] ；BFExist key [key ...] ；BFMERGE destkey sourcekey [sourcekey ...]
 * <p>
 */
public class BloomFilterMeta {

    private static Logger log = Logger.getLogger(BloomFilterMeta.class);


//    ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();

    private static RocksDB db;

    private byte[] NS;
    private static byte[] TYPE = DataType.KEY_META;

    private ByteBuf metaKey;
    private ByteBuf metaVal;


    private BloomFilterMeta() {
    }

    private static BloomFilterMeta instance = new BloomFilterMeta();

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
    public static BloomFilterMeta getInstance(RocksDB db0, byte[] ns0) {
        instance.db = db0;
        instance.NS = ns0;
        return instance;
    }

    /**
     * 构造 MetaKey
     * <p>
     * todo
     *
     * @param key0
     * @return
     * @throws RedisException
     */
    public BloomFilterMeta genMetaKey(byte[] key0) throws RedisException {
        if (key0 == null) {
            throw new RedisException(String.format("主键不能为空"));
        }
        metaKey = MyUtils.concat(instance.NS, DataType.SPLIT, key0, DataType.SPLIT, TYPE);

        return this;
    }


    public static byte[] getMetaKey(byte[] key0) {
        ByteBuf metaKey = MyUtils.concat(instance.NS, DataType.SPLIT, key0, DataType.SPLIT, TYPE);
        return MyUtils.toByteArray(metaKey);
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


//    private byte[] getVal() {
//        if (metaVal == null) {
//            return null;
//        } else {
//            metaVal.resetReaderIndex();
//            return metaVal.readBytes(metaVal.readableBytes()).array();
//        }
//    }

//    private byte[] getVal0() throws RedisException {
//        //test fixme
//        metaVal.resetReaderIndex();
//        ByteBuf valueBuf = metaVal.slice(8 + 4 + 4 + 3, metaVal.readableBytes() - 8 - 4 - 4 - 3);
//        //数据过期处理
////        if (getTtl() < now() && getTtl() != -1) {
////            try {
////                db.delete(getKey());
////
////                metaVal = null;
////                metaKey = null;
////
////                return null;
////            } catch (RocksDBException e) {
////                e.printStackTrace();
////                throw new RedisException(e.getMessage());
////            }
////        }
//        return valueBuf.readBytes(valueBuf.readableBytes()).array();
//    }

    public static byte[] getMetaVal0(ByteBuf metaVal) throws RedisException {
        metaVal.resetReaderIndex();
        ByteBuf valueBuf = metaVal.slice(8 + 4 + 4 + 3, metaVal.readableBytes() - 8 - 4 - 4 - 3);
        return MyUtils.toByteArray(valueBuf);
    }

//    private String getVal0Str() throws RedisException {
//        return new String(getVal0());
//    }

//    protected BloomFilterMeta genVal(byte[] value, long expiration) {
//
//        ByteBuf ttlBuf = Unpooled.buffer(12);
//        ttlBuf.writeLong(expiration); //ttl 无限期 -1
//        ttlBuf.writeBytes(DataType.SPLIT);
//
//        ttlBuf.writeInt(DataType.KEY_BLOOMFILTER); //value type
//        ttlBuf.writeBytes(DataType.SPLIT);
//
//        ttlBuf.writeInt(value.length); //value size
//        ttlBuf.writeBytes(DataType.SPLIT);
//
//
//        ByteBuf valueBuf = Unpooled.wrappedBuffer(value); //零拷贝
//        this.metaVal = Unpooled.wrappedBuffer(ttlBuf, valueBuf);//零拷贝
//
////      return valbuf.readBytes(valbuf.readableBytes()).array();
//
////        return getVal();
//        return this;
//    }


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

//    /**
//     * 分解 Value,获取业务数据
//     *
//     * @param key0
//     * @param values
//     * @return
//     * @throws RedisException
//     */
//    protected byte[] parseValue(byte[] key0, byte[] values) throws RedisException {
//        if (values != null) {
//
//            ByteBuf vvBuf = Unpooled.wrappedBuffer(values);
//
//            vvBuf.resetReaderIndex();
//
//            ByteBuf ttlBuf = vvBuf.readSlice(8);
//            ByteBuf typeBuf = vvBuf.slice(8 + 1, 4);
//            ByteBuf sizeBuf = vvBuf.slice(8 + 1 + 4 + 1, 4);
//
//            ByteBuf valueBuf = vvBuf.slice(8 + 4 + 4 + 3, values.length - 8 - 4 - 4 - 3);
//
//            long ttl = ttlBuf.readLong();//ttl
//            long size = sizeBuf.readInt();//长度数据
//
////            //数据过期处理,对应的数据返回null；
////            if (ttl < now() && ttl != -1) {
////                try {
////                    db.delete(key0);
////                } catch (RocksDBException e) {
////                    e.printStackTrace();
////                    throw new RedisException(e.getMessage());
////                }
////                return null;
////            }
//
//            return valueBuf.readBytes(valueBuf.readableBytes()).array();
//
//        } else return null; //数据不存在 ？ 测试验证
//    }

//
//    public long getTtl() {
//        return metaVal.getLong(0);
//    }
//
//    public int getSize() {
//        return metaVal.getInt(8 + 4 + 2);
//    }
//
//
//    private long now() {
//        return System.currentTimeMillis();
//    }

//
//    public String info() throws RedisException {
//
//        StringBuilder sb = new StringBuilder(getKey0Str());
//
//        sb.append(":");
//        sb.append("  size=");
//        sb.append(getSize());
//        sb.append("  str:");
//        sb.append(getVal0Str());
//
//        log.debug(sb.toString());
//
//        return sb.toString();
//    }


//    /**
//     * 主键索赔员类型
//     *
//     * @return
//     */
//    private byte[] genKeyPartten() {
//        ByteBuf byteBuf = MyUtils.concat(NS, DataType.SPLIT, getKey0(), DataType.SPLIT);
//        return byteBuf.readBytes(byteBuf.readableBytes()).array();
//    }

//    /**
//     * 指定元素子类型
//     *
//     * @param filedType
//     * @return
//     * @throws RedisException
//     */
//    public byte[] genKeyPartten(byte[] filedType) throws RedisException {
//        ByteBuf byteBuf = MyUtils.concat(NS, DataType.SPLIT, getKey0(), DataType.SPLIT, filedType, DataType.SPLIT);
//        return byteBuf.readBytes(byteBuf.readableBytes()).array();
//    }

//    /**
//     * 清除所有key
//     *
//     * @param key0
//     * @throws RedisException
//     */
//    public void cleanBy(byte[] key0) throws RedisException {
//
//        cleanBy(db, key0);
//    }

//    private void cleanBy(RocksDB db9, byte[] key0) throws RedisException {
//        ByteBuf byteBufBegin = MyUtils.concat(key0);
//        ByteBuf byteBufEnd = MyUtils.concat(key0, "z".getBytes());
//
//        byte[] begin = byteBufBegin.readBytes(byteBufBegin.readableBytes()).array();
//        byte[] end = byteBufEnd.readBytes(byteBufEnd.readableBytes()).array();
//
//        try {
//            db9.deleteRange(begin, end);
//        } catch (RocksDBException e) {
//            e.printStackTrace();
//            throw new RedisException(e.getMessage());
//        }
//
//    }

//    /**
//     * 如果字段是哈希表中的一个新建字段，并且值设置成功，返回 1 。 如果哈希表中域字段已经存在且旧值已被新值覆盖，返回 0 。
//     *
//     * @param key0
//     * @param val1
//     * @return
//     * @throws RedisException
//     */
//    public StatusReply set(byte[] key0, byte[] val1, byte[] seconds2) throws RedisException {
//        //todo  ???数据是否存在
//
//        genMetaKey(key0);
//
//        long ttl = 0;
//        if (seconds2 == null) {
//            ttl = -1; //ttl 无限期 -1
//
//        } else {
//            ttl = RocksdbRedis.bytesToLong(seconds2);//fixme 重构
//        }
//
//        genVal(val1, ttl);
//
//        try {
//
//            db.put(getKey(), getVal());
//
//        } catch (RocksDBException e) {
//            throw new RedisException(e.getMessage());
//        }
//
//
////        singleThreadExecutor.execute(new MetaCleanCaller(db,
////                genKeyPartten(DataType.KEY_HASH_FIELD),
////                genKeyPartten(DataType.KEY_LIST_ELEMENT),
////                genKeyPartten(DataType.KEY_SET_MEMBER),
////                genKeyPartten(DataType.KEY_ZSET_SCORE),
////                genKeyPartten(DataType.KEY_ZSET_SORT)));
//
//
//        return OK;
//    }


//    class MetaCleanCaller implements Runnable {
//
//        private RocksDB db0;
//        private byte[] keyPartten1;
//        private byte[] keyPartten2;
//        private byte[] keyPartten3;
//        private byte[] keyPartten4;
//        private byte[] keyPartten5;
//
//        public MetaCleanCaller(RocksDB db0, byte[] key1, byte[] key2, byte[] key3, byte[] key4, byte[] key5) {
//            this.db0 = db0;
//            this.keyPartten1 = key1;
//            this.keyPartten2 = key2;
//            this.keyPartten3 = key3;
//            this.keyPartten4 = key4;
//            this.keyPartten5 = key5;
//        }
//
//        @Override
//        public void run() {
//            try {
//                cleanBy(db0, keyPartten1);
//                cleanBy(db0, keyPartten2);
//                cleanBy(db0, keyPartten3);
//                cleanBy(db0, keyPartten4);
//                cleanBy(db0, keyPartten5);
//            } catch (RedisException e) {
//                e.printStackTrace();
////                throw new RedisException(e.getMessage());
//            }
//
//        }
//    }


    /**
     * 通过key 获取string 的数据
     *
     * @param key0
     * @return
     * @throws RedisException
     */
    public BulkReply get(byte[] key0) throws RedisException {

        try {

            byte[] values = db.get(getMetaKey(key0));

            if (values == null) {
                return NIL_REPLY;
            }

            this.metaVal = Unpooled.wrappedBuffer(values);

            return new BulkReply(key0);

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
    protected BloomFilterMeta getMeta() throws RedisException {

        try {
            byte[] value = db.get(getKey());
            if (value == null) this.metaVal = null;
            else
                this.metaVal = MyUtils.concat(value);
        } catch (RocksDBException e) {
            e.printStackTrace();
            throw new RedisException(e.getMessage());
        }

        return this;
    }


    /**
     * 创建BloomFilter
     *
     * @param expectedInsertions 预估数
     * @param fpp                误差率
     * @return
     * @throws RedisException
     */
    public StatusReply bfcreate(long expectedInsertions, double fpp) throws RedisException {


        //判断类型，非hash 类型返回异常信息；
        int type = getMeta().getType();

        if (type != -1 && type != DataType.KEY_BLOOMFILTER) {
            //抛出异常 类型不匹配
            throw invalidValue();
        }

        log.debug(getKey0Str());

        //初始化HLL
        BloomFilter<byte[]> filter = BloomFilter.create(Funnels.byteArrayFunnel(), expectedInsertions, fpp);

        try {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            filter.writeTo(output);
            db.put(getMetaKey(getKey0()), getMetaVal(output.toByteArray(), -1));
        } catch (IOException | RocksDBException e) {
            e.printStackTrace();
            Throwables.propagateIfPossible(e, RedisException.class);
        }

        return OK;
    }


    /**
     * Pfadd 命令将所有元素参数添加到 BloomFilter 数据结构中。
     *
     * 整型，如果至少有个元素被添加返回 1， 否则返回 0。
     *
     * @param args
     * @return
     * @throws RedisException
     */
    /**
     * @param members
     * @return
     * @throws RedisException
     */
    public IntegerReply bfadd(byte[][] members) throws RedisException {
        if (members.length == 0) {
            throw new RedisException("wrong number of arguments for BFADD");
        }

        //判断类型，非hash 类型返回异常信息；
        int type = getMeta().getType();

        if (type != -1 && type != DataType.KEY_BLOOMFILTER) {
            //抛出异常 类型不匹配
            throw invalidValue();
        }

        log.debug(getKey0Str());


        BloomFilter<byte[]> filter = null;
        try {
            filter = bloomFilterCache.get(Unpooled.wrappedBuffer(getKey0()));
        } catch (ExecutionException e) {
            e.printStackTrace();
            Throwables.propagateIfPossible(e, RedisException.class);
        }

        //赋值HLL
        for (int i = 0; i < members.length; i++) {
            filter.put(members[i]);
        }

        log.debug(String.format("BloomFitler实际数量:%s", filter.approximateElementCount()));


        return integer(members.length);
    }


    /**
     * bfexists 命令返回给定 filed 是否存在。
     *
     * @param field1
     * @return
     * @throws RedisException
     */
    public IntegerReply bfexists(byte[] field1) throws RedisException {


        BloomFilter<byte[]> filter = null;
        try {
            filter = bloomFilterCache.get(Unpooled.wrappedBuffer(getKey0()));
        } catch (ExecutionException e) {
            e.printStackTrace();
            Throwables.propagateIfPossible(e, RedisException.class);
        }

        boolean b = filter.mightContain(field1);
        return integer(b ? 1 : 0);
    }

    /**
     * 合并两个BloomFiler的数据
     *
     * @param key0
     * @return
     * @throws RedisException
     */
    public IntegerReply bfmerge(byte[]... key0) throws RedisException {
//        long count=0l;

        //合并
        try {
            BloomFilter<byte[]> filter = null;
            for (int i = 0; i < key0.length; i++) {
                if (i == 0) {
                    byte[] metaVal0 = getMetaVal0(Unpooled.wrappedBuffer(db.get(getMetaKey(getKey0()))));
                    ByteArrayInputStream input = new ByteArrayInputStream(metaVal0);
                    filter = BloomFilter.readFrom(input, Funnels.byteArrayFunnel());

                } else {
                    byte[] metaVal0 = getMetaVal0(Unpooled.wrappedBuffer(db.get(getMetaKey(getKey0()))));
                    ByteArrayInputStream input = new ByteArrayInputStream(metaVal0);
                    BloomFilter<byte[]> filter1 = BloomFilter.readFrom(input, Funnels.byteArrayFunnel());
                    filter.putAll(filter1);
                }
            }

            //持久化
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            filter.writeTo(output);

            db.put(getMetaKey(getKey0()), getMetaVal(output.toByteArray(), -1));

        } catch (IOException | RocksDBException e) {
            e.printStackTrace();
            Throwables.propagateIfPossible(e, RedisException.class);
        }

        //当前key缓存失效
        bloomFilterCache.invalidate(getMetaKey(key0[0]));

        return integer(key0.length);
    }

    /**
     * 批量删除主键(0-9.A-Z,a-z)；
     * 根据genkey 特征，增加风格符号，避免误删除数据；
     *
     * @param key0
     * @throws RedisException
     */
    public void deleteRange(byte[] key0) throws RedisException {

        ByteBuf byteBufBegin = MyUtils.concat(NS, DataType.SPLIT, key0, DataType.SPLIT);
        ByteBuf byteBufEnd = MyUtils.concat(NS, DataType.SPLIT, key0, DataType.SPLIT, "z".getBytes());

        byte[] begin = byteBufBegin.readBytes(byteBufBegin.readableBytes()).array();
        byte[] end = byteBufEnd.readBytes(byteBufEnd.readableBytes()).array();

        log.debug(String.format("begin %s -> end %s", new String(begin), new String(end)));

        try {
            db.deleteRange(begin, end);
        } catch (RocksDBException e) {
            e.printStackTrace();
            throw new RedisException(e.getMessage());
        }
    }


    // 缓存接口这里是LoadingCache，LoadingCache在缓存项不存在时可以自动加载缓存
    // fixme key 不支持byte[]
    static LoadingCache<ByteBuf, BloomFilter<byte[]>> bloomFilterCache
            // CacheBuilder的构造函数是私有的，只能通过其静态方法newBuilder()来获得CacheBuilder的实例
            = CacheBuilder.newBuilder()
            // 设置并发级别为8，并发级别是指可以同时写缓存的线程数
            .concurrencyLevel(8)
            // 设置写缓存后8秒钟过期
            .expireAfterWrite(120, TimeUnit.SECONDS)
            // 设置缓存容器的初始容量为10
            .initialCapacity(10)
//            .maximumWeight(10*1024*1024)
            // 设置缓存最大容量为100，超过100之后就会按照LRU最近虽少使用算法来移除缓存项
            .maximumSize(100)
            // 设置要统计缓存的命中率
            .recordStats()
            // 设置缓存的移除通知 ,将数据持久化到RocksDb
            .removalListener(new RemovalListener<ByteBuf, BloomFilter<byte[]>>() {
                public void onRemoval(RemovalNotification<ByteBuf, BloomFilter<byte[]>> notification) {
                    log.debug(String.format("因为%s的原因，%s=%s已经删除",
                            notification.getCause(),
                            MyUtils.ByteBuf2String(notification.getKey()),
                            notification.getValue().expectedFpp()));

                    try {
                        ByteArrayOutputStream output = new ByteArrayOutputStream();
                        {
                            BloomFilter<byte[]> filter = notification.getValue();
                            filter.writeTo(output);
                            output.flush();

                            log.debug(String.format("获取bloomFilter(误差率%s) 列表，持久化到 RocksDb。\n key:%s 数量%s 初始验证：%s  修改验证：%s",
                                    filter.expectedFpp(),
                                    MyUtils.ByteBuf2String(notification.getKey()),
                                    filter.approximateElementCount(),
                                    filter.mightContain("test_one".getBytes()),
                                    filter.mightContain("test_modify".getBytes())
                            ));

                        }

                        //持久化数据
                        db.put(getMetaKey(notification.getKey().array()), getMetaVal(output.toByteArray(), -1));

                    } catch (RocksDBException | IOException e) {
                        e.printStackTrace();
//                        Throwables.propagateIfPossible(e, RedisException.class);
                    }

                }
            })

            // build方法中可以指定CacheLoader，在缓存不存在时通过CacheLoader的实现自动加载缓存
            // 从RocksDb中加载数据
            .build(new CacheLoader<ByteBuf, BloomFilter<byte[]>>() {
                @Override
                public BloomFilter<byte[]> load(ByteBuf key0) throws Exception {

                    log.debug(MyUtils.ByteBuf2String(key0));

                    byte[] value = db.get(getMetaKey(key0.array()));

                    //解析得到业务数据
                    byte[] metaVal0 = getMetaVal0(MyUtils.concat(value));

                    ByteArrayInputStream input = new ByteArrayInputStream(metaVal0);
                    BloomFilter<byte[]> filter = BloomFilter.readFrom(input, Funnels.byteArrayFunnel());

                    log.debug(String.format("加载BloomFilter(误差率%s)从RocksDb: key %s  数量%s  初始验证：%s 修改验证：%s",
                            filter.expectedFpp(),
                            MyUtils.ByteBuf2String(key0),
                            filter.approximateElementCount(),
                            filter.mightContain("test_one".getBytes()),
                            filter.mightContain("test_modify".getBytes())
                    ));
                    return filter;
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
