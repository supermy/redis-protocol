package redis.server.netty.rocksdb;

import com.google.common.base.Throwables;
import com.google.common.cache.*;
import com.google.common.collect.BiMap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.supermy.util.MyUtils;
import org.xerial.snappy.Snappy;
import redis.netty4.*;
import redis.server.netty.RedisException;
import redis.server.netty.utis.DataType;
import redis.server.netty.utis.DbUtils;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static redis.netty4.BulkReply.NIL_REPLY;
import static redis.netty4.IntegerReply.integer;
import static redis.netty4.StatusReply.OK;
import static redis.server.netty.rocksdb.RedisBase.invalidValue;
import static redis.server.netty.rocksdb.RedisBase.notInteger;
import static redis.server.netty.rocksdb.RocksdbRedis._toposint;
import static redis.util.Encoding.bytesToNum;
import static redis.util.Encoding.numToBytes;


/**
 * String Meta 元素方便促常用操作
 * 链式使用bean
 * <p>
 * String set/del 可以覆盖其他类型，Hash/Set/List/ZSet类型不可以转换覆盖
 * <p>
 * String      [<ns>] <key> KEY_META                 KEY_STRING <MetaObject>
 * <p>
 * <p>
 * key and value 都采用 | 分隔符号
 * * getKey 一般是包含组合键；
 * * getkey0 是纯粹的业务主键；
 * * 参见setVal0 long+int+int ttl,数据类型,数据长度；
 * * val 一般是包含ttl 的数据；val0是实际的业务数据
 * <p>
 * Created by moyong on 2017/11/29.
 * Update by moyong 2018/09/18.
 * method: set get getrange getset mget setex setnx setrange strlen mset msetnx  psetex  incr incrby incrbyfloat decr decrby append
 * <p>
 * todo 使用bitfield实现getbits和setbits
 */
public class StringMeta extends BaseMeta {

    private static Logger log = Logger.getLogger(StringMeta.class);


    ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();

//    private static RocksDB db;

//    private static byte[] NS;
//    private static byte[] TYPE = DataType.KEY_META;

//    private ByteBuf metaKey;
//    private ByteBuf metaVal;


    private StringMeta() {
    }

    private static StringMeta instance = new StringMeta();

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
    public static StringMeta getInstance(RocksDB db0, byte[] ns0) {
        instance.db = db0;
        instance.NS = ns0;
        instance.VAlTYPE = DataType.KEY_STRING;
        return instance;
    }

    /**
     * 构造 MetaKey
     *
     * @param key0
     * @return
     * @throws RedisException
     */
    @Deprecated
    public StringMeta genMetaKey(byte[] key0) throws RedisException {
        if (key0 == null) {
            throw new RedisException(String.format("主键不能为空"));
        }
        instance.metaKey = MyUtils.concat(instance.NS, DataType.SPLIT, key0, DataType.SPLIT, KEYTYPE);
        return instance;
    }


//    private byte[] getKey0() {
//        metaKey.resetReaderIndex();
//        ByteBuf bb = metaKey.slice(NS.length + DataType.SPLIT.length, metaKey.readableBytes() - 8);
//        return bb.readBytes(bb.readableBytes()).array();
//    }

//    private String getKey0Str() {
//        return new String(getKey0());
//    }

//    private byte[] getKey() {
//        metaKey.resetReaderIndex();
//        return metaKey.readBytes(metaKey.readableBytes()).array();
//    }

//    public void setKey0(byte[] key0)  {
//        metaKey.resetReaderIndex();
//        this.metaKey = Unpooled.wrappedBuffer(NS, key0, KEYTYPE);
//    }


    private byte[] getVal() {
        if (metaVal == null) {
            return null;
        } else {
            metaVal.resetReaderIndex();
            return metaVal.readBytes(metaVal.readableBytes()).array();
        }
    }


    /**
     * 返回业务数据
     *
     * @return
     * @throws RedisException
     */
    private byte[] getVal0() throws RedisException {
        metaVal.resetReaderIndex();
        ByteBuf valueBuf = metaVal.slice(8 + 4 + 8 + 3, metaVal.readableBytes() - 8 - 4 - 8 - 3);
        if (valueBuf.readableBytes() == 0) {
            return null;
        }
        return MyUtils.toByteArray(valueBuf);
    }


    private String getVal0Str() throws RedisException {
        return new String(getVal0());
    }

    @Deprecated
    protected StringMeta genVal(byte[] value, long expiration) {

        ByteBuf ttlBuf = Unpooled.buffer(12);
        ttlBuf.writeLong(expiration); //ttl 无限期 -1
        ttlBuf.writeBytes(DataType.SPLIT);

        ttlBuf.writeInt(DataType.KEY_STRING); //value type
        ttlBuf.writeBytes(DataType.SPLIT);

        ttlBuf.writeLong(value.length); //value size
        ttlBuf.writeBytes(DataType.SPLIT);

        ttlBuf.writeBytes(value);

//        ByteBuf valueBuf = Unpooled.wrappedBuffer(value); //零拷贝
//        this.metaVal = MyUtils.concat(ttlBuf, valueBuf);//零拷贝

        metaVal = ttlBuf;

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

//            vvBuf.resetReaderIndex();

//            ByteBuf ttlBuf = vvBuf.readSlice(8);
//            ByteBuf typeBuf = vvBuf.slice(8 + 1, 4);
//            ByteBuf sizeBuf = vvBuf.slice(8 + 1 + 4 + 1, 4);
//            ByteBuf sizeBuf = vvBuf.slice(8 + 1 + 4 + 1, 8); //size Long

            log.debug(vvBuf.readableBytes());
            log.debug(MyUtils.ByteBuf2String(vvBuf));
            ByteBuf valueBuf = vvBuf.slice(8 + 4 + 8 + 3, values.length - 8 - 4 - 8 - 3);

//            long ttl = ttlBuf.readLong();//ttl
//            long size = sizeBuf.readInt();//长度数据
//            long size = sizeBuf.readLong();//长度数据

//            //数据过期处理,对应的数据返回null；
//            if (ttl < now() && ttl != -1) {
//                try {
//                    db.delete(key0);
//                } catch (RocksDBException e) {
//                    e.printStackTrace();
//                    throw new RedisException(e.getMessage());
//                }
//                return null;
//            }

            return valueBuf.readBytes(valueBuf.readableBytes()).array();

        } else return null; //数据不存在 ？ 测试验证
    }


    public long getTtl() {
        return metaVal.getLong(0);
    }

    public int getSize() {
        return metaVal.getInt(8 + 4 + 2);
    }


//    private long now() {
//        return System.currentTimeMillis();
//    }


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
     * 删除数据，支持stinrg/hash/list/set/zset 类型
     *
     * @return
     * @throws RedisException
     */
    public StringMeta del() throws RedisException {
//        try {

        cleanBy(genKeyPartten());

//        } catch (RocksDBException e) {
//            throw new RedisException(e.getMessage());
//        }
        return this;
    }

    /**
     * 主键索赔员类型
     *
     * @return
     */
    private byte[] genKeyPartten() throws RedisException {
        log.debug(MyUtils.ByteBuf2String(metaKey));

        ByteBuf bb = metaKey.slice(NS.length + 1, metaKey.readableBytes() - NS.length - DataType.SPLIT.length * 2 - KEYTYPE.length);

        log.debug(MyUtils.ByteBuf2String(bb));

        try {
            ByteBuf byteBuf = MyUtils.concat(NS, DataType.SPLIT, getKey0(), DataType.SPLIT);
            return MyUtils.toByteArray(byteBuf);
        } catch (RedisException e) {
            e.printStackTrace();
            Throwables.propagateIfPossible(e, RedisException.class);
        }
        return null;
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
     * @param keyPartten0
     * @throws RedisException
     */
    public void cleanBy(byte[] keyPartten0) throws RedisException {

        cleanBy(db, keyPartten0);
    }

    /**
     * 清理各种类型的数据keyPattern0（不含类型）
     *
     * @param db9
     * @param key0
     * @throws RedisException
     */
    private void cleanBy(RocksDB db9, byte[] key0) throws RedisException {
        ByteBuf byteBufBegin = MyUtils.concat(key0);
        ByteBuf byteBufEnd = MyUtils.concat(key0, "z".getBytes());

        try {
            db9.deleteRange(MyUtils.toByteArray(byteBufBegin), MyUtils.toByteArray(byteBufEnd));
        } catch (RocksDBException e) {
            e.printStackTrace();
            throw new RedisException(e.getMessage());
        }

    }

    /**
     * Redis SET 命令用于设置给定 key 的值。如果 key 已经存储其他值， SET 就覆写旧值，且无视类型。
     * <p>
     * 如果字段是哈希表中的一个新建字段，并且值设置成功，返回 1 。 如果哈希表中域字段已经存在且旧值已被新值覆盖，返回 0 。
     *
     * @param key0
     * @param val1
     * @return
     * @throws RedisException
     */
    public StatusReply set(byte[] key0, byte[] val1, byte[] seconds2) throws RedisException {
//        if (checkTypeAndTTL(key0, DataType.KEY_STRING)) return QUIT; //fixme 无视类型/无视ttl 覆盖

        long ttl = 0;
        if (seconds2 == null) {
            ttl = -1; //ttl 无限期 -1

        } else {
            ttl = RocksdbRedis.bytesToLong(seconds2);//fixme 重构
        }



        try {

            //从缓存获取数据
            metaVal = metaCache.get(Unpooled.wrappedBuffer(key0));

//        log.debug(MyUtils.ByteBuf2String(metaVal));
//        log.debug(new String(val1));

            metaVal.setLong(0, ttl);//ttl
            metaVal.setLong(8 + 4 + 2, val1.length);//size

            //设定读写指针
            metaVal.setIndex(0, 8 + 4 + 8 + 3);
            metaVal.writeBytes(val1);

            ByteBuf actual = metaCache.get(Unpooled.wrappedBuffer(key0));

////            actual.resetWriterIndex();
////            actual.resetReaderIndex();
            log.debug(MyUtils.ByteBuf2String(metaVal));
            log.debug(MyUtils.ByteBuf2String(actual));

            Assert.assertEquals(metaVal, actual);

        } catch (ExecutionException e) {
            e.printStackTrace();
            Throwables.propagateIfPossible(e,RedisException.class);
        }

        //少进行一次读取操作；进行key 的孤独元素处理
        //todo 异步队列删除 Hash Set SortSet List 的 meta数据与element 数据
        //todo 先使用异步线程，后续使用异步队列替换；
//        singleThreadExecutor.execute(() -> {
//            try {
//                //清除hash 类型元数据
//                cleanBy(genKeyPartten(DataType.KEY_HASH_FIELD));
//                //liset,set,sortset
//                cleanBy(genKeyPartten(DataType.KEY_LIST_ELEMENT));
//                cleanBy(genKeyPartten(DataType.KEY_SET_MEMBER));
//                cleanBy(genKeyPartten(DataType.KEY_ZSET_SCORE));
//                cleanBy(genKeyPartten(DataType.KEY_ZSET_SORT));
//            } catch (RedisException e) {
//                e.printStackTrace();
//            }
//        });

//        metaCache.invalidate(key0);//fixme 缓存不能同步问题处理
//        metaCache.put(Unpooled.wrappedBuffer(key0),metaVal);//缓存不能同步问题处理

        //清除类型数据，如果覆盖的话；  fixme
        singleThreadExecutor.execute(new MetaCleanCaller(db,
                genKeyPartten(DataType.KEY_HASH_FIELD),
                genKeyPartten(DataType.KEY_LIST_ELEMENT),
                genKeyPartten(DataType.KEY_SET_MEMBER),
                genKeyPartten(DataType.KEY_ZSET_SCORE),
                genKeyPartten(DataType.KEY_ZSET_SORT)));


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
                cleanBy(db0, keyPartten1);
                cleanBy(db0, keyPartten2);
                cleanBy(db0, keyPartten3);
                cleanBy(db0, keyPartten4);
                cleanBy(db0, keyPartten5);
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
        if (checkTypeAndTTL(key0, DataType.KEY_STRING)) return NIL_REPLY;

        byte[] values = getVal0();

        if (values == null) {
            return NIL_REPLY;
        }

        return new BulkReply(values);

//        genMetaKey(key0);
//
//        try {
//
//            if (metaKey == null) {
//                throw new RedisException(String.format("主键不能为空"));
//            }
//
//            byte[] values = db.get(getKey());
//
//            if (values == null) {
//                return NIL_REPLY;
//            }
//
//            this.metaVal = Unpooled.wrappedBuffer(values);
//
//            return new BulkReply(getVal0());
//
//        } catch (RocksDBException e) {
//            e.printStackTrace();
//            throw new RedisException(e.getMessage());
//        }

    }


    public BulkReply getRange(byte[] key0, byte[] start1, byte[] end2) throws RedisException {
//        if (checkTypeAndTTL(key0, DataType.KEY_STRING)) return NIL_REPLY;

//        genMetaKey(key0);

        BulkReply bulkReply = get(key0);

        if (bulkReply.isEmpty()) {
            return NIL_REPLY;
        }

        //从开始到结束，二次进行截取；

        byte[] bytes = bulkReply.data().array();

        int size = bytes.length;
        int s = RocksdbRedis.__torange(start1, size);
        int e = RocksdbRedis.__torange(end2, size);
        if (e < s) e = s;
        int length = e - s + 1;

        ByteBuf slice = Unpooled.wrappedBuffer(bytes).slice(s, length);

        return new BulkReply(MyUtils.toByteArray(slice));

    }

    /**
     * 将给定 key 的值设为 value ，并返回 key 的旧值(old value)。
     *
     * @param key0
     * @param value1
     * @return
     * @throws RedisException
     */
    public BulkReply getset(byte[] key0, byte[] value1) throws RedisException {

//        genMetaKey(key0);

        BulkReply put = get(key0);
        set(key0, value1, null);
        if (put.isEmpty() || put.data().array() instanceof byte[]) {
            return put == null ? NIL_REPLY : new BulkReply(put.data().array());
        } else {
            throw invalidValue();
        }
    }


    /**
     * 获取所有(一个或多个)给定 key 的值;
     * key不存在或者类型不为string都返回nil;
     * 按key list的次序返回value;
     *
     * @param keys
     * @return
     * @throws RedisException
     */
    public MultiBulkReply mget(byte[]... keys) throws RedisException {

//        if (checkTypeAndTTL(key0, DataType.KEY_STRING)) return MultiBulkReply.EMPTY;
        return mget(Arrays.asList(keys));
    }


    public MultiBulkReply mget(List<byte[]> keys) throws RedisException {

//        if (checkTypeAndTTL(key0, DataType.KEY_STRING)) return MultiBulkReply.EMPTY;

        List<BulkReply> result = new ArrayList<BulkReply>();


        //1.先从db获取最新数据；
        List<byte[]> keysDb = keys;
        keysDb.removeAll(metaCache.asMap().keySet());
        BiMap<ByteBuf, ByteBuf> dbValues = metaMGet(keysDb);


        //2.封装返回结果
        try {
            for (byte[] k : keys
            ) {

                //2.1先从DB获取数据
                ByteBuf val = dbValues.get(Unpooled.wrappedBuffer(k));
                if (val == null || val.readableBytes() == 0) {
                    //2.2再从缓存获取数据 //如果不存在会产生空数据，确保不产生空数据
                    val = metaCache.get(Unpooled.wrappedBuffer(k));
                }

                if (val != null) {
                    byte[] val0 = getVal0(val);
                    if (val0==null){
                        result.add(NIL_REPLY);
                    }else
                    result.add(new BulkReply(val0));
                    //2.3都不存在返回nil
                } else result.add(NIL_REPLY);


            }

        } catch (ExecutionException e) {
            e.printStackTrace();
            Throwables.propagateIfPossible(e, RedisException.class);

        }

        return new MultiBulkReply(result.toArray(new BulkReply[result.size()]));

    }

    /**
     * guava 加载与清除都支持批量处理？？？
     *
     * @param listFds
     * @return
     * @throws RedisException
     */
    @Deprecated
    protected List<BulkReply> __mget(List<byte[]> listFds) throws RedisException {
        List<BulkReply> list = new ArrayList<BulkReply>();

        try {
            Map<byte[], byte[]> fvals = this.db.multiGet(listFds);
            for (byte[] fk : listFds
            ) {
                byte[] val = parseValue(fk, fvals.get(fk));
                if (val != null) {
                    list.add(new BulkReply(val));
                } else list.add(NIL_REPLY);

            }

        } catch (RocksDBException e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
        return list;
    }


    public StatusReply mset(byte[]... field_or_value1) throws RedisException {


        if (field_or_value1.length % 2 != 0) {
            throw new RedisException("wrong number of arguments for HMSET");
        }

        __mput(field_or_value1);

        return OK;
    }

    /**
     * 批量数据存储处理
     *
     * @param field_or_value1
     */
    protected void __mput(byte[][] field_or_value1) throws RedisException {
        log.debug("__mput......begin");

        final WriteOptions writeOpt = new WriteOptions();
        final WriteBatch batch = new WriteBatch();

        try {

            for (int i = 0; i < field_or_value1.length; i += 2) {
                //封装metakey
//                field_or_value1[i] = getMetaKey2Byte(field_or_value1[i]);
                byte[] key0 = getMetaKey2Byte(field_or_value1[i]);

//            }

                //封装metaVal
//            for (int i = 0; i < field_or_value1.length; i += 2) {
//            byte[] val = genDataVal(field_or_value1[i + 1], -1).getVal();
                byte[] val0 = getMetaVal(field_or_value1[i + 1], -1);
//                batch.put(field_or_value1[i], val);
                batch.put(key0, val0);

                metaCache.invalidate(key0);

            }


            db.write(writeOpt, batch);

        } catch (RocksDBException e) {
//            throw new RuntimeException(e.getMessage());
            Throwables.propagateIfPossible(e, RedisException.class);
        }

        log.debug("__mput......end");

    }


    public IntegerReply strlen(byte[] key0) throws RedisException {
        BulkReply bulkReply = get(key0);//todo

        return integer(bulkReply.data().array().length);
    }

//5.67/3.5


    /**
     * 命令用于所有给定 key 都不存在时，同时设置一个或多个 key-value 对
     *
     * @param key_or_value0
     * @return
     * @throws RedisException
     */
    public IntegerReply msetnx(byte[]... key_or_value0) throws RedisException {
        log.debug("msetnx......begin");

        int length = key_or_value0.length;
        if (length % 2 != 0) {
            throw new RedisException("wrong number of arguments for MSETNX");
        }


        List<byte[]> keys = new ArrayList<byte[]>();
        //处理 string 的主键
        for (int i = 0; i < length; i += 2) {
//            key_or_value0[i] = getMetaKey2Byte(key_or_value0[i]);
//            keys.add(key_or_value0[i]);
            keys.add(key_or_value0[i]);

        }

        //有一个不存在,返回错误
//        List<BulkReply> bulkReplies = __mget(keys);

        MultiBulkReply bulkReplies = mget(keys);

        for (String str : bulkReplies.asStringList(Charset.defaultCharset())) {

            if (str != null) {
                return integer(0);
            }
        }

        __mput(key_or_value0);

        log.debug("msetnx......end");

        return integer(1);
    }


    public Reply psetex(byte[] key0, byte[] milliseconds1, byte[] value2) throws RedisException {

        long l = bytesToNum(milliseconds1) + now();
        return setex(key0, (l + "").getBytes(), value2);

    }

    public IntegerReply incr(byte[] key0) throws RedisException {
        return incrby(key0, "1".getBytes());
    }


    public IntegerReply incrby(byte[] key0, byte[] increment2) throws RedisException {
//        genKey(key0);

        long incr = bytesToNum(increment2);
        BulkReply field = get(key0);

//        log.debug("val:"+field.asAsciiString());


        if (field.data() == null) {
            set(key0, increment2, null);
            return new IntegerReply(incr);
        } else {

            String val = new String(field.dataByte());


            try {
                long value = Long.parseLong(val);

//                String fld = field.asAsciiString();//fixme
//                long value = Long.parseLong(val);
//            long value =             field.data().readLong();

                value = value + incr;

                set(key0, (value + "").getBytes(), null);

                metaCache.invalidate(key0);

                return new IntegerReply(value);

            } catch (NumberFormatException e) {
                throw notInteger();
//                System.out.println("异常：\"" + str + "\"不是数字/整数...");
            }

//
        }

    }

    public IntegerReply decr(byte[] key0) throws RedisException {
//        genKey(key0);

        return incrby(key0, "-1".getBytes());
    }

    public IntegerReply decrby(byte[] key0, byte[] decrement1) throws RedisException {

        long l = -bytesToNum(decrement1);

        return incrby(key0, numToBytes(l));
    }


    public IntegerReply append(byte[] key0, byte[] value1) throws RedisException {
        BulkReply src = get(key0);
        ByteBuf targetBuf = MyUtils.concat(src.data().array(), value1);

        byte[] obj = targetBuf.readBytes(targetBuf.readableBytes()).array();
        set(key0, obj, null);
        return integer(obj.length);
    }

//
//    public IntegerReply del(byte[]... keys) throws RedisException {
//
//        for (byte[] key : keys) {
//            try {
//                RocksdbRedis.mydata.del(HashNode.genKey(getKey0(),hkey));
//            } catch (RocksDBException e) {
//                e.printStackTrace();
//                throw new RedisException(e.getMessage());
//            }
//        }
//        return integer(field1.length);
//    }


    /**
     * 将值 value 关联到 key ，并将 key 的过期时间设为 seconds (以秒为单位)。
     *
     * @param key0
     * @param seconds1
     * @param value2
     * @return
     * @throws RedisException
     */
    public StatusReply setex(byte[] key0, byte[] seconds1, byte[] value2) throws RedisException {

        return set(key0, value2, seconds1);
    }

    public IntegerReply setnx(byte[] key0, byte[] value1) throws RedisException {

        BulkReply bulkReply = get(key0);

        if (bulkReply.data() == null) {
            set(key0, value1, null);
            return integer(1);
        } else {
            return integer(0);

        }

    }

    /**
     * 用 value 参数覆写给定 key 所储存的字符串值，从偏移量 offset 开始。
     *
     * @param key0
     * @param offset1
     * @param value2
     * @return
     * @throws RedisException
     */
    public IntegerReply setrange(byte[] key0, byte[] offset1, byte[] value2) throws RedisException {

        byte[] data = get(key0).data().array();
        ByteBuf buf = get(key0).data();

        long sec = bytesToNum(offset1);

        int offset = _toposint(offset1) < data.length ? _toposint(offset1) : data.length;


//        buf.writeBytes(value2, offset, value2.length);
        buf.setIndex(0, offset);
        buf.writeBytes(value2);

        byte[] array = buf.readBytes(buf.readableBytes()).array();

        set(key0, array, null);

        return integer(array.length);

    }

    /**
     * SETBIT key offset value
     * <p>
     * 对 key 所储存的字符串值，设置或清除指定偏移量上的位(bit)。
     * <p>
     * 位的设置或清除取决于 value 参数，可以是 0 也可以是 1 。
     * <p>
     * 当 key 不存在时，自动生成一个新的字符串值。
     * <p>
     * 字符串会进行伸展(grown)以确保它可以将 value 保存在指定的偏移量上。当字符串值进行伸展时，空白位置以 0 填充。
     * <p>
     * offset 参数必须大于或等于 0 ，小于 2^32 (bit 映射被限制在 512 MB 之内)。
     * <p>
     * 指定偏移量原来储存的位。
     * redis> SETBIT bit 10086 1
     * redis> GETBIT bit 10086
     * redis> GETBIT bit 100   # bit 默认被初始化为 0
     * <p>
     * 返回值：
     * 字符串值指定偏移量上原来储存的位(bit)。
     *
     * @param key0
     * @param offset1
     * @param value2
     * @return
     * @throws RedisException
     */
    public IntegerReply setbit(byte[] key0, byte[] offset1, byte[] value2) throws RedisException {
        //判断类型是否String
        if (checkTypeAndTTL(getKey0(), DataType.KEY_STRING)) return integer(0);


        //todo 可以支持一批数据，以提升效率
        int bit = (int) bytesToNum(value2);
        if (bit != 0 && bit != 1) throw notInteger();

//        long offset = bytesToNum(offset1);
//        long offset= Longs.fromByteArray(offset1);//fixme 是否与Redis协议一致
        long offset = Long.parseLong(new String(offset1));

//        long div = offset / 8;
//        if (div + 1 > MAX_VALUE) throw notInteger();

        Roaring64NavigableMap r64nm = null;
        try {
            r64nm = bitCache.get(MyUtils.concat(key0, "bit".getBytes()));//todo key0+"bits" 产生的数据需要del时候考虑删除。
        } catch (ExecutionException e) {
            e.printStackTrace();
            Throwables.propagateIfPossible(e, RedisException.class);
        }

        //返回值：
        //     * 字符串值指定偏移量上原来储存的位(bit)。
        boolean result = r64nm.contains(offset);

        //1设置，0清除
        if (bit == 1) {
            r64nm.addLong(offset);
        } else {
            r64nm.removeLong(offset);
        }

//        log.debug(r64nm.contains(offset));
//        try {
//            log.debug(bitCache.get(Unpooled.wrappedBuffer(key0)).contains(offset));//fixme
//        } catch (ExecutionException e) {
//            e.printStackTrace();
//        }

//        bitCache.put(Unpooled.wrappedBuffer(key0),r64nm);

        return result ? integer(1) : integer(0);

//        //        Object o = _get(key0);
////        if (o instanceof byte[] || o == null) {
//            long offset = bytesToNum(offset1);
//            long div = offset / 8;
//            if (div + 1 > MAX_VALUE) throw notInteger();
////
////            byte[] bytes = (byte[]) o;
////            if (bytes == null || bytes.length < div + 1) {
////                byte[] tmp = bytes;
////                bytes = new byte[(int) div + 1];
////                if (tmp != null) System.arraycopy(tmp, 0, bytes, 0, tmp.length);
////                _put(key0, bytes);
////            }
////            int mod = (int) (offset % 8);
////            int value = bytes[((int) div)] & 0xFF;
////            int i = value & mask[mod];
////            if (i == 0) {
////                if (bit != 0) {
////                    bytes[((int) div)] += mask[mod];
////                }
////                return integer(0);
////            } else {
////                if (bit == 0) {
////                    bytes[((int) div)] -= mask[mod];
////                }
////                return integer(1);
////            }
////        } else {
////            throw invalidValue();
////        }
//        return null;
    }

    /**
     * GETBIT key offset
     * 对 key 所储存的字符串值，获取指定偏移量上的位(bit)。
     * 当 offset 比字符串值的长度大，或者 key 不存在时，返回 0 。
     * <p>
     * 字符串值指定偏移量上的位(bit)。
     * # 对不存在的 key 或者不存在的 offset 进行 GETBIT， 返回 0
     * <p>
     * <p>
     * redis> EXISTS bit
     * redis> GETBIT bit 10086*
     * <p>
     * # 对已存在的 offset 进行 GETBIT
     * <p>
     * redis> SETBIT bit 10086 1
     * redis> GETBIT bit 10086
     * <p>
     * <p>
     * 当 offset 比字符串值的长度大，或者 key 不存在时，返回 0 。
     * 返回值：
     * 字符串值指定偏移量上的位(bit)。
     *
     * @param key0
     * @param offset1
     * @return
     * @throws RedisException
     */
    public IntegerReply getbit(byte[] key0, byte[] offset1) throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_STRING)) return integer(0);

        long offset = bytesToNum(offset1);

        Roaring64NavigableMap r64nm = null;
        try {
            r64nm = bitCache.get(MyUtils.concat(key0, "bit".getBytes()));
        } catch (ExecutionException e) {
            e.printStackTrace();
            Throwables.propagateIfPossible(e, RedisException.class);
        }
        boolean result = r64nm.contains(offset);

//        Object o = _get(key0);
//        if (o instanceof byte[]) {
//            long offset = bytesToNum(offset1);
//            byte[] bytes = (byte[]) o;
//            return _test(bytes, offset) == 1 ? integer(1) : integer(0);
//        } else if (o == null) {
//            return integer(0);
//        } else {
//            throw invalidValue();
//        }
        return result ? integer(1) : integer(0);
    }

    /**
     * 如果你的 bitmap 数据非常大，那么可以考虑使用以下两种方法：
     * <p>
     * 将一个大的 bitmap 分散到不同的 key 中，作为小的 bitmap 来处理。使用 Lua 脚本可以很方便地完成这一工作。
     * 使用 BITCOUNT 的 start 和 end 参数，每次只对所需的部分位进行计算，将位的累积工作(accumulating)放到客户端进行，并且对结果进行缓存 (caching)。
     * <p>
     * <p>
     * BITCOUNT key [start] [end]
     * 计算给定字符串中，被设置为 1 的比特位的数量。
     * 一般情况下，给定的整个字符串都会被进行计数，通过指定额外的 start 或 end 参数，可以让计数只在特定的位上进行。
     * start 和 end 参数的设置和 GETRANGE 命令类似，都可以使用负数值： 比如 -1 表示最后一个字节， -2 表示倒数第二个字节，以此类推。
     * 不存在的 key 被当成是空字符串来处理，因此对一个不存在的 key 进行 BITCOUNT 操作，结果为 0 。
     *
     * @param key0
     * @param start1
     * @param end2
     * @return
     * @throws RedisException
     */
    public IntegerReply bitcount(byte[] key0, byte[] start1, byte[] end2) throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_STRING)) return integer(0);

        if (start1 != null || end2 != null) {
            long s1 = bytesToNum(start1);
            long e1 = bytesToNum(end2);
        }

        Roaring64NavigableMap r64nm = null;
        try {
            r64nm = bitCache.get(MyUtils.concat(key0, "bit".getBytes()));
//            r64nm.rankLong(1);
        } catch (ExecutionException e) {
            e.printStackTrace();
            Throwables.propagateIfPossible(e, RedisException.class);
        }
//        boolean result = r64nm.contains(offset);
        long longCardinality = r64nm.getLongCardinality();

//        r64nm.limit(1l);
//        RoaringBitmap rb=null;
//        rb.limit();

        return integer(longCardinality);

//        Object o = _get(key0);
//        if (o instanceof byte[]) {
//            byte[] bytes = (byte[]) o;
//            int size = bytes.length;
//            int s = _torange(start1, size);
//            int e = _torange(end2, size);
//            if (e < s) e = s;
//            int total = 0;
//            for (int i = s; i <= e; i++) {
//                int b = bytes[i] & 0xFF;
//                for (int j = 0; j < 8; j++) {
//                    if ((b & mask[j]) != 0) {
//                        total++;
//                    }
//                }
//            }
//            return integer(total);
//        } else if (o == null) {
//            return integer(0);
//        } else {
//            throw invalidValue();
//        }
    }

    /**
     * BITOP operation destkey key [key ...]
     * <p>
     * 对一个或多个保存二进制位的字符串 key 进行位元操作，并将结果保存到 destkey 上。
     * <p>
     * operation 可以是 AND 、 OR 、 NOT 、 XOR 这四种操作中的任意一种：
     * <p>
     * BITOP AND destkey key [key ...] ，对一个或多个 key 求逻辑并，并将结果保存到 destkey 。
     * BITOP OR destkey key [key ...] ，对一个或多个 key 求逻辑或，并将结果保存到 destkey 。
     * BITOP XOR destkey key [key ...] ，对一个或多个 key 求逻辑异或，并将结果保存到 destkey 。
     * BITOP NOT destkey key ，对给定 key 求逻辑非，并将结果保存到 destkey 。
     * 除了 NOT 操作之外，其他操作都可以接受一个或多个 key 作为输入。
     * <p>
     * 处理不同长度的字符串
     * <p>
     * 当 BITOP 处理不同长度的字符串时，较短的那个字符串所缺少的部分会被看作 0 。
     * <p>
     * 空的 key 也被看作是包含 0 的字符串序列。
     * <p>
     * >>>保存到 destkey 的字符串的长度，和输入 key 中最长的字符串长度相等。
     *
     * @param operation0
     * @param destkey1
     * @param key2
     * @return
     * @throws RedisException
     */
    public IntegerReply bitop(byte[] operation0, byte[] destkey1, byte[]... key2) throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_STRING)) return integer(0);

        Roaring64NavigableMap r64nm = null;
        try {
            r64nm = bitCache.get(MyUtils.concat(destkey1, "bit".getBytes()));
        } catch (ExecutionException e) {
            e.printStackTrace();
            Throwables.propagateIfPossible(e, RedisException.class);
        }

        BitOp bitOp = BitOp.valueOf(new String(operation0).toUpperCase());

//        int size = 0;
//        for (byte[] aKey2 : key2) {
//            int length = aKey2.length;
//            if (length > size) {
//                size = length;
//            }
//        }

//        byte[] bytes = null;
        for (byte[] aKey2 : key2) {
//            byte[] src;
//            src = _getbytes(aKey2);

            Roaring64NavigableMap src = null;
            try {
                src = bitCache.get(MyUtils.concat(aKey2, "bit".getBytes()));
            } catch (ExecutionException e) {
                e.printStackTrace();
                Throwables.propagateIfPossible(e, RedisException.class);
            }

            switch (bitOp) {
                case AND:
                    r64nm.and(src);
                    break;
                case OR:
                    r64nm.or(src);
                    break;
                case XOR:
                    r64nm.xor(src);
                    break;

                case NOT:
                    r64nm.andNot(src);
                    break;
            }
//
//            if (bytes == null) {
//                bytes = new byte[size];
//                if (bitOp == BitOp.NOT) {
//                    if (key2.length > 1) {
//                        throw new RedisException("invalid number of arguments for 'bitop' NOT operation");
//                    }
//                    for (int i = 0; i < src.length; i++) {
//                        bytes[i] = (byte) ~(src[i] & 0xFF);
//                    }
//                } else {
//                    System.arraycopy(src, 0, bytes, 0, src.length);
//                }
//            } else {
//                for (int i = 0; i < src.length; i++) {
//                    int d = bytes[i] & 0xFF;
//                    int s = src[i] & 0xFF;
//                    switch (bitOp) {
//                        case AND:
//                            bytes[i] = (byte) (d & s);
//                            break;
//                        case OR:
//                            bytes[i] = (byte) (d | s);
//                            break;
//                        case XOR:
//                            bytes[i] = (byte) (d ^ s);
//                            break;
//                    }
//                }
//            }
        }
//        _put(destkey1, bytes);
        bitCache.invalidate(Unpooled.wrappedBuffer(destkey1));
        return integer(r64nm.getLongCardinality());
    }

    enum BitOp {AND, OR, XOR, NOT}


//    public static byte[] getMetaKey(byte[] key0) {
//        ByteBuf metaKey = MyUtils.concat(NS, DataType.SPLIT, key0, DataType.SPLIT, TYPE);
//        return MyUtils.toByteArray(metaKey);
//    }

//    public static ByteBuf getMetaKey1(byte[] key0) {
//        ByteBuf metaKey = MyUtils.concat(NS, DataType.SPLIT, key0, DataType.SPLIT, TYPE);
//        return metaKey;
//    }

    @Deprecated
    public static byte[] getMetaVal(byte[] value, long expiration) {

        ByteBuf buf = Unpooled.buffer(12);
        buf.writeLong(expiration); //ttl 无限期 -1
        buf.writeBytes(DataType.SPLIT);

        buf.writeInt(DataType.KEY_STRING); //value type
        buf.writeBytes(DataType.SPLIT);

        buf.writeLong(value.length); //value size
        buf.writeBytes(DataType.SPLIT);

        //业务数据
        buf.writeBytes(value);

        return MyUtils.toByteArray(buf);

    }


    static LoadingCache<ByteBuf, Roaring64NavigableMap> bitCache
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
//            .weakKeys()
//            .weakValues()
            // 设置缓存的移除通知 ,将数据持久化到RocksDb
            .removalListener(new RemovalListener<ByteBuf, Roaring64NavigableMap>() {
                public void onRemoval(RemovalNotification<ByteBuf, Roaring64NavigableMap> notification) {
                    log.debug(String.format("因为%s的原因，%s=%s已经删除",
                            notification.getCause(),
                            MyUtils.ByteBuf2String(notification.getKey()),
                            notification.getValue().getLongCardinality()));

                    try {
                        {
                            Roaring64NavigableMap r64nm = notification.getValue();

                            ByteBuf metaKey = getMetaKey(MyUtils.toByteArray(notification.getKey()));

//                            db.put(notification.getKey().array(), Snappy.compress(DbUtils.exportBitmap(r64nm).toByteArray()));
                            db.put(MyUtils.toByteArray(metaKey), getMetaVal(Snappy.compress(DbUtils.exportBitmap(r64nm).toByteArray()), -1));

                            log.debug(String.format("获取Roaring64NavigableMap(数量=%s) 列表，持久化到 RocksDb。\n key:%s 初始验证：%s  修改验证：%s",
                                    r64nm.getLongCardinality(),
                                    MyUtils.ByteBuf2String(notification.getKey()),
                                    r64nm.contains(13691588588l),
                                    r64nm.contains(15510325566l))
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
            .build(new CacheLoader<ByteBuf, Roaring64NavigableMap>() {
                @Override
                public Roaring64NavigableMap load(ByteBuf key) throws Exception {

//                    key.writeBytes("bits");

                    byte[] value = db.get(MyUtils.toByteArray(getMetaKey(MyUtils.toByteArray(key))));

                    log.debug(value);

                    Roaring64NavigableMap r64nm = null;
                    if (null == value) {
                        r64nm = new Roaring64NavigableMap();
                    } else {
                        r64nm = DbUtils.importBitmap(Snappy.uncompress(value));
                    }

//                    byte[] value = db.get(getMetaKey(key0.array()));


                    log.debug(String.format("加载Roaring64NavigableMap从RocksDb: key %s  数量%s  初始验证：%s 修改验证：%s\"",
                            MyUtils.ByteBuf2String(key),
                            r64nm.getLongCardinality(),
                            r64nm.contains(13691588588l),
                            r64nm.contains(15510325566l))
                    );

//                    r64nm.addLong(15510325566l);

                    return r64nm;
                }
            });

    /**
     *  bitfield命令
     *  通过bitfield命令我们可以一次性对多个比特位域进行操作
     *  GET <type> <offset>
     * SET <type> <offset> <value>
     * INCRBY <type> <offset> <increment>
     *     其中，get命令的作用是读取指定位域的值，
     *     set命令的作用是设置指定位域的值并返回旧的值，
     *     increby命令的作用是增加或减少指定位域的值并返回新的值。
     *
     * BITFIELD mykey SET i5 100 10 GET u4 2
     *
     * 这个命令包含了2个子操作，分别是SET i5 100 10和GET u4 2。SET i5 100 10的作用是从第100位开始，将接下来的5位用有符号数10代替，其中i表示的是有符号整数。GET u4 2的作用是从第2位开始，将接下来的4位当成无符号整数并取出，其中u表示的是无符号整数。
     * bitfield k1 set u1 1 1 set u1 3 1 set u1 6 1
     * bitfield k1 get u1 2 get u1 4 get u1 7
     *
     */

    /**
     * BITPOS key bit [start] [end]
     * 返回字符串里面第一个被设置为1或者0的bit位。
     * <p>
     * assertEquals(rr.select(3),1000);//0-3
     */

    public static void main(String[] args) throws Exception {

    }


}
