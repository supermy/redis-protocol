package redis.server.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import redis.netty4.*;
import redis.server.netty.utis.DataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static redis.netty4.BulkReply.NIL_REPLY;
import static redis.netty4.IntegerReply.integer;
import static redis.netty4.StatusReply.OK;
import static redis.server.netty.RedisBase.invalidValue;
import static redis.server.netty.RocksdbRedis._toposint;
import static redis.util.Encoding.bytesToNum;
import static redis.util.Encoding.numToBytes;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

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
 * not support method: getbit setbit
 */
public class StringMeta {

    private static Logger log = Logger.getLogger(StringMeta.class);


    ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();

    private RocksDB db;

    private byte[] NS;
    private static byte[] TYPE = DataType.KEY_META;

    private ByteBuf metaKey;
    private ByteBuf metaVal;


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
        return instance;
    }

    /**
     * 构造 MetaKey
     *
     * @param key0
     * @return
     * @throws RedisException
     */
    public StringMeta genMetaKey(byte[] key0) throws RedisException {
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

    protected StringMeta genVal(byte[] value, long expiration) {

        ByteBuf ttlBuf = Unpooled.buffer(12);
        ttlBuf.writeLong(expiration); //ttl 无限期 -1
        ttlBuf.writeBytes(DataType.SPLIT);

        ttlBuf.writeInt(DataType.KEY_STRING); //value type
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


    public BulkReply getRange(byte[] key0, byte[] start1, byte[] end2) throws RedisException {
        genMetaKey(key0);

        BulkReply bulkReply = get(key0);

//        db.get(key0,out1); //fixme 从开始到结束，二次进行截取；

        byte[] bytes = bulkReply.data().array();

        int size = bytes.length;
        int s = RocksdbRedis.__torange(start1, size);
        int e = RocksdbRedis.__torange(end2, size);
        if (e < s) e = s;
        int length = e - s + 1;

        ByteBuf valueBuf = Unpooled.wrappedBuffer(bytes); //零拷贝
        ByteBuf slice = valueBuf.slice(s, length);

        return new BulkReply(slice.readBytes(slice.readableBytes()).array());

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

        genMetaKey(key0);

        BulkReply put = get(key0);
        set(key0, value1, null);
        if (put.data() == null || put.data().array() instanceof byte[]) {
            return put == null ? NIL_REPLY : new BulkReply((byte[]) put.data().array());
        } else {
            throw invalidValue();
        }
    }


    /**
     * 获取所有(一个或多个)给定 key 的值。
     *
     * @param keys
     * @return
     * @throws RedisException
     */
    public MultiBulkReply mget(byte[]... keys) throws RedisException {


        List<byte[]> listFds = new ArrayList<byte[]>();

        for (byte[] k : keys
        ) {
            listFds.add(genMetaKey(k).getKey());
        }

        List<BulkReply> list = __mget(listFds);

        return new MultiBulkReply(list.toArray(new BulkReply[list.size()]));

    }


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

        //处理 field key
        for (int i = 0; i < field_or_value1.length; i += 2) {
            field_or_value1[i] = genMetaKey(field_or_value1[i]).getKey();
//            log.debug(new String(field_or_value1[i]));
        }

        __mput(field_or_value1);

        return OK;
    }

    protected void __mput(byte[][] field_or_value1) {
        final WriteOptions writeOpt = new WriteOptions();
        final WriteBatch batch = new WriteBatch();
        for (int i = 0; i < field_or_value1.length; i += 2) {

            byte[] val = genVal(field_or_value1[i + 1], -1).getVal();

            try {

                batch.put(field_or_value1[i], val);
                //fixme  通过 keys 获取数量
            } catch (RocksDBException e) {
                e.printStackTrace();
            }

        }
        try {

            db.write(writeOpt, batch);

        } catch (RocksDBException e) {
            throw new RuntimeException(e.getMessage());
        }
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


        int length = key_or_value0.length;
        if (length % 2 != 0) {
            throw new RedisException("wrong number of arguments for MSETNX");
        }


        List<byte[]> listFds = new ArrayList<byte[]>();
        //处理 string 的主键
        for (int i = 0; i < length; i += 2) {
            //_put(key_or_value0[i], key_or_value0[i + 1]);
            key_or_value0[i] = genMetaKey(key_or_value0[i]).getKey();
            listFds.add(key_or_value0[i]);
        }

        //有一个不存在,返回错误
//        List<BulkReply> bulkReplies = mget(listFds.toArray());
        List<BulkReply> bulkReplies = __mget(listFds);

        for (BulkReply br : bulkReplies
        ) {
            if (br.data() != null) {
                return integer(0);
            }

        }

        __mput(key_or_value0);

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
//        genKey1(key0);

        long incr = bytesToNum(increment2);
        BulkReply field = get(key0);

//        log.debug("val:"+field.asAsciiString());


        if (field.data() == null) {
            set(key0, increment2, null);
            return new IntegerReply(incr);
        } else {

            String fld = field.asAsciiString();
            long value = Long.parseLong(fld);

//            long value =             field.data().readLong();

            value = value + incr;

            set(key0, (value + "").getBytes(), null);

            return new IntegerReply(value);
        }

    }

    public IntegerReply decr(byte[] key0) throws RedisException {
//        genKey1(key0);

        return incrby(key0, "-1".getBytes());
    }

    public IntegerReply decrby(byte[] key0, byte[] decrement1) throws RedisException {

        long l = -bytesToNum(decrement1);

        return incrby(key0, numToBytes(l));
    }


    public IntegerReply append(byte[] key0, byte[] value1) throws RedisException {
        BulkReply src = get(key0);
        ByteBuf targetBuf = Unpooled.wrappedBuffer(src.data().array(), value1);

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

        buf.writeBytes(value2, offset, value2.length);

        byte[] array = buf.readBytes(buf.readableBytes()).array();

        set(key0, array, null);

        return integer(array.length);

    }


    public static void main(String[] args) throws Exception {
        testString();

    }

    /**
     * Hash数据集测试
     *
     * @throws RedisException
     */
    private static void testString() throws RedisException {
        //String set get 操作
        //通过给数据增加版本号，进行快速删除？需要多进行一次查询

        StringMeta metaString = StringMeta.getInstance(RocksdbRedis.mymeta, "redis".getBytes());
        metaString.genMetaKey("HashUpdate".getBytes()).del();

        // 已经存在的 key
        metaString.get("HashUpdate".getBytes()).data();
        Assert.assertNull(metaString.get("HashInit".getBytes()).data());


        //测试创建
        metaString.set("key".getBytes(), "value".getBytes(), null);

        log.debug(metaString.get("key".getBytes()).asUTF8String());

        Assert.assertArrayEquals("value".getBytes(), metaString.get("key".getBytes()).data().array());
        Assert.assertEquals("value".length(), metaString.strlen("key".getBytes()).data().intValue());

        log.debug(metaString.getKey0Str());

        Assert.assertArrayEquals("key".getBytes(), metaString.getKey0());

        Assert.assertArrayEquals("lu".getBytes(), metaString.getRange("key".getBytes(), "2".getBytes(), "3".getBytes()).data().array());
        Assert.assertArrayEquals("value".getBytes(), metaString.getset("key".getBytes(), "val".getBytes()).data().array());


        metaString.mset("k1".getBytes(), "v1".getBytes(), "k2".getBytes(), "v2".getBytes(), "k3".getBytes(), "v3".getBytes());
        metaString.msetnx("k1".getBytes(), "va".getBytes(), "k4".getBytes(), "v4".getBytes(), "k5".getBytes(), "v5".getBytes());
        metaString.msetnx("k7".getBytes(), "v7".getBytes(), "k8".getBytes(), "v8".getBytes(), "k9".getBytes(), "v9".getBytes());

        String[] strings5 = {"v1", "v2", "v3"};
        Assert.assertEquals(metaString.mget("k1".getBytes(), "k2".getBytes(), "k3".getBytes()).toString(), Arrays.asList(strings5).toString());

        String[] strings6 = {"v7", "v8", "v9"};
        Assert.assertEquals(metaString.mget("k7".getBytes(), "k8".getBytes(), "k9".getBytes()).toString(), Arrays.asList(strings6).toString());

        String[] strings7 = {null, null, null};
        Assert.assertEquals(metaString.mget("k4".getBytes(), "k5".getBytes(), "k6".getBytes()).toString(), Arrays.asList(strings7).toString());


        metaString.info();

        metaString.genMetaKey("incr1".getBytes()).del();

        Assert.assertEquals(metaString.decr("incr1".getBytes()).data().intValue(), -1);
        Assert.assertEquals(metaString.incr("incr1".getBytes()).data().intValue(), 0);
        Assert.assertEquals(metaString.decrby("incr1".getBytes(), "2".getBytes()).data().intValue(), -2);
        Assert.assertEquals(metaString.incrby("incr1".getBytes(), "2".getBytes()).data().intValue(), 0);

        metaString.info();

        Assert.assertEquals(metaString.append("key".getBytes(), "append".getBytes()).data().intValue(), 9);
        Assert.assertEquals(metaString.get("key".getBytes()).asUTF8String(), "valappend");

        metaString.info();

        log.debug("done ......");

    }

}
