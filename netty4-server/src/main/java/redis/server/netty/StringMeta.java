package redis.server.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
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

import static java.lang.Double.parseDouble;
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
 * String      [<ns>] <key> KEY_META                 KEY_STRING <MetaObject>
 * <p>
 * Created by moyong on 2017/11/29.
 * Update by moyong 2018/09/18.
 * method: set get getrange getset mget setex setnx setrange strlen mset msetnx  psetex  incr incrby incrbyfloat decr decrby append
 * <p>
 * not support method: getbit setbit
 */
public class StringMeta {

//    private static final org.slf4j.Logger log = LoggerFactory.getLogger(StringMeta.class);


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
     *
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


    private byte[] getKey0()  {
        metaKey.resetReaderIndex();
        ByteBuf bb = metaKey.slice(NS.length + DataType.SPLIT.length, metaKey.readableBytes() - 8);
        return bb.readBytes(bb.readableBytes()).array();
    }

    private String getKey0Str()  {
        return new String(getKey0());
    }

    private byte[] getKey()  {
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
        ByteBuf valueBuf = metaVal.slice(8 + 4, metaVal.readableBytes() - 8 - 4);
        //数据过期处理
        if (getTtl() < now() && getTtl() != -1) {
            try {
                db.delete(getKey());
                //throw new RedisException(String.format("没有如此的主键:%s", getKey0Str()));
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


    public long getTtl() {
        return metaVal.getLong(0);
    }

    public int getSize() {
        return metaVal.getInt(8);
    }


    private long now() {
        return System.currentTimeMillis();
    }


    public String info() throws RedisException {

        StringBuilder sb = new StringBuilder(getKey0Str());

        sb.append(":");
//        sb.append("  count=");
        sb.append(getVal0Str());

        System.out.println(sb.toString());

        return sb.toString();
    }


    public StringMeta destory(byte[] key) throws RedisException {
        try {
            genMetaKey(key);
            db.delete(getKey());
        } catch (RocksDBException e) {
            throw new RedisException(e.getMessage());
        }
        return this;
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

        ByteBuf ttlBuf = Unpooled.buffer(12);

        if (seconds2 == null) {
            ttlBuf.writeLong(-1); //ttl 无限期 -1

        } else {
            ttlBuf.writeLong(RocksdbRedis.bytesToLong(seconds2));
        }

        ttlBuf.writeInt(DataType.VAL_STRING); //value type

        ByteBuf val0Buf = Unpooled.wrappedBuffer(val1);

        this.metaVal = Unpooled.wrappedBuffer(ttlBuf, val0Buf);//零拷贝

        //todo 异步队列删除 Hash Set SortSet List 的 meta数据与element 数据

        try {
            db.put(getKey(), getVal());

        } catch (RocksDBException e) {
            throw new RedisException(e.getMessage());
        }

        return OK;
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
     *
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

    /**
     * 分解 Value,获取业务数据
     *
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
            ByteBuf sizeBuf = vvBuf.readSlice(4);
            ByteBuf valueBuf = vvBuf.slice(8 + 4, values.length - 8 - 4);

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


    public StatusReply mset(byte[]... field_or_value1) throws RedisException {
        if (field_or_value1.length % 2 != 0) {
            throw new RedisException("wrong number of arguments for HMSET");
        }

        //处理 field key
        for (int i = 0; i < field_or_value1.length; i += 2) {
            field_or_value1[i] = genMetaKey(field_or_value1[i]).getKey();
//            System.out.println(new String(field_or_value1[i]));
        }

        __mput(field_or_value1);

        return OK;
    }

    protected void __mput(byte[][] field_or_value1) {
        final WriteOptions writeOpt = new WriteOptions();
        final WriteBatch batch = new WriteBatch();
        for (int i = 0; i < field_or_value1.length; i += 2) {

            byte[] val = genVal(field_or_value1[i + 1], -1);

            //System.out.println(new String(field_or_value1[i]));
            //System.out.println(new String(val));

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

    protected byte[] genVal(byte[] value, long expiration) {
        ByteBuf ttlBuf = Unpooled.buffer(12);
        ttlBuf.writeLong(expiration); //ttl 无限期 -1
        ttlBuf.writeInt(value.length); //value size

        ByteBuf valueBuf = Unpooled.wrappedBuffer(value); //零拷贝
        ByteBuf valbuf = Unpooled.wrappedBuffer(ttlBuf, valueBuf);//零拷贝

        return valbuf.readBytes(valbuf.readableBytes()).array();
    }

    public IntegerReply strlen(byte[] key0) throws RedisException {
        BulkReply bulkReply = get(key0);

        return integer(bulkReply.data().array().length);
    }


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

//        System.out.println("val:"+field.asAsciiString());


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
//                RocksdbRedis.mydata.delete(HashNode.genKey(getKey0(),hkey));
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

    protected double _todouble(byte[] score) {
        return parseDouble(new String(score));
    }

    protected byte[] _tobytes(double score) {
        return String.valueOf(score).getBytes();
    }


    public static void main(String[] args) throws Exception {
        testString();

    }

    /**
     * Hash数据集测试
     *
     *
     * @throws RedisException
     */
    private static void testString() throws RedisException {
        //String set get 操作
        //通过给数据增加版本号，进行快速删除？需要多进行一次查询

        StringMeta metaString = StringMeta.getInstance(RocksdbRedis.mymeta, "redis".getBytes());
        metaString.destory("HashUpdate".getBytes()).destory("HashInit".getBytes());

        // 已经存在的 key
        metaString.get("HashUpdate".getBytes()).data();
        Assert.assertNull(metaString.get("HashInit".getBytes()).data());


        //测试创建
        metaString.set("key".getBytes(), "value".getBytes(), null);

        System.out.println(metaString.get("key".getBytes()).asUTF8String());

        Assert.assertArrayEquals("value".getBytes(), metaString.get("key".getBytes()).data().array());
        Assert.assertEquals("value".length(), metaString.strlen("key".getBytes()).data().intValue());

        System.out.println(metaString.getKey0Str());

        Assert.assertArrayEquals("key".getBytes(), metaString.getKey0());

        Assert.assertArrayEquals("lu".getBytes(), metaString.getRange("key".getBytes(), "2".getBytes(), "3".getBytes()).data().array());
        Assert.assertArrayEquals("value".getBytes(), metaString.getset("key".getBytes(), "val".getBytes()).data().array());

//        metaString.info();

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

        metaString.destory("incr1".getBytes());

        Assert.assertEquals(metaString.decr("incr1".getBytes()).data().intValue(), -1);
        Assert.assertEquals(metaString.incr("incr1".getBytes()).data().intValue(), 0);
        Assert.assertEquals(metaString.decrby("incr1".getBytes(), "2".getBytes()).data().intValue(), -2);
        Assert.assertEquals(metaString.incrby("incr1".getBytes(), "2".getBytes()).data().intValue(), 0);

        metaString.info();

        Assert.assertEquals(metaString.append("key".getBytes(), "append".getBytes()).data().intValue(), 9);
        Assert.assertEquals(metaString.get("key".getBytes()).asUTF8String(), "valappend");

        metaString.info();

    }

}
