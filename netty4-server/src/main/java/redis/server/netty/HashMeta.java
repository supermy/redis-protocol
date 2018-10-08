package redis.server.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import redis.netty4.*;
import redis.server.netty.utis.DataType;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.lang.Double.parseDouble;
import static java.lang.Double.valueOf;
import static redis.netty4.BulkReply.NIL_REPLY;
import static redis.netty4.IntegerReply.integer;
import static redis.netty4.StatusReply.OK;
import static redis.util.Encoding.bytesToNum;


/**
 * List Meta 元素方便促常用操作
 * <p>
 * Hash    [<ns>] <key> KEY_META                 KEY_HASH <MetaObject>
 *         [<ns>] <key> KEY_HASH_FIELD <field>   KEY_HASH_FIELD <field-value>
 * </p>
 * <p>
 * Created by moyong on 2017/11/9.
 * Update by moyong on 2018/09/24
 * </p>
 */
public class HashMeta {

//    private static byte[] NS = "+".getBytes();
//    private static byte[] TYPE = "hash".getBytes();
    ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();


    protected static HashNode hashNode;


    private byte[] NS;
    private static byte[] TYPE = DataType.KEY_META;

    private RocksDB db;

    private ByteBuf metaKey;
    private ByteBuf metaVal;

    private HashMeta() {
    }

    private static HashMeta instance = new HashMeta();

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
    public static HashMeta getInstance(RocksDB db0, byte[] ns0) {
        instance.db = db0;
        instance.NS = ns0;
        hashNode = HashNode.getInstance(db0, "redis".getBytes());
        return instance;
    }

    /**
     * 构造 MetaKey
     *
     * @param key0
     * @return
     * @throws RedisException
     */
    public HashMeta genMetaKey(byte[] key0) throws RedisException {
        if (key0 == null) {
            throw new RedisException(String.format("key0 主键不能为空"));
        }
        instance.metaKey = Unpooled.wrappedBuffer(instance.NS, DataType.SPLIT, key0, DataType.SPLIT, TYPE);
        return instance;
    }
    /////////////////////////

    /**
     * 快捷入口
     *
     * @param db0
     * @param key0
     * @param count
     * @return
     * @throws RedisException
     */
//    public HashMeta hset(RocksDB db0, byte[] key0, long count) throws RedisException {
//        return new HashMeta(db0, key0, count);
//    }

//    public HashMeta(RocksDB db0, byte[] key0, long count) throws RedisException {
//        this.db = db0;
//        hset(key0, count);
//    }

    /**
     * 创建metakey
     * @param count
     * @return
     * @throws RedisException
     */
    protected HashMeta setMeta(long count) throws RedisException {

        this.metaVal = Unpooled.buffer(8);
        this.metaVal.writeLong(-1); //ttl 无限期 -1
        this.metaVal.writeInt(DataType.KEY_HASH); //long 8 bit
        this.metaVal.writeLong(count);  //数量

        System.out.println(String.format("count:%d   主键：%s   value:%s", count, getKey0Str(), getVal0()));

        try {
            db.put(getKey(), getVal());
        } catch (RocksDBException e) {
            throw new RedisException(e.getMessage());
        }

        return this;
    }

    /**
     * 快捷入口
     *
     * @param db0
     * @param key0
     * @param notExsitCreate
     * @return
     * @throws RedisException
     */
//    public HashMeta set(RocksDB db0, byte[] key0, boolean notExsitCreate) throws RedisException {
//        return new HashMeta(db0, key0, notExsitCreate);
//    }

    /**
     * 数据初始化
     *
     * @param key
     * @throws RedisException
     */
    @Deprecated
    public HashMeta(RocksDB db0, byte[] key0, boolean notExsitCreate) throws RedisException {
        this.db = db0;
        this.metaKey = Unpooled.wrappedBuffer(NS, key0, TYPE);
//        set(notExsitCreate);
    }

    @Deprecated
    private HashMeta set(boolean notExsitCreate, long count) throws RedisException {
        try {

            byte[] values = db.get(getKey());
            if (values == null) {
                if (notExsitCreate) {
//                    return init();
                    return setMeta(count);
                } else throw new RedisException(String.format("没有如此的主键:%s", new String(getKey0())));
            }
            metaVal = Unpooled.wrappedBuffer(values);
            //todo conut 计数+1

            //数据过期处理
            if (getTtl() < now() && getTtl() != -1) {
                db.delete(getKey());
                metaVal = null;
                throw new RedisException(String.format("没有如此的主键:%s", new String(getKey0())));
            }

        } catch (RocksDBException e) {
            e.printStackTrace();
            throw new RedisException(String.format("没有如此的主键:%s", new String(getKey0())));
        }
        return this;
    }


    @Deprecated
    public void flush() throws RedisException {
        try {
            db.put(getKey(), getVal());
        } catch (RocksDBException e) {
            throw new RedisException(e.getMessage());
        }
    }

    /**
     * 参见genkey1,获取key0的分解
     *
     * @return
     * @throws RedisException
     */
    public byte[] getKey0() throws RedisException {
        metaKey.resetReaderIndex();
        ByteBuf bb = metaKey.slice(NS.length, metaKey.readableBytes() - NS.length - DataType.SPLIT.length * 2 - TYPE.length);
        return bb.readBytes(bb.readableBytes()).array();
    }


    public String getKey0Str() throws RedisException {
        return new String(getKey0());
    }

    public byte[] getKey() throws RedisException {
        metaKey.resetReaderIndex();
        return metaKey.readBytes(metaKey.readableBytes()).array();
    }

    /**
     * @param key0
     * @throws RedisException
     */
    public void setKey0(byte[] key0) throws RedisException {
        metaKey.resetReaderIndex();
        this.metaKey = Unpooled.wrappedBuffer(NS, DataType.SPLIT, key0, DataType.SPLIT, TYPE);
    }

    public long getVal0() throws RedisException {
        return metaVal.getLong(12);
    }

    public void setVal0(long val0) throws RedisException {
        this.metaVal.setLong(12, val0);  //数量
    }

    public byte[] getVal() throws RedisException {
        metaVal.resetReaderIndex();
        return metaVal.readBytes(metaVal.readableBytes()).array();
    }


    public long getTtl() {
        return metaVal.getLong(0);
    }

    public int getSize() {
        return metaVal.getInt(8);
    }


    @Deprecated
    public void sync() throws RedisException {
//        set(false);
    }

    /**
     * 元素数量
     *
     * @return
     */
    public long getCount() throws RedisException {
        return getVal0();
    }

    public void setCount(long val) throws RedisException {
        setVal0(val);
    }


    public long incrCount() throws RedisException {
        long pval = Math.incrementExact(getCount());
        setCount(pval);
        return pval;
    }

    public long decrCount() throws RedisException {
        long pval = Math.decrementExact(getCount());
        setCount(pval);
        return pval;
    }


    private long now() {
        return System.currentTimeMillis();
    }


    public boolean isNew() throws RedisException {
        return getCount() == 0;
    }


    public String info() throws RedisException {

        StringBuilder sb = new StringBuilder(getKey0Str());

        sb.append(":");
        sb.append("  count=");
        sb.append(getCount());

        System.out.println(sb.toString());

        return sb.toString();
    }

    public HashMeta init() throws RedisException {
        setMeta(0);
        return this;
    }

    /**
     * 批量删除主键(0-9.A-Z,a-z)；
     * 根据genkey 特征，增加风格符号，避免误删除数据；
     * @param key0
     * @throws RedisException
     */
    public void deleteRange(byte[] key0) throws RedisException {
        ByteBuf byteBufBegin = Unpooled.wrappedBuffer(NS, DataType.SPLIT, key0,DataType.SPLIT);
        ByteBuf byteBufEnd =  Unpooled.wrappedBuffer(NS, DataType.SPLIT, key0,DataType.SPLIT,"z".getBytes());

        byte[] begin=byteBufBegin.readBytes(byteBufBegin.readableBytes()).array();
        byte[] end=byteBufEnd.readBytes(byteBufEnd.readableBytes()).array();

        try {
            db.deleteRange(begin,end);
        } catch (RocksDBException e) {
            e.printStackTrace();
            throw new RedisException(e.getMessage());
        }
    }

    @Deprecated
    public void destory() throws RedisException {


        try {
            //fixme  db.deleteRange();
            db.delete(getKey());

            List<String> strings = hkeys().asStringList(Charset.defaultCharset());

//                            System.out.println(strings.size());

            for (String str : strings
            ) {
//                System.out.println(getKey0Str());
//                System.out.println(str);
                byte[] key = HashNode.genKey(getKey0(), str.getBytes());
//                System.out.println(new String(key));

                db.delete(key);
            }
        } catch (RocksDBException e) {
            throw new RedisException(e.getMessage());
        }
    }

    /**
     * 如果字段是哈希表中的一个新建字段，并且值设置成功，返回 1 。 如果哈希表中域字段已经存在且旧值已被新值覆盖，返回 0 。
     * meta 的计数，推送到任务队列进行异步处理。meta 数据有元素个数和TTL 数据。 todo 提升性能，减少交换次数。 先异步线程实现，再改为异步队列；
     *
     * @param field1
     * @param value2
     * @return
     * @throws RedisException
     */
    public IntegerReply hset(byte[] field1, byte[] value2) throws RedisException {

        hashNode.genKey1(getKey0(), field1).hset(value2);
        //todo 增加一个异步计数队列 增加一个删除其它类型的异步数据清理；先使用异步线程，后续使用异步队列替换；
        setMeta(hlen().data());

        //若有，算出原有的metakey，删除原有的elementKey 子元素；
        StringBuilder metaV = hashNode.existsBy(db, getKey());

        //维护metakey；计数；清理其他类型的key；
        //singleThreadExecutor

        //return put == null ? integer(0) : integer(1);
        return integer(1);
    }




    public BulkReply hget(byte[] field1) throws RedisException {
        HashNode node = hashNode.genKey1(getKey0(), field1).hget();

//        HashNode newnode = new HashNode(RocksdbRedis.mydata, getKey0(), field1);

        if (node == null || node.data() == null) {
            return NIL_REPLY;
        } else {
            return new BulkReply(node.getVal0());
        }
    }

    public IntegerReply hdel(byte[]... field1) throws RedisException {

        for (byte[] hkey : field1) {
            try {
                RocksdbRedis.mydata.delete(HashNode.genKey(getKey0(), hkey));
            } catch (RocksDBException e) {
                e.printStackTrace();
                throw new RedisException(e.getMessage());
            }
        }
        return integer(field1.length);
    }

    public IntegerReply hlen() throws RedisException {
        Long cnt = HashNode.countBy(db, hashNode.genKeyPartten());
        return integer(cnt);
    }

    /**
     * hash meta keys
     *
     * @param key
     * @return
     * @throws RedisException
     */
    protected List<byte[]> __hkeys() throws RedisException {

        List<byte[]> results = new ArrayList<>();

        List<byte[]> keys = HashNode.keys(db, hashNode.genKeyPartten());

        for (byte[] k : keys
        ) {

            byte[] f = HashNode.parseHField(getKey0(), k);

            results.add(f);
        }
        return results;

    }


    public MultiBulkReply hkeys() throws RedisException {

        List<byte[]> bytes = __hkeys();

        List<Reply<ByteBuf>> replies = new ArrayList<Reply<ByteBuf>>();
        for (byte[] bt : bytes
        ) {
            replies.add(new BulkReply(bt));
        }

        return new MultiBulkReply(replies.toArray(new Reply[replies.size()]));
    }


    public IntegerReply hexists(byte[] field1) throws RedisException {

        return hget(field1).data() == null ? integer(0) : integer(1);
    }


    public MultiBulkReply hgetall() throws RedisException {

        List<Reply<ByteBuf>> replies = new ArrayList<Reply<ByteBuf>>();

        List<byte[]> keyVals = HashNode.keyVals(db, hashNode.genKeyPartten());//顺序读取 ;field 过期逻辑复杂，暂不处理


        int i = 0;
        byte[] curKey = new byte[0];
        for (byte[] bt : keyVals
        ) {
            if (i % 2 == 0) {  //key 处理
                curKey = bt;
                ByteBuf hkeybuf1 = Unpooled.wrappedBuffer(bt); //优化 零拷贝
                ByteBuf slice = hkeybuf1.slice(3 + getKey0().length, bt.length - 3 - getKey0().length);

                //HashNode.parseValue(db,getKey(),bt);

                replies.add(new BulkReply(slice.readBytes(slice.readableBytes()).array()));
            } else {  // value

                replies.add(new BulkReply(HashNode.parseValue(db, curKey, bt)));
            }
            i++;
        }

        return new MultiBulkReply(replies.toArray(new Reply[replies.size()]));
    }


    public IntegerReply hincrby(byte[] field1, byte[] increment2) throws RedisException {
        long incr = bytesToNum(increment2);
        BulkReply field = hget(field1);

        if (field.data() == null) {
            hset(field1, increment2);
            return new IntegerReply(incr);
        } else {
            String fld = field.asAsciiString();
            long value = Long.parseLong(fld);

            value = value + incr;

            hset(field1, (value + "").getBytes());

            return new IntegerReply(value);
        }

    }

    protected double _todouble(byte[] score) {
        return parseDouble(new String(score));
    }

    protected byte[] _tobytes(double score) {
        return String.valueOf(score).getBytes();
    }

    public BulkReply hincrbyfloat(byte[] field1, byte[] increment2) throws RedisException {

        double incr = _todouble(increment2);
        BulkReply field = hget(field1);


        if (field.data() == null) {
            hset(field1, increment2);
            return new BulkReply(increment2);
        } else {
            double value = _todouble(field.data().array());

            value = value + incr;

            byte[] bytes = _tobytes(value);

            hset(field1, bytes);

            return new BulkReply(bytes);
        }
    }


    public MultiBulkReply hmget(byte[]... field1) throws RedisException {


        List<byte[]> listFds = new ArrayList<byte[]>();

        for (byte[] fd : field1
        ) {
            listFds.add(HashNode.genKey(getKey0(), fd));
        }

        List<BulkReply> list = HashNode.__mget(listFds);

        return new MultiBulkReply(list.toArray(new BulkReply[list.size()]));

    }


    public StatusReply hmset(byte[]... field_or_value1) throws RedisException {
        if (field_or_value1.length % 2 != 0) {
            throw new RedisException("wrong number of arguments for HMSET");
        }

        //处理 field key
        for (int i = 0; i < field_or_value1.length; i += 2) {
            field_or_value1[i] = HashNode.genKey(getKey0(), field_or_value1[i]);
        }

        HashNode.__mput(field_or_value1);

        return OK;
    }

    protected IntegerReply __hputnx(byte[] field, byte[] value) throws RedisException {
        boolean b = hget(field).data() == null ? false : true;
        if (b) {
            return integer(0);
        } else {
            hset(field, value);
            return integer(1);
        }

    }

    public IntegerReply hsetnx(byte[] key0, byte[] field1, byte[] value2) throws RedisException {

        return __hputnx(field1, value2);

    }


    public MultiBulkReply hvals() throws RedisException {


        //检索所有的 hash field key  所有的 key 都是有序的
        List<Reply<ByteBuf>> replies = new ArrayList<Reply<ByteBuf>>();


        List<byte[]> keyVals = HashNode.keyVals(db, hashNode.genKeyPartten());//顺序读取 ;field 过期逻辑复杂，暂不处理


        int i = 0;
        byte[] curKey = new byte[0];
        for (byte[] bt : keyVals
        ) {
            if (i % 2 == 0) {  //key 处理
                curKey = bt;
//                ByteBuf hkeybuf1 = Unpooled.wrappedBuffer(bt); //优化 零拷贝

//                ByteBuf slice = hkeybuf1.slice(3 + key.length, bt.length - 3 - key.length);
//
//                replies.add(new BulkReply(slice.readBytes(slice.readableBytes()).array()));
            } else {
//                replies.add(new BulkReply(bt));
                replies.add(new BulkReply(HashNode.parseValue(db, curKey, bt)));

            }
            i++;
        }

        return new MultiBulkReply(replies.toArray(new Reply[replies.size()]));

    }

    public static void main(String[] args) throws Exception {
        testHash();

    }

    /**
     * Hash数据集测试
     *
     * @throws RedisException
     */
    private static void testHash() throws RedisException {
//        HashMeta meta = new HashMeta(RocksdbRedis.mymeta, "HashUpdate".getBytes(), true);


        HashMeta meta9 = HashMeta.getInstance(RocksdbRedis.mymeta, "redis".getBytes());
        meta9.genMetaKey("HashUpdate".getBytes()).hset("f1".getBytes(), "v1".getBytes());

        //测试删除
        //meta9.destory();
        meta9.deleteRange(meta9.getKey0());


        //LPUSHX 已经存在的 key
        try {
            new HashMeta(RocksdbRedis.mymeta, "HashInit".getBytes(), false);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof RedisException);
            Assert.assertTrue(e.getMessage().contains("没有如此的主键"));
        }

        //测试创建
        HashMeta meta1 = new HashMeta(RocksdbRedis.mymeta, "HashInit".getBytes(), true);

        Assert.assertEquals(0, meta1.getCount());
        Assert.assertEquals(-1, meta1.getTtl());
        Assert.assertEquals(0, meta1.getVal0());
        Assert.assertArrayEquals("HashInit".getBytes(), meta1.getKey0());

        meta1.info();

        //测试数据更新
        meta1.setKey0("HashUpdate".getBytes());
        Assert.assertArrayEquals(meta1.getKey0(), "HashUpdate".getBytes());
        meta1.setCount(2);
        Assert.assertEquals(2, meta1.getCount());
        meta1.setVal0(0);
        Assert.assertEquals(0, meta1.getVal0());

        //测试数据持久化
        meta1.info();
        meta1.flush();//持久化

        //引入
        HashMeta meta2 = new HashMeta(RocksdbRedis.mymeta, "HashUpdate".getBytes(), false);
        meta2.info();
        Assert.assertEquals(0, meta2.getCount());
        meta2.incrCount();
        Assert.assertEquals(1, meta2.getCount());

        byte[] f1 = "f1".getBytes();
        byte[] f2 = "f2".getBytes();

        byte[] v1 = "v1".getBytes();
        byte[] v2 = "v2".getBytes();

        byte[] f3 = "f3".getBytes();
        byte[] f4 = "f4".getBytes();

        byte[] v3 = "v3".getBytes();
        byte[] v4 = "v4".getBytes();


        meta2.hset(f1, v1);
        meta2.hset(f2, v2);

//        meta2.hdel(f3);
//        meta2.hdel(f4);

        Assert.assertArrayEquals(meta2.hget(f1).data().array(), v1);
        Assert.assertArrayEquals(meta2.hget(f2).data().array(), v2);

        Assert.assertEquals(meta2.hlen().data().longValue(), 2);

        meta2.hdel(f2);

//        Assert.assertNull(meta2.hget(f2).data());

        Assert.assertEquals(meta2.hlen().data().longValue(), 1);


        meta2.hmset(f3, v3, f4, v4);

        Assert.assertEquals(meta2.hlen().data().longValue(), 3);

        String[] strings3 = {"v1", "v3"};

        Assert.assertEquals(meta2.hmget(f1, f3).toString(), Arrays.asList(strings3).toString());

        String[] strings4 = {"f1", "v1", "f3", "v3", "f4", "v4"};

        Assert.assertEquals(meta2.hgetall().toString(), Arrays.asList(strings4).toString());

        Assert.assertTrue(meta2.hexists("f1".getBytes()).data().intValue() == 1);

        Assert.assertTrue(meta2.hincrby("f8".getBytes(), "1".getBytes()).data().intValue() == 1);

        Assert.assertArrayEquals(meta2.hget("f8".getBytes()).data().array(), "1".getBytes());

        String[] strings5 = {"v1", "v3", "v4", "1"};

        Assert.assertEquals(meta2.hvals().toString(), Arrays.asList(strings5).toString());


    }

}
