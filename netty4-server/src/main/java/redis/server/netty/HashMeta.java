package redis.server.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import redis.netty4.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static java.lang.Double.parseDouble;
import static redis.netty4.BulkReply.NIL_REPLY;
import static redis.netty4.IntegerReply.integer;
import static redis.netty4.StatusReply.OK;
import static redis.util.Encoding.bytesToNum;


/**
 * List Meta 元素方便促常用操作
 * <p>
 * Created by moyong on 2017/11/9.
 */
public class HashMeta {

    private static byte[] PRE = "+".getBytes();
    private static byte[] SUF = "hash".getBytes();

    private RocksDB db;

    private ByteBuf metaKey;
    private ByteBuf metaVal;

    /**
     * 快捷入口
     *
     * @param db0
     * @param key0
     * @param count
     * @return
     * @throws RedisException
     */
    public HashMeta create(RocksDB db0, byte[] key0, long count) throws RedisException {
        return new HashMeta(db0, key0, count);
    }

    public HashMeta(RocksDB db0, byte[] key0, long count) throws RedisException {
        this.db = db0;
        create(key0, count);
    }

    protected HashMeta create(byte[] key0, long count) throws RedisException {
        this.metaKey = Unpooled.wrappedBuffer(PRE, key0, SUF);

        this.metaVal = Unpooled.buffer(8);
        this.metaVal.writeLong(-1); //ttl 无限期 -1
        this.metaVal.writeInt(8); //long 8 bit
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
    public HashMeta get(RocksDB db0, byte[] key0, boolean notExsitCreate) throws RedisException {
        return new HashMeta(db0, key0, notExsitCreate);
    }

    /**
     * 数据初始化
     *
     * @param key
     * @throws RedisException
     */
    public HashMeta(RocksDB db0, byte[] key0, boolean notExsitCreate) throws RedisException {
        this.db = db0;
        this.metaKey = Unpooled.wrappedBuffer(PRE, key0, SUF);
        get(notExsitCreate);
    }

    private HashMeta get(boolean notExsitCreate) throws RedisException {

        try {
            byte[] values = db.get(getKey());
            if (values == null) {
                if (notExsitCreate) {
                    return init();
                } else throw new RedisException(String.format("没有如此的主键:%s", new String(getKey0())));
            }
            metaVal = Unpooled.wrappedBuffer(values);

            //数据过期处理
            if (getTtl() < now() && getTtl() != -1) {
                db.delete(getKey());
                throw new RedisException(String.format("没有如此的主键:%s", new String(getKey0())));
            }

        } catch (RocksDBException e) {
            e.printStackTrace();
            throw new RedisException(String.format("没有如此的主键:%s", new String(getKey0())));
        }
        return this;
    }

    public void flush() throws RedisException {
        try {
            db.put(getKey(), getVal());
        } catch (RocksDBException e) {
            throw new RedisException(e.getMessage());
        }
    }

    public byte[] getKey0() throws RedisException {
        metaKey.resetReaderIndex();
        ByteBuf bb = metaKey.slice(1, metaKey.readableBytes() - 5);
        return bb.readBytes(bb.readableBytes()).array();
    }

    public String getKey0Str() throws RedisException {
        return new String(getKey0());
    }

    public byte[] getKey() throws RedisException {
        metaKey.resetReaderIndex();
        return metaKey.readBytes(metaKey.readableBytes()).array();
    }

    public void setKey0(byte[] key0) throws RedisException {
        metaKey.resetReaderIndex();
        this.metaKey = Unpooled.wrappedBuffer(PRE, key0, SUF);
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


    public void sync() throws RedisException {
        get(false);
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
        create(getKey0(), 0);
        return this;
    }


    public void destory() throws RedisException {
        try {
            db.delete(getKey());
        } catch (RocksDBException e) {
            throw new RedisException(e.getMessage());
        }
    }

    /**
     * 如果字段是哈希表中的一个新建字段，并且值设置成功，返回 1 。 如果哈希表中域字段已经存在且旧值已被新值覆盖，返回 0 。
     *
     * @param field1
     * @param value2
     * @return
     * @throws RedisException
     */
    public IntegerReply hset(byte[] field1, byte[] value2) throws RedisException {
//        System.out.println(new String(getKey0()));
        HashNode newnode = new HashNode(RocksdbRedis.mydata, getKey0(), field1, value2);
        //fixme 影响性能 覆盖还是新增不确定
//        incrCount();
//        flush();
//        return put == null ? integer(0) : integer(1);
        return integer(1);
    }

    public BulkReply hget(byte[] field1) throws RedisException {
        HashNode newnode = new HashNode(RocksdbRedis.mydata, getKey0(), field1);
        if (newnode == null) {
            return NIL_REPLY;
        } else {
            return new BulkReply(newnode.getVal0());
        }
    }

    public IntegerReply hdel(byte[]... field1) throws RedisException {

        for (byte[] hkey : field1) {
            try {
                RocksdbRedis.mydata.delete(HashNode.genKey(getKey0(),hkey));
            } catch (RocksDBException e) {
                e.printStackTrace();
                throw new RedisException(e.getMessage());
            }
        }
        return integer(field1.length);
    }

    public IntegerReply hlen() throws RedisException {
        List<byte[]> keys = HashNode.keys(RocksdbRedis.mydata, HashNode.genKeyPartten(getKey0()));
        return integer(keys.size());
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

        List<byte[]> keys = HashNode.keys(RocksdbRedis.mydata, HashNode.genKeyPartten(getKey0()));

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


    public MultiBulkReply hgetall(byte[] key0) throws RedisException {

        List<Reply<ByteBuf>> replies = new ArrayList<Reply<ByteBuf>>();

        List<byte[]> keyVals = HashNode.keyVals(RocksdbRedis.mydata, HashNode.genKeyPartten(getKey0()));//顺序读取 ;field 过期逻辑复杂，暂不处理


        int i = 0;
        for (byte[] bt : keyVals
                ) {
            if (i % 2 == 0) {  //key 处理

                ByteBuf hkeybuf1 = Unpooled.wrappedBuffer(bt); //优化 零拷贝
                ByteBuf slice = hkeybuf1.slice(3 + getKey0().length, bt.length - 3 - getKey0().length);

                //HashNode.parseValue(RocksdbRedis.mydata,getKey(),bt);

                replies.add(new BulkReply(slice.readBytes(slice.readableBytes()).array()));
            } else {

                replies.add(new BulkReply(bt));
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


    public MultiBulkReply hvals(byte[] key0) throws RedisException {


        //检索所有的 hash field key  所有的 key 都是有序的
        List<Reply<ByteBuf>> replies = new ArrayList<Reply<ByteBuf>>();


        List<byte[]> keyVals = HashNode.keyVals(RocksdbRedis.mydata, HashNode.genKeyPartten(getKey0()));//顺序读取 ;field 过期逻辑复杂，暂不处理


        int i = 0;
        for (byte[] bt : keyVals
                ) {
            if (i % 2 == 0) {  //key 处理
//                ByteBuf hkeybuf1 = Unpooled.wrappedBuffer(bt); //优化 零拷贝

//                ByteBuf slice = hkeybuf1.slice(3 + key.length, bt.length - 3 - key.length);
//
//                replies.add(new BulkReply(slice.readBytes(slice.readableBytes()).array()));
            } else {
                replies.add(new BulkReply(bt));
            }
            i++;
        }

        return new MultiBulkReply(replies.toArray(new Reply[replies.size()]));

    }

    public static void main(String[] args) throws Exception {
        testHash();

    }

    /**
     * 坐车插入数据集测试
     *
     * @throws RedisException
     */
    private static void testHash() throws RedisException {
        //测试删除
        HashMeta meta0 = new HashMeta(RocksdbRedis.mymeta, "HashInit".getBytes(), true);
        meta0.destory();

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

        meta2.hdel(f3);
        meta2.hdel(f4);

        Assert.assertArrayEquals(meta2.hget(f1).data().array(),v1);
        Assert.assertArrayEquals(meta2.hget(f2).data().array(),v2);

        Assert.assertEquals(meta2.hlen().data().longValue(),2);

        meta2.hdel(f2);

//        Assert.assertNull(meta2.hget(f2).data());

        Assert.assertEquals(meta2.hlen().data().longValue(),1);



        meta2.hmset(f3,v3,f4,v4);

        Assert.assertEquals(meta2.hlen().data().longValue(),3);

        String[] strings3 = {"v1","v3" ,"v4"};

        Assert.assertEquals(meta2.hmget(f1,f3,f4).toString(),Arrays.asList(strings3).toString());


    }

}
