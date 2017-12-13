package redis.server.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import redis.netty4.*;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.lang.Double.parseDouble;
import static redis.netty4.BulkReply.NIL_REPLY;
import static redis.netty4.IntegerReply.integer;
import static redis.netty4.StatusReply.OK;
import static redis.server.netty.RedisBase.invalidValue;
import static redis.server.netty.RocksdbRedis._toposint;
import static redis.util.Encoding.bytesToNum;


/**
 * String Meta 元素方便促常用操作
 * <p>
 * Created by moyong on 2017/11/29.
 */
public class StringMeta {

    private static byte[] PRE = "+".getBytes();
    private static byte[] SUF = "string".getBytes();

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
    public StringMeta create(RocksDB db0, byte[] key0, long count) throws RedisException {
        return new StringMeta(db0, key0, count);
    }

    public StringMeta(RocksDB db0, byte[] key0, long count) throws RedisException {
        this.db = db0;
        create(key0, count);
    }

    protected StringMeta create(byte[] key0, long count) throws RedisException {
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
    public StringMeta get(RocksDB db0, byte[] key0, boolean notExsitCreate) throws RedisException {
        return new StringMeta(db0, key0, notExsitCreate);
    }

    /**
     * 数据初始化
     *
     * @param key
     * @throws RedisException
     */
    public StringMeta(RocksDB db0, byte[] key0, boolean notExsitCreate) throws RedisException {
        this.db = db0;
        this.metaKey = Unpooled.wrappedBuffer(PRE, key0, SUF);
        get(notExsitCreate);
    }

    private StringMeta get(boolean notExsitCreate) throws RedisException {

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
        ByteBuf bb = metaKey.slice(1, metaKey.readableBytes() - 7);
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

    public StringMeta init() throws RedisException {
        create(getKey0(), 0);
        return this;
    }


    public void destory() throws RedisException {
        try {
            db.delete(getKey());

//            List<String> strings = hkeys().asStringList(Charset.defaultCharset());
//
////                            System.out.println(strings.size());
//
//            for (String str:strings
//                 ) {
////                System.out.println(getKey0Str());
////                System.out.println(str);
//                byte[] key = HashNode.genKey(getKey0(), str.getBytes());
////                System.out.println(new String(key));
//
//                RocksdbRedis.mydata.delete(key);
//            }
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
    public StatusReply set(byte[] value2) throws RedisException {
        StringNode newnode = new StringNode(RocksdbRedis.mydata, getKey0(), value2);
        return OK;
    }


    public BulkReply get() throws RedisException {
        StringNode newnode = new StringNode(RocksdbRedis.mydata, getKey0());
        if (newnode == null || newnode.value()==null) {
            return NIL_REPLY;
        } else {
            return new BulkReply(newnode.getVal0());
        }
    }

    public BulkReply getrange(byte[] start1, byte[] end2) throws RedisException {

        BulkReply bulkReply = get();

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
    public BulkReply getset(byte[] value1) throws RedisException {
        BulkReply put = get();
        set(value1);
        if (put.data() == null || put.data().array() instanceof byte[]) {
            return put == null ? NIL_REPLY : new BulkReply((byte[]) put.data().array());
        } else {
            throw invalidValue();
        }
    }



    public MultiBulkReply mget(byte[]... keys) throws RedisException {


        List<byte[]> listFds = new ArrayList<byte[]>();

        for (byte[] k : keys
                ) {
            listFds.add(StringNode.genKey(k));
        }

        List<BulkReply> list = StringNode.__mget(listFds);

        return new MultiBulkReply(list.toArray(new BulkReply[list.size()]));

    }


    public StatusReply mset(byte[]... field_or_value1) throws RedisException {
        if (field_or_value1.length % 2 != 0) {
            throw new RedisException("wrong number of arguments for HMSET");
        }

        //处理 field key
        for (int i = 0; i < field_or_value1.length; i += 2) {
            field_or_value1[i] = StringNode.genKey(field_or_value1[i]);
        }

        StringNode.__mput(field_or_value1);

        return OK;
    }

    public IntegerReply strlen() throws RedisException {
        BulkReply bulkReply = get();

        return integer(bulkReply.data().array().length);
    }


    /**
     *
     * 命令用于所有给定 key 都不存在时，同时设置一个或多个 key-value 对
     *
     * @param key_or_value0
     * @return
     * @throws RedisException
     */
    public IntegerReply msetnx(byte[][] key_or_value0) throws RedisException {


        int length = key_or_value0.length;
        if (length % 2 != 0) {
            throw new RedisException("wrong number of arguments for MSETNX");
        }


        List<byte[]> listFds = new ArrayList<byte[]>();
        //处理 string 的主键
        for (int i = 0; i < length; i += 2) {
            //_put(key_or_value0[i], key_or_value0[i + 1]);
            key_or_value0[i] = StringNode.genKey(key_or_value0[i]);
            listFds.add(key_or_value0[i]);
        }

        //有一个不存在,返回错误
//        List<BulkReply> bulkReplies = mget(listFds.toArray());
        List<BulkReply> bulkReplies = StringNode.__mget(listFds);

        for (BulkReply br : bulkReplies
                ) {
            if (br.data() == null) {
                return integer(0);
            }

        }

        StringNode.__mput(key_or_value0);

        return integer(1);
    }


    public Reply psetex(byte[] milliseconds1, byte[] value2) throws RedisException {

        setex(value2, bytesToNum(milliseconds1) + now());
        return OK;
    }

    public IntegerReply incr(byte[] key0) throws RedisException {
        return incrby("1".getBytes());
    }


    public IntegerReply incrby(byte[] increment2) throws RedisException {
        long incr = bytesToNum(increment2);
        BulkReply field = get();

        if (field.data() == null) {
            set(increment2);
            return new IntegerReply(incr);
        } else {
            String fld = field.asAsciiString();
            long value = Long.parseLong(fld);

            value = value + incr;

            set((value + "").getBytes());

            return new IntegerReply(value);
        }

    }

    public IntegerReply decr() throws RedisException {

        return incrby("-1".getBytes());
    }

    public IntegerReply decrby(byte[] decrement1) throws RedisException {
        return incrby(decrement1);
    }


    public IntegerReply append(byte[] value1) throws RedisException {
        BulkReply src = get();
        ByteBuf targetBuf = Unpooled.wrappedBuffer(src.data().array(), value1);

        set(targetBuf.array());
        return integer(targetBuf.array().length);
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


    public StatusReply setex(byte[] seconds1, byte[] value2) throws RedisException {
        long sec = bytesToNum(seconds1);
//        set(value2);
        StringNode newnode = new StringNode(RocksdbRedis.mydata, getKey0(), value2,seconds1);

        return OK;
    }

    public IntegerReply setnx(byte[] value1) throws RedisException {

        BulkReply bulkReply = get();

        if(bulkReply.data()==null){
            set(value1);
            return integer(1);
        }else{
            return integer(0);

        }

    }

    public IntegerReply setrange(byte[] offset1, byte[] value2) throws RedisException {
        byte[] data = get().data().array();
        ByteBuf buf = get().data();

        long sec = bytesToNum(offset1);

        int offset = _toposint(offset1) < data.length ? _toposint(offset1) : data.length;

        buf.writeBytes(value2, offset, value2.length);

        byte[] array = buf.readBytes(buf.readableBytes()).array();
        set(array);

        return integer(array.length);

    }

    public IntegerReply strlen(byte[] key0) throws RedisException {
        return integer(get().data().array().length);
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


    public MultiBulkReply hgetall() throws RedisException {

        List<Reply<ByteBuf>> replies = new ArrayList<Reply<ByteBuf>>();

        List<byte[]> keyVals = HashNode.keyVals(RocksdbRedis.mydata, HashNode.genKeyPartten(getKey0()));//顺序读取 ;field 过期逻辑复杂，暂不处理


        int i = 0;
        byte[] curKey = new byte[0];
        for (byte[] bt : keyVals
                ) {
            if (i % 2 == 0) {  //key 处理
                curKey = bt;
                ByteBuf hkeybuf1 = Unpooled.wrappedBuffer(bt); //优化 零拷贝
                ByteBuf slice = hkeybuf1.slice(3 + getKey0().length, bt.length - 3 - getKey0().length);

                //HashNode.parseValue(RocksdbRedis.mydata,getKey(),bt);

                replies.add(new BulkReply(slice.readBytes(slice.readableBytes()).array()));
            } else {  // value

                replies.add(new BulkReply(HashNode.parseValue(RocksdbRedis.mydata,curKey,bt)));
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


        List<byte[]> keyVals = HashNode.keyVals(RocksdbRedis.mydata, HashNode.genKeyPartten(getKey0()));//顺序读取 ;field 过期逻辑复杂，暂不处理


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
                replies.add(new BulkReply(HashNode.parseValue(RocksdbRedis.mydata,curKey,bt)));

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
     *
     * @throws RedisException
     */
    private static void testHash() throws RedisException {
        StringMeta meta9 = new StringMeta(RocksdbRedis.mymeta, "HashUpdate".getBytes(), true);
        meta9.destory();
        //测试删除
        StringMeta meta0 = new StringMeta(RocksdbRedis.mymeta, "HashInit".getBytes(), true);
        meta0.destory();

        //LPUSHX 已经存在的 key
        try {
            new StringMeta(RocksdbRedis.mymeta, "HashInit".getBytes(), false);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof RedisException);
            Assert.assertTrue(e.getMessage().contains("没有如此的主键"));
        }

        //测试创建
        StringMeta meta1 = new StringMeta(RocksdbRedis.mymeta, "HashInit".getBytes(), true);

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
        StringMeta meta2 = new StringMeta(RocksdbRedis.mymeta, "HashUpdate".getBytes(), false);
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

        Assert.assertArrayEquals(meta2.hget(f1).data().array(),v1);
        Assert.assertArrayEquals(meta2.hget(f2).data().array(),v2);

        Assert.assertEquals(meta2.hlen().data().longValue(),2);

        meta2.hdel(f2);

//        Assert.assertNull(meta2.hget(f2).data());

        Assert.assertEquals(meta2.hlen().data().longValue(),1);


        meta2.hmset(f3,v3,f4,v4);

        Assert.assertEquals(meta2.hlen().data().longValue(),3);

        String[] strings3 = {"v1","v3"};

        Assert.assertEquals(meta2.hmget(f1,f3).toString(),Arrays.asList(strings3).toString());

        String[] strings4 = {"f1","v1","f3","v3","f4","v4"};

        Assert.assertEquals(meta2.hgetall().toString(),Arrays.asList(strings4).toString());

        Assert.assertTrue(meta2.hexists("f1".getBytes()).data().intValue()==1);

        Assert.assertTrue(meta2.hincrby("f8".getBytes(),"1".getBytes()).data().intValue()==1);

        Assert.assertArrayEquals(meta2.hget("f8".getBytes()).data().array(),"1".getBytes());

        String[] strings5 = {"v1","v3","v4","1"};

        Assert.assertEquals(meta2.hvals().toString(),Arrays.asList(strings5).toString());



    }

}
