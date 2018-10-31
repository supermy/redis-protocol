package redis.server.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import redis.netty4.*;
import redis.server.netty.utis.DataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.lang.Double.parseDouble;
import static redis.netty4.BulkReply.NIL_REPLY;
import static redis.netty4.IntegerReply.integer;
import static redis.netty4.StatusReply.OK;
import static redis.server.netty.RedisBase.invalidValue;
import static redis.util.Encoding.bytesToNum;


/**
 * List Meta 元素方便促常用操作
 * <p>
 * Hash    [<ns>] <key> KEY_META                 KEY_HASH <MetaObject>
 * [<ns>] <key> KEY_HASH_FIELD <field>   KEY_HASH_FIELD <field-value>
 * </p>
 * <p>
 * key and value 都采用 | 分隔符号
 * * getKey 一般是包含组合键；
 * * getkey0 是纯粹的业务主键；
 * * 参见setVal0 long+int+int ttl,数据类型,数据长度；
 * * val 一般是包含ttl 的数据；val0是实际的业务数据
 * <p>
 * Created by moyong on 2017/11/9.
 * Update by moyong on 2018/09/24
 * </p>
 * <p>
 * todo 重构&测试 单个element 的操作方法在node;多个element 操作方法在meta,meta 的操作方法；
 */
public class HashMeta extends BaseMeta {

    private static Logger log = Logger.getLogger(HashMeta.class);


    protected static HashNode hashNode;

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
        hashNode = HashNode.getInstance(db0, ns0);
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

    private ByteBuf genMetaVal(long count) {
        ByteBuf val = Unpooled.buffer(8);

        val.writeLong(-1); //ttl 无限期 -1
        val.writeBytes(DataType.SPLIT);

        val.writeInt(DataType.KEY_HASH); //long 8 bit
        val.writeBytes(DataType.SPLIT);

        val.writeLong(count);  //数量
        val.writeBytes(DataType.SPLIT);

        return val;
    }

    /**
     * 创建meta key
     *
     * @param count
     * @return
     * @throws RedisException
     */
    protected HashMeta setMeta(long count) throws RedisException {

        this.metaVal = genMetaVal(count);

        log.debug(String.format("count:%d;  主键：%s; value:%s", count, getKey0Str(), getVal0()));

        try {
            db.put(getKey(), getVal());//fixme
        } catch (RocksDBException e) {
            e.printStackTrace();
            throw new RedisException(e.getMessage());
        }

        return this;
    }

    /**
     * 获取meta 数据
     *
     * @return
     * @throws RedisException
     */
    protected HashMeta getMeta() throws RedisException {

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




    public long getVal0() throws RedisException {
        return metaVal.getLong(8 + 4 + 2);
    }

    public void setVal0(long val0) throws RedisException {
        this.metaVal.setLong(8 + 4 + 2, val0);  //数量
    }

    public byte[] getVal() throws RedisException {
        this.metaVal.resetReaderIndex();
        return this.metaVal.readBytes(metaVal.readableBytes()).array();
    }

    public byte[] getVal(ByteBuf val) throws RedisException {
        val.resetReaderIndex();
        return val.readBytes(val.readableBytes()).array();
    }

    public long getTtl() {
        return metaVal.getLong(0);
    }

    public int getType() throws RedisException {
        if (metaVal == null) return -1;
        return this.metaVal.getInt(8 + 1);
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


    @Deprecated
    public long incrCount() throws RedisException {
        long pval = Math.incrementExact(getCount());
        setCount(pval);
        return pval;
    }

    //删除元素可用
    @Deprecated
    public long decrCount() throws RedisException {
        long pval = Math.decrementExact(getCount());
        setCount(pval);
        return pval;
    }

    /**
     * TTL 过期数据处理
     *
     * @return
     */
    private long now() {
        return System.currentTimeMillis();
    }


    public String info() throws RedisException {

        StringBuilder sb = new StringBuilder(getKey0Str());

        sb.append(":");
        sb.append("  count=");
        sb.append(getCount());

        log.debug(sb.toString());

        return sb.toString();
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
        hset1(field1, value2);

        return integer(1);
    }

    private HashMeta hset1(byte[] field1, byte[] value2) throws RedisException {
        //判断类型，非hash 类型返回异常信息；
        int type = getMeta().getType();//hashNode.typeBy(db, getKey());

        if (type != -1 && type != DataType.KEY_HASH) {
            //抛出异常 类型不匹配
            throw invalidValue();
        }

        //数据持久化
        hashNode.genKey1(getKey0(), field1).hset(value2);

        //todo 增加一个异步计数队列 ；先使用异步线程，后续使用异步队列替换； setMeta(hlen().data());

        MetaCountCaller taskCnt = new MetaCountCaller(db, getKey(), genKeyPartten(DataType.KEY_HASH_FIELD));
        singleThreadExecutor.execute(taskCnt);

        return this;
    }


    /**
     * 异步进行hash计数
     */
    class MetaCountCaller implements Runnable {

        private RocksDB db;
        private byte[] key0;
        private byte[] keyPartten;

        public MetaCountCaller(RocksDB db, byte[] key, byte[] keyPartten) {
            this.db = db;
            this.key0 = key;
            this.keyPartten = keyPartten;
        }


        @Override
        public void run() {

            long cnt = countBy(db, keyPartten);

            log.debug("MetaCountCaller ... cnt:" + cnt);


            try {
                db.put(key0, getVal(genMetaVal(cnt)));

            } catch (RocksDBException e) {
                e.printStackTrace();
            } catch (RedisException e) {
                e.printStackTrace();
            }


        }
    }


    public BulkReply hget(byte[] field1) throws RedisException {
        HashNode node = hashNode.genKey1(getKey0(), field1).hget();

        if (node == null || node.data() == null) {
            return NIL_REPLY;
        } else {
            return new BulkReply(node.getVal0());
        }
    }

    public IntegerReply hdel(byte[]... field1) throws RedisException {

        for (byte[] hkey : field1) {
            hashNode.genKey1(getKey0(), hkey).hdel();
        }

        //todo 重新计数；增加一个异步计数队列 ；先使用异步线程，后续使用异步队列替换；

        singleThreadExecutor.execute(new MetaCountCaller(db, getKey(), genKeyPartten(DataType.KEY_HASH_FIELD)));

        return integer(field1.length);
    }

    /**
     * 构建子元素扫描key
     *
     * @param filedType
     * @return
     */
    public byte[] genKeyPartten(byte[] filedType) throws RedisException {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(NS, DataType.SPLIT, getKey0(), DataType.SPLIT, filedType, DataType.SPLIT);
        return byteBuf.readBytes(byteBuf.readableBytes()).array();
    }

    /**
     * 与keys 分开，减少内存占用；
     *
     * @param db
     * @param pattern0
     * @return
     */
    protected static long countBy(RocksDB db, byte[] pattern0) {
        //按 key 检索所有数据
//        List<byte[]> keys = new ArrayList<>();
        long cnt = 0;
        try (final RocksIterator iterator = db.newIterator()) {
            for (iterator.seek(pattern0); iterator.isValid(); iterator.next()) {

                //确保检索有数据，hkeybuf.slice 不错误
                if (pattern0.length <= iterator.key().length) {
                    ByteBuf hkeybuf = Unpooled.wrappedBuffer(iterator.key()); //优化 零拷贝
                    ByteBuf slice = hkeybuf.slice(0, pattern0.length); //获取指定前缀长度的 byte[]

                    slice.resetReaderIndex();

                    //key有序 不相等后面无数据
                    if (Arrays.equals(slice.readBytes(slice.readableBytes()).array(), pattern0)) {
                        cnt = cnt + 1;
                    } else {
                        break;
                    }
                } else break;

            }
        }

        //检索过期数据,处理过期数据 ;暂不处理影响效率 fixme
//        log.debug(keys.size());

        return cnt;
    }

    /**
     * 按前缀检索所有的 keys
     *
     * @param pattern0
     * @return
     */
    protected static List<byte[]> keys(RocksDB db, byte[] pattern0) {
        //按 key 检索所有数据
        List<byte[]> keys = new ArrayList<>();
        try (final RocksIterator iterator = db.newIterator()) {
            for (iterator.seek(pattern0); iterator.isValid(); iterator.next()) {

                //确保检索有数据，hkeybuf.slice 不错误
                if (pattern0.length <= iterator.key().length) {
                    ByteBuf hkeybuf = Unpooled.wrappedBuffer(iterator.key()); //优化 零拷贝
                    ByteBuf slice = hkeybuf.slice(0, pattern0.length); //获取指定前缀长度的 byte[]

                    slice.resetReaderIndex();

                    //key有序 不相等后面无数据
                    if (Arrays.equals(slice.readBytes(slice.readableBytes()).array(), pattern0)) {

                        keys.add(iterator.key());
//                        log.debug(new String(iterator.key()));
//                        if (keys.size() >= 100000) {
//                            //数据大于1万条直接退出
//                            break;
//                        }
                    } else {
                        break;
                    }
                } else break;

            }
        }

        //检索过期数据,处理过期数据 ;暂不处理影响效率 fixme
//        log.debug(keys.size());

        return keys;
    }

    /**
     * 返回 key-value 直对形式
     *
     * @param data
     * @param pattern0
     * @return
     * @throws RedisException
     */
    protected static List<byte[]> keyVals(RocksDB data, byte[] pattern0) throws RedisException {

        //按 key 检索所有数据
        List<byte[]> keys = new ArrayList<>();
        try (final RocksIterator iterator = data.newIterator()) {
            for (iterator.seek(pattern0); iterator.isValid(); iterator.next()) {

                //确保检索有数据，hkeybuf.slice 不错误
                if (pattern0.length <= iterator.key().length) {
                    ByteBuf hkeybuf = Unpooled.wrappedBuffer(iterator.key()); //优化 零拷贝
                    ByteBuf slice = hkeybuf.slice(0, pattern0.length); //获取指定前缀长度的 byte[]

                    slice.resetReaderIndex();

                    //key有序 不相等后面无数据
                    if (Arrays.equals(slice.readBytes(slice.readableBytes()).array(), pattern0)) {

//                        byte[] value = __getValue(data, iterator.key(), iterator.value());
//                        HashNode newnode = new HashNode(RocksdbRedis.mydata, getKey0(), field1);


//                        if (value != null) {
                        keys.add(iterator.key());
                        keys.add(iterator.value());

//                        }

//                        if (keys.size() >= 100000) {
//                            //数据大于1万条直接退出  fixme
//                            break;
//                        }
                    } else {
                        break;
                    }
                } else break;

            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RedisException(e.getMessage());
        }

        //检索过期数据,处理过期数据 ;暂不处理影响效率 fixme
//        log.debug(keys.size());

        return keys;
    }


    public IntegerReply hlen() throws RedisException {
        Long cnt = countBy(db, genKeyPartten(DataType.KEY_HASH_FIELD));
        return integer(cnt);
    }


    public MultiBulkReply hkeys() throws RedisException {

        List<Reply<ByteBuf>> replies = new ArrayList<Reply<ByteBuf>>();


        List<byte[]> keys = keys(db, genKeyPartten(DataType.KEY_HASH_FIELD));

        for (byte[] k : keys
        ) {

            byte[] f = hashNode.parseHField(getKey0(), k);

            replies.add(new BulkReply(f));

        }

        return new MultiBulkReply(replies.toArray(new Reply[replies.size()]));
    }


    public IntegerReply hexists(byte[] field1) throws RedisException {

        boolean exists = hashNode.genKey1(getKey0(), field1).exists();

        return exists ? integer(1) : integer(0);
    }

    public MultiBulkReply hvals() throws RedisException {


        //检索所有的 hash field key  所有的 key 都是有序的
        List<Reply<ByteBuf>> replies = new ArrayList<Reply<ByteBuf>>();


        List<byte[]> keyVals = keyVals(db, genKeyPartten(DataType.KEY_HASH_FIELD));//顺序读取 ;field 过期逻辑复杂，暂不处理


        int i = 0;
//        byte[] curKey = new byte[0];
        for (byte[] bt : keyVals
        ) {
            if (i % 2 == 0) {  //key 处理
//                curKey = bt;
//                ByteBuf hkeybuf1 = Unpooled.wrappedBuffer(bt); //优化 零拷贝

//                ByteBuf slice = hkeybuf1.slice(3 + key.length, bt.length - 3 - key.length);
//
//                replies.add(new BulkReply(slice.readBytes(slice.readableBytes()).array()));
            } else {
//                replies.add(new BulkReply(bt));
                replies.add(new BulkReply(hashNode.parseValue0(bt)));

            }
            i++;
        }

        return new MultiBulkReply(replies.toArray(new Reply[replies.size()]));

    }

    public MultiBulkReply hgetall() throws RedisException {

        List<Reply<ByteBuf>> replies = new ArrayList<Reply<ByteBuf>>();

        List<byte[]> keyVals = keyVals(db, genKeyPartten(DataType.KEY_HASH_FIELD));//顺序读取 ;field 过期逻辑复杂，暂不处理


        int i = 0;
        byte[] curKey = new byte[0];
        for (byte[] bt : keyVals
        ) {
            if (i % 2 == 0) {  //key 处理
                curKey = bt;

                byte[] f = hashNode.parseHField(getKey0(), bt);

                //ByteBuf hkeybuf1 = Unpooled.wrappedBuffer(bt); //优化 零拷贝
                //ByteBuf slice = hkeybuf1.slice(3+NS.length + getKey0().length+TYPE.length, bt.length - 3 -NS.length - getKey0().length -TYPE.length);
                //byte[] f = slice.readBytes(slice.readableBytes()).array();

                replies.add(new BulkReply(f));
            } else {

                replies.add(new BulkReply(hashNode.parseValue0(bt)));
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

    public BulkReply hincrbyfloat(byte[] field1, byte[] increment2) throws RedisException {

        double incr = parseDouble(new String(increment2));
        BulkReply field = hget(field1);


        if (field.data() == null) {
            hset(field1, increment2);
            return new BulkReply(increment2);
        } else {
            double value = parseDouble(new String(field.data().array()));

            value = value + incr;

            byte[] bytes = String.valueOf(value).getBytes();

            hset(field1, bytes);

            return new BulkReply(bytes);
        }
    }


    public MultiBulkReply hmget(byte[]... field1) throws RedisException {


        List<byte[]> listFds = new ArrayList<byte[]>();

        for (byte[] fd : field1
        ) {
            listFds.add(hashNode.genKey1(getKey0(), fd).getKey());
        }

        List<BulkReply> list = hashNode.hmget(listFds);

        return new MultiBulkReply(list.toArray(new BulkReply[list.size()]));

    }


    public StatusReply hmset(byte[]... field_or_value1) throws RedisException {
        if (field_or_value1.length % 2 != 0) {
            throw new RedisException("wrong number of arguments for HMSET");
        }

        //处理 field key
        for (int i = 0; i < field_or_value1.length; i += 2) {
            field_or_value1[i] = hashNode.genKey1(getKey0(), field_or_value1[i]).getKey();
        }

        hashNode.hmput(field_or_value1);

        return OK;
    }

    public IntegerReply hsetnx(byte[] field1, byte[] value2) throws RedisException {

        if (hexists(field1).data() == 0) {
            return integer(0);
        } else {
            hset(field1, value2);
            return integer(1);
        }

    }


    public static void main(String[] args) throws Exception {
        testHash();

    }

    /**
     * Hash数据集测试
     *
     * @throws RedisException
     */
    private static void testHash() throws RedisException, InterruptedException {

        HashMeta meta9 = HashMeta.getInstance(RocksdbRedis.mydata, "redis".getBytes());
        //测试删除
        meta9.genMetaKey("HashUpdate".getBytes()).deleteRange(meta9.getKey0());
//        log.debug("hkeys0:"+meta9.members());


//        log.debug(meta9.hget("f1".getBytes()));
        Assert.assertNull(meta9.hget("f1".getBytes()).asUTF8String());

        meta9.genMetaKey("HashUpdate".getBytes()).hset("f1".getBytes(), "v1".getBytes());
//        log.debug(meta9.hget("f1".getBytes()));
        Assert.assertEquals("v1", meta9.hget("f1".getBytes()).asUTF8String());

        Thread.sleep(500);

//        log.debug("cnt:"+meta9.getCount());
        Assert.assertEquals(1, meta9.getMeta().getCount());


        meta9.genMetaKey("HashUpdate".getBytes()).hset("f2".getBytes(), "v2".getBytes());

        Thread.sleep(500);

//        log.debug("val:"+meta9.getVal0());
        Assert.assertEquals(2, meta9.getMeta().getCount());

//        log.debug("hkeys9:"+meta9.members());
//        log.debug("hvals:"+meta9.hvals());
//        log.debug("hgetall:"+meta9.hgetall());
//        log.debug("hmget:"+meta9.hmget("f1".getBytes(),"f2".getBytes()));


        //引入

        HashMeta meta2 = HashMeta.getInstance(RocksdbRedis.mydata, "TEST1".getBytes());
        meta2.genMetaKey("BATCH".getBytes()).deleteRange(meta2.getKey0());


        byte[] f1 = "f1".getBytes();
        byte[] f2 = "f2".getBytes();

        byte[] v1 = "v1".getBytes();
        byte[] v2 = "v2".getBytes();

        byte[] f3 = "f3".getBytes();
        byte[] f4 = "f4".getBytes();

        byte[] v3 = "v3".getBytes();
        byte[] v4 = "v4".getBytes();

//        log.debug("hkeys0:"+meta2.genMetaKey("BATCH".getBytes()).members());
//        log.debug("hkeys1:"+meta2.members());
//        log.debug("hlens1:"+meta2.hlen().data().longValue());

        meta2.genMetaKey("BATCH".getBytes()).hset1(f1, v1).hset1(f2, v2);

        Thread.sleep(500);

//        log.debug("hkeys2:"+meta2.members());
//        log.debug("hlens2:"+meta2.hlen().data().longValue());

        Assert.assertArrayEquals(meta2.hget(f1).data().array(), v1);
        Assert.assertArrayEquals(meta2.hget(f2).data().array(), v2);
        Assert.assertEquals(meta2.hlen().data().longValue(), 2);

        meta2.hdel(f2);
        Assert.assertNull(meta2.hget(f2).data());
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

        log.debug("Over ... ...");

    }

}
