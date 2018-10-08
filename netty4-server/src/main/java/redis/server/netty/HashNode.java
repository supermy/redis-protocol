package redis.server.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.rocksdb.*;
import redis.netty4.BulkReply;
import redis.server.netty.utis.DataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static redis.netty4.BulkReply.NIL_REPLY;


/**
 * Hash Node 元素方便促常用操作
 *
 * <p>
 * Hash    [<ns>] <key> KEY_META                 KEY_HASH <MetaObject>
 *         [<ns>] <key> KEY_HASH_FIELD <field>   KEY_HASH_FIELD <field-value>
 * </p>
 *
 *      * getKey 一般是包含组合键；
 *      * getkey0 是纯粹的业务主键；
 *      * 参见setVal0 long+int 数据长度，拆分ttl,获取到实际的数据；
 *      * val 一般是包含ttl 的数据；val0是实际的业务数据
 *
 * <p>
 * Created by moyong on 2017/11/23.
 * Updated by moyong on 2018/09/24
 *      hset hget
 * </p>
 */
public class HashNode {

    private static byte[] NS;
    private static byte[] TYPE = DataType.KEY_HASH_FIELD;

//    private static final   byte[] NS = "_h".getBytes();
//    private static final   byte[] TYPE = "#".getBytes();


    private RocksDB db;

    private byte[] key; //冗余缓存多拷贝一次 fixme 优化性能使用 keyBuf
    private byte[] field;

    private ByteBuf keyBuf;
    private ByteBuf valBuf;

    public ByteBuf data() {
        return valBuf;
    }

    private HashNode() {
    }

    private static HashNode instance = new HashNode();

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
    public static HashNode getInstance(RocksDB db0, byte[] ns0) {
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
    public HashNode genKey1(byte[] key0, byte[] field1) throws RedisException {
        if (key0 == null) {
            throw new RedisException(String.format("主键不能为空"));
        }

        this.key = key0;
        this.field = field1;

        instance.keyBuf = Unpooled.wrappedBuffer(instance.NS, DataType.SPLIT, key0, DataType.SPLIT, TYPE, DataType.SPLIT, field1);
        return instance;
    }


    private HashNode(RocksDB db) {
        db = RocksdbRedis.mydata;

        keyBuf = null;
        valBuf = null;

        field = null;
        key = null;
    }

    /**
     * 创建元素
     *
     * @param db0
     * @param key0
     * @param field0
     * @param val0
     * @throws RedisException
     */
    public HashNode(RocksDB db0, byte[] key0, byte[] field0, byte[] val0) throws RedisException {
        this.db = db0;
        this.key = key0;
        this.field = field0;

        genKey1(key0, field0);
        hset(val0);
    }

    protected void hset(byte[] val0) throws RedisException {
//        keyBuf = Unpooled.wrappedBuffer(NS, key, TYPE,field);
        ByteBuf ttlBuf = Unpooled.buffer(28);
        ttlBuf.writeLong(-1); //ttl 无限期 -1
        ttlBuf.writeInt(val0.length); //value size

        ByteBuf val0Buf = Unpooled.wrappedBuffer(val0);

        valBuf = Unpooled.wrappedBuffer(ttlBuf, val0Buf);//零拷贝

        try {
            db.put(getKey(), getVal());
        } catch (RocksDBException e) {
            throw new RedisException(e.getMessage());
        }
    }

    /**
     * 数据初始化
     *
     * @param db0
     * @param key0
     * @param field0
     * @throws RedisException
     */
    public HashNode(RocksDB db0, byte[] key0, byte[] field0) throws RedisException {
        this.db = db0;
        this.key = key0;
        this.field = field0;
        keyBuf = Unpooled.wrappedBuffer(NS, key, TYPE, field);
        hget();
    }

//    public HashNode(RocksDB db0, byte[] key, byte[] val) throws RedisException {
//        this.db = db0;
//
//        this.key = key0;
//        this.field = field0;
//
//        keyBuf = Unpooled.wrappedBuffer(key);
//        valBuf = Unpooled.wrappedBuffer(val);
//    }

    public HashNode hget() throws RedisException {
        try {

            byte[] values = db.get(getKey());

            if (values == null) {
                //throw new RedisException(String.format("没有如此的主键:%s|%s", getKey0Str(),getField0Str()));
//                return;
                valBuf = null;
                return null;
            }

            valBuf = Unpooled.wrappedBuffer(values);
            valBuf.resetReaderIndex();

            //数据过期处理
            if (getTtl() < now() && getTtl() != -1) {
                db.delete(getKey());
                valBuf = null;
//                throw new RedisException(String.format("没有如此的主键:%s|%s", getKey0Str(), getField0Str()));
                return null;
            }

            return this;

        } catch (RocksDBException e) {
            e.printStackTrace();
            throw new RedisException(String.format("没有如此的主键:%s|%s", getKey0Str(), getField0Str()));
        }
    }

    @Deprecated
    public void flush() throws RedisException {
        try {
            db.put(getKey(), getVal());
        } catch (RocksDBException e) {
            throw new RedisException(e.getMessage());
        }

    }

    public void destory() throws RedisException {
        try {
            db.delete(getKey());
        } catch (RocksDBException e) {
            throw new RedisException(e.getMessage());
        }
    }

    public void sync() throws RedisException {
        hget();
    }

    public void setVal(byte[] val0) throws RedisException {
        ByteBuf ttlBuf = Unpooled.buffer(28);
        ttlBuf.writeLong(-1); //ttl 无限期 -1
        ttlBuf.writeInt(val0.length); //value size

        ByteBuf val0Buf = Unpooled.wrappedBuffer(val0);

        valBuf = Unpooled.wrappedBuffer(ttlBuf, val0Buf);//零拷贝
    }


    @Deprecated
    public void setKey0(byte[] key0) throws RedisException {
        key = key0;
        keyBuf = Unpooled.wrappedBuffer(NS, DataType.SPLIT, key, DataType.SPLIT, TYPE, DataType.SPLIT, field);
    }

    private static long now() {
        return System.currentTimeMillis();
    }

    /**
     * 构造模式 Key, 用来遍历元素；
     *
     * @return
     */
    public  byte[] genKeyPartten() {

        ByteBuf byteBuf = Unpooled.wrappedBuffer(NS, DataType.SPLIT, getKey0(), DataType.SPLIT, TYPE, DataType.SPLIT);
        return byteBuf.readBytes(byteBuf.readableBytes()).array();
    }

    /**
     * 构造字段 Key,用来获取字段数据
     *
     * @param metakey0
     * @param field0
     * @return
     */
    @Deprecated
    public static byte[] genKey(byte[] metakey0, byte[] field0) {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(NS, DataType.SPLIT, metakey0, DataType.SPLIT, TYPE, DataType.SPLIT, field0);
        return byteBuf.readBytes(byteBuf.readableBytes()).array();
    }


    protected static byte[] parseHField(byte[] metakey0, byte[] value) throws RedisException {

        ByteBuf valueBuf = Unpooled.wrappedBuffer(value); //优化 零拷贝
        ByteBuf slice = valueBuf.slice(3 + metakey0.length, value.length - 3 - metakey0.length);

        return slice.readBytes(slice.readableBytes()).array();
    }

    /**
     * 合成 Value 直接存储
     *
     * @param value
     * @param expiration
     * @return
     */
    protected static byte[] genVal(byte[] value, long expiration) {
        ByteBuf ttlBuf = Unpooled.buffer(12);
        ttlBuf.writeLong(expiration); //ttl 无限期 -1
        ttlBuf.writeInt(value.length); //value size

        ByteBuf valueBuf = Unpooled.wrappedBuffer(value); //零拷贝
        ByteBuf valbuf = Unpooled.wrappedBuffer(ttlBuf, valueBuf);//零拷贝

        return valbuf.readBytes(valbuf.readableBytes()).array();
    }

    /**
     * 分解 Value,获取业务数据
     *
     * @param db
     * @param key0
     * @param values
     * @return
     * @throws RedisException
     */
    protected static byte[] parseValue(RocksDB db, byte[] key0, byte[] values) throws RedisException {
        if (values != null) {

            ByteBuf vvBuf = Unpooled.wrappedBuffer(values);
            vvBuf.resetReaderIndex();
            ByteBuf ttlBuf = vvBuf.readSlice(8);
            ByteBuf sizeBuf = vvBuf.readSlice(4);
            ByteBuf valueBuf = vvBuf.slice(8 + 4, values.length - 8 - 4);

            long ttl = ttlBuf.readLong();//ttl
            long size = sizeBuf.readInt();//长度数据

            //数据过期处理
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

    /**
     * 与keys 分开，减少内存占用；
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
                        cnt=cnt+1;
                    } else {
                        break;
                    }
                } else break;

            }
        }

        //检索过期数据,处理过期数据 ;暂不处理影响效率 fixme
//        System.out.println(keys.size());

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
//                        System.out.println(new String(iterator.key()));
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
//        System.out.println(keys.size());

        return keys;
    }

    /**
     * 返回 key-value 直对形式
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
//        System.out.println(keys.size());

        return keys;
    }

    /**
     * 主键存在则返回数据
     * @return
     */
    public static StringBuilder existsBy(RocksDB db,byte[] key)  {
        StringBuilder val=new StringBuilder();
        if (db.keyMayExist(key, val)){
            return null;
        }else return val;
    }

    protected static List<BulkReply> __mget(List<byte[]> listFds) throws RedisException {
        List<BulkReply> list = new ArrayList<BulkReply>();

        try {
            Map<byte[], byte[]> fvals = RocksdbRedis.mydata.multiGet(listFds);
            for (byte[] fk : listFds
            ) {
                byte[] val = HashNode.parseValue(RocksdbRedis.mydata, fk, fvals.get(fk));
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

    protected static void __mput(byte[][] field_or_value1) {
        final WriteOptions writeOpt = new WriteOptions();
        final WriteBatch batch = new WriteBatch();
        for (int i = 0; i < field_or_value1.length; i += 2) {

            byte[] val = HashNode.genVal(field_or_value1[i + 1], -1);


            try {
                batch.put(field_or_value1[i], val);
                //fixme  通过 keys 获取数量
            } catch (RocksDBException e) {
                e.printStackTrace();
            }


        }
        try {

            RocksdbRedis.mydata.write(writeOpt, batch);

        } catch (RocksDBException e) {
            throw new RuntimeException(e.getMessage());
        }
    }


    public long getTtl() throws RedisException {
        return this.valBuf.getLong(0);
    }

    public int getSize() throws RedisException {
        return this.valBuf.getInt(8);
    }

    /**
     * getKey 一般是包含组合键；
     * getkey0 是纯粹的业务主键；
     * @return
     */
    public byte[] getKey() {
        keyBuf.resetReaderIndex();
        return keyBuf.readBytes(keyBuf.readableBytes()).array();
    }

    public byte[] getKey0()  {
        return key;
    }

    public String getKey0Str() throws RedisException {
        return new String(getKey0());
    }

    public byte[] getField0() throws RedisException {
        return field;
    }

    public String getField0Str() throws RedisException {
        return new String(getField0());
    }


    public void setField0(byte[] pval) throws RedisException {
        field = pval;
        keyBuf = Unpooled.wrappedBuffer(NS, key, TYPE, field);
    }


    public byte[] getVal() {
        valBuf.resetReaderIndex();
        return valBuf.readBytes(valBuf.readableBytes()).array();
    }

    /**
     * 参见setVal0 long+int 数据长度，拆分ttl,获取到实际的数据；
     * val 一般是包含ttl 的数据；val0是实际的业务数据
     * @return
     * @throws RedisException
     */
    public byte[] getVal0() throws RedisException {
        valBuf.resetReaderIndex();
        ByteBuf valueBuf = valBuf.slice(8 + 4, valBuf.readableBytes() - 8 - 4);
        //数据过期处理
        if (getTtl() < now() && getTtl() != -1) {
            try {
                db.delete(getKey());
                valBuf = null;
            } catch (RocksDBException e) {
                e.printStackTrace();
                throw new RedisException(e.getMessage());
            }
            return null;
        }
        return valueBuf.readBytes(valueBuf.readableBytes()).array();
    }

    public String getVal0Str() throws RedisException {
        return new String(getVal0());
    }

    public String info() throws RedisException {
        StringBuilder sb = new StringBuilder(getKey0Str());
        sb.append("|");
        sb.append(getField0Str());
        sb.append("=");
        sb.append(getVal0Str());

        sb.append(" , TTL =");
        sb.append(getTtl());

        sb.append(" , SIZE =");
        sb.append(getSize());

        System.out.println(sb.toString());

        return sb.toString();
    }


    public static void main(String[] args) throws Exception {
//
//        HashNode meta = new HashNode(RocksdbRedis.mydata, "HashTest".getBytes(), "f1".getBytes(), "value".getBytes());
        HashNode meta = HashNode.getInstance(RocksdbRedis.mydata, "redis".getBytes());
        meta.genKey1("HashTest".getBytes(), "f1".getBytes()).hset("value".getBytes());

        byte[] val1 = meta.genKey1("HashTest".getBytes(), "f1".getBytes()).hget().getVal0();
        System.out.println(":::::"+new String(val1));
        Assert.assertArrayEquals(val1, "value".getBytes());
        meta.info();

        Assert.assertEquals(meta.getSize(), 5);
        Assert.assertArrayEquals(meta.getField0(), "f1".getBytes());
        Assert.assertArrayEquals(meta.getKey0(), "HashTest".getBytes());
        Assert.assertArrayEquals(meta.getVal0(), "value".getBytes());

        meta.setKey0("abc".getBytes());
        meta.setField0("f2".getBytes());
        meta.setVal("v1".getBytes());

        Assert.assertArrayEquals(meta.getKey0(), "abc".getBytes());
        Assert.assertArrayEquals(meta.getField0(), "f2".getBytes());
        Assert.assertArrayEquals(meta.getVal0(), "v1".getBytes());

        meta.info();

        meta.flush();

//        HashNode meta1 = new HashNode(RocksdbRedis.mydata,"abc".getBytes(), "f2".getBytes());
        meta.genKey1("abc".getBytes(), "f2".getBytes()).hset("v2".getBytes());


        meta.info();

        Assert.assertArrayEquals(meta.getKey0(), "abc".getBytes());
        Assert.assertArrayEquals(meta.getField0(), "f2".getBytes());
        Assert.assertArrayEquals(meta.getVal0(), "v2".getBytes());

        byte[] val0 = meta.genKey1("abc".getBytes(), "f2".getBytes()).hget().getVal0();
        System.out.println(":::::"+new String(val0));
        Assert.assertArrayEquals(val0, "v2".getBytes());

    }


}
