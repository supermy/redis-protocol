package redis.server.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import redis.netty4.BulkReply;
import redis.server.netty.utis.DataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static redis.netty4.BulkReply.NIL_REPLY;


/**
 * Hash Node 元素方便促常用操作
 *
 * <p>
 * Hash    [<ns>] <key> KEY_META                 KEY_HASH <MetaObject>
 * [<ns>] <key> KEY_HASH_FIELD <field>   KEY_HASH_FIELD <field-value>
 * </p>
 * key and value 都采用 | 分隔符号
 * * getKey 一般是包含组合键；
 * * getkey0 是纯粹的业务主键；
 * * 参见setVal0 long+int+int ttl,数据类型,数据长度；
 * * val 一般是包含ttl 的数据；val0是实际的业务数据
 *
 * <p>
 * Created by moyong on 2017/11/23.
 * Updated by moyong on 2018/09/24
 * zadd hget
 * </p>
 */
public class HashNode extends BaseNode{

    private static Logger log = Logger.getLogger(HashNode.class);


    private static byte[] NS;
    private static byte[] TYPE = DataType.KEY_HASH_FIELD;

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


    public long getTtl() throws RedisException {
        return this.valBuf.getLong(0);
    }

    public int getType() throws RedisException {
        return this.valBuf.getInt(8 + 1);
    }

    public int getSize() throws RedisException {
        return this.valBuf.getInt(8 + 4 + 2);
    }

    /**
     * getKey 一般是包含组合键；
     * getkey0 是纯粹的业务主键；
     *
     * @return
     */
    public byte[] getKey() {
        keyBuf.resetReaderIndex();
        return keyBuf.readBytes(keyBuf.readableBytes()).array();
    }

    public byte[] getKey0() {
        return key;
    }

    public String getKey0Str() throws RedisException {
        return new String(getKey0());
    }

    public byte[] getField0() throws RedisException {
        //fixme 从硬盘载入的数据
        return field;
    }

    public String getField0Str() throws RedisException {
        return new String(getField0());
    }


    @Deprecated
    public void setField0(byte[] pval) throws RedisException {
        field = pval;
        keyBuf = Unpooled.wrappedBuffer(NS, DataType.SPLIT, key, DataType.SPLIT, TYPE, DataType.SPLIT, field);
    }


    public byte[] getVal() {
        valBuf.resetReaderIndex();
        return valBuf.readBytes(valBuf.readableBytes()).array();
    }

    /**
     * 参见setVal0 long+int 数据长度，拆分ttl,获取到实际的数据；
     * val 一般是包含ttl 的数据；val0是实际的业务数据
     *
     * @return
     * @throws RedisException
     */
    public byte[] getVal0() throws RedisException {
        valBuf.resetReaderIndex();
        ByteBuf valueBuf = valBuf.slice(8 + 4 + 4 + 3, valBuf.readableBytes() - 8 - 4 - 4 - 3);
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


    public HashNode setVal(byte[] val0, long ttl) throws RedisException {

        ByteBuf ttlBuf = Unpooled.buffer(28);

        ttlBuf.writeLong(ttl); //ttl 无限期 -1
        ttlBuf.writeBytes(DataType.SPLIT);

        ttlBuf.writeInt(DataType.VAL_HASH_FIELD); //value type
        ttlBuf.writeBytes(DataType.SPLIT);

        ttlBuf.writeInt(val0.length); //value size
        ttlBuf.writeBytes(DataType.SPLIT);

        ByteBuf val0Buf = Unpooled.wrappedBuffer(val0);
        valBuf = Unpooled.wrappedBuffer(ttlBuf, val0Buf);//零拷贝

        return this;
    }

    public byte[] parseValue0(byte[] values) throws RedisException {

        if (values != null) {

            ByteBuf vvBuf = Unpooled.wrappedBuffer(values);
            vvBuf.resetReaderIndex();

            ByteBuf valueBuf = vvBuf.slice(8 + 4 + 4 + 3, values.length - 8 - 4 - 4 - 3);

            return valueBuf.readBytes(valueBuf.readableBytes()).array();

        } else return null;

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
    @Deprecated
    protected static byte[] parseValue(RocksDB db, byte[] key0, byte[] values) throws RedisException {
        if (values != null) {

            ByteBuf vvBuf = Unpooled.wrappedBuffer(values);
            vvBuf.resetReaderIndex();
            ByteBuf ttlBuf = vvBuf.readSlice(8);
            ByteBuf typeBuf = vvBuf.slice(8 + 1, 4);
            ByteBuf sizeBuf = vvBuf.slice(8 + 4 + 2, 4);

            ByteBuf valueBuf = vvBuf.slice(8 + 4 + 4 + 3, values.length - 8 - 4 - 4 - 3);

            long ttl = ttlBuf.readLong();//ttl
            long size = sizeBuf.readInt();//长度数据

            //数据过期处理 todo 暂不元素过期处理，简化逻辑
//            if (ttl < now() && ttl != -1) {
//                try {
//                    db.del(key0);
////                    valueBuf = null;
//                } catch (RocksDBException e) {
//                    e.printStackTrace();
//                    throw new RedisException(e.getMessage());
//                }
//                return null;
//            }

            return valueBuf.readBytes(valueBuf.readableBytes()).array();

        } else return null; //数据不存在 ？ 测试验证
    }


    /**
     * 构造模式 Key, 用来遍历元素；
     * 使用了元素的类型
     *
     * @return
     */
    public byte[] genKeyPartten() {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(NS, DataType.SPLIT, getKey0(), DataType.SPLIT, TYPE, DataType.SPLIT);
        return byteBuf.readBytes(byteBuf.readableBytes()).array();
    }


    protected byte[] parseHField(byte[] metakey0, byte[] value) throws RedisException {

        ByteBuf valueBuf = Unpooled.wrappedBuffer(value); //优化 零拷贝
        //get field0 name
        ByteBuf slice = valueBuf.slice(NS.length + metakey0.length + TYPE.length + 3, value.length - NS.length - metakey0.length - TYPE.length - 3);//fixme

        return slice.readBytes(slice.readableBytes()).array();
    }


    private static long now() {
        return System.currentTimeMillis();
    }


    @Override
    public int hashCode() {
        return super.hashCode();
    }

    /**
     * redis hash元素不支持ttl
     *
     * @param val0
     * @throws RedisException
     */
    protected void hset(byte[] val0) throws RedisException {

        setVal(val0, -1);

        try {
            db.put(getKey(), getVal());
        } catch (RocksDBException e) {
            throw new RedisException(e.getMessage());
        }

    }


    public HashNode hget() throws RedisException {
        try {

            byte[] values = db.get(getKey());

            if (values == null) {
                valBuf = null;
                return null;
            }

            valBuf = Unpooled.wrappedBuffer(values);
            valBuf.resetReaderIndex();

            //数据过期处理 fixme 元素不支持过期，可否作为磁盘缓存增强功能特色；
//            if (getTtl() < now() && getTtl() != -1) {
//                db.del(getKey());
//                valBuf = null;
//                return null;
//            }

            return this;

        } catch (RocksDBException e) {
            e.printStackTrace();
            throw new RedisException(e.getMessage());
        }
    }


    public HashNode hdel() throws RedisException {
        try {

            db.delete(getKey());//todo 没有元素，清空meta
            valBuf = null;
            keyBuf = null;
            return this;

        } catch (RocksDBException e) {
            e.printStackTrace();
            throw new RedisException(e.getMessage());
        }
    }


    /**
     * 主键是否存在
     */
    public boolean exists() {
        StringBuilder val = new StringBuilder();
        if (db.keyMayExist(getKey(), val)) { //从缓存数据块中返回值数据不全不能使用
            return true;
        } else return false;
    }


    protected List<BulkReply> hmget(List<byte[]> listFds) throws RedisException {
        List<BulkReply> list = new ArrayList<BulkReply>();

        try {
            Map<byte[], byte[]> fvals = db.multiGet(listFds);
            for (byte[] fk : listFds
            ) {
                byte[] val = parseValue0(fvals.get(fk));
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

    protected void hmput(byte[][] field_or_value1) throws RedisException {
        final WriteOptions writeOpt = new WriteOptions();
        final WriteBatch batch = new WriteBatch();
        try {
            for (int i = 0; i < field_or_value1.length; i += 2) {

                byte[] val = setVal(field_or_value1[i + 1], -1).getVal();
//            try {
                batch.put(field_or_value1[i], val);
                //fixme  通过 keys 获取数量
//            } catch (RocksDBException e) {
//                e.printStackTrace();
//            }
            }
            db.write(writeOpt, batch);
        } catch (RocksDBException e) {
            throw new RuntimeException(e.getMessage());
        }
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

        log.debug(sb.toString());

        return sb.toString();
    }


    public static void main(String[] args) throws Exception {
//
        HashNode meta = HashNode.getInstance(RocksdbRedis.mydata, "redis".getBytes());
        meta.genKey1("HashTest".getBytes(), "f1".getBytes()).hset("value".getBytes());

        byte[] val1 = meta.genKey1("HashTest".getBytes(), "f1".getBytes()).hget().getVal0();
        log.debug(":::::" + new String(val1));
        Assert.assertArrayEquals(val1, "value".getBytes());
        meta.info();

        Assert.assertEquals(meta.getSize(), 5);
        Assert.assertArrayEquals(meta.getField0(), "f1".getBytes());
        Assert.assertArrayEquals(meta.getKey0(), "HashTest".getBytes());
        Assert.assertArrayEquals(meta.getVal0(), "value".getBytes());

        meta.setField0("f2".getBytes());
        meta.setVal("v1".getBytes(), -1);

        Assert.assertArrayEquals(meta.getField0(), "f2".getBytes());
        Assert.assertArrayEquals(meta.getVal0(), "v1".getBytes());

        meta.info();


        meta.genKey1("abc".getBytes(), "f2".getBytes()).hset("v2".getBytes());


        meta.info();

        Assert.assertArrayEquals(meta.getKey0(), "abc".getBytes());
        Assert.assertArrayEquals(meta.getField0(), "f2".getBytes());
        Assert.assertArrayEquals(meta.getVal0(), "v2".getBytes());

        byte[] val0 = meta.genKey1("abc".getBytes(), "f2".getBytes()).hget().getVal0();
        log.debug(":::::" + new String(val0));
        Assert.assertArrayEquals(val0, "v2".getBytes());

    }


}
