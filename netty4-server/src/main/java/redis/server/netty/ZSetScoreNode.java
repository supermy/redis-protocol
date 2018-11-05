package redis.server.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import redis.server.netty.utis.DataType;



/**
 * Hash Node 元素方便促常用操作
 *
 * <p>
 *         Sorted Set   [<ns>] <key> KEY_META                 KEY_ZSET <MetaObject>
 *                      [<ns>] <key> KEY_ZSET_SCORE <member>  KEY_ZSET_SCORE <score>
 *                      [<ns>] <key> KEY_ZSET_SORT <score> <member> KEY_ZSET_SORT
 * </p>
 * key and value 都采用 | 分隔符号
 * * getKey 一般是包含组合键；
 * * getkey0 是纯粹的业务主键；
 * * 参见setVal0 long+int+int ttl,数据类型,数据长度；
 * * val 一般是包含ttl 的数据；val0是实际的业务数据
 *
 * <p>
 * Created by moyong on 2018/11/1.
 *
 * zadd zscore zrank zrem
 * </p>
 */
public class ZSetScoreNode extends BaseNode{

    private static Logger log = Logger.getLogger(ZSetScoreNode.class);


    private static byte[] NS;
    private static byte[] TYPE = DataType.KEY_ZSET_SCORE;

    private RocksDB db;

    private byte[] key; //冗余缓存多拷贝一次 fixme 优化性能使用 keyBuf
    private byte[] member;

    private ByteBuf keyBuf;
    private ByteBuf valBuf;

    public ByteBuf data() {
        return valBuf;
    }

    public boolean isEmpty() {
        return valBuf==null || valBuf.capacity()==0;
    }

    private ZSetScoreNode() {
    }

    private static ZSetScoreNode instance = new ZSetScoreNode();

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
    public static ZSetScoreNode getInstance(RocksDB db0, byte[] ns0) {
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
    public ZSetScoreNode genKey(byte[] key0, byte[] member1) throws RedisException {
        if (key0 == null) {
            throw new RedisException(String.format("主键不能为空"));
        }

        this.key = key0;
        this.member = member1;

        instance.keyBuf = Unpooled.wrappedBuffer(instance.NS, DataType.SPLIT, key0, DataType.SPLIT, TYPE, DataType.SPLIT, member1);
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

    public byte[] getMember0() throws RedisException {
        //fixme 从硬盘载入的数据
        return member;
    }

    public String getMember0Str() throws RedisException {
        return new String(getMember0());
    }


    @Deprecated
    public void setMember0(byte[] pval) throws RedisException {
        member = pval;
        keyBuf = Unpooled.wrappedBuffer(NS, DataType.SPLIT, key, DataType.SPLIT, TYPE, DataType.SPLIT, member);
    }


    public byte[] getVal() {
        valBuf.resetReaderIndex();
        return valBuf.readBytes(valBuf.readableBytes()).array();
    }


    public ZSetScoreNode setVal(byte[] score,long ttl) throws RedisException {

        ByteBuf ttlBuf = Unpooled.buffer(28);

        ttlBuf.writeLong(ttl); //ttl 无限期 -1
        ttlBuf.writeBytes(DataType.SPLIT);

        ttlBuf.writeInt(DataType.VAL_ZSET_SCORE); //value type
        ttlBuf.writeBytes(DataType.SPLIT);

        ttlBuf.writeInt(score.length); //value size
        ttlBuf.writeBytes(DataType.SPLIT);

        ByteBuf val0Buf = Unpooled.wrappedBuffer(score);

        valBuf = Unpooled.wrappedBuffer(ttlBuf, val0Buf);//零拷贝


        return this;
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
//        if (getTtl() < now() && getTtl() != -1) {
//            try {
//                db.delete(getKey());
//                valBuf = null;
//            } catch (RocksDBException e) {
//                e.printStackTrace();
//                throw new RedisException(e.getMessage());
//            }
//            return null;
//        }
        return valueBuf.readBytes(valueBuf.readableBytes()).array();
    }

    public String getVal0Str() throws RedisException {
        return new String(getVal0());
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


    protected byte[] parseMember(byte[] metakey0, byte[] value) throws RedisException {

        ByteBuf valueBuf = Unpooled.wrappedBuffer(value); //优化 零拷贝
        //get field0 name
        int index = NS.length + metakey0.length + TYPE.length + 3;
        int length = value.length - NS.length - metakey0.length - TYPE.length - 3;

        ByteBuf slice = valueBuf.slice(index, length);//fixme

        return slice.readBytes(slice.readableBytes()).array();
    }


    private static long now() {
        return System.currentTimeMillis();
    }



    /**
     *
     * Redis Zadd 命令用于将一个或多个成员元素及其分数值加入到有序集当中。
     *
     * 如果某个成员已经是有序集的成员，那么更新这个成员的分数值，并通过重新插入这个成员元素，来保证该成员在正确的位置上。
     *
     * 分数值可以是整数值或双精度浮点数。
     *
     * 如果有序集合 key 不存在，则创建一个空的有序集并执行 ZADD 操作。
     *
     * 当 key 存在但不是有序集类型时，返回一个错误。
     *
     *
     * @param val0
     * @throws RedisException
     */
    protected void zadd(byte[] score) throws RedisException {

        setVal(score,-1);

        try {
            db.put(getKey(), getVal());
        } catch (RocksDBException e) {
            throw new RedisException(e.getMessage());
        }

    }


    public ZSetScoreNode zscore() throws RedisException {
        try {

            byte[] values = db.get(getKey());

            if (values == null) {
                valBuf = null;
                return this;
            }

            valBuf = Unpooled.wrappedBuffer(values);
            valBuf.resetReaderIndex();

            return this;

        } catch (RocksDBException e) {
            e.printStackTrace();
            throw new RedisException(e.getMessage());
        }
    }


    public ZSetScoreNode zrem() throws RedisException {
        try {

//            log.debug(new String(getKey()));

            db.delete(getKey());
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
     *
     * db.keyMayExist fixme
     *
     */
    public boolean exists() {
//        StringBuilder val = new StringBuilder();

        byte[] obj = new byte[0];
        try {
            obj = db.get(getKey());
        } catch (RocksDBException e) {
            e.printStackTrace();
        }

        if (obj == null){
            return false;
        }else return true;
//
//        log.debug(new String(getKey()));
//
//        if (db.keyMayExist(getKey(), val)) { //从缓存数据块中返回值数据不全不能使用
//            return true;
//        } else return false;


    }



    /**
     *
     * @param members
     * @throws RedisException
     */
    protected void zadd(byte[][] members) throws RedisException {
        final WriteOptions writeOpt = new WriteOptions();
        final WriteBatch batch = new WriteBatch();

        try {
            for (byte[] member:members
                 ) {
                byte[] val = setVal("8".getBytes(),-1).getVal();//fixme
//                log.debug(String.format("key:%s,value:%s", new String(member),new String(val)));
                batch.put(member, val);
            }

            db.write(writeOpt, batch);
        } catch (RocksDBException e) {
            throw new RuntimeException(e.getMessage());
        }
    }


    public String info() throws RedisException {
        StringBuilder sb = new StringBuilder(getKey0Str());
        sb.append("|");
        sb.append(getMember0Str());
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

        ZSetScoreNode meta = ZSetScoreNode.getInstance(RocksdbRedis.mydata, "redis".getBytes());

        meta.genKey("ZSetTest".getBytes(), "f1".getBytes()).zadd("8".getBytes());
        boolean exists = meta.genKey("ZSetTest".getBytes(), "f1".getBytes()).exists();
        System.out.println(exists);
        Assert.assertTrue(exists);
        ZSetScoreNode zscore = meta.genKey("ZSetTest".getBytes(), "f1".getBytes()).zscore();
        log.debug(new String(zscore.getVal()));
        log.debug(new String(zscore.getVal0()));
        Assert.assertArrayEquals(zscore.getVal0(),"8".getBytes());

        meta.genKey("ZSetTest".getBytes(), "f1".getBytes()).zrem();
        exists=meta.genKey("ZSetTest".getBytes(), "f1".getBytes()).exists();
        System.out.println(exists);
        Assert.assertFalse(exists);


    }


}
