package redis.server.netty.rocksdb;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.log4j.Logger;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import redis.server.netty.RedisException;
import redis.server.netty.utis.DataType;


import java.util.Random;



/**
 * Hash Node 元素方便促常用操作
 *
 * <p>
 * Set      [<ns>] <key> KEY_META                 KEY_SET <MetaObject>
 *          [<ns>] <key> KEY_SET_MEMBER <member>  KEY_SET_MEMBER
 * </p>
 * key and value 都采用 | 分隔符号
 * * getKey 一般是包含组合键；
 * * getkey0 是纯粹的业务主键；
 * * 参见setVal0 long+int+int ttl,数据类型,数据长度；
 * * val 一般是包含ttl 的数据；val0是实际的业务数据
 *
 * <p>
 * Created by moyong on 2018/10/25.
 *
 * zadd
 * </p>
 */
public class SetNode extends BaseNode{

    private static Logger log = Logger.getLogger(SetNode.class);


    private static byte[] NS;
    private static byte[] TYPE = DataType.KEY_SET_MEMBER;

    private RocksDB db;

    private byte[] key; //冗余缓存多拷贝一次 fixme 优化性能使用 keyBuf
    private byte[] member;

    private ByteBuf keyBuf;
    private ByteBuf valBuf;

    public ByteBuf data() {
        return valBuf;
    }

    private SetNode() {
    }

    private static SetNode instance = new SetNode();

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
    public static SetNode getInstance(RocksDB db0, byte[] ns0) {
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
    public SetNode genKey1(byte[] key0, byte[] member1) throws RedisException {
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




    public SetNode setVal(long ttl) throws RedisException {

        ByteBuf ttlBuf = Unpooled.buffer(28);

        ttlBuf.writeLong(ttl); //ttl 无限期 -1
        ttlBuf.writeBytes(DataType.SPLIT);

        ttlBuf.writeInt(DataType.VAL_SET_MEMBER); //value type
//        ttlBuf.writeBytes(DataType.SPLIT);

//        ttlBuf.writeInt(val0.length); //value size
//        ttlBuf.writeBytes(DataType.SPLIT);
//
//        ByteBuf val0Buf = Unpooled.wrappedBuffer(val0);
//        valBuf = Unpooled.wrappedBuffer(ttlBuf, val0Buf);//零拷贝
        valBuf = ttlBuf;

        return this;
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
        ByteBuf slice = valueBuf.slice(NS.length + metakey0.length + TYPE.length + 3, value.length - NS.length - metakey0.length - TYPE.length - 3);//fixme

        return slice.readBytes(slice.readableBytes()).array();
    }


    private static long now() {
        return System.currentTimeMillis();
    }



    /**
     * redis hash元素不支持ttl
     *
     * @param val0
     * @throws RedisException
     */
    protected void sadd() throws RedisException {

        setVal(-1);

        try {
            db.put(getKey(), getVal());
        } catch (RocksDBException e) {
            throw new RedisException(e.getMessage());
        }

    }



    public SetNode srem() throws RedisException {
        try {

            log.debug(new String(getKey()));

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
        StringBuilder val = new StringBuilder();

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
    protected void sadd(byte[][] members) throws RedisException {
        final WriteOptions writeOpt = new WriteOptions();
        final WriteBatch batch = new WriteBatch();

        try {
            for (byte[] member:members
                 ) {
                byte[] val = setVal(-1).getVal();
                log.debug(String.format("key:%s,value:%s", new String(member),new String(val)));
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
       // sb.append("=");
       // sb.append(getVal0Str());

        sb.append(" , TTL =");
        sb.append(getTtl());

        sb.append(" , SIZE =");
        sb.append(getSize());

        log.debug(sb.toString());

        return sb.toString();
    }


    public static void main(String[] args) throws Exception {

        Random random = new Random();
        System.out.println(random.nextInt(41) + 10);//随机生成[10, 50]之间的随机数。

        for (int i = 0; i <20 ; i++) {
            System.out.println(random.nextInt(15) );
        }


        SetNode meta = SetNode.getInstance(RocksdbRedis.mydata, "redis".getBytes());

        meta.genKey1("SetTest".getBytes(), "f1".getBytes()).sadd();
        boolean exists = meta.genKey1("SetTest".getBytes(), "f1".getBytes()).exists();
        System.out.println(exists);

        meta.genKey1("SetTest".getBytes(), "f1".getBytes()).srem();
        exists=meta.genKey1("SetTest".getBytes(), "f1".getBytes()).exists();
        System.out.println(exists);


    }


}
