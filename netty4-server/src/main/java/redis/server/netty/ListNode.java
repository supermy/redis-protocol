package redis.server.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import redis.server.netty.utis.DataType;

import static redis.server.netty.ListNode.Meta.*;


/**
 * List Meta 元素方便促常用操作
 *
 * value 格式： ttl|type|size|prev-next|value
 *
 * <p>
 *
 * Created by moyong on 2017/11/9.
 * Update by moyong on 2018/10/14。
 *
 */
public class ListNode extends  BaseNode{



    public boolean isFirst() throws RedisException {
        return getPseq()==-1;
    }

    public boolean isLast() throws RedisException {
        return getNseq()==-1;
    }

    enum Meta {
        KEY, SEQ, TTL, SIZE, PSEQ, NSEQ ,TYPE1
    }

    private static byte[] NS;
    private static byte[] TYPE = DataType.KEY_LIST_ELEMENT;

    private RocksDB db;

    private byte[] key;
    private long seq;
//    private byte[] val;

    private ByteBuf keyBuf;

//    private ByteBuf metaBuf; //ttl size
//    private ByteBuf linBuf;  //pseq nseq
    private ByteBuf valBuf;  //value

    public ByteBuf data() {
        return valBuf;
    }

    private ListNode() {
    }

    private static ListNode instance = new ListNode();

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
    public static ListNode getInstance(RocksDB db0, byte[] ns0) {
        instance.db = db0;
        instance.NS = ns0;
        return instance;
    }

    /**
     * 创建一个新的节点实例
     * @param db0
     * @param ns0
     * @return
     */
    public ListNode create() {
        ListNode ins = new ListNode();
        ins.db = db;
        ins.NS = NS;
        return ins;
    }

    /**
     * 构造 MetaKey
     *
     * @param key0
     * @return
     * @throws RedisException
     */
    public ListNode genKey(byte[] key0, long seq1) throws RedisException {
        if (key0 == null) {
            throw new RedisException(String.format("主键不能为空"));
        }

        this.key = key0;
        this.seq = seq1;
//        this.seq = RocksdbRedis.bytesToLong(seq1);

        ByteBuf preKeyBuf = Unpooled.wrappedBuffer(instance.NS, DataType.SPLIT, key0, DataType.SPLIT, TYPE, DataType.SPLIT);

        ByteBuf val0Buf = Unpooled.buffer(8);
        val0Buf.writeLong(seq);

        this.keyBuf = Unpooled.wrappedBuffer(preKeyBuf, val0Buf);

        return this;
    }

    /**
     * 持久化节点数据
     *
     * @param val0
     * @param pseq1
     * @param nseq2
     * @return
     * @throws RedisException
     */
    public ListNode put(byte[] val0, long pseq1, long nseq2) throws RedisException {
//        System.out.println(pseq1);
//        System.out.println(nseq2);
        this.valBuf = setVal(val0, pseq1, nseq2);
//        this.info();
        try {
            db.put(getKey(), getVal());

        } catch (RocksDBException e) {
            throw new RedisException(e.getMessage());
        }

        return this;

    }



    /**
     * 创建元素
     *
     * @param db0
     * @param key0
     * @param seq0
     * @param val0
     * @param pseq
     * @param nseq
     * @throws RedisException
     */
    @Deprecated
    public ListNode(RocksDB db0, byte[] key0, long seq0, byte[] val0, long pseq, long nseq) throws RedisException {

        this.db = db0;
        create(key0, seq0, val0, pseq, nseq);

    }

    @Deprecated
    protected void create(byte[] key0, long seq0, byte[] val0, long pseq, long nseq) throws RedisException {
        ByteBuf keyPreBuf = Unpooled.wrappedBuffer("_l".getBytes(), key0, "#".getBytes());

        ByteBuf val0Buf = Unpooled.buffer(8);
        val0Buf.writeLong(seq0);

        this.keyBuf = Unpooled.wrappedBuffer(keyPreBuf, val0Buf);
        this.key = keyBuf.readBytes(keyBuf.readableBytes()).array();

//        keyBuf.resetReaderIndex();
//        keyBuf.setLong(keyBuf.readableBytes() - 8, 99);


//        keyPreBuf.resetReaderIndex();
//
//        ByteBuf bb=keyBuf.slice(2,keyPreBuf.readableBytes()-3);
//
//        bb.resetReaderIndex();
//        System.out.println(new String(bb.readBytes(bb.readableBytes()).array()));


        ByteBuf valueBuf = setVal(val0, pseq, nseq);

        try {
            db.put(key, valueBuf.readBytes(valueBuf.readableBytes()).array());

        } catch (RocksDBException e) {
            throw new RedisException(e.getMessage());
        }
    }

//    public HashNode setVal1(byte[] val0, long ttl) throws RedisException {
//
//        ByteBuf ttlBuf = Unpooled.buffer(28);
//
//        ttlBuf.writeLong(ttl); //ttl 无限期 -1
//        ttlBuf.writeBytes(DataType.SPLIT);
//
//        ttlBuf.writeInt(DataType.VAL_HASH_FIELD); //value type
//        ttlBuf.writeBytes(DataType.SPLIT);
//
//        ttlBuf.writeInt(val0.length); //value size
//        ttlBuf.writeBytes(DataType.SPLIT);
//
//        ByteBuf val0Buf = Unpooled.wrappedBuffer(val0);
//        valBuf = Unpooled.wrappedBuffer(ttlBuf, val0Buf);//零拷贝
//
//        return this;
//    }

    /**
     * 初始化节点值
     *
     *  ttl|type|size|prev-next|value
     *
     * @param val0
     * @param pseq
     * @param nseq
     * @return
     */
    private ByteBuf setVal(byte[] val0, long pseq, long nseq) {

        ByteBuf ttlBuf = Unpooled.buffer(28);

        ttlBuf.writeLong(-1); //ttl 无限期 -1
        ttlBuf.writeBytes(DataType.SPLIT);


        ttlBuf.writeInt(DataType.VAL_LIST_ELEMENT); //value type
        ttlBuf.writeBytes(DataType.SPLIT);

        ttlBuf.writeInt(val0.length); //value size
        ttlBuf.writeBytes(DataType.SPLIT);

        ttlBuf.writeLong(pseq); //上一个元素
        ttlBuf.writeLong(nseq); //下
        ttlBuf.writeBytes(DataType.SPLIT);

//        this.metaBuf = ttlBuf.slice(0, 12);
//        this.linBuf = ttlBuf.slice(12, 16);

        ByteBuf val1Buf = Unpooled.wrappedBuffer(val0);

        ByteBuf valueBuf = Unpooled.wrappedBuffer(ttlBuf, val1Buf);//零拷贝

//        valueBuf.resetReaderIndex();

//        val = val0;
        return valueBuf;
    }


    /**
     *
     *  数据初始化
     *
     * @param db0
     * @param key0
     * @param seq
     * @throws RedisException
     */
    @Deprecated
    public ListNode(RocksDB db0, byte[] key0, long seq) throws RedisException {
        this.db = db0;

//        System.out.println(new String(key0));
//        System.out.println(seq);

        ByteBuf keyPreBuf = Unpooled.wrappedBuffer("_l".getBytes(), key0, "#".getBytes());
        ByteBuf keySeqBuf = Unpooled.buffer(8);
        keySeqBuf.writeLong(seq);
        this.keyBuf = Unpooled.wrappedBuffer(keyPreBuf, keySeqBuf);
        this.key = keyBuf.readBytes(keyBuf.readableBytes()).array();

        get();
    }

    public ListNode get() throws RedisException {
        try {
            byte[] values = db.get(getKey());

            if(values == null){
                this.valBuf=null;
                return null;
            }
            this.valBuf= Unpooled.wrappedBuffer(values);

        } catch (RocksDBException e) {
            e.printStackTrace();
            throw new RedisException(String.format("获取数据错误:key%s;%S",getKey0(),e.getStatus()));
        }
        return this;
    }

//    @Deprecated
    public void flush() throws RedisException {

//        ByteBuf ttlBuf = Unpooled.buffer(12);
//        ttlBuf.writeLong(getTtl()); //ttl 无限期 -1
//        ttlBuf.writeInt(getSize()); //value size
//
//        ttlBuf.writeLong(getPseq()); //ttl 无限期 -1
//        ttlBuf.writeLong(getNseq()); //value size


//        metaBuf.resetReaderIndex();
//        linBuf.resetReaderIndex();
//        valBuf.resetReaderIndex();
//        System.out.println("++++++++++++++");
//        System.out.println(getTtl());
//        System.out.println(getSize());
//        System.out.println(getPseq());
//        System.out.println(getNseq());
//        System.out.println("++++++++++++++");
//        System.out.println(metaBuf);
//        System.out.println(linBuf);

//        metaBuf.retain();
//        linBuf.retain();
//        ByteBuf ttlBuf=Unpooled.wrappedBuffer(metaBuf,linBuf);

//        valBuf.resetReaderIndex();
//
//        ByteBuf valueBuf = Unpooled.wrappedBuffer(ttlBuf, this.valBuf);//零拷贝


        try {
//            db.put(key, valueBuf.readBytes(valueBuf.readableBytes()).array());
            db.put(getKey(), getVal());
        } catch (RocksDBException e) {
            throw new RedisException(e.getMessage());
        }
    }

    @Deprecated
    public void destory() throws RedisException {
        try {
            db.delete(getKey());
        } catch (RocksDBException e) {
            throw new RedisException(e.getMessage());
        }
    }

    public void del() throws RedisException {
        try {
            db.delete(getKey());
        } catch (RocksDBException e) {
            throw new RedisException(e.getMessage());
        }
    }

    @Deprecated
    public void sync() throws RedisException {
        get();
    }


    public long getPseq() throws RedisException {
        return get(PSEQ);
    }

    public ListNode prev() throws RedisException {
        if(getPseq() == -1){
            return null;
        }

        return create().genKey(getKey0(),getPseq()).get();

//        return genKey(getKey0(),getPseq()).get();
    }

    public ListNode setPseq(long val) throws RedisException {
        set(PSEQ, val);
        return this;
    }

    //@Deprecated

    /**
     * 更改字段值
     * @param val0
     * @throws RedisException
     */
    public void setVal(byte[] val0) throws RedisException {

        int indexVal = 8 + 4 + 4 + 8 + 8 + 4;

        ByteBuf ttlBuf = this.valBuf.slice(0,indexVal);
        ByteBuf val1Buf = Unpooled.wrappedBuffer(val0);
        valBuf= Unpooled.wrappedBuffer(ttlBuf, val1Buf);//零拷贝

    }

    public long getNseq() throws RedisException {
        return get(NSEQ);
    }

    public ListNode next() throws RedisException {
        if(getNseq() == -1){

            return null;
        }
//        return genKey(getKey0(),getNseq()).get();
        return create().genKey(getKey0(),getNseq()).get();

//        create().genKey(getKey0(),getPseq()).get();
    }

    public ListNode setNseq(long val) throws RedisException {
        set(NSEQ, val);
        return this;
    }


    public long getSeq() throws RedisException {
//        keyBuf.resetReaderIndex();
        return get(SEQ);
    }

    public long getSeq0() throws RedisException {
        return seq;
    }

    public String getSeq0Str() throws RedisException {
        return getSeq0()+"";
    }


    @Deprecated
    public void setSeq(long pval) throws RedisException {

        seq=pval;

        keyBuf.resetReaderIndex();
//        System.out.println(String.format("key buf length:%d", keyBuf.readableBytes()));
//        keyBuf.setLong(keyBuf.readableBytes() - 8, pval);
        set(SEQ,pval);
    }

//    public byte[] getKey0() throws RedisException {
//
//        keyBuf.resetReaderIndex();
//        ByteBuf bb = keyBuf.slice(2, keyBuf.readableBytes() - 3 - 8);
////        System.out.println(new String(bb.readBytes(bb.readableBytes()).array()));
//        return bb.readBytes(bb.readableBytes()).array();
//    }

    public byte[] getKey0() {
        return key;
    }

    public String getKey0Str() throws RedisException {
        return new String(getKey0());
    }



    @Deprecated
    public void setKey0(byte[] key0) throws RedisException {
        keyBuf.resetReaderIndex();
        ByteBuf b1 = keyBuf.slice(0, 2);
        ByteBuf b3 = keyBuf.slice(keyBuf.readableBytes() - 1 - 8, 1);
        ByteBuf b4 = keyBuf.slice(keyBuf.readableBytes() - 8, 8);

        ByteBuf b2 = Unpooled.wrappedBuffer(key0);
        keyBuf = Unpooled.wrappedBuffer(b1, b2, b3, b4);
        key = keyBuf.readBytes(keyBuf.readableBytes()).array();

//        System.out.println(new String(b1.readBytes(b1.readableBytes()).array()));
//        System.out.println(new String(b3.readBytes(b3.readableBytes()).array()));
//        System.out.println(b4.readLong());

    }


    /**
     *
     * 获取指针数据
     *
     * @param fd
     * @return
     */
    private long get(Meta fd) throws RedisException {
        long result = 0;

        keyBuf.resetReaderIndex();
        valBuf.resetReaderIndex();

        switch (fd) {
            case SEQ:

               // ByteBuf preKeyBuf = Unpooled.wrappedBuffer
                // (instance.NS, DataType.SPLIT, key0, DataType.SPLIT, TYPE, DataType.SPLIT);

                int preKeySize = NS.length + key.length + TYPE.length + DataType.SPLIT.length * 3;
//                System.out.println("pre key size:"+preKeySize);
//                print(keyBuf);
//                keyBuf.resetReaderIndex();
                ByteBuf bb = keyBuf.slice(preKeySize, keyBuf.readableBytes() - preKeySize);
//                System.out.println(bb.getLong(0));
//
//                result = keyBuf.getLong(keyBuf.readableBytes()-preKeySize);
//                System.out.println(result);

                result =bb.getLong(0) ;

                break;

            case TTL:

                result = this.valBuf.getLong(0);
                break;

            case TYPE1:

                result = this.valBuf.getInt(8+1);
                break;

            case SIZE:

                result = this.valBuf.getInt(8+4+2);
                break;

            case PSEQ:

                result = this.valBuf.getLong(8+4+4+3);
                break;

            case NSEQ:
                result = this.valBuf.getLong(8+4+4+3+8);
                break;

            default:
                System.out.println("default");
                throw new RedisException(String.format("没有如此的字段:%s", fd));
        }
        return result;
    }

    /**
     * 设置指针数据
     * @param fd
     * @param pval
     * @return
     * @throws RedisException
     */
    private long set(Meta fd, long pval) throws RedisException {
        long result = 0;

        keyBuf.resetReaderIndex();
        valBuf.resetReaderIndex();

        switch (fd) {

            case SEQ:
                int preKeySize = NS.length + key.length + TYPE.length + DataType.SPLIT.length * 3;
                keyBuf.setLong(preKeySize, pval);
                break;

            case TTL:
                valBuf.setLong(0, pval);
                break;

            case TYPE1:
                valBuf.setInt(8+1, (int) pval);
                break;

            case SIZE:
                valBuf.setInt(8+4+2, (int) pval);
                break;

            case PSEQ:
                valBuf.setLong(8+4+4+3, pval);
                break;

            case NSEQ:
                valBuf.setLong(8+4+4+3+8, pval);
                break;


            default:
                System.out.println("default");
                throw new RedisException(String.format("没有如此的字段:%s", fd));
        }
        return result;
    }


    private long now() {
        return System.currentTimeMillis();
    }


    public long getTtl() throws RedisException {
        return get(TTL);
    }

    public int getSize() throws RedisException {
        return (int) get(SIZE);
    }

    public int getType() throws RedisException {
        return (int) get(Meta.TYPE1);
    }

    public byte[] getKey() {
        keyBuf.resetReaderIndex();
        return keyBuf.readBytes(keyBuf.readableBytes()).array();
    }

    public byte[] getVal() {
        valBuf.resetReaderIndex();
        return valBuf.readBytes(valBuf.readableBytes()).array();
    }



    public byte[] getVal0() {

//        print(valBuf);

        this.valBuf.resetReaderIndex();
        int indexVal = 8 + 4 + 4 + 8 + 8 + 4;
        ByteBuf valueBuf = valBuf.slice(indexVal, valBuf.readableBytes() - indexVal);
        return valueBuf.readBytes(valueBuf.readableBytes()).array();

    }

    public String getVal0Str() {

        return new String(getVal0());

    }



    /**
     * 节点数据信息
     *
     * @return
     * @throws RedisException
     */
    public String info() throws RedisException {
        StringBuilder sb = new StringBuilder(getKey0Str());
        sb.append("|");
        sb.append(getSeq0());
        sb.append("=");
        sb.append(getVal0Str());

        sb.append(" , TTL =");
        sb.append(getTtl());

        sb.append(" , TYPE =");
        sb.append(get(Meta.TYPE1));

        sb.append(" , SIZE =");
        sb.append(getSize());

        sb.append(" , pseq =");
        sb.append(getPseq());

        sb.append(" , nseq =");
        sb.append(getNseq());

        System.out.println(sb.toString());

        return sb.toString();
    }


    public static void main(String[] args) throws Exception {

//        ListNode meta =  ListNode.getInstance(RocksdbRedis.mydata, "ListTest".getBytes(), 0, "value".getBytes(), -1, -1);
        ListNode meta =  ListNode.getInstance(RocksdbRedis.mydata, "redis".getBytes());
        meta.genKey( "ListTest".getBytes(),0).put("value".getBytes(), -1, -1);

        Assert.assertArrayEquals(meta.getKey0(),"ListTest".getBytes());
        Assert.assertEquals(meta.getSeq(),0);
        Assert.assertEquals(meta.getSeq0(),0);

        Assert.assertEquals(meta.getTtl(),-1);
        Assert.assertEquals(meta.getType(),DataType.VAL_LIST_ELEMENT);
        Assert.assertEquals(meta.getSize(),"value".getBytes().length);

        Assert.assertEquals(meta.getNseq(),-1);
        Assert.assertEquals(meta.getPseq(),-1);

        Assert.assertArrayEquals(meta.getVal0(),"value".getBytes());

        meta.info();

//        System.out.println(new String(meta.getKey0()));
//        System.out.println(String.format("seq: %d", meta.getSeq()));

//        meta.setKey0("abc".getBytes());
//        System.out.println(new String(meta.getKey0()));
        meta.setSeq(12);
        meta.setNseq(3);
        meta.setPseq(4);

        Assert.assertEquals(meta.getSeq(),12);
        Assert.assertEquals(meta.getSeq0(),12);

        Assert.assertEquals(meta.getNseq(),3);
        Assert.assertEquals(meta.getPseq(),4);

        Assert.assertArrayEquals(meta.getKey0(), "ListTest".getBytes());

        meta.info();

        meta.flush();


        ListNode meta1 =  ListNode.getInstance(RocksdbRedis.mydata, "redis".getBytes());

        meta1.genKey( "ListTest".getBytes(),12).get();

        meta1.info();

        meta1.genKey( "ListTest".getBytes(),0).get();

        meta1.info();


    }

}
