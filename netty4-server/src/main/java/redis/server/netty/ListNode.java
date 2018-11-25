package redis.server.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import redis.server.netty.utis.DataType;

import static redis.server.netty.ListNode.Meta.*;


/**
 * List Meta 元素方便促常用操作
 * <p>
 * value 格式： ttl|type|size|prev-next|value
 *
 * <p>
 * <p>
 * Created by moyong on 2017/11/9.
 * Update by moyong on 2018/10/22。
 */
public class ListNode extends BaseNode {

    private static Logger log = Logger.getLogger(ListNode.class);


    public boolean isFirst() throws RedisException {
        return getPseq() == -1;
    }

    public boolean isLast() throws RedisException {
        return getNseq() == -1;
    }

    enum Meta {
        KEY, SEQ, TTL, SIZE, PSEQ, NSEQ, TYPE1
    }

    private static byte[] NS;
    private static byte[] TYPE = DataType.KEY_LIST_ELEMENT;

    private RocksDB db;

    private byte[] key;
    private long seq;

    private ByteBuf keyBuf;
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
     *
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
     * @param pseq1 暂时无用
     * @param nseq2 暂时无用
     * @return
     * @throws RedisException
     */
    public ListNode put(byte[] val0, long pseq1, long nseq2) throws RedisException {
        this.valBuf = setVal(val0, pseq1, nseq2);
        try {

            log.debug(new String(getKey()));
            log.debug(new String(getVal()));

            db.put(getKey(), getVal());

        } catch (RocksDBException e) {
            throw new RedisException(e.getMessage());
        }

        return this;

    }


    /**
     * 初始化节点值
     * <p>
     * ttl|type|size|prev-next|value
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

//        log.debug(toString(ttlBuf));
//        log.debug(toString(val1Buf));
//        log.debug(toString(valueBuf));

        return valueBuf;
    }

    public byte[] toArray(ByteBuf buf){
     return buf.readBytes(buf.readableBytes()).array();
    }

    public String toString(byte[] byt){
        return new String(byt);
    }
    public String toString(ByteBuf buf){
        return toString(toArray(buf));
    }
    /**
     * 数据初始化
     *
     * @param db0
     * @param key0
     * @param seq
     * @throws RedisException
     */
    @Deprecated
    public ListNode(RocksDB db0, byte[] key0, long seq) throws RedisException {
        this.db = db0;

//        log.debug(new String(key0));
//        log.debug(seq);

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

            if (values == null) {
                this.valBuf = null;
                return null;
            }
            this.valBuf = Unpooled.wrappedBuffer(values);

        } catch (RocksDBException e) {
            e.printStackTrace();
            throw new RedisException(String.format("获取数据错误:key%s;%S", getKey0(), e.getStatus()));
        }
        return this;
    }

    public ListNode getNode(byte[] key) throws RedisException {
        try {
            byte[] values = db.get(key);

//            log.debug(toString(values));

            if (values == null) {
                this.valBuf = null;
                return null;
            }
            this.valBuf = Unpooled.wrappedBuffer(values);
            this.keyBuf = Unpooled.wrappedBuffer(key);

            this.key=getKey0();

        } catch (RocksDBException e) {
            e.printStackTrace();
            throw new RedisException(String.format("获取数据错误:key%s;%S", getKey0(), e.getStatus()));
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
        if (getPseq() == -1) {
            return null;
        }

        return create().genKey(getKey0(), getPseq()).get();
    }

    public ListNode setPseq(long val) throws RedisException {
        set(PSEQ, val);
        return this;
    }

    //@Deprecated

    /**
     * 更改字段值
     *
     * @param val0
     * @throws RedisException
     */
    public byte[] setVal(byte[] source0,byte[] updateval0) throws RedisException {

        int indexVal = 8 + 4 + 4 + 8 + 8 + 4;

        ByteBuf sourceBuf = Unpooled.wrappedBuffer(source0);

        ByteBuf ttlBuf = sourceBuf.slice(0, indexVal);
        ByteBuf val1Buf = Unpooled.wrappedBuffer(updateval0);
        valBuf = Unpooled.wrappedBuffer(ttlBuf, val1Buf);//零拷贝

        valBuf.resetReaderIndex();

        return valBuf.readBytes(valBuf.readableBytes()).array();

    }

    public long getNseq() throws RedisException {
        return get(NSEQ);
    }

    public ListNode next() throws RedisException {
        if (getNseq() == -1) {

            return null;
        }
        return create().genKey(getKey0(), getNseq()).get();
    }

    public ListNode setNseq(long val) throws RedisException {
        set(NSEQ, val);
        return this;
    }


    public long incrSEQ() throws RedisException {
        return Math.incrementExact(getSeq());
    }

    public long decrSEQ() throws RedisException {
        return Math.decrementExact(getSeq());
    }


    public long getSeq() throws RedisException {
//        keyBuf.resetReaderIndex();
        return get(SEQ);
    }



    public long getSeq0() throws RedisException {
        return seq;
    }

    public String getSeq0Str() throws RedisException {
        return getSeq0() + "";
    }


    @Deprecated
    public void setSeq(long pval) throws RedisException {

        seq = pval;

        keyBuf.resetReaderIndex();
        set(SEQ, pval);
    }


    public byte[] getKey0() {

        if(key!=null) return key;
        keyBuf.resetReaderIndex();

//        ByteBuf preKeyBuf = Unpooled.wrappedBuffer(instance.NS, DataType.SPLIT, key0, DataType.SPLIT, TYPE, DataType.SPLIT);

//        ByteBuf val0Buf = Unpooled.buffer(8);
//        val0Buf.writeLong(seq);

//        this.keyBuf = Unpooled.wrappedBuffer(preKeyBuf, val0Buf);

        int start = instance.NS.length + DataType.SPLIT.length;

        int length = keyBuf.capacity() - DataType.SPLIT.length * 2 - TYPE.length - 8-start;
        ByteBuf kb=keyBuf.slice(start, length);

        log.debug(toString(kb));

        kb.resetReaderIndex();
//        return key;
        return toArray(kb);
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


    }


    /**
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


                int preKeySize = NS.length + key.length + TYPE.length + DataType.SPLIT.length * 3;

                ByteBuf bb = keyBuf.slice(preKeySize, keyBuf.readableBytes() - preKeySize);

                result = bb.getLong(0);

                break;

            case TTL:

                result = this.valBuf.getLong(0);
                break;

            case TYPE1:

                result = this.valBuf.getInt(8 + 1);
                break;

            case SIZE:

                result = this.valBuf.getInt(8 + 4 + 2);
                break;

            case PSEQ:

                result = this.valBuf.getLong(8 + 4 + 4 + 3);
                break;

            case NSEQ:
                result = this.valBuf.getLong(8 + 4 + 4 + 3 + 8);
                break;

            default:
//                log.debug("default");
                log.debug("default");
                throw new RedisException(String.format("没有如此的字段:%s", fd));
        }
        return result;
    }

    /**
     * 设置指针数据
     *
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
                valBuf.setInt(8 + 1, (int) pval);
                break;

            case SIZE:
                valBuf.setInt(8 + 4 + 2, (int) pval);
                break;

            case PSEQ:
                valBuf.setLong(8 + 4 + 4 + 3, pval);
                break;

            case NSEQ:
                valBuf.setLong(8 + 4 + 4 + 3 + 8, pval);
                break;


            default:
                log.debug("default");
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

    public ByteBuf getVal0(byte[] val0) {

        ByteBuf val=Unpooled.wrappedBuffer(val0);
//        print(valBuf);

        val.resetReaderIndex();
        int indexVal = 8 + 4 + 4 + 8 + 8 + 4;
        ByteBuf valueBuf = val.slice(indexVal, val0.length - indexVal);
        return valueBuf;
//        return valueBuf.readBytes(valueBuf.readableBytes()).array();

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

        log.debug(sb.toString());

        return sb.toString();
    }


    public static void main(String[] args) throws Exception {
        log.debug("****************:");
        log.debug("main:");



    }

}
