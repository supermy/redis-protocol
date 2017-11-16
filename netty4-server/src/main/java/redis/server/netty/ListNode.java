package redis.server.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.util.Arrays;

import static redis.server.netty.ListNode.Meta.*;


/**
 * List Meta 元素方便促常用操作
 * <p>
 * Created by moyong on 2017/11/9.
 */
public class ListNode {

    enum Meta {
        KEY, SEQ, TTL, SIZE, PSEQ, NSEQ
    }

    private RocksDB db;

    private byte[] key;
    private byte[] val;

    private ByteBuf keyBuf;

    private ByteBuf metaBuf; //ttl size
    private ByteBuf linBuf;  //pseq nseq
    private ByteBuf valBuf;  //value


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
    public ListNode(RocksDB db0, byte[] key0, long seq0, byte[] val0, long pseq, long nseq) throws RedisException {
        this.db = db0;

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


        ByteBuf ttlBuf = Unpooled.buffer(28);
        ttlBuf.writeLong(-1); //ttl 无限期 -1
        ttlBuf.writeInt(val0.length); //value size
        ttlBuf.writeLong(pseq); //上一个元素
        ttlBuf.writeLong(nseq); //下

        this.metaBuf = ttlBuf.slice(0, 12);
        this.linBuf = ttlBuf.slice(12, 16);

        this.valBuf = Unpooled.wrappedBuffer(val0);

        ByteBuf valueBuf = Unpooled.wrappedBuffer(metaBuf, linBuf, valBuf);//零拷贝

        val = val0;
        try {
            db.put(key, valueBuf.readBytes(valueBuf.readableBytes()).array());
        } catch (RocksDBException e) {
            throw new RedisException(e.getMessage());
        }

    }

    /**
     * 数据初始化
     *
     * @param key
     * @throws RedisException
     */
    public ListNode(RocksDB db0, byte[] key0, long seq) throws RedisException {
        this.db = db0;

        ByteBuf keyPreBuf = Unpooled.wrappedBuffer("_l".getBytes(), key0, "#".getBytes());
        ByteBuf keySeqBuf = Unpooled.buffer(8);
        keySeqBuf.writeLong(seq);
        this.keyBuf = Unpooled.wrappedBuffer(keyPreBuf, keySeqBuf);
        this.key = keyBuf.readBytes(keyBuf.readableBytes()).array();

        get();
    }

    private void get() throws RedisException {
        try {
            byte[] values = db.get(key);

            ByteBuf valBuf = Unpooled.wrappedBuffer(values);

            this.metaBuf = valBuf.slice(0, 8 + 4);
            this.linBuf = valBuf.slice(8 + 4, 8 + 8);
            this.valBuf = valBuf.slice(12 + 16, values.length - 12 - 16);

            this.valBuf.resetReaderIndex();
            val = this.valBuf.readBytes(this.valBuf.readableBytes()).array();

            //数据过期处理
            if (getTtl() < now() && getTtl() != -1) {
                db.delete(key);
                throw new RedisException(String.format("没有如此的主键:%s", new String(key)));
            }

        } catch (RocksDBException e) {
            e.printStackTrace();
            throw new RedisException(String.format("没有如此的主键:%s", new String(key)));
        }
    }

    public void flush() throws RedisException {

//        ByteBuf ttlBuf = Unpooled.buffer(12);
//        ttlBuf.writeLong(getTtl()); //ttl 无限期 -1
//        ttlBuf.writeInt(getSize()); //value size
//
//        ttlBuf.writeLong(getPseq()); //ttl 无限期 -1
//        ttlBuf.writeLong(getNseq()); //value size


        valBuf.resetReaderIndex();
        ByteBuf valueBuf = Unpooled.wrappedBuffer(metaBuf, linBuf, valBuf);//零拷贝

        try {
            db.put(key, valueBuf.readBytes(valueBuf.readableBytes()).array());
        } catch (RocksDBException e) {
            throw new RedisException(e.getMessage());
        }
    }

    public void sync() throws RedisException {
        get();
    }


    public long getPseq() throws RedisException {
        return get(PSEQ);
    }

    public void setPseq(long val) throws RedisException {
        set(PSEQ, val);
    }


    public long getNseq() throws RedisException {
        return get(NSEQ);
    }

    public void setNseq(long val) throws RedisException {
        set(NSEQ, val);
    }


    public long getSeq() throws RedisException {
        keyBuf.resetReaderIndex();
        return get(SEQ);
    }

    public void setSeq(long pval) throws RedisException {
        keyBuf.resetReaderIndex();
//        System.out.println(String.format("key buf length:%d", keyBuf.readableBytes()));
        keyBuf.setLong(keyBuf.readableBytes() - 8, pval);
    }

    public byte[] getKey0() throws RedisException {

        keyBuf.resetReaderIndex();
        ByteBuf bb = keyBuf.slice(2, keyBuf.readableBytes() - 3 - 8);
//        System.out.println(new String(bb.readBytes(bb.readableBytes()).array()));
        return bb.readBytes(bb.readableBytes()).array();
    }

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
     * 获取指针数据
     *
     * @param fd
     * @return
     */
    private long get(Meta fd) throws RedisException {
        long result = 0;

        this.linBuf.resetReaderIndex();
        this.metaBuf.resetReaderIndex();


        switch (fd) {
            case SEQ:
//                keyBuf.resetReaderIndex();

                result = keyBuf.getLong(keyBuf.readableBytes()-8);
                break;

            case TTL:
                result = this.metaBuf.getLong(0);
                break;

            case SIZE:
                result = this.metaBuf.getInt(8);
                break;
            case PSEQ:
                result = this.linBuf.getLong(0);
                break;

            case NSEQ:
                result = this.linBuf.getLong(8);
                break;

            default:
                System.out.println("default");
                throw new RedisException(String.format("没有如此的字段:%s", fd));
        }
        return result;
    }

    private long set(Meta fd, long pval) throws RedisException {
        long result = 0;

        linBuf.resetWriterIndex();
        metaBuf.resetWriterIndex();

//        keyBuf.resetReaderIndex();

        switch (fd) {

            case SEQ:
                keyBuf.resetWriterIndex();
                keyBuf.setLong(keyBuf.readableBytes() - 8, pval);
                break;

            case TTL:
                linBuf.setLong(0, pval);
                break;

            case SIZE:
                linBuf.setInt(8, (int) pval);
                break;

            case PSEQ:
                linBuf.setLong(0, pval);
                break;

            case NSEQ:
                linBuf.setLong(8, pval);
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

    public String getKey() {
        return new String(key);
    }

    public String getVal() {
        return new String(val);
    }


    public String info() throws RedisException {
        StringBuilder sb = new StringBuilder(new String(getKey0()));
        sb.append("|");
        sb.append(getSeq());
        sb.append("=");
        sb.append(getVal());

        sb.append(" , TTL =");
        sb.append(getTtl());

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

        ListNode meta = new ListNode(RocksdbRedis.mymeta, "ListTest".getBytes(), 1, "value".getBytes(), 0, 0);
        meta.info();

        Assert.assertEquals(meta.getSeq(),1);
        Assert.assertEquals(meta.getNseq(),0);
        Assert.assertEquals(meta.getPseq(),0);

        Assert.assertTrue(Arrays.equals(meta.getKey0(), "ListTest".getBytes()));

//        System.out.println(new String(meta.getKey0()));
//        System.out.println(String.format("seq: %d", meta.getSeq()));

        meta.setKey0("abc".getBytes());
//        System.out.println(new String(meta.getKey0()));
        meta.setSeq(12);
        meta.setNseq(3);
        meta.setPseq(4);

        Assert.assertEquals(meta.getSeq(),12);
        Assert.assertEquals(meta.getNseq(),3);
        Assert.assertEquals(meta.getPseq(),4);

        Assert.assertTrue(Arrays.equals(meta.getKey0(), "abc".getBytes()));

        meta.info();

    }

}
