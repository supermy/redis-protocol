package redis.server.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import static redis.server.netty.ListMeta.Meta.*;


/**
 *
 * List Meta 元素方便促常用操作
 *
 * Created by moyong on 2017/11/9.
 */
public class ListMeta {

    enum Meta {
        COUNT, SSEQ, ESEQ, CSEQ
    }

    private RocksDB db;

    private byte[] key;
    private byte[] val;

    private ByteBuf metaVal;
    private ByteBuf metaKey;

    private long ttl;
    private int size;


    public ListMeta(RocksDB db0,byte[] key0,long count,long sseq,long eseq,long cseq) throws RedisException {
        this.db = db0;

        this.metaKey = Unpooled.wrappedBuffer("+".getBytes(), key0, "list".getBytes());
        this.key = metaKey.readBytes(metaKey.readableBytes()).array();

        this.metaVal = Unpooled.buffer(32);
        this.metaVal.writeLong(count);  //数量
        this.metaVal.writeLong(sseq);    //第一个元素
        this.metaVal.writeLong(eseq);    //最后一个元素
        this.metaVal.writeLong(cseq);    //最新主键编号

        val = metaVal.readBytes(metaVal.readableBytes()).array();

        System.out.println(String.format("count:%d 第一个元素：%d 最后一个元素：%d 自增主键：%d  value:%s", count, sseq, eseq, sseq,new String(val)));

        this.ttl = -1;
        this.size = val.length;

        ByteBuf ttlBuf = Unpooled.buffer(12);
        ttlBuf.writeLong(getTtl()); //ttl 无限期 -1
        ttlBuf.writeInt(getSize()); //value size

        metaVal.resetReaderIndex();
//        ByteBuf valueBuf = Unpooled.wrappedBuffer(value); //零拷贝
        ByteBuf valbuf = Unpooled.wrappedBuffer(ttlBuf, metaVal);//零拷贝

        try {
            db.put(key, valbuf.readBytes(valbuf.readableBytes()).array());
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
    public ListMeta(RocksDB db0,byte[] key0) throws RedisException {
        this.db=db0;
        this.metaKey = Unpooled.wrappedBuffer("+".getBytes(), key0, "list".getBytes());
        this.key = metaKey.readBytes(metaKey.readableBytes()).array();
        get();
    }

    public void get() throws RedisException {


        try {
            byte[]  values = db.get(key);

            ByteBuf valBuf= Unpooled.wrappedBuffer(values);

            ByteBuf ttlBuf = valBuf.readSlice(8);
            ByteBuf sizeBuf = valBuf.readSlice(4);
            metaVal = valBuf.slice(8 + 4, values.length - 8 - 4);

            ttl = ttlBuf.readLong();//ttl
            size = sizeBuf.readInt();//长度数据

            metaVal.resetReaderIndex();
            val = metaVal.readBytes(metaVal.readableBytes()).array();

//            System.out.println(val);
//            System.out.println(new String(val));
//
//


            //数据过期处理
            if (ttl < now() && ttl != -1) {
                db.delete(key);
                throw new RedisException(String.format("没有如此的主键:%s", new String(key)));
            }

        } catch (RocksDBException e) {
            e.printStackTrace();
            throw new RedisException(String.format("没有如此的主键:%s", new String(key)));
        }
    }

    public  void flush() throws RedisException {

        ByteBuf ttlBuf = Unpooled.buffer(12);
        ttlBuf.writeLong(getTtl()); //ttl 无限期 -1
        ttlBuf.writeInt(getSize()); //value size

        metaVal.resetReaderIndex();
        ByteBuf valBuf = Unpooled.wrappedBuffer(ttlBuf, metaVal);//零拷贝

//        System.out.println(db);
//        System.out.println(key);
//        System.out.println(metaVal);

        try {
            db.put(key, valBuf.readBytes(valBuf.readableBytes()).array());
        } catch (RocksDBException e) {
            throw new RedisException(e.getMessage());
        }
    }

    public  void sync() throws RedisException {
        get();
    }
    /**
     * 元素数量
     * @return
     */
    public long getCount() throws RedisException {
        return get(COUNT);
    }

    public  void setCount(long val) throws RedisException {
         set(COUNT,val);
    }
    /**
     * 首元素
     * @return
     */
    public long getSseq() throws RedisException {
        return get(SSEQ);
    }

    public void setSseq(long val) throws RedisException {
        set(SSEQ,val);
    }

    /**
     * 尾元素
     * @return
     */
    public long getEseq() throws RedisException {
        return get(ESEQ);
    }

    public void setEseq(long val) throws RedisException {
        set(ESEQ,val);
    }

    /**
     * 自增主键
     * @return
     */
    public long getCseq() throws RedisException {
        return get(CSEQ);
    }

    public void setCseq(long val) throws RedisException {
        set(CSEQ,val);
    }

    public long incrCseq() throws RedisException {
        long pval = Math.incrementExact(getCseq());
        set(CSEQ, pval);
        return pval;
    }


    /**
     * 获取指针数据
     *
     * @param fd
     * @return
     */
    private long get(Meta fd) throws RedisException {
        long result = 0;

        //this.metaVal=Unpooled.wrappedBuffer(val);

        this.metaVal.resetReaderIndex();

        switch (fd) {

            case COUNT:
                result = this.metaVal.getLong(0);
                break;

            case SSEQ:
                result = this.metaVal.getLong(8);
                break;

            case ESEQ:
                result = this.metaVal.getLong(16);
                break;

            case CSEQ:
                result = this.metaVal.getLong(24);
                break;

            default:
                System.out.println("default");
                throw new RedisException(String.format("没有如此的字段:%s", fd));
        }
        return result;
    }

    private long set(Meta fd,long pval) throws RedisException {
        long result = 0;

        //metaVal=Unpooled.wrappedBuffer(val);

        switch (fd) {

            case COUNT:
                metaVal.setLong(0,pval);
                break;

            case SSEQ:
                metaVal.setLong(8,pval);
                break;

            case ESEQ:
                metaVal.setLong(16,pval);
                break;

            case CSEQ:
                metaVal.setLong(24,pval);
                break;

            default:
                System.out.println("default");
                throw new RedisException(String.format("没有如此的字段:%s", fd));
        }
        return result;
    }

    private long now() {
       return  System.currentTimeMillis();
    }


    public long getTtl() {
        return ttl;
    }

    public int getSize() {
        return val.length;
    }

    public String  getKey() {
        return new String(key);
    }

    public String info() throws RedisException {
        StringBuilder sb =new StringBuilder(getKey());
        sb.append(":");
        sb.append("  count,");
        sb.append(getCount());

        sb.append("  sseq,");
        sb.append(getSseq());

        sb.append("  eseq,");
        sb.append(getEseq());

        sb.append("  cseq,");
        sb.append(getCseq());

        System.out.println(sb.toString());

        return sb.toString();
    }



    public static void main(String[] args) throws Exception {

       ListMeta meta=new ListMeta(RocksdbRedis.mymeta,"ListTest".getBytes(),6,5,0,6);
        meta.info();

        meta.setCount(2);
        Assert.assertEquals(2,meta.getCount());
        meta.setSseq(3);
        Assert.assertEquals(3,meta.getSseq());
        meta.setEseq(4);
        Assert.assertEquals(4,meta.getEseq());
        meta.setCseq(5);
        Assert.assertEquals(5,meta.getCseq());

        meta.info();


        ListMeta meta1=new ListMeta(RocksdbRedis.mymeta,"ListTest".getBytes());
        meta1.info();

        meta.flush();

        meta1.sync();

        meta1.info();

        meta1.incrCseq();

        meta1.info();


    }

}
