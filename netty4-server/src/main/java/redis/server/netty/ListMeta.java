package redis.server.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import redis.netty4.BulkReply;
import redis.netty4.IntegerReply;

import static redis.netty4.IntegerReply.integer;
import static redis.server.netty.ListMeta.Meta.*;


/**
 *
 * List Meta 元素方便促常用操作
 *
 * Created by moyong on 2017/11/9.
 *
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
        create(key0, count, sseq, eseq, cseq);
    }


    protected ListMeta create(byte[] key0, long count, long sseq, long eseq, long cseq) throws RedisException {
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

        return this;
    }


    /**
     * 数据初始化
     *
     * @param key
     * @throws RedisException
     */
    public ListMeta(RocksDB db0,byte[] key0,boolean notExsitCreate) throws RedisException {
        this.db=db0;
        this.metaKey = Unpooled.wrappedBuffer("+".getBytes(), key0, "list".getBytes());
        this.key = metaKey.readBytes(metaKey.readableBytes()).array();

        get(notExsitCreate);

    }

    public ListMeta get(boolean notExsitCreate) throws RedisException {

        try {
            byte[]  values = db.get(key);
            if (values == null ){
                if (notExsitCreate){
                    return init();
                }else  throw new RedisException(String.format("没有如此的主键:%s", new String(getKey0())));
            }


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

            //数据过期处理
            if (ttl < now() && ttl != -1) {
                db.delete(key);
                throw new RedisException(String.format("没有如此的主键:%s", new String(getKey0())));
            }

        } catch (RocksDBException e) {
            e.printStackTrace();
            throw new RedisException(String.format("没有如此的主键:%s", new String(getKey0())));
        }
        return this;
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

    public byte[] getKey0() throws RedisException {

        metaKey.resetReaderIndex();
        ByteBuf bb = metaKey.slice(1, metaKey.readableBytes() - 5);
//        System.out.println(new String(bb.readBytes(bb.readableBytes()).array()));
        return bb.readBytes(bb.readableBytes()).array();
    }

    public void setKey0(byte[] key0) throws RedisException {
        metaKey.resetReaderIndex();

        this.metaKey = Unpooled.wrappedBuffer("+".getBytes(), key0, "list".getBytes());
        this.key = metaKey.readBytes(metaKey.readableBytes()).array();


//        System.out.println(new String(b1.readBytes(b1.readableBytes()).array()));
//        System.out.println(new String(b3.readBytes(b3.readableBytes()).array()));
//        System.out.println(b4.readLong());

    }

    public  void sync() throws RedisException {
        get(false);
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
        System.out.println("========begin=======");

        System.out.println(getCseq());

        long pval = Math.incrementExact(getCseq());
        set(CSEQ, pval);

        System.out.println(pval);

        System.out.println(getCseq());

        System.out.println("========end=======");

        return pval;
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

    public boolean isNew() throws RedisException {
        return getCount() == 0;
    }

    public String  getKey() {
        return new String(key);
    }


    public String info() throws RedisException {
        StringBuilder sb =new StringBuilder(getKey());
        sb.append(":");
        sb.append("  count=");
        sb.append(getCount());

        sb.append("  sseq=");
        sb.append(getSseq());

        sb.append("  eseq=");
        sb.append(getEseq());

        sb.append("  cseq=");
        sb.append(getCseq());

        System.out.println(sb.toString());

        return sb.toString();
    }


    /**
     * plush 指令
     * @param key0
     * @param value1
     * @return
     * @throws RedisException
     */
    public IntegerReply lpush(byte[][] value1) throws RedisException {
        //批量处理
        final WriteOptions writeOpt = new WriteOptions();
        final WriteBatch batch = new WriteBatch();

        for (byte[] val :value1
             ) {

            if(isNew()){

                ListNode newnode = new ListNode(RocksdbRedis.mydata, getKey0(), 0, val, -1, -1);

//                ListNode newnode1 = new ListNode(RocksdbRedis.mydata, getKey0(), 0);

//                incrCseq();
                setCseq(newnode.getSeq());
                setSseq(newnode.getSeq());
                setEseq(newnode.getSeq());

            }else {

//                batch.put(newKey, curVal);
                ListNode firstNode = getFirstNode();

                incrCseq();

                //新建元素
                ListNode newnode = new ListNode(RocksdbRedis.mydata, getKey0(), getCseq(), val, -1, firstNode.getSeq());

//                ListNode newnode1 = new ListNode(RocksdbRedis.mydata, getKey0(), getCseq());

                //变更元素指针
                firstNode.setPseq(newnode.getSeq());
//                firstNode.info();
                firstNode.flush();
//                firstNode.info();

                //变更元素指针
                setSseq(getCseq());
            }
            incrCount();
            this.info();

        }


        this.flush();

//        return value1.length;
        return integer(value1.length);

    }

    public BulkReply lpop() throws RedisException {
        ListNode firstNode = getFirstNode();
        this.setSseq(firstNode.getNseq());
        this.decrCount();
        this.flush();
        firstNode.destory();
        getFirstNode().setPseq(-1).flush();
//        long pval = Math.incrementExact(getCseq());
//        set(CSEQ, pval);
////        return pval;
        return new BulkReply(firstNode.getVal0());
    }

    public ListMeta init() throws RedisException {
        create(getKey0(), 0, -1, -1, -1);
       return this;
    }

    public void destory() throws RedisException {
        try {
            db.delete(key);
        } catch (RocksDBException e) {
            throw new RedisException(e.getMessage());
        }
    }

    protected ListNode getLastNode() throws RedisException {
        return new ListNode(RocksdbRedis.mydata,getKey0(),getEseq());
    }

    protected ListNode getFirstNode() throws RedisException {
        return new ListNode(RocksdbRedis.mydata,getKey0(),getSseq());
    }


    public static void main(String[] args) throws Exception {

        //测试删除
        ListMeta meta0=new ListMeta(RocksdbRedis.mymeta,"ListInit".getBytes(),true);
        meta0.destory();

        try {
            new ListMeta(RocksdbRedis.mymeta,"ListInit".getBytes(),false);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof RedisException);
            Assert.assertTrue(e.getMessage().contains("没有如此的主键"));
        }

        //测试创建
        ListMeta meta1=new ListMeta(RocksdbRedis.mymeta,"ListInit".getBytes(),true);

        Assert.assertEquals(0,meta1.getCount());
        Assert.assertEquals(-1,meta1.getSseq());
        Assert.assertEquals(-1,meta1.getEseq());
        Assert.assertEquals(-1,meta1.getCseq());
        Assert.assertArrayEquals("ListInit".getBytes(),meta1.getKey0());


        //测试数据更新
        meta1.setKey0("CSTList".getBytes());
        Assert.assertArrayEquals(meta1.getKey0(),"CSTList".getBytes());
        meta1.setCount(2);
        Assert.assertEquals(2,meta1.getCount());
        meta1.setSseq(3);
        Assert.assertEquals(3,meta1.getSseq());
        meta1.setEseq(4);
        Assert.assertEquals(4,meta1.getEseq());
        meta1.setCseq(5);
        Assert.assertEquals(5,meta1.getCseq());


        //测试数据持久化
        meta1.info();
        meta1.flush();//持久化

        //引入
        ListMeta meta2=new ListMeta(RocksdbRedis.mymeta,"CSTList".getBytes(),false);
        meta2.info();
        Assert.assertEquals(5,meta2.getCseq());
        meta2.incrCseq();
        Assert.assertEquals(6,meta2.getCseq());

        System.out.println("============test lpush");

        //测试 lpush
        ListMeta metaLPush0=new ListMeta(RocksdbRedis.mymeta,"LPUSH".getBytes(),true);
        metaLPush0.destory();
        ListMeta metaLPush=new ListMeta(RocksdbRedis.mymeta,"LPUSH".getBytes(),true);

        byte[][] lpusharray = {"XXX".getBytes(),"YYY".getBytes(),"ZZZ".getBytes()};

        Assert.assertEquals(metaLPush.lpush(lpusharray).data().intValue(),3);

        Assert.assertEquals(metaLPush.getCount(),3);
        Assert.assertEquals(metaLPush.getSseq(),2);
        Assert.assertEquals(metaLPush.getEseq(),0);
        Assert.assertEquals(metaLPush.getCseq(),2);

        System.out.println("========================2 !!!");

        ListNode n3 = new ListNode(RocksdbRedis.mydata, "LPUSH".getBytes(), 2);
        Assert.assertEquals(n3.getNseq(),1);
        Assert.assertEquals(n3.getPseq(),-1);
        Assert.assertEquals(n3.getSeq(),2);
        Assert.assertEquals(n3.getSize(),3);

        Assert.assertArrayEquals(n3.getVal0(),"ZZZ".getBytes());
        System.out.println("========================1 !!!");

        ListNode n2 = new ListNode(RocksdbRedis.mydata, "LPUSH".getBytes(), 1);

        Assert.assertEquals(n2.getNseq(),0);
        Assert.assertEquals(n2.getPseq(),2);
        Assert.assertEquals(n2.getSeq(),1);
        Assert.assertEquals(n2.getSize(),3);

        Assert.assertArrayEquals(n2.getVal0(),"YYY".getBytes());
        System.out.println("========================0 !!!");


        ListNode n1 = new ListNode(RocksdbRedis.mydata, "LPUSH".getBytes(), 0);
        Assert.assertEquals(n1.getNseq(),-1);
        Assert.assertEquals(n1.getPseq(),1);
        Assert.assertEquals(n1.getSeq(),0);
        Assert.assertEquals(n1.getSize(),3);
        Assert.assertArrayEquals(n1.getVal0(),"XXX".getBytes());


//        ByteBuf keyPreBuf = Unpooled.wrappedBuffer("_l".getBytes(), "LPUSH".getBytes(), "#".getBytes());
//        ByteBuf keySeqBuf = Unpooled.buffer(8);
//        keySeqBuf.writeLong(0);
//
//        ByteBuf keyBuf = Unpooled.wrappedBuffer(keyPreBuf, keySeqBuf);
//        byte[] key = keyBuf.readBytes(keyBuf.readableBytes()).array();
//
//        System.out.println(new String(key));
//
//
//        byte[] bytes = RocksdbRedis.mydata.get(key);
//
//        System.out.println("========================!!!");
//        System.out.println(new String(bytes));
//        System.out.println("========================@@@");


    }

}
