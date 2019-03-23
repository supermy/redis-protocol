package redis.server.netty.rocksdb;

import com.google.common.base.Throwables;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.rocksdb.*;
import org.supermy.util.MyUtils;
import redis.netty4.*;
import redis.server.netty.RedisException;
import redis.server.netty.utis.DataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static redis.netty4.BulkReply.NIL_REPLY;
import static redis.netty4.IntegerReply.integer;
import static redis.netty4.MultiBulkReply.EMPTY;
import static redis.netty4.StatusReply.OK;
import static redis.netty4.StatusReply.QUIT;
import static redis.server.netty.rocksdb.ListMeta.Meta.*;
import static redis.server.netty.rocksdb.RedisBase.invalidValue;
import static redis.server.netty.rocksdb.RedisBase.notInteger;


/**
 *
 * <p>
 *     采用rocks原生的数据结构,减少交互次数，提高数据处理效率；不支持linsert 命令
 *     //队列的导航依赖RocksDb;meta value 只存放数量；
 *     //队列的排序依赖序号
 *
 * List Meta 元素方便促常用操作
 * List        [<ns>] <key> KEY_META                 KEY_LIST <MetaObject>
 * [<ns>] <key> KEY_LIST_ELEMENT <index> KEY_LIST_ELEMENT <element-value>
 * <p>
 *
 * <p>
 * Created by moyong on 2018/11/15.
 * Update by moyong on 2018/11/15.
 */
public class ListMeta extends BaseMeta {

    private static Logger log = Logger.getLogger(ListMeta.class);


    enum Meta {
        COUNT, SSEQ, ESEQ, CSEQ
    }


//    private byte[] key;
//    private byte[] val;


//    private long ttl;
//    private int size;

//
//    public long getVal0() throws RedisException {
//        print(metaVal);
//        metaVal.resetReaderIndex();
//        return metaVal.getLong(8 + 4 + 2);
//    }

//    public void setVal0(long val0) throws RedisException {
//        this.metaVal.setLong(8 + 4 + 2, val0);  //数量
//    }

//    public byte[] getVal() throws RedisException {
//        this.metaVal.resetReaderIndex();
//        return this.metaVal.readBytes(metaVal.readableBytes()).array();
//    }

//    public byte[] getVal(ByteBuf val) throws RedisException {
//        val.resetReaderIndex();
//        return val.readBytes(val.readableBytes()).array();
//    }

//    public long getTtl() {
//        return metaVal.getLong(0);
//    }
//
//    public int getType() throws RedisException {
//        if (metaVal == null) return -1;
//        return this.metaVal.getInt(8 + 1);
//    }


    protected static ListNode listNode;

    private ListMeta() {
    }

    private static ListMeta instance = new ListMeta();


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
    public static ListMeta getInstance(RocksDB db0, byte[] ns0) {
        instance.db = db0;
        instance.NS = ns0;
        instance.VAlTYPE = DataType.KEY_LIST;
        listNode = ListNode.getInstance(db0, ns0);
        return instance;
    }

    /**
     * 构造 MetaKey
     *
     * @param key0
     * @return
     * @throws RedisException
     */
    @Deprecated
    public ListMeta genMetaKey(byte[] key0) throws RedisException {
        if (key0 == null) {
            throw new RedisException(String.format("key0 主键不能为空"));
        }
        instance.metaKey = MyUtils.concat(instance.NS, DataType.SPLIT, key0, DataType.SPLIT, KEYTYPE);
        return instance;
    }

//
//    private ByteBuf genMetaVal(long count, long sseq, long eseq, long cseq) {
//
//        ByteBuf ttlBuf = Unpooled.buffer(12);
//
//        ttlBuf.writeLong(-1); //ttl 无限期 -1
//        ttlBuf.writeBytes(DataType.SPLIT);
//
//        ttlBuf.writeInt(DataType.KEY_LIST); //value type
//        ttlBuf.writeBytes(DataType.SPLIT);
//
//
//        ByteBuf val1 = Unpooled.buffer(32);
//        val1.writeLong(count);  //数量
//        val1.writeLong(sseq);    //第一个元素
//        val1.writeLong(eseq);    //最后一个元素
//        val1.writeLong(cseq);    //最新主键编号 //展示废弃不用
//
//        log.debug(String.format("count:%d 第一个元素：%d 最后一个元素：%d 自增主键：%d  value:%s", count, sseq, eseq, sseq, new String(val1.readBytes(val1.readableBytes()).array())));
//
//
//        val1.resetReaderIndex();
//        ttlBuf.resetReaderIndex();
//
//        return Unpooled.wrappedBuffer(ttlBuf, val1);
//
//    }

//    /**
//     * 创建meta key
//     *
//     * @param count
//     * @return
//     * @throws RedisException
//     */
//    protected ListMeta setMeta(long count, long sseq, long eseq, long cseq) throws RedisException {
//
//        this.metaVal = genMetaVal(count, sseq, eseq, cseq);
//
////        log.debug(String.format("count:%d;  主键：%s; value:%s", count, getKey0Str(), getVal0()));
//
//        try {
//            db.put(getKey(), getVal());
//        } catch (RocksDBException e) {
//            e.printStackTrace();
//            throw new RedisException(e.getMessage());
//        }
//
//
//        return this;
//    }

//    /**
//     * 获取meta 数据
//     *
//     * @return
//     * @throws RedisException
//     */
//    protected ListMeta getMeta() throws RedisException {
//
//        try {
//            byte[] value = db.get(getKey());
//            if (value == null) this.metaVal = null;
//            else
//                this.metaVal = Unpooled.wrappedBuffer(value);
//        } catch (RocksDBException e) {
//            e.printStackTrace();
//            throw new RedisException(e.getMessage());
//        }
//
//        return this;
//    }

//    ///////////////
//    public ListMeta(RocksDB db0, byte[] key0, long count, long sseq, long eseq, long cseq) throws RedisException {
//        this.db = db0;
//        create(key0, count, sseq, eseq, cseq);
//    }


//    protected ListMeta create(byte[] key0, long count, long sseq, long eseq, long cseq) throws RedisException {
//
//        ByteBuf valbuf = genMetaVal(count, sseq, eseq, cseq);
//
//        try {
//            db.put(key, valbuf.readBytes(valbuf.readableBytes()).array());
//        } catch (RocksDBException e) {
//            throw new RedisException(e.getMessage());
//        }
//
//        return this;
//    }


//    /**
//     * 持久化数据到硬盘
//     *
//     * @throws RedisException
//     */
//    public void flush() throws RedisException {
//
//        try {
//            db.put(getKey(), getVal());
//        } catch (RocksDBException e) {
//            throw new RedisException(e.getMessage());
//        }
//    }


//    /**
//     * 元素数量
//     *
//     * @return
//     */
//    public long getCount() throws RedisException {
//        if (metaVal == null) return 0;
//
//        return get(COUNT);
//    }

//    public void setCount(long val) throws RedisException {
//        set(COUNT, val);
//    }

//    /**
//     * 首元素
//     *
//     * @return
//     */
//    public long getSseq() throws RedisException {
//        return get(SSEQ);
//    }

//    public void setSseq(long val) throws RedisException {
//        set(SSEQ, val);
//    }

//    /**
//     * 尾元素
//     *
//     * @return
//     */
//    public long getEseq() throws RedisException {
//        return get(ESEQ);
//    }

//    public void setEseq(long val) throws RedisException {
//        set(ESEQ, val);
//    }
//
//    /**
//     * 自增主键
//     *
//     * @return
//     */
//    public long getCseq() throws RedisException {
//        return get(CSEQ);
//    }

//    public void setCseq(long val) throws RedisException {
//        set(CSEQ, val);
//    }
//
//    public long incrCseq() throws RedisException {
//
//        long pval = Math.incrementExact(getCseq());
//        set(CSEQ, pval);
//        return pval;
//    }

//    /**
//     * 开始扩容
//     * @return
//     * @throws RedisException
//     */
//    public long incrSseq() throws RedisException {
//        long pval = Math.incrementExact(getSseq());
//        set(SSEQ, pval);
//        return pval;
//    }

//    /**
//     * 结束扩容
//     * @return
//     * @throws RedisException
//     */
//    public long decrEseq() throws RedisException {
//        long pval = Math.decrementExact(getEseq());
//        set(ESEQ, pval);
//        return pval;
//    }

//    public long incrCount() throws RedisException {
//        long pval = Math.incrementExact(getCount());
//        setCount(pval);
//        return pval;
//    }
//
//    public long decrCount() throws RedisException {
//        long pval = Math.decrementExact(getCount());
//        setCount(pval);
//        return pval;
//    }


//    /**
//     * 获取指针数据
//     *
//     * @param fd
//     * @return
//     */
//    private long get(Meta fd) throws RedisException {
//        long result = 0;
//
////        if (metaVal==null) return result;
//
//        this.metaVal.resetReaderIndex();
//        int ttlSize = 8 + 4 + 2;
//        ByteBuf linkBuf = metaVal.slice(ttlSize, metaVal.readableBytes() - ttlSize);
//
//
//        switch (fd) {
//
//            case COUNT:
//                result = linkBuf.getLong(0);
//                break;
//
//            case SSEQ:
//                result = linkBuf.getLong(8);
//                break;
//
//            case ESEQ:
//                result = linkBuf.getLong(16);
//                break;
//
//            case CSEQ:
//                result = linkBuf.getLong(24);
//                break;
//
//            default:
//                log.debug("default");
//                throw new RedisException(String.format("没有如此的字段:%s", fd));
//        }
//        return result;
//    }

//    private long set(Meta fd, long pval) throws RedisException {
//        long result = 0;
//
//        //metaVal=Unpooled.wrappedBuffer(val);
//        int ttlSize = 8 + 4 + 2;
//
//
//        switch (fd) {
//
//            case COUNT:
//                metaVal.setLong(ttlSize, pval);
//                break;
//
//            case SSEQ:
//                metaVal.setLong(ttlSize + 8, pval);
//                break;
//
//            case ESEQ:
//                metaVal.setLong(ttlSize + 16, pval);
//                break;
//
//            case CSEQ:
//                metaVal.setLong(ttlSize + 24, pval);
//                break;
//
//            default:
//                log.debug("default");
//                throw new RedisException(String.format("没有如此的字段:%s", fd));
//        }
//        return result;
//    }



//    public int getSize() {
//        return val.length;
//    }
//
//    public boolean isNew() throws RedisException {
//        return getCount() == 0;
//    }


//    public String info() throws RedisException {
//        StringBuilder sb = new StringBuilder("meta data ~ ");
//        sb.append(getKey0Str());
//        sb.append(":");
//        sb.append("  count=");
//        sb.append(getCount());
//
//        sb.append("  sseq=");
//        sb.append(getSseq());
//
//        sb.append("  eseq=");
//        sb.append(getEseq());
//
//        sb.append("  cseq=");
//        sb.append(getCseq());
//
//        log.debug(sb.toString());
//
//        return sb.toString();
//    }


    //meta value 存储cnt,其他的不存储
    //firstNode
    // push:id(create) pop  ;if is emptu  id=1  else incr

    //lastNode
    // push:id(create) pop  ;if is empty  id=1  else decr

//    /**
//     * 构建子元素扫描key
//     *
//     * @param filedType
//     * @return
//     */
//    public byte[] genKeyPartten(byte[] filedType) throws RedisException {
//        return super.genKeyPartten(filedType);
//    }


    /**
     *
     * 第一个节点
     *
     * @param pattern0
     * @return
     */
    protected static byte[] firstNode(RocksDB db, byte[] pattern0) {
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

                        log.debug(new String(iterator.key()));
                        log.debug(new String(iterator.value()));
                        return iterator.key();

                    } else {
                        break;
                    }
                } else break;

            }
        }

        //检索过期数据,处理过期数据 ;暂不处理影响效率 fixme
//        log.debug(keys.size());

        return null;
    }


    /**
     *
     * 尾节点
     *
     * @param db
     * @param pattern0
     * @return
     */
    protected static byte[] lastNode(RocksDB db, byte[] pattern0) {
        //按 key 检索所有数据
        List<byte[]> keys = new ArrayList<>();

        try (final RocksIterator iterator = db.newIterator()) {
//            iterator.seekToLast();
//            iterator.seekToFirst();
            //反向查询匹配最后一个数据
            ByteBuf patt = Unpooled.wrappedBuffer(pattern0,"z".getBytes());
            for (iterator.seekForPrev(toByteArray(patt)); iterator.isValid(); iterator.prev()) {

                //确保检索有数据，hkeybuf.slice 不错误
                if (pattern0.length <= iterator.key().length) {
                    ByteBuf hkeybuf = Unpooled.wrappedBuffer(iterator.key()); //优化 零拷贝
                    ByteBuf slice = hkeybuf.slice(0, pattern0.length); //获取指定前缀长度的 byte[]

                    slice.resetReaderIndex();

                    log.debug(toString(iterator.key()));
                    log.debug(toString(iterator.value()));

                    //key有序 不相等后面无数据
                    if (Arrays.equals(toByteArray(slice), pattern0)) {

                        log.debug(toString(iterator.key()));
                        log.debug(toString(iterator.value()));

                        return iterator.key();

                    } else {
                        break;
                    }
                } else break;

            }
        }

        //检索过期数据,处理过期数据 ;暂不处理影响效率 fixme
//        log.debug(keys.size());

        return null;
    }

    /**
     * ok
     * plush 指令
     * 将一个或多个值插入到列表头部。 如果 key 不存在，一个空列表会被创建并执行 LPUSH 操作。 当 key 存在但不是列表类型时，返回一个错误。
     *
     * @param value1
     * @return
     * @throws RedisException
     */
    public IntegerReply lpush(byte[]... value1) throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_LIST)) return integer(0);

//        //判断类型，非hash 类型返回异常信息；
//        int type = getMeta().getType();//hashNode.typeBy(db, getKey());
//
//        if (type != -1 && type != DataType.KEY_LIST) {
//            //抛出异常 类型不匹配
//            throw invalidValue();
//        }

        //批量处理
        final WriteOptions writeOpt = new WriteOptions();
        final WriteBatch batch = new WriteBatch();

        for (byte[] val1 : value1
        ) {

            //队列的导航依赖RocksDb;meta value 只存放数量；
            //队列的排序依赖序号

            ListNode firstNode = getFirstNode();


            if (firstNode==null){

                listNode.genKey(getKey0(), now()).put(val1, -1, -1);//后面两个参数无用

//                setMeta(1, listNode.getSeq(), listNode.getSeq(), listNode.getSeq());//fixme
                metaVal=getMetaVal(1,-1);
                metaCache.put(Unpooled.wrappedBuffer(getKey0()),metaVal);


//                setCseq(listNode.getSeq());
//                log.debug(listNode.getSeq());
//                setSseq(listNode.getSeq());
//                setEseq(listNode.getSeq());

            }else{
                firstNode.info();
//                log.debug(new String(firstNode.getKey0()));
//                log.debug(firstNode.getSeq());

                listNode.genKey(getKey0(), firstNode.decrSEQ()).put(val1, -1, -1);//后面两个参数无用

//                log.debug(listNode.getSeq());
//
//                setSseq(listNode.getSeq());
//
//                incrCount();
                log.debug(getCount());
                metaVal=getMetaVal(Math.incrementExact(getCount()),-1);
                metaCache.put(Unpooled.wrappedBuffer(getKey0()),metaVal);

            }

            listNode.info();


//            this.info();
        }

//        this.flush();



        return integer(value1.length);

    }

    /**
     * ok
     * Redis Rpush 命令用于将一个或多个值插入到列表的尾部(最右边)。
     * 如果列表不存在，一个空列表会被创建并执行 RPUSH 操作。 当列表存在但不是列表类型时，返回一个错误。
     *
     * @param value1
     * @return
     * @throws RedisException
     */
    public IntegerReply rpush(byte[]... value1) throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_LIST)) return integer(0);
//
//        //判断类型，非hash 类型返回异常信息；
//        int type = getMeta().getType();//hashNode.typeBy(db, getKey());
//
//        if (type != -1 && type != DataType.KEY_LIST) {
//            //抛出异常 类型不匹配
//            throw invalidValue();
//        }


        //批量处理
        final WriteOptions writeOpt = new WriteOptions();
        final WriteBatch batch = new WriteBatch();

        for (byte[] val1 : value1
        ) {


            ListNode lastNode = getLastNode();

            if (lastNode == null){
                listNode.genKey(getKey0(), now()).put(val1, -1, -1);//后面两个参数无用

                //修改缓存中的计数
                metaVal=getMetaVal(1,-1);
                metaCache.put(Unpooled.wrappedBuffer(getKey0()),metaVal);
//                setMeta(1, listNode.getSeq(), listNode.getSeq(), listNode.getSeq());//fixme

//                setCseq(listNode.getSeq());
//                setSseq(listNode.getSeq());
//                setEseq(listNode.getSeq());
//
//                log.debug(listNode.getSeq());


            }else{

                lastNode.info();
//                log.debug(lastNode.getSeq());

                listNode.genKey(getKey0(), lastNode.incrSEQ()).put(val1, -1, -1);//后面两个参数无用

//                log.debug(listNode.getSeq());
//
//                setEseq(listNode.getSeq());

                //incr count
                metaVal=getMetaVal(Math.incrementExact(getCount()),-1);
                metaCache.put(Unpooled.wrappedBuffer(getKey0()),metaVal);

//                incrCount();

            }
            listNode.info();

//            this.info();
        }


//        this.flush();

        return integer(value1.length);

    }

    /**
     * Redis Lpop 命令用于移除并返回列表的第一个元素。
     *
     * @return
     * @throws RedisException
     */
    public BulkReply lpop() throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_LIST)) return NIL_REPLY;

        ListNode firstNode = getFirstNode();
        if (firstNode==null) return NIL_REPLY;

//        this.decrCount();
        metaVal=getMetaVal(Math.decrementExact(getCount()),-1);
        metaCache.put(Unpooled.wrappedBuffer(getKey0()),metaVal);

//        this.flush();
        firstNode.del();
//        getFirstNode().setPseq(-1).flush();
        return new BulkReply(firstNode.getVal0());
    }

    /**
     * Redis Rpop 命令用于移除并返回列表的最后一个元素。
     *
     * @return
     * @throws RedisException
     */
    public BulkReply rpop() throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_LIST)) return NIL_REPLY;

        ListNode lastNode = getLastNode();
        if (lastNode==null) return NIL_REPLY;

//        this.decrCount();
        metaVal=getMetaVal(Math.decrementExact(getCount()),-1);
        metaCache.put(Unpooled.wrappedBuffer(getKey0()),metaVal);
//        this.flush();
        lastNode.del();
        return new BulkReply(lastNode.getVal0());
    }

    /**
     * Redis Llen 命令用于返回列表的长度。 如果列表 key 不存在，则 key 被解释为一个空列表，返回 0 。
     * 如果 key 不是列表类型，返回一个错误。
     *
     * @return
     * @throws RedisException
     */
    public IntegerReply llen() throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_LIST)) return integer(0);

        long l = countBy(db, genKeyPartten(DataType.KEY_LIST_ELEMENT));

        return integer(l);
    }


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
     * 按位置索引提取数据
     *
     * @param db
     * @param pattern0
     * @return
     */
    protected static List<ByteBuf> zgetBy(RocksDB db, byte[] pattern0, long start, long stop, boolean delete,boolean update,byte[] updateValue) throws RedisException {
        //按 key 检索所有数据
        List<ByteBuf> keys = new ArrayList<ByteBuf>();
        try (final RocksIterator iterator = db.newIterator()) {
            int index = 0;
            for (iterator.seek(pattern0); iterator.isValid(); iterator.next()) {

                //确保检索有数据，hkeybuf.slice 不错误
                if (pattern0.length <= iterator.key().length) {
                    ByteBuf hkeybuf = Unpooled.wrappedBuffer(iterator.key()); //优化 零拷贝
                    ByteBuf slice = hkeybuf.slice(0, pattern0.length); //获取指定前缀长度的 byte[]

                    slice.resetReaderIndex();

                    //key有序 不相等后面无数据
                    if (Arrays.equals(slice.readBytes(slice.readableBytes()).array(), pattern0)) {

                        //获取指定索引的数据
                        if ( index>=start  && index <= stop) {


                            ByteBuf val0 = listNode.getVal0(iterator.value());
                            log.debug(toString(val0));
                            val0.resetReaderIndex();

                            keys.add(val0);

                            //delete score and sort data node;
                            if (delete) {
                                db.delete(iterator.key());
                            }

                            if (update){
                                db.put(iterator.key(), listNode.setVal(iterator.value(),updateValue));
                            }

                        }

                        index++;

                    } else {
                        break;
                    }
                } else break;

            }
        } catch (RocksDBException e) {
            e.printStackTrace();
            Throwables.propagateIfPossible(e, RedisException.class);
        }

        return keys;
    }


    protected static List<ByteBuf> zdeleteBy(RocksDB db, byte[] pattern0,boolean reverse, long cnt, byte[] deleteValue) throws RedisException {
        //按 key 检索所有数据
        List<ByteBuf> keys = new ArrayList<ByteBuf>();
        try (final RocksIterator iterator = db.newIterator()) {
            int index = 0;
            int count = 0;

            if (!reverse){
                //从开始查询
                for (iterator.seek(pattern0); iterator.isValid(); iterator.next()) {

                    //确保检索有数据，hkeybuf.slice 不错误
                    if (pattern0.length <= iterator.key().length) {
                        ByteBuf hkeybuf = Unpooled.wrappedBuffer(iterator.key()); //优化 零拷贝
                        ByteBuf slice = hkeybuf.slice(0, pattern0.length); //获取指定前缀长度的 byte[]

                        slice.resetReaderIndex();

                        //key有序 不相等后面无数据
                        if (Arrays.equals(slice.readBytes(slice.readableBytes()).array(), pattern0)) {


                            //获取指定索引的数据
                            ByteBuf val0 = listNode.getVal0(iterator.value());
                            if (Arrays.equals(deleteValue, toByteArray(val0)) && count<=cnt) {

                                keys.add(Unpooled.wrappedBuffer(iterator.key()));
                                db.delete(iterator.key());
                                count++;
                            }

                            index++;

                        } else {
                            break;
                        }
                    } else break;

                }
            }else{

                //从底部反向查询；
                iterator.seekToLast();
                for (iterator.seekForPrev(pattern0); iterator.isValid(); iterator.prev()) {

                    //确保检索有数据，hkeybuf.slice 不错误
                    if (pattern0.length <= iterator.key().length) {
                        ByteBuf hkeybuf = Unpooled.wrappedBuffer(iterator.key()); //优化 零拷贝
                        ByteBuf slice = hkeybuf.slice(0, pattern0.length); //获取指定前缀长度的 byte[]

                        slice.resetReaderIndex();

                        //key有序 不相等后面无数据
                        if (Arrays.equals(slice.readBytes(slice.readableBytes()).array(), pattern0)) {

                            //获取指定索引的数据
                            if (Arrays.equals(deleteValue,iterator.value()) && count<=cnt) {

                                keys.add(Unpooled.wrappedBuffer(iterator.key()));
                                db.delete(iterator.key());
                                count++;
                            }

                            index++;

                        } else {
                            break;
                        }
                    } else break;

                }

            }

        } catch (RocksDBException e) {
            e.printStackTrace();
            Throwables.propagateIfPossible(e, RedisException.class);
        }

        return keys;
    }


    /**
     * 保留指定区间内的数据
     *
     * @param db
     * @param pattern0
     * @param start
     * @param stop
     * @return
     * @throws RedisException
     */
    protected static List<ByteBuf> ztrimBy(RocksDB db, byte[] pattern0,long start,long stop) throws RedisException {
        //按 key 检索所有数据
        List<ByteBuf> keys = new ArrayList<ByteBuf>();
        try (final RocksIterator iterator = db.newIterator()) {
            int index = 0;

                //从开始查询
                for (iterator.seek(pattern0); iterator.isValid(); iterator.next()) {

                    //确保检索有数据，hkeybuf.slice 不错误
                    if (pattern0.length <= iterator.key().length) {
                        ByteBuf hkeybuf = Unpooled.wrappedBuffer(iterator.key()); //优化 零拷贝
                        ByteBuf slice = hkeybuf.slice(0, pattern0.length); //获取指定前缀长度的 byte[]

                        slice.resetReaderIndex();

                        //key有序 不相等后面无数据
                        if (Arrays.equals(slice.readBytes(slice.readableBytes()).array(), pattern0)) {

                            //获取指定索引的数据
                            if(index<start || index>stop){
                                keys.add(Unpooled.wrappedBuffer(iterator.key()));

                                log.debug(toString(iterator.value()));
                                db.delete(iterator.key());
                            }

                            index++;

                        } else {
                            break;
                        }
                    } else break;

                }


        } catch (RocksDBException e) {
            e.printStackTrace();
            Throwables.propagateIfPossible(e, RedisException.class);
        }

        return keys;
    }

    /**
     * Redis Lindex 命令用于通过索引获取列表中的元素。你也可以使用负数下标，以 -1 表示列表的最后一个元素，
     * -2 表示列表的倒数第二个元素，以此类推。
     *
     * @param index1
     * @return
     * @throws RedisException
     */
    public BulkReply lindex(byte[] index1) throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_LIST)) return NIL_REPLY;

        long i = RocksdbRedis.bytesToInt(index1);

        if (i<0){
            i = getCount()-i;
        }

        List<ByteBuf> list = zgetBy(db, genKeyPartten(DataType.KEY_LIST_ELEMENT), i, i, false,false,null);
        if (list.isEmpty()){
            return NIL_REPLY;
        }else{

            log.debug(toString(list.get(0)));

            return new BulkReply(list.get(0));
        }

    }

    /**
     * Redis Lrange 返回列表中指定区间内的元素，区间以偏移量 START 和 END 指定。 其中 0 表示列表的第一个元素，
     * 1 表示列表的第二个元素，以此类推。 你也可以使用负数下标，以 -1 表示列表的最后一个元素， -2 表示列表的倒数第二个元素，
     * 以此类推。
     *
     * @param start1
     * @param stop2
     * @return
     * @throws RedisException
     */
    public MultiBulkReply lrange(byte[] start1, byte[] stop2) throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_LIST)) return EMPTY;

        long s = RocksdbRedis.__torange(start1, getCount());
        long e = RocksdbRedis.__torange(stop2, getCount());

        if (s<0){
            s = getCount()-s;
        }
        if (e<0){
            e = getCount()-e;
        }
        List<ByteBuf> list = zgetBy(db, genKeyPartten(DataType.KEY_LIST_ELEMENT), s, e, false,false,null);

        if (list.isEmpty()){
            return EMPTY;

        }else{
            log.debug(list.size());
            List<Reply<ByteBuf>> replies = new ArrayList<Reply<ByteBuf>>();

            for (ByteBuf k : list
            ) {
                replies.add(new BulkReply(toByteArray(k)));
            }

            return new MultiBulkReply(replies.toArray(new Reply[replies.size()]));
        }


    }

    /**
     * Redis Lpushx 将一个值插入到已存在的列表头部，列表不存在时操作无效。
     * <p>
     * 非自创建模式
     *
     * @param value1
     * @return
     * @throws RedisException
     */
    public IntegerReply lpushx(byte[] value1) throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_LIST)) return integer(0);
//
//        //todo 判断列表是否存在
//        int type = getMeta().getType();//hashNode.typeBy(db, getKey());
//
//        if (type != -1 && type != DataType.KEY_LIST) {
//            //抛出异常 类型不匹配
//            return integer(0);
//
//        }
        return lpush(value1);
    }

    /**
     * Redis Rpushx 命令用于将一个值插入到已存在的列表尾部(最右边)。如果列表不存在，操作无效。
     *
     * @param value1
     * @return
     * @throws RedisException
     */
    public IntegerReply rpushx(byte[] value1) throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_LIST)) return integer(0);
//
//        //todo 判断列表是否存在
//        int type = getMeta().getType();//hashNode.typeBy(db, getKey());
//
//        if (type != -1 && type != DataType.KEY_LIST) {
//            //抛出异常 类型不匹配
//            return integer(0);
//
//        }

        return rpush(value1);
    }

    /**
     * todo ???  rpop and lpush 命令组合
     * <p>
     * 用于移除列表的最后一个元素，并将该元素添加到另一个列表并返回
     *
     * @param source0
     * @param destination1
     * @return
     * @throws RedisException
     */
//    @Deprecated
    public BulkReply rpoplpush(byte[] source0, byte[] destination1) throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_LIST)) return NIL_REPLY;


        BulkReply rpop = rpop();

        metaKey=getMetaKey(destination1);
        rpush(rpop.data().array());

        return rpop;


    }

    /**
     * Redis Lset 通过索引来设置元素的值。
     * 当索引参数超出范围，或对一个空列表进行 LSET 时，返回一个错误。
     *
     * @param index1
     * @param value2
     * @return
     * @throws RedisException
     */
    public StatusReply lset(byte[] index1, byte[] value2) throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_LIST)) return QUIT;

        int i = RocksdbRedis.bytesToInt(index1);

        List<ByteBuf> list = zgetBy(db, genKeyPartten(DataType.KEY_LIST_ELEMENT), i, i, false,true,value2);

        if (list.isEmpty()){
            throw notInteger();
        }


        return OK;
    }


    /**
     * Redis Lrem 根据参数 COUNT 的值，移除列表中与参数 VALUE 相等的元素。
     * COUNT 的值可以是以下几种：
     * count > 0 : 从表头开始向表尾搜索，移除与 VALUE 相等的元素，数量为 COUNT 。
     * count < 0 : 从表尾开始向表头搜索，移除与 VALUE 相等的元素，数量为 COUNT 的绝对值。
     * count = 0 : 移除表中所有与 VALUE 相等的值。
     *
     * @param count1
     * @param value2
     * @return
     * @throws RedisException
     */
    public IntegerReply lrem(byte[] count1, byte[] value2) throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_LIST)) return integer(0);

        int i = RocksdbRedis.bytesToInt(count1);

        List<ByteBuf> list = zdeleteBy(db, genKeyPartten(DataType.KEY_LIST_ELEMENT),i<0, Math.abs(i), value2);

        return integer(list.size());



    }

    /**
     * 让列表只保留指定区间内的元素，不在指定区间之内的元素都将被删除。
     * <p>
     * Redis Ltrim 对一个列表进行修剪(trim)，就是说，让列表只保留指定区间内的元素，不在指定区间之内的元素都将被删除。
     * 下标 0 表示列表的第一个元素，以 1 表示列表的第二个元素，以此类推。 你也可以使用负数下标，以 -1 表示列表的最后一个元素，
     * -2 表示列表的倒数第二个元素，以此类推。
     *
     * @param start1
     * @param stop2
     * @return
     * @throws RedisException
     */
    public StatusReply ltrim(byte[] start1, byte[] stop2) throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_LIST)) return QUIT;

//        long s = RocksdbRedis.__torange(start1, getCount());
//        long e = RocksdbRedis.__torange(stop2, getCount());

        long s = RocksdbRedis.bytesToInt(start1);
        long e = RocksdbRedis.bytesToInt(stop2);

        if (s<0){s=getCount()+s;}
        if (e<0){e=getCount()+e;} //以 -1 表示列表的最后一个元素 -2 表示列表的倒数第二个元素，以此类推

        log.debug(s);
        log.debug(e);

        List<ByteBuf> list = ztrimBy(db, genKeyPartten(DataType.KEY_LIST_ELEMENT),s,e);

        log.debug(list.size());



        return OK;
    }


    /**
     * RocksDb不支持此结构；
     *
     * Redis Linsert 命令用于在列表的元素前或者后插入元素。当指定元素不存在于列表中时，不执行任何操作。
     * 当列表不存在时，被视为空列表，不执行任何操作。
     * 如果 key 不是列表类型，返回一个错误。
     *
     * @param where1
     * @param pivot2
     * @param value3
     * @return
     * @throws RedisException
     */
    @Deprecated
    public IntegerReply linsert(byte[] where1, byte[] pivot2, byte[] value3) throws RedisException {
        throw  invalidValue();


    }

//
//    public void destory() throws RedisException {
//        try {
//            db.delete(getKey());
//        } catch (RocksDBException e) {
//            throw new RedisException(e.getMessage());
//        }
//    }

    /**
     * 尾节点
     * @return
     * @throws RedisException
     */
    protected ListNode getLastNode() throws RedisException {

//        return listNode.create().genKey(getKey0(), getEseq()).get();

        byte[] lastkey = lastNode(db, genKeyPartten(DataType.KEY_LIST_ELEMENT));
        if (lastkey == null){
            return null;
        }else{
            return listNode.create().getNode(lastkey);
        }

    }

    /**
     * 首节点
     * @return
     * @throws RedisException
     */
    protected ListNode getFirstNode() throws RedisException {

        byte[] firstkey = firstNode(db, genKeyPartten(DataType.KEY_LIST_ELEMENT));
//        log.debug(firstkey);

        if (firstkey == null){
            return null;
        }else{
//            log.debug(new String(firstkey));
            return listNode.create().getNode(firstkey);
        }

    }


    public static void main(String[] args) throws Exception {
//        testListL();
//        testListR();
    }
//
//    private static void testListR() throws RedisException {
//        //测试删除
//        ListMeta metaRPush = ListMeta.getInstance(RocksdbRedis.mydata, "redis".getBytes());
//        metaRPush.genMetaKey("ListRight".getBytes()).clearMetaDataNodeData(metaRPush.getKey0());
//
//        //测试 rpush
//        byte[][] rpusharray = {"XXX".getBytes(), "YYY".getBytes(), "ZZZ".getBytes(), "111".getBytes(), "222".getBytes(), "333".getBytes()};
//
//        Assert.assertEquals(metaRPush.rpush(rpusharray).data().intValue(), 6);
//
//        Assert.assertEquals(metaRPush.getCount(), 6);
//        Assert.assertEquals(metaRPush.getSseq(), metaRPush.getCseq());
//
//        metaRPush.info();
//
//
//        String[] strings3 = {"XXX", "YYY", "ZZZ"};
//        Assert.assertEquals(metaRPush.lrange("0".getBytes(), "2".getBytes()).toString(), Arrays.asList(strings3).toString());
//
//
//        log.debug("========================2 !!!");
//
//        ListNode n3 = ListNode.getInstance(RocksdbRedis.mydata, "redis".getBytes());
//
//        n3.genKey("ListRight".getBytes(), metaRPush.getCseq()+0).get().info();
//        n3.genKey("ListRight".getBytes(), metaRPush.getCseq()+1).get().info();
//        n3.genKey("ListRight".getBytes(), metaRPush.getCseq()+2).get().info();
//        n3.genKey("ListRight".getBytes(), metaRPush.getCseq()+3).get().info();
//        n3.genKey("ListRight".getBytes(), metaRPush.getCseq()+4).get().info();
//        n3.genKey("ListRight".getBytes(), metaRPush.getCseq()+5).get().info();
//
//
//        n3.genKey("ListRight".getBytes(), metaRPush.getCseq()+2).get();
//
//        Assert.assertEquals(n3.getSize(), 3);
//
//        Assert.assertArrayEquals(n3.getVal0(), "ZZZ".getBytes());
//        log.debug("========================1 !!!");
//
//        ListNode n2 = ListNode.getInstance(RocksdbRedis.mydata, "redis".getBytes());
//        n2.genKey("ListRight".getBytes(), metaRPush.getCseq()+1).get();
//
//        Assert.assertEquals(n2.getSize(), 3);
//
//        Assert.assertArrayEquals(n2.getVal0(), "YYY".getBytes());
//        log.debug("========================0 !!!");
//
//
//
//        ListNode n1 = ListNode.getInstance(RocksdbRedis.mydata, "redis".getBytes());
//        n1.genKey("ListRight".getBytes(), metaRPush.getCseq()+0).get();
//
//        Assert.assertEquals(n1.getSize(), 3);
//        Assert.assertArrayEquals(n1.getVal0(), "XXX".getBytes());
//
//
//        Assert.assertArrayEquals(metaRPush.rpop().data().array(), "333".getBytes());
//
//        Assert.assertEquals(metaRPush.getCount(), 5);
//
//        Assert.assertEquals(metaRPush.llen().data().intValue(), 5);
//    }
//
//    /**
//     * 左侧插入数据集测试
//     *
//     * @throws RedisException
//     */
//    private static void testListL() throws RedisException {
//
//        ListMeta metaLPush = ListMeta.getInstance(RocksdbRedis.mydata, "redis".getBytes());
//        metaLPush.genMetaKey("LPUSH".getBytes()).clearMetaDataNodeData(metaLPush.getKey0());
//
//        byte[][] lpusharray = {"XXX".getBytes(), "YYY".getBytes(), "ZZZ".getBytes(), "111".getBytes(), "222".getBytes(), "333".getBytes()};
//        Assert.assertEquals(metaLPush.lpush(lpusharray).data().intValue(), 6);
//
//        Assert.assertEquals(metaLPush.getCount(), 6);
//        log.debug(metaLPush.getSseq());
////        Assert.assertEquals(metaLPush.getSseq(), metaLPush.getCseq());
//        Assert.assertEquals(metaLPush.getEseq(), metaLPush.getCseq());
////        Assert.assertEquals(metaLPush.getCseq(), 5);
//        Assert.assertArrayEquals("LPUSH".getBytes(), metaLPush.getKey0());
//
//
//        log.debug("========================2 !!!");
//
//        ListNode n3 = ListNode.getInstance(RocksdbRedis.mydata, "redis".getBytes());
//        n3.genKey("LPUSH".getBytes(), metaLPush.getCseq()-2).get();
//        n3.info();
//
//        Assert.assertEquals(n3.getSize(), 3);
//
//        Assert.assertArrayEquals(n3.getVal0(), "ZZZ".getBytes());
//
//        log.debug("========================1 !!!");
//
//        ListNode n2 = n3.genKey("LPUSH".getBytes(), metaLPush.getCseq()-1).get();
//
//        Assert.assertEquals(n2.getSize(), 3);
//
//        Assert.assertArrayEquals(n2.getVal0(), "YYY".getBytes());
//        log.debug("========================0 !!!");
//
//
//        ListNode n1 = n3.genKey("LPUSH".getBytes(), metaLPush.getCseq()-0).get();
//
//        Assert.assertEquals(n1.getSize(), 3);
//        Assert.assertArrayEquals(n1.getVal0(), "XXX".getBytes());
//
//        log.debug("========================LPOP !!!");
//
//        Assert.assertArrayEquals(metaLPush.lpop().data().array(), "333".getBytes());
//
//        Assert.assertEquals(metaLPush.getCount(), 5);
//        Assert.assertEquals(metaLPush.getSseq(), metaLPush.getCseq()-metaLPush.getCount());
//
//        Assert.assertEquals(metaLPush.llen().data().intValue(), 5);
//
//        log.debug("========================1 LINDEX !!!");
//
//
//        Assert.assertEquals(toString(metaLPush.lindex("1".getBytes()).data()), "111");
//
//        Assert.assertEquals(metaLPush.lset("1".getBytes(), "Modify".getBytes()), OK);
//
//
//        Assert.assertEquals(toString(metaLPush.lindex("1".getBytes()).data()), "Modify");
//
//        Assert.assertEquals(metaLPush.lrange("0".getBytes(), "-1".getBytes()).data().length, 5);
//
//        log.debug("========================2 LRANGE !!!");
//
//        n3.genKey("LPUSH".getBytes(), metaLPush.getCseq()-0).get().info();
//        n3.genKey("LPUSH".getBytes(), metaLPush.getCseq()-1).get().info();
//        n3.genKey("LPUSH".getBytes(), metaLPush.getCseq()-2).get().info();
//        n3.genKey("LPUSH".getBytes(), metaLPush.getCseq()-3).get().info();
//        n3.genKey("LPUSH".getBytes(), metaLPush.getCseq()-4).get().info();
//
//        log.debug("========================2 LRANGE !!!");
//
//        String[] strings = {"222", "Modify", "ZZZ"};
//        Assert.assertEquals(metaLPush.lrange("0".getBytes(), "2".getBytes()).toString(), Arrays.asList(strings).toString());
//
////        log.debug("*********count 111:"+metaLPush.getCount());
//        Assert.assertEquals(metaLPush.lrem("1".getBytes(), "Modify".getBytes()).data().intValue(), 1);
//
//
//
////        log.debug("**********count 222:"+metaLPush.getCount());
//
//        Assert.assertEquals(metaLPush.lrange("0".getBytes(), "-1".getBytes()).data().length, 4);
//
//        log.debug("--------------------------- !!!");
//
//
//        String[] strings1 = {"222", "ZZZ", "YYY", "XXX"};
//
//        metaLPush.info();
//
//        Assert.assertEquals(metaLPush.lrange("0".getBytes(), "-1".getBytes()).toString(), Arrays.asList(strings1).toString());
//
//        Assert.assertEquals(metaLPush.ltrim("1".getBytes(), "2".getBytes()).toString(), "OK");
//
//        Assert.assertEquals(metaLPush.lrange("0".getBytes(), "-1".getBytes()).data().length, 2);
//        String[] strings2 = {"ZZZ", "YYY"};
//        Assert.assertEquals(metaLPush.lrange("0".getBytes(), "-1".getBytes()).toString(), Arrays.asList(strings2).toString());
//
////        Assert.fail(metaLPush.linsert("BEFORE".getBytes(), "YYY".getBytes(), "OOO".getBytes()));
////        String[] strings3 = {"ZZZ", "OOO", "YYY"};
////        Assert.assertEquals(metaLPush.lrange("0".getBytes(), "-1".getBytes()).toString(), Arrays.asList(strings3).toString());
//    }

}
