package redis.server.netty.rocksdb;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import redis.netty4.*;
import redis.server.netty.RedisException;
import redis.server.netty.utis.DataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static redis.netty4.BulkReply.NIL_REPLY;
import static redis.netty4.IntegerReply.integer;
import static redis.netty4.StatusReply.OK;
import static redis.server.netty.rocksdb.ListLinkMeta.Meta.*;
import static redis.server.netty.rocksdb.RedisBase.invalidValue;
import static redis.server.netty.rocksdb.RedisBase.notInteger;


/**
 * todo 只支持push pop 指令，采用rocks原生的数据结构,减少交互次数，提高数据处理效率；
 * <p>
 * List Meta 元素方便促常用操作
 * List        [<ns>] <key> KEY_META                 KEY_LIST <MetaObject>
 * [<ns>] <key> KEY_LIST_ELEMENT <index> KEY_LIST_ELEMENT <element-value>
 * <p>
 * 采用链表实现 RocksDb 对List 数据类型的支持，与硬盘交互次数较多，性能影响较大，建议优先使用hash 数据类型。
 *
 * <p>
 * Created by moyong on 2017/11/9.
 * Update by moyong on 2018/10/22
 */
public class ListLinkMeta extends BaseMeta {

    private static Logger log = Logger.getLogger(ListLinkMeta.class);


    enum Meta {
        COUNT, SSEQ, ESEQ, CSEQ
    }


    private byte[] key;
    private byte[] val;


    private long ttl;
    private int size;


    public long getVal0() throws RedisException {
        print(metaVal);
        metaVal.resetReaderIndex();
        return metaVal.getLong(8 + 4 + 2);
    }

    public void setVal0(long val0) throws RedisException {
        this.metaVal.setLong(8 + 4 + 2, val0);  //数量
    }

    public byte[] getVal() throws RedisException {
        this.metaVal.resetReaderIndex();
        return this.metaVal.readBytes(metaVal.readableBytes()).array();
    }

    public byte[] getVal(ByteBuf val) throws RedisException {
        val.resetReaderIndex();
        return val.readBytes(val.readableBytes()).array();
    }

    public long getTtl() {
        return metaVal.getLong(0);
    }

    public int getType() throws RedisException {
        if (metaVal == null) return -1;
        return this.metaVal.getInt(8 + 1);
    }


    protected static ListLinkNode listNode;

    private ListLinkMeta() {
    }

    private static ListLinkMeta instance = new ListLinkMeta();


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
    public static ListLinkMeta getInstance(RocksDB db0, byte[] ns0) {
        instance.db = db0;
        instance.NS = ns0;
        listNode = ListLinkNode.getInstance(db0, ns0);
        return instance;
    }

    /**
     * 构造 MetaKey
     *
     * @param key0
     * @return
     * @throws RedisException
     */
    public ListLinkMeta genMetaKey(byte[] key0) throws RedisException {
        if (key0 == null) {
            throw new RedisException(String.format("key0 主键不能为空"));
        }
        instance.metaKey = Unpooled.wrappedBuffer(instance.NS, DataType.SPLIT, key0, DataType.SPLIT, TYPE);
        return instance;
    }


    private ByteBuf genMetaVal(long count, long sseq, long eseq, long cseq) {

        ByteBuf ttlBuf = Unpooled.buffer(12);

        ttlBuf.writeLong(-1); //ttl 无限期 -1
        ttlBuf.writeBytes(DataType.SPLIT);

        ttlBuf.writeInt(DataType.KEY_LIST); //value type
        ttlBuf.writeBytes(DataType.SPLIT);


        ByteBuf val1 = Unpooled.buffer(32);
        val1.writeLong(count);  //数量
        val1.writeLong(sseq);    //第一个元素
        val1.writeLong(eseq);    //最后一个元素
        val1.writeLong(cseq);    //最新主键编号

        log.debug(String.format("count:%d 第一个元素：%d 最后一个元素：%d 自增主键：%d  value:%s", count, sseq, eseq, sseq, new String(val1.readBytes(val1.readableBytes()).array())));


        val1.resetReaderIndex();
        ttlBuf.resetReaderIndex();

        return Unpooled.wrappedBuffer(ttlBuf, val1);

    }

    /**
     * 创建meta key
     *
     * @param count
     * @return
     * @throws RedisException
     */
    protected ListLinkMeta setMeta(long count, long sseq, long eseq, long cseq) throws RedisException {

        this.metaVal = genMetaVal(count, sseq, eseq, cseq);

        log.debug(String.format("count:%d;  主键：%s; value:%s", count, getKey0Str(), getVal0()));

        try {
            db.put(getKey(), getVal());
        } catch (RocksDBException e) {
            e.printStackTrace();
            throw new RedisException(e.getMessage());
        }


        return this;
    }

    /**
     * 获取meta 数据
     *
     * @return
     * @throws RedisException
     */
    protected ListLinkMeta getMeta() throws RedisException {

        try {
            byte[] value = db.get(getKey());
            if (value == null) this.metaVal = null;
            else
                this.metaVal = Unpooled.wrappedBuffer(value);
        } catch (RocksDBException e) {
            e.printStackTrace();
            throw new RedisException(e.getMessage());
        }

        return this;
    }

    ///////////////
    public ListLinkMeta(RocksDB db0, byte[] key0, long count, long sseq, long eseq, long cseq) throws RedisException {
        this.db = db0;
        create(key0, count, sseq, eseq, cseq);
    }


    protected ListLinkMeta create(byte[] key0, long count, long sseq, long eseq, long cseq) throws RedisException {
//        this.metaKey = Unpooled.wrappedBuffer("+".getBytes(), key0, "list".getBytes());
//        this.key = metaKey.readBytes(metaKey.readableBytes()).array();

        ByteBuf valbuf = genMetaVal(count, sseq, eseq, cseq);

        try {
            db.put(key, valbuf.readBytes(valbuf.readableBytes()).array());
        } catch (RocksDBException e) {
            throw new RedisException(e.getMessage());
        }

        return this;
    }


    /**
     * 持久化数据到硬盘
     *
     * @throws RedisException
     */
    public void flush() throws RedisException {

        try {
            db.put(getKey(), getVal());
        } catch (RocksDBException e) {
            throw new RedisException(e.getMessage());
        }
    }


    /**
     * 元素数量
     *
     * @return
     */
    public long getCount() throws RedisException {
        if (metaVal == null) return 0;

        return get(COUNT);
    }

    public void setCount(long val) throws RedisException {
        set(COUNT, val);
    }

    /**
     * 首元素
     *
     * @return
     */
    public long getSseq() throws RedisException {
        return get(SSEQ);
    }

    public void setSseq(long val) throws RedisException {
        set(SSEQ, val);
    }

    /**
     * 尾元素
     *
     * @return
     */
    public long getEseq() throws RedisException {
        return get(ESEQ);
    }

    public void setEseq(long val) throws RedisException {
        set(ESEQ, val);
    }

    /**
     * 自增主键
     *
     * @return
     */
    public long getCseq() throws RedisException {
        return get(CSEQ);
    }

    public void setCseq(long val) throws RedisException {
        set(CSEQ, val);
    }

    public long incrCseq() throws RedisException {

        long pval = Math.incrementExact(getCseq());
        set(CSEQ, pval);
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

//        if (metaVal==null) return result;

        this.metaVal.resetReaderIndex();
        int ttlSize = 8 + 4 + 2;
        ByteBuf linkBuf = metaVal.slice(ttlSize, metaVal.readableBytes() - ttlSize);


        switch (fd) {

            case COUNT:
                result = linkBuf.getLong(0);
                break;

            case SSEQ:
                result = linkBuf.getLong(8);
                break;

            case ESEQ:
                result = linkBuf.getLong(16);
                break;

            case CSEQ:
                result = linkBuf.getLong(24);
                break;

            default:
                log.debug("default");
                throw new RedisException(String.format("没有如此的字段:%s", fd));
        }
        return result;
    }

    private long set(Meta fd, long pval) throws RedisException {
        long result = 0;

        //metaVal=Unpooled.wrappedBuffer(val);
        int ttlSize = 8 + 4 + 2;


        switch (fd) {

            case COUNT:
                metaVal.setLong(ttlSize, pval);
                break;

            case SSEQ:
                metaVal.setLong(ttlSize + 8, pval);
                break;

            case ESEQ:
                metaVal.setLong(ttlSize + 16, pval);
                break;

            case CSEQ:
                metaVal.setLong(ttlSize + 24, pval);
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


    public int getSize() {
        return val.length;
    }

    public boolean isNew() throws RedisException {
        return getCount() == 0;
    }


    public String info() throws RedisException {
        StringBuilder sb = new StringBuilder("meta data ~ ");
        sb.append(getKey0Str());
        sb.append(":");
        sb.append("  count=");
        sb.append(getCount());

        sb.append("  sseq=");
        sb.append(getSseq());

        sb.append("  eseq=");
        sb.append(getEseq());

        sb.append("  cseq=");
        sb.append(getCseq());

        log.debug(sb.toString());

        return sb.toString();
    }


    /**
     * plush 指令
     * 将一个或多个值插入到列表头部。 如果 key 不存在，一个空列表会被创建并执行 LPUSH 操作。 当 key 存在但不是列表类型时，返回一个错误。
     *
     * @param value1
     * @return
     * @throws RedisException
     */
    public IntegerReply lpush(byte[]... value1) throws RedisException {
        //判断类型，非hash 类型返回异常信息；
        int type = getMeta().getType();//hashNode.typeBy(db, getKey());

        if (type != -1 && type != DataType.KEY_HASH) {
            //抛出异常 类型不匹配
            throw invalidValue();
        }

        //批量处理
        final WriteOptions writeOpt = new WriteOptions();
        final WriteBatch batch = new WriteBatch();

        for (byte[] val1 : value1
        ) {


            if (isNew()) {

                listNode.genKey(getKey0(), 0).put(val1, -1, -1);
                listNode.info();
                //设置meta 参数
                setMeta(1, listNode.getSeq(), listNode.getSeq(), listNode.getSeq());//fixme

                setCseq(listNode.getSeq());
                setSseq(listNode.getSeq());
                setEseq(listNode.getSeq());

            } else {

//                batch.put(newKey, curVal);
                //头部节点
                ListLinkNode firstNode = getFirstNode();

                firstNode.info();

                //meta=主键++
                incrCseq();

                //新建元素
                listNode.genKey(getKey0(), getCseq()).put(val1, -1, firstNode.getSeq());


                //变更元素指针


                long seq1 = listNode.getSeq();
                listNode.info();

                firstNode.setPseq(seq1);  //fixme
                firstNode.flush();  ///////go go go
                firstNode.info();

                //meta-开始指针
                setSseq(getCseq());

                incrCount();

            }


            this.info();
        }

        this.flush();

        return integer(value1.length);

    }

    /**
     * Redis Rpush 命令用于将一个或多个值插入到列表的尾部(最右边)。
     * 如果列表不存在，一个空列表会被创建并执行 RPUSH 操作。 当列表存在但不是列表类型时，返回一个错误。
     *
     * @param value1
     * @return
     * @throws RedisException
     */
    public IntegerReply rpush(byte[]... value1) throws RedisException {
        //判断类型，非hash 类型返回异常信息；
        int type = getMeta().getType();//hashNode.typeBy(db, getKey());

        if (type != -1 && type != DataType.KEY_HASH) {
            //抛出异常 类型不匹配
            throw invalidValue();
        }


        //批量处理
        final WriteOptions writeOpt = new WriteOptions();
        final WriteBatch batch = new WriteBatch();

        for (byte[] val1 : value1
        ) {

            if (isNew()) {

                listNode.genKey(getKey0(), 0).put(val1, -1, -1);

                listNode.info();

                setMeta(1, listNode.getSeq(), listNode.getSeq(), listNode.getSeq());//fixme

                setCseq(listNode.getSeq());
                setSseq(listNode.getSeq());
                setEseq(listNode.getSeq());


            } else {

//                batch.put(newKey, curVal);
                ListLinkNode lastNode = getLastNode();

//                getLastNode();
//                lastNode.info();

                incrCseq();

                //新建元素
                listNode.genKey(getKey0(), getCseq()).put(val1, lastNode.getSeq(), -1);

                listNode.info();

                //变更元素指针
                lastNode.setNseq(listNode.getSeq());
                lastNode.flush();

                lastNode.info();

                //meta=尾部指针
                setEseq(getCseq());

                incrCount();


            }

            this.info();
        }

        this.flush();

        return integer(value1.length);

    }

    /**
     * Redis Lpop 命令用于移除并返回列表的第一个元素。
     *
     * @return
     * @throws RedisException
     */
    public BulkReply lpop() throws RedisException {
        ListLinkNode firstNode = getFirstNode();
        this.setSseq(firstNode.getNseq());
        this.decrCount();
        this.flush();
        firstNode.del();
        getFirstNode().setPseq(-1).flush();
        return new BulkReply(firstNode.getVal0());
    }

    /**
     * Redis Rpop 命令用于移除并返回列表的最后一个元素。
     *
     * @return
     * @throws RedisException
     */
    public BulkReply rpop() throws RedisException {
        ListLinkNode lastNode = getLastNode();
        this.setEseq(lastNode.getPseq());
        this.decrCount();
        this.flush();
        lastNode.del();
        getLastNode().setNseq(-1).flush();
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
        return integer(getCount());

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
        int i = RocksdbRedis.bytesToInt(index1);
        ListLinkNode node = getFirstNode();
        for (int j = 0; j < i; j++) {
            ListLinkNode node1 = node.next();
            if (node1 == null) {
                return NIL_REPLY;
            } else
                node = node1;
        }
        return new BulkReply(node.getVal0());
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


        List<BulkReply> results = new ArrayList<BulkReply>();

        long s = RocksdbRedis.__torange(start1, getCount());
        long e = RocksdbRedis.__torange(stop2, getCount());


        ListLinkNode node = getFirstNode();

        for (int j = 0; j < getCount(); j++) {

            if (j >= s && j <= e) {
                results.add(new BulkReply(node.getVal0()));
            }


            if (node.getNseq() == -1) {

                break;
            }


            ListLinkNode node1 = node.next();


            if (node1 == null) {

                break;

            } else
                node = node1;


            //超出范围跳出循环
            if (j > e) {
                break;
            }

        }
        return new MultiBulkReply(results.toArray(new Reply[results.size()]));
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
        //todo 判断列表是否存在
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
        //todo 判断列表是否存在
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


        BulkReply rpop = rpop();

        genMetaKey(destination1).rpush(rpop.data().array());

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
        int i = RocksdbRedis.bytesToInt(index1);
        ListLinkNode node = getFirstNode();
        for (int j = 0; j < i; j++) {
            ListLinkNode node1 = node.next();
            if (node1 == null) {
                throw notInteger();
            } else
                node = node1;
        }

        node.setVal(value2);
        node.flush();
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

        int i = RocksdbRedis.bytesToInt(count1);

        List<BulkReply> results = new ArrayList<BulkReply>();


        int delcnt = 0;

        if (i >= 0) {

            ListLinkNode node = getFirstNode();
            long cnt = getCount();
            for (int j = 0; j < cnt; j++) {

                if (Arrays.equals(node.getVal0(), value2)) {

                    //修复上下节点
                    if (!node.isFirst()) {
                        ListLinkNode prev = node.prev();
                        prev.setNseq(node.getNseq());
                        prev.flush();
                    }

                    if (!node.isLast()) {
                        ListLinkNode next = node.next();
                        next.setPseq(node.getPseq());
                        next.flush();
                    }

                    node.del();

                    delcnt++;

                    decrCount();

                    //更新元数据
                    if (node.isFirst()) {
                        setSseq(node.getNseq());
                    }

                    if (node.isLast()) {
                        setEseq(node.getPseq());
                    }

                }


                ListLinkNode node1 = node.next();

                if (delcnt == i && i != 0) {
                    break;
                }


                if (node1 == null) {
                    break;
                } else
                    node = node1;
            }
        } else { //<0

            ListLinkNode node = getLastNode();

            long cnt = getCount();
            for (int j = 0; j < cnt; j++) {

                if (Arrays.equals(node.getVal0(), value2)) {

                    if (!node.isLast()) { //尾部节点 直接删除

                        //修复上下节点
                        ListLinkNode prev = node.prev();
                        prev.setNseq(node.getNseq());
                        prev.flush();
                    }

                    if (!node.isFirst()) { //头部节点直接删除

                        ListLinkNode next = node.next();
                        next.setPseq(node.getPseq());
                        next.flush();
                    }

                    node.del();

                    delcnt++;

                    decrCount();

                    //更新元数据
                    if (node.isFirst()) {
                        setSseq(node.getNseq());
                    }

                    if (node.isLast()) {
                        setEseq(node.getPseq());
                    }

                }


                ListLinkNode node1 = node.prev();

                if (delcnt == i && i != 0) {
                    break;
                }


                if (node1 == null) {
                    break;
                } else
                    node = node1;
            }

        }

        this.flush();

        return integer(delcnt);

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

        long s = RocksdbRedis.__torange(start1, getCount());
        long e = RocksdbRedis.__torange(stop2, getCount());

        log.debug("ssssss:" + s);
        log.debug("eeeeee:" + e);
        log.debug("eeeeee:" + getCount());

        ListLinkNode node = getFirstNode();

        long cnt = getCount();
        for (int j = 0; j < cnt; j++) {
            log.debug(j);

            node.info();

            if (j < s) {
                //首位置
                node.destory();
                setSseq(node.getNseq());
                decrCount();
            }


            if (j > e) {
                node.destory();

                //结束节点
                if (j - e == 1 || node.isLast()) {
                    setEseq(node.getPseq());
                }
                decrCount();
            }


            ListLinkNode node1 = node.next();

            if (node1 == null) {

                break;
            } else
                node = node1;


        }

        //修正开始与结束节点

        ListLinkNode firstNode = getFirstNode();
        firstNode.setPseq(-1);
        firstNode.flush();
        ;

        ListLinkNode lastNode = getLastNode();
        lastNode.setNseq(-1);
        lastNode.flush();

        this.flush();

        return OK;
    }


    /**
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
    public IntegerReply linsert(byte[] where1, byte[] pivot2, byte[] value3) throws RedisException {
        boolean isBefore = Arrays.equals("BEFORE".getBytes(), where1) || Arrays.equals("before".getBytes(), where1) ? true : false; //false = after

        ListLinkNode node = getFirstNode();

        long cnt = getCount();
        for (int j = 0; j < cnt; j++) {

            if (Arrays.equals(node.getVal0(), pivot2)) {

                listNode.genKey(getKey0(), incrCseq()).put(value3, -1, -1);

                if (isBefore) {
                    //更新元数据
                    if (node.isFirst()) {
                        setSseq(listNode.getSeq());

                    } else {

                        ListLinkNode prev = node.prev();
                        prev.setNseq(listNode.getSeq());
                        listNode.setPseq(prev.getSeq());

                        prev.flush();
                    }

                    node.setPseq(listNode.getSeq());
                    listNode.setNseq(node.getSeq());

                    listNode.flush();
                    node.flush();


                } else {
                    if (node.isLast()) {
                        setEseq(listNode.getSeq());

                    } else {
                        ListLinkNode next = node.next();

                        next.setPseq(listNode.getSeq());
                        listNode.setNseq(next.getSeq());
                        next.flush();
                    }

                    node.setNseq(listNode.getSeq());
                    listNode.setPseq(node.getSeq());

                    listNode.flush();
                    node.flush();

                }

                incrCount();
                break; //只适配一个复合条件的数据
            }

            ListLinkNode node1 = node.next();

            if (node1 == null) {
                break;
            } else
                node = node1;
        }

        this.flush();

        return integer(getCount());

    }


    public void destory() throws RedisException {
        try {
            db.delete(getKey());
        } catch (RocksDBException e) {
            throw new RedisException(e.getMessage());
        }
    }

    protected ListLinkNode getLastNode() throws RedisException {
        return listNode.create().genKey(getKey0(), getEseq()).get();
    }

    protected ListLinkNode getFirstNode() throws RedisException {
        return listNode.create().genKey(getKey0(), getSseq()).get();
    }


    public static void main(String[] args) throws Exception {
        testListL();
        testListR();
    }

    private static void testListR() throws RedisException {
        //测试删除
        ListLinkMeta metaRPush = ListLinkMeta.getInstance(RocksdbRedis.mydata, "redis".getBytes());
        metaRPush.genMetaKey("ListRight".getBytes()).deleteRange(metaRPush.getKey0());

        //测试 rpush
        byte[][] rpusharray = {"XXX".getBytes(), "YYY".getBytes(), "ZZZ".getBytes(), "111".getBytes(), "222".getBytes(), "333".getBytes()};

        Assert.assertEquals(metaRPush.rpush(rpusharray).data().intValue(), 6);

        Assert.assertEquals(metaRPush.getCount(), 6);
        Assert.assertEquals(metaRPush.getSseq(), 0);
        Assert.assertEquals(metaRPush.getEseq(), 5);
        Assert.assertEquals(metaRPush.getCseq(), 5);

        metaRPush.info();


        String[] strings3 = {"XXX", "YYY", "ZZZ"};
        Assert.assertEquals(metaRPush.lrange("0".getBytes(), "2".getBytes()).toString(), Arrays.asList(strings3).toString());


        log.debug("========================2 !!!");

        ListLinkNode n3 = ListLinkNode.getInstance(RocksdbRedis.mydata, "redis".getBytes());

        n3.genKey("ListRight".getBytes(), 0).get().info();
//        n3.genKey("ListRight".getBytes(), 1).get().info();
//        n3.genKey("ListRight".getBytes(), 2).get().info();
//        n3.genKey("ListRight".getBytes(), 3).get().info();
//        n3.genKey("ListRight".getBytes(), 4).get().info();
//        n3.genKey("ListRight".getBytes(), 5).get().info();


        n3.genKey("ListRight".getBytes(), 2).get();

        Assert.assertEquals(n3.getNseq(), 3);
        Assert.assertEquals(n3.getPseq(), 1);
        Assert.assertEquals(n3.getSeq(), 2);
        Assert.assertEquals(n3.getSize(), 3);

        Assert.assertArrayEquals(n3.getVal0(), "ZZZ".getBytes());
        log.debug("========================1 !!!");

        ListLinkNode n2 = ListLinkNode.getInstance(RocksdbRedis.mydata, "redis".getBytes());
        n2.genKey("ListRight".getBytes(), 1).get();

        Assert.assertEquals(n2.getNseq(), 2);
        Assert.assertEquals(n2.getPseq(), 0);
        Assert.assertEquals(n2.getSeq(), 1);
        Assert.assertEquals(n2.getSize(), 3);

        Assert.assertArrayEquals(n2.getVal0(), "YYY".getBytes());
        log.debug("========================0 !!!");


//        ListNode n1 = new ListNode(RocksdbRedis.mydata, "ListRight".getBytes(), 0);

        ListLinkNode n1 = ListLinkNode.getInstance(RocksdbRedis.mydata, "redis".getBytes());
        n1.genKey("ListRight".getBytes(), 0).get();

        Assert.assertEquals(n1.getNseq(), 1);
        Assert.assertEquals(n1.getPseq(), -1);
        Assert.assertEquals(n1.getSeq(), 0);
        Assert.assertEquals(n1.getSize(), 3);
        Assert.assertArrayEquals(n1.getVal0(), "XXX".getBytes());


        Assert.assertArrayEquals(metaRPush.rpop().data().array(), "333".getBytes());

        Assert.assertEquals(metaRPush.getCount(), 5);
        Assert.assertEquals(metaRPush.getSseq(), 0);
        Assert.assertEquals(metaRPush.getEseq(), 4);
        Assert.assertEquals(metaRPush.getCseq(), 5);

        Assert.assertEquals(metaRPush.llen().data().intValue(), 5);
    }

    /**
     * 左侧插入数据集测试
     *
     * @throws RedisException
     */
    private static void testListL() throws RedisException {

        ListLinkMeta metaLPush = ListLinkMeta.getInstance(RocksdbRedis.mydata, "redis".getBytes());
        metaLPush.genMetaKey("LPUSH".getBytes()).deleteRange(metaLPush.getKey0());

        byte[][] lpusharray = {"XXX".getBytes(), "YYY".getBytes(), "ZZZ".getBytes(), "111".getBytes(), "222".getBytes(), "333".getBytes()};
        Assert.assertEquals(metaLPush.lpush(lpusharray).data().intValue(), 6);

        Assert.assertEquals(metaLPush.getCount(), 6);
        Assert.assertEquals(metaLPush.getSseq(), 5);
        Assert.assertEquals(metaLPush.getEseq(), 0);
        Assert.assertEquals(metaLPush.getCseq(), 5);
        Assert.assertArrayEquals("LPUSH".getBytes(), metaLPush.getKey0());


        log.debug("========================2 !!!");

        ListLinkNode n3 = ListLinkNode.getInstance(RocksdbRedis.mydata, "redis".getBytes());
        n3.genKey("LPUSH".getBytes(), 2).get();
        n3.info();

        Assert.assertEquals(n3.getNseq(), 1);
        Assert.assertEquals(n3.getPseq(), 3);
        Assert.assertEquals(n3.getSeq(), 2);
        Assert.assertEquals(n3.getSize(), 3);

        Assert.assertArrayEquals(n3.getVal0(), "ZZZ".getBytes());

        log.debug("========================1 !!!");

        ListLinkNode n2 = n3.genKey("LPUSH".getBytes(), 1).get();

        Assert.assertEquals(n2.getNseq(), 0);
        Assert.assertEquals(n2.getPseq(), 2);
        Assert.assertEquals(n2.getSeq(), 1);
        Assert.assertEquals(n2.getSize(), 3);

        Assert.assertArrayEquals(n2.getVal0(), "YYY".getBytes());
        log.debug("========================0 !!!");


//        ListNode n1 = new ListNode(RocksdbRedis.mydata, "LPUSH".getBytes(), 0);
        ListLinkNode n1 = n3.genKey("LPUSH".getBytes(), 0).get();

        Assert.assertEquals(n1.getNseq(), -1);
        Assert.assertEquals(n1.getPseq(), 1);
        Assert.assertEquals(n1.getSeq(), 0);
        Assert.assertEquals(n1.getSize(), 3);
        Assert.assertArrayEquals(n1.getVal0(), "XXX".getBytes());

        log.debug("========================LPOP !!!");

        Assert.assertArrayEquals(metaLPush.lpop().data().array(), "333".getBytes());

        Assert.assertEquals(metaLPush.getCount(), 5);
        Assert.assertEquals(metaLPush.getSseq(), 4);
        Assert.assertEquals(metaLPush.getEseq(), 0);
        Assert.assertEquals(metaLPush.getCseq(), 5);

        Assert.assertEquals(metaLPush.llen().data().intValue(), 5);

        log.debug("========================1 LINDEX !!!");

        Assert.assertArrayEquals(metaLPush.lindex("1".getBytes()).data().array(), "111".getBytes());

        Assert.assertEquals(metaLPush.lset("1".getBytes(), "Modify".getBytes()), OK);


        Assert.assertArrayEquals(metaLPush.lindex("1".getBytes()).data().array(), "Modify".getBytes());

        Assert.assertEquals(metaLPush.lrange("0".getBytes(), "-1".getBytes()).data().length, 5);

        log.debug("========================2 LRANGE !!!");

        n3.genKey("LPUSH".getBytes(), 0).get().info();
        n3.genKey("LPUSH".getBytes(), 1).get().info();
        n3.genKey("LPUSH".getBytes(), 2).get().info();
        n3.genKey("LPUSH".getBytes(), 3).get().info();
        n3.genKey("LPUSH".getBytes(), 4).get().info();

        log.debug("========================2 LRANGE !!!");

        String[] strings = {"222", "Modify", "ZZZ"};
        Assert.assertEquals(metaLPush.lrange("0".getBytes(), "2".getBytes()).toString(), Arrays.asList(strings).toString());

//        log.debug("*********count 111:"+metaLPush.getCount());
        Assert.assertEquals(metaLPush.lrem("1".getBytes(), "Modify".getBytes()).data().intValue(), 1);


//        n3.genKey("LPUSH".getBytes(), 0).get().info();
//        n3.genKey("LPUSH".getBytes(), 1).get().info();
//        n3.genKey("LPUSH".getBytes(), 2).get().info();
//        n3.genKey("LPUSH".getBytes(), 4).get().info();

//        log.debug("**********count 222:"+metaLPush.getCount());

        Assert.assertEquals(metaLPush.lrange("0".getBytes(), "-1".getBytes()).data().length, 4);

        log.debug("--------------------------- !!!");


        String[] strings1 = {"222", "ZZZ", "YYY", "XXX"};

        metaLPush.info();

        Assert.assertEquals(metaLPush.lrange("0".getBytes(), "-1".getBytes()).toString(), Arrays.asList(strings1).toString());

        Assert.assertEquals(metaLPush.ltrim("1".getBytes(), "2".getBytes()).toString(), "OK");
        Assert.assertEquals(metaLPush.lrange("0".getBytes(), "-1".getBytes()).data().length, 2);
        String[] strings2 = {"ZZZ", "YYY"};
        Assert.assertEquals(metaLPush.lrange("0".getBytes(), "-1".getBytes()).toString(), Arrays.asList(strings2).toString());

        Assert.assertEquals(metaLPush.linsert("BEFORE".getBytes(), "YYY".getBytes(), "OOO".getBytes()).data().longValue(), metaLPush.getCount());
        String[] strings3 = {"ZZZ", "OOO", "YYY"};
        Assert.assertEquals(metaLPush.lrange("0".getBytes(), "-1".getBytes()).toString(), Arrays.asList(strings3).toString());
    }

}
