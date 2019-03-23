package redis.server.netty.rocksdb;

import com.google.common.base.Throwables;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.rocksdb.*;
import org.supermy.util.MyUtils;
import redis.netty4.BulkReply;
import redis.netty4.IntegerReply;
import redis.netty4.MultiBulkReply;
import redis.netty4.Reply;
import redis.server.netty.RedisException;
import redis.server.netty.utis.DataType;

import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static redis.netty4.BulkReply.NIL_REPLY;
import static redis.netty4.IntegerReply.integer;
import static redis.netty4.MultiBulkReply.EMPTY;
import static redis.server.netty.rocksdb.RedisBase.invalidValue;
import static redis.server.netty.rocksdb.RedisBase.notInteger;
import static redis.server.netty.rocksdb.RocksdbRedis._toint;
import static redis.util.Encoding.bytesToNum;
import static redis.util.Encoding.numToBytes;


/**
 * Set Meta 元素方便促常用操作
 * <p>
 * Sorted Set  [<ns>] <key> KEY_META                 KEY_ZSET <MetaObject>
 * [<ns>] <key> KEY_ZSET_SCORE <member>  KEY_ZSET_SCORE <score>
 * [<ns>] <key> KEY_ZSET_SORT <score> <member> KEY_ZSET_SORT
 * </p>
 *
 * <p>
 * key and value 都采用 | 分隔符号
 * * getKey 一般是包含组合键；
 * * getkey0 是纯粹的业务主键；
 * * 参见setVal0 long+int+int ttl,数据类型,数据长度；
 * * val 一般是包含ttl 的数据；val0是实际的业务数据
 * <p>
 * Created by moyong on 2018/11/01.
 * Update by moyong on 2018/11/01
 * </p>
 */
public class ZSetMeta extends BaseMeta {

    private static Logger log = Logger.getLogger(ZSetMeta.class);


    protected static ZSetScoreNode zsetScoreNode;
    protected static ZSetRankNode zsetRankNode;

    private ZSetMeta() {
    }

    private static ZSetMeta instance = new ZSetMeta();

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
    public static ZSetMeta getInstance(RocksDB db0, byte[] ns0) {
        instance.db = db0;
        instance.NS = ns0;
        instance.VAlTYPE = DataType.KEY_ZSET;
        zsetScoreNode = ZSetScoreNode.getInstance(db0, ns0);
        zsetRankNode = ZSetRankNode.getInstance(db0, ns0);
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
    public ZSetMeta genMetaKey(byte[] key0) throws RedisException {
        if (key0 == null) {
            throw new RedisException(String.format("key0 主键不能为空"));
        }
        instance.metaKey =  MyUtils.concat(instance.NS, DataType.SPLIT, key0, DataType.SPLIT, KEYTYPE);
        return instance;
    }

//    /**
//     * 保存元素数量
//     * 保存score 范围 1,20
//     *
//     * @param count
//     * @return
//     */
//    private ByteBuf genMetaVal(long count) {
//        ByteBuf val = Unpooled.buffer(8);
//
//        val.writeLong(-1); //ttl 无限期 -1
//        val.writeBytes(DataType.SPLIT);
//
//        val.writeInt(DataType.KEY_ZSET); //long 8 bit
//        val.writeBytes(DataType.SPLIT);
//
//        val.writeLong(count);  //数量
////        val.writeBytes(DataType.SPLIT);
//
//        return val;
//    }

//    /**
//     * 创建meta key
//     *
//     * @param count
//     * @return
//     * @throws RedisException
//     */
//    protected ZSetMeta setMeta(long count) throws RedisException {
//
//        this.metaVal = genMetaVal(count);
//
//        log.debug(String.format("count:%d;  主键：%s; value:%s", count, getKey0Str(), getVal0()));
//
//        try {
//            db.put(getKey(), getVal());//fixme
//        } catch (RocksDBException e) {
//            e.printStackTrace();
//            throw new RedisException(e.getMessage());
//        }
//
//        return this;
//    }

// 重构之后走缓存创建，缓存获取
//    /**
//     * 获取meta 数据
//     *
//     * @return
//     * @throws RedisException
//     */
//    protected ZSetMeta getMeta() throws RedisException {
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


//    public long getVal0() throws RedisException {
////        log.debug(MyUtils.ByteBuf2String(metaVal));
////        log.debug(metaVal.array().length);
////        try {
////            log.debug(new String(db.get("redis|ZSet|1".getBytes())));
////        } catch (RocksDBException e) {
////            e.printStackTrace();
////        }
////        metaVal.resetReaderIndex();
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


//    public void setCount(long val) throws RedisException {
//        setVal0(val);
//    }



//
//    public String info() throws RedisException {
//
//        StringBuilder sb = new StringBuilder(getKey0Str());
//
//        sb.append(":");
//        sb.append("  count=");
//        sb.append(getCount());
//
//        log.debug(sb.toString());
//
//        return sb.toString();
//    }


    /**
     * 异步进行hash计数
     */
    class MetaCountCaller implements Runnable {

        private RocksDB db;
        private byte[] key0;
        private byte[] keyPartten;

        public MetaCountCaller(RocksDB db, byte[] key, byte[] keyPartten) {
            this.db = db;
            this.key0 = key;
            this.keyPartten = keyPartten;
        }


        @Override
        public void run() {

            long cnt = countBy(db, keyPartten);

            log.debug(String.format("MetaCountCaller key=%s... cnt:%s",
                    new String(key0),
                    cnt));
            //更新缓存计数，直接修改缓存数据；
            metaCache.put(Unpooled.wrappedBuffer(key0),getMetaVal(cnt,-1));
//            metaCache.invalidate(Unpooled.wrappedBuffer(key0));//持久化到rocksdb

//            try {
//                db.put(key0, MyUtils.toByteArray(getMetaVal(cnt,-1)));
//            } catch (RocksDBException e) {
//                e.printStackTrace();
//            } catch (ExecutionException e) {
//                e.printStackTrace();
//            }


        }
    }


    /**
     * 与keys 分开，减少内存占用；
     *
     * @param db
     * @param pattern0
     * @return
     */
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
    protected static List<ByteBuf> zgetBy(RocksDB db, byte[] pattern0, long start, long stop, boolean delete) throws RedisException {
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
                        if (start <= index && stop >= index) {

                            hkeybuf.resetReaderIndex();
                            ByteBuf member = hkeybuf.slice(pattern0.length, iterator.key().length - pattern0.length); //获取指定前缀长度的 byte[]
                            keys.add(member);


                            //delete score and sort data node;
                            ScoreMember sm = splitScoreMember(member);

                            if (delete) {
                                //delete score|member
                                db.delete(iterator.key());

                                //delete member
                                ByteBuf patternBuf = Unpooled.wrappedBuffer(pattern0); //优化 零拷贝
                                ByteBuf baseBuf = hkeybuf.slice(0, pattern0.length - DataType.KEY_ZSET_SORT.length - 1); //获取指定前缀长度的 byte[]
                                ByteBuf memberBuf = Unpooled.wrappedBuffer(toByteArray(baseBuf), DataType.KEY_ZSET_SCORE, DataType.SPLIT, sm.member); //优化 零拷贝
                                log.debug(toString(memberBuf));
                                memberBuf.resetReaderIndex();
                                db.delete(toByteArray(memberBuf));
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

    /**
     * 按分数提取数据
     *
     * @param db
     * @param pattern0
     * @param min
     * @param max
     * @param reverse
     * @param delete
     * @return
     * @throws RedisException
     */
    protected static List<ScoreMember> zgetBy(RocksDB db, byte[] pattern0, Score min, Score max,
                                              long offset, long num,
                                              boolean reverse, boolean delete) throws RedisException {
        //按 key 检索所有数据
        List<ScoreMember> keys = new ArrayList<ScoreMember>();

        try (final RocksIterator iterator = db.newIterator()) {
            long cur = 0;
            for (iterator.seek(pattern0); iterator.isValid(); iterator.next()) {

                //确保检索有数据，hkeybuf.slice 不错误
                if (pattern0.length <= iterator.key().length) {
                    ByteBuf hkeybuf = Unpooled.wrappedBuffer(iterator.key()); //优化 零拷贝
                    ByteBuf slice = hkeybuf.slice(0, pattern0.length); //获取指定前缀长度的 byte[]

                    slice.resetReaderIndex();

                    //key有序 不相等后面无数据
                    if (Arrays.equals(slice.readBytes(slice.readableBytes()).array(), pattern0)) {


                        //1.获取数据
                        hkeybuf.resetReaderIndex();
                        ByteBuf scoremember = hkeybuf.slice(pattern0.length, iterator.key().length - pattern0.length); //获取指定前缀长度的 byte[]


                        //2.拆解数据，获取分数；
                        ScoreMember sm = splitScoreMember(scoremember);


                        //3.符合分数范围，提取数据；
                        //获取指定索引的数据
                        //fixme 优化考虑使用antlr
                        if (!min.inclusive && min.value == sm.scoreNum) {
                            continue;
                        }

                        if (!max.inclusive && max.value == sm.scoreNum) {
                            continue;
                        }

                        if (min.value <= sm.scoreNum && max.value >= sm.scoreNum) {
                            if (cur >= offset && cur < offset + num) {//分页 LIMIT offset count
                                keys.add(sm);
                            }

                            //可以获取List 进行逐个删除 要删除两种记录  member是不连续的，智能逐个删除；
                            if (delete) {
                                //socre|member
                                db.delete(iterator.key());


                                //member
                                ByteBuf patternBuf = Unpooled.wrappedBuffer(pattern0); //优化 零拷贝
                                ByteBuf baseBuf = hkeybuf.slice(0, pattern0.length - DataType.KEY_ZSET_SORT.length - 1); //获取指定前缀长度的 byte[]
                                ByteBuf memberBuf = Unpooled.wrappedBuffer(toByteArray(baseBuf), DataType.KEY_ZSET_SCORE, DataType.SPLIT, sm.member); //优化 零拷贝
                                log.debug(toString(memberBuf));
                                memberBuf.resetReaderIndex();
                                db.delete(toByteArray(memberBuf));
                            }

                            cur++;
                        }
//                        }


                    } else {
                        break;
                    }
                } else break;

            }
        } catch (RocksDBException e) {
            e.printStackTrace();
            Throwables.propagateIfPossible(e, RedisException.class);
        }

        if (reverse) Collections.reverse(keys);

        return keys;
    }


    /**
     * 按字典提取数据
     *
     *
     * @param db
     * @param pattern0
     * @param min
     * @param max
     * @param reverse
     * @param delete
     * @return
     * @throws RedisException
     */
    protected static List<ScoreMember> zgetBy(RocksDB db, byte[] pattern0, Lex min, Lex max, boolean reverse, boolean delete) throws RedisException {
        //按 key 检索所有数据
        List<ScoreMember> keys = new ArrayList<ScoreMember>();

        List<byte[]> dellist = new ArrayList<byte[]>();

        boolean extract = false;
        try (final RocksIterator iterator = db.newIterator()) {
            for (iterator.seek(pattern0); iterator.isValid(); iterator.next()) {

                //确保检索有数据，hkeybuf.slice 不错误
                if (pattern0.length <= iterator.key().length) {
                    ByteBuf hkeybuf = Unpooled.wrappedBuffer(iterator.key()); //优化 零拷贝
                    ByteBuf slice = hkeybuf.slice(0, pattern0.length); //获取指定前缀长度的 byte[]

                    slice.resetReaderIndex();

                    //key有序 不相等后面无数据
                    if (Arrays.equals(toByteArray(slice), pattern0)) {

                        //1.获取数据
                        hkeybuf.resetReaderIndex();
                        ByteBuf member = hkeybuf.slice(pattern0.length, iterator.key().length - pattern0.length); //获取指定前缀长度的 byte[]


                        ByteBuf minbuf = Unpooled.wrappedBuffer(min.lex); //优化 零拷贝
                        ByteBuf maxbuf = Unpooled.wrappedBuffer(max.lex); //优化 零拷贝

                        //compareTo() 的返回值是int, 它是先比较对应字符的大小(ASCII码顺序)
                        //1、如果字符串相等返回值0
                        //2、如果第一个字符和参数的第一个字符不等,结束比较,返回他们之间的差值（ascii码值）（负值前字符串的值小于后字符串，正值前字符串大于后字符串）
                        //3、如果第一个字符和参数的第一个字符相等,则以第二个字符和参数的第二个字符做比较,以此类推,直至比较的字符或被比较的字符有一方全比较完,这时就比较字符的长度.

//                        log.debug(new String(min.lex));
//                        log.debug(new String(max.lex));
//                        log.debug(new String(toByteArray(member)));

                        member.resetReaderIndex();

                        int imin = minbuf.compareTo(member);
                        int imax = maxbuf.compareTo(member);

//                        log.debug(imin);
//                        log.debug(imax);

//                        log.debug(min.inclusive);
//                        log.debug(max.inclusive);


                        //2.1 获取字典开始位置，开始提取数据；
                        if (imin <= 0) {
                            log.debug("start....");
                            extract = true;
                        }

                        //2.2 获取字典结束位置，结束提取数据；
                        if (imax < 0) {
                            log.debug("end....");

                            extract = false;
                        }

                        //fixme 优化考虑使用antlr
                        if (imin == 0 && !min.inclusive) {
                            log.debug("skip min");
                            continue;
                        }

                        if (imax == 0 && !max.inclusive) {
                            log.debug("skip max");
                            continue;
                        }

                        //3.提取数据
                        if (extract) {

                            log.debug("add +++");


                            ScoreMember sm = new ScoreMember(null, null, member);

                            keys.add(sm);

                            if (delete) {


                                //delete socre|member
                                byte[] val0score = zsetScoreNode.getVal0(Unpooled.wrappedBuffer(iterator.value()));
                                ByteBuf baseBuf = hkeybuf.slice(0, pattern0.length - DataType.KEY_ZSET_SCORE.length - 1); //获取指定前缀长度的 byte[]
                                ByteBuf sortBuf = Unpooled.wrappedBuffer(toByteArray(baseBuf), DataType.KEY_ZSET_SORT,DataType.SPLIT,val0score, DataType.SPLIT, sm.member); //优化 零拷贝
                                dellist.add(toByteArray(sortBuf));

                                //delete member
                                db.delete(iterator.key());

                            }
                        }

                    } else {
                        break;
                    }
                } else break;

            }
        } catch (RocksDBException e) {
            e.printStackTrace();
            Throwables.propagateIfPossible(e,RedisException.class);
        }

//        if (delete) { //fixme 可以获取List 进行逐个删除 sort 或 score 总有一个数据是不连续的。
//            ByteBuf byteBufBegin = Unpooled.wrappedBuffer(pattern0, min.lex);
//            ByteBuf byteBufEnd = Unpooled.wrappedBuffer(pattern0, max.lex);
//
//            byte[] begin = byteBufBegin.readBytes(byteBufBegin.readableBytes()).array();
//            byte[] end = byteBufEnd.readBytes(byteBufEnd.readableBytes()).array();
//
//            try {
//
//                //delete member
//                db.clearMetaDataNodeData(begin, end);
//
//                //delete sort=score+member
//                for (byte[] key : dellist
//                ) {
//                    db.delete(key);
//                }
//
//            } catch (RocksDBException throwable) {
//                throwable.printStackTrace();
//                Throwables.propagateIfPossible(throwable, RedisException.class);
//            }
//
//
//        }

        log.debug(keys.size());

        if (reverse) Collections.reverse(keys);

        return keys;
    }

    /**
     * 返回排名,0-9，a-z,A-Z
     *
     * @param db
     * @param pattern0
     * @param member
     * @return
     */
    protected static int zsortBy(RocksDB db, byte[] pattern0, byte[] member) {
        //按 key 检索所有数据
        try (final RocksIterator iterator = db.newIterator()) {

            int index = 0;
            for (iterator.seek(pattern0); iterator.isValid(); iterator.next()) {

                //确保检索有数据，hkeybuf.slice 不错误
                if (pattern0.length <= iterator.key().length) {
                    ByteBuf hkeybuf = Unpooled.wrappedBuffer(iterator.key()); //优化 零拷贝
                    ByteBuf slice = hkeybuf.slice(0, pattern0.length); //获取指定前缀长度的 byte[]

                    slice.resetReaderIndex();
                    //key有序 不相等后面无数据
                    if (Arrays.equals(toByteArray(slice), pattern0)) {

                        ByteBuf memberBuf = hkeybuf.slice(pattern0.length, iterator.key().length - pattern0.length); //获取指定前缀长度的 byte[]
                        ScoreMember scoreMember = splitScoreMember(memberBuf);

                        //如果包含索引，则返回数据
                        if (Arrays.equals(member, scoreMember.member)) {

                            return index;
                        }

                        index++;

                    } else {
                        break;
                    }
                } else break;

            }
        }

        return -1;
    }

//    /**
//     * 获取 member by pattern
//     * 注意pattern 的长度符合截取的要求
//     *
//     * @param db
//     * @param pattern0
//     * @return
//     */
//    protected static Set<ByteBuf> members(RocksDB db, byte[] pattern0) {
//        //按 key 检索所有数据
//        Set<ByteBuf> keys = new HashSet<ByteBuf>();
//
//        try (final RocksIterator iterator = db.newIterator()) {
//            for (iterator.seek(pattern0); iterator.isValid(); iterator.next()) {
//
//                //确保检索有数据，hkeybuf.slice 不错误
//                if (pattern0.length <= iterator.key().length) {
//                    ByteBuf hkeybuf = Unpooled.wrappedBuffer(iterator.key()); //优化 零拷贝
//                    ByteBuf slice = hkeybuf.slice(0, pattern0.length); //获取指定前缀长度的 byte[]
//
//                    slice.resetReaderIndex();
//
//                    //key有序 不相等后面无数据
//                    if (Arrays.equals(slice.readBytes(slice.readableBytes()).array(), pattern0)) {
//
//                        hkeybuf.resetReaderIndex();
//                        ByteBuf member = hkeybuf.slice(pattern0.length, iterator.key().length - pattern0.length); //获取指定前缀长度的 byte[]
//
////                        keys.add(member.readBytes(member.readableBytes()).array());
//                        keys.add(member);
//
//                    } else {
//                        break;
//                    }
//                } else break;
//
//            }
//        }
//
//        //检索过期数据,处理过期数据 ;暂不处理影响效率 fixme
////        log.debug(keys.size());
//
//        return keys;
//    }

//
//    /**
//     * 按前缀检索所有的 keys
//     *
//     * @param pattern0
//     * @return
//     */
//    protected static List<byte[]> keys(RocksDB db, byte[] pattern0) {
//        //按 key 检索所有数据
//        List<byte[]> keys = new ArrayList<>();
//
//        try (final RocksIterator iterator = db.newIterator()) {
//            for (iterator.seek(pattern0); iterator.isValid(); iterator.next()) {
//
//                //确保检索有数据，hkeybuf.slice 不错误
//                if (pattern0.length <= iterator.key().length) {
//                    ByteBuf hkeybuf = Unpooled.wrappedBuffer(iterator.key()); //优化 零拷贝
//                    ByteBuf slice = hkeybuf.slice(0, pattern0.length); //获取指定前缀长度的 byte[]
//
//                    slice.resetReaderIndex();
//
//                    //key有序 不相等后面无数据
//                    if (Arrays.equals(slice.readBytes(slice.readableBytes()).array(), pattern0)) {
//
//                        keys.add(iterator.key());
////                        log.debug(new String(iterator.key()));
////                        if (keys.size() >= 100000) {
////                            //数据大于1万条直接退出
////                            break;
////                        }
//                    } else {
//                        break;
//                    }
//                } else break;
//
//            }
//        }
//
//        //检索过期数据,处理过期数据 ;暂不处理影响效率 fixme
////        log.debug(keys.size());
//
//        return keys;
//    }


    /**
     * Redis Scard 命令返回集合中元素的数量。
     *
     * @return
     * @throws RedisException
     */
    public IntegerReply zcard() throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_ZSET)) return integer(0);

        long cnt = countBy(db, genKeyPartten(DataType.KEY_ZSET_SCORE));//fixme 优化，从meta 获取数量
//        log.debug("元素数量："+getMeta().getCount());
//        log.debug("元素数量："+cnt);
//        Assert.assertEquals(cnt,getMeta().getCount());
        return integer(cnt);
//        return integer(getMeta().getCount());
    }


    /**
     * Redis Zcount 命令用于计算有序集合中指定分数区间的成员数量。
     * <p>
     * 分数值在 min 和 max 之间的成员的数量。
     *
     * @param min1
     * @param max2
     * @return
     * @throws RedisException
     */
    public IntegerReply zcount(byte[] min1, byte[] max2) throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_ZSET)) return integer(0);

        if (min1 == null || max2 == null) {
            throw new RedisException("wrong number of arguments for 'zcount' command");
        }

        MultiBulkReply reply = _zrangebyscore(min1, max2, null, false, false);
        return integer(reply.data().length);

    }

    /**
     * Redis Zincrby 命令对有序集合中指定成员的分数加上增量 increment
     * <p>
     * 可以通过传递一个负数值 increment ，让分数减去相应的值，比如 ZINCRBY key -5 member ，就是让 member 的 score 值减去 5 。
     * <p>
     * 当 key 不存在，或分数不是 key 的成员时， ZINCRBY key increment member 等同于 ZADD key increment member 。
     * <p>
     * 当 key 不是有序集类型时，返回一个错误。
     * <p>
     * 分数值可以是整数值或双精度浮点数。
     * <p>
     * member 成员的新分数值，以字符串形式表示。
     *
     * @param increment1
     * @param member2
     * @return
     * @throws RedisException
     */
    public BulkReply zincrby(byte[] increment1, byte[] member2) throws RedisException {
        log.debug("zincrby--------------------");

        if (checkTypeAndTTL(getKey0(), DataType.KEY_ZSET)) return NIL_REPLY;

//
//        //判断类型，非hash 类型返回异常信息；
//        int type = genMetaKey(getKey0()).getMeta().getType();
//
//        if (type != -1 && type != DataType.KEY_ZSET) {
//            //抛出异常 类型不匹配
//            throw invalidValue();
//        }

        BulkReply zscore = zscore(member2);
        if (zscore.isEmpty()) {
            //新增
            metaKey=getMetaKey(getKey0());
            zadd(increment1, member2);
            return new BulkReply(increment1);
        } else {
            //修改分数 注意修改两个数据
            zscore.data();
            long score = bytesToNum(MyUtils.toByteArray(zscore.data()));
            long incr = bytesToNum(increment1);
            byte[] dest = numToBytes(score + incr);
            metaKey=getMetaKey(getKey0());
            zrem(member2);
            metaKey=getMetaKey(getKey0());
            zadd(dest, member2);
            return new BulkReply(dest);
        }

    }


    /**
     * Redis Zinterstore 命令计算给定的一个或多个有序集的交集，其中给定 key 的数量必须以 numkeys 参数指定，并将该交集(结果集)储存到 destination 。
     * <p>
     * 默认情况下，结果集中某个成员的分数值是所有给定集下该成员分数值之和。
     * <p>
     * 返回值：保存到目标结果集的的成员数量。
     *
     * @param destination0
     * @param numkeys1
     * @param key2
     * @return
     * @throws RedisException
     */
    public IntegerReply zinterstore(byte[] destination0, byte[] numkeys1, byte[][] key2) throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_ZSET)) return integer(0);

        return _zstore(destination0, numkeys1, key2, "zinterstore", false);
    }

    private IntegerReply _zstore(byte[] destination0, byte[] numkeys1, byte[][] key2, String name, boolean union) throws RedisException {

//        if (destination0 == null || numkeys1 == null) {
//            throw new RedisException("wrong number of arguments for '" + name + "' command");
//        }
//        int numkeys = _toint(numkeys1);
//        if (key2.length < numkeys) {
//            throw new RedisException("wrong number of arguments for '" + name + "' command");
//        }
//
//        //todo antlr 进行语法解析
//        int position = numkeys;
//        double[] weights = null;
//        Aggregate type = null;
//        if (key2.length > position) {
//            if ("weights".equals(new String(key2[position]).toLowerCase())) {
//                position++;
//                if (key2.length < position + numkeys) {
//                    throw new RedisException("wrong number of arguments for '" + name + "' command");
//                }
//                weights = new double[numkeys];
//                for (int i = position; i < position + numkeys; i++) {
//                    weights[i - position] = RocksdbRedis._todouble(key2[i]);
//                }
//                position += numkeys;
//            }
//            if (key2.length > position + 1) {
//                if ("aggregate".equals(new String(key2[position]).toLowerCase())) {
//                    type = Aggregate.valueOf(new String(key2[position + 1]).toUpperCase());
//                }
//            } else if (key2.length != position) {
//                throw new RedisException("wrong number of arguments for '" + name + "' command");
//            }
//        }
//
//        zrem(destination0);
//        Set destination = new HashSet();
////        del(new byte[][]{destination0});
////        ZSet destination = _getzset(destination0, true);
////        MultiBulkReply zrange = zrange("-1".getBytes(), "1".getBytes(), "1".getBytes());
//
//        for (int i = 0; i < numkeys; i++) {
////            ZSet zset = _getzset(key2[i], false);
//            MultiBulkReply zrange = genMetaKey(key2[i]).zrange("-1".getBytes(), "1".getBytes(), "1".getBytes());
//
//            if (i == 0) {
//                if (weights == null) {
//                    destination.addAll(zrange.asStringSet(Charset.defaultCharset()));  //todo
//                } else {
//                    double weight = weights[i];
//                    for (int j = 0; j < zrange.data().length; j++) {
//                        Reply reply = zrange.data()[i];
//
//                        ByteBuf buf=Unpooled.wrappedBuffer(reply.data());
//                        int index = buf.bytesBefore(Byte.parseByte("|"));
//                        ByteBuf scoreBuf = buf.slice(0, index);
//                        ByteBuf memberBuf = buf.slice(index + 1, buf.capacity() - 1);
//
//                        long score = bytesToNum(getVal(scoreBuf));
////                        long incr = bytesToNum(increment1);
//
//                        destination.add(memberBuf,numToBytes(score * weight));
//
//                    }
//
//                }
//            } else {
//                for (ZSetEntry entry : zset) {
//                    BytesKey key = entry.getKey();
//                    ZSetEntry current = destination.get(key);
//                    destination.remove(key);
//                    if (union || current != null) {
//                        double newscore = entry.getScore() * (weights == null ? 1 : weights[i]);
//                        if (type == null || type == SimpleRedisServer.Aggregate.SUM) {
//                            if (current != null) {
//                                newscore += current.getScore();
//                            }
//                        } else if (type == SimpleRedisServer.Aggregate.MIN) {
//                            if (current != null && newscore > current.getScore()) {
//                                newscore = current.getScore();
//                            }
//                        } else if (type == SimpleRedisServer.Aggregate.MAX) {
//                            if (current != null && newscore < current.getScore()) {
//                                newscore = current.getScore();
//                            }
//                        }
//                        destination.add(key, newscore);
//                    }
//                }
//                if (!union) {
//                    for (ZSetEntry entry : new ZSet(destination)) {
//                        BytesKey key = entry.getKey();
//                        if (zset.get(key) == null) {
//                            destination.remove(key);
//                        }
//                    }
//                }
//            }
//        }
//        return integer(destination.size());
        return null;
    }

    enum Aggregate {SUM, MIN, MAX}

    /**
     * Redis Zlexcount 命令在计算有序集合中指定字典区间内成员数量。
     * <p>
     * 指定区间内的成员数量。
     *
     * @param min1
     * @param max2
     * @return
     * @throws RedisException
     */
    public IntegerReply zlexcount(byte[] min1, byte[] max2) throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_ZSET)) return integer(0);

        if (min1 == null || max2 == null) {
            throw new RedisException("wrong number of arguments for 'zcount' command");
        }

        Lex min = _tolexrange(min1);
        Lex max = _tolexrange(max2);

        List<ScoreMember> byteBufs = zgetBy(db, genKeyPartten(DataType.KEY_ZSET_SCORE), min, max, false, false);

        return integer(byteBufs.size());

    }


    /**
     * Redis Zscore 命令返回有序集中，成员的分数值。 如果成员元素不是有序集 key 的成员，或 key 不存在，返回 nil 。
     *
     * @param member1
     * @return
     * @throws RedisException
     */
    public BulkReply zscore(byte[] member1) throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_ZSET)) return NIL_REPLY;


        ZSetScoreNode zscore = zsetScoreNode.genKey(getKey0(), member1).zscore();

        if (zscore.isEmpty()) {

            return NIL_REPLY;
        } else {

            byte[] score = zscore.getVal0();
            return new BulkReply(score);
        }
    }


    /**
     * Redis Zunionstore 命令计算给定的一个或多个有序集的并集，其中给定 key 的数量必须以 numkeys 参数指定，并将该并集(结果集)储存到 destination 。
     * <p>
     * 默认情况下，结果集中某个成员的分数值是所有给定集下该成员分数值之和 。
     * <p>
     * 保存到 destination 的结果集的成员数量。
     *
     * @param destination0
     * @param numkeys1
     * @param key2
     * @return
     * @throws RedisException
     */
    public IntegerReply zunionstore(byte[] destination0, byte[] numkeys1, byte[][] key2) throws RedisException {
//        if (checkTypeAndTTL(getKey0(), DataType.KEY_ZSET)) return integer(0);

//        return _zstore(destination0, numkeys1, key2, "zunionstore", true);
        return null;
    }


    /**
     * Redis Zrank 返回有序集中指定成员的排名。其中有序集成员按分数值递增(从小到大)顺序排列。
     * 如果成员是有序集 key 的成员，返回 member 的排名。 如果成员不是有序集 key 的成员，返回 nil 。
     *
     * @param member1
     * @return
     * @throws RedisException
     */
    public Reply zrank(byte[] member1) throws RedisException {
        log.debug("zrank--------------------");
        if (checkTypeAndTTL(getKey0(), DataType.KEY_ZSET)) return NIL_REPLY;

        int sort = zsortBy(db, genKeyPartten(DataType.KEY_ZSET_SORT), member1);
        if (sort == -1) {
            return NIL_REPLY;
        } else
            return integer(sort);
    }


    /**
     * Redis Zrevrank 命令返回有序集中成员的排名。其中有序集成员按分数值递减(从大到小)排序。
     * <p>
     * 排名以 0 为底，也就是说， 分数值最大的成员排名为 0 。
     * <p>
     * 使用 ZRANK 命令可以获得成员按分数值递增(从小到大)排列的排名。
     * <p>
     * 如果成员是有序集 key 的成员，返回成员的排名。 如果成员不是有序集 key 的成员，返回 nil 。
     *
     * @param member1
     * @return
     * @throws RedisException
     */

    public Reply zrevrank(byte[] member1) throws RedisException {
        log.debug("zrevrank--------------------");

        if (checkTypeAndTTL(getKey0(), DataType.KEY_ZSET)) return NIL_REPLY;

        int sort = zsortBy(db, genKeyPartten(DataType.KEY_ZSET_SORT), member1);
        if (sort == -1) {
            return NIL_REPLY;
        } else{
            return integer(getCount() - 1 - sort);
        }
    }

    /**
     * Redis Zrem 命令用于移除有序集中的一个或多个成员，不存在的成员将被忽略。
     * 当 key 存在但不是有序集类型时，返回一个错误。
     * <p>
     * 被成功移除的成员的数量，不包括被忽略的成员。
     *
     * @param member1
     * @return
     * @throws RedisException
     */
    public IntegerReply zrem(byte[]... member1) throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_ZSET)) return integer(0);

        int rmCnt = 0;
        for (byte[] member : member1) {

            ZSetScoreNode zscore = zsetScoreNode.genKey(getKey0(), member).zscore();
//            log.debug(zscore.getKey0());

            byte[] val = zscore.getVal0();

            if (!zscore.isEmpty()) {
                zsetScoreNode.genKey(getKey0(), member).zrem();
                zsetRankNode.genKey(getKey0(), val, member).zrem();
                rmCnt++;
            }

        }

        singleThreadExecutor.execute(new MetaCountCaller(db, getKey0(), genKeyPartten(DataType.KEY_ZSET_SCORE)));

        return integer(rmCnt);

    }


    /**
     * Redis Zrange 返回有序集中，指定区间内的成员。
     * 其中成员的位置按分数值递增(从小到大)来排序。
     * 具有相同分数值的成员按字典序(lexicographical order )来排列。
     * 如果你需要成员按
     * 值递减(从大到小)来排列，请使用 ZREVRANGE 命令。
     * 下标参数 start 和 stop 都以 0 为底，也就是说，以 0 表示有序集第一个成员，以 1 表示有序集第二个成员，以此类推。
     * 你也可以使用负数下标，以 -1 表示最后一个成员， -2 表示倒数第二个成员，以此类推。
     * <p>
     * 指定区间内，带有分数值(可选)的有序集成员的列表。
     *
     * @param start1
     * @param stop2
     * @param withscores3
     * @return
     * @throws RedisException
     */
    public MultiBulkReply zrange(byte[] start1, byte[] stop2, byte[] withscores3) throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_ZSET)) return EMPTY;


        if (start1 == null || stop2 == null) {
            throw new RedisException("invalid number of argumenst for 'zrange' command");
        }

//        long size = getMeta().getCount();
        long size = getCount();

        log.debug(String.format("count:%s", size));
        log.debug(String.format("count:%s", getCount()));

        long start = RocksdbRedis._torange(start1, size);
        long end = RocksdbRedis._torange(stop2, size);
        int withscores = _toint(withscores3);

        List<ByteBuf> members = zgetBy(db, genKeyPartten(DataType.KEY_ZSET_SORT), start, end, false);

        return _setReply(members, withscores > 0);
    }


    /**
     * Redis Zrevrange 命令返回有序集中，指定区间内的成员。
     * <p>
     * 其中成员的位置按分数值递减(从大到小)来排列。
     * <p>
     * 具有相同分数值的成员按字典序的逆序(reverse lexicographical order)排列。
     * <p>
     * 除了成员按分数值递减的次序排列这一点外， ZREVRANGE 命令的其他方面和 ZRANGE 命令一样。
     *
     * @param start1
     * @param stop2
     * @param withscores3
     * @return
     * @throws RedisException
     */

    public MultiBulkReply zrevrange(byte[] start1, byte[] stop2, byte[] withscores3) throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_ZSET)) return EMPTY;

        if (start1 == null || stop2 == null) {
            throw new RedisException("invalid number of argumenst for 'zrange' command");
        }

//        long size = getMeta().getCount();
        long size = getCount();
        long start = RocksdbRedis._torange(start1, size);
        long end = RocksdbRedis._torange(stop2, size);
        int withscores = _toint(withscores3);

        List<ByteBuf> members = zgetBy(db, genKeyPartten(DataType.KEY_ZSET_SORT), start, end, false);

        Collections.reverse(members);//倒序 排序

        return _setReply(members, withscores > 0);

    }


    private MultiBulkReply _setReply(List<ScoreMember> members, boolean withscores, int offset, int number) throws RedisException {

        List<Reply<ByteBuf>> list = new ArrayList<Reply<ByteBuf>>();
//        int current=0;
        for (ScoreMember buf : members
        ) {

//            if (current >= offset && current < offset + number) {
            list.add(new BulkReply(buf.member));
            if (withscores) {
                list.add(new BulkReply(buf.score));
            }
//            }
//            current++;
        }

        return new MultiBulkReply(list.toArray(new Reply[list.size()]));
    }

    /**
     * 构建返回数据 score + members
     * 删除节点数据 score,sort 两种数据
     *
     * @param members
     * @param withscores
     * @return
     * @throws RedisException
     */
    private MultiBulkReply _setReply(List<ByteBuf> members, boolean withscores) throws RedisException {

        List<Reply<ByteBuf>> list = new ArrayList<Reply<ByteBuf>>();
        int current = 0;
        for (ByteBuf buf : members
        ) {

            ScoreMember scoreMember = splitScoreMember(buf);
//            buf.resetReaderIndex();
//            int index = buf.forEachByte(ByteBufProcessor.FIND_LF);
//                int index = buf.bytesBefore(Byte.parseByte("|"));
//                ByteBuf scoreBuf = buf.slice(0, index);
//                ByteBuf memberBuf = buf.slice(index + 1, buf.capacity() - 1);

            list.add(new BulkReply(scoreMember.member));
            if (withscores) {
                list.add(new BulkReply(scoreMember.score));
            }

        }


        return new MultiBulkReply(list.toArray(new Reply[list.size()]));
    }

    private void _deleteScoreAndSort(List<ByteBuf> members) throws RedisException {

        //fixme member 只能逐个删除

        ByteBuf f = members.get(0);
        ByteBuf l = members.get(members.size() - 1);

        ScoreMember first = splitScoreMember(f);
        ScoreMember last = splitScoreMember(l);

//        ByteBuf firstMember=getMember(first);
//            ByteBuf lastMember=getMember(last);

//            clearMetaDataNodeData(getKey0(), DataType.KEY_ZSET_SCORE,first.member, last.member);
        clearMetaDataNodeData(getKey0(), DataType.KEY_ZSET_SORT, first.score, last.score);//score+member
    }


    /**
     *
     * Redis Zrangebylex 通过字典区间返回有序集合的成员。
     * 指定区间内的元素列表。
     * ZRANGEBYLEX myzset - [c 包含c
     * ZRANGEBYLEX myzset - (c 不包含c
     * ZRANGEBYLEX myzset [aaa (g   相当于(a (g
     *
     * @param key0
     * @param min1
     * @param max2
     * @param withscores_offset_or_count4
     * @return
     * @throws RedisException
     */


    /**
     * Redis Zrangebyscore 返回有序集合中指定分数区间的成员列表。有序集成员按分数值递增(从小到大)次序排列。
     * <p>
     * 具有相同分数值的成员按字典序来排列(该属性是有序集提供的，不需要额外的计算)。
     * <p>
     * 默认情况下，区间的取值使用闭区间 (小于等于或大于等于)，你也可以通过给参数前增加 ( 符号来使用可选的开区间 (小于或大于)。
     *
     * @param min1
     * @param max2
     * @param withscores_offset_or_count4
     * @return
     * @throws RedisException
     */
    public MultiBulkReply zrangebyscore(byte[] min1, byte[] max2, byte[]... withscores_offset_or_count4) throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_ZSET)) return EMPTY;

        return _zrangebyscore(min1, max2, withscores_offset_or_count4, false, false);
    }


    public MultiBulkReply zrangebylex(byte[] min1, byte[] max2, byte[]... offset_or_count4) throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_ZSET)) return EMPTY;

        return _zrangebylex(min1, max2, offset_or_count4, false, false);
    }

    /**
     * Redis Zrevrangebyscore 返回有序集中指定分数区间内的所有的成员。有序集成员按分数值递减(从大到小)的次序排列。
     * <p>
     * 具有相同分数值的成员按字典序的逆序(reverse lexicographical order )排列。
     * <p>
     * 除了成员按分数值递减的次序排列这一点外， ZREVRANGEBYSCORE 命令的其他方面和 ZRANGEBYSCORE 命令一样。
     * <p>
     * 指定区间内，带有分数值(可选)的有序集成员的列表。
     *
     * @param max1
     * @param min2
     * @param withscores_offset_or_count4
     * @return
     * @throws RedisException
     */
    public MultiBulkReply zrevrangebyscore(byte[] max1, byte[] min2, byte[]... withscores_offset_or_count4) throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_ZSET)) return EMPTY;

        return _zrangebyscore(max1, min2, withscores_offset_or_count4, true, false);
    }


    /**
     * Redis Zremrangebyscore 命令用于移除有序集中，指定分数（score）区间内的所有成员。
     * <p>
     * 被移除成员的数量。
     *
     * @param min1
     * @param max2
     * @return
     * @throws RedisException
     */
    public IntegerReply zremrangebyscore(byte[] min1, byte[] max2) throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_ZSET)) return integer(0);

        MultiBulkReply reply = _zrangebyscore(min1, max2, null, false, true);
        return integer(reply.data().length);
    }

    /**
     * Redis Zremrangebylex 命令用于移除有序集合中给定的字典区间的所有成员。
     * <p>
     * 被成功移除的成员的数量，不包括被忽略的成员。
     *
     * @param min1
     * @param max2
     * @return
     * @throws RedisException
     */
    public IntegerReply zremrangebyslex(byte[] min1, byte[] max2) throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_ZSET)) return integer(0);

        Lex min = _tolexrange(min1);
        Lex max = _tolexrange(max2);

        List<ScoreMember> members = zgetBy(db, genKeyPartten(DataType.KEY_ZSET_SCORE), min, max, false, true);

        return integer(members.size());
    }

    /**
     * Redis Zremrangebyrank 命令用于移除有序集中，指定排名(rank)区间内的所有成员。
     * <p>
     * 被移除成员的数量。
     *
     * @param start1
     * @param stop2
     * @return
     * @throws RedisException
     */
    public IntegerReply zremrangebyrank(byte[] start1, byte[] stop2) throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_ZSET)) return integer(0);

//        long size = getMeta().getCount();
        long size = getCount();
        long start = RocksdbRedis._torange(start1, size);
        long end = RocksdbRedis._torange(stop2, size);

        List<ByteBuf> members = zgetBy(db, genKeyPartten(DataType.KEY_ZSET_SORT), start, end, true);

        //删除数据
//        _deleteScoreAndSort(members);

        return integer(members.size());

    }


    /**
     * 按分数区间查询
     *
     * @param min1
     * @param max2
     * @param withscores_offset_or_count4
     * @param reverse
     * @return
     * @throws RedisException
     */
    private MultiBulkReply _zrangebyscore(byte[] min1, byte[] max2, byte[][] withscores_offset_or_count4, boolean reverse, boolean delete) throws RedisException {

        boolean withscores = false;
        int offset = 0;
        int number = Integer.MAX_VALUE;

        if (!delete & withscores_offset_or_count4 != null) { //withscores_offset_or_count4=null 删除数据不进行语法检查
            //fixme antlr 进行语法解析
            int position = 0;

            if (withscores_offset_or_count4.length > 0) {
                withscores = _checkcommand(withscores_offset_or_count4[0], "withscores", false);
            }
            if (withscores) position++;
            boolean limit = false;
            if (withscores_offset_or_count4.length > position) {
                limit = _checkcommand(withscores_offset_or_count4[position++], "limit", true);
            }
            if (withscores_offset_or_count4.length != position + (limit ? 2 : 0)) {
                throw new RedisException("syntax error");
            }

            if (limit) {
                offset = _toint(withscores_offset_or_count4[position++]);
                number = _toint(withscores_offset_or_count4[position]);
                if (offset < 0 || number < 1) {
                    throw notInteger();
                }
            }
        }

        Score min = _toscorerange(min1);
        Score max = _toscorerange(max2);

        List<ScoreMember> members = zgetBy(db, genKeyPartten(DataType.KEY_ZSET_SORT), min, max, offset, number, reverse, delete);


        return _setReply(members, withscores, offset, number);
    }

    /**
     * Redis Zrangebylex 通过字典区间返回有序集合的成员。
     * 指定区间内的元素列表。
     *
     * @param min1
     * @param max2
     * @param offset_or_count4
     * @param reverse
     * @param delete
     * @return
     * @throws RedisException
     */
    private MultiBulkReply _zrangebylex(byte[] min1, byte[] max2, byte[][] offset_or_count4, boolean reverse, boolean delete) throws RedisException {

//        boolean withscores = false;
        int offset = 0;
        int number = Integer.MAX_VALUE;

        if (!delete) { //offset_or_count4=null 删除数据不进行语法检查
            //fixme antlr 进行语法解析
            int position = 0;

//            if (offset_or_count4.length > 0) {
//                withscores = _checkcommand(offset_or_count4[0], "withscores", false);
//            }
//            if (withscores) position++;

            boolean limit = false;
            if (offset_or_count4.length > position) {
                limit = _checkcommand(offset_or_count4[position], "limit", true);
            }
            if (offset_or_count4.length != position + (limit ? 2 : 0)) {
                throw new RedisException("syntax error");
            }

            if (limit) {
                offset = _toint(offset_or_count4[position++]);
                number = _toint(offset_or_count4[position]);
                if (offset < 0 || number < 1) {
                    throw notInteger();
                }
            }
        }

        Lex min = _tolexrange(min1);
        Lex max = _tolexrange(max2);

        List<ScoreMember> members = zgetBy(db, genKeyPartten(DataType.KEY_ZSET_SCORE), min, max, false, false);

        return _setReply(members, false, offset, number);
    }

    private boolean _checkcommand(byte[] check, String command, boolean syntax) throws RedisException {
        boolean result;
        if (check != null) {
            if (new String(check).toLowerCase().equals(command)) {
                result = true;
            } else {
                if (syntax) {
                    throw new RedisException("syntax error");
                } else {
                    return false;
                }
            }
        } else {
            result = false;
        }
        return result;
    }

    /**
     * 分数区间指令拆解 min max  todo by ByteBuf
     *
     * @param specifier
     * @return
     */
    private Score _toscorerange(byte[] specifier) {
        Score score = new Score();
        String s = new String(specifier).toLowerCase();
        if (s.startsWith("(")) {
            score.inclusive = false;
            s = s.substring(1);
        }
        if (s.equals("-inf")) {
            score.value = Double.NEGATIVE_INFINITY;
            score.source = "-1.0".getBytes();
        } else if (s.equals("inf") || s.equals("+inf")) {
            score.value = Double.POSITIVE_INFINITY;
            score.source = "1.0".getBytes();
        } else {
            score.value = Double.parseDouble(s);
            score.source = s.getBytes();

        }
        return score;
    }

    /**
     * 分数区间
     */
    static class Score {
        boolean inclusive = true;
        byte[] source;
        double value;
    }


    private Lex _tolexrange(byte[] specifier) {
        Lex lex = new Lex();
        String s = new String(specifier).toLowerCase();
        if (s.startsWith("(")) {
            lex.inclusive = false;
            s = s.substring(1);
        }
        if (s.equals("-")) {
            lex.lex = "-1.0".getBytes();
            lex.buf = Unpooled.wrappedBuffer("-1.0".getBytes()); //优化 零拷贝
        } else if (s.equals("+")) {
            lex.lex = "1.0".getBytes();
            lex.buf = Unpooled.wrappedBuffer("1.0".getBytes()); //优化 零拷贝
        } else {
            lex.lex = s.getBytes();
            lex.buf = Unpooled.wrappedBuffer(s.getBytes()); //优化 零拷贝

        }
        return lex;
    }

    /**
     * 字典
     */
    static class Lex {
        boolean inclusive = true;
        byte[] lex;
        ByteBuf buf;
    }


    ////////////////


    private void debugBytebuf(Set<ByteBuf> set) {
        StringBuilder sb = new StringBuilder();
        for (ByteBuf buf : set
        ) {
            sb.append(new String(buf.readBytes(buf.readableBytes()).array()));
        }
        log.debug(sb.toString());
    }

    /**
     * 重置集合中ByteBuf 的指针
     *
     * @param set
     */
    private void resetBytebuf(Set<ByteBuf> set) {
        for (ByteBuf buf : set
        ) {
            buf.resetReaderIndex();
        }
    }


    /**
     * Redis Zadd 命令用于将一个或多个成员元素及其分数值加入到有序集当中。
     * <p>
     * 如果某个成员已经是有序集的成员，那么更新这个成员的分数值，并通过重新插入这个成员元素，来保证该成员在正确的位置上。
     * <p>
     * 分数值可以是整数值或双精度浮点数。
     * <p>
     * 如果有序集合 key 不存在，则创建一个空的有序集并执行 ZADD 操作。
     * <p>
     * 当 key 存在但不是有序集类型时，返回一个错误。
     *
     * @param args
     * @return
     * @throws RedisException
     */
    public IntegerReply zadd(byte[]... args) throws RedisException {


//        if (args.length < 3 || (args.length - 1) % 2 == 1) {
//            throw new RedisException("wrong number of arguments for 'zadd' command");
//        }

        //获取子数组，对子数组进行操作
//        args = Arrays.copyOfRange(args, 1, args.length-1);
        log.debug(Arrays.toString(args));


//        byte[] key = args[0];
        byte[] key = getKey0();

//        //判断类型，非hash 类型返回异常信息；
//        int type = genMetaKey(key).getMeta().getType();
//
//        if (type != -1 && type != DataType.KEY_ZSET) {
//            //抛出异常 类型不匹配
//            throw invalidValue();
//        }
        if (checkTypeAndTTL(getKey0(), DataType.KEY_ZSET)) return integer(0);


        //生成RocksDb 能够识别的 member key ;key=0;i=1 , score=i, member=i+1
        final WriteBatch batch = new WriteBatch();

        int total = 0;
        //数据从0开始，如果包含key,则从1开始。
        for (int i = 0; i < args.length; i += 2) {
            byte[] member = args[i + 1];
            byte[] score = args[i];

            byte[] scoreKey = zsetScoreNode.genKey(key, member).getKey();
            byte[] memberKey = zsetRankNode.genKey(key, score, member).getKey();
            byte[] scoreVal = zsetScoreNode.setVal(score, -1).getVal();
            byte[] memberVal = zsetRankNode.setVal(-1).getVal();

            try {
                batch.put(scoreKey, scoreVal);
                batch.put(memberKey, memberVal);

            } catch (Throwable e) {
                e.printStackTrace();
                Throwables.propagateIfPossible(e, RedisException.class);
            }

            total++;

        }

        //持久化到RocksDb
        final WriteOptions writeOpt = new WriteOptions();
        try {
            db.write(writeOpt, batch);
        } catch (RocksDBException e) {
            throw new RuntimeException(e.getMessage());
        }

        //todo 修改meta 计数 逻辑上一条，实际上两条：一条score 一条rank(sort) 实际上在rocksdb 中保留了两条记录
        metaKey=getMetaKey(key);
        singleThreadExecutor.execute(new MetaCountCaller(db, getKey0(), genKeyPartten(DataType.KEY_ZSET_SCORE)));

        return integer(total);
    }


    public static void main(String[] args) throws Exception {
//        testSet();

    }

//    /**
//     * Hash数据集测试
//     *
//     * @throws RedisException
//     */
//    private static void testSet() throws RedisException, InterruptedException {
////
//        log.debug("|".getBytes().length);
//        Random random = new Random();
//        random.ints(6).limit(3).sorted().forEach(System.out::println);
//
//        List<String> strings = Arrays.asList("abc", "", "bc", "efg", "abcd", "", "jkl");
//        long count = strings.parallelStream().filter(string -> string.compareTo("e") > 0).count();
//        strings.stream().filter(string -> string.compareTo("e") > 0).count();
//        log.debug(count);
//        List<Integer> numbers = Arrays.asList(3, 2, 2, 3, 7, 3, 5);
//// 获取对应的平方数
//        List<Integer> squaresList = numbers.stream().map(i -> i * i).distinct().collect(Collectors.toList());
//        Set<Integer> squaresList1 = numbers.stream().map(i -> i * i).collect(Collectors.toSet());
//        log.debug(squaresList);
//        log.debug(squaresList1);
//
//        ZSetMeta setMeta = ZSetMeta.getInstance(RocksdbRedis.mydata, "redis".getBytes());
//        setMeta.genMetaKey("ZSet".getBytes()).clearMetaDataNodeData(setMeta.getKey0());
//        Assert.assertEquals(setMeta.zcard().data().intValue(), 0);
//
//        setMeta.genMetaKey("ZSet".getBytes()).
//                zadd("10".getBytes(), "f1".getBytes(),
//                        "20".getBytes(), "f2".getBytes(),
//                        "30".getBytes(), "f3".getBytes());
//
//        Assert.assertEquals(setMeta.zcard().data().intValue(), 3);
//
//        Assert.assertEquals(setMeta.zcount("1".getBytes(), "40".getBytes()).data().intValue(), 3);
//        Assert.assertEquals(setMeta.zcount("15".getBytes(), "40".getBytes()).data().intValue(), 2);
//        Assert.assertEquals(setMeta.zlexcount("f2".getBytes(), "f3".getBytes()).data().intValue(), 2);
//
//        Assert.assertArrayEquals(setMeta.zscore("f1".getBytes()).dataByte(), "10".getBytes());
//
//        String[] zrangestr = {"f2", "20", "f3", "30"};
//        String[] zrevrangestr = {"f3", "30", "f2", "20"};
//        String[] zrangebylexstr = {"f2", "f3"};
//        //安索引
//        MultiBulkReply zrange = setMeta.zrange("1".getBytes(), "2".getBytes(), "1".getBytes());
//        //索引倒序
//        MultiBulkReply zrevrange = setMeta.zrevrange("1".getBytes(), "2".getBytes(), "1".getBytes());
//        //按分数
//        MultiBulkReply zrangebyscore = setMeta.zrangebyscore("19".getBytes(), "31".getBytes(), "WITHSCORES".getBytes());
//        MultiBulkReply zrevrangebyscore = setMeta.zrevrangebyscore("19".getBytes(), "31".getBytes(), "WITHSCORES".getBytes());
//        //按字母
//        MultiBulkReply zrangebylex = setMeta.zrangebylex("f2".getBytes(), "f3".getBytes());
//
//        Assert.assertEquals(Arrays.asList(zrangestr).toString(), zrange.asStringList(Charset.defaultCharset()).toString());
//        Assert.assertEquals(Arrays.asList(zrangestr).toString(), zrangebyscore.asStringList(Charset.defaultCharset()).toString());
////        Assert.assertEquals(Arrays.asList(zrevrangestr).toString(),zrevrangebyscore.asStringList(Charset.defaultCharset()).toString());
//        Assert.assertEquals(Arrays.asList(zrangebylexstr).toString(), zrangebylex.asStringList(Charset.defaultCharset()).toString());
//        Assert.assertEquals(Arrays.asList(zrevrangestr).toString(), zrevrange.asStringList(Charset.defaultCharset()).toString());
//
//        log.debug(zrange.asStringList(Charset.defaultCharset()));
//        log.debug(zrangebyscore.asStringList(Charset.defaultCharset()));
//        log.debug(zrevrangebyscore.asStringList(Charset.defaultCharset()));
//        log.debug(zrangebylex.asStringList(Charset.defaultCharset()));
//        log.debug(zrevrange.asStringList(Charset.defaultCharset()));
//
//
//        IntegerReply zrank = (IntegerReply) setMeta.zrank("f1".getBytes());
//        IntegerReply zrevrank = (IntegerReply) setMeta.zrevrank("f1".getBytes());
//        Assert.assertEquals(zrank.data().intValue(), 0);
//        Assert.assertEquals(zrevrank.data().intValue(), 2);
//
//        Assert.assertArrayEquals(setMeta.zincrby("-1".getBytes(), "f1".getBytes()).dataByte(), "9".getBytes());
//
//
//        setMeta.zrem("f1".getBytes());
//        Assert.assertNull(setMeta.zscore("f1".getBytes()).data());
//        setMeta.zadd("10".getBytes(), "f1".getBytes());
//        Assert.assertNotNull(setMeta.zscore("f1".getBytes()).data());
//
//        log.debug(setMeta.zremrangebyscore("12".getBytes(), "22".getBytes()));
//        Assert.assertNull(setMeta.zscore("f2".getBytes()).data());
//        setMeta.zadd("20".getBytes(), "f2".getBytes());
//        Assert.assertNotNull(setMeta.zscore("f2".getBytes()).data());
//
//
//        log.debug(setMeta.zremrangebyslex("f1".getBytes(), "f2".getBytes()));
//        Assert.assertNull(setMeta.zscore("f1".getBytes()).data());
////        Assert.assertNull(setMeta.zscore("f2".getBytes()).data());
//        setMeta.zadd("10".getBytes(), "f1".getBytes(), "20".getBytes(), "f2".getBytes());
//
//
//        log.debug(setMeta.zremrangebyrank("1".getBytes(), "2".getBytes()));
//        Assert.assertNull(setMeta.zscore("f3".getBytes()).data());
//
//
//        log.debug("Over ... ...");
//
//    }

}
