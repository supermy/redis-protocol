package redis.server.netty.rocksdb;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import redis.netty4.*;
import redis.server.netty.RedisException;
import redis.server.netty.utis.DataType;

import java.nio.charset.Charset;
import java.util.*;

import static redis.netty4.BulkReply.NIL_REPLY;
import static redis.netty4.IntegerReply.integer;
import static redis.netty4.MultiBulkReply.EMPTY;
import static redis.netty4.StatusReply.OK;
import static redis.netty4.StatusReply.QUIT;
import static redis.server.netty.rocksdb.RedisBase.invalidValue;
import static redis.util.Encoding.bytesToNum;


/**
 * Set Meta 元素方便促常用操作
 * <p>
 * Set      [<ns>] <key> KEY_META                 KEY_SET <MetaObject>
 *          [<ns>] <key> KEY_SET_MEMBER <member>  KEY_SET_MEMBER
 * </p>
 * <p>
 * key and value 都采用 | 分隔符号
 * * getKey 一般是包含组合键；
 * * getkey0 是纯粹的业务主键；
 * * 参见setVal0 long+int+int ttl,数据类型,数据长度；
 * * val 一般是包含ttl 的数据；val0是实际的业务数据
 * <p>
 * Created by moyong on 2018/10/23.
 * Update by moyong on 2018/10/23
 * </p>
 *
 */
public class SetMeta extends BaseMeta {

    private static Logger log = Logger.getLogger(SetMeta.class);


    protected static SetNode setNode;

    private SetMeta() {
    }

    private static SetMeta instance = new SetMeta();

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
    public static SetMeta getInstance(RocksDB db0, byte[] ns0) {
        instance.db = db0;
        instance.NS = ns0;
        instance.VAlTYPE = DataType.KEY_SET;
        setNode = SetNode.getInstance(db0, ns0);
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
    public SetMeta genMetaKey(byte[] key0) throws RedisException {
        if (key0 == null) {
            throw new RedisException(String.format("key0 主键不能为空"));
        }
        instance.metaKey = Unpooled.wrappedBuffer(instance.NS, DataType.SPLIT, key0, DataType.SPLIT, KEYTYPE);
        return instance;
    }

//    private ByteBuf genMetaVal(long count) {
//        ByteBuf val = Unpooled.buffer(8);
//
//        val.writeLong(-1); //ttl 无限期 -1
//        val.writeBytes(DataType.SPLIT);
//
//        val.writeInt(DataType.KEY_SET); //long 8 bit
//        val.writeBytes(DataType.SPLIT);
//
//        val.writeLong(count);  //数量
//        val.writeBytes(DataType.SPLIT);
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
//    protected SetMeta setMeta(long count) throws RedisException {
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

//    /**
//     * 获取meta 数据
//     *
//     * @return
//     * @throws RedisException
//     */
//    protected SetMeta getMeta() throws RedisException {
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



//
//    public long getVal0() throws RedisException {
//        return metaVal.getLong(8 + 4 + 2);
//    }
//
//    public void setVal0(long val0) throws RedisException {
//        this.metaVal.setLong(8 + 4 + 2, val0);  //数量
//    }

//    public byte[] getVal() throws RedisException {
//        this.metaVal.resetReaderIndex();
//        return this.metaVal.readBytes(metaVal.readableBytes()).array();
//    }
//
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

//    /**
//     * 元素数量
//     *
//     * @return
//     */
//    public long getCount() throws RedisException {
//        return getVal0();
//    }
//
//
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

//            log.debug("MetaCountCaller ... cnt:" + cnt);
//
//
//            try {
//                db.put(key0, getVal(genMetaVal(cnt)));
//
//            } catch (RocksDBException e) {
//                e.printStackTrace();
//            } catch (RedisException e) {
//                e.printStackTrace();
//            }


        }
    }



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
     * 按索引提取数据
     * @param db
     * @param pattern0
     * @return
     */
    protected static List<byte[]> getBy(RocksDB db, byte[] pattern0,Set<Integer> indexs) {
        //按 key 检索所有数据
        List<byte[]> keys = new ArrayList<>();
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

                        //如果包含索引，则返回数据
                        if(indexs.contains(index)){
                            keys.add(iterator.key());
                        }

                        index++;

                    } else {
                        break;
                    }
                } else break;

            }
        }

        //检索过期数据,处理过期数据 ;暂不处理影响效率 fixme
//        log.debug(keys.size());

        return keys;
    }


    /**
     * 获取 member by pattern
     * 注意pattern 的长度符合截取的要求
     * @param db
     * @param pattern0
     * @return
     */
    protected static Set<ByteBuf> members(RocksDB db, byte[] pattern0) {
        //按 key 检索所有数据
        Set<ByteBuf> keys = new HashSet<ByteBuf>();

        try (final RocksIterator iterator = db.newIterator()) {
            for (iterator.seek(pattern0); iterator.isValid(); iterator.next()) {

                //确保检索有数据，hkeybuf.slice 不错误
                if (pattern0.length <= iterator.key().length) {
                    ByteBuf hkeybuf = Unpooled.wrappedBuffer(iterator.key()); //优化 零拷贝
                    ByteBuf slice = hkeybuf.slice(0, pattern0.length); //获取指定前缀长度的 byte[]

                    slice.resetReaderIndex();

                    //key有序 不相等后面无数据
                    if (Arrays.equals(slice.readBytes(slice.readableBytes()).array(), pattern0)) {

                        hkeybuf.resetReaderIndex();
                        ByteBuf member = hkeybuf.slice(pattern0.length, iterator.key().length-pattern0.length); //获取指定前缀长度的 byte[]

//                        keys.add(member.readBytes(member.readableBytes()).array());
                        keys.add(member);

                    } else {
                        break;
                    }
                } else break;

            }
        }

        //检索过期数据,处理过期数据 ;暂不处理影响效率 fixme
//        log.debug(keys.size());

        return keys;
    }


    /**
     * 按前缀检索所有的 keys
     *
     * @param pattern0
     * @return
     */
    protected static List<byte[]> keys(RocksDB db, byte[] pattern0) {
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

                        keys.add(iterator.key());
//                        log.debug(new String(iterator.key()));
//                        if (keys.size() >= 100000) {
//                            //数据大于1万条直接退出
//                            break;
//                        }
                    } else {
                        break;
                    }
                } else break;

            }
        }

        //检索过期数据,处理过期数据 ;暂不处理影响效率 fixme
//        log.debug(keys.size());

        return keys;
    }




    /**
     * Redis Scard 命令返回集合中元素的数量。
     * @param key0
     * @return
     * @throws RedisException
     */
    public IntegerReply scard() throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_SET)) return integer(0);

//        long cnt = countBy(db, genKeyPartten(DataType.KEY_SET_MEMBER));//fixme 优化，从meta 获取数量
//        log.debug("元素数量："+getMeta().getCount());
//        log.debug("元素数量："+cnt);
//        Assert.assertEquals(cnt,getMeta().getCount());
//        return integer(cnt);
        return integer(getCount());
//        return integer(getMeta().getCount());
    }

    /**
     *
     * Redis Sdiff 命令返回给定集合之间的差集。不存在的集合 key 将视为空集。
     *
     * 差集的结果来自前面的 FIRST_KEY ,而不是后面的 OTHER_KEY1，也不是整个 FIRST_KEY OTHER_KEY1..OTHER_KEYN 的差集。
     *
     * 实例:
     *
     * key1 = {a,b,c,d}
     * key2 = {c}
     * key3 = {a,c,e}
     * SDIFF key1 key2 key3 = {b,d}
     *
     * @param key0
     * @return
     * @throws RedisException
     */
    public MultiBulkReply sdiff(byte[]... key0) throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_SET)) return EMPTY;

        Set set = _sdiff(key0);
        debugBytebuf(set);
        return _setreply(set);
    }

    /**
     * 差集
     * @param key0
     * @return
     * @throws RedisException
     */
    private Set _sdiff(byte[]... key0) throws RedisException {

        Set<ByteBuf> set = new HashSet<ByteBuf>();
        for (byte[] key : key0) {
            if (set.isEmpty()) {
                metaKey=getMetaKey(key);
                set.addAll(members(db, genKeyPartten(DataType.KEY_SET_MEMBER)));
            } else {
                metaKey=getMetaKey(key);
                Set<ByteBuf> members = members(db, genKeyPartten(DataType.KEY_SET_MEMBER));
                set.removeAll(members);
                resetBytebuf(set);
            }
        }

        if (set.isEmpty()) {
            throw new RedisException("wrong number of arguments for 'sdiff' command");
        }
        return set;
    }

    private void debugBytebuf(Set<ByteBuf> set) {
        StringBuilder sb=new StringBuilder();
        for (ByteBuf buf : set
        ) {
            sb.append(new String(buf.readBytes(buf.readableBytes()).array()));
        }
        log.debug(sb.toString());
    }

    /**
     * 重置集合中ByteBuf 的指针
     * @param set
     */
    private void resetBytebuf(Set<ByteBuf> set) {
        for (ByteBuf buf : set
        ) {
            buf.resetReaderIndex();
        }
    }


    private MultiBulkReply _setreply(Set<ByteBuf> set) {
        log.debug(set.size());
        Reply[] replies = new Reply[set.size()];
        int i = 0;
        for (ByteBuf value : set) {
            value.resetReaderIndex();
            replies[i++] = new BulkReply(value.readBytes(value.readableBytes()).array());
        }
        return new MultiBulkReply(replies);
    }


    /**
     *
     * Redis Sdiffstore 命令将给定集合之间的差集存储在指定的集合中。如果指定的集合 key 已存在，则会被覆盖。
     *
     *
     * @param destination0
     * @param key1
     * @return
     * @throws RedisException
     */
    public IntegerReply sdiffstore(byte[] destination0, byte[]... key1) throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_SET)) return integer(0);

//        //判断类型，非set 类型返回异常信息；
//        int type = getMeta().getType();
//
//        log.debug(type);
//
//        if (type != -1 && type != DataType.KEY_SET) {
//            //抛出异常 类型不匹配
//            throw invalidValue();
//        }

        Set<ByteBuf> set = _sdiff(key1);

        IntegerReply store = store(destination0, set);

        metaKey=getMetaKey(destination0);
        singleThreadExecutor.execute(new MetaCountCaller(db, getKey0(), genKeyPartten(DataType.KEY_SET_MEMBER)));

        return store;
    }

    /**
     * 持久化key 集合到
     * @param destination0
     * @param set
     * @return
     * @throws RedisException
     */
    private IntegerReply store(byte[] destination0, Set<ByteBuf> set) throws RedisException {

        if (!set.isEmpty()) {

            byte[][] replies = new byte[set.size()][];
            int i = 0;
            for (ByteBuf value : set) {
                value.resetReaderIndex();
                replies[i++] = value.readBytes(value.readableBytes()).array();
            }

            metaKey=getMetaKey(destination0);
            sadd(replies);

            return integer(set.size());
        } else {
            throw invalidValue();
        }
    }


    /**
     * Redis Sinter 命令返回给定所有给定集合的交集。 不存在的集合 key 被视为空集。 当给定集合当中有一个空集时，结果也为空集(根据集合运算定律)。
     *
     * @param key0
     * @return
     * @throws RedisException
     */
    public MultiBulkReply sinter(byte[]... key0) throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_SET)) return EMPTY;

        Set set = _sinter(key0);
        debugBytebuf(set);
        return _setreply(set);
    }

    /**
     * 交集
     * @param key0
     * @return
     * @throws RedisException
     */
    private Set<ByteBuf> _sinter(byte[]... key0) throws RedisException {

        Set<ByteBuf> set = new HashSet<ByteBuf>();
        for (byte[] key : key0) {
            if (set.isEmpty()) {
                metaKey=getMetaKey(key);
                set.addAll(members(db, genKeyPartten(DataType.KEY_SET_MEMBER)));
            } else {
                metaKey= getMetaKey(key);
                set.retainAll(members(db, genKeyPartten(DataType.KEY_SET_MEMBER)));
                resetBytebuf(set);
            }
        }

        if (set == null) {
            throw new RedisException("wrong number of arguments for 'sinter' command");
        }
        return set;
    }


    /**
     * 并集
     * @param key0
     * @return
     * @throws RedisException
     */
    protected Set<ByteBuf> _sunion(byte[][] key0) throws RedisException {
        Set<ByteBuf> set = new HashSet<ByteBuf>();
        for (byte[] key : key0) {
            if (set.isEmpty()) {
                metaKey=getMetaKey(key);
                set.addAll(members(db, genKeyPartten(DataType.KEY_SET_MEMBER)));
            } else {
                metaKey=getMetaKey(key);
                set.addAll(members(db, genKeyPartten(DataType.KEY_SET_MEMBER)));
                resetBytebuf(set);
            }
        }

        if (set == null) {
            throw new RedisException("wrong number of arguments for 'sunion' command");
        }
        return set;
    }


    /**
     * Redis Sinterstore 命令将给定集合之间的交集存储在指定的集合中。如果指定的集合已经存在，则将其覆盖。
     *
     * @param destination0
     * @param key1
     * @return
     * @throws RedisException
     */
    public IntegerReply sinterstore(byte[] destination0, byte[]... key1) throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_SET)) return integer(0);
//
//        //判断类型，非set 类型返回异常信息；
//        int type = getMeta().getType();
//
//        if (type != -1 && type != DataType.KEY_SET) {
//            //抛出异常 类型不匹配
//            throw invalidValue();
//        }

        Set<ByteBuf> set = _sinter(key1);

        IntegerReply store = store(destination0, set);

        metaKey=getMetaKey(destination0);

        singleThreadExecutor.execute(new MetaCountCaller(db, getKey0(), genKeyPartten(DataType.KEY_SET_MEMBER)));

        return store;

    }


    /**
     * Redis Sismember 命令判断成员元素是否是集合的成员。
     * 如果成员元素是集合的成员，返回 1 。 如果成员元素不是集合的成员，或 key 不存在，返回 0 。
     *
     *
     * @param member1
     * @return
     * @throws RedisException
     */
    public IntegerReply sismember(byte[] member1) throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_SET)) return integer(0);

        boolean exists = setNode.genKey1(getKey0(), member1).exists();
        return exists?integer(1) : integer(0);
    }


    /**
     * Redis Smembers 命令返回集合中的所有的成员。 不存在的集合 key 被视为空集合。
     *
     *
     * @return
     * @throws RedisException
     */
    public MultiBulkReply smembers() throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_SET)) return EMPTY;

        Set<ByteBuf> members = members(db, genKeyPartten(DataType.KEY_SET_MEMBER));

        return _setreply(members);

//        List<Reply<ByteBuf>> replies = new ArrayList<Reply<ByteBuf>>();
//
//        List<byte[]> keys = keys(db, genKeyPartten(DataType.KEY_SET_MEMBER));
//
//        for (byte[] k : keys
//        ) {
//
//            byte[] f = setNode.parseMember(getKey0(), k);
//
//            replies.add(new BulkReply(f));
//
//        }
//
//        return new MultiBulkReply(replies.toArray(new Reply[replies.size()]));
    }


    /**
     *
     * Redis Smove 命令将指定成员 member 元素从 source 集合移动到 destination 集合。
     *
     * SMOVE 是原子性操作。
     *
     * 如果 source 集合不存在或不包含指定的 member 元素，则 SMOVE 命令不执行任何操作，仅返回 0 。否则， member 元素从 source 集合中被移除，并添加到 destination 集合中去。
     *
     * 当 destination 集合已经包含 member 元素时， SMOVE 命令只是简单地将 source 集合中的 member 元素删除。
     *
     * 当 source 或 destination 不是集合类型时，返回一个错误。
     *
     * 如果成员元素被成功移除，返回 1 。 如果成员元素不是 source 集合的成员，并且没有任何操作对 destination 集合执行，那么返回 0
     *
     *
     * @param source0
     * @param destination1
     * @param member2
     * @return
     * @throws RedisException
     */
    public IntegerReply smove(byte[] source0, byte[] destination1, byte[] member2) throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_SET)) return integer(0);

//
//        int type = getMeta().getType();
//
//        if (type != -1 && type != DataType.KEY_SET) {
//            //抛出异常 类型不匹配
//            throw invalidValue();
//        }

        boolean exists = setNode.genKey1(source0, member2).exists();

        if (exists){
            setNode.genKey1(source0,member2).srem();
            setNode.genKey1(destination1,member2).sadd();

            metaKey=getMetaKey(source0);
            singleThreadExecutor.execute(new MetaCountCaller(db, getKey0(), genKeyPartten(DataType.KEY_SET_MEMBER)));

            metaKey=getMetaKey(destination1);
            singleThreadExecutor.execute(new MetaCountCaller(db, getKey0(), genKeyPartten(DataType.KEY_SET_MEMBER)));


            return integer(1);
        }else{
            return integer(0);
        }

    }


    /**
     * Redis Spop 命令用于移除集合中的指定 key 的一个或多个随机元素，移除后会返回移除的元素。
     *
     * 该命令类似 Srandmember 命令，但 SPOP 将随机元素从集合中移除并返回，而 Srandmember 则仅仅返回随机元素，而不对集合进行任何改动。
     *
     * 被移除的随机元素。 当集合不存在或是空集时，返回 nil 。
     *
     *
     * @return
     * @throws RedisException
     */
    public BulkReply spop() throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_SET)) return NIL_REPLY;

        Random random = new Random();
        Integer rand = random.nextInt(scard().data().intValue());
        Set rands = new HashSet();
        rands.add(rand);

        List<byte[]> keyList = getBy(db,  genKeyPartten(DataType.KEY_SET_MEMBER), rands);

        if (keyList.size() == 0) return NIL_REPLY;

        try {
            for (byte[] key:keyList
                 ) {
                db.delete(key);
            }
        } catch (RocksDBException e) {
            e.printStackTrace();
            throw new RedisException(e.getMessage());
        }

        singleThreadExecutor.execute(new MetaCountCaller(db, getKey0(), genKeyPartten(DataType.KEY_SET_MEMBER)));

        return new BulkReply(setNode.parseMember(getKey0(),keyList.get(0)));

    }

    /**
     *
     * Redis Srandmember 命令用于返回集合中的一个随机元素。
     *
     * 从 Redis 2.6 版本开始， Srandmember 命令接受可选的 count 参数：
     *
     * 如果 count 为正数，且小于集合基数，那么命令返回一个包含 count 个元素的数组，数组中的元素各不相同。如果 count 大于等于集合基数，那么返回整个集合。
     * 如果 count 为负数，那么命令返回一个数组，数组中的元素可能会重复出现多次，而数组的长度为 count 的绝对值。
     * 该操作和 SPOP 相似，但 SPOP 将随机元素从集合中移除并返回，而 Srandmember 则仅仅返回随机元素，而不对集合进行任何改动。
     *
     *
     * 只提供集合 key 参数时，返回一个元素；如果集合为空，返回 nil 。 如果提供了 count 参数，那么返回一个数组；如果集合为空，返回空数组。
     *
     *
     * @param count1
     * @return
     * @throws RedisException
     */
    public Reply srandmember(byte[] count1) throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_SET)) return NIL_REPLY;

        long cnt = bytesToNum(count1);

        //绝对值
        cnt=Math.abs(cnt);

        //生成随机数
        Random random = new Random();
        Set rands = new HashSet();
        while (rands.size()<cnt){
//        for (int i = 0; i < cnt; i++) {
            int rand = random.nextInt(scard().data().intValue());
            log.debug("随机数："+rand);
            rands.add(rand);
        }

        //获取数据
        List<byte[]> keyList = getBy(db,  genKeyPartten(DataType.KEY_SET_MEMBER), rands);

        if (keyList.size() == 0) return NIL_REPLY;

        List<Reply<ByteBuf>> replies = new ArrayList<Reply<ByteBuf>>();
        for (byte[] key:keyList
        ) {
            //获取member
            byte[] f=setNode.parseMember(getKey0(), key);
            replies.add(new BulkReply(f));
        }

        return new MultiBulkReply(replies.toArray(new Reply[replies.size()]));
    }


    /**
     * Redis Srem 命令用于移除集合中的一个或多个成员元素，不存在的成员元素会被忽略。
     *
     * 当 key 不是集合类型，返回一个错误。
     *
     * 在 Redis 2.4 版本以前， SREM 只接受单个成员值。
     *
     * 被成功移除的元素的数量，不包括被忽略的元素。
     *
     *
     * @param member1
     * @return
     * @throws RedisException
     */
    public IntegerReply srem(byte[]... member1) throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_SET)) return integer(0);


        for (byte[] hkey : member1) {
            setNode.genKey1(getKey0(), hkey).srem();
        }

        //todo 重新计数；增加一个异步计数队列 ；先使用异步线程，后续使用异步队列替换；

        singleThreadExecutor.execute(new MetaCountCaller(db, getKey0(), genKeyPartten(DataType.KEY_SET_MEMBER)));

        return integer(member1.length);
    }


    /**
     * Redis Sunion 命令返回给定集合的并集。不存在的集合 key 被视为空集。
     * 并集成员的列表。
     * @param key0
     * @return
     * @throws RedisException
     */
    public MultiBulkReply sunion(byte[]... key0) throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_SET)) return EMPTY;

        Set set = _sunion(key0);
        debugBytebuf(set);
        return _setreply(set);
    }


//    Redis Sscan 命令用于迭代集合中键的元素。
//    数组列表。
/**
 * SCAN 命令、 SSCAN 命令、 HSCAN 命令和 ZSCAN 命令都返回一个包含两个元素的 multi-bulk 回复：
 *
 * 回复的第一个元素是字符串表示的无符号 64 位整数（游标），
 * SCAN 命令每次被调用之后， 都会向用户返回一个新的游标， 用户在下次迭代时需要使用这个新游标作为 SCAN 命令的游标参数， 以此来延续之前的迭代过程。
 * 当 SCAN 命令的游标参数被设置为 0 时， 服务器将开始一次新的迭代， 而当服务器向用户返回值为 0 的游标时， 表示迭代已结束。
 *
 * 回复的第二个元素是另一个 multi-bulk 回复， 这个 multi-bulk 回复包含了本次被迭代的元素。
 *
 * 注意：SCAN命令不能保证每次返回的值都是有序的，另外同一个key有可能返回多次，不做区分，需要应用程序去处理。
 *
 * SCAN 命令返回的每个元素都是一个数据库键。
 * SSCAN 命令返回的每个元素都是一个集合成员。
 * HSCAN 命令返回的每个元素都是一个键值对，一个键值对由一个键和一个值组成。
 * ZSCAN 命令返回的每个元素都是一个有序集合元素，一个有序集合元素由一个成员（member）和一个分值（score）组成。
 *
 */




    /**
     * Redis Sunionstore 命令将给定集合的并集存储在指定的集合 destination 中。如果 destination 已经存在，则将其覆盖。
     * 结果集中的元素数量。
     * @param destination0
     * @param key1
     * @return
     * @throws RedisException
     */
    public IntegerReply sunionstore(byte[] destination0, byte[]... key1) throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_SET)) return integer(0);
//
//
//        //判断类型，非set 类型返回异常信息；
//        int type = getMeta().getType();
//
//        if (type != -1 && type != DataType.KEY_SET) {
//            //抛出异常 类型不匹配
//            throw invalidValue();
//        }

        Set<ByteBuf> set = _sunion(key1);

        IntegerReply store = store(destination0, set);

        metaKey=getMetaKey(destination0);
        singleThreadExecutor.execute(new MetaCountCaller(db, getKey0(), genKeyPartten(DataType.KEY_SET_MEMBER)));

        return store;
    }





    /**
     *
     * @param members
     * @return
     * @throws RedisException
     */
    public StatusReply sadd(byte[]... members) throws RedisException {

        if (members.length == 0) {
            throw new RedisException("wrong number of arguments for SADD");
        }

//        //判断类型，非hash 类型返回异常信息；
//        int type = getMeta().getType();
//
//        if (type != -1 && type != DataType.KEY_SET) {
//            //抛出异常 类型不匹配
//            throw invalidValue();
//        }

        if (checkTypeAndTTL(getKey0(), DataType.KEY_SET)) return QUIT;


        for (int i = 0; i < members.length; i++) {
            members[i] = setNode.genKey1(getKey0(), members[i]).getKey();
            log.debug(new String(members[i]));

        }


        setNode.sadd(members);

        //后台线程计数
        MetaCountCaller taskCnt = new MetaCountCaller(db, getKey0(), genKeyPartten(DataType.KEY_SET_MEMBER));
        singleThreadExecutor.execute(taskCnt);

        return OK;
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
//
//        SetMeta setMeta = SetMeta.getInstance(RocksdbRedis.mydata, "redis".getBytes());
//        setMeta.genMetaKey("SetUpdate".getBytes()).clearMetaDataNodeData(setMeta.getKey0());
//        Assert.assertEquals(setMeta.sismember("f1".getBytes()).data().intValue(),0);
//
//        setMeta.genMetaKey("SetUpdate".getBytes()).sadd("f1".getBytes(), "f2".getBytes());
//        Assert.assertEquals(1, setMeta.sismember("f1".getBytes()).data().intValue());
//
//        Thread.sleep(200);
//
//        Assert.assertEquals(2, setMeta.scard().data().intValue());
//        Assert.assertEquals(2, setMeta.getMeta().getCount());
////        Assert.assertEquals(2, setMeta.smembers().getCount());
//
//        IntegerReply smove = setMeta.smove("SetUpdate".getBytes(), "SetMove".getBytes(), "f2".getBytes());
//
//        Thread.sleep(200);
//        log.debug(smove.data().intValue());
//
//        Assert.assertEquals(0, setMeta.genMetaKey("SetUpdate".getBytes()).sismember("f2".getBytes()).data().intValue());
//        Assert.assertEquals(1, setMeta.scard().data().intValue());
//        Assert.assertEquals(1, setMeta.genMetaKey("SetMove".getBytes()).sismember("f2".getBytes()).data().intValue());
//        Assert.assertEquals(1, setMeta.scard().data().intValue());
//
//
//
//        //////////////////////////////////
//        setMeta.genMetaKey("SetA".getBytes()).clearMetaDataNodeData(setMeta.getKey0());
//        setMeta.genMetaKey("SetB".getBytes()).clearMetaDataNodeData(setMeta.getKey0());
//        setMeta.genMetaKey("SetC".getBytes()).clearMetaDataNodeData(setMeta.getKey0());
//
//        setMeta.genMetaKey("SetA".getBytes()).sadd("a".getBytes(), "b".getBytes(), "c".getBytes(), "d".getBytes());
//        setMeta.genMetaKey("SetB".getBytes()).sadd("c".getBytes(), "d".getBytes(), "e".getBytes(), "f".getBytes());
//        setMeta.genMetaKey("SetC".getBytes()).sadd("i".getBytes(), "j".getBytes(), "c".getBytes(), "d".getBytes());
//        Thread.sleep(500);
//
//        Assert.assertEquals(setMeta.genMetaKey("SetA".getBytes()).getMeta().getCount(),4);
//        Assert.assertEquals(setMeta.genMetaKey("SetB".getBytes()).getMeta().getCount(),4);
//        Assert.assertEquals(setMeta.genMetaKey("SetC".getBytes()).getMeta().getCount(),4);
//
//        String[] sdiffstr = {"a", "b"};
//        String[] sinterstr = {"c", "d"};
//        String[] sunionstr = {"a", "b","c","d","e","f","i","j"};
//
//        //差集
//        MultiBulkReply sdiff = setMeta.sdiff("SetA".getBytes(), "SetB".getBytes(), "SetC".getBytes());
//        Assert.assertEquals(Arrays.asList(sdiffstr).toString(),sdiff.asStringSet(Charset.defaultCharset()).toString());
//        //交集
//        MultiBulkReply sinter = setMeta.sinter("SetA".getBytes(), "SetB".getBytes(), "SetC".getBytes());
//        Assert.assertEquals(Arrays.asList(sinterstr).toString(),sinter.asStringList(Charset.defaultCharset()).toString());
//        //并集
//        MultiBulkReply sunion = setMeta.sunion("SetA".getBytes(), "SetB".getBytes(), "SetC".getBytes());
//        Assert.assertEquals(Arrays.asList(sunionstr).toString(),sunion.asStringList(Charset.defaultCharset()).toString());
//
//
//        log.debug(sdiff.asStringList(Charset.defaultCharset()));
//        log.debug(sinter.asStringList(Charset.defaultCharset()));
//        log.debug(sunion.asStringList(Charset.defaultCharset()));
//
//        setMeta.sdiffstore("SetDiff".getBytes(),"SetA".getBytes(), "SetB".getBytes(), "SetC".getBytes());
//        setMeta.sinterstore("SetInter".getBytes(),"SetA".getBytes(), "SetB".getBytes(), "SetC".getBytes());
//        setMeta.sunionstore("SetUnion".getBytes(),"SetA".getBytes(), "SetB".getBytes(), "SetC".getBytes());
//
//        log.debug(setMeta.genMetaKey("SetDiff".getBytes()).smembers().asStringList(Charset.defaultCharset()));
//        log.debug(setMeta.genMetaKey("SetInter".getBytes()).smembers().asStringList(Charset.defaultCharset()));
//        log.debug(setMeta.genMetaKey("SetUnion".getBytes()).smembers().asStringList(Charset.defaultCharset()));
//
//        Assert.assertEquals(Arrays.asList(sdiffstr).toString(),setMeta.genMetaKey("SetDiff".getBytes()).smembers().asStringList(Charset.defaultCharset()).toString());
//        Assert.assertEquals(Arrays.asList(sinterstr).toString(),setMeta.genMetaKey("SetInter".getBytes()).smembers().asStringList(Charset.defaultCharset()).toString());
//        Assert.assertEquals(Arrays.asList(sunionstr).toString(),setMeta.genMetaKey("SetUnion".getBytes()).smembers().asStringList(Charset.defaultCharset()).toString());
//
//        ///////////////////////////////////////////
//
//        String randstr = setMeta.genMetaKey("SetUnion".getBytes()).spop().asUTF8String();
//        log.debug(randstr);
//        Assert.assertNotNull(randstr);
//
//        Thread.sleep(100);
//
//        Assert.assertEquals(7,setMeta.genMetaKey("SetUnion".getBytes()).scard().data().intValue());
//
//
//        MultiBulkReply srandmember = (MultiBulkReply) setMeta.genMetaKey("SetUnion".getBytes()).srandmember("3".getBytes());
//
//        log.debug((srandmember.asStringList(Charset.defaultCharset())));
//        Assert.assertEquals(srandmember.data().length,3);
//
//         srandmember = (MultiBulkReply) setMeta.genMetaKey("SetUnion".getBytes()).srandmember("1".getBytes());
//        Assert.assertEquals(srandmember.data().length,1);
//
//        srandmember = (MultiBulkReply) setMeta.genMetaKey("SetUnion".getBytes()).srandmember("5".getBytes());
//        Assert.assertEquals(srandmember.data().length,5);
//
//
//        log.debug("Over ... ...");
//
//    }

}
