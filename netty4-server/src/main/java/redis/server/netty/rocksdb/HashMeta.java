package redis.server.netty.rocksdb;

import com.google.common.base.Throwables;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.log4j.Logger;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.supermy.util.MyUtils;
import redis.netty4.*;
import redis.server.netty.RedisException;
import redis.server.netty.utis.DataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static java.lang.Double.parseDouble;
import static redis.netty4.BulkReply.NIL_REPLY;
import static redis.netty4.IntegerReply.integer;
import static redis.netty4.MultiBulkReply.EMPTY;
import static redis.netty4.StatusReply.OK;
import static redis.netty4.StatusReply.QUIT;
import static redis.server.netty.rocksdb.RedisBase.invalidValue;
import static redis.util.Encoding.bytesToNum;


/**
 * List Meta 元素方便促常用操作
 * <p>
 * Hash    [<ns>] <key> KEY_META                 KEY_HASH <MetaObject>
 * [<ns>] <key> KEY_HASH_FIELD <field>   KEY_HASH_FIELD <field-value>
 * </p>
 * <p>
 * key and value 都采用 | 分隔符号
 * * getKey 一般是包含组合键；
 * * getkey0 是纯粹的业务主键；
 * * 参见setVal0 long+int+int ttl,数据类型,数据长度；
 * * val 一般是包含ttl 的数据；val0是实际的业务数据
 * <p>
 * Created by moyong on 2017/11/9.
 * Update by moyong on 2018/09/24
 * </p>
 * <p>
 * todo 重构&测试 单个element 的操作方法在node;多个element 操作方法在meta,meta 的操作方法；
 */
public class HashMeta extends BaseMeta {

    private static Logger log = Logger.getLogger(HashMeta.class);


    protected static HashNode hashNode;

    private HashMeta() {
    }

    private static HashMeta instance = new HashMeta();

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
    public static HashMeta getInstance(RocksDB db0, byte[] ns0) {
        instance.db = db0;
        instance.NS = ns0;
        instance.VAlTYPE = DataType.KEY_HASH;
        hashNode = HashNode.getInstance(db0, ns0);
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
    public HashMeta genMetaKey(byte[] key0) throws RedisException {
        if (key0 == null) {
            throw new RedisException(String.format("key0 主键不能为空"));
        }
        instance.metaKey = MyUtils.concat(instance.NS, DataType.SPLIT, key0, DataType.SPLIT, KEYTYPE);
        return instance;
    }

//    private ByteBuf genMetaVal(long count) {
//        ByteBuf val = Unpooled.buffer(8);
//
//        val.writeLong(-1); //ttl 无限期 -1
//        val.writeBytes(DataType.SPLIT);
//
//        val.writeInt(VAlTYPE); //long 8 bit
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
//    protected HashMeta setMeta(long count) throws RedisException {
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
//    public HashMeta getMeta() throws RedisException {
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

//    /**
//     * 元素数量
//     *
//     * @return
//     */
//    public long getCount() throws RedisException {
//        return getVal0();
//    }

//
//    public void setCount(long val) throws RedisException {
//        setVal0(val);
//    }


//    @Deprecated
//    public long incrCount() throws RedisException {
//        long pval = Math.incrementExact(getCount());
//        setCount(pval);
//        return pval;
//    }
//
//    //删除元素可用
//    @Deprecated
//    public long decrCount() throws RedisException {
//        long pval = Math.decrementExact(getCount());
//        setCount(pval);
//        return pval;
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
     * 如果字段是哈希表中的一个新建字段，并且值设置成功，返回 1 。 如果哈希表中域字段已经存在且旧值已被新值覆盖，返回 0 。
     * meta 的计数，推送到任务队列进行异步处理。meta 数据有元素个数和TTL 数据。 todo 提升性能，减少交换次数。 先异步线程实现，再改为异步队列；
     *
     * @param field1
     * @param value2
     * @return
     * @throws RedisException
     */
    public IntegerReply hset(byte[] field1, byte[] value2) throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_HASH)) return integer(0);


        hset1(field1, value2);

//        metaCache.put(Unpooled.wrappedBuffer(getKey0()),getMetaVal(Math.incrementExact(getCount()),-1));

        return integer(1);
    }

    public HashMeta hset1(byte[] field1, byte[] value2) throws RedisException {
        //判断类型，非hash 类型返回异常信息；
//        int type = getMeta().getType();//hashNode.typeBy(db, getKey());
//
//        if (type != -1 && type != DataType.KEY_HASH) {
//            //抛出异常 类型不匹配
//            throw invalidValue();
//        }

        //数据持久化
        hashNode.genKey1(getKey0(), field1).hset(value2);

        //todo 增加一个异步计数队列 ；先使用异步线程，后续使用异步队列替换； setMeta(hlen().data());

        MetaCountCaller taskCnt = new MetaCountCaller(db, getKey0(), genKeyPartten(DataType.KEY_HASH_FIELD));
        singleThreadExecutor.execute(taskCnt);

        return this;
    }


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

            log.debug("MetaCountCaller ... cnt:" + cnt);

            log.debug(String.format("MetaCountCaller key=%s... cnt:%s",
                    new String(key0),
                    cnt));

            //更新缓存计数，直接修改缓存数据；
            metaCache.put(Unpooled.wrappedBuffer(key0),getMetaVal(cnt,-1));

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


    /**
     *
     * 基础方法；类型不对，不询盘提高效率；
     *
     * @param field1
     * @return
     * @throws RedisException
     */
    public BulkReply hget(byte[] field1) throws RedisException {

        //类型判定 todo 是否过期
        if (checkTypeAndTTL(getKey0(), DataType.KEY_HASH)) return NIL_REPLY;


        //获取节点数据；批次数据Rocksdb的内置缓存处理
        HashNode node = hashNode.genKey1(getKey0(), field1).hget();

        if (node == null || node.data() == null) {
            return NIL_REPLY;
        } else {
            return new BulkReply(node.getVal0());
        }

    }

    public IntegerReply hdel(byte[]... field1) throws RedisException {

        if (checkTypeAndTTL(getKey0(), DataType.KEY_HASH)) return integer(0);


        for (byte[] hkey : field1) {
            hashNode.genKey1(getKey0(), hkey).hdel();
        }

        //todo 重新计数；增加一个异步计数队列 ；先使用异步线程，后续使用异步队列替换；

        singleThreadExecutor.execute(new MetaCountCaller(db, getKey(), genKeyPartten(DataType.KEY_HASH_FIELD)));

        return integer(field1.length);
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
     * 返回 key-value 直对形式
     *
     * @param data
     * @param pattern0
     * @return
     * @throws RedisException
     */
    protected static List<byte[]> keyVals(RocksDB data, byte[] pattern0) throws RedisException {

        //按 key 检索所有数据
        List<byte[]> keys = new ArrayList<>();
        try (final RocksIterator iterator = data.newIterator()) {
            for (iterator.seek(pattern0); iterator.isValid(); iterator.next()) {

                //确保检索有数据，hkeybuf.slice 不错误
                if (pattern0.length <= iterator.key().length) {
                    ByteBuf hkeybuf = Unpooled.wrappedBuffer(iterator.key()); //优化 零拷贝
                    ByteBuf slice = hkeybuf.slice(0, pattern0.length); //获取指定前缀长度的 byte[]

                    slice.resetReaderIndex();

                    //key有序 不相等后面无数据
                    if (Arrays.equals(slice.readBytes(slice.readableBytes()).array(), pattern0)) {

//                        byte[] value = __getValue(data, iterator.key(), iterator.value());
//                        HashNode newnode = new HashNode(RocksdbRedis.mydata, getKey0(), field1);


//                        if (value != null) {
                        keys.add(iterator.key());
                        keys.add(iterator.value());

//                        }

//                        if (keys.size() >= 100000) {
//                            //数据大于1万条直接退出  fixme
//                            break;
//                        }
                    } else {
                        break;
                    }
                } else break;

            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RedisException(e.getMessage());
        }

        //检索过期数据,处理过期数据 ;暂不处理影响效率 fixme
//        log.debug(keys.size());

        return keys;
    }


    public IntegerReply hlen() throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_HASH)) return integer(0);


        Long cnt = countBy(db, genKeyPartten(DataType.KEY_HASH_FIELD));
        return integer(cnt);
    }


    public MultiBulkReply hkeys() throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_HASH)) return EMPTY;


        List<Reply<ByteBuf>> replies = new ArrayList<Reply<ByteBuf>>();


        List<byte[]> keys = keys(db, genKeyPartten(DataType.KEY_HASH_FIELD));

        for (byte[] k : keys
        ) {

            byte[] f = hashNode.parseHField(getKey0(), k);

            replies.add(new BulkReply(f));

        }

        return new MultiBulkReply(replies.toArray(new Reply[replies.size()]));
    }


    public IntegerReply hexists(byte[] field1) throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_HASH)) return integer(0);


        boolean exists = hashNode.genKey1(getKey0(), field1).exists();

        return exists ? integer(1) : integer(0);
    }

    public MultiBulkReply hvals() throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_HASH)) return EMPTY;


        //检索所有的 hash field key  所有的 key 都是有序的
        List<Reply<ByteBuf>> replies = new ArrayList<Reply<ByteBuf>>();


        List<byte[]> keyVals = keyVals(db, genKeyPartten(DataType.KEY_HASH_FIELD));//顺序读取 ;field 过期逻辑复杂，暂不处理


        int i = 0;
//        byte[] curKey = new byte[0];
        for (byte[] bt : keyVals
        ) {
            if (i % 2 == 0) {  //key 处理
//                curKey = bt;
//                ByteBuf hkeybuf1 = Unpooled.wrappedBuffer(bt); //优化 零拷贝

//                ByteBuf slice = hkeybuf1.slice(3 + key.length, bt.length - 3 - key.length);
//
//                replies.add(new BulkReply(slice.readBytes(slice.readableBytes()).array()));
            } else {
//                replies.add(new BulkReply(bt));
                replies.add(new BulkReply(hashNode.parseValue0(bt)));

            }
            i++;
        }

        return new MultiBulkReply(replies.toArray(new Reply[replies.size()]));

    }

    public MultiBulkReply hgetall() throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_HASH)) return EMPTY;


        List<Reply<ByteBuf>> replies = new ArrayList<Reply<ByteBuf>>();

        List<byte[]> keyVals = keyVals(db, genKeyPartten(DataType.KEY_HASH_FIELD));//顺序读取 ;field 过期逻辑复杂，暂不处理


        int i = 0;
        byte[] curKey = new byte[0];
        for (byte[] bt : keyVals
        ) {
            if (i % 2 == 0) {  //key 处理
                curKey = bt;

                byte[] f = hashNode.parseHField(getKey0(), bt);

                //ByteBuf hkeybuf1 = Unpooled.wrappedBuffer(bt); //优化 零拷贝
                //ByteBuf slice = hkeybuf1.slice(3+NS.length + getKey0().length+KEYTYPE.length, bt.length - 3 -NS.length - getKey0().length -KEYTYPE.length);
                //byte[] f = slice.readBytes(slice.readableBytes()).array();

                replies.add(new BulkReply(f));
            } else {

                replies.add(new BulkReply(hashNode.parseValue0(bt)));
            }
            i++;
        }

        return new MultiBulkReply(replies.toArray(new Reply[replies.size()]));
    }


    public IntegerReply hincrby(byte[] field1, byte[] increment2) throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_HASH)) return integer(0);

        long incr = bytesToNum(increment2);

        BulkReply field = hget(field1);

        if (field.data() == null) {
            hset(field1, increment2);
            return new IntegerReply(incr);
        } else {
            String fld = field.asAsciiString();
            long value = Long.parseLong(fld);

            value = value + incr;

            hset(field1, (value + "").getBytes());

            return new IntegerReply(value);
        }

    }

    public BulkReply hincrbyfloat(byte[] field1, byte[] increment2) throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_HASH)) return NIL_REPLY;


        double incr = parseDouble(new String(increment2));
        BulkReply field = hget(field1);


        if (field.data() == null) {
            hset(field1, increment2);
            return new BulkReply(increment2);
        } else {
            double value = parseDouble(new String(field.data().array()));

            value = value + incr;

            byte[] bytes = String.valueOf(value).getBytes();

            hset(field1, bytes);

            return new BulkReply(bytes);
        }
    }


    public MultiBulkReply hmget(byte[]... field1) throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_HASH)) return EMPTY;


        List<byte[]> listFds = new ArrayList<byte[]>();

        for (byte[] fd : field1
        ) {
            listFds.add(hashNode.genKey1(getKey0(), fd).getKey());
        }

        List<BulkReply> list = hashNode.hmget(listFds);

        return new MultiBulkReply(list.toArray(new BulkReply[list.size()]));

    }


    public StatusReply hmset(byte[]... field_or_value1) throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_HASH)) return QUIT;


        if (field_or_value1.length % 2 != 0) {
            throw new RedisException("wrong number of arguments for HMSET");
        }

        //处理 field key
        for (int i = 0; i < field_or_value1.length; i += 2) {
            field_or_value1[i] = hashNode.genKey1(getKey0(), field_or_value1[i]).getKey();
        }

        hashNode.hmput(field_or_value1);

        return OK;
    }

    public IntegerReply hsetnx(byte[] field1, byte[] value2) throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_HASH)) return integer(0);

        if (hexists(field1).data() == 0) {
            return integer(0);
        } else {
            hset(field1, value2);
            return integer(1);
        }

    }







    public static void main(String[] args) throws Exception {

    }


}
