package redis.server.netty.rocksdb;

import com.google.common.base.Throwables;
import com.google.common.cache.*;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.log4j.Logger;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.supermy.util.MyUtils;
import redis.netty4.BulkReply;
import redis.server.netty.RedisException;
import redis.server.netty.utis.DataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static redis.netty4.BulkReply.NIL_REPLY;
import static redis.server.netty.rocksdb.RedisBase.invalidValue;
import static redis.util.Encoding.bytesToNum;

/**
 * redis 元数据类型的基类
 *
 */
public abstract class BaseMeta extends DataMeta {
    private static Logger log = Logger.getLogger(BaseMeta.class);

    /**
     * 异步线程处理元素数据计数；冗余元素数据清理。
     *
     */
    protected ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();


    protected static byte[] NS;
    protected static byte[] KEYTYPE = DataType.KEY_META;
    protected static int VAlTYPE;


    protected static RocksDB db;

    //
    protected ByteBuf metaKey;
    protected ByteBuf metaVal;


    public String getKey0Str() throws RedisException {
        return new String(getKey0());
    }

    public byte[] getKey() throws RedisException {
        metaKey.resetReaderIndex();
        return metaKey.readBytes(metaKey.readableBytes()).array();
    }


    /**
     * 参见genkey1,获取key0的分解
     *
     * @return
     * @throws RedisException
     */
    public byte[] getKey0() throws RedisException {
//        log.debug(metaKey);
        metaKey.resetReaderIndex();
        ByteBuf bb = metaKey.slice(NS.length + 1, metaKey.readableBytes() - NS.length - DataType.SPLIT.length * 2 - KEYTYPE.length);
        return MyUtils.toByteArray(bb);
    }

    /**
     * 用于批量获取数据后获取业务key
     * @return
     * @throws RedisException
     */
    public ByteBuf getKey0(ByteBuf metaKey0) throws RedisException {
//        log.debug(metaKey);
        metaKey0.resetReaderIndex();
        ByteBuf bb = metaKey0.slice(NS.length + 1, metaKey0.readableBytes() - NS.length - DataType.SPLIT.length * 2 - KEYTYPE.length);
//        return MyUtils.toByteArray(bb);
        return bb;
    }


    /**
     * 用于批量处理获取数据后的业务value
     * @param metaVal0
     * @return
     * @throws RedisException
     */
    public static byte[] getVal0(ByteBuf metaVal0) throws RedisException {
//        log.debug(MyUtils.ByteBuf2String(metaVal0));

        metaVal0.resetReaderIndex();
        ByteBuf valueBuf = metaVal0.slice(8 + 4 + 8 + 3, metaVal0.readableBytes() - 8 - 4 - 8 - 3);

        if(valueBuf.readableBytes()==0){
            return null;
        }else
        return MyUtils.toByteArray(valueBuf);
    }

    public static ByteBuf getMetaVal0(ByteBuf metaVal) throws RedisException {
        metaVal.resetReaderIndex();
        ByteBuf valueBuf = metaVal.slice(8 + 4 + 4 + 3, metaVal.readableBytes() - 8 - 4 - 4 - 3);
        return valueBuf;
    }

    /**
     * 批量删除主键(0-9.A-Z,a-z)；
     * 根据genkey 特征，增加分割符号，避免误删除数据；
     *
     * @param key0
     * @throws RedisException
     */
    public void clearMetaDataNodeData(byte[] key0) throws RedisException {

        ByteBuf byteBufBegin = MyUtils.concat(NS, DataType.SPLIT, key0, DataType.SPLIT);
        ByteBuf byteBufEnd = MyUtils.concat(NS, DataType.SPLIT, key0, DataType.SPLIT, "z".getBytes());

        log.debug(String.format("清除数据：begin %s -> end %s",
                MyUtils.ByteBuf2String(byteBufBegin),
                MyUtils.ByteBuf2String(byteBufEnd)
                )
        );

        try {
            db.deleteRange(MyUtils.toByteArray(byteBufBegin),MyUtils.toByteArray(byteBufEnd));


        } catch (RocksDBException e) {
            e.printStackTrace();
            throw new RedisException(e.getMessage());
        }
    }


    protected void clearMetaDataNodeData(byte[] key0, byte[] type, byte[] start, byte[] stop) throws RedisException {

        ByteBuf byteBufBegin = MyUtils.concat(NS, DataType.SPLIT, key0, DataType.SPLIT,type,DataType.SPLIT,start);
        ByteBuf byteBufEnd = MyUtils.concat(NS, DataType.SPLIT, key0, DataType.SPLIT,type,DataType.SPLIT,stop);

        byte[] begin = byteBufBegin.readBytes(byteBufBegin.readableBytes()).array();
        byte[] end = byteBufEnd.readBytes(byteBufEnd.readableBytes()).array();

        log.debug(String.format("begin %s -> end %s", new String(begin),new String(end)));

        try {
            db.deleteRange(begin, end);
        } catch (RocksDBException e) {
            e.printStackTrace();
            throw new RedisException(e.getMessage());
        }
    }





    /**
     * 打印调试
     * @param buf
     */
    public void print(ByteBuf buf) {
        buf.resetReaderIndex();
        System.out.println(new String(buf.readBytes(buf.readableBytes()).array()));
    }

    /**
     *
     * 从缓存中拆分score and member
     *
     * @param scoremember
     * @return
     */
    protected static ScoreMember splitScoreMember(ByteBuf scoremember) {

        //2.拆解数据，获取分数；
//        log.debug(scoremember.capacity());
//        log.debug(new String(toByteArray(scoremember)));

        scoremember.resetReaderIndex();
        int splitIndex = scoremember.bytesBefore("|".getBytes()[0]);

        if (splitIndex>0) {

            ByteBuf scoreBuf = scoremember.slice(0, splitIndex);
            ByteBuf memberBuf = scoremember.slice(splitIndex+1 , scoremember.capacity() - splitIndex-1);

            return new ScoreMember(scoremember,scoreBuf,memberBuf);

        } else{

            return new ScoreMember(null,null,scoremember);
        }

    }

    /**
     * TTL 过期数据处理
     *
     * @return
     */
    protected long now() {
        return System.currentTimeMillis();
    }

    public long getTtl() {
        return metaVal.getLong(0);
    }

    public int getType() throws RedisException {
        if (metaVal == null) return -1;
        return metaVal.getInt(8 + 1);
    }

    /**
     * 元素数量
     *
     * @return
     */
    public long getCount() throws RedisException {

        if (metaVal == null) return 0;
        return metaVal.getLong(8 + 4 + 2);
    }

    public long getCacheCount() throws RedisException {
        try {
            //fixme 缓存更新meta数据后，刷新到最新的数据 for hashmeta
            metaVal=metaCache.get(Unpooled.wrappedBuffer(getKey0()));
        } catch (ExecutionException e) {
            e.printStackTrace();
        }


        if (metaVal == null) return 0;
        return metaVal.getLong(8 + 4 + 2);
    }

    /**
     * 检测类型与是否过期
     * @return
     * @throws RedisException
     * @param key0
     * @param dataType1
     */
    public boolean checkTypeAndTTL(byte[] key0, int dataType1) throws RedisException {
        log.trace("数据类型及数据过期(TTL)检测");
        try {
            metaVal = metaCache.get(Unpooled.wrappedBuffer(key0));

            log.trace(String.format("数据类型及数据过期(TTL)检测，key=%s value=%s count=%s dataType=%s type=%s ",
                    new String(key0),toString(metaVal),getCount(),
                    dataType1,
                    getType()
            ));

            if (getType() != -1 && getType() != dataType1) {
                //抛出异常 类型不匹配
                throw invalidValue();
            }

            //异步线程数据过期处理,对应的数据返回null；
            if (getTtl() < now() && getTtl() != -1) {
                return true;
            }

        } catch (ExecutionException e) {
            e.printStackTrace();
            log.debug(e.getMessage());
            Throwables.propagateIfPossible(e, RedisException.class);
        }
        return false;
    }

    /**
     * 构建子元素扫描key
     *
     * @param filedType
     * @return
     */
    public byte[] genKeyPartten(byte[] filedType) throws RedisException {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(NS, DataType.SPLIT, getKey0(), DataType.SPLIT, filedType, DataType.SPLIT);
        return byteBuf.readBytes(byteBuf.readableBytes()).array();
    }


    protected static class ScoreMember {

        ScoreMember(ByteBuf scoreMemberBuf, ByteBuf scoreBuf, ByteBuf memberBuf) {
            scoreMemberBuf = scoreMemberBuf;
            scoreBuf = scoreBuf;
            memberBuf = memberBuf;

            if (scoreMemberBuf != null) {
                scoreMember = toByteArray(scoreMemberBuf);
                log.debug(new String(scoreMember));
            }
            if (scoreBuf != null) {
                score = toByteArray(scoreBuf);
                scoreNum = bytesToNum(score);
                log.debug(new String(score));
            }
            if (memberBuf != null) {
                member = toByteArray(memberBuf);
                log.debug(new String(member));
            }

        }

        ByteBuf scoreMemberBuf;
        ByteBuf scoreBuf;
        ByteBuf memberBuf;

        byte[] scoreMember;
        byte[] score;
        byte[] member;

        long scoreNum;


    }

    protected static byte[] toByteArray(ByteBuf scoremember) {
        scoremember.resetReaderIndex();
        return scoremember.readBytes(scoremember.readableBytes()).array();
    }

    protected static String toString(ByteBuf scoremember) {
        return new String(toByteArray(scoremember));
    }

    protected static String toString(byte[] scoremember) {
        return new String(scoremember);
    }


   ////////cache 优化改造/////////


    /**
     * 构造罗盘key
     * @param key0
     * @return
     */
    public static ByteBuf getMetaKey(byte[] key0) {
        ByteBuf metaKey = MyUtils.concat(NS, DataType.SPLIT, key0, DataType.SPLIT, KEYTYPE);
        return metaKey;
    }


    public static byte[] getMetaKey2Byte(byte[] key0) {
        return MyUtils.toByteArray(getMetaKey(key0));
    }


    /**
     * 构造落盘value: ttl|type|count|value
     * @param value
     * @param expiration
     * @return
     */
    public static ByteBuf getMetaVal(long count, long expiration) {

        ByteBuf buf = Unpooled.buffer(16);
        buf.writeLong(expiration); //ttl 无限期 -1
        buf.writeBytes(DataType.SPLIT);

        buf.writeInt(VAlTYPE); //value type
        buf.writeBytes(DataType.SPLIT);

        buf.writeLong(count); //value count

//        buf.writeLong(value); //value size
        buf.writeBytes(DataType.SPLIT); //for String Type support

        //业务数据
//        buf.writeBytes(value);

        return buf;

    }

    /**
     * for ListLink type
     * @param count
     * @param sseq
     * @param eseq
     * @param cseq
     * @return
     */
    public static ByteBuf getMetaVal(long count, long sseq, long eseq, long cseq) {

        ByteBuf valBuf = Unpooled.buffer(16);

        valBuf.writeLong(-1); //ttl 无限期 -1
        valBuf.writeBytes(DataType.SPLIT);

        valBuf.writeInt(VAlTYPE); //value type
        valBuf.writeBytes(DataType.SPLIT);


//        ByteBuf val1 = Unpooled.buffer(32);
        valBuf.writeLong(count);  //数量  8+4+2
        valBuf.writeLong(sseq);    //第一个元素 8*2+4+2
        valBuf.writeLong(eseq);    //最后一个元素 8*3+4+2
        valBuf.writeLong(cseq);    //最新主键编号 8*4+4+2

        log.debug(String.format("count:%d 第一个元素：%d 最后一个元素：%d 自增主键：%d  value:%s",
                count, sseq, eseq, sseq, new String(valBuf.readBytes(valBuf.readableBytes()).array())));


//        val1.resetReaderIndex();
        valBuf.resetReaderIndex();

//        return Unpooled.wrappedBuffer(valBuf, val1);

        return  valBuf;
    }


    /**
     * 独立的数据key,避免metacache占用过多的内存
     * @param key0
     * @param datatype
     * @return
     */
    public static ByteBuf getDataKey(byte[] key0,byte[] datatype) {
        ByteBuf dataKey = MyUtils.concat(NS, DataType.SPLIT, key0, DataType.SPLIT,KEYTYPE,DataType.SPLIT, datatype);
        return dataKey;
    }


    public static byte[] getDataKey2Byte(byte[] key0,byte[] datatype) {
        return MyUtils.toByteArray(getDataKey(key0,datatype));
    }

    /**
     * 数据节点构造通用方法：HyperLogLog
     *
     * @param value
     * @param expiration
     * @return
     */
    public static ByteBuf genDataVal(byte[] value, long expiration) {

        ByteBuf buf = Unpooled.buffer(16);
        buf.writeLong(expiration); //ttl 无限期 -1
        buf.writeBytes(DataType.SPLIT);

        buf.writeInt(VAlTYPE); //value type
        buf.writeBytes(DataType.SPLIT);

        buf.writeLong(value.length); //value size
        buf.writeBytes(DataType.SPLIT);

        buf.writeBytes(value);

        return buf;
    }

    public static byte[] genDataByteVal(byte[] value, long expiration) {

       return  MyUtils.toByteArray(genDataVal(value, expiration));

    }

    /**
     * 业务key列表获取封装
     * 返回业务key/元数据
     * @param keys0
     * @return
     * @throws RedisException
     */
    public  BiMap<ByteBuf, ByteBuf> metaMGet(List<byte[]> keys0) throws RedisException {
        log.debug("metaMGet......begin");
        //1。封装元数据key
        List<byte[]> keys4Db = new ArrayList<byte[]>();
        for (byte[] k : keys0
        ) {
            keys4Db.add(MyUtils.toByteArray(getMetaKey(k)));
        }

        //2.批量获取数据
        Map<byte[], byte[]> fvals = null;
        try {
            fvals = db.multiGet(keys4Db);
        } catch (RocksDBException e) {
            e.printStackTrace();
            Throwables.propagateIfPossible(e,RedisException.class);
        }

        //3.解封元key,封装业务eky/元数据
        BiMap<ByteBuf, ByteBuf> values = HashBiMap.create();//业务key,元数据metaValue
        for (byte[] k:fvals.keySet()
        ) {
            log.debug(MyUtils.ByteBuf2String(getKey0(MyUtils.concat(k))));

            byte[] val = getVal0(MyUtils.concat(fvals.get(k)));
            if (val!=null){
                log.debug(new String(fvals.get(k)));
                values.put(getKey0(MyUtils.concat(k)),MyUtils.concat(fvals.get(k)));
            }

        }

        log.debug("metaMGet......end");

        return values;
    }


    /**
     *
     * 业务key获取封装
     * 返回业务元数据
     *
     * @param keys0
     * @return
     * @throws RedisException
     */
    public  byte[] metaGet(byte[] keys0) throws RedisException {
        //1。封装元数据key
        byte[] metaKey0 = MyUtils.toByteArray(getMetaKey(keys0));

        //2.获取数据
        try {
            byte[] value = db.get(metaKey0);

//            //3.解封元key,封装业务eky/元数据
//            if (value == null) {
//                return null;
//            }
//            return getVal0(Unpooled.wrappedBuffer(value));
            return value;
        } catch (RocksDBException e) {
            e.printStackTrace();
            Throwables.propagateIfPossible(e,RedisException.class);
        }
        return null;
    }


    /**
     * 通过业务key,返回业务数据
     * @param keys0
     * @return
     * @throws RedisException
     */
    public  byte[] Get(byte[] keys0) throws RedisException {
        byte[] value = metaGet(keys0);
        if (value == null) {
            return null;
        }
        return getVal0(Unpooled.wrappedBuffer(value));
    }


    /**
     * 通过业务key;返回协议业务数据
     * @param keys0
     * @return
     * @throws RedisException
     */
    public BulkReply GetRedisProtocol(byte[] keys0) throws RedisException {
        byte[] value = Get(keys0);
        if (value == null) {
            return NIL_REPLY;
        }
        return new BulkReply(value);
    }



    /**
     * meta 缓存，大多数数据落盘RocksDb 由两次简化为一次。
     * 业务key(预防不同类型的重复数据，落盘元key则起不到同样的作用)/落盘元value
     */
    public static LoadingCache<ByteBuf, ByteBuf> metaCache
            // CacheBuilder的构造函数是私有的，只能通过其静态方法newBuilder()来获得CacheBuilder的实例
            = CacheBuilder.newBuilder()
            // 设置并发级别为8，并发级别是指可以同时写缓存的线程数
            .concurrencyLevel(8)
            // 设置写缓存后8秒钟过期
            .expireAfterWrite(60, TimeUnit.SECONDS)
            // 设置缓存容器的初始容量为10
            .initialCapacity(10)
//            .maximumWeight(10*1024*1024)
            // 设置缓存最大容量为100，超过100之后就会按照LRU最近虽少使用算法来移除缓存项 注意数量是1024的倍数
            .maximumSize(100)
            // 设置要统计缓存的命中率
            .recordStats()
            // 设置缓存的移除通知 ,将数据持久化到RocksDb
            .removalListener(new RemovalListener<ByteBuf, ByteBuf>() {
                public void onRemoval(RemovalNotification<ByteBuf, ByteBuf> notification) {
                    log.debug(String.format("因为%s的原因，%s=%s已经删除",
                            notification.getCause(),
                            MyUtils.ByteBuf2String(notification.getKey()),
                            MyUtils.ByteBuf2String(notification.getValue())
                    ));

                    try {
                        {
                            ByteBuf metaKey = getMetaKey(MyUtils.toByteArray(notification.getKey()));
//                            ByteBuf metaVal = getMetaVal(notification.getValue().array(), -1);
                            notification.getValue().resetReaderIndex();//完整的数据
                            db.put(MyUtils.toByteArray(metaKey), MyUtils.toByteArray(notification.getValue()));

                            log.debug(String.format("获取缓存中Meta元数据，持久化到 RocksDb。 key:%s value:%s count:%s metakey:%s",
                                    MyUtils.ByteBuf2String(notification.getKey()),
                                    MyUtils.ByteBuf2String(notification.getValue()),
                                    notification.getValue().getLong(8+4+2),
                                    MyUtils.ByteBuf2String(metaKey)
                                    )
                            );
                        }
                    } catch (RocksDBException  e) {
                        log.debug(e.getMessage());
                        e.printStackTrace();
                    }

                }
            })

            // build方法中可以指定CacheLoader，在缓存不存在时通过CacheLoader的实现自动加载缓存
            // 从RocksDb中加载数据
            // 根据业务key,生成落盘元key,返回落盘元value(需要拆解类型信息)
            .build(new CacheLoader<ByteBuf, ByteBuf>() {
                @Override
                public ByteBuf load(ByteBuf key0) throws Exception {
                    key0.resetReaderIndex();

                    log.debug(MyUtils.ByteBuf2String(key0));

                    ByteBuf metaKey = getMetaKey(MyUtils.toByteArray(key0));

                    log.debug(MyUtils.ByteBuf2String(metaKey));
                    byte[] value = db.get(MyUtils.toByteArray(metaKey));

//                    log.debug(db.get("redis|HashUpdate|1".getBytes()));

                    if (null==value){//生成新的元数据，数量为0，落盘持久化

                        ByteBuf metaVal=null;
                        if (VAlTYPE==DataType.KEY_LIST_LINK){
                            //for ListLink type 新建元素有0个的元数据
                             metaVal = getMetaVal(0,0,0,0);
                        }else{
                             metaVal = getMetaVal(0, -1);
                        }


                        db.put(MyUtils.toByteArray(metaKey), MyUtils.toByteArray(metaVal));
                        log.debug(String.format("创建Meta元数据，持久化到 RocksDb。 key:%s value:%s count:%s",
                                MyUtils.ByteBuf2String(key0),
                                MyUtils.ByteBuf2String(metaVal),
                                metaVal.getLong(8+4+2)
                                )
                        );
                        metaVal.resetReaderIndex();
                        return metaVal;
                    }else {

                        log.debug(new String(value));

//                        log.debug(MyUtils.concat(value).getLong(0));
//                        log.debug(MyUtils.concat(value).getInt(8+1));
//                        log.debug(MyUtils.concat(value).getLong(8+4+2));

                        log.debug(String.format("获取Meta元数据RocksDb。 key:%s value:%s count:%s metakey=%s",
                                MyUtils.ByteBuf2String(key0),
                                MyUtils.ByteBuf2String(MyUtils.concat(value)),
                                MyUtils.concat(value).getLong(8+4+2),
                                MyUtils.ByteBuf2String(metaKey)
                                ));
//                        return Unpooled.wrappedBuffer(getMetaVal0(Unpooled.wrappedBuffer(value)));
//                        ByteBuf metaVal =MyUtils.concat(value);
                        return MyUtils.concat(value);

                    }
                }
            });
}
