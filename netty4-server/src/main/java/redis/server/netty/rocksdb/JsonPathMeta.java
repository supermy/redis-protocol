package redis.server.netty.rocksdb;

import com.google.common.base.Throwables;
import com.google.common.cache.*;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.google.gson.*;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import com.jayway.jsonpath.spi.cache.CacheProvider;
import com.jayway.jsonpath.spi.json.GsonJsonProvider;
import com.jayway.jsonpath.spi.mapper.GsonMappingProvider;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.log4j.Logger;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.supermy.util.MyUtils;
import org.xerial.snappy.Snappy;
import redis.netty4.BulkReply;
import redis.netty4.IntegerReply;
import redis.netty4.MultiBulkReply;
import redis.netty4.StatusReply;
import redis.server.netty.RedisException;
import redis.server.netty.utis.DataType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static redis.netty4.BulkReply.NIL_REPLY;
import static redis.netty4.IntegerReply.integer;
import static redis.netty4.StatusReply.OK;
import static redis.netty4.StatusReply.QUIT;
import static redis.server.netty.rocksdb.RedisBase.invalidValue;


/**
 *
 * JsonPath
 *
 * <p>
 * JsonPath
 * <p>
 * JsonPath      [<ns>] <key> KEY_META                 KEY_JsonPath <MetaObject>
 * <p>
 * <p>
 * key 都采用 | 分隔符号
 * 业务数据转化：jsonPath.jsonString()
 * 业务数据合并：filter.putAll(filter);
 * 业务数据恢复：JsonPath.using(conf).parse(Snappy.uncompressString(value));
 * <p>
 * * getKey 一般是包含组合键；
 * * getkey0 是纯粹的业务主键；
 *
 * <p>
 * Created by moyong on 2019/02/04.
 * Update by moyong 2019/02/04.
 * method: JPCreate key element [element ...] ；JPGet key path;JPSet key path val;JPDel key path
 * <p>
 */
public class JsonPathMeta extends BaseMeta{

    private static Logger log = Logger.getLogger(JsonPathMeta.class);


//    private static RocksDB db;

//    private byte[] NS;
//    private static byte[] TYPE = DataType.KEY_META;

//    private ByteBuf metaKey;
//    private ByteBuf metaVal;


    private JsonPathMeta() {
    }

    private static JsonPathMeta instance = new JsonPathMeta();

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
    public static JsonPathMeta getInstance(RocksDB db0, byte[] ns0) {
        instance.db = db0;
        instance.NS = ns0;
        instance.VAlTYPE = DataType.KEY_JSONPATH;
        return instance;
    }

    /**
     * 构造 MetaKey
     * <p>
     * todo
     *
     * @param key0
     * @return
     * @throws RedisException
     */
    @Deprecated
    public JsonPathMeta genMetaKey(byte[] key0) throws RedisException {
        if (key0 == null) {
            throw new RedisException(String.format("主键不能为空"));
        }
        metaKey = MyUtils.concat(instance.NS, DataType.SPLIT, key0, DataType.SPLIT, KEYTYPE);

        return this;
    }

//    @Deprecated
//    public static byte[] getMetaKey(byte[] key0) {
//        ByteBuf metaKey = MyUtils.concat(instance.NS, DataType.SPLIT, key0, DataType.SPLIT, KEYTYPE);
//        return MyUtils.toByteArray(metaKey);
//    }

//    private byte[] getKey0() {
//        metaKey.resetReaderIndex();
//        ByteBuf bb = metaKey.slice(NS.length + DataType.SPLIT.length, metaKey.readableBytes() - 8);
//        return bb.readBytes(bb.readableBytes()).array();
//    }
//
//    private String getKey0Str() {
//        return new String(getKey0());
//    }
//
//    private byte[] getKey() {
//        metaKey.resetReaderIndex();
//        return metaKey.readBytes(metaKey.readableBytes()).array();
//    }

//
//    public static byte[] getMetaVal0(ByteBuf metaVal) throws RedisException {
//        metaVal.resetReaderIndex();
//        ByteBuf valueBuf = metaVal.slice(8 + 4 + 4 + 3, metaVal.readableBytes() - 8 - 4 - 4 - 3);
//        return MyUtils.toByteArray(valueBuf);
//    }
//
//    public static byte[] getMetaVal(byte[] value, long expiration) {
//
//        ByteBuf buf = Unpooled.buffer(12);
//        buf.writeLong(expiration); //ttl 无限期 -1
//        buf.writeBytes(DataType.SPLIT);
//
//        buf.writeInt(DataType.KEY_JSONPATH); //value type
//        buf.writeBytes(DataType.SPLIT);
//
//        buf.writeInt(value.length); //value size
//        buf.writeBytes(DataType.SPLIT);
//
//        //业务数据
//        buf.writeBytes(value);
//
//        return MyUtils.toByteArray(buf);
//
//    }



    /**
     * 通过key 获取string 的数据
     *
     * @param key0
     * @return
     * @throws RedisException
     */
    public BulkReply get(byte[] key0) throws RedisException {
        if (checkTypeAndTTL(key0, DataType.KEY_JSONPATH)) return NIL_REPLY;

        byte[] val0 = getVal0(metaVal);

        return val0==null?NIL_REPLY:new BulkReply(val0);
//
//        try {
//
//            byte[] values = db.get(getMetaKey(key0));
//
//            if (values == null) {
//                return NIL_REPLY;
//            }
//
//            this.metaVal = Unpooled.wrappedBuffer(values);
//
//            return new BulkReply(key0);
//
//        } catch (RocksDBException e) {
//            e.printStackTrace();
//            throw new RedisException(e.getMessage());
//        }

    }

//
//    //////////////////////
//    public int getType() throws RedisException {
//        if (metaVal == null) return -1;
//        return this.metaVal.getInt(8 + 1);
//    }
//
//    /**
//     * 获取meta 数据
//     *
//     * @return
//     * @throws RedisException
//     */
//    protected JsonPathMeta getMeta() throws RedisException {
//
//        try {
//            byte[] value = db.get(getKey());
//            if (value == null) this.metaVal = null;
//            else
//                this.metaVal = MyUtils.concat(value);
//        } catch (RocksDBException e) {
//            e.printStackTrace();
//            throw new RedisException(e.getMessage());
//        }
//
//        return this;
//    }


    /**
     * 创建JsonPath
     *
     * @param json0
     * @return
     * @throws RedisException
     */
    public StatusReply jpcreate(byte[] json0) throws RedisException {

        if (checkTypeAndTTL(getKey0(), DataType.KEY_JSONPATH)) return QUIT;
//
//        //判断类型，非hash 类型返回异常信息；
//        int type = getMeta().getType();
//
//        if (type != -1 && type != DataType.KEY_JSONPATH) {
//            //抛出异常 类型不匹配
//            throw invalidValue();
//        }

//        log.debug(getKey0Str());

        //进行json格式校验，然后压缩存储
        Gson gson3 = new GsonBuilder().enableComplexMapKeySerialization().create(); //开启复杂处理Map方法
        JsonObject jsonObject = gson3.fromJson(new String(json0), JsonObject.class);//进行json格式验证；


        try {
            db.put(getDataKey2Byte(getKey0(),DataType.KEY_JSONPATH_DATA), genDataByteVal(Snappy.compress(json0), -1));
        } catch (IOException | RocksDBException e) {
            e.printStackTrace();
            Throwables.propagateIfPossible(e, RedisException.class);
        }

        return OK;
    }


    /**
     * 支持单个jsonpath查询
     * @param jsonpath0
     * @return
     * @throws RedisException
     */
    public MultiBulkReply jpget(byte[] jsonpath0) throws RedisException {
        if (checkTypeAndTTL(getKey0(), DataType.KEY_JSONPATH)) return MultiBulkReply.EMPTY;

        JsonElement result = null;
        try {
            DocumentContext json  = jsonCache.get(Unpooled.wrappedBuffer(getKey0()));
            result = json.read(new String(jsonpath0));
        } catch (PathNotFoundException e) {
            log.error(e.getMessage());
            e.printStackTrace();
            return MultiBulkReply.EMPTY;
//            Throwables.propagateIfPossible(e, RedisException.class);
        } catch (ExecutionException e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }

        if (null==result){
            return MultiBulkReply.EMPTY;
        }

        log.debug(String.format("json get :%s", result));
        List<BulkReply> list = new ArrayList<BulkReply>();
        if (result.isJsonArray()){
            JsonArray asJsonArray = result.getAsJsonArray();
            for (JsonElement je:asJsonArray)
            {
                list.add(new BulkReply(je.getAsString().getBytes()));
            }

        }
        if (result.isJsonObject()){
            list.add(new BulkReply(result.getAsJsonObject().getAsString().getBytes()));
        }
        if (result.isJsonPrimitive()){
            list.add(new BulkReply(result.getAsJsonPrimitive().getAsString().getBytes()));
        }
        if (result.isJsonNull()){
            return MultiBulkReply.EMPTY;
        }

        return new MultiBulkReply(list.toArray(new BulkReply[list.size()]));

    }


    /**
     * bfexists 命令返回给定 filed 是否存在。
     *
     * @param field1
     * @return
     * @throws RedisException
     */
    public IntegerReply jpset(byte[] jsonpath0,byte[] val1) throws RedisException {


        if (jsonpath0.length == 0) {
            throw new RedisException("wrong number of arguments for JPGet");
        }

        if (checkTypeAndTTL(getKey0(), DataType.KEY_JSONPATH)) return integer(0);

        //判断类型，非hash 类型返回异常信息；
//        int type = getMeta().getType();
//
//        if (type != -1 && type != DataType.KEY_JSONPATH) {
//            //抛出异常 类型不匹配
//            throw invalidValue();
//        }
//
//        log.debug(getKey0Str());

        try {
            DocumentContext json  = jsonCache.get(Unpooled.wrappedBuffer(getKey0()));
            JsonPath p = JsonPath.compile(new String(jsonpath0));
            json.set(p,new String(val1));
        } catch (ExecutionException e) {
            e.printStackTrace();
            Throwables.propagateIfPossible(e, RedisException.class);
        }

        //及时进行持久化
        jsonCache.invalidate(Unpooled.wrappedBuffer(getKey0()));

        return integer(1);
    }

    /**
     * 合并两个BloomFiler的数据
     *
     * @param key0
     * @return
     * @throws RedisException
     */
    public IntegerReply jpdel(byte[] jsonpath0) throws RedisException {

        if (checkTypeAndTTL(getKey0(), DataType.KEY_JSONPATH)) return integer(0);

        try {
            DocumentContext json  = jsonCache.get(Unpooled.wrappedBuffer(getKey0()));
            JsonPath p = JsonPath.compile(new String(jsonpath0));
            json.delete(p);
//            result = json.read(new String(jsonpath0), List.class);
        } catch (ExecutionException e) {
            e.printStackTrace();
            Throwables.propagateIfPossible(e, RedisException.class);
        }

        //及时进行持久化
        jsonCache.invalidate(Unpooled.wrappedBuffer(getKey0()));

        return integer(1);
    }

    /**
     * 批量删除主键(0-9.A-Z,a-z)；
     * 根据genkey 特征，增加风格符号，避免误删除数据；
     *
     * @param key0
     * @throws RedisException
     */
    public void deleteRange(byte[] key0) throws RedisException {

        ByteBuf byteBufBegin = MyUtils.concat(NS, DataType.SPLIT, key0, DataType.SPLIT);
        ByteBuf byteBufEnd = MyUtils.concat(NS, DataType.SPLIT, key0, DataType.SPLIT, "z".getBytes());

        byte[] begin = byteBufBegin.readBytes(byteBufBegin.readableBytes()).array();
        byte[] end = byteBufEnd.readBytes(byteBufEnd.readableBytes()).array();

        log.debug(String.format("begin %s -> end %s", new String(begin), new String(end)));

        try {
            db.deleteRange(begin, end);
        } catch (RocksDBException e) {
            e.printStackTrace();
            throw new RedisException(e.getMessage());
        }
    }



    static LoadingCache<ByteBuf, DocumentContext> jsonCache
            // CacheBuilder的构造函数是私有的，只能通过其静态方法newBuilder()来获得CacheBuilder的实例
            = CacheBuilder.newBuilder()
            // 设置并发级别为8，并发级别是指可以同时写缓存的线程数
            .concurrencyLevel(8)
            // 设置写缓存后8秒钟过期
            .expireAfterWrite(60, TimeUnit.SECONDS)
            // 设置缓存容器的初始容量为10
            .initialCapacity(10)
//            .maximumWeight(10*1024*1024)
            // 设置缓存最大容量为100，超过100之后就会按照LRU最近虽少使用算法来移除缓存项
            .maximumSize(100)
            // 设置要统计缓存的命中率
            .recordStats()
            // 设置缓存的移除通知 ,将数据持久化到RocksDb
            .removalListener(new RemovalListener<ByteBuf, DocumentContext>() {
                public void onRemoval(RemovalNotification<ByteBuf, DocumentContext> notification) {
                    log.debug(String.format("因为%s的原因，%s=%s已经删除",
                            notification.getCause(),
                            MyUtils.ByteBuf2String(notification.getKey()),
                            notification.getValue().jsonString()));

                    try {
                        {
                            DocumentContext json = notification.getValue();

//                            db.put(getMetaKey(notification.getKey().array()), getMetaVal(Snappy.compress(json.jsonString()), -1));
                            db.put(getDataKey2Byte(notification.getKey().array(),DataType.KEY_JSONPATH_DATA), genDataByteVal(Snappy.compress(json.jsonString()), -1));
//                            db.put(notification.getKey().array(), Snappy.compress(json.jsonString()));

                            log.debug(String.format("获取DocumentContext Json文档，持久化到 RocksDb。\n key:%s json:%s",
                                    MyUtils.ByteBuf2String(notification.getKey()),
                                    json.jsonString()
                                    )
                            );
                        }
                    } catch (RocksDBException | IOException e) {
                        e.printStackTrace();
//                        Throwables.propagateIfPossible(e, RedisException.class);
                    }

                }
            })

            // build方法中可以指定CacheLoader，在缓存不存在时通过CacheLoader的实现自动加载缓存
            // 从RocksDb中加载数据
            .build(new CacheLoader<ByteBuf, DocumentContext>() {
                @Override
                public DocumentContext load(ByteBuf key) throws Exception {
                    key.resetReaderIndex();

//                    byte[] value = db.get(key.array());
//                    byte[] value = db.get(getMetaKey(key.array()));
                    byte[] value = db.get(getDataKey2Byte(key.array(),DataType.KEY_JSONPATH_DATA));



                    Configuration conf = Configuration.builder().jsonProvider(new GsonJsonProvider()).mappingProvider(new GsonMappingProvider()).build();
//                    CacheProvider.setCache(new com.jayway.jsonpath.spi.cache.Cache() {
//                        com.google.common.cache.Cache<String, JsonPath> caches = CacheBuilder.newBuilder()
//                                .maximumSize(1000)
//                                .expireAfterWrite(10, TimeUnit.MINUTES)
//                                .build();
//
//                        @Override
//                        public JsonPath get(String key) {
////                            log.debug("get===========key:" + key);
//                            return caches.getIfPresent(key);
//                        }
//
//                        @Override
//                        public void put(String key, JsonPath jsonPath) {
//                            log.debug("put===========key:" + key);
//                            caches.put(key, jsonPath);
//                        }
//
//                    });

                    byte[] metaVal0 = getVal0(MyUtils.concat(value));
                    DocumentContext json = JsonPath.using(conf).parse(Snappy.uncompressString(metaVal0));

                    log.debug(String.format("获取DocumentContext Json文档，持久化到 RocksDb。\n key:%s json:%s",
                            MyUtils.ByteBuf2String(key),
                            json.jsonString()
                            )
                    );

                    return json;
                }
            });



    public static void main(String[] args) throws Exception {
    }


}
