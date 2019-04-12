package guava;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.google.common.base.Strings;
import com.google.common.cache.*;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.spi.cache.CacheProvider;
import com.jayway.jsonpath.spi.json.GsonJsonProvider;
import com.jayway.jsonpath.spi.mapper.GsonMappingProvider;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.supermy.util.MyUtils;
import org.xerial.snappy.Snappy;
import redis.server.netty.rocksdb.RocksdbRedis;
import redis.server.netty.utis.DbUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 谷歌LoadingCache缓存使用
 * <p>
 * <p>
 * <p> 作为LoadingCache 与 Cache实例的创建者，具有以下特征： </p>
 * <p>       1、自动载入键值至缓存；</p>
 * <p>       2、当缓存器溢出时，采用最近最少使用原则进行替换。</p>
 * <p>       3、过期规则可基于最后读写时间。</p>
 * <p>       4、设置键值引用级别。</p>
 * <p>       5、元素移出通知。</p>
 * <p>       6、缓存访问统计。</p><p>
 */
public class Cache4RocksTest {

    private static Logger log = Logger.getLogger(Cache4RocksTest.class);

    // 缓存接口这里是LoadingCache，LoadingCache在缓存项不存在时可以自动加载缓存
    static LoadingCache<Integer, HashMap> kvCache
            // CacheBuilder的构造函数是私有的，只能通过其静态方法newBuilder()来获得CacheBuilder的实例
            = CacheBuilder.newBuilder()
            // 设置并发级别为8，并发级别是指可以同时写缓存的线程数
            .concurrencyLevel(8)
//            .refreshAfterWrite(1, TimeUnit.MINUTES)// 给定时间内没有被读/写访问，则回收。
//            .expireAfterAccess(3, TimeUnit.SECONDS)// 缓存过期时间为3秒

            // 设置写缓存后8秒钟过期 给定时间内没有写访问，则回收。
            .expireAfterWrite(8, TimeUnit.SECONDS)
            // 设置缓存容器的初始容量为10
            .initialCapacity(10)
            // 设置缓存最大容量为100，超过100之后就会按照LRU最近虽少使用算法来移除缓存项
            .maximumSize(20)
            // 设置要统计缓存的命中率
            .recordStats()
            // 设置缓存的移除通知
            .removalListener(new RemovalListener<Object, Object>() {
                public void onRemoval(RemovalNotification<Object, Object> notification) {
                    log.debug(String.format("因为%s的原因，%s=%s已经删除", notification.getCause(), notification.getKey(), notification.getValue()));
                }
            })
            // build方法中可以指定CacheLoader，在缓存不存在时通过CacheLoader的实现自动加载缓存
            .build(new CacheLoader<Integer, HashMap>() {
                /**
                 * 当本地缓存命没有中时，调用load方法获取结果并将结果缓存
                 * @param key
                 * @return
                 * @throws Exception
                 */
                @Override
                public HashMap load(Integer key) throws Exception {
                    log.debug(String.format("加载%s", key));
                    HashMap student = new HashMap();
                    student.put("key", key);
                    student.put("name ", key + " auto gen data");
                    return student;

                }
            });


    /**
     * 获取数据之后，修改缓存的数据，可以在后续持久化
     * 08:03:50,057 DEBUG CacheTest:46 - 因为EXPIRED的原因，19={modify=modify, key=19, name =19 auto gen data}已经删除
     *
     * @throws Exception
     */
    @Test
    public void cache() throws Exception {
        /*
        显式插入:该方法可以直接向缓存中插入值，如果缓存中有相同key则之前的会被覆盖。

        cache.put(key, value);
        cache.invalidate(key); //单个清除
        cache.invalidateAll(keys); //批量清除
        cache.invalidateAll(); //清除所有缓存项
        expireAfterAccess(long, TimeUnit); 该键值对最后一次访问后超过指定时间再移除
        expireAfterWrite(long, TimeUnit) ;该键值对被创建或值被替换后超过指定时间再移除

        基于大小的移除:指如果缓存的对象格式即将到达指定的大小，就会将不常用的键值对从cache中移除。
　　     size是指cache中缓存的对象个数。当缓存的个数开始接近size的时候系统就会进行移除的操作
        cacheBuilder.maximumSize(long)
        */

        for (int i = 0; i < 20; i++) {
            // 从缓存中得到数据，由于我们没有设置过缓存，所以需要通过CacheLoader加载缓存数据
//            HashMap student = kvCache.get(i);
            HashMap student = kvCache.get(1);
            log.debug(String.format("获取数据%s", student));
            //修改数据，影响到缓存的数据；
            student.put("modify", "modify");

            log.debug(kvCache.get(1));//验证缓存引用被修改
            // 休眠1秒
            TimeUnit.SECONDS.sleep(1);
        }

        log.debug(String.format("命中率统计信息：%s", kvCache.stats().toString()));

//        cahceBuilder.invalidateAll();
    }

    static RocksDB mymeta = RocksdbRedis.mymeta;
    // 缓存接口这里是LoadingCache，LoadingCache在缓存项不存在时可以自动加载缓存
    // fixme key 不支持byte[]
    static LoadingCache<ByteBuf, BloomFilter<byte[]>> bloomFilterCache
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
            .removalListener(new RemovalListener<ByteBuf, BloomFilter<byte[]>>() {
                public void onRemoval(RemovalNotification<ByteBuf, BloomFilter<byte[]>> notification) {
                    log.debug(String.format("因为%s的原因，%s=%s已经删除",
                            notification.getCause(),
                            MyUtils.ByteBuf2String(notification.getKey()),
                            notification.getValue().expectedFpp()));

                    try {
                        ByteArrayOutputStream output = new ByteArrayOutputStream();
                        {
                            BloomFilter<byte[]> filter = notification.getValue();
                            filter.writeTo(output);
                            output.flush();
                            mymeta.put(notification.getKey().array(), output.toByteArray());

                            log.debug(String.format("获取bloomFilter(误差率%s) 列表，持久化到 RocksDb。\n key:%s 数量%s 初始验证：%s  修改验证：%s",
                                    filter.expectedFpp(),
                                    notification.getKey(),
                                    filter.approximateElementCount(),
                                    filter.mightContain("test_one".getBytes()),
                                    filter.mightContain("test_modify".getBytes())
                            ));
                        }
                    } catch (RocksDBException | IOException e) {
                        e.printStackTrace();
//                        Throwables.propagateIfPossible(e, RedisException.class);
                    }

                }
            })

            // build方法中可以指定CacheLoader，在缓存不存在时通过CacheLoader的实现自动加载缓存
            // 从RocksDb中加载数据
            .build(new CacheLoader<ByteBuf, BloomFilter<byte[]>>() {
                @Override
                public BloomFilter<byte[]> load(ByteBuf key) throws Exception {
                    byte[] value = mymeta.get(key.array());
                    ByteArrayInputStream input = new ByteArrayInputStream(value);
                    BloomFilter<byte[]> filter = BloomFilter.readFrom(input, Funnels.byteArrayFunnel());

                    log.debug(String.format("加载BloomFilter(误差率%s)从RocksDb: key %s  数量%s  初始验证：%s 修改验证：%s",
                            filter.expectedFpp(),
                            MyUtils.ByteBuf2String(key),
                            filter.approximateElementCount(),
                            filter.mightContain("test_one".getBytes()),
                            filter.mightContain("test_modify".getBytes())
                    ));
                    return filter;
                }
            });


    /**
     * 验证是否存在--->缓存--->业务处理--->返回结果---->结束
     * \
     * \缓存无数据--->加载RocksDb数据---->业务处理----->返回结果----->结束
     * \
     * \缓存数据失效---->持久化数据到RocksDb;
     * <p>
     * <p>
     * 命中率统计信息：CacheStats{hitCount=0, missCount=0, loadSuccessCount=0, loadExceptionCount=0, totalLoadTime=0, evictionCount=0}
     * 13:42:54,655 DEBUG CacheTest:234 - BloomFilter恢复创建，共BloomFilter.readFrom 1000 个时间8772毫秒;平均单个value(大小2.29MB)put时间：8毫秒
     * 13:42:54,689 DEBUG CacheTest:179 - 加载BloomFilter(误差率6.413930923868137E-81)从RocksDb: key bloom_filter  数量1  初始验证：true 修改验证：false
     * 13:42:56,317 DEBUG CacheTest:268 - BloomFilter 过滤器，共put 1000000 个时间1653毫秒;平均单个value(缓存)gut时间：1653纳秒
     * 13:42:57,201 DEBUG CacheTest:290 - 漏网监测，BloomFilter 过滤器，共mightContain 1000000 个时间874毫秒;平均单个value(缓存)mightContain时间：874纳秒
     * 13:42:57,214 DEBUG CacheTest:310 - 万分之一的错杀率。
     * 判定1000000个数据，错杀：1, 花费时间:11毫秒
     * 13:42:57,214 DEBUG CacheTest:315 - 错杀监测，BloomFilter 过滤器，共mightContain 1000000 个时间12毫秒;平均单个value(缓存)mightContain时间：12纳秒
     * 13:42:57,215 DEBUG CacheTest:324 - 命中率统计信息：CacheStats{hitCount=2009999, missCount=1, loadSuccessCount=1, loadExceptionCount=0, totalLoadTime=15016816, evictionCount=0}
     *
     * @throws Exception
     */
    @Test
    public void cacheBloomFilterToRocksDb() throws Exception {

        log.debug(String.format("命中率统计信息：%s", bloomFilterCache.stats().toString()));

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        {
            //数据准备
            long expectedInsertions = 1000000;//预估数量
            double fpp = 0.0001;//错误率  万分之一
            BloomFilter<byte[]> filter = BloomFilter.create(Funnels.byteArrayFunnel(), expectedInsertions, fpp);
            filter.put("test_one".getBytes());
            filter.writeTo(output);
            output.flush();
            mymeta.put("bloom_filter".getBytes(), output.toByteArray());
        }


        {
            //测试平均恢复时间
            long start = System.currentTimeMillis();
            long count = 1000;

            HashMap<String, BloomFilter<byte[]>> map = new HashMap<String, BloomFilter<byte[]>>();
            for (int i = 0; i < count; i++) {

                ByteArrayInputStream input = new ByteArrayInputStream(output.toByteArray());
                BloomFilter<byte[]> filter = BloomFilter.readFrom(input, Funnels.byteArrayFunnel());

                map.put("filter", filter);
                Assert.assertTrue(map.get("filter").mightContain("test_one".getBytes()));
            }

            log.debug(String.format("BloomFilter恢复创建，共BloomFilter.readFrom %s 个时间%s毫秒;平均单个value(大小%s)put时间：%s毫秒",
                    count,
                    (System.currentTimeMillis() - start),
                    MyUtils.bytes2kb(output.toByteArray().length),
                    (System.currentTimeMillis() - start) / count));


        }


//        {
        long count = 1000000;
        ByteBuf key = Unpooled.wrappedBuffer("bloom_filter".getBytes());
        long start0 = System.nanoTime();

        {  //初始化数据
            for (int i = 0; i < count; i++) {
                // 从缓存中得到数据，由于我们没有设置过缓存，所以需要通过CacheLoader加载缓存数据
                BloomFilter<byte[]> filter = bloomFilterCache.get(key);

//                log.debug(String.format("获取BloomFilter从RocksDb: key %s 数量%s  初始验证：%s 修改验证：%s",
//                        key,
//                        filter.approximateElementCount(),
//                        filter.mightContain("test_one".getBytes()),
//                        filter.mightContain("test_modify".getBytes())
//                ));

                // 休眠1秒
//                TimeUnit.MILLISECONDS.sleep(100);

                //
                filter.put(("test_abc" + i).getBytes());
                filter.put(("test_modify").getBytes());
            }
            log.debug(String.format("BloomFilter 过滤器，共put %s 个时间%s毫秒;平均单个value(%s)gut时间：%s纳秒",
                    count,
                    (System.nanoTime() - start0) / 1000 / 1000,
                    "缓存",
                    (System.nanoTime() - start0) / count));

        }


        {

            start0 = System.nanoTime();
            //验证数据
            for (int i = 0; i < count; i++) {
                // 从缓存中得到数据，由于我们没有设置过缓存，所以需要通过CacheLoader加载缓存数据
                BloomFilter<byte[]> bloomFilter = bloomFilterCache.get(key);

                if (!bloomFilter.mightContain(("test_abc" + i).getBytes())) {
                    log.debug(String.format("漏网之鱼:%s", "test_abc" + i));
                }
            }

            log.debug(String.format("漏网监测，BloomFilter 过滤器，共mightContain %s 个时间%s毫秒;平均单个value(%s)mightContain时间：%s纳秒",
                    count,
                    (System.nanoTime() - start0) / 1000 / 1000,
                    "缓存",
                    (System.nanoTime() - start0) / count));
        }


        {

            long start = System.currentTimeMillis();
            start0 = System.nanoTime();

            List<String> list = new ArrayList<String>(1000);
            for (long i = count + 10000; i < count + 20000; i++) {
                BloomFilter<byte[]> filter = bloomFilterCache.get(key);
                if (filter.mightContain(("test_abc" + i).getBytes())) {
                    list.add("test_abc_" + i);
                }
            }
            log.debug(String.format("万分之一的错杀率。\n 判定%s个数据，错杀：%s, 花费时间:%s毫秒",
                    count,
                    list.size(),
                    System.currentTimeMillis() - start));

            log.debug(String.format("错杀监测，BloomFilter 过滤器，共mightContain %s 个时间%s毫秒;平均单个value(%s)mightContain时间：%s纳秒",
                    count,
                    (System.nanoTime() - start0) / 1000 / 1000,
                    "缓存",
                    (System.nanoTime() - start0) / count));

        }


        log.debug(String.format("命中率统计信息：%s", bloomFilterCache.stats().toString()));
    }


    static LoadingCache<ByteBuf, HyperLogLog> hyperLogLogCache
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
            .removalListener(new RemovalListener<ByteBuf, HyperLogLog>() {
                public void onRemoval(RemovalNotification<ByteBuf, HyperLogLog> notification) {
                    log.debug(String.format("因为%s的原因，%s=%s已经删除",
                            notification.getCause(),
                            MyUtils.ByteBuf2String(notification.getKey()),
                            notification.getValue().cardinality()));

                    try {
                        ByteArrayOutputStream output = new ByteArrayOutputStream();
                        {
                            HyperLogLog hll = notification.getValue();

                            mymeta.put(notification.getKey().array(), hll.getBytes());

                            log.debug(String.format("获取HyperLogLog(基数=%s) 列表，持久化到 RocksDb。\n key:%s 初始验证：%s  修改验证：%s",
                                    hll.cardinality(),
                                    MyUtils.ByteBuf2String(notification.getKey()),
                                    hll.offer("test_one".getBytes()),
                                    hll.offer("test_modify".getBytes())
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
            .build(new CacheLoader<ByteBuf, HyperLogLog>() {
                @Override
                public HyperLogLog load(ByteBuf key) throws Exception {
                    key.resetReaderIndex();
                    byte[] value = mymeta.get(key.array());
                    HyperLogLog hll = HyperLogLog.Builder.build(value);

                    log.debug(String.format("加载HyperLogLog从RocksDb: key %s  数量%s  初始验证：%s ",
                            MyUtils.ByteBuf2String(key),
                            hll.cardinality(),
                            hll.offer("test_one".getBytes()))
                    );
                    return hll;
                }
            });


    /**
     * 基于基数统计，花费时间756毫秒 ,单个花费756纳秒；Total count:[1000000]  Unique count:[988401] FreeMemory:[119070K] ..
     * 22:39:54,876 DEBUG Cache4RocksTest:448 - 命中率统计信息：CacheStats{hitCount=1000000, missCount=1, loadSuccessCount=1, loadExceptionCount=0, totalLoadTime=6693129, evictionCount=0}
     *
     * @throws Exception
     */
    @Test
    public void cacheHyperLogLogToRocksDb() throws Exception {

        log.debug(String.format("基数统计信息：%s", hyperLogLogCache.stats().toString()));

        ByteBuf key = Unpooled.wrappedBuffer("hyperLogLog".getBytes());
        {
            //数据准备
            HyperLogLog hll = new HyperLogLog(12);
            hll.offer("test_one".getBytes());
            mymeta.put(key.array(), hll.getBytes());
        }


        long count = 1000000;
        long start0 = System.nanoTime();
        Runtime rt = Runtime.getRuntime();
        {  //初始化数据
            HyperLogLog hll = null;
            hll = hyperLogLogCache.get(key);//尽量放在循环体外，可以节约一半的时间；

            for (int i = 0; i < count + 1; i++) {
                hll.offer("" + i);

                if (i % 50000 == 1) {
                    log.debug(String.format("\n Total count:[%s]  Unique count:[%s] FreeMemory:[%sK] ..",
                            i,
                            hll.cardinality(),
                            rt.freeMemory() / 1024
                    ));
                }
            }

            log.debug(String.format("\n 基于基数统计，花费时间%s毫秒 ,单个花费%s纳秒；Total count:[%s]  Unique count:[%s] FreeMemory:[%sK] ..",
                    (System.nanoTime() - start0) / 1000 / 1000,
                    (System.nanoTime() - start0) / count,
                    count,
                    hll.cardinality(),
                    rt.freeMemory() / 1024
            ));

        }

        log.debug(String.format("命中率统计信息：%s", hyperLogLogCache.stats().toString()));
    }


    /**
     * 容量计算，1亿手机占用容量：字符，1049.05MB 压缩后字节，49.37MB
     * 50MB 对于RocksDb是大Value
     * 对象级别的存储在Hash文件;
     * 元素级别的存储在RocksDb;
     * metaData先缓存在内存，再定时存储在RocksDb;
     * nodeData存储在RocksDb;
     *
     * @throws Exception
     */
    @Test
    public void capToRocksDb() throws Exception {

        String phones = Strings.repeat("13689588588", 10000000);

        log.debug(String.format("1亿手机占用容量：字符，%s 压缩后字节，%s",
                MyUtils.bytes2kb(phones.length() * 10),
                MyUtils.bytes2kb(Snappy.compress(phones).length * 10)
        ));

    }

    static LoadingCache<ByteBuf, Roaring64NavigableMap> bitCache
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
            .removalListener(new RemovalListener<ByteBuf, Roaring64NavigableMap>() {
                public void onRemoval(RemovalNotification<ByteBuf, Roaring64NavigableMap> notification) {
                    log.debug(String.format("因为%s的原因，%s=%s已经删除",
                            notification.getCause(),
                            MyUtils.ByteBuf2String(notification.getKey()),
                            notification.getValue().getLongCardinality()));

                    try {
                        {
                            Roaring64NavigableMap r64nm = notification.getValue();

                            mymeta.put(notification.getKey().array(), Snappy.compress(DbUtils.exportBitmap(r64nm).toByteArray()));

                            log.debug(String.format("获取Roaring64NavigableMap(数量=%s) 列表，持久化到 RocksDb。\n key:%s 初始验证：%s  修改验证：%s",
                                    r64nm.getLongCardinality(),
                                    MyUtils.ByteBuf2String(notification.getKey()),
                                    r64nm.contains(13691588588l),
                                    r64nm.contains(15510325566l))
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
            .build(new CacheLoader<ByteBuf, Roaring64NavigableMap>() {
                @Override
                public Roaring64NavigableMap load(ByteBuf key) throws Exception {
                    key.resetReaderIndex();
                    byte[] value = Snappy.uncompress(mymeta.get(key.array()));

                    Roaring64NavigableMap r64nm = DbUtils.importBitmap(value);

                    log.debug(String.format("加载Roaring64NavigableMap: key %s  数量%s  初始验证：%s 修改验证：%s\"",
                            MyUtils.ByteBuf2String(key),
                            r64nm.getLongCardinality(),
                            r64nm.contains(13691588588l),
                            r64nm.contains(15510325566l))
                    );

                    r64nm.addLong(15510325566l);

                    return r64nm;
                }
            });


    /**
     * 有使用Snappy进行数据压缩
     * <p>
     * 22:35:04,926 DEBUG Cache4RocksTest:539 - 加载HyperLogLog从RocksDb: key roaring_bit  数量1  初始验证：true 修改验证：false"
     * 22:35:05,444 DEBUG Cache4RocksTest:604 - Roaring64NavigableMap 位计算，共put 1000000 个时间541毫秒;平均单个value(缓存)gut时间：541纳秒
     * 22:35:05,762 DEBUG Cache4RocksTest:626 - 漏网监测，Roaring64NavigableMap 过滤器，contains 1000000 个时间316毫秒;平均单个value(缓存)contains时间：316纳秒
     * 22:35:05,762 DEBUG Cache4RocksTest:634 - 命中率统计信息：CacheStats{hitCount=1999999, missCount=1, loadSuccessCount=1, loadExceptionCount=0, totalLoadTime=15509755, evictionCount=0}
     * <p>
     * 循环内获取cache
     * 22:36:02,950 DEBUG Cache4RocksTest:539 - 加载HyperLogLog从RocksDb: key roaring_bit  数量1  初始验证：true 修改验证：false"
     * 22:36:17,755 DEBUG Cache4RocksTest:614 - Roaring64NavigableMap 位计算，共put 100000000 个时间14877毫秒;平均单个value(缓存)gut时间：148纳秒
     * 22:36:32,966 DEBUG Cache4RocksTest:636 - 漏网监测，Roaring64NavigableMap 过滤器，contains 100000000 个时间15202毫秒;平均单个value(缓存)contains时间：152纳秒
     * 22:36:32,975 DEBUG Cache4RocksTest:644 - 命中率统计信息：CacheStats{hitCount=199999999, missCount=1, loadSuccessCount=1, loadExceptionCount=0, totalLoadTime=13996846, evictionCount=0}
     * <p>
     * 循环外获取cache
     * 22:37:37,222 DEBUG Cache4RocksTest:539 - 加载HyperLogLog从RocksDb: key roaring_bit  数量1  初始验证：true 修改验证：false"
     * 22:37:40,951 DEBUG Cache4RocksTest:620 - Roaring64NavigableMap 位计算，共put 100 000 000 个时间3771毫秒;平均单个value(缓存)gut时间：37纳秒
     * 22:37:43,879 DEBUG Cache4RocksTest:643 - 漏网监测，Roaring64NavigableMap 过滤器，contains 100 000 000 个时间2924毫秒;平均单个value(缓存)contains时间：29纳秒
     * 22:37:43,880 DEBUG Cache4RocksTest:651 - 命中率统计信息：CacheStats{hitCount=1, missCount=1, loadSuccessCount=1, loadExceptionCount=0, totalLoadTime=12113301, evictionCount=0}
     *
     * @throws Exception
     */
    @Test
    public void cacheBitToRocksDb() throws Exception {

        log.debug(String.format("命中率统计信息：%s", bitCache.stats().toString()));

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        {
            //数据准备
            Roaring64NavigableMap r64nm = new Roaring64NavigableMap();
            r64nm.addLong(13691588588l);
            mymeta.put("roaring_bit".getBytes(), Snappy.compress(DbUtils.exportBitmap(r64nm).toByteArray()));
        }

//        {
        long count = 100000000;
        ByteBuf key = Unpooled.wrappedBuffer("roaring_bit".getBytes());
        long start0 = System.nanoTime();

        {  //初始化数据
            Roaring64NavigableMap bits = bitCache.get(key);
            for (long i = 0; i < count; i++) {
                // 从缓存中得到数据，由于我们没有设置过缓存，所以需要通过CacheLoader加载缓存数据
                bits.add(i);
            }

//            bitCache.put(key,bits);
//            bitCache.refresh(key);

            log.debug(bits.getLongCardinality());

            log.debug(String.format("Roaring64NavigableMap 位计算，共put %s 个时间%s毫秒;平均单个value(%s)gut时间：%s纳秒",
                    count,
                    (System.nanoTime() - start0) / 1000 / 1000,
                    "缓存",
                    (System.nanoTime() - start0) / count));

        }


        {

            start0 = System.nanoTime();
            Roaring64NavigableMap bits = bitCache.get(key);
            log.debug(bits.getLongCardinality());


            //验证数据
            for (long i = 0; i < count; i++) {
                // 从缓存中得到数据，由于我们没有设置过缓存，所以需要通过CacheLoader加载缓存数据

                if (!bits.contains(i)) {
                    log.debug(String.format("漏网之鱼:%s", i));
                }
            }

            log.debug(String.format("漏网监测，Roaring64NavigableMap 过滤器，contains %s 个时间%s毫秒;平均单个value(%s)contains时间：%s纳秒",
                    count,
                    (System.nanoTime() - start0) / 1000 / 1000,
                    "缓存",
                    (System.nanoTime() - start0) / count));
        }


        log.debug(String.format("命中率统计信息：%s", bitCache.stats().toString()));
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

                            mymeta.put(notification.getKey().array(), Snappy.compress(json.jsonString()));

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

                    byte[] value = mymeta.get(key.array());

                    Configuration conf = Configuration.builder().jsonProvider(new GsonJsonProvider()).mappingProvider(new GsonMappingProvider()).build();
                    CacheProvider.setCache(new com.jayway.jsonpath.spi.cache.Cache() {
                        com.google.common.cache.Cache<String, JsonPath> caches = CacheBuilder.newBuilder()
                                .maximumSize(1000)
                                .expireAfterWrite(10, TimeUnit.MINUTES)
                                .build();

                        @Override
                        public JsonPath get(String key) {
//                            log.debug("get===========key:" + key);
                            return caches.getIfPresent(key);
                        }

                        @Override
                        public void put(String key, JsonPath jsonPath) {
                            log.debug("put===========key:" + key);
                            caches.put(key, jsonPath);
                        }

                    });

                    DocumentContext json = JsonPath.using(conf).parse(Snappy.uncompressString(value));

                    log.debug(String.format("获取DocumentContext Json文档，持久化到 RocksDb。\n key:%s json:%s",
                            MyUtils.ByteBuf2String(key),
                            json.jsonString()
                            )
                    );

                    return json;
                }
            });



    @Test
    public void cacheJsonPathToRocksDb() throws Exception {

        log.debug(String.format("JsonPath解析：%s", jsonCache.stats().toString()));

        ByteBuf key = Unpooled.wrappedBuffer("jsonPath".getBytes());
        {
            //数据准备
            String json1="{\"books\":[{\"category\":\"fiction\"},{\"category\":\"reference\"},{\"category\":\"fiction\"},{\"category\":\"fiction\"},{\"category\":\"reference\"},{\"category\":\"fiction\"},{\"category\":\"reference\"},{\"category\":\"reference\"},{\"category\":\"reference\"},{\"category\":\"reference\"},{\"category\":\"reference\"}]}";
            DocumentContext ext = JsonPath.parse(json1);
            mymeta.put(key.array(), Snappy.compress(ext.jsonString()));
        }

        long count = 100000;
        long start0 = System.nanoTime();
        Runtime rt = Runtime.getRuntime();
        {  //初始化数据
            DocumentContext json =  jsonCache.get(key);//尽量放在循环体外，可以节约一半的时间；
            List<String> categories=null;
            for (int i = 0; i < count + 1; i++) {
//                json.delete("$.books[?(@.category == 'reference')]");
                categories = json.read("$..category", List.class);
            }


            log.debug(String.format("\n JsonPath解析(path%s = result%s)，花费时间%s毫秒 ,单个花费%s纳秒； FreeMemory:[%sK] ..",
                    "$..category",
                    categories,
                    (System.nanoTime() - start0) / 1000 / 1000,
                    (System.nanoTime() - start0) / count,
                    rt.freeMemory() / 1024
            ));

        }

        log.debug(String.format("命中率统计信息：%s", jsonCache.stats().toString()));
    }


    /**
     * meta 缓存，大多数数据落盘RocksDb 由两次简化为一次。
     */
    static LoadingCache<ByteBuf, ByteBuf> metaCache
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
            .removalListener(new RemovalListener<ByteBuf, ByteBuf>() {
                public void onRemoval(RemovalNotification<ByteBuf, ByteBuf> notification) {
                    log.debug(String.format("因为%s的原因，%s=%s已经删除",
                            notification.getCause(),
                            notification.getKey(),
                            MyUtils.ByteBuf2String(notification.getValue())
                            ));

                    try {
                        {
                            mymeta.put(MyUtils.toByteArray(notification.getKey()), MyUtils.toByteArray(notification.getValue()));

                            log.debug(String.format("获取meta，持久化到 RocksDb。\n key:%s json:%s",
                                    notification.getKey(),
                                    notification.getKey()
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
            .build(new CacheLoader<ByteBuf, ByteBuf>() {
                @Override
                public ByteBuf load(ByteBuf key) throws Exception {
//                    log.debug(new Date());

//                    key.resetReaderIndex();

                    byte[] value = mymeta.get(MyUtils.toByteArray(key));
//                    log.debug(new String(key.array()));
//                    log.debug(new String(MyUtils.toByteArray(key)));
//
//
//                    byte[] value = mymeta.get(key.array());


                    log.debug(String.format("获取meta，key:%s json:%s",
                            key,
                            new String(value)
                            )
                    );

                    return MyUtils.concat(value);
//                    return Unpooled.copiedBuffer(value);
                }
            });



    @Test
    public void metaToRocksDb() throws Exception {

        log.debug(String.format("meta数据：%s", metaCache.stats().toString()));

        ByteBuf key = Unpooled.wrappedBuffer("metaTest".getBytes());
//        String key="metaTest";
        ByteBuf val = MyUtils.concat("metaValue".getBytes());
        {
            mymeta.put("metaTest".getBytes(), MyUtils.toByteArray(val));
        }

        long count = 100000;
        long start0 = System.nanoTime();
        Runtime rt = Runtime.getRuntime();
        log.debug("-----------------0 失效再加载一次");

        {  //初始化数据
            for (int i = 0; i < count + 1; i++) {
//                ByteBuf val0 =  metaCache.get(key);//尽量放在循环体外，可以节约一半的时间；
                ByteBuf val0 =  metaCache.get(key);//尽量放在循环体外，可以节约一半的时间；
//                log.debug(MyUtils.ByteBuf2String(val));
//                log.debug(MyUtils.ByteBuf2String(val0));
                Assert.assertEquals(val,val0);
            }
            log.debug("-----------------1");

            {
                ByteBuf val0 =  metaCache.get(key);//尽量放在循环体外，可以节约一半的时间；

                log.debug(metaCache.get(key));
                log.debug(MyUtils.ByteBuf2String(metaCache.get(key)));

                //ByteBuf 自动扩容，缓存中的内容同步改变。
//                val0.resetWriterIndex();
//                val0.resetReaderIndex();
                val0.setIndex(0,0);
                val0.writeBytes("modify By JamesMo 2019-04-10 in BeiJing".getBytes());

                log.debug(metaCache.get(key));
                log.debug(MyUtils.ByteBuf2String(metaCache.get(key)));

//                Assert.assertEquals("modify By JamesMo 2019-04-10 in BeiJing".getBytes(),metaCache.get(key));


            }


            log.debug(String.format("\n meta数据析(key%s = val%s)，花费时间%s毫秒 ,单个花费%s纳秒； FreeMemory:[%sK] ..",
                    key,
                    MyUtils.ByteBuf2String(val),
                    (System.nanoTime() - start0) / 1000 / 1000,
                    (System.nanoTime() - start0) / count,
                    rt.freeMemory() / 1024
            ));

        }

        log.debug(String.format("命中率统计信息：%s", metaCache.stats().toString()));
    }

}