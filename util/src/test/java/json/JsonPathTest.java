package json;

import com.google.common.cache.*;
import com.google.gson.JsonObject;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.spi.cache.Cache;
import com.jayway.jsonpath.spi.cache.CacheProvider;
import com.jayway.jsonpath.spi.json.GsonJsonProvider;
import com.jayway.jsonpath.spi.mapper.GsonMappingProvider;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

public class JsonPathTest {
    private static Logger log = Logger.getLogger(JsonPathTest.class);

    /**
     *
     */
    @Test
    public void testgson() {

        String json = "{\"objs\" : [{\"obj\" : 19740423},{\"obj2\" : 19740423}],\"id\":1}";
        DocumentContext ext = JsonPath.parse(json);


        JsonPath p = JsonPath.compile("$.objs[0].obj");

        int a = ext.read(p);
        System.out.println(a);

        ext.set(p, 141145561197333L);

        String author = ext.jsonString();
        System.err.println(author);


        String json1 = "{ \"books\": [ " + "{ \"category\": \"fiction\" }, " + "{ \"category\": \"reference\" }, " + "{ \"category\": \"fiction\" }, " + "{ \"category\": \"fiction\" }, " + "{ \"category\": \"reference\" }, " + "{ \"category\": \"fiction\" }, " + "{ \"category\": \"reference\" }, " + "{ \"category\": \"reference\" }, " + "{ \"category\": \"reference\" }, " + "{ \"category\": \"reference\" }, " + "{ \"category\": \"reference\" } ]  }";
        Configuration conf = Configuration.builder().jsonProvider(new GsonJsonProvider()).mappingProvider(new GsonMappingProvider()).build();


        //google 的cache 或者定义存储到rocksdb
        CacheProvider.setCache(new Cache() {

//        //Not thread safe simple cache
//        private Map<String, JsonPath> map = new HashMap<String, JsonPath>();
//
//        @Override
//        public JsonPath get(String key) {
//            return map.get(key);
//        }
//
//        @Override
//        public void put(String key, JsonPath jsonPath) {
//            map.put(key, jsonPath);
//        }


            com.google.common.cache.Cache<String, JsonPath> caches = CacheBuilder.newBuilder()
                    .maximumSize(1000)
                    .expireAfterWrite(10, TimeUnit.MINUTES)
                    .build();

            @Override
            public JsonPath get(String key) {
                System.out.println("get===========key:" + key);

                return caches.getIfPresent(key);
            }

            @Override
            public void put(String key, JsonPath jsonPath) {
                System.out.println("put===========key:" + key);

                caches.put(key, jsonPath);
            }


        });


        DocumentContext context = JsonPath.using(conf).parse(json1);
        context.delete("$.books[?(@.category == 'reference')]");
        List<String> categories = context.read("$..category", List.class);
        System.out.println(categories);
        assertTrue(categories.contains("fiction"));


    }

    /**
     * 看看到在20此循环中命中次数是17次，未命中3次，这是因为我们设定缓存的过期时间是写入后的8秒，所以20秒内会失效两次，
     * 另外第一次获取时缓存中也是没有值的，所以才会未命中3次，其他则命中。
     * <p>
     * guava的内存缓存非常强大，可以设置各种选项，而且很轻量，使用方便。另外还提供了下面一些方法，来方便各种需要：
     * <p>
     * ImmutableMap<K, V> getAllPresent(Iterable<?> keys) 一次获得多个键的缓存值
     * put和putAll方法向缓存中添加一个或者多个缓存项
     * invalidate 和 invalidateAll方法从缓存中移除缓存项
     * asMap()方法获得缓存数据的ConcurrentMap<K, V>快照
     * cleanUp()清空缓存
     * refresh(Key) 刷新缓存，即重新取缓存数据，更新缓存
     */
    @Test
    public void cacheTest() throws ExecutionException, InterruptedException {
        //缓存接口这里是LoadingCache，LoadingCache在缓存项不存在时可以自动加载缓存
        LoadingCache<Integer, JsonObject> cache
                //CacheBuilder的构造函数是私有的，只能通过其静态方法newBuilder()来获得CacheBuilder的实例
                = CacheBuilder.newBuilder()
                //设置并发级别为8，并发级别是指可以同时写缓存的线程数
                .concurrencyLevel(8)
                //设置写缓存后8秒钟过期
                .expireAfterWrite(8, TimeUnit.SECONDS)
                //设置缓存容器的初始容量为10
                .initialCapacity(10)
                //设置缓存最大容量为100，超过100之后就会按照LRU最近虽少使用算法来移除缓存项
                .maximumSize(100)
                //设置要统计缓存的命中率
                .recordStats()
                //设置缓存的移除通知
                .removalListener(new RemovalListener<Object, Object>() {
                    @Override
                    public void onRemoval(RemovalNotification<Object, Object> notification) {
                        System.out.println(notification.getKey() + " was removed, cause is " + notification.getCause());
                    }
                })
                //build方法中可以指定CacheLoader，在缓存不存在时通过CacheLoader的实现自动加载缓存
                .build(
                        new CacheLoader<Integer, JsonObject>() {
                            @Override
                            public JsonObject load(Integer key) throws Exception {
                                System.out.println("load student " + key);
                                JsonObject man = new JsonObject();
                                man.addProperty("id", key);
                                man.addProperty("name ", key);
                                return man;
                            }
                        }
                );

//        cache.put();
//        cache.refresh();
//        cache.invalidate();
//        cache.invalidateAll();
//        cache.cleanUp();
//        cache.getAllPresent()


        for (int i = 0; i < 20; i++) {
            //从缓存中得到数据，由于我们没有设置过缓存，所以需要通过CacheLoader加载缓存数据
            JsonObject man = cache.get(1);
            System.out.println(man);
            //休眠1秒
            TimeUnit.SECONDS.sleep(1);
        }

        System.out.println("cache stats:");
        //最后打印缓存的命中率等 情况
        System.out.println(cache.stats().toString());

        com.google.common.cache.Cache<String, String> cache111 = CacheBuilder.newBuilder().maximumSize(1000).build();


    }

}
