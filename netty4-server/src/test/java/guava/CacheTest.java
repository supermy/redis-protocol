package guava;

import com.google.common.cache.*;
import org.junit.Test;

import java.util.HashMap;
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
public class CacheTest {

    // 缓存接口这里是LoadingCache，LoadingCache在缓存项不存在时可以自动加载缓存
    static LoadingCache<Integer, HashMap> studentCache
            // CacheBuilder的构造函数是私有的，只能通过其静态方法newBuilder()来获得CacheBuilder的实例
            = CacheBuilder.newBuilder()
            // 设置并发级别为8，并发级别是指可以同时写缓存的线程数
            .concurrencyLevel(8)
            // 设置写缓存后8秒钟过期
            .expireAfterWrite(18, TimeUnit.SECONDS)
            // 设置缓存容器的初始容量为10
            .initialCapacity(2)
            // 设置缓存最大容量为100，超过100之后就会按照LRU最近虽少使用算法来移除缓存项
            .maximumSize(2)
            // 设置要统计缓存的命中率
            .recordStats()
            // 设置缓存的移除通知
            .removalListener(new RemovalListener<Object, Object>() {
                public void onRemoval(RemovalNotification<Object, Object> notification) {
                    System.out.println(
                            notification.getKey() + " was removed, cause is " + notification.getCause());
                }
            })

            // build方法中可以指定CacheLoader，在缓存不存在时通过CacheLoader的实现自动加载缓存
            .build(new CacheLoader<Integer, HashMap>() {
                @Override
                public HashMap load(Integer key) throws Exception {
                    System.out.println("load student " + key);
                    HashMap student = new HashMap();
                    student.put("key", key);
                    student.put("name ", key+"auto gen data");
                    return student;
                }
            });


    @Test
    public  void cache() throws Exception {

        for (int i = 0; i < 20; i++) {
            // 从缓存中得到数据，由于我们没有设置过缓存，所以需要通过CacheLoader加载缓存数据
            HashMap student = studentCache.get(i);
            System.out.println(student);
            // 休眠1秒
            TimeUnit.SECONDS.sleep(1);
        }

        System.out.println("cache stats:");
        // 最后打印缓存的命中率等 情况
        System.out.println(studentCache.stats().toString());

//        cahceBuilder.invalidateAll();
    }




}