package redis.server.netty.rocksdb;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import redis.netty4.IntegerReply;
import redis.netty4.MultiBulkReply;
import redis.server.netty.RedisException;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;


/**
 * Test data Hash Meta.
 */
public class ZSetMetaTest {
  private static Logger log = Logger.getLogger(ZSetMetaTest.class);


  /**
   * Hash数据集测试
   *
   * @throws RedisException
   */

  @Test
  public  void testSet() throws RedisException, InterruptedException {
//
    log.debug("|".getBytes().length);
    Random random = new Random();
    random.ints(6).limit(3).sorted().forEach(System.out::println);

    List<String> strings = Arrays.asList("abc", "", "bc", "efg", "abcd", "", "jkl");
    long count = strings.parallelStream().filter(string -> string.compareTo("e") > 0).count();
    strings.stream().filter(string -> string.compareTo("e") > 0).count();
    log.debug(count);
    List<Integer> numbers = Arrays.asList(3, 2, 2, 3, 7, 3, 5);
// 获取对应的平方数
    List<Integer> squaresList = numbers.stream().map(i -> i * i).distinct().collect(Collectors.toList());
    Set<Integer> squaresList1 = numbers.stream().map(i -> i * i).collect(Collectors.toSet());
    log.debug(squaresList);
    log.debug(squaresList1);

    /////////

    ZSetMeta setMeta = ZSetMeta.getInstance(RocksdbRedis.mydata, "redis".getBytes());
    setMeta.genMetaKey("ZSet".getBytes()).clearMetaDataNodeData(setMeta.getKey0());
    Assert.assertEquals(setMeta.zcard().data().intValue(), 0);

    setMeta.genMetaKey("ZSet".getBytes()).zadd("10".getBytes(), "f1".getBytes(),
                    "20".getBytes(), "f2".getBytes(),
                    "30".getBytes(), "f3".getBytes());

    Assert.assertEquals(setMeta.zcard().data().intValue(), 3);

    Assert.assertEquals(setMeta.zcount("1".getBytes(), "40".getBytes()).data().intValue(), 3);
    Assert.assertEquals(setMeta.zcount("15".getBytes(), "40".getBytes()).data().intValue(), 2);
    Assert.assertEquals(setMeta.zlexcount("f2".getBytes(), "f3".getBytes()).data().intValue(), 2);

    Assert.assertArrayEquals(setMeta.zscore("f1".getBytes()).dataByte(), "10".getBytes());

    String[] zrangestr = {"f2", "20", "f3", "30"};
    String[] zrevrangestr = {"f3", "30", "f2", "20"};
    String[] zrangebylexstr = {"f2", "f3"};
    //安索引
    MultiBulkReply zrange = setMeta.zrange("1".getBytes(), "2".getBytes(), "1".getBytes());
    //索引倒序
    MultiBulkReply zrevrange = setMeta.zrevrange("1".getBytes(), "2".getBytes(), "1".getBytes());
    //按分数
    MultiBulkReply zrangebyscore = setMeta.zrangebyscore("19".getBytes(), "31".getBytes(), "WITHSCORES".getBytes());
    MultiBulkReply zrevrangebyscore = setMeta.zrevrangebyscore("19".getBytes(), "31".getBytes(), "WITHSCORES".getBytes());
    //按字母
    MultiBulkReply zrangebylex = setMeta.zrangebylex("f2".getBytes(), "f3".getBytes());

    Assert.assertEquals(Arrays.asList(zrangestr).toString(), zrange.asStringList(Charset.defaultCharset()).toString());
    Assert.assertEquals(Arrays.asList(zrangestr).toString(), zrangebyscore.asStringList(Charset.defaultCharset()).toString());
//        Assert.assertEquals(Arrays.asList(zrevrangestr).toString(),zrevrangebyscore.asStringList(Charset.defaultCharset()).toString());
    Assert.assertEquals(Arrays.asList(zrangebylexstr).toString(), zrangebylex.asStringList(Charset.defaultCharset()).toString());
    Assert.assertEquals(Arrays.asList(zrevrangestr).toString(), zrevrange.asStringList(Charset.defaultCharset()).toString());

    log.debug(zrange.asStringList(Charset.defaultCharset()));
    log.debug(zrangebyscore.asStringList(Charset.defaultCharset()));
    log.debug(zrevrangebyscore.asStringList(Charset.defaultCharset()));
    log.debug(zrangebylex.asStringList(Charset.defaultCharset()));
    log.debug(zrevrange.asStringList(Charset.defaultCharset()));


    IntegerReply zrank = (IntegerReply) setMeta.zrank("f1".getBytes());
    IntegerReply zrevrank = (IntegerReply) setMeta.zrevrank("f1".getBytes());
    Assert.assertEquals(zrank.data().intValue(), 0);
    Assert.assertEquals(zrevrank.data().intValue(), 2);

    Assert.assertArrayEquals(setMeta.zincrby("-1".getBytes(), "f1".getBytes()).dataByte(), "9".getBytes());


    setMeta.zrem("f1".getBytes());
    Assert.assertNull(setMeta.zscore("f1".getBytes()).data());
    setMeta.zadd("10".getBytes(), "f1".getBytes());
    Assert.assertNotNull(setMeta.zscore("f1".getBytes()).data());

    log.debug(setMeta.zremrangebyscore("12".getBytes(), "22".getBytes()));
    Assert.assertNull(setMeta.zscore("f2".getBytes()).data());
    setMeta.zadd("20".getBytes(), "f2".getBytes());
    Assert.assertNotNull(setMeta.zscore("f2".getBytes()).data());


    log.debug(setMeta.zremrangebyslex("f1".getBytes(), "f2".getBytes()));
    Assert.assertNull(setMeta.zscore("f1".getBytes()).data());
//        Assert.assertNull(setMeta.zscore("f2".getBytes()).data());
    setMeta.zadd("10".getBytes(), "f1".getBytes(), "20".getBytes(), "f2".getBytes());


    log.debug(setMeta.zremrangebyrank("1".getBytes(), "2".getBytes()));
    Assert.assertNull(setMeta.zscore("f3".getBytes()).data());


    log.debug("Over ... ...");

  }
}
