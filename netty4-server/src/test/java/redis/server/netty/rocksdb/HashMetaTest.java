package redis.server.netty.rocksdb;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import redis.server.netty.RedisException;

import java.util.Arrays;



/**
 * Test data Hash Meta.
 */
public class HashMetaTest {
  private static Logger log = Logger.getLogger(HashMetaTest.class);


  @Test
  public  void testHash() throws RedisException, InterruptedException {

    HashMeta meta9 = HashMeta.getInstance(RocksdbRedis.mydata, "redis".getBytes());
    //测试删除
    meta9.genMetaKey("HashUpdate".getBytes()).deleteRange(meta9.getKey0());
//        log.debug("hkeys0:"+meta9.members());


//        log.debug(meta9.hget("f1".getBytes()));
    Assert.assertNull(meta9.hget("f1".getBytes()).asUTF8String());

    meta9.genMetaKey("HashUpdate".getBytes()).hset("f1".getBytes(), "v1".getBytes());
//        log.debug(meta9.hget("f1".getBytes()));
    Assert.assertEquals("v1", meta9.hget("f1".getBytes()).asUTF8String());

    Thread.sleep(500);

//        log.debug("cnt:"+meta9.getCount());
    Assert.assertEquals(1, meta9.getMeta().getCount());


    meta9.genMetaKey("HashUpdate".getBytes()).hset("f2".getBytes(), "v2".getBytes());

    Thread.sleep(500);

//        log.debug("val:"+meta9.getVal0());
    Assert.assertEquals(2, meta9.getMeta().getCount());

//        log.debug("hkeys9:"+meta9.members());
//        log.debug("hvals:"+meta9.hvals());
//        log.debug("hgetall:"+meta9.hgetall());
//        log.debug("hmget:"+meta9.hmget("f1".getBytes(),"f2".getBytes()));


    //引入

    HashMeta meta2 = HashMeta.getInstance(RocksdbRedis.mydata, "TEST1".getBytes());
    meta2.genMetaKey("BATCH".getBytes()).deleteRange(meta2.getKey0());


    byte[] f1 = "f1".getBytes();
    byte[] f2 = "f2".getBytes();

    byte[] v1 = "v1".getBytes();
    byte[] v2 = "v2".getBytes();

    byte[] f3 = "f3".getBytes();
    byte[] f4 = "f4".getBytes();

    byte[] v3 = "v3".getBytes();
    byte[] v4 = "v4".getBytes();

//        log.debug("hkeys0:"+meta2.genMetaKey("BATCH".getBytes()).members());
//        log.debug("hkeys1:"+meta2.members());
//        log.debug("hlens1:"+meta2.hlen().data().longValue());

    meta2.genMetaKey("BATCH".getBytes()).hset1(f1, v1).hset1(f2, v2);

    Thread.sleep(500);

//        log.debug("hkeys2:"+meta2.members());
//        log.debug("hlens2:"+meta2.hlen().data().longValue());

    Assert.assertArrayEquals(meta2.hget(f1).data().array(), v1);
    Assert.assertArrayEquals(meta2.hget(f2).data().array(), v2);
    Assert.assertEquals(meta2.hlen().data().longValue(), 2);

    meta2.hdel(f2);
    Assert.assertNull(meta2.hget(f2).data());
    Assert.assertEquals(meta2.hlen().data().longValue(), 1);


    meta2.hmset(f3, v3, f4, v4);
    Assert.assertEquals(meta2.hlen().data().longValue(), 3);

    String[] strings3 = {"v1", "v3"};
    Assert.assertEquals(meta2.hmget(f1, f3).toString(), Arrays.asList(strings3).toString());

    String[] strings4 = {"f1", "v1", "f3", "v3", "f4", "v4"};
    Assert.assertEquals(meta2.hgetall().toString(), Arrays.asList(strings4).toString());

    Assert.assertTrue(meta2.hexists("f1".getBytes()).data().intValue() == 1);

    Assert.assertTrue(meta2.hincrby("f8".getBytes(), "1".getBytes()).data().intValue() == 1);

    Assert.assertArrayEquals(meta2.hget("f8".getBytes()).data().array(), "1".getBytes());

    String[] strings5 = {"v1", "v3", "v4", "1"};

    Assert.assertEquals(meta2.hvals().toString(), Arrays.asList(strings5).toString());

    log.debug("Over ... ...");

  }
}
