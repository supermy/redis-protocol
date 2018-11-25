package redis.server.netty.rocksdb;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import redis.server.netty.RedisException;


/**
 * Test data Hash Node.
 */
public class ZSetScoreNodeTest {
  private static Logger log = Logger.getLogger(ZSetScoreNodeTest.class);


  @Test
  public  void testZSetScoreNode() throws RedisException, InterruptedException {


    ZSetScoreNode meta = ZSetScoreNode.getInstance(RocksdbRedis.mydata, "redis".getBytes());

    meta.genKey("ZSetTest".getBytes(), "f1".getBytes()).zadd("8".getBytes());
    boolean exists = meta.genKey("ZSetTest".getBytes(), "f1".getBytes()).exists();
    System.out.println(exists);
    Assert.assertTrue(exists);
    ZSetScoreNode zscore = meta.genKey("ZSetTest".getBytes(), "f1".getBytes()).zscore();
    log.debug(new String(zscore.getVal()));
    log.debug(new String(zscore.getVal0()));
    Assert.assertArrayEquals(zscore.getVal0(),"8".getBytes());

    meta.genKey("ZSetTest".getBytes(), "f1".getBytes()).zrem();
    exists=meta.genKey("ZSetTest".getBytes(), "f1".getBytes()).exists();
    System.out.println(exists);
    Assert.assertFalse(exists);


  }
}
