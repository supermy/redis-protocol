package redis.server.netty;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;


/**
 * Test data Hash Node.
 */
public class ZSetRankNodeTest {
  private static Logger log = Logger.getLogger(ZSetRankNodeTest.class);


  @Test
  public  void testZSetRankNode() throws RedisException, InterruptedException {
    ZSetRankNode meta = ZSetRankNode.getInstance(RocksdbRedis.mydata, "redis".getBytes());

    meta.genKey("ZSetTest".getBytes(),"8".getBytes(), "f1".getBytes()).zadd();
    boolean exists = meta.genKey("ZSetTest".getBytes(),"8".getBytes(), "f1".getBytes()).exists();
    System.out.println(exists);
    Assert.assertTrue(exists);

//        ZSetRankNode zscore = meta.genKey("ZSetTest".getBytes(),"8".getBytes(), "f1".getBytes()).zrank();
//        log.debug(new String(zscore.getVal()));
//        log.debug(new String(zscore.getVal0()));
//        Assert.assertArrayEquals(zscore.getVal0(),"8".getBytes());

    meta.genKey("ZSetTest".getBytes(),"8".getBytes(), "f1".getBytes()).zrem();
    exists=meta.genKey("ZSetTest".getBytes(),"8".getBytes(), "f1".getBytes()).exists();
    System.out.println(exists);
    Assert.assertFalse(exists);

  }
}
