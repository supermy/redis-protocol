package redis.server.netty.rocksdb;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import redis.server.netty.RedisException;


/**
 * Test data Hash Node.
 */
public class HashNodeTest {
  private static Logger log = Logger.getLogger(HashNodeTest.class);


  @Test
  public  void testHashNode() throws RedisException, InterruptedException {

    HashNode meta = HashNode.getInstance(RocksdbRedis.mydata, "redis".getBytes());
    meta.genKey1("HashTest".getBytes(), "f1".getBytes()).hset("value".getBytes());

    byte[] val1 = meta.genKey1("HashTest".getBytes(), "f1".getBytes()).hget().getVal0();
    log.debug(":::::" + new String(val1));
    Assert.assertArrayEquals(val1, "value".getBytes());
    meta.info();

    Assert.assertEquals(meta.getSize(), 5);
    Assert.assertArrayEquals(meta.getField0(), "f1".getBytes());
    Assert.assertArrayEquals(meta.getKey0(), "HashTest".getBytes());
    Assert.assertArrayEquals(meta.getVal0(), "value".getBytes());

    meta.setField0("f2".getBytes());
    meta.setVal("v1".getBytes(), -1);

    Assert.assertArrayEquals(meta.getField0(), "f2".getBytes());
    Assert.assertArrayEquals(meta.getVal0(), "v1".getBytes());

    meta.info();


    meta.genKey1("abc".getBytes(), "f2".getBytes()).hset("v2".getBytes());


    meta.info();

    Assert.assertArrayEquals(meta.getKey0(), "abc".getBytes());
    Assert.assertArrayEquals(meta.getField0(), "f2".getBytes());
    Assert.assertArrayEquals(meta.getVal0(), "v2".getBytes());

    byte[] val0 = meta.genKey1("abc".getBytes(), "f2".getBytes()).hget().getVal0();
    log.debug(":::::" + new String(val0));
    Assert.assertArrayEquals(val0, "v2".getBytes());

  }
}
