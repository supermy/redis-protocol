package redis.server.netty.rocksdb;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import redis.server.netty.RedisException;
import redis.server.netty.utis.DataType;


/**
 * Test data Hash Node.
 */
public class ListNodeTest {
  private static Logger log = Logger.getLogger(ListNodeTest.class);


  @Test
  public  void testListNode() throws RedisException, InterruptedException {


//        ListNode meta =  ListNode.getInstance(RocksdbRedis.mydata, "ListTest".getBytes(), 0, "value".getBytes(), -1, -1);
    ListNode meta = ListNode.getInstance(RocksdbRedis.mydata, "redis".getBytes());
    meta.genKey("ListTest".getBytes(), 0).put("value".getBytes(), -1, -1);

    Assert.assertArrayEquals(meta.getKey0(), "ListTest".getBytes());
    Assert.assertEquals(meta.getSeq(), 0);
    Assert.assertEquals(meta.getSeq0(), 0);

    Assert.assertEquals(meta.getTtl(), -1);
    Assert.assertEquals(meta.getType(), DataType.VAL_LIST_ELEMENT);
    Assert.assertEquals(meta.getSize(), "value".getBytes().length);

    Assert.assertEquals(meta.getNseq(), -1);
    Assert.assertEquals(meta.getPseq(), -1);

    Assert.assertArrayEquals(meta.getVal0(), "value".getBytes());

    meta.info();

//        log.debug(new String(meta.getKey0()));
//        log.debug(String.format("seq: %d", meta.getSeq()));

//        meta.setKey0("abc".getBytes());
//        log.debug(new String(meta.getKey0()));
    meta.setSeq(12);
    meta.setNseq(3);
    meta.setPseq(4);

    Assert.assertEquals(meta.getSeq(), 12);
    Assert.assertEquals(meta.getSeq0(), 12);

    Assert.assertEquals(meta.getNseq(), 3);
    Assert.assertEquals(meta.getPseq(), 4);

    Assert.assertArrayEquals(meta.getKey0(), "ListTest".getBytes());

    meta.info();

    meta.flush();


    ListNode meta1 = ListNode.getInstance(RocksdbRedis.mydata, "redis".getBytes());

    meta1.genKey("ListTest".getBytes(), 12).get();

    meta1.info();

    meta1.genKey("ListTest".getBytes(), 0).get();

    meta1.info();

  }
}
