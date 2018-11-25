package redis.server.netty.rocksdb;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import redis.server.netty.RedisException;

import java.util.Arrays;

import static redis.netty4.StatusReply.OK;


/**
 * Test data Hash Meta.
 */
public class ListLinkMetaTest {
  private static Logger log = Logger.getLogger(ListLinkMetaTest.class);


  @Test
  /**
   * 左侧插入数据集测试
   *
   * @throws RedisException
   */
  public  void testListL() throws RedisException {

    ListLinkMeta metaLPush = ListLinkMeta.getInstance(RocksdbRedis.mydata, "redis".getBytes());
    metaLPush.genMetaKey("LPUSH".getBytes()).deleteRange(metaLPush.getKey0());

    byte[][] lpusharray = {"XXX".getBytes(), "YYY".getBytes(), "ZZZ".getBytes(), "111".getBytes(), "222".getBytes(), "333".getBytes()};
    Assert.assertEquals(metaLPush.lpush(lpusharray).data().intValue(), 6);

    Assert.assertEquals(metaLPush.getCount(), 6);
    Assert.assertEquals(metaLPush.getSseq(), 5);
    Assert.assertEquals(metaLPush.getEseq(), 0);
    Assert.assertEquals(metaLPush.getCseq(), 5);
    Assert.assertArrayEquals("LPUSH".getBytes(), metaLPush.getKey0());


    log.debug("========================2 !!!");

    ListLinkNode n3 = ListLinkNode.getInstance(RocksdbRedis.mydata, "redis".getBytes());
    n3.genKey("LPUSH".getBytes(), 2).get();
    n3.info();

    Assert.assertEquals(n3.getNseq(), 1);
    Assert.assertEquals(n3.getPseq(), 3);
    Assert.assertEquals(n3.getSeq(), 2);
    Assert.assertEquals(n3.getSize(), 3);

    Assert.assertArrayEquals(n3.getVal0(), "ZZZ".getBytes());

    log.debug("========================1 !!!");

    ListLinkNode n2 = n3.genKey("LPUSH".getBytes(), 1).get();

    Assert.assertEquals(n2.getNseq(), 0);
    Assert.assertEquals(n2.getPseq(), 2);
    Assert.assertEquals(n2.getSeq(), 1);
    Assert.assertEquals(n2.getSize(), 3);

    Assert.assertArrayEquals(n2.getVal0(), "YYY".getBytes());
    log.debug("========================0 !!!");


//        ListNode n1 = new ListNode(RocksdbRedis.mydata, "LPUSH".getBytes(), 0);
    ListLinkNode n1 = n3.genKey("LPUSH".getBytes(), 0).get();

    Assert.assertEquals(n1.getNseq(), -1);
    Assert.assertEquals(n1.getPseq(), 1);
    Assert.assertEquals(n1.getSeq(), 0);
    Assert.assertEquals(n1.getSize(), 3);
    Assert.assertArrayEquals(n1.getVal0(), "XXX".getBytes());

    log.debug("========================LPOP !!!");

    Assert.assertArrayEquals(metaLPush.lpop().data().array(), "333".getBytes());

    Assert.assertEquals(metaLPush.getCount(), 5);
    Assert.assertEquals(metaLPush.getSseq(), 4);
    Assert.assertEquals(metaLPush.getEseq(), 0);
    Assert.assertEquals(metaLPush.getCseq(), 5);

    Assert.assertEquals(metaLPush.llen().data().intValue(), 5);

    log.debug("========================1 LINDEX !!!");

    Assert.assertArrayEquals(metaLPush.lindex("1".getBytes()).data().array(), "111".getBytes());

    Assert.assertEquals(metaLPush.lset("1".getBytes(), "Modify".getBytes()), OK);


    Assert.assertArrayEquals(metaLPush.lindex("1".getBytes()).data().array(), "Modify".getBytes());

    Assert.assertEquals(metaLPush.lrange("0".getBytes(), "-1".getBytes()).data().length, 5);

    log.debug("========================2 LRANGE !!!");

    n3.genKey("LPUSH".getBytes(), 0).get().info();
    n3.genKey("LPUSH".getBytes(), 1).get().info();
    n3.genKey("LPUSH".getBytes(), 2).get().info();
    n3.genKey("LPUSH".getBytes(), 3).get().info();
    n3.genKey("LPUSH".getBytes(), 4).get().info();

    log.debug("========================2 LRANGE !!!");

    String[] strings = {"222", "Modify", "ZZZ"};
    Assert.assertEquals(metaLPush.lrange("0".getBytes(), "2".getBytes()).toString(), Arrays.asList(strings).toString());

//        log.debug("*********count 111:"+metaLPush.getCount());
    Assert.assertEquals(metaLPush.lrem("1".getBytes(), "Modify".getBytes()).data().intValue(), 1);


//        n3.genKey("LPUSH".getBytes(), 0).get().info();
//        n3.genKey("LPUSH".getBytes(), 1).get().info();
//        n3.genKey("LPUSH".getBytes(), 2).get().info();
//        n3.genKey("LPUSH".getBytes(), 4).get().info();

//        log.debug("**********count 222:"+metaLPush.getCount());

    Assert.assertEquals(metaLPush.lrange("0".getBytes(), "-1".getBytes()).data().length, 4);

    log.debug("--------------------------- !!!");


    String[] strings1 = {"222", "ZZZ", "YYY", "XXX"};

    metaLPush.info();

    Assert.assertEquals(metaLPush.lrange("0".getBytes(), "-1".getBytes()).toString(), Arrays.asList(strings1).toString());

    Assert.assertEquals(metaLPush.ltrim("1".getBytes(), "2".getBytes()).toString(), "OK");
    Assert.assertEquals(metaLPush.lrange("0".getBytes(), "-1".getBytes()).data().length, 2);
    String[] strings2 = {"ZZZ", "YYY"};
    Assert.assertEquals(metaLPush.lrange("0".getBytes(), "-1".getBytes()).toString(), Arrays.asList(strings2).toString());

    Assert.assertEquals(metaLPush.linsert("BEFORE".getBytes(), "YYY".getBytes(), "OOO".getBytes()).data().longValue(), metaLPush.getCount());
    String[] strings3 = {"ZZZ", "OOO", "YYY"};
    Assert.assertEquals(metaLPush.lrange("0".getBytes(), "-1".getBytes()).toString(), Arrays.asList(strings3).toString());
  }


  @Test
  public  void testListR() throws RedisException {
    //测试删除
    ListLinkMeta metaRPush = ListLinkMeta.getInstance(RocksdbRedis.mydata, "redis".getBytes());
    metaRPush.genMetaKey("ListRight".getBytes()).deleteRange(metaRPush.getKey0());

    //测试 rpush
    byte[][] rpusharray = {"XXX".getBytes(), "YYY".getBytes(), "ZZZ".getBytes(), "111".getBytes(), "222".getBytes(), "333".getBytes()};

    Assert.assertEquals(metaRPush.rpush(rpusharray).data().intValue(), 6);

    Assert.assertEquals(metaRPush.getCount(), 6);
    Assert.assertEquals(metaRPush.getSseq(), 0);
    Assert.assertEquals(metaRPush.getEseq(), 5);
    Assert.assertEquals(metaRPush.getCseq(), 5);

    metaRPush.info();


    String[] strings3 = {"XXX", "YYY", "ZZZ"};
    Assert.assertEquals(metaRPush.lrange("0".getBytes(), "2".getBytes()).toString(), Arrays.asList(strings3).toString());


    log.debug("========================2 !!!");

    ListLinkNode n3 = ListLinkNode.getInstance(RocksdbRedis.mydata, "redis".getBytes());

    n3.genKey("ListRight".getBytes(), 0).get().info();
//        n3.genKey("ListRight".getBytes(), 1).get().info();
//        n3.genKey("ListRight".getBytes(), 2).get().info();
//        n3.genKey("ListRight".getBytes(), 3).get().info();
//        n3.genKey("ListRight".getBytes(), 4).get().info();
//        n3.genKey("ListRight".getBytes(), 5).get().info();


    n3.genKey("ListRight".getBytes(), 2).get();

    Assert.assertEquals(n3.getNseq(), 3);
    Assert.assertEquals(n3.getPseq(), 1);
    Assert.assertEquals(n3.getSeq(), 2);
    Assert.assertEquals(n3.getSize(), 3);

    Assert.assertArrayEquals(n3.getVal0(), "ZZZ".getBytes());
    log.debug("========================1 !!!");

    ListLinkNode n2 = ListLinkNode.getInstance(RocksdbRedis.mydata, "redis".getBytes());
    n2.genKey("ListRight".getBytes(), 1).get();

    Assert.assertEquals(n2.getNseq(), 2);
    Assert.assertEquals(n2.getPseq(), 0);
    Assert.assertEquals(n2.getSeq(), 1);
    Assert.assertEquals(n2.getSize(), 3);

    Assert.assertArrayEquals(n2.getVal0(), "YYY".getBytes());
    log.debug("========================0 !!!");


//        ListNode n1 = new ListNode(RocksdbRedis.mydata, "ListRight".getBytes(), 0);

    ListLinkNode n1 = ListLinkNode.getInstance(RocksdbRedis.mydata, "redis".getBytes());
    n1.genKey("ListRight".getBytes(), 0).get();

    Assert.assertEquals(n1.getNseq(), 1);
    Assert.assertEquals(n1.getPseq(), -1);
    Assert.assertEquals(n1.getSeq(), 0);
    Assert.assertEquals(n1.getSize(), 3);
    Assert.assertArrayEquals(n1.getVal0(), "XXX".getBytes());


    Assert.assertArrayEquals(metaRPush.rpop().data().array(), "333".getBytes());

    Assert.assertEquals(metaRPush.getCount(), 5);
    Assert.assertEquals(metaRPush.getSseq(), 0);
    Assert.assertEquals(metaRPush.getEseq(), 4);
    Assert.assertEquals(metaRPush.getCseq(), 5);

    Assert.assertEquals(metaRPush.llen().data().intValue(), 5);
  }
}
