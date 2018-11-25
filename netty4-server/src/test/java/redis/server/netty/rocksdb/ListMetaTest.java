package redis.server.netty.rocksdb;

import io.netty.buffer.ByteBuf;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import redis.server.netty.RedisException;

import java.util.Arrays;

import static redis.netty4.StatusReply.OK;


/**
 * Test data Hash Meta.
 */
public class ListMetaTest {
  private static Logger log = Logger.getLogger(ListMetaTest.class);


  @Test
  public  void testListR() throws RedisException {
    //测试删除
    ListMeta metaRPush = ListMeta.getInstance(RocksdbRedis.mydata, "redis".getBytes());
    metaRPush.genMetaKey("ListRight".getBytes()).deleteRange(metaRPush.getKey0());

    //测试 rpush
    byte[][] rpusharray = {"XXX".getBytes(), "YYY".getBytes(), "ZZZ".getBytes(), "111".getBytes(), "222".getBytes(), "333".getBytes()};

    Assert.assertEquals(metaRPush.rpush(rpusharray).data().intValue(), 6);

    Assert.assertEquals(metaRPush.getCount(), 6);
    Assert.assertEquals(metaRPush.getSseq(), metaRPush.getCseq());

    metaRPush.info();


    String[] strings3 = {"XXX", "YYY", "ZZZ"};
    Assert.assertEquals(metaRPush.lrange("0".getBytes(), "2".getBytes()).toString(), Arrays.asList(strings3).toString());


    log.debug("========================2 !!!");

    ListNode n3 = ListNode.getInstance(RocksdbRedis.mydata, "redis".getBytes());

    n3.genKey("ListRight".getBytes(), metaRPush.getCseq()+0).get().info();
    n3.genKey("ListRight".getBytes(), metaRPush.getCseq()+1).get().info();
    n3.genKey("ListRight".getBytes(), metaRPush.getCseq()+2).get().info();
    n3.genKey("ListRight".getBytes(), metaRPush.getCseq()+3).get().info();
    n3.genKey("ListRight".getBytes(), metaRPush.getCseq()+4).get().info();
    n3.genKey("ListRight".getBytes(), metaRPush.getCseq()+5).get().info();


    n3.genKey("ListRight".getBytes(), metaRPush.getCseq()+2).get();

    Assert.assertEquals(n3.getSize(), 3);

    Assert.assertArrayEquals(n3.getVal0(), "ZZZ".getBytes());
    log.debug("========================1 !!!");

    ListNode n2 = ListNode.getInstance(RocksdbRedis.mydata, "redis".getBytes());
    n2.genKey("ListRight".getBytes(), metaRPush.getCseq()+1).get();

    Assert.assertEquals(n2.getSize(), 3);

    Assert.assertArrayEquals(n2.getVal0(), "YYY".getBytes());
    log.debug("========================0 !!!");



    ListNode n1 = ListNode.getInstance(RocksdbRedis.mydata, "redis".getBytes());
    n1.genKey("ListRight".getBytes(), metaRPush.getCseq()+0).get();

    Assert.assertEquals(n1.getSize(), 3);
    Assert.assertArrayEquals(n1.getVal0(), "XXX".getBytes());


    Assert.assertArrayEquals(metaRPush.rpop().data().array(), "333".getBytes());

    Assert.assertEquals(metaRPush.getCount(), 5);

    Assert.assertEquals(metaRPush.llen().data().intValue(), 5);
  }

  /**
   * 左侧插入数据集测试
   *
   * @throws RedisException
   */
  @Test
  public  void testListL() throws RedisException {

    ListMeta metaLPush = ListMeta.getInstance(RocksdbRedis.mydata, "redis".getBytes());
    metaLPush.genMetaKey("LPUSH".getBytes()).deleteRange(metaLPush.getKey0());

    byte[][] lpusharray = {"XXX".getBytes(), "YYY".getBytes(), "ZZZ".getBytes(), "111".getBytes(), "222".getBytes(), "333".getBytes()};
    Assert.assertEquals(metaLPush.lpush(lpusharray).data().intValue(), 6);

    Assert.assertEquals(metaLPush.getCount(), 6);
    log.debug(metaLPush.getSseq());
//        Assert.assertEquals(metaLPush.getSseq(), metaLPush.getCseq());
    Assert.assertEquals(metaLPush.getEseq(), metaLPush.getCseq());
//        Assert.assertEquals(metaLPush.getCseq(), 5);
    Assert.assertArrayEquals("LPUSH".getBytes(), metaLPush.getKey0());


    log.debug("========================2 !!!");

    ListNode n3 = ListNode.getInstance(RocksdbRedis.mydata, "redis".getBytes());
    n3.genKey("LPUSH".getBytes(), metaLPush.getCseq()-2).get();
    n3.info();

    Assert.assertEquals(n3.getSize(), 3);

    Assert.assertArrayEquals(n3.getVal0(), "ZZZ".getBytes());

    log.debug("========================1 !!!");

    ListNode n2 = n3.genKey("LPUSH".getBytes(), metaLPush.getCseq()-1).get();

    Assert.assertEquals(n2.getSize(), 3);

    Assert.assertArrayEquals(n2.getVal0(), "YYY".getBytes());
    log.debug("========================0 !!!");


    ListNode n1 = n3.genKey("LPUSH".getBytes(), metaLPush.getCseq()-0).get();

    Assert.assertEquals(n1.getSize(), 3);
    Assert.assertArrayEquals(n1.getVal0(), "XXX".getBytes());

    log.debug("========================LPOP !!!");

    Assert.assertArrayEquals(metaLPush.lpop().data().array(), "333".getBytes());

    Assert.assertEquals(metaLPush.getCount(), 5);
    Assert.assertEquals(metaLPush.getSseq(), metaLPush.getCseq()-metaLPush.getCount());

    Assert.assertEquals(metaLPush.llen().data().intValue(), 5);

    log.debug("========================1 LINDEX !!!");


    Assert.assertEquals(toString(metaLPush.lindex("1".getBytes()).data()), "111");

    Assert.assertEquals(metaLPush.lset("1".getBytes(), "Modify".getBytes()), OK);


    Assert.assertEquals(toString(metaLPush.lindex("1".getBytes()).data()), "Modify");

    Assert.assertEquals(metaLPush.lrange("0".getBytes(), "-1".getBytes()).data().length, 5);

    log.debug("========================2 LRANGE !!!");

    n3.genKey("LPUSH".getBytes(), metaLPush.getCseq()-0).get().info();
    n3.genKey("LPUSH".getBytes(), metaLPush.getCseq()-1).get().info();
    n3.genKey("LPUSH".getBytes(), metaLPush.getCseq()-2).get().info();
    n3.genKey("LPUSH".getBytes(), metaLPush.getCseq()-3).get().info();
    n3.genKey("LPUSH".getBytes(), metaLPush.getCseq()-4).get().info();

    log.debug("========================2 LRANGE !!!");

    String[] strings = {"222", "Modify", "ZZZ"};
    Assert.assertEquals(metaLPush.lrange("0".getBytes(), "2".getBytes()).toString(), Arrays.asList(strings).toString());

//        log.debug("*********count 111:"+metaLPush.getCount());
    Assert.assertEquals(metaLPush.lrem("1".getBytes(), "Modify".getBytes()).data().intValue(), 1);



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

//        Assert.fail(metaLPush.linsert("BEFORE".getBytes(), "YYY".getBytes(), "OOO".getBytes()));
//        String[] strings3 = {"ZZZ", "OOO", "YYY"};
//        Assert.assertEquals(metaLPush.lrange("0".getBytes(), "-1".getBytes()).toString(), Arrays.asList(strings3).toString());
  }


  protected static byte[] toByteArray(ByteBuf scoremember) {
    scoremember.resetReaderIndex();
    return scoremember.readBytes(scoremember.readableBytes()).array();
  }

  protected static String toString(ByteBuf scoremember) {
    return new String(toByteArray(scoremember));
  }

  protected static String toString(byte[] scoremember) {
    return new String(scoremember);
  }



}
