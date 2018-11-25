package redis.server.netty;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import redis.netty4.IntegerReply;
import redis.netty4.MultiBulkReply;

import java.nio.charset.Charset;
import java.util.Arrays;


/**
 * Test data Hash Meta.
 */
public class SetMetaTest {
  private static Logger log = Logger.getLogger(SetMetaTest.class);


  @Test
  /**
   * Hash数据集测试
   *
   * @throws RedisException
   */
  public  void testSet() throws RedisException, InterruptedException {

    SetMeta setMeta = SetMeta.getInstance(RocksdbRedis.mydata, "redis".getBytes());
    setMeta.genMetaKey("SetUpdate".getBytes()).deleteRange(setMeta.getKey0());
    Assert.assertEquals(setMeta.sismember("f1".getBytes()).data().intValue(),0);

    setMeta.genMetaKey("SetUpdate".getBytes()).sadd("f1".getBytes(), "f2".getBytes());
    Assert.assertEquals(1, setMeta.sismember("f1".getBytes()).data().intValue());

    Thread.sleep(200);

    Assert.assertEquals(2, setMeta.scard().data().intValue());
    Assert.assertEquals(2, setMeta.getMeta().getCount());
//        Assert.assertEquals(2, setMeta.smembers().getCount());

    IntegerReply smove = setMeta.smove("SetUpdate".getBytes(), "SetMove".getBytes(), "f2".getBytes());

    Thread.sleep(200);
    log.debug(smove.data().intValue());

    Assert.assertEquals(0, setMeta.genMetaKey("SetUpdate".getBytes()).sismember("f2".getBytes()).data().intValue());
    Assert.assertEquals(1, setMeta.scard().data().intValue());
    Assert.assertEquals(1, setMeta.genMetaKey("SetMove".getBytes()).sismember("f2".getBytes()).data().intValue());
    Assert.assertEquals(1, setMeta.scard().data().intValue());



    //////////////////////////////////
    setMeta.genMetaKey("SetA".getBytes()).deleteRange(setMeta.getKey0());
    setMeta.genMetaKey("SetB".getBytes()).deleteRange(setMeta.getKey0());
    setMeta.genMetaKey("SetC".getBytes()).deleteRange(setMeta.getKey0());

    setMeta.genMetaKey("SetA".getBytes()).sadd("a".getBytes(), "b".getBytes(), "c".getBytes(), "d".getBytes());
    setMeta.genMetaKey("SetB".getBytes()).sadd("c".getBytes(), "d".getBytes(), "e".getBytes(), "f".getBytes());
    setMeta.genMetaKey("SetC".getBytes()).sadd("i".getBytes(), "j".getBytes(), "c".getBytes(), "d".getBytes());
    Thread.sleep(500);

    Assert.assertEquals(setMeta.genMetaKey("SetA".getBytes()).getMeta().getCount(),4);
    Assert.assertEquals(setMeta.genMetaKey("SetB".getBytes()).getMeta().getCount(),4);
    Assert.assertEquals(setMeta.genMetaKey("SetC".getBytes()).getMeta().getCount(),4);

    String[] sdiffstr = {"a", "b"};
    String[] sinterstr = {"c", "d"};
    String[] sunionstr = {"a", "b","c","d","e","f","i","j"};

    //差集
    MultiBulkReply sdiff = setMeta.sdiff("SetA".getBytes(), "SetB".getBytes(), "SetC".getBytes());
    Assert.assertEquals(Arrays.asList(sdiffstr).toString(),sdiff.asStringSet(Charset.defaultCharset()).toString());
    //交集
    MultiBulkReply sinter = setMeta.sinter("SetA".getBytes(), "SetB".getBytes(), "SetC".getBytes());
    Assert.assertEquals(Arrays.asList(sinterstr).toString(),sinter.asStringList(Charset.defaultCharset()).toString());
    //并集
    MultiBulkReply sunion = setMeta.sunion("SetA".getBytes(), "SetB".getBytes(), "SetC".getBytes());
    Assert.assertEquals(Arrays.asList(sunionstr).toString(),sunion.asStringList(Charset.defaultCharset()).toString());


    log.debug(sdiff.asStringList(Charset.defaultCharset()));
    log.debug(sinter.asStringList(Charset.defaultCharset()));
    log.debug(sunion.asStringList(Charset.defaultCharset()));

    setMeta.sdiffstore("SetDiff".getBytes(),"SetA".getBytes(), "SetB".getBytes(), "SetC".getBytes());
    setMeta.sinterstore("SetInter".getBytes(),"SetA".getBytes(), "SetB".getBytes(), "SetC".getBytes());
    setMeta.sunionstore("SetUnion".getBytes(),"SetA".getBytes(), "SetB".getBytes(), "SetC".getBytes());

    log.debug(setMeta.genMetaKey("SetDiff".getBytes()).smembers().asStringList(Charset.defaultCharset()));
    log.debug(setMeta.genMetaKey("SetInter".getBytes()).smembers().asStringList(Charset.defaultCharset()));
    log.debug(setMeta.genMetaKey("SetUnion".getBytes()).smembers().asStringList(Charset.defaultCharset()));

    Assert.assertEquals(Arrays.asList(sdiffstr).toString(),setMeta.genMetaKey("SetDiff".getBytes()).smembers().asStringList(Charset.defaultCharset()).toString());
    Assert.assertEquals(Arrays.asList(sinterstr).toString(),setMeta.genMetaKey("SetInter".getBytes()).smembers().asStringList(Charset.defaultCharset()).toString());
    Assert.assertEquals(Arrays.asList(sunionstr).toString(),setMeta.genMetaKey("SetUnion".getBytes()).smembers().asStringList(Charset.defaultCharset()).toString());

    ///////////////////////////////////////////

    String randstr = setMeta.genMetaKey("SetUnion".getBytes()).spop().asUTF8String();
    log.debug(randstr);
    Assert.assertNotNull(randstr);

    Thread.sleep(100);

    Assert.assertEquals(7,setMeta.genMetaKey("SetUnion".getBytes()).scard().data().intValue());


    MultiBulkReply srandmember = (MultiBulkReply) setMeta.genMetaKey("SetUnion".getBytes()).srandmember("3".getBytes());

    log.debug((srandmember.asStringList(Charset.defaultCharset())));
    Assert.assertEquals(srandmember.data().length,3);

    srandmember = (MultiBulkReply) setMeta.genMetaKey("SetUnion".getBytes()).srandmember("1".getBytes());
    Assert.assertEquals(srandmember.data().length,1);

    srandmember = (MultiBulkReply) setMeta.genMetaKey("SetUnion".getBytes()).srandmember("5".getBytes());
    Assert.assertEquals(srandmember.data().length,5);


    log.debug("Over ... ...");

  }

}
