package redis.server.netty.rocksdb;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.xerial.snappy.Snappy;
import redis.netty4.MultiBulkReply;
import redis.server.netty.RedisException;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;


/**
 * Test data JsonPath Meta.
 */
public class JsonPathMetaTest {
  private static Logger log = Logger.getLogger(JsonPathMetaTest.class);


  @Test
  public  void jpCreateGetSetDel() throws RedisException, CardinalityMergeException, InterruptedException, IOException {

    byte[] test_string_compresses = Snappy.compress("test String compress");
    byte[] test_string_compresses1 = Snappy.compress("test String compress".getBytes());
    log.debug(Snappy.uncompressString(test_string_compresses));
    log.debug(Snappy.uncompressString(test_string_compresses1));
    Assert.assertEquals(Snappy.uncompressString(test_string_compresses),Snappy.uncompressString(test_string_compresses1));

    JsonPathMeta meta= JsonPathMeta.getInstance(RocksdbRedis.mymeta, "redis".getBytes());
    byte[] testkey="jsonpath".getBytes();
    meta.deleteRange(testkey);

    // 已经存在的 key
    Assert.assertNull(meta.get(testkey).data());

      String json1="{\"books\":[{\"category\":\"fiction\"},{\"category\":\"reference\"},{\"category\":\"fiction\"},{\"category\":\"fiction\"},{\"category\":\"reference\"},{\"category\":\"fiction\"},{\"category\":\"reference\"},{\"category\":\"reference\"},{\"category\":\"reference\"},{\"category\":\"reference\"},{\"category\":\"reference\"}]}";

    meta.genMetaKey(testkey);
    meta.jpcreate(json1.getBytes());

      {
          //2. 将一部分数据添加进去
//          MultiBulkReply jpget = meta.jpget("$..category".getBytes());
          MultiBulkReply jpget = meta.jpget("$.books[0].category".getBytes());
          log.debug(jpget.asStringList(Charset.defaultCharset()));

          meta.jpset("$.books[0].category".getBytes(),"f0".getBytes());
          jpget = meta.jpget("$.books[0].category".getBytes());
          log.debug(jpget.asStringList(Charset.defaultCharset()));

          log.debug(meta.jpdel("$.books[0].category".getBytes()));
          jpget = meta.jpget("$.books[0].category".getBytes());
          log.debug(jpget.asStringList(Charset.defaultCharset()));

//          meta.jpdel();
      }


//
//    // 3. 测试结果
//    // expectedInsertions
//    {
//      long start = System.nanoTime();
//        List<Long> list1 = new ArrayList<Long>(1000);
//
//        for (long i = 0; i < expectedInsertions; i++) {
//        if (meta.bfexists(("test_abc_"+i).getBytes()).data()==0) {
//            list1.add(i);
//            log.debug(String.format("漏网之鱼:%s", i));
//        }
//      }
//      log.debug(String.format("BloomFilter验证%s个漏网%s 个，时间%s毫秒，平均单个value 验证时间：%s纳秒",
//              expectedInsertions,
//              list1.size(),
//              (System.nanoTime() - start)/1000/1000,
//              (System.nanoTime() - start) / expectedInsertions));
//    }
//
//    {
//      long start = System.nanoTime();
//      List<Long> list1 = new ArrayList<Long>(1000);
//      for (long i = expectedInsertions + 10000; i < expectedInsertions + 110000; i++) {
//        if (meta.bfexists(("test_abc_"+i).getBytes()).data()==1) {
//          list1.add(i);
//        }
//      }
//      log.debug(String.format("BloomFilter验证%s错杀%s 个，时间%s毫秒，平均单个value 验证时间：%s纳秒",
//              110000-10000,
//              list1.size(),
//              (System.nanoTime() - start)/1000/1000,
//              (System.nanoTime() - start) / ( 110000-10000)));
//    }




//
//   测试创建 存储方法是get 与set;可以增加缓存；对外方法是pfadd/pfcount/pfmerge

//   meta.info();

    log.debug("Over ... ...");

  }
}
