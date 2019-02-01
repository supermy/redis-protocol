package redis.server.netty.rocksdb;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.google.common.primitives.Longs;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.supermy.util.MyUtils;
import redis.server.netty.RedisException;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;


/**
 * Test data BloomFilter Meta.
 */
public class BloomFilterMetaTest {
  private static Logger log = Logger.getLogger(BloomFilterMetaTest.class);


  @Test
  public  void pfaddPfcountPfmerge() throws RedisException, CardinalityMergeException, InterruptedException {
    //String set get 操作
    //通过给数据增加版本号，进行快速删除？需要多进行一次查询

    BloomFilterMeta metaString = BloomFilterMeta.getInstance(RocksdbRedis.mymeta, "redis".getBytes());
    metaString.deleteRange("BloomFilter".getBytes());
    metaString.deleteRange("BloomFilter2".getBytes());

    // 已经存在的 key
    Assert.assertNull(metaString.get("BloomFilter".getBytes()).data());
    Assert.assertNull(metaString.get("BloomFilter2".getBytes()).data());

    long expectedInsertions = 1000000;//预估数量
    double fpp = 0.0001;//错误率
    metaString.genMetaKey("BloomFilter".getBytes());
    metaString.bfcreate(expectedInsertions,fpp);

      {
          //2. 将一部分数据添加进去
          List<byte[]> list=new ArrayList<byte[]>();
          for (int index = 0; index < expectedInsertions+1; index++) {
//      list.add(Longs.toByteArray(index));
              list.add(("test_abc_"+index).getBytes());

              if (index%100000==0){
                  byte[][] array =new byte[list.size()][];
                  metaString.bfadd(list.toArray(array));
                  Thread.sleep(30);
              }
          }
//    System.out.println(bytes.length);
          System.out.println("write all...");
      }



    // 3. 测试结果
    // expectedInsertions
    {
      long start = System.nanoTime();
        List<Long> list1 = new ArrayList<Long>(1000);

        for (long i = 0; i < expectedInsertions; i++) {
        if (metaString.bfexists(("test_abc_"+i).getBytes()).data()==0) {
            list1.add(i);
            log.debug(String.format("漏网之鱼:%s", i));
        }
      }
      log.debug(String.format("BloomFilter验证%s个漏网%s 个，时间%s毫秒，平均单个value 验证时间：%s纳秒",
              expectedInsertions,
              list1.size(),
              (System.nanoTime() - start)/1000/1000,
              (System.nanoTime() - start) / expectedInsertions));
    }

    {
      long start = System.nanoTime();
      List<Long> list1 = new ArrayList<Long>(1000);
      for (long i = expectedInsertions + 10000; i < expectedInsertions + 110000; i++) {
        if (metaString.bfexists(("test_abc_"+i).getBytes()).data()==1) {
          list1.add(i);
        }
      }
      log.debug(String.format("BloomFilter验证%s错杀%s 个，时间%s毫秒，平均单个value 验证时间：%s纳秒",
              110000-10000,
              list1.size(),
              (System.nanoTime() - start)/1000/1000,
              (System.nanoTime() - start) / ( 110000-10000)));
    }




//
//   测试创建 存储方法是get 与set;可以增加缓存；对外方法是pfadd/pfcount/pfmerge

//   metaString.info();

    log.debug("Over ... ...");

  }
}
