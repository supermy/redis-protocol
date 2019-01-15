package redis.server.netty.rocksdb;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import redis.server.netty.RedisException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;

import static org.junit.Assert.assertEquals;


/**
 * Test data HyperLogLog Meta.
 */
public class HyperLogLogMetaTest {
  private static Logger log = Logger.getLogger(HyperLogLogMetaTest.class);


  @Test
  public  void pfaddPfcountPfmerge() throws RedisException, CardinalityMergeException {
    //String set get 操作
    //通过给数据增加版本号，进行快速删除？需要多进行一次查询

    HyperLogLogMeta metaString = HyperLogLogMeta.getInstance(RocksdbRedis.mymeta, "redis".getBytes());
    metaString.deleteRange("HyperLogLog".getBytes());
    metaString.deleteRange("HyperLogLog2".getBytes());

    // 已经存在的 key
    Assert.assertNull(metaString.get("HyperLogLog".getBytes()).data());
    Assert.assertNull(metaString.get("HyperLogLog2".getBytes()).data());

    metaString.genMetaKey("HyperLogLog".getBytes()).pfadd(new byte[][]{"a".getBytes(), "b".getBytes(), "c".getBytes(),"d".getBytes()});
    assertEquals(metaString.pfcount("HyperLogLog".getBytes()).data().longValue(),4);

    metaString.genMetaKey("HyperLogLog2".getBytes()).pfadd(new byte[][]{"e".getBytes(), "f".getBytes(),"d".getBytes()});
    assertEquals(metaString.pfcount("HyperLogLog2".getBytes()).data().longValue(),3);


    System.out.println(metaString.pfmerge("HyperLogLog".getBytes(),"HyperLogLog2".getBytes()).data());
    assertEquals(metaString.pfmerge("HyperLogLog".getBytes(),"HyperLogLog2".getBytes()).data().longValue(),6);



//
//   测试创建 存储方法是get 与set;可以增加缓存；对外方法是pfadd/pfcount/pfmerge

//   metaString.info();

    log.debug("Over ... ...");

  }
}
