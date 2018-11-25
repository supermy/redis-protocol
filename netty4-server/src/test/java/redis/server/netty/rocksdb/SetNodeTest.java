package redis.server.netty.rocksdb;

import org.apache.log4j.Logger;
import org.junit.Test;
import redis.server.netty.RedisException;

import java.util.Random;


/**
 * Test data Hash Node.
 */
public class SetNodeTest {
  private static Logger log = Logger.getLogger(SetNodeTest.class);


  @Test
  public  void testSetNode() throws RedisException, InterruptedException {


    Random random = new Random();
    System.out.println(random.nextInt(41) + 10);//随机生成[10, 50]之间的随机数。

    for (int i = 0; i <20 ; i++) {
      System.out.println(random.nextInt(15) );
    }


    SetNode meta = SetNode.getInstance(RocksdbRedis.mydata, "redis".getBytes());

    meta.genKey1("SetTest".getBytes(), "f1".getBytes()).sadd();
    boolean exists = meta.genKey1("SetTest".getBytes(), "f1".getBytes()).exists();
    System.out.println(exists);

    meta.genKey1("SetTest".getBytes(), "f1".getBytes()).srem();
    exists=meta.genKey1("SetTest".getBytes(), "f1".getBytes()).exists();
    System.out.println(exists);


  }
}
