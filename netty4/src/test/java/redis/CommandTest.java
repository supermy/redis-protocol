package redis;

import com.google.common.base.Charsets;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Longs;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;
import redis.netty4.BulkReply;
import redis.netty4.MultiBulkReply;
import redis.netty4.RedisReplyDecoder;
import redis.netty4.Reply;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static junit.framework.Assert.assertEquals;
import static redis.util.Encoding.numToBytes;

/**
 * Some low level tests
 * 增加Longs.toByteArray 性能测试
 */
public class CommandTest {
  @Test
  public void testNumToBytes() {
    {
      assertEquals("-12345678", new String(numToBytes(-12345678, false)));
      assertEquals("-1", new String(numToBytes(-1, false)));
      assertEquals("0", new String(numToBytes(0, false)));
      assertEquals("10", new String(numToBytes(10, false)));
      assertEquals("12345678", new String(numToBytes(12345678, false)));
      assertEquals("-1\r\n", new String(numToBytes(-1, true)));
      assertEquals("10\r\n", new String(numToBytes(10, true)));
      assertEquals("12345678\r\n", new String(numToBytes(12345678, true)));
    }
    {
      assertEquals(-12345678, Longs.fromByteArray(Longs.toByteArray(-12345678)));
      assertEquals(-1, Longs.fromByteArray(Longs.toByteArray(-1)));
      assertEquals(0, Longs.fromByteArray(Longs.toByteArray(0)));
      assertEquals(10, Longs.fromByteArray(Longs.toByteArray(10)));
      assertEquals(12345678, Longs.fromByteArray(Longs.toByteArray(12345678)));
      //todo
      assertEquals("-1\r\n", byteArry2num(num2byteArry(-1,true),true));
      assertEquals("10\r\n",  byteArry2num(num2byteArry(10,true),true));
      assertEquals("12345678\r\n",  byteArry2num(num2byteArry(12345678,true),true));
    }

  }

  public byte[] num2byteArry(long v,boolean withCRLF){
    byte[] array = Unpooled.buffer().writeChar('\r').writeChar('\n').array();
    return withCRLF ? Bytes.concat(Longs.toByteArray(v), array):Longs.toByteArray(v);
  }

  public String byteArry2num(byte[] v,boolean withCRLF){
    byte[] array = Unpooled.buffer().writeChar('\r').writeChar('\n').array();
    return withCRLF ? Longs.fromByteArray(v)+"\r\n":Longs.fromByteArray(v)+"";
  }

  /**
   * Long.toString(i).getBytes(Charsets.UTF_8); 857
   * numToBytes 688
   * Longs.tobytearray 62
   */
  @Test
  public void benchmark() {
    if (System.getenv().containsKey("CI") || System.getProperty("CI") != null) return;
    long diff,diff1=0;
    long total;
    {
      // Warm them up ；预热
      for (int i = 0; i < 10000000; i++) {
        Long.toString(i).getBytes(Charsets.UTF_8);
        Longs.toByteArray(i);
        numToBytes(i, true);
      }
    }
    {
      long start = System.currentTimeMillis();
      for (int i = 0; i < 10000000; i++) {
        Long.toString(i).getBytes(Charsets.UTF_8);
      }
      total = diff = System.currentTimeMillis() - start;
    }
    {
      long start = System.currentTimeMillis();
      for (int i = 0; i < 10000000; i++) {
        numToBytes(i, true);
      }
      diff = System.currentTimeMillis() - start;
    }

    {
      long start1 = System.currentTimeMillis();
      for (int i = 0; i < 10000000; i++) {
//        numToBytes(i, true);
        Longs.toByteArray(i);

      }
      diff1 = System.currentTimeMillis() - start1 ;
    }

    System.out.println(total + ", " + diff+", "+diff1);
  }

  @Test
  public void freelsBench() throws IOException {
    if (System.getenv().containsKey("CI") || System.getProperty("CI") != null) return;
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    baos.write(MultiBulkReply.MARKER);
    baos.write("100\r\n".getBytes());
    for (int i = 0; i < 100; i++) {
      baos.write(BulkReply.MARKER);
      baos.write("6\r\n".getBytes());
      baos.write("foobar\r\n".getBytes());
    }

    byte[] multiBulkReply = baos.toByteArray();
    long start = System.currentTimeMillis();
    RedisReplyDecoder redisDecoder = new RedisReplyDecoder(false);
    ByteBuf cb = Unpooled.wrappedBuffer(multiBulkReply);

    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 100000; j++) {
        Reply receive = redisDecoder.receive(cb);
        cb.resetReaderIndex();
      }
      long end = System.currentTimeMillis();
      long diff = end - start;
      System.out.println(diff + " " + ((double)diff)/100000);
      start = end;
    }

  }
}
