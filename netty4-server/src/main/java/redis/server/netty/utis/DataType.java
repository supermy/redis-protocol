package redis.server.netty.utis;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * 数据类型.
 */
public class DataType {

  public static final byte[] SPLIT = "|".getBytes();

  public static final byte[] KEY_META = "1".getBytes();
  public static final int KEY_STRING = 50;

  public static final int KEY_HASH = 3;
  public static final byte[] KEY_HASH_FIELD = "4".getBytes();
  public static final int VAL_HASH_FIELD = 54;
  public static final int KEY_SET = 5;
  public static final byte[] KEY_SET_MEMBER = "6".getBytes();
  public static final int VAL_SET_MEMBER = 56;
  public static final int KEY_LIST = 7;
  public static final int KEY_LIST_LINK = 12;

  public static final byte[] KEY_LIST_ELEMENT = "8".getBytes();
  public static final int VAL_LIST_ELEMENT = 58;
  public static final int KEY_ZSET = 11;
  public static final byte[] KEY_ZSET_SCORE = "9".getBytes();
  public static final int VAL_ZSET_SCORE = 59;
  public static final byte[] KEY_ZSET_SORT = "10".getBytes();
  public static final int VAL_ZSET_SORT = 60;


  public static final int KEY_HYPERLOGLOG = 70;
  public static final int KEY_BLOOMFILTER = 80;
  public static final int KEY_JSONPATH = 90;



  public static final int DEFAULT_NS = 0;

//  public static ByteBuf getNs(){
//    return Unpooled.wrappedBuffer(DEFAULT_NS);
//  }



}
