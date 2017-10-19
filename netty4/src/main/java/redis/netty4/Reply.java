package redis.netty4;

import java.io.IOException;

import io.netty.buffer.ByteBuf;

/**
 * 回复信息接口
 *
 * @param <T>
 */
public interface Reply<T> {
  byte[] CRLF = new byte[] { RedisReplyDecoder.CR, RedisReplyDecoder.LF };

  T data();
  void write(ByteBuf os) throws IOException;
}
