package redis.netty4;

import com.google.common.base.Charsets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.nio.charset.Charset;

import static redis.util.Encoding.numToBytes;

/**
 * 批量回复
 */
public class BulkReply implements Reply<ByteBuf> {
  public static final BulkReply NIL_REPLY = new BulkReply();

  public static final char MARKER = '$';
  //字节缓存
  private final ByteBuf bytes;
  private final int capacity;

  /**
   * 构造子，字节内容与容量都是空
   */
  private BulkReply() {
    bytes = null;
    capacity = -1;
  }

  public BulkReply(byte[] bytes) {
    this.bytes = Unpooled.wrappedBuffer(bytes);
    capacity = bytes.length;
  }

  public BulkReply(ByteBuf bytes) {
    this.bytes = bytes;
    capacity = bytes.capacity();
  }

  @Override
  public ByteBuf data() {
//    bytes.resetReaderIndex();
    return bytes;
  }

  public byte[] dataByte() {
    bytes.resetReaderIndex();
    return bytes.readBytes(bytes.readableBytes()).array();
  }

  public boolean isEmpty(){
    return bytes == null;
  }

  public String asAsciiString() {
    if (bytes == null) return null;
    return bytes.toString(Charsets.US_ASCII);
  }

  public String asUTF8String() {
    if (bytes == null) return null;
    return bytes.toString(Charsets.UTF_8);
  }

  public String asString(Charset charset) {
    if (bytes == null) return null;
    return bytes.toString(charset);
  }

  @Override
  public void write(ByteBuf os) throws IOException {
    os.writeByte(MARKER);
    os.writeBytes(numToBytes(capacity, true));
    if (capacity > 0) {
      os.writeBytes(bytes);
      os.writeBytes(CRLF);
    }
  }

  public String toString() {
    return asUTF8String();
  }
}
