package redis.server.netty.rocksdb;

import io.netty.buffer.ByteBuf;

/**
 * redis 数据节点类型的基类
 */
public class BaseNode {
    /**
     * 打印调试
     * @param buf
     */
    public void print(ByteBuf buf) {
        buf.resetReaderIndex();
        System.out.println(new String(buf.readBytes(buf.readableBytes()).array()));
    }
}
