package redis.server.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;
import redis.netty4.Command;

import java.io.IOException;
import java.util.List;

import static redis.netty4.RedisReplyDecoder.readLong;

/**
 * Decode commands.
 */
public class RedisCommandDecoder extends ReplayingDecoder<Void> {

    private byte[][] bytes;//二维数组，可定义数组个数
    private int arguments = 0;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        //bytes 计算了命令参数个数
        if (bytes != null) {
            int numArgs = bytes.length;
            //依次获取参数
            for (int i = arguments; i < numArgs; i++) { //参数开始位置可定义?
                //参数行的开始标志
                if (in.readByte() == '$') {
                    //获取参数的长度
                    long l = readLong(in);
                    if (l > Integer.MAX_VALUE) {
                        throw new IllegalArgumentException("Java only supports arrays up to " + Integer.MAX_VALUE + " in size");
                    }
                    int size = (int) l;
                    bytes[i] = new byte[size];
                    //写入参数到直接变量,参数的长度是目标字节的长度
                    in.readBytes(bytes[i]);
                    //参数必须 CRLF 结尾
                    if (in.bytesBefore((byte) '\r') != 0) {
                        throw new RedisException("Argument doesn't end in CRLF");
                    }
                    //跳过回车与换行
                    in.skipBytes(2);
                    //参数计数+1
                    arguments++;
                    checkpoint();
                } else {
                    throw new IOException("Unexpected character");
                }
            }
            try {
//                for (byte[] byt:bytes
//                     ) {
//                    System.out.println(new String(byt));
//                }
                out.add(new Command(bytes));
            } finally {
                //命令恢复到初始状态
                bytes = null;
                arguments = 0;
            }


        } else if (in.readByte() == '*') {//参数个数
            long l = readLong(in);//读取参数个数
            if (l > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Java only supports arrays up to " + Integer.MAX_VALUE + " in size");
            }
            int numArgs = (int) l;
            if (numArgs < 0) {
                throw new RedisException("Invalid size: " + numArgs);
            }
            //获取命令关键字数量
            bytes = new byte[numArgs][];
            checkpoint();
            //参数提取
            decode(ctx, in, out);


        } else {
            // Go backwards one 回退导航
            in.readerIndex(in.readerIndex() - 1);
            // Read command -- can't be interupted
            byte[][] b = new byte[1][];
            b[0] = in.readBytes(in.bytesBefore((byte) '\r')).array();
            //跳过两个字节
            in.skipBytes(2);
            //inline 是老协议，直接转发
            out.add(new Command(b, true));
        }
    }

}
