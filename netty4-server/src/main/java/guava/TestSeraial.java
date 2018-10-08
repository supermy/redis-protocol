package guava;

/**
 * Created by moyong on 2017/10/31.
 */
import java.util.Arrays;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class TestSeraial {
    public static void main(String[] args) {
        // 长度可动态扩展
        ByteBuf buffer = Unpooled.buffer(8);
        buffer.writeInt(8);
        buffer.writeInt(20);
        System.out.println("长度："+buffer.array().length);

        buffer.writeChar('|');
        buffer.writeBytes("|".getBytes());

        System.out.println("长度："+buffer.array().length);


        // 序列化
        byte[] bytes = new byte[buffer.writerIndex()];
        // 从channelBuffer读取至二进制数组
        buffer.readBytes(bytes);
        System.out.println(Arrays.toString(bytes));

        // 反序列化
        ByteBuf wrappedBuffer = Unpooled.wrappedBuffer(bytes);
        System.out.println(wrappedBuffer.readInt());
        System.out.println(wrappedBuffer.readInt());
        System.out.println(wrappedBuffer.readChar());
        System.out.println(new String(wrappedBuffer.readBytes(1).array()));

    }
}
