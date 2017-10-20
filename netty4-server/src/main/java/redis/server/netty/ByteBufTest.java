package redis.server.netty;

import com.sampullara.cli.Args;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;

import java.io.InputStreamReader;
import java.util.Iterator;

/**
 * 主键的设计
 * Created by moyong on 2017/10/20.
 */
public class ByteBufTest {
    public static void main(String[] args) throws InterruptedException {
            // TODO Auto-generated method stub
//        ByteBufTest calcsizof= new ByteBufTest();
        ByteBufTest.calSize();


        //创建一个16字节的buffer,这里默认是创建heap buffer
        ByteBuf buf = Unpooled.buffer(16);
        //写数据到buffer
        for(int i=0; i<16; i++){
            buf.writeByte(i+1);
        }

        //读数据
        for(int i=0; i<buf.capacity(); i++){
            System.out.print(buf.getByte(i)+", ");
        }

        System.out.println("");

        System.out.println("hash key value begin=============================================================");
        //hash key 组成  h-0 key-1 field  key value 长度浮动  方便 put get del hput hget hdel

        ByteBuf hkeybuf = Unpooled.buffer(16);
        hkeybuf.writeBytes("_h".getBytes());
        hkeybuf.writeBytes("key".getBytes());
        hkeybuf.writeBytes("field".getBytes());

        System.out.println(hkeybuf.readableBytes());
        System.out.println(hkeybuf.writableBytes());
        System.out.println(new String(hkeybuf.readBytes(hkeybuf.readableBytes()).array())); //hkey

        //hash val 组成  val-0 ttl-8 size-4  有读取前面字节的方法
        ByteBuf valbuf = Unpooled.buffer(16);
        valbuf.writeLong(System.currentTimeMillis());
        valbuf.writeInt(123456789);
        valbuf.writeBytes("field value".getBytes());

        System.out.println(valbuf.readableBytes());
        System.out.println(valbuf.writableBytes());

        System.out.println(valbuf.readLong()); //ttl
        System.out.println(valbuf.readInt()); //size
        System.out.println(new String(valbuf.readBytes(valbuf.readableBytes()).array()));

        //hash meta 元数据组成  H-0 key
        ByteBuf metabuf = Unpooled.buffer(16);
        hkeybuf.writeBytes("_H".getBytes());
        hkeybuf.writeBytes("key".getBytes());


        System.out.println(metabuf.readableBytes());
        System.out.println(metabuf.writableBytes());

        System.out.println(new String(hkeybuf.readBytes(hkeybuf.readableBytes()).array())); //hkey


        System.out.println("hash key value end=============================================================");


        //实现一个自定义的消息协议，消息包括header和body两部分内容，body里放的是JSON字符串。那么就可以使用ByteBufInputStream来避免把ByteBuf里的字节拷贝到字节数组的开销：
        ByteBuf bb = Unpooled.buffer(16);

        // read header
        // bb.readXxx()...

        // read body
        InputStreamReader reader = new InputStreamReader(new ByteBufInputStream(bb));
//         new Gson().fromJson(reader, JsonObject.class);


        ////////////
        ByteBuf dBuf = Unpooled.directBuffer(16);
        if(!dBuf.hasArray()){
            int len = dBuf.readableBytes();
            byte[] arr = new byte[len];
            dBuf.getBytes(0, arr);
        }


        //组合缓冲区
        CompositeByteBuf compBuf = Unpooled.compositeBuffer();
        //堆缓冲区
        ByteBuf heapBuf = Unpooled.buffer(8);
        //直接缓冲区
        ByteBuf directBuf = Unpooled.directBuffer(16);
        //添加ByteBuf到CompositeByteBuf
        compBuf.addComponents(heapBuf, directBuf);
        //删除第一个ByteBuf
//        compBuf.removeComponent(0);
        Iterator<ByteBuf> iter = compBuf.iterator();
        while(iter.hasNext()){
            System.out.println(iter.next().toString());
        }

        //使用数组访问数据
        if(!compBuf.hasArray()){
            int len = compBuf.readableBytes();
            byte[] arr = new byte[len];
            compBuf.getBytes(0, arr);
        }
    }

    /**
     * @param args
     */
    private static void calSize() {
        System.out.println("Integer: " + Integer.SIZE/8);           // 4
        System.out.println("Short: " + Short.SIZE/8);               // 2
        System.out.println("Long: " + Long.SIZE/8);                 // 8
        System.out.println("Byte: " + Byte.SIZE/8);                 // 1
        System.out.println("Character: " + Character.SIZE/8);       // 2
        System.out.println("Float: " + Float.SIZE/8);               // 4
        System.out.println("Double: " + Double.SIZE/8);             // 8
        System.out.println("Boolean: " + Boolean.toString(false));

    }

}
