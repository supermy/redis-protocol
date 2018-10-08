package guava;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;

import java.io.InputStreamReader;
import java.util.Iterator;

/**
 * 主键的设计
 * ByteBuf 的练习
 *
 * Created by moyong on 2017/10/20.
 *
 * netty中ByteBuf的缓冲区的优势：
 * （1）可以自定义缓冲区的类型；
 *
 * （2）通过内置的复合缓冲类型实现零拷贝；
 *
 * （3）不需要调用flip()函数切换读/写模式
 *
 * （4）读取和写入的索引分开了，不像JDK中使用一个索引
 *
 * （5）引用计数（referenceCounting的实现原理？）
 *
 * （6） Pooling池
 *
 *
 * 从内存分配的角度看，ByteBuf可以分为两类：
 *
 * 1、堆内存（HeapByteBuf）字节缓冲区：特点是内存的分配和回收速度快，可以被JVM自动回收；缺点就是如果进行Socket的IO读写，需要额外做一次内存复制，将堆内存对应的缓冲区复制到内核Channel中，性能会有一定程度的下降
 *
 * 2、直接内存（DirectByteBuf） 字节缓冲区：非堆内存，它在对外进行内存分配，相比于堆内存，它的分配和回收速度会慢一些，但是将它写入或者从Socket Channel中读取时，由于少一次内存复制，速度比堆内存快
 *
 * Netty的最佳实践是在I/O通信线程的读写缓冲区使用DirectByteBuf，后端业务消息的编解码模块使用HeapByteBuf，这样组合可以达到性能最优。
 *
 */
public class ByteBufTest {
    public static void main(String[] args) throws InterruptedException {
            // TODO Auto-generated method stub
//        ByteBufTest calcsizof= new ByteBufTest();
        ByteBufTest.calSize();

        System.out.println("");

        System.out.println("=============================================================");

//        ridx是readerIndex读取数据索引，位置从0开始
//        widx是writeIndex写数据索引，位置从0开始
//        cap是capacity缓冲区初始化的容量，默认256，可以通过Unpooled.buffer(8)设置，初始化缓冲区容量是8。

//
//        如果写入内容超过cap，cap会自动增加容量，但不能超过缓冲区最大容量maxCapacity。
        ByteBuf heapBuffer1 = Unpooled.buffer();
        System.out.println("默认:"+heapBuffer1);

        ByteBuf heapBuffer = Unpooled.buffer(8);
        System.out.println("初始化："+heapBuffer);
        heapBuffer.writeBytes("测试测试测试".getBytes());
        System.out.println("写入测试测试测试："+heapBuffer);

        System.out.println("");

        System.out.println("=============================================================");

        //创建一个16字节的buffer,这里默认是创建heap buffer
        ByteBuf buf = Unpooled.buffer(16);
        //写数据到buffer
        //byte 0-127
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


        String header="h";
        String body="b";
        ByteBuf footer = Unpooled.buffer(16);
        footer.writeBytes("footer".getBytes());
        footer.writeLong(123456);

        //组合类型和组件类型不匹配
        ByteBuf message = Unpooled.wrappedBuffer(header.getBytes(), body.getBytes());

        //因此，你可以通过混和一个组合buffer和一个普通buffer创建一个组合buffer

        ByteBuf messageWithFooter = Unpooled.wrappedBuffer(message, footer);

        //由于组合buffer仍然是一个ByteBuf，你可以很容易的获取它的内容，即便你要获取的区域跨越多个组件，和获取简单Buffer的获取方式也是一样的。

        //实例中获取的unsigned整型跨越了内容和尾部。
        messageWithFooter.getUnsignedInt(
                messageWithFooter.readableBytes() - footer.readableBytes() - 1);


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
