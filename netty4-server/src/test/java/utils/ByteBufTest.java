package utils;

import com.google.common.primitives.Longs;
import io.netty.buffer.*;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.supermy.util.MyUtils;

import java.io.InputStreamReader;
import java.util.Iterator;

/**
 * 主键的设计
 * ByteBuf 的练习
 * <p>
 * Created by moyong on 2017/10/20.
 * <p>
 * netty中ByteBuf的缓冲区的优势：
 * （1）可以自定义缓冲区的类型；
 * <p>
 * （2）通过内置的复合缓冲类型实现零拷贝；
 * <p>
 * （3）不需要调用flip()函数切换读/写模式
 * <p>
 * （4）读取和写入的索引分开了，不像JDK中使用一个索引
 * <p>
 * （5）引用计数（referenceCounting的实现原理？）
 * <p>
 * （6） Pooling池
 * <p>
 * <p>
 * 从内存分配的角度看，ByteBuf可以分为两类：
 * <p>
 * 1、堆内存（HeapByteBuf）字节缓冲区：特点是内存的分配和回收速度快，可以被JVM自动回收；缺点就是如果进行Socket的IO读写，
 * 需要额外做一次内存复制，将堆内存对应的缓冲区复制到内核Channel中，性能会有一定程度的下降
 * <p>
 * 2、直接内存（DirectByteBuf） 字节缓冲区：非堆内存，它在对外进行内存分配，相比于堆内存，它的分配和回收速度会慢一些，
 * 但是将它写入或者从Socket Channel中读取时，由于少一次内存复制，速度比堆内存快
 * <p>
 * Netty的最佳实践是在I/O通信线程的读写缓冲区使用DirectByteBuf，后端业务消息的编解码模块使用HeapByteBuf，这样组合
 * 可以达到性能最优。
 */
public class ByteBufTest {
    private static Logger log = Logger.getLogger(ByteBufTest.class);


    @Test
    public void byteBufTest() throws InterruptedException {

        ByteBuf dBuf = Unpooled.directBuffer(16);
        if (!dBuf.hasArray()) {
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
        while (iter.hasNext()) {
            log.debug(iter.next().toString());
        }

        //使用数组访问数据
        if (!compBuf.hasArray()) {
            int len = compBuf.readableBytes();
            byte[] arr = new byte[len];
            compBuf.getBytes(0, arr);
        }


        String header = "h";
        String body = "directBuffer_b";
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

    @Test
    public void protocol() {
        //实现一个自定义的消息协议，消息包括header和body两部分内容，body里放的是JSON字符串。那么就可以使用ByteBufInputStream来避免把ByteBuf里的字节拷贝到字节数组的开销：
        ByteBuf bb = Unpooled.buffer(16);

        // read header
        // bb.readXxx()...

        // read body
        InputStreamReader reader = new InputStreamReader(new ByteBufInputStream(bb));
//         new Gson().fromJson(reader, JsonObject.class);
    }

    @Test
    public void byteBuf() {
        log.debug("=============================================================");

//        ridx是readerIndex读取数据索引，位置从0开始
//        widx是writeIndex写数据索引，位置从0开始
//        cap是capacity缓冲区初始化的容量，默认256，可以通过Unpooled.buffer(8)设置，初始化缓冲区容量是8。

//
//        如果写入内容超过cap，cap会自动增加容量，但不能超过缓冲区最大容量maxCapacity。
        ByteBuf heapBuffer1 = Unpooled.buffer();
        Assert.assertEquals(heapBuffer1.capacity(), 256);

        ByteBuf heapBuffer = Unpooled.buffer(8);
        Assert.assertEquals(heapBuffer.capacity(), 8);

        heapBuffer.writeBytes("测试测试测试".getBytes());
        log.debug("写入测试测试测试：" + heapBuffer);

        //创建一个16字节的buffer,这里默认是创建heap buffer
        ByteBuf buf = Unpooled.buffer(16);
        Assert.assertEquals(buf.capacity(), 16);

        //写数据到buffer
        //byte 0-127
        int[] index = new int[16];
        for (int i = 0; i < 16; i++) {
            buf.writeInt(i + 1);
            index[i] = i + 1;
        }

        //读数据
        for (int i = 0; i < 16; i++) {
            int i1 = buf.readInt();
            log.debug(i1 + "  " + index[i]);
            Assert.assertEquals(i1, index[i]);
        }
    }

    @Test
    public void fieldKey() {
        log.debug("hash key value begin=============================================================");
        //hash key 组成  h-0 key-1 field  key value 长度浮动  方便 put get del hput hget hdel
        {
            //会自动进行内存释放
            ByteBuf hkeybuf = Unpooled.buffer(16);
            hkeybuf.writeBytes("_h".getBytes());
            hkeybuf.writeBytes("key".getBytes());
            hkeybuf.writeBytes("field".getBytes());
            Assert.assertEquals(new String(hkeybuf.readBytes(hkeybuf.readableBytes()).array()), "_hkeyfield");
        }

        {
            //Unpooled.wrappedBuffer 需要主动进行内存释放
            ByteBuf hkeybuf1 = Unpooled.wrappedBuffer("_h".getBytes(),"key".getBytes(),"field".getBytes());
            Assert.assertEquals(new String(hkeybuf1.readBytes(hkeybuf1.readableBytes()).array()), "_hkeyfield");
            hkeybuf1.release();
        }


    }

    @Test
    public void fieldValue() {
        //hash val 组成  val-0 ttl-8 size-4  有读取前面字节的方法
        ByteBuf valbuf = Unpooled.buffer(16);
        long value = System.currentTimeMillis();
        valbuf.writeLong(value);
        valbuf.writeInt(123456789);
        valbuf.writeBytes("field value".getBytes());

        Assert.assertEquals(valbuf.readLong(), value); //ttl
        Assert.assertEquals(valbuf.readInt(), 123456789); //size

        Assert.assertEquals(new String(valbuf.readBytes(valbuf.readableBytes()).array()), "field value");
    }

    @Test
    public void metaKey() {
        //hash meta 元数据组成  H-0 key
        ByteBuf metabuf = Unpooled.buffer(16);
        metabuf.writeBytes("_h".getBytes());
        metabuf.writeBytes("key".getBytes());

        ByteBuf metabuf1 = Unpooled.wrappedBuffer("_h".getBytes(),"key".getBytes());

        log.debug(metabuf.readableBytes());
        log.debug(metabuf.writableBytes());

        metabuf.resetReaderIndex();
        Assert.assertEquals(new String(metabuf.readBytes(metabuf.readableBytes()).array()), "_hkey");
        Assert.assertEquals(new String(metabuf1.readBytes(metabuf1.readableBytes()).array()), "_hkey");
    }

    /**
     * @param args
     */
    @Test
    public void calSize() {
        Assert.assertEquals(Integer.SIZE / 8, 4);           // 4
        Assert.assertEquals(Short.SIZE / 8, 2);               // 2
        Assert.assertEquals(Long.SIZE / 8, 8);                 // 8
        Assert.assertEquals(Byte.SIZE / 8, 1);                 // 1
        Assert.assertEquals(Character.SIZE / 8, 2);       // 2
        Assert.assertEquals(Float.SIZE / 8, 4);               // 4
        Assert.assertEquals(Double.SIZE / 8, 8);             // 8
        Assert.assertEquals(Boolean.toString(false), "false");

    }

    @Test
    public void memoryLeakByteBuf() {
        {
            {
                long count = 10000000;
                long start=System.currentTimeMillis();
                long freeMemory = Runtime.getRuntime().freeMemory();
                for (int i = 0; i <count ; i++) {
                    ByteBuf buf1 = Unpooled.wrappedBuffer(Longs.toByteArray(i));
                    ByteBuf buf2 = Unpooled.wrappedBuffer(Longs.toByteArray(i));



                    ByteBuf buf = Unpooled.buffer();
                    buf.writeBytes(buf1.array());
                    buf.writeBytes(buf2.array());

                    ByteBuf buf3 = Unpooled.wrappedBuffer(buf1,buf2);
                    buf3.release(); //最后才能释放，否则buf1+buf2会被回收；
                    // This will print buf to System.out and destroy it.
//                buf.writeInt(i);
                }
                log.debug(String.format("使用的堆内存 赋值%s个 :时间，%s毫秒； 内存，%s",
                        count,
                        System.currentTimeMillis()-start,
                        MyUtils.bytes2kb(freeMemory-Runtime.getRuntime().freeMemory())));
            }


            ByteBuf buf=Unpooled.wrappedBuffer("abcd".getBytes());
            Assert.assertArrayEquals(buf.array(),"abcd".getBytes());

        }
    }

    /**
     *
     * 内存泄露测试
     *
     * PooledByteBufAllocator采用了jemalloc内存分配算法，大致思想如下：
     *
     * 为了降低锁竞争，将池化空间划分为多个PoolArena，每个线程按照负载均衡算法绑定到特定的PoolArena；
     * 为了进一步降低同一个PoolArena不同线程间的锁竞争，每个线程内部增加threadcache；
     * 为了提高内存分配效率并减少内部碎片，内存划分为tiny、small和normal，采取不同的策略进行管理；
     *
     *
     */
    @Test
    public void memoryLeak() {
        {
            long start=System.currentTimeMillis();
            long freeMemory = Runtime.getRuntime().freeMemory();
            for (int i = 0; i <1000000 ; i++) {
                ByteBuf buf = Unpooled.directBuffer(12);
                // This will print buf to System.out and destroy it.
                buf.writeInt(i);
                directBuffer_c(directBuffer_b(start_a(buf)));
                assert buf.refCnt() == 0;
            }
            log.debug(String.format("使用的是系统的核心内存 CPU效率低 directbuffer:时间，%s； 内存，%s", System.currentTimeMillis()-start,(freeMemory-Runtime.getRuntime().freeMemory())/1024/1024+"Mb"));

        }

        {
            long start=System.currentTimeMillis();
            long freeMemory = Runtime.getRuntime().freeMemory();
            for (int i = 0; i <1000000 ; i++) {
                ByteBuf buf = Unpooled.buffer(12);
//                ByteBuf buf = PooledByteBufAllocator.DEFAULT.heapBuffer(12);

                // This will print buf to System.out and destroy it.
                buf.writeInt(i);
                headBuf_c1(headBuf_b1(start_a(buf)));
//                assert buf.refCnt() == 0;
            }
            log.debug(String.format("自动回收内存 效率更高 headbuffer:时间，%s； 内存，%s", System.currentTimeMillis()-start,(freeMemory-Runtime.getRuntime().freeMemory())/1024/1024+"Mb"));

        }

        {

//            ByteBuf outBuf= UnpooledByteBufAllocator.DEFAULT.heapBuffer();

//ByteBuf outBuf=PooledByteBufAllocator.DEFAULT.heapBuffer();

//ByteBuf outBuf=PooledByteBufAllocator.DEFAULT.buffer();

//ByteBuf outBuf=UnpooledByteBufAllocator.DEFAULT.buffer();

        }

//        new BulkReply(ChannelBuffers.wrappedBuffer(message.getBytes())),

    }

    public ByteBuf start_a(ByteBuf input) {
        input.writeByte(42);
        return input;
    }

    /**
     * 直接内存（DirectByteBuf）
     *
     * @param input
     * @return
     */
    public ByteBuf directBuffer_b(ByteBuf input) {
        try {
            ByteBuf output = input.alloc().directBuffer(input.readableBytes() + 1);
            output.writeBytes(input);
            output.writeByte(42);
//            log.debug(output);
            return output;
        } finally {
            input.release();
        }

    }

    public void directBuffer_c(ByteBuf input) {
//        log.debug(input);
        input.release();
    }


    /**
     * 堆内存（HeapByteBuf）字节缓冲区
     *
     * @param input
     * @return
     */
    public ByteBuf headBuf_b1(ByteBuf input) {
        try {
            ByteBuf output = input.alloc().heapBuffer(input.readableBytes() + 1);
//            ByteBuf output = Unpooled.buffer(12);
            output.writeBytes(input);
            output.writeByte(42);
//            log.debug(output);
            return output;
        } finally {
//            input.release();
        }

    }

    public void headBuf_c1(ByteBuf input) {
//        log.debug(input);
//        input.release();
    }

}
