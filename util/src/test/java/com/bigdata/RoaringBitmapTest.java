package com.bigdata;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Preconditions;
import com.google.common.collect.Streams;
import com.google.common.hash.RoaringSerializer;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.supermy.util.MyUtils;
import org.xerial.snappy.Snappy;

import java.io.*;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RoaringBitmapTest {
    private static Logger log = Logger.getLogger(RoaringBitmapTest.class);


    /**
     *
     * 21:27:46,574 DEBUG RoaringBitmapTest:106 - 样本数据：10000000000,数据量：100 0000,数据原大小：136.18KB,Snappy压缩后大小:6.56KB
     * 21:29:22,722 DEBUG RoaringBitmapTest:112 - 样本数据：10000000000,数据量：1000 0000,数据原大小：1.21MB,Snappy压缩后大小:59.05KB
     * 21:29:54,498 DEBUG RoaringBitmapTest:114 - 样本数据：10000000000,数据量：1 0000 0000,数据原大小：11.95MB,Snappy压缩后大小:585.02KB
     * 21:30:48,153 DEBUG RoaringBitmapTest:115 - 样本数据：10000000000,数据量：10 0000 0000,数据原大小：119.34MB,Snappy压缩后大小:5.72MB
     *
     * 21:45:10,465 DEBUG RoaringBitmapTest:121 - 样本数据：10000000000,数据量：1000000000,数据原大小：119.34MB,Snappy压缩后大小:5.72MB
     *
     * @throws IOException
     */
    @Test
    public  void redisCmd() throws IOException {


        /**
         *
         * redis> SETBIT bit 10086 1
         * redis> GETBIT bit 10086
         * redis> GETBIT bit 100   # bit 默认被初始化为 0
         * redis> BITCOUNT bit
         *
         *
         */
//            RoaringBitmap rr = RoaringBitmap.bitmapOf(1, 2, 3, 1000);
        Roaring64NavigableMap r1 = new Roaring64NavigableMap();
//        r1.add(1l,1008000l);//按范围添加数据
        r1.addLong(10086);//数据是1

        Assert.assertEquals(r1.contains(10086),true);
        Assert.assertEquals(r1.contains(100860),false);
        Assert.assertEquals(r1.getLongCardinality(),1l);
        r1.add(10010);
        Assert.assertEquals(r1.getLongCardinality(),2l);
//        Assert.assertEquals(r1.getIntCardinality(),1l);
//        enum BitOp {AND, OR, XOR, NOT}

        Roaring64NavigableMap r2 = new Roaring64NavigableMap();
        r2.add(10086);
        r2.add(10000);

        //        Assert.assertEquals(r1.rankLong(1),10086);
        Assert.assertEquals(r2.select(1),10086);

        ByteArrayOutputStream baos = exportBitmap(r1);
        Roaring64NavigableMap r11 = importBitmap(baos);
        Roaring64NavigableMap r12 = importBitmap(baos);
        Roaring64NavigableMap r13 = importBitmap(baos);
        Roaring64NavigableMap r14 = importBitmap(baos);

        Assert.assertEquals(r11.getLongCardinality(),2l);
        Assert.assertEquals(r12.getLongCardinality(),2l);
        Assert.assertEquals(r13.getLongCardinality(),2l);
        Assert.assertEquals(r14.getLongCardinality(),2l);

        r11.and(r2);

        r12.or(r2);

        r13.xor(r2);

        r14.andNot(r2);

        Assert.assertEquals(r11.getLongCardinality(),1l);
        Assert.assertTrue(r11.contains(10086));

        Assert.assertEquals(r12.getLongCardinality(),3l);
        r12.forEach(item->log.debug("Item : " +item));
        r12.flip(10010);//存在则删除，不存在则增加；
        r12.flip(10010);
        r12.removeLong(10086);//数据是0
        Assert.assertEquals(r12.getLongCardinality(),2l);


        Assert.assertEquals(r13.getLongCardinality(),2l);
        Assert.assertTrue(r13.contains(10010));
        Assert.assertTrue(r13.contains(10000));

        Assert.assertEquals(r14.getLongCardinality(),1l);
        Assert.assertTrue(r14.contains(10010));

//        ByteArrayOutputStream baos14 = exportBitmap(r14);
        ByteArrayOutputStream baos14 = exportKryoBitmap(r14);


        log.debug(baos14.toByteArray().length);
        long count = 1000000000;
        long start = 10000000000l;
        for (int i = 0; i < count; i++) {
            r14.addLong(start+i);
        }


        baos14 = exportBitmap(r14);
        log.debug(String.format("样本数据：%s,数据量：%s,数据原大小：%s,Snappy压缩后大小:%s",
                start,
                count,
                MyUtils.bytes2kb(baos14.toByteArray().length),
                MyUtils.bytes2kb(Snappy.compress(baos14.toByteArray()).length)));
//        r1.rankLong() ;按数据的位置进行设置


    }

    public ByteArrayOutputStream exportBitmap(Roaring64NavigableMap r1) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        RoaringSerializer rs=new RoaringSerializer();
//        rs.write(new Kryo(),dos,r1);
        r1.serialize(dos);
        dos.flush();
        dos.close();
        return baos;
    }

    /**
     * 使用Kryo序列化优化；没有效果
     * @param r1
     * @return
     * @throws IOException
     */
    public ByteArrayOutputStream exportKryoBitmap(Roaring64NavigableMap r1) throws IOException {
        Output output = new Output(new ByteArrayOutputStream());

        RoaringSerializer rs=new RoaringSerializer();
        rs.write(new Kryo(),output,r1);
//        r1.serialize(dos);
        output.flush();
        output.close();
        return new ByteArrayOutputStream();
    }

    public Roaring64NavigableMap importBitmap(ByteArrayOutputStream baos) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream dis = new DataInputStream(bais);
        Roaring64NavigableMap r11=new Roaring64NavigableMap();
        r11.deserialize(dis);
        dis.close();
        return r11;
    }

    @Test
    public  void basic() {
        {
            RoaringBitmap rr = RoaringBitmap.bitmapOf(1, 2, 3, 1000);

//            rr.deserialize();
//            rr.serialize();


            RoaringBitmap rr2 = new RoaringBitmap();
            rr2.add(4000L, 4255L);

//            System.out.println(rr.select(3)); // would return the third value or 1000
            assertEquals(rr.select(3),1000);//0-3
//            System.out.println(rr.rank(2)); // would return the rank of 2, which is index 1
            assertEquals(rr.rank(2),2);

            assertTrue(rr.contains(1000)); // will return true
            assertFalse(rr.contains(7)); // will return false

            RoaringBitmap rror = RoaringBitmap.or(rr, rr2);// new bitmap
            rr.or(rr2); //in-place computation
            boolean equals = rror.equals(rr);// true
            assertTrue(equals);
            assertEquals(rror,rr);
            if (!equals) throw new RuntimeException("bug");

            // number of values stored?
            long cardinality = rr.getLongCardinality();
            System.out.println(cardinality);
            assertEquals(cardinality,259);
            // a "forEach" is faster than this loop, but a loop is possible:
            for (int i : rr) {
                System.out.println(i);
            }
        }


    }


    @Test
    public  void mutable() throws IOException {

        {
            MutableRoaringBitmap rr1 = MutableRoaringBitmap.bitmapOf(1, 2, 3, 1000);
            MutableRoaringBitmap rr2 = MutableRoaringBitmap.bitmapOf( 2, 3, 1010);

            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(bos);
            // If there were runs of consecutive values, you could
            // call rr1.runOptimize(); or rr2.runOptimize(); to improve compression
            rr1.serialize(dos);
            rr2.serialize(dos);
            dos.close();

//            RoaringSerializer rs=new RoaringSerializer();
//            rs.write(new Kryo(),dos,rr1);

            System.out.println(bos.toByteArray().length);
            System.out.println(Snappy.compress(bos.toByteArray()).length);

            ByteBuffer bb = ByteBuffer.wrap(bos.toByteArray());
            ImmutableRoaringBitmap rrback1 = new ImmutableRoaringBitmap(bb);

            bb.position(bb.position() + rrback1.serializedSizeInBytes());
            ImmutableRoaringBitmap rrback2 = new ImmutableRoaringBitmap(bb);


            assertEquals(rr1,rrback1);
            assertEquals(rr2,rrback2);
            System.out.println(rr1);
            System.out.println(rrback1);
            System.out.println(rr2);
            System.out.println(rrback2);

        }
    }

    @Test
    public  void longbit() throws IOException {
        Roaring64NavigableMap r = null;
        {
            r = Roaring64NavigableMap.bitmapOf(1,2,100,1000);
            r.addLong(1234);
            assertTrue(r.contains(1)); // true
            assertFalse(r.contains(3)); // false
            LongIterator i = r.getLongIterator();
            while(i.hasNext()) System.out.println(i.next());

            ByteArrayDataOutput bads= ByteStreams.newDataOutput();
            r.serialize(bads);

        }

        {
            RoaringSerializer rs=new RoaringSerializer();
            ByteArrayOutputStream baos=new ByteArrayOutputStream();
//            ByteBufferOutputStream bbos=new ByteBufferOutputStream();
            ByteBufferOutput bbo = new ByteBufferOutput(baos);
            rs.write(new Kryo(),bbo,r);

            System.out.println("Kryo Serialize："+ MyUtils.bytes2kb(baos.toByteArray().length));
            System.out.println("Kryo Serialize 压缩后："+MyUtils.bytes2kb(Snappy.compress(baos.toByteArray()).length));

        }

    }
}
