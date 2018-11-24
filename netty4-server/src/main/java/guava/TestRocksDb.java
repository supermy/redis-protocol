package guava;

import com.google.common.primitives.Doubles;
import com.google.common.primitives.Longs;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static redis.util.Encoding.numToBytes;

/**
 * Created by moyong on 2017/10/22.
 *
 * 多线程 批量读取
 *
 * SkipList 模型下
 * 批量写 32w/sec
 * 批量读 8w/sec
 *
 */
public class TestRocksDb {

    static {
        RocksDB.loadLibrary();
    }

    //直接通过Java的get方法
    public static void main(String[] args) throws RocksDBException, InterruptedException {

        //默认设置性能最好
        //get ops 46202.18 requests per second
        //set ops 25489.40 requests per second
        Options options = new Options();
        final BlockBasedTableConfig table_options = new BlockBasedTableConfig();
        final Filter bloomFilter = new BloomFilter(10);

        try {
            options.setCreateIfMissing(true)
                    .setWriteBufferSize(8 * SizeUnit.KB)
                    .setMaxWriteBufferNumber(3)
                    .setMaxBackgroundCompactions(2)
                    .setCompressionType(CompressionType.SNAPPY_COMPRESSION)
                    .setCompactionStyle(CompactionStyle.UNIVERSAL);

            //对于读取性能影响很大
//                    table_options.setBlockCacheSize(64 * SizeUnit.KB)
//                    .setFilter(bloomFilter)
//                    .setCacheNumShardBits(6)
//                    .setBlockSizeDeviation(5)
//                    .setBlockRestartInterval(10)
//                    .setCacheIndexAndFilterBlocks(true)
//                    .setHashIndexAllowCollision(false)
//                    .setBlockCacheCompressedSize(64 * SizeUnit.KB)
//                    .setBlockCacheCompressedNumShardBits(10);

        } catch (final IllegalArgumentException e) {
            assert (false);
        }
//        options.setTableFormatConfig(table_options);

        options.setMemTableConfig(new SkipListMemTableConfig());

        final RocksDB db = RocksDB.open(options, "netty4-server/db/data");


        RocksIterator ri = db.newIterator();
        for (ri.seekToFirst(); ri.isValid(); ri.next()) {
           db.delete(ri.key());
        }


//        Longs.asList()

//        Long.BYTES;

        long[] abc = {1,2,3,0,-1,-2,-3};
        List<Long> longs = Longs.asList(abc);
        Collections.sort(longs);

//        Doubles.BYTES


        for (Long aLong : longs) {
            System.out.println(aLong);
        }

//        db1.close();

//        Thread.sleep(1000);

//        final RocksDB db = RocksDB.open(options, "netty4-server/db/data");

//        System.out.println( new String(db.get("0x1".getBytes())));
//        System.out.println( new String(db.get("5x1".getBytes())));



        //a        :0
        //a       :1
        //a       :2
        //a��������:-3
        //a��������:-2
        //a��������:-1

        ByteBuf buf= Unpooled.buffer(8);
        buf.writeBytes("+".getBytes());
        buf.writeLong(-1);

        ByteBuf buf3= Unpooled.buffer(8);
        buf3.writeBytes("+".getBytes());
        buf3.writeLong(-2);

        ByteBuf buf4= Unpooled.buffer(8);
        buf4.writeBytes("+".getBytes());
        buf4.writeLong(-3);

        ByteBuf buf1= Unpooled.buffer(8);
        buf1.writeBytes("-".getBytes());
        buf1.writeLong(1);

        ByteBuf buf2= Unpooled.buffer(8);
        buf2.writeBytes("-".getBytes());
        buf2.writeLong(2);

        ByteBuf buf5= Unpooled.buffer(8);
        buf5.writeBytes("-".getBytes());
        buf5.writeLong(0);

        db.put(buf.readBytes(buf.readableBytes()).array(),"-1".getBytes());
        db.put(buf1.readBytes(buf1.readableBytes()).array(),"1".getBytes());
        db.put(buf2.readBytes(buf2.readableBytes()).array(),"2".getBytes());
        db.put(buf3.readBytes(buf3.readableBytes()).array(),"-2".getBytes());
        db.put(buf4.readBytes(buf4.readableBytes()).array(),"-3".getBytes());
        db.put(buf5.readBytes(buf5.readableBytes()).array(),"0".getBytes());


        //s-1:s-1
        //s-2:s-2
        //s-3:s-3
        //s1:s1
        //s2:s2
        //s3:s3
//
//        db.put("s1".getBytes(),"s1".getBytes());
//        db.put("s2".getBytes(),"s2".getBytes());
//        db.put("s3".getBytes(),"s3".getBytes());
//        db.put("s-3".getBytes(),"s-3".getBytes());
//        db.put("s-2".getBytes(),"s-2".getBytes());
//        db.put("s-1".getBytes(),"s-1".getBytes());




        //        :0
        //       :1
        //       :2
        //       :3
        //��������:-3
        //��������:-2
        //��������:-1

        System.out.println(System.currentTimeMillis());

        db.put("aa".getBytes(),"aa".getBytes());
        db.put("ab".getBytes(),"ab".getBytes());
        db.put("ac".getBytes(),"ac".getBytes());
        db.put("ad".getBytes(),"ad".getBytes());
        db.put("az".getBytes(),"az".getBytes());
        db.put("aZ".getBytes(),"aZ".getBytes());


        db.put("#".getBytes(),"#".getBytes());
        db.put("$".getBytes(),"$".getBytes());
        db.put("*".getBytes(),"*".getBytes());
        db.put("-".getBytes(),"-".getBytes());
        db.put("+".getBytes(),"+".getBytes());


//        db.put(Longs.toByteArray(0L),numToBytes(0));
//        db.put(Longs.toByteArray(1L),numToBytes(1));
//        db.put(Longs.toByteArray(2L),numToBytes(2));
//        db.put(Longs.toByteArray(3L),numToBytes(3));
//        db.put(Longs.toByteArray(-1L),numToBytes(-1));
//        db.put(Longs.toByteArray(-2L),numToBytes(-2));
//        db.put(Longs.toByteArray(-3L),numToBytes(-3));


        //        :0
        //       :1
        //       :2
        //       :3
        //��������:-3
        //��������:-2
        //��������:-1

//        db.put(long2Bytes(0),numToBytes(0));
//        db.put(long2Bytes(1),numToBytes(1));
//        db.put(long2Bytes(2),numToBytes(2));
//        db.put(long2Bytes(3),numToBytes(3));
//        db.put(long2Bytes(-1),numToBytes(-1));
//        db.put(long2Bytes(-2),numToBytes(-2));
//        db.put(long2Bytes(-3),numToBytes(-3));


        //        :0
        //       :1
        //       :2
        //       :3
        //��������:-3
        //��������:-2
        //��������:-1

//        db.put(longToBytes(0),numToBytes(0));
//        db.put(longToBytes(1),numToBytes(1));
//        db.put(longToBytes(2),numToBytes(2));
//        db.put(longToBytes(3),numToBytes(3));
//        db.put(longToBytes(-1),numToBytes(-1));
//        db.put(longToBytes(-2),numToBytes(-2));
//        db.put(longToBytes(-3),numToBytes(-3));


        //        :0
        //?�      :1
        //@       :2
        //      :3
        //��      :-1
        //�       :-2
        //      :-3
//
//        db.put(doubleToBytes(0),numToBytes(0));
//        db.put(doubleToBytes(1),numToBytes(1));
//        db.put(doubleToBytes(2),numToBytes(2));
//        db.put(doubleToBytes(3),numToBytes(3));
//        db.put(doubleToBytes(-1),numToBytes(-1));
//        db.put(doubleToBytes(-2),numToBytes(-2));
//        db.put(doubleToBytes(-3),numToBytes(-3));


//-1:-1
//-2:-2
//-3:-3
//0:0
//1:1
//2:2
//3:3

//        db.put(numToBytes(0),numToBytes(0));
//        db.put(numToBytes(1),numToBytes(1));
//        db.put(numToBytes(2),numToBytes(2));
//        db.put(numToBytes(3),numToBytes(3));
//        db.put(numToBytes(-1),numToBytes(-1));
//        db.put(numToBytes(-2),numToBytes(-2));
//        db.put(numToBytes(-3),numToBytes(-3));



        System.out.println("&&&&&&&&&&&&&&&&&&&&&&&");

        for (ri.seekToFirst(); ri.isValid(); ri.next()) {
//            System.out.println(Longs.fromByteArray(ri.key()) + ":" + new String(ri.value()));
            System.out.println(new String(ri.key()) + ":" + new String(ri.value()));
        }
        System.out.println("======================");



        for (ri.seek("a".getBytes()); ri.isValid(); ri.next()) {
//            for (ri.seek("a".getBytes()); ri.isValid(); ri.prev()) {
//            for (ri.seekForPrev("a".getBytes()); ri.isValid(); ri.prev()) {
            System.out.println(new String(ri.key()) + ":" + new String(ri.value()));

        }

        System.out.println("----------------------");

//        ri.seekToLast();
        //关键字后面+Z
        for (ri.seekForPrev("az".getBytes()); ri.isValid(); ri.prev()) {
//            for (ri.seek("a".getBytes()); ri.isValid(); ri.prev()) {
//            for (ri.seekForPrev("a".getBytes()); ri.isValid(); ri.prev()) {
            System.out.println(new String(ri.key()) + ":" + new String(ri.value()));

        }

//        testPut(db);
        final String str1 = db.getProperty("rocksdb.stats");
//        System.out.println(str1);




//        testGet(db);
        final String str2 = db.getProperty("rocksdb.stats");
//        System.out.println(str2);

        db.close();


    }


    private static ByteBuffer buffer = ByteBuffer.allocate(8);
    //byte 数组与 long 的相互转换
    public static byte[] longToBytes(long x) {
        buffer.putLong(0, x);
        return buffer.array();
    }

    public static byte[] doubleToBytes(double x) {
        buffer.putDouble(0, x);
        return buffer.array();
    }

    public static long bytesToLong(byte[] bytes) {
        buffer.put(bytes, 0, bytes.length);
        buffer.flip();//need flip
        return buffer.getLong();
    }

    public static byte[] long2Bytes(long num) {
        byte[] byteNum = new byte[8];
        for (int ix = 0; ix < 8; ++ix) {
            int offset = 64 - (ix + 1) * 8;
            byteNum[ix] = (byte) ((num >> offset) & 0xff);
        }
        return byteNum;
    }


    public static long bytes2Long(byte[] byteNum) {
        long num = 0;
        for (int ix = 0; ix < 8; ++ix) {
            num <<= 8;
            num |= (byteNum[ix] & 0xff);
        }
        return num;
    }



    /**
     * 引入批量数据处理
     * @param db
     */
    private static void testPut(final RocksDB db) {
        long start = System.currentTimeMillis();

        final AtomicInteger atomicInteger = new AtomicInteger(0);
        final CountDownLatch countDownLatch = new CountDownLatch(16);
        ExecutorService executorService = Executors.newCachedThreadPool();

        try {

            for (int i = 0; i < 16; i++) {

                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
//                        批量写
                        final WriteOptions writeOpt = new WriteOptions();
                        for (int j = 0; j <= 10; ++j) {

                            try (final WriteBatch batch = new WriteBatch()) {
                                for (int k = 0; k <= 80000; ++k) {   //处理287350条记录/秒
                                    atomicInteger.incrementAndGet();

                                    batch.put(String.format("%dx%d", j, k).getBytes(),
                                            String.format("%d", j * k).getBytes());
                                }
                                db.write(writeOpt, batch); //批量数据存储

//                                byte[] key1 = String.format("%dx%d", j, 1).getBytes();
//                                byte[] val = db.get(key1);
//                                System.out.println(String.format("%s %s",new String(key1),new String(val) ));


                            } catch (RocksDBException e) {
                                e.printStackTrace();
                            }

                        }

//                        //每个线程增加100000次，每次加1
//                        for (int j = 0; j < 100000; j++) {
//                            atomicInteger.incrementAndGet();
//                            String tid = String.valueOf(Thread.currentThread().getId());
//                            try {
//                                db.put((finalI + j+"").getBytes(), (tid + finalI + j).getBytes());
//                            } catch (RocksDBException e) {
//                                e.printStackTrace();
//                            }
//                        }

                        countDownLatch.countDown();
                    }
                });
            }
            System.out.println("**************************TestPut****************************************");
            System.out.println(countDownLatch.getCount());

            countDownLatch.await();

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // 关闭线程池
        executorService.shutdown();


        long end = System.currentTimeMillis();

        System.out.println("处理" + atomicInteger + "条记录");
        System.out.println("处理" + atomicInteger.intValue() + "条记录");
        System.out.println(String.format("having times %d毫秒:", (end - start)));
        System.out.println(String.format("having times %d秒:", (end - start) / 1000));
        System.out.println(String.format("having times %d分钟:", (end - start) / 1000 / 60));
        System.out.println(String.format("having times %d个/毫秒:", atomicInteger.intValue() / (end - start)));
        System.out.println("处理" + (atomicInteger.intValue() / ((end - start) / 1000)) + "条记录/秒");
    }

    /**
     * 引入批量数据读取
     * @param db
     */
    private static void testGet(final RocksDB db) {
        long start = System.currentTimeMillis();

        final AtomicInteger atomicInteger = new AtomicInteger(0);
        final CountDownLatch countDownLatch = new CountDownLatch(16);
        ExecutorService executorService = Executors.newCachedThreadPool();

        try {
            for (int i = 0; i < 16; i++) {

                executorService.submit(new Runnable() {
                    @Override
                    public void run() {

                        //每个线程增加100000次，每次加1
                        for (int j = 0; j < 10; j++) {

                            List<byte[]> list = new ArrayList<>();
                            for (int k = 0; k < 20000; k++) {

                                atomicInteger.incrementAndGet();

                                byte[] key = String.format("%dx%d", j, k).getBytes();

                                list.add(key);

//                                String tid = String.valueOf(Thread.currentThread().getId());
//                                try {
//                                    db.get((finalI + j+"").getBytes());
//                                } catch (RocksDBException e) {
//                                    e.printStackTrace();
//                                }
                            }

                            Map<byte[], byte[]> values = null;
                            try {
                                values = db.multiGet(list); //批量数据读取

                                for (byte[] bt:values.values()
                                     ) {
//                                    System.out.println(new String(bt));
                                }

                            } catch (RocksDBException e) {
                                e.printStackTrace();
                            }
//                            System.out.println(values.size());
                        }

                        countDownLatch.countDown();

                    }
                });
            }
            System.out.println("***************************TestGet***************************************");
            System.out.println(countDownLatch.getCount());

            countDownLatch.await();

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // 关闭线程池
        executorService.shutdown();

        long end = System.currentTimeMillis();

        System.out.println("处理" + atomicInteger + "条记录");
        System.out.println("处理" + atomicInteger.intValue() + "条记录");
        System.out.println(String.format("having times %d毫秒:", (end - start)));
        System.out.println(String.format("having times %d秒:", (end - start) / 1000));
        System.out.println(String.format("having times %d分钟:", (end - start) / 1000 / 60));
        System.out.println(String.format("having times %d个/毫秒:", atomicInteger.intValue() / (end - start)));
        System.out.println("处理" + (atomicInteger.intValue() / ((end - start) / 1000)) + "条记录/秒");
    }
}
