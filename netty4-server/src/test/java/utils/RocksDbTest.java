package utils;

import com.google.common.base.Strings;
import com.google.common.primitives.Longs;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static redis.util.Encoding.numToBytes;
import org.supermy.util.MyUtils;

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
public class RocksDbTest {
    private static org.apache.log4j.Logger log = Logger.getLogger(RocksDbTest.class);

    static {
        RocksDB.loadLibrary();
    }

    /**
     * 有序主键，//long 转换为byte[]后的排序
     * @throws RocksDBException
     * @throws InterruptedException
     */
    @Test
    public  void long2ByteSort() throws RocksDBException, InterruptedException {

        //默认设置性能最好
        //get ops 46202.18 requests per second
        //set ops 25489.40 requests per second
        final RocksDB db = open();


        RocksIterator ri = db.newIterator();

        db.deleteRange("0".getBytes(),"z".getBytes());
        for (ri.seekToFirst(); ri.isValid(); ri.next()) {
           db.delete(ri.key());
        }

        //sort
        initdata(db);
//        initByteBuf(db);
//        initNumToBytes(db);
        initLongsByteArray(db);


        System.out.println("遍历 &&&&&&&&&&&&&&&&&&&&&&&");

        for (ri.seekToFirst(); ri.isValid(); ri.next()) {
//            System.out.println(Longs.fromByteArray(ri.key()) + ":" + new String(ri.value()));
            System.out.println(new String(ri.key()) + ":" + new String(ri.value()));
        }
        System.out.println("查询 start_a ======================");



        for (ri.seek("start_a".getBytes()); ri.isValid(); ri.next()) {
//            for (ri.seek("start_a".getBytes()); ri.isValid(); ri.prev()) {
//            for (ri.seekForPrev("start_a".getBytes()); ri.isValid(); ri.prev()) {
            System.out.println(new String(ri.key()) + ":" + new String(ri.value()));

        }

        System.out.println("反向查询 start_a ----------------------");

//        ri.seekToLast();
        //关键字后面+Z
        for (ri.seekForPrev("az".getBytes()); ri.isValid(); ri.prev()) {
//            for (ri.seek("start_a".getBytes()); ri.isValid(); ri.prev()) {
//            for (ri.seekForPrev("start_a".getBytes()); ri.isValid(); ri.prev()) {
            System.out.println(new String(ri.key()) + ":" + new String(ri.value()));

        }


    }


    public RocksDB open() throws RocksDBException {
        // file:/E:/workspace/AppServer/target/test-classes/

        String basepath = basePath();

        Options options = new Options();
        final BlockBasedTableConfig table_options = new BlockBasedTableConfig();
        final Filter bloomFilter = new BloomFilter(10);

        try {
            options.setCreateIfMissing(true)

                    .setWriteBufferSize(8 * 1024 * SizeUnit.KB)
                    .setMaxWriteBufferNumber(3)
                    .setMaxBackgroundCompactions(2)
                    .setCompressionType(CompressionType.SNAPPY_COMPRESSION)
                    .setCompactionStyle(CompactionStyle.LEVEL);
//                    .setCompactionStyle(CompactionStyle.UNIVERSAL);


            //对于读取性能影响很大
                    table_options.setBlockCacheSize(64 * SizeUnit.KB)
//                    .setFilter(bloomFilter)
//                    .setCacheNumShardBits(6)
//                    .setBlockSizeDeviation(5)
//                    .setBlockRestartInterval(10)
                    .setCacheIndexAndFilterBlocks(true)
//                    .setHashIndexAllowCollision(false)
                    .setBlockCacheCompressedSize(64 * SizeUnit.KB)
                    .setBlockCacheCompressedNumShardBits(10);

        } catch (final IllegalArgumentException e) {
            assert (false);
        }
//        options.setTableFormatConfig(table_options);

        options.setMemTableConfig(new SkipListMemTableConfig());

        return RocksDB.open(options, basepath+"db/data");
    }


    /**
     * 项目路径
     *
     * @param url
     * @return
     */
    public String basePath() {
        URL url = this.getClass().getResource("/");

        String path = url.getPath();

        System.out.println(path);

        String basepath = path.substring(0,path.indexOf("target"));

        System.out.println(basepath);
        return basepath;
    }

    /**
     *  //默认设置性能最好
     *         //get ops 46202.18 requests per second
     *         //set ops 25489.40 requests per second
     *
     * @param db
     * @throws RocksDBException
     */
    @Test
    public void testGet() throws RocksDBException {
        final RocksDB db = open();

        testGet(db);
        final String str2 = db.getProperty("rocksdb.stats");
        System.out.println(str2);

        db.close();
    }

    /**
     * RocksDb在大value有效率问题，减小value的策略包括分割与压缩，配置优化参数；
     *
     *
     * 14:48:30,798 DEBUG RocksDbTest:193 - 压缩时间：32毫秒，压缩前：9.0MB,压缩后：432.57KB, 占比：4.69%
     * 14:48:30,814 DEBUG RocksDbTest:202 - 解压时间：12毫秒,解压前：432.57KB,解压后：9.0MB, 占比：4.69%
     * 14:49:00,963 DEBUG RocksDbTest:214 - 平均单个压缩时间：3毫秒
     * 14:50:24,207 DEBUG RocksDbTest:221 - 平均单个解压时间：8毫秒
     *
     * @throws RocksDBException
     * @throws IOException
     */
    @Test
    public void snappy() throws RocksDBException, IOException {
        //数据准备
        byte[] bigStr= Strings.repeat("abc",3*1024*1024).getBytes();
        byte[] bigStrCompress=null;
        byte[] bigStrUnCompress=null;

        {
            //单个压缩时间
            long start = System.currentTimeMillis();
            bigStrCompress= Snappy.compress(bigStr);
            log.debug(String.format("压缩时间：%s毫秒，压缩前：%s,压缩后：%s, 压缩比：%s",
                    (System.currentTimeMillis()-start),
                    MyUtils.bytes2kb(bigStr.length),
                    MyUtils.bytes2kb(bigStrCompress.length),
                    MyUtils.percent(bigStrCompress.length,bigStr.length,2)));
        }

        {
            //单个解压时间
            long start = System.currentTimeMillis();
            bigStrUnCompress= Snappy.uncompress(bigStrCompress);
            log.debug(String.format("解压时间：%s毫秒,解压前：%s,解压后：%s, 压缩比：%s",
                    (System.currentTimeMillis()-start),
                    MyUtils.bytes2kb(bigStrCompress.length),
                    MyUtils.bytes2kb(bigStrUnCompress.length),
                    MyUtils.percent(bigStrCompress.length,bigStrUnCompress.length,2)));
            Assert.assertArrayEquals(bigStr,bigStrUnCompress);
        }


        {
            //平均压缩时间
            long start1 = System.currentTimeMillis();
            for (int i = 0; i <10000 ; i++) {
                bigStrCompress = Snappy.compress(bigStr);
            }
            log.debug(String.format("平均单个压缩时间：%s毫秒", (System.currentTimeMillis()-start1)/10000));
        }

        {
            //平均解压时间
            long start2 = System.currentTimeMillis();
            for (int i = 0; i <10000 ; i++) {
                bigStrUnCompress=Snappy.uncompress(bigStrCompress);
            }
            log.debug(String.format("平均单个解压时间：%s毫秒", (System.currentTimeMillis()-start2)/10000));
        }




    }


    /**
     * RocksDb put get have times
     *
     * 18:57:39,367 DEBUG RocksDbTest:304 - 小数据，共put 10000 个时间489毫秒;平均单个value(大小3.0KB)put时间：49微秒
     * 18:58:46,632 DEBUG RocksDbTest:317 - 小数据，共put 100,0000 个时间67,256毫秒;平均单个value(大小3.0KB)put时间：67微秒
     * put 100万大数据，需要一个小时，批量put BloomFilter批量put
     * 18:59:24,611 DEBUG RocksDbTest:330 - 大数据，共put 10000 个时间37,957毫秒,平均单个value(大小432.57KB)put时间：3毫秒
     *
     * 18:59:24,805 DEBUG RocksDbTest:345 - 小数据，共get 10000 个时间193毫秒;平均单个value(大小3.0KB)get时间：19微秒
     * 18:59:38,264 DEBUG RocksDbTest:361 - 小数据，共get 100,0000 个时间13,457毫秒;平均单个value(大小3.0KB)get时间：13微秒
     * get 100万大数据，需要8分钟,单独get BloomFilter单独get
     * 18:59:44,154 DEBUG RocksDbTest:376 - 大数据，共get 10000 个时间5,886毫秒,平均单个value(大小432.57KB)get时间：588微秒
     *
     * @throws RocksDBException
     * @throws IOException
     */
    @Test
    public void testGetPutBigData() throws RocksDBException, IOException {
        final RocksDB db = open();

        long count=10000;
        long countBig=1000000;

        byte[] smallStr= Strings.repeat("abc",1*1024).getBytes();

        byte[] bigStr= Strings.repeat("abc",3*1024*1024).getBytes();
        byte[] bigStrCompress= Snappy.compress(bigStr);

        {
            //单个数据测试
            db.put("bigdata1".getBytes(),bigStrCompress);
            byte[] bigdata1 = db.get("bigdata1".getBytes());
            Assert.assertArrayEquals(bigdata1,bigStrCompress);
        }

        {
            //清理数据
            db.deleteRange("bigdata".getBytes(),"bigdztaz".getBytes());
            byte[] bigdata1 = db.get("bigdata1".getBytes());
            Assert.assertNull(bigdata1);
        }


        {
            //小数据put
            long start0=System.nanoTime();
            for (int i = 0; i <count ; i++) {
                db.put(("bigdata_sma"+i).getBytes(),smallStr);
            }
            log.debug(String.format("小数据，共put %s 个时间%s毫秒;平均单个value(大小%s)put时间：%s微秒",
                    count,
                    (System.nanoTime()-start0)/1000/1000,
                    MyUtils.bytes2kb(smallStr.length),
                    (System.nanoTime()-start0)/1000/count));
        }

        {
            //小数据put
            long start0=System.nanoTime();
            for (int i = 0; i < countBig; i++) {
                db.put(("bigdata_smabig"+i).getBytes(),smallStr);
            }
            log.debug(String.format("小数据，共put %s 个时间%s毫秒;平均单个value(大小%s)put时间：%s微秒",
                    countBig,
                    (System.nanoTime()-start0)/1000/1000,
                    MyUtils.bytes2kb(smallStr.length),
                    (System.nanoTime()-start0)/1000/countBig));
        }

        {
            //大数据put
            long start=System.currentTimeMillis();
            for (int i = 0; i <count ; i++) {
                db.put(("bigdata_big"+i).getBytes(),bigStrCompress);
            }
            log.debug(String.format("大数据，共put %s 个时间%s毫秒,平均单个value(大小%s)put时间：%s毫秒",
                    count,
                    (System.currentTimeMillis()-start),
                    MyUtils.bytes2kb(bigStrCompress.length),
                    (System.currentTimeMillis()-start)/count));

        }

        {
            //小数据get
            long start0=System.nanoTime();
            byte[] smallStr1=null;
            for (int i = 0; i <count ; i++) {
                smallStr1=db.get(("bigdata_sma"+i).getBytes());
            }
            log.debug(String.format("小数据，共get %s 个时间%s毫秒;平均单个value(大小%s)get时间：%s微秒",
                    count,
                    (System.nanoTime()-start0)/1000/1000,
                    MyUtils.bytes2kb(smallStr.length),
                    (System.nanoTime()-start0)/1000/count));
//            log.debug(String.format("source:%s;dest:%s",new String(smallStr),new String(smallStr1)));
            Assert.assertArrayEquals(smallStr,smallStr1);
        }

        {
            //小数据get
            long start0=System.nanoTime();
            byte[] smallStr1 = null;
            for (int i = 0; i <countBig ; i++) {
                smallStr1=db.get(("bigdata_smabig"+i).getBytes());
            }
            log.debug(String.format("小数据，共get %s 个时间%s毫秒;平均单个value(大小%s)get时间：%s微秒",
                    countBig,
                    (System.nanoTime()-start0)/1000/1000,
                    MyUtils.bytes2kb(smallStr.length),
                    (System.nanoTime()-start0)/1000/countBig));
//            log.debug(String.format("source:%s;dest:%s",new String(smallStr),new String(smallStr1)));
            Assert.assertArrayEquals(smallStr,smallStr1);
        }

        {
            //大数据get
            long start=System.nanoTime();
            for (int i = 0; i <count ; i++) {
                bigStrCompress=db.get(("bigdata_big"+i).getBytes());
            }
            log.debug(String.format("大数据，共get %s 个时间%s毫秒,平均单个value(大小%s)get时间：%s微秒",
                    count,
                    (System.nanoTime()-start)/1000000,
                    MyUtils.bytes2kb(bigStrCompress.length),
                    (System.nanoTime()-start)/count/1000));

            Assert.assertArrayEquals(bigStr,Snappy.uncompress(bigStrCompress));

        }

//
//        long start1=System.currentTimeMillis();
//        for (int i = 0; i <10000 ; i++) {
//            db.get(("bigdata"+i).getBytes());
//        }
//        log.debug(System.currentTimeMillis()-start1);

        final String str2 = db.getProperty("rocksdb.stats");
        log.debug(str2);

        db.close();
    }



    /**
     * 多线程
     * //默认设置性能最好
     *      *         //get ops 46202.18 requests per second
     *      *         //set ops 25489.40 requests per second
     *
     * @throws RocksDBException
     */
    @Test
    public void testPut() throws RocksDBException {
        final RocksDB db = open();

        testPut(db);
        final String str1 = db.getProperty("rocksdb.stats");
        System.out.println(str1);
        db.close();

    }

    public void initNumToBytes(RocksDB db) throws RocksDBException {
        //-1:-1
//-2:-2
//-3:-3
//0:0
//1:1
//2:2
//3:3

        db.put(numToBytes(0),numToBytes(0));
        db.put(numToBytes(1),numToBytes(1));
        db.put(numToBytes(2),numToBytes(2));
        db.put(numToBytes(3),numToBytes(3));
        db.put(numToBytes(-1),numToBytes(-1));
        db.put(numToBytes(-2),numToBytes(-2));
        db.put(numToBytes(-3),numToBytes(-3));
    }


    public void initLongsByteArray(RocksDB db) throws RocksDBException {
        //        :0
        //       :1
        //       :2
        //       :3
        //��������:-3
        //��������:-2
        //��������:-1


        db.put(Longs.toByteArray(0L),numToBytes(0));
        db.put(Longs.toByteArray(1L),numToBytes(1));
        db.put(Longs.toByteArray(2L),numToBytes(2));
        db.put(Longs.toByteArray(3L),numToBytes(3));
        db.put(Longs.toByteArray(-1L),numToBytes(-1));
        db.put(Longs.toByteArray(-2L),numToBytes(-2));
        db.put(Longs.toByteArray(-3L),numToBytes(-3));
    }

    public void initdata(RocksDB db) throws RocksDBException {
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
    }

    public void initByteBuf(RocksDB db) throws RocksDBException {
        //a        :0
        //a       :1
        //a       :2
        //start_a��������:-3
        //start_a��������:-2
        //start_a��������:-1

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
    }

    @Test
    public void longSort() {
        long[] abc = {1,2,3,0,-1,-2,-3};
        List<Long> longs = Longs.asList(abc);
        Collections.sort(longs);

//        Doubles.BYTES


        for (Long aLong : longs) {
            System.out.println(aLong);
        }

        Assert.assertEquals(longs.get(0).longValue(),-3);
        Assert.assertEquals(longs.get(6).longValue(),3);
    }




    /**
     * 多线程,批次数据处理，一批数据8万
     *
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
     * 多线程，批次数据处理，一批数据20000
     *
     *
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
