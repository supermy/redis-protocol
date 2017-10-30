package redis.server.netty;

import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

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
    public static void main(String[] args) throws RocksDBException {

        //默认设置性能最好
        //get ops 46202.18 requests per second
        //set ops 25489.40 requests per second
        Options options = new Options();

        try {
            options.setCreateIfMissing(true)
                    .setWriteBufferSize(8 * SizeUnit.KB)
                    .setMaxWriteBufferNumber(3)
                    .setMaxBackgroundCompactions(2)
                    .setCompressionType(CompressionType.SNAPPY_COMPRESSION)
                    .setCompactionStyle(CompactionStyle.UNIVERSAL);
        } catch (final IllegalArgumentException e) {
            assert (false);
        }

        options.setMemTableConfig(new SkipListMemTableConfig());

        final RocksDB db = RocksDB.open(options, "netty4-server/db/data");



        System.out.println( new String(db.get("0x1".getBytes())));
        System.out.println( new String(db.get("5x1".getBytes())));



        testPut(db);
        final String str1 = db.getProperty("rocksdb.stats");
        System.out.println(str1);




        testGet(db);
        final String str2 = db.getProperty("rocksdb.stats");
        System.out.println(str2);

        db.close();


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
