package com.bigdata;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import net.agkn.hll.HLL;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * 对于海量数据来说，数据内存占用会变得很高. Probabilistic数据结构牺牲了一下准确率去换取更低内存占用。
 * 比如一个HyperLogLog的数据结构只需要花费12KB内存，就可以计算接近2^64个不同元素的基数，而错误率在1.625%.
 *
 * HyperLogLog 在计数比较小时，它的存储空间采用稀疏矩阵存储，空间占用很小，仅仅在计数慢慢变大，稀疏矩阵占用空间渐渐超过了阈值时会一次性
 * 转变成稠密矩阵，会占用 12k 的空间。因此不适合统计单个用户相关的数据。
 *
 * HyperLogLog这个数据结构相对set的好处，就在于统计大数据量情况下，比较节省空间，但是统计值是有误差的，并且只提供pfadd，只能递增，不能递减。
 *
 * HyperLogLog特别是适合用来对海量数据进行unique统计，对内存占用有要求，而且还能够接受一定的错误率的场景。对于union操作由于是O(N)，在海量数据层面需要注意慢查询问题。
 *
 * 和bitmap相比，属于两种特定统计情况，简单来说，HyperLogLog 去重比 bitmap 方便很多
 * 一般可以bitmap和hyperloglog配合使用，bitmap标识哪些用户活跃，hyperloglog计数
 * 一般使用：
 *
 * 统计注册 IP 数
 * 统计每日访问 IP 数
 * 统计页面实时 UV 数
 * 统计在线用户数
 * 统计用户每天搜索不同词条的个数
 *
 */
public class HyperLogLogTest {
    final int seed = 123456;
    HashFunction hash = Hashing.murmur3_128(seed);

    @Test
    public void testSimpleUse(){
        final int seed = 123456;
        HashFunction hash = Hashing.murmur3_128(seed);
        // data on which to calculate distinct count
        final Integer[] data = new Integer[]{1, 1, 2, 3, 4, 5, 6, 6, 6, 7, 7, 7, 7, 8, 10};
        final HLL hll = new HLL(13, 5); //number of bucket and bits per bucket;桶数与桶的位数
        for (int item : data) {
            final long value = hash.newHasher().putInt(item).hash().asLong();
            hll.addRaw(value);
        }

        System.out.println("Distinct count="+ hll.cardinality());

        byte[] bytes = hll.toBytes();
        HLL hll1 = HLL.fromBytes(bytes);

        System.out.println("HLL from bytes:Distinct count="+ hll1.cardinality());

    }

    /**
     * 更快，占用内存更少，误差率为5%
     * Total count:[100000]  Unique count:[94518] FreeMemory:[116628K] ..
     * Total cost:[130] ms ..
     *
     * Total count:[100000]  Unique count:[99193] FreeMemory:[97325K] ..
     * Total cost:[173] ms ..
     *
     * @throws IOException
     */
    @Test
    public void testSimpleUse1() throws IOException {
        Runtime rt = Runtime.getRuntime();
        long start = System.currentTimeMillis();
        long count = 0l;

        HLL hll = new HLL(13,5);
        for (int i = 0; i < 100000; i++) {
            long hashedValue = hash.newHasher().putInt(i).hash().asLong();
            hll.addRaw(hashedValue);

            //hll.addRaw(i);

            count++;

            if (i%5000==1) {
            System.out.println("Total count:[" + count +
                    "]  Unique count:[" + hll.cardinality() +
                    "] FreeMemory:[" + rt.freeMemory()/1024 + "K] ..");
            }
        }
        System.out.println(hll.cardinality());

        long end = System.currentTimeMillis();
        System.out.println("Total count:[" + count +
                "]  Unique count:[" + hll.cardinality() +
                "] FreeMemory:[" + rt.freeMemory()/1024 + "K] ..");

        System.out.println("Total cost:[" + (end - start) + "] ms ..");


        byte[] bytes = hll.toBytes();
        HLL hll1 = HLL.fromBytes(bytes);

        System.out.println("HLL from bytes:Distinct count="+ hll1.cardinality());

    }


    /**
     *
     * stream_lib
     *
     * @throws IOException
     */
    @Test
    public void testSerializationUsingBuilder() throws IOException {
        HyperLogLog hll = new HyperLogLog(8);
        hll.offer("a");
        hll.offer("b");
        hll.offer("c");
        hll.offer("d");
        hll.offer("e");

        HyperLogLog hll2 = HyperLogLog.Builder.build(hll.getBytes());
        assertEquals(hll.cardinality(), hll2.cardinality());

    }

    /**
     * 效率与误差率占优
     *
     * 误差率为1%;随着计算增加内存；
     * Total count:[100000]  Unique count:[99829] FreeMemory:[99988K] ..
     * Total cost:[152] ms ..
     *
     * Long类型
     * Total count:[100000]  Unique count:[99439] FreeMemory:[98656K] ..
     * Total cost:[144] ms ..
     *
     * @throws IOException
     */
    @Test
    public void testSerialization1_Normal() throws IOException {
        Runtime rt = Runtime.getRuntime();
        long start = System.currentTimeMillis();
        long count = 0l;



        HyperLogLog hll = new HyperLogLog(12);
        for (int i = 0; i < 100000; i++) {
            hll.offer("" + i);
//            long hashedValue = hash.newHasher().putInt(i).hash().asLong();
//            hll.offerHashed(hashedValue);
            count++;

            if (i % 5000 == 1) {
                System.out.println("Total count:[" + count +
                        "]  Unique count:[" + hll.cardinality() +
                        "] FreeMemory:[" + rt.freeMemory() / 1024 + "K] ..");
            }
        }
        System.out.println(hll.cardinality());

        long end = System.currentTimeMillis();
        System.out.println("Total count:[" + count +
                "]  Unique count:[" + hll.cardinality() +
                "] FreeMemory:[" + rt.freeMemory() / 1024 + "K] ..");

        System.out.println("Total cost:[" + (end - start) + "] ms ..");


        HyperLogLog hll2 = HyperLogLog.Builder.build(hll.getBytes());

        System.out.println("HLL from bytes:Distinct count=" + hll2.cardinality());

    }

    /**
     * Objet 慢，且内存消耗不稳定 hll.offer(""+i);
     * Total count:[100000]  Unique count:[98247] FreeMemory:[99312K] ..
     * Total cost:[165] ms ..
     *
     * Long  hll.offerHashed(i);
     * Total count:[100000]  Unique count:[99604] FreeMemory:[97884K] ..
     * Total cost:[151] ms ..
     *
     * @throws IOException
     */
    @Test
    public void testSerialization_Normal() throws IOException {

        Runtime rt = Runtime.getRuntime();
        long start = System.currentTimeMillis();
        long count = 0l;

        HyperLogLogPlus hll = new HyperLogLogPlus(13,25);
        for (int i = 0; i < 100000; i++) {
//            hll.offer(""+i);
            long hashedValue = hash.newHasher().putInt(i).hash().asLong();
            hll.offerHashed(hashedValue);

            count++;

            if (i % 5000 == 1) {
                System.out.println("Total count:[" + count +
                        "]  Unique count:[" + hll.cardinality() +
                        "] FreeMemory:[" + rt.freeMemory() / 1024 + "K] ..");
            }
        }
        System.out.println(hll.cardinality());

        long end = System.currentTimeMillis();
        System.out.println("Total count:[" + count +
                "]  Unique count:[" + hll.cardinality() +
                "] FreeMemory:[" + rt.freeMemory() / 1024 + "K] ..");

        System.out.println("Total cost:[" + (end - start) + "] ms ..");


        HyperLogLogPlus hll2 = HyperLogLogPlus.Builder.build(hll.getBytes());
        assertEquals(hll.cardinality(), hll2.cardinality());

    }

    @Test
    public void testSerialization_Sparse() throws IOException {
        HyperLogLogPlus hll = new HyperLogLogPlus(14, 25);
        hll.offer("a");
        hll.offer("b");
        hll.offer("c");
        hll.offer("d");
        hll.offer("e");

        HyperLogLogPlus hll2 = HyperLogLogPlus.Builder.build(hll.getBytes());
        assertEquals(hll.cardinality(), hll2.cardinality());
        System.out.println(hll.cardinality());
    }


    /**
     * 合并测试
     * @throws CardinalityMergeException
     */
    @Test
    public void testMerge() throws CardinalityMergeException {
        HyperLogLog hll01=new HyperLogLog(12);
        HyperLogLog hll02=new HyperLogLog(12);
        hll01.offer("a");
        hll01.offer("b");

        hll02.offer("c");
        hll02.offer("d");

        System.out.println(hll01.cardinality());
        System.out.println(hll02.cardinality());

        hll01.addAll(hll02);
        System.out.println(hll01.cardinality());
        assertEquals(hll01.cardinality(),4);
    }

}
