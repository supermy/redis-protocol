package com.bigdata;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.esotericsoftware.kryo.io.KryoDataOutput;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;
import com.google.common.primitives.Longs;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.supermy.util.MyUtils;
import org.xerial.snappy.Snappy;

import java.io.*;
import java.net.URL;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * 简介：布隆过滤器实际上是一个很长的二进制向量和一系列随机映射函数。布隆过滤器可以用于检索一个元素是否在一个集合中。它的优点是空间效率和查询时间都远远超过一般的算法，缺点是有一定的误识别率和删除困难。
 * 原理：当一个元素被加入集合时，通过K个散列函数将这个元素映射成一个位数组中的K个点，把它们置为1。检索时，我们只要看看这些点是不是都是1就（大约）知道集合中有没有它了：如果这些点有任何一个0，则被检元素一定不在；如果都是1，则被检元素很可能在。
 * 优点：相比于其它的数据结构，布隆过滤器在空间和时间方面都有巨大的优势。布隆过滤器存储空间和插入/查询时间都是常数（O(k)）。而且它不存储元素本身，在某些对保密要求非常严格的场合有优势。
 * 缺点：一定的误识别率和删除困难。
 * 结合以上几点及去重需求（容忍误判，会误判在，在则丢，无妨），决定使用BlomFilter。
 * <p>
 *     目前已经有相应实现的开源类库，如Google的Guava类库，Twitter的Algebird类库，和ScalaNLP breeze等等，其中Guava 11.0版本中增加了
 *     BloomFilter类，它使用了Funnel和Sink的设计，增强了泛化的能力，使其可以支持任何数据类型，其利用murmur3 hash来做哈希映射函数，不过
 *     它底层并没有使用传统的java.util.BitSet来做bit数组，而是用long型数组进行了重新封装，大部分操作均基于位的运算，因此能达到一个非常好
 *     的性能；下面我们就Guava类库中实现布隆过滤器的源码作详细分析，最后出于灵活性和解耦等因素的考虑，我们想要把布隆过滤器从JVM中拿出来，
 *     于是利用了Redis自带的Bitmaps作为底层的bit数组进行重构，另外随着插入的元素越来越多，当实际数量远远大于创建时设置的预计数量时，布隆
 *     过滤器的误判率会越来越高，因此在重构的过程中增加了自动扩容的特性，最后通过测试验证其正确性。
 * *
 */
public class BloomFilterTest {
    private static Logger log = Logger.getLogger(BloomFilterTest.class);


    @Test
    public void rocksdb() throws IOException {

    }

        /**
         * BloomFilter的关键在于hash算法的设定和bit数组的大小确定，通过权衡得到一个错误概率可以接受的结果。
         * <p>
         * 要存下这一百万个数，位数组的大小是7298440，700多万位，实际上要完整存下100万个数，一个int是4字节32位，我们需要4X8X1000000=3千2百万位，
         * 差不多只用了1/5的容量，如果是HashMap，按HashMap 50%的存储效率，我们需要6千4百万位，所有布隆过滤器占用空间很小，只有HashMap的1/10-1/5作用。
         * <p>
         * 预估数据量100w，错误率需要减小到万分之一。使用如下代码进行创建。
         * 根据《数学之美》中给出的数据，在使用8个哈希函数的情况下，512MB大小的位数组在误报率万分之五的情况下可以对约两亿的url去重。而若单纯的使用set()去重的话，以一个url64个字节记，两亿url约需要128GB的内存空间,不敢想象。
         */
    @Test
    public void basic() throws IOException {

        long expectedInsertions = 1000000;//预估数量
        double fpp = 0.0001;//错误率

// 1. 创建符合条件的布隆过滤器
//        BloomFilter<String> filter = BloomFilter.create(Funnels.stringFunnel(Charsets.UTF_8), expectedInsertions, fpp);
        BloomFilter<byte[]> filter = BloomFilter.create(Funnels.byteArrayFunnel(), expectedInsertions, fpp);

//2. 将一部分数据添加进去
        {
            long start=System.currentTimeMillis();

            for (int index = 0; index < expectedInsertions; index++) {
                filter.put(("abc_test_" + index).getBytes());
            }

            log.debug(String.format("加载%s个数据，花费时间:%s毫秒",expectedInsertions,System.currentTimeMillis()-start));

            System.out.println("write all...");
        }


        //数据持久化到本地  bloomfilter.data 2.4Mb
        File f= new File(basePath("util") + File.separator + "bloomfilter.data");
        OutputStream out =  new FileOutputStream(f);

        try {

            {
                long start = System.currentTimeMillis();

                filter.writeTo(out);

                log.debug(String.format("%s个数据，持久化到硬盘(size=%s)，花费时间:%s毫秒", 1, MyUtils.bytes2kb(f.length()), System.currentTimeMillis() - start));
            }

            /**
             * 21:09:05,280 DEBUG BloomFilterTest:74 - 加载1000000个数据，花费时间:1835毫秒
             * write all...
             * 21:09:06,938 DEBUG BloomFilterTest:91 - 1个数据，持久化到硬盘(size=2.29MB)，花费时间:1651毫秒
             * 21:09:06,996 DEBUG BloomFilterTest:102 - 原来大小：2396278 ；压缩后大小：2396393
             * 21:09:06,997 DEBUG BloomFilterTest:104 - 1个数据，持久化到内存(size=2.29MB)，花费时间:58毫秒
             * 21:09:07,579 DEBUG BloomFilterTest:126 - 1个数据，从磁盘文件(size=2.29MB)恢复数据，花费时间:582毫秒
             * 21:09:08,196 DEBUG BloomFilterTest:149 - 判定1000000个数据，花费时间:617毫秒
             * 错杀：1
             * 21:09:08,227 DEBUG BloomFilterTest:163 - 判定1000000个数据，花费时间:30毫秒
             *
             * 21:41:13,286 DEBUG BloomFilterTest:95 - 1个数据，持久化到硬盘(size=2.29MB)，花费时间:1468毫秒
             * 21:41:13,330 DEBUG BloomFilterTest:128 - 原来大小：118 ；压缩后大小：121
             * 21:41:13,331 DEBUG BloomFilterTest:130 - 1个数据，持久化到内存(size=0.12KB)，花费时间:44毫秒
             *
             */

            {


                ByteArrayOutputStream bao=new ByteArrayOutputStream();

                long start=System.currentTimeMillis();

                filter.writeTo(bao);

                filter.writeToCompress(bao);


                byte[] bytes = bao.toByteArray();
//                String ba1=Base64.getEncoder().encodeToString(bytes);
//                System.out.println(ba1.length());
//                System.out.println(ba1);
                byte[] compress = Snappy.compress(bytes);

                //bloomfilter真实数据，压缩比没有效率;zip对文件压缩，也没有效果;7z a -t7z files.7z bloomfilter.data -mx9  /7z毫无效果
                log.debug(String.format("原来大小：%s ；压缩后大小：%s", bytes.length,compress.length));

                log.debug(String.format("%s个数据，持久化到内存(size=%s)，花费时间:%s毫秒",1,MyUtils.bytes2kb(compress.length),System.currentTimeMillis()-start));
            }


        } catch (IOException e) {
            e.printStackTrace();
        }finally{
            out.flush();
            out.close();
        }

        //将之前持久化的数据加载到Filter
        InputStream in  = new FileInputStream(f);
        try {

            {
                long start=System.currentTimeMillis();
//            filter = BloomFilter.readFrom(in,Funnels.stringFunnel(Charsets.UTF_8));

//                for (int i = 0; i < expectedInsertions; i++) {
                    filter = BloomFilter.readFrom(in,Funnels.byteArrayFunnel());
//                }
                log.debug(String.format("%s个数据，从磁盘文件(size=%s)恢复数据，花费时间:%s毫秒",1, MyUtils.bytes2kb(f.length()),System.currentTimeMillis()-start));

            }


        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            in.close();
        }

// 3. 测试结果

        {
            //用已有的数据遍历，判定是否存在，没有漏判的。
            long start=System.currentTimeMillis();

            for (int i = 0; i < expectedInsertions; i++) {
                if (!filter.mightContain(("abc_test_" + i).getBytes())) {
                    System.out.println("漏网之鱼");
                }
            }

            log.debug(String.format("判定%s个数据，花费时间:%s毫秒",expectedInsertions,System.currentTimeMillis()-start));

        }


        {   //用新的数据列表，判定是否存在，有错杀的。符合错误率。
            long start=System.currentTimeMillis();
            List<String> list = new ArrayList<String>(1000);
            for (long i = expectedInsertions + 10000; i < expectedInsertions + 20000; i++) {
                if (filter.mightContain(("abc_test_" + i).getBytes())) {
                    list.add("abc_test_" + i);
                }
            }
            System.out.println("错杀：" + list.size());
            log.debug(String.format("判定%s个数据，花费时间:%s毫秒",expectedInsertions,System.currentTimeMillis()-start));

        }





    }

    /**
     * 常见的几个应用场景：
     * <p>
     * cerberus在收集监控数据的时候, 有的系统的监控项量会很大, 需要检查一个监控项的名字是否已经被记录到db过了, 如果没有的话就需要写入db.
     * 爬虫过滤已抓到的url就不再抓，可用bloom filter过滤
     * 垃圾邮件过滤。如果用哈希表，每存储一亿个 email地址，就需要 1.6GB的内存（用哈希表实现的具体办法是将每一个 email地址对应成一个八字
     * 节的信息指纹，然后将这些信息指纹存入哈希表，由于哈希表的存储效率一般只有 50%，因此一个 email地址需要占用十六个字节。一亿个地址大约
     * 要 1.6GB，即十六亿字节的内存）。因此存贮几十亿个邮件地址可能需要上百 GB的内存。而Bloom Filter只需要哈希表 1/8到 1/4 的大小就能
     * 解决同样的问题。
     */
    @Test
    public void email() {
        long expectedInsertions = 10000000;
        double fpp = 0.00001;

        BloomFilter<Email> emailBloomFilter = BloomFilter
                .create((Funnel<Email>) (from, into) -> into.putString(from.getDomain()+from.getUserName(), Charsets.UTF_8),
                        expectedInsertions, fpp);

        emailBloomFilter.put(new Email("james.mo", "163.com"));
        boolean containsEmail = emailBloomFilter.mightContain(new Email("james.mo", "163.com"));
        boolean containsEmail1 = emailBloomFilter.mightContain(new Email("tiger.mo", "163.com"));
        System.out.println(containsEmail);
        System.out.println(containsEmail1);
        assertTrue(containsEmail);
        assertFalse(containsEmail1);
    }

    //    @Data
//    @Builder
//    @ToString
//    @AllArgsConstructor
    public static class Email {
        private String userName;
        private String domain;

        public Email(String s, String s1) {
            this.userName = s;
            this.domain = s1;
        }

        public String getUserName() {
            return userName;
        }

        public void setUserName(String userName) {
            this.userName = userName;
        }

        public String getDomain() {
            return domain;
        }

        public void setDomain(String domain) {
            this.domain = domain;
        }
    }

    /**
     * 项目路径
     *
     * @param url
     * @return
     */
    public  String basePath(String projectName) {
        URL url = this.getClass().getResource("/");

        String path = url.getPath();

//        LOG.info(path);

        String basepath = path.substring(0,path.indexOf(projectName));

//        LOG.info(basepath);
        return basepath;
//        return path;
    }

}
