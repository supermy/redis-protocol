package redis.server.netty.rocksdb;

import com.google.common.primitives.Longs;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import redis.server.netty.RedisException;

import java.util.Arrays;


/**
 * todo 使用bitfield实现getbits和setbits
 */
public class StringMetaTest {

    private static Logger log = Logger.getLogger(StringMetaTest.class);


    /**
     * Hash数据集测试
     *
     * @throws RedisException
     */
    @Test
    public  void testString() throws RedisException {
        //String set get 操作
        //通过给数据增加版本号，进行快速删除？需要多进行一次查询

        StringMeta metaString = StringMeta.getInstance(RocksdbRedis.mymeta, "redis".getBytes());
        metaString.genMetaKey("HashUpdate".getBytes()).del();
        metaString.genMetaKey("HashInit".getBytes()).del();

        // 已经存在的 key
        metaString.get("HashUpdate".getBytes()).data();
        log.debug(metaString.get("HashInit".getBytes()).asUTF8String());
        Assert.assertNull(metaString.get("HashInit".getBytes()).data());


        //测试创建
        metaString.genMetaKey("key".getBytes()).del();
        metaString.set("key".getBytes(), "value".getBytes(), null);

        log.debug(metaString.get("key".getBytes()).asUTF8String());

        Assert.assertArrayEquals("value".getBytes(), metaString.get("key".getBytes()).data().array());
        Assert.assertEquals("value".length(), metaString.strlen("key".getBytes()).data().intValue());

//        log.debug(metaString.getKey0Str());

//        Assert.assertArrayEquals("key".getBytes(), metaString.getKey0());

        Assert.assertArrayEquals("lu".getBytes(), metaString.getRange("key".getBytes(), "2".getBytes(), "3".getBytes()).data().array());
        Assert.assertArrayEquals("value".getBytes(), metaString.getset("key".getBytes(), "val".getBytes()).data().array());

        metaString.genMetaKey("k1".getBytes()).del();
        metaString.genMetaKey("k2".getBytes()).del();
        metaString.genMetaKey("k3".getBytes()).del();
        metaString.genMetaKey("k4".getBytes()).del();
        metaString.genMetaKey("k5".getBytes()).del();
        metaString.genMetaKey("k7".getBytes()).del();
        metaString.genMetaKey("k8".getBytes()).del();
        metaString.genMetaKey("k9".getBytes()).del();

        metaString.mset("k1".getBytes(), "v1".getBytes(), "k2".getBytes(), "v2".getBytes(), "k3".getBytes(), "v3".getBytes());

        //Redis Msetnx 命令用于所有给定 key 都不存在时，同时设置一个或多个 key-value 对。
        log.debug("msetnx ...... k1 k4 k5 ......begin");
        metaString.msetnx("k1".getBytes(), "va".getBytes(), "k4".getBytes(), "v4".getBytes(), "k5".getBytes(), "v5".getBytes());
        log.debug("msetnx ...... k1 k4 k5 ......end");

        log.debug("msetnx ...... k7 k8 k9 ......begin");
        metaString.msetnx("k7".getBytes(), "v7".getBytes(), "k8".getBytes(), "v8".getBytes(), "k9".getBytes(), "v9".getBytes());
        log.debug("msetnx ...... k7 k8 k9 ......end");

        String[] strings5 = {"v1", "v2", "v3"};
        Assert.assertEquals(metaString.mget("k1".getBytes(), "k2".getBytes(), "k3".getBytes()).toString(), Arrays.asList(strings5).toString());

        String[] strings6 = {"v7", "v8", "v9"};
        Assert.assertEquals(metaString.mget("k7".getBytes(), "k8".getBytes(), "k9".getBytes()).toString(), Arrays.asList(strings6).toString());

        String[] strings7 = {null, null, null};
        Assert.assertEquals(metaString.mget("k4".getBytes(), "k5".getBytes(), "k6".getBytes()).toString(), Arrays.asList(strings7).toString());


        metaString.info();

        metaString.genMetaKey("incr1".getBytes()).del();

        Assert.assertEquals(metaString.decr("incr1".getBytes()).data().intValue(), -1);
        Assert.assertEquals(metaString.incr("incr1".getBytes()).data().intValue(), 0);
        Assert.assertEquals(metaString.decrby("incr1".getBytes(), "2".getBytes()).data().intValue(), -2);
        Assert.assertEquals(metaString.incrby("incr1".getBytes(), "2".getBytes()).data().intValue(), 0);

        metaString.info();

        Assert.assertEquals(metaString.append("key".getBytes(), "append".getBytes()).data().intValue(), 9);
        Assert.assertEquals(metaString.get("key".getBytes()).asUTF8String(), "valappend");

        metaString.info();

        log.debug("done ......");




    }

    @Test
    public  void testBits() throws RedisException {

        StringMeta metaString = StringMeta.getInstance(RocksdbRedis.mymeta, "redis".getBytes());


        System.out.println(Longs.fromByteArray(Longs.toByteArray(10010)));
        System.out.println(new String("10010".getBytes()));

        new String("1".getBytes());
        //todo test setbit getbit bitcount bitop

        byte[] bitskey="bits".getBytes();
        byte[] bitskey1="bits2".getBytes();
        byte[] bitskey2="bitsAND".getBytes();
        byte[] bitskey3="bitsOR".getBytes();
        byte[] bitskey4="bitsXOR".getBytes();
        byte[] bitskey5="bitsNOT".getBytes();

        metaString.genMetaKey(bitskey).del();
        metaString.genMetaKey(bitskey1).del();
        metaString.genMetaKey(bitskey2).del();
        metaString.genMetaKey(bitskey3).del();
        metaString.genMetaKey(bitskey4).del();
        metaString.genMetaKey(bitskey5).del();

//        metaString.genMetaKey(bitskey);
        metaString.setbit(bitskey,"10000".getBytes(),"1".getBytes());
        Assert.assertEquals(metaString.getbit(bitskey,"10000".getBytes()).data().intValue(),1);
        Assert.assertEquals(metaString.bitcount(bitskey,null,null).data().intValue(),1);




        metaString.setbit(bitskey2,"10000".getBytes(),"1".getBytes());
        metaString.setbit(bitskey2,"10086".getBytes(),"1".getBytes());

        metaString.setbit(bitskey3,"10000".getBytes(),"1".getBytes());
        metaString.setbit(bitskey3,"10086".getBytes(),"1".getBytes());

        metaString.setbit(bitskey4,"10000".getBytes(),"1".getBytes());
        metaString.setbit(bitskey4,"10086".getBytes(),"1".getBytes());

        metaString.setbit(bitskey5,"10000".getBytes(),"1".getBytes());
        metaString.setbit(bitskey5,"10086".getBytes(),"1".getBytes());



        metaString.setbit(bitskey1,"10000".getBytes(),"1".getBytes());
        metaString.setbit(bitskey1,"10010".getBytes(),"1".getBytes());

        Assert.assertEquals(metaString.bitop("AND".getBytes(),bitskey2,bitskey1).data().intValue(),1);
        Assert.assertEquals(metaString.bitop("OR".getBytes(),bitskey3,bitskey1).data().intValue(),3);
        Assert.assertEquals(metaString.bitop("XOR".getBytes(),bitskey4,bitskey1).data().intValue(),2);
        Assert.assertEquals(metaString.bitop("NOT".getBytes(),bitskey5,bitskey1).data().intValue(),1);


        long start0 = System.nanoTime();
        long count = 10000000;
        for (int i = 0; i < count; i++) {
            metaString.setbit(bitskey, (i+"").getBytes(),"1".getBytes());
        }
        log.debug(String.format("Roaring64NavigableMap 位容器，setbit %s 个时间%s毫秒;平均单个value(%s)setbit时间：%s纳秒",
                count,
                (System.nanoTime() - start0) / 1000 / 1000,
                "缓存",
                (System.nanoTime() - start0)  / count));


    }
}
