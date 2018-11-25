package guava;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 *
 * 测试对于 byte[] 是否支持
 * Created by moyong on 2017/11/4.
 */
public class BiMapTest {

    @Test
    public  void bimap() {
        BiMap<byte[],byte[]> weekNameMap = HashBiMap.create();

        byte[] key="key".getBytes();
        byte[] value="val".getBytes();

        weekNameMap.put(key,value);


        Assert.assertArrayEquals(value,weekNameMap.get(key));
        Assert.assertArrayEquals( key,weekNameMap.inverse().get(value));

        Assert.assertTrue(weekNameMap.containsKey(key));
        Assert.assertTrue(weekNameMap.containsValue(value));


    }

}

