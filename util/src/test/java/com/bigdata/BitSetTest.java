package com.bigdata;

import org.junit.Test;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Random;

/**
 * BitSet 简单说明
 *
 *    在内存中是一串连续的内存空间，从0开始的正整数
 *    按位操作，每一位的值只有两种 0 或者 1，来表示某个值是否出现过。
 *
 */
public class BitSetTest {

    @Test
    public void test() {

        BitSet bitSet0 = new BitSet();

        bitSet0.set(1);
        bitSet0.set(3);
        bitSet0.set(5);
        bitSet0.set((586586));

//    这时候bitSet的长度是 最大数+1=5+1=6

        for (int i = 0; i < bitSet0.length(); i = i + 1) {
            System.out.print(bitSet0.get(i) + "-");
        }



        //转换为字节数组，可以存储到rdb
        bitSet0.toByteArray();


        /**
         * 有1千万个随机数，随机数的范围在1到1亿之间。现在要求写出一种算法，将1到1亿之间没有在随机数中的数求出来？
         */

        Random random=new Random();

        List<Integer> list=new ArrayList<>();
        for(int i=0;i<10000000;i++)
        {
            int randomResult=random.nextInt(100000000);
            list.add(randomResult);
        }
        System.out.println("产生的随机数有");
        for(int i=0;i<list.size();i++)
        {
            System.out.println(list.get(i));
        }

        BitSet bitSet=new BitSet(100000000);
        for(int i=0;i<10000000;i++)
        {
            bitSet.set(list.get(i));
        }

        System.out.println("0~1亿不在上述随机数中有"+bitSet.size());
        for (int i = 0; i < 100000000; i++)
        {
            if(!bitSet.get(i))
            {
                System.out.println(i);
            }
        }


    }
}
