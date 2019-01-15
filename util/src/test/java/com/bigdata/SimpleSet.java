package com.bigdata;

import org.junit.Assert;

/**
 *
 * 众所周知，计算机底层是二进制。而java作为一门计算机编程语言，也对二进制的位运算提供了完整的支持。
 *
 * 在java中，int是32位的，也就是说可以用来实现32位的位运算。方便起见，我们一般用16进制对它赋值，比如： 0011表示成16进制是 0x3, 110111表示成16进制是 0x37。
 *
 * 那么什么是位运算呢？位运算是将数据看做二进制，进行位级别的操作。主要有移位运算和逻辑运算
 *
 * 移位运算：
 *
 * 左移：操作符为<<，向左移动，右边的低位补0，左边高位舍弃，将二进制看做整数，左移1位就相当于乘以2。
 * 无符号右移：操作符为>>>，向右移动，右边的舍弃掉，左边补0。
 * 有符号右移：操作符为>>，向右移动，右边的舍弃掉，左边补的值取决于原来最高位，原来是1就补1，原来是0就补0，将二进制看做整数，右移1位相当于除以2。
 *
 * int a = 4; // 100
 * a = a >> 2; // 001，等于1
 * a = a << 3 // 1000，变为8
 *
 *
 * 逻辑运算有：
 *
 * 按位与 &：两位都为1才为1
 * 按位或 |：只要有一位为1，就为1
 * 按位取反 ~： 1变为0，0变为1
 * 按位异或 ^ ：相异为真，相同为假
 *
 * int a = ...;
 * a = a & 0x1 // 返回0或1，就是a最右边一位的值。
 * a = a | 0x1 //不管a原来最右边一位是什么，都将设为1
 *
 * ~  按位非（NOT）（一元运算）
 *
 * &  按位与（AND）
 *
 * |  按位或（OR）
 *
 * ^  按位异或（XOR）
 *
 * >>  右移
 *
 * >>>  右移，左边空出的位以0填充 ；无符号右移
 *
 * <<  左移
 *
 * &=  按位与赋值
 *
 * |=  按位或赋值
 *
 * ^=  按位异或赋值
 *
 * >>=  右移赋值
 *
 * >>>=  右移赋值，左边空出的位以0填充 ；无符号左移
 *
 * <<=  左移赋值
 *
 * 判断int型变量a是奇数还是偶数
 * a&1 = 0 偶数
 * a&1 = 1 奇数
 *
 * 求平均值，比如有两个int类型变量x、y,首先要求x+y的和，再除以2，但是有可能x+y的结果会超过int的最大表示范围，所以位运算就派上用场啦。
 * (x&y)+((x^y)>>1);
 *
 * 对于一个大于0的整数，判断它是不是2的几次方
 * ((x&(x-1))==0)&&(x!=0)；
 *
 * 比如有两个int类型变量x、y,要求两者数字交换，位运算的实现方法：性能绝对高效
 * x ^= y;
 * y ^= x;
 * x ^= y;
 *
 * 求绝对值
 * int abs( int x )
 * {
 * int y ;
 * y = x >> 31 ;
 * return (x^y)-y ; //or: (x+y)^y
 * }
 *
 * 取模运算，采用位运算实现：
 * a % (2^n) 等价于 a & (2^n - 1)
 *
 * 乘法运算 采用位运算实现
 * a * (2^n) 等价于 a << n
 *
 * 除法运算转化成位运算
 * a / (2^n) 等价于 a>> n
 *
 * 求相反数
 * (~x+1)
 *
 * a % 2 等价于 a & 1
 *
 */
public class SimpleSet {

    public static final int A = 0x01;// 最后四位为0001
    public static final int B = 0x02;// 最后四位为0010
    public static final int C = 0x04;// 最后四位为0100
    public static final int D = 0x08;// 最后四位为1000

    private int set = 0x00;// 初始0000，空集合

    public void add(int i) {// 将i对应位的值置为1，重复add不影响。默认传入值为ABCD之一，此处省去边界判断
        set |= i;
    }

    public boolean contain(int i) {// 判断相应位置是否为1
        return (set & i) == i;
    }

    public boolean remove(int i) {// 来不及不解释了快看代码
        if (contain(i)) {
            set -= i;
            return true;
        } else {
            return false;
        }
    }

    //A、B、C、D有点类似于枚举，这个方案它有个名字，叫位向量。
    public static void main(String[] args) {
        SimpleSet set = new SimpleSet();
        System.out.println(set.contain(A));
        Assert.assertFalse(set.contain(A));

        set.add(B);
        System.out.println(set.contain(A));
        Assert.assertFalse(set.contain(A));
        System.out.println(set.contain(B));
        Assert.assertTrue(set.contain(B));

        set.add(A);
        set.add(C);
        System.out.println(set.contain(A));
        Assert.assertTrue(set.contain(A));

        set.remove(A);
        System.out.println(set.contain(A));
        Assert.assertFalse(set.contain(A));

        System.out.println(set.remove(A));
        Assert.assertFalse(set.remove(A));

        System.out.println(set.contain(C));
        Assert.assertTrue(set.contain(C));


        int a = 4; // 100
//        a = a >> 2; // 001，等于1
//        a = a << 3 // 1000，变为16

        Assert.assertEquals(1,a>>2);
        Assert.assertEquals(16,a<<2);

        int b = 1;
//        b = b & 0x1 // 返回0或1，就是a最右边一位的值。
//        b = b | 0x1 //不管a原来最右边一位是什么，都将设为1
        Assert.assertEquals(1,b & 0x1);
        Assert.assertEquals(0,2 & 0x1);
        Assert.assertEquals(1,b | 0x1);

        /**
         * 场景一：判断奇偶
         *
         * 分析：奇数都不是2的整数倍，转换成二进制后最低位必然为1，偶数则相反。
         * 利用这个特性我们可以很容易的通过位运算判断一个整数的奇偶性。
         *
         */

        int i = 1;// 二进制存储方式为00000000000000000000000000000001
        int j = 5;// 二进制存储方式为00000000000000000000000000000101
        int k = 6;// 二进制存储方式为00000000000000000000000000000110
        if ((i & j) == 1) {
            System.out.println("j的最低位为1，为奇数");
            Assert.assertEquals(1,i & j);
        }

        if ((i & k) == 0) {
            System.out.println("k的最低位为0，为偶数");
            Assert.assertEquals(0,i & k);

        }

        /**
         * 场景二：n&1 == 1?”奇数”:”偶数”
         * 推断int型变量a是奇数还是偶数
         *       a&1  = 0 偶数
         *       a&1 =  1 奇数
         *
         * (2) 取int型变量a的第k位 (k=0,1,2……sizeof(int))。即a>>k&1
         * (3) 将int型变量a的第k位清0。即a=a&~(1 < <k)
         * (4) 将int型变量a的第k位置1， 即a=a ¦(1 < <k)
         * (5) int型变量循环左移k次，即a=a < <k ¦a>>16-k  (设sizeof(int)=16)
         * (6) int型变量a循环右移k次，即a=a>>k ¦a < <16-k  (设sizeof(int)=16)
         *
         * (7)整数的平均值
         * 对于两个整数x,y，假设用 (x+y)/2 求平均值。会产生溢出。由于 x+y 可能会大于INT_MAX，可是我们知道它们的平均值是肯定不会溢出的。我们用例如以下算法：
         * int average(int x, int y)  //返回X,Y 的平均值
         * {
         *     return (x&y)+((x^y)>>1);
         * }
         * (8)推断一个整数是不是2的幂,对于一个数 x >= 0，推断他是不是2的幂
         * boolean power2(int x)
         * {
         *     return ((x&(x-1))==0)&&(x!=0)。
         * }
         * (9)不用temp交换两个整数
         * void swap(int x , int y)
         * {
         *     x ^= y;
         *     y ^= x;
         *     x ^= y;
         * }
         * (10)计算绝对值
         * int abs( int x )
         * {
         * int y ;
         * y = x >> 31 ;
         * return (x^y)-y ;        //or: (x+y)^y
         * }
         * (11)取模运算转化成位运算 (在不产生溢出的情况下)
         *         a % (2^n) 等价于 a & (2^n - 1)
         * (12)乘法运算转化成位运算 (在不产生溢出的情况下)
         *         a * (2^n) 等价于 a < < n
         * (13)除法运算转化成位运算 (在不产生溢出的情况下)
         *         a / (2^n) 等价于 a>> n
         *         例: 12/8 == 12>>3
         * (14) a % 2 等价于 a & 1
         * (15) if (x == a) x= b;
         * 　　          else x= a;
         * 　　      等价于 x= a ^ b ^ x;
         * (16) x 的 相反数 表示为 (~x+1)
         */

        for (int n = 2; n <12 ; n=n+2) {
            System.out.println(n);
            System.out.println(n&1);
            Assert.assertEquals(0,(n&1));
        }

        int[] c={1,2,3};
        reverse(c);
        System.out.println("-----------------");
        for (int i1 : c) {
            System.out.println(i1);
        }



    }

    /**
     * 不用临时变量交换两个数
     * @param nums
     * @return
     */
    public static int[] reverse(int[] nums){
        int i = 0;
        int j = nums.length-1;
        while(j>i){
            nums[i]= nums[i]^nums[j];
            nums[j] = nums[j]^nums[i];
            nums[i] = nums[i]^nums[j];
            j--;
            i++;
        }
        return nums;
    }


}
