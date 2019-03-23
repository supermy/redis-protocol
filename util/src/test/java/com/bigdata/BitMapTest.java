package com.bigdata;

/**
 *
 *
 * 其实，就是按int从小到大的顺序依次摆放到byte[]中，仅涉及到一些除以2的整次幂和对2的整次幂取余的位操作小技巧。
 * 很显然，对于小数据量、数据取值很稀疏，上面的方法并没有什么优势，但对于海量的、取值分布很均匀的集合进行去重，
 * Bitmap极大地压缩了所需要的内存空间。于此同时，还额外地完成了对原始数组的排序工作。缺点是，Bitmap对于每个元素只能记录1bit信息，
 * 如果还想完成额外的功能，恐怕只能靠牺牲更多的空间、时间来完成了。
 *
 *
 * bitmap一个最大的优势是它通常能在存储信息的时候节省大量空间。比方说一个用增量ID来辨别用户的系统，
 * 可以用仅仅512MB的空间来标识40亿个用户是否想要接受通知。
 *
 *
 */
public class BitMapTest {


    public static final int _1MB = 1024 * 1024;
    //每个byte记录8bit信息,也就是8个数是否存在于数组中
    public static byte[] flags = new byte[ 512 * _1MB ];

    public static void main(String[] args) {

        //如果长度不够扩充长度；
        System.out.println(flags.length);


        //待判重数据
        int[] array = {255, 1024, 0, 65536, 255};

        int index = 0;
        for(int num : array) {
            if(!getFlags(num)) {
                //未出现的元素
                array[index] = num;
                index = index + 1;
                //设置标志位
                setFlags(num);
                System.out.println("set " + num);
            } else {
                System.out.println(num + " already exist");
            }
        }
    }

    public static void setFlags(int num) {
        //使用每个数的低三位作为byte内的映射
        //例如: 255 = 0x11111111
        //低三位(也就是num & (0x07))为0x111 = 7, 则byte的第7位为1, 表示255已存在
        flags[num >> 3] |= 0x01 << (num & (0x07));
    }

    public static boolean getFlags(int num) {
        return (flags[num >> 3] >> (num & (0x07)) & 0x01) == 0x01;
    }

}
