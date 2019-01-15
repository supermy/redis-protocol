package com.bigdata;


import java.util.BitSet;


public class BitSetDemo {
    public static void main(String[] args) {
        BitSet bm = new BitSet();
        bm.set(1);
        bm.set(2);
/* System.out.println(bm.isEmpty() + "--" + bm.size());
bm.set(0);
System.out.println(bm.isEmpty() + "--" + bm.size());
bm.set(1);
System.out.println(bm.isEmpty() + "--" + bm.size());
System.out.println(bm.get(65));
System.out.println(bm.isEmpty() + "--" + bm.size());
bm.set(65);
System.out.println(bm.isEmpty() + "--" + bm.size());*/

        BitSet bm2 = new BitSet();

        bm2.set(1);
        bm2.set(2);
        bm2.set(3);
        bm2.set(6);
//求并集
//bm.or(bm2);
//[1 2 3 6]
//0 1 1 1 0 0 1
// 1 2 3   6

//求交集
        bm.and(bm2);
        System.out.println("求并集 " + bm.cardinality());


        BitSet bitSet1 = new BitSet();

//将BitSet对象转成byte数组
        byte[] bytes1 = bitSet2ByteArray(bm);

        //在将byte数组转回来
        bitSet1 = byteArray2BitSet(bytes1);
        System.out.println(bitSet1.size() + "," + bitSet1.cardinality());
        System.out.println("是否包含值1:" + bitSet1.get(1));
        System.out.println(bitSet1.get(2));
        System.out.println(bitSet1.get(4));
        System.out.println(bitSet1.get(5));
        System.out.println("\n===列出BitSet处理后的结果=======");
        for (int i = bm.nextSetBit(0); i >= 0; i = bm.nextSetBit(i + 1)) {
            System.out.print(i + "\t");
        }


    }


    /**
     * 将BitSet对象转化为ByteArray
     *
     * @param bitSet
     * @return
     */
    public static byte[] bitSet2ByteArray(BitSet bitSet) {
        byte[] bytes = new byte[bitSet.size() / 8];
        for (int i = 0; i < bitSet.size(); i++) {
            int index = i / 8;
            int offset = 7 - i % 8;
            bytes[index] |= (bitSet.get(i) ? 1 : 0) << offset;
        }
        return bytes;
    }

    /**
     * 将ByteArray对象转化为BitSet
     *
     * @param bytes
     * @return
     */
    public static BitSet byteArray2BitSet(byte[] bytes) {
        BitSet bitSet = new BitSet(bytes.length * 8);
        int index = 0;
        for (int i = 0; i < bytes.length; i++) {
            for (int j = 7; j >= 0; j--) {
                bitSet.set(index++, (bytes[i] & (1 << j)) >> j == 1 ? true
                        : false);
            }
        }
        return bitSet;
    }

    //true,false换成1,0为了好看
    public static String getBitTo10(boolean flag) {
        String a = "";
        if (flag == true) {
            return "1";
        } else {
            return "0";
        }
    }
}
