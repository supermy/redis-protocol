package org.supermy.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.NumberFormat;

/**
 * 常用快捷方法
 *
 */
public class MyUtils {

    /**
     * 计算百分比
     *
     * @param fz
     * @param fm
     * @param scale
     * @return
     */
    public static String percent(long fz, long fm, int scale) {
        BigDecimal divide = new BigDecimal(fz).divide(new BigDecimal(fm), 6, RoundingMode.HALF_UP);

//        System.out.println(divide);
        NumberFormat nf = NumberFormat.getPercentInstance();
        nf.setMinimumFractionDigits(2);
        nf.setMaximumFractionDigits(scale);
        return  nf.format(divide);

//        return divide.toPlainString();
    }



    /**
     * byte(字节)根据长度转成kb(千字节)和mb(兆字节)
     *
     * @param bytes
     * @return
     */
    public static String bytes2kb(long bytes) {
        BigDecimal filesize = new BigDecimal(bytes);
        BigDecimal megabyte = new BigDecimal(1024 * 1024);
        float returnValue = filesize.divide(megabyte, 2, BigDecimal.ROUND_UP)
                .floatValue();
        if (returnValue > 1)
            return (returnValue + "MB");
        BigDecimal kilobyte = new BigDecimal(1024);
        returnValue = filesize.divide(kilobyte, 2, BigDecimal.ROUND_UP)
                .floatValue();
        return (returnValue + "KB");
    }


    /**
     * 替换  Unpooled.wrappedBuffer
     * @param byts
     * @return
     */
    public static ByteBuf concat(byte[]... byts){
        ByteBuf buf= Unpooled.buffer(32);
        for (byte[] byt:byts){
            buf.writeBytes(byt);
        }
        return buf;
    }

    public static ByteBuf concat(ByteBuf... byts){
        ByteBuf buf= Unpooled.buffer(32);
        for (ByteBuf byt:byts){
            buf.writeBytes(byt);
        }
        return buf;
    }

    /**
     * 值Key时候需要注意读指针回复。
     *
     * @param buf
     * @return
     */
    public static byte[] toByteArray(ByteBuf buf) {
        buf.resetReaderIndex();
        byte[] array = buf.readBytes(buf.readableBytes()).array();
        buf.resetReaderIndex();
        return array;

    }

    public static String ByteBuf2String(ByteBuf key) {
//        key.resetReaderIndex();
        return new String(toByteArray(key));
    }
}
