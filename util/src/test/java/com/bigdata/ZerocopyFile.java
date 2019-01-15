package com.bigdata;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URL;
import java.nio.channels.FileChannel;

/**
 * disk-disk零拷贝
 *
 *
 * (1)RandomAccessFile raf = new RandomAccessFile (File, "rw");
 *
 * (2)FileChannel channel = raf.getChannel();
 *
 * (3)MappedByteBuffer buff = channel.map(FileChannel.MapMode.READ_WRITE,startAddr,SIZE);
 *
 * (4)buf.put((byte)255);
 *
 * (5)buf.write(byte[] data)
 *
 *
 */
class ZerocopyFile {
    @SuppressWarnings("resource")
    /**
     * RandomAccessFile是Java中输入，输出流体系中功能最丰富的文件内容访问类，它提供很多方法来操作文件，包括读写支持，与普通的IO流相比，它最大的特别之处就是支持任意访问的方式，程序可以直接跳到任意地方来读写数据。
     * 如果我们只希望访问文件的部分内容，而不是把文件从头读到尾，使用RandomAccessFile将会带来更简洁的代码以及更好的性能。
     *
     */
    public static void transferToDemo(String from, String to) throws IOException {
        FileChannel fromChannel = new RandomAccessFile(from, "rw").getChannel();
        FileChannel toChannel = new RandomAccessFile(to, "rw").getChannel();

        long position = 0;
        long count = fromChannel.size();

        fromChannel.transferTo(position, count, toChannel);

        fromChannel.close();
        toChannel.close();
    }

    @SuppressWarnings("resource")
    public static void transferFromDemo(String from, String to) throws IOException {
        FileChannel fromChannel = new FileInputStream(from).getChannel();
        FileChannel toChannel = new FileOutputStream(to).getChannel();

        long position = 0;
        long count = fromChannel.size();

        toChannel.transferFrom(fromChannel, position, count);

        fromChannel.close();
        toChannel.close();
    }

    public static void main(String[] args) throws IOException {

        ZerocopyFile zf=new ZerocopyFile();


        String from = zf.basePath("util")+"pom.xml";
        String to = zf.basePath("util")+"pom.xml.zerocopy";
         transferToDemo(from,to);
//        transferFromDemo(from, to);
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
        String basepath = path.substring(0,path.indexOf(projectName));
        return basepath;
    }
}