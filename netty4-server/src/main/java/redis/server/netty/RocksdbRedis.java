package redis.server.netty;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.gson.JsonObject;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;
import redis.netty4.*;
import redis.util.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.*;

import static java.lang.Double.parseDouble;
import static java.lang.Integer.MAX_VALUE;
import static redis.netty4.BulkReply.NIL_REPLY;
import static redis.netty4.IntegerReply.integer;
import static redis.netty4.StatusReply.OK;
import static redis.util.Encoding.bytesToNum;
import static redis.util.Encoding.numToBytes;


/**
 * Rocksdb 相关的方法操作
 * <p>
 * 核心指令 __put  __get  __del  __exists
 * keys  按前缀检索
 * hkeys 按前缀检索
 * __hput __inclHMeta Hash 指令
 * __hget
 * __hdel __reduHMeta
 * __hexists
 * __hputnx
 * <p>
 * __change（incr）
 * <p>
 * 批量指令 __hmset(RocksDb 批量操作) __hmget(mget 批量获取)  __hgetall(批量seek)
 * </p>
 * Created by moyong on 2017/10/20.
 */
public class RocksdbRedis extends RedisBase {

    /**
     * @param src
     * @return
     * @throws RedisException
     */
    public static int bytesToInt(byte[] src) throws RedisException {

        long offset = bytesToLong(src);

        System.out.println(String.format("offset %d", offset));

        if (offset < -MAX_VALUE || offset > MAX_VALUE) {
            throw notInteger();
        }
        return (int) offset;
    }

    /**
     * @param src
     * @return
     */
    public static long bytesToLong(byte[] src) {
        String s = new String(src);
        return Long.parseLong(s);
    }


    protected static int __torange(byte[] offset1, long length) throws RedisException {
        long offset = bytesToLong(offset1);
        if (offset > MAX_VALUE) {
            throw notInteger();
        }
        if (offset < 0) {
//            offset = (length + offset + 1);
            offset = (length + offset);
        }
        if (offset >= length) {
//            offset = length - 1;
            offset = length;
        }
        return (int) offset;
    }

//    /**
//     * List 数据类型枚举
//     */
//    enum ListMeta {
//        COUNT, SSEQ, ESEQ, CSEQ
//    }

    //list meta key

    /**
     * 导航索引
     *
     * @param key0
     * @param name
     * @return
     */
//    protected Long getListMeta(byte[] key0, ListMeta fd) throws RedisException {
        //获取元数据的索引值
//        byte[] keyMeta = __genkey("+".getBytes(), key0, "list".getBytes());
//
//        byte[] valMeta = __get(mymeta, keyMeta); //'0,2'  开始指针，结束指针
//
//        long result = 0;
//
//        if (valMeta == null) {
//
//            throw notListMeta();
//
//        } else {
//            ByteBuf metaBuf = Unpooled.wrappedBuffer(valMeta);
//
//
//            long count = metaBuf.readLong();
//            long sseq = metaBuf.readLong();
//            long eseq = metaBuf.readLong();
//            long cseq = metaBuf.readLong();
//            System.out.println(String.format("List Meta:count:%d,first:%d,end:%d,auto:%d", count, sseq, eseq, cseq));
//
//            switch (fd) {
//
//                case COUNT:
//                    result = metaBuf.getLong(0);
//                    break;
//
//                case SSEQ:
//                    result = metaBuf.getLong(8);
//                    break;
//
//                case ESEQ:
//                    result = metaBuf.getLong(16);
//                    break;
//
//                case CSEQ:
//                    result = metaBuf.getLong(24);
//                    break;
//
//                default:
//                    System.out.println("default");
//                    throw notListMeta();
//            }
//
//        }
//        return result;
//    }

    protected BulkReply __rpop(byte[] key0) throws RedisException {

//        ListMeta meta = new ListMeta(mymeta,key0,false);
//        meta.rpop();


        //初始化List 开始于结束指针
        long count = 0;
        long sseq = 0;
        long eseq = 0;
        long cseq = 0;

        //获取元数据的索引值
        byte[] keyMeta = __genkey("+".getBytes(), key0, "list".getBytes());
        byte[] valMeta = __get(mymeta, keyMeta); //'0,2'  开始指针，结束指针

        ByteBuf metaBuf = null;
        ByteBuf endBuf = null;

        if (valMeta == null) {
            return NIL_REPLY;
        } else {

            metaBuf = Unpooled.wrappedBuffer(valMeta);

            endBuf = metaBuf.slice(16, 8);

            count = metaBuf.getLong(0); //元素个数
            sseq = metaBuf.getLong(8);  //开始元素
            eseq = metaBuf.getLong(16);  //结束元素
            cseq = metaBuf.getLong(24);  //元素最新编号
        }


        //生成获取头部元素的 key
        byte[] keyPre = __genkey("_l".getBytes(), key0, "#".getBytes());
        ByteBuf keyPreBuf = Unpooled.wrappedBuffer(keyPre);

        ByteBuf keyBuf = Unpooled.wrappedBuffer(keyPreBuf, endBuf);
        byte[] endkey = keyBuf.readBytes(keyBuf.readableBytes()).array();


        //获取第一个数据
        JsonObject jo = new JsonObject();
        byte[] endVal = __getValueList(mydata, endkey, jo);

        //最后一个数据简单处理，删除元数据 与最后一个数据
        if (count == 1) {
            __del(endkey);
            __del(mymeta, keyMeta);
            return new BulkReply(endVal);
        }

        ByteBuf secNumBuf = Unpooled.buffer(8);
        secNumBuf.writeLong(jo.get("pseq").getAsLong());
        ByteBuf secBuf = Unpooled.wrappedBuffer(keyPreBuf, secNumBuf);
        byte[] seckey = secBuf.readBytes(secBuf.readableBytes()).array();
        JsonObject jo2 = new JsonObject();
        byte[] secval = __getValueList(mydata, seckey, jo2);

        byte[] pvalue = __genValList(secval, -1,  jo2.get("pseq").getAsLong(),-1); //当前数据，有效期，上一个元素，下一个元素
        __put(mydata, endkey, pvalue, -1); //第一个数据


        //从存储中删除数据
        __del(endkey);

        count--;

        //数据列表为空，清除元数据
        if (count == 0) {
            __del(mymeta, keyMeta);

        } else {


            metaBuf.resetWriterIndex();

            metaBuf.writeLong(count);  //数量
            metaBuf.writeLong(sseq);    //第一个元素
            metaBuf.writeLong(eseq-1);    //最后一个元素
            metaBuf.writeLong(cseq);    //最新主键编号

            System.out.println(String.format("count:%d 第一个元素：%d 最后一个元素：%d 自增主键：%d", count, sseq, eseq-1, cseq));

            metaBuf.resetReaderIndex();
            __put(mymeta, keyMeta, metaBuf.readBytes(metaBuf.readableBytes()).array(), -1);

        }

        return new BulkReply(endVal);
    }


    protected IntegerReply __rpush(byte[] key0, byte[][] value1) throws RedisException {

        long pseq = -1;//上
        long nseq = -1;//下

        long count = 0;//元素数量
        long sseq = 0;//开始 -1 ?
        long eseq = 0;//结束 -1 ?
        long cseq = 0;//当前

        //获取元数据的索引值 +keylist
        byte[] keyMeta = __genkey("+".getBytes(), key0, "list".getBytes());
        byte[] valMeta = __get(mymeta, keyMeta); //'count,sseq,eseq,cseq'  开始指针，结束指针

        ByteBuf metaBuf = null;
        if (valMeta == null) {

            metaBuf = Unpooled.buffer(8);
            metaBuf.resetReaderIndex();

        } else {
            metaBuf = Unpooled.wrappedBuffer(valMeta);

            count = metaBuf.readLong(); //元素个数
            sseq = metaBuf.readLong();  //开始元素
            eseq = metaBuf.readLong();  //结束元素
            cseq = metaBuf.readLong();  //元素最新编号

            metaBuf.resetReaderIndex();
            metaBuf.resetWriterIndex();
        }

        System.out.println(String.format("count:%d,sseq:%d,eseq:%d,cseq:%d", count, sseq, eseq, cseq));


        //批量处理
        final WriteOptions writeOpt = new WriteOptions();
        final WriteBatch batch = new WriteBatch();

        ByteBuf keySulBuf = Unpooled.buffer(8);

//        long it = eseq;
        //添加元素
        for (byte[] bt : value1) {
            byte[] keyPre = __genkey("_l".getBytes(), key0, "#".getBytes());
            ByteBuf keyPreBuf = Unpooled.wrappedBuffer(keyPre);

            byte[] curVal = null;
            //第一个元素
            if (count == 0) {
                //构建数据
                curVal = __genValList(bt, -1, pseq, nseq);
                keySulBuf.writeLong(eseq);
                //构建 key
                ByteBuf keyBuf = Unpooled.wrappedBuffer(keyPreBuf, keySulBuf);
                byte[] newKey = keyBuf.readBytes(keyBuf.readableBytes()).array();

                batch.put(newKey, curVal);

            } else {
                // 取出当前的数据 lpush list 开始的第一个元素
                keySulBuf.writeLong(eseq);
                ByteBuf firstKeyBuf = Unpooled.wrappedBuffer(keyPreBuf, keySulBuf);
                byte[] firstkey = firstKeyBuf.readBytes(firstKeyBuf.readableBytes()).array();
                JsonObject jo = new JsonObject();
                byte[] pval = __getValueList(mydata, firstkey, jo);

                cseq = Math.incrementExact(cseq); //后面数据要自增长 cseq 是自增主键
                eseq = cseq;
                System.out.println("===============cseq" + cseq);


                //更新当前元素指针
                byte[] pvalue = __genValList(pval, -1, jo.get("pseq").getAsLong(), eseq); //当前数据，有效期，上一个元素，下一个元素
                batch.put(firstkey, pvalue); //第一个数据


                //设置新增数据及其指针
                curVal = __genValList(bt, -1, Math.decrementExact(cseq), -1);

                keySulBuf.resetWriterIndex();
                keySulBuf.writeLong(eseq);
                ByteBuf keyBuf = Unpooled.wrappedBuffer(keyPreBuf, keySulBuf);
                byte[] newKey = keyBuf.readBytes(keyBuf.readableBytes()).array();
                batch.put(newKey, curVal); //新增数据

            }
            //元素计数+1
            count++;

        }


        try {
            System.out.println("===============6");

            mydata.write(writeOpt, batch);
            metaBuf.resetWriterIndex();

            metaBuf.writeLong(count);  //数量
            metaBuf.writeLong(sseq);    //第一个元素
            metaBuf.writeLong(eseq);    //最后一个元素
            metaBuf.writeLong(cseq);    //最新主键编号

            System.out.println(String.format("count:%d 第一个元素：%d 最后一个元素：%d 自增主键：%d", count, sseq, eseq, cseq));

            metaBuf.resetReaderIndex();
            __put(mymeta, keyMeta, metaBuf.readBytes(metaBuf.readableBytes()).array(), -1);

        } catch (RocksDBException e) {
            throw new RuntimeException(e.getMessage());
        }

        return integer(count);
    }


    /**
     * @param key0
     * @param start1
     * @param stop2
     * @return
     * @throws RedisException
     */
    protected StatusReply __ltrim(byte[] key0, byte[] start1, byte[] stop2) throws RedisException {

        redis.server.netty.ListMeta meta=new redis.server.netty.ListMeta(mydata,key0,false);

//        long count = getListMeta(key0, COUNT); // FIXME: 2017/11/9  三次访问
//        Long sseq = getListMeta(key0, SSEQ);
//        long eseq = getListMeta(key0, ESEQ);
//        long cseq = getListMeta(key0, CSEQ);

        long count = meta.getCount();
        long sseq = meta.getSseq();
        long eseq = meta.getEseq();
        long cseq = meta.getCseq();

        long s = __torange(start1, count);
        long e = __torange(stop2, count);

        if (e < s) e = s;
        long length = e - s;

        System.out.println(String.format("开始数据 %d 结束数据 %d:", s, e));


        //生成获取头部元素的 key
        byte[] keyPre = __genkey("_l".getBytes(), key0, "#".getBytes());
        ByteBuf keyPreBuf = Unpooled.wrappedBuffer(keyPre);
        ByteBuf startBuf = Unpooled.buffer(8);

        JsonObject jo = new JsonObject();

        long cnt = count;
        long it = sseq;


        //获取范围内的元素;不删除原有数据;遍历
        for (long i = 0; i <= count; i++) {
            System.out.println(String.format(" 迭代号: %d", it));

            //遍历元素 key
            startBuf.resetWriterIndex();
            startBuf.writeLong(it);

            ByteBuf keyBuf = Unpooled.wrappedBuffer(keyPreBuf, startBuf);


            byte[] key = keyBuf.readBytes(keyBuf.readableBytes()).array();
            System.out.println("3333333333333" + new String(key));

            //获取数据
            byte[] vals = __getValueList(mydata, key, jo);


            if (i < s) {
                System.out.println("44444444444" + jo);

                //首位置
                sseq = jo.get("nseq").getAsLong();

                __del(key);
                cnt--;
            }

            if (i > e) {
                System.out.println("555555555" + jo);

                //尾位置
                eseq = jo.get("pseq").getAsLong();

                __del(key);
                cnt--;
            }


            //链表遍历
            it = jo.get("nseq").getAsLong();

            if (it == -1) {
                break;
            }

        }

        if (cnt == count || cnt <= 0) {
            return OK;
        }

        System.out.println("555555555" + sseq);
        System.out.println("555555555" + eseq);

//        byte[] keyPre = __genkey("_l".getBytes(), key0, "#".getBytes());
//        ByteBuf keyPreBuf = Unpooled.wrappedBuffer(keyPre);

        //修正结束节点
        jo.addProperty("pseq", eseq);
        jo.addProperty("nseq", -1);
        fixPNode(startBuf, keyPreBuf, jo);

        //修正开始节点
        jo.addProperty("nseq", sseq);
        jo.addProperty("pseq", -1);
        fixNnode(startBuf, keyPreBuf, jo);


        ByteBuf metaBuf = Unpooled.buffer(24);

        metaBuf.writeLong(cnt);  //数量

        metaBuf.writeLong(sseq);    //第一个元素
        metaBuf.writeLong(eseq);    //最后一个元素

        metaBuf.writeLong(cseq);    //最新主键编号

        System.out.println(String.format("count:%d 第一：%d 最后：%d 自增：%d", cnt, sseq, eseq, cseq));

        metaBuf.resetReaderIndex();

        byte[] keyMeta = __genkey("+".getBytes(), key0, "list".getBytes());
        __put(mymeta, keyMeta, metaBuf.readBytes(metaBuf.readableBytes()).array(), -1);


        return OK;
    }


    /**
     * Linsert 命令用于在列表的元素前或者后插入元素。 当指定元素不存在于列表中时，不执行任何操作。 当列表不存在时，被视为空列表，
     * 不执行任何操作。 如果 key 不是列表类型，返回一个错误。
     *
     * @param key0
     * @param where1
     * @param pivot2
     * @param value3
     * @return
     * @throws RedisException
     */
    protected IntegerReply __linsert(byte[] key0, byte[] where1, byte[] pivot2, byte[] value3) throws RedisException {
        boolean isBefore = Arrays.equals("BEFORE".getBytes(), where1) || Arrays.equals("before".getBytes(), where1) ? true : false; //false = after

        System.out.println("数据值插入位置 before：" + isBefore);


        //初始化List 开始于结束指针
        long count = 0;
        long sseq = 0;
        long eseq = 0;
        long cseq = 0;

        long addcount = 0;


        //获取元数据的索引值
        byte[] keyMeta = __genkey("+".getBytes(), key0, "list".getBytes());
        byte[] valMeta = __get(mymeta, keyMeta); //'0,2'  开始指针，结束指针

        ByteBuf metaBuf = null;
        ByteBuf startBuf = Unpooled.buffer(8);

        if (valMeta == null) {

            return integer(0);

        } else {
            metaBuf = Unpooled.wrappedBuffer(valMeta);

            count = metaBuf.readLong();
            addcount = count;
            sseq = metaBuf.readLong();
            eseq = metaBuf.readLong();
            cseq = metaBuf.readLong();

            //数据列表为空，清除元数据
            if (count == 0) {
                __del(mymeta, keyMeta);
                return integer(0);
            }

            metaBuf.resetReaderIndex();
            metaBuf.resetWriterIndex();
        }


        System.out.println(String.format("导航, 元素个数：%d 开始：%d 结束：%d 主键：%d", count, sseq, eseq, cseq));


        long it = sseq;

        JsonObject curJo = new JsonObject();

//        curJo.addProperty("cseq", cseq);???

        //生成获取头部元素的 key
        byte[] keyPre = __genkey("_l".getBytes(), key0, "#".getBytes());
        ByteBuf keyPreBuf = Unpooled.wrappedBuffer(keyPre);

        for (long i = 0; i <= count; i++) {

            //遍历元素 key
            startBuf.resetWriterIndex();
            startBuf.writeLong(it);
            System.out.println("key 编号:" + it);
            ByteBuf keyBuf = Unpooled.wrappedBuffer(keyPreBuf, startBuf);
            byte[] curKey = keyBuf.readBytes(keyBuf.readableBytes()).array();
            //获取数据
            byte[] curVal = __getValueList(mydata, curKey, curJo);
            System.out.println("数据值：" + new String(curVal));

            if (Arrays.equals(curVal, pivot2)) {
                try {
                    if (isBefore) {

                        //新增节点编号
                        cseq = Math.incrementExact(cseq);

                        System.out.println(String.format("数据值插入位置 before %s 编号：%d", isBefore, cseq));


                        //修改当前节点；
                        byte[] curValue = __genValList(curVal, -1, cseq, curJo.get("nseq").getAsLong());
                        mydata.put(curKey, curValue);

                        //保存新增节点；
                        byte[] addValue = __genValList(value3, -1, curJo.get("pseq").getAsLong(), it);

                        startBuf.resetWriterIndex();
                        startBuf.writeLong(cseq);
                        ByteBuf addBuf = Unpooled.wrappedBuffer(keyPreBuf, startBuf);
                        byte[] addKey = addBuf.readBytes(addBuf.readableBytes()).array();

                        mydata.put(addKey, addValue);

                        addcount++;

                        //修改上一节点
                        //修改上一个元素；
                        long nseq = curJo.get("nseq").getAsLong();
                        curJo.addProperty("nseq", cseq);
                        fixPNode(startBuf, keyPreBuf, curJo);
                        System.out.println("修正的末端节点，数据值：" + curJo.get("sseq"));
                        if (curJo.get("sseq") != null) {
                            sseq = curJo.get("sseq").getAsLong();
                        }
                        curJo.addProperty("nseq", nseq); //复原


                    } else {

                        //新增节点编号
                        cseq = Math.incrementExact(cseq);

                        System.out.println(String.format("数据值插入位置 before %s 编号：%d", isBefore, cseq));


                        //修改当前节点；
                        byte[] curValue = __genValList(curVal, -1, curJo.get("pseq").getAsLong(), cseq);
                        mydata.put(curKey, curValue);

                        //保存新增节点；
                        byte[] addValue = __genValList(value3, -1, it, curJo.get("nseq").getAsLong());

                        startBuf.resetWriterIndex();
                        startBuf.writeLong(cseq);
                        ByteBuf addBuf = Unpooled.wrappedBuffer(keyPreBuf, startBuf);
                        byte[] addKey = addBuf.readBytes(addBuf.readableBytes()).array();

                        mydata.put(addKey, addValue);

                        addcount++;

                        //修改下一个元素;
                        long pseq = curJo.get("pseq").getAsLong();

                        curJo.addProperty("pseq", cseq);
                        fixNnode(startBuf, keyPreBuf, curJo);
                        if (curJo.get("eseq") != null) {
                            eseq = curJo.get("eseq").getAsLong();
                        }
                        curJo.addProperty("pseq", pseq); //复原


                    }

                } catch (RocksDBException e) {
                    e.printStackTrace();
                }

            }


            //链表遍历
            System.out.println("下一个数据编号：" + curJo.get("nseq").getAsLong()); //
            it = curJo.get("nseq").getAsLong();

            if (it == -1) {
                break;
            }
        }

        System.out.println("");
        System.out.println("导航数据处理......");
        System.out.println("");


        metaBuf.resetWriterIndex();

        metaBuf.writeLong(addcount);  //数量

        metaBuf.writeLong(sseq);    //第一个元素
        metaBuf.writeLong(eseq);    //最后一个元素

        metaBuf.writeLong(cseq);    //最新主键编号

        System.out.println(String.format("count:%d 第一：%d 最后：%d 自增：%d", addcount, sseq, eseq, cseq));

        metaBuf.resetReaderIndex();
        __put(mymeta, keyMeta, metaBuf.readBytes(metaBuf.readableBytes()).array(), -1);


        return integer(addcount);
//        return null;

    }

    /**
     * 当索引参数超出范围，或对一个空列表进行 LSET 时，返回一个错误。
     *
     * @param key0
     * @param index1
     * @param value2
     * @return
     * @throws RedisException
     */

    public StatusReply __lset(byte[] key0, byte[] index1, byte[] value2) throws RedisException {
//        int index = bytesToInt(index1);
        byte[] key = __getList(key0, index1, true);

        byte[] val = new byte[0];
        try {
            val = mydata.get(key);
        } catch (RocksDBException e) {
            throw new RedisException(e.getMessage());
        }

        if (val != null) {

            ByteBuf vvBuf = Unpooled.wrappedBuffer(val);
            vvBuf.setInt(8, value2.length); //更新数据长度
            vvBuf.setBytes(8 + 4 + 8 + 8, value2); //更新数据
            try {
                mydata.put(key, vvBuf.readBytes(vvBuf.readableBytes()).array());
            } catch (RocksDBException e) {
                throw new RedisException(e.getMessage());
            }
            return OK;
        } else {
            throw noSuchKey();
        }

    }

    public BulkReply __lindex(byte[] key0, byte[] index1) throws RedisException {
        return new BulkReply(__getList(key0, index1, false));
    }

    /**
     * 根据索引查找数据
     *
     * @param key0
     * @param index1
     * @param iskey
     * @return
     * @throws RedisException
     */
    private byte[] __getList(byte[] key0, byte[] index1, boolean iskey) throws RedisException {
        int index = bytesToInt(index1);

        System.out.println(index);


        //初始化List 开始于结束指针
        long count = 0;
        long sseq = 0;
        long eseq = 0;
        long cseq = 0;


        //获取元数据的索引值
        byte[] keyMeta = __genkey("+".getBytes(), key0, "list".getBytes());
        byte[] valMeta = __get(mymeta, keyMeta); //'0,2'  开始指针，结束指针

        ByteBuf metaBuf = null;
        ByteBuf startBuf = Unpooled.buffer(8);
//        ByteBuf endBuf = null;

        if (valMeta == null) {

            return null;

        } else {
            System.out.println("===============3");

            metaBuf = Unpooled.wrappedBuffer(valMeta);

            count = metaBuf.readLong();
            sseq = metaBuf.readLong();
            eseq = metaBuf.readLong();
            cseq = metaBuf.readLong();

            //数据列表为空，清除元数据
            if (count == 0) {
                __del(mymeta, keyMeta);
                return null;
            }

            metaBuf.resetReaderIndex();
            metaBuf.resetWriterIndex();
        }

        System.out.println("当前数据===============4 index:" + index);
        System.out.println("当前数据===============4 count:" + count);
        System.out.println("当前数据===============4 开始元素 sseq:" + sseq);
        System.out.println("当前数据===============4 结束元素 eseq:" + eseq);
        System.out.println("当前数据===============4 主键 cseq:" + cseq);


        List<byte[]> results = new ArrayList<byte[]>();

        //获取范围内的元素;不删除原有数据;遍历
        for (long i = 0; i <= count; i++) {

            //生成获取头部元素的 key
            byte[] keyPre = __genkey("_l".getBytes(), key0, "#".getBytes());
            ByteBuf keyPreBuf = Unpooled.wrappedBuffer(keyPre);
            //遍历元素 key
            startBuf.resetWriterIndex();

            if (index < 0) {
                startBuf.writeLong(eseq);
                System.out.println("key: " + eseq);

            } else {
                startBuf.writeLong(sseq);
                System.out.println("key: " + sseq);

            }

            ByteBuf keyBuf = Unpooled.wrappedBuffer(keyPreBuf, startBuf);
            byte[] key = keyBuf.readBytes(keyBuf.readableBytes()).array();

            //获取数据
            JsonObject jo = new JsonObject();
            byte[] vals = __getValueList(mydata, key, jo);

            System.out.println("===============6 数据编号 " + new String(vals));

            System.out.println(String.format("===============6 数据编号 %s %d %d  ", count == Math.abs(index), count, index));

            if (i == (Math.abs(index) - 1)) {
//                return new BulkReply(vals);
                if (iskey) {
                    return key;
                } else return vals;
            }

            //链表遍历

            if (index < 0) {
                eseq = jo.get("pseq").getAsLong();

            } else {
                sseq = jo.get("nseq").getAsLong();

            }

            if (sseq == -1 || sseq == -1) {
                break;
            }


        }

        return null;
    }

    /**
     * Redis Lrem 根据参数 COUNT 的值，移除列表中与参数 VALUE 相等的元素。
     * COUNT 的值可以是以下几种：
     * count > 0 : 从表头开始向表尾搜索，移除与 VALUE 相等的元素，数量为 COUNT 。
     * count < 0 : 从表尾开始向表头搜索，移除与 VALUE 相等的元素，数量为 COUNT 的绝对值。
     * count = 0 : 移除表中所有与 VALUE 相等的值。
     * <p>
     * <p>
     * 中间删掉一批数据导致索引断裂，在向上的方向上修复索引，涉及到多个数据的修改，采用批量操作；
     * 这是小概率事件，多数据操作会导致性能影响；但是这种数据结构对于大概率事件的常用操作效率较好；
     * 采用 链表模式会 看上去更好，每个操作会至少涉及到两个元素的修改，但是对于常用操作增加了成本支出。
     * <p>
     * 如果是大数据的情况，有百万数据的话，考虑效率问题。
     *
     * @param key0
     * @param count1
     * @param value2
     * @return
     * @throws RedisException
     */
    // FIXME: 2017/11/2
    protected IntegerReply __lrem(byte[] key0, byte[] count1, byte[] value2) throws RedisException {

        //初始化List 开始于结束指针
        long count = 0;
        long sseq = 0;
        long eseq = 0;
        long cseq = 0;

        int delcount = _toint(count1);

        //获取元数据的索引值
        byte[] keyMeta = __genkey("+".getBytes(), key0, "list".getBytes());
        byte[] valMeta = __get(mymeta, keyMeta); //'0,2'  开始指针，结束指针

        ByteBuf metaBuf = null;
        ByteBuf startBuf = Unpooled.buffer(8);

        if (valMeta == null) {
            System.out.println("===============2");
            return integer(0);
        } else {
            System.out.println("===============3");

            metaBuf = Unpooled.wrappedBuffer(valMeta);

            count = metaBuf.readLong();
            sseq = metaBuf.readLong();
            eseq = metaBuf.readLong();
            cseq = metaBuf.readLong();

            //数据列表为空，清除元数据
            if (count == 0) {
                __del(mymeta, keyMeta);
                return integer(0);
            }

            metaBuf.resetReaderIndex();
            metaBuf.resetWriterIndex();
        }

        System.out.println("当前数据===============4 count:" + count);
        System.out.println("当前数据===============4 开始节点 sseq:" + sseq);
        System.out.println("当前数据===============4 结束节点 eseq:" + eseq);
        System.out.println("当前数据===============4 主键编号 cseq:" + cseq);

        long it = eseq;

        int delCnt = 0;
        //获取范围内的元素;不删除原有数据;遍历
        if (delcount >= 0) {

            for (long i = 0; i <= count; i++) {

                JsonObject curJo = new JsonObject();

                //生成获取头部元素的 key
                byte[] keyPre = __genkey("_l".getBytes(), key0, "#".getBytes());
                ByteBuf keyPreBuf = Unpooled.wrappedBuffer(keyPre);

                //遍历元素 key
                startBuf.resetWriterIndex();
                startBuf.writeLong(it);

                System.out.println("key 编号:" + it);

                ByteBuf keyBuf = Unpooled.wrappedBuffer(keyPreBuf, startBuf);
                byte[] curKey = keyBuf.readBytes(keyBuf.readableBytes()).array();

                //获取数据
                byte[] curVal = __getValueList(mydata, curKey, curJo);

                System.out.println("数据值：" + new String(curVal));

                if (Arrays.equals(curVal, value2)) {

                    //修改相关节点指针，修改元素信息
                    //修改上一个元素；
                    fixPNode(startBuf, keyPreBuf, curJo);
                    System.out.println("修正的末端节点，数据值：" + curJo.get("sseq"));
                    if (curJo.get("sseq") != null) {
                        sseq = curJo.get("sseq").getAsLong();
                    }

                    //修改下一个元素;
                    fixNnode(startBuf, keyPreBuf, curJo);
                    if (curJo.get("eseq") != null) {
                        eseq = curJo.get("eseq").getAsLong();
                    }

                    __del(curKey); //删除数据

                    delCnt++;  //删除数据计数

                    count--;    //遍历数据计数
                }

                if (delCnt >= delcount) {
                    //删除足够的数据，中断删除；
                    break;
                }

                //链表遍历
                System.out.println("下一个数据编号：" + curJo.get("nseq").getAsLong());
                it = curJo.get("nseq").getAsLong();

                if (it == -1) {
                    break;
                }


            }
        }


        //     * count < 0 : 从表尾开始向表头搜索，移除与 VALUE 相等的元素，数量为 COUNT 的绝对值。

        if (delcount < 0) {

            for (long i = 0; i <= count; i++) {

                JsonObject curJo = new JsonObject();

                //生成获取头部元素的 key
                byte[] keyPre = __genkey("_l".getBytes(), key0, "#".getBytes());
                ByteBuf keyPreBuf = Unpooled.wrappedBuffer(keyPre);

                //遍历元素 key
                startBuf.resetWriterIndex();
                startBuf.writeLong(it);

                System.out.println("key 编号:" + it);

                ByteBuf keyBuf = Unpooled.wrappedBuffer(keyPreBuf, startBuf);
                byte[] curKey = keyBuf.readBytes(keyBuf.readableBytes()).array();

                //获取数据
                byte[] curVal = __getValueList(mydata, curKey, curJo);

                System.out.println("数据值：" + new String(curVal));

                if (Arrays.equals(curVal, value2)) {

                    //修改相关节点指针，修改元素信息
                    //修改上一个元素；
                    fixPNode(startBuf, keyPreBuf, curJo);
                    System.out.println("修正的末端节点，数据值：" + curJo.get("sseq"));
                    if (curJo.get("sseq") != null) {
                        sseq = curJo.get("sseq").getAsLong();
                    }

                    //修改下一个元素;
                    fixNnode(startBuf, keyPreBuf, curJo);
                    if (curJo.get("eseq") != null) {
                        eseq = curJo.get("eseq").getAsLong();
                    }

                    __del(curKey); //删除数据

                    delCnt++;  //删除数据计数

                    count--;    //遍历数据计数
                }

                if (delCnt >= Math.abs(delcount)) {
                    //删除足够的数据，中断删除；
                    break;
                }

                //链表遍历
                System.out.println("下一个数据编号：" + curJo.get("pseq").getAsLong());
                it = curJo.get("pseq").getAsLong();

                if (it == -1) {
                    break;
                }


            }
        }


        //数据列表为空，清除元数据
        if (count == 0) {
            __del(mymeta, keyMeta);

        } else {


            metaBuf.resetWriterIndex();

            metaBuf.writeLong(count);  //数量

            metaBuf.writeLong(sseq);    //第一个元素
            metaBuf.writeLong(eseq);    //最后一个元素

            metaBuf.writeLong(cseq);    //最新主键编号

            System.out.println(String.format("count:%d 第一个元素：%d 最后一个元素：%d 自增主键：%d", count, sseq, eseq, sseq));

            metaBuf.resetReaderIndex();
            __put(mymeta, keyMeta, metaBuf.readBytes(metaBuf.readableBytes()).array(), -1);


        }


        return integer(delCnt);

//        List<BytesValue> list = _getlist(key0, false);
//        if (list == null) {
//            return integer(0);
//        } else {
//            int count = _toint(count1);
//            BytesKey value = new BytesKey(value2);
//            int size = list.size();
//            int dir = 1;
//            int s = 0;
//            int e = size;
//            int rem = 0;
//            boolean all = count == 0;
//            if (count < 0) {
//                count = -count;
//                dir = -1;
//                s = e;
//                e = -1;
//            }
//            for (int i = s; (all || count != 0) && i != e; i += dir) {
//                if (list.get(i).equals(value)) {
//                    list.remove(i);
//                    e -= dir;
//                    i -= dir;
//                    rem++;
//                    count--;
//                }
//            }
//            return integer(rem);
//        }
    }

    /**
     * 修正下一个节点的数据
     * 同时修正结束导航节点
     *
     * @param startBuf
     * @param keyPreBuf
     * @param joCur
     * @param eseq
     * @throws RedisException
     */
    private void fixNnode(ByteBuf startBuf, ByteBuf keyPreBuf, JsonObject joCur) throws RedisException {
        //修改下一个元素；
        long n = joCur.get("nseq").getAsLong();

        if (n == -1) {
            //没有下一个元素
            joCur.addProperty("eseq", joCur.get("pseq").getAsLong());

        } else {

            startBuf.resetWriterIndex();
            startBuf.writeLong(n);
            ByteBuf keyBuf = Unpooled.wrappedBuffer(keyPreBuf, startBuf);
            byte[] nkey = keyBuf.readBytes(keyBuf.readableBytes()).array();
            //获取数据
            JsonObject joN = new JsonObject();
            byte[] valsN = __getValueList(mydata, nkey, joN);

            byte[] nvalue = __genValList(valsN, -1, joCur.get("pseq").getAsLong(), joN.get("nseq").getAsLong()); //当前数据，有效期，上一个元素，下一个元素
            try {
                mydata.put(nkey, nvalue); //下一个数据
            } catch (RocksDBException e) {
                e.printStackTrace();
            }

        }
    }


    /**
     * 修正上一个节点数据的指针
     *
     * @param startBuf
     * @param keyPreBuf
     * @param joCur
     * @param sseq
     * @throws RedisException
     */
    private void fixPNode(ByteBuf startBuf, ByteBuf keyPreBuf, JsonObject joCur) throws RedisException {
        long p = joCur.get("pseq").getAsLong();

        if (p == -1) {
            //没有上一个元素 设置元数据导航的开始数据
            joCur.addProperty("sseq", joCur.get("nseq").getAsLong());

        } else {
            System.out.println("修正上一个节点，节点编号：" + p);

            startBuf.resetWriterIndex();
            startBuf.writeLong(p);

            ByteBuf keyBuf = Unpooled.wrappedBuffer(keyPreBuf, startBuf);
            byte[] pkey = keyBuf.readBytes(keyBuf.readableBytes()).array();

            //获取数据
            JsonObject joP = new JsonObject();
            byte[] valsP = __getValueList(mydata, pkey, joP);

            System.out.println(String.format("修正上一个节点，向上节点编号：%d 向下节点编号：%d 数据：%s", joP.get("pseq").getAsLong(), joCur.get("nseq").getAsLong(), new String(valsP)));

            byte[] pvalue = __genValList(valsP, -1, joP.get("pseq").getAsLong(), joCur.get("nseq").getAsLong()); //当前数据，有效期，上一个元素，下一个元素

            try {
                mydata.put(pkey, pvalue);
            } catch (RocksDBException e) {
                e.printStackTrace();
            }

        }
    }


    /**
     * 插入的时候主键为 paretn@child 可以使用 seek 获取数据,可以顺序读取
     *
     * @param key0
     * @param start1
     * @param stop2
     * @return
     * @throws RedisException
     */
    protected MultiBulkReply __lrange(byte[] key0, byte[] start1, byte[] stop2) throws RedisException {

        //初始化List 开始于结束指针
        long count = 0;
        long sseq = 0;
        long eseq = 0;
        long cseq = 0;


        //获取元数据的索引值
        byte[] keyMeta = __genkey("+".getBytes(), key0, "list".getBytes());
        byte[] valMeta = __get(mymeta, keyMeta); //'0,2'  开始指针，结束指针

        ByteBuf metaBuf = null;
        ByteBuf startBuf = Unpooled.buffer(8);
//        ByteBuf endBuf = null;

        if (valMeta == null) {

            return MultiBulkReply.EMPTY;
        } else {
            System.out.println("===============3");

            metaBuf = Unpooled.wrappedBuffer(valMeta);

            count = metaBuf.readLong();
            sseq = metaBuf.readLong();
            eseq = metaBuf.readLong();
            cseq = metaBuf.readLong();

            //数据列表为空，清除元数据
            if (count == 0) {
                __del(mymeta, keyMeta);
                return MultiBulkReply.EMPTY;
            }

            metaBuf.resetReaderIndex();
            metaBuf.resetWriterIndex();
        }

//        if(sseq == -1){
//            return MultiBulkReply.EMPTY;
//            break;
//        }

        System.out.println("当前数据===============4 count:" + count);
        System.out.println("当前数据===============4 开始元素 sseq:" + sseq);
        System.out.println("当前数据===============4 结束元素 eseq:" + eseq);
        System.out.println("当前数据===============4 主键 cseq:" + cseq);


        long s = _torange(start1, count);
        long e = _torange(stop2, count);

        System.out.println("当前数据===============4.1 编号e:" + e);
        if (e < s) e = s;
        long length = e - s;


        System.out.println("当前数据===============4.2 开始编号 s:" + s);
        System.out.println("当前数据===============4.3 结束编号 e:" + e);

        List<BulkReply> results = new ArrayList<BulkReply>();

        //获取范围内的元素;不删除原有数据;遍历
        for (long i = 0; i <= count + length; i++) {

            //生成获取头部元素的 key
            byte[] keyPre = __genkey("_l".getBytes(), key0, "#".getBytes());
            ByteBuf keyPreBuf = Unpooled.wrappedBuffer(keyPre);
            //遍历元素 key
            startBuf.resetWriterIndex();
            startBuf.writeLong(sseq);

            System.out.println("当前数据===============5 开始编号sseq:" + sseq);

            ByteBuf keyBuf = Unpooled.wrappedBuffer(keyPreBuf, startBuf);
            byte[] key = keyBuf.readBytes(keyBuf.readableBytes()).array();

            //获取数据
            JsonObject jo = new JsonObject();
            byte[] vals = __getValueList(mydata, key, jo);

            System.out.println("===============6 数据编号 " + new String(vals));


            if (i >= s && i <= e) {
                results.add(new BulkReply(vals));
            }

            //超出范围跳出循环
            if (i > e) {
                break;
            }

            System.out.println("===============6 下一个元素 nseq:" + jo.get("nseq").getAsLong());
            //链表遍历
            sseq = jo.get("nseq").getAsLong();

            if (sseq == -1) {
                break;
            }


        }

        return new MultiBulkReply(results.toArray(new Reply[results.size()]));

//        return new MultiBulkReply(results);
//
//        List<BytesValue> list = _getlist(key0, false);
//        if (list == null) {
//            return MultiBulkReply.EMPTY;
//        } else {
//            int size = list.size();
//            int s = _torange(start1, size);
//            int e = _torange(stop2, size);
//            if (e < s) e = s;
//            int length = e - s + 1;
//            Reply[] replies = new Reply[length];
//            for (int i = s; i <= e; i++) {
//                replies[i - s] = new BulkReply(list.get(i).getBytes());
//            }
//            return new MultiBulkReply(replies);
//        }
    }

    protected BulkReply __lpop(byte[] key0) throws RedisException {
        //Put(‘+[mylist]list’, ‘0,2’)
        //Put(‘_l[mylist]#0’, ‘a’)
        //Put(‘_l[mylist]#1’, ‘b’)
        //Put(‘_l[mylist]#2’, ‘c’)

        System.out.println("===============1");

        //初始化List 开始于结束指针
        long count = 0;
        long sseq = 0;
        long eseq = 0;
        long cseq = 0;

        //获取元数据的索引值
        byte[] keyMeta = __genkey("+".getBytes(), key0, "list".getBytes());
        byte[] valMeta = __get(mymeta, keyMeta); //'0,2'  开始指针，结束指针

        ByteBuf metaBuf = null;
        ByteBuf firstBuf = null;
//        ByteBuf endBuf = null;

        if (valMeta == null) {
            System.out.println("===============2");
            return NIL_REPLY;
        } else {
            System.out.println("===============3");

            metaBuf = Unpooled.wrappedBuffer(valMeta);

            firstBuf = metaBuf.slice(8, 8);
//            endBuf = metaBuf.slice(8, 8);

            count = metaBuf.readLong(); //元素个数
            sseq = metaBuf.readLong();  //开始元素
            eseq = metaBuf.readLong();  //结束元素
            cseq = metaBuf.readLong();  //元素最新编号

//            metaBuf.resetReaderIndex();
//            metaBuf.resetWriterIndex();

//            metaBuf.writeLong(metaIndex+1);
        }


        System.out.println("当前数据===============4 count:" + count);
        System.out.println("当前数据===============4 cseq:" + cseq);
        System.out.println("first数据===============4 sseq:" + sseq);

        //生成获取头部元素的 key
        byte[] keyPre = __genkey("_l".getBytes(), key0, "#".getBytes());
        ByteBuf keyPreBuf = Unpooled.wrappedBuffer(keyPre);
        ByteBuf keyBuf = Unpooled.wrappedBuffer(keyPreBuf, firstBuf);
        byte[] firstkey = keyBuf.readBytes(keyBuf.readableBytes()).array();
        System.out.println("===============5.1" + new String(firstkey));


        //获取第一个数据
//        byte[] vals = __get(firstkey);
        JsonObject jo = new JsonObject();
        byte[] firstval = __getValueList(mydata, firstkey, jo);

        //最后一个数据简单处理，删除元数据 与最后一个数据
        if (count == 1) {
            __del(firstkey);
            __del(mymeta, keyMeta);
            return new BulkReply(firstval);
        }

        ByteBuf secNumBuf = Unpooled.buffer();
        secNumBuf.writeLong(jo.get("nseq").getAsLong());
        ByteBuf secBuf = Unpooled.wrappedBuffer(keyPreBuf, secNumBuf);
        byte[] seckey = secBuf.readBytes(secBuf.readableBytes()).array();
        System.out.println("===============5.2" + new String(seckey));
        JsonObject jo2 = new JsonObject();
        byte[] secval = __getValueList(mydata, seckey, jo2);

        byte[] pvalue = __genValList(secval, -1, -1, jo2.get("nseq").getAsLong()); //当前数据，有效期，上一个元素，下一个元素
        __put(mydata, firstkey, pvalue, -1); //第一个数据


        //从存储中删除数据
        __del(firstkey);

        count--;

        //数据列表为空，清除元数据
        if (count == 0) {
            __del(mymeta, keyMeta);

        } else {


            metaBuf.resetWriterIndex();

            metaBuf.writeLong(count);  //数量
            metaBuf.writeLong(sseq - 1);    //第一个元素
            metaBuf.writeLong(eseq);    //最后一个元素
            metaBuf.writeLong(cseq);    //最新主键编号

            System.out.println(String.format("count:%d 第一个元素：%d 最后一个元素：%d 自增主键：%d", count, sseq - 1, eseq, sseq));

            metaBuf.resetReaderIndex();
            __put(mymeta, keyMeta, metaBuf.readBytes(metaBuf.readableBytes()).array(), -1);


//            //meta元数据计数器+1
//            metaEnd = Math.decrementExact(metaEnd);
//            endBuf.resetWriterIndex();
//            endBuf.resetReaderIndex();
//            endBuf.writeLong(metaEnd);
//
//            System.out.println("当前数据===============4 end:" + metaEnd);
//
//            metaBuf = Unpooled.wrappedBuffer(startBuf, endBuf);
//
//            __put(mymeta, keyMeta, metaBuf.readBytes(metaBuf.readableBytes()).array(), -1);

        }

        return new BulkReply(firstval);


//        try {
////            mydata.write(writeOpt, batch);
//
//            Math.incrementExact(-3);
//            metaBuf.resetWriterIndex();
//
//            metaBuf.writeLong(metaStart);
//            metaBuf.writeLong(metaEnd);
//
//            metaBuf.resetReaderIndex();
//
//
//
//        } catch (RocksDBException e) {
//            throw new RuntimeException(e.getMessage());
//        }

//        return integer(batch.count());
//        if (list == null || list.size() == 0) {
//            return NIL_REPLY;
//        } else {
//            return new BulkReply(vals);
//        }

//        List<BytesValue> list = _getlist(key0, true);
//        for (byte[] value : value1) {
//            list.add(0, new BytesKey(value));
//        }
//        return integer(list.size());
//        return null;
    }


    public IntegerReply __llen(byte[] key0) throws RedisException {
        //初始化List 开始于结束指针
        long count = 0;
        long sseq = 0;
        long eseq = 0;
        long cseq = 0;

        byte[] keyMeta = __genkey("+".getBytes(), key0, "list".getBytes());
        byte[] valMeta = __get(mymeta, keyMeta); //'0,2'  开始指针，结束指针
        ByteBuf metaBuf = null;
        if (valMeta == null) {
            System.out.println("===============2");
        } else {
            System.out.println("===============3");

            metaBuf = Unpooled.wrappedBuffer(valMeta);

            count = metaBuf.readLong();
            sseq = metaBuf.readLong();
            eseq = metaBuf.readLong();
            cseq = metaBuf.readLong();

        }

        System.out.println("当前数据===============4 count:" + count);
        System.out.println("当前数据===============4 sseq:" + sseq);
        System.out.println("当前数据===============4 eseq:" + eseq);
        System.out.println("当前数据===============4 cseq:" + cseq);

        return integer(count);
    }

    /**
     * List 类型处理
     * Lpush 命令将一个或多个值插入到列表头部。 如果 key 不存在，一个空列表会被创建并执行 LPUSH 操作。 当 key 存在但不是列表类型时，返回一个错误。
     *
     * @param key0
     * @param value1
     * @return
     * @throws RedisException
     */
    protected IntegerReply __lpush(byte[] key0, byte[][] value1) throws RedisException {


        long pseq = -1;//上
        long nseq = -1;//下

        long count = 0;//元素数量
        long sseq = 0;//开始 -1 ?
        long eseq = 0;//结束 -1 ?
        long cseq = 0;//当前

        //获取元数据的索引值 +keylist
        byte[] keyMeta = __genkey("+".getBytes(), key0, "list".getBytes());
        byte[] valMeta = __get(mymeta, keyMeta); //'count,sseq,eseq,cseq'  开始指针，结束指针

        ByteBuf metaBuf = null;
        if (valMeta == null) {

            metaBuf = Unpooled.buffer(8);
            metaBuf.resetReaderIndex();

        } else {
            metaBuf = Unpooled.wrappedBuffer(valMeta);

            count = metaBuf.readLong(); //元素个数
            sseq = metaBuf.readLong();  //开始元素
            eseq = metaBuf.readLong();  //结束元素
            cseq = metaBuf.readLong();  //元素最新编号

            metaBuf.resetReaderIndex();
            metaBuf.resetWriterIndex();
        }

        System.out.println(String.format("count:%d,sseq:%d,eseq:%d,cseq:%d", count, sseq, eseq, cseq));


        //批量处理
        final WriteOptions writeOpt = new WriteOptions();
        final WriteBatch batch = new WriteBatch();

        ByteBuf keySulBuf = Unpooled.buffer(8);
        //添加元素
        for (byte[] bt : value1) {
            byte[] keyPre = __genkey("_l".getBytes(), key0, "#".getBytes());
            ByteBuf keyPreBuf = Unpooled.wrappedBuffer(keyPre);

            byte[] curVal = null;
            //第一个元素
            if (count == 0) {
                //构建数据
                curVal = __genValList(bt, -1, pseq, nseq);
                keySulBuf.writeLong(cseq);
                //构建 key
                ByteBuf keyBuf = Unpooled.wrappedBuffer(keyPreBuf, keySulBuf);
                byte[] newKey = keyBuf.readBytes(keyBuf.readableBytes()).array();

                batch.put(newKey, curVal);

            } else {
                // 取出当前的数据 lpush list 开始的第一个元素
                keySulBuf.writeLong(sseq);
                ByteBuf firstKeyBuf = Unpooled.wrappedBuffer(keyPreBuf, keySulBuf);
                byte[] firstkey = firstKeyBuf.readBytes(firstKeyBuf.readableBytes()).array();

                JsonObject jo = new JsonObject();

                byte[] pval = __getValueList(mydata, firstkey, jo);

                cseq = Math.incrementExact(cseq); //后面数据要自增长 cseq 是自增主键
                sseq = cseq;
                System.out.println("===============cseq" + cseq);
                System.out.println("===============sseq" + sseq);


                //更新当前元素指针
                byte[] pvalue = __genValList(pval, -1, sseq, jo.get("nseq").getAsLong()); //当前数据，有效期，上一个元素，下一个元素
                batch.put(firstkey, pvalue); //第一个数据


                //设置新增数据及其指针
                curVal = __genValList(bt, -1, -1, Math.decrementExact(sseq));

                keySulBuf.resetWriterIndex();
                keySulBuf.writeLong(sseq);
                ByteBuf keyBuf = Unpooled.wrappedBuffer(keyPreBuf, keySulBuf);
                byte[] newKey = keyBuf.readBytes(keyBuf.readableBytes()).array();
                batch.put(newKey, curVal); //新增数据

            }
            //元素计数+1
            count++;

        }


        try {
            System.out.println("===============6");


            mydata.write(writeOpt, batch);


            metaBuf.resetWriterIndex();

            metaBuf.writeLong(count);  //数量
            metaBuf.writeLong(sseq);    //第一个元素
            metaBuf.writeLong(eseq);    //最后一个元素
            metaBuf.writeLong(sseq);    //最新主键编号

            System.out.println(String.format("count:%d 第一个元素：%d 最后一个元素：%d 自增主键：%d", count, sseq, eseq, sseq));

            metaBuf.resetReaderIndex();
            __put(mymeta, keyMeta, metaBuf.readBytes(metaBuf.readableBytes()).array(), -1);

        } catch (RocksDBException e) {
            throw new RuntimeException(e.getMessage());
        }
        return integer(count);
    }


    /**
     * 设置成功，返回 1 。 如果给定字段已经存在且没有操作被执行，返回 0 。
     *
     * @param key
     * @param field
     * @param value
     * @return
     * @throws RedisException
     */
    protected IntegerReply __hputnx(byte[] key, byte[] field, byte[] value) throws RedisException {
        boolean b = __hexists(key, field);
        if (b) {
            return integer(0);
        } else {
            __hput(key, field, value);
            return integer(1);
        }

    }

    /**
     * @param field
     * @param key
     * @return
     * @throws RedisException
     */
    protected List<BulkReply> __hmget(byte[] key, byte[][] fields) throws RedisException {

        List<BulkReply> list = new ArrayList<BulkReply>();

        //批量获取
        byte[] hkpre = "+".getBytes();
        byte[] hksuf = "hash".getBytes();
        byte[] fkpre = "_h".getBytes();

        //hash field key pre
        List<byte[]> listFds = new ArrayList<byte[]>();

        for (byte[] fd : fields
                ) {
            byte[] fkey = __genkey(fkpre, key, "|".getBytes(), fd);
            listFds.add(fkey);
        }

        list = __mget(listFds);

        return list;
    }

    protected List<BulkReply> __mget(List<byte[]> listFds) throws RedisException {
        List<BulkReply> list = new ArrayList<BulkReply>();

        try {
            Map<byte[], byte[]> fvals = mydata.multiGet(listFds);
//            System.out.println(fvals.size());
            for (byte[] fk : listFds
                    ) {
                byte[] val = __getValue(mydata, fk, fvals.get(fk));
                if (val != null) {
                    list.add(new BulkReply(val));
                } else list.add(NIL_REPLY);

            }
        } catch (RocksDBException e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
        return list;
    }


    protected void __hmput(byte[] key, byte[][] field_or_value1) throws RedisException {

        if (field_or_value1.length % 2 != 0) {
            throw new RedisException("wrong number of arguments for HMSET");
        }

        //处理 field key
        for (int i = 0; i < field_or_value1.length; i += 2) {
            byte[][] keys = _genhkey(key, field_or_value1[i]);
            field_or_value1[i] = keys[1];
        }

        __mput(field_or_value1);


    }

    protected void __mput(byte[][] field_or_value1) {
        final WriteOptions writeOpt = new WriteOptions();
        final WriteBatch batch = new WriteBatch();
        for (int i = 0; i < field_or_value1.length; i += 2) {

//            byte[][] keys = _genhkey(key, field_or_value1[i]);

            byte[] val = __genVal(field_or_value1[i + 1], -1);

//            batch.put(keys[1],val);
            batch.put(field_or_value1[i], val);

            //fixme  通过 keys 获取数量
//            executorService.execute(new Runnable() {
//                @Override
//                public void run() {
//                    try {
//                        __incrHMeta(keys,1); //fixme 异步
//                    } catch (RedisException e) {
//                        e.printStackTrace();
//                    }
////            __hput(key, field_or_value1[i], field_or_value1[i + 1]);
//                }
//            });


        }
        try {

            mydata.write(writeOpt, batch);

        } catch (RocksDBException e) {
            throw new RuntimeException(e.getMessage());
        }
    }


    protected IntegerReply __hlen(byte[] key0) throws RedisException {

        byte[] hkpre = "+".getBytes();
        byte[] hksuf = "hash".getBytes();
        byte[] fkpre = "_h".getBytes();

        byte[] hkey = __genkey(hkpre, key0, hksuf);

        return integer(__hkeys(hkey).size());

//        int i = _toint(__get(hkey));
//        return integer(i);
    }

    protected BulkReply _change(byte[] key0, double delta) throws RedisException {
        byte[] o = __get(key0);
        if (o == null) {
            byte[] bytes = _tobytes(delta);
            __put(key0, bytes);
            return new BulkReply(bytes);
        } else if (o instanceof byte[]) {
            try {
                double number = _todouble((byte[]) o) + delta;
                byte[] bytes = _tobytes(number);
                __put(key0, bytes);
                return new BulkReply(bytes);
            } catch (IllegalArgumentException e) {
                throw new RedisException(e.getMessage());
            }
        } else {
            throw notInteger();
        }
    }

    protected IntegerReply __change(byte[] key0, byte[] increment1) throws RedisException {

        System.out.println(_toint(increment1));

        return __change(key0, _toint(increment1));
    }


    /**
     * 自动增加;字符串形式保存
     *
     * @param key0
     * @param delta
     * @return
     * @throws RedisException
     */
    protected IntegerReply __change(byte[] key0, long delta) throws RedisException {
//          __get(key0);

        //hash meta count +1
//        ByteBuf hval = Unpooled.buffer(8);


        byte[] fbytes = __get(key0);
        if (fbytes != null) {

            long integer = bytesToNum((fbytes)) + delta;
//            hval.writeBytes(fbytes);

//            long count = hval.readLong() + delta;
//            hval.clear();
//            hval.writeLong(count);

//            System.out.println(delta + " count +:" + count);

            __put(key0, numToBytes(integer, false)); //key count +1
            return integer(integer);
        }

        //如果 field-key 不存在 不计数
//        hval.writeLong(delta);

        __put(key0, numToBytes(delta, false)); //key count +1

        return integer(delta);
//        throw notInteger();


//        byte[] o = __get(key0);
//        if (o == null) {
//            __put(key0, numToBytes(delta, false));
//            return integer(delta);
//        } else if (o instanceof byte[]) {
//            try {
//                long integer = bytesToNum((byte[]) o) + delta;
//                __put(key0, numToBytes(integer, false));
//                return integer(integer);
//            } catch (IllegalArgumentException e) {
//                throw new RedisException(e.getMessage());
//            }
//        } else {
//            throw notInteger();
//        }
    }

    /**
     * hash meta keys
     *
     * @param key
     * @return
     * @throws RedisException
     */
    protected List<byte[]> __hkeys(byte[] key) throws RedisException {

        byte[] hkpre = "+".getBytes();
        byte[] hksuf = "hash".getBytes();
        byte[] fkpre = "_h".getBytes();

        //hash field key pre
        byte[] fkeypre = __genkey(fkpre, key, "|".getBytes());

        List<byte[]> keys = new ArrayList<>();

        List<byte[]> bytes = __keys(mydata, fkeypre);

        for (byte[] bt : bytes
                ) {

            ByteBuf hkeybuf1 = Unpooled.wrappedBuffer(bt); //优化 零拷贝
            ByteBuf slice = hkeybuf1.slice(3 + key.length, bt.length - 3 - key.length);

            byte[] btv = slice.readBytes(slice.readableBytes()).array();

            keys.add(btv);
        }
        return keys;

    }

    /**
     * 按前缀检索所有的 keys
     *
     * @param pattern0
     * @return
     */
    protected List<byte[]> __keys(RocksDB data, byte[] pattern0) {
//                        System.out.println("bbbbbbbb"+new String(pattern0));

        //按 key 检索所有数据
        List<byte[]> keys = new ArrayList<>();
        try (final RocksIterator iterator = data.newIterator()) {
            for (iterator.seek(pattern0); iterator.isValid(); iterator.next()) {


                //确保检索有数据，hkeybuf.slice 不错误
                if (pattern0.length <= iterator.key().length) {
                    ByteBuf hkeybuf = Unpooled.wrappedBuffer(iterator.key()); //优化 零拷贝
                    ByteBuf slice = hkeybuf.slice(0, pattern0.length); //获取指定前缀长度的 byte[]

                    slice.resetReaderIndex();

                    //key有序 不相等后面无数据
                    if (Arrays.equals(slice.readBytes(slice.readableBytes()).array(), pattern0)) {

                        keys.add(iterator.key());
                        if (keys.size() >= 100000) {
                            //数据大于1万条直接退出
                            break;
                        }
                    } else {
                        break;
                    }
                } else break;

            }
        }

        //检索过期数据,处理过期数据 ;暂不处理影响效率 fixme
//        System.out.println(keys.size());

        return keys;
    }

    protected List<byte[]> __keyVals(RocksDB data, byte[] pattern0) {

        //按 key 检索所有数据
        List<byte[]> keys = new ArrayList<>();
        try (final RocksIterator iterator = data.newIterator()) {
            for (iterator.seek(pattern0); iterator.isValid(); iterator.next()) {

                //确保检索有数据，hkeybuf.slice 不错误
                if (pattern0.length <= iterator.key().length) {
                    ByteBuf hkeybuf = Unpooled.wrappedBuffer(iterator.key()); //优化 零拷贝
                    ByteBuf slice = hkeybuf.slice(0, pattern0.length); //获取指定前缀长度的 byte[]

                    slice.resetReaderIndex();

                    //key有序 不相等后面无数据
                    if (Arrays.equals(slice.readBytes(slice.readableBytes()).array(), pattern0)) {


                        byte[] value = __getValue(data, iterator.key(), iterator.value());

                        if (value != null) {
                            keys.add(iterator.key());
                            keys.add(value);
                        }

                        if (keys.size() >= 100000) {
                            //数据大于1万条直接退出  fixme
                            break;
                        }
                    } else {
                        break;
                    }
                } else break;

            }
        } catch (RedisException e) {
            e.printStackTrace();
        }

        //检索过期数据,处理过期数据 ;暂不处理影响效率 fixme
//        System.out.println(keys.size());

        return keys;
    }


    protected MultiBulkReply __hgetall(byte[] key) throws RedisException {

        byte[] hkpre = "+".getBytes();
        byte[] hksuf = "hash".getBytes();
        byte[] fkpre = "_h".getBytes();

        //hash field key pre
        byte[] fkeypre = __genkey(fkpre, key, "|".getBytes());

        //检索所有的 hash field key  所有的 key 都是有序的
        List<Reply<ByteBuf>> replies = new ArrayList<Reply<ByteBuf>>();


        List<byte[]> keyVals = __keyVals(mydata, fkeypre);//顺序读取 ;field 过期逻辑复杂，暂不处理


        int i = 0;
        for (byte[] bt : keyVals
                ) {
            if (i % 2 == 0) {  //key 处理
                ByteBuf hkeybuf1 = Unpooled.wrappedBuffer(bt); //优化 零拷贝

                ByteBuf slice = hkeybuf1.slice(3 + key.length, bt.length - 3 - key.length);

                replies.add(new BulkReply(slice.readBytes(slice.readableBytes()).array()));
            } else {

                replies.add(new BulkReply(bt));
            }
            i++;
        }

//        try (final RocksIterator iterator = mydata.newIterator()) {
//            for (iterator.seek(fkeypre); iterator.isValid(); iterator.next()) {
//
//                ByteBuf hkeybuf = Unpooled.buffer(16);
//                hkeybuf.writeBytes(iterator.key(), 0, fkeypre.length);
//
//                if (Arrays.equals(hkeybuf.readBytes(hkeybuf.readableBytes()).array(), fkeypre)) {
//
//                    ByteBuf hkeybuf1 = Unpooled.buffer(16);
//                    hkeybuf1.writeBytes(iterator.key());
////
//////                    hkeybuf.setIndex(key.length,0);
////                    hkeybuf.resetReaderIndex();
////
////                    System.out.println("&&&&&&&&&&&&&&&&&");
////                    System.out.println(new String(hkeybuf.readBytes(hkeybuf.readableBytes()).array()));
//
//                    byte[] fkey = new byte[iterator.key().length - 2 - key.length];
//                    hkeybuf1.getBytes(2 + key.length, fkey);
//
//
//                    byte[] val = __get(iterator.key());
//                    if (val != null) {
//
////                        replies.add(new BulkReply(hkeybuf.readBytes(hkeybuf.readableBytes()).array()));
//                        replies.add(new BulkReply(fkey));
//                        replies.add(new BulkReply(val));
//
//                        if (replies.size() >= 100000) {
//                            //数据大于1万条直接退出
//                            break;
//                        }
//
//                    }
//
//                } else {
//                    break;
//                }
//
//            }
//        }
        return new MultiBulkReply(replies.toArray(new Reply[replies.size()]));
    }


    protected boolean __hexists(byte[] key0, byte[] field) throws RedisException {
        byte[][] keys = _genhkey(key0, field);
        return __exists(keys[1]);
    }

    /**
     * hash meta count -1
     *
     * @param key
     * @param field
     * @return
     * @throws RedisException
     */
    protected int __hdel(byte[] key, byte[] field) throws RedisException {

        byte[][] keys = _genhkey(key, field);

        __reduHMeta(keys, 1);

        __del(keys[1]);

        return 1;
    }

    /**
     * 考虑数据过期情况
     *
     * @param key0
     * @return
     * @throws RedisException
     */
    protected boolean __exists(byte[] key0) throws RedisException {
        return __get(key0) == null ? false : true;
    }

    /**
     * @param key
     * @param field
     * @return
     * @throws RedisException
     */
    protected byte[] __hget(byte[] key, byte[] field) throws RedisException {

        byte[][] keys = _genhkey(key, field);

        int cnt = __gethmeta(keys);

        byte[] fval = __get(keys[1]);

        return fval;
    }

    /**
     * Hash 数据保存 meta count 持久化;不提供 fkey 过期，否则计数处理太复杂，无意义
     * <p>
     * 3次交互
     *
     * @param key
     * @param field
     * @param value
     * @return
     */
    protected byte[] __hput(byte[] key, byte[] field, byte[] value) throws RedisException {

        byte[][] keys = _genhkey(key, field);

        //可以并行  fixme 并行
        __incrHMeta(keys, 1);
        __put(keys[1], value);

        return value;
    }

    /**
     * 查询是否过期
     *
     * @param keys
     * @return
     * @throws RedisException
     */
    private int __gethmeta(byte[][] keys) throws RedisException {
        //hash meta count +1

        byte[] metaVal = __get(mymeta, keys[0]);

        if (metaVal == null || metaVal.length == 0) {

            return 0;

        } else {
            ByteBuf vvBuf = Unpooled.wrappedBuffer(metaVal);

            return vvBuf.readInt();

        }
    }

    /**
     * 存储hash元数据
     * 存储字段的数量
     *
     * @param keys
     * @throws RedisException
     */
    private void __incrHMeta(byte[][] keys, int incr) throws RedisException {
        //hash meta count +1
//        int incr=1;
//fixme 用 hlens 优化
        byte[] metaVal = __get(mymeta, keys[0]);

        if (metaVal == null || metaVal.length == 0) {

            System.out.println("hash count ==1:");

            ByteBuf hval = Unpooled.buffer(4);
            hval.writeInt(1);
            __put(mymeta, keys[0], hval.array(), -1); //key count +1

        } else {

            //如果 field-key 不存在 则计数；存在不进行计数
            byte[] fbytes = __get(keys[1]);

            if (fbytes == null || fbytes.length == 0) {

                ByteBuf hval = Unpooled.wrappedBuffer(metaVal);  //零拷贝优化

//                hval.writeBytes(metaVal);

                int count = hval.readInt() + incr;

                hval.clear();
                hval.writeInt(count);

                System.out.println("hash count +1:" + hval.readInt());

                __put(mymeta, keys[0], hval.array(), -1); //key count +1

            }
        }
    }

    private int __reduHMeta(byte[][] keys, int redu) throws RedisException {

        byte[] metaVal = __get(mymeta, keys[0]);

        if (metaVal == null || metaVal.length == 0) {

            System.out.println("hash count ==0:");

            ByteBuf hval = Unpooled.buffer(4);
            hval.writeInt(0);
            __put(mymeta, keys[0], hval.array(), -1); //key count +1

            return 0;

        } else {

            //如果 field-key 不存在 则计数；存在不进行计数
            byte[] fbytes = __get(keys[1]);

            if (fbytes != null) {

                ByteBuf hval = Unpooled.wrappedBuffer(metaVal);  //零拷贝优化

                int count = hval.readInt() - redu;

                hval.clear();

                hval.writeInt(count);

                System.out.println("hash count +1:" + hval.readInt());

                __put(mymeta, keys[0], hval.array(), -1); //key count +1

                return count;

            }
        }
        return redu;
    }

    /**
     * 删除数据
     *
     * @param bytes
     * @throws RedisException
     */
    protected void __del(byte[] bytes) throws RedisException {
        __del(mydata, bytes);
    }


    private void __del(RocksDB db, byte[] bytes) throws RedisException {
        try {
            db.delete(bytes);
        } catch (RocksDBException e) {
            throw new RedisException(e.getMessage());
        }
    }

    /**
     * RocksDb 存储数据
     * 实现零拷贝
     * 过期数据处理
     *
     * @param key0
     * @return
     */
    protected byte[] __get(byte[] key0) throws RedisException {
        return __get(mydata, key0);
    }

    /**
     * 获取数据
     *
     * @param data
     * @param key0
     * @return
     * @throws RedisException
     */
    protected byte[] __get(RocksDB data, byte[] key0) throws RedisException {
        try {

            byte[] values = data.get(key0);
            return __getValue(data, key0, values);

        } catch (RocksDBException e) {
            throw new RedisException(e.getMessage());
        }
    }

    /**
     * 提取数据；
     * 删除过期数据
     *
     * @param key0
     * @param values
     * @return
     * @throws RedisException
     */
    private byte[] __getValue(RocksDB db, byte[] key0, byte[] values) throws RedisException {
        if (values != null) {

            ByteBuf vvBuf = Unpooled.wrappedBuffer(values);
            vvBuf.resetReaderIndex();
            ByteBuf ttlBuf = vvBuf.readSlice(8);
            ByteBuf sizeBuf = vvBuf.readSlice(4);
            ByteBuf valueBuf = vvBuf.slice(8 + 4, values.length - 8 - 4);

            long ttl = ttlBuf.readLong();//ttl
            long size = sizeBuf.readInt();//长度数据

            //数据过期处理
            if (ttl < now() && ttl != -1) {
                __del(db, key0);
                return null;
            }

            byte[] array = valueBuf.readBytes(valueBuf.readableBytes()).array();

//                System.out.println("get key:" + new String(key0));
//                System.out.println("get value:" + new String(array));


            return array;

        } else return null; //数据不存在 ？ 测试验证
    }

    /**
     * 列表数据值获取
     * byte 可复用，零拷贝
     *
     * @param db
     * @param key0
     * @param values
     * @return
     * @throws RedisException
     */
    private byte[] __getValueList(RocksDB db, byte[] key0, JsonObject jo) throws RedisException {
//        JsonObject jo =new JsonObject();

        byte[] values = null;
        try {
            values = db.get(key0);

            System.out.println("get value:" + values);
            System.out.println("get value:" + new String(values));

        } catch (RocksDBException e) {
            throw new RedisException(e.getMessage());
        }

        if (values != null) {


            ByteBuf vvBuf = Unpooled.wrappedBuffer(values);

            ByteBuf ttlBuf = vvBuf.readSlice(8);  //ttl
            ByteBuf sizeBuf = vvBuf.readSlice(4); //length
            ByteBuf pseqBuf = vvBuf.readSlice(8); //p
            ByteBuf nseqBuf = vvBuf.readSlice(8); //n
            ByteBuf valueBuf = vvBuf.slice(8 + 4 + 8 + 8, values.length - 8 - 4 - 8 - 8);


            long ttl = ttlBuf.readLong();//ttl
            long size = sizeBuf.readInt();//长度数据
            long pseq = pseqBuf.readLong();
            long nseq = nseqBuf.readLong();

            System.out.println(String.format("过期时间：%d 数据长度：%d 上指针：%d 下指针：%d 数据%s", ttl, size, pseq, nseq, new String(valueBuf.array())));

            //数据过期处理
            if (ttl < now() && ttl != -1) {
                __del(db, key0);
                return null;
            }

            valueBuf.resetReaderIndex();

            byte[] array = valueBuf.readBytes(valueBuf.readableBytes()).array();

            System.out.println("get value:" + new String(array));

            jo.addProperty("ttl", ttl);
            jo.addProperty("size", size);
            jo.addProperty("pseq", pseq);
            jo.addProperty("nseq", nseq);

            return array;

        } else return null; //数据不存在 ？ 测试验证
    }

    /**
     * key/val
     * <p>
     * value结构 ：ttl-size-value
     * <p>
     * get-8Byte ttl 获取指定字节长度
     * get-4Byte size
     *
     * @param key
     * @param value
     * @return
     */
    protected byte[] __put(byte[] key, byte[] value) {

        return __put(mydata, key, value, -1);
    }

    /**
     * key/val
     * <p>
     * value结构 ：ttl-size-value
     * <p>
     * get-8Byte ttl 获取指定字节长度
     * get-4Byte size
     *
     * @param data
     * @param key
     * @param value
     * @param expiration
     * @return
     */
    protected byte[] __put(RocksDB data, byte[] key, byte[] value, long expiration) {
//        System.out.println("ppppppppppppppppppppppppppppp");
        try {


            byte[] bt = __genVal(value, expiration);

//            System.out.println("data byte length:" + bt.length);
//            System.out.println("db value:" + new String(bt));
//            System.out.println("set value:" + new String(value));

            data.put(key, bt);
        } catch (RocksDBException e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }

        return value;
    }

    /**
     * @param value
     * @param expiration
     * @return
     */
    private byte[] __genVal(byte[] value, long expiration) {
        ByteBuf ttlBuf = Unpooled.buffer(12);
        ttlBuf.writeLong(expiration); //ttl 无限期 -1
        ttlBuf.writeInt(value.length); //value size

        ByteBuf valueBuf = Unpooled.wrappedBuffer(value); //零拷贝
        ByteBuf valbuf = Unpooled.wrappedBuffer(ttlBuf, valueBuf);//零拷贝

        return valbuf.readBytes(valbuf.readableBytes()).array();
    }

    /**
     * 列表数据值生成
     *
     * @param value
     * @param expiration
     * @param pseq
     * @param nseq
     * @return
     */
    private byte[] __genValList(byte[] value, long expiration, long pseq, long nseq) {

        ByteBuf ttlBuf = Unpooled.buffer(12);
        ttlBuf.writeLong(expiration); //ttl 无限期 -1
        ttlBuf.writeInt(value.length); //value size
        ttlBuf.writeLong(pseq); //上一个元素
        ttlBuf.writeLong(nseq); //下一个元素

//            ttlBuf.writeBytes(value); //value

        ByteBuf valueBuf = Unpooled.wrappedBuffer(value); //零拷贝
        ByteBuf valbuf = Unpooled.wrappedBuffer(ttlBuf, valueBuf);//零拷贝
//            valueBuf.writeLong(expiration); //ttl 无限期 -1
//            valueBuf.writeInt(value.length); //value size

//            byte[] bt = new byte[valbuf.readableBytes()];
//            valbuf.readBytes(bt);

        return valbuf.readBytes(valbuf.readableBytes()).array();
    }


    /**
     * 构建 hashkey
     * <p>
     * 相似 hash (key+filed) 在 seek 的时候会出现混淆
     *
     * @param keys
     * @param key
     * @param field
     */
    private byte[][] _genhkey(byte[] key, byte[] field) {
        byte[][] keys = new byte[2][];

        byte[] hkpre = "+".getBytes();
        byte[] hksuf = "hash".getBytes();
        byte[] fkpre = "_h".getBytes();

        byte[] hkey = __genkey(hkpre, key, hksuf);
        byte[] fkey = __genkey(fkpre, key, "|".getBytes(), field);

        keys[0] = hkey;
        keys[1] = fkey;

        return keys;
    }

//    /**
//     * 组装 key
//     *
//     * @param hkpre
//     * @param key
//     * @param hksuf
//     */
//    private byte[] _genkey(byte[] hkpre, byte[] key, byte[] hksuf) {
//
//        return __genkey(hkpre,key,hksuf);
//
////
//////        ByteBuf buf1 = Unpooled.buffer(16);
//////        buf1.writeBytes(hkpre);
//////        buf1.writeBytes(key);
////
////        ByteBuf buf1 = Unpooled.wrappedBuffer(hkpre, key);  //优化，零拷贝
////
////        if (hksuf != null) {
////
////
////            ByteBuf buf2 = Unpooled.wrappedBuffer(hksuf);
//////            buf1.writeBytes(hksuf);
////            ByteBuf buf3 = Unpooled.wrappedBuffer(buf1, buf2);
////            buf3.resetReaderIndex();
////            byte[] array = buf3.readBytes(buf3.readableBytes()).array();
////
////            System.out.println(String.format("buf3组合键为 %s", new String(array)));
////            return array;
////
////        } else {
////            byte[] array = buf1.readBytes(buf1.readableBytes()).array();
////
////            System.out.println(String.format("buf1组合键为 %s", new String(array)));
////            return array;
////        }
//
//
//    }

    /**
     * 组合key 不允许为null
     *
     * @param keys
     * @return
     */
    protected static byte[]  __genkey(byte[]... keys) {
        ByteBuf buf3 = Unpooled.wrappedBuffer(keys);
        byte[] array = buf3.readBytes(buf3.readableBytes()).array();
        System.out.println(String.format("buf3组合键为 %s", new String(array)));
        return array;
    }

    ///////////////////////////
    protected static int _toposint(byte[] offset1) throws RedisException {
        long offset = bytesToNum(offset1);
        if (offset < 0 || offset > MAX_VALUE) {
            throw notInteger();
        }
        return (int) offset;
    }

    protected static int _toint(byte[] offset1) throws RedisException {
        long offset = bytesToNum(offset1);
        if (offset > MAX_VALUE) {
            throw notInteger();
        }
        return (int) offset;
    }

    protected static int _torange(byte[] offset1, int length) throws RedisException {
        long offset = bytesToNum(offset1);
        if (offset > MAX_VALUE) {
            throw notInteger();
        }
        if (offset < 0) {
            offset = (length + offset);
        }
        if (offset >= length) {
            offset = length - 1;
        }
        return (int) offset;
    }


    protected static int _torange(byte[] offset1, long length) throws RedisException {
        long offset = bytesToNum(offset1);
        if (offset > MAX_VALUE) {
            throw notInteger();
        }
        if (offset < 0) {
//            offset = (length + offset + 1);
            offset = (length + offset);
        }
        if (offset >= length) {
//            offset = length - 1;
            offset = length;
        }
        return (int) offset;
    }

    protected byte[] _tobytes(double score) {
        return String.valueOf(score).getBytes();
    }

    @SuppressWarnings("unchecked")
    protected BytesKeySet _getset(byte[] key0, boolean create) throws RedisException {
        Object o = _get(key0);
        if (o == null) {
            o = new BytesKeySet();
            if (create) {

                try {
                    mydata.put(key0, ObjectToByte(o));
                } catch (RocksDBException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e.getMessage());
                }

                data.put(key0, o);
            }
        }
        if (!(o instanceof BytesKeySet)) {
            throw invalidValue();
        }
        return (BytesKeySet) o;
    }

    @SuppressWarnings("unchecked")
    protected ZSet _getzset(byte[] key0, boolean create) throws RedisException {
        Object o = _get(key0);
        if (o == null) {
            o = new ZSet();
            if (create) {

                try {
                    mydata.put(key0, ObjectToByte(o));
                } catch (RocksDBException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e.getMessage());
                }

                data.put(key0, o);
            }
        }
        if (!(o instanceof ZSet)) {
            throw invalidValue();
        }
        return (ZSet) o;
    }


    @Deprecated
    protected Object _get(byte[] key0) {
        Object o = data.get(key0);
        if (o != null) {
            Long l = expires.get(key0);
            if (l != null) {
                if (l < now()) {
                    data.remove(key0);
                    return null;
                }
            }
        }
        return o;
    }

    protected IntegerReply _change(byte[] key0, long delta) throws RedisException {
        byte[] o = __get(key0);
        if (o == null) {
            __put(key0, numToBytes(delta, false));
            return integer(delta);
        } else if (o instanceof byte[]) {
            try {
                long integer = bytesToNum((byte[]) o) + delta;
                __put(key0, numToBytes(integer, false));
                return integer(integer);
            } catch (IllegalArgumentException e) {
                throw new RedisException(e.getMessage());
            }
        } else {
            throw notInteger();
        }
    }


    protected static int _test(byte[] bytes, long offset) throws RedisException {
        long div = offset / 8;
        if (div > MAX_VALUE) throw notInteger();
        int i;
        if (bytes.length < div + 1) {
            i = 0;
        } else {
            int mod = (int) (offset % 8);
            int value = bytes[((int) div)] & 0xFF;
            i = value & mask[mod];
        }
        return i != 0 ? 1 : 0;
    }

    protected byte[] _getbytes(byte[] aKey2) throws RedisException {
        byte[] src;
        Object o = _get(aKey2);
        if (o instanceof byte[]) {
            src = (byte[]) o;
        } else if (o != null) {
            throw invalidValue();
        } else {
            src = new byte[0];
        }
        return src;
    }

    @SuppressWarnings("unchecked")
    protected List<BytesValue> _getlist(byte[] key0, boolean create) throws RedisException {
        Object o = _get(key0);
        if (o instanceof List) {
            return (List<BytesValue>) o;
        } else if (o == null) {
            if (create) {
                ArrayList<BytesValue> list = new ArrayList<BytesValue>();
                _put(key0, list);
                return list;
            } else {
                return null;
            }
        } else {
            throw invalidValue();
        }
    }

    protected Object _put(byte[] key, Object value) {
        expires.remove(key);
        return data.put(key, value);
    }


    protected Object _put(byte[] key, byte[] value, long expiration) {
        try {
            myexpires.put(key, ObjectToByte(expiration));
            mydata.put(key, ObjectToByte(value));
        } catch (RocksDBException e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }

        expires.put(key, expiration);
        return data.put(key, value);
    }


    protected boolean __existsTTL(byte[] key0) throws RedisException {
//        Object o = _get(key0);
        StringBuilder sb = new StringBuilder();
        boolean o = myexpires.keyMayExist(key0, sb);
        return o;
    }


    protected static byte[] byteMerger(byte[] byte_1, byte[] byte_2) {
        StringBuilder sb = new StringBuilder();
        byte[] byte_3 = new byte[byte_1.length + byte_2.length];
        System.arraycopy(byte_1, 0, byte_3, 0, byte_1.length);
        System.arraycopy(byte_2, 0, byte_3, byte_1.length, byte_2.length);
        return byte_3;
    }


    protected static RocksDB mydata = getDb("netty4-server/db/data");
    protected static RocksDB mymeta = getDb("netty4-server/db/meta");
    protected static RocksDB myexpires = getDb("netty4-server/db/expires");

    protected static RocksDB getDb(String filename) {
//    String filename = env.getRequiredProperty(filename);

        System.out.println("rocks db path:" + filename);

        RocksDB.loadLibrary();
        // the Options class contains a set of configurable DB options
        // that determines the behavior of a database.
        //默认设置性能最好
        //get ops 46202.18 requests per second
        //set ops 25489.40 requests per second

//        try {
//            return RocksDB.open(filename);
//        } catch (RocksDBException e) {
//            e.printStackTrace();
//        }
//        return null;


        final Options options = new Options();

//        final Statistics stats = new Statistics();

        try {
            options.setCreateIfMissing(true)
                    .setWriteBufferSize(8 * SizeUnit.KB)
                    .setMaxWriteBufferNumber(3)
                    .setMaxBackgroundCompactions(2)
                    .setCompressionType(CompressionType.SNAPPY_COMPRESSION)
                    .setCompactionStyle(CompactionStyle.UNIVERSAL);
        } catch (final IllegalArgumentException e) {
            assert (false);
        }

        options.setMemTableConfig(new SkipListMemTableConfig());

        final StringAppendOperator stringAppendOperator = new StringAppendOperator();
        options.setMergeOperator(stringAppendOperator);


        RocksDB db = null;
        try {
            // a factory method that returns a RocksDB instance
            db = RocksDB.open(options, filename);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        return db;
    }

    public static byte[] ObjectToByte(java.lang.Object obj) {
        byte[] bytes = null;
        try {
            // object to bytearray
            ByteArrayOutputStream bo = new ByteArrayOutputStream();
            ObjectOutputStream oo = new ObjectOutputStream(bo);
            oo.writeObject(obj);

            bytes = bo.toByteArray();

            bo.close();
            oo.close();
        } catch (Exception e) {
            System.out.println("translation" + e.getMessage());
            e.printStackTrace();
        }
        return bytes;
    }

    public static Object ByteToObject(byte[] bytes) {
        Object obj = null;
        try {
            // bytearray to object
            ByteArrayInputStream bi = new ByteArrayInputStream(bytes);
            ObjectInputStream oi = new ObjectInputStream(bi);

            obj = oi.readObject();
            bi.close();
            oi.close();
        } catch (Exception e) {
            System.out.println("translation" + e.getMessage());
            e.printStackTrace();
        }
        return obj;
    }

    @SuppressWarnings("unchecked")
    protected BytesKeyObjectMap<byte[]> _gethash(byte[] key0, boolean create) throws RedisException {
        Object o = _get(key0);
        if (o == null) {
            o = new BytesKeyObjectMap();
            if (create) {
                try {
                    mydata.put(key0, ObjectToByte(o));
                } catch (RocksDBException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e.getMessage());
                }


                data.put(key0, o);
            }
        }
        if (!(o instanceof HashMap)) {
            throw invalidValue();
        }
        return (BytesKeyObjectMap<byte[]>) o;
    }

    protected int mydbsize() {
        final List<byte[]> keys = new ArrayList<>();
        try (final RocksIterator iterator = mydata.newIterator()) {
            for (iterator.seekToLast(); iterator.isValid(); iterator.prev()) {
                keys.add(iterator.key());
            }
        }
        return keys.size();
    }

    protected void dbclear() {

        final List<byte[]> keys = new ArrayList<>();
        try (final RocksIterator iterator = mydata.newIterator()) {
            for (iterator.seekToLast(); iterator.isValid(); iterator.prev()) {
                keys.add(iterator.key());
            }
        }

        //删除所有数据
        try (final WriteOptions writeOpt = new WriteOptions()) {
            try (final WriteBatch batch = new WriteBatch()) {
                for (byte[] key1 : keys) {
                    batch.remove(key1);
                }
                mydata.write(writeOpt, batch);
            } catch (RocksDBException e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * Delete a key
     * Generic
     *
     * @param key0
     * @return IntegerReply
     */
    public IntegerReply del(byte[][] key0) throws RedisException {
        System.out.println("del......");
        int total = 0;
        for (byte[] bytes : key0) {

            byte[] key = __genkey(bytes, "|".getBytes(), "string".getBytes());

            __del(key);

            total++;

        }
        return integer(total);
    }


    protected BytesKeySet _sdiff(byte[][] key0) throws RedisException {
        BytesKeySet set = null;
        for (byte[] key : key0) {
            if (set == null) {
                set = new BytesKeySet();
                set.addAll(_getset(key, false));
            } else {
                BytesKeySet c = _getset(key, false);
                set.removeAll(c);
            }
        }
        if (set == null) {
            throw new RedisException("wrong number of arguments for 'sdiff' command");
        }
        return set;
    }

    protected BytesKeySet _sinter(byte[][] key0) throws RedisException {
        BytesKeySet set = null;
        for (byte[] key : key0) {
            if (set == null) {
                set = _getset(key, false);
            } else {
                BytesKeySet inter = new BytesKeySet();
                BytesKeySet newset = _getset(key, false);
                for (BytesKey bytesKey : newset) {
                    if (set.contains(bytesKey)) {
                        inter.add(bytesKey);
                    }
                }
                set = inter;
            }
        }
        if (set == null) {
            throw new RedisException("wrong number of arguments for 'sinter' command");
        }
        return set;
    }

    protected MultiBulkReply _setreply(BytesKeySet set) {
        Reply[] replies = new Reply[set.size()];
        int i = 0;
        for (BytesKey value : set) {
            replies[i++] = new BulkReply(value.getBytes());
        }
        return new MultiBulkReply(replies);
    }

    protected BytesKeySet _sunion(byte[][] key0) throws RedisException {
        BytesKeySet set = null;
        for (byte[] key : key0) {
            if (set == null) {
                set = new BytesKeySet();
                set.addAll(_getset(key, false));
            } else {
                set.addAll(_getset(key, false));
            }
        }
        if (set == null) {
            throw new RedisException("wrong number of arguments for 'sunion' command");
        }
        return set;
    }

    protected double _todouble(byte[] score) {
        return parseDouble(new String(score));
    }

    protected IntegerReply _zstore(byte[] destination0, byte[] numkeys1, byte[][] key2, String name, boolean union) throws RedisException {
        if (destination0 == null || numkeys1 == null) {
            throw new RedisException("wrong number of arguments for '" + name + "' command");
        }
        int numkeys = _toint(numkeys1);
        if (key2.length < numkeys) {
            throw new RedisException("wrong number of arguments for '" + name + "' command");
        }
        int position = numkeys;
        double[] weights = null;
        RocksdbRedisServer.Aggregate type = null;
        if (key2.length > position) {
            if ("weights".equals(new String(key2[position]).toLowerCase())) {
                position++;
                if (key2.length < position + numkeys) {
                    throw new RedisException("wrong number of arguments for '" + name + "' command");
                }
                weights = new double[numkeys];
                for (int i = position; i < position + numkeys; i++) {
                    weights[i - position] = _todouble(key2[i]);
                }
                position += numkeys;
            }
            if (key2.length > position + 1) {
                if ("aggregate".equals(new String(key2[position]).toLowerCase())) {
                    type = RocksdbRedisServer.Aggregate.valueOf(new String(key2[position + 1]).toUpperCase());
                }
            } else if (key2.length != position) {
                throw new RedisException("wrong number of arguments for '" + name + "' command");
            }
        }
        del(new byte[][]{destination0});
        ZSet destination = _getzset(destination0, true);
        for (int i = 0; i < numkeys; i++) {
            ZSet zset = _getzset(key2[i], false);
            if (i == 0) {
                if (weights == null) {
                    destination.addAll(zset);
                } else {
                    double weight = weights[i];
                    for (ZSetEntry entry : zset) {
                        destination.add(entry.getKey(), entry.getScore() * weight);
                    }
                }
            } else {
                for (ZSetEntry entry : zset) {
                    BytesKey key = entry.getKey();
                    ZSetEntry current = destination.get(key);
                    destination.remove(key);
                    if (union || current != null) {
                        double newscore = entry.getScore() * (weights == null ? 1 : weights[i]);
                        if (type == null || type == RocksdbRedisServer.Aggregate.SUM) {
                            if (current != null) {
                                newscore += current.getScore();
                            }
                        } else if (type == RocksdbRedisServer.Aggregate.MIN) {
                            if (current != null && newscore > current.getScore()) {
                                newscore = current.getScore();
                            }
                        } else if (type == RocksdbRedisServer.Aggregate.MAX) {
                            if (current != null && newscore < current.getScore()) {
                                newscore = current.getScore();
                            }
                        }
                        destination.add(key, newscore);
                    }
                }
                if (!union) {
                    for (ZSetEntry entry : new ZSet(destination)) {
                        BytesKey key = entry.getKey();
                        if (zset.get(key) == null) {
                            destination.remove(key);
                        }
                    }
                }
            }
        }
        return integer(destination.size());
    }

    protected boolean _checkcommand(byte[] check, String command, boolean syntax) throws RedisException {
        boolean result;
        if (check != null) {
            if (new String(check).toLowerCase().equals(command)) {
                result = true;
            } else {
                if (syntax) {
                    throw new RedisException("syntax error");
                } else {
                    return false;
                }
            }
        } else {
            result = false;
        }
        return result;
    }

    protected List<Reply<ByteBuf>> _zrangebyscore(byte[] min1, byte[] max2, byte[][] withscores_offset_or_count4, ZSet zset, boolean reverse) throws RedisException {
        int position = 0;
        boolean withscores = false;
        if (withscores_offset_or_count4.length > 0) {
            withscores = _checkcommand(withscores_offset_or_count4[0], "withscores", false);
        }
        if (withscores) position++;
        boolean limit = false;
        if (withscores_offset_or_count4.length > position) {
            limit = _checkcommand(withscores_offset_or_count4[position++], "limit", true);
        }
        if (withscores_offset_or_count4.length != position + (limit ? 2 : 0)) {
            throw new RedisException("syntax error");
        }
        int offset = 0;
        int number = Integer.MAX_VALUE;
        if (limit) {
            offset = _toint(withscores_offset_or_count4[position++]);
            number = _toint(withscores_offset_or_count4[position]);
            if (offset < 0 || number < 1) {
                throw notInteger();
            }
        }
        RocksdbRedisServer.Score min = _toscorerange(min1);
        RocksdbRedisServer.Score max = _toscorerange(max2);
        List<ZSetEntry> entries = zset.subSet(min.value, max.value);
        if (reverse) Collections.reverse(entries);
        int current = 0;
        List<Reply<ByteBuf>> list = new ArrayList<Reply<ByteBuf>>();
        for (ZSetEntry entry : entries) {
            if (current >= offset && current < offset + number) {
                list.add(new BulkReply(entry.getKey().getBytes()));
                if (withscores) list.add(new BulkReply(_tobytes(entry.getScore())));
            }
            current++;
        }
        return list;
    }

    protected RocksdbRedisServer.Score _toscorerange(byte[] specifier) {
        RocksdbRedisServer.Score score = new RocksdbRedisServer.Score();
        String s = new String(specifier).toLowerCase();
        if (s.startsWith("(")) {
            score.inclusive = false;
            s = s.substring(1);
        }
        if (s.equals("-inf")) {
            score.value = Double.NEGATIVE_INFINITY;
        } else if (s.equals("inf") || s.equals("+inf")) {
            score.value = Double.POSITIVE_INFINITY;
        } else {
            score.value = Double.parseDouble(s);
        }
        return score;
    }

    protected Reply _zrank(byte[] member1, List<ZSetEntry> zset) {
        BytesKey member = new BytesKey(member1);
        int position = 0;
        for (ZSetEntry entry : zset) {
            if (entry.getKey().equals(member)) {
                return integer(position);
            }
            position++;
        }
        return NIL_REPLY;
    }
}
