package redis.server.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;
import redis.netty4.BulkReply;
import redis.netty4.IntegerReply;
import redis.netty4.MultiBulkReply;
import redis.netty4.Reply;
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
import static redis.util.Encoding.bytesToNum;
import static redis.util.Encoding.numToBytes;

/**
 * Rocksdb 相关的方法操作
 * <p>
 * 核心指令 __put  __get  __del  __exists
 * __keys  按前缀检索
 * __hkeys 按前缀检索
 * __hput __inclHMeta Hash 指令
 * __hget
 * __hdel __reduHMeta
 * __hexists
 * __hputnx
 *
 * __change（incr）
 *
 * 批量指令 __hmset(RocksDb 批量操作) __hmget(mget 批量获取)  __hgetall(批量seek)
 * </p>
 * Created by moyong on 2017/10/20.
 */
public class RocksdbRedis extends RedisBase {

    /**
     *
     * Redis Lrem 根据参数 COUNT 的值，移除列表中与参数 VALUE 相等的元素。
     COUNT 的值可以是以下几种：
     count > 0 : 从表头开始向表尾搜索，移除与 VALUE 相等的元素，数量为 COUNT 。
     count < 0 : 从表尾开始向表头搜索，移除与 VALUE 相等的元素，数量为 COUNT 的绝对值。
     count = 0 : 移除表中所有与 VALUE 相等的值。
     *
     * @param key0
     * @param count1
     * @param value2
     * @return
     * @throws RedisException
     */
    protected IntegerReply __lrem(byte[] key0, byte[] count1, byte[] value2) throws RedisException {

        //初始化List 开始于结束指针
        long metaStart = 0;
        long metaEnd = 0;
        //获取元数据的索引值
        byte[] keyMeta = __genkey("+".getBytes(), key0, "list".getBytes());
        byte[] valMeta = __get(mymeta, keyMeta); //'0,2'  开始指针，结束指针

        ByteBuf metaBuf = null;
        ByteBuf startBuf = null;
        ByteBuf endBuf = null;

        if(valMeta == null){
            System.out.println("===============2");

            return integer(0);

        }else{
            System.out.println("===============3");

            metaBuf = Unpooled.wrappedBuffer(valMeta);

            startBuf=metaBuf.slice(0,8);
            endBuf=metaBuf.slice(8,8);

            metaStart = metaBuf.readLong();
            metaEnd = metaBuf.readLong();

            //数据列表为空，清除元数据
            if((metaStart-metaEnd) == 0){
                __del(keyMeta);
                return integer(0);
            }

            metaBuf.resetReaderIndex();
            metaBuf.resetWriterIndex();
        }

        System.out.println("当前数据===============4 start:"+metaStart);
        System.out.println("当前数据===============4 end:"+metaEnd);



        int count = _toint(count1);
        int delCnt = 0;
        if(count == 0 || count >0){
            // count > 0 : 从表头开始向表尾搜索，移除与 VALUE 相等的元素，数量为 COUNT 。
            // count = 0 : 移除表中所有与 VALUE 相等的值。

            //获取范围内的元素;不删除原有数据
            for (long i = metaStart; i <=metaEnd; i++) {

                //生成获取头部元素的 key
                byte[] keyPre = __genkey("_l".getBytes(), key0, "#".getBytes());
                ByteBuf keyPreBuf = Unpooled.wrappedBuffer(keyPre);
                //遍历元素 key
                startBuf.resetWriterIndex();
                startBuf.writeLong(i);

                System.out.println("当前数据===============5 i:"+i);

                ByteBuf keyBuf = Unpooled.wrappedBuffer(keyPreBuf,startBuf);
                byte[] array = keyBuf.readBytes(keyBuf.readableBytes()).array();
                System.out.println("===============5"+new String(array));

                //获取数据
                byte[] vals = __get(array);

                if(Arrays.equals(vals,value2)){
                    __del(array);
                    delCnt++;
                }

                if(count>0 && delCnt > count){
                    break;
                }


            }

        }

        if(count < 0){ //count < 0 : 从表尾开始向表头搜索，移除与 VALUE 相等的元素，数量为 COUNT 的绝对值。

            //获取范围内的元素;不删除原有数据
            for (long i = metaEnd; i <=metaStart; i--) {

                //生成获取头部元素的 key
                byte[] keyPre = __genkey("_l".getBytes(), key0, "#".getBytes());
                ByteBuf keyPreBuf = Unpooled.wrappedBuffer(keyPre);
                //遍历元素 key
                startBuf.resetWriterIndex();
                startBuf.writeLong(i);

                System.out.println("当前数据===============5 i:"+i);

                ByteBuf keyBuf = Unpooled.wrappedBuffer(keyPreBuf,startBuf);
                byte[] array = keyBuf.readBytes(keyBuf.readableBytes()).array();
                System.out.println("===============5"+new String(array));

                //获取数据
                byte[] vals = __get(array);

                if(Arrays.equals(vals,value2)){
                    __del(array);
                    delCnt++;
                }

                if(delCnt >= count){
                    break;
                }

            }
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
     *
     * @param key0
     * @param start1
     * @param stop2
     * @return
     * @throws RedisException
     */
    protected MultiBulkReply __lrange(byte[] key0, byte[] start1, byte[] stop2) throws RedisException {

        //初始化List 开始于结束指针
        long metaStart = 0;
        long metaEnd = 0;
        //获取元数据的索引值
        byte[] keyMeta = __genkey("+".getBytes(), key0, "list".getBytes());
        byte[] valMeta = __get(mymeta, keyMeta); //'0,2'  开始指针，结束指针

        ByteBuf metaBuf = null;
        ByteBuf startBuf = null;
        ByteBuf endBuf = null;

        if(valMeta == null){
            System.out.println("===============2");

            return MultiBulkReply.EMPTY;

        }else{
            System.out.println("===============3");

            metaBuf = Unpooled.wrappedBuffer(valMeta);

            startBuf=metaBuf.slice(0,8);
            endBuf=metaBuf.slice(8,8);

            metaStart = metaBuf.readLong();
            metaEnd = metaBuf.readLong();

            //数据列表为空，清除元数据
            if((metaStart-metaEnd) == 0){
                __del(keyMeta);
                return MultiBulkReply.EMPTY;
            }

            metaBuf.resetReaderIndex();
            metaBuf.resetWriterIndex();
        }

        System.out.println("当前数据===============4 start:"+metaStart);
        System.out.println("当前数据===============4 end:"+metaEnd);


        long s = _torange(start1, (metaEnd-metaStart));
        long e = _torange(stop2, (metaEnd-metaStart));
        System.out.println("当前数据===============4.1 e:"+e);
        if (e < s) e = s;
        long length = e - s;


        System.out.println("当前数据===============4.2 s:"+s);
        System.out.println("当前数据===============4.3 e:"+e);

        List<BulkReply> results = new ArrayList<BulkReply>();
        //获取范围内的元素;不删除原有数据
        for (long i = metaStart+s; i <= metaStart+s+length; i++) {

            //生成获取头部元素的 key
            byte[] keyPre = __genkey("_l".getBytes(), key0, "#".getBytes());
            ByteBuf keyPreBuf = Unpooled.wrappedBuffer(keyPre);
            //遍历元素 key
            startBuf.resetWriterIndex();
            startBuf.writeLong(i);

            System.out.println("当前数据===============5 i:"+i);

            ByteBuf keyBuf = Unpooled.wrappedBuffer(keyPreBuf,startBuf);
            byte[] array = keyBuf.readBytes(keyBuf.readableBytes()).array();
            System.out.println("===============5"+new String(array));

            //获取数据
            byte[] vals = __get(array);

            System.out.println("===============6"+new String(vals));


            results.add(new BulkReply(vals));
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
        long metaStart = 0;
        long metaEnd = 0;
        //获取元数据的索引值
        byte[] keyMeta = __genkey("+".getBytes(), key0, "list".getBytes());
        byte[] valMeta = __get(mymeta, keyMeta); //'0,2'  开始指针，结束指针

        ByteBuf metaBuf = null;
        ByteBuf startBuf = null;
        ByteBuf endBuf = null;

        if(valMeta == null){
            System.out.println("===============2");

//            startBuf = Unpooled.buffer(8);
//            endBuf = Unpooled.buffer(8);
//
//            startBuf.writeLong(metaStart); //start
//            endBuf.writeLong(metaEnd); //end
//
//            metaBuf=Unpooled.wrappedBuffer(startBuf,endBuf);

            return NIL_REPLY;

        }else{
            System.out.println("===============3");

            metaBuf = Unpooled.wrappedBuffer(valMeta);

            startBuf=metaBuf.slice(0,8);
            endBuf=metaBuf.slice(8,8);

            metaStart = metaBuf.readLong();
            metaEnd = metaBuf.readLong();

            metaBuf.resetReaderIndex();
            metaBuf.resetWriterIndex();

//            metaBuf.writeLong(metaIndex+1);
        }

        System.out.println("当前数据===============4 start:"+metaStart);
        System.out.println("当前数据===============4 end:"+metaEnd);

        //生成获取头部元素的 key
        byte[] keyPre = __genkey("_l".getBytes(), key0, "#".getBytes());
        ByteBuf keyPreBuf = Unpooled.wrappedBuffer(keyPre);
        ByteBuf keyBuf = Unpooled.wrappedBuffer(keyPreBuf,endBuf);
        byte[] array = keyBuf.readBytes(keyBuf.readableBytes()).array();
        System.out.println("===============5"+new String(array));

        //获取数据
        byte[] vals = __get(array);

        //从存储中删除数据
        __del(array);


        //数据列表为空，清除元数据
        if((metaEnd-metaStart) == 0){
            __del(mymeta,keyMeta);
        } else {

            //meta元数据计数器+1
            metaEnd=Math.decrementExact(metaEnd);
            endBuf.resetWriterIndex();
            endBuf.resetReaderIndex();
            endBuf.writeLong(metaEnd);

            System.out.println("当前数据===============4 end:"+metaEnd);

            metaBuf=Unpooled.wrappedBuffer(startBuf,endBuf);

            __put(mymeta,keyMeta,metaBuf.readBytes(metaBuf.readableBytes()).array(),-1);

        }

        return new BulkReply(vals);





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
        long metaStart = 0;
        long metaEnd = 0;

        byte[] keyMeta = __genkey("+".getBytes(), key0, "list".getBytes());
        byte[] valMeta = __get(mymeta, keyMeta); //'0,2'  开始指针，结束指针
        ByteBuf metaBuf = null;
        if(valMeta == null){
            System.out.println("===============2");
        }else{
            System.out.println("===============3");

            metaBuf = Unpooled.wrappedBuffer(valMeta);

            metaStart = metaBuf.readLong();
            metaEnd = metaBuf.readLong();

        }


        System.out.println("当前数据===============4 start:"+metaStart);
        System.out.println("当前数据===============4 end:"+metaEnd);
        System.out.println("当前数据===============4 count:"+(metaEnd-metaStart+1));

        return integer(metaEnd-metaStart+1);
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
        //Put(‘+[mylist]list’, ‘0,2’)
        //Put(‘_l[mylist]#0’, ‘a’)
        //Put(‘_l[mylist]#1’, ‘b’)
        //Put(‘_l[mylist]#2’, ‘c’)

            System.out.println("===============1");

        //初始化List 开始于结束指针
        long metaStart = 0;
        long metaEnd = 0;
        //获取元数据的索引值
        byte[] keyMeta = __genkey("+".getBytes(), key0, "list".getBytes());
        byte[] valMeta = __get(mymeta, keyMeta); //'0,2'  开始指针，结束指针
        ByteBuf metaBuf = null;
        if(valMeta == null){
            System.out.println("===============2");

            metaBuf = Unpooled.buffer(8);
            metaBuf.writeLong(metaStart); //start
            metaBuf.writeLong(metaEnd); //end

            metaBuf.resetReaderIndex();

        }else{
            System.out.println("===============3");

            metaBuf = Unpooled.wrappedBuffer(valMeta);

            metaStart = metaBuf.readLong();
            metaEnd = metaBuf.readLong();

            metaStart=Math.decrementExact(metaStart); //后面数据要自增长

            metaBuf.resetReaderIndex();
            metaBuf.resetWriterIndex();

//            metaBuf.writeLong(metaIndex+1);
        }

        System.out.println("当前数据===============4 start:"+metaStart);
        System.out.println("当前数据===============4 end:"+metaEnd);

        //批量处理
        final WriteOptions writeOpt = new WriteOptions();
        final WriteBatch batch = new WriteBatch();

        ByteBuf startBuf = Unpooled.buffer(8);
        //添加元素
        for (byte[] bt:value1
             ) {
            System.out.println("===============5"+new String(bt));

            byte[] keyPre = __genkey("_l".getBytes(), key0, "#".getBytes());
            ByteBuf keyPreBuf = Unpooled.wrappedBuffer(keyPre);
            //插入头部，开始位置-1
//            metaStart=metaStart-1;
//            if(metaStart != 0){ //第一个数据写在 0,0 ; 后面数据要自增长
//                metaStart=Math.decrementExact(metaStart);
//            }

            startBuf.writeLong(metaStart);
            ByteBuf keyBuf = Unpooled.wrappedBuffer(keyPreBuf,startBuf);
//            keyBuf.writeLong(metaIndex);

            byte[] array = keyBuf.readBytes(keyBuf.readableBytes()).array();
            System.out.println("===============5"+new String(array));

            batch.put(array,__genVal(bt,-1));

            startBuf.resetWriterIndex();
            startBuf.resetReaderIndex();

            //元素下标数据增加
//            metaBuf.resetReaderIndex();
//            System.out.println("===============5.1");

//            metaIndex = metaBuf.readLong();
//            metaBuf.resetWriterIndex();
//            System.out.println("===============5.2");

//            metaBuf.writeLong(metaIndex+1);
//            System.out.println("===============5.3");

//            metaBuf.resetReaderIndex();

        }

        System.out.println("===============6");

        try {
            mydata.write(writeOpt, batch);


            metaBuf.resetWriterIndex();

            metaBuf.writeLong(metaStart);
            metaBuf.writeLong(metaEnd);

            metaBuf.resetReaderIndex();
            __put(mymeta,keyMeta,metaBuf.readBytes(metaBuf.readableBytes()).array(),-1);

        } catch (RocksDBException e) {
            throw new RuntimeException(e.getMessage());
        }
        return integer(batch.count());


//        List<BytesValue> list = _getlist(key0, true);
//        for (byte[] value : value1) {
//            list.add(0, new BytesKey(value));
//        }
//        return integer(list.size());
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

        for (byte[] fd:fields
             ) {
            byte[] fkey = __genkey(fkpre, key,"|".getBytes(), fd);
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
            for (byte[] fk:listFds
                 ) {
                byte[] val = __getValue(mydata, fk, fvals.get(fk));
                if(val != null){
                    list.add(new BulkReply(val));
                }else list.add(NIL_REPLY);

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
            batch.put(field_or_value1[i],val);

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
        byte[] fkeypre = __genkey(fkpre, key,"|".getBytes());

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

                        if(value!=null){
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
        byte[] fkeypre = __genkey(fkpre, key,"|".getBytes());

        //检索所有的 hash field key  所有的 key 都是有序的
        List<Reply<ByteBuf>> replies = new ArrayList<Reply<ByteBuf>>();


        List<byte[]> keyVals = __keyVals(mydata, fkeypre);//顺序读取 ;field 过期逻辑复杂，暂不处理


        int i=0;
        for (byte[] bt:keyVals
             ) {
            if( i%2 == 0){  //key 处理
                ByteBuf hkeybuf1 = Unpooled.wrappedBuffer(bt); //优化 零拷贝

                ByteBuf slice = hkeybuf1.slice(3 + key.length, bt.length - 3 - key.length);

                replies.add(new BulkReply(slice.readBytes(slice.readableBytes()).array()));
            }else {

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

        __reduHMeta(keys,1);

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
        __incrHMeta(keys,1);
        __put(keys[1], value);

        return value;
    }

    /**
     *
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

            if (fbytes != null ) {

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
            __del(mydata,bytes);
    }


    private void __del(RocksDB db,byte[] bytes) throws RedisException {
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
     * @param data
     * @param key0
     * @return
     * @throws RedisException
     */
    protected byte[] __get(RocksDB data, byte[] key0) throws RedisException {
        try {

            byte[] values = data.get(key0);
            return __getValue(data,key0, values);

        } catch (RocksDBException e) {
            throw new RedisException(e.getMessage());
        }
    }

    /**
     *
     * 提取数据；
     * 删除过期数据
     *
     * @param key0
     * @param values
     * @return
     * @throws RedisException
     */
    private byte[] __getValue(RocksDB db,byte[] key0, byte[] values) throws RedisException {
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
                __del(db,key0);
                return null;
            }

            byte[] array = valueBuf.readBytes(valueBuf.readableBytes()).array();

//                System.out.println("get key:" + new String(key0));
//                System.out.println("get value:" + new String(array));


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
     *
     * @param value
     * @param expiration
     * @return
     */
    private byte[] __genVal(byte[] value, long expiration) {
        ByteBuf ttlBuf = Unpooled.buffer(12);
        ttlBuf.writeLong(expiration); //ttl 无限期 -1
        ttlBuf.writeInt(value.length); //value size

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
     *
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
        byte[] fkey = __genkey(fkpre, key,"|".getBytes() ,field);

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
     * @param keys
     * @return
     */
    protected byte[] __genkey(byte[]... keys) {
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
