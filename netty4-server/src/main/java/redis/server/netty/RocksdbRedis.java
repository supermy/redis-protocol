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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static java.lang.Double.parseDouble;
import static java.lang.Integer.MAX_VALUE;
import static redis.netty4.BulkReply.NIL_REPLY;
import static redis.netty4.IntegerReply.integer;
import static redis.util.Encoding.bytesToNum;
import static redis.util.Encoding.numToBytes;

/**
 * Rocksdb 相关的方法操作
 * Created by moyong on 2017/10/20.
 */
public class RocksdbRedis extends RedisBase {

    /**
     * hash meta count -1
     * @param key
     * @param field
     * @return
     * @throws RedisException
     */
    protected int _hdel(byte[] key, byte[] field) throws RedisException {

        byte[][] keys = _genhkey(key, field);

        //hash meta count +1
        ByteBuf hval = Unpooled.buffer(4);
        byte[] hbytes = __get(keys[0]);
        if (hbytes == null || hbytes.length == 0) {
            //hval.writeInt(1);
        } else {
            //如果 field-key 不存在 不计数
            byte[] fbytes = __get(keys[1]);
            if (fbytes != null || fbytes.length != 0) {

                hval.writeBytes(hbytes);
                int count = hval.readInt() - 1;
                hval.clear();
                hval.writeInt(count);

                System.out.println("hash count -1:" + count);

                __put(keys[0], hval.array()); //key count +1

            }
        }


        __del(keys[1]);

        return 1;
    }
    /**
     * 没有考虑数据过期情况
     *
     * @param key0
     * @return
     * @throws RedisException
     */
    @Deprecated
    protected boolean __exists(byte[] key0) throws RedisException {
//        Object o = _get(key0);
        StringBuilder sb = new StringBuilder();
        boolean o = mydata.keyMayExist(key0, sb);
        return o;
    }

    /**
     * @param key
     * @param field
     * @return
     * @throws RedisException
     */
    protected byte[] __hget(byte[] key, byte[] field) throws RedisException {

        byte[][] keys = _genhkey(key, field);

        byte[] metabytes = __get(keys[0]);
        ByteBuf hval = Unpooled.buffer(4);
        hval.writeBytes(metabytes);
        System.out.println("hash count have:" + hval.readInt());

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

        //hash meta count +1
        ByteBuf hval = Unpooled.buffer(4);
        byte[] metabytes = __get(keys[0]);

        if (metabytes == null || metabytes.length == 0) {

            System.out.println("hash count ==1:");

            hval.writeInt(1);
            __put(keys[0], hval.array()); //key count +1

        } else {
            //如果 field-key 不存在 则计数
            byte[] fbytes = __get(keys[1]);
            if (fbytes == null || fbytes.length == 0) {

                hval.writeBytes(metabytes);
                int count = hval.readInt() + 1;
                hval.clear();
                hval.writeInt(count);

                System.out.println("hash count +1:" + count);

                __put(keys[0], hval.array()); //key count +1

            }
        }


        __put(keys[1], value);

        return value;
    }


    /**
     * 删除数据
     *
     * @param bytes
     * @throws RedisException
     */
    private void __del(byte[] bytes) throws RedisException {
        try {
            mydata.delete(bytes);
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
        System.out.println("gggggggggggggggggggggggg");

        //数据是否过期处理
        //get ttl  value-size
        byte[] ttl_size = new byte[12];
        try {
            int i = mydata.get(key0, ttl_size);

            System.out.println("ttl have :" + i);

            if (i > 0) {

                ByteBuf ttlbuf = Unpooled.buffer(12);
                ttlbuf.writeBytes(ttl_size);
                long ttl = ttlbuf.readLong();
                long size = ttlbuf.readInt();

                System.out.println("ttl:" + ttl);
                System.out.println("data length:" + size);

                //数据过期处理
                if (ttl < now() && ttl != -1) {
                    __del(key0);
                    return null;
                }

                byte[] bytes = mydata.get(key0);
                ByteBuf valbuf = Unpooled.buffer(bytes.length);
                valbuf.writeBytes(bytes);

                valbuf.readLong();
                valbuf.readInt();
                byte[] array = valbuf.readBytes(valbuf.readableBytes()).array();

                System.out.println("get key:" + new String(key0));
                System.out.println("get value:" + new String(array));


                return array;

            } else return null; //数据不存在 ？ 测试验证

        } catch (RocksDBException e) {
            throw new RedisException(e.getMessage());
        }
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

        return __put(key, value, -1);
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
    protected byte[] __put(byte[] key, byte[] value, long expiration) {
        System.out.println("ppppppppppppppppppppppppppppp");

        try {
            ByteBuf valbuf = Unpooled.buffer(16);
            valbuf.writeLong(expiration); //ttl 无限期 -1
            valbuf.writeInt(value.length); //value size
            valbuf.writeBytes(value); //value


            byte[] bt = valbuf.readBytes(valbuf.readableBytes()).array();

            System.out.println("data byte length:" + bt.length);
            System.out.println("set key:" + new String(key));
            System.out.println("set value:" + new String(value));

            mydata.put(key, bt);
        } catch (RocksDBException e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }

        return value;
    }


    /**
     * 构建 hashkey
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

        byte[] hkey = _genkey(hkpre, key, hksuf);
        byte[] fkey = _genkey(fkpre, key, field);

        keys[0] = hkey;
        keys[1] = fkey;

        return keys;
    }

    /**
     * 组装 key
     *
     * @param hkpre
     * @param key
     * @param hksuf
     */
    private byte[] _genkey(byte[] hkpre, byte[] key, byte[] hksuf) {
        ByteBuf buf1 = Unpooled.buffer(16);
        buf1.writeBytes(hkpre);
        buf1.writeBytes(key);
        buf1.writeBytes(hksuf);
        byte[] array = buf1.readBytes(buf1.readableBytes()).array();

        System.out.println(String.format("组合键为 %s", new String(array)));

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

    protected boolean __hexists(byte[] key0, byte[] field) throws RedisException {
        byte[] hkpre = "+".getBytes();
        byte[] hksuf = "hash".getBytes();
        byte[] fkpre = "_h".getBytes();

        byte[] hkey = byteMerger(hkpre, byteMerger(key0, hksuf));
        byte[] fkey = byteMerger(fkpre, byteMerger(key0, field));

//        Object o = _get(key0);
        StringBuilder sb = new StringBuilder();
        boolean o = mydata.keyMayExist(fkey, sb);
        return o;
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


    protected MultiBulkReply __hgetall(byte[] key) throws RedisException {
        byte[] hkpre = "+".getBytes();
        byte[] hksuf = "hash".getBytes();
        byte[] fkpre = "_h".getBytes();

        byte[] fkeypre = byteMerger(fkpre, key);
        //检索所有的 hash field key
        List<byte[]> keys = new ArrayList<>();
        try (final RocksIterator iterator = mydata.newIterator()) {
            for (iterator.seek(fkeypre); iterator.isValid() && new String(iterator.key()).startsWith(new String(fkeypre)); iterator.next()) {//fixme
                keys.add(iterator.key());
//                if (keys.size() >= 10000) {
//                    //数据大于1万条直接退出
//                    break;
//                }
            }
        }

        List<Reply<ByteBuf>> replies = new ArrayList<Reply<ByteBuf>>();

        //检索过期数据,处理过期数据
        try (final WriteOptions writeOpt = new WriteOptions()) {
            try (final WriteBatch batch = new WriteBatch()) {
                for (byte[] key1 : keys) {
                    byte[] bytes = myexpires.get(key1);

                    if (bytes != null) {
                        long l = bytesToNum(bytes);
                        if (l < now()) {
                            batch.remove(key1);
                        } else {
                            replies.add(new BulkReply(key1));
                            replies.add(new BulkReply(__get(key1)));

                        }
                    } else {
                        replies.add(new BulkReply(key1));
                        replies.add(new BulkReply(__get(key1)));

                    }
                }
                mydata.write(writeOpt, batch);
                myexpires.write(writeOpt, batch);
            } catch (RocksDBException e) {
                e.printStackTrace();
            }
        }


        return new MultiBulkReply(replies.toArray(new Reply[replies.size()]));
    }


    protected RocksDB mydata = getDb("netty4-server/db/data");
    protected RocksDB myexpires = getDb("netty4-server/db/expires");

    protected RocksDB getDb(String filename) {
//    String filename = env.getRequiredProperty(filename);

        System.out.println("rocks db path:" + filename);

        RocksDB.loadLibrary();
        // the Options class contains a set of configurable DB options
        // that determines the behavior of a database.
        final Options options = new Options();

        final Statistics stats = new Statistics();


        try {
            options.setCreateIfMissing(true)
                    .setStatistics(stats)
                    .setWriteBufferSize(8 * SizeUnit.KB)
                    .setMaxWriteBufferNumber(3)
                    .setMaxBackgroundCompactions(10)
                    .setCompressionType(CompressionType.SNAPPY_COMPRESSION)
                    .setCompactionStyle(CompactionStyle.UNIVERSAL);
        } catch (final IllegalArgumentException e) {
            assert (false);
        }

        final Filter bloomFilter = new BloomFilter(10);
        final ReadOptions readOptions = new ReadOptions()
                .setFillCache(false);
        final RateLimiter rateLimiter = new RateLimiter(10000000, 10000, 10);

        options.setMemTableConfig(
                new HashSkipListMemTableConfig()
                        .setHeight(4)
                        .setBranchingFactor(4)
                        .setBucketCount(2000000));

        options.setMemTableConfig(
                new HashLinkedListMemTableConfig()
                        .setBucketCount(100000));
        options.setMemTableConfig(
                new VectorMemTableConfig().setReservedSize(10000));

        options.setMemTableConfig(new SkipListMemTableConfig());

        options.setTableFormatConfig(new PlainTableConfig());
        // Plain-Table requires mmap read
        options.setAllowMmapReads(true);

        options.setRateLimiter(rateLimiter);

        final StringAppendOperator stringAppendOperator = new StringAppendOperator();
        options.setMergeOperator(stringAppendOperator);

        final BlockBasedTableConfig table_options = new BlockBasedTableConfig();
        table_options.setBlockCacheSize(64 * SizeUnit.KB)
                .setFilter(bloomFilter)
                .setCacheNumShardBits(6)
                .setBlockSizeDeviation(5)
                .setBlockRestartInterval(10)
                .setCacheIndexAndFilterBlocks(true)
                .setHashIndexAllowCollision(false)
                .setBlockCacheCompressedSize(64 * SizeUnit.KB)
                .setBlockCacheCompressedNumShardBits(10);

        options.setTableFormatConfig(table_options);
        //options.setCompressionType(CompressionType.SNAPPY_COMPRESSION).setCreateIfMissing(true);

        RocksDB db = null;
        try {
            // a factory method that returns a RocksDB instance
            //String filename = "/Users/moyong/project/env-myopensource/1-spring/12-spring/rocksdb-service/src/main/resources/data";
            //db = factory.open(new File("example"), options);

            db = RocksDB.open(options, filename);
            // do something
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
        int total = 0;
        for (byte[] bytes : key0) {

            __del(bytes);

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
