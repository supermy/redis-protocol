package redis.server.netty;

import io.netty.buffer.ByteBuf;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.lang.Double.parseDouble;
import static java.lang.Integer.MAX_VALUE;
import static redis.netty4.BulkReply.NIL_REPLY;
import static redis.netty4.IntegerReply.integer;
import static redis.util.Encoding.bytesToNum;
import static redis.util.Encoding.numToBytes;

/**
 * RedisServer 的基础定义
 * Created by moyong on 2017/10/20.
 */
public class RedisBase {

    protected static int[] mask = {128, 64, 32, 16, 8, 4, 2, 1};
    //数据存储
    protected BytesKeyObjectMap<Object> data = new BytesKeyObjectMap<Object>();
    //失效数据存储
    protected BytesKeyObjectMap<Long> expires = new BytesKeyObjectMap<Long>();

    protected static RedisException invalidValue() {
        return new RedisException("Operation against a key holding the wrong kind of value");
    }

    protected static RedisException notInteger() {
        return new RedisException("value is not an integer or out of range");
    }

    protected static RedisException notListMeta() {
        return new RedisException("list value no such key");
    }

    protected static RedisException notFloat() {
        return new RedisException("value is not a float or out of range");
    }

    protected long now() {
        return System.currentTimeMillis();
    }

    protected ExecutorService executorService = Executors.newCachedThreadPool();

    protected static RedisException noSuchKey() {
        return new RedisException("no such key");
    }

}
