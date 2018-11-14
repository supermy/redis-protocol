package redis.server.netty;

import com.google.common.base.Throwables;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.log4j.Logger;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import redis.server.netty.utis.DataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static redis.util.Encoding.bytesToNum;

/**
 * redis 元数据类型的基类
 *
 */
public class BaseMeta {
    private static Logger log = Logger.getLogger(BaseMeta.class);

    /**
     * 异步线程处理元素数据计数；冗余元素数据清理。
     *
     */
    protected ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();


    protected byte[] NS;
    protected static byte[] TYPE = DataType.KEY_META;

    protected RocksDB db;

    protected ByteBuf metaKey;
    protected ByteBuf metaVal;


    public String getKey0Str() throws RedisException {
        return new String(getKey0());
    }

    public byte[] getKey() throws RedisException {
        metaKey.resetReaderIndex();
        return metaKey.readBytes(metaKey.readableBytes()).array();
    }


    /**
     * 参见genkey1,获取key0的分解
     *
     * @return
     * @throws RedisException
     */
    public byte[] getKey0() throws RedisException {
        metaKey.resetReaderIndex();
        ByteBuf bb = metaKey.slice(NS.length + 1, metaKey.readableBytes() - NS.length - DataType.SPLIT.length * 2 - TYPE.length);
        return bb.readBytes(bb.readableBytes()).array();
    }





    /**
     * 批量删除主键(0-9.A-Z,a-z)；
     * 根据genkey 特征，增加风格符号，避免误删除数据；
     *
     * @param key0
     * @throws RedisException
     */
    protected void deleteRange(byte[] key0) throws RedisException {



        ByteBuf byteBufBegin = Unpooled.wrappedBuffer(NS, DataType.SPLIT, key0, DataType.SPLIT);
        ByteBuf byteBufEnd = Unpooled.wrappedBuffer(NS, DataType.SPLIT, key0, DataType.SPLIT, "z".getBytes());

        byte[] begin = byteBufBegin.readBytes(byteBufBegin.readableBytes()).array();
        byte[] end = byteBufEnd.readBytes(byteBufEnd.readableBytes()).array();

        log.debug(String.format("begin %s -> end %s", new String(begin),new String(end)));

        try {
            db.deleteRange(begin, end);
        } catch (RocksDBException e) {
            e.printStackTrace();
            throw new RedisException(e.getMessage());
        }
    }


    protected void deleteRange(byte[] key0,byte[] type,byte[] start,byte[] stop) throws RedisException {

        ByteBuf byteBufBegin = Unpooled.wrappedBuffer(NS, DataType.SPLIT, key0, DataType.SPLIT,type,DataType.SPLIT,start);
        ByteBuf byteBufEnd = Unpooled.wrappedBuffer(NS, DataType.SPLIT, key0, DataType.SPLIT,type,DataType.SPLIT,stop);

        byte[] begin = byteBufBegin.readBytes(byteBufBegin.readableBytes()).array();
        byte[] end = byteBufEnd.readBytes(byteBufEnd.readableBytes()).array();

        log.debug(String.format("begin %s -> end %s", new String(begin),new String(end)));

        try {
            db.deleteRange(begin, end);
        } catch (RocksDBException e) {
            e.printStackTrace();
            throw new RedisException(e.getMessage());
        }
    }





    /**
     * 打印调试
     * @param buf
     */
    public void print(ByteBuf buf) {
        buf.resetReaderIndex();
        System.out.println(new String(buf.readBytes(buf.readableBytes()).array()));
    }

    /**
     *
     * 从缓存中拆分score and member
     *
     * @param scoremember
     * @return
     */
    protected static ScoreMember splitScoreMember(ByteBuf scoremember) {

        //2.拆解数据，获取分数；
//        log.debug(scoremember.capacity());
//        log.debug(new String(toByteArray(scoremember)));

        scoremember.resetReaderIndex();
        int splitIndex = scoremember.bytesBefore("|".getBytes()[0]);

        if (splitIndex>0) {

            ByteBuf scoreBuf = scoremember.slice(0, splitIndex);
            ByteBuf memberBuf = scoremember.slice(splitIndex+1 , scoremember.capacity() - splitIndex-1);

            return new ScoreMember(scoremember,scoreBuf,memberBuf);

        } else{

            return new ScoreMember(null,null,scoremember);
        }

    }

    protected static class ScoreMember {

        ScoreMember(ByteBuf scoreMemberBuf, ByteBuf scoreBuf, ByteBuf memberBuf) {
            scoreMemberBuf = scoreMemberBuf;
            scoreBuf = scoreBuf;
            memberBuf = memberBuf;

            if (scoreMemberBuf != null) {
                scoreMember = toByteArray(scoreMemberBuf);
                log.debug(new String(scoreMember));
            }
            if (scoreBuf != null) {
                score = toByteArray(scoreBuf);
                scoreNum = bytesToNum(score);
                log.debug(new String(score));
            }
            if (memberBuf != null) {
                member = toByteArray(memberBuf);
                log.debug(new String(member));
            }

        }

        ByteBuf scoreMemberBuf;
        ByteBuf scoreBuf;
        ByteBuf memberBuf;

        byte[] scoreMember;
        byte[] score;
        byte[] member;

        long scoreNum;


    }

    protected static byte[] toByteArray(ByteBuf scoremember) {
        scoremember.resetReaderIndex();
        return scoremember.readBytes(scoremember.readableBytes()).array();
    }

    protected static String toString(ByteBuf scoremember) {
        return new String(toByteArray(scoremember));
    }

    protected static String toString(byte[] scoremember) {
        return new String(scoremember);
    }



}
