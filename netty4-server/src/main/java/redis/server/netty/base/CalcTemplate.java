package redis.server.netty.base;


import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.log4j.Logger;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;

import redis.server.netty.RedisException;


import java.util.*;

import static redis.util.Encoding.bytesToNum;


/**
 * 算法模板，公用算法部分；
 * 两个模板,不带分页的；
 * 不能再循环内部使用；
 *
 * <p>
 * Created by moyong on 2018/11/6.
 * Update by moyong on 2018/11/6
 * </p>
 */
public class CalcTemplate
{
    private static Logger log = Logger.getLogger(CalcTemplate.class);

//    /**
//     * 分数区间指令拆解 min max  todo by ByteBuf
//     * @param specifier
//     * @return
//     */
//    private Score _toscorerange(byte[] specifier) {
//        Score score = new Score();
//        String s = new String(specifier).toLowerCase();
//        if (s.startsWith("(")) {
//            score.inclusive = false;
//            s = s.substring(1);
//        }
//        if (s.equals("-inf")) {
//            score.value = Double.NEGATIVE_INFINITY;
//            score.source = "-1.0".getBytes();
//        } else if (s.equals("inf") || s.equals("+inf")) {
//            score.value = Double.POSITIVE_INFINITY;
//            score.source = "1.0".getBytes();
//        } else {
//            score.value = Double.parseDouble(s);
//            score.source = s.getBytes();
//
//        }
//        return score;
//    }

//    static class Score {
//        boolean inclusive = true;
//        byte[] source;
//        double value;
//    }

//    /**
//     *
//     * 从缓存中拆分score and member
//     *
//     * @param scoremember
//     * @return
//     */
//    protected static ScoreMember splitScoreMember(ByteBuf scoremember) {
//
//        scoremember.resetReaderIndex();
//        int splitIndex = scoremember.bytesBefore("|".getBytes()[0]);
//        ByteBuf scoreBuf = scoremember.slice(0, splitIndex);
//        ByteBuf memberBuf = scoremember.slice(splitIndex+1 , scoremember.capacity() - splitIndex-1);
//
//        return new ScoreMember(scoremember,scoreBuf,memberBuf);
//
//    }

    public static class ScoreMember {

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


    /**
     * 不带规则模板
     *
     * @param call
     * @param db
     * @param pattern0
     * @param min
     * @param max
     * @param offset
     * @param num
     * @param reverse
     * @param delete
     * @return
     * @throws RedisException
     */
    public  static List<ScoreMember> zgetByPage(CallBack call,
                                              RocksDB db, byte[] pattern0,
                                              byte[] min,byte[] max,long offset,long num,
                                              boolean reverse, boolean delete) throws RedisException {
        //按 key 检索所有数据
        List<ScoreMember> keys = new ArrayList<ScoreMember>();

        try (final RocksIterator iterator = db.newIterator()) {
            //记录号=游标
            long cur = 0;
            //匹配前缀的记录
            for (iterator.seek(pattern0); iterator.isValid(); iterator.next()) {

                //确保检索有数据，hkeybuf.slice 不错误
                byte[] key = iterator.key();
                //匹配符合条件
                if (pattern0.length <= key.length) {

                    ByteBuf hkeybuf = Unpooled.wrappedBuffer(key); //优化 零拷贝
                    ByteBuf slice = hkeybuf.slice(0, pattern0.length); //获取指定前缀长度的 byte[]

                    slice.resetReaderIndex();

                    //key有序 不相等后面无数据 ；二次确认有符合条件的记录
                    if (Arrays.equals( toByteArray(slice), pattern0)) {

                        //1.获取数据
                        hkeybuf.resetReaderIndex();
                        ByteBuf scoremember = hkeybuf.slice(pattern0.length, key.length-pattern0.length); //获取指定前缀长度的 byte[]

                        //符合条件才进行计数
                        cur = call.getByPage(db, min, max, offset, num, delete,
                                cur, key, scoremember,
                                keys);

                        continue;


                    } else {
                        break;
                    }
                } else break;

            }
//        } catch (RocksDBException e) {
//            e.printStackTrace();
//            Throwables.propagateIfPossible(e,RedisException.class);
        }

        if (reverse) Collections.reverse(keys);

        return keys;
    }




    /**
     * 不带规则模板
     *
     * @param call
     * @param db
     * @param pattern0
     * @param min
     * @param max
     * @param offset
     * @param num
     * @param reverse
     * @param delete
     * @return
     * @throws RedisException
     */
    public  static List<ScoreMember> zgetBy(CallBack call,
                                            RocksDB db, byte[] pattern0,
                                            byte[] min,byte[] max,
                                            boolean reverse, boolean delete) throws RedisException {
        //按 key 检索所有数据
        List<ScoreMember> keys = new ArrayList<ScoreMember>();

        try (final RocksIterator iterator = db.newIterator()) {
            //记录号=游标
            long cur = 0;
            //匹配前缀的记录
            for (iterator.seek(pattern0); iterator.isValid(); iterator.next()) {

                //确保检索有数据，hkeybuf.slice 不错误
                byte[] key = iterator.key();
                //匹配符合条件
                if (pattern0.length <= key.length) {

                    ByteBuf hkeybuf = Unpooled.wrappedBuffer(key); //优化 零拷贝
                    ByteBuf slice = hkeybuf.slice(0, pattern0.length); //获取指定前缀长度的 byte[]

                    slice.resetReaderIndex();

                    //key有序 不相等后面无数据 ；二次确认有符合条件的记录
                    if (Arrays.equals( toByteArray(slice), pattern0)) {

                        //1.获取数据
                        hkeybuf.resetReaderIndex();
                        ByteBuf scoremember = hkeybuf.slice(pattern0.length, key.length-pattern0.length); //获取指定前缀长度的 byte[]

                        //todo 符合条件才进行计数
//                        cur=call.getBy(db, min, max,delete,
//                                cur, key, scoremember,
//                                keys);

                        continue;


                    } else {
                        break;
                    }
                } else break;

            }
//        } catch (RocksDBException e) {
//            e.printStackTrace();
//            Throwables.propagateIfPossible(e,RedisException.class);
        }

        if (reverse) Collections.reverse(keys);

        return keys;
    }

//    private static void getCur(RocksDB db, byte[] min, byte[] max, long offset, long num, boolean delete, List<ScoreMember> keys, long cur, byte[] key, ByteBuf scoremember) throws RocksDBException {
//
//        Score min = _toscorerange(min);
//        Score max = _toscorerange(max);
//
//        //2.拆解数据，获取分数；
//        ScoreMember sm =splitScoreMember(scoremember);
//
//        //3.符合分数范围，提取数据；
//        //获取指定索引的数据
//        //fixme 优化考虑使用antlr
//        if(!min.inclusive && min.value==sm.scoreNum){
//            return cur;
//        }
//
//        if(!max.inclusive && max.value==sm.scoreNum){
//            return cur;
//        }
//
////                        if (min.inclusive && max.inclusive){
//        if (min.value <= sm.scoreNum && max.value >= sm.scoreNum){
////                                keys.add(scoremember);
//            if (cur >= offset && cur < offset + num) {//分页 LIMIT offset count
//
//                keys.add(sm);
//            }
//
//            if (delete){
//                db.delete(key);
////
////                                ByteBuf patternBuf = Unpooled.wrappedBuffer(pattern0); //优化 零拷贝
////                                ByteBuf baseBuf = hkeybuf.slice(0, pattern0.length-DataType.KEY_ZSET_SORT.length-1); //获取指定前缀长度的 byte[]
////                                ByteBuf memberBuf = Unpooled.wrappedBuffer(toByteArray(baseBuf),DataType.KEY_ZSET_SCORE,DataType.SPLIT,sm.member); //优化 零拷贝
////                                log.debug(toString(memberBuf));
////                                memberBuf.resetReaderIndex();
////                                db.delete(toByteArray(memberBuf));
//            }
//
//            cur++;
//        }
////                        }
//        return cur;
//    }


}