package redis.server.netty.base;

import io.netty.buffer.ByteBuf;
import org.rocksdb.RocksDB;

import java.util.List;

public interface CallBack
{
//    public void doProcess(long start, long index, long stop, List<ByteBuf> list,ByteBuf buf);

    /**
     * 分页获取数据
     * @param db 数据库
     * @param min 范围开始
     * @param max 范围结束
     * @param offset 开始记录
     * @param num   页码
     * @param delete 是否删除
     * @param cur 当前数据序号
     * @param key 数据key
     * @param scoremember 业务数据
     * @param keys 返回记录集合
     * @return
     */
    public long getByPage(RocksDB db0, byte[] min1, byte[] max2, long offset3, long num4,
                          boolean delete5, long cur6, byte[] key7, ByteBuf scoremember8, List<CalcTemplate.ScoreMember> keys9);

//    public void getBy(RocksDB db, byte[] min, byte[] max,
//                          boolean delete, long cur, byte[] key, ByteBuf scoremember, List<CalcTemplate.ScoreMember> keys);
}
