package com.bigdata;


import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;


/**
 * 轮询调度算法假设所有服务器的处理性能都相同，不关心每台服务器的当前连接数和响应速度。当请求服务间隔时间变化比较大时，轮询调度算法容易导
 * 致服务器间的负载不平衡。所以此种均衡算法适合于服务器组中的所有服务器都有相同的软硬件配置并且平均服务请求相对均衡的情况。
 *
 * 轮询调度
 */
public class RoundRobin2Test {

    /**
     * 线程安全的
     */
    private final static AtomicInteger next = new AtomicInteger(0);

    private int select(int S[]) throws Exception {
        if (S == null || S.length == 0)
            throw new Exception("exception");
        else {
            return S[next.getAndIncrement() % S.length];
        }
    }

    @Test
    public  void basic() throws Exception {
        int S[] = {0, 1, 2, 3, 4};
        for (int i = 0; i < 10; i++) {
            System.out.println(select(S));
        }
    }
}