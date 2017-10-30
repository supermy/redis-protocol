package redis.server.netty;

import com.sampullara.cli.Args;
import com.sampullara.cli.Argument;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;

/**
 * Redis server
 */
public class Main {
  @Argument(alias = "p")
  private static Integer port = 6380;

  public static void main(String[] args) throws InterruptedException {
    try {
      Args.parse(Main.class, args);
    } catch (IllegalArgumentException e) {
      Args.usage(Main.class);
      System.exit(1);
    }

    // Only execute the command handler in a single thread
//      final RedisCommandHandler commandHandler = new RedisCommandHandler(new SimpleRedisServer());//内存方案
      final RedisCommandHandler commandHandler = new RedisCommandHandler(new RocksdbRedisServer());//硬盘方案

    // Configure the server.
    ServerBootstrap b = new ServerBootstrap();
    final DefaultEventExecutorGroup group = new DefaultEventExecutorGroup(1);
    try {
        b.group(new NioEventLoopGroup(), new NioEventLoopGroup(4))
         .channel(NioServerSocketChannel.class)
         .option(ChannelOption.SO_BACKLOG, 64)  //默认 50 BACKLOG用于构造服务端套接字ServerSocket对象，标识当服务器请求处理线程全满时，用于临时存放已完成三次握手的请求的队列的最大长度。如果未设置或所设置的值小于1，Java将使用默认值50。
        //值得注意的是，如果使用内存池，完成ByteBuf的解码工作之后必须显式的调用ReferenceCountUtil.release(msg)对接收缓冲区ByteBuf进行内存释放，否则它会被认为仍然在使用中，这样会导致内存泄露。
        //.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT) //使用内存池之后，内存的申请和释放必须成对出现，即retain()和release()要成对出现，否则会导致内存泄露。
        .localAddress(port)
               // .childOption(ChannelOption.SO_KEEPALIVE, true)//是否启用心跳保活机制。在双方TCP套接字建立连接后（即都进入ESTABLISHED状态）并且在两个小时左右上层没有任何数据传输的情况下，这套机制才会被激活。
        .childOption(ChannelOption.TCP_NODELAY, true)//在TCP/IP协议中，无论发送多少数据，总是要在数据前面加上协议头，同时，对方接收到数据，也需要发送ACK表示确认。为了尽可能的利用网络带宽，TCP总是希望尽可能的发送足够大的数据。这里就涉及到一个名为Nagle的算法，该算法的目的就是为了尽可能发送大块数据，避免网络中充斥着许多小数据块。

        //TCP_NODELAY就是用于启用或关于Nagle算法。如果要求高实时性，有数据发送时就马上发送，就将该选项设置为true关闭Nagle算法；如果要减少发送次数减少网络交互，就设置为false等累积一定大小后再发送。默认为false。
         .childHandler(new ChannelInitializer<SocketChannel>() {
           @Override
           public void initChannel(SocketChannel ch) throws Exception {
             ChannelPipeline p = ch.pipeline();
//             p.addLast(new ByteLoggingHandler(LogLevel.INFO));
             p.addLast(new RedisCommandDecoder());
             p.addLast(new RedisReplyEncoder());
             p.addLast(group, commandHandler);
           }
         });

        // Start the server.
        ChannelFuture f = b.bind().sync();

        // Wait until the server socket is closed.
        f.channel().closeFuture().sync();
    } finally {
        // Shut down all event loops to terminate all threads.
      group.shutdownGracefully();
    }
  }
}
