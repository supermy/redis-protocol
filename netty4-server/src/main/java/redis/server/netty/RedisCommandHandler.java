package redis.server.netty;

import com.google.common.base.Charsets;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import redis.netty4.Command;
import redis.netty4.ErrorReply;
import redis.netty4.InlineReply;
import redis.netty4.Reply;
import redis.util.BytesKey;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import static redis.netty4.ErrorReply.NYI_REPLY;
import static redis.netty4.StatusReply.QUIT;

/**
 * Handle decoded commands
 */
@ChannelHandler.Sharable
public class RedisCommandHandler extends SimpleChannelInboundHandler<Command> {

    //定义 Map,key 为命令字节,值为可执行回调的类
    private Map<BytesKey, Wrapper> methods = new HashMap<BytesKey, Wrapper>();

    /**
     * 执行回调接口
     */
    interface Wrapper {
        /**
         * 执行命令
         * 回调返回回复数据类
         *
         * @param command
         * @return
         * @throws RedisException
         */
        Reply execute(Command command) throws RedisException;
    }

    //RedisServer中定义了Redis的所有命令
    public RedisCommandHandler(final RedisServer rs) {

        Class<? extends RedisServer> aClass = rs.getClass();

        //获取所有的方法
        for (final Method method : aClass.getMethods()) {

            final Class<?>[] types = method.getParameterTypes();

            //方法名称为 key ,value 为执行回调为钩子类,通过反射执行
            methods.put(new BytesKey(method.getName().getBytes()),
                    new Wrapper() {
                        @Override
                        public Reply execute(Command command) throws RedisException {

                            Object[] objects = new Object[types.length];
                            try {
                                //参数处理
                                command.toArguments(objects, types);

//                                System.out.println(objects.length);
//                                for (Object obj:objects
//                                     ) {
//                                    System.out.println(new String((byte[]) obj));
//                                }

                                //通过反射执行回调
                                return (Reply) method.invoke(rs, objects);

                            } catch (IllegalAccessException e) {
                                throw new RedisException("Invalid server implementation");
                            } catch (InvocationTargetException e) {
                                Throwable te = e.getTargetException();
                                if (!(te instanceof RedisException)) {
                                    te.printStackTrace();
                                }
                                return new ErrorReply("ERR " + te.getMessage());
                            } catch (Exception e) {
                                return new ErrorReply("ERR " + e.getMessage());
                            }
                        }
                    });
        }
    }

    private static final byte LOWER_DIFF = 'a' - 'A';

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Command msg) throws Exception {
        //获取指令，将大写转小写
        byte[] name = msg.getName();
        for (int i = 0; i < name.length; i++) {
            byte b = name[i];
            if (b >= 'A' && b <= 'Z') {
                name[i] = (byte) (b + LOWER_DIFF);
            }
        }

        //获取指令,取出执行指令
        Wrapper wrapper = methods.get(new BytesKey(name));
        Reply reply;
        //是否有效指令
        if (wrapper == null) {
            reply = new ErrorReply("unknown command '" + new String(name, Charsets.US_ASCII) + "'");
        } else {
            //执行指令,传入数据 msg 是字节序列化的执行命令
            reply = wrapper.execute(msg);
        }

        //退出
        if (reply == QUIT) {
            ctx.close();
        } else {

            //Redis 老协议
            if (msg.isInline()) {
                if (reply == null) {
                    reply = new InlineReply(null);
                } else {
                    reply = new InlineReply(reply.data());
                }
            }

            //无结果，返回空
            if (reply == null) {
                reply = NYI_REPLY;
            }

            //执行的结果返回给客户端
            ctx.write(reply);
        }
    }
}
