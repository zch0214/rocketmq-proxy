package com.example.demo;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.remoting.netty.NettyDecoder;
import org.apache.rocketmq.remoting.netty.NettyEncoder;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class BrokerProxy {

    private final int localPort;
    private final String brokerHost;
    private final int brokerPort;

    public BrokerProxy(int localPort, String brokerHost, int brokerPort) {
        this.localPort = localPort;
        this.brokerHost = brokerHost;
        this.brokerPort = brokerPort;
    }

    public void start() throws InterruptedException {
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new NettyDecoder());
                            ch.pipeline().addLast(new NettyEncoder());
                            ch.pipeline().addLast(new ProxyHandler(brokerHost, brokerPort));
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

            ChannelFuture f = b.bind(localPort).sync();
            System.out.println("Enhanced RocketMQ Proxy started on port " + localPort);
            f.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }

    private static class ProxyHandler extends SimpleChannelInboundHandler<RemotingCommand> {
        private final String brokerHost;
        private final int brokerPort;
        // 连接池 - 键为目标地址，值为Channel
        private final ConcurrentHashMap<String, Channel> channelPool = new ConcurrentHashMap<>();
        // 重试配置
        private static final int MAX_RETRY_TIMES = 2;

        public ProxyHandler(String brokerHost, int brokerPort) {
            this.brokerHost = brokerHost;
            this.brokerPort = brokerPort;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
            int requestCode = msg.getCode();
            System.out.println("-------------当前请求是："+RequestCodeUtil.getCodeName(requestCode));
            // 打印请求日志
            System.out.println("[PROXY] Received request with code: " + RequestCodeUtil.getCodeName(requestCode));
            if (msg.getBody()!=null&&msg.getBody().length!=0) {
                System.out.println("Body: " + new String(msg.getBody()));
            }

            if (requestCode== RequestCode.SEND_MESSAGE||requestCode==RequestCode.SEND_MESSAGE_V2) {
                System.out.println("发送消息，做增强。增强前："+msg);
                EnhanceUtil.enhanceSendMessageRequest(msg);
                System.out.println("发送消息，做增强。增强后："+msg);
                return;
            }
//              过滤 HEART_BEAT 请求 ,居然是在心跳时做过滤条件的
            if (requestCode == RequestCode.HEART_BEAT) {
                EnhanceUtil.filterPullMessageRequest(msg);
            }

            forwardToBroker(ctx, msg, 0);
        }

        private void forwardToBroker(ChannelHandlerContext ctx, RemotingCommand request, int retryCount) {
            String channelKey = brokerHost + ":" + brokerPort;
            
            // 尝试从连接池获取已有连接
            Channel channel = channelPool.get(channelKey);
            if (channel != null && channel.isActive()) {
                // 使用现有连接
                channel.writeAndFlush(request).addListener((ChannelFutureListener) writeFuture -> {
                    if (!writeFuture.isSuccess()) {
                        // 发送失败，移除失效连接并重试
                        channelPool.remove(channelKey);
                        retryOrFail(ctx, request, retryCount, writeFuture.cause());
                    }
                });
                return;
            }
            
            // 创建新连接
            Bootstrap b = new Bootstrap();
            b.group(ctx.channel().eventLoop())
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 3000) // 连接超时设置
                    .handler(new BrokerResponseHandler(ctx));

            ChannelFuture f = b.connect(brokerHost, brokerPort);
            f.addListener((ChannelFutureListener) future -> {
                if (future.isSuccess()) {
                    // 连接成功，保存到连接池
                    System.out.println("[PROXY] 转发请求到 broker at " + brokerHost + ":" + brokerPort+". "+request);
                    Channel newChannel = future.channel();
                    channelPool.put(channelKey, newChannel);
                    
                    // 添加连接关闭监听器，自动从连接池移除
                    newChannel.closeFuture().addListener((ChannelFutureListener) closeFuture -> {
                        channelPool.remove(channelKey);
                    });
                    
                    // 发送请求
                    newChannel.writeAndFlush(request);
                } else {
                    // 连接失败，尝试重试
                    System.out.println("[PROXY] Failed to connect to broker: " + future.cause().getMessage());
                    retryOrFail(ctx, request, retryCount, future.cause());
                }
            });
        }
        
        private void retryOrFail(ChannelHandlerContext ctx, RemotingCommand request, 
                                int retryCount, Throwable cause) {
            if (retryCount < MAX_RETRY_TIMES) {
                // 重试
                ctx.executor().execute(() -> {
                    forwardToBroker(ctx, request, retryCount + 1);
                });
            } else {
                // 超过最大重试次数，返回错误响应
                System.out.println("[PROXY] Failed to connect to broker after retries");
                RemotingCommand response = RemotingCommand.createResponseCommand(
                        ResponseCode.SYSTEM_ERROR,
                        "Failed to connect to broker");
                response.setOpaque(request.getOpaque()); // 保持请求-响应的匹配
                ctx.writeAndFlush(response);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            System.out.println("[PROXY] Error: " + cause.getMessage());
            ctx.close();
        }
        
        // 提取内部类，使代码结构更清晰
        private class BrokerResponseHandler extends ChannelInitializer<SocketChannel> {
            private final ChannelHandlerContext clientCtx;
            
            public BrokerResponseHandler(ChannelHandlerContext clientCtx) {
                this.clientCtx = clientCtx;
            }
            
            @Override
            protected void initChannel(SocketChannel ch) {
                ch.pipeline().addLast(new NettyDecoder());
                ch.pipeline().addLast(new NettyEncoder());
                ch.pipeline().addLast(new SimpleChannelInboundHandler<RemotingCommand>() {
                    @Override
                    protected void channelRead0(ChannelHandlerContext brokerCtx, RemotingCommand response) {
                        System.out.println("[PROXY] Received response from broker."+response);

                        // 转发响应给客户端
                        clientCtx.writeAndFlush(response);
                    }
                    
                    @Override
                    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                        System.out.println("[PROXY] Error in broker response handler: " + cause.getMessage());
                        ctx.close();
                    }
                });
            }
        }
    }

        public static void main(String[] args) throws InterruptedException {
        // 代理监听8888端口，转发请求到localhost:10911
        BrokerProxy proxy = new BrokerProxy(8888, "localhost", 10911);
        proxy.start();
    }
}