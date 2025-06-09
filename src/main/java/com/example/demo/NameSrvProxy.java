package com.example.demo;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.rocketmq.remoting.netty.NettyDecoder;
import org.apache.rocketmq.remoting.netty.NettyEncoder;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;

import java.util.HashMap;

public class NameSrvProxy {

    private final int localPort;
    private final String nameServerHost;
    private final int nameServerPort;
//    private final String brokerHost;
//    private final int brokerPort;

    public NameSrvProxy(int localPort, String nameServerHost, int nameServerPort,
                        String brokerHost, int brokerPort) {
        this.localPort = localPort;
        this.nameServerHost = nameServerHost;
        this.nameServerPort = nameServerPort;
//        this.brokerHost = brokerHost;
//        this.brokerPort = brokerPort;
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
                            ch.pipeline().addLast(new ProxyHandler(nameServerHost, nameServerPort));
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
        private final String nameServerHost;
        private final int nameServerPort;

        public ProxyHandler(String nameServerHost, int nameServerPort) {
            this.nameServerHost = nameServerHost;
            this.nameServerPort = nameServerPort;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
            int requestCode = msg.getCode();

            // 打印请求日志
            System.out.println("[PROXY] Received request with code: " + RequestCodeUtil.getCodeName(requestCode));

            // 判断请求类型并转发
//            if (isNameServerRequest(requestCode)) {
                forwardToNameServer(ctx, msg);
//            } else {
//                System.out.println("[PROXY] Unknown request type: " + RequestCodeUtil.getCodeName(requestCode));
//                ctx.close();
//            }
        }

        private boolean isNameServerRequest(int requestCode) {
            return requestCode == RequestCode.GET_ROUTEINFO_BY_TOPIC
                    || requestCode == RequestCode.GET_BROKER_CLUSTER_INFO
                    || requestCode == RequestCode.UNREGISTER_CLIENT
                    || requestCode == RequestCode.REGISTER_BROKER;
        }

        private void forwardToNameServer(ChannelHandlerContext ctx, RemotingCommand request) {
            forwardRequest(ctx, request, nameServerHost, nameServerPort, "NameServer");
        }


        private void forwardRequest(ChannelHandlerContext ctx, RemotingCommand request,
                                    String host, int port, String targetName) {
            Bootstrap b = new Bootstrap();
            b.group(ctx.channel().eventLoop())
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new NettyDecoder());
                            ch.pipeline().addLast(new NettyEncoder());
                            ch.pipeline().addLast(new SimpleChannelInboundHandler<RemotingCommand>() {
                                @Override
                                protected void channelRead0(ChannelHandlerContext targetCtx,
                                                            RemotingCommand response) {
                                    System.out.println("[PROXY] Received response from " + targetName);
                                    System.out.println("[PROXY] Received response is: " + response);
                                    System.out.println("[PROXY] Received response is: " + response.getBody());
                                    if(request.getCode()==RequestCode.GET_ROUTEINFO_BY_TOPIC&&response.getCode()==ResponseCode.SUCCESS){
                                        byte[] body = response.getBody();
                                        if (body != null) {
                                            TopicRouteData data = TopicRouteData.decode(body, TopicRouteData.class);

                                            for (BrokerData brokerData : data.getBrokerDatas()) {
                                                HashMap<Long, String> newBrokerAddrs = new HashMap<>();
                                                for (Long key : brokerData.getBrokerAddrs().keySet()) {
                                                    newBrokerAddrs.put(key, "127.0.0.1:8888");
                                                }
                                                brokerData.setBrokerAddrs(newBrokerAddrs);
                                            }
                                            System.out.println("修改后："+data);
                                            response.setBody(data.encode());
                                        }
                                    }
                                    ctx.writeAndFlush(response);
                                }
                            });
                        }
                    });

            ChannelFuture f = b.connect(host, port);
            f.addListener((ChannelFutureListener) future -> {
                if (future.isSuccess()) {
                    System.out.println("[PROXY] Forwarding request to " + targetName + " at " + host + ":" + port);
                    future.channel().writeAndFlush(request);
                } else {
                    System.out.println("[PROXY] Failed to connect to " + targetName);
                    ctx.close();
                }
            });
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            System.out.println("[PROXY] Error: " + cause.getMessage());
            ctx.close();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        // 代理监听9877端口，转发NameServer请求到9876端口，Broker请求到10911端口
        NameSrvProxy proxy = new NameSrvProxy(
                9999, "localhost", 9876, "localhost", 10911);
        proxy.start();
    }
}