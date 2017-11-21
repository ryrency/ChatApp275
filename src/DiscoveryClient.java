//package gash.router.discovery;

import gash.router.container.RoutingConf;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.util.internal.SocketUtils;

import routing.Pipe.NetworkDiscoveryPacket;
import routing.Pipe.Route;

import java.net.InetAddress;

public final class DiscoveryClient/* implements Runnable */{

    // track requests
    private static long curID = 0;

    private RoutingConf conf;

    public DiscoveryClient(RoutingConf conf) {
        this.conf = conf;
    }
    
    public static void main(String[] args) {
		DiscoveryClient dc = new DiscoveryClient(null);
		dc.run();
	}

   /* @Override*/
    public void run() {
    	System.out.println("***DiscoveryClient***fn:Run***");
        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            b.group(group)
                    .channel(NioDatagramChannel.class)
                    .option(ChannelOption.SO_BROADCAST, true)
                    .handler(new DiscoveryClientHandler());

            Channel ch = b.bind(8887).sync().channel();

            // construct the networkDiscoveryPacket to send
            NetworkDiscoveryPacket.Builder ndpb = NetworkDiscoveryPacket.newBuilder();
            ndpb.setMode(NetworkDiscoveryPacket.Mode.REQUEST);
            ndpb.setSender(NetworkDiscoveryPacket.Sender.EXTERNAL_SERVER_NODE);
//            ndpb.setGroupTag(conf.getGroupTag());
            ndpb.setGroupTag("groupJPRS");
            ndpb.setNodeAddress("127.0.0.1");
            ndpb.setNodePort(8888);
            //ndpb.setNodePort(8887);
//            ndpb.setSecret(conf.getSecret());
            ndpb.setSecret("secret");

            Route.Builder rb = Route.newBuilder();
            rb.setId(nextId());
            rb.setPath(Route.Path.NETWORK_DISCOVERY);
            rb.setNetworkDiscoveryPacket(ndpb);

            // Broadcast the NetworkDiscovery request to internal discovery port.
            System.out.println("Going to broadcast");
            ch.writeAndFlush(new DatagramPacket(
                    Unpooled.copiedBuffer(rb.build().toByteArray()),
                    SocketUtils.socketAddress("255.255.255.255",8888))).sync();
            System.out.println("Broadcast done!");

            // DiscoveryClientHandler will close the DatagramChannel when a
            // response is received.  If the channel is not closed within 5 seconds,
            // print an error message and quit.
            if (!ch.closeFuture().await(10000)) {
                System.err.println("NetworkDiscovery request timed out.");
            }
        } catch (Exception e) {
            System.out.println("Failed to read route " + e);
        } finally {
            group.shutdownGracefully();
        }
    }

    /**
     * Since the service/server is asychronous we need a unique ID to associate
     * our requests with the server's reply
     *
     * @return
     */
    private static synchronized long nextId() {
        return ++curID;
    }
}

