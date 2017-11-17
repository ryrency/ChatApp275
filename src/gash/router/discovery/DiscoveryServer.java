package gash.router.discovery;

import gash.router.container.NodeConf;
import gash.router.container.RoutingConf;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DiscoveryServer implements Runnable {

    protected static Logger logger = LoggerFactory.getLogger("discovery");

    private RoutingConf conf;
    private NodeConf nodeConf;

    public DiscoveryServer(RoutingConf conf, NodeConf nodeConf) {
    	this.conf = conf;
        this.nodeConf = nodeConf;
    }

    @Override
    public void run() {
        logger.info("Discovery starting");

        EventLoopGroup group = new NioEventLoopGroup();

        try {
            Bootstrap b = new Bootstrap();
            b.group(group)
                    .channel(NioDatagramChannel.class)
                    .option(ChannelOption.SO_BROADCAST, true)
                    .handler(new DiscoveryInit(conf, nodeConf));

            logger.info("Starting server, listening on port = " + nodeConf.getNetworkDiscoveryPort());
            ChannelFuture f = b.bind(nodeConf.getNetworkDiscoveryPort()).sync();


            logger.info(f.channel().localAddress() + " -> open: " + f.channel().isOpen() + ", write: " + f.channel().isWritable() + ", act: " + f.channel().isActive());

            f.channel().closeFuture().await();

        } catch (InterruptedException ex) {
            // on bind().sync()
            logger.error("Failed to setup handler.", ex);
        } finally {
            group.shutdownGracefully();
        }
    }
}