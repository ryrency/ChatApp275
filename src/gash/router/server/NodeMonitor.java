package gash.router.server;

import java.util.HashMap;
import java.util.List;

import gash.router.container.NodeConf;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup; 
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.CharsetUtil;


public class NodeMonitor implements Runnable {
	
	HashMap<Integer, TopologyStat> statMap = new HashMap<Integer, TopologyStat>();
	
	boolean forever = true;
	
//	List<RoutingEntry> entryList;
	
	
	NodeMonitor(NodeConf nc){
		for(NodeConf.RoutingEntry re : nc.getRouting()) {
		System.out.println("***Node Monitor****fn:Construcutor***");
		System.out.println(re.getId() + "," + re.getHost() + ","  + re.getPort());
		TopologyStat ts = new TopologyStat(re.getId(),re.getHost(),re.getPort());
		statMap.put(re.getId(),ts);
		}
		
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		System.out.println("***Node monitor***fn:run***");
		while(forever)
		{
			try {
				//System.out.println("***Node monitor***Call Write Call***");
				for(TopologyStat ts : this.statMap.values()) {
					if(ts.isActive() && ts.getChannel() !=null) {
						
						System.out.println("***Node monitor***Write Call***");
						ChannelFuture cf = ts.getChannel().writeAndFlush(Unpooled.copiedBuffer("Ack", CharsetUtil.UTF_8));
						
						if (cf.isDone() && !cf.isSuccess()) {
						    System.out.println("Send failed: " + cf.cause());
						}
						
						if(cf.isDone()&& !cf.isSuccess()) {
							System.out.println("Comm failed");
						}
						
					}
					else {
						onAdd(ts);
					}
				}
				
			}
			catch(Exception ex) {
				ex.printStackTrace();
			}
		}
		
	
		
	}
	
	public void onAdd(TopologyStat ts) {
		
		try {
			System.out.println("/t***Node Monitor****fn:onAdd***");
			System.out.println(ts.getRef() + "," + ts.getHost() + ","  + ts.getPort());
			EventLoopGroup group = new NioEventLoopGroup();
			Bootstrap b = new Bootstrap();
			b.handler(new WorkHandler());
//			b.group(group).channel(NioSocketChannel.class).handler(new WorkInit(state, false));
			b.group(group).channel(NioSocketChannel.class).handler(new WorkInit());
			b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000);
			b.option(ChannelOption.TCP_NODELAY, true);
			b.option(ChannelOption.SO_KEEPALIVE, true);

			
			ChannelFuture cf = b.connect(ts.getHost(), ts.getPort()).syncUninterruptibly();

			ts.setChannel(cf.channel());
			ts.setActive(true);
			cf.channel().closeFuture();
		} catch (Exception ex) {
			ex.printStackTrace();
			
		}
		
	}
	
	

}
