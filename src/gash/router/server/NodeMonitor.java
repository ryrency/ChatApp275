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
	NodeConf nodeConf;
	boolean forever = true;
	
//	List<RoutingEntry> entryList;
	public static NodeMonitor nodeMonitor;
	public static NodeMonitor getInstance(NodeConf nodeConf) {
		if (nodeMonitor == null) {
			nodeMonitor = new NodeMonitor(nodeConf);
		}
		return nodeMonitor;
	}
	
	NodeMonitor(NodeConf nc){
		this.nodeConf = nc;
		for(NodeConf.RoutingEntry re : nc.getRouting()) {
//			System.out.println("***Node Monitor****fn:Construcutor***");
			System.out.println(re.getId() + "," + re.getHost() + ","  + re.getPort());
			TopologyStat ts = new TopologyStat(re.getId(),re.getHost(),re.getPort());
			statMap.put(re.getId(),ts);
		}
		
	}
	
	public HashMap<Integer, TopologyStat> getStatMap() {
		return statMap;
	}
	
	public NodeConf getNodeConf() {
		return this.nodeConf;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		System.out.println("***Node monitor***fn:run***");
		for(TopologyStat ts : this.statMap.values()) {
			
			System.out.println("TOPO STat :"+ ts.getHost()+ "--" +ts.getPort()+ "--"+ts.isActive());
		}
		
		
		while(forever)
		{
			try {
				//System.out.println("***Node monitor***Call Write Call***");
				for(TopologyStat ts : this.statMap.values()) {
					if(ts.isActive() && ts.getChannel() !=null) {
						
//						System.out.println("***Node monitor***Write Call***");
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
//					for(TopologyStat ts1 : this.statMap.values()) {
//						
//						System.out.println("TOPO STat after On Add :"+ ts1.getHost()+ "--" +ts1.getPort()+ "--"+ts1.isActive());
//					}
//					
				}
				
			}
			catch(Exception ex) {
				ex.printStackTrace();
			}
		}
		
	
		
	}
	
	public void onAdd(TopologyStat ts) {
		EventLoopGroup group = new NioEventLoopGroup();
		try {
			System.out.println("/t***Node Monitor****fn:onAdd***");
			System.out.println(ts.getRef() + "," + ts.getHost() + ","  + ts.getPort());
			
			Bootstrap b = new Bootstrap();
			b.handler(new WorkHandler());
//			b.group(group).channel(NioSocketChannel.class).handler(new WorkInit(state, false));
			b.group(group).channel(NioSocketChannel.class).handler(new WorkInit());
			b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000);
			b.option(ChannelOption.TCP_NODELAY, true);
			b.option(ChannelOption.SO_KEEPALIVE, true);

			System.out.println("Host **" + ts.getHost());
			System.out.println("Port ** " + ts.getPort());
			
			ChannelFuture cf = b.connect(ts.getHost(), ts.getPort()).syncUninterruptibly();

			ts.setChannel(cf.channel());
			
			
			if (ts.getChannel()==null) System.out.println("Chanel is null");
			ts.setActive(true);
			cf.channel().closeFuture();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		finally {
			if(group!=null)
				group =null;
		}
		
	}
	
	

}
