package gash.router.server;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import gash.router.container.NodeConf;
import gash.router.server.raft.MessageBuilder;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import raft.proto.Work.WorkMessage;

public class NodeMonitor implements Runnable {

	static ConcurrentHashMap<Integer, TopologyStat> statMap = new ConcurrentHashMap<Integer, TopologyStat>();
//	EventLoopGroup group = null;
//	static Bootstrap b;
	
	NodeConf nodeConf;
	boolean forever = true;
	private Timer timer_;
	static int count = 0;

	public static NodeMonitor nodeMonitor;
//	static {
//		 b = new Bootstrap();
//		 group = new NioEventLoopGroup();
//	}

	public static NodeMonitor getInstance(NodeConf nodeConf) {
		if (nodeMonitor == null) {
			nodeMonitor = new NodeMonitor(nodeConf);
		}
		return nodeMonitor;
	}

	public static NodeMonitor getInstance() {
		return nodeMonitor;
	}

	NodeMonitor(NodeConf nc) {
		this.nodeConf = nc;
		for (NodeConf.RoutingEntry re : nc.getRouting()) {
			System.out
					.println("Adding adjacent node channel -->" + re.getId() + "," + re.getHost() + "," + re.getPort());
			TopologyStat ts = new TopologyStat(re.getId(), re.getHost(), re.getPort());
			statMap.put(re.getId(), ts);
		}

	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		while (forever) {
			try {
				// System.out.println("size of hashmap *** " + statMap.size());
				for (TopologyStat ts : statMap.values()) {
					// System.out.println("Step1: Adjacent Node status -->" + ts.getHost() + "--" +
					// ts.getPort() + "--"
					// + ts.isActive() + "--" + ts.isExists());
					if (!ts.isActive() && ts.getChannel() == null) {
						count++;
						addAdjacentNode(ts);
						System.out.println("**Current state before wait - " + ts.isActive() +"," + ts.getChannel());
						while(!ts.isActive() && ts.getChannel() == null) {
							System.out.println("**Waiting for channel to come up for given node ID - **" + ts.getRef() );
							Thread.sleep(10000);
						}
						System.out.println("**Current state after wait - " + ts.isActive() +"," + ts.getChannel());
						// System.out.println("size of hashmap after adding a new node *** " +
						// statMap.size());
					}
				}
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
	}

	public synchronized void addAdjacentNode(TopologyStat ts) {
		
		try {
			System.out.println("Getting the count --> " + String.valueOf(count));
			EventLoopGroup group = new NioEventLoopGroup();
			Bootstrap b = new Bootstrap();
			System.out
					.println("Step 2: Add adjacent node --> " + ts.getRef() + "," + ts.getHost() + "," + ts.getPort());
			
			b.handler(new WorkHandler());
			// b.group(group).channel(NioSocketChannel.class).handler(new WorkInit(state,
			// false));
			b.group(group).channel(NioSocketChannel.class).handler(new WorkInit());
			b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000);
			b.option(ChannelOption.TCP_NODELAY, true);
			b.option(ChannelOption.SO_KEEPALIVE, true);

			ChannelFuture cf = b.connect(ts.getHost(), ts.getPort()).syncUninterruptibly();

			ts.setChannel(cf.channel());
			ts.setActive(true);
			ts.setExists(true);
			
			System.out.println(
					"Step 3: Adjacent Node status -->" + ts.getHost() + "--" + ts.getPort() + "--" + ts.isActive());
			
			cf.channel().closeFuture();
			cf.channel().closeFuture().addListener(new ChannelFutureListener() {
				@Override
				public void operationComplete(ChannelFuture future) throws Exception {
					System.out.println("Error: Connection closed");
					statMap.remove(ts.getRef());
					try {
						future.channel().close();
					}catch(Exception ex) {
						System.out.println("Exception" + ex.getMessage());
					}
					//scheduleConnect(ts, 1000);
				}
			});
			sendAddRequestToExistingNode(ts);
			statMap.put(ts.getRef(), ts);
			System.out.println("Printing hash map values");
			printStatMap();

		} catch (Exception ex) {
			ex.printStackTrace();

		} 
	}

	private synchronized void scheduleConnect(TopologyStat ts, long millis) {
		System.out.println("Error: Connection closed");
		timer_.schedule(new TimerTask() {
			@Override
			public void run() {
				addAdjacentNode(ts);
			}
		}, millis);
	}

	public void sendAddRequestToExistingNode(TopologyStat ts) {
		try {
			
			String hostAddress = getLocalHostAddress();
			
			System.out.println("Generated request to add adjacent node" + nodeConf.getNodeId() + ","
					+ hostAddress + "," + nodeConf.getWorkPort());

			WorkMessage workMessage = MessageBuilder.prepareInternalNodeAddRequest(nodeConf.getNodeId(),
					hostAddress, nodeConf.getWorkPort());

			ChannelFuture cf = ts.getChannel().writeAndFlush(workMessage);

			if (cf.isDone() && !cf.isSuccess()) {
				System.out.println("Send failed: " + cf.cause());
			}

			if (cf.isDone() && !cf.isSuccess()) {
				System.out.println("Comm failed");
			}
			// System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public ConcurrentHashMap<Integer, TopologyStat> getStatMap() {
		return statMap;
	}

	public synchronized void setStatMap(TopologyStat ts) {
		System.out.println("setStatMap --> " + statMap.size() + ts.getHost() + "," +ts.getPort());
		statMap.put(ts.getRef(), ts);
	}

	public NodeConf getNodeConf() {
		return this.nodeConf;
	}

	public void printStatMap() {
		System.out.println("***Printing stat map****");
		for (TopologyStat ts : statMap.values()) {

			System.out.println("TOPO STat :" + ts.getHost() + "--" + ts.getPort() + "--" + ts.isActive() + "------"
					+ ts.getChannel());
		}
	}
	
	public String getLocalHostAddress() {
		System.out.println("***NodeMonitor*** fn:getLocalHostAddress***");
		String hostAddress = null;
		
		try {
		Enumeration<NetworkInterface> netInterfaceEnum = NetworkInterface.getNetworkInterfaces();
		while(netInterfaceEnum.hasMoreElements()) {
//			System.out.println("***NodeMonitor*** fn:getLocalHostAddress*** Inside while #1*** ");
			NetworkInterface netInterface = netInterfaceEnum.nextElement();
//			System.out.println("NetworkInterface = "+netInterface.toString());
			if(!netInterface.isUp()) {
				continue;
			}
			
			Enumeration<InetAddress> inetAddrEnum = netInterface.getInetAddresses();
			
			while(inetAddrEnum.hasMoreElements()) {
//				System.out.println("***NodeMonitor*** dn:getLocalHostAddress*** Inside while #2");
				InetAddress inetAddr = inetAddrEnum.nextElement();
//				System.out.println("***NodeMonitor*** dn:getLocalHostAddress*** InetAdress = "+inetAddr);
				if(!inetAddr.isLoopbackAddress() && inetAddr instanceof Inet4Address) {
					hostAddress =  inetAddr.getHostAddress().toString();
//					System.out.println("***NodeMonitor*** dn:getLocalHostAddress*** hostAdress = "+hostAddress);
					break;
					
				}
			}
			if(hostAddress != null) {
				break;
			}
		}
	}
	
	catch(Exception e) {
		e.printStackTrace();
		
	}
	finally {
	}
		return hostAddress;

	}

}
