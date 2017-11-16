package gash.router.server;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import gash.router.container.NodeConf;
import gash.router.container.RoutingConf;
import gash.router.server.raft.MessageBuilder;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import raft.proto.Work.WorkMessage;

public class NodeMonitor {

	private static final long RECONNECT_DELAY = 1000;
	
	static ConcurrentHashMap<Integer, RemoteNode> nodeMap = new ConcurrentHashMap<Integer, RemoteNode>();
//	EventLoopGroup group = null;
//	static Bootstrap b;
	
	NodeConf nodeConf;
	private EventLoopGroup workerGroup = new NioEventLoopGroup();
	private ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);

	public static NodeMonitor instance;

	public static NodeMonitor getInstance() {
		if (instance == null) {
			instance = new NodeMonitor();
		}
		return instance;
	}

	NodeMonitor() {
		

	}
	
	public void init(RoutingConf conf, NodeConf nodeConf) {
		this.nodeConf = nodeConf;
		
		for (NodeConf node : conf.getNodes()) {
			RemoteNode rm = new RemoteNode(node);
			addNode(rm);
		}
	}

	public void start() {
		// TODO Auto-generated method stub
		for (RemoteNode rm : nodeMap.values()) {
			scheduleConnect(rm, 0);
		}
	}

	public synchronized void connectWithNode(RemoteNode rm) {
		try {
			Logger.getGlobal().info("starting to connect with node --> " + rm.getNodeConf().getInternalSocketServerAddress());
			Bootstrap b = new Bootstrap();
				
			b.group(workerGroup).channel(NioSocketChannel.class).handler(new WorkInit());
			b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000);
			b.option(ChannelOption.TCP_NODELAY, true);
			b.option(ChannelOption.SO_KEEPALIVE, true);

			ChannelFuture cf = b.connect(rm.getNodeConf().getHost(), rm.getNodeConf().getInternalPort()).syncUninterruptibly();
			Logger.getGlobal().info("remote node status--> " + rm.getNodeConf().getInternalSocketServerAddress() + " - channel status: open: " + cf.channel().isOpen() + ", active: " + cf.channel().isActive() + ", isWritable: " + cf.channel().isWritable());
				
			rm.setChannel(cf.channel());
				
			cf.channel().closeFuture().addListener(new ChannelClosedListener(rm, this));
			
			sendAddRequestToExistingNode(rm);
			
		} catch (Exception ex) {
			Logger.getGlobal().info("channel failed to connect with --> " + rm.getNodeConf().getInternalSocketServerAddress());
			scheduleConnect(rm, RECONNECT_DELAY);
		} 
	}

	public synchronized void scheduleConnect(final RemoteNode rm, long millis) {
		Logger.getGlobal().info("scheduling connect with " + rm.getNodeConf().getInternalSocketServerAddress() + " in " + millis + "ms");
		executor.schedule(new Runnable() {
			
			@Override
			public void run() {
				// TODO Auto-generated method stub
				NodeMonitor.this.connectWithNode(rm);
				
			}
		}, millis, TimeUnit.MILLISECONDS);
	}
	
	public void resetConnection(RemoteNode rm) {
		
	}

	public void sendAddRequestToExistingNode(RemoteNode rm) {
		try {
			
			String hostAddress = getLocalHostAddress();
			
			Logger.getGlobal().info("Generated request to add adjacent node: "  + rm.getNodeConf().getInternalSocketServerAddress());

			WorkMessage workMessage = MessageBuilder.prepareInternalNodeAddRequest(
					nodeConf.getNodeId(), 
					hostAddress, 
					nodeConf.getInternalPort()
			);

			ChannelFuture cf = rm.getChannel().writeAndFlush(workMessage);

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

	public ConcurrentHashMap<Integer, RemoteNode> getNodeMap() {
		return nodeMap;
	}

	public synchronized void addNode(RemoteNode rm) {
		Logger.getGlobal().info("adding new node in config " + rm.getNodeConf().getInternalSocketServerAddress());
		int nodeId = rm.getNodeConf().getNodeId();
		if (!nodeMap.containsKey(nodeId) || !nodeMap.get(nodeId).isActive()) {
			nodeMap.put(nodeId, rm);
		}
	}

	public NodeConf getNodeConf() {
		return this.nodeConf;
	}

	public void printNodeMap() {
		System.out.println("***Printing stat map****");
		for (RemoteNode rm : nodeMap.values()) {

			System.out.println("TOPO STat :" + rm.getNodeConf().getInternalSocketServerAddress() + "--" + rm.isActive() + "------"
					+ rm.getChannel());
		}
	}
	
	public  static String getLocalHostAddress() {
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
	
	public static class ChannelClosedListener implements ChannelFutureListener {
		RemoteNode rm;
		NodeMonitor nm;

		public ChannelClosedListener(RemoteNode stat, NodeMonitor monitor) {
			rm = stat;
			nm = monitor;
		}

		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			// we lost the connection or have shutdown.
			Logger.getGlobal().info("channel closed with server: " + rm.getNodeConf().getInternalSocketServerAddress());
			nm.scheduleConnect(rm, RECONNECT_DELAY);
		}
	}
	
	

}
