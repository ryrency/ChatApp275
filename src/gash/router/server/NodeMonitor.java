package gash.router.server;

import java.net.InetAddress;
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

	NodeConf nodeConf;
	boolean forever = true;
	private Timer timer_;

	public static NodeMonitor nodeMonitor;

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
//				System.out.println("size of hashmap *** " + statMap.size());
				for (TopologyStat ts : statMap.values()) {
//					System.out.println("Step1: Adjacent Node status -->" + ts.getHost() + "--" + ts.getPort() + "--"
//							+ ts.isActive() + "--" + ts.isExists());
					if (!ts.isActive() && ts.getChannel() == null) {
							addAdjacentNode(ts);
//							System.out.println("size of hashmap  after adding a new node *** " + statMap.size());
						}
				}
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
	}

	public void addAdjacentNode(TopologyStat ts) {
		EventLoopGroup group = new NioEventLoopGroup();

		try {
			System.out
					.println("Step 2: Add adjacent node --> " + ts.getRef() + "," + ts.getHost() + "," + ts.getPort());
			Bootstrap b = new Bootstrap();
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
			// Thread.sleep(10000);
			System.out.println(
					"Step 3: Adjacent Node status -->" + ts.getHost() + "--" + ts.getPort() + "--" + ts.isActive());
			// Thread.sleep(10000);
			cf.channel().closeFuture();
			cf.channel().closeFuture().addListener(new ChannelFutureListener() {
				@Override
				public void operationComplete(ChannelFuture future) throws Exception {
					scheduleConnect(ts, 1);
				}

			});
			sendAddRequestToExistingNode(ts);
			statMap.put(ts.getRef(), ts);
			System.out.println("Printing hash map values");
			printStatMap();

		} catch (Exception ex) {
			ex.printStackTrace();

		} finally {
			if (group != null) {
				group = null;
			}
		}

	}

	private void scheduleConnect(TopologyStat ts, long millis) {
		System.out.println("Error 1: Connection closed");
		timer_.schedule(new TimerTask() {
			@Override
			public void run() {
				addAdjacentNode(ts);
			}
		}, millis);
	}

	public void sendAddRequestToExistingNode(TopologyStat ts) {
		try {
			System.out.println("Generated request to add adjacent node" + nodeConf.getNodeId() + ","
					+ InetAddress.getLocalHost().getHostAddress() + "," + nodeConf.getWorkPort());

			WorkMessage workMessage = MessageBuilder.prepareInternalNodeAddRequest(nodeConf.getNodeId(),
					InetAddress.getLocalHost().getHostAddress(), nodeConf.getWorkPort());

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

}
