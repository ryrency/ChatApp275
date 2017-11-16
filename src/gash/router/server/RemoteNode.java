package gash.router.server;

import gash.router.container.NodeConf;
import gash.router.container.RoutingConf.RoutingEntry;
import io.netty.channel.Channel;

public class RemoteNode {
	
	private NodeConf nodeConf;

	
	//state variables
	Channel channel;
	 
	 
	 RemoteNode(NodeConf conf){
		 nodeConf = conf;
	 }
 
	public boolean isActive() {
		return channel != null && channel.isActive() && channel.isWritable();
	}
	
	public Channel getChannel() {
		return channel;
	}
	public void setChannel(Channel channel) {
		this.channel = channel;
	}
	
	public NodeConf getNodeConf() {
		return nodeConf;
	}
}
