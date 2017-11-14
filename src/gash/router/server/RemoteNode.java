package gash.router.server;

import gash.router.container.NodeConf;
import gash.router.container.RoutingConf.RoutingEntry;
import io.netty.channel.Channel;

public class RemoteNode {
	
	private int ref;
	private String host;
	private int port;
	boolean active;
	Channel channel;
	boolean exists;
	 
	 
	 RemoteNode(int ref, String host, int port){
		 setRef(ref);
		 setHost(host);
		 setPort(port);
	 }
	 
	 RemoteNode(int ref, String host, int port, Channel channel){
		 setRef(ref);
		 setHost(host);
		 setPort(port);
		 setChannel(channel);
	 }
 
	public boolean isActive() {
		return channel != null && channel.isActive();
	}
	
	public Channel getChannel() {
		return channel;
	}
	public void setChannel(Channel channel) {
		this.channel = channel;
	}
	public int getRef() {
		return ref;
	}
	public void setRef(int ref) {
		this.ref = ref;
	}
	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}

}
