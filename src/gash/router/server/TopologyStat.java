package gash.router.server;

import gash.router.container.NodeConf;
import gash.router.container.RoutingConf.RoutingEntry;
import io.netty.channel.Channel;

public class TopologyStat {
	
	private int ref;
	private String host;
	private int port;
	boolean active;
	Channel channel;
	 
	 TopologyStat(int ref, String host, int port){
		 this.ref = ref;
		 this.host = host;
		 this.port = port;
	 }
	
	public boolean isActive() {
		return active;
	}
	public void setActive(boolean active) {
		this.active = active;
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
