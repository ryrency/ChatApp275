package gash.router.server.resources;

import gash.router.container.RoutingConf;
import gash.router.server.raft.MessageBuilder;
import routing.Pipe.Route;

public class NetworkDiscoveryResource implements RouteResource {

	@Override
	public String getPath() {
		return "/NetworkDiscovery";
	}

	@Override
	public Route process(Route msg, RoutingConf conf ){
		Route response = null;
		if(msg.hasNetworkDiscoveryPacket()) {
			
		response = MessageBuilder.buildNetworkDiscoveryResponse(conf);
		}
		return response;
	}
}
