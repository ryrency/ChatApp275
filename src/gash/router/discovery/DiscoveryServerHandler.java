/**
 * Copyright 2016 Gash.
 *
 * This file and intellectual content is protected under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package gash.router.discovery;

import gash.router.container.NodeConf;
import gash.router.container.RoutingConf;
import gash.router.server.resources.NetworkDiscoveryResource;
import gash.router.server.resources.RouteResource;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import io.netty.util.CharsetUtil;
import io.netty.util.internal.SocketUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import routing.Pipe;
import routing.Pipe.Route;

import java.beans.Beans;
import java.util.HashMap;

/**
 * The networkDiscovery handler processes json messages that are delimited by a 'newline'
 *
 * TODO replace println with logging!
 *
 * @author gash
 *
 */
public class DiscoveryServerHandler extends SimpleChannelInboundHandler<Route> {
	protected static Logger logger = LoggerFactory.getLogger("discovery");
	RoutingConf conf;
	NodeConf nodeConf;
	private HashMap<String, String> routing;

	public DiscoveryServerHandler(RoutingConf conf, NodeConf nodeConf) {
		this.conf = conf;
		this.nodeConf = nodeConf;
		if (conf != null)
			routing = conf.asHashMap();
	}

	/**
	 * override this method to provide processing behavior. This implementation
	 * mimics the routing we see in annotating classes to support a RESTful-like
	 * behavior (e.g., jax-rs).
	 *
	 * @param msg
	 */
	public void handleMessage(Route msg, Channel channel) {
		if (msg == null) {
			// TODO add logging
			System.out.println("ERROR: Unexpected content - " + msg);
			return;
		}

		System.out.println("---> " + msg.getId() + ": " + msg.getPath());

		try {
			System.out.println("/" + msg.getPath().toString().toLowerCase());
			String clazz = routing.get("/" + msg.getPath().toString().toLowerCase());
			if (clazz != null) {
				RouteResource rsc = (RouteResource) Beans.instantiate(RouteResource.class.getClassLoader(), clazz);
//				NetworkDiscoveryResource rsc = new NetworkDiscoveryResource();
				try {
					Route response = rsc.process(msg, conf);
					System.out.println("---> reply: " + response + " to: " + msg.getNetworkDiscoveryPacket().getNodeAddress());
					if (response != null) {

						channel.writeAndFlush(new DatagramPacket(
								Unpooled.copiedBuffer(response.toByteArray()),
								SocketUtils.socketAddress(
										msg.getNetworkDiscoveryPacket().getNodeAddress(), 
										nodeConf.getNetworkDiscoveryPort())))
								.sync();

					}
				} catch (Exception e) {
					logger.error("Failed to read route.", e);
				}
			} else {
				// TODO add logging
				System.out.println("ERROR: unknown path - " + msg.getPath());
			}
		} catch (Exception ex) {
			// TODO add logging
			System.out.println("ERROR: processing request - " + ex.getMessage());
		}

		System.out.flush();
	}

	/**
	 * a message was received from the server. Here we dispatch the message to
	 * the client's thread pool to minimize the time it takes to process other
	 * messages.
	 *
	 * @param ctx
	 *            The channel the message was received from
	 * @param msg
	 *            The message
	 */
	@Override
	protected void channelRead0(ChannelHandlerContext ctx, Route msg) throws Exception {
		System.out.println("------------");
		handleMessage(msg, ctx.channel());
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		logger.error("Unexpected exception from downstream.", cause);
		ctx.close();
	}

}