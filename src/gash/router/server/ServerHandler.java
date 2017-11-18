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
package gash.router.server;

import java.util.HashMap;
import java.util.logging.Logger;

import gash.router.container.RoutingConf;
import gash.router.server.raft.ClientChannelCache;
import gash.router.server.raft.RaftNode;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import raft.proto.Internal.ForwardMessageRequest;
import raft.proto.Internal.InternalPacket;
import routing.Pipe.Route;
/**
 * The message handler processes json messages that are delimited by a 'newline'
 * 
 * TODO replace println with logging!
 * 
 * @author gash
 * 
 */
public class ServerHandler extends SimpleChannelInboundHandler<Route> {

	private HashMap<String, String> routing;
	String uname = "user1";

	int currentNodeId;
	int currentLeaderId;
	RemoteNode leaderRemoteNode;

	public ServerHandler(RoutingConf conf) {
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
			System.out.println("ERROR: Unexpected content - " + msg);
			return;
		}
		
		currentNodeId = NodeMonitor.getInstance().getNodeConf().getNodeId();
		
		currentLeaderId = RaftNode.getInstance().getState().getCurrentLeader();
		
		if (currentNodeId == currentLeaderId) {
			//Append entries needs to be called 
			System.out.println("Leader is going to handle");
		}else {
			ForwardMessageRequest request = 
					ForwardMessageRequest
					.newBuilder()
					.setSenderId(msg.getMessage().getSenderId())
					.setPayload(msg.getMessage().getPayload())
					.setReceiverId(msg.getMessage().getReceiverId())
					.setTimestamp(msg.getMessage().getTimestamp())
					.build();
			
			InternalPacket packet = 
					InternalPacket
					.newBuilder()
					.setForwardMessageRequest(request)
					.build();
	
			leaderRemoteNode = NodeMonitor.getInstance().getNodeMap().get(currentLeaderId);
			if (leaderRemoteNode.isActive()) { 
				leaderRemoteNode.getChannel().writeAndFlush(packet);
			}	
		}
		
//		System.out.println("---> " + msg.getId() + ": " + msg.getPath() + ", " + msg.getMessage());
//		if(NodeState.getInstance().getState() == NodeState.LEADER) {
//			if (msg.hasMessage())
//			{
//				leader.handleClientRequest(msg);
//			   
//
//			}
//			else if (msg.hasUser()) {
//				/*Rency - Call leader to do user functions*/
//				leader.handleUsers(msg);
//			}
//			else if (msg.hasMessagesRequest()) {
//				/*Pull messages from server*/
//				leader.handleMessageRequest(msg);
//				}
//			else if (msg.hasGroup()) {{
//				/*
//				 * to implement group!!
//				 */
//			}
//				
//		}
//		else if(NodeState.getInstance().getState() == NodeState.FOLLOWER) {
//			// TO be implemented later ******************
//		}
//
//		System.out.flush();
//		}
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
		System.out.println("-------Jsingh-----");
		handleMessage(msg, ctx.channel());
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		System.out.println("Unexpected exception from downstream." + cause);
//		ClientChannelCache.getInstance().deleteClientChannelFromMap(uname);
		ctx.close();
	}

}