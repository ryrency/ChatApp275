package gash.router.server;

import gash.router.server.raft.RaftNode;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.logging.Logger;

import raft.proto.Internal.InternalPacket;
//import pipe.work.Work.WorkMessage; For now msg is coming as string
//import pipe.common.Common.Failure;
//import pipe.work.Work.Heartbeat;
//import pipe.work.Work.Task;
//import pipe.work.Work.WorkMessage;
//import pipe.work.Work.WorkState;

public class WorkHandler extends SimpleChannelInboundHandler<InternalPacket> {

	WorkHandler() {
		
	}

	public void handleMessage(InternalPacket packet, Channel channel) {
		
		if (packet.hasAppendEntriesRequest()) {
			RaftNode.getInstance().handleAppendEntriesRequest(packet.getAppendEntriesRequest());
			
		} else if (packet.hasAppendEntriesResponse()) {
			RaftNode.getInstance().handleAppendEntriesResponse(packet.getAppendEntriesResponse());
			
		} else if (packet.hasVoteRequest()) {
			RaftNode.getInstance().handleVoteRequest(packet.getVoteRequest());
			
		} else if (packet.hasVoteResponse()) {
			RaftNode.getInstance().handleVoteResponse(packet.getVoteResponse());
			
		} else if (packet.hasConnectionActiveAck()) {
			int nodeId = packet.getConnectionActiveAck().getNodeId();
			Logger.getGlobal().info("received connection alive ack from node: " + nodeId);
			
			NodeMonitor
			.getInstance()
			.getNodeMap()
			.get(nodeId)
			.setChannel(channel);
		} else if (packet.hasForwardMessageRequest()) {
			RaftNode.getInstance().addMessage(packet.getForwardMessageRequest().getMessage());
		}
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, InternalPacket packet) throws Exception {
		handleMessage(packet, ctx.channel());

	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		
		System.out.println("Unexpected exception");
		System.out.println(cause.getMessage());

		ctx.close();
	}

}
