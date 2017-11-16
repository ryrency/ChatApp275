package gash.router.server;

import gash.router.server.raft.Candidate;
import gash.router.server.raft.Follower;
import gash.router.server.raft.Leader;
import gash.router.server.raft.NodeState;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.CharsetUtil;
import raft.proto.Internal.InternalPacket;
import raft.proto.InternalNodeAdd.InternalNodeAddRequest;
//import pipe.work.Work.WorkMessage; For now msg is coming as string
//import pipe.common.Common.Failure;
//import pipe.work.Work.Heartbeat;
//import pipe.work.Work.Task;
//import pipe.work.Work.WorkMessage;
//import pipe.work.Work.WorkState;
import raft.proto.Work.WorkMessage;

public class WorkHandler extends SimpleChannelInboundHandler<InternalPacket> {

	Follower foll;
	Leader leader;

	Candidate candidate;

	WorkHandler() {
		foll = Follower.getInstance();
		leader = Leader.getInstance();
		candidate = Candidate.getInstance();
	}

	public void handleMessage(InternalPacket packet, Channel channel) {
		
		if (packet.hasAppendEntriesRequest()) {
			
		} else if (packet.hasAppendEntriesResponse()) {
			
		} else if (packet.hasVoteRequest()) {
			
		} else if (packet.hasVoteResponse()) {
			
		}
		
//		if(wm.hasInternalNodeAddPacket()) {
//			if(wm.getInternalNodeAddPacket().hasInternalNodeAddRequest()) {
//				InternalNodeAddRequest internalNodeAddRequest = wm.getInternalNodeAddPacket().getInternalNodeAddRequest();
//				NodeMonitor nodeMonitor = NodeMonitor.getInstance();
//				nodeMonitor.addNode(new RemoteNode(internalNodeAddRequest.getId(), 
//						internalNodeAddRequest.getHost(), 
//						internalNodeAddRequest.getPort(), 
//						channel));
//			}
//		}
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
