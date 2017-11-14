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
import raft.proto.InternalNodeAdd.InternalNodeAddRequest;
//import pipe.work.Work.WorkMessage; For now msg is coming as string
//import pipe.common.Common.Failure;
//import pipe.work.Work.Heartbeat;
//import pipe.work.Work.Task;
//import pipe.work.Work.WorkMessage;
//import pipe.work.Work.WorkState;
import raft.proto.Work.WorkMessage;

public class WorkHandler extends SimpleChannelInboundHandler<WorkMessage> {

	Follower foll;
	Leader leader;

	Candidate candidate;

	WorkHandler() {
		foll = Follower.getInstance();
		leader = Leader.getInstance();
		candidate = Candidate.getInstance();
	}

	public void handleMessage(WorkMessage wm, Channel channel) {
		
		try {
			if (wm == null) {
				System.out.println("ERROR: Unexpected content - " + wm);
				return;
			}

			System.out.println("Into handleMessage : Message - " + wm);
			if (wm.hasHeartBeatPacket()) {
				if (NodeState.getInstance().getState() == NodeState.FOLLOWER)
					foll.handleHeartBeat(wm);
				else if (NodeState.getInstance().getState() == NodeState.LEADER)
					leader.handleHeartBeat(wm);
			}
			if (wm.hasVoteRPCPacket()) {
				if (NodeState.getInstance().getState() == NodeState.FOLLOWER)
					foll.handleRequestVote(wm);
				else if (NodeState.getInstance().getState() == NodeState.CANDIDATE)
					candidate.handleResponseVote(wm);

			}
			if (wm.hasAppendEntriesPacket()) {
				if (NodeState.getInstance().getState() == NodeState.FOLLOWER)
					foll.handleAppendEntries(wm);
				else if (NodeState.getInstance().getState() == NodeState.LEADER)
					leader.handleAppendEntries(wm);
			}
			if (wm.hasInternalNodeAddPacket()) {
				if (wm.getInternalNodeAddPacket().hasInternalNodeAddRequest()) {
					InternalNodeAddRequest internalNodeAddRequest = wm.getInternalNodeAddPacket()
							.getInternalNodeAddRequest();
					NodeMonitor nodeMonitor = NodeMonitor.getInstance();
					nodeMonitor.setStatMap(new TopologyStat(internalNodeAddRequest.getId(),
							internalNodeAddRequest.getHost(), internalNodeAddRequest.getPort()));
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();

		}
		System.out.flush();

	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, WorkMessage workMessage) throws Exception {
		handleMessage(workMessage, ctx.channel());

	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		
		System.out.println("Unexpected exception");
		System.out.println(cause.getMessage());

		ctx.close();
	}

}
