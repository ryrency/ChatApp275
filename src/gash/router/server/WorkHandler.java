package gash.router.server;

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

public class WorkHandler extends SimpleChannelInboundHandler<WorkMessage>{
	
	Follower foll;
	Leader leader;
		WorkHandler(){
			foll = Follower.getInstance();
			leader = Leader.getInstance();
		}
		
		
		
		
		public void handleMessage(WorkMessage wm, Channel channel) {
//			if (msg == null) {
//				// TODO add logging
//				System.out.println("ERROR: Unexpected content - " + msg);
//				return;
//			}
			try {
			if (wm == null) {
				// TODO add logging
				System.out.println("ERROR: Unexpected content - " + wm);
				return;
			}

			System.out.println("Into handleMessage : Message - "+wm); 
				if (wm.hasHeartBeatPacket() ) {
				                if (NodeState.getInstance().getState() == NodeState.FOLLOWER)
				                    foll.handleHeartBeat(wm);
				                else if (NodeState.getInstance().getState() == NodeState.LEADER)
				                    leader.handleHeartBeat(wm);
				            }
			if (wm.hasAppendEntriesPacket())
			{
//                foll.getWorkQueue();
			}
			if(wm.hasInternalNodeAddPacket())
			{
				if(wm.getInternalNodeAddPacket().hasInternalNodeAddRequest()) {
					InternalNodeAddRequest internalNodeAddRequest = wm.getInternalNodeAddPacket().getInternalNodeAddRequest();
					NodeMonitor nodeMonitor = NodeMonitor.getInstance();
					nodeMonitor.setStatMap(new TopologyStat(internalNodeAddRequest.getId(), internalNodeAddRequest.getHost(), internalNodeAddRequest.getPort()));
				}
			}
			}catch(Exception ex) {
				ex.printStackTrace();
				
			}

//			if (debug)
//				PrintUtil.printWork(msg);
			
			
			// TODO How can you implement this without if-else statements?
//			try {
//				if (msg.hasBeat()) {
//					Heartbeat hb = msg.getBeat();
//					logger.debug("heartbeat from " + msg.getHeader().getNodeId());
//				} else if (msg.hasPing()) {
//					logger.info("ping from " + msg.getHeader().getNodeId());
//					boolean p = msg.getPing();
//					WorkMessage.Builder rb = WorkMessage.newBuilder();
//					rb.setPing(true);
//					rb.setSecret(1);
//					channel.write(rb.build());
//				} else if (msg.hasErr()) {
//					Failure err = msg.getErr();
//					logger.error("failure from " + msg.getHeader().getNodeId());
//					// PrintUtil.printFailure(err);
//				} else if (msg.hasTask()) {
//					Task t = msg.getTask();
//				} else if (msg.hasState()) {
//					WorkState s = msg.getState();
//				}
//			} catch (Exception e) {
//				// TODO add logging
//				Failure.Builder eb = Failure.newBuilder();
//				eb.setId(state.getConf().getNodeId());
//				eb.setRefId(msg.getHeader().getNodeId());
//				eb.setMessage(e.getMessage());
//				WorkMessage.Builder rb = WorkMessage.newBuilder(msg);
//				rb.setErr(eb);
//				rb.setSecret(1);
//				channel.write(rb.build());
//			}

			System.out.flush();

		}


		@Override
		protected void channelRead0(ChannelHandlerContext ctx, WorkMessage workMessage) throws Exception {
			// TODO Auto-generated method stub
//			System.out.println("****WorkInit*****Channel Read****");
//			ByteBuf in = (ByteBuf) msg;
			handleMessage(workMessage, ctx.channel());
			
		}
		
		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
//			logger.error("Unexpected exception from downstream.", cause);
			System.out.println("Unexpected exception");
			System.out.println(cause.getMessage());
			
			ctx.close();
		}
		
		
		
		
}
