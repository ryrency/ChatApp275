package gash.router.server;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.CharsetUtil;
//import pipe.work.Work.WorkMessage; For now msg is coming as string
//import pipe.common.Common.Failure;
//import pipe.work.Work.Heartbeat;
//import pipe.work.Work.Task;
//import pipe.work.Work.WorkMessage;
//import pipe.work.Work.WorkState;

public class WorkHandler extends SimpleChannelInboundHandler<ByteBuf>{
		WorkHandler(){
			
		}
		
		public void handleMessage(String msg, Channel channel) {
			if (msg == null) {
				// TODO add logging
				System.out.println("ERROR: Unexpected content - " + msg);
				return;
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
				try {
//					System.out.println("Message is -"+msg);
				}
				catch(Exception e) {
					e.printStackTrace();
				}

			System.out.flush();

		}


		@Override
		protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
			// TODO Auto-generated method stub
//			System.out.println("****WorkInit*****Channel Read****");
			ByteBuf in = (ByteBuf) msg;
			handleMessage(in.toString(CharsetUtil.UTF_8), ctx.channel());
			
		}
		
		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
//			logger.error("Unexpected exception from downstream.", cause);
			System.out.println("Unexpected exception");
			System.out.println(cause.getMessage());
			
			ctx.close();
		}
		
		
		
		
}
